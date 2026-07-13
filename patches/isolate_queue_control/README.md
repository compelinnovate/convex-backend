# Isolate Queue Delay Control and Deployment Lane

This patch adds a lane-aware queue policy only to the isolate scheduler. It is
strictly opt in through `ISOLATE_QUEUE_DELAY_CONTROL_ENABLED=true`. When the
knob is unset or false, the scheduler continues to use the existing generic
CoDel queue, and generic CoDel behavior elsewhere in the backend is unchanged.

The policy is intended to make isolate queue overload bounded and observable without sacrificing
dependency progress. An optional extension classifies backend-known analysis and configuration
evaluation requests into a bounded `control_plane` lane with a longer hard deadline and no adaptive
shedding. Neither feature creates execution capacity, replaces worker limits, or grants
unconditional dispatch priority.

## Relationship to generic CoDel

The isolate scheduler constructs exactly one external queue implementation when
the isolate client starts. `new_scheduler_queue` selects:

- `SchedulerQueueSender::Legacy` and `SchedulerQueueReceiver::Legacy`, backed
  by the generic `CoDelQueue`, when
  `ISOLATE_QUEUE_DELAY_CONTROL_ENABLED=false`;
- `SchedulerQueueSender::LaneDelayControl` and
  `SchedulerQueueReceiver::LaneDelayControl`, backed by the isolate-specific
  `IsolateDelayQueue` through `IsolateQueueSender` and
  `IsolateQueueReceiver`, when the knob is true.

These are alternative implementations, not consecutive queue stages. An
external isolate request enters one of them, never both. Generic `CoDelQueue`
instances used by caches, thread pools, search, or other backend components are
independent of this scheduler choice.

`CODEL_QUEUE_IDLE_EXPIRATION_MILLIS` and
`CODEL_QUEUE_CONGESTED_EXPIRATION_MILLIS` configure the legacy isolate queue
and other generic CoDel users. They have no effect on `IsolateDelayQueue`.
Conversely, the `ISOLATE_QUEUE_DELAY_*` and
`ISOLATE_CONTROL_PLANE_*` duration and lane settings do not change generic
CoDel behavior. The isolate client still parses the lane-policy settings while
the policy is disabled so malformed latent configuration fails at startup.

`IsolateDelayQueue` shares CoDel's broad goal of bounding sustained queueing
delay, but it neither wraps nor implements the generic CoDel algorithm. After
the generic queue remains continuously nonempty for its idle-expiration period,
it dispatches unexpired work in LIFO rather than FIFO order and returns expired
oldest entries separately. The isolate-specific policy instead preserves FIFO
among simultaneously eligible requests and adds scheduler-aware eligibility,
per-lane delay observations, adaptive shedding, and absolute hard deadlines.

## Queue and lane model

The scheduler keeps one `VecDeque`, not one queue per lane. Its shared base
capacity is `ISOLATE_QUEUE_SIZE`. Only dependency requests may use an additional
`ISOLATE_DEPENDENCY_WORKER_RESERVE` entries. The sum is the finite physical
capacity.

Each request is classified from the scheduler properties already propagated by
the dependency-reserve patch:

- `dependency` when `unblocks_ancestor` is true;
- `independent_action` when it is an isolate action and does not unblock an
  ancestor;
- `ordinary` otherwise.

When `ISOLATE_CONTROL_PLANE_LANE_ENABLED=true`, the five typed analysis and configuration
evaluation variants described below map to `control_plane` before the ordinary fallback. A
control-plane request paired with dependency ancestry is an invariant violation; deployment work
never consumes dependency reserve.

Dependency role takes precedence over action identity. A child action that
unblocks an isolate-holding ancestor is in the dependency lane, while active
request metrics retain `is_isolate_action` separately. This prevents action
identity and ancestry role from being collapsed into one classification.

The queue remains physically FIFO. On each dispatch attempt, the scheduler
evaluates entries against one immutable snapshot of worker state and selects
the oldest eligible request in the entire queue. It may skip an older request
only when that request is currently blocked by one or more of:

- the physical worker total;
- shared-base worker capacity;
- the per-client total;
- the per-client shared base;
- the independent-action cap.

This selection rule preserves FIFO among simultaneously eligible requests and
allows work for one client or class to proceed around an ineligible head. It
does not grant queue admission beyond the class's finite capacity.

## Per-lane delay controller

Each lane has independent state:

- an observation interval deadline;
- minimum selection sojourn observed in the current interval;
- sample count in that interval;
- current overloaded state.

Only selected, non-expired requests are controller observations. An interval is
evaluated after it has completed, before the current selection becomes the first
sample of a fresh interval. The dispatch-sojourn histogram is recorded only
after the worker channel accepts the selected request.

A lane enters overload only when a completed interval contains at least two
samples and its minimum sojourn is strictly greater than
`ISOLATE_QUEUE_DELAY_TARGET_MILLIS`. Requiring two samples prevents one delayed
request from declaring persistent overload.

A measured interval clears overload when it contains at least one sample and
its minimum sojourn is at or below the target. An interval with no samples does
not synthesize a minimum and does not change overload state. Completely
draining a lane clears its controller immediately, including partial interval
state. A later enqueue starts that lane with a fresh interval.

The controller never changes request eligibility. Once an ordinary or
independent-action request has been selected by normal scheduler eligibility,
it is adaptively rejected only when its lane is overloaded and its own sojourn
is strictly greater than `ISOLATE_QUEUE_DELAY_SHED_THRESHOLD_MILLIS`. A newer
request is not rejected because another request was old. Dependencies are
observed and can report overload, but they are never adaptively shed.

Ordinary, independent-action, and dependency entries use the same finite hard age,
`ISOLATE_QUEUE_HARD_MAX_AGE_MILLIS`. An enabled control-plane entry uses
`ISOLATE_CONTROL_PLANE_HARD_MAX_AGE_MILLIS`. Each entry stores its absolute deadline, and the
consuming receiver and its non-consuming expiry companion arm the earliest deadline across the
queue, so a newer ordinary request can expire before an older control-plane request and expiry does
not depend on another enqueue or worker completion. The companion remains polled while a selected
request waits for its initial active-JavaScript permit. Hard expiry is checked before normal
selection.

## Cancellation, closure, and resource ownership

Receive cancellation does not remove an item unless the receive has completed.
Event listeners and expiry timers are receiver-local, so canceling one pending
receive leaves queue contents and subsequent wakeups intact. The expiry
companion does not increment the consuming-receiver count or keep admission
open after the main receiver closes.

Dropping the last sender wakes the receiver. The receiver returns `None` only
after already queued entries have dispatched or expired. Dropping the last
receiver closes admission and drains the queue. Rejected sends and drained
entries are dropped after releasing the queue mutex because request-owned
resources may have arbitrary drop implementations. Receiver-local expiration
timer futures are also replaced and dropped only after releasing that mutex.
The legacy CoDel sender and expiration receiver use the same destruction rule
without changing capacity, deadline, selection, or wake behavior. Both expiry
companions drain retained entries after the last sender closes and terminate
when the last consuming receiver closes and drains the queue.

Scheduler worker completion remains the first branch of the biased dispatch
loop. A completion updates global and per-client accounting before the next
selection snapshot. Both external queue policies keep an eligibility-aware
receive pending while no queued request can dispatch and a separate expiry
receive pending while a selected request waits for its initial permit. Their
deadline timers continue to expire retained requests without requiring another
enqueue or worker completion. The lane policy additionally reports
physical-total and other ineligibility reasons for the retained entries. Direct nested-UDF
callbacks use upstream's separate internal priority path and do not consume an
external CoDel or lane entry. That path removes closed callers and can skip an
older per-client-ineligible callback for the oldest callback currently eligible
in the same snapshot; FIFO remains intact among simultaneously eligible
internal callbacks.

After an external entry becomes scheduler-eligible, its original hard deadline
continues to bound the low-priority active-JavaScript permit wait. The worker is
assigned only after that permit is acquired. Direct internal callbacks instead
use upstream's high-priority permit wait without a CoDel deadline because those
nested requests cannot be retried safely. Both paths still consume the same
physical worker total and dependency reserve when the scheduler assigns a
worker.

The periodic lane metrics reporter shares only the locked queue state. It does
not clone receiver wake state or keep the queue logically open. The expiry
companion likewise shares queue state without becoming a consuming receiver.
This separation allows the scheduler to poll queue receipt, expiry during
permit acquisition, and metric refresh in one `select!` without conflicting
receiver borrows.

## Strict configuration

All isolate-specific queue knobs used by this patch are parsed strictly when the
isolate client is built, including when lane control is disabled. Present
numeric values must contain only ASCII decimal digits, and the enable knob must
be a valid Boolean. Empty, signed, malformed, and numerically overflowed values
fail startup. The generic `CODEL_QUEUE_*` knobs retain their existing parser and
remain outside this patch's configuration contract.

Construction additionally requires:

- nonzero queue base capacity;
- queue base plus dependency reserve to fit in `usize`;
- nonzero delay target and observation interval;
- adaptive-shed threshold greater than the delay target;
- hard maximum age greater than the adaptive-shed threshold;
- timing durations representable by the runtime timer.

### Knob reference

- `ISOLATE_QUEUE_SIZE` has default `2000` in unmodified
  `get-convex/convex-backend`. It is the shared-base queue capacity under
  either policy, not a throughput target. Larger values retain more request
  arguments and stale work during overload. Dependency-only entries supplied
  by `ISOLATE_DEPENDENCY_WORKER_RESERVE` are additional capacity owned by the
  dependency-capacity patch.
- `ISOLATE_QUEUE_DELAY_CONTROL_ENABLED` has built-in patch value `false` when
  unset. Enabling it selects `IsolateDelayQueue` at startup; changing it
  requires a process restart and should be verified through
  `isolate_queue_policy_info`.
- `ISOLATE_QUEUE_DELAY_TARGET_MILLIS` has built-in patch value `150`. It is the
  minimum-sojourn target used to classify a completed observation interval,
  not a request deadline. Lower values detect delay sooner; set it from healthy
  queue-sojourn measurements rather than end-to-end request latency.
- `ISOLATE_QUEUE_DELAY_INTERVAL_MILLIS` has built-in patch value `1000`. Each
  lane evaluates its minimum observed sojourn over this interval. Shorter
  intervals react faster but are more sensitive to sparse samples.
- `ISOLATE_QUEUE_DELAY_SHED_THRESHOLD_MILLIS` is derived as twice the configured
  target when unset, which is `300` with the built-in target. An overloaded
  ordinary or independent-action lane sheds a selected request only when that
  request's own queue age exceeds this value. Set it above the target and below
  the ordinary hard maximum age. It never adaptively sheds dependency or
  control-plane work.
- `ISOLATE_QUEUE_HARD_MAX_AGE_MILLIS` has built-in patch value `5000`. It is the
  absolute enqueue-to-initial-active-permit deadline for ordinary,
  independent-action, and dependency requests. Set it above the adaptive-shed
  threshold but below relevant outer request timeouts.
- `ISOLATE_CONTROL_PLANE_LANE_ENABLED` has built-in patch value `false`. It
  classifies only the five typed analysis and configuration-evaluation request
  variants. It requires lane delay control and grants neither priority nor
  reserved queue or worker capacity.
- `ISOLATE_CONTROL_PLANE_QUEUE_CAPACITY` has built-in patch value `16`. It is a
  queued-entry sub-cap inside `ISOLATE_QUEUE_SIZE`, not a separate queue or a
  reservation. When the lane is enabled it must be at least
  `ANALYZE_CONCURRENCY` and no greater than `ISOLATE_QUEUE_SIZE`.
- `ISOLATE_CONTROL_PLANE_HARD_MAX_AGE_MILLIS` has built-in patch value `30000`.
  It gives control-plane work a finite enqueue-to-initial-active-permit budget
  and must exceed the ordinary hard maximum age. Keep caller, proxy, and
  deployment-phase timeouts long enough for this wait plus execution.
- `CODEL_QUEUE_IDLE_EXPIRATION_MILLIS` has default `5000` in unmodified
  `get-convex/convex-backend`. It controls when the generic CoDel queue changes
  from its idle FIFO regime to congestion handling; it is not a hard maximum
  request age.
- `CODEL_QUEUE_CONGESTED_EXPIRATION_MILLIS` has default `50` in unmodified
  `get-convex/convex-backend`. It controls item expiration in the generic
  CoDel queue's congested regime. It does not configure lane-mode shedding or
  hard deadlines.

The eight `ISOLATE_QUEUE_DELAY_*`, `ISOLATE_QUEUE_HARD_MAX_AGE_MILLIS`, and
`ISOLATE_CONTROL_PLANE_*` knobs above are introduced by this patch. Their
built-in values are part of the patch contract, not defaults in unmodified
`get-convex/convex-backend`.

All three `ISOLATE_CONTROL_PLANE_*` values and `ANALYZE_CONCURRENCY` are parsed
strictly even while the lane is disabled. Intrinsic values must be positive and
representable by the runtime timer. When enabled, lane-aware queueing must also
be enabled, the lane cap must be at least `ANALYZE_CONCURRENCY` and no greater
than `ISOLATE_QUEUE_SIZE`, and the control-plane hard deadline must exceed the
ordinary hard deadline. These checks prevent one push from rejecting its
configured fan-out and keep the longer contract explicit.

## Metrics and interpretation

The queue publishes bounded-label metrics keyed by `pool_name` and closed enum
labels. No request, client, deployment, route, or application identifier is
used as a queue metric label.

- `isolate_queue_policy_info{policy}` identifies `legacy_codel` or
  `lane_delay_control`.
- `isolate_queue_capacity_info{capacity_kind}` reports shared-base and total
  queue capacity. Lane mode also reports the parsed control-plane sub-cap even
  when control-plane classification is disabled.
- `isolate_queue_config_millis_info{config_kind}` reports the active queue's
  timing configuration. Lane mode reports target, interval, adaptive-shed
  threshold, ordinary hard age, and control-plane hard age; legacy mode reports
  its idle and congested expiration values.

The remaining metrics in this list are published only while lane delay control
is enabled. Legacy mode retains its existing generic CoDel and scheduler
metrics in addition to the policy, capacity, and timing configuration above.

- `isolate_queue_depth_info{lane}` is updated on every lane mutation.
- `isolate_queue_oldest_age_seconds{lane}` is refreshed once per second and is
  set to zero immediately when a lane drains.
- `isolate_queue_sojourn_seconds{lane}` observes only requests actually
  dispatched to workers. Rejected requests are not dispatch samples.
- `isolate_queue_rejections_total{lane,reason}` separates `hard_expired`,
  `delay_control_shed`, and `queue_full`.
- `isolate_queue_overloaded_info{lane}` and
  `isolate_queue_overload_transitions_total{lane,transition}` expose controller
  state and entered/cleared transitions.
- `isolate_queue_ineligible_info{lane,reason}` reports the count blocked by each
  scheduler constraint in the latest selection snapshot. One entry can count
  under more than one reason.

When lane delay control is enabled, the closed `lane` set includes `control_plane`, and the queue
reports `capacity_kind="control_plane_lane"` plus
`config_kind="control_plane_hard_max_age_millis"` from the parsed settings even while
control-plane classification is disabled. Enabling the deployment lane also adds
`scheduler_class="control_plane"`; `isolate_control_plane_lane_enabled_info{pool_name}` proves
whether that classification is effective.
Rejection reasons distinguish `lane_full`, `queue_full`, `hard_expired`, `caller_dropped`,
`scheduler_closed`, and `no_worker`. `delay_control_shed` and dependency-reserve use for
`control_plane` are invariant violations and must remain zero.

The patch extends the dependency-capacity scheduler counter initialization with the control-plane
class and the additional lane-aware rejection reasons. It also initializes every valid queue
rejection and overload-transition label combination at zero. These labeled failure counters remain
non-evicting, so a compatible running backend exposes a measured zero without requiring a failure
or overload transition first.

Depth and overload are mutation-exact. Oldest age necessarily advances without
queue mutation, so its scrape-facing value can lag real time by up to the
one-second refresh period. Receiver drain clears depth, oldest age, overload,
and ineligibility gauges so scheduler shutdown does not leave positive queue
state behind.

The scheduler's existing enqueue, dispatch, expiry, rejection, active-class,
reserve-use, and capacity metrics remain necessary context. In particular,
`isolate_scheduler_active_requests_info` carries `is_isolate_action`
independently from `scheduler_class`. The scheduler expiry counter covers the
original enqueue deadline whether it is reached while the entry remains queued
or during the selected entry's initial active-permit wait.

HTTP admission is a separate queue. Its waiter gauge counts only requests that
actually entered that wait, and its wait histogram records waits ending in
permit handoff or cancellation. Immediate admissions produce no wait sample.
These metrics should be compared with isolate queue age rather than treated as
the same backlog.

## Optional deployment/control-plane lane

The optional extension protects admitted module analysis and configuration evaluation from the
short ordinary overload deadline without treating deployment as dependency or priority work. It is
useful when a self-hosted backend must support function pushes during ordinary or high application
load and already uses the lane-aware queue policy.

The lane contains exactly these `RequestType` variants:

- `Analyze`;
- `EvaluateSchema`;
- `EvaluateAuthConfig`;
- `EvaluateAppDefinitions`;
- `EvaluateComponentInitializer`.

The production match is exhaustive. Runtime UDF or HTTP-action module evaluation, database work
during a push, `finish_push` transactions, source upload, and Node-executor analysis remain outside
the lane. Classification never matches application module, function, component, route, deployment,
client, or tenant names.

When disabled, these requests retain ordinary scheduler class, deadline, and shedding behavior.
When enabled, they remain in the same physical FIFO and use only shared-base queue and worker
capacity. An older eligible ordinary request runs before a newer control-plane request, and an older
eligible control-plane request runs before newer ordinary work. Existing eligibility may skip an
older entry blocked by physical, shared-base, per-client, or action-cap constraints; the lane adds
no priority rule.

The control-plane lane has a sub-cap inside `ISOLATE_QUEUE_SIZE`. Admission requires both:

```text
control_plane_depth < ISOLATE_CONTROL_PLANE_QUEUE_CAPACITY
total_shared_depth < ISOLATE_QUEUE_SIZE
```

The cap does not reserve entries from ordinary work and does not add to physical capacity. A full
control-plane lane rejects another control-plane request while ordinary work can still use shared
space. A full shared base rejects control-plane work even when dependency-reserve entries remain.

Control-plane entries participate in delay observations and overload metrics but are never rejected
by adaptive delay shedding. They still fail on lane-full or shared-queue admission, caller drop,
their finite hard deadline, scheduler closure, or worker failure. The built-in 30-second deadline is
an enqueue-to-active-permit budget: it bounds both queue residence and the low-priority
active-JavaScript permit wait that now precedes worker assignment. V8 execution, HTTP, proxy, CLI,
and deployment-phase timeouts remain separate.

Each post-admission analysis or evaluation retry is a new queue entry with a new deadline. The
patch does not combine existing bounded attempts into one larger budget or change
`ANALYZE_CONCURRENCY`.

Analysis and evaluation requests carry one-shot response senders but no UDF cancellation signal.
After selection and before worker allocation, the scheduler checks whether a control-plane caller
has disappeared. It also watches response closure while acquiring the initial active-JavaScript
permit, canceling that permit wait when the caller disappears. A closed caller is removed with
reason `caller_dropped` without incrementing active-worker accounting. Cancellation is lazy while
queued: a canceled entry can remain counted until selection or hard expiry, but the lane cap and
deadline bound retained state. If the caller drops after the final pre-dispatch check, evaluation
can still begin and response delivery fails normally.

The longer deadline does not guarantee deployment success. A request can still approach hard
expiry when shared-base workers, active-JavaScript permits, or CPU remain saturated, or fail at an
outer HTTP, source-storage, Node, database, or caller boundary. Correlate lane age and dispatch with
analysis duration, active permits, host CPU, deployment endpoint status, and retries.

### Deferred shared-base reservation

The initial patch does not reserve a worker. Consider carving one worker from shared base only if
repeated measurements show all of the following:

- an admitted control-plane request approaches its hard deadline;
- non-dependency runtime work continuously occupies shared base;
- the control-plane lane is below its cap and dependency reserve is healthy;
- active-JavaScript permits and CPU have headroom;
- no outer service or caller deadline is responsible.

That later design would reduce the ordinary ceiling from `B` to `B - 1` while allowing
control-plane work to use `B`; it would not add capacity above `T` or borrow dependency reserve.
Correct implementation would need global and per-client active-class accounting. Do not add a
borrowing, preemption, or dynamic-priority protocol without separate evidence and review.

### Deployment-lane rollout

1. Apply the extension with `ISOLATE_CONTROL_PLANE_LANE_ENABLED=false` and verify the closed lane,
   metric, configuration, and exhaustive-match changes.
2. Record ordinary deployment duration, queue age, scheduler class, active-permit wait, CPU, retry,
   and final status.
3. Enable the lane with a cap consistent with `ANALYZE_CONCURRENCY` and restart one controlled
   backend population.
4. Confirm enabled state, lane capacity, deadline, and zero initial depth locally and in remote
   metrics.
5. Run an ordinary push under representative traffic. Verify FIFO dispatch, no adaptive shedding,
   no dependency-reserve use, finite expiry, and unchanged ordinary deadlines.
6. Observe repeated normal and busy pushes before considering a worker reservation or larger cap.

Rollback sets `ISOLATE_CONTROL_PLANE_LANE_ENABLED=false` and restarts the backend. The lane-aware
ordinary/dependency/action policy remains active, and no schema or data migration is required.

### Deployment-lane verification

Focused tests cover all five typed variants, disabled behavior, impossible dependency ancestry,
lane and shared-base caps, no reserve use, per-entry deadlines, earliest-expiry timers, no adaptive
shedding, FIFO across ordinary and control-plane entries, caller-drop removal, scheduler closure,
strict configuration, and bounded metrics. Production verification uses an ordinary push and real
traffic; a dedicated deployment stress fixture is not required.

Rejected designs include treating deployment as a dependency, unconditional priority, a second
queue or worker pool, immediate worker reservation, borrowing dependency reserve, globally longer
ordinary deadlines, deployment-wide shedding switches, larger total queues, HTTP-path or module-name
classification, and relying on retries alone. Each either weakens liveness/fairness, adds substantial
state, or fails to classify the typed work the isolate scheduler actually executes.

## Resource cost and current evidence

Lane mode stores an enqueue timestamp and lane with each bounded queue entry.
Expiry discovery, oldest-eligible selection, and earliest-deadline discovery
scan at most `ISOLATE_QUEUE_SIZE + ISOLATE_DEPENDENCY_WORKER_RESERVE` entries;
removing an interior `VecDeque` entry can also move entries. The once-per-second
metrics refresh scans the queue for each lane. The expiry companion repeats a
bounded deadline/ineligibility scan when the scheduler polls it. Lane mode adds
at most two receiver-local hard-expiry timers (the consuming receive and its
expiry companion) plus one scheduler metrics timer. The legacy path uses the
existing CoDel receiver and expiry-companion timers but does not construct the
lane metrics timer.

No representative load test has yet measured the lane policy's throughput,
latency, memory, or scan cost. The existing scheduler load results in
the dependency-capacity design reference predate this policy and are not validation of it.
Current evidence is deterministic queue and scheduler regression coverage; a
mixed-load comparison remains required before changing the policy from opt-in.

## Interaction with other backend patches

The dependency-reserve patch supplies the ancestry property, finite queue
reserve, worker reserve, application-gate reserve, per-client rules, and action
cap used by this policy. Lane control does not replace those mechanisms. If
dependency propagation or reserve sizing is wrong, disabling lane control will
not repair dependency liveness.

The HTTP admission patch protects callback re-entry at the outer service gate.
Its reserve and waiter queue are independent of isolate queue capacity. A
request can pass HTTP admission and still wait at an application gate or in the
isolate queue. Size each stage from its own occupancy and wait metrics.

The unified context-reuse patch reduces repeated V8 context initialization for
eligible modules and can therefore change isolate service time, queue sojourn,
and memory per worker. It does not alter queue classification or capacity. Evaluate context
reuse and lane control as separate opt-ins so a latency or memory regression
has an unambiguous rollback. Context reuse can be disabled without changing
queue state; queue control can be disabled without changing context semantics.

`FUNRUN_ISOLATE_ACTIVE_THREADS` remains a CPU-execution gate below worker
assignment. A request can retain a worker while not holding an active-thread
permit. Queue delay control cannot create CPU capacity or reserve active-thread
permits for dependencies.

Query coalescing and application dependency gates run before the isolate queue.
They must continue propagating dependency role so a child does not wait behind
work that cannot use the worker reserve. The lane policy assumes those existing
liveness contracts; it does not infer ancestry from request type.

## Rollout and rollback

Before enabling lane control, collect the policy, capacity, scheduler class,
HTTP waiter, application wait, and generic CoDel baseline metrics. Verify that
dependency requests dispatch through worker reserve under a bounded saturation
test and that independent actions obey their cap.

Enable the queue policy on one controlled backend population and restart. Check
that policy and duration metrics match the intended configuration, all four
lane gauges initialize, dependency shedding stays at zero, hard expiry remains
finite, and oldest age returns to zero after drain. Compare adaptive shedding
with actual dispatch sojourn rather than with end-to-end request latency.

The immediate rollback is:

1. Set `ISOLATE_QUEUE_DELAY_CONTROL_ENABLED=false`.
2. Restart the affected backend process.
3. Confirm `isolate_queue_policy_info{policy="legacy_codel"}` and the legacy
   expiration configuration.

This rollback leaves dependency propagation, HTTP and application admission,
worker reserve, queue capacity, action caps, and context-reuse policy unchanged.
If overload continues after rollback, reduce incoming admission or execution
capacity separately; increasing queue depth alone cannot increase throughput.
