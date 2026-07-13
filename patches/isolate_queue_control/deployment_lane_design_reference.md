# Deployment and Control-Plane Isolate Lane

> Detailed design reference. This is not a separate adoption unit. The current operator-facing
> queue and deployment-lane contract is consolidated in
> [`README.md`](README.md). This reference preserves the complete
> motivation, scheduling proof, deferred-reservation design, rejected alternatives, and test matrix.

Status: implemented by the current `convex-backend` patch stack. Production
enablement and mixed-load validation remain operator rollout work.

This patch classifies backend-known module analysis and configuration
evaluation requests as a `control_plane` isolate queue lane. These requests
remain inside ordinary shared-base queue and worker capacity. They do
not use dependency reserve, receive unconditional dispatch priority, or create
a dedicated worker pool. The lane instead receives three narrow
protections:

- a finite lane-local queue occupancy cap;
- exemption from adaptive queue-delay shedding;
- a finite hard queue deadline longer than the ordinary request deadline.

The built-in patch value for the initial hard deadline is 30 seconds. The single physical queue
continues to select the oldest eligible request, preserving FIFO among
eligible control-plane and runtime work. A later one-worker reservation from
shared base is deliberately excluded from the initial patch and should be
considered only if measurements still show deployment starvation after
ordinary query containment is active.

The classification is derived from Rust request variants. It contains no
application module, function, component, route, deployment, client, or tenant
allowlist.

## Operator decision summary

This patch is useful for a self-hosted deployment that must support function
pushes during normal or high application load and already carries the
lane-aware isolate queue policy. It improves the chance that admitted analysis
and evaluation work survives a short runtime burst without weakening the
dependency liveness reserve.

The patch does not guarantee that a deployment succeeds under sustained CPU
saturation, a full shared-base queue, a caller timeout shorter than the lane
deadline, or a deployment failure outside the isolate scheduler. It does not
reserve execution capacity in its initial form. Operators still need enough
shared-base worker and active-JavaScript capacity for the request to dispatch
and execute.

## Motivation

Function deployment has a different latency contract from ordinary reactive
traffic. An interactive query can often be retried or temporarily represented
by an older value. Module analysis and configuration evaluation are finite
steps in an operator-requested control-plane operation. Rejecting one can fail
or restart a complete push after source upload and other preparation have
already succeeded.

The isolate client currently sends module analysis and all configuration
evaluation variants through the same scheduler as UDFs and actions. They are
non-actions, do not unblock an isolate-holding ancestor, and cannot block on a
separately scheduled descendant. Their current scheduling properties therefore
classify them as ordinary independent work.

Under the lane-aware queue policy, ordinary requests can be adaptively shed
after their lane demonstrates sustained queue delay and the selected request's
own age exceeds `ISOLATE_QUEUE_DELAY_SHED_THRESHOLD_MILLIS`. Every ordinary,
independent-action, and dependency entry also shares the hard maximum age from
`ISOLATE_QUEUE_HARD_MAX_AGE_MILLIS`. This policy is designed for short-latency
runtime requests rather than a bounded deployment phase.

Analysis already limits each push to `ANALYZE_CONCURRENCY` simultaneous module
requests. After a request is admitted, analysis retries a rejected-before-
execution or overload response up to three attempts with backoff. The four
evaluation entry points use the same post-admission retry pattern. An immediate
physical-queue or lane-cap admission error returns directly from the isolate
client; any retry at that boundary belongs to the outer caller. Concurrent
pushes, dry-run evaluations, retries, and normal runtime work can still overlap.
Rejection followed by retry adds more scheduler arrivals without making the
original control-plane step progress.

The intended behavior is:

1. The backend recognizes its own five analysis and evaluation request
   variants.
2. An admitted control-plane request consumes only shared queue and worker
   capacity.
3. It waits in the same FIFO as ordinary and dependency work.
4. It cannot be rejected by adaptive delay control.
5. It still fails after a finite, lane-specific hard deadline or when its
   lane-local or physical queue capacity is full.
6. Its queue occupancy, age, dispatch, expiry, and rejection remain visible
   through bounded metrics.

This changes overload treatment without asserting that deployment work is more
important than dependency progress or every runtime request.

## Exact classification boundary

The control-plane lane contains exactly these `RequestType` variants from the
isolate client:

- `Analyze`;
- `EvaluateSchema`;
- `EvaluateAuthConfig`;
- `EvaluateAppDefinitions`;
- `EvaluateComponentInitializer`.

`EvaluateSchema` and `EvaluateAuthConfig` can also be called by control-plane
operations outside the main push sequence. They still perform backend-owned
configuration evaluation and should receive the same queue contract.

The classifier should be an exhaustive method on `RequestType`, conceptually:

```rust
fn is_control_plane(&self) -> bool {
    match self {
        Self::Analyze { .. }
        | Self::EvaluateSchema { .. }
        | Self::EvaluateAuthConfig { .. }
        | Self::EvaluateAppDefinitions { .. }
        | Self::EvaluateComponentInitializer { .. } => true,
        Self::Udf { .. } | Self::Action { .. } | Self::HttpAction { .. } => false,
        #[cfg(test)]
        Self::Test { is_control_plane, .. } => *is_control_plane,
    }
}
```

Do not use a wildcard for production variants. A future request type should
force a compile-time decision about its lane.

The classifier must not include:

- runtime UDF or HTTP-action module evaluation;
- database queries or mutations used during a push;
- `finish_push` transaction and OCC retry work;
- source-package upload or storage work;
- Node executor analysis, which does not use this isolate queue;
- requests selected by module, function, route, or component name.

The word `Evaluate` in an internal timer, trace, or function name is not a
classification boundary. Only the typed isolate request variants above qualify.

## Goals

- Improve peak-hour reliability of admitted analysis and configuration
  evaluation.
- Keep classification backend-owned and exhaustive.
- Keep control-plane requests below shared-base queue and worker ceilings.
- Preserve dependency-only queue and worker overflow.
- Preserve FIFO among all simultaneously eligible requests.
- Exempt the lane from adaptive delay shedding without making it immortal.
- Give the lane a finite, configurable hard deadline.
- Bound lane occupancy independently of total queue capacity.
- Distinguish lane-capacity, physical-capacity, hard-expiry, caller-drop, and
  dispatch outcomes in metrics.
- Preserve current behavior when the feature is disabled.
- Remain a small extension of the maintained isolate queue and scheduler patch.

## Non-goals

- The patch does not classify HTTP deployment routes at the proxy.
- It does not grant deployment requests dependency status.
- It does not reserve dependency queue entries or workers.
- It does not provide unconditional control-plane priority.
- It does not guarantee deployment completion under continuous overload.
- It does not increase total queue, worker, CPU, or memory capacity.
- It does not change `ANALYZE_CONCURRENCY` or deployment retry counts.
- It does not redesign deployment API retries or idempotency.
- It does not cancel an analysis already executing inside V8 when its caller
  disconnects.
- It does not cover Node executor analysis or database commit contention.
- It does not add a shared-base worker reservation in the initial patch.

## Scheduling properties

`RequestSchedulingProperties` should gain an `is_control_plane` Boolean derived
from the exact request variant. Its queue-lane mapping should be exhaustive and
apply these rules:

1. Assert that a control-plane request never carries dependency ancestry.
2. An enabled, valid control-plane request is control plane.
3. A backend dependency is a dependency.
4. An independent V8 or HTTP action is an independent action.
5. Everything else is ordinary.

All five control-plane constructors currently use `Request::new`, which assigns
`SchedulerDependencyClass::Independent`. `Request` construction or scheduling-
property derivation should fail fast if one of these variants is ever paired
with `UnblocksAncestor`. The implementation must not silently ignore the
ancestry bit or map the impossible combination to the dependency lane, because
either path could make later eligibility consume reserve.

The scheduler class label should report `control_plane` for this lane instead
of the current `independent`. `can_block_on_descendant` and
`is_isolate_action` remain false. Active request accounting should retain a
separate bounded control-plane count only if needed for later reservation or
metrics; the initial worker eligibility policy can use the existing total and
per-client counts.

Classification is effective only when the lane feature is enabled. When it is
disabled, these variants retain their current ordinary lane, `independent`
scheduler label, queue deadline, and shedding behavior. This makes rollback a
real behavior rollback rather than only a configuration display change.

## Shared capacity and reserve invariants

The maintained scheduler uses:

- `T = MAX_ISOLATE_WORKERS`, the physical assigned-worker maximum;
- `R = ISOLATE_DEPENDENCY_WORKER_RESERVE`, dependency-only overflow;
- `B = T - R`, the shared worker base.

A control-plane request is a non-dependency. It can start only while global and
per-client shared-base eligibility allows it. It can never raise global
occupancy above `B` or per-client occupancy above that client's base. It is not
an isolate action and does not consume the independent-action cap.

The queue has the analogous structure:

- `Q = ISOLATE_QUEUE_SIZE`, shared physical queue capacity;
- `R` additional dependency-only entries;
- `Q + R` total physical entries.

A control-plane enqueue uses only `Q`. If total queue occupancy is already at
`Q`, the enqueue fails even if dependency reserve entries remain unused. A
control-plane request must never cause `used_reserved_capacity` to become true,
and dependency queue-reserve metrics must remain zero for its scheduler class.

These rules are more important than the longer deadline. Treating deployment
as dependency work would let a large module analysis fan-out consume the slots
that prevent action callback deadlock.

## One physical FIFO

The patch should extend the existing `IsolateQueueLane` enum with
`ControlPlane`. It should not create a second queue.

The lane-aware queue already stores all lanes in one `VecDeque` and selects the
oldest eligible entry across the complete buffer. That selection rule remains:

- an older eligible ordinary request runs before a newer control-plane request;
- an older eligible control-plane request runs before a newer ordinary request;
- an ineligible entry can be skipped according to existing global, per-client,
  shared-base, physical-total, and action-cap constraints;
- dependency overflow remains eligibility, not unconditional priority below
  shared base.

New arrivals cannot overtake an admitted, eligible control-plane request. The
longer deadline therefore allows finite older work to drain without introducing
a priority scheduler. Continuous normal traffic arriving later cannot starve an
already queued control-plane request under this FIFO rule.

The legacy CoDel queue does not carry the lane-specific deadline and shedding
contract. The initial patch should require the maintained lane-aware queue when
the control-plane feature is enabled rather than attempting to reproduce the
policy through a second wrapper around legacy CoDel.

## Lane-local queue occupancy

Control-plane requests can carry source maps, module bundles, component
definitions, and environment maps. A longer hard deadline must not allow them
to occupy the entire shared queue or retain unbounded request memory.

The queue should enforce a lane-local maximum in addition to shared physical
capacity:

```text
control_plane_depth < ISOLATE_CONTROL_PLANE_QUEUE_CAPACITY
total_depth < ISOLATE_QUEUE_SIZE
```

Both conditions must hold before a control-plane enqueue. The lane capacity is
a sub-cap inside shared capacity; it does not add queue entries or reserve them
from ordinary work. If the control-plane lane is full but the shared queue has
space, ordinary and independent-action requests may still enqueue. If the
shared queue is full, control-plane admission fails even when its lane depth is
below its own cap.

The queue sender should return a typed bounded error that distinguishes:

- `queue_full`, when shared physical base is full;
- `lane_full`, when the control-plane lane cap is full;
- `scheduler_closed`, when the receiver is gone.

The public deployment call continues receiving a rejected-before-execution
error compatible with its existing error handling. The isolate client's own
three-attempt loops retry only errors received after a successful enqueue;
`queue_full` and `lane_full` return directly. Metrics and traces retain the
exact internal reason.

Lane depth is incremented only after both checks succeed and decremented on
dispatch, hard expiry, receiver drain, or any other removal. Overflow and
underflow remain fail-fast invariants. Queue-owned request resources are still
dropped after releasing the queue mutex.

The lane cap bounds simultaneous queued attempts, not total modules in a push.
`analyze` submits at most `ANALYZE_CONCURRENCY` module requests concurrently for
one push and submits later modules as earlier ones finish. Concurrent pushes
multiply that fan-out, which is why a separate finite lane cap is still needed.

## Adaptive shedding and hard expiry

The control-plane lane should maintain queue-depth, oldest-age, sojourn, and
delay-controller observations. Its controller may report that the lane is
overloaded, but selection must never return
`IsolateQueueRejection::DelayControlShed` for either `Dependency` or
`ControlPlane`.

The exemption is narrow. A control-plane entry can still be rejected because:

- the lane was full at enqueue;
- shared physical queue base was full at enqueue;
- its caller disappeared before dispatch;
- it reached its control-plane hard deadline;
- the scheduler closed or a selected worker failed.

Every queue entry should store its absolute hard deadline at enqueue. Ordinary,
independent-action, and dependency entries derive it from
`ISOLATE_QUEUE_HARD_MAX_AGE_MILLIS`. Control-plane entries derive it from
`ISOLATE_CONTROL_PLANE_HARD_MAX_AGE_MILLIS`.

Storing the deadline on the entry keeps expiration stable and makes the
different lane durations explicit. `next_expiration` must arm the earliest
absolute deadline across all entries. Hard-expiry discovery must inspect each
entry's own deadline, so an older 30-second control-plane request cannot prevent
a newer ordinary request from expiring at its shorter deadline.

The scheduler must keep a non-consuming expiry companion polled while a
selected external request waits for its initial active-JavaScript permit.
Otherwise serial queue receipt can leave a different retained entry past its
own deadline until the selected request's later permit deadline. The companion
shares queue state but does not keep admission open after the consuming
receiver closes.

The initial control-plane built-in value should be 30 seconds. Upstream now acquires
the low-priority active-JavaScript permit before worker assignment, so this is
an enqueue-to-active-permit budget: the original deadline bounds both queue
residence and that permit wait. It is not a total deployment or JavaScript
execution timeout. Existing V8, HTTP, proxy, CLI, and deployment-phase
deadlines remain separate. Operators must keep those outer deadlines long
enough for this admission budget plus expected execution and response time; the
backend cannot validate remote caller or proxy timeouts at startup.

Each post-admission analysis or evaluation retry is a new queue entry with a
new hard deadline. The patch does not turn three bounded attempts into one
shared 90-second budget. Attempt count and backoff remain unchanged, so
operators must also account for their total possible wall time.

## Caller cancellation and abandoned requests

The five control-plane request variants carry oneshot response senders but no
UDF-style cancellation signal. If an HTTP request, CLI process, or deployment
future is dropped while an item waits, its response receiver can close before
dispatch. Extending hard queue age from the ordinary value to 30 seconds makes
executing such abandoned work materially more expensive.

Current source reuses exhaustive synchronous and asynchronous response-closure
helpers across every isolate request variant. After either ingress selects an
item, the scheduler discards a closed-caller request and also observes closure
while the initial active-JavaScript permit is pending. The local
`caller_dropped` scheduler and queue metrics are emitted only for control-plane
requests. A discarded request does not allocate a worker or increment
active-request accounting.

This is lazy cancellation while the request remains in the external queue. A
canceled entry can remain counted until it reaches selection or hard expiry,
but lane and physical caps keep that state bounded. Closure that occurs during
the selected request's permit wait cancels that wait rather than retaining the
ingress until the deadline.
Adding one event listener per response sender or redesigning queue wakeups is
not justified for the initial patch.

If the caller disappears after worker dispatch, current evaluation continues
and its response send fails harmlessly. Active V8 cancellation would require
separate environment and isolate-cleanliness analysis and is outside this
patch.

## Configuration

The initial patch should expose:

```text
ISOLATE_CONTROL_PLANE_LANE_ENABLED=false
ISOLATE_CONTROL_PLANE_QUEUE_CAPACITY=16
ISOLATE_CONTROL_PLANE_HARD_MAX_AGE_MILLIS=30000
```

The enable value must parse as a Boolean. Numeric values must contain only
ASCII decimal digits. Empty, signed, malformed, non-Unicode, overflowed, and
otherwise invalid values fail startup rather than falling back.

All three values should be parsed even while the lane is disabled. Malformed,
zero, overflowing, non-representable, and other intrinsically invalid values
always fail. Cross-setting policy checks apply when the lane is enabled; this
preserves startup for an operator whose existing ordinary deadline or queue
size is intentionally outside the new lane's built-in relationship.

Intrinsic validation requires:

- positive lane capacity;
- a positive control-plane hard deadline;
- a control-plane deadline representable by the runtime timer.

`ANALYZE_CONCURRENCY` must also be greater than zero regardless of lane
enablement. `buffer_unordered(0)` does not poll its source, so accepting zero
would stall isolate analysis before any request reaches the queue.

Enabled-mode validation additionally requires:

- lane capacity at least `ANALYZE_CONCURRENCY`, so one push's configured
  analysis fan-out cannot reject itself solely on the lane cap;
- lane capacity no greater than `ISOLATE_QUEUE_SIZE`;
- control-plane hard deadline greater than
  `ISOLATE_QUEUE_HARD_MAX_AGE_MILLIS`;
- `ISOLATE_QUEUE_DELAY_CONTROL_ENABLED=true`.

The built-in patch lane capacity of 16 is generic rather than host-sized. With
the default `ANALYZE_CONCURRENCY=4` in unmodified
`get-convex/convex-backend`, it permits one push's module fan-out, its
evaluation requests, and limited overlap while preventing concurrent pushes
from filling a large shared queue with bundles. Operators that raise analysis
concurrency must raise the lane cap explicitly before enabling the feature.

The self-hosted Compose template should pass all three values through without
supplying deployment-specific overrides. The backend remains the source of
built-in values and strict validation.

There is intentionally no control-plane worker-reserve setting in the initial
patch. Do not add a dormant optional value before measurements justify that
policy.

## Metrics and traces

All metric labels must remain closed and bounded. Do not label by module,
component, source package, route, deployment, push identifier, client, tenant,
or request ID.

The bounded scheduler and lane telemetry includes these closed values:

- `isolate_control_plane_lane_enabled_info{pool_name}` as a `0` or `1` gauge
  proving whether the five request variants are effectively classified;
- `scheduler_class="control_plane"` for enqueue, dispatch, expiry, rejection,
  and active-request metrics;
- `lane="control_plane"` for depth, oldest age, sojourn, overload state,
  overload transitions, ineligibility, and rejection metrics;
- `capacity_kind="control_plane_lane"` for the configured lane occupancy cap;
- `config_kind="control_plane_hard_max_age_millis"` for its hard deadline;
- rejection reasons `lane_full`, `queue_full`, `hard_expired`,
  `caller_dropped`, `scheduler_closed`, and `no_worker` at their existing
  ownership boundaries.

The enabled-state gauge is emitted on both queue policies. The control-plane
capacity and deadline series are emitted only when the lane-aware queue is
constructed. With that queue active they report parsed settings even while
control-plane classification is disabled; their absence on the legacy queue
path is expected.

`delay_control_shed` for `lane="control_plane"` is an invariant violation and
should remain zero. Dependency queue-reserve enqueue and worker-reserve dispatch
must also remain zero for `scheduler_class="control_plane"`.

The five request variants are already a closed set, but the initial scheduler
metrics do not need another operation label. Existing analysis and evaluation
timers can identify service cost. Add a closed `operation` label only if those
signals cannot answer which variant dominates, not in the initial design.

Useful trace properties are effective lane, request variant as a closed enum,
queue admission outcome, queue rejection reason, and caller-drop outcome. Do
not record module paths, source, environment variables, component arguments, or
other request payloads.

Operators should correlate:

- control-plane depth, oldest age, sojourn, and hard expiry;
- lane-full and physical queue-full rejection;
- enqueue-to-dispatch ratio across the bounded deployment retry window;
- ordinary, action, and dependency queue age and rejection;
- shared-base and dependency-reserve worker occupancy;
- active-JavaScript permit wait;
- module analysis and evaluation duration;
- backend CPU throttling and pressure;
- deployment endpoint duration and final status.

A healthy busy deployment can show nonzero control-plane queue age without
adaptive shedding, followed by FIFO dispatch and successful completion. A
30-second hard expiry with idle CPU indicates a different gate or scheduler
misconfiguration. High CPU and active-permit wait indicate that a queue lane
cannot create the missing execution capacity.

## Interaction with other maintained patches

[`README.md`](README.md) defines the combined operator contract for all four
lanes. The control-plane feature extends the original three-lane implementation
with one enum variant, one controller slot, one lane-local cap, and one longer
deadline. Enabling the control-plane lane requires lane-aware queueing.
Disabling the control-plane lane preserves lane-aware policy for dependency,
independent-action, and ordinary requests.

[`dependency_capacity/README.md`](../dependency_capacity/README.md) supplies the
shared-base and dependency-reserve worker and queue model. Control-plane work
uses base capacity only. The patch must not change dependency eligibility,
reserve sizing, action caps, per-client limits, or worker selection.

[`degradable_reactive_queries/README.md`](../degradable_reactive_queries/README.md) is
complementary demand containment. Its degradable query leader cap can keep a
bounded frontend recomputation wave from consuming all shared-base workers.
That capacity makes FIFO control-plane progress more likely without reserving a
worker. Neither patch requires application module names, and either feature can
be disabled independently.

Context reuse and query-context prewarming can reduce competing runtime service
cost and leave more shared capacity available. Context reuse supports the
eligible database-UDF, ordinary-action, and HTTP-action kinds; query-context
prewarming remains query-specific. Neither applies to the five control-plane
request variants, which have no reusable-context key in the current isolate
client. Recycling or prewarming analysis contexts would be a separate backend
design. None of these features changes lane
classification, queue capacity, deadlines, or reserve access.

The outer HTTP service, application function limiters, storage uploads, Node
executor, database transactions, and reverse proxy retain their own queues and
timeouts. This isolate lane cannot repair starvation or failure at those
boundaries.

## Deferred shared-base worker reservation

Do not reserve a worker in the initial patch. First combine the lane with
ordinary query containment and measure deployments under representative busy
traffic.

Consider one statically reserved shared-base worker only when all of the
following recur:

- a control-plane request is admitted and remains queued near its hard
  deadline;
- shared-base workers are continuously occupied by non-dependency ordinary
  work;
- the control-plane lane is below its lane cap;
- dependency queue and worker reserve are healthy;
- active-JavaScript permits and host CPU have enough headroom for another
  execution;
- the failure is not an outer HTTP, storage, Node, database, or caller timeout.

That later design would carve `C=1` from `B`, not add a worker above `T` and not
take one from `R`. Non-dependency ordinary work would stop at `B-C`, while
control-plane work could use the original shared-base ceiling `B`. Dependencies
would retain their current eligibility through `T`.

Correct implementation would require active class accounting globally and per
client. An ordinary eligibility check cannot use only total active workers,
because one active control-plane request should occupy the reserved class slot
without reducing the intended ordinary allowance twice.

The reservation would be static and can leave one worker idle when no
control-plane request exists. It should not become a borrowing or preemption
protocol in the same patch. Dependency work may still consume available
physical capacity first; preserving dependency liveness remains more important
than an absolute deployment guarantee.

If CPU or active-JavaScript permits are saturated, a worker reservation merely
moves the wait below worker assignment and can make throughput worse. That is
why queue evidence alone is not sufficient to enable it.

## Resource cost

The initial patch adds one Boolean scheduling property, one closed queue lane,
one lane-depth counter, one controller slot for observation, and one absolute
deadline per bounded queue entry. Existing fixed arrays grow from three lanes
to four. Queue scans remain bounded by
`ISOLATE_QUEUE_SIZE + ISOLATE_DEPENDENCY_WORKER_RESERVE`.

The lane-local cap reduces the maximum number of queued source-bearing
control-plane requests. The 30-second deadline can retain each admitted request
longer than the ordinary lane, but total retained control-plane entries remain
bounded by the smaller lane cap.

The expiry companion performs the same bounded deadline and ineligibility scan
when polled; it does not add an unbounded index or per-entry listener.

No thread, worker, semaphore, timer per request, separate queue, module map, or
background task is added. The consuming receiver and its non-consuming expiry
companion each own at most one timer for the earliest hard deadline across all
lanes. Separate queue-change events prevent one receiver from consuming the
other's wakeup.

## Rollout and rollback

1. Apply the patch with `ISOLATE_CONTROL_PLANE_LANE_ENABLED=false`.
2. Verify that all existing lane arrays, exhaustive matches, metrics, and tests
   include the new closed value without changing ordinary behavior.
3. Record baseline deployment analysis/evaluation duration, ordinary and
   dependency queue metrics, active permits, CPU, and deployment outcomes.
4. Set a lane capacity consistent with `ANALYZE_CONCURRENCY`, enable the feature,
   and restart one controlled backend population.
5. Confirm the enabled gauge is `1`, then confirm the policy, lane capacity,
   hard deadline, and zero initial depth on the backend metrics endpoint and
   remote metrics store. With lane-aware queueing active, capacity and deadline
   alone do not prove enablement because their parsed values are also exposed
   while classification is disabled.
6. Run an ordinary push while representative runtime traffic is present.
7. Verify FIFO dispatch, no adaptive control-plane shedding, no dependency
   reserve use, finite hard expiry, and unchanged ordinary hard deadline.
8. Observe multiple normal deployment cycles before considering any capacity
   adjustment.
9. Do not add the deferred worker reservation unless the evidence conditions in
   the previous section are met.

The immediate rollback is:

1. Set `ISOLATE_CONTROL_PLANE_LANE_ENABLED=false`.
2. Restart the backend.
3. Confirm that analysis and evaluation requests report the ordinary lane and
   scheduler class.

Rollback leaves the lane-aware queue itself, dependency propagation, queue and
worker reserve, action cap, query containment, and context reuse unchanged. It
requires no schema or data migration.

## Verification boundary

Focused deterministic tests should cover the scheduler and queue state machine.
A dedicated deployment stress fixture is not required.

Classification tests:

- all five production request variants classify as control plane when enabled;
- UDF, action, and HTTP-action variants remain non-control-plane;
- disabling the feature restores ordinary classification;
- a control-plane variant paired with dependency ancestry fails the invariant;
- future `RequestType` variants require an exhaustive classification decision.

Queue tests:

- control-plane enqueue uses shared base and never dependency reserve;
- lane capacity rejects only additional control-plane requests;
- ordinary work can use shared queue capacity while the control-plane lane is
  full;
- shared-base fullness rejects control-plane work even when dependency reserve
  remains;
- queue depth decrements on dispatch, expiry, drain, and caller-drop removal;
- per-entry deadlines allow an ordinary request to expire before an older
  control-plane request;
- the expiration timer arms the earliest deadline across lanes;
- retained entries continue to expire while a selected request waits for its
  longer initial-permit deadline;
- control-plane entries never receive adaptive-delay rejection;
- control-plane hard expiry remains finite;
- oldest eligible selection preserves FIFO across control-plane and ordinary
  entries;
- ineligible older entries can still be skipped under existing rules.

Scheduler tests:

- control-plane requests stop at global and per-client shared base;
- dependency requests retain overflow and can dispatch while base is full;
- independent-action accounting does not include control-plane work;
- active, enqueue, dispatch, expiry, and rejection metrics use the bounded
  control-plane class;
- a closed response receiver is discarded before worker allocation;
- worker-channel failure and scheduler shutdown release all class accounting.

Configuration tests:

- valid enabled and disabled configurations;
- enabled-state telemetry reports both closed Boolean values;
- malformed, empty, non-Unicode, signed, zero, and overflowing values;
- zero `ANALYZE_CONCURRENCY` fails startup instead of stalling analysis;
- lane capacity below `ANALYZE_CONCURRENCY` or above shared queue capacity;
- control-plane deadline at or below the ordinary deadline;
- enabled control-plane lane with legacy queue policy;
- durations that cannot be represented by the runtime timer.

Production-shaped verification should use an ordinary push and existing
traffic, then confirm both local and remotely ingested metrics. It should not
rely only on successful HTTP status: queue age, retry, reserve, and CPU evidence
are necessary to show that the lane behaved as designed.

## Rejected alternatives

### Treat deployment work as a dependency

Analysis and evaluation do not unblock an isolate-holding ancestor. Granting
dependency status would let a module fan-out consume the finite queue and worker
slots that prevent callback deadlock. A longer deadline is not a reason to
weaken the ancestry contract.

### Give control-plane work unconditional priority

Analysis can contain many modules and substantial JavaScript evaluation. Strict
priority could delay runtime queries, mutations, and actions for the duration of
a push. The single FIFO lets older eligible work finish while preventing newer
ordinary arrivals from overtaking an admitted control-plane request.

### Add a separate control-plane queue

Separate queues require an arbitration policy, fairness weights, independent
capacity accounting, shutdown coordination, expiration timers, and proofs that
one queue cannot starve another. One bounded lane in the existing physical FIFO
provides the required deadline and shedding distinction with much less state.

### Add a dedicated worker pool

A second pool duplicates V8 threads, heaps, isolate lifecycle, client isolation,
context caches, worker metrics, and CPU allocation. It also reserves resources
even when no deployment is active. The initial problem is queue treatment, not
evidence that deployment needs a separate runtime.

### Reserve a worker immediately

A static reservation reduces ordinary throughput and can leave CPU capacity
idle. It does not reserve an active-JavaScript permit and therefore may only
move the bottleneck. Query containment plus a longer FIFO deadline should be
measured first. The document retains a bounded later design if starvation is
proved.

### Let control-plane work borrow dependency reserve

Borrowing makes reserve availability timing-dependent and can admit deployment
work just before a real callback arrives. Returning the slot later does not
repair the callback failure. Dependency reserve remains ancestry-only.

### Increase the hard deadline for every lane

Longer ordinary deadlines retain more stale requests and turn overload into
latency without increasing throughput. The different deadline is justified by
the bounded operator control-plane operation and its lane-local occupancy cap.

### Disable adaptive shedding globally during deployments

Global mode switches create races at deployment start and finish, change
runtime overload behavior for every request, and require cleanup after failed or
abandoned pushes. Classification on immutable request variants is local and
does not depend on deployment lifecycle toggles.

### Increase total queue depth

A larger queue does not distinguish deployment work, preserve dependency
reserve, or increase service rate. It increases retained request memory and can
make ordinary tail latency worse. The proposed lane is a sub-cap inside the
existing finite queue.

### Rely only on deployment retries

Retries are useful for short races but recreate scheduler arrivals and can
exhaust their bounded attempt count without any analysis progress. Keeping one
admitted attempt alive for a finite longer wait is more direct and observable.

### Match deployment HTTP paths

The isolate scheduler sees typed work after HTTP routing and should classify the
request it actually executes. Path matching misses internal callers and couples
scheduler policy to API routing. It also cannot distinguish later isolate work
inside a broad endpoint.

### Match module or component names

Application names are not part of the backend control-plane contract and would
make the patch deployment-specific. The Rust request variants already provide a
complete generic boundary.

### Classify every module evaluation as control plane

Normal UDF execution can load and evaluate user modules. Those evaluations are
runtime work and can occur on every cold request. Classifying from evaluation
timers or V8 operations would effectively exempt ordinary traffic from
shedding. Only the five explicit analysis and configuration request variants
belong in this lane.

### Solve the problem only with a global query cap

Query containment can leave useful shared-base capacity and should normally be
the first capacity control. It does not change the hard deadline or adaptive
shedding applied to an analysis request that still waits behind older work. The
two mechanisms are complementary.

### Add per-request cancellation wakeups in the initial patch

One listener and wake path per queued response sender would complicate receiver
ownership and cancellation safety. A pre-dispatch closed-sender check, finite
lane cap, and finite deadline bound abandoned queue state. More eager removal
should be considered only if caller-drop metrics show material waste.
