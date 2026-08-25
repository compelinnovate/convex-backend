# Isolate Delay Queue Design Reference

> Detailed implementation reference. The operator-facing adoption contract,
> configuration, metrics, rollout, and rollback remain in
> [`README.md`](README.md). The
> [deployment-lane reference](deployment_lane_design_reference.md) preserves
> the control-plane motivation, alternatives, and verification analysis.

`IsolateDelayQueue` replaces the isolate scheduler's generic CoDel queue when
`ISOLATE_QUEUE_DELAY_CONTROL_ENABLED=true`. The implementation adds lane-local
delay observations, adaptive shedding for retryable application work, finite
hard deadlines, and scheduler-aware oldest-eligible selection without creating
separate physical queues.

The word `lane` refers only to an `IsolateQueueLane` variant. Scheduler labels,
active-JavaScript classes, active-permit phases, application gates, and worker
limits classify or constrain the same request in different ways. Treating any
of those mechanisms as another queue lane obscures which resource is full and
which policy selected a request.

## Classification model

An isolate request carries several independent properties:

- `RequestType` identifies the operation executed by the isolate worker.
- `SchedulerDependencyClass` records whether completion releases an
  isolate-holding ancestor.
- `can_block_on_descendant` records whether the request can retain an isolate
  while waiting for separately scheduled work.
- `is_isolate_action` records action identity independently from ancestry.
- `ActiveJavascriptClass` selects service at the finite active-JavaScript gate.
- `client_id` selects global and per-client worker limits.

`RequestSchedulingProperties` derives the queue lane and scheduler metric label
from those properties. No caller-supplied module, function, component, route,
deployment, client, or tenant name selects a lane or active class.

### Queue lanes

`IsolateQueueLane` contains exactly four variants:

| Rust variant | Metric label | Classification | Queue and scheduler policy |
| --- | --- | --- | --- |
| `Dependency` | `dependency` | `unblocks_ancestor=true` | Can use dependency-only queue and worker overflow; bypasses adaptive shedding |
| `ControlPlane` | `control_plane` | One of the five typed analysis or configuration-evaluation requests while control-plane classification is enabled | Uses shared base, has a lane-local queue sub-cap and longer hard deadline, and bypasses adaptive shedding |
| `IndependentAction` | `independent_action` | `Action` or `HttpAction` without dependency ancestry | Uses shared base, obeys the independent-action worker cap, and participates in adaptive shedding |
| `Ordinary` | `ordinary` | Every request not classified above | Uses shared base and participates in adaptive shedding |

The mapping order is:

1. enabled typed control-plane request;
2. ancestor-unblocking dependency;
3. independent isolate action;
4. ordinary fallback.

A control-plane request with dependency ancestry is an invariant violation.
For every other request, dependency ownership takes precedence over action
identity. An ancestor-unblocking action therefore uses `Dependency`, while
`is_isolate_action` remains available for metrics and execution behavior.

The five typed control-plane request variants are:

- `Analyze`;
- `EvaluateSchema`;
- `EvaluateAuthConfig`;
- `EvaluateAppDefinitions`;
- `EvaluateComponentInitializer`.

Disabling control-plane classification maps those five variants to `Ordinary`.
`Udf`, `Action`, and `HttpAction` never become control-plane work.

### Scheduler labels

The scheduler metric label is not the queue lane. Apart from the explicit
`control_plane` label, `RequestSchedulingProperties::as_label` derives the
label from `unblocks_ancestor` and `can_block_on_descendant`:

| `unblocks_ancestor` | `can_block_on_descendant` | Scheduler label |
| --- | --- | --- |
| `false` | `false` | `independent` |
| `false` | `true` | `descendant_holder` |
| `true` | `false` | `dependency` |
| `true` | `true` | `dependency_descendant_holder` |

Action identity remains a separate `is_isolate_action` metric dimension. An
independent `Action` or `HttpAction` therefore normally has queue lane
`independent_action`, scheduler label `descendant_holder`, and
`is_isolate_action=true`. An ancestor-unblocking action has queue lane
`dependency` and can have scheduler label `dependency_descendant_holder`.

`can_block_on_descendant` refines scheduler accounting and liveness diagnosis.
`can_block_on_descendant` does not create a queue lane, queue capacity, hard
deadline, or delay controller.

### Active-JavaScript classes and phases

The active-JavaScript limiter controls isolates executing JavaScript at one
instant. `ActiveJavascriptClass` contains:

- `Dependency`, for work whose completion releases an isolate-holding
  ancestor;
- `Protected`, for ordinary application work, actions, analysis, evaluation,
  and every request without a narrower active class;
- `Degradable`, for an admitted independent root query cache-miss leader.

Backend-derived dependency ownership overrides `Protected` or `Degradable`.
Only the query cache assigns `Degradable`, after a cache recheck confirms that
the admitted request still owns the cache miss. A client declaration alone
does not assign an active class.

The usual mapping is:

| Queue lane | Active-JavaScript class |
| --- | --- |
| `Dependency` | `Dependency` |
| `Ordinary` admitted degradable root query | `Degradable` |
| Other `Ordinary` work | `Protected` |
| `IndependentAction` | `Protected` |
| `ControlPlane` | `Protected` |

Lane-local delay state is keyed only by queue lane. Protected and degradable
requests in `Ordinary` therefore contribute to the same ordinary delay
controller, overload state, and shedding policy. Conversely, protected
ordinary work, protected independent actions, and protected control-plane work
use separate queue-lane controllers even though all three use the same active
class.

Each active class has an `Initial` waiter queue and a `Resume` waiter queue.
The resulting six limiter queues are not isolate scheduler queues. A resume
retains the request's active class, precedes an initial start within that class,
and does not re-enter `IsolateDelayQueue`.

When active-class minimums are zero, active-class declarations collapse to the
phase-only compatibility policy. The scheduler then exposes only one effective
external initial-wait class. Direct internal dependencies and active-permit
reacquisitions retain the established resume-phase treatment.

## One physical queue

`IsolateDelayQueue` stores every external entry in one `VecDeque`. Each entry
contains:

- the request;
- its `IsolateQueueLane`;
- its enqueue timestamp;
- its absolute hard deadline.

Lane-local state contains depth and one `LaneDelayController` per lane. The
queue does not maintain a per-lane `VecDeque`, fixed lane priority, round-robin
lane selection, or a lane-specific worker pool.

### Queue capacity

Let:

- `Q = ISOLATE_QUEUE_SIZE`, the shared-base queue capacity;
- `R = ISOLATE_DEPENDENCY_WORKER_RESERVE`, the additional dependency-only
  queue capacity;
- `C = ISOLATE_CONTROL_PLANE_QUEUE_CAPACITY`, the control-plane sub-cap.

Ordinary, independent-action, and control-plane entries can enqueue only while
total occupancy is below `Q`. Dependency entries can enqueue up to `Q + R`.
Only a dependency can make `used_reserved_capacity=true`.

Control-plane admission additionally requires control-plane depth below `C`.
`C` is inside `Q`: `C` neither reserves entries from other lanes nor increases
physical capacity. Ordinary and independent-action lanes have no lane-local
queue cap. The independent-action limit applies later to assigned-worker
eligibility.

### Oldest-eligible selection

Every receive first removes one hard-expired entry when one exists. Otherwise,
the receiver scans the complete `VecDeque` from oldest to newest and selects
the first entry with no scheduler ineligibility reason.

`IsolateQueueEligibility` represents blocking reasons as Boolean fields:

- `physical_total`;
- `shared_base`;
- `per_client_total`;
- `per_client_base`;
- `independent_action_cap`;
- `active_javascript_class_pending`.

An all-false value is eligible. One request can report more than one blocking
reason. The scan records every blocked lane/reason combination for metrics even
after finding the oldest eligible request.

Selection therefore preserves FIFO among requests eligible in the same worker
snapshot. An older blocked request does not hide a younger request that can use
available capacity. Dependency entries do not jump older eligible shared-base
work while shared-base capacity remains available; dependency entries become
the only externally eligible work after shared base is full.

Worker completion is processed before another selection attempt. Completed
worker accounting therefore contributes to the next immutable selection
snapshot.

### Active-permit exposure and worker reservations

Queue selection precedes initial active-JavaScript permit acquisition. Removing
many entries and hiding them in permit futures would defeat queue delay,
deadline, and class-demand accounting. The scheduler therefore exposes at most
one external initial active-permit waiter per effective active class.

While one external waiter is exposed, another queued request in the same
effective class receives `active_javascript_class_pending=true` and remains in
`IsolateDelayQueue`. With active-class admission enabled, dependency, protected,
and degradable external waits are tracked separately. Compatibility mode maps
all external requests to one protected pending-class slot.

The pending-class slot is shared across queue lanes. One exposed protected
waiter can keep protected `Ordinary`, `IndependentAction`, and `ControlPlane`
entries queued, while one degradable `Ordinary` entry and one external
`Dependency` entry can expose their own waits. An older request blocked by its
pending active class can be skipped for a younger request in another active
class. FIFO remains intact among requests eligible in the same snapshot.

Each exposed waiter reserves the global and per-client worker eligibility that
allowed selection. The reservation prevents concurrent active classes from
passing the same worker check and collectively exceeding physical, shared-base,
per-client, or independent-action limits after their active permits arrive.
The scheduler releases the reservation immediately before worker assignment or
terminal rejection.

Completed permit waits are processed before fresh ingress. A permit already
granted to a request cannot remain hidden behind a stream of newly ready queue
receives.

## Per-lane delay control

Each lane owns:

- an observation-interval deadline;
- the minimum selected-request sojourn observed in the interval;
- the number of observations in the interval;
- an overloaded Boolean.

Only selected, non-expired requests are observations. The controller evaluates
a completed interval before the selected request becomes the first observation
of the next interval.

A completed interval enters overload when it contains at least two observations
and its minimum sojourn is greater than
`ISOLATE_QUEUE_DELAY_TARGET_MILLIS`. A measured interval clears overload when
it contains at least one observation and its minimum is at or below the target.
An interval with no observations preserves the previous state. Draining a lane
clears its controller and partial interval state.

Overload state does not change scheduler eligibility. After normal selection,
an `Ordinary` or `IndependentAction` request is adaptively rejected only when:

- its lane is overloaded; and
- that selected request's own sojourn is greater than
  `ISOLATE_QUEUE_DELAY_SHED_THRESHOLD_MILLIS`.

An older blocked peer cannot cause a younger eligible request to be shed.
`Dependency` and `ControlPlane` collect delay observations and can report
overload, but neither lane receives `DelayControlShed`.

## Deadlines, cancellation, and closure

Ordinary, independent-action, and dependency entries receive
`ISOLATE_QUEUE_HARD_MAX_AGE_MILLIS`. Control-plane entries receive
`ISOLATE_CONTROL_PLANE_HARD_MAX_AGE_MILLIS`. The absolute deadline stored at
enqueue continues through initial active-permit acquisition. Worker execution
and later active-permit reacquisition have separate time limits.

The queue finds the earliest deadline across all entries rather than relying on
FIFO order. A newer ordinary request can therefore expire before an older
control-plane request. A non-consuming expiry receiver remains active while a
selected request waits for its initial permit, so retained entries continue to
expire without another enqueue or worker completion.

Canceling a pending receive does not remove an entry. Response-channel closure
is checked after selection and while the selected request waits for its initial
permit. Caller cancellation while an entry remains queued is lazy: selection
or hard expiry eventually removes the entry, while finite queue capacity and
deadlines bound retained state.

Dropping the last sender lets already queued entries dispatch or expire before
the consuming receiver ends. Dropping the last consuming receiver closes
admission and drains retained entries. Queue-owned request resources and timer
futures are dropped after releasing the queue mutex because their destructors
can execute arbitrary code.

## Requests that bypass `IsolateDelayQueue`

Direct separately scheduled nested UDF callbacks use the scheduler's internal
dependency channel. The internal channel has no external queue deadline because
the nested call cannot be retried safely. The scheduler selects the oldest
internal request eligible in the worker snapshot, exposes one internal
dependency active-permit waiter, and reserves that request's worker eligibility
while the permit is pending. Internal ingress is considered before fresh
external queue receipt. Internal work uses the same physical worker total and
dependency reserve as external work.

An isolate that releases its active permit around a supported asynchronous wait
retains its assigned worker. Reacquisition uses the request's original active
class and `Resume` phase without entering the external queue again.

These paths explain why active-JavaScript waiter counts can exceed queue
dispatches and why a dependency active-permit waiter does not necessarily have
a corresponding `lane="dependency"` queue entry.

## Interaction with other patches

### Dependency capacity

[`dependency_capacity`](../dependency_capacity/README.md) supplies
`SchedulerDependencyClass`, global and per-client shared-base arithmetic,
dependency-only application overflow, queue overflow, worker overflow, and the
independent-action worker cap.

`IsolateDelayQueue` consumes that classification; `IsolateDelayQueue` does not
infer dependency role from request type. Query-cache coalescing, function-runner
calls, and trusted callback paths preserve ancestry before the request reaches
the queue. Losing ancestry can leave an ancestor-unblocking request in ordinary
capacity and recreate the capacity inversion that the reserve prevents.

The queue changes overload handling without changing `T`, `B`, `R`, per-client
limits, or dependency ownership. A dependency can use queue or worker overflow
and still wait for a finite active-JavaScript permit or CPU because the reserve
does not add active execution capacity.

### Degradable reactive queries

[`degradable_reactive_queries`](../degradable_reactive_queries/README.md) adds an
application-scoped lifetime gate for admitted degradable root query cache-miss
leaders. The gate acts before `IsolateDelayQueue`:

1. the cache recheck identifies an actual cache-miss leader;
2. immediate degradable admission grants or defers that leader;
3. an admitted leader creates an isolate request with
   `ActiveJavascriptClass::Degradable`;
4. the request enters the `Ordinary` queue lane;
5. queue selection exposes at most one degradable initial active-permit waiter;
6. JavaScript suspension and resumption retain the degradable active class.

Cache hits and coalesced followers do not create another degradable isolate
execution. A degradable declaration that does not own an admitted cache miss
does not reach the isolate scheduler as `Degradable`.

The leader permit and active-JavaScript permit control different lifetimes. The
leader permit remains held across database and supported asynchronous waits;
the active permit is released while JavaScript is not executing. Class-aware
active admission lets the leader cap exceed active-JavaScript capacity while a
degradable service floor preserves execution progress.

Degradable classification does not exempt the root from ordinary queue policy.
The root can encounter queue-full rejection, ordinary hard expiry, adaptive
delay shedding, shared-base worker limits, or caller cancellation. A separately
scheduled descendant receives queue lane `Dependency` and active class
`Dependency`, even when the root is degradable.

The complete active-class grant policy is documented in
[`active_javascript_admission.md`](../degradable_reactive_queries/active_javascript_admission.md).
The queue-specific interaction is the one-external-waiter-per-effective-class
rule and the worker reservation held during each exposed permit wait.

### Deployment-analysis pacing

[`deployment_analysis_pacing`](../deployment_analysis_pacing/README.md) lets
each isolate module analysis attempt fairly borrow from the same
application-scoped capacity used by degradable leaders. The combined upper
level gate is:

```text
degradable query cache-miss leaders + isolate module analysis attempts
```

Pacing acts before queue admission and does not create a queue lane, worker
reservation, or active-JavaScript permit. The analysis response sender retains
the pacing permit through queueing and execution, releasing the permit on a
terminal result or pre-dispatch discard.

With control-plane classification enabled, analysis enters `ControlPlane` and
uses active class `Protected`. With control-plane classification disabled,
analysis enters `Ordinary`, receives the ordinary deadline and shedding policy,
and still uses active class `Protected`. Evaluation requests do not borrow the
analysis pacing permit.

Pacing reduces additive analysis arrivals above degradable demand. Pacing does
not guarantee queue admission, active-permit acquisition, worker assignment,
CPU time, or deployment completion.

### Shared-base HTTP admission

[`shared_base_http_admission`](../shared_base_http_admission/README.md) protects
the outer HTTP service and authenticated callback headroom before application
and isolate scheduling. HTTP admission capacity, waiter order, and timeout are
independent from isolate queue capacity and deadlines.

An HTTP request can pass the outer gate and still wait at an application gate,
`IsolateDelayQueue`, the active-JavaScript limiter, worker assignment, or the
database. Conversely, isolate dependency reserve cannot admit a callback that
never passes the outer HTTP gate. Trusted callback metadata preserves
dependency ancestry across those stages; the queue consumes the resulting
backend-owned dependency class.

### Scheduled-action admission

[`scheduled_action_admission`](../scheduled_action_admission/README.md) delays a
scheduled or cron action's durable `Pending -> InProgress` claim until normal
runtime admission has selected an execution slot. A scheduled V8 action enters
the isolate scheduler as `IndependentAction` with active class `Protected`.
Backend-derived ancestry instead makes the request a `Dependency`. A scheduled
Node action uses Node executor admission and does not enter `IsolateDelayQueue`;
its later isolate callbacks follow ordinary callback and dependency rules.

For a scheduled V8 action, queue-full rejection, hard expiry, adaptive
shedding, scheduler closure, or active-permit timeout before the execution
barrier leaves the durable action available for retry. After successful
admission, the selected worker is held briefly while the database claim
commits. The claim barrier does not create a queue lane or reserve idle worker
capacity.

### Context reuse and memory controls

[`context_reuse`](../context_reuse/README.md)
acts after request classification and admission. Reuse can reduce module loading,
V8 context initialization, service time, and resulting queue sojourn. Reuse can
also change retained memory per worker. Reuse does not change queue lane,
active class, queue capacity, deadline, shedding, or worker eligibility.

[`context_reuse`](../context_reuse/README.md) owns
context-cache sizing and eviction. The cache also consumes the pressure signal
owned by
[`backend_memory_resilience`](../backend_memory_resilience/README.md).
An eviction can reduce later reuse and increase queue sojourn through higher
service cost; an eviction does not create a queue class.
[`context_reuse`](../context_reuse/README.md), queue
metrics, active-permit wait, worker occupancy, CPU, and memory pressure provide
separate evidence for those mechanisms.

## End-to-end request sequence

An external request can encounter the following stages:

1. outer HTTP admission, when the request arrives through HTTP;
2. application function admission and query-cache coalescing;
3. degradable leader admission or deployment-analysis pacing when applicable;
4. bounded isolate queue admission;
5. hard-expiry enforcement and oldest-eligible selection under worker and
   per-client limits;
6. adaptive shedding for a selected retryable lane;
7. one exposed initial active-JavaScript permit wait for the effective class;
8. worker assignment using the eligibility reservation;
9. JavaScript execution, including release and resume of active permits;
10. completion and release of worker, application, leader, or pacing ownership.

No single metric describes all ten stages. Queue lane metrics describe stages
4 through 7. Scheduler-class and reserve metrics describe dependency and worker
eligibility. Active-class and phase metrics describe active-JavaScript service.
Application, degradable, analysis, HTTP, database, CPU, and memory metrics
describe their corresponding stages.

## Invariants

The implementation preserves these invariants:

- one physical external queue serves all four lanes;
- only `Dependency` can use queue or worker overflow;
- `ControlPlane` never carries dependency ancestry;
- only `Ordinary` and `IndependentAction` receive adaptive shedding;
- every external hard deadline continues through initial active-permit wait;
- one external initial active-permit waiter is exposed per effective active
  class;
- every exposed permit waiter reserves its worker eligibility;
- resumptions and direct internal callbacks do not re-enter the external queue;
- control-plane capacity is a sub-cap, not reserved capacity;
- active-class minimums add service policy, not active permits;
- deployment pacing and degradable admission constrain arrivals before the
  queue and do not create queue capacity.

Violating one invariant changes liveness, overload, or capacity semantics even
when aggregate throughput appears unchanged. Tests and metrics therefore retain
the exact lane, scheduler class, active class, phase, rejection reason, and
reserve-use distinctions.
