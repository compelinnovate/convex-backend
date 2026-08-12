# Scheduled Node Action Admission Across Generation Cutover

Status: the generation-level start fence is implemented with the existing
linear active-request guard and no new durable state. The current package
preparation endpoint does not retain its cache lease across the durable claim,
so the stronger expiring-authority guarantee described below remains open.

## Patch ownership

This work belongs in the coordinated local Node executor pools patch rather
than in a separate adoption unit. The pool patch introduces the resident
generation fingerprint, topology publication ordering, candidate promotion,
old-generation admission closure, and stale-request rejection that create the
remaining race. It also owns the generation guard needed to close that race.

The existing
[`scheduled_action_admission`](../scheduled_action_admission/README.md) patch
supplies the scheduler-side durable-claim controller and the general two-way
start barrier. This patch carries that barrier through Node routing and
generation admission so its `ready` signal has the same concrete meaning for
Node that it already has for an assigned V8 worker.

During development, a focused commit can keep the change reviewable. The final
patch train should fold that commit into this patch so an adopter cannot apply
coordinated Node cutover while omitting its required scheduled-action
composition. No new stored job state, rollout step, or independent activation
unit is required.

## Problem

Before this completion, the scheduled-action barrier signaled `ready` after
the application built an `ExecuteRequest`, but immediately before
`NodeActions::execute`. The scheduler then committed the exact job's monotonic
`Pending -> InProgress` claim and released the request.

After release, Node execution still had to:

1. serialize the executor request;
2. reconcile the request's topology snapshot with router publication;
3. select the exact logical pool incarnation;
4. reconcile the source-package and environment fingerprint;
5. wait for or create a compatible local generation;
6. acquire that generation's active-request guard; and
7. send the request to the Node child.

A deployment can publish or promote a newer generation between the early
`ready` signal and those steps. The router correctly rejects a request whose
older source-package and environment fingerprint is no longer resident, but
that rejection then occurs after the durable claim. The scheduler cannot move
the job back to `Pending`, because it cannot generally infer from a
post-claim failure that user code did not begin. A later scheduler pass
therefore applies the conservative at-most-once failure contract.

The two mechanisms are individually correct:

- the scheduler does not retry work that may have produced external effects;
- the router does not run a request in a generation that disagrees with its
  validated snapshot.

Their prior composition left an ordinary deployment race on the post-claim
side of the barrier.

## Required guarantee

For a scheduled or registered-cron Node action, a successful `ready` signal
must mean all of the following:

- request serialization and other deterministic application-side preparation
  have succeeded;
- the request agrees with the committed router topology and exact logical pool
  incarnation;
- a generation with the compatible source-package and effective-environment
  identity is resident and admitted;
- normal candidate promotion and topology drain cannot invalidate that
  admission;
- no application module has been imported or invoked for this request.

After `ready`, the scheduler commits the exact durable claim. Only the matching
prepared invocation may consume the subsequent `start` signal. It must use the
same generation ownership that justified `ready`; it must not repeat selection
or reacquire an equivalent-looking generation.

This is a narrower guarantee than exactly-once execution or universal
failure-free deployment. Backend process loss, an ambiguous claim result,
hard generation retirement, forced cutover, or transport loss after a visible
claim can still leave an `InProgress` job whose execution outcome is unknown.
Those cases retain conservative recovery.

Package preparation currently proves publication only at the preparation
response. It does not retain package-cache ownership through the claim. The
remaining limitation is described under **Package authority is not yet
retained** and must not be presented as part of the implemented guarantee.

## Non-goals

This completion does not add:

- an `InProgress -> Pending` transition;
- a durable `Preparing` state;
- a retry based on a Node error string;
- a global deployment or scheduler lock;
- a permanent scheduled-action pool or quota;
- retention or reconstruction of arbitrary historical generations;
- a change to repeatable-snapshot function semantics;
- a cross-process parked-request protocol in the Node child; or
- a promise that forced cutover, host pressure, shutdown, or process failure
  cannot interrupt an action.

Avoiding generation rotation for a push whose complete Node-relevant package
and environment are unchanged can reduce cutover work, but it does not close a
real source, environment, or topology change race and is not part of this
correctness fix.

## Design

### Extend the existing start barrier

Keep the scheduler-side controller and exact claim flow unchanged. Pass the
optional runtime-side start gate through `NodeActions::execute` and the
`NodeExecutor` invocation boundary. Ordinary actions, analysis, dependency
builds, and HTTP actions do not create the gate and retain their current
behavior.

For a local execute request, the runtime performs this sequence while the job
is still `Pending`:

```text
build ExecuteRequest under its repeatable database snapshot
acquire the application Node-action permit
serialize the executor request
validate topology and module-to-pool selection
select the exact logical pool incarnation
reconcile or join the exact cutover target
acquire a compatible generation under its generation-state mutex
retain a linear generation admission guard
prepare the package in the selected child
signal ready and wait for start
```

The scheduler then performs:

```text
open a fresh short transaction
verify the complete expected Pending job
commit Pending -> InProgress with its request and execution IDs
send start
```

After `start`, the prepared invocation:

```text
starts the Node request deadline
sends /invoke through the already selected child
holds the same generation guard through the complete response
```

No router lock, generation-state mutex, startup lock, or durable-claim
transaction is held while waiting for the claim. The function runner's
repeatable-snapshot transaction remains part of the prepared action state, as
it does for the V8 barrier; the scheduler opens the fresh short claim
transaction only after `ready`.

### Use a linear generation admission guard

`LocalNodeExecutor` already increments a generation's active-request count
under the same state mutex used by promotion to close old admission. That is
the correct synchronization primitive. The prepared invocation should retain
that ownership from `ready` until cancellation or response completion.

The guard or prepared-invocation handle must not be cloneable. Dropping it
before `start` must:

- release the generation admission;
- release application capacity through the existing future stack;
- wake a normal old-generation drain if this was its last owner; and
- record a bounded pre-start outcome rather than an invocation success or user
  failure.

An advisory fingerprint check is insufficient. A separate `reserve` method is
also insufficient if `execute` later selects or acquires the generation again.
The resource that justifies `ready` must be the resource used to send the
request.

### Linearization with deployment promotion

Let:

- `A` be generation admission, including the active increment under the
  generation-state mutex; and
- `P` be candidate promotion, including closing old admission under that same
  mutex.

Only two orderings are possible:

| Ordering | Required result |
| --- | --- |
| `A < P` | The action owns the old generation. Promotion may install the candidate, but old-generation drain waits for the guard. The exact claim can commit and the action can run against its prepared snapshot. |
| `P < A` | Old admission is closed. The stale request cannot acquire a compatible guard, so it ends before `ready`; its durable job remains `Pending` and a later attempt rebuilds from a fresh snapshot. |

This ordering also permits admission after topology publication but before
candidate promotion when the older request still matches the sole admitted
resident generation. Once admitted, that request is assigned old-generation
work even if its user code begins after candidate promotion. This matches the
existing hot-drain contract for work assigned before old admission closes.
The deployment caller waits for old drain before returning.

### Preserve the complete identity fence

Generation admission must retain the existing layered checks. Compatibility
is not only a source-package comparison. It includes:

- complete committed pool topology or the permitted older-snapshot relation;
- the module's exact named or default-pool selection;
- logical pool incarnation, including its introduction version;
- source-package identity;
- the canonical effective-environment hash; and
- cutover publication and target ordering.

A request must not join a candidate for a different source or environment
target at the same version. A stale request must not recreate a removed pool or
start a detached generation after that pool incarnation closes. If no exact
compatible generation remains admitted, the request ends before `ready` and
the scheduler obtains a fresh snapshot on retry.

### Observe exceptional generation loss

The active-request guard makes normal promotion and graceful retirement wait;
it does not make the child immortal. Backend shutdown, health retirement,
cgroup pressure, forced cutover, or child failure can terminate a generation
with active guards.

While the scheduler claim is in flight, the prepared Node invocation must race
the start gate against a generation-unavailable signal. Immediate retirement
must publish that signal before it kills or detaches the child. A normal
deployment drain must not publish it, because the already admitted guard
remains executable there.

If exceptional loss wins before a definitely visible claim, the prepared
future fails without consuming `start`; the pending scheduler retry contract
applies. If the commit may already be visible, the scheduler retains its
existing conservative ambiguity handling. A returned error or timeout is not
proof that a submitted database commit stayed invisible.

The current child health signal cannot prove future execution. A child can
still die immediately after any check. The purpose of the unavailable signal
is narrower: never release a reservation that the runtime already knows it has
lost.

### Package authority is not yet retained

Package URLs are signed before Node routing. An already expired descriptor is a
pre-admission failure, and `/prepare` moves the ordinary download, checksum,
and publication work before `ready`.

The existing endpoint acquires and releases a source-package lease before it
returns success. As documented by
[`atomic_node_executor_source_packages`](../atomic_node_executor_source_packages/README.md),
the package is then a zero-owner cache entry. Count or byte pressure can retire
it before `/invoke`, which will try to acquire the package again and may need
the same signed URL after the durable claim. In particular, if every older
cache entry is active while a new preparation releases its lease, bounded
cache enforcement can retire the newly prepared entry immediately.

The current implementation therefore does not remove expiring package
authority from an unbounded post-claim path. Closing that gap would require a
real lease handoff between preparation and invocation, or another bounded
authority contract. That is a separate child-protocol decision; this document
must not imply that the process-local generation guard supplies package-cache
ownership.

### Preserve timeout boundaries

The Node process request deadline and user-facing execution timeout begin only
after `start`. Claim latency must not consume user execution time or make a
freshly released request immediately time out.

Application end-to-end action time and durable-admission time may include the
claim, as they do for the V8 barrier. Node executor service-time and transport
metrics should preserve their post-release meaning. If the existing active
guard is reused directly, its diagnostics and request-start metrics need an
explicit reserved phase or a bounded `canceled_before_start` outcome so a
canceled claim is not reported as a submitted Node invocation.

## Cancellation and failure behavior

| Point | Durable state | Generation ownership | Result |
| --- | --- | --- | --- |
| Serialization, topology, pool, cutover, fingerprint, startup, or authority failure before `ready` | `Pending` | Never acquired or dropped | Existing scheduler retry from a fresh snapshot |
| Controller or scheduler task drops before `ready` | `Pending` | Dropped | No child request; later scheduler attempt may proceed |
| Exact job changes while admission waits | Changed by the winning operation | Dropped | No stale action starts |
| Generation becomes exceptionally unavailable while the claim is in flight | `Pending` if the claim is definitely invisible; otherwise possibly `InProgress` | Dropped without `start` | Pending retry or conservative ambiguous recovery |
| Claim returns `None` after exact-state comparison | Changed by the winning operation | Dropped | No child request |
| Claim definitely fails before commit submission | `Pending` | Dropped | Existing scheduler retry |
| Claim result is ambiguous | Possibly `InProgress` | Never released | Conservative recovery; no inferred retry |
| Claim commits and `start` delivery fails | `InProgress` | Lost or closing | Existing generic transient failure |
| Normal deployment promotes after admission | `InProgress` after claim | Old generation remains guarded | Candidate serves new work; old request executes and drains |
| Hard pressure, shutdown, or force interrupts after claim | `InProgress` | Terminated | Existing conservative action failure |
| Completion transaction temporarily fails | `InProgress` | Invocation already completed | Existing completion retry loop |

Cancellation after a visible claim retains the current scheduled-action
contract. A cancellation operation may change the durable job before the
controller releases `start`, while the already claimed action can still run.
Completion must continue to compare the exact claimed job and must not
overwrite a winning cancellation.

## Liveness and progress

### No structural deadlock

The design introduces no lock cycle when implemented with drop-based ownership:

1. request admission briefly takes the generation-state mutex;
2. it increments active ownership and releases the mutex;
3. the scheduler commits the job claim in a fresh transaction; and
4. deployment drain may wait for that ownership to leave.

The claim does not wait for deployment cleanup, and deployment runtime cleanup
does not hold the database transaction that published it. The wait relation is
therefore a one-way delay from deployment drain to the admitted action, not a
deadlock.

Holding a router lock, generation mutex, startup lock, or deployment surge
coordinator lock across the database claim would invalidate this argument and
must be rejected.

### Claim stalls can delay deployment completion

An admitted action owns its generation while the short exact-state claim
commits. A database stall can therefore delay old-generation drain and the
deployment response. This is head-of-line blocking, not a logical cycle. The
V8 admission barrier already holds selected runtime capacity across the same
claim; Node additionally couples that delay to generation drain.

Do not add a blind claim timeout that treats cancellation as proof of an
invisible commit. A useful timeout contract must distinguish:

- definitely not committed;
- committed; and
- result unknown.

The unknown result must retain conservative recovery. Initially, keep the
claim to one fresh transaction, instrument its latency, and use existing
database cancellation semantics. Add a more elaborate lease only if observed
claim stalls justify its state and failure protocol.

### Long actions can hold deployment surge capacity

If admission wins before promotion, the action is legitimate old-generation
work. Candidate promotion still makes new-generation service available, but
the old generation and global surge lease remain until its admitted actions
finish or an exceptional policy terminates them. `finish_push` and a later
cutover can therefore wait for the full action duration.

This is the existing hot-drain policy, not downtime. The completion expands
the set of requests correctly classified as admitted instead of failing them,
so it can make the cost more visible. Application action concurrency and the
existing execution deadline bound ordinary ownership. Forced cutover remains
an explicit operator decision that can interrupt external effects.

### Repeated deployments can delay a pending job

A request can repeatedly lose admission if every fresh attempt overlaps a
newer topology publication. Ordinary scheduled jobs and registered crons
already apply backoff to pre-admission system failures, so repeated stale
snapshots do not create a hot retry loop.

The design is not wait-free under infinite deployment churn. It guarantees
eventual progress after topology churn stops, provided the database, executor,
and scheduler remain healthy. A special fairness scheduler or deployment
quiescence protocol is not justified without evidence of persistent
starvation.

### Concurrent preparations remain safe

The durable job stays `Pending` during admission, so more than one scheduler
owner can theoretically prepare it. Only the exact fresh claim can win.
Losing owners drop their generation guards without sending child requests.
This may consume bounded transient capacity but cannot duplicate user code.

## Observability

The local generation guard records bounded pre-start completion outcomes for
controller cancellation, package expiry or preparation failure, and known
generation loss. Existing router and durable-admission metrics continue to
classify topology, incarnation, and fingerprint failures.

Do not use module paths, source-package IDs, environment names or values,
fingerprints, request IDs, job IDs, tenants, or function arguments as metric
labels. Do not copy raw executor or provider payloads into logs.

Control flow must depend on the barrier phase and typed ownership result, not
the literal stale-fingerprint error string. The string may remain useful as a
sanitized invariant error, but deployment races are expected pre-admission
outcomes once this boundary is complete.

The Node execution deadline and service timer start after release. The durable
claim histogram continues to measure the only interval added to generation
ownership by this completion.

## Compatibility and rollout

The completion changes no durable job schema, request ID, execution ID, source
package format, topology format, pool declaration, or Node JSON request body.
It is process-local and activates automatically when the scheduler,
application runner, and local Node executor use the completed backend image.

The neutral start-gate type lives in `common` and is re-exported by the function
runner for compatibility. The Node invocation interface carries that gate
without serializing it. Node executor code does not depend on scheduler job
semantics; it owns only `ready`, `start`, cancellation, and generation
ownership.

The no-op executor must fail before `ready`. A future remote Node executor must
not report local preparation as remote admission. It would need an explicit
remote prepare/continue acknowledgment protocol or must reject the optional
gate. Mixed versions remain only partially protected while an older scheduler
can still claim before the completed Node boundary.

## Regressions to prevent

- **Advisory readiness:** signaling after a fingerprint check without owning
  the exact generation leaves the original race intact.
- **Reacquisition after claim:** dropping the prepared guard and selecting
  again after `start` turns readiness into a stale hint.
- **Lock retention:** holding executor locks across the database claim can
  block promotion, shutdown, and cancellation and can create a real wait
  cycle.
- **Deadline drift:** starting the Node timeout before the claim charges
  database delay to user execution.
- **Dead reservation release:** ignoring shutdown or hard retirement can
  commit a job against a child already known to be unavailable.
- **Expired package authority:** a preparation result without retained package
  ownership can still leave a deterministic post-claim download failure.
- **Old-incarnation resurrection:** a removed and reintroduced pool name must
  not make a prior request eligible for the new slot.
- **Stale-target joining:** source or environment disagreement at one topology
  version must fail instead of joining a convenient candidate.
- **Metric reclassification:** a claim cancellation must not be counted as a
  child invocation or user-code failure.
- **Error-string retry:** scheduler behavior must not parse the stale resident
  fingerprint message.

## Rejected alternatives

### Retry or reset `InProgress`

Moving a claimed job back to `Pending`, or retrying because one runtime path
reports that it did not start, weakens the durable at-most-once contract. A
process can fail after user code starts but before publishing that fact. A
safe durable retry protocol would need a new attempt epoch, exact completion
fencing, cancellation semantics, garbage collection, mixed-version behavior,
and a proof that no executor for the old epoch can still run. Moving ordinary
admission before the existing monotonic claim is smaller and safer.

### Accept or reconstruct stale generations

Keeping enough historical generations to run every prepared snapshot has
unbounded memory and lifecycle cost, conflicts with the one-surge-slot host
budget, complicates removed-pool identity, and can execute obsolete code long
after deployment. Rewriting an old request to use the current package breaks
the repeatable snapshot that supplied validation, environment, routing, and
source identity.

### Pause the scheduler during deployment

A global quiescence protocol delays unrelated work, needs coordination across
backend owners, and still must resolve preparations already in flight. It
turns a local ordering bug into application-wide deployment downtime.

### Hold the function transaction through cutover

The preparation transaction is a repeatable snapshot, not a generation lease.
Holding it longer increases snapshot age and OCC exposure without preventing a
router publication or candidate promotion after the durable job claim.

### Add a child-side parked-request protocol now

A child protocol could serialize and acknowledge a request without importing
or invoking its application module, then start it after the durable claim. It
would require bounded parked capacity, reservation IDs, cancellation,
expiration, child-restart semantics, authentication, cleanup, and
mixed-version negotiation. It would still not remove backend failure after the
claim and before `start`. The local generation guard closes the observed
deployment race without that protocol. Add it only if measured transport or
child-start ambiguity justifies the additional state machine.

### Avoid rotations for apparently unrelated pushes

A content-addressed Node import-closure identity could reuse generations when
the complete executable package and environment are unchanged. That is a
useful optimization but requires a precise package-identity contract. It does
not protect real Node source, environment, or topology changes and cannot
replace the admission fix.

## Implementation

The optional execution start gate is carried through `NodeActions::execute`,
the `NodeExecutor` invocation boundary, routed pool selection, and exact local
generation acquisition without entering the serialized Node request. The
local executor prepares the package in the selected child, then reports
`ready` while retaining that generation's active-request guard. It races the
claim wait against controller cancellation, backend shutdown, and a distinct
hard-unavailability signal. Graceful promotion does not publish that signal.
Package preparation does not retain the child package-cache lease.

After `start`, the same invocation begins its Node request deadline, sends
`/invoke` through the already selected child, and retains the guard through the
response. A start-observation handshake begins the Node executor service timer
after release, and the gate waits for its diagnostic acknowledgment before
allowing `/invoke`. Durable-claim latency is therefore not charged to Node
service time, and the timer cannot start after request submission. Pre-start
exits use bounded outcomes for cancellation, package expiry or preparation
failure, and generation loss.

The implementation keeps this behavior inside the existing action future. It
does not add a separate externally visible reservation registry or detached
task.

## Verification

Current focused tests cover the barrier cancellation contract, exact-generation
guard retention after `ready`, graceful drain waiting for that guard, hard
unavailability canceling it, preparation and expiry failure before `ready`, and
no-op rejection before `ready`.

The following material integration coverage is still missing:

- a prepared older request when promotion wins before generation acquisition:
  no durable claim and no child invocation;
- generation acquisition winning before promotion: candidate promotion,
  old-generation execution, and drain complete without a stale rejection;
- exact job cancellation or replacement after `ready`, releasing the guard
  without sending `/invoke`;
- an ambiguous claim result never releasing `start` and never being treated as
  safe to retry;
- pool removal and same-name reintroduction while admission waits;
- a same-version cutover target disagreement;
- both ordinary scheduled actions and registered-cron actions; and
- an instrumented child observing no `/invoke` before the exact durable claim.

Package-cache pressure also needs a regression proving the open lease gap, or a
future lease-handoff fix, before the stronger package-authority guarantee can
be accepted.

## Acceptance statement

The implemented generation fence prevents an ordinary coordinated cutover
from turning a definitively pre-execution stale Node request into an abandoned
durable action claim. Either the action owns an exact generation before its
claim and normal drain honors that ownership, or generation cutover wins and
the action remains pending for a fresh retry. This statement does not close the
package-cache lease gap described above.

The completion deliberately does not claim that every visible `InProgress`
action will execute or return a result. That stronger claim is impossible
across process, database, and transport failure without a larger durable and
cross-process protocol.
