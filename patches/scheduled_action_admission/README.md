# Admit Scheduled Actions Before Their Durable Claim

## Summary

This patch prevents runtime admission failures from consuming the at-most-once execution of an
ordinary scheduled action or registered cron action. It prepares and admits the action while its
durable job remains `Pending`, commits the exact job's monotonic `Pending -> InProgress` transition
only after the execution reaches its environment-specific admission boundary, and then releases
the prepared execution to user code. Executable paths own the capacity described below;
deterministic validation failures reach a no-runtime boundary because they need no worker.

The coordination is a two-way, process-local start barrier. For V8 actions the barrier is reached
after the application action permit, isolate queue admission, active-JavaScript permit, and an
eligible isolate worker are all owned. For Node actions it is reached after application admission
and application-side request preparation, immediately before calling `NodeActions::execute`.
Failure before `ready` drops the preparation while the job is still pending. A changed job or
failed claim also drops the prepared execution without starting user code; existing retry behavior
applies when the claim is definitely not visible, while an ambiguous visible claim retains
conservative recovery.

The patch does not change at-most-once recovery after `InProgress` is visible. A process can still
fail after the claim commit and before or after the barrier release, and the scheduler still cannot
prove whether external effects occurred. Such a job remains `InProgress` and is conservatively
completed as the existing generic transient action failure.

This is a distinct adoption unit downstream of dependency capacity (`8e8ad8b8a`) and isolate queue
control (`8dc75417e`). It uses their application, queue, active-permit, worker, and cancellation
boundaries but does not change their capacities or policy. In particular, scheduled roots remain
`SchedulerDependencyClass::Independent`; they do not borrow ancestor-unblocking overflow.

## Background

### Durable action lifecycle

Convex can retry a scheduled mutation because the mutation transaction is atomic. Actions are
different: user code can perform external effects that cannot be rolled back or identified by the
database commit protocol. Scheduled and cron actions therefore use a conservative at-most-once
lifecycle.

For an ordinary scheduled action the relevant states are:

```text
Pending -> InProgress -> Success
                      -> Failed
```

Registered cron actions use the same `Pending -> InProgress` claim and then record the run result
and advance the cron schedule. Both executors attach a request ID and execution ID to the
`InProgress` state. If a later scheduler pass finds the job there, it cannot distinguish these
histories:

- the action never reached user code;
- user code started but produced no external effect;
- user code completed external effects and the backend failed before recording completion; or
- completion recording itself failed.

The safe common treatment is to avoid another execution. The existing scheduler marks the run
failed with `Transient error while executing action` and restores the stored request and execution
IDs for logging. A zero-runtime generic failure is therefore evidence of an abandoned
`InProgress` claim, not evidence that JavaScript necessarily began.

This monotonic state machine is implemented in
[`scheduled_jobs/mod.rs`](../../crates/application/src/scheduled_jobs/mod.rs) and
[`cron_jobs/mod.rs`](../../crates/application/src/cron_jobs/mod.rs).

### Runtime admission below the scheduler

Before user JavaScript can run, a V8 action crosses several independent boundaries:

1. the scheduled-job executor's source parallelism limit;
2. the application V8-action limiter;
3. the isolate scheduler's bounded external queue;
4. scheduler eligibility, including worker, per-client, and independent-action limits;
5. the low-priority active-JavaScript permit wait; and
6. assignment to an eligible isolate worker.

Dependency capacity can add bounded overflow for work that releases an isolate-holding ancestor.
A scheduled root has no such ancestor, so it correctly remains in shared capacity and can be
rejected by the normal queue contract. With lane-aware queueing enabled, an independent scheduled
action can encounter `queue_full`, `delay_control_shed`, `hard_expired`, caller cancellation,
scheduler closure, or worker failure. With lane-aware queueing disabled, the legacy CoDel path can
still reject or expire it. The active-JavaScript permit wait also retains the external queue's
absolute deadline.

Node actions use a separate application limiter and executor transport. Their preparation includes
loading the current source package, environment, signed package URLs, and callback credentials.
The Node process has no equivalent in-process isolate-worker handoff, so its barrier is immediately
before `NodeActions::execute` rather than inside the child process.

`SCHEDULED_JOB_EXECUTION_PARALLELISM` limits source tasks, not V8 CPU slots. A value such as 64 can
be intentional when many actions spend most of their lifetime awaiting `fetch()` or other I/O,
and it covers Node and V8 actions as well as mutations. It is not required to equal an eight-worker
independent-action cap.

## Failure mode

Before this patch, both schedulers committed `Pending -> InProgress` before calling the application
function runner. All runtime admission happened after the durable at-most-once claim:

```text
read exact Pending job
commit InProgress
wait for application permit
enqueue in isolate scheduler
wait for active-JavaScript permit and worker
start JavaScript
```

A queue rejection or runtime shutdown between the commit and JavaScript start returned a system
error to the scheduled-job executor. Its retry path compared the original `Pending` job with the
now-`InProgress` row, correctly refused to change it, and reported the error. A later scheduler pass
then converted the abandoned claim to the generic transient failure. The action was not retried
because the durable state could no longer prove that it had not started.

This explains why an isolated failure can occur without sustained overload or a visibly full
queue. Adaptive delay control acts on measured dispatch delay, not only physical depth, and both
queue implementations retain finite expiry and closure paths. More importantly, the scheduler had
made an ordinary pre-execution rejection irreversible by claiming the action too early. The defect
was the ordering between two valid mechanisms, not necessarily an incorrectly sized queue or a
malfunctioning load-shedding controller.

## Required invariants

The patch preserves these properties:

- `Pending -> InProgress` remains monotonic. There is no transition back to `Pending`.
- User action code cannot start before the exact durable claim commits successfully.
- The claim is not attempted until the action reaches its environment-specific admission
  boundary.
- Queueing, admission, preparation, or validation system failures before that boundary leave the
  job pending for its existing retry contract.
- The fresh claim transaction verifies the expected pending job's mutable execution metadata, not
  only its document ID or state tag.
- Cancellation or state change before a successful claim drops the prepared execution and releases
  its application permit, active permit, and worker.
- Scheduled roots remain independent and use neither dependency queue reserve nor dependency worker
  reserve.
- A successful claim followed by process loss remains conservatively at most once; the patch does
  not infer that a missing completion is safe to retry.
- No metric label includes a job, function, module, component, request, deployment, client, or
  tenant identifier.

## Design

### Two-way start barrier

[`function_runner/server.rs`](../../crates/function_runner/src/server.rs) defines a paired
controller and gate backed by two one-shot channels:

- the runtime-owned gate sends `ready` and waits for `start`;
- the scheduler-owned controller waits for `ready` and, only after the claim, sends `start`.

Dropping either side closes the corresponding channel. The drop behavior is the cancellation
protocol: no detached task, cancellation token, durable reservation row, or compensating state
transition is required.

The existing `function_started_sender` remains separate. It is a one-way notification used after
V8 request state has started and cannot prevent JavaScript from advancing. The new gate must stop
the execution, so reusing the old sender without an acknowledgment channel would leave the same
claim race.

The gate is optional on the general `FunctionRunner` interface. Queries, mutations, ordinary
actions, and HTTP actions keep their previous behavior. Database functions and HTTP actions reject
an unexpected gate as an invariant violation. Only the scheduled and cron action call sites create
one.

### Exact event sequence

For a pending durable action the new sequence is:

```text
read and classify the exact Pending job
drop the scheduler's original transaction
create request and execution IDs
begin action preparation while the job remains Pending
reach the environment-specific execution admission boundary
runtime sends ready and waits
open a fresh transaction
verify the job still matches the expected Pending execution metadata
commit Pending -> InProgress with the prepared request and execution IDs
scheduler sends start
begin user execution
record Success or Failed using the exact InProgress job
```

The original scheduler transaction is deliberately dropped before admission. The claim transaction
is created only after `ready`, so it has a current read set and a short lifetime. Ordinary scheduled
jobs compare the fresh metadata with the expected job and use the fresh transaction's namespace
mapping. That comparison covers path, state, schedule timestamps, and attempt counters. It does not
compare the metadata's `args_id` or legacy inline argument bytes: normal model paths insert argument
storage once, never replace it, and delete it only with the job. If that immutability contract
changes, the matcher and claim token must change with it. Cron jobs compare the complete registered
cron job, including its definition, arguments, run time, and state. Cancellation or replacement
during admission therefore makes the claim return `state_changed` and cancels the prepared
execution.

Action preparation still uses the function runner's repeatable database snapshot. A deployment can
change module state while the action waits, just as a normal queued invocation can outlive a later
deployment. The prepared invocation executes the validated snapshot only if the durable job itself
still matches at claim time. The patch does not add module generation to scheduled-job identity or
hold a deployment transaction open as a second durable fence.

### V8 admission boundary

The V8 gate travels through
[`application_function_runner/mod.rs`](../../crates/application/src/application_function_runner/mod.rs),
the in-process `FunctionRunner`, and
[`isolate/client.rs`](../../crates/isolate/src/client.rs). The isolate scheduler first performs its
normal queue selection and acquires the initial active-JavaScript permit. It assigns the request to
an eligible worker with the normal active-class accounting. Only then does
[`isolate_worker.rs`](../../crates/isolate/src/isolate_worker.rs) signal `ready`.

The selected worker waits before constructing `ActionEnvironment`, starting V8 request state, or
creating the action timeout. Consequently the claim transaction does not consume user JavaScript
timeout and no module evaluation or user callback can begin across the barrier. The worker and its
active permit remain owned for the short claim, which makes the reservation real rather than an
advisory scheduler counter.

If the controller disappears while the worker waits, the gate returns an error before touching
isolate request state. The worker marks the isolate clean, sends the canceled response, and returns
through the normal worker-completion path. Existing guards release active accounting and the
active-JavaScript permit. There is no worker or permit leak and no context is published for reuse.

The controller also keeps polling the action response while the database claim is in flight. If a
selected worker or scheduler disappears after `ready` but before the claim result, the controller
drops the claim future and never sends `start`. A commit already accepted by the database can still
become visible after its response path is dropped; that ambiguity is handled exactly like any other
possibly visible claim and is never used as permission to retry or release execution.

After release, the worker still constructs `ActionEnvironment`, rechecks isolate cleanliness,
starts request state and its timeout, and creates the V8 request scope before evaluating the
module. Those runtime-start steps can still fail before the first user instruction, as can the
process itself. They are on the post-claim side of the at-most-once boundary and retain the
existing conservative result.

### Node and invalid-action behavior

A Node action performs database and package preparation and acquires the Node action application
permit while the durable job remains pending. It signals `ready` after the complete
`ExecuteRequest` is built and immediately before `node_actions.execute`. Dropping the controller
releases that permit without sending the request.

This boundary prevents application admission and application-side package, environment, and
request-preparation failures from consuming the durable claim. After release,
`NodeActions::execute` still serializes the executor request, checks executor shutdown, may acquire
or start the local Node generation, and sends the HTTP request. The barrier cannot reserve capacity
inside the separate Node process or prove where a startup or transport failure occurred relative to
user code. Those failures remain on the conservative post-claim side even when the request never
reached the child. Package URLs are also signed before `ready`; an unusually slow claim can leave
them expired by the time the executor downloads the package, which is another conservative
post-claim failure. Moving the protocol into the Node child would be a separate cross-process
design.

A path or argument validation result that is already a deterministic developer error does not need
an isolate or Node worker. It still waits on the gate at the application boundary so the durable
claim precedes the failed action completion. An invalid module environment also consumes the gate
before returning its existing system error, placing that result on the conservative post-claim
side. A system error during validation instead ends the action future before `ready` and leaves the
job pending.

## Cancellation and failure matrix

| Point | Durable state | Prepared execution | Result |
| --- | --- | --- | --- |
| Validation or preparation system error before `ready` | `Pending` | Dropped | Existing scheduler retry |
| Queue full, CoDel/lane shedding, hard expiry, or scheduler closure | `Pending` | Never admitted or dropped | Existing scheduler retry |
| Caller or executor task canceled before `ready` | `Pending` | Gate/controller drop releases ownership | Later scheduler attempt may proceed |
| Exact job changes before claim | Changed by the winning operation | Selected execution canceled | No stale action starts |
| Selected execution is lost while the claim is in flight | `Pending` if the claim is definitely not visible; otherwise possibly `InProgress` | Never released | Pending jobs use the existing retry; a visible claim uses conservative recovery |
| Claim loses OCC or returns another commit error | `Pending` if the commit is definitely not visible; otherwise possibly `InProgress` | Selected execution canceled without release | Pending jobs use the existing retry; a visible claim uses conservative recovery |
| Claim commits, then `start` delivery fails | `InProgress` | Worker is gone or closing | Existing conservative transient failure |
| Backend exits after commit and before release | `InProgress` | Lost with process | Existing conservative transient failure |
| Backend exits after JavaScript starts | `InProgress` | Execution outcome unknown | Existing conservative transient failure |
| Completion transaction temporarily fails | `InProgress` | Action already completed | Existing completion retry loop |

Executor-task cancellation concurrent with the claim has one unavoidable boundary. If task
cancellation wins before the database commit, dropping the controller cancels the prepared
execution and the row stays pending. If the commit becomes visible first—even if the caller does
not observe its return—the row is `InProgress` and must be treated as possibly executed. This is
the same uncertainty as a process failure after commit and is why the patch does not promise
exactly once.

After a visible ordinary scheduled-job claim, the existing cancellation API can change
`InProgress` to `Canceled` before the controller sends `start`. Cancellation does not abort an
already claimed action, so the controller can still release it and the later completion observes
the changed state without overwriting `Canceled`. This race already existed across the larger
pre-patch interval between claim and execution; the barrier preserves that cancellation contract.

A returned commit error is not itself proof that the transaction stayed invisible. The committer
can lose its result path after accepting work, and a persistence failure can have an uncertain
durability result. The controller never releases the prepared execution on an error. If the claim
did become visible, the existing `InProgress` recovery therefore remains deliberately conservative.

The action future is polled while waiting for `ready` and while the claim is in flight. A
pre-admission error therefore wins without running the claim, and a lost admitted execution stops
the controller from releasing it. A successful action completion before release is an invariant
violation because every action environment must consume the optional gate before completing.

## Timing and metrics

The patch adds two bounded histograms:

- `durable_action_admission_wait_seconds{source,status}` measures from starting durable action
  preparation until the environment signals `ready`;
- `durable_action_claim_seconds{source,status}` measures fresh exact-state verification and claim
  commit after admission.

`source` is only `scheduled` or `cron`. Admission status normally uses `success`, `canceled`, or the
bounded system error classification. Claim status additionally uses `state_changed` or
`execution_lost`; an impossible action completion before the barrier uses `invariant_violation`.
Dropping either timer before an explicit result records `canceled` rather than misclassifying task
cancellation as a system error.

These histograms supplement rather than replace application-permit, isolate queue, scheduler,
active-permit, Node executor, database commit, sampled ready-queue lag, and ordinary scheduler
admission-lag metrics. Admission time includes validation and environment preparation as well as
actual permit and queue waits. Use the inner metrics to localize a high sample. Claim time should
normally remain close to one short database transaction; persistent growth indicates database or
contention pressure while admitted runtime capacity is being held.

The V8 service timer and JavaScript timeout begin after barrier release, so claim latency is not
charged as V8 service or user execution. The application action's end-to-end elapsed time and
outer function-runner timers do include the brief claim interval. Node executor timing starts only
after release, while the action's outer elapsed time includes preparation and claim. This split is
intentional and should be preserved when interpreting cron execution duration.

## Interaction with capacity and queue control

The dependency-capacity patch supplies the application limits, independent-action cap, queue and
worker arithmetic, active ownership, and cancellation-safe permit handling used by this patch. The
new barrier adds no overflow. A selected scheduled V8 action holds exactly the capacities it would
hold while beginning execution; it merely pauses for one fresh database claim.

The queue-control patch determines whether external isolate admission uses legacy CoDel or the
optional lane-aware policy. Either policy can reject before dispatch, and this patch makes both
forms retryable by moving them before the durable claim. It does not exempt scheduled actions from
adaptive shedding, lengthen deadlines, increase queue size, or add a scheduled lane.

This brief reservation is different from reserving shared-base workers for a deployment or another
class before a concrete request is selected. It neither lowers ordinary capacity nor leaves idle
capacity unused. Under heavy claim latency, however, admitted scheduled actions can temporarily
hold several workers while committing. Operators should use the new claim histogram and database
signals before changing any capacity knob.

`SCHEDULED_JOB_EXECUTION_PARALLELISM`, `APPLICATION_MAX_CONCURRENT_V8_ACTIONS`,
`MAX_ISOLATE_ACTION_WORKERS`, `MAX_ISOLATE_WORKERS`, and `FUNRUN_ISOLATE_ACTIVE_THREADS` retain their
separate meanings. Matching them numerically would collapse useful I/O concurrency and would not
remove queue-full, expiry, shutdown, or Node-executor failures.

## Mixed-version rollout

The patch changes no stored schema, job representation, request ID format, or completion state. A
patched scheduler can process jobs created by an older backend, and an older scheduler can process
jobs created by a patched backend. `InProgress` recovery is identical on both versions.

Behavior is not uniformly protected while versions overlap. An older scheduler can still claim a
pending action before runtime admission and expose the original failure window. Begin acceptance
measurements only after old scheduled-job and cron executor tasks have drained or their processes
have stopped.

The barrier is process-local and is implemented only by the in-process function runner used by the
self-hosted backend. A future remote function-runner implementation would need an explicit
admission/continue protocol rather than silently discarding the optional gate.

## Adoption and rollback

Apply this patch after the dependency-capacity and isolate queue-control commits in the maintained
source chain. The lane-aware policy may remain disabled; the legacy CoDel path benefits as well. No
new knob is required and activation is automatic after all scheduler processes use the patched
image.

Before rollout, record scheduled and cron generic transient action failures, sampled ready-queue
lag, ordinary scheduler admission lag, application action waits, queue rejection/expiry, active
worker classes, active-JavaScript waits, and database commit latency. After rollout, verify
successful admission and claim samples for both sources and confirm that pre-dispatch queue
failures leave jobs pending for retry rather than creating zero-runtime abandoned claims.

Rollback restores the previous backend image. No data or configuration rollback is required. A
job already in `InProgress` keeps the same conservative recovery semantics across rollback; do not
manually move it back to `Pending`.

## Verification

Focused unit tests cover:

- a runtime gate remaining blocked after `ready` until the controller releases it;
- controller drop before admission;
- controller drop after admission but before release;
- gate drop while the controller waits; and
- isolate-worker wait and cancellation behavior.

The affected Rust packages compile together through the application crate. Production verification
must additionally exercise both ordinary scheduled and cron actions under a controlled queue wait,
observe `Pending` until runtime admission, observe `InProgress` before JavaScript effects, and
confirm that a forced pre-admission rejection is retried without an abandoned claim. A post-claim
process-stop test should continue producing the existing generic transient failure; changing that
result would violate the at-most-once boundary.

Run the focused checks before publishing an image:

```sh
scripts/run_cargo.sh test -p function_runner execution_start_tests
scripts/run_cargo.sh test -p isolate execution_start_tests
scripts/run_cargo.sh check -p application
```

Also run the repository's normal formatting, clippy, build, and broader test gates for the affected
packages before release.

## Rejected alternatives

- **Move `InProgress` back to `Pending` after a pre-execution failure.** This creates an ABA state
  transition. A stale executor, cancellation, replacement, or delayed completion can no longer
  identify which `Pending` generation it observes. It also breaks the established semantic rule
  that `InProgress` means execution may have crossed an irreversible external-effect boundary.
- **Retry after the durable claim when the runtime reports it did not start.** A one-way error or
  missing start notification is not durable proof. Process loss can occur after JavaScript starts
  and before the report, so this can duplicate external effects. The two-way barrier instead keeps
  retryable failures on the pre-claim side.
- **Exempt scheduled actions only from adaptive delay shedding.** Lane shedding is only one failure
  path. Queue-full rejection, legacy CoDel expiry, finite hard expiry, active-permit timeout,
  scheduler closure, worker failure, and application-side Node preparation failures would still
  consume an early claim. Removing shedding also weakens overload protection without repairing the
  ordering defect.
- **Classify scheduled roots as dependencies.** A scheduled root releases no isolate-holding
  ancestor. Letting it consume dependency queue, application, or worker overflow can delay the
  actual descendants that reserve exists to unblock and still does not guarantee admission.
- **Set scheduled parallelism, the application V8 limit, or all action limits to the isolate action
  cap.** These controls govern different resources. Scheduled and Node actions often wait on I/O,
  so a source limit of 64 with eight independent isolate workers can be intentional. Equal values
  neither reserve a worker nor remove finite queue and shutdown failures.
- **Reduce `SCHEDULED_JOB_EXECUTION_PARALLELISM`.** Lower fan-out can reduce incident frequency, but
  it also reduces useful Node and I/O overlap and leaves every admitted action exposed to the same
  irreversible pre-execution window.
- **Increase queue depth or deadlines.** Larger buffers retain more work and longer deadlines defer
  failure; neither creates service capacity or removes queue-full, shutdown, and worker-loss paths.
- **Create a dedicated scheduled-action pool or permanent worker quota.** A second pool fragments
  capacity, needs its own per-client, active-permit, shutdown, and Node policy, and can sit idle
  while ordinary work queues. The short barrier uses a worker selected by the existing fair
  scheduler and releases it immediately after one claim.
- **Reserve a worker as soon as the scheduled executor sees a pending job.** Reservation before
  normal application admission and queue selection bypasses the capacity model and requires a new
  cancellation-safe reservation scheduler. This patch reserves only an already admitted request.
- **Signal readiness after JavaScript starts.** User code may perform an external effect in its
  first instruction. Committing the claim after that signal would make `Pending` coexist with a
  possibly executed action and permit duplication after a crash.
- **Hold the original durable transaction while waiting for capacity.** Queue waits can be long.
  Holding the transaction lengthens snapshot lifetime, increases OCC exposure, and still requires a
  final exact-state check. A fresh short claim transaction after admission gives the database one
  clear ordering point.
- **Add a durable `Preparing` state.** A new state would require schema, cancellation, restart,
  garbage-collection, dashboard, and mixed-version semantics. If `Preparing` were retryable after a
  crash it would still need proof that JavaScript had not started; if it were not retryable it would
  reproduce the abandoned-claim failure under another name. The process-local barrier supplies the
  needed proof before the existing claim without expanding the durable state machine.
