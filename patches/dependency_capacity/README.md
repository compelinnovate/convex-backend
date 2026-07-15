# Dependency Capacity and Isolate Scheduler Liveness

## Summary

This patch prevents an isolate-capacity inversion in which an action retains a worker while the
query, mutation, or child action it awaits cannot obtain one. It propagates ancestor-unblocking
ownership through the application, function-runner, V8, and Node boundaries and gives only that
dependency work bounded overflow above shared application, queue, and worker capacity.

The patch also adds a separate cap for independent V8 and HTTP action shells. It does not give
dependencies unconditional priority while shared capacity is available, reserve CPU execution
permits, or guarantee arbitrary recursive fan-out.

Upstream now has a distinct internal path for direct nested UDF callbacks. Those requests bypass
CoDel, are selected before external queued requests, and acquire active-JavaScript permits at high
priority. This patch keeps that upstream path intact. Its bounded `Q + R` CoDel admission remains
necessary for dependencies that arrive through external boundaries, especially Node/action
callback chains. Both paths dispatch into the same physical worker total and dependency reserve;
neither path creates a second worker pool or extra active-JavaScript permits.

Outer HTTP admission is an independent adoption unit described in
[`shared_base_http_admission/README.md`](../shared_base_http_admission/README.md). A Node action chain normally needs
both patches because its authenticated callbacks re-enter the main HTTP service before reaching the
application and isolate gates.

## Motivation

Default-runtime and HTTP actions can retain an isolate worker while awaiting `ctx.runQuery`,
`ctx.runMutation`, or `ctx.runAction`. Separately scheduled transactional subfunctions can do the
same when `SUBFUNCTIONS_IN_SAME_ISOLATE=false`. If independent ancestor work occupies every worker,
its descendants wait in the same pool and the ancestors cannot finish. Queue expiry then surfaces
as function or HTTP failure even when the database and host still have headroom.

The relevant property is not function type or whether a request can itself call a descendant. It
is whether completing this request releases an isolate worker retained by an ancestor. The patch
therefore tracks three independent properties:

- `unblocks_ancestor`: completion releases an isolate-holding caller;
- `can_block_on_descendant`: execution can retain its worker while calling another function;
- `is_isolate_action`: an independent V8 or HTTP action subject to the action-shell cap.

Metrics expose the role combinations as `independent`, `descendant_holder`, `dependency`, and
`dependency_descendant_holder`. Only `unblocks_ancestor` grants overflow eligibility.

## Finite capacity model

Let:

- `T = MAX_ISOLATE_WORKERS`, the physical assigned-worker limit;
- `R = ISOLATE_DEPENDENCY_WORKER_RESERVE`, dependency-only overflow;
- `B = T - R`, the shared worker base;
- `Q = ISOLATE_QUEUE_SIZE`, the shared queue base.

Every request class consumes shared base worker capacity while it is available. Only a dependency
may raise worker occupancy above `B`, up to `T`. At the external CoDel boundary, only a dependency
may raise queue occupancy above `Q`, up to `Q + R`. Direct internal nested-UDF callbacks use
upstream's separate priority channel and therefore do not consume CoDel entries. Dependencies do
not reserve idle workers or ordinary queue entries: ordinary work can use all shared base capacity.
The reserve is bounded and cannot make unbounded recursion or parallel fan-out safe.

The same rule applies independently per isolate client. Global capacity being available does not
allow one client to exceed its base with ordinary work. Dependency eligibility above base remains
subject to physical and per-client totals.

Each nonzero `APPLICATION_MAX_CONCURRENT_*` function limit also carves dependency overflow from
its configured total. The effective reserve is the smaller of `R` and one less than that limit.
Queries, mutations, and default-runtime actions use it only when they unblock an isolate-holding
ancestor. Nested Node actions receive equivalent overflow at the Node-action application gate
because their parent retains a permit from that gate.

`MAX_ISOLATE_ACTION_WORKERS` separately caps assigned independent V8 and HTTP actions. `0` derives
the cap from `B`; an explicit value cannot exceed `B`. Queries, mutations, and dependency actions
do not consume this class cap. The cap protects mixed traffic without reducing total query or
mutation worker capacity.

## Propagation and request ownership

The dependency marker is created only at a boundary that knows an isolate worker remains held:

- direct separately scheduled query and mutation callbacks, which use upstream's internal
  scheduler path;
- V8 child-action calls;
- authenticated Node callbacks and nested Node actions, which re-enter through external request
  boundaries.

The marker follows the request through application admission, query-cache planning, the
`FunctionRunner` boundary, isolate queueing, and worker assignment. A top-level browser, HTTP
client, cron, scheduled function, or Node action is independent because it has no isolate-holding
ancestor.

Node callbacks carry the ancestry marker alongside the existing authenticated callback token. A
reverse proxy on `CONVEX_CLOUD_ORIGIN` must preserve both headers for trusted callback routes. The
marker is not accepted as an application priority declaration and is not inferred from module,
function, route, user, or tenant identity.

Query-cache coalescing must not make a dependency wait for an independent cache-miss leader that
cannot run. In that case the dependency starts its own execution. This can duplicate database work,
but it preserves the ancestor-release contract and is counted separately.

## Queue and worker scheduling

The scheduler has two ingress paths with intentionally different waiting semantics:

- Direct nested UDF callbacks use upstream's internal unbounded channel. The scheduler polls this
  path before the external stream, it has no CoDel expiry, and it acquires the existing
  active-JavaScript permit in upstream's high-priority mode before worker assignment. Its local
  buffer discards closed callers and selects the oldest request eligible in the current worker
  snapshot, so an older callback at one client's total cannot hide an eligible callback for
  another client.
- External requests use the upstream bounded CoDel queue. Dependencies can use `R` additional
  entries after the shared `Q` entries are occupied. Class-aware selection leaves requests queued
  until global and per-client worker capacity permits them to start. The optional lane controls in
  [`isolate_queue_control/README.md`](../isolate_queue_control/README.md) retain these capacities
  and external dependency semantics.

Both paths use the same worker-selection and accounting rules. Every dispatch counts against the
same physical `T`; only an ancestor-unblocking request can use occupancy above `B`. Direct internal
priority therefore cannot duplicate reserve capacity. Worker completion updates global and
per-client accounting before another dispatch decision. External dependencies do not jump older
eligible external work below shared base capacity; they become the only externally eligible class
when the shared base is full.

`FUNRUN_ISOLATE_ACTIVE_THREADS` remains a separate active-JavaScript permit gate. Upstream now
acquires that permit before worker assignment. Its two-tier limiter has one fixed permit total:
direct internal nested transactional functions and all permit reacquisitions wait at high priority,
while initial external root and action requests wait at low priority. The tiers change notification
order but do not reserve or add active-JavaScript capacity. A dependency can be eligible for the
worker reserve and still wait for an active permit or CPU. Initial external root and action permit
waits remain bounded by the request's original queue deadline: the applicable CoDel expiration or
lane hard deadline. Direct separately scheduled nested transactional functions do not use that
external deadline because they cannot be retried safely. Both ingress paths cancel an outstanding
permit acquisition when response-channel closure shows that the caller has disappeared; dropping
that acquisition uses the limiter's cancellation-safe notification handoff and cannot leak a
permit. While one selected external request waits for its permit, a non-consuming queue expiry
companion continues enforcing every other retained entry's own deadline without another enqueue or
worker completion.

Scheduler dependency ownership and active-permit priority intentionally differ at resource
boundaries. A transactional descendant that retains an isolate-holding ancestor is eligible for
scheduler overflow and receives high active-permit priority. An action callback can be eligible for
scheduler overflow while retaining root-style, low-priority initial active-permit acquisition. An
already-started independent root reacquires its suspended permit at high priority even though it is
not a scheduler dependency. The patch does not add active-permit overflow because doing so could
oversubscribe the host and move rather than remove the bottleneck.

## Configuration

The self-hosted Compose template passes through:

- `MAX_ISOLATE_WORKERS` (default `300`);
- `ISOLATE_DEPENDENCY_WORKER_RESERVE` (default `1`);
- `MAX_ISOLATE_ACTION_WORKERS` (default `0`, meaning derive from shared base);
- `ISOLATE_QUEUE_SIZE` (default `2000`, for the external CoDel base);
- `FUNRUN_ISOLATE_ACTIVE_THREADS` (default `0`, meaning unlimited).

Isolate capacities use strict parsing. `T` and `Q` must be positive, `R` must be smaller than `T`,
`Q + R` must fit in `usize`, and an explicit independent-action cap cannot exceed `B`. Invalid
present values fail startup rather than silently falling back.

Choose `R` from measured maximum nested depth and bounded fan-out, not CPU count alone. A larger
reserve increases the number of assigned workers and retained request state; it does not add CPU.
Choose the independent-action cap from mixed-traffic evidence. An action-only benchmark is not
enough to establish a safe value for queries and mutations.

Cron and ordinary scheduled-function executors each independently apply
`SCHEDULED_JOB_EXECUTION_PARALLELISM`. The value must be a positive decimal integer within Tokio's
semaphore permit limit. Both executors resolve it before their futures are spawned, so invalid
values fail application startup instead of leaving a running backend with dead executor tasks. The
patch exports `scheduled_job_execution_parallelism_info` as the effective per-executor value. It is
not the combined cron-plus-scheduled limit and not a running-job count. The gauge changes no
scheduling behavior and should not justify a standalone backend restart.

The patch also exports source current-occupancy gauges for both executors:
`scheduled_job_num_running_info` for ordinary scheduled functions and
`cron_job_num_running_info` for registered crons. The executors update these gauges after each start,
after each cron completion notification, and after each drained ordinary-completion batch; the
external scrape interval still determines which short-lived states are retained. Executor startup
initializes its gauge to zero, and cancellation or exit resets it to zero so application shutdown
cannot leave stale process occupancy. A completion for an unowned job, or duplicate IDs in one
ordinary batch, fails the executor iteration after applying valid completions and publishing the
resulting occupancy instead of silently accepting the broken ownership transition. The gauges
complement the sampled ready-queue lag histograms, the direct ordinary scheduled-job admission-lag
histogram, and the legacy sampled ordinary-occupancy histogram without adding function, module, or
job identifiers. Both executors refresh the first non-running queue entry while their execution
slots are full, so cancellation or replacement cannot leave an obsolete ready time in those
sampled histograms until another job finishes.

## Metrics

Use these bounded metric families together:

- `isolate_scheduler_capacity_info{pool_name,capacity_kind}` for physical, shared-base, and
  independent-action worker capacities;
- `isolate_scheduler_requests_enqueued_total{pool_name,scheduler_class}` and
  `isolate_scheduler_requests_dispatched_total{...}` for role progress;
- `isolate_scheduler_requests_expired_total{...}` and
  `isolate_scheduler_requests_rejected_total{...,reason}` for pre-dispatch deadline and admission
  failure;
- `isolate_scheduler_active_requests_info{pool_name,scheduler_class,is_isolate_action}` for current
  assigned-worker ownership;
- `isolate_scheduler_dependency_reserve_dispatch_total{pool_name}` for dispatch above shared base;
- `isolate_scheduler_dependency_queue_reserve_enqueue_total{pool_name}` for external dependency
  enqueue above `Q`;
- `cache_plan_go_total{reason="dependency_cannot_wait_for_independent_peer"}` for duplicated
  dependency cache leaders;
- `scheduled_job_execution_parallelism_info` for the resolved per-executor scheduled-job limit;
- `scheduled_job_num_running_info` and `cron_job_num_running_info` for source current occupancy;
- `scheduled_job_execution_lag_seconds` and `cron_job_execution_lag_seconds` for sampled
  ready-queue lag in each executor;
- `scheduled_job_admission_lag_seconds` for direct target-time-to-admission lag on each ordinary
  scheduled-job admission attempt.

Dependency enqueue without dispatch indicates a downstream capacity or scheduling problem.
Dependency expiry or rejection is the liveness failure signal. Reserve use without failures is
evidence that the bounded policy is doing useful work, not evidence that the reserve is too small.
Correlate all three with active-JavaScript wait, CPU throttling, HTTP admission, application limits,
database waits, and end-to-end errors.

Metric labels are closed enums. Do not add module, function, route, client, request, deployment, or
tenant identifiers.

The backend initializes every valid scheduler expiry and rejection label combination at zero when
the isolate client starts and keeps those labeled counters non-evicting. It also initializes the
unlabeled execute-full counter. Each application function limiter initializes its environment and
function-type timeout counter at zero and keeps that labeled series non-evicting. A compatible
running backend therefore exposes a measured zero without requiring a failure first.

## Interaction with other patches

[`shared_base_http_admission/README.md`](../shared_base_http_admission/README.md) provides bounded callback overflow
at the outer HTTP service. Its total and reserve are independent from `T` and `R` because a callback
can wait at later application and isolate stages while retaining its HTTP permit.

[`isolate_queue_control/README.md`](../isolate_queue_control/README.md) changes overload timing and queue selection,
not the dependency definition or capacity arithmetic. The deployment control-plane lane remains
inside shared base and can never consume dependency overflow.

Context reuse for HTTP actions, database UDFs, and ordinary actions can reduce service time, but
none of those features changes dependency ownership. Degradable query admission can reduce
independent demand, but a client declaration cannot grant dependency status.

## Rollout and rollback

1. Record current HTTP admission, application waits, queue depth and expiry, scheduler role,
   active-permit wait, CPU, database, and function-error baselines.
2. Set explicit worker, reserve, action-cap, queue, and active-thread values appropriate to the
   host. Keep outer HTTP admission fixed during the first scheduler comparison.
3. Restart one controlled backend population and verify the capacity gauges before sending traffic.
4. Exercise representative default-runtime, HTTP, V8, and Node action chains together with ordinary
   queries, mutations, cron, and scheduled functions.
5. Confirm dependency dispatch above base, bounded queue behavior, action-cap enforcement, and zero
   sustained dependency expiry or rejection.
6. Compare CPU pressure and active-permit wait before increasing any limit.

Rollback requires restoring the previous backend and capacity settings together. Lower Caddy or
other ingress limits before lowering an action-shell or HTTP capacity that those limits assumed.
Removing the patch restores the original inversion risk; lowering demand is the safer temporary
mitigation while investigating.

## Verification boundary

Focused tests cover ancestry propagation across V8, Node, transactional callback, cache,
application-gate, `FunctionRunner`, queue, and worker boundaries; upstream internal callbacks using
the shared physical reserve; external CoDel selection and dependency-only queue overflow; canceled
callers and permit handoffs; queue expiry and closure; per-client totals; action-cap behavior; and
bounded metrics. Production verification should use ordinary application traffic and existing
deployment operations. A synthetic stress fixture is not required.

## Rejected alternatives

- Reserving workers for every action type wastes capacity and does not identify work that releases
  an ancestor.
- Giving all mutations or actions priority can starve independent work and still misses nested
  query ownership.
- Increasing queue depth retains more stale work without increasing service rate.
- Treating a client or module marker as dependency status lets untrusted or application-owned
  metadata consume a liveness reserve.
- Reserving active-JavaScript permits can oversubscribe CPU and move the wait below assignment.
- Unlimited dependency admission cannot be made safe because recursive depth, fan-out, memory, and
  CPU remain finite.
