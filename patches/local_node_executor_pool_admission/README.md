# Local Node Executor Pool Admission Policy

Status: implemented as an optional operator policy for application-declared
local Node executor pools.

This patch adds independent-action concurrency limits, queue-delay
observability, and event-loop unresponsiveness budgets for the default and
named local Node executor pools. It is a separate adoption unit from pool
routing: an application declaration selects a process pool, while this patch
lets an operator constrain how work enters that pool and choose how long its
main event loop may stop responding before the process is replaced.

The design rationale, liveness analysis, and rejected alternatives are in
[`design_reference.md`](design_reference.md).

## Patch composition

This patch requires
[`pinned_local_node_executor_pools`](../pinned_local_node_executor_pools/README.md)
for named routing, resident-generation ownership, and cutover fencing. It also
requires
[`scheduled_action_admission`](../scheduled_action_admission/README.md) so a
scheduled or registered-cron action waits for runtime capacity while its
durable job is still `Pending`.

The application-wide `APPLICATION_MAX_CONCURRENT_NODE_ACTIONS` limit remains
the hard aggregate Node action bound. A configured per-pool limit is an
additional child bound; it never creates capacity above the global limit.

## Configuration

`LOCAL_NODE_EXECUTOR_POOL_POLICIES` is a strict JSON object keyed by a valid
application-declared pool name. The reserved `default` key configures modules
without a named assignment. For example:

```json
{
  "planning": {
    "maxConcurrency": 1,
    "maxEventLoopUnresponsiveSeconds": 30,
    "queueWarningSeconds": 10
  }
}
```

All fields are optional within a non-empty pool policy:

- `maxConcurrency` limits independent actions admitted to that pool. It must
  be positive and no greater than
  `APPLICATION_MAX_CONCURRENT_NODE_ACTIONS`.
- `maxEventLoopUnresponsiveSeconds` is the maximum elapsed interval for which
  the pool's resident Node main event loop may fail health probes before the
  backend retires it. It is not a CPU-time quota.
- `queueWarningSeconds` records one warning metric when an ongoing
  pool-admission wait reaches the configured duration. It requires
  `maxConcurrency`; it does not reject a scheduled job or change its durable
  state.

Unknown fields, invalid pool names, empty policies, zero values, a queue
warning without a pool concurrency limit, and a per-pool concurrency limit
above the application-wide limit fail backend startup. The map accepts at most
nine entries, matching the default pool plus the routing protocol's eight named
pools. A valid policy may name a pool that is not present in the current
application topology, which permits operator configuration before application
deployment.

Omitted pools preserve the existing global-only admission and default
watchdog behavior. Omitting one field preserves that field's existing
behavior.

## Admission behavior

For a configured pool, an action acquires capacity in this order:

1. the per-pool independent-action permit;
2. the application-wide Node action permit;
3. the exact resident-generation admission used by Node routing; and
4. for durable scheduled work, the monotonic `Pending -> InProgress` claim.

The pool permit is retained through invocation. Pool-first ordering prevents a
backlog for one pool from holding every global Node permit. All Node paths use
the same order, so the two semaphore levels cannot form a permit cycle.

Direct action requests use the existing bounded application-runner wait. Pool
and global admission share one absolute deadline, so adding a pool policy does
not double that wait. Scheduled and registered-cron actions may wait without
that direct-call timeout, but remain durably `Pending` until all runtime
admission required by the start barrier succeeds.

Dependency work uses the existing bounded dependency-overflow model. A
concurrency-one pool therefore admits one independent root plus only the
bounded descendant work needed to unblock an admitted ancestor. This avoids a
parent-child capacity deadlock; `maxConcurrency` is deliberately the
independent-root limit rather than an absolute count including dependencies.
The application-wide Node limit remains the aggregate hard bound.

Cancellation drops both permits. No new durable queue, claim state, retry
protocol, or recovery state is introduced.

## Watchdog behavior

The local Node `/health` request is served by the same main event loop that
accepts `/invoke`. Failed probes therefore provide evidence that the process
cannot currently make invocation progress. A per-pool unresponsiveness budget
changes how long that evidence is tolerated; it does not move the probe to an
isolated worker and does not claim to measure the action's consumed CPU time.

Short probes and first-miss diagnostics remain active. The configured budget
also bounds the first in-flight probe from the time its request starts, even
when the ordinary probe timeout is longer. A successful response before the
deadline clears the failed interval. A completed failed probe retains that
request start as the interval origin, and the watchdog races the remaining
budget independently of later probe intervals and request timeouts. Once the
budget expires, the backend identity-fences, terminates, and replaces that
generation through the existing local-executor lifecycle. An action interrupted
by hard retirement retains the existing conservative outcome because external
effects may already have started.

A larger budget is appropriate only when the operator intentionally accepts a
longer interval without Node callback or invocation progress. It does not make
an unbounded CPU-bound algorithm safe; such actions still need an algorithmic
bound or explicit yielding.

## Observability

Metrics report, by pool:

- configured independent concurrency;
- the configured queue-warning duration, with zero meaning disabled;
- active and waiting pool-admission requests;
- admission wait duration and outcome;
- waits that reached the configured warning duration;
- the configured event-loop unresponsiveness budget; and
- the existing health-probe, consecutive-miss, generation, and request
  lifecycle signals.

Queue warnings are evidence for capacity tuning. They do not automatically
fail or retry work.

## Activation and rollback

Apply the routing and scheduled-admission prerequisites first. Configure only
the pools that need an additional bound, verify that every per-pool value is no
greater than the application-wide Node limit, restart the backend, and verify
the configuration and queue metrics before increasing traffic.

To roll back policy without changing application routing, remove
`LOCAL_NODE_EXECUTOR_POOL_POLICIES` and restart the patched backend. To remove
the patch itself, remove the setting before restoring an earlier image. No
schema or data migration is required. Existing `InProgress` jobs retain their
normal conservative recovery contract.
