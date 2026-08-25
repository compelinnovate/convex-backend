# Pace Isolate Module Analysis Through Elastic Query Capacity

This integration patch prevents a function push from adding its complete isolate-module analysis
fan-out on top of the configured degradable-query load. It uses one application-scoped fair gate:

```text
degradable query cache-miss leaders + isolate module analysis attempts
    <= APPLICATION_MAX_CONCURRENT_DEGRADABLE_QUERY_LEADERS
```

The gate remains work-conserving. Degradable leaders can use the full capacity when no analysis is
running. One analysis call can use up to the smaller of the configured capacity, its number of
modules, and `ANALYZE_CONCURRENCY` when degradable leaders are absent. Concurrent calls share the
gate but each retains that per-call fan-out. No worker is kept idle for a deployment.

## Patch placement and prerequisites

This is an ordered integration patch on top of two independently useful adoption units:

- [`isolate_queue_control/README.md`](../isolate_queue_control/README.md) identifies typed backend
  analysis and evaluation work and can place it in the bounded control-plane lane;
- [`degradable_reactive_queries/README.md`](../degradable_reactive_queries/README.md) supplies the
  application-scoped immediate-admission gate and typed stale-result pressure behavior.

The queue-control patch precedes the degradable-query patch in the maintained train, so it cannot
own an implementation that depends on the later gate. Folding deployment pacing into the
degradable client-protocol patch would also make that adoption unit silently own a separate
control-plane behavior. Keeping this composition in a small later patch makes the dependency,
activation, rollback, and future rebase boundary explicit.

The pacing mechanism requires the degradable-query patch. The control-plane lane is recommended
but is not a code prerequisite: without it, paced analysis retains ordinary queue classification,
adaptive shedding, and the ordinary hard deadline.

## Why module analysis creates scheduler pressure

The isolate analyzer does not behave like a typical asynchronous action waiting on network or
database I/O:

- it creates one isolate request for every non-dependency module and runs up to
  `ANALYZE_CONCURRENCY` requests concurrently;
- each request registers, instantiates, and evaluates the selected module's import graph in V8,
  then inspects the resulting exports;
- the analysis environment rejects asynchronous syscalls and asynchronous ops at import time, so
  analysis cannot release its active-JavaScript permit around supported application I/O;
- one rejected-before-execution or overload response can be retried up to two times after the
  first attempt, for at most three queue entries per module;
- analysis is not an isolate action, so it does not consume the application V8-action limiter or
  `MAX_ISOLATE_ACTION_WORKERS`.

With the optional control-plane lane enabled, analysis still consumes shared-base physical workers,
active-JavaScript permits, CPU, and shared queue entries. It cannot use dependency reserve and has
no dedicated worker. A lane changes classification, deadline, and shedding policy; it does not add
service capacity.

This explains why deployment effects vary. A push with few or cheap isolate modules may never
create a meaningful queue. A push with more modules, larger or repeated import graphs, cold V8
work, or retries can sustain the configured fan-out long enough to overlap an unrelated query
recomputation wave. Ambient occupancy determines whether that overlap is absorbed or turns into a
short rejection burst. Changing only the adaptive-shed age can absorb a brief queue but does not
change this arrival pattern or execution cost; sustained pressure can move to hard expiry or queue
admission failure instead.

## Admission and ownership

`ApplicationFunctionRunner` creates one gate for its application when
`APPLICATION_MAX_CONCURRENT_DEGRADABLE_QUERY_LEADERS` is configured. The query cache and every
isolate analysis call from that runner receive clones of the same gate.

Degradable cache-miss leaders retain their existing contract:

1. inspect the cache without publishing a waiting leader;
2. attempt immediate gate acquisition;
3. return the typed degradable-capacity outcome when no permit is available;
4. recheck the cache after acquisition and retain the permit only for actual leader execution.

Each isolate module analysis future instead waits for one permit immediately before creating and
sending an `Analyze` isolate request. The queued request's response sender owns the reservation
through queueing, initial active-JavaScript admission, and worker execution. A terminal response
returns the reservation to the caller, which releases it before retry matching and backoff. If the
caller has disappeared after dispatch, failed response delivery releases the reservation only after
the analysis attempt produces its terminal result. A request discarded before dispatch releases it
without allocating a worker.
Node module analysis, source upload, schema and configuration evaluation, push commit, and the
complete deployment lifetime do not hold a permit.

The Tokio semaphore assigns released permits to queued asynchronous waiters before making them
available to a later immediate acquisition. Once at least one analysis future has entered the fair
wait queue, a stream of new degradable cache misses cannot repeatedly steal each released permit.
This supplies practical deployment liveness without a special minimum-one rule. It cannot make
progress until an existing permit holder completes, and it does not guarantee worker assignment
after gate admission.

`ANALYZE_CONCURRENCY` remains a per-call upper bound, not a reservation. With `N` concurrent
analysis calls, aggregate isolate analysis can reach
`min(shared capacity, N * ANALYZE_CONCURRENCY)`. A configured gate capacity below the per-call value
deliberately lowers effective analysis concurrency. The existing control-plane queue capacity
validation can remain based on the per-call upper bound because pacing never increases queue
fan-out.

Fair handoff deliberately favors already queued analysis over later immediate degradable
acquisition. Continuous concurrent deployment analysis can therefore consume the full elastic gate
and keep new degradable leaders in typed deferral. That is the work-conserving deployment-progress
tradeoff; it is not a reservation for degradable traffic.

## Capacity example and limits

Suppose shared-base worker capacity is 48, active-JavaScript capacity is 28, the elastic root-work
gate is 32, and analysis concurrency is 4. Before this integration, 32 degradable leaders and four
analysis attempts could directly present 36 roots to lower execution gates. With this integration,
four analysis reservations leave at most 28 root-work permits for degradable leaders. The active
gate independently applies non-preemptive protected and degradable service floors under
two-class contention; analysis uses the protected class.

This is intentionally not a mathematical bound on physical workers. One admitted query leader can
spawn separately scheduled dependencies, and dependency work retains its existing reserve. Normal
queries, mutations, actions, and non-degradable clients also bypass the elastic gate. The configured
cap must therefore leave measured headroom at the application query gate, isolate shared base, and
other lifetime limits. With active-JavaScript service floors enabled, the cap can exceed finite
active capacity because these gates bound different resources. Without service floors, any
configured finite active capacity must exceed the cap; `0` continues to mean unlimited.

The patch does not add an analysis-specific scheduler reservation. Analysis can still wait after
gate admission when protected work or dependencies occupy workers, when active-JavaScript permits
are full, or when CPU is saturated. Protected active admission gives analysis access to the shared
protected floor without prioritizing deployment work over other protected requests. A dedicated
analysis reservation requires separate evidence and policy.

## Metrics and controlled validation

The integration adds two bounded process metrics without application, module, deployment, or caller
labels and reuses the existing shared-capacity metric:

- `degradable_query_leader_capacity_info` reports the configured shared gate capacity from
  application-runner construction;
- `isolate_analysis_reservations_in_use_info` reports analysis permits currently held;
- `isolate_analysis_capacity_wait_seconds` records each analysis acquisition wait, including
  retries, immediate acquisitions, and canceled waiters.
- `active_javascript_capacity_info`, `active_javascript_occupancy_info`,
  `active_javascript_waiters_info`, and class-and-phase labels on active permit acquisition report
  the lower execution gate independently from the root-work gate.

These gauges are process-global. The direct occupancy-sum check below assumes the standard process
model with one live application runner. A process that intentionally hosts multiple live runners
needs capacity telemetry aggregated across those independent application-scoped gates before using
the same check.

Correlate these with:

- `degradable_query_leader_permits_in_use_info` and degradable admission outcomes;
- control-plane queue depth, oldest age, enqueue, dispatch, expiry, lane-full, and queue-full
  counters when the lane is enabled;
- active requests by scheduler class, active-JavaScript occupancy and wait by class and phase, and
  physical worker occupancy;
- analysis duration, deployment phase duration and outcome, CPU pressure, and module count.

A focused controlled deployment should hold representative degradable query pressure, then push a
bundle with enough independent isolate modules to reach the analysis upper bound. Validate all of
the following:

1. analysis reservations rise while degradable permits fall or new degradable leaders receive
   typed deferral;
2. with one live application runner, the sum of the two in-use gauges never exceeds the
   shared-capacity gauge;
3. analysis-capacity wait is bounded and at least one queued attempt acquires the next released
   permit;
4. control-plane queue arrivals are paced instead of appearing as the full analysis fan-out;
5. ordinary queue age and adaptive shedding decrease without a corresponding rise in control-plane
   hard expiry, lane-full, or shared queue-full rejection;
6. total analysis takes longer when the elastic class is busy but the deployment still completes
   within its outer deadline.

For an A/B comparison, use the same backend configuration and module bundle with and without this
integration patch. Changing the queue delay threshold in the same experiment would confound
capacity pacing with rejection timing. An unpaced image may be used only in a controlled
environment where the prior overload behavior is acceptable.

## Activation, rollout, and rollback

The integration is active whenever
`APPLICATION_MAX_CONCURRENT_DEGRADABLE_QUERY_LEADERS` is configured. It adds no second enable knob.
When the setting is absent, degradable clients retain normal query admission and isolate analysis
preserves its previous unpaced behavior.

Roll out with the existing cap unchanged, confirm the existing shared-capacity gauge, and run one
controlled push before relying on the behavior under normal deployment traffic. Do not raise
`ANALYZE_CONCURRENCY` at the same time. The immediate operational rollback is to restore the prior
backend image. Unsetting the cap also disables pacing, but simultaneously disables degradable-query
backpressure and is therefore not an equivalent isolated rollback.

Focused tests prove that analysis reservations reduce available degradable admission, a queued
analysis attempt receives a released permit before a new immediate degradable acquisition,
post-dispatch caller and sibling cancellation retain admission until each attempt produces its
terminal result, permit release restores capacity, and existing query-cache permit ownership
remains intact.

## Deliberately excluded alternatives

This patch does not add a dedicated worker pool, permanently idle worker, strict deployment
priority, dependency-reserve borrowing, deployment-wide query shutdown, module allowlist, larger
queue, longer deadline, or automatic `ANALYZE_CONCURRENCY` formula. Those choices either leave the
additive demand unchanged, weaken dependency liveness, introduce idle capacity, or require more
scheduler state than the current evidence justifies.
