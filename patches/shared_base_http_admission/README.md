# Shared-Base HTTP Admission

## Summary

This self-hosted patch makes both local Convex HTTP gates use the strict
`HTTP_SERVER_MAX_CONCURRENT_REQUESTS` setting and adds bounded dependency-only overflow to the main
backend gate through `HTTP_SERVER_DEPENDENCY_RESERVE`.

All requests consume shared base admission while it is available. Only authenticated Node callback
routes carrying the callback-token header may raise main-service occupancy above that base. The
patch does not prioritize application routes, bound an external reverse proxy, or replace
application and isolate limits.

The isolate and application dependency-capacity companion is documented in
[`dependency_capacity/README.md`](../dependency_capacity/README.md). The patches are independently selectable, but a
Node action chain under saturation normally needs callback headroom at both the HTTP and isolate
stages.

## Motivation

The stock local backend historically used unrelated fixed gates: `128` requests for the main API on
port `3210` and `4` for the HTTP-actions proxy on port `3211`. They were not selected from host
capacity and could not be configured together. Replacing them with the common upstream default
without an explicit operator value can also admit much more work than the old local limits.

Node actions add a liveness constraint. Their callbacks re-enter the main API while an ancestor may
retain application or isolate capacity. If independent requests consume every HTTP permit, the
callback cannot enter and the ancestor cannot release its downstream resources.

## Admission model

Let:

- `H = HTTP_SERVER_MAX_CONCURRENT_REQUESTS`, the total permits in each local HTTP service;
- `D = HTTP_SERVER_DEPENDENCY_RESERVE`, callback-only overflow in the main service;
- `H - D`, the main-service shared base.

The main API and port `3211` proxy have separate gates with the same `H`. A request sent to port
`3211` retains a proxy permit while its forwarded `/http` request holds a main-service permit. A
reverse proxy may instead route directly to `/http` on port `3210`, which uses only the main gate.

Every main-service request consumes shared base while it has room. Only `/api/actions/*` requests
carrying the callback-token header may raise occupancy above `H - D`, up to `H`. The port `3211`
proxy has no dependency overflow because Node callbacks target the main API origin.

This classification occurs before route middleware authenticates the token. The token still
protects callback operations, but the reserve is not a denial-of-service security boundary: a
public caller can forge the header and compete for the finite overflow permits. Restrict callback
routes at the external proxy to backend or Node-executor sources when the network layout supports
it, and keep public admission bounded regardless.

All callback operations share `D`. A callback retains its HTTP permit while it waits at later
application, isolate, or database stages. Nested chains and parallel fan-out therefore need enough
HTTP headroom for their measured concurrent callbacks. This finite reserve cannot guarantee
arbitrary recursion.

## Permit lifetime and waiting

The concurrency middleware releases a permit when the service future returns the HTTP response
head. A streaming HTTP action body and its producing isolate work can outlive both main and proxy
permits. These settings do not bound concurrently streaming response bodies.

Requests above a gate wait for a permit. The current request instrumentation and
`HTTP_SERVER_TIMEOUT_SECONDS` layer start after permit acquisition, so neither measures nor bounds
that pre-permit wait. Use Caddy or another upstream load balancer for finite external queueing,
route-specific bulkheads, and caller-visible overload timeouts.

Application `APPLICATION_MAX_CONCURRENT_*`, isolate queue, worker, action-shell, active-JavaScript,
and database limits remain independent downstream gates. Raising `H` can simply create a deeper
wait at one of those stages. It does not create CPU or isolate throughput.

## Configuration

The self-hosted Compose template passes through:

- `HTTP_SERVER_MAX_CONCURRENT_REQUESTS`;
- `HTTP_SERVER_DEPENDENCY_RESERVE`.

If `H` is unset, both local services use the backend default `1024`. This is higher than the old
local fixed limits, so operators should normally select an explicit value from measured workload
and downstream capacity.

`H` must be a positive integer no larger than Tokio's supported semaphore bound. `D` defaults to
`1`, may be `0` to disable callback overflow, and must be smaller than `H`. Empty, malformed,
non-Unicode, signed, overflowing, zero total, and inconsistent values fail before runtime and
database initialization rather than falling back.

If the published host `PORT` differs from `3210`, set `CONVEX_CLOUD_ORIGIN` to an API address that
the backend container can reach. The backend listener remains on internal port `3210`;
`127.0.0.1:$PORT` inside the container does not reach a differently published host port.

A reverse proxy on `CONVEX_CLOUD_ORIGIN` must preserve the callback-token header and the isolate
ancestry header used by the dependency-capacity patch. Its callback route must also leave enough
headroom; the backend cannot reserve capacity in an external proxy.

## Metrics

Use the HTTP metrics as a stage-specific admission view:

- total and base concurrent-request gauges for the main service;
- `http_admission_waiters_info{service_name,is_dependency}` for requests that entered permit wait;
- `http_admission_wait_seconds{service_name,is_dependency}` for waits ending in handoff or
  cancellation;
- request status and duration after permit acquisition.

Each HTTP service initializes the dependency and non-dependency waiter gauges at zero. The gauge is
a sampled occupancy signal and can miss a wait that starts and finishes between scrapes. The wait
histogram count, sum, and buckets retain every completed wait, including waits shorter than the
scrape interval. Immediate admission produces no wait sample. Caddy duration includes any upstream
transport wait, backend execution, and response transfer, while Convex request duration begins
after HTTP permit acquisition. Compare the two rather than attributing all proxy latency to Convex
execution.

Do not infer saturation from one positive queued or waiter sample. A normal asynchronous handoff can
briefly report waiting. Capacity conclusions require wait duration, running occupancy near the
configured limit, timeout or rejection, and downstream queue and CPU evidence.

Labels remain bounded by service and dependency class. Do not add route, module, client, tenant,
request, or deployment identifiers to backend metric labels. Route policy belongs at the proxy.

## Interaction with dependency capacity

[`dependency_capacity/README.md`](../dependency_capacity/README.md) supplies ancestry propagation and bounded
overflow at application, cache, isolate queue, and worker stages. `D` and
`ISOLATE_DEPENDENCY_WORKER_RESERVE` are intentionally separate because their totals and permit
lifetimes differ.

An HTTP callback can pass its reserve and still wait for an application permit, isolate worker,
active-JavaScript permit, database connection, or external provider. Conversely, isolate reserve
cannot help a callback that never entered the HTTP service. Size and observe each stage separately.

HTTP-action context reuse can lower service time after admission but does not alter gate ownership.
Lane-aware isolate queue control and degradable query admission operate downstream and do not
replace upstream proxy bulkheads.

## Rollout and rollback

1. Record route arrival rate, Caddy and Convex latency, HTTP wait, application wait, isolate queue,
   action-shell occupancy, CPU, and error baselines.
2. Set explicit `H` and `D`, validate the rendered Compose configuration, and verify callback-header
   preservation at the reverse proxy.
3. Restart one controlled backend population and confirm the resolved HTTP capacity metrics.
4. Exercise direct API traffic, port `3211` or direct `/http` traffic as deployed, and nested Node
   callbacks together.
5. Confirm finite callback progress without sustained ordinary HTTP wait, downstream shedding, or
   CPU saturation.
6. Change route-specific Caddy limits separately unless a combined policy trial is explicitly
   intended and has a rollback order.

Rollback restores the prior image and both HTTP values. Lower external proxy concurrency before
lowering backend admission if the proxy configuration assumes the larger gate. Removing the
callback reserve can reintroduce Node callback deadlock under HTTP saturation.

## Verification boundary

Focused tests cover strict total and reserve parsing, invalid relationships, shared-base FIFO,
dependency-only overflow, cancellation before and after permit handoff, callback classification,
both local services, constructor bounds, zero-initialized waiter labels, and histogram observation
of a wait that returns the sampled waiter gauge to zero. Production verification should use real
callback and HTTP-action traffic; a synthetic stress fixture is not required.

## Rejected alternatives

- A single fixed local limit cannot represent different host sizes or workloads.
- Giving callback traffic an unbounded or external priority queue weakens overload bounds.
- Classifying by application route or module name makes the backend deployment-specific.
- Treating every HTTP action as a dependency spends liveness headroom on independent work.
- Increasing `H` to remove proxy wait can move the backlog into application or isolate queues and
  increase failures without improving throughput.
- Relying only on `HTTP_SERVER_TIMEOUT_SECONDS` does not bound the current pre-permit wait because
  that timeout begins after admission.
