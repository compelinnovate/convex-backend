# Advanced Configuration and Tuning

There is a large number of detailed configuration options in
[knobs.rs](/crates/common/src/knobs.rs). These options are configurable via
environment variables. In order to tune your Convex instance at scale for your
workload, you may need to adjust these knobs. You will have to set these
environment variables by adding them to your `docker-compose.yml` file. Commonly
overriden knobs are listed in the `env` section of the
[`docker-compose.yml`](../docker/docker-compose.yml)

## `HTTP_SERVER_MAX_CONCURRENT_REQUESTS`

This limits concurrent application requests admitted by the backend HTTP
server. It also applies to the local HTTP actions proxy exposed on port `3211`.
Requests through that proxy also pass through the main backend gate on port
`3210`. This is an outer HTTP limit; the `APPLICATION_MAX_CONCURRENT_*` knobs
and isolate worker limits still apply independently. The `/version` and
`/metrics` service routes retain their existing admission bypass.

If the variable is unset, both services use the common default of `1024`. This
replaces the former local-backend limits of `128` on port `3210` and `4` on port
`3211`, so adopting this version without setting the variable raises admission.
The stock Compose entry does not set another default: an unset host variable is
omitted from the container environment. An empty, malformed, zero, or
larger-than-supported value is passed to the backend and fails startup before
runtime and database initialization.

The two services have separate admission gates. A request sent to port `3211`
uses a proxy permit and a main backend permit while it is forwarded. A reverse proxy
can instead rewrite HTTP action traffic to `/http` on port `3210`; that route
uses only the main backend permit. Raising the limit can increase load and
queueing. Without dependency-aware isolate scheduling, it can also let actions
in the default Convex runtime and HTTP actions occupy every isolate worker while
waiting for their `ctx.runQuery` or `ctx.runMutation` calls.

If the published `PORT` is changed from `3210`, set
`CONVEX_CLOUD_ORIGIN` explicitly to an API address that both clients and the
backend container can reach. The backend listener remains on internal port
`3210`; the Compose default `127.0.0.1:$PORT` does not reach that listener from
inside the container when `$PORT` differs from `3210`, so Node action callbacks
would fail.

Each permit covers request handling only until that service returns the HTTP
response head. A streaming HTTP action body, and the isolate work producing it,
can outlive the main and proxy permits. These knobs therefore do not bound the
number of response bodies being streamed concurrently.

Requests above the limit enter an unbounded in-process permit wait instead of
being rejected immediately. `HTTP_SERVER_TIMEOUT_SECONDS` and request handling
metrics start after permit acquisition, so they do not bound or measure this
admission wait. Use an upstream proxy or load balancer with bounded request
queues and timeouts when overload must be rejected within a fixed time.

## `HTTP_SERVER_DEPENDENCY_RESERVE`

This creates dependency-only overflow above shared base capacity on port
`3210`, so Node action callbacks can enter while their parent requests retain
base permits. All requests, including callbacks, consume the shared base while
it has room. Only `/api/actions/*` requests carrying the action callback-token
header may raise total occupancy above the base. The default is `1`; `0`
disables the overflow. The value must be a nonnegative integer smaller than
`HTTP_SERVER_MAX_CONCURRENT_REQUESTS`. The port `3211` proxy does not use this
reserve because Node callbacks target the main API origin.

All Node callback operations share this finite reserve. A callback retains its
HTTP permit while it waits at later application or isolate stages, so callback
chains and parallel fanout need enough HTTP reserve for their expected depth
and concurrency.

An empty, malformed, non-Unicode, negative, or too-large reserve fails startup
before runtime and database initialization instead of falling back to the
default.

The callback path and presence of the callback-token header are classified
before the token is authenticated. The token still protects callback
operations, but an untrusted client can forge the header and compete for
reserved permits.
Restrict `/api/actions/*` at the public reverse proxy to the backend or Node
executor network source when the network layout permits it. A reverse proxy on
`CONVEX_CLOUD_ORIGIN` must preserve the callback-token header and, when the
scheduler dependency-reserve patch is installed, the isolate-ancestry header
used by downstream scheduler classification. It must also leave callback
headroom because the backend cannot reserve capacity in an external proxy.

## `APPLICATION_MAX_CONCURRENT_*` knobs

You can increase the max concurrency on your self-hosted instance with these
environment variables. Note that increasing concurrency will increase load on
your system and after a certain threshold, performance will degrade. You will
have to tune parameters based on your own hardware and workload.

Each nonzero application limit has dependency-only overflow carved out of its
configured total. The effective reserve is the smaller of
`ISOLATE_DEPENDENCY_WORKER_RESERVE` and one less than the application limit.
Every request class consumes shared base capacity first. At the query, mutation,
and default-runtime action limits, only a request that unblocks a function
retaining an isolate worker may raise occupancy above that base. Nested Node
actions receive the same treatment at the Node action limit because their
parent retains a permit from that limit.

## Isolate worker and queue knobs

`MAX_ISOLATE_WORKERS` is the total number of isolate workers assigned requests
at once. `ISOLATE_DEPENDENCY_WORKER_RESERVE` is dependency-only overflow carved
out of that total: all requests share
`MAX_ISOLATE_WORKERS - ISOLATE_DEPENDENCY_WORKER_RESERVE` base occupancy, and
only work that unblocks an isolate-holding ancestor can use the remainder. The
reserve must be smaller than the worker total, and the worker total must be
nonzero.

`MAX_ISOLATE_ACTION_WORKERS` separately caps independent default-runtime and
HTTP actions that retain workers. `0` derives this cap from shared base worker
capacity. An explicit value cannot exceed base capacity. Queries, mutations,
and child actions that unblock an ancestor are not subject to this cap.

`ISOLATE_QUEUE_SIZE` is the shared base capacity of the bounded CoDel queue and
must be nonzero. Dependencies can use
`ISOLATE_DEPENDENCY_WORKER_RESERVE` additional queue entries. Once both parts
are full, another enqueue fails immediately instead of waiting in an unbounded
queue.

A one-worker pool cannot run a function and a separately scheduled descendant
at the same time. Use at least two workers and a reserve of at least one for
applications with these call patterns. The reserve is finite; deep chains or
parallel fanout can still consume every worker and queue entry.

## `FUNRUN_ISOLATE_ACTIVE_THREADS`

This caps isolates actively executing JavaScript. `0` means unlimited. A
request can release this permit while waiting for asynchronous work, so this is
not the same as assigned isolate workers and does not provide dependency-only
overflow. Use it to control CPU oversubscription, and raise it only after
checking backend CPU headroom and throttling.
