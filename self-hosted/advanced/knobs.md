# Advanced Configuration and Tuning

There is a large number of detailed configuration options in
[knobs.rs](/crates/common/src/knobs.rs). These options are configurable via
environment variables. In order to tune your Convex instance at scale for your
workload, you may need to adjust these knobs. The stock Compose file passes
commonly overridden knobs from the host environment or Compose `.env` file.
Add other variables to the backend service's `environment` section in
[`docker-compose.yml`](../docker/docker-compose.yml).

## Backend memory feasibility and local Node lifetime

On Linux, startup checks a finite cgroup v2 `memory.max` before constructing the
database caches, isolate pool, or local Node executor. The configured budget is
the checked sum of:

- `(ISOLATE_MAX_USER_HEAP_SIZE + ISOLATE_MAX_HEAP_EXTRA_SIZE) *
  MAX_ISOLATE_WORKERS`;
- `ISOLATE_MAX_ARRAY_BUFFER_TOTAL_SIZE * MAX_ISOLATE_WORKERS`;
- `UDF_CACHE_MAX_SIZE` and `INDEX_CACHE_SIZE`;
- `SOURCE_MAP_CACHE_MAX_SIZE_BYTES`, `FUNRUN_INDEX_CACHE_SIZE`,
  `FUNRUN_MODULE_CACHE_SIZE`, and `FUNRUN_CODE_CACHE_SIZE`;
- `LOCAL_NODE_EXECUTOR_MAX_RSS_BYTES`; and
- `LOCAL_BACKEND_NATIVE_KERNEL_MEMORY_RESERVE_BYTES`, which defaults to 2 GiB.

The reserve covers allocations without a separate configured ceiling,
including Rust data structures, allocator retention, thread stacks, page
tables, and cgroup kernel memory. The backend rejects startup when this total
exceeds a finite cgroup limit. It skips the feasibility failure when the cgroup
memory controller is absent or unlimited. A malformed present controller file
is a startup error.

This calculation checks configuration feasibility; it is not current
allocation and does not prove that every cache's weight exactly matches RSS.
The Node budget entry is a planning allowance equal to a sampled Linux
direct-child graceful-retirement trigger, not a hard maximum. The child can
grow between samples and while active requests drain. Short-lived descendant
processes created while building dependencies are not sampled and remain part
of the native and kernel reserve.
The backend publishes each bounded component, the configured total, finite
limit availability, and finite-limit headroom as startup memory-budget metrics.
The periodic process, allocator, and cgroup metrics remain the source for
actual use.

The local Node executor also has independent lifetime controls:

- `LOCAL_NODE_EXECUTOR_MAX_OLD_SPACE_SIZE_MIB`, default `2048`;
- `LOCAL_NODE_EXECUTOR_MAX_RSS_BYTES`, default `3221225472` (3 GiB);
- `LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_MIN_RSS_BYTES`, default `2147483648` (2 GiB);
- `LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_GRACE_SECS`, default `60`;
- `LOCAL_NODE_EXECUTOR_MAX_GENERATION_AGE_SECS`, default `21600` (6 hours);
- `LOCAL_NODE_EXECUTOR_MAX_IMPORTED_SOURCE_PACKAGES`, default `1000`.

V8 old space excludes Buffers, native modules, executable code, and allocator
retention, so the RSS threshold must remain larger and is validated
independently. The age and lifetime-unique imported source-package thresholds
bound growth that disk-cache eviction cannot reclaim from Node's ESM module
graph. When any threshold is reached, the generation stops accepting new
requests and starts detached drain completion. The watchdog continues health
checks while in-flight requests finish; repeated health failure can preempt a
stuck drain. A healthy drained generation is terminated and reaped before the
next request starts a replacement generation.

The watchdog normally observes direct-child RSS roughly every one to two
seconds. An active invocation can delay healthy proactive drain completion
until its remaining Rust deadline expires, up to 605 seconds with the default
Node action timeout, and termination/reaping can take longer. It does not delay
the unhealthy-watchdog threshold. Non-Linux builds report RSS sampling as
unsupported and do not use the RSS trigger. A failed Linux sample marks RSS
telemetry unavailable and skips only that trigger for the iteration; age,
package, and unhealthy-generation checks continue. The RSS byte gauge retains
its last value while unavailable, so pair it with the RSS
telemetry-availability gauge.

The stock Compose file passes these controls through without setting
different defaults. Changing one requires a backend restart.

## Cgroup memory-pressure reclamation and admission

`LOCAL_BACKEND_MEMORY_RECLAMATION_ENABLED` defaults to `false`. When enabled on Linux, the backend
requires a readable finite cgroup v2 memory limit and enters internal reclamation when headroom
reaches `LOCAL_BACKEND_MEMORY_RECLAMATION_ENTER_HEADROOM_BYTES`, which defaults to 6 GiB. It clears
the pressure signal only after headroom reaches
`LOCAL_BACKEND_MEMORY_RECLAMATION_EXIT_HEADROOM_BYTES`, which defaults to 8 GiB.

On entry, the controller first evaluates an optional glibc trim and resamples cgroup headroom. If
pressure remains, it publishes one shared signal. When the bounded context-reuse patch is also
carried, idle isolates drop their fresh context, prune the reusable cache from its normal
one-probationary-plus-five-protected capacity to the two strongest protected entries, suppress new
reusable admission, and ask V8 to collect after removing roots. The local Node watchdog gracefully
retires its generation only after the signal has remained active for
`LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_GRACE_SECS` and a successful direct-child RSS sample reaches
`LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_MIN_RSS_BYTES`. The ordinary Node RSS limit remains the
higher-priority retirement reason.

`LOCAL_BACKEND_MALLOC_TRIM_ENABLED` also defaults to `false` and requires internal reclamation to
be enabled. On glibc builds, an eligible pressure sample calls `malloc_trim(0)` in bounded blocking
work only when `mallinfo2` reports at least `LOCAL_BACKEND_MALLOC_TRIM_MIN_FREE_BYTES` of logical
arena free space, default 1 GiB. Evaluation is limited by
`LOCAL_BACKEND_MALLOC_TRIM_COOLDOWN_SECS`, default 300 seconds. Logical free space and the Boolean
trim result do not prove resident bytes were released, so the backend records immediate signed
changes in process RSS, anonymous RSS, cgroup usage, cgroup anonymous memory, and allocator free
space, plus duration and page faults. Unsupported allocators report that trim is unavailable.

The reclamation exit threshold must exceed its entry threshold and remain below the cgroup limit.
When reclamation and external shedding are both enabled, both reclamation thresholds must preserve
more headroom than the corresponding shedding thresholds. This makes optional-memory reclamation
start before admission control.

`LOCAL_BACKEND_MEMORY_PRESSURE_SHEDDING_ENABLED` defaults to `false`. When
enabled on Linux, the backend requires a readable finite cgroup v2 memory limit
and samples current usage once per second. It rejects new non-dependency HTTP
requests with `503 BackendMemoryPressure` when headroom reaches
`LOCAL_BACKEND_MEMORY_PRESSURE_ENTER_HEADROOM_BYTES`, which defaults to 3 GiB.
Admission resumes only after headroom reaches
`LOCAL_BACKEND_MEMORY_PRESSURE_EXIT_HEADROOM_BYTES`, which defaults to 5 GiB.
The exit threshold must exceed the enter threshold and remain below the cgroup
limit.

The check runs before request-body handling and is repeated after the ordinary
HTTP concurrency wait. `/version` and `/metrics` remain available. Node action
callback paths carrying the callback-token header remain eligible for
downstream authentication so already-running actions can complete their
internal queries and mutations. Path and header presence are classified before
the token is authenticated, so this exemption is not a denial-of-service
boundary. The callback middleware still authenticates the token before handler
body extraction.

Only new HTTP requests pass through this gate. A new WebSocket handshake is
eligible for rejection, but frames on an established sync WebSocket,
already-admitted handlers, and scheduled, database, or other backend background
work continue. The site proxy uses the same state and rejects before forwarding
to the main listener.

An enabled reclamation or shedding controller is a safety dependency. Losing its cgroup source or a
runtime cgroup change that makes the thresholds invalid triggers controlled
backend shutdown instead of silently disabling shedding. The active state,
headroom, thresholds, transitions, failures, and rejected-request counts are
published as bounded metrics.

The stock Compose file passes the reclamation, trim, Node pressure, and shedding variables through
without enabling either controller. Changing one requires a backend restart. See
[`patches/backend_memory_resilience/README.md`](../../patches/backend_memory_resilience/README.md)
for the complete metric and failure contract.

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
metrics start after permit acquisition, so they do not bound this admission
wait. `http_admission_waiters_info{service_name,is_dependency}` reports actual
queued waiters, and
`http_admission_wait_seconds{service_name,is_dependency}` records waits ending
in handoff or cancellation. Immediate admissions are not histogram samples.
Use an upstream proxy or load balancer with bounded request queues and timeouts
when overload must be rejected within a fixed time.

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

`APPLICATION_MAX_CONCURRENT_DEGRADABLE_QUERY_LEADERS` is an optional elastic
capacity sized below the application query and isolate shared-base limits.
Degradable root query cache-miss leaders use immediate admission: when no permit
is available, the sync protocol keeps their previous values and returns typed
temporary pressure. Isolate module analysis fairly waits for one permit per
module attempt before entering the isolate queue. Analysis therefore borrows
from the same capacity instead of adding its configured fan-out on top of every
admitted degradable leader:

```text
degradable query leaders + isolate analysis attempts <= configured cap
```

For example, with a cap of 24 and `ANALYZE_CONCURRENCY=4`, four active module
analyses leave at most 20 permits for degradable leaders. With no analysis, all
24 remain available to degradable leaders. Normal queries, mutations, actions,
query dependencies, and Node module analysis do not use this gate. The bound is
on root query leaders and analysis attempts, not on every dependency a query
tree can spawn.

`ANALYZE_CONCURRENCY` is a per-call limit. `N` concurrent analysis calls can
collectively reach `min(24, N * 4)` in this example. Continuous queued analysis
can use the complete elastic gate and keep new degradable leaders in typed
deferral. The separate active-JavaScript class minimums described below provide
execution liveness after degradable leaders are admitted; they do not change
this root-work gate.

The analysis wait is fair with existing permit holders and queued analysis
attempts. It does not reserve an isolate worker, borrow dependency overflow, or
guarantee progress while all underlying execution resources remain occupied.
Unset the knob to preserve the original unpaced analysis behavior. See
[`patches/deployment_analysis_pacing/README.md`](../../patches/deployment_analysis_pacing/README.md)
for sizing, telemetry, and rollout guidance.

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

`ISOLATE_QUEUE_SIZE` is the shared base capacity of the finite external isolate
queue and must be nonzero. Dependencies can use
`ISOLATE_DEPENDENCY_WORKER_RESERVE` additional queue entries. Other requests
cannot use that extra capacity. Once the applicable capacity is full, another
enqueue fails immediately. Direct internal nested-UDF callbacks bypass this
queue but still share its scheduler's physical workers and active permits.

The default isolate queue policy remains generic CoDel. It uses FIFO while idle,
LIFO while congested, and the existing CoDel deadlines. Set
`ISOLATE_QUEUE_DELAY_CONTROL_ENABLED=true` to opt into the isolate-only
lane-aware policy. That policy keeps FIFO queue order, selects the oldest
request eligible under worker and client constraints, never adaptively sheds a
dependency, and hard-expires every lane at a finite maximum age.

The lane-aware timing knobs are:

- `ISOLATE_QUEUE_DELAY_TARGET_MILLIS`, default `150`;
- `ISOLATE_QUEUE_DELAY_INTERVAL_MILLIS`, default `1000`;
- `ISOLATE_QUEUE_DELAY_SHED_THRESHOLD_MILLIS`, default twice the configured
  target (`300` with the default target);
- `ISOLATE_QUEUE_HARD_MAX_AGE_MILLIS`, default `5000`.

The target and interval must be nonzero. The adaptive-shed threshold must be
greater than the target, hard maximum age must be greater than the
adaptive-shed threshold, and every duration must fit the runtime timer. Numeric
values must contain only ASCII decimal digits; empty, signed, malformed,
non-Unicode, overflowed, and inconsistent settings fail startup. The timing
values are validated even when lane control is disabled.

`ISOLATE_CONTROL_PLANE_LANE_ENABLED=true` classifies isolate module analysis,
schema evaluation, auth configuration evaluation, app definition evaluation,
and component initializer evaluation into a `control_plane` lane. It does not
match application module or component names. The lane remains in the same FIFO
and uses only shared-base queue and worker capacity. It does not use dependency
reserve, receive dispatch priority, or reserve a worker. It is exempt from
adaptive delay shedding but retains a finite hard queue deadline.

The control-plane settings are:

- `ISOLATE_CONTROL_PLANE_LANE_ENABLED`, default `false`;
- `ISOLATE_CONTROL_PLANE_QUEUE_CAPACITY`, default `16`;
- `ISOLATE_CONTROL_PLANE_HARD_MAX_AGE_MILLIS`, default `30000`.

The capacity is a positive sub-cap inside `ISOLATE_QUEUE_SIZE`, not reserved
capacity. The deadline bounds time from external queue enqueue through initial
active-permit acquisition, not execution or the complete push. All three
settings are parsed and intrinsically validated when the lane is disabled.
`ANALYZE_CONCURRENCY` must also be greater than zero because
zero would stall isolate analysis before enqueue. When enabled, lane-aware
delay control must also be enabled, the lane capacity must be at least
`ANALYZE_CONCURRENCY` and no greater than `ISOLATE_QUEUE_SIZE`, and the lane
deadline must be greater than the ordinary hard queue age. A queued control-
plane request is discarded before worker assignment if its response receiver
has already closed. Immediate physical-queue and lane-cap admission errors
return directly; the isolate client's bounded retry loops cover only errors
received after successful enqueue.

`isolate_control_plane_lane_enabled_info{pool_name}` is `1` only when this
classification is effective and `0` when the five request variants retain
ordinary queue behavior. When lane-aware queueing is active, capacity and
deadline metrics report parsed settings even when classification is disabled;
those series are absent on the legacy queue path. Use the enabled gauge for
rollout confirmation. See
[`patches/isolate_queue_control/README.md`](../../patches/isolate_queue_control/README.md)
for the exact classification, capacity, deadline, metrics, and rollout contract.

Lane control publishes pool-scoped queue policy, configuration, capacity,
depth, oldest age, dispatch sojourn, overload, rejection, and ineligibility
metrics. To roll back only queue behavior, set
`ISOLATE_CONTROL_PLANE_LANE_ENABLED=false` to return analysis and evaluation to
ordinary queue behavior, or set `ISOLATE_QUEUE_DELAY_CONTROL_ENABLED=false`
after disabling the control-plane lane to restore generic CoDel. Worker
reserves, application and HTTP admission, action caps, and HTTP action context
reuse are unchanged. See
[`patches/isolate_queue_control/README.md`](../../patches/isolate_queue_control/README.md) for
the exact policy and rollout guidance.

A one-worker pool cannot run a function and a separately scheduled descendant
at the same time. Use at least two workers and a reserve of at least one for
applications with these call patterns. The reserve is finite; deep chains or
parallel fanout can still consume every worker and queue entry.

## `ISOLATE_CONTEXT_CACHE_PROTECTED_RESIDENTS_PER_ISOLATE`

The context-reuse patch retains one probationary reusable context plus a configurable
frequency-protected segment in each isolate worker. This setting controls the protected segment and
defaults to `5`, producing the default `5+1` layout. Values below `2`, malformed values, addition or
frequency-aging overflow, and values above the platform permit bound fail backend startup. The
minimum preserves the separate pressure layout, which removes the probationary resident and keeps
at most the two strongest protected entries.

Changing this value also changes the structural maximum used to validate
`ISOLATE_CONTEXT_CACHE_MAX_RESIDENTS`. The maintained Docker Compose file passes the setting through
without overriding the backend default. Changing it requires a backend restart.

## `ISOLATE_CONTEXT_CACHE_MAX_RESIDENTS`

By default, the scheduler-pool resident-token capacity is the configured per-isolate protected
capacity plus one probationary resident, multiplied by `MAX_ISOLATE_WORKERS`. Set
`ISOLATE_CONTEXT_CACHE_MAX_RESIDENTS` to a smaller positive decimal value when the backend memory
allocation cannot support full population. Values greater than the structural maximum, zero,
malformed values, and multiplication overflow fail backend startup.

The limit applies to database-UDF, ordinary-action, and HTTP-action reusable contexts together. A
token remains owned while a cache hit is in flight, so a returning context cannot lose its capacity
to another worker. The setting does not change V8 user or extra heap limits, does not enable context
reuse, and does not make mutable module state safe. The maintained Docker Compose file passes the
variable through without a default. Changing it requires a backend restart. See
[`patches/context_reuse/README.md`](../../patches/context_reuse/README.md)
for admission, memory pressure, metrics, rollout, and rollback behavior.

## `FUNRUN_ISOLATE_ACTIVE_THREADS`

This caps isolates actively executing JavaScript. `0` means unlimited. A
request can release this permit while waiting for asynchronous work, so this is
not the same as assigned isolate workers. Initial external requests remain
bounded by their original queue deadline. A request that releases its permit
for asynchronous work retains the resumption phase when it later reacquires
the permit.

Set `FUNRUN_ISOLATE_PROTECTED_ACTIVE_THREADS_MIN` and
`FUNRUN_ISOLATE_DEGRADABLE_ACTIVE_THREADS_MIN` together to positive values to
enable work-conserving service floors for protected and degradable JavaScript.
With these floors enabled, dependency work receives the next available permit
because completing it releases an isolate-holding ancestor. The service class
follows an execution across every release and reacquisition.
Only an admitted degradable root query cache-miss leader uses the degradable
class. Cache hits, followers, ordinary roots, actions, deployment analysis, and
other backend work use the protected class unless backend-derived ancestry
classifies the request as a dependency.

The floors apply when both protected and degradable work are runnable and no
dependency grant is pending. They do not preempt JavaScript that is already
active and do not keep permits idle. Either class borrows all capacity that the
other class does not need. After both floors are met, elastic occupancy is
balanced between the two classes; resumptions precede initial starts within a
selected class.

Both minimums are `0` by default, which preserves the ordinary two-phase
admission behavior. Positive minimums require a finite
`FUNRUN_ISOLATE_ACTIVE_THREADS`, require
`APPLICATION_MAX_CONCURRENT_DEGRADABLE_QUERY_LEADERS`, and their sum cannot
exceed the active-thread total. The degradable minimum cannot exceed the
degradable leader cap. With these minimums enabled, the leader cap can exceed
the active-thread total because the leader cap bounds admitted query-tree
lifetime while the active gate bounds runnable JavaScript occupancy.

For example, a total of `28`, protected minimum `4`, and degradable minimum
`14` leaves ten elastic permits. Under sustained two-class contention the
elastic permits are shared, producing approximately nine protected and
nineteen degradable active executions after non-preemptive convergence. If
either class is idle, the other can use all 28 permits. Use the total to
control CPU oversubscription and select the minimums from measured progress,
wait time, CPU headroom, and throttling.
