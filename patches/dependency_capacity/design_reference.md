# Self-hosted Runtime Admission and Scheduler Knobs

> Detailed design and measurement reference. This is not a separate adoption unit. Use
> [`README.md`](README.md) and
> [`shared_base_http_admission/README.md`](../shared_base_http_admission/README.md) for current operator decisions,
> prerequisites, rollout, and rollback. This reference preserves the original combined rationale,
> benchmark tables, stage interaction analysis, and coverage matrix.

This document covers two patches that can be applied independently:

- the runtime admission patch makes the self-hosted HTTP gates configurable,
  adds bounded dependency-only HTTP callback overflow, and passes those
  settings through Compose;
- the scheduler dependency-reserve patch tracks whether a request unblocks an
  isolate-holding ancestor independently from whether it can block on a
  descendant, then adds bounded dependency-only overflow above shared
  application and worker base capacity plus external CoDel queue capacity. It
  composes with upstream's separate priority path for direct nested UDFs.

The patches have no code dependency. The admission patch is useful without the
scheduler patch for workloads that do not saturate the isolate pool with actions
waiting on `ctx.runQuery` or `ctx.runMutation`. For child requests admitted to
the isolate queue, the scheduler patch removes that worker-capacity inversion at
any HTTP admission limit when the isolate pool has at least two workers, and
does not require the admission patch.

They are operationally related. Raising HTTP admission allows more
Convex-runtime and HTTP actions to enter the backend at once, so it can expose
the capacity inversion more readily when the scheduler patch is absent. The
admission patch also changes the unset local-backend limits from `128` on port
`3210` and `4` on port `3211` to the common `1024` default. An operator adopting
only the admission patch should set an explicit HTTP gate and verify that nested
V8 and HTTP actions continue to make progress under saturation. Adopting only
the scheduler patch preserves the upstream HTTP gates while removing the isolate
worker-capacity inversion for admitted action query/mutation callbacks. The
measurements in this document used both patches, with HTTP action context reuse
enabled where stated.

Node.js action callbacks re-enter the main HTTP server on port `3210` while the
parent request can retain its base HTTP permit. The main backend HTTP gate
provides `HTTP_SERVER_DEPENDENCY_RESERVE` dependency-only overflow for
`/api/actions/*` requests carrying the callback-token header. A Node action that
has an isolate-holding ancestor also carries an ancestry marker alongside its
authenticated callback token for downstream application and isolate scheduling;
the port `3211` development proxy does not need that reserve because Node
callbacks target the main API origin. This HTTP reserve is separate from
`ISOLATE_DEPENDENCY_WORKER_RESERVE` because the HTTP and isolate stages have
different totals and can require different headroom. A public
`CONVEX_CLOUD_ORIGIN` can send callbacks through Caddy or another reverse proxy,
so that external route must preserve the ancestry header on trusted callbacks
and the callback-token header on every callback, and it must not impose a
smaller, unreserved limit. If the published `PORT` changes, the origin must be
set explicitly to an address reachable from the backend container;
`127.0.0.1:$PORT` does not reach the listener fixed at internal port `3210`.

The outer HTTP gate recognizes the callback path and presence of the callback-
token header before the token is authenticated by the route middleware. The
callback token still protects application data, but the HTTP reserve is not a
denial-of-service security boundary: an untrusted caller can forge the header
and compete for those permits. On a public deployment, restrict
`/api/actions/*` at the reverse proxy to the backend or Node executor network
source when the network layout makes that possible, and apply bounded upstream
admission regardless. Confirm that legitimate Node callbacks still reach the
main backend after adding such a rule.

Together, the two patches make the self-hosted backend's runtime admission gates
and isolate scheduler policy explicit:

- `HTTP_SERVER_MAX_CONCURRENT_REQUESTS` is parsed strictly, must be nonzero, and
  must fit the HTTP server's supported admission range.
- The local backend HTTP service and the port `3211` HTTP actions proxy use the
  same HTTP concurrency knob.
- The main HTTP gate provides `HTTP_SERVER_DEPENDENCY_RESERVE`
  dependency-only overflow permits above shared base occupancy for Node
  callbacks.
- The self-hosted Compose file passes both HTTP settings into the backend
  container.
- The application function gates, isolate queue, and isolate worker scheduler
  provide dependency-only overflow above shared base capacity according to
  `ISOLATE_DEPENDENCY_WORKER_RESERVE`.
- Scheduler metrics expose enqueue, dispatch, expiry, rejection, active request
  classes, and queue and worker reserve use.

The production failures behind these patches were a small self-hosted backend
being constrained by arbitrary admission gates and upstream defaults. The
upstream defaults are `MAX_ISOLATE_WORKERS=300` and
`FUNRUN_ISOLATE_ACTIVE_THREADS=0`, which allow up to 300 assigned workers with
no separate active-execution permit cap. The old local backend also had fixed
HTTP gates: `128` for the main backend service and `4` for the port `3211`
proxy. Those limits were not configurable together and were not selected for the
backend's workload or CPU allocation.

The more subtle failure was a scheduler capacity inversion. HTTP actions hold an
isolate worker while they wait for `ctx.runQuery` or `ctx.runMutation`. Those
nested UDFs are submitted back into the same isolate worker pool. If the HTTP
actions occupy all workers, the queries or mutations they await cannot start.
CoDel expiry then shows up as HTTP `503`, even when MySQL, disk I/O, and host
memory are not saturated.

## Admission Patch

`HTTP_SERVER_MAX_CONCURRENT_REQUESTS` now fails startup for malformed,
non-Unicode, zero, or larger-than-supported values. The upper bound is Tokio's
`Semaphore::MAX_PERMITS`, retained as the supported service-construction bound
and enforced before either local HTTP gate is built. The old generic parser
could warn and fall back to the default `1024`, which is unsafe for an
operator-selected self-hosted admission gate. The local backend resolves and
fully validates the value before initializing its runtime, database connection,
or application.

The local backend now uses `HTTP_SERVER_MAX_CONCURRENT_REQUESTS` for both HTTP
entry points:

- the main backend service on port `3210`;
- the local HTTP actions proxy on port `3211`.

The services have independent admission gates with the same resolved limit. A
request through port `3211` holds a proxy permit while its forwarded request
holds a main backend permit. Direct API traffic and Node.js action callbacks share only
the main backend gate. A reverse proxy can also rewrite HTTP action traffic to
`/http` on port `3210`; that direct route bypasses the port `3211` gate and
uses only the main backend gate. The `APPLICATION_MAX_CONCURRENT_*` limits and
the isolate worker limits are additional, independent caps on work after HTTP
admission.

The main service has a shared base of
`HTTP_SERVER_MAX_CONCURRENT_REQUESTS - HTTP_SERVER_DEPENDENCY_RESERVE` permits.
Every request class consumes this base while it has room. Only callback paths
carrying the callback-token header may raise total occupancy above the base, up
to the configured total.
The default reserve is `1`; `0` disables the overflow. Empty,
malformed, non-Unicode, negative, or too-large values, including values at or
above `HTTP_SERVER_MAX_CONCURRENT_REQUESTS`, fail startup instead of falling
back to the default. Node action callbacks can use the full HTTP total.
The port `3211` proxy has no dependency reserve because Node callbacks target
the main API origin.

The HTTP reserve is shared by all Node callback operations. A callback keeps
its HTTP permit while it waits at later application or isolate stages, so one
stalled callback can occupy an overflow slot. Callback chains or parallel
fanout that require more than `HTTP_SERVER_DEPENDENCY_RESERVE` simultaneous
HTTP permits can still stall or time out; size HTTP headroom separately from
the isolate worker reserve.

The HTTP middleware releases each permit when the service future returns the
HTTP response head. A streaming HTTP action body, and isolate work that
continues producing it, can therefore outlive both the main and proxy permits.
The HTTP knob does not bound concurrently streaming response bodies.

Requests above the limit enter an unbounded in-process permit wait rather than
being shed immediately. Request instrumentation and the current
`HTTP_SERVER_TIMEOUT_SECONDS` layer are inside the concurrency layer, so neither
request metrics nor that timeout include permit wait time. Operators that need
a bounded overload response must enforce queue and timeout policy at an upstream
proxy or load balancer.

Before the admission patch, direct local-backend metrics showed the old proxy
cap as `backend_http_proxy=4`. With the patch, the proxy follows the same
`HTTP_SERVER_MAX_CONCURRENT_REQUESTS` value as the main backend service. The
self-hosted Compose file now passes that environment variable through instead of
requiring operators to edit Compose by hand. The key-only Compose entry does not
supply its own default: when the variable is unset, the container uses the
common `1024` default. Empty, malformed, or out-of-range values for either HTTP
admission setting are forwarded and fail startup.

The self-hosted backend has one in-process function runner inside the backend
process. `MAX_ISOLATE_WORKERS` caps the isolate worker threads allocated by that
runner and the isolate requests assigned to workers concurrently. It does not
cap external requests waiting in the bounded isolate queue or direct nested UDF
callbacks waiting in upstream's internal priority channel.
`ISOLATE_DEPENDENCY_WORKER_RESERVE` sets both dependency-only overflow above the
shared worker base and the additional external dependency-only queue capacity.
`ISOLATE_QUEUE_SIZE` remains the external shared base queue capacity. The worker
threads share the backend container's CPU quota with the HTTP and application
runtime.

`FUNRUN_ISOLATE_ACTIVE_THREADS` caps active isolate execution permits. A value
of `0` means unlimited. A request can release the active permit while waiting on
async work, so this is not the same as requests in flight. It is still the main
CPU oversubscription control for V8 work.

The stock self-hosted Compose file passes through `MAX_ISOLATE_WORKERS`,
`ISOLATE_DEPENDENCY_WORKER_RESERVE`, `MAX_ISOLATE_ACTION_WORKERS`,
`ISOLATE_QUEUE_SIZE`, and `FUNRUN_ISOLATE_ACTIVE_THREADS`. Unset values continue
to use backend defaults.

## Scheduler Dependency-Reserve Patch

The scheduler records three independent request properties:

- `unblocks_ancestor`: this request was submitted by a function that is still
  retaining an isolate worker;
- `can_block_on_descendant`: this request can submit another separately
  scheduled function while retaining its own worker;
- `is_isolate_action`: this is a V8 or HTTP action subject to the independent
  action-shell cap.

Queries and mutations are `can_block_on_descendant` when
`SUBFUNCTIONS_IN_SAME_ISOLATE=false`; actions always have that property. A
nested query, mutation, or action can be both `unblocks_ancestor` and
`can_block_on_descendant`. Metrics expose the four combinations as
`independent`, `descendant_holder`, `dependency`, and
`dependency_descendant_holder`. These are scheduler roles, not browser, IP, or
end-user fairness classes.

Only `unblocks_ancestor` affects access to worker overflow and external CoDel
queue overflow. `can_block_on_descendant` is retained for accurate role metrics
and to represent nested functions that have both properties; it does not change
eligibility or ordering. `is_isolate_action` affects only the independent
action cap. There is no additional first-caller or role-combination policy.

Upstream splits scheduler ingress by ownership boundary. A direct nested UDF
callback enters an internal unbounded channel, is polled before the external
stream, has no CoDel deadline, and requests its active-JavaScript permit at high
priority before worker assignment. The scheduler buffers internal arrivals,
discards closed callers, and selects the oldest callback eligible in one worker
snapshot. It can therefore skip a per-client-ineligible callback without letting
newer eligible callbacks from other clients lose otherwise usable physical
capacity. Externally submitted requests retain bounded admission. Its default
policy retains CoDel's FIFO-while-idle and
LIFO-while-congested behavior; the opt-in lane policy selects the oldest eligible
request and preserves FIFO among eligible work. Both policies let externally
propagated dependencies use `Q + R` without allowing ordinary work above `Q`.
Both ingress paths then use the same worker scheduler, global and per-client
fences, physical `T`, and worker reserve `R`; internal priority does not create
another reserve. Once either path selects a request, response-channel closure
cancels a pending active-permit acquisition. The limiter transfers any consumed
notification on cancellation, so abandoned work neither serializes the ingress
until permit availability nor leaks active capacity.

The worker policy has three capacities:

- `T = MAX_ISOLATE_WORKERS`, the physical worker and assigned-request maximum;
- `B = T - R`, the shared base assigned-worker capacity;
- `R = ISOLATE_DEPENDENCY_WORKER_RESERVE`, dependency-only overflow above `B`.

Every request class, including dependencies, consumes shared base occupancy
while total occupancy is below `B`. A non-dependency can start only when global
and per-client total occupancy are both below their base limits. A dependency
can start up to the corresponding global and per-client totals. Therefore, once
occupancy reaches `B`, only dependencies can use the remaining `R` slots. There
is no target number of active dependencies and no dependency-first priority
below `B`. Completion accounting remains the first branch of the scheduler's
biased selection loop, matching upstream ordering; the rejected dependency-
share policy was the only reason to reverse those branches.

The per-client total remains
`ceil(T * FUNRUN_SCHEDULER_MAX_PERCENT_PER_CLIENT / 100)`, bounded by `T` and
with the existing minimum of one. Its effective reserve is
`min(R, per_client_total - 1)`, and its shared base is the remainder. This
preserves the existing per-client total instead of adding the global reserve on
top of it. The standard single-tenant in-process runner uses `100%`, so its
per-client capacities equal the global capacities.
If a multi-tenant runner resolves a per-client total of one, its effective
reserve is zero: a caller from that client cannot overlap a separately scheduled
descendant even when another global worker is free. Configure a per-client total
of at least two for clients that use these call patterns.

`MAX_ISOLATE_ACTION_WORKERS` separately caps independent V8 and HTTP actions
retaining workers. `0` derives the cap from `B`. The cap does not apply to
queries or mutations merely because they can call transactional subfunctions,
and it does not apply to a child action that is itself unblocking an ancestor.
Applying the cap to either group would unnecessarily limit independent database
functions or recreate the action-to-action deadlock. The action cap is workload
shaping and mixed-traffic protection; the dependency overflow is the liveness
mechanism.

The application query, mutation, V8-action, and Node-action limiters use the
same total-occupancy rule before the isolate queue. Their effective dependency
overflow is `min(R, configured_limit - 1)`, retaining at least one shared base
slot for a nonzero limiter. Every class consumes base occupancy first; only
dependencies can raise total occupancy above it. The gate queues requests
without letting a blocked waiter hold partial capacity, preserves FIFO among
all classes below the base, and skips an ineligible non-dependency only while
occupancy is already in the overflow range. Operators should keep query,
mutation, and action limits above `R` when they expect the full dependency burst
to pass concurrently. The Node-action gate also treats a nested Node action as
a dependency because its parent retains a permit from that same gate. This does
not grant isolate queue or worker overflow when the Node chain has no
isolate-holding ancestor.

Query-cache coalescing also preserves the class. A dependency query can wait for
another dependency computing the same cache key, but it does not wait behind an
independent cache miss whose isolate request may be outside the worker reserve.
It runs a side-effect-free duplicate query with dependency scheduling instead.

`ISOLATE_QUEUE_SIZE` must be greater than zero. Every externally submitted
request class can fill that many shared base queue entries. External dependency
sends can use `R` additional bounded entries, so a base-full queue can admit a
callback burst matching worker overflow capacity before eligibility-aware
selection. The physical queue can still reject a dependency when all
`ISOLATE_QUEUE_SIZE + R` entries are occupied. This is finite overload behavior,
not an unbounded or separate external dependency queue; failed callbacks return
errors and release their callers rather than hanging forever. Direct internal
nested UDF callbacks do not consume these CoDel entries and instead retain
upstream's non-expiring priority-channel semantics.

Under the default policy, upstream's improved CoDel implementation assigns
monotonically ordered deadlines using the later of the queue's idle-to-congested
transition and the request's congested expiry. The front is therefore the
earliest deadline. Class-aware selection drains an expired front before serving
non-expired work, then applies the upstream FIFO/LIFO rule among eligible
entries. The opt-in lane policy derives a hard deadline from enqueue time for
every entry and waits for the earliest deadline across lanes. Both policies'
consuming receivers arm their deadline while scheduler eligibility blocks
dispatch, so an ineligible entry can expire without another enqueue or
worker-completion event.
Each policy also exposes a non-consuming expiry companion that the scheduler
continues polling while a selected external request waits for its initial
active-JavaScript permit. Thus one selected request cannot suspend the hard
deadline of a different retained dependency, and the companion does not create
another consuming receiver or keep admission open after scheduler shutdown.

A one-worker pool cannot run a caller and a separately scheduled descendant
concurrently. Configure `R >= 1` and `T >= 2` for applications using these call
patterns. Queue deadlines still wake and reject stalled work, but expiry is a
failure signal rather than successful progress. Construction rejects
`MAX_ISOLATE_WORKERS=0`, `ISOLATE_QUEUE_SIZE=0`, `R >= T`, and an explicit
`MAX_ISOLATE_ACTION_WORKERS > B`. Malformed or non-Unicode values for these
knobs and `FUNRUN_ISOLATE_ACTIVE_THREADS` fail startup instead of falling back
to defaults.

The standard tree has one dependency-aware in-process runner. A custom runner
must enforce the same bounded dependency-admission contract at its real worker
pool. The application layer does not infer a custom runner's capacity from the
local `MAX_ISOLATE_WORKERS` knob or apply a best-effort compatibility cap.

## Request Admission and Wait Stages

The isolate queue is not the backend's only wait point. A request can wait
at several stages, and each stage has a different capacity, timeout, fairness
policy, and overload response:

1. A deployment-owned reverse proxy, load balancer, and the host TCP accept
   queues act before Convex. Convex does not configure or measure those queues.
2. `HTTP_SERVER_MAX_CONCURRENT_REQUESTS` is the total of a concurrency gate in
   each Convex HTTP service. Application requests above the limit enter an
   in-process wait without a configured queue-size bound.
   `HTTP_SERVER_TIMEOUT_SECONDS` and request metrics start after permit
   acquisition. The service concurrency gauge reports permits in use; separate
   admission metrics report waiters and wait duration. The `/version` and
   `/metrics` service routes retain their existing admission bypass. Traffic
   through the port `3211` proxy acquires a proxy permit and then a main-service
   permit; direct `/http` traffic uses only the main permit. Node callbacks
   carrying the callback-token header can use
   `HTTP_SERVER_DEPENDENCY_RESERVE` dependency-only overflow once shared base
   occupancy is full.
3. The application function limiters independently gate queries, mutations,
   Convex-runtime/HTTP actions, and Node actions. Query and mutation waits time
   out after `5` seconds by default; action waits time out after `10` seconds.
   The effective dependency overflow at each limiter is
   `min(R, configured_limit - 1)` above shared base occupancy. Outstanding,
   wait-duration, and wait-timeout metrics identify this stage.
4. Equal query cache misses can coalesce, making followers wait for one peer
   computation without entering the isolate queue. A protected direct action
   query bypasses an independent peer and may duplicate the query instead.
5. Externally submitted isolate work shares one bounded queue. Its shared base
   capacity is `ISOLATE_QUEUE_SIZE`, with `R` extra external dependency-only
   slots. Queue-full sends fail immediately. The default policy assigns the `5`
   second idle deadline or the `50` ms congested deadline and exposes combined
   generic CoDel depth. The opt-in lane policy uses its configured hard age and
   lane-specific depth, delay, overload, and rejection metrics. Scheduler
   counters separate class-specific enqueue, dispatch, expiry, and rejection in
   both modes. Direct nested UDF callbacks bypass this queue through upstream's
   internal high-priority channel and have no queue expiry.
6. The isolate scheduler assigns at most `MAX_ISOLATE_WORKERS` requests to
   workers. This is not another backlog: eligible work remains in the queue until a
   worker is available. Every class shares occupancy below `B`; only
   ancestor-unblocking work can raise occupancy from `B` to `T`; independent
   V8/HTTP actions are additionally capped by
   `MAX_ISOLATE_ACTION_WORKERS`.
7. `FUNRUN_ISOLATE_ACTIVE_THREADS` separately limits JavaScript actively using
   CPU, and upstream acquires its permit before assigning a worker. An action
   retains its assigned isolate worker while awaiting supported asynchronous
   operations but releases its active execution permit. The upstream limiter
   keeps one fixed permit total and two FIFO waiter tiers. It always hands an
   available permit to a high-priority waiter before a low-priority waiter.
   Direct separately scheduled nested transactional functions and every
   suspended-permit reacquisition use the high-priority tier; initial external
   root and action requests use the low-priority tier. The tiers add no active
   permits and do not preempt JavaScript already running. Permit acquisition
   latency and backend CPU throttling identify this stage. The original external
   queue deadline continues to bound low-priority initial permit acquisition
   after queue selection: the applicable CoDel expiration or lane hard deadline.
   An action callback retains this bound even when it is a scheduler dependency.
   Separately scheduled nested transactional functions use the internal path and
   wait without an external deadline because they cannot be retried safely.
   Scheduler dependency ownership and active-permit priority are deliberately
   separate: an action callback can use scheduler overflow but start at low
   priority, while an already-started independent root reacquires at high
   priority.
8. A Node action does not use an isolate worker, but it has its own application
   action permit and executor capacity. Every Node `ctx.run*` callback re-enters
   the main HTTP service and then the corresponding application and isolate
   stages. A nested Node action can use Node-action application overflow even
   when no isolate ancestry exists; only the ancestry marker grants downstream
   isolate overflow. This feedback path is why Node callback HTTP admission and
   isolate scheduling must be evaluated together.
9. Queries and mutations can then wait for database connections. Mutations also
   enter the bounded, single-threaded commit queue and can retry after OCC or
   write-throughput failures. Cron and scheduled functions bypass HTTP admission
   but join the application, isolate, and database stages. The cron executor and
   ordinary scheduled-job executor each independently admit up to the resolved
   `SCHEDULED_JOB_EXECUTION_PARALLELISM` value.

The numerical limits are not expected to match. HTTP and application limits
count requests that may be suspended on I/O or waiting downstream;
`MAX_ISOLATE_WORKERS` counts assigned isolate requests; and
`FUNRUN_ISOLATE_ACTIVE_THREADS` counts JavaScript currently eligible to consume
CPU. A high upstream limit does not create worker or CPU capacity. It changes
where overload waits and which timeout or rejection policy handles it. Lowering
an application limit can move work from legacy CoDel's short congested expiry
into a longer FIFO admission wait, so related knobs should be compared as
a complete admission path rather than normalized to the same number.

## Evidence

The profiling workload was one hot HTTP action route on an `8 vCPU / 32 GiB`
host with the Convex backend container limited to five CPUs and `14 GiB`. MySQL
was colocated. Common settings for the successful scheduler runs were:

- `CONVEX_BACKEND_CPUS=5`
- `CONVEX_BACKEND_MEMORY=14g`
- `HTTP_SERVER_MAX_CONCURRENT_REQUESTS=192`
- one hot HTTP action route
- one hot request key
- `30s` load runs for the steady-state samples

These measurements predate the review hardening that prevents independent work
from using dependency overflow, reserves application and queue admission, avoids
dependency query-cache waits behind independent work, and caps independent
action shells. They remain useful workload-tuning evidence, but they are not a
load-test validation of the final scheduler policy. The final policy is covered
by deterministic scheduler-loop and queue regression tests; operators should
repeat representative load tests before rollout.

HTTP action context reuse is a separate patch. It was enabled for most
successful stress results below.

With context reuse disabled at `200 rps`, the scheduler-dependency backend only
returned `587/6000` successful responses, returned `5413` HTTP `503` responses,
and had p95 latency `6651 ms`. With context reuse enabled and
`HTTP_SERVER_MAX_CONCURRENT_REQUESTS=192`, one warm `200 rps` run returned
`6000/6000` successful responses with p50 `46 ms`, p95 `110 ms`, and p99
`296 ms`.

The same backend with context reuse enabled passed a cold `20 rps` sample at the
`192` HTTP gate: `200/200` OK, p50 `49 ms`, p95 `1111 ms`, p99 `1483 ms`.
Raising the gate to `256` did not materially improve the `272 rps` overload
sample, so `192` stayed the selected gate for this five-CPU candidate.

## Worker And Active-Thread Matrix

The matrix below used `HTTP_SERVER_MAX_CONCURRENT_REQUESTS=192` and
`REUSE_HTTP_ACTION_CONTEXTS=true`.

At `200 rps`, `8/8` had the best warmed p95 latency in this single-route test.
`5/5`, `8/5`, and `10/8` also completed without HTTP failures in warmed runs,
but with higher p95 latency. `10/5`, `10/10`, `12/8`, `12/10`, and `16/8`
returned HTTP `503` responses at the same load.

| `MAX_ISOLATE_WORKERS` | `FUNRUN_ISOLATE_ACTIVE_THREADS` | Result at `200 rps`             |     p50 |       p95 |       p99 | Max backend HTTP concurrency | Max CoDel queue | Backend CPU avg/max | Backend throttled time |
| --------------------: | ------------------------------: | ------------------------------- | ------: | --------: | --------: | ---------------------------: | --------------: | ------------------: | ---------------------: |
|                     5 |                               5 | `6000/6000` OK                  | `47 ms` |  `390 ms` |  `691 ms` |                            3 |               0 |       `259% / 265%` |                 `0 ms` |
|                     8 |                               5 | `6000/6000` OK                  | `47 ms` |  `414 ms` |  `634 ms` |                            4 |               0 |       `264% / 305%` |                 `0 ms` |
|                     8 |                               8 | `6000/6000` OK                  | `47 ms` |  `236 ms` |  `443 ms` |                            3 |               0 |       `254% / 267%` |                 `0 ms` |
|                    10 |                               5 | `5989/6000` OK, `11` HTTP `503` | `47 ms` |  `559 ms` |  `729 ms` |                           62 |              53 |       `312% / 527%` |              `3524 ms` |
|                    10 |                               8 | `6000/6000` OK                  | `47 ms` |  `341 ms` |  `504 ms` |                           17 |               7 |       `362% / 524%` |              `3095 ms` |
|                    10 |                              10 | `5985/6000` OK, `15` HTTP `503` | `47 ms` |  `511 ms` |  `704 ms` |                           41 |              32 |       `326% / 523%` |              `1978 ms` |
|                    12 |                               8 | `5973/6000` OK, `27` HTTP `503` | `47 ms` |  `517 ms` |  `697 ms` |                           66 |              55 |       `310% / 533%` |              `4869 ms` |
|                    12 |                              10 | `5974/6000` OK, `26` HTTP `503` | `55 ms` |  `532 ms` |  `695 ms` |                           68 |              58 |       `388% / 511%` |             `11772 ms` |
|                    16 |                               8 | `5946/6000` OK, `54` HTTP `503` | `48 ms` |  `543 ms` |  `779 ms` |                           76 |              61 |       `318% / 514%` |              `7587 ms` |
|     5, no warm-up run |                               5 | `6000/6000` OK                  | `46 ms` | `1175 ms` | `1482 ms` |                           61 |              58 |       `290% / 430%` |                 `0 ms` |

The no-warm `5/5` rerun intentionally skipped a separate warm-up before the
`200 rps` sample. It still completed without HTTP failures, but it captured cold
queueing that made p95 much worse than the warmed `5/5` sample.

At `272 rps`, every tested setting returned at least some HTTP `503`s. `5/5` was
much stronger than the other settings: it accepted almost all requests, kept p95
below one second in both samples, and did not hit backend cgroup throttling.
Settings with active permits above the CPU quota drove the backend into CFS
throttling and filled the HTTP admission gate.

| `MAX_ISOLATE_WORKERS` | `FUNRUN_ISOLATE_ACTIVE_THREADS` | Result at `272 rps`               |      p50 |       p95 |       p99 | Max backend HTTP concurrency | Max CoDel queue | Backend CPU avg/max | Backend throttled time |
| --------------------: | ------------------------------: | --------------------------------- | -------: | --------: | --------: | ---------------------------: | --------------: | ------------------: | ---------------------: |
|                     5 |                               5 | `8010/8160` OK, `150` HTTP `503`  |  `65 ms` |  `534 ms` | `1436 ms` |                          118 |             113 |       `431% / 438%` |                 `0 ms` |
|              5, rerun |                               5 | `8126/8160` OK, `34` HTTP `503`   |  `66 ms` |  `482 ms` |  `956 ms` |                           91 |              85 |       `431% / 443%` |                 `0 ms` |
|                     8 |                               5 | `6984/8160` OK, `1176` HTTP `503` | `446 ms` | `4129 ms` | `5992 ms` |                          192 |             185 |       `515% / 522%` |             `32031 ms` |
|                     8 |                               8 | `6888/8160` OK, `1272` HTTP `503` | `375 ms` | `5035 ms` | `5571 ms` |                          192 |             185 |       `523% / 526%` |             `22931 ms` |
|                    10 |                               5 | `6400/8160` OK, `1760` HTTP `503` | `673 ms` | `4950 ms` | `6143 ms` |                          192 |             184 |       `516% / 523%` |             `34566 ms` |
|                    10 |                               8 | `6385/8160` OK, `1775` HTTP `503` | `631 ms` | `4768 ms` | `6202 ms` |                          192 |             183 |       `525% / 532%` |             `26831 ms` |
|                    10 |                              10 | `6454/8160` OK, `1706` HTTP `503` | `696 ms` | `4425 ms` | `6262 ms` |                          192 |             183 |       `526% / 532%` |             `28121 ms` |
|                    12 |                               8 | `6287/8160` OK, `1873` HTTP `503` | `729 ms` | `5287 ms` | `5875 ms` |                          192 |             181 |       `519% / 531%` |             `35630 ms` |
|                    12 |                              10 | `6210/8160` OK, `1950` HTTP `503` | `796 ms` | `4441 ms` | `6450 ms` |                          192 |             181 |       `521% / 522%` |             `28254 ms` |
|                    16 |                               8 | `6077/8160` OK, `2083` HTTP `503` | `845 ms` | `4794 ms` | `6298 ms` |                          192 |             177 |       `519% / 527%` |             `35118 ms` |

MySQL stayed low in these runs: roughly `0.3-6.4%` CPU in the captured samples.
The backend was the limiting process. During overload, the five-CPU Docker quota
was binding even while the host still had idle CPU outside the backend cgroup.

## Evidence-Based Starting Point

The matrix predates the final multi-worker dependency reserve. Its `5/5` result
supports five base assigned requests and five active JavaScript permits for
the measured five-CPU backend cgroup. In the final policy, preserve that
base capacity with `B=5`, which means
`MAX_ISOLATE_WORKERS=5+ISOLATE_DEPENDENCY_WORKER_RESERVE`, and keep
`FUNRUN_ISOLATE_ACTIVE_THREADS=5` as the conservative measured starting point.
The matrix does not establish an optimum dependency reserve or independent
action cap.

The old `5/5` setting did not win the warmed `200 rps` p95 result; `8/8` did,
with p95 `236 ms` versus `390 ms` for warmed `5/5`. The reason to preserve the
measured five-request base capacity is overload behavior. At `272 rps`, old
`5/5` accepted almost all requests and avoided cgroup throttling, while `8/8`
filled the `192` HTTP gate, returned `1272` HTTP `503`s, and accumulated
`22931 ms` of backend throttled time.

Do not derive base capacity or active permits as a fixed multiple of CPU
count. In this matrix, old `10/10` was the mechanical "2x CPU" setting for a
five-CPU cgroup. It already returned HTTP `503`s at `200 rps`, and it failed
much worse than old `5/5` under the `272 rps` overload point. Adding dependency
overflow to a measured `B=5` is different from raising base capacity to ten:
non-dependencies cannot consume the overflow, and active JavaScript remains
capped separately.

`8/8` is still a useful candidate when the target is lowest warmed latency for a
single hot HTTP action route and overload behavior is covered by upstream
traffic shaping. It is not a universal best setting. The single-route result
does not prove the same choice for mixed HTTP actions, queries, mutations,
scheduled work, deploys, indexing, or CPU-heavy JavaScript.

If backend CPU allocation changes, rerun a small matrix around the CPU quota and
one or two higher worker counts. For a six-CPU backend, start with `5/5`, `6/6`,
`8/8`, and `10/8`; add `10/10` only if the lower settings clear the target load
without queueing and there is CPU headroom inside the backend cgroup.

Keep `HTTP_SERVER_MAX_CONCURRENT_REQUESTS` fixed while comparing isolate knobs.
For the measured five-CPU candidate, `192` was enough to pass the clean
`200 rps` point and raising it to `256` did not improve the overload result.
That comparison predates the HTTP dependency reserve: with total capacity `192`
and `HTTP_SERVER_DEPENDENCY_RESERVE=1`, the final gate admits at most `191`
requests before occupancy enters dependency-only overflow. Higher HTTP
admission mainly creates a deeper queue unless backend CPU or runtime cost also
changes.

## Metrics To Use

The new scheduler metrics are meant to answer concrete questions during a load
run.

Use `isolate_scheduler_requests_enqueued_total{pool_name, scheduler_class}` and
`isolate_scheduler_requests_dispatched_total{pool_name, scheduler_class}` to see
whether dependency work is entering and leaving the scheduler. Match both
`dependency` and `dependency_descendant_holder`; the latter is the common label
for a separately scheduled query, mutation, or child action that can make
another nested call. If dependency-role enqueue rises while dispatch stays flat,
awaited functions are not getting workers.

Use `isolate_scheduler_requests_expired_total{pool_name, scheduler_class}` to
separate external pre-dispatch deadline failures by class. It includes both an
entry hard-expired while retained in the queue and a selected entry that
reached the same original deadline while acquiring its initial active permit.
Dependency expiry means a Convex-runtime action, HTTP action, query, or mutation
may fail because the function it awaited timed out before isolate dispatch.
Direct internal nested UDF callbacks have no external queue deadline.

Use
`isolate_scheduler_requests_rejected_total{pool_name, scheduler_class, reason}`
to distinguish queue-full or lane-full admission rejection, delay-control
shedding, a caller dropping a control-plane response before dispatch, and an
unexpected no-worker rejection. `reason="queue_full"` or `reason="lane_full"`
points at admission pressure before the scheduler can choose a worker.
`reason="delay_control_shed"` is a queue delay-control decision and
`reason="caller_dropped"` avoids dispatching abandoned control-plane work.
`reason="no_worker"` should be rare; it means a request selected for dispatch no
longer satisfied scheduler capacity checks.
`reason="scheduler_closed"` means the scheduler already exited, for example
after an isolate worker or worker channel failed. New sends after the receiver
closes increment this reason. Requests already queued are dropped so their
callers receive a shutdown error rather than hanging, but those drops are not
individually added to the rejection counter. These six reason values and the
five scheduler classes below are closed metric-label contracts.

Use
`isolate_scheduler_active_requests_info{pool_name,scheduler_class,is_isolate_action}`
to see which role combinations occupy workers during the sample while retaining
action identity separately. The `scheduler_class` values are `independent`,
`descendant_holder`, `dependency`, `dependency_descendant_holder`, and
`control_plane`. Control-plane requests use shared base capacity rather than the
dependency reserve or independent-action allowance. Active totals follow the
worker-request lifetime, including worker or channel failure, so a failed
scheduler does not leave these gauges permanently nonzero. Gauge updates use
paired increments and decrements so concurrent worker completions cannot publish
an older absolute count after a newer one.

Use `isolate_scheduler_capacity_info{pool_name,capacity_kind}` to confirm the
resolved `physical`, `base`, and `independent_action` capacities. This is
the fastest way to detect a missing or stale deployment knob.

Use `scheduled_job_execution_parallelism_info` to confirm the resolved
`SCHEDULED_JOB_EXECUTION_PARALLELISM` value without depending on process startup
logs. This unlabelled process gauge is initialized from the value enforced by
the ordinary scheduled-job executor. The cron executor applies the same value
as its own independent cap, so the gauge is a per-executor parallel job limit,
not the combined number of cron and scheduled jobs that the process can run and
not a count of jobs currently running. The gauge does not change runtime
behavior and should ride the next backend image justified by other work rather
than trigger a standalone rollout.

Use `scheduled_job_num_running_info` and `cron_job_num_running_info` for source
current occupancy of the two executors. The executors update the gauges after
each start, each cron completion notification, and each drained ordinary-
completion batch. Duplicate or unowned completion notifications fail before a
false occupancy is accepted; valid completions in the same batch are applied
and the resulting occupancy is published before the error propagates. External
scrapes can still miss states shorter than the scrape interval. Compare the gauges with
`scheduled_job_execution_lag_seconds` and `cron_job_execution_lag_seconds`
respectively. The legacy `scheduled_job_num_running_total` histogram is
event-sampled and should not be interpreted as a time-weighted occupancy
distribution.

Use `isolate_scheduler_dependency_reserve_dispatch_total{pool_name}` as direct
evidence that an ancestor-unblocking request was dispatched while pre-dispatch
global total occupancy was at or above shared base `B`. Per-client overflow
eligibility alone does not increment it. This counter records physical
dependency-only worker overflow admission, not the number or share of active
dependencies.

Use `isolate_scheduler_dependency_queue_reserve_enqueue_total{pool_name}` to
count dependencies that entered the extra queue capacity after the shared base
`ISOLATE_QUEUE_SIZE` capacity was full. A rising value proves that the queue
reserve prevented an immediate base-queue-full rejection; it does not mean
the request eventually dispatched before its applicable queue expiry.

`cache_plan_go_total{reason="dependency_cannot_wait_for_independent_peer"}`
counts dependency queries that duplicated an independent in-flight cache miss
instead of waiting behind it.

When `HTTP_SERVER_DEPENDENCY_RESERVE` is nonzero, the main backend also exports
`backend_http_service_base_concurrent_requests`; compare it with total HTTP
concurrency to see whether occupancy is using dependency-only HTTP overflow.
`http_admission_waiters_info{service_name,is_dependency}` counts requests that
actually entered each HTTP admission wait, and
`http_admission_wait_seconds{service_name,is_dependency}` records waits ending
in permit handoff or cancellation. Immediate admission produces neither.
When isolate queue delay control is enabled,
`isolate_queue_ineligible_info{lane,reason="independent_action_cap"}` reports the
latest queued count blocked by the action cap, but not the duration of that
wait.
Metrics still do not include queued depth by scheduler role, resolved per-client
capacities, application-limit reserve use, call-chain depth, or time spent
specifically waiting on a descendant.

Externally submitted scheduler roles share one bounded isolate queue.
Non-dependency work cannot use the `R` extra entries, but external dependencies
can occupy shared base entries as well. A queue holding
`ISOLATE_QUEUE_SIZE + R` entries still rejects subsequent external dependencies.
The reserve matches a bounded callback burst; it is not lossless admission under
unbounded overload. Direct internal nested UDF callbacks use upstream's separate
priority channel but still consume the same physical worker reserve when
dispatched.

Separate-isolate transactional callbacks are marked at the `UdfCallback`
boundary. `ctx.runAction` propagates isolate ancestry through both V8 and Node:
a Node executor receives the ancestry bit and returns it on authenticated
callback requests, while a child V8 action can be both dependency and
descendant-holder. Top-level Node actions do not carry the bit because they do
not retain an isolate worker.

This is a finite capacity model, not an unbounded call-stack proof. A chain can
retain one worker at each separately scheduled level. A chain or parallel fanout
requiring more than `R` workers beyond available base capacity can still
fill `T`. `SUBFUNCTIONS_IN_SAME_ISOLATE=true` removes the transactional worker
chain but changes heap, timeout, and fatal-error behavior for the nested stack.
Deep or highly parallel action chains still require explicit capacity planning;
no finite worker reserve can guarantee arbitrary recursion.

## Operator Coverage and Rollout

The standard self-hosted backend already uses the dependency-aware in-process
runner. An operator using the standard backend image does not need to inspect a
runner capability claim or enable another scheduler setting.

The patch covers direct action callbacks, separately scheduled transactional
subfunctions, and `ctx.runAction` chains across V8 and Node. Browser queries,
`useQuery`, `ConvexHttpClient`, and other ordinary clients do not carry the
marker because they do not leave an isolate-holding caller behind.

The following table starts from source patterns and settings visible to an
application owner. A row is irrelevant when the application does not contain
that code pattern.

| Application code or operating condition                                                                                                                             | Coverage and operator response                                                                                                                                                                                                                                                                                                               |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Default-runtime or HTTP actions call `ctx.runQuery` or `ctx.runMutation`                                                                                            | Covered. Configure `R >= 1`, keep the corresponding application limit above `R`, and include the actions in representative load tests.                                                                                                                                                                                                       |
| An action awaits `ctx.runAction`, including a runtime crossing into or within `"use node"`                                                                        | Covered across V8 and Node. Node callbacks receive main-HTTP admission reserve, and nested Node actions receive Node-action application overflow. Only chains with an isolate-holding ancestor receive isolate scheduler overflow. Any reverse proxy on `CONVEX_CLOUD_ORIGIN` must leave callback headroom matching `HTTP_SERVER_DEPENDENCY_RESERVE`. |
| A query calls `ctx.runQuery`, or a mutation calls `ctx.runQuery` or `ctx.runMutation`, with `SUBFUNCTIONS_IN_SAME_ISOLATE=false`                                    | Covered as a finite chain: every separately scheduled child is dependency-marked and may itself be a descendant-holder. Size `R` for expected nesting and fanout; a chain requiring more than `T` simultaneous workers can still fail. `useStaleSnapshot` and `transactionLimits` do not change isolate placement.                           |
| The application runs many independent V8 or HTTP actions                                                                                                            | `MAX_ISOLATE_ACTION_WORKERS` limits assigned independent action shells without reducing query/mutation worker capacity. A lower value protects mixed traffic; a higher value can improve an action-only route. There is no universal ratio, so document whether the selected value is benchmarked or an informed starting point.             |
| `HTTP_SERVER_MAX_CONCURRENT_REQUESTS <= HTTP_SERVER_DEPENDENCY_RESERVE`                                                                                             | The standard local backend does not start because the configured overflow would leave no shared base HTTP admission. Set the HTTP total above the HTTP reserve. This matters for `"use node"` chains; V8-only callbacks do not re-enter through HTTP.                                                                    |
| `concurrency_permit_acquire_seconds` rises or root-style functions report `InitialPermitTimeoutError` while dependency work is queued                              | Worker reserve is available, but the shared active-JavaScript permits or CPU are saturated. Initial nested transactional waits and all permit reacquisitions use the high-priority tier; nested transactional initial waits have no external deadline. Initial roots and actions, including action callbacks that are scheduler dependencies, use the low-priority tier and remain bounded by the original applicable CoDel or lane queue deadline. Check backend CPU throttling and long-running JavaScript. Reduce admitted CPU work or raise active permits only after confirming CPU headroom; tiering does not add active capacity. |
| Public clients can reach `/api/actions/*`                                                                                                                           | Callback tokens protect operations, but the outer HTTP gate classifies reserve use before token authentication. Restrict this path to the backend or Node executor network source where practical, verify legitimate callbacks after the rule, and keep bounded public admission.                                                            |
| `isolate_scheduler_requests_rejected_total{scheduler_class=~"dependency.*",reason="queue_full"}` or dependency expiry rises continuously during representative load | The bounded reserve is exhausted. Correlate with action failures, HTTP errors, backend CPU, active-permit wait, and dependency dispatch. Reduce or shape admitted traffic, reduce nesting/fanout, or add measured capacity. Do not increase queue depth alone: it can replace rejection with longer latency without increasing service rate. |

Test rollout with a representative mix of default-runtime actions, HTTP actions,
independent queries and mutations, cron and scheduled functions, and cold as
well as warm queries. Include `"use node"` actions if the application uses them.
The performance matrix above predates the final worker, application, queue,
cache, and fairness hardening, so it does not replace this deployment test.

The measured performance matrix predates the multi-worker overflow, action cap,
transactional propagation, Node propagation, and HTTP reserve. It establishes
useful CPU and active-thread evidence, not the optimum `R` or action cap. Treat
new values as selected policy until a representative mixed-route test validates
them.

The query-cache bypass is an implementation cost, not a separate application
architecture limitation. Occasional
`cache_plan_go_total{reason="dependency_cannot_wait_for_independent_peer"}`
increments mean an action callback duplicated an independent in-flight cache
miss to preserve progress. Investigate only sustained growth that correlates
with CPU pressure or dependency expiry.

Deployments that version the Node executor separately from the backend should
roll out executor support for `hasIsolateWorkerAncestor` before relying on Node
callback isolate overflow. The request field is additive, so mixed versions can
still execute actions, but an older executor omits the ancestry header from its
callbacks and those callbacks use ordinary isolate admission until the executor
is updated.

Custom function runners are a backend-integration concern, not a standard
self-hosted operator check. Such a runner must implement equivalent bounded
dependency admission at its real worker pool; the application layer has no
best-effort fallback for a runner that does not honor this contract.

## Scope

These patches do not solve every throughput problem. HTTP action context reuse
is a separate patch, and it was enabled for the successful `200 rps` and worker
matrix results above. Without that patch, the same scheduler-dependency backend
failed the `200 rps` sample with `5413` HTTP `503`s.

Docker CPU and memory split is deployment policy. The measurements above show
the backend cgroup CPU limit becoming binding while MySQL stayed low. Moving CPU
from MySQL to the backend, or adding host CPU, should be tested as an explicit
deployment change with the same latency, HTTP `503`, CoDel, cgroup throttling,
and MySQL counters.

For dependency-classified calls that pass bounded HTTP and application
admission, the worker and queue reserves admit up to `R` ancestor-unblocking
requests beyond shared base capacity. This does not make overload lossless or
guarantee call chains deeper than configured physical capacity. The patch does
not remove CPU cost from JavaScript initialization, module metadata lookup,
source loading, action setup, or application code.

## Interaction with isolate queue delay control

The opt-in isolate queue policy in
[`isolate_queue_control/README.md`](../isolate_queue_control/README.md) consumes the dependency
role, action identity, queue reserve, worker reserve, per-client limits, and
independent-action cap defined by this patch. It changes queue selection and
expiry policy, not those capacity contracts. Disabling queue delay control
restores the legacy CoDel policy while preserving dependency-aware scheduling.

Review and test the patches together at their shared boundaries: ancestry
propagation through V8 and Node calls, application and HTTP dependency gates,
query-cache bypass, per-client eligibility, and dependency-only queue and worker
overflow. A queue-policy rollback cannot repair an incorrect dependency marker
or an undersized reserve, and increasing a reserve cannot create CPU capacity.
