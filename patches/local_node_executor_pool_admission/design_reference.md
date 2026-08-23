# Local Node Pool Admission and Responsiveness Design Reference

Status: accepted design for the maintained
[`local_node_executor_pool_admission`](README.md) patch.

## Context

The local Node executor already has two important bounds:

- one application-wide action concurrency limit; and
- one resident Node process for each application-declared logical pool.

Those bounds answer different questions. The global limit caps aggregate Node
actions, but it cannot reserve behavior for a specific pool. The process pool
isolates module state and process lifecycle, but without an admission limit it
does not prevent many independent actions from entering the same event loop.

This matters most for modules that intentionally perform CPU-heavy work. A
single Node main event loop cannot execute JavaScript from two such actions in
parallel. Admitting additional independent actions to that process increases
latency and memory without increasing JavaScript throughput, and it can make
the health endpoint unresponsive. A uniform health threshold can then replace
a process that is busy in a workload the operator intentionally permits.

The narrow objective is therefore:

- bound independent work per existing logical pool;
- queue before scarce global and resident-generation ownership is consumed;
- retain the application-wide hard cap;
- keep scheduled work `Pending` until admission is real;
- let an operator choose a per-pool event-loop unresponsiveness budget; and
- expose enough queue evidence to tune the static values.

The objective is not to introduce a general Node scheduling framework, make
CPU-bound JavaScript preemptible, or provide exactly-once recovery after user
code may have started.

## Decision

### Add optional policy to existing logical pools

An operator supplies a strict map from the existing pool name to three
optional values: independent concurrency, queue-warning duration, and maximum
event-loop unresponsiveness. The map is bounded to the routing protocol's
maximum of the default pool plus eight named pools, while still permitting
configuration before a declared name is deployed. The application continues
to own module-to-pool routing. Operator configuration cannot create a pool,
reroute a module, or alter durable topology.

A queue-warning duration requires an independent-concurrency limit. Without
that gate there is no pool-local wait to observe, so accepting the field would
create an inert configuration that appears effective.

This keeps routing correctness and capacity policy separate. Pool routing is a
required application capability and deployment protocol. Admission policy is
an optional host choice that can be enabled, tuned, or removed without an
application deployment or data migration.

### Use hierarchical admission, pool first

A configured action first acquires its pool permit and then acquires the
existing application-wide Node permit. The pool permit is held until the
invocation finishes or is canceled.

Pool-first ordering is intentional. If the order were reversed, every request
queued for one saturated pool could retain a global permit and prevent an
otherwise runnable action in another pool from starting. With pool-first
ordering, only actions that fit their local policy compete for the global
bound.

All paths use the same order, so there is no pool/global wait cycle. A request
can hold a pool permit while waiting for a global permit, but no request can
hold a global permit while waiting for a pool permit. Cancellation-safe guards
release both levels.

For direct calls, both waits consume one absolute timeout budget. A composed
limit must not turn one bounded wait into two consecutive full waits.

### Preserve dependency progress

`maxConcurrency` bounds independent roots. Descendant Node work may use the
same bounded dependency overflow already used by the application-wide Node
gate, without exceeding that global hard limit.

Because pool admission precedes global admission, independent roots can hold
pool permits while waiting for global base capacity. Those holders do not
consume the pool-side dependency overflow. The pool gate may therefore have
more permit holders than the global hard limit, but the later global gate still
bounds actions that can run. This preserves the complete global dependency
reserve even when `maxConcurrency` is close to the global limit.

An absolute concurrency-one semaphore would deadlock a root that awaits a
nested Node action routed to the same pool: the parent would retain the only
permit while waiting for the child. Dependency-only overflow admits the child
needed for progress. It does not admit a second independent root. This is a
deliberate semantic qualification of the pool limit, not an accidental limit
bypass.

### Queue on the pre-claim side of scheduled execution

Scheduled and registered-cron actions enter pool and global admission through
the existing execution-start barrier. They do not commit `InProgress` until
the pool permit, global permit, and exact resident-generation reservation are
held and user code is still blocked.

If configuration, topology, or generation availability changes before that
admission succeeds, the attempt can end while the durable job remains
`Pending`. Once `InProgress` is visible, the scheduler retains its existing
conservative behavior because it cannot generally prove that external effects
did not begin.

The first version does not add pool identity to scheduled-job records or build
a second durable scheduler. A waiting attempt therefore still occupies one of
the scheduler's existing bounded execution slots. A sufficiently large
backlog for one saturated pool can delay later jobs for another pool until a
waiting attempt progresses. This is bounded head-of-line delay, not a permit
deadlock or an unbounded in-memory queue, and all affected jobs remain
`Pending`.

That limitation is accepted to keep the change local and reversible. Removing
it soundly would require scheduler-visible, snapshot-consistent pool
reservations, stale-route reconstruction, and a wakeup protocol that scans
past saturated pools. That is a materially larger scheduler design. Queue-age
and scheduler-lag evidence should justify such work before it is added. If the
bounded source slots repeatedly fill with one pool's waiters and delay
unrelated work, this decision must be revisited rather than hidden by raising
limits without measurement.

### Treat health as main-event-loop progress

The watchdog retains a short `/health` request served by the same Node main
event loop as `/invoke`. This is the property the health check needs: it tests
whether the process can make progress on the execution protocol, not merely
whether a side thread or operating-system process remains alive.

The per-pool value is an elapsed unresponsiveness budget. The first probe's
request start is the deadline origin even before the response or ordinary
probe timeout completes. A successful response before the deadline clears the
interval. A completed failed probe retains its request start as the origin,
and the watchdog races the remaining budget against both its interval and any
later in-flight probe. Neither the first nor a later probe timeout can extend
the configured threshold. Consecutive misses remain useful diagnostics but
are not the operator-facing unit.

The budget is not a CPU-time quota. It also includes other causes that block
the main event loop, such as synchronous I/O, native code, or runtime stalls.
Conversely, JavaScript that periodically yields can consume substantial CPU
while remaining responsive. The name and metrics use `unresponsive` rather
than `CPU` so configuration does not promise enforcement it cannot provide.

## Safety and progress properties

The design preserves these invariants:

- aggregate active Node work remains bounded by
  `APPLICATION_MAX_CONCURRENT_NODE_ACTIONS`;
- one saturated configured pool cannot consume global permits with waiters;
- a second independent root cannot exceed a configured pool limit;
- pool-side dependency overflow is bounded, and permit holders above the
  global hard limit remain queued at the later global gate;
- the global gate never admits active Node work above its hard limit;
- every path acquires pool capacity before global capacity;
- cancellation releases queued and active accounting;
- scheduled actions do not claim `InProgress` merely to wait for pool
  capacity;
- generation replacement cannot invalidate a successful pre-claim reservation
  without using the existing conservative hard-failure path; and
- watchdog retirement still fences the exact generation before replacement.

Progress depends on the same finite assumptions as the existing executor: an
active action eventually completes, times out, or loses its generation; the
global gate eventually releases capacity; and backend tasks continue to be
polled. Static semaphore ordering introduces no deadlock. FIFO behavior may
produce queue delay, and the scheduler-slot limitation above may produce
bounded cross-pool head-of-line blocking, but neither creates a closed wait
cycle.

Increasing an unresponsiveness budget delays recovery by design. It cannot
create an internal deadlock, but a process that remains blocked below or until
that budget provides no Node progress during that interval. Operators must set
the value below the longest outage interval they are prepared to tolerate and
must not use it as a substitute for bounding the algorithm.

## Rejected alternatives

### Worker thread per action

Rejected for this patch. It changes module isolation, native-addon
compatibility, environment setup, cancellation, logging, package ownership,
and process diagnostics. Worker threads also have separate V8 isolates, so
they do not provide free module initialization reuse. A correct worker
lifecycle would be a new runtime architecture rather than an admission-policy
extension.

### Several fixed Node children per logical pool

Rejected for now. It multiplies baseline RSS on constrained hosts and changes
the meaning of rebuildable process-local module state. It also requires
selection, per-child generation cutover, and aggregate pool health semantics.
The existing one-child pool plus bounded admission addresses the observed
need with less state.

### Process per action

Rejected. It provides strong isolation but loses resident module and V8
initialization reuse and has materially higher startup and memory cost. It
also duplicates lifecycle work already owned by the resident-generation
executor.

### Generic external Node pool library

Rejected as low leverage. The difficult boundaries are in Rust: durable
claim ordering, application-wide dependency capacity, topology fingerprinting,
generation promotion, process ownership, and conservative recovery. A
JavaScript pooling library does not own those boundaries and would add another
lifecycle model.

### Move `/health` to an isolated worker

Rejected because it would make the check vacuous for the intended property. A
worker could report healthy while the main event loop serving `/invoke` is
blocked. Separate process-liveness telemetry can be useful, but it cannot
replace a main-event-loop progress probe.

### Replace the watchdog with CPU accounting

Rejected for this patch. CPU accounting answers resource-consumption questions
but does not prove that the invocation event loop can serve requests. Hard CPU
isolation also requires operating-system scheduling or container boundaries,
not an in-process duration knob. The patch names and enforces only the progress
property it can observe.

### Automatic limits, adaptive concurrency, or autoscaling

Rejected until static policy and queue metrics show a concrete need. Adaptive
control would require an objective function, stability bounds, workload
classification, and operational override semantics. The small static map is
easier to reason about and roll back.

### Durable per-pool queues or priority scheduling

Rejected in the first version. Existing scheduled jobs already provide a
durable `Pending` queue, and direct calls already have bounded admission.
Adding pool ownership, priorities, or retry state to durable records would
expand schema and recovery contracts. The accepted scheduler-slot limitation
is observable and can motivate a focused follow-up if real workloads require
cross-pool scan-past behavior.

### Return `InProgress` to `Pending` after an apparent pre-execution failure

Rejected as unsafe. After a durable claim, transport loss, generation loss, or
runtime failure can be ambiguous about whether external effects began. The
correct boundary is to reserve admission before the claim, not to invent a
rollback transition after it.

## Consequences

The change is intentionally static and local. Operators gain a useful control
for CPU-heavy or latency-sensitive pools without paying for worker-runtime or
durable-scheduler redesign. Default behavior remains unchanged until a policy
is configured.

The tradeoff is that this is admission control, not CPU preemption or complete
quality-of-service isolation. One admitted action can still block its pool's
event loop, a longer watchdog budget intentionally delays replacement, and a
scheduled backlog can occupy the scheduler's bounded source slots. These are
explicit limits of the design and should be evaluated with the emitted queue,
scheduler-lag, health, and generation metrics.
