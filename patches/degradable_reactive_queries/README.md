# Degradable Reactive Queries and Client Backpressure

Status: the maintained backend patch implements reactive-query admission, cache bypass, typed
deferral, backend-owned deferred-query retries, and a capability-negotiated pressure lifecycle
behind an optional leader cap. With the cap absent, the backend retains only declaration and
suppression telemetry and follows the previous execution path. The matching `convex-js` patch
negotiates lifecycle version 1, validates active and cleared pressure events, and exposes one
epoch-scoped retry request. A downstream frontend can keep successful subscriptions mounted,
present pressure as visible staleness, and use the explicit cleared event as its catch-up boundary. An
HTTP-action extension and an isolate queue lane remain separate optional designs. Optional
active-JavaScript service floors let the leader cap exceed active execution capacity while
preserving protected and degradable progress.

This patch lets a cooperating sync client declare that its root reactive
queries may become temporarily stale during overload. The backend places only
those query cache-miss leaders under a separate finite admission cap. When that
cap is full, the backend retains the exact deferred query set, lets the rest of
the transition complete, and retries only that set. The client displays stale
state without removing successful subscriptions; mutations, actions, and
non-deferred reactive queries continue normally.

The classification is deliberately negative. A client may opt its root queries
down to `degradable`; it cannot claim a privileged class. The default remains
the current normal behavior. Backend-derived dependency work always takes
precedence over the client classification.

The design is generic. It does not contain application module names, function
names, route names, client identities, or a backend allowlist chosen for one
deployment.

## Operator decision summary

This patch is useful when all of the following are true:

- many interactive clients maintain reactive subscriptions;
- those clients can display a recent result for a short period instead of
  requiring every invalidation to recompute immediately;
- background workers, control-plane work, mutations, actions, or important
  HTTP endpoints must retain capacity during a synchronized query wave;
- the application can implement one central stale-data state instead of adding
  policy to individual functions.

The patch is not useful as a security boundary or as a substitute for total
capacity planning. A client can omit the opt-down field and receive normal
admission. Public rate limits and abuse controls remain necessary. Applications
whose reactive query results cannot be stale should not mark that client
connection as degradable.

## Motivation

A function deployment or configuration push can produce a short, synchronized
increase in reactive query work. Active subscriptions whose execution state is
no longer valid must rerun against the new function configuration. Module
analysis and evaluation can be cold at the same time. Normal application
traffic, scheduled work, worker traffic, and deployment work can already be in
flight when this wave begins.

The existing query cache coalesces requests only when their complete cache keys
match. More connected users still increase the number of distinct combinations
of function, arguments, identity, timestamp, and journal. A deployment can
therefore produce many concurrent cache-miss leaders even when exact duplicate
requests are coalesced correctly.

The maintained scheduler patches protect a different invariant. A query,
mutation, or action that unblocks an isolate-holding ancestor can use dependency
reserve. This prevents an admitted action from deadlocking behind unrelated
work. It does not reserve ordinary capacity before progress-making work starts,
and it does not distinguish stale-tolerant page subscriptions from root queries
used by workers.

Generic query admission is too coarse for that distinction. Lowering
`APPLICATION_MAX_CONCURRENT_QUERIES` also limits worker and watcher queries,
direct query clients, scheduled query work, and action-originated query
callbacks at the application gate. Raising queue depth or delay thresholds
allows more requests to wait but does not increase the service rate. Generic
overload errors are also a poor sync protocol boundary: the sync worker retries
retriable query failures internally, and one retrying query can delay completion
of the whole transition.

The intended overload behavior is instead:

1. Explicitly degradable root reactive queries receive a finite share of
   cache-miss execution capacity.
2. Normal work and backend-derived dependencies do not coalesce behind an
   in-flight degradable cache leader.
3. A deferred query does not fail the connection and does not discard an older
   successful client value.
4. The server assigns one connection-local pressure epoch, reports a bounded
   pending count, and retries only its deferred set after a bounded delay.
5. Successful subscriptions remain mounted. The client marks live data stale
   and reduces optional imperative reads without recreating the query set.
6. The server emits an explicit cleared event after every deferred query in the
   epoch either recovers or is removed.
7. A user may request one immediate retry for the current epoch. The request
   never removes or recreates page subscriptions and never changes workload
   class.

This turns an explicitly accepted freshness reduction into load reduction. It
does not infer which functions are important from their names or UDF types.

## Goals

- Provide one connection-level opt-down for stale-tolerant reactive queries.
- Preserve current behavior for every client that omits the option.
- Apply the option only to independent root queries from that sync connection.
- Keep mutations and actions from the same connection normal.
- Preserve dependency reserve and dependency liveness without trusting client
  input.
- Limit actual query cache-miss leaders rather than charging every coalesced
  subscriber as an independent execution.
- Avoid priority inversion through the query cache.
- Do not publish a waiting cache leader until its degradable permit has been
  acquired.
- Return a distinct, temporary deferral that the sync worker does not retry in
  its generic overload loop.
- Give a frontend enough information to show bounded stale state and converge
  on an explicit server-owned completion condition.
- Preserve successful subscriptions while deferred queries recover.
- Retry only queries that the backend previously marked deferred.
- Keep configuration strict and telemetry labels bounded.
- Remain maintainable as a small patch on top of upstream backend and client
  changes.

## Non-goals

- The patch does not identify or grant a globally privileged client.
- It does not protect normal capacity from a hostile client that omits the
  opt-down field.
- It does not guarantee that every normal request starts immediately.
- It does not preempt a JavaScript execution that has already started.
- It does not infer whether an action will eventually call a mutation.
- It does not classify individual application modules or functions.
- It does not replace proxy rate limits, request authentication, or tenant
  fairness.
- It does not make query results durable after a subscription is removed.
- It does not identify deferred query IDs, function names, arguments, routes,
  or identities to the client.
- It does not let the client choose which deferred query to retry.
- It does not reduce external provider latency inside actions.
- It does not create a dedicated deployment or control-plane scheduler lane.
- It does not replace context reuse, module prewarming, import-graph cleanup,
  or additional measured CPU capacity.

## Terminology and trust model

`Normal` is the default class. It means that this patch adds no opt-down
restriction. It does not mean guaranteed priority.

`Degradable` means that a cooperating caller accepts temporary deferral and
stale presentation when the dedicated sub-cap is full. It is a resource policy,
not a statement about a function's source code or business importance.

`Dependency` is backend-derived work that unblocks a still-running ancestor.
Only authenticated runtime propagation can establish this property. Client
metadata cannot claim it.

Dependency role overrides a degradable declaration. Otherwise, a configured
degradable declaration opts the root query down, and absence means normal. The
service protections are deliberately local:

- dependencies retain their existing finite overflow;
- degradable roots have a finite sub-cap;
- normal and dependency queries bypass an in-flight degradable cache leader.

The active-JavaScript gate maps normal roots and backend work to `Protected`,
maps only admitted degradable cache-miss leaders to `Degradable`, and gives
`Dependency` the next grant. Protected and degradable work use work-conserving
minimums rather than global strict priority. Normal still means unaffected by
the degradable sub-cap, not permission to claim an unbounded priority class.

The client field only opts work down. A forged degradable value can reduce the sender's own
service. Omitting the field remains possible, so the mechanism is not an overload defense against
uncooperative public clients.

## Coordinated patch composition

The core feature requires changes in both the backend and JavaScript client
packages. They may be maintained in one source tree, but operators deploy a
backend image and applications consume a published client package on separate
release schedules.

The `convex-js` part would:

- add one optional client construction setting and lifecycle capability;
- serialize them on every `Connect`, including reconnects;
- strictly decode legacy, active, and cleared pressure metadata on a `Transition`;
- expose that metadata through a dedicated callback after applying the transition;
- expose one epoch-scoped retry method with local deduplication;
- document that lifecycle pressure keeps successful subscriptions mounted.

The `convex-backend` part would:

- parse and retain the connection's query workload class;
- compute an effective class from the declaration and whether degradable
  admission is configured;
- pass it only to root queries rerun by that sync worker;
- add a finite, immediate-admission gate for degradable query cache-miss
  leaders;
- propagate enough class information through query caching and isolate
  scheduling to avoid priority inversion;
- map cap rejection to a distinct temporary-unavailability reason;
- include pressure metadata in the transition that observed a deferral;
- add bounded metrics and trace fields;
- strictly validate the new self-hosted configuration.

## Client protocol contract

### Connect field

The JavaScript client option and wire field should use the same concept:

```ts
type QueryWorkloadClass = "degradable";

interface BaseConvexClientOptions {
  queryWorkloadClass?: QueryWorkloadClass;
}

type Connect = {
  type: "Connect";
  // Existing fields omitted.
  queryWorkloadClass?: QueryWorkloadClass;
  degradableQueryPressureVersion?: 1;
};
```

Absence means normal behavior. There is intentionally no `"priority"` or
`"critical"` value. An explicit `"normal"` value is unnecessary and would
make the wire contract larger without adding semantics.

The backend must reject a present unknown value as a malformed `Connect` rather
than silently treating it as normal. This fail-fast rule prevents a spelling or
version error from unexpectedly disabling containment. Existing backends use
Serde without unknown-field rejection for the `Connect` object, so they ignore
this new optional field. A protocol regression test must preserve that rolling
upgrade property.

`convex-js` must put the field on the initial connection and every reconnect.
Connection count, authentication updates, and query-set changes do not alter the
class. Changing the class requires constructing a client with the intended
option; it is not a per-query mutation of a live connection. A sync worker
accepts exactly one `Connect` message, so a custom client cannot renegotiate
the workload class or lifecycle capability in place.

`degradableQueryPressureVersion: 1` is a separate capability declaration. It
is sent by a client that understands the active/cleared lifecycle and the
epoch-scoped retry request. Omission requests the legacy one-shot pressure
shape. A present null, unsupported version, non-integer, or out-of-range value
is malformed. Keeping workload classification and response capability separate
allows a new backend to preserve the legacy event for already-published
degradable clients.

### Server pressure field

The backend should add optional metadata to the existing transition message:

```ts
type ServerPressure =
  | {
      kind: "degradable_query_capacity";
      retryAfterMs: number;
    }
  | {
      kind: "degradable_query_capacity";
      state: "active";
      epoch: number;
      retryAfterMs: number;
      pendingQueryCount: number;
    }
  | {
      kind: "degradable_query_capacity";
      state: "cleared";
      epoch: number;
      pendingQueryCount: 0;
    };

type Transition = {
  type: "Transition";
  // Existing fields omitted.
  serverPressure?: ServerPressure;
};
```

An optional transition field is preferable to a new server-message variant.
Existing JavaScript clients already tolerate unknown object properties, while
an unknown discriminated message variant can reach an exhaustive switch and
break the connection. New clients talking to old backends simply never receive
the field.

`retryAfterMs` is a lower bound selected by the backend. Its wire range is a
strictly positive unsigned 32-bit integer (`1..=4294967295`). `epoch` is also a
strictly positive unsigned 32-bit connection-local value. `pendingQueryCount`
is in `1..=4294967295` for `active` and exactly zero for `cleared`. Lifecycle
version 1 does not expose query identifiers or query metadata.

For a capable connection, the first deferral opens an epoch and emits
`state: "active"`. Further deferrals remain in that epoch and may update the
pending count. The epoch clears only after the backend's deferred set is empty,
including recovery with an unchanged result and removal from the query set.
Absence of pressure metadata does not imply clear. A reconnect creates a new
backend worker and resets lifecycle state.

For a client that omits the capability, the backend emits only the original
`{ kind, retryAfterMs }` event and never emits `state`, `epoch`, or a clear
event. This preserves the published protocol exactly. A capable client accepts
both shapes so it can connect to a backend that predates lifecycle version 1.

The transition carries at most one pressure object. It does not identify the
deferred function, query ID, user, component, or cache key. If several queries
in one update are deferred for the same reason, one pressure object is enough.

The JavaScript client should expose a dedicated handler such as:

```ts
type ServerPressureHandler = (pressure: ServerPressure) => void;
```

The exact public method name should follow the client package's existing event
API. The pressure event should not be represented as a failed query result and
should not be mixed into the normal query-change callback. The client applies
the rest of the transition normally before notifying the pressure handler.

Lifecycle version 1 also adds one client message:

```ts
type RetryDegradableQueries = {
  type: "RetryDegradableQueries";
  epoch: number;
};
```

The JavaScript client sends this only for the currently active epoch and at
most once for that epoch. The backend independently validates the epoch and
deduplicates the request. A stale, duplicate, inactive, or unsupported request
does not alter the query set and does not receive privileged admission.

### Backend-owned deferred set and retry transition

The sync state records a structural set for every query deliberately left
without a subscription and a strict degradable subset, including a newly added
query that has no previous result. The degradable subset is the only authority
for automatic and user-requested pressure retry and the lifecycle pending
count. Ordinary `need_fetch()` state and the structural deferred set are broader
and are not valid pressure-retry selectors.

The retry path does not call the normal update path. A normal update takes all
subscriptions so it can advance the connection's global timestamp; using that
path for pressure retry needlessly refreshes the entire query set. Instead, a
deferred-only retry:

1. snapshots only the currently deferred queries;
2. executes them at the current sync-state timestamp;
3. leaves every successful subscription and invalidation future mounted;
4. completes results in a same-version transition;
5. emits an updated active event or the explicit cleared event.

The same-version rule is required. Advancing the global timestamp without
extending untouched subscriptions would advertise a state version those
subscriptions do not cover. A later ordinary invalidation, identity, or
query-set update keeps its existing all-subscription refresh behavior and may
also recover deferred queries as part of that transition.

Only one deferred-only retry may run at a time. A normal update already
scheduled for the connection takes precedence and subsumes a pending deferred
retry. The automatic three-second timer is re-armed while the set remains
non-empty. The manual request schedules one immediate attempt for the active
epoch, replaces the old pressure-timer boundary, and re-arms the automatic delay
if the attempt still defers; it does not disable bounded automatic retry.

Feature-temporarily-unavailable queries retain the existing normal update timer.
That timer is independent from the pressure timer. A transition may contain
both reasons, and both timers must remain armed. If a pressure retry encounters
feature unavailability, the query remains structurally deferred but leaves the
degradable subset, so it is not retried every three seconds and does not keep a
pressure epoch active. Starting a normal update cancels the previous feature
deadline; completion re-arms it only after a fresh feature deferral. A stale
feature timer also checks for actual feature-deferred queries rather than using
broad `need_fetch()` state, so it cannot turn a later pressure-only retry into a
whole-query-set update.

## Classification scope and precedence

The connection value applies only to independent root queries executed by that
connection's sync worker. It does not apply to every operation carried by the
WebSocket.

One-shot queries sent through `ConvexHttpClient` have no `Connect` message and
remain normal in the initial patch. Extending opt-down classification to that
request path would require a separate HTTP protocol contract.

| Operation | Effective class |
| --- | --- |
| Root reactive query from a connection with no option | Normal |
| Root reactive query from a degradable connection | Degradable |
| Mutation sent on a degradable connection | Normal |
| Action sent on a degradable connection | Normal |
| Query or mutation that unblocks an isolate-holding ancestor | Dependency |
| Nested query called by an admitted degradable root query | Dependency while it unblocks that root |
| Cron, scheduled function, deployment analysis, or direct server work | Existing normal or backend-derived class |
| Query from a worker client that omits the option | Normal |

The degradable marker terminates at the admitted root query. The root retains
its degradable leader permit until its query tree completes. A separately
scheduled descendant receives backend-derived dependency treatment so it can
finish the already admitted root and release resources. This prevents the
client option from weakening dependency liveness.

The implementation should use a typed query execution class parameter rather
than putting the option into a generic caller string or request metadata map.
Mutation and action call sites should pass no query class. This makes accidental
classification of another UDF type a compile-time-visible change.

## Degradable query admission

### Admit cache-miss executions, not subscribers

The dedicated cap applies whenever the query cache selects `CacheOp::Go` for a
degradable root query. Most such operations publish a cache-miss leader. A
cached or waiting result newer than the requested timestamp instead requires a
side-effect-free direct execution; that execution also consumes a permit so it
cannot bypass the finite root-execution bound. `CacheOp::Ready` performs no new
JavaScript execution and does not need a degradable permit. `CacheOp::Wait` is a
follower of an already admitted leader and must not consume another permit.

Cache planning needs a two-phase miss path:

1. Under the cache lock, return ready results and waiting leaders normally. If
   the request would execute, report that admission is required without
   publishing a waiting entry or starting a direct timestamp-duplicate
   execution.
2. Outside the cache lock, try to acquire the degradable leader permit.
3. If admission fails, return the typed deferral. No waiting entry or broadcast
   sender has been created.
4. If admission succeeds, recheck the cache under its lock. Another request may
   have published a usable leader or ready result in the meantime. Follow that
   result and release the unnecessary permit, or attach the permit to the
   resulting published-leader or direct `CacheOp::Go` execution.

This recheck is required for correctness. It avoids holding a cache mutex while
touching admission state, preserves normal cache coalescing, and guarantees that
every published degradable leader already owns capacity.

The leader gate should use immediate admission. When no permit is available,
the backend returns a typed temporary deferral instead of adding another waiter
queue. This is the point of the sub-cap: a synchronized wave should turn into a
bounded number of executions plus explicit backpressure, not a second hidden
backlog.

The permit is held from leader admission until the root query finishes or
fails. It therefore bounds admitted root query trees, including time spent in
the ordinary query limiter, isolate queue, database calls, and separately
scheduled descendants. Cancellation and every error path must release it.

The ordinary query limiter remains in place. A degradable leader must pass both
the degradable leader cap and the existing query limiter. Normal queries use
only the existing limiter. The degradable cap is a subset, not a replacement or
an increase in total query admission.

### Deferral result

Cap rejection needs a distinct internal error or result, for example
`DegradableQueryCapacity`. It may share the existing
feature-temporarily-unavailable transition machinery, but it must remain
distinguishable from search-index unavailability and from generic overload.
The reason must survive through `QueryResult` and `TransitionState`; reducing it
to the existing temporary-unavailability Boolean before transition construction
would lose the information needed for pressure metadata.

The sync worker's current generic retry set includes overload and
rejected-before-execution errors. Mapping this cap to either category would make
the sync worker retry inside `run_update_queries`, hold completion of the
transition, and recreate load without informing the client. The new reason must
instead:

- omit a new result for the deferred query;
- preserve any older result already held by the client;
- allow non-deferred query modifications in the same transition to complete;
- advance the query-set transition normally;
- schedule only the dedicated bounded pressure retry;
- add `serverPressure` to that transition;
- stop retrying after the client removes the deferred query from its query set.

The retry timer may still wake after the final deferred query is removed, but
the worker checks current query state and does not schedule an empty update.

The same validated retry delay drives the dedicated pressure timer and the
`retryAfterMs` value. It does not replace or reuse the independent search/index
feature-unavailable delay.

A custom client that declares itself degradable but ignores the pressure event
will receive periodic server-side retries. The finite leader cap still protects
the backend, but that client will not provide the intended reduction in query
set size.

## Query-cache interaction

The cache waiting entry must retain the admitted leader's effective class. A
ready result is safe to use according to the existing timestamp and journal
checks regardless of which class computed it. Only an in-flight waiting entry
can create a priority inversion.

The wait rules are:

| Incoming query | Waiting leader | Behavior |
| --- | --- | --- |
| Dependency | Dependency | Wait and coalesce |
| Dependency | Normal or degradable | Run a side-effect-free duplicate |
| Normal | Dependency or normal | Wait and coalesce |
| Normal | Degradable | Run a side-effect-free duplicate |
| Degradable | Any class | Wait and coalesce |

The dependency-versus-normal case is the existing dependency-reserve
invariant. The new normal-versus-degradable case is equally necessary: a normal
query must not wait for a lower-class leader that can be queued at later generic
application or isolate gates. Duplicate query execution is safe because queries
are side-effect-free. The bypass should use a closed metric reason and must not
replace or cancel the lower-class leader.

A request rejected by degradable admission never becomes a published cache
leader. Other requests therefore make their own admission decisions instead of
waking from a dropped sender and repeatedly retrying cache leadership. Once a
leader has been admitted and published, degradable followers coalesce behind it
normally.

The second cache check can discover a leader or ready result created while the
request acquired its permit. That branch must release the unused permit before
waiting or returning. Cache size accounting, cancellation, sender drop, and
timeout cleanup remain exact on every branch after a leader is published.

## Application, queue, and scheduler behavior

The degradable class should remain separate from
`SchedulerDependencyClass`. One describes a cooperating root's overload policy;
the other describes a liveness dependency established by the runtime. Combining
them into one Boolean would make precedence and metrics ambiguous.

The degradable-query patch does not add an isolate queue lane. The leader cap
remains strictly below application and isolate-worker shared-base capacity.
The active-JavaScript gate separately applies backend-owned `Dependency`,
`Protected`, and `Degradable` classes. Only an actual admitted cache-miss
leader receives `Degradable`; separately scheduled descendants override it
with the existing backend-derived dependency class.

Protected and degradable minimums are non-preemptive service floors. Either
class borrows every permit that the other class does not need. Dependencies
receive the next grant because they release isolate-holding ancestors. After
both application floors are met, elastic occupancy is balanced between the two
classes, and resumptions precede initial starts within a selected class.

The scheduler exposes at most one external initial waiter per active class so
a wave from one class cannot hide the other class behind serial permit waits.
Each exposed waiter reserves global and per-client worker eligibility until
permit acquisition finishes. Requests that remain queued retain queue expiry
and delay accounting. These mechanics are detailed in
[`active_javascript_admission.md`](active_javascript_admission.md).

This model is intentionally finite. One admitted query can perform expensive
work or separately scheduled fan-out. The cap bounds admitted roots, not every
possible descendant. Existing transaction limits, action caps, dependency
reserve, CPU execution permits, and hard queue age continue to enforce their
own boundaries.

## Configuration and capacity sizing

The backend exposes strictly parsed self-hosted settings:

```text
APPLICATION_MAX_CONCURRENT_DEGRADABLE_QUERY_LEADERS
FUNRUN_ISOLATE_PROTECTED_ACTIVE_THREADS_MIN
FUNRUN_ISOLATE_DEGRADABLE_ACTIVE_THREADS_MIN
```

When absent, the degradable sub-cap is disabled and the backend preserves
upstream behavior even if a new client sends the optional field. The backend
may retain that declaration for bounded telemetry, but root executions use the
normal class, no separate queue lane, and no pressure response. When present,
the value must contain only ASCII decimal digits and must be strictly positive.
Malformed, empty, signed, zero, overflowed, or inconsistent values fail startup.

For the standard in-process self-hosted runner, startup requires the cap to be
strictly below:

- the query application's shared-base limit after dependency overflow;
- the isolate scheduler shared base
  (`MAX_ISOLATE_WORKERS - ISOLATE_DEPENDENCY_WORKER_RESERVE`).

Both active-JavaScript minimums are zero or both are positive. Positive
minimums require finite `FUNRUN_ISOLATE_ACTIVE_THREADS`, require the leader
cap, cannot sum above active capacity, and cannot set the degradable minimum
above the leader cap. With positive minimums the leader cap can exceed active
capacity because the cap bounds admitted tree lifetime while active capacity
bounds instantaneous JavaScript execution. With zero minimums, the previous
validation remains: a finite active capacity must exceed the leader cap.

If a custom runner does not expose equivalent capacity, it cannot claim the
same normal-capacity reservation from this cap alone.

The backend uses one fixed three-second pressure-retry delay for this slice. The
same resolved duration supplies both the sync worker's dedicated degradable
timer and transition `retryAfterMs`; there is no deployment-specific default or
second pressure-retry knob. The existing feature-unavailable timer remains
separate and keeps its existing delay.

There is no universal leader-cap or service-floor value. Select the leader cap
from measured query-tree lifetime and the memory cost of admitted trees, not
from frontend connection count alone. Select active capacity from measured CPU
service and throttling. Select the two minimums from protected and degradable
progress under sustained contention. Raising the leader cap increases
freshness and retained in-flight work; raising active capacity increases
instantaneous CPU demand; changing a minimum redistributes progress only while
both application classes are runnable.

Do not increase the general query limiter to compensate for a lower degradable
cap. The two controls have different purposes.

The self-hosted Compose template should pass the setting through without
silently supplying a deployment-specific value. The backend remains the source
of defaults and validation.

## Frontend operational pattern

The intended application pattern is one central policy for an interactive
client, not annotations on individual queries. Server pressure and inactive
page suspension are separate states because they require different subscription
behavior.

### Client construction

Construct the singleton browser client with:

```ts
const client = new ConvexReactClient(deploymentUrl, {
  queryWorkloadClass: "degradable",
  onServerPressure: handleServerPressure,
});
```

The exact callback option may instead be an event registration method, matching
the final `convex-js` API. Headless workers, watchers, and clients that require
continuous freshness omit `queryWorkloadClass` and remain normal.

The connection-level choice is deliberately broad. It prevents the application
from maintaining a changing list of supposedly important modules and from
making every query author choose a scheduler policy. If one application surface
cannot tolerate this stale-data contract, it should use a client role with
normal behavior rather than adding per-function exceptions to the degradable
connection.

### Pressure state

The pressure handler stores the lifecycle epoch, pending count, first-observed
time, and backend retry lower bound. It does not set the inactive-page pause
bit, pass `skip` to mounted reactive hooks, remove local query results, or
change the query set. Successful subscriptions therefore keep receiving normal
updates while the backend retains and retries only the deferred subset.

An active event for the same epoch updates the pending count and retry timing
without creating a new frontend generation. A different active epoch before a
matching clear is a protocol violation; after clear, the next contiguous epoch
starts a new pressure interval. A matching cleared event ends visible staleness
and emits the frontend catch-up completion event. A stale clear event is ignored.
The application also watches the client's connection count: replacing the sync
worker clears an old connection-local epoch and emits a distinct
`ui.live_data.pressure_connection_reset` event rather than claiming backend
deferred-set recovery.

Inactive-page suspension keeps its existing application semantics: after the
configured hidden-page delay, wrappers may pass `skip`, retain values keyed by
function, serialized arguments, component, and authentication generation, and
perform the existing fresh-result handshake when the page resumes. Pressure
does not reuse that whole-page resubscription handshake.

Optional imperative reads, including application-owned one-shot or TanStack
fetches, may be blocked while pressure is active so they do not create extra
cache-miss leaders. In-flight reads are not cancelled. Mutations and actions
remain available. A page with no result continues to show its normal loading
state together with the pressure indicator; it never invents an empty result.

### User-visible state

While pressure is active, show a fixed bottom-right indicator that live data
may be stale. It includes a `Retry now` action. The action calls
`retryDegradableQueries(epoch)` once; it does not clear local pressure, upgrade
the connection, remove subscriptions, or restart all page queries. The button
becomes unavailable after that epoch's request has been accepted locally.

The application emits bounded `ui.live_data.pressure_active`,
`ui.live_data.pressure_retry`, `ui.live_data.catch_up_complete`,
`ui.live_data.catch_up_blocked`, `ui.live_data.pressure_connection_reset`, and
`ui.live_data.legacy_pressure_elapsed` events. `catch_up_blocked` is delayed by
a bounded threshold and contains only an opaque pressure-interval identifier,
epoch, pending count, enabled-query count, and elapsed time. The frontend creates
one random identifier when an interval starts and retains it across same-epoch
updates and authentication-scoped provider remounts. Active, retry, blocked,
clear, connection-reset, and legacy-expiry rows carry that identifier so browser
events can be paired without treating a connection-local epoch as globally
unique. The identifier is a log correlation field, never a metric label, and
does not encode identity. Pressure telemetry does not include function names,
query IDs, arguments, routes, cached values, or identity.

The application uses two pressure delivery forms with separate purposes. A
latest-value external-store snapshot drives immediate rendering and blocks an
imperative read even before React commits. A single-consumer ordered event
channel delivers every lifecycle event to state reduction and telemetry. The
source buffers at most 100 events before that root consumer attaches and fails
if the provider wiring invariant is broken. This prevents React batching from
turning a fast active/clear pair into an unobservable clear-only snapshot.
Internal clear events retain only the pressure interval's start timestamp, so
duration telemetry survives an authentication-scoped provider remount without
retaining identity. A new provider consumer establishes the source's current
sequence and seeds active UI state without emitting a duplicate active event.

Legacy one-shot pressure from an older backend may produce a bounded temporary
stale indication, but it cannot prove catch-up or support epoch retry. The
production rollout therefore places the lifecycle-capable backend before the
capable frontend. Whole-page staggered resubscription is a rollback fallback,
not the normal lifecycle version 1 behavior.

### Library boundary

The `convex-js` patch exposes the connection option, lifecycle event, and one
epoch-scoped retry method. It does not silently remove subscriptions or retain
framework values. Presentation and optional imperative-read policy remain in
the application. The backend, not the framework, owns membership of the
deferred set.

## Later HTTP-action extension

Lower-importance HTTP reads can compete with important webhooks, but that path is not implemented
by this patch. Use separate reverse-proxy transports and route caps first; they classify traffic
before backend admission without adding another backend protocol.

If several ingress paths eventually require one enforcement point, a later generic extension may
let a trusted proxy remove any caller value and inject
`X-Convex-Overload-Class: degradable` for selected routes. Backend admission would need an optional
strict sub-cap, immediate temporary response with `Retry-After`, bounded metrics, and a rule that
nested callbacks revert to backend-derived dependency service. Application route names must remain
in proxy configuration, not backend code or labels.

Do not classify inside an HTTP handler: the request has already consumed proxy, application, and
possibly isolate capacity. This later extension is independent from the sync protocol and requires
its own implementation and review.

## Interaction with maintained backend patches

The core Connect field, query-cache leader gate, typed deferral, and transition
field can be implemented without changing generic CoDel or adding a deployment
lane. Some guarantees in this document intentionally compose with other
maintained patches.

[`dependency_capacity/README.md`](../dependency_capacity/README.md) supplies the
backend-derived ancestry class, dependency-only application and worker
overflow, and existing dependency-versus-independent query-cache bypass. The
degradable patch should extend those typed scheduling properties instead of
creating a parallel ancestry mechanism. Operators not carrying an equivalent
dependency patch still gain a finite degradable root cap, but cannot claim the
dependency-liveness and descendant-override guarantees described here.

[`isolate_queue_control/README.md`](../isolate_queue_control/README.md) remains independent. This
slice does not add a degradable lane even when lane-aware queue control is
enabled; the leader cap reserves capacity and the existing queue policy remains
unchanged. A later lane extension must not change Connect parsing, leader
admission, cache bypass, or client pressure semantics.

[`cancellation_safe_database_context_reuse/README.md`](../cancellation_safe_database_context_reuse/README.md)
and [`context_reuse_observability/README.md`](../context_reuse_observability/README.md) are
independent service-cost patches. Reuse can change query service time and the
leader-cap value that is appropriate, but it does not change workload class or
pressure semantics. Measure and roll back the features independently.

## Deployment and control-plane interaction

This patch directly addresses the reactive recomputation wave that can follow a
configuration push. Degradable clients absorb part of that wave as temporary
staleness. The deployment analysis and configuration mutation themselves remain
in their existing backend class.

A dedicated control-plane lane is an optional extension implemented by the
separate [`isolate_queue_control/README.md`](../isolate_queue_control/README.md) patch. It
uses backend-owned request variants, finite capacity, and tests across module
analysis, evaluation, schema updates, and application traffic. Client metadata
must never claim that class. The degradable-client patch does not depend on such
a lane and should not encode deployment paths as application allowlists.

Context reuse and bounded query-context prewarming are also complementary.
They can lower module-evaluation CPU and improve warm service time. They do not
provide backpressure, do not prevent every first concurrent cold evaluation,
and do not reserve capacity for normal work. The two features should have
separate opt-ins and telemetry so either can be rolled back independently.

## Metrics and traces

All backend metric labels must use closed enums. Do not label by function,
module, route, component, deployment, session, user, client version, cache key,
or identity.

The implemented backend emits current sync connections by
`query_workload_class={normal,degradable}` and records degradable declarations
as `decision={effective,suppressed_disabled}` according to whether the leader
cap is configured. Both closed current-connection and decision label values are
initialized together. Admission outcome, permits in use, configured capacity,
typed sync deferrals, pressure transitions, cache recheck outcomes including
direct execution, degradable wait classes, and closed bypass reasons are also
emitted. The metrics do not carry application or caller identifiers. Closed
feature counters remain cumulative until process restart rather than
disappearing after an inactive period.

Lifecycle metrics add only closed labels: pressure
`state={active,cleared}`, retry `trigger={timer,client}`, and client retry
`outcome={scheduled,duplicate,stale,inactive,unsupported}`. Active events record
the pending-query count in a histogram; actual retry attempts record their
selected-query count in a second histogram. Counter families initialize every
closed label when a sync connection establishes the compatible backend, even
when admission is disabled; event histograms remain absent until they have an
observation. They never label by epoch because epochs are connection-local and
unbounded across connections.

The query path should expose at least:

- current sync connections by declared
  `query_workload_class={normal,degradable}`;
- declared-degradable execution decisions by
  `decision={effective,suppressed_disabled}`;
- degradable leader admission by `outcome={admitted,deferred}`;
- current degradable leader permits in use and the configured capacity;
- typed temporary deferrals returned to sync workers;
- pressure transitions emitted by closed pressure kind and lifecycle state;
- bounded pending-query-count observations;
- deferred-only retry attempts by timer or client trigger;
- queries selected per actual deferred-only retry;
- client retry decisions by a closed outcome;
- query-cache wait, leader-recheck, and bypass counts by closed reason;
- dependency override is proved by the closed cache execution-class matrix and
  the existing backend-derived dependency scheduler metrics;
- configured active capacity and class minimums, occupancy by active class,
  waiters by active class and phase, and acquisition latency by the same
  bounded labels.

The optional HTTP path should expose current admission, admitted and deferred
totals, and temporary responses for `normal` and `degradable` using bounded
labels. Existing Caddy or proxy metrics should retain route-level operational
detail outside the backend's Prometheus cardinality contract.

Trace properties may record the closed workload class, effective class,
admission outcome, pressure kind, and cache bypass reason. They must not record
raw query arguments or client identity. The implementation should verify that
these fields reach the self-hosted tracing and metrics ingestion path rather
than relying only on a local tracing backend that is not enabled by default.

Operational interpretation should compare:

- degradable admissions and deferrals;
- queue age and rejection by existing scheduler lane;
- sync transition latency and internal retry counts;
- query module evaluation and execution time;
- active-JavaScript occupancy, waiters, and permit wait by class and phase;
- backend CPU throttling and pressure;
- mutation, action, worker, and deployment latency;
- frontend pressure duration and stale-session ratio.

A healthy overload event has bounded degradable leaders, active epochs that
converge to cleared events, stable successful query-set membership, no
dependency shedding, and continued normal progress. A high pressure count with
low CPU can indicate an undersized cap. A high cap with normal queue delay and
CPU throttling indicates that the cap is not preserving enough execution
capacity. Active epochs without clears identify retry or lifecycle convergence
failures rather than proving continuing backend pressure.

## Compatibility and rolling upgrades

The supported combinations are:

| Backend | Client | Behavior |
| --- | --- | --- |
| Old | Old | Existing behavior |
| Old | New, option omitted | Existing behavior |
| Old | New, degradable option present | Unknown `Connect` capability is ignored; existing behavior and no lifecycle signal |
| New | Old | Client is normal; existing behavior |
| New | New, option omitted | Client is normal; existing behavior |
| New | Existing degradable client without lifecycle capability | Degradable admission and legacy one-shot pressure event |
| New | New, degradable option and lifecycle capability present | Degradable admission, active/cleared lifecycle, and epoch retry when configured |

The backend should be deployed first so telemetry and admission are available
before application code depends on pressure signals. The wire additions are
still designed to permit either binary order during a rolling upgrade.

Transition chunking must preserve the optional pressure field because chunks
contain the serialized transition. JSON round-trip tests must cover both direct
and chunked messages.

Custom clients that send an unknown workload class or malformed capability
receive a protocol error. Custom clients that send the supported class but
ignore pressure remain bounded by backend admission and backend-owned retry.

## Failure modes and safety invariants

- A missing client field always means normal.
- A malformed present field never silently becomes normal.
- Client input can opt work down but cannot create dependency or priority.
- A declared degradable class is behaviorally normal while the sub-cap is
  disabled.
- Mutations and actions from a degradable connection never consume degradable
  query permits.
- A descendant needed to complete an admitted root is dependency-classified.
- A normal or dependency query never waits behind a waiting degradable cache
  leader.
- A ready cache value remains reusable under existing validity checks.
- A failed degradable admission never publishes a waiting cache entry.
- A deferred query does not erase an older successful client value.
- A retained frontend value never crosses a query-key or authentication change.
- A deferred query does not prevent successful modifications in the same
  transition from being sent.
- Automatic and manual pressure retries select only the backend-owned deferred
  set, never every query lacking work for another reason.
- Structural feature-unavailable deferral and degradable pressure membership
  remain distinct; a query can move between them without violating sync-state
  subscription invariants.
- Feature-unavailable and degradable timers are independent, and a transition
  containing both reasons preserves both retries.
- A deferred-only retry runs at the current state timestamp and does not take,
  refresh, or invalidate successful subscriptions.
- Recovery with an unchanged result still removes the query from the deferred
  set and can clear the pressure epoch.
- Removing a deferred query reduces the pending count and may clear the epoch.
- A manual retry is accepted at most once for the current active epoch and does
  not mutate query membership.
- A normal update takes precedence over a pending deferred-only retry and may
  recover that set through the existing update path.
- Every success, error, cancellation, timeout, disconnect, and task-drop path
  releases the degradable permit and removes owned waiting-cache state.
- Pressure metadata has bounded size and contains no application identifiers.
- Missing pressure metadata does not clear active lifecycle state.
- Server pressure does not pause the socket, successful subscriptions,
  authentication, mutations, or actions.
- Manual retry does not grant normal admission.
- Dependency reserve, hard queue age, and total application limits remain
  finite.
- Disabling the feature restores current admission without a data migration.

## Resource cost

The backend adds one closed enum to sync connection state and explicit query
execution metadata, one finite permit counter, class data on a waiting cache
entry, and bounded metric updates. Two-phase miss planning can take the cache
lock twice when a new degradable leader is needed; ready and ordinary waiting
paths remain single-pass. The patch adds no module or client allowlist and no
per-identity queue. The optional pressure object adds a small amount of JSON
only to transitions that observed a deferral.

The cache bypass can duplicate a side-effect-free query when a higher-class
request arrives behind a degradable leader. This is an intentional CPU cost to
avoid priority inversion. It is bounded by actual higher-class arrivals and
should be measured by reason. Sustained bypass volume can indicate that the
degradable cap is too low, a query key is shared across client roles, or a
degradable leader has unexpectedly long service time.

The backend adds two bounded query-ID sets per sync worker: one structural
deferred set and one degradable subset, plus one connection-local pressure
epoch state. IDs already belong to the query set and are never copied into
telemetry or the wire protocol. The frontend needs one latest pressure snapshot,
one bounded pre-consumer event buffer, one exact lifecycle consumer, and one
bounded blocked-event timer; it does not create a timer, connection, or
resubscription sequence per query.

## Rollout and rollback

1. Apply the backend protocol, classification, cache, admission, and telemetry
   patch with the sub-cap unset. Verify that normal traffic is unchanged.
2. Release the matching `convex-js` version with lifecycle capability and retry
   support; keep application opt-in disabled by default.
3. Verify old/new protocol combinations, legacy one-shot pressure, lifecycle
   active/cleared events, reconnect, and transition chunking.
4. Configure a conservative degradable leader cap on one self-hosted backend
   population and restart. Confirm the configured-cap metric.
5. Mark one interactive client role as degradable and record admission,
   deferral, active/cleared lifecycle, deferred-only retry, cache bypass, queue,
   CPU, and normal-work latency.
6. Add the central frontend stale indicator, epoch retry, bounded blocked
   telemetry, and optional imperative-read suppression. Verify that server
   pressure does not remove successful subscriptions.
7. Exercise an ordinary function deployment while representative reactive and
   worker traffic is present. Verify bounded degradable work and continued
   mutation, action, worker, and deployment progress.
8. Adjust one cap at a time from measured behavior. Do not simultaneously raise
   general queue delay or query concurrency.
9. Adopt degradable HTTP-action admission separately, starting with proxy
   header enforcement and one lower-importance route class.

The fastest client-side rollback is to omit `queryWorkloadClass`. Existing
connections must reconnect under a newly constructed client for the change to
take effect. The fastest backend rollback is to unset the degradable leader cap
and restart. Either rollback leaves data, schemas, subscriptions, and existing
normal admission unchanged.

If a lifecycle-capable client retains an active epoch during a backend rollback,
reconnection resets the connection-local epoch. The frontend also bounds its
legacy one-shot indication because an old backend cannot emit explicit clear.

The optional HTTP path rolls back by removing the proxy-injected header or
unsetting its cap. Removing the header returns those routes to normal admission
and can increase competition immediately.

## Verification boundary

Focused deterministic tests should cover the contract without requiring a
custom stress fixture.

Backend protocol tests:

- absent, valid, and invalid `queryWorkloadClass` values;
- absent, valid, and invalid lifecycle capability values;
- absent, valid, stale, and malformed epoch retry messages;
- a current backend ignoring a future unknown `Connect` property;
- transition JSON with legacy, active, cleared, and absent pressure;
- direct and chunked transition round trips;
- old-client parsing of a transition with the optional field.

Backend admission and cache tests:

- only degradable `CacheOp::Go` operations consume the new permit;
- ready results and waiting followers do not consume another permit;
- cap exhaustion returns the typed deferral immediately;
- cap exhaustion does not publish a waiting entry;
- the post-admission cache recheck releases an unnecessary permit when another
  request became the leader;
- all error and cancellation branches release permits and waiting entries;
- normal and dependency callers bypass a waiting degradable leader;
- degradable callers may wait for any leader;
- existing dependency-versus-normal bypass remains unchanged;
- dependency descendants of a degradable root use dependency reserve;
- mutations and actions from the same connection remain normal.

Sync-worker tests:

- a degradable-cap deferral does not enter the generic overload retry loop;
- successful query updates in the same transition are still sent;
- an older client result is not replaced with an error or empty value;
- pressure metadata is emitted only for the degradable-cap reason;
- repeated deferral retains the same active epoch;
- unchanged successful recovery emits clear;
- removing deferred queries updates or clears pressure;
- timer and client retries execute only the deferred set at the current state
  timestamp and leave successful subscriptions mounted;
- stale and duplicate client retry requests do not schedule work;
- reconnect starts with no active epoch;
- removing deferred queries stops later unavailable retries.

`convex-js` tests:

- the option and lifecycle capability are serialized on initial connect and
  reconnect;
- omission preserves the old wire object;
- legacy, active, and cleared pressure parse with strict bounded fields;
- pressure invokes the dedicated handler after transition application;
- one retry for the current epoch serializes correctly and duplicates are
  rejected locally;
- unknown optional transition properties remain tolerated;
- mutations and actions continue during pressure.

Frontend tests cover that server pressure does not produce `skip`, remove local
results, or restart successful queries; inactive-page suspension still does.
They also cover same-epoch updates, epoch replacement, explicit clear,
epoch-scoped retry, bounded blocked telemetry, imperative-read suppression, and
argument/login/logout/identity changes for any retained inactive-page value.

HTTP extension tests:

- header absence remains normal;
- the supported value uses the sub-cap;
- malformed values fail before handler execution;
- cap exhaustion returns the documented temporary response and `Retry-After`;
- a nested callback receives dependency treatment;
- proxy configuration strips caller input and injects the route-selected value.

Production-shaped canary verification should use ordinary application traffic
and one deployment or invalidation event. It should confirm metrics at both the
backend endpoint and the remote metrics store. A dedicated synthetic stress
fixture is not required before carrying the patch, but deterministic saturation
tests of the finite permit and cache state machine remain part of backend unit
coverage.

## Rejected alternatives

### Lower the global query concurrency limit

A lower `APPLICATION_MAX_CONCURRENT_QUERIES` applies to important root queries
from workers and watchers as well as page subscriptions. It cannot express
accepted staleness and can move the same bottleneck to a generic application
wait timeout. The degradable leader cap is additive containment for an explicit
client role and leaves the normal limit unchanged.

### Mark background modules or individual functions

A module or function is not inherently lower importance. The same query can be
called by an interactive page, a worker, an action callback, or an operator.
Markers also create an ongoing review burden, drift as imports and callers
change, and encourage backend allowlists. Connection-level opt-down describes
the caller's freshness contract without encoding application source names.

### Give all mutations and actions priority over queries

UDF type is not a reliable progress or importance signal. A mutation can be a
low-value UI write, while a query can drive a worker or unblock an action.
Strict type priority can starve queries and still cannot distinguish an
important webhook from a lower-importance HTTP read. The proposed design
reserves capacity by opting one known stale-tolerant class down.

### Prioritize actions that eventually call a mutation

The backend cannot know before execution which branch an action will take or
whether it will call a mutation. Classification after the call begins is too
late to solve admission. Maintaining a static action call graph across dynamic
JavaScript and components would be a large, fragile analysis project.

### Treat every sync-worker query as degradable

Browser clients are not the only users of the sync protocol. Headless workers
and watchers may require continuous query progress. The optional connection
field lets those clients remain normal without a per-query policy.

### Detect frontend origin from user agent, IP address, or authentication

These properties are deployment-specific, easy to proxy or change, and do not
state whether stale data is acceptable. They also risk high-cardinality policy
and telemetry. An explicit opt-down value is stable and auditable.

### Add a positive priority client tag

A client-asserted priority value creates a privilege and abuse problem. It also
requires deciding whether untagged existing clients are low priority. A
degradable-only value preserves the current default and can only reduce the
declaring client's service.

### Put application module allowlists in the backend

Backend module allowlists are specific to one deployment, require synchronized edits with
application code, and are difficult to maintain on top of upstream. Application query policy
belongs to the client role.

### Add a new server-message variant

Old JavaScript clients use a discriminated union and exhaustive message switch.
An unknown variant can terminate or corrupt the connection. An optional field
on a transition follows the existing compatibility behavior and naturally
accompanies query-set progress.

### Return a generic overload error and let the sync worker retry

Generic overload and rejected-before-execution errors are retriable inside the
sync worker. Repeated internal retries consume backend work, delay the entire
transition, and give the frontend no stale-data state. A distinct temporary
deferral allows the transition to complete, records the exact backend-owned
retry set, and gives the frontend an explicit stale-data lifecycle.

### Increase queue depth or delay targets

Larger or older queues can replace immediate rejection with longer latency, but
they do not increase execution capacity or reduce a synchronized invalidation
wave. This design bounds cache-miss leaders and retries only the deferred set
instead of rebuilding the whole query set.

### Splay every subscription or add more server-side coalescing first

Splaying can reduce synchronization, and broader coalescing may help workloads
with equivalent queries. Neither defines which callers accept staleness, and
distinct identities, arguments, timestamps, and journals still produce
separate leaders. These optimizations can complement explicit backpressure but
do not replace its capacity contract.

### Remove and recreate every page subscription on pressure

Whole-page resubscription reduces occupancy temporarily, but it also discards
backend knowledge of which queries were actually deferred. When the page
returns, every successful query becomes eligible for fresh work and can
recreate the synchronized wave. Backend-owned deferred-set retry preserves
successful subscriptions and makes completion explicit. Staggered whole-page
resubscription remains a rollback fallback for a backend without lifecycle
support, not the normal design.

### Pause or reconnect the entire JavaScript client

Closing or pausing the socket also delays authentication, mutation responses,
actions, and recovery state. Removing only query subscriptions still amplifies
recovery. The intended behavior keeps the socket and successful subscriptions
operational while the backend retries its deferred set.

### Make `convex-js` automatically pause every degradable client

The client library does not know how an application should label stale values,
combine visibility and pressure state, or present manual retry. Silent
automatic pause would hide an important freshness contract and amplify recovery
by replacing the whole query set. The library exposes the event and bounded
retry primitive; applications implement one central visible policy while the
backend owns deferred membership.

### Use context reuse or module prewarming as the overload control

Context reuse and prewarming can lower service cost and improve warm latency.
They do not cover every module, eliminate the first concurrent cold wave, or
tell clients to reduce subscriptions. They are useful performance patches but
not admission or backpressure mechanisms.

### Create a deployment-specific control-plane allowlist

Deployment work may deserve a backend-authenticated lane, but matching module
names or HTTP paths in a self-hosted patch is not a maintainable definition of
control-plane work. That feature should be designed separately from internal
request types and authenticated backend boundaries.
