# Application-Declared Pinned Local Node Executor Pools

Status: implemented; named pools require an application declaration and an
operator-provided total Node RSS budget covering every steady slot and one
global hot-replacement surge slot.

This patch lets a root application assign a Node action module to a dedicated
local executor pool:

```ts
"use node";
"use node pool:consumer";
```

The declaration applies to the complete module, so every export uses the same
pool. Modules without a pool declaration use the default local Node executor.
The application declares required runtime isolation; this routing patch asks
the host only for a generic resource limit. There is no host module allowlist,
pool-name allowlist, or duplicate route configuration. The optional
[`local_node_executor_pool_admission`](../local_node_executor_pool_admission/README.md)
patch can separately attach host capacity and health policy to those declared
names without changing their routes.

A named process can retain rebuildable module-level state between invocations,
but it remains disposable. Timeouts, health failure, memory or lifetime limits,
topology changes, source or environment changes, backend shutdown, and process
failure can remove it. Process-local state must not own authorization, durable
truth, distributed locking, or a required data watermark.

Healthy generation changes use hot replacement. One global coordinator starts
and prepares a candidate while the current generation remains available,
promotes the candidate, then lets the old generation drain. It serializes this
temporary overlap across the default and named pools.

## Patch composition

The patch reuses
[`local_node_executor_resilience`](../local_node_executor_resilience/README.md)
for generation ownership, admission closure, drain, termination, reaping,
candidate preparation and promotion, health checks, diagnostics, and memory
and lifetime limits. It reuses
[`atomic_node_executor_source_packages`](../atomic_node_executor_source_packages/README.md)
for package publication and package-cache identity. It extends
[`backend_memory_resilience`](../backend_memory_resilience/README.md) with a
total planning allowance for every possible steady local Node process slot and
the global surge slot.

The patch also completes its composition with scheduled and registered-cron
action admission. Their durable start barrier now extends through Node routing,
package preparation, and exact resident-generation admission. A request that
loses to candidate promotion ends before `ready` while its job remains
`Pending`; a request admitted first retains the same linear generation guard
through `/invoke`, so normal promotion waits for it to drain. Topology can be
published before either ordering resolves. Hard generation loss is still
conservative after a visible claim. The ordering and failure contract are
specified in
[`scheduled_node_action_admission.md`](scheduled_node_action_admission.md).
The current preparation endpoint does not retain its zero-owner package-cache
lease across the claim; the supplemental document records that remaining
package-authority limitation separately from the completed generation fence.

## Source declaration and bundling

The CLI parses directive prologues on each function entry module. A pool
declaration is valid only when the same module also declares `"use node"`.
Pool names must match `[a-z][a-z0-9_]{0,31}`, and `default` is reserved.
Schema, HTTP, cron, auth configuration, dependency chunks, and component
modules cannot select a named pool. Components continue to reject Node modules.

The bundler records the declaration on the esbuild output whose metadata names
that entry point. Shared and dependency chunks have no pool declaration. This
keeps module ownership unambiguous: the entry module selects the process, while
the complete source package remains available inside that process.

Pool metadata participates in changed-module comparison. Moving a module to or
from a pool therefore cannot be sent as an unchanged hash with stale routing.
The changed and unchanged deploy representations both carry the selected pool.

The wire `environment` value for a pooled module is
`node:pool:<pool_name>`. A new CLI cannot silently deploy a required pool to an
older backend: an older backend rejects this unknown required environment value.
The explicit `nodePool` field is also sent for validation, but it is not the
compatibility boundary because older Serde structures can ignore unknown
optional fields. CLI deployment schemas reject invalid or reserved explicit
names and reject an explicit field that disagrees with the required marker.

The same required marker is retained in durable module metadata and in the
source-package archive's `moduleEnvironments` entries. The current Node
executor recognizes a valid pool-bearing marker as a Node module, while an
older executor rejects the unknown environment. The optional pool field is
repeated in those formats for agreement checks, not used as the capability
boundary.

The backend also reads the retained directive prologue from each bundled entry
module. It requires a pool declaration to agree with the wire metadata and to
have a separate `"use node"` directive. It rejects retained pool declarations
on schema, HTTP, cron, auth configuration, application definition, and
component modules. Dependency chunks are excluded from source scanning because
their directive prologues can come from imported packages; they still cannot
carry pool metadata. Esbuild retains and normalizes entry directives in output,
after an entry hashbang when one is present; backend scanning skips that
hashbang before validating the directive prologue. This reverse check is
necessary for older CLIs: a CLI that understands only the exact `"use node"`
directive otherwise leaves `"use node pool:<name>"` in the bundle while
silently sending ordinary Node metadata. Such a deployment is rejected instead
of losing its required pool assignment.

## Durable topology

The backend parses the pool-bearing environment into the ordinary Node module
environment plus a validated optional pool name. The pool name is retained in:

- `ModuleConfig` and `ModuleHashConfig`;
- source-package `metadata.json`, including restored unchanged modules;
- the complete canonical module-to-pool topology on the durable
  `SourcePackage` record;
- the exact canonical paths and count of ordinary Node action modules routed
  to the default pool;
- durable `ModuleMetadata`; and
- the internal `ExecuteRequest` selected from committed module metadata.

Old source packages and module records deserialize with no pool assignment.
Source packages and module records written by the first version of this
protocol, with an ordinary `node` environment plus optional pool metadata,
also remain readable and are rewritten with the required marker. First-version
source packages with assignment arrays but no default-route count remain
readable. A default-route count without both assignment arrays is rejected as
an incomplete protocol record. Count-bearing records without exact default
paths also remain readable; reconciliation uses their count conservatively
until the next deployment writes exact membership. The first transition
between count-only and exact default membership retires the default generation
even when the counts match, because equal counts cannot prove equal membership.
New records retain exact membership as `nodePoolDefaultModulePaths` and require
its length to equal `nodePoolDefaultRouteCount`. Durable module metadata cannot
assign a pool to a non-Node environment. Package download rejects duplicate or
orphaned environment and pool entries, overlapping default and named
assignments, and verifies that archive metadata agrees with durable
source-package topology.
It also rejects duplicate archive metadata, modules, or source maps and source
maps without a module. The Node package consumer independently rejects
duplicate module paths, environment entries, and source maps without a module,
and requires supplied environment entries to name exactly the archived
JavaScript modules; restored or prebuilt packages therefore cannot bypass the
Rust download boundary with ambiguous environment metadata.
Module content equality includes the pool assignment, and source-package
metadata equality includes the complete topology.

`finish_push` downloads every package after the client round trip and replaces
its round-tripped topology with the complete topology reconstructed from the
archive. The root topology used after commit is derived only after that
normalization, then validated again against the runtime pool limit and host
process budget before the commit transaction starts. A client that omits newly
added optional topology fields therefore cannot preserve an incomplete older
record or bypass the runtime capability check. Downloaded component archives
are also rechecked as isolate-only before commit, so the round trip cannot add a
Node component.

The complete proposed root topology is reconstructed before analysis and
schema work. The active `NodeExecutor` validates it before the deployment can
commit. A deployment can contain at most eight distinct named pools. A runtime
that does not implement dedicated pools rejects every topology with a named
assignment while continuing to support ordinary default-routed Node modules.

After package download, archive normalization, analysis, and resource
validation, every routed deployment reserves the global surge slot and
installs an exact router-visible claim before its durable deployment
transaction. Cold acquisition is immediate, but still reserves capacity so a
resident generation created by concurrent analysis or dependency work between
admission and the final resident scan can be included without competing for
capacity after commit.
Every successful root push commits a new source-package identity, so every
resident generation requires cutover. Admission waits at most 120 seconds. If
an earlier candidate or draining generation still occupies the global surge
slot, the backend returns the typed
`NodeExecutorCutoverCapacityUnavailable` error and does not start the commit:

> Deployment was not applied because Node executor cutover capacity remained
> occupied by an earlier generation for 120 seconds. Wait for its active
> actions to finish, or retry with `--force-node-cutover` to terminate
> superseded actions.

The optional `--force-node-cutover` deployment flag cancels and reaps an
unpromoted routine candidate or terminates and reaps a superseded old
generation that is already draining, including one left by an earlier
deployment promotion. It obtains capacity only after confirmed reaping. It does
not kill the current serving generation before a ready candidate exists and
does not steal a candidate promotion already owned by another deployment. The
CLI warns that an interrupted action may have completed
external effects without returning a result; the operator must read
authoritative state before retrying it. The start-push response advertises
cutover protocol version 1. The CLI rejects the force flag when that capability
is absent, so an older or unsupported backend cannot silently ignore the
operator's request.

Dry runs, failed analysis, abandoned pushes, failed admission, and failed
commits do not change the running router. An uncommitted claim may temporarily
displace failed-cutover recovery, but cancellation restores that exact recovery
owner before releasing capacity. Commit converts only the matching claim to a
committed claim. The reservation retains it until runtime ownership is
installed, so cancellation after commit restores the committed version as
recoverable pending work instead of exposing capacity to a later deployment.

After a successful commit, deployment code synchronously publishes the new
topology and commit timestamp. Before its first await, it moves target
reconstruction, the held reservation, and cutover completion into a detached
runtime task. A candidate prepares the incoming source and external packages
without importing or invoking an application module. Affected pools replace
one at a time through the global surge slot. The router retains the deployment
session lease while each local candidate and drain holds a clone, and it does
not reuse that session for the next pool until the prior extra direct child is
confirmed reaped. The caller waits for this rolling cleanup before returning,
but caller cancellation cannot cancel runtime ownership or return capacity.

An unexpected failure after commit is reported as
`NodeExecutorCutoverFailedAfterCommit`, with the fact that the deployment
committed before the cutover error. It is never wrapped as an ordinary failed
deployment. Publications are version ordered, so a delayed older deployment
cannot replace a newer topology.

Node execution also reconciles from the same repeatable database snapshot that
supplied its module and source-package metadata. A request that observes a
committed topology newer than the router waits for publication and the matching
pool cutover instead of running in an incompatible generation. An older request
is rejected when its source-package topology no longer agrees with the
published topology. An older request with the same complete topology is
admitted to the matching current generation while its admission remains open;
this prevents unrelated newer action snapshots with unchanged topology from
invalidating concurrent work. The router also verifies that the request's
module-to-pool selection agrees with that topology. Named slots reconcile the
request's source-package and environment fingerprint before admission.

Execution-side reconciliation closes the gap if a deployment caller is
canceled after its durable commit completes. It joins or reconstructs the same
deployment-priority cutover. If execution installs runtime ownership before the
deployment caller reaches post-commit handling, the caller transfers its held
pre-commit permit directly to that runtime owner without releasing it to queued
work. Runtime ownership identifies topology, source package, and effective
environment together, so a request cannot join a same-version cutover for a
different target. Two publications at the same version must carry the same topology;
disagreement fails as an internal invariant violation rather than being treated
as a duplicate. Topology publication installs runtime-owned candidate and drain
ownership, so cancellation of the deployment or action request does not cancel
cleanup.

At backend startup, the local backend loads the latest committed root source
package after database initialization. It validates that topology before it
constructs and enables the router. Invalid committed topology or insufficient
configured budget fails startup instead of moving modules to the default pool.

## Routing and lifecycle

`RoutedLocalNodeExecutor` owns one default `LocalNodeExecutor` and a dynamic map
of named logical slots plus one global surge coordinator. Named logical slots
are created when a committed topology first uses their name. Their steady Node
children remain lazy until the first selected action. The surge process is also
lazy. Analysis and dependency builds always use the default slot and cannot
replace resident state for an uncommitted candidate package. They do not carry
a committed execution fingerprint. A utility-only generation created after a
deployment's final resident scan can remain resident, but the next `Execute`
request performs fingerprint reconciliation before it can serve the action.

An execute request carries the pool selected from durable module metadata. The
router verifies that the request agrees with its committed topology, then
selects that named slot or the default slot. It does not reparse source, consult
host routes, or fall back when a named slot fails. For new topology records it
also requires a default request's module to appear in the exact default-route
set. Older records without exact membership retain the legacy unassigned-module
check until a deployment upgrades their metadata.

Every named generation has the fingerprint:

```text
(SourcePackageId, SHA-256(canonical effective environment map))
```

The environment encoding is versioned, ordered, and length-prefixed. The
fingerprint is Rust-only and does not change the Node JSON protocol. A matching
fingerprint reuses the current generation. A different fingerprint creates or
joins a hot-replacement transition. Requests for the new fingerprint wait for
its candidate, while requests still eligible for the current fingerprint can
continue until promotion. The candidate passes health checks and prepares the
incoming source and external packages. One generation-state transition then
promotes it and closes old admission. Waiter cancellation does not cancel the
runtime-owned candidate, promotion, or drain.

Topology reconciliation compares the old and new assignment for every module.
It hot-replaces the default generation when a module moves to or from default
and when exact default membership changes. If either record predates exact
default paths, a count change, a precision change between count-only and exact
membership, or a named assignment entering or leaving the unassigned route
conservatively replaces the default generation. It replaces every affected
named generation. Removed named slots close admission and drain even if they
receive no later request. The operation does not permanently shut down a
reusable name; a later committed topology can create a fresh lazy slot with
that name.

The global surge coordinator issues one shared session lease for one
unpromoted candidate or one promoted but unreaped old generation across all
pools. The router can retain a deployment-session owner while the active local
candidate or drain holds a clone. It waits for confirmed per-pool cleanup
before passing a clone to the next pool, and only the last owner releases the
coordinator. The lease covers candidate startup, package preparation,
promotion, old drain, termination, and confirmed reaping.
Routine `age_limit`, `package_limit`, and healthy-headroom `rss_limit`
transitions queue and coalesce per logical slot. Duplicate triggers do not
create candidates, and a newer source or environment target replaces an older
queued target. Deployment cutovers have priority over queued routine work.

An unhealthy current generation bypasses the surge queue. It closes admission,
terminates, and reaps in its ordinary process slot before replacement. Actual
cgroup pressure cancels an unpromoted candidate and immediately terminates a
promoted old generation or removed-pool generation that is still draining.
Neither path reports the surge slot free before direct-child reaping.

Repeated topology changes join or supersede identity-fenced transition targets
rather than building retirement dependency chains. A candidate promotes only
if its own transition's expected current generation, topology version, source
package, and environment fingerprint still match. A newer source or
environment snapshot does not join an older running target. It waits for that
identity-fenced transition, then performs the required follow-up replacement
before executing. A stale candidate is terminated and reaped while the valid
current generation remains serving. A canceled or panicked runtime-owned
candidate, drain, or termination task publishes failure and wakes surge and
request waiters rather than leaving them waiting indefinitely.

Execution readiness is fenced first by the router's complete topology, source,
and environment target, then by the selected pool's current admitted generation
matching that source and environment. The matching generation may be reused or
newly promoted. The request does not wait for that pool's old generation or
unrelated pools to finish cleanup. Actions already assigned to the old
generation can overlap with replacement actions while they finish.
The deployment cutover remains incomplete and retains its surge and cleanup
ownership until every affected old child is reaped. Requests for a pool whose
target is not ready still wait for that pool's turn in the serialized cutover.
This is safe only because process-local state is rebuildable and cannot own
durable authority. An old action that reaches its existing absolute deadline
terminates the old generation and releases surge capacity after reaping.

Backend shutdown wakes requests waiting for topology publication or surge
capacity, cancels candidates, closes every current slot, and upgrades draining
generations to immediate child termination and reaping. `NodeExecutor::shutdown`
remains synchronous and does not wait for active actions or child cleanup.
Runtime-owned owners continue the work; process exit remains the final fallback.

The existing application-wide Node action limit remains outside pool selection.
The patch does not add a per-pool queue, durable actor, singleton guarantee,
throughput pool, or new Node callback API. Candidate package preparation is a
local lifecycle protocol and does not invoke application code.

## Resource policy

Each pool inherits the existing local executor timeout, old-space, RSS,
pressure, age, imported-package, health, and diagnostic settings. In this
routing patch, the only pool-wide host policy is:

```text
LOCAL_NODE_EXECUTOR_TOTAL_RSS_BUDGET_BYTES
```

The default covers the default steady slot and one global surge slot. For `N`
distinct named pools, a deployment requires:

```text
(2 + N) * LOCAL_NODE_EXECUTOR_MAX_RSS_BYTES
```

The required product must not exceed
`LOCAL_NODE_EXECUTOR_TOTAL_RSS_BUDGET_BYTES`. The backend checks the proposed
deployment before commit and the committed topology during startup. The Linux
startup memory-feasibility calculation reserves the configured total Node RSS
budget directly, including capacity for lazy steady slots and the lazy surge
slot that have no child yet. The surge reserve is one complete
`LOCAL_NODE_EXECUTOR_MAX_RSS_BYTES` allowance, not the expected smaller RSS of
a fresh process. This is a planning allowance, not a hard aggregate limit;
sampled child RSS can grow between checks and while requests drain.

All pools receive the same cgroup-pressure signal and apply their ordinary
per-generation pressure rules independently. Pressure also cancels the global
candidate or terminates its draining old generation before additional external
admission is shed. Named pools isolate JavaScript event loops and process
globals, but they still share CPU, cgroup memory, database capacity, outbound
network, the global surge slot, and the application action limit.

## Observability

Every per-pool local executor lifecycle, health, RSS, package-cache,
diagnostic, waiting, and active-request metric has a validated `pool_name`
label. The default slot uses `default`; named values come only from application
declarations. Named values can still churn across deployment history, so
counters, histograms, and runtime gauges use the metrics inactivity-eviction
mechanism. Removed configuration and route gauges delete their label sets
immediately, including all current pool labels when the router shuts down.
Runtime state gauges instead age out after inactivity. In particular, candidate
presence remains one if exceptional task loss leaves its identity-fenced
transition present and potentially owning an unconfirmed child; only ownership
resolution can publish zero. The topology and surge-coordinator families below
are application-wide and do not carry `pool_name`; there is one router and one
surge slot.

Additional families report:

- whether a pool is present in the committed topology;
- the number of committed application modules assigned to each pool;
- requests selected for each pool and request kind; and
- fingerprint transition outcomes: `startup_failed`, `replacement_ready`,
  `joined`, and `retirement_failed`.

The topology and surge-coordinator protocol adds these bounded series:

- `local_node_executor_topology_publications_total{outcome}` counts applied
  topology changes and stale or duplicate publications ignored as
  `applied`, `ignored_stale`, or `ignored_duplicate`. Execution-side
  reconciliation from a newer snapshot with unchanged topology only advances
  the ordering watermark; it does not increment this counter on every ordinary
  request. Repeated ignored reconciliations at the same snapshot version are
  coalesced to one counter increment and debug event.
- `local_node_executor_configured_named_pools_info` is the current number of
  configured named pools. Shutdown sets it to zero.
- `local_node_executor_surge_slot_info{phase}` reports whether the one global
  slot is `unused`, held by a `reservation` before commit or between pools in a
  deployment session, starting or preparing a `candidate`, or held by a
  `draining` old generation. Exactly one bounded phase can be one.
- `local_node_executor_surge_queue_info{priority}` reports queued `routine` and
  `deployment` transitions. Repeated routine triggers for one logical pool
  count once.
- `local_node_executor_surge_wait_seconds{priority,outcome}` records `acquired`
  and `canceled` waits. A deployment admission timeout closes its pending wait
  as `canceled` here and separately records `timed_out` in the deployment
  cutover event family.
- `local_node_executor_deployment_cutover_events_total{event}` records
  `admitted`, `timed_out`, `forced_candidate_cancel`,
  `forced_drain_termination`, `promoted`, and `post_commit_failed`. Admission
  events occur once per deployment attempt; promotion events occur once per
  affected resident pool.

An applied topology change emits one info event with `topology_version`, the
bounded `outcome`, configured named-pool count, changed route count including
exact default-membership changes when available, affected named-pool count,
whether the default pool was affected, and the bounded cutover phase. An
ignored stale or duplicate publication emits a debug event with only its
version, the current version, and bounded outcome. Candidate start, readiness,
promotion, old drain completion, admission timeout, forced reclamation, and
post-commit failure emit lifecycle events with only the validated pool name,
bounded outcome, and lifecycle context. New topology logging never includes
module paths, source-package IDs, environment data, fingerprints, request
arguments, or raw errors, and there is no per-request info event.

The route gauge includes ordinary Node action modules assigned to the default
pool. Older durable source-package records do not have this count; they omit the
default route series rather than reporting a false zero until the next deploy.
Module paths, source-package IDs, environment names and values, fingerprints,
and request arguments are not metric labels. Lifecycle errors identify only
the validated pool name and generation.

## Failure behavior

- Invalid directive combinations and pool names fail CLI bundling.
- A retained bundled pool directive that is absent from or disagrees with wire
  metadata fails backend deployment validation, including bundles from older
  CLIs.
- A retained pool declaration without a separate `"use node"` directive, or on
  a static or component module, fails backend deployment validation.
- A pool declaration without the required wire environment marker is rejected.
- Pool metadata that disagrees across the environment marker, explicit field,
  archive, durable package, unchanged hash, or module record is rejected.
- Duplicate or orphaned archive environment and pool entries are rejected.
- More than eight named pools or a topology whose steady and global surge slots
  exceed the total RSS budget is rejected before deployment commit.
- A deployment that cannot obtain cutover capacity within 120 seconds fails
  with `NodeExecutorCutoverCapacityUnavailable` before its commit begins.
- Forced cutover can cancel an unpromoted routine candidate or terminate a
  superseded draining generation, but it cannot kill the only serving current
  generation before a candidate is ready.
- An unexpected cutover failure after commit reports
  `NodeExecutorCutoverFailedAfterCommit` and does not claim that the deployment
  was not applied.
- Unsupported runtimes reject named topology rather than using default.
- A request that disagrees with the committed router topology fails rather than
  running in another process.
- Named generation startup, preparation, promotion, drain, termination, and
  reaping failures propagate; there is no fallback to default.

These boundaries preserve the declaration as a required capability and prevent
ambiguous routing. After promotion, only the replacement accepts new work; an
old process can overlap only while its previously assigned actions finish.

## Verification

Focused coverage includes:

- directive parsing, required `"use node"`, name validation, and entry-output
  association, including bundled entries with a hashbang;
- dependency chunks receiving no pool;
- changed-module comparison including the pool-bearing environment and explicit
  pool field;
- wire parsing and older environment rejection behavior;
- current-executor acceptance of valid pool-bearing archive environments;
- old archive and durable-record compatibility with no pool or with the first
  pool protocol's optional-only metadata;
- archive and durable topology agreement;
- duplicate and orphaned archive module/environment metadata rejection at both
  Rust and Node package boundaries;
- distinct-name and steady-plus-surge RSS-budget limits;
- startup loading of committed topology;
- failed or dry-run deployment leaving routing unchanged;
- pre-commit cutover admission and 120-second timeout leaving durable
  state unchanged;
- start-push capability negotiation rejecting forced cutover against an older
  or unsupported backend;
- forced cancellation of an unpromoted routine candidate and forced
  termination of a superseded draining generation, with confirmed reaping and
  no serving-current termination;
- post-commit reconciliation and rolling candidate promotion for add, remove,
  and move transitions;
- explicit committed-cutover failure reporting after an unexpected post-commit
  error;
- post-round-trip reconstruction of the archive's complete topology;
- post-round-trip runtime and process-budget revalidation;
- commit-version ordering, post-commit request waiting, matching older-request
  admission, and stale topology rejection;
- removed slots closing admission and draining without a later request;
- exact default membership changes hot-replacing the default generation without
  replacing it for unrelated named-module churn;
- count-only compatibility changes conservatively hot-replacing the default
  generation, including the first equal-count transition to exact membership;
- one global surge permit serializing default and named transitions, routine
  trigger coalescing, and deployment priority over queued routine work;
- candidate package preparation before promotion without application-module
  import or invocation;
- same-fingerprint reuse, changed-fingerprint hot replacement, stale-candidate
  fencing, matching-target admission before old cleanup, and old-request overlap
  only after promotion;
- unhealthy retirement bypassing the surge queue and cgroup pressure canceling
  candidates or terminating promoted and removed-pool draining generations;
- topology and surge waiters waking on shutdown while synchronous shutdown
  remains non-waiting;
- no named-pool fallback; and
- finite-TTL runtime metric labels, removed configuration label deletion, and
  the default-pool route count.

Run the narrow TypeScript checks for `npm-packages/convex`, the `model`,
`node_executor`, `application`, and `local_backend` Rust tests and checks, and
the existing package-publication and local-generation lifecycle tests.

The bundled CLI and the separately published CLI source carry the same pool and
forced-cutover protocol.

## Considered alternatives

Per-pool surge slots were rejected for the initial implementation because they
multiply reserved memory. One global slot removes the routine availability gap
and supplies evidence for adding capacity later if rolling cutovers are too
slow.

Starting all deployment candidates simultaneously was rejected for the same
reason. With one surge slot, affected resident pools replace serially while
their valid current generations remain available.

Waiting for capacity only after durable commit was rejected because the caller
could receive an ordinary deployment failure after the new application state
was already applied. The two-minute capacity boundary is before commit, and
unexpected errors after commit explicitly report that fact.

Unconditionally killing a long-running serving generation for deployment was
rejected. The explicit force option can reclaim an unpromoted routine candidate
or a superseded draining old generation, but it cannot kill the only current
generation before its candidate is ready.

Random age offsets were not added. Routine rotations queue and coalesce, and
successful serialized rotations naturally separate later age deadlines.

## Rollout and rollback

1. Choose the maximum total Node RSS allowance, including one full surge
   generation, include it in finite-cgroup memory planning, and verify startup
   feasibility before replacement.
2. Set `LOCAL_NODE_EXECUTOR_TOTAL_RSS_BUDGET_BYTES` and replace the backend with
   one that advertises Node cutover capability version 1.
3. Distribute the matching CLI before using `--force-node-cutover`. Omitting the
   option remains compatible with older CLIs.
4. Deploy source declarations only after the backend understands the
   pool-bearing environment marker.
5. Verify committed-pool gauges, per-pool lifecycle health, surge occupancy and
   queue metrics, aggregate RSS, and startup memory headroom.

To remove a pool, first deploy the module without its pool directive. Successful
commit reconciliation closes and drains removed generations and hot-replaces
other affected slots. Use `--force-node-cutover` only after checking the
interrupted-action warning and reading authoritative external state. To roll
back to a backend without this protocol, stop using the force option, remove
every pool declaration with the compatible backend and CLI, and complete that
deploy before replacing the backend. Remove the force-capable CLI afterward if
required; older backends intentionally reject the pool-bearing environment
value.
