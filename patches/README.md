# Maintained Self-Hosted Patch Set

These documents describe locally maintained, generic patches on top of `convex-backend` upstream.
They are operator adoption units, not one all-or-nothing fork. Read the owning essay before carrying
a patch, preserve its prerequisites, and verify the effective configuration and metrics after every
backend replacement.

The maintained backend source chain normally keeps each product patch in its own operator-adoption
commit; repository-maintenance commits can remain separate. Patch-history is also used to preserve
fine-grained bisectability when one adopter-facing feature is developed from several historical
seams. When lifecycle ownership spans patches, an explicitly ordered corrective integration commit
may complete several earlier adoption commits; the owning essays identify those compositions.
Lane-aware queueing and its optional deployment
extension are one queue-control patch. The matching degradable-query client half is maintained in
`convex-js`; it shares the protocol and adoption essay but is not another commit in this backend
chain. Non-committing codegen analysis likewise has a matching CLI commit, with the combined
adoption contract documented in the backend essay.

## Database connection reliability

### [Cancellation-safe MySQL connections](cancellation_safe_mysql_connections/README.md)

- Purpose: discard interrupted pooled MySQL connections, with optional server-side cancellation
  through dedicated control capacity on an operator-asserted trusted topology.
- Prerequisites: MySQL persistence; no runtime scheduler or function-execution patch.
- Activation: cancel-safe ownership and client force-disconnect of canceled or incomplete
  connections are automatic for direct MySQL operations and lease-owned transactions. In the
  default untrusted-topology mode, cancellation force-closes the client connection and never sends
  numeric `KILL CONNECTION`. Server-side cancellation requires the strict
  `MYSQL_SERVER_SIDE_CANCELLATION_TRUSTED_SINGLE_NAMESPACE=true` operator assertion described in
  the patch essay.
- Rollback: restore the upstream connection wrapper only after confirming that canceled operations
  cannot leave pending statements or reusable unread responses.

## Snapshot and import reliability

### [Materialize snapshot import ZIPs](snapshot_import_zip_materialization/README.md)

- Purpose: download a remote ZIP once, verify its length, and parse entries from a retained local
  file instead of provider-backed range streams.
- Prerequisites: enough local temporary disk for the archive; none of the runtime scheduler patches.
- Activation: automatic for ZIP snapshot imports after applying the patch.
- Rollback: restore the upstream streaming importer only after confirming the object store path is
  reliable for seek-heavy ZIP parsing.

### [Repair failed snapshot import checkpoints](snapshot_import_checkpoint_repair/README.md)

- Purpose: dry-run and explicitly finalize a failed replace-all import whose checkpoint tablets are
  complete.
- Prerequisites: a qualifying failed import and privileged operator permission. ZIP materialization
  is complementary but not required once valid checkpoints exist.
- Activation: only through the privileged repair endpoint; dry-run is the default.
- Rollback: do not execute a stale plan. There is no generic undo after destructive finalization.

## Deployment and code generation

### [Non-committing codegen analysis](non_committing_codegen_analysis/README.md)

- Purpose: let standalone codegen obtain authoritative evaluated component analysis without
  committing pending schema or index metadata or starting validation and backfill workers.
- Prerequisites: the matching `convex-js` CLI patch. Upgrade the backend before distributing that
  CLI; other backend patches are not required.
- Activation: automatic when the matching CLI sends `includeAnalysis` to `evaluate_push`.
- Rollback: roll back the CLI before the backend if codegen must remain available; no data or
  configuration rollback is required.

### [Paced isolate module analysis](deployment_analysis_pacing/README.md)

- Purpose: make each isolate module analysis attempt fairly borrow from the configured degradable
  query capacity instead of adding deployment fan-out above that elastic root-work ceiling.
- Prerequisites: degradable reactive-query admission supplies the shared application-scoped gate.
  The control-plane lane is recommended for typed queue treatment but is not required for pacing.
- Activation: automatic when `APPLICATION_MAX_CONCURRENT_DEGRADABLE_QUERY_LEADERS` is configured;
  no analysis or query behavior changes while it is unset.
- Rollback: restore the previous backend image. Unsetting the cap also disables pacing, but it
  simultaneously disables degradable-query backpressure.

## Schema reliability

### [Isolate schema-validation progress OCC](schema_validation_progress_occ/README.md)

- Purpose: prevent progress checkpoints from repeatedly aborting app writes that fail a pending
  schema while preserving schema-state fencing, bounded restart/history cleanup, and dashboard
  progress.
- Prerequisites: deploy the matching dashboard zero-total guard before the backend, or upgrade the
  backend and dashboard together.
- Activation: automatic during schema validation after the coordinated backend/dashboard rollout.
- Rollback: restore the backend before the dashboard; no data or configuration rollback is
  required, but the previous backend restores the original contention risk.

## Log privacy

### [Redact validator values from external log sinks](validation_error_log_redaction/README.md)

- Purpose: retain validator-error classification without sending rejected arguments, return
  values, or validator details to durable external log sinks.
- Prerequisites: none.
- Activation: automatic after deploying the patched backend image; historical events are unchanged.
- Rollback: an upstream image without equivalent redaction restores the sensitive-value leak.

## Backend memory resilience

### [Backend memory resilience](backend_memory_resilience/README.md)

- Purpose: account for configured and observed backend memory, reclaim optional allocator and local
  Node state before external HTTP shedding, reserve one full local Node hot-replacement surge
  allowance, select bounded jemalloc in the standard backend build, export a shared pressure signal
  for owner-specific patches, and preserve finite cgroup limits as the hard boundary. Healthy age,
  package-count, and ordinary RSS changes use the surge process; actual cgroup pressure cancels a
  candidate and terminates draining old generations before further shedding.
- Prerequisites: local Node executor resilience for pressure-triggered generation retirement and
  shared-base HTTP admission for dependency-preserving external shedding. Pressure control also
  requires Linux cgroup v2 with a finite readable memory limit; explicit allocator trim requires a
  GNU libc control build. Arena counting is available for GNU libc and jemalloc builds.
- Activation: jemalloc is the default `local_backend` feature; a GNU libc control build uses
  `--no-default-features`. All pressure switches default to disabled. Internal reclamation,
  allocator trim, and external shedding have separate enable switches and ordered headroom
  thresholds documented in the patch essay. The shedding-entry value also bounds trim deferral
  while reclamation is enabled.
- Rollback: restore the previous backend image and remove settings it does not understand; no schema
  or data change is required.

## Build and runtime packaging

### [Backend build improvements](backend_build_improvements/README.md)

- Purpose: centralize compiler and code-generation prerequisites; honor the selected Cargo profile,
  debuginfo, and strip behavior; use shallow locked Cargo Git dependencies and shared build caches;
  and avoid a redundant eager JavaScript install and unrelated browser download.
- Prerequisites: the pinned Rust, protoc, pnpm, and Turbo tools. Image builds additionally require
  the BuildKit cache-mount support already used by the backend Dockerfile.
- Activation: local Cargo commands use `scripts/run_cargo.sh`. Default image builds remain release
  builds; dependency caching and source layering are automatic, while custom artifact behavior
  requires build args or Cargo profile settings.
- Rollback: return to the normal release profile and default strip behavior, or restore the previous
  dependency-fetch and JavaScript-install layers. Runtime behavior and data are unchanged.

### [Atomic Node executor source packages](atomic_node_executor_source_packages/README.md)

- Purpose: publish source and external packages atomically, bound their retained filesystem and
  stack-root lifetime without deleting active trees, expose preparation-only acquisition for
  candidate readiness, and keep concurrent external-dependency builds private, output-size- and
  time-bounded, and responsive to the local event-loop watchdog. On Unix, an npm supervisor also
  attempts to stop its process group if the Node executor generation exits.
- Prerequisites: none.
- Activation: automatic in the local Node executor.
- Rollback: restore upstream only if atomic publication, active package ownership, bounded
  retirement, direct stack-root lookup, and watchdog-safe dependency building are all replaced.

### [Local Node executor resilience](local_node_executor_resilience/README.md)

- Purpose: retire a selected local Node generation on request/stream timeout, transport failure,
  a process-declared exit, repeated event-loop health failure, or backend shutdown; hot-replace a
  healthy generation after RSS, imported-package, age, source, or environment changes; prepare
  source and external packages in the candidate without invoking application code; bound startup
  probes and local response streaming; prevent child stdio from bypassing function-log handling;
  terminate and reap only owned direct children; and expose bounded lifecycle and health metrics.
  Candidate promotion and old admission closure are atomic, while already assigned old requests
  drain within their existing action deadlines. Backend memory resilience extends the same
  mechanism with retire-first cgroup-pressure handling. This patch also moves the local runtime to
  Node.js 24 and captures bounded
  active-request, process, diagnostic-report, and main-thread CPU-profile evidence on the first
  watchdog miss without delaying replacement. Published diagnostic artifacts are private,
  retained local files rather than logs. Detached descendant process groups, including
  `build_deps` npm installs, require separate ownership. The atomic-package patch adds best-effort
  npm process-group containment, but Rust does not wait for descendant exit before removing a
  generation tempdir.
- Prerequisites: none for generation recovery, RSS/package/age hot replacement, or diagnostics. The
  atomic-package patch adds package and stack aggregate metrics to the same health protocol; backend
  memory resilience adds cgroup-pressure retirement and enforced surge-capacity planning.
- Activation: automatic in the local Node executor. Set
  `LOCAL_NODE_EXECUTOR_DIAGNOSTICS_DIR` to an absolute mounted path when first-miss artifacts must
  survive container replacement.
- Rollback: restore the previous backend image if healthy generations are retired unexpectedly.

## Optional runtime routing

### [Application-declared pinned local Node executor pools](pinned_local_node_executor_pools/README.md)

- Status: implemented; named pools require both a source declaration and sufficient host budget.
- Purpose: let a root Node action module require a bounded named one-process local executor pool.
  A selected pool preserves rebuildable module-level state across ordinary action invocations and
  hot-replaces its complete process generation when source, environment, or committed membership
  changes. One global surge coordinator serializes temporary overlap across default and named
  pools, coalesces routine rotations, and gives deployments bounded priority. Required pool
  capability survives wire, durable-record, and source-archive round trips; stale requests cannot
  replace a newer resident generation. Other actions continue through the default executor.
- Prerequisites: local Node executor resilience supplies generation ownership and drain;
  atomic Node source packages supply package identity and ownership; backend memory resilience must
  reserve the configured total Node RSS budget when that patch is present.
- Activation: first set `LOCAL_NODE_EXECUTOR_TOTAL_RSS_BUDGET_BYTES` to cover the default slot,
  every named slot, and one full surge allowance, and verify that the complete configured memory
  budget fits the finite cgroup. Replace the backend with one advertising cutover capability
  version 1 before using `--force-node-cutover` from the matching CLI; ordinary requests that omit
  the option remain compatible with older CLIs. Then add `"use node"` and
  `"use node pool:<name>"` to selected root modules. The backend validates the complete proposed
  topology before commit and again after the client round trip. A deployment waits at most two
  minutes for cutover capacity before commit; the force option can reclaim an unpromoted routine
  candidate or a superseded draining old generation after an explicit interruption warning.
- Rollback: stop using the force option, deploy source without pool declarations through the
  compatible backend and CLI, then restore an image that does not understand the required
  pool-bearing module environment. Roll back the CLI afterward if required.

### [Local Node executor pool admission policy](local_node_executor_pool_admission/README.md)

- Purpose: add optional independent-action concurrency, queue-delay observability, and
  main-event-loop unresponsiveness budgets to the default and application-declared local Node
  pools while preserving the application-wide Node action cap.
- Prerequisites: application-declared pinned local Node executor pools for routing and generation
  ownership, and scheduled-action admission for the pre-claim durable start boundary.
- Activation: set the strict `LOCAL_NODE_EXECUTOR_POOL_POLICIES` JSON map for only the pools that
  need an additional bound, restart the backend, and verify per-pool admission and health metrics.
  Missing pools and fields preserve existing behavior.
- Rollback: remove the setting and restart before restoring an earlier image. No schema, topology,
  or durable-job migration is required.

## Scheduler, admission, and queueing

### [Dependency capacity](dependency_capacity/README.md)

- Purpose: propagate ancestor-unblocking ownership and allow only dependencies to use bounded
  application, queue, and worker overflow; cap independent action shells.
- Prerequisites: none within this patch set.
- Activation: carrying the patch enables its finite model. Operators must choose coherent worker,
  reserve, action, queue, and active-thread settings.
- Rollback: restore the prior image and capacity settings together; removing it restores the
  action/descendant capacity inversion.

### [Shared-base HTTP admission](shared_base_http_admission/README.md)

- Purpose: make both local HTTP gates configurable and preserve bounded main-service headroom for
  authenticated Node callbacks.
- Prerequisites: none, although Node chains normally also need dependency capacity downstream.
- Activation: carrying the patch replaces the old local fixed gates; explicit total and reserve
  values are recommended because the unset total uses the common backend default.
- Rollback: lower external proxy concurrency before restoring a smaller backend gate.

### [Isolate queue delay control and deployment lane](isolate_queue_control/README.md)

- Purpose: add bounded per-lane delay control, dependency-safe shedding, finite hard expiry, and an
  optional typed analysis/evaluation lane.
- Prerequisites: dependency-capacity scheduling and its propagated request properties.
- Activation: lane-aware queueing is disabled by default. The deployment lane is a second opt-in and
  refuses to start unless lane-aware queueing is enabled with coherent caps and deadlines.
- Rollback: disable the deployment lane first, then lane-aware queueing if necessary; both require a
  backend restart and leave the dependency-capacity patch intact.

### [Scheduled action admission before durable claim](scheduled_action_admission/README.md)

- Purpose: admit scheduled and cron actions to environment-specific execution capacity before
  committing their monotonic at-most-once `Pending -> InProgress` claim.
- Prerequisites: the maintained dependency-capacity and isolate queue-control commits; lane-aware
  queueing may remain disabled.
- Activation: automatic for scheduled and cron actions after the patched backend starts; no new
  knob or data migration is required.
- Rollback: restore the prior backend image. Existing `InProgress` jobs retain conservative
  at-most-once recovery, and must not be moved back to `Pending`.

### [Runtime health dashboard semantics](runtime_health_dashboard/README.md)

- Purpose: report observed queueing without unsupported saturation claims and display
  scheduled-function lag with second-level resolution, correct ready-state sampling, and direct
  ordinary scheduler admission-lag telemetry.
- Prerequisites: none. The backend and dashboard halves are compatible with staggered rollout but
  give the clearest result together.
- Activation: automatic after deploying the corresponding backend and dashboard artifacts.
- Rollback: restore either artifact; an older backend can still extrapolate stale ready time, and an
  older dashboard retains minute rounding.

## Context reuse

### [Context reuse](context_reuse/README.md)

- Purpose: unify bounded, cancellation-safe V8 context reuse for queries, mutations, ordinary
  Convex-runtime actions, and HTTP actions under one per-module policy and one set of observability
  signals.
- Prerequisites: backend memory resilience and the current isolate scheduler/cache-capacity
  machinery; application source review for every module graph that opts in.
- Activation: automatic cache support and metrics after backend rollout. Applications opt into each
  execution kind with the typed `experimental_reuseContext` export; the legacy boolean remains
  query/mutation-only compatible. There is no startup-time HTTP reuse knob.
- Rollback: remove the corresponding policy property, redeploy the module, and restart backend
  workers to clear process-local cached contexts before restoring traffic.

## Degradable client behavior

### [Degradable reactive queries and client backpressure](degradable_reactive_queries/README.md)

- Purpose: let a cooperating sync connection opt root reactive queries down into a finite cache-miss
  leader cap and receive a typed pressure lifecycle for visible stale state and epoch-scoped retry.
- Prerequisites: matching `convex-backend` and `convex-js` wire support. Stale presentation and
  optional imperative-read suppression remain explicit frontend policy; successful reactive
  subscriptions stay mounted.
- Activation: backend admission is inert while
  `APPLICATION_MAX_CONCURRENT_DEGRADABLE_QUERY_LEADERS` is unset. Clients must explicitly send the
  degradable declaration; normal clients, mutations, and actions remain outside the degradable
  sub-cap, while dependencies retain their backend-derived treatment.
  Optional protected and degradable active-JavaScript minimums provide work-conserving execution
  progress when the leader cap exceeds active-JavaScript capacity.
- Rollback: remove the application opt-in first, then unset the backend cap. The protocol fields can
  remain deployed inertly.

## Detailed design references

These files preserve detailed implementation and design analysis without creating additional
operator adoption units:

- [Combined dependency and HTTP capacity design](dependency_capacity/design_reference.md)
  retains the benchmark tables, full stage model, metrics interpretation, and application coverage
  matrix that preceded the two concise adoption essays. Its phase-only active-permit discussion
  describes zero-floor compatibility mode; the class-aware policy is specified in the degradable
  active-JavaScript admission note below.
- [Isolate delay queue design](isolate_queue_control/isolate_delay_queue_design_reference.md)
  defines the queue-lane, scheduler-property, active-class, and active-permit-phase distinctions;
  the oldest-eligible selection and lifecycle mechanics; and interactions with the surrounding
  admission and execution patches.
- [Deployment control-plane lane design](isolate_queue_control/deployment_lane_design_reference.md) retains
  the complete classifier, FIFO and reserve proof, deferred worker-reservation design, rejected
  alternatives, and test matrix.
- [Degradable active-JavaScript admission](degradable_reactive_queries/active_javascript_admission.md)
  records the service-class propagation, work-conserving floor policy, scheduler exposure
  invariant, configuration rules, and deliberately excluded generalizations.
- [Context reuse design](context_reuse/design_reference.md) retains lifecycle, compatibility,
  cancellation, cache, metrics, scheduler, and cross-patch interaction details.

## Recommended rollout order

1. Apply standalone import, build, and Node-package reliability fixes as needed.
2. Deploy non-committing codegen analysis in the backend before distributing the matching CLI.
3. Deploy dependency capacity before lane-aware queueing or the deployment lane.
4. Deploy scheduled-action pre-claim admission after those scheduler patches; it protects both the
   legacy CoDel and lane-aware queue paths.
5. Add shared-base HTTP admission when Node callbacks need outer-service headroom; size it from its
   own wait and occupancy signals.
6. Deploy the unified context-reuse patch before enabling reviewed module policy properties.
7. Enable reviewed context-reuse properties in application-owned stages; consider prewarming only
   after cold-miss evidence.
8. Deliver matching backend and client protocol before enabling degradable frontend behavior.
9. Add deployment-analysis pacing after degradable admission; validate capacity transfer with a
   controlled multi-module push before changing analysis concurrency or queue deadlines.
10. Change one independent capacity or semantic opt-in at a time unless the documented policy
   explicitly requires a coupled rollout and rollback order.

Do not use module, function, route, client, deployment, or tenant names in generic backend logic or
metric labels. Application modules may opt into application-owned semantics; route policy belongs at
the reverse proxy when it is inherently deployment-specific.
