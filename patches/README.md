# Maintained Self-Hosted Patch Set

These documents describe locally maintained, generic patches on top of `convex-backend` upstream.
They are operator adoption units, not one all-or-nothing fork. Read the owning essay before carrying
a patch, preserve its prerequisites, and verify the effective configuration and metrics after every
backend replacement.

The current backend source chain has 14 commits and 14 operator essays, one commit per backend
adoption unit. Lane-aware queueing and its optional deployment extension are one queue-control
patch. The matching degradable-query client half is maintained in `convex-js`; it shares the
protocol and adoption essay but is not another commit in this backend chain.

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
  stack-root lifetime without deleting active trees, and keep concurrent external-dependency builds
  private, output-size- and time-bounded, and responsive to the local event-loop watchdog. On Unix,
  an npm supervisor also attempts to stop its process group if the Node executor generation exits.
- Prerequisites: none.
- Activation: automatic in the local Node executor.
- Rollback: restore upstream only if atomic publication, active package ownership, bounded
  retirement, direct stack-root lookup, and watchdog-safe dependency building are all replaced.

### [Local Node executor resilience](local_node_executor_resilience/README.md)

- Purpose: retire a selected local Node generation on request/stream timeout, transport failure,
  a process-declared exit, repeated event-loop health failure, or backend shutdown; bound startup
  probes and local response streaming; prevent child stdio from bypassing function-log handling;
  terminate and reap only that direct child; and expose bounded lifecycle and health metrics.
  Detached descendant process groups, including `build_deps` npm installs, require separate
  ownership. The atomic-package patch adds best-effort npm process-group containment, but Rust does
  not wait for descendant exit before removing a generation tempdir.
- Prerequisites: none for generation recovery; the atomic package patch adds package and stack
  aggregate metrics to the same health protocol.
- Activation: automatic in the local Node executor.
- Rollback: restore the previous backend image if healthy generations are retired unexpectedly.

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

### [Cancellation-safe database context reuse](cancellation_safe_database_context_reuse/README.md)

- Purpose: preserve upstream module-wide query and mutation reuse while preventing canceled or
  visibly terminated executions from publishing a context.
- Prerequisites: application source review for every marked entry module.
- Activation: application modules opt in with `experimental_reuseContext`; the backend never carries
  a deployment module allowlist.
- Rollback: remove the marker from every opted-in module, deploy those application changes, and
  restart backend workers to clear process-local cached contexts before restoring traffic.

### [Context reuse observability](context_reuse_observability/README.md)

- Purpose: expose bounded allow/suppress, lookup, validation, take/save/clear, affinity, occupancy,
  and isolate-memory signals.
- Prerequisites: none for metrics; it is most useful with one of the context-reuse patches.
- Activation: automatic after backend rollout. Absent series can be valid when no matching context
  activity occurred.
- Rollback: remove the patch; runtime context policy is otherwise unchanged.

### [HTTP action context reuse](reuse_http_action_contexts/README.md)

- Purpose: reuse V8 contexts for hot HTTP action modules while reinstalling per-request Rust state.
- Prerequisites: source-purity review of every reachable HTTP action module graph. Context reuse
  observability is strongly recommended.
- Activation: disabled by default through `REUSE_HTTP_ACTION_CONTEXTS=false`.
- Rollback: disable the knob and restart to clear worker-local contexts.

## Degradable client behavior

### [Degradable reactive queries and client backpressure](degradable_reactive_queries/README.md)

- Purpose: let a cooperating sync connection opt root reactive queries down into a finite cache-miss
  leader cap and receive a typed pressure lifecycle for visible stale state and epoch-scoped retry.
- Prerequisites: matching `convex-backend` and `convex-js` wire support. Stale presentation and
  optional imperative-read suppression remain explicit frontend policy; successful reactive
  subscriptions stay mounted.
- Activation: backend admission is inert while
  `APPLICATION_MAX_CONCURRENT_DEGRADABLE_QUERY_LEADERS` is unset. Clients must explicitly send the
  degradable declaration; normal clients, mutations, actions, and dependencies remain normal.
  Optional protected and degradable active-JavaScript minimums provide work-conserving execution
  progress when the leader cap exceeds active-JavaScript capacity.
- Rollback: remove the application opt-in first, then unset the backend cap. The protocol fields can
  remain deployed inertly.

The short HTTP-action note in this essay is future work, not part of the current implementation.

## Detailed design references

These files preserve the full earlier analysis without creating additional operator adoption units:

- [Combined dependency and HTTP capacity design](dependency_capacity/design_reference.md)
  retains the benchmark tables, full stage model, metrics interpretation, and application coverage
  matrix that preceded the two concise adoption essays.
- [Deployment control-plane lane design](isolate_queue_control/deployment_lane_design_reference.md) retains
  the complete classifier, FIFO and reserve proof, deferred worker-reservation design, rejected
  alternatives, and test matrix.
- [Degradable active-JavaScript admission](degradable_reactive_queries/active_javascript_admission.md)
  records the service-class propagation, work-conserving floor policy, scheduler exposure
  invariant, configuration rules, and deliberately excluded generalizations.
- [Database context cancellation design](cancellation_safe_database_context_reuse/cancellation_design_reference.md)
  retains the full signal ownership, memory ordering, nested-call, timing, and save-boundary analysis.

## Recommended rollout order

1. Apply standalone import, build, and Node-package reliability fixes as needed.
2. Deploy dependency capacity before lane-aware queueing or the deployment lane.
3. Add shared-base HTTP admission when Node callbacks need outer-service headroom; size it from its
   own wait and occupancy signals.
4. Establish observability before enabling application context-reuse markers or HTTP reuse.
5. Enable reviewed database-UDF context reuse in application-owned stages; consider prewarming only
   after cold-miss evidence.
6. Deliver matching backend and client protocol before enabling degradable frontend behavior.
7. Add deployment-analysis pacing after degradable admission; validate capacity transfer with a
   controlled multi-module push before changing analysis concurrency or queue deadlines.
8. Change one independent capacity or semantic opt-in at a time unless the documented policy
   explicitly requires a coupled rollout and rollback order.

Do not use module, function, route, client, deployment, or tenant names in generic backend logic or
metric labels. Application modules may opt into application-owned semantics; route policy belongs at
the reverse proxy when it is inherently deployment-specific.
