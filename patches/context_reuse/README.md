# Context Reuse

This patch makes V8 context reuse one per-module feature covering queries, mutations, ordinary
Convex-runtime actions, and HTTP actions. It keeps one bounded isolate-local cache for all three
execution kinds and reinstalls request-owned Rust state on every invocation.

The cache is an optimization, not a correctness boundary. Reuse preserves JavaScript module and
global state, so an application must opt in only for module graphs that are safe when state survives
between executions. The backend does not contain an application module allowlist.

## Application policy

An entry module can opt into each execution kind independently:

```js
export const experimental_reuseContext = {
  queries: true,
  mutations: true,
  actions: true,
  httpActions: true,
};
```

The old `experimental_reuseContext = true` form remains compatible and means
`{ queries: true, mutations: true }`. An omitted property means `false`. The four permissions are
module-wide and do not vary by exported function; the request's validated UDF type selects the
applicable permission. HTTP actions use the same analyzed module policy and no longer require a
startup-time backend permission knob.

Review the complete static and dynamic import graph before enabling a permission. Do not enable it
for code that retains arguments, identities, transactions, documents, request or response objects,
callbacks, errors, promises, streams, or other request-derived state in module globals or imported
package state. The same review applies to third-party packages and import-time initialization.

## What is bounded and safe

Each isolate retains at most one probationary plus the configured protected number of contexts. The
default is five protected residents (`5+1`).
`ISOLATE_CONTEXT_CACHE_PROTECTED_RESIDENTS_PER_ISOLATE` changes the protected segment; the shared
pool bound and optional `ISOLATE_CONTEXT_CACHE_MAX_RESIDENTS` setting are derived and validated by
the existing cache-capacity machinery. Database-UDF, ordinary-action, and HTTP-action contexts
have distinct cache kinds and cannot alias, but they compete for the same bounded resident budget.

Reuse is best effort. A miss creates a fresh context, and cache frequency, memory pressure, worker
capacity, isolate recreation, and concurrent use can all prevent retention. The scheduler advertises
only resident keys through a thread-safe mirror; V8 roots stay on their owning isolate thread.

Before taking a resident, the backend validates its initialization read set. Deploys, environment
changes, component resources, or other initialization changes therefore force a fresh context.
Every request gets fresh identity, transaction, callbacks, streams, task state, and timeout state.
Database UDFs and actions also carry caller-drop cancellation to the save boundary. A context is
published only after successful execution, a final microtask checkpoint, clean termination, no
pending request-owned work, and a valid read set. HTTP actions additionally require a successfully
streamed response, an open isolate response stream at finalization, and successful delivery of the
outcome to the function-runner response channel. Later forwarding from that channel to an outer HTTP
transport is not a cache-publication boundary.

## Adoption and rollback

Apply this patch after backend memory resilience and the current isolate scheduler/cache-capacity
machinery. The required metrics are emitted automatically; establish fresh-versus-reused,
module-evaluation, cache, isolate-memory, termination, and recreation baselines before enabling
application permissions.

Roll out the backend first, then enable one reviewed module or execution kind at a time. To roll
back semantic reuse, remove the corresponding property (or the legacy marker), redeploy the module,
and restart backend workers so process-local residents are destroyed. There is no backend-wide HTTP
reuse switch.

See [the design reference](design_reference.md) for lifecycle ordering, protocol compatibility,
metrics, scheduler affinity, cancellation, and interactions with dependency and degradable-query
admission.
