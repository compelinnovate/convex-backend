# Non-Committing Analysis for Code Generation

This patch lets `convex codegen` obtain authoritative server analysis without
starting a deployment or publishing pending schema and index metadata.

It adds an optional analysis result to the existing `evaluate_push` deployment
preflight endpoint. The matching `convex-js` patch uses that endpoint for
standalone code generation. Normal development and deployment flows continue
to use `start_push`, wait for schema and index preparation, and finish the push.

## Background

Generated API, data-model, component, and environment-variable types depend on
the backend's evaluated module and component definitions. The deployment
protocol already returned that analysis from `start_push`, so standalone
codegen historically began the same multiphase push used by `dev` and
`deploy`, even though codegen never called `finish_push`.

For an ordinary application, `start_push` commits pending schema and index
metadata before returning. Schema validation and index backfill workers can
then observe that metadata and begin work. A later real deployment can report
that codegen-created metadata as removed or replaced. The generated files do
not require any of those durable changes.

`evaluate_push` already computes deployment schema and index differences
without committing its transaction. Before this patch, however, its response
contained only those differences, not the evaluated component analysis needed
by codegen.

## Protocol

`StartPushRequest` has an additive `includeAnalysis` field. When it is true on
an `evaluate_push` request, the response includes the evaluated component map
under `analysis`. When it is absent or false, `evaluate_push` retains its small
schema-difference response. `start_push` continues to return analysis
regardless of this field.

The backend derives both responses from the same evaluated component map. It
also applies the same file-based component exports before returning either
analysis result. Generated code therefore receives the same module, schema,
function, export, and environment-variable information from `evaluate_push`
that it previously received from `start_push`.

Deployment and large-index preflight requests omit `includeAnalysis`. They do
not pay the serialization and response-size cost of analysis they do not use.

## Non-committing schema and index preparation

`evaluate_push` calls the canonical component schema and index preparation
logic in a database transaction. That logic can stage schema, index, table, and
component-namespace writes while calculating the exact deployment difference.
The endpoint drops the transaction without committing it.

Schema validation and index backfill workers can observe only committed
metadata. An `evaluate_push` request therefore does not:

- publish a pending schema or index;
- start schema validation or an index backfill;
- load a proposed index into the worker-visible in-memory index set; or
- replace the currently deployed functions.

The endpoint is not globally free of side effects. Existing analysis paths can
upload source packages, populate external-dependency and runtime caches, and
enable Node action infrastructure. Those operations support analysis and do
not publish deployment schema or index state.

## CLI behavior and compatibility

The matching CLI sends standalone `convex codegen` to `evaluate_push` with
`includeAnalysis: true`. It never falls back to `start_push`. If an older
backend returns the legacy response without `analysis`, the CLI reports an
explicit backend-version error and stops.

The response decoder continues to accept a legacy `evaluate_push` response so
the CLI can produce that specific error. The request and response additions
are otherwise optional:

- an older CLI can use the patched backend, but its standalone codegen retains
  the older `start_push` behavior;
- an older backend continues to serve deployment requests from the patched
  CLI because those requests omit `includeAnalysis`; and
- `dev` and `deploy` retain the existing
  `start_push -> wait_for_schema -> finish_push` lifecycle.

Upgrade the backend before distributing the matching CLI. Roll back the CLI
before the backend if standalone codegen must remain available throughout the
rollback.

## Standalone component environment bindings

`convex codegen --component-dir` evaluates a target component beneath a
synthetic root. That root cannot provide concrete values for the target's
required environment variables. The backend allows missing bindings only for
the single component directly mounted beneath that synthetic root.

Components instantiated by the target remain subject to normal required
environment-variable validation. A symbolic binding from a nested component
to the target's environment remains structurally valid even though the
synthetic root cannot provide the final value. A malformed synthetic root with
zero or multiple target children is rejected instead of widening the binding
exception ambiguously.

Ordinary codegen and every deployment path retain full environment-binding
validation.

## Considered alternatives

### Continue using `start_push`

Codegen would keep publishing deployment metadata and starting background
work. Avoiding `finish_push` does not undo the committed preparation phase.

### Fall back to `start_push` for an older backend

An automatic fallback would silently restore the mutation this patch is meant
to prevent. A version error is safer and makes the required rollout order
explicit.

### Return analysis from every preflight request

Large-index preflight only needs the schema difference. Always returning the
component map would add unnecessary serialization, transfer, and client memory
cost.

### Reimplement schema and index comparison for codegen

A second comparison path could drift from deployment semantics. Running the
canonical preparation logic in a dropped transaction preserves one source of
truth without making its staged writes durable.

### Disable environment validation recursively for component codegen

Only the synthetic root-to-target binding is unavailable. Disabling validation
below that boundary would accept genuinely missing bindings in nested
components and could generate types for a component graph that cannot be
deployed.

## Operator rollout

This patch has no database migration or configuration setting.

1. Deploy the patched backend.
2. Distribute a CLI containing the matching non-committing codegen patch.
3. Run codegen and confirm that it uses `evaluate_push` and returns generated
   files without publishing pending schema or index state.

The patch can be adopted independently of scheduler, schema-validation OCC,
Node executor, and memory-management patches.
