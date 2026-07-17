# Isolate Schema-Validation Progress from Application Writes

This patch prevents schema-validation progress checkpoints from repeatedly
aborting application mutations that discover an incompatibility with a schema
being deployed.

The fix preserves optimistic concurrency control (OCC). It does not exempt
private system tables from conflict detection and does not add another retry
loop. Instead, it makes the schema document, rather than the frequently updated
progress document, the ordering point between schema validation and live
application writes.

## Background

### Schema deployment lifecycle

Convex stores schema metadata separately for each namespace: the root
application and each component have their own schema lifecycle. A newly
submitted schema normally enters the `Pending` state in the private `_schemas`
system table.

`SchemaWorker` is a background loop in `convex-backend`. It finds pending
schemas and determines which tables need a full document walk. A table can be
skipped when either the active schema or the stored inferred table shape already
proves that every document is compatible with the submitted schema. The worker
reads every document in the remaining tables from a stable snapshot.

The normal lifecycle is:

```text
Pending -> Validated -> Active
Pending or Validated -> Failed
Pending or Validated -> Overwritten
Active -> Overwritten
```

- `Validated` means that the document walk completed, but the deployment has not
  yet activated the schema.
- `Failed` means that an existing document or a live application write did not
  match the submitted schema.
- `Overwritten` means that a newer deployment replaced an in-progress schema, or
  that a previously active schema was replaced or cleared.

The relevant implementation starts in
[`SchemaWorker`](../../crates/application/src/schema_worker/mod.rs) and
[`SchemaModel`](../../crates/database/src/bootstrap_model/schema/mod.rs).

### Persisted validation progress

For each schema being checked, Convex stores a document in the private
`_schema_validation_progress` table. It contains:

- the ID of the schema being validated;
- `numDocsValidated`, the number of documents checked so far; and
- `totalDocs`, an approximate total derived from table counts at the document
  walk's original snapshot when those counts are available.

The progress document is for the whole schema, even when validation walks
several tables. It does not contain a scan cursor. The worker's document
iterator owns the scan position in memory.

The worker writes a progress checkpoint after approximately 5% of the documents,
capped at 500:

```text
min(max(ceil(totalDocs / 20), 1), 500)
```

When the total is unavailable, the threshold is 500. An exact zero uses the
nonzero minimum of 1; because it comes from the same snapshot as the walk, no
document is available to trigger a periodic checkpoint. A sufficiently large
validation replaces the same progress document after every 500 compatible
documents. The initial write, periodic replacements, and final flush use these
write-source names:

- `schema_validation_tracker_initialized`;
- `schema_validation_progress_updated`; and
- `schema_validation_progress_finished`.

Persisted progress has three responsibilities:

1. The dashboard can report approximate validation progress.
2. The existence of the row is a cancellation signal for a running worker.
3. The schema ID distinguishes one deployment generation from a later
   replacement.

The row must remain available through `Pending` and `Validated`. It is no longer
live after the schema becomes `Active`, `Failed`, or `Overwritten`.

### Live writes also check the in-progress schema

Every inserted, replaced, or patched document is checked against the active
schema. While a schema is `Pending` or `Validated`, Convex also checks the new
version of the document against that in-progress schema.

If the document violates the active schema, the application mutation is invalid
and fails. If it satisfies the active schema but violates the in-progress
schema, the application mutation remains valid. Convex allows the document write
to proceed and, in the same transaction, changes the in-progress schema to
`Failed`. This lets a live write report an incompatibility before the background
scan encounters a similar document.

This behavior is implemented by `SchemaModel::enforce`, which is called from the
transaction write path. The application document and the schema failure are
intentionally atomic.

## The OCC defect

Before this patch, `SchemaModel::mark_failed` also deleted the failed schema's
progress document. Deletion first queried
`_schema_validation_progress.by_schema_id` and then read the matching document.
An application transaction that disproved the in-progress schema therefore
acquired read dependencies on the progress index entry and progress document.

At the same time, `SchemaWorker` periodically replaced that document. The
following ordering could repeat across every application retry:

1. An application transaction begins.
2. It stages a document that is valid under the active schema but invalid under
   the pending schema.
3. It stages the pending schema's transition to `Failed` and reads the progress
   row in order to delete it.
4. Before the application transaction commits, `SchemaWorker` commits another
   progress checkpoint.
5. The application commit fails OCC because its progress read is stale.
6. The backend retries the application mutation, which reads the same hot
   progress row again.

If checkpoint commits are more frequent than the application mutation's commit
opportunities, the mutation can exhaust its bounded OCC retry budget. This is a
timing-dependent failure, but the underlying defect is the unnecessary
read/write overlap.

The OCC metadata names `schema_validation_progress_updated` because that is the
write which invalidated the application transaction's read set. System-table
conflicts can omit an application table name, which is expected here.

One progress commit can invalidate several concurrent mutations, and one
mutation attempt can overlap several progress commits. A count of OCC records
naming this write source is therefore not a count of checkpoints or validated
documents.

`schema_worker_mark_failed` describes a different race. That transaction changes
`_schemas`, which live writes read while enforcing active and in-progress
schemas. It is not the recurring hot progress-row conflict fixed here.

## Required behavior

The patch must preserve all of the following:

- A document accepted by the active schema can commit even when it disproves the
  in-progress schema.
- That document and the transition of the exact in-progress schema to `Failed`
  remain atomic.
- Progress can be initialized or updated only for the exact schema that is still
  `Pending`.
- A checkpoint cannot publish progress after failure, overwrite, activation, or
  schema-table recreation.
- Completed progress remains available while the schema is `Validated`, until
  activation removes it.
- A missing progress document continues to cancel a worker for a still-pending
  schema.
- Failed, overwritten, missing, and otherwise orphaned progress is eventually
  deleted, including after a backend restart.
- Dashboard progress disappears as soon as the schema is no longer pending,
  without waiting for physical cleanup.
- Components and recreated system tables cannot cause a stale worker or cleanup
  pass to act on a newer schema generation.

## Design

### Use schema state as the ordering point

Schema failure and pending-schema overwrite no longer read or delete the
transitioning schema's current progress document. They update the authoritative
schema lifecycle state without adding the hot progress row to their read set.

In the other direction, every progress initialization and checkpoint now reads
the exact schema document before it reads or writes progress. The operation
proceeds only if that exact schema is still `Pending`.

This produces a safe result regardless of commit order:

| First commit                                     | Result                                                                                                                      |
| ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| Progress checkpoint                              | The schema-failing application transaction has no progress dependency and can commit immediately afterward.                 |
| Schema failure or overwrite                      | The checkpoint's schema read is stale, so the checkpoint loses OCC and cannot publish progress for the terminal schema.     |
| Activation                                       | Progress writers have already stopped at `Validated`; activation removes completed progress synchronously.                  |
| Replacement schema or recreated `_schemas` table | The stale worker's exact schema identity no longer matches, so it stops without changing progress owned by the replacement. |

No special conflict exemption is needed. A stale progress transaction loses
ordinary OCC on its schema read.

The progress transaction is not retried locally. Retrying it would add another
contention loop and would be unnecessary after the schema has left `Pending`.

The central changes are in:

- [`SchemaModel::mark_failed`](../../crates/database/src/bootstrap_model/schema/mod.rs),
  which no longer touches the current hot progress row;
- [`SchemaValidationProgressModel`](../../crates/database/src/bootstrap_model/schema_validation_progress/mod.rs),
  which checks the exact schema state before initializing or updating progress;
  and
- [`SchemaValidationProgressTracker`](../../crates/application/src/schema_worker/mod.rs),
  which stops cleanly when the state fence rejects a stale worker.

### Clean terminal progress outside application transactions

Deferring deletion removes the conflict from application writes, but it creates
a cleanup responsibility. There are two cleanup paths:

- Activation removes progress synchronously. A schema reaches `Validated` only
  after live progress writers have stopped, so this path does not race periodic
  checkpoints.
- `Failed`, `Overwritten`, and orphaned progress is removed by `SchemaWorker`
  outside the application transaction that changed the schema state.

On each pass, the worker uses a read-only transaction to discover inactive
progress. It preserves rows owned by `Pending` and `Validated` schemas and
classifies rows owned by `Active`, `Failed`, `Overwritten`, missing, or inactive
schemas as cleanup candidates.

Discovery is separate from deletion. The worker then deletes concrete document
IDs in namespace-local batches of at most 16. Each write transaction rechecks
the current schema owner and the exact progress document before deleting it.
These point checks avoid broad progress-index or schema-history dependencies and
make cleanup safe when another worker, a component lifecycle change, or a new
deployment races the batch.

Cleanup is idempotent and has no single owner. Multiple backend instances may
discover the same row; normal OCC or an already-absent point read resolves that
race.

When no schema work exists, `SchemaWorker` subscribes to both schema changes and
progress changes. This matters during mixed-version rollout: if an old backend
writes progress for a terminal schema, the late progress write wakes a patched
worker even though `_schemas` did not change. Creating the first schema tables
for a new component also invalidates the worker's table-mapping dependency and
wakes it.

### Fence physical table generations

A developer document ID alone is not sufficient for stale-worker safety. Convex
can retain an old system-table tablet while a new physical `_schemas` or
`_schema_validation_progress` table reuses a table number and document internal
ID.

The patch therefore compares full resolved document IDs, including the physical
tablet generation. Schema lifecycle operations resolve through the currently
active `_schemas` table, and cached schema-state results are filtered to that
table. A stale worker or lifecycle request cannot act on a retained schema from
an inactive tablet or delete progress that has been reassigned to a new
generation.

Deleted component namespaces are handled separately:

- If the progress table is still active but the schema table is gone, its
  progress is orphaned and eligible for cleanup.
- If the component's system tables are already inactive, tablet retention owns
  their remaining documents and the worker skips them.

These cases are uncommon, but they are part of the same rule: progress belongs
to one exact schema generation, not merely to a reusable developer ID.

### Keep schema-history pruning bounded

Failure and overwrite transactions may also prune old terminal schemas. The
schema being changed by the current transaction is protected from pruning so
that history cleanup cannot pull its hot progress row back into the application
transaction.

A later lifecycle transaction may remove older terminal schemas and their exact
progress rows. Each call removes at most 16 old terminal schemas in one
namespace. This keeps application write sets bounded while ensuring that
deferred progress does not outlive all record of its owning schema.

## Related correctness fixes

The patch also closes several lifecycle and progress edge cases needed to make
deferred cleanup safe:

- Schema submission, bulk overwrite, enforcement, worker discovery, activation,
  snapshot import, and the schema system UDFs reject the invalid state where
  `Pending` and `Validated` schemas coexist.
- A worker reports `Schema is invalid` only after the exact schema has
  successfully reached `Failed`. If a replacement wins first, the old validation
  is canceled instead.
- A worker that loses the race after its final progress flush stops instead of
  reporting a false validation result.
- The checkpoint threshold never becomes zero when `totalDocs` is zero.
- Stored progress counts reject negative values and integer overflow.
- The system UDF preserves an exact `totalDocs = 0`; the dashboard avoids
  division by zero and displays progress only for the exact pending schema.

There is no schema or progress document-format migration. Existing valid rows
remain compatible.

## Why simpler mitigations are insufficient

- **Write progress less often:** reduces the probability of the race but
  preserves the same read/write overlap.
- **Increase mutation retries:** gives the same conflict more opportunities to
  repeat and does not correct the dependency.
- **Ignore system-table writes during OCC validation:** weakens serializability
  and can allow a stale worker to publish progress after a lifecycle transition.
- **Delete progress later in the same transaction:** statement order does not
  change a transaction's combined read set.
- **Retry progress commits locally:** adds another contention loop and obscures
  the fact that a terminal schema should stop the worker.
- **Remove persisted progress:** loses dashboard visibility and the existing
  cancellation signal.
- **Store progress on `_schemas`:** turns every checkpoint into a write to the
  schema document read by live enforcement and lifecycle operations.
- **Only defer deletion:** without the exact schema-state and generation fence,
  an old worker can recreate or update progress after failure or replacement.
  Without restart cleanup, deferred rows can remain indefinitely after a crash.

## Operator rollout

No database migration, configuration change, or traffic pause is required.

The adoption unit changes the backend-embedded system UDF and the shared
dashboard component in addition to the backend. The new system UDF preserves an
exact `totalDocs = 0`, while the previous dashboard can divide that value by
zero. Upgrade the dashboard first, or upgrade the dashboard and backend
together. The new dashboard remains compatible with the previous system UDF,
which exposed zero as `null`.

After replacing the backend, run both a compatible schema push and an
intentionally incompatible push in staging or a canary while representative
writes continue. Verify that:

- compatible validation reaches `Validated` and activation;
- an application write accepted by the active schema can fail the pending schema
  and still commit;
- application OCC retries are no longer attributed to
  `schema_validation_tracker_initialized`, `schema_validation_progress_updated`,
  or `schema_validation_progress_finished`;
- no progress checkpoint becomes visible after the schema reaches `Failed` or
  `Overwritten`;
- dashboard progress remains visible while the schema is pending and disappears
  when it leaves `Pending`; and
- a worker restart removes terminal or orphaned progress.

Do not use eventual success after an outer client retry as the only acceptance
criterion. The patch is intended to remove the inner progress conflict, not
merely to hide it behind another retry.

### Mixed-version rollout

The data format is compatible, but behavior is not uniform while patched and
unpatched backend instances overlap. An old instance can still make application
writes read and delete the hot progress row, and an old worker can write
progress without the new schema-state fence.

Patched cleanup repairs late mixed-version leftovers, but acceptance
measurements should begin only after old backend instances and their schema
workers have drained.

### Rollback

Roll back the backend before rolling back the dashboard. The new dashboard can
consume the old system-UDF response; the old dashboard is unsafe with the new
exact-zero response.

No data or configuration rollback is required. Returning to the previous backend
restores the original OCC risk. If possible, allow a patched worker to complete
a cleanup pass before the rollback; the previous worker has no general
inactive-progress cleanup pass.

## Verification

The primary regression test uses real database transactions and the real OCC
committer:

1. Commit a pending schema and initialize its progress row.
2. Begin an application transaction that inserts a document accepted by the
   active schema and rejected by the pending schema. Let it stage both the
   document and the schema failure without committing.
3. Begin and commit a later progress checkpoint for the same schema.
4. Commit the application transaction once, without an outer retry helper.
5. Verify that the application document exists and the schema is `Failed`.
6. Run the next checkpoint or cleanup pass and verify that stale progress is
   removed and the old worker stops.

Before the patch, step 4 fails OCC because the application transaction read the
progress row. After the patch, it succeeds because progress is absent from that
transaction's read set.

The reverse-order regression begins a checkpoint transaction, commits the schema
failure first, and then verifies that the checkpoint loses OCC on its schema
read. Additional tests cover:

- progress initialization after overwrite;
- preservation through `Validated` and deletion on activation;
- cleanup after restart and late mixed-version progress writes;
- component creation, deletion, and namespace isolation;
- schema and progress table recreation with reused developer IDs;
- bounded terminal-history pruning;
- zero and malformed progress counts;
- contradictory schema lifecycle states; and
- dashboard visibility for pending, validated, failed, replaced, and zero-total
  schemas.

The focused source files are:

- [`schema_worker/tests.rs`](../../crates/application/src/schema_worker/tests.rs);
- [`getSchemas.test.ts`](../../npm-packages/system-udfs/convex/_system/frontend/getSchemas.test.ts);
- [`getSchemas.ts`](../../npm-packages/system-udfs/convex/_system/frontend/getSchemas.ts);
  and
- [`ShowSchema.tsx`](../../npm-packages/dashboard-common/src/features/data/components/ShowSchema.tsx).

Run the focused checks and package build gates before publishing an image:

```sh
cargo test -p application schema_worker::tests

cd npm-packages/system-udfs
npm run test -- convex/_system/frontend/getSchemas.test.ts
cd ../..

just rush build -t system-udfs
just rush build -t dashboard-common
```

Also run the repository's normal Rust formatting, lint, and release-build gates
for the affected packages.
