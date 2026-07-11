# Repair Snapshot Import Checkpoints

This patch adds a privileged repair path for a failed replace-all ZIP snapshot
import that already wrote complete checkpoint tables. It is for the case where
the import finished writing hidden replacement tablets, recorded complete
checkpoints, then failed before or during finalization. In that state an
operator can recover from the checkpoint tablets instead of rerunning the entire
import.

The repair assumes the original import input was read correctly enough to
produce valid hidden checkpoint tablets. Provider-specific ZIP materialization
belongs to the neighboring materialization patch. This patch starts from the
checkpoint metadata and the hidden tablets already present in Convex.

## Failure Shape

A normal replace-all snapshot import writes incoming tables into hidden tablets.
Finalization then deletes active tables that are no longer present, activates
the hidden tablets selected by the import, writes the audit log entry, and marks
the import completed.

If the write phase succeeds and finalization fails, the deployment can be left
with a failed import record and complete hidden checkpoint tablets. In the
intended repair case, the active tables are still the pre-finalization tables,
possibly with rows written by later operator actions. Re-importing the whole
snapshot may be slow or fragile when the import is large. This repair path lets
an operator validate the failed import checkpoints and then run the same
finalization machinery against those hidden tablets.

That operation is destructive. It can activate checkpoint tables and delete or
replace active tables. A stale repair plan can discard active rows written after
the failed import was checkpointed. The patch therefore treats repair as a
fail-closed operator action, not as an application API.

## Operator Flow

The local backend exposes `POST /api/repair_failed_import_from_checkpoints`. The
caller must have the deployment import-backups operation permission. The request
takes an import id and an optional `execute` flag. `execute` defaults to
`false`, so the route is a dry run unless the caller explicitly asks it to
finalize.

The dry run builds the repair plan and returns a report with the import id,
import mode, total checkpoint count, selected table count, skipped empty
component `_storage` checkpoint count, selected row count, selected table
details, skipped checkpoint details, and no `documentsDeleted` value. The
execute call builds the plan again, finalizes the validated hidden tablets, and
returns the same report shape with `documentsDeleted` set.

Run dry run immediately before execute. A dry-run response is not a durable
approval token. If application writes, another import attempt, a manual table
change, or another repair attempt happens after the dry run, rerun dry run
before executing. The execute path rechecks the important state inside the
finalization transaction, but the safe operating model is still a paused
write-side deployment.

Repair refuses to plan or execute while any other snapshot import is uploaded,
waiting for confirmation, or in progress. Cancel that import or wait for it to
reach a terminal state first. Execute repeats this full import-state check in
the activation transaction. Starting or confirming another import concurrently
conflicts with that transaction; its retry observes the competing import and
rejects the repair instead of finalizing both operations concurrently.

## Finalization Guards

The patch makes the finalization state guard explicit.

- Normal in-progress imports use an in-progress guard. Finalization only
  proceeds while the import state is still `InProgress`, and activation, audit
  logging, and the transition to `Completed` commit atomically. This closes the
  prior window where a concurrent dashboard cancellation could mark an already
  activated import as failed before the separate completion transaction.
- Failed checkpoint repair uses a failed-repair guard. Finalization only
  proceeds while the full import record still matches the failed record used to
  build the plan, and it revalidates the component namespace mappings,
  absence of other nonterminal imports, active-table guards, and hidden
  checkpoint tablet metadata and row counts in the same transaction that
  activates tables.
- Clear-tables uses no import guard because it is implemented as a direct
  table-clearing operation rather than a snapshot import record.

This keeps the existing normal import contract intact while giving the repair
path its own terminal-state contract. A failed repair finalization cannot
silently complete an import that was already completed, restarted, or moved to
another state. Checkpoint activation, the audit log entry, and the transition
from `Failed` to `Completed` commit atomically.

The repair audit entry uses the repair caller's authenticated token and member,
when present, plus the request IP address and user agent. It does not attribute
the destructive repair action to the member who originally uploaded the failed
import. The snapshot import record itself retains its original `memberId`.

## Checkpoint Validation

Repair is limited to failed ZIP imports in `ReplaceAll` mode. The import must
have a non-empty checkpoint list, and every selected checkpoint must be
complete: `numRowsWritten` must equal `totalNumRowsToWrite`, and row-count
fields must be non-negative.

For each checkpoint, repair validates the component path and resolves it to the
current component namespace. The checkpoint display table name is mapped to the
physical table name. The virtual `_storage` table maps to the physical
`_file_storage` table. Raw system table names are rejected; `_storage` is the
only supported system-looking checkpoint name because it is the export format's
virtual storage table.

For selected checkpoints, the tablet id must be present. The tablet metadata
must still be hidden, must be in the resolved namespace, and must have the
expected physical table name. The hidden tablet row count must match the
checkpoint total. The selected table numbers must be unique within each
namespace and must not conflict with a retained active system table. Duplicate
checkpoints for the same namespace and physical table are rejected.

Selected hidden tablets must also have fully enabled indexes matching the
enabled index descriptors and specifications on the current active table. An
active backfill rejects repair, matching normal replacement behavior. If there
is no current active table, the hidden tablet may have only the built-in by-id
and by-creation-time indexes that a fresh replacement would create. These checks
prevent repair from restoring an index that was dropped or losing one that was
added after the checkpoint tablet was prepared.

Execute repeats the component namespace, hidden tablet metadata, and hidden row
count checks inside the activation transaction. It also repeats the active and
hidden index comparison there. A component path that resolves to a different
namespace, or a checkpoint tablet whose rows or indexes changed after plan
construction, aborts the transaction.

The repair also refuses plans where the current active user-table set is not
represented by the failed import checkpoints. It does not infer a deletion from
a zero-row user-table checkpoint with no tablet id. That record is ambiguous: it
can describe either an active table omitted from the source or an empty source
table whose hidden-table preparation never completed. Guessing would let repair
silently delete a table in the second case. Imports with such deletion-only
checkpoints need a clean re-import or a separate repair plan with an explicit
deletion contract.

Repair also refuses active-schema tables missing from the checkpoints. Normal
replace-all creates an empty replacement for such a table. Confirmation now
records checkpoint entries for active-schema tables even when they are absent
from both the source and the current physical table mapping, so subsequent
imports retain the hidden tablet id needed for repair. Failed import records
created before that change can still lack the checkpoint and remain
unrepairable from checkpoint metadata alone.

After building the table mapping, dry run and execute both scan every selected
hidden user-table document and validate it against the current active, pending,
and validated schemas using the final replacement table mapping. This is
required because schema validation workers intentionally do not validate hidden
tablets. Without this scan, a schema change that made normal finalization fail
could be accepted as the repair's new baseline and invalid checkpoint rows could
then be activated.

Dry run scans at the repair plan's repeatable timestamp. Execute scans inside
the activation transaction at that transaction's repeatable timestamp. The
activation transaction records a dependency on each selected tablet's full
by-id index while it verifies row counts. A hidden-row mutation after the
transaction begins therefore causes an OCC retry, which repeats the metadata,
row-count, index, schema-document, activation, audit, and completion work from a
new transaction snapshot. This closes the same-row-count hidden-table mutation
window between document validation and activation.

## Active Table Drift

During plan construction, repair records an active-table guard for every
checkpointed table. The guard records the component path, display name, physical
table name, namespace, current active tablet id if one exists, the checkpoint's
`existingRowsInTable`, and the checkpoint's `existingRowsToDelete`.

For replace-all repair, `existingRowsInTable` must equal `existingRowsToDelete`.
The dry run checks that the current active table has the same row count recorded
by the checkpoint. During execute, finalization rechecks the same active tablet
id and row count inside the activation transaction, and it rejects active user
tables that are missing from the repair guards.

This catches changes in active-table identity or row count that happen after a
repair plan is built. Checkpoints do not record the original active tablet id or
active row contents, so a same-count replacement, creation, deletion, or content
change that happens after the import checkpoint but before the activation
transaction begins cannot always be detected. Once that transaction begins,
the row-count checks depend on each active table's full by-id index, so a later
write causes an OCC retry. Keep application writes stopped from the failed
import through repair; this patch does not reconcile live multi-writer changes.

## Storage Handling

Snapshot export exposes file storage as a virtual `_storage` table, while Convex
stores the rows in the physical `_file_storage` table. The repair path and the
normal checkpoint resume path both keep that mapping explicit.

Root `_storage` is not silently ignored. A root `_storage` checkpoint is treated
as a selected `_file_storage` checkpoint and must pass the hidden-table,
namespace, row-count, duplicate, and active-table checks.

Empty component `_storage` checkpoints can be skipped only under strict
conditions:

- the checkpoint is for a non-root component;
- the component path still exists;
- the display table name is exactly `_storage`;
- `numRowsWritten`, `totalNumRowsToWrite`, `existingRowsInTable`, and
  `existingRowsToDelete` are all zero;
- any referenced checkpoint tablet is hidden, belongs to the component
  namespace, is named `_file_storage`, and has zero rows;
- the skipped checkpoint still participates in duplicate table-name detection
  and active-table drift guarding.

The storage-table changes also make `_storage` import and checkpoint resume fail
closed:

- negative declared file sizes are rejected instead of being converted to large
  unsigned content lengths;
- duplicate `_storage` metadata ids are rejected;
- duplicate `_storage` blob entries are rejected;
- a blob entry without a matching metadata row is rejected;
- a metadata row without a matching blob entry is rejected;
- leftover `_storage` blob entries with no matching `_storage/documents.jsonl`
  table are rejected;
- checkpoint skip counts larger than the source metadata or document count are
  rejected.

The importer compares the complete metadata-id and blob-id sets before opening
any blob stream. A duplicate, missing, or unexpected blob therefore fails before
the import uploads files or writes metadata rows for that `_storage` table.

When checkpoint resume skips already-imported storage rows, it skips before
opening the blob stream. Re-uploading a skipped blob would create an
unreferenced file object because the metadata row already exists in the hidden
checkpoint tablet.

Repair validates `_file_storage` tablet metadata, row counts, and indexes, but
it does not reopen every referenced blob or recompute its checksum. A complete
checkpoint row normally exists only after its blob upload succeeded. If the
storage provider may have lost or corrupted objects after that point, use a
clean restore or a separate blob-integrity check instead of checkpoint repair.

## Row Counts

The repair report's `selectedRowCount` counts rows in selected hidden checkpoint
tablets, including `_file_storage` rows. That is useful for operator inspection.

The completed import state's `numRowsWritten` follows normal snapshot import
semantics. Storage rows are excluded from the completed row count even though
storage progress and repair reports expose storage counts. This keeps repaired
imports aligned with imports that completed normally.

The activation transaction marks the import completed only through a
failed-import completion path. That path requires the import to still be
`Failed`; `Completed` is rejected rather than treated as an idempotent success.
If completion fails, table activation and the audit log entry roll back with it.
For both normal and repaired imports, the completed state's timestamp is the
activation transaction's begin timestamp. The commit timestamp is not available
while writing the completion state atomically, so the displayed completion time
can precede the actual commit by the duration of the activation transaction.

## Scope

Use this patch for operator repair after a failed replace-all ZIP import whose
selected checkpoints have hidden tablets and still match the active-table state
recorded by the import. It is not a live reconciliation mechanism, not a general
recovery path for partial or ambiguous deletion-only checkpoints, and not a
substitute for a clean re-import when checkpoint metadata, hidden tablets,
`_storage` metadata, or active-table guards do not validate.

If validation fails, the deployment needs a different recovery path: a clean
re-import from valid input, a reset and import, or a targeted data repair with a
separate plan. This endpoint should not grow skip-and-continue behavior for
invalid checkpoints.

## Verification State

The follow-up review ran `rustfmt --check` on the Rust sources changed by this
patch and `git diff --check` from the parent of the original patch commit across
the changed files successfully.

`cargo check -p application -p model -p local_backend` could not compile project
code in this worktree because the `aws-lc-sys` build requires `cmake`, which is
not installed. No backend image was built and no import or live infrastructure
operation was run.
