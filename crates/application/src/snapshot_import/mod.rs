//! Bulk import of external data into Convex.
//!
//! The core import logic lives in
//! [SnapshotImportExecutor::attempt_perform_import]. The flow is roughly:
//! - [SnapshotImportExecutor::parse_import] -> [parse_import_file] creates a
//!   [ParsedImport] from the import file.
//!   - For a zip file, this walks the zip directory and creates a lazy
//!     [ImportDocumentStream] for each table (found in
//!     [ParsedImport::documents]). This works because zip files are seekable.
//!   - Other import formats resolve to just one table.
//!   - At the same time, we save a copy of the schemas from the database.
//! - [import_objects] copies data from the [ParsedImport] into the database,
//!   writing into hidden tables (except in [ImportMode::Append]).
//!   - During a multi-table import, we [assign_table_numbers] and create hidden
//!     tables via [prepare_table_for_import] before writing any data. This
//!     requires reading the `_tables` tables first (if present) to find table
//!     numbers; we also attempt to guess table numbers by reading `_id` fields
//!     (see [table_number_for_import]).
//!   - Schemas are validated using a simulated version of the final table
//!     mapping.
//! - [finalize_import] deletes tables that are being replaced and promotes
//!   hidden tables to active, finishing the import. This does more schema
//!   checks (see [schema_constraints]), and additionally checks again for table
//!   number uniqueness, which could fail if there were racing table mapping
//!   changes. See [TableModel::activate_tables].

use std::{
    collections::{
        btree_map::Entry,
        BTreeMap,
        BTreeSet,
        HashSet,
    },
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use bytes::Bytes;
use common::{
    bootstrap_model::{
        schema::SchemaState,
        tables::{
            TableState,
            TABLES_TABLE,
        },
    },
    comparators::tuple::two::TupleKey,
    components::{
        ComponentId,
        ComponentPath,
    },
    document::{
        CreationTime,
        ParsedDocument,
        ID_FIELD,
    },
    errors::report_error,
    execution_context::{
        ExecutionId,
        RequestMetadata,
    },
    knobs::{
        MAX_IMPORT_AGE,
        TRANSACTION_MAX_NUM_USER_WRITES,
        TRANSACTION_MAX_USER_WRITE_SIZE_BYTES,
    },
    persistence::LatestDocument,
    runtime::{
        assert_send,
        Runtime,
    },
    types::{
        FullyQualifiedObjectKey,
        IndexId,
        MemberId,
        RepeatableTimestamp,
        TableName,
        UdfIdentifier,
    },
    virtual_system_mapping::VirtualSystemMapping,
    RequestId,
};
use database::{
    BootstrapComponentsModel,
    Database,
    ImportFacingModel,
    IndexModel,
    SchemaModel,
    TableModel,
    Transaction,
};
use errors::{
    ErrorMetadata,
    ErrorMetadataAnyhowExt,
};
use file_storage::FileStorage;
use futures::{
    pin_mut,
    stream::{
        self,
        BoxStream,
        Peekable,
    },
    Stream,
    StreamExt,
    TryStreamExt,
};
use keybroker::{
    DeploymentOp,
    Identity,
};
use model::{
    deployment_audit_log::{
        types::DeploymentAuditLogEvent,
        DeploymentAuditLogModel,
    },
    file_storage::{
        FILE_STORAGE_TABLE,
        FILE_STORAGE_VIRTUAL_TABLE,
    },
    snapshot_imports::{
        types::{
            ImportFormat,
            ImportMode,
            ImportRequestor,
            ImportState,
            SnapshotImport,
        },
        SnapshotImportModel,
    },
};
use roles::RequireDeploymentOp;
use serde::Serialize;
use serde_json::Value as JsonValue;
use shape_inference::{
    export_context::GeneratedSchema,
    ProdConfig,
};
use storage::Storage;
use sync_types::{
    backoff::Backoff,
    Timestamp,
};
use thousands::Separable;
use usage_tracking::{
    CallType,
    FunctionUsageTracker,
    StorageCallTracker,
    UsageCounter,
};
use value::{
    id_v6::DeveloperDocumentId,
    ConvexObject,
    ConvexValue,
    IdentifierFieldName,
    Size,
    TableMapping,
    TableNamespace,
    TableNumber,
    TabletId,
    TabletIdAndTableNumber,
};

use crate::{
    snapshot_import::{
        audit_log::make_audit_log_event,
        confirmation::info_message_for_import,
        import_error::{
            wrap_import_err,
            ImportError,
        },
        import_file_storage::import_storage_table,
        metrics::log_snapshot_import_age,
        parse::{
            parse_import_file,
            ImportDocumentStream,
            ImportStorageFileStream,
            ParsedImport,
        },
        prepare_component::prepare_component_for_import,
        progress::{
            add_checkpoint_message,
            best_effort_update_progress_message,
        },
        schema_constraints::{
            schemas_for_import,
            ImportSchemaConstraints,
            SchemasForImport,
        },
    },
    Application,
};

mod audit_log;
mod confirmation;
mod import_error;
mod import_file_storage;
mod metrics;
mod parse;
mod prepare_component;
mod progress;
mod schema_constraints;
mod table_change;
mod worker;

pub use worker::SnapshotImportWorker;

// NB: This is a bandaid. In general, we want to retry forever on system
// failures, because all system failures should be transient. If we have
// nontransient system errors, those are bugs and we should fix them. However,
// while we are in the process, use this as a bandaid to limit the damage. Once
// nontransient system errors are fixed, we can remove this.
const SNAPSHOT_IMPORT_MAX_SYSTEM_FAILURES: u32 = 5;

struct SnapshotImportExecutor<RT: Runtime> {
    runtime: RT,
    database: Database<RT>,
    snapshot_imports_storage: Arc<dyn Storage>,
    file_storage: FileStorage<RT>,
    usage_tracking: UsageCounter,
    backoff: Backoff,
}

impl<RT: Runtime> SnapshotImportExecutor<RT> {
    async fn handle_uploaded_state(
        &self,
        snapshot_import: ParsedDocument<SnapshotImport>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(snapshot_import.state == ImportState::Uploaded);
        tracing::info!("Marking snapshot import as WaitingForConfirmation");
        let import_id = snapshot_import.developer_id();
        match info_message_for_import(self, snapshot_import).await {
            Ok((info_message, require_manual_confirmation, new_checkpoints)) => {
                self.database
                    .execute_with_overloaded_retries(
                        Identity::system(),
                        FunctionUsageTracker::new(),
                        "snapshot_import_waiting_for_confirmation",
                        |tx| {
                            async {
                                let mut import_model = SnapshotImportModel::new(tx);
                                import_model
                                    .mark_waiting_for_confirmation(
                                        import_id,
                                        info_message.clone(),
                                        require_manual_confirmation,
                                        new_checkpoints.clone(),
                                    )
                                    .await?;
                                Ok(())
                            }
                            .into()
                        },
                    )
                    .await?;
            },
            Err(e) => {
                let mut e = wrap_import_err(e);
                if e.is_bad_request()
                    || self.backoff.failures() >= SNAPSHOT_IMPORT_MAX_SYSTEM_FAILURES
                {
                    report_error(&mut e).await;
                    self.database
                        .execute_with_overloaded_retries(
                            Identity::system(),
                            FunctionUsageTracker::new(),
                            "snapshot_import_fail",
                            |tx| {
                                async {
                                    let mut import_model = SnapshotImportModel::new(tx);
                                    import_model
                                        .fail_import(import_id, e.user_facing_message())
                                        .await?;
                                    Ok(())
                                }
                                .into()
                            },
                        )
                        .await?;
                } else {
                    anyhow::bail!(e);
                }
            },
        }
        Ok(())
    }

    async fn handle_in_progress_state(
        &mut self,
        snapshot_import: ParsedDocument<SnapshotImport>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(matches!(
            snapshot_import.state,
            ImportState::InProgress { .. }
        ));
        let import_id = snapshot_import.developer_id();
        match self.attempt_perform_import(snapshot_import).await {
            Ok(()) => {},
            Err(e) => {
                let mut e = wrap_import_err(e);
                if e.is_bad_request()
                    || self.backoff.failures() >= SNAPSHOT_IMPORT_MAX_SYSTEM_FAILURES
                {
                    report_error(&mut e).await;
                    self.database
                        .execute_with_overloaded_retries(
                            Identity::system(),
                            FunctionUsageTracker::new(),
                            "snapshot_import_fail",
                            |tx| {
                                async {
                                    let mut import_model = SnapshotImportModel::new(tx);
                                    import_model
                                        .fail_import(import_id, e.user_facing_message())
                                        .await?;
                                    Ok(())
                                }
                                .into()
                            },
                        )
                        .await?;
                } else {
                    anyhow::bail!(e);
                }
            },
        }
        Ok(())
    }

    fn fail_if_too_old(
        &self,
        snapshot_import: &ParsedDocument<SnapshotImport>,
    ) -> anyhow::Result<()> {
        let creation_time = snapshot_import.creation_time();
        let now = CreationTime::try_from(*self.database.now_ts_for_reads())?;
        let age = Duration::from_millis((f64::from(now) - f64::from(creation_time)) as u64);
        log_snapshot_import_age(age);
        tracing::info!(
            "SnapshotImport attempt of {} starting ({:?}) after its creation.",
            snapshot_import.id(),
            age
        );
        if age > *MAX_IMPORT_AGE {
            anyhow::bail!(ErrorMetadata::bad_request(
                "ImportFailed",
                "Import took too long. Try again or contact Convex."
            ));
        }
        Ok(())
    }

    async fn attempt_perform_import(
        &mut self,
        snapshot_import: ParsedDocument<SnapshotImport>,
    ) -> anyhow::Result<()> {
        self.fail_if_too_old(&snapshot_import)?;
        let (initial_schemas, import) = self.parse_import(snapshot_import.developer_id()).await?;
        let source_size = import.source_size;

        let usage = FunctionUsageTracker::new();

        let (imported_tables, total_documents_imported) = import_objects(
            &self.database,
            &self.file_storage,
            Identity::system(),
            &initial_schemas,
            snapshot_import.mode,
            import,
            usage.clone(),
            Some(snapshot_import.developer_id()),
            snapshot_import.requestor.clone(),
        )
        .await?;

        // Charge file bandwidth for the download of the snapshot from imports storage
        usage
            .track_storage_egress(
                ComponentPath::root(),
                snapshot_import.requestor.usage_tag().to_string(),
                source_size,
            )
            .await;

        let pause_client = self.runtime.pause_client();
        pause_client.wait("before_finalize_import").await;
        let (id, snapshot_import) = snapshot_import.into_id_and_value();
        finalize_import(
            &self.database,
            Identity::system(),
            snapshot_import.member_id,
            RequestMetadata::system(),
            initial_schemas,
            snapshot_import.mode,
            imported_tables,
            AuditLogInfo::SnapshotImport {
                import_format: snapshot_import.format,
            },
            FinalizeImportGuard::InProgress {
                import_id: id.developer_id,
                completed_num_rows_written: total_documents_imported,
            },
            snapshot_import.requestor.clone(),
            usage.clone(),
        )
        .await?;

        // Track usage for snapshot imports
        let tag = snapshot_import.requestor.usage_tag().to_string();
        let call_type = match snapshot_import.requestor {
            ImportRequestor::SnapshotImport | ImportRequestor::StreamingImport => CallType::Import,
            ImportRequestor::CloudRestore { .. } => CallType::CloudRestore,
        };
        self.usage_tracking
            .track_call(
                UdfIdentifier::SystemJob(tag),
                ExecutionId::new(),
                RequestId::new(),
                call_type,
                true,
                usage.gather_user_stats(),
            )
            .await;

        Ok(())
    }

    async fn parse_import(
        &self,
        import_id: DeveloperDocumentId,
    ) -> anyhow::Result<(SchemasForImport, ParsedImport)> {
        let SnapshotImport {
            object_key,
            format,
            component_path,
            ..
        } = {
            let mut tx = self.database.begin(Identity::system()).await?;
            let mut model = SnapshotImportModel::new(&mut tx);
            model
                .get(import_id)
                .await?
                .context("import not found")?
                .into_value()
        };
        let fq_key = match object_key {
            Ok(key) => key,
            Err(key) => self.snapshot_imports_storage.fully_qualified_key(&key),
        };
        let import = parse_import_file(
            format.clone(),
            component_path.clone(),
            self.snapshot_imports_storage.clone(),
            fq_key,
        )
        .await?;

        let component_id = prepare_component_for_import(&self.database, &component_path).await?;
        // Remapping could be more extensive here, it's just relatively simple to handle
        // optional types. We do remapping after parsing rather than during parsing
        // because it seems expensive to read the data for and parse all objects inside
        // of a transaction, though I haven't explicitly tested the performance.
        let mut tx = self.database.begin(Identity::system()).await?;
        let initial_schemas = schemas_for_import(&mut tx).await?;
        let import = match format {
            ImportFormat::Csv(table_name) => {
                remap_empty_string_by_schema(
                    TableNamespace::from(component_id),
                    table_name,
                    &mut tx,
                    import,
                )
                .await?
            },
            _ => import,
        };
        drop(tx);
        Ok((initial_schemas, import))
    }
}

pub async fn start_stored_import<RT: Runtime>(
    application: &Application<RT>,
    identity: Identity,
    format: ImportFormat,
    mode: ImportMode,
    component_path: ComponentPath,
    fq_object_key: FullyQualifiedObjectKey,
    requestor: ImportRequestor,
) -> anyhow::Result<DeveloperDocumentId> {
    identity.require_operation(DeploymentOp::ImportBackups)?;
    let (_, id, _) = application
        .database
        .execute_with_overloaded_retries(
            identity,
            FunctionUsageTracker::new(),
            "snapshot_import_store_uploaded",
            |tx| {
                async {
                    let mut model = SnapshotImportModel::new(tx);
                    model
                        .start_import(
                            format.clone(),
                            mode,
                            component_path.clone(),
                            fq_object_key.clone(),
                            requestor.clone(),
                        )
                        .await
                }
                .into()
            },
        )
        .await?;
    Ok(id)
}

pub async fn perform_import<RT: Runtime>(
    application: &Application<RT>,
    identity: Identity,
    import_id: DeveloperDocumentId,
) -> anyhow::Result<()> {
    identity.require_operation(DeploymentOp::ImportBackups)?;
    application
        .database
        .execute_with_overloaded_retries(
            identity,
            FunctionUsageTracker::new(),
            "snapshot_import_perform",
            |tx| {
                async {
                    let mut import_model = SnapshotImportModel::new(tx);
                    import_model.confirm_import(import_id).await?;
                    Ok(())
                }
                .into()
            },
        )
        .await?;
    Ok(())
}

pub async fn cancel_import<RT: Runtime>(
    application: &Application<RT>,
    identity: Identity,
    import_id: DeveloperDocumentId,
) -> anyhow::Result<()> {
    identity.require_operation(DeploymentOp::ImportBackups)?;
    application
        .database
        .execute_with_overloaded_retries(
            identity,
            FunctionUsageTracker::new(),
            "snapshot_import_cancel",
            |tx| {
                async {
                    let mut import_model = SnapshotImportModel::new(tx);
                    import_model.cancel_import(import_id).await?;
                    Ok(())
                }
                .into()
            },
        )
        .await?;
    Ok(())
}

async fn wait_for_import_worker<RT: Runtime>(
    application: &Application<RT>,
    identity: Identity,
    import_id: DeveloperDocumentId,
) -> anyhow::Result<ParsedDocument<SnapshotImport>> {
    let snapshot_import = loop {
        let mut tx = application.begin(identity.clone()).await?;
        let mut import_model = SnapshotImportModel::new(&mut tx);
        let snapshot_import =
            import_model
                .get(import_id)
                .await?
                .context(ErrorMetadata::not_found(
                    "ImportNotFound",
                    format!("import {import_id} not found"),
                ))?;
        match &snapshot_import.state {
            ImportState::Uploaded | ImportState::InProgress { .. } => {
                let token = tx.into_token()?;
                application
                    .subscribe_and_wait_for_invalidation(token)
                    .await?;
            },
            ImportState::WaitingForConfirmation { .. }
            | ImportState::Completed { .. }
            | ImportState::Failed(..) => {
                break snapshot_import;
            },
        }
    };
    Ok(snapshot_import)
}

pub async fn do_import<RT: Runtime>(
    application: &Application<RT>,
    identity: Identity,
    format: ImportFormat,
    mode: ImportMode,
    component_path: ComponentPath,
    body_stream: BoxStream<'_, anyhow::Result<Bytes>>,
) -> anyhow::Result<u64> {
    let object_key = application.upload_snapshot_import(body_stream).await?;
    do_import_from_object_key(
        application,
        identity,
        format,
        mode,
        component_path,
        object_key,
    )
    .await
}

pub async fn do_import_from_object_key<RT: Runtime>(
    application: &Application<RT>,
    identity: Identity,
    format: ImportFormat,
    mode: ImportMode,
    component_path: ComponentPath,
    export_object_key: FullyQualifiedObjectKey,
) -> anyhow::Result<u64> {
    let import_id = start_stored_import(
        application,
        identity.clone(),
        format,
        mode,
        component_path,
        export_object_key,
        ImportRequestor::SnapshotImport,
    )
    .await?;

    let snapshot_import = wait_for_import_worker(application, identity.clone(), import_id).await?;
    match &snapshot_import.state {
        ImportState::Uploaded | ImportState::InProgress { .. } | ImportState::Completed { .. } => {
            anyhow::bail!("should be WaitingForConfirmation, is {snapshot_import:?}")
        },
        ImportState::WaitingForConfirmation { .. } => {},
        ImportState::Failed(e) => {
            anyhow::bail!(ErrorMetadata::bad_request("ImportFailed", e.to_string()))
        },
    }

    perform_import(application, identity.clone(), import_id).await?;

    let snapshot_import = wait_for_import_worker(application, identity.clone(), import_id).await?;
    match &snapshot_import.state {
        ImportState::Uploaded
        | ImportState::WaitingForConfirmation { .. }
        | ImportState::InProgress { .. } => {
            anyhow::bail!("should be done, is {snapshot_import:?}")
        },
        ImportState::Completed {
            ts: _,
            num_rows_written,
        } => Ok(*num_rows_written as u64),
        ImportState::Failed(e) => {
            anyhow::bail!(ErrorMetadata::bad_request("ImportFailed", e.to_string()))
        },
    }
}

/// Clears tables atomically.
/// This is implemented as an import of empty tables in Replace mode.
///
/// N.B.: This will create components and tables if they don't exist.
pub async fn clear_tables<RT: Runtime>(
    application: &Application<RT>,
    identity: &Identity,
    request_metadata: RequestMetadata,
    table_names: Vec<(ComponentPath, TableName)>,
    requestor: ImportRequestor,
    usage: FunctionUsageTracker,
) -> anyhow::Result<u64> {
    let (initial_schemas, original_table_mapping) = {
        let mut tx = application.begin(identity.clone()).await?;
        (
            schemas_for_import(&mut tx).await?,
            tx.table_mapping().clone(),
        )
    };

    let mut table_mapping = TableMapping::new();
    for (component_path, table_name) in table_names {
        let component_id =
            prepare_component_for_import(&application.database, &component_path).await?;
        let table_number = original_table_mapping
            .namespace(component_id.into())
            .id_and_number_if_exists(&table_name)
            .map(|t| t.table_number);
        let table_id = create_empty_table(
            &application.database,
            identity,
            component_id,
            &table_name,
            table_number,
            None, /* import_id */
            &table_name,
            &component_path,
        )
        .await?;
        table_mapping.insert(
            table_id.tablet_id,
            component_id.into(),
            table_id.table_number,
            table_name,
        );
    }

    let (_ts, documents_deleted) = finalize_import(
        &application.database,
        identity.clone(),
        None,
        request_metadata,
        initial_schemas,
        ImportMode::Replace,
        table_mapping,
        AuditLogInfo::ClearTables,
        FinalizeImportGuard::None,
        requestor,
        usage.clone(),
    )
    .await?;
    Ok(documents_deleted)
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepairFailedImportFromCheckpointsReport {
    pub import_id: String,
    pub dry_run: bool,
    pub import_mode: String,
    pub total_checkpoints: usize,
    pub selected_table_count: usize,
    pub skipped_empty_component_storage_count: usize,
    pub selected_row_count: u64,
    pub documents_deleted: Option<u64>,
    pub selected_tables: Vec<RepairImportTableReport>,
    pub skipped_empty_component_storage: Vec<RepairSkippedCheckpointReport>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepairImportTableReport {
    pub component_path: Option<String>,
    pub display_table_name: String,
    pub physical_table_name: String,
    pub tablet_id: String,
    pub table_number: u32,
    pub row_count: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepairSkippedCheckpointReport {
    pub component_path: Option<String>,
    pub display_table_name: String,
    pub tablet_id: Option<String>,
    pub row_count: u64,
}

struct RepairFailedImportFromCheckpointsPlan {
    import_id: DeveloperDocumentId,
    expected_failed_import: SnapshotImport,
    mode: ImportMode,
    format: ImportFormat,
    requestor: ImportRequestor,
    initial_schemas: SchemasForImport,
    imported_tables: TableMapping,
    table_mapping_for_schema: TableMapping,
    virtual_system_mapping: VirtualSystemMapping,
    validation_ts: RepeatableTimestamp,
    checkpoint_by_id_indexes: BTreeMap<TabletId, IndexId>,
    active_table_guards: Vec<RepairActiveTableGuard>,
    checkpoint_tablet_guards: Vec<RepairCheckpointTabletGuard>,
    completed_num_rows_written: u64,
    report: RepairFailedImportFromCheckpointsReport,
}

async fn ensure_no_other_nonterminal_snapshot_imports<RT: Runtime>(
    tx: &mut Transaction<RT>,
    repair_import_id: DeveloperDocumentId,
) -> anyhow::Result<()> {
    let imports = SnapshotImportModel::new(tx).list().await?;
    let has_conflicting_import = imports.iter().any(|snapshot_import| {
        snapshot_import.developer_id() != repair_import_id
            && matches!(
                &snapshot_import.state,
                ImportState::Uploaded
                    | ImportState::WaitingForConfirmation { .. }
                    | ImportState::InProgress { .. }
            )
    });
    anyhow::ensure!(
        !has_conflicting_import,
        ErrorMetadata::bad_request(
            "ImportRepairConflictingImport",
            "another snapshot import is uploaded, waiting for confirmation, or in progress",
        )
    );
    Ok(())
}

struct RepairActiveTableGuard {
    component_path: ComponentPath,
    display_table_name: TableName,
    physical_table_name: TableName,
    namespace: TableNamespace,
    active_tablet_id: Option<TabletId>,
    existing_rows_in_table: i64,
    existing_rows_to_delete: i64,
}

struct RepairCheckpointTabletGuard {
    component_path: ComponentPath,
    display_table_name: TableName,
    physical_table_name: TableName,
    namespace: TableNamespace,
    tablet_id: TabletId,
    table_number: TableNumber,
    expected_row_count: u64,
}

fn repair_table_mapping_for_schema(
    current_table_mapping: &TableMapping,
    imported_tables: &TableMapping,
) -> TableMapping {
    // Replace-all retains active system tables unless the import replaces the
    // same physical table (currently only _file_storage).
    let mut table_mapping_for_schema = TableMapping::new();
    for (tablet_id, namespace, table_number, table_name) in current_table_mapping.iter() {
        if current_table_mapping.is_active(tablet_id)
            && table_name.is_system()
            && !imported_tables.namespace(namespace).name_exists(table_name)
        {
            table_mapping_for_schema.insert(tablet_id, namespace, table_number, table_name.clone());
        }
    }
    for (tablet_id, namespace, table_number, table_name) in imported_tables.iter() {
        table_mapping_for_schema.insert(tablet_id, namespace, table_number, table_name.clone());
    }
    table_mapping_for_schema
}

fn checkpoint_physical_table_name(display_table_name: &TableName) -> anyhow::Result<TableName> {
    if display_table_name == &FILE_STORAGE_VIRTUAL_TABLE {
        return Ok(FILE_STORAGE_TABLE.clone());
    }
    anyhow::ensure!(
        !display_table_name.is_system(),
        ErrorMetadata::bad_request(
            "InvalidImportCheckpointTable",
            format!("checkpoint maps to unsupported system table {display_table_name}"),
        )
    );
    Ok(display_table_name.clone())
}

fn is_skippable_empty_component_storage_checkpoint(
    component_path: &ComponentPath,
    display_table_name: &TableName,
    num_rows_written: i64,
    total_num_rows_to_write: i64,
    existing_rows_in_table: i64,
    existing_rows_to_delete: i64,
) -> bool {
    !component_path.is_root()
        && display_table_name == &FILE_STORAGE_VIRTUAL_TABLE
        && num_rows_written == 0
        && total_num_rows_to_write == 0
        && existing_rows_in_table == 0
        && existing_rows_to_delete == 0
}

async fn build_repair_active_table_guard<RT: Runtime>(
    tx: &mut Transaction<RT>,
    component_path: &ComponentPath,
    display_table_name: &TableName,
    physical_table_name: &TableName,
    namespace: TableNamespace,
    existing_rows_in_table: i64,
    existing_rows_to_delete: i64,
) -> anyhow::Result<RepairActiveTableGuard> {
    anyhow::ensure!(
        existing_rows_in_table == existing_rows_to_delete,
        ErrorMetadata::bad_request(
            "ImportCheckpointExistingRowsMismatch",
            format!(
                "replace-all checkpoint {}{} expected to delete {} of {} existing rows",
                display_table_name,
                component_path.in_component_str(),
                existing_rows_to_delete,
                existing_rows_in_table,
            ),
        )
    );
    let active_tablet_id = tx
        .table_mapping()
        .namespace(namespace)
        .id_if_exists(physical_table_name);
    let current_existing_rows = match active_tablet_id {
        Some(tablet_id) => TableModel::new(tx).must_count_tablet(tablet_id).await?,
        None => 0,
    };
    anyhow::ensure!(
        current_existing_rows == existing_rows_in_table as u64,
        ErrorMetadata::bad_request(
            "ImportCheckpointActiveTableChanged",
            format!(
                "checkpoint {}{} recorded {} existing rows, but the active table currently has \
                 {current_existing_rows}",
                display_table_name,
                component_path.in_component_str(),
                existing_rows_in_table,
            ),
        )
    );
    Ok(RepairActiveTableGuard {
        component_path: component_path.clone(),
        display_table_name: display_table_name.clone(),
        physical_table_name: physical_table_name.clone(),
        namespace,
        active_tablet_id,
        existing_rows_in_table,
        existing_rows_to_delete,
    })
}

async fn validate_repair_active_table_guard<RT: Runtime>(
    tx: &mut Transaction<RT>,
    guard: &RepairActiveTableGuard,
) -> anyhow::Result<()> {
    let (_, current_component_id) = BootstrapComponentsModel::new(tx)
        .must_component_path_to_ids(&guard.component_path)
        .with_context(|| {
            ErrorMetadata::bad_request(
                "ImportCheckpointComponentChanged",
                format!(
                    "component path {} for checkpoint {} changed before repair finalization",
                    String::from(guard.component_path.clone()),
                    guard.display_table_name,
                ),
            )
        })?;
    anyhow::ensure!(
        TableNamespace::from(current_component_id) == guard.namespace,
        ErrorMetadata::bad_request(
            "ImportCheckpointComponentChanged",
            format!(
                "component path {} for checkpoint {} changed namespace before repair finalization",
                String::from(guard.component_path.clone()),
                guard.display_table_name,
            ),
        )
    );
    let current_active_tablet_id = tx
        .table_mapping()
        .namespace(guard.namespace)
        .id_if_exists(&guard.physical_table_name);
    anyhow::ensure!(
        current_active_tablet_id == guard.active_tablet_id,
        ErrorMetadata::bad_request(
            "ImportCheckpointActiveTableChanged",
            format!(
                "checkpoint {}{} expected active tablet {:?}, but found {:?}",
                guard.display_table_name,
                guard.component_path.in_component_str(),
                guard.active_tablet_id,
                current_active_tablet_id,
            ),
        )
    );
    let current_existing_rows = match current_active_tablet_id {
        Some(tablet_id) => TableModel::new(tx).must_count_tablet(tablet_id).await?,
        None => 0,
    };
    anyhow::ensure!(
        guard.existing_rows_in_table == guard.existing_rows_to_delete
            && current_existing_rows == guard.existing_rows_in_table as u64,
        ErrorMetadata::bad_request(
            "ImportCheckpointActiveTableChanged",
            format!(
                "checkpoint {}{} recorded {} existing rows, but the active table currently has \
                 {current_existing_rows}",
                guard.display_table_name,
                guard.component_path.in_component_str(),
                guard.existing_rows_in_table,
            ),
        )
    );
    Ok(())
}

async fn validate_repair_checkpoint_tablet_guard<RT: Runtime>(
    tx: &mut Transaction<RT>,
    guard: &RepairCheckpointTabletGuard,
) -> anyhow::Result<()> {
    let table_metadata = TableModel::new(tx)
        .get_table_metadata(guard.tablet_id)
        .await?;
    anyhow::ensure!(
        table_metadata.state == TableState::Hidden
            && table_metadata.namespace == guard.namespace
            && table_metadata.number == guard.table_number
            && table_metadata.name == guard.physical_table_name,
        ErrorMetadata::bad_request(
            "ImportRepairCheckpointChanged",
            format!(
                "checkpoint tablet {} for {}{} changed before repair finalization",
                guard.tablet_id,
                guard.display_table_name,
                guard.component_path.in_component_str(),
            ),
        )
    );
    let row_count = TableModel::new(tx)
        .must_count_tablet(guard.tablet_id)
        .await?;
    anyhow::ensure!(
        row_count == guard.expected_row_count,
        ErrorMetadata::bad_request(
            "ImportRepairCheckpointChanged",
            format!(
                "checkpoint tablet {} for {}{} has {row_count} rows, expected {}",
                guard.tablet_id,
                guard.display_table_name,
                guard.component_path.in_component_str(),
                guard.expected_row_count,
            ),
        )
    );
    Ok(())
}

async fn validate_repair_checkpoint_indexes<RT: Runtime>(
    tx: &mut Transaction<RT>,
    imported_tables: &TableMapping,
    checkpoint_tablet_guards: &[RepairCheckpointTabletGuard],
) -> anyhow::Result<()> {
    for guard in checkpoint_tablet_guards {
        if !imported_tables
            .namespace(guard.namespace)
            .name_exists(&guard.physical_table_name)
        {
            // A strictly empty component storage checkpoint can be guarded but
            // intentionally omitted from activation.
            continue;
        }
        let hidden_indexes = IndexModel::new(tx)
            .all_indexes_on_table(guard.tablet_id)
            .await?;
        anyhow::ensure!(
            hidden_indexes.iter().all(|index| index.config.is_enabled()),
            ErrorMetadata::bad_request(
                "ImportCheckpointIndexNotReady",
                format!(
                    "checkpoint tablet {} for {}{} has an index that is not enabled",
                    guard.tablet_id,
                    guard.display_table_name,
                    guard.component_path.in_component_str(),
                ),
            )
        );

        let active_tablet_id = tx
            .table_mapping()
            .namespace(guard.namespace)
            .id_if_exists(&guard.physical_table_name);
        let Some(active_tablet_id) = active_tablet_id else {
            // A fresh replacement for a nonexistent table starts with exactly
            // the two built-in indexes.
            anyhow::ensure!(
                hidden_indexes.len() == 2
                    && hidden_indexes
                        .iter()
                        .all(|index| index.name.is_by_id_or_creation_time()),
                ErrorMetadata::bad_request(
                    "ImportCheckpointIndexesChanged",
                    format!(
                        "checkpoint tablet {} for {}{} has indexes that would not be created for \
                         a currently nonexistent table",
                        guard.tablet_id,
                        guard.display_table_name,
                        guard.component_path.in_component_str(),
                    ),
                )
            );
            continue;
        };

        let active_indexes = IndexModel::new(tx)
            .all_indexes_on_table(active_tablet_id)
            .await?;
        anyhow::ensure!(
            active_indexes
                .iter()
                .all(|index| !index.config.is_backfilling()),
            ErrorMetadata::bad_request(
                "ImportCheckpointIndexesChanged",
                format!(
                    "active table {}{} has an index that is still backfilling",
                    guard.display_table_name,
                    guard.component_path.in_component_str(),
                ),
            )
        );
        let active_enabled_indexes = active_indexes
            .iter()
            .filter(|index| index.config.is_enabled())
            .collect::<Vec<_>>();
        let indexes_match = hidden_indexes.len() == active_enabled_indexes.len()
            && active_enabled_indexes.iter().all(|active_index| {
                hidden_indexes.iter().any(|hidden_index| {
                    hidden_index.name.descriptor() == active_index.name.descriptor()
                        && hidden_index.config.same_spec(&active_index.config)
                })
            });
        anyhow::ensure!(
            indexes_match,
            ErrorMetadata::bad_request(
                "ImportCheckpointIndexesChanged",
                format!(
                    "checkpoint indexes for {}{} do not match the current active table",
                    guard.display_table_name,
                    guard.component_path.in_component_str(),
                ),
            )
        );
    }
    Ok(())
}

fn validate_repair_table_number_conflicts<RT: Runtime>(
    tx: &mut Transaction<RT>,
    imported_tables: &TableMapping,
) -> anyhow::Result<()> {
    for (_tablet_id, namespace, table_number, table_name) in imported_tables.iter() {
        let Some(active_table_name) = tx
            .table_mapping()
            .namespace(namespace)
            .name_by_number_if_exists(table_number)
            .cloned()
        else {
            continue;
        };
        if &active_table_name == table_name || !active_table_name.is_system() {
            continue;
        }
        anyhow::bail!(ErrorMetadata::bad_request(
            "ImportCheckpointTableNumberConflict",
            format!(
                "checkpoint table {table_name} in namespace {namespace:?} uses table number {}, \
                 which conflicts with active system table {active_table_name}",
                u32::from(table_number),
            ),
        ));
    }
    Ok(())
}

async fn build_repair_failed_import_from_checkpoints_plan<RT: Runtime>(
    tx: &mut Transaction<RT>,
    import_id: DeveloperDocumentId,
    dry_run: bool,
) -> anyhow::Result<RepairFailedImportFromCheckpointsPlan> {
    let snapshot_import =
        SnapshotImportModel::new(tx)
            .get(import_id)
            .await?
            .context(ErrorMetadata::not_found(
                "ImportNotFound",
                format!("import {import_id} not found"),
            ))?;
    match &snapshot_import.state {
        ImportState::Failed(_) => {},
        ImportState::Completed { .. } => {
            anyhow::bail!(ErrorMetadata::bad_request(
                "ImportAlreadyCompleted",
                "import is already completed",
            ))
        },
        state => {
            anyhow::bail!(ErrorMetadata::bad_request(
                "ImportNotFailed",
                format!("expected failed import, found {state:?}"),
            ))
        },
    }
    let expected_failed_import = snapshot_import.clone().into_value();
    ensure_no_other_nonterminal_snapshot_imports(tx, import_id).await?;
    anyhow::ensure!(
        matches!(&snapshot_import.format, ImportFormat::Zip),
        ErrorMetadata::bad_request(
            "UnsupportedImportRepair",
            "checkpoint repair currently supports only ZIP snapshot imports",
        )
    );
    anyhow::ensure!(
        matches!(snapshot_import.mode, ImportMode::ReplaceAll),
        ErrorMetadata::bad_request(
            "UnsupportedImportRepair",
            "checkpoint repair currently supports only replace-all imports",
        )
    );
    let checkpoints = snapshot_import
        .checkpoints
        .clone()
        .context(ErrorMetadata::bad_request(
            "MissingImportCheckpoints",
            "failed import has no checkpoints",
        ))?;
    anyhow::ensure!(
        !checkpoints.is_empty(),
        ErrorMetadata::bad_request(
            "MissingImportCheckpoints",
            "failed import has an empty checkpoint list",
        )
    );

    let initial_schemas = schemas_for_import(tx).await?;
    let mut imported_tables = TableMapping::new();
    let mut selected_tables = vec![];
    let mut skipped_empty_component_storage = vec![];
    let mut selected_row_count = 0u64;
    let mut completed_num_rows_written = 0u64;
    let mut seen_names = BTreeSet::new();
    let mut selected_numbers = BTreeSet::new();
    let mut active_table_guards = vec![];
    let mut checkpoint_tablet_guards = vec![];

    for checkpoint in &checkpoints {
        anyhow::ensure!(
            checkpoint.num_rows_written == checkpoint.total_num_rows_to_write,
            ErrorMetadata::bad_request(
                "IncompleteImportCheckpoint",
                format!(
                    "checkpoint {}{} is incomplete: wrote {} of {} rows",
                    checkpoint.display_table_name,
                    checkpoint.component_path.in_component_str(),
                    checkpoint.num_rows_written,
                    checkpoint.total_num_rows_to_write,
                ),
            )
        );
        anyhow::ensure!(
            checkpoint.num_rows_written >= 0,
            ErrorMetadata::bad_request(
                "InvalidImportCheckpoint",
                format!(
                    "checkpoint {}{} has negative row count",
                    checkpoint.display_table_name,
                    checkpoint.component_path.in_component_str(),
                ),
            )
        );
        anyhow::ensure!(
            checkpoint.existing_rows_in_table >= 0 && checkpoint.existing_rows_to_delete >= 0,
            ErrorMetadata::bad_request(
                "InvalidImportCheckpoint",
                format!(
                    "checkpoint {}{} has negative existing row counts",
                    checkpoint.display_table_name,
                    checkpoint.component_path.in_component_str(),
                ),
            )
        );

        if is_skippable_empty_component_storage_checkpoint(
            &checkpoint.component_path,
            &checkpoint.display_table_name,
            checkpoint.num_rows_written,
            checkpoint.total_num_rows_to_write,
            checkpoint.existing_rows_in_table,
            checkpoint.existing_rows_to_delete,
        ) {
            let (_, component_id) = BootstrapComponentsModel::new(tx)
                .must_component_path_to_ids(&checkpoint.component_path)
                .with_context(|| {
                    ErrorMetadata::bad_request(
                        "ImportCheckpointComponentMissing",
                        format!(
                            "component path {} for empty component storage checkpoint is missing",
                            String::from(checkpoint.component_path.clone()),
                        ),
                    )
                })?;
            let namespace = component_id.into();
            let active_table_guard = build_repair_active_table_guard(
                tx,
                &checkpoint.component_path,
                &checkpoint.display_table_name,
                &FILE_STORAGE_TABLE,
                namespace,
                checkpoint.existing_rows_in_table,
                checkpoint.existing_rows_to_delete,
            )
            .await?;
            anyhow::ensure!(
                seen_names.insert((namespace, FILE_STORAGE_TABLE.clone())),
                ErrorMetadata::bad_request(
                    "ImportCheckpointDuplicateTable",
                    format!(
                        "duplicate checkpoint for {}{}",
                        checkpoint.display_table_name,
                        checkpoint.component_path.in_component_str(),
                    ),
                )
            );
            active_table_guards.push(active_table_guard);
            let row_count = if let Some(tablet_id) = checkpoint.tablet_id {
                let table_metadata = TableModel::new(tx).get_table_metadata(tablet_id).await?;
                anyhow::ensure!(
                    table_metadata.state == TableState::Hidden,
                    ErrorMetadata::bad_request(
                        "ImportCheckpointNotHidden",
                        format!(
                            "empty component storage checkpoint {}{} is not hidden",
                            checkpoint.display_table_name,
                            checkpoint.component_path.in_component_str(),
                        ),
                    )
                );
                anyhow::ensure!(
                    table_metadata.namespace == namespace
                        && table_metadata.name == FILE_STORAGE_TABLE,
                    ErrorMetadata::bad_request(
                        "ImportCheckpointTableMismatch",
                        format!(
                            "empty component storage checkpoint {}{} maps to {} in namespace {:?}",
                            checkpoint.display_table_name,
                            checkpoint.component_path.in_component_str(),
                            table_metadata.name,
                            table_metadata.namespace,
                        ),
                    )
                );
                let row_count = TableModel::new(tx).must_count_tablet(tablet_id).await?;
                checkpoint_tablet_guards.push(RepairCheckpointTabletGuard {
                    component_path: checkpoint.component_path.clone(),
                    display_table_name: checkpoint.display_table_name.clone(),
                    physical_table_name: FILE_STORAGE_TABLE.clone(),
                    namespace,
                    tablet_id,
                    table_number: table_metadata.number,
                    expected_row_count: 0,
                });
                row_count
            } else {
                0
            };
            anyhow::ensure!(
                row_count == 0,
                ErrorMetadata::bad_request(
                    "ImportCheckpointNotEmpty",
                    format!(
                        "empty component storage checkpoint {}{} has {row_count} hidden rows",
                        checkpoint.display_table_name,
                        checkpoint.component_path.in_component_str(),
                    ),
                )
            );
            skipped_empty_component_storage.push(RepairSkippedCheckpointReport {
                component_path: checkpoint.component_path.clone().serialize(),
                display_table_name: checkpoint.display_table_name.to_string(),
                tablet_id: checkpoint.tablet_id.map(|tablet_id| tablet_id.to_string()),
                row_count,
            });
            continue;
        }

        let tablet_id = checkpoint.tablet_id.context(ErrorMetadata::bad_request(
            "ImportCheckpointMissingTablet",
            format!(
                "checkpoint {}{} has no tablet id",
                checkpoint.display_table_name,
                checkpoint.component_path.in_component_str(),
            ),
        ))?;
        let (_, component_id) = BootstrapComponentsModel::new(tx)
            .must_component_path_to_ids(&checkpoint.component_path)
            .with_context(|| {
                ErrorMetadata::bad_request(
                    "ImportCheckpointComponentMissing",
                    format!(
                        "component path {} for checkpoint {} is missing",
                        String::from(checkpoint.component_path.clone()),
                        checkpoint.display_table_name,
                    ),
                )
            })?;
        let namespace = component_id.into();
        let physical_table_name = checkpoint_physical_table_name(&checkpoint.display_table_name)?;
        let active_table_guard = build_repair_active_table_guard(
            tx,
            &checkpoint.component_path,
            &checkpoint.display_table_name,
            &physical_table_name,
            namespace,
            checkpoint.existing_rows_in_table,
            checkpoint.existing_rows_to_delete,
        )
        .await?;

        let table_metadata = TableModel::new(tx).get_table_metadata(tablet_id).await?;
        anyhow::ensure!(
            table_metadata.state == TableState::Hidden,
            ErrorMetadata::bad_request(
                "ImportCheckpointNotHidden",
                format!(
                    "checkpoint {}{} points at a non-hidden tablet",
                    checkpoint.display_table_name,
                    checkpoint.component_path.in_component_str(),
                ),
            )
        );
        anyhow::ensure!(
            table_metadata.namespace == namespace && table_metadata.name == physical_table_name,
            ErrorMetadata::bad_request(
                "ImportCheckpointTableMismatch",
                format!(
                    "checkpoint {}{} maps to {} in namespace {:?}",
                    checkpoint.display_table_name,
                    checkpoint.component_path.in_component_str(),
                    table_metadata.name,
                    table_metadata.namespace,
                ),
            )
        );
        let row_count = TableModel::new(tx).must_count_tablet(tablet_id).await?;
        anyhow::ensure!(
            row_count == checkpoint.total_num_rows_to_write as u64,
            ErrorMetadata::bad_request(
                "ImportCheckpointCountMismatch",
                format!(
                    "checkpoint {}{} has {row_count} hidden rows, expected {}",
                    checkpoint.display_table_name,
                    checkpoint.component_path.in_component_str(),
                    checkpoint.total_num_rows_to_write,
                ),
            )
        );
        anyhow::ensure!(
            seen_names.insert((namespace, physical_table_name.clone())),
            ErrorMetadata::bad_request(
                "ImportCheckpointDuplicateTable",
                format!(
                    "duplicate checkpoint for {}{}",
                    checkpoint.display_table_name,
                    checkpoint.component_path.in_component_str(),
                ),
            )
        );
        active_table_guards.push(active_table_guard);
        anyhow::ensure!(
            selected_numbers.insert((namespace, table_metadata.number)),
            ErrorMetadata::bad_request(
                "ImportCheckpointDuplicateTableNumber",
                format!(
                    "duplicate table number {} in checkpoint {}{}",
                    u32::from(table_metadata.number),
                    checkpoint.display_table_name,
                    checkpoint.component_path.in_component_str(),
                ),
            )
        );
        imported_tables.insert(
            tablet_id,
            namespace,
            table_metadata.number,
            physical_table_name.clone(),
        );
        checkpoint_tablet_guards.push(RepairCheckpointTabletGuard {
            component_path: checkpoint.component_path.clone(),
            display_table_name: checkpoint.display_table_name.clone(),
            physical_table_name: physical_table_name.clone(),
            namespace,
            tablet_id,
            table_number: table_metadata.number,
            expected_row_count: row_count,
        });
        selected_row_count =
            selected_row_count
                .checked_add(row_count)
                .context(ErrorMetadata::bad_request(
                    "ImportCheckpointCountOverflow",
                    "selected checkpoint row count exceeds the supported range",
                ))?;
        if physical_table_name != FILE_STORAGE_TABLE {
            // Normal imports report storage files in progress messages, but
            // exclude them from Completed.num_rows_written.
            completed_num_rows_written = completed_num_rows_written
                .checked_add(row_count)
                .filter(|count| *count <= i64::MAX as u64)
                .context(ErrorMetadata::bad_request(
                    "ImportCheckpointCountOverflow",
                    "completed checkpoint row count exceeds the supported range",
                ))?;
        }
        selected_tables.push(RepairImportTableReport {
            component_path: checkpoint.component_path.clone().serialize(),
            display_table_name: checkpoint.display_table_name.to_string(),
            physical_table_name: physical_table_name.to_string(),
            tablet_id: tablet_id.to_string(),
            table_number: u32::from(table_metadata.number),
            row_count,
        });
    }

    // This temporary repair path only activates complete checkpoint tablets.
    // Refuse to run if the current active user-table set is not represented by
    // the validated checkpoints.
    let active_user_tables = tx
        .table_mapping()
        .iter_active_user_tables()
        .map(|(_tablet_id, namespace, _table_number, table_name)| (namespace, table_name.clone()))
        .collect::<Vec<_>>();
    for (namespace, table_name) in active_user_tables {
        if tx.get_component_path(namespace.into()).is_none() {
            continue;
        }
        anyhow::ensure!(
            seen_names.contains(&(namespace, table_name.clone())),
            ErrorMetadata::bad_request(
                "ImportCheckpointMissingActiveTable",
                format!(
                    "active table {table_name} in namespace {namespace:?} is missing from the \
                     failed import checkpoints",
                ),
            )
        );
    }
    validate_repair_table_number_conflicts(tx, &imported_tables)?;

    // Normal replace-all imports create empty hidden tables for every table in
    // the active schema, even when the source has no rows for that table. Such
    // a table is not recoverable unless its checkpoint identifies the hidden
    // tablet, so do not silently finalize a different table mapping.
    for (namespace, schema_state, (_, schema)) in &initial_schemas {
        if schema_state != &SchemaState::Active
            || tx.get_component_path((*namespace).into()).is_none()
        {
            continue;
        }
        for table_name in schema.tables.keys() {
            anyhow::ensure!(
                seen_names.contains(&(*namespace, table_name.clone())),
                ErrorMetadata::bad_request(
                    "ImportCheckpointMissingSchemaTable",
                    format!(
                        "active schema table {table_name} in namespace {namespace:?} is missing \
                         from the failed import checkpoints",
                    ),
                )
            );
        }
    }

    anyhow::ensure!(
        !selected_tables.is_empty(),
        ErrorMetadata::bad_request(
            "ImportRepairNoTables",
            "no checkpoint tablets selected for repair",
        )
    );
    let schema_constraints =
        ImportSchemaConstraints::new(&imported_tables, initial_schemas.clone());
    schema_constraints.validate(tx).await?;
    validate_repair_checkpoint_indexes(tx, &imported_tables, &checkpoint_tablet_guards).await?;

    let current_table_mapping = tx.table_mapping().clone();
    let table_mapping_for_schema =
        repair_table_mapping_for_schema(&current_table_mapping, &imported_tables);
    let checkpoint_by_id_indexes = IndexModel::new(tx).by_id_indexes().await?;
    for guard in &checkpoint_tablet_guards {
        anyhow::ensure!(
            checkpoint_by_id_indexes.contains_key(&guard.tablet_id),
            ErrorMetadata::bad_request(
                "ImportCheckpointIndexMissing",
                format!(
                    "checkpoint tablet {} for {}{} has no by-id index",
                    guard.tablet_id,
                    guard.display_table_name,
                    guard.component_path.in_component_str(),
                ),
            )
        );
    }
    let virtual_system_mapping = tx.virtual_system_mapping().clone();
    let validation_ts = tx.begin_timestamp();

    tracing::info!(
        "Validated failed import repair plan for {import_id}: selected {} hidden tables, skipped \
         {} empty component storage checkpoints, dry_run={dry_run}",
        selected_tables.len(),
        skipped_empty_component_storage.len(),
    );

    Ok(RepairFailedImportFromCheckpointsPlan {
        import_id,
        expected_failed_import,
        mode: snapshot_import.mode,
        format: snapshot_import.format.clone(),
        requestor: snapshot_import.requestor.clone(),
        initial_schemas,
        imported_tables,
        table_mapping_for_schema,
        virtual_system_mapping,
        validation_ts,
        checkpoint_by_id_indexes,
        active_table_guards,
        checkpoint_tablet_guards,
        completed_num_rows_written,
        report: RepairFailedImportFromCheckpointsReport {
            import_id: import_id.to_string(),
            dry_run,
            import_mode: snapshot_import.mode.to_string(),
            total_checkpoints: checkpoints.len(),
            selected_table_count: selected_tables.len(),
            skipped_empty_component_storage_count: skipped_empty_component_storage.len(),
            selected_row_count,
            documents_deleted: None,
            selected_tables,
            skipped_empty_component_storage,
        },
    })
}

async fn validate_repair_checkpoint_documents<RT: Runtime>(
    database: &Database<RT>,
    validation_ts: RepeatableTimestamp,
    checkpoint_tablet_guards: &[RepairCheckpointTabletGuard],
    initial_schemas: &SchemasForImport,
    table_mapping_for_schema: &TableMapping,
    virtual_system_mapping: &VirtualSystemMapping,
    checkpoint_by_id_indexes: &BTreeMap<TabletId, IndexId>,
) -> anyhow::Result<()> {
    let user_tablet_ids = checkpoint_tablet_guards
        .iter()
        .filter(|guard| !guard.physical_table_name.is_system())
        .map(|guard| guard.tablet_id)
        .collect::<Vec<_>>();
    if user_tablet_ids.is_empty() {
        return Ok(());
    }
    let mut table_iterator = database
        .table_iterator(validation_ts, 1000)
        .multi(user_tablet_ids);

    for guard in checkpoint_tablet_guards
        .iter()
        .filter(|guard| !guard.physical_table_name.is_system())
    {
        let by_id_index = *checkpoint_by_id_indexes
            .get(&guard.tablet_id)
            .context("validated checkpoint tablet has no by-id index")?;
        {
            let stream =
                table_iterator.stream_documents_in_table(guard.tablet_id, by_id_index, None);
            pin_mut!(stream);
            while let Some(LatestDocument {
                value: document, ..
            }) = stream.try_next().await?
            {
                for (namespace, _schema_state, (_, schema)) in initial_schemas {
                    if *namespace != guard.namespace {
                        continue;
                    }
                    if let Err(schema_error) = schema.check_existing_document(
                        &document,
                        guard.physical_table_name.clone(),
                        &table_mapping_for_schema.namespace(guard.namespace),
                        virtual_system_mapping,
                    ) {
                        anyhow::bail!(ErrorMetadata::bad_request(
                            "ImportCheckpointSchemaMismatch",
                            format!(
                                "checkpoint {}{} does not match the current schema: {schema_error}",
                                guard.display_table_name,
                                guard.component_path.in_component_str(),
                            ),
                        ));
                    }
                }
            }
        }
        table_iterator.unregister_table(guard.tablet_id)?;
    }
    Ok(())
}

pub async fn repair_failed_import_from_checkpoints<RT: Runtime>(
    application: &Application<RT>,
    identity: Identity,
    request_metadata: RequestMetadata,
    import_id: DeveloperDocumentId,
    dry_run: bool,
) -> anyhow::Result<RepairFailedImportFromCheckpointsReport> {
    identity.require_operation(DeploymentOp::ImportBackups)?;
    let (_plan_ts, plan, _plan_occ_stats) = application
        .database
        .execute_with_overloaded_retries(
            identity.clone(),
            FunctionUsageTracker::new(),
            "snapshot_import_repair_from_checkpoints_validate",
            |tx| {
                async {
                    build_repair_failed_import_from_checkpoints_plan(tx, import_id, dry_run).await
                }
                .into()
            },
        )
        .await?;
    if dry_run {
        validate_repair_checkpoint_documents(
            &application.database,
            plan.validation_ts,
            &plan.checkpoint_tablet_guards,
            &plan.initial_schemas,
            &plan.table_mapping_for_schema,
            &plan.virtual_system_mapping,
            &plan.checkpoint_by_id_indexes,
        )
        .await?;
        return Ok(plan.report);
    }

    let (_ts, documents_deleted) = finalize_import(
        &application.database,
        identity.clone(),
        None,
        request_metadata,
        plan.initial_schemas,
        plan.mode,
        plan.imported_tables,
        AuditLogInfo::SnapshotImport {
            import_format: plan.format,
        },
        FinalizeImportGuard::FailedRepair {
            import_id: plan.import_id,
            expected_failed_import: plan.expected_failed_import,
            active_table_guards: plan.active_table_guards,
            checkpoint_tablet_guards: plan.checkpoint_tablet_guards,
            completed_num_rows_written: plan.completed_num_rows_written,
        },
        plan.requestor,
        FunctionUsageTracker::new(),
    )
    .await?;

    let mut report = plan.report;
    report.dry_run = false;
    report.documents_deleted = Some(documents_deleted);
    Ok(report)
}

/// Reads all objects from a [`ParsedImport`] and writes them into the database.
/// Returns a table mapping containing just the imported tables. If `mode` is
/// ReplaceAll this includes empty tables for tables that should be cleared.
async fn import_objects<RT: Runtime>(
    database: &Database<RT>,
    file_storage: &FileStorage<RT>,
    identity: Identity,
    initial_schemas: &SchemasForImport,
    mode: ImportMode,
    import: ParsedImport,
    usage: FunctionUsageTracker,
    import_id: Option<DeveloperDocumentId>,
    requestor: ImportRequestor,
) -> anyhow::Result<(TableMapping, u64)> {
    let mut generated_schemas: BTreeMap<_, _> = import
        .generated_schemas
        .into_iter()
        .map(|(component_path, table_name, generated_schema)| {
            ((component_path, table_name), generated_schema)
        })
        .collect();
    let mut total_num_documents = 0;

    // First make sure all components exist, and find their IDs.
    let tables: Vec<(
        ComponentPath,
        ComponentId,
        TableName,
        Peekable<ImportDocumentStream>,
    )> = stream::iter(import.documents)
        .then(async |(component_path, mut table_name, objects)| {
            let component_id = prepare_component_for_import(database, &component_path).await?;
            // Remap the storage table; this is a bit hacky
            if table_name == FILE_STORAGE_VIRTUAL_TABLE {
                table_name = FILE_STORAGE_TABLE.clone();
            }
            anyhow::Ok((component_path, component_id, table_name, objects.peekable()))
        })
        .try_collect()
        .await?;

    // In ReplaceAll mode, we want to delete all unaffected user tables.
    // If there's a schema, then we want to clear it instead. Capture this
    // mapping after component preparation so it includes the system tables
    // initialized for newly created component namespaces.
    let db_snapshot = database.latest_snapshot()?;
    let original_table_mapping = db_snapshot.table_mapping();

    let (tables_tables, mut tables) = tables
        .into_iter()
        .partition::<Vec<_>, _>(|(_, _, table_name, _)| *table_name == TABLES_TABLE);

    database
        .runtime()
        .pause_client()
        .wait("before_assign_table_numbers")
        .await;

    // Assign table numbers based on the `_tables` tables and peeking at the
    // imported objects.
    let table_name_to_number = assign_table_numbers(
        database,
        &mode,
        tables_tables,
        &mut tables,
        original_table_mapping,
        initial_schemas,
    )
    .await?;

    // Now prepare all imported tables using the requested table numbers.
    // This creates empty hidden tables or resumes from a checkpoint.
    let mut table_mapping_in_import = TableMapping::new();
    let mut tablet_id_to_num_to_skip: BTreeMap<TabletId, u64> = BTreeMap::new();
    for (&(component_id, ref table_name), &table_number) in &table_name_to_number {
        let (table_id, num_to_skip) = prepare_table_for_import(
            database,
            &identity,
            mode,
            component_id,
            table_name,
            table_number,
            import_id,
        )
        .await?;
        table_mapping_in_import.insert(
            table_id.tablet_id,
            component_id.into(),
            table_id.table_number,
            table_name.clone(),
        );
        anyhow::ensure!(tablet_id_to_num_to_skip
            .insert(table_id.tablet_id, num_to_skip)
            .is_none());
    }
    validate_prepared_table_numbers(database, mode, &table_mapping_in_import).await?;

    let table_mapping_for_schema = {
        let mut mapping = TableMapping::new();
        for (tablet_id, namespace, table_number, table_name) in original_table_mapping.iter() {
            let component_id = ComponentId::from(namespace);
            // In ReplaceAll mode, we delete all non-system tables,
            // and in all cases tables in `table_mapping_in_import` overwrite
            // the original mapping.
            let will_delete = (matches!(mode, ImportMode::ReplaceAll) && !table_name.is_system())
                || table_name_to_number
                    .contains_key(&(&component_id, table_name) as &dyn TupleKey<_, _>);
            if !will_delete {
                mapping.insert(tablet_id, namespace, table_number, table_name.clone());
            }
        }
        for (table_id, namespace, table_number, table_name) in table_mapping_in_import.iter() {
            mapping.insert(table_id, namespace, table_number, table_name.clone());
        }
        mapping
    };

    let mut storage_files_by_component: BTreeMap<
        ComponentPath,
        Vec<(DeveloperDocumentId, ImportStorageFileStream)>,
    > = BTreeMap::new();
    let mut storage_files = import.storage_files;
    while let Some((component_path, id, stream)) = storage_files.try_next().await? {
        storage_files_by_component
            .entry(component_path)
            .or_default()
            .push((id, stream));
    }

    for (component_path, component_id, table_name, document_stream) in tables {
        let generated_schema =
            generated_schemas.get_mut(&(&component_path, &table_name) as &dyn TupleKey<_, _>);
        let table_id = table_mapping_in_import
            .namespace(component_id.into())
            .id(&table_name)?;
        total_num_documents += import_single_table(
            database,
            file_storage,
            &identity,
            &component_path,
            &table_name,
            document_stream,
            &mut storage_files_by_component,
            generated_schema,
            &table_mapping_for_schema,
            table_id,
            *tablet_id_to_num_to_skip
                .get(&table_id.tablet_id)
                .context("missing entry in tablet_id_to_num_to_skip")?,
            usage.clone(),
            import_id,
            requestor.clone(),
        )
        .await?;
    }
    if let Some((component_path, storage_files)) = storage_files_by_component.iter().next() {
        anyhow::bail!(ErrorMetadata::bad_request(
            "UnexpectedStorageFiles",
            format!(
                "snapshot import contains {} _storage file entries{} but no matching \
                 _storage/documents.jsonl table",
                storage_files.len(),
                component_path.in_component_str(),
            ),
        ));
    }

    Ok((table_mapping_in_import, total_num_documents))
}

/// Verifies the realized table numbers can coexist after finalization.
///
/// [`assign_table_numbers`] leaves fresh allocations as `None`, so preparation
/// can choose a fresh number that conflicts with one explicitly assigned to
/// another imported table. The active mapping can also change after the
/// snapshot used for assignment is captured. This check catches both cases
/// before importing rows; finalization remains authoritative for later races.
async fn validate_prepared_table_numbers<RT: Runtime>(
    database: &Database<RT>,
    mode: ImportMode,
    imported_tables: &TableMapping,
) -> anyhow::Result<()> {
    #[derive(Default)]
    struct Candidates {
        prepared: BTreeSet<TableName>,
        surviving: BTreeSet<TableName>,
    }

    let mut tx = database.begin_system().await?;
    let current_table_mapping = tx.table_mapping().clone();
    let mut candidates = BTreeMap::<(TableNamespace, TableNumber), Candidates>::new();
    for (_, namespace, table_number, table_name) in imported_tables.iter() {
        candidates
            .entry((namespace, table_number))
            .or_default()
            .prepared
            .insert(table_name.clone());
    }

    // Include active tables that finalization will neither replace nor delete.
    for (tablet_id, namespace, table_number, table_name) in current_table_mapping.iter() {
        if !current_table_mapping.is_active(tablet_id)
            || imported_tables.tablet_id_exists(tablet_id)
        {
            continue;
        }
        let replaced_by_import = imported_tables.namespace(namespace).name_exists(table_name);
        let deleted_by_replace_all = matches!(mode, ImportMode::ReplaceAll)
            && !table_name.is_system()
            && tx.get_component_path(namespace.into()).is_some();
        if !replaced_by_import && !deleted_by_replace_all {
            candidates
                .entry((namespace, table_number))
                .or_default()
                .surviving
                .insert(table_name.clone());
        }
    }

    for ((namespace, number), candidates) in candidates {
        let mut prepared = candidates.prepared.into_iter();
        let Some(first_prepared) = prepared.next() else {
            continue;
        };
        if let Some(second_prepared) = prepared.next() {
            let component_path =
                BootstrapComponentsModel::new(&mut tx).get_component_path(namespace.into());
            anyhow::bail!(ErrorMetadata::bad_request(
                "TableNumberConflict",
                format!(
                    "conflict between `{first_prepared}` and `{second_prepared}`{} with table \
                     number {number}",
                    component_path.unwrap_or_default().in_component_str(),
                )
            ));
        }
        if let Some(existing) = candidates.surviving.into_iter().next() {
            let component_path =
                BootstrapComponentsModel::new(&mut tx).get_component_path(namespace.into());
            anyhow::bail!(TableModel::<RT>::table_conflict_error(
                tx.virtual_system_mapping(),
                &component_path.unwrap_or_default(),
                &first_prepared,
                &existing,
            ));
        }
    }
    Ok(())
}

struct TableMappingForImport {
    table_mapping_in_import: TableMapping,
    to_delete: BTreeMap<TabletId, (TableNamespace, TableNumber, TableName)>,
}

impl TableMappingForImport {
    fn tables_imported(&self) -> BTreeSet<(TableNamespace, TableName)> {
        self.table_mapping_in_import
            .iter()
            .map(|(_, namespace, _, table_name)| (namespace, table_name.clone()))
            .collect()
    }

    fn tables_deleted(&self) -> BTreeSet<(TableNamespace, TableName)> {
        self.to_delete
            .values()
            .filter(|(namespace, _table_number, table_name)| {
                !self
                    .table_mapping_in_import
                    .namespace(*namespace)
                    .name_exists(table_name)
            })
            .map(|(namespace, _table_number, table_name)| (*namespace, table_name.clone()))
            .collect()
    }
}

enum AuditLogInfo {
    ClearTables,
    SnapshotImport { import_format: ImportFormat },
}

enum FinalizeImportGuard {
    None,
    InProgress {
        import_id: DeveloperDocumentId,
        completed_num_rows_written: u64,
    },
    FailedRepair {
        import_id: DeveloperDocumentId,
        expected_failed_import: SnapshotImport,
        active_table_guards: Vec<RepairActiveTableGuard>,
        checkpoint_tablet_guards: Vec<RepairCheckpointTabletGuard>,
        completed_num_rows_written: u64,
    },
}

impl FinalizeImportGuard {
    fn import_id(&self) -> Option<DeveloperDocumentId> {
        match self {
            Self::None => None,
            Self::InProgress {
                import_id,
                completed_num_rows_written: _,
            }
            | Self::FailedRepair {
                import_id,
                expected_failed_import: _,
                active_table_guards: _,
                checkpoint_tablet_guards: _,
                completed_num_rows_written: _,
            } => Some(*import_id),
        }
    }
}

async fn finalize_import<RT: Runtime>(
    database: &Database<RT>,
    identity: Identity,
    member_id_override: Option<MemberId>,
    request_metadata: RequestMetadata,
    initial_schemas: SchemasForImport,
    mode: ImportMode,
    imported_tables: TableMapping,
    audit_log_info: AuditLogInfo,
    import_guard: FinalizeImportGuard,
    requestor: ImportRequestor,
    usage: FunctionUsageTracker,
) -> anyhow::Result<(Timestamp, u64)> {
    // Ensure that schemas will be valid after the tables are activated.
    let schema_constraints =
        ImportSchemaConstraints::new(&imported_tables, initial_schemas.clone());

    // If we inserted into an existing table, we're done because the table is
    // now populated and active.
    // If we inserted into an Hidden table, make it Active.
    let (ts, documents_deleted, _occ_stats) = database
        .execute_with_overloaded_retries(identity, usage, "snapshot_import_finalize", |tx| {
            async {
                match &import_guard {
                    FinalizeImportGuard::None => {},
                    FinalizeImportGuard::InProgress {
                        import_id,
                        completed_num_rows_written: _,
                    } => {
                        // Only finalize the import if it's in progress.
                        let mut snapshot_import_model = SnapshotImportModel::new(tx);
                        let snapshot_import_state = snapshot_import_model
                            .must_get(*import_id)
                            .await?
                            .into_value()
                            .state;
                        match snapshot_import_state {
                            ImportState::InProgress { .. } => {},
                            // This can happen if the import was canceled or somehow retried after
                            // completion. These errors won't show up to
                            // the user because they are already terminal states,
                            // so we won't transition to a new state due to this error.
                            ImportState::Failed(e) => anyhow::bail!("Import failed: {e}"),
                            ImportState::Completed { .. } => {
                                anyhow::bail!("Import already completed")
                            },
                            // Indicates a bug -- we shouldn't be finalizing an import that hasn't
                            // started yet.
                            ImportState::Uploaded | ImportState::WaitingForConfirmation { .. } => {
                                anyhow::bail!("Import is not in progress")
                            },
                        }
                    },
                    FinalizeImportGuard::FailedRepair {
                        import_id,
                        expected_failed_import,
                        active_table_guards,
                        checkpoint_tablet_guards,
                        completed_num_rows_written: _,
                    } => {
                        // The repair plan is validated before this transaction. Recheck the
                        // terminal import state, selected hidden tablets, active-table set, and
                        // active table counts here so a stale plan cannot activate tablets after
                        // another operator action changed the import target.
                        let current_import = SnapshotImportModel::new(tx)
                            .get(*import_id)
                            .await?
                            .context(ErrorMetadata::not_found(
                                "ImportNotFound",
                                "import disappeared before repair finalization",
                            ))?
                            .into_value();
                        anyhow::ensure!(
                            current_import == *expected_failed_import,
                            ErrorMetadata::bad_request(
                                "ImportRepairStateChanged",
                                "failed import metadata or checkpoints changed before repair \
                                 finalization",
                            )
                        );
                        // The full-table read also makes a concurrent import creation or state
                        // transition conflict with this activation transaction. A retry then sees
                        // the competing nonterminal import and rejects the repair.
                        ensure_no_other_nonterminal_snapshot_imports(tx, *import_id).await?;
                        for active_table_guard in active_table_guards {
                            validate_repair_active_table_guard(tx, active_table_guard).await?;
                        }
                        for checkpoint_tablet_guard in checkpoint_tablet_guards {
                            validate_repair_checkpoint_tablet_guard(tx, checkpoint_tablet_guard)
                                .await?;
                        }
                        validate_repair_table_number_conflicts(tx, &imported_tables)?;
                        validate_repair_checkpoint_indexes(
                            tx,
                            &imported_tables,
                            checkpoint_tablet_guards,
                        )
                        .await?;
                        let active_table_guard_names = active_table_guards
                            .iter()
                            .map(|guard| (guard.namespace, guard.physical_table_name.clone()))
                            .collect::<BTreeSet<_>>();
                        let active_user_tables = tx
                            .table_mapping()
                            .iter_active_user_tables()
                            .map(|(_tablet_id, namespace, _table_number, table_name)| {
                                (namespace, table_name.clone())
                            })
                            .collect::<Vec<_>>();
                        for (namespace, table_name) in active_user_tables {
                            if tx.get_component_path(namespace.into()).is_none() {
                                continue;
                            }
                            anyhow::ensure!(
                                active_table_guard_names.contains(&(namespace, table_name.clone())),
                                ErrorMetadata::bad_request(
                                    "ImportCheckpointMissingActiveTable",
                                    format!(
                                        "active table {table_name} in namespace {namespace:?} is \
                                         missing from the failed import checkpoints",
                                    ),
                                )
                            );
                        }

                        // Scan at this transaction's repeatable timestamp. The tablet row-count
                        // checks above depend on each full by-id index, so a concurrent hidden-row
                        // mutation forces an OCC retry and a new scan before activation.
                        let current_table_mapping = tx.table_mapping().clone();
                        let table_mapping_for_schema = repair_table_mapping_for_schema(
                            &current_table_mapping,
                            &imported_tables,
                        );
                        let checkpoint_by_id_indexes = IndexModel::new(tx).by_id_indexes().await?;
                        let virtual_system_mapping = tx.virtual_system_mapping().clone();
                        validate_repair_checkpoint_documents(
                            database,
                            tx.begin_timestamp(),
                            checkpoint_tablet_guards,
                            &initial_schemas,
                            &table_mapping_for_schema,
                            &virtual_system_mapping,
                            &checkpoint_by_id_indexes,
                        )
                        .await?;
                    },
                }
                let import_id = import_guard.import_id();

                let to_delete = match mode {
                    ImportMode::Append | ImportMode::Replace | ImportMode::RequireEmpty => {
                        BTreeMap::new()
                    },
                    ImportMode::ReplaceAll => {
                        let existing_tables = tx.table_mapping().clone();
                        existing_tables
                            .iter_active_user_tables()
                            .filter(|&(_tablet_id, namespace, _table_number, table_name)| {
                                // Avoid deleting componentless namespaces (created during
                                // start_push).
                                if tx.get_component_path(namespace.into()).is_none() {
                                    return false;
                                }
                                // If it was written by the import, don't clear it or delete it.
                                !imported_tables.namespace(namespace).name_exists(table_name)
                            })
                            .map(|(tablet_id, namespace, table_number, table_name)| {
                                (tablet_id, (namespace, table_number, table_name.clone()))
                            })
                            .collect()
                    },
                };
                let table_mapping_for_import = TableMappingForImport {
                    table_mapping_in_import: imported_tables.clone(),
                    to_delete,
                };

                let audit_log_event = match &audit_log_info {
                    AuditLogInfo::ClearTables => DeploymentAuditLogEvent::ClearTables,
                    AuditLogInfo::SnapshotImport { import_format } => {
                        make_audit_log_event(
                            tx,
                            &table_mapping_for_import,
                            mode,
                            import_format.clone(),
                            requestor.clone(),
                        )
                        .await?
                    },
                };

                let mut documents_deleted = 0;
                for tablet_id in table_mapping_for_import.to_delete.keys() {
                    let namespace = tx.table_mapping().tablet_namespace(*tablet_id)?;
                    let table_name = tx.table_mapping().tablet_name(*tablet_id)?;
                    let mut table_model = TableModel::new(tx);
                    documents_deleted += table_model
                        .count(namespace, &table_name)
                        .await?
                        .unwrap_or(0);
                    tracing::info!(
                        "finalize_import({import_id:?}) Deleting table {table_name} in namespace \
                         {namespace:?}"
                    );
                    table_model
                        .delete_active_table(namespace, table_name)
                        .await?;
                }
                schema_constraints.validate(tx).await?;
                let mut table_model = TableModel::new(tx);
                documents_deleted += assert_send(table_model.activate_tables(
                    table_mapping_for_import.table_mapping_in_import.iter().map(
                        |(tablet_id, namespace, _table_number, table_name)| {
                            tracing::info!(
                                "finalize_import({import_id:?}) Activating table {table_name} in \
                                 namespace {namespace:?}"
                            );
                            tablet_id
                        },
                    ),
                ))
                .await?;
                DeploymentAuditLogModel::new(tx)
                    .insert_with_member_override(
                        vec![audit_log_event],
                        member_id_override,
                        &request_metadata,
                    )
                    .await?;
                let completion_ts = *tx.begin_timestamp();
                match &import_guard {
                    FinalizeImportGuard::None => {},
                    FinalizeImportGuard::InProgress {
                        import_id,
                        completed_num_rows_written,
                    } => {
                        SnapshotImportModel::new(tx)
                            .complete_import(*import_id, completion_ts, *completed_num_rows_written)
                            .await?;
                    },
                    FinalizeImportGuard::FailedRepair {
                        import_id,
                        completed_num_rows_written,
                        ..
                    } => {
                        SnapshotImportModel::new(tx)
                            .complete_failed_import_from_checkpoints(
                                *import_id,
                                completion_ts,
                                *completed_num_rows_written,
                            )
                            .await?;
                    },
                }

                Ok(documents_deleted)
            }
            .into()
        })
        .await?;

    Ok((ts, documents_deleted))
}

/// Assign table numbers. There are numerous constraints:
/// - table numbers must not conflict after the import is finalized
/// - table numbers encoded in _id fields should match their tables
/// - schema validation for v.id() columns must pass with the final table
///   numbers
async fn assign_table_numbers<RT: Runtime>(
    database: &Database<RT>,
    mode: &ImportMode,
    tables_tables: Vec<(
        ComponentPath,
        ComponentId,
        TableName,
        Peekable<ImportDocumentStream>,
    )>,
    tables: &mut Vec<(
        ComponentPath,
        ComponentId,
        TableName,
        Peekable<ImportDocumentStream>,
    )>,
    original_table_mapping: &TableMapping,
    initial_schemas: &SchemasForImport,
) -> anyhow::Result<BTreeMap<(ComponentId, TableName), Option<TableNumber>>> {
    let mut table_name_to_number: BTreeMap<(ComponentId, TableName), Option<TableNumber>> =
        BTreeMap::new(); // None here means that we'll pick any number
    let mut table_number_to_name: BTreeMap<(ComponentId, TableNumber), TableName> = BTreeMap::new();
    let mut assign_number = |component_path: &ComponentPath,
                             component_id: ComponentId,
                             table_name: TableName,
                             table_number: TableNumber| {
        match table_number_to_name.entry((component_id, table_number)) {
            Entry::Vacant(v) => v.insert(table_name),
            Entry::Occupied(o) => anyhow::bail!(ErrorMetadata::bad_request(
                "InvalidId",
                format!(
                    "conflict between `{}` and `{}`{} with number {table_number}",
                    o.get(),
                    table_name,
                    component_path.in_component_str(),
                )
            )),
        };
        anyhow::Ok(())
    };

    // Step 1: Read _tables if present in the import. If we're importing an
    // untouched snapshot export this will assign every table a proper number.
    for (component_path, component_id, _, objects) in tables_tables {
        let mut stream = parse_tables_table(objects);
        while let Some((table_name, table_number)) = stream.try_next().await? {
            anyhow::ensure!(
                table_name_to_number
                    .insert((component_id, table_name.clone()), Some(table_number))
                    .is_none(),
                ErrorMetadata::bad_request(
                    "DuplicateTableName",
                    format!(
                        "`_tables` contains duplicate entries for `{table_name}`{}",
                        component_path.in_component_str()
                    )
                )
            );
            assign_number(&component_path, component_id, table_name, table_number)?;
        }
    }

    // Step 2: For tables that aren't listed in `_tables`, read their first
    // object's _id field (if present) to infer a table number to assign that
    // table.
    for (component_path, component_id, table_name, objects) in tables.iter_mut() {
        let Entry::Vacant(v) = table_name_to_number.entry((*component_id, table_name.clone()))
        else {
            continue;
        };
        if let Some(table_number) = table_number_for_import(objects).await {
            v.insert(Some(table_number));
            // If this creates a table number conflict with step 1, raise an
            // error right away, since the documents will otherwise fail to
            // insert.
            assign_number(
                component_path,
                *component_id,
                table_name.clone(),
                table_number,
            )?;
        }
    }

    // Step 3: For tables that don't have _id fields, reuse their table number
    // from the current table mapping, if possible.
    // Otherwise, they will get a fresh table number (`None` in the map).
    let mut assign_existing_table_number = |component_id: ComponentId, table_name: &TableName| {
        if let Entry::Vacant(v) = table_name_to_number.entry((component_id, table_name.clone())) {
            let number = if let Some(existing) = original_table_mapping
                .namespace(component_id.into())
                .id_and_number_if_exists(table_name)
                && let Entry::Vacant(v2) =
                    table_number_to_name.entry((component_id, existing.table_number))
            {
                v2.insert(table_name.clone());
                Some(existing.table_number)
            } else {
                None
            };
            v.insert(number);
        }
    };
    for (_, component_id, table_name, _) in tables.iter() {
        assign_existing_table_number(*component_id, table_name);
    }

    // In ReplaceAll mode, any tables that aren't imported are going to be
    // deleted during finalization. For tables in the schema, we should replace
    // them with empty tables instead.
    // TODO: this is racy, as the schemas could change before finalization.
    if let ImportMode::ReplaceAll = mode {
        for (namespace, schema_state, (_, schema)) in initial_schemas {
            let component_id = ComponentId::from(*namespace);
            if let SchemaState::Active = schema_state {
                for table_name in schema.tables.keys() {
                    assign_existing_table_number(component_id, table_name);
                }
            }
        }
    } else {
        // Proactively check for table number conflicts before inserting any
        // objects. This check is not authoritative, as the table mapping may
        // change again before the import is finalized.
        //
        // This isn't relevant for ReplaceAll mode since all original tables
        // will be deleted.
        for (tablet_id, namespace, table_number, table_name) in original_table_mapping.iter() {
            let component_id = ComponentId::from(namespace);
            if original_table_mapping.is_active(tablet_id)
                && !table_name_to_number
                    .contains_key(&(&component_id, table_name) as &dyn TupleKey<_, _>)
                && let Some(imported_table_name) =
                    table_number_to_name.get(&(component_id, table_number))
            {
                let mut tx = database.begin_system().await?;
                let component_path =
                    BootstrapComponentsModel::new(&mut tx).get_component_path(component_id);
                anyhow::bail!(TableModel::<RT>::table_conflict_error(
                    tx.virtual_system_mapping(),
                    &component_path.unwrap_or_default(),
                    imported_table_name,
                    table_name
                ));
            }
        }
    }
    Ok(table_name_to_number)
}

fn parse_tables_table(
    objects: impl Stream<Item = anyhow::Result<JsonValue>> + Unpin,
) -> impl Stream<Item = anyhow::Result<(TableName, TableNumber)>> + Unpin {
    let mut lineno = 0;
    objects.map(move |r| {
        let exported_value = r?;
        lineno += 1;
        let exported_object = exported_value
            .as_object()
            .with_context(|| ImportError::NotAnObject(lineno))?;
        let table_name = exported_object
            .get("name")
            .and_then(|name| name.as_str())
            .with_context(|| {
                ImportError::InvalidConvexValue(lineno, anyhow::anyhow!("table requires name"))
            })?;
        let table_name = table_name
            .parse()
            .map_err(|e| ImportError::InvalidName(table_name.to_string(), e))?;
        let table_number = exported_object
            .get("id")
            .and_then(|id| id.as_f64())
            .and_then(|id| TableNumber::try_from(id as u32).ok())
            .with_context(|| {
                ImportError::InvalidConvexValue(
                    lineno,
                    anyhow::anyhow!(
                        "table requires id (received {:?})",
                        exported_object.get("id")
                    ),
                )
            })?;
        Ok((table_name, table_number))
    })
}

async fn import_single_table<RT: Runtime>(
    database: &Database<RT>,
    file_storage: &FileStorage<RT>,
    identity: &Identity,
    component_path: &ComponentPath,
    table_name: &TableName,
    mut objects: Peekable<ImportDocumentStream>,
    storage_files_by_component: &mut BTreeMap<
        ComponentPath,
        Vec<(DeveloperDocumentId, ImportStorageFileStream)>,
    >,
    mut generated_schema: Option<&mut GeneratedSchema<ProdConfig>>,
    table_mapping_for_schema: &TableMapping,
    table_id: TabletIdAndTableNumber,
    num_to_skip: u64,
    usage: FunctionUsageTracker,
    import_id: Option<DeveloperDocumentId>,
    requestor: ImportRequestor,
) -> anyhow::Result<u64> {
    if let Some(import_id) = import_id {
        best_effort_update_progress_message(
            database,
            identity,
            import_id,
            format!(
                "Importing \"{}\"{}",
                table_name,
                component_path.in_component_str()
            ),
            component_path,
            table_name,
            0,
        )
        .await;
    }

    anyhow::ensure!(*table_name != TABLES_TABLE);

    if *table_name == FILE_STORAGE_TABLE {
        let storage_files = storage_files_by_component
            .remove(component_path)
            .unwrap_or_default();
        import_storage_table(
            database,
            file_storage,
            identity,
            table_id,
            component_path,
            objects,
            storage_files,
            &usage,
            import_id,
            num_to_skip,
            requestor,
            table_mapping_for_schema,
        )
        .await?;
        return Ok(0);
    }

    let mut num_objects = 0;

    let mut objects_to_insert = vec![];
    let mut objects_to_insert_size = 0;
    while let Some(exported_value) = objects.try_next().await? {
        if num_objects < num_to_skip {
            num_objects += 1;
            continue;
        }
        let row_number = num_objects + 1;
        let convex_value =
            GeneratedSchema::<ProdConfig>::apply(generated_schema.as_deref_mut(), exported_value)
                .map_err(|e| ImportError::InvalidConvexValue(row_number, e))?;
        let ConvexValue::Object(convex_object) = convex_value else {
            anyhow::bail!(ImportError::NotAnObject(row_number));
        };
        objects_to_insert_size += convex_object.size();
        objects_to_insert.push(convex_object);

        if objects_to_insert_size > *TRANSACTION_MAX_USER_WRITE_SIZE_BYTES / 2
            || objects_to_insert.len() > *TRANSACTION_MAX_NUM_USER_WRITES / 2
        {
            insert_import_objects(
                database,
                identity,
                objects_to_insert,
                table_name,
                table_id,
                table_mapping_for_schema,
                usage.clone(),
            )
            .await?;
            objects_to_insert = Vec::new();
            objects_to_insert_size = 0;
            if let Some(import_id) = import_id {
                best_effort_update_progress_message(
                    database,
                    identity,
                    import_id,
                    format!(
                        "Importing \"{table_name}\" ({} documents)",
                        num_objects.separate_with_commas()
                    ),
                    component_path,
                    table_name,
                    num_objects as i64,
                )
                .await;
            }
        }
        num_objects += 1;
    }
    anyhow::ensure!(
        num_to_skip <= num_objects,
        ErrorMetadata::bad_request(
            "InvalidImportCheckpoint",
            format!(
                "checkpoint for \"{table_name}\"{} has {num_to_skip} existing rows, but import \
                 contains {num_objects} documents",
                component_path.in_component_str(),
            ),
        )
    );

    insert_import_objects(
        database,
        identity,
        objects_to_insert,
        table_name,
        table_id,
        table_mapping_for_schema,
        usage,
    )
    .await?;

    if let Some(import_id) = import_id {
        add_checkpoint_message(
            database,
            identity,
            import_id,
            format!(
                "Imported \"{table_name}\"{} ({} documents)",
                component_path.in_component_str(),
                num_objects.separate_with_commas()
            ),
            component_path,
            table_name,
            num_objects as i64,
        )
        .await?;
    }

    Ok(num_objects)
}

async fn insert_import_objects<RT: Runtime>(
    database: &Database<RT>,
    identity: &Identity,
    objects_to_insert: Vec<ConvexObject>,
    table_name: &TableName,
    table_id: TabletIdAndTableNumber,
    table_mapping_for_schema: &TableMapping,
    usage: FunctionUsageTracker,
) -> anyhow::Result<()> {
    if objects_to_insert.is_empty() {
        return Ok(());
    }
    let object_ids: Vec<_> = objects_to_insert
        .iter()
        .filter_map(|object| object.get(&*ID_FIELD))
        .collect();
    let object_ids_dedup: BTreeSet<_> = object_ids.iter().collect();
    if object_ids_dedup.len() < object_ids.len() {
        anyhow::bail!(ErrorMetadata::bad_request(
            "DuplicateId",
            format!("Objects in table \"{table_name}\" have duplicate _id fields")
        ));
    }
    database
        .execute_with_overloaded_and_ratelimited_retries(
            identity.clone(),
            usage,
            "snapshot_import_insert_objects",
            |tx| {
                async {
                    for object_to_insert in objects_to_insert.clone() {
                        ImportFacingModel::new(tx)
                            .insert(
                                table_id,
                                table_name,
                                object_to_insert,
                                table_mapping_for_schema,
                            )
                            .await?;
                    }
                    Ok(())
                }
                .into()
            },
        )
        .await?;
    Ok(())
}

async fn prepare_table_for_import<RT: Runtime>(
    database: &Database<RT>,
    identity: &Identity,
    mode: ImportMode,
    component_id: ComponentId,
    table_name: &TableName,
    table_number: Option<TableNumber>,
    import_id: Option<DeveloperDocumentId>,
) -> anyhow::Result<(TabletIdAndTableNumber, u64)> {
    anyhow::ensure!(
        table_name == &FILE_STORAGE_TABLE || !table_name.is_system(),
        ErrorMetadata::bad_request(
            "InvalidTableName",
            format!("Invalid table name {table_name} starts with metadata prefix '_'")
        )
    );
    let display_table_name = if table_name == &FILE_STORAGE_TABLE {
        &FILE_STORAGE_VIRTUAL_TABLE
    } else {
        table_name
    };
    let mut tx = database.begin(identity.clone()).await?;
    let component_path = BootstrapComponentsModel::new(&mut tx)
        .get_component_path(component_id)
        .context("component disappeared")?;
    let existing_checkpoint = match import_id {
        Some(import_id) => {
            SnapshotImportModel::new(&mut tx)
                .get_table_checkpoint(import_id, &component_path, display_table_name)
                .await?
        },
        None => None,
    };
    let existing_checkpoint_tablet = existing_checkpoint
        .as_ref()
        .and_then(|checkpoint| checkpoint.tablet_id);
    let (insert_into_existing_table_id, num_to_skip) = match existing_checkpoint_tablet {
        Some(tablet_id) => {
            if let ImportMode::Append = mode {
                // TODO: resuming an append from checkpoint isn't possible
                // without a data model change (writing a cursor transactionally
                // with the written documents)
                anyhow::bail!("can't resume append import");
            }
            let existing_table_number = tx.table_mapping().tablet_number(tablet_id)?;
            let num_to_skip = TableModel::new(&mut tx)
                .must_count_tablet(tablet_id)
                .await?;
            (
                Some(TabletIdAndTableNumber {
                    tablet_id,
                    table_number: existing_table_number,
                }),
                num_to_skip,
            )
        },
        None => {
            let tablet_id = match mode {
                ImportMode::Append => tx
                    .table_mapping()
                    .namespace(component_id.into())
                    .id_and_number_if_exists(table_name),
                ImportMode::RequireEmpty => {
                    if TableModel::new(&mut tx)
                        .must_count(component_id.into(), table_name)
                        .await?
                        != 0
                    {
                        anyhow::bail!(ImportError::TableExists(table_name.clone()));
                    }
                    None
                },
                ImportMode::Replace | ImportMode::ReplaceAll => None,
            };
            (tablet_id, 0)
        },
    };
    drop(tx);
    let table_id = if let Some(insert_into_existing_table_id) = insert_into_existing_table_id {
        insert_into_existing_table_id
    } else {
        create_empty_table(
            database,
            identity,
            component_id,
            table_name,
            table_number,
            import_id,
            display_table_name,
            &component_path,
        )
        .await?
    };
    if let Some(requested_table_number) = table_number {
        // This should only happen for ImportMode::Append
        anyhow::ensure!(
            requested_table_number == table_id.table_number,
            ErrorMetadata::bad_request(
                "TableNumberConflict",
                format!(
                    "table {table_name}{component} wants table number {requested_table_number} \
                     but was already assigned {actual_table_number}",
                    component = component_path.in_component_str(),
                    actual_table_number = table_id.table_number,
                )
            )
        );
    }
    Ok((table_id, num_to_skip))
}

async fn create_empty_table<RT: Runtime>(
    database: &Database<RT>,
    identity: &Identity,
    component_id: ComponentId,
    table_name: &TableName,
    table_number: Option<TableNumber>,
    import_id: Option<DeveloperDocumentId>,
    display_table_name: &TableName,
    component_path: &ComponentPath,
) -> anyhow::Result<TabletIdAndTableNumber> {
    let (_, table_id, _) = database
        .execute_with_overloaded_retries(
            identity.clone(),
            FunctionUsageTracker::new(),
            "snapshot_import_prepare_table",
            |tx| {
                async {
                    // Create a new table in state Hidden, that will later be changed to Active.
                    let table_id = TableModel::new(tx)
                        .insert_table_for_import(component_id.into(), table_name, table_number)
                        .await?;
                    IndexModel::new(tx)
                        .copy_indexes_to_table(component_id.into(), table_name, table_id.tablet_id)
                        .await?;
                    if let Some(import_id) = import_id {
                        SnapshotImportModel::new(tx)
                            .checkpoint_tablet_created(
                                import_id,
                                component_path,
                                display_table_name,
                                table_id.tablet_id,
                            )
                            .await?;
                    }
                    Ok(table_id)
                }
                .into()
            },
        )
        .await?;
    backfill_and_enable_indexes_on_table(database, identity, table_id.tablet_id).await?;
    Ok(table_id)
}

/// Waits for all indexes on a table to be backfilled, which may take a while
/// for large tables. After the indexes are backfilled, enable them.
async fn backfill_and_enable_indexes_on_table<RT: Runtime>(
    database: &Database<RT>,
    identity: &Identity,
    tablet_id: TabletId,
) -> anyhow::Result<()> {
    loop {
        let mut tx = database.begin(identity.clone()).await?;
        let still_backfilling = IndexModel::new(&mut tx)
            .all_indexes_on_table(tablet_id)
            .await?
            .into_iter()
            .any(|index| index.config.is_backfilling());
        if !still_backfilling {
            break;
        }
        let token = tx.into_token()?;
        database.subscribe_and_wait_for_invalidation(token).await?;
    }
    // Enable the indexes now that they are backfilled.
    database
        .execute_with_overloaded_retries(
            identity.clone(),
            FunctionUsageTracker::new(),
            "snapshot_import_enable_indexes",
            |tx| {
                async {
                    let mut index_model = IndexModel::new(tx);
                    let mut backfilled_indexes = vec![];
                    for index in index_model.all_indexes_on_table(tablet_id).await? {
                        if !index.config.is_enabled() {
                            backfilled_indexes.push(index);
                        }
                    }
                    index_model
                        .enable_backfilled_indexes(backfilled_indexes)
                        .await?;
                    Ok(())
                }
                .into()
            },
        )
        .await?;
    Ok(())
}

async fn table_number_for_import(
    objects: &mut Peekable<ImportDocumentStream>,
) -> Option<TableNumber> {
    let first_object = Pin::new(objects).peek().await?.as_ref().ok()?;
    let object = first_object.as_object()?;
    let first_id = object.get(&*ID_FIELD)?;
    let JsonValue::String(id) = first_id else {
        return None;
    };
    let id_v6 = DeveloperDocumentId::decode(id).ok()?;
    Some(id_v6.table())
}

async fn remap_empty_string_by_schema<RT: Runtime>(
    namespace: TableNamespace,
    table_name: TableName,
    tx: &mut Transaction<RT>,
    mut import: ParsedImport,
) -> anyhow::Result<ParsedImport> {
    if let Some((_, schema)) = SchemaModel::new(tx, namespace)
        .get_by_state(SchemaState::Active)
        .await?
    {
        let document_schema = match schema
            .tables
            .get(&table_name)
            .and_then(|table_schema| table_schema.document_type.clone())
        {
            None => return Ok(import),
            Some(document_schema) => document_schema,
        };
        let optional_fields = document_schema.optional_top_level_fields();
        if optional_fields.is_empty() {
            return Ok(import);
        }

        import.documents = import
            .documents
            .into_iter()
            .map(move |(component, table, stream)| {
                let optional_fields = optional_fields.clone();
                (
                    component,
                    table,
                    stream
                        .map_ok(move |mut object| {
                            remove_empty_string_optional_entries(&optional_fields, &mut object);
                            object
                        })
                        .boxed(),
                )
            })
            .collect();
    }
    Ok(import)
}

fn remove_empty_string_optional_entries(
    optional_fields: &HashSet<IdentifierFieldName>,
    object: &mut JsonValue,
) {
    let Some(object) = object.as_object_mut() else {
        return;
    };
    object.retain(|field_name, value| {
        // Remove optional fields that have an empty string as their value.
        let Ok(identifier_field_name) = field_name.parse::<IdentifierFieldName>() else {
            return true;
        };
        if !optional_fields.contains(&identifier_field_name) {
            return true;
        }
        let &mut JsonValue::String(ref s) = value else {
            return true;
        };
        !s.is_empty()
    });
}
