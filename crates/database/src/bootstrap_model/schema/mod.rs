pub mod types;

use std::{
    sync::{
        Arc,
        LazyLock,
    },
    time::Duration,
};

use anyhow::Context;
use async_recursion::async_recursion;
use common::{
    self,
    bootstrap_model::schema::{
        SchemaMetadata,
        SchemaState,
    },
    document::{
        ParsedDocument,
        ResolvedDocument,
    },
    runtime::Runtime,
    schemas::{
        DatabaseSchema,
        SchemaValidationError,
    },
};
use errors::ErrorMetadata;
use value::{
    FieldPath,
    NamespacedTableMapping,
    ResolvedDocumentId,
    TableName,
    TableNamespace,
};

use self::types::SchemaDiff;
use crate::{
    patch_value,
    system_tables::{
        SystemIndex,
        SystemTable,
    },
    SchemaValidationProgressModel,
    SystemMetadataModel,
    TableModel,
    Transaction,
};

pub const SCHEMAS_TABLE: TableName = TableName::const_new("_schemas");

pub static SCHEMAS_STATE_INDEX: LazyLock<SystemIndex<SchemasTable>> =
    LazyLock::new(|| SystemIndex::new("by_state", [&SCHEMA_STATE_FIELD]).unwrap());

pub static SCHEMA_STATE_FIELD: LazyLock<FieldPath> =
    LazyLock::new(|| "state".parse().expect("invalid state field"));

const MAX_TIME_TO_KEEP_FAILED_AND_OVERWRITTEN_SCHEMAS: Duration = Duration::from_secs(60 * 60); // 1 hour
const MAX_SCHEMA_HISTORY_DELETIONS_PER_TRANSACTION: usize = 16;

pub struct SchemasTable;
impl SystemTable for SchemasTable {
    type Metadata = SchemaMetadata;

    const TABLE_NAME: TableName = SCHEMAS_TABLE;

    fn indexes() -> Vec<SystemIndex<Self>> {
        vec![SCHEMAS_STATE_INDEX.clone()]
    }
}

pub struct SchemaModel<'a, RT: Runtime> {
    tx: &'a mut Transaction<RT>,
    namespace: TableNamespace,
}

impl<'a, RT: Runtime> SchemaModel<'a, RT> {
    pub fn new(tx: &'a mut Transaction<RT>, namespace: TableNamespace) -> Self {
        Self { tx, namespace }
    }

    #[fastrace::trace]
    pub async fn apply(
        &mut self,
        schema_id: Option<ResolvedDocumentId>,
    ) -> anyhow::Result<(Option<SchemaDiff>, Option<DatabaseSchema>)> {
        let previous_schema = self
            .get_by_state(SchemaState::Active)
            .await?
            .map(|(_id, schema)| schema);
        let next_schema = if let Some(schema_id) = schema_id {
            Some(
                self.get_validated_or_active(schema_id)
                    .await?
                    .database_schema()?,
            )
        } else {
            None
        };
        let schema_diff: Option<SchemaDiff> = (previous_schema.as_deref() != next_schema.as_ref())
            .then_some(SchemaDiff {
                previous_schema: previous_schema.map(Arc::unwrap_or_clone),
                next_schema: next_schema.clone(),
            });
        if let Some(schema_id) = schema_id {
            self.mark_active(schema_id).await?;
        } else {
            self.clear_active().await?;
        }

        Ok((schema_diff, next_schema))
    }

    #[fastrace::trace]
    pub async fn enforce(&mut self, document: &ResolvedDocument) -> anyhow::Result<()> {
        let schema_table_mapping = self.tx.table_mapping().namespace(self.namespace);
        if schema_table_mapping.is_system_tablet(document.id().tablet_id) {
            // System tables are not subject to schema validation.
            return Ok(());
        }
        self.enforce_with_table_mapping(document, &schema_table_mapping)
            .await
    }

    pub async fn enforce_table_deletion(
        &mut self,
        active_table_to_delete: TableName,
    ) -> anyhow::Result<()> {
        // Check the combined invariant before an active-schema error can mask it.
        let in_progress_schema = self.get_in_progress_schema().await?;
        if let Some((_id, active_schema)) = self.get_by_state(SchemaState::Active).await?
            && let Err(schema_error) =
                active_schema.check_delete_table(active_table_to_delete.clone())
        {
            anyhow::bail!(schema_error.to_error_metadata());
        }
        if let Some((id, in_progress_schema, _state)) = in_progress_schema
            && let Err(enforcement_error) =
                in_progress_schema.check_delete_table(active_table_to_delete)
        {
            anyhow::ensure!(
                self.mark_failed(id, enforcement_error.into()).await?,
                "In-progress schema changed state within one enforcement transaction"
            );
        }

        Ok(())
    }

    pub async fn get_in_progress_schema(
        &mut self,
    ) -> anyhow::Result<Option<(ResolvedDocumentId, Arc<DatabaseSchema>, SchemaState)>> {
        let pending_schema = self.get_by_state(SchemaState::Pending).await?;
        let validated_schema = self.get_by_state(SchemaState::Validated).await?;
        match (pending_schema, validated_schema) {
            (None, None) => Ok(None),
            (Some((id, schema)), None) => Ok(Some((id, schema, SchemaState::Pending))),
            (None, Some((id, schema))) => Ok(Some((id, schema, SchemaState::Validated))),
            (Some(_), Some(_)) => {
                anyhow::bail!("Invalid schema state: both pending and validated schemas exist")
            },
        }
    }

    /// You probably want to use `enforce`.
    /// enforce_with_table_mapping allows schema validation to use a custom
    /// TableMapping for validating foreign references, which is useful for
    /// snapshot imports where hidden tables can have foreign references to
    /// other hidden tables in the same import.
    pub async fn enforce_with_table_mapping(
        &mut self,
        document: &ResolvedDocument,
        table_mapping_for_schema: &NamespacedTableMapping,
    ) -> anyhow::Result<()> {
        let table_name = table_mapping_for_schema.tablet_name(document.id().tablet_id)?;
        // Check the combined invariant before an active-schema error can mask it.
        let in_progress_schema = self.get_in_progress_schema().await?;
        if let Some((_id, active_schema)) = self.get_by_state(SchemaState::Active).await?
            && let Err(schema_error) = active_schema.check_new_document(
                document,
                table_name.clone(),
                table_mapping_for_schema,
                self.tx.virtual_system_mapping(),
            )
        {
            anyhow::bail!(schema_error.to_error_metadata());
        }
        if let Some((id, in_progress_schema, _state)) = in_progress_schema
            && let Err(enforcement_error) = in_progress_schema.check_new_document(
                document,
                table_name,
                table_mapping_for_schema,
                self.tx.virtual_system_mapping(),
            )
        {
            anyhow::ensure!(
                self.mark_failed(id, enforcement_error.into()).await?,
                "In-progress schema changed state within one enforcement transaction"
            );
        }

        Ok(())
    }

    async fn get_exact_schema(
        &mut self,
        document_id: ResolvedDocumentId,
    ) -> anyhow::Result<Option<Arc<ParsedDocument<SchemaMetadata>>>> {
        // A resolved ID can still read a retained inactive tablet. Resolve through
        // the active system table first so a stale lifecycle request cannot act on
        // a recreated schema-table generation.
        if !self
            .tx
            .table_mapping()
            .namespace(self.namespace)
            .name_exists(&SCHEMAS_TABLE)
        {
            return Ok(None);
        }
        Ok(self
            .tx
            .get_system::<SchemasTable>(self.namespace, document_id.developer_id)
            .await?
            .filter(|schema| schema.id() == document_id))
    }

    pub async fn get_by_state(
        &mut self,
        state: SchemaState,
    ) -> anyhow::Result<Option<(ResolvedDocumentId, Arc<DatabaseSchema>)>> {
        anyhow::ensure!(
            state.is_unique(),
            "Getting schema by state is only permitted for Pending, Validated, or Active states, \
             since Failed or Overwritten states may have multiple documents."
        );
        let Some(schema_tablet_id) = self
            .tx
            .table_mapping()
            .namespace(self.namespace)
            .id_if_exists(&SCHEMAS_TABLE)
        else {
            return Ok(None);
        };
        let schema = self.tx.get_schema_by_state(self.namespace, state)?;
        // SchemaRegistry retains unique-state entries by namespace when a system
        // table is recreated. Its read dependency uses the current tablet, but the
        // cached result can still belong to the retained inactive generation.
        Ok(schema.filter(|(id, _schema)| id.tablet_id == schema_tablet_id))
    }

    #[fastrace::trace]
    pub async fn submit_pending(
        &mut self,
        schema: DatabaseSchema,
    ) -> anyhow::Result<(ResolvedDocumentId, SchemaState)> {
        let mut table_model = TableModel::new(self.tx);
        for name in schema.tables.keys() {
            if !table_model.table_exists(self.namespace, name) {
                table_model
                    .insert_table_metadata(self.namespace, name)
                    .await?;
            }
        }
        let active_schema = self.get_by_state(SchemaState::Active).await?;
        let in_progress_schema = self.get_in_progress_schema().await?;
        if let Some((id, active_schema)) = active_schema
            && *active_schema == schema
        {
            if let Some((in_progress_id, _in_progress_schema, state)) = in_progress_schema {
                self.mark_overwritten(in_progress_id, state).await?;
                self.delete_old_failed_and_overwritten_schemas(&[in_progress_id])
                    .await?;
            }
            return Ok((id, SchemaState::Active));
        }
        if let Some((id, existing_schema, state)) = in_progress_schema {
            if *existing_schema == schema {
                return Ok((id, state));
            }
            self.mark_overwritten(id, state).await?;
            self.delete_old_failed_and_overwritten_schemas(&[id])
                .await?;
        }

        let schema_metadata = SchemaMetadata::new(SchemaState::Pending, schema)?;
        let id = SystemMetadataModel::new(self.tx, self.namespace)
            .insert(&SCHEMAS_TABLE, schema_metadata.try_into()?)
            .await?;
        Ok((id, SchemaState::Pending))
    }

    pub async fn mark_validated(&mut self, document_id: ResolvedDocumentId) -> anyhow::Result<()> {
        let doc = self
            .get_exact_schema(document_id)
            .await?
            .context("Schema to mark as validated must exist.")?;
        match &doc.state {
            SchemaState::Pending => {
                anyhow::ensure!(
                    self.get_by_state(SchemaState::Validated).await?.is_none(),
                    "Invalid schema state: both pending and validated schemas exist"
                );
                SystemMetadataModel::new(self.tx, self.namespace)
                    .patch(
                        document_id,
                        patch_value!("state" => Some(SchemaState::Validated.try_into()?))?,
                    )
                    .await?;
                tracing::info!("Marked pending schema as validated");
                Ok(())
            },
            SchemaState::Validated => Err(anyhow::anyhow!("Schema is already validated.")),
            SchemaState::Active => Err(anyhow::anyhow!("Schema is already active.")),
            SchemaState::Failed { error, .. } => Err(ErrorMetadata::bad_request(
                "SchemaAlreadyFailed",
                format!("Schema has already been failed with error: {error}"),
            )
            .into()),
            SchemaState::Overwritten => Err(ErrorMetadata::bad_request(
                "SchemaAlreadyOverwritten",
                "Schema has already been overwritten.",
            )
            .into()),
        }
    }

    pub async fn get_validated_or_active(
        &mut self,
        schema_id: ResolvedDocumentId,
    ) -> anyhow::Result<SchemaMetadata> {
        let doc = self
            .get_exact_schema(schema_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("No document found for schema ID {schema_id}"))?;
        let schema = Arc::unwrap_or_clone(doc).into_value();
        match schema.state {
            SchemaState::Pending => {
                anyhow::bail!("Expected schema to be Validated, but it's Pending {schema_id}")
            },
            SchemaState::Validated => Ok(schema),
            SchemaState::Active => Ok(schema),
            SchemaState::Failed { error, .. } => Err(ErrorMetadata::bad_request(
                "SchemaAlreadyFailed",
                format!("Schema has already been failed with error: {error}"),
            )
            .into()),
            SchemaState::Overwritten => Err(ErrorMetadata::bad_request(
                "SchemaAlreadyOverwritten",
                "Schema has already been overwritten.",
            )
            .into()),
        }
    }

    pub async fn mark_active(&mut self, document_id: ResolvedDocumentId) -> anyhow::Result<()> {
        // Make sure it's already Validated or Active.
        let schema = self.get_validated_or_active(document_id).await?;
        // Check the combined in-progress invariant before the Active no-op can
        // delete mixed-version progress or hide contradictory lifecycle state.
        let in_progress_schema = self.get_in_progress_schema().await?;
        match schema.state {
            // Already active: clean any mixed-version progress and otherwise no-op.
            SchemaState::Active => {
                SchemaValidationProgressModel::new(self.tx, self.namespace)
                    .delete_schema_validation_progress(document_id)
                    .await?;
                Ok(())
            },
            // If it's validated, mark as active.
            SchemaState::Validated => {
                anyhow::ensure!(
                    matches!(
                        in_progress_schema,
                        Some((id, _, SchemaState::Validated)) if id == document_id
                    ),
                    "Schema to mark active is not the current validated schema"
                );
                SchemaValidationProgressModel::new(self.tx, self.namespace)
                    .delete_schema_validation_progress(document_id)
                    .await?;
                self.clear_active().await?;
                SystemMetadataModel::new(self.tx, self.namespace)
                    .patch(
                        document_id,
                        patch_value!("state" => Some(SchemaState::Active.try_into()?))?,
                    )
                    .await?;
                Ok(())
            },
            SchemaState::Overwritten | SchemaState::Pending | SchemaState::Failed { .. } => {
                anyhow::bail!("expected validated or active schema")
            },
        }
    }

    #[async_recursion]
    /// Mark pending or validated schemas as failed. Error if the schema is
    /// already active. Returns whether the exact schema is failed after this
    /// call; an overwritten schema or a terminal schema removed by retention
    /// returns false.
    pub async fn mark_failed(
        &mut self,
        document_id: ResolvedDocumentId,
        error: SchemaValidationError,
    ) -> anyhow::Result<bool> {
        let Some(doc) = self.get_exact_schema(document_id).await? else {
            // A schema worker retries in a new transaction. A replacement older
            // than the retention window can be pruned between those attempts.
            // Enforcement callers use one snapshot and reject this result as an
            // internal inconsistency.
            return Ok(false);
        };
        let schema_is_failed = match doc.state.clone() {
            SchemaState::Pending | SchemaState::Validated => {
                let error_message = error.to_string();
                let table_name = match error {
                    SchemaValidationError::ExistingDocument { table_name, .. } => table_name,
                    SchemaValidationError::NewDocument { table_name, .. } => table_name,
                    SchemaValidationError::TableCannotBeDeleted { table_name } => table_name,
                    SchemaValidationError::ReferencedTableCannotBeDeleted {
                        table_name, ..
                    } => table_name,
                };
                SystemMetadataModel::new(self.tx, self.namespace)
                    .patch(
                        document_id,
                        patch_value!(
                            "state" => Some(
                                SchemaState::Failed {
                                    error: error_message,
                                    table_name: Some(table_name.to_string())
                                }.try_into()?
                            )
                        )?,
                    )
                    .await?;
                true
            },
            SchemaState::Active => {
                anyhow::bail!("Active schemas cannot be marked as failed.")
            },
            SchemaState::Failed { .. } => true,
            SchemaState::Overwritten => false,
        };
        self.delete_old_failed_and_overwritten_schemas(&[document_id])
            .await?;
        // User writes can fail a pending schema. Do not make those transactions read
        // the hot progress row: tracker checkpoints fence on this schema state and the
        // schema worker removes inactive progress after cancellation or restart.
        Ok(schema_is_failed)
    }

    pub async fn overwrite_all(&mut self) -> anyhow::Result<bool> {
        let mut schemas_to_overwrite = Vec::new();
        if let Some((id, _schema, state)) = self.get_in_progress_schema().await? {
            schemas_to_overwrite.push((id, state));
        }
        if let Some((id, _schema)) = self.get_by_state(SchemaState::Active).await? {
            schemas_to_overwrite.push((id, SchemaState::Active));
        }
        for (id, state) in &schemas_to_overwrite {
            self.mark_overwritten(*id, state.clone()).await?;
        }
        if !schemas_to_overwrite.is_empty() {
            let schemas_to_keep: Vec<_> = schemas_to_overwrite.iter().map(|(id, _)| *id).collect();
            self.delete_old_failed_and_overwritten_schemas(&schemas_to_keep)
                .await?;
        }
        Ok(!schemas_to_overwrite.is_empty())
    }

    pub async fn clear_active(&mut self) -> anyhow::Result<()> {
        // Applying no schema is still a lifecycle transition. Do not overwrite
        // the active schema while preserving contradictory in-progress state.
        self.get_in_progress_schema().await?;
        if let Some((id, _schema)) = self.get_by_state(SchemaState::Active).await? {
            self.mark_overwritten(id, SchemaState::Active).await?;
            self.delete_old_failed_and_overwritten_schemas(&[id])
                .await?;
        }
        Ok(())
    }

    /// Deletes a bounded batch of failed and overwritten schemas older than an
    /// hour. Keeps schemas table small without making a schema transition own
    /// an unbounded cleanup.
    async fn delete_old_failed_and_overwritten_schemas(
        &mut self,
        schemas_to_keep: &[ResolvedDocumentId],
    ) -> anyhow::Result<()> {
        let oldest_schema_to_keep = (*self
            .tx
            .begin_timestamp()
            .sub(MAX_TIME_TO_KEEP_FAILED_AND_OVERWRITTEN_SCHEMAS)
            .context("Should be able to subtract an hour from creation time")?)
        .try_into()?;
        let namespace = self.namespace;
        let mut num_deleted = 0;
        let creation_time_index = SystemIndex::<SchemasTable>::by_creation_time();
        let mut schemas = self
            .tx
            .query_system(namespace, &creation_time_index)?
            .build();
        while let Some(schema_doc) = schemas.next().await? {
            // Only delete failed and overwritten schemas
            match schema_doc.state {
                SchemaState::Failed { .. } | SchemaState::Overwritten => {},
                SchemaState::Active | SchemaState::Pending | SchemaState::Validated => continue,
            }
            // A schema changed by this transaction may still have a live checkpoint
            // from before the transition. Keep it for out-of-transaction cleanup.
            if schemas_to_keep.contains(&schema_doc.id()) {
                continue;
            }
            // Break if the schemas are not old enough to be deleted
            if schema_doc.creation_time() > oldest_schema_to_keep {
                break;
            }
            // Unprotected schemas were terminal before this transaction began, so a
            // state-fenced worker cannot publish another checkpoint for them.
            SchemaValidationProgressModel::new(schemas.tx(), namespace)
                .delete_schema_validation_progress(schema_doc.id())
                .await?;
            SystemMetadataModel::new(schemas.tx(), namespace)
                .delete(schema_doc.id())
                .await?;
            num_deleted += 1;
            if num_deleted >= MAX_SCHEMA_HISTORY_DELETIONS_PER_TRANSACTION {
                break;
            }
        }
        Ok(())
    }

    async fn mark_overwritten(
        &mut self,
        id: ResolvedDocumentId,
        previous_state: SchemaState,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            previous_state.is_unique(),
            "Only a unique schema state can be overwritten",
        );
        SystemMetadataModel::new(self.tx, self.namespace)
            .patch(
                id,
                patch_value!("state" => Some(SchemaState::Overwritten.try_into()?))?,
            )
            .await?;
        if previous_state == SchemaState::Pending {
            // Pending-schema replacement must win over progress bookkeeping. The
            // state-fenced tracker or the schema worker's next pass removes this row.
            return Ok(());
        }
        let mut model = SchemaValidationProgressModel::new(self.tx, self.namespace);
        model.delete_schema_validation_progress(id).await?;
        Ok(())
    }
}
