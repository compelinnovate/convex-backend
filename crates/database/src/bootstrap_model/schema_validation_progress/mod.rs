pub mod types;

use std::sync::{
    Arc,
    LazyLock,
};

use anyhow::Context;
use common::{
    bootstrap_model::schema::SchemaState,
    document::{
        ParsedDocument,
        CREATION_TIME_FIELD_PATH,
    },
    runtime::Runtime,
};
use value::{
    DeveloperDocumentId,
    FieldPath,
    ResolvedDocumentId,
    TableName,
    TableNamespace,
};

use crate::{
    system_tables::{
        SystemIndex,
        SystemTable,
    },
    SchemaValidationProgressMetadata,
    SchemasTable,
    SystemMetadataModel,
    Transaction,
    SCHEMAS_TABLE,
};

pub const SCHEMA_VALIDATION_PROGRESS_TABLE: TableName =
    TableName::const_new("_schema_validation_progress");

pub static SCHEMA_VALIDATION_PROGRESS_BY_SCHEMA_ID: LazyLock<
    SystemIndex<SchemaValidationProgressTable>,
> = LazyLock::new(|| {
    SystemIndex::new(
        "by_schema_id",
        [&SCHEMA_ID_FIELD, &CREATION_TIME_FIELD_PATH],
    )
    .unwrap()
});

static SCHEMA_ID_FIELD: LazyLock<FieldPath> =
    LazyLock::new(|| "schemaId".parse().expect("invalid schemaId field"));

pub struct SchemaValidationProgressTable;

impl SystemTable for SchemaValidationProgressTable {
    type Metadata = types::SchemaValidationProgressMetadata;

    const TABLE_NAME: TableName = SCHEMA_VALIDATION_PROGRESS_TABLE;

    fn indexes() -> Vec<SystemIndex<Self>> {
        vec![SCHEMA_VALIDATION_PROGRESS_BY_SCHEMA_ID.clone()]
    }
}

pub struct SchemaValidationProgressModel<'a, RT: Runtime> {
    tx: &'a mut Transaction<RT>,
    namespace: TableNamespace,
}

impl<'a, RT: Runtime> SchemaValidationProgressModel<'a, RT> {
    pub fn new(tx: &'a mut Transaction<RT>, namespace: TableNamespace) -> Self {
        Self { tx, namespace }
    }

    pub async fn existing_schema_validation_progress(
        &mut self,
        schema_id: ResolvedDocumentId,
    ) -> anyhow::Result<Option<Arc<ParsedDocument<SchemaValidationProgressMetadata>>>> {
        self.tx
            .query_system(self.namespace, &*SCHEMA_VALIDATION_PROGRESS_BY_SCHEMA_ID)?
            .eq(&[schema_id.developer_id.encode_into(&mut Default::default())])?
            .unique()
            .await
    }

    /// Initialize progress only if this exact schema is still pending. Returns
    /// false when a stale worker should stop.
    pub async fn initialize_schema_validation_progress(
        &mut self,
        schema_id: ResolvedDocumentId,
        total_docs: Option<u64>,
    ) -> anyhow::Result<bool> {
        if !self.schema_allows_progress_write(schema_id).await? {
            return Ok(false);
        }
        let maybe_existing_metadata = self.existing_schema_validation_progress(schema_id).await?;
        let mut system_model = SystemMetadataModel::new(self.tx, self.namespace);
        let new_metadata = SchemaValidationProgressMetadata {
            schema_id: schema_id.developer_id,
            total_docs,
            num_docs_validated: 0,
        };
        if let Some(existing_metadata) = maybe_existing_metadata {
            system_model
                .replace(existing_metadata.id(), new_metadata.try_into()?)
                .await?;
            Ok(true)
        } else {
            system_model
                .insert(&SCHEMA_VALIDATION_PROGRESS_TABLE, new_metadata.try_into()?)
                .await?;
            Ok(true)
        }
    }

    /// Update the schema validation progress for a schema, adding
    /// `num_docs_validated` to the existing progress.
    /// Returns false if the schema is no longer pending or there is no existing
    /// progress metadata to update.
    pub async fn update_schema_validation_progress(
        &mut self,
        schema_id: ResolvedDocumentId,
        num_docs_validated: u64,
        // Only used if total_docs is missing
        total_docs: Option<u64>,
    ) -> anyhow::Result<bool> {
        // The schema read is the generation fence. A concurrent failure may commit
        // without reading progress; this transaction must then lose OCC instead of
        // publishing progress for a failed schema.
        if !self.schema_allows_progress_write(schema_id).await? {
            return Ok(false);
        }
        let Some(existing_metadata) = self.existing_schema_validation_progress(schema_id).await?
        else {
            return Ok(false);
        };

        let num_docs_validated = existing_metadata
            .num_docs_validated
            .checked_add(num_docs_validated)
            .context("numDocsValidated overflowed while updating progress")?;
        let new_metadata = SchemaValidationProgressMetadata {
            schema_id: schema_id.developer_id,
            total_docs: existing_metadata.total_docs.or(total_docs),
            num_docs_validated,
        };
        SystemMetadataModel::new(self.tx, self.namespace)
            .replace(existing_metadata.id(), new_metadata.try_into()?)
            .await?;
        Ok(true)
    }

    /// Resolve progress documents whose schema is inactive or missing.
    pub async fn inactive_schema_validation_progress(
        &mut self,
    ) -> anyhow::Result<Vec<(ResolvedDocumentId, DeveloperDocumentId)>> {
        let schemas_table_exists = self
            .tx
            .table_mapping()
            .namespace(self.namespace)
            .name_exists(&SCHEMAS_TABLE);
        let progress_documents = self
            .tx
            .query_system(
                self.namespace,
                &SystemIndex::<SchemaValidationProgressTable>::by_creation_time(),
            )?
            .all()
            .await?;
        let mut inactive_progress_ids = Vec::new();
        for progress in progress_documents {
            let schema = if schemas_table_exists {
                self.tx
                    .get_system::<SchemasTable>(self.namespace, progress.schema_id)
                    .await?
            } else {
                None
            };
            if !matches!(
                schema.as_deref().map(|schema| &schema.state),
                Some(SchemaState::Pending | SchemaState::Validated)
            ) {
                inactive_progress_ids.push((progress.id(), progress.schema_id));
            }
        }
        Ok(inactive_progress_ids)
    }

    /// Delete exact progress documents. Callers bound the slice so cleanup
    /// transactions cannot accumulate a deployment-wide write set. The by-ID
    /// reads also make concurrent cleanup idempotent without querying either
    /// table's secondary indexes.
    pub async fn delete_schema_validation_progress_documents(
        &mut self,
        progress_ids: &[(ResolvedDocumentId, DeveloperDocumentId)],
    ) -> anyhow::Result<usize> {
        // Component deletion deactivates the whole table atomically. Retention owns
        // its remaining documents, so a cleanup batch discovered just before that
        // transition has no current progress records to delete.
        let namespaced_table_mapping = self.tx.table_mapping().namespace(self.namespace);
        let Some(progress_tablet_id) =
            namespaced_table_mapping.id_if_exists(&SCHEMA_VALIDATION_PROGRESS_TABLE)
        else {
            return Ok(0);
        };
        let schemas_table_exists = namespaced_table_mapping.name_exists(&SCHEMAS_TABLE);
        let mut num_deleted = 0;
        for &(progress_id, schema_id) in progress_ids {
            // A recreated progress table owns different resolved document IDs,
            // even if its table number and document internal IDs are reused.
            if progress_id.tablet_id != progress_tablet_id {
                continue;
            }
            // Discovery can observe an empty recreated schema table before a new
            // pending schema reuses this developer ID and takes ownership of the
            // progress row. Recheck the exact schema key in the delete transaction;
            // a concurrent insertion then either wins this check or invalidates it.
            // Check schema first so a stale batch does not read a live validation's
            // hot progress row while deleting unrelated terminal rows.
            let schema = if schemas_table_exists {
                self.tx
                    .get_system::<SchemasTable>(self.namespace, schema_id)
                    .await?
            } else {
                None
            };
            if matches!(
                schema.as_deref().map(|schema| &schema.state),
                Some(SchemaState::Pending | SchemaState::Validated)
            ) {
                continue;
            }
            let Some(progress) = self
                .tx
                .get_system::<SchemaValidationProgressTable>(
                    self.namespace,
                    progress_id.developer_id,
                )
                .await?
            else {
                continue;
            };
            anyhow::ensure!(
                progress.id() == progress_id,
                "Schema validation progress document changed tables during cleanup"
            );
            anyhow::ensure!(
                progress.schema_id == schema_id,
                "Schema validation progress document changed schema ownership during cleanup"
            );
            SystemMetadataModel::new(self.tx, self.namespace)
                .delete(progress_id)
                .await?;
            num_deleted += 1;
        }
        Ok(num_deleted)
    }

    pub async fn delete_schema_validation_progress(
        &mut self,
        schema_id: ResolvedDocumentId,
    ) -> anyhow::Result<()> {
        if let Some(existing_metadata) = self.existing_schema_validation_progress(schema_id).await?
        {
            SystemMetadataModel::new(self.tx, self.namespace)
                .delete(existing_metadata.id())
                .await?;
        }
        Ok(())
    }

    async fn schema_allows_progress_write(
        &mut self,
        schema_id: ResolvedDocumentId,
    ) -> anyhow::Result<bool> {
        // This exact-document read makes the schema generation and state the
        // ordering boundary for every progress write.
        let namespaced_table_mapping = self.tx.table_mapping().namespace(self.namespace);
        let schemas_table_exists = namespaced_table_mapping.name_exists(&SCHEMAS_TABLE);
        let progress_table_exists =
            namespaced_table_mapping.name_exists(&SCHEMA_VALIDATION_PROGRESS_TABLE);
        let schema = if schemas_table_exists {
            self.tx
                .get_system::<SchemasTable>(self.namespace, schema_id.developer_id)
                .await?
        } else {
            None
        };
        match schema.as_deref() {
            // A recreated system table can reuse its table number. The full
            // resolved ID keeps an old worker from writing progress for a new
            // physical schema-table generation with the same developer ID.
            Some(schema) if schema.id() != schema_id => Ok(false),
            Some(schema) if schema.state == SchemaState::Pending => Ok(true),
            Some(schema) if schema.state == SchemaState::Validated => Ok(false),
            Some(_) | None => {
                if progress_table_exists {
                    self.delete_schema_validation_progress(schema_id).await?;
                }
                Ok(false)
            },
        }
    }
}
