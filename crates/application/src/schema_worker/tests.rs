use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{
            AtomicU64,
            Ordering,
        },
        Arc,
    },
    time::{
        Duration,
        SystemTime,
    },
};

use anyhow::Context;
use common::{
    bootstrap_model::schema::{
        SchemaMetadata,
        SchemaState,
    },
    db_schema,
    object_validator,
    pause::PauseClient,
    persistence::Persistence,
    runtime::{
        new_unlimited_rate_limiter,
        Runtime,
        SpawnHandle,
    },
    schemas::{
        validator::{
            FieldValidator,
            Validator,
        },
        DatabaseSchema,
        DocumentSchema,
        SchemaValidationError,
    },
    shutdown::ShutdownSignal,
    virtual_system_mapping::VirtualSystemMapping,
};
use database::{
    Database,
    SchemaModel,
    SchemaValidationProgressMetadata,
    SchemaValidationProgressModel,
    SchemaValidationProgressTable,
    SchemasTable,
    SystemMetadataModel,
    TableModel,
    Transaction,
    UserFacingModel,
    COMPONENTS_TABLE,
    SCHEMAS_TABLE,
    SCHEMA_VALIDATION_PROGRESS_TABLE,
};
use errors::ErrorMetadataAnyhowExt;
use futures::future::FusedFuture;
use indexing::index_cache::IndexCache;
use keybroker::Identity;
use model::{
    initialize_application_system_table,
    DEFAULT_TABLE_NUMBERS,
};
use rand::RngCore;
use runtime::prod::ProdRuntime;
use search::searcher::SearcherStub;
use sqlite::SqlitePersistence;
use value::{
    obj,
    ConvexObject,
    DeveloperDocumentId,
    ResolvedDocumentId,
    TableName,
    TableNamespace,
};

use super::{
    SchemaValidationProgressTracker,
    SchemaWorker,
};

const TEST_NAMESPACE: TableNamespace = TableNamespace::root_component();
const RETENTION_AGE: Duration = Duration::from_secs(2 * 60 * 60);

#[derive(Clone)]
struct SchemaTestRuntime {
    inner: ProdRuntime,
    wall_clock_offset_seconds: Arc<AtomicU64>,
}

impl SchemaTestRuntime {
    fn new(inner: ProdRuntime) -> Self {
        Self {
            inner,
            wall_clock_offset_seconds: Arc::new(AtomicU64::new(0)),
        }
    }

    fn advance_wall_clock(&self, duration: Duration) {
        self.wall_clock_offset_seconds
            .fetch_add(duration.as_secs(), Ordering::SeqCst);
    }

    fn block_on<F: Future>(&self, name: &'static str, future: F) -> F::Output {
        self.inner.block_on(name, future)
    }
}

impl Runtime for SchemaTestRuntime {
    fn wait(&self, duration: Duration) -> Pin<Box<dyn FusedFuture<Output = ()> + Send + 'static>> {
        self.inner.wait(duration)
    }

    fn spawn(
        &self,
        name: &'static str,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Box<dyn SpawnHandle> {
        self.inner.spawn(name, future)
    }

    fn spawn_thread<Fut: Future<Output = ()>, F: FnOnce() -> Fut + Send + 'static>(
        &self,
        name: &str,
        future: F,
    ) -> Box<dyn SpawnHandle> {
        self.inner.spawn_thread(name, future)
    }

    fn system_time(&self) -> SystemTime {
        self.inner.system_time()
            + Duration::from_secs(self.wall_clock_offset_seconds.load(Ordering::SeqCst))
    }

    fn monotonic_now(&self) -> tokio::time::Instant {
        self.inner.monotonic_now()
    }

    fn rng(&self) -> Box<dyn RngCore> {
        self.inner.rng()
    }

    fn pause_client(&self) -> PauseClient {
        self.inner.pause_client()
    }
}

fn run_schema_test<F, Fut>(name: &'static str, test: F) -> anyhow::Result<()>
where
    F: FnOnce(SchemaTestRuntime) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    let tokio = ProdRuntime::init_tokio()?;
    let runtime = SchemaTestRuntime::new(ProdRuntime::new(&tokio));
    runtime.block_on(name, test(runtime.clone()))
}

async fn new_test_database(
    runtime: SchemaTestRuntime,
) -> anyhow::Result<Database<SchemaTestRuntime>> {
    // ba16e0638 removed the public in-memory database fixture. SQLite plus the
    // production Database constructor is the narrowest remaining fixture that
    // still exercises real transaction read sets and the OCC committer.
    let persistence: Arc<dyn Persistence> = Arc::new(SqlitePersistence::new(":memory:")?);
    let (deleted_tablet_sender, _deleted_tablet_receiver) = tokio::sync::mpsc::channel(16);
    Database::load(
        persistence,
        runtime.clone(),
        Arc::new(SearcherStub),
        ShutdownSignal::panic(),
        VirtualSystemMapping::default(),
        IndexCache::new(1 << 20).new_handle(),
        Arc::new(new_unlimited_rate_limiter(runtime)),
        deleted_tablet_sender,
        "schema_worker_tests".to_owned(),
    )
    .await
}

async fn commit(
    database: &Database<SchemaTestRuntime>,
    transaction: Transaction<SchemaTestRuntime>,
    write_source: &'static str,
) -> anyhow::Result<()> {
    database
        .commit_with_write_source(transaction, write_source)
        .await?;
    Ok(())
}

fn restrictive_schema(table_name: &TableName) -> anyhow::Result<DatabaseSchema> {
    Ok(db_schema!(
        table_name => DocumentSchema::Union(vec![object_validator!(
            "required" => FieldValidator::required_field_type(Validator::Int64)
        )]),
    ))
}

async fn submit_active_and_pending_schema(
    database: &Database<SchemaTestRuntime>,
    table_name: &TableName,
) -> anyhow::Result<ResolvedDocumentId> {
    let mut tx = database.begin_system().await?;
    let active_schema = db_schema!(table_name => DocumentSchema::Any);
    let (active_id, active_state) = SchemaModel::new(&mut tx, TEST_NAMESPACE)
        .submit_pending(active_schema)
        .await?;
    assert_eq!(active_state, SchemaState::Pending);
    SchemaModel::new(&mut tx, TEST_NAMESPACE)
        .mark_validated(active_id)
        .await?;
    SchemaModel::new(&mut tx, TEST_NAMESPACE)
        .mark_active(active_id)
        .await?;

    let (pending_id, pending_state) = SchemaModel::new(&mut tx, TEST_NAMESPACE)
        .submit_pending(restrictive_schema(table_name)?)
        .await?;
    assert_eq!(pending_state, SchemaState::Pending);
    commit(database, tx, "test_schema_setup").await?;
    Ok(pending_id)
}

async fn submit_pending_schema(
    database: &Database<SchemaTestRuntime>,
    table_name: &TableName,
) -> anyhow::Result<ResolvedDocumentId> {
    let mut tx = database.begin_system().await?;
    let (schema_id, state) = SchemaModel::new(&mut tx, TEST_NAMESPACE)
        .submit_pending(db_schema!(table_name => DocumentSchema::Any))
        .await?;
    assert_eq!(state, SchemaState::Pending);
    commit(database, tx, "test_pending_schema_submitted").await?;
    Ok(schema_id)
}

async fn initialize_progress(
    database: &Database<SchemaTestRuntime>,
    schema_id: ResolvedDocumentId,
) -> anyhow::Result<()> {
    let mut tx = database.begin_system().await?;
    assert!(
        SchemaValidationProgressModel::new(&mut tx, TEST_NAMESPACE)
            .initialize_schema_validation_progress(schema_id, Some(10))
            .await?
    );
    commit(database, tx, "test_progress_initialized").await
}

async fn schema_state(
    tx: &mut Transaction<SchemaTestRuntime>,
    schema_id: ResolvedDocumentId,
) -> anyhow::Result<Option<SchemaState>> {
    tx.get(schema_id)
        .await?
        .map(|document| {
            let metadata = SchemaMetadata::try_from(document.into_value().into_value())?;
            Ok(metadata.state)
        })
        .transpose()
}

async fn progress(
    tx: &mut Transaction<SchemaTestRuntime>,
    schema_id: ResolvedDocumentId,
) -> anyhow::Result<Option<Arc<common::document::ParsedDocument<SchemaValidationProgressMetadata>>>>
{
    SchemaValidationProgressModel::new(tx, TEST_NAMESPACE)
        .existing_schema_validation_progress(schema_id)
        .await
}

async fn mark_failed(
    tx: &mut Transaction<SchemaTestRuntime>,
    schema_id: ResolvedDocumentId,
    table_name: &TableName,
) -> anyhow::Result<bool> {
    SchemaModel::new(tx, TEST_NAMESPACE)
        .mark_failed(
            schema_id,
            SchemaValidationError::TableCannotBeDeleted {
                table_name: table_name.clone(),
            },
        )
        .await
}

async fn insert_schema_failing_document(
    tx: &mut Transaction<SchemaTestRuntime>,
    table_name: &TableName,
) -> anyhow::Result<DeveloperDocumentId> {
    UserFacingModel::new(tx, TEST_NAMESPACE)
        .insert(table_name.clone(), ConvexObject::empty())
        .await
}

#[test]
fn schema_validation_progress_update_does_not_abort_schema_failure() -> anyhow::Result<()> {
    run_schema_test(
        "schema_validation_progress_update_does_not_abort_schema_failure",
        |runtime| async move {
            let database = new_test_database(runtime).await?;
            let table_name: TableName = "documents".parse()?;
            let schema_id = submit_active_and_pending_schema(&database, &table_name).await?;
            initialize_progress(&database, schema_id).await?;

            // Begin and fully stage the application write first. The pending schema
            // is Failed locally, but remains Pending to other transactions.
            let mut application_tx = database.begin(Identity::Unknown(None)).await?;
            let document_id =
                insert_schema_failing_document(&mut application_tx, &table_name).await?;
            assert!(matches!(
                schema_state(&mut application_tx, schema_id).await?,
                Some(SchemaState::Failed { .. })
            ));

            // Commit a checkpoint after the application transaction began. Before
            // this fix, mark_failed had read this row and the following commit made
            // the single application commit lose OCC.
            let mut progress_tx = database.begin_system().await?;
            assert!(
                SchemaValidationProgressModel::new(&mut progress_tx, TEST_NAMESPACE)
                    .update_schema_validation_progress(schema_id, 1, Some(10))
                    .await?
            );
            commit(&database, progress_tx, "schema_validation_progress_updated").await?;

            // No outer retry helper: this is the already-open transaction's only
            // commit attempt.
            commit(&database, application_tx, "test_schema_failing_write").await?;

            let mut verify_tx = database.begin_system().await?;
            assert!(matches!(
                schema_state(&mut verify_tx, schema_id).await?,
                Some(SchemaState::Failed { .. })
            ));
            let resolved_document_id =
                verify_tx.resolve_developer_id(&document_id, TEST_NAMESPACE)?;
            assert!(verify_tx.get(resolved_document_id).await?.is_some());
            assert_eq!(
                progress(&mut verify_tx, schema_id)
                    .await?
                    .expect("failure cleanup is deferred")
                    .num_docs_validated,
                1
            );
            Ok(())
        },
    )
}

#[test]
fn schema_validation_progress_checkpoint_loses_to_schema_failure() -> anyhow::Result<()> {
    run_schema_test(
        "schema_validation_progress_checkpoint_loses_to_schema_failure",
        |runtime| async move {
            let database = new_test_database(runtime).await?;
            let table_name: TableName = "documents".parse()?;
            let schema_id = submit_active_and_pending_schema(&database, &table_name).await?;
            initialize_progress(&database, schema_id).await?;

            // The checkpoint reads the exact schema document while it is Pending,
            // then stages a progress replacement without committing.
            let mut stale_progress_tx = database.begin_system().await?;
            assert!(
                SchemaValidationProgressModel::new(&mut stale_progress_tx, TEST_NAMESPACE)
                    .update_schema_validation_progress(schema_id, 1, Some(10))
                    .await?
            );

            let mut application_tx = database.begin(Identity::Unknown(None)).await?;
            let document_id =
                insert_schema_failing_document(&mut application_tx, &table_name).await?;
            commit(&database, application_tx, "test_schema_failing_write").await?;

            // The failure transaction never reads progress, so its _schemas write is
            // the only possible conflict with this stale checkpoint transaction.
            let error = database
                .commit_with_write_source(stale_progress_tx, "schema_validation_progress_updated")
                .await
                .expect_err("a checkpoint based on Pending must lose to Failed");
            assert!(error.is_occ(), "expected OCC, got {error:#}");
            assert_eq!(
                error
                    .occ_info()
                    .expect("OCC metadata")
                    .write_source
                    .as_deref(),
                Some("test_schema_failing_write")
            );

            let mut verify_tx = database.begin_system().await?;
            assert!(matches!(
                schema_state(&mut verify_tx, schema_id).await?,
                Some(SchemaState::Failed { .. })
            ));
            let resolved_document_id =
                verify_tx.resolve_developer_id(&document_id, TEST_NAMESPACE)?;
            assert!(verify_tx.get(resolved_document_id).await?.is_some());
            assert_eq!(
                progress(&mut verify_tx, schema_id)
                    .await?
                    .expect("physical cleanup has not run")
                    .num_docs_validated,
                0,
                "the stale checkpoint must not publish"
            );
            Ok(())
        },
    )
}

#[test]
fn schema_validation_progress_tracker_respects_lifecycle_states() -> anyhow::Result<()> {
    run_schema_test(
        "schema_validation_progress_tracker_respects_lifecycle_states",
        |runtime| async move {
            let database = new_test_database(runtime).await?;
            let table_name: TableName = "documents".parse()?;

            let overwritten_id = submit_pending_schema(&database, &table_name).await?;
            initialize_progress(&database, overwritten_id).await?;
            let mut overwrite_tx = database.begin_system().await?;
            let replacement_table: TableName = "replacement".parse()?;
            let (replacement_id, state) = SchemaModel::new(&mut overwrite_tx, TEST_NAMESPACE)
                .submit_pending(db_schema!(&replacement_table => DocumentSchema::Any))
                .await?;
            assert_eq!(state, SchemaState::Pending);
            commit(&database, overwrite_tx, "test_pending_schema_overwritten").await?;

            let mut before_stale_init = database.begin_system().await?;
            assert!(progress(&mut before_stale_init, overwritten_id)
                .await?
                .is_some());
            drop(before_stale_init);

            let mut stale_init_tx = database.begin_system().await?;
            assert!(
                !SchemaValidationProgressModel::new(&mut stale_init_tx, TEST_NAMESPACE)
                    .initialize_schema_validation_progress(overwritten_id, Some(10))
                    .await?
            );
            commit(&database, stale_init_tx, "test_stale_tracker_initialized").await?;
            let mut verify_overwrite_tx = database.begin_system().await?;
            assert!(progress(&mut verify_overwrite_tx, overwritten_id)
                .await?
                .is_none());
            assert_eq!(
                schema_state(&mut verify_overwrite_tx, replacement_id).await?,
                Some(SchemaState::Pending)
            );
            drop(verify_overwrite_tx);

            let mut stale_failure_tx = database.begin_system().await?;
            assert!(
                !mark_failed(&mut stale_failure_tx, overwritten_id, &table_name).await?,
                "a replacement must not be reported as a schema failure"
            );
            commit(
                &database,
                stale_failure_tx,
                "test_overwrite_wins_schema_failure_race",
            )
            .await?;

            let mut missing_progress_tx = database.begin_system().await?;
            assert!(
                !SchemaValidationProgressModel::new(&mut missing_progress_tx, TEST_NAMESPACE)
                    .update_schema_validation_progress(replacement_id, 1, Some(10))
                    .await?
            );
            commit(
                &database,
                missing_progress_tx,
                "test_missing_progress_cancels_tracker",
            )
            .await?;

            initialize_progress(&database, replacement_id).await?;
            let mut completed_progress_tx = database.begin_system().await?;
            assert!(
                SchemaValidationProgressModel::new(&mut completed_progress_tx, TEST_NAMESPACE,)
                    .update_schema_validation_progress(replacement_id, 7, Some(10))
                    .await?
            );
            commit(&database, completed_progress_tx, "test_completed_progress").await?;

            let mut overflowing_progress_tx = database.begin_system().await?;
            let overflow_error =
                SchemaValidationProgressModel::new(&mut overflowing_progress_tx, TEST_NAMESPACE)
                    .update_schema_validation_progress(replacement_id, u64::MAX, Some(10))
                    .await
                    .expect_err("progress addition must not wrap");
            assert!(overflow_error
                .to_string()
                .contains("numDocsValidated overflowed while updating progress"));

            let mut validated_tx = database.begin_system().await?;
            SchemaModel::new(&mut validated_tx, TEST_NAMESPACE)
                .mark_validated(replacement_id)
                .await?;
            commit(&database, validated_tx, "test_schema_validated").await?;

            let duplicate_tracker = SchemaValidationProgressTracker::new(
                database.clone(),
                TEST_NAMESPACE,
                [replacement_table].into_iter().collect(),
                replacement_id,
                Some(10),
            )
            .await?;
            assert!(duplicate_tracker.is_none());
            let mut verify_validated_tx = database.begin_system().await?;
            let completed_progress = progress(&mut verify_validated_tx, replacement_id)
                .await?
                .expect("Validated preserves completed progress");
            assert_eq!(completed_progress.num_docs_validated, 7);
            drop(verify_validated_tx);

            let mut active_tx = database.begin_system().await?;
            SchemaModel::new(&mut active_tx, TEST_NAMESPACE)
                .mark_active(replacement_id)
                .await?;
            commit(&database, active_tx, "test_schema_activated").await?;
            let mut verify_active_tx = database.begin_system().await?;
            assert_eq!(
                schema_state(&mut verify_active_tx, replacement_id).await?,
                Some(SchemaState::Active)
            );
            assert!(progress(&mut verify_active_tx, replacement_id)
                .await?
                .is_none());
            drop(verify_active_tx);

            // Even an idempotent activation must reject contradictory in-progress
            // state before deleting mixed-version progress for the active schema.
            let mut contradictory_tx = database.begin_system().await?;
            SystemMetadataModel::new(&mut contradictory_tx, TEST_NAMESPACE)
                .insert(
                    &SCHEMA_VALIDATION_PROGRESS_TABLE,
                    SchemaValidationProgressMetadata {
                        schema_id: replacement_id.developer_id,
                        num_docs_validated: 3,
                        total_docs: Some(10),
                    }
                    .try_into()?,
                )
                .await?;
            for state in [SchemaState::Pending, SchemaState::Validated] {
                SystemMetadataModel::new(&mut contradictory_tx, TEST_NAMESPACE)
                    .insert(
                        &SCHEMAS_TABLE,
                        SchemaMetadata::new(state, db_schema!())?.try_into()?,
                    )
                    .await?;
            }
            let activation_error = SchemaModel::new(&mut contradictory_tx, TEST_NAMESPACE)
                .mark_active(replacement_id)
                .await
                .expect_err("activation must reject contradictory in-progress schemas");
            assert!(activation_error
                .to_string()
                .contains("both pending and validated schemas exist"));
            let clear_error = SchemaModel::new(&mut contradictory_tx, TEST_NAMESPACE)
                .clear_active()
                .await
                .expect_err("clearing active must reject contradictory in-progress schemas");
            assert!(clear_error
                .to_string()
                .contains("both pending and validated schemas exist"));
            assert!(progress(&mut contradictory_tx, replacement_id)
                .await?
                .is_some());
            Ok(())
        },
    )
}

#[test]
fn schema_validation_progress_checkpoint_threshold_is_never_zero() {
    assert_eq!(
        SchemaValidationProgressTracker::<SchemaTestRuntime>::checkpoint_threshold(None),
        500
    );
    assert_eq!(
        SchemaValidationProgressTracker::<SchemaTestRuntime>::checkpoint_threshold(Some(0)),
        1
    );
    assert_eq!(
        SchemaValidationProgressTracker::<SchemaTestRuntime>::checkpoint_threshold(Some(1)),
        1
    );
    assert_eq!(
        SchemaValidationProgressTracker::<SchemaTestRuntime>::checkpoint_threshold(Some(10_000)),
        500
    );
}

#[test]
fn schema_validation_worker_finishes_and_preserves_progress_until_activation() -> anyhow::Result<()>
{
    run_schema_test(
        "schema_validation_worker_finishes_and_preserves_progress_until_activation",
        |runtime| async move {
            let database = new_test_database(runtime.clone()).await?;
            let table_name: TableName = "completed_validation".parse()?;
            let mut setup_tx = database.begin_system().await?;
            let (schema_id, state) = SchemaModel::new(&mut setup_tx, TEST_NAMESPACE)
                .submit_pending(restrictive_schema(&table_name)?)
                .await?;
            assert_eq!(state, SchemaState::Pending);
            UserFacingModel::new(&mut setup_tx, TEST_NAMESPACE)
                .insert(table_name.clone(), obj!("required" => 1_i64)?)
                .await?;
            commit(&database, setup_tx, "test_validation_setup").await?;

            let worker = SchemaWorker {
                runtime,
                database: database.clone(),
            };
            worker.run().await?;

            let mut validated_tx = database.begin_system().await?;
            assert_eq!(
                schema_state(&mut validated_tx, schema_id).await?,
                Some(SchemaState::Validated)
            );
            let completed_progress = progress(&mut validated_tx, schema_id)
                .await?
                .expect("successful validation preserves completed progress");
            assert_eq!(completed_progress.num_docs_validated, 1);
            drop(validated_tx);

            let duplicate_tracker = SchemaValidationProgressTracker::new(
                database.clone(),
                TEST_NAMESPACE,
                [table_name].into_iter().collect(),
                schema_id,
                Some(1),
            )
            .await?;
            assert!(duplicate_tracker.is_none());
            let mut duplicate_verify_tx = database.begin_system().await?;
            assert_eq!(
                progress(&mut duplicate_verify_tx, schema_id)
                    .await?
                    .expect("duplicate worker must preserve completed progress")
                    .num_docs_validated,
                1
            );
            drop(duplicate_verify_tx);

            let mut activation_tx = database.begin_system().await?;
            SchemaModel::new(&mut activation_tx, TEST_NAMESPACE)
                .mark_active(schema_id)
                .await?;
            commit(&database, activation_tx, "test_completed_schema_activated").await?;

            let mut active_verify_tx = database.begin_system().await?;
            assert_eq!(
                schema_state(&mut active_verify_tx, schema_id).await?,
                Some(SchemaState::Active)
            );
            assert!(progress(&mut active_verify_tx, schema_id).await?.is_none());
            Ok(())
        },
    )
}

#[test]
fn schema_validation_progress_restart_cleans_all_inactive_progress() -> anyhow::Result<()> {
    run_schema_test(
        "schema_validation_progress_restart_cleans_all_inactive_progress",
        |runtime| async move {
            let database = new_test_database(runtime.clone()).await?;
            let failed_table: TableName = "failed".parse()?;
            let failed_id = submit_pending_schema(&database, &failed_table).await?;
            initialize_progress(&database, failed_id).await?;
            let mut fail_tx = database.begin_system().await?;
            assert!(mark_failed(&mut fail_tx, failed_id, &failed_table).await?);
            commit(&database, fail_tx, "test_schema_failed").await?;

            let overwritten_table: TableName = "overwritten".parse()?;
            let overwritten_id = submit_pending_schema(&database, &overwritten_table).await?;
            initialize_progress(&database, overwritten_id).await?;
            let mut overwrite_tx = database.begin_system().await?;
            assert!(
                SchemaModel::new(&mut overwrite_tx, TEST_NAMESPACE)
                    .overwrite_all()
                    .await?
            );
            commit(&database, overwrite_tx, "test_schema_overwritten").await?;

            // Old unfenced workers can recreate progress after activation during a
            // mixed-version rollout. Cleanup must not rely on terminal schema state.
            let active_table: TableName = "active".parse()?;
            let active_id = submit_pending_schema(&database, &active_table).await?;
            let mut activate_tx = database.begin_system().await?;
            SchemaModel::new(&mut activate_tx, TEST_NAMESPACE)
                .mark_validated(active_id)
                .await?;
            SchemaModel::new(&mut activate_tx, TEST_NAMESPACE)
                .mark_active(active_id)
                .await?;
            commit(&database, activate_tx, "test_schema_activated").await?;
            let mut stale_active_progress_tx = database.begin_system().await?;
            SystemMetadataModel::new(&mut stale_active_progress_tx, TEST_NAMESPACE)
                .insert(
                    &SCHEMA_VALIDATION_PROGRESS_TABLE,
                    SchemaValidationProgressMetadata {
                        schema_id: active_id.developer_id,
                        num_docs_validated: 3,
                        total_docs: Some(10),
                    }
                    .try_into()?,
                )
                .await?;
            commit(
                &database,
                stale_active_progress_tx,
                "test_old_worker_recreated_active_progress",
            )
            .await?;

            // An old backend can prune a terminal schema before a new worker's
            // deferred cleanup. The progress scan must also remove that orphan.
            let missing_table: TableName = "missing".parse()?;
            let missing_id = submit_pending_schema(&database, &missing_table).await?;
            initialize_progress(&database, missing_id).await?;
            let mut prune_tx = database.begin_system().await?;
            assert!(mark_failed(&mut prune_tx, missing_id, &missing_table).await?);
            SystemMetadataModel::new(&mut prune_tx, TEST_NAMESPACE)
                .delete(missing_id)
                .await?;
            commit(&database, prune_tx, "test_old_backend_pruned_schema").await?;

            let validated_table: TableName = "validated".parse()?;
            let validated_id = submit_pending_schema(&database, &validated_table).await?;
            initialize_progress(&database, validated_id).await?;
            let mut validated_tx = database.begin_system().await?;
            SchemaModel::new(&mut validated_tx, TEST_NAMESPACE)
                .mark_validated(validated_id)
                .await?;
            commit(&database, validated_tx, "test_schema_validated").await?;

            let mut before_restart_tx = database.begin_system().await?;
            assert!(progress(&mut before_restart_tx, failed_id).await?.is_some());
            assert!(progress(&mut before_restart_tx, overwritten_id)
                .await?
                .is_some());
            assert!(progress(&mut before_restart_tx, active_id).await?.is_some());
            assert!(progress(&mut before_restart_tx, missing_id)
                .await?
                .is_some());
            assert!(progress(&mut before_restart_tx, validated_id)
                .await?
                .is_some());
            drop(before_restart_tx);

            // A new worker instance has no in-memory tracker ownership. Its first
            // pass discovers inactive progress read-only, then point-deletes only
            // those progress documents.
            let restarted_worker = SchemaWorker {
                runtime,
                database: database.clone(),
            };
            restarted_worker.run().await?;

            let mut verify_tx = database.begin_system().await?;
            assert!(progress(&mut verify_tx, failed_id).await?.is_none());
            assert!(progress(&mut verify_tx, overwritten_id).await?.is_none());
            assert!(progress(&mut verify_tx, active_id).await?.is_none());
            assert!(progress(&mut verify_tx, missing_id).await?.is_none());
            assert!(progress(&mut verify_tx, validated_id).await?.is_some());
            drop(verify_tx);

            let cleanup_token = restarted_worker
                .run()
                .await?
                .cleanup_token
                .expect("a clean no-work pass subscribes to progress cleanup");

            // An old worker can write after the new worker's cleanup discovery
            // without changing schema state. The cleanup token must wake the new
            // worker instead of leaving this active-schema row until another push.
            let mut late_stale_progress_tx = database.begin_system().await?;
            SystemMetadataModel::new(&mut late_stale_progress_tx, TEST_NAMESPACE)
                .insert(
                    &SCHEMA_VALIDATION_PROGRESS_TABLE,
                    SchemaValidationProgressMetadata {
                        schema_id: active_id.developer_id,
                        num_docs_validated: 4,
                        total_docs: Some(10),
                    }
                    .try_into()?,
                )
                .await?;
            commit(
                &database,
                late_stale_progress_tx,
                "test_old_worker_wrote_after_cleanup_discovery",
            )
            .await?;
            tokio::time::timeout(
                Duration::from_secs(1),
                database.subscribe_and_wait_for_invalidation(cleanup_token),
            )
            .await
            .context("late inactive progress did not invalidate the cleanup token")??;

            restarted_worker.run().await?;
            let mut late_cleanup_verify_tx = database.begin_system().await?;
            assert!(progress(&mut late_cleanup_verify_tx, active_id)
                .await?
                .is_none());
            assert!(progress(&mut late_cleanup_verify_tx, validated_id)
                .await?
                .is_some());
            Ok(())
        },
    )
}

#[test]
fn schema_validation_progress_cleanup_handles_deleted_component_tables() -> anyhow::Result<()> {
    run_schema_test(
        "schema_validation_progress_cleanup_handles_deleted_component_tables",
        |runtime| async move {
            let database = new_test_database(runtime.clone()).await?;
            let worker = SchemaWorker {
                runtime,
                database: database.clone(),
            };
            let namespace_discovery_token = worker.run().await?.token;
            let table_name: TableName = "deleted_component".parse()?;
            let mut setup_tx = database.begin_system().await?;
            let namespace_id =
                SystemMetadataModel::new_global(&mut setup_tx).allocate_internal_id()?;
            let namespace_table_number = setup_tx
                .table_mapping()
                .namespace(TEST_NAMESPACE)
                .id(&COMPONENTS_TABLE)?
                .table_number;
            let namespace = TableNamespace::ByComponent(DeveloperDocumentId::new(
                namespace_table_number,
                namespace_id,
            ));
            initialize_application_system_table(
                &mut setup_tx,
                &SchemasTable,
                namespace,
                &DEFAULT_TABLE_NUMBERS,
            )
            .await?;
            initialize_application_system_table(
                &mut setup_tx,
                &SchemaValidationProgressTable,
                namespace,
                &DEFAULT_TABLE_NUMBERS,
            )
            .await?;
            let (schema_id, state) = SchemaModel::new(&mut setup_tx, namespace)
                .submit_pending(db_schema!(&table_name => DocumentSchema::Any))
                .await?;
            assert_eq!(state, SchemaState::Pending);
            assert!(
                SchemaValidationProgressModel::new(&mut setup_tx, namespace)
                    .initialize_schema_validation_progress(schema_id, Some(10))
                    .await?
            );
            SchemaModel::new(&mut setup_tx, namespace)
                .mark_validated(schema_id)
                .await?;
            commit(&database, setup_tx, "test_component_schema_setup").await?;
            tokio::time::timeout(
                Duration::from_secs(1),
                database.subscribe_and_wait_for_invalidation(namespace_discovery_token),
            )
            .await
            .context("new component namespace did not invalidate schema discovery")??;

            // Mixed-version or interrupted maintenance can leave an active
            // progress table after its schema table is gone.
            let mut delete_schema_tx = database.begin_system().await?;
            TableModel::new(&mut delete_schema_tx)
                .delete_active_table(namespace, SCHEMAS_TABLE.clone())
                .await?;
            commit(&database, delete_schema_tx, "test_schema_table_deleted").await?;

            // Recreate the schema table first, but leave it empty while cleanup
            // discovers the old progress row. Fencing only on the recreated table
            // generation is insufficient for this ordering.
            let mut recreate_schema_table_tx = database.begin_system().await?;
            initialize_application_system_table(
                &mut recreate_schema_table_tx,
                &SchemasTable,
                namespace,
                &DEFAULT_TABLE_NUMBERS,
            )
            .await?;
            commit(
                &database,
                recreate_schema_table_tx,
                "test_schema_table_recreated_empty",
            )
            .await?;

            let inactive_progress = {
                let mut cleanup_discovery_tx = database.begin_system().await?;
                let discovered =
                    SchemaValidationProgressModel::new(&mut cleanup_discovery_tx, namespace)
                        .inactive_schema_validation_progress()
                        .await?;
                assert_eq!(discovered.len(), 1);
                discovered
            };

            // A recreated system table can reuse its table number. Even if a
            // replacement schema also reuses the old internal ID, the old resolved
            // schema ID must not authorize writes to the replacement's progress.
            let mut recreate_schema_tx = database.begin_system().await?;
            let recreated_schema_id = SystemMetadataModel::new(&mut recreate_schema_tx, namespace)
                .insert_with_internal_id(
                    &SCHEMAS_TABLE,
                    schema_id.developer_id.internal_id(),
                    SchemaMetadata::new(
                        SchemaState::Pending,
                        db_schema!(&table_name => DocumentSchema::Any),
                    )?
                    .try_into()?,
                )
                .await?;
            assert_eq!(recreated_schema_id.developer_id, schema_id.developer_id);
            assert_ne!(recreated_schema_id.tablet_id, schema_id.tablet_id);
            assert!(
                SchemaValidationProgressModel::new(&mut recreate_schema_tx, namespace)
                    .initialize_schema_validation_progress(recreated_schema_id, Some(10))
                    .await?
            );
            commit(&database, recreate_schema_tx, "test_schema_table_recreated").await?;

            let mut recreated_state_tx = database.begin_system().await?;
            assert_eq!(
                SchemaModel::new(&mut recreated_state_tx, namespace)
                    .get_by_state(SchemaState::Pending)
                    .await?
                    .map(|(id, _schema)| id),
                Some(recreated_schema_id)
            );
            assert!(
                SchemaModel::new(&mut recreated_state_tx, namespace)
                    .get_by_state(SchemaState::Validated)
                    .await?
                    .is_none(),
                "the inactive generation must not leak cached unique schema state"
            );
            drop(recreated_state_tx);

            // Cleanup discovered against the empty recreated table must recheck the
            // exact schema key and preserve progress that initialization has since
            // assigned to the replacement pending schema.
            let mut stale_cleanup_tx = database.begin_system().await?;
            assert_eq!(
                SchemaValidationProgressModel::new(&mut stale_cleanup_tx, namespace)
                    .delete_schema_validation_progress_documents(&inactive_progress)
                    .await?,
                0
            );
            drop(stale_cleanup_tx);

            // Recreating the progress table can also reuse its table number and
            // document internal ID. A stale cleanup list must still address only
            // the old physical progress-table generation.
            let old_progress_id = inactive_progress[0].0;
            let mut recreate_progress_table_tx = database.begin_system().await?;
            let old_progress_tablet_id = recreate_progress_table_tx
                .table_mapping()
                .namespace(namespace)
                .id(&SCHEMA_VALIDATION_PROGRESS_TABLE)?
                .tablet_id;
            TableModel::new(&mut recreate_progress_table_tx)
                .delete_table_by_id_bypassing_schema_enforcement(old_progress_tablet_id)
                .await?;
            initialize_application_system_table(
                &mut recreate_progress_table_tx,
                &SchemaValidationProgressTable,
                namespace,
                &DEFAULT_TABLE_NUMBERS,
            )
            .await?;
            let recreated_progress_id =
                SystemMetadataModel::new(&mut recreate_progress_table_tx, namespace)
                    .insert_with_internal_id(
                        &SCHEMA_VALIDATION_PROGRESS_TABLE,
                        old_progress_id.developer_id.internal_id(),
                        SchemaValidationProgressMetadata {
                            schema_id: recreated_schema_id.developer_id,
                            num_docs_validated: 0,
                            total_docs: Some(10),
                        }
                        .try_into()?,
                    )
                    .await?;
            assert_eq!(
                recreated_progress_id.developer_id,
                old_progress_id.developer_id
            );
            assert_ne!(recreated_progress_id.tablet_id, old_progress_id.tablet_id);
            commit(
                &database,
                recreate_progress_table_tx,
                "test_schema_progress_table_recreated",
            )
            .await?;

            let mut old_progress_cleanup_tx = database.begin_system().await?;
            assert_eq!(
                SchemaValidationProgressModel::new(&mut old_progress_cleanup_tx, namespace)
                    .delete_schema_validation_progress_documents(&inactive_progress)
                    .await?,
                0
            );
            drop(old_progress_cleanup_tx);

            // A stale finish request can still address the retained old tablet by
            // resolved ID. It must not activate that generation or delete progress
            // belonging to the recreated pending schema.
            let mut stale_activation_tx = database.begin_system().await?;
            let stale_activation_error = SchemaModel::new(&mut stale_activation_tx, namespace)
                .mark_active(schema_id)
                .await
                .expect_err("an inactive schema-table generation cannot be activated");
            assert!(stale_activation_error
                .to_string()
                .contains("No document found for schema ID"));
            drop(stale_activation_tx);

            let mut old_generation_checkpoint_tx = database.begin_system().await?;
            assert!(
                !SchemaValidationProgressModel::new(&mut old_generation_checkpoint_tx, namespace,)
                    .update_schema_validation_progress(schema_id, 1, Some(10))
                    .await?
            );
            commit(
                &database,
                old_generation_checkpoint_tx,
                "test_old_schema_generation_checkpoint_stopped",
            )
            .await?;
            let mut verify_recreated_progress_tx = database.begin_system().await?;
            assert_eq!(
                SchemaValidationProgressModel::new(&mut verify_recreated_progress_tx, namespace)
                    .existing_schema_validation_progress(recreated_schema_id)
                    .await?
                    .expect("replacement generation keeps its progress")
                    .num_docs_validated,
                0
            );
            drop(verify_recreated_progress_tx);

            // Once the recreated schema table is gone, its stale checkpoint stops
            // and removes the orphan from the still-active progress table.
            let mut delete_recreated_schema_tx = database.begin_system().await?;
            TableModel::new(&mut delete_recreated_schema_tx)
                .delete_active_table(namespace, SCHEMAS_TABLE.clone())
                .await?;
            commit(
                &database,
                delete_recreated_schema_tx,
                "test_recreated_schema_table_deleted",
            )
            .await?;
            let mut stale_checkpoint_tx = database.begin_system().await?;
            assert!(
                !SchemaValidationProgressModel::new(&mut stale_checkpoint_tx, namespace)
                    .update_schema_validation_progress(recreated_schema_id, 1, Some(10))
                    .await?
            );
            commit(
                &database,
                stale_checkpoint_tx,
                "test_deleted_component_checkpoint",
            )
            .await?;
            let mut verify_cleanup_tx = database.begin_system().await?;
            assert!(
                SchemaValidationProgressModel::new(&mut verify_cleanup_tx, namespace)
                    .existing_schema_validation_progress(recreated_schema_id)
                    .await?
                    .is_none()
            );
            drop(verify_cleanup_tx);

            // Historical mappings remain after the whole component namespace is
            // deactivated. Worker discovery must skip the inactive progress table;
            // tablet retention now owns any remaining physical documents.
            let mut delete_progress_tx = database.begin_system().await?;
            TableModel::new(&mut delete_progress_tx)
                .delete_active_table(namespace, SCHEMA_VALIDATION_PROGRESS_TABLE.clone())
                .await?;
            commit(
                &database,
                delete_progress_tx,
                "test_schema_progress_table_deleted",
            )
            .await?;

            let mut after_table_deletion_tx = database.begin_system().await?;
            assert!(
                !SchemaValidationProgressModel::new(&mut after_table_deletion_tx, namespace)
                    .update_schema_validation_progress(schema_id, 1, Some(10))
                    .await?
            );
            commit(
                &database,
                after_table_deletion_tx,
                "test_deleted_component_checkpoint_stopped",
            )
            .await?;

            worker.run().await?;
            Ok(())
        },
    )
}

#[test]
fn schema_validation_progress_history_pruning_deletes_orphans() -> anyhow::Result<()> {
    run_schema_test(
        "schema_validation_progress_history_pruning_deletes_orphans",
        |runtime| async move {
            let database = new_test_database(runtime.clone()).await?;
            let old_table: TableName = "old_terminal".parse()?;
            let old_schema_id = submit_pending_schema(&database, &old_table).await?;
            initialize_progress(&database, old_schema_id).await?;
            let mut old_failure_tx = database.begin_system().await?;
            assert!(mark_failed(&mut old_failure_tx, old_schema_id, &old_table).await?);
            commit(&database, old_failure_tx, "test_old_schema_failed").await?;

            runtime.advance_wall_clock(RETENTION_AGE);

            let current_table: TableName = "current_terminal".parse()?;
            let current_schema_id = submit_pending_schema(&database, &current_table).await?;
            let mut current_failure_tx = database.begin_system().await?;
            assert!(mark_failed(&mut current_failure_tx, current_schema_id, &current_table).await?);
            commit(&database, current_failure_tx, "test_current_schema_failed").await?;

            let mut verify_tx = database.begin_system().await?;
            assert_eq!(schema_state(&mut verify_tx, old_schema_id).await?, None);
            assert!(progress(&mut verify_tx, old_schema_id).await?.is_none());
            assert!(matches!(
                schema_state(&mut verify_tx, current_schema_id).await?,
                Some(SchemaState::Failed { .. })
            ));
            drop(verify_tx);

            // A worker can lose an OCC to replacement and retry after a later
            // schema transition has pruned the old terminal generation.
            let mut stale_worker_tx = database.begin_system().await?;
            assert!(!mark_failed(&mut stale_worker_tx, old_schema_id, &old_table).await?);
            commit(
                &database,
                stale_worker_tx,
                "test_stale_worker_after_schema_pruning",
            )
            .await?;
            Ok(())
        },
    )
}

#[test]
fn old_schema_failure_and_overwrite_do_not_read_hot_progress() -> anyhow::Result<()> {
    run_schema_test(
        "old_schema_failure_and_overwrite_do_not_read_hot_progress",
        |runtime| async move {
            let database = new_test_database(runtime.clone()).await?;
            let failing_table: TableName = "old_failure".parse()?;
            let failing_schema_id = submit_pending_schema(&database, &failing_table).await?;
            initialize_progress(&database, failing_schema_id).await?;
            runtime.advance_wall_clock(RETENTION_AGE);

            let mut failure_tx = database.begin_system().await?;
            assert!(mark_failed(&mut failure_tx, failing_schema_id, &failing_table).await?);
            let mut concurrent_progress_tx = database.begin_system().await?;
            assert!(
                SchemaValidationProgressModel::new(&mut concurrent_progress_tx, TEST_NAMESPACE,)
                    .update_schema_validation_progress(failing_schema_id, 1, Some(10))
                    .await?
            );
            commit(
                &database,
                concurrent_progress_tx,
                "schema_validation_progress_updated",
            )
            .await?;
            commit(&database, failure_tx, "test_old_schema_failure").await?;
            let mut failure_verify_tx = database.begin_system().await?;
            assert!(matches!(
                schema_state(&mut failure_verify_tx, failing_schema_id).await?,
                Some(SchemaState::Failed { .. })
            ));
            assert_eq!(
                progress(&mut failure_verify_tx, failing_schema_id)
                    .await?
                    .expect("the current old schema is retained for deferred cleanup")
                    .num_docs_validated,
                1
            );
            drop(failure_verify_tx);

            let overwriting_table: TableName = "old_overwrite".parse()?;
            let overwriting_schema_id =
                submit_pending_schema(&database, &overwriting_table).await?;
            initialize_progress(&database, overwriting_schema_id).await?;
            runtime.advance_wall_clock(RETENTION_AGE);

            let mut overwrite_tx = database.begin_system().await?;
            let replacement_table: TableName = "new_pending".parse()?;
            let (replacement_id, replacement_state) =
                SchemaModel::new(&mut overwrite_tx, TEST_NAMESPACE)
                    .submit_pending(db_schema!(&replacement_table => DocumentSchema::Any))
                    .await?;
            assert_eq!(replacement_state, SchemaState::Pending);
            let mut second_progress_tx = database.begin_system().await?;
            assert!(
                SchemaValidationProgressModel::new(&mut second_progress_tx, TEST_NAMESPACE)
                    .update_schema_validation_progress(overwriting_schema_id, 1, Some(10))
                    .await?
            );
            commit(
                &database,
                second_progress_tx,
                "schema_validation_progress_updated",
            )
            .await?;
            commit(&database, overwrite_tx, "test_old_schema_overwrite").await?;

            let mut verify_tx = database.begin_system().await?;
            assert!(matches!(
                schema_state(&mut verify_tx, failing_schema_id).await?,
                Some(SchemaState::Failed { .. }) | None
            ));
            assert_eq!(
                schema_state(&mut verify_tx, overwriting_schema_id).await?,
                Some(SchemaState::Overwritten)
            );
            assert_eq!(
                schema_state(&mut verify_tx, replacement_id).await?,
                Some(SchemaState::Pending)
            );
            Ok(())
        },
    )
}
