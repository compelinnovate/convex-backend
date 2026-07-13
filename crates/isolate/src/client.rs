use std::{
    collections::{
        BTreeMap,
        BTreeSet,
        HashMap,
        VecDeque,
    },
    env,
    pin::pin,
    sync::{
        atomic::{
            AtomicUsize,
            Ordering,
        },
        Arc,
        Once,
    },
    task::Poll,
    time::Duration,
};

use ::metrics::{
    IntoLabel,
    Timer,
};
use anyhow::Context as _;
use async_trait::async_trait;
use common::{
    auth::AuthConfig,
    backoff::Backoff,
    bootstrap_model::components::definition::ComponentDefinitionMetadata,
    codel_queue::{
        new_codel_queue_async_with_reserved_capacity,
        CoDelQueueExpiredReceiver,
        CoDelQueueReceiver,
        CoDelQueueSendError,
        CoDelQueueSender,
        ExpiredInQueue,
    },
    components::{
        CanonicalizedComponentModulePath,
        ComponentDefinitionPath,
        ComponentName,
        Resource,
    },
    errors::{
        recapture_stacktrace,
        JsError,
    },
    execution_context::ExecutionContext,
    fastrace_helpers::{
        initialize_root_from_parent,
        EncodedSpan,
    },
    http::{
        fetch::FetchClient,
        RoutedHttpPath,
    },
    knobs::{
        ANALYZE_CONCURRENCY,
        CODEL_QUEUE_CONGESTED_EXPIRATION_MILLIS,
        CODEL_QUEUE_IDLE_EXPIRATION_MILLIS,
        HEAP_WORKER_REPORT_INTERVAL_SECONDS,
        ISOLATE_CONTROL_PLANE_HARD_MAX_AGE_MILLIS,
        ISOLATE_CONTROL_PLANE_LANE_ENABLED,
        ISOLATE_CONTROL_PLANE_QUEUE_CAPACITY,
        ISOLATE_DEPENDENCY_WORKER_RESERVE,
        ISOLATE_IDLE_TIMEOUT,
        ISOLATE_MAX_LIFETIME,
        ISOLATE_MAX_USER_HEAP_SIZE,
        ISOLATE_QUEUE_DELAY_CONTROL_ENABLED,
        ISOLATE_QUEUE_DELAY_INTERVAL_MILLIS,
        ISOLATE_QUEUE_DELAY_SHED_THRESHOLD_MILLIS,
        ISOLATE_QUEUE_DELAY_TARGET_MILLIS,
        ISOLATE_QUEUE_HARD_MAX_AGE_MILLIS,
        ISOLATE_QUEUE_SIZE,
        MAX_ISOLATE_ACTION_WORKERS,
        REUSE_ISOLATES,
        V8_THREADS,
    },
    log_lines::LogLine,
    query_journal::QueryJournal,
    runtime::{
        shutdown_and_join,
        Runtime,
        SpawnHandle,
        UnixTimestamp,
    },
    schemas::DatabaseSchema,
    static_span,
    types::{
        DeploymentMetadata,
        ModuleEnvironment,
        SchedulerDependencyClass,
        UdfType,
    },
    utils::ensure_utc,
};
use database::{
    shutdown_error,
    Transaction,
};
use deno_core::v8::{
    self,
    V8,
};
use errors::{
    ErrorMetadata,
    ErrorMetadataAnyhowExt,
};
use fastrace::{
    func_path,
    future::FutureExt as _,
    local::LocalSpan,
    Event,
};
use file_storage::TransactionalFileStorage;
use futures::{
    future::{
        self,
        Join,
        Ready,
    },
    stream::{
        self,
        FuturesUnordered,
        PollNext,
        StreamExt,
    },
    TryStreamExt as _,
};
use itertools::Either;
use keybroker::{
    FunctionRunnerKeyBroker,
    Identity,
};
use model::{
    config::types::ModuleConfig,
    environment_variables::types::{
        EnvVarName,
        EnvVarValue,
    },
    modules::module_versions::{
        AnalyzedModule,
        FullModuleSource,
        ModuleSource,
        SourceMap,
    },
    udf_config::types::UdfConfig,
};
use parking_lot::Mutex;
use prometheus::VMHistogram;
use sync_types::CanonicalizedModulePath;
use tokio::sync::{
    mpsc,
    oneshot,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::Instrument as _;
use udf::{
    validation::{
        ValidatedHttpPath,
        ValidatedPathAndArgs,
    },
    ActionCallbacks,
    ActionOutcome,
    EvaluateAppDefinitionsResult,
    FunctionOutcome,
    HttpActionOutcome,
    HttpActionResponseStreamer,
    NestedUdfOutcome,
};
use value::identifier::Identifier;

use crate::{
    concurrency_limiter::ConcurrencyLimiter,
    context_cache::{
        CachedContexts,
        ContextCache,
    },
    isolate::{
        Isolate,
        IsolateHeapStats,
    },
    isolate_queue::{
        new_isolate_queue,
        IsolateQueueConfig,
        IsolateQueueEligibility,
        IsolateQueueExpiredReceiver,
        IsolateQueueLane,
        IsolateQueueMetricsReporter,
        IsolateQueueOutput,
        IsolateQueueReceiver,
        IsolateQueueRejection,
        IsolateQueueSendError,
        IsolateQueueSender,
    },
    metrics::{
        self,
        log_aggregated_heap_stats,
        log_pool_max,
        log_pool_running_count,
        log_worker_stolen,
        queue_timer,
        rejected_before_execution_error,
        RejectedBeforeExecutionReason,
    },
    module_cache::{
        ModuleCache,
        V8ModuleSource,
    },
    ConcurrencyPermit,
};

const ISOLATE_QUEUE_METRICS_LOG_FREQUENCY: Duration = Duration::from_secs(1);

pub const PAUSE_RECREATE_CLIENT: &str = "recreate_client";
pub const PAUSE_REQUEST: &str = "pause_request";
pub const NO_AVAILABLE_WORKERS: &str = "There are no available workers to process the request";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RequestSchedulingProperties {
    unblocks_ancestor: bool,
    can_block_on_descendant: bool,
    is_isolate_action: bool,
    is_control_plane: bool,
}

impl RequestSchedulingProperties {
    fn queue_lane(self) -> IsolateQueueLane {
        assert!(
            !self.is_control_plane || !self.unblocks_ancestor,
            "control-plane isolate requests must not unblock an ancestor"
        );
        if self.is_control_plane {
            IsolateQueueLane::ControlPlane
        } else if self.unblocks_ancestor {
            IsolateQueueLane::Dependency
        } else if self.is_isolate_action {
            IsolateQueueLane::IndependentAction
        } else {
            IsolateQueueLane::Ordinary
        }
    }
}

impl IntoLabel for RequestSchedulingProperties {
    fn as_label(&self) -> &'static str {
        if self.is_control_plane {
            assert!(
                !self.unblocks_ancestor && !self.can_block_on_descendant && !self.is_isolate_action,
                "control-plane isolate scheduling properties are inconsistent"
            );
            return "control_plane";
        }
        match (self.unblocks_ancestor, self.can_block_on_descendant) {
            (false, false) => "independent",
            (false, true) => "descendant_holder",
            (true, false) => "dependency",
            (true, true) => "dependency_descendant_holder",
        }
    }
}

fn log_scheduler_caller_dropped(
    pool_name: &'static str,
    scheduling_properties: RequestSchedulingProperties,
) {
    if !scheduling_properties.is_control_plane {
        return;
    }
    metrics::log_scheduler_request_rejected(
        pool_name,
        scheduling_properties.as_label(),
        "caller_dropped",
    );
    metrics::log_isolate_queue_rejection(
        pool_name,
        scheduling_properties.queue_lane().as_label(),
        "caller_dropped",
    );
}

enum SchedulerQueueSender<RT: Runtime, T> {
    Legacy(CoDelQueueSender<RT, T>),
    LaneDelayControl(IsolateQueueSender<RT, T>),
}

impl<RT: Runtime, T> Clone for SchedulerQueueSender<RT, T> {
    fn clone(&self) -> Self {
        match self {
            Self::Legacy(sender) => Self::Legacy(sender.clone()),
            Self::LaneDelayControl(sender) => Self::LaneDelayControl(sender.clone()),
        }
    }
}

impl<RT: Runtime, T> SchedulerQueueSender<RT, T> {
    fn try_send(&self, item: T, lane: IsolateQueueLane) -> Result<bool, IsolateQueueSendError> {
        match self {
            Self::Legacy(sender) => {
                let result = if lane == IsolateQueueLane::Dependency {
                    sender.try_send_with_reserved_capacity_detailed(item)
                } else {
                    sender.try_send_detailed(item).map(|()| false)
                };
                result.map_err(|error| match error {
                    CoDelQueueSendError::Full => IsolateQueueSendError::QueueFull,
                    CoDelQueueSendError::Closed => IsolateQueueSendError::SchedulerClosed,
                })
            },
            Self::LaneDelayControl(sender) => sender.try_send(item, lane),
        }
    }
}

pub(crate) enum SchedulerQueueReceiver<RT: Runtime, T> {
    Legacy(CoDelQueueReceiver<RT, T>),
    LaneDelayControl(IsolateQueueReceiver<RT, T>),
}

enum SchedulerQueueExpiredReceiver<RT: Runtime, T> {
    Legacy(CoDelQueueExpiredReceiver<RT, T>),
    LaneDelayControl(IsolateQueueExpiredReceiver<RT, T>),
}

impl<RT: Runtime, T> SchedulerQueueReceiver<RT, T> {
    async fn recv_next_selecting(
        &mut self,
        mut select: impl FnMut(&T) -> IsolateQueueEligibility,
    ) -> Option<IsolateQueueOutput<T>> {
        match self {
            Self::Legacy(receiver) => receiver
                .recv_next_selecting(|item| select(item).is_eligible().then_some(()))
                .await
                .map(|(item, expiration)| match expiration {
                    Ok(permit_deadline) => IsolateQueueOutput {
                        item,
                        rejection: None,
                        permit_deadline: Some(permit_deadline),
                        dispatch_sojourn: None,
                    },
                    Err(_) => IsolateQueueOutput {
                        item,
                        rejection: Some(IsolateQueueRejection::HardExpired),
                        permit_deadline: None,
                        dispatch_sojourn: None,
                    },
                }),
            Self::LaneDelayControl(receiver) => receiver.recv_next_selecting(select).await,
        }
    }

    fn metrics_reporter(&self) -> Option<IsolateQueueMetricsReporter<RT, T>> {
        match self {
            Self::Legacy(_) => None,
            Self::LaneDelayControl(receiver) => Some(receiver.metrics_reporter()),
        }
    }

    fn expired_receiver(&self) -> SchedulerQueueExpiredReceiver<RT, T> {
        match self {
            Self::Legacy(receiver) => {
                SchedulerQueueExpiredReceiver::Legacy(receiver.expired_receiver())
            },
            Self::LaneDelayControl(receiver) => {
                SchedulerQueueExpiredReceiver::LaneDelayControl(receiver.expired_receiver())
            },
        }
    }
}

impl<RT: Runtime, T> SchedulerQueueExpiredReceiver<RT, T> {
    async fn recv_next_expired(
        &mut self,
        select: impl FnMut(&T) -> IsolateQueueEligibility,
    ) -> Option<IsolateQueueOutput<T>> {
        match self {
            Self::Legacy(receiver) => receiver.next().await.map(|(item, _)| IsolateQueueOutput {
                item,
                rejection: Some(IsolateQueueRejection::HardExpired),
                permit_deadline: None,
                dispatch_sojourn: None,
            }),
            Self::LaneDelayControl(receiver) => receiver.recv_next_expired(select).await,
        }
    }
}

fn new_scheduler_queue<RT: Runtime, T>(
    rt: RT,
    pool_name: &'static str,
    capacity: usize,
    reserved_capacity: usize,
    delay_control_config: Option<IsolateQueueConfig>,
) -> (SchedulerQueueSender<RT, T>, SchedulerQueueReceiver<RT, T>) {
    let total_capacity = capacity
        .checked_add(reserved_capacity)
        .expect("validated isolate queue capacity must not overflow");
    metrics::log_isolate_queue_capacity(pool_name, "shared_base", capacity);
    metrics::log_isolate_queue_capacity(pool_name, "total", total_capacity);
    if let Some(config) = delay_control_config {
        metrics::log_isolate_queue_capacity(
            pool_name,
            "control_plane_lane",
            config.control_plane_capacity,
        );
        let (sender, receiver) =
            new_isolate_queue(rt, pool_name, capacity, reserved_capacity, config);
        (
            SchedulerQueueSender::LaneDelayControl(sender),
            SchedulerQueueReceiver::LaneDelayControl(receiver),
        )
    } else {
        let (sender, receiver) =
            new_codel_queue_async_with_reserved_capacity(rt, capacity, reserved_capacity);
        metrics::log_isolate_queue_policy(pool_name, "legacy_codel");
        metrics::log_isolate_queue_config(
            pool_name,
            "idle_expiration_millis",
            *CODEL_QUEUE_IDLE_EXPIRATION_MILLIS,
        );
        metrics::log_isolate_queue_config(
            pool_name,
            "congested_expiration_millis",
            *CODEL_QUEUE_CONGESTED_EXPIRATION_MILLIS,
        );
        (
            SchedulerQueueSender::Legacy(sender),
            SchedulerQueueReceiver::Legacy(receiver),
        )
    }
}

enum ExternalPermitWaitOutcome {
    Acquired(ConcurrencyPermit),
    CallerDropped,
    DeadlineElapsed,
}

fn reject_queued_request<RT: Runtime>(
    request: Request<RT>,
    rejection: IsolateQueueRejection,
    pool_name: &'static str,
    control_plane_lane_enabled: bool,
) {
    let scheduling_properties = request.scheduling_properties(control_plane_lane_enabled);
    match rejection {
        IsolateQueueRejection::HardExpired => {
            metrics::log_scheduler_request_expired(pool_name, scheduling_properties.as_label());
        },
        IsolateQueueRejection::DelayControlShed => {
            metrics::log_scheduler_request_rejected(
                pool_name,
                scheduling_properties.as_label(),
                "delay_control_shed",
            );
        },
    }
    request.expire(ExpiredInQueue);
}

async fn wait_for_external_permit<RT: Runtime>(
    rt: &RT,
    limiter: &ConcurrencyLimiter,
    client_id: Arc<String>,
    permit_deadline: tokio::time::Instant,
    response_closed: impl std::future::Future<Output = ()>,
) -> ExternalPermitWaitOutcome {
    let permit = tokio::select! {
        biased;
        () = response_closed => return ExternalPermitWaitOutcome::CallerDropped,
        () = rt.wait(
            permit_deadline.saturating_duration_since(rt.monotonic_now()),
        ) => return ExternalPermitWaitOutcome::DeadlineElapsed,
        permit = limiter.acquire(
            client_id,
            // Initial external requests stay in the low-priority
            // active-JavaScript tier.
            false,
        ) => permit,
    };
    // The timer precedes the permit so an ordinary readiness tie times out
    // without registering capacity. Recheck absolute time after acquisition
    // in case the clock advanced between branch polls or wake delivery lagged.
    if rt.monotonic_now() >= permit_deadline {
        ExternalPermitWaitOutcome::DeadlineElapsed
    } else {
        ExternalPermitWaitOutcome::Acquired(permit)
    }
}

fn validate_control_plane_lane_config(
    enabled: bool,
    control_plane_capacity: usize,
    control_plane_hard_max_age: Duration,
    queue_capacity: usize,
    ordinary_hard_max_age: Duration,
    delay_control_enabled: bool,
    analyze_concurrency: usize,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        analyze_concurrency > 0,
        "ANALYZE_CONCURRENCY must be greater than zero"
    );
    if !enabled {
        return Ok(());
    }
    anyhow::ensure!(
        delay_control_enabled,
        "ISOLATE_CONTROL_PLANE_LANE_ENABLED requires ISOLATE_QUEUE_DELAY_CONTROL_ENABLED=true"
    );
    anyhow::ensure!(
        control_plane_capacity >= analyze_concurrency,
        "ISOLATE_CONTROL_PLANE_QUEUE_CAPACITY must be at least ANALYZE_CONCURRENCY"
    );
    anyhow::ensure!(
        control_plane_capacity <= queue_capacity,
        "ISOLATE_CONTROL_PLANE_QUEUE_CAPACITY must not exceed ISOLATE_QUEUE_SIZE"
    );
    anyhow::ensure!(
        control_plane_hard_max_age > ordinary_hard_max_age,
        "ISOLATE_CONTROL_PLANE_HARD_MAX_AGE_MILLIS must be greater than \
         ISOLATE_QUEUE_HARD_MAX_AGE_MILLIS"
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, Default)]
struct ActiveRequestCounts {
    total: usize,
    independent_actions: usize,
}

impl ActiveRequestCounts {
    fn is_empty(&self) -> bool {
        self.total == 0
    }

    fn increment(&mut self, properties: RequestSchedulingProperties) {
        self.total += 1;
        if properties.is_isolate_action && !properties.unblocks_ancestor {
            self.independent_actions += 1;
        }
    }

    fn decrement(&mut self, properties: RequestSchedulingProperties) {
        self.total = self
            .total
            .checked_sub(1)
            .expect("active request class count underflow");
        if properties.is_isolate_action && !properties.unblocks_ancestor {
            self.independent_actions = self
                .independent_actions
                .checked_sub(1)
                .expect("active independent action count underflow");
        }
    }
}

struct ActiveRequestGuard {
    active_workers: Arc<AtomicUsize>,
    pool_name: &'static str,
    scheduling_properties: RequestSchedulingProperties,
}

fn per_client_worker_capacity(max_workers: usize, max_percent_per_client: usize) -> usize {
    // Split the percentage calculation so a representable worker total cannot
    // overflow before the result is clamped back to that same total.
    let percentage_capacity = if max_percent_per_client >= 100 {
        max_workers
    } else {
        let complete_hundreds = (max_workers / 100) * max_percent_per_client;
        let remainder = (max_workers % 100) * max_percent_per_client;
        complete_hundreds + remainder.div_ceil(100)
    };
    percentage_capacity.max(1).min(max_workers)
}

impl ActiveRequestGuard {
    fn new(
        active_workers: Arc<AtomicUsize>,
        pool_name: &'static str,
        scheduling_properties: RequestSchedulingProperties,
    ) -> Self {
        active_workers.fetch_add(1, Ordering::Relaxed);
        metrics::log_scheduler_active_request_started(
            pool_name,
            scheduling_properties.as_label(),
            scheduling_properties.is_isolate_action,
        );
        Self {
            active_workers,
            pool_name,
            scheduling_properties,
        }
    }
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        self.active_workers
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                count.checked_sub(1)
            })
            .expect("active worker count underflow");
        metrics::log_scheduler_active_request_finished(
            self.pool_name,
            self.scheduling_properties.as_label(),
            self.scheduling_properties.is_isolate_action,
        );
    }
}

struct SchedulerStateSnapshot {
    in_progress_counts_by_client: HashMap<String, ActiveRequestCounts>,
    active_counts: ActiveRequestCounts,
    max_workers: usize,
    base_worker_capacity: usize,
    max_independent_actions: usize,
    max_workers_per_client: usize,
    base_workers_per_client: usize,
}

impl SchedulerStateSnapshot {
    fn can_start_request(&self, properties: RequestSchedulingProperties, client_id: &str) -> bool {
        self.request_eligibility(properties, client_id)
            .is_eligible()
    }

    fn request_eligibility(
        &self,
        properties: RequestSchedulingProperties,
        client_id: &str,
    ) -> IsolateQueueEligibility {
        let client_active_counts = self
            .in_progress_counts_by_client
            .get(client_id)
            .copied()
            .unwrap_or_default();
        IsolateQueueEligibility {
            physical_total: self.active_counts.total >= self.max_workers,
            shared_base: !properties.unblocks_ancestor
                && self.active_counts.total >= self.base_worker_capacity,
            per_client_total: client_active_counts.total >= self.max_workers_per_client,
            per_client_base: !properties.unblocks_ancestor
                && client_active_counts.total >= self.base_workers_per_client,
            independent_action_cap: properties.is_isolate_action
                && !properties.unblocks_ancestor
                && self.active_counts.independent_actions >= self.max_independent_actions,
        }
    }

    fn dependency_dispatch_uses_reserve(&self) -> bool {
        self.active_counts.total >= self.base_worker_capacity
    }
}

fn worker_rejection_reason(
    eligibility: IsolateQueueEligibility,
) -> Option<RejectedBeforeExecutionReason> {
    if eligibility.is_eligible() {
        None
    } else if eligibility.per_client_total || eligibility.per_client_base {
        // Preserve the scheduler's historical per-client-first classification
        // if both a client fence and a global fence became full concurrently.
        Some(RejectedBeforeExecutionReason::PerClientWorkerOverloaded)
    } else {
        Some(RejectedBeforeExecutionReason::WorkerPoolOverloaded)
    }
}

#[derive(Clone)]
pub struct IsolateConfig {
    // Name of isolate pool, used in metrics.
    pub name: &'static str,

    // Typically, the user timeout is configured based on environment. This
    // allows us to set an upper bound to it that we use for tests.
    max_user_timeout: Option<Duration>,

    pub(crate) limiter: ConcurrencyLimiter,
}

impl IsolateConfig {
    pub fn new(name: &'static str, limiter: ConcurrencyLimiter) -> Self {
        Self {
            name,
            max_user_timeout: None,
            limiter,
        }
    }
}

pub struct UdfRequest<RT: Runtime> {
    pub path_and_args: ValidatedPathAndArgs,
    pub udf_type: UdfType,
    pub transaction: Transaction<RT>,
    pub unix_timestamp: UnixTimestamp,
    pub journal: QueryJournal,
    pub context: ExecutionContext,
    pub environment_data: EnvironmentData<RT>,
}

pub struct HttpActionRequest<RT: Runtime> {
    pub http_module_path: ValidatedHttpPath,
    pub routed_path: RoutedHttpPath,
    pub http_request: udf::HttpActionRequest,
    pub transaction: Transaction<RT>,
    pub identity: Identity,
    pub context: ExecutionContext,
    pub environment_data: EnvironmentData<RT>,
}

pub struct ActionRequest<RT: Runtime> {
    pub params: ActionRequestParams,
    pub transaction: Transaction<RT>,
    pub identity: Identity,
    pub context: ExecutionContext,
    pub environment_data: EnvironmentData<RT>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ActionRequestParams {
    pub path_and_args: ValidatedPathAndArgs,
}

#[derive(Clone)]
pub struct EnvironmentData<RT: Runtime> {
    pub key_broker: FunctionRunnerKeyBroker,
    pub default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    pub file_storage: TransactionalFileStorage<RT>,
    pub module_loader: Arc<dyn ModuleCache<RT>>,
    pub deployment: DeploymentMetadata,
}

pub struct Request<RT: Runtime> {
    pub client_id: String,
    pub inner: RequestType<RT>,
    pub parent_trace: EncodedSpan,
    pub scheduler_dependency: SchedulerDependencyClass,
}

impl<RT: Runtime> Request<RT> {
    pub fn new(client_id: String, inner: RequestType<RT>, parent_trace: EncodedSpan) -> Self {
        Self::new_with_scheduler_dependency(
            client_id,
            inner,
            parent_trace,
            SchedulerDependencyClass::Independent,
        )
    }

    pub fn new_with_scheduler_dependency(
        client_id: String,
        inner: RequestType<RT>,
        parent_trace: EncodedSpan,
        scheduler_dependency: SchedulerDependencyClass,
    ) -> Self {
        let request = Self {
            client_id,
            inner,
            parent_trace,
            scheduler_dependency,
        };
        assert!(
            !request.inner.is_control_plane() || !request.scheduler_dependency.unblocks_ancestor(),
            "control-plane isolate requests must not unblock an ancestor"
        );
        request
    }

    pub fn module(&self) -> Option<CanonicalizedComponentModulePath> {
        let function_path = match &self.inner {
            RequestType::Udf { request, .. } => request.path_and_args.path(),
            RequestType::Action { request, .. } => request.params.path_and_args.path(),
            RequestType::HttpAction { request, .. } => request.http_module_path.path(),
            RequestType::Analyze { .. }
            | RequestType::EvaluateSchema { .. }
            | RequestType::EvaluateAuthConfig { .. }
            | RequestType::EvaluateAppDefinitions { .. }
            | RequestType::EvaluateComponentInitializer { .. } => return None,
            #[cfg(test)]
            RequestType::Test { .. } => return None,
        };
        Some(CanonicalizedComponentModulePath {
            component: function_path.component,
            module_path: function_path.udf_path.module().clone(),
        })
    }

    fn scheduling_properties(
        &self,
        control_plane_lane_enabled: bool,
    ) -> RequestSchedulingProperties {
        let (can_block_on_descendant, is_isolate_action) = match &self.inner {
            RequestType::Udf { udf_callback, .. } => (udf_callback.is_some(), false),
            RequestType::Action { .. } | RequestType::HttpAction { .. } => (true, true),
            #[cfg(test)]
            RequestType::Test {
                can_block_on_descendant,
                is_isolate_action,
                ..
            } => (*can_block_on_descendant, *is_isolate_action),
            RequestType::Analyze { .. }
            | RequestType::EvaluateSchema { .. }
            | RequestType::EvaluateAuthConfig { .. }
            | RequestType::EvaluateAppDefinitions { .. }
            | RequestType::EvaluateComponentInitializer { .. } => (false, false),
        };
        RequestSchedulingProperties {
            unblocks_ancestor: self.scheduler_dependency.unblocks_ancestor(),
            can_block_on_descendant,
            is_isolate_action,
            is_control_plane: control_plane_lane_enabled && self.inner.is_control_plane(),
        }
    }
}

pub enum RequestType<RT: Runtime> {
    Udf {
        request: UdfRequest<RT>,
        response: oneshot::Sender<anyhow::Result<(Transaction<RT>, FunctionOutcome)>>,
        queue_timer: Timer<VMHistogram>,
        rng_seed: [u8; 32],
        reactor_depth: usize,
        udf_callback: Option<IsolateClient<RT>>,
        function_started_sender: Option<oneshot::Sender<()>>,
    },
    Action {
        request: ActionRequest<RT>,
        response: oneshot::Sender<anyhow::Result<ActionOutcome>>,
        queue_timer: Timer<VMHistogram>,
        action_callbacks: Arc<dyn ActionCallbacks>,
        fetch_client: Arc<dyn FetchClient>,
        log_line_sender: mpsc::UnboundedSender<LogLine>,
        function_started_sender: Option<oneshot::Sender<()>>,
    },
    HttpAction {
        request: HttpActionRequest<RT>,
        response: oneshot::Sender<anyhow::Result<HttpActionOutcome>>,
        queue_timer: Timer<VMHistogram>,
        action_callbacks: Arc<dyn ActionCallbacks>,
        fetch_client: Arc<dyn FetchClient>,
        log_line_sender: mpsc::UnboundedSender<LogLine>,
        http_response_streamer: HttpActionResponseStreamer,
        function_started_sender: Option<oneshot::Sender<()>>,
    },
    Analyze {
        udf_config: UdfConfig,
        modules: Arc<BTreeMap<CanonicalizedModulePath, Arc<V8ModuleSource>>>,
        to_analyze: CanonicalizedModulePath,
        environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        response: oneshot::Sender<anyhow::Result<Result<AnalyzedModule, JsError>>>,
    },
    EvaluateSchema {
        schema_bundle: ModuleSource,
        source_map: Option<SourceMap>,
        rng_seed: [u8; 32],
        unix_timestamp: UnixTimestamp,
        response: oneshot::Sender<anyhow::Result<DatabaseSchema>>,
    },
    EvaluateAuthConfig {
        auth_config_bundle: ModuleSource,
        source_map: Option<SourceMap>,
        environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        response: oneshot::Sender<anyhow::Result<AuthConfig>>,
    },
    EvaluateAppDefinitions {
        app_definition: ModuleConfig,
        component_definitions: BTreeMap<ComponentDefinitionPath, ModuleConfig>,
        dependency_graph: BTreeSet<(ComponentDefinitionPath, ComponentDefinitionPath)>,
        user_environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
        response: oneshot::Sender<anyhow::Result<EvaluateAppDefinitionsResult>>,
    },
    EvaluateComponentInitializer {
        evaluated_definitions: BTreeMap<ComponentDefinitionPath, ComponentDefinitionMetadata>,
        path: ComponentDefinitionPath,
        definition: ModuleConfig,
        args: BTreeMap<Identifier, Resource>,
        name: ComponentName,
        response: oneshot::Sender<anyhow::Result<BTreeMap<Identifier, Resource>>>,
    },
    #[cfg(test)]
    Test {
        id: usize,
        can_block_on_descendant: bool,
        is_isolate_action: bool,
        is_control_plane: bool,
        fail_worker: bool,
        started: mpsc::UnboundedSender<usize>,
        completion: oneshot::Receiver<()>,
        response: oneshot::Sender<anyhow::Result<()>>,
    },
}

impl<RT: Runtime> RequestType<RT> {
    fn is_control_plane(&self) -> bool {
        match self {
            Self::Analyze { .. }
            | Self::EvaluateSchema { .. }
            | Self::EvaluateAuthConfig { .. }
            | Self::EvaluateAppDefinitions { .. }
            | Self::EvaluateComponentInitializer { .. } => true,
            Self::Udf { .. } | Self::Action { .. } | Self::HttpAction { .. } => false,
            #[cfg(test)]
            Self::Test {
                is_control_plane, ..
            } => *is_control_plane,
        }
    }
}

#[allow(async_fn_in_trait)]
pub trait UdfCallback<RT: Runtime> {
    /// Execute a subfunction in a new V8 context.
    /// This can either be in the same isolate (RunUdf), or another one
    /// (IsolateClient).
    async fn execute_nested_udf(
        self,
        client_id: String,
        udf_request: UdfRequest<RT>,
        rng_seed: [u8; 32],
        reactor_depth: usize,
    ) -> anyhow::Result<(Transaction<RT>, NestedUdfOutcome)>;
}

impl<RT: Runtime, T, U> UdfCallback<RT> for Either<T, U>
where
    T: UdfCallback<RT>,
    U: UdfCallback<RT>,
{
    async fn execute_nested_udf(
        self,
        client_id: String,
        udf_request: UdfRequest<RT>,
        rng_seed: [u8; 32],
        reactor_depth: usize,
    ) -> anyhow::Result<(Transaction<RT>, NestedUdfOutcome)> {
        match self {
            Either::Left(l) => {
                l.execute_nested_udf(client_id, udf_request, rng_seed, reactor_depth)
                    .await
            },
            Either::Right(r) => {
                r.execute_nested_udf(client_id, udf_request, rng_seed, reactor_depth)
                    .await
            },
        }
    }
}

impl<RT: Runtime> Request<RT> {
    fn expire(self, error: ExpiredInQueue) {
        let error = anyhow::anyhow!(error).context(rejected_before_execution_error(
            RejectedBeforeExecutionReason::ExpiredInQueue,
        ));
        self.send_error(error);
    }

    fn reject(self, reason: RejectedBeforeExecutionReason) {
        let error = rejected_before_execution_error(reason).into();
        self.send_error(error);
    }

    fn send_error(self, error: anyhow::Error) {
        match self.inner {
            RequestType::Udf { response, .. } => {
                let _ = response.send(Err(error));
            },
            RequestType::Action { response, .. } => {
                let _ = response.send(Err(error));
            },
            RequestType::HttpAction { response, .. } => {
                let _ = response.send(Err(error));
            },
            RequestType::Analyze { response, .. } => {
                let _ = response.send(Err(error));
            },
            RequestType::EvaluateSchema { response, .. } => {
                let _ = response.send(Err(error));
            },
            RequestType::EvaluateAuthConfig { response, .. } => {
                let _ = response.send(Err(error));
            },
            RequestType::EvaluateAppDefinitions { response, .. } => {
                let _ = response.send(Err(error));
            },
            RequestType::EvaluateComponentInitializer { response, .. } => {
                let _ = response.send(Err(error));
            },
            #[cfg(test)]
            RequestType::Test { response, .. } => {
                let _ = response.send(Err(error));
            },
        }
    }

    fn is_response_closed(&self) -> bool {
        match &self.inner {
            RequestType::Udf { response, .. } => response.is_closed(),
            RequestType::Action { response, .. } => response.is_closed(),
            RequestType::HttpAction { response, .. } => response.is_closed(),
            RequestType::Analyze { response, .. } => response.is_closed(),
            RequestType::EvaluateSchema { response, .. } => response.is_closed(),
            RequestType::EvaluateAuthConfig { response, .. } => response.is_closed(),
            RequestType::EvaluateAppDefinitions { response, .. } => response.is_closed(),
            RequestType::EvaluateComponentInitializer { response, .. } => response.is_closed(),
            #[cfg(test)]
            RequestType::Test { response, .. } => response.is_closed(),
        }
    }

    async fn response_closed(&mut self) {
        match &mut self.inner {
            RequestType::Udf { response, .. } => response.closed().await,
            RequestType::Action { response, .. } => response.closed().await,
            RequestType::HttpAction { response, .. } => response.closed().await,
            RequestType::Analyze { response, .. } => response.closed().await,
            RequestType::EvaluateSchema { response, .. } => response.closed().await,
            RequestType::EvaluateAuthConfig { response, .. } => response.closed().await,
            RequestType::EvaluateAppDefinitions { response, .. } => response.closed().await,
            RequestType::EvaluateComponentInitializer { response, .. } => response.closed().await,
            #[cfg(test)]
            RequestType::Test { response, .. } => response.closed().await,
        }
    }
}

impl<RT: Runtime> Clone for IsolateClient<RT> {
    fn clone(&self) -> Self {
        Self {
            rt: self.rt.clone(),
            handles: self.handles.clone(),
            scheduler: self.scheduler.clone(),
            sender: self.sender.clone(),
            internal_sender: self.internal_sender.clone(),
            pool_name: self.pool_name,
            active_workers: self.active_workers.clone(),
            max_workers: self.max_workers,
            control_plane_lane_enabled: self.control_plane_lane_enabled,
        }
    }
}

pub fn initialize_v8() {
    ensure_utc().expect("Failed to setup timezone");
    static V8_INIT: Once = Once::new();
    V8_INIT.call_once(|| {
        let _s = static_span!("initialize_v8");

        // `deno_core_icudata` internally loads this with proper 16-byte alignment.
        assert!(v8::icu::set_common_data_77(deno_core_icudata::ICU_DATA).is_ok());

        // Calls into `v8::platform::v8__Platform__NewUnprotectedDefaultPlatform`
        // Can configure with...
        // - thread_pool_size (default: zero): number of worker threads for background
        //   jobs, picks a reasonable default based on number of cores if set to zero
        // - idle_task_support (default: false): platform will except idle tasks and
        //   will rely on embedder calling `v8::platform::RunIdleTasks`. Idle tasks are
        //   low-priority tasks that are run with a deadline indicating how long the
        //   scheduler expects to be idle (e.g. unused remainder of a frame budget)
        // - in_process_stack_dumping (default: false)
        // - tracing_controller (default: null): if null, the platform creates a
        //   `v8::platform::TracingController` instance and uses it
        // Why "unprotected"? The "protected" default platform utilizes Memory
        // Protection Keys (PKU), which requires that all threads utilizing V8 are
        // descendents of the thread that initialized V8. Unfortunately, this is
        // not compatible with how Rust tests run and additionally, the version of V8
        // used at the time of this comment has a bug with PKU on certain Intel CPUs.
        // See https://github.com/denoland/rusty_v8/issues/1381
        let platform = v8::new_unprotected_default_platform(*V8_THREADS, false).make_shared();

        // Calls into `v8::V8::InitializePlatform`, sets global platform.
        V8::initialize_platform(platform);

        // TODO: Figure out what V8 uses entropy for and set it here.
        // V8::set_entropy_source(...);

        // Set V8 command line flags.
        // https://github.com/v8/v8/blob/master/src/flags/flag-definitions.h
        let mut argv = vec![
            "".to_owned(), // first arg is ignored
            // See https://github.com/denoland/deno/issues/2544
            "--no-wasm-async-compilation".to_string(),
            // Disable `eval` or `new Function()`.
            "--disallow-code-generation-from-strings".to_string(),
            // We ensure 4MiB of stack space on all of our threads, so
            // tell V8 it can use up to 2MiB of stack space itself. The
            // default is 1MiB. Note that the flag is in KiB (https://github.com/v8/v8/blob/master/src/flags/flag-definitions.h#L1594).
            "--stack-size=2048".to_string(),
            "--js-base-64".to_string(),
        ];
        if let Ok(flags) = env::var("ISOLATE_V8_FLAGS") {
            argv.extend(
                flags
                    .split(" ")
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_owned()),
            );
            tracing::info!("Final V8 flags: {:?}", argv);
        }
        // v8 returns the args that were misunderstood
        let misunderstood = V8::set_flags_from_command_line(argv);
        assert_eq!(misunderstood, vec![""]);

        // Calls into `v8::V8::Initialize`
        V8::initialize();

        crate::udf_runtime::initialize();
    });
}

/// The V8 code all expects to run on a single thread, which makes it ineligible
/// for Tokio's scheduler, which wants the ability to move work across scheduler
/// threads. Instead, we'll manage our V8 threads ourselves.
///
/// [`IsolateClient`] is the "client" entry point to our V8 threads.
pub struct IsolateClient<RT: Runtime> {
    rt: RT,
    handles: Arc<Mutex<Vec<IsolateWorkerHandle>>>,
    scheduler: Arc<Mutex<Option<Box<dyn SpawnHandle>>>>,
    sender: SchedulerQueueSender<RT, Request<RT>>,
    internal_sender: mpsc::UnboundedSender<Request<RT>>,
    pool_name: &'static str,
    /// Shared with the scheduler. Tracks the total number of in-progress
    /// workers across all clients.
    active_workers: Arc<AtomicUsize>,
    max_workers: usize,
    control_plane_lane_enabled: bool,
}

impl<RT: Runtime> IsolateClient<RT> {
    pub fn new<W: IsolateWorker<RT>>(
        rt: RT,
        max_percent_per_client: usize,
        max_isolate_workers: usize,
        isolate_worker: W,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            max_isolate_workers > 0,
            "MAX_ISOLATE_WORKERS must be greater than zero"
        );
        anyhow::ensure!(
            *ISOLATE_QUEUE_SIZE > 0,
            "ISOLATE_QUEUE_SIZE must be greater than zero"
        );
        anyhow::ensure!(
            *ISOLATE_DEPENDENCY_WORKER_RESERVE < max_isolate_workers,
            "ISOLATE_DEPENDENCY_WORKER_RESERVE must be smaller than MAX_ISOLATE_WORKERS"
        );
        anyhow::ensure!(
            (*ISOLATE_QUEUE_SIZE)
                .checked_add(*ISOLATE_DEPENDENCY_WORKER_RESERVE)
                .is_some(),
            "ISOLATE_QUEUE_SIZE plus ISOLATE_DEPENDENCY_WORKER_RESERVE must not overflow"
        );
        let base_worker_capacity = max_isolate_workers - *ISOLATE_DEPENDENCY_WORKER_RESERVE;
        let max_independent_actions = if *MAX_ISOLATE_ACTION_WORKERS == 0 {
            base_worker_capacity
        } else {
            *MAX_ISOLATE_ACTION_WORKERS
        };
        anyhow::ensure!(
            max_independent_actions <= base_worker_capacity,
            "MAX_ISOLATE_ACTION_WORKERS must not exceed shared base isolate worker capacity"
        );
        let queue_config = IsolateQueueConfig::new(
            *ISOLATE_QUEUE_DELAY_TARGET_MILLIS,
            *ISOLATE_QUEUE_DELAY_INTERVAL_MILLIS,
            *ISOLATE_QUEUE_DELAY_SHED_THRESHOLD_MILLIS,
            *ISOLATE_QUEUE_HARD_MAX_AGE_MILLIS,
            *ISOLATE_CONTROL_PLANE_HARD_MAX_AGE_MILLIS,
            *ISOLATE_CONTROL_PLANE_QUEUE_CAPACITY,
        )?;
        let control_plane_lane_enabled = *ISOLATE_CONTROL_PLANE_LANE_ENABLED;
        validate_control_plane_lane_config(
            control_plane_lane_enabled,
            *ISOLATE_CONTROL_PLANE_QUEUE_CAPACITY,
            *ISOLATE_CONTROL_PLANE_HARD_MAX_AGE_MILLIS,
            *ISOLATE_QUEUE_SIZE,
            *ISOLATE_QUEUE_HARD_MAX_AGE_MILLIS,
            *ISOLATE_QUEUE_DELAY_CONTROL_ENABLED,
            *ANALYZE_CONCURRENCY,
        )?;
        let delay_control_config = (*ISOLATE_QUEUE_DELAY_CONTROL_ENABLED).then_some(queue_config);
        let pool_name = isolate_worker.config().name;
        metrics::initialize_capacity_counters(pool_name);
        metrics::log_control_plane_lane_enabled(pool_name, control_plane_lane_enabled);

        initialize_v8();
        // NB: We don't call V8::Dispose or V8::ShutdownPlatform since we just assume a
        // single V8 instance per process and don't need to clean up its
        // resources.
        let (sender, receiver) = new_scheduler_queue::<_, Request<_>>(
            rt.clone(),
            pool_name,
            *ISOLATE_QUEUE_SIZE,
            *ISOLATE_DEPENDENCY_WORKER_RESERVE,
            delay_control_config,
        );
        let (internal_sender, internal_receiver) = mpsc::unbounded_channel();
        let handles = Arc::new(Mutex::new(Vec::new()));
        let handles_clone = handles.clone();
        let active_workers = Arc::new(AtomicUsize::new(0));
        let _active_workers = active_workers.clone();
        let rt_clone = rt.clone();
        let scheduler = rt.spawn("shared_isolate_scheduler", async move {
            // The scheduler thread selects a queued request and an available
            // worker, then sends the request to that worker.
            let scheduler = SharedIsolateScheduler::new(
                rt_clone,
                isolate_worker,
                max_isolate_workers,
                base_worker_capacity,
                max_independent_actions,
                handles_clone,
                max_percent_per_client,
                _active_workers,
                control_plane_lane_enabled,
            );
            scheduler.run(receiver, internal_receiver).await
        });
        Ok(Self {
            rt,
            sender,
            internal_sender,
            pool_name,
            scheduler: Arc::new(Mutex::new(Some(scheduler))),
            handles,
            active_workers,
            max_workers: max_isolate_workers,
            control_plane_lane_enabled,
        })
    }

    /// Returns the total number of isolate workers currently servicing a
    /// request across all clients.
    pub fn active_workers(&self) -> usize {
        self.active_workers.load(Ordering::Relaxed)
    }

    /// Returns the maximum number of isolate workers this client's scheduler
    /// is permitted to create.
    pub fn max_workers(&self) -> usize {
        self.max_workers
    }

    pub fn aggregate_heap_stats(&self) -> IsolateHeapStats {
        let mut total = IsolateHeapStats::default();
        for handle in self.handles.lock().iter() {
            total += handle.heap_stats.get();
        }
        total
    }

    #[fastrace::trace]
    pub async fn execute_udf(
        &self,
        udf_type: UdfType,
        path_and_args: ValidatedPathAndArgs,
        transaction: Transaction<RT>,
        journal: QueryJournal,
        context: ExecutionContext,
        environment_data: EnvironmentData<RT>,
        rng_seed: [u8; 32],
        unix_timestamp: UnixTimestamp,
        reactor_depth: usize,
        instance_name: String,
        function_started_sender: Option<oneshot::Sender<()>>,
        subfunctions_in_same_isolate: bool,
        scheduler_dependency: SchedulerDependencyClass,
    ) -> anyhow::Result<(Transaction<RT>, FunctionOutcome)> {
        let (tx, rx) = oneshot::channel();
        let request = RequestType::Udf {
            request: UdfRequest {
                path_and_args,
                udf_type,
                transaction,
                unix_timestamp,
                journal,
                context,
                environment_data,
            },
            response: tx,
            queue_timer: queue_timer(),
            rng_seed,
            reactor_depth,
            function_started_sender,
            udf_callback: if subfunctions_in_same_isolate {
                None
            } else {
                Some(self.clone())
            },
        };
        self.send_request(Request::new_with_scheduler_dependency(
            instance_name,
            request,
            EncodedSpan::from_parent(),
            scheduler_dependency,
        ))?;
        let (tx, outcome) = Self::receive_response(rx).await??;

        Ok((tx, outcome))
    }

    #[fastrace::trace]
    pub async fn execute_action(
        &self,
        path_and_args: ValidatedPathAndArgs,
        transaction: Transaction<RT>,
        action_callbacks: Arc<dyn ActionCallbacks>,
        fetch_client: Arc<dyn FetchClient>,
        log_line_sender: mpsc::UnboundedSender<LogLine>,
        context: ExecutionContext,
        environment_data: EnvironmentData<RT>,
        instance_name: String,
        function_started_sender: Option<oneshot::Sender<()>>,
        scheduler_dependency: SchedulerDependencyClass,
    ) -> anyhow::Result<ActionOutcome> {
        let (tx, rx) = oneshot::channel();
        let request = RequestType::Action {
            request: ActionRequest {
                params: ActionRequestParams { path_and_args },
                identity: transaction.identity().clone(),
                transaction,
                context,
                environment_data,
            },
            response: tx,
            queue_timer: queue_timer(),
            action_callbacks,
            fetch_client,
            log_line_sender,
            function_started_sender,
        };
        self.send_request(Request::new_with_scheduler_dependency(
            instance_name,
            request,
            EncodedSpan::from_parent(),
            scheduler_dependency,
        ))?;
        match Self::receive_response(rx).await? {
            Ok(outcome) => Ok(outcome),
            Err(e) => Err(recapture_stacktrace(e).await),
        }
    }

    /// Execute an HTTP action.
    /// HTTP actions can run other UDFs, so they take in a ActionCallbacks from
    /// the application layer. This creates a transient reference cycle.
    #[fastrace::trace]
    pub async fn execute_http_action(
        &self,
        http_module_path: ValidatedHttpPath,
        routed_path: RoutedHttpPath,
        http_request: udf::HttpActionRequest,
        identity: Identity,
        action_callbacks: Arc<dyn ActionCallbacks>,
        fetch_client: Arc<dyn FetchClient>,
        log_line_sender: mpsc::UnboundedSender<LogLine>,
        http_response_streamer: HttpActionResponseStreamer,
        transaction: Transaction<RT>,
        context: ExecutionContext,
        environment_data: EnvironmentData<RT>,
        instance_name: String,
        function_started_sender: Option<oneshot::Sender<()>>,
    ) -> anyhow::Result<HttpActionOutcome> {
        let (tx, rx) = oneshot::channel();
        let request = RequestType::HttpAction {
            request: HttpActionRequest {
                http_module_path,
                routed_path,
                http_request,
                identity,
                transaction,
                context,
                environment_data,
            },
            response: tx,
            queue_timer: queue_timer(),
            action_callbacks,
            fetch_client,
            log_line_sender,
            http_response_streamer,
            function_started_sender,
        };
        self.send_request(Request::new(
            instance_name,
            request,
            EncodedSpan::from_parent(),
        ))?;
        match Self::receive_response(rx).await? {
            Ok(outcome) => Ok(outcome),
            Err(e) => Err(recapture_stacktrace(e).await),
        }
    }

    /// Analyze a set of user-defined modules.
    #[fastrace::trace]
    pub async fn analyze(
        &self,
        udf_config: UdfConfig,
        modules: BTreeMap<CanonicalizedModulePath, ModuleConfig>,
        environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        instance_name: String,
    ) -> anyhow::Result<Result<BTreeMap<CanonicalizedModulePath, AnalyzedModule>, JsError>> {
        anyhow::ensure!(
            modules
                .values()
                .all(|m| m.environment == ModuleEnvironment::Isolate),
            "Can only analyze Isolate modules"
        );
        let to_analyze: Vec<_> = modules
            .keys()
            .filter(|path| !path.is_deps())
            .cloned()
            .collect();
        let modules: Arc<BTreeMap<_, _>> = Arc::new(
            modules
                .into_iter()
                .map(|(path, module_config)| {
                    (
                        path,
                        Arc::new(V8ModuleSource::new(FullModuleSource {
                            source: module_config.source,
                            source_map: module_config.source_map,
                        })),
                    )
                })
                .collect(),
        );
        let mut stream = pin!(stream::iter(to_analyze)
            .map(|to_analyze| async {
                let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(2));
                let mut attempt = 1;
                const MAX_ATTEMPTS: u32 = 3;
                loop {
                    let (tx, rx) = oneshot::channel();
                    let request = RequestType::Analyze {
                        modules: modules.clone(),
                        to_analyze: to_analyze.clone(),
                        response: tx,
                        udf_config: udf_config.clone(),
                        environment_variables: environment_variables.clone(),
                    };
                    self.send_request(Request::new(
                        instance_name.clone(),
                        request,
                        EncodedSpan::from_parent(),
                    ))?;
                    match IsolateClient::<RT>::receive_response(rx).await? {
                        Ok(outcome) => return Ok((to_analyze, outcome)),
                        Err(e)
                            if attempt < MAX_ATTEMPTS
                                && (e.is_rejected_before_execution() || e.is_overloaded()) =>
                        {
                            tracing::warn!("Retrying analyze after system error: {e:?}");
                            let wait = backoff.fail(&mut self.rt.rng());
                            self.rt.wait(wait).await;
                            attempt += 1;
                            continue;
                        },
                        Err(e) => return Err(recapture_stacktrace(e).await),
                    }
                }
            })
            .buffer_unordered(*ANALYZE_CONCURRENCY));
        let mut analyzed_modules = BTreeMap::new();
        while let Some((path, r)) = stream.try_next().await? {
            match r {
                Ok(analyzed_module) => analyzed_modules.insert(path, analyzed_module),
                Err(r) => return Ok(Err(r)),
            };
        }
        Ok(Ok(analyzed_modules))
    }

    #[fastrace::trace]
    pub async fn evaluate_app_definitions(
        &self,
        app_definition: ModuleConfig,
        component_definitions: BTreeMap<ComponentDefinitionPath, ModuleConfig>,
        dependency_graph: BTreeSet<(ComponentDefinitionPath, ComponentDefinitionPath)>,
        user_environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
        instance_name: String,
    ) -> anyhow::Result<EvaluateAppDefinitionsResult> {
        anyhow::ensure!(
            app_definition.environment == ModuleEnvironment::Isolate,
            "Can only evaluate Isolate modules"
        );
        anyhow::ensure!(
            component_definitions
                .values()
                .all(|m| m.environment == ModuleEnvironment::Isolate),
            "Can only evaluate Isolate modules"
        );
        let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(2));
        let mut attempt = 1;
        const MAX_ATTEMPTS: u32 = 3;
        loop {
            let (tx, rx) = oneshot::channel();
            let request = RequestType::EvaluateAppDefinitions {
                app_definition: app_definition.clone(),
                component_definitions: component_definitions.clone(),
                dependency_graph: dependency_graph.clone(),
                user_environment_variables: user_environment_variables.clone(),
                system_env_vars: system_env_vars.clone(),
                response: tx,
            };
            self.send_request(Request::new(
                instance_name.clone(),
                request,
                EncodedSpan::from_parent(),
            ))?;
            match IsolateClient::<RT>::receive_response(rx).await? {
                Ok(outcome) => return Ok(outcome),
                Err(e)
                    if attempt < MAX_ATTEMPTS
                        && (e.is_rejected_before_execution() || e.is_overloaded()) =>
                {
                    tracing::warn!("Retrying evaluate_app_definitions after system error: {e:?}");
                    let wait = backoff.fail(&mut self.rt.rng());
                    self.rt.wait(wait).await;
                    attempt += 1;
                    continue;
                },
                Err(e) => return Err(recapture_stacktrace(e).await),
            }
        }
    }

    #[fastrace::trace]
    pub async fn evaluate_component_initializer(
        &self,
        evaluated_definitions: BTreeMap<ComponentDefinitionPath, ComponentDefinitionMetadata>,
        path: ComponentDefinitionPath,
        definition: ModuleConfig,
        args: BTreeMap<Identifier, Resource>,
        name: ComponentName,
        instance_name: String,
    ) -> anyhow::Result<BTreeMap<Identifier, Resource>> {
        let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(2));
        let mut attempt = 1;
        const MAX_ATTEMPTS: u32 = 3;
        loop {
            let (tx, rx) = oneshot::channel();
            let request = RequestType::EvaluateComponentInitializer {
                evaluated_definitions: evaluated_definitions.clone(),
                path: path.clone(),
                definition: definition.clone(),
                args: args.clone(),
                name: name.clone(),
                response: tx,
            };
            self.send_request(Request::new(
                instance_name.clone(),
                request,
                EncodedSpan::from_parent(),
            ))?;
            match IsolateClient::<RT>::receive_response(rx).await? {
                Ok(outcome) => return Ok(outcome),
                Err(e)
                    if attempt < MAX_ATTEMPTS
                        && (e.is_rejected_before_execution() || e.is_overloaded()) =>
                {
                    tracing::warn!(
                        "Retrying evaluate_component_initializer after system error: {e:?}"
                    );
                    let wait = backoff.fail(&mut self.rt.rng());
                    self.rt.wait(wait).await;
                    attempt += 1;
                    continue;
                },
                Err(e) => return Err(recapture_stacktrace(e).await),
            }
        }
    }

    #[fastrace::trace]
    pub async fn evaluate_schema(
        &self,
        schema_bundle: ModuleSource,
        source_map: Option<SourceMap>,
        rng_seed: [u8; 32],
        unix_timestamp: UnixTimestamp,
        instance_name: String,
    ) -> anyhow::Result<DatabaseSchema> {
        let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(2));
        let mut attempt = 1;
        const MAX_ATTEMPTS: u32 = 3;
        loop {
            let (tx, rx) = oneshot::channel();
            let request = RequestType::EvaluateSchema {
                schema_bundle: schema_bundle.clone(),
                source_map: source_map.clone(),
                rng_seed,
                unix_timestamp,
                response: tx,
            };
            self.send_request(Request::new(
                instance_name.clone(),
                request,
                EncodedSpan::from_parent(),
            ))?;
            match IsolateClient::<RT>::receive_response(rx).await? {
                Ok(outcome) => return Ok(outcome),
                Err(e)
                    if attempt < MAX_ATTEMPTS
                        && (e.is_rejected_before_execution() || e.is_overloaded()) =>
                {
                    tracing::warn!("Retrying evaluate_schema after system error: {e:?}");
                    let wait = backoff.fail(&mut self.rt.rng());
                    self.rt.wait(wait).await;
                    attempt += 1;
                    continue;
                },
                Err(e) => return Err(recapture_stacktrace(e).await),
            }
        }
    }

    #[fastrace::trace]
    pub async fn evaluate_auth_config(
        &self,
        auth_config_bundle: ModuleSource,
        source_map: Option<SourceMap>,
        environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        explanation: &str,
        instance_name: String,
    ) -> anyhow::Result<AuthConfig> {
        let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(2));
        let mut attempt = 1;
        const MAX_ATTEMPTS: u32 = 3;
        let result = loop {
            let (tx, rx) = oneshot::channel();
            let request = RequestType::EvaluateAuthConfig {
                auth_config_bundle: auth_config_bundle.clone(),
                source_map: source_map.clone(),
                environment_variables: environment_variables.clone(),
                response: tx,
            };
            self.send_request(Request::new(
                instance_name.clone(),
                request,
                EncodedSpan::from_parent(),
            ))?;
            match IsolateClient::<RT>::receive_response(rx).await? {
                Ok(outcome) => return Ok(outcome),
                Err(e)
                    if attempt < MAX_ATTEMPTS
                        && (e.is_rejected_before_execution() || e.is_overloaded()) =>
                {
                    tracing::warn!("Retrying evaluate_auth_config after system error: {e:?}");
                    let wait = backoff.fail(&mut self.rt.rng());
                    self.rt.wait(wait).await;
                    attempt += 1;
                    continue;
                },
                Err(e) => break e,
            }
        };
        let is_env_var_error = result
            .to_string()
            .starts_with("Uncaught Error: Environment variable");
        let err = recapture_stacktrace(result).await;
        if err.is_rejected_before_execution() {
            return Err(err);
        }
        let error = err.to_string();
        if is_env_var_error {
            // Reformatting the underlying message to be nicer
            // here. Since we lost the underlying ErrorMetadata into the JSError,
            // we do some string matching instead. CX-4531
            Err(anyhow::anyhow!(ErrorMetadata::bad_request(
                "AuthConfigMissingEnvironmentVariable",
                error.trim_start_matches("Uncaught Error: ").to_string(),
            )))
        } else {
            Err(anyhow::anyhow!(ErrorMetadata::bad_request(
                "InvalidAuthConfig",
                format!("{explanation}: {error}"),
            )))
        }
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        // Stop the scheduler before snapshotting worker handles. Otherwise it
        // can create a worker after the snapshot and escape this shutdown.
        let scheduler = self.scheduler.lock().take();
        if let Some(scheduler) = scheduler {
            shutdown_and_join(scheduler).await?;
        }
        {
            let handles: Vec<_> = {
                let mut handles = self.handles.lock();
                for handle in &mut *handles {
                    handle.handle.shutdown();
                }
                handles.drain(..).collect()
            };
            for handle in handles.into_iter() {
                shutdown_and_join(handle.handle).await?;
            }
        }
        Ok(())
    }

    fn send_request(&self, request: Request<RT>) -> anyhow::Result<()> {
        let scheduling_properties = request.scheduling_properties(self.control_plane_lane_enabled);
        let send_result = self
            .sender
            .try_send(request, scheduling_properties.queue_lane());
        let used_reserved_capacity = send_result.map_err(|error| {
            metrics::log_scheduler_request_rejected(
                self.pool_name,
                scheduling_properties.as_label(),
                error.as_label(),
            );
            if error == IsolateQueueSendError::SchedulerClosed {
                return shutdown_error();
            }
            metrics::execute_full_error().into()
        })?;
        metrics::log_scheduler_request_enqueued(self.pool_name, scheduling_properties.as_label());
        if used_reserved_capacity {
            assert!(
                scheduling_properties.unblocks_ancestor,
                "only dependency requests may enqueue in isolate queue reserve"
            );
            metrics::log_scheduler_dependency_queue_reserve_enqueue(self.pool_name);
        }
        Ok(())
    }

    async fn receive_response<T>(rx: oneshot::Receiver<T>) -> anyhow::Result<T> {
        // The only reason a oneshot response channel wil be dropped prematurely if the
        // isolate worker is shutting down.
        rx.await.map_err(|_| shutdown_error())
    }
}

impl<RT: Runtime> UdfCallback<RT> for &IsolateClient<RT> {
    async fn execute_nested_udf(
        self,
        client_id: String,
        udf_request: UdfRequest<RT>,
        rng_seed: [u8; 32],
        reactor_depth: usize,
    ) -> anyhow::Result<(Transaction<RT>, NestedUdfOutcome)> {
        let subquery_path = udf_request.path_and_args.path().clone();
        let (tx, rx) = oneshot::channel();
        let request = RequestType::Udf {
            request: udf_request,
            response: tx,
            queue_timer: queue_timer(),
            rng_seed,
            reactor_depth,
            function_started_sender: None,
            udf_callback: Some(self.clone()),
        };
        let request = Request::new_with_scheduler_dependency(
            client_id,
            request,
            EncodedSpan::from_parent(),
            // This queue exists only for a separately scheduled nested UDF
            // whose caller retains an isolate worker.
            SchedulerDependencyClass::UnblocksAncestor,
        );
        let scheduling_properties = request.scheduling_properties(self.control_plane_lane_enabled);
        self.internal_sender
            .send(request)
            .ok()
            .context("scheduler shut down")?;
        metrics::log_scheduler_request_enqueued(self.pool_name, scheduling_properties.as_label());
        let (tx, outcome) = IsolateClient::<RT>::receive_response(rx).await??;
        let outcome = match outcome {
            FunctionOutcome::Query(outcome) | FunctionOutcome::Mutation(outcome) => {
                NestedUdfOutcome {
                    observed_identity: outcome.observed_identity,
                    observed_rng: outcome.observed_rng,
                    observed_time: outcome.observed_time,
                    log_lines: outcome.log_lines,
                    audit_log_lines: outcome.audit_log_lines,
                    journal: outcome.journal,
                    result: match outcome.result {
                        Ok(t) => Ok(t.unpack().map_err(|e| {
                            e.wrap_error_message(|msg| {
                                format!(
                                    "Subquery {} return value invalid: {msg}",
                                    subquery_path.for_logging().debug_str(),
                                )
                            })
                        })?),
                        Err(e) => Err(e),
                    },
                    syscall_trace: outcome.syscall_trace,
                }
            },
            FunctionOutcome::Action(_) | FunctionOutcome::HttpAction(_) => {
                anyhow::bail!("nested udf must be query or mutation")
            },
        };
        Ok((tx, outcome))
    }
}

pub struct SharedIsolateScheduler<RT: Runtime, W: IsolateWorker<RT>> {
    rt: RT,
    worker: W,
    /// Vec of channels for sending work to individual workers.
    worker_senders: Vec<mpsc::Sender<IsolateWorkerRequest<RT>>>,
    /// Map from client_id to stack of workers (implemented with a deque). The
    /// most recently used worker for a given client is at the front of the
    /// deque. These workers were previously used by this client, but may
    /// safely be "stolen" for use by another client. A worker with a
    /// `last_used_ts` older than `ISOLATE_IDLE_TIMEOUT` has already been
    /// recreated and there will be no penalty for reassigning this worker to a
    /// new client.
    available_workers: HashMap<String, VecDeque<IdleWorkerState>>,
    /// Set of futures awaiting a response from an active worker.
    in_progress_workers:
        FuturesUnordered<Join<oneshot::Receiver<IdleWorkerInfo>, Ready<ActiveWorkerState>>>,
    /// Counts active request properties per client. Should only contain a key
    /// if at least one request is active for that client.
    in_progress_counts_by_client: HashMap<String, ActiveRequestCounts>,
    in_progress_counts: ActiveRequestCounts,
    /// Externally visible active-worker accounting. The worker request owns the
    /// corresponding guard so failure paths cannot leave this count stale.
    active_workers: Arc<AtomicUsize>,
    /// The max number of workers this scheduler is permitted to create.
    max_workers: usize,
    /// Shared capacity available to every request class.
    base_worker_capacity: usize,
    /// Maximum independent V8 and HTTP actions retaining workers.
    max_independent_actions: usize,
    handles: Arc<Mutex<Vec<IsolateWorkerHandle>>>,
    max_workers_per_client: usize,
    base_workers_per_client: usize,
    control_plane_lane_enabled: bool,
}

pub struct IdleWorkerInfo {
    cached_contexts: Arc<CachedContexts>,
}

pub struct IsolateWorkerRequest<RT: Runtime> {
    request: Request<RT>,
    permit: ConcurrencyPermit,
    done: oneshot::Sender<IdleWorkerInfo>,
    active_request_guard: ActiveRequestGuard,
}

struct IdleWorkerState {
    worker_id: usize,
    last_used_ts: tokio::time::Instant,
    info: IdleWorkerInfo,
}
struct ActiveWorkerState {
    worker_id: usize,
    client_id: String,
    scheduling_properties: RequestSchedulingProperties,
}

impl<RT: Runtime, W: IsolateWorker<RT>> SharedIsolateScheduler<RT, W> {
    pub fn new(
        rt: RT,
        worker: W,
        max_workers: usize,
        base_worker_capacity: usize,
        max_independent_actions: usize,
        handles: Arc<Mutex<Vec<IsolateWorkerHandle>>>,
        max_percent_per_client: usize,
        active_workers: Arc<AtomicUsize>,
        control_plane_lane_enabled: bool,
    ) -> Self {
        let dependency_reserve = max_workers - base_worker_capacity;
        let max_workers_per_client =
            per_client_worker_capacity(max_workers, max_percent_per_client);
        // Preserve the existing percentage-derived per-client total. Carve the
        // dependency overflow out of that total just as the global policy carves
        // R out of T, while retaining one shared base slot for small clients.
        let effective_dependency_reserve =
            dependency_reserve.min(max_workers_per_client.saturating_sub(1));
        let base_workers_per_client = max_workers_per_client - effective_dependency_reserve;
        Self {
            rt,
            worker,
            worker_senders: Vec::new(),
            in_progress_workers: FuturesUnordered::new(),
            in_progress_counts_by_client: HashMap::new(),
            in_progress_counts: ActiveRequestCounts::default(),
            active_workers,
            available_workers: HashMap::new(),
            max_workers,
            base_worker_capacity,
            max_independent_actions,
            handles,
            max_workers_per_client,
            base_workers_per_client,
            control_plane_lane_enabled,
        }
    }

    fn handle_completed_worker(
        &mut self,
        completed_worker: ActiveWorkerState,
        info: IdleWorkerInfo,
    ) {
        let new_count = match self
            .in_progress_counts_by_client
            .remove_entry(&completed_worker.client_id)
        {
            Some((client_id, mut counts)) => {
                counts.decrement(completed_worker.scheduling_properties);
                let new_count = counts.total;
                if !counts.is_empty() {
                    self.in_progress_counts_by_client.insert(client_id, counts);
                }
                new_count
            },
            _ => panic!(
                "Inconsistent state in `in_progress_counts_by_client` map; the count of active \
                 workers for client {} must be >= 1",
                completed_worker.client_id
            ),
        };
        self.in_progress_counts
            .decrement(completed_worker.scheduling_properties);
        log_pool_running_count(
            self.worker.config().name,
            new_count,
            &completed_worker.client_id,
        );

        self.available_workers
            .entry(completed_worker.client_id)
            .or_default()
            .push_front(IdleWorkerState {
                worker_id: completed_worker.worker_id,
                last_used_ts: self.rt.monotonic_now(),
                info,
            });
    }

    fn state_snapshot(&self) -> SchedulerStateSnapshot {
        SchedulerStateSnapshot {
            in_progress_counts_by_client: self.in_progress_counts_by_client.clone(),
            active_counts: self.in_progress_counts,
            max_workers: self.max_workers,
            base_worker_capacity: self.base_worker_capacity,
            max_independent_actions: self.max_independent_actions,
            max_workers_per_client: self.max_workers_per_client,
            base_workers_per_client: self.base_workers_per_client,
        }
    }

    pub(crate) async fn run(
        mut self,
        receiver: SchedulerQueueReceiver<RT, Request<RT>>,
        mut internal_receiver: mpsc::UnboundedReceiver<Request<RT>>,
    ) {
        log_pool_max(self.worker.config().name, self.max_workers);
        metrics::log_scheduler_capacity(self.worker.config().name, "physical", self.max_workers);
        metrics::log_scheduler_capacity(
            self.worker.config().name,
            "base",
            self.base_worker_capacity,
        );
        metrics::log_scheduler_capacity(
            self.worker.config().name,
            "independent_action",
            self.max_independent_actions,
        );
        let mut report_stats = self.rt.wait(*HEAP_WORKER_REPORT_INTERVAL_SECONDS);
        let queue_metrics_reporter = receiver.metrics_reporter();
        // Queue receipt is serialized through the active-permit wait below.
        // Keep a non-consuming expiry companion live so other retained entries
        // still honor their own absolute deadlines during that wait.
        let mut external_expired_receiver = receiver.expired_receiver();
        let limiter = self.worker.config().limiter.clone();
        let scheduler_state = Arc::new(Mutex::new(self.state_snapshot()));
        let control_plane_lane_enabled = self.control_plane_lane_enabled;
        let selection_state = scheduler_state.clone();
        let pool_name = self.worker.config().name;
        let external_rt = self.rt.clone();
        let external_limiter = limiter.clone();
        let external_request_stream = stream::unfold(receiver, move |mut receiver| {
            let selection_state = selection_state.clone();
            async move {
                let output = receiver
                    .recv_next_selecting(|request| {
                        selection_state.lock().request_eligibility(
                            request.scheduling_properties(control_plane_lane_enabled),
                            &request.client_id,
                        )
                    })
                    .await?;
                Some((output, receiver))
            }
        })
        .filter_map(move |output| {
            let limiter = external_limiter.clone();
            let rt = external_rt.clone();
            async move {
                let IsolateQueueOutput {
                    item: mut request,
                    rejection,
                    permit_deadline,
                    dispatch_sojourn,
                } = output;
                let scheduling_properties =
                    request.scheduling_properties(control_plane_lane_enabled);
                if let Some(rejection) = rejection {
                    reject_queued_request(
                        request,
                        rejection,
                        pool_name,
                        control_plane_lane_enabled,
                    );
                    return None;
                }
                if request.is_response_closed() {
                    log_scheduler_caller_dropped(pool_name, scheduling_properties);
                    return None;
                }
                let permit_deadline = permit_deadline
                    .expect("non-rejected external request must retain its queue deadline");
                let client_id = Arc::new(request.client_id.clone());
                let permit = match wait_for_external_permit(
                    &rt,
                    &limiter,
                    client_id,
                    permit_deadline,
                    request.response_closed(),
                )
                .await
                {
                    ExternalPermitWaitOutcome::Acquired(permit) => permit,
                    ExternalPermitWaitOutcome::CallerDropped => {
                        log_scheduler_caller_dropped(pool_name, scheduling_properties);
                        return None;
                    },
                    ExternalPermitWaitOutcome::DeadlineElapsed => {
                        metrics::log_scheduler_request_expired(
                            pool_name,
                            scheduling_properties.as_label(),
                        );
                        request.reject(RejectedBeforeExecutionReason::InitialPermitTimeout);
                        return None;
                    },
                };
                Some((request, permit, dispatch_sojourn))
            }
        });
        let internal_selection_state = scheduler_state.clone();
        let mut pending_internal: VecDeque<Request<RT>> = VecDeque::new();
        let internal_request_stream = stream::poll_fn(move |cx| {
            loop {
                // Internal admission is intentionally unbounded, but one client at
                // its per-client total must not hide an eligible callback for
                // another client. Discard abandoned callbacks and otherwise keep
                // FIFO order among requests eligible in the current snapshot.
                pending_internal.retain(|request| !request.is_response_closed());
                let selected = {
                    let state = internal_selection_state.lock();
                    pending_internal.iter().position(|request| {
                        state.can_start_request(
                            request.scheduling_properties(control_plane_lane_enabled),
                            &request.client_id,
                        )
                    })
                };
                if let Some(index) = selected {
                    return Poll::Ready(Some(
                        pending_internal
                            .remove(index)
                            .expect("selected internal request must still be buffered"),
                    ));
                }
                match internal_receiver.poll_recv(cx) {
                    Poll::Ready(Some(request)) => pending_internal.push_back(request),
                    Poll::Ready(None) if pending_internal.is_empty() => {
                        return Poll::Ready(None);
                    },
                    Poll::Ready(None) => return Poll::Pending,
                    Poll::Pending => return Poll::Pending,
                }
            }
        })
        .filter_map(move |mut request| {
            let limiter = limiter.clone();
            async move {
                // Internal requests (for nested UDFs) get priority because they
                // block workers. Upstream gives this permit wait no queue
                // deadline because the nested UDF cannot be retried safely.
                let client_id = request.client_id.clone();
                let permit = tokio::select! {
                    biased;
                    () = request.response_closed() => return None,
                    permit = limiter.acquire(client_id.into(), true) => permit,
                };
                Some((request, permit, None))
            }
        });
        let mut next_request_stream = pin!(stream::select_with_strategy(
            internal_request_stream,
            external_request_stream,
            |&mut ()| PollNext::Left
        ));
        let expiration_selection_state = scheduler_state.clone();
        let mut external_expiration_receiver_open = true;
        let mut report_queue_metrics = if queue_metrics_reporter.is_some() {
            Either::Left(self.rt.wait(ISOLATE_QUEUE_METRICS_LOG_FREQUENCY))
        } else {
            // Do not even construct the periodic runtime timer on the legacy
            // path; the opt-in policy must not add default scheduler wakeups.
            Either::Right(future::pending())
        };
        loop {
            *scheduler_state.lock() = self.state_snapshot();
            tokio::select! {
                biased;
                completed_worker = self.in_progress_workers.next(),
                if !self.in_progress_workers.is_empty() => {
                    let Some((Ok(info), completed_worker)) = completed_worker
                    else {
                        tracing::warn!(
                            "Worker has shut down uncleanly. Shutting down {} scheduler.",
                            self.worker.config().name
                        );
                        return;
                    };
                    self.handle_completed_worker(completed_worker, info);
                },
                _ = &mut report_queue_metrics, if queue_metrics_reporter.is_some() => {
                    queue_metrics_reporter
                        .as_ref()
                        .expect("lane queue metrics reporter must exist when enabled")
                        .report();
                    report_queue_metrics =
                        Either::Left(self.rt.wait(ISOLATE_QUEUE_METRICS_LOG_FREQUENCY));
                },
                request = next_request_stream.next() => {
                    let Some((request, permit, dispatch_sojourn)) = request else {
                        tracing::warn!(
                            "Request sender went away; {} scheduler shutting down",
                            self.worker.config().name,
                        );
                        return;
                    };
                    let scheduling_properties =
                        request.scheduling_properties(self.control_plane_lane_enabled);
                    if request.is_response_closed() {
                        log_scheduler_caller_dropped(
                            self.worker.config().name,
                            scheduling_properties,
                        );
                        continue;
                    }
                    let dispatch_state = self.state_snapshot();
                    let dependency_reserve_dispatch = scheduling_properties.unblocks_ancestor
                        && dispatch_state.dependency_dispatch_uses_reserve();
                    let worker_id = match self.get_worker(&request, &dispatch_state) {
                        Ok(worker_id) => worker_id,
                        Err(reason) => {
                            metrics::log_scheduler_request_rejected(
                                self.worker.config().name,
                                scheduling_properties.as_label(),
                                "no_worker",
                            );
                            request.reject(reason);
                            continue;
                        },
                    };
                    let (done_sender, done_receiver) = oneshot::channel();
                    let client_id = request.client_id.clone();
                    let st = ActiveWorkerState {
                        client_id: client_id.clone(),
                        worker_id,
                        scheduling_properties,
                    };
                    let active_request_guard = ActiveRequestGuard::new(
                        self.active_workers.clone(),
                        self.worker.config().name,
                        scheduling_properties,
                    );
                    if self.worker_senders[worker_id]
                        .try_send(IsolateWorkerRequest {
                            request,
                            permit,
                            done: done_sender,
                            active_request_guard,
                        })
                        .is_err()
                    {
                        // Available worker should have an empty channel, so if we fail
                        // here it must be shut down. We should shut down too.
                        tracing::warn!(
                            "Worker died or dropped channel. Shutting down {} scheduler.",
                            self.worker.config().name
                        );
                        return;
                    }
                    if let Some((lane, sojourn)) = dispatch_sojourn {
                        metrics::log_isolate_queue_sojourn(
                            self.worker.config().name,
                            lane.as_label(),
                            sojourn,
                        );
                    }
                    // Record scheduler-owned state only after the worker
                    // accepted the request. The worker message owns externally
                    // visible active accounting and clears it on every drop path.
                    self.in_progress_workers
                        .push(future::join(done_receiver, future::ready(st)));
                    let entry = self
                        .in_progress_counts_by_client
                        .entry(client_id.clone())
                        .or_default();
                    entry.increment(scheduling_properties);
                    let client_running_count = entry.total;
                    self.in_progress_counts.increment(scheduling_properties);
                    log_pool_running_count(
                        self.worker.config().name,
                        client_running_count,
                        &client_id,
                    );
                    metrics::log_scheduler_request_dispatched(
                        self.worker.config().name,
                        scheduling_properties.as_label(),
                    );
                    if dependency_reserve_dispatch {
                        metrics::log_scheduler_dependency_reserve_dispatch(
                            self.worker.config().name,
                        );
                    }
                },
                // Preserve the established internal-ingress priority instead
                // of draining a bounded batch of expired external entries
                // ahead of a ready nested callback. The consuming external
                // receiver itself still checks hard expiry before selection.
                expired = external_expired_receiver.recv_next_expired(|request| {
                    expiration_selection_state.lock().request_eligibility(
                        request.scheduling_properties(control_plane_lane_enabled),
                        &request.client_id,
                    )
                }), if external_expiration_receiver_open => {
                    match expired {
                        Some(IsolateQueueOutput {
                            item: request,
                            rejection: Some(rejection @ IsolateQueueRejection::HardExpired),
                            permit_deadline: None,
                            dispatch_sojourn: None,
                        }) => reject_queued_request(
                            request,
                            rejection,
                            self.worker.config().name,
                            self.control_plane_lane_enabled,
                        ),
                        Some(_) => unreachable!(
                            "isolate expiry companion returned a non-expiration output"
                        ),
                        // A selected request may still be waiting for its permit
                        // after the queue itself closes, so companion closure is
                        // not a scheduler-shutdown signal.
                        None => external_expiration_receiver_open = false,
                    }
                },
                _ = &mut report_stats => {
                    let heap_stats = self.aggregate_heap_stats();
                    log_aggregated_heap_stats(&heap_stats);
                    report_stats = self.rt.wait(*HEAP_WORKER_REPORT_INTERVAL_SECONDS);
                },
            }
        }
    }

    /// Find a worker for the given `client_id`.`
    /// Returns an error if no worker can be allocated for this client.
    ///
    /// Note that the returned worker id is removed from the
    /// `self.available_workers` state, so the caller is responsible for using
    /// the worker and returning it back to `self.available_workers` after it is
    /// done.
    fn get_worker(
        &mut self,
        request: &Request<RT>,
        state: &SchedulerStateSnapshot,
    ) -> Result<usize, RejectedBeforeExecutionReason> {
        let client_id = request.client_id.as_str();
        let eligibility = state.request_eligibility(
            request.scheduling_properties(self.control_plane_lane_enabled),
            client_id,
        );
        if let Some(reason) = worker_rejection_reason(eligibility) {
            tracing::warn!(
                "Selected request no longer satisfies {} scheduler capacity constraints",
                self.worker.config().name,
            );
            return Err(reason);
        }
        // Try to find an existing worker for this client.
        if let Some((client_id, mut workers)) = self.available_workers.remove_entry(client_id) {
            // If there is a worker with an appropriate reusable context, pick that one
            // first.
            // This skips workers with inapplicable reused contexts.
            // TODO: just promote the saved context's module path into the hashmap key.
            let worker = workers
                .extract_if(.., |worker| {
                    worker.info.cached_contexts.can_serve_request(request)
                })
                .next();
            if !workers.is_empty() {
                self.available_workers.insert(client_id, workers);
            }
            if let Some(worker) = worker {
                return Ok(worker.worker_id);
            }
            // Otherwise all the workers have cached contexts for other modules
            // that we don't want to clobber; try to assign a new worker
            // instead.
            // It's possible that one of our own workers will end up being the
            // least-recently-used one.
        }
        // If we've recently started up and haven't yet created `max_workers` threads,
        // create a new worker instead of "stealing" some other client's worker.
        if self.worker_senders.len() < self.max_workers {
            let new_worker = self.worker.clone();
            let heap_stats = SharedIsolateHeapStats::new();
            let heap_stats_ = heap_stats.clone();
            let (work_sender, work_receiver) = mpsc::channel(1);
            let handle = self.rt.spawn_thread("isolate", move || {
                new_worker.service_requests(work_receiver, heap_stats_)
            });
            self.worker_senders.push(work_sender);
            self.handles
                .lock()
                .push(IsolateWorkerHandle { handle, heap_stats });
            tracing::info!(
                "Created {} isolate worker {}",
                self.worker.config().name,
                self.worker_senders.len() - 1
            );
            return Ok(self.worker_senders.len() - 1);
        }
        // No existing worker for this client and we've already started the max number
        // of workers -- just grab the least recently used worker. This worker is least
        // likely to be reused by its' previous client.
        let Some((key, workers)) = self
            .available_workers
            .iter_mut()
            .min_by_key(|(_, workers)| {
                workers
                    .back()
                    .expect("Available worker map should never contain an empty list")
                    .last_used_ts
            })
        else {
            // No available workers. This should be unreachable since we don't
            // pull a request from the queue until there is a free worker.
            tracing::error!("unexpected: couldn't find a worker?");
            return Err(RejectedBeforeExecutionReason::WorkerPoolOverloaded);
        };
        let worker = workers
            .pop_back()
            .expect("Available worker map should never contain an empty list");
        log_worker_stolen(worker.last_used_ts.elapsed());
        if workers.is_empty() {
            // This variable shadowing drops the mutable reference to
            // `self.available_workers`.
            let key = key.clone();
            self.available_workers.remove(&key);
        }
        Ok(worker.worker_id)
    }

    fn aggregate_heap_stats(&self) -> IsolateHeapStats {
        let mut total = IsolateHeapStats::default();
        for handle in self.handles.lock().iter() {
            total += handle.heap_stats.get();
        }
        total
    }
}

pub struct IsolateWorkerHandle {
    pub handle: Box<dyn SpawnHandle>,
    heap_stats: SharedIsolateHeapStats,
}

#[derive(Clone)]
pub struct SharedIsolateHeapStats(Arc<Mutex<IsolateHeapStats>>);

impl SharedIsolateHeapStats {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(IsolateHeapStats::default())))
    }

    pub(crate) fn get(&self) -> IsolateHeapStats {
        *self.0.lock()
    }

    pub fn store(&self, stats: IsolateHeapStats) {
        *self.0.lock() = stats;
    }
}

#[async_trait(?Send)]
pub trait IsolateWorker<RT: Runtime>: Clone + Send + 'static {
    async fn service_requests(
        self,
        reqs: mpsc::Receiver<IsolateWorkerRequest<RT>>,
        heap_stats: SharedIsolateHeapStats,
    ) {
        let IsolateConfig {
            max_user_timeout, ..
        } = self.config();
        let mut reqs = std::pin::pin!(ReceiverStream::new(reqs).peekable());
        let mut ready: Option<(oneshot::Sender<_>, ActiveRequestGuard)> = None;
        'recreate_isolate: loop {
            let mut last_client_id: Option<String> = None;
            let mut last_request: Option<String> = None;
            let mut isolate = Isolate::new(
                self.rt().clone(),
                *max_user_timeout,
                *ISOLATE_MAX_USER_HEAP_SIZE,
            );
            isolate.set_heap_stats_handle(heap_stats.clone());
            let mut context_cache = ContextCache::new();
            heap_stats.store(isolate.heap_stats());
            loop {
                context_cache.prepare(isolate.isolate());
                // Check again whether the isolate has enough free heap memory
                // before starting the next request
                if let Some(debug_str) = &last_request
                    && should_recreate_isolate(&mut isolate, &mut context_cache, debug_str)
                {
                    continue 'recreate_isolate;
                }
                heap_stats.store(isolate.heap_stats());
                if let Some((done, active_request_guard)) = ready.take() {
                    // Inform the scheduler that this thread is ready to accept a new request.
                    // Clear externally visible activity before exposing the
                    // reusable worker to the scheduler.
                    drop(active_request_guard);
                    let _ = done.send(IdleWorkerInfo {
                        cached_contexts: context_cache.cached_contexts().clone(),
                    });
                }
                tokio::select! {
                    // If the isolate isn't "tainted", no need to wait for the idle timeout.
                    _ = self.rt().wait(*ISOLATE_IDLE_TIMEOUT), if last_client_id.is_some() => {
                        tracing::debug!("Restarting isolate for {last_client_id:?} due to idle timeout");
                        metrics::log_recreate_isolate("idle_timeout");
                        continue 'recreate_isolate;
                    },
                    // First peek the request to decide if we need to make a new isolate.
                    req = reqs.as_mut().peek() => {
                        let Some(req) = req else {
                            return;
                        };
                        let req = &req.request;
                        let reused = last_client_id.is_some();
                        // If we receive a request from a different client (i.e. a different instance),
                        // recreate the isolate. We don't allow an isolate to be reused
                        // across clients for security isolation.
                        if last_client_id.get_or_insert_with(|| {
                            req.client_id.clone()
                        }) != &req.client_id {
                            let pause_client = self.rt().pause_client();
                            pause_client.wait(PAUSE_RECREATE_CLIENT).await;
                            tracing::debug!("Restarting isolate due to client change, previous: {:?}, new: {:?}", last_client_id, req.client_id);
                            metrics::log_recreate_isolate("client_id_changed");
                            continue 'recreate_isolate;
                        } else if reused {
                            tracing::debug!("Reusing isolate for client {}", req.client_id);
                        }
                        // Ok, we're ready to accept the request for real.
                        let Some(IsolateWorkerRequest {
                            request: req,
                            permit,
                            done,
                            active_request_guard,
                        }) = reqs.next().await else { return };
                        // Note that we won't reply to `done` until
                        // `context_cache` has been prepared. This improves
                        // latency in the common case since requests will be
                        // routed to a thread that has a context ready to go.
                        ready = Some((done, active_request_guard));
                        let root = initialize_root_from_parent(
                            func_path!(),
                            req.parent_trace.clone(),
                        );
                        root.add_property(|| ("reused_isolate", reused.as_label()));
                        let (debug_str, isolate_clean) = self
                            .handle_request(
                                &mut isolate,
                                &mut context_cache,
                                req,
                                permit,
                            )
                            .in_span(root)
                            .await;
                        if !isolate_clean {
                            continue 'recreate_isolate;
                        }
                        last_request = Some(debug_str);
                    }
                }
            }
        }
    }

    #[fastrace::trace]
    async fn handle_request(
        &self,
        isolate: &mut Isolate<RT>,
        context_cache: &mut ContextCache,
        request: Request<RT>,
        permit: ConcurrencyPermit,
    ) -> (String, bool) {
        let pause_client = self.rt().pause_client();
        pause_client.wait(PAUSE_REQUEST).await;
        let client_id = &request.client_id;
        // Set the scope to be tagged with the client_id just for the duration of
        // handling the request. It would be nice to get sentry::with_scope to work, but
        // it uses a synchronous callback and we need `report_error` in the future to
        // have the client_id tag.
        sentry::configure_scope(|scope| scope.set_tag("client_id", client_id));
        // Also add the tag to tracing so it shows up in DataDog logs.
        let span = tracing::info_span!("isolate_worker_handle_request", instance_name = client_id);
        let result = self
            .handle_request_inner(isolate, context_cache, request, permit)
            .instrument(span)
            .await;
        sentry::configure_scope(|scope| scope.remove_tag("client_id"));
        result
    }

    async fn handle_request_inner(
        &self,
        isolate: &mut Isolate<RT>,
        context_cache: &mut ContextCache,
        request: Request<RT>,
        permit: ConcurrencyPermit,
    ) -> (String, bool);

    fn config(&self) -> &IsolateConfig;
    fn rt(&self) -> &RT;
}

pub fn should_recreate_isolate<RT: Runtime>(
    isolate: &mut Isolate<RT>,
    context_cache: &mut ContextCache,
    last_executed: &str,
) -> bool {
    if !*REUSE_ISOLATES {
        metrics::log_recreate_isolate("env_disabled");
        return true;
    }
    if let Err(e) = isolate.check_isolate_clean(context_cache) {
        tracing::debug!(
            "Restarting Isolate {}: {e:?}, last request: {last_executed:?}",
            e.reason()
        );
        metrics::log_recreate_isolate(e.reason());
        LocalSpan::add_event(
            Event::new("isolate_unclean")
                .with_property(|| ("reason", e.reason()))
                .with_property(|| ("last_executed", last_executed.to_owned())),
        );
        return true;
    }

    if isolate.created().elapsed() > *ISOLATE_MAX_LIFETIME {
        metrics::log_recreate_isolate("max_lifetime");
        return true;
    }

    false
}
#[cfg(test)]
mod tests {
    use std::{
        collections::{
            BTreeMap,
            HashMap,
        },
        future::Future,
        pin::Pin,
        sync::{
            atomic::{
                AtomicUsize,
                Ordering,
            },
            Arc,
        },
        time::{
            Duration,
            SystemTime,
        },
    };

    use ::metrics::IntoLabel as _;
    use common::{
        components::{
            ComponentDefinitionPath,
            ComponentName,
        },
        fastrace_helpers::EncodedSpan,
        knobs::CODEL_QUEUE_IDLE_EXPIRATION_MILLIS,
        pause::PauseClient,
        runtime::{
            shutdown_and_join,
            Runtime,
            SpawnHandle,
            UnixTimestamp,
        },
        types::{
            ModuleEnvironment,
            SchedulerDependencyClass,
        },
    };
    use errors::ErrorMetadataAnyhowExt as _;
    use futures::future::FusedFuture;
    use parking_lot::Mutex;
    use runtime::prod::ProdRuntime;
    use semver::Version;
    use sync_types::{
        CanonicalizedModulePath,
        ModulePath,
    };
    use tokio::sync::{
        mpsc,
        oneshot,
    };

    use super::{
        new_scheduler_queue,
        per_client_worker_capacity,
        validate_control_plane_lane_config,
        wait_for_external_permit,
        worker_rejection_reason,
        ActiveRequestCounts,
        ExternalPermitWaitOutcome,
        IdleWorkerInfo,
        IsolateConfig,
        IsolateWorker,
        IsolateWorkerHandle,
        IsolateWorkerRequest,
        Request,
        RequestSchedulingProperties,
        RequestType,
        SchedulerQueueReceiver,
        SchedulerQueueSender,
        SchedulerStateSnapshot,
        SharedIsolateHeapStats,
        SharedIsolateScheduler,
    };
    use crate::{
        context_cache::ContextCache,
        isolate::Isolate,
        isolate_queue::{
            IsolateQueueConfig,
            IsolateQueueEligibility,
            IsolateQueueLane,
        },
        metrics::RejectedBeforeExecutionReason,
        ConcurrencyLimiter,
        ConcurrencyPermit,
    };

    const SCHEDULER_TEST_TIMEOUT: Duration = Duration::from_secs(5);

    #[derive(Clone)]
    struct SchedulerTestRuntime {
        inner: ProdRuntime,
        now: Arc<Mutex<tokio::time::Instant>>,
    }

    impl SchedulerTestRuntime {
        fn new(inner: ProdRuntime) -> Self {
            Self {
                inner,
                now: Arc::new(Mutex::new(tokio::time::Instant::now())),
            }
        }

        fn advance(&self, duration: Duration) {
            let mut now = self.now.lock();
            *now += duration;
        }
    }

    impl Runtime for SchedulerTestRuntime {
        fn wait(
            &self,
            duration: Duration,
        ) -> Pin<Box<dyn FusedFuture<Output = ()> + Send + 'static>> {
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
            f: F,
        ) -> Box<dyn SpawnHandle> {
            self.inner.spawn_thread(name, f)
        }

        fn system_time(&self) -> SystemTime {
            self.inner.system_time()
        }

        fn monotonic_now(&self) -> tokio::time::Instant {
            *self.now.lock()
        }

        fn rng(&self) -> Box<dyn rand::RngCore> {
            self.inner.rng()
        }

        fn pause_client(&self) -> PauseClient {
            self.inner.pause_client()
        }
    }

    #[derive(Clone)]
    struct TestIsolateWorker {
        rt: SchedulerTestRuntime,
        config: IsolateConfig,
    }

    #[async_trait::async_trait(?Send)]
    impl IsolateWorker<SchedulerTestRuntime> for TestIsolateWorker {
        async fn service_requests(
            self,
            mut requests: mpsc::Receiver<IsolateWorkerRequest<SchedulerTestRuntime>>,
            _heap_stats: SharedIsolateHeapStats,
        ) {
            let cached_contexts = ContextCache::new().cached_contexts().clone();
            while let Some(IsolateWorkerRequest {
                request,
                permit,
                done,
                active_request_guard,
            }) = requests.recv().await
            {
                let (id, started, completion, response, fail_worker) = match request.inner {
                    RequestType::Test {
                        id,
                        can_block_on_descendant: _,
                        is_isolate_action: _,
                        is_control_plane: _,
                        fail_worker,
                        started,
                        completion,
                        response,
                    } => (id, started, completion, response, fail_worker),
                    _ => panic!("fake worker received a production request"),
                };
                let _ = started.send(id);
                if fail_worker {
                    return;
                }
                let _ = completion.await;
                let _ = response.send(Ok(()));
                drop(permit);
                drop(active_request_guard);
                if done
                    .send(IdleWorkerInfo {
                        cached_contexts: cached_contexts.clone(),
                    })
                    .is_err()
                {
                    return;
                }
            }
        }

        async fn handle_request_inner(
            &self,
            _isolate: &mut Isolate<SchedulerTestRuntime>,
            _context_cache: &mut ContextCache,
            _req: Request<SchedulerTestRuntime>,
            _permit: ConcurrencyPermit,
        ) -> (String, bool) {
            unreachable!("fake worker overrides service_requests")
        }

        fn config(&self) -> &IsolateConfig {
            &self.config
        }

        fn rt(&self) -> &SchedulerTestRuntime {
            &self.rt
        }
    }

    #[derive(Clone, Copy)]
    enum TestRequestKind {
        Dependency,
        DependencyHolder,
        Independent,
        Action,
        ControlPlane,
        WorkerFailure,
    }

    struct PendingTestRequest {
        request: Request<SchedulerTestRuntime>,
        completion: oneshot::Sender<()>,
        response: oneshot::Receiver<anyhow::Result<()>>,
    }

    struct InFlightTestRequest {
        completion: oneshot::Sender<()>,
        response: oneshot::Receiver<anyhow::Result<()>>,
    }

    impl InFlightTestRequest {
        async fn complete(self) {
            self.completion.send(()).expect("fake worker stopped early");
            self.response
                .await
                .expect("fake worker response dropped")
                .expect("fake request failed");
        }

        async fn expect_dropped(self) {
            drop(self.completion);
            assert!(
                self.response.await.is_err(),
                "failed scheduler request unexpectedly returned a response"
            );
        }

        async fn expect_expired(self) {
            drop(self.completion);
            let error = self
                .response
                .await
                .expect("expired request response dropped")
                .expect_err("expired scheduler request unexpectedly succeeded");
            assert!(error.is_rejected_before_execution());
            assert_eq!(error.short_msg(), "ExpiredInQueue");
        }
    }

    fn test_request(
        id: usize,
        kind: TestRequestKind,
        started: mpsc::UnboundedSender<usize>,
    ) -> PendingTestRequest {
        let (completion, completion_receiver) = oneshot::channel();
        let (response_sender, response) = oneshot::channel();
        let inner = RequestType::Test {
            id,
            can_block_on_descendant: matches!(
                kind,
                TestRequestKind::DependencyHolder | TestRequestKind::Action
            ),
            is_isolate_action: matches!(kind, TestRequestKind::Action),
            is_control_plane: matches!(kind, TestRequestKind::ControlPlane),
            fail_worker: matches!(kind, TestRequestKind::WorkerFailure),
            started,
            completion: completion_receiver,
            response: response_sender,
        };
        let request = match kind {
            TestRequestKind::Dependency | TestRequestKind::DependencyHolder => {
                Request::new_with_scheduler_dependency(
                    "deployment".to_string(),
                    inner,
                    EncodedSpan::empty(),
                    SchedulerDependencyClass::UnblocksAncestor,
                )
            },
            TestRequestKind::Independent
            | TestRequestKind::Action
            | TestRequestKind::ControlPlane
            | TestRequestKind::WorkerFailure => {
                Request::new("deployment".to_string(), inner, EncodedSpan::empty())
            },
        };
        PendingTestRequest {
            request,
            completion,
            response,
        }
    }

    fn control_plane_request_types() -> Vec<RequestType<SchedulerTestRuntime>> {
        let udf_config = model::udf_config::types::UdfConfig {
            server_version: Version::new(1, 0, 0),
            import_phase_rng_seed: [0; 32],
            import_phase_unix_timestamp: UnixTimestamp::from_nanos(0),
        };
        let module_config = model::config::types::ModuleConfig {
            path: "test.js".parse::<ModulePath>().unwrap(),
            source: model::modules::module_versions::ModuleSource::new(""),
            source_map: None,
            environment: ModuleEnvironment::Isolate,
        };
        let module_path = "test.js".parse::<CanonicalizedModulePath>().unwrap();

        let (analyze_response, analyze_receiver) = oneshot::channel();
        drop(analyze_receiver);
        let (schema_response, schema_receiver) = oneshot::channel();
        drop(schema_receiver);
        let (auth_response, auth_receiver) = oneshot::channel();
        drop(auth_receiver);
        let (app_definitions_response, app_definitions_receiver) = oneshot::channel();
        drop(app_definitions_receiver);
        let (component_initializer_response, component_initializer_receiver) = oneshot::channel();
        drop(component_initializer_receiver);

        vec![
            RequestType::Analyze {
                udf_config,
                modules: Arc::new(BTreeMap::new()),
                to_analyze: module_path,
                environment_variables: BTreeMap::new(),
                response: analyze_response,
            },
            RequestType::EvaluateSchema {
                schema_bundle: model::modules::module_versions::ModuleSource::new(""),
                source_map: None,
                rng_seed: [0; 32],
                unix_timestamp: UnixTimestamp::from_nanos(0),
                response: schema_response,
            },
            RequestType::EvaluateAuthConfig {
                auth_config_bundle: model::modules::module_versions::ModuleSource::new(""),
                source_map: None,
                environment_variables: BTreeMap::new(),
                response: auth_response,
            },
            RequestType::EvaluateAppDefinitions {
                app_definition: module_config.clone(),
                component_definitions: BTreeMap::new(),
                dependency_graph: Default::default(),
                user_environment_variables: BTreeMap::new(),
                system_env_vars: BTreeMap::new(),
                response: app_definitions_response,
            },
            RequestType::EvaluateComponentInitializer {
                evaluated_definitions: BTreeMap::new(),
                path: ComponentDefinitionPath::root(),
                definition: module_config,
                args: BTreeMap::new(),
                name: ComponentName::min(),
                response: component_initializer_response,
            },
        ]
    }

    struct SchedulerHarness {
        sender: SchedulerQueueSender<SchedulerTestRuntime, Request<SchedulerTestRuntime>>,
        started_sender: mpsc::UnboundedSender<usize>,
        started: mpsc::UnboundedReceiver<usize>,
        pending_scheduler: Option<(
            SchedulerTestRuntime,
            SharedIsolateScheduler<SchedulerTestRuntime, TestIsolateWorker>,
            SchedulerQueueReceiver<SchedulerTestRuntime, Request<SchedulerTestRuntime>>,
            mpsc::UnboundedReceiver<Request<SchedulerTestRuntime>>,
        )>,
        internal_sender: mpsc::UnboundedSender<Request<SchedulerTestRuntime>>,
        scheduler: Option<Box<dyn SpawnHandle>>,
        worker_handles: Arc<Mutex<Vec<IsolateWorkerHandle>>>,
        active_workers: Arc<AtomicUsize>,
        control_plane_lane_enabled: bool,
    }

    impl SchedulerHarness {
        fn new(
            rt: SchedulerTestRuntime,
            max_workers: usize,
            base_worker_capacity: usize,
            max_independent_actions: usize,
            max_percent_per_client: usize,
            queue_capacity: usize,
        ) -> Self {
            Self::new_with_policy(
                rt,
                max_workers,
                base_worker_capacity,
                max_independent_actions,
                max_percent_per_client,
                queue_capacity,
                None,
                false,
            )
        }

        fn new_control_plane(
            rt: SchedulerTestRuntime,
            max_workers: usize,
            base_worker_capacity: usize,
            max_independent_actions: usize,
            max_percent_per_client: usize,
            queue_capacity: usize,
        ) -> Self {
            let queue_config = IsolateQueueConfig::new(
                Duration::from_millis(10),
                Duration::from_millis(100),
                Duration::from_millis(20),
                Duration::from_secs(1),
                Duration::from_secs(30),
                queue_capacity,
            )
            .unwrap();
            Self::new_with_policy(
                rt,
                max_workers,
                base_worker_capacity,
                max_independent_actions,
                max_percent_per_client,
                queue_capacity,
                Some(queue_config),
                true,
            )
        }

        #[allow(clippy::too_many_arguments)]
        fn new_with_policy(
            rt: SchedulerTestRuntime,
            max_workers: usize,
            base_worker_capacity: usize,
            max_independent_actions: usize,
            max_percent_per_client: usize,
            queue_capacity: usize,
            queue_config: Option<IsolateQueueConfig>,
            control_plane_lane_enabled: bool,
        ) -> Self {
            let (sender, receiver) = new_scheduler_queue::<_, Request<_>>(
                rt.clone(),
                "scheduler_test",
                queue_capacity,
                max_workers - base_worker_capacity,
                queue_config,
            );
            let (started_sender, started) = mpsc::unbounded_channel();
            let (internal_sender, internal_receiver) = mpsc::unbounded_channel();
            let worker_handles = Arc::new(Mutex::new(Vec::new()));
            let active_workers = Arc::new(AtomicUsize::new(0));
            let scheduler = SharedIsolateScheduler::new(
                rt.clone(),
                TestIsolateWorker {
                    rt: rt.clone(),
                    config: IsolateConfig::new(pool_name, ConcurrencyLimiter::unlimited()),
                },
                max_workers,
                base_worker_capacity,
                max_independent_actions,
                worker_handles.clone(),
                max_percent_per_client,
                active_workers.clone(),
                control_plane_lane_enabled,
            );
            Self {
                sender,
                started_sender,
                started,
                pending_scheduler: Some((rt, scheduler, receiver, internal_receiver)),
                internal_sender,
                scheduler: None,
                worker_handles,
                active_workers,
                control_plane_lane_enabled,
            }
        }

        fn start(&mut self) {
            let (rt, scheduler, receiver, internal_receiver) = self
                .pending_scheduler
                .take()
                .expect("test scheduler started twice");
            self.scheduler =
                Some(rt.spawn("scheduler_test", scheduler.run(receiver, internal_receiver)));
        }

        fn set_concurrency_limiter(&mut self, limiter: ConcurrencyLimiter) {
            self.pending_scheduler
                .as_mut()
                .expect("test scheduler already started")
                .1
                .worker
                .config
                .limiter = limiter;
        }

        fn enqueue(&self, request: Request<SchedulerTestRuntime>) {
            let properties = request.scheduling_properties(self.control_plane_lane_enabled);
            self.sender
                .try_send(request, properties.queue_lane())
                .expect("test queue including dependency reserve is full");
        }

        fn lane_queue_len(&self) -> usize {
            match &self.sender {
                SchedulerQueueSender::LaneDelayControl(sender) => sender.len(),
                SchedulerQueueSender::Legacy(_) => {
                    panic!("lane queue depth requested for legacy scheduler")
                },
            }
        }

        fn enqueue_test(&self, id: usize, kind: TestRequestKind) -> InFlightTestRequest {
            self.enqueue_test_for_client(id, kind, "deployment")
        }

        fn enqueue_internal_test(&self, id: usize) -> InFlightTestRequest {
            self.enqueue_internal_test_for_client(id, "deployment")
        }

        fn enqueue_internal_test_for_client(
            &self,
            id: usize,
            client_id: &str,
        ) -> InFlightTestRequest {
            let PendingTestRequest {
                mut request,
                completion,
                response,
            } = test_request(id, TestRequestKind::Dependency, self.started_sender.clone());
            request.client_id = client_id.to_string();
            self.internal_sender
                .send(request)
                .expect("test internal scheduler queue is closed");
            InFlightTestRequest {
                completion,
                response,
            }
        }

        fn enqueue_test_for_client(
            &self,
            id: usize,
            kind: TestRequestKind,
            client_id: &str,
        ) -> InFlightTestRequest {
            let PendingTestRequest {
                mut request,
                completion,
                response,
            } = test_request(id, kind, self.started_sender.clone());
            request.client_id = client_id.to_string();
            self.enqueue(request);
            InFlightTestRequest {
                completion,
                response,
            }
        }

        async fn next_started(&mut self) -> usize {
            tokio::time::timeout(SCHEDULER_TEST_TIMEOUT, self.started.recv())
                .await
                .expect("scheduler made no progress")
                .expect("fake worker start channel closed")
        }

        async fn shutdown(self) {
            let Self {
                sender,
                internal_sender,
                started_sender,
                started: _,
                pending_scheduler,
                scheduler,
                worker_handles,
                active_workers: _,
                control_plane_lane_enabled: _,
            } = self;
            assert!(
                pending_scheduler.is_none(),
                "test scheduler was never started"
            );
            drop(sender);
            drop(internal_sender);
            drop(started_sender);
            tokio::time::timeout(
                SCHEDULER_TEST_TIMEOUT,
                scheduler.expect("test scheduler was never started").join(),
            )
            .await
            .expect("scheduler did not shut down")
            .expect("scheduler task failed");
            let handles = std::mem::take(&mut *worker_handles.lock());
            for handle in handles {
                tokio::time::timeout(SCHEDULER_TEST_TIMEOUT, shutdown_and_join(handle.handle))
                    .await
                    .expect("fake worker did not shut down")
                    .expect("fake worker task failed");
            }
        }
    }

    fn properties(
        unblocks_ancestor: bool,
        can_block_on_descendant: bool,
        is_isolate_action: bool,
    ) -> RequestSchedulingProperties {
        RequestSchedulingProperties {
            unblocks_ancestor,
            can_block_on_descendant,
            is_isolate_action,
            is_control_plane: false,
        }
    }

    fn snapshot(
        global_active: ActiveRequestCounts,
        client_active: ActiveRequestCounts,
    ) -> SchedulerStateSnapshot {
        SchedulerStateSnapshot {
            in_progress_counts_by_client: HashMap::from([(
                "deployment".to_string(),
                client_active,
            )]),
            active_counts: global_active,
            max_workers: 6,
            base_worker_capacity: 5,
            max_independent_actions: 3,
            max_workers_per_client: 6,
            base_workers_per_client: 5,
        }
    }

    #[test]
    fn dependency_and_descendant_capability_are_independent() {
        assert_eq!(properties(false, false, false).as_label(), "independent");
        assert_eq!(
            properties(false, true, false).as_label(),
            "descendant_holder"
        );
        assert_eq!(properties(true, false, false).as_label(), "dependency");
        assert_eq!(
            properties(true, true, false).as_label(),
            "dependency_descendant_holder"
        );
    }

    #[test]
    fn exact_backend_control_plane_variants_use_the_lane_only_when_enabled() {
        let requests = control_plane_request_types();
        assert_eq!(requests.len(), 5);
        for inner in requests {
            assert!(inner.is_control_plane());
            let request = Request::new("deployment".to_string(), inner, EncodedSpan::empty());
            assert!(request.is_response_closed());
            let enabled = request.scheduling_properties(true);
            assert_eq!(enabled.queue_lane(), IsolateQueueLane::ControlPlane);
            assert_eq!(enabled.as_label(), "control_plane");
            assert!(!enabled.can_block_on_descendant);
            assert!(!enabled.is_isolate_action);

            let disabled = request.scheduling_properties(false);
            assert_eq!(disabled.queue_lane(), IsolateQueueLane::Ordinary);
            assert_eq!(disabled.as_label(), "independent");
        }
    }

    #[test]
    #[should_panic(expected = "control-plane isolate requests must not unblock an ancestor")]
    fn control_plane_request_rejects_dependency_ancestry() {
        let inner = control_plane_request_types()
            .pop()
            .expect("control-plane request fixture must not be empty");
        let _ = Request::new_with_scheduler_dependency(
            "deployment".to_string(),
            inner,
            EncodedSpan::empty(),
            SchedulerDependencyClass::UnblocksAncestor,
        );
    }

    #[test]
    fn enabled_control_plane_configuration_requires_lane_policy_and_bounded_values() {
        let valid = || {
            validate_control_plane_lane_config(
                true,
                16,
                Duration::from_secs(30),
                2000,
                Duration::from_secs(5),
                true,
                4,
            )
        };
        assert!(valid().is_ok());
        assert!(validate_control_plane_lane_config(
            false,
            1,
            Duration::from_millis(1),
            1,
            Duration::from_secs(5),
            false,
            4,
        )
        .is_ok());
        assert!(validate_control_plane_lane_config(
            false,
            1,
            Duration::from_millis(1),
            1,
            Duration::from_secs(5),
            false,
            0,
        )
        .is_err());
        assert!(validate_control_plane_lane_config(
            true,
            16,
            Duration::from_secs(30),
            2000,
            Duration::from_secs(5),
            false,
            4,
        )
        .is_err());
        assert!(validate_control_plane_lane_config(
            true,
            3,
            Duration::from_secs(30),
            2000,
            Duration::from_secs(5),
            true,
            4,
        )
        .is_err());
        assert!(validate_control_plane_lane_config(
            true,
            2001,
            Duration::from_secs(30),
            2000,
            Duration::from_secs(5),
            true,
            4,
        )
        .is_err());
        assert!(validate_control_plane_lane_config(
            true,
            16,
            Duration::from_secs(5),
            2000,
            Duration::from_secs(5),
            true,
            4,
        )
        .is_err());
    }

    #[test]
    fn external_permit_notification_at_deadline_is_rejected() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        rt.clone()
            .block_on("external_permit_deadline_tie_test", async move {
                let clock = SchedulerTestRuntime::new(rt);
                let limiter = ConcurrencyLimiter::new(1);
                let held = limiter.acquire(Arc::new("active".to_owned()), false).await;
                let deadline = clock.monotonic_now() + Duration::from_secs(60);
                let mut wait = Box::pin(wait_for_external_permit(
                    &clock,
                    &limiter,
                    Arc::new("waiting".to_owned()),
                    deadline,
                    futures::future::pending(),
                ));
                assert!(futures::poll!(wait.as_mut()).is_pending());

                clock.advance(Duration::from_secs(60));
                drop(held);
                assert!(matches!(
                    futures::poll!(wait.as_mut()),
                    std::task::Poll::Ready(ExternalPermitWaitOutcome::DeadlineElapsed)
                ));
                assert_eq!(limiter.active_permits(), 0);
            });
    }

    #[test]
    fn control_plane_uses_shared_base_and_not_action_or_dependency_reserve() {
        let active = ActiveRequestCounts {
            total: 5,
            independent_actions: 3,
        };
        let state = snapshot(active, active);
        let control_plane = RequestSchedulingProperties {
            unblocks_ancestor: false,
            can_block_on_descendant: false,
            is_isolate_action: false,
            is_control_plane: true,
        };
        assert_eq!(control_plane.queue_lane(), IsolateQueueLane::ControlPlane);
        assert!(!state.can_start_request(control_plane, "deployment"));
        assert!(state.can_start_request(properties(true, false, false), "deployment"));

        let mut counts = ActiveRequestCounts::default();
        counts.increment(control_plane);
        assert_eq!(counts.total, 1);
        assert_eq!(counts.independent_actions, 0);
    }

    #[test]
    fn total_base_occupancy_preserves_dependency_overflow() {
        // Four dependencies and one root occupy all five shared base slots.
        let active = ActiveRequestCounts {
            total: 5,
            independent_actions: 0,
        };
        let state = snapshot(active, active);
        assert!(!state.can_start_request(properties(false, false, false), "deployment"));
        assert!(state.can_start_request(properties(true, false, false), "deployment"));
        assert!(state.dependency_dispatch_uses_reserve());
    }

    #[test]
    fn dependencies_use_base_before_overflow() {
        let active = ActiveRequestCounts {
            total: 4,
            independent_actions: 0,
        };
        let state = snapshot(active, active);
        assert!(state.can_start_request(properties(false, false, false), "deployment"));
        assert!(state.can_start_request(properties(true, false, false), "deployment"));
        assert!(!state.dependency_dispatch_uses_reserve());
    }

    #[test]
    fn total_capacity_blocks_every_request_class() {
        let active = ActiveRequestCounts {
            total: 6,
            independent_actions: 0,
        };
        let state = snapshot(active, active);
        assert!(!state.can_start_request(properties(false, false, false), "deployment"));
        assert!(!state.can_start_request(properties(true, false, false), "deployment"));
    }

    #[test]
    fn stale_worker_recheck_distinguishes_global_and_per_client_overload() {
        assert!(matches!(
            worker_rejection_reason(IsolateQueueEligibility {
                physical_total: true,
                ..Default::default()
            }),
            Some(RejectedBeforeExecutionReason::WorkerPoolOverloaded)
        ));
        assert!(matches!(
            worker_rejection_reason(IsolateQueueEligibility {
                physical_total: true,
                per_client_total: true,
                ..Default::default()
            }),
            Some(RejectedBeforeExecutionReason::PerClientWorkerOverloaded)
        ));
    }

    #[test]
    fn per_client_capacity_handles_large_worker_totals_without_overflow() {
        assert_eq!(
            per_client_worker_capacity(usize::MAX, 50),
            usize::MAX.div_ceil(2)
        );
        assert_eq!(per_client_worker_capacity(usize::MAX, 100), usize::MAX);
        assert_eq!(
            per_client_worker_capacity(usize::MAX, usize::MAX),
            usize::MAX
        );
    }

    #[test]
    fn per_client_capacity_uses_total_occupancy() {
        let global = ActiveRequestCounts {
            total: 4,
            independent_actions: 0,
        };
        let client = ActiveRequestCounts {
            total: 2,
            independent_actions: 0,
        };
        let mut state = snapshot(global, client);
        state.base_workers_per_client = 2;
        state.max_workers_per_client = 3;
        assert!(!state.can_start_request(properties(false, false, false), "deployment"));
        assert!(state.can_start_request(properties(true, false, false), "deployment"));
        assert!(!state.dependency_dispatch_uses_reserve());

        let client_at_total = ActiveRequestCounts {
            total: 3,
            independent_actions: 0,
        };
        let mut state = snapshot(global, client_at_total);
        state.base_workers_per_client = 2;
        state.max_workers_per_client = 3;
        assert!(!state.can_start_request(properties(false, false, false), "deployment"));
        assert!(!state.can_start_request(properties(true, false, false), "deployment"));
    }

    #[test]
    fn action_cap_does_not_cap_queries_or_dependency_actions() {
        let active = ActiveRequestCounts {
            total: 3,
            independent_actions: 3,
        };
        let state = snapshot(active, active);
        assert!(!state.can_start_request(properties(false, true, true), "deployment"));
        assert!(state.can_start_request(properties(false, true, false), "deployment"));
        assert!(state.can_start_request(properties(true, true, true), "deployment"));
    }

    #[test]
    fn zero_workers_are_rejected_at_construction() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let isolate_worker = crate::isolate_worker::FunctionRunnerIsolateWorker::new(
            rt.clone(),
            IsolateConfig::new("zero_worker_test", ConcurrencyLimiter::unlimited()),
        );
        let Err(error) = super::IsolateClient::new(rt, 100, 0, isolate_worker) else {
            panic!("zero-worker isolate client unexpectedly succeeded")
        };
        assert!(error.to_string().contains("greater than zero"));
    }

    #[test]
    fn congested_scheduler_preserves_worker_and_queue_dependency_reserves() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let scheduler_rt = SchedulerTestRuntime::new(rt.clone());
        rt.block_on("scheduler_congested_reserve_test", async move {
            let mut harness = SchedulerHarness::new(scheduler_rt, 3, 2, 1, 100, 2);
            let action = harness.enqueue_test(1, TestRequestKind::Action);
            let occupying_independent = harness.enqueue_test(2, TestRequestKind::Independent);
            harness.start();
            let mut initially_started =
                [harness.next_started().await, harness.next_started().await];
            initially_started.sort_unstable();
            assert_eq!(initially_started, [1, 2]);

            let older_independent = harness.enqueue_test(3, TestRequestKind::Independent);
            let newer_independent = harness.enqueue_test(4, TestRequestKind::Independent);
            let dependency = harness.enqueue_test(5, TestRequestKind::DependencyHolder);

            // Both shared base worker slots and both base queue slots are full.
            // The dependency uses the extra queue and worker capacities.
            assert_eq!(harness.next_started().await, 5);
            dependency.complete().await;
            action.complete().await;
            let next = harness.next_started().await;
            if next == 3 {
                older_independent.complete().await;
                assert_eq!(harness.next_started().await, 4);
                newer_independent.complete().await;
            } else {
                assert_eq!(next, 4);
                newer_independent.complete().await;
                assert_eq!(harness.next_started().await, 3);
                older_independent.complete().await;
            }
            occupying_independent.complete().await;

            harness.shutdown().await;
        });
    }

    #[test]
    fn dependencies_use_shared_base_capacity() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let scheduler_rt = SchedulerTestRuntime::new(rt.clone());
        rt.block_on("scheduler_dependency_borrow_test", async move {
            let mut harness = SchedulerHarness::new(scheduler_rt, 3, 2, 2, 100, 64);
            let first = harness.enqueue_test(1, TestRequestKind::Dependency);
            let second = harness.enqueue_test(2, TestRequestKind::Dependency);
            let third = harness.enqueue_test(3, TestRequestKind::Dependency);
            harness.start();

            // This test checks that all three requests are admitted before any
            // completes. Separate worker threads can report their starts in any order.
            let mut started = [
                harness.next_started().await,
                harness.next_started().await,
                harness.next_started().await,
            ];
            started.sort_unstable();
            assert_eq!(started, [1, 2, 3]);

            first.complete().await;
            second.complete().await;
            third.complete().await;
            harness.shutdown().await;
        });
    }

    #[test]
    fn internal_nested_callback_uses_physical_reserve() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let scheduler_rt = SchedulerTestRuntime::new(rt.clone());
        rt.block_on("scheduler_internal_reserve_test", async move {
            let mut harness = SchedulerHarness::new(scheduler_rt, 2, 1, 1, 100, 64);
            let parent = harness.enqueue_test(1, TestRequestKind::Action);
            harness.start();
            assert_eq!(harness.next_started().await, 1);

            // Internal nested UDF callbacks use upstream's priority queue, but
            // consume the same physical reserve as externally propagated
            // dependency callbacks.
            let dependency = harness.enqueue_internal_test(2);
            assert_eq!(harness.next_started().await, 2);

            dependency.complete().await;
            parent.complete().await;
            harness.shutdown().await;
        });
    }

    #[test]
    fn internal_callbacks_skip_canceled_and_per_client_ineligible_entries() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let scheduler_rt = SchedulerTestRuntime::new(rt.clone());
        rt.block_on("scheduler_internal_eligibility_test", async move {
            // T=4 and 50% give each client a total of two and a shared base
            // of one. Client A starts at its total while one physical reserve
            // slot remains usable by a Client B descendant.
            let mut harness = SchedulerHarness::new(scheduler_rt, 4, 3, 3, 50, 64);
            let client_a_root =
                harness.enqueue_test_for_client(1, TestRequestKind::Independent, "deployment_a");
            let client_a_dependency =
                harness.enqueue_test_for_client(2, TestRequestKind::Dependency, "deployment_a");
            let client_b_root =
                harness.enqueue_test_for_client(3, TestRequestKind::Independent, "deployment_b");
            harness.start();

            let mut initially_started = [
                harness.next_started().await,
                harness.next_started().await,
                harness.next_started().await,
            ];
            initially_started.sort_unstable();
            assert_eq!(initially_started, [1, 2, 3]);

            let InFlightTestRequest {
                completion: canceled_completion,
                response: canceled_response,
            } = harness.enqueue_internal_test_for_client(4, "deployment_a");
            drop(canceled_response);
            let client_a_waiting = harness.enqueue_internal_test_for_client(5, "deployment_a");
            let client_b_dependency = harness.enqueue_internal_test_for_client(6, "deployment_b");

            // Canceled work is removed, and the live but per-client-ineligible
            // A request cannot hide B's eligible callback behind it.
            assert_eq!(harness.next_started().await, 6);
            assert!(
                canceled_completion.send(()).is_err(),
                "canceled internal callback unexpectedly reached a worker"
            );

            client_a_dependency.complete().await;
            assert_eq!(harness.next_started().await, 5);

            client_a_waiting.complete().await;
            client_b_dependency.complete().await;
            client_a_root.complete().await;
            client_b_root.complete().await;
            harness.shutdown().await;
        });
    }

    #[test]
    fn canceled_internal_permit_wait_does_not_retain_the_request_or_permit() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let scheduler_rt = SchedulerTestRuntime::new(rt.clone());
        rt.block_on("scheduler_internal_permit_cancellation_test", async move {
            let limiter = ConcurrencyLimiter::new(1);
            let mut harness = SchedulerHarness::new(scheduler_rt, 2, 1, 1, 100, 64);
            harness.set_concurrency_limiter(limiter.clone());
            let parent = harness.enqueue_test(1, TestRequestKind::Action);
            harness.start();
            assert_eq!(harness.next_started().await, 1);

            let InFlightTestRequest {
                mut completion,
                response,
            } = harness.enqueue_internal_test(2);
            drop(response);
            tokio::time::timeout(SCHEDULER_TEST_TIMEOUT, completion.closed())
                .await
                .expect("canceled internal request remained in its permit wait");

            let next = harness.enqueue_internal_test(3);
            parent.complete().await;
            assert_eq!(harness.next_started().await, 3);
            next.complete().await;
            harness.shutdown().await;
            assert_eq!(limiter.active_permits(), 0);
        });
    }

    #[test]
    fn canceled_external_permit_wait_does_not_retain_the_request_or_permit() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let scheduler_rt = SchedulerTestRuntime::new(rt.clone());
        rt.block_on("scheduler_external_permit_cancellation_test", async move {
            let limiter = ConcurrencyLimiter::new(1);
            let mut harness = SchedulerHarness::new(scheduler_rt, 2, 2, 2, 100, 64);
            harness.set_concurrency_limiter(limiter.clone());
            let active = harness.enqueue_test(1, TestRequestKind::Independent);
            harness.start();
            assert_eq!(harness.next_started().await, 1);

            let InFlightTestRequest {
                mut completion,
                response,
            } = harness.enqueue_test(2, TestRequestKind::Independent);
            drop(response);
            tokio::time::timeout(SCHEDULER_TEST_TIMEOUT, completion.closed())
                .await
                .expect("canceled external request remained in its permit wait");

            let next = harness.enqueue_test(3, TestRequestKind::Independent);
            active.complete().await;
            assert_eq!(harness.next_started().await, 3);
            next.complete().await;
            harness.shutdown().await;
            assert_eq!(limiter.active_permits(), 0);
        });
    }

    #[test]
    fn queued_dependency_expires_while_selected_control_plane_waits_for_permit() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let scheduler_rt = SchedulerTestRuntime::new(rt.clone());
        let clock = scheduler_rt.clone();
        rt.block_on(
            "scheduler_queued_expiry_during_permit_wait_test",
            async move {
                let limiter = ConcurrencyLimiter::new(1);
                let queue_config = IsolateQueueConfig::new(
                    Duration::from_millis(1),
                    Duration::from_millis(10),
                    Duration::from_millis(2),
                    Duration::from_millis(50),
                    Duration::from_secs(2),
                    64,
                )
                .unwrap();
                // T=2 with a 50% per-client limit lets client B's control-plane
                // request leave the queue while client A's dependency remains
                // ineligible behind A's active request.
                let mut harness = SchedulerHarness::new_with_policy(
                    scheduler_rt,
                    2,
                    2,
                    2,
                    50,
                    64,
                    Some(queue_config),
                    true,
                );
                harness.set_concurrency_limiter(limiter.clone());
                let active_workers = harness.active_workers.clone();
                let active = harness.enqueue_test_for_client(
                    1,
                    TestRequestKind::Independent,
                    "deployment_a",
                );
                harness.start();
                assert_eq!(harness.next_started().await, 1);

                let dependency =
                    harness.enqueue_test_for_client(2, TestRequestKind::Dependency, "deployment_a");
                let InFlightTestRequest {
                    completion: mut control_plane_completion,
                    response: control_plane_response,
                } = harness.enqueue_test_for_client(
                    3,
                    TestRequestKind::ControlPlane,
                    "deployment_b",
                );
                tokio::time::timeout(SCHEDULER_TEST_TIMEOUT, async {
                    while harness.lane_queue_len() != 1 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("control-plane request never entered its active-permit wait");

                clock.advance(Duration::from_millis(51));
                tokio::time::timeout(Duration::from_secs(1), dependency.expect_expired())
                    .await
                    .expect(
                        "dependency outlived its deadline behind another request's permit wait",
                    );

                drop(control_plane_response);
                tokio::time::timeout(SCHEDULER_TEST_TIMEOUT, control_plane_completion.closed())
                    .await
                    .expect("canceled control-plane request remained in its permit wait");
                active.complete().await;
                harness.shutdown().await;
                assert_eq!(limiter.active_permits(), 0);
                assert_eq!(active_workers.load(Ordering::Relaxed), 0);
            },
        );
    }

    #[test]
    fn per_client_overflow_stays_within_percentage_derived_total() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let scheduler_rt = SchedulerTestRuntime::new(rt.clone());
        rt.block_on("scheduler_per_client_reserve_test", async move {
            // T=4 at 50% preserves the old per-client total of two. Its
            // dependency overflow is carved out of that total, so B_client=1.
            let mut harness = SchedulerHarness::new(scheduler_rt, 4, 3, 3, 50, 64);
            let client_a_root =
                harness.enqueue_test_for_client(1, TestRequestKind::Independent, "deployment_a");
            harness.start();
            assert_eq!(harness.next_started().await, 1);

            let client_a_ordinary =
                harness.enqueue_test_for_client(2, TestRequestKind::Independent, "deployment_a");
            let client_a_dependency =
                harness.enqueue_test_for_client(3, TestRequestKind::Dependency, "deployment_a");
            assert_eq!(harness.next_started().await, 3);

            let client_a_waiting_dependency =
                harness.enqueue_test_for_client(4, TestRequestKind::Dependency, "deployment_a");
            let client_b_root =
                harness.enqueue_test_for_client(5, TestRequestKind::Independent, "deployment_b");
            assert_eq!(harness.next_started().await, 5);

            client_a_dependency.complete().await;
            assert_eq!(harness.next_started().await, 4);
            client_a_root.complete().await;
            client_a_waiting_dependency.complete().await;
            assert_eq!(harness.next_started().await, 2);

            client_a_ordinary.complete().await;
            client_b_root.complete().await;
            harness.shutdown().await;
        });
    }

    #[test]
    fn one_worker_dependency_expires_instead_of_hanging() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let scheduler_rt = SchedulerTestRuntime::new(rt.clone());
        let clock = scheduler_rt.clone();
        rt.block_on("scheduler_one_worker_expiry_test", async move {
            let mut harness = SchedulerHarness::new(scheduler_rt, 1, 1, 1, 100, 64);
            let parent = harness.enqueue_test(1, TestRequestKind::Action);
            harness.start();
            assert_eq!(harness.next_started().await, 1);

            let dependency = harness.enqueue_test(2, TestRequestKind::Dependency);
            clock.advance(*CODEL_QUEUE_IDLE_EXPIRATION_MILLIS + Duration::from_millis(1));
            // Sending another request wakes the busy scheduler so it observes
            // the manually advanced CoDel deadline without a wall-clock wait.
            let wake = harness.enqueue_test(3, TestRequestKind::Dependency);
            dependency.expect_expired().await;

            parent.complete().await;
            assert_eq!(harness.next_started().await, 3);
            wake.complete().await;
            harness.shutdown().await;
        });
    }

    #[test]
    fn worker_failure_closes_queue_and_clears_active_accounting() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let scheduler_rt = SchedulerTestRuntime::new(rt.clone());
        rt.block_on("scheduler_worker_failure_test", async move {
            let mut harness = SchedulerHarness::new(scheduler_rt, 1, 1, 1, 100, 64);
            let active_workers = harness.active_workers.clone();
            let failed = harness.enqueue_test(1, TestRequestKind::WorkerFailure);
            let queued = harness.enqueue_test(2, TestRequestKind::Independent);

            harness.start();
            assert_eq!(harness.next_started().await, 1);
            failed.expect_dropped().await;
            queued.expect_dropped().await;
            harness.shutdown().await;

            assert_eq!(active_workers.load(Ordering::Relaxed), 0);
        });
    }

    #[test]
    fn canceled_queued_request_does_not_leak_active_accounting() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let scheduler_rt = SchedulerTestRuntime::new(rt.clone());
        rt.block_on("scheduler_canceled_request_test", async move {
            let mut harness = SchedulerHarness::new(scheduler_rt, 1, 1, 1, 100, 64);
            let active_workers = harness.active_workers.clone();
            let active = harness.enqueue_test(1, TestRequestKind::Independent);
            harness.start();
            assert_eq!(harness.next_started().await, 1);

            let InFlightTestRequest {
                completion,
                response,
            } = harness.enqueue_test(2, TestRequestKind::Independent);
            drop(response);

            active.complete().await;
            let next = harness.enqueue_test(3, TestRequestKind::Independent);
            assert_eq!(harness.next_started().await, 3);
            assert!(
                completion.send(()).is_err(),
                "caller-canceled request unexpectedly reached a worker"
            );
            next.complete().await;
            harness.shutdown().await;

            assert_eq!(active_workers.load(Ordering::Relaxed), 0);
        });
    }

    #[test]
    fn canceled_control_plane_request_is_discarded_before_worker_assignment() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let scheduler_rt = SchedulerTestRuntime::new(rt.clone());
        rt.block_on("scheduler_control_plane_cancellation_test", async move {
            let mut harness = SchedulerHarness::new_control_plane(scheduler_rt, 1, 1, 1, 100, 64);
            let active_workers = harness.active_workers.clone();
            let active = harness.enqueue_test(1, TestRequestKind::Independent);
            harness.start();
            assert_eq!(harness.next_started().await, 1);

            let InFlightTestRequest {
                completion: canceled_completion,
                response: canceled_response,
            } = harness.enqueue_test(2, TestRequestKind::ControlPlane);
            drop(canceled_response);
            let follow_up = harness.enqueue_test(3, TestRequestKind::Independent);

            active.complete().await;
            assert_eq!(harness.next_started().await, 3);
            follow_up.complete().await;
            drop(canceled_completion);
            harness.shutdown().await;

            assert_eq!(active_workers.load(Ordering::Relaxed), 0);
        });
    }

    #[test]
    fn control_plane_waits_in_shared_base_while_dependency_uses_reserves() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let scheduler_rt = SchedulerTestRuntime::new(rt.clone());
        rt.block_on("scheduler_control_plane_reserve_test", async move {
            // One request occupies the only shared-base worker. The older
            // control-plane request also fills the shared queue entry, so only
            // the newer dependency may use both overflow capacities.
            let mut harness = SchedulerHarness::new_control_plane(scheduler_rt, 2, 1, 1, 100, 1);
            let base = harness.enqueue_test(1, TestRequestKind::Independent);
            harness.start();
            assert_eq!(harness.next_started().await, 1);

            let control_plane = harness.enqueue_test(2, TestRequestKind::ControlPlane);
            let dependency = harness.enqueue_test(3, TestRequestKind::Dependency);
            assert_eq!(harness.next_started().await, 3);

            dependency.complete().await;
            base.complete().await;
            assert_eq!(harness.next_started().await, 2);
            control_plane.complete().await;
            harness.shutdown().await;
        });
    }

    #[test]
    fn scheduler_admits_all_classes_below_base_capacity() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let scheduler_rt = SchedulerTestRuntime::new(rt.clone());
        rt.block_on("scheduler_fairness_test", async move {
            let mut harness = SchedulerHarness::new(scheduler_rt, 3, 2, 2, 100, 64);
            let independent = harness.enqueue_test(0, TestRequestKind::Independent);
            let first_dependency = harness.enqueue_test(1, TestRequestKind::Dependency);
            let second_dependency = harness.enqueue_test(2, TestRequestKind::Dependency);
            harness.start();

            // Separate worker threads can report starts in any order after the
            // scheduler dispatches all three requests.
            let mut started = [
                harness.next_started().await,
                harness.next_started().await,
                harness.next_started().await,
            ];
            started.sort_unstable();
            assert_eq!(started, [0, 1, 2]);
            first_dependency.complete().await;
            independent.complete().await;
            second_dependency.complete().await;

            harness.shutdown().await;
        });
    }
}
