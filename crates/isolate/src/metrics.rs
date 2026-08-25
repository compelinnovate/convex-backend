use std::{
    borrow::Cow,
    sync::{
        Arc,
        Once,
    },
    time::Duration,
};

use common::{
    components::ResolvedComponentFunctionPath,
    types::{
        ActiveJavascriptClass,
        UdfType,
    },
};
use deno_core::v8;
use errors::ErrorMetadata;
use fastrace::{
    local::LocalSpan,
    Event,
};
use metrics::{
    add_to_gauge_with_labels,
    log_counter,
    log_counter_with_labels,
    log_distribution,
    log_distribution_with_labels,
    log_gauge,
    log_gauge_with_labels,
    register_convex_counter,
    register_convex_gauge,
    register_convex_gauge_evictable,
    register_convex_histogram,
    subtract_from_gauge_with_labels,
    CancelableTimer,
    IntoLabel,
    MetricLabel,
    StaticMetricLabel,
    StatusTimer,
    Timer,
    STATUS_LABEL,
};
use prometheus::VMHistogram;

use crate::{
    context_cache::{
        ContextCacheClearReason,
        ReusableContextKind,
    },
    module_map::ModulesRegistered,
    IsolateHeapStats,
};

fn reusable_context_kind_label(context_kind: ReusableContextKind) -> &'static str {
    match context_kind {
        ReusableContextKind::DatabaseUdf => "database_udf",
        ReusableContextKind::Action => "action",
        ReusableContextKind::HttpAction => "http_action",
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ContextCacheOperation {
    Save,
    Take,
    RejectPoolCapacity,
    RejectFrequency,
    RejectMemoryPressure,
}

fn context_cache_operation_label(operation: ContextCacheOperation) -> &'static str {
    match operation {
        ContextCacheOperation::Save => "save",
        ContextCacheOperation::Take => "take",
        ContextCacheOperation::RejectPoolCapacity => "reject_pool_capacity",
        ContextCacheOperation::RejectFrequency => "reject_frequency",
        ContextCacheOperation::RejectMemoryPressure => "reject_memory_pressure",
    }
}

fn context_cache_clear_reason_label(reason: ContextCacheClearReason) -> &'static str {
    match reason {
        ContextCacheClearReason::AdmissionReplacement => "admission_replacement",
        ContextCacheClearReason::PoolCapacityReplacement => "pool_capacity_replacement",
        ContextCacheClearReason::DuplicateReplacement => "duplicate_replacement",
        ContextCacheClearReason::MemoryPressure => "memory_pressure",
        ContextCacheClearReason::CgroupMemoryPressure => "cgroup_memory_pressure",
        ContextCacheClearReason::AppDefinitionEvaluation => "app_definition_evaluation",
        ContextCacheClearReason::CacheDrop => "cache_drop",
    }
}

// `client_id` is unbounded and a client that disconnects stops updating this
// gauge, leaving a stale frozen value, so evict label sets that go idle.
register_convex_gauge_evictable!(
    ISOLATE_POOL_RUNNING_COUNT_INFO,
    "How many isolate workers are currently running work",
    &["pool_name", "client_id"]
);
pub fn log_pool_running_count(name: &'static str, count: usize, client_id: &str) {
    log_gauge_with_labels(
        &ISOLATE_POOL_RUNNING_COUNT_INFO,
        count as f64,
        vec![
            StaticMetricLabel::new("pool_name", name),
            MetricLabel::new("client_id", client_id),
        ],
    );
}

register_convex_gauge!(
    ISOLATE_POOL_MAX_INFO,
    "How many isolate workers can be running",
    &["pool_name"]
);
pub fn log_pool_max(name: &'static str, count: usize) {
    log_gauge_with_labels(
        &ISOLATE_POOL_MAX_INFO,
        count as f64,
        vec![StaticMetricLabel::new("pool_name", name)],
    );
}

register_convex_gauge!(
    ISOLATE_ANALYSIS_RESERVATIONS_IN_USE_INFO,
    "Shared-capacity permits currently held by isolate module analysis"
);
register_convex_histogram!(
    ISOLATE_ANALYSIS_CAPACITY_WAIT_SECONDS,
    "Time isolate module analysis waits to borrow shared degradable-query capacity"
);

pub fn initialize_analysis_reservation_metrics() {
    // Register without resetting reservations held by another concurrent
    // analysis call.
    let _ = ISOLATE_ANALYSIS_RESERVATIONS_IN_USE_INFO.get();
}

pub fn analysis_capacity_wait_timer() -> Timer<VMHistogram> {
    Timer::new(&ISOLATE_ANALYSIS_CAPACITY_WAIT_SECONDS)
}

pub fn increment_analysis_capacity_reservations_in_use() {
    ISOLATE_ANALYSIS_RESERVATIONS_IN_USE_INFO.inc();
}

pub fn decrement_analysis_capacity_reservations_in_use() {
    ISOLATE_ANALYSIS_RESERVATIONS_IN_USE_INFO.dec();
}

fn scheduler_class_labels(
    name: &'static str,
    scheduler_class: &'static str,
) -> Vec<StaticMetricLabel> {
    vec![
        StaticMetricLabel::new("pool_name", name),
        StaticMetricLabel::new("scheduler_class", scheduler_class),
    ]
}

register_convex_counter!(
    ISOLATE_SCHEDULER_REQUESTS_ENQUEUED_TOTAL,
    "Number of requests accepted through the isolate scheduler's external or internal ingress",
    &["pool_name", "scheduler_class"]
);
pub fn log_scheduler_request_enqueued(name: &'static str, scheduler_class: &'static str) {
    log_counter_with_labels(
        &ISOLATE_SCHEDULER_REQUESTS_ENQUEUED_TOTAL,
        1,
        scheduler_class_labels(name, scheduler_class),
    );
}

register_convex_counter!(
    ISOLATE_SCHEDULER_REQUESTS_DISPATCHED_TOTAL,
    "Number of requests dispatched by the isolate scheduler",
    &["pool_name", "scheduler_class"]
);
pub fn log_scheduler_request_dispatched(name: &'static str, scheduler_class: &'static str) {
    log_counter_with_labels(
        &ISOLATE_SCHEDULER_REQUESTS_DISPATCHED_TOTAL,
        1,
        scheduler_class_labels(name, scheduler_class),
    );
}

register_convex_counter!(
    ISOLATE_SCHEDULER_CONTEXT_AFFINITY_TOTAL,
    "Number of reusable-context-eligible worker selections accepted by a worker channel, by \
     affinity outcome",
    &["pool_name", "context_kind", "outcome"],
    std::time::Duration::MAX,
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SchedulerContextAffinityOutcome {
    Hit,
    SameClientWorker,
    NewWorker,
    StolenWorker,
}

impl SchedulerContextAffinityOutcome {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::SameClientWorker => "same_client_worker",
            Self::NewWorker => "new_worker",
            Self::StolenWorker => "stolen_worker",
        }
    }
}

pub(crate) fn log_scheduler_context_affinity(
    name: &'static str,
    context_kind: ReusableContextKind,
    outcome: SchedulerContextAffinityOutcome,
) {
    log_counter_with_labels(
        &ISOLATE_SCHEDULER_CONTEXT_AFFINITY_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("pool_name", name),
            StaticMetricLabel::new("context_kind", reusable_context_kind_label(context_kind)),
            StaticMetricLabel::new("outcome", outcome.metric_label()),
        ],
    );
}

register_convex_counter!(
    ISOLATE_SCHEDULER_REQUESTS_EXPIRED_TOTAL,
    "Number of isolate scheduler requests that reached their original queue deadline before \
     dispatch",
    &["pool_name", "scheduler_class"],
    std::time::Duration::MAX,
);
pub fn log_scheduler_request_expired(name: &'static str, scheduler_class: &'static str) {
    log_counter_with_labels(
        &ISOLATE_SCHEDULER_REQUESTS_EXPIRED_TOTAL,
        1,
        scheduler_class_labels(name, scheduler_class),
    );
}

register_convex_counter!(
    ISOLATE_SCHEDULER_REQUESTS_REJECTED_TOTAL,
    "Number of isolate scheduler requests rejected before dispatch",
    &["pool_name", "scheduler_class", "reason"],
    std::time::Duration::MAX,
);
pub fn log_scheduler_request_rejected(
    name: &'static str,
    scheduler_class: &'static str,
    reason: &'static str,
) {
    let mut labels = scheduler_class_labels(name, scheduler_class);
    labels.push(StaticMetricLabel::new("reason", reason));
    log_counter_with_labels(&ISOLATE_SCHEDULER_REQUESTS_REJECTED_TOTAL, 1, labels);
}

register_convex_gauge!(
    ISOLATE_SCHEDULER_ACTIVE_REQUESTS_INFO,
    "How many isolate scheduler requests are currently active by scheduler class",
    &["pool_name", "scheduler_class", "is_isolate_action"]
);
fn scheduler_active_labels(
    name: &'static str,
    scheduler_class: &'static str,
    is_isolate_action: bool,
) -> Vec<StaticMetricLabel> {
    vec![
        StaticMetricLabel::new("pool_name", name),
        StaticMetricLabel::new("scheduler_class", scheduler_class),
        StaticMetricLabel::new("is_isolate_action", is_isolate_action.as_label()),
    ]
}

pub fn log_scheduler_active_request_started(
    name: &'static str,
    scheduler_class: &'static str,
    is_isolate_action: bool,
) {
    add_to_gauge_with_labels(
        &ISOLATE_SCHEDULER_ACTIVE_REQUESTS_INFO,
        1.0,
        scheduler_active_labels(name, scheduler_class, is_isolate_action),
    );
}

pub fn log_scheduler_active_request_finished(
    name: &'static str,
    scheduler_class: &'static str,
    is_isolate_action: bool,
) {
    subtract_from_gauge_with_labels(
        &ISOLATE_SCHEDULER_ACTIVE_REQUESTS_INFO,
        1.0,
        scheduler_active_labels(name, scheduler_class, is_isolate_action),
    );
}

register_convex_counter!(
    ISOLATE_SCHEDULER_DEPENDENCY_RESERVE_DISPATCH_TOTAL,
    "Number of dependency requests dispatched while global occupancy was at or above shared base \
     capacity",
    &["pool_name"]
);
pub fn log_scheduler_dependency_reserve_dispatch(name: &'static str) {
    log_counter_with_labels(
        &ISOLATE_SCHEDULER_DEPENDENCY_RESERVE_DISPATCH_TOTAL,
        1,
        vec![StaticMetricLabel::new("pool_name", name)],
    );
}

register_convex_counter!(
    ISOLATE_SCHEDULER_DEPENDENCY_QUEUE_RESERVE_ENQUEUE_TOTAL,
    "Number of dependency requests enqueued using queue capacity unavailable to non-dependency \
     work",
    &["pool_name"]
);
pub fn log_scheduler_dependency_queue_reserve_enqueue(name: &'static str) {
    log_counter_with_labels(
        &ISOLATE_SCHEDULER_DEPENDENCY_QUEUE_RESERVE_ENQUEUE_TOTAL,
        1,
        vec![StaticMetricLabel::new("pool_name", name)],
    );
}

register_convex_gauge!(
    ISOLATE_SCHEDULER_CAPACITY_INFO,
    "Configured isolate scheduler capacity by kind",
    &["pool_name", "capacity_kind"]
);
pub fn log_scheduler_capacity(name: &'static str, capacity_kind: &'static str, capacity: usize) {
    log_gauge_with_labels(
        &ISOLATE_SCHEDULER_CAPACITY_INFO,
        capacity as f64,
        vec![
            StaticMetricLabel::new("pool_name", name),
            StaticMetricLabel::new("capacity_kind", capacity_kind),
        ],
    );
}

register_convex_gauge!(
    ISOLATE_CONTROL_PLANE_LANE_ENABLED_INFO,
    "Whether control-plane isolate request classification is enabled",
    &["pool_name"]
);
fn control_plane_lane_enabled_value(enabled: bool) -> f64 {
    if enabled {
        1.0
    } else {
        0.0
    }
}

pub fn log_control_plane_lane_enabled(name: &'static str, enabled: bool) {
    log_gauge_with_labels(
        &ISOLATE_CONTROL_PLANE_LANE_ENABLED_INFO,
        control_plane_lane_enabled_value(enabled),
        vec![StaticMetricLabel::new("pool_name", name)],
    );
}

register_convex_gauge!(
    ISOLATE_QUEUE_DEPTH_INFO,
    "Current number of queued isolate requests by scheduler lane",
    &["pool_name", "lane"]
);
pub fn log_isolate_queue_depth(name: &'static str, lane: &'static str, depth: usize) {
    log_gauge_with_labels(
        &ISOLATE_QUEUE_DEPTH_INFO,
        depth as f64,
        vec![
            StaticMetricLabel::new("pool_name", name),
            StaticMetricLabel::new("lane", lane),
        ],
    );
}

register_convex_gauge!(
    ISOLATE_QUEUE_OLDEST_AGE_SECONDS,
    "Age of the oldest queued isolate request by scheduler lane",
    &["pool_name", "lane"]
);
pub fn log_isolate_queue_oldest_age(name: &'static str, lane: &'static str, age: Duration) {
    log_gauge_with_labels(
        &ISOLATE_QUEUE_OLDEST_AGE_SECONDS,
        age.as_secs_f64(),
        vec![
            StaticMetricLabel::new("pool_name", name),
            StaticMetricLabel::new("lane", lane),
        ],
    );
}

register_convex_histogram!(
    ISOLATE_QUEUE_SOJOURN_SECONDS,
    "Time dispatched isolate requests spent in the scheduler queue",
    &["pool_name", "lane"]
);
pub fn log_isolate_queue_sojourn(name: &'static str, lane: &'static str, sojourn: Duration) {
    log_distribution_with_labels(
        &ISOLATE_QUEUE_SOJOURN_SECONDS,
        sojourn.as_secs_f64(),
        vec![
            StaticMetricLabel::new("pool_name", name),
            StaticMetricLabel::new("lane", lane),
        ],
    );
}

register_convex_counter!(
    ISOLATE_QUEUE_REJECTIONS_TOTAL,
    "Number of isolate queue requests rejected before dispatch",
    &["pool_name", "lane", "reason"],
    std::time::Duration::MAX,
);
pub fn log_isolate_queue_rejection(name: &'static str, lane: &'static str, reason: &'static str) {
    log_counter_with_labels(
        &ISOLATE_QUEUE_REJECTIONS_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("pool_name", name),
            StaticMetricLabel::new("lane", lane),
            StaticMetricLabel::new("reason", reason),
        ],
    );
}

register_convex_gauge!(
    ISOLATE_QUEUE_OVERLOADED_INFO,
    "Whether an isolate queue scheduler lane is overloaded",
    &["pool_name", "lane"]
);
pub fn log_isolate_queue_overloaded(name: &'static str, lane: &'static str, overloaded: bool) {
    log_gauge_with_labels(
        &ISOLATE_QUEUE_OVERLOADED_INFO,
        if overloaded { 1.0 } else { 0.0 },
        vec![
            StaticMetricLabel::new("pool_name", name),
            StaticMetricLabel::new("lane", lane),
        ],
    );
}

register_convex_counter!(
    ISOLATE_QUEUE_OVERLOAD_TRANSITIONS_TOTAL,
    "Number of isolate queue scheduler lane overload transitions",
    &["pool_name", "lane", "transition"],
    std::time::Duration::MAX,
);
pub fn log_isolate_queue_overload_transition(
    name: &'static str,
    lane: &'static str,
    transition: &'static str,
) {
    log_counter_with_labels(
        &ISOLATE_QUEUE_OVERLOAD_TRANSITIONS_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("pool_name", name),
            StaticMetricLabel::new("lane", lane),
            StaticMetricLabel::new("transition", transition),
        ],
    );
}

register_convex_gauge!(
    ISOLATE_QUEUE_INELIGIBLE_INFO,
    "Current queued isolate requests blocked by each scheduler eligibility limit",
    &["pool_name", "lane", "reason"]
);
pub fn log_isolate_queue_ineligible(
    name: &'static str,
    lane: &'static str,
    reason: &'static str,
    count: usize,
) {
    log_gauge_with_labels(
        &ISOLATE_QUEUE_INELIGIBLE_INFO,
        count as f64,
        vec![
            StaticMetricLabel::new("pool_name", name),
            StaticMetricLabel::new("lane", lane),
            StaticMetricLabel::new("reason", reason),
        ],
    );
}

register_convex_gauge!(
    ISOLATE_QUEUE_CAPACITY_INFO,
    "Configured isolate scheduler queue capacity",
    &["pool_name", "capacity_kind"]
);
pub fn log_isolate_queue_capacity(
    name: &'static str,
    capacity_kind: &'static str,
    capacity: usize,
) {
    log_gauge_with_labels(
        &ISOLATE_QUEUE_CAPACITY_INFO,
        capacity as f64,
        vec![
            StaticMetricLabel::new("pool_name", name),
            StaticMetricLabel::new("capacity_kind", capacity_kind),
        ],
    );
}

register_convex_gauge!(
    ISOLATE_QUEUE_POLICY_INFO,
    "Selected isolate scheduler queue policy",
    &["pool_name", "policy"]
);
pub fn log_isolate_queue_policy(name: &'static str, policy: &'static str) {
    log_gauge_with_labels(
        &ISOLATE_QUEUE_POLICY_INFO,
        1.0,
        vec![
            StaticMetricLabel::new("pool_name", name),
            StaticMetricLabel::new("policy", policy),
        ],
    );
}

register_convex_gauge!(
    ISOLATE_QUEUE_CONFIG_MILLIS_INFO,
    "Configured isolate scheduler queue durations in milliseconds",
    &["pool_name", "config_kind"]
);
pub fn log_isolate_queue_config(name: &'static str, config_kind: &'static str, duration: Duration) {
    log_gauge_with_labels(
        &ISOLATE_QUEUE_CONFIG_MILLIS_INFO,
        duration.as_secs_f64() * 1000.0,
        vec![
            StaticMetricLabel::new("pool_name", name),
            StaticMetricLabel::new("config_kind", config_kind),
        ],
    );
}

register_convex_counter!(UDF_EXECUTE_FULL_TOTAL, "UDF execution queue full count");

register_convex_counter!(
    UDF_REJECTED_BEFORE_EXECUTION_TOTAL,
    "UDF execution attempts rejected before execution",
    &["reason"]
);

#[derive(Clone, Copy, Debug, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum RejectedBeforeExecutionReason {
    ExpiredInQueue,
    IsolateNotClean,
    InitialPermitTimeout,
    ExecuteQueueFull,
}

impl RejectedBeforeExecutionReason {
    fn error_metadata(self) -> ErrorMetadata {
        match self {
            Self::ExpiredInQueue => ErrorMetadata::rejected_before_execution(
                "ExpiredInQueue",
                "Too many concurrent requests in a short period of time. Spread out your requests \
                 out over time or throttle them to avoid errors.",
            ),
            Self::IsolateNotClean => ErrorMetadata::rejected_before_execution(
                "IsolateNotClean",
                "Selected isolate was not clean",
            ),
            Self::InitialPermitTimeout => ErrorMetadata::rejected_before_execution(
                "InitialPermitTimeoutError",
                "Couldn't acquire a permit on this funrun",
            ),
            Self::ExecuteQueueFull => ErrorMetadata::rejected_before_execution(
                "ExecuteFullError",
                "Too many concurrent requests in a short period of time. Spread out your requests \
                 out over time or throttle them to avoid errors.",
            ),
        }
    }
}

pub(crate) fn rejected_before_execution_error(
    reason: RejectedBeforeExecutionReason,
) -> ErrorMetadata {
    let label: &'static str = reason.into();
    log_counter_with_labels(
        &UDF_REJECTED_BEFORE_EXECUTION_TOTAL,
        1,
        vec![StaticMetricLabel::new("reason", label)],
    );
    reason.error_metadata()
}

pub fn initialize_capacity_counters(name: &'static str) {
    const SCHEDULER_CLASSES: [&str; 5] = [
        "independent",
        "descendant_holder",
        "dependency",
        "dependency_descendant_holder",
        "control_plane",
    ];
    const SCHEDULER_REJECTION_REASONS: [&str; 5] = [
        "queue_full",
        "lane_full",
        "scheduler_closed",
        "delay_control_shed",
        "caller_dropped",
    ];
    const QUEUE_LANES: [&str; 4] = [
        "dependency",
        "control_plane",
        "independent_action",
        "ordinary",
    ];
    const QUEUE_REJECTION_REASONS: [&str; 6] = [
        "queue_full",
        "lane_full",
        "scheduler_closed",
        "hard_expired",
        "delay_control_shed",
        "caller_dropped",
    ];

    for scheduler_class in SCHEDULER_CLASSES {
        log_counter_with_labels(
            &ISOLATE_SCHEDULER_REQUESTS_EXPIRED_TOTAL,
            0,
            scheduler_class_labels(name, scheduler_class),
        );
        for reason in SCHEDULER_REJECTION_REASONS {
            let mut labels = scheduler_class_labels(name, scheduler_class);
            labels.push(StaticMetricLabel::new("reason", reason));
            log_counter_with_labels(&ISOLATE_SCHEDULER_REQUESTS_REJECTED_TOTAL, 0, labels);
        }
    }
    for lane in QUEUE_LANES {
        for reason in QUEUE_REJECTION_REASONS {
            log_counter_with_labels(
                &ISOLATE_QUEUE_REJECTIONS_TOTAL,
                0,
                vec![
                    StaticMetricLabel::new("pool_name", name),
                    StaticMetricLabel::new("lane", lane),
                    StaticMetricLabel::new("reason", reason),
                ],
            );
        }
        for transition in ["entered", "cleared"] {
            log_counter_with_labels(
                &ISOLATE_QUEUE_OVERLOAD_TRANSITIONS_TOTAL,
                0,
                vec![
                    StaticMetricLabel::new("pool_name", name),
                    StaticMetricLabel::new("lane", lane),
                    StaticMetricLabel::new("transition", transition),
                ],
            );
        }
    }
    log_counter(&UDF_EXECUTE_FULL_TOTAL, 0);
}

pub fn execute_full_error() -> ErrorMetadata {
    log_counter(&UDF_EXECUTE_FULL_TOTAL, 1);
    rejected_before_execution_error(RejectedBeforeExecutionReason::ExecuteQueueFull)
}

register_convex_histogram!(
    UDF_SERVICE_REQUEST_SECONDS,
    "Time to service an UDF request",
    &["status", "udf_type"]
);
pub fn service_request_timer(udf_type: &UdfType) -> StatusTimer {
    let mut t = StatusTimer::new(&UDF_SERVICE_REQUEST_SECONDS);
    t.add_label(udf_type.metric_label());
    t
}

register_convex_histogram!(
    ISOLATE_SCHEDULER_STOLEN_WORKER_AGE_SECONDS,
    "The now - last_used_ts in seconds for the stolen worker",
);
pub fn log_worker_stolen(age: Duration) {
    log_distribution(
        &ISOLATE_SCHEDULER_STOLEN_WORKER_AGE_SECONDS,
        age.as_secs_f64(),
    );
}

register_convex_histogram!(UDF_QUEUE_SECONDS, "UDF queue time");
pub fn queue_timer() -> Timer<VMHistogram> {
    Timer::new(&UDF_QUEUE_SECONDS)
}

pub enum RequestStatus {
    Success,
    DeveloperError,
    SystemError,
}

pub fn finish_service_request_timer(timer: StatusTimer, status: RequestStatus) {
    match status {
        RequestStatus::Success => {
            timer.finish();
        },
        RequestStatus::DeveloperError => {
            timer.finish_developer_error();
        },
        RequestStatus::SystemError => (),
    };
}

const CONTROL_PLANE_REQUEST_KINDS: [&str; 5] = [
    "analyze",
    "evaluate_schema",
    "evaluate_auth_config",
    "evaluate_app_definitions",
    "evaluate_component_initializer",
];

register_convex_counter!(
    ISOLATE_CONTROL_PLANE_REQUESTS_TOTAL,
    "Number of isolate control-plane evaluation requests started",
    &["pool_name", "request_kind"],
    std::time::Duration::MAX,
);
register_convex_gauge!(
    ISOLATE_CONTROL_PLANE_REQUESTS_IN_FLIGHT_INFO,
    "Current isolate control-plane evaluation requests",
    &["pool_name", "request_kind"],
);

pub(crate) fn initialize_control_plane_request_metrics(pool_name: &'static str) {
    for request_kind in CONTROL_PLANE_REQUEST_KINDS {
        let labels = vec![
            StaticMetricLabel::new("pool_name", pool_name),
            StaticMetricLabel::new("request_kind", request_kind),
        ];
        log_counter_with_labels(&ISOLATE_CONTROL_PLANE_REQUESTS_TOTAL, 0, labels.clone());
        log_gauge_with_labels(&ISOLATE_CONTROL_PLANE_REQUESTS_IN_FLIGHT_INFO, 0.0, labels);
    }
}

pub(crate) struct ControlPlaneRequestGuard {
    pool_name: &'static str,
    request_kind: &'static str,
}

impl ControlPlaneRequestGuard {
    pub(crate) fn new(pool_name: &'static str, request_kind: &'static str) -> Self {
        let labels = vec![
            StaticMetricLabel::new("pool_name", pool_name),
            StaticMetricLabel::new("request_kind", request_kind),
        ];
        log_counter_with_labels(&ISOLATE_CONTROL_PLANE_REQUESTS_TOTAL, 1, labels.clone());
        add_to_gauge_with_labels(&ISOLATE_CONTROL_PLANE_REQUESTS_IN_FLIGHT_INFO, 1.0, labels);
        Self {
            pool_name,
            request_kind,
        }
    }
}

impl Drop for ControlPlaneRequestGuard {
    fn drop(&mut self) {
        subtract_from_gauge_with_labels(
            &ISOLATE_CONTROL_PLANE_REQUESTS_IN_FLIGHT_INFO,
            1.0,
            vec![
                StaticMetricLabel::new("pool_name", self.pool_name),
                StaticMetricLabel::new("request_kind", self.request_kind),
            ],
        );
    }
}

register_convex_histogram!(
    UDF_ISOLATE_BUILD_SECONDS,
    "Time to build isolate context",
    &STATUS_LABEL
);
pub fn context_build_timer() -> StatusTimer {
    StatusTimer::new(&UDF_ISOLATE_BUILD_SECONDS)
}

register_convex_histogram!(
    UDF_ISOLATE_LOAD_USER_MODULES_SECONDS,
    "Time to load all user modules for a request",
    &["udf_type", "is_dynamic", "status"],
);
pub fn eval_user_module_timer(udf_type: UdfType, is_dynamic: bool) -> StatusTimer {
    let mut t = StatusTimer::new(&UDF_ISOLATE_LOAD_USER_MODULES_SECONDS);
    t.add_label(udf_type.metric_label());
    t.add_label(StaticMetricLabel::new("is_dynamic", is_dynamic.as_label()));
    t
}

register_convex_histogram!(
    UDF_ISOLATE_MODULES_REGISTERED_TOTAL,
    "Number of modules registered while loading user modules for a request",
    &["udf_type", "is_dynamic"],
);
register_convex_histogram!(
    UDF_ISOLATE_MODULES_REGISTERED_SOURCE_BYTES,
    "Source bytes of the modules registered while loading user modules for a request",
    &["udf_type", "is_dynamic"],
);
/// Counts only the modules this request had to register: a context reused from
/// the context cache already has its import closure in the `ModuleMap` and
/// registers nothing.
pub fn log_modules_registered(
    udf_type: UdfType,
    is_dynamic: bool,
    ModulesRegistered {
        module_count,
        source_size,
    }: ModulesRegistered,
) {
    let labels = vec![
        udf_type.metric_label(),
        StaticMetricLabel::new("is_dynamic", is_dynamic.as_label()),
    ];
    log_distribution_with_labels(
        &UDF_ISOLATE_MODULES_REGISTERED_TOTAL,
        module_count as f64,
        labels.clone(),
    );
    log_distribution_with_labels(
        &UDF_ISOLATE_MODULES_REGISTERED_SOURCE_BYTES,
        source_size as f64,
        labels,
    );
}

register_convex_histogram!(
    UDF_ISOLATE_LOOKUP_SOURCE_SECONDS,
    "Time to load a single module's source",
    &["is_system", "status"],
);
pub fn lookup_source_timer(is_system: bool) -> StatusTimer {
    let mut t = StatusTimer::new(&UDF_ISOLATE_LOOKUP_SOURCE_SECONDS);
    t.add_label(StaticMetricLabel::new("is_system", is_system.as_label()));
    t
}

register_convex_histogram!(
    UDF_ISOLATE_COMPILE_MODULE_SECONDS,
    "Time to compile a single module's source",
    &["status", "cached"],
);
pub fn compile_module_timer(cached: bool) -> StatusTimer {
    let mut timer = StatusTimer::new(&UDF_ISOLATE_COMPILE_MODULE_SECONDS);
    timer.add_label(MetricLabel::new("cached", cached.as_label()));
    timer
}

register_convex_histogram!(
    UDF_ISOLATE_INSTANTIATE_MODULE_SECONDS,
    "Time to instantiate the top-level module",
    &["status"],
);
pub fn instantiate_module_timer() -> StatusTimer {
    StatusTimer::new(&UDF_ISOLATE_INSTANTIATE_MODULE_SECONDS)
}

register_convex_histogram!(
    UDF_ISOLATE_EVALUATE_MODULE_SECONDS,
    "Time to evaluate the top-level module",
    &["status"],
);
pub fn evaluate_module_timer() -> StatusTimer {
    StatusTimer::new(&UDF_ISOLATE_EVALUATE_MODULE_SECONDS)
}

register_convex_histogram!(
    UDF_ISOLATE_ARGUMENTS_BYTES,
    "Size of isolate arguments in bytes"
);
pub fn log_argument_length(args: &str) {
    log_distribution(&UDF_ISOLATE_ARGUMENTS_BYTES, args.len() as f64);
}

register_convex_histogram!(UDF_ISOLATE_RESULT_BYTES, "Size of isolate results in bytes");
pub fn log_result_length(result: &str) {
    log_distribution(&UDF_ISOLATE_RESULT_BYTES, result.len() as f64);
}

register_convex_histogram!(UDF_OP_SECONDS, "Duration of UDF op", &["status", "op"]);
pub fn op_timer(op_name: &str) -> StatusTimer {
    let mut t = StatusTimer::new(&UDF_OP_SECONDS);
    t.add_label(StaticMetricLabel::new("op", op_name.to_owned()));
    t
}

register_convex_counter!(
    ISOLATE_DIRECT_FUNCTION_CALL_TOTAL,
    "Number of calls to registered UDFs as js functions"
);
fn log_direct_function_call() {
    log_counter(&ISOLATE_DIRECT_FUNCTION_CALL_TOTAL, 1);
}

pub fn log_log_line(line: &str) {
    // We log a console.warn line containing this link when a function is called
    // directly. These are potentially problematic because it looks like arg and
    // return values are being validated, and a new isolate is running the UDF,
    // but actually the plain JS function is being called. If the non-isolated,
    // non-validated behavior is intended, the helper function should be explicit.
    if line.contains("https://docs.convex.dev/production/best-practices/#use-helper-functions-to-write-shared-code") {
        tracing::warn!("Direct function call detected: '{line}'");
        log_direct_function_call();
    }
}

register_convex_histogram!(
    UDF_SYSCALL_SECONDS,
    "Duration of UDF syscall",
    &["status", "syscall"]
);
pub fn syscall_timer(op_name: &str) -> StatusTimer {
    let mut t = StatusTimer::new(&UDF_SYSCALL_SECONDS);
    t.add_label(StaticMetricLabel::new("syscall", op_name.to_owned()));
    t
}

register_convex_histogram!(
    UDF_ASYNC_SYSCALL_SECONDS,
    "Duration of UDF async syscall",
    &["status", "syscall"]
);
pub fn async_syscall_timer(op_name: &str) -> StatusTimer {
    let mut t = StatusTimer::new(&UDF_ASYNC_SYSCALL_SECONDS);
    t.add_label(StaticMetricLabel::new("syscall", op_name.to_owned()));
    t
}

register_convex_counter!(
    UDF_UNAWAITED_OP_TOTAL,
    "Count of async syscalls/ops still pending when a function resolves",
    &["environment"],
);
pub fn log_unawaited_pending_op(count: usize, environment: &'static str) {
    log_counter_with_labels(
        &UDF_UNAWAITED_OP_TOTAL,
        count as u64,
        vec![StaticMetricLabel::new("environment", environment)],
    );
}

register_convex_counter!(
    UDF_SOURCE_MAP_FAILURE_TOTAL,
    "Number of source map failures"
);
pub fn log_source_map_failure(exception_message: &str, e: &anyhow::Error) {
    tracing::error!("Failed to extract error from {exception_message:?}: {e}");
    log_counter(&UDF_SOURCE_MAP_FAILURE_TOTAL, 1);
}

register_convex_counter!(UDF_USER_TIMEOUT_TOTAL, "Number of UDF user timeouts");
pub fn log_user_timeout() {
    log_counter(&UDF_USER_TIMEOUT_TOTAL, 1);
}

register_convex_counter!(UDF_SYSTEM_TIMEOUT_TOTAL, "Number of UDF system timeouts");
pub fn log_system_timeout() {
    log_counter(&UDF_SYSTEM_TIMEOUT_TOTAL, 1);
}

register_convex_counter!(
    ARRAY_BUFFER_OOM_TOTAL,
    "Number of times that isolates hit the ArrayBuffer memory limit"
);
pub fn log_array_buffer_oom() {
    log_counter(&ARRAY_BUFFER_OOM_TOTAL, 1);
}

register_convex_counter!(
    RECREATE_ISOLATE_TOTAL,
    "Number of times an isolate is recreated",
    &["reason"]
);
pub fn log_recreate_isolate(reason: &'static str) {
    log_counter_with_labels(
        &RECREATE_ISOLATE_TOTAL,
        1,
        vec![StaticMetricLabel::new("reason", reason)],
    )
}

register_convex_counter!(
    ISOLATE_REQUEST_CANCELED_TOTAL,
    "Number of times an isolate execution have exited due to cancellation",
);
pub fn log_isolate_request_cancelled() {
    log_counter(&ISOLATE_REQUEST_CANCELED_TOTAL, 1)
}

register_convex_counter!(
    PROMISE_HANDLER_ADDED_AFTER_REJECT_TOTAL,
    "Number of times a promise handler was added after rejection"
);
pub fn log_promise_handler_added_after_reject() {
    log_counter(&PROMISE_HANDLER_ADDED_AFTER_REJECT_TOTAL, 1);
}

register_convex_counter!(
    PROMISE_REJECTED_AFTER_RESOLVED_TOTAL,
    "Number of times a promise was rejected after it was resolved"
);
pub fn log_promise_rejected_after_resolved() {
    log_counter(&PROMISE_REJECTED_AFTER_RESOLVED_TOTAL, 1);
}

register_convex_counter!(
    PROMISE_RESOLVED_AFTER_RESOLVED_TOTAL,
    "Number of times a promise was resolved after it was resolved"
);
pub fn log_promise_resolved_after_resolved() {
    log_counter(&PROMISE_RESOLVED_AFTER_RESOLVED_TOTAL, 1);
}

register_convex_histogram!(ISOLATE_USED_HEAP_SIZE_BYTES, "Isolate used heap size");
register_convex_histogram!(ISOLATE_HEAP_SIZE_LIMIT_BYTES, "Isolate heap size limit");
register_convex_histogram!(ISOLATE_AVAILABLE_SIZE_BYTES, "Isolate available size");
register_convex_histogram!(ISOLATE_HEAP_SIZE_BYTES, "Isolate heap size");
register_convex_histogram!(
    ISOLATE_HEAP_SIZE_EXECUTABLE_BYTES,
    "Isolate executable heap size "
);
register_convex_histogram!(ISOLATE_EXTERNAL_MEMORY_BYTES, "Isolate external memory");
register_convex_histogram!(ISOLATE_PHYSICAL_SIZE_BYTES, "Isolate physical size");
register_convex_histogram!(ISOLATE_MALLOCED_MEMORY_BYTES, "Isolate malloc'd memory");
register_convex_histogram!(
    ISOLATE_PEAK_MALLOCED_MEMORY_BYTES,
    "Isolate peak malloc'd memory"
);
register_convex_histogram!(
    ISOLATE_GLOBAL_HANDLES_SIZE_BYTES,
    "Isolate size of all global handles"
);
register_convex_histogram!(
    ISOLATE_NATIVE_CONTEXT_TOTAL,
    "Isolate number of native contexts"
);
register_convex_histogram!(
    ISOLATE_DETACHED_CONTEXT_TOTAL,
    "Isolate number of detached contexts"
);
/// Heap statistics currently logged before building the Context and running the
/// UDF, to detect leaks between UDFs.
pub fn log_heap_statistics(stats: &v8::HeapStatistics) {
    log_distribution(&ISOLATE_USED_HEAP_SIZE_BYTES, stats.used_heap_size() as f64);
    log_distribution(
        &ISOLATE_HEAP_SIZE_LIMIT_BYTES,
        stats.heap_size_limit() as f64,
    );
    log_distribution(
        &ISOLATE_AVAILABLE_SIZE_BYTES,
        stats.total_available_size() as f64,
    );
    log_distribution(&ISOLATE_HEAP_SIZE_BYTES, stats.total_heap_size() as f64);
    log_distribution(
        &ISOLATE_HEAP_SIZE_EXECUTABLE_BYTES,
        stats.total_heap_size_executable() as f64,
    );
    log_distribution(
        &ISOLATE_EXTERNAL_MEMORY_BYTES,
        stats.external_memory() as f64,
    );
    log_distribution(
        &ISOLATE_PHYSICAL_SIZE_BYTES,
        stats.total_physical_size() as f64,
    );
    log_distribution(
        &ISOLATE_MALLOCED_MEMORY_BYTES,
        stats.malloced_memory() as f64,
    );
    log_distribution(
        &ISOLATE_PEAK_MALLOCED_MEMORY_BYTES,
        stats.peak_malloced_memory() as f64,
    );
    log_distribution(
        &ISOLATE_GLOBAL_HANDLES_SIZE_BYTES,
        stats.total_global_handles_size() as f64,
    );

    log_distribution(
        &ISOLATE_NATIVE_CONTEXT_TOTAL,
        stats.number_of_native_contexts() as f64,
    );
    log_distribution(
        &ISOLATE_DETACHED_CONTEXT_TOTAL,
        stats.number_of_detached_contexts() as f64,
    );
}

register_convex_gauge!(
    ISOLATE_TOTAL_USED_HEAP_SIZE_BYTES,
    "Total isolate used heap size across all isolates"
);
register_convex_gauge!(
    ISOLATE_TOTAL_HEAP_SIZE_BYTES,
    "Total isolate heap size across all isolates"
);
register_convex_gauge!(
    ISOLATE_TOTAL_HEAP_SIZE_EXECUTABLE_BYTES,
    "Total isolate executable heap siz across all isolates "
);
register_convex_gauge!(
    ISOLATE_TOTAL_EXTERNAL_MEMORY_BYTES,
    "Total isolate external memory across all isolates"
);
register_convex_gauge!(
    ISOLATE_TOTAL_PHYSICAL_SIZE_BYTES,
    "Total isolate physical size across all isolates"
);
register_convex_gauge!(
    ISOLATE_TOTAL_MALLOCED_MEMORY_BYTES,
    "Total isolate malloc'd memory across all isolates"
);
register_convex_gauge!(
    ISOLATE_TOTAL_ARRAY_BUFFER_MEMORY_BYTES,
    "Total isolate ArrayBuffer-allocated memory across all isolates"
);

pub fn log_aggregated_heap_stats(stats: &IsolateHeapStats) {
    log_gauge(
        &ISOLATE_TOTAL_USED_HEAP_SIZE_BYTES,
        stats.v8_used_heap_size as f64,
    );
    log_gauge(
        &ISOLATE_TOTAL_HEAP_SIZE_BYTES,
        stats.v8_total_heap_size as f64,
    );
    log_gauge(
        &ISOLATE_TOTAL_HEAP_SIZE_EXECUTABLE_BYTES,
        stats.v8_total_heap_size_executable as f64,
    );
    log_gauge(
        &ISOLATE_TOTAL_EXTERNAL_MEMORY_BYTES,
        stats.v8_external_memory_bytes as f64,
    );
    log_gauge(
        &ISOLATE_TOTAL_PHYSICAL_SIZE_BYTES,
        stats.v8_total_physical_size as f64,
    );
    log_gauge(
        &ISOLATE_TOTAL_MALLOCED_MEMORY_BYTES,
        stats.v8_malloced_memory as f64,
    );
    log_gauge(
        &ISOLATE_TOTAL_ARRAY_BUFFER_MEMORY_BYTES,
        stats.array_buffer_size as f64,
    );
}

register_convex_histogram!(UDF_FETCH_SECONDS, "Duration of UDF fetch", &STATUS_LABEL);
pub fn udf_fetch_timer() -> StatusTimer {
    StatusTimer::new(&UDF_FETCH_SECONDS)
}

register_convex_histogram!(CREATE_ISOLATE_SECONDS, "Time to create a new isolate");
pub fn create_isolate_timer() -> Timer<prometheus::VMHistogram> {
    Timer::new(&CREATE_ISOLATE_SECONDS)
}

register_convex_histogram!(DESTROY_ISOLATE_SECONDS, "Time to destroy an isolate");
pub fn destroy_isolate_timer() -> Timer<prometheus::VMHistogram> {
    Timer::new(&DESTROY_ISOLATE_SECONDS)
}

register_convex_histogram!(CREATE_CONTEXT_SECONDS, "Time to create a new V8 context");
pub fn create_context_timer() -> Timer<prometheus::VMHistogram> {
    Timer::new(&CREATE_CONTEXT_SECONDS)
}

register_convex_histogram!(
    CREATE_CODE_CACHE_SECONDS,
    "Time to create a code cache for a module",
    &STATUS_LABEL
);
pub fn create_code_cache_timer() -> StatusTimer {
    StatusTimer::new(&CREATE_CODE_CACHE_SECONDS)
}

register_convex_histogram!(
    CONCURRENCY_PERMIT_ACQUIRE_SECONDS,
    "Time waiting for active-JavaScript admission by effective service class and phase",
    &[STATUS_LABEL[0], "active_javascript_class", "phase"]
);

fn active_javascript_class_label(class: ActiveJavascriptClass) -> StaticMetricLabel {
    StaticMetricLabel::new(
        "active_javascript_class",
        match class {
            ActiveJavascriptClass::Dependency => "dependency",
            ActiveJavascriptClass::Protected => "protected",
            ActiveJavascriptClass::Degradable => "degradable",
        },
    )
}

fn active_javascript_phase_label(phase: &'static str) -> StaticMetricLabel {
    StaticMetricLabel::new("phase", phase)
}

pub(crate) fn concurrency_permit_acquire_timer(
    class: ActiveJavascriptClass,
    phase: &'static str,
) -> CancelableTimer {
    let mut timer = CancelableTimer::new(&CONCURRENCY_PERMIT_ACQUIRE_SECONDS);
    timer.add_label(active_javascript_class_label(class));
    timer.add_label(active_javascript_phase_label(phase));
    timer
}

register_convex_gauge!(
    ACTIVE_JAVASCRIPT_CAPACITY_INFO,
    "Configured active-JavaScript total and class minimums by kind; zero total means unlimited",
    &["capacity_kind"]
);
register_convex_gauge!(
    ACTIVE_JAVASCRIPT_OCCUPANCY_INFO,
    "Active-JavaScript permits held or granted by effective service class",
    &["active_javascript_class"]
);
register_convex_gauge!(
    ACTIVE_JAVASCRIPT_WAITERS_INFO,
    "Queued, not-yet-granted active-JavaScript waiters by effective service class and phase",
    &["active_javascript_class", "phase"]
);

pub(crate) fn initialize_active_javascript_metrics(
    total: usize,
    protected_minimum: usize,
    degradable_minimum: usize,
) {
    for (capacity_kind, capacity) in [
        ("total", total),
        ("protected_minimum", protected_minimum),
        ("degradable_minimum", degradable_minimum),
    ] {
        log_gauge_with_labels(
            &ACTIVE_JAVASCRIPT_CAPACITY_INFO,
            capacity as f64,
            vec![StaticMetricLabel::new("capacity_kind", capacity_kind)],
        );
    }
    static INITIALIZE_OCCUPANCY: Once = Once::new();
    INITIALIZE_OCCUPANCY.call_once(|| {
        for class in [
            ActiveJavascriptClass::Dependency,
            ActiveJavascriptClass::Protected,
            ActiveJavascriptClass::Degradable,
        ] {
            log_gauge_with_labels(
                &ACTIVE_JAVASCRIPT_OCCUPANCY_INFO,
                0.0,
                vec![active_javascript_class_label(class)],
            );
            for phase in ["initial", "resume"] {
                log_gauge_with_labels(
                    &ACTIVE_JAVASCRIPT_WAITERS_INFO,
                    0.0,
                    vec![
                        active_javascript_class_label(class),
                        active_javascript_phase_label(phase),
                    ],
                );
            }
        }
    });
}

pub(crate) fn increment_active_javascript_occupancy(class: ActiveJavascriptClass) {
    add_to_gauge_with_labels(
        &ACTIVE_JAVASCRIPT_OCCUPANCY_INFO,
        1.0,
        vec![active_javascript_class_label(class)],
    );
}

pub(crate) fn decrement_active_javascript_occupancy(class: ActiveJavascriptClass) {
    subtract_from_gauge_with_labels(
        &ACTIVE_JAVASCRIPT_OCCUPANCY_INFO,
        1.0,
        vec![active_javascript_class_label(class)],
    );
}

pub(crate) fn increment_active_javascript_waiters(
    class: ActiveJavascriptClass,
    phase: &'static str,
) {
    add_to_gauge_with_labels(
        &ACTIVE_JAVASCRIPT_WAITERS_INFO,
        1.0,
        vec![
            active_javascript_class_label(class),
            active_javascript_phase_label(phase),
        ],
    );
}

pub(crate) fn decrement_active_javascript_waiters(
    class: ActiveJavascriptClass,
    phase: &'static str,
) {
    subtract_from_gauge_with_labels(
        &ACTIVE_JAVASCRIPT_WAITERS_INFO,
        1.0,
        vec![
            active_javascript_class_label(class),
            active_javascript_phase_label(phase),
        ],
    );
}

register_convex_counter!(
    CONCURRENCY_PERMIT_TOTAL_HOLD_TIME_SECONDS,
    "The total time concurrency limit was held for ",
    &["client_id"]
);
pub fn log_concurrency_permit_used(client_id: Arc<String>, duration: Duration) {
    let duration_ms = duration
        .as_millis()
        .try_into()
        .expect("Hold duration is too long {}");
    // This is fairly high cardinality but also super important metric.
    if duration_ms > 0 {
        log_counter_with_labels(
            &CONCURRENCY_PERMIT_TOTAL_HOLD_TIME_SECONDS,
            duration_ms,
            vec![StaticMetricLabel::new("client_id", client_id.to_string())],
        );
    }
}

register_convex_counter!(UDF_FETCH_TOTAL, "Number of UDF fetches", &STATUS_LABEL);
register_convex_counter!(UDF_FETCH_BYTES_TOTAL, "Number of bytes fetched in UDFs");
pub fn finish_udf_fetch_timer(t: StatusTimer, success: Result<usize, ()>) {
    let status_label = if let Ok(size) = success {
        t.finish();
        log_counter(&UDF_FETCH_BYTES_TOTAL, size as u64);
        StaticMetricLabel::STATUS_SUCCESS
    } else {
        StaticMetricLabel::STATUS_ERROR
    };
    log_counter_with_labels(&UDF_FETCH_TOTAL, 1, vec![status_label]);
}

// Analyze counters
register_convex_counter!(
    SOURCE_MAP_MISSING_TOTAL,
    "Number of times source map is missing during a UDF or HTTP analysis"
);
pub fn log_source_map_missing() {
    log_counter(&SOURCE_MAP_MISSING_TOTAL, 1);
}

register_convex_counter!(
    SOURCE_MAP_TOKEN_LOOKUP_FAILED_TOTAL,
    "Number of times source map exists but token lookup yields an invalid value during a UDF or \
     HTTP analysis"
);
pub fn log_source_map_token_lookup_failed() {
    log_counter(&SOURCE_MAP_TOKEN_LOOKUP_FAILED_TOTAL, 1);
}

register_convex_counter!(
    SOURCE_MAP_ORIGIN_IN_SEPARATE_MODULE_TOTAL,
    "Number of times the origin of a V8 Function is in a separate module during a UDF or HTTP \
     analysis"
);
pub fn log_source_map_origin_in_separate_module() {
    log_counter(&SOURCE_MAP_ORIGIN_IN_SEPARATE_MODULE_TOTAL, 1);
}

register_convex_counter!(
    ISOLATE_OUT_OF_MEMORY_TOTAL,
    "Number of times isolate ran out of memory during function execution"
);
pub fn log_isolate_out_of_memory() {
    log_counter(&ISOLATE_OUT_OF_MEMORY_TOTAL, 1);
}

pub fn record_component_function_path(component_function_path: &ResolvedComponentFunctionPath) {
    LocalSpan::add_event(Event::new("component_function_path").with_properties(|| {
        let mut labels = vec![(
            Cow::Borrowed("udf_path"),
            Cow::Owned(component_function_path.udf_path.to_string()),
        )];
        if !component_function_path.component_path.is_root() {
            labels.push((
                Cow::Borrowed("component"),
                Cow::Owned(component_function_path.component_path.to_string()),
            ));
        }
        labels
    }));
}

register_convex_counter!(
    HTTP_ACTION_WITH_UNKNOWN_IDENTITY_TOTAL,
    "Number of HTTP actions that were called with an unknown identity",
);

pub fn log_http_action_with_unknown_identity() {
    log_counter(&HTTP_ACTION_WITH_UNKNOWN_IDENTITY_TOTAL, 1);
}

register_convex_counter!(
    RUN_UDF_TOTAL,
    "Number of times that UDFs invoke nested UDFs",
    &[
        "outer_type",
        "inner_type",
        "outer_observed_identity",
        "inner_observed_identity"
    ]
);

pub fn log_run_udf(
    outer_type: UdfType,
    inner_type: UdfType,
    outer_observed_identity: bool,
    inner_observed_identity: bool,
) {
    log_counter_with_labels(
        &RUN_UDF_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("outer_type", outer_type.to_lowercase_string()),
            StaticMetricLabel::new("inner_type", inner_type.to_lowercase_string()),
            StaticMetricLabel::new(
                "outer_observed_identity",
                outer_observed_identity.as_label(),
            ),
            StaticMetricLabel::new(
                "inner_observed_identity",
                inner_observed_identity.as_label(),
            ),
        ],
    );
}

register_convex_counter!(
    COMPONENT_GET_USER_IDENTITY_TOTAL,
    "Number of times that components call getUserIdentity()",
    &["has_user_identity"]
);

pub fn log_component_get_user_identity(has_user_identity: bool) {
    log_counter_with_labels(
        &COMPONENT_GET_USER_IDENTITY_TOTAL,
        1,
        vec![StaticMetricLabel::new(
            "has_user_identity",
            has_user_identity.as_label(),
        )],
    );
}

register_convex_histogram!(
    USER_FUNCTION_EXECUTION_SECONDS,
    "Time running user code for a function in the isolate",
    &["udf_type"]
);
pub fn log_user_function_execution_time(udf_type: UdfType, execution_time: Duration) {
    log_distribution_with_labels(
        &USER_FUNCTION_EXECUTION_SECONDS,
        execution_time.as_secs_f64(),
        vec![StaticMetricLabel::new(
            "udf_type",
            udf_type.to_lowercase_string(),
        )],
    );
}

register_convex_counter!(
    REUSABLE_CONTEXT_INIT_TOTAL,
    "Number of UDF and action attempts entering reusable-context initialization",
    &["udf_type", "reused"],
);

pub fn log_reusable_context_init(udf_type: UdfType, reused: bool) {
    log_counter_with_labels(
        &REUSABLE_CONTEXT_INIT_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("udf_type", udf_type.to_lowercase_string()),
            StaticMetricLabel::new("reused", reused.as_label()),
        ],
    );
}
register_convex_counter!(
    NORMALIZE_ID_OLD_FORMAT_TOTAL,
    "Number of successful calls to normalizeId with old ID formats",
    &["format"],
);

pub fn log_normalize_id_old_format(format: &'static str) {
    log_counter_with_labels(
        &NORMALIZE_ID_OLD_FORMAT_TOTAL,
        1,
        vec![StaticMetricLabel::new("format", format)],
    );
}

register_convex_counter!(
    DATABASE_UDF_CONTEXT_REUSE_LOOKUP_TOTAL,
    "Number of reusable database UDF context lookups by outcome",
    &["udf_type", "outcome"],
    std::time::Duration::MAX,
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DatabaseUdfContextReuseLookupOutcome {
    NotFound,
    ValidationFailed,
    ValidationError,
    Hit,
}

impl DatabaseUdfContextReuseLookupOutcome {
    fn metric_label(self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::ValidationFailed => "validation_failed",
            Self::ValidationError => "validation_error",
            Self::Hit => "hit",
        }
    }
}

pub(crate) fn log_database_udf_context_reuse_lookup(
    udf_type: UdfType,
    outcome: DatabaseUdfContextReuseLookupOutcome,
) {
    let udf_type = match udf_type {
        UdfType::Query => "query",
        UdfType::Mutation => "mutation",
        UdfType::Action | UdfType::HttpAction => {
            unreachable!("database UDF context lookup recorded for an action")
        },
    };
    log_counter_with_labels(
        &DATABASE_UDF_CONTEXT_REUSE_LOOKUP_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("udf_type", udf_type),
            StaticMetricLabel::new("outcome", outcome.metric_label()),
        ],
    );
}

register_convex_counter!(
    ISOLATE_CONTEXT_CACHE_OPERATIONS_TOTAL,
    "Number of reusable isolate context cache operations",
    &["context_kind", "operation"],
    std::time::Duration::MAX,
);

pub(crate) fn log_context_cache_operation(
    context_kind: ReusableContextKind,
    operation: ContextCacheOperation,
) {
    log_counter_with_labels(
        &ISOLATE_CONTEXT_CACHE_OPERATIONS_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("context_kind", reusable_context_kind_label(context_kind)),
            StaticMetricLabel::new("operation", context_cache_operation_label(operation)),
        ],
    );
}

register_convex_counter!(
    ISOLATE_CONTEXT_CACHE_CLEARED_TOTAL,
    "Number of saved reusable isolate contexts cleared by reason",
    &["context_kind", "reason"],
    std::time::Duration::MAX,
);

pub(crate) fn log_context_cache_cleared(
    context_kind: ReusableContextKind,
    reason: ContextCacheClearReason,
) {
    log_counter_with_labels(
        &ISOLATE_CONTEXT_CACHE_CLEARED_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("context_kind", reusable_context_kind_label(context_kind)),
            StaticMetricLabel::new("reason", context_cache_clear_reason_label(reason)),
        ],
    );
}

register_convex_gauge!(
    ISOLATE_CONTEXT_CACHE_ENTRIES_INFO,
    "Current number of saved reusable isolate contexts",
    &["context_kind"],
);

register_convex_gauge!(
    ISOLATE_CONTEXT_CACHE_CAPACITY_INFO,
    "Configured reusable isolate context cache capacity",
    &["pool_name", "scope"],
);
register_convex_gauge!(
    ISOLATE_CONTEXT_CACHE_OWNED_INFO,
    "Current reusable isolate contexts owning shared pool capacity, including contexts in flight",
    &["pool_name"],
);
register_convex_gauge!(
    ISOLATE_MEMORY_CAPACITY_BYTES,
    "Configured V8 isolate memory capacity before native runtime overhead",
    &["pool_name", "capacity_kind"],
);

pub(crate) fn log_context_cache_capacity(pool_name: &'static str, per_isolate: usize, pool: usize) {
    for (scope, capacity) in [("per_isolate", per_isolate), ("pool", pool)] {
        log_gauge_with_labels(
            &ISOLATE_CONTEXT_CACHE_CAPACITY_INFO,
            capacity as f64,
            vec![
                StaticMetricLabel::new("pool_name", pool_name),
                StaticMetricLabel::new("scope", scope),
            ],
        );
    }
}

pub(crate) fn log_context_cache_owned(pool_name: &'static str, owned: usize) {
    log_gauge_with_labels(
        &ISOLATE_CONTEXT_CACHE_OWNED_INFO,
        owned as f64,
        vec![StaticMetricLabel::new("pool_name", pool_name)],
    );
}

pub(crate) fn log_isolate_memory_capacity(
    pool_name: &'static str,
    heap_per_worker: usize,
    heap_pool: usize,
    array_buffer_per_worker: usize,
    array_buffer_pool: usize,
) {
    for (capacity_kind, capacity) in [
        ("heap_per_worker", heap_per_worker),
        ("heap_pool", heap_pool),
        ("array_buffer_per_worker", array_buffer_per_worker),
        ("array_buffer_pool", array_buffer_pool),
    ] {
        log_gauge_with_labels(
            &ISOLATE_MEMORY_CAPACITY_BYTES,
            capacity as f64,
            vec![
                StaticMetricLabel::new("pool_name", pool_name),
                StaticMetricLabel::new("capacity_kind", capacity_kind),
            ],
        );
    }
}

pub(crate) fn log_context_cache_entry_added(context_kind: ReusableContextKind) {
    add_to_gauge_with_labels(
        &ISOLATE_CONTEXT_CACHE_ENTRIES_INFO,
        1.0,
        vec![StaticMetricLabel::new(
            "context_kind",
            reusable_context_kind_label(context_kind),
        )],
    );
}

pub(crate) fn log_context_cache_entry_removed(context_kind: ReusableContextKind) {
    subtract_from_gauge_with_labels(
        &ISOLATE_CONTEXT_CACHE_ENTRIES_INFO,
        1.0,
        vec![StaticMetricLabel::new(
            "context_kind",
            reusable_context_kind_label(context_kind),
        )],
    );
}

#[cfg(test)]
mod tests {
    use prometheus::core::Collector;

    use super::{
        control_plane_lane_enabled_value,
        initialize_capacity_counters,
        ISOLATE_QUEUE_OVERLOAD_TRANSITIONS_TOTAL,
        ISOLATE_QUEUE_REJECTIONS_TOTAL,
        ISOLATE_SCHEDULER_REQUESTS_EXPIRED_TOTAL,
        ISOLATE_SCHEDULER_REQUESTS_REJECTED_TOTAL,
    };

    fn zero_series_for_pool<C: Collector>(collector: &C, pool_name: &str) -> usize {
        let families = collector.collect();
        families
            .iter()
            .flat_map(|family| family.get_metric())
            .filter(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.name() == "pool_name" && label.value() == pool_name)
            })
            .inspect(|metric| assert_eq!(metric.get_counter().value(), 0.0))
            .count()
    }

    #[test]
    fn control_plane_lane_enabled_metric_is_closed_boolean_state() {
        assert_eq!(control_plane_lane_enabled_value(false), 0.0);
        assert_eq!(control_plane_lane_enabled_value(true), 1.0);
    }

    #[test]
    fn capacity_counter_initialization_covers_closed_label_sets() {
        const POOL_NAME: &str = "capacity_counter_initialization_test";
        initialize_capacity_counters(POOL_NAME);

        assert_eq!(
            zero_series_for_pool(&*ISOLATE_SCHEDULER_REQUESTS_EXPIRED_TOTAL, POOL_NAME),
            5
        );
        assert_eq!(
            zero_series_for_pool(&*ISOLATE_SCHEDULER_REQUESTS_REJECTED_TOTAL, POOL_NAME),
            30
        );
        assert_eq!(
            zero_series_for_pool(&*ISOLATE_QUEUE_REJECTIONS_TOTAL, POOL_NAME),
            24
        );
        assert_eq!(
            zero_series_for_pool(&*ISOLATE_QUEUE_OVERLOAD_TRANSITIONS_TOTAL, POOL_NAME),
            8
        );
    }
}
