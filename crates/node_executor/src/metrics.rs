use std::time::Duration;

use metrics::{
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
    StaticMetricLabel,
    StatusTimer,
};
use model::source_packages::types::PackageSize;

register_convex_histogram!(
    NODE_EXECUTOR_TOTAL_SECONDS,
    "Duration of Node executor",
    &["status", "method"]
);
pub fn node_executor(method: &'static str) -> StatusTimer {
    let mut t = StatusTimer::new(&NODE_EXECUTOR_TOTAL_SECONDS);
    t.add_label(StaticMetricLabel::new("method", method));
    t
}

register_convex_histogram!(
    NODE_EXECUTOR_DOWNLOAD_SECONDS,
    "Download duration of Node executor"
);
pub fn log_download_time(elapsed: Duration) {
    log_distribution(&NODE_EXECUTOR_DOWNLOAD_SECONDS, elapsed.as_secs_f64());
}

register_convex_histogram!(
    NODE_EXECUTOR_IMPORT_SECONDS,
    "Import duration of Node executor"
);
pub fn log_import_time(elapsed: Duration) {
    log_distribution(&NODE_EXECUTOR_IMPORT_SECONDS, elapsed.as_secs_f64());
}

register_convex_histogram!(
    NODE_EXECUTOR_UDF_SECONDS,
    "UDF execution time in Node executor"
);
pub fn log_udf_time(elapsed: Duration) {
    log_distribution(&NODE_EXECUTOR_UDF_SECONDS, elapsed.as_secs_f64());
}

register_convex_histogram!(NODE_EXECUTOR_OVERHEAD_SECONDS, "Overhead of Node executor");
pub fn log_overhead(elapsed: Duration) {
    log_distribution(&NODE_EXECUTOR_OVERHEAD_SECONDS, elapsed.as_secs_f64());
}

register_convex_histogram!(
    NODE_EXECUTOR_LAMBDA_TOTAL_SECONDS,
    "Node executor total duration"
);
pub fn log_total_executor_time(elapsed: Duration) {
    log_distribution(&NODE_EXECUTOR_LAMBDA_TOTAL_SECONDS, elapsed.as_secs_f64());
}

register_convex_counter!(
    NODE_EXECUTOR_COLD_START_TOTAL,
    "Number of cold starts in the Node executor"
);
register_convex_counter!(
    NODE_EXECUTOR_NON_LAMBDA_RESPONSE_TOTAL,
    "Number of non-lambda responses in the Node executor"
);
pub fn log_function_execution(cold_start: Option<bool>) {
    match cold_start {
        Some(cold_start) => {
            let value = if cold_start { 1 } else { 0 };
            log_counter(&NODE_EXECUTOR_COLD_START_TOTAL, value);
        },
        None => {
            // If cold_start is not set, the error didn't come up from the function
            // executor.
            log_counter(&NODE_EXECUTOR_NON_LAMBDA_RESPONSE_TOTAL, 1);
        },
    }
}

register_convex_counter!(
    NODE_SOURCE_MAP_MISSING_TOTAL,
    "Number of times source map is missing during a UDF or HTTP analysis"
);
pub fn log_node_source_map_missing() {
    log_counter(&NODE_SOURCE_MAP_MISSING_TOTAL, 1);
}

register_convex_counter!(
    NODE_SOURCE_MAP_TOKEN_LOOKUP_FAILED_TOTAL,
    "Number of times source map exists but token lookup yields an invalid value during a UDF or \
     HTTP analysis"
);
pub fn log_node_source_map_token_lookup_failed() {
    log_counter(&NODE_SOURCE_MAP_TOKEN_LOOKUP_FAILED_TOTAL, 1);
}

register_convex_histogram!(
    EXTERNAL_DEPS_SIZE_BYTES_TOTAL,
    "Size of external deps",
    &["compressed"],
);
pub fn log_external_deps_size_bytes_total(pkg_size: PackageSize) {
    let zipped_label = StaticMetricLabel::new("compressed", "true");
    let unzipped_label = StaticMetricLabel::new("compressed", "false");

    log_distribution_with_labels(
        &EXTERNAL_DEPS_SIZE_BYTES_TOTAL,
        pkg_size.zipped_size_bytes as f64,
        vec![zipped_label],
    );
    log_distribution_with_labels(
        &EXTERNAL_DEPS_SIZE_BYTES_TOTAL,
        pkg_size.unzipped_size_bytes as f64,
        vec![unzipped_label],
    );
}

register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_GENERATION_PRESENT_INFO,
    "Whether a local Node executor generation is currently available",
    &["pool_name"]
);
pub fn set_local_node_generation_present(pool_name: &str, present: bool) {
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_GENERATION_PRESENT_INFO,
        if present { 1.0 } else { 0.0 },
        vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_GENERATION_STARTS_TOTAL,
    "Number of local Node executor generations started",
    &["pool_name"]
);
pub fn log_local_node_generation_start(pool_name: &str) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_GENERATION_STARTS_TOTAL,
        1,
        vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_CHILD_STARTS_TOTAL,
    "Number of local Node executor server child processes started",
    &["pool_name"]
);
pub fn log_local_node_child_start(pool_name: &str) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_CHILD_STARTS_TOTAL,
        1,
        vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_CHILD_EXITS_TOTAL,
    "Number of local Node executor server child-process exits",
    &["pool_name", "class"]
);
pub fn log_local_node_child_exit(pool_name: &str, exit_class: &'static str) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_CHILD_EXITS_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("pool_name", pool_name.to_owned()),
            StaticMetricLabel::new("class", exit_class),
        ],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_GENERATION_RETIREMENTS_TOTAL,
    "Number of local Node executor generations retired",
    &["pool_name", "reason"]
);
pub fn log_local_node_generation_retirement(pool_name: &str, reason: &'static str) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_GENERATION_RETIREMENTS_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("pool_name", pool_name.to_owned()),
            StaticMetricLabel::new("reason", reason),
        ],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_RETIREMENT_DIAGNOSTICS_TOTAL,
    "Bounded diagnostic context for local Node executor generation retirement",
    &[
        "pool_name",
        "reason",
        "request_kind",
        "phase",
        "transport_error_kind"
    ]
);
pub fn log_local_node_retirement_diagnostics(
    pool_name: &str,
    reason: &'static str,
    request_kind: &'static str,
    phase: &'static str,
    transport_error_kind: &'static str,
) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_RETIREMENT_DIAGNOSTICS_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("pool_name", pool_name.to_owned()),
            StaticMetricLabel::new("reason", reason),
            StaticMetricLabel::new("request_kind", request_kind),
            StaticMetricLabel::new("phase", phase),
            StaticMetricLabel::new("transport_error_kind", transport_error_kind),
        ],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_FIRST_MISS_DIAGNOSTICS_TOTAL,
    "Outcomes of first-watchdog-miss local Node executor diagnostics",
    &["pool_name", "operation", "outcome"],
);

macro_rules! first_miss_diagnostic_outcomes {
    ($($variant:ident => ($operation:literal, $outcome:literal)),+ $(,)?) => {
        #[derive(Clone, Copy)]
        pub(crate) enum FirstMissDiagnosticOutcome {
            $($variant),+
        }

        impl FirstMissDiagnosticOutcome {
            const ALL: &'static [Self] = &[
                $(Self::$variant),+
            ];

            fn labels(self) -> (&'static str, &'static str) {
                match self {
                    $(Self::$variant => ($operation, $outcome)),+
                }
            }
        }
    };
}

first_miss_diagnostic_outcomes! {
    DiagnosticDirectorySuccess => ("diagnostic_directory", "success"),
    DiagnosticDirectoryFailure => ("diagnostic_directory", "failure"),
    RetentionSuccess => ("retention", "success"),
    RetentionFailure => ("retention", "failure"),
    DiagnosticReportRequested => ("diagnostic_report", "requested"),
    DiagnosticReportCompleted => ("diagnostic_report", "completed"),
    DiagnosticReportRequestFailed => ("diagnostic_report", "request_failed"),
    DiagnosticReportWriteFailed => ("diagnostic_report", "write_failed"),
    DiagnosticReportInvalidPid => ("diagnostic_report", "invalid_pid"),
    DiagnosticReportUnsupported => ("diagnostic_report", "unsupported"),
    ProcSnapshotCompleted => ("proc_snapshot", "completed"),
    ProcSnapshotWriteFailed => ("proc_snapshot", "write_failed"),
    ProcSnapshotSerializationFailed => ("proc_snapshot", "serialization_failed"),
    ProcSnapshotClockFailure => ("proc_snapshot", "clock_failure"),
    CpuProfileCompleted => ("cpu_profile", "completed"),
    CpuProfileAlreadyStarted => ("cpu_profile", "already_started"),
    CpuProfileEnableFailed => ("cpu_profile", "enable_failed"),
    CpuProfileStartFailed => ("cpu_profile", "start_failed"),
    CpuProfileStopFailed => ("cpu_profile", "stop_failed"),
    CpuProfileTooLarge => ("cpu_profile", "profile_too_large"),
    CpuProfileWriteFailed => ("cpu_profile", "write_failed"),
    CpuProfileTimeout => ("cpu_profile", "timeout"),
    CpuProfileTransportFailed => ("cpu_profile", "transport_failed"),
    CpuProfileResponseTooLarge => ("cpu_profile", "response_too_large"),
    CpuProfileInvalidResponse => ("cpu_profile", "invalid_response"),
    CpuProfileUnsupported => ("cpu_profile", "unsupported"),
}

fn log_local_node_first_miss_diagnostic_counter(
    pool_name: &str,
    outcome: FirstMissDiagnosticOutcome,
    increment: u64,
) {
    let (operation, outcome) = outcome.labels();
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_FIRST_MISS_DIAGNOSTICS_TOTAL,
        increment,
        vec![
            StaticMetricLabel::new("pool_name", pool_name.to_owned()),
            StaticMetricLabel::new("operation", operation),
            StaticMetricLabel::new("outcome", outcome),
        ],
    );
}

pub(crate) fn initialize_local_node_first_miss_diagnostic_counters(pool_name: &str) {
    for &outcome in FirstMissDiagnosticOutcome::ALL {
        log_local_node_first_miss_diagnostic_counter(pool_name, outcome, 0);
    }
}

pub(crate) fn log_local_node_first_miss_diagnostic(
    pool_name: &str,
    outcome: FirstMissDiagnosticOutcome,
) {
    log_local_node_first_miss_diagnostic_counter(pool_name, outcome, 1);
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_CHILD_TERMINATIONS_TOTAL,
    "Completed supervisor termination of retired local Node executor children",
    &[
        "pool_name",
        "reason",
        "state_before",
        "supervisor_kill_requested",
        "exit_class"
    ]
);
pub fn log_local_node_child_termination(
    pool_name: &str,
    reason: &'static str,
    state_before: &'static str,
    supervisor_kill_requested: bool,
    exit_class: &'static str,
) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_CHILD_TERMINATIONS_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("pool_name", pool_name.to_owned()),
            StaticMetricLabel::new("reason", reason),
            StaticMetricLabel::new("state_before", state_before),
            StaticMetricLabel::new(
                "supervisor_kill_requested",
                if supervisor_kill_requested {
                    "true"
                } else {
                    "false"
                },
            ),
            StaticMetricLabel::new("exit_class", exit_class),
        ],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_REPLACEMENT_OUTCOMES_TOTAL,
    "Outcomes of local Node executor replacement attempts",
    &["pool_name", "outcome"]
);
pub fn log_local_node_replacement_outcome(pool_name: &str, outcome: &'static str) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_REPLACEMENT_OUTCOMES_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("pool_name", pool_name.to_owned()),
            StaticMetricLabel::new("outcome", outcome),
        ],
    );
}

register_convex_histogram!(
    LOCAL_NODE_EXECUTOR_REPLACEMENT_SECONDS,
    "Time to start a replacement local Node executor after the next invocation",
    &["pool_name"]
);
pub fn log_local_node_replacement_time(pool_name: &str, elapsed: Duration) {
    log_distribution_with_labels(
        &LOCAL_NODE_EXECUTOR_REPLACEMENT_SECONDS,
        elapsed.as_secs_f64(),
        vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())],
    );
}

register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_GENERATION_AGE_SECONDS,
    "Age of the current local Node executor generation",
    &["pool_name"]
);
pub fn set_local_node_generation_age(pool_name: &str, age: Duration) {
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_GENERATION_AGE_SECONDS,
        age.as_secs_f64(),
        vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())],
    );
}

register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_OLD_SPACE_LIMIT_BYTES,
    "Configured V8 old-space allowance for the local Node executor child",
    &["pool_name"]
);
register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_RSS_RETIREMENT_THRESHOLD_BYTES,
    "Configured RSS threshold for graceful local Node executor generation retirement",
    &["pool_name"]
);
register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_RSS_THRESHOLD_BYTES,
    "Configured direct-child RSS threshold for retirement during sustained cgroup memory pressure",
    &["pool_name"]
);
register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_GRACE_SECONDS,
    "Configured cgroup memory-pressure duration before local Node executor retirement",
    &["pool_name"]
);
register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_AGE_RETIREMENT_THRESHOLD_SECONDS,
    "Configured age threshold for graceful local Node executor generation retirement",
    &["pool_name"]
);
register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_PACKAGE_RETIREMENT_THRESHOLD_INFO,
    "Configured lifetime imported source-package threshold for graceful local Node executor \
     generation retirement",
    &["pool_name"]
);
pub fn set_local_node_memory_configuration(
    pool_name: &str,
    old_space_limit_bytes: u64,
    rss_threshold_bytes: u64,
    memory_pressure_rss_threshold_bytes: u64,
    memory_pressure_grace: Duration,
    age_threshold: Duration,
    package_threshold: u64,
) {
    let labels = || vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())];
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_OLD_SPACE_LIMIT_BYTES,
        old_space_limit_bytes as f64,
        labels(),
    );
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_RSS_RETIREMENT_THRESHOLD_BYTES,
        rss_threshold_bytes as f64,
        labels(),
    );
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_RSS_THRESHOLD_BYTES,
        memory_pressure_rss_threshold_bytes as f64,
        labels(),
    );
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_GRACE_SECONDS,
        memory_pressure_grace.as_secs_f64(),
        labels(),
    );
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_AGE_RETIREMENT_THRESHOLD_SECONDS,
        age_threshold.as_secs_f64(),
        labels(),
    );
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_PACKAGE_RETIREMENT_THRESHOLD_INFO,
        package_threshold as f64,
        labels(),
    );
}

register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_ACTIVE_INFO,
    "Whether the current local Node executor generation observes cgroup memory pressure",
    &["pool_name"]
);
pub fn set_local_node_memory_pressure_active(pool_name: &str, active: bool) {
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_ACTIVE_INFO,
        if active { 1.0 } else { 0.0 },
        vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())],
    );
}

register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_CHILD_RSS_BYTES,
    "Resident memory of the current local Node executor child",
    &["pool_name"]
);
register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_CHILD_RSS_TELEMETRY_INFO,
    "Whether the latest local Node executor child RSS sample succeeded",
    &["pool_name"]
);
pub fn set_local_node_child_rss(pool_name: &str, rss: Option<u64>) {
    let labels = || vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())];
    match rss {
        Some(rss) => {
            log_gauge_with_labels(&LOCAL_NODE_EXECUTOR_CHILD_RSS_BYTES, rss as f64, labels());
            log_gauge_with_labels(&LOCAL_NODE_EXECUTOR_CHILD_RSS_TELEMETRY_INFO, 1.0, labels());
        },
        None => log_gauge_with_labels(&LOCAL_NODE_EXECUTOR_CHILD_RSS_TELEMETRY_INFO, 0.0, labels()),
    }
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_CHILD_RSS_SAMPLES_TOTAL,
    "Local Node executor direct-child RSS sampling outcomes",
    &["pool_name", "outcome"]
);
pub fn log_local_node_child_rss_sample(pool_name: &str, outcome: &'static str) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_CHILD_RSS_SAMPLES_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("pool_name", pool_name.to_owned()),
            StaticMetricLabel::new("outcome", outcome),
        ],
    );
}

register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_GENERATION_DRAINING_INFO,
    "Whether a local Node executor generation is in graceful drain",
    &["pool_name"]
);
pub fn set_local_node_generation_draining(pool_name: &str, draining: bool) {
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_GENERATION_DRAINING_INFO,
        if draining { 1.0 } else { 0.0 },
        vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_RETIREMENT_DECISIONS_TOTAL,
    "Local Node executor generation retirement decisions",
    &["pool_name", "reason", "decision"]
);
pub fn log_local_node_retirement_decision(
    pool_name: &str,
    reason: &'static str,
    decision: &'static str,
) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_RETIREMENT_DECISIONS_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("pool_name", pool_name.to_owned()),
            StaticMetricLabel::new("reason", reason),
            StaticMetricLabel::new("decision", decision),
        ],
    );
}

register_convex_histogram!(
    LOCAL_NODE_EXECUTOR_HEALTH_CHECK_SECONDS,
    "Duration of local Node executor health checks",
    &["pool_name", "phase", "outcome"]
);
pub fn log_local_node_health_check(
    pool_name: &str,
    elapsed: Duration,
    phase: &'static str,
    success: bool,
) {
    log_distribution_with_labels(
        &LOCAL_NODE_EXECUTOR_HEALTH_CHECK_SECONDS,
        elapsed.as_secs_f64(),
        vec![
            StaticMetricLabel::new("pool_name", pool_name.to_owned()),
            StaticMetricLabel::new("phase", phase),
            StaticMetricLabel::new("outcome", if success { "success" } else { "failure" }),
        ],
    );
}

register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_CONSECUTIVE_HEALTH_MISSES,
    "Consecutive failed health checks for the current local Node executor generation",
    &["pool_name"]
);
pub fn set_local_node_consecutive_health_misses(pool_name: &str, misses: u32) {
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_CONSECUTIVE_HEALTH_MISSES,
        misses as f64,
        vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())],
    );
}

register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_EVENT_LOOP_UNRESPONSIVE_BUDGET_SECONDS,
    "Configured elapsed event-loop unresponsiveness budget for a local Node executor pool; zero \
     preserves the legacy consecutive-miss policy",
    &["pool_name"]
);
pub fn set_local_node_event_loop_unresponsive_budget(pool_name: &str, budget: Option<Duration>) {
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_EVENT_LOOP_UNRESPONSIVE_BUDGET_SECONDS,
        budget.map_or(0.0, |budget| budget.as_secs_f64()),
        vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())],
    );
}

register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_WAITING_REQUESTS,
    "Current requests waiting for a local Node executor generation",
    &["pool_name"]
);
pub fn set_local_node_waiting_requests(pool_name: &str, waiting: usize) {
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_WAITING_REQUESTS,
        waiting as f64,
        vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_REQUEST_STARTS_TOTAL,
    "Number of local Node executor requests started",
    &["pool_name"]
);
pub fn log_local_node_request_start(pool_name: &str) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_REQUEST_STARTS_TOTAL,
        1,
        vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_REQUEST_COMPLETIONS_TOTAL,
    "Number of local Node executor requests completed",
    &["pool_name", "outcome"]
);
pub fn log_local_node_request_completion(pool_name: &str, outcome: &'static str) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_REQUEST_COMPLETIONS_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("pool_name", pool_name.to_owned()),
            StaticMetricLabel::new("outcome", outcome),
        ],
    );
}

register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_ACTIVE_REQUESTS,
    "Current requests assigned to local Node executor generations",
    &["pool_name"]
);
pub fn set_local_node_active_requests(pool_name: &str, active: usize) {
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_ACTIVE_REQUESTS,
        active as f64,
        vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())],
    );
}

register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_RETAINED_SOURCE_PACKAGES_INFO,
    "Retained dynamic source packages in the current local Node executor generation",
    &["pool_name"]
);
register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_IMPORTED_SOURCE_PACKAGES_INFO,
    "Lifetime-unique imported source-package roots in the current local Node executor generation",
    &["pool_name"]
);
register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_RETAINED_SOURCE_PACKAGE_BYTES,
    "Retained dynamic source-package bytes in the current local Node executor generation",
    &["pool_name"]
);
register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_ACTIVE_SOURCE_PACKAGE_OWNERS_INFO,
    "Active source-package owners in the current local Node executor generation",
    &["pool_name"]
);
register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_RETAINED_EXTERNAL_PACKAGES_INFO,
    "Retained dynamic external packages in the current local Node executor generation",
    &["pool_name"]
);
register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_RETAINED_EXTERNAL_PACKAGE_BYTES,
    "Retained dynamic external-package bytes in the current local Node executor generation",
    &["pool_name"]
);
register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_REGISTERED_STACK_ROOTS_INFO,
    "Registered source-package stack roots in the current local Node executor generation",
    &["pool_name"]
);
pub fn set_local_node_package_state(
    pool_name: &str,
    imported_source_packages: u64,
    source_packages: u64,
    source_bytes: u64,
    active_source_owners: u64,
    external_packages: u64,
    external_bytes: u64,
    stack_roots: u64,
) {
    let labels = || vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())];
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_IMPORTED_SOURCE_PACKAGES_INFO,
        imported_source_packages as f64,
        labels(),
    );
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_RETAINED_SOURCE_PACKAGES_INFO,
        source_packages as f64,
        labels(),
    );
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_RETAINED_SOURCE_PACKAGE_BYTES,
        source_bytes as f64,
        labels(),
    );
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_ACTIVE_SOURCE_PACKAGE_OWNERS_INFO,
        active_source_owners as f64,
        labels(),
    );
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_RETAINED_EXTERNAL_PACKAGES_INFO,
        external_packages as f64,
        labels(),
    );
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_RETAINED_EXTERNAL_PACKAGE_BYTES,
        external_bytes as f64,
        labels(),
    );
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_REGISTERED_STACK_ROOTS_INFO,
        stack_roots as f64,
        labels(),
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_PACKAGE_EVENTS_TOTAL,
    "Local Node executor package-cache events",
    &["pool_name", "package_kind", "operation"]
);
pub fn log_local_node_package_events(
    pool_name: &str,
    package_kind: &'static str,
    operation: &'static str,
    count: u64,
) {
    if count == 0 {
        return;
    }
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_PACKAGE_EVENTS_TOTAL,
        count,
        vec![
            StaticMetricLabel::new("pool_name", pool_name.to_owned()),
            StaticMetricLabel::new("package_kind", package_kind),
            StaticMetricLabel::new("operation", operation),
        ],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_STACK_FORMAT_INVOCATIONS_TOTAL,
    "Stack-format invocations in the local Node executor",
    &["pool_name"]
);
register_convex_counter!(
    LOCAL_NODE_EXECUTOR_STACK_FORMAT_FRAMES_TOTAL,
    "Stack frames processed in the local Node executor",
    &["pool_name"]
);
register_convex_histogram!(
    LOCAL_NODE_EXECUTOR_STACK_FORMAT_SECONDS,
    "Stack-format time accumulated between successful local Node executor health observations",
    &["pool_name"]
);
pub fn log_local_node_stack_format_deltas(
    pool_name: &str,
    invocations: u64,
    frames: u64,
    duration_ms: f64,
) {
    assert!(
        duration_ms.is_finite() && duration_ms >= 0.0,
        "Local Node executor stack-format duration delta is invalid"
    );
    if invocations > 0 {
        log_counter_with_labels(
            &LOCAL_NODE_EXECUTOR_STACK_FORMAT_INVOCATIONS_TOTAL,
            invocations,
            vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())],
        );
    }
    if frames > 0 {
        log_counter_with_labels(
            &LOCAL_NODE_EXECUTOR_STACK_FORMAT_FRAMES_TOTAL,
            frames,
            vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())],
        );
    }
    // A zero observation is measured idle stack-format work, not a missing
    // metric family. Record every successful-observation interval.
    log_distribution_with_labels(
        &LOCAL_NODE_EXECUTOR_STACK_FORMAT_SECONDS,
        duration_ms / 1000.0,
        vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())],
    );
}

register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_POOL_CONFIGURED_INFO,
    "Whether a local Node executor pool is configured",
    &["pool_name"]
);
register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_POOL_ROUTES_INFO,
    "Number of application modules assigned to a local Node executor pool",
    &["pool_name"]
);
pub fn set_local_node_pool_configuration(pool_name: &str, routes: Option<usize>) {
    let labels = || vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())];
    log_gauge_with_labels(&LOCAL_NODE_EXECUTOR_POOL_CONFIGURED_INFO, 1.0, labels());
    match routes {
        Some(routes) => log_gauge_with_labels(
            &LOCAL_NODE_EXECUTOR_POOL_ROUTES_INFO,
            routes as f64,
            labels(),
        ),
        None => {
            let _ = LOCAL_NODE_EXECUTOR_POOL_ROUTES_INFO.remove_label_values(&[pool_name]);
        },
    }
}

pub fn clear_local_node_pool_configuration(pool_name: &str) {
    for gauge in [
        &*LOCAL_NODE_EXECUTOR_POOL_CONFIGURED_INFO,
        &*LOCAL_NODE_EXECUTOR_POOL_ROUTES_INFO,
        &*LOCAL_NODE_EXECUTOR_OLD_SPACE_LIMIT_BYTES,
        &*LOCAL_NODE_EXECUTOR_RSS_RETIREMENT_THRESHOLD_BYTES,
        &*LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_RSS_THRESHOLD_BYTES,
        &*LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_GRACE_SECONDS,
        &*LOCAL_NODE_EXECUTOR_AGE_RETIREMENT_THRESHOLD_SECONDS,
        &*LOCAL_NODE_EXECUTOR_PACKAGE_RETIREMENT_THRESHOLD_INFO,
        &*LOCAL_NODE_EXECUTOR_EVENT_LOOP_UNRESPONSIVE_BUDGET_SECONDS,
    ] {
        let _ = gauge.remove_label_values(&[pool_name]);
    }
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_TOPOLOGY_PUBLICATIONS_TOTAL,
    "Outcomes of local Node executor topology publications",
    &["outcome"]
);
pub fn log_local_node_topology_publication(outcome: &'static str) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_TOPOLOGY_PUBLICATIONS_TOTAL,
        1,
        vec![StaticMetricLabel::new("outcome", outcome)],
    );
}

register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_CONFIGURED_NAMED_POOLS_INFO,
    "Current number of configured named local Node executor pools"
);
pub fn set_local_node_configured_named_pools(count: usize) {
    log_gauge(
        &LOCAL_NODE_EXECUTOR_CONFIGURED_NAMED_POOLS_INFO,
        count as f64,
    );
}

register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_SURGE_SLOT_INFO,
    "Current phase of the application-wide local Node executor surge slot",
    &["phase"]
);
pub fn set_local_node_surge_phase(phase: &'static str) {
    for candidate in ["unused", "reservation", "candidate", "draining"] {
        log_gauge_with_labels(
            &LOCAL_NODE_EXECUTOR_SURGE_SLOT_INFO,
            f64::from(candidate == phase),
            vec![StaticMetricLabel::new("phase", candidate)],
        );
    }
}

register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_SURGE_QUEUE_INFO,
    "Queued application-wide local Node executor surge transitions",
    &["priority"]
);
pub fn set_local_node_surge_queue(priority: &'static str, count: usize) {
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_SURGE_QUEUE_INFO,
        count as f64,
        vec![StaticMetricLabel::new("priority", priority)],
    );
}

register_convex_histogram!(
    LOCAL_NODE_EXECUTOR_SURGE_WAIT_SECONDS,
    "Time spent waiting for the application-wide local Node executor surge slot",
    &["priority", "outcome"]
);
pub fn log_local_node_surge_wait(priority: &'static str, elapsed: Duration, outcome: &'static str) {
    log_distribution_with_labels(
        &LOCAL_NODE_EXECUTOR_SURGE_WAIT_SECONDS,
        elapsed.as_secs_f64(),
        vec![
            StaticMetricLabel::new("priority", priority),
            StaticMetricLabel::new("outcome", outcome),
        ],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_DEPLOYMENT_CUTOVER_EVENTS_TOTAL,
    "Bounded application-wide local Node executor deployment cutover events",
    &["event"]
);
pub fn log_local_node_deployment_cutover_event(event: &'static str) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_DEPLOYMENT_CUTOVER_EVENTS_TOTAL,
        1,
        vec![StaticMetricLabel::new("event", event)],
    );
}

register_convex_gauge_evictable!(
    LOCAL_NODE_EXECUTOR_CANDIDATE_PRESENT_INFO,
    "Whether a local Node executor candidate is present",
    &["pool_name"]
);
pub fn set_local_node_candidate_present(pool_name: &str, present: bool) {
    log_gauge_with_labels(
        &LOCAL_NODE_EXECUTOR_CANDIDATE_PRESENT_INFO,
        f64::from(present),
        vec![StaticMetricLabel::new("pool_name", pool_name.to_owned())],
    );
}

register_convex_histogram!(
    LOCAL_NODE_EXECUTOR_CANDIDATE_PREPARATION_SECONDS,
    "Time spent preparing a local Node executor candidate",
    &["pool_name", "outcome"]
);
pub fn log_local_node_candidate_preparation(
    pool_name: &str,
    elapsed: Duration,
    outcome: &'static str,
) {
    log_distribution_with_labels(
        &LOCAL_NODE_EXECUTOR_CANDIDATE_PREPARATION_SECONDS,
        elapsed.as_secs_f64(),
        vec![
            StaticMetricLabel::new("pool_name", pool_name.to_owned()),
            StaticMetricLabel::new("outcome", outcome),
        ],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_ROUTE_REQUESTS_TOTAL,
    "Number of requests routed to local Node executor pools",
    &["pool_name", "request_kind"]
);
pub fn log_local_node_route_request(pool_name: &str, request_kind: &'static str) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_ROUTE_REQUESTS_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("pool_name", pool_name.to_owned()),
            StaticMetricLabel::new("request_kind", request_kind),
        ],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_FINGERPRINT_TRANSITIONS_TOTAL,
    "Outcomes of local Node executor resident-fingerprint transitions",
    &["pool_name", "outcome"]
);

#[derive(Clone, Copy)]
pub(crate) enum FingerprintTransitionOutcome {
    StartupFailed,
    ReplacementReady,
    Joined,
    RetirementFailed,
}

impl FingerprintTransitionOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::StartupFailed => "startup_failed",
            Self::ReplacementReady => "replacement_ready",
            Self::Joined => "joined",
            Self::RetirementFailed => "retirement_failed",
        }
    }
}

pub(crate) fn log_local_node_fingerprint_transition(
    pool_name: &str,
    outcome: FingerprintTransitionOutcome,
) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_FINGERPRINT_TRANSITIONS_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("pool_name", pool_name.to_owned()),
            StaticMetricLabel::new("outcome", outcome.as_str()),
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clearing_pool_configuration_removes_label_sets() {
        let pool_name = "metrics_removal_test";
        set_local_node_pool_configuration(pool_name, Some(3));
        set_local_node_memory_configuration(
            pool_name,
            1,
            2,
            3,
            Duration::from_secs(4),
            Duration::from_secs(5),
            6,
        );
        set_local_node_event_loop_unresponsive_budget(pool_name, Some(Duration::from_secs(7)));
        clear_local_node_pool_configuration(pool_name);
        assert!(LOCAL_NODE_EXECUTOR_POOL_CONFIGURED_INFO
            .remove_label_values(&[pool_name])
            .is_err());
        assert!(LOCAL_NODE_EXECUTOR_POOL_ROUTES_INFO
            .remove_label_values(&[pool_name])
            .is_err());
        assert!(LOCAL_NODE_EXECUTOR_OLD_SPACE_LIMIT_BYTES
            .remove_label_values(&[pool_name])
            .is_err());
        assert!(LOCAL_NODE_EXECUTOR_EVENT_LOOP_UNRESPONSIVE_BUDGET_SECONDS
            .remove_label_values(&[pool_name])
            .is_err());
    }
}
