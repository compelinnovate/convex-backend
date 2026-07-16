use std::time::Duration;

use metrics::{
    log_counter,
    log_counter_with_labels,
    log_distribution,
    log_distribution_with_labels,
    log_gauge,
    register_convex_counter,
    register_convex_gauge,
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

register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_GENERATION_PRESENT_INFO,
    "Whether a local Node executor generation is currently available"
);
pub fn set_local_node_generation_present(present: bool) {
    log_gauge(
        &LOCAL_NODE_EXECUTOR_GENERATION_PRESENT_INFO,
        if present { 1.0 } else { 0.0 },
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_GENERATION_STARTS_TOTAL,
    "Number of local Node executor generations started"
);
pub fn log_local_node_generation_start() {
    log_counter(&LOCAL_NODE_EXECUTOR_GENERATION_STARTS_TOTAL, 1);
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_CHILD_STARTS_TOTAL,
    "Number of local Node executor server child processes started"
);
pub fn log_local_node_child_start() {
    log_counter(&LOCAL_NODE_EXECUTOR_CHILD_STARTS_TOTAL, 1);
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_CHILD_EXITS_TOTAL,
    "Number of local Node executor server child-process exits",
    &["class"]
);
pub fn log_local_node_child_exit(exit_class: &'static str) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_CHILD_EXITS_TOTAL,
        1,
        vec![StaticMetricLabel::new("class", exit_class)],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_GENERATION_RETIREMENTS_TOTAL,
    "Number of local Node executor generations retired",
    &["reason"]
);
pub fn log_local_node_generation_retirement(reason: &'static str) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_GENERATION_RETIREMENTS_TOTAL,
        1,
        vec![StaticMetricLabel::new("reason", reason)],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_RETIREMENT_DIAGNOSTICS_TOTAL,
    "Bounded diagnostic context for local Node executor generation retirement",
    &["reason", "request_kind", "phase", "transport_error_kind"]
);
pub fn log_local_node_retirement_diagnostics(
    reason: &'static str,
    request_kind: &'static str,
    phase: &'static str,
    transport_error_kind: &'static str,
) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_RETIREMENT_DIAGNOSTICS_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("reason", reason),
            StaticMetricLabel::new("request_kind", request_kind),
            StaticMetricLabel::new("phase", phase),
            StaticMetricLabel::new("transport_error_kind", transport_error_kind),
        ],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_CHILD_TERMINATIONS_TOTAL,
    "Completed supervisor termination of retired local Node executor children",
    &[
        "reason",
        "state_before",
        "supervisor_kill_requested",
        "exit_class"
    ]
);
pub fn log_local_node_child_termination(
    reason: &'static str,
    state_before: &'static str,
    supervisor_kill_requested: bool,
    exit_class: &'static str,
) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_CHILD_TERMINATIONS_TOTAL,
        1,
        vec![
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
    &["outcome"]
);
pub fn log_local_node_replacement_outcome(outcome: &'static str) {
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_REPLACEMENT_OUTCOMES_TOTAL,
        1,
        vec![StaticMetricLabel::new("outcome", outcome)],
    );
}

register_convex_histogram!(
    LOCAL_NODE_EXECUTOR_REPLACEMENT_SECONDS,
    "Time to start a replacement local Node executor after the next invocation"
);
pub fn log_local_node_replacement_time(elapsed: Duration) {
    log_distribution(
        &LOCAL_NODE_EXECUTOR_REPLACEMENT_SECONDS,
        elapsed.as_secs_f64(),
    );
}

register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_GENERATION_AGE_SECONDS,
    "Age of the current local Node executor generation"
);
pub fn set_local_node_generation_age(age: Duration) {
    log_gauge(
        &LOCAL_NODE_EXECUTOR_GENERATION_AGE_SECONDS,
        age.as_secs_f64(),
    );
}

register_convex_histogram!(
    LOCAL_NODE_EXECUTOR_HEALTH_CHECK_SECONDS,
    "Duration of local Node executor health checks",
    &["phase", "outcome"]
);
pub fn log_local_node_health_check(elapsed: Duration, phase: &'static str, success: bool) {
    log_distribution_with_labels(
        &LOCAL_NODE_EXECUTOR_HEALTH_CHECK_SECONDS,
        elapsed.as_secs_f64(),
        vec![
            StaticMetricLabel::new("phase", phase),
            StaticMetricLabel::new("outcome", if success { "success" } else { "failure" }),
        ],
    );
}

register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_CONSECUTIVE_HEALTH_MISSES,
    "Consecutive failed health checks for the current local Node executor generation"
);
pub fn set_local_node_consecutive_health_misses(misses: u32) {
    log_gauge(
        &LOCAL_NODE_EXECUTOR_CONSECUTIVE_HEALTH_MISSES,
        misses as f64,
    );
}

register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_WAITING_REQUESTS,
    "Current requests waiting for a local Node executor generation"
);
pub fn set_local_node_waiting_requests(waiting: usize) {
    log_gauge(&LOCAL_NODE_EXECUTOR_WAITING_REQUESTS, waiting as f64);
}
pub fn increment_local_node_waiting_requests() {
    LOCAL_NODE_EXECUTOR_WAITING_REQUESTS.inc();
}
pub fn decrement_local_node_waiting_requests() {
    LOCAL_NODE_EXECUTOR_WAITING_REQUESTS.dec();
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_REQUEST_STARTS_TOTAL,
    "Number of local Node executor requests started"
);
pub fn log_local_node_request_start() {
    log_counter(&LOCAL_NODE_EXECUTOR_REQUEST_STARTS_TOTAL, 1);
    LOCAL_NODE_EXECUTOR_ACTIVE_REQUESTS.inc();
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_REQUEST_COMPLETIONS_TOTAL,
    "Number of local Node executor requests completed",
    &["outcome"]
);
pub fn log_local_node_request_completion(outcome: &'static str) {
    LOCAL_NODE_EXECUTOR_ACTIVE_REQUESTS.dec();
    log_counter_with_labels(
        &LOCAL_NODE_EXECUTOR_REQUEST_COMPLETIONS_TOTAL,
        1,
        vec![StaticMetricLabel::new("outcome", outcome)],
    );
}

register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_ACTIVE_REQUESTS,
    "Current requests assigned to local Node executor generations"
);
pub fn set_local_node_active_requests(active: usize) {
    log_gauge(&LOCAL_NODE_EXECUTOR_ACTIVE_REQUESTS, active as f64);
}

register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_RETAINED_SOURCE_PACKAGES_INFO,
    "Retained dynamic source packages in the current local Node executor generation"
);
register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_RETAINED_SOURCE_PACKAGE_BYTES,
    "Retained dynamic source-package bytes in the current local Node executor generation"
);
register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_ACTIVE_SOURCE_PACKAGE_OWNERS_INFO,
    "Active source-package owners in the current local Node executor generation"
);
register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_RETAINED_EXTERNAL_PACKAGES_INFO,
    "Retained dynamic external packages in the current local Node executor generation"
);
register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_RETAINED_EXTERNAL_PACKAGE_BYTES,
    "Retained dynamic external-package bytes in the current local Node executor generation"
);
register_convex_gauge!(
    LOCAL_NODE_EXECUTOR_REGISTERED_STACK_ROOTS_INFO,
    "Registered source-package stack roots in the current local Node executor generation"
);
pub fn set_local_node_package_state(
    source_packages: u64,
    source_bytes: u64,
    active_source_owners: u64,
    external_packages: u64,
    external_bytes: u64,
    stack_roots: u64,
) {
    log_gauge(
        &LOCAL_NODE_EXECUTOR_RETAINED_SOURCE_PACKAGES_INFO,
        source_packages as f64,
    );
    log_gauge(
        &LOCAL_NODE_EXECUTOR_RETAINED_SOURCE_PACKAGE_BYTES,
        source_bytes as f64,
    );
    log_gauge(
        &LOCAL_NODE_EXECUTOR_ACTIVE_SOURCE_PACKAGE_OWNERS_INFO,
        active_source_owners as f64,
    );
    log_gauge(
        &LOCAL_NODE_EXECUTOR_RETAINED_EXTERNAL_PACKAGES_INFO,
        external_packages as f64,
    );
    log_gauge(
        &LOCAL_NODE_EXECUTOR_RETAINED_EXTERNAL_PACKAGE_BYTES,
        external_bytes as f64,
    );
    log_gauge(
        &LOCAL_NODE_EXECUTOR_REGISTERED_STACK_ROOTS_INFO,
        stack_roots as f64,
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_PACKAGE_EVENTS_TOTAL,
    "Local Node executor package-cache events",
    &["package_kind", "operation"]
);
pub fn log_local_node_package_events(
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
            StaticMetricLabel::new("package_kind", package_kind),
            StaticMetricLabel::new("operation", operation),
        ],
    );
}

register_convex_counter!(
    LOCAL_NODE_EXECUTOR_STACK_FORMAT_INVOCATIONS_TOTAL,
    "Stack-format invocations in the local Node executor"
);
register_convex_counter!(
    LOCAL_NODE_EXECUTOR_STACK_FORMAT_FRAMES_TOTAL,
    "Stack frames processed in the local Node executor"
);
register_convex_histogram!(
    LOCAL_NODE_EXECUTOR_STACK_FORMAT_SECONDS,
    "Stack-format time accumulated between successful local Node executor health observations"
);
pub fn log_local_node_stack_format_deltas(invocations: u64, frames: u64, duration_ms: f64) {
    assert!(
        duration_ms.is_finite() && duration_ms >= 0.0,
        "Local Node executor stack-format duration delta is invalid"
    );
    if invocations > 0 {
        log_counter(
            &LOCAL_NODE_EXECUTOR_STACK_FORMAT_INVOCATIONS_TOTAL,
            invocations,
        );
    }
    if frames > 0 {
        log_counter(&LOCAL_NODE_EXECUTOR_STACK_FORMAT_FRAMES_TOTAL, frames);
    }
    // A zero observation is measured idle stack-format work, not a missing
    // metric family. Record every successful-observation interval.
    log_distribution(
        &LOCAL_NODE_EXECUTOR_STACK_FORMAT_SECONDS,
        duration_ms / 1000.0,
    );
}
