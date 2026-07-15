use std::time::Duration;

use common::identity::IDENTITY_LABEL;
use metrics::{
    log_counter,
    log_counter_with_labels,
    log_distribution,
    log_gauge,
    register_convex_counter,
    register_convex_gauge,
    register_convex_histogram,
    StaticMetricLabel,
    StatusTimer,
    STATUS_LABEL,
};
use strum::VariantArray;
register_convex_histogram!(
    CACHE_GET_SECONDS,
    "Time taken for a UDF cache read",
    &["status", "cache_status", "is_paginated"]
);
pub fn get_timer() -> StatusTimer {
    let mut t = StatusTimer::new(&CACHE_GET_SECONDS);
    // Start with the error tag until the application calls
    // `succeed_udf_read_timer`, which replaces it with the success tag. This
    // way the success case is the deliberate one, and we'll default to
    // accidentally logging errors over successes.
    t.add_label(StaticMetricLabel::new("cache_status", "unknown"));
    t.add_label(StaticMetricLabel::new("is_paginated", "unpaginated"));
    t
}

pub fn succeed_get_timer(mut timer: StatusTimer, is_cache_hit: bool, is_paginated: bool) {
    if is_cache_hit {
        timer.replace_label(
            StaticMetricLabel::new("cache_status", "unknown"),
            StaticMetricLabel::new("cache_status", "hit"),
        );
    } else {
        timer.replace_label(
            StaticMetricLabel::new("cache_status", "unknown"),
            StaticMetricLabel::new("cache_status", "miss"),
        );
    }
    if is_paginated {
        timer.replace_label(
            StaticMetricLabel::new("is_paginated", "unpaginated"),
            StaticMetricLabel::new("is_paginated", "paginated"),
        );
    }
    timer.finish();
}

register_convex_histogram!(
    CACHE_SUCCESS_ATTEMPTS_TOTAL,
    "Number of attempts needed on a successful cache fetch"
);

pub fn log_success(num_attempts: usize) {
    log_distribution(&CACHE_SUCCESS_ATTEMPTS_TOTAL, num_attempts as f64);
}

register_convex_counter!(
    CACHE_PLAN_READY_TOTAL,
    "Number of times a cache entry was already ready"
);
pub fn log_plan_ready() {
    log_counter(&CACHE_PLAN_READY_TOTAL, 1);
}

register_convex_counter!(
    CACHE_PLAN_PEER_TIMEOUT_TOTAL,
    "Number of times a peer was found to have timed out when computing a cache result"
);
pub fn log_plan_peer_timeout() {
    log_counter(&CACHE_PLAN_PEER_TIMEOUT_TOTAL, 1);
}

register_convex_counter!(
    CACHE_PLAN_WAIT_TOTAL,
    "Number of times an execution plans to wait for a cache result"
);
pub fn log_plan_wait() {
    log_counter(&CACHE_PLAN_WAIT_TOTAL, 1);
}

register_convex_counter!(
    DEGRADABLE_QUERY_LEADER_ADMISSION_TOTAL,
    "Immediate degradable query cache-miss leader admission decisions",
    &["outcome"],
    Duration::MAX
);
register_convex_gauge!(
    DEGRADABLE_QUERY_LEADER_PERMITS_IN_USE_INFO,
    "Degradable query cache-miss leader permits currently in use"
);
register_convex_gauge!(
    DEGRADABLE_QUERY_LEADER_CAPACITY_INFO,
    "Configured degradable query cache-miss leader capacity"
);
register_convex_counter!(
    DEGRADABLE_QUERY_CACHE_RECHECK_TOTAL,
    "Cache outcomes after acquiring a degradable query leader permit",
    &["outcome"],
    Duration::MAX
);
register_convex_counter!(
    DEGRADABLE_QUERY_CACHE_WAIT_TOTAL,
    "Degradable query cache waits by leader execution class",
    &["leader_class"],
    Duration::MAX
);

#[derive(Copy, Clone, Debug, Eq, PartialEq, strum::VariantArray)]
pub enum DegradableLeaderAdmissionOutcome {
    Admitted,
    Deferred,
}

impl DegradableLeaderAdmissionOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Deferred => "deferred",
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, strum::VariantArray)]
pub enum DegradableCacheRecheckOutcome {
    Published,
    Ready,
    Wait,
    DirectExecution,
    Retry,
}

impl DegradableCacheRecheckOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::Published => "published",
            Self::Ready => "ready",
            Self::Wait => "wait",
            Self::DirectExecution => "direct_execution",
            Self::Retry => "retry",
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, strum::VariantArray)]
pub enum DegradableCacheLeaderClass {
    Dependency,
    Normal,
    Degradable,
}

impl DegradableCacheLeaderClass {
    fn label(self) -> &'static str {
        match self {
            Self::Dependency => "dependency",
            Self::Normal => "normal",
            Self::Degradable => "degradable",
        }
    }
}

pub fn initialize_degradable_leader_metrics(capacity: usize) {
    for outcome in DegradableLeaderAdmissionOutcome::VARIANTS {
        log_counter_with_labels(
            &DEGRADABLE_QUERY_LEADER_ADMISSION_TOTAL,
            0,
            vec![StaticMetricLabel::new("outcome", outcome.label())],
        );
    }
    for outcome in DegradableCacheRecheckOutcome::VARIANTS {
        log_counter_with_labels(
            &DEGRADABLE_QUERY_CACHE_RECHECK_TOTAL,
            0,
            vec![StaticMetricLabel::new("outcome", outcome.label())],
        );
    }
    for leader_class in DegradableCacheLeaderClass::VARIANTS {
        log_counter_with_labels(
            &DEGRADABLE_QUERY_CACHE_WAIT_TOTAL,
            0,
            vec![StaticMetricLabel::new("leader_class", leader_class.label())],
        );
    }
    for reason in GoReason::VARIANTS {
        log_counter_with_labels(
            &CACHE_PLAN_GO_TOTAL,
            0,
            vec![StaticMetricLabel::new("reason", reason.label())],
        );
    }
    // Register the process-wide occupancy gauge without resetting permits held
    // by another live cache manager or a concurrent test instance.
    let _ = DEGRADABLE_QUERY_LEADER_PERMITS_IN_USE_INFO.get();
    log_gauge(&DEGRADABLE_QUERY_LEADER_CAPACITY_INFO, capacity as f64);
}

pub fn log_degradable_leader_admission(outcome: DegradableLeaderAdmissionOutcome) {
    log_counter_with_labels(
        &DEGRADABLE_QUERY_LEADER_ADMISSION_TOTAL,
        1,
        vec![StaticMetricLabel::new("outcome", outcome.label())],
    );
}

pub fn increment_degradable_leader_permits_in_use() {
    DEGRADABLE_QUERY_LEADER_PERMITS_IN_USE_INFO.inc();
}

pub fn decrement_degradable_leader_permits_in_use() {
    DEGRADABLE_QUERY_LEADER_PERMITS_IN_USE_INFO.dec();
}

pub fn log_degradable_cache_recheck(outcome: DegradableCacheRecheckOutcome) {
    log_counter_with_labels(
        &DEGRADABLE_QUERY_CACHE_RECHECK_TOTAL,
        1,
        vec![StaticMetricLabel::new("outcome", outcome.label())],
    );
}

pub fn log_degradable_cache_wait(leader_class: DegradableCacheLeaderClass) {
    log_counter_with_labels(
        &DEGRADABLE_QUERY_CACHE_WAIT_TOTAL,
        1,
        vec![StaticMetricLabel::new("leader_class", leader_class.label())],
    );
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, strum::VariantArray)]
pub enum GoReason {
    NoCacheResult,
    PeerTimestampTooNew,
    DependencyCannotWaitForIndependentPeer,
    DependencyCannotWaitForDegradablePeer,
    NormalCannotWaitForDegradablePeer,
}

impl GoReason {
    fn label(self) -> &'static str {
        match self {
            Self::NoCacheResult => "no_cache_result",
            Self::PeerTimestampTooNew => "peer_timestamp_too_new",
            Self::DependencyCannotWaitForIndependentPeer => {
                "dependency_cannot_wait_for_independent_peer"
            },
            Self::DependencyCannotWaitForDegradablePeer => {
                "dependency_cannot_wait_for_degradable_peer"
            },
            Self::NormalCannotWaitForDegradablePeer => "normal_cannot_wait_for_degradable_peer",
        }
    }
}

register_convex_counter!(
    CACHE_PLAN_GO_TOTAL,
    "Number of times an execution plans to compute the cache result",
    &["reason"]
);
pub fn log_plan_go(reason: GoReason) {
    let label = StaticMetricLabel::new("reason", reason.label());
    log_counter_with_labels(&CACHE_PLAN_GO_TOTAL, 1, vec![label]);
}

register_convex_counter!(
    CACHE_PERFORM_PEER_TIMEOUT_TOTAL,
    "Number of times a waiting execution determined that a peer timed out"
);
pub fn log_perform_wait_peer_timeout() {
    log_counter(&CACHE_PERFORM_PEER_TIMEOUT_TOTAL, 1);
}

register_convex_counter!(
    CACHE_PERFORM_SELF_TIMEOUT_TOTAL,
    "Number of times an execution determined its own cache computation timed out"
);
pub fn log_perform_wait_self_timeout() {
    log_counter(&CACHE_PERFORM_SELF_TIMEOUT_TOTAL, 1);
}
register_convex_counter!(
    CACHE_PERFORM_GO_TOTAL,
    "Number of times an execution begins computing a cache result",
    &STATUS_LABEL
);
pub fn log_perform_go(is_ok: bool) {
    log_counter_with_labels(
        &CACHE_PERFORM_GO_TOTAL,
        1,
        vec![StaticMetricLabel::status(is_ok)],
    );
}

register_convex_counter!(
    CACHE_TS_TOO_OLD_TOTAL,
    "Number of times a cache entry disregarded as it is too new for the requested timestamp"
);
pub fn log_validate_ts_too_old() {
    log_counter(&CACHE_TS_TOO_OLD_TOTAL, 1);
}

register_convex_counter!(
    CACHE_DROP_CACHE_RESULT_TOO_OLD_TOTAL,
    "Number of times a cache result is dropped as it is older than the existing entry"
);
pub fn log_drop_cache_result_too_old() {
    log_counter(&CACHE_DROP_CACHE_RESULT_TOO_OLD_TOTAL, 1);
}

register_convex_counter!(
    CACHE_VALIDATE_REFRESH_FAILED_TOTAL,
    "Number of times a cache entry couldn't be refreshed during validation"
);
pub fn log_validate_refresh_failed() {
    log_counter(&CACHE_VALIDATE_REFRESH_FAILED_TOTAL, 1);
}

register_convex_counter!(
    CACHE_VALIDATE_SYSTEM_TIME_TOO_OLD_TOTAL,
    "Number of times a cache entry's system time was too old"
);
pub fn log_validate_system_time_too_old() {
    log_counter(&CACHE_VALIDATE_SYSTEM_TIME_TOO_OLD_TOTAL, 1);
}
register_convex_counter!(
    CACHE_VALIDATE_SYSTEM_TIME_IN_THE_FUTURE_TOTAL,
    "Number of times a cache entry's system time was in the future"
);
pub fn log_validate_system_time_in_the_future() {
    log_counter(&CACHE_VALIDATE_SYSTEM_TIME_IN_THE_FUTURE_TOTAL, 1);
}

// n.b. this gauge is safe in a multi-instance context because it is shared
// across all instances.
register_convex_gauge!(CACHE_SIZE_BYTES, "Size of the cache in bytes");
pub fn log_cache_size(size: usize) {
    log_gauge(&CACHE_SIZE_BYTES, size as f64)
}

register_convex_counter!(
    QUERY_BANDWIDTH_BYTES,
    "Database bandwidth used for queries",
    &["is_paginated"]
);
pub fn log_query_bandwidth_bytes(is_paginated: bool, bytes: u64) {
    log_counter_with_labels(
        &QUERY_BANDWIDTH_BYTES,
        bytes,
        vec![StaticMetricLabel::new(
            "is_paginated",
            if is_paginated {
                "paginated"
            } else {
                "unpaginated"
            },
        )],
    );
}

register_convex_counter!(
    QUERY_CACHE_EVICTED_TOTAL,
    "The total number of records evicted",
);
// n.b. this gauge is safe in a multi-instance context because it is shared
// across all instances.
register_convex_gauge!(
    QUERY_CACHE_EVICTED_AGE_SECONDS,
    "The age of the last evicted entry",
);
pub fn query_cache_log_eviction(age: Duration) {
    log_counter(&QUERY_CACHE_EVICTED_TOTAL, 1);
    log_gauge(&QUERY_CACHE_EVICTED_AGE_SECONDS, age.as_secs_f64())
}

register_convex_counter!(
    QUERY_CACHE_VISIBILITY_REJECTED_TOTAL,
    "Number of cache hits refused because the caller may not run the function",
    &[IDENTITY_LABEL],
);
pub fn log_cache_hit_visibility_rejected(identity: StaticMetricLabel) {
    log_counter_with_labels(&QUERY_CACHE_VISIBILITY_REJECTED_TOTAL, 1, vec![identity]);
}
