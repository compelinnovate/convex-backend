use std::{
    collections::BTreeMap,
    fs,
    path::Path,
    time::{
        Duration,
        Instant,
    },
};

use anyhow::Context;
use common::{
    http::ExternalRequestShedding,
    knobs::{
        FUNRUN_CODE_CACHE_SIZE,
        FUNRUN_INDEX_CACHE_SIZE,
        FUNRUN_MODULE_CACHE_SIZE,
        INDEX_CACHE_SIZE,
        ISOLATE_MAX_ARRAY_BUFFER_TOTAL_SIZE,
        ISOLATE_MAX_HEAP_EXTRA_SIZE,
        ISOLATE_MAX_USER_HEAP_SIZE,
        LOCAL_BACKEND_MALLOC_TRIM_COOLDOWN,
        LOCAL_BACKEND_MALLOC_TRIM_ENABLED,
        LOCAL_BACKEND_MALLOC_TRIM_MIN_FREE_BYTES,
        LOCAL_BACKEND_MEMORY_PRESSURE_ENTER_HEADROOM_BYTES,
        LOCAL_BACKEND_MEMORY_PRESSURE_EXIT_HEADROOM_BYTES,
        LOCAL_BACKEND_MEMORY_PRESSURE_SHEDDING_ENABLED,
        LOCAL_BACKEND_MEMORY_RECLAMATION_ENABLED,
        LOCAL_BACKEND_MEMORY_RECLAMATION_ENTER_HEADROOM_BYTES,
        LOCAL_BACKEND_MEMORY_RECLAMATION_EXIT_HEADROOM_BYTES,
        LOCAL_BACKEND_NATIVE_KERNEL_MEMORY_RESERVE_BYTES,
        LOCAL_NODE_EXECUTOR_MAX_RSS_BYTES,
        MAX_ISOLATE_WORKERS,
        SOURCE_MAP_CACHE_MAX_SIZE_BYTES,
        UDF_CACHE_MAX_SIZE,
    },
    memory_pressure::MemoryPressureSignal,
    runtime::{
        tokio_spawn_blocking,
        Runtime,
    },
    shutdown::ShutdownSignal,
};
use metrics::{
    log_counter,
    log_counter_with_labels,
    log_distribution,
    log_gauge,
    log_gauge_with_labels,
    register_convex_counter,
    register_convex_gauge,
    register_convex_histogram,
    StaticMetricLabel,
};

const REPORT_INTERVAL: Duration = Duration::from_secs(15);
const PRESSURE_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const ALLOCATOR_ARENA_REPORT_INTERVAL: Duration = Duration::from_secs(5 * 60);
const MEMORY_REPORTS_PER_ALLOCATOR_ARENA_REPORT: usize =
    (ALLOCATOR_ARENA_REPORT_INTERVAL.as_secs() / REPORT_INTERVAL.as_secs()) as usize;
const MAX_MALLOC_INFO_BYTES: usize = 4 * 1024 * 1024;
const CGROUP_ROOT: &str = "/sys/fs/cgroup";

register_convex_gauge!(
    BACKEND_PROCESS_MEMORY_BYTES,
    "Memory attributed directly to the backend process",
    &["component"]
);
register_convex_gauge!(
    BACKEND_ALLOCATOR_MEMORY_BYTES,
    "Memory reported by the backend process allocator",
    &["component"]
);
register_convex_gauge!(
    BACKEND_ALLOCATOR_MMAP_REGIONS_INFO,
    "Number of mmap-backed regions reported by the backend process allocator"
);
register_convex_gauge!(
    BACKEND_ALLOCATOR_ARENAS_INFO,
    "Number of glibc malloc arenas reported by malloc_info"
);
register_convex_gauge!(
    BACKEND_ALLOCATOR_ARENA_TELEMETRY_INFO,
    "Whether glibc malloc arena-count telemetry is available"
);
register_convex_gauge!(
    BACKEND_ALLOCATOR_TELEMETRY_INFO,
    "Whether allocator-specific backend memory telemetry is available"
);
register_convex_gauge!(
    BACKEND_CGROUP_MEMORY_CONTROLLER_INFO,
    "Whether the backend can read its cgroup v2 memory controller"
);
register_convex_gauge!(
    BACKEND_CGROUP_MEMORY_BYTES,
    "Memory reported by the backend cgroup v2 memory controller",
    &["component"]
);
register_convex_gauge!(
    BACKEND_CGROUP_MEMORY_COMPONENT_AVAILABLE_INFO,
    "Whether a selected cgroup v2 memory.stat component is available",
    &["component"]
);
register_convex_gauge!(
    BACKEND_CGROUP_MEMORY_LIMITED_INFO,
    "Whether the backend cgroup v2 has a finite memory limit"
);
register_convex_gauge!(
    BACKEND_CGROUP_MEMORY_EVENTS_INFO,
    "Absolute cgroup v2 memory event counts for the backend container",
    &["event"]
);
register_convex_gauge!(
    BACKEND_CGROUP_MEMORY_EVENT_AVAILABLE_INFO,
    "Whether a selected cgroup v2 memory.events field is available",
    &["event"]
);
register_convex_gauge!(
    BACKEND_MEMORY_TELEMETRY_UP_INFO,
    "Whether the latest backend memory telemetry sample completed without source errors"
);
register_convex_gauge!(
    BACKEND_MEMORY_TELEMETRY_SOURCE_UP_INFO,
    "Whether a backend memory telemetry source succeeded in the latest sample",
    &["source"]
);
register_convex_counter!(
    BACKEND_MEMORY_TELEMETRY_FAILURES_TOTAL,
    "Number of failed backend memory telemetry samples"
);
register_convex_gauge!(
    BACKEND_STARTUP_MEMORY_BUDGET_BYTES,
    "Configured startup memory budget; this is configuration feasibility, not current allocation",
    &["component"]
);
register_convex_gauge!(
    BACKEND_STARTUP_MEMORY_BUDGET_LIMIT_AVAILABLE_INFO,
    "Whether startup memory feasibility was checked against a finite cgroup v2 limit"
);
register_convex_gauge!(
    BACKEND_STARTUP_MEMORY_BUDGET_HEADROOM_BYTES,
    "Cgroup memory limit remaining after the configured startup memory budget"
);
register_convex_gauge!(
    BACKEND_MEMORY_PRESSURE_SHEDDING_ENABLED_INFO,
    "Whether cgroup memory pressure can shed new external HTTP work"
);
register_convex_gauge!(
    BACKEND_MEMORY_PRESSURE_SHEDDING_ACTIVE_INFO,
    "Whether new external HTTP work is currently shed because cgroup memory headroom is low"
);
register_convex_gauge!(
    BACKEND_MEMORY_PRESSURE_HEADROOM_BYTES,
    "Finite cgroup memory headroom used by the external-admission pressure controller"
);
register_convex_gauge!(
    BACKEND_MEMORY_PRESSURE_HEADROOM_THRESHOLD_BYTES,
    "Configured cgroup memory headroom boundaries for external-admission shedding",
    &["boundary"]
);
register_convex_counter!(
    BACKEND_MEMORY_PRESSURE_TRANSITIONS_TOTAL,
    "Transitions of cgroup memory external-admission shedding",
    &["state"]
);
register_convex_counter!(
    BACKEND_MEMORY_PRESSURE_FAILURES_TOTAL,
    "Fatal cgroup source or configuration failures in the enabled memory pressure controller"
);
register_convex_gauge!(
    BACKEND_MEMORY_RECLAMATION_ENABLED_INFO,
    "Whether cgroup memory pressure can reclaim optional backend memory"
);
register_convex_gauge!(
    BACKEND_MEMORY_RECLAMATION_ACTIVE_INFO,
    "Whether optional backend memory reclamation is currently active"
);
register_convex_gauge!(
    BACKEND_MEMORY_RECLAMATION_HEADROOM_THRESHOLD_BYTES,
    "Configured cgroup memory headroom boundaries for internal reclamation",
    &["boundary"]
);
register_convex_counter!(
    BACKEND_MEMORY_RECLAMATION_TRANSITIONS_TOTAL,
    "Transitions of cgroup memory internal reclamation",
    &["state"]
);
register_convex_gauge!(
    BACKEND_ALLOCATOR_TRIM_ENABLED_INFO,
    "Whether glibc malloc trim is enabled during cgroup memory reclamation"
);
register_convex_gauge!(
    BACKEND_ALLOCATOR_TRIM_ACTIVE_INFO,
    "Whether a glibc malloc trim call is currently running"
);
register_convex_gauge!(
    BACKEND_ALLOCATOR_TRIM_CONFIGURATION_INFO,
    "Configured glibc malloc trim controls",
    &["component"]
);
register_convex_counter!(
    BACKEND_ALLOCATOR_TRIM_ATTEMPTS_TOTAL,
    "Explicit backend allocator trim attempts by bounded measurement outcome",
    &["outcome"]
);
register_convex_histogram!(
    BACKEND_ALLOCATOR_TRIM_SECONDS,
    "Duration of explicit backend allocator trim calls"
);
register_convex_gauge!(
    BACKEND_ALLOCATOR_TRIM_MEMORY_CHANGE_BYTES,
    "Immediate signed memory change after explicit allocator trim",
    &["component"]
);
register_convex_counter!(
    BACKEND_ALLOCATOR_TRIM_PAGE_FAULTS_TOTAL,
    "Process page faults observed during an explicit allocator trim sample",
    &["kind"]
);

#[derive(Debug, Eq, PartialEq)]
struct ProcessMemory {
    virtual_bytes: u64,
    resident_bytes: u64,
    resident_anon_bytes: u64,
    resident_file_bytes: u64,
    resident_shmem_bytes: u64,
    swap_bytes: u64,
}

#[derive(Debug, Eq, PartialEq)]
struct AllocatorMemory {
    arena_bytes: u64,
    mmap_bytes: u64,
    in_use_bytes: u64,
    free_bytes: u64,
    releasable_bytes: u64,
    mmap_regions: u64,
}

#[derive(Debug, Eq, PartialEq)]
struct CgroupMemory {
    current_bytes: u64,
    max_bytes: Option<u64>,
}

pub struct CgroupMemoryPressureController {
    external_request_shedding: Option<ExternalRequestShedding>,
    shedding_enter_headroom_bytes: u64,
    shedding_exit_headroom_bytes: u64,
    memory_reclamation: MemoryPressureSignal,
    reclamation_active: bool,
    reclamation_enabled: bool,
    reclamation_enter_headroom_bytes: u64,
    reclamation_exit_headroom_bytes: u64,
    allocator_trim_enabled: bool,
    allocator_trim_min_free_bytes: u64,
    allocator_trim_cooldown: Duration,
    last_allocator_trim_evaluated: Option<Instant>,
}

#[derive(Debug, Eq, PartialEq)]
struct PageFaults {
    minor: u64,
    major: u64,
}

#[derive(Debug, Eq, PartialEq)]
struct AllocatorTrimSnapshot {
    process: ProcessMemory,
    allocator: Option<AllocatorMemory>,
    cgroup_current_bytes: u64,
    cgroup_anon_bytes: u64,
    page_faults: PageFaults,
}

enum AllocatorTrimRun {
    Unsupported,
    BelowFreeThreshold,
    Completed {
        before: AllocatorTrimSnapshot,
        after: anyhow::Result<AllocatorTrimSnapshot>,
        returned: bool,
        elapsed: Duration,
    },
}

#[derive(Debug, Eq, PartialEq)]
struct MemoryBudgetComponent {
    name: &'static str,
    bytes: u64,
}

#[derive(Debug, Eq, PartialEq)]
struct StartupMemoryBudget {
    components: Vec<MemoryBudgetComponent>,
    total_bytes: u64,
}

impl StartupMemoryBudget {
    fn new(components: Vec<MemoryBudgetComponent>) -> anyhow::Result<Self> {
        let total_bytes = components.iter().try_fold(0u64, |total, component| {
            total
                .checked_add(component.bytes)
                .context("configured startup memory budget overflow")
        })?;
        Ok(Self {
            components,
            total_bytes,
        })
    }
}

#[derive(Debug)]
struct SourceFailure {
    source: &'static str,
    error: anyhow::Error,
}

pub fn initialize_memory_pressure_controller(
) -> anyhow::Result<Option<CgroupMemoryPressureController>> {
    let shedding_enabled = *LOCAL_BACKEND_MEMORY_PRESSURE_SHEDDING_ENABLED;
    let reclamation_enabled = *LOCAL_BACKEND_MEMORY_RECLAMATION_ENABLED;
    let allocator_trim_enabled = *LOCAL_BACKEND_MALLOC_TRIM_ENABLED;
    log_gauge(
        &BACKEND_MEMORY_PRESSURE_SHEDDING_ENABLED_INFO,
        if shedding_enabled { 1.0 } else { 0.0 },
    );
    log_gauge(&BACKEND_MEMORY_PRESSURE_SHEDDING_ACTIVE_INFO, 0.0);
    log_gauge(
        &BACKEND_MEMORY_RECLAMATION_ENABLED_INFO,
        if reclamation_enabled { 1.0 } else { 0.0 },
    );
    log_gauge(&BACKEND_MEMORY_RECLAMATION_ACTIVE_INFO, 0.0);
    log_gauge(
        &BACKEND_ALLOCATOR_TRIM_ENABLED_INFO,
        if allocator_trim_enabled { 1.0 } else { 0.0 },
    );
    log_gauge(&BACKEND_ALLOCATOR_TRIM_ACTIVE_INFO, 0.0);
    log_counter(&BACKEND_MEMORY_PRESSURE_FAILURES_TOTAL, 0);
    for state in ["active", "inactive"] {
        log_counter_with_labels(
            &BACKEND_MEMORY_PRESSURE_TRANSITIONS_TOTAL,
            0,
            vec![StaticMetricLabel::new("state", state)],
        );
        log_counter_with_labels(
            &BACKEND_MEMORY_RECLAMATION_TRANSITIONS_TOTAL,
            0,
            vec![StaticMetricLabel::new("state", state)],
        );
    }
    for outcome in [
        "returned_true",
        "returned_false",
        "unsupported",
        "sample_failure",
    ] {
        log_counter_with_labels(
            &BACKEND_ALLOCATOR_TRIM_ATTEMPTS_TOTAL,
            0,
            vec![StaticMetricLabel::new("outcome", outcome)],
        );
    }
    anyhow::ensure!(
        !allocator_trim_enabled || reclamation_enabled,
        "Allocator trim requires internal memory reclamation"
    );
    let allocator_trim_min_free_bytes = u64::try_from(*LOCAL_BACKEND_MALLOC_TRIM_MIN_FREE_BYTES)?;
    let allocator_trim_cooldown = *LOCAL_BACKEND_MALLOC_TRIM_COOLDOWN;
    anyhow::ensure!(
        allocator_trim_min_free_bytes > 0,
        "Allocator trim minimum free bytes must be greater than zero"
    );
    anyhow::ensure!(
        allocator_trim_cooldown > Duration::ZERO,
        "Allocator trim cooldown must be greater than zero"
    );
    for (component, value) in [
        ("min_free_bytes", allocator_trim_min_free_bytes as f64),
        ("cooldown_seconds", allocator_trim_cooldown.as_secs_f64()),
    ] {
        log_gauge_with_labels(
            &BACKEND_ALLOCATOR_TRIM_CONFIGURATION_INFO,
            value,
            vec![StaticMetricLabel::new("component", component)],
        );
    }

    if !shedding_enabled && !reclamation_enabled {
        return Ok(None);
    }

    let shedding_enter_headroom_bytes =
        u64::try_from(*LOCAL_BACKEND_MEMORY_PRESSURE_ENTER_HEADROOM_BYTES)?;
    let shedding_exit_headroom_bytes =
        u64::try_from(*LOCAL_BACKEND_MEMORY_PRESSURE_EXIT_HEADROOM_BYTES)?;
    let reclamation_enter_headroom_bytes =
        u64::try_from(*LOCAL_BACKEND_MEMORY_RECLAMATION_ENTER_HEADROOM_BYTES)?;
    let reclamation_exit_headroom_bytes =
        u64::try_from(*LOCAL_BACKEND_MEMORY_RECLAMATION_EXIT_HEADROOM_BYTES)?;

    if shedding_enabled {
        anyhow::ensure!(
            shedding_enter_headroom_bytes < shedding_exit_headroom_bytes,
            "Memory pressure shedding exit headroom must exceed enter headroom"
        );
        for (boundary, bytes) in [
            ("enter", shedding_enter_headroom_bytes),
            ("exit", shedding_exit_headroom_bytes),
        ] {
            log_gauge_with_labels(
                &BACKEND_MEMORY_PRESSURE_HEADROOM_THRESHOLD_BYTES,
                bytes as f64,
                vec![StaticMetricLabel::new("boundary", boundary)],
            );
        }
    }
    if reclamation_enabled {
        anyhow::ensure!(
            reclamation_enter_headroom_bytes < reclamation_exit_headroom_bytes,
            "Memory reclamation exit headroom must exceed enter headroom"
        );
        for (boundary, bytes) in [
            ("enter", reclamation_enter_headroom_bytes),
            ("exit", reclamation_exit_headroom_bytes),
        ] {
            log_gauge_with_labels(
                &BACKEND_MEMORY_RECLAMATION_HEADROOM_THRESHOLD_BYTES,
                bytes as f64,
                vec![StaticMetricLabel::new("boundary", boundary)],
            );
        }
    }
    if shedding_enabled && reclamation_enabled {
        anyhow::ensure!(
            reclamation_enter_headroom_bytes > shedding_enter_headroom_bytes
                && reclamation_exit_headroom_bytes > shedding_exit_headroom_bytes,
            "Internal memory reclamation must start and recover with more headroom than external \
             shedding"
        );
    }

    let cgroup = read_cgroup_memory(Path::new(CGROUP_ROOT))?
        .context("Memory pressure control requires the cgroup v2 memory controller")?;
    let max_bytes = cgroup
        .max_bytes
        .context("Memory pressure control requires a finite cgroup v2 memory limit")?;
    if shedding_enabled {
        anyhow::ensure!(
            shedding_exit_headroom_bytes < max_bytes,
            "Memory pressure shedding exit headroom must be smaller than the finite cgroup memory \
             limit"
        );
    }
    if reclamation_enabled {
        anyhow::ensure!(
            reclamation_exit_headroom_bytes < max_bytes,
            "Memory reclamation exit headroom must be smaller than the finite cgroup memory limit"
        );
    }
    let headroom_bytes = max_bytes.saturating_sub(cgroup.current_bytes);
    log_gauge(
        &BACKEND_MEMORY_PRESSURE_HEADROOM_BYTES,
        headroom_bytes as f64,
    );
    let shedding_initially_active = shedding_enabled
        && pressure_state(
            false,
            headroom_bytes,
            shedding_enter_headroom_bytes,
            shedding_exit_headroom_bytes,
        );
    let reclamation_initially_active = reclamation_enabled
        && pressure_state(
            false,
            headroom_bytes,
            reclamation_enter_headroom_bytes,
            reclamation_exit_headroom_bytes,
        );
    log_gauge(
        &BACKEND_MEMORY_PRESSURE_SHEDDING_ACTIVE_INFO,
        if shedding_initially_active { 1.0 } else { 0.0 },
    );
    log_gauge(
        &BACKEND_MEMORY_RECLAMATION_ACTIVE_INFO,
        if reclamation_initially_active {
            1.0
        } else {
            0.0
        },
    );

    Ok(Some(CgroupMemoryPressureController {
        external_request_shedding: shedding_enabled
            .then(|| ExternalRequestShedding::new(shedding_initially_active)),
        shedding_enter_headroom_bytes,
        shedding_exit_headroom_bytes,
        // Consumers enter only after the controller has attempted allocator
        // trim for the initial pressure state.
        memory_reclamation: MemoryPressureSignal::default(),
        reclamation_active: reclamation_initially_active,
        reclamation_enabled,
        reclamation_enter_headroom_bytes,
        reclamation_exit_headroom_bytes,
        allocator_trim_enabled,
        allocator_trim_min_free_bytes,
        allocator_trim_cooldown,
        last_allocator_trim_evaluated: None,
    }))
}

impl CgroupMemoryPressureController {
    pub fn external_request_shedding(&self) -> Option<ExternalRequestShedding> {
        self.external_request_shedding.clone()
    }

    pub fn memory_reclamation(&self) -> MemoryPressureSignal {
        self.memory_reclamation.clone()
    }

    fn update(&mut self, cgroup: &CgroupMemory) -> anyhow::Result<()> {
        let max_bytes = cgroup
            .max_bytes
            .context("Memory pressure control requires a finite cgroup v2 memory limit")?;
        let headroom_bytes = max_bytes.saturating_sub(cgroup.current_bytes);
        log_gauge(
            &BACKEND_MEMORY_PRESSURE_HEADROOM_BYTES,
            headroom_bytes as f64,
        );
        if self.reclamation_enabled {
            anyhow::ensure!(
                self.reclamation_exit_headroom_bytes < max_bytes,
                "Memory reclamation exit headroom must be smaller than the finite cgroup memory \
                 limit"
            );
            let was_active = self.reclamation_active;
            let is_active = pressure_state(
                was_active,
                headroom_bytes,
                self.reclamation_enter_headroom_bytes,
                self.reclamation_exit_headroom_bytes,
            );
            if is_active != was_active {
                self.reclamation_active = is_active;
                log_gauge(
                    &BACKEND_MEMORY_RECLAMATION_ACTIVE_INFO,
                    if is_active { 1.0 } else { 0.0 },
                );
                let state = if is_active { "active" } else { "inactive" };
                log_counter_with_labels(
                    &BACKEND_MEMORY_RECLAMATION_TRANSITIONS_TOTAL,
                    1,
                    vec![StaticMetricLabel::new("state", state)],
                );
                if is_active {
                    tracing::warn!(
                        cgroup_memory_current_bytes = cgroup.current_bytes,
                        cgroup_memory_max_bytes = max_bytes,
                        cgroup_memory_headroom_bytes = headroom_bytes,
                        "Started reclaiming optional backend memory because cgroup headroom is low"
                    );
                } else {
                    tracing::info!(
                        cgroup_memory_current_bytes = cgroup.current_bytes,
                        cgroup_memory_max_bytes = max_bytes,
                        cgroup_memory_headroom_bytes = headroom_bytes,
                        "Stopped internal memory reclamation after cgroup headroom recovered"
                    );
                }
            }
        }
        if let Some(external_request_shedding) = &self.external_request_shedding {
            anyhow::ensure!(
                self.shedding_exit_headroom_bytes < max_bytes,
                "Memory pressure shedding exit headroom must be smaller than the finite cgroup \
                 memory limit"
            );
            let was_active = external_request_shedding.is_active();
            let is_active = pressure_state(
                was_active,
                headroom_bytes,
                self.shedding_enter_headroom_bytes,
                self.shedding_exit_headroom_bytes,
            );
            if is_active != was_active {
                let prior = external_request_shedding.set_active(is_active);
                assert_eq!(
                    prior, was_active,
                    "memory pressure shedding has more than one state writer"
                );
                log_gauge(
                    &BACKEND_MEMORY_PRESSURE_SHEDDING_ACTIVE_INFO,
                    if is_active { 1.0 } else { 0.0 },
                );
                let state = if is_active { "active" } else { "inactive" };
                log_counter_with_labels(
                    &BACKEND_MEMORY_PRESSURE_TRANSITIONS_TOTAL,
                    1,
                    vec![StaticMetricLabel::new("state", state)],
                );
                if is_active {
                    tracing::warn!(
                        cgroup_memory_current_bytes = cgroup.current_bytes,
                        cgroup_memory_max_bytes = max_bytes,
                        cgroup_memory_headroom_bytes = headroom_bytes,
                        "Started shedding new external HTTP work because cgroup memory headroom \
                         is low"
                    );
                } else {
                    tracing::info!(
                        cgroup_memory_current_bytes = cgroup.current_bytes,
                        cgroup_memory_max_bytes = max_bytes,
                        cgroup_memory_headroom_bytes = headroom_bytes,
                        "Stopped shedding external HTTP work after cgroup memory headroom \
                         recovered"
                    );
                }
            }
        }
        Ok(())
    }

    fn claim_allocator_trim(&mut self) -> bool {
        if !self.allocator_trim_enabled || !self.reclamation_active {
            return false;
        }
        let now = Instant::now();
        if self
            .last_allocator_trim_evaluated
            .is_some_and(|last| now.duration_since(last) < self.allocator_trim_cooldown)
        {
            return false;
        }
        // The blocking trim task checks logical free space. Claim its cooldown
        // first so pressure cannot start an arena-lock scan every second when
        // the threshold is not met.
        self.last_allocator_trim_evaluated = Some(now);
        true
    }

    fn publish_reclamation_state(&self) {
        let was_active = self.memory_reclamation.is_active();
        if was_active == self.reclamation_active {
            return;
        }
        let prior = self.memory_reclamation.set_active(self.reclamation_active);
        assert_eq!(
            prior, was_active,
            "memory reclamation has more than one state writer"
        );
    }
}

fn update_memory_pressure_controller(
    controller: &mut CgroupMemoryPressureController,
) -> anyhow::Result<()> {
    let cgroup = read_cgroup_memory(Path::new(CGROUP_ROOT))?
        .context("Enabled memory pressure controller lost the cgroup v2 memory controller")?;
    controller.update(&cgroup)
}

fn pressure_state(
    active: bool,
    headroom_bytes: u64,
    enter_headroom_bytes: u64,
    exit_headroom_bytes: u64,
) -> bool {
    if active {
        headroom_bytes < exit_headroom_bytes
    } else {
        headroom_bytes <= enter_headroom_bytes
    }
}

async fn run_allocator_trim(root: &Path, min_free_bytes: u64) -> anyhow::Result<()> {
    let root = root.to_owned();
    let run = match tokio_spawn_blocking("backend_allocator_trim", move || {
        measure_allocator_trim(&root, min_free_bytes)
    })
    .await
    .context("allocator trim blocking task failed")
    .and_then(|result| result)
    {
        Ok(run) => run,
        Err(error) => {
            log_allocator_trim_attempt("sample_failure");
            return Err(error);
        },
    };
    let (before, after, returned, elapsed) = match run {
        AllocatorTrimRun::Unsupported => {
            log_allocator_trim_attempt("unsupported");
            return Ok(());
        },
        AllocatorTrimRun::BelowFreeThreshold => return Ok(()),
        AllocatorTrimRun::Completed {
            before,
            after,
            returned,
            elapsed,
        } => (before, after, returned, elapsed),
    };
    let outcome = if returned {
        "returned_true"
    } else {
        "returned_false"
    };
    log_distribution(&BACKEND_ALLOCATOR_TRIM_SECONDS, elapsed.as_secs_f64());
    let after = match after {
        Ok(after) => after,
        Err(error) => {
            log_allocator_trim_attempt("sample_failure");
            return Err(error);
        },
    };
    let minor_faults = match after
        .page_faults
        .minor
        .checked_sub(before.page_faults.minor)
    {
        Some(faults) => faults,
        None => {
            log_allocator_trim_attempt("sample_failure");
            anyhow::bail!("process minor page-fault counter decreased during allocator trim");
        },
    };
    let major_faults = match after
        .page_faults
        .major
        .checked_sub(before.page_faults.major)
    {
        Some(faults) => faults,
        None => {
            log_allocator_trim_attempt("sample_failure");
            anyhow::bail!("process major page-fault counter decreased during allocator trim");
        },
    };
    for (component, before_bytes, after_bytes) in [
        (
            "process_resident",
            before.process.resident_bytes,
            after.process.resident_bytes,
        ),
        (
            "process_resident_anon",
            before.process.resident_anon_bytes,
            after.process.resident_anon_bytes,
        ),
        (
            "cgroup_current",
            before.cgroup_current_bytes,
            after.cgroup_current_bytes,
        ),
        (
            "cgroup_anon",
            before.cgroup_anon_bytes,
            after.cgroup_anon_bytes,
        ),
    ] {
        log_trim_memory_change(component, before_bytes, after_bytes);
    }
    if let (Some(before_allocator), Some(after_allocator)) = (&before.allocator, &after.allocator) {
        log_trim_memory_change(
            "allocator_free",
            before_allocator.free_bytes,
            after_allocator.free_bytes,
        );
    }
    for (kind, faults) in [("minor", minor_faults), ("major", major_faults)] {
        log_counter_with_labels(
            &BACKEND_ALLOCATOR_TRIM_PAGE_FAULTS_TOTAL,
            faults,
            vec![StaticMetricLabel::new("kind", kind)],
        );
    }
    log_allocator_trim_attempt(outcome);
    tracing::info!(
        allocator_trim_outcome = outcome,
        allocator_trim_duration_seconds = elapsed.as_secs_f64(),
        process_resident_before_bytes = before.process.resident_bytes,
        process_resident_after_bytes = after.process.resident_bytes,
        cgroup_current_before_bytes = before.cgroup_current_bytes,
        cgroup_current_after_bytes = after.cgroup_current_bytes,
        "Completed explicit backend allocator trim"
    );
    Ok(())
}

fn log_allocator_trim_attempt(outcome: &'static str) {
    log_counter_with_labels(
        &BACKEND_ALLOCATOR_TRIM_ATTEMPTS_TOTAL,
        1,
        vec![StaticMetricLabel::new("outcome", outcome)],
    );
}

fn measure_allocator_trim(root: &Path, min_free_bytes: u64) -> anyhow::Result<AllocatorTrimRun> {
    let before = allocator_trim_snapshot(root)?;
    let Some(allocator) = &before.allocator else {
        return Ok(AllocatorTrimRun::Unsupported);
    };
    if allocator.free_bytes < min_free_bytes {
        return Ok(AllocatorTrimRun::BelowFreeThreshold);
    }

    log_gauge(&BACKEND_ALLOCATOR_TRIM_ACTIVE_INFO, 1.0);
    let started = Instant::now();
    let returned = explicit_allocator_trim();
    let elapsed = started.elapsed();
    log_gauge(&BACKEND_ALLOCATOR_TRIM_ACTIVE_INFO, 0.0);
    let returned = returned.context("allocator telemetry was available but trim is unsupported")?;
    let after = allocator_trim_snapshot(root);
    Ok(AllocatorTrimRun::Completed {
        before,
        after,
        returned,
        elapsed,
    })
}

fn log_trim_memory_change(component: &'static str, before: u64, after: u64) {
    let change = signed_memory_change(before, after);
    log_gauge_with_labels(
        &BACKEND_ALLOCATOR_TRIM_MEMORY_CHANGE_BYTES,
        change as f64,
        vec![StaticMetricLabel::new("component", component)],
    );
}

fn signed_memory_change(before: u64, after: u64) -> i128 {
    i128::from(after) - i128::from(before)
}

fn allocator_trim_snapshot(root: &Path) -> anyhow::Result<AllocatorTrimSnapshot> {
    let process = parse_process_status(&fs::read_to_string("/proc/self/status")?)?;
    let allocator = allocator_memory()?;
    let cgroup = read_cgroup_memory(root)?
        .context("allocator trim requires the cgroup v2 memory controller")?;
    let stat = parse_keyed_u64(&fs::read_to_string(root.join("memory.stat"))?)?;
    let cgroup_anon_bytes = *stat
        .get("anon")
        .context("allocator trim requires cgroup anonymous-memory accounting")?;
    Ok(AllocatorTrimSnapshot {
        process,
        allocator,
        cgroup_current_bytes: cgroup.current_bytes,
        cgroup_anon_bytes,
        page_faults: read_process_page_faults()?,
    })
}

fn read_process_page_faults() -> anyhow::Result<PageFaults> {
    // SAFETY: `getrusage` initializes the provided process-local `rusage`
    // structure and does not retain the pointer.
    let mut usage = unsafe { std::mem::zeroed::<libc::rusage>() };
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    anyhow::ensure!(result == 0, "getrusage failed for allocator trim telemetry");
    Ok(PageFaults {
        minor: u64::try_from(usage.ru_minflt)?,
        major: u64::try_from(usage.ru_majflt)?,
    })
}

#[cfg(target_env = "gnu")]
fn explicit_allocator_trim() -> Option<bool> {
    // SAFETY: glibc documents `malloc_trim` as MT-Safe. A zero pad asks the
    // allocator to retain no extra main-arena top space.
    Some(unsafe { libc::malloc_trim(0) } != 0)
}

#[cfg(not(target_env = "gnu"))]
fn explicit_allocator_trim() -> Option<bool> {
    None
}

fn report_allocator_arena_count() -> anyhow::Result<()> {
    match allocator_arena_count()? {
        Some(count) => {
            log_gauge(&BACKEND_ALLOCATOR_ARENA_TELEMETRY_INFO, 1.0);
            log_gauge(&BACKEND_ALLOCATOR_ARENAS_INFO, count as f64);
        },
        None => log_gauge(&BACKEND_ALLOCATOR_ARENA_TELEMETRY_INFO, 0.0),
    }
    Ok(())
}

#[cfg(target_env = "gnu")]
fn allocator_arena_count() -> anyhow::Result<Option<usize>> {
    let mut contents = vec![0u8; MAX_MALLOC_INFO_BYTES];
    // SAFETY: `contents` is writable for its full length and remains alive until
    // after the returned stream has been closed. The mode is NUL terminated.
    let stream =
        unsafe { libc::fmemopen(contents.as_mut_ptr().cast(), contents.len(), c"w".as_ptr()) };
    anyhow::ensure!(!stream.is_null(), "fmemopen failed for malloc_info");
    // SAFETY: the stream is a valid writable FILE owned by this function. The
    // fixed backing buffer bounds output even if allocator state is malformed.
    let info_result = unsafe { libc::malloc_info(0, stream) };
    let flush_result = unsafe { libc::fflush(stream) };
    let stream_error = unsafe { libc::ferror(stream) };
    let length = unsafe { libc::ftell(stream) };
    let close_result = unsafe { libc::fclose(stream) };
    anyhow::ensure!(info_result == 0, "malloc_info failed");
    anyhow::ensure!(flush_result == 0, "flushing malloc_info stream failed");
    anyhow::ensure!(
        stream_error == 0,
        "malloc_info output exceeded its size limit"
    );
    anyhow::ensure!(length >= 0, "malloc_info stream position is unavailable");
    anyhow::ensure!(close_result == 0, "closing malloc_info stream failed");
    let length = usize::try_from(length)?;
    anyhow::ensure!(
        length < contents.len(),
        "malloc_info output exceeded its size limit"
    );
    let xml =
        std::str::from_utf8(&contents[..length]).context("malloc_info returned invalid UTF-8")?;
    let count = xml.match_indices("<heap nr=").count();
    anyhow::ensure!(count > 0, "malloc_info returned no allocator arenas");
    Ok(Some(count))
}

#[cfg(not(target_env = "gnu"))]
fn allocator_arena_count() -> anyhow::Result<Option<usize>> {
    Ok(None)
}

pub fn validate_startup_budget() -> anyhow::Result<()> {
    let budget = configured_startup_budget()?;
    for component in &budget.components {
        log_gauge_with_labels(
            &BACKEND_STARTUP_MEMORY_BUDGET_BYTES,
            component.bytes as f64,
            vec![StaticMetricLabel::new("component", component.name)],
        );
    }
    log_gauge_with_labels(
        &BACKEND_STARTUP_MEMORY_BUDGET_BYTES,
        budget.total_bytes as f64,
        vec![StaticMetricLabel::new("component", "configured_total")],
    );

    match startup_budget_headroom(Path::new(CGROUP_ROOT), &budget)? {
        Some(headroom_bytes) => {
            log_gauge(&BACKEND_STARTUP_MEMORY_BUDGET_LIMIT_AVAILABLE_INFO, 1.0);
            log_gauge(
                &BACKEND_STARTUP_MEMORY_BUDGET_HEADROOM_BYTES,
                headroom_bytes as f64,
            );
            tracing::info!(
                configured_memory_budget_bytes = budget.total_bytes,
                memory_budget_headroom_bytes = headroom_bytes,
                "Validated configured startup memory budget against the cgroup limit"
            );
        },
        None => {
            log_gauge(&BACKEND_STARTUP_MEMORY_BUDGET_LIMIT_AVAILABLE_INFO, 0.0);
            tracing::info!(
                configured_memory_budget_bytes = budget.total_bytes,
                "Skipped startup memory feasibility because no finite cgroup v2 limit is available"
            );
        },
    }
    Ok(())
}

fn configured_startup_budget() -> anyhow::Result<StartupMemoryBudget> {
    let isolate_workers =
        u64::try_from(*MAX_ISOLATE_WORKERS).context("MAX_ISOLATE_WORKERS does not fit in u64")?;
    let isolate_heap_per_worker = ISOLATE_MAX_USER_HEAP_SIZE
        .checked_add(*ISOLATE_MAX_HEAP_EXTRA_SIZE)
        .context("configured isolate heap size overflow")?;
    let isolate_heap_pool = u64::try_from(isolate_heap_per_worker)?
        .checked_mul(isolate_workers)
        .context("configured isolate heap pool overflow")?;
    let isolate_array_buffer_pool = u64::try_from(*ISOLATE_MAX_ARRAY_BUFFER_TOTAL_SIZE)?
        .checked_mul(isolate_workers)
        .context("configured isolate ArrayBuffer pool overflow")?;
    StartupMemoryBudget::new(vec![
        MemoryBudgetComponent {
            name: "isolate_heap_pool",
            bytes: isolate_heap_pool,
        },
        MemoryBudgetComponent {
            name: "isolate_array_buffer_pool",
            bytes: isolate_array_buffer_pool,
        },
        MemoryBudgetComponent {
            name: "query_cache",
            bytes: u64::try_from(*UDF_CACHE_MAX_SIZE)?,
        },
        MemoryBudgetComponent {
            name: "shared_index_cache",
            bytes: *INDEX_CACHE_SIZE,
        },
        MemoryBudgetComponent {
            name: "source_map_cache",
            bytes: *SOURCE_MAP_CACHE_MAX_SIZE_BYTES,
        },
        MemoryBudgetComponent {
            name: "function_runner_index_cache",
            bytes: *FUNRUN_INDEX_CACHE_SIZE,
        },
        MemoryBudgetComponent {
            name: "function_runner_module_cache",
            bytes: *FUNRUN_MODULE_CACHE_SIZE,
        },
        MemoryBudgetComponent {
            name: "function_runner_code_cache",
            bytes: *FUNRUN_CODE_CACHE_SIZE,
        },
        MemoryBudgetComponent {
            // This sampled Linux direct-child retirement trigger is a planning allowance, not a
            // hard RSS maximum. Sampling delay, active-request drain, and descendants can exceed
            // it.
            name: "local_node_rss_threshold",
            bytes: u64::try_from(*LOCAL_NODE_EXECUTOR_MAX_RSS_BYTES)?,
        },
        MemoryBudgetComponent {
            name: "native_kernel_reserve",
            bytes: u64::try_from(*LOCAL_BACKEND_NATIVE_KERNEL_MEMORY_RESERVE_BYTES)?,
        },
    ])
}

fn startup_budget_headroom(
    root: &Path,
    budget: &StartupMemoryBudget,
) -> anyhow::Result<Option<u64>> {
    let Some(cgroup) = read_cgroup_memory(root)? else {
        return Ok(None);
    };
    let Some(limit_bytes) = cgroup.max_bytes else {
        return Ok(None);
    };
    anyhow::ensure!(
        budget.total_bytes <= limit_bytes,
        "Configured startup memory budget exceeds the finite cgroup memory limit: budget={} \
         bytes, limit={} bytes",
        budget.total_bytes,
        limit_bytes
    );
    Ok(Some(limit_bytes - budget.total_bytes))
}

pub fn start<RT: Runtime>(
    runtime: RT,
    pressure_controller: Option<CgroupMemoryPressureController>,
    shutdown: ShutdownSignal,
) {
    log_counter(&BACKEND_MEMORY_TELEMETRY_FAILURES_TOTAL, 0);
    if let Some(mut controller) = pressure_controller {
        let pressure_runtime = runtime.clone();
        runtime
            .clone()
            .spawn_background("backend_memory_pressure_controller", async move {
                loop {
                    if let Err(error) = update_memory_pressure_controller(&mut controller) {
                        log_counter(&BACKEND_MEMORY_PRESSURE_FAILURES_TOTAL, 1);
                        shutdown.signal(error);
                        return;
                    }
                    if controller.claim_allocator_trim() {
                        if let Err(error) = run_allocator_trim(
                            Path::new(CGROUP_ROOT),
                            controller.allocator_trim_min_free_bytes,
                        )
                        .await
                        {
                            // Trimming is an optional recovery action. Keep the
                            // remaining reclamation controls available when the
                            // allocator or its diagnostic snapshot fails.
                            log_counter(&BACKEND_MEMORY_TELEMETRY_FAILURES_TOTAL, 1);
                            tracing::error!(
                                "Backend allocator trim or its telemetry failed: {error:#}"
                            );
                        }
                        // Trim can either recover the reclamation threshold or
                        // take long enough for headroom to cross the shedding
                        // threshold. Publish only after a fresh cgroup sample.
                        if let Err(error) = update_memory_pressure_controller(&mut controller) {
                            log_counter(&BACKEND_MEMORY_PRESSURE_FAILURES_TOTAL, 1);
                            shutdown.signal(error);
                            return;
                        }
                    }
                    // Allocator trim is the first pressure action. Context and
                    // Node consumers observe entry only after it completes or
                    // is skipped; exit is published in the same sample.
                    controller.publish_reclamation_state();
                    pressure_runtime.wait(PRESSURE_SAMPLE_INTERVAL).await;
                }
            });
    }

    runtime
        .clone()
        .spawn_background("backend_memory_metrics", async move {
            let mut memory_reports_since_allocator_arena_report = 0;
            loop {
                let report = tokio_spawn_blocking("backend_memory_metrics_sample", || {
                    let root = Path::new(CGROUP_ROOT);
                    report_with_cgroup(root, read_cgroup_memory(root))
                })
                .await;
                let failures = match report {
                    Ok(failures) => failures,
                    Err(error) => vec![SourceFailure {
                        source: "blocking_task",
                        error: error.into(),
                    }],
                };
                log_gauge(
                    &BACKEND_MEMORY_TELEMETRY_UP_INFO,
                    if failures.is_empty() { 1.0 } else { 0.0 },
                );
                if !failures.is_empty() {
                    // Memory telemetry is an observability boundary when it is
                    // not also the configured pressure-control source. Publish
                    // coverage loss and retry.
                    log_counter(&BACKEND_MEMORY_TELEMETRY_FAILURES_TOTAL, 1);
                }
                for failure in failures {
                    tracing::error!(
                        source = failure.source,
                        "Backend memory telemetry source failed: {:#}",
                        failure.error
                    );
                }

                if memory_reports_since_allocator_arena_report == 0 {
                    let arena_report =
                        tokio_spawn_blocking("backend_allocator_arena_metrics", || {
                            report_allocator_arena_count()
                        })
                        .await;
                    match arena_report {
                        Ok(Ok(())) => {},
                        Ok(Err(error)) => {
                            log_gauge(&BACKEND_ALLOCATOR_ARENA_TELEMETRY_INFO, 0.0);
                            log_counter(&BACKEND_MEMORY_TELEMETRY_FAILURES_TOTAL, 1);
                            tracing::error!(
                                "Backend allocator arena telemetry source failed: {error:#}"
                            );
                        },
                        Err(error) => {
                            log_gauge(&BACKEND_ALLOCATOR_ARENA_TELEMETRY_INFO, 0.0);
                            log_counter(&BACKEND_MEMORY_TELEMETRY_FAILURES_TOTAL, 1);
                            tracing::error!(
                                "Backend allocator arena telemetry task failed: {error:#}"
                            );
                        },
                    }
                }
                memory_reports_since_allocator_arena_report =
                    (memory_reports_since_allocator_arena_report + 1)
                        % MEMORY_REPORTS_PER_ALLOCATOR_ARENA_REPORT;
                runtime.wait(REPORT_INTERVAL).await;
            }
        });
}

#[cfg(test)]
fn report() -> Vec<SourceFailure> {
    let root = Path::new(CGROUP_ROOT);
    report_with_cgroup(root, read_cgroup_memory(root))
}

fn report_with_cgroup(
    root: &Path,
    cgroup: anyhow::Result<Option<CgroupMemory>>,
) -> Vec<SourceFailure> {
    let mut failures = Vec::new();
    sample_source("process", report_process_memory, &mut failures);
    sample_source("allocator", report_allocator_memory, &mut failures);
    report_cgroup_memory(root, cgroup, &mut failures);
    failures
}

fn sample_source(
    source: &'static str,
    sample: impl FnOnce() -> anyhow::Result<()>,
    failures: &mut Vec<SourceFailure>,
) {
    match sample() {
        Ok(()) => log_gauge_with_labels(
            &BACKEND_MEMORY_TELEMETRY_SOURCE_UP_INFO,
            1.0,
            vec![StaticMetricLabel::new("source", source)],
        ),
        Err(error) => {
            log_gauge_with_labels(
                &BACKEND_MEMORY_TELEMETRY_SOURCE_UP_INFO,
                0.0,
                vec![StaticMetricLabel::new("source", source)],
            );
            failures.push(SourceFailure { source, error });
        },
    }
}

fn report_process_memory() -> anyhow::Result<()> {
    let process = parse_process_status(&fs::read_to_string("/proc/self/status")?)?;
    for (component, value) in [
        ("virtual", process.virtual_bytes),
        ("resident", process.resident_bytes),
        ("resident_anon", process.resident_anon_bytes),
        ("resident_file", process.resident_file_bytes),
        ("resident_shmem", process.resident_shmem_bytes),
        ("swap", process.swap_bytes),
    ] {
        log_gauge_with_labels(
            &BACKEND_PROCESS_MEMORY_BYTES,
            value as f64,
            vec![StaticMetricLabel::new("component", component)],
        );
    }
    Ok(())
}

fn report_allocator_memory() -> anyhow::Result<()> {
    if let Some(allocator) = allocator_memory()? {
        log_gauge(&BACKEND_ALLOCATOR_TELEMETRY_INFO, 1.0);
        for (component, value) in [
            ("arena", allocator.arena_bytes),
            ("mmap", allocator.mmap_bytes),
            ("in_use", allocator.in_use_bytes),
            ("free", allocator.free_bytes),
            ("releasable", allocator.releasable_bytes),
        ] {
            log_gauge_with_labels(
                &BACKEND_ALLOCATOR_MEMORY_BYTES,
                value as f64,
                vec![StaticMetricLabel::new("component", component)],
            );
        }
        log_gauge(
            &BACKEND_ALLOCATOR_MMAP_REGIONS_INFO,
            allocator.mmap_regions as f64,
        );
    } else {
        log_gauge(&BACKEND_ALLOCATOR_TELEMETRY_INFO, 0.0);
    }
    Ok(())
}

fn report_cgroup_memory(
    root: &Path,
    cgroup: anyhow::Result<Option<CgroupMemory>>,
    failures: &mut Vec<SourceFailure>,
) {
    let cgroup = match cgroup {
        Ok(cgroup) => {
            log_gauge_with_labels(
                &BACKEND_MEMORY_TELEMETRY_SOURCE_UP_INFO,
                1.0,
                vec![StaticMetricLabel::new("source", "cgroup_controller")],
            );
            cgroup
        },
        Err(error) => {
            log_gauge(&BACKEND_CGROUP_MEMORY_CONTROLLER_INFO, 0.0);
            log_gauge_with_labels(
                &BACKEND_MEMORY_TELEMETRY_SOURCE_UP_INFO,
                0.0,
                vec![StaticMetricLabel::new("source", "cgroup_controller")],
            );
            failures.push(SourceFailure {
                source: "cgroup_controller",
                error,
            });
            None
        },
    };
    let Some(cgroup) = cgroup else {
        log_gauge(&BACKEND_CGROUP_MEMORY_CONTROLLER_INFO, 0.0);
        for source in ["cgroup_stat", "cgroup_events"] {
            log_gauge_with_labels(
                &BACKEND_MEMORY_TELEMETRY_SOURCE_UP_INFO,
                0.0,
                vec![StaticMetricLabel::new("source", source)],
            );
        }
        set_cgroup_field_availability(0.0);
        return;
    };

    log_gauge(&BACKEND_CGROUP_MEMORY_CONTROLLER_INFO, 1.0);
    log_gauge_with_labels(
        &BACKEND_CGROUP_MEMORY_BYTES,
        cgroup.current_bytes as f64,
        vec![StaticMetricLabel::new("component", "current")],
    );
    match cgroup.max_bytes {
        Some(max_bytes) => {
            log_gauge(&BACKEND_CGROUP_MEMORY_LIMITED_INFO, 1.0);
            log_gauge_with_labels(
                &BACKEND_CGROUP_MEMORY_BYTES,
                max_bytes as f64,
                vec![StaticMetricLabel::new("component", "max")],
            );
        },
        None => log_gauge(&BACKEND_CGROUP_MEMORY_LIMITED_INFO, 0.0),
    }

    sample_source("cgroup_stat", || report_cgroup_stat(root), failures);
    sample_source("cgroup_events", || report_cgroup_events(root), failures);
}

fn report_cgroup_stat(root: &Path) -> anyhow::Result<()> {
    let stat = parse_keyed_u64(&fs::read_to_string(root.join("memory.stat"))?)?;
    for component in ["anon", "file", "kernel", "shmem", "sock"] {
        let value = stat.get(component);
        log_gauge_with_labels(
            &BACKEND_CGROUP_MEMORY_COMPONENT_AVAILABLE_INFO,
            if value.is_some() { 1.0 } else { 0.0 },
            vec![StaticMetricLabel::new("component", component)],
        );
        if let Some(value) = value {
            log_gauge_with_labels(
                &BACKEND_CGROUP_MEMORY_BYTES,
                *value as f64,
                vec![StaticMetricLabel::new("component", component)],
            );
        }
    }
    anyhow::ensure!(
        stat.contains_key("anon") && stat.contains_key("file"),
        "cgroup memory.stat is missing required anon or file accounting"
    );
    Ok(())
}

fn report_cgroup_events(root: &Path) -> anyhow::Result<()> {
    let events = parse_keyed_u64(&fs::read_to_string(root.join("memory.events"))?)?;
    for event in ["low", "high", "max", "oom", "oom_kill"] {
        let value = events.get(event);
        log_gauge_with_labels(
            &BACKEND_CGROUP_MEMORY_EVENT_AVAILABLE_INFO,
            if value.is_some() { 1.0 } else { 0.0 },
            vec![StaticMetricLabel::new("event", event)],
        );
        if let Some(value) = value {
            log_gauge_with_labels(
                &BACKEND_CGROUP_MEMORY_EVENTS_INFO,
                *value as f64,
                vec![StaticMetricLabel::new("event", event)],
            );
        }
    }
    anyhow::ensure!(
        ["low", "high", "max", "oom", "oom_kill"]
            .iter()
            .all(|event| events.contains_key(*event)),
        "cgroup memory.events is missing a required event"
    );

    let oom_group_kill = events.get("oom_group_kill");
    log_gauge_with_labels(
        &BACKEND_CGROUP_MEMORY_EVENT_AVAILABLE_INFO,
        if oom_group_kill.is_some() { 1.0 } else { 0.0 },
        vec![StaticMetricLabel::new("event", "oom_group_kill")],
    );
    if let Some(value) = oom_group_kill {
        log_gauge_with_labels(
            &BACKEND_CGROUP_MEMORY_EVENTS_INFO,
            *value as f64,
            vec![StaticMetricLabel::new("event", "oom_group_kill")],
        );
    }
    Ok(())
}

fn set_cgroup_field_availability(value: f64) {
    for component in ["anon", "file", "kernel", "shmem", "sock"] {
        log_gauge_with_labels(
            &BACKEND_CGROUP_MEMORY_COMPONENT_AVAILABLE_INFO,
            value,
            vec![StaticMetricLabel::new("component", component)],
        );
    }
    for event in ["low", "high", "max", "oom", "oom_kill", "oom_group_kill"] {
        log_gauge_with_labels(
            &BACKEND_CGROUP_MEMORY_EVENT_AVAILABLE_INFO,
            value,
            vec![StaticMetricLabel::new("event", event)],
        );
    }
}

fn parse_process_status(status: &str) -> anyhow::Result<ProcessMemory> {
    let mut values = BTreeMap::new();
    for line in status.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if matches!(
            name,
            "VmSize" | "VmRSS" | "RssAnon" | "RssFile" | "RssShmem" | "VmSwap"
        ) {
            anyhow::ensure!(
                values
                    .insert(name, parse_kib(value).with_context(|| name.to_owned())?)
                    .is_none(),
                "duplicate {name} in /proc/self/status"
            );
        }
    }
    let get = |name| {
        values
            .get(name)
            .copied()
            .with_context(|| format!("/proc/self/status missing {name}"))
    };
    let memory = ProcessMemory {
        virtual_bytes: get("VmSize")?,
        resident_bytes: get("VmRSS")?,
        resident_anon_bytes: get("RssAnon")?,
        resident_file_bytes: get("RssFile")?,
        resident_shmem_bytes: get("RssShmem")?,
        swap_bytes: get("VmSwap")?,
    };
    let resident_parts = memory
        .resident_anon_bytes
        .checked_add(memory.resident_file_bytes)
        .and_then(|value| value.checked_add(memory.resident_shmem_bytes))
        .context("process resident-memory accounting overflow")?;
    anyhow::ensure!(
        resident_parts == memory.resident_bytes,
        "process resident-memory accounting does not reconcile"
    );
    Ok(memory)
}

fn parse_kib(value: &str) -> anyhow::Result<u64> {
    let mut fields = value.split_whitespace();
    let kib: u64 = fields.next().context("missing KiB value")?.parse()?;
    anyhow::ensure!(fields.next() == Some("kB"), "memory value is not in KiB");
    anyhow::ensure!(fields.next().is_none(), "memory value has trailing fields");
    kib.checked_mul(1024).context("memory byte count overflow")
}

#[cfg(target_env = "gnu")]
fn allocator_memory() -> anyhow::Result<Option<AllocatorMemory>> {
    // SAFETY: `mallinfo2` has no arguments and returns a value snapshot. glibc
    // documents it as MT-Safe.
    let info = unsafe { libc::mallinfo2() };
    let arena_bytes = u64::try_from(info.arena)?;
    let in_use_bytes = u64::try_from(info.uordblks)?;
    let free_bytes = u64::try_from(info.fordblks)?;
    anyhow::ensure!(
        in_use_bytes.checked_add(free_bytes) == Some(arena_bytes),
        "allocator arena accounting does not reconcile"
    );
    let releasable_bytes = u64::try_from(info.keepcost)?;
    anyhow::ensure!(
        releasable_bytes <= free_bytes,
        "allocator releasable bytes exceed free arena bytes"
    );
    Ok(Some(AllocatorMemory {
        arena_bytes,
        mmap_bytes: u64::try_from(info.hblkhd)?,
        in_use_bytes,
        free_bytes,
        releasable_bytes,
        mmap_regions: u64::try_from(info.hblks)?,
    }))
}

#[cfg(not(target_env = "gnu"))]
fn allocator_memory() -> anyhow::Result<Option<AllocatorMemory>> {
    Ok(None)
}

fn read_cgroup_memory(root: &Path) -> anyhow::Result<Option<CgroupMemory>> {
    let current_path = root.join("memory.current");
    let max_path = root.join("memory.max");
    match (current_path.exists(), max_path.exists()) {
        (false, false) => return Ok(None),
        (true, true) => {},
        _ => anyhow::bail!(
            "incomplete cgroup v2 memory controller: memory.current and memory.max must both exist"
        ),
    }
    let current_bytes = parse_single_u64(&fs::read_to_string(current_path)?)?;
    let max = fs::read_to_string(max_path)?;
    let max = max.trim();
    let max_bytes = if max == "max" {
        None
    } else {
        Some(max.parse().context("invalid cgroup memory.max")?)
    };
    Ok(Some(CgroupMemory {
        current_bytes,
        max_bytes,
    }))
}

fn parse_single_u64(value: &str) -> anyhow::Result<u64> {
    let mut fields = value.split_whitespace();
    let value = fields.next().context("missing integer")?.parse()?;
    anyhow::ensure!(fields.next().is_none(), "integer has trailing fields");
    Ok(value)
}

fn parse_keyed_u64(input: &str) -> anyhow::Result<BTreeMap<String, u64>> {
    let mut values = BTreeMap::new();
    for line in input.lines() {
        let mut fields = line.split_whitespace();
        let key = fields.next().context("missing key")?;
        let value = fields.next().context("missing value")?.parse()?;
        anyhow::ensure!(
            fields.next().is_none(),
            "value for {key} has trailing fields"
        );
        anyhow::ensure!(
            values.insert(key.to_owned(), value).is_none(),
            "duplicate key {key}"
        );
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{
            Duration,
            Instant,
        },
    };

    use common::memory_pressure::MemoryPressureSignal;
    use tempfile::TempDir;

    #[cfg(target_env = "gnu")]
    use super::allocator_arena_count;
    use super::{
        parse_process_status,
        pressure_state,
        read_cgroup_memory,
        report,
        report_cgroup_events,
        report_cgroup_stat,
        signed_memory_change,
        startup_budget_headroom,
        CgroupMemory,
        CgroupMemoryPressureController,
        MemoryBudgetComponent,
        ProcessMemory,
        StartupMemoryBudget,
    };

    #[test]
    fn memory_pressure_state_uses_inclusive_enter_and_exit_hysteresis() {
        let enter = 3;
        let exit = 5;

        assert!(!pressure_state(false, 4, enter, exit));
        assert!(pressure_state(false, 3, enter, exit));
        assert!(pressure_state(true, 4, enter, exit));
        assert!(!pressure_state(true, 5, enter, exit));
    }

    #[test]
    fn reclamation_signal_is_published_after_controller_state_changes() {
        let signal = MemoryPressureSignal::default();
        let mut controller = CgroupMemoryPressureController {
            external_request_shedding: None,
            shedding_enter_headroom_bytes: 3,
            shedding_exit_headroom_bytes: 5,
            memory_reclamation: signal.clone(),
            reclamation_active: false,
            reclamation_enabled: true,
            reclamation_enter_headroom_bytes: 6,
            reclamation_exit_headroom_bytes: 8,
            allocator_trim_enabled: false,
            allocator_trim_min_free_bytes: 1,
            allocator_trim_cooldown: Duration::from_secs(1),
            last_allocator_trim_evaluated: None,
        };

        controller
            .update(&CgroupMemory {
                current_bytes: 94,
                max_bytes: Some(100),
            })
            .unwrap();
        assert!(controller.reclamation_active);
        assert!(!signal.is_active());
        controller.publish_reclamation_state();
        assert!(signal.is_active());

        controller
            .update(&CgroupMemory {
                current_bytes: 92,
                max_bytes: Some(100),
            })
            .unwrap();
        assert!(!controller.reclamation_active);
        assert!(signal.is_active());
        controller.publish_reclamation_state();
        assert!(!signal.is_active());
    }

    #[test]
    fn allocator_trim_eligibility_scan_obeys_the_trim_cooldown() {
        let cooldown = Duration::from_secs(300);
        let mut controller = CgroupMemoryPressureController {
            external_request_shedding: None,
            shedding_enter_headroom_bytes: 3,
            shedding_exit_headroom_bytes: 5,
            memory_reclamation: MemoryPressureSignal::default(),
            reclamation_active: true,
            reclamation_enabled: true,
            reclamation_enter_headroom_bytes: 6,
            reclamation_exit_headroom_bytes: 8,
            allocator_trim_enabled: true,
            allocator_trim_min_free_bytes: 1,
            allocator_trim_cooldown: cooldown,
            last_allocator_trim_evaluated: None,
        };

        assert!(controller.claim_allocator_trim());
        assert!(!controller.claim_allocator_trim());
        controller.last_allocator_trim_evaluated = Some(Instant::now() - cooldown);
        assert!(controller.claim_allocator_trim());
    }

    #[test]
    fn trim_memory_change_preserves_release_direction() {
        assert_eq!(signed_memory_change(100, 40), -60);
        assert_eq!(signed_memory_change(40, 100), 60);
        assert_eq!(signed_memory_change(40, 40), 0);
    }

    #[cfg(target_env = "gnu")]
    #[test]
    fn live_glibc_arena_count_is_bounded_and_nonzero() {
        assert!(allocator_arena_count().unwrap().unwrap() > 0);
    }

    #[test]
    fn live_process_memory_accounting_reconciles() {
        assert!(report().is_empty());
    }

    #[test]
    fn process_status_requires_reconciled_rss_components() {
        let status = "\
VmSize:\t1000 kB
VmRSS:\t600 kB
RssAnon:\t300 kB
RssFile:\t200 kB
RssShmem:\t100 kB
VmSwap:\t50 kB
";
        assert_eq!(
            parse_process_status(status).unwrap(),
            ProcessMemory {
                virtual_bytes: 1_024_000,
                resident_bytes: 614_400,
                resident_anon_bytes: 307_200,
                resident_file_bytes: 204_800,
                resident_shmem_bytes: 102_400,
                swap_bytes: 51_200,
            }
        );

        let inconsistent = status.replace("VmRSS:\t600", "VmRSS:\t601");
        assert!(parse_process_status(&inconsistent)
            .unwrap_err()
            .to_string()
            .contains("does not reconcile"));
    }

    #[test]
    fn cgroup_memory_preserves_absolute_events_and_unlimited_state() {
        let root = TempDir::new().unwrap();
        fs::write(root.path().join("memory.current"), "123\n").unwrap();
        fs::write(root.path().join("memory.max"), "max\n").unwrap();
        fs::write(root.path().join("memory.stat"), "anon 10\nfile 20\n").unwrap();
        fs::write(
            root.path().join("memory.events"),
            "low 1\nhigh 2\nmax 3\noom 4\noom_kill 5\noom_group_kill 6\n",
        )
        .unwrap();

        let memory = read_cgroup_memory(root.path()).unwrap().unwrap();
        assert_eq!(memory.current_bytes, 123);
        assert_eq!(memory.max_bytes, None);
        report_cgroup_stat(root.path()).unwrap();
        report_cgroup_events(root.path()).unwrap();
    }

    #[test]
    fn missing_cgroup_memory_controller_is_explicit() {
        let root = TempDir::new().unwrap();
        assert_eq!(read_cgroup_memory(root.path()).unwrap(), None);
    }

    #[test]
    fn finite_cgroup_limit_must_cover_configured_budget() {
        let root = TempDir::new().unwrap();
        fs::write(root.path().join("memory.current"), "80\n").unwrap();
        fs::write(root.path().join("memory.max"), "150\n").unwrap();
        let budget = StartupMemoryBudget::new(vec![MemoryBudgetComponent {
            name: "test",
            bytes: 100,
        }])
        .unwrap();

        assert_eq!(
            startup_budget_headroom(root.path(), &budget).unwrap(),
            Some(50)
        );

        fs::write(root.path().join("memory.max"), "99\n").unwrap();
        assert!(startup_budget_headroom(root.path(), &budget)
            .unwrap_err()
            .to_string()
            .contains("exceeds the finite cgroup memory limit"));
    }

    #[test]
    fn unlimited_or_absent_cgroup_skips_feasibility_failure() {
        let budget = StartupMemoryBudget::new(vec![MemoryBudgetComponent {
            name: "test",
            bytes: 100,
        }])
        .unwrap();

        let absent = TempDir::new().unwrap();
        assert_eq!(
            startup_budget_headroom(absent.path(), &budget).unwrap(),
            None
        );

        let unlimited = TempDir::new().unwrap();
        fs::write(unlimited.path().join("memory.current"), "80\n").unwrap();
        fs::write(unlimited.path().join("memory.max"), "max\n").unwrap();
        assert_eq!(
            startup_budget_headroom(unlimited.path(), &budget).unwrap(),
            None
        );
    }

    #[test]
    fn malformed_present_cgroup_controller_fails_feasibility_check() {
        let root = TempDir::new().unwrap();
        fs::write(root.path().join("memory.current"), "80\n").unwrap();
        fs::write(root.path().join("memory.max"), "not-a-limit\n").unwrap();
        let budget = StartupMemoryBudget::new(vec![MemoryBudgetComponent {
            name: "test",
            bytes: 100,
        }])
        .unwrap();

        assert!(startup_budget_headroom(root.path(), &budget).is_err());
    }

    #[test]
    fn incomplete_present_cgroup_controller_fails_feasibility_check() {
        let root = TempDir::new().unwrap();
        fs::write(root.path().join("memory.max"), "100\n").unwrap();
        let budget = StartupMemoryBudget::new(vec![MemoryBudgetComponent {
            name: "test",
            bytes: 100,
        }])
        .unwrap();

        assert!(startup_budget_headroom(root.path(), &budget).is_err());
    }
}
