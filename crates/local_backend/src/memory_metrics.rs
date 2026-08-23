use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    os::unix::ffi::OsStringExt,
    path::{
        Path,
        PathBuf,
    },
    sync::mpsc,
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
        LOCAL_BACKEND_STARTUP_ISOLATE_MEMORY_COMMIT_PERCENT,
        LOCAL_NODE_EXECUTOR_TOTAL_RSS_BUDGET_BYTES,
        MAX_ISOLATE_WORKERS,
        SOURCE_MAP_CACHE_MAX_SIZE_BYTES,
        UDF_CACHE_MAX_SIZE,
    },
    memory_pressure::MemoryPressureSignal,
    runtime::{
        propagate_tracing_blocking,
        tokio_spawn_blocking,
        Runtime,
    },
    shutdown::ShutdownSignal,
};
use metrics::{
    log_counter,
    log_counter_with_labels,
    log_gauge,
    log_gauge_with_labels,
    register_convex_counter,
    register_convex_gauge,
    StaticMetricLabel,
};
#[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
use metrics::{
    log_distribution,
    register_convex_histogram,
};
#[cfg(local_backend_jemalloc)]
use tikv_jemalloc_ctl::{
    arenas,
    background_thread,
    epoch,
    opt,
    raw,
    stats,
};

const REPORT_INTERVAL: Duration = Duration::from_secs(15);
const PRESSURE_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const ALLOCATOR_ARENA_REPORT_INTERVAL: Duration = Duration::from_secs(5 * 60);
const MEMORY_REPORTS_PER_ALLOCATOR_ARENA_REPORT: usize =
    (ALLOCATOR_ARENA_REPORT_INTERVAL.as_secs() / REPORT_INTERVAL.as_secs()) as usize;
#[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
const MAX_MALLOC_INFO_BYTES: usize = 4 * 1024 * 1024;
#[cfg(any(
    test,
    all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc))
))]
const MAX_MALLOC_INFO_XML_DEPTH: usize = 16;
#[cfg(any(
    test,
    all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc))
))]
const MAX_MALLOC_INFO_TAG_ATTRIBUTES: usize = 16;
const PROC_SELF_CGROUP: &str = "/proc/self/cgroup";
const PROC_SELF_MOUNTINFO: &str = "/proc/self/mountinfo";

register_convex_gauge!(
    BACKEND_PROCESS_MEMORY_BYTES,
    "Memory attributed directly to the backend process",
    &["component"]
);
register_convex_gauge!(
    BACKEND_ALLOCATOR_MEMORY_BYTES,
    "Memory reported by the selected backend process allocator",
    &["component"]
);
register_convex_gauge!(
    BACKEND_ALLOCATOR_SELECTED_INFO,
    "Selected backend process allocator",
    &["allocator"]
);
register_convex_gauge!(
    BACKEND_ALLOCATOR_CONFIGURATION_INFO,
    "Numeric configuration of the selected backend process allocator",
    &["component"]
);
register_convex_gauge!(
    BACKEND_ALLOCATOR_MMAP_REGIONS_INFO,
    "Number of mmap-backed regions reported by glibc malloc"
);
register_convex_gauge!(
    BACKEND_ALLOCATOR_ARENAS_INFO,
    "Number of initialized arenas reported by the selected backend process allocator"
);
register_convex_gauge!(
    BACKEND_ALLOCATOR_ARENA_TELEMETRY_INFO,
    "Whether allocator arena-count telemetry is available"
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
    BACKEND_STARTUP_ISOLATE_MEMORY_COMMIT_PERCENT_INFO,
    "Percentage of aggregate isolate memory ceilings included in the startup memory budget"
);
register_convex_gauge!(
    BACKEND_STARTUP_ISOLATE_MEMORY_BYTES,
    "Aggregate isolate memory used for startup planning",
    &["accounting_kind"]
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
    "Finite cgroup memory headroom used by the memory-pressure controller"
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
    "Explicit backend allocator trim attempts by outcome",
    &["outcome"]
);
#[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
register_convex_histogram!(
    BACKEND_ALLOCATOR_TRIM_SECONDS,
    "Duration of explicit backend allocator trim calls"
);
#[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
register_convex_gauge!(
    BACKEND_ALLOCATOR_TRIM_MEMORY_CHANGE_BYTES,
    "Immediate signed memory change after explicit allocator trim",
    &["component"]
);
#[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
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

#[cfg(not(local_backend_jemalloc))]
#[derive(Debug, Eq, PartialEq)]
struct GlibcAllocatorMemory {
    arena_bytes: u64,
    mmap_bytes: u64,
    in_use_bytes: u64,
    free_bytes: u64,
    main_arena_top_chunk_bytes: u64,
    mmap_regions: u64,
}

#[cfg(local_backend_jemalloc)]
#[derive(Debug, Eq, PartialEq)]
struct JemallocMemory {
    allocated_bytes: u64,
    active_bytes: u64,
    metadata_bytes: u64,
    resident_bytes: u64,
    mapped_bytes: u64,
    retained_bytes: u64,
}

#[cfg(local_backend_jemalloc)]
#[derive(Debug, Eq, PartialEq)]
struct JemallocConfiguration {
    narenas: u32,
    dirty_decay_ms: libc::ssize_t,
    abort_on_invalid_configuration: bool,
    background_thread_configured: bool,
    background_thread_active: bool,
    statistics_supported: bool,
    profiling_supported: bool,
    profiling_enabled: bool,
    profiling_active: bool,
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
    latest_headroom_bytes: u64,
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

#[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
#[derive(Debug, Eq, PartialEq)]
struct PageFaults {
    minor: u64,
    major: u64,
}

#[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
#[derive(Debug, Eq, PartialEq)]
struct AllocatorTrimSnapshot {
    process: ProcessMemory,
    allocator: Option<GlibcAllocatorMemory>,
    cgroup_current_bytes: u64,
    cgroup_anon_bytes: u64,
    page_faults: PageFaults,
}

enum AllocatorTrimRun {
    Unsupported,
    #[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
    BelowFreeThreshold,
    #[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
    Completed {
        before: AllocatorTrimSnapshot,
        after: anyhow::Result<AllocatorTrimSnapshot>,
        returned: bool,
        elapsed: Duration,
    },
}

struct AllocatorTrimTask {
    completion: mpsc::Receiver<anyhow::Result<()>>,
}

impl AllocatorTrimTask {
    fn start(root: PathBuf, min_free_bytes: u64) -> anyhow::Result<Self> {
        let (completion_tx, completion) = mpsc::sync_channel(1);
        // Tokio runtime shutdown waits indefinitely for spawn_blocking tasks.
        // malloc_trim cannot be canceled after it enters libc, so detach this
        // duration-unbounded call from the runtime and let process exit remain
        // its final termination boundary.
        std::thread::Builder::new()
            .name("backend-allocator-trim".to_owned())
            .spawn(propagate_tracing_blocking(move || {
                let _ = completion_tx.send(run_allocator_trim(&root, min_free_bytes));
            }))?;
        Ok(Self { completion })
    }

    fn try_result(&self) -> Option<anyhow::Result<()>> {
        match self.completion.try_recv() {
            Ok(result) => Some(result),
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => {
                log_allocator_trim_attempt("sample_failure");
                Some(Err(anyhow::anyhow!(
                    "allocator trim worker stopped without a result"
                )))
            },
        }
    }
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

#[derive(Debug, Eq, PartialEq)]
struct StartupIsolateMemoryAccounting {
    raw_capacity_bytes: u64,
    accounted_bytes: u64,
    commit_percent: usize,
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

fn account_isolate_memory_capacity(raw_bytes: u64, commit_percent: usize) -> anyhow::Result<u64> {
    anyhow::ensure!(
        (1..=100).contains(&commit_percent),
        "startup isolate memory commit percentage must be between 1 and 100"
    );
    let commit_percent = u64::try_from(commit_percent)?;
    let whole = (raw_bytes / 100)
        .checked_mul(commit_percent)
        .context("accounted isolate memory capacity overflow")?;
    let remainder = (raw_bytes % 100)
        .checked_mul(commit_percent)
        .context("accounted isolate memory capacity remainder overflow")?
        .div_ceil(100);
    whole
        .checked_add(remainder)
        .context("accounted isolate memory capacity overflow")
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
    validate_allocator_configuration(allocator_trim_enabled)?;
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

    let cgroup_root = effective_cgroup_root()?
        .context("Memory pressure control requires a mounted cgroup v2 hierarchy")?;
    let cgroup = read_cgroup_memory(&cgroup_root)?
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
        latest_headroom_bytes: headroom_bytes,
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
        self.latest_headroom_bytes = headroom_bytes;
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

    fn publish_reclamation_state(&self, allocator_trim_in_flight: bool) {
        let was_active = self.memory_reclamation.is_active();
        // Defer only a new reclamation entry while trim has the first chance to
        // recover. The numeric shedding boundary remains the safety cutoff even
        // when request shedding is disabled, so owner reclamation cannot remain
        // behind an unbounded trim. An existing signal still clears immediately
        // on recovery.
        let shedding_active = self
            .external_request_shedding
            .as_ref()
            .is_some_and(|shedding| shedding.is_active());
        let reached_shedding_boundary =
            self.latest_headroom_bytes <= self.shedding_enter_headroom_bytes;
        if allocator_trim_in_flight
            && self.reclamation_active
            && !was_active
            && !shedding_active
            && !reached_shedding_boundary
        {
            return;
        }
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
) -> anyhow::Result<PathBuf> {
    let cgroup_root = effective_cgroup_root()?
        .context("Enabled memory pressure controller lost its mounted cgroup v2 hierarchy")?;
    let cgroup = read_cgroup_memory(&cgroup_root)?
        .context("Enabled memory pressure controller lost the cgroup v2 memory controller")?;
    controller.update(&cgroup)?;
    Ok(cgroup_root)
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

fn run_allocator_trim(root: &Path, min_free_bytes: u64) -> anyhow::Result<()> {
    let run = match measure_allocator_trim(root, min_free_bytes) {
        Ok(run) => run,
        Err(error) => {
            log_allocator_trim_attempt("sample_failure");
            return Err(error);
        },
    };
    match run {
        AllocatorTrimRun::Unsupported => {
            log_allocator_trim_attempt("unsupported");
            Ok(())
        },
        #[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
        AllocatorTrimRun::BelowFreeThreshold => Ok(()),
        #[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
        AllocatorTrimRun::Completed {
            before,
            after,
            returned,
            elapsed,
        } => finish_allocator_trim(before, after, returned, elapsed),
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
fn finish_allocator_trim(
    before: AllocatorTrimSnapshot,
    after: anyhow::Result<AllocatorTrimSnapshot>,
    returned: bool,
    elapsed: Duration,
) -> anyhow::Result<()> {
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

#[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
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
    let after = allocator_trim_snapshot(root);
    Ok(AllocatorTrimRun::Completed {
        before,
        after,
        returned,
        elapsed,
    })
}

#[cfg(any(
    not(all(target_os = "linux", target_env = "gnu")),
    local_backend_jemalloc
))]
fn measure_allocator_trim(_root: &Path, _min_free_bytes: u64) -> anyhow::Result<AllocatorTrimRun> {
    Ok(AllocatorTrimRun::Unsupported)
}

#[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
fn log_trim_memory_change(component: &'static str, before: u64, after: u64) {
    let change = signed_memory_change(before, after);
    log_gauge_with_labels(
        &BACKEND_ALLOCATOR_TRIM_MEMORY_CHANGE_BYTES,
        change as f64,
        vec![StaticMetricLabel::new("component", component)],
    );
}

#[cfg(any(
    test,
    all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc))
))]
fn signed_memory_change(before: u64, after: u64) -> i128 {
    i128::from(after) - i128::from(before)
}

#[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
fn allocator_trim_snapshot(root: &Path) -> anyhow::Result<AllocatorTrimSnapshot> {
    let process = parse_process_status(&fs::read_to_string("/proc/self/status")?)?;
    let allocator = glibc_allocator_memory()?;
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

#[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
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

#[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
fn explicit_allocator_trim() -> bool {
    // SAFETY: glibc documents `malloc_trim` as MT-Safe. A zero pad asks the
    // allocator to retain no extra main-arena top space.
    (unsafe { libc::malloc_trim(0) }) != 0
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

#[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
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
    Ok(Some(parse_malloc_info_arena_count(xml)?))
}

#[cfg(local_backend_jemalloc)]
fn allocator_arena_count() -> anyhow::Result<Option<usize>> {
    epoch::advance()?;
    // `opt.narenas` bounds automatic thread multiplexing, but the dedicated
    // oversize arena and any explicitly created arenas have higher indexes.
    let narenas = usize::try_from(arenas::narenas::read()?)?;
    let mut initialized_mib = [0usize; 3];
    raw::name_to_mib(b"arena.0.initialized\0", &mut initialized_mib)?;
    let mut initialized = 0usize;
    for arena in 0..narenas {
        initialized_mib[1] = arena;
        // SAFETY: jemalloc documents arena.<i>.initialized as a boolean
        // mallctl value, and name_to_mib resolved the remaining components.
        if unsafe { raw::read_mib::<bool>(&initialized_mib)? } {
            initialized = initialized
                .checked_add(1)
                .context("jemalloc initialized arena count overflow")?;
        }
    }
    anyhow::ensure!(initialized > 0, "jemalloc reported no initialized arenas");
    Ok(Some(initialized))
}

#[cfg(all(
    not(all(target_os = "linux", target_env = "gnu")),
    not(local_backend_jemalloc)
))]
fn allocator_arena_count() -> anyhow::Result<Option<usize>> {
    Ok(None)
}

#[cfg(any(
    test,
    all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc))
))]
fn parse_malloc_info_arena_count(xml: &str) -> anyhow::Result<usize> {
    let bytes = xml.as_bytes();
    anyhow::ensure!(!bytes.contains(&0), "malloc_info output contains NUL");

    let mut open_tags = [""; MAX_MALLOC_INFO_XML_DEPTH];
    let mut depth = 0;
    let mut root_seen = false;
    let mut root_complete = false;
    let mut arena_count = 0usize;
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'<' {
            anyhow::ensure!(
                bytes[index].is_ascii_whitespace(),
                "malloc_info output contains text outside a tag"
            );
            index += 1;
            continue;
        }
        anyhow::ensure!(
            !root_complete,
            "malloc_info output contains content after its root element"
        );
        index += 1;
        anyhow::ensure!(
            index < bytes.len(),
            "malloc_info output has an incomplete tag"
        );

        if bytes[index] == b'/' {
            index += 1;
            let name = parse_malloc_info_xml_name(xml, &mut index)?;
            skip_ascii_whitespace(bytes, &mut index);
            anyhow::ensure!(
                bytes.get(index) == Some(&b'>'),
                "malloc_info closing tag is malformed"
            );
            index += 1;
            anyhow::ensure!(depth > 0, "malloc_info output has an unmatched closing tag");
            anyhow::ensure!(
                open_tags[depth - 1] == name,
                "malloc_info output has mismatched tags"
            );
            depth -= 1;
            if depth == 0 {
                root_complete = true;
            }
            continue;
        }

        let name = parse_malloc_info_xml_name(xml, &mut index)?;
        if depth == 0 {
            anyhow::ensure!(!root_seen, "malloc_info output has multiple root elements");
            anyhow::ensure!(name == "malloc", "malloc_info root element is not malloc");
            root_seen = true;
        }

        let mut attributes = [""; MAX_MALLOC_INFO_TAG_ATTRIBUTES];
        let mut attribute_count = 0;
        let self_closing = loop {
            let before_whitespace = index;
            skip_ascii_whitespace(bytes, &mut index);
            match bytes.get(index) {
                Some(b'>') => {
                    index += 1;
                    break false;
                },
                Some(b'/') if bytes.get(index + 1) == Some(&b'>') => {
                    index += 2;
                    break true;
                },
                Some(_) => {},
                None => anyhow::bail!("malloc_info output has an incomplete opening tag"),
            }
            anyhow::ensure!(
                index > before_whitespace,
                "malloc_info tag attributes are not separated by whitespace"
            );
            anyhow::ensure!(
                attribute_count < attributes.len(),
                "malloc_info tag has too many attributes"
            );
            let attribute = parse_malloc_info_xml_name(xml, &mut index)?;
            anyhow::ensure!(
                !attributes[..attribute_count].contains(&attribute),
                "malloc_info tag has a duplicate attribute"
            );
            attributes[attribute_count] = attribute;
            attribute_count += 1;
            skip_ascii_whitespace(bytes, &mut index);
            anyhow::ensure!(
                bytes.get(index) == Some(&b'='),
                "malloc_info tag attribute is missing '='"
            );
            index += 1;
            skip_ascii_whitespace(bytes, &mut index);
            let quote = *bytes
                .get(index)
                .context("malloc_info tag attribute is missing a value")?;
            anyhow::ensure!(
                quote == b'\'' || quote == b'\"',
                "malloc_info tag attribute value is not quoted"
            );
            index += 1;
            while bytes.get(index).is_some_and(|byte| *byte != quote) {
                anyhow::ensure!(
                    bytes[index] != b'<',
                    "malloc_info tag attribute value contains '<'"
                );
                index += 1;
            }
            anyhow::ensure!(
                bytes.get(index) == Some(&quote),
                "malloc_info tag attribute value is unterminated"
            );
            index += 1;
        };

        if depth == 1 && name == "heap" {
            arena_count = arena_count
                .checked_add(1)
                .context("malloc_info arena count overflow")?;
        }
        if self_closing {
            if depth == 0 {
                root_complete = true;
            }
        } else {
            anyhow::ensure!(
                depth < open_tags.len(),
                "malloc_info output exceeds its nesting limit"
            );
            open_tags[depth] = name;
            depth += 1;
        }
    }

    anyhow::ensure!(root_seen, "malloc_info output has no root element");
    anyhow::ensure!(
        root_complete && depth == 0,
        "malloc_info output is truncated"
    );
    anyhow::ensure!(arena_count > 0, "malloc_info returned no allocator arenas");
    Ok(arena_count)
}

#[cfg(any(
    test,
    all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc))
))]
fn parse_malloc_info_xml_name<'a>(xml: &'a str, index: &mut usize) -> anyhow::Result<&'a str> {
    let bytes = xml.as_bytes();
    let start = *index;
    let first = *bytes
        .get(*index)
        .context("malloc_info tag or attribute name is missing")?;
    anyhow::ensure!(
        first.is_ascii_alphabetic() || matches!(first, b'_' | b':'),
        "malloc_info tag or attribute name is invalid"
    );
    *index += 1;
    while bytes.get(*index).is_some_and(|byte| {
        byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b':' | b'.' | b'-')
    }) {
        *index += 1;
    }
    Ok(&xml[start..*index])
}

#[cfg(any(
    test,
    all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc))
))]
fn skip_ascii_whitespace(bytes: &[u8], index: &mut usize) {
    while bytes
        .get(*index)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        *index += 1;
    }
}

pub fn validate_startup_budget() -> anyhow::Result<()> {
    let (budget, isolate_memory) = configured_startup_budget()?;
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
    log_gauge(
        &BACKEND_STARTUP_ISOLATE_MEMORY_COMMIT_PERCENT_INFO,
        isolate_memory.commit_percent as f64,
    );
    for (accounting_kind, bytes) in [
        ("raw_capacity", isolate_memory.raw_capacity_bytes),
        ("accounted", isolate_memory.accounted_bytes),
    ] {
        log_gauge_with_labels(
            &BACKEND_STARTUP_ISOLATE_MEMORY_BYTES,
            bytes as f64,
            vec![StaticMetricLabel::new("accounting_kind", accounting_kind)],
        );
    }

    let headroom = match effective_cgroup_root()? {
        Some(root) => startup_budget_headroom(&root, &budget)?,
        None => None,
    };
    match headroom {
        Some(headroom_bytes) => {
            log_gauge(&BACKEND_STARTUP_MEMORY_BUDGET_LIMIT_AVAILABLE_INFO, 1.0);
            log_gauge(
                &BACKEND_STARTUP_MEMORY_BUDGET_HEADROOM_BYTES,
                headroom_bytes as f64,
            );
            tracing::info!(
                configured_memory_budget_bytes = budget.total_bytes,
                isolate_memory_raw_capacity_bytes = isolate_memory.raw_capacity_bytes,
                isolate_memory_accounted_bytes = isolate_memory.accounted_bytes,
                isolate_memory_commit_percent = isolate_memory.commit_percent,
                memory_budget_headroom_bytes = headroom_bytes,
                "Validated configured startup memory budget against the cgroup limit"
            );
        },
        None => {
            log_gauge(&BACKEND_STARTUP_MEMORY_BUDGET_LIMIT_AVAILABLE_INFO, 0.0);
            tracing::info!(
                configured_memory_budget_bytes = budget.total_bytes,
                isolate_memory_raw_capacity_bytes = isolate_memory.raw_capacity_bytes,
                isolate_memory_accounted_bytes = isolate_memory.accounted_bytes,
                isolate_memory_commit_percent = isolate_memory.commit_percent,
                "Skipped startup memory feasibility because no finite cgroup v2 limit is available"
            );
        },
    }
    Ok(())
}

fn configured_startup_budget(
) -> anyhow::Result<(StartupMemoryBudget, StartupIsolateMemoryAccounting)> {
    configured_startup_budget_with_isolate_memory_commit_percent(
        *LOCAL_BACKEND_STARTUP_ISOLATE_MEMORY_COMMIT_PERCENT,
    )
}

fn configured_startup_budget_with_isolate_memory_commit_percent(
    commit_percent: usize,
) -> anyhow::Result<(StartupMemoryBudget, StartupIsolateMemoryAccounting)> {
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
    let accounted_isolate_heap_pool =
        account_isolate_memory_capacity(isolate_heap_pool, commit_percent)?;
    let accounted_isolate_array_buffer_pool =
        account_isolate_memory_capacity(isolate_array_buffer_pool, commit_percent)?;
    let isolate_memory = StartupIsolateMemoryAccounting {
        raw_capacity_bytes: isolate_heap_pool
            .checked_add(isolate_array_buffer_pool)
            .context("configured raw isolate memory capacity overflow")?,
        accounted_bytes: accounted_isolate_heap_pool
            .checked_add(accounted_isolate_array_buffer_pool)
            .context("configured accounted isolate memory capacity overflow")?,
        commit_percent,
    };
    let budget = StartupMemoryBudget::new(vec![
        MemoryBudgetComponent {
            name: "isolate_heap_pool",
            bytes: accounted_isolate_heap_pool,
        },
        MemoryBudgetComponent {
            name: "isolate_array_buffer_pool",
            bytes: accounted_isolate_array_buffer_pool,
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
            name: "local_node_rss_budget",
            bytes: u64::try_from(*LOCAL_NODE_EXECUTOR_TOTAL_RSS_BUDGET_BYTES)?,
        },
        MemoryBudgetComponent {
            name: "native_kernel_reserve",
            bytes: u64::try_from(*LOCAL_BACKEND_NATIVE_KERNEL_MEMORY_RESERVE_BYTES)?,
        },
    ])?;
    Ok((budget, isolate_memory))
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
                let mut allocator_trim: Option<AllocatorTrimTask> = None;
                loop {
                    let mut cgroup_root = match update_memory_pressure_controller(&mut controller) {
                        Ok(root) => root,
                        Err(error) => {
                            log_counter(&BACKEND_MEMORY_PRESSURE_FAILURES_TOTAL, 1);
                            shutdown.signal(error);
                            return;
                        },
                    };
                    let completed_trim = allocator_trim
                        .as_ref()
                        .and_then(AllocatorTrimTask::try_result);
                    if let Some(completed) = completed_trim {
                        allocator_trim = None;
                        if let Err(error) = completed {
                            // Trimming is an optional recovery action. Keep
                            // the remaining reclamation controls available
                            // when a diagnostic snapshot fails.
                            log_counter(&BACKEND_MEMORY_TELEMETRY_FAILURES_TOTAL, 1);
                            tracing::error!(
                                "Backend allocator trim or its telemetry failed: {error:#}"
                            );
                        }
                        // A completed trim may have crossed either controller
                        // boundary. Resample before publishing owner pressure.
                        cgroup_root = match update_memory_pressure_controller(&mut controller) {
                            Ok(root) => root,
                            Err(error) => {
                                log_counter(&BACKEND_MEMORY_PRESSURE_FAILURES_TOTAL, 1);
                                shutdown.signal(error);
                                return;
                            },
                        };
                    }
                    if allocator_trim.is_none() && controller.claim_allocator_trim() {
                        let min_free_bytes = controller.allocator_trim_min_free_bytes;
                        match AllocatorTrimTask::start(cgroup_root, min_free_bytes) {
                            Ok(task) => allocator_trim = Some(task),
                            Err(error) => {
                                log_allocator_trim_attempt("sample_failure");
                                log_counter(&BACKEND_MEMORY_TELEMETRY_FAILURES_TOTAL, 1);
                                tracing::error!(
                                    "Failed to start backend allocator trim worker: {error}"
                                );
                            },
                        }
                    }
                    // A trim runs independently so the controller keeps its
                    // one-second cgroup sampling and can activate external
                    // shedding while allocator work is slow. New owner pressure
                    // waits for that trim; recovery still clears an old signal.
                    controller.publish_reclamation_state(allocator_trim.is_some());
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
                    match effective_cgroup_root() {
                        Ok(Some(root)) => report_with_cgroup(&root, read_cgroup_memory(&root)),
                        Ok(None) => report_with_cgroup(Path::new(""), Ok(None)),
                        Err(error) => report_with_cgroup(Path::new(""), Err(error)),
                    }
                })
                .await;
                let failures = match report {
                    Ok(failures) => failures,
                    Err(error) => {
                        log_gauge(&BACKEND_ALLOCATOR_TELEMETRY_INFO, 0.0);
                        log_gauge(&BACKEND_CGROUP_MEMORY_CONTROLLER_INFO, 0.0);
                        log_gauge(&BACKEND_CGROUP_MEMORY_LIMITED_INFO, 0.0);
                        set_cgroup_field_availability(0.0);
                        for source in [
                            "process",
                            "allocator",
                            "cgroup_controller",
                            "cgroup_stat",
                            "cgroup_events",
                        ] {
                            log_gauge_with_labels(
                                &BACKEND_MEMORY_TELEMETRY_SOURCE_UP_INFO,
                                0.0,
                                vec![StaticMetricLabel::new("source", source)],
                            );
                        }
                        vec![SourceFailure {
                            source: "blocking_task",
                            error: error.into(),
                        }]
                    },
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
    match effective_cgroup_root() {
        Ok(Some(root)) => report_with_cgroup(&root, read_cgroup_memory(&root)),
        Ok(None) => report_with_cgroup(Path::new(""), Ok(None)),
        Err(error) => report_with_cgroup(Path::new(""), Err(error)),
    }
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

#[cfg(not(local_backend_jemalloc))]
fn report_allocator_memory() -> anyhow::Result<()> {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    let allocator_name = "glibc";
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    let allocator_name = "system";
    log_gauge_with_labels(
        &BACKEND_ALLOCATOR_SELECTED_INFO,
        1.0,
        vec![StaticMetricLabel::new("allocator", allocator_name)],
    );
    let allocator = match glibc_allocator_memory() {
        Ok(allocator) => allocator,
        Err(error) => {
            log_gauge(&BACKEND_ALLOCATOR_TELEMETRY_INFO, 0.0);
            return Err(error);
        },
    };
    if let Some(allocator) = allocator {
        log_gauge(&BACKEND_ALLOCATOR_TELEMETRY_INFO, 1.0);
        for (component, value) in [
            ("arena", allocator.arena_bytes),
            ("mmap", allocator.mmap_bytes),
            ("in_use", allocator.in_use_bytes),
            ("free", allocator.free_bytes),
            ("main_arena_top_chunk", allocator.main_arena_top_chunk_bytes),
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

#[cfg(local_backend_jemalloc)]
fn report_allocator_memory() -> anyhow::Result<()> {
    log_gauge_with_labels(
        &BACKEND_ALLOCATOR_SELECTED_INFO,
        1.0,
        vec![StaticMetricLabel::new("allocator", "jemalloc")],
    );
    let sample = jemalloc_memory().and_then(|memory| {
        let configuration = jemalloc_configuration()?;
        Ok((memory, configuration))
    });
    let (allocator, configuration) = match sample {
        Ok(sample) => sample,
        Err(error) => {
            log_gauge(&BACKEND_ALLOCATOR_TELEMETRY_INFO, 0.0);
            return Err(error);
        },
    };
    log_gauge(&BACKEND_ALLOCATOR_TELEMETRY_INFO, 1.0);
    for (component, value) in [
        ("allocated", allocator.allocated_bytes),
        ("active", allocator.active_bytes),
        ("metadata", allocator.metadata_bytes),
        ("resident", allocator.resident_bytes),
        ("mapped", allocator.mapped_bytes),
        ("retained", allocator.retained_bytes),
    ] {
        log_gauge_with_labels(
            &BACKEND_ALLOCATOR_MEMORY_BYTES,
            value as f64,
            vec![StaticMetricLabel::new("component", component)],
        );
    }
    for (component, value) in [
        ("narenas", f64::from(configuration.narenas)),
        ("dirty_decay_ms", configuration.dirty_decay_ms as f64),
        (
            "abort_on_invalid_configuration",
            if configuration.abort_on_invalid_configuration {
                1.0
            } else {
                0.0
            },
        ),
        (
            "background_thread_configured",
            if configuration.background_thread_configured {
                1.0
            } else {
                0.0
            },
        ),
        (
            "background_thread_active",
            if configuration.background_thread_active {
                1.0
            } else {
                0.0
            },
        ),
        (
            "statistics_supported",
            if configuration.statistics_supported {
                1.0
            } else {
                0.0
            },
        ),
        (
            "profiling_supported",
            if configuration.profiling_supported {
                1.0
            } else {
                0.0
            },
        ),
        (
            "profiling_enabled",
            if configuration.profiling_enabled {
                1.0
            } else {
                0.0
            },
        ),
        (
            "profiling_active",
            if configuration.profiling_active {
                1.0
            } else {
                0.0
            },
        ),
    ] {
        log_gauge_with_labels(
            &BACKEND_ALLOCATOR_CONFIGURATION_INFO,
            value,
            vec![StaticMetricLabel::new("component", component)],
        );
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
        log_gauge(&BACKEND_CGROUP_MEMORY_LIMITED_INFO, 0.0);
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
    // Value gauges retain their last sample; availability describes this attempt.
    set_cgroup_component_availability(0.0);
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
    // Value gauges retain their last sample; availability describes this attempt.
    set_cgroup_event_availability(0.0);
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
    set_cgroup_component_availability(value);
    set_cgroup_event_availability(value);
}

fn set_cgroup_component_availability(value: f64) {
    for component in ["anon", "file", "kernel", "shmem", "sock"] {
        log_gauge_with_labels(
            &BACKEND_CGROUP_MEMORY_COMPONENT_AVAILABLE_INFO,
            value,
            vec![StaticMetricLabel::new("component", component)],
        );
    }
}

fn set_cgroup_event_availability(value: f64) {
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

#[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
fn glibc_allocator_memory() -> anyhow::Result<Option<GlibcAllocatorMemory>> {
    // SAFETY: `mallinfo2` has no arguments and returns a value snapshot. glibc
    // marks it unsafe only during allocator initialization or concurrent
    // `mallopt`; initialization is complete and this backend does not call
    // `mallopt`. Its arena, in-use, free, and top-chunk fields cover only the
    // main arena; mmap fields are process-wide.
    let info = unsafe { libc::mallinfo2() };
    let arena_bytes = u64::try_from(info.arena)?;
    let in_use_bytes = u64::try_from(info.uordblks)?;
    let free_bytes = u64::try_from(info.fordblks)?;
    anyhow::ensure!(
        in_use_bytes.checked_add(free_bytes) == Some(arena_bytes),
        "allocator arena accounting does not reconcile"
    );
    let main_arena_top_chunk_bytes = u64::try_from(info.keepcost)?;
    anyhow::ensure!(
        main_arena_top_chunk_bytes <= free_bytes,
        "allocator main-arena top chunk exceeds free arena bytes"
    );
    Ok(Some(GlibcAllocatorMemory {
        arena_bytes,
        mmap_bytes: u64::try_from(info.hblkhd)?,
        in_use_bytes,
        free_bytes,
        main_arena_top_chunk_bytes,
        mmap_regions: u64::try_from(info.hblks)?,
    }))
}

#[cfg(all(
    not(local_backend_jemalloc),
    not(all(target_os = "linux", target_env = "gnu"))
))]
fn glibc_allocator_memory() -> anyhow::Result<Option<GlibcAllocatorMemory>> {
    Ok(None)
}

#[cfg(local_backend_jemalloc)]
fn jemalloc_memory() -> anyhow::Result<JemallocMemory> {
    epoch::advance()?;
    let memory = JemallocMemory {
        allocated_bytes: u64::try_from(stats::allocated::read()?)?,
        active_bytes: u64::try_from(stats::active::read()?)?,
        metadata_bytes: u64::try_from(stats::metadata::read()?)?,
        resident_bytes: u64::try_from(stats::resident::read()?)?,
        mapped_bytes: u64::try_from(stats::mapped::read()?)?,
        retained_bytes: u64::try_from(stats::retained::read()?)?,
    };
    anyhow::ensure!(
        memory.allocated_bytes <= memory.active_bytes,
        "jemalloc allocated bytes exceed active bytes"
    );
    anyhow::ensure!(
        memory.active_bytes <= memory.resident_bytes,
        "jemalloc active bytes exceed resident bytes"
    );
    anyhow::ensure!(
        memory.metadata_bytes <= memory.resident_bytes,
        "jemalloc metadata bytes exceed resident bytes"
    );
    Ok(memory)
}

#[cfg(local_backend_jemalloc)]
fn jemalloc_configuration() -> anyhow::Result<JemallocConfiguration> {
    // SAFETY: jemalloc documents config.stats and config.prof as boolean
    // mallctl values. Profiling controls exist only when support is compiled in.
    let statistics_supported = unsafe { raw::read::<bool>(b"config.stats\0")? };
    let profiling_supported = unsafe { raw::read::<bool>(b"config.prof\0")? };
    let (profiling_enabled, profiling_active) = if profiling_supported {
        // SAFETY: jemalloc documents opt.prof and prof.active as boolean
        // mallctl values.
        unsafe {
            (
                raw::read::<bool>(b"opt.prof\0")?,
                raw::read::<bool>(b"prof.active\0")?,
            )
        }
    } else {
        (false, false)
    };
    let background_thread_active = if crate::JEMALLOC_BACKGROUND_THREAD_SUPPORTED {
        background_thread::read()?
    } else {
        false
    };
    Ok(JemallocConfiguration {
        narenas: opt::narenas::read()?,
        // SAFETY: jemalloc documents opt.dirty_decay_ms as an ssize_t mallctl value.
        dirty_decay_ms: unsafe { raw::read::<libc::ssize_t>(b"opt.dirty_decay_ms\0")? },
        // SAFETY: jemalloc documents opt.abort_conf as a boolean mallctl value.
        abort_on_invalid_configuration: unsafe { raw::read::<bool>(b"opt.abort_conf\0")? },
        background_thread_configured: opt::background_thread::read()?,
        background_thread_active,
        statistics_supported,
        profiling_supported,
        profiling_enabled,
        profiling_active,
    })
}

#[cfg(local_backend_jemalloc)]
fn validate_jemalloc_configuration(
    configuration: &JemallocConfiguration,
    allocator_trim_enabled: bool,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !allocator_trim_enabled,
        "LOCAL_BACKEND_MALLOC_TRIM_ENABLED is incompatible with the jemalloc backend build"
    );
    anyhow::ensure!(
        (1..=128).contains(&configuration.narenas),
        "jemalloc automatic arena limit must be between 1 and 128"
    );
    anyhow::ensure!(
        configuration.abort_on_invalid_configuration,
        "jemalloc invalid-configuration handling must remain fatal for this backend build"
    );
    if crate::JEMALLOC_BACKGROUND_THREAD_SUPPORTED {
        anyhow::ensure!(
            configuration.background_thread_configured && configuration.background_thread_active,
            "jemalloc background threads must remain configured and active for this backend build"
        );
    } else {
        anyhow::ensure!(
            !configuration.background_thread_configured && !configuration.background_thread_active,
            "jemalloc background threads must remain disabled when this target does not support \
             them"
        );
    }
    anyhow::ensure!(
        configuration.dirty_decay_ms >= 0,
        "jemalloc dirty-page purging must remain enabled for this backend build"
    );
    anyhow::ensure!(
        configuration.statistics_supported,
        "jemalloc statistics support is required for this backend build"
    );
    anyhow::ensure!(
        configuration.profiling_supported,
        "jemalloc profiling support is required for this backend build"
    );
    anyhow::ensure!(
        !configuration.profiling_enabled && !configuration.profiling_active,
        "jemalloc profiling must remain disabled and inactive for this backend build"
    );
    Ok(())
}

#[cfg(local_backend_jemalloc)]
fn validate_allocator_configuration(allocator_trim_enabled: bool) -> anyhow::Result<()> {
    let configuration = jemalloc_configuration()?;
    validate_jemalloc_configuration(&configuration, allocator_trim_enabled)
}

#[cfg(all(not(local_backend_jemalloc), target_os = "linux", target_env = "gnu"))]
fn validate_allocator_configuration(_allocator_trim_enabled: bool) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(all(
    not(local_backend_jemalloc),
    not(all(target_os = "linux", target_env = "gnu"))
))]
fn validate_allocator_configuration(allocator_trim_enabled: bool) -> anyhow::Result<()> {
    anyhow::ensure!(
        !allocator_trim_enabled,
        "LOCAL_BACKEND_MALLOC_TRIM_ENABLED requires a GNU libc Linux backend build"
    );
    Ok(())
}

fn effective_cgroup_root() -> anyhow::Result<Option<PathBuf>> {
    let cgroup = match fs::read_to_string(PROC_SELF_CGROUP) {
        Ok(cgroup) => cgroup,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mountinfo = match fs::read_to_string(PROC_SELF_MOUNTINFO) {
        Ok(mountinfo) => mountinfo,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    effective_cgroup_root_from(&cgroup, &mountinfo)
}

fn effective_cgroup_root_from(cgroup: &str, mountinfo: &str) -> anyhow::Result<Option<PathBuf>> {
    let mut process_cgroup = None;
    for line in cgroup.lines() {
        let mut fields = line.splitn(3, ':');
        let hierarchy = fields
            .next()
            .context("cgroup entry is missing its hierarchy")?;
        let controllers = fields
            .next()
            .context("cgroup entry is missing its controller list")?;
        let path = fields.next().context("cgroup entry is missing its path")?;
        if hierarchy == "0" && controllers.is_empty() {
            anyhow::ensure!(
                process_cgroup.is_none(),
                "process has multiple unified cgroup entries"
            );
            let path = PathBuf::from(path);
            anyhow::ensure!(path.is_absolute(), "unified cgroup path is not absolute");
            process_cgroup = Some(path);
        }
    }
    let Some(process_cgroup) = process_cgroup else {
        return Ok(None);
    };

    let mut best_match: Option<(usize, PathBuf)> = None;
    for line in mountinfo.lines() {
        let (mount, filesystem) = line
            .split_once(" - ")
            .context("mountinfo entry is missing its filesystem separator")?;
        let mut filesystem_fields = filesystem.split_whitespace();
        if filesystem_fields.next() != Some("cgroup2") {
            continue;
        }
        let mut mount_fields = mount.split_whitespace();
        for _ in 0..3 {
            mount_fields
                .next()
                .context("cgroup2 mountinfo entry is incomplete")?;
        }
        let mount_root = decode_mountinfo_path(
            mount_fields
                .next()
                .context("cgroup2 mountinfo entry is missing its root")?,
        )?;
        let mount_point = decode_mountinfo_path(
            mount_fields
                .next()
                .context("cgroup2 mountinfo entry is missing its mount point")?,
        )?;
        mount_fields
            .next()
            .context("cgroup2 mountinfo entry is missing its mount options")?;
        anyhow::ensure!(
            mount_root.is_absolute() && mount_point.is_absolute(),
            "cgroup2 mount paths are not absolute"
        );
        let Ok(relative) = process_cgroup.strip_prefix(&mount_root) else {
            continue;
        };
        let specificity = mount_root.components().count();
        if best_match
            .as_ref()
            .is_none_or(|(best_specificity, _)| specificity > *best_specificity)
        {
            best_match = Some((specificity, mount_point.join(relative)));
        }
    }
    best_match
        .map(|(_, root)| root)
        .context("unified process cgroup is not exposed by a cgroup2 mount")
        .map(Some)
}

fn decode_mountinfo_path(value: &str) -> anyhow::Result<PathBuf> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\\' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        anyhow::ensure!(
            index + 3 < bytes.len(),
            "mountinfo path has an incomplete escape"
        );
        let mut octal = 0u8;
        for digit in &bytes[index + 1..=index + 3] {
            anyhow::ensure!(
                (b'0'..=b'7').contains(digit),
                "mountinfo path has an invalid escape"
            );
            octal = octal
                .checked_mul(8)
                .and_then(|value| value.checked_add(*digit - b'0'))
                .context("mountinfo path escape overflow")?;
        }
        decoded.push(octal);
        index += 4;
    }
    Ok(PathBuf::from(OsString::from_vec(decoded)))
}

fn read_cgroup_memory(root: &Path) -> anyhow::Result<Option<CgroupMemory>> {
    let current_path = root.join("memory.current");
    let max_path = root.join("memory.max");
    match (current_path.try_exists()?, max_path.try_exists()?) {
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
    #[cfg(any(
        not(all(target_os = "linux", target_env = "gnu")),
        local_backend_jemalloc
    ))]
    use std::path::Path;
    use std::{
        fs,
        path::PathBuf,
        time::{
            Duration,
            Instant,
        },
    };

    use common::{
        http::ExternalRequestShedding,
        knobs::LOCAL_NODE_EXECUTOR_TOTAL_RSS_BUDGET_BYTES,
        memory_pressure::MemoryPressureSignal,
    };
    use tempfile::TempDir;

    #[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
    use super::allocator_arena_count;
    use super::{
        account_isolate_memory_capacity,
        configured_startup_budget,
        configured_startup_budget_with_isolate_memory_commit_percent,
        effective_cgroup_root_from,
        parse_malloc_info_arena_count,
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
    fn reclamation_entry_waits_for_trim_only_above_the_shedding_boundary() {
        let signal = MemoryPressureSignal::default();
        let shedding = ExternalRequestShedding::new(false);
        let mut controller = CgroupMemoryPressureController {
            external_request_shedding: Some(shedding.clone()),
            shedding_enter_headroom_bytes: 3,
            shedding_exit_headroom_bytes: 5,
            latest_headroom_bytes: 100,
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
                current_bytes: 95,
                max_bytes: Some(100),
            })
            .unwrap();
        assert!(controller.reclamation_active);
        assert!(!shedding.is_active());
        assert!(!signal.is_active());
        controller.publish_reclamation_state(true);
        assert!(!signal.is_active());

        controller
            .update(&CgroupMemory {
                current_bytes: 97,
                max_bytes: Some(100),
            })
            .unwrap();
        assert!(shedding.is_active());
        controller.publish_reclamation_state(true);
        assert!(signal.is_active());
        controller.publish_reclamation_state(false);
        assert!(signal.is_active());

        controller
            .update(&CgroupMemory {
                current_bytes: 92,
                max_bytes: Some(100),
            })
            .unwrap();
        assert!(!controller.reclamation_active);
        assert!(!shedding.is_active());
        assert!(signal.is_active());
        controller.publish_reclamation_state(true);
        assert!(!signal.is_active());
    }

    #[test]
    fn reclamation_entry_does_not_wait_below_shedding_boundary_when_shedding_is_disabled() {
        let signal = MemoryPressureSignal::default();
        let mut controller = CgroupMemoryPressureController {
            external_request_shedding: None,
            shedding_enter_headroom_bytes: 3,
            shedding_exit_headroom_bytes: 5,
            latest_headroom_bytes: 100,
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
                current_bytes: 95,
                max_bytes: Some(100),
            })
            .unwrap();
        controller.publish_reclamation_state(true);
        assert!(!signal.is_active());

        controller
            .update(&CgroupMemory {
                current_bytes: 97,
                max_bytes: Some(100),
            })
            .unwrap();
        controller.publish_reclamation_state(true);
        assert!(signal.is_active());
    }

    #[test]
    fn allocator_trim_eligibility_scan_obeys_the_trim_cooldown() {
        let cooldown = Duration::from_secs(300);
        let mut controller = CgroupMemoryPressureController {
            external_request_shedding: None,
            shedding_enter_headroom_bytes: 3,
            shedding_exit_headroom_bytes: 5,
            latest_headroom_bytes: 3,
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

    #[cfg(all(target_os = "linux", target_env = "gnu", not(local_backend_jemalloc)))]
    #[test]
    fn live_glibc_arena_count_is_bounded_and_nonzero() {
        assert!(allocator_arena_count().unwrap().unwrap() > 0);
    }

    #[test]
    fn malloc_info_parser_counts_only_structural_heap_elements() {
        let xml = r#"
<malloc version="1" note="&lt;heap nr=&quot;9&quot;&gt;">
  <heap nr="0"><sizes><size from="1" to='2'/></sizes></heap>
  <heap nr="1"></heap>
  <total type="rest" count="0" size="0"/>
</malloc>
"#;
        assert_eq!(parse_malloc_info_arena_count(xml).unwrap(), 2);
    }

    #[test]
    fn malloc_info_parser_rejects_malformed_or_incomplete_output() {
        for xml in [
            "",
            "<malloc></malloc>",
            "<malloc><heap nr=\"0\"></malloc>",
            "<malloc><heap nr=\"0\"></heap>",
            "<malloc><heap nr=0/></malloc>",
            "<malloc><heap nr=\"0\" nr=\"1\"/></malloc>",
            "<malloc><heap nr=\"0\"/></malloc><malloc><heap nr=\"1\"/></malloc>",
            "<malloc><heap nr=\"0\0\"/></malloc>",
        ] {
            assert!(parse_malloc_info_arena_count(xml).is_err(), "{xml:?}");
        }
    }

    #[cfg(any(
        not(all(target_os = "linux", target_env = "gnu")),
        local_backend_jemalloc
    ))]
    #[test]
    fn unsupported_allocator_trim_does_not_sample_proc_or_cgroup_files() {
        assert!(matches!(
            super::measure_allocator_trim(Path::new("/missing"), 1).unwrap(),
            super::AllocatorTrimRun::Unsupported
        ));
    }

    #[cfg(local_backend_jemalloc)]
    #[test]
    fn live_jemalloc_configuration_matches_the_backend_contract() {
        // SAFETY: the backend exports this symbol as a non-null pointer to a
        // static NUL-terminated configuration string.
        let malloc_conf = unsafe {
            let malloc_conf = tikv_jemalloc_sys::malloc_conf.unwrap();
            std::ffi::CStr::from_ptr(malloc_conf)
        };
        let expected: &[u8] = if crate::JEMALLOC_BACKGROUND_THREAD_SUPPORTED {
            b"abort_conf:true,background_thread:true,narenas:32,prof:false"
        } else {
            b"abort_conf:true,narenas:32,prof:false"
        };
        assert_eq!(malloc_conf.to_bytes(), expected);
        super::validate_allocator_configuration(false).unwrap();
        assert!(super::allocator_arena_count().unwrap().unwrap() > 0);
        assert!(super::jemalloc_memory().unwrap().allocated_bytes > 0);
    }

    #[cfg(local_backend_jemalloc)]
    #[test]
    fn jemalloc_build_rejects_glibc_trim() {
        let error = super::validate_allocator_configuration(true).unwrap_err();
        assert!(error
            .to_string()
            .contains("LOCAL_BACKEND_MALLOC_TRIM_ENABLED is incompatible"));
    }

    #[cfg(local_backend_jemalloc)]
    #[test]
    fn jemalloc_build_requires_profiling_support() {
        let mut configuration = super::jemalloc_configuration().unwrap();
        configuration.profiling_supported = false;
        let error = super::validate_jemalloc_configuration(&configuration, false).unwrap_err();
        assert_eq!(
            error.to_string(),
            "jemalloc profiling support is required for this backend build"
        );
    }

    #[cfg(all(
        not(local_backend_jemalloc),
        not(all(target_os = "linux", target_env = "gnu"))
    ))]
    #[test]
    fn system_allocator_build_rejects_glibc_trim() {
        let error = super::validate_allocator_configuration(true).unwrap_err();
        assert_eq!(
            error.to_string(),
            "LOCAL_BACKEND_MALLOC_TRIM_ENABLED requires a GNU libc Linux backend build"
        );
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
    fn effective_cgroup_root_handles_private_and_host_cgroup_namespaces() {
        let private = effective_cgroup_root_from(
            "0::/\n",
            "29 23 0:26 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
        )
        .unwrap();
        assert_eq!(private, Some(PathBuf::from("/sys/fs/cgroup")));

        let host = effective_cgroup_root_from(
            "0::/system.slice/backend.service\n",
            "29 23 0:26 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
        )
        .unwrap();
        assert_eq!(
            host,
            Some(PathBuf::from("/sys/fs/cgroup/system.slice/backend.service"))
        );

        let subtree = effective_cgroup_root_from(
            "0::/system.slice/backend.service\n",
            "29 23 0:26 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n30 23 0:26 /system.slice \
             /run/cgroup rw - cgroup2 cgroup rw\n",
        )
        .unwrap();
        assert_eq!(subtree, Some(PathBuf::from("/run/cgroup/backend.service")));
    }

    #[test]
    fn effective_cgroup_root_decodes_mountinfo_paths() {
        let root = effective_cgroup_root_from(
            "0::/tenant/backend\n",
            "29 23 0:26 /tenant /sys/fs/cgroup\\040space rw - cgroup2 cgroup rw\n",
        )
        .unwrap();
        assert_eq!(root, Some(PathBuf::from("/sys/fs/cgroup space/backend")));
    }

    #[test]
    fn effective_cgroup_root_distinguishes_absent_and_unmatched_v2() {
        assert_eq!(
            effective_cgroup_root_from(
                "2:memory:/backend\n",
                "29 23 0:26 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n",
            )
            .unwrap(),
            None
        );

        let error = effective_cgroup_root_from(
            "0::/backend\n",
            "29 23 0:26 /other /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
        )
        .unwrap_err();
        assert!(error.to_string().contains("not exposed"));
    }

    #[test]
    fn effective_cgroup_root_rejects_malformed_unified_entries() {
        for cgroup in ["0::relative\n", "0::/one\n0::/two\n"] {
            assert!(effective_cgroup_root_from(
                cgroup,
                "29 23 0:26 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
            )
            .is_err());
        }
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
    fn startup_budget_reserves_total_local_node_budget() {
        let (budget, _) = configured_startup_budget().unwrap();
        let node = budget
            .components
            .iter()
            .find(|component| component.name == "local_node_rss_budget")
            .unwrap();
        assert_eq!(
            node.bytes,
            u64::try_from(*LOCAL_NODE_EXECUTOR_TOTAL_RSS_BUDGET_BYTES).unwrap()
        );
    }

    #[test]
    fn startup_budget_accounts_only_the_selected_isolate_capacity_fraction() {
        let (full_budget, full_isolate_memory) =
            configured_startup_budget_with_isolate_memory_commit_percent(100).unwrap();
        let (partial_budget, partial_isolate_memory) =
            configured_startup_budget_with_isolate_memory_commit_percent(75).unwrap();

        assert_eq!(
            full_isolate_memory.raw_capacity_bytes,
            partial_isolate_memory.raw_capacity_bytes
        );
        assert_eq!(
            full_isolate_memory.accounted_bytes,
            full_isolate_memory.raw_capacity_bytes
        );
        assert_eq!(partial_isolate_memory.commit_percent, 75);
        assert_eq!(
            partial_isolate_memory.accounted_bytes,
            account_isolate_memory_capacity(partial_isolate_memory.raw_capacity_bytes, 75).unwrap()
        );
        let fixed_budget = full_budget.total_bytes - full_isolate_memory.accounted_bytes;
        assert_eq!(
            partial_budget.total_bytes,
            fixed_budget + partial_isolate_memory.accounted_bytes
        );
    }

    #[test]
    fn isolate_capacity_accounting_rounds_up_and_rejects_invalid_percentages() {
        assert_eq!(account_isolate_memory_capacity(1, 75).unwrap(), 1);
        assert_eq!(account_isolate_memory_capacity(101, 75).unwrap(), 76);
        assert!(account_isolate_memory_capacity(100, 0).is_err());
        assert!(account_isolate_memory_capacity(100, 101).is_err());
    }

    #[test]
    fn finite_cgroup_limit_covers_total_local_node_budget() {
        let (budget, _) = configured_startup_budget().unwrap();
        let root = TempDir::new().unwrap();
        fs::write(root.path().join("memory.current"), "0\n").unwrap();
        fs::write(
            root.path().join("memory.max"),
            format!("{}\n", budget.total_bytes - 1),
        )
        .unwrap();
        assert!(startup_budget_headroom(root.path(), &budget).is_err());
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
