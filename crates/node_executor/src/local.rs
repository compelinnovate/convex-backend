#[cfg(unix)]
use std::env;
#[cfg(unix)]
use std::io::Read as _;
#[cfg(unix)]
use std::os::unix::fs::{
    DirBuilderExt,
    MetadataExt,
    OpenOptionsExt,
    PermissionsExt,
};
use std::{
    collections::{
        BTreeMap,
        VecDeque,
    },
    fs,
    io::Write as _,
    path::{
        Path,
        PathBuf,
    },
    process::{
        ExitStatus,
        Stdio,
    },
    sync::{
        atomic::{
            AtomicBool,
            AtomicU64,
            AtomicUsize,
            Ordering,
        },
        Arc,
        Mutex as StdMutex,
        Weak,
    },
    time::{
        Duration,
        Instant,
        SystemTime,
        UNIX_EPOCH,
    },
};

use anyhow::Context;
use async_trait::async_trait;
use common::{
    execution_start::FunctionExecutionStartGate,
    knobs::{
        LOCAL_NODE_EXECUTOR_MAX_GENERATION_AGE,
        LOCAL_NODE_EXECUTOR_MAX_IMPORTED_SOURCE_PACKAGES,
        LOCAL_NODE_EXECUTOR_MAX_OLD_SPACE_SIZE_MIB,
        LOCAL_NODE_EXECUTOR_MAX_RSS_BYTES,
        LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_GRACE,
        LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_MIN_RSS_BYTES,
    },
    log_lines::LogLine,
    memory_pressure::MemoryPressureSignal,
};
use errors::ErrorMetadata;
use futures_async_stream::try_stream;
use isolate::bundled_js::node_executor_file;
use rand::Rng;
use reqwest::Client;
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value as JsonValue;
use tempfile::{
    Builder as TempFileBuilder,
    TempDir,
};
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::{
    io::{
        AsyncReadExt,
        AsyncWriteExt,
    },
    process::{
        Child,
        Command as TokioCommand,
    },
    sync::{
        mpsc,
        Mutex,
        Notify,
    },
};

use crate::{
    executor::{
        handle_node_executor_stream,
        ExecutorRequest,
        InvokeCompletion,
        InvokeResponse,
        NodeExecutor,
        NodeExecutorStreamPart,
        ARGS_TOO_LARGE_RESPONSE_MESSAGE,
        EXECUTE_TIMEOUT_RESPONSE_JSON,
    },
    metrics::FirstMissDiagnosticOutcome,
};

const NVMRC_VERSION: &str = include_str!("../../../.nvmrc");
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_millis(100);
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_HEALTH_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_PREPARATION_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_INVOKE_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const MAX_NODE_VERSION_OUTPUT_BYTES: usize = 1024;
const MAX_HEALTH_CHECK_ATTEMPTS: u32 = 50;
const NODE_VERSION_CHECK_TIMEOUT: Duration = Duration::from_secs(5);
const PROCESS_PROCFS_READ_TIMEOUT: Duration = Duration::from_secs(1);
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(1);
const WATCHDOG_FAILURE_THRESHOLD: u32 = 5;
const DIAGNOSTIC_PROFILE_DURATION_MS: u64 = 4_000;
const DIAGNOSTIC_FILESYSTEM_TIMEOUT: Duration = Duration::from_secs(2);
const DIAGNOSTIC_PROCESS_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(2);
const DIAGNOSTIC_REPORT_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
const DIAGNOSTIC_TRIGGER_TIMEOUT: Duration = Duration::from_secs(7);
const DIAGNOSTIC_ARTIFACT_POLL_INTERVAL: Duration = Duration::from_millis(50);
#[cfg(unix)]
const DIAGNOSTIC_PROFILE_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
#[cfg(unix)]
const DIAGNOSTIC_PROFILE_CONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(25);
#[cfg(unix)]
const MAX_DIAGNOSTIC_CONTROL_RESPONSE_BYTES: u64 = 64;
#[cfg(unix)]
const MAX_DIAGNOSTIC_PROFILE_BYTES: u64 = 32 * 1024 * 1024;
const MAX_DIAGNOSTIC_REPORT_BYTES: u64 = 32 * 1024 * 1024;
const MAX_DIAGNOSTIC_ACTIVE_REQUESTS: usize = 64;
const MAX_DIAGNOSTIC_REQUEST_IDENTITY_BYTES: usize = 512;
const MAX_DIAGNOSTIC_THREADS: usize = 256;
const MAX_DIAGNOSTIC_ARTIFACTS: usize = 96;
const MAX_DIAGNOSTIC_ARTIFACT_BYTES: u64 = 256 * 1024 * 1024;
const MAX_DIAGNOSTIC_ARTIFACT_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const MIN_DIAGNOSTIC_ARTIFACT_PRUNE_AGE: Duration = Duration::from_secs(30);
const MAX_DIAGNOSTIC_CLOCK_SKEW: Duration = Duration::from_secs(5 * 60);
const LOCAL_NODE_EXECUTOR_DIAGNOSTICS_DIR_ENV: &str = "LOCAL_NODE_EXECUTOR_DIAGNOSTICS_DIR";
const MIB_BYTES: u64 = 1024 * 1024;

pub struct LocalNodeExecutor {
    state: Arc<Mutex<LocalNodeExecutorState>>,
    transition_changed: Arc<Notify>,
    startup_lock: Mutex<()>,
    /// Closes request admission for both topology retirement and backend
    /// shutdown.
    shutting_down: Arc<AtomicBool>,
    /// Ensures backend shutdown still upgrades a topology-retiring executor to
    /// immediate cleanup.
    shutdown_started: Arc<AtomicBool>,
    activity: Arc<ExecutorPoolActivity>,
    config: LocalNodeExecutorConfig,
}

struct ExecutorPoolActivity {
    pool_name: Arc<str>,
    waiting_requests: AtomicUsize,
    active_requests: AtomicUsize,
}

#[derive(Default)]
struct LocalNodeExecutorState {
    inner: Option<Arc<InnerLocalNodeExecutor>>,
    retiring: Option<Arc<InnerLocalNodeExecutor>>,
    hot_transition: Option<HotTransition>,
    replacement_for_generation: Option<u64>,
    next_generation: u64,
    next_transition: u64,
}

#[derive(Clone)]
/// Validated local-Node settings captured before expensive backend startup.
pub struct LocalNodeExecutorConfig {
    pool_name: Arc<str>,
    node_process_timeout: Duration,
    /// Overrides the initial callback retry backoff in the spawned node
    /// process (read by syscalls.ts at module load). Tests zero this so
    /// callbacks retrying against an unreachable backend settle within test
    /// timeouts.
    callback_initial_backoff: Option<Duration>,
    health_check_timeout: Duration,
    watchdog_interval: Duration,
    watchdog_failure_threshold: u32,
    max_old_space_size_mib: usize,
    max_rss_bytes: u64,
    memory_pressure: MemoryPressureSignal,
    memory_pressure_min_rss_bytes: u64,
    memory_pressure_grace: Duration,
    max_generation_age: Duration,
    max_imported_source_packages: u64,
    diagnostics_dir: Option<PathBuf>,
    diagnostic_pruning_in_progress: Arc<AtomicBool>,
    surge_coordinator: Arc<SurgeCoordinator>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SurgePriority {
    Routine,
    Deployment,
}

impl SurgePriority {
    fn as_str(self) -> &'static str {
        match self {
            Self::Routine => "routine",
            Self::Deployment => "deployment",
        }
    }
}

struct SurgeWaiter {
    id: u64,
}

struct SurgeCoordinatorState {
    occupied: Option<SurgeOccupant>,
    next_id: u64,
    deployment: VecDeque<SurgeWaiter>,
    routine: VecDeque<SurgeWaiter>,
}

struct SurgeOccupant {
    id: u64,
    candidate_reclaimable: bool,
    preemption: Arc<SurgePreemption>,
    phase: &'static str,
}

struct SurgePreemption {
    requested: AtomicBool,
    request_sequence: AtomicU64,
    changed: Notify,
}

pub(crate) struct SurgeCoordinator {
    state: StdMutex<SurgeCoordinatorState>,
    changed: Notify,
}

#[derive(Clone)]
pub(crate) struct SurgePermit {
    inner: Arc<SurgePermitInner>,
}

struct SurgePermitInner {
    coordinator: Arc<SurgeCoordinator>,
    id: u64,
    cleanup_required: AtomicBool,
    preemption: Arc<SurgePreemption>,
}

struct SurgeWaitRegistration {
    coordinator: Arc<SurgeCoordinator>,
    priority: SurgePriority,
    id: u64,
    armed: bool,
    started_at: Instant,
}

impl SurgeCoordinator {
    pub(crate) fn new() -> Arc<Self> {
        crate::metrics::set_local_node_surge_phase("unused");
        crate::metrics::set_local_node_surge_queue("routine", 0);
        crate::metrics::set_local_node_surge_queue("deployment", 0);
        Arc::new(Self {
            state: StdMutex::new(SurgeCoordinatorState {
                occupied: None,
                next_id: 0,
                deployment: VecDeque::new(),
                routine: VecDeque::new(),
            }),
            changed: Notify::new(),
        })
    }

    pub(crate) async fn acquire(
        self: &Arc<Self>,
        priority: SurgePriority,
        _pool_name: Arc<str>,
    ) -> SurgePermit {
        let id = {
            let mut state = self
                .state
                .lock()
                .expect("Local Node surge coordinator lock poisoned");
            state.next_id = state
                .next_id
                .checked_add(1)
                .expect("Local Node surge waiter id overflow");
            let id = state.next_id;
            let queue = match priority {
                SurgePriority::Routine => &mut state.routine,
                SurgePriority::Deployment => &mut state.deployment,
            };
            queue.push_back(SurgeWaiter { id });
            Self::publish_queue_metrics(&state);
            id
        };
        let mut registration = SurgeWaitRegistration {
            coordinator: self.clone(),
            priority,
            id,
            armed: true,
            started_at: Instant::now(),
        };
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let acquired = {
                let mut state = self
                    .state
                    .lock()
                    .expect("Local Node surge coordinator lock poisoned");
                let at_front = match priority {
                    SurgePriority::Deployment => state
                        .deployment
                        .front()
                        .is_some_and(|waiter| waiter.id == id),
                    SurgePriority::Routine => {
                        state.deployment.is_empty()
                            && state.routine.front().is_some_and(|waiter| waiter.id == id)
                    },
                };
                if state.occupied.is_none() && at_front {
                    let waiter = match priority {
                        SurgePriority::Routine => state.routine.pop_front(),
                        SurgePriority::Deployment => state.deployment.pop_front(),
                    }
                    .expect("Local Node surge waiter disappeared");
                    assert_eq!(waiter.id, id);
                    let preemption = Arc::new(SurgePreemption {
                        requested: AtomicBool::new(false),
                        request_sequence: AtomicU64::new(0),
                        changed: Notify::new(),
                    });
                    state.occupied = Some(SurgeOccupant {
                        id,
                        candidate_reclaimable: priority == SurgePriority::Routine,
                        preemption: preemption.clone(),
                        phase: "candidate",
                    });
                    Self::publish_queue_metrics(&state);
                    crate::metrics::set_local_node_surge_phase("candidate");
                    Some(preemption)
                } else {
                    None
                }
            };
            if let Some(preemption) = acquired {
                registration.armed = false;
                crate::metrics::log_local_node_surge_wait(
                    priority.as_str(),
                    registration.started_at.elapsed(),
                    "acquired",
                );
                return SurgePermit {
                    inner: Arc::new(SurgePermitInner {
                        coordinator: self.clone(),
                        id,
                        cleanup_required: AtomicBool::new(false),
                        preemption,
                    }),
                };
            }
            notified.await;
        }
    }

    fn publish_queue_metrics(state: &SurgeCoordinatorState) {
        crate::metrics::set_local_node_surge_queue("routine", state.routine.len());
        crate::metrics::set_local_node_surge_queue("deployment", state.deployment.len());
    }

    pub(crate) fn force_preempt_reclaimable(&self) -> Option<&'static str> {
        let state = self
            .state
            .lock()
            .expect("Local Node surge coordinator lock poisoned");
        let Some(occupant) = &state.occupied else {
            return None;
        };
        // A deployment candidate owns its promotion and cannot be stolen. Once
        // cleanup claims that candidate, or it promotes and leaves an old
        // draining generation, a later forced deployment may reclaim it.
        if !occupant.candidate_reclaimable && occupant.phase != "draining" {
            return None;
        }
        occupant.preemption.requested.store(true, Ordering::Release);
        occupant
            .preemption
            .request_sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |sequence| {
                sequence.checked_add(1)
            })
            .expect("Local Node surge preemption request sequence overflow");
        occupant.preemption.changed.notify_waiters();
        Some(occupant.phase)
    }
}

impl SurgePermit {
    fn require_confirmed_cleanup(&self) {
        self.inner.cleanup_required.store(true, Ordering::Release);
    }

    fn allow_forced_candidate_reclamation(&self) {
        let mut state = self
            .inner
            .coordinator
            .state
            .lock()
            .expect("Local Node surge coordinator lock poisoned");
        if let Some(occupant) = state
            .occupied
            .as_mut()
            .filter(|occupant| occupant.id == self.inner.id)
        {
            occupant.candidate_reclaimable = true;
        }
    }

    pub(crate) fn confirm_direct_child_reaped(&self) {
        // State publication can still await the generation mutex after reap.
        // From this point, task cancellation may release global surge capacity
        // because no extra direct child remains.
        self.inner.cleanup_required.store(false, Ordering::Release);
    }

    pub(crate) fn direct_child_cleanup_confirmed(&self) -> bool {
        !self.inner.cleanup_required.load(Ordering::Acquire)
    }

    pub(crate) fn set_phase(&self, phase: &'static str) {
        if phase == "draining" {
            self.require_confirmed_cleanup();
        }
        let mut state = self
            .inner
            .coordinator
            .state
            .lock()
            .expect("Local Node surge coordinator lock poisoned");
        if let Some(occupant) = state
            .occupied
            .as_mut()
            .filter(|occupant| occupant.id == self.inner.id)
        {
            occupant.phase = phase;
            crate::metrics::set_local_node_surge_phase(phase);
        }
    }

    pub(crate) fn release(self) {
        // A deployment cutover keeps one clone across all resident pools while
        // local candidate/drain ownership temporarily holds another. The
        // coordinator is released only when the last clone is gone.
    }

    pub(crate) fn preempted(&self) -> bool {
        self.inner.preemption.requested.load(Ordering::Acquire)
    }

    pub(crate) async fn wait_until_preempted(&self) {
        wait_for_atomic_flag(
            &self.inner.preemption.requested,
            &self.inner.preemption.changed,
        )
        .await;
    }

    pub(crate) async fn wait_for_preemption_request_after(&self, observed: u64) -> u64 {
        wait_for_atomic_advance(
            &self.inner.preemption.request_sequence,
            observed,
            &self.inner.preemption.changed,
        )
        .await
    }
}

impl Drop for SurgePermitInner {
    fn drop(&mut self) {
        if !self.cleanup_required.load(Ordering::Acquire) {
            let mut state = self
                .coordinator
                .state
                .lock()
                .expect("Local Node surge coordinator lock poisoned");
            assert!(
                state
                    .occupied
                    .as_ref()
                    .is_some_and(|occupant| occupant.id == self.id),
                "Local Node surge permit no longer owns the coordinator"
            );
            state.occupied = None;
            crate::metrics::set_local_node_surge_phase("unused");
            drop(state);
            self.coordinator.changed.notify_waiters();
            return;
        }
        // Preserve the occupied slot when ownership ends without confirmed
        // reaping. A later transition must not overlap an unconfirmed child.
        tracing::error!("Local Node surge ownership ended without confirmed direct-child reaping");
    }
}

impl Drop for SurgeWaitRegistration {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut state = self
            .coordinator
            .state
            .lock()
            .expect("Local Node surge coordinator lock poisoned");
        let queue = match self.priority {
            SurgePriority::Routine => &mut state.routine,
            SurgePriority::Deployment => &mut state.deployment,
        };
        if let Some(position) = queue.iter().position(|waiter| waiter.id == self.id) {
            queue.remove(position);
            SurgeCoordinator::publish_queue_metrics(&state);
            crate::metrics::log_local_node_surge_wait(
                self.priority.as_str(),
                self.started_at.elapsed(),
                "canceled",
            );
        }
        drop(state);
        self.coordinator.changed.notify_waiters();
    }
}

async fn wait_for_atomic_flag(flag: &AtomicBool, changed: &Notify) {
    loop {
        let notified = changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if flag.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }
}

async fn wait_for_atomic_advance(value: &AtomicU64, observed: u64, changed: &Notify) -> u64 {
    loop {
        let notified = changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let current = value.load(Ordering::Acquire);
        if current > observed {
            return current;
        }
        notified.await;
    }
}

#[derive(Clone)]
struct PreparationDescriptor {
    source_package: crate::executor::SourcePackage,
}

impl PreparationDescriptor {
    fn is_expired(&self) -> bool {
        self.source_package.download_url_expiration <= Instant::now()
    }

    fn retain_fresher(slot: &mut Option<Self>, incoming: Self) {
        if slot.as_ref().is_none_or(|current| {
            incoming.source_package.download_url_expiration
                > current.source_package.download_url_expiration
        }) {
            *slot = Some(incoming);
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
enum PreparationResponse {
    Success,
    Error,
}

struct CandidateTransition {
    token: u64,
    expected: Arc<InnerLocalNodeExecutor>,
    target_fingerprint: Option<ResidentGenerationFingerprint>,
    descriptor: Option<PreparationDescriptor>,
    startup_started: bool,
    reason: GenerationRetirementReason,
    canceled: Arc<AtomicBool>,
    canceled_changed: Arc<Notify>,
    status: Arc<HotTransitionStatus>,
    cleanup: Arc<HotTransitionCleanupOwner>,
}

struct CandidateStartupCancellation {
    canceled: Arc<AtomicBool>,
    preemption: Arc<SurgePreemption>,
    shutting_down: Arc<AtomicBool>,
    memory_pressure: MemoryPressureSignal,
}

impl CandidateStartupCancellation {
    fn outcome(&self) -> Option<&'static str> {
        if self.shutting_down.load(Ordering::Acquire) {
            Some("shutdown_canceled")
        } else if self.memory_pressure.is_active() {
            Some("pressure_canceled")
        } else if self.canceled.load(Ordering::Acquire)
            || self.preemption.requested.load(Ordering::Acquire)
        {
            Some("stale")
        } else {
            None
        }
    }
}

struct DrainingTransition {
    token: u64,
    old: Arc<InnerLocalNodeExecutor>,
    status: Arc<HotTransitionStatus>,
    cleanup: Arc<HotTransitionCleanupOwner>,
}

enum HotTransition {
    Candidate(CandidateTransition),
    Draining(DrainingTransition),
}

enum DeploymentTransitionDisposition {
    Start,
    WaitForRoutineCancellation,
    Join(Arc<HotTransitionStatus>),
    Conflict,
}

pub(crate) enum DeploymentReplacementOutcome {
    Reused,
    Promoted,
}

struct HotTransitionStatus {
    failed: AtomicBool,
    promoted: AtomicBool,
    cleanup_failed: AtomicBool,
}

enum HotTransitionCleanupPhase {
    Candidate {
        child: Option<HotTransitionCleanupChild>,
        startup_finished: bool,
        termination_started: bool,
    },
    Draining {
        old: Arc<InnerLocalNodeExecutor>,
    },
}

#[derive(Clone)]
enum HotTransitionCleanupChild {
    Startup(Arc<Mutex<ManagedChild>>),
    Candidate(Arc<InnerLocalNodeExecutor>),
}

enum HotTransitionCleanupTarget {
    Candidate(Option<HotTransitionCleanupChild>),
    Draining(Arc<InnerLocalNodeExecutor>),
}

struct HotTransitionCleanupOwner {
    token: u64,
    state: Weak<Mutex<LocalNodeExecutorState>>,
    transition_changed: Arc<Notify>,
    status: Arc<HotTransitionStatus>,
    reason: GenerationRetirementReason,
    pool_name: Arc<str>,
    phase: StdMutex<HotTransitionCleanupPhase>,
    phase_changed: Notify,
    permit: StdMutex<Option<SurgePermit>>,
    outcome: StdMutex<Option<&'static str>>,
    attempt: StdMutex<HotTransitionCleanupAttemptState>,
    confirmed: AtomicBool,
    confirmed_changed: Notify,
}

struct HotTransitionCleanupAttemptState {
    running: bool,
    retry_requested: bool,
}

struct HotTransitionCleanupAttemptGuard {
    owner: Arc<HotTransitionCleanupOwner>,
    armed: bool,
}

struct HotTransitionTaskGuard {
    status: Arc<HotTransitionStatus>,
    transition_changed: Arc<Notify>,
    expected: Arc<InnerLocalNodeExecutor>,
    reason: GenerationRetirementReason,
    cleanup: Arc<HotTransitionCleanupOwner>,
    armed: bool,
}

impl HotTransitionTaskGuard {
    fn new(
        status: Arc<HotTransitionStatus>,
        transition_changed: Arc<Notify>,
        expected: Arc<InnerLocalNodeExecutor>,
        reason: GenerationRetirementReason,
        cleanup: Arc<HotTransitionCleanupOwner>,
    ) -> Self {
        Self {
            status,
            transition_changed,
            expected,
            reason,
            cleanup,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl HotTransitionCleanupAttemptGuard {
    fn new(owner: Arc<HotTransitionCleanupOwner>) -> Self {
        Self { owner, armed: true }
    }

    fn finish(&mut self, succeeded: bool) {
        assert!(
            self.armed,
            "Local Node cleanup attempt guard finished twice"
        );
        self.armed = false;
        self.owner.finish_cleanup_attempt(succeeded);
    }
}

impl Drop for HotTransitionCleanupAttemptGuard {
    fn drop(&mut self) {
        if self.armed {
            // Cancellation and panic must publish a terminal attempt and make
            // any concurrent retry request runnable again.
            self.owner.finish_cleanup_attempt(false);
        }
    }
}

impl HotTransitionCleanupOwner {
    fn new(
        token: u64,
        state: &Arc<Mutex<LocalNodeExecutorState>>,
        transition_changed: Arc<Notify>,
        status: Arc<HotTransitionStatus>,
        reason: GenerationRetirementReason,
        pool_name: Arc<str>,
    ) -> Arc<Self> {
        Arc::new(Self {
            token,
            state: Arc::downgrade(state),
            transition_changed,
            status,
            reason,
            pool_name,
            phase: StdMutex::new(HotTransitionCleanupPhase::Candidate {
                child: None,
                startup_finished: false,
                termination_started: false,
            }),
            phase_changed: Notify::new(),
            permit: StdMutex::new(None),
            outcome: StdMutex::new(None),
            attempt: StdMutex::new(HotTransitionCleanupAttemptState {
                running: false,
                retry_requested: false,
            }),
            confirmed: AtomicBool::new(false),
            confirmed_changed: Notify::new(),
        })
    }

    fn retain_permit(
        self: &Arc<Self>,
        permit: &SurgePermit,
        memory_pressure: MemoryPressureSignal,
    ) {
        permit.require_confirmed_cleanup();
        let mut owned = self
            .permit
            .lock()
            .expect("Local Node hot-transition cleanup permit lock poisoned");
        assert!(
            owned.is_none(),
            "Local Node hot-transition retained two permits"
        );
        *owned = Some(permit.clone());
        drop(owned);
        self.publish_candidate_cleanup_reclaimable();

        // The watcher is the final retry owner if the executor itself is
        // dropped after a failed cleanup attempt.
        let owner = self.clone();
        let preemption = permit.inner.preemption.clone();
        tokio::spawn(async move {
            let mut observed_request = 0;
            let mut pressure = memory_pressure.subscribe();
            let mut pressure_publication_pending = true;
            loop {
                // Watch can coalesce clear and re-entry before this task is
                // polled. Any unseen active publication is therefore a new
                // bounded retry signal, even when the last observed value was
                // also active.
                let pressure_entered = if pressure_publication_pending {
                    pressure_publication_pending = false;
                    *pressure.borrow_and_update()
                } else {
                    false
                };
                let outcome = if pressure_entered {
                    "pressure_canceled"
                } else {
                    observed_request = tokio::select! {
                        request = wait_for_atomic_advance(
                            &preemption.request_sequence,
                            observed_request,
                            &preemption.changed,
                        ) => request,
                        changed = pressure.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            pressure_publication_pending = true;
                            continue;
                        },
                        () = owner.wait_until_confirmed() => return,
                    };
                    "stale"
                };
                if owner.cleanup(outcome).await.is_err() && owner.cleanup(outcome).await.is_err() {
                    // Keep the watcher with the exact owner. A later force or
                    // pressure-entry request retries without a hot loop after
                    // repeated operating-system failures.
                    continue;
                }
                return;
            }
        });
    }

    fn attach_startup_child(&self, child: Arc<Mutex<ManagedChild>>) {
        let mut phase = self
            .phase
            .lock()
            .expect("Local Node hot-transition cleanup phase lock poisoned");
        match &mut *phase {
            HotTransitionCleanupPhase::Candidate {
                child: child_slot, ..
            } => {
                assert!(
                    child_slot.is_none(),
                    "Local Node candidate child was attached twice"
                );
                *child_slot = Some(HotTransitionCleanupChild::Startup(child));
            },
            HotTransitionCleanupPhase::Draining { .. } => {
                unreachable!("Local Node candidate child attached after promotion or cleanup")
            },
        }
        drop(phase);
        self.phase_changed.notify_waiters();
    }

    fn attach_candidate(&self, candidate: Arc<InnerLocalNodeExecutor>) {
        let mut phase = self
            .phase
            .lock()
            .expect("Local Node hot-transition cleanup phase lock poisoned");
        match &mut *phase {
            HotTransitionCleanupPhase::Candidate {
                child,
                startup_finished,
                ..
            } => {
                // Cleanup may already hold a snapshot of the startup variant.
                // Both variants use this same ManagedChild, and a claimed
                // termination prevents promotion, so replacing the view cannot
                // change or strand the cleanup target.
                assert!(
                    matches!(child, Some(HotTransitionCleanupChild::Startup(_))),
                    "Ready Local Node candidate has no startup child owner"
                );
                *child = Some(HotTransitionCleanupChild::Candidate(candidate));
                *startup_finished = true;
            },
            HotTransitionCleanupPhase::Draining { .. } => {
                unreachable!("Local Node candidate became ready after promotion or cleanup")
            },
        }
        drop(phase);
        self.phase_changed.notify_waiters();
    }

    fn finish_candidate_startup(&self) {
        let mut phase = self
            .phase
            .lock()
            .expect("Local Node hot-transition cleanup phase lock poisoned");
        if let HotTransitionCleanupPhase::Candidate {
            startup_finished, ..
        } = &mut *phase
        {
            *startup_finished = true;
        }
        drop(phase);
        self.phase_changed.notify_waiters();
    }

    fn promote_to_draining(&self, old: Arc<InnerLocalNodeExecutor>) -> bool {
        let mut phase = self
            .phase
            .lock()
            .expect("Local Node hot-transition cleanup phase lock poisoned");
        match &*phase {
            HotTransitionCleanupPhase::Candidate {
                startup_finished: true,
                termination_started: false,
                child: Some(HotTransitionCleanupChild::Candidate(_)),
            } => {
                *phase = HotTransitionCleanupPhase::Draining { old };
                true
            },
            HotTransitionCleanupPhase::Candidate { .. }
            | HotTransitionCleanupPhase::Draining { .. } => false,
        }
    }

    fn request_cleanup(self: &Arc<Self>, outcome: &'static str) {
        self.claim_cleanup(outcome);
        let should_start = {
            let mut attempt = self
                .attempt
                .lock()
                .expect("Local Node hot-transition cleanup attempt lock poisoned");
            if attempt.running {
                attempt.retry_requested = true;
                false
            } else {
                attempt.running = true;
                attempt.retry_requested = false;
                true
            }
        };
        if !should_start {
            return;
        }
        self.clear_cleanup_failure();
        self.spawn_cleanup_attempt();
    }

    fn clear_cleanup_failure(&self) {
        self.status.cleanup_failed.store(false, Ordering::Release);
        let old = {
            let phase = self
                .phase
                .lock()
                .expect("Local Node hot-transition cleanup phase lock poisoned");
            match &*phase {
                HotTransitionCleanupPhase::Candidate { .. } => None,
                HotTransitionCleanupPhase::Draining { old, .. } => Some(old.clone()),
            }
        };
        if let Some(old) = old {
            old.retirement_failed.store(false, Ordering::Release);
        }
    }

    fn spawn_cleanup_attempt(self: &Arc<Self>) {
        let owner = self.clone();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.finish_cleanup_attempt(false);
            return;
        };
        // Create the guard before spawning. If runtime shutdown drops the task
        // before its first poll, dropping the future must still end the
        // published attempt and wake retry owners.
        let mut guard = HotTransitionCleanupAttemptGuard::new(owner.clone());
        runtime.spawn(async move {
            let succeeded = owner.run_cleanup_attempt().await.is_ok();
            guard.finish(succeeded);
        });
    }

    fn finish_cleanup_attempt(self: &Arc<Self>, succeeded: bool) {
        let confirmed = self.confirmed.load(Ordering::Acquire);
        if !succeeded && !confirmed {
            self.publish_cleanup_failure();
        }
        let retry = {
            let mut attempt = self
                .attempt
                .lock()
                .expect("Local Node hot-transition cleanup attempt lock poisoned");
            assert!(attempt.running, "Local Node cleanup attempt ended twice");
            let retry = !succeeded && !confirmed && attempt.retry_requested;
            attempt.running = retry;
            attempt.retry_requested = false;
            retry
        };
        if retry {
            self.clear_cleanup_failure();
            self.spawn_cleanup_attempt();
        }
        self.transition_changed.notify_waiters();
    }

    async fn cleanup(self: &Arc<Self>, outcome: &'static str) -> anyhow::Result<()> {
        self.request_cleanup(outcome);
        loop {
            let changed = self.transition_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.confirmed.load(Ordering::Acquire) {
                return Ok(());
            }
            let attempt_running = self
                .attempt
                .lock()
                .expect("Local Node hot-transition cleanup attempt lock poisoned")
                .running;
            if self.status.cleanup_failed.load(Ordering::Acquire) && !attempt_running {
                anyhow::bail!("Local Node hot-transition cleanup failed");
            }
            changed.await;
        }
    }

    fn claim_cleanup(&self, outcome: &'static str) {
        let mut stored_outcome = self
            .outcome
            .lock()
            .expect("Local Node hot-transition cleanup outcome lock poisoned");
        if stored_outcome.is_none() {
            *stored_outcome = Some(outcome);
        }
        drop(stored_outcome);

        let mut phase = self
            .phase
            .lock()
            .expect("Local Node hot-transition cleanup phase lock poisoned");
        match &mut *phase {
            HotTransitionCleanupPhase::Candidate {
                termination_started,
                ..
            } => *termination_started = true,
            HotTransitionCleanupPhase::Draining { .. } => {},
        }
        drop(phase);
        self.publish_candidate_cleanup_reclaimable();
    }

    fn publish_candidate_cleanup_reclaimable(&self) {
        let cleanup_claimed = matches!(
            &*self
                .phase
                .lock()
                .expect("Local Node hot-transition cleanup phase lock poisoned"),
            HotTransitionCleanupPhase::Candidate {
                termination_started: true,
                ..
            }
        );
        if !cleanup_claimed {
            return;
        }
        let permit = self
            .permit
            .lock()
            .expect("Local Node hot-transition cleanup permit lock poisoned")
            .clone();
        if let Some(permit) = permit {
            permit.allow_forced_candidate_reclamation();
        }
    }

    async fn cleanup_target(&self) -> HotTransitionCleanupTarget {
        loop {
            let changed = self.phase_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let target = {
                let phase = self
                    .phase
                    .lock()
                    .expect("Local Node hot-transition cleanup phase lock poisoned");
                match &*phase {
                    HotTransitionCleanupPhase::Candidate {
                        child,
                        startup_finished,
                        ..
                    } if child.is_some() || *startup_finished => {
                        Some(HotTransitionCleanupTarget::Candidate(child.clone()))
                    },
                    HotTransitionCleanupPhase::Candidate { .. } => None,
                    HotTransitionCleanupPhase::Draining { old, .. } => {
                        Some(HotTransitionCleanupTarget::Draining(old.clone()))
                    },
                }
            };
            if let Some(target) = target {
                return target;
            }
            changed.await;
        }
    }

    async fn run_cleanup_attempt(self: &Arc<Self>) -> anyhow::Result<()> {
        if self.confirmed.load(Ordering::Acquire) {
            return Ok(());
        }
        let target = self.cleanup_target().await;
        let draining = match &target {
            HotTransitionCleanupTarget::Candidate(Some(HotTransitionCleanupChild::Startup(
                child,
            ))) => {
                child.lock().await.terminate_if_needed().await?;
                None
            },
            HotTransitionCleanupTarget::Candidate(Some(HotTransitionCleanupChild::Candidate(
                candidate,
            ))) => {
                candidate.terminate_for_hot_cleanup().await?;
                None
            },
            HotTransitionCleanupTarget::Candidate(None) => None,
            HotTransitionCleanupTarget::Draining(old) => {
                let observation = old.terminate_for_hot_cleanup().await?;
                Some((old.clone(), observation))
            },
        };

        // A dropped executor has no transition slot left to clear. The exact
        // cleanup owner must still publish confirmed reaping and release its
        // permit instead of turning owner loss into a permanent surge fence.
        let state_owner = self.state.upgrade();
        let mut state = match &state_owner {
            Some(state) => Some(state.lock().await),
            None => None,
        };
        if let Some(state) = &state {
            let exact_transition = match (&state.hot_transition, &target) {
                (
                    Some(HotTransition::Candidate(candidate)),
                    HotTransitionCleanupTarget::Candidate(_),
                ) => candidate.token == self.token && Arc::ptr_eq(&candidate.cleanup, self),
                (
                    Some(HotTransition::Draining(active)),
                    HotTransitionCleanupTarget::Draining(old),
                ) => {
                    active.token == self.token
                        && Arc::ptr_eq(&active.old, old)
                        && Arc::ptr_eq(&active.cleanup, self)
                },
                (Some(HotTransition::Candidate(_)), HotTransitionCleanupTarget::Draining(_))
                | (Some(HotTransition::Draining(_)), HotTransitionCleanupTarget::Candidate(_))
                | (None, HotTransitionCleanupTarget::Candidate(_))
                | (None, HotTransitionCleanupTarget::Draining(_)) => false,
            };
            anyhow::ensure!(
                exact_transition,
                "Local Node hot-transition cleanup lost exact state ownership"
            );
        }

        let mut retired = None;
        match draining {
            Some((old, observation)) => {
                if let Some(observation) = observation {
                    crate::metrics::log_local_node_child_termination(
                        &old.pool_name,
                        self.reason.as_str(),
                        observation.state_before,
                        observation.supervisor_kill_requested,
                        observation.exit_class,
                    );
                }
                crate::metrics::set_local_node_generation_draining(&self.pool_name, false);
                old.retired.store(true, Ordering::Release);
                retired = Some(old);
            },
            None => {
                let outcome = self
                    .outcome
                    .lock()
                    .expect("Local Node hot-transition cleanup outcome lock poisoned")
                    .unwrap_or("task_failed");
                self.status.failed.store(true, Ordering::Release);
                crate::metrics::set_local_node_candidate_present(&self.pool_name, false);
                crate::metrics::log_local_node_replacement_outcome(&self.pool_name, outcome);
                if outcome == "task_failed"
                    && matches!(
                        self.reason,
                        GenerationRetirementReason::FingerprintChange
                            | GenerationRetirementReason::TopologyChange
                    )
                {
                    crate::metrics::log_local_node_fingerprint_transition(
                        &self.pool_name,
                        crate::metrics::FingerprintTransitionOutcome::RetirementFailed,
                    );
                }
            },
        }
        self.status.cleanup_failed.store(false, Ordering::Release);
        let permit = self
            .permit
            .lock()
            .expect("Local Node hot-transition cleanup permit lock poisoned")
            .clone();
        // The retained clone prevents coordinator release while exact state is
        // still present. Mark the already-completed reap before clearing and
        // notifying state so cancellation cannot publish a false cleanup need
        // after the child is gone.
        if let Some(permit) = &permit {
            permit.confirm_direct_child_reaped();
        }
        if let Some(state) = &mut state {
            state.hot_transition = None;
        }
        drop(state);
        if let Some(retired) = retired {
            tracing::info!(
                pool_name = %retired.pool_name,
                generation = retired.generation,
                "Completed draining old local Node executor generation"
            );
            retired.retired_notify.notify_waiters();
        }
        self.transition_changed.notify_waiters();

        self.finish_confirmed_cleanup();
        let retained_permit = self
            .permit
            .lock()
            .expect("Local Node hot-transition cleanup permit lock poisoned")
            .take();
        if let Some(permit) = retained_permit {
            permit.release();
        }
        Ok(())
    }

    fn publish_cleanup_failure(&self) {
        self.status.cleanup_failed.store(true, Ordering::Release);
        let old = {
            let phase = self
                .phase
                .lock()
                .expect("Local Node hot-transition cleanup phase lock poisoned");
            match &*phase {
                HotTransitionCleanupPhase::Candidate { .. } => None,
                HotTransitionCleanupPhase::Draining { old, .. } => Some(old.clone()),
            }
        };
        if let Some(old) = old {
            old.mark_retirement_failed();
        }
        tracing::error!(
            pool_name = %self.pool_name,
            reason = self.reason.as_str(),
            "Failed to terminate and reap a hot-transition local Node executor child"
        );
    }

    fn finish_confirmed_cleanup(&self) {
        self.confirmed.store(true, Ordering::Release);
        self.confirmed_changed.notify_waiters();
        self.transition_changed.notify_waiters();
    }

    async fn wait_until_confirmed(&self) {
        loop {
            let changed = self.confirmed_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.confirmed.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

impl Drop for HotTransitionTaskGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let promoted = self.status.promoted.load(Ordering::Acquire);
        // Keep the exact child and surge lease in transition state. The
        // runtime-owned cleanup attempt survives cancellation of this task.
        self.cleanup.finish_candidate_startup();
        self.cleanup.request_cleanup("task_failed");
        tracing::error!(
            pool_name = %self.expected.pool_name,
            generation = self.expected.generation,
            reason = self.reason.as_str(),
            promoted,
            "Local Node executor hot-replacement task ended without publishing completion"
        );
        self.transition_changed.notify_waiters();
    }
}

struct ManagedChild {
    // Rust owns only the direct server child. Descendant containment has no
    // completion acknowledgment at this boundary.
    generation: u64,
    pool_name: Arc<str>,
    child: Option<Child>,
    source_dir: Option<TempDir>,
}

#[derive(Debug)]
struct UnconfirmedStartupChildCleanup;

impl std::fmt::Display for UnconfirmedStartupChildCleanup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Local Node startup child cleanup was not confirmed")
    }
}

impl std::error::Error for UnconfirmedStartupChildCleanup {}

#[derive(Debug)]
struct CandidateStartupCanceled {
    outcome: &'static str,
}

impl std::fmt::Display for CandidateStartupCanceled {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Local Node candidate startup was canceled")
    }
}

impl std::error::Error for CandidateStartupCanceled {}

#[derive(Debug)]
struct CandidateStartupHealthFailed;

impl std::fmt::Display for CandidateStartupHealthFailed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Node executor server failed to start and become healthy")
    }
}

impl std::error::Error for CandidateStartupHealthFailed {}

#[derive(Debug)]
struct CandidatePreparationTimedOut;

impl std::fmt::Display for CandidatePreparationTimedOut {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Local Node executor package preparation timed out")
    }
}

impl std::error::Error for CandidatePreparationTimedOut {}

struct ReapingTempDir {
    generation: u64,
    pool_name: Arc<str>,
    source_dir: Option<TempDir>,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ResidentGenerationFingerprint {
    pub(crate) source_package_id: model::source_packages::types::SourcePackageId,
    pub(crate) environment_sha256: common::sha256::Sha256Digest,
    pub(crate) topology_version: common::types::Timestamp,
}

impl ResidentGenerationFingerprint {
    fn same_package_and_environment(&self, other: &Self) -> bool {
        self.source_package_id == other.source_package_id
            && self.environment_sha256 == other.environment_sha256
    }
}

fn same_resident_package_and_environment(
    current: Option<&ResidentGenerationFingerprint>,
    incoming: Option<&ResidentGenerationFingerprint>,
) -> bool {
    match (current, incoming) {
        (Some(current), Some(incoming)) => current.same_package_and_environment(incoming),
        (None, None) => true,
        _ => false,
    }
}

struct InnerLocalNodeExecutor {
    generation: u64,
    pool_name: Arc<str>,
    resident_fingerprint: Option<ResidentGenerationFingerprint>,
    activity: Arc<ExecutorPoolActivity>,
    pid: u32,
    started_at: Instant,
    runtime_stats_supported: bool,
    active_requests: AtomicUsize,
    retirement_requested: AtomicBool,
    idle: Notify,
    /// Published only when the child can no longer honor an already admitted
    /// request. Graceful generation drain deliberately leaves this false.
    execution_unavailable: AtomicBool,
    execution_unavailable_notify: Notify,
    terminate_draining: AtomicBool,
    terminate_draining_notify: Notify,
    retired: AtomicBool,
    retirement_failed: AtomicBool,
    retired_notify: Notify,
    #[cfg(test)]
    termination_failures_remaining: AtomicUsize,
    retained_source_packages: AtomicU64,
    retained_external_packages: AtomicU64,
    imported_source_packages: AtomicU64,
    registered_stack_roots: AtomicU64,
    first_miss_diagnostics_started: AtomicBool,
    next_active_request_id: AtomicU64,
    active_request_diagnostics: StdMutex<BTreeMap<u64, ActiveRequestDiagnostic>>,
    preparation_descriptor: StdMutex<Option<PreparationDescriptor>>,
    diagnostic_paths: Option<NodeDiagnosticPaths>,
    // Initiate kill and reaping before removing the tempdir if explicit
    // termination cannot complete or startup is canceled.
    server_handle: Arc<Mutex<ManagedChild>>,
    client: reqwest::Client,
}

struct RetirementTaskGuard {
    expected: Arc<InnerLocalNodeExecutor>,
    armed: bool,
}

impl RetirementTaskGuard {
    fn new(expected: Arc<InnerLocalNodeExecutor>) -> Self {
        Self {
            expected,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RetirementTaskGuard {
    fn drop(&mut self) {
        // The join owner can be canceled while this runtime-owned task remains
        // detached. Publish a terminal result if the task itself is then
        // aborted or panics, so shutdown and replacement waiters cannot hang.
        if self.armed && !self.expected.retired.load(Ordering::Acquire) {
            self.expected.mark_retirement_failed();
        }
    }
}

#[derive(Clone)]
struct NodeDiagnosticPaths {
    control_path: PathBuf,
    first_miss_path: PathBuf,
    profile_source_path: PathBuf,
    profile_path: PathBuf,
    report: NodeDiagnosticReportPaths,
}

#[derive(Clone)]
struct NodeDiagnosticReportPaths {
    source_path: PathBuf,
    destination_path: PathBuf,
    filename: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DiagnosticFileIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

#[derive(Clone, Copy)]
enum ExpectedDiagnosticSource {
    AnyPrivateRegularFile,
    Exact(DiagnosticFileIdentity),
}

struct DiagnosticArtifactPruningClaim {
    in_progress: Arc<AtomicBool>,
}

#[derive(Clone)]
struct RequestDiagnosticMetadata {
    request_kind: &'static str,
    module_path: Option<String>,
    function_name: Option<String>,
}

struct ActiveRequestDiagnostic {
    metadata: RequestDiagnosticMetadata,
    started_at: Instant,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ActiveRequestDiagnosticSnapshot {
    request_kind: &'static str,
    module_path: Option<String>,
    function_name: Option<String>,
    elapsed_ms: u64,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessStatSnapshot {
    state: char,
    user_ticks: u64,
    system_ticks: u64,
    thread_count: u64,
    start_time_ticks: u64,
}

#[derive(Clone)]
struct ProcessStatBaseline {
    sequence: u64,
    sampled_at: Instant,
    process: Option<ProcessStatSnapshot>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessCpuDelta {
    user_ticks: u64,
    system_ticks: u64,
    interval_ms: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadStatSnapshot {
    tid: u32,
    state: char,
    user_ticks: u64,
    system_ticks: u64,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum DiagnosticSnapshotOutcome {
    Success,
    Unsupported,
    Failure,
    Timeout,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FirstMissDiagnosticArtifact {
    schema_version: u32,
    pool_name: String,
    captured_at_unix_ms: u128,
    generation: u64,
    pid: u32,
    generation_age_ms: u64,
    active_request_count: usize,
    active_requests_truncated: bool,
    active_requests: Vec<ActiveRequestDiagnosticSnapshot>,
    rss_bytes: Option<u64>,
    old_space_limit_bytes: u64,
    rss_retirement_threshold_bytes: u64,
    generation_age_retirement_threshold_ms: u64,
    imported_source_packages: u64,
    imported_source_package_retirement_threshold: u64,
    process_stat_outcome: DiagnosticSnapshotOutcome,
    process: Option<ProcessStatSnapshot>,
    process_cpu_delta: Option<ProcessCpuDelta>,
    thread_stat_outcome: DiagnosticSnapshotOutcome,
    threads_truncated: bool,
    threads: Vec<ThreadStatSnapshot>,
}

impl Drop for DiagnosticArtifactPruningClaim {
    fn drop(&mut self) {
        self.in_progress.store(false, Ordering::Release);
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NodeExecutorHealth {
    status: String,
    #[serde(default, deserialize_with = "deserialize_present_runtime_stats")]
    package_cache: Option<NodePackageCacheStats>,
    #[serde(default, deserialize_with = "deserialize_present_runtime_stats")]
    stack_trace: Option<NodeStackTraceStats>,
}

fn deserialize_present_runtime_stats<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NodePackageCacheStats {
    imported_source_packages: u64,
    retained_source_packages: u64,
    retained_source_bytes: u64,
    active_source_owners: u64,
    retained_external_packages: u64,
    retained_external_bytes: u64,
    source_hits: u64,
    source_publishes: u64,
    source_retirements: u64,
    source_failed_publications: u64,
    external_hits: u64,
    external_publishes: u64,
    external_retirements: u64,
    external_failed_publications: u64,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NodeStackTraceStats {
    registered_roots: u64,
    invocations: u64,
    frames_processed: u64,
    duration_ms: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GenerationRetirementReason {
    RequestTimeout,
    ResponseStreamTimeout,
    ConnectionError,
    ProcessExiting,
    HealthCheckFailed,
    RssLimit,
    CgroupPressure,
    AgeLimit,
    PackageLimit,
    FingerprintChange,
    TopologyChange,
    ExplicitShutdown,
}

impl GenerationRetirementReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::RequestTimeout => "request_timeout",
            Self::ResponseStreamTimeout => "response_stream_timeout",
            Self::ConnectionError => "connection_error",
            Self::ProcessExiting => "process_exiting",
            Self::HealthCheckFailed => "health_check_failed",
            Self::RssLimit => "rss_limit",
            Self::CgroupPressure => "cgroup_pressure",
            Self::AgeLimit => "age_limit",
            Self::PackageLimit => "package_limit",
            Self::FingerprintChange => "fingerprint_change",
            Self::TopologyChange => "topology_change",
            Self::ExplicitShutdown => "explicit_shutdown",
        }
    }
}

#[derive(Clone, Copy)]
struct GenerationRetirementDiagnostics {
    reason: GenerationRetirementReason,
    request_kind: &'static str,
    phase: &'static str,
    transport_error_kind: &'static str,
}

impl GenerationRetirementDiagnostics {
    fn request(
        reason: GenerationRetirementReason,
        request_kind: &'static str,
        phase: &'static str,
        transport_error_kind: &'static str,
    ) -> Self {
        Self {
            reason,
            request_kind,
            phase,
            transport_error_kind,
        }
    }

    fn watchdog() -> Self {
        Self {
            reason: GenerationRetirementReason::HealthCheckFailed,
            request_kind: "not_applicable",
            phase: "health_check",
            transport_error_kind: "not_applicable",
        }
    }

    fn shutdown() -> Self {
        Self {
            reason: GenerationRetirementReason::ExplicitShutdown,
            request_kind: "not_applicable",
            phase: "shutdown",
            transport_error_kind: "not_applicable",
        }
    }

    fn topology_change() -> Self {
        Self {
            reason: GenerationRetirementReason::TopologyChange,
            request_kind: "not_applicable",
            phase: "topology_reconciliation",
            transport_error_kind: "not_applicable",
        }
    }

    fn proactive(reason: GenerationRetirementReason) -> Self {
        assert!(matches!(
            reason,
            GenerationRetirementReason::RssLimit
                | GenerationRetirementReason::CgroupPressure
                | GenerationRetirementReason::AgeLimit
                | GenerationRetirementReason::PackageLimit
        ));
        Self {
            reason,
            request_kind: "not_applicable",
            phase: "watchdog",
            transport_error_kind: "not_applicable",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ChildTerminationObservation {
    state_before: &'static str,
    supervisor_kill_requested: bool,
    exit_class: &'static str,
}

fn classify_reqwest_transport_error(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        return "timeout";
    }

    let mut source = std::error::Error::source(error);
    let mut io_error_kind = None;
    while let Some(candidate) = source {
        if let Some(io_error) = candidate.downcast_ref::<std::io::Error>() {
            let candidate_kind = classify_io_error_kind(io_error.kind());
            if candidate_kind != "other_io" {
                return candidate_kind;
            }
            io_error_kind = Some(candidate_kind);
        }
        source = candidate.source();
    }
    if let Some(io_error_kind) = io_error_kind {
        io_error_kind
    } else if error.is_connect() {
        "connect"
    } else if error.is_body() {
        "body"
    } else if error.is_request() {
        "request"
    } else {
        "other"
    }
}

fn classify_io_error_kind(error_kind: std::io::ErrorKind) -> &'static str {
    match error_kind {
        std::io::ErrorKind::ConnectionRefused => "connection_refused",
        std::io::ErrorKind::ConnectionReset => "connection_reset",
        std::io::ErrorKind::ConnectionAborted => "connection_aborted",
        std::io::ErrorKind::NotConnected => "not_connected",
        std::io::ErrorKind::BrokenPipe => "broken_pipe",
        std::io::ErrorKind::UnexpectedEof => "unexpected_eof",
        std::io::ErrorKind::TimedOut => "timeout",
        _ => "other_io",
    }
}

fn proactive_retirement_reason(
    config: &LocalNodeExecutorConfig,
    age: Duration,
    rss_bytes: Option<u64>,
    imported_source_packages: u64,
    memory_pressure_active_for: Option<Duration>,
) -> Option<GenerationRetirementReason> {
    if let Some(active_for) = memory_pressure_active_for {
        return (active_for >= config.memory_pressure_grace
            && rss_bytes.is_some_and(|rss| rss >= config.memory_pressure_min_rss_bytes))
        .then_some(GenerationRetirementReason::CgroupPressure);
    }
    if rss_bytes.is_some_and(|rss| rss >= config.max_rss_bytes) {
        Some(GenerationRetirementReason::RssLimit)
    } else if imported_source_packages >= config.max_imported_source_packages {
        Some(GenerationRetirementReason::PackageLimit)
    } else if age >= config.max_generation_age {
        Some(GenerationRetirementReason::AgeLimit)
    } else {
        None
    }
}

#[derive(Clone, Copy, Debug)]
struct MemoryPressureObservation {
    active_since: Option<Instant>,
}

impl MemoryPressureObservation {
    fn new(active: bool, observed_at: Instant) -> Self {
        Self {
            active_since: active.then_some(observed_at),
        }
    }

    fn observe_publication(&mut self, active: bool, observed_at: Instant) {
        // The controller publishes only state changes, but a watch receiver can
        // coalesce false -> true before it is polled. Restarting the grace on
        // every active publication preserves continuous-pressure semantics in
        // that case and remains conservative for a redundant publication.
        self.active_since = active.then_some(observed_at);
    }

    fn is_active(self) -> bool {
        self.active_since.is_some()
    }

    fn active_for(self, observed_at: Instant) -> Option<Duration> {
        self.active_since
            .map(|active_since| observed_at.duration_since(active_since))
    }
}

fn bounded_request_identity(value: &str) -> String {
    let mut end = value.len().min(MAX_DIAGNOSTIC_REQUEST_IDENTITY_BYTES);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

fn request_diagnostic_metadata(request: &ExecutorRequest) -> RequestDiagnosticMetadata {
    match request {
        ExecutorRequest::Execute { request, .. } => {
            let path = request.path_and_args.path();
            RequestDiagnosticMetadata {
                request_kind: "execute",
                module_path: Some(bounded_request_identity(path.udf_path.module().as_str())),
                function_name: Some(bounded_request_identity(path.udf_path.function_name())),
            }
        },
        ExecutorRequest::Analyze(_) => RequestDiagnosticMetadata {
            request_kind: "analyze",
            module_path: None,
            function_name: None,
        },
        ExecutorRequest::BuildDeps(_) => RequestDiagnosticMetadata {
            request_kind: "build_deps",
            module_path: None,
            function_name: None,
        },
    }
}

#[cfg(target_os = "linux")]
fn parse_process_rss(status: &str) -> anyhow::Result<u64> {
    let mut rss = None;
    for line in status.lines() {
        let Some(value) = line.strip_prefix("VmRSS:") else {
            continue;
        };
        anyhow::ensure!(
            rss.is_none(),
            "Node process status contains duplicate VmRSS"
        );
        let mut fields = value.split_whitespace();
        let kib: u64 = fields
            .next()
            .context("Node process VmRSS is missing a value")?
            .parse()
            .context("Node process VmRSS is invalid")?;
        anyhow::ensure!(
            fields.next() == Some("kB") && fields.next().is_none(),
            "Node process VmRSS has an invalid unit"
        );
        rss = Some(
            kib.checked_mul(1024)
                .context("Node process RSS byte count overflow")?,
        );
    }
    rss.context("Node process status is missing VmRSS")
}

#[cfg(target_os = "linux")]
async fn read_process_rss(pid: u32) -> anyhow::Result<Option<u64>> {
    let status = tokio::time::timeout(
        PROCESS_PROCFS_READ_TIMEOUT,
        tokio::fs::read_to_string(format!("/proc/{pid}/status")),
    )
    .await
    .context("Timed out reading local Node process status")??;
    Ok(Some(parse_process_rss(&status)?))
}

#[cfg(not(target_os = "linux"))]
async fn read_process_rss(_pid: u32) -> anyhow::Result<Option<u64>> {
    Ok(None)
}

#[cfg(unix)]
fn create_private_diagnostic_directory(path: &Path) -> anyhow::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true).mode(0o700);
    builder
        .create(path)
        .context("Failed to create local Node diagnostic directory")?;

    // Open before changing permissions so a concurrent path replacement cannot
    // redirect chmod to a different filesystem object.
    let directory = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
        .context("Failed to open local Node diagnostic directory")?;
    let metadata = directory
        .metadata()
        .context("Failed to inspect opened local Node diagnostic directory")?;
    anyhow::ensure!(
        metadata.file_type().is_dir(),
        "Local Node diagnostic path is not a directory"
    );
    anyhow::ensure!(
        metadata.uid() == unsafe { libc::geteuid() },
        "Local Node diagnostic directory has an unexpected owner"
    );
    if metadata.mode() & 0o7777 != 0o700 {
        // Do not turn an accidentally configured shared or system directory
        // into a private executor directory. Artifact-like names do not prove
        // that this lifecycle created the contents.
        anyhow::ensure!(
            fs::read_dir(path)
                .context("Failed to inspect local Node diagnostic directory")?
                .next()
                .transpose()
                .context("Failed to inspect local Node diagnostic directory entry")?
                .is_none(),
            "Refusing to restrict a nonempty local Node diagnostic directory"
        );
        directory
            .set_permissions(fs::Permissions::from_mode(0o700))
            .context("Failed to restrict local Node diagnostic directory permissions")?;
    }
    let opened_metadata = directory
        .metadata()
        .context("Failed to recheck opened local Node diagnostic directory")?;
    let path_metadata =
        fs::symlink_metadata(path).context("Failed to recheck local Node diagnostic directory")?;
    anyhow::ensure!(
        path_metadata.file_type().is_dir()
            && path_metadata.dev() == opened_metadata.dev()
            && path_metadata.ino() == opened_metadata.ino(),
        "Local Node diagnostic directory changed during setup"
    );
    anyhow::ensure!(
        opened_metadata.mode() & 0o7777 == 0o700,
        "Local Node diagnostic directory permissions are not private"
    );
    Ok(())
}

#[cfg(unix)]
fn diagnostic_directory() -> anyhow::Result<PathBuf> {
    let path = match env::var_os(LOCAL_NODE_EXECUTOR_DIAGNOSTICS_DIR_ENV) {
        Some(path) => {
            anyhow::ensure!(
                !path.is_empty(),
                "{LOCAL_NODE_EXECUTOR_DIAGNOSTICS_DIR_ENV} must not be empty"
            );
            PathBuf::from(path)
        },
        None => env::temp_dir().join("convex-node-executor-diagnostics"),
    };
    anyhow::ensure!(
        path.is_absolute(),
        "{LOCAL_NODE_EXECUTOR_DIAGNOSTICS_DIR_ENV} must be an absolute path"
    );
    create_private_diagnostic_directory(&path)?;
    Ok(path)
}

#[cfg(unix)]
async fn prepare_diagnostic_directory(pool_name: &str) -> Option<PathBuf> {
    let result = tokio::time::timeout(
        DIAGNOSTIC_FILESYSTEM_TIMEOUT,
        tokio::task::spawn_blocking(diagnostic_directory),
    )
    .await;
    match result {
        Ok(Ok(Ok(path))) => {
            crate::metrics::log_local_node_first_miss_diagnostic(
                pool_name,
                FirstMissDiagnosticOutcome::DiagnosticDirectorySuccess,
            );
            Some(path)
        },
        Err(_) | Ok(Err(_)) | Ok(Ok(Err(_))) => {
            crate::metrics::log_local_node_first_miss_diagnostic(
                pool_name,
                FirstMissDiagnosticOutcome::DiagnosticDirectoryFailure,
            );
            tracing::warn!(
                "Disabled first-miss local Node diagnostics because private artifact directory \
                 setup failed"
            );
            None
        },
    }
}

#[cfg(not(unix))]
async fn prepare_diagnostic_directory(_pool_name: &str) -> Option<PathBuf> {
    None
}

fn is_local_node_diagnostic_artifact(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    (name.starts_with("node-first-miss-") && name.ends_with(".json"))
        || (name.starts_with("node-profile-") && name.ends_with(".cpuprofile"))
        || (name.starts_with("node-report-") && name.ends_with(".json"))
}

fn is_partial_local_node_diagnostic_artifact(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            (name.starts_with("node-diagnostic-partial-") && name.ends_with(".partial"))
                || (name.starts_with("node-first-miss-partial-") && name.ends_with(".json.partial"))
        })
}

fn prune_diagnostic_artifacts(diagnostics_dir: &Path) -> anyhow::Result<()> {
    let now = SystemTime::now();
    let mut old_artifacts = BTreeMap::new();
    let mut retained_count = 0usize;
    let mut total_bytes = 0u64;
    for entry in
        fs::read_dir(diagnostics_dir).context("Failed to read local Node diagnostic directory")?
    {
        let entry = entry.context("Failed to read local Node diagnostic directory entry")?;
        let path = entry.path();
        let is_artifact = is_local_node_diagnostic_artifact(&path);
        let is_partial_artifact = is_partial_local_node_diagnostic_artifact(&path);
        if !entry
            .file_type()
            .context("Failed to inspect local Node diagnostic artifact type")?
            .is_file()
            || (!is_artifact && !is_partial_artifact)
        {
            continue;
        }
        let metadata = entry
            .metadata()
            .context("Failed to inspect local Node diagnostic artifact")?;
        let modified = metadata
            .modified()
            .context("Local Node diagnostic artifact has no modification time")?;
        let age = match now.duration_since(modified) {
            Ok(age) => age,
            Err(error) if error.duration() > MAX_DIAGNOSTIC_CLOCK_SKEW => {
                if is_partial_artifact {
                    // A non-cancelable writer can outlive the async filesystem
                    // timeout. A wall-clock rollback must not unlink its path.
                    Duration::ZERO
                } else {
                    fs::remove_file(&path)
                        .context("Failed to remove future-dated local Node diagnostic artifact")?;
                    continue;
                }
            },
            Err(_) => Duration::ZERO,
        };
        if is_partial_artifact {
            if age >= MIN_DIAGNOSTIC_ARTIFACT_PRUNE_AGE {
                fs::remove_file(&path)
                    .context("Failed to remove partial local Node diagnostic artifact")?;
            }
            continue;
        }
        assert!(is_artifact);
        if age > MAX_DIAGNOSTIC_ARTIFACT_AGE {
            fs::remove_file(&path)
                .context("Failed to remove expired local Node diagnostic artifact")?;
            continue;
        }
        retained_count = retained_count
            .checked_add(1)
            .context("Local Node diagnostic artifact count overflow")?;
        total_bytes = total_bytes
            .checked_add(metadata.len())
            .context("Local Node diagnostic artifact size overflow")?;
        if age >= MIN_DIAGNOSTIC_ARTIFACT_PRUNE_AGE {
            let replaced = old_artifacts.insert(
                (if modified > now { now } else { modified }, path),
                metadata.len(),
            );
            assert!(replaced.is_none());
        }

        while retained_count > MAX_DIAGNOSTIC_ARTIFACTS
            || total_bytes > MAX_DIAGNOSTIC_ARTIFACT_BYTES
        {
            // Startup pruning is detached and can overlap artifact creation.
            // Count recent files toward both limits, but leave a later
            // generation to prune them after all bounded writers should have
            // finished. Keeping only removable paths also bounds memory if the
            // private directory already contains many artifacts.
            let Some(((_, oldest_path), oldest_bytes)) = old_artifacts.pop_first() else {
                break;
            };
            fs::remove_file(oldest_path)
                .context("Failed to remove excess local Node diagnostic artifact")?;
            retained_count -= 1;
            total_bytes -= oldest_bytes;
        }
    }
    Ok(())
}

fn spawn_diagnostic_artifact_pruning(
    pool_name: Arc<str>,
    diagnostics_dir: PathBuf,
    in_progress: Arc<AtomicBool>,
) {
    if in_progress
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    let claim = DiagnosticArtifactPruningClaim { in_progress };
    tokio::spawn(async move {
        let result = tokio::time::timeout(
            DIAGNOSTIC_FILESYSTEM_TIMEOUT,
            tokio::task::spawn_blocking(move || {
                // A timed-out blocking filesystem call continues in the
                // background. Keep the claim there so later generations do not
                // start overlapping retention passes against the same files.
                let _claim = claim;
                prune_diagnostic_artifacts(&diagnostics_dir)
            }),
        )
        .await;
        let outcome = match result {
            Ok(Ok(Ok(()))) => FirstMissDiagnosticOutcome::RetentionSuccess,
            Err(_) | Ok(Err(_)) | Ok(Ok(Err(_))) => {
                tracing::warn!("Failed to prune local Node diagnostic artifacts");
                FirstMissDiagnosticOutcome::RetentionFailure
            },
        };
        crate::metrics::log_local_node_first_miss_diagnostic(&pool_name, outcome);
    });
}

impl NodeDiagnosticPaths {
    fn new(source_dir: &TempDir, diagnostics_dir: &Path, generation: u64) -> Self {
        let nonce = rand::rng().random::<u64>();
        let artifact_id = format!("g{generation}-{nonce:016x}");
        let report_filename = format!("node-report-{artifact_id}.json");
        let report_source_path = source_dir.path().join(&report_filename);
        let report = NodeDiagnosticReportPaths {
            source_path: report_source_path,
            destination_path: diagnostics_dir.join(&report_filename),
            filename: report_filename,
        };
        Self {
            control_path: source_dir.path().join(".diagnostic-profiler.sock"),
            first_miss_path: diagnostics_dir.join(format!("node-first-miss-{artifact_id}.json")),
            profile_source_path: source_dir
                .path()
                .join(format!("node-profile-{artifact_id}.cpuprofile")),
            profile_path: diagnostics_dir.join(format!("node-profile-{artifact_id}.cpuprofile")),
            report,
        }
    }
}

fn parse_process_stat(stat: &str) -> anyhow::Result<ProcessStatSnapshot> {
    let comm_end = stat
        .rfind(')')
        .context("Node process stat is missing command terminator")?;
    let fields: Vec<_> = stat
        .get(comm_end + 1..)
        .context("Node process stat command terminator is invalid")?
        .split_whitespace()
        .collect();
    anyhow::ensure!(fields.len() > 19, "Node process stat is truncated");
    let mut state_chars = fields[0].chars();
    let state = state_chars
        .next()
        .context("Node process stat is missing process state")?;
    anyhow::ensure!(
        state.is_ascii_alphabetic() && state_chars.next().is_none(),
        "Node process stat has an invalid process state"
    );
    Ok(ProcessStatSnapshot {
        state,
        user_ticks: fields[11]
            .parse()
            .context("Node process stat has invalid user CPU ticks")?,
        system_ticks: fields[12]
            .parse()
            .context("Node process stat has invalid system CPU ticks")?,
        thread_count: fields[17]
            .parse()
            .context("Node process stat has invalid thread count")?,
        start_time_ticks: fields[19]
            .parse()
            .context("Node process stat has invalid start time")?,
    })
}

#[cfg(target_os = "linux")]
async fn read_process_stat(pid: u32) -> anyhow::Result<Option<ProcessStatSnapshot>> {
    let stat = tokio::time::timeout(
        PROCESS_PROCFS_READ_TIMEOUT,
        tokio::fs::read_to_string(format!("/proc/{pid}/stat")),
    )
    .await
    .context("Timed out reading local Node process stat")??;
    Ok(Some(parse_process_stat(&stat)?))
}

#[cfg(not(target_os = "linux"))]
async fn read_process_stat(_pid: u32) -> anyhow::Result<Option<ProcessStatSnapshot>> {
    Ok(None)
}

#[cfg(target_os = "linux")]
async fn read_thread_stats(
    pid: u32,
    expected_start_time_ticks: u64,
) -> anyhow::Result<Option<(Vec<ThreadStatSnapshot>, bool)>> {
    let mut directory = tokio::fs::read_dir(format!("/proc/{pid}/task"))
        .await
        .context("Failed to read local Node thread directory")?;
    let mut tids = vec![pid];
    let mut truncated = false;
    while let Some(entry) = directory
        .next_entry()
        .await
        .context("Failed to read local Node thread directory entry")?
    {
        let Some(tid) = entry
            .file_name()
            .to_str()
            .and_then(|value| value.parse::<u32>().ok())
        else {
            continue;
        };
        if tid == pid {
            continue;
        }
        if tids.len() == MAX_DIAGNOSTIC_THREADS {
            truncated = true;
            break;
        }
        tids.push(tid);
    }
    tids.sort_unstable();
    let mut threads = Vec::with_capacity(tids.len());
    for tid in tids {
        let stat = match tokio::fs::read_to_string(format!("/proc/{pid}/task/{tid}/stat")).await {
            Ok(stat) => stat,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error).context("Failed to read local Node thread stat"),
        };
        let process = parse_process_stat(&stat)?;
        threads.push(ThreadStatSnapshot {
            tid,
            state: process.state,
            user_ticks: process.user_ticks,
            system_ticks: process.system_ticks,
        });
    }
    let process = read_process_stat(pid)
        .await?
        .context("Local Node process disappeared during thread sampling")?;
    anyhow::ensure!(
        process.start_time_ticks == expected_start_time_ticks,
        "Local Node process identity changed during thread sampling"
    );
    Ok(Some((threads, truncated)))
}

#[cfg(not(target_os = "linux"))]
async fn read_thread_stats(
    _pid: u32,
    _expected_start_time_ticks: u64,
) -> anyhow::Result<Option<(Vec<ThreadStatSnapshot>, bool)>> {
    Ok(None)
}

#[cfg(unix)]
fn private_diagnostic_file_identity(
    metadata: &fs::Metadata,
) -> anyhow::Result<DiagnosticFileIdentity> {
    anyhow::ensure!(
        metadata.file_type().is_file()
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.mode() & 0o7777 == 0o600
            && metadata.nlink() == 1,
        "Local Node diagnostic file is not a private regular file"
    );
    Ok(DiagnosticFileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

fn write_private_diagnostic_artifact(path: &Path, contents: &[u8]) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("Local Node diagnostic artifact has no parent directory")?;
    let mut file = TempFileBuilder::new()
        .prefix("node-diagnostic-partial-")
        .suffix(".partial")
        .tempfile_in(parent)
        .context("Failed to create partial local Node diagnostic artifact")?;
    #[cfg(unix)]
    file.as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))
        .context("Failed to restrict local Node diagnostic artifact permissions")?;
    file.write_all(contents)
        .and_then(|()| file.as_file().sync_all())
        .context("Failed to write local Node diagnostic artifact")?;
    #[cfg(unix)]
    let source_identity = private_diagnostic_file_identity(
        &file
            .as_file()
            .metadata()
            .context("Failed to inspect partial local Node diagnostic artifact")?,
    )?;
    let persisted_file = file
        .persist_noclobber(path)
        .map_err(std::io::Error::from)
        .context("Failed to publish local Node diagnostic artifact")?;
    #[cfg(unix)]
    {
        let persisted_identity = private_diagnostic_file_identity(
            &persisted_file
                .metadata()
                .context("Failed to inspect published local Node diagnostic artifact")?,
        )?;
        let destination = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(path)
            .context("Failed to open published local Node diagnostic artifact")?;
        let destination_identity = private_diagnostic_file_identity(
            &destination
                .metadata()
                .context("Failed to recheck published local Node diagnostic artifact")?,
        )?;
        anyhow::ensure!(
            persisted_identity == source_identity && destination_identity == source_identity,
            "Local Node diagnostic artifact identity changed during publication"
        );
    }
    drop(persisted_file);
    Ok(())
}

#[cfg(unix)]
fn prepare_private_diagnostic_file(path: &Path) -> anyhow::Result<DiagnosticFileIdentity> {
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .context("Failed to open private local Node diagnostic file")?;
    let original = file
        .metadata()
        .context("Failed to inspect private local Node diagnostic file")?;
    // A report written before the watchdog may already occupy this path. Check
    // the open inode before changing it so a hard link cannot turn report
    // preparation into truncation of another same-UID file.
    anyhow::ensure!(
        original.file_type().is_file()
            && original.uid() == unsafe { libc::geteuid() }
            && original.nlink() == 1,
        "Local Node diagnostic file is not an owned single-link regular file"
    );
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .context("Failed to restrict local Node diagnostic file permissions")?;
    file.set_len(0)
        .context("Failed to truncate local Node diagnostic file")?;
    let prepared = file
        .metadata()
        .context("Failed to recheck private local Node diagnostic file")?;
    let identity = private_diagnostic_file_identity(&prepared)?;
    anyhow::ensure!(
        identity.device == original.dev() && identity.inode == original.ino(),
        "Local Node diagnostic file identity changed during preparation"
    );
    Ok(identity)
}

#[cfg(not(unix))]
fn prepare_private_diagnostic_file(_path: &Path) -> anyhow::Result<DiagnosticFileIdentity> {
    anyhow::bail!("Private local Node diagnostic files are unsupported")
}

#[cfg(unix)]
fn read_private_diagnostic_artifact(
    path: &Path,
    max_bytes: u64,
    expected_source: ExpectedDiagnosticSource,
) -> anyhow::Result<Option<Vec<u8>>> {
    let mut file = match fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("Failed to open local Node diagnostic source"),
    };
    let before = file
        .metadata()
        .context("Failed to inspect local Node diagnostic source")?;
    let before_identity = private_diagnostic_file_identity(&before)?;
    if let ExpectedDiagnosticSource::Exact(expected_identity) = expected_source {
        anyhow::ensure!(
            before_identity == expected_identity,
            "Local Node diagnostic source identity changed before publication"
        );
    }
    if before.len() == 0 {
        return Ok(None);
    }
    anyhow::ensure!(
        before.len() <= max_bytes,
        "Local Node diagnostic source exceeded its size limit"
    );
    let mut contents = Vec::with_capacity(usize::try_from(before.len())?);
    std::io::Read::by_ref(&mut file)
        .take(max_bytes + 1)
        .read_to_end(&mut contents)
        .context("Failed to read local Node diagnostic source")?;
    anyhow::ensure!(
        contents.len() as u64 <= max_bytes,
        "Local Node diagnostic source exceeded its size limit"
    );
    let after = file
        .metadata()
        .context("Failed to recheck local Node diagnostic source")?;
    let after_identity = private_diagnostic_file_identity(&after)?;
    if before.len() != contents.len() as u64
        || before.len() != after.len()
        || before.mtime() != after.mtime()
        || before.mtime_nsec() != after.mtime_nsec()
    {
        return Ok(None);
    }
    anyhow::ensure!(
        before_identity == after_identity,
        "Local Node diagnostic source changed while being read"
    );
    Ok(Some(contents))
}

#[cfg(not(unix))]
fn read_private_diagnostic_artifact(
    _path: &Path,
    _max_bytes: u64,
    _expected_source: ExpectedDiagnosticSource,
) -> anyhow::Result<Option<Vec<u8>>> {
    anyhow::bail!("Private local Node diagnostic artifacts are unsupported")
}

fn copy_private_diagnostic_artifact(
    source_path: &Path,
    destination_path: &Path,
    max_bytes: u64,
) -> anyhow::Result<()> {
    let contents = read_private_diagnostic_artifact(
        source_path,
        max_bytes,
        ExpectedDiagnosticSource::AnyPrivateRegularFile,
    )?
    .context("Local Node diagnostic source is incomplete")?;
    write_private_diagnostic_artifact(destination_path, &contents)
}

fn remove_sensitive_node_diagnostic_report_fields(value: &mut JsonValue) {
    match value {
        JsonValue::Object(fields) => {
            for field in [
                "environmentVariables",
                "networkInterfaces",
                "localEndpoint",
                "remoteEndpoint",
            ] {
                fields.remove(field);
            }
            for value in fields.values_mut() {
                remove_sensitive_node_diagnostic_report_fields(value);
            }
        },
        JsonValue::Array(values) => {
            for value in values {
                remove_sensitive_node_diagnostic_report_fields(value);
            }
        },
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {},
    }
}

fn try_publish_node_diagnostic_report(
    paths: &NodeDiagnosticReportPaths,
    source_identity: DiagnosticFileIdentity,
) -> anyhow::Result<bool> {
    let Some(contents) = read_private_diagnostic_artifact(
        &paths.source_path,
        MAX_DIAGNOSTIC_REPORT_BYTES,
        ExpectedDiagnosticSource::Exact(source_identity),
    )?
    else {
        return Ok(false);
    };
    let mut report = match serde_json::from_slice::<JsonValue>(&contents) {
        Ok(report) => report,
        Err(_) => return Ok(false),
    };
    remove_sensitive_node_diagnostic_report_fields(&mut report);
    let report = serde_json::to_vec(&report)
        .context("Failed to serialize sanitized local Node diagnostic report")?;
    anyhow::ensure!(
        report.len() as u64 <= MAX_DIAGNOSTIC_REPORT_BYTES,
        "Sanitized local Node diagnostic report exceeded its size limit"
    );
    write_private_diagnostic_artifact(&paths.destination_path, &report)?;
    Ok(true)
}

async fn publish_node_diagnostic_report(
    paths: NodeDiagnosticReportPaths,
    source_identity: DiagnosticFileIdentity,
    source_owner: Arc<InnerLocalNodeExecutor>,
) -> bool {
    tokio::time::timeout(DIAGNOSTIC_TRIGGER_TIMEOUT, async {
        loop {
            let attempt_paths = paths.clone();
            let attempt_source_owner = source_owner.clone();
            // A timed-out blocking read can outlive this async task. Keep its
            // generation tempdir alive until the read actually returns.
            let result = tokio::task::spawn_blocking(move || {
                let result = try_publish_node_diagnostic_report(&attempt_paths, source_identity);
                drop(attempt_source_owner);
                result
            })
            .await;
            match result {
                Ok(Ok(true)) => return true,
                Ok(Ok(false)) => tokio::time::sleep(DIAGNOSTIC_ARTIFACT_POLL_INTERVAL).await,
                Ok(Err(_)) | Err(_) => return false,
            }
        }
    })
    .await
    .unwrap_or(false)
}

#[cfg(unix)]
fn trigger_node_diagnostic_report(pid: u32) -> FirstMissDiagnosticOutcome {
    if pid == 0 {
        return FirstMissDiagnosticOutcome::DiagnosticReportInvalidPid;
    }
    let Ok(pid) = i32::try_from(pid) else {
        return FirstMissDiagnosticOutcome::DiagnosticReportInvalidPid;
    };
    // The child is launched with SIGUSR2 diagnostic-report handling enabled.
    let result = unsafe { libc::kill(pid, libc::SIGUSR2) };
    if result == 0 {
        FirstMissDiagnosticOutcome::DiagnosticReportRequested
    } else {
        FirstMissDiagnosticOutcome::DiagnosticReportRequestFailed
    }
}

#[cfg(not(unix))]
fn trigger_node_diagnostic_report(_pid: u32) -> FirstMissDiagnosticOutcome {
    FirstMissDiagnosticOutcome::DiagnosticReportUnsupported
}

#[cfg(unix)]
async fn connect_main_thread_profiler(path: &Path) -> std::io::Result<UnixStream> {
    let started_at = Instant::now();
    loop {
        match UnixStream::connect(path).await {
            Ok(stream) => return Ok(stream),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) && started_at.elapsed() < DIAGNOSTIC_PROFILE_CONNECT_TIMEOUT =>
            {
                let remaining =
                    DIAGNOSTIC_PROFILE_CONNECT_TIMEOUT.saturating_sub(started_at.elapsed());
                tokio::time::sleep(remaining.min(DIAGNOSTIC_PROFILE_CONNECT_RETRY_INTERVAL)).await;
            },
            Err(error) => return Err(error),
        }
    }
}

#[cfg(unix)]
async fn trigger_main_thread_profile(
    diagnostic_paths: &NodeDiagnosticPaths,
    source_owner: Arc<InnerLocalNodeExecutor>,
) -> FirstMissDiagnosticOutcome {
    let result = tokio::time::timeout(DIAGNOSTIC_TRIGGER_TIMEOUT, async {
        let mut stream = connect_main_thread_profiler(&diagnostic_paths.control_path).await?;
        stream.write_all(b"profile\n").await?;
        // The Worker validates the complete command at EOF. Half-close the
        // request side while keeping the response side open.
        stream.shutdown().await?;
        let mut response = Vec::new();
        stream
            .take(MAX_DIAGNOSTIC_CONTROL_RESPONSE_BYTES + 1)
            .read_to_end(&mut response)
            .await?;
        Ok::<_, std::io::Error>(response)
    })
    .await;
    let response = match result {
        Err(_) => return FirstMissDiagnosticOutcome::CpuProfileTimeout,
        Ok(Err(_)) => return FirstMissDiagnosticOutcome::CpuProfileTransportFailed,
        Ok(Ok(response)) => response,
    };
    if response.len() as u64 > MAX_DIAGNOSTIC_CONTROL_RESPONSE_BYTES {
        return FirstMissDiagnosticOutcome::CpuProfileResponseTooLarge;
    }
    match response.as_slice() {
        b"completed\n" => {
            let source_path = diagnostic_paths.profile_source_path.clone();
            let destination_path = diagnostic_paths.profile_path.clone();
            let copy_source_owner = source_owner.clone();
            match tokio::time::timeout(
                DIAGNOSTIC_FILESYSTEM_TIMEOUT,
                tokio::task::spawn_blocking(move || {
                    // `spawn_blocking` continues after an async timeout, so it
                    // must retain the generation-local source directory.
                    let result = copy_private_diagnostic_artifact(
                        &source_path,
                        &destination_path,
                        MAX_DIAGNOSTIC_PROFILE_BYTES,
                    );
                    drop(copy_source_owner);
                    result
                }),
            )
            .await
            {
                Ok(Ok(Ok(()))) => FirstMissDiagnosticOutcome::CpuProfileCompleted,
                Err(_) | Ok(Err(_)) | Ok(Ok(Err(_))) => {
                    FirstMissDiagnosticOutcome::CpuProfileWriteFailed
                },
            }
        },
        b"already_started\n" => FirstMissDiagnosticOutcome::CpuProfileAlreadyStarted,
        b"enable_failed\n" => FirstMissDiagnosticOutcome::CpuProfileEnableFailed,
        b"start_failed\n" => FirstMissDiagnosticOutcome::CpuProfileStartFailed,
        b"stop_failed\n" => FirstMissDiagnosticOutcome::CpuProfileStopFailed,
        b"profile_too_large\n" => FirstMissDiagnosticOutcome::CpuProfileTooLarge,
        b"write_failed\n" => FirstMissDiagnosticOutcome::CpuProfileWriteFailed,
        _ => FirstMissDiagnosticOutcome::CpuProfileInvalidResponse,
    }
}

#[cfg(not(unix))]
async fn trigger_main_thread_profile(
    _diagnostic_paths: &NodeDiagnosticPaths,
    _source_owner: Arc<InnerLocalNodeExecutor>,
) -> FirstMissDiagnosticOutcome {
    FirstMissDiagnosticOutcome::CpuProfileUnsupported
}

struct ActiveRequestGuard {
    inner: Arc<InnerLocalNodeExecutor>,
    diagnostic_id: Option<u64>,
    outcome: &'static str,
}

struct WaitingRequestGuard {
    activity: Arc<ExecutorPoolActivity>,
    waiting: bool,
}

enum InnerAcquisition {
    Ready {
        inner: Arc<InnerLocalNodeExecutor>,
        guard: ActiveRequestGuard,
    },
    Draining(Arc<InnerLocalNodeExecutor>),
    Transition(Arc<HotTransitionStatus>),
    Missing,
}

impl ReapingTempDir {
    fn new(pool_name: Arc<str>, generation: u64, source_dir: TempDir) -> Self {
        Self {
            generation,
            pool_name,
            source_dir: Some(source_dir),
        }
    }

    fn remove_after_reaping(mut self) {
        let source_dir = self
            .source_dir
            .take()
            .expect("Reaped local Node executor child has no temp directory");
        let generation = self.generation;
        let pool_name = self.pool_name.clone();
        let cleanup_pool_name = pool_name.clone();
        // Package trees can be several GiB. Retain the path before spawning so
        // thread-start failure preserves it, and keep recursive deletion out
        // of both async workers and Tokio's shutdown-waited blocking pool.
        let source_path = source_dir.keep();
        if let Err(error) = std::thread::Builder::new()
            .name("local-node-tempdir-cleanup".to_owned())
            .spawn(move || {
                if let Err(error) = fs::remove_dir_all(source_path) {
                    tracing::error!(
                        pool_name = %cleanup_pool_name,
                        generation,
                        error_kind = ?error.kind(),
                        "Failed to remove reaped local Node executor temp directory"
                    );
                }
            })
        {
            tracing::error!(
                pool_name = %pool_name,
                generation,
                error_kind = ?error.kind(),
                "Failed to start reaped local Node executor temp directory cleanup"
            );
        }
    }
}

impl Drop for ReapingTempDir {
    fn drop(&mut self) {
        if let Some(source_dir) = self.source_dir.take() {
            // Cleanup-task cancellation and runtime teardown must not remove
            // files while the direct child may still be using them.
            drop(source_dir.keep());
            tracing::error!(
                pool_name = %self.pool_name,
                generation = self.generation,
                "Retained local Node executor temp directory because direct child reaping was not \
                 confirmed"
            );
        }
    }
}

impl ManagedChild {
    fn new(pool_name: Arc<str>, generation: u64, child: Child, source_dir: TempDir) -> Self {
        Self {
            generation,
            pool_name,
            child: Some(child),
            source_dir: Some(source_dir),
        }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child
            .as_mut()
            .expect("Local Node executor child owner is empty")
    }

    fn mark_reaped(&mut self) {
        self.child
            .take()
            .expect("Local Node executor child was reaped twice");
    }

    async fn terminate(&mut self) -> anyhow::Result<ChildTerminationObservation> {
        anyhow::ensure!(
            self.child.is_some(),
            "Local Node executor child was already reaped"
        );
        let generation = self.generation;
        let pool_name = self.pool_name.clone();
        let result =
            InnerLocalNodeExecutor::terminate_child(&pool_name, generation, self.child_mut()).await;
        if result.is_ok() {
            self.mark_reaped();
        }
        result
    }

    async fn terminate_if_needed(&mut self) -> anyhow::Result<Option<ChildTerminationObservation>> {
        if self.child.is_none() {
            return Ok(None);
        }
        self.terminate().await.map(Some)
    }

    fn spawn_drop_cleanup(&mut self) {
        let mut child = self
            .child
            .take()
            .expect("Unreaped local Node executor child has no owner");
        // Startup cancellation drops this owner before InnerLocalNodeExecutor
        // exists. Transfer the tempdir with the child so the socket and script
        // tree remain valid until the detached cleanup has reaped the process.
        let generation = self.generation;
        let pool_name = self.pool_name.clone();
        let source_dir = ReapingTempDir::new(
            pool_name.clone(),
            generation,
            self.source_dir
                .take()
                .expect("Local Node executor child has no temp directory"),
        );
        let retry_kill = match child.start_kill() {
            Ok(()) => false,
            Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => false,
            Err(error) => {
                tracing::error!(
                    pool_name = %pool_name,
                    generation,
                    error_kind = ?error.kind(),
                    "Failed to terminate dropped local Node executor child"
                );
                true
            },
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            // `kill_on_drop` and Tokio's orphan reaper remain the final fallback
            // when the runtime itself is already gone. ReapingTempDir preserves
            // the files because this path cannot confirm reaping.
            drop(child);
            return;
        };
        runtime.spawn(async move {
            if retry_kill
                && let Err(error) = child.start_kill()
                && error.kind() != std::io::ErrorKind::InvalidInput
            {
                // Do not wait forever on a child whose termination never
                // started. Dropping it retries kill-on-drop and hands any
                // resulting zombie to Tokio's orphan reaper.
                tracing::error!(
                    pool_name = %pool_name,
                    generation,
                    error_kind = ?error.kind(),
                    "Failed to retry termination of dropped local Node executor child"
                );
                drop(child);
                return;
            }
            match child.wait().await {
                Ok(status) => {
                    InnerLocalNodeExecutor::record_child_exit(&pool_name, status);
                    drop(child);
                    source_dir.remove_after_reaping();
                },
                Err(error) => {
                    tracing::error!(
                        pool_name = %pool_name,
                        generation,
                        error_kind = ?error.kind(),
                        "Failed to reap dropped local Node executor child"
                    );
                },
            }
        });
    }
}

impl NodeExecutorHealth {
    fn runtime_stats_supported(&self) -> Option<bool> {
        match (self.package_cache.is_some(), self.stack_trace.is_some()) {
            (true, true) => Some(true),
            (false, false) => Some(false),
            _ => None,
        }
    }

    fn valid_runtime_stats_support(
        &self,
        previous_package: &NodePackageCacheStats,
        previous_stack: &NodeStackTraceStats,
    ) -> Option<bool> {
        match self.runtime_stats_supported()? {
            false => Some(false),
            true if self.runtime_counters_are_monotonic(previous_package, previous_stack) => {
                Some(true)
            },
            true => None,
        }
    }

    fn runtime_counters_are_monotonic(
        &self,
        previous_package: &NodePackageCacheStats,
        previous_stack: &NodeStackTraceStats,
    ) -> bool {
        let package = self
            .package_cache
            .as_ref()
            .expect("Validated Node health response is missing package stats");
        let stack = self
            .stack_trace
            .as_ref()
            .expect("Validated Node health response is missing stack stats");
        package.imported_source_packages >= previous_package.imported_source_packages
            && package.source_hits >= previous_package.source_hits
            && package.source_publishes >= previous_package.source_publishes
            && package.source_retirements >= previous_package.source_retirements
            && package.source_failed_publications >= previous_package.source_failed_publications
            && package.external_hits >= previous_package.external_hits
            && package.external_publishes >= previous_package.external_publishes
            && package.external_retirements >= previous_package.external_retirements
            && package.external_failed_publications >= previous_package.external_failed_publications
            && stack.invocations >= previous_stack.invocations
            && stack.frames_processed >= previous_stack.frames_processed
            && stack.duration_ms.is_finite()
            && stack.duration_ms >= previous_stack.duration_ms
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if self.child.is_some() {
            // A request can be canceled while an unpublished child is starting.
            // Transfer the wait to the runtime instead of relying only on
            // Tokio's best-effort orphan reaper.
            self.spawn_drop_cleanup();
        } else if let Some(source_dir) = self.source_dir.take() {
            ReapingTempDir::new(self.pool_name.clone(), self.generation, source_dir)
                .remove_after_reaping();
        }
    }
}

impl WaitingRequestGuard {
    fn new(activity: Arc<ExecutorPoolActivity>) -> Self {
        let waiting = activity.waiting_requests.fetch_add(1, Ordering::Relaxed) + 1;
        crate::metrics::set_local_node_waiting_requests(&activity.pool_name, waiting);
        Self {
            activity,
            waiting: true,
        }
    }

    fn finish(mut self) {
        let previous = self
            .activity
            .waiting_requests
            .fetch_sub(1, Ordering::Relaxed);
        assert!(previous > 0);
        crate::metrics::set_local_node_waiting_requests(&self.activity.pool_name, previous - 1);
        self.waiting = false;
    }
}

impl Drop for WaitingRequestGuard {
    fn drop(&mut self) {
        if self.waiting {
            let previous = self
                .activity
                .waiting_requests
                .fetch_sub(1, Ordering::Relaxed);
            assert!(previous > 0);
            crate::metrics::set_local_node_waiting_requests(&self.activity.pool_name, previous - 1);
        }
    }
}

impl ActiveRequestGuard {
    fn new(inner: Arc<InnerLocalNodeExecutor>, metadata: RequestDiagnosticMetadata) -> Self {
        let mut guard = Self {
            inner,
            diagnostic_id: None,
            outcome: "internal_error",
        };
        guard.inner.active_requests.fetch_add(1, Ordering::Relaxed);
        let diagnostic_id = {
            let mut active = guard
                .inner
                .active_request_diagnostics
                .lock()
                .expect("Local Node active-request diagnostic lock is poisoned");
            if active.len() >= MAX_DIAGNOSTIC_ACTIVE_REQUESTS {
                None
            } else {
                let id = guard
                    .inner
                    .next_active_request_id
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                        current.checked_add(1)
                    })
                    .expect("Local Node active-request diagnostic id overflow");
                active.insert(
                    id,
                    ActiveRequestDiagnostic {
                        metadata,
                        started_at: Instant::now(),
                    },
                );
                Some(id)
            }
        };
        guard.diagnostic_id = diagnostic_id;
        let active = guard
            .inner
            .activity
            .active_requests
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        crate::metrics::log_local_node_request_start(&guard.inner.pool_name);
        crate::metrics::set_local_node_active_requests(&guard.inner.pool_name, active);
        guard
    }

    fn set_outcome(&mut self, outcome: &'static str) {
        self.outcome = outcome;
    }
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        if let Some(diagnostic_id) = self.diagnostic_id {
            let removed = self
                .inner
                .active_request_diagnostics
                .lock()
                .expect("Local Node active-request diagnostic lock is poisoned")
                .remove(&diagnostic_id);
            assert!(removed.is_some());
        }
        let generation_previous = self.inner.active_requests.fetch_sub(1, Ordering::Relaxed);
        assert!(generation_previous > 0);
        if generation_previous == 1 {
            self.inner.idle.notify_waiters();
        }
        let pool_previous = self
            .inner
            .activity
            .active_requests
            .fetch_sub(1, Ordering::Relaxed);
        assert!(pool_previous > 0);
        crate::metrics::set_local_node_active_requests(&self.inner.pool_name, pool_previous - 1);
        crate::metrics::log_local_node_request_completion(&self.inner.pool_name, self.outcome);
    }
}

impl LocalNodeExecutorConfig {
    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.node_process_timeout > Duration::ZERO,
            "Local Node executor process timeout must be greater than zero"
        );
        anyhow::ensure!(
            self.health_check_timeout > Duration::ZERO,
            "Local Node executor health-check timeout must be greater than zero"
        );
        anyhow::ensure!(
            self.watchdog_interval > Duration::ZERO,
            "Local Node executor watchdog interval must be greater than zero"
        );
        anyhow::ensure!(
            self.watchdog_failure_threshold > 0,
            "Local Node executor watchdog failure threshold must be greater than zero"
        );
        anyhow::ensure!(
            self.max_old_space_size_mib > 0,
            "Local Node executor old-space allowance must be greater than zero"
        );
        anyhow::ensure!(
            self.max_rss_bytes > 0,
            "Local Node executor RSS threshold must be greater than zero"
        );
        anyhow::ensure!(
            self.memory_pressure_min_rss_bytes > 0,
            "Local Node executor cgroup-pressure RSS threshold must be greater than zero"
        );
        anyhow::ensure!(
            self.memory_pressure_min_rss_bytes < self.max_rss_bytes,
            "Local Node executor cgroup-pressure RSS threshold must be below the ordinary RSS \
             threshold"
        );
        anyhow::ensure!(
            self.memory_pressure_grace > Duration::ZERO,
            "Local Node executor cgroup-pressure grace must be greater than zero"
        );
        anyhow::ensure!(
            self.max_generation_age > Duration::ZERO,
            "Local Node executor generation age threshold must be greater than zero"
        );
        anyhow::ensure!(
            self.max_imported_source_packages > 0,
            "Local Node executor package threshold must be greater than zero"
        );
        let old_space_bytes = u64::try_from(self.max_old_space_size_mib)?
            .checked_mul(MIB_BYTES)
            .context("Local Node executor old-space allowance overflow")?;
        anyhow::ensure!(
            old_space_bytes < self.max_rss_bytes,
            "Local Node executor RSS threshold must exceed its V8 old-space allowance"
        );
        Ok(())
    }

    fn old_space_bytes(&self) -> u64 {
        u64::try_from(self.max_old_space_size_mib)
            .expect("validated local Node old-space allowance does not fit u64")
            .checked_mul(MIB_BYTES)
            .expect("validated local Node old-space allowance overflow")
    }

    pub(crate) fn with_pool_name(mut self, pool_name: Arc<str>) -> Self {
        self.pool_name = pool_name;
        self
    }

    pub(crate) fn surge_coordinator(&self) -> Arc<SurgeCoordinator> {
        self.surge_coordinator.clone()
    }
}

impl InnerLocalNodeExecutor {
    fn mark_execution_unavailable(&self) {
        self.execution_unavailable.store(true, Ordering::Release);
        self.execution_unavailable_notify.notify_waiters();
    }

    async fn wait_until_execution_unavailable(&self) {
        loop {
            let notified = self.execution_unavailable_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.execution_unavailable.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    fn admit_request(
        self: &Arc<Self>,
        request_metadata: RequestDiagnosticMetadata,
        preparation_descriptor: Option<PreparationDescriptor>,
    ) -> ActiveRequestGuard {
        // Callers hold the generation-state mutex, so descriptor retention and
        // the active increment both happen before promotion can close admission.
        if let Some(preparation_descriptor) = preparation_descriptor {
            PreparationDescriptor::retain_fresher(
                &mut self
                    .preparation_descriptor
                    .lock()
                    .expect("Local Node preparation descriptor lock poisoned"),
                preparation_descriptor,
            );
        }
        ActiveRequestGuard::new(self.clone(), request_metadata)
    }

    async fn terminate_startup_child(
        server_handle: &Arc<Mutex<ManagedChild>>,
    ) -> anyhow::Result<()> {
        server_handle
            .lock()
            .await
            .terminate()
            .await
            .map(|_| ())
            .map_err(|_| UnconfirmedStartupChildCleanup.into())
    }

    async fn terminate_unmanaged_startup_child(child: &mut Child) -> anyhow::Result<()> {
        match child.try_wait() {
            Ok(Some(_)) => return Ok(()),
            Ok(None) | Err(_) => {},
        }
        match child.start_kill() {
            Ok(()) => {},
            Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => {},
            Err(_) => return Err(UnconfirmedStartupChildCleanup.into()),
        }
        child
            .wait()
            .await
            .map(|_| ())
            .map_err(|_| UnconfirmedStartupChildCleanup.into())
    }

    fn claim_first_miss_diagnostics(&self) -> bool {
        self.diagnostic_paths.is_some()
            && !self
                .first_miss_diagnostics_started
                .swap(true, Ordering::AcqRel)
    }

    async fn wait_until_idle(&self) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.active_requests.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    async fn wait_until_retired(&self) -> anyhow::Result<()> {
        loop {
            let notified = self.retired_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.retired.load(Ordering::Acquire) {
                return Ok(());
            }
            if self.retirement_failed.load(Ordering::Acquire) {
                anyhow::bail!("Local Node executor generation retirement failed");
            }
            notified.await;
        }
    }

    fn mark_retirement_failed(&self) {
        self.retirement_failed.store(true, Ordering::Release);
        self.retired_notify.notify_waiters();
    }

    async fn new(
        generation: u64,
        resident_fingerprint: Option<ResidentGenerationFingerprint>,
        activity: Arc<ExecutorPoolActivity>,
        config: &LocalNodeExecutorConfig,
        candidate_cancellation: Option<&CandidateStartupCancellation>,
        cleanup: Option<&HotTransitionCleanupOwner>,
    ) -> anyhow::Result<Self> {
        tracing::info!(pool_name = %config.pool_name, "Initializing inner local node executor");
        if let Some(diagnostics_dir) = &config.diagnostics_dir {
            // Retention is lifecycle telemetry, not a prerequisite for child
            // startup or replacement.
            spawn_diagnostic_artifact_pruning(
                config.pool_name.clone(),
                diagnostics_dir.clone(),
                config.diagnostic_pruning_in_progress.clone(),
            );
        }
        // Create a single temp directory for both source files and Node.js temp files
        let source_dir = TempDir::new()?;
        let diagnostic_paths = config.diagnostics_dir.as_deref().map(|diagnostics_dir| {
            NodeDiagnosticPaths::new(&source_dir, diagnostics_dir, generation)
        });
        let (source, source_map) =
            node_executor_file("local.cjs").expect("local.cjs not generated!");
        let source_map = source_map.context("Missing local.cjs.map")?;
        let source_path = source_dir.path().join("local.cjs");
        let source_map_path = source_dir.path().join("local.cjs.map");
        fs::write(&source_path, source.as_bytes())?;
        fs::write(source_map_path, source_map.as_bytes())?;
        let socket_path = if cfg!(unix) {
            source_dir.path().join(".executor.sock")
        } else if cfg!(windows) {
            PathBuf::from(format!(
                r"\\.\pipe\cvx-node-executor-{:016x}",
                rand::rng().random::<u64>()
            ))
        } else {
            panic!("not supported");
        };
        // Don't keep idle connections in the pool. The Node HTTP server closes
        // idle keep-alive connections after its (default 5s) `keepAliveTimeout`,
        // but hyper's pool would hold one much longer and reuse it right as the
        // server closes it, surfacing as a spurious "connection reset by peer".
        // Opening a fresh connection per request is cheap over a local socket.
        let mut client_builder = Client::builder().pool_max_idle_per_host(0);
        #[cfg(unix)]
        {
            client_builder = client_builder.unix_socket(socket_path.clone());
        }
        #[cfg(windows)]
        {
            client_builder = client_builder.windows_named_pipe(socket_path.clone());
        }
        let client = client_builder.build()?;
        let server_handle = Self::start_node_with_listener(
            config,
            &source_path,
            &source_dir,
            &socket_path,
            diagnostic_paths.as_ref(),
        )
        .await?;
        let pid = server_handle.id();
        let server_handle = Arc::new(Mutex::new(ManagedChild::new(
            config.pool_name.clone(),
            generation,
            server_handle,
            source_dir,
        )));
        if let Some(cleanup) = cleanup {
            // Publish the owner before the next await so cancellation cannot
            // strand a spawned candidate behind a best-effort drop cleanup.
            cleanup.attach_startup_child(server_handle.clone());
        }
        crate::metrics::log_local_node_child_start(&config.pool_name);
        let Some(pid) = pid else {
            if cleanup.is_none() {
                Self::terminate_startup_child(&server_handle).await?;
            }
            anyhow::bail!("Local Node executor child has no process id");
        };

        // A new child has no prior backend observation. Use a zero baseline so
        // startup cannot accept cumulative values that the watchdog rejects.
        let empty_package_stats = NodePackageCacheStats::default();
        let empty_stack_stats = NodeStackTraceStats::default();
        // Wait for the Node process to be ready to handle HTTP requests.
        for _ in 0..MAX_HEALTH_CHECK_ATTEMPTS {
            if let Some(outcome) =
                candidate_cancellation.and_then(CandidateStartupCancellation::outcome)
            {
                if cleanup.is_none() {
                    Self::terminate_startup_child(&server_handle).await?;
                }
                return Err(CandidateStartupCanceled { outcome }.into());
            }
            let child_status = server_handle.lock().await.child_mut().try_wait();
            match child_status {
                Ok(Some(status)) => {
                    Self::record_child_exit(&config.pool_name, status);
                    server_handle.lock().await.mark_reaped();
                    anyhow::bail!("Node executor server exited before becoming healthy");
                },
                Ok(None) => {},
                Err(error) => {
                    if cleanup.is_none() {
                        Self::terminate_startup_child(&server_handle).await?;
                    }
                    anyhow::bail!(
                        "Failed to inspect local Node executor child: {:?}",
                        error.kind()
                    );
                },
            }
            let health_check_started = Instant::now();
            let health = Self::check_server_health(&client, config.health_check_timeout).await;
            if let Some(outcome) =
                candidate_cancellation.and_then(CandidateStartupCancellation::outcome)
            {
                if cleanup.is_none() {
                    Self::terminate_startup_child(&server_handle).await?;
                }
                return Err(CandidateStartupCanceled { outcome }.into());
            }
            let runtime_stats_supported = health
                .as_ref()
                .filter(|health| health.status == "ok")
                .and_then(|health| {
                    health.valid_runtime_stats_support(&empty_package_stats, &empty_stack_stats)
                });
            crate::metrics::log_local_node_health_check(
                &config.pool_name,
                health_check_started.elapsed(),
                "startup",
                runtime_stats_supported.is_some(),
            );
            if let Some(runtime_stats_supported) = runtime_stats_supported {
                return Ok(Self {
                    generation,
                    pool_name: config.pool_name.clone(),
                    resident_fingerprint,
                    activity,
                    pid,
                    started_at: Instant::now(),
                    runtime_stats_supported,
                    active_requests: AtomicUsize::new(0),
                    retirement_requested: AtomicBool::new(false),
                    idle: Notify::new(),
                    execution_unavailable: AtomicBool::new(false),
                    execution_unavailable_notify: Notify::new(),
                    terminate_draining: AtomicBool::new(false),
                    terminate_draining_notify: Notify::new(),
                    retired: AtomicBool::new(false),
                    retirement_failed: AtomicBool::new(false),
                    retired_notify: Notify::new(),
                    #[cfg(test)]
                    termination_failures_remaining: AtomicUsize::new(0),
                    retained_source_packages: AtomicU64::new(0),
                    retained_external_packages: AtomicU64::new(0),
                    imported_source_packages: AtomicU64::new(0),
                    registered_stack_roots: AtomicU64::new(0),
                    first_miss_diagnostics_started: AtomicBool::new(false),
                    next_active_request_id: AtomicU64::new(0),
                    active_request_diagnostics: StdMutex::new(BTreeMap::new()),
                    preparation_descriptor: StdMutex::new(None),
                    diagnostic_paths,
                    server_handle,
                    client,
                });
            }
            tokio::time::sleep(HEALTH_CHECK_INTERVAL).await;
        }
        if cleanup.is_none() {
            Self::terminate_startup_child(&server_handle).await?;
        }
        Err(CandidateStartupHealthFailed.into())
    }

    async fn check_node_version(node_path: &Path) -> anyhow::Result<()> {
        let mut command = TokioCommand::new(node_path);
        // This probe runs before the server child enters ManagedChild, so it
        // needs its own bounded, cancellation-safe kill behavior.
        command
            .arg("--version")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| {
            anyhow::anyhow!(
                "Failed to start local Node version check: {:?}",
                error.kind()
            )
        })?;
        let mut stdout = child
            .stdout
            .take()
            .expect("Piped local Node version check has no stdout");
        let probe_result = {
            let probe = async {
                let mut version = Vec::new();
                let mut buffer = [0; 256];
                loop {
                    let read = stdout.read(&mut buffer).await?;
                    if read == 0 {
                        break;
                    }
                    let retained = MAX_NODE_VERSION_OUTPUT_BYTES.saturating_sub(version.len());
                    version.extend_from_slice(&buffer[..read.min(retained)]);
                    if read > retained {
                        // Stop at the first excess chunk. A continuously writable
                        // pipe can otherwise keep every read immediately ready and
                        // prevent the outer timeout from being polled.
                        match child.start_kill() {
                            Ok(()) => {},
                            Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => {},
                            Err(error) => return Err(error),
                        }
                        let status = child.wait().await?;
                        return Ok::<_, std::io::Error>((status, version, true));
                    }
                }
                let status = child.wait().await?;
                Ok::<_, std::io::Error>((status, version, false))
            };
            tokio::pin!(probe);
            tokio::time::timeout(NODE_VERSION_CHECK_TIMEOUT, &mut probe).await
        };
        let (status, version, output_too_large) = match probe_result {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                Self::terminate_unmanaged_startup_child(&mut child).await?;
                anyhow::bail!(
                    "Failed to complete local Node version check: {:?}",
                    error.kind()
                );
            },
            Err(_) => {
                Self::terminate_unmanaged_startup_child(&mut child).await?;
                return Err(ErrorMetadata::bad_request(
                    "DeploymentNotConfiguredForNodeActions",
                    "Deployment is not configured to deploy \"use node\" actions. The Node.js \
                     version check timed out.",
                )
                .into());
            },
        };

        if output_too_large || !status.success() || !version.starts_with(b"v24.") {
            anyhow::bail!(ErrorMetadata::bad_request(
                "DeploymentNotConfiguredForNodeActions",
                "Deployment is not configured to deploy \"use node\" actions. \
                 Node.js v24 is not installed. \
                 Install Node.js v24 with nvm (https://github.com/nvm-sh/nvm) \
                  to deploy Node.js actions."
            ))
        }
        Ok(())
    }

    async fn check_server_health(client: &Client, timeout: Duration) -> Option<NodeExecutorHealth> {
        let mut response = match client
            .get("http://localhost/health")
            .timeout(timeout)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => response,
            _ => return None,
        };
        if response
            .content_length()
            .is_some_and(|length| length > MAX_HEALTH_RESPONSE_BYTES as u64)
        {
            return None;
        }
        // User modules share the process and can replace serialization globals.
        // Bound this watchdog input before accumulating and parsing it.
        let mut body = Vec::new();
        loop {
            let Some(chunk) = response.chunk().await.ok()? else {
                break;
            };
            let body_len = body.len().checked_add(chunk.len())?;
            if body_len > MAX_HEALTH_RESPONSE_BYTES {
                return None;
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&body).ok()
    }

    async fn prepare_package(
        &self,
        descriptor: &PreparationDescriptor,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let request = serde_json::json!({
            "sourcePackage": JsonValue::from(descriptor.source_package.clone()),
        });
        let deadline = tokio::time::Instant::now() + timeout;
        let mut response = tokio::time::timeout_at(
            deadline,
            self.client
                .post("http://localhost/prepare")
                .json(&request)
                .send(),
        )
        .await
        .map_err(|_| CandidatePreparationTimedOut)?
        .map_err(|_| anyhow::anyhow!("Local Node executor package preparation request failed"))?;
        anyhow::ensure!(
            response.status().is_success(),
            "Local Node executor package preparation failed"
        );
        anyhow::ensure!(
            response
                .content_length()
                .is_none_or(|length| length <= MAX_PREPARATION_RESPONSE_BYTES as u64),
            "Local Node executor package preparation response exceeded its size limit"
        );
        let mut body = Vec::new();
        loop {
            let chunk = tokio::time::timeout_at(deadline, response.chunk())
                .await
                .map_err(|_| CandidatePreparationTimedOut)?
                .map_err(|_| {
                    anyhow::anyhow!("Local Node executor package preparation response failed")
                })?;
            let Some(chunk) = chunk else {
                break;
            };
            let body_len = body
                .len()
                .checked_add(chunk.len())
                .context("Local Node executor preparation response size overflow")?;
            anyhow::ensure!(
                body_len <= MAX_PREPARATION_RESPONSE_BYTES,
                "Local Node executor package preparation response exceeded its size limit"
            );
            body.extend_from_slice(&chunk);
        }
        match serde_json::from_slice::<PreparationResponse>(&body)
            .map_err(|_| anyhow::anyhow!("Invalid local Node executor preparation response"))?
        {
            PreparationResponse::Success => Ok(()),
            PreparationResponse::Error => {
                anyhow::bail!("Local Node executor package preparation failed")
            },
        }
    }

    async fn terminate(&self) -> anyhow::Result<ChildTerminationObservation> {
        // Request admission can outlive removal from the executor state. Wake
        // any pre-start owner before termination so it cannot authorize a
        // durable claim for a child already selected for hard retirement.
        self.mark_execution_unavailable();
        #[cfg(test)]
        if self
            .termination_failures_remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            anyhow::bail!("Injected local Node executor termination failure");
        }
        let mut child = self.server_handle.lock().await;
        child.terminate().await
    }

    async fn terminate_for_hot_cleanup(
        &self,
    ) -> anyhow::Result<Option<ChildTerminationObservation>> {
        self.mark_execution_unavailable();
        #[cfg(test)]
        if self
            .termination_failures_remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            anyhow::bail!("Injected local Node executor termination failure");
        }
        let mut child = self.server_handle.lock().await;
        child.terminate_if_needed().await
    }

    async fn owns_child_pid(&self) -> bool {
        self.server_handle
            .lock()
            .await
            .child
            .as_ref()
            .and_then(Child::id)
            == Some(self.pid)
    }

    async fn read_owned_process_stat(&self) -> anyhow::Result<Option<ProcessStatSnapshot>> {
        anyhow::ensure!(
            self.owns_child_pid().await,
            "Local Node child was reaped before process sampling"
        );
        let process = read_process_stat(self.pid).await?;
        // Retirement may reap the child while /proc is being read. Verify the
        // owner again so PID reuse can never turn another process into evidence
        // for this generation.
        anyhow::ensure!(
            self.owns_child_pid().await,
            "Local Node child was reaped during process sampling"
        );
        Ok(process)
    }

    async fn read_owned_thread_stats(
        &self,
        expected_start_time_ticks: u64,
    ) -> anyhow::Result<Option<(Vec<ThreadStatSnapshot>, bool)>> {
        anyhow::ensure!(
            self.owns_child_pid().await,
            "Local Node child was reaped before thread sampling"
        );
        let threads = read_thread_stats(self.pid, expected_start_time_ticks).await?;
        anyhow::ensure!(
            self.owns_child_pid().await,
            "Local Node child was reaped during thread sampling"
        );
        Ok(threads)
    }

    async fn request_diagnostic_report(self: &Arc<Self>) -> FirstMissDiagnosticOutcome {
        let report = self
            .diagnostic_paths
            .as_ref()
            .expect("First-miss report request has no diagnostic paths")
            .report
            .clone();
        let report_source_path = report.source_path.clone();
        let setup_source_owner = self.clone();
        let source_identity = match tokio::time::timeout(
            DIAGNOSTIC_FILESYSTEM_TIMEOUT,
            tokio::task::spawn_blocking(move || {
                let result = prepare_private_diagnostic_file(&report_source_path);
                // The blocking create can continue after its async timeout.
                drop(setup_source_owner);
                result
            }),
        )
        .await
        {
            Ok(Ok(Ok(source_identity))) => source_identity,
            Err(_) | Ok(Err(_)) | Ok(Ok(Err(_))) => {
                return FirstMissDiagnosticOutcome::DiagnosticReportRequestFailed;
            },
        };
        let child = self.server_handle.lock().await;
        let Some(pid) = child.child.as_ref().and_then(Child::id) else {
            return FirstMissDiagnosticOutcome::DiagnosticReportRequestFailed;
        };
        if pid != self.pid {
            return FirstMissDiagnosticOutcome::DiagnosticReportInvalidPid;
        }
        // ManagedChild retains an exited child until it is reaped, so this PID
        // cannot be reused while the owner lock is held.
        let outcome = trigger_node_diagnostic_report(pid);
        drop(child);
        if matches!(
            outcome,
            FirstMissDiagnosticOutcome::DiagnosticReportRequested
        ) {
            let source_owner = self.clone();
            let pool_name = self.pool_name.clone();
            tokio::spawn(async move {
                let publication_outcome = if publish_node_diagnostic_report(
                    report,
                    source_identity,
                    source_owner,
                )
                .await
                {
                    FirstMissDiagnosticOutcome::DiagnosticReportCompleted
                } else {
                    tracing::warn!("Failed to publish local Node diagnostic report");
                    FirstMissDiagnosticOutcome::DiagnosticReportWriteFailed
                };
                crate::metrics::log_local_node_first_miss_diagnostic(
                    &pool_name,
                    publication_outcome,
                );
            });
        }
        outcome
    }

    async fn terminate_child(
        pool_name: &str,
        generation: u64,
        child: &mut Child,
    ) -> anyhow::Result<ChildTerminationObservation> {
        let state_before = match child.try_wait() {
            Ok(Some(status)) => {
                let exit_class = Self::record_child_exit(pool_name, status);
                return Ok(ChildTerminationObservation {
                    state_before: "already_exited",
                    supervisor_kill_requested: false,
                    exit_class,
                });
            },
            Ok(None) => "running",
            Err(error) => {
                tracing::warn!(
                    pool_name,
                    generation,
                    error_kind = ?error.kind(),
                    "Failed to inspect local Node executor child before termination"
                );
                "probe_failed"
            },
        };
        let supervisor_kill_requested = match child.start_kill() {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => {
                // The operator or the process itself may have won the exit
                // race. Waiting below still reaps the child and records its
                // exit class.
                false
            },
            Err(error) => {
                anyhow::bail!(
                    "Failed to terminate local Node executor generation {generation}: {:?}",
                    error.kind()
                );
            },
        };
        let status = child.wait().await.map_err(|error| {
            anyhow::anyhow!(
                "Failed to reap local Node executor generation {generation}: {:?}",
                error.kind()
            )
        })?;
        let exit_class = Self::record_child_exit(pool_name, status);
        Ok(ChildTerminationObservation {
            state_before,
            supervisor_kill_requested,
            exit_class,
        })
    }

    fn record_child_exit(pool_name: &str, status: ExitStatus) -> &'static str {
        let exit_class = if status.success() {
            "success"
        } else if status.code().is_some() {
            "failure"
        } else {
            "signal"
        };
        crate::metrics::log_local_node_child_exit(pool_name, exit_class);
        exit_class
    }

    async fn start_node_with_listener(
        config: &LocalNodeExecutorConfig,
        source_path: &Path,
        temp_dir: &TempDir,
        socket_path: &Path,
        diagnostic_paths: Option<&NodeDiagnosticPaths>,
    ) -> anyhow::Result<Child> {
        let preferred_node_version = NVMRC_VERSION.trim();

        // Look for node in a few places.
        let possible_path = home::home_dir().map(|home| {
            home.join(".nvm")
                .join(format!("versions/node/v{preferred_node_version}/bin/node"))
        });
        let node_path = possible_path
            .filter(|path| path.exists())
            .unwrap_or_else(|| PathBuf::from("node"));
        Self::check_node_version(&node_path).await?;

        let mut cmd = TokioCommand::new(node_path);
        cmd.arg(format!(
            "--max-old-space-size={}",
            config.max_old_space_size_mib
        ));
        #[cfg(unix)]
        if let Some(report) = diagnostic_paths.map(|paths| &paths.report) {
            cmd.arg("--report-on-signal")
                .arg("--report-signal=SIGUSR2")
                .arg("--report-exclude-env")
                .arg("--report-exclude-network")
                .arg("--report-directory")
                .arg(temp_dir.path())
                .arg("--report-filename")
                .arg(&report.filename);
        }
        cmd.arg(source_path)
            .arg("--ipc-path")
            .arg(socket_path)
            .arg("--tempdir")
            .arg(temp_dir.path());
        #[cfg(unix)]
        if let Some(diagnostic_paths) = diagnostic_paths {
            cmd.arg("--diagnostic-control-path")
                .arg(&diagnostic_paths.control_path)
                .arg("--diagnostic-profile-path")
                .arg(&diagnostic_paths.profile_source_path)
                .arg("--diagnostic-profile-duration-ms")
                .arg(DIAGNOSTIC_PROFILE_DURATION_MS.to_string());
        }
        #[cfg(not(unix))]
        let _ = diagnostic_paths;
        // Function console output uses the bounded response protocol.
        // Do not let direct user writes bypass it into infrastructure logs.
        cmd.stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if let Some(backoff) = config.callback_initial_backoff {
            cmd.env(
                "CALLBACK_INITIAL_BACKOFF_MS",
                backoff.as_millis().to_string(),
            );
        }

        let child = cmd.spawn()?;

        Ok(child)
    }
}

impl LocalNodeExecutor {
    pub(crate) fn pool_name(&self) -> &str {
        &self.config.pool_name
    }

    pub(crate) fn begin_close_for_topology_change(&self) {
        self.shutting_down.store(true, Ordering::Release);
    }

    pub(crate) async fn finish_close_for_topology_change(&self) -> anyhow::Result<()> {
        // Child startup is intentionally outside the generation-state mutex. Wait
        // for it here so a topology transition cannot finish before an already
        // selected request either publishes its child or observes shutdown.
        let _startup_guard = self.startup_lock.lock().await;
        // A removed pool has no use for an unpromoted candidate, and a
        // promoted old generation must not keep draining after pool cleanup
        // has requested immediate termination.
        Self::preempt_hot_transition_state(&self.state, "shutdown_canceled").await;
        let (expected, already_retiring) = {
            let state = self.state.lock().await;
            match (&state.inner, &state.retiring) {
                (Some(inner), None) => (Some(inner.clone()), false),
                (None, Some(retiring)) => (Some(retiring.clone()), true),
                (None, None) => (None, false),
                (Some(_), Some(_)) => {
                    unreachable!("Local Node executor has current and retiring generations")
                },
            }
        };
        let Some(expected) = expected else {
            return self.wait_for_hot_transition_completion().await;
        };
        let diagnostics = GenerationRetirementDiagnostics::topology_change();
        let retirement_result = async {
            if already_retiring && self.shutdown_started.load(Ordering::Acquire) {
                // Removed-pool force, pressure, or router shutdown can retry a
                // failed exact retirement. Do not treat the earlier published
                // wait failure as permanent after immediate cleanup owns it.
                Self::finish_retiring_inner_for_shutdown(&self.state, &expected).await?;
                return anyhow::Ok(());
            }
            if Self::start_draining_inner_state(&self.state, &expected, diagnostics.reason).await {
                if !Self::finish_draining_inner_state(&self.state, &expected, diagnostics).await? {
                    // Health or request failure can preempt the graceful drain.
                    // The topology barrier must still wait for that owner's
                    // direct-child termination and reap before replacement.
                    expected.wait_until_retired().await?;
                }
            } else {
                expected.wait_until_retired().await?;
            }
            anyhow::Ok(())
        }
        .await;
        let transition_result = self.wait_for_hot_transition_completion().await;
        retirement_result?;
        transition_result
    }

    async fn wait_for_hot_transition_completion(&self) -> anyhow::Result<()> {
        Self::finish_hot_transition_cleanup_state(&self.state, "shutdown_canceled").await
    }

    async fn finish_hot_transition_cleanup_state(
        state: &Arc<Mutex<LocalNodeExecutorState>>,
        outcome: &'static str,
    ) -> anyhow::Result<()> {
        let mut retried_cleanup = false;
        loop {
            let state_guard = state.lock().await;
            let Some(transition) = &state_guard.hot_transition else {
                return Ok(());
            };
            let cleanup = match transition {
                HotTransition::Candidate(candidate) => candidate.cleanup.clone(),
                HotTransition::Draining(draining) => draining.cleanup.clone(),
            };
            drop(state_guard);
            match cleanup.cleanup(outcome).await {
                Ok(()) => retried_cleanup = false,
                Err(_) if !retried_cleanup => {
                    retried_cleanup = true;
                    continue;
                },
                Err(error) => return Err(error),
            }
        }
    }

    async fn wait_for_hot_transition_status_completion(
        &self,
        expected_status: &Arc<HotTransitionStatus>,
    ) -> anyhow::Result<()> {
        loop {
            let changed = self.transition_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let state = self.state.lock().await;
            let current_status = state
                .hot_transition
                .as_ref()
                .map(|transition| match transition {
                    HotTransition::Candidate(candidate) => &candidate.status,
                    HotTransition::Draining(draining) => &draining.status,
                });
            if !current_status.is_some_and(|status| Arc::ptr_eq(status, expected_status)) {
                return Ok(());
            }
            anyhow::ensure!(
                !expected_status.cleanup_failed.load(Ordering::Acquire),
                "Local Node executor deployment cleanup transition failed"
            );
            drop(state);
            changed.await;
        }
    }

    pub(crate) async fn has_resident_generation(&self) -> bool {
        self.state.lock().await.inner.is_some()
    }

    pub(crate) async fn resident_fingerprint_matches(
        &self,
        target: &ResidentGenerationFingerprint,
    ) -> bool {
        self.state
            .lock()
            .await
            .inner
            .as_ref()
            .and_then(|inner| inner.resident_fingerprint.as_ref())
            .is_some_and(|current| current.same_package_and_environment(target))
    }

    pub(crate) async fn wait_for_resident_package_and_environment_ready(
        &self,
        target: &ResidentGenerationFingerprint,
    ) {
        loop {
            let changed = self.transition_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let state = self.state.lock().await;
            let target_ready = state.inner.as_ref().is_some_and(|current| {
                current
                    .resident_fingerprint
                    .as_ref()
                    .is_some_and(|resident| resident.same_package_and_environment(target))
                    && !current.retirement_requested.load(Ordering::Acquire)
                    && !current.execution_unavailable.load(Ordering::Acquire)
            });
            if target_ready {
                return;
            }
            drop(state);
            changed.await;
        }
    }

    pub(crate) async fn replace_for_deployment(
        &self,
        target_fingerprint: Option<ResidentGenerationFingerprint>,
        source_package: crate::executor::SourcePackage,
        reserved_permit: SurgePermit,
    ) -> anyhow::Result<DeploymentReplacementOutcome> {
        let mut reserved_permit = Some(reserved_permit);
        let expected = self.state.lock().await.inner.clone();
        let Some(expected) = expected else {
            reserved_permit
                .take()
                .expect("Local Node deployment permit is missing")
                .release();
            return Ok(DeploymentReplacementOutcome::Reused);
        };
        if same_resident_package_and_environment(
            expected.resident_fingerprint.as_ref(),
            target_fingerprint.as_ref(),
        ) {
            let matching_drain_status = match &self.state.lock().await.hot_transition {
                Some(HotTransition::Draining(draining)) => Some(draining.status.clone()),
                Some(HotTransition::Candidate(_)) | None => None,
            };
            reserved_permit
                .take()
                .expect("Local Node deployment permit is missing")
                .release();
            // A matching generation can already be the promoted side of a hot
            // transition. Wait only for that drain: an unpromoted candidate
            // has no extra old child and may itself be queued behind this
            // deployment lease.
            if let Some(status) = matching_drain_status {
                self.wait_for_hot_transition_status_completion(&status)
                    .await?;
            }
            return Ok(DeploymentReplacementOutcome::Reused);
        }
        loop {
            let changed = self.transition_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let disposition = {
                let state = self.state.lock().await;
                if !state
                    .inner
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, &expected))
                {
                    DeploymentTransitionDisposition::Conflict
                } else {
                    match &state.hot_transition {
                        None => DeploymentTransitionDisposition::Start,
                        Some(HotTransition::Candidate(candidate))
                            if candidate.status.failed.load(Ordering::Acquire)
                                || candidate.status.cleanup_failed.load(Ordering::Acquire) =>
                        {
                            DeploymentTransitionDisposition::Conflict
                        },
                        Some(HotTransition::Draining(draining))
                            if draining.status.failed.load(Ordering::Acquire)
                                || draining.status.cleanup_failed.load(Ordering::Acquire) =>
                        {
                            DeploymentTransitionDisposition::Conflict
                        },
                        Some(HotTransition::Candidate(candidate))
                            if matches!(
                                candidate.reason,
                                GenerationRetirementReason::RssLimit
                                    | GenerationRetirementReason::AgeLimit
                                    | GenerationRetirementReason::PackageLimit
                            ) =>
                        {
                            candidate.canceled.store(true, Ordering::Release);
                            candidate.canceled_changed.notify_waiters();
                            DeploymentTransitionDisposition::WaitForRoutineCancellation
                        },
                        Some(HotTransition::Candidate(candidate))
                            if Arc::ptr_eq(&candidate.expected, &expected)
                                && same_resident_package_and_environment(
                                    candidate.target_fingerprint.as_ref(),
                                    target_fingerprint.as_ref(),
                                ) =>
                        {
                            DeploymentTransitionDisposition::Join(candidate.status.clone())
                        },
                        Some(HotTransition::Draining(draining))
                            if state.inner.as_ref().is_some_and(|current| {
                                same_resident_package_and_environment(
                                    current.resident_fingerprint.as_ref(),
                                    target_fingerprint.as_ref(),
                                )
                            }) =>
                        {
                            DeploymentTransitionDisposition::Join(draining.status.clone())
                        },
                        Some(HotTransition::Candidate(_) | HotTransition::Draining(_)) => {
                            DeploymentTransitionDisposition::Conflict
                        },
                    }
                }
            };
            match disposition {
                DeploymentTransitionDisposition::Start => break,
                DeploymentTransitionDisposition::WaitForRoutineCancellation => changed.await,
                DeploymentTransitionDisposition::Join(status) => {
                    reserved_permit
                        .take()
                        .expect("Local Node deployment permit is missing")
                        .release();
                    loop {
                        let changed = self.transition_changed.notified();
                        tokio::pin!(changed);
                        changed.as_mut().enable();
                        {
                            let state = self.state.lock().await;
                            if state.inner.as_ref().is_some_and(|current| {
                                same_resident_package_and_environment(
                                    current.resident_fingerprint.as_ref(),
                                    target_fingerprint.as_ref(),
                                )
                            }) {
                                drop(state);
                                self.wait_for_hot_transition_status_completion(&status)
                                    .await?;
                                return Ok(DeploymentReplacementOutcome::Promoted);
                            }
                            anyhow::ensure!(
                                !status.failed.load(Ordering::Acquire)
                                    && !status.cleanup_failed.load(Ordering::Acquire),
                                "Local Node executor deployment candidate task failed"
                            );
                        }
                        changed.await;
                    }
                },
                DeploymentTransitionDisposition::Conflict => {
                    reserved_permit
                        .take()
                        .expect("Local Node deployment permit is missing")
                        .release();
                    anyhow::bail!(
                        "Local Node executor deployment cutover conflicts with another transition"
                    );
                },
            }
        }
        let started_status = Self::request_hot_replacement_state(
            &self.state,
            &self.transition_changed,
            &self.shutting_down,
            &self.activity,
            &self.config,
            &expected,
            target_fingerprint.clone(),
            Some(PreparationDescriptor { source_package }),
            GenerationRetirementReason::TopologyChange,
            reserved_permit.take(),
        )
        .await
        .context(
            "Local Node executor deployment cutover could not start its reserved transition",
        )?;
        loop {
            let changed = self.transition_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.state.lock().await;
                if state.inner.as_ref().is_some_and(|current| {
                    same_resident_package_and_environment(
                        current.resident_fingerprint.as_ref(),
                        target_fingerprint.as_ref(),
                    )
                }) {
                    drop(state);
                    self.wait_for_hot_transition_status_completion(&started_status)
                        .await?;
                    return Ok(DeploymentReplacementOutcome::Promoted);
                }
                anyhow::ensure!(
                    !started_status.failed.load(Ordering::Acquire)
                        && !started_status.cleanup_failed.load(Ordering::Acquire),
                    "Local Node executor deployment candidate task failed"
                );
                anyhow::ensure!(
                    state
                        .inner
                        .as_ref()
                        .is_some_and(|current| Arc::ptr_eq(current, &expected)),
                    "Local Node executor deployment candidate was superseded before promotion"
                );
                let transition_status =
                    state
                        .hot_transition
                        .as_ref()
                        .map(|transition| match transition {
                            HotTransition::Candidate(candidate) => &candidate.status,
                            HotTransition::Draining(draining) => &draining.status,
                        });
                anyhow::ensure!(
                    transition_status.is_some_and(|status| Arc::ptr_eq(status, &started_status)),
                    "Local Node executor deployment candidate ended before promotion"
                );
            }
            changed.await;
        }
    }

    pub async fn new(node_process_timeout: Duration) -> anyhow::Result<Self> {
        Self::new_with_memory_pressure(node_process_timeout, MemoryPressureSignal::default()).await
    }

    pub fn preflight_configuration(
        node_process_timeout: Duration,
        memory_pressure: MemoryPressureSignal,
    ) -> anyhow::Result<LocalNodeExecutorConfig> {
        let config = LocalNodeExecutorConfig {
            pool_name: Arc::from("default"),
            node_process_timeout,
            callback_initial_backoff: None,
            health_check_timeout: HEALTH_CHECK_TIMEOUT,
            watchdog_interval: WATCHDOG_INTERVAL,
            watchdog_failure_threshold: WATCHDOG_FAILURE_THRESHOLD,
            max_old_space_size_mib: *LOCAL_NODE_EXECUTOR_MAX_OLD_SPACE_SIZE_MIB,
            max_rss_bytes: u64::try_from(*LOCAL_NODE_EXECUTOR_MAX_RSS_BYTES)
                .context("Local Node executor RSS threshold does not fit u64")?,
            memory_pressure,
            memory_pressure_min_rss_bytes: u64::try_from(
                *LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_MIN_RSS_BYTES,
            )
            .context("Local Node executor cgroup-pressure RSS threshold does not fit u64")?,
            memory_pressure_grace: *LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_GRACE,
            max_generation_age: *LOCAL_NODE_EXECUTOR_MAX_GENERATION_AGE,
            max_imported_source_packages: u64::try_from(
                *LOCAL_NODE_EXECUTOR_MAX_IMPORTED_SOURCE_PACKAGES,
            )
            .context("Local Node executor package threshold does not fit u64")?,
            diagnostics_dir: None,
            diagnostic_pruning_in_progress: Arc::new(AtomicBool::new(false)),
            surge_coordinator: SurgeCoordinator::new(),
        };
        config.validate()?;
        Ok(config)
    }

    pub async fn new_with_memory_pressure(
        node_process_timeout: Duration,
        memory_pressure: MemoryPressureSignal,
    ) -> anyhow::Result<Self> {
        let config = Self::preflight_configuration(node_process_timeout, memory_pressure)?;
        Self::new_with_configuration(config).await
    }

    pub async fn new_with_configuration(
        mut config: LocalNodeExecutorConfig,
    ) -> anyhow::Result<Self> {
        let pool_name = config.pool_name.clone();
        crate::metrics::initialize_local_node_first_miss_diagnostic_counters(&pool_name);
        config.diagnostics_dir = prepare_diagnostic_directory(&pool_name).await;
        let activity = Arc::new(ExecutorPoolActivity {
            pool_name: pool_name.clone(),
            waiting_requests: AtomicUsize::new(0),
            active_requests: AtomicUsize::new(0),
        });
        let executor = Self {
            state: Arc::new(Mutex::new(LocalNodeExecutorState::default())),
            transition_changed: Arc::new(Notify::new()),
            startup_lock: Mutex::new(()),
            shutting_down: Arc::new(AtomicBool::new(false)),
            shutdown_started: Arc::new(AtomicBool::new(false)),
            activity,
            config,
        };

        crate::metrics::set_local_node_generation_present(&pool_name, false);
        crate::metrics::set_local_node_generation_age(&pool_name, Duration::ZERO);
        crate::metrics::set_local_node_generation_draining(&pool_name, false);
        crate::metrics::set_local_node_candidate_present(&pool_name, false);
        crate::metrics::set_local_node_child_rss(&pool_name, None);
        crate::metrics::set_local_node_waiting_requests(&pool_name, 0);
        crate::metrics::set_local_node_active_requests(&pool_name, 0);
        crate::metrics::set_local_node_consecutive_health_misses(&pool_name, 0);
        crate::metrics::set_local_node_memory_pressure_active(&pool_name, false);
        crate::metrics::set_local_node_memory_configuration(
            &pool_name,
            executor.config.old_space_bytes(),
            executor.config.max_rss_bytes,
            executor.config.memory_pressure_min_rss_bytes,
            executor.config.memory_pressure_grace,
            executor.config.max_generation_age,
            executor.config.max_imported_source_packages,
        );

        Ok(executor)
    }

    async fn request_hot_replacement(
        &self,
        expected: &Arc<InnerLocalNodeExecutor>,
        target_fingerprint: Option<ResidentGenerationFingerprint>,
        descriptor: Option<PreparationDescriptor>,
        reason: GenerationRetirementReason,
    ) -> Option<Arc<HotTransitionStatus>> {
        Self::request_hot_replacement_state(
            &self.state,
            &self.transition_changed,
            &self.shutting_down,
            &self.activity,
            &self.config,
            expected,
            target_fingerprint,
            descriptor,
            reason,
            None,
        )
        .await
    }

    async fn request_hot_replacement_state(
        state: &Arc<Mutex<LocalNodeExecutorState>>,
        transition_changed: &Arc<Notify>,
        shutting_down: &Arc<AtomicBool>,
        activity: &Arc<ExecutorPoolActivity>,
        config: &LocalNodeExecutorConfig,
        expected: &Arc<InnerLocalNodeExecutor>,
        target_fingerprint: Option<ResidentGenerationFingerprint>,
        descriptor: Option<PreparationDescriptor>,
        reason: GenerationRetirementReason,
        mut reserved_permit: Option<SurgePermit>,
    ) -> Option<Arc<HotTransitionStatus>> {
        let (token, canceled, canceled_changed, status, cleanup) = {
            let mut state_guard = state.lock().await;
            if shutting_down.load(Ordering::Acquire)
                || config.memory_pressure.is_active()
                || !state_guard
                    .inner
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, expected))
            {
                if let Some(permit) = reserved_permit.take() {
                    permit.release();
                }
                return None;
            }
            if let Some(transition) = &mut state_guard.hot_transition {
                let mut matching_status = None;
                if let HotTransition::Candidate(candidate) = transition
                    && Arc::ptr_eq(&candidate.expected, expected)
                    && matches!(
                        candidate.reason,
                        GenerationRetirementReason::FingerprintChange
                    )
                    && matches!(reason, GenerationRetirementReason::FingerprintChange)
                {
                    let replaces_target = match (
                        candidate.target_fingerprint.as_ref(),
                        target_fingerprint.as_ref(),
                    ) {
                        (Some(current), Some(incoming)) => {
                            incoming.topology_version >= current.topology_version
                        },
                        (None, Some(_)) => true,
                        _ => false,
                    };
                    let same_target = same_resident_package_and_environment(
                        candidate.target_fingerprint.as_ref(),
                        target_fingerprint.as_ref(),
                    );
                    if replaces_target && !candidate.startup_started {
                        candidate.target_fingerprint = target_fingerprint;
                        if same_target {
                            if let Some(descriptor) = descriptor {
                                PreparationDescriptor::retain_fresher(
                                    &mut candidate.descriptor,
                                    descriptor,
                                );
                            }
                        } else {
                            candidate.descriptor = descriptor;
                        }
                        matching_status = Some(candidate.status.clone());
                    } else if same_target {
                        // Once startup captures a fingerprint, changing its
                        // target would guarantee stale promotion. Requests for
                        // the same package and environment can still join it.
                        if let Some(descriptor) = descriptor {
                            PreparationDescriptor::retain_fresher(
                                &mut candidate.descriptor,
                                descriptor,
                            );
                        }
                        matching_status = Some(candidate.status.clone());
                    }
                }
                if let Some(permit) = reserved_permit.take() {
                    permit.release();
                }
                return matching_status;
            }
            state_guard.next_transition = state_guard
                .next_transition
                .checked_add(1)
                .expect("Local Node executor transition id overflow");
            let token = state_guard.next_transition;
            let canceled = Arc::new(AtomicBool::new(false));
            let canceled_changed = Arc::new(Notify::new());
            let status = Arc::new(HotTransitionStatus {
                failed: AtomicBool::new(false),
                promoted: AtomicBool::new(false),
                cleanup_failed: AtomicBool::new(false),
            });
            let cleanup = HotTransitionCleanupOwner::new(
                token,
                state,
                transition_changed.clone(),
                status.clone(),
                reason,
                config.pool_name.clone(),
            );
            state_guard.hot_transition = Some(HotTransition::Candidate(CandidateTransition {
                token,
                expected: expected.clone(),
                target_fingerprint,
                descriptor,
                startup_started: false,
                reason,
                canceled: canceled.clone(),
                canceled_changed: canceled_changed.clone(),
                status: status.clone(),
                cleanup: cleanup.clone(),
            }));
            crate::metrics::set_local_node_candidate_present(&config.pool_name, true);
            (token, canceled, canceled_changed, status, cleanup)
        };
        let state = state.clone();
        let transition_changed = transition_changed.clone();
        let shutting_down = shutting_down.clone();
        let activity = activity.clone();
        let config = config.clone();
        let expected = expected.clone();
        let task_status = status.clone();
        // Capture the guard before spawning so runtime shutdown cannot drop an
        // unpolled transition task without publishing cleanup ownership.
        let task_guard = HotTransitionTaskGuard::new(
            task_status.clone(),
            transition_changed.clone(),
            expected.clone(),
            reason,
            cleanup.clone(),
        );
        tokio::spawn(async move {
            Self::run_hot_replacement(
                state,
                transition_changed,
                shutting_down,
                activity,
                config,
                expected,
                token,
                canceled,
                canceled_changed,
                task_status,
                cleanup,
                reason,
                reserved_permit,
                task_guard,
            )
            .await;
        });
        Some(status)
    }

    async fn run_hot_replacement(
        state: Arc<Mutex<LocalNodeExecutorState>>,
        transition_changed: Arc<Notify>,
        shutting_down: Arc<AtomicBool>,
        activity: Arc<ExecutorPoolActivity>,
        config: LocalNodeExecutorConfig,
        expected: Arc<InnerLocalNodeExecutor>,
        token: u64,
        canceled: Arc<AtomicBool>,
        canceled_changed: Arc<Notify>,
        status: Arc<HotTransitionStatus>,
        cleanup: Arc<HotTransitionCleanupOwner>,
        reason: GenerationRetirementReason,
        reserved_permit: Option<SurgePermit>,
        mut task_guard: HotTransitionTaskGuard,
    ) {
        let priority = if matches!(
            reason,
            GenerationRetirementReason::FingerprintChange
                | GenerationRetirementReason::TopologyChange
        ) {
            SurgePriority::Deployment
        } else {
            SurgePriority::Routine
        };
        let permit = match reserved_permit {
            Some(permit) => permit,
            None => {
                let acquire = config
                    .surge_coordinator
                    .acquire(priority, config.pool_name.clone());
                tokio::pin!(acquire);
                let cancellation = wait_for_atomic_flag(&canceled, &canceled_changed);
                tokio::pin!(cancellation);
                match tokio::select! {
                    permit = &mut acquire => Some(permit),
                    _ = &mut cancellation => None,
                } {
                    Some(permit) => permit,
                    None => {
                        let outcome = Self::candidate_cancellation_outcome(&shutting_down, &config);
                        cleanup.finish_candidate_startup();
                        let _ = cleanup.cleanup(outcome).await;
                        task_guard.disarm();
                        return;
                    },
                }
            },
        };
        cleanup.retain_permit(&permit, config.memory_pressure.clone());
        permit.set_phase("candidate");
        if permit.preempted()
            || canceled.load(Ordering::Acquire)
            || shutting_down.load(Ordering::Acquire)
            || config.memory_pressure.is_active()
        {
            let outcome = Self::candidate_cancellation_outcome(&shutting_down, &config);
            cleanup.finish_candidate_startup();
            let _ = cleanup.cleanup(outcome).await;
            task_guard.disarm();
            return;
        }
        let preparation_url_expired = {
            let state_guard = state.lock().await;
            matches!(
                &state_guard.hot_transition,
                Some(HotTransition::Candidate(candidate))
                    if candidate.token == token
                        && candidate
                            .descriptor
                            .as_ref()
                            .is_some_and(PreparationDescriptor::is_expired)
            )
        };
        if preparation_url_expired {
            cleanup.finish_candidate_startup();
            let _ = cleanup.cleanup("preparation_failed").await;
            task_guard.disarm();
            return;
        }
        let (generation, resident_fingerprint) = {
            let mut state_guard = state.lock().await;
            let current_matches = state_guard
                .inner
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &expected));
            let candidate = match &mut state_guard.hot_transition {
                Some(HotTransition::Candidate(candidate)) if candidate.token == token => candidate,
                Some(HotTransition::Candidate(_)) | Some(HotTransition::Draining(_)) | None => {
                    drop(state_guard);
                    cleanup.finish_candidate_startup();
                    let _ = cleanup.cleanup("stale").await;
                    task_guard.disarm();
                    return;
                },
            };
            if !current_matches {
                drop(state_guard);
                cleanup.finish_candidate_startup();
                let _ = cleanup.cleanup("stale").await;
                task_guard.disarm();
                return;
            }
            candidate.startup_started = true;
            let resident_fingerprint = candidate.target_fingerprint.clone();
            state_guard.next_generation = state_guard
                .next_generation
                .checked_add(1)
                .expect("Local Node executor generation overflow");
            (state_guard.next_generation, resident_fingerprint)
        };
        tracing::info!(
            pool_name = %config.pool_name,
            generation,
            replaces_generation = expected.generation,
            reason = reason.as_str(),
            "Starting local Node executor candidate"
        );
        let started_at = Instant::now();
        let startup_cancellation = CandidateStartupCancellation {
            canceled: canceled.clone(),
            preemption: permit.inner.preemption.clone(),
            shutting_down: shutting_down.clone(),
            memory_pressure: config.memory_pressure.clone(),
        };
        let candidate = match InnerLocalNodeExecutor::new(
            generation,
            resident_fingerprint,
            activity,
            &config,
            Some(&startup_cancellation),
            Some(cleanup.as_ref()),
        )
        .await
        {
            Ok(candidate) => {
                let candidate = Arc::new(candidate);
                cleanup.attach_candidate(candidate.clone());
                candidate
            },
            Err(error) => {
                cleanup.finish_candidate_startup();
                let outcome =
                    if let Some(canceled) = error.downcast_ref::<CandidateStartupCanceled>() {
                        canceled.outcome
                    } else if error.is::<CandidateStartupHealthFailed>() {
                        "health_failed"
                    } else {
                        "startup_failed"
                    };
                tracing::warn!(
                    pool_name = %config.pool_name,
                    generation,
                    reason = reason.as_str(),
                    outcome,
                    "Local Node executor candidate startup ended before readiness"
                );
                let _ = cleanup.cleanup(outcome).await;
                task_guard.disarm();
                return;
            },
        };
        let descriptor = {
            let state_guard = state.lock().await;
            match &state_guard.hot_transition {
                Some(HotTransition::Candidate(transition)) if transition.token == token => {
                    transition.descriptor.clone()
                },
                Some(HotTransition::Candidate(_)) | Some(HotTransition::Draining(_)) | None => None,
            }
        };
        let package_preparation_required = descriptor.is_some();
        if permit.preempted()
            || canceled.load(Ordering::Acquire)
            || shutting_down.load(Ordering::Acquire)
            || config.memory_pressure.is_active()
        {
            let outcome = Self::candidate_cancellation_outcome(&shutting_down, &config);
            let _ = cleanup.cleanup(outcome).await;
            task_guard.disarm();
            return;
        }
        if let Some(descriptor) = descriptor {
            let preparation_started = Instant::now();
            if descriptor.is_expired() {
                crate::metrics::log_local_node_candidate_preparation(
                    &config.pool_name,
                    preparation_started.elapsed(),
                    "failed",
                );
                let _ = cleanup.cleanup("preparation_failed").await;
                task_guard.disarm();
                return;
            }
            let preparation_descriptor = descriptor.clone();
            let preparation =
                candidate.prepare_package(&preparation_descriptor, config.node_process_timeout);
            tokio::pin!(preparation);
            let cancellation = wait_for_atomic_flag(&canceled, &canceled_changed);
            tokio::pin!(cancellation);
            let preparation_result = tokio::select! {
                result = &mut preparation => Some(result),
                _ = permit.wait_until_preempted() => None,
                _ = &mut cancellation => None,
            };
            if preparation_result.is_none() {
                let outcome = Self::candidate_cancellation_outcome(&shutting_down, &config);
                crate::metrics::log_local_node_candidate_preparation(
                    &config.pool_name,
                    preparation_started.elapsed(),
                    "canceled",
                );
                let _ = cleanup.cleanup(outcome).await;
                task_guard.disarm();
                return;
            }
            if let Err(error) = preparation_result.expect("Checked preparation result is missing") {
                let preparation_outcome = if error.is::<CandidatePreparationTimedOut>() {
                    "timed_out"
                } else {
                    "failed"
                };
                crate::metrics::log_local_node_candidate_preparation(
                    &config.pool_name,
                    preparation_started.elapsed(),
                    preparation_outcome,
                );
                tracing::warn!(
                    pool_name = %config.pool_name,
                    generation,
                    reason = reason.as_str(),
                    outcome = "preparation_failed",
                    "Local Node executor candidate preparation failed"
                );
                let _ = cleanup.cleanup("preparation_failed").await;
                task_guard.disarm();
                return;
            }
            *candidate
                .preparation_descriptor
                .lock()
                .expect("Local Node preparation descriptor lock poisoned") = Some(descriptor);
            crate::metrics::log_local_node_candidate_preparation(
                &config.pool_name,
                preparation_started.elapsed(),
                "ready",
            );
        }
        tracing::info!(
            pool_name = %config.pool_name,
            generation,
            reason = reason.as_str(),
            package_prepared = package_preparation_required,
            "Local Node executor candidate is ready for promotion"
        );
        let promoted = {
            let mut state_guard = state.lock().await;
            let transition_matches = matches!(
                &state_guard.hot_transition,
                Some(HotTransition::Candidate(transition))
                    if transition.token == token
                        && Arc::ptr_eq(&transition.expected, &expected)
                        && Arc::ptr_eq(&transition.cleanup, &cleanup)
                        && transition.target_fingerprint == candidate.resident_fingerprint
            );
            if transition_matches
                && !canceled.load(Ordering::Acquire)
                && !permit.preempted()
                && !shutting_down.load(Ordering::Acquire)
                && !config.memory_pressure.is_active()
                && state_guard
                    .inner
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, &expected))
                && cleanup.promote_to_draining(expected.clone())
            {
                expected.retirement_requested.store(true, Ordering::Release);
                state_guard.inner = Some(candidate.clone());
                state_guard.hot_transition = Some(HotTransition::Draining(DrainingTransition {
                    token,
                    old: expected.clone(),
                    status: status.clone(),
                    cleanup: cleanup.clone(),
                }));
                permit.set_phase("draining");
                status.promoted.store(true, Ordering::Release);
                crate::metrics::set_local_node_candidate_present(&config.pool_name, false);
                crate::metrics::set_local_node_generation_draining(&config.pool_name, true);
                crate::metrics::set_local_node_generation_present(&config.pool_name, true);
                crate::metrics::set_local_node_generation_age(&config.pool_name, Duration::ZERO);
                crate::metrics::set_local_node_child_rss(&config.pool_name, None);
                crate::metrics::set_local_node_consecutive_health_misses(&config.pool_name, 0);
                if candidate.runtime_stats_supported {
                    crate::metrics::set_local_node_package_state(
                        &config.pool_name,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                    );
                }
                true
            } else {
                false
            }
        };
        if !promoted {
            let _ = cleanup.cleanup("stale").await;
            task_guard.disarm();
            return;
        }
        crate::metrics::log_local_node_generation_start(&config.pool_name);
        crate::metrics::log_local_node_replacement_time(&config.pool_name, started_at.elapsed());
        crate::metrics::log_local_node_replacement_outcome(&config.pool_name, "promoted");
        tracing::info!(
            pool_name = %config.pool_name,
            generation,
            replaces_generation = expected.generation,
            reason = reason.as_str(),
            "Promoted local Node executor candidate"
        );
        transition_changed.notify_waiters();
        Self::spawn_watchdog_state(
            &state,
            &candidate,
            config.clone(),
            transition_changed.clone(),
            shutting_down,
        );

        // Preemption can be published after promotion but before these waiters
        // register. Pair each notification with its atomic state so an old
        // request timeout or forced cutover cannot be lost in that interval.
        tokio::select! {
            _ = expected.wait_until_idle() => {},
            _ = wait_for_atomic_flag(
                &expected.terminate_draining,
                &expected.terminate_draining_notify,
            ) => {},
            _ = permit.wait_until_preempted() => {},
        }
        let _ = cleanup.cleanup("drained").await;
        task_guard.disarm();
    }

    fn candidate_cancellation_outcome(
        shutting_down: &AtomicBool,
        config: &LocalNodeExecutorConfig,
    ) -> &'static str {
        if shutting_down.load(Ordering::Acquire) {
            "shutdown_canceled"
        } else if config.memory_pressure.is_active() {
            "pressure_canceled"
        } else {
            "stale"
        }
    }

    async fn preempt_hot_transition_state(
        state: &Arc<Mutex<LocalNodeExecutorState>>,
        outcome: &'static str,
    ) {
        let state_guard = state.lock().await;
        let cleanup = match &state_guard.hot_transition {
            Some(HotTransition::Candidate(candidate)) => {
                candidate.canceled.store(true, Ordering::Release);
                candidate.canceled_changed.notify_waiters();
                Some(candidate.cleanup.clone())
            },
            Some(HotTransition::Draining(draining)) => {
                // Unlike normal drain completion, preemption may terminate the
                // old child while request guards are still active. Publish the
                // distinction before waking cleanup so pre-start reservations
                // cannot race the termination task.
                draining.old.mark_execution_unavailable();
                draining
                    .old
                    .terminate_draining
                    .store(true, Ordering::Release);
                draining.old.terminate_draining_notify.notify_waiters();
                Some(draining.cleanup.clone())
            },
            None => None,
        };
        drop(state_guard);
        if let Some(cleanup) = cleanup
            && cleanup.cleanup(outcome).await.is_err()
        {
            let _ = cleanup.cleanup(outcome).await;
        }
    }

    async fn acquire_inner(
        &self,
        request_metadata: RequestDiagnosticMetadata,
        resident_fingerprint: Option<ResidentGenerationFingerprint>,
        preparation_descriptor: Option<PreparationDescriptor>,
    ) -> anyhow::Result<(Arc<InnerLocalNodeExecutor>, ActiveRequestGuard, bool)> {
        loop {
            let transition_changed = self.transition_changed.notified();
            tokio::pin!(transition_changed);
            transition_changed.as_mut().enable();
            match self
                .acquire_existing_inner_with_descriptor(
                    request_metadata.clone(),
                    resident_fingerprint.clone(),
                    preparation_descriptor.clone(),
                )
                .await?
            {
                InnerAcquisition::Ready { inner, guard } => {
                    return Ok((inner, guard, false));
                },
                InnerAcquisition::Draining(inner) => inner.wait_until_retired().await?,
                InnerAcquisition::Transition(status) => {
                    transition_changed.await;
                    if status.failed.load(Ordering::Acquire)
                        || status.cleanup_failed.load(Ordering::Acquire)
                    {
                        anyhow::ensure!(
                            self.failed_hot_transition_allows_cold_retry().await,
                            "Local Node executor hot replacement failed"
                        );
                    }
                },
                InnerAcquisition::Missing => break,
            }
        }

        // Child startup can take several health-check intervals. Serialize that
        // work separately so late failures from the retired generation can
        // still inspect the generation slot without waiting for its replacement.
        let _startup_guard = self.startup_lock.lock().await;
        loop {
            let transition_changed = self.transition_changed.notified();
            tokio::pin!(transition_changed);
            transition_changed.as_mut().enable();
            match self
                .acquire_existing_inner_with_descriptor(
                    request_metadata.clone(),
                    resident_fingerprint.clone(),
                    preparation_descriptor.clone(),
                )
                .await?
            {
                InnerAcquisition::Ready { inner, guard } => {
                    return Ok((inner, guard, false));
                },
                InnerAcquisition::Draining(inner) => inner.wait_until_retired().await?,
                InnerAcquisition::Transition(status) => {
                    transition_changed.await;
                    if status.failed.load(Ordering::Acquire)
                        || status.cleanup_failed.load(Ordering::Acquire)
                    {
                        anyhow::ensure!(
                            self.failed_hot_transition_allows_cold_retry().await,
                            "Local Node executor hot replacement failed"
                        );
                    }
                },
                InnerAcquisition::Missing => break,
            }
        }
        let (generation, replaces_generation) = {
            let mut state = self.state.lock().await;
            anyhow::ensure!(
                !self.shutting_down.load(Ordering::Acquire),
                "Local Node executor is shutting down"
            );
            assert!(state.inner.is_none());
            assert!(state.retiring.is_none());
            assert!(state.hot_transition.is_none());
            state.next_generation = state
                .next_generation
                .checked_add(1)
                .expect("Local Node executor generation overflow");
            (state.next_generation, state.replacement_for_generation)
        };

        let fingerprinted_generation = resident_fingerprint.is_some();
        let replacement_started = Instant::now();
        let replacement = match InnerLocalNodeExecutor::new(
            generation,
            resident_fingerprint,
            self.activity.clone(),
            &self.config,
            None,
            None,
        )
        .await
        {
            Ok(replacement) => Arc::new(replacement),
            Err(error) => {
                if fingerprinted_generation {
                    crate::metrics::log_local_node_fingerprint_transition(
                        &self.config.pool_name,
                        crate::metrics::FingerprintTransitionOutcome::StartupFailed,
                    );
                }
                if let Some(replaces_generation) = replaces_generation {
                    crate::metrics::log_local_node_replacement_outcome(
                        &self.config.pool_name,
                        "startup_failed",
                    );
                    tracing::warn!(
                        pool_name = %self.config.pool_name,
                        generation,
                        replaces_generation,
                        "Failed to start replacement local Node executor generation"
                    );
                }
                return Err(error).context("Failed to create inner local node executor");
            },
        };
        if self.shutting_down.load(Ordering::Acquire) {
            if let Some(replaces_generation) = replaces_generation {
                crate::metrics::log_local_node_replacement_outcome(
                    &self.config.pool_name,
                    "aborted_shutdown",
                );
                tracing::info!(
                    pool_name = %self.config.pool_name,
                    generation,
                    replaces_generation,
                    "Discarding replacement local Node executor generation during shutdown"
                );
            }
            replacement.terminate().await?;
            anyhow::bail!("Local Node executor is shutting down");
        }

        let mut state = self.state.lock().await;
        if self.shutting_down.load(Ordering::Acquire) {
            drop(state);
            if let Some(replaces_generation) = replaces_generation {
                crate::metrics::log_local_node_replacement_outcome(
                    &self.config.pool_name,
                    "aborted_shutdown",
                );
                tracing::info!(
                    pool_name = %self.config.pool_name,
                    generation,
                    replaces_generation,
                    "Discarding replacement local Node executor generation during shutdown"
                );
            }
            replacement.terminate().await?;
            anyhow::bail!("Local Node executor is shutting down");
        }
        assert!(state.inner.is_none());
        assert!(state.retiring.is_none());
        assert!(state.hot_transition.is_none());
        assert_eq!(state.replacement_for_generation, replaces_generation);
        state.inner = Some(replacement.clone());
        state.replacement_for_generation = None;
        crate::metrics::set_local_node_generation_present(&self.config.pool_name, true);
        crate::metrics::set_local_node_memory_pressure_active(
            &self.config.pool_name,
            self.config.memory_pressure.is_active(),
        );
        crate::metrics::set_local_node_generation_age(&self.config.pool_name, Duration::ZERO);
        crate::metrics::set_local_node_generation_draining(&self.config.pool_name, false);
        crate::metrics::set_local_node_child_rss(&self.config.pool_name, None);
        crate::metrics::set_local_node_consecutive_health_misses(&self.config.pool_name, 0);
        if replacement.runtime_stats_supported {
            crate::metrics::set_local_node_package_state(
                &self.config.pool_name,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            );
        }
        crate::metrics::log_local_node_generation_start(&self.config.pool_name);
        let startup_elapsed = replacement_started.elapsed();
        if replaces_generation.is_some() {
            crate::metrics::log_local_node_replacement_time(
                &self.config.pool_name,
                startup_elapsed,
            );
            crate::metrics::log_local_node_replacement_outcome(&self.config.pool_name, "ready");
            if fingerprinted_generation {
                crate::metrics::log_local_node_fingerprint_transition(
                    &self.config.pool_name,
                    crate::metrics::FingerprintTransitionOutcome::ReplacementReady,
                );
            }
        }
        tracing::info!(
            pool_name = %self.config.pool_name,
            generation,
            replacement = replaces_generation.is_some(),
            replaces_generation = ?replaces_generation,
            runtime_stats_supported = replacement.runtime_stats_supported,
            startup_seconds = startup_elapsed.as_secs_f64(),
            "Started local Node executor generation"
        );
        let request_guard = replacement.admit_request(request_metadata, preparation_descriptor);
        Ok((replacement, request_guard, true))
    }

    async fn failed_hot_transition_allows_cold_retry(&self) -> bool {
        let state = self.state.lock().await;
        // Immediate retirement can cancel a candidate while also removing its
        // expected current generation. Once candidate ownership is cleared,
        // joined requests may continue through the ordinary cold-start path.
        state.inner.is_none() && state.hot_transition.is_none()
    }

    async fn acquire_existing_inner_with_descriptor(
        &self,
        request_metadata: RequestDiagnosticMetadata,
        resident_fingerprint: Option<ResidentGenerationFingerprint>,
        preparation_descriptor: Option<PreparationDescriptor>,
    ) -> anyhow::Result<InnerAcquisition> {
        let state = self.state.lock().await;
        anyhow::ensure!(
            !self.shutting_down.load(Ordering::Acquire),
            "Local Node executor is shutting down"
        );
        if let Some(inner) = &state.inner {
            let inner = inner.clone();
            if inner.retirement_requested.load(Ordering::Acquire) {
                if resident_fingerprint.is_some() {
                    crate::metrics::log_local_node_fingerprint_transition(
                        &inner.pool_name,
                        crate::metrics::FingerprintTransitionOutcome::Joined,
                    );
                }
                return Ok(InnerAcquisition::Draining(inner));
            }
            let fingerprint_matches = same_resident_package_and_environment(
                inner.resident_fingerprint.as_ref(),
                resident_fingerprint.as_ref(),
            );
            if resident_fingerprint.is_some() && !fingerprint_matches {
                if let (Some(current), Some(incoming)) =
                    (&inner.resident_fingerprint, &resident_fingerprint)
                {
                    anyhow::ensure!(
                        incoming.topology_version >= current.topology_version,
                        "Node executor request uses a stale resident generation fingerprint"
                    );
                }
                let existing_transition_status =
                    state
                        .hot_transition
                        .as_ref()
                        .map(|transition| match transition {
                            HotTransition::Candidate(candidate) => candidate.status.clone(),
                            HotTransition::Draining(draining) => draining.status.clone(),
                        });
                if let Some(status) = &existing_transition_status {
                    anyhow::ensure!(
                        !status.failed.load(Ordering::Acquire)
                            && !status.cleanup_failed.load(Ordering::Acquire),
                        "Local Node executor hot replacement failed"
                    );
                }
                drop(state);
                let status = self
                    .request_hot_replacement(
                        &inner,
                        resident_fingerprint,
                        preparation_descriptor,
                        GenerationRetirementReason::FingerprintChange,
                    )
                    .await;
                let Some(status) = status.or(existing_transition_status) else {
                    anyhow::bail!("Local Node executor hot replacement could not start");
                };
                crate::metrics::log_local_node_fingerprint_transition(
                    &inner.pool_name,
                    crate::metrics::FingerprintTransitionOutcome::Joined,
                );
                return Ok(InnerAcquisition::Transition(status));
            }
            // Selection and the active increment happen under the generation
            // slot lock, so proactive retirement cannot observe zero and close
            // admission between these operations.
            let guard = inner.admit_request(request_metadata, preparation_descriptor);
            return Ok(InnerAcquisition::Ready { inner, guard });
        }
        if let Some(retiring) = &state.retiring {
            if resident_fingerprint.is_some() {
                crate::metrics::log_local_node_fingerprint_transition(
                    &retiring.pool_name,
                    crate::metrics::FingerprintTransitionOutcome::Joined,
                );
            }
            return Ok(InnerAcquisition::Draining(retiring.clone()));
        }
        if let Some(transition) = &state.hot_transition {
            let status = match transition {
                HotTransition::Candidate(candidate) => candidate.status.clone(),
                HotTransition::Draining(draining) => draining.status.clone(),
            };
            anyhow::ensure!(
                !status.failed.load(Ordering::Acquire)
                    && !status.cleanup_failed.load(Ordering::Acquire),
                "Local Node executor hot replacement failed"
            );
            return Ok(InnerAcquisition::Transition(status));
        }
        Ok(InnerAcquisition::Missing)
    }

    #[cfg(test)]
    async fn acquire_existing_inner(
        &self,
        request_metadata: RequestDiagnosticMetadata,
        resident_fingerprint: Option<ResidentGenerationFingerprint>,
    ) -> anyhow::Result<InnerAcquisition> {
        self.acquire_existing_inner_with_descriptor(request_metadata, resident_fingerprint, None)
            .await
    }

    #[try_stream(ok = NodeExecutorStreamPart, error = anyhow::Error)]
    async fn response_stream(mut response: reqwest::Response, deadline: tokio::time::Instant) {
        anyhow::ensure!(
            response
                .content_length()
                .is_none_or(|length| length <= MAX_INVOKE_RESPONSE_BYTES as u64),
            "Local Node executor response exceeded size limit"
        );
        let mut response_bytes = 0usize;
        loop {
            let part = match tokio::time::timeout_at(deadline, response.chunk()).await {
                Ok(chunk) => match chunk? {
                    Some(chunk) => {
                        response_bytes = response_bytes
                            .checked_add(chunk.len())
                            .filter(|size| *size <= MAX_INVOKE_RESPONSE_BYTES)
                            .ok_or_else(|| {
                                anyhow::anyhow!("Local Node executor response exceeded size limit")
                            })?;
                        NodeExecutorStreamPart::Chunk(chunk)
                    },
                    None => NodeExecutorStreamPart::InvokeComplete(InvokeCompletion::Success),
                },
                Err(_) => NodeExecutorStreamPart::InvokeComplete(InvokeCompletion::ExplicitError(
                    InvokeResponse {
                        response: EXECUTE_TIMEOUT_RESPONSE_JSON.clone(),
                        aws_request_id: None,
                    },
                )),
            };
            if let NodeExecutorStreamPart::InvokeComplete(_) = part {
                yield part;
                break;
            } else {
                yield part;
            }
        }
    }

    async fn retire_inner_if_current(
        &self,
        expected: &Arc<InnerLocalNodeExecutor>,
        diagnostics: GenerationRetirementDiagnostics,
    ) -> anyhow::Result<bool> {
        Self::retire_inner_state(&self.state, expected, diagnostics).await
    }

    async fn retire_inner_state(
        state: &Arc<Mutex<LocalNodeExecutorState>>,
        expected: &Arc<InnerLocalNodeExecutor>,
        diagnostics: GenerationRetirementDiagnostics,
    ) -> anyhow::Result<bool> {
        let reason = diagnostics.reason;
        {
            let state_guard = state.lock().await;
            if let Some(HotTransition::Draining(draining)) = &state_guard.hot_transition
                && Arc::ptr_eq(&draining.old, expected)
            {
                expected.mark_execution_unavailable();
                expected.terminate_draining.store(true, Ordering::Release);
                expected.terminate_draining_notify.notify_waiters();
                drop(state_guard);
                expected.wait_until_retired().await?;
                return Ok(true);
            }
        }
        let retired = {
            let mut state = state.lock().await;
            if state
                .inner
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, expected))
            {
                // A late result from an old generation cannot retire its replacement.
                // Publish hard unavailability while admission is still fenced
                // by this mutex. Normal promotion uses the graceful drain path
                // and therefore does not publish this signal.
                expected.mark_execution_unavailable();
                state.inner.take();
                assert!(state.retiring.is_none());
                if let Some(HotTransition::Candidate(candidate)) = &state.hot_transition {
                    candidate.canceled.store(true, Ordering::Release);
                    // The candidate may be asleep in the global surge queue.
                    // Closing its expected generation must wake that waiter so
                    // the stale transition cannot fence cold replacement.
                    candidate.canceled_changed.notify_waiters();
                }
                state.retiring = Some(expected.clone());
                state.replacement_for_generation =
                    (!matches!(reason, GenerationRetirementReason::ExplicitShutdown))
                        .then_some(expected.generation);
                // A prior drain task can fail before claiming the slot. This
                // transition installs a new retirement owner, so its terminal
                // result supersedes that earlier failed attempt.
                expected.retirement_failed.store(false, Ordering::Release);
                crate::metrics::set_local_node_generation_present(&expected.pool_name, false);
                crate::metrics::set_local_node_memory_pressure_active(&expected.pool_name, false);
                crate::metrics::set_local_node_generation_age(&expected.pool_name, Duration::ZERO);
                crate::metrics::set_local_node_generation_draining(&expected.pool_name, false);
                crate::metrics::set_local_node_child_rss(&expected.pool_name, None);
                crate::metrics::set_local_node_consecutive_health_misses(&expected.pool_name, 0);
                if expected.runtime_stats_supported {
                    crate::metrics::set_local_node_package_state(
                        &expected.pool_name,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                    );
                }
                crate::metrics::log_local_node_generation_retirement(
                    &expected.pool_name,
                    reason.as_str(),
                );
                crate::metrics::log_local_node_retirement_diagnostics(
                    &expected.pool_name,
                    reason.as_str(),
                    diagnostics.request_kind,
                    diagnostics.phase,
                    diagnostics.transport_error_kind,
                );
                if matches!(reason, GenerationRetirementReason::ExplicitShutdown) {
                    tracing::info!(
                        pool_name = %expected.pool_name,
                        generation = expected.generation,
                        reason = reason.as_str(),
                        request_kind = diagnostics.request_kind,
                        phase = diagnostics.phase,
                        transport_error_kind = diagnostics.transport_error_kind,
                        replacement_expected = false,
                        runtime_stats_supported = expected.runtime_stats_supported,
                        generation_age_seconds = expected.started_at.elapsed().as_secs_f64(),
                        active_requests = expected.active_requests.load(Ordering::Relaxed),
                        last_observed_retained_source_packages =
                            expected.retained_source_packages.load(Ordering::Relaxed),
                        last_observed_retained_external_packages =
                            expected.retained_external_packages.load(Ordering::Relaxed),
                        last_observed_imported_source_packages =
                            expected.imported_source_packages.load(Ordering::Relaxed),
                        last_observed_registered_stack_roots =
                            expected.registered_stack_roots.load(Ordering::Relaxed),
                        "Retiring local Node executor generation"
                    );
                } else {
                    tracing::warn!(
                        pool_name = %expected.pool_name,
                        generation = expected.generation,
                        reason = reason.as_str(),
                        request_kind = diagnostics.request_kind,
                        phase = diagnostics.phase,
                        transport_error_kind = diagnostics.transport_error_kind,
                        replacement_expected = true,
                        runtime_stats_supported = expected.runtime_stats_supported,
                        generation_age_seconds = expected.started_at.elapsed().as_secs_f64(),
                        active_requests = expected.active_requests.load(Ordering::Relaxed),
                        last_observed_retained_source_packages =
                            expected.retained_source_packages.load(Ordering::Relaxed),
                        last_observed_retained_external_packages =
                            expected.retained_external_packages.load(Ordering::Relaxed),
                        last_observed_imported_source_packages =
                            expected.imported_source_packages.load(Ordering::Relaxed),
                        last_observed_registered_stack_roots =
                            expected.registered_stack_roots.load(Ordering::Relaxed),
                        "Retiring local Node executor generation"
                    );
                }
                true
            } else {
                false
            }
        };
        if !retired {
            return Ok(false);
        }

        // Request-held Arcs can outlive retirement. Stop the selected child now
        // so a blocked event loop does not continue consuming a core until each
        // old request reaches its ten-minute timeout. The spawned task remains
        // the child owner if the request that initiated retirement is canceled.
        let state = state.clone();
        let termination_expected = expected.clone();
        let mut task_guard = RetirementTaskGuard::new(termination_expected.clone());
        let termination = tokio::spawn(async move {
            let result =
                Self::terminate_retiring_inner_state(&state, &termination_expected, reason).await;
            task_guard.disarm();
            result
        })
        .await;
        match termination {
            Ok(result) => {
                result?;
            },
            Err(error) if error.is_cancelled() => {
                anyhow::bail!("Local Node executor child termination task was canceled")
            },
            Err(error) if error.is_panic() => {
                anyhow::bail!("Local Node executor child termination task panicked")
            },
            Err(_) => {
                anyhow::bail!("Local Node executor child termination task failed")
            },
        }
        Ok(true)
    }

    async fn terminate_retiring_inner_state(
        state: &Arc<Mutex<LocalNodeExecutorState>>,
        expected: &Arc<InnerLocalNodeExecutor>,
        reason: GenerationRetirementReason,
    ) -> anyhow::Result<()> {
        let result = expected.terminate().await;
        match &result {
            Ok(observation) => {
                crate::metrics::log_local_node_child_termination(
                    &expected.pool_name,
                    reason.as_str(),
                    observation.state_before,
                    observation.supervisor_kill_requested,
                    observation.exit_class,
                );
                tracing::info!(
                    pool_name = %expected.pool_name,
                    generation = expected.generation,
                    reason = reason.as_str(),
                    state_before = observation.state_before,
                    supervisor_kill_requested = observation.supervisor_kill_requested,
                    exit_class = observation.exit_class,
                    "Completed local Node executor child termination"
                );
            },
            Err(_) => {
                tracing::error!(
                    pool_name = %expected.pool_name,
                    generation = expected.generation,
                    reason = reason.as_str(),
                    "Failed to terminate and reap local Node executor child"
                );
            },
        }
        if result.is_ok() {
            let mut state = state.lock().await;
            let retiring = state
                .retiring
                .take()
                .expect("retiring local Node generation is missing");
            assert!(Arc::ptr_eq(&retiring, expected));
            if matches!(reason, GenerationRetirementReason::ExplicitShutdown) {
                state.replacement_for_generation = None;
            }
            drop(state);
            // A waiter must not start the replacement while the old child is
            // still resident. A short process overlap is unsafe when RSS
            // retirement is preserving cgroup memory headroom.
            expected.retired.store(true, Ordering::Release);
            expected.retired_notify.notify_waiters();
        } else {
            expected.mark_retirement_failed();
        }
        result.map(|_| ())
    }

    async fn finish_retiring_inner_for_shutdown(
        state: &Arc<Mutex<LocalNodeExecutorState>>,
        expected: &Arc<InnerLocalNodeExecutor>,
    ) -> anyhow::Result<()> {
        loop {
            if expected.retired.load(Ordering::Acquire) {
                return Ok(());
            }
            if expected
                .retirement_failed
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // Claim the published failure before retrying. A false flag
                // means another termination attempt is still in progress.
                return Self::terminate_retiring_inner_state(
                    state,
                    expected,
                    GenerationRetirementReason::ExplicitShutdown,
                )
                .await;
            }
            match expected.wait_until_retired().await {
                Ok(()) => return Ok(()),
                Err(_) => {},
            }
        }
    }

    async fn start_draining_inner_state(
        state: &Arc<Mutex<LocalNodeExecutorState>>,
        expected: &Arc<InnerLocalNodeExecutor>,
        reason: GenerationRetirementReason,
    ) -> bool {
        let started_draining = {
            let state = state.lock().await;
            if !state
                .inner
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, expected))
            {
                crate::metrics::log_local_node_retirement_decision(
                    &expected.pool_name,
                    reason.as_str(),
                    "not_current",
                );
                return false;
            }
            let started_draining = !expected.retirement_requested.swap(true, Ordering::AcqRel);
            if started_draining {
                // Retirement and request admission share this lock. Publish the
                // corresponding gauge transition here too, so an immediate
                // request-triggered retirement cannot reset it before this write.
                crate::metrics::set_local_node_generation_draining(&expected.pool_name, true);
            }
            started_draining
        };
        if !started_draining {
            crate::metrics::log_local_node_retirement_decision(
                &expected.pool_name,
                reason.as_str(),
                "already_draining",
            );
            return false;
        }

        // Admission and the active-request increment share the state lock with
        // this transition. Once draining is visible, the count can only fall.
        crate::metrics::log_local_node_retirement_decision(
            &expected.pool_name,
            reason.as_str(),
            "drain_started",
        );
        true
    }

    async fn finish_draining_inner_state(
        state: &Arc<Mutex<LocalNodeExecutorState>>,
        expected: &Arc<InnerLocalNodeExecutor>,
        diagnostics: GenerationRetirementDiagnostics,
    ) -> anyhow::Result<bool> {
        let state = state.clone();
        let drain_expected = expected.clone();
        let mut task_guard = RetirementTaskGuard::new(drain_expected.clone());
        let retirement = tokio::spawn(async move {
            let result = tokio::select! {
                _ = drain_expected.wait_until_idle() => {
                    Self::retire_inner_state(&state, &drain_expected, diagnostics).await
                },
                retired = drain_expected.wait_until_retired() => {
                    // Another retirement owner can terminate and reap the child
                    // while request guards remain alive. Reaping is sufficient
                    // to release this topology barrier.
                    retired.map(|()| false)
                },
            };
            task_guard.disarm();
            result
        })
        .await;
        match retirement {
            Ok(result) => result,
            Err(error) if error.is_cancelled() => {
                // The task guard publishes a terminal result even when the task
                // stopped before it moved the generation into `retiring`.
                anyhow::bail!("Local Node executor drain task was canceled")
            },
            Err(error) if error.is_panic() => {
                anyhow::bail!("Local Node executor drain task panicked")
            },
            Err(_) => {
                anyhow::bail!("Local Node executor drain task failed")
            },
        }
    }

    #[cfg(all(test, unix))]
    async fn drain_and_retire_inner_state(
        state: &Arc<Mutex<LocalNodeExecutorState>>,
        expected: &Arc<InnerLocalNodeExecutor>,
        diagnostics: GenerationRetirementDiagnostics,
    ) -> anyhow::Result<bool> {
        if !Self::start_draining_inner_state(state, expected, diagnostics.reason).await {
            return Ok(false);
        }
        Self::finish_draining_inner_state(state, expected, diagnostics).await
    }

    fn spawn_first_miss_diagnostics(
        expected: &Arc<InnerLocalNodeExecutor>,
        rss_bytes: Option<u64>,
        previous_process: Option<ProcessStatBaseline>,
        config: &LocalNodeExecutorConfig,
    ) {
        if !expected.claim_first_miss_diagnostics() {
            return;
        }

        let expected = expected.clone();
        let config = config.clone();
        tokio::spawn(async move {
            let captured_at = Instant::now();
            let (active_request_count, active_requests) = {
                let active = expected
                    .active_request_diagnostics
                    .lock()
                    .expect("Local Node active-request diagnostic lock is poisoned");
                // Admissions increment the aggregate count before taking this lock,
                // while completions remove metadata before decrementing it. Reading
                // the count under the metadata lock cannot undercount these entries.
                let active_request_count = expected.active_requests.load(Ordering::Relaxed);
                let active_requests = active
                    .values()
                    .map(|request| ActiveRequestDiagnosticSnapshot {
                        request_kind: request.metadata.request_kind,
                        module_path: request.metadata.module_path.clone(),
                        function_name: request.metadata.function_name.clone(),
                        elapsed_ms: u64::try_from(
                            captured_at.duration_since(request.started_at).as_millis(),
                        )
                        .unwrap_or(u64::MAX),
                    })
                    .collect::<Vec<_>>();
                (active_request_count, active_requests)
            };
            let active_requests_truncated = active_request_count > active_requests.len();
            let generation = expected.generation;
            let pid = expected.pid;
            let generation_age_ms =
                u64::try_from(expected.started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
            let imported_source_packages =
                expected.imported_source_packages.load(Ordering::Relaxed);
            let diagnostic_paths = expected
                .diagnostic_paths
                .clone()
                .expect("Claimed first-miss diagnostics have no paths");
            tracing::warn!(
                pool_name = %expected.pool_name,
                generation,
                pid,
                generation_age_seconds = expected.started_at.elapsed().as_secs_f64(),
                active_requests = active_request_count,
                active_requests_truncated,
                "Started first-miss local Node executor diagnostics"
            );

            let report_expected = expected.clone();
            tokio::spawn(async move {
                let outcome = match tokio::time::timeout(
                    DIAGNOSTIC_REPORT_REQUEST_TIMEOUT,
                    report_expected.request_diagnostic_report(),
                )
                .await
                {
                    Ok(outcome) => outcome,
                    Err(_) => FirstMissDiagnosticOutcome::DiagnosticReportRequestFailed,
                };
                crate::metrics::log_local_node_first_miss_diagnostic(
                    &report_expected.pool_name,
                    outcome,
                );
            });

            let profile_paths = diagnostic_paths.clone();
            let profile_source_owner = expected.clone();
            let profile_pool_name = expected.pool_name.clone();
            tokio::spawn(async move {
                let outcome =
                    trigger_main_thread_profile(&profile_paths, profile_source_owner).await;
                crate::metrics::log_local_node_first_miss_diagnostic(&profile_pool_name, outcome);
            });

            let captured_at_unix_ms = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(duration) => duration.as_millis(),
                Err(_) => {
                    crate::metrics::log_local_node_first_miss_diagnostic(
                        &expected.pool_name,
                        FirstMissDiagnosticOutcome::ProcSnapshotClockFailure,
                    );
                    return;
                },
            };
            let (process, process_stat_outcome, process_sampled_at) = match tokio::time::timeout(
                DIAGNOSTIC_PROCESS_SNAPSHOT_TIMEOUT,
                expected.read_owned_process_stat(),
            )
            .await
            {
                Ok(Ok(Some(process))) => (
                    Some(process),
                    DiagnosticSnapshotOutcome::Success,
                    Some(Instant::now()),
                ),
                Ok(Ok(None)) => (None, DiagnosticSnapshotOutcome::Unsupported, None),
                Ok(Err(_)) => (None, DiagnosticSnapshotOutcome::Failure, None),
                Err(_) => (None, DiagnosticSnapshotOutcome::Timeout, None),
            };
            let process_cpu_delta = process.as_ref().and_then(|current| {
                let current_sampled_at = process_sampled_at?;
                let previous = previous_process.as_ref()?;
                let previous_process = previous.process.as_ref()?;
                if current.start_time_ticks != previous_process.start_time_ticks {
                    return None;
                }
                Some(ProcessCpuDelta {
                    user_ticks: current
                        .user_ticks
                        .checked_sub(previous_process.user_ticks)?,
                    system_ticks: current
                        .system_ticks
                        .checked_sub(previous_process.system_ticks)?,
                    interval_ms: u64::try_from(
                        current_sampled_at
                            .saturating_duration_since(previous.sampled_at)
                            .as_millis(),
                    )
                    .unwrap_or(u64::MAX),
                })
            });
            let (threads, threads_truncated, thread_stat_outcome) = match process.as_ref() {
                Some(process) => match tokio::time::timeout(
                    DIAGNOSTIC_PROCESS_SNAPSHOT_TIMEOUT,
                    expected.read_owned_thread_stats(process.start_time_ticks),
                )
                .await
                {
                    Ok(Ok(Some((threads, truncated)))) => {
                        (threads, truncated, DiagnosticSnapshotOutcome::Success)
                    },
                    Ok(Ok(None)) => (Vec::new(), false, DiagnosticSnapshotOutcome::Unsupported),
                    Ok(Err(_)) => (Vec::new(), false, DiagnosticSnapshotOutcome::Failure),
                    Err(_) => (Vec::new(), false, DiagnosticSnapshotOutcome::Timeout),
                },
                None => (Vec::new(), false, process_stat_outcome),
            };
            let artifact = FirstMissDiagnosticArtifact {
                schema_version: 1,
                pool_name: expected.pool_name.to_string(),
                captured_at_unix_ms,
                generation,
                pid,
                generation_age_ms,
                active_request_count,
                active_requests_truncated,
                active_requests,
                rss_bytes,
                old_space_limit_bytes: config.old_space_bytes(),
                rss_retirement_threshold_bytes: config.max_rss_bytes,
                generation_age_retirement_threshold_ms: u64::try_from(
                    config.max_generation_age.as_millis(),
                )
                .unwrap_or(u64::MAX),
                imported_source_packages,
                imported_source_package_retirement_threshold: config.max_imported_source_packages,
                process_stat_outcome,
                process,
                process_cpu_delta,
                thread_stat_outcome,
                threads_truncated,
                threads,
            };
            let outcome = match serde_json::to_vec(&artifact) {
                Ok(contents) => match tokio::time::timeout(
                    DIAGNOSTIC_FILESYSTEM_TIMEOUT,
                    tokio::task::spawn_blocking(move || {
                        write_private_diagnostic_artifact(
                            &diagnostic_paths.first_miss_path,
                            &contents,
                        )
                    }),
                )
                .await
                {
                    Ok(Ok(Ok(()))) => FirstMissDiagnosticOutcome::ProcSnapshotCompleted,
                    Err(_) | Ok(Err(_)) | Ok(Ok(Err(_))) => {
                        FirstMissDiagnosticOutcome::ProcSnapshotWriteFailed
                    },
                },
                Err(_) => FirstMissDiagnosticOutcome::ProcSnapshotSerializationFailed,
            };
            crate::metrics::log_local_node_first_miss_diagnostic(&expected.pool_name, outcome);
        });
    }

    fn spawn_process_stat_sample(
        expected: &Arc<InnerLocalNodeExecutor>,
        sequence: u64,
        latest: &Arc<StdMutex<Option<ProcessStatBaseline>>>,
    ) {
        let expected = expected.clone();
        let latest = latest.clone();
        tokio::spawn(async move {
            let process = match tokio::time::timeout(
                DIAGNOSTIC_PROCESS_SNAPSHOT_TIMEOUT,
                expected.read_owned_process_stat(),
            )
            .await
            {
                Ok(Ok(process)) => process,
                Err(_) | Ok(Err(_)) => None,
            };
            let mut latest = latest
                .lock()
                .expect("Local Node process-stat diagnostic lock is poisoned");
            if latest
                .as_ref()
                .is_none_or(|previous| previous.sequence < sequence)
            {
                *latest = Some(ProcessStatBaseline {
                    sequence,
                    sampled_at: Instant::now(),
                    process,
                });
            }
        });
    }

    fn spawn_watchdog(&self, inner: &Arc<InnerLocalNodeExecutor>) {
        Self::spawn_watchdog_state(
            &self.state,
            inner,
            self.config.clone(),
            self.transition_changed.clone(),
            self.shutting_down.clone(),
        );
    }

    fn spawn_watchdog_state(
        state: &Arc<Mutex<LocalNodeExecutorState>>,
        inner: &Arc<InnerLocalNodeExecutor>,
        config: LocalNodeExecutorConfig,
        transition_changed: Arc<Notify>,
        shutting_down: Arc<AtomicBool>,
    ) {
        let state = Arc::downgrade(state);
        let expected = Arc::downgrade(inner);
        tokio::spawn(async move {
            Self::watch_generation_with_lifecycle(
                state,
                expected,
                config,
                transition_changed,
                shutting_down,
            )
            .await;
        });
    }

    async fn watch_generation_with_lifecycle(
        state: Weak<Mutex<LocalNodeExecutorState>>,
        expected: Weak<InnerLocalNodeExecutor>,
        config: LocalNodeExecutorConfig,
        transition_changed: Arc<Notify>,
        shutting_down: Arc<AtomicBool>,
    ) {
        let mut memory_pressure = config.memory_pressure.subscribe();
        let memory_pressure_observation = Arc::new(StdMutex::new(MemoryPressureObservation::new(
            *memory_pressure.borrow_and_update(),
            Instant::now(),
        )));
        let pressure_tracking_observation = memory_pressure_observation.clone();
        let pressure_tracking = async move {
            loop {
                memory_pressure.changed().await.expect(
                    "Local Node memory-pressure signal unexpectedly closed while configured",
                );
                let active = *memory_pressure.borrow_and_update();
                pressure_tracking_observation
                    .lock()
                    .expect("Local Node memory-pressure observation lock is poisoned")
                    .observe_publication(active, Instant::now());
            }
        };
        tokio::select! {
            // A pressure publication and a completed health check can wake this
            // task together. Apply any ready pressure update before the check
            // can use the prior episode's grace to start a proactive drain.
            biased;
            () = pressure_tracking => {
                unreachable!("Local Node memory-pressure tracking loop returned")
            },
            () = Self::watch_generation_checks(
                state,
                expected,
                config,
                memory_pressure_observation,
                transition_changed,
                shutting_down,
            ) => {},
        }
    }

    #[cfg(test)]
    async fn watch_generation(
        state: Weak<Mutex<LocalNodeExecutorState>>,
        expected: Weak<InnerLocalNodeExecutor>,
        config: LocalNodeExecutorConfig,
    ) {
        Self::watch_generation_with_lifecycle(
            state,
            expected,
            config,
            Arc::new(Notify::new()),
            Arc::new(AtomicBool::new(false)),
        )
        .await;
    }

    async fn watch_generation_checks(
        state: Weak<Mutex<LocalNodeExecutorState>>,
        expected: Weak<InnerLocalNodeExecutor>,
        config: LocalNodeExecutorConfig,
        memory_pressure_observation: Arc<StdMutex<MemoryPressureObservation>>,
        transition_changed: Arc<Notify>,
        shutting_down: Arc<AtomicBool>,
    ) {
        let mut consecutive_misses = 0;
        let mut previous_package_stats = NodePackageCacheStats::default();
        let mut previous_stack_stats = NodeStackTraceStats::default();
        let latest_process_stat = Arc::new(StdMutex::new(None));
        let mut process_stat_sequence = 0u64;
        loop {
            tokio::time::sleep(config.watchdog_interval).await;
            let Some(state) = state.upgrade() else {
                return;
            };
            let Some(expected) = expected.upgrade() else {
                return;
            };
            if !state
                .lock()
                .await
                .inner
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &expected))
            {
                return;
            }

            let health_check = async {
                let started_at = Instant::now();
                let health = InnerLocalNodeExecutor::check_server_health(
                    &expected.client,
                    config.health_check_timeout,
                )
                .await;
                (health, started_at.elapsed())
            };
            let ((health, health_check_elapsed), rss) =
                tokio::join!(health_check, read_process_rss(expected.pid),);
            // RSS enforcement is Linux-only. A failed or unsupported sample
            // skips only the RSS trigger for this iteration; age, package, and
            // unhealthy-generation checks remain active.
            let (rss_bytes, rss_sample_outcome) = match rss {
                Ok(Some(rss_bytes)) => (Some(rss_bytes), "success"),
                Ok(None) => (None, "unsupported"),
                Err(_) => (None, "failure"),
            };
            let success = health.as_ref().is_some_and(|health| {
                health.status == "ok"
                    && health
                        .valid_runtime_stats_support(&previous_package_stats, &previous_stack_stats)
                        == Some(expected.runtime_stats_supported)
            });
            // A health response can complete after a separate timeout retired
            // this generation. Do not publish an old-generation observation
            // after its replacement.
            let current_state = state.lock().await;
            if !current_state
                .inner
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &expected))
            {
                return;
            }
            crate::metrics::log_local_node_health_check(
                &expected.pool_name,
                health_check_elapsed,
                "watchdog",
                success,
            );
            crate::metrics::log_local_node_child_rss_sample(
                &expected.pool_name,
                rss_sample_outcome,
            );
            crate::metrics::set_local_node_child_rss(&expected.pool_name, rss_bytes);
            let generation_age = expected.started_at.elapsed();
            crate::metrics::set_local_node_generation_age(&expected.pool_name, generation_age);
            let memory_pressure = *memory_pressure_observation
                .lock()
                .expect("Local Node memory-pressure observation lock is poisoned");
            let memory_pressure_active = memory_pressure.is_active();
            crate::metrics::set_local_node_memory_pressure_active(
                &expected.pool_name,
                memory_pressure_active,
            );
            let memory_pressure_active_for = memory_pressure.active_for(Instant::now());

            let should_retire_unhealthy = if let Some(health) = health.filter(|_| success) {
                consecutive_misses = 0;
                crate::metrics::set_local_node_consecutive_health_misses(&expected.pool_name, 0);
                if expected.runtime_stats_supported {
                    Self::record_node_health_metrics(
                        &expected,
                        health,
                        &mut previous_package_stats,
                        &mut previous_stack_stats,
                    );
                }
                if expected.diagnostic_paths.is_some()
                    && !expected
                        .first_miss_diagnostics_started
                        .load(Ordering::Acquire)
                {
                    process_stat_sequence = process_stat_sequence
                        .checked_add(1)
                        .expect("Local Node process-stat sample sequence overflow");
                    Self::spawn_process_stat_sample(
                        &expected,
                        process_stat_sequence,
                        &latest_process_stat,
                    );
                }
                false
            } else {
                consecutive_misses += 1;
                crate::metrics::set_local_node_consecutive_health_misses(
                    &expected.pool_name,
                    consecutive_misses,
                );
                if consecutive_misses == 1 {
                    // A CPU-delta baseline is optional evidence. Never block the
                    // watchdog behind a sample task that was descheduled while
                    // publishing it.
                    let previous_process = match latest_process_stat.try_lock() {
                        Ok(latest) => latest.clone(),
                        Err(std::sync::TryLockError::WouldBlock) => None,
                        Err(std::sync::TryLockError::Poisoned(_)) => {
                            panic!("Local Node process-stat diagnostic lock is poisoned")
                        },
                    };
                    Self::spawn_first_miss_diagnostics(
                        &expected,
                        rss_bytes,
                        previous_process,
                        &config,
                    );
                }
                consecutive_misses >= config.watchdog_failure_threshold
            };

            let retirement_reason = proactive_retirement_reason(
                &config,
                generation_age,
                rss_bytes,
                expected.imported_source_packages.load(Ordering::Relaxed),
                memory_pressure_active_for,
            );
            drop(current_state);
            if memory_pressure_active {
                Self::preempt_hot_transition_state(&state, "pressure_canceled").await;
            }
            if should_retire_unhealthy {
                // Do not start a new graceful drain on the terminal miss. An
                // idle drain could otherwise win the slot race and misclassify
                // this unhealthy retirement as proactive.
                if Self::retire_inner_state(
                    &state,
                    &expected,
                    GenerationRetirementDiagnostics::watchdog(),
                )
                .await
                .is_err()
                {
                    // The identity-fenced slot is already absent. This detached
                    // boundary can only report the bounded cleanup failure.
                    tracing::error!(
                        pool_name = %expected.pool_name,
                        generation = expected.generation,
                        "Failed to terminate and reap unhealthy local Node executor child"
                    );
                }
                return;
            }
            if let Some(reason) = retirement_reason {
                if matches!(reason, GenerationRetirementReason::CgroupPressure) {
                    if Self::retire_inner_state(
                        &state,
                        &expected,
                        GenerationRetirementDiagnostics::proactive(reason),
                    )
                    .await
                    .is_err()
                    {
                        tracing::error!(
                            pool_name = %expected.pool_name,
                            generation = expected.generation,
                            "Failed to terminate and reap a pressured local Node executor child"
                        );
                    }
                    return;
                }
                let descriptor = expected
                    .preparation_descriptor
                    .lock()
                    .expect("Local Node preparation descriptor lock poisoned")
                    .clone();
                // A generation with no admitted execute package may rotate
                // after health alone. Expired authority is not absence and
                // must not silently downgrade to health-only readiness.
                match descriptor {
                    Some(descriptor) if descriptor.is_expired() => {},
                    descriptor => {
                        Self::request_hot_replacement_state(
                            &state,
                            &transition_changed,
                            &shutting_down,
                            &expected.activity,
                            &config,
                            &expected,
                            expected.resident_fingerprint.clone(),
                            descriptor,
                            reason,
                            None,
                        )
                        .await;
                    },
                }
            }
        }
    }

    fn record_node_health_metrics(
        inner: &InnerLocalNodeExecutor,
        health: NodeExecutorHealth,
        previous_package_stats: &mut NodePackageCacheStats,
        previous_stack_stats: &mut NodeStackTraceStats,
    ) {
        let NodeExecutorHealth {
            package_cache: Some(package),
            stack_trace: Some(stack),
            ..
        } = health
        else {
            unreachable!("Validated Node health response is missing runtime stats");
        };
        inner
            .retained_source_packages
            .store(package.retained_source_packages, Ordering::Relaxed);
        inner
            .retained_external_packages
            .store(package.retained_external_packages, Ordering::Relaxed);
        inner
            .imported_source_packages
            .store(package.imported_source_packages, Ordering::Relaxed);
        inner
            .registered_stack_roots
            .store(stack.registered_roots, Ordering::Relaxed);
        crate::metrics::set_local_node_package_state(
            &inner.pool_name,
            package.imported_source_packages,
            package.retained_source_packages,
            package.retained_source_bytes,
            package.active_source_owners,
            package.retained_external_packages,
            package.retained_external_bytes,
            stack.registered_roots,
        );
        for (package_kind, operation, current, previous) in [
            (
                "source",
                "hit",
                package.source_hits,
                previous_package_stats.source_hits,
            ),
            (
                "source",
                "publish",
                package.source_publishes,
                previous_package_stats.source_publishes,
            ),
            (
                "source",
                "retire",
                package.source_retirements,
                previous_package_stats.source_retirements,
            ),
            (
                "source",
                "failed_publication",
                package.source_failed_publications,
                previous_package_stats.source_failed_publications,
            ),
            (
                "external",
                "hit",
                package.external_hits,
                previous_package_stats.external_hits,
            ),
            (
                "external",
                "publish",
                package.external_publishes,
                previous_package_stats.external_publishes,
            ),
            (
                "external",
                "retire",
                package.external_retirements,
                previous_package_stats.external_retirements,
            ),
            (
                "external",
                "failed_publication",
                package.external_failed_publications,
                previous_package_stats.external_failed_publications,
            ),
        ] {
            crate::metrics::log_local_node_package_events(
                &inner.pool_name,
                package_kind,
                operation,
                current - previous,
            );
        }

        crate::metrics::log_local_node_stack_format_deltas(
            &inner.pool_name,
            stack.invocations - previous_stack_stats.invocations,
            stack.frames_processed - previous_stack_stats.frames_processed,
            stack.duration_ms - previous_stack_stats.duration_ms,
        );
        *previous_package_stats = package;
        *previous_stack_stats = stack;
    }
}

impl LocalNodeExecutor {
    async fn prepare_and_wait_for_execution_start(
        &self,
        inner: &Arc<InnerLocalNodeExecutor>,
        descriptor: &PreparationDescriptor,
        start_gate: FunctionExecutionStartGate,
        request_guard: &mut ActiveRequestGuard,
    ) -> anyhow::Result<()> {
        if descriptor.is_expired() {
            request_guard.set_outcome("package_authority_expired_before_start");
            anyhow::bail!("Node package authority expired before execution admission");
        }

        // Preparing through the selected child moves the ordinary package
        // download before the durable claim without importing or invoking
        // application code. The endpoint does not retain its package-cache
        // lease, so `/invoke` can still need the signed authority again.
        let preparation = inner.prepare_package(descriptor, self.config.node_process_timeout);
        tokio::pin!(preparation);
        let hard_shutdown = wait_for_atomic_flag(&self.shutdown_started, &self.transition_changed);
        tokio::pin!(hard_shutdown);
        let unavailable = inner.wait_until_execution_unavailable();
        tokio::pin!(unavailable);
        let preparation_result = tokio::select! {
            biased;
            _ = &mut hard_shutdown => {
                request_guard.set_outcome("generation_lost_before_start");
                anyhow::bail!("Local Node executor shut down before execution start");
            },
            _ = &mut unavailable => {
                request_guard.set_outcome("generation_lost_before_start");
                anyhow::bail!("Local Node executor generation became unavailable before execution start");
            },
            result = &mut preparation => result,
        };
        if let Err(error) = preparation_result {
            request_guard.set_outcome("preparation_failed_before_start");
            return Err(error.context("Node package preparation failed before execution admission"));
        }

        // `acquire_inner` incremented this exact generation's active count
        // under the generation-state mutex shared with promotion. Do not
        // retain that mutex across the durable claim: this linear guard is
        // sufficient to make graceful drain wait, and it must remain alive
        // through the eventual `/invoke` response.
        let start = start_gate.wait();
        tokio::pin!(start);
        let hard_shutdown = wait_for_atomic_flag(&self.shutdown_started, &self.transition_changed);
        tokio::pin!(hard_shutdown);
        let unavailable = inner.wait_until_execution_unavailable();
        tokio::pin!(unavailable);
        let start_result = tokio::select! {
            biased;
            _ = &mut hard_shutdown => {
                request_guard.set_outcome("generation_lost_before_start");
                anyhow::bail!("Local Node executor shut down before execution start");
            },
            _ = &mut unavailable => {
                request_guard.set_outcome("generation_lost_before_start");
                anyhow::bail!("Local Node executor generation became unavailable before execution start");
            },
            result = &mut start => result,
        };
        if let Err(error) = start_result {
            request_guard.set_outcome("canceled_before_start");
            return Err(error);
        }
        Ok(())
    }

    pub(crate) async fn invoke_with_fingerprint(
        &self,
        request: ExecutorRequest,
        log_line_sender: mpsc::UnboundedSender<LogLine>,
        resident_fingerprint: Option<ResidentGenerationFingerprint>,
        function_execution_start: Option<FunctionExecutionStartGate>,
    ) -> anyhow::Result<InvokeResponse> {
        anyhow::ensure!(
            !self.shutting_down.load(Ordering::Acquire),
            "Local Node executor is shutting down"
        );
        anyhow::ensure!(
            function_execution_start.is_none()
                || matches!(&request, ExecutorRequest::Execute { .. }),
            "Only Node execute requests can have a function execution start barrier"
        );
        let request_kind = request.kind();
        let request_metadata = request_diagnostic_metadata(&request);
        let preparation_descriptor = request
            .preparation_source_package()
            .map(|source_package| PreparationDescriptor { source_package });
        let request_json = JsonValue::try_from(request)?;
        let waiting_guard = WaitingRequestGuard::new(self.activity.clone());
        let (inner, mut request_guard, created) = self
            .acquire_inner(
                request_metadata,
                resident_fingerprint,
                preparation_descriptor.clone(),
            )
            .await?;
        waiting_guard.finish();
        if created {
            self.spawn_watchdog(&inner);
        }
        let client = inner.client.clone();

        if let Some(start_gate) = function_execution_start {
            let descriptor = preparation_descriptor
                .as_ref()
                .context("Gated Node execute request is missing its package descriptor")?;
            self.prepare_and_wait_for_execution_start(
                &inner,
                descriptor,
                start_gate,
                &mut request_guard,
            )
            .await?;
        }

        // Use one absolute deadline for both phases. Reqwest's request timeout
        // also wraps the response body and would otherwise surface as an
        // untyped chunk error before the stream-timeout retirement path runs.
        let request_deadline = tokio::time::Instant::now() + self.config.node_process_timeout;
        let response_result = tokio::time::timeout_at(
            request_deadline,
            client
                .post("http://localhost/invoke")
                .json(&request_json)
                .send(),
        )
        .await;
        let response = match response_result {
            Err(_) => {
                self.retire_inner_if_current(
                    &inner,
                    GenerationRetirementDiagnostics::request(
                        GenerationRetirementReason::RequestTimeout,
                        request_kind,
                        "before_response_headers",
                        "timeout",
                    ),
                )
                .await?;
                request_guard.set_outcome("request_timeout");
                return Ok(InvokeResponse {
                    response: EXECUTE_TIMEOUT_RESPONSE_JSON.clone(),
                    aws_request_id: None,
                });
            },
            Ok(Ok(response)) => response,
            Ok(Err(e)) => {
                if e.is_timeout() {
                    self.retire_inner_if_current(
                        &inner,
                        GenerationRetirementDiagnostics::request(
                            GenerationRetirementReason::RequestTimeout,
                            request_kind,
                            "before_response_headers",
                            "timeout",
                        ),
                    )
                    .await?;
                    request_guard.set_outcome("request_timeout");
                    return Ok(InvokeResponse {
                        response: EXECUTE_TIMEOUT_RESPONSE_JSON.clone(),
                        aws_request_id: None,
                    });
                } else if e.is_connect() {
                    let transport_error_kind = classify_reqwest_transport_error(&e);
                    self.retire_inner_if_current(
                        &inner,
                        GenerationRetirementDiagnostics::request(
                            GenerationRetirementReason::ConnectionError,
                            request_kind,
                            "before_response_headers",
                            transport_error_kind,
                        ),
                    )
                    .await?;
                    request_guard.set_outcome("connection_error");
                    anyhow::bail!("Node server connection failed");
                } else {
                    // The URL and JSON body are fixed by this internal
                    // protocol. Any other submission error means the selected
                    // local server failed before returning response headers.
                    let transport_error_kind = classify_reqwest_transport_error(&e);
                    self.retire_inner_if_current(
                        &inner,
                        GenerationRetirementDiagnostics::request(
                            GenerationRetirementReason::ConnectionError,
                            request_kind,
                            "before_response_headers",
                            transport_error_kind,
                        ),
                    )
                    .await?;
                    request_guard.set_outcome("transport_error");
                    anyhow::bail!("Node server request failed");
                }
            },
        };

        if let Err(e) = response.error_for_status_ref() {
            if e.status() == Some(reqwest::StatusCode::PAYLOAD_TOO_LARGE) {
                request_guard.set_outcome("args_too_large");
                return Err(
                    anyhow::anyhow!(e.without_url()).context(ErrorMetadata::bad_request(
                        "ArgsTooLarge",
                        ARGS_TOO_LARGE_RESPONSE_MESSAGE,
                    )),
                );
            }
            request_guard.set_outcome("http_error");
            anyhow::bail!(
                "Node executor server returned HTTP {}",
                response.status().as_u16()
            );
        }
        let stream = Self::response_stream(response, request_deadline);
        let stream = Box::pin(stream);
        let result = match handle_node_executor_stream(log_line_sender, stream).await {
            Ok(result) => result,
            Err(error) => {
                if let Some(request_error) = error.downcast_ref::<reqwest::Error>() {
                    if request_error.is_timeout() {
                        self.retire_inner_if_current(
                            &inner,
                            GenerationRetirementDiagnostics::request(
                                GenerationRetirementReason::ResponseStreamTimeout,
                                request_kind,
                                "response_body",
                                "timeout",
                            ),
                        )
                        .await?;
                        request_guard.set_outcome("response_stream_timeout");
                        return Ok(InvokeResponse {
                            response: EXECUTE_TIMEOUT_RESPONSE_JSON.clone(),
                            aws_request_id: None,
                        });
                    }
                    // Once response headers exist, every remaining reqwest
                    // error comes from body transport. Reqwest may classify a
                    // truncated body more narrowly than `is_body()`, but the
                    // selected shared process is unhealthy either way.
                    let transport_error_kind = classify_reqwest_transport_error(request_error);
                    self.retire_inner_if_current(
                        &inner,
                        GenerationRetirementDiagnostics::request(
                            GenerationRetirementReason::ConnectionError,
                            request_kind,
                            "response_body",
                            transport_error_kind,
                        ),
                    )
                    .await?;
                    request_guard.set_outcome("connection_error");
                    anyhow::bail!("Node server response stream failed");
                }
                request_guard.set_outcome("response_stream_error");
                anyhow::bail!("Failed to process local Node executor response stream");
            },
        };
        match result {
            Ok(payload) => {
                let outcome = match payload.get("type").and_then(|value| value.as_str()) {
                    Some("success") => "success",
                    Some("error") => "user_error",
                    _ => {
                        request_guard.set_outcome("invalid_response");
                        anyhow::bail!("Node executor returned an invalid response type");
                    },
                };
                let process_exiting = match payload.get("exitingProcess") {
                    Some(JsonValue::Bool(process_exiting)) => *process_exiting,
                    Some(_) => {
                        request_guard.set_outcome("invalid_response");
                        anyhow::bail!(
                            "Node executor returned an invalid exitingProcess response field"
                        );
                    },
                    None => false,
                };
                if process_exiting {
                    self.retire_inner_if_current(
                        &inner,
                        GenerationRetirementDiagnostics::request(
                            GenerationRetirementReason::ProcessExiting,
                            request_kind,
                            "response_payload",
                            "not_applicable",
                        ),
                    )
                    .await?;
                }
                request_guard.set_outcome(outcome);
                Ok(InvokeResponse {
                    response: payload,
                    aws_request_id: None,
                })
            },
            Err(e) => {
                self.retire_inner_if_current(
                    &inner,
                    GenerationRetirementDiagnostics::request(
                        GenerationRetirementReason::ResponseStreamTimeout,
                        request_kind,
                        "response_body",
                        "timeout",
                    ),
                )
                .await?;
                request_guard.set_outcome("response_stream_timeout");
                Ok(e)
            },
        }
    }
}

#[async_trait]
impl NodeExecutor for LocalNodeExecutor {
    fn enable(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn validate_pool_topology(
        &self,
        topology: &model::source_packages::types::NodeExecutorPoolTopology,
    ) -> anyhow::Result<()> {
        if !topology.is_empty() {
            anyhow::bail!(ErrorMetadata::bad_request(
                "NodeExecutorPoolsNotSupported",
                "This Node executor does not support dedicated pools",
            ));
        }
        Ok(())
    }

    fn reconcile_pool_topology(
        &self,
        topology: &model::source_packages::types::NodeExecutorPoolTopology,
        _version: common::types::Timestamp,
    ) -> anyhow::Result<()> {
        self.validate_pool_topology(topology)
    }

    async fn invoke(
        &self,
        request: ExecutorRequest,
        log_line_sender: mpsc::UnboundedSender<LogLine>,
        function_execution_start: Option<FunctionExecutionStartGate>,
    ) -> anyhow::Result<InvokeResponse> {
        self.invoke_with_fingerprint(request, log_line_sender, None, function_execution_start)
            .await
    }

    fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            self.transition_changed.notify_waiters();
            let state = self.state.clone();
            tokio::spawn(async move {
                Self::preempt_hot_transition_state(&state, "shutdown_canceled").await;
                let _ =
                    Self::finish_hot_transition_cleanup_state(&state, "shutdown_canceled").await;
            });
            return;
        }
        // Wake pre-start reservations synchronously. The asynchronous
        // retirement task below may not be polled before a scheduler releases
        // an otherwise ready action.
        self.transition_changed.notify_waiters();
        let state = self.state.clone();
        tokio::spawn(async move {
            Self::preempt_hot_transition_state(&state, "shutdown_canceled").await;
            let retirement_result = loop {
                let (expected, already_retiring) = {
                    let state = state.lock().await;
                    if let Some(retiring) = &state.retiring {
                        (retiring.clone(), true)
                    } else if let Some(inner) = &state.inner {
                        (inner.clone(), false)
                    } else {
                        break Ok(());
                    }
                };
                let result = if already_retiring {
                    Self::finish_retiring_inner_for_shutdown(&state, &expected)
                        .await
                        .map(|()| true)
                } else {
                    Self::retire_inner_state(
                        &state,
                        &expected,
                        GenerationRetirementDiagnostics::shutdown(),
                    )
                    .await
                };
                match result {
                    Ok(true) => break Ok(()),
                    // Another retirement can move the generation between the
                    // state snapshot and the explicit-shutdown transition.
                    Ok(false) => continue,
                    Err(error) => break Err((expected, error)),
                }
            };
            if let Err((expected, _)) = retirement_result {
                tracing::error!(
                    pool_name = %expected.pool_name,
                    generation = expected.generation,
                    "Failed to terminate and reap local Node executor child during shutdown"
                );
            }
            if Self::finish_hot_transition_cleanup_state(&state, "shutdown_canceled")
                .await
                .is_err()
            {
                tracing::error!(
                    "Failed to terminate and reap a hot-transition local Node executor child \
                     during shutdown"
                );
            }
        });
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        fs::FileTimes,
        future,
        os::unix::fs::{
            symlink,
            PermissionsExt,
        },
    };

    use common::{
        execution_start::function_execution_start_barrier,
        types::Timestamp,
    };
    use futures::future::join_all;
    use tokio::{
        io::{
            AsyncReadExt,
            AsyncWriteExt,
        },
        net::UnixListener,
        sync::oneshot,
    };

    use super::*;

    fn test_config() -> LocalNodeExecutorConfig {
        LocalNodeExecutorConfig {
            pool_name: Arc::from("default"),
            node_process_timeout: Duration::from_secs(1),
            callback_initial_backoff: None,
            health_check_timeout: Duration::from_millis(10),
            watchdog_interval: Duration::from_millis(10),
            watchdog_failure_threshold: 2,
            max_old_space_size_mib: 128,
            max_rss_bytes: 256 * MIB_BYTES,
            memory_pressure: MemoryPressureSignal::default(),
            memory_pressure_min_rss_bytes: 192 * MIB_BYTES,
            memory_pressure_grace: Duration::from_secs(5),
            max_generation_age: Duration::from_secs(60),
            max_imported_source_packages: 100,
            diagnostics_dir: None,
            diagnostic_pruning_in_progress: Arc::new(AtomicBool::new(false)),
            surge_coordinator: SurgeCoordinator::new(),
        }
    }

    fn test_source_package() -> crate::executor::SourcePackage {
        crate::executor::SourcePackage {
            bundled_source: crate::executor::Package {
                uri: "https://packages.invalid/source.zip".to_owned(),
                key: common::types::ObjectKey::try_from("source-package").unwrap(),
                sha256: common::sha256::Sha256::hash(b"source"),
            },
            external_deps: None,
            download_url_expiration: Instant::now() + Duration::from_secs(5 * 60),
        }
    }

    fn test_request_metadata() -> RequestDiagnosticMetadata {
        RequestDiagnosticMetadata {
            request_kind: "build_deps",
            module_path: None,
            function_name: None,
        }
    }

    fn test_request_retirement(
        reason: GenerationRetirementReason,
    ) -> GenerationRetirementDiagnostics {
        GenerationRetirementDiagnostics::request(
            reason,
            "build_deps",
            "before_response_headers",
            "other",
        )
    }

    #[tokio::test]
    async fn surge_coordinator_prioritizes_deployment_waiters() {
        let coordinator = SurgeCoordinator::new();
        let held = coordinator
            .acquire(SurgePriority::Routine, Arc::from("held"))
            .await;
        let routine_coordinator = coordinator.clone();
        let routine = tokio::spawn(async move {
            routine_coordinator
                .acquire(SurgePriority::Routine, Arc::from("routine"))
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if coordinator
                    .state
                    .lock()
                    .expect("Local Node surge coordinator lock poisoned")
                    .routine
                    .len()
                    == 1
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let deployment_coordinator = coordinator.clone();
        let deployment = tokio::spawn(async move {
            deployment_coordinator
                .acquire(SurgePriority::Deployment, Arc::from("deployment"))
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if coordinator
                    .state
                    .lock()
                    .expect("Local Node surge coordinator lock poisoned")
                    .deployment
                    .len()
                    == 1
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        held.release();
        let deployment_permit = tokio::time::timeout(Duration::from_secs(1), deployment)
            .await
            .unwrap()
            .unwrap();
        assert!(!routine.is_finished());
        deployment_permit.release();
        routine.await.unwrap().release();
    }

    #[tokio::test]
    async fn force_preemption_rejects_deployment_candidates_and_wakes_each_force_request() {
        let coordinator = SurgeCoordinator::new();
        let deployment = coordinator
            .acquire(SurgePriority::Deployment, Arc::from("deployment"))
            .await;
        assert_eq!(coordinator.force_preempt_reclaimable(), None);
        deployment.set_phase("draining");
        assert_eq!(coordinator.force_preempt_reclaimable(), Some("draining"));
        let first_request = deployment.wait_for_preemption_request_after(0).await;
        assert_eq!(coordinator.force_preempt_reclaimable(), Some("draining"));
        assert!(
            deployment
                .wait_for_preemption_request_after(first_request)
                .await
                > first_request
        );
        assert!(deployment.preempted());
        deployment.confirm_direct_child_reaped();
        deployment.release();

        let routine = coordinator
            .acquire(SurgePriority::Routine, Arc::from("routine"))
            .await;
        assert_eq!(coordinator.force_preempt_reclaimable(), Some("candidate"));
        assert_eq!(coordinator.force_preempt_reclaimable(), Some("candidate"));
        assert!(routine.preempted());
        tokio::time::timeout(Duration::from_secs(1), routine.wait_until_preempted())
            .await
            .expect("A preemption published before waiter registration was lost");
        routine.release();
    }

    #[tokio::test]
    async fn dropped_reservation_releases_capacity_before_child_startup() {
        let coordinator = SurgeCoordinator::new();
        let permit = coordinator
            .acquire(SurgePriority::Deployment, Arc::from("deployment"))
            .await;

        drop(permit);

        assert!(coordinator
            .state
            .lock()
            .expect("Local Node surge coordinator lock poisoned")
            .occupied
            .is_none());
    }

    #[tokio::test]
    async fn dropped_child_owning_surge_permit_preserves_occupied_capacity() {
        let coordinator = SurgeCoordinator::new();
        let permit = coordinator
            .acquire(SurgePriority::Routine, Arc::from("routine"))
            .await;
        permit.require_confirmed_cleanup();

        drop(permit);

        assert!(coordinator
            .state
            .lock()
            .expect("Local Node surge coordinator lock poisoned")
            .occupied
            .is_some());
    }

    #[tokio::test]
    async fn confirmed_child_reaping_restores_drop_release() {
        let coordinator = SurgeCoordinator::new();
        let permit = coordinator
            .acquire(SurgePriority::Routine, Arc::from("routine"))
            .await;
        permit.require_confirmed_cleanup();
        permit.confirm_direct_child_reaped();

        drop(permit);

        assert!(coordinator
            .state
            .lock()
            .expect("Local Node surge coordinator lock poisoned")
            .occupied
            .is_none());
    }

    #[tokio::test]
    async fn shared_surge_lease_stays_occupied_until_the_session_owner_releases_it() {
        let coordinator = SurgeCoordinator::new();
        let session = coordinator
            .acquire(SurgePriority::Deployment, Arc::from("deployment"))
            .await;
        let transition = session.clone();
        transition.require_confirmed_cleanup();
        transition.confirm_direct_child_reaped();

        transition.release();
        assert!(coordinator
            .state
            .lock()
            .expect("Local Node surge coordinator lock poisoned")
            .occupied
            .is_some());

        session.release();
        assert!(coordinator
            .state
            .lock()
            .expect("Local Node surge coordinator lock poisoned")
            .occupied
            .is_none());
    }

    #[tokio::test]
    async fn hot_transition_guard_publishes_task_failure() {
        let expected = test_inner(1).await;
        let status = Arc::new(HotTransitionStatus {
            failed: AtomicBool::new(false),
            promoted: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
        });
        let changed = Arc::new(Notify::new());
        let state = Arc::new(Mutex::new(LocalNodeExecutorState {
            inner: Some(expected.clone()),
            retiring: None,
            hot_transition: None,
            replacement_for_generation: None,
            next_generation: 1,
            next_transition: 1,
        }));
        let cleanup = HotTransitionCleanupOwner::new(
            1,
            &state,
            changed.clone(),
            status.clone(),
            GenerationRetirementReason::TopologyChange,
            expected.pool_name.clone(),
        );
        state.lock().await.hot_transition = Some(HotTransition::Candidate(CandidateTransition {
            token: 1,
            expected: expected.clone(),
            target_fingerprint: None,
            descriptor: None,
            startup_started: false,
            reason: GenerationRetirementReason::TopologyChange,
            canceled: Arc::new(AtomicBool::new(false)),
            canceled_changed: Arc::new(Notify::new()),
            status: status.clone(),
            cleanup: cleanup.clone(),
        }));
        drop(HotTransitionTaskGuard::new(
            status.clone(),
            changed,
            expected.clone(),
            GenerationRetirementReason::TopologyChange,
            cleanup.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(1), cleanup.wait_until_confirmed())
            .await
            .unwrap();
        assert!(status.failed.load(Ordering::Acquire));
        assert!(!status.cleanup_failed.load(Ordering::Acquire));
        assert!(state.lock().await.hot_transition.is_none());
        expected.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn package_preparation_bounds_chunked_responses_while_streaming() {
        let socket_dir = TempDir::new().unwrap();
        let socket_path = socket_dir.path().join("executor.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            let oversized = vec![b'a'; MAX_PREPARATION_RESPONSE_BYTES + 1];
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: \
                         close\r\n\r\n{:X}\r\n",
                        oversized.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket.write_all(&oversized).await.unwrap();
            socket.write_all(b"\r\n0\r\n\r\n").await.unwrap();
        });
        let client = Client::builder()
            .no_proxy()
            .unix_socket(socket_path)
            .build()
            .unwrap();
        let generation = test_inner_with_client(1, client).await;
        let descriptor = PreparationDescriptor {
            source_package: test_source_package(),
        };

        let error = generation
            .prepare_package(&descriptor, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("preparation response exceeded its size limit"));
        server.await.unwrap();
        generation.terminate().await.unwrap();
    }

    async fn test_inner_with_preparation_response(
        response_body: &'static str,
    ) -> (Arc<InnerLocalNodeExecutor>, tokio::task::JoinHandle<()>) {
        let socket_dir = TempDir::new().unwrap();
        let socket_path = socket_dir.path().join("executor.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \
             {}\r\nConnection: close\r\n\r\n{response_body}",
            response_body.len()
        );
        let server = tokio::spawn(async move {
            let _socket_dir = socket_dir;
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let client = Client::builder()
            .no_proxy()
            .unix_socket(socket_path)
            .build()
            .unwrap();
        (test_inner_with_client(1, client).await, server)
    }

    #[tokio::test]
    async fn execution_start_ready_holds_exact_generation_admission() {
        let (inner, server) = test_inner_with_preparation_response(r#"{"type":"success"}"#).await;
        let (executor, _) = test_executor(inner.clone(), test_config());
        let guard = match executor
            .acquire_existing_inner(test_request_metadata(), None)
            .await
            .unwrap()
        {
            InnerAcquisition::Ready { guard, .. } => guard,
            InnerAcquisition::Draining(_)
            | InnerAcquisition::Transition(_)
            | InnerAcquisition::Missing => panic!("Test generation was not admitted"),
        };
        let descriptor = PreparationDescriptor {
            source_package: test_source_package(),
        };
        let (mut controller, gate) = function_execution_start_barrier();
        let execution_inner = inner.clone();
        let task = tokio::spawn(async move {
            let mut guard = guard;
            executor
                .prepare_and_wait_for_execution_start(
                    &execution_inner,
                    &descriptor,
                    gate,
                    &mut guard,
                )
                .await
        });

        controller.wait_until_ready().await.unwrap();
        assert!(!task.is_finished(), "Execution passed the start gate early");
        assert_eq!(inner.active_requests.load(Ordering::Acquire), 1);
        drop(controller);
        assert!(task.await.unwrap().is_err());
        assert_eq!(inner.active_requests.load(Ordering::Acquire), 0);
        server.await.unwrap();
        inner.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn graceful_drain_waits_for_execution_start_reservation() {
        let (inner, server) = test_inner_with_preparation_response(r#"{"type":"success"}"#).await;
        let (executor, state) = test_executor(inner.clone(), test_config());
        let guard = match executor
            .acquire_existing_inner(test_request_metadata(), None)
            .await
            .unwrap()
        {
            InnerAcquisition::Ready { guard, .. } => guard,
            InnerAcquisition::Draining(_)
            | InnerAcquisition::Transition(_)
            | InnerAcquisition::Missing => panic!("Test generation was not admitted"),
        };
        let descriptor = PreparationDescriptor {
            source_package: test_source_package(),
        };
        let (mut controller, gate) = function_execution_start_barrier();
        let execution_inner = inner.clone();
        let execution = tokio::spawn(async move {
            let mut guard = guard;
            executor
                .prepare_and_wait_for_execution_start(
                    &execution_inner,
                    &descriptor,
                    gate,
                    &mut guard,
                )
                .await
        });

        controller.wait_until_ready().await.unwrap();
        let diagnostics = GenerationRetirementDiagnostics::topology_change();
        assert!(
            LocalNodeExecutor::start_draining_inner_state(&state, &inner, diagnostics.reason).await
        );
        let retirement_state = state.clone();
        let retirement_inner = inner.clone();
        let retirement = tokio::spawn(async move {
            LocalNodeExecutor::finish_draining_inner_state(
                &retirement_state,
                &retirement_inner,
                diagnostics,
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(!retirement.is_finished());
        assert!(!inner.execution_unavailable.load(Ordering::Acquire));

        controller.start().unwrap();
        execution.await.unwrap().unwrap();
        assert!(retirement.await.unwrap().unwrap());
        server.await.unwrap();
        assert!(inner.retired.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn hard_unavailability_cancels_execution_start_reservation() {
        let (inner, server) = test_inner_with_preparation_response(r#"{"type":"success"}"#).await;
        let (executor, _) = test_executor(inner.clone(), test_config());
        let guard = ActiveRequestGuard::new(inner.clone(), test_request_metadata());
        let descriptor = PreparationDescriptor {
            source_package: test_source_package(),
        };
        let (mut controller, gate) = function_execution_start_barrier();
        let execution_inner = inner.clone();
        let task = tokio::spawn(async move {
            let mut guard = guard;
            executor
                .prepare_and_wait_for_execution_start(
                    &execution_inner,
                    &descriptor,
                    gate,
                    &mut guard,
                )
                .await
        });

        controller.wait_until_ready().await.unwrap();
        inner.mark_execution_unavailable();
        let error = task.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("became unavailable"));
        assert_eq!(inner.active_requests.load(Ordering::Acquire), 0);
        server.await.unwrap();
        inner.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn hard_unavailability_wins_a_simultaneous_start_release() {
        let (inner, server) = test_inner_with_preparation_response(r#"{"type":"success"}"#).await;
        let (executor, _) = test_executor(inner.clone(), test_config());
        let guard = ActiveRequestGuard::new(inner.clone(), test_request_metadata());
        let descriptor = PreparationDescriptor {
            source_package: test_source_package(),
        };
        let (mut controller, gate) = function_execution_start_barrier();
        let execution_inner = inner.clone();
        let task = tokio::spawn(async move {
            let mut guard = guard;
            executor
                .prepare_and_wait_for_execution_start(
                    &execution_inner,
                    &descriptor,
                    gate,
                    &mut guard,
                )
                .await
        });

        controller.wait_until_ready().await.unwrap();
        controller.start().unwrap();
        inner.mark_execution_unavailable();
        let error = task.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("became unavailable"));
        assert_eq!(inner.active_requests.load(Ordering::Acquire), 0);
        server.await.unwrap();
        inner.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn package_preparation_failure_happens_before_ready() {
        let (inner, server) = test_inner_with_preparation_response(r#"{"type":"error"}"#).await;
        let (executor, _) = test_executor(inner.clone(), test_config());
        let guard = ActiveRequestGuard::new(inner.clone(), test_request_metadata());
        let descriptor = PreparationDescriptor {
            source_package: test_source_package(),
        };
        let (mut controller, gate) = function_execution_start_barrier();
        let execution_inner = inner.clone();
        let task = tokio::spawn(async move {
            let mut guard = guard;
            executor
                .prepare_and_wait_for_execution_start(
                    &execution_inner,
                    &descriptor,
                    gate,
                    &mut guard,
                )
                .await
        });

        assert!(controller.wait_until_ready().await.is_err());
        let error = task.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("before execution admission"));
        assert_eq!(inner.active_requests.load(Ordering::Acquire), 0);
        server.await.unwrap();
        inner.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn expired_package_authority_fails_before_ready() {
        let inner = test_inner(1).await;
        let (executor, _) = test_executor(inner.clone(), test_config());
        let guard = ActiveRequestGuard::new(inner.clone(), test_request_metadata());
        let mut source_package = test_source_package();
        source_package.download_url_expiration = Instant::now();
        let descriptor = PreparationDescriptor { source_package };
        let (mut controller, gate) = function_execution_start_barrier();
        let execution_inner = inner.clone();
        let task = tokio::spawn(async move {
            let mut guard = guard;
            executor
                .prepare_and_wait_for_execution_start(
                    &execution_inner,
                    &descriptor,
                    gate,
                    &mut guard,
                )
                .await
        });

        assert!(controller.wait_until_ready().await.is_err());
        let error = task.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("authority expired"));
        assert_eq!(inner.active_requests.load(Ordering::Acquire), 0);
        inner.terminate().await.unwrap();
    }

    #[test]
    fn health_runtime_stats_require_a_complete_pair_and_valid_counters() {
        let upstream: NodeExecutorHealth =
            serde_json::from_value(serde_json::json!({ "status": "ok" })).unwrap();
        assert_eq!(upstream.runtime_stats_supported(), Some(false));

        let current = NodeExecutorHealth {
            status: "ok".to_string(),
            package_cache: Some(NodePackageCacheStats::default()),
            stack_trace: Some(NodeStackTraceStats::default()),
        };
        assert_eq!(current.runtime_stats_supported(), Some(true));
        assert_eq!(
            current.valid_runtime_stats_support(
                &NodePackageCacheStats::default(),
                &NodeStackTraceStats::default(),
            ),
            Some(true)
        );

        let invalid_duration = NodeExecutorHealth {
            status: "ok".to_string(),
            package_cache: Some(NodePackageCacheStats::default()),
            stack_trace: Some(NodeStackTraceStats {
                duration_ms: -1.0,
                ..NodeStackTraceStats::default()
            }),
        };
        assert_eq!(
            invalid_duration.valid_runtime_stats_support(
                &NodePackageCacheStats::default(),
                &NodeStackTraceStats::default(),
            ),
            None
        );

        let partial = NodeExecutorHealth {
            status: "ok".to_string(),
            package_cache: Some(NodePackageCacheStats::default()),
            stack_trace: None,
        };
        assert_eq!(partial.runtime_stats_supported(), None);
        assert!(
            serde_json::from_value::<NodeExecutorHealth>(serde_json::json!({
                "status": "ok",
                "packageCache": null,
                "stackTrace": null,
            }))
            .is_err()
        );

        let imported_package_regression = NodeExecutorHealth {
            status: "ok".to_string(),
            package_cache: Some(NodePackageCacheStats {
                imported_source_packages: 1,
                ..NodePackageCacheStats::default()
            }),
            stack_trace: Some(NodeStackTraceStats::default()),
        };
        assert_eq!(
            imported_package_regression.valid_runtime_stats_support(
                &NodePackageCacheStats {
                    imported_source_packages: 2,
                    ..NodePackageCacheStats::default()
                },
                &NodeStackTraceStats::default(),
            ),
            None
        );
    }

    #[test]
    fn config_rejects_rss_threshold_at_or_below_old_space_allowance() {
        let config = test_config();
        config.validate().unwrap();

        let mut equal = config.clone();
        equal.max_rss_bytes = equal.old_space_bytes();
        assert!(equal.validate().is_err());

        let mut below = config;
        below.max_rss_bytes = below.old_space_bytes() - 1;
        assert!(below.validate().is_err());
    }

    #[test]
    fn proactive_retirement_thresholds_are_inclusive_and_prioritized() {
        let config = test_config();
        assert_eq!(
            proactive_retirement_reason(
                &config,
                config.max_generation_age - Duration::from_nanos(1),
                Some(config.max_rss_bytes - 1),
                config.max_imported_source_packages - 1,
                None,
            ),
            None
        );
        assert_eq!(
            proactive_retirement_reason(
                &config,
                config.max_generation_age,
                Some(config.max_rss_bytes - 1),
                config.max_imported_source_packages - 1,
                None,
            ),
            Some(GenerationRetirementReason::AgeLimit)
        );
        assert_eq!(
            proactive_retirement_reason(
                &config,
                config.max_generation_age,
                Some(config.max_rss_bytes - 1),
                config.max_imported_source_packages,
                None,
            ),
            Some(GenerationRetirementReason::PackageLimit)
        );
        assert_eq!(
            proactive_retirement_reason(
                &config,
                config.max_generation_age,
                Some(config.max_rss_bytes),
                config.max_imported_source_packages,
                None,
            ),
            Some(GenerationRetirementReason::RssLimit)
        );
    }

    #[test]
    fn cgroup_pressure_retirement_requires_grace_and_material_rss() {
        let config = test_config();
        let below_hard_limit = config.max_rss_bytes - 1;
        assert_eq!(
            proactive_retirement_reason(
                &config,
                Duration::ZERO,
                Some(below_hard_limit),
                0,
                Some(config.memory_pressure_grace - Duration::from_nanos(1)),
            ),
            None
        );
        assert_eq!(
            proactive_retirement_reason(
                &config,
                Duration::ZERO,
                Some(config.memory_pressure_min_rss_bytes - 1),
                0,
                Some(config.memory_pressure_grace),
            ),
            None
        );
        assert_eq!(
            proactive_retirement_reason(
                &config,
                Duration::ZERO,
                None,
                0,
                Some(config.memory_pressure_grace),
            ),
            None
        );
        assert_eq!(
            proactive_retirement_reason(
                &config,
                Duration::ZERO,
                Some(config.memory_pressure_min_rss_bytes),
                0,
                Some(config.memory_pressure_grace),
            ),
            Some(GenerationRetirementReason::CgroupPressure)
        );
        assert_eq!(
            proactive_retirement_reason(
                &config,
                Duration::ZERO,
                Some(config.max_rss_bytes),
                0,
                Some(config.memory_pressure_grace),
            ),
            Some(GenerationRetirementReason::CgroupPressure)
        );
    }

    #[test]
    fn memory_pressure_observation_requires_continuous_grace() {
        let first_entry = Instant::now();
        let mut observation = MemoryPressureObservation::new(true, first_entry);
        assert_eq!(
            observation.active_for(first_entry + Duration::from_secs(59)),
            Some(Duration::from_secs(59))
        );

        observation.observe_publication(false, first_entry + Duration::from_secs(59));
        assert!(!observation.is_active());
        assert_eq!(
            observation.active_for(first_entry + Duration::from_secs(60)),
            None
        );

        let second_entry = first_entry + Duration::from_secs(60);
        observation.observe_publication(true, second_entry);
        assert_eq!(
            observation.active_for(second_entry + Duration::from_secs(59)),
            Some(Duration::from_secs(59))
        );
        assert_eq!(
            observation.active_for(second_entry + Duration::from_secs(60)),
            Some(Duration::from_secs(60))
        );

        // If watch coalesces false -> true, the active publication still
        // restarts the grace even though the last observed value was true.
        observation.observe_publication(true, second_entry + Duration::from_secs(60));
        assert_eq!(
            observation.active_for(second_entry + Duration::from_secs(119)),
            Some(Duration::from_secs(59))
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn process_rss_parser_requires_one_kib_value() {
        assert_eq!(
            parse_process_rss("Name:\tnode\nVmRSS:\t12345 kB\nVmSize:\t99999 kB\n").unwrap(),
            12_641_280
        );
        assert!(parse_process_rss("Name:\tnode\n").is_err());
        assert!(parse_process_rss("VmRSS:\t12345 MB\n").is_err());
        assert!(parse_process_rss("VmRSS:\t1 kB\nVmRSS:\t2 kB\n").is_err());
    }

    #[test]
    fn process_stat_parser_handles_spaces_and_parentheses_in_command() {
        let process = parse_process_stat(
            "123 (node worker (main)) R 1 2 3 4 5 6 7 8 9 10 101 202 13 14 15 16 8 18 303\n",
        )
        .unwrap();
        assert_eq!(process.state, 'R');
        assert_eq!(process.user_ticks, 101);
        assert_eq!(process.system_ticks, 202);
        assert_eq!(process.thread_count, 8);
        assert_eq!(process.start_time_ticks, 303);

        assert!(parse_process_stat("123 node R 1 2 3").is_err());
        assert!(parse_process_stat("123 (node) running 1 2 3").is_err());
        assert!(parse_process_stat("123 (node) R 1 2 3").is_err());
    }

    #[test]
    fn private_diagnostic_directory_rejects_symlinks_and_nonempty_nonprivate_directories() {
        let parent = TempDir::new().unwrap();
        let directory = parent.path().join("artifacts");
        create_private_diagnostic_directory(&directory).unwrap();
        assert_eq!(
            fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
            0o700
        );

        let symlink_path = parent.path().join("artifact-link");
        symlink(&directory, &symlink_path).unwrap();
        assert!(create_private_diagnostic_directory(&symlink_path).is_err());

        let nonempty_directory = parent.path().join("nonempty");
        fs::create_dir(&nonempty_directory).unwrap();
        fs::write(
            nonempty_directory.join("node-report-existing.json"),
            b"keep",
        )
        .unwrap();
        fs::set_permissions(&nonempty_directory, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(create_private_diagnostic_directory(&nonempty_directory).is_err());
        assert_eq!(
            fs::metadata(&nonempty_directory)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
    }

    #[test]
    fn diagnostic_retention_ignores_unrecognized_recent_files_and_directories() {
        let diagnostics_dir = TempDir::new().unwrap();
        let old_modified = SystemTime::now()
            .checked_sub(MIN_DIAGNOSTIC_ARTIFACT_PRUNE_AGE + Duration::from_secs(1))
            .unwrap();
        for index in 0..=MAX_DIAGNOSTIC_ARTIFACTS {
            let path = diagnostics_dir
                .path()
                .join(format!("node-first-miss-{index}.json"));
            fs::write(&path, b"diagnostic").unwrap();
            fs::File::options()
                .write(true)
                .open(path)
                .unwrap()
                .set_times(FileTimes::new().set_modified(old_modified))
                .unwrap();
        }
        let recent_artifact = diagnostics_dir
            .path()
            .join("node-profile-recent.cpuprofile");
        fs::write(&recent_artifact, b"diagnostic").unwrap();
        let stale_partial = diagnostics_dir
            .path()
            .join("node-diagnostic-partial-stale.partial");
        fs::write(&stale_partial, b"partial").unwrap();
        fs::File::options()
            .write(true)
            .open(&stale_partial)
            .unwrap()
            .set_times(FileTimes::new().set_modified(old_modified))
            .unwrap();
        let recent_partial = diagnostics_dir
            .path()
            .join("node-first-miss-partial-recent.json.partial");
        fs::write(&recent_partial, b"partial").unwrap();
        let future_partial = diagnostics_dir
            .path()
            .join("node-diagnostic-partial-future.partial");
        fs::write(&future_partial, b"partial").unwrap();
        let future_modified = SystemTime::now()
            .checked_add(MAX_DIAGNOSTIC_CLOCK_SKEW + Duration::from_secs(1))
            .unwrap();
        fs::File::options()
            .write(true)
            .open(&future_partial)
            .unwrap()
            .set_times(FileTimes::new().set_modified(future_modified))
            .unwrap();
        let unrelated_file = diagnostics_dir.path().join("operator-notes.json");
        fs::write(&unrelated_file, b"keep").unwrap();
        let misleading_file = diagnostics_dir.path().join("node-report-keep.txt");
        fs::write(&misleading_file, b"keep").unwrap();
        let recognized_directory = diagnostics_dir
            .path()
            .join("node-profile-directory.cpuprofile");
        fs::create_dir(&recognized_directory).unwrap();

        prune_diagnostic_artifacts(diagnostics_dir.path()).unwrap();

        let retained_artifacts = fs::read_dir(diagnostics_dir.path())
            .unwrap()
            .map(Result::unwrap)
            .filter(|entry| {
                entry.file_type().unwrap().is_file()
                    && is_local_node_diagnostic_artifact(&entry.path())
            })
            .count();
        assert_eq!(retained_artifacts, MAX_DIAGNOSTIC_ARTIFACTS);
        assert!(recent_artifact.exists());
        assert!(!stale_partial.exists());
        assert!(recent_partial.exists());
        assert!(future_partial.exists());
        assert!(unrelated_file.exists());
        assert!(misleading_file.exists());
        assert!(recognized_directory.is_dir());
    }

    #[test]
    fn diagnostic_copy_out_requires_complete_private_generation_local_sources() {
        let source_dir = TempDir::new().unwrap();
        let diagnostics_dir = TempDir::new().unwrap();
        let paths = NodeDiagnosticPaths::new(&source_dir, diagnostics_dir.path(), 1);

        assert_eq!(paths.report.source_path.parent(), Some(source_dir.path()));
        assert_eq!(
            paths.report.destination_path.parent(),
            Some(diagnostics_dir.path())
        );
        assert_eq!(paths.profile_source_path.parent(), Some(source_dir.path()));
        assert_eq!(paths.profile_path.parent(), Some(diagnostics_dir.path()));

        fs::write(&paths.report.source_path, b"stale report").unwrap();
        fs::set_permissions(&paths.report.source_path, fs::Permissions::from_mode(0o644)).unwrap();
        let stale_report_metadata = fs::metadata(&paths.report.source_path).unwrap();
        let report_source_identity =
            prepare_private_diagnostic_file(&paths.report.source_path).unwrap();
        assert_eq!(
            report_source_identity,
            DiagnosticFileIdentity {
                device: stale_report_metadata.dev(),
                inode: stale_report_metadata.ino(),
            }
        );
        assert_eq!(fs::metadata(&paths.report.source_path).unwrap().len(), 0);
        assert_eq!(
            fs::metadata(&paths.report.source_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        fs::write(&paths.report.source_path, br#"{"complete":"#).unwrap();
        assert!(
            !try_publish_node_diagnostic_report(&paths.report, report_source_identity).unwrap()
        );
        assert!(!paths.report.destination_path.exists());

        fs::write(
            &paths.report.source_path,
            br#"{
                "complete": true,
                "environmentVariables": {"SECRET": "value"},
                "header": {"networkInterfaces": [{"address": "127.0.0.1"}]},
                "libuv": [{
                    "type": "tcp",
                    "localEndpoint": {"host": "127.0.0.1", "port": 1},
                    "remoteEndpoint": {"host": "127.0.0.1", "port": 2}
                }]
            }"#,
        )
        .unwrap();
        assert!(try_publish_node_diagnostic_report(&paths.report, report_source_identity).unwrap());
        let published: JsonValue =
            serde_json::from_slice(&fs::read(&paths.report.destination_path).unwrap()).unwrap();
        assert_eq!(published["complete"], true);
        assert!(published.get("environmentVariables").is_none());
        assert!(published["header"].get("networkInterfaces").is_none());
        assert!(published["libuv"][0].get("localEndpoint").is_none());
        assert!(published["libuv"][0].get("remoteEndpoint").is_none());
        assert_eq!(published["libuv"][0]["type"], "tcp");
        assert_eq!(
            fs::metadata(&paths.report.destination_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert!(try_publish_node_diagnostic_report(&paths.report, report_source_identity).is_err());

        let replaced_paths =
            NodeDiagnosticPaths::new(&source_dir, diagnostics_dir.path(), 2).report;
        let replaced_identity =
            prepare_private_diagnostic_file(&replaced_paths.source_path).unwrap();
        fs::rename(
            &replaced_paths.source_path,
            source_dir.path().join("original-report-source"),
        )
        .unwrap();
        prepare_private_diagnostic_file(&replaced_paths.source_path).unwrap();
        fs::write(&replaced_paths.source_path, br#"{"complete":true}"#).unwrap();
        assert!(try_publish_node_diagnostic_report(&replaced_paths, replaced_identity).is_err());
        assert!(!replaced_paths.destination_path.exists());

        prepare_private_diagnostic_file(&paths.profile_source_path).unwrap();
        fs::write(&paths.profile_source_path, b"profile").unwrap();
        copy_private_diagnostic_artifact(
            &paths.profile_source_path,
            &paths.profile_path,
            MAX_DIAGNOSTIC_PROFILE_BYTES,
        )
        .unwrap();
        assert_eq!(fs::read(&paths.profile_path).unwrap(), b"profile");
        assert_eq!(
            fs::metadata(&paths.profile_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );

        let profile_link = source_dir.path().join("linked-profile-source");
        fs::hard_link(&paths.profile_source_path, &profile_link).unwrap();
        let linked_destination = diagnostics_dir
            .path()
            .join("node-profile-linked.cpuprofile");
        assert!(copy_private_diagnostic_artifact(
            &paths.profile_source_path,
            &linked_destination,
            MAX_DIAGNOSTIC_PROFILE_BYTES,
        )
        .is_err());
        assert!(!linked_destination.exists());
        fs::remove_file(profile_link).unwrap();

        fs::remove_file(&paths.profile_source_path).unwrap();
        symlink(&paths.report.destination_path, &paths.profile_source_path).unwrap();
        let rejected_destination = diagnostics_dir
            .path()
            .join("node-profile-rejected.cpuprofile");
        assert!(copy_private_diagnostic_artifact(
            &paths.profile_source_path,
            &rejected_destination,
            MAX_DIAGNOSTIC_PROFILE_BYTES,
        )
        .is_err());
        assert!(!rejected_destination.exists());
    }

    #[tokio::test]
    async fn profiler_control_connect_retries_worker_startup_race() {
        let socket_dir = TempDir::new().unwrap();
        let socket_path = socket_dir.path().join("profiler.sock");
        let connecting_path = socket_path.clone();
        let connecting =
            tokio::spawn(async move { connect_main_thread_profiler(&connecting_path).await });

        tokio::time::sleep(Duration::from_millis(75)).await;
        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        let (accepted, _) = listener.accept().await.unwrap();
        let connected = connecting.await.unwrap().unwrap();

        drop(accepted);
        drop(connected);
    }

    #[tokio::test]
    async fn profiler_control_half_closes_request_before_reading_response() {
        let source_dir = TempDir::new().unwrap();
        let diagnostics_dir = TempDir::new().unwrap();
        let paths = NodeDiagnosticPaths::new(&source_dir, diagnostics_dir.path(), 1);
        let listener = UnixListener::bind(&paths.control_path).unwrap();
        let server = tokio::spawn(async move {
            let (mut accepted, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            accepted.read_to_end(&mut request).await.unwrap();
            assert_eq!(request, b"profile\n");
            accepted.write_all(b"already_started\n").await.unwrap();
        });

        let outcome = trigger_main_thread_profile(&paths, test_inner(1).await).await;
        assert!(matches!(
            outcome,
            FirstMissDiagnosticOutcome::CpuProfileAlreadyStarted
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn node_version_probe_stops_reading_oversized_output() {
        let temp_dir = TempDir::new().unwrap();
        let node_path = temp_dir.path().join("node");
        fs::write(
            &node_path,
            r#"#!/bin/sh
printf 'v24.'
while true; do
  printf x
done
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(&node_path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&node_path, permissions).unwrap();

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            InnerLocalNodeExecutor::check_node_version(&node_path),
        )
        .await
        .expect("Oversized version output was not terminated promptly");
        assert!(result.is_err());
    }

    async fn test_inner(generation: u64) -> Arc<InnerLocalNodeExecutor> {
        test_inner_with_client(generation, Client::builder().build().unwrap()).await
    }

    async fn test_inner_with_client(
        generation: u64,
        client: Client,
    ) -> Arc<InnerLocalNodeExecutor> {
        test_inner_with_client_and_fingerprint(generation, client, None).await
    }

    async fn test_inner_with_fingerprint(
        generation: u64,
        resident_fingerprint: ResidentGenerationFingerprint,
    ) -> Arc<InnerLocalNodeExecutor> {
        test_inner_with_client_and_fingerprint(
            generation,
            Client::builder().build().unwrap(),
            Some(resident_fingerprint),
        )
        .await
    }

    async fn test_inner_with_client_and_fingerprint(
        generation: u64,
        client: Client,
        resident_fingerprint: Option<ResidentGenerationFingerprint>,
    ) -> Arc<InnerLocalNodeExecutor> {
        let source_dir = TempDir::new().unwrap();
        let server_handle = TokioCommand::new("sleep")
            .arg("300")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = server_handle
            .id()
            .expect("Test local Node executor child has no process id");
        let pool_name: Arc<str> = Arc::from("default");
        let activity = Arc::new(ExecutorPoolActivity {
            pool_name: pool_name.clone(),
            waiting_requests: AtomicUsize::new(0),
            active_requests: AtomicUsize::new(0),
        });
        Arc::new(InnerLocalNodeExecutor {
            generation,
            pool_name: pool_name.clone(),
            resident_fingerprint,
            activity,
            pid,
            started_at: Instant::now(),
            runtime_stats_supported: false,
            active_requests: AtomicUsize::new(0),
            retirement_requested: AtomicBool::new(false),
            idle: Notify::new(),
            execution_unavailable: AtomicBool::new(false),
            execution_unavailable_notify: Notify::new(),
            terminate_draining: AtomicBool::new(false),
            terminate_draining_notify: Notify::new(),
            retired: AtomicBool::new(false),
            retirement_failed: AtomicBool::new(false),
            retired_notify: Notify::new(),
            termination_failures_remaining: AtomicUsize::new(0),
            retained_source_packages: AtomicU64::new(0),
            retained_external_packages: AtomicU64::new(0),
            imported_source_packages: AtomicU64::new(0),
            registered_stack_roots: AtomicU64::new(0),
            first_miss_diagnostics_started: AtomicBool::new(false),
            next_active_request_id: AtomicU64::new(0),
            active_request_diagnostics: StdMutex::new(BTreeMap::new()),
            preparation_descriptor: StdMutex::new(None),
            diagnostic_paths: None,
            server_handle: Arc::new(Mutex::new(ManagedChild::new(
                pool_name,
                generation,
                server_handle,
                source_dir,
            ))),
            client,
        })
    }

    fn test_fingerprint(environment: &[u8]) -> ResidentGenerationFingerprint {
        ResidentGenerationFingerprint {
            source_package_id: value::DeveloperDocumentId::MIN.into(),
            environment_sha256: common::sha256::Sha256::hash(environment),
            topology_version: common::types::Timestamp::MIN,
        }
    }

    #[tokio::test]
    async fn active_request_diagnostics_are_bounded_and_removed_with_guards() {
        let generation = test_inner(1).await;
        let mut guards = Vec::new();
        for index in 0..=MAX_DIAGNOSTIC_ACTIVE_REQUESTS {
            guards.push(ActiveRequestGuard::new(
                generation.clone(),
                RequestDiagnosticMetadata {
                    request_kind: "execute",
                    module_path: Some(format!("module_{index}.js")),
                    function_name: Some("run".to_owned()),
                },
            ));
        }
        assert_eq!(
            generation.active_requests.load(Ordering::Relaxed),
            MAX_DIAGNOSTIC_ACTIVE_REQUESTS + 1
        );
        assert_eq!(
            generation.active_request_diagnostics.lock().unwrap().len(),
            MAX_DIAGNOSTIC_ACTIVE_REQUESTS
        );

        drop(guards.remove(0));
        let replacement = ActiveRequestGuard::new(generation.clone(), test_request_metadata());
        assert_eq!(
            generation.active_request_diagnostics.lock().unwrap().len(),
            MAX_DIAGNOSTIC_ACTIVE_REQUESTS
        );

        drop(replacement);
        drop(guards);
        assert_eq!(generation.active_requests.load(Ordering::Relaxed), 0);
        assert!(generation
            .active_request_diagnostics
            .lock()
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn first_miss_diagnostics_are_claimed_once_per_enabled_generation() {
        let disabled = test_inner(1).await;
        assert!(!disabled.claim_first_miss_diagnostics());
        assert!(!disabled
            .first_miss_diagnostics_started
            .load(Ordering::Acquire));

        let mut enabled = test_inner(2).await;
        let source_dir = TempDir::new().unwrap();
        Arc::get_mut(&mut enabled).unwrap().diagnostic_paths =
            Some(NodeDiagnosticPaths::new(&source_dir, source_dir.path(), 2));
        assert!(enabled.claim_first_miss_diagnostics());
        assert!(!enabled.claim_first_miss_diagnostics());
        assert!(enabled
            .first_miss_diagnostics_started
            .load(Ordering::Acquire));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropped_unpublished_child_retains_tempdir_until_cleanup_finishes() {
        let source_dir = TempDir::new().unwrap();
        let source_dir_path = source_dir.path().to_owned();
        let server_handle = TokioCommand::new("sleep")
            .arg("300")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let server_handle = ManagedChild::new(Arc::from("default"), 1, server_handle, source_dir);

        drop(server_handle);

        // This current-thread test has not yielded to the detached cleanup yet.
        // The tempdir must already belong to that task rather than being removed
        // while the child is only scheduled for termination.
        assert!(source_dir_path.exists());
        tokio::time::timeout(Duration::from_secs(1), async {
            while source_dir_path.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn tempdir_removal_requires_confirmed_direct_child_reaping() {
        assert!(tokio::runtime::Handle::try_current().is_err());
        let retained_dir = TempDir::new().unwrap();
        let retained_path = retained_dir.path().to_owned();
        drop(ReapingTempDir::new(Arc::from("default"), 1, retained_dir));
        assert!(retained_path.exists());
        fs::remove_dir_all(retained_path).unwrap();

        let removed_dir = TempDir::new().unwrap();
        let removed_path = removed_dir.path().to_owned();
        ReapingTempDir::new(Arc::from("default"), 2, removed_dir).remove_after_reaping();
        let deadline = Instant::now() + Duration::from_secs(1);
        while removed_path.exists() {
            assert!(
                Instant::now() < deadline,
                "Detached local Node temp directory cleanup did not finish"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn io_transport_error_classification_is_bounded() {
        assert_eq!(
            classify_io_error_kind(std::io::ErrorKind::ConnectionRefused),
            "connection_refused"
        );
        assert_eq!(
            classify_io_error_kind(std::io::ErrorKind::ConnectionReset),
            "connection_reset"
        );
        assert_eq!(
            classify_io_error_kind(std::io::ErrorKind::ConnectionAborted),
            "connection_aborted"
        );
        assert_eq!(
            classify_io_error_kind(std::io::ErrorKind::NotConnected),
            "not_connected"
        );
        assert_eq!(
            classify_io_error_kind(std::io::ErrorKind::BrokenPipe),
            "broken_pipe"
        );
        assert_eq!(
            classify_io_error_kind(std::io::ErrorKind::UnexpectedEof),
            "unexpected_eof"
        );
        assert_eq!(
            classify_io_error_kind(std::io::ErrorKind::TimedOut),
            "timeout"
        );
        assert_eq!(
            classify_io_error_kind(std::io::ErrorKind::InvalidData),
            "other_io"
        );
    }

    #[tokio::test]
    async fn child_termination_records_supervisor_kill_of_running_child() {
        let mut child = TokioCommand::new("sleep")
            .arg("300")
            .kill_on_drop(true)
            .spawn()
            .unwrap();

        let observation = InnerLocalNodeExecutor::terminate_child("default", 1, &mut child)
            .await
            .unwrap();

        assert_eq!(
            observation,
            ChildTerminationObservation {
                state_before: "running",
                supervisor_kill_requested: true,
                exit_class: "signal",
            }
        );
    }

    #[tokio::test]
    async fn child_termination_records_child_that_already_exited() {
        let mut child = TokioCommand::new("sh")
            .arg("-c")
            .arg("exit 7")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if child.try_wait().unwrap().is_some() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let observation = InnerLocalNodeExecutor::terminate_child("default", 2, &mut child)
            .await
            .unwrap();

        assert_eq!(
            observation,
            ChildTerminationObservation {
                state_before: "already_exited",
                supervisor_kill_requested: false,
                exit_class: "failure",
            }
        );
    }

    fn test_executor(
        inner: Arc<InnerLocalNodeExecutor>,
        config: LocalNodeExecutorConfig,
    ) -> (LocalNodeExecutor, Arc<Mutex<LocalNodeExecutorState>>) {
        let activity = inner.activity.clone();
        let state = Arc::new(Mutex::new(LocalNodeExecutorState {
            next_generation: inner.generation,
            inner: Some(inner),
            retiring: None,
            hot_transition: None,
            replacement_for_generation: None,
            next_transition: 0,
        }));
        let executor = LocalNodeExecutor {
            state: state.clone(),
            transition_changed: Arc::new(Notify::new()),
            startup_lock: Mutex::new(()),
            shutting_down: Arc::new(AtomicBool::new(false)),
            shutdown_started: Arc::new(AtomicBool::new(false)),
            activity,
            config,
        };
        (executor, state)
    }

    fn test_cleanup_owner(
        state: &Arc<Mutex<LocalNodeExecutorState>>,
        transition_changed: &Arc<Notify>,
        token: u64,
        status: &Arc<HotTransitionStatus>,
        reason: GenerationRetirementReason,
        generation: &Arc<InnerLocalNodeExecutor>,
    ) -> Arc<HotTransitionCleanupOwner> {
        HotTransitionCleanupOwner::new(
            token,
            state,
            transition_changed.clone(),
            status.clone(),
            reason,
            generation.pool_name.clone(),
        )
    }

    #[tokio::test]
    async fn preemption_reaps_after_local_state_owner_is_lost() {
        let current = test_inner(1).await;
        let candidate = test_inner(2).await;
        let config = test_config();
        let coordinator = config.surge_coordinator.clone();
        let (executor, state) = test_executor(current.clone(), config);
        let status = Arc::new(HotTransitionStatus {
            failed: AtomicBool::new(false),
            promoted: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
        });
        let cleanup = test_cleanup_owner(
            &state,
            &executor.transition_changed,
            1,
            &status,
            GenerationRetirementReason::AgeLimit,
            &current,
        );
        cleanup.attach_startup_child(candidate.server_handle.clone());
        cleanup.attach_candidate(candidate.clone());
        let permit = coordinator
            .acquire(SurgePriority::Routine, Arc::from("candidate"))
            .await;
        cleanup.retain_permit(&permit, MemoryPressureSignal::default());
        permit.release();
        state.lock().await.hot_transition = Some(HotTransition::Candidate(CandidateTransition {
            token: 1,
            expected: current.clone(),
            target_fingerprint: None,
            descriptor: None,
            startup_started: true,
            reason: GenerationRetirementReason::AgeLimit,
            canceled: Arc::new(AtomicBool::new(false)),
            canceled_changed: Arc::new(Notify::new()),
            status: status.clone(),
            cleanup: cleanup.clone(),
        }));

        // The exact cleanup watcher, rather than the executor state, now owns
        // the candidate and its permit through confirmed reaping.
        drop(executor);
        drop(state);
        drop(cleanup);
        assert_eq!(coordinator.force_preempt_reclaimable(), Some("candidate"));

        tokio::time::timeout(Duration::from_secs(1), async {
            while coordinator.state.lock().unwrap().occupied.is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Lost local state stranded the candidate surge permit");
        assert!(status.failed.load(Ordering::Acquire));
        assert!(!status.cleanup_failed.load(Ordering::Acquire));
        assert!(candidate.server_handle.lock().await.child.is_none());
        current.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn deployment_candidate_cleanup_failure_becomes_force_reclaimable() {
        let current = test_inner(1).await;
        let candidate = test_inner(2).await;
        let config = test_config();
        let coordinator = config.surge_coordinator.clone();
        let (executor, state) = test_executor(current.clone(), config);
        let status = Arc::new(HotTransitionStatus {
            failed: AtomicBool::new(false),
            promoted: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
        });
        let cleanup = test_cleanup_owner(
            &state,
            &executor.transition_changed,
            1,
            &status,
            GenerationRetirementReason::FingerprintChange,
            &current,
        );
        cleanup.attach_startup_child(candidate.server_handle.clone());
        cleanup.attach_candidate(candidate.clone());
        let permit = coordinator
            .acquire(SurgePriority::Deployment, Arc::from("candidate"))
            .await;
        cleanup.retain_permit(&permit, MemoryPressureSignal::default());
        permit.release();
        state.lock().await.hot_transition = Some(HotTransition::Candidate(CandidateTransition {
            token: 1,
            expected: current.clone(),
            target_fingerprint: None,
            descriptor: None,
            startup_started: true,
            reason: GenerationRetirementReason::FingerprintChange,
            canceled: Arc::new(AtomicBool::new(false)),
            canceled_changed: Arc::new(Notify::new()),
            status: status.clone(),
            cleanup: cleanup.clone(),
        }));
        candidate
            .termination_failures_remaining
            .store(1, Ordering::Release);

        assert_eq!(coordinator.force_preempt_reclaimable(), None);
        assert!(cleanup.cleanup("stale").await.is_err());

        assert!(status.cleanup_failed.load(Ordering::Acquire));
        assert!(!status.failed.load(Ordering::Acquire));
        assert!(candidate.server_handle.lock().await.child.is_some());
        assert!(coordinator.state.lock().unwrap().occupied.is_some());
        {
            let state = state.lock().await;
            let Some(HotTransition::Candidate(retained)) = &state.hot_transition else {
                panic!("Failed candidate cleanup did not retain candidate state");
            };
            assert_eq!(retained.token, 1);
            assert!(Arc::ptr_eq(&retained.cleanup, &cleanup));
        }

        assert_eq!(coordinator.force_preempt_reclaimable(), Some("candidate"));
        tokio::time::timeout(Duration::from_secs(1), cleanup.wait_until_confirmed())
            .await
            .unwrap();

        assert!(state.lock().await.hot_transition.is_none());
        assert!(status.failed.load(Ordering::Acquire));
        assert!(!status.cleanup_failed.load(Ordering::Acquire));
        assert!(candidate.server_handle.lock().await.child.is_none());
        tokio::time::timeout(Duration::from_secs(1), async {
            while coordinator.state.lock().unwrap().occupied.is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(
            Duration::from_secs(1),
            coordinator.acquire(SurgePriority::Routine, Arc::from("candidate-recovered")),
        )
        .await
        .unwrap()
        .release();
        current.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_cleanup_request_retries_a_failed_attempt() {
        let current = test_inner(1).await;
        let candidate = test_inner(2).await;
        let (executor, state) = test_executor(current.clone(), test_config());
        let status = Arc::new(HotTransitionStatus {
            failed: AtomicBool::new(false),
            promoted: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
        });
        let cleanup = test_cleanup_owner(
            &state,
            &executor.transition_changed,
            1,
            &status,
            GenerationRetirementReason::AgeLimit,
            &current,
        );
        cleanup.attach_startup_child(candidate.server_handle.clone());
        cleanup.attach_candidate(candidate.clone());
        state.lock().await.hot_transition = Some(HotTransition::Candidate(CandidateTransition {
            token: 1,
            expected: current.clone(),
            target_fingerprint: None,
            descriptor: None,
            startup_started: true,
            reason: GenerationRetirementReason::AgeLimit,
            canceled: Arc::new(AtomicBool::new(false)),
            canceled_changed: Arc::new(Notify::new()),
            status: status.clone(),
            cleanup: cleanup.clone(),
        }));
        candidate
            .termination_failures_remaining
            .store(1, Ordering::Release);

        // Both requests are published before the first spawned attempt can be
        // polled. The second request must become a pending retry rather than
        // only joining the attempt that is about to fail.
        cleanup.request_cleanup("stale");
        cleanup.request_cleanup("stale");
        tokio::time::timeout(Duration::from_secs(1), cleanup.wait_until_confirmed())
            .await
            .expect("Concurrent cleanup retry request was lost");

        assert!(state.lock().await.hot_transition.is_none());
        assert!(status.failed.load(Ordering::Acquire));
        assert!(!status.cleanup_failed.load(Ordering::Acquire));
        assert!(candidate.server_handle.lock().await.child.is_none());
        current.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn later_force_request_retries_the_retained_cleanup_owner() {
        let current = test_inner(1).await;
        let candidate = test_inner(2).await;
        let config = test_config();
        let coordinator = config.surge_coordinator.clone();
        let (executor, state) = test_executor(current.clone(), config);
        let status = Arc::new(HotTransitionStatus {
            failed: AtomicBool::new(false),
            promoted: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
        });
        let cleanup = test_cleanup_owner(
            &state,
            &executor.transition_changed,
            1,
            &status,
            GenerationRetirementReason::AgeLimit,
            &current,
        );
        cleanup.attach_startup_child(candidate.server_handle.clone());
        cleanup.attach_candidate(candidate.clone());
        let permit = coordinator
            .acquire(SurgePriority::Routine, Arc::from("candidate"))
            .await;
        cleanup.retain_permit(&permit, MemoryPressureSignal::default());
        permit.release();
        state.lock().await.hot_transition = Some(HotTransition::Candidate(CandidateTransition {
            token: 1,
            expected: current.clone(),
            target_fingerprint: None,
            descriptor: None,
            startup_started: true,
            reason: GenerationRetirementReason::AgeLimit,
            canceled: Arc::new(AtomicBool::new(false)),
            canceled_changed: Arc::new(Notify::new()),
            status: status.clone(),
            cleanup: cleanup.clone(),
        }));
        candidate
            .termination_failures_remaining
            .store(3, Ordering::Release);

        assert_eq!(coordinator.force_preempt_reclaimable(), Some("candidate"));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let attempt_running = cleanup.attempt.lock().unwrap().running;
                if status.cleanup_failed.load(Ordering::Acquire) && !attempt_running {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Initial force cleanup failures were not published");
        assert!(coordinator.state.lock().unwrap().occupied.is_some());

        assert_eq!(coordinator.force_preempt_reclaimable(), Some("candidate"));
        tokio::time::timeout(Duration::from_secs(1), cleanup.wait_until_confirmed())
            .await
            .expect("Later force request did not retry the retained cleanup owner");

        assert!(state.lock().await.hot_transition.is_none());
        assert!(candidate.server_handle.lock().await.child.is_none());
        assert!(coordinator.state.lock().unwrap().occupied.is_none());
        current.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn pressure_reentry_retries_the_retained_cleanup_owner() {
        let current = test_inner(1).await;
        let candidate = test_inner(2).await;
        let config = test_config();
        let coordinator = config.surge_coordinator.clone();
        let pressure = config.memory_pressure.clone();
        let (executor, state) = test_executor(current.clone(), config);
        let status = Arc::new(HotTransitionStatus {
            failed: AtomicBool::new(false),
            promoted: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
        });
        let cleanup = test_cleanup_owner(
            &state,
            &executor.transition_changed,
            1,
            &status,
            GenerationRetirementReason::AgeLimit,
            &current,
        );
        cleanup.attach_startup_child(candidate.server_handle.clone());
        cleanup.attach_candidate(candidate.clone());
        let permit = coordinator
            .acquire(SurgePriority::Routine, Arc::from("candidate"))
            .await;
        cleanup.retain_permit(&permit, pressure.clone());
        permit.release();
        state.lock().await.hot_transition = Some(HotTransition::Candidate(CandidateTransition {
            token: 1,
            expected: current.clone(),
            target_fingerprint: None,
            descriptor: None,
            startup_started: true,
            reason: GenerationRetirementReason::AgeLimit,
            canceled: Arc::new(AtomicBool::new(false)),
            canceled_changed: Arc::new(Notify::new()),
            status: status.clone(),
            cleanup: cleanup.clone(),
        }));
        candidate
            .termination_failures_remaining
            .store(3, Ordering::Release);

        pressure.set_active(true);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if candidate
                    .termination_failures_remaining
                    .load(Ordering::Acquire)
                    == 1
                    && status.cleanup_failed.load(Ordering::Acquire)
                    && !cleanup.attempt.lock().unwrap().running
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Initial pressure cleanup failures were not published");
        assert!(coordinator.state.lock().unwrap().occupied.is_some());

        pressure.set_active(false);
        pressure.set_active(true);
        tokio::time::timeout(Duration::from_secs(1), cleanup.wait_until_confirmed())
            .await
            .expect("Pressure re-entry did not retry the retained cleanup owner");

        assert!(state.lock().await.hot_transition.is_none());
        assert!(candidate.server_handle.lock().await.child.is_none());
        assert!(coordinator.state.lock().unwrap().occupied.is_none());
        current.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn draining_cleanup_failure_retains_exact_owner_and_recovers_on_preemption() {
        let old = test_inner(1).await;
        let current = test_inner(2).await;
        let config = test_config();
        let coordinator = config.surge_coordinator.clone();
        let (executor, state) = test_executor(current.clone(), config);
        let status = Arc::new(HotTransitionStatus {
            failed: AtomicBool::new(false),
            promoted: AtomicBool::new(true),
            cleanup_failed: AtomicBool::new(false),
        });
        let cleanup = test_cleanup_owner(
            &state,
            &executor.transition_changed,
            1,
            &status,
            GenerationRetirementReason::AgeLimit,
            &old,
        );
        cleanup.attach_startup_child(current.server_handle.clone());
        cleanup.attach_candidate(current.clone());
        assert!(cleanup.promote_to_draining(old.clone()));
        let permit = coordinator
            .acquire(SurgePriority::Routine, Arc::from("draining"))
            .await;
        cleanup.retain_permit(&permit, MemoryPressureSignal::default());
        permit.set_phase("draining");
        permit.release();
        state.lock().await.hot_transition = Some(HotTransition::Draining(DrainingTransition {
            token: 1,
            old: old.clone(),
            status: status.clone(),
            cleanup: cleanup.clone(),
        }));
        old.termination_failures_remaining
            .store(1, Ordering::Release);

        assert!(cleanup.cleanup("drained").await.is_err());

        assert!(status.cleanup_failed.load(Ordering::Acquire));
        assert!(!status.failed.load(Ordering::Acquire));
        assert!(!old.retired.load(Ordering::Acquire));
        assert!(
            tokio::time::timeout(Duration::from_secs(1), old.wait_until_retired())
                .await
                .expect("Draining-generation waiter was not woken after cleanup failure")
                .is_err()
        );
        assert!(old.server_handle.lock().await.child.is_some());
        assert!(coordinator.state.lock().unwrap().occupied.is_some());
        {
            let state = state.lock().await;
            let Some(HotTransition::Draining(retained)) = &state.hot_transition else {
                panic!("Failed draining cleanup did not retain draining state");
            };
            assert_eq!(retained.token, 1);
            assert!(Arc::ptr_eq(&retained.old, &old));
            assert!(Arc::ptr_eq(&retained.cleanup, &cleanup));
        }

        LocalNodeExecutor::preempt_hot_transition_state(&state, "stale").await;
        tokio::time::timeout(Duration::from_secs(1), cleanup.wait_until_confirmed())
            .await
            .unwrap();

        let state_guard = state.lock().await;
        assert!(state_guard.hot_transition.is_none());
        assert!(state_guard
            .inner
            .as_ref()
            .is_some_and(|inner| Arc::ptr_eq(inner, &current)));
        drop(state_guard);
        assert!(old.retired.load(Ordering::Acquire));
        assert!(!old.retirement_failed.load(Ordering::Acquire));
        assert!(!status.failed.load(Ordering::Acquire));
        assert!(!status.cleanup_failed.load(Ordering::Acquire));
        assert!(old.server_handle.lock().await.child.is_none());
        tokio::time::timeout(Duration::from_secs(1), async {
            while coordinator.state.lock().unwrap().occupied.is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(
            Duration::from_secs(1),
            coordinator.acquire(SurgePriority::Routine, Arc::from("draining-recovered")),
        )
        .await
        .unwrap()
        .release();
        current.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn graceful_retirement_stops_admission_and_waits_for_active_request() {
        let generation = test_inner(1).await;
        let (executor, state) = test_executor(generation.clone(), test_config());
        let active_guard = match executor
            .acquire_existing_inner(test_request_metadata(), None)
            .await
            .unwrap()
        {
            InnerAcquisition::Ready { inner, guard } => {
                assert!(Arc::ptr_eq(&inner, &generation));
                guard
            },
            InnerAcquisition::Draining(_)
            | InnerAcquisition::Transition(_)
            | InnerAcquisition::Missing => {
                panic!("Test generation was not available")
            },
        };
        assert_eq!(generation.active_requests.load(Ordering::Acquire), 1);

        let retirement_state = state.clone();
        let retirement_generation = generation.clone();
        let retirement = tokio::spawn(async move {
            LocalNodeExecutor::drain_and_retire_inner_state(
                &retirement_state,
                &retirement_generation,
                GenerationRetirementDiagnostics::proactive(
                    GenerationRetirementReason::PackageLimit,
                ),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !generation.retirement_requested.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        match executor
            .acquire_existing_inner(test_request_metadata(), None)
            .await
            .unwrap()
        {
            InnerAcquisition::Draining(inner) => assert!(Arc::ptr_eq(&inner, &generation)),
            InnerAcquisition::Ready { .. }
            | InnerAcquisition::Transition(_)
            | InnerAcquisition::Missing => {
                panic!("Draining generation admitted a new request")
            },
        }
        assert_eq!(generation.active_requests.load(Ordering::Acquire), 1);
        assert!(state.lock().await.inner.is_some());

        drop(active_guard);
        assert!(tokio::time::timeout(Duration::from_secs(1), retirement)
            .await
            .unwrap()
            .unwrap()
            .unwrap());
        assert!(state.lock().await.inner.is_none());
        assert!(generation.retired.load(Ordering::Acquire));
        assert!(generation.server_handle.lock().await.child.is_none());
    }

    #[tokio::test]
    async fn resident_fingerprint_reuses_matching_generation() {
        let fingerprint = test_fingerprint(b"first");
        let generation = test_inner_with_fingerprint(1, fingerprint.clone()).await;
        let (executor, _) = test_executor(generation.clone(), test_config());

        let acquisition = executor
            .acquire_existing_inner(test_request_metadata(), Some(fingerprint))
            .await
            .unwrap();
        match acquisition {
            InnerAcquisition::Ready { inner, guard } => {
                assert!(Arc::ptr_eq(&inner, &generation));
                drop(guard);
            },
            InnerAcquisition::Draining(_)
            | InnerAcquisition::Transition(_)
            | InnerAcquisition::Missing => {
                panic!("Matching resident generation was not reused")
            },
        }
        assert!(!generation.retirement_requested.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn matching_package_reuses_generation_across_topology_watermarks() {
        let fingerprint = test_fingerprint(b"first");
        let generation = test_inner_with_fingerprint(1, fingerprint.clone()).await;
        let (executor, _) = test_executor(generation.clone(), test_config());
        let mut newer_snapshot = fingerprint;
        newer_snapshot.topology_version = Timestamp::try_from(1_u64).unwrap();

        match executor
            .acquire_existing_inner(test_request_metadata(), Some(newer_snapshot))
            .await
            .unwrap()
        {
            InnerAcquisition::Ready { inner, guard } => {
                assert!(Arc::ptr_eq(&inner, &generation));
                drop(guard);
            },
            InnerAcquisition::Draining(_)
            | InnerAcquisition::Transition(_)
            | InnerAcquisition::Missing => {
                panic!("Matching package and environment started a watermark-only rotation")
            },
        }
    }

    #[tokio::test]
    async fn admitted_execute_requests_retain_the_freshest_preparation_descriptor() {
        let fingerprint = test_fingerprint(b"resident");
        let generation = test_inner_with_fingerprint(1, fingerprint.clone()).await;
        let (executor, _) = test_executor(generation.clone(), test_config());
        let mut older = test_source_package();
        older.download_url_expiration = Instant::now() + Duration::from_secs(60);
        let mut newer = older.clone();
        newer.download_url_expiration = Instant::now() + Duration::from_secs(120);

        for source_package in [older.clone(), newer.clone(), older] {
            match executor
                .acquire_existing_inner_with_descriptor(
                    test_request_metadata(),
                    Some(fingerprint.clone()),
                    Some(PreparationDescriptor { source_package }),
                )
                .await
                .unwrap()
            {
                InnerAcquisition::Ready { guard, .. } => drop(guard),
                InnerAcquisition::Draining(_)
                | InnerAcquisition::Transition(_)
                | InnerAcquisition::Missing => {
                    panic!("Matching execute request was not admitted")
                },
            }
        }

        let retained = generation
            .preparation_descriptor
            .lock()
            .expect("Local Node preparation descriptor lock poisoned")
            .clone()
            .expect("Admitted execute descriptor was not retained");
        assert_eq!(
            retained.source_package.download_url_expiration,
            newer.download_url_expiration
        );
        generation.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn deployment_cutover_reuses_matching_package_across_topology_watermarks() {
        let current_fingerprint = test_fingerprint(b"current");
        let generation = test_inner_with_fingerprint(1, current_fingerprint.clone()).await;
        let config = test_config();
        let coordinator = config.surge_coordinator.clone();
        let permit = coordinator
            .acquire(SurgePriority::Deployment, Arc::from("deployment"))
            .await;
        let (executor, state) = test_executor(generation.clone(), config);
        let mut target_fingerprint = current_fingerprint;
        target_fingerprint.topology_version = Timestamp::try_from(1_u64).unwrap();

        let outcome = executor
            .replace_for_deployment(Some(target_fingerprint), test_source_package(), permit)
            .await
            .unwrap();
        assert!(matches!(outcome, DeploymentReplacementOutcome::Reused));

        assert!(Arc::ptr_eq(
            state.lock().await.inner.as_ref().unwrap(),
            &generation
        ));
        assert!(coordinator
            .state
            .lock()
            .expect("Local Node surge coordinator lock poisoned")
            .occupied
            .is_none());
        generation.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn deployment_target_readiness_does_not_wait_for_old_generation_cleanup() {
        let target_fingerprint = test_fingerprint(b"target");
        let old = test_inner_with_fingerprint(1, test_fingerprint(b"old")).await;
        let replacement = test_inner_with_fingerprint(2, target_fingerprint.clone()).await;
        let (executor, state) = test_executor(old.clone(), test_config());
        let transition_changed = executor.transition_changed.clone();
        let status = Arc::new(HotTransitionStatus {
            failed: AtomicBool::new(false),
            promoted: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
        });
        let cleanup = test_cleanup_owner(
            &state,
            &transition_changed,
            1,
            &status,
            GenerationRetirementReason::TopologyChange,
            &old,
        );
        cleanup.attach_startup_child(replacement.server_handle.clone());
        cleanup.attach_candidate(replacement.clone());
        state.lock().await.hot_transition = Some(HotTransition::Candidate(CandidateTransition {
            token: 1,
            expected: old.clone(),
            target_fingerprint: Some(target_fingerprint.clone()),
            descriptor: None,
            startup_started: true,
            reason: GenerationRetirementReason::TopologyChange,
            canceled: Arc::new(AtomicBool::new(false)),
            canceled_changed: Arc::new(Notify::new()),
            status: status.clone(),
            cleanup: cleanup.clone(),
        }));

        let readiness = tokio::spawn(async move {
            executor
                .wait_for_resident_package_and_environment_ready(&target_fingerprint)
                .await;
        });
        tokio::task::yield_now().await;
        assert!(!readiness.is_finished());

        {
            let mut state = state.lock().await;
            assert!(cleanup.promote_to_draining(old.clone()));
            old.retirement_requested.store(true, Ordering::Release);
            state.inner = Some(replacement.clone());
            state.hot_transition = Some(HotTransition::Draining(DrainingTransition {
                token: 1,
                old: old.clone(),
                status: status.clone(),
                cleanup,
            }));
            status.promoted.store(true, Ordering::Release);
        }
        transition_changed.notify_waiters();

        tokio::time::timeout(Duration::from_secs(1), readiness)
            .await
            .expect("Promoted target did not become ready before old-generation cleanup")
            .unwrap();
        assert!(matches!(
            &state.lock().await.hot_transition,
            Some(HotTransition::Draining(_))
        ));
        assert!(!old.retired.load(Ordering::Acquire));
        assert!(old.server_handle.lock().await.child.is_some());

        state.lock().await.hot_transition = None;
        replacement.terminate().await.unwrap();
        old.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn deployment_reuse_waits_for_matching_generation_to_finish_draining() {
        let target_fingerprint = test_fingerprint(b"target");
        let old = test_inner_with_fingerprint(1, test_fingerprint(b"old")).await;
        let current = test_inner_with_fingerprint(2, target_fingerprint.clone()).await;
        let config = test_config();
        let coordinator = config.surge_coordinator.clone();
        let permit = coordinator
            .acquire(SurgePriority::Deployment, Arc::from("deployment"))
            .await;
        let draining_permit = permit.clone();
        draining_permit.set_phase("draining");
        let (executor, state) = test_executor(current.clone(), config);
        let transition_changed = executor.transition_changed.clone();
        let draining_status = Arc::new(HotTransitionStatus {
            failed: AtomicBool::new(false),
            promoted: AtomicBool::new(true),
            cleanup_failed: AtomicBool::new(false),
        });
        let draining_cleanup = test_cleanup_owner(
            &state,
            &transition_changed,
            1,
            &draining_status,
            GenerationRetirementReason::TopologyChange,
            &old,
        );
        *draining_cleanup.phase.lock().unwrap() =
            HotTransitionCleanupPhase::Draining { old: old.clone() };
        state.lock().await.hot_transition = Some(HotTransition::Draining(DrainingTransition {
            token: 1,
            old: old.clone(),
            status: draining_status,
            cleanup: draining_cleanup,
        }));

        let replacement = tokio::spawn(async move {
            executor
                .replace_for_deployment(Some(target_fingerprint), test_source_package(), permit)
                .await
        });
        tokio::task::yield_now().await;
        assert!(!replacement.is_finished());
        assert!(coordinator
            .state
            .lock()
            .expect("Local Node surge coordinator lock poisoned")
            .occupied
            .is_some());

        draining_permit.confirm_direct_child_reaped();
        let candidate_status = Arc::new(HotTransitionStatus {
            failed: AtomicBool::new(false),
            promoted: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
        });
        let candidate_cleanup = test_cleanup_owner(
            &state,
            &transition_changed,
            2,
            &candidate_status,
            GenerationRetirementReason::AgeLimit,
            &current,
        );
        state.lock().await.hot_transition = Some(HotTransition::Candidate(CandidateTransition {
            token: 2,
            expected: current.clone(),
            target_fingerprint: None,
            descriptor: None,
            startup_started: false,
            reason: GenerationRetirementReason::AgeLimit,
            canceled: Arc::new(AtomicBool::new(false)),
            canceled_changed: Arc::new(Notify::new()),
            status: candidate_status,
            cleanup: candidate_cleanup,
        }));
        transition_changed.notify_waiters();
        draining_permit.release();

        let outcome = replacement.await.unwrap().unwrap();
        assert!(matches!(outcome, DeploymentReplacementOutcome::Reused));
        assert!(coordinator
            .state
            .lock()
            .expect("Local Node surge coordinator lock poisoned")
            .occupied
            .is_none());
        state.lock().await.hot_transition = None;
        current.terminate().await.unwrap();
        old.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn queued_fingerprint_target_coalesces_but_started_target_is_stable() {
        let generation = test_inner_with_fingerprint(1, test_fingerprint(b"current")).await;
        let (executor, state) = test_executor(generation.clone(), test_config());
        let status = Arc::new(HotTransitionStatus {
            failed: AtomicBool::new(false),
            promoted: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
        });
        let cleanup = test_cleanup_owner(
            &state,
            &executor.transition_changed,
            1,
            &status,
            GenerationRetirementReason::FingerprintChange,
            &generation,
        );
        let mut first_target = test_fingerprint(b"first");
        first_target.topology_version = Timestamp::try_from(1_u64).unwrap();
        state.lock().await.hot_transition = Some(HotTransition::Candidate(CandidateTransition {
            token: 1,
            expected: generation.clone(),
            target_fingerprint: Some(first_target),
            descriptor: None,
            startup_started: false,
            reason: GenerationRetirementReason::FingerprintChange,
            canceled: Arc::new(AtomicBool::new(false)),
            canceled_changed: Arc::new(Notify::new()),
            status: status.clone(),
            cleanup,
        }));

        let mut queued_target = test_fingerprint(b"queued");
        queued_target.topology_version = Timestamp::try_from(2_u64).unwrap();
        let mut queued_source_package = test_source_package();
        queued_source_package.download_url_expiration = Instant::now() + Duration::from_secs(60);
        let queued_status = executor
            .request_hot_replacement(
                &generation,
                Some(queued_target.clone()),
                Some(PreparationDescriptor {
                    source_package: queued_source_package,
                }),
                GenerationRetirementReason::FingerprintChange,
            )
            .await
            .expect("Newer queued target did not join the transition");
        assert!(Arc::ptr_eq(&queued_status, &status));
        {
            let mut state = state.lock().await;
            let Some(HotTransition::Candidate(candidate)) = &mut state.hot_transition else {
                panic!("Fingerprint transition is not a candidate");
            };
            assert!(candidate.target_fingerprint == Some(queued_target.clone()));
            candidate.startup_started = true;
        }

        let mut matching_target = queued_target.clone();
        matching_target.topology_version = Timestamp::try_from(3_u64).unwrap();
        let mut freshest_source_package = test_source_package();
        freshest_source_package.download_url_expiration = Instant::now() + Duration::from_secs(120);
        let freshest_expiration = freshest_source_package.download_url_expiration;
        let matching_status = executor
            .request_hot_replacement(
                &generation,
                Some(matching_target),
                Some(PreparationDescriptor {
                    source_package: freshest_source_package,
                }),
                GenerationRetirementReason::FingerprintChange,
            )
            .await
            .expect("Matching package did not join a started candidate");
        assert!(Arc::ptr_eq(&matching_status, &status));

        let mut different_target = test_fingerprint(b"different");
        different_target.topology_version = Timestamp::try_from(4_u64).unwrap();
        assert!(executor
            .request_hot_replacement(
                &generation,
                Some(different_target),
                None,
                GenerationRetirementReason::FingerprintChange,
            )
            .await
            .is_none());
        let state_guard = state.lock().await;
        let Some(HotTransition::Candidate(candidate)) = &state_guard.hot_transition else {
            panic!("Fingerprint transition is not a candidate");
        };
        assert!(candidate.target_fingerprint == Some(queued_target));
        assert_eq!(
            candidate
                .descriptor
                .as_ref()
                .expect("Matching candidate descriptor was not retained")
                .source_package
                .download_url_expiration,
            freshest_expiration
        );
        drop(state_guard);
        generation.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn stale_package_fingerprint_cannot_replace_newer_generation() {
        let mut current = test_fingerprint(b"current");
        current.topology_version = Timestamp::try_from(2_u64).unwrap();
        let generation = test_inner_with_fingerprint(1, current).await;
        let (executor, _) = test_executor(generation, test_config());
        let mut stale = test_fingerprint(b"stale");
        stale.topology_version = Timestamp::try_from(1_u64).unwrap();

        assert!(executor
            .acquire_existing_inner(test_request_metadata(), Some(stale))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn fingerprint_change_rejected_by_pressure_does_not_wait_forever() {
        let generation = test_inner_with_fingerprint(1, test_fingerprint(b"current")).await;
        let mut config = test_config();
        config.memory_pressure = MemoryPressureSignal::new(true);
        let (executor, state) = test_executor(generation.clone(), config);

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            executor.acquire_inner(
                test_request_metadata(),
                Some(test_fingerprint(b"candidate")),
                Some(PreparationDescriptor {
                    source_package: test_source_package(),
                }),
            ),
        )
        .await
        .expect("Rejected fingerprint replacement did not complete promptly");

        assert!(result.is_err());
        assert!(state.lock().await.hot_transition.is_none());
        assert!(state
            .lock()
            .await
            .inner
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &generation)));
        generation.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn fingerprint_change_joins_an_existing_routine_transition() {
        let generation = test_inner_with_fingerprint(1, test_fingerprint(b"current")).await;
        let (executor, state) = test_executor(generation.clone(), test_config());
        let status = Arc::new(HotTransitionStatus {
            failed: AtomicBool::new(false),
            promoted: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
        });
        let cleanup = test_cleanup_owner(
            &state,
            &executor.transition_changed,
            1,
            &status,
            GenerationRetirementReason::AgeLimit,
            &generation,
        );
        state.lock().await.hot_transition = Some(HotTransition::Candidate(CandidateTransition {
            token: 1,
            expected: generation.clone(),
            target_fingerprint: generation.resident_fingerprint.clone(),
            descriptor: None,
            startup_started: false,
            reason: GenerationRetirementReason::AgeLimit,
            canceled: Arc::new(AtomicBool::new(false)),
            canceled_changed: Arc::new(Notify::new()),
            status: status.clone(),
            cleanup,
        }));

        match executor
            .acquire_existing_inner(
                test_request_metadata(),
                Some(test_fingerprint(b"candidate")),
            )
            .await
            .unwrap()
        {
            InnerAcquisition::Transition(joined) => assert!(Arc::ptr_eq(&joined, &status)),
            InnerAcquisition::Ready { .. }
            | InnerAcquisition::Draining(_)
            | InnerAcquisition::Missing => {
                panic!("Fingerprint request did not join existing routine transition")
            },
        }

        state.lock().await.hot_transition = None;
        generation.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn failed_fingerprint_transition_fails_its_waiter_without_retrying() {
        let current_fingerprint = test_fingerprint(b"current");
        let target_fingerprint = test_fingerprint(b"target");
        let generation = test_inner_with_fingerprint(1, current_fingerprint).await;
        let (executor, state) = test_executor(generation.clone(), test_config());
        let status = Arc::new(HotTransitionStatus {
            failed: AtomicBool::new(false),
            promoted: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
        });
        let cleanup = test_cleanup_owner(
            &state,
            &executor.transition_changed,
            1,
            &status,
            GenerationRetirementReason::FingerprintChange,
            &generation,
        );
        state.lock().await.hot_transition = Some(HotTransition::Candidate(CandidateTransition {
            token: 1,
            expected: generation.clone(),
            target_fingerprint: Some(target_fingerprint.clone()),
            descriptor: None,
            startup_started: false,
            reason: GenerationRetirementReason::FingerprintChange,
            canceled: Arc::new(AtomicBool::new(false)),
            canceled_changed: Arc::new(Notify::new()),
            status: status.clone(),
            cleanup,
        }));
        let transition_changed = executor.transition_changed.clone();
        let waiter = tokio::spawn(async move {
            executor
                .acquire_inner(test_request_metadata(), Some(target_fingerprint), None)
                .await
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        status.failed.store(true, Ordering::Release);
        state.lock().await.hot_transition = None;
        transition_changed.notify_waiters();

        assert!(tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        generation.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn joined_waiter_retries_cold_after_its_current_generation_is_removed() {
        let target_fingerprint = test_fingerprint(b"target");
        let generation = test_inner_with_fingerprint(1, test_fingerprint(b"current")).await;
        let (executor, state) = test_executor(generation.clone(), test_config());
        let status = Arc::new(HotTransitionStatus {
            failed: AtomicBool::new(false),
            promoted: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
        });
        let cleanup = test_cleanup_owner(
            &state,
            &executor.transition_changed,
            1,
            &status,
            GenerationRetirementReason::FingerprintChange,
            &generation,
        );
        state.lock().await.hot_transition = Some(HotTransition::Candidate(CandidateTransition {
            token: 1,
            expected: generation.clone(),
            target_fingerprint: Some(target_fingerprint.clone()),
            descriptor: None,
            startup_started: false,
            reason: GenerationRetirementReason::FingerprintChange,
            canceled: Arc::new(AtomicBool::new(false)),
            canceled_changed: Arc::new(Notify::new()),
            status: status.clone(),
            cleanup,
        }));
        let transition_changed = executor.transition_changed.clone();
        let waiter = tokio::spawn(async move {
            executor
                .acquire_inner(test_request_metadata(), Some(target_fingerprint), None)
                .await
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        status.failed.store(true, Ordering::Release);
        {
            let mut state = state.lock().await;
            state.inner = None;
            state.hot_transition = None;
        }
        transition_changed.notify_waiters();

        let (replacement, request_guard, created) =
            tokio::time::timeout(Duration::from_secs(5), waiter)
                .await
                .expect("Joined waiter did not retry the cold-start path")
                .unwrap()
                .unwrap();
        assert!(created);
        assert!(!Arc::ptr_eq(&replacement, &generation));
        drop(request_guard);
        replacement.terminate().await.unwrap();
        generation.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn failed_transition_without_a_current_generation_does_not_wait_forever() {
        let generation = test_inner(1).await;
        let (executor, state) = test_executor(generation.clone(), test_config());
        let status = Arc::new(HotTransitionStatus {
            failed: AtomicBool::new(true),
            promoted: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
        });
        let cleanup = test_cleanup_owner(
            &state,
            &executor.transition_changed,
            1,
            &status,
            GenerationRetirementReason::AgeLimit,
            &generation,
        );
        let mut state_guard = state.lock().await;
        state_guard.inner = None;
        state_guard.hot_transition = Some(HotTransition::Candidate(CandidateTransition {
            token: 1,
            expected: generation.clone(),
            target_fingerprint: None,
            descriptor: None,
            startup_started: false,
            reason: GenerationRetirementReason::AgeLimit,
            canceled: Arc::new(AtomicBool::new(false)),
            canceled_changed: Arc::new(Notify::new()),
            status,
            cleanup,
        }));
        drop(state_guard);

        assert!(executor
            .acquire_existing_inner(test_request_metadata(), None)
            .await
            .is_err());
        generation.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn current_generation_admits_matching_work_while_candidate_is_queued() {
        let config = test_config();
        let blocker = config
            .surge_coordinator
            .acquire(SurgePriority::Routine, Arc::from("blocker"))
            .await;
        let current_fingerprint = test_fingerprint(b"current");
        let generation = test_inner_with_fingerprint(1, current_fingerprint.clone()).await;
        let (executor, _) = test_executor(generation.clone(), config.clone());

        assert!(matches!(
            executor
                .acquire_existing_inner(
                    test_request_metadata(),
                    Some(test_fingerprint(b"candidate")),
                )
                .await
                .unwrap(),
            InnerAcquisition::Transition(_)
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if config
                    .surge_coordinator
                    .state
                    .lock()
                    .expect("Local Node surge coordinator lock poisoned")
                    .deployment
                    .len()
                    == 1
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        match executor
            .acquire_existing_inner(test_request_metadata(), Some(current_fingerprint))
            .await
            .unwrap()
        {
            InnerAcquisition::Ready { inner, guard } => {
                assert!(Arc::ptr_eq(&inner, &generation));
                drop(guard);
            },
            InnerAcquisition::Draining(_)
            | InnerAcquisition::Transition(_)
            | InnerAcquisition::Missing => {
                panic!("Queued candidate blocked matching current-generation work")
            },
        }
        assert!(!generation.retirement_requested.load(Ordering::Acquire));

        executor.shutdown();
        blocker.release();
    }

    #[tokio::test]
    async fn immediate_retirement_wakes_a_queued_candidate() {
        let config = test_config();
        let blocker = config
            .surge_coordinator
            .acquire(SurgePriority::Routine, Arc::from("blocker"))
            .await;
        let generation = test_inner(1).await;
        let (executor, state) = test_executor(generation.clone(), config.clone());
        let status = executor
            .request_hot_replacement(
                &generation,
                None,
                None,
                GenerationRetirementReason::AgeLimit,
            )
            .await
            .expect("Routine candidate was not queued");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if config
                    .surge_coordinator
                    .state
                    .lock()
                    .expect("Local Node surge coordinator lock poisoned")
                    .routine
                    .len()
                    == 1
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        LocalNodeExecutor::retire_inner_state(
            &state,
            &generation,
            test_request_retirement(GenerationRetirementReason::RequestTimeout),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if state.lock().await.hot_transition.is_none() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Canceled candidate remained queued after its current generation retired");
        assert!(status.failed.load(Ordering::Acquire));
        assert!(config
            .surge_coordinator
            .state
            .lock()
            .expect("Local Node surge coordinator lock poisoned")
            .routine
            .is_empty());
        blocker.release();
    }

    #[tokio::test]
    async fn resident_fingerprint_change_drains_after_canceled_waiter() {
        let generation = test_inner_with_fingerprint(1, test_fingerprint(b"first")).await;
        let (executor, state) = test_executor(generation.clone(), test_config());
        let active_guard = match executor
            .acquire_existing_inner(test_request_metadata(), Some(test_fingerprint(b"first")))
            .await
            .unwrap()
        {
            InnerAcquisition::Ready { guard, .. } => guard,
            InnerAcquisition::Draining(_)
            | InnerAcquisition::Transition(_)
            | InnerAcquisition::Missing => {
                panic!("Test generation was not available")
            },
        };

        let changed = executor
            .acquire_existing_inner(test_request_metadata(), Some(test_fingerprint(b"second")))
            .await
            .unwrap();
        match changed {
            InnerAcquisition::Transition(_) => {},
            InnerAcquisition::Draining(inner) => assert!(Arc::ptr_eq(&inner, &generation)),
            InnerAcquisition::Ready { .. } | InnerAcquisition::Missing => {
                panic!("Changed fingerprint did not close generation admission")
            },
        }
        assert!(!generation.retirement_requested.load(Ordering::Acquire));
        tokio::task::yield_now().await;
        assert!(!generation.retired.load(Ordering::Acquire));

        // Dropping the changed-fingerprint acquisition represents cancellation
        // of the waiter. The runtime-owned drain must still finish after the
        // already admitted request completes.
        drop(active_guard);
        tokio::time::timeout(Duration::from_secs(1), generation.wait_until_retired())
            .await
            .unwrap()
            .unwrap();
        let state = state.lock().await;
        let replacement = state
            .inner
            .as_ref()
            .expect("Hot replacement did not publish a current generation");
        assert!(!Arc::ptr_eq(replacement, &generation));
        assert!(replacement.resident_fingerprint == Some(test_fingerprint(b"second")));
        assert!(state.retiring.is_none());
        assert!(state.hot_transition.is_none());
    }

    #[tokio::test]
    async fn canceled_drain_caller_does_not_wedge_generation_retirement() {
        let generation = test_inner(1).await;
        let (executor, state) = test_executor(generation.clone(), test_config());
        let active_guard = match executor
            .acquire_existing_inner(test_request_metadata(), None)
            .await
            .unwrap()
        {
            InnerAcquisition::Ready { guard, .. } => guard,
            InnerAcquisition::Draining(_)
            | InnerAcquisition::Transition(_)
            | InnerAcquisition::Missing => {
                panic!("Test generation was not available")
            },
        };

        let retirement_state = state.clone();
        let retirement_generation = generation.clone();
        let retirement = tokio::spawn(async move {
            LocalNodeExecutor::drain_and_retire_inner_state(
                &retirement_state,
                &retirement_generation,
                GenerationRetirementDiagnostics::proactive(
                    GenerationRetirementReason::PackageLimit,
                ),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !generation.retirement_requested.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        retirement.abort();
        assert!(retirement.await.unwrap_err().is_cancelled());
        drop(active_guard);

        tokio::time::timeout(Duration::from_secs(1), async {
            generation.wait_until_retired().await.unwrap();
            loop {
                if generation.server_handle.lock().await.child.is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(state.lock().await.inner.is_none());
    }

    #[tokio::test]
    async fn late_old_generation_retirement_preserves_replacement() {
        let old = test_inner(1).await;
        let replacement = test_inner(2).await;
        let state = Arc::new(Mutex::new(LocalNodeExecutorState {
            inner: Some(old.clone()),
            retiring: None,
            hot_transition: None,
            replacement_for_generation: None,
            next_generation: 2,
            next_transition: 0,
        }));
        old.server_handle
            .lock()
            .await
            .child_mut()
            .start_kill()
            .unwrap();

        assert!(LocalNodeExecutor::retire_inner_state(
            &state,
            &old,
            test_request_retirement(GenerationRetirementReason::RequestTimeout),
        )
        .await
        .unwrap());
        assert!(old.server_handle.lock().await.child.is_none());
        {
            let mut state = state.lock().await;
            assert_eq!(state.replacement_for_generation, Some(old.generation));
            state.inner = Some(replacement.clone());
            state.replacement_for_generation = None;
        }

        assert!(!LocalNodeExecutor::retire_inner_state(
            &state,
            &old,
            test_request_retirement(GenerationRetirementReason::ResponseStreamTimeout),
        )
        .await
        .unwrap());
        assert!(state
            .lock()
            .await
            .inner
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &replacement)));

        LocalNodeExecutor::retire_inner_state(
            &state,
            &replacement,
            test_request_retirement(GenerationRetirementReason::ProcessExiting),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn concurrent_retirements_remove_one_generation_once() {
        let generation = test_inner(1).await;
        let state = Arc::new(Mutex::new(LocalNodeExecutorState {
            inner: Some(generation.clone()),
            retiring: None,
            hot_transition: None,
            replacement_for_generation: None,
            next_generation: 1,
            next_transition: 0,
        }));

        let retirements = (0..8).map(|_| {
            LocalNodeExecutor::retire_inner_state(
                &state,
                &generation,
                GenerationRetirementDiagnostics::watchdog(),
            )
        });
        let results = join_all(retirements).await;

        assert_eq!(
            results
                .into_iter()
                .map(Result::unwrap)
                .filter(|retired| *retired)
                .count(),
            1
        );
        let state = state.lock().await;
        assert!(state.inner.is_none());
        assert_eq!(
            state.replacement_for_generation,
            Some(generation.generation)
        );
    }

    #[tokio::test]
    async fn retirement_reaps_child_after_retiring_caller_is_canceled() {
        let generation = test_inner(1).await;
        let (executor, state) = test_executor(generation.clone(), test_config());

        // Hold the child lock so the detached termination owner cannot finish
        // before the task that initiated retirement is canceled.
        let child_guard = generation.server_handle.lock().await;
        let retirement_state = state.clone();
        let retirement_generation = generation.clone();
        let retirement_task = tokio::spawn(async move {
            LocalNodeExecutor::retire_inner_state(
                &retirement_state,
                &retirement_generation,
                test_request_retirement(GenerationRetirementReason::RequestTimeout),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while state.lock().await.inner.is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        match executor
            .acquire_existing_inner(test_request_metadata(), None)
            .await
            .unwrap()
        {
            InnerAcquisition::Draining(inner) => assert!(Arc::ptr_eq(&inner, &generation)),
            InnerAcquisition::Ready { .. }
            | InnerAcquisition::Transition(_)
            | InnerAcquisition::Missing => {
                panic!("Unreaped generation did not fence replacement startup")
            },
        }
        retirement_task.abort();
        assert!(retirement_task.await.unwrap_err().is_cancelled());
        drop(child_guard);

        tokio::time::timeout(Duration::from_secs(1), async {
            generation.wait_until_retired().await.unwrap();
            loop {
                if generation.server_handle.lock().await.child.is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(state.lock().await.retiring.is_none());
    }

    #[tokio::test]
    async fn watchdog_starts_health_only_replacement_without_execute_descriptor() {
        let socket_dir = TempDir::new().unwrap();
        let socket_path = socket_dir.path().join("executor.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server_task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0; 1024];
                assert!(socket.read(&mut request).await.unwrap() > 0);
                socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\nConnection: close\r\n\r\n{\"status\":\"ok\"}",
                    )
                    .await
                    .unwrap();
            }
        });
        let client = Client::builder()
            .no_proxy()
            .unix_socket(socket_path)
            .build()
            .unwrap();
        let generation = test_inner_with_client(1, client).await;
        let state = Arc::new(Mutex::new(LocalNodeExecutorState {
            inner: Some(generation.clone()),
            retiring: None,
            hot_transition: None,
            replacement_for_generation: None,
            next_generation: 1,
            next_transition: 0,
        }));
        let mut config = test_config();
        config.health_check_timeout = Duration::from_secs(1);
        config.watchdog_interval = Duration::from_millis(1);
        config.watchdog_failure_threshold = 100;
        config.max_generation_age = Duration::from_nanos(1);
        config.max_rss_bytes = u64::MAX;
        let blocker = config
            .surge_coordinator
            .acquire(SurgePriority::Routine, Arc::from("blocker"))
            .await;
        let watchdog = tokio::spawn(LocalNodeExecutor::watch_generation(
            Arc::downgrade(&state),
            Arc::downgrade(&generation),
            config,
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let state = state.lock().await;
                if let Some(HotTransition::Candidate(candidate)) = &state.hot_transition {
                    assert!(candidate.descriptor.is_none());
                    return;
                }
                drop(state);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Watchdog did not request a health-only replacement");

        watchdog.abort();
        assert!(watchdog.await.unwrap_err().is_cancelled());
        LocalNodeExecutor::preempt_hot_transition_state(&state, "stale").await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while state.lock().await.hot_transition.is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Canceled health-only replacement retained transition ownership");
        blocker.release();
        generation.terminate().await.unwrap();
        server_task.abort();
        assert!(server_task.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn watchdog_resets_transient_miss_before_retiring_generation() {
        let socket_dir = TempDir::new().unwrap();
        let socket_path = socket_dir.path().join("executor.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let health_requests = Arc::new(AtomicUsize::new(0));
        let server_health_requests = health_requests.clone();
        let server_task = tokio::spawn(async move {
            for attempt in 1..=4 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0; 1024];
                assert!(socket.read(&mut request).await.unwrap() > 0);
                server_health_requests.fetch_add(1, Ordering::Relaxed);
                if attempt == 2 {
                    socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\nConnection: close\r\n\r\n{\"status\":\"ok\"}",
                        )
                        .await
                        .unwrap();
                } else {
                    socket
                        .write_all(
                            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .await
                        .unwrap();
                }
            }
        });
        let client = Client::builder()
            .no_proxy()
            .unix_socket(socket_path)
            .build()
            .unwrap();
        let generation = test_inner_with_client(1, client).await;
        let state = Arc::new(Mutex::new(LocalNodeExecutorState {
            inner: Some(generation.clone()),
            retiring: None,
            hot_transition: None,
            replacement_for_generation: None,
            next_generation: 1,
            next_transition: 0,
        }));
        let mut config = test_config();
        config.health_check_timeout = Duration::from_secs(1);
        config.watchdog_interval = Duration::from_millis(1);

        tokio::time::timeout(
            Duration::from_secs(1),
            LocalNodeExecutor::watch_generation(
                Arc::downgrade(&state),
                Arc::downgrade(&generation),
                config,
            ),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), server_task)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(health_requests.load(Ordering::Relaxed), 4);
        assert!(state.lock().await.inner.is_none());
        assert!(generation.server_handle.lock().await.child.is_none());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn pressure_clear_and_reentry_during_health_check_restarts_grace() {
        let socket_dir = TempDir::new().unwrap();
        let socket_path = socket_dir.path().join("executor.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let (blocked_health_started_sender, blocked_health_started_receiver) = oneshot::channel();
        let (release_blocked_health_sender, release_blocked_health_receiver) = oneshot::channel();
        let (post_reentry_health_started_sender, post_reentry_health_started_receiver) =
            oneshot::channel();
        let server_task = tokio::spawn(async move {
            let mut blocked_health_started_sender = Some(blocked_health_started_sender);
            let mut release_blocked_health_receiver = Some(release_blocked_health_receiver);
            let mut post_reentry_health_started_sender = Some(post_reentry_health_started_sender);
            let mut health_request = 0;
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0; 1024];
                assert!(socket.read(&mut request).await.unwrap() > 0);
                health_request += 1;
                if health_request == 2 {
                    blocked_health_started_sender
                        .take()
                        .unwrap()
                        .send(())
                        .unwrap();
                    release_blocked_health_receiver
                        .take()
                        .unwrap()
                        .await
                        .unwrap();
                } else if health_request == 3 {
                    post_reentry_health_started_sender
                        .take()
                        .unwrap()
                        .send(())
                        .unwrap();
                }
                socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\nConnection: close\r\n\r\n{\"status\":\"ok\"}",
                    )
                    .await
                    .unwrap();
            }
        });
        let client = Client::builder()
            .no_proxy()
            .unix_socket(socket_path)
            .build()
            .unwrap();
        let generation = test_inner_with_client(1, client).await;
        let state = Arc::new(Mutex::new(LocalNodeExecutorState {
            inner: Some(generation.clone()),
            retiring: None,
            hot_transition: None,
            replacement_for_generation: None,
            next_generation: 1,
            next_transition: 0,
        }));
        let pressure = MemoryPressureSignal::new(true);
        let mut config = test_config();
        config.health_check_timeout = Duration::from_secs(1);
        config.watchdog_interval = Duration::from_millis(1);
        config.max_rss_bytes = u64::MAX;
        config.memory_pressure = pressure.clone();
        config.memory_pressure_min_rss_bytes = 1;
        config.memory_pressure_grace = Duration::from_millis(500);
        let watchdog_task = tokio::spawn(LocalNodeExecutor::watch_generation(
            Arc::downgrade(&state),
            Arc::downgrade(&generation),
            config,
        ));

        // The first health check establishes the old implementation's sampled
        // grace. Hold the second check until that grace has expired, then prove
        // that a clear and re-entry observed during the check starts a new one.
        blocked_health_started_receiver.await.unwrap();
        tokio::time::sleep(Duration::from_millis(550)).await;
        assert!(pressure.set_active(false));
        assert!(!pressure.set_active(true));
        release_blocked_health_sender.send(()).unwrap();

        tokio::time::timeout(Duration::from_secs(1), post_reentry_health_started_receiver)
            .await
            .unwrap()
            .unwrap();
        assert!(state
            .lock()
            .await
            .inner
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &generation)));

        tokio::time::timeout(Duration::from_secs(1), watchdog_task)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), generation.wait_until_retired())
            .await
            .unwrap()
            .unwrap();
        assert!(state.lock().await.inner.is_none());
        server_task.abort();
        assert!(server_task.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn unhealthy_watchdog_preempts_stuck_proactive_drain() {
        let socket_dir = TempDir::new().unwrap();
        let socket_path = socket_dir.path().join("executor.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server_task = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0; 1024];
                assert!(socket.read(&mut request).await.unwrap() > 0);
                socket
                    .write_all(
                        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
            }
        });
        let client = Client::builder()
            .no_proxy()
            .unix_socket(socket_path)
            .build()
            .unwrap();
        let generation = test_inner_with_client(1, client).await;
        let active_guard = ActiveRequestGuard::new(generation.clone(), test_request_metadata());
        let state = Arc::new(Mutex::new(LocalNodeExecutorState {
            inner: Some(generation.clone()),
            retiring: None,
            hot_transition: None,
            replacement_for_generation: None,
            next_generation: 1,
            next_transition: 0,
        }));
        let mut config = test_config();
        config.health_check_timeout = Duration::from_secs(1);
        config.watchdog_interval = Duration::from_millis(1);
        config.max_generation_age = Duration::from_nanos(1);

        tokio::time::timeout(
            Duration::from_secs(1),
            LocalNodeExecutor::watch_generation(
                Arc::downgrade(&state),
                Arc::downgrade(&generation),
                config,
            ),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), server_task)
            .await
            .unwrap()
            .unwrap();

        assert!(generation.retired.load(Ordering::Acquire));
        assert_eq!(generation.active_requests.load(Ordering::Acquire), 1);
        assert!(state.lock().await.inner.is_none());
        assert!(generation.server_handle.lock().await.child.is_none());
        drop(active_guard);
    }

    #[tokio::test]
    async fn request_timeout_retires_generation_before_headers() {
        let socket_dir = TempDir::new().unwrap();
        let socket_path = socket_dir.path().join("executor.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let request_received = Arc::new(AtomicBool::new(false));
        let server_request_received = request_received.clone();
        let server_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            server_request_received.store(true, Ordering::Release);
            future::pending::<()>().await;
        });
        let client = Client::builder()
            .no_proxy()
            .unix_socket(socket_path)
            .build()
            .unwrap();
        let inner = test_inner_with_client(1, client).await;
        let mut config = test_config();
        config.node_process_timeout = Duration::from_millis(100);
        let (executor, state) = test_executor(inner.clone(), config);
        let (log_line_sender, _log_line_receiver) = mpsc::unbounded_channel();

        let response = executor
            .invoke(
                ExecutorRequest::BuildDeps(crate::executor::BuildDepsRequest {
                    deps: vec![],
                    upload_url: String::new(),
                }),
                log_line_sender,
                None,
            )
            .await
            .unwrap();
        assert!(request_received.load(Ordering::Acquire));
        assert_eq!(response.response, EXECUTE_TIMEOUT_RESPONSE_JSON.clone());
        {
            let state = state.lock().await;
            assert!(state.inner.is_none());
            assert_eq!(state.replacement_for_generation, Some(inner.generation));
        }
        assert!(inner.server_handle.lock().await.child.is_none());
        server_task.abort();
    }

    #[tokio::test]
    async fn pre_header_transport_failure_retires_generation() {
        let socket_dir = TempDir::new().unwrap();
        let socket_path = socket_dir.path().join("executor.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            assert!(socket.read(&mut request).await.unwrap() > 0);
        });
        let client = Client::builder()
            .no_proxy()
            .unix_socket(socket_path)
            .build()
            .unwrap();
        let inner = test_inner_with_client(1, client).await;
        let (executor, state) = test_executor(inner.clone(), test_config());
        let (log_line_sender, _log_line_receiver) = mpsc::unbounded_channel();

        let result = executor
            .invoke(
                ExecutorRequest::BuildDeps(crate::executor::BuildDepsRequest {
                    deps: vec![],
                    upload_url: String::new(),
                }),
                log_line_sender,
                None,
            )
            .await;
        assert!(result.is_err());
        server_task.await.unwrap();
        {
            let state = state.lock().await;
            assert!(state.inner.is_none());
            assert_eq!(state.replacement_for_generation, Some(inner.generation));
        }
        assert!(inner.server_handle.lock().await.child.is_none());
    }

    #[tokio::test]
    async fn response_body_transport_failure_retires_generation() {
        let socket_dir = TempDir::new().unwrap();
        let socket_path = socket_dir.path().join("executor.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nContent-Length: 100\r\n\r\n{",
                )
                .await
                .unwrap();
        });
        let client = Client::builder()
            .no_proxy()
            .unix_socket(socket_path)
            .build()
            .unwrap();
        let inner = test_inner_with_client(1, client).await;
        let (executor, state) = test_executor(inner.clone(), test_config());
        let (log_line_sender, _log_line_receiver) = mpsc::unbounded_channel();

        let result = executor
            .invoke(
                ExecutorRequest::BuildDeps(crate::executor::BuildDepsRequest {
                    deps: vec![],
                    upload_url: String::new(),
                }),
                log_line_sender,
                None,
            )
            .await;
        assert!(result.is_err());
        server_task.await.unwrap();
        {
            let state = state.lock().await;
            assert!(state.inner.is_none());
            assert_eq!(state.replacement_for_generation, Some(inner.generation));
        }
        assert!(inner.server_handle.lock().await.child.is_none());
    }

    #[tokio::test]
    async fn response_stream_timeout_retires_generation_after_headers() {
        let socket_dir = TempDir::new().unwrap();
        let socket_path = socket_dir.path().join("executor.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let headers_sent = Arc::new(AtomicBool::new(false));
        let server_headers_sent = headers_sent.clone();
        let server_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nContent-Length: 1\r\n\r\n",
                )
                .await
                .unwrap();
            server_headers_sent.store(true, Ordering::Release);
            future::pending::<()>().await;
        });
        let client = Client::builder()
            .no_proxy()
            .unix_socket(socket_path)
            .build()
            .unwrap();
        let inner = test_inner_with_client(1, client).await;
        let mut config = test_config();
        config.node_process_timeout = Duration::from_millis(100);
        let (executor, state) = test_executor(inner.clone(), config);
        let (log_line_sender, _log_line_receiver) = mpsc::unbounded_channel();

        let response = executor
            .invoke(
                ExecutorRequest::BuildDeps(crate::executor::BuildDepsRequest {
                    deps: vec![],
                    upload_url: String::new(),
                }),
                log_line_sender,
                None,
            )
            .await
            .unwrap();
        assert!(headers_sent.load(Ordering::Acquire));
        assert_eq!(response.response, EXECUTE_TIMEOUT_RESPONSE_JSON.clone());
        {
            let state = state.lock().await;
            assert!(state.inner.is_none());
            assert_eq!(state.replacement_for_generation, Some(inner.generation));
        }
        assert!(inner.server_handle.lock().await.child.is_none());
        server_task.abort();
    }

    #[tokio::test]
    async fn shutdown_retires_and_reaps_current_generation() {
        let inner = test_inner(1).await;
        let (executor, state) = test_executor(inner.clone(), test_config());

        NodeExecutor::shutdown(&executor);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if state.lock().await.inner.is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(inner.server_handle.lock().await.child.is_none());
    }

    #[tokio::test]
    async fn retirement_task_guard_publishes_only_unfinished_failure() {
        let inner = test_inner(1).await;
        let mut task_guard = RetirementTaskGuard::new(inner.clone());
        let retirement = async move {
            task_guard.disarm();
        };
        drop(retirement);

        assert!(inner.wait_until_retired().await.is_err());
        assert!(inner.retirement_failed.load(Ordering::Acquire));
        inner.terminate().await.unwrap();

        let retired = test_inner(2).await;
        retired.retired.store(true, Ordering::Release);
        drop(RetirementTaskGuard::new(retired.clone()));
        assert!(!retired.retirement_failed.load(Ordering::Acquire));
        retired.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_upgrades_topology_close_to_immediate_retirement() {
        let inner = test_inner(1).await;
        let (executor, state) = test_executor(inner.clone(), test_config());
        let active_guard = match executor
            .acquire_existing_inner(test_request_metadata(), None)
            .await
            .unwrap()
        {
            InnerAcquisition::Ready { guard, .. } => guard,
            InnerAcquisition::Draining(_)
            | InnerAcquisition::Transition(_)
            | InnerAcquisition::Missing => {
                panic!("Test generation was not available")
            },
        };

        executor.begin_close_for_topology_change();
        NodeExecutor::shutdown(&executor);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if state.lock().await.inner.is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(inner.server_handle.lock().await.child.is_none());
        drop(active_guard);
    }

    #[tokio::test]
    async fn shutdown_retries_failed_topology_retirement() {
        let inner = test_inner(1).await;
        inner
            .termination_failures_remaining
            .store(1, Ordering::Release);
        let (executor, state) = test_executor(inner.clone(), test_config());

        assert!(LocalNodeExecutor::retire_inner_state(
            &state,
            &inner,
            GenerationRetirementDiagnostics::topology_change(),
        )
        .await
        .is_err());
        {
            let state = state.lock().await;
            assert!(state.inner.is_none());
            assert!(state
                .retiring
                .as_ref()
                .is_some_and(|retiring| Arc::ptr_eq(retiring, &inner)));
        }
        assert!(inner.retirement_failed.load(Ordering::Acquire));
        assert!(inner.server_handle.lock().await.child.is_some());

        NodeExecutor::shutdown(&executor);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !inner.retired.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let state = state.lock().await;
        assert!(state.retiring.is_none());
        assert_eq!(state.replacement_for_generation, None);
        drop(state);
        assert!(!inner.retirement_failed.load(Ordering::Acquire));
        assert!(inner.server_handle.lock().await.child.is_none());
    }

    #[tokio::test]
    async fn topology_close_can_confirm_reaping_after_shutdown_retry() {
        let inner = test_inner(1).await;
        inner
            .termination_failures_remaining
            .store(1, Ordering::Release);
        let (executor, state) = test_executor(inner.clone(), test_config());

        assert!(LocalNodeExecutor::retire_inner_state(
            &state,
            &inner,
            GenerationRetirementDiagnostics::topology_change(),
        )
        .await
        .is_err());
        executor.begin_close_for_topology_change();
        // Removed-pool shutdown owns the retry. Set its synchronous boundary
        // without starting the duplicate background shutdown task in this
        // focused barrier test.
        executor.shutdown_started.store(true, Ordering::Release);

        executor.finish_close_for_topology_change().await.unwrap();

        assert!(state.lock().await.retiring.is_none());
        assert!(inner.retired.load(Ordering::Acquire));
        assert!(inner.server_handle.lock().await.child.is_none());
    }

    #[tokio::test]
    async fn shutdown_waits_for_in_progress_topology_retirement() {
        let inner = test_inner(1).await;
        let (_executor, state) = test_executor(inner.clone(), test_config());
        let child_guard = inner.server_handle.lock().await;
        let retirement_state = state.clone();
        let retirement_inner = inner.clone();
        let retirement = tokio::spawn(async move {
            LocalNodeExecutor::retire_inner_state(
                &retirement_state,
                &retirement_inner,
                GenerationRetirementDiagnostics::topology_change(),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while state.lock().await.retiring.is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let cleanup_state = state.clone();
        let cleanup_inner = inner.clone();
        let shutdown_cleanup = tokio::spawn(async move {
            LocalNodeExecutor::finish_retiring_inner_for_shutdown(&cleanup_state, &cleanup_inner)
                .await
        });
        assert!(!inner.retirement_failed.load(Ordering::Acquire));
        drop(child_guard);
        assert!(retirement.await.unwrap().unwrap());
        shutdown_cleanup.await.unwrap().unwrap();
        assert!(state.lock().await.retiring.is_none());
        assert!(inner.retired.load(Ordering::Acquire));
        assert!(!inner.retirement_failed.load(Ordering::Acquire));
        assert!(inner.server_handle.lock().await.child.is_none());
    }

    #[tokio::test]
    async fn topology_close_waits_for_preempting_retirement_to_reap() {
        let inner = test_inner(1).await;
        let (executor, state) = test_executor(inner.clone(), test_config());
        let executor = Arc::new(executor);
        let active_guard = match executor
            .acquire_existing_inner(test_request_metadata(), None)
            .await
            .unwrap()
        {
            InnerAcquisition::Ready { guard, .. } => guard,
            InnerAcquisition::Draining(_)
            | InnerAcquisition::Transition(_)
            | InnerAcquisition::Missing => {
                panic!("Test generation was not available")
            },
        };

        executor.begin_close_for_topology_change();
        let closing_executor = executor.clone();
        let topology_close =
            tokio::spawn(async move { closing_executor.finish_close_for_topology_change().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !inner.retirement_requested.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let child_guard = inner.server_handle.lock().await;
        let retirement_state = state.clone();
        let retirement_inner = inner.clone();
        let preempting_retirement = tokio::spawn(async move {
            LocalNodeExecutor::retire_inner_state(
                &retirement_state,
                &retirement_inner,
                GenerationRetirementDiagnostics::watchdog(),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while state.lock().await.retiring.is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        tokio::task::yield_now().await;
        assert!(!topology_close.is_finished());
        drop(child_guard);
        assert!(preempting_retirement.await.unwrap().unwrap());
        topology_close.await.unwrap().unwrap();
        drop(active_guard);
        assert!(inner.retired.load(Ordering::Acquire));
        assert!(inner.server_handle.lock().await.child.is_none());
    }

    #[tokio::test]
    async fn topology_close_waits_for_canceled_candidate_ownership_to_clear() {
        let inner = test_inner(1).await;
        let (executor, state) = test_executor(inner.clone(), test_config());
        let executor = Arc::new(executor);
        let canceled = Arc::new(AtomicBool::new(false));
        let status = Arc::new(HotTransitionStatus {
            failed: AtomicBool::new(false),
            promoted: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
        });
        let cleanup = test_cleanup_owner(
            &state,
            &executor.transition_changed,
            1,
            &status,
            GenerationRetirementReason::AgeLimit,
            &inner,
        );
        cleanup.attach_startup_child(inner.server_handle.clone());
        cleanup.attach_candidate(inner.clone());
        let mut state_guard = state.lock().await;
        state_guard.inner = None;
        state_guard.hot_transition = Some(HotTransition::Candidate(CandidateTransition {
            token: 1,
            expected: inner.clone(),
            target_fingerprint: None,
            descriptor: None,
            startup_started: true,
            reason: GenerationRetirementReason::AgeLimit,
            canceled: canceled.clone(),
            canceled_changed: Arc::new(Notify::new()),
            status,
            cleanup,
        }));
        drop(state_guard);

        let child_guard = inner.server_handle.lock().await;
        executor.begin_close_for_topology_change();
        let closing_executor = executor.clone();
        let topology_close =
            tokio::spawn(async move { closing_executor.finish_close_for_topology_change().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !canceled.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!topology_close.is_finished());

        drop(child_guard);
        topology_close.await.unwrap().unwrap();
        assert!(state.lock().await.hot_transition.is_none());
        assert!(inner.server_handle.lock().await.child.is_none());
    }
}
