use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    sync::{
        atomic::{
            AtomicBool,
            Ordering,
        },
        Arc,
        Mutex as StdMutex,
        RwLock,
    },
    time::Duration,
};

use async_trait::async_trait;
use common::{
    execution_start::FunctionExecutionStartGate,
    knobs::{
        LOCAL_NODE_EXECUTOR_MAX_RSS_BYTES,
        LOCAL_NODE_EXECUTOR_TOTAL_RSS_BUDGET_BYTES,
    },
    log_lines::LogLine,
    memory_pressure::MemoryPressureSignal,
    sha256::Sha256,
    types::Timestamp,
};
use errors::ErrorMetadata;
use model::{
    config::types::NodeExecutorPoolName,
    environment_variables::types::{
        EnvVarName,
        EnvVarValue,
    },
    source_packages::types::NodeExecutorPoolTopology,
};
use tokio::sync::{
    mpsc,
    oneshot,
    watch,
    Mutex,
    Notify,
    OnceCell,
};

use crate::{
    executor::{
        NodeExecutorCutoverClaim,
        NodeExecutorCutoverReservation,
        NodeExecutorCutoverTarget,
    },
    local::{
        DeploymentReplacementOutcome,
        LocalNodeExecutor,
        LocalNodeExecutorConfig,
        ResidentGenerationFingerprint,
    },
    ExecutorRequest,
    InvokeResponse,
    NodeExecutor,
};

const MAX_NAMED_POOLS: usize = 8;
const DEPLOYMENT_CUTOVER_ADMISSION_TIMEOUT: Duration = Duration::from_secs(120);
const FORCED_CUTOVER_PREEMPTION_INTERVAL: Duration = Duration::from_millis(100);
const ENVIRONMENT_FINGERPRINT_VERSION: &[u8] = b"local-node-environment-v1";

pub struct RoutedLocalNodeExecutorConfig {
    local: LocalNodeExecutorConfig,
    total_rss_budget_bytes: usize,
    memory_pressure: MemoryPressureSignal,
}

#[derive(Clone, Copy)]
enum TopologyPublicationOutcome {
    Applied,
    IgnoredStale,
    IgnoredDuplicate,
}

impl TopologyPublicationOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::IgnoredStale => "ignored_stale",
            Self::IgnoredDuplicate => "ignored_duplicate",
        }
    }
}

#[derive(Clone)]
struct RemovedPoolOwner {
    retiring_pool: Arc<StdMutex<Option<Arc<RoutedPool>>>>,
    result: watch::Receiver<Option<Result<(), ()>>>,
    result_sender: watch::Sender<Option<Result<(), ()>>>,
    cleanup_lock: Arc<Mutex<()>>,
    cleanup_permit: Arc<StdMutex<RemovedPoolCleanupLease>>,
}

enum RemovedPoolCleanupLease {
    Unconfirmed {
        permit: Option<crate::local::SurgePermit>,
    },
    Confirmed,
}

struct RemovedPoolCleanupAttemptGuard {
    result_sender: watch::Sender<Option<Result<(), ()>>>,
    armed: bool,
}

impl RemovedPoolCleanupAttemptGuard {
    fn new(result_sender: watch::Sender<Option<Result<(), ()>>>) -> Self {
        Self {
            result_sender,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RemovedPoolCleanupAttemptGuard {
    fn drop(&mut self) {
        if self.armed {
            // Keep cancellation or panic terminal for waiters while retaining
            // the exact pool and any bound cleanup lease for a later retry.
            self.result_sender.send_replace(Some(Err(())));
        }
    }
}

impl RemovedPoolOwner {
    fn shutdown(&self) {
        let pool = self
            .retiring_pool
            .lock()
            .expect("Local Node executor retirement owner lock poisoned")
            .as_ref()
            .cloned();
        if let Some(pool) = pool {
            pool.shutdown();
            let owner = self.clone();
            tokio::spawn(async move {
                // A prior termination or wait failure retains the exact pool
                // and lease. Shutdown is the retry boundary for that owner.
                let _ = owner.run_cleanup_attempt().await;
            });
        }
    }

    async fn run_cleanup_attempt(&self) -> Result<(), ()> {
        let _cleanup_guard = self.cleanup_lock.lock().await;
        if matches!(&*self.result.borrow(), Some(Ok(()))) {
            return Ok(());
        }
        let mut attempt_guard = RemovedPoolCleanupAttemptGuard::new(self.result_sender.clone());
        let Some(pool) = self
            .retiring_pool
            .lock()
            .expect("Local Node executor retirement owner lock poisoned")
            .as_ref()
            .cloned()
        else {
            return Err(());
        };
        if self.result.borrow().is_some() {
            self.result_sender.send_replace(None);
        }
        let result = pool.finish_removal_cleanup().await.map_err(|_| ());
        if result.is_err() {
            tracing::warn!(
                pool_name = %pool.name,
                lifecycle_context = "topology_retirement",
                outcome = "failure",
                "Local Node executor removed-pool cleanup failed"
            );
        } else {
            // Retaining completed removed pools would grow lifecycle state
            // across topology changes even though no cleanup remains.
            self.retiring_pool
                .lock()
                .expect("Local Node executor retirement owner lock poisoned")
                .take();
            let permit = {
                let mut cleanup_permit = self
                    .cleanup_permit
                    .lock()
                    .expect("Local Node removed-pool permit lock poisoned");
                match std::mem::replace(&mut *cleanup_permit, RemovedPoolCleanupLease::Confirmed) {
                    RemovedPoolCleanupLease::Unconfirmed { permit } => permit,
                    RemovedPoolCleanupLease::Confirmed => {
                        unreachable!("Removed Local Node pool cleanup was confirmed twice")
                    },
                }
            };
            if let Some(permit) = permit {
                permit.confirm_direct_child_reaped();
            }
        }
        self.result_sender.send_replace(Some(result));
        attempt_guard.disarm();
        result
    }

    fn retain_cleanup_permit(&self, permit: crate::local::SurgePermit) {
        let mut cleanup_permit = self
            .cleanup_permit
            .lock()
            .expect("Local Node removed-pool permit lock poisoned");
        let permit_slot = match &mut *cleanup_permit {
            RemovedPoolCleanupLease::Unconfirmed {
                permit: permit_slot,
            } => {
                if permit_slot.is_some() {
                    return;
                }
                permit_slot
            },
            RemovedPoolCleanupLease::Confirmed => return,
        };
        // Topology publication precedes committed-target reconstruction. Bind
        // the deployment lease to this exact removed owner now, so target
        // reconstruction failure cannot expose capacity while its child is
        // still draining.
        permit.set_phase("draining");
        *permit_slot = Some(permit.clone());
        drop(cleanup_permit);

        let mut owner = self.clone();
        tokio::spawn(async move {
            let mut automatic_retry_started = false;
            let mut observed_preemption_request = 0;
            loop {
                let cleanup_result = *owner.result.borrow_and_update();
                match cleanup_result {
                    Some(Ok(())) => return,
                    Some(Err(())) => {
                        // Keep the exact lease reachable with the failed owner.
                        // Retry it once through immediate shutdown; capacity
                        // remains closed until that exact cleanup confirms
                        // reaping. Later pressure, shutdown, or force can retry
                        // the same owner again.
                        if !automatic_retry_started {
                            owner.shutdown();
                            automatic_retry_started = true;
                        }
                    },
                    None => {},
                }
                tokio::select! {
                    changed = owner.result.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    },
                    request = permit.wait_for_preemption_request_after(
                        observed_preemption_request,
                    ) => {
                        observed_preemption_request = request;
                        // The permit was bound to this owner before a runtime
                        // waiter existed, so force must act on this incarnation
                        // rather than a later pool with the same name. Keep the
                        // watcher installed so a later forced deployment can
                        // retry the exact owner after repeated cleanup failure.
                        owner.shutdown();
                    },
                }
            }
        });
    }
}

struct RoutedPool {
    name: Arc<str>,
    introduced_at: Timestamp,
    config: LocalNodeExecutorConfig,
    executor: OnceCell<Arc<LocalNodeExecutor>>,
    initialization_lock: Mutex<()>,
    retired: AtomicBool,
    shutdown_started: AtomicBool,
}

impl RoutedPool {
    fn new(name: Arc<str>, introduced_at: Timestamp, config: &LocalNodeExecutorConfig) -> Self {
        Self {
            name: name.clone(),
            introduced_at,
            config: config.clone().with_pool_name(name),
            executor: OnceCell::new(),
            initialization_lock: Mutex::new(()),
            retired: AtomicBool::new(false),
            shutdown_started: AtomicBool::new(false),
        }
    }

    async fn executor(&self) -> anyhow::Result<Arc<LocalNodeExecutor>> {
        let _initialization_guard = self.initialization_lock.lock().await;
        self.executor_with_initialization_lock_held().await
    }

    async fn executor_with_initialization_lock_held(
        &self,
    ) -> anyhow::Result<Arc<LocalNodeExecutor>> {
        anyhow::ensure!(
            !self.retired.load(Ordering::Acquire),
            "Node executor pool topology changed during request selection"
        );
        let executor = self
            .executor
            .get_or_try_init(|| async {
                Ok::<_, anyhow::Error>(Arc::new(
                    LocalNodeExecutor::new_with_configuration(self.config.clone()).await?,
                ))
            })
            .await
            .cloned()?;
        if self.retired.load(Ordering::Acquire) {
            // Shutdown can race OnceCell initialization after it observed an
            // empty slot. Upgrade that newly created executor immediately.
            if self.shutdown_started.load(Ordering::Acquire) {
                executor.shutdown();
            } else {
                executor.begin_close_for_topology_change();
            }
            anyhow::bail!("Node executor pool topology changed during request selection");
        }
        Ok(executor)
    }

    async fn finish_removal_cleanup(&self) -> anyhow::Result<()> {
        let _initialization_guard = self.initialization_lock.lock().await;
        if let Some(executor) = self.executor.get() {
            executor.begin_close_for_topology_change();
            executor.finish_close_for_topology_change().await?;
        }
        Ok(())
    }

    fn retire_for_removal(
        self: &Arc<Self>,
        memory_pressure: MemoryPressureSignal,
    ) -> RemovedPoolOwner {
        let mut pressure = memory_pressure.subscribe();
        let pressure_at_removal = {
            // Publish retirement while the pressure value is stable. Otherwise
            // pressure could clear between these operations and the removed
            // incarnation would miss an active-at-removal upgrade.
            let pressure_state = memory_pressure.lock_state();
            self.retired.store(true, Ordering::Release);
            if let Some(executor) = self.executor.get() {
                executor.begin_close_for_topology_change();
            }
            pressure_state.is_active()
        };
        let pressure_at_watch_start = *pressure.borrow_and_update();
        if pressure_at_removal || pressure_at_watch_start {
            // Observe already-active pressure synchronously with removal
            // publication; the cleanup task may not be polled immediately.
            self.shutdown();
        }
        let (result_tx, result_rx) = watch::channel(None);
        let retiring_pool = Arc::new(StdMutex::new(Some(self.clone())));
        let cleanup_permit = Arc::new(StdMutex::new(RemovedPoolCleanupLease::Unconfirmed {
            permit: None,
        }));
        let owner = RemovedPoolOwner {
            retiring_pool,
            result: result_rx,
            result_sender: result_tx,
            cleanup_lock: Arc::new(Mutex::new(())),
            cleanup_permit,
        };
        let task_owner = owner.clone();
        tokio::spawn(async move {
            let _ = task_owner.run_cleanup_attempt().await;
        });
        let mut pressure_owner = owner.clone();
        tokio::spawn(async move {
            let mut pressure_active = pressure_at_watch_start;
            loop {
                if matches!(&*pressure_owner.result.borrow(), Some(Ok(()))) {
                    return;
                }
                tokio::select! {
                    changed = pressure_owner.result.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    },
                    changed = pressure.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        let active = *pressure.borrow_and_update();
                        if active && !pressure_active {
                            // Keep observing pressure re-entry through earlier
                            // cleanup failures. The exact removed owner remains
                            // the retry boundary before runtime reconstruction.
                            pressure_owner.shutdown();
                        }
                        pressure_active = active;
                    },
                }
            }
        });
        owner
    }

    fn enable(&self) -> anyhow::Result<()> {
        if let Some(executor) = self.executor.get() {
            executor.enable()?;
        }
        Ok(())
    }

    fn shutdown(&self) {
        self.shutdown_started.store(true, Ordering::Release);
        self.retired.store(true, Ordering::Release);
        if let Some(executor) = self.executor.get() {
            executor.shutdown();
        }
    }
}

struct RoutedState {
    default: Arc<RoutedPool>,
    topology: NodeExecutorPoolTopology,
    topology_version: Timestamp,
    publication: u64,
    last_reported_ignored_version: Option<Timestamp>,
    named: BTreeMap<NodeExecutorPoolName, Arc<RoutedPool>>,
    retiring: Vec<RemovedPoolOwner>,
    next_cutover_claim: u64,
    cutover: Option<ActiveCutover>,
}

#[derive(Clone)]
enum ActiveCutover {
    UncommittedClaim {
        claim: DeploymentCutoverClaim,
    },
    CommittedClaim {
        claim_id: u64,
        topology: NodeExecutorPoolTopology,
        version: Timestamp,
    },
    Pending {
        version: Timestamp,
    },
    Running {
        version: Timestamp,
        target: RuntimeCutoverTargetIdentity,
        ownership: Arc<RuntimeCutoverOwnership>,
        result: watch::Receiver<Option<Result<(), ()>>>,
        reservation_transfer: Option<RuntimeCutoverReservationTransfer>,
    },
}

#[derive(Clone)]
struct DeploymentCutoverClaim {
    id: u64,
    topology: NodeExecutorPoolTopology,
    displaced_recovery: Option<Timestamp>,
}

struct DeploymentCutoverClaimGuard {
    id: u64,
    state: Arc<RwLock<RoutedState>>,
    topology_updates: watch::Sender<u64>,
    committed_version: Option<Timestamp>,
}

impl DeploymentCutoverClaimGuard {
    fn owns_active_claim(&self) -> anyhow::Result<bool> {
        let state = self
            .state
            .read()
            .map_err(|_| anyhow::anyhow!("Local Node executor router lock poisoned"))?;
        Ok(matches!(
            state.cutover.as_ref(),
            Some(ActiveCutover::UncommittedClaim { claim }) if claim.id == self.id
        ))
    }
}

impl NodeExecutorCutoverClaim for DeploymentCutoverClaimGuard {
    fn commit(
        &mut self,
        topology: &NodeExecutorPoolTopology,
        version: Timestamp,
    ) -> anyhow::Result<()> {
        let publication = {
            let mut state = self
                .state
                .write()
                .map_err(|_| anyhow::anyhow!("Local Node executor router lock poisoned"))?;
            anyhow::ensure!(
                version >= state.topology_version,
                "Local Node executor deployment cutover is stale"
            );
            let claim = match state.cutover.as_ref() {
                Some(ActiveCutover::UncommittedClaim { claim }) if claim.id == self.id => claim,
                Some(
                    ActiveCutover::UncommittedClaim { .. }
                    | ActiveCutover::CommittedClaim { .. }
                    | ActiveCutover::Pending { .. }
                    | ActiveCutover::Running { .. },
                )
                | None => {
                    anyhow::bail!("Local Node executor deployment reservation lost its claim")
                },
            };
            anyhow::ensure!(
                &claim.topology == topology,
                "Committed Local Node executor topology disagrees with its reservation"
            );
            state.cutover = Some(ActiveCutover::CommittedClaim {
                claim_id: self.id,
                topology: topology.clone(),
                version,
            });
            state.publication
        };
        self.committed_version = Some(version);
        self.topology_updates.send_replace(publication);
        Ok(())
    }
}

impl Drop for DeploymentCutoverClaimGuard {
    fn drop(&mut self) {
        let publication = if let Ok(mut state) = self.state.write() {
            match self.committed_version {
                None if matches!(
                    state.cutover.as_ref(),
                    Some(ActiveCutover::UncommittedClaim { claim }) if claim.id == self.id
                ) =>
                {
                    let displaced_recovery = match state.cutover.take() {
                        Some(ActiveCutover::UncommittedClaim { claim }) => {
                            assert_eq!(
                                claim.id, self.id,
                                "Checked Local Node cutover claim changed identity"
                            );
                            claim.displaced_recovery
                        },
                        Some(
                            ActiveCutover::CommittedClaim { .. }
                            | ActiveCutover::Pending { .. }
                            | ActiveCutover::Running { .. },
                        )
                        | None => unreachable!("Checked Local Node cutover claim disappeared"),
                    };
                    state.cutover =
                        displaced_recovery.map(|version| ActiveCutover::Pending { version });
                    Some(state.publication)
                },
                Some(version)
                    if matches!(
                        state.cutover.as_ref(),
                        Some(ActiveCutover::CommittedClaim { claim_id, .. })
                            if *claim_id == self.id
                    ) =>
                {
                    state.cutover = Some(ActiveCutover::Pending { version });
                    Some(state.publication)
                },
                None | Some(_) => None,
            }
        } else {
            None
        };
        if let Some(publication) = publication {
            self.topology_updates.send_replace(publication);
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
struct RuntimeCutoverTargetIdentity {
    topology: NodeExecutorPoolTopology,
    source_package_id: model::source_packages::types::SourcePackageId,
    environment_sha256: common::sha256::Sha256Digest,
}

impl RuntimeCutoverTargetIdentity {
    fn new(target: &NodeExecutorCutoverTarget) -> Self {
        Self {
            topology: target.topology.clone(),
            source_package_id: target.source_package_id,
            environment_sha256: environment_fingerprint(&target.environment_variables),
        }
    }

    fn from_request(request: &crate::executor::ExecuteRequest) -> Self {
        Self {
            topology: request.node_executor_pool_topology.clone(),
            source_package_id: request.source_package_id,
            environment_sha256: environment_fingerprint(&request.environment_variables),
        }
    }
}

struct RuntimeCutoverOwnership;

#[derive(Clone)]
struct RuntimeCutoverReservationTransfer {
    state: Arc<StdMutex<RuntimeCutoverReservationTransferState>>,
}

enum RuntimeCutoverReservationTransferState {
    Open(oneshot::Sender<NodeExecutorCutoverReservation>),
    Accepted,
    ReceiverGone,
}

enum RuntimeCutoverReservationOffer {
    Accepted,
    AlreadyAccepted(NodeExecutorCutoverReservation),
    ReceiverGone(NodeExecutorCutoverReservation),
}

impl RuntimeCutoverReservationTransfer {
    fn new() -> (Self, oneshot::Receiver<NodeExecutorCutoverReservation>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                state: Arc::new(StdMutex::new(RuntimeCutoverReservationTransferState::Open(
                    sender,
                ))),
            },
            receiver,
        )
    }

    fn offer(&self, reservation: NodeExecutorCutoverReservation) -> RuntimeCutoverReservationOffer {
        let mut state = self
            .state
            .lock()
            .expect("Local Node cutover reservation transfer lock poisoned");
        match std::mem::replace(
            &mut *state,
            RuntimeCutoverReservationTransferState::ReceiverGone,
        ) {
            RuntimeCutoverReservationTransferState::Open(sender) => {
                match sender.send(reservation) {
                    Ok(()) => {
                        *state = RuntimeCutoverReservationTransferState::Accepted;
                        RuntimeCutoverReservationOffer::Accepted
                    },
                    Err(reservation) => RuntimeCutoverReservationOffer::ReceiverGone(reservation),
                }
            },
            RuntimeCutoverReservationTransferState::Accepted => {
                *state = RuntimeCutoverReservationTransferState::Accepted;
                RuntimeCutoverReservationOffer::AlreadyAccepted(reservation)
            },
            RuntimeCutoverReservationTransferState::ReceiverGone => {
                RuntimeCutoverReservationOffer::ReceiverGone(reservation)
            },
        }
    }
}

struct RuntimeCutoverTaskGuard {
    version: Timestamp,
    ownership: Arc<RuntimeCutoverOwnership>,
    state: Arc<RwLock<RoutedState>>,
    _session_permit: Option<crate::local::SurgePermit>,
    succeeded: bool,
}

impl RuntimeCutoverTaskGuard {
    fn new(
        version: Timestamp,
        ownership: Arc<RuntimeCutoverOwnership>,
        state: Arc<RwLock<RoutedState>>,
    ) -> Self {
        Self {
            version,
            ownership,
            state,
            _session_permit: None,
            succeeded: false,
        }
    }

    fn retain_session_permit(&mut self, permit: &crate::local::SurgePermit) {
        if self._session_permit.is_none() {
            self._session_permit = Some(permit.clone());
        }
    }

    fn mark_succeeded(&mut self) {
        self.succeeded = true;
    }
}

impl Drop for RuntimeCutoverTaskGuard {
    fn drop(&mut self) {
        if self.succeeded {
            return;
        }
        if let Ok(mut state) = self.state.write()
            && matches!(
                state.cutover.as_ref(),
                Some(ActiveCutover::Running {
                    version,
                    ownership,
                    ..
                }) if *version == self.version && Arc::ptr_eq(ownership, &self.ownership)
            )
        {
            // A spawned cutover task can be canceled or panic independently of
            // the backend. Restore only this exact task's ownership: a retry
            // for the same topology version may already have installed a new
            // Running owner after this task published failure.
            state.cutover = Some(ActiveCutover::Pending {
                version: self.version,
            });
        }
        crate::metrics::log_local_node_deployment_cutover_event("post_commit_failed");
        tracing::error!(
            topology_version = %self.version,
            lifecycle_context = "deployment_cutover",
            outcome = "post_commit_failed",
            "Committed Local Node executor cutover failed"
        );
    }
}

pub struct RoutedLocalNodeExecutor {
    local_config: LocalNodeExecutorConfig,
    total_rss_budget_bytes: usize,
    memory_pressure: MemoryPressureSignal,
    state: Arc<RwLock<RoutedState>>,
    topology_updates: watch::Sender<u64>,
    shutting_down: Arc<AtomicBool>,
    shutdown_changed: Arc<Notify>,
}

impl RoutedLocalNodeExecutorConfig {
    pub fn preflight_configuration(
        node_process_timeout: Duration,
        memory_pressure: MemoryPressureSignal,
    ) -> anyhow::Result<Self> {
        let total_rss_budget_bytes = *LOCAL_NODE_EXECUTOR_TOTAL_RSS_BUDGET_BYTES;
        anyhow::ensure!(
            total_rss_budget_bytes
                >= LOCAL_NODE_EXECUTOR_MAX_RSS_BYTES
                    .checked_mul(2)
                    .expect("Default local Node executor RSS budget overflow"),
            "LOCAL_NODE_EXECUTOR_TOTAL_RSS_BUDGET_BYTES must cover the default steady slot and \
             one LOCAL_NODE_EXECUTOR_MAX_RSS_BYTES surge slot"
        );
        Ok(Self {
            local: LocalNodeExecutor::preflight_configuration(
                node_process_timeout,
                memory_pressure.clone(),
            )?,
            total_rss_budget_bytes,
            memory_pressure,
        })
    }

    pub fn validate_pool_topology(
        &self,
        topology: &NodeExecutorPoolTopology,
    ) -> anyhow::Result<()> {
        validate_pool_topology(topology, self.total_rss_budget_bytes)
    }
}

impl RoutedLocalNodeExecutor {
    pub async fn new_with_configuration(
        config: RoutedLocalNodeExecutorConfig,
    ) -> anyhow::Result<Self> {
        crate::metrics::set_local_node_pool_configuration("default", None);
        crate::metrics::set_local_node_configured_named_pools(0);
        let default = Arc::new(RoutedPool::new(
            Arc::from("default"),
            Timestamp::MIN,
            &config.local,
        ));
        let (topology_updates, _) = watch::channel(0);
        Ok(Self {
            local_config: config.local,
            total_rss_budget_bytes: config.total_rss_budget_bytes,
            memory_pressure: config.memory_pressure,
            state: Arc::new(RwLock::new(RoutedState {
                default,
                topology: NodeExecutorPoolTopology::default(),
                topology_version: Timestamp::MIN,
                publication: 0,
                last_reported_ignored_version: None,
                named: BTreeMap::new(),
                retiring: Vec::new(),
                next_cutover_claim: 0,
                cutover: None,
            })),
            topology_updates,
            shutting_down: Arc::new(AtomicBool::new(false)),
            shutdown_changed: Arc::new(Notify::new()),
        })
    }

    async fn selected_pool(
        &self,
        request: &ExecutorRequest,
    ) -> anyhow::Result<(
        Arc<LocalNodeExecutor>,
        Option<ResidentGenerationFingerprint>,
        &'static str,
    )> {
        match request {
            ExecutorRequest::Execute { request, .. } => {
                let module = request.path_and_args.path().udf_path.module();
                let pool = self
                    .execute_pool(
                        module,
                        request.node_pool.as_ref(),
                        &request.node_executor_pool_topology,
                        request.topology_version,
                    )
                    .await?;
                let fingerprint = ResidentGenerationFingerprint {
                    source_package_id: request.source_package_id,
                    environment_sha256: environment_fingerprint(&request.environment_variables),
                    topology_version: request.topology_version,
                };
                self.ensure_execution_cutover(request, &pool, &fingerprint)
                    .await?;
                let executor = pool.executor().await?;
                Ok((executor, Some(fingerprint), "execute"))
            },
            ExecutorRequest::Analyze(_) | ExecutorRequest::BuildDeps(_) => {
                let (pool, request_kind) = {
                    let state = self
                        .state
                        .read()
                        .map_err(|_| anyhow::anyhow!("Local Node executor router lock poisoned"))?;
                    let request_kind = match request {
                        ExecutorRequest::Analyze(_) => "analyze",
                        ExecutorRequest::BuildDeps(_) => "build_deps",
                        ExecutorRequest::Execute { .. } => unreachable!(),
                    };
                    (state.default.clone(), request_kind)
                };
                Ok((pool.executor().await?, None, request_kind))
            },
        }
    }

    async fn resident_cutover_pools(
        state: &Arc<RwLock<RoutedState>>,
        topology: &NodeExecutorPoolTopology,
    ) -> anyhow::Result<Vec<Arc<RoutedPool>>> {
        let pools = {
            let state = state
                .read()
                .map_err(|_| anyhow::anyhow!("Local Node executor router lock poisoned"))?;
            let configured_names: BTreeSet<_> = topology.values().collect();
            let mut pools = vec![state.default.clone()];
            pools.extend(
                state
                    .named
                    .iter()
                    .filter(|(name, _)| configured_names.contains(name))
                    .map(|(_, pool)| pool.clone()),
            );
            pools
        };
        let mut resident = Vec::new();
        for pool in pools {
            if let Some(executor) = pool.executor.get()
                && executor.has_resident_generation().await
            {
                resident.push(pool);
            }
        }
        Ok(resident)
    }

    async fn resident_cutover_needed(
        &self,
        topology: &NodeExecutorPoolTopology,
        fingerprint: &ResidentGenerationFingerprint,
    ) -> anyhow::Result<bool> {
        for pool in Self::resident_cutover_pools(&self.state, topology).await? {
            let executor = pool
                .executor
                .get()
                .expect("Resident Local Node executor pool has no executor");
            if !executor.resident_fingerprint_matches(fingerprint).await {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn try_claim_deployment_cutover(
        &self,
        topology: &NodeExecutorPoolTopology,
    ) -> anyhow::Result<Option<DeploymentCutoverClaimGuard>> {
        let publication = {
            let mut state = self
                .state
                .write()
                .map_err(|_| anyhow::anyhow!("Local Node executor router lock poisoned"))?;
            anyhow::ensure!(
                !self.shutting_down.load(Ordering::Acquire),
                "Local Node executor router is shutting down"
            );
            let displaced_recovery = match &state.cutover {
                None => None,
                Some(ActiveCutover::Pending { version }) => Some(*version),
                Some(
                    ActiveCutover::UncommittedClaim { .. }
                    | ActiveCutover::CommittedClaim { .. }
                    | ActiveCutover::Running { .. },
                ) => return Ok(None),
            };
            state.next_cutover_claim = state
                .next_cutover_claim
                .checked_add(1)
                .expect("Local Node executor cutover claim id overflow");
            let id = state.next_cutover_claim;
            state.cutover = Some(ActiveCutover::UncommittedClaim {
                claim: DeploymentCutoverClaim {
                    id,
                    topology: topology.clone(),
                    displaced_recovery,
                },
            });
            (id, state.publication)
        };
        self.topology_updates.send_replace(publication.1);
        Ok(Some(DeploymentCutoverClaimGuard {
            id: publication.0,
            state: self.state.clone(),
            topology_updates: self.topology_updates.clone(),
            committed_version: None,
        }))
    }

    fn install_committed_cutover(
        &self,
        topology: &NodeExecutorPoolTopology,
        version: Timestamp,
    ) -> anyhow::Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| anyhow::anyhow!("Local Node executor router lock poisoned"))?;
        anyhow::ensure!(
            !self.shutting_down.load(Ordering::Acquire),
            "Local Node executor router is shutting down"
        );
        anyhow::ensure!(
            version >= state.topology_version,
            "Local Node executor deployment cutover is stale"
        );
        if version == state.topology_version {
            anyhow::ensure!(
                topology == &state.topology,
                "Local Node executor topology disagrees at the cutover version"
            );
        }
        let replacement = match &state.cutover {
            Some(ActiveCutover::UncommittedClaim { .. }) => anyhow::bail!(
                "Local Node executor cutover requires its exact deployment reservation"
            ),
            Some(ActiveCutover::CommittedClaim {
                topology: claimed_topology,
                version: claimed_version,
                ..
            }) => {
                anyhow::ensure!(
                    claimed_topology == topology && *claimed_version == version,
                    "Another Local Node executor topology cutover is still active"
                );
                None
            },
            Some(ActiveCutover::Running {
                version: running_version,
                ..
            }) => {
                anyhow::ensure!(
                    *running_version == version,
                    "Another Local Node executor topology cutover is still active"
                );
                None
            },
            Some(ActiveCutover::Pending {
                version: pending_version,
            }) => {
                anyhow::ensure!(
                    *pending_version <= version,
                    "Local Node executor deployment cutover is stale"
                );
                Some(ActiveCutover::Pending { version })
            },
            None => Some(ActiveCutover::Pending { version }),
        };
        if let Some(replacement) = replacement {
            state.cutover = Some(replacement);
        }
        let publication = state.publication;
        drop(state);
        self.topology_updates.send_replace(publication);
        Ok(())
    }

    fn start_runtime_cutover(
        &self,
        target: NodeExecutorCutoverTarget,
        version: Timestamp,
        reservation: Option<NodeExecutorCutoverReservation>,
    ) -> anyhow::Result<watch::Receiver<Option<Result<(), ()>>>> {
        let result = self.start_runtime_cutover_inner(target, version, reservation);
        if result.is_err() {
            Self::record_runtime_cutover_start_failure(version);
        }
        result
    }

    fn record_runtime_cutover_start_failure(version: Timestamp) {
        crate::metrics::log_local_node_deployment_cutover_event("post_commit_failed");
        tracing::error!(
            topology_version = %version,
            lifecycle_context = "deployment_cutover",
            outcome = "start_failed",
            "Committed Local Node executor cutover failed before runtime ownership was installed"
        );
    }

    fn start_runtime_cutover_inner(
        &self,
        target: NodeExecutorCutoverTarget,
        version: Timestamp,
        reservation: Option<NodeExecutorCutoverReservation>,
    ) -> anyhow::Result<watch::Receiver<Option<Result<(), ()>>>> {
        let mut reservation = reservation;
        let target_identity = RuntimeCutoverTargetIdentity::new(&target);
        let mut state = self
            .state
            .write()
            .map_err(|_| anyhow::anyhow!("Local Node executor router lock poisoned"))?;
        match &state.cutover {
            Some(ActiveCutover::Running {
                version: running_version,
                target: running_target,
                result,
                reservation_transfer,
                ..
            }) => {
                anyhow::ensure!(
                    *running_version == version,
                    "Another Local Node executor topology cutover is still active"
                );
                anyhow::ensure!(
                    running_target == &target_identity,
                    "Local Node executor cutover target disagrees at the same version"
                );
                let result = result.clone();
                let Some(offered_reservation) = reservation.take() else {
                    return Ok(result);
                };
                let Some(reservation_transfer) = reservation_transfer else {
                    // This owner already started with a deployment reservation,
                    // so an additional reservation is not part of its lifecycle.
                    drop(offered_reservation);
                    return Ok(result);
                };
                match reservation_transfer.offer(offered_reservation) {
                    RuntimeCutoverReservationOffer::Accepted => return Ok(result),
                    RuntimeCutoverReservationOffer::AlreadyAccepted(reservation) => {
                        drop(reservation);
                        return Ok(result);
                    },
                    RuntimeCutoverReservationOffer::ReceiverGone(returned_reservation) => {
                        // The result receiver can outlive a canceled task. Keep
                        // the pre-commit reservation and replace that exact dead
                        // runtime owner instead of releasing capacity to queued
                        // work while the committed cutover is still pending.
                        reservation = Some(returned_reservation);
                    },
                }
            },
            Some(ActiveCutover::CommittedClaim {
                topology,
                version: claimed_version,
                ..
            }) => {
                anyhow::ensure!(
                    *claimed_version == version && topology == &target.topology,
                    "Another Local Node executor topology cutover is still active"
                );
            },
            Some(ActiveCutover::Pending {
                version: pending_version,
            }) => {
                anyhow::ensure!(
                    *pending_version == version,
                    "Another Local Node executor topology cutover is still active"
                );
            },
            Some(ActiveCutover::UncommittedClaim { .. }) => {
                anyhow::bail!("Local Node executor deployment reservation has not committed")
            },
            None => {
                anyhow::ensure!(
                    state.topology_version == version && state.topology == target.topology,
                    "Local Node executor cutover has no matching topology publication"
                );
            },
        }
        let (result_tx, result_rx) = watch::channel(None);
        let (reservation_transfer, reservation_receiver) = if reservation.is_none() {
            let (transfer, receiver) = RuntimeCutoverReservationTransfer::new();
            (Some(transfer), Some(receiver))
        } else {
            (None, None)
        };
        let ownership = Arc::new(RuntimeCutoverOwnership);
        state.cutover = Some(ActiveCutover::Running {
            version,
            target: target_identity.clone(),
            ownership: ownership.clone(),
            result: result_rx.clone(),
            reservation_transfer: reservation_transfer.clone(),
        });
        let publication = state.publication;
        drop(state);
        self.topology_updates.send_replace(publication);

        let state = self.state.clone();
        let topology_updates = self.topology_updates.clone();
        let coordinator = self.local_config.surge_coordinator();
        let memory_pressure = self.memory_pressure.clone();
        let shutting_down = self.shutting_down.clone();
        let shutdown_changed = self.shutdown_changed.clone();
        // Capture the guard before spawning so an unpolled runtime task still
        // restores this exact committed cutover to recoverable pending state.
        let mut task_guard =
            RuntimeCutoverTaskGuard::new(version, ownership.clone(), state.clone());
        tokio::spawn(async move {
            let environment_sha256 = target_identity.environment_sha256.clone();
            let mut reservation = reservation;
            let mut reservation_receiver = reservation_receiver;
            let mut reserved_permit = reservation
                .as_mut()
                .and_then(|reservation| reservation.permit.take());
            if let Some(permit) = &reserved_permit {
                task_guard.retain_session_permit(permit);
            }
            let result = async {
                let mut resident_pools = None;
                let removed_pool_owners = {
                    let state = state
                        .read()
                        .map_err(|_| anyhow::anyhow!("Local Node executor router lock poisoned"))?;
                    state.retiring.clone()
                };
                if !removed_pool_owners.is_empty() {
                    // A removed pool's old generation is the extra process
                    // relative to the newly published topology. Reap it under
                    // the one global surge permit before starting a candidate.
                    if reserved_permit.is_none() {
                        reserved_permit = Some(
                            acquire_runtime_cutover_permit_with_reservation(
                                &coordinator,
                                Arc::from("removed"),
                                &shutting_down,
                                &shutdown_changed,
                                &mut reservation_receiver,
                            )
                            .await?,
                        );
                        task_guard.retain_session_permit(
                            reserved_permit
                                .as_ref()
                                .expect("Local Node cutover session permit is missing"),
                        );
                    }
                    let permit = reserved_permit
                        .as_ref()
                        .expect("Local Node cutover session permit is missing");
                    permit.set_phase("draining");
                    // Execution-side recovery can reach this point without a
                    // pre-commit reservation. Bind the acquired session to
                    // every exact removed owner before the first cleanup await
                    // so task loss cannot strand an occupied coordinator
                    // without a permit handle that confirmed reaping can
                    // release.
                    for owner in &removed_pool_owners {
                        owner.retain_cleanup_permit(permit.clone());
                    }
                    for mut removed_pool_owner in removed_pool_owners {
                        wait_for_removed_pool_cleanup_with_preemption(
                            &mut removed_pool_owner,
                            permit,
                            &memory_pressure,
                            &shutting_down,
                            &shutdown_changed,
                        )
                        .await?;
                    }
                    permit.confirm_direct_child_reaped();
                    permit.set_phase("reservation");
                    resident_pools = Some(
                        RoutedLocalNodeExecutor::resident_cutover_pools(&state, &target.topology)
                            .await?,
                    );
                }
                // Discover residents only after runtime ownership is visible and
                // removed pools are reaped. A pool can become resident while an
                // execution-side recovery task is claiming the pending cutover.
                let pools = match resident_pools {
                    Some(pools) => pools,
                    None => {
                        RoutedLocalNodeExecutor::resident_cutover_pools(&state, &target.topology)
                            .await?
                    },
                };
                for pool in pools {
                    let Some(executor) = pool.executor.get().cloned() else {
                        continue;
                    };
                    if !executor.has_resident_generation().await {
                        continue;
                    }
                    if reserved_permit.is_none() {
                        reserved_permit = Some(
                            acquire_runtime_cutover_permit_with_reservation(
                                &coordinator,
                                pool.name.clone(),
                                &shutting_down,
                                &shutdown_changed,
                                &mut reservation_receiver,
                            )
                            .await?,
                        );
                        task_guard.retain_session_permit(
                            reserved_permit
                                .as_ref()
                                .expect("Local Node cutover session permit is missing"),
                        );
                    }
                    let permit = reserved_permit
                        .as_ref()
                        .expect("Local Node cutover session permit is missing")
                        .clone();
                    let outcome = executor
                        .replace_for_deployment(
                            Some(ResidentGenerationFingerprint {
                                source_package_id: target.source_package_id,
                                environment_sha256: environment_sha256.clone(),
                                topology_version: version,
                            }),
                            target.source_package.clone(),
                            permit,
                        )
                        .await;
                    let session_permit = reserved_permit
                        .as_ref()
                        .expect("Local Node cutover session permit is missing");
                    if session_permit.direct_child_cleanup_confirmed() {
                        session_permit.set_phase("reservation");
                    }
                    let outcome = outcome?;
                    if matches!(outcome, DeploymentReplacementOutcome::Promoted) {
                        crate::metrics::log_local_node_deployment_cutover_event("promoted");
                    }
                }
                if let Some(permit) = reserved_permit.take() {
                    permit.release();
                }
                anyhow::ensure!(
                    !shutting_down.load(Ordering::Acquire),
                    "Local Node executor router shut down during cutover"
                );
                anyhow::Ok(())
            }
            .await;

            let mut published_result = result.map_err(|_| ());
            let publication = match state.write() {
                Ok(mut state) => {
                    state
                        .retiring
                        .retain(|owner| !matches!(&*owner.result.borrow(), Some(Ok(()))));
                    if matches!(
                        state.cutover.as_ref(),
                        Some(ActiveCutover::Running {
                            version: running_version,
                            ownership: running_ownership,
                            ..
                        }) if *running_version == version
                            && Arc::ptr_eq(running_ownership, &ownership)
                    ) {
                        state.cutover = published_result
                            .is_err()
                            .then_some(ActiveCutover::Pending { version });
                    }
                    Some(state.publication)
                },
                Err(_) => {
                    published_result = Err(());
                    tracing::error!(
                        topology_version = %version,
                        lifecycle_context = "deployment_cutover",
                        outcome = "state_publication_failed",
                        "Local Node executor router lock poisoned after cutover"
                    );
                    None
                },
            };
            result_tx.send_replace(Some(published_result));
            if let Some(publication) = publication {
                topology_updates.send_replace(publication);
            }
            if result_tx
                .borrow()
                .as_ref()
                .is_some_and(|result| result.is_ok())
            {
                task_guard.mark_succeeded();
            }
        });
        Ok(result_rx)
    }

    async fn wait_for_runtime_cutover(
        mut result: watch::Receiver<Option<Result<(), ()>>>,
    ) -> anyhow::Result<()> {
        loop {
            if let Some(result) = *result.borrow() {
                return result
                    .map_err(|()| anyhow::anyhow!("Committed Local Node executor cutover failed"));
            }
            result.changed().await.map_err(|_| {
                anyhow::anyhow!("Local Node executor cutover result publication stopped")
            })?;
        }
    }

    async fn wait_for_execution_target_ready(
        pool: &RoutedPool,
        fingerprint: &ResidentGenerationFingerprint,
        result: watch::Receiver<Option<Result<(), ()>>>,
    ) -> anyhow::Result<()> {
        let Some(executor) = pool.executor.get() else {
            return Self::wait_for_runtime_cutover(result).await;
        };
        // The caller has already matched the complete topology, source, and
        // environment target. The local generation owns only the latter two.
        let target_ready = executor.wait_for_resident_package_and_environment_ready(fingerprint);
        tokio::pin!(target_ready);
        let cutover_complete = Self::wait_for_runtime_cutover(result);
        tokio::pin!(cutover_complete);
        // Full cutover completion retains resource and cleanup ownership, but it
        // is not the availability boundary for a pool whose target is ready.
        // Poll readiness first because a later old-child cleanup failure does
        // not make the already-promoted replacement unsafe for execution.
        tokio::select! {
            biased;
            () = &mut target_ready => Ok(()),
            result = &mut cutover_complete => result,
        }
    }

    async fn ensure_execution_cutover(
        &self,
        request: &crate::executor::ExecuteRequest,
        pool: &RoutedPool,
        fingerprint: &ResidentGenerationFingerprint,
    ) -> anyhow::Result<()> {
        let version = request.topology_version;
        let request_target = RuntimeCutoverTargetIdentity::from_request(request);
        let mut topology_updates = self.topology_updates.subscribe();
        loop {
            anyhow::ensure!(
                !self.shutting_down.load(Ordering::Acquire),
                "Local Node executor router is shutting down"
            );
            let _ = topology_updates.borrow_and_update();
            let (cutover, published_version) = {
                let state = self
                    .state
                    .read()
                    .map_err(|_| anyhow::anyhow!("Local Node executor router lock poisoned"))?;
                (state.cutover.clone(), state.topology_version)
            };
            if published_version > version {
                Self::ensure_stale_execution_resident(pool, fingerprint).await?;
                return Ok(());
            }
            let Some(cutover) = cutover else {
                if !self
                    .resident_cutover_needed(&request.node_executor_pool_topology, fingerprint)
                    .await?
                {
                    return Ok(());
                }
                let publication = {
                    let mut state = self
                        .state
                        .write()
                        .map_err(|_| anyhow::anyhow!("Local Node executor router lock poisoned"))?;
                    if state.cutover.is_some() {
                        None
                    } else {
                        state.cutover = Some(ActiveCutover::Pending { version });
                        Some(state.publication)
                    }
                };
                if let Some(publication) = publication {
                    self.topology_updates.send_replace(publication);
                }
                continue;
            };
            match cutover {
                ActiveCutover::UncommittedClaim { .. } => {
                    // Old committed work may continue on an exact resident, but
                    // the deployment claim fences creation of any runtime owner
                    // until commit or cancellation resolves that claim.
                    if let Some(executor) = pool.executor.get()
                        && executor.resident_fingerprint_matches(fingerprint).await
                    {
                        return Ok(());
                    }
                    topology_updates.changed().await.map_err(|_| {
                        anyhow::anyhow!("Local Node executor cutover claim publication stopped")
                    })?;
                },
                ActiveCutover::CommittedClaim {
                    version: claimed_version,
                    ..
                } => {
                    if claimed_version > version {
                        Self::ensure_stale_execution_resident(pool, fingerprint).await?;
                        return Ok(());
                    }
                    if claimed_version < version {
                        topology_updates.changed().await.map_err(|_| {
                            anyhow::anyhow!(
                                "Local Node executor committed claim publication stopped"
                            )
                        })?;
                        continue;
                    }
                    let target = NodeExecutorCutoverTarget {
                        topology: request.node_executor_pool_topology.clone(),
                        source_package: request.source_package.clone(),
                        source_package_id: request.source_package_id,
                        environment_variables: request.environment_variables.clone(),
                    };
                    let result = self.start_runtime_cutover(target, version, None)?;
                    return Self::wait_for_execution_target_ready(pool, fingerprint, result).await;
                },
                ActiveCutover::Pending {
                    version: pending_version,
                } => {
                    if pending_version > version {
                        Self::ensure_stale_execution_resident(pool, fingerprint).await?;
                        return Ok(());
                    }
                    if pending_version < version {
                        let publication = {
                            let mut state = self.state.write().map_err(|_| {
                                anyhow::anyhow!("Local Node executor router lock poisoned")
                            })?;
                            if matches!(
                                state.cutover.as_ref(),
                                Some(ActiveCutover::Pending { version })
                                    if *version == pending_version
                            ) {
                                state.cutover = Some(ActiveCutover::Pending { version });
                                Some(state.publication)
                            } else {
                                None
                            }
                        };
                        if let Some(publication) = publication {
                            self.topology_updates.send_replace(publication);
                        }
                        continue;
                    }
                    let target = NodeExecutorCutoverTarget {
                        topology: request.node_executor_pool_topology.clone(),
                        source_package: request.source_package.clone(),
                        source_package_id: request.source_package_id,
                        environment_variables: request.environment_variables.clone(),
                    };
                    let result = self.start_runtime_cutover(target, version, None)?;
                    return Self::wait_for_execution_target_ready(pool, fingerprint, result).await;
                },
                ActiveCutover::Running {
                    version: running_version,
                    target,
                    result,
                    ..
                } => {
                    if running_version > version {
                        Self::ensure_stale_execution_resident(pool, fingerprint).await?;
                        return Ok(());
                    }
                    if target == request_target {
                        return Self::wait_for_execution_target_ready(pool, fingerprint, result)
                            .await;
                    }
                    anyhow::ensure!(
                        running_version < version,
                        "Local Node executor cutover target disagrees at the same version"
                    );
                    // Do not join a source or environment target from an older
                    // snapshot. Wait for its identity-fenced owner to resolve,
                    // then install the newer target in the next loop iteration.
                    let _ = Self::wait_for_runtime_cutover(result).await;
                },
            }
        }
    }

    async fn ensure_stale_execution_resident(
        pool: &RoutedPool,
        fingerprint: &ResidentGenerationFingerprint,
    ) -> anyhow::Result<()> {
        let executor = pool.executor.get().ok_or_else(|| {
            anyhow::anyhow!("Node executor request has no stale resident generation")
        })?;
        anyhow::ensure!(
            executor.resident_fingerprint_matches(fingerprint).await,
            "Node executor request uses a stale resident generation fingerprint"
        );
        Ok(())
    }

    async fn execute_pool(
        &self,
        module: &sync_types::CanonicalizedModulePath,
        requested_pool: Option<&NodeExecutorPoolName>,
        request_topology: &NodeExecutorPoolTopology,
        request_version: Timestamp,
    ) -> anyhow::Result<Arc<RoutedPool>> {
        let mut topology_updates = self.topology_updates.subscribe();
        let mut deferred_by_claim = false;
        loop {
            anyhow::ensure!(
                !self.shutting_down.load(Ordering::Acquire),
                "Local Node executor router is shutting down"
            );
            let _ = topology_updates.borrow_and_update();
            let should_reconcile = {
                let state = self
                    .state
                    .read()
                    .map_err(|_| anyhow::anyhow!("Local Node executor router lock poisoned"))?;
                if request_version <= state.topology_version {
                    if request_topology != &state.topology {
                        if request_version < state.topology_version {
                            anyhow::bail!("Node executor request uses a stale pool topology");
                        }
                        anyhow::bail!(
                            "Node executor request topology disagrees at the publication version"
                        );
                    }
                    let topology_matches = match requested_pool {
                        Some(pool) => state.topology.get(module) == Some(pool),
                        None => {
                            state.topology.get(module).is_none()
                                && state
                                    .topology
                                    .default_routes()
                                    .is_none_or(|routes| routes.contains(module))
                        },
                    };
                    anyhow::ensure!(
                        topology_matches,
                        "Node executor request does not match the committed pool topology"
                    );
                    let selected = match requested_pool {
                        Some(pool) => state.named.get(pool).cloned().ok_or_else(|| {
                            anyhow::anyhow!("Committed Node executor pool has no runtime slot")
                        }),
                        None => Ok(state.default.clone()),
                    }?;
                    // A removed name can be reintroduced with the same routes.
                    // Fence snapshots from its prior incarnation even when the
                    // complete topology happens to compare equal again.
                    anyhow::ensure!(
                        request_version >= selected.introduced_at,
                        "Node executor request predates the current pool incarnation"
                    );
                    return Ok(selected);
                }
                let claim_blocks = matches!(
                    state.cutover.as_ref(),
                    Some(ActiveCutover::UncommittedClaim { .. })
                );
                deferred_by_claim |= claim_blocks;
                deferred_by_claim && !claim_blocks
            };
            if should_reconcile {
                self.reconcile_pool_topology(request_topology, request_version)?;
                continue;
            }
            topology_updates
                .changed()
                .await
                .map_err(|_| anyhow::anyhow!("Local Node executor topology publication stopped"))?;
        }
    }
}

fn validate_pool_topology(
    topology: &NodeExecutorPoolTopology,
    total_rss_budget_bytes: usize,
) -> anyhow::Result<()> {
    let invalid_module_path = |path: &sync_types::CanonicalizedModulePath| {
        path.is_system()
            || path.is_deps()
            || path.is_http()
            || path.is_cron()
            || path.as_str() == "schema.js"
            || path.as_str() == "auth.config.js"
    };
    if topology.keys().any(invalid_module_path)
        || topology
            .default_routes()
            .is_some_and(|routes| routes.iter().any(invalid_module_path))
        || topology
            .default_routes()
            .is_some_and(|routes| routes.iter().any(|path| topology.contains_key(path)))
    {
        anyhow::bail!(ErrorMetadata::bad_request(
            "InvalidNodeExecutorPoolModule",
            "A Node executor pool can only be assigned to a Node action module",
        ));
    }
    let names: BTreeSet<_> = topology.values().collect();
    if names.len() > MAX_NAMED_POOLS {
        anyhow::bail!(ErrorMetadata::bad_request(
            "TooManyNodeExecutorPools",
            format!(
                "A deployment can declare at most {MAX_NAMED_POOLS} dedicated Node executor pools"
            ),
        ));
    }
    let slots = names
        .len()
        .checked_add(2)
        .expect("validated Node executor process-slot count overflow");
    let required = LOCAL_NODE_EXECUTOR_MAX_RSS_BYTES
        .checked_mul(slots)
        .ok_or_else(|| anyhow::anyhow!("Required Node executor RSS budget overflow"))?;
    if required > total_rss_budget_bytes {
        anyhow::bail!(ErrorMetadata::bad_request(
            "NodeExecutorPoolBudgetExceeded",
            format!(
                "The deployment requires {slots} Node executor process slots and {required} bytes \
                 of RSS budget, but LOCAL_NODE_EXECUTOR_TOTAL_RSS_BUDGET_BYTES is \
                 {total_rss_budget_bytes}"
            ),
        ));
    }
    Ok(())
}

fn environment_fingerprint(
    environment: &BTreeMap<EnvVarName, EnvVarValue>,
) -> common::sha256::Sha256Digest {
    let mut encoded = Vec::new();
    append_length_prefixed(&mut encoded, ENVIRONMENT_FINGERPRINT_VERSION);
    append_u64(
        &mut encoded,
        u64::try_from(environment.len()).expect("environment entry count does not fit u64"),
    );
    for (name, value) in environment {
        append_length_prefixed(&mut encoded, name.as_ref().as_bytes());
        append_length_prefixed(&mut encoded, value.as_ref().as_bytes());
    }
    Sha256::hash(&encoded)
}

async fn wait_for_router_shutdown(shutting_down: &AtomicBool, shutdown_changed: &Notify) {
    loop {
        let notified = shutdown_changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if shutting_down.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }
}

async fn acquire_runtime_cutover_permit(
    coordinator: &Arc<crate::local::SurgeCoordinator>,
    pool_name: Arc<str>,
    shutting_down: &AtomicBool,
    shutdown_changed: &Notify,
) -> anyhow::Result<crate::local::SurgePermit> {
    let acquire = coordinator.acquire(crate::local::SurgePriority::Deployment, pool_name);
    tokio::pin!(acquire);
    tokio::select! {
        permit = &mut acquire => {
            if shutting_down.load(Ordering::Acquire) {
                permit.release();
                anyhow::bail!(
                    "Local Node executor router shut down while waiting for cutover capacity"
                );
            }
            Ok(permit)
        },
        () = wait_for_router_shutdown(shutting_down, shutdown_changed) => {
            anyhow::bail!("Local Node executor router shut down while waiting for cutover capacity")
        },
    }
}

async fn acquire_runtime_cutover_permit_with_reservation(
    coordinator: &Arc<crate::local::SurgeCoordinator>,
    pool_name: Arc<str>,
    shutting_down: &AtomicBool,
    shutdown_changed: &Notify,
    reservation_receiver: &mut Option<oneshot::Receiver<NodeExecutorCutoverReservation>>,
) -> anyhow::Result<crate::local::SurgePermit> {
    let Some(receiver) = reservation_receiver.take() else {
        return acquire_runtime_cutover_permit(
            coordinator,
            pool_name,
            shutting_down,
            shutdown_changed,
        )
        .await;
    };
    let acquire = coordinator.acquire(crate::local::SurgePriority::Deployment, pool_name);
    tokio::pin!(acquire);
    tokio::select! {
        reservation = receiver => {
            let mut reservation = reservation.map_err(|_| {
                anyhow::anyhow!("Local Node cutover reservation transfer stopped")
            })?;
            let permit = reservation.permit.take().ok_or_else(|| {
                anyhow::anyhow!("Local Node cutover reservation has no surge permit")
            })?;
            if shutting_down.load(Ordering::Acquire) {
                permit.release();
                anyhow::bail!(
                    "Local Node executor router shut down while waiting for cutover capacity"
                );
            }
            Ok(permit)
        },
        permit = &mut acquire => {
            if shutting_down.load(Ordering::Acquire) {
                permit.release();
                anyhow::bail!(
                    "Local Node executor router shut down while waiting for cutover capacity"
                );
            }
            Ok(permit)
        },
        () = wait_for_router_shutdown(shutting_down, shutdown_changed) => {
            anyhow::bail!("Local Node executor router shut down while waiting for cutover capacity")
        },
    }
}

async fn wait_for_removed_pool_cleanup(
    result: &mut watch::Receiver<Option<Result<(), ()>>>,
    shutting_down: &AtomicBool,
    shutdown_changed: &Notify,
) -> anyhow::Result<()> {
    loop {
        if let Some(result) = *result.borrow() {
            return result
                .map_err(|()| anyhow::anyhow!("Removed Local Node executor pool cleanup failed"));
        }
        tokio::select! {
            changed = result.changed() => changed.map_err(|_| {
                anyhow::anyhow!("Removed Local Node executor pool cleanup publication stopped")
            })?,
            () = wait_for_router_shutdown(shutting_down, shutdown_changed) => {
                anyhow::bail!("Local Node executor router shut down during removed-pool cleanup")
            },
        }
    }
}

async fn wait_for_removed_pool_cleanup_with_preemption(
    owner: &mut RemovedPoolOwner,
    permit: &crate::local::SurgePermit,
    memory_pressure: &MemoryPressureSignal,
    shutting_down: &AtomicBool,
    shutdown_changed: &Notify,
) -> anyhow::Result<()> {
    if matches!(*owner.result.borrow_and_update(), Some(Err(()))) {
        // A prior runtime owner published failure while retaining the exact
        // pool and permit. Retry that owner before treating the recovery
        // attempt as terminal.
        owner.shutdown();
        owner.result.changed().await.map_err(|_| {
            anyhow::anyhow!("Removed Local Node executor pool retry publication stopped")
        })?;
        match *owner.result.borrow_and_update() {
            Some(Ok(())) => return Ok(()),
            Some(Err(())) => {
                anyhow::bail!("Removed Local Node executor pool cleanup retry failed")
            },
            None => {},
        }
    }
    tokio::select! {
        result = wait_for_removed_pool_cleanup(
            &mut owner.result,
            shutting_down,
            shutdown_changed,
        ) => result,
        () = permit.wait_until_preempted() => {
            owner.shutdown();
            wait_for_removed_pool_cleanup(
                &mut owner.result,
                shutting_down,
                shutdown_changed,
            ).await
        },
        () = wait_for_memory_pressure(memory_pressure) => {
            owner.shutdown();
            wait_for_removed_pool_cleanup(
                &mut owner.result,
                shutting_down,
                shutdown_changed,
            ).await
        },
    }
}

async fn wait_for_memory_pressure(memory_pressure: &MemoryPressureSignal) {
    wait_for_memory_pressure_receiver(memory_pressure.subscribe()).await;
}

async fn wait_for_memory_pressure_receiver(mut pressure: watch::Receiver<bool>) {
    loop {
        if *pressure.borrow_and_update() {
            return;
        }
        pressure
            .changed()
            .await
            .expect("Local Node memory-pressure publication stopped");
    }
}

async fn acquire_deployment_cutover_permit(
    coordinator: &Arc<crate::local::SurgeCoordinator>,
    timeout: Duration,
    force: bool,
    shutting_down: &AtomicBool,
    shutdown_changed: &Notify,
) -> anyhow::Result<crate::local::SurgePermit> {
    let acquire = async {
        let acquire = coordinator.acquire(
            crate::local::SurgePriority::Deployment,
            Arc::from("deployment"),
        );
        tokio::pin!(acquire);
        if force {
            let mut preemption_interval = tokio::time::interval(FORCED_CUTOVER_PREEMPTION_INTERVAL);
            preemption_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut preemption_requested = false;
            loop {
                // Poll acquisition first so the deployment registers before it
                // signals the current owner. Recheck because an unstealable
                // deployment candidate can later become a reclaimable drain.
                tokio::select! {
                    biased;
                    permit = &mut acquire => return permit,
                    _ = preemption_interval.tick(), if !preemption_requested => {
                        if let Some(phase) = coordinator.force_preempt_reclaimable() {
                            preemption_requested = true;
                            let event = if phase == "draining" {
                                "forced_drain_termination"
                            } else {
                                "forced_candidate_cancel"
                            };
                            crate::metrics::log_local_node_deployment_cutover_event(event);
                            tracing::warn!(
                                lifecycle_context = "deployment_cutover",
                                outcome = event,
                                "Forced local Node executor surge reclamation"
                            );
                        }
                    },
                }
            }
        }
        acquire.await
    };
    tokio::select! {
        result = tokio::time::timeout(timeout, acquire) => match result {
            Ok(permit) => {
                if shutting_down.load(Ordering::Acquire) {
                    permit.release();
                    anyhow::bail!(
                        "Local Node executor router shut down while waiting for deployment cutover capacity"
                    );
                }
                Ok(permit)
            },
            Err(_) => {
                crate::metrics::log_local_node_deployment_cutover_event("timed_out");
                anyhow::bail!(ErrorMetadata::bad_request(
                    "NodeExecutorCutoverCapacityUnavailable",
                    "Deployment was not applied because Node executor cutover capacity remained \
                     occupied by an earlier generation for 120 seconds. Wait for its active actions \
                     to finish, or retry with --force-node-cutover to terminate superseded actions.",
                ));
            },
        },
        () = wait_for_router_shutdown(shutting_down, shutdown_changed) => {
            anyhow::bail!("Local Node executor router shut down while waiting for deployment cutover capacity")
        },
    }
}

fn append_length_prefixed(encoded: &mut Vec<u8>, value: &[u8]) {
    append_u64(
        encoded,
        u64::try_from(value.len()).expect("environment field length does not fit u64"),
    );
    encoded.extend_from_slice(value);
}

fn append_u64(encoded: &mut Vec<u8>, value: u64) {
    encoded.extend_from_slice(&value.to_be_bytes());
}

#[async_trait]
impl NodeExecutor for RoutedLocalNodeExecutor {
    fn enable(&self) -> anyhow::Result<()> {
        let state = self
            .state
            .read()
            .map_err(|_| anyhow::anyhow!("Local Node executor router lock poisoned"))?;
        state.default.enable()?;
        for pool in state.named.values() {
            pool.enable()?;
        }
        Ok(())
    }

    fn validate_pool_topology(&self, topology: &NodeExecutorPoolTopology) -> anyhow::Result<()> {
        validate_pool_topology(topology, self.total_rss_budget_bytes)
    }

    fn reconcile_pool_topology(
        &self,
        topology: &NodeExecutorPoolTopology,
        version: Timestamp,
    ) -> anyhow::Result<()> {
        self.validate_pool_topology(topology)?;
        anyhow::ensure!(
            !self.shutting_down.load(Ordering::Acquire),
            "Local Node executor router is shutting down"
        );

        let mut state = self
            .state
            .write()
            .map_err(|_| anyhow::anyhow!("Local Node executor router lock poisoned"))?;
        // Shutdown can begin after the lock-free fast-path check. Recheck under
        // the publication lock so it cannot miss newly installed pool owners.
        anyhow::ensure!(
            !self.shutting_down.load(Ordering::Acquire),
            "Local Node executor router is shutting down"
        );
        if version <= state.topology_version {
            let outcome = if version == state.topology_version {
                anyhow::ensure!(
                    topology == &state.topology,
                    "Local Node executor topology disagrees at the same publication version"
                );
                TopologyPublicationOutcome::IgnoredDuplicate
            } else {
                TopologyPublicationOutcome::IgnoredStale
            };
            if state.last_reported_ignored_version != Some(version) {
                crate::metrics::log_local_node_topology_publication(outcome.as_str());
                tracing::debug!(
                    topology_version = %version,
                    current_topology_version = %state.topology_version,
                    outcome = outcome.as_str(),
                    "Ignored local Node executor topology publication"
                );
                state.last_reported_ignored_version = Some(version);
            }
            return Ok(());
        }
        match &state.cutover {
            Some(ActiveCutover::UncommittedClaim { claim }) => {
                anyhow::ensure!(
                    topology == &state.topology || topology == &claim.topology,
                    "Topology does not match the current or reserved Local Node executor state"
                );
                // The reservation's commit timestamp is not known yet. Do not
                // advance past it for a concurrent environment edit; request
                // selection republishes that newer snapshot after the claim
                // commits or is canceled.
                return Ok(());
            },
            Some(ActiveCutover::CommittedClaim {
                topology: claimed_topology,
                version: claimed_version,
                ..
            }) => anyhow::ensure!(
                topology == claimed_topology && version >= *claimed_version,
                "Another Local Node executor topology cutover is still active"
            ),
            Some(ActiveCutover::Running {
                version: running_version,
                ..
            }) => {
                anyhow::ensure!(
                    topology == &state.topology && *running_version <= version,
                    "Another Local Node executor topology cutover is still active"
                );
            },
            Some(ActiveCutover::Pending {
                version: pending_version,
            }) => anyhow::ensure!(
                *pending_version <= version,
                "Local Node executor topology publication is stale"
            ),
            None => {},
        }
        if topology == &state.topology {
            // Execution reconciles from every request snapshot. Advance the
            // ordering watermark without reporting a topology transition.
            // Retain runtime ownership; request selection may advance an
            // unstarted claim to the newer durable snapshot before it starts.
            state.topology_version = version;
            state.last_reported_ignored_version = None;
            state.publication = state
                .publication
                .checked_add(1)
                .expect("Node executor topology publication count overflow");
            self.topology_updates.send_replace(state.publication);
            return Ok(());
        }
        match &state.cutover {
            Some(ActiveCutover::CommittedClaim { .. } | ActiveCutover::Running { .. }) => {},
            Some(ActiveCutover::Pending { .. }) | None => {
                // Execution-side reconciliation can publish the durable topology
                // after the deployment caller is canceled. Install a recovery
                // owner under the same lock so new-topology requests cannot race
                // package preparation in the old resident generation.
                state.cutover = Some(ActiveCutover::Pending { version });
            },
            Some(ActiveCutover::UncommittedClaim { .. }) => {
                anyhow::bail!("A Local Node executor deployment reservation is still uncommitted")
            },
        }
        let mut retiring = std::mem::take(&mut state.retiring);
        retiring.retain(|owner| !matches!(&*owner.result.borrow(), Some(Ok(()))));
        let mut changed_modules: BTreeSet<_> = state
            .topology
            .keys()
            .chain(topology.keys())
            .filter(|module| state.topology.get(*module) != topology.get(*module))
            .cloned()
            .collect();
        let exact_default_membership =
            match (state.topology.default_routes(), topology.default_routes()) {
                (Some(previous), Some(next)) => {
                    changed_modules.extend(previous.symmetric_difference(next).cloned());
                    Some(previous != next)
                },
                _ => None,
            };
        let changed_route_count = changed_modules.len();
        // Equal counts cannot prove equal membership when a deployment upgrades
        // a legacy count-only record to exact default routes. Report the default
        // route as affected at that precision boundary.
        let default_membership_precision_changed =
            state.topology.default_routes().is_some() != topology.default_routes().is_some();
        let mut default_affected = default_membership_precision_changed
            || exact_default_membership.unwrap_or_else(|| {
                state.topology.default_route_count() != topology.default_route_count()
            });
        let mut affected_names = BTreeSet::new();
        for module in changed_modules {
            for assignment in [state.topology.get(&module), topology.get(&module)] {
                match assignment {
                    Some(name) => {
                        affected_names.insert(name.clone());
                    },
                    None if exact_default_membership.is_none() => default_affected = true,
                    None => {},
                }
            }
        }
        let configured_names: BTreeSet<_> = topology.values().cloned().collect();
        let memory_pressure = self.memory_pressure.clone();
        state.named.retain(|name, pool| {
            if configured_names.contains(name) {
                true
            } else {
                retiring.push(pool.retire_for_removal(memory_pressure.clone()));
                crate::metrics::clear_local_node_pool_configuration(name.as_ref());
                false
            }
        });
        for name in &configured_names {
            if !state.named.contains_key(name) {
                state.named.insert(
                    name.clone(),
                    Arc::new(RoutedPool::new(
                        Arc::from(name.as_ref()),
                        version,
                        &self.local_config,
                    )),
                );
            }
            let module_count = topology.values().filter(|pool| *pool == name).count();
            crate::metrics::set_local_node_pool_configuration(name.as_ref(), Some(module_count));
        }
        crate::metrics::set_local_node_pool_configuration(
            "default",
            topology.default_route_count(),
        );
        crate::metrics::set_local_node_configured_named_pools(configured_names.len());
        let configured_named_pool_count = configured_names.len();
        let affected_named_pool_count = affected_names.len();
        state.retiring = retiring;
        state.topology = topology.clone();
        state.topology_version = version;
        state.last_reported_ignored_version = None;
        state.publication = state
            .publication
            .checked_add(1)
            .expect("Node executor topology publication count overflow");
        self.topology_updates.send_replace(state.publication);
        crate::metrics::log_local_node_topology_publication(
            TopologyPublicationOutcome::Applied.as_str(),
        );
        tracing::info!(
            topology_version = %version,
            outcome = TopologyPublicationOutcome::Applied.as_str(),
            configured_named_pool_count,
            changed_route_count,
            affected_named_pool_count,
            default_affected,
            cutover_phase = "queued",
            "Applied local Node executor topology publication"
        );
        Ok(())
    }

    fn begin_pool_cutover(
        &self,
        topology: &NodeExecutorPoolTopology,
        version: Timestamp,
        reservation: &mut Option<NodeExecutorCutoverReservation>,
    ) -> anyhow::Result<()> {
        let removed_pool_permit = reservation
            .as_ref()
            .and_then(|reservation| reservation.permit.as_ref())
            .cloned();
        match reservation {
            Some(reservation) => reservation.commit_claim(topology, version)?,
            None => self.install_committed_cutover(topology, version)?,
        }
        if let Err(error) = self.reconcile_pool_topology(topology, version) {
            if let Ok(mut state) = self.state.write()
                && matches!(
                    &state.cutover,
                    Some(ActiveCutover::CommittedClaim {
                        version: claimed_version,
                        ..
                    }) if *claimed_version == version
                )
            {
                state.cutover = Some(ActiveCutover::Pending { version });
            }
            return Err(error);
        }
        if let Some(permit) = removed_pool_permit {
            let removed_pool_owners = self
                .state
                .read()
                .map_err(|_| anyhow::anyhow!("Local Node executor router lock poisoned"))?
                .retiring
                .clone();
            for owner in removed_pool_owners {
                owner.retain_cleanup_permit(permit.clone());
            }
        }
        Ok(())
    }

    async fn reserve_pool_cutover(
        &self,
        topology: &NodeExecutorPoolTopology,
        force: bool,
    ) -> anyhow::Result<Option<NodeExecutorCutoverReservation>> {
        self.validate_pool_topology(topology)?;
        anyhow::ensure!(
            !self.shutting_down.load(Ordering::Acquire),
            "Local Node executor router is shutting down"
        );
        // Install the claim before waiting whenever there is only recoverable
        // pending state. This prevents execution from starting that older
        // target while the newer deployment waits for its pre-commit permit.
        let mut claim_guard = self.try_claim_deployment_cutover(topology)?;
        let coordinator = self.local_config.surge_coordinator();
        let permit = acquire_deployment_cutover_permit(
            &coordinator,
            DEPLOYMENT_CUTOVER_ADMISSION_TIMEOUT,
            force,
            &self.shutting_down,
            &self.shutdown_changed,
        )
        .await?;
        if claim_guard.is_none() {
            claim_guard = self.try_claim_deployment_cutover(topology)?;
        }
        let Some(claim_guard) = claim_guard else {
            permit.release();
            anyhow::bail!(ErrorMetadata::overloaded(
                "NodeExecutorCutoverRecoveryPending",
                "A committed Node executor cutover is still recovering. Retry the deployment \
                 after it completes.",
            ));
        };
        if !claim_guard.owns_active_claim()? {
            permit.release();
            anyhow::bail!(ErrorMetadata::overloaded(
                "NodeExecutorCutoverRecoveryPending",
                "A committed Node executor cutover changed during deployment admission. Retry the \
                 deployment after it completes.",
            ));
        }
        // Reserve a cold router too. The permit and the router-visible claim
        // jointly fence late residents and runtime cutover ownership through
        // the deployment commit.
        permit.set_phase("reservation");
        crate::metrics::log_local_node_deployment_cutover_event("admitted");
        Ok(Some(NodeExecutorCutoverReservation::with_claim(
            permit,
            claim_guard,
        )))
    }

    async fn complete_pool_cutover(
        &self,
        target: NodeExecutorCutoverTarget,
        version: Timestamp,
        reservation: Option<NodeExecutorCutoverReservation>,
    ) -> anyhow::Result<()> {
        let result = self.start_runtime_cutover(target, version, reservation)?;
        Self::wait_for_runtime_cutover(result).await
    }

    async fn invoke(
        &self,
        request: ExecutorRequest,
        log_line_sender: mpsc::UnboundedSender<LogLine>,
        function_execution_start: Option<FunctionExecutionStartGate>,
    ) -> anyhow::Result<InvokeResponse> {
        anyhow::ensure!(
            !self.shutting_down.load(Ordering::Acquire),
            "Local Node executor router is shutting down"
        );
        let (executor, fingerprint, request_kind) = self.selected_pool(&request).await?;
        crate::metrics::log_local_node_route_request(executor.pool_name(), request_kind);
        executor
            .invoke_with_fingerprint(
                request,
                log_line_sender,
                fingerprint,
                function_execution_start,
            )
            .await
    }

    fn shutdown(&self) {
        if self.shutting_down.swap(true, Ordering::AcqRel) {
            return;
        }
        self.shutdown_changed.notify_waiters();
        self.topology_updates.send_replace(u64::MAX);
        crate::metrics::set_local_node_configured_named_pools(0);
        match self.state.read() {
            Ok(state) => {
                crate::metrics::clear_local_node_pool_configuration("default");
                state.default.shutdown();
                for (name, pool) in &state.named {
                    crate::metrics::clear_local_node_pool_configuration(name.as_ref());
                    pool.shutdown();
                }
                for owner in &state.retiring {
                    owner.shutdown();
                }
            },
            Err(_) => tracing::error!("Local Node executor router lock poisoned during shutdown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        time::Instant,
    };

    use errors::ErrorMetadataAnyhowExt;
    use maplit::btreemap;
    use sync_types::CanonicalizedModulePath;

    use super::*;

    fn topology(assignments: &[(&str, &str)]) -> NodeExecutorPoolTopology {
        NodeExecutorPoolTopology::new(
            assignments
                .iter()
                .map(|(path, pool)| {
                    (
                        CanonicalizedModulePath::from_str(path).unwrap(),
                        NodeExecutorPoolName::from_str(pool).unwrap(),
                    )
                })
                .collect(),
            Some(0),
        )
    }

    fn cutover_target(topology: NodeExecutorPoolTopology) -> NodeExecutorCutoverTarget {
        NodeExecutorCutoverTarget {
            topology,
            source_package: crate::executor::SourcePackage {
                bundled_source: crate::executor::Package {
                    uri: "https://packages.invalid/source.zip".to_owned(),
                    key: common::types::ObjectKey::try_from("source-package").unwrap(),
                    sha256: Sha256::hash(b"source"),
                },
                external_deps: None,
                download_url_expiration: Instant::now() + Duration::from_secs(60),
            },
            source_package_id: value::DeveloperDocumentId::MIN.into(),
            environment_variables: BTreeMap::new(),
        }
    }

    fn cutover_target_identity(topology: NodeExecutorPoolTopology) -> RuntimeCutoverTargetIdentity {
        RuntimeCutoverTargetIdentity::new(&cutover_target(topology))
    }

    async fn test_executor() -> RoutedLocalNodeExecutor {
        let memory_pressure = MemoryPressureSignal::default();
        let local = LocalNodeExecutor::preflight_configuration(
            Duration::from_secs(1),
            memory_pressure.clone(),
        )
        .unwrap();
        RoutedLocalNodeExecutor::new_with_configuration(RoutedLocalNodeExecutorConfig {
            local,
            total_rss_budget_bytes: *LOCAL_NODE_EXECUTOR_MAX_RSS_BYTES * 10,
            memory_pressure,
        })
        .await
        .unwrap()
    }

    #[test]
    fn validates_pool_count_and_full_surge_budget() {
        let two_pools = topology(&[("a.js", "one"), ("b.js", "two")]);
        let per_slot = *LOCAL_NODE_EXECUTOR_MAX_RSS_BYTES;
        validate_pool_topology(&two_pools, per_slot * 4).unwrap();
        assert!(validate_pool_topology(&two_pools, per_slot * 4 - 1).is_err());

        let too_many = NodeExecutorPoolTopology::new(
            (0..=MAX_NAMED_POOLS)
                .map(|index| {
                    (
                        format!("module_{index}.js").parse().unwrap(),
                        format!("pool_{index}").parse().unwrap(),
                    )
                })
                .collect(),
            Some(0),
        );
        assert!(validate_pool_topology(&too_many, usize::MAX).is_err());

        let invalid_default = NodeExecutorPoolTopology::new_complete(
            BTreeMap::new(),
            ["http.js".parse().unwrap()].into_iter().collect(),
        );
        assert!(validate_pool_topology(&invalid_default, per_slot * 2).is_err());
    }

    #[test]
    fn environment_fingerprint_is_ordered_and_length_delimited() {
        let first: BTreeMap<EnvVarName, EnvVarValue> = btreemap! {
            "A".parse().unwrap() => "BC".parse().unwrap(),
            "AB".parse().unwrap() => "C".parse().unwrap(),
        };
        let reordered = first
            .iter()
            .rev()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(
            environment_fingerprint(&first),
            environment_fingerprint(&reordered)
        );

        let ambiguous_without_lengths = btreemap! {
            "A".parse().unwrap() => "B".parse().unwrap(),
            "C".parse().unwrap() => "".parse().unwrap(),
        };
        assert_ne!(
            environment_fingerprint(&first),
            environment_fingerprint(&ambiguous_without_lengths)
        );
    }

    #[tokio::test]
    async fn unchanged_topology_only_advances_the_watermark() {
        let executor = test_executor().await;
        let topology = NodeExecutorPoolTopology::default();
        executor
            .reconcile_pool_topology(&topology, Timestamp::try_from(1_u64).unwrap())
            .unwrap();

        let state = executor.state.read().unwrap();
        assert_eq!(state.topology_version, Timestamp::try_from(1_u64).unwrap());
        assert!(state.cutover.is_none());
        assert_eq!(state.publication, 1);
    }

    #[tokio::test]
    async fn newer_unchanged_topology_retains_unstarted_cutover() {
        let executor = test_executor().await;
        let topology = NodeExecutorPoolTopology::default();
        executor.state.write().unwrap().cutover = Some(ActiveCutover::Pending {
            version: Timestamp::try_from(1_u64).unwrap(),
        });

        executor
            .reconcile_pool_topology(&topology, Timestamp::try_from(2_u64).unwrap())
            .unwrap();

        let state = executor.state.read().unwrap();
        assert_eq!(state.topology_version, Timestamp::try_from(2_u64).unwrap());
        assert!(matches!(
            &state.cutover,
            Some(ActiveCutover::Pending {
                version: pending_version
            }) if *pending_version == Timestamp::try_from(1_u64).unwrap()
        ));
    }

    #[tokio::test]
    async fn newer_request_claim_supersedes_unstarted_cutover() {
        let executor = test_executor().await;
        let topology = NodeExecutorPoolTopology::default();
        executor.state.write().unwrap().cutover = Some(ActiveCutover::Pending {
            version: Timestamp::try_from(1_u64).unwrap(),
        });
        executor
            .reconcile_pool_topology(&topology, Timestamp::try_from(2_u64).unwrap())
            .unwrap();

        executor
            .install_committed_cutover(&topology, Timestamp::try_from(2_u64).unwrap())
            .unwrap();

        assert!(matches!(
            &executor.state.read().unwrap().cutover,
            Some(ActiveCutover::Pending {
                version: pending_version
            }) if *pending_version == Timestamp::try_from(2_u64).unwrap()
        ));
    }

    #[tokio::test]
    async fn newer_unchanged_topology_retains_running_cutover() {
        let executor = test_executor().await;
        let topology = NodeExecutorPoolTopology::default();
        let (_result_tx, result_rx) = watch::channel(None);
        executor.state.write().unwrap().cutover = Some(ActiveCutover::Running {
            version: Timestamp::try_from(1_u64).unwrap(),
            target: cutover_target_identity(topology.clone()),
            ownership: Arc::new(RuntimeCutoverOwnership),
            result: result_rx,
            reservation_transfer: None,
        });

        executor
            .reconcile_pool_topology(&topology, Timestamp::try_from(2_u64).unwrap())
            .unwrap();

        let state = executor.state.read().unwrap();
        assert_eq!(state.topology_version, Timestamp::try_from(2_u64).unwrap());
        assert!(matches!(
            &state.cutover,
            Some(ActiveCutover::Running {
                version: running_version,
                ..
            }) if *running_version == Timestamp::try_from(1_u64).unwrap()
        ));
    }

    #[tokio::test]
    async fn changed_topology_publication_installs_recovery_ownership_atomically() {
        let executor = test_executor().await;
        let target = topology(&[("a.js", "one")]);
        let version = Timestamp::try_from(2_u64).unwrap();

        executor.reconcile_pool_topology(&target, version).unwrap();

        let state = executor.state.read().unwrap();
        assert_eq!(state.topology, target);
        assert_eq!(state.topology_version, version);
        assert!(matches!(
            &state.cutover,
            Some(ActiveCutover::Pending {
                version: pending_version
            }) if *pending_version == version
        ));
    }

    #[tokio::test]
    async fn deployment_claim_precedes_topology_publication() {
        let executor = test_executor().await;
        let target = topology(&[("a.js", "one")]);
        let version = Timestamp::try_from(3_u64).unwrap();

        let mut claim = executor
            .try_claim_deployment_cutover(&target)
            .unwrap()
            .expect("Deployment did not install its pre-commit claim");
        claim.commit(&target, version).unwrap();
        {
            let state = executor.state.read().unwrap();
            assert_ne!(state.topology, target);
            assert!(matches!(
                &state.cutover,
                Some(ActiveCutover::CommittedClaim {
                    version: claimed_version,
                    ..
                }) if *claimed_version == version
            ));
        }

        executor.reconcile_pool_topology(&target, version).unwrap();
        let state = executor.state.read().unwrap();
        assert_eq!(state.topology, target);
        assert!(matches!(
            &state.cutover,
            Some(ActiveCutover::CommittedClaim {
                version: claimed_version,
                ..
            }) if *claimed_version == version
        ));
        drop(state);
    }

    #[tokio::test]
    async fn newer_environment_snapshot_waits_for_uncommitted_claim_then_advances() {
        let executor = Arc::new(test_executor().await);
        let target = NodeExecutorPoolTopology::new_complete(
            BTreeMap::new(),
            ["a.js".parse().unwrap()].into_iter().collect(),
        );
        let commit_version = Timestamp::try_from(1_u64).unwrap();
        let environment_version = Timestamp::try_from(2_u64).unwrap();
        let mut claim = executor
            .try_claim_deployment_cutover(&target)
            .unwrap()
            .expect("Deployment did not install its pre-commit claim");

        // A request may observe a UI environment edit committed immediately
        // after the deployment but before the caller publishes its commit.
        executor
            .reconcile_pool_topology(&target, environment_version)
            .unwrap();
        assert_eq!(
            executor.state.read().unwrap().topology_version,
            Timestamp::MIN
        );
        let waiting_executor = executor.clone();
        let waiting_target = target.clone();
        let waiting = tokio::spawn(async move {
            waiting_executor
                .execute_pool(
                    &"a.js".parse().unwrap(),
                    None,
                    &waiting_target,
                    environment_version,
                )
                .await
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        claim.commit(&target, commit_version).unwrap();
        executor
            .reconcile_pool_topology(&target, commit_version)
            .unwrap();
        drop(claim);
        waiting.await.unwrap().unwrap();
        assert_eq!(
            executor.state.read().unwrap().topology_version,
            environment_version
        );
    }

    #[tokio::test]
    async fn newer_topology_supersedes_an_unstarted_recovery_cutover() {
        let executor = test_executor().await;
        let first = topology(&[("a.js", "one")]);
        let second = topology(&[("b.js", "two")]);
        executor
            .reconcile_pool_topology(&first, Timestamp::try_from(1_u64).unwrap())
            .unwrap();
        executor
            .reconcile_pool_topology(&second, Timestamp::try_from(2_u64).unwrap())
            .unwrap();

        let state = executor.state.read().unwrap();
        assert_eq!(state.topology, second);
        assert!(matches!(
            &state.cutover,
            Some(ActiveCutover::Pending {
                version: pending_version
            }) if *pending_version == Timestamp::try_from(2_u64).unwrap()
        ));
    }

    #[tokio::test]
    async fn topology_publication_is_version_ordered() {
        let executor = test_executor().await;
        let first = topology(&[("a.js", "one")]);
        let version = Timestamp::try_from(2_u64).unwrap();
        executor.reconcile_pool_topology(&first, version).unwrap();
        executor.reconcile_pool_topology(&first, version).unwrap();
        assert!(executor
            .reconcile_pool_topology(&topology(&[("different.js", "one")]), version,)
            .is_err());
        executor
            .reconcile_pool_topology(
                &topology(&[("stale.js", "stale")]),
                Timestamp::try_from(1_u64).unwrap(),
            )
            .unwrap();

        let state = executor.state.read().unwrap();
        assert_eq!(state.topology, first);
        assert_eq!(state.topology_version, version);
    }

    #[tokio::test]
    async fn matching_older_topology_selects_the_same_pool_after_watermark_advance() {
        let executor = test_executor().await;
        let current = topology(&[("a.js", "one")]);
        executor
            .reconcile_pool_topology(&current, Timestamp::try_from(1_u64).unwrap())
            .unwrap();
        executor.state.write().unwrap().cutover = None;
        executor
            .reconcile_pool_topology(&current, Timestamp::try_from(2_u64).unwrap())
            .unwrap();

        let selected = executor
            .execute_pool(
                &"a.js".parse().unwrap(),
                Some(&"one".parse().unwrap()),
                &current,
                Timestamp::try_from(1_u64).unwrap(),
            )
            .await
            .unwrap();
        assert!(Arc::ptr_eq(
            &selected,
            executor
                .state
                .read()
                .unwrap()
                .named
                .get(&"one".parse().unwrap())
                .unwrap()
        ));
    }

    #[tokio::test]
    async fn stale_snapshot_does_not_initialize_an_empty_pool() {
        let executor = test_executor().await;
        let current = topology(&[("a.js", "one")]);
        executor
            .reconcile_pool_topology(&current, Timestamp::try_from(1_u64).unwrap())
            .unwrap();
        let pool = executor
            .state
            .read()
            .unwrap()
            .named
            .get(&"one".parse().unwrap())
            .unwrap()
            .clone();
        let fingerprint = ResidentGenerationFingerprint {
            source_package_id: value::DeveloperDocumentId::MIN.into(),
            environment_sha256: Sha256::hash(b"environment"),
            topology_version: Timestamp::try_from(1_u64).unwrap(),
        };

        assert!(
            RoutedLocalNodeExecutor::ensure_stale_execution_resident(&pool, &fingerprint)
                .await
                .is_err()
        );
        assert!(pool.executor.get().is_none());
    }

    #[tokio::test]
    async fn stale_snapshot_cannot_enter_a_reintroduced_pool_name() {
        let executor = test_executor().await;
        let configured = topology(&[("a.js", "one")]);
        executor
            .reconcile_pool_topology(&configured, Timestamp::try_from(1_u64).unwrap())
            .unwrap();
        executor
            .reconcile_pool_topology(&topology(&[]), Timestamp::try_from(2_u64).unwrap())
            .unwrap();
        executor
            .reconcile_pool_topology(&configured, Timestamp::try_from(3_u64).unwrap())
            .unwrap();

        assert!(executor
            .execute_pool(
                &"a.js".parse().unwrap(),
                Some(&"one".parse().unwrap()),
                &configured,
                Timestamp::try_from(1_u64).unwrap(),
            )
            .await
            .is_err());
        executor
            .execute_pool(
                &"a.js".parse().unwrap(),
                Some(&"one".parse().unwrap()),
                &configured,
                Timestamp::try_from(3_u64).unwrap(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn exact_default_topology_rejects_an_unassigned_module() {
        let executor = test_executor().await;
        let current = NodeExecutorPoolTopology::new_complete(
            BTreeMap::new(),
            ["ordinary.js".parse().unwrap()].into_iter().collect(),
        );
        executor
            .reconcile_pool_topology(&current, Timestamp::try_from(1_u64).unwrap())
            .unwrap();
        executor.state.write().unwrap().cutover = None;

        executor
            .execute_pool(
                &"ordinary.js".parse().unwrap(),
                None,
                &current,
                Timestamp::try_from(1_u64).unwrap(),
            )
            .await
            .unwrap();
        assert!(executor
            .execute_pool(
                &"other.js".parse().unwrap(),
                None,
                &current,
                Timestamp::try_from(1_u64).unwrap(),
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn runtime_cutover_result_wakes_joiners() {
        let (result_tx, result_rx) = watch::channel(None);
        let waiter = tokio::spawn(RoutedLocalNodeExecutor::wait_for_runtime_cutover(result_rx));
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        result_tx.send_replace(Some(Ok(())));
        waiter.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn abandoned_runtime_cutover_restores_recovery_ownership() {
        let executor = test_executor().await;
        let version = Timestamp::try_from(1_u64).unwrap();
        let ownership = Arc::new(RuntimeCutoverOwnership);
        let (_result_tx, result) = watch::channel(None);
        executor.state.write().unwrap().cutover = Some(ActiveCutover::Running {
            version,
            target: cutover_target_identity(NodeExecutorPoolTopology::default()),
            ownership: ownership.clone(),
            result,
            reservation_transfer: None,
        });

        drop(RuntimeCutoverTaskGuard::new(
            version,
            ownership,
            executor.state.clone(),
        ));

        assert!(matches!(
            &executor.state.read().unwrap().cutover,
            Some(ActiveCutover::Pending {
                version: pending_version
            }) if *pending_version == version
        ));
    }

    #[tokio::test]
    async fn abandoned_runtime_cutover_restores_pending_before_releasing_its_session() {
        let executor = test_executor().await;
        let coordinator = executor.local_config.surge_coordinator();
        let permit = coordinator
            .acquire(
                crate::local::SurgePriority::Deployment,
                Arc::from("deployment"),
            )
            .await;
        let version = Timestamp::try_from(1_u64).unwrap();
        let ownership = Arc::new(RuntimeCutoverOwnership);
        let (_result_tx, result) = watch::channel(None);
        executor.state.write().unwrap().cutover = Some(ActiveCutover::Running {
            version,
            target: cutover_target_identity(NodeExecutorPoolTopology::default()),
            ownership: ownership.clone(),
            result,
            reservation_transfer: None,
        });
        let mut guard = RuntimeCutoverTaskGuard::new(version, ownership, executor.state.clone());
        guard.retain_session_permit(&permit);
        permit.release();

        let waiting_coordinator = coordinator.clone();
        let later = tokio::spawn(async move {
            waiting_coordinator
                .acquire(crate::local::SurgePriority::Deployment, Arc::from("later"))
                .await
        });
        tokio::task::yield_now().await;
        assert!(!later.is_finished());

        drop(guard);
        let later_permit = later.await.unwrap();
        assert!(matches!(
            &executor.state.read().unwrap().cutover,
            Some(ActiveCutover::Pending {
                version: pending_version
            }) if *pending_version == version
        ));
        later_permit.release();
    }

    #[tokio::test]
    async fn failed_runtime_cutover_does_not_replace_recovery_for_the_same_version() {
        let executor = test_executor().await;
        let version = Timestamp::try_from(1_u64).unwrap();
        let failed_ownership = Arc::new(RuntimeCutoverOwnership);
        let replacement_ownership = Arc::new(RuntimeCutoverOwnership);
        let (_result_tx, result) = watch::channel(None);
        executor.state.write().unwrap().cutover = Some(ActiveCutover::Running {
            version,
            target: cutover_target_identity(NodeExecutorPoolTopology::default()),
            ownership: replacement_ownership.clone(),
            result,
            reservation_transfer: None,
        });

        drop(RuntimeCutoverTaskGuard::new(
            version,
            failed_ownership,
            executor.state.clone(),
        ));

        assert!(matches!(
            &executor.state.read().unwrap().cutover,
            Some(ActiveCutover::Running { ownership, .. })
                if Arc::ptr_eq(ownership, &replacement_ownership)
        ));
    }

    #[tokio::test]
    async fn runtime_cutover_join_requires_the_same_effective_environment_target() {
        let executor = test_executor().await;
        let version = Timestamp::try_from(1_u64).unwrap();
        let topology = NodeExecutorPoolTopology::default();
        let first_target = cutover_target(topology.clone());
        let (_result_tx, result) = watch::channel(None);
        executor.state.write().unwrap().cutover = Some(ActiveCutover::Running {
            version,
            target: RuntimeCutoverTargetIdentity::new(&first_target),
            ownership: Arc::new(RuntimeCutoverOwnership),
            result,
            reservation_transfer: None,
        });
        let mut different_target = cutover_target(topology);
        different_target
            .environment_variables
            .insert("TARGET".parse().unwrap(), "new-value".parse().unwrap());

        assert!(executor
            .start_runtime_cutover_inner(different_target, version, None)
            .is_err());
    }

    #[tokio::test]
    async fn execution_started_cutover_takes_the_precommit_reservation_directly() {
        let executor = test_executor().await;
        let coordinator = executor.local_config.surge_coordinator();
        let reservation = NodeExecutorCutoverReservation::new(
            coordinator
                .acquire(
                    crate::local::SurgePriority::Deployment,
                    Arc::from("deployment"),
                )
                .await,
        );
        let (cleanup_tx, cleanup_result) = watch::channel(None);
        executor
            .state
            .write()
            .unwrap()
            .retiring
            .push(RemovedPoolOwner {
                retiring_pool: Arc::new(StdMutex::new(None)),
                result: cleanup_result,
                result_sender: cleanup_tx.clone(),
                cleanup_lock: Arc::new(Mutex::new(())),
                cleanup_permit: Arc::new(StdMutex::new(RemovedPoolCleanupLease::Unconfirmed {
                    permit: None,
                })),
            });
        let version = Timestamp::try_from(1_u64).unwrap();
        let topology = NodeExecutorPoolTopology::default();
        executor
            .install_committed_cutover(&topology, version)
            .unwrap();
        let target = cutover_target(topology);

        executor
            .start_runtime_cutover(target.clone(), version, None)
            .unwrap();
        let waiting_coordinator = coordinator.clone();
        let later_deployment = tokio::spawn(async move {
            waiting_coordinator
                .acquire(crate::local::SurgePriority::Deployment, Arc::from("later"))
                .await
        });
        let joined = executor
            .start_runtime_cutover(target, version, Some(reservation))
            .unwrap();

        tokio::task::yield_now().await;
        assert!(!later_deployment.is_finished());
        cleanup_tx.send_replace(Some(Ok(())));
        RoutedLocalNodeExecutor::wait_for_runtime_cutover(joined)
            .await
            .unwrap();
        later_deployment.await.unwrap().release();
    }

    #[tokio::test]
    async fn execution_started_removed_cleanup_retains_recoverable_session_ownership() {
        let executor = test_executor().await;
        let pool = Arc::new(RoutedPool::new(
            Arc::from("removed"),
            Timestamp::MIN,
            &executor.local_config,
        ));
        let initialization_guard = pool.initialization_lock.lock().await;
        let owner = pool.retire_for_removal(executor.memory_pressure.clone());
        let version = Timestamp::try_from(1_u64).unwrap();
        {
            let mut state = executor.state.write().unwrap();
            state.retiring.push(owner.clone());
            state.cutover = Some(ActiveCutover::Pending { version });
        }

        let result = executor
            .start_runtime_cutover(
                cutover_target(NodeExecutorPoolTopology::default()),
                version,
                None,
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if matches!(
                    &*owner.cleanup_permit.lock().unwrap(),
                    RemovedPoolCleanupLease::Unconfirmed { permit: Some(_) }
                ) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        // End the runtime waiter while the exact pool is still unresolved.
        // Its retained owner must be able to publish later confirmed cleanup
        // and release the otherwise fail-closed coordinator.
        executor.shutting_down.store(true, Ordering::Release);
        executor.shutdown_changed.notify_waiters();
        assert!(RoutedLocalNodeExecutor::wait_for_runtime_cutover(result)
            .await
            .is_err());
        let coordinator = executor.local_config.surge_coordinator();
        let waiting_coordinator = coordinator.clone();
        let later = tokio::spawn(async move {
            waiting_coordinator
                .acquire(crate::local::SurgePriority::Routine, Arc::from("later"))
                .await
        });
        tokio::task::yield_now().await;
        assert!(!later.is_finished());

        drop(initialization_guard);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !matches!(&*owner.result.borrow(), Some(Ok(()))) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), later)
            .await
            .unwrap()
            .unwrap()
            .release();
    }

    #[tokio::test]
    async fn failed_reservation_transfer_replaces_the_abandoned_runtime_owner() {
        let executor = test_executor().await;
        let coordinator = executor.local_config.surge_coordinator();
        let reservation = NodeExecutorCutoverReservation::new(
            coordinator
                .acquire(
                    crate::local::SurgePriority::Deployment,
                    Arc::from("deployment"),
                )
                .await,
        );
        let (cleanup_tx, cleanup_result) = watch::channel(None);
        let (reservation_transfer, reservation_receiver) = RuntimeCutoverReservationTransfer::new();
        drop(reservation_receiver);
        let version = Timestamp::try_from(1_u64).unwrap();
        let abandoned_ownership = Arc::new(RuntimeCutoverOwnership);
        let (_abandoned_result_tx, abandoned_result) = watch::channel(None);
        {
            let mut state = executor.state.write().unwrap();
            state.retiring.push(RemovedPoolOwner {
                retiring_pool: Arc::new(StdMutex::new(None)),
                result: cleanup_result,
                result_sender: cleanup_tx.clone(),
                cleanup_lock: Arc::new(Mutex::new(())),
                cleanup_permit: Arc::new(StdMutex::new(RemovedPoolCleanupLease::Unconfirmed {
                    permit: None,
                })),
            });
            state.cutover = Some(ActiveCutover::Running {
                version,
                target: cutover_target_identity(NodeExecutorPoolTopology::default()),
                ownership: abandoned_ownership.clone(),
                result: abandoned_result,
                reservation_transfer: Some(reservation_transfer),
            });
        }

        let joined = executor
            .start_runtime_cutover(
                cutover_target(NodeExecutorPoolTopology::default()),
                version,
                Some(reservation),
            )
            .unwrap();

        assert!(matches!(
            &executor.state.read().unwrap().cutover,
            Some(ActiveCutover::Running { ownership, .. })
                if !Arc::ptr_eq(ownership, &abandoned_ownership)
        ));
        let waiting_coordinator = coordinator.clone();
        let later_deployment = tokio::spawn(async move {
            waiting_coordinator
                .acquire(crate::local::SurgePriority::Deployment, Arc::from("later"))
                .await
        });
        tokio::task::yield_now().await;
        assert!(!later_deployment.is_finished());

        cleanup_tx.send_replace(Some(Ok(())));
        RoutedLocalNodeExecutor::wait_for_runtime_cutover(joined)
            .await
            .unwrap();
        later_deployment.await.unwrap().release();
    }

    #[tokio::test]
    async fn deployment_admission_timeout_is_typed_and_preserves_ownership() {
        let coordinator = crate::local::SurgeCoordinator::new();
        let held = coordinator
            .acquire(crate::local::SurgePriority::Routine, Arc::from("routine"))
            .await;

        let shutting_down = AtomicBool::new(false);
        let shutdown_changed = Notify::new();
        let error = match acquire_deployment_cutover_permit(
            &coordinator,
            Duration::from_millis(1),
            false,
            &shutting_down,
            &shutdown_changed,
        )
        .await
        {
            Ok(_) => panic!("Occupied deployment cutover capacity was admitted"),
            Err(error) => error,
        };
        assert_eq!(error.short_msg(), "NodeExecutorCutoverCapacityUnavailable");
        assert!(coordinator.force_preempt_reclaimable().is_some());
        held.release();
    }

    #[tokio::test]
    async fn cold_router_reserves_deployment_capacity() {
        let executor = test_executor().await;
        let coordinator = executor.local_config.surge_coordinator();
        let reservation = executor
            .reserve_pool_cutover(&NodeExecutorPoolTopology::default(), false)
            .await
            .unwrap()
            .expect("Cold Local Node router did not reserve cutover capacity");
        let waiting_coordinator = coordinator.clone();
        let routine = tokio::spawn(async move {
            waiting_coordinator
                .acquire(crate::local::SurgePriority::Routine, Arc::from("routine"))
                .await
        });
        tokio::task::yield_now().await;
        assert!(!routine.is_finished());

        drop(reservation);
        routine.await.unwrap().release();
    }

    #[tokio::test]
    async fn deployment_admission_rechecks_recovery_after_capacity_wait() {
        let executor = Arc::new(test_executor().await);
        let coordinator = executor.local_config.surge_coordinator();
        let held = coordinator
            .acquire(crate::local::SurgePriority::Routine, Arc::from("held"))
            .await;
        let waiting_executor = executor.clone();
        let admission = tokio::spawn(async move {
            waiting_executor
                .reserve_pool_cutover(&NodeExecutorPoolTopology::default(), true)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !held.preempted() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        executor.state.write().unwrap().cutover = Some(ActiveCutover::Pending {
            version: Timestamp::try_from(1_u64).unwrap(),
        });
        held.release();
        let error = match admission.await.unwrap() {
            Ok(_) => panic!("Deployment admission ignored cutover recovery after capacity wait"),
            Err(error) => error,
        };
        assert_eq!(error.short_msg(), "NodeExecutorCutoverRecoveryPending");

        tokio::time::timeout(
            Duration::from_secs(1),
            coordinator.acquire(crate::local::SurgePriority::Routine, Arc::from("routine")),
        )
        .await
        .unwrap()
        .release();
    }

    #[tokio::test]
    async fn unresolved_removed_pool_requires_deployment_admission() {
        let executor = test_executor().await;
        let (result_tx, result) = watch::channel(None);
        executor
            .state
            .write()
            .unwrap()
            .retiring
            .push(RemovedPoolOwner {
                retiring_pool: Arc::new(StdMutex::new(None)),
                result,
                result_sender: result_tx,
                cleanup_lock: Arc::new(Mutex::new(())),
                cleanup_permit: Arc::new(StdMutex::new(RemovedPoolCleanupLease::Unconfirmed {
                    permit: None,
                })),
            });

        let reservation = executor
            .reserve_pool_cutover(&NodeExecutorPoolTopology::default(), false)
            .await
            .unwrap();
        assert!(reservation.is_some());
    }

    #[tokio::test]
    async fn newer_deployment_claim_supersedes_and_restores_failed_recovery_exactly() {
        let executor = test_executor().await;
        let recovery_version = Timestamp::try_from(1_u64).unwrap();
        executor.state.write().unwrap().cutover = Some(ActiveCutover::Pending {
            version: recovery_version,
        });

        let reservation = executor
            .reserve_pool_cutover(&NodeExecutorPoolTopology::default(), false)
            .await
            .unwrap()
            .expect("New deployment did not supersede recoverable pending ownership");
        assert!(matches!(
            &executor.state.read().unwrap().cutover,
            Some(ActiveCutover::UncommittedClaim { claim })
                if claim.displaced_recovery == Some(recovery_version)
        ));

        drop(reservation);
        assert!(matches!(
            &executor.state.read().unwrap().cutover,
            Some(ActiveCutover::Pending { version }) if *version == recovery_version
        ));
    }

    #[tokio::test]
    async fn committed_claim_cancellation_restores_recovery_before_newer_admission() {
        let executor = test_executor().await;
        let committed_topology = topology(&[("a.js", "one")]);
        let committed_version = Timestamp::try_from(1_u64).unwrap();
        let mut reservation = executor
            .reserve_pool_cutover(&committed_topology, false)
            .await
            .unwrap();

        executor
            .begin_pool_cutover(&committed_topology, committed_version, &mut reservation)
            .unwrap();
        assert!(matches!(
            &executor.state.read().unwrap().cutover,
            Some(ActiveCutover::CommittedClaim { version, .. })
                if *version == committed_version
        ));

        drop(reservation);
        assert!(matches!(
            &executor.state.read().unwrap().cutover,
            Some(ActiveCutover::Pending { version }) if *version == committed_version
        ));

        let newer_topology = topology(&[("b.js", "two")]);
        let newer_reservation = executor
            .reserve_pool_cutover(&newer_topology, false)
            .await
            .unwrap()
            .expect("Newer deployment did not supersede recoverable pending ownership");
        assert!(executor
            .start_runtime_cutover(cutover_target(committed_topology), committed_version, None,)
            .is_err());
        assert!(matches!(
            &executor.state.read().unwrap().cutover,
            Some(ActiveCutover::UncommittedClaim { claim })
                if claim.topology == newer_topology
                    && claim.displaced_recovery == Some(committed_version)
        ));

        drop(newer_reservation);
        assert!(matches!(
            &executor.state.read().unwrap().cutover,
            Some(ActiveCutover::Pending { version }) if *version == committed_version
        ));
    }

    #[tokio::test]
    async fn forced_deployment_is_queued_before_it_preempts_routine_ownership() {
        let coordinator = crate::local::SurgeCoordinator::new();
        let held = coordinator
            .acquire(crate::local::SurgePriority::Routine, Arc::from("held"))
            .await;
        let routine_coordinator = coordinator.clone();
        let routine = tokio::spawn(async move {
            routine_coordinator
                .acquire(crate::local::SurgePriority::Routine, Arc::from("routine"))
                .await
        });
        tokio::task::yield_now().await;

        let deployment_coordinator = coordinator.clone();
        let deployment = tokio::spawn(async move {
            acquire_deployment_cutover_permit(
                &deployment_coordinator,
                Duration::from_secs(1),
                true,
                &AtomicBool::new(false),
                &Notify::new(),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !held.preempted() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        held.release();
        let deployment_permit = deployment.await.unwrap().unwrap();
        assert!(!routine.is_finished());
        deployment_permit.release();
        routine.await.unwrap().release();
    }

    #[tokio::test]
    async fn forced_deployment_rechecks_a_candidate_that_becomes_a_drain() {
        let coordinator = crate::local::SurgeCoordinator::new();
        let held = coordinator
            .acquire(crate::local::SurgePriority::Deployment, Arc::from("held"))
            .await;
        let deployment_coordinator = coordinator.clone();
        let deployment = tokio::spawn(async move {
            acquire_deployment_cutover_permit(
                &deployment_coordinator,
                Duration::from_secs(1),
                true,
                &AtomicBool::new(false),
                &Notify::new(),
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(!held.preempted());

        held.set_phase("draining");
        tokio::time::timeout(Duration::from_secs(1), async {
            while !held.preempted() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        held.confirm_direct_child_reaped();
        held.release();
        deployment.await.unwrap().unwrap().release();
    }

    #[tokio::test]
    async fn router_shutdown_cancels_a_deployment_cutover_capacity_wait() {
        let coordinator = crate::local::SurgeCoordinator::new();
        let held = coordinator
            .acquire(crate::local::SurgePriority::Routine, Arc::from("held"))
            .await;
        let shutting_down = Arc::new(AtomicBool::new(false));
        let shutdown_changed = Arc::new(Notify::new());
        let waiting_coordinator = coordinator.clone();
        let waiting_shutdown = shutting_down.clone();
        let waiting_changed = shutdown_changed.clone();
        let waiting = tokio::spawn(async move {
            acquire_deployment_cutover_permit(
                &waiting_coordinator,
                Duration::from_secs(60),
                false,
                &waiting_shutdown,
                &waiting_changed,
            )
            .await
        });
        tokio::task::yield_now().await;

        shutting_down.store(true, Ordering::Release);
        shutdown_changed.notify_waiters();
        assert!(waiting.await.unwrap().is_err());
        held.release();
    }

    #[tokio::test]
    async fn forced_deployment_terminates_removed_pool_cleanup() {
        let executor = test_executor().await;
        let pool = Arc::new(RoutedPool::new(
            Arc::from("removed"),
            Timestamp::MIN,
            &executor.local_config,
        ));
        let initialization_guard = pool.initialization_lock.lock().await;
        let mut owner = pool.retire_for_removal(executor.memory_pressure.clone());
        let coordinator = executor.local_config.surge_coordinator();
        let permit = coordinator
            .acquire(
                crate::local::SurgePriority::Deployment,
                Arc::from("deployment"),
            )
            .await;
        permit.set_phase("draining");
        let shutting_down = Arc::new(AtomicBool::new(false));
        let shutdown_changed = Arc::new(Notify::new());
        let memory_pressure = executor.memory_pressure.clone();
        let waiting_shutdown = shutting_down.clone();
        let waiting_changed = shutdown_changed.clone();
        let waiting = tokio::spawn(async move {
            let result = wait_for_removed_pool_cleanup_with_preemption(
                &mut owner,
                &permit,
                &memory_pressure,
                &waiting_shutdown,
                &waiting_changed,
            )
            .await;
            if result.is_ok() {
                permit.confirm_direct_child_reaped();
            }
            permit.release();
            result
        });

        assert_eq!(coordinator.force_preempt_reclaimable(), Some("draining"));
        tokio::time::timeout(Duration::from_secs(1), async {
            while !pool.shutdown_started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(initialization_guard);
        waiting.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn removed_pool_retains_capacity_before_runtime_target_reconstruction() {
        let executor = test_executor().await;
        let configured = topology(&[
            ("removed_one.js", "removed_one"),
            ("removed_two.js", "removed_two"),
        ]);
        executor
            .reconcile_pool_topology(&configured, Timestamp::try_from(1_u64).unwrap())
            .unwrap();
        executor.state.write().unwrap().cutover = None;
        let (first_pool, second_pool) = {
            let state = executor.state.read().unwrap();
            (
                state
                    .named
                    .get(&"removed_one".parse().unwrap())
                    .unwrap()
                    .clone(),
                state
                    .named
                    .get(&"removed_two".parse().unwrap())
                    .unwrap()
                    .clone(),
            )
        };
        let first_initialization_guard = first_pool.initialization_lock.lock().await;
        let second_initialization_guard = second_pool.initialization_lock.lock().await;
        let coordinator = executor.local_config.surge_coordinator();
        let removed = NodeExecutorPoolTopology::default();
        let mut reservation = executor
            .reserve_pool_cutover(&removed, false)
            .await
            .unwrap();

        executor
            .begin_pool_cutover(
                &removed,
                Timestamp::try_from(2_u64).unwrap(),
                &mut reservation,
            )
            .unwrap();
        let owners = executor.state.read().unwrap().retiring.clone();
        assert_eq!(owners.len(), 2);
        assert!(owners.iter().all(|owner| matches!(
            &*owner.cleanup_permit.lock().unwrap(),
            RemovedPoolCleanupLease::Unconfirmed { permit: Some(_) }
        )));

        // Simulate target reconstruction failing before the reservation can be
        // handed to the runtime cutover task.
        drop(reservation);
        let waiting_coordinator = coordinator.clone();
        let later = tokio::spawn(async move {
            waiting_coordinator
                .acquire(crate::local::SurgePriority::Routine, Arc::from("later"))
                .await
        });
        tokio::task::yield_now().await;
        assert!(!later.is_finished());

        assert_eq!(coordinator.force_preempt_reclaimable(), Some("draining"));
        tokio::time::timeout(Duration::from_secs(1), async {
            while !first_pool.shutdown_started.load(Ordering::Acquire)
                || !second_pool.shutdown_started.load(Ordering::Acquire)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(first_initialization_guard);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !owners
                .iter()
                .any(|owner| matches!(&*owner.result.borrow(), Some(Ok(()))))
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!later.is_finished());
        drop(second_initialization_guard);

        let later_permit = tokio::time::timeout(Duration::from_secs(1), later)
            .await
            .unwrap()
            .unwrap();
        assert!(owners
            .iter()
            .all(|owner| matches!(&*owner.result.borrow(), Some(Ok(())))));
        assert!(owners.iter().all(|owner| matches!(
            &*owner.cleanup_permit.lock().unwrap(),
            RemovedPoolCleanupLease::Confirmed
        )));
        later_permit.release();
    }

    #[tokio::test]
    async fn memory_pressure_terminates_removed_pool_cleanup() {
        let executor = test_executor().await;
        let pool = Arc::new(RoutedPool::new(
            Arc::from("removed"),
            Timestamp::MIN,
            &executor.local_config,
        ));
        let initialization_guard = pool.initialization_lock.lock().await;
        let mut owner = pool.retire_for_removal(executor.memory_pressure.clone());
        let permit = executor
            .local_config
            .surge_coordinator()
            .acquire(
                crate::local::SurgePriority::Deployment,
                Arc::from("deployment"),
            )
            .await;
        permit.set_phase("draining");
        let memory_pressure = executor.memory_pressure.clone();
        memory_pressure.set_active(true);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !pool.shutdown_started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        // The removal owner must observe pressure before runtime cutover has
        // begun waiting on that owner.
        let waiting_pressure = memory_pressure.clone();
        let shutting_down = Arc::new(AtomicBool::new(false));
        let shutdown_changed = Arc::new(Notify::new());
        let waiting_shutdown = shutting_down.clone();
        let waiting_changed = shutdown_changed.clone();
        let waiting = tokio::spawn(async move {
            let result = wait_for_removed_pool_cleanup_with_preemption(
                &mut owner,
                &permit,
                &waiting_pressure,
                &waiting_shutdown,
                &waiting_changed,
            )
            .await;
            if result.is_ok() {
                permit.confirm_direct_child_reaped();
            }
            permit.release();
            result
        });

        drop(initialization_guard);
        waiting.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn already_active_pressure_upgrades_removal_synchronously() {
        let executor = test_executor().await;
        let pool = Arc::new(RoutedPool::new(
            Arc::from("removed"),
            Timestamp::MIN,
            &executor.local_config,
        ));
        let initialization_guard = pool.initialization_lock.lock().await;
        executor.memory_pressure.set_active(true);

        let mut owner = pool.retire_for_removal(executor.memory_pressure.clone());

        assert!(pool.shutdown_started.load(Ordering::Acquire));
        drop(initialization_guard);
        let shutting_down = AtomicBool::new(false);
        let shutdown_changed = Notify::new();
        wait_for_removed_pool_cleanup(&mut owner.result, &shutting_down, &shutdown_changed)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn router_shutdown_cancels_a_runtime_cutover_capacity_wait() {
        let coordinator = crate::local::SurgeCoordinator::new();
        let held = coordinator
            .acquire(crate::local::SurgePriority::Routine, Arc::from("held"))
            .await;
        let shutting_down = Arc::new(AtomicBool::new(false));
        let shutdown_changed = Arc::new(Notify::new());
        let waiting_coordinator = coordinator.clone();
        let waiting_shutdown = shutting_down.clone();
        let waiting_changed = shutdown_changed.clone();
        let waiting = tokio::spawn(async move {
            acquire_runtime_cutover_permit(
                &waiting_coordinator,
                Arc::from("deployment"),
                &waiting_shutdown,
                &waiting_changed,
            )
            .await
        });
        tokio::task::yield_now().await;

        shutting_down.store(true, Ordering::Release);
        shutdown_changed.notify_waiters();
        assert!(waiting.await.unwrap().is_err());
        held.release();
    }

    #[tokio::test]
    async fn shutdown_wakes_topology_waiters() {
        let executor = Arc::new(test_executor().await);
        let waiting_topology = topology(&[("consumer.js", "consumer")]);
        let waiting_executor = executor.clone();
        let waiting = tokio::spawn(async move {
            waiting_executor
                .execute_pool(
                    &"consumer.js".parse().unwrap(),
                    Some(&"consumer".parse().unwrap()),
                    &waiting_topology,
                    Timestamp::try_from(1_u64).unwrap(),
                )
                .await
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        executor.shutdown();
        assert!(waiting.await.unwrap().is_err());
    }
}
