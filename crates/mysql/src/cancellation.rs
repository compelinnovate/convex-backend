use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        Mutex,
    },
    time::Duration,
};

use common::runtime::Runtime;
use mysql_async::{
    prelude::Queryable,
    Conn,
    Pool,
};
use tokio::{
    sync::{
        mpsc,
        oneshot,
        watch,
        OnceCell,
    },
    time::{
        timeout_at,
        Instant,
    },
};

use crate::metrics::{
    log_mysql_cancellation_requested,
    log_mysql_cancellation_terminal,
};

const CANCELLATION_DEADLINE: Duration = Duration::from_secs(2);

/// Relationship between the numeric connection identifiers returned for data
/// and cancellation-control connections.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MySqlConnectionIdTopology {
    /// The relationship is not trusted, so cancellation only force-closes the
    /// interrupted client transport.
    Untrusted,
    /// The operator has verified that every data and control connection always
    /// shares one numeric connection-identifier namespace.
    TrustedSingleNamespace,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ConnectionIdentity {
    id: u32,
    generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CancellationTerminal {
    ClientDisconnected,
    KillAccepted,
    StaleGeneration,
    ControlFailure,
}

impl CancellationTerminal {
    fn metric_label(self) -> &'static str {
        match self {
            Self::ClientDisconnected => "client_disconnected",
            Self::KillAccepted => "kill_accepted",
            Self::StaleGeneration => "stale_generation",
            Self::ControlFailure => "control_failure",
        }
    }
}

trait ForceDisconnect: Send + 'static {
    fn identity(&self) -> ConnectionIdentity;

    fn force_disconnect(self);
}

impl ForceDisconnect for Conn {
    fn identity(&self) -> ConnectionIdentity {
        ConnectionIdentity {
            id: self.id(),
            generation: self.local_generation(),
        }
    }

    fn force_disconnect(self) {
        Conn::force_disconnect(self);
    }
}

struct OwnedCancellationTarget<C: ForceDisconnect> {
    connection: Option<C>,
    identity: ConnectionIdentity,
}

impl<C: ForceDisconnect> OwnedCancellationTarget<C> {
    fn new(connection: C) -> Self {
        let identity = connection.identity();
        Self {
            connection: Some(connection),
            identity,
        }
    }

    fn identity(&self) -> ConnectionIdentity {
        self.identity
    }

    fn force_disconnect(mut self) {
        self.connection
            .take()
            .expect("cancellation target connection missing")
            .force_disconnect();
    }
}

impl<C: ForceDisconnect> Drop for OwnedCancellationTarget<C> {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            // The worker can be aborted or shut down with work still queued.
            // A canceled operation's connection must never return to recycler
            // cleanup with an incomplete protocol response.
            connection.force_disconnect();
        }
    }
}

struct CancellationRequest<C: ForceDisconnect> {
    connection: OwnedCancellationTarget<C>,
    requested_at: Instant,
    completion: oneshot::Sender<CancellationTerminal>,
}

#[derive(Clone, Copy)]
struct CancellationTarget {
    identity: ConnectionIdentity,
    requested_at: Instant,
}

struct ControlConnection {
    conn: Option<Conn>,
}

impl ControlConnection {
    fn new(conn: Conn) -> Self {
        Self { conn: Some(conn) }
    }

    fn conn_mut(&mut self) -> &mut Conn {
        self.conn.as_mut().expect("control connection missing")
    }

    fn release(mut self) {
        drop(self.conn.take());
    }
}

impl Drop for ControlConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // A timed-out or failed KILL may leave unread protocol data. Never
            // return that control connection to the single-connection pool.
            conn.force_disconnect();
        }
    }
}

type ProcessRequest = Arc<
    dyn Fn(CancellationTarget) -> Pin<Box<dyn Future<Output = CancellationTerminal> + Send>>
        + Send
        + Sync,
>;

enum WorkerMessage<C: ForceDisconnect> {
    Cancel(CancellationRequest<C>),
    Shutdown,
}

#[derive(Clone, Copy)]
enum ControlInitialization {
    Ready { generation: u64 },
    Failed,
}

enum CancellationRoute<R> {
    ClientDisconnectOnly,
    TrustedSingleNamespace(R),
}

pub(super) struct CancellationReservation {
    route: CancellationRoute<mpsc::OwnedPermit<WorkerMessage<Conn>>>,
}

enum CancellationDispatch<T> {
    ClientDisconnected,
    TrustedSingleNamespace(T),
}

fn dispatch_cancellation<C: ForceDisconnect, R, T>(
    connection: C,
    route: CancellationRoute<R>,
    trusted_single_namespace: impl FnOnce(C, R) -> T,
) -> CancellationDispatch<T> {
    match route {
        CancellationRoute::ClientDisconnectOnly => {
            connection.force_disconnect();
            CancellationDispatch::ClientDisconnected
        },
        CancellationRoute::TrustedSingleNamespace(reservation) => {
            CancellationDispatch::TrustedSingleNamespace(trusted_single_namespace(
                connection,
                reservation,
            ))
        },
    }
}

pub(super) enum CancellationCompletion {
    Immediate(CancellationTerminal),
    Pending(oneshot::Receiver<CancellationTerminal>),
}

impl CancellationCompletion {
    pub(super) async fn wait(self) -> CancellationTerminal {
        match self {
            Self::Immediate(terminal) => terminal,
            Self::Pending(receiver) => receiver
                .await
                .unwrap_or(CancellationTerminal::ControlFailure),
        }
    }
}

#[derive(Clone)]
pub(super) struct MySqlCancellationController<RT: Runtime> {
    sender: mpsc::Sender<WorkerMessage<Conn>>,
    control_pool: Pool,
    control_initialization: Arc<OnceCell<ControlInitialization>>,
    active_generations: Arc<Mutex<HashMap<u32, u64>>>,
    worker_done: watch::Receiver<bool>,
    cluster_name: String,
    connection_id_topology: MySqlConnectionIdTopology,
    _runtime: RT,
}

impl<RT: Runtime> MySqlCancellationController<RT> {
    pub(super) fn new(
        control_pool: Pool,
        runtime: RT,
        capacity: usize,
        cluster_name: String,
        connection_id_topology: MySqlConnectionIdTopology,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);
        let control_initialization = Arc::new(OnceCell::new());
        let active_generations = Arc::new(Mutex::new(HashMap::new()));
        let (worker_done_tx, worker_done) = watch::channel(false);
        let process_control_pool = control_pool.clone();
        let process_control_initialization = control_initialization.clone();
        let process_active_generations = active_generations.clone();
        let process_request: ProcessRequest = Arc::new(move |target| {
            let control_pool = process_control_pool.clone();
            let control_initialization = process_control_initialization.clone();
            let active_generations = process_active_generations.clone();
            Box::pin(async move {
                let ControlInitialization::Ready { generation } = control_initialization
                    .get()
                    .expect("cancellation request admitted before control initialization")
                else {
                    panic!("cancellation request admitted after control initialization failed");
                };
                Self::process_request(&control_pool, *generation, &active_generations, target).await
            })
        });
        runtime.spawn_background(
            "mysql_cancellation_worker",
            Self::run_worker(
                receiver,
                process_request,
                active_generations.clone(),
                worker_done_tx,
                cluster_name.clone(),
            ),
        );
        Self {
            sender,
            control_pool,
            control_initialization,
            active_generations,
            worker_done,
            cluster_name,
            connection_id_topology,
            _runtime: runtime,
        }
    }

    pub(super) fn register(&self, conn: &Conn) {
        match self.connection_id_topology {
            MySqlConnectionIdTopology::Untrusted => return,
            MySqlConnectionIdTopology::TrustedSingleNamespace => {},
        }
        self.active_generations
            .lock()
            .expect("MySQL cancellation generation registry poisoned")
            .insert(conn.id(), conn.local_generation());
    }

    pub(super) fn unregister(&self, conn: &Conn) {
        match self.connection_id_topology {
            MySqlConnectionIdTopology::Untrusted => return,
            MySqlConnectionIdTopology::TrustedSingleNamespace => {},
        }
        Self::unregister_identity(
            &self.active_generations,
            ConnectionIdentity {
                id: conn.id(),
                generation: conn.local_generation(),
            },
        );
    }

    fn unregister_identity(
        active_generations: &Mutex<HashMap<u32, u64>>,
        identity: ConnectionIdentity,
    ) {
        let mut active_generations = active_generations
            .lock()
            .expect("MySQL cancellation generation registry poisoned");
        if active_generations.get(&identity.id) == Some(&identity.generation) {
            active_generations.remove(&identity.id);
        }
    }

    fn identity_is_active(
        active_generations: &Mutex<HashMap<u32, u64>>,
        identity: ConnectionIdentity,
    ) -> bool {
        active_generations
            .lock()
            .expect("MySQL cancellation generation registry poisoned")
            .get(&identity.id)
            == Some(&identity.generation)
    }

    pub(super) async fn initialize(&self) -> anyhow::Result<()> {
        match self.connection_id_topology {
            MySqlConnectionIdTopology::Untrusted => return Ok(()),
            MySqlConnectionIdTopology::TrustedSingleNamespace => {},
        }
        let control_initialization = self
            .control_initialization
            .get_or_init(|| async {
                match crate::connection::with_timeout(self.control_pool.get_conn()).await {
                    Ok(conn) => {
                        let generation = conn.local_generation();
                        drop(conn);
                        ControlInitialization::Ready { generation }
                    },
                    Err(_) => ControlInitialization::Failed,
                }
            })
            .await;
        anyhow::ensure!(
            matches!(control_initialization, ControlInitialization::Ready { .. }),
            "MySQL server-side cancellation is unavailable"
        );
        Ok(())
    }

    pub(super) async fn reserve(&self) -> anyhow::Result<CancellationReservation> {
        match self.connection_id_topology {
            MySqlConnectionIdTopology::Untrusted => {
                return Ok(CancellationReservation {
                    route: CancellationRoute::ClientDisconnectOnly,
                });
            },
            MySqlConnectionIdTopology::TrustedSingleNamespace => {},
        }
        self.initialize().await?;
        let permit = self
            .sender
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| anyhow::anyhow!("MySQL server-side cancellation is unavailable"))?;
        Ok(CancellationReservation {
            route: CancellationRoute::TrustedSingleNamespace(permit),
        })
    }

    pub(super) fn cancel(
        &self,
        conn: Conn,
        reservation: CancellationReservation,
    ) -> CancellationCompletion {
        let requested_at = Instant::now();
        log_mysql_cancellation_requested(&self.cluster_name);
        match dispatch_cancellation(conn, reservation.route, |conn, permit| {
            let (completion, receiver) = oneshot::channel();
            permit.send(WorkerMessage::Cancel(CancellationRequest {
                connection: OwnedCancellationTarget::new(conn),
                requested_at,
                completion,
            }));
            receiver
        }) {
            CancellationDispatch::ClientDisconnected => {
                let terminal = CancellationTerminal::ClientDisconnected;
                log_mysql_cancellation_terminal(
                    &self.cluster_name,
                    terminal.metric_label(),
                    requested_at.elapsed(),
                );
                CancellationCompletion::Immediate(terminal)
            },
            CancellationDispatch::TrustedSingleNamespace(receiver) => {
                CancellationCompletion::Pending(receiver)
            },
        }
    }

    async fn run_worker<C: ForceDisconnect>(
        mut receiver: mpsc::Receiver<WorkerMessage<C>>,
        process_request: ProcessRequest,
        active_generations: Arc<Mutex<HashMap<u32, u64>>>,
        worker_done: watch::Sender<bool>,
        cluster_name: String,
    ) {
        let mut control_failed = false;
        while let Some(message) = receiver.recv().await {
            match message {
                WorkerMessage::Cancel(request) => {
                    let identity = request.connection.identity();
                    let terminal = if control_failed {
                        CancellationTerminal::ControlFailure
                    } else {
                        process_request(CancellationTarget {
                            identity,
                            requested_at: request.requested_at,
                        })
                        .await
                    };
                    if terminal == CancellationTerminal::ControlFailure {
                        // Close admission before publishing the failure. Waking
                        // the request waiter first could let another runtime
                        // thread reserve cancellation capacity and issue SQL
                        // after the control lane had already failed.
                        control_failed = true;
                        receiver.close();
                    }
                    // Keep the target transport alive until KILL has reached a
                    // terminal response. Closing it earlier can let a completed
                    // statement's server session disappear before KILL, and can
                    // let the numeric ID be assigned to an unrelated session.
                    request.connection.force_disconnect();
                    // The request owns this physical generation. Do not remove a
                    // newer connection that has reused the same server ID.
                    Self::unregister_identity(&active_generations, identity);
                    log_mysql_cancellation_terminal(
                        &cluster_name,
                        terminal.metric_label(),
                        request.requested_at.elapsed(),
                    );
                    let _ = request.completion.send(terminal);
                },
                WorkerMessage::Shutdown => break,
            }
        }
        worker_done.send_replace(true);
    }

    async fn process_request(
        control_pool: &Pool,
        control_generation: u64,
        active_generations: &Mutex<HashMap<u32, u64>>,
        target: CancellationTarget,
    ) -> CancellationTerminal {
        // A superseding physical connection proves that this server ID no
        // longer identifies the target. Do not acquire scarce control capacity
        // or fail the cancellation lane for a request that no longer needs a
        // kill.
        if !Self::identity_is_active(active_generations, target.identity) {
            return CancellationTerminal::StaleGeneration;
        }
        let deadline = target.requested_at + CANCELLATION_DEADLINE;
        if Instant::now() >= deadline {
            return CancellationTerminal::ControlFailure;
        }
        match timeout_at(deadline, async {
            let mut control = ControlConnection::new(control_pool.get_conn().await?);
            if control.conn_mut().local_generation() != control_generation {
                // A replacement control transport can belong to a restarted
                // server or a new backend namespace where the same numeric ID
                // identifies an unrelated session.
                return Ok::<_, mysql_async::Error>(CancellationTerminal::ControlFailure);
            }
            // The data-connection generation rejects local ID reuse. With the
            // original control transport still present, the hard deadline bounds
            // the remaining check-to-KILL race within one server epoch.
            if !Self::identity_is_active(active_generations, target.identity) {
                control.release();
                return Ok::<_, mysql_async::Error>(CancellationTerminal::StaleGeneration);
            }
            let statement = format!("KILL CONNECTION {}", target.identity.id);
            control.conn_mut().query_drop(statement).await?;
            control.release();
            Ok(CancellationTerminal::KillAccepted)
        })
        .await
        {
            Ok(Ok(terminal)) => terminal,
            Ok(Err(_)) | Err(_) => CancellationTerminal::ControlFailure,
        }
    }

    pub(super) async fn shutdown(&self) -> anyhow::Result<()> {
        if !*self.worker_done.borrow() {
            let _ = self.sender.send(WorkerMessage::Shutdown).await;
            let mut worker_done = self.worker_done.clone();
            worker_done.wait_for(|done| *done).await?;
        }
        Ok(self.control_pool.clone().disconnect().await?)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            atomic::{
                AtomicBool,
                Ordering,
            },
            Arc,
            Mutex,
        },
    };

    use mysql_async::{
        Opts,
        Pool,
    };
    use tokio::sync::{
        mpsc,
        oneshot,
        watch,
        Notify,
    };

    use super::{
        dispatch_cancellation,
        CancellationDispatch,
        CancellationRequest,
        CancellationRoute,
        CancellationTarget,
        CancellationTerminal,
        ConnectionIdentity,
        ForceDisconnect,
        MySqlCancellationController,
        ProcessRequest,
        WorkerMessage,
    };

    struct TestConnection {
        identity: ConnectionIdentity,
        disconnected: Arc<AtomicBool>,
    }

    impl ForceDisconnect for TestConnection {
        fn identity(&self) -> ConnectionIdentity {
            self.identity
        }

        fn force_disconnect(self) {
            assert!(!self.disconnected.swap(true, Ordering::SeqCst));
        }
    }

    struct RecyclerProbeConnection {
        disconnected: Arc<AtomicBool>,
        recycled: Arc<AtomicBool>,
    }

    impl ForceDisconnect for RecyclerProbeConnection {
        fn identity(&self) -> ConnectionIdentity {
            ConnectionIdentity {
                id: 17,
                generation: 12,
            }
        }

        fn force_disconnect(self) {
            self.disconnected.store(true, Ordering::SeqCst);
        }
    }

    impl Drop for RecyclerProbeConnection {
        fn drop(&mut self) {
            if !self.disconnected.load(Ordering::SeqCst) {
                self.recycled.store(true, Ordering::SeqCst);
            }
        }
    }

    fn request(
        id: u32,
        generation: u64,
    ) -> (
        CancellationRequest<TestConnection>,
        oneshot::Receiver<CancellationTerminal>,
        Arc<AtomicBool>,
    ) {
        let (completion, receiver) = oneshot::channel();
        let disconnected = Arc::new(AtomicBool::new(false));
        (
            CancellationRequest {
                connection: super::OwnedCancellationTarget::new(TestConnection {
                    identity: ConnectionIdentity { id, generation },
                    disconnected: disconnected.clone(),
                }),
                requested_at: tokio::time::Instant::now(),
                completion,
            },
            receiver,
            disconnected,
        )
    }

    #[test]
    fn untrusted_topology_disconnects_without_kill_or_recycling() {
        let disconnected = Arc::new(AtomicBool::new(false));
        let recycled = Arc::new(AtomicBool::new(false));
        let server_side_dispatched = Arc::new(AtomicBool::new(false));
        let connection = RecyclerProbeConnection {
            disconnected: disconnected.clone(),
            recycled: recycled.clone(),
        };

        let dispatch =
            dispatch_cancellation(connection, CancellationRoute::<()>::ClientDisconnectOnly, {
                let server_side_dispatched = server_side_dispatched.clone();
                move |_connection, ()| {
                    server_side_dispatched.store(true, Ordering::SeqCst);
                }
            });

        assert!(matches!(dispatch, CancellationDispatch::ClientDisconnected));
        assert!(disconnected.load(Ordering::SeqCst));
        assert!(!recycled.load(Ordering::SeqCst));
        assert!(!server_side_dispatched.load(Ordering::SeqCst));
    }

    #[test]
    fn trusted_single_namespace_dispatches_without_early_disconnect() {
        let disconnected = Arc::new(AtomicBool::new(false));
        let server_side_dispatched = Arc::new(AtomicBool::new(false));
        let connection = TestConnection {
            identity: ConnectionIdentity {
                id: 17,
                generation: 12,
            },
            disconnected: disconnected.clone(),
        };

        let dispatch =
            dispatch_cancellation(connection, CancellationRoute::TrustedSingleNamespace(()), {
                let disconnected = disconnected.clone();
                let server_side_dispatched = server_side_dispatched.clone();
                move |_connection, ()| {
                    assert!(!disconnected.load(Ordering::SeqCst));
                    server_side_dispatched.store(true, Ordering::SeqCst);
                    23
                }
            });

        assert!(matches!(
            dispatch,
            CancellationDispatch::TrustedSingleNamespace(23)
        ));
        assert!(server_side_dispatched.load(Ordering::SeqCst));
        assert!(!disconnected.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn newer_local_generation_makes_cancellation_stale_without_control_acquisition() {
        let control_pool = Pool::new(Opts::default());
        let active = Mutex::new(HashMap::from([(17, 12)]));
        let terminal = MySqlCancellationController::<runtime::prod::ProdRuntime>::process_request(
            &control_pool,
            0,
            &active,
            CancellationTarget {
                identity: ConnectionIdentity {
                    id: 17,
                    generation: 11,
                },
                requested_at: tokio::time::Instant::now(),
            },
        )
        .await;
        assert_eq!(terminal, CancellationTerminal::StaleGeneration);
    }

    #[test]
    fn unregister_removes_only_the_matching_physical_generation() {
        let active = Mutex::new(HashMap::from([(17, 12)]));
        MySqlCancellationController::<runtime::prod::ProdRuntime>::unregister_identity(
            &active,
            ConnectionIdentity {
                id: 17,
                generation: 11,
            },
        );
        assert_eq!(active.lock().unwrap().get(&17), Some(&12));

        MySqlCancellationController::<runtime::prod::ProdRuntime>::unregister_identity(
            &active,
            ConnectionIdentity {
                id: 17,
                generation: 12,
            },
        );
        assert!(!active.lock().unwrap().contains_key(&17));
    }

    #[test]
    fn kill_statement_contains_only_numeric_connection_id() {
        let identity = ConnectionIdentity {
            id: u32::MAX,
            generation: 1,
        };
        assert_eq!(
            format!("KILL CONNECTION {}", identity.id),
            "KILL CONNECTION 4294967295"
        );
    }

    #[tokio::test]
    async fn target_connection_remains_open_through_kill_response() {
        let (sender, receiver) = mpsc::channel(1);
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (request, completion, disconnected) = request(1, 1);
        let process: ProcessRequest = Arc::new({
            let started = started.clone();
            let release = release.clone();
            let disconnected = disconnected.clone();
            move |_target| {
                let started = started.clone();
                let release = release.clone();
                let disconnected = disconnected.clone();
                Box::pin(async move {
                    assert!(!disconnected.load(Ordering::SeqCst));
                    started.notify_one();
                    release.notified().await;
                    assert!(!disconnected.load(Ordering::SeqCst));
                    CancellationTerminal::KillAccepted
                })
            }
        });
        let worker = tokio::spawn(
            MySqlCancellationController::<runtime::prod::ProdRuntime>::run_worker(
                receiver,
                process,
                Arc::new(Mutex::new(HashMap::from([(1, 1)]))),
                watch::channel(false).0,
                "test".to_string(),
            ),
        );

        sender.send(WorkerMessage::Cancel(request)).await.unwrap();
        started.notified().await;
        assert!(!disconnected.load(Ordering::SeqCst));
        release.notify_one();
        assert_eq!(
            completion.await.unwrap(),
            CancellationTerminal::KillAccepted
        );
        // Pool capacity is released before the terminal result is published.
        assert!(disconnected.load(Ordering::SeqCst));

        sender.send(WorkerMessage::Shutdown).await.unwrap();
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn aborting_worker_force_disconnects_active_target() {
        let (sender, receiver) = mpsc::channel(1);
        let started = Arc::new(Notify::new());
        let process: ProcessRequest = Arc::new({
            let started = started.clone();
            move |_target| {
                let started = started.clone();
                Box::pin(async move {
                    started.notify_one();
                    std::future::pending().await
                })
            }
        });
        let worker = tokio::spawn(
            MySqlCancellationController::<runtime::prod::ProdRuntime>::run_worker(
                receiver,
                process,
                Arc::new(Mutex::new(HashMap::from([(1, 1)]))),
                watch::channel(false).0,
                "test".to_string(),
            ),
        );
        let (request, completion, disconnected) = request(1, 1);
        sender.send(WorkerMessage::Cancel(request)).await.unwrap();
        started.notified().await;

        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        assert!(disconnected.load(Ordering::SeqCst));
        assert!(completion.await.is_err());
    }

    #[tokio::test]
    async fn request_queued_after_shutdown_is_force_disconnected() {
        let (sender, receiver) = mpsc::channel(2);
        sender.send(WorkerMessage::Shutdown).await.unwrap();
        let (request, completion, disconnected) = request(1, 1);
        sender.send(WorkerMessage::Cancel(request)).await.unwrap();
        let process: ProcessRequest = Arc::new(|_target| {
            Box::pin(async { panic!("request after shutdown must not be processed") })
        });

        MySqlCancellationController::<runtime::prod::ProdRuntime>::run_worker(
            receiver,
            process,
            Arc::new(Mutex::new(HashMap::from([(1, 1)]))),
            watch::channel(false).0,
            "test".to_string(),
        )
        .await;

        assert!(disconnected.load(Ordering::SeqCst));
        assert!(completion.await.is_err());
    }

    #[tokio::test]
    async fn dedicated_worker_progresses_while_next_request_is_reserved() {
        let (sender, receiver) = mpsc::channel(1);
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let process: ProcessRequest = Arc::new({
            let started = started.clone();
            let release = release.clone();
            move |_target: CancellationTarget| {
                let started = started.clone();
                let release = release.clone();
                Box::pin(async move {
                    started.notify_one();
                    release.notified().await;
                    CancellationTerminal::KillAccepted
                })
            }
        });
        let worker = tokio::spawn(
            MySqlCancellationController::<runtime::prod::ProdRuntime>::run_worker(
                receiver,
                process,
                Arc::new(Mutex::new(HashMap::from([(1, 1)]))),
                watch::channel(false).0,
                "test".to_string(),
            ),
        );

        let first_permit = sender.clone().reserve_owned().await.unwrap();
        let (first, first_completion, first_disconnected) = request(1, 1);
        first_permit.send(WorkerMessage::Cancel(first));
        started.notified().await;

        // This reservation does not depend on ordinary data-pool capacity.
        let second_permit = sender.clone().reserve_owned().await.unwrap();
        release.notify_one();
        assert_eq!(
            first_completion.await.unwrap(),
            CancellationTerminal::KillAccepted
        );
        assert!(first_disconnected.load(Ordering::SeqCst));
        drop(second_permit);
        sender.send(WorkerMessage::Shutdown).await.unwrap();
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn control_failure_closes_new_reservations_and_drains_existing_reservations() {
        let (sender, receiver) = mpsc::channel(2);
        let process: ProcessRequest =
            Arc::new(|_target| Box::pin(async { CancellationTerminal::ControlFailure }));
        let (worker_done, worker_done_rx) = watch::channel(false);
        let worker = tokio::spawn(
            MySqlCancellationController::<runtime::prod::ProdRuntime>::run_worker(
                receiver,
                process,
                Arc::new(Mutex::new(HashMap::from([(1, 1), (2, 2)]))),
                worker_done,
                "test".to_string(),
            ),
        );
        let first_permit = sender.clone().reserve_owned().await.unwrap();
        let second_permit = sender.clone().reserve_owned().await.unwrap();
        let (first, first_completion, first_disconnected) = request(1, 1);
        let (second, second_completion, second_disconnected) = request(2, 2);
        first_permit.send(WorkerMessage::Cancel(first));

        assert_eq!(
            first_completion.await.unwrap(),
            CancellationTerminal::ControlFailure
        );
        assert!(first_disconnected.load(Ordering::SeqCst));
        // Failure publication happens only after admission is closed.
        assert!(sender.is_closed());
        assert!(sender.reserve().await.is_err());
        // Receiver::close must retain permits acquired before the failure. An
        // already-running database operation can still need to enqueue its
        // cancellation after the control lane has failed.
        second_permit.send(WorkerMessage::Cancel(second));
        assert_eq!(
            second_completion.await.unwrap(),
            CancellationTerminal::ControlFailure
        );
        assert!(second_disconnected.load(Ordering::SeqCst));
        worker.await.unwrap();
        assert!(*worker_done_rx.borrow());
    }
}
