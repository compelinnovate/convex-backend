use std::{
    fs,
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
        Weak,
    },
    time::{
        Duration,
        Instant,
    },
};

use anyhow::Context;
use async_trait::async_trait;
use common::log_lines::LogLine;
use errors::ErrorMetadata;
use futures_async_stream::try_stream;
use isolate::bundled_js::node_executor_file;
use rand::Rng;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use tokio::{
    io::AsyncReadExt,
    process::{
        Child,
        Command as TokioCommand,
    },
    sync::{
        mpsc,
        Mutex,
    },
};

use crate::executor::{
    handle_node_executor_stream,
    ExecutorRequest,
    InvokeCompletion,
    InvokeResponse,
    NodeExecutor,
    NodeExecutorStreamPart,
    ARGS_TOO_LARGE_RESPONSE_MESSAGE,
    EXECUTE_TIMEOUT_RESPONSE_JSON,
};

const NVMRC_VERSION: &str = include_str!("../../../.nvmrc");
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_millis(100);
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_HEALTH_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_INVOKE_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const MAX_NODE_VERSION_OUTPUT_BYTES: usize = 1024;
const MAX_HEALTH_CHECK_ATTEMPTS: u32 = 50;
const NODE_VERSION_CHECK_TIMEOUT: Duration = Duration::from_secs(5);
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(1);
const WATCHDOG_FAILURE_THRESHOLD: u32 = 5;

pub struct LocalNodeExecutor {
    state: Arc<Mutex<LocalNodeExecutorState>>,
    startup_lock: Mutex<()>,
    shutting_down: AtomicBool,
    config: LocalNodeExecutorConfig,
}

#[derive(Default)]
struct LocalNodeExecutorState {
    inner: Option<Arc<InnerLocalNodeExecutor>>,
    replacement_for_generation: Option<u64>,
    next_generation: u64,
}

#[derive(Clone)]
struct LocalNodeExecutorConfig {
    node_process_timeout: Duration,
    /// Overrides the initial callback retry backoff in the spawned node
    /// process (read by syscalls.ts at module load). Tests zero this so
    /// callbacks retrying against an unreachable backend settle within test
    /// timeouts.
    callback_initial_backoff: Option<Duration>,
    health_check_timeout: Duration,
    watchdog_interval: Duration,
    watchdog_failure_threshold: u32,
}

struct ManagedChild {
    // Rust owns only the direct server child. Descendant containment has no
    // completion acknowledgment at this boundary.
    generation: u64,
    child: Option<Child>,
    source_dir: Option<TempDir>,
}

struct ReapingTempDir {
    generation: u64,
    source_dir: Option<TempDir>,
}

struct InnerLocalNodeExecutor {
    generation: u64,
    started_at: Instant,
    runtime_stats_supported: bool,
    active_requests: AtomicUsize,
    retained_source_packages: AtomicU64,
    retained_external_packages: AtomicU64,
    registered_stack_roots: AtomicU64,
    // Initiate kill and reaping before removing the tempdir if explicit
    // termination cannot complete or startup is canceled.
    server_handle: Mutex<ManagedChild>,
    client: reqwest::Client,
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

#[derive(Clone, Copy)]
enum GenerationRetirementReason {
    RequestTimeout,
    ResponseStreamTimeout,
    ConnectionError,
    ProcessExiting,
    HealthCheckFailed,
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

struct ActiveRequestGuard {
    inner: Arc<InnerLocalNodeExecutor>,
    outcome: &'static str,
}

struct WaitingRequestGuard {
    waiting: bool,
}

impl ReapingTempDir {
    fn new(generation: u64, source_dir: TempDir) -> Self {
        Self {
            generation,
            source_dir: Some(source_dir),
        }
    }

    fn remove_after_reaping(mut self) {
        let source_dir = self
            .source_dir
            .take()
            .expect("Reaped local Node executor child has no temp directory");
        let generation = self.generation;
        // Package trees can be several GiB. Retain the path before spawning so
        // thread-start failure preserves it, and keep recursive deletion out
        // of both async workers and Tokio's shutdown-waited blocking pool.
        let source_path = source_dir.keep();
        if let Err(error) = std::thread::Builder::new()
            .name("local-node-tempdir-cleanup".to_owned())
            .spawn(move || {
                if let Err(error) = fs::remove_dir_all(source_path) {
                    tracing::error!(
                        generation,
                        error_kind = ?error.kind(),
                        "Failed to remove reaped local Node executor temp directory"
                    );
                }
            })
        {
            tracing::error!(
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
                generation = self.generation,
                "Retained local Node executor temp directory because direct child reaping was not \
                 confirmed"
            );
        }
    }
}

impl ManagedChild {
    fn new(generation: u64, child: Child, source_dir: TempDir) -> Self {
        Self {
            generation,
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
        let result = InnerLocalNodeExecutor::terminate_child(generation, self.child_mut()).await;
        if result.is_ok() {
            self.mark_reaped();
        }
        result
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
        let source_dir = ReapingTempDir::new(
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
                    generation,
                    error_kind = ?error.kind(),
                    "Failed to retry termination of dropped local Node executor child"
                );
                drop(child);
                return;
            }
            match child.wait().await {
                Ok(status) => {
                    InnerLocalNodeExecutor::record_child_exit(status);
                    drop(child);
                    source_dir.remove_after_reaping();
                },
                Err(error) => {
                    tracing::error!(
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
        package.source_hits >= previous_package.source_hits
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
            ReapingTempDir::new(self.generation, source_dir).remove_after_reaping();
        }
    }
}

impl WaitingRequestGuard {
    fn new() -> Self {
        crate::metrics::increment_local_node_waiting_requests();
        Self { waiting: true }
    }

    fn finish(mut self) {
        crate::metrics::decrement_local_node_waiting_requests();
        self.waiting = false;
    }
}

impl Drop for WaitingRequestGuard {
    fn drop(&mut self) {
        if self.waiting {
            crate::metrics::decrement_local_node_waiting_requests();
        }
    }
}

impl ActiveRequestGuard {
    fn new(inner: Arc<InnerLocalNodeExecutor>) -> Self {
        inner.active_requests.fetch_add(1, Ordering::Relaxed);
        crate::metrics::log_local_node_request_start();
        Self {
            inner,
            outcome: "internal_error",
        }
    }

    fn set_outcome(&mut self, outcome: &'static str) {
        self.outcome = outcome;
    }
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        let generation_previous = self.inner.active_requests.fetch_sub(1, Ordering::Relaxed);
        assert!(generation_previous > 0);
        crate::metrics::log_local_node_request_completion(self.outcome);
    }
}

impl InnerLocalNodeExecutor {
    async fn new(generation: u64, config: &LocalNodeExecutorConfig) -> anyhow::Result<Self> {
        tracing::info!("Initializing inner local node executor");
        // Create a single temp directory for both source files and Node.js temp files
        let source_dir = TempDir::new()?;
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
        let server_handle =
            Self::start_node_with_listener(config, &source_path, &source_dir, &socket_path).await?;
        let mut server_handle = ManagedChild::new(generation, server_handle, source_dir);
        crate::metrics::log_local_node_child_start();

        // A new child has no prior backend observation. Use a zero baseline so
        // startup cannot accept cumulative values that the watchdog rejects.
        let empty_package_stats = NodePackageCacheStats::default();
        let empty_stack_stats = NodeStackTraceStats::default();
        // Wait for the Node process to be ready to handle HTTP requests.
        for _ in 0..MAX_HEALTH_CHECK_ATTEMPTS {
            match server_handle.child_mut().try_wait() {
                Ok(Some(status)) => {
                    Self::record_child_exit(status);
                    server_handle.mark_reaped();
                    anyhow::bail!("Node executor server exited before becoming healthy");
                },
                Ok(None) => {},
                Err(error) => {
                    server_handle.terminate().await?;
                    anyhow::bail!(
                        "Failed to inspect local Node executor child: {:?}",
                        error.kind()
                    );
                },
            }
            let health_check_started = Instant::now();
            let health = Self::check_server_health(&client, config.health_check_timeout).await;
            let runtime_stats_supported = health
                .as_ref()
                .filter(|health| health.status == "ok")
                .and_then(|health| {
                    health.valid_runtime_stats_support(&empty_package_stats, &empty_stack_stats)
                });
            crate::metrics::log_local_node_health_check(
                health_check_started.elapsed(),
                "startup",
                runtime_stats_supported.is_some(),
            );
            if let Some(runtime_stats_supported) = runtime_stats_supported {
                return Ok(Self {
                    generation,
                    started_at: Instant::now(),
                    runtime_stats_supported,
                    active_requests: AtomicUsize::new(0),
                    retained_source_packages: AtomicU64::new(0),
                    retained_external_packages: AtomicU64::new(0),
                    registered_stack_roots: AtomicU64::new(0),
                    server_handle: Mutex::new(server_handle),
                    client,
                });
            }
            tokio::time::sleep(HEALTH_CHECK_INTERVAL).await;
        }
        server_handle.terminate().await?;
        anyhow::bail!("Node executor server failed to start and become healthy")
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
        let (status, version, output_too_large) =
            tokio::time::timeout(NODE_VERSION_CHECK_TIMEOUT, probe)
                .await
                .map_err(|_| {
                    ErrorMetadata::bad_request(
                        "DeploymentNotConfiguredForNodeActions",
                        "Deployment is not configured to deploy \"use node\" actions. The Node.js \
                         version check timed out.",
                    )
                })?
                .map_err(|error| {
                    anyhow::anyhow!(
                        "Failed to complete local Node version check: {:?}",
                        error.kind()
                    )
                })?;

        if output_too_large
            || !status.success()
            || (!version.starts_with(b"v20.")
                && !version.starts_with(b"v22.")
                && !version.starts_with(b"v24."))
        {
            anyhow::bail!(ErrorMetadata::bad_request(
                "DeploymentNotConfiguredForNodeActions",
                "Deployment is not configured to deploy \"use node\" actions. \
                 Node.js v20, 22, or 24 is not installed. \
                 Install a supported Node.js version with nvm (https://github.com/nvm-sh/nvm) \
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

    async fn terminate(&self) -> anyhow::Result<ChildTerminationObservation> {
        let mut child = self.server_handle.lock().await;
        child.terminate().await
    }

    async fn terminate_child(
        generation: u64,
        child: &mut Child,
    ) -> anyhow::Result<ChildTerminationObservation> {
        let state_before = match child.try_wait() {
            Ok(Some(status)) => {
                let exit_class = Self::record_child_exit(status);
                return Ok(ChildTerminationObservation {
                    state_before: "already_exited",
                    supervisor_kill_requested: false,
                    exit_class,
                });
            },
            Ok(None) => "running",
            Err(error) => {
                tracing::warn!(
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
        let exit_class = Self::record_child_exit(status);
        Ok(ChildTerminationObservation {
            state_before,
            supervisor_kill_requested,
            exit_class,
        })
    }

    fn record_child_exit(status: ExitStatus) -> &'static str {
        let exit_class = if status.success() {
            "success"
        } else if status.code().is_some() {
            "failure"
        } else {
            "signal"
        };
        crate::metrics::log_local_node_child_exit(exit_class);
        exit_class
    }

    async fn start_node_with_listener(
        config: &LocalNodeExecutorConfig,
        source_path: &Path,
        temp_dir: &TempDir,
        socket_path: &Path,
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
        cmd.arg(source_path)
            .arg("--ipc-path")
            .arg(socket_path)
            .arg("--tempdir")
            .arg(temp_dir.path())
            // Function console output uses the bounded response protocol.
            // Do not let direct user writes bypass it into infrastructure logs.
            .stdin(Stdio::null())
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
    pub async fn new(node_process_timeout: Duration) -> anyhow::Result<Self> {
        let executor = Self {
            state: Arc::new(Mutex::new(LocalNodeExecutorState::default())),
            startup_lock: Mutex::new(()),
            shutting_down: AtomicBool::new(false),
            config: LocalNodeExecutorConfig {
                node_process_timeout,
                callback_initial_backoff: None,
                health_check_timeout: HEALTH_CHECK_TIMEOUT,
                watchdog_interval: WATCHDOG_INTERVAL,
                watchdog_failure_threshold: WATCHDOG_FAILURE_THRESHOLD,
            },
        };

        crate::metrics::set_local_node_generation_present(false);
        crate::metrics::set_local_node_generation_age(Duration::ZERO);
        crate::metrics::set_local_node_waiting_requests(0);
        crate::metrics::set_local_node_active_requests(0);
        crate::metrics::set_local_node_consecutive_health_misses(0);

        Ok(executor)
    }

    async fn acquire_inner(
        &self,
    ) -> anyhow::Result<(Arc<InnerLocalNodeExecutor>, ActiveRequestGuard, bool)> {
        {
            let state = self.state.lock().await;
            anyhow::ensure!(
                !self.shutting_down.load(Ordering::Acquire),
                "Local Node executor is shutting down"
            );
            if let Some(inner) = &state.inner {
                let inner = inner.clone();
                let request_guard = ActiveRequestGuard::new(inner.clone());
                return Ok((inner, request_guard, false));
            }
        }

        // Child startup can take several health-check intervals. Serialize that
        // work separately so late failures from the retired generation can
        // still inspect the generation slot without waiting for its replacement.
        let _startup_guard = self.startup_lock.lock().await;
        let (generation, replaces_generation) = {
            let mut state = self.state.lock().await;
            anyhow::ensure!(
                !self.shutting_down.load(Ordering::Acquire),
                "Local Node executor is shutting down"
            );
            if let Some(inner) = &state.inner {
                let inner = inner.clone();
                let request_guard = ActiveRequestGuard::new(inner.clone());
                return Ok((inner, request_guard, false));
            }
            state.next_generation = state
                .next_generation
                .checked_add(1)
                .expect("Local Node executor generation overflow");
            (state.next_generation, state.replacement_for_generation)
        };

        let replacement_started = Instant::now();
        let replacement = match InnerLocalNodeExecutor::new(generation, &self.config).await {
            Ok(replacement) => Arc::new(replacement),
            Err(error) => {
                if let Some(replaces_generation) = replaces_generation {
                    crate::metrics::log_local_node_replacement_outcome("startup_failed");
                    tracing::warn!(
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
                crate::metrics::log_local_node_replacement_outcome("aborted_shutdown");
                tracing::info!(
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
                crate::metrics::log_local_node_replacement_outcome("aborted_shutdown");
                tracing::info!(
                    generation,
                    replaces_generation,
                    "Discarding replacement local Node executor generation during shutdown"
                );
            }
            replacement.terminate().await?;
            anyhow::bail!("Local Node executor is shutting down");
        }
        assert!(state.inner.is_none());
        assert_eq!(state.replacement_for_generation, replaces_generation);
        state.inner = Some(replacement.clone());
        state.replacement_for_generation = None;
        crate::metrics::set_local_node_generation_present(true);
        crate::metrics::set_local_node_generation_age(Duration::ZERO);
        crate::metrics::set_local_node_consecutive_health_misses(0);
        if replacement.runtime_stats_supported {
            crate::metrics::set_local_node_package_state(0, 0, 0, 0, 0, 0);
        }
        crate::metrics::log_local_node_generation_start();
        let startup_elapsed = replacement_started.elapsed();
        if replaces_generation.is_some() {
            crate::metrics::log_local_node_replacement_time(startup_elapsed);
            crate::metrics::log_local_node_replacement_outcome("ready");
        }
        tracing::info!(
            generation,
            replacement = replaces_generation.is_some(),
            replaces_generation = ?replaces_generation,
            runtime_stats_supported = replacement.runtime_stats_supported,
            startup_seconds = startup_elapsed.as_secs_f64(),
            "Started local Node executor generation"
        );
        let request_guard = ActiveRequestGuard::new(replacement.clone());
        Ok((replacement, request_guard, true))
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
        let retired = {
            let mut state = state.lock().await;
            if state
                .inner
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, expected))
            {
                // A late result from an old generation cannot retire its replacement.
                state.inner.take();
                state.replacement_for_generation =
                    (!matches!(reason, GenerationRetirementReason::ExplicitShutdown))
                        .then_some(expected.generation);
                crate::metrics::set_local_node_generation_present(false);
                crate::metrics::set_local_node_generation_age(Duration::ZERO);
                crate::metrics::set_local_node_consecutive_health_misses(0);
                if expected.runtime_stats_supported {
                    crate::metrics::set_local_node_package_state(0, 0, 0, 0, 0, 0);
                }
                crate::metrics::log_local_node_generation_retirement(reason.as_str());
                crate::metrics::log_local_node_retirement_diagnostics(
                    reason.as_str(),
                    diagnostics.request_kind,
                    diagnostics.phase,
                    diagnostics.transport_error_kind,
                );
                if matches!(reason, GenerationRetirementReason::ExplicitShutdown) {
                    tracing::info!(
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
                        last_observed_registered_stack_roots =
                            expected.registered_stack_roots.load(Ordering::Relaxed),
                        "Retiring local Node executor generation"
                    );
                } else {
                    tracing::warn!(
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
        let expected = expected.clone();
        let generation = expected.generation;
        let termination = tokio::spawn(async move {
            let result = expected.terminate().await;
            match &result {
                Ok(observation) => {
                    crate::metrics::log_local_node_child_termination(
                        reason.as_str(),
                        observation.state_before,
                        observation.supervisor_kill_requested,
                        observation.exit_class,
                    );
                    tracing::info!(
                        generation,
                        reason = reason.as_str(),
                        state_before = observation.state_before,
                        supervisor_kill_requested = observation.supervisor_kill_requested,
                        exit_class = observation.exit_class,
                        "Completed local Node executor child termination"
                    );
                },
                Err(_) => {
                    tracing::error!(
                        generation,
                        reason = reason.as_str(),
                        "Failed to terminate and reap local Node executor child"
                    );
                },
            }
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
            Err(_) => anyhow::bail!("Local Node executor child termination task failed"),
        }
        Ok(true)
    }

    fn spawn_watchdog(&self, inner: &Arc<InnerLocalNodeExecutor>) {
        let state = Arc::downgrade(&self.state);
        let expected = Arc::downgrade(inner);
        let config = self.config.clone();
        tokio::spawn(async move {
            Self::watch_generation(state, expected, config).await;
        });
    }

    async fn watch_generation(
        state: Weak<Mutex<LocalNodeExecutorState>>,
        expected: Weak<InnerLocalNodeExecutor>,
        config: LocalNodeExecutorConfig,
    ) {
        let mut consecutive_misses = 0;
        let mut previous_package_stats = NodePackageCacheStats::default();
        let mut previous_stack_stats = NodeStackTraceStats::default();
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

            let health_check_started = Instant::now();
            let health = InnerLocalNodeExecutor::check_server_health(
                &expected.client,
                config.health_check_timeout,
            )
            .await;
            let success = health.as_ref().is_some_and(|health| {
                health.status == "ok"
                    && health
                        .valid_runtime_stats_support(&previous_package_stats, &previous_stack_stats)
                        == Some(expected.runtime_stats_supported)
            });
            let health_check_elapsed = health_check_started.elapsed();

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
            crate::metrics::log_local_node_health_check(health_check_elapsed, "watchdog", success);
            crate::metrics::set_local_node_generation_age(expected.started_at.elapsed());

            if let Some(health) = health.filter(|_| success) {
                consecutive_misses = 0;
                crate::metrics::set_local_node_consecutive_health_misses(0);
                if expected.runtime_stats_supported {
                    Self::record_node_health_metrics(
                        &expected,
                        health,
                        &mut previous_package_stats,
                        &mut previous_stack_stats,
                    );
                }
                continue;
            }

            consecutive_misses += 1;
            crate::metrics::set_local_node_consecutive_health_misses(consecutive_misses);
            let should_retire = consecutive_misses >= config.watchdog_failure_threshold;
            drop(current_state);
            if should_retire {
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
                        generation = expected.generation,
                        "Failed to terminate and reap unhealthy local Node executor child"
                    );
                }
                return;
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
            .registered_stack_roots
            .store(stack.registered_roots, Ordering::Relaxed);
        crate::metrics::set_local_node_package_state(
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
                package_kind,
                operation,
                current - previous,
            );
        }

        crate::metrics::log_local_node_stack_format_deltas(
            stack.invocations - previous_stack_stats.invocations,
            stack.frames_processed - previous_stack_stats.frames_processed,
            stack.duration_ms - previous_stack_stats.duration_ms,
        );
        *previous_package_stats = package;
        *previous_stack_stats = stack;
    }
}

#[async_trait]
impl NodeExecutor for LocalNodeExecutor {
    fn enable(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn invoke(
        &self,
        request: ExecutorRequest,
        log_line_sender: mpsc::UnboundedSender<LogLine>,
    ) -> anyhow::Result<InvokeResponse> {
        anyhow::ensure!(
            !self.shutting_down.load(Ordering::Acquire),
            "Local Node executor is shutting down"
        );
        let request_kind = request.kind();
        let request_json = JsonValue::try_from(request)?;
        let waiting_guard = WaitingRequestGuard::new();
        let (inner, mut request_guard, created) = self.acquire_inner().await?;
        waiting_guard.finish();
        if created {
            self.spawn_watchdog(&inner);
        }
        let client = inner.client.clone();

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

    fn shutdown(&self) {
        if self.shutting_down.swap(true, Ordering::AcqRel) {
            return;
        }
        let state = self.state.clone();
        tokio::spawn(async move {
            let Some(expected) = state.lock().await.inner.clone() else {
                return;
            };
            if Self::retire_inner_state(
                &state,
                &expected,
                GenerationRetirementDiagnostics::shutdown(),
            )
            .await
            .is_err()
            {
                // Shutdown is a synchronous trait boundary. Report the
                // bounded cleanup failure after the slot transition.
                tracing::error!(
                    generation = expected.generation,
                    "Failed to terminate and reap local Node executor child during shutdown"
                );
            }
        });
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        future,
        os::unix::fs::PermissionsExt,
    };

    use futures::future::join_all;
    use tokio::{
        io::{
            AsyncReadExt,
            AsyncWriteExt,
        },
        net::UnixListener,
    };

    use super::*;

    fn test_config() -> LocalNodeExecutorConfig {
        LocalNodeExecutorConfig {
            node_process_timeout: Duration::from_secs(1),
            callback_initial_backoff: None,
            health_check_timeout: Duration::from_millis(10),
            watchdog_interval: Duration::from_millis(10),
            watchdog_failure_threshold: 2,
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
    }

    #[tokio::test]
    async fn node_version_probe_stops_reading_oversized_output() {
        let temp_dir = TempDir::new().unwrap();
        let node_path = temp_dir.path().join("node");
        fs::write(
            &node_path,
            r#"#!/bin/sh
printf 'v22.'
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
        let source_dir = TempDir::new().unwrap();
        let server_handle = TokioCommand::new("sleep")
            .arg("300")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        Arc::new(InnerLocalNodeExecutor {
            generation,
            started_at: Instant::now(),
            runtime_stats_supported: false,
            active_requests: AtomicUsize::new(0),
            retained_source_packages: AtomicU64::new(0),
            retained_external_packages: AtomicU64::new(0),
            registered_stack_roots: AtomicU64::new(0),
            server_handle: Mutex::new(ManagedChild::new(generation, server_handle, source_dir)),
            client,
        })
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
        let server_handle = ManagedChild::new(1, server_handle, source_dir);

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
        drop(ReapingTempDir::new(1, retained_dir));
        assert!(retained_path.exists());
        fs::remove_dir_all(retained_path).unwrap();

        let removed_dir = TempDir::new().unwrap();
        let removed_path = removed_dir.path().to_owned();
        ReapingTempDir::new(2, removed_dir).remove_after_reaping();
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

        let observation = InnerLocalNodeExecutor::terminate_child(1, &mut child)
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

        let observation = InnerLocalNodeExecutor::terminate_child(2, &mut child)
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
        let state = Arc::new(Mutex::new(LocalNodeExecutorState {
            next_generation: inner.generation,
            inner: Some(inner),
            replacement_for_generation: None,
        }));
        let executor = LocalNodeExecutor {
            state: state.clone(),
            startup_lock: Mutex::new(()),
            shutting_down: AtomicBool::new(false),
            config,
        };
        (executor, state)
    }

    #[tokio::test]
    async fn late_old_generation_retirement_preserves_replacement() {
        let old = test_inner(1).await;
        let replacement = test_inner(2).await;
        let state = Arc::new(Mutex::new(LocalNodeExecutorState {
            inner: Some(old.clone()),
            replacement_for_generation: None,
            next_generation: 2,
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
            replacement_for_generation: None,
            next_generation: 1,
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
        let state = Arc::new(Mutex::new(LocalNodeExecutorState {
            inner: Some(generation.clone()),
            replacement_for_generation: None,
            next_generation: 1,
        }));

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
        retirement_task.abort();
        assert!(retirement_task.await.unwrap_err().is_cancelled());
        drop(child_guard);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if generation.server_handle.lock().await.child.is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
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
            replacement_for_generation: None,
            next_generation: 1,
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
}
