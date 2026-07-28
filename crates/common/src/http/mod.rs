use std::{
    borrow::Cow,
    convert::Infallible,
    fmt,
    future::Future,
    net::SocketAddr,
    ops::Deref,
    pin::Pin,
    str::{
        self,
        FromStr,
    },
    sync::{
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
        Arc,
        LazyLock,
    },
    time::{
        Duration,
        Instant,
    },
};

use ::metrics::{
    CONVEX_METRICS_REGISTRY,
    SERVER_VERSION_STR,
    SERVICE_NAME,
};
use anyhow::Context;
use axum::{
    body::Body,
    error_handling::HandleErrorLayer,
    extract::{
        connect_info::IntoMakeServiceWithConnectInfo,
        rejection::ExtensionRejection,
        FromRequestParts,
        OptionalFromRequestParts,
        State,
    },
    response::{
        IntoResponse,
        Response,
    },
    routing::get,
    BoxError,
    RequestPartsExt,
    Router,
    ServiceExt,
};
use axum_extra::extract::Host;
use bytes::Bytes;
use errors::{
    ErrorMetadata,
    ErrorMetadataAnyhowExt,
};
use fastrace::{
    future::FutureExt as _,
    prelude::SpanContext,
    Span,
};
use futures::{
    stream::BoxStream,
    Stream,
    StreamExt,
};
use http::{
    header::{
        HeaderName,
        HeaderValue,
    },
    HeaderMap,
    Method,
    StatusCode,
    Uri,
};
use http_body_util::BodyExt;
use itertools::Itertools;
use prometheus::{
    PullingGauge,
    TextEncoder,
};
use regex::Regex;
use sentry::integrations::tower as sentry_tower;
use serde::{
    Deserialize,
    Serialize,
};
use tokio::net::TcpSocket;
use tower::{
    timeout::TimeoutLayer,
    Layer,
    Service,
    ServiceBuilder,
};
use tower_http::cors::{
    AllowHeaders,
    AllowOrigin,
    CorsLayer,
};
use url::Url;
use utoipa::ToSchema;

use self::metrics::log_http_request;
use crate::{
    dyn_event,
    errors::report_error_sync,
    execution_context::{
        ClientIp,
        ClientUserAgent,
    },
    knobs::{
        DISABLE_METRICS_ENDPOINT,
        HTTP_SERVER_TCP_BACKLOG,
        PROPAGATE_UPSTREAM_TRACES,
    },
    metrics::{
        log_client_version_unsupported,
        log_http_service_max_concurrent_requests,
    },
    runtime::TaskManager,
    version::{
        ClientVersion,
        ClientVersionState,
    },
    RequestId,
    RequestMetadata,
};

pub mod extract;
pub mod fetch;
pub mod fork_of_axum_serve;
pub mod websocket;

const MAX_HTTP2_STREAMS: u32 = 1024;

pub use sync_types::headers::{
    DEPRECATION_MSG_HEADER_NAME,
    DEPRECATION_STATE_HEADER_NAME,
};
use value::heap_size::HeapSize;

/// Header carrying the authenticated token on Node action callback requests.
pub const CONVEX_ACTIONS_CALLBACK_TOKEN: &str = "Convex-Action-Callback-Token";

mod metrics {
    use std::time::Duration;

    use metrics::{
        add_to_gauge_with_labels,
        log_counter_with_labels,
        log_distribution_with_labels,
        log_gauge_with_labels,
        register_convex_counter,
        register_convex_gauge,
        register_convex_histogram,
        subtract_from_gauge_with_labels,
        MetricLabel,
        StaticMetricLabel,
    };

    register_convex_histogram!(
        HTTP_HANDLE_DURATION_SECONDS,
        "Time to handle an HTTP request",
        &["endpoint", "method", "status", "client_version", "is_test"]
    );

    pub fn log_http_request(
        client_version: &str,
        route: &str,
        method: &str,
        status: &str,
        duration: Duration,
        is_test: bool,
    ) {
        // There are a lot of labels in here and some (e.g., client_version) are
        // pretty high cardinality. If this gets too expensive we can split out
        // separate logging just for client version.
        let labels = vec![
            MetricLabel::new("endpoint", route),
            MetricLabel::new("method", method),
            MetricLabel::new("status", status),
            MetricLabel::new("client_version", client_version),
            MetricLabel::new("is_test", is_test.to_string()),
        ];
        log_distribution_with_labels(
            &HTTP_HANDLE_DURATION_SECONDS,
            duration.as_secs_f64(),
            labels,
        );
    }

    register_convex_gauge!(
        HTTP_ADMISSION_WAITERS_INFO,
        "Number of HTTP requests waiting for service admission",
        &["service_name", "is_dependency"]
    );
    register_convex_histogram!(
        HTTP_ADMISSION_WAIT_SECONDS,
        "Time HTTP requests spent waiting for service admission",
        &["service_name", "is_dependency"]
    );
    register_convex_counter!(
        HTTP_MEMORY_PRESSURE_SHED_TOTAL,
        "External HTTP requests rejected before handling because backend cgroup memory headroom \
         was low",
        &["service_name"]
    );

    fn http_admission_labels(
        service_name: &'static str,
        is_dependency: &'static str,
    ) -> Vec<StaticMetricLabel> {
        vec![
            StaticMetricLabel::new("service_name", service_name),
            StaticMetricLabel::new("is_dependency", is_dependency),
        ]
    }

    pub fn initialize_http_admission_metrics(service_name: &'static str) {
        for is_dependency in ["false", "true"] {
            log_gauge_with_labels(
                &HTTP_ADMISSION_WAITERS_INFO,
                0.0,
                http_admission_labels(service_name, is_dependency),
            );
        }
        log_counter_with_labels(
            &HTTP_MEMORY_PRESSURE_SHED_TOTAL,
            0,
            vec![StaticMetricLabel::new("service_name", service_name)],
        );
    }

    pub fn log_http_memory_pressure_shed(service_name: &'static str) {
        log_counter_with_labels(
            &HTTP_MEMORY_PRESSURE_SHED_TOTAL,
            1,
            vec![StaticMetricLabel::new("service_name", service_name)],
        );
    }

    #[cfg(test)]
    pub fn http_admission_waiters(service_name: &'static str, is_dependency: &'static str) -> f64 {
        HTTP_ADMISSION_WAITERS_INFO
            .with_label_values(&[service_name, is_dependency])
            .get()
    }

    #[cfg(test)]
    pub fn http_admission_wait_observations(
        service_name: &'static str,
        is_dependency: &'static str,
    ) -> u64 {
        use prometheus::core::Metric as _;

        HTTP_ADMISSION_WAIT_SECONDS
            .with_label_values(&[service_name, is_dependency])
            .metric()
            .get_vm_histogram()
            .sample_count()
    }

    pub struct HttpAdmissionWaitGuard {
        service_name: &'static str,
        is_dependency: &'static str,
        started: std::time::Instant,
    }

    impl HttpAdmissionWaitGuard {
        pub fn new(service_name: &'static str, is_dependency: bool) -> Self {
            let is_dependency = if is_dependency { "true" } else { "false" };
            let labels = http_admission_labels(service_name, is_dependency);
            add_to_gauge_with_labels(&HTTP_ADMISSION_WAITERS_INFO, 1.0, labels);
            Self {
                service_name,
                is_dependency,
                started: std::time::Instant::now(),
            }
        }

        fn labels(&self) -> Vec<StaticMetricLabel> {
            http_admission_labels(self.service_name, self.is_dependency)
        }
    }

    impl Drop for HttpAdmissionWaitGuard {
        fn drop(&mut self) {
            subtract_from_gauge_with_labels(&HTTP_ADMISSION_WAITERS_INFO, 1.0, self.labels());
            log_distribution_with_labels(
                &HTTP_ADMISSION_WAIT_SECONDS,
                self.started.elapsed().as_secs_f64(),
                self.labels(),
            );
        }
    }
}

#[allow(clippy::declare_interior_mutable_const)]
pub const APPLICATION_JSON_CONTENT_TYPE: HeaderValue = HeaderValue::from_static("application/json");

#[derive(Debug, PartialEq, Eq)]
pub struct HttpRequest {
    pub headers: HeaderMap,
    pub url: Url,
    pub method: Method,
    pub body: Option<Bytes>,
}

impl From<HttpRequest> for HttpRequestStream {
    fn from(value: HttpRequest) -> Self {
        let body = value.body.map(|b| {
            Box::pin(futures::stream::once(async move { Ok(b) }))
                as Pin<Box<dyn Stream<Item = anyhow::Result<bytes::Bytes>> + Sync + Send>>
        });

        Self {
            headers: value.headers,
            url: value.url,
            method: value.method,
            body,
            // This kind of HttpRequest can't be aborted.
            signal: Box::pin(futures::future::pending()),
        }
    }
}

impl HeapSize for HttpRequest {
    fn heap_size(&self) -> usize {
        // Assume heap size is dominated by body (because the rest is annoying
        // to calculate).
        self.body.as_ref().map_or(0, |body| body.len())
    }
}

pub struct HttpRequestStream {
    pub headers: HeaderMap,
    pub url: Url,
    pub method: Method,
    /// The request body, or `None` for a body-less request such as a plain GET.
    /// This distinction must be preserved end-to-end: a body-less request has
    /// to be sent without a body so it is framed correctly over HTTP/2 (see
    /// the `fetch` impl in `fetch.rs`). It deliberately cannot be recovered
    /// by inspecting the stream, because the stream may be a full-duplex
    /// body whose first chunk is only produced after the response headers
    /// arrive.
    pub body: Option<Pin<Box<dyn Stream<Item = anyhow::Result<bytes::Bytes>> + Sync + Send>>>,
    pub signal: Pin<Box<dyn Future<Output = ()> + Sync + Send + 'static>>,
}

impl std::fmt::Debug for HttpRequestStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpRequestStream")
            .field("headers", &self.headers)
            .field("url", &self.url)
            .field("method", &self.method)
            .finish()
    }
}

// Components can mount other components' HTTP routers, so a child component's
// HTTP router may receive a different path from the original HTTP request. For
// example, let's say we mount a rate limiter's router at `/rl/` and the rate
// limiter has a route for "/index.html". Then, an incoming request for
// `/rl/index.html` will get passed to the rate limiter's router with the routed
// path of `/index.html`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedHttpPath(pub String);

impl Deref for RoutedHttpPath {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct HttpResponse {
    pub body: Option<Vec<u8>>,
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub url: Option<Url>,
    pub request_size: u64,
}

impl HttpResponse {
    pub fn new(
        status: StatusCode,
        headers: HeaderMap,
        body: Option<Vec<u8>>,
        url: Option<Url>,
        request_size: u64,
    ) -> Self {
        Self {
            body,
            status,
            headers,
            url,
            request_size,
        }
    }
}

impl From<HttpResponse> for HttpResponseStream {
    fn from(value: HttpResponse) -> Self {
        Self {
            body: value
                .body
                .map(|b| futures::stream::once(async move { Ok(bytes::Bytes::from(b)) }).boxed()),
            status: value.status,
            headers: value.headers,
            url: value.url,
            request_size: Arc::new(AtomicU64::new(value.request_size)),
        }
    }
}

pub struct HttpResponseStream {
    pub body: Option<BoxStream<'static, anyhow::Result<bytes::Bytes>>>,
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub url: Option<Url>,
    pub request_size: Arc<AtomicU64>,
}

impl HttpResponseStream {
    pub async fn into_http_response(self) -> anyhow::Result<HttpResponse> {
        let body = if let Some(mut body) = self.body {
            let mut bytes = vec![];
            while let Some(chunk) = body.next().await.transpose()? {
                bytes.append(&mut chunk.to_vec());
            }
            Some(bytes)
        } else {
            None
        };

        Ok(HttpResponse {
            body,
            status: self.status,
            headers: self.headers,
            url: self.url,
            request_size: self.request_size.load(Ordering::Relaxed),
        })
    }
}

/// `HttpError` is used as a vehicle for getting client facing error messages
/// to clients on the HTTP protocol. Errors that are tagged with ErrorMetadata
/// can be used to build these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpError {
    /// HTTP Status Code
    status_code: StatusCode,
    /// Human-readable error code sent in HTTP response
    error_code: Cow<'static, str>,
    /// Detailed customer-facing error message sent in HTTP response
    msg: Cow<'static, str>,
}

impl HttpError {
    pub fn new<S, T>(status_code: StatusCode, error_code: S, msg: T) -> Self
    where
        S: Into<Cow<'static, str>>,
        T: Into<Cow<'static, str>>,
    {
        Self {
            status_code,
            error_code: error_code.into(),
            msg: msg.into(),
        }
    }

    pub fn status_code(&self) -> StatusCode {
        self.status_code
    }

    pub fn error_code(&self) -> &str {
        &self.error_code
    }

    pub fn message(&self) -> &str {
        &self.msg
    }

    pub fn into_response(self) -> Response {
        if self.msg.is_empty() && self.error_code.is_empty() {
            self.status_code.into_response()
        } else {
            (
                self.status_code,
                extract::Json(ResponseErrorMessage {
                    code: self.error_code,
                    message: self.msg,
                }),
            )
                .into_response()
        }
    }

    pub fn error_message_from_bytes(
        bytes: &[u8],
    ) -> anyhow::Result<(Cow<'static, str>, Cow<'static, str>)> {
        let ResponseErrorMessage { code, message } =
            serde_json::from_slice(bytes).with_context(|| {
                format!(
                    "Couldn't deserialize as json: {}",
                    String::from_utf8_lossy(bytes)
                )
            })?;

        Ok((code, message))
    }

    pub async fn from_response(response: Response) -> anyhow::Result<Self> {
        let (parts, body) = response.into_parts();
        let (code, message) = Self::error_message_from_bytes(
            &body
                .collect()
                .await
                .expect("Couldn't collect body")
                .to_bytes(),
        )?;

        Ok(Self {
            status_code: parts.status,
            error_code: code,
            msg: message,
        })
    }
}

/// `HttpResponseError` is used to convert `anyhow::Error` (and
/// `HttpError` inside it if present) into `http::Response` that is returned
/// from the HTTP middleware. All HTTP handlers should return
/// `HttpResponseError`s. Sentry errors are captured in the `IntoResponse` impl,
/// the exit point of the HTTP middleware.
#[derive(Debug)]
pub struct HttpResponseError {
    trace: anyhow::Error,
    http_error: HttpError,
}

impl From<Infallible> for HttpResponseError {
    fn from(x: Infallible) -> Self {
        match x {}
    }
}

#[derive(Serialize, Deserialize)]
struct ResponseErrorMessage {
    code: Cow<'static, str>,
    message: Cow<'static, str>,
}

impl IntoResponse for HttpResponseError {
    fn into_response(mut self) -> Response {
        // This is the only place we capture errors to sentry because it is the exit
        // point of the HTTP layer
        report_error_sync(&mut self.trace);
        self.http_error.into_response()
    }
}

impl From<anyhow::Error> for HttpResponseError {
    fn from(err: anyhow::Error) -> HttpResponseError {
        let http_error = HttpError {
            status_code: err.http_status(),
            error_code: err.short_msg().to_string().into(),
            msg: err.msg().to_string().into(),
        };
        Self {
            trace: err,
            http_error,
        }
    }
}

impl From<HttpResponseError> for anyhow::Error {
    fn from(value: HttpResponseError) -> Self {
        value.trace
    }
}

pub trait RouteMapper: Send + Sync + Clone + 'static {
    fn map_route(&self, route: String) -> String;
}

#[derive(Clone)]
pub struct NoopRouteMapper;

impl RouteMapper for NoopRouteMapper {
    fn map_route(&self, route: String) -> String {
        route
    }
}

fn concurrency_metric_name(service_name: &str, namespace: &str) -> String {
    let name = format!(
        "{}_http_service_concurrent_requests",
        service_name.replace('-', "_")
    );
    // The registry already prefixes metrics with the executable name.
    name.strip_prefix(&format!("{namespace}_"))
        .unwrap_or(&name)
        .to_owned()
}

/// Router + Middleware for a Convex service
pub struct ConvexHttpService {
    router: Router,
    meta_routes_enabled: bool,
    version: String,
    service_name: &'static str,
    _concurrency_gauge: Option<PullingGauge>,
    _base_concurrency_gauge: Option<PullingGauge>,
}

/// Shared state used by a host-specific controller to shed new external HTTP
/// work. The HTTP layer owns request classification; the controller owns only
/// the current pressure state.
#[derive(Clone)]
pub struct ExternalRequestShedding {
    active: Arc<AtomicBool>,
}

impl ExternalRequestShedding {
    pub fn new(active: bool) -> Self {
        Self {
            active: Arc::new(AtomicBool::new(active)),
        }
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Returns the prior state so the controller can record transitions.
    pub fn set_active(&self, active: bool) -> bool {
        self.active.swap(active, Ordering::AcqRel)
    }
}

#[derive(Clone)]
struct HttpConcurrencyLimiter {
    gate: crate::dependency_overflow::DependencyOverflowGate,
    dependency_path_prefixes: &'static [&'static str],
    external_request_shedding: Option<ExternalRequestShedding>,
    service_name: &'static str,
}

fn memory_pressure_response(service_name: &'static str) -> Response {
    metrics::log_http_memory_pressure_shed(service_name);
    let mut response = HttpError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "BackendMemoryPressure",
        "The backend is temporarily limiting new work because memory headroom is low. Retry the \
         request.",
    )
    .into_response();
    response
        .headers_mut()
        .insert(http::header::RETRY_AFTER, HeaderValue::from_static("1"));
    response
}

async fn dependency_aware_concurrency_middleware(
    State(limiter): State<HttpConcurrencyLimiter>,
    request: http::Request<Body>,
    next: axum::middleware::Next,
) -> Response {
    // Every callback can be waiting behind the HTTP permit retained by its
    // parent Node action request. Isolate-worker ancestry remains a separate
    // downstream classification carried by a different request header.
    let is_dependency = limiter
        .dependency_path_prefixes
        .iter()
        .any(|prefix| request.uri().path().starts_with(prefix))
        && request
            .headers()
            .contains_key(CONVEX_ACTIONS_CALLBACK_TOKEN);
    if !is_dependency
        && limiter
            .external_request_shedding
            .as_ref()
            .is_some_and(ExternalRequestShedding::is_active)
    {
        return memory_pressure_response(limiter.service_name);
    }
    let permit = limiter
        .gate
        .acquire_with_waiter(is_dependency, || {
            metrics::HttpAdmissionWaitGuard::new(limiter.service_name, is_dependency)
        })
        .await;
    // A request can wait for a normal concurrency permit while memory pressure
    // begins. Recheck before running the handler so queued external work does
    // not bypass an active shedding interval.
    if !is_dependency
        && limiter
            .external_request_shedding
            .as_ref()
            .is_some_and(ExternalRequestShedding::is_active)
    {
        drop(permit);
        return memory_pressure_response(limiter.service_name);
    }
    let response = next.run(request).await;
    drop(permit);
    response
}

impl ConvexHttpService {
    pub fn new<RM: RouteMapper>(
        router: Router,
        service_name: &'static str,
        version: String,
        max_concurrency: usize,
        request_timeout: Duration,
        route_metric_mapper: RM,
    ) -> Self {
        Self::new_with_dependency_reserve(
            router,
            service_name,
            version,
            max_concurrency,
            0,
            &[],
            None,
            request_timeout,
            route_metric_mapper,
        )
    }

    pub fn new_with_dependency_reserve<RM: RouteMapper>(
        router: Router,
        service_name: &'static str,
        version: String,
        max_concurrency: usize,
        dependency_reserve: usize,
        dependency_path_prefixes: &'static [&'static str],
        external_request_shedding: Option<ExternalRequestShedding>,
        request_timeout: Duration,
        route_metric_mapper: RM,
    ) -> Self {
        assert!(
            max_concurrency <= tokio::sync::Semaphore::MAX_PERMITS,
            "HTTP concurrency must not exceed Tokio's supported semaphore capacity"
        );
        assert!(
            dependency_reserve < max_concurrency,
            "HTTP dependency reserve must be smaller than total concurrency"
        );
        let sentry_layer = ServiceBuilder::new()
            .layer(sentry_tower::NewSentryLayer::<_>::new_from_top())
            .layer(sentry_tower::SentryHttpLayer::new());
        metrics::initialize_http_admission_metrics(service_name);
        log_http_service_max_concurrent_requests(service_name, max_concurrency);
        let base_capacity = max_concurrency - dependency_reserve;
        let concurrency_gate =
            crate::dependency_overflow::DependencyOverflowGate::new(base_capacity, max_concurrency);
        let concurrency_gate_for_base_gauge = concurrency_gate.clone();
        let base_concurrency_gauge = (dependency_reserve > 0).then(|| {
            PullingGauge::new(
                format!(
                    "{}_http_service_base_concurrent_requests",
                    service_name.replace('-', "_")
                ),
                "The amount of shared base HTTP admission capacity in use",
                Box::new(move || concurrency_gate_for_base_gauge.base_in_use() as f64),
            )
            .expect("Invalid base concurrency gauge initialization")
        });
        if let Some(gauge) = &base_concurrency_gauge
            && let Err(e) = CONVEX_METRICS_REGISTRY.register(Box::new(gauge.clone()))
        {
            tracing::error!(
                "Failed to register base request concurrency gauge for {service_name}: {e}"
            );
        }
        let concurrency_gate_for_total_gauge = concurrency_gate.clone();
        let concurrency_limiter = HttpConcurrencyLimiter {
            gate: concurrency_gate,
            dependency_path_prefixes,
            external_request_shedding,
            service_name,
        };
        let concurrency_gauge = PullingGauge::new(
            concurrency_metric_name(service_name, &SERVICE_NAME),
            "The number of currently outstanding requests on the ConvexHttpService",
            Box::new(move || concurrency_gate_for_total_gauge.active() as f64),
        )
        .expect("Invalid gauge initialization");
        if let Err(e) = CONVEX_METRICS_REGISTRY.register(Box::new(concurrency_gauge.clone())) {
            tracing::error!("Failed to register request concurrency gauge for {service_name}: {e}");
        }

        let router = router
            .layer(
                ServiceBuilder::new()
                    // Admission stays outside request instrumentation and timeout
                    // so queued requests do not consume their budgets before starting.
                    .layer(axum::middleware::from_fn_with_state(
                        concurrency_limiter,
                        dependency_aware_concurrency_middleware,
                    ))
                    // Order important. Log/stats first because they are infallible.
                    .layer(axum::middleware::from_fn(tokio_instrumentation_middleware))
                    .layer(axum::middleware::from_fn(log_middleware))
                    .layer(axum::middleware::from_fn_with_state(
                        route_metric_mapper,
                        stats_middleware::<RM>,
                    ))
                    .layer(axum::middleware::from_fn(client_version_state_middleware))
                    .layer(tower_cookies::CookieManagerLayer::new())
                    .layer(HandleErrorLayer::new(|_: BoxError| async {
                        StatusCode::REQUEST_TIMEOUT
                    }))
                    .layer(TimeoutLayer::new(request_timeout)),
            )
            .layer(sentry_layer);

        Self {
            router,
            version,
            _concurrency_gauge: Some(concurrency_gauge),
            _base_concurrency_gauge: base_concurrency_gauge,
            service_name,
            meta_routes_enabled: true,
        }
    }

    pub fn set_meta_routes_enabled(&mut self, enabled: bool) {
        self.meta_routes_enabled = enabled;
    }

    /// Routes not handled by the passed-in router.
    fn meta_routes(&self) -> Router {
        let version = self.version.clone();
        Router::new()
            .route("/version", get(move || async move { version }))
            .route("/metrics", get(metrics))
    }

    pub async fn serve<F: Future<Output = ()> + Send + 'static>(
        self,
        addr: impl MakeSocket,
        shutdown: F,
    ) -> anyhow::Result<()> {
        let extra = self.meta_routes();
        let mut router = self.router;
        if self.meta_routes_enabled {
            router = router.merge(extra);
        }
        let make_svc = router.into_make_service_with_connect_info::<SocketAddr>();
        let socket = addr.make_socket()?;
        let local_addr = socket.local_addr()?;
        tracing::info!("{} listening on {local_addr}", self.service_name);
        serve_http(make_svc, socket, shutdown)
            .await
            .with_context(|| format!("Could not start {} on {local_addr}", self.service_name))
    }

    /// Apply `middleware_fn` to incoming requests *before* passing them to
    /// the router. Because the middleware is applied before routing, it is
    /// allowed to change the request URI and affect which route will be
    /// matched.
    pub async fn serve_with_middleware<F, Fut, Rejection>(
        self,
        addr: impl MakeSocket,
        shutdown: F,
        middleware_fn: impl FnMut(http::Request<Body>) -> Fut + Clone + Send + Sync + 'static,
    ) -> anyhow::Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
        Fut: Future<Output = Result<http::Request<Body>, Rejection>> + Send + 'static,
        Rejection: IntoResponse + Send + 'static,
    {
        let middleware = axum::middleware::map_request(middleware_fn);
        let meta_router = self.meta_routes();
        let wrapped_svc = middleware.layer(self.router);

        let socket = addr.make_socket()?;
        let local_addr = socket.local_addr()?;
        tracing::info!("{} listening on {local_addr}", self.service_name);
        if self.meta_routes_enabled {
            // Fall back to the middleware-wrapped service if the request doesn't match the
            // meta router.
            serve_http(
                meta_router
                    .fallback_service(wrapped_svc)
                    .into_make_service_with_connect_info::<SocketAddr>(),
                socket,
                shutdown,
            )
            .await
        } else {
            // If we're not serving meta routes, simply serve the middleware-wrapped service
            serve_http(
                wrapped_svc.into_make_service_with_connect_info::<SocketAddr>(),
                socket,
                shutdown,
            )
            .await
        }
    }
}

/// Serves an HTTP server using the given service.
pub async fn serve_http<F, R>(
    make_service: IntoMakeServiceWithConnectInfo<R, SocketAddr>,
    addr: impl MakeSocket,
    shutdown: F,
) -> anyhow::Result<()>
where
    R: Service<http::Request<Body>, Response = Response, Error = Infallible>
        + Send
        + Clone
        + 'static,
    <R as Service<http::Request<Body>>>::Future: Send,
    F: Future<Output = ()> + Send + 'static,
{
    let listener = addr.make_socket()?.listen(*HTTP_SERVER_TCP_BACKLOG)?;
    fork_of_axum_serve::serve(listener, make_service)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

pub fn server_socket(addr: SocketAddr) -> anyhow::Result<TcpSocket> {
    // Set SO_REUSEADDR and a bounded TCP accept backlog for our server's listening
    // socket.
    let socket = TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    // Set TCP_NODELAY on accepted connections.
    socket.set_nodelay(true)?;
    socket.bind(addr)?;
    Ok(socket)
}

pub trait MakeSocket {
    fn make_socket(self) -> anyhow::Result<TcpSocket>;
}

impl MakeSocket for SocketAddr {
    fn make_socket(self) -> anyhow::Result<TcpSocket> {
        server_socket(self)
    }
}

impl MakeSocket for TcpSocket {
    fn make_socket(self) -> anyhow::Result<TcpSocket> {
        Ok(self)
    }
}

async fn client_version_state_middleware(
    ExtractClientVersion(client_version): ExtractClientVersion,
    req: http::request::Request<Body>,
    next: axum::middleware::Next,
) -> Result<Response, HttpResponseError> {
    let version_state = client_version.current_state();

    let mut resp = match &version_state {
        ClientVersionState::Unsupported(message) => {
            let message = message.lines().join(" ");
            log_client_version_unsupported(client_version.to_string());
            let http_err_resp: HttpResponseError = anyhow::anyhow!(ErrorMetadata::bad_request(
                "ClientVersionUnsupported",
                message,
            ))
            .into();
            http_err_resp.into_response()
        },
        _ => next.run(req).await,
    };

    match &version_state {
        ClientVersionState::Unsupported(message) | ClientVersionState::UpgradeRequired(message) => {
            let message = message.lines().join(" ");
            let headers = resp.headers_mut();
            let state_str = version_state.variant_name();
            headers.insert(
                HeaderName::from_static(DEPRECATION_STATE_HEADER_NAME),
                HeaderValue::from_str(state_str).context(format!(
                    "Failed to create deprecation state header value: {state_str}"
                ))?,
            );
            headers.insert(
                HeaderName::from_static(DEPRECATION_MSG_HEADER_NAME),
                HeaderValue::from_str(message.as_str()).context(format!(
                    "Failed to create deprecation msg header value: {message}"
                ))?,
            );
        },
        ClientVersionState::Supported => (),
    };
    Ok(resp)
}

/// Guard that records request metrics on drop. Always reports — uses
/// the stored status (defaulting to "499" for cancelled/timed-out requests).
struct RequestStatsGuard {
    start: Instant,
    route: String,
    method: Method,
    client_version: String,
    is_test: bool,
    status: String,
}

impl Drop for RequestStatsGuard {
    fn drop(&mut self) {
        log_http_request(
            &self.client_version,
            &self.route,
            self.method.as_str(),
            &self.status,
            self.start.elapsed(),
            self.is_test,
        );
    }
}

pub async fn stats_middleware<RM: RouteMapper>(
    State(route_metric_mapper): State<RM>,
    matched_path: Option<axum::extract::MatchedPath>,
    ExtractRequestId(request_id): ExtractRequestId,
    ExtractClientVersion(client_version): ExtractClientVersion,
    ExtractResolvedHostname(resolved_host): ExtractResolvedHostname,
    ExtractTraceparent(traceparent): ExtractTraceparent,
    req: http::request::Request<Body>,
    next: axum::middleware::Next,
) -> Result<impl IntoResponse, HttpResponseError> {
    let start = Instant::now();
    let method = req.method().clone();
    // tag with the route. 404s lack matched query path - and the
    // uri is generally unhelpful for metrics aggregation, so leave it out there.
    let route = matched_path
        .map(|r| r.as_str().to_owned())
        .unwrap_or("unknown".to_owned());

    // Capture URI before req is moved
    let uri = req.uri().to_string();

    let client_version_s = client_version.to_string();
    let is_test = resolved_host.deployment_name.starts_with("test-");
    let mapped_route = route_metric_mapper.map_route(route.clone());

    let mut stats_guard = RequestStatsGuard {
        start,
        route: mapped_route.clone(),
        method: method.clone(),
        client_version: client_version_s,
        is_test,
        status: "499".to_string(),
    };

    // Sampling isn't done here, and should be done upstream
    // Use the raw route (not mapped) for the tracing span so specific
    // endpoint paths are preserved in distributed traces.
    let root = match traceparent {
        Some(span_ctx) if *PROPAGATE_UPSTREAM_TRACES => {
            Span::root(route.to_owned(), span_ctx).with_property(|| ("span.kind", "server"))
        },
        _ => Span::noop(),
    };

    // Add the request_id to sentry
    sentry::configure_scope(|scope| scope.set_tag("request_id", &request_id));

    let resp = next.run(req).in_span(root).await;

    if mapped_route == "unknown" {
        tracing::info!("stats_middleware: matched_path is None, uri: {}", uri);
    }

    // Set the real status — drop will report metrics.
    stats_guard.status = resp.status().as_str().to_string();

    Ok::<_, _>(resp)
}

pub struct InstanceNameExt(pub String);

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PaginationMetadata {
    pub has_more: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(ToSchema, Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Ord, PartialOrd)]
#[serde(rename_all = "camelCase")]
pub enum RequestDestination {
    ConvexCloud,
    ConvexSite,
}

impl FromStr for RequestDestination {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "convexCloud" => Ok(RequestDestination::ConvexCloud),
            "convexSite" => Ok(RequestDestination::ConvexSite),
            _ => Err(anyhow::anyhow!("Invalid request destination: {}", s)),
        }
    }
}

impl std::fmt::Display for RequestDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestDestination::ConvexCloud => write!(f, "convexCloud"),
            RequestDestination::ConvexSite => write!(f, "convexSite"),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResolvedHostname {
    pub deployment_name: String,
    pub destination: RequestDestination,
}

pub const CONVEX_DOMAIN_REGEX_INSTANCE_CAPTURE: &str = "instance";
pub const CONVEX_DOMAIN_REGEX_TLD_CAPTURE: &str = "tld";
pub static CONVEX_DOMAIN_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?<instance>[A-Za-z0-9-]+)(\.[A-Za-z0-9-]+)?\.convex\.(?<tld>cloud|site)$")
        .unwrap()
});
pub static LOCAL_DEPLOYMENT_NAME_PII_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"deployment/local-([^/]*)/").unwrap());

pub fn resolve_convex_domain(uri: &Uri) -> anyhow::Result<Option<ResolvedHostname>> {
    let host = uri.host().context("URI does not have valid host")?;
    if let Some(captures) = CONVEX_DOMAIN_REGEX.captures(host) {
        let deployment_name = captures[CONVEX_DOMAIN_REGEX_INSTANCE_CAPTURE].to_string();
        let destination = match &captures[CONVEX_DOMAIN_REGEX_TLD_CAPTURE] {
            "cloud" => RequestDestination::ConvexCloud,
            "site" => RequestDestination::ConvexSite,
            _ => unreachable!("Regex capture only matches cloud or site"),
        };
        return Ok(Some(ResolvedHostname {
            deployment_name,
            destination,
        }));
    }
    Ok(None)
}

pub struct ExtractResolvedHostname(pub ResolvedHostname);

#[derive(Clone, Debug)]
pub struct OriginalHttpUri(pub Uri);

impl<S: Sync> FromRequestParts<S> for ExtractResolvedHostname {
    type Rejection = Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        // Check if we've already resolved this earlier in the stack.
        // We allow `Extension` here instead of `State` as we specifically want this
        // extraction to be fallible and optional.
        #[allow(clippy::disallowed_types)]
        if let Ok(axum::Extension(resolved)) =
            parts.extract::<axum::Extension<ResolvedHostname>>().await
        {
            return Ok(ExtractResolvedHostname(resolved));
        }
        // Try to parse the Host header as a URI and then resolve it as a Convex domain
        let host = parts.extract::<Host>().await.map_err(anyhow::Error::from);
        if let Ok(Some(resolved)) = host
            .and_then(|Host(host)| Uri::try_from(host).map_err(anyhow::Error::from))
            .and_then(|uri| resolve_convex_domain(&uri))
        {
            return Ok(ExtractResolvedHostname(resolved));
        }

        // No luck -- fall back to `CONVEX_SITE` and assume `convex.cloud` as this is
        // likely a request to localhost.
        Ok(ExtractResolvedHostname(ResolvedHostname {
            deployment_name: ::std::env::var("CONVEX_SITE").unwrap_or_default(),
            destination: RequestDestination::ConvexCloud,
        }))
    }
}

impl<S: Sync> OptionalFromRequestParts<S> for OriginalHttpUri {
    type Rejection = Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Option<Self>, Self::Rejection> {
        Ok(parts.extensions.get::<Self>().cloned())
    }
}

#[allow(clippy::declare_interior_mutable_const)]
pub const CONVEX_CLIENT_HEADER: HeaderName = HeaderName::from_static("convex-client");

// The client version header to use for requests from this service.
pub static CONVEX_CLIENT_HEADER_VALUE: LazyLock<HeaderValue> = LazyLock::new(|| {
    let service_name = &*SERVICE_NAME;
    let server_version = &*SERVER_VERSION_STR;
    HeaderValue::from_str(&format!("{service_name}-{server_version}")).unwrap()
});

pub struct ExtractClientVersion(pub ClientVersion);

async fn client_version_from_req_parts(
    parts: &mut axum::http::request::Parts,
) -> anyhow::Result<ClientVersion> {
    let client_version = if let Some(version_header) = parts
        .headers
        .get(CONVEX_CLIENT_HEADER)
        .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
    {
        version_header.parse::<ClientVersion>()?
    } else {
        #[derive(Deserialize, Default)]
        struct Params {
            client_version: Option<String>,
        }
        let Params { client_version } = parts
            .extract::<extract::Path<_>>()
            .await
            .map(|path| path.0)
            .unwrap_or_default();
        match client_version {
            None => ClientVersion::unknown(),
            Some(version) => ClientVersion::from_path_param(
                version.parse().map_err(|e| {
                    ErrorMetadata::bad_request(
                        "InvalidVersion",
                        format!("Failed to parse client version: {e}"),
                    )
                })?,
                parts.uri.path(),
            ),
        }
    };
    Ok(client_version)
}

impl<S> FromRequestParts<S> for ExtractClientVersion
where
    S: Send + Sync,
{
    type Rejection = HttpResponseError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let client_version = client_version_from_req_parts(parts).await.map_err(|e| {
            anyhow::anyhow!(ErrorMetadata::bad_request(
                "InvalidClientVersion",
                e.to_string(),
            ))
        })?;
        sentry::configure_scope(|scope| scope.set_tag("convex_client_version", &client_version));
        Ok(Self(client_version))
    }
}

#[allow(clippy::declare_interior_mutable_const)]
pub const CONVEX_REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("convex-request-id");

pub struct ExtractRequestId(pub RequestId);

async fn request_id_from_req_parts(
    parts: &mut axum::http::request::Parts,
) -> anyhow::Result<RequestId> {
    if let Some(request_id_header) = parts
        .headers
        .get(CONVEX_REQUEST_ID_HEADER)
        .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
    {
        request_id_header.parse::<RequestId>()
    } else {
        // Generate a new request_id
        let request_id = RequestId::new();
        parts
            .headers
            .insert(CONVEX_REQUEST_ID_HEADER, request_id.as_str().parse()?);
        Ok(request_id)
    }
}

impl<S> FromRequestParts<S> for ExtractRequestId
where
    S: Send + Sync,
{
    type Rejection = HttpResponseError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let request_id = request_id_from_req_parts(parts).await.map_err(|e| {
            anyhow::anyhow!(ErrorMetadata::bad_request(
                "InvalidRequestId",
                e.to_string(),
            ))
        })?;
        Ok(Self(request_id))
    }
}

pub struct ExtractRequestMetadata(pub RequestMetadata);

impl<S> FromRequestParts<S> for ExtractRequestMetadata
where
    S: Send + Sync,
{
    type Rejection = HttpResponseError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let ip = parts
            .headers
            .get("x-forwarded-for")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.split(',').next())
            .map(|s| s.trim().to_owned())
            .or_else(|| {
                parts
                    .extensions
                    .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
                    .map(|ci| ci.0.ip().to_string())
            });
        let ip = ip.map(ClientIp::from);
        let user_agent = parts
            .headers
            .get(http::header::USER_AGENT)
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_owned());
        let user_agent = user_agent.map(ClientUserAgent::from);
        Ok(ExtractRequestMetadata(RequestMetadata { ip, user_agent }))
    }
}

#[allow(clippy::declare_interior_mutable_const)]
pub const CONVEX_CHEF_DEPLOY_SECRET_HEADER: HeaderName =
    HeaderName::from_static("convex-chef-deploy-secret");

pub struct ExtractChefDeploySecret(pub String);

fn chef_deploy_secret_from_req_parts(
    parts: &mut axum::http::request::Parts,
) -> anyhow::Result<String> {
    parts
        .headers
        .get(CONVEX_CHEF_DEPLOY_SECRET_HEADER)
        .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
        .context(ErrorMetadata::bad_request(
            "InvalidChefDeploySecret",
            "convex-chef-deploy-secret header is not set",
        ))
}

impl<S> FromRequestParts<S> for ExtractChefDeploySecret
where
    S: Send + Sync,
{
    type Rejection = HttpResponseError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let shared_secret = chef_deploy_secret_from_req_parts(parts)?;
        Ok(Self(shared_secret))
    }
}

pub const TRACEPARENT_HEADER_STR: &str = "traceparent";
pub const TRACEPARENT_HEADER: HeaderName = HeaderName::from_static(TRACEPARENT_HEADER_STR);

pub struct ExtractTraceparent(pub Option<SpanContext>);

impl<S> FromRequestParts<S> for ExtractTraceparent
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let traceparent = parts
            .headers
            .get(TRACEPARENT_HEADER)
            .and_then(|h| h.to_str().ok())
            .and_then(SpanContext::decode_w3c_traceparent);
        Ok(Self(traceparent))
    }
}

async fn tokio_instrumentation_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<Response, HttpResponseError> {
    let resp = TaskManager::instrument("axum_handler", next.run(req)).await;
    Ok(resp)
}

fn sanitize_uri_for_logging(uri: &Uri) -> Cow<'_, str> {
    let path = uri.path();
    let path_and_query_str = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or(path);

    let uri_for_logging: Cow<str> = if let Some(query) = uri.query() {
        if query.contains("adminKey=") {
            // Remove the entire query string to avoid logging the admin key
            Cow::Borrowed(path)
        } else {
            Cow::Borrowed(path_and_query_str)
        }
    } else {
        Cow::Borrowed(path_and_query_str)
    };

    // Then handle PII in path if present
    if path.contains("deployment/local-") {
        Cow::Owned(
            LOCAL_DEPLOYMENT_NAME_PII_REGEX
                .replace(&uri_for_logging, r"deployment/local-*/")
                .into_owned(),
        )
    } else {
        uri_for_logging
    }
}

fn is_high_volume_path(path: &str) -> bool {
    path == "/instance_version"
        || path == "/instance_name"
        || path == "/get_backend_info"
        || path == "/get_system_stop_state"
        || path == "/api/shapes2"
        || path == "/api/actions/query"
        || path == "/api/actions/mutation"
        || path == "/api/actions/action"
        || path == "/api/stream_function_logs"
        || path == "/api/app_metrics/stream_function_logs"
        || path == "/"
        || path == "/health"
}

/// Emit an HTTP access log line. Used by both the normal completion path
/// and the drop guard (cancelled/timed-out requests).
fn log_http_access(
    site_id: &str,
    remote_addr: Option<SocketAddr>,
    method: &Method,
    uri: &Uri,
    version: http::Version,
    status: impl fmt::Display,
    referer: Option<&str>,
    user_agent: Option<&str>,
    content_type: Option<&str>,
    content_length: Option<&str>,
    elapsed: Duration,
) {
    let uri_for_logging = sanitize_uri_for_logging(uri);
    let level = if is_high_volume_path(uri.path()) {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };
    dyn_event!(
        level,
        target: "convex-cloud-http",
        "[{}] {} \"{} {} {:?}\" {} \"{}\" \"{}\" {} {} {:.3}ms",
        site_id,
        LogOptFmt(remote_addr),
        method,
        uri_for_logging,
        version,
        status,
        LogOptFmt(referer),
        LogOptFmt(user_agent),
        LogOptFmt(content_type.as_ref()),
        LogOptFmt(content_length.as_ref()),
        elapsed.as_secs_f64() * 1000.0,
    );
}

/// Guard that logs the HTTP access line on drop. Always reports — defaults
/// to status 499 (client closed request) for cancelled/timed-out requests.
struct RequestLogGuard {
    site_id: String,
    remote_addr: Option<SocketAddr>,
    method: Method,
    uri: Uri,
    version: http::Version,
    referer: Option<String>,
    user_agent: Option<String>,
    start: Instant,
    status: u16,
    content_type: Option<String>,
    content_length: Option<String>,
}

impl Drop for RequestLogGuard {
    fn drop(&mut self) {
        log_http_access(
            &self.site_id,
            self.remote_addr,
            &self.method,
            &self.uri,
            self.version,
            self.status,
            self.referer.as_deref(),
            self.user_agent.as_deref(),
            self.content_type.as_deref(),
            self.content_length.as_deref(),
            self.start.elapsed(),
        );
    }
}

async fn log_middleware(
    remote_addr: Result<axum::extract::ConnectInfo<SocketAddr>, ExtensionRejection>,
    ExtractResolvedHostname(resolved_host): ExtractResolvedHostname,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<Response, HttpResponseError> {
    let site_id = resolved_host.deployment_name;
    let start = Instant::now();

    let remote_addr = remote_addr.ok().map(|connect_info| connect_info.0);
    let method = req.method().clone();
    let uri = req.uri().clone();
    let version = req.version();
    let get_header = |headers: &HeaderMap, name: HeaderName| -> Option<String> {
        headers
            .get(name)
            .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
    };
    let referer = get_header(req.headers(), http::header::REFERER);
    let user_agent = get_header(req.headers(), http::header::USER_AGENT);

    let mut guard = RequestLogGuard {
        site_id,
        remote_addr,
        method,
        uri,
        version,
        referer,
        user_agent,
        start,
        status: 499,
        content_type: None,
        content_length: None,
    };

    let resp = next.run(req).await;

    // Set the real values — drop will log.
    guard.status = resp.status().as_u16();
    guard.content_length = get_header(resp.headers(), http::header::CONTENT_LENGTH);
    guard.content_type = get_header(resp.headers(), http::header::CONTENT_TYPE);

    Ok(resp)
}

struct LogOptFmt<T>(Option<T>);

impl<T: fmt::Display> fmt::Display for LogOptFmt<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref t) = self.0 {
            fmt::Display::fmt(t, f)
        } else {
            f.write_str("-")
        }
    }
}

/// CLI endpoints can be used from browser IDEs (e.g. StackBlitz), which send
/// different headers.
pub fn cli_cors() -> CorsLayer {
    CorsLayer::new()
        .allow_headers(AllowHeaders::mirror_request())
        .allow_credentials(true)
        .allow_methods(vec![
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::OPTIONS,
            Method::DELETE,
        ])
        .allow_origin(AllowOrigin::mirror_request())
        .max_age(Duration::from_secs(86400))
}

/// Platform APIs dont' accept cookies so there's minimal risk (and plenty of
/// convenience) from allowing browsers to make these requests.
pub fn platform_api_cors() -> CorsLayer {
    CorsLayer::new()
        .allow_headers(AllowHeaders::mirror_request())
        .allow_credentials(true)
        .allow_methods(vec![
            Method::GET,
            Method::POST,
            Method::PATCH,
            Method::OPTIONS,
        ])
        .allow_origin(AllowOrigin::mirror_request())
        .max_age(Duration::from_secs(86400))
}

/// Collects metrics and returns them in the Prometheus exposition format.
/// Returns an empty response if no metrics have been recorded yet.
/// Note that registered metrics will not show here until recorded at least
/// once.
pub async fn metrics() -> Result<impl IntoResponse, HttpResponseError> {
    if *DISABLE_METRICS_ENDPOINT {
        return Err(anyhow::anyhow!(ErrorMetadata::not_found(
            "MetricsDisabled",
            "/metrics endpoint disabled"
        ))
        .into());
    }
    // Idempotent; starts the sweeper on the first scrape.
    ::metrics::spawn_sweep_task(None);
    let encoder = TextEncoder::new();
    let metrics = CONVEX_METRICS_REGISTRY.gather();
    let output = encoder
        .encode_to_string(&metrics)
        .map_err(anyhow::Error::from)?;
    Ok(output)
}

/// Converts a [`HeaderMap`] into an iterator of key-value tuples, handling
/// `None` keys by using the last seen `HeaderName`. This is needed as
/// [`HeaderMap::into_iter`](http::header::HeaderMap#method.into_iter) provides
/// an iterator of `(Option<HeaderName>, T)`.
pub fn normalize_header_map<T>(header_map: HeaderMap<T>) -> impl Iterator<Item = (HeaderName, T)>
where
    T: Clone,
{
    let mut last_key: Option<HeaderName> = None;

    header_map.into_iter().map(move |(key, value)| {
        match key {
            Some(ref key) => last_key = Some(key.clone()),
            None => {},
        }

        let key = last_key
            .clone()
            .expect("HeaderMap should not have a None key without a previous Some key");
        (key, value)
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::Duration,
    };

    use axum::{
        extract::State,
        routing::get,
        Router,
    };
    use http::Request;
    use tokio::sync::{
        mpsc,
        Semaphore,
    };
    use tower::ServiceExt as _;

    use super::{
        dependency_aware_concurrency_middleware,
        metrics::{
            http_admission_wait_observations,
            http_admission_waiters,
            initialize_http_admission_metrics,
            HttpAdmissionWaitGuard,
        },
        Body,
        ExternalRequestShedding,
        HttpConcurrencyLimiter,
        StatusCode,
    };
    use crate::dependency_overflow::DependencyOverflowGate;

    #[derive(Clone)]
    struct BlockingHandlerState {
        started: mpsc::UnboundedSender<&'static str>,
        release: Arc<Semaphore>,
    }

    async fn ordinary_handler(State(state): State<BlockingHandlerState>) -> StatusCode {
        state.started.send("ordinary").unwrap();
        state.release.acquire().await.unwrap().forget();
        StatusCode::OK
    }

    async fn dependency_handler(State(state): State<BlockingHandlerState>) -> StatusCode {
        state.started.send("dependency").unwrap();
        state.release.acquire().await.unwrap().forget();
        StatusCode::OK
    }

    async fn ok_handler() -> StatusCode {
        StatusCode::OK
    }

    #[tokio::test]
    async fn memory_pressure_sheds_external_requests_but_preserves_dependencies() {
        let shedding = ExternalRequestShedding::new(true);
        let limiter = HttpConcurrencyLimiter {
            gate: DependencyOverflowGate::new(1, 2),
            dependency_path_prefixes: &["/api/actions/"],
            external_request_shedding: Some(shedding.clone()),
            service_name: "memory_pressure_test",
        };
        let app = Router::new()
            .route("/ordinary", get(ok_handler))
            .route("/api/actions/query", get(ok_handler))
            .layer(axum::middleware::from_fn_with_state(
                limiter,
                dependency_aware_concurrency_middleware,
            ));

        let ordinary = app
            .clone()
            .oneshot(Request::get("/ordinary").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(ordinary.status(), StatusCode::SERVICE_UNAVAILABLE);

        let dependency = app
            .clone()
            .oneshot(
                Request::get("/api/actions/query")
                    .header("Convex-Action-Callback-Token", "token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(dependency.status(), StatusCode::OK);

        assert!(shedding.set_active(false));
        let recovered = app
            .oneshot(Request::get("/ordinary").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(recovered.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn queued_external_request_rechecks_memory_pressure_before_handling() {
        let shedding = ExternalRequestShedding::new(false);
        let limiter = HttpConcurrencyLimiter {
            gate: DependencyOverflowGate::new(1, 1),
            dependency_path_prefixes: &[],
            external_request_shedding: Some(shedding.clone()),
            service_name: "queued_memory_pressure_test",
        };
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Semaphore::new(0));
        let app = Router::new()
            .route("/ordinary", get(ordinary_handler))
            .with_state(BlockingHandlerState {
                started: started_tx,
                release: release.clone(),
            })
            .layer(axum::middleware::from_fn_with_state(
                limiter,
                dependency_aware_concurrency_middleware,
            ));

        let first = tokio::spawn(
            app.clone()
                .oneshot(Request::get("/ordinary").body(Body::empty()).unwrap()),
        );
        assert_eq!(started_rx.recv().await, Some("ordinary"));
        let queued =
            tokio::spawn(app.oneshot(Request::get("/ordinary").body(Body::empty()).unwrap()));
        match tokio::time::timeout(Duration::from_millis(25), started_rx.recv()).await {
            Ok(Some(request_kind)) => {
                panic!("shed queued request reached the handler: {request_kind}")
            },
            Ok(None) | Err(_) => {},
        }

        assert!(!shedding.set_active(true));
        release.add_permits(1);
        assert_eq!(first.await.unwrap().unwrap().status(), StatusCode::OK);
        assert_eq!(
            queued.await.unwrap().unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        match tokio::time::timeout(Duration::from_millis(25), started_rx.recv()).await {
            Ok(Some(request_kind)) => {
                panic!("shed queued request reached the handler: {request_kind}")
            },
            Ok(None) | Err(_) => {},
        }
    }

    #[tokio::test]
    async fn dependencies_share_base_before_using_http_overflow() {
        let limiter = HttpConcurrencyLimiter {
            gate: DependencyOverflowGate::new(2, 3),
            dependency_path_prefixes: &["/api/actions/"],
            external_request_shedding: None,
            service_name: "test",
        };
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Semaphore::new(0));
        let app = Router::new()
            .route("/ordinary", get(ordinary_handler))
            .route("/api/actions/query", get(dependency_handler))
            .with_state(BlockingHandlerState {
                started: started_tx,
                release: release.clone(),
            })
            .layer(axum::middleware::from_fn_with_state(
                limiter,
                dependency_aware_concurrency_middleware,
            ));

        let first_dependency = tokio::spawn(
            app.clone().oneshot(
                Request::get("/api/actions/query")
                    .header("Convex-Action-Callback-Token", "token")
                    .body(Body::empty())
                    .unwrap(),
            ),
        );
        assert_eq!(started_rx.recv().await, Some("dependency"));

        let first_ordinary = tokio::spawn(
            app.clone()
                .oneshot(Request::get("/ordinary").body(Body::empty()).unwrap()),
        );
        assert_eq!(started_rx.recv().await, Some("ordinary"));

        let second_ordinary = tokio::spawn(
            app.clone().oneshot(
                Request::get("/ordinary")
                    .header("Convex-Action-Callback-Token", "forged")
                    .body(Body::empty())
                    .unwrap(),
            ),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(25), started_rx.recv())
                .await
                .is_err()
        );

        let unmarked_callback = tokio::spawn(
            app.clone().oneshot(
                Request::get("/api/actions/query")
                    .body(Body::empty())
                    .unwrap(),
            ),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(25), started_rx.recv())
                .await
                .is_err()
        );

        let overflow_dependency = tokio::spawn(
            app.oneshot(
                Request::get("/api/actions/query")
                    .header("Convex-Action-Callback-Token", "token")
                    .body(Body::empty())
                    .unwrap(),
            ),
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
                .await
                .unwrap(),
            Some("dependency")
        );

        release.add_permits(5);
        first_dependency.await.unwrap().unwrap();
        first_ordinary.await.unwrap().unwrap();
        overflow_dependency.await.unwrap().unwrap();
        second_ordinary.await.unwrap().unwrap();
        unmarked_callback.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn dependency_http_requests_still_obey_total_capacity() {
        let limiter = HttpConcurrencyLimiter {
            gate: DependencyOverflowGate::new(1, 1),
            dependency_path_prefixes: &["/api/actions/"],
            external_request_shedding: None,
            service_name: "test",
        };
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Semaphore::new(0));
        let app = Router::new()
            .route("/api/actions/query", get(dependency_handler))
            .with_state(BlockingHandlerState {
                started: started_tx,
                release: release.clone(),
            })
            .layer(axum::middleware::from_fn_with_state(
                limiter,
                dependency_aware_concurrency_middleware,
            ));
        let request = || {
            Request::get("/api/actions/query")
                .header("Convex-Action-Callback-Token", "token")
                .body(Body::empty())
                .unwrap()
        };

        let first = tokio::spawn(app.clone().oneshot(request()));
        assert_eq!(started_rx.recv().await, Some("dependency"));
        let second = tokio::spawn(app.oneshot(request()));
        assert!(
            tokio::time::timeout(Duration::from_millis(25), started_rx.recv())
                .await
                .is_err()
        );

        release.add_permits(1);
        first.await.unwrap().unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
                .await
                .unwrap(),
            Some("dependency")
        );
        release.add_permits(1);
        second.await.unwrap().unwrap();
    }

    #[test]
    fn short_http_admission_waits_emit_histogram_observations() {
        const SERVICE_NAME: &str = "short_http_admission_wait_test";
        initialize_http_admission_metrics(SERVICE_NAME);
        for is_dependency in ["false", "true"] {
            assert_eq!(http_admission_waiters(SERVICE_NAME, is_dependency), 0.0);
        }

        let before = http_admission_wait_observations(SERVICE_NAME, "false");
        drop(HttpAdmissionWaitGuard::new(SERVICE_NAME, false));

        assert_eq!(http_admission_waiters(SERVICE_NAME, "false"), 0.0);
        assert_eq!(
            http_admission_wait_observations(SERVICE_NAME, "false"),
            before + 1
        );
    }
}
