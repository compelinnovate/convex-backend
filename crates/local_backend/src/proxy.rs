use std::net::SocketAddr;

use axum::{
    body::Body,
    extract::{
        Request,
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use common::{
    http::{
        ConvexHttpService,
        ExternalRequestShedding,
        HttpResponseError,
        NoopRouteMapper,
    },
    knobs::HTTP_SERVER_TIMEOUT_DURATION,
};
use hyper_util::{
    client::legacy::{
        connect::HttpConnector,
        Client,
    },
    rt::TokioExecutor,
};

type ProxyClient = Client<HttpConnector, Body>;

#[derive(Clone)]
struct ProxyState {
    site_forward_prefix: String,
    client: ProxyClient,
}

/// Routes HTTP actions to the main webserver
pub async fn dev_site_proxy(
    site_bind_addr: Option<([u8; 4], u16)>,
    site_forward_prefix: String,
    max_concurrent_requests: usize,
    external_request_shedding: Option<ExternalRequestShedding>,
    mut shutdown_rx: async_broadcast::Receiver<()>,
) -> anyhow::Result<()> {
    let Some(addr) = site_bind_addr else {
        return Ok(());
    };
    let addr = SocketAddr::from(addr);
    tracing::info!("Starting dev site proxy at {:?}...", addr);

    async fn proxy_method(
        State(state): State<ProxyState>,
        mut request: Request,
    ) -> Result<impl IntoResponse, HttpResponseError> {
        let new_uri = format!("{}{}", state.site_forward_prefix, request.uri());
        *request.uri_mut() = new_uri.parse().map_err(anyhow::Error::new)?;
        let resp = state
            .client
            .request(request)
            .await
            .map_err(anyhow::Error::new)?;
        Ok(resp)
    }

    let proxy_handler = get(proxy_method)
        .post(proxy_method)
        .delete(proxy_method)
        .patch(proxy_method)
        .put(proxy_method)
        .options(proxy_method);
    let router = Router::new()
        .route("/{*rest}", proxy_handler.clone())
        .route("/", proxy_handler)
        .with_state(ProxyState {
            site_forward_prefix,
            client: Client::builder(TokioExecutor::new()).build_http(),
        });

    let service = ConvexHttpService::new_with_dependency_reserve(
        Router::new().fallback_service(router),
        "backend_http_proxy",
        "unknown".to_string(),
        max_concurrent_requests,
        0,
        &[],
        external_request_shedding,
        *HTTP_SERVER_TIMEOUT_DURATION,
        NoopRouteMapper,
    );
    let proxy_server = service.serve(addr, async move {
        let _ = shutdown_rx.recv().await;
        tracing::info!("Shut down proxy");
    });
    proxy_server.await?;
    Ok(())
}
