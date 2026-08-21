//! Router assembly and HTTP serving lifecycle.
//!
//! Route table, middleware layer order (startup gate → CORS → auth → request logging),
//! listener bind/serve. Middleware order is observable behavior; see `build_router`.

use std::future::Future as _;
use std::sync::Arc;
use std::{future::IntoFuture as _, task::Poll};

use axum::extract::{MatchedPath, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::server::ServerError;

use super::auth::{
    auth_middleware, diagnostic_auth_middleware, diagnostic_bounds_middleware,
    diagnostic_method_not_allowed,
};
use super::catalog::{
    execute_sql, get_graph, get_stream, list_connectors, list_mvs, list_sinks, list_sources,
    list_streams,
};
use super::checkpoints::{cluster_checkpoints, trigger_checkpoint};
use super::cluster_admin::{
    cluster_leader, cluster_nodes, cluster_status, cluster_vnodes, pipeline_status, start_pipeline,
    stop_pipeline,
};
use super::cluster_evidence::{cluster_local_checkpoint_barrier_timings, cluster_local_evidence};
use super::error_response;
use super::ops::{handle_reload, health_check, prometheus_metrics, readiness_check};
use super::state::AppState;
use super::ws::ws_upgrade;

pub fn build_router(state: Arc<AppState>) -> Router {
    let cors = build_cors_layer(&state);

    // Public, unauthenticated routes: liveness/readiness probes and the
    // Prometheus scrape endpoint. These must stay reachable without a token so
    // orchestrators and the metrics scraper keep working.
    let public = Router::new()
        .route("/health", get(health_check))
        .route("/ready", get(readiness_check))
        .route("/metrics", get(prometheus_metrics));

    // Control-plane (`/api/v1/*`) and realtime (`/ws/{name}`) routes, gated by
    // the console bearer token when one is configured.
    let console = Router::new()
        .route("/api/v1/sources", get(list_sources))
        .route("/api/v1/sinks", get(list_sinks))
        .route("/api/v1/streams", get(list_streams))
        .route("/api/v1/streams/{name}", get(get_stream))
        .route("/api/v1/mvs", get(list_mvs))
        .route("/api/v1/connectors", get(list_connectors))
        .route("/api/v1/checkpoint", post(trigger_checkpoint))
        .route("/api/v1/sql", post(execute_sql))
        .route("/api/v1/reload", post(handle_reload))
        .route("/api/v1/graph", get(get_graph))
        .route("/api/v1/cluster", get(cluster_status))
        .route("/api/v1/cluster/nodes", get(cluster_nodes))
        .route("/api/v1/cluster/vnodes", get(cluster_vnodes))
        .route("/api/v1/cluster/leader", get(cluster_leader))
        .route("/api/v1/cluster/checkpoints", get(cluster_checkpoints))
        .route("/api/v1/pipeline/stop", post(stop_pipeline))
        .route("/api/v1/pipeline/start", post(start_pipeline))
        .route("/api/v1/pipeline/status", get(pipeline_status))
        .route("/ws/{name}", get(ws_upgrade))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    // The local evidence routes accept the console administrator or the strictly narrower
    // diagnostic-read bearer. They intentionally sit outside console CORS and override Axum's
    // implicit GET-to-HEAD behavior.
    let diagnostics = Router::new()
        .route(
            "/api/v1/cluster/local-evidence",
            get(cluster_local_evidence).head(diagnostic_method_not_allowed),
        )
        .route(
            "/api/v1/cluster/local-checkpoint-barrier-timings",
            get(cluster_local_checkpoint_barrier_timings).head(diagnostic_method_not_allowed),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            diagnostic_bounds_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            diagnostic_auth_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            startup_gate_middleware,
        ));

    let public_console = public
        .merge(console)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            startup_gate_middleware,
        ))
        .layer(cors)
        .merge(diagnostics);

    public_console
        .layer(axum::middleware::from_fn(request_logging))
        .with_state(state)
}

async fn startup_gate_middleware(
    State(state): State<Arc<AppState>>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let path = req.uri().path();
    let startup_probe = matches!(path, "/health" | "/ready" | "/metrics");
    if !startup_probe {
        if let Some(reason) = state.serving_rejection() {
            return error_response(StatusCode::SERVICE_UNAVAILABLE, reason).into_response();
        }
    }
    next.run(req).await
}

/// Build the CORS policy from config. With an explicit allow-list of console
/// origins we restrict `Access-Control-Allow-Origin` to those; otherwise we
/// fall back to the legacy permissive policy (dev only).
fn build_cors_layer(state: &AppState) -> CorsLayer {
    let allowed = state
        .current_config
        .read()
        .server
        .console_cors_allowed_origins
        .clone();

    match allowed {
        Some(origins) => {
            let values: Vec<axum::http::HeaderValue> =
                origins.iter().filter_map(|o| o.parse().ok()).collect();
            CorsLayer::new()
                .allow_origin(values)
                .allow_methods(Any)
                .allow_headers(Any)
        }
        None => CorsLayer::permissive(),
    }
}

async fn request_logging(
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> impl IntoResponse {
    let method = req.method().clone();
    // Log only the matched route template. Raw targets can carry a WebSocket query token or an
    // attacker-controlled credential-shaped path segment and must not land in access logs.
    let path = req
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or("<unmatched>")
        .to_owned();
    let start = std::time::Instant::now();

    let response = next.run(req).await;

    let duration_ms = start.elapsed().as_millis();
    let status = response.status();
    info!("{method} {path} -> {status} ({duration_ms}ms)");

    response
}

pub async fn bind_listener(bind: &str) -> Result<tokio::net::TcpListener, ServerError> {
    tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|e| ServerError::Http(format!("failed to bind to {bind}: {e}")))
}

pub fn serve_listener(
    router: Router,
    listener: tokio::net::TcpListener,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let mut server = Box::pin(axum::serve(listener, router).into_future());
        let stopped = std::future::poll_fn(|context| match server.as_mut().poll(context) {
            Poll::Pending => Poll::Ready(None),
            Poll::Ready(result) => Poll::Ready(Some(result)),
        })
        .await;
        if let Some(result) = stopped {
            if let Err(error) = result {
                tracing::error!(%error, "HTTP server stopped before its accept loop started");
            }
            return;
        }
        let _ = started_tx.send(());
        if let Err(error) = server.await {
            tracing::error!(%error, "HTTP server error");
        }
    });
    (handle, started_rx)
}
