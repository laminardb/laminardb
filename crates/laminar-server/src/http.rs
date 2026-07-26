//! HTTP API for LaminarDB server.

use std::io::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;
use std::{future::Future as _, future::IntoFuture as _, task::Poll};

use prometheus::Registry;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};
use tracing::{info, warn};

use laminar_db::{ConnectorInfo, LaminarDB, PipelineNodeType};

use crate::config::{ServerConfig, ServerMode};
use crate::metrics::ServerMetrics;
use crate::reload::{self, ReloadGuard};
use crate::server::ServerError;

/// Cluster control-plane handles backing the `/api/v1/cluster/*` endpoints.
/// Absent in single-node mode, where those endpoints return `404`.
#[cfg(feature = "cluster")]
#[derive(Clone)]
pub struct ClusterComponents {
    /// Leader-election / membership controller.
    pub controller: Arc<laminar_core::cluster::control::ClusterController>,
    /// Durable vnode-assignment snapshot store.
    pub snapshot_store: Arc<laminar_core::cluster::control::AssignmentSnapshotStore>,
    /// Live cluster membership feed.
    pub membership_rx:
        tokio::sync::watch::Receiver<Vec<laminar_core::cluster::discovery::NodeInfo>>,
}

pub struct AppState {
    pub db: Arc<LaminarDB>,
    pub config_path: PathBuf,
    pub current_config: parking_lot::RwLock<ServerConfig>,
    pub reload_guard: ReloadGuard,
    pub registry: Arc<Registry>,
    pub server_metrics: ServerMetrics,
    pub(crate) ws_slots: Arc<tokio::sync::Semaphore>,
    pub(crate) serving_gate: Arc<ServingGate>,
    /// Cluster control-plane handles (cluster mode only). `None` in
    /// single-node mode; the cluster endpoints 404 when absent.
    #[cfg(feature = "cluster")]
    pub cluster: Option<ClusterComponents>,
}

const SERVING_STARTING: u8 = 0;
const SERVING_READY: u8 = 1;
const SERVING_FENCED: u8 = 2;

/// One-way serving authority shared by startup and the terminal cluster lease fence.
pub(crate) struct ServingGate {
    state: AtomicU8,
    fenced: tokio::sync::Notify,
    #[cfg(feature = "cluster")]
    process_deadline: std::sync::OnceLock<Arc<laminar_core::cluster::control::LeaseDeadline>>,
    #[cfg(feature = "cluster")]
    deadline_watcher: parking_lot::Mutex<Option<tokio::task::AbortHandle>>,
}

impl ServingGate {
    pub(crate) fn starting() -> Self {
        Self {
            state: AtomicU8::new(SERVING_STARTING),
            fenced: tokio::sync::Notify::new(),
            #[cfg(feature = "cluster")]
            process_deadline: std::sync::OnceLock::new(),
            #[cfg(feature = "cluster")]
            deadline_watcher: parking_lot::Mutex::new(None),
        }
    }

    /// Open serving after startup. A terminal fence can never be reopened.
    pub(crate) fn open(&self) -> bool {
        match self.state.compare_exchange(
            SERVING_STARTING,
            SERVING_READY,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(SERVING_READY) => true,
            Err(SERVING_FENCED) => false,
            Err(_) => unreachable!("serving gate contains an invalid state"),
        }
    }

    /// Permanently revoke serving authority.
    pub(crate) fn fence(&self) {
        self.state.store(SERVING_FENCED, Ordering::Release);
        self.fenced.notify_waiters();
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn install_process_lease_deadline(
        self: &Arc<Self>,
        deadline: Arc<laminar_core::cluster::control::LeaseDeadline>,
    ) -> Result<(), &'static str> {
        if !deadline.is_live() {
            self.fence();
            return Err("HTTP process lease is already expired");
        }
        let mut watcher_slot = self.deadline_watcher.lock();
        if let Some(current) = self.process_deadline.get() {
            return if Arc::ptr_eq(current, &deadline) {
                Ok(())
            } else {
                Err("HTTP process lease deadline is already installed")
            };
        }
        self.process_deadline
            .set(Arc::clone(&deadline))
            .map_err(|_| "HTTP process lease deadline is already installed")?;
        let gate = Arc::downgrade(self);
        let watcher = tokio::spawn(async move {
            deadline.wait_until_expired().await;
            if let Some(gate) = gate.upgrade() {
                gate.fence();
            }
        });
        *watcher_slot = Some(watcher.abort_handle());
        Ok(())
    }

    async fn wait_fenced(&self) {
        loop {
            let fenced = self.fenced.notified();
            tokio::pin!(fenced);
            fenced.as_mut().enable();
            if self.state.load(Ordering::Acquire) == SERVING_FENCED {
                return;
            }
            fenced.await;
        }
    }

    pub(crate) fn rejection_message(&self) -> Option<&'static str> {
        match self.state.load(Ordering::Acquire) {
            SERVING_STARTING => Some("server startup is not complete"),
            SERVING_READY => None,
            SERVING_FENCED => Some("server serving authority is fenced"),
            _ => unreachable!("serving gate contains an invalid state"),
        }
    }
}

impl AppState {
    /// Open the startup gate after the runtime has established serving authority.
    pub(crate) fn open_startup_gate(&self) -> bool {
        self.serving_gate.open()
    }

    fn serving_rejection(&self) -> Option<&'static str> {
        if let Some(reason) = self.serving_gate.rejection_message() {
            return Some(reason);
        }
        #[cfg(feature = "cluster")]
        if let Some(cluster) = self.cluster.as_ref() {
            if !cluster.controller.process_lease_is_live() {
                return Some("server process lease is no longer live");
            }
            if cluster.controller.is_recovering() || self.db.coordinated_recovery_in_progress() {
                return Some("server is completing coordinated recovery");
            }
        }
        None
    }

    async fn wait_for_serving_fence(&self) {
        #[cfg(feature = "cluster")]
        if let Some(cluster) = self.cluster.as_ref() {
            tokio::select! {
                biased;
                () = cluster.controller.wait_for_process_lease_loss() => return,
                () = self.serving_gate.wait_fenced() => return,
            }
        }
        self.serving_gate.wait_fenced().await;
    }
}

impl Drop for ServingGate {
    fn drop(&mut self) {
        #[cfg(feature = "cluster")]
        if let Some(watcher) = self.deadline_watcher.get_mut().take() {
            watcher.abort();
        }
    }
}

pub(crate) fn ws_connection_slots() -> Arc<tokio::sync::Semaphore> {
    Arc::new(tokio::sync::Semaphore::new(MAX_WS_CONNECTIONS))
}

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
    let protected = Router::new()
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
        .route(
            "/api/v1/cluster/local-evidence",
            get(cluster_local_evidence),
        )
        .route(
            "/api/v1/cluster/local-checkpoint-barrier-timings",
            get(cluster_local_checkpoint_barrier_timings),
        )
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

    public
        .merge(protected)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            startup_gate_middleware,
        ))
        .layer(cors)
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

/// Bearer-token gate for the control-plane API. When `server.console_token` is
/// configured, every request to a protected route must present the token
/// either as an `Authorization: Bearer <token>` header or as a `?token=<token>`
/// query parameter (the latter for browser WebSocket clients, which can't set
/// custom headers on the upgrade request). When no token is configured the
/// HTTP API is left open — loopback/dev only.
async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    // Clone the token out and drop the guard before any `.await`; the
    // parking_lot guard is `!Send` and must not cross `next.run`.
    let expected = state.current_config.read().server.console_token.clone();

    if let Some(expected) = expected {
        let expected = expected.expose();
        // The `?token=` query parameter exists for browser WebSocket clients,
        // which can't set the `Authorization` header on the upgrade request.
        // Restrict it to WS routes (`/ws/…`) so it can't leak into access
        // logs, referrers, or proxy caches on regular control-plane requests.
        let is_ws = req.uri().path().starts_with("/ws/");
        let authorized = bearer_token(req.headers()).is_some_and(|t| ct_eq(t, expected))
            || (is_ws && query_token(req.uri()).is_some_and(|t| ct_eq(&t, expected)));
        if !authorized {
            return error_response(StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }

    next.run(req).await
}

/// Extract the bearer token from an `Authorization: Bearer <token>` header.
fn bearer_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

/// Extract and percent-decode the `token` query parameter, if present. Browser
/// WebSocket clients URL-encode the value, so it must be decoded before the
/// constant-time comparison.
fn query_token(uri: &axum::http::Uri) -> Option<String> {
    let raw = uri.query()?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == "token").then_some(value)
    })?;
    Some(
        percent_encoding::percent_decode_str(raw)
            .decode_utf8_lossy()
            .into_owned(),
    )
}

/// Constant-time string comparison so token validation doesn't leak the secret
/// through response timing.
fn ct_eq(a: &str, b: &str) -> bool {
    use subtle::ConstantTimeEq;
    a.as_bytes().ct_eq(b.as_bytes()).unwrap_u8() == 1
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

/// Health check response.
#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    pipeline_state: &'static str,
}

#[derive(Debug, Serialize)]
struct SourceResponse {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    watermark_column: Option<String>,
}

#[derive(Debug, Serialize)]
struct StreamResponse {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sql: Option<String>,
}

#[derive(Debug, Serialize)]
struct SinkResponse {
    name: String,
}

#[derive(Debug, Serialize)]
struct CheckpointResponse {
    success: bool,
    checkpoint_id: u64,
    epoch: u64,
    duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure_disposition: Option<laminar_db::CheckpointFailureDisposition>,
}

#[derive(Debug, Deserialize)]
struct SqlRequest {
    sql: String,
}

#[derive(Debug, Serialize)]
struct SqlResponse {
    result_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    object_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows_affected: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Box<serde_json::value::RawValue>>,
    /// `true` when the result was capped at `MAX_SQL_RESULT_ROWS` or the
    /// collection timed out, so `data` is a prefix of the full result. Omitted
    /// when the result is complete.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    truncated: bool,
}

/// Trim `batches` to at most `cap` rows, returning the trimmed batches and
/// whether any rows were dropped (i.e. the input held more than `cap`).
fn cap_result(
    batches: Vec<arrow_array::RecordBatch>,
    cap: usize,
) -> (Vec<arrow_array::RecordBatch>, bool) {
    let total: usize = batches.iter().map(arrow_array::RecordBatch::num_rows).sum();
    if total <= cap {
        return (batches, false);
    }
    let mut kept = 0;
    let mut out = Vec::with_capacity(batches.len());
    for b in batches {
        let room = cap - kept;
        if b.num_rows() >= room {
            out.push(b.slice(0, room));
            break;
        }
        kept += b.num_rows();
        out.push(b);
    }
    (out, true)
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

fn error_response(status: StatusCode, msg: impl Into<String>) -> impl IntoResponse {
    (status, Json(ErrorBody { error: msg.into() }))
}

async fn request_logging(
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> impl IntoResponse {
    let method = req.method().clone();
    // Log only the path — the query string can carry the `?token=` secret used
    // by browser WebSocket clients, which must not land in access logs.
    let path = req.uri().path().to_owned();
    let start = Instant::now();

    let response = next.run(req).await;

    let duration_ms = start.elapsed().as_millis();
    let status = response.status();
    info!("{method} {path} -> {status} ({duration_ms}ms)");

    response
}

async fn health_check(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let pipeline_state = state.db.pipeline_state();
    let status = if pipeline_state == "Stopped" {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };

    (
        status,
        Json(HealthResponse {
            status: if status == StatusCode::OK {
                "healthy"
            } else {
                "unhealthy"
            },
            version: env!("CARGO_PKG_VERSION"),
            pipeline_state,
        }),
    )
}

async fn readiness_check(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if let Some(reason) = state.serving_rejection() {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, reason).into_response();
    }
    let pipeline_state = state.db.pipeline_state();
    if pipeline_state == "Running" {
        (
            StatusCode::OK,
            Json(HealthResponse {
                status: "ready",
                version: env!("CARGO_PKG_VERSION"),
                pipeline_state,
            }),
        )
            .into_response()
    } else {
        // Generic on the unauthenticated probe — the fault reason (which may echo
        // SQL/connector config) is exposed only on the authed status endpoint.
        error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("pipeline is {pipeline_state}, not Running"),
        )
        .into_response()
    }
}

async fn prometheus_metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    // Update uptime gauge on each scrape — cheap, and always fresh.
    #[allow(clippy::cast_possible_wrap)]
    state
        .server_metrics
        .uptime_seconds
        .set(state.db.uptime().as_secs() as i64);

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        crate::metrics::render(&state.registry),
    )
}

async fn list_sources(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let sources: Vec<SourceResponse> = state
        .db
        .sources()
        .into_iter()
        .map(|s| SourceResponse {
            name: s.name,
            watermark_column: s.watermark_column,
        })
        .collect();
    Json(sources)
}

async fn list_sinks(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let sinks: Vec<SinkResponse> = state
        .db
        .sinks()
        .into_iter()
        .map(|s| SinkResponse { name: s.name })
        .collect();
    Json(sinks)
}

async fn list_streams(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let streams: Vec<StreamResponse> = state
        .db
        .streams()
        .into_iter()
        .map(|s| StreamResponse {
            name: s.name,
            sql: s.sql,
        })
        .collect();
    Json(streams)
}

async fn get_stream(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let streams = state.db.streams();
    match streams.into_iter().find(|s| s.name == name) {
        Some(s) => Json(StreamResponse {
            name: s.name,
            sql: s.sql,
        })
        .into_response(),
        None => error_response(StatusCode::NOT_FOUND, format!("stream '{name}' not found"))
            .into_response(),
    }
}

async fn list_mvs(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    // `MaterializedViewInfo` already serializes to `{name, sql, state}`.
    Json(state.db.materialized_views())
}

/// Connector catalog: the registered source and sink connector types and the
/// configuration keys each accepts. Drives the console's source-creation wizard.
#[derive(Debug, Serialize)]
struct ConnectorsResponse {
    sources: Vec<ConnectorInfo>,
    sinks: Vec<ConnectorInfo>,
}

async fn list_connectors(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let registry = state.db.connector_registry();
    let sources: Vec<ConnectorInfo> = registry
        .list_sources()
        .iter()
        .filter_map(|name| registry.source_info(name))
        .collect();
    let sinks: Vec<ConnectorInfo> = registry
        .list_sinks()
        .iter()
        .filter_map(|name| registry.sink_info(name))
        .collect();
    Json(ConnectorsResponse { sources, sinks })
}

async fn trigger_checkpoint(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.db.checkpoint().await {
        Ok(result) => {
            let status = if result.success {
                StatusCode::OK
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            #[allow(clippy::cast_possible_truncation)]
            let duration_ms = result.duration.as_millis() as u64;
            (
                status,
                Json(CheckpointResponse {
                    success: result.success,
                    checkpoint_id: result.checkpoint_id,
                    epoch: result.epoch,
                    duration_ms,
                    error: result.error,
                    failure_disposition: result.failure_disposition,
                }),
            )
                .into_response()
        }
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Hard cap on rows materialized into an HTTP `SELECT` response. Streaming
/// queries are unbounded; without a cap a single request could consume
/// arbitrary memory. UIs should paginate via SQL or use the WS subscription
/// for live tailing.
const MAX_SQL_RESULT_ROWS: usize = 1000;

/// Wall-clock budget for collecting rows from a streaming `Query` result.
/// Sparse/empty streams would otherwise block the HTTP request indefinitely;
/// we return whatever has arrived by the deadline.
const SQL_RESULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

async fn execute_sql(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SqlRequest>,
) -> impl IntoResponse {
    match state.db.execute(&req.sql).await {
        Ok(result) => {
            use laminar_db::ExecuteResult;
            let resp = match result {
                ExecuteResult::Ddl(info) => SqlResponse {
                    result_type: info.statement_type,
                    object_name: Some(info.object_name),
                    rows_affected: None,
                    data: None,
                    truncated: false,
                },
                ExecuteResult::RowsAffected(n) => SqlResponse {
                    result_type: "rows_affected".to_string(),
                    object_name: None,
                    rows_affected: Some(n),
                    data: None,
                    truncated: false,
                },
                ExecuteResult::Metadata(batch) => {
                    let data = match batches_to_json_raw(&[batch]) {
                        Ok(json) => json,
                        Err(e) => {
                            return error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("failed to serialize result: {e}"),
                            )
                            .into_response();
                        }
                    };
                    SqlResponse {
                        result_type: "metadata".to_string(),
                        object_name: None,
                        rows_affected: None,
                        data: Some(data),
                        truncated: false,
                    }
                }
                ExecuteResult::Query(mut handle) => {
                    let mut batches: Vec<arrow_array::RecordBatch> = Vec::new();
                    let mut total_rows = 0;
                    let mut timed_out = false;
                    if let Ok(mut sub) = handle.subscribe_raw() {
                        // Gather one batch *past* the cap so `cap_result` can tell
                        // "exactly at the cap" (complete) from "more rows exist".
                        let collect = async {
                            while let Ok(batch) = sub.recv_async().await {
                                total_rows += batch.num_rows();
                                batches.push(batch);
                                if total_rows > MAX_SQL_RESULT_ROWS {
                                    break;
                                }
                            }
                        };
                        timed_out = tokio::time::timeout(SQL_RESULT_TIMEOUT, collect)
                            .await
                            .is_err();
                    }
                    // Trim to the cap; `over_cap` true means rows were dropped.
                    // A timeout also leaves `data` a prefix of the full result.
                    let (batches, over_cap) = cap_result(batches, MAX_SQL_RESULT_ROWS);
                    let raw_data = if batches.is_empty() {
                        serde_json::value::RawValue::from_string("[]".to_string()).ok()
                    } else {
                        batches_to_json_raw(&batches).ok()
                    };
                    SqlResponse {
                        result_type: "query".to_string(),
                        object_name: None,
                        rows_affected: None,
                        data: raw_data,
                        truncated: over_cap || timed_out,
                    }
                }
            };
            Json(resp).into_response()
        }
        Err(e) => error_response(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn handle_reload(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    // Acquire concurrency guard
    let _guard = match state.reload_guard.try_acquire() {
        Some(g) => g,
        None => {
            return error_response(StatusCode::CONFLICT, "a reload is already in progress")
                .into_response();
        }
    };

    // Load and validate the new config
    let new_config = match crate::config::load_config(&state.config_path) {
        Ok(c) => c,
        Err(e) => {
            warn!("Reload failed: config error: {e}");
            return error_response(StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
    };

    // Diff against current config
    // Tight guard scope so the `!Send` parking_lot guard doesn't cross
    // the next `.await`.
    let diff = {
        let current = state.current_config.read();
        reload::diff_configs(&current, &new_config)
    };

    if diff.is_empty() && diff.warnings.is_empty() {
        return Json(reload::ReloadResult {
            success: true,
            applied: vec![],
            failed: vec![],
            warnings: vec!["no changes detected".to_string()],
        })
        .into_response();
    }

    // Apply the diff
    let result = reload::apply_reload(&state.db, &diff).await;

    // Update metrics
    state.server_metrics.reload_total.inc();

    // Update current config on success
    if result.success {
        let mut current = state.current_config.write();
        *current = new_config;
        info!(
            "Configuration reloaded successfully ({} ops)",
            result.applied.len()
        );
    } else {
        warn!(
            "Configuration reload partially failed: {} applied, {} failed",
            result.applied.len(),
            result.failed.len()
        );
    }

    let status = if result.success {
        StatusCode::OK
    } else {
        StatusCode::MULTI_STATUS
    };

    (status, Json(result)).into_response()
}

async fn cluster_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let config = state.current_config.read();
    if config.server.mode != ServerMode::Cluster {
        return error_response(
            StatusCode::NOT_FOUND,
            "cluster endpoint is only available when server.mode = \"cluster\"",
        )
        .into_response();
    }

    let node_id = config.node_id.clone().unwrap_or_default();
    drop(config);

    #[derive(Serialize)]
    struct ClusterStatusResponse {
        mode: &'static str,
        node_id: String,
        pipeline_state: &'static str,
    }

    let pipeline_state = state.db.pipeline_state();
    Json(ClusterStatusResponse {
        mode: "cluster",
        node_id,
        pipeline_state,
    })
    .into_response()
}

#[derive(Debug, serde::Deserialize)]
struct PipelineControlParams {
    #[serde(default)]
    local: bool,
}

/// Fan a fire-and-forget pipeline-control POST out to every peer (so a
/// cluster-wide start/stop reaches the whole cluster). `local` short-circuits
/// the fan-out, and it is a no-op outside cluster mode. Peer failures are
/// logged, not fatal.
async fn fan_out_pipeline_control(state: &AppState, local: bool, path: &str) {
    #[cfg(feature = "cluster")]
    {
        if local {
            return;
        }
        let Some(cluster) = state.cluster.as_ref() else {
            return;
        };
        let self_id = cluster.controller.instance_id();
        let peers: Vec<String> = cluster
            .membership_rx
            .borrow()
            .iter()
            .filter(|m| self_id != m.id)
            .map(|m| m.rpc_address.clone())
            .collect();
        let token = state.current_config.read().server.console_token.clone();
        let client = reqwest::Client::new();
        // Fan out concurrently with `join_all`; these are I/O-bound futures with
        // no CPU work, so there's nothing to gain from spawning a task each.
        let futures = peers.into_iter().map(|peer| {
            let client = client.clone();
            let token = token.clone();
            let url = format!("http://{peer}{path}");
            async move {
                let mut req = client
                    .post(&url)
                    .timeout(std::time::Duration::from_secs(10));
                if let Some(t) = token {
                    req = req.bearer_auth(t.expose());
                }
                if let Err(e) = req.send().await {
                    tracing::warn!("failed to forward pipeline control to {url}: {e}");
                }
            }
        });
        futures::future::join_all(futures).await;
    }
    #[cfg(not(feature = "cluster"))]
    let _ = (state, local, path);
}

async fn stop_pipeline(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PipelineControlParams>,
) -> impl IntoResponse {
    // Stop locally first; only fan out to peers once this node succeeds, so a
    // local failure doesn't leave the cluster in a split (some stopped) state.
    if let Err(e) = state.db.stop_pipeline().await {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    fan_out_pipeline_control(&state, params.local, "/api/v1/pipeline/stop?local=true").await;
    (
        StatusCode::OK,
        Json(serde_json::json!({ "message": "Pipeline suspended successfully" })),
    )
        .into_response()
}

async fn start_pipeline(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PipelineControlParams>,
) -> impl IntoResponse {
    // Start locally first; only fan out to peers once this node succeeds.
    if let Err(e) = state.db.start().await {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    fan_out_pipeline_control(&state, params.local, "/api/v1/pipeline/start?local=true").await;
    (
        StatusCode::OK,
        Json(serde_json::json!({ "message": "Pipeline started successfully" })),
    )
        .into_response()
}

async fn pipeline_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let pipeline_state = state.db.pipeline_state();
    let mut body = serde_json::json!({ "pipeline_state": pipeline_state });
    // The panic is async (after the DDL/start call returned), so surface it here.
    if let Some(reason) = state.db.last_fault() {
        body["last_error"] = serde_json::Value::String(reason);
    }
    (StatusCode::OK, Json(body)).into_response()
}

// ---------------------------------------------------------------------------
// Pipeline lineage graph
// ---------------------------------------------------------------------------

/// A node in the lineage graph returned by `GET /api/v1/graph`.
#[derive(Debug, Serialize)]
struct NodeResponse {
    name: String,
    /// `"Source"`, `"Stream"`, or `"Sink"`.
    node_type: String,
    sql: Option<String>,
}

/// A directed edge in the lineage graph returned by `GET /api/v1/graph`.
#[derive(Debug, Serialize)]
struct EdgeResponse {
    from: String,
    to: String,
}

#[derive(Debug, Serialize)]
struct GraphResponse {
    nodes: Vec<NodeResponse>,
    edges: Vec<EdgeResponse>,
}

/// Returns the pipeline lineage graph (sources → streams → sinks) as
/// `{ "nodes": [...], "edges": [...] }` for the console's lineage view.
async fn get_graph(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let topology = state.db.pipeline_topology();
    let nodes = topology
        .nodes
        .into_iter()
        .map(|n| NodeResponse {
            name: n.name,
            node_type: match n.node_type {
                PipelineNodeType::Source => "Source",
                PipelineNodeType::Stream => "Stream",
                PipelineNodeType::Sink => "Sink",
            }
            .to_string(),
            sql: n.sql,
        })
        .collect();
    let edges = topology
        .edges
        .into_iter()
        .map(|e| EdgeResponse {
            from: e.from,
            to: e.to,
        })
        .collect();
    Json(GraphResponse { nodes, edges }).into_response()
}

// ---------------------------------------------------------------------------
// Cluster topology endpoints
// ---------------------------------------------------------------------------

/// 404 returned by the cluster endpoints when the server is not running in
/// cluster mode (single-node, or compiled without the `cluster` feature).
#[cfg(feature = "cluster")]
const CLUSTER_DISABLED_MSG: &str = "cluster endpoints are only available in cluster mode";
#[cfg(not(feature = "cluster"))]
const CLUSTER_DISABLED_MSG: &str = "cluster endpoints require the `cluster` feature";

#[cfg(feature = "cluster")]
const LOCAL_EVIDENCE_SCHEMA_VERSION: &str = "laminardb-local-authority-evidence/v1";
#[cfg(feature = "cluster")]
const MAX_LOCAL_EVIDENCE_RESPONSE_BYTES: usize = 4_096;
#[cfg(feature = "cluster")]
const LOCAL_EVIDENCE_TOKEN_REQUIRED_MSG: &str =
    "local process authority evidence requires server.console_token";
#[cfg(feature = "cluster")]
const LOCAL_EVIDENCE_UNAVAILABLE_MSG: &str = "local process authority evidence is unavailable";
#[cfg(feature = "cluster")]
const LOCAL_EVIDENCE_INVALID_MSG: &str = "local process authority evidence is invalid";
#[cfg(feature = "cluster")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_SCHEMA_VERSION: &str =
    "laminardb-local-checkpoint-barrier-timings/v1";
#[cfg(feature = "cluster")]
const MAX_LOCAL_CHECKPOINT_BARRIER_TIMINGS_RESPONSE_BYTES: usize = 64 * 1_024;
#[cfg(feature = "cluster")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_TOKEN_REQUIRED_MSG: &str =
    "local checkpoint barrier timings require server.console_token";
#[cfg(feature = "cluster")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_UNAVAILABLE_MSG: &str =
    "local checkpoint barrier timings are unavailable";
#[cfg(feature = "cluster")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_INVALID_MSG: &str =
    "local checkpoint barrier timings are invalid";
#[cfg(feature = "cluster")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_QUERY_MSG: &str =
    "invalid local checkpoint barrier timing query";
#[cfg(feature = "cluster")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_CONFLICT_MSG: &str =
    "local checkpoint barrier timing cursor conflicts with this process";
#[cfg(feature = "cluster")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_OVERWRITTEN_MSG: &str =
    "local checkpoint barrier timing cursor has been overwritten";

#[cfg(feature = "cluster")]
#[derive(Serialize)]
struct LocalEvidenceResponse {
    schema_version: &'static str,
    evidence: laminar_core::cluster::control::LocalProcessAuthorityEvidence,
}

#[cfg(feature = "cluster")]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalCheckpointBarrierTimingsQuery {
    after_sequence: u64,
    expected_node_id: Option<u64>,
    expected_boot_incarnation: Option<String>,
    expected_process_term: Option<u64>,
}

#[cfg(feature = "cluster")]
#[derive(Serialize)]
struct LocalCheckpointBarrierTimingsResponse<'a> {
    schema_version: &'static str,
    process_identity: laminar_core::cluster::control::LocalProcessAuthorityIdentity,
    after_sequence: u64,
    page: LocalCheckpointBarrierTimingsPage<'a>,
}

#[cfg(feature = "cluster")]
#[derive(Serialize)]
struct LocalCheckpointBarrierTimingsPage<'a> {
    capacity: usize,
    oldest_retained_sequence: Option<u64>,
    next_sequence: u64,
    overwritten_record_count: u64,
    recording_loss_count: u64,
    metadata_exhausted: bool,
    has_more: bool,
    records: &'a [laminar_db::checkpoint_timing::CheckpointBarrierTimingRecord],
}

#[cfg(feature = "cluster")]
impl<'a> LocalCheckpointBarrierTimingsResponse<'a> {
    fn new(
        after_sequence: u64,
        timing_page: &'a laminar_db::checkpoint_timing::CheckpointBarrierTimingPage,
    ) -> Self {
        let snapshot = &timing_page.snapshot;
        Self {
            schema_version: LOCAL_CHECKPOINT_BARRIER_TIMINGS_SCHEMA_VERSION,
            process_identity: timing_page.process,
            after_sequence,
            page: LocalCheckpointBarrierTimingsPage {
                capacity: snapshot.capacity,
                oldest_retained_sequence: snapshot.oldest_retained_sequence,
                next_sequence: snapshot.next_sequence,
                overwritten_record_count: snapshot.overwritten_record_count,
                recording_loss_count: snapshot.recording_loss_count,
                metadata_exhausted: snapshot.metadata_exhausted,
                has_more: snapshot.has_more,
                records: &snapshot.records,
            },
        }
    }
}

#[cfg(feature = "cluster")]
fn local_checkpoint_barrier_timing_error_response(
    error: &laminar_db::checkpoint_timing::CheckpointBarrierTimingReadError,
) -> (StatusCode, &'static str) {
    use laminar_db::checkpoint_timing::{
        CheckpointBarrierTimingReadError, CheckpointBarrierTimingSnapshotError,
    };

    match error {
        CheckpointBarrierTimingReadError::ProcessIdentityMismatch { .. }
        | CheckpointBarrierTimingReadError::Snapshot(
            CheckpointBarrierTimingSnapshotError::CursorAhead { .. },
        ) => (
            StatusCode::CONFLICT,
            LOCAL_CHECKPOINT_BARRIER_TIMINGS_CONFLICT_MSG,
        ),
        CheckpointBarrierTimingReadError::Snapshot(
            CheckpointBarrierTimingSnapshotError::CursorOverwritten { .. },
        ) => (
            StatusCode::GONE,
            LOCAL_CHECKPOINT_BARRIER_TIMINGS_OVERWRITTEN_MSG,
        ),
        CheckpointBarrierTimingReadError::ProcessIdentityUnavailable
        | CheckpointBarrierTimingReadError::ProcessIdentityChanged { .. }
        | CheckpointBarrierTimingReadError::Snapshot(CheckpointBarrierTimingSnapshotError::Busy) => {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                LOCAL_CHECKPOINT_BARRIER_TIMINGS_UNAVAILABLE_MSG,
            )
        }
        CheckpointBarrierTimingReadError::ProcessIdentityRequired
        | CheckpointBarrierTimingReadError::LedgerProcessMismatch { .. }
        | CheckpointBarrierTimingReadError::Snapshot(
            CheckpointBarrierTimingSnapshotError::InvalidLimit { .. },
        ) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            LOCAL_CHECKPOINT_BARRIER_TIMINGS_INVALID_MSG,
        ),
    }
}

/// `GET /api/v1/cluster/nodes` — current cluster membership.
async fn cluster_nodes(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[cfg(feature = "cluster")]
    {
        let Some(cluster) = state.cluster.as_ref() else {
            return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
        };
        // Clone the snapshot out of the watch guard so it isn't held across
        // serialization.
        let nodes: Vec<laminar_core::cluster::discovery::NodeInfo> =
            cluster.membership_rx.borrow().clone();
        Json(nodes).into_response()
    }
    #[cfg(not(feature = "cluster"))]
    {
        let _ = state;
        error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
    }
}

/// `GET /api/v1/cluster/vnodes` — the latest vnode-to-instance assignment snapshot.
async fn cluster_vnodes(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[cfg(feature = "cluster")]
    {
        let Some(cluster) = state.cluster.as_ref() else {
            return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
        };
        let snapshot = match cluster.snapshot_store.load().await {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "durable assignment snapshot is missing",
                )
                .into_response();
            }
            Err(error) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to load assignment snapshot: {error}"),
                )
                .into_response();
            }
        };
        Json(snapshot).into_response()
    }
    #[cfg(not(feature = "cluster"))]
    {
        let _ = state;
        error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
    }
}

/// `GET /api/v1/cluster/local-evidence` — bounded evidence retained by this exact process.
///
/// This route intentionally remains behind the normal startup/recovery serving gate. It never
/// rereads the durable assignment snapshot or treats shared publication as local convergence.
async fn cluster_local_evidence(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    #[cfg(feature = "cluster")]
    {
        let Some(cluster) = state.cluster.as_ref() else {
            return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
        };

        // The protected router authenticates this request first. Require a configured token for
        // this sensitive route and accept only the bearer form before capturing evidence. The
        // same checks are repeated immediately before 200 to close a configuration-reload race.
        let expected = state.current_config.read().server.console_token.clone();
        let Some(expected) = expected else {
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                LOCAL_EVIDENCE_TOKEN_REQUIRED_MSG,
            )
            .into_response();
        };
        if !bearer_token(&headers).is_some_and(|token| ct_eq(token, expected.expose())) {
            return error_response(StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }

        let evidence = match cluster
            .controller
            .read_local_process_authority_evidence()
            .await
        {
            Ok(evidence) => evidence,
            Err(
                laminar_core::cluster::control::LocalProcessAuthorityEvidenceError::Unavailable(
                    error,
                ),
            ) => {
                warn!(%error, "local process authority evidence is unavailable");
                return error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    LOCAL_EVIDENCE_UNAVAILABLE_MSG,
                )
                .into_response();
            }
            Err(laminar_core::cluster::control::LocalProcessAuthorityEvidenceError::Invalid(
                error,
            )) => {
                warn!(%error, "local process authority evidence is invalid");
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    LOCAL_EVIDENCE_INVALID_MSG,
                )
                .into_response();
            }
        };

        let adoption = &evidence.adopted_assignment;
        if evidence.participant.node_id == 0
            || evidence.participant.boot_incarnation.is_nil()
            || evidence.process_term == 0
            || !adoption.is_canonical()
            || adoption.participant != evidence.participant
        {
            warn!("local process authority evidence violated its canonical response contract");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                LOCAL_EVIDENCE_INVALID_MSG,
            )
            .into_response();
        }

        let envelope = LocalEvidenceResponse {
            schema_version: LOCAL_EVIDENCE_SCHEMA_VERSION,
            evidence,
        };
        let encoded = match serde_json::to_vec(&envelope) {
            Ok(encoded) => encoded,
            Err(error) => {
                warn!(%error, "failed to serialize local process authority evidence");
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    LOCAL_EVIDENCE_INVALID_MSG,
                )
                .into_response();
            }
        };
        if encoded.len() > MAX_LOCAL_EVIDENCE_RESPONSE_BYTES {
            warn!(
                encoded_bytes = encoded.len(),
                maximum_bytes = MAX_LOCAL_EVIDENCE_RESPONSE_BYTES,
                "local process authority evidence exceeded its response bound"
            );
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                LOCAL_EVIDENCE_INVALID_MSG,
            )
            .into_response();
        }

        // Close a capture/response race with terminal fencing or a newly started recovery. The
        // middleware performs the same check before capture; neither check grants data authority.
        if let Some(reason) = state.serving_rejection() {
            return error_response(StatusCode::SERVICE_UNAVAILABLE, reason).into_response();
        }

        let expected = state.current_config.read().server.console_token.clone();
        let Some(expected) = expected else {
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                LOCAL_EVIDENCE_TOKEN_REQUIRED_MSG,
            )
            .into_response();
        };
        if !bearer_token(&headers).is_some_and(|token| ct_eq(token, expected.expose())) {
            return error_response(StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }

        let mut response = (StatusCode::OK, encoded).into_response();
        response.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/json"),
        );
        response.headers_mut().insert(
            axum::http::header::CACHE_CONTROL,
            axum::http::HeaderValue::from_static("no-store"),
        );
        response
    }
    #[cfg(not(feature = "cluster"))]
    {
        let _ = (state, headers);
        error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
    }
}

/// `GET /api/v1/cluster/local-checkpoint-barrier-timings` — bounded local pause evidence.
///
/// This route reads only the process-local fixed-capacity ledger. Sequence zero bootstraps the
/// current identity; every continuation cursor is inseparable from that returned identity,
/// preventing an old process's cursor from skipping records after a restart. It does not read
/// checkpoint authority or imply durable settlement.
#[cfg(feature = "cluster")]
async fn cluster_local_checkpoint_barrier_timings(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    query: Result<
        Query<LocalCheckpointBarrierTimingsQuery>,
        axum::extract::rejection::QueryRejection,
    >,
) -> impl IntoResponse {
    #[cfg(feature = "cluster")]
    {
        use laminar_db::checkpoint_timing::MAX_CHECKPOINT_BARRIER_TIMING_PAGE_RECORDS;

        let Some(_cluster) = state.cluster.as_ref() else {
            return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
        };

        // The router has already authenticated configured tokens. This evidence route is stricter:
        // it is disabled without a token and accepts only the bearer header. Repeat this check
        // immediately before 200 to close a concurrent configuration-reload race.
        let expected_token = state.current_config.read().server.console_token.clone();
        let Some(expected_token) = expected_token else {
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                LOCAL_CHECKPOINT_BARRIER_TIMINGS_TOKEN_REQUIRED_MSG,
            )
            .into_response();
        };
        if !bearer_token(&headers).is_some_and(|token| ct_eq(token, expected_token.expose())) {
            return error_response(StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }

        let Query(query) = match query {
            Ok(query) => query,
            Err(error) => {
                warn!(%error, "rejected local checkpoint barrier timing query");
                return error_response(
                    StatusCode::BAD_REQUEST,
                    LOCAL_CHECKPOINT_BARRIER_TIMINGS_QUERY_MSG,
                )
                .into_response();
            }
        };
        let expected_process = match (
            query.expected_node_id,
            query.expected_boot_incarnation.as_deref(),
            query.expected_process_term,
        ) {
            (None, None, None) if query.after_sequence == 0 => None,
            (Some(node_id), Some(boot_incarnation), Some(process_term)) => {
                let Ok(boot_incarnation) = uuid::Uuid::parse_str(boot_incarnation) else {
                    return error_response(
                        StatusCode::BAD_REQUEST,
                        LOCAL_CHECKPOINT_BARRIER_TIMINGS_QUERY_MSG,
                    )
                    .into_response();
                };
                let process = laminar_core::cluster::control::LocalProcessAuthorityIdentity {
                    participant: laminar_core::checkpoint::CheckpointParticipant {
                        node_id,
                        boot_incarnation,
                    },
                    process_term,
                };
                if !process.is_canonical() {
                    return error_response(
                        StatusCode::BAD_REQUEST,
                        LOCAL_CHECKPOINT_BARRIER_TIMINGS_QUERY_MSG,
                    )
                    .into_response();
                }
                Some(process)
            }
            _ => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    LOCAL_CHECKPOINT_BARRIER_TIMINGS_QUERY_MSG,
                )
                .into_response();
            }
        };

        let timing_page = match state.db.checkpoint_barrier_timing_snapshot(
            expected_process,
            query.after_sequence,
            MAX_CHECKPOINT_BARRIER_TIMING_PAGE_RECORDS,
        ) {
            Ok(page) => page,
            Err(error) => {
                let (status, message) = local_checkpoint_barrier_timing_error_response(&error);
                if status == StatusCode::INTERNAL_SERVER_ERROR {
                    warn!(%error, "local checkpoint barrier timing page violated its contract");
                }
                return error_response(status, message).into_response();
            }
        };

        if expected_process.is_some_and(|expected| timing_page.process != expected)
            || timing_page
                .snapshot
                .records
                .iter()
                .any(|record| record.process != timing_page.process)
        {
            warn!("local checkpoint barrier timing response mixed process identities");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                LOCAL_CHECKPOINT_BARRIER_TIMINGS_INVALID_MSG,
            )
            .into_response();
        }
        let envelope =
            LocalCheckpointBarrierTimingsResponse::new(query.after_sequence, &timing_page);
        let encoded = match serde_json::to_vec(&envelope) {
            Ok(encoded) => encoded,
            Err(error) => {
                warn!(%error, "failed to serialize local checkpoint barrier timings");
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    LOCAL_CHECKPOINT_BARRIER_TIMINGS_INVALID_MSG,
                )
                .into_response();
            }
        };
        if encoded.len() > MAX_LOCAL_CHECKPOINT_BARRIER_TIMINGS_RESPONSE_BYTES {
            warn!(
                encoded_bytes = encoded.len(),
                maximum_bytes = MAX_LOCAL_CHECKPOINT_BARRIER_TIMINGS_RESPONSE_BYTES,
                "local checkpoint barrier timing response exceeded its bound"
            );
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                LOCAL_CHECKPOINT_BARRIER_TIMINGS_INVALID_MSG,
            )
            .into_response();
        }

        if let Some(reason) = state.serving_rejection() {
            return error_response(StatusCode::SERVICE_UNAVAILABLE, reason).into_response();
        }
        let expected_token = state.current_config.read().server.console_token.clone();
        let Some(expected_token) = expected_token else {
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                LOCAL_CHECKPOINT_BARRIER_TIMINGS_TOKEN_REQUIRED_MSG,
            )
            .into_response();
        };
        if !bearer_token(&headers).is_some_and(|token| ct_eq(token, expected_token.expose())) {
            return error_response(StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }

        let mut response = (StatusCode::OK, encoded).into_response();
        response.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/json"),
        );
        response.headers_mut().insert(
            axum::http::header::CACHE_CONTROL,
            axum::http::HeaderValue::from_static("no-store"),
        );
        response
    }
}

#[cfg(not(feature = "cluster"))]
async fn cluster_local_checkpoint_barrier_timings(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let _ = state;
    error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
}

/// `GET /api/v1/cluster/leader` — the current leader's `NodeInfo` (if known)
/// and whether this node is the leader.
async fn cluster_leader(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[cfg(feature = "cluster")]
    {
        #[derive(Serialize)]
        struct LeaderResponse {
            leader: Option<laminar_core::cluster::discovery::NodeInfo>,
            is_leader: bool,
        }

        let Some(cluster) = state.cluster.as_ref() else {
            return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
        };
        let leader_id = cluster.controller.current_leader();
        let is_leader = cluster.controller.is_leader();
        let leader = leader_id.and_then(|id| {
            cluster
                .membership_rx
                .borrow()
                .iter()
                .find(|n| n.id == id)
                .cloned()
        });
        Json(LeaderResponse { leader, is_leader }).into_response()
    }
    #[cfg(not(feature = "cluster"))]
    {
        let _ = state;
        error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
    }
}

/// `GET /api/v1/cluster/checkpoints` — latest checkpoint metadata. Available
/// in both single-node and cluster mode.
async fn cluster_checkpoints(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.db.build_show_checkpoint_status().await {
        Ok(batch) => match batches_to_json_raw(&[batch]) {
            Ok(raw) => Json(raw).into_response(),
            Err(e) => error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to serialize checkpoint status: {e}"),
            )
            .into_response(),
        },
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// WebSocket stream subscriptions
// ---------------------------------------------------------------------------

const MAX_WS_CONNECTIONS: usize = 10_000;
const MAX_WS_FRAME_BYTES: usize = 1024 * 1024;
const MAX_WS_CONTROL_FIELD_BYTES: usize = 4096;
const MAX_WS_SUBSCRIPTION_ID_BYTES: usize = 1024;
const MAX_WS_INBOUND_BYTES: usize = 4096;
const WS_HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);
const WS_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const MAX_UNANSWERED_WS_PINGS: u8 = 2;

#[derive(Default)]
struct WsPongDeadline {
    unanswered: u8,
}

impl WsPongDeadline {
    fn before_ping(&mut self) -> bool {
        if self.unanswered >= MAX_UNANSWERED_WS_PINGS {
            return false;
        }
        self.unanswered += 1;
        true
    }

    fn on_pong(&mut self) {
        self.unanswered = 0;
    }
}

fn try_acquire_ws_slot(
    slots: &Arc<tokio::sync::Semaphore>,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    Arc::clone(slots).try_acquire_owned().ok()
}

fn ws_error_json(name: &str, code: &str, message: &str, sequence: u64) -> String {
    let out = serde_json::json!({
        "type": "error",
        "subscription_id": name,
        "code": truncate_utf8(code, MAX_WS_CONTROL_FIELD_BYTES),
        "message": truncate_utf8(message, MAX_WS_CONTROL_FIELD_BYTES),
        "sequence": sequence.to_string(),
    })
    .to_string();
    debug_assert!(out.len() <= MAX_WS_FRAME_BYTES);
    out
}

fn ws_gap_json(name: &str, skipped: u64, sequence: u64) -> String {
    let out = serde_json::json!({
        "type": "gap",
        "subscription_id": name,
        "code": "subscription_lagged",
        "message": format!("subscription lagged: skipped {skipped} messages"),
        "skipped_messages": skipped.to_string(),
        "sequence": sequence.to_string(),
    })
    .to_string();
    debug_assert!(out.len() <= MAX_WS_FRAME_BYTES);
    out
}

fn ws_progress_json(
    name: &str,
    epoch: u64,
    checkpoint_id: u64,
    log_sequence: u64,
    through_sequence: u64,
    sequence: u64,
) -> String {
    let out = serde_json::json!({
        "type": "progress",
        "subscription_id": name,
        "epoch": epoch.to_string(),
        "checkpoint_id": checkpoint_id.to_string(),
        "log_sequence": log_sequence.to_string(),
        "through_log_sequence": through_sequence.to_string(),
        "sequence": sequence.to_string(),
    })
    .to_string();
    debug_assert!(out.len() <= MAX_WS_FRAME_BYTES);
    out
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

#[derive(Debug, PartialEq, Eq)]
enum WsFrameBuildError {
    TooLarge,
    Serialization(String),
}

#[derive(Default)]
struct WsBatchFrameState {
    /// First row not yet included in a completed frame.
    offset: usize,
    /// A row encoded while filling the previous frame that did not fit.
    pending_row: Option<Vec<u8>>,
}

const WS_DATA_SUFFIX_FIXED_BYTES: usize =
    r#"],"sequence":"","log_sequence":"","row_offset":"","row_count":""}"#.len();

fn decimal_digits_u64(value: u64) -> usize {
    if value == 0 {
        1
    } else {
        value.ilog10() as usize + 1
    }
}

fn decimal_digits_usize(value: usize) -> usize {
    if value == 0 {
        1
    } else {
        value.ilog10() as usize + 1
    }
}

fn ws_data_suffix_len(sequence: u64, log_sequence: u64, offset: usize, rows: usize) -> usize {
    WS_DATA_SUFFIX_FIXED_BYTES
        + decimal_digits_u64(sequence)
        + decimal_digits_u64(log_sequence)
        + decimal_digits_usize(offset)
        + decimal_digits_usize(rows)
}

fn ws_data_suffix(sequence: u64, log_sequence: u64, offset: usize, rows: usize) -> String {
    format!(
        "],\"sequence\":\"{sequence}\",\"log_sequence\":\"{log_sequence}\",\"row_offset\":\"{offset}\",\"row_count\":\"{rows}\"}}"
    )
}

fn next_ws_data_frame(
    name: &str,
    batch: &arrow_array::RecordBatch,
    state: &mut WsBatchFrameState,
    sequence: u64,
    log_sequence: u64,
) -> Result<Option<String>, WsFrameBuildError> {
    if state.offset >= batch.num_rows() {
        debug_assert!(state.pending_row.is_none());
        return Ok(None);
    }

    let subid = serde_json::to_string(name)
        .map_err(|error| WsFrameBuildError::Serialization(error.to_string()))?;
    let prefix = format!("{{\"type\":\"data\",\"subscription_id\":{subid},\"data\":[");
    let frame_offset = state.offset;
    if prefix
        .len()
        .saturating_add(ws_data_suffix_len(sequence, log_sequence, frame_offset, 1))
        >= MAX_WS_FRAME_BYTES
    {
        return Err(WsFrameBuildError::TooLarge);
    }

    // Build one root encoder per output frame. It is deliberately local so no
    // non-Send Arrow encoder is retained across the socket write await.
    let root = arrow_array::StructArray::from(batch.clone());
    let root_field = Arc::new(arrow_schema::Field::new_struct(
        "",
        batch.schema().fields().clone(),
        false,
    ));
    let options = exact_json_encoder_options();
    let mut encoder = arrow_json::writer::make_encoder(&root_field, &root, &options)
        .map_err(|error| WsFrameBuildError::Serialization(error.to_string()))?;

    let mut bytes = Vec::with_capacity(MAX_WS_FRAME_BYTES.min(prefix.len() + 16 * 1024));
    bytes.extend_from_slice(prefix.as_bytes());
    let mut rows = 0_usize;

    while state.offset < batch.num_rows() {
        let row = state.pending_row.take().unwrap_or_else(|| {
            let mut row = Vec::new();
            encoder.encode(state.offset, &mut row);
            row
        });
        let separator_bytes = usize::from(rows != 0);
        let candidate_rows = rows + 1;
        let candidate_len = bytes
            .len()
            .saturating_add(separator_bytes)
            .saturating_add(row.len())
            .saturating_add(ws_data_suffix_len(
                sequence,
                log_sequence,
                frame_offset,
                candidate_rows,
            ));

        if candidate_len > MAX_WS_FRAME_BYTES {
            if rows == 0 {
                return Err(WsFrameBuildError::TooLarge);
            }
            state.pending_row = Some(row);
            break;
        }

        if separator_bytes != 0 {
            bytes.push(b',');
        }
        bytes.extend_from_slice(&row);
        state.offset += 1;
        rows = candidate_rows;
    }

    debug_assert!(rows > 0);
    bytes.extend_from_slice(ws_data_suffix(sequence, log_sequence, frame_offset, rows).as_bytes());
    debug_assert!(bytes.len() <= MAX_WS_FRAME_BYTES);
    let frame = String::from_utf8(bytes)
        .map_err(|error| WsFrameBuildError::Serialization(error.to_string()))?;
    Ok(Some(frame))
}

async fn ws_send(socket: &mut WebSocket, message: Message, state: &AppState) -> bool {
    if state.serving_rejection().is_some() {
        return false;
    }
    tokio::select! {
        biased;
        () = state.wait_for_serving_fence() => false,
        result = tokio::time::timeout(WS_WRITE_TIMEOUT, socket.send(message)) => {
            matches!(result, Ok(Ok(())))
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct WsSubscribeParams {
    as_of_epoch: Option<u64>,
}

async fn ws_upgrade(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(params): Query<WsSubscribeParams>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    if name.is_empty() || name.len() > MAX_WS_SUBSCRIPTION_ID_BYTES {
        return error_response(
            StatusCode::BAD_REQUEST,
            format!(
                "WebSocket subscription name must contain 1..={MAX_WS_SUBSCRIPTION_ID_BYTES} UTF-8 bytes"
            ),
        )
        .into_response();
    }
    let Some(slot) = try_acquire_ws_slot(&state.ws_slots) else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "too many WebSocket connections".to_string(),
        )
        .into_response();
    };

    let start = params.as_of_epoch.map_or(
        laminar_db::subscription::SubscribeStart::Tail,
        laminar_db::subscription::SubscribeStart::AsOfEpoch,
    );
    let portal = match state.db.open_subscription(&name, None, start).await {
        Ok(p) => p,
        Err(laminar_db::DbError::StreamNotFound(_)) => {
            return error_response(StatusCode::NOT_FOUND, format!("stream '{name}' not found"))
                .into_response();
        }
        Err(error @ laminar_db::DbError::SubscriptionReplayPruned { .. }) => {
            return error_response(StatusCode::GONE, error.to_string()).into_response();
        }
        Err(error @ laminar_db::DbError::SubscriptionEpochNotCommitted { .. }) => {
            return error_response(StatusCode::CONFLICT, error.to_string()).into_response();
        }
        Err(error) => {
            warn!(stream = %name, error = %error, "failed to open WebSocket subscription");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to subscribe to '{name}'"),
            )
            .into_response();
        }
    };
    if let Some(reason) = state.serving_rejection() {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, reason).into_response();
    }

    let st = Arc::clone(&state);
    ws.max_message_size(MAX_WS_INBOUND_BYTES)
        .max_frame_size(MAX_WS_INBOUND_BYTES)
        .on_upgrade(move |socket| async move {
            let _slot = slot;
            st.server_metrics.ws_connections.inc();
            ws_client(socket, portal, name, Arc::clone(&st)).await;
            st.server_metrics.ws_connections.dec();
        })
        .into_response()
}

async fn ws_client(
    mut socket: WebSocket,
    mut portal: laminar_db::subscription::SubscriptionPortal,
    name: String,
    state: Arc<AppState>,
) {
    let mut heartbeat = tokio::time::interval(WS_HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut pong_deadline = WsPongDeadline::default();
    let mut seq: u64 = 0;

    'subscription: loop {
        tokio::select! {
            biased;
            () = state.wait_for_serving_fence() => break,
            frame = portal.next_frame() => {
                match frame {
                    Some(laminar_db::subscription::PortalFrame::Batch {
                        batch,
                        sequence: log_sequence,
                        lease: _lease,
                    }) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        let mut frame_state = WsBatchFrameState::default();
                        while frame_state.offset < batch.num_rows() {
                            let out = match next_ws_data_frame(
                                &name,
                                &batch,
                                &mut frame_state,
                                seq,
                                log_sequence,
                            ) {
                                Ok(Some(frame)) => frame,
                                Ok(None) => break,
                                Err(WsFrameBuildError::TooLarge) => {
                                    let out = ws_error_json(
                                        &name,
                                        "row_too_large",
                                        "one subscription row exceeds the WebSocket frame limit",
                                        seq,
                                    );
                                    let _ = ws_send(
                                        &mut socket,
                                        Message::Text(out.into()),
                                        &state,
                                    ).await;
                                    break 'subscription;
                                }
                                Err(WsFrameBuildError::Serialization(error)) => {
                                    warn!(stream = %name, error = %error, "serialize error");
                                    let message = format!("subscription batch serialization failed: {error}");
                                    let out = ws_error_json(
                                        &name,
                                        "serialization_failed",
                                        &message,
                                        seq,
                                    );
                                    let _ = ws_send(
                                        &mut socket,
                                        Message::Text(out.into()),
                                        &state,
                                    ).await;
                                    break 'subscription;
                                }
                            };
                            if !ws_send(
                                &mut socket,
                                Message::Text(out.into()),
                                &state,
                            ).await {
                                break 'subscription;
                            }
                            let Some(next) = seq.checked_add(1) else {
                                break 'subscription;
                            };
                            seq = next;
                        }
                    }
                    Some(laminar_db::subscription::PortalFrame::Barrier {
                        sequence: log_sequence,
                        epoch,
                        checkpoint_id,
                        through_sequence,
                    }) => {
                        let out = ws_progress_json(
                            &name,
                            epoch,
                            checkpoint_id,
                            log_sequence,
                            through_sequence,
                            seq,
                        );
                        if !ws_send(
                            &mut socket,
                            Message::Text(out.into()),
                            &state,
                        ).await {
                            break;
                        }
                        let Some(next) = seq.checked_add(1) else {
                            break;
                        };
                        seq = next;
                    }
                    Some(laminar_db::subscription::PortalFrame::Lagged(n)) => {
                        warn!(stream = %name, skipped = n, "WS client fell behind, disconnecting");
                        let out = ws_gap_json(&name, n, seq);
                        let _ = ws_send(
                            &mut socket,
                            Message::Text(out.into()),
                            &state,
                        ).await;
                        break;
                    }
                    Some(laminar_db::subscription::PortalFrame::Error { message }) => {
                        warn!(stream = %name, error = %message, "WS subscription failed, disconnecting");
                        let out = ws_error_json(&name, "subscription_failed", &message, seq);
                        let _ = ws_send(
                            &mut socket,
                            Message::Text(out.into()),
                            &state,
                        ).await;
                        break;
                    }
                    None => break, // disconnected
                }
            }
            _ = heartbeat.tick() => {
                if !pong_deadline.before_ping() {
                    break;
                }
                if !ws_send(
                    &mut socket,
                    Message::Ping(bytes::Bytes::new()),
                    &state,
                ).await {
                    break;
                }
            }
            msg = socket.recv() => {
                // `data` moves into `Pong`, so the inner `if` can't fold into the guard.
                #[allow(clippy::collapsible_match)]
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(data))) => {
                        if !ws_send(&mut socket, Message::Pong(data), &state).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => pong_deadline.on_pong(),
                    Some(Ok(Message::Text(_) | Message::Binary(_))) => {
                        let out = ws_error_json(
                            &name,
                            "unsupported_client_message",
                            "subscription WebSocket accepts control frames only",
                            seq,
                        );
                        let _ = ws_send(
                            &mut socket,
                            Message::Text(out.into()),
                            &state,
                        ).await;
                        break;
                    }
                    Some(Err(error)) => {
                        warn!(stream = %name, %error, "WebSocket receive failed");
                        break;
                    }
                }
            }
        }
    }
    if state.serving_rejection().is_none() {
        let _ = ws_send(&mut socket, Message::Close(None), &state).await;
    }
}

const EXACT_DISPLAY_OPTIONS: arrow_cast::display::FormatOptions<'static> =
    arrow_cast::display::FormatOptions::new().with_display_error(true);

#[derive(Debug)]
struct ExactJsonEncoderFactory;

struct QuotedFormatterEncoder<'a> {
    formatter: arrow_cast::display::ArrayFormatter<'a>,
}

impl arrow_json::writer::Encoder for QuotedFormatterEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.push(b'"');
        write!(out, "{}", self.formatter.value(idx)).expect("writing to Vec cannot fail");
        out.push(b'"');
    }
}

fn encode_non_finite_float(value: f64, out: &mut Vec<u8>) -> bool {
    let value = if value.is_nan() {
        "\"NaN\""
    } else if value == f64::INFINITY {
        "\"Infinity\""
    } else if value == f64::NEG_INFINITY {
        "\"-Infinity\""
    } else {
        return false;
    };
    out.extend_from_slice(value.as_bytes());
    true
}

struct Float16JsonEncoder<'a>(&'a arrow_array::Float16Array);

impl arrow_json::writer::Encoder for Float16JsonEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let value = f32::from(self.0.value(idx));
        if !encode_non_finite_float(f64::from(value), out) {
            serde_json::to_writer(out, &value).expect("finite f32 is valid JSON");
        }
    }
}

struct Float32JsonEncoder<'a>(&'a arrow_array::Float32Array);

impl arrow_json::writer::Encoder for Float32JsonEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let value = self.0.value(idx);
        if !encode_non_finite_float(f64::from(value), out) {
            serde_json::to_writer(out, &value).expect("finite f32 is valid JSON");
        }
    }
}

struct Float64JsonEncoder<'a>(&'a arrow_array::Float64Array);

impl arrow_json::writer::Encoder for Float64JsonEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let value = self.0.value(idx);
        if !encode_non_finite_float(value, out) {
            serde_json::to_writer(out, &value).expect("finite f64 is valid JSON");
        }
    }
}

impl arrow_json::writer::EncoderFactory for ExactJsonEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a arrow_schema::FieldRef,
        array: &'a dyn arrow_array::Array,
        _options: &'a arrow_json::writer::EncoderOptions,
    ) -> Result<Option<arrow_json::writer::NullableEncoder<'a>>, arrow_schema::ArrowError> {
        use arrow_schema::DataType;

        let encoder: Option<Box<dyn arrow_json::writer::Encoder + 'a>> = match array.data_type() {
            // These types cannot be represented exactly by all JSON consumers.
            DataType::Int64
            | DataType::UInt64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => {
                let formatter =
                    arrow_cast::display::ArrayFormatter::try_new(array, &EXACT_DISPLAY_OPTIONS)?;
                Some(Box::new(QuotedFormatterEncoder { formatter }))
            }
            DataType::Float16 => Some(Box::new(Float16JsonEncoder(
                array
                    .as_any()
                    .downcast_ref::<arrow_array::Float16Array>()
                    .expect("Float16 data type must use Float16Array"),
            ))),
            DataType::Float32 => Some(Box::new(Float32JsonEncoder(
                array
                    .as_any()
                    .downcast_ref::<arrow_array::Float32Array>()
                    .expect("Float32 data type must use Float32Array"),
            ))),
            DataType::Float64 => Some(Box::new(Float64JsonEncoder(
                array
                    .as_any()
                    .downcast_ref::<arrow_array::Float64Array>()
                    .expect("Float64 data type must use Float64Array"),
            ))),
            _ => None,
        };

        Ok(encoder.map(|encoder| {
            arrow_json::writer::NullableEncoder::new(encoder, array.nulls().cloned())
        }))
    }
}

fn exact_json_encoder_options() -> arrow_json::writer::EncoderOptions {
    arrow_json::writer::EncoderOptions::default()
        .with_explicit_nulls(true)
        .with_encoder_factory(Arc::new(ExactJsonEncoderFactory))
}

/// Serialize Arrow batches using the same exact JSON value contract as WS data frames.
fn batches_to_json_string(batches: &[arrow_array::RecordBatch]) -> Result<String, String> {
    let mut buf = Vec::new();
    let mut writer = arrow_json::writer::WriterBuilder::new()
        .with_explicit_nulls(true)
        .with_encoder_factory(Arc::new(ExactJsonEncoderFactory))
        .build::<_, arrow_json::writer::JsonArray>(&mut buf);
    for batch in batches {
        writer.write(batch).map_err(|e| e.to_string())?;
    }
    writer.finish().map_err(|e| e.to_string())?;
    String::from_utf8(buf).map_err(|e| e.to_string())
}

fn batches_to_json_raw(
    batches: &[arrow_array::RecordBatch],
) -> Result<Box<serde_json::value::RawValue>, String> {
    let s = batches_to_json_string(batches)?;
    serde_json::value::RawValue::from_string(s).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests;
