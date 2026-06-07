//! HTTP API for LaminarDB server.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use prometheus::Registry;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};
use tracing::{info, warn};

use laminar_db::{ConnectorInfo, LaminarDB, PipelineNodeType};

use crate::config::ServerConfig;
use crate::metrics::ServerMetrics;
use crate::reload::{self, ReloadGuard};
use crate::server::ServerError;

/// Cluster control-plane handles backing the `/api/v1/cluster/*` endpoints.
/// Absent in single-node mode, where those endpoints return `404`.
#[cfg(feature = "cluster")]
#[derive(Clone)]
pub struct ClusterComponents {
    /// Leader-election / membership controller (gossip discovery only).
    pub controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    /// Durable vnode-assignment snapshot store.
    pub snapshot_store: Option<Arc<laminar_core::cluster::control::AssignmentSnapshotStore>>,
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
    /// Tracks ephemeral streams created by the console (`POST /api/v1/queries`)
    /// so abandoned ones can be reaped.
    pub ephemeral: Arc<EphemeralTracker>,
    /// Cluster control-plane handles (cluster mode only). `None` in
    /// single-node mode; the cluster endpoints 404 when absent.
    #[cfg(feature = "cluster")]
    pub cluster: Option<ClusterComponents>,
}

// ---------------------------------------------------------------------------
// Ephemeral console streams
// ---------------------------------------------------------------------------

/// How long an ephemeral console stream may sit `Pending` (created but never
/// connected to over WebSocket) before its one-shot reaper task drops it.
const EPHEMERAL_PENDING_TTL: Duration = Duration::from_secs(30);

/// Lifecycle state of a console-managed ephemeral stream.
#[derive(Clone, Copy, PartialEq, Eq)]
enum EphemeralState {
    /// Created by `POST /api/v1/queries`, no WebSocket attached yet.
    Pending,
    /// A WebSocket client is (or was) attached.
    Connected,
}

/// Tracks console-initiated ephemeral streams so abandoned ones (created but
/// never connected) can be reaped after [`EPHEMERAL_PENDING_TTL`].
#[derive(Default)]
pub struct EphemeralTracker {
    inner: parking_lot::Mutex<HashMap<String, EphemeralState>>,
}

impl EphemeralTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn add_pending(&self, name: String) {
        self.inner.lock().insert(name, EphemeralState::Pending);
    }

    /// Marks a pending stream `Connected`; returns `false` if it wasn't tracked.
    fn mark_connected(&self, name: &str) -> bool {
        let mut guard = self.inner.lock();
        if let Some(state) = guard.get_mut(name) {
            *state = EphemeralState::Connected;
            true
        } else {
            false
        }
    }

    fn remove(&self, name: &str) -> bool {
        self.inner.lock().remove(name).is_some()
    }

    /// Removes `name` only if still `Pending`, so the reaper leaves a stream
    /// that connected (or was already torn down) in the meantime untouched.
    fn remove_if_pending(&self, name: &str) -> bool {
        let mut guard = self.inner.lock();
        if matches!(guard.get(name), Some(EphemeralState::Pending)) {
            guard.remove(name);
            true
        } else {
            false
        }
    }

    #[cfg(test)]
    fn is_tracked(&self, name: &str) -> bool {
        self.inner.lock().contains_key(name)
    }
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
        .route("/api/v1/queries", post(create_query))
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

    public
        .merge(protected)
        .layer(cors)
        .layer(axum::middleware::from_fn(request_logging))
        .with_state(state)
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

pub async fn serve(router: Router, bind: &str) -> Result<tokio::task::JoinHandle<()>, ServerError> {
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|e| ServerError::Http(format!("failed to bind to {bind}: {e}")))?;

    let handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            tracing::error!("HTTP server error: {e}");
        }
    });

    Ok(handle)
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
    #[serde(skip_serializing_if = "is_false")]
    truncated: bool,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde skip_serializing_if signature
fn is_false(b: &bool) -> bool {
    !*b
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

#[derive(Debug, Deserialize)]
struct CreateQueryRequest {
    sql: String,
}

#[derive(Debug, Serialize)]
struct CreateQueryResponse {
    stream_id: String,
    ws_url: String,
}

/// Generate a unique name for an ephemeral console stream. Combines a
/// millisecond timestamp with a random suffix so concurrent requests don't
/// collide. The leading `__console_` marks it as console-managed.
fn console_stream_name() -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    let rand: u32 = rand::random();
    format!("__console_{ts}_{rand:08x}")
}

/// Register an ad-hoc live query as an ephemeral stream and return the URL the
/// console connects its WebSocket to. The stream is reaped on WS disconnect, or
/// by a one-shot reaper task if no client connects within
/// [`EPHEMERAL_PENDING_TTL`].
async fn create_query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateQueryRequest>,
) -> impl IntoResponse {
    let name = console_stream_name();
    let ddl = format!("CREATE STREAM {name} AS {}", req.sql);

    match state.db.execute(&ddl).await {
        Ok(_) => {
            state.ephemeral.add_pending(name.clone());

            // Arm a one-shot reaper: if no WebSocket connects within the TTL,
            // drop the still-`Pending` stream so an abandoned request can't
            // leak it.
            let tracker = Arc::clone(&state.ephemeral);
            let db = Arc::clone(&state.db);
            let name_clone = name.clone();
            tokio::spawn(async move {
                tokio::time::sleep(EPHEMERAL_PENDING_TTL).await;
                if tracker.remove_if_pending(&name_clone) {
                    let ddl = format!("DROP STREAM IF EXISTS {name_clone}");
                    if let Err(e) = db.execute(&ddl).await {
                        warn!(stream = %name_clone, error = %e, "failed to drop abandoned ephemeral stream");
                    } else {
                        info!(stream = %name_clone, "reaped abandoned ephemeral console stream");
                    }
                }
            });

            let ws_url = format!("/ws/{name}");
            Json(CreateQueryResponse {
                stream_id: name,
                ws_url,
            })
            .into_response()
        }
        Err(e) => error_response(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
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
    if config.server.mode != "cluster" {
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
        let self_id = cluster.controller.as_ref().map(|c| c.instance_id());
        let peers: Vec<String> = cluster
            .membership_rx
            .borrow()
            .iter()
            .filter(|m| self_id != Some(m.id))
            .map(|m| m.rpc_address.clone())
            .collect();
        let token = state.current_config.read().server.console_token.clone();
        let client = reqwest::Client::new();
        let tasks: Vec<_> = peers
            .into_iter()
            .map(|peer| {
                let client = client.clone();
                let token = token.clone();
                let url = format!("http://{peer}{path}");
                tokio::spawn(async move {
                    let mut req = client
                        .post(&url)
                        .timeout(std::time::Duration::from_secs(10));
                    if let Some(t) = token {
                        req = req.bearer_auth(t.expose());
                    }
                    if let Err(e) = req.send().await {
                        tracing::warn!("failed to forward pipeline control to {url}: {e}");
                    }
                })
            })
            .collect();
        for task in tasks {
            let _ = task.await;
        }
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
    (
        StatusCode::OK,
        Json(serde_json::json!({ "pipeline_state": pipeline_state })),
    )
        .into_response()
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

/// `GET /api/v1/cluster/vnodes` — the latest vnode→instance assignment
/// snapshot (or an empty snapshot when none has been written yet).
async fn cluster_vnodes(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[cfg(feature = "cluster")]
    {
        use laminar_core::cluster::control::AssignmentSnapshot;

        let Some(cluster) = state.cluster.as_ref() else {
            return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
        };
        let snapshot = match &cluster.snapshot_store {
            Some(store) => match store.load().await {
                Ok(Some(snap)) => snap,
                Ok(None) => AssignmentSnapshot::empty(),
                Err(e) => {
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("failed to load assignment snapshot: {e}"),
                    )
                    .into_response();
                }
            },
            None => AssignmentSnapshot::empty(),
        };
        Json(snapshot).into_response()
    }
    #[cfg(not(feature = "cluster"))]
    {
        let _ = state;
        error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
    }
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
        let (leader_id, is_leader) = match &cluster.controller {
            Some(controller) => (controller.current_leader(), controller.is_leader()),
            None => (None, false),
        };
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

const MAX_WS_CONNECTIONS: i64 = 10_000;
const WS_HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);

async fn ws_upgrade(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    if state.server_metrics.ws_connections.get() >= MAX_WS_CONNECTIONS {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "too many WebSocket connections".to_string(),
        )
        .into_response();
    }

    if !state.db.streams().iter().any(|s| s.name == name) {
        return error_response(StatusCode::NOT_FOUND, format!("stream '{name}' not found"))
            .into_response();
    }

    // Each WS client gets its own broadcast subscription — fan-out is in the Sink.
    let portal = match state
        .db
        .open_subscription(&name, None, laminar_db::subscription::SubscribeStart::Tail)
        .await
    {
        Ok(p) => p,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to subscribe to '{name}'"),
            )
            .into_response();
        }
    };

    let st = Arc::clone(&state);
    ws.on_upgrade(move |socket| async move {
        // If this is a console-initiated ephemeral stream, transition it to
        // `Connected` so the GC task won't reap it while a client is attached.
        let is_console = st.ephemeral.mark_connected(&name);

        st.server_metrics.ws_connections.inc();
        ws_client(socket, portal, name.clone()).await;
        st.server_metrics.ws_connections.dec();

        // On disconnect, tear down the ephemeral stream so it doesn't linger
        // after the console tab that owned it goes away.
        if is_console {
            let ddl = format!("DROP STREAM IF EXISTS {name}");
            if let Err(e) = st.db.execute(&ddl).await {
                warn!(stream = %name, error = %e, "failed to drop ephemeral stream on disconnect");
            }
            st.ephemeral.remove(&name);
            info!(stream = %name, "dropped ephemeral console stream on disconnect");
        }
    })
    .into_response()
}

async fn ws_client(
    mut socket: WebSocket,
    mut portal: laminar_db::subscription::SubscriptionPortal,
    name: String,
) {
    let mut heartbeat = tokio::time::interval(WS_HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut seq: u64 = 0;

    loop {
        tokio::select! {
            biased;
            frame = portal.next_frame() => {
                match frame {
                    Some(laminar_db::subscription::PortalFrame::Batch(batch)) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        let raw_json = match batches_to_json_raw(&[batch]) {
                            Ok(j) => j,
                            Err(e) => {
                                warn!(stream = %name, error = %e, "serialize error");
                                continue;
                            }
                        };
                        let out = serde_json::json!({
                            "type": "data",
                            "subscription_id": &name,
                            "data": raw_json,
                            "sequence": seq,
                        });
                        seq += 1;
                        if socket.send(Message::Text(out.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    Some(laminar_db::subscription::PortalFrame::Barrier { .. }) => {
                        // Checkpoint barriers have no WS wire representation.
                        continue;
                    }
                    Some(laminar_db::subscription::PortalFrame::Lagged(n)) => {
                        warn!(stream = %name, skipped = n, "WS client fell behind, disconnecting");
                        break;
                    }
                    None => break, // disconnected
                }
            }
            _ = heartbeat.tick() => {
                let hb = serde_json::json!({
                    "type": "heartbeat",
                    "server_time": chrono::Utc::now().timestamp_millis(),
                });
                if socket.send(Message::Text(hb.to_string().into())).await.is_err() {
                    break;
                }
            }
            msg = socket.recv() => {
                // `data` moves into `Pong`, so the inner `if` can't fold into the guard.
                #[allow(clippy::collapsible_match)]
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(data))) => {
                        if socket.send(Message::Pong(data)).await.is_err() { break; }
                    }
                    _ => {}
                }
            }
        }
    }
}

fn batches_to_json_raw(
    batches: &[arrow_array::RecordBatch],
) -> Result<Box<serde_json::value::RawValue>, String> {
    let mut buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(&mut buf);
    for batch in batches {
        writer.write(batch).map_err(|e| e.to_string())?;
    }
    writer.finish().map_err(|e| e.to_string())?;
    let s = String::from_utf8(buf).map_err(|e| e.to_string())?;
    serde_json::value::RawValue::from_string(s).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[test]
    fn cap_result_trims_and_flags() {
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let batch = |n: i32| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from((0..n).collect::<Vec<_>>()))],
            )
            .unwrap()
        };
        let rows = |bs: &[RecordBatch]| bs.iter().map(RecordBatch::num_rows).sum::<usize>();

        // Under the cap: unchanged, not truncated.
        let (b, t) = cap_result(vec![batch(3)], 5);
        assert_eq!((rows(&b), t), (3, false));
        // Exactly at the cap across batches: complete, not truncated.
        let (b, t) = cap_result(vec![batch(3), batch(2)], 5);
        assert_eq!((rows(&b), t), (5, false));
        // Over the cap: trimmed to the cap, truncated.
        let (b, t) = cap_result(vec![batch(3), batch(4)], 5);
        assert_eq!((rows(&b), t), (5, true));
    }

    fn test_state() -> Arc<AppState> {
        let registry = Arc::new(crate::metrics::build_registry([
            ("instance".into(), "test".into()),
            ("pipeline".into(), "test".into()),
        ]));
        let engine_metrics = Arc::new(laminar_db::EngineMetrics::new(&registry));
        let db = Arc::new(LaminarDB::open().unwrap());
        db.set_engine_metrics(engine_metrics);
        let server_metrics = crate::metrics::ServerMetrics::new(&registry);
        Arc::new(AppState {
            db,
            config_path: PathBuf::from("test.toml"),
            current_config: parking_lot::RwLock::new(crate::config::ServerConfig {
                server: crate::config::ServerSection::default(),
                state: laminar_core::state::StateBackendConfig::default(),
                checkpoint: crate::config::CheckpointSection::default(),
                sources: vec![],
                lookups: vec![],
                pipelines: vec![],
                sinks: vec![],
                discovery: None,
                coordination: None,
                node_id: None,
                sql: None,
                ai: Default::default(),
                models: Default::default(),
            }),
            reload_guard: ReloadGuard::new(),

            registry,
            server_metrics,
            ephemeral: Arc::new(EphemeralTracker::new()),
            #[cfg(feature = "cluster")]
            cluster: None,
        })
    }

    /// Like [`test_state`] but with a console bearer token configured, so the
    /// auth middleware is active on protected routes.
    fn test_state_with_token(token: &str) -> Arc<AppState> {
        let registry = Arc::new(crate::metrics::build_registry([
            ("instance".into(), "test".into()),
            ("pipeline".into(), "test".into()),
        ]));
        let engine_metrics = Arc::new(laminar_db::EngineMetrics::new(&registry));
        let db = Arc::new(LaminarDB::open().unwrap());
        db.set_engine_metrics(engine_metrics);
        let server_metrics = crate::metrics::ServerMetrics::new(&registry);
        let server = crate::config::ServerSection {
            console_token: Some(crate::config::Secret::new(token)),
            ..Default::default()
        };
        Arc::new(AppState {
            db,
            config_path: PathBuf::from("test.toml"),
            current_config: parking_lot::RwLock::new(crate::config::ServerConfig {
                server,
                state: laminar_core::state::StateBackendConfig::default(),
                checkpoint: crate::config::CheckpointSection::default(),
                sources: vec![],
                lookups: vec![],
                pipelines: vec![],
                sinks: vec![],
                discovery: None,
                coordination: None,
                node_id: None,
                sql: None,
                ai: Default::default(),
                models: Default::default(),
            }),
            reload_guard: ReloadGuard::new(),
            registry,
            server_metrics,
            ephemeral: Arc::new(EphemeralTracker::new()),
            #[cfg(feature = "cluster")]
            cluster: None,
        })
    }

    #[tokio::test]
    async fn test_auth_required_without_token_returns_401() {
        let state = test_state_with_token("supersecret-token");
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/sources")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_auth_with_valid_bearer_returns_200() {
        let state = test_state_with_token("supersecret-token");
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/sources")
            .header("authorization", "Bearer supersecret-token")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_auth_with_wrong_bearer_returns_401() {
        let state = test_state_with_token("supersecret-token");
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/sources")
            .header("authorization", "Bearer not-the-token")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_auth_with_query_token_returns_200() {
        // WebSocket clients can't set the Authorization header, so the token is
        // accepted from the query string — but only on `/ws/` routes. A plain
        // (non-upgrade) GET to a WS route passes auth and is then rejected by
        // the WebSocket upgrade extractor, so the meaningful assertion is that
        // auth did not reject it with 401.
        let state = test_state_with_token("supersecret-token");
        let app = build_router(state);

        let req = Request::builder()
            .uri("/ws/events?token=supersecret-token")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_auth_query_token_on_http_returns_401() {
        // The `?token=` query parameter is honored only on WS upgrade routes.
        // On a normal HTTP control-plane route it is ignored, so a request
        // without a bearer header is unauthorized.
        let state = test_state_with_token("supersecret-token");
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/sources?token=supersecret-token")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_public_health_bypasses_auth() {
        // /health is public even when a console token is configured.
        let state = test_state_with_token("supersecret-token");
        let app = build_router(state);

        let req = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_check() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "healthy");
        assert!(json["version"].is_string());
    }

    #[tokio::test]
    async fn test_readiness_not_running() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/ready")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        // Pipeline is in Created state, not Running
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_metrics() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            ct.contains("text/plain"),
            "expected text/plain content-type, got: {ct}"
        );

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            text.contains("laminardb_events_ingested_total"),
            "missing events_ingested_total"
        );
        assert!(
            text.contains("laminardb_cycles_total"),
            "missing cycles_total"
        );
        assert!(
            text.contains("laminardb_checkpoints_completed_total"),
            "missing checkpoints_completed_total"
        );
        // Prometheus text format includes HELP and TYPE annotations.
        assert!(text.contains("# HELP"), "missing # HELP annotation");
        assert!(text.contains("# TYPE"), "missing # TYPE annotation");
    }

    #[tokio::test]
    async fn test_list_sources_empty() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/sources")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_get_stream_not_found() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/streams/nonexistent")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_execute_sql_create_source() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/sql")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({
                    "sql": "CREATE SOURCE test_src (id BIGINT, name VARCHAR)"
                }))
                .unwrap(),
            ))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["result_type"], "CREATE SOURCE");
    }

    #[tokio::test]
    async fn test_execute_sql_metadata_returns_rows() {
        let state = test_state();
        let app = build_router(state);

        // Create a source so SHOW SOURCES has a row to return.
        let create = Request::builder()
            .method("POST")
            .uri("/api/v1/sql")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({
                    "sql": "CREATE SOURCE meta_src (id BIGINT, name VARCHAR)"
                }))
                .unwrap(),
            ))
            .unwrap();
        let resp = app.clone().oneshot(create).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // SHOW SOURCES yields an ExecuteResult::Metadata batch — the handler
        // must serialize it into the `data` field.
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/sql")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({ "sql": "SHOW SOURCES" })).unwrap(),
            ))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["result_type"], "metadata");
        let data = json["data"]
            .as_array()
            .expect("data should be a JSON array");
        assert_eq!(data.len(), 1, "expected the one created source");
        assert_eq!(data[0]["source_name"], "meta_src");
    }

    #[tokio::test]
    async fn test_execute_sql_invalid() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/sql")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({
                    "sql": "NOT VALID SQL AT ALL BLAH"
                }))
                .unwrap(),
            ))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_reload_invalid_config_path() {
        // test_state has config_path = "test.toml" which doesn't exist → 400
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/reload")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_reload_concurrent_returns_conflict() {
        let state = test_state();
        // Hold the guard before making the request
        let _guard = state.reload_guard.try_acquire().unwrap();

        let app = build_router(state);
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/reload")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_reload_with_valid_config() {
        use std::io::Write;

        // Create a real temp config file
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmpfile, "[server]").unwrap();
        let path = tmpfile.path().to_path_buf();

        let registry = Arc::new(crate::metrics::build_registry([
            ("instance".into(), "test".into()),
            ("pipeline".into(), "test".into()),
        ]));
        let db = Arc::new(LaminarDB::open().unwrap());
        let engine_metrics = Arc::new(laminar_db::EngineMetrics::new(&registry));
        db.set_engine_metrics(engine_metrics);
        let server_metrics = crate::metrics::ServerMetrics::new(&registry);
        let state = Arc::new(AppState {
            db,
            config_path: path,
            current_config: parking_lot::RwLock::new(crate::config::ServerConfig {
                server: crate::config::ServerSection::default(),
                state: laminar_core::state::StateBackendConfig::default(),
                checkpoint: crate::config::CheckpointSection::default(),
                sources: vec![],
                lookups: vec![],
                pipelines: vec![],
                sinks: vec![],
                discovery: None,
                coordination: None,
                node_id: None,
                sql: None,
                ai: Default::default(),
                models: Default::default(),
            }),
            reload_guard: ReloadGuard::new(),

            registry,
            server_metrics,
            ephemeral: Arc::new(EphemeralTracker::new()),
            #[cfg(feature = "cluster")]
            cluster: None,
        });

        let app = build_router(state.clone());
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/reload")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["success"], true);
    }

    /// POST a SQL statement to `/api/v1/sql`, asserting it succeeds.
    async fn exec_sql(app: &Router, sql: &str) {
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/sql")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({ "sql": sql })).unwrap(),
            ))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "exec failed: {sql}");
    }

    #[tokio::test]
    async fn test_list_mvs_empty() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/mvs")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_list_mvs_after_create() {
        let state = test_state();
        let app = build_router(state);

        exec_sql(&app, "CREATE SOURCE events (id INT, value DOUBLE)").await;
        // Registers the MV in the registry (see ddl.rs); query execution is not
        // required for it to be listed.
        exec_sql(
            &app,
            "CREATE MATERIALIZED VIEW event_stats AS SELECT * FROM events",
        )
        .await;

        let req = Request::builder()
            .uri("/api/v1/mvs")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let mvs = json.as_array().expect("mvs should be an array");
        let found = mvs
            .iter()
            .find(|m| m["name"] == "event_stats")
            .expect("event_stats should be listed");
        assert_eq!(found["state"], "Running");
        assert!(
            found["sql"].as_str().unwrap().contains("event_stats"),
            "sql should be the full CREATE statement: {found:?}"
        );
    }

    #[tokio::test]
    async fn test_list_connectors() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/connectors")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // Shape is `{sources: [...], sinks: [...]}`; the exact connectors depend
        // on enabled features, so only assert the structure here.
        assert!(json["sources"].is_array(), "sources should be an array");
        assert!(json["sinks"].is_array(), "sinks should be an array");
    }

    #[tokio::test]
    async fn test_create_query_returns_stream_id_and_ws_url() {
        let state = test_state();
        let app = build_router(state.clone());

        exec_sql(&app, "CREATE SOURCE events (id INT, value DOUBLE)").await;

        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/queries")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({ "sql": "SELECT * FROM events" }))
                    .unwrap(),
            ))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let stream_id = json["stream_id"].as_str().expect("stream_id present");
        assert!(
            stream_id.starts_with("__console_"),
            "unexpected stream_id: {stream_id}"
        );
        assert_eq!(json["ws_url"], format!("/ws/{stream_id}"));

        // The ephemeral stream is registered and tracked as pending.
        assert!(
            state.db.streams().iter().any(|s| s.name == stream_id),
            "ephemeral stream should be registered"
        );
        assert!(state.ephemeral.is_tracked(stream_id));
    }

    #[tokio::test]
    async fn test_create_query_invalid_sql_returns_400() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/queries")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({ "sql": "NOT VALID SQL BLAH" })).unwrap(),
            ))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// Unit test of the ephemeral-stream lifecycle the WS handler and reaper
    /// task drive: pending → connected, removal, and pending-only reaping.
    #[test]
    fn test_ephemeral_tracker_lifecycle() {
        let tracker = EphemeralTracker::new();

        // Unknown stream: not tracked, can't be marked connected or reaped.
        assert!(!tracker.is_tracked("__console_x"));
        assert!(!tracker.mark_connected("__console_x"));
        assert!(!tracker.remove_if_pending("__console_x"));

        // A pending stream is tracked and reaped by `remove_if_pending`.
        tracker.add_pending("__console_x".to_string());
        assert!(tracker.is_tracked("__console_x"));
        assert!(
            tracker.remove_if_pending("__console_x"),
            "a pending stream is reaped"
        );
        assert!(
            !tracker.is_tracked("__console_x"),
            "reaping stops tracking the stream"
        );

        // Once connected, the reaper must never drop it.
        tracker.add_pending("__console_x".to_string());
        assert!(tracker.mark_connected("__console_x"));
        assert!(
            !tracker.remove_if_pending("__console_x"),
            "a connected stream is never reaped"
        );
        assert!(
            tracker.is_tracked("__console_x"),
            "a connected stream stays tracked"
        );

        // Removal stops tracking.
        assert!(tracker.remove("__console_x"));
        assert!(!tracker.is_tracked("__console_x"));
        assert!(!tracker.remove("__console_x"));
    }

    /// Bind a real ephemeral-port server so the WebSocket upgrade runs over a
    /// genuine hyper connection (the `tower::oneshot` harness can't upgrade —
    /// the request has no `OnUpgrade` extension, so axum rejects with 426).
    async fn spawn_test_server(state: Arc<AppState>) -> std::net::SocketAddr {
        let router = build_router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        addr
    }

    /// Send a raw WebSocket upgrade request for `path` and return the first
    /// chunk of the HTTP response (enough to read the status line).
    async fn ws_handshake(addr: std::net::SocketAddr, path: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "GET {path} HTTP/1.1\r\n\
             Host: localhost\r\n\
             Connection: Upgrade\r\n\
             Upgrade: websocket\r\n\
             Sec-WebSocket-Version: 13\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             \r\n"
        );
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).into_owned()
    }

    #[tokio::test]
    async fn test_ws_upgrade_switching_protocols() {
        let state = test_state();
        let app = build_router(state.clone());
        exec_sql(&app, "CREATE SOURCE events (id INT, value DOUBLE)").await;

        // Create an ephemeral console stream to connect to.
        let create = Request::builder()
            .method("POST")
            .uri("/api/v1/queries")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({ "sql": "SELECT * FROM events" }))
                    .unwrap(),
            ))
            .unwrap();
        let resp = app.oneshot(create).await.unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let stream_id = json["stream_id"].as_str().unwrap().to_string();

        let addr = spawn_test_server(state).await;
        let resp = ws_handshake(addr, &format!("/ws/{stream_id}")).await;
        assert!(
            resp.starts_with("HTTP/1.1 101"),
            "expected 101 Switching Protocols, got: {resp}"
        );
    }

    #[tokio::test]
    async fn test_ws_upgrade_unknown_stream_returns_404() {
        // Over a real connection the upgrade extractor succeeds, so the
        // handler's stream-existence check runs and returns 404.
        let state = test_state();
        let addr = spawn_test_server(state).await;
        let resp = ws_handshake(addr, "/ws/does_not_exist").await;
        assert!(
            resp.starts_with("HTTP/1.1 404"),
            "expected 404 Not Found for unknown stream, got: {resp}"
        );
    }

    #[tokio::test]
    async fn test_get_graph_returns_nodes_and_edges() {
        let state = test_state();
        let app = build_router(state);

        exec_sql(&app, "CREATE SOURCE events (id INT, value DOUBLE)").await;
        exec_sql(&app, "CREATE STREAM s1 AS SELECT * FROM events").await;

        let req = Request::builder()
            .uri("/api/v1/graph")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let nodes = json["nodes"].as_array().expect("nodes should be an array");
        let edges = json["edges"].as_array().expect("edges should be an array");

        let source = nodes
            .iter()
            .find(|n| n["name"] == "events")
            .expect("events source node should be present");
        assert_eq!(source["node_type"], "Source");

        let stream = nodes
            .iter()
            .find(|n| n["name"] == "s1")
            .expect("s1 stream node should be present");
        assert_eq!(stream["node_type"], "Stream");
        assert!(
            stream["sql"].as_str().unwrap().contains("events"),
            "stream node should carry its defining SQL: {stream:?}"
        );

        assert!(
            edges
                .iter()
                .any(|e| e["from"] == "events" && e["to"] == "s1"),
            "expected an edge events -> s1, got: {edges:?}"
        );
    }

    #[tokio::test]
    async fn test_get_graph_empty() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/graph")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["nodes"].as_array().unwrap().is_empty());
        assert!(json["edges"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_cluster_nodes_404_when_not_cluster() {
        // test_state() leaves `cluster` as None, so the cluster endpoints 404
        // even when compiled with the `cluster` feature.
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/cluster/nodes")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_cluster_vnodes_404_when_not_cluster() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/cluster/vnodes")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_cluster_leader_404_when_not_cluster() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/cluster/leader")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_cluster_checkpoints_returns_metadata() {
        // Available in both single-node and cluster mode. With no checkpoint
        // taken yet it still returns a single metadata row of zeros.
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/cluster/checkpoints")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let rows = json
            .as_array()
            .expect("checkpoint status should be an array");
        assert_eq!(rows.len(), 1, "expected one checkpoint-status row");
        let row = &rows[0];
        assert!(
            row.get("checkpoint_id").is_some(),
            "row should carry checkpoint_id: {row:?}"
        );
        assert!(
            row.get("total_checkpoints").is_some(),
            "row should carry total_checkpoints: {row:?}"
        );
    }
}
