//! HTTP API for LaminarDB server.

use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

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

use crate::config::{ServerConfig, ServerMode};
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
    pub(crate) ws_slots: Arc<tokio::sync::Semaphore>,
    /// Cluster control-plane handles (cluster mode only). `None` in
    /// single-node mode; the cluster endpoints 404 when absent.
    #[cfg(feature = "cluster")]
    pub cluster: Option<ClusterComponents>,
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

pub async fn bind_listener(bind: &str) -> Result<tokio::net::TcpListener, ServerError> {
    tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|e| ServerError::Http(format!("failed to bind to {bind}: {e}")))
}

pub fn serve_listener(
    router: Router,
    listener: tokio::net::TcpListener,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            tracing::error!("HTTP server error: {e}");
        }
    })
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

async fn ws_send(socket: &mut WebSocket, message: Message) -> bool {
    matches!(
        tokio::time::timeout(WS_WRITE_TIMEOUT, socket.send(message)).await,
        Ok(Ok(()))
    )
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

    let st = Arc::clone(&state);
    ws.max_message_size(MAX_WS_INBOUND_BYTES)
        .max_frame_size(MAX_WS_INBOUND_BYTES)
        .on_upgrade(move |socket| async move {
            let _slot = slot;
            st.server_metrics.ws_connections.inc();
            ws_client(socket, portal, name).await;
            st.server_metrics.ws_connections.dec();
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
    let mut pong_deadline = WsPongDeadline::default();
    let mut seq: u64 = 0;

    'subscription: loop {
        tokio::select! {
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
                                    let _ = ws_send(&mut socket, Message::Text(out.into())).await;
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
                                    let _ = ws_send(&mut socket, Message::Text(out.into())).await;
                                    break 'subscription;
                                }
                            };
                            if !ws_send(&mut socket, Message::Text(out.into())).await {
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
                        if !ws_send(&mut socket, Message::Text(out.into())).await {
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
                        let _ = ws_send(&mut socket, Message::Text(out.into())).await;
                        break;
                    }
                    Some(laminar_db::subscription::PortalFrame::Error { message }) => {
                        warn!(stream = %name, error = %message, "WS subscription failed, disconnecting");
                        let out = ws_error_json(&name, "subscription_failed", &message, seq);
                        let _ = ws_send(&mut socket, Message::Text(out.into())).await;
                        break;
                    }
                    None => break, // disconnected
                }
            }
            _ = heartbeat.tick() => {
                if !pong_deadline.before_ping() {
                    break;
                }
                if !ws_send(&mut socket, Message::Ping(bytes::Bytes::new())).await {
                    break;
                }
            }
            msg = socket.recv() => {
                // `data` moves into `Pong`, so the inner `if` can't fold into the guard.
                #[allow(clippy::collapsible_match)]
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(data))) => {
                        if !ws_send(&mut socket, Message::Pong(data)).await { break; }
                    }
                    Some(Ok(Message::Pong(_))) => pong_deadline.on_pong(),
                    Some(Ok(Message::Text(_) | Message::Binary(_))) => {
                        let out = ws_error_json(
                            &name,
                            "unsupported_client_message",
                            "subscription WebSocket accepts control frames only",
                            seq,
                        );
                        let _ = ws_send(&mut socket, Message::Text(out.into())).await;
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
    let _ = ws_send(&mut socket, Message::Close(None)).await;
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

    fn test_state_with_db(db: Arc<LaminarDB>) -> Arc<AppState> {
        let registry = Arc::new(crate::metrics::build_registry([
            ("instance".into(), "test".into()),
            ("pipeline".into(), "test".into()),
        ]));
        let engine_metrics = Arc::new(laminar_db::EngineMetrics::new(&registry));
        db.set_engine_metrics(engine_metrics);
        let server_metrics = crate::metrics::ServerMetrics::new(&registry);
        Arc::new(AppState {
            db,
            config_path: PathBuf::from("test.toml"),
            current_config: parking_lot::RwLock::new(crate::config::ServerConfig {
                server: crate::config::ServerSection::default(),
                state: laminar_core::state::StateBackendConfig::default(),
                checkpoint: crate::config::CheckpointSection::default(),
                supervision: Default::default(),
                sources: vec![],
                lookups: vec![],
                pipelines: vec![],
                sinks: vec![],
                discovery: None,
                node_id: None,
                sql: None,
                ai: Default::default(),
                models: Default::default(),
            }),
            reload_guard: ReloadGuard::new(),

            registry,
            server_metrics,
            ws_slots: ws_connection_slots(),
            #[cfg(feature = "cluster")]
            cluster: None,
        })
    }

    fn test_state() -> Arc<AppState> {
        test_state_with_db(LaminarDB::open().unwrap())
    }

    /// Like [`test_state`] but with a console bearer token configured, so the
    /// auth middleware is active on protected routes.
    fn test_state_with_token(token: &str) -> Arc<AppState> {
        let registry = Arc::new(crate::metrics::build_registry([
            ("instance".into(), "test".into()),
            ("pipeline".into(), "test".into()),
        ]));
        let engine_metrics = Arc::new(laminar_db::EngineMetrics::new(&registry));
        let db = LaminarDB::open().unwrap();
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
                supervision: Default::default(),
                sources: vec![],
                lookups: vec![],
                pipelines: vec![],
                sinks: vec![],
                discovery: None,
                node_id: None,
                sql: None,
                ai: Default::default(),
                models: Default::default(),
            }),
            reload_guard: ReloadGuard::new(),
            registry,
            server_metrics,
            ws_slots: ws_connection_slots(),
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
        let db = LaminarDB::open().unwrap();
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
                supervision: Default::default(),
                sources: vec![],
                lookups: vec![],
                pipelines: vec![],
                sinks: vec![],
                discovery: None,
                node_id: None,
                sql: None,
                ai: Default::default(),
                models: Default::default(),
            }),
            reload_guard: ReloadGuard::new(),

            registry,
            server_metrics,
            ws_slots: ws_connection_slots(),
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

    #[test]
    fn test_ws_terminal_frames_expose_error_and_gap_details() {
        let error: serde_json::Value = serde_json::from_str(&ws_error_json(
            "orders",
            "subscription_failed",
            "bad filter",
            7,
        ))
        .unwrap();
        assert_eq!(error["type"], "error");
        assert_eq!(error["subscription_id"], "orders");
        assert_eq!(error["code"], "subscription_failed");
        assert_eq!(error["message"], "bad filter");
        assert_eq!(error["sequence"], "7");

        let gap: serde_json::Value = serde_json::from_str(&ws_gap_json("orders", 12, 8)).unwrap();
        assert_eq!(gap["type"], "gap");
        assert_eq!(gap["code"], "subscription_lagged");
        assert_eq!(gap["skipped_messages"], "12");
        assert_eq!(gap["sequence"], "8");

        let progress: serde_json::Value =
            serde_json::from_str(&ws_progress_json("orders", 9, 42, 8, 6, 10)).unwrap();
        assert_eq!(progress["type"], "progress");
        assert_eq!(progress["epoch"], "9");
        assert_eq!(progress["checkpoint_id"], "42");
        assert_eq!(progress["log_sequence"], "8");
        assert_eq!(progress["through_log_sequence"], "6");
        assert_eq!(progress["sequence"], "10");
    }

    #[test]
    fn ws_terminal_frames_bound_untrusted_text() {
        let text = "\u{10ffff}".repeat(MAX_WS_CONTROL_FIELD_BYTES + 1);
        let frame = ws_error_json(&text, &text, &text, 1);
        assert!(frame.len() <= MAX_WS_FRAME_BYTES);
        let parsed: serde_json::Value = serde_json::from_str(&frame).unwrap();
        assert!(parsed["message"].as_str().unwrap().len() <= MAX_WS_CONTROL_FIELD_BYTES);
    }

    #[test]
    fn ws_data_frames_split_before_the_wire_limit() {
        use arrow_array::{Int32Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        let value = "x".repeat(MAX_WS_FRAME_BYTES / 2);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2])),
                Arc::new(StringArray::from(vec![
                    value.as_str(),
                    value.as_str(),
                    value.as_str(),
                ])),
            ],
        )
        .unwrap();

        let mut state = WsBatchFrameState::default();
        let mut sequence = 0;
        let mut ids = Vec::new();
        while state.offset < batch.num_rows() {
            let expected_offset = state.offset;
            let frame = next_ws_data_frame("large", &batch, &mut state, sequence, u64::MAX)
                .unwrap()
                .unwrap();
            let consumed = state.offset - expected_offset;
            assert!(frame.len() <= MAX_WS_FRAME_BYTES);
            let json: serde_json::Value = serde_json::from_str(&frame).unwrap();
            assert_eq!(json["sequence"], sequence.to_string());
            assert_eq!(json["log_sequence"], u64::MAX.to_string());
            assert_eq!(json["row_offset"], expected_offset.to_string());
            assert_eq!(json["row_count"], consumed.to_string());
            assert_eq!(json["data"].as_array().unwrap().len(), consumed);
            ids.extend(
                json["data"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| row["id"].as_i64().unwrap()),
            );
            sequence += 1;
        }
        assert!(sequence > 1, "oversized batches must be split");
        assert_eq!(state.offset, batch.num_rows());
        assert_eq!(ids, vec![0, 1, 2], "rows must not be duplicated or skipped");
        assert!(state.pending_row.is_none());
        assert_eq!(
            next_ws_data_frame("large", &batch, &mut state, sequence, 99).unwrap(),
            None
        );
    }

    #[test]
    fn ws_data_frame_rejects_a_single_oversized_row() {
        use arrow_array::{RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        let value = "x".repeat(MAX_WS_FRAME_BYTES);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec![value.as_str()]))],
        )
        .unwrap();
        let mut state = WsBatchFrameState::default();
        assert_eq!(
            next_ws_data_frame("large", &batch, &mut state, 0, 0),
            Err(WsFrameBuildError::TooLarge)
        );
        assert_eq!(state.offset, 0);
    }

    #[test]
    fn http_and_ws_json_preserve_exact_nested_values_and_nulls() {
        use arrow_array::builder::{Float64Builder, ListBuilder};
        use arrow_array::types::Int8Type;
        use arrow_array::{
            Array, Decimal128Array, DictionaryArray, Float32Array, Int64Array, Int8Array,
            RecordBatch, StructArray, UInt64Array,
        };
        use arrow_schema::{DataType, Field, Fields, Schema};

        let nested_decimal = Decimal128Array::from(vec![Some(12_345_i128), None])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let nested_fields = Fields::from(vec![
            Arc::new(Field::new("large", DataType::Int64, true)),
            Arc::new(Field::new(
                "amount",
                nested_decimal.data_type().clone(),
                true,
            )),
        ]);
        let nested = StructArray::try_new(
            nested_fields,
            vec![
                Arc::new(Int64Array::from(vec![Some(i64::MIN), None])),
                Arc::new(nested_decimal),
            ],
            None,
        )
        .unwrap();

        let dictionary = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), None]),
            Arc::new(UInt64Array::from(vec![u64::MAX])),
        )
        .unwrap();

        let mut floats = ListBuilder::new(Float64Builder::new());
        floats.values().append_value(1.25);
        floats.values().append_value(f64::NAN);
        floats.values().append_value(f64::INFINITY);
        floats.values().append_value(f64::NEG_INFINITY);
        floats.values().append_null();
        floats.append(true);
        floats.append_null();
        let floats = floats.finish();

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("unsigned", DataType::UInt64, false),
                Field::new("nested", nested.data_type().clone(), false),
                Field::new("dictionary", dictionary.data_type().clone(), true),
                Field::new("floats", floats.data_type().clone(), true),
                Field::new("float32", DataType::Float32, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(vec![u64::MAX, 0])),
                Arc::new(nested),
                Arc::new(dictionary),
                Arc::new(floats),
                Arc::new(Float32Array::from(vec![1.234_567_f32, f32::INFINITY])),
            ],
        )
        .unwrap();

        let rows: serde_json::Value =
            serde_json::from_str(&batches_to_json_string(std::slice::from_ref(&batch)).unwrap())
                .unwrap();
        let first = &rows[0];
        assert_eq!(first["unsigned"], u64::MAX.to_string());
        assert_eq!(first["nested"]["large"], i64::MIN.to_string());
        assert_eq!(first["nested"]["amount"], "123.45");
        assert_eq!(first["dictionary"], u64::MAX.to_string());
        assert!(first["floats"][0].is_number());
        assert_eq!(first["floats"][0].as_f64(), Some(1.25));
        assert_eq!(first["floats"][1], "NaN");
        assert_eq!(first["floats"][2], "Infinity");
        assert_eq!(first["floats"][3], "-Infinity");
        assert!(first["floats"][4].is_null());
        assert!(first["float32"].is_number());

        let second = &rows[1];
        assert!(second["nested"]["large"].is_null());
        assert!(second["nested"]["amount"].is_null());
        assert!(second["dictionary"].is_null());
        assert!(second["floats"].is_null());
        assert_eq!(second["float32"], "Infinity");

        let mut state = WsBatchFrameState::default();
        let frame = next_ws_data_frame("exact", &batch, &mut state, 0, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(state.offset, 2);
        let frame: serde_json::Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(frame["data"], rows);
        assert_eq!(frame["log_sequence"], u64::MAX.to_string());
    }

    #[test]
    fn ws_slot_admission_is_atomic() {
        const CAPACITY: usize = 4;
        const CONTENDERS: usize = 32;

        let slots = Arc::new(tokio::sync::Semaphore::new(CAPACITY));
        let start = Arc::new(std::sync::Barrier::new(CONTENDERS + 1));
        let release = Arc::new(std::sync::Barrier::new(CONTENDERS + 1));
        let (tx, rx) = std::sync::mpsc::channel();
        let mut threads = Vec::new();
        for _ in 0..CONTENDERS {
            let slots = Arc::clone(&slots);
            let start = Arc::clone(&start);
            let release = Arc::clone(&release);
            let tx = tx.clone();
            threads.push(std::thread::spawn(move || {
                start.wait();
                let permit = try_acquire_ws_slot(&slots);
                tx.send(permit.is_some()).unwrap();
                release.wait();
                drop(permit);
            }));
        }
        drop(tx);
        start.wait();
        let admitted = (0..CONTENDERS)
            .map(|_| rx.recv().unwrap())
            .filter(|admitted| *admitted)
            .count();
        assert_eq!(admitted, CAPACITY);
        release.wait();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(slots.available_permits(), CAPACITY);
    }

    #[test]
    fn ws_liveness_expires_without_pongs_and_recovers_on_pong() {
        let mut deadline = WsPongDeadline::default();
        assert!(deadline.before_ping());
        assert!(deadline.before_ping());
        assert!(!deadline.before_ping());
        deadline.on_pong();
        assert!(deadline.before_ping());
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
        state
            .db
            .execute("CREATE SOURCE events (id INT, value DOUBLE)")
            .await
            .unwrap();
        state
            .db
            .execute("CREATE STREAM visible AS SELECT * FROM events")
            .await
            .unwrap();
        state.db.start().await.unwrap();

        let addr = spawn_test_server(state).await;
        let resp = ws_handshake(addr, "/ws/visible").await;
        assert!(
            resp.starts_with("HTTP/1.1 101"),
            "expected 101 Switching Protocols, got: {resp}"
        );
    }

    #[tokio::test]
    async fn test_ws_upgrade_unknown_stream_returns_404() {
        let state = test_state();
        let addr = spawn_test_server(state).await;
        let resp = ws_handshake(addr, "/ws/does_not_exist").await;
        assert!(
            resp.starts_with("HTTP/1.1 404"),
            "expected 404 Not Found for unknown stream, got: {resp}"
        );
    }

    #[tokio::test]
    async fn ws_emits_committed_checkpoint_progress() {
        let checkpoint_dir = tempfile::tempdir().unwrap();
        let db = LaminarDB::open_with_config(laminar_db::LaminarConfig {
            checkpoint: Some(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: None,
                data_dir: Some(checkpoint_dir.path().to_path_buf()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
        let state = test_state_with_db(db);
        state
            .db
            .execute("CREATE SOURCE events (id BIGINT)")
            .await
            .unwrap();
        state
            .db
            .execute("CREATE MATERIALIZED VIEW visible AS SELECT id FROM events")
            .await
            .unwrap();
        state.db.start().await.unwrap();
        let addr = spawn_test_server(Arc::clone(&state)).await;

        let (attached_tx, attached_rx) = tokio::sync::oneshot::channel();
        let (data_tx, data_rx) = tokio::sync::oneshot::channel();
        let reader = tokio::task::spawn_blocking(move || {
            let (mut socket, _) =
                tungstenite::connect(format!("ws://{addr}/ws/visible")).expect("WS connect");
            let _ = attached_tx.send(());
            let mut data_tx = Some(data_tx);
            let mut frames = Vec::new();
            loop {
                match socket.read().expect("WS frame") {
                    tungstenite::Message::Text(text) => {
                        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
                        frames.push(json.clone());
                        if json["type"] == "data" {
                            if let Some(data_tx) = data_tx.take() {
                                let _ = data_tx.send(());
                            }
                        }
                        if json["type"] == "progress" {
                            return frames;
                        }
                    }
                    tungstenite::Message::Ping(data) => {
                        socket.send(tungstenite::Message::Pong(data)).expect("pong");
                    }
                    tungstenite::Message::Close(_) => panic!("WS closed before progress"),
                    _ => {}
                }
            }
        });
        attached_rx.await.expect("reader attached");
        let source = state.db.source_untyped("events").unwrap();
        source
            .push_arrow(
                arrow_array::RecordBatch::try_new(
                    source.schema().clone(),
                    vec![Arc::new(arrow_array::Int64Array::from(vec![7]))],
                )
                .unwrap(),
            )
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), data_rx)
            .await
            .expect("the input must reach the WebSocket before checkpointing")
            .expect("WebSocket reader must remain attached");
        let committed = state.db.checkpoint().await.expect("checkpoint");
        assert!(committed.success);
        let frames = tokio::time::timeout(std::time::Duration::from_secs(5), reader)
            .await
            .expect("progress frame arrives")
            .expect("reader task");
        assert_eq!(frames.len(), 2, "data must precede its progress cut");
        assert_eq!(frames[0]["type"], "data");
        assert_eq!(frames[0]["sequence"], "0");
        assert_eq!(frames[0]["log_sequence"], "0");
        let progress = &frames[1];
        assert_eq!(progress["epoch"], committed.epoch.to_string());
        assert_eq!(
            progress["checkpoint_id"],
            committed.checkpoint_id.to_string()
        );
        assert_eq!(progress["log_sequence"], "1");
        assert_eq!(progress["through_log_sequence"], "1");
        assert_eq!(progress["sequence"], "1");
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
