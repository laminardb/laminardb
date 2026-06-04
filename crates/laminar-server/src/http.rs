//! HTTP API for LaminarDB server.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use prometheus::Registry;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};
use tracing::{info, warn};

use laminar_db::{ConnectorInfo, LaminarDB};

use crate::config::ServerConfig;
use crate::metrics::ServerMetrics;
use crate::reload::{self, ReloadGuard};
use crate::server::ServerError;

pub struct AppState {
    pub db: Arc<LaminarDB>,
    pub config_path: PathBuf,
    /// `parking_lot::RwLock` is correct here — every reader drops the
    /// guard before awaiting any I/O (see `reload_config`/`cluster_status`/
    /// watcher.rs). No writer holds the lock across `.await` either.
    pub current_config: parking_lot::RwLock<ServerConfig>,
    pub reload_guard: ReloadGuard,
    pub registry: Arc<Registry>,
    pub server_metrics: ServerMetrics,
    /// Tracks ephemeral streams created by the console (`POST /api/v1/queries`)
    /// so abandoned ones can be reaped.
    pub ephemeral: Arc<EphemeralTracker>,
}

// ---------------------------------------------------------------------------
// Ephemeral console streams
// ---------------------------------------------------------------------------

/// How long an ephemeral console stream may sit `Pending` (created but never
/// connected to over WebSocket) before the GC task drops it.
const EPHEMERAL_PENDING_TTL: Duration = Duration::from_secs(30);

/// How often the background GC task sweeps for abandoned ephemeral streams.
const EPHEMERAL_GC_INTERVAL: Duration = Duration::from_secs(10);

/// Lifecycle state of a console-managed ephemeral stream.
#[derive(Clone, Copy, PartialEq, Eq)]
enum EphemeralState {
    /// Created by `POST /api/v1/queries`, no WebSocket attached yet.
    Pending,
    /// A WebSocket client is (or was) attached.
    Connected,
}

struct EphemeralEntry {
    state: EphemeralState,
    created_at: Instant,
}

/// Tracks the lifecycle of console-initiated ephemeral streams.
///
/// A stream is registered `Pending` when `POST /api/v1/queries` creates it,
/// transitions to `Connected` when its WebSocket is opened, and is removed
/// when the WebSocket closes. The background GC task ([`spawn_ephemeral_gc`])
/// drops any stream still `Pending` after [`EPHEMERAL_PENDING_TTL`], so a
/// client that requests a query but never connects can't leak a stream.
#[derive(Default)]
pub struct EphemeralTracker {
    inner: parking_lot::Mutex<HashMap<String, EphemeralEntry>>,
}

impl EphemeralTracker {
    /// Creates an empty tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a newly created ephemeral stream as `Pending`.
    fn add_pending(&self, name: String) {
        self.inner.lock().insert(
            name,
            EphemeralEntry {
                state: EphemeralState::Pending,
                created_at: Instant::now(),
            },
        );
    }

    /// Transitions a pending stream to `Connected`. Returns `true` if the
    /// stream was tracked (i.e. it is a console ephemeral stream).
    fn mark_connected(&self, name: &str) -> bool {
        let mut guard = self.inner.lock();
        if let Some(entry) = guard.get_mut(name) {
            entry.state = EphemeralState::Connected;
            true
        } else {
            false
        }
    }

    /// Stops tracking `name`. Returns `true` if it was tracked.
    fn remove(&self, name: &str) -> bool {
        self.inner.lock().remove(name).is_some()
    }

    /// Returns `true` if `name` is currently tracked (test-only introspection).
    #[cfg(test)]
    fn is_tracked(&self, name: &str) -> bool {
        self.inner.lock().contains_key(name)
    }

    /// Returns the names of streams still `Pending` past `ttl`.
    fn expired_pending(&self, ttl: Duration) -> Vec<String> {
        let guard = self.inner.lock();
        guard
            .iter()
            .filter(|(_, e)| e.state == EphemeralState::Pending && e.created_at.elapsed() >= ttl)
            .map(|(name, _)| name.clone())
            .collect()
    }
}

/// Spawns the background task that reaps abandoned ephemeral console streams.
///
/// Every [`EPHEMERAL_GC_INTERVAL`] it drops any stream that has been `Pending`
/// (created but never connected to) for longer than [`EPHEMERAL_PENDING_TTL`].
pub fn spawn_ephemeral_gc(state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(EPHEMERAL_GC_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            for name in state.ephemeral.expired_pending(EPHEMERAL_PENDING_TTL) {
                let ddl = format!("DROP STREAM IF EXISTS {name}");
                if let Err(e) = state.db.execute(&ddl).await {
                    warn!(stream = %name, error = %e, "failed to drop abandoned ephemeral stream");
                }
                state.ephemeral.remove(&name);
                info!(stream = %name, "reaped abandoned ephemeral console stream");
            }
        }
    })
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
        .route("/api/v1/cluster", get(cluster_status))
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
        let authorized = bearer_token(req.headers()).is_some_and(|t| ct_eq(t, expected))
            || query_token(req.uri()).is_some_and(|t| ct_eq(&t, expected));
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

/// Extract the `token` query parameter from a request URI, if present.
fn query_token(uri: &axum::http::Uri) -> Option<String> {
    uri.query()?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == "token").then(|| value.to_string())
    })
}

/// Constant-time string comparison so token validation doesn't leak the secret
/// through response timing. Length is compared eagerly (already public-ish).
fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
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
    /// Result rows as a JSON array (one object per row), for `Query` and
    /// `Metadata` results. Capped at [`MAX_SQL_RESULT_ROWS`] for `Query`.
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
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
    let uri = req.uri().clone();
    let start = Instant::now();

    let response = next.run(req).await;

    let duration_ms = start.elapsed().as_millis();
    let status = response.status();
    info!("{method} {uri} -> {status} ({duration_ms}ms)");

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
/// by the GC task if no client connects within [`EPHEMERAL_PENDING_TTL`].
async fn create_query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateQueryRequest>,
) -> impl IntoResponse {
    let name = console_stream_name();
    let ddl = format!("CREATE STREAM {name} AS {}", req.sql);

    match state.db.execute(&ddl).await {
        Ok(_) => {
            state.ephemeral.add_pending(name.clone());
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
                },
                ExecuteResult::RowsAffected(n) => SqlResponse {
                    result_type: "rows_affected".to_string(),
                    object_name: None,
                    rows_affected: Some(n),
                    data: None,
                },
                ExecuteResult::Metadata(batch) => {
                    let data = match batch_to_json(&batch) {
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
                    }
                }
                ExecuteResult::Query(mut handle) => {
                    let mut sub = match handle.subscribe_raw() {
                        Ok(s) => s,
                        Err(e) => {
                            return error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                e.to_string(),
                            )
                            .into_response();
                        }
                    };

                    // Accumulate rows up to the cap or until the deadline. The
                    // accumulator lives outside the timed future so partial
                    // results survive a timeout cancellation.
                    let mut rows: Vec<serde_json::Value> = Vec::new();
                    let collect = async {
                        while rows.len() < MAX_SQL_RESULT_ROWS {
                            match sub.recv_async().await {
                                Ok(batch) => match batch_to_json(&batch) {
                                    Ok(serde_json::Value::Array(batch_rows)) => {
                                        for row in batch_rows {
                                            if rows.len() >= MAX_SQL_RESULT_ROWS {
                                                break;
                                            }
                                            rows.push(row);
                                        }
                                    }
                                    Ok(_) => {}
                                    Err(e) => {
                                        warn!(error = %e, "serialize error for SQL result batch");
                                    }
                                },
                                Err(_) => break, // stream disconnected
                            }
                        }
                    };
                    let _ = tokio::time::timeout(SQL_RESULT_TIMEOUT, collect).await;

                    SqlResponse {
                        result_type: "query".to_string(),
                        object_name: None,
                        rows_affected: None,
                        data: Some(serde_json::Value::Array(rows)),
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
    let sub = match state.db.subscribe_raw(&name) {
        Ok(s) => s,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to subscribe to '{name}'"),
            )
            .into_response();
        }
    };

    // If this is a console-initiated ephemeral stream, transition it to
    // `Connected` so the GC task won't reap it while a client is attached.
    let is_console = state.ephemeral.mark_connected(&name);

    let st = Arc::clone(&state);
    ws.on_upgrade(move |socket| async move {
        st.server_metrics.ws_connections.inc();
        ws_client(socket, sub, name.clone()).await;
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
    mut sub: laminar_core::streaming::Subscription<laminar_db::ArrowRecord>,
    name: String,
) {
    let mut heartbeat = tokio::time::interval(WS_HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut seq: u64 = 0;

    loop {
        tokio::select! {
            biased;
            result = sub.recv_async() => {
                match result {
                    Ok(batch) => {
                        let json = match batch_to_json(&batch) {
                            Ok(j) => j,
                            Err(e) => {
                                warn!(stream = %name, error = %e, "serialize error");
                                continue;
                            }
                        };
                        let out = serde_json::json!({
                            "type": "data",
                            "subscription_id": &name,
                            "data": json,
                            "sequence": seq,
                        });
                        seq += 1;
                        if socket.send(Message::Text(out.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break, // disconnected
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

fn batch_to_json(batch: &arrow_array::RecordBatch) -> Result<serde_json::Value, String> {
    let mut buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(&mut buf);
    writer.write(batch).map_err(|e| e.to_string())?;
    writer.finish().map_err(|e| e.to_string())?;
    Ok(serde_json::from_slice(&buf).unwrap_or(serde_json::Value::Array(vec![])))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

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
        // WebSocket clients can't set the Authorization header, so the token
        // is accepted from the query string as well.
        let state = test_state_with_token("supersecret-token");
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/sources?token=supersecret-token")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
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
    async fn test_list_sinks_empty() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/sinks")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_list_streams_empty() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/streams")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
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

    #[tokio::test]
    async fn test_metrics_contains_help_and_type() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        // Prometheus text format includes HELP and TYPE annotations
        assert!(text.contains("# HELP"), "missing # HELP annotation");
        assert!(text.contains("# TYPE"), "missing # TYPE annotation");
    }

    // ── Console control-plane endpoints (Phase 1) ──

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

    /// Unit test of the ephemeral-stream lifecycle the WS handler and GC task
    /// drive: pending → connected, removal, and pending-TTL expiry.
    #[test]
    fn test_ephemeral_tracker_lifecycle() {
        let tracker = EphemeralTracker::new();

        // Unknown stream: not tracked, can't be marked connected.
        assert!(!tracker.is_tracked("__console_x"));
        assert!(!tracker.mark_connected("__console_x"));

        // Pending stream is tracked and reported as expired once past the TTL.
        tracker.add_pending("__console_x".to_string());
        assert!(tracker.is_tracked("__console_x"));
        assert_eq!(
            tracker.expired_pending(Duration::ZERO),
            vec!["__console_x".to_string()],
            "a pending stream older than a zero TTL is expired"
        );
        assert!(
            tracker
                .expired_pending(Duration::from_secs(3600))
                .is_empty(),
            "a fresh pending stream is not expired under a long TTL"
        );

        // Once connected, the GC task must never reap it, regardless of age.
        assert!(tracker.mark_connected("__console_x"));
        assert!(
            tracker.expired_pending(Duration::ZERO).is_empty(),
            "a connected stream is never expired"
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
}
