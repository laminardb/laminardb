//! HTTP API for LaminarDB server.
//!
//! Provides health checks, metrics, pipeline introspection, checkpoint
//! control, and ad-hoc SQL execution via a REST API.
//!
//! # Endpoints
//!
//! | Method | Path | Description |
//! |--------|------|-------------|
//! | `GET` | `/health` | Liveness probe |
//! | `GET` | `/ready` | Readiness probe |
//! | `GET` | `/metrics` | Prometheus text metrics |
//! | `GET` | `/api/v1/sources` | List sources |
//! | `GET` | `/api/v1/sinks` | List sinks |
//! | `GET` | `/api/v1/streams` | List streams |
//! | `GET` | `/api/v1/streams/{name}` | Stream detail |
//! | `POST` | `/api/v1/checkpoint` | Trigger checkpoint |
//! | `POST` | `/api/v1/sql` | Execute ad-hoc SQL |

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

use laminar_db::LaminarDB;

use crate::config::ServerConfig;
use crate::reload::{self, ReloadGuard};
use crate::server::ServerError;

/// Shared application state for all HTTP handlers.
#[allow(dead_code)]
pub struct AppState {
    /// Reference to the running LaminarDB instance.
    pub db: Arc<LaminarDB>,
    /// Path to the configuration file used to start this server.
    pub config_path: PathBuf,
    /// Server start time (UTC).
    pub started_at: chrono::DateTime<chrono::Utc>,
    /// Current active configuration (updated on reload).
    pub current_config: tokio::sync::RwLock<ServerConfig>,
    /// Guard preventing concurrent reloads.
    pub reload_guard: ReloadGuard,
    /// Total number of successful + failed reloads.
    pub reload_total: AtomicU64,
    /// Unix timestamp (seconds) of last reload attempt.
    pub reload_last_ts: AtomicU64,
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        // Admin dashboard
        .route("/", get(dashboard))
        // Health and observability
        .route("/health", get(health_check))
        .route("/ready", get(readiness_check))
        .route("/metrics", get(prometheus_metrics))
        .route("/api/v1/diagnostics", get(diagnostics))
        // Pipeline introspection
        .route("/api/v1/sources", get(list_sources))
        .route("/api/v1/sinks", get(list_sinks))
        .route("/api/v1/streams", get(list_streams))
        .route("/api/v1/streams/{name}", get(get_stream))
        // Actions
        .route("/api/v1/checkpoint", post(trigger_checkpoint))
        .route("/api/v1/sql", post(execute_sql))
        .route("/api/v1/reload", post(handle_reload))
        // Cluster (delta mode)
        .route("/api/v1/cluster", get(cluster_status))
        // Stubs (501 Not Implemented)
        .route("/api/v1/pause", post(not_implemented))
        .route("/api/v1/resume", post(not_implemented))
        .layer(CorsLayer::permissive())
        .layer(axum::middleware::from_fn(request_logging))
        .with_state(state)
}

/// Bind the router to a TCP address and spawn the server task.
///
/// Returns a `JoinHandle` that can be aborted for shutdown.
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

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Health check response.
#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    pipeline_state: &'static str,
}

/// Source listing response.
#[derive(Debug, Serialize)]
struct SourceResponse {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    watermark_column: Option<String>,
}

/// Stream listing response.
#[derive(Debug, Serialize)]
struct StreamResponse {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sql: Option<String>,
}

/// Sink listing response.
#[derive(Debug, Serialize)]
struct SinkResponse {
    name: String,
}

/// Checkpoint trigger response.
#[derive(Debug, Serialize)]
struct CheckpointResponse {
    success: bool,
    checkpoint_id: u64,
    epoch: u64,
    duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// SQL execution request.
#[derive(Debug, Deserialize)]
struct SqlRequest {
    sql: String,
}

/// SQL execution response.
#[derive(Debug, Serialize)]
struct SqlResponse {
    result_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    object_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows_affected: Option<u64>,
}

/// Diagnostics response.
#[derive(Debug, Serialize)]
struct DiagnosticsResponse {
    version: &'static str,
    pipeline_state: &'static str,
    uptime_seconds: u64,
    active_queries: usize,
    source_count: usize,
    stream_count: usize,
    sink_count: usize,
    counters: DiagnosticsCounters,
}

/// Counter subset for diagnostics.
#[derive(Debug, Serialize)]
struct DiagnosticsCounters {
    events_ingested: u64,
    events_emitted: u64,
    events_dropped: u64,
    checkpoints_completed: u64,
    checkpoints_failed: u64,
    checkpoint_epoch: u64,
}

/// Error response body.
#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

/// Escape a string for use as a Prometheus label value.
fn prom_label_value(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('\n', r"\n")
        .replace('"', r#"\""#)
}

/// Escape a string for safe embedding in HTML.
fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn error_response(status: StatusCode, msg: impl Into<String>) -> impl IntoResponse {
    (status, Json(ErrorBody { error: msg.into() }))
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /health` — liveness probe.
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

/// `GET /ready` — readiness probe (200 only when Running).
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

/// `GET /metrics` — Prometheus text format metrics.
async fn prometheus_metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let metrics = state.db.metrics();
    let source_metrics = state.db.all_source_metrics();
    let pipeline_state = state.db.pipeline_state();
    let uptime_secs = state.started_at.signed_duration_since(chrono::Utc::now());
    #[allow(clippy::cast_sign_loss)]
    let uptime = uptime_secs.num_seconds().unsigned_abs();

    let mut lines = Vec::new();

    lines.push(format!(
        "laminardb_events_ingested_total {}",
        metrics.total_events_ingested
    ));
    lines.push(format!(
        "laminardb_events_emitted_total {}",
        metrics.total_events_emitted
    ));
    lines.push(format!(
        "laminardb_events_dropped_total {}",
        metrics.total_events_dropped
    ));

    // Per-source detailed metrics
    for sm in &source_metrics {
        let label = prom_label_value(&sm.name);
        lines.push(format!(
            "laminardb_source_events_total{{source=\"{label}\"}} {}",
            sm.total_events
        ));
        lines.push(format!(
            "laminardb_source_pending{{source=\"{label}\"}} {}",
            sm.pending
        ));
        lines.push(format!(
            "laminardb_source_utilization{{source=\"{label}\"}} {:.4}",
            sm.utilization
        ));
        lines.push(format!(
            "laminardb_source_backpressured{{source=\"{label}\"}} {}",
            if sm.is_backpressured { 1 } else { 0 }
        ));
    }

    // Per-stream metrics
    let stream_metrics = state.db.all_stream_metrics();
    for sm in &stream_metrics {
        let label = prom_label_value(&sm.name);
        lines.push(format!(
            "laminardb_stream_events_total{{stream=\"{label}\"}} {}",
            sm.total_events
        ));
        lines.push(format!(
            "laminardb_stream_pending{{stream=\"{label}\"}} {}",
            sm.pending
        ));
    }

    // Active queries
    lines.push(format!(
        "laminardb_active_queries {}",
        state.db.active_query_count()
    ));

    lines.push(format!(
        "laminardb_pipeline_state_info{{state=\"{pipeline_state}\"}} 1"
    ));
    lines.push(format!("laminardb_uptime_seconds {uptime}"));
    lines.push(format!("laminardb_source_count {}", metrics.source_count));
    lines.push(format!("laminardb_stream_count {}", metrics.stream_count));
    lines.push(format!("laminardb_sink_count {}", metrics.sink_count));

    // Checkpoint metrics
    let snap = state.db.counters().snapshot();
    lines.push(format!(
        "laminardb_checkpoints_completed_total {}",
        snap.checkpoints_completed
    ));
    lines.push(format!(
        "laminardb_checkpoints_failed_total {}",
        snap.checkpoints_failed
    ));
    lines.push(format!(
        "laminardb_checkpoint_epoch {}",
        snap.checkpoint_epoch
    ));
    if snap.last_checkpoint_duration_ms > 0 {
        lines.push(format!(
            "laminardb_checkpoint_last_duration_ms {}",
            snap.last_checkpoint_duration_ms
        ));
    }

    // Cycle duration percentiles
    lines.push(format!(
        "laminardb_cycle_duration_p50_ns {}",
        snap.cycle_p50_ns
    ));
    lines.push(format!(
        "laminardb_cycle_duration_p95_ns {}",
        snap.cycle_p95_ns
    ));
    lines.push(format!(
        "laminardb_cycle_duration_p99_ns {}",
        snap.cycle_p99_ns
    ));

    let reload_total = state.reload_total.load(Ordering::Relaxed);
    let reload_last_ts = state.reload_last_ts.load(Ordering::Relaxed);
    lines.push(format!("laminardb_reload_total {reload_total}"));
    if reload_last_ts > 0 {
        lines.push(format!("laminardb_reload_last_timestamp {reload_last_ts}"));
    }

    (
        StatusCode::OK,
        [("content-type", "text/plain; charset=utf-8")],
        lines.join("\n"),
    )
}

/// `GET /api/v1/sources` — list all registered sources.
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

/// `GET /api/v1/sinks` — list all registered sinks.
async fn list_sinks(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let sinks: Vec<SinkResponse> = state
        .db
        .sinks()
        .into_iter()
        .map(|s| SinkResponse { name: s.name })
        .collect();
    Json(sinks)
}

/// `GET /api/v1/streams` — list all registered streams.
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

/// `GET /api/v1/streams/{name}` — get a stream by name.
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

/// `POST /api/v1/checkpoint` — trigger a manual checkpoint.
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

/// `POST /api/v1/sql` — execute ad-hoc SQL.
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
                },
                ExecuteResult::RowsAffected(n) => SqlResponse {
                    result_type: "rows_affected".to_string(),
                    object_name: None,
                    rows_affected: Some(n),
                },
                ExecuteResult::Query(_) => SqlResponse {
                    result_type: "query".to_string(),
                    object_name: None,
                    rows_affected: None,
                },
                ExecuteResult::Metadata(_) => SqlResponse {
                    result_type: "metadata".to_string(),
                    object_name: None,
                    rows_affected: None,
                },
            };
            Json(resp).into_response()
        }
        Err(e) => error_response(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

/// `POST /api/v1/reload` — trigger a configuration reload.
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
    let current = state.current_config.read().await;
    let diff = reload::diff_configs(&current, &new_config);
    drop(current);

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
    state.reload_total.fetch_add(1, Ordering::Relaxed);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let now = chrono::Utc::now().timestamp() as u64;
    state.reload_last_ts.store(now, Ordering::Relaxed);

    // Update current config on success
    if result.success {
        let mut current = state.current_config.write().await;
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

/// `GET /api/v1/cluster` — cluster status (delta mode only).
async fn cluster_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let config = state.current_config.read().await;
    if config.server.mode != "delta" {
        return error_response(
            StatusCode::NOT_FOUND,
            "cluster endpoint is only available in delta mode",
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
        mode: "delta",
        node_id,
        pipeline_state,
    })
    .into_response()
}

/// `GET /` -- minimal HTML status dashboard.
async fn dashboard(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let metrics = state.db.metrics();
    let pipeline_state = state.db.pipeline_state();
    let source_metrics = state.db.all_source_metrics();
    let stream_metrics = state.db.all_stream_metrics();
    let snap = state.db.counters().snapshot();
    let uptime = state.started_at.signed_duration_since(chrono::Utc::now());
    #[allow(clippy::cast_sign_loss)]
    let uptime_secs = uptime.num_seconds().unsigned_abs();

    let mut source_rows = String::new();
    for sm in &source_metrics {
        source_rows.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{:.1}%</td></tr>",
            escape_html(&sm.name),
            sm.total_events,
            sm.pending,
            sm.utilization * 100.0
        ));
    }
    let mut stream_rows = String::new();
    for sm in &stream_metrics {
        stream_rows.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape_html(&sm.name),
            sm.total_events,
            sm.pending
        ));
    }

    let html = format!(
        r#"<!DOCTYPE html>
<html><head><title>LaminarDB</title>
<meta http-equiv="refresh" content="5">
<style>
body {{ font-family: monospace; margin: 2em; background: #1a1a2e; color: #eee; }}
table {{ border-collapse: collapse; margin: 1em 0; }}
th, td {{ border: 1px solid #444; padding: 4px 12px; text-align: left; }}
th {{ background: #16213e; }}
.ok {{ color: #0f0; }} .warn {{ color: #ff0; }}
h1 {{ color: #e94560; }}
</style></head><body>
<h1>LaminarDB v{version}</h1>
<p>State: <span class="{state_class}">{pipeline_state}</span> | Uptime: {uptime_secs}s</p>
<h2>Counters</h2>
<table>
<tr><th>Metric</th><th>Value</th></tr>
<tr><td>Events ingested</td><td>{ingested}</td></tr>
<tr><td>Events emitted</td><td>{emitted}</td></tr>
<tr><td>Events dropped</td><td>{dropped}</td></tr>
<tr><td>Checkpoints completed</td><td>{ckpt_ok}</td></tr>
<tr><td>Checkpoints failed</td><td>{ckpt_fail}</td></tr>
</table>
<h2>Sources ({source_count})</h2>
<table><tr><th>Name</th><th>Events</th><th>Pending</th><th>Utilization</th></tr>{source_rows}</table>
<h2>Streams ({stream_count})</h2>
<table><tr><th>Name</th><th>Events</th><th>Pending</th></tr>{stream_rows}</table>
<h2>Sinks ({sink_count})</h2>
<p>{sink_count} sink(s) registered</p>
</body></html>"#,
        version = env!("CARGO_PKG_VERSION"),
        state_class = if pipeline_state == "Running" {
            "ok"
        } else {
            "warn"
        },
        pipeline_state = pipeline_state,
        uptime_secs = uptime_secs,
        ingested = metrics.total_events_ingested,
        emitted = metrics.total_events_emitted,
        dropped = metrics.total_events_dropped,
        ckpt_ok = snap.checkpoints_completed,
        ckpt_fail = snap.checkpoints_failed,
        source_count = metrics.source_count,
        source_rows = source_rows,
        stream_count = metrics.stream_count,
        stream_rows = stream_rows,
        sink_count = metrics.sink_count,
    );

    (
        StatusCode::OK,
        [("content-type", "text/html; charset=utf-8")],
        html,
    )
}

/// `GET /api/v1/diagnostics` -- server diagnostics snapshot.
async fn diagnostics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let metrics = state.db.metrics();
    let snap = state.db.counters().snapshot();
    let pipeline_state = state.db.pipeline_state();
    let uptime = state.started_at.signed_duration_since(chrono::Utc::now());
    #[allow(clippy::cast_sign_loss)]
    let uptime_secs = uptime.num_seconds().unsigned_abs();

    Json(DiagnosticsResponse {
        version: env!("CARGO_PKG_VERSION"),
        pipeline_state,
        uptime_seconds: uptime_secs,
        active_queries: state.db.active_query_count(),
        source_count: metrics.source_count,
        stream_count: metrics.stream_count,
        sink_count: metrics.sink_count,
        counters: DiagnosticsCounters {
            events_ingested: snap.events_ingested,
            events_emitted: snap.events_emitted,
            events_dropped: snap.events_dropped,
            checkpoints_completed: snap.checkpoints_completed,
            checkpoints_failed: snap.checkpoints_failed,
            checkpoint_epoch: snap.checkpoint_epoch,
        },
    })
}

/// Stub handler for unimplemented endpoints.
async fn not_implemented() -> impl IntoResponse {
    error_response(
        StatusCode::NOT_IMPLEMENTED,
        "this endpoint is not yet implemented",
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_state() -> Arc<AppState> {
        Arc::new(AppState {
            db: Arc::new(LaminarDB::open().unwrap()),
            config_path: PathBuf::from("test.toml"),
            started_at: chrono::Utc::now(),
            current_config: tokio::sync::RwLock::new(crate::config::ServerConfig {
                server: crate::config::ServerSection::default(),
                state: crate::config::StateSection::default(),
                checkpoint: crate::config::CheckpointSection::default(),
                sources: vec![],
                lookups: vec![],
                pipelines: vec![],
                sinks: vec![],
                discovery: None,
                coordination: None,
                node_id: None,
                telemetry: None,
            }),
            reload_guard: ReloadGuard::new(),
            reload_total: AtomicU64::new(0),
            reload_last_ts: AtomicU64::new(0),
        })
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

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("laminardb_events_ingested_total"));
        assert!(text.contains("laminardb_pipeline_state_info"));
        assert!(text.contains("laminardb_uptime_seconds"));
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
    async fn test_not_implemented_stubs() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/pause")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
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

        let state = Arc::new(AppState {
            db: Arc::new(LaminarDB::open().unwrap()),
            config_path: path,
            started_at: chrono::Utc::now(),
            current_config: tokio::sync::RwLock::new(crate::config::ServerConfig {
                server: crate::config::ServerSection::default(),
                state: crate::config::StateSection::default(),
                checkpoint: crate::config::CheckpointSection::default(),
                sources: vec![],
                lookups: vec![],
                pipelines: vec![],
                sinks: vec![],
                discovery: None,
                coordination: None,
                node_id: None,
                telemetry: None,
            }),
            reload_guard: ReloadGuard::new(),
            reload_total: AtomicU64::new(0),
            reload_last_ts: AtomicU64::new(0),
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
    async fn test_metrics_includes_reload() {
        let state = test_state();
        state.reload_total.store(5, Ordering::Relaxed);
        state.reload_last_ts.store(1_700_000_000, Ordering::Relaxed);
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
        assert!(text.contains("laminardb_reload_total 5"));
        assert!(text.contains("laminardb_reload_last_timestamp 1700000000"));
    }

    #[tokio::test]
    async fn test_dashboard() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder().uri("/").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("LaminarDB"));
        assert!(text.contains("<!DOCTYPE html>"));
    }

    #[tokio::test]
    async fn test_diagnostics() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/diagnostics")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["version"].is_string());
        assert!(json["pipeline_state"].is_string());
        assert!(json["counters"]["events_ingested"].is_number());
    }

    #[tokio::test]
    async fn test_metrics_enhanced() {
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
        assert!(text.contains("laminardb_active_queries"));
        assert!(text.contains("laminardb_checkpoints_completed_total"));
        assert!(text.contains("laminardb_cycle_duration_p50_ns"));
    }
}
