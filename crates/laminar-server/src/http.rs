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
//! | `GET` | `/console` | SQL query console (web UI) |
//! | `GET` | `/dashboard` | Observability dashboard (web UI) |
//! | `GET` | `/api/v1/config` | Current running config (secrets masked) |
//! | `PUT` | `/api/v1/config` | Update config values at runtime |
//! | `POST` | `/api/v1/config/validate` | Validate config without applying |

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
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
    // Admin/write routes: no permissive CORS (same-origin only by default).
    // Browser JS from other origins cannot call these endpoints.
    let admin_routes = Router::new()
        .route("/api/v1/checkpoint", post(trigger_checkpoint))
        .route("/api/v1/sql", post(execute_sql))
        .route("/api/v1/reload", post(handle_reload))
        .route("/api/v1/config", get(get_config).put(update_config))
        .route("/api/v1/config/validate", post(validate_config_endpoint))
        .route("/api/v1/pause", post(not_implemented))
        .route("/api/v1/resume", post(not_implemented))
        .layer(CorsLayer::new()); // restrictive: no allow_origin = deny cross-origin

    // Read-only/public routes: permissive CORS for dashboards and monitoring.
    let public_routes = Router::new()
        .route("/health", get(health_check))
        .route("/ready", get(readiness_check))
        .route("/metrics", get(prometheus_metrics))
        .route("/console", get(sql_console_page))
        .route("/dashboard", get(dashboard_page))
        .route("/api/v1/sources", get(list_sources))
        .route("/api/v1/sinks", get(list_sinks))
        .route("/api/v1/streams", get(list_streams))
        .route("/api/v1/streams/{name}", get(get_stream))
        .route("/api/v1/cluster", get(cluster_status))
        .layer(
            CorsLayer::new()
                .allow_origin(tower_http::cors::Any)
                .allow_methods([axum::http::Method::GET, axum::http::Method::OPTIONS])
                .allow_headers([axum::http::header::CONTENT_TYPE]),
        );

    public_routes
        .merge(admin_routes)
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

/// Error response body.
#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
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

    for sm in &source_metrics {
        lines.push(format!(
            "laminardb_source_events_total{{source=\"{}\"}} {}",
            sm.name, sm.total_events
        ));
    }

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
///
/// Returns tabular results (columns + rows) for Metadata queries (SHOW, DESCRIBE).
async fn execute_sql(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SqlRequest>,
) -> impl IntoResponse {
    match state.db.execute(&req.sql).await {
        Ok(result) => {
            use laminar_db::ExecuteResult;
            match result {
                ExecuteResult::Ddl(info) => Json(serde_json::json!({
                    "result_type": info.statement_type,
                    "object_name": info.object_name,
                }))
                .into_response(),
                ExecuteResult::RowsAffected(n) => Json(serde_json::json!({
                    "result_type": "rows_affected",
                    "rows_affected": n,
                }))
                .into_response(),
                ExecuteResult::Query(_) => Json(serde_json::json!({
                    "result_type": "query",
                }))
                .into_response(),
                ExecuteResult::Metadata(batch) => {
                    Json(record_batch_to_json(&batch)).into_response()
                }
            }
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

    // Always update current_config to reflect the file on disk. Even on
    // partial apply, the next diff should be computed from what the file says.
    {
        let mut current = state.current_config.write().await;
        *current = new_config;
    }
    if result.success {
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

// ---------------------------------------------------------------------------
// RecordBatch → JSON helper
// ---------------------------------------------------------------------------

/// Convert an Arrow `RecordBatch` to a JSON object with `columns` and `rows`.
fn record_batch_to_json(batch: &arrow_array::RecordBatch) -> serde_json::Value {
    let schema = batch.schema();
    let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let opts = arrow_cast::display::FormatOptions::default();

    // Pre-build formatters for each column
    let formatters: Vec<Option<arrow_cast::display::ArrayFormatter>> = (0..batch.num_columns())
        .map(|i| {
            let col: &dyn arrow_array::Array = batch.column(i).as_ref();
            arrow_cast::display::ArrayFormatter::try_new(col, &opts).ok()
        })
        .collect();

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut row_values = Vec::with_capacity(columns.len());
        for (col_idx, fmt) in formatters.iter().enumerate() {
            let col = batch.column(col_idx);
            if col.is_null(row) {
                row_values.push(serde_json::Value::Null);
            } else if let Some(f) = fmt {
                row_values.push(serde_json::Value::String(f.value(row).to_string()));
            } else {
                row_values.push(serde_json::Value::String("?".to_string()));
            }
        }
        rows.push(serde_json::Value::Array(row_values));
    }
    serde_json::json!({ "columns": columns, "rows": rows })
}

// ---------------------------------------------------------------------------
// Web UIs (embedded HTML)
// ---------------------------------------------------------------------------

/// `GET /console` — SQL query console web UI.
async fn sql_console_page() -> impl IntoResponse {
    Html(include_str!("console.html"))
}

/// `GET /dashboard` — observability dashboard web UI.
async fn dashboard_page(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Html(build_dashboard_html(&state).await)
}

/// Build the dashboard HTML with current server data embedded for initial render.
async fn build_dashboard_html(state: &AppState) -> String {
    let metrics = state.db.metrics();
    let source_metrics = state.db.all_source_metrics();
    let pipeline_state = state.db.pipeline_state();
    let uptime_secs = (chrono::Utc::now() - state.started_at).num_seconds().max(0);
    let snap = state.db.counters().snapshot();
    let sources_json_arr: Vec<serde_json::Value> = source_metrics
        .iter()
        .map(|s| {
            serde_json::json!({
                "name": s.name,
                "total_events": s.total_events,
                "pending": s.pending,
                "capacity": s.capacity,
                "is_backpressured": s.is_backpressured,
            })
        })
        .collect();
    // Escape "</script>" in JSON to prevent XSS when injected into HTML <script> tags.
    let sources_json = serde_json::to_string(&sources_json_arr)
        .unwrap_or_else(|_| "[]".into())
        .replace("</", "<\\/");
    let streams = state.db.streams();
    let sinks = state.db.sinks();

    format!(
        include_str!("dashboard.html"),
        version = env!("CARGO_PKG_VERSION"),
        pipeline_state = pipeline_state,
        uptime = format_duration(uptime_secs as u64),
        events_ingested = metrics.total_events_ingested,
        events_emitted = metrics.total_events_emitted,
        events_dropped = metrics.total_events_dropped,
        source_count = metrics.source_count,
        stream_count = metrics.stream_count,
        sink_count = metrics.sink_count,
        checkpoints_completed = snap.checkpoints_completed,
        checkpoints_failed = snap.checkpoints_failed,
        checkpoint_epoch = snap.checkpoint_epoch,
        cycle_p50 = snap.cycle_p50_ns,
        cycle_p99 = snap.cycle_p99_ns,
        sources_json = sources_json,
        streams_len = streams.len(),
        sinks_len = sinks.len(),
    )
}

/// Format seconds into a human-friendly duration string.
fn format_duration(secs: u64) -> String {
    if secs < 60 {
        return format!("{secs}s");
    }
    if secs < 3600 {
        return format!("{}m {}s", secs / 60, secs % 60);
    }
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    format!("{hours}h {mins}m")
}

// ---------------------------------------------------------------------------
// Configuration management
// ---------------------------------------------------------------------------

/// `GET /api/v1/config` — return current running configuration with secrets masked.
async fn get_config(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let config = state.current_config.read().await;
    let masked = mask_config_secrets(&config);
    Json(masked)
}

/// `PUT /api/v1/config` — update specific config values at runtime.
async fn update_config(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let _guard = match state.reload_guard.try_acquire() {
        Some(g) => g,
        None => {
            return error_response(StatusCode::CONFLICT, "a reload is already in progress")
                .into_response();
        }
    };

    let current = state.current_config.read().await;
    // Serialize the REAL config to TOML string (round-trip safe).
    let base_toml_str = match toml::to_string(&config_to_merge_toml(&current)) {
        Ok(s) => s,
        Err(e) => {
            drop(current);
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("internal config serialization error: {e}"),
            )
            .into_response();
        }
    };
    drop(current);

    // Parse base TOML string back as a generic TOML value for merging.
    let mut merged: toml::Value = match toml::from_str(&base_toml_str) {
        Ok(v) => v,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("internal config parse error: {e}"),
            )
            .into_response();
        }
    };

    // Filter out "***" masked placeholders before merging, so that values
    // copied from GET /config don't overwrite real secrets with the mask.
    let filtered_body = filter_masked_values(&body);

    // Convert the JSON patch to a TOML value and merge.
    let patch_toml: toml::Value = match serde_json::from_value(filtered_body) {
        Ok(v) => v,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("invalid config update: {e}"),
            )
            .into_response();
        }
    };
    merge_toml(&mut merged, &patch_toml);

    let merged_toml = match toml::to_string(&merged) {
        Ok(t) => t,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("invalid config update: {e}"),
            )
            .into_response();
        }
    };

    let new_config: crate::config::ServerConfig = match toml::from_str(&merged_toml) {
        Ok(c) => c,
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, format!("config parse error: {e}"))
                .into_response();
        }
    };

    if let Err(e) = crate::config::validate_config_public(&new_config) {
        return error_response(
            StatusCode::BAD_REQUEST,
            format!("config validation error: {e}"),
        )
        .into_response();
    }

    let current = state.current_config.read().await;
    let diff = crate::reload::diff_configs(&current, &new_config);
    drop(current);

    if diff.is_empty() && diff.warnings.is_empty() {
        return Json(serde_json::json!({
            "success": true,
            "message": "no changes detected",
        }))
        .into_response();
    }

    let result = crate::reload::apply_reload(&state.db, &diff).await;

    // Always update current_config to reflect the desired state. Even on
    // partial apply (207), the config represents what the user intended.
    // A subsequent reload or PUT will re-diff from the correct base.
    {
        let mut current = state.current_config.write().await;
        *current = new_config;
    }

    let status = if result.success {
        StatusCode::OK
    } else {
        StatusCode::MULTI_STATUS
    };

    (status, Json(result)).into_response()
}

/// `POST /api/v1/config/validate` — validate a configuration without applying it.
async fn validate_config_endpoint(Json(body): Json<ConfigValidateRequest>) -> impl IntoResponse {
    let config: Result<crate::config::ServerConfig, _> = toml::from_str(&body.config);
    match config {
        Ok(cfg) => match crate::config::validate_config_public(&cfg) {
            Ok(()) => Json(serde_json::json!({ "valid": true, "errors": [] })).into_response(),
            Err(e) => Json(serde_json::json!({ "valid": false, "errors": [e.to_string()] }))
                .into_response(),
        },
        Err(e) => Json(
            serde_json::json!({ "valid": false, "errors": [format!("TOML parse error: {e}")] }),
        )
        .into_response(),
    }
}

/// Request body for config validation.
#[derive(Debug, Deserialize)]
struct ConfigValidateRequest {
    config: String,
}

/// Serialize config to a TOML value for merge operations.
///
/// Uses serde serialization to produce a complete, round-trip-safe TOML
/// representation including all fields (schema, watermark, discovery, etc.).
fn config_to_merge_toml(config: &crate::config::ServerConfig) -> toml::Value {
    toml::Value::try_from(config).unwrap_or_else(|_| toml::Value::Table(toml::Table::new()))
}

/// Convert a TOML value to a JSON value for merge operations.
fn toml_to_json(value: &toml::Value) -> serde_json::Value {
    match value {
        toml::Value::String(s) => serde_json::Value::String(s.clone()),
        toml::Value::Integer(i) => serde_json::json!(*i),
        toml::Value::Float(f) => serde_json::json!(*f),
        toml::Value::Boolean(b) => serde_json::json!(*b),
        toml::Value::Datetime(dt) => serde_json::Value::String(dt.to_string()),
        toml::Value::Array(arr) => serde_json::Value::Array(arr.iter().map(toml_to_json).collect()),
        toml::Value::Table(t) => {
            let obj: serde_json::Map<String, serde_json::Value> = t
                .iter()
                .map(|(k, v)| (k.clone(), toml_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}

/// Mask sensitive values in config for safe display.
///
/// Serializes the full config via serde, then walks the JSON tree to replace
/// any property value whose key looks sensitive with `"***"`.
fn mask_config_secrets(config: &crate::config::ServerConfig) -> serde_json::Value {
    let toml_val = config_to_merge_toml(config);
    let mut json = toml_to_json(&toml_val);
    mask_sensitive_values(&mut json);
    json
}

/// Recursively walk a JSON value and replace sensitive property values with `"***"`.
fn mask_sensitive_values(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(obj) => {
            for (key, val) in obj.iter_mut() {
                if is_sensitive_key(key) && val.is_string() {
                    *val = serde_json::Value::String("***".to_string());
                } else {
                    mask_sensitive_values(val);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for item in arr.iter_mut() {
                mask_sensitive_values(item);
            }
        }
        _ => {}
    }
}

/// Returns `true` if a config key name looks like it contains credentials.
fn is_sensitive_key(key: &str) -> bool {
    let lower = key.to_lowercase();
    lower.contains("password")
        || lower.contains("secret")
        || lower.contains("token")
        || lower.contains("credential")
        || lower.contains("api_key")
        || lower.contains("apikey")
        || lower.contains("access_key")
}

/// Recursively strip any JSON string values equal to `"***"` from the object.
///
/// When a client PUTs config data that was previously read from GET /config,
/// masked placeholders (`"***"`) would overwrite real secrets. This filter
/// removes those entries so the merge only touches values the client intended
/// to change.
fn filter_masked_values(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(obj) => {
            let filtered: serde_json::Map<String, serde_json::Value> = obj
                .iter()
                .filter(|(_, v)| v.as_str() != Some("***"))
                .map(|(k, v)| (k.clone(), filter_masked_values(v)))
                .collect();
            serde_json::Value::Object(filtered)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(filter_masked_values).collect())
        }
        other => other.clone(),
    }
}

/// Deep-merge `patch` into `target` in TOML value space.
///
/// Arrays of tables with a `"name"` key are merged by matching name.
fn merge_toml(target: &mut toml::Value, patch: &toml::Value) {
    if let (Some(target_tbl), Some(patch_tbl)) = (target.as_table_mut(), patch.as_table()) {
        for (key, value) in patch_tbl {
            if value.is_table() {
                let entry = target_tbl
                    .entry(key)
                    .or_insert_with(|| toml::Value::Table(toml::Table::new()));
                merge_toml(entry, value);
            } else if let Some(patch_arr) = value.as_array() {
                // Merge arrays of tables by matching on "name" key
                if let Some(target_val) = target_tbl.get(key) {
                    if let Some(target_arr) = target_val.as_array() {
                        let merged = merge_named_toml_arrays(target_arr, patch_arr);
                        target_tbl.insert(key.clone(), toml::Value::Array(merged));
                        continue;
                    }
                }
                target_tbl.insert(key.clone(), value.clone());
            } else {
                target_tbl.insert(key.clone(), value.clone());
            }
        }
    }
}

/// Merge two TOML arrays of tables by matching on the `"name"` field.
///
/// Patch items are merged into matching target items. Target items not
/// present in the patch are preserved (not dropped).
fn merge_named_toml_arrays(target: &[toml::Value], patch: &[toml::Value]) -> Vec<toml::Value> {
    let patch_has_names = patch.iter().all(|v| {
        v.as_table()
            .and_then(|t| t.get("name"))
            .and_then(|n| n.as_str())
            .is_some()
    });

    if patch.is_empty() {
        return patch.to_vec();
    }

    // If patch items lack "name" fields, we cannot match them against
    // target items. Preserve the existing target to avoid silently
    // dropping sensitive values (secrets would be lost on wholesale
    // replacement since filter_masked_values strips "***" placeholders).
    if !patch_has_names {
        return target.to_vec();
    }

    // Collect patch names for lookup.
    let patch_names: Vec<Option<&str>> = patch
        .iter()
        .map(|v| {
            v.as_table()
                .and_then(|t| t.get("name"))
                .and_then(|n| n.as_str())
        })
        .collect();

    let mut result = Vec::with_capacity(target.len().max(patch.len()));

    // Start with all target items, merging in patches where names match.
    for target_item in target {
        let target_name = target_item
            .as_table()
            .and_then(|t| t.get("name"))
            .and_then(|n| n.as_str());

        if let Some(name) = target_name {
            if let Some(idx) = patch_names.iter().position(|pn| *pn == Some(name)) {
                // Merge patch into target item
                let mut merged = target_item.clone();
                merge_toml(&mut merged, &patch[idx]);
                result.push(merged);
                continue;
            }
        }
        // Target item not in patch: preserve it
        result.push(target_item.clone());
    }

    // Append patch items that don't match any target item (new entries).
    for (i, patch_item) in patch.iter().enumerate() {
        let patch_name = patch_names[i];
        if let Some(name) = patch_name {
            let already_merged = target.iter().any(|t| {
                t.as_table()
                    .and_then(|tbl| tbl.get("name"))
                    .and_then(|n| n.as_str())
                    == Some(name)
            });
            if !already_merged {
                result.push(patch_item.clone());
            }
        } else {
            result.push(patch_item.clone());
        }
    }

    result
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

    /// Deep-merge `patch` into `target` (JSON merge-patch semantics, test helper).
    fn merge_json(target: &mut serde_json::Value, patch: &serde_json::Value) {
        if let (Some(target_obj), Some(patch_obj)) = (target.as_object_mut(), patch.as_object()) {
            for (key, value) in patch_obj {
                if value.is_null() {
                    target_obj.remove(key);
                } else if value.is_object() {
                    let entry = target_obj
                        .entry(key.clone())
                        .or_insert(serde_json::json!({}));
                    merge_json(entry, value);
                } else {
                    target_obj.insert(key.clone(), value.clone());
                }
            }
        } else {
            *target = patch.clone();
        }
    }
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
    async fn test_sql_console_page() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/console")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("SQL Console"));
        assert!(text.contains("/api/v1/sql"));
    }

    #[tokio::test]
    async fn test_dashboard_page() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/dashboard")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("LaminarDB Dashboard"));
        assert!(text.contains("auto-refresh"));
    }

    #[tokio::test]
    async fn test_get_config() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/config")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["server"]["mode"], "embedded");
        assert_eq!(json["server"]["bind"], "127.0.0.1:8080");
    }

    #[tokio::test]
    async fn test_config_masks_secrets() {
        let config = crate::config::ServerConfig {
            server: crate::config::ServerSection::default(),
            state: crate::config::StateSection::default(),
            checkpoint: crate::config::CheckpointSection::default(),
            sources: vec![crate::config::SourceConfig {
                name: "src1".to_string(),
                connector: "kafka".to_string(),
                format: "json".to_string(),
                properties: {
                    let mut t = toml::Table::new();
                    t.insert(
                        "password".to_string(),
                        toml::Value::String("hunter2".to_string()),
                    );
                    t.insert(
                        "topic".to_string(),
                        toml::Value::String("events".to_string()),
                    );
                    t
                },
                schema: vec![],
                watermark: None,
            }],
            lookups: vec![],
            pipelines: vec![],
            sinks: vec![],
            discovery: None,
            coordination: None,
            node_id: None,
        };

        let masked = mask_config_secrets(&config);
        let sources = masked["source"].as_array().unwrap();
        let props = &sources[0]["properties"];
        assert_eq!(props["password"], "***");
        assert_ne!(props["topic"], "***");
    }

    #[tokio::test]
    async fn test_validate_config_valid() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/config/validate")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({
                    "config": "[server]\nbind = \"127.0.0.1:8080\"\n"
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
        assert_eq!(json["valid"], true);
    }

    #[tokio::test]
    async fn test_validate_config_invalid_toml() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/config/validate")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({
                    "config": "this is not valid toml {{{"
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
        assert_eq!(json["valid"], false);
        assert!(!json["errors"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_validate_config_semantic_error() {
        let state = test_state();
        let app = build_router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/config/validate")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({
                    "config": "[server]\nbind = \"not-a-socket-addr\"\n"
                }))
                .unwrap(),
            ))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["valid"], false);
    }

    #[tokio::test]
    async fn test_execute_sql_show_sources() {
        let state = test_state();
        state
            .db
            .execute("CREATE SOURCE test_show (id BIGINT)")
            .await
            .unwrap();
        let app = build_router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/sql")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({
                    "sql": "SHOW SOURCES"
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
        assert!(json["columns"].is_array());
        assert!(json["rows"].is_array());
    }

    #[test]
    fn test_is_sensitive_key() {
        assert!(is_sensitive_key("password"));
        assert!(is_sensitive_key("kafka_password"));
        assert!(is_sensitive_key("secret_key"));
        assert!(is_sensitive_key("api_key"));
        assert!(is_sensitive_key("access_key_id"));
        assert!(is_sensitive_key("TOKEN"));
        assert!(!is_sensitive_key("topic"));
        assert!(!is_sensitive_key("brokers"));
        assert!(!is_sensitive_key("format"));
    }

    #[test]
    fn test_merge_json() {
        let mut target = serde_json::json!({"a": 1, "b": {"c": 2}});
        let patch = serde_json::json!({"b": {"d": 3}, "e": 4});
        merge_json(&mut target, &patch);
        assert_eq!(target["a"], 1);
        assert_eq!(target["b"]["c"], 2);
        assert_eq!(target["b"]["d"], 3);
        assert_eq!(target["e"], 4);
    }

    #[test]
    fn test_merge_json_null_removes() {
        let mut target = serde_json::json!({"a": 1, "b": 2});
        let patch = serde_json::json!({"b": null});
        merge_json(&mut target, &patch);
        assert_eq!(target["a"], 1);
        assert!(target.get("b").is_none());
    }

    #[test]
    fn test_filter_masked_values_strips_stars() {
        let input = serde_json::json!({
            "name": "kafka_src",
            "password": "***",
            "nested": {
                "secret": "***",
                "keep": "value"
            }
        });
        let filtered = filter_masked_values(&input);
        assert!(
            filtered.get("password").is_none(),
            "masked placeholder must be removed"
        );
        assert_eq!(filtered["name"], "kafka_src");
        assert!(filtered["nested"].get("secret").is_none());
        assert_eq!(filtered["nested"]["keep"], "value");
    }

    #[tokio::test]
    async fn test_update_config_preserves_secrets() {
        // Regression: PUT /api/v1/config must merge against the real config,
        // not the masked one, otherwise secrets get replaced with "***".
        let state = Arc::new(AppState {
            db: Arc::new(LaminarDB::open().unwrap()),
            config_path: PathBuf::from("test.toml"),
            started_at: chrono::Utc::now(),
            current_config: tokio::sync::RwLock::new(crate::config::ServerConfig {
                server: crate::config::ServerSection::default(),
                state: crate::config::StateSection::default(),
                checkpoint: crate::config::CheckpointSection::default(),
                sources: vec![crate::config::SourceConfig {
                    name: "kafka_src".to_string(),
                    connector: "kafka".to_string(),
                    format: "json".to_string(),
                    properties: {
                        let mut t = toml::Table::new();
                        t.insert(
                            "password".to_string(),
                            toml::Value::String("super_secret_123".to_string()),
                        );
                        t.insert(
                            "topic".to_string(),
                            toml::Value::String("events".to_string()),
                        );
                        t
                    },
                    schema: vec![],
                    watermark: None,
                }],
                lookups: vec![],
                pipelines: vec![],
                sinks: vec![],
                discovery: None,
                coordination: None,
                node_id: None,
            }),
            reload_guard: ReloadGuard::new(),
            reload_total: AtomicU64::new(0),
            reload_last_ts: AtomicU64::new(0),
        });

        // PUT that changes only the server log level — should not touch source passwords.
        let app = build_router(Arc::clone(&state));
        let req = Request::builder()
            .method("PUT")
            .uri("/api/v1/config")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({
                    "server": { "log_level": "debug" }
                }))
                .unwrap(),
            ))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert!(
            resp.status().is_success(),
            "PUT /api/v1/config should succeed, got {}",
            resp.status()
        );

        // Verify the real config still has the original password, not "***".
        let cfg = state.current_config.read().await;
        let pw = cfg.sources[0]
            .properties
            .get("password")
            .and_then(|v| v.as_str());
        assert_eq!(
            pw,
            Some("super_secret_123"),
            "PUT must not destroy secrets — password should survive config merge"
        );
    }

    #[test]
    fn test_merge_named_toml_arrays_preserves_target_on_nameless_patch() {
        // Regression: a patch with items lacking "name" should NOT wholesale
        // replace the target array, as this would lose secrets that
        // filter_masked_values already stripped from the patch.
        let target = vec![toml::Value::Table({
            let mut t = toml::Table::new();
            t.insert("name".into(), toml::Value::String("src1".into()));
            t.insert("password".into(), toml::Value::String("real_secret".into()));
            t
        })];
        let nameless_patch = vec![toml::Value::Table({
            let mut t = toml::Table::new();
            t.insert("connector".into(), toml::Value::String("kafka".into()));
            t
        })];
        let result = merge_named_toml_arrays(&target, &nameless_patch);
        // Target must be preserved since patch items lack names.
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0]
                .as_table()
                .unwrap()
                .get("password")
                .unwrap()
                .as_str(),
            Some("real_secret"),
        );
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(45), "45s");
        assert_eq!(format_duration(90), "1m 30s");
        assert_eq!(format_duration(3661), "1h 1m");
    }

    #[test]
    fn test_record_batch_to_json() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc as StdArc;

        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![1, 2])),
                StdArc::new(StringArray::from(vec![Some("alice"), None])),
            ],
        )
        .unwrap();

        let json = record_batch_to_json(&batch);
        assert_eq!(json["columns"], serde_json::json!(["id", "name"]));
        let rows = json["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], "1");
        assert_eq!(rows[0][1], "alice");
        assert!(rows[1][1].is_null());
    }
}
