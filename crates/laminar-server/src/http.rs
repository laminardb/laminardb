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
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tracing::info;

use laminar_db::LaminarDB;

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
}

/// Build the axum router with all endpoints.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        // Health and observability
        .route("/health", get(health_check))
        .route("/ready", get(readiness_check))
        .route("/metrics", get(prometheus_metrics))
        // Pipeline introspection
        .route("/api/v1/sources", get(list_sources))
        .route("/api/v1/sinks", get(list_sinks))
        .route("/api/v1/streams", get(list_streams))
        .route("/api/v1/streams/{name}", get(get_stream))
        // Actions
        .route("/api/v1/checkpoint", post(trigger_checkpoint))
        .route("/api/v1/sql", post(execute_sql))
        // Stubs (501 Not Implemented)
        .route("/api/v1/pause", post(not_implemented))
        .route("/api/v1/resume", post(not_implemented))
        .route("/api/v1/reload", post(not_implemented))
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
    lines.push(format!(
        "laminardb_source_count {}",
        metrics.source_count
    ));
    lines.push(format!(
        "laminardb_stream_count {}",
        metrics.stream_count
    ));
    lines.push(format!(
        "laminardb_sink_count {}",
        metrics.sink_count
    ));

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
        None => {
            error_response(StatusCode::NOT_FOUND, format!("stream '{name}' not found"))
                .into_response()
        }
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
        Err(e) => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
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
        Err(e) => {
            error_response(StatusCode::BAD_REQUEST, e.to_string()).into_response()
        }
    }
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
}
