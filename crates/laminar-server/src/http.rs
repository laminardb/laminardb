//! HTTP API for LaminarDB server.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use prometheus::Registry;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
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
use crate::metrics::ServerMetrics;
use crate::reload::{self, ReloadGuard};
use crate::server::ServerError;

pub struct AppState {
    pub db: Arc<LaminarDB>,
    pub config_path: PathBuf,
    pub current_config: tokio::sync::RwLock<ServerConfig>,
    pub reload_guard: ReloadGuard,
    pub registry: Arc<Registry>,
    pub server_metrics: ServerMetrics,
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/ready", get(readiness_check))
        .route("/metrics", get(prometheus_metrics))
        .route("/api/v1/sources", get(list_sources))
        .route("/api/v1/sinks", get(list_sinks))
        .route("/api/v1/streams", get(list_streams))
        .route("/api/v1/streams/{name}", get(get_stream))
        .route("/api/v1/checkpoint", post(trigger_checkpoint))
        .route("/api/v1/sql", post(execute_sql))
        .route("/api/v1/reload", post(handle_reload))
        .route("/api/v1/cluster", get(cluster_status))
        .route("/ws/{name}", get(ws_upgrade))
        .layer(CorsLayer::permissive())
        .layer(axum::middleware::from_fn(request_logging))
        .with_state(state)
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
    state.server_metrics.reload_total.inc();

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

    let st = Arc::clone(&state);
    ws.on_upgrade(move |socket| async move {
        st.server_metrics.ws_connections.inc();
        ws_client(socket, sub, name).await;
        st.server_metrics.ws_connections.dec();
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
            current_config: tokio::sync::RwLock::new(crate::config::ServerConfig {
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
            }),
            reload_guard: ReloadGuard::new(),

            registry,
            server_metrics,
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
            current_config: tokio::sync::RwLock::new(crate::config::ServerConfig {
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
            }),
            reload_guard: ReloadGuard::new(),

            registry,
            server_metrics,
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
}
