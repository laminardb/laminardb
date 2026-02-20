# F-SERVER-003: HTTP API (axum)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SERVER-003 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6c |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-SERVER-002 (Engine Construction) |
| **Blocks** | F-SERVER-004 (Hot Reload) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-server` |
| **Module** | `crates/laminar-server/src/http.rs` |

## Summary

RESTful HTTP API built on axum for pipeline management, observability, and operational control of a running LaminarDB instance. Provides health checks, Prometheus-format metrics export, pipeline lifecycle management (list, pause, resume), manual checkpoint triggering, hot-reload endpoint, and state querying. The API is designed to be the primary operational interface for both human operators and automation tools (Kubernetes probes, CI/CD pipelines, monitoring systems). Authentication hooks are prepared for Phase 4 integration (F035) but not enforced in Phase 6c.

## Goals

- Provide a complete REST API for operating a LaminarDB instance
- Health endpoint suitable for Kubernetes liveness and readiness probes
- Prometheus-compatible metrics endpoint for monitoring integration
- Pipeline lifecycle management (list, inspect, pause, resume)
- Manual checkpoint and hot-reload triggers
- State querying for debugging and ad-hoc inspection
- Structured JSON error responses with consistent format
- CORS support for browser-based dashboard (Phase 5)
- Prepared authentication middleware hooks for Phase 4 (F035)
- Rate limiting to prevent API abuse

## Non-Goals

- WebSocket streaming API (future feature)
- SQL console via HTTP (covered by Phase 5 F049)
- Full admin dashboard (covered by Phase 5 F047)
- mTLS termination (handled by load balancer or sidecar)
- GraphQL API (REST only for v1)

## Technical Design

### Architecture

The HTTP layer is a thin wrapper around the `LaminarEngine`. Each endpoint handler calls engine methods and serializes the response to JSON. The axum router is constructed once at startup and shared across all connections via `Arc<AppState>`.

```
Client (curl / dashboard / k8s probe)
    |
    v  HTTP
+----------------------------+
| axum Router                |
| +-----------+  +---------+ |
| | Middleware |  | Metrics | |
| | - Auth    |  | Counter | |
| | - CORS    |  +---------+ |
| | - Rate    |              |
| |   Limit   |              |
| +-----------+              |
|                            |
| +------------------------+ |
| | Route Handlers         | |
| | GET /health            | |
| | GET /metrics           | |
| | GET /api/v1/pipelines  | |
| | POST /api/v1/reload    | |
| | ...                    | |
| +------------------------+ |
|            |               |
+------------|---------------+
             v
      +---------------+
      | LaminarEngine |
      | (Arc<Engine>) |
      +---------------+
```

### API/Interface

```rust
use axum::{
    extract::{Path, State},
    http::StatusCode,
    middleware,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::cors::CorsLayer;

/// Shared application state for all handlers.
pub struct AppState {
    /// Reference to the running engine.
    pub engine: Arc<LaminarEngine>,
    /// Server configuration (for reload).
    pub config_path: std::path::PathBuf,
    /// Startup timestamp.
    pub started_at: chrono::DateTime<chrono::Utc>,
}

/// Build the axum router with all routes and middleware.
pub fn build_router(state: Arc<AppState>) -> Router {
    let api_v1 = Router::new()
        // Pipeline management
        .route("/pipelines", get(list_pipelines))
        .route("/pipelines/{name}", get(get_pipeline))
        .route("/pipelines/{name}/pause", post(pause_pipeline))
        .route("/pipelines/{name}/resume", post(resume_pipeline))
        // Operations
        .route("/checkpoint", post(trigger_checkpoint))
        .route("/reload", post(hot_reload))
        // State inspection
        .route("/state/{pipeline}/{key}", get(query_state));

    Router::new()
        // Health and metrics (no /api/v1 prefix)
        .route("/health", get(health_check))
        .route("/metrics", get(prometheus_metrics))
        // API v1
        .nest("/api/v1", api_v1)
        // Middleware
        .layer(CorsLayer::permissive()) // Tighten in production
        .layer(middleware::from_fn(request_logging))
        // Auth middleware hook (no-op in Phase 6c, wired in Phase 4)
        // .layer(middleware::from_fn_with_state(state.clone(), auth_middleware))
        .with_state(state)
}

/// Start the HTTP server on the given bind address.
///
/// Returns a JoinHandle that completes when the server shuts down.
pub async fn serve(
    router: Router,
    bind: &str,
) -> Result<tokio::task::JoinHandle<()>, ServerError> {
    let listener = tokio::net::TcpListener::bind(bind).await
        .map_err(|e| ServerError::Runtime(e.into()))?;

    tracing::info!(bind = %bind, "HTTP API listening");

    let handle = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("HTTP server failed");
    });

    Ok(handle)
}

// ─── Health ──────────────────────────────────────────────────────────

/// GET /health
///
/// Returns node health status suitable for Kubernetes probes.
/// Returns 200 if the node is operational, 503 if degraded.
async fn health_check(
    State(state): State<Arc<AppState>>,
) -> Result<Json<HealthResponse>, ApiError> {
    let status = state.engine.health_status().await;
    let response = HealthResponse {
        status: status.overall.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: (chrono::Utc::now() - state.started_at).num_seconds() as u64,
        pipelines_active: status.active_pipelines,
        pipelines_total: status.total_pipelines,
        node_id: status.node_id.clone(),
    };

    if status.overall == "healthy" {
        Ok(Json(response))
    } else {
        Err(ApiError::ServiceUnavailable(response))
    }
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_seconds: u64,
    pub pipelines_active: usize,
    pub pipelines_total: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
}

// ─── Metrics ─────────────────────────────────────────────────────────

/// GET /metrics
///
/// Returns metrics in Prometheus exposition format.
/// Compatible with Prometheus scraping and Grafana dashboards.
async fn prometheus_metrics(
    State(state): State<Arc<AppState>>,
) -> Result<String, ApiError> {
    let metrics = state.engine.export_prometheus_metrics().await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(metrics)
}

// ─── Pipelines ───────────────────────────────────────────────────────

/// GET /api/v1/pipelines
///
/// List all pipelines with their current status.
async fn list_pipelines(
    State(state): State<Arc<AppState>>,
) -> Result<Json<PipelineListResponse>, ApiError> {
    let pipelines = state.engine.list_pipelines().await
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    let items: Vec<PipelineSummary> = pipelines
        .into_iter()
        .map(|p| PipelineSummary {
            name: p.name,
            status: p.status,
            events_processed: p.events_processed,
            events_per_second: p.events_per_second,
            last_checkpoint: p.last_checkpoint,
        })
        .collect();

    Ok(Json(PipelineListResponse { pipelines: items }))
}

#[derive(Debug, Serialize)]
pub struct PipelineListResponse {
    pub pipelines: Vec<PipelineSummary>,
}

#[derive(Debug, Serialize)]
pub struct PipelineSummary {
    pub name: String,
    pub status: String,
    pub events_processed: u64,
    pub events_per_second: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_checkpoint: Option<String>,
}

/// GET /api/v1/pipelines/{name}
///
/// Get detailed information about a specific pipeline.
async fn get_pipeline(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<PipelineDetail>, ApiError> {
    let pipeline = state.engine.get_pipeline(&name).await
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .ok_or_else(|| ApiError::NotFound(format!("pipeline '{}' not found", name)))?;

    Ok(Json(PipelineDetail {
        name: pipeline.name,
        status: pipeline.status,
        sql: pipeline.sql,
        parallelism: pipeline.parallelism,
        events_processed: pipeline.events_processed,
        events_per_second: pipeline.events_per_second,
        last_checkpoint: pipeline.last_checkpoint,
        sources: pipeline.source_names,
        sinks: pipeline.sink_names,
        state_size_bytes: pipeline.state_size_bytes,
        started_at: pipeline.started_at,
    }))
}

#[derive(Debug, Serialize)]
pub struct PipelineDetail {
    pub name: String,
    pub status: String,
    pub sql: String,
    pub parallelism: usize,
    pub events_processed: u64,
    pub events_per_second: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_checkpoint: Option<String>,
    pub sources: Vec<String>,
    pub sinks: Vec<String>,
    pub state_size_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
}

/// POST /api/v1/pipelines/{name}/pause
///
/// Pause a running pipeline. Events accumulate in source buffers
/// and are processed when resumed.
async fn pause_pipeline(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<StatusMessage>, ApiError> {
    state.engine.pause_pipeline(&name).await
        .map_err(|e| match e {
            EngineError::PipelineNotFound(_) => {
                ApiError::NotFound(format!("pipeline '{}' not found", name))
            }
            other => ApiError::Internal(other.to_string()),
        })?;

    Ok(Json(StatusMessage {
        message: format!("pipeline '{}' paused", name),
    }))
}

/// POST /api/v1/pipelines/{name}/resume
///
/// Resume a paused pipeline. Processing continues from where it stopped.
async fn resume_pipeline(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<StatusMessage>, ApiError> {
    state.engine.resume_pipeline(&name).await
        .map_err(|e| match e {
            EngineError::PipelineNotFound(_) => {
                ApiError::NotFound(format!("pipeline '{}' not found", name))
            }
            other => ApiError::Internal(other.to_string()),
        })?;

    Ok(Json(StatusMessage {
        message: format!("pipeline '{}' resumed", name),
    }))
}

// ─── Operations ──────────────────────────────────────────────────────

/// POST /api/v1/checkpoint
///
/// Trigger an immediate manual checkpoint across all pipelines.
async fn trigger_checkpoint(
    State(state): State<Arc<AppState>>,
) -> Result<Json<CheckpointResponse>, ApiError> {
    let checkpoint_id = state.engine.checkpoint().await
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(Json(CheckpointResponse {
        message: "checkpoint initiated".to_string(),
        checkpoint_id,
    }))
}

#[derive(Debug, Serialize)]
pub struct CheckpointResponse {
    pub message: String,
    pub checkpoint_id: String,
}

/// POST /api/v1/reload
///
/// Hot-reload configuration from disk.
/// See F-SERVER-004 for details on diff-based reload.
async fn hot_reload(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ReloadResponse>, ApiError> {
    let result = state.engine.reload_config(&state.config_path).await
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(Json(ReloadResponse {
        message: "reload complete".to_string(),
        added: result.added,
        removed: result.removed,
        modified: result.modified,
        unchanged: result.unchanged,
    }))
}

#[derive(Debug, Serialize)]
pub struct ReloadResponse {
    pub message: String,
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub modified: Vec<String>,
    pub unchanged: Vec<String>,
}

// ─── State Query ─────────────────────────────────────────────────────

/// GET /api/v1/state/{pipeline}/{key}
///
/// Query operator state for debugging. Returns the raw value
/// associated with a key in the pipeline's state store.
async fn query_state(
    State(state): State<Arc<AppState>>,
    Path((pipeline, key)): Path<(String, String)>,
) -> Result<Json<StateQueryResponse>, ApiError> {
    let value = state.engine.query_state(&pipeline, key.as_bytes()).await
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    match value {
        Some(bytes) => Ok(Json(StateQueryResponse {
            pipeline,
            key,
            value: Some(String::from_utf8_lossy(&bytes).to_string()),
            found: true,
        })),
        None => Ok(Json(StateQueryResponse {
            pipeline,
            key,
            value: None,
            found: false,
        })),
    }
}

#[derive(Debug, Serialize)]
pub struct StateQueryResponse {
    pub pipeline: String,
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    pub found: bool,
}

// ─── Error Handling ──────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct StatusMessage {
    pub message: String,
}

/// Standard JSON error response format.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

/// API error type with HTTP status code mapping.
#[derive(Debug)]
pub enum ApiError {
    /// 400 Bad Request
    BadRequest(String),
    /// 401 Unauthorized
    Unauthorized(String),
    /// 404 Not Found
    NotFound(String),
    /// 429 Too Many Requests
    RateLimited,
    /// 500 Internal Server Error
    Internal(String),
    /// 503 Service Unavailable
    ServiceUnavailable(HealthResponse),
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_response) = match self {
            ApiError::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                ErrorResponse { error: msg, code: 400, details: None },
            ),
            ApiError::Unauthorized(msg) => (
                StatusCode::UNAUTHORIZED,
                ErrorResponse { error: msg, code: 401, details: None },
            ),
            ApiError::NotFound(msg) => (
                StatusCode::NOT_FOUND,
                ErrorResponse { error: msg, code: 404, details: None },
            ),
            ApiError::RateLimited => (
                StatusCode::TOO_MANY_REQUESTS,
                ErrorResponse {
                    error: "rate limit exceeded".to_string(),
                    code: 429,
                    details: Some("retry after 1 second".to_string()),
                },
            ),
            ApiError::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorResponse { error: msg, code: 500, details: None },
            ),
            ApiError::ServiceUnavailable(health) => (
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorResponse {
                    error: format!("service unavailable: {}", health.status),
                    code: 503,
                    details: None,
                },
            ),
        };

        (status, Json(error_response)).into_response()
    }
}

/// Request logging middleware.
async fn request_logging(
    req: axum::extract::Request,
    next: middleware::Next,
) -> axum::response::Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let start = std::time::Instant::now();

    let response = next.run(req).await;

    let duration = start.elapsed();
    let status = response.status();

    tracing::info!(
        method = %method,
        uri = %uri,
        status = %status.as_u16(),
        duration_ms = %duration.as_millis(),
        "HTTP request"
    );

    response
}
```

### Data Structures

All request and response types are defined inline above. The key patterns:

- All responses are JSON-serialized via `serde::Serialize`
- Error responses use a consistent `ErrorResponse` format
- Optional fields use `#[serde(skip_serializing_if = "Option::is_none")]`
- The `ApiError` enum maps to HTTP status codes via `IntoResponse`

### Algorithm/Flow

#### Request Processing Flow

```
1. HTTP request arrives at axum listener
2. Middleware stack executes:
   a. CORS headers applied (permissive in dev, restrictive in prod)
   b. Request logging records method, URI, start time
   c. Rate limiting checks client IP (if enabled)
   d. Auth middleware validates token (Phase 4, no-op in Phase 6c)
3. Router matches path to handler
4. Handler extracts path parameters and state
5. Handler calls engine method
6. Result serialized to JSON
7. Response sent with appropriate status code
8. Request logging records duration and status
```

### Error Handling

| Error | HTTP Status | Response |
|-------|-------------|----------|
| Pipeline not found | 404 | `{"error": "pipeline 'foo' not found", "code": 404}` |
| Invalid request body | 400 | `{"error": "invalid JSON: ...", "code": 400}` |
| Missing auth token | 401 | `{"error": "authentication required", "code": 401}` |
| Rate limit exceeded | 429 | `{"error": "rate limit exceeded", "code": 429, "details": "retry after 1 second"}` |
| Engine internal error | 500 | `{"error": "checkpoint failed: ...", "code": 500}` |
| Node unhealthy | 503 | `{"error": "service unavailable: degraded", "code": 503}` |

## Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| /health response time | < 1ms | Load test |
| /metrics response time | < 10ms | Load test with 100 pipelines |
| /api/v1/pipelines response time | < 5ms | Load test |
| /api/v1/pipelines/{name} response time | < 2ms | Load test |
| Concurrent connections | 1000+ | Load test with wrk |
| Request logging overhead | < 50us | Benchmark |
| JSON serialization overhead | < 100us | Benchmark per response |

## Test Plan

### Unit Tests

- [ ] `test_health_check_returns_200_when_healthy` -- Healthy engine returns 200
- [ ] `test_health_check_returns_503_when_degraded` -- Degraded engine returns 503
- [ ] `test_list_pipelines_empty` -- No pipelines returns empty array
- [ ] `test_list_pipelines_with_data` -- Multiple pipelines returned with status
- [ ] `test_get_pipeline_found` -- Existing pipeline returns detail
- [ ] `test_get_pipeline_not_found` -- Missing pipeline returns 404
- [ ] `test_pause_pipeline_success` -- Running pipeline paused
- [ ] `test_pause_pipeline_not_found` -- Missing pipeline returns 404
- [ ] `test_resume_pipeline_success` -- Paused pipeline resumed
- [ ] `test_trigger_checkpoint_success` -- Checkpoint initiated, ID returned
- [ ] `test_hot_reload_success` -- Config reloaded, diff returned
- [ ] `test_query_state_found` -- Key exists, value returned
- [ ] `test_query_state_not_found` -- Key missing, found=false
- [ ] `test_error_response_format` -- All errors produce consistent JSON
- [ ] `test_cors_headers_present` -- CORS headers included in response
- [ ] `test_prometheus_metrics_format` -- Metrics output parseable by Prometheus

### Integration Tests

- [ ] `test_http_api_end_to_end` -- Start server, hit all endpoints, verify responses
- [ ] `test_concurrent_requests` -- 100 concurrent requests, no errors
- [ ] `test_api_with_running_pipeline` -- Start pipeline, query via API, verify status
- [ ] `test_pause_resume_via_api` -- Full lifecycle via HTTP calls
- [ ] `test_checkpoint_via_api` -- Trigger checkpoint via POST, verify completion

### Benchmarks

- [ ] `bench_health_endpoint` -- Target: < 1ms p99
- [ ] `bench_pipeline_list` -- Target: < 5ms with 50 pipelines
- [ ] `bench_concurrent_connections` -- Target: 1000+ concurrent with < 10ms p99

## Rollout Plan

1. **Phase 1**: Define response types and error format
2. **Phase 2**: Implement /health and /metrics endpoints
3. **Phase 3**: Implement pipeline management endpoints
4. **Phase 4**: Implement checkpoint and reload endpoints
5. **Phase 5**: Implement state query endpoint
6. **Phase 6**: Add middleware (logging, CORS, rate limiting)
7. **Phase 7**: Prepare auth middleware hooks (no-op implementation)
8. **Phase 8**: Integration tests and load testing
9. **Phase 9**: Code review and merge

## Open Questions

- [ ] Should /metrics be on a separate port (common for Prometheus setups) or same port?
- [ ] Should state query support range queries (GET /state/{pipeline}?from=X&to=Y)?
- [ ] Should the API version be in the URL path (/api/v1/) or Accept header?
- [ ] Should pipeline pause/resume be idempotent (pausing an already-paused pipeline returns 200)?
- [ ] Should we add a WebSocket endpoint for real-time pipeline event streaming?

## Completion Checklist

- [ ] All endpoints implemented and tested
- [ ] Error response format consistent across all endpoints
- [ ] CORS configuration working
- [ ] Request logging middleware operational
- [ ] Auth middleware hooks prepared (no-op)
- [ ] Rate limiting implemented (configurable)
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Load test validates performance targets
- [ ] Documentation updated (`#![deny(missing_docs)]`)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [axum documentation](https://docs.rs/axum/latest/axum/) -- HTTP framework
- [tower-http](https://docs.rs/tower-http/latest/tower_http/) -- Middleware (CORS, tracing)
- [F-SERVER-002: Engine Construction](F-SERVER-002-engine-construction.md) -- Engine API
- [F-SERVER-004: Hot Reload](F-SERVER-004-hot-reload.md) -- Reload endpoint implementation
- [F035: Authentication Framework](../../phase-4/F035-authn-framework.md) -- Auth middleware integration
- [F050: Prometheus Export](../../phase-5/F050-prometheus-export.md) -- Metrics format
- [F052: Health Checks](../../phase-5/F052-health-checks.md) -- Health check design
