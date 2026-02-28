//! Admin REST API endpoints for checkpoint management.
//!
//! Provides HTTP endpoints to list, inspect, and trigger checkpoints.
//!
//! # Endpoints
//!
//! | Method | Path | Description |
//! |--------|------|-------------|
//! | `GET` | `/api/v1/checkpoints` | List all checkpoints |
//! | `GET` | `/api/v1/checkpoints/latest` | Get latest checkpoint |
//! | `GET` | `/api/v1/checkpoints/stats` | Get checkpoint statistics |
//! | `GET` | `/api/v1/checkpoints/:id` | Get checkpoint by ID |
//! | `POST` | `/api/v1/checkpoints/trigger` | Trigger a manual checkpoint |

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;

use laminar_db::LaminarDB;
use laminar_storage::checkpoint_store::CheckpointStore;

/// Application state shared across all handlers.
pub type AppState = Arc<LaminarDB>;

/// Creates the checkpoint REST API router.
///
/// All routes are nested under `/api/v1/checkpoints`.
///
/// # Arguments
///
/// * `db` - Shared reference to the `LaminarDB` instance.
pub fn checkpoint_router(db: AppState) -> Router {
    Router::new()
        .route("/api/v1/checkpoints", get(list_checkpoints))
        .route("/api/v1/checkpoints/latest", get(get_latest_checkpoint))
        .route("/api/v1/checkpoints/stats", get(get_checkpoint_stats))
        .route("/api/v1/checkpoints/trigger", post(trigger_checkpoint))
        .route("/api/v1/checkpoints/{id}", get(get_checkpoint_by_id))
        .with_state(db)
}

/// Summary of a checkpoint for list responses.
#[derive(Debug, Serialize)]
struct CheckpointSummary {
    checkpoint_id: u64,
    epoch: u64,
}

/// Response for the trigger endpoint.
#[derive(Debug, Serialize)]
struct TriggerResponse {
    success: bool,
    checkpoint_id: u64,
    epoch: u64,
    duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Error response body.
#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

fn error_response(status: StatusCode, msg: impl Into<String>) -> impl IntoResponse {
    (status, Json(ErrorResponse { error: msg.into() }))
}

/// `GET /api/v1/checkpoints` — list all checkpoints.
async fn list_checkpoints(State(db): State<AppState>) -> impl IntoResponse {
    let Some(store) = db.checkpoint_store() else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "checkpointing is not configured",
        )
        .into_response();
    };

    match store.list() {
        Ok(items) => {
            let summaries: Vec<CheckpointSummary> = items
                .into_iter()
                .map(|(checkpoint_id, epoch)| CheckpointSummary {
                    checkpoint_id,
                    epoch,
                })
                .collect();
            Json(summaries).into_response()
        }
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// `GET /api/v1/checkpoints/latest` — get the latest checkpoint manifest.
async fn get_latest_checkpoint(State(db): State<AppState>) -> impl IntoResponse {
    let Some(store) = db.checkpoint_store() else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "checkpointing is not configured",
        )
        .into_response();
    };

    match store.load_latest() {
        Ok(Some(manifest)) => Json(manifest).into_response(),
        Ok(None) => error_response(StatusCode::NOT_FOUND, "no checkpoints exist").into_response(),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// `GET /api/v1/checkpoints/:id` — get a checkpoint by ID.
async fn get_checkpoint_by_id(
    State(db): State<AppState>,
    Path(id): Path<u64>,
) -> impl IntoResponse {
    let Some(store) = db.checkpoint_store() else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "checkpointing is not configured",
        )
        .into_response();
    };

    match store.load_by_id(id) {
        Ok(Some(manifest)) => Json(manifest).into_response(),
        Ok(None) => error_response(StatusCode::NOT_FOUND, format!("checkpoint {id} not found"))
            .into_response(),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Stats response wrapping coordinator stats plus store-level counts.
#[derive(Debug, Serialize)]
struct StatsResponse {
    enabled: bool,
    total_checkpoints: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    coordinator: Option<laminar_db::CheckpointStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latest: Option<LatestSummary>,
}

/// Minimal summary of the latest checkpoint for the stats endpoint.
#[derive(Debug, Serialize)]
struct LatestSummary {
    checkpoint_id: u64,
    epoch: u64,
    timestamp_ms: u64,
}

/// `GET /api/v1/checkpoints/stats` — checkpoint statistics.
async fn get_checkpoint_stats(State(db): State<AppState>) -> impl IntoResponse {
    if !db.is_checkpoint_enabled() {
        return Json(StatsResponse {
            enabled: false,
            total_checkpoints: 0,
            coordinator: None,
            latest: None,
        })
        .into_response();
    }

    let coordinator = db.checkpoint_stats().await;

    let store = db.checkpoint_store();
    let (total, latest) = match &store {
        Some(s) => {
            let list = s.list().unwrap_or_default();
            let total = list.len();
            let latest = s.load_latest().ok().flatten().map(|m| LatestSummary {
                checkpoint_id: m.checkpoint_id,
                epoch: m.epoch,
                timestamp_ms: m.timestamp_ms,
            });
            (total, latest)
        }
        None => (0, None),
    };

    Json(StatsResponse {
        enabled: true,
        total_checkpoints: total,
        coordinator,
        latest,
    })
    .into_response()
}

/// `POST /api/v1/checkpoints/trigger` — trigger a manual checkpoint.
async fn trigger_checkpoint(State(db): State<AppState>) -> impl IntoResponse {
    match db.checkpoint().await {
        Ok(result) => {
            let status = if result.success {
                StatusCode::OK
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            #[allow(clippy::cast_possible_truncation)] // checkpoint duration < 1 day
            let duration_ms = result.duration.as_millis() as u64;
            (
                status,
                Json(TriggerResponse {
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_db() -> AppState {
        Arc::new(laminar_db::LaminarDB::open().unwrap())
    }

    #[tokio::test]
    async fn test_list_checkpoints_no_config() {
        let db = test_db();
        let app = checkpoint_router(db);

        let req = Request::builder()
            .uri("/api/v1/checkpoints")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        // No checkpoint config → 503
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_stats_no_config() {
        let db = test_db();
        let app = checkpoint_router(db);

        let req = Request::builder()
            .uri("/api/v1/checkpoints/stats")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let stats: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(stats["enabled"], false);
    }

    #[tokio::test]
    async fn test_latest_no_config() {
        let db = test_db();
        let app = checkpoint_router(db);

        let req = Request::builder()
            .uri("/api/v1/checkpoints/latest")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_get_by_id_no_config() {
        let db = test_db();
        let app = checkpoint_router(db);

        let req = Request::builder()
            .uri("/api/v1/checkpoints/42")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_trigger_no_config() {
        let db = test_db();
        let app = checkpoint_router(db);

        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/checkpoints/trigger")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        // No checkpoint config → 500 from DbError
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
