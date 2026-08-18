//! Checkpoint endpoints: manual trigger with optional forwarded budget, and
//! latest checkpoint metadata (`/api/v1/cluster/checkpoints`, both modes).

use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

use super::error_response;
use super::json_encoding::batches_to_json_raw;
use super::state::AppState;

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

pub(super) async fn trigger_checkpoint(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let forwarded_budget = match headers.get("x-laminar-checkpoint-budget-nanos") {
        None => None,
        Some(value) => {
            let parsed = value
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|budget| *budget != 0);
            let Some(budget) = parsed else {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid forwarded checkpoint budget",
                )
                .into_response();
            };
            Some(Duration::from_nanos(budget))
        }
    };
    let result = match forwarded_budget {
        Some(timeout) => state.db.checkpoint_forwarded_with_timeout(timeout).await,
        None => state.db.checkpoint().await,
    };
    match result {
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

/// `GET /api/v1/cluster/checkpoints` — latest checkpoint metadata. Available
/// in both single-node and cluster mode.
pub(super) async fn cluster_checkpoints(State(state): State<Arc<AppState>>) -> impl IntoResponse {
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
