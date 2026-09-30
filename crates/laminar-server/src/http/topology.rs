//! Bounded, authenticated read-only durable topology status.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
#[cfg(feature = "cluster")]
use axum::Json;

use super::cluster_admin::CLUSTER_DISABLED_MSG;
use super::{error_response, AppState};

pub(super) async fn cluster_topology(State(state): State<Arc<AppState>>) -> Response {
    #[cfg(not(feature = "cluster"))]
    {
        let _ = state;
        error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
    }
    #[cfg(feature = "cluster")]
    {
        if state.cluster.is_none() {
            return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
        }
        let status = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            state.db.cluster_topology_status(),
        )
        .await;
        // Serving authority may be lost while the object-store read is outstanding.
        if let Some(reason) = state.serving_rejection() {
            return error_response(StatusCode::SERVICE_UNAVAILABLE, reason).into_response();
        }
        match status {
            Ok(Ok(status)) => ([("cache-control", "no-store")], Json(status)).into_response(),
            Ok(Err(
                error @ laminar_db::DbError::Topology(
                    laminar_core::cluster::control::TopologyError::ReadTimedOut,
                ),
            )) => error_response(StatusCode::GATEWAY_TIMEOUT, error.to_string()).into_response(),
            Ok(Err(error)) => {
                error_response(StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response()
            }
            Err(_) => error_response(
                StatusCode::GATEWAY_TIMEOUT,
                laminar_core::cluster::control::TopologyError::ReadTimedOut.to_string(),
            )
            .into_response(),
        }
    }
}
