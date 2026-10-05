//! Console-authorized atomic topology admission and explicit legacy adoption.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;

use super::cluster_admin::CLUSTER_DISABLED_MSG;
use super::{error_response, AppState};

#[cfg(feature = "cluster")]
pub(super) type SubmissionRequest = laminar_db::ClusterTopologyRequest;
#[cfg(not(feature = "cluster"))]
pub(super) type SubmissionRequest = serde_json::Value;
#[cfg(feature = "cluster")]
pub(super) type AdoptionRequest = laminar_db::ClusterTopologyAdoptionRequest;
#[cfg(not(feature = "cluster"))]
pub(super) type AdoptionRequest = serde_json::Value;

pub(super) async fn submit_cluster_topology(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    request: Result<Json<SubmissionRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    #[cfg(not(feature = "cluster"))]
    let response = {
        let _ = (state, headers, request);
        error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
    };
    #[cfg(feature = "cluster")]
    let response = async {
        if state.cluster.is_none() {
            return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
        }
        let request = match request {
            Ok(Json(request)) => request,
            Err(error) => return error_response(error.status(), error.body_text()).into_response(),
        };
        let budget = match forwarded_budget(&headers) {
            Ok(budget) => budget,
            Err(message) => {
                return error_response(StatusCode::BAD_REQUEST, message).into_response()
            }
        };
        let result = match budget {
            Some(remaining) => {
                Box::pin(
                    state
                        .db
                        .submit_cluster_topology_forwarded(&request, remaining),
                )
                .await
            }
            None => Box::pin(state.db.submit_cluster_topology_change(&request)).await,
        };
        let response = match result {
            Ok(operation) => (StatusCode::ACCEPTED, Json(operation)).into_response(),
            Err(error) => topology_write_error(error),
        };
        operation_header(response, request.operation_id)
    }
    .await;
    no_store(response)
}

pub(super) async fn adopt_cluster_topology(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    request: Result<Json<AdoptionRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    #[cfg(not(feature = "cluster"))]
    let response = {
        let _ = (state, headers, request);
        error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
    };
    #[cfg(feature = "cluster")]
    let response = async {
        if state.cluster.is_none() {
            return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
        }
        let request = match request {
            Ok(Json(request)) => request,
            Err(error) => return error_response(error.status(), error.body_text()).into_response(),
        };
        let budget = match forwarded_budget(&headers) {
            Ok(budget) => budget,
            Err(message) => {
                return error_response(StatusCode::BAD_REQUEST, message).into_response()
            }
        };
        let result = match budget {
            Some(remaining) => {
                state
                    .db
                    .adopt_cluster_topology_forwarded(&request, remaining)
                    .await
            }
            None => state.db.adopt_cluster_topology(&request).await,
        };
        let response = match result {
            Ok(baseline) => Json(baseline).into_response(),
            Err(error) => topology_write_error(error),
        };
        operation_header(response, request.operation_id)
    }
    .await;
    no_store(response)
}

#[cfg(feature = "cluster")]
fn forwarded_budget(headers: &HeaderMap) -> Result<Option<std::time::Duration>, &'static str> {
    let Some(value) = headers.get("x-laminar-topology-budget-nanos") else {
        return Ok(None);
    };
    value
        .to_str()
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| (1..=45_000_000_000).contains(value))
        .map(|value| Some(std::time::Duration::from_nanos(value)))
        .ok_or("invalid forwarded topology budget")
}

#[cfg(feature = "cluster")]
pub(super) fn topology_write_error(error: laminar_db::DbError) -> Response {
    #[derive(serde::Serialize)]
    struct ErrorBody {
        error: String,
        code: &'static str,
    }
    use laminar_core::cluster::control::TopologyError;
    if let laminar_db::DbError::TopologySubmission {
        operation_id,
        source,
    } = error
    {
        return operation_header(topology_write_error(*source), operation_id);
    }
    let status = match &error {
        laminar_db::DbError::Topology(TopologyError::Conflict(_)) => StatusCode::CONFLICT,
        laminar_db::DbError::Topology(TopologyError::Invalid(_))
        | laminar_db::DbError::Sql(_)
        | laminar_db::DbError::SqlParse(_) => StatusCode::BAD_REQUEST,
        laminar_db::DbError::Topology(
            TopologyError::Protocol(_) | TopologyError::Unsupported(_),
        ) => StatusCode::UNPROCESSABLE_ENTITY,
        laminar_db::DbError::Topology(TopologyError::PlanningBusy) => StatusCode::TOO_MANY_REQUESTS,
        laminar_db::DbError::Topology(
            TopologyError::Contended
            | TopologyError::PlanningTimedOut
            | TopologyError::ReadTimedOut,
        ) => StatusCode::GATEWAY_TIMEOUT,
        laminar_db::DbError::Topology(_) | laminar_db::DbError::Shutdown => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    };
    no_store(
        (
            status,
            Json(ErrorBody {
                code: error.code(),
                error: error.to_string(),
            }),
        )
            .into_response(),
    )
}

#[cfg(feature = "cluster")]
fn operation_header(
    mut response: Response,
    operation: laminar_core::cluster::control::TopologyOperationId,
) -> Response {
    if let Ok(value) = axum::http::HeaderValue::from_str(&operation.get().to_string()) {
        response
            .headers_mut()
            .insert("x-laminar-topology-operation-id", value);
    }
    response
}

fn no_store(mut response: Response) -> Response {
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    if matches!(
        response.status(),
        StatusCode::TOO_MANY_REQUESTS | StatusCode::GATEWAY_TIMEOUT
    ) {
        response.headers_mut().insert(
            axum::http::header::RETRY_AFTER,
            axum::http::HeaderValue::from_static("1"),
        );
    }
    response
}
