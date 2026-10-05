//! Bounded, authenticated topology status and effect-free candidate validation.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

use super::cluster_admin::CLUSTER_DISABLED_MSG;
use super::{error_response, AppState};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(not(feature = "cluster"), allow(dead_code))]
pub(super) struct TopologyValidationRequest {
    expected_parent_version: u64,
    statements: Vec<String>,
}

/// Local console-authorized preparation of an immutable, already admitted request. Every
/// required participant is called separately; forwarding would incorrectly certify the leader.
pub(super) async fn prepare_cluster_topology(
    State(state): State<Arc<AppState>>,
    Path(operation_id): Path<String>,
) -> Response {
    #[cfg(not(feature = "cluster"))]
    let response = {
        let _ = (state, operation_id);
        error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
    };
    #[cfg(feature = "cluster")]
    let response = async {
        use laminar_core::cluster::control::{TopologyError, TopologyOperationId};
        if state.cluster.is_none() {
            return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
        }
        let Some(identity) = uuid::Uuid::parse_str(&operation_id)
            .ok()
            .and_then(|uuid| TopologyOperationId::try_from(uuid).ok())
        else {
            return error_response(
                StatusCode::BAD_REQUEST,
                TopologyError::Invalid("operation identity must be a nonzero UUID".into())
                    .to_string(),
            )
            .into_response();
        };
        let result = state.db.prepare_cluster_topology_operation(identity).await;
        if let Some(reason) = state.serving_rejection() {
            return error_response(StatusCode::SERVICE_UNAVAILABLE, reason).into_response();
        }
        match result {
            Ok(status) => Json(status).into_response(),
            Err(error) => {
                let status = match &error {
                    laminar_db::DbError::Topology(TopologyError::Conflict(_)) => {
                        StatusCode::CONFLICT
                    }
                    laminar_db::DbError::Topology(TopologyError::Invalid(_)) => {
                        StatusCode::BAD_REQUEST
                    }
                    laminar_db::DbError::Topology(
                        TopologyError::Protocol(_) | TopologyError::Unsupported(_),
                    ) => StatusCode::UNPROCESSABLE_ENTITY,
                    laminar_db::DbError::Topology(TopologyError::PlanningBusy) => {
                        StatusCode::TOO_MANY_REQUESTS
                    }
                    laminar_db::DbError::Topology(
                        TopologyError::Contended
                        | TopologyError::ReadTimedOut
                        | TopologyError::PlanningTimedOut,
                    ) => StatusCode::GATEWAY_TIMEOUT,
                    _ => StatusCode::SERVICE_UNAVAILABLE,
                };
                error_response(status, error.to_string()).into_response()
            }
        }
    }
    .await;
    let mut response = response;
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

/// This local validation route follows the same console authorization/startup gates as status.
/// No leader forwarding is needed: the result explicitly certifies this process's local plan only.
pub(super) async fn validate_cluster_topology(
    State(state): State<Arc<AppState>>,
    request: Result<Json<TopologyValidationRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    #[cfg(not(feature = "cluster"))]
    let response = {
        let _ = (state, request);
        error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
    };
    #[cfg(feature = "cluster")]
    let response = {
        use laminar_core::cluster::control::{TopologyError, TopologyVersion};

        let validation = async {
            if state.cluster.is_none() {
                return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
            }
            let request = match request {
                Ok(Json(request)) => request,
                Err(error) => {
                    return error_response(error.status(), error.body_text()).into_response()
                }
            };
            let expected_parent = match TopologyVersion::new(request.expected_parent_version) {
                Ok(version) => version,
                Err(error) => {
                    return error_response(StatusCode::BAD_REQUEST, error.to_string())
                        .into_response()
                }
            };
            let result = state
                .db
                .validate_cluster_topology_change(expected_parent, &request.statements)
                .await;
            if let Some(reason) = state.serving_rejection() {
                return error_response(StatusCode::SERVICE_UNAVAILABLE, reason).into_response();
            }
            match result {
                Ok(validation) => Json(validation).into_response(),
                Err(error) => {
                    let status = match &error {
                        laminar_db::DbError::Topology(TopologyError::Conflict(_)) => {
                            StatusCode::CONFLICT
                        }
                        laminar_db::DbError::Topology(TopologyError::Invalid(_))
                        | laminar_db::DbError::SqlParse(_)
                        | laminar_db::DbError::Sql(_) => StatusCode::BAD_REQUEST,
                        laminar_db::DbError::Topology(
                            TopologyError::Unsupported(_) | TopologyError::Protocol(_),
                        ) => StatusCode::UNPROCESSABLE_ENTITY,
                        laminar_db::DbError::Topology(TopologyError::PlanningBusy) => {
                            StatusCode::TOO_MANY_REQUESTS
                        }
                        laminar_db::DbError::Topology(
                            TopologyError::PlanningTimedOut | TopologyError::ReadTimedOut,
                        ) => StatusCode::GATEWAY_TIMEOUT,
                        laminar_db::DbError::Topology(_) | laminar_db::DbError::Shutdown => {
                            StatusCode::SERVICE_UNAVAILABLE
                        }
                        _ => StatusCode::UNPROCESSABLE_ENTITY,
                    };
                    error_response(status, error.to_string()).into_response()
                }
            }
        };
        validation.await
    };
    let mut response = response;
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    if response.status() == StatusCode::TOO_MANY_REQUESTS {
        response.headers_mut().insert(
            axum::http::header::RETRY_AFTER,
            axum::http::HeaderValue::from_static("1"),
        );
    }
    response
}

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

pub(super) async fn cluster_topology_operation(
    State(state): State<Arc<AppState>>,
    Path(operation_id): Path<String>,
) -> Response {
    #[cfg(not(feature = "cluster"))]
    {
        let _ = (state, operation_id);
        error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
    }
    #[cfg(feature = "cluster")]
    {
        use laminar_core::cluster::control::{TopologyError, TopologyOperationId};
        if state.cluster.is_none() {
            return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
        }
        let identity = uuid::Uuid::parse_str(&operation_id)
            .ok()
            .and_then(|uuid| TopologyOperationId::try_from(uuid).ok());
        let Some(identity) = identity else {
            return error_response(
                StatusCode::BAD_REQUEST,
                TopologyError::Invalid("operation identity must be a nonzero UUID".into())
                    .to_string(),
            )
            .into_response();
        };
        let status = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            state.db.cluster_topology_operation_status(identity),
        )
        .await;
        if let Some(reason) = state.serving_rejection() {
            return error_response(StatusCode::SERVICE_UNAVAILABLE, reason).into_response();
        }
        match status {
            Ok(Ok(Some(status))) => ([("cache-control", "no-store")], Json(status)).into_response(),
            Ok(Ok(None)) => {
                let mut response = error_response(
                    StatusCode::NOT_FOUND,
                    TopologyError::Conflict("unknown topology operation".into()).to_string(),
                )
                .into_response();
                response.headers_mut().insert(
                    axum::http::header::CACHE_CONTROL,
                    axum::http::HeaderValue::from_static("no-store"),
                );
                response
            }
            Ok(Err(error @ laminar_db::DbError::Topology(TopologyError::ReadTimedOut))) => {
                error_response(StatusCode::GATEWAY_TIMEOUT, error.to_string()).into_response()
            }
            Ok(Err(error)) => {
                error_response(StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response()
            }
            Err(_) => error_response(
                StatusCode::GATEWAY_TIMEOUT,
                TopologyError::ReadTimedOut.to_string(),
            )
            .into_response(),
        }
    }
}
