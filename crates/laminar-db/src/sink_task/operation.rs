//! Connector-operation await and deadline infrastructure shared by every
//! sink command path: bounded execution, process-authority fencing (cluster),
//! and the typed deadline/protocol errors.

use std::time::Duration;

#[cfg(feature = "cluster")]
use std::sync::Arc;

use laminar_connectors::connector::ConnectorCancellationPolicy;
use laminar_connectors::error::ConnectorError;
#[cfg(feature = "cluster")]
use laminar_core::cluster::control::ClusterController;
use tokio::time::Instant;

use super::protocol::SINK_CLOSE_TIMEOUT;

pub(super) enum ConnectorOperationOutcome<T> {
    Completed(T),
    Deadline,
    #[cfg(feature = "cluster")]
    ProcessAuthorityLost,
}

pub(super) async fn await_connector_operation_local<T>(
    deadline: Instant,
    future: impl std::future::Future<Output = T>,
) -> ConnectorOperationOutcome<T> {
    if Instant::now() >= deadline {
        return ConnectorOperationOutcome::Deadline;
    }
    match tokio::time::timeout_at(deadline, future).await {
        Ok(_) if Instant::now() >= deadline => ConnectorOperationOutcome::Deadline,
        Ok(result) => ConnectorOperationOutcome::Completed(result),
        Err(_) => ConnectorOperationOutcome::Deadline,
    }
}

#[cfg(feature = "cluster")]
pub(super) async fn await_connector_operation_fenced<T>(
    controller: &ClusterController,
    deadline: Instant,
    future: impl std::future::Future<Output = T>,
) -> ConnectorOperationOutcome<T> {
    tokio::pin!(future);

    tokio::select! {
        biased;
        () = controller.wait_for_process_lease_loss() => {
            ConnectorOperationOutcome::ProcessAuthorityLost
        }
        () = tokio::time::sleep_until(deadline) => {
            ConnectorOperationOutcome::Deadline
        }
        result = &mut future => {
            if !controller.process_lease_is_live() {
                ConnectorOperationOutcome::ProcessAuthorityLost
            } else if Instant::now() >= deadline {
                ConnectorOperationOutcome::Deadline
            } else {
                ConnectorOperationOutcome::Completed(result)
            }
        }
    }
}

#[cfg(feature = "cluster")]
pub(super) async fn await_connector_operation<T, F, Fut>(
    deadline: Instant,
    process_authority: Option<Arc<ClusterController>>,
    make_future: F,
) -> ConnectorOperationOutcome<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let Some(controller) = process_authority else {
        return await_connector_operation_local(deadline, make_future()).await;
    };
    if !controller.process_lease_is_live() {
        return ConnectorOperationOutcome::ProcessAuthorityLost;
    }
    await_connector_operation_fenced(controller.as_ref(), deadline, make_future()).await
}

#[cfg(not(feature = "cluster"))]
pub(super) async fn await_connector_operation<T, F, Fut>(
    deadline: Instant,
    make_future: F,
) -> ConnectorOperationOutcome<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    await_connector_operation_local(deadline, make_future()).await
}

pub(super) async fn bounded_connector_operation<T, F, Fut>(
    sink_name: &str,
    operation: &str,
    deadline: Instant,
    cancellation_policy: ConnectorCancellationPolicy,
    #[cfg(feature = "cluster")] process_authority: Option<Arc<ClusterController>>,
    make_future: F,
) -> (Result<T, ConnectorError>, bool)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, ConnectorError>>,
{
    match await_connector_operation(
        deadline,
        #[cfg(feature = "cluster")]
        process_authority,
        make_future,
    )
    .await
    {
        ConnectorOperationOutcome::Completed(result) => {
            let retire = result
                .as_ref()
                .err()
                .is_some_and(ConnectorError::is_outcome_unknown);
            (result, retire)
        }
        ConnectorOperationOutcome::Deadline => (
            Err(protocol_deadline_error(sink_name, operation)),
            cancellation_policy == ConnectorCancellationPolicy::RetireConnector,
        ),
        #[cfg(feature = "cluster")]
        ConnectorOperationOutcome::ProcessAuthorityLost => {
            (Err(process_authority_error(sink_name, operation)), true)
        }
    }
}

#[cfg(feature = "cluster")]
pub(super) fn process_authority_error(sink_name: &str, operation: &str) -> ConnectorError {
    ConnectorError::InvalidState {
        expected: "live cluster process lease".into(),
        actual: format!("sink '{sink_name}' lost process authority before {operation}"),
    }
}

pub(super) fn protocol_deadline_error(sink_name: &str, operation: &str) -> ConnectorError {
    ConnectorError::Internal(format!(
        "sink '{sink_name}' {operation} exceeded its end-to-end deadline"
    ))
}

pub(super) fn operation_deadline(timeout: Duration) -> Instant {
    Instant::now() + timeout
}

pub(super) fn close_deadline_error(sink_name: &str, phase: &str) -> ConnectorError {
    ConnectorError::Internal(format!(
        "sink task '{sink_name}' close {phase} exceeded its {SINK_CLOSE_TIMEOUT:?} end-to-end \
         deadline"
    ))
}

pub(super) fn command_deadline_error(
    sink_name: &str,
    operation: &str,
    timeout: Duration,
) -> ConnectorError {
    ConnectorError::Internal(format!(
        "sink task '{sink_name}' {operation} exceeded its {timeout:?} end-to-end deadline"
    ))
}
