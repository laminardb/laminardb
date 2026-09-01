//! Sink close driver: drives a close to its terminal state on a dedicated
//! future, with disconnected-sink completion and terminal-state waits.

use futures::FutureExt;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crossfire::{oneshot, SendTimeoutError};
use laminar_connectors::error::ConnectorError;
use tokio::time::Instant;

use super::actor::{OwnedSinkTask, SinkActorState, SinkCloseOutcome, SinkCloseState};
use super::operation::close_deadline_error;
use super::protocol::{SinkCommand, SinkCommandTx, SinkOperation};

pub(super) fn spawn_sink_close_driver(
    name: Arc<str>,
    tx: SinkCommandTx,
    task: OwnedSinkTask,
    state: Arc<SinkCloseState>,
    actor_state: Arc<SinkActorState>,
    deadline: Instant,
    runtime: &tokio::runtime::Handle,
) {
    let close = drive_sink_close(
        Arc::clone(&name),
        tx,
        task,
        Arc::clone(&state),
        actor_state,
        deadline,
    );
    spawn_sink_close_driver_future(name, state, close, runtime);
}

pub(super) fn spawn_sink_close_driver_future<F>(
    name: Arc<str>,
    state: Arc<SinkCloseState>,
    close: F,
    runtime: &tokio::runtime::Handle,
) where
    F: std::future::Future<Output = SinkCloseOutcome> + Send + 'static,
{
    let supervisor = runtime.spawn(async move {
        if let Ok(outcome) = std::panic::AssertUnwindSafe(close).catch_unwind().await {
            state.finish(outcome);
        } else {
            state.set_phase("terminal driver panic");
            state.finish(SinkCloseOutcome::Failure(Arc::from(format!(
                "sink task '{name}' terminal close driver panicked"
            ))));
            tracing::error!(sink = %name, "sink terminal close driver panicked");
        }
    });
    drop(supervisor); // detached by design; shared state and the DB registry retain ownership
}

pub(super) async fn wait_for_sink_close(
    name: &str,
    state: Arc<SinkCloseState>,
    deadline: Instant,
) -> Result<(), ConnectorError> {
    loop {
        let notified = state.notify.notified();
        tokio::pin!(notified);
        // Register before inspecting the outcome so `notify_waiters` cannot land in the gap
        // between the check and the first poll of `Notified`.
        notified.as_mut().enable();
        if let Some(outcome) = state.outcome() {
            return outcome.into_result();
        }
        if tokio::time::timeout_at(deadline, notified.as_mut())
            .await
            .is_err()
        {
            // Deadline and completion can become ready in the same scheduler turn.
            return state.outcome().map_or_else(
                || Err(close_deadline_error(name, state.phase())),
                SinkCloseOutcome::into_result,
            );
        }
    }
}

pub(super) async fn drive_sink_close(
    name: Arc<str>,
    tx: SinkCommandTx,
    mut task: OwnedSinkTask,
    state: Arc<SinkCloseState>,
    actor_state: Arc<SinkActorState>,
    deadline: Instant,
) -> SinkCloseOutcome {
    if !actor_state.accepting.load(Ordering::Acquire) {
        task.abort_actor();
        let outcome = SinkCloseOutcome::Failure(Arc::from(format!(
            "sink task '{name}' retired before close"
        )));
        state.publish_outcome(outcome.clone());
        let _ = wait_for_sink_terminal(&name, &mut task).await;
        return outcome;
    }
    let (ack_tx, mut ack_rx) = oneshot::oneshot();
    let command = SinkCommand {
        deadline,
        operation: SinkOperation::Close { ack: ack_tx },
    };

    state.set_phase("enqueue");
    match tx
        .send_with_timer(command, tokio::time::sleep_until(deadline))
        .await
    {
        Ok(()) => {}
        Err(SendTimeoutError::Disconnected(_)) => {
            return finish_disconnected_sink_close(&name, task, &state).await;
        }
        Err(SendTimeoutError::Timeout(_)) => {
            task.abort_actor();
            let outcome = SinkCloseOutcome::Failure(Arc::from(
                close_deadline_error(&name, "enqueue").to_string(),
            ));
            state.publish_outcome(outcome.clone());
            let _ = wait_for_sink_terminal(&name, &mut task).await;
            return outcome;
        }
    }

    state.set_phase("acknowledgement");
    let connector_result = match tokio::time::timeout_at(deadline, &mut ack_rx).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(ConnectorError::ConnectionFailed(format!(
            "sink task '{name}' dropped close acknowledgment"
        ))),
        Err(_) => {
            task.abort_actor();
            let outcome = SinkCloseOutcome::Failure(Arc::from(
                close_deadline_error(&name, "acknowledgement").to_string(),
            ));
            state.publish_outcome(outcome.clone());
            let _ = wait_for_sink_terminal(&name, &mut task).await;
            return outcome;
        }
    };

    state.set_phase("join");
    let Ok(join_result) =
        tokio::time::timeout_at(deadline, wait_for_sink_terminal(&name, &mut task)).await
    else {
        task.abort_actor();
        let outcome =
            SinkCloseOutcome::Failure(Arc::from(close_deadline_error(&name, "join").to_string()));
        state.publish_outcome(outcome.clone());
        let _ = wait_for_sink_terminal(&name, &mut task).await;
        return outcome;
    };

    match (connector_result, join_result) {
        (Ok(()), Ok(())) => SinkCloseOutcome::Success,
        (Err(error), Ok(())) | (Ok(()), Err(error)) => {
            SinkCloseOutcome::Failure(Arc::from(error.to_string()))
        }
        (Err(connector), Err(join)) => SinkCloseOutcome::Failure(Arc::from(format!(
            "sink '{name}' connector close failed: {connector}; task join also failed: {join}"
        ))),
    }
}

pub(super) async fn wait_for_sink_terminal(
    name: &str,
    task: &mut OwnedSinkTask,
) -> Result<(), ConnectorError> {
    match (&mut task.terminal_join).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(ConnectorError::Internal(format!(
            "sink task '{name}' failed while joining after close: {error}"
        ))),
        Err(error) => Err(ConnectorError::Internal(format!(
            "sink task '{name}' terminal supervisor failed: {error}"
        ))),
    }
}

pub(super) async fn finish_disconnected_sink_close(
    name: &str,
    mut task: OwnedSinkTask,
    state: &SinkCloseState,
) -> SinkCloseOutcome {
    task.abort_actor();
    let outcome = SinkCloseOutcome::Failure(Arc::from(format!(
        "sink task '{name}' rejected close command: channel closed"
    )));
    state.publish_outcome(outcome.clone());
    let _ = wait_for_sink_terminal(name, &mut task).await;
    outcome
}
