use super::{
    deliver_checkpoint_completion, deliver_checkpoint_failure, set_checkpoint_fault,
    CheckpointCompletion, ConnectorPipelineCallback, LeaderTail, CHECKPOINT_FAILURE_REPORT_TIMEOUT,
};

impl ConnectorPipelineCallback {
    pub(super) async fn complete_successful_leader_tail(
        tail: &mut LeaderTail,
        result: crate::checkpoint_coordinator::CheckpointResult,
    ) {
        let mut continuation_error = result.continuation_error().map(str::to_owned);
        match CheckpointCompletion::validated(
            tail.attempt,
            result,
            tail.fan_out.clone(),
            tail.handoff.replay_pending,
        ) {
            Ok(completion) => {
                let terminal_handoff = tail.handoff.terminal();
                if let Some(error) = continuation_error.as_ref() {
                    tail.in_flight.fail_sink_epoch(error.clone());
                } else if !terminal_handoff {
                    if let Err(error) = tail.in_flight.publish_successor() {
                        let error = format!(
                            "checkpoint {} epoch {} committed, but successor sink publication failed: {error}",
                            tail.attempt.checkpoint_id, tail.attempt.epoch
                        );
                        set_checkpoint_fault(&tail.checkpoint_fault, error.clone());
                        continuation_error = Some(error);
                    }
                }
                if let Some(guard) = tail.mutable_operator_capture_guard.as_mut() {
                    guard.disarm();
                }
                let report_deadline =
                    tokio::time::Instant::now() + CHECKPOINT_FAILURE_REPORT_TIMEOUT;
                if !deliver_checkpoint_completion(&tail.complete_tx, completion, report_deadline)
                    .await
                {
                    set_checkpoint_fault(
                        &tail.checkpoint_fault,
                        format!(
                            "checkpoint {} epoch {} committed but its completion could not be \
                             reported within {:?}",
                            tail.attempt.checkpoint_id,
                            tail.attempt.epoch,
                            CHECKPOINT_FAILURE_REPORT_TIMEOUT,
                        ),
                    );
                    return;
                }
                if let Some(error) = continuation_error {
                    set_checkpoint_fault(&tail.checkpoint_fault, error);
                } else {
                    tail.in_flight.disarm_sink_epoch();
                }
            }
            Err(reason) => {
                tracing::error!(
                    error = %reason,
                    "[LDB-6048] refusing mismatched checkpoint completion"
                );
                tail.in_flight.fail_sink_epoch(reason.clone());
                set_checkpoint_fault(&tail.checkpoint_fault, reason.clone());
                deliver_checkpoint_failure(
                    &tail.complete_tx,
                    tail.attempt,
                    reason,
                    &tail.checkpoint_fault,
                )
                .await;
            }
        }
    }
}
