use super::{
    deliver_checkpoint_completion, deliver_checkpoint_failure, set_checkpoint_fault,
    CheckpointCompletion, ConnectorPipelineCallback, LeaderTail, CHECKPOINT_FAILURE_REPORT_TIMEOUT,
};

impl ConnectorPipelineCallback {
    pub(super) fn reap_checkpoint_tail_tasks(&mut self) {
        while let Some(result) = self.checkpoint_tail_tasks.try_join_next() {
            if let Err(error) = result {
                set_checkpoint_fault(
                    &self.checkpoint_fault,
                    format!("checkpoint durable tail terminated unexpectedly: {error}"),
                );
            }
        }
    }

    pub(super) fn spawn_checkpoint_tail(
        &mut self,
        tail: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        self.reap_checkpoint_tail_tasks();
        self.checkpoint_tail_tasks
            .spawn_on(tail, &self.checkpoint_tail_runtime);
    }

    pub(super) fn cancel_fenced_checkpoint_tail_tasks(&mut self) -> bool {
        #[cfg(feature = "cluster")]
        {
            use std::sync::atomic::Ordering;

            if !self.coordinated_lifecycle_active.load(Ordering::Acquire) {
                return false;
            }
            self.reap_checkpoint_tail_tasks();
            let tail_count = self.checkpoint_tail_tasks.len();
            if tail_count != 0 {
                tracing::warn!(
                    tail_count,
                    "coordinated recovery cancelled fenced checkpoint durable tails"
                );
                self.checkpoint_tail_tasks.abort_all();
            }
        }
        true
    }

    pub(super) async fn settle_spawned_checkpoint_tail_tasks(&mut self) -> Result<(), String> {
        let mut failures = Vec::new();
        while let Some(result) = self.checkpoint_tail_tasks.join_next().await {
            match result {
                Ok(()) => {}
                #[cfg(feature = "cluster")]
                Err(error)
                    if error.is_cancelled()
                        && self
                            .coordinated_lifecycle_active
                            .load(std::sync::atomic::Ordering::Acquire) => {}
                Err(error) => failures.push(error.to_string()),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "checkpoint durable tail task failure: {}",
                failures.join("; ")
            ))
        }
    }

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
