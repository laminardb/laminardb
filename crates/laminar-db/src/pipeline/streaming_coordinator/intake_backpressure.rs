use super::{
    CoordinatorGates, CoordinatorRunState, CycleError, PendingBarrier, PipelineCallback,
    QueuedSourceMsg, StreamingCoordinator,
};

impl CoordinatorGates {
    pub(super) fn capture(
        callback: &mut impl PipelineCallback,
        replay_pending: bool,
        pending_barrier: &PendingBarrier,
    ) -> Self {
        let intake_paused = callback.intake_paused();
        if intake_paused || replay_pending {
            let _ = callback.is_recovering();
        }
        let external_output_backpressured = callback
            .external_output_pressure()
            .pauses_intake(pending_barrier.active);
        Self {
            intake_paused,
            external_output_backpressured,
        }
    }

    pub(super) fn compute_admitted(&self) -> bool {
        !self.intake_paused && !self.external_output_backpressured
    }
}

impl StreamingCoordinator {
    pub(super) async fn service_paused_intake(
        &mut self,
        callback: &mut impl PipelineCallback,
        state: &mut CoordinatorRunState,
        message: Option<QueuedSourceMsg>,
    ) -> bool {
        if let Some(message) = message {
            if self.parked_source_msg.is_some() {
                state.fault =
                    Some("source intake gate race exceeded its single parked-message slot".into());
                return false;
            }
            self.parked_source_msg = Some(message);
        }
        self.drain_manual_requests();
        self.prune_manual_requests();
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("fenced vnode transition completion") {
            state.fault = Some(error.to_string());
            return false;
        }
        match callback.complete_pending_vnode_transition().await {
            Ok(_) => {}
            Err(CycleError::Halt(reason)) => {
                tracing::warn!(%reason, "[LDB-3022] fenced vnode transition halted");
                state.halt(reason);
                return false;
            }
            Err(CycleError::Recovery(reason) | CycleError::Fatal(reason)) => {
                state.fault = Some(format!(
                    "fenced vnode transition completion failed: {reason}"
                ));
                return false;
            }
        }
        // A strong intake fence blocks the mixed FIFO carrying both data and source barriers.
        // Only pre-reservation public-request lifecycle is safe until the fence reopens.
        self.drain_manual_requests();
        self.prune_manual_requests();
        true
    }

    pub(super) async fn service_external_output_backpressure(
        &mut self,
        callback: &mut impl PipelineCallback,
        state: &mut CoordinatorRunState,
        checkpoint_control_due: bool,
    ) -> bool {
        // No unhandled source barrier exists here: a prepared cut already owns its barriers, and
        // checkpoint backpressure lifts once a new attempt injects them. Keep completion,
        // checkpoint control, and public-request lifecycle active until output pressure eases.
        state.barriers.clear();
        self.service_background_work(callback, state, checkpoint_control_due)
            .await
    }
}
