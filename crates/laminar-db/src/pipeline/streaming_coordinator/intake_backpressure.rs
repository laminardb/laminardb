#[cfg(feature = "cluster")]
use super::drain_ownership::source_drain_awaits_intake;
use tokio::sync::OwnedSemaphorePermit;

use super::flow_metrics::IntakePause;
use super::{
    CoordinatorGates, CoordinatorRunState, CycleError, PipelineCallback, PortHeadroom,
    QueuedSourceMsg, SourceMsg, StreamingCoordinator,
};

/// Room on each source's graph ports for this cycle, and the byte permits of the messages staged
/// against it. A staged message keeps its permit until its port admits it, so
/// `source_queue_max_bytes` bounds queued, parked and staged bytes alike.
#[derive(Default)]
pub(super) struct SourceAdmission {
    headroom: Vec<PortHeadroom>,
    permits: Vec<OwnedSemaphorePermit>,
}

impl CoordinatorGates {
    pub(super) fn compute_admitted(&self) -> bool {
        !self.intake_paused && !self.external_output_backpressured
    }
}

impl StreamingCoordinator {
    pub(super) fn capture_gates(&self, callback: &mut impl PipelineCallback) -> CoordinatorGates {
        let intake_paused = callback.intake_paused();
        if intake_paused || self.replay_pending {
            let _ = callback.is_recovering();
        }
        let external_output_backpressured =
            callback.external_output_pressure().pauses_intake(|| {
                self.pending_barrier.active || {
                    #[cfg(feature = "cluster")]
                    {
                        self.source_handles
                            .iter()
                            .any(|source| source_drain_awaits_intake(&source.task))
                    }
                    #[cfg(not(feature = "cluster"))]
                    {
                        false
                    }
                }
            });
        CoordinatorGates {
            intake_paused,
            external_output_backpressured,
        }
    }

    pub(super) fn observe_intake_gates(
        &mut self,
        state: &mut CoordinatorRunState,
        gates: &CoordinatorGates,
        intake_blocked: bool,
    ) {
        self.observe_intake_gate_for_checkpoint_cadence(state, intake_blocked);
        self.intake_stats.observe_queue(self.rx.len());
        let output_backpressured = !intake_blocked && gates.external_output_backpressured;
        self.intake_stats
            .observe(IntakePause::Fence, intake_blocked);
        self.intake_stats
            .observe(IntakePause::Output, output_backpressured);
        if intake_blocked || output_backpressured {
            self.intake_stats
                .observe(IntakePause::PortBackpressure, false);
        }
    }

    /// Start staging a cycle's source input against the room its graph ports have now.
    pub(super) fn begin_source_staging(&mut self, callback: &impl PipelineCallback) {
        self.source_batches_buf.clear();
        self.release_staged_permits();
        self.refresh_source_headroom(callback);
    }

    /// The graph has admitted, or the cycle has dropped, every staged batch.
    pub(super) fn release_staged_permits(&mut self) {
        self.source_admission.permits.clear();
    }

    pub(super) fn hold_staged_permit(&mut self, permit: Option<OwnedSemaphorePermit>) {
        self.source_admission.permits.extend(permit);
    }

    fn refresh_source_headroom(&mut self, callback: &impl PipelineCallback) {
        let headroom = &mut self.source_admission.headroom;
        headroom.clear();
        headroom.extend(
            self.source_names
                .iter()
                .map(|name| callback.source_port_headroom(name)),
        );
    }

    /// Charge a message against its source's room. Barriers carry no rows and always fit.
    pub(super) fn charge_source_headroom(&mut self, message: &SourceMsg) -> bool {
        let SourceMsg::Batch {
            source_idx, batch, ..
        } = message
        else {
            return true;
        };
        self.source_admission
            .headroom
            .get_mut(*source_idx)
            .is_none_or(|room| room.try_charge(batch))
    }

    /// No later message fits during the shutdown drain.
    pub(super) fn close_shutdown_staging(&mut self) {
        self.source_admission.headroom.fill(PortHeadroom {
            batches: 0,
            bytes: None,
            empty: false,
        });
    }

    /// Whether the parked message can start a cycle: it fits its ports now, or retained graph
    /// work can run and make room. Otherwise the coordinator waits for another wake.
    pub(super) fn parked_message_ready(&mut self, callback: &impl PipelineCallback) -> bool {
        if self.parked_source_msg.is_none() {
            return false;
        }
        self.refresh_source_headroom(callback);
        let fits = match self
            .parked_source_msg
            .as_ref()
            .map(|parked| &parked.message)
        {
            Some(SourceMsg::Batch {
                source_idx, batch, ..
            }) => self
                .source_admission
                .headroom
                .get(*source_idx)
                .copied()
                .is_none_or(|mut room| room.try_charge(batch)),
            _ => true,
        };
        fits || callback.has_runnable_deferred_input()
    }

    /// Park the first message that does not fit its source's ports; staging stops behind it, so
    /// every later message keeps its FIFO order.
    pub(super) fn park_for_headroom(
        &mut self,
        message: QueuedSourceMsg,
        callback: &impl PipelineCallback,
    ) {
        debug_assert!(self.parked_source_msg.is_none());
        self.parked_source_msg = Some(message);
        callback.note_source_backpressure();
    }

    pub(super) fn observe_port_backpressure(&self) {
        self.intake_stats.observe(
            IntakePause::PortBackpressure,
            self.parked_source_msg.is_some(),
        );
    }

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
