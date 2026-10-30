//! Epoch admission of a checkpoint-committable sink: waiting for, publishing, sealing and
//! failing the writable epoch every clone of the handle observes.

use std::sync::atomic::Ordering;

use laminar_connectors::error::ConnectorError;
use tokio::time::Instant;

use super::handle::SinkTaskHandle;
use super::operation::command_deadline_error;
use super::protocol::{SinkEpochAdmission, SinkEpochGateState};
use crate::flow_metrics::SinkFlowMetrics;

impl SinkTaskHandle {
    pub(super) fn epoch_gate_error(
        &self,
        expected: impl Into<String>,
        actual: SinkEpochGateState,
    ) -> ConnectorError {
        ConnectorError::InvalidState {
            expected: expected.into(),
            actual: format!("sink '{}' epoch gate is {actual:?}", self.name),
        }
    }

    pub(crate) async fn wait_for_open_epoch_until(
        &self,
        deadline: Option<Instant>,
    ) -> Result<Option<SinkEpochAdmission>, ConnectorError> {
        let Some(gate) = self.epoch_gate.as_ref() else {
            return Ok(None);
        };
        let mut state = gate.subscribe();
        let mut wait_timer = None;
        loop {
            let observed = *state.borrow_and_update();
            match observed {
                SinkEpochGateState::Open(admission) => return Ok(Some(admission)),
                SinkEpochGateState::Failed { .. } => {
                    return Err(self.epoch_gate_error("a writable sink epoch", observed));
                }
                SinkEpochGateState::Unopened
                | SinkEpochGateState::Sealed(_)
                | SinkEpochGateState::Opening(_)
                | SinkEpochGateState::Begun(_) => {}
            }
            if wait_timer.is_none() {
                wait_timer = self.flow.get().map(SinkFlowMetrics::time_epoch_gate_wait);
            }
            let changed = state.changed();
            tokio::pin!(changed);
            let actor_finished = self.actor_state.finished_notify.notified();
            tokio::pin!(actor_finished);
            actor_finished.as_mut().enable();
            if self.actor_state.finished.load(Ordering::Acquire) {
                return Err(self.closed_err());
            }
            if let Some(deadline) = deadline {
                tokio::select! {
                    biased;
                    result = &mut changed => result.map_err(|_| self.closed_err())?,
                    () = actor_finished.as_mut() => return Err(self.closed_err()),
                    () = tokio::time::sleep_until(deadline) => {
                        return Err(command_deadline_error(
                            &self.name,
                            "sink epoch gate",
                            deadline.saturating_duration_since(Instant::now()),
                        ));
                    }
                }
            } else {
                let stalled = async {
                    let Some(timeout) = self.epoch_gate_stall_timeout else {
                        return std::future::pending().await;
                    };
                    tokio::time::sleep(timeout).await;
                    timeout
                };
                tokio::select! {
                    biased;
                    result = &mut changed => result.map_err(|_| self.closed_err())?,
                    () = actor_finished.as_mut() => return Err(self.closed_err()),
                    timeout = stalled => {
                        return Err(ConnectorError::Internal(format!(
                            "sink '{}' epoch gate stayed {observed:?} for the {timeout:?} \
                             epoch-gate stall timeout",
                            self.name
                        )));
                    }
                }
            }
        }
    }

    /// Wait for the checkpoint-committable sink group to publish a writable epoch. This does not
    /// take command admission; `write_batch_before` locks and rechecks the exact generation.
    pub(crate) async fn wait_for_write_gate_until(
        &self,
        supplied_deadline: Option<Instant>,
    ) -> Result<Option<SinkEpochAdmission>, ConnectorError> {
        self.ensure_open()?;
        self.wait_for_open_epoch_until(supplied_deadline).await
    }

    pub(crate) fn begun_epoch_admission(&self, epoch: u64) -> Option<SinkEpochAdmission> {
        self.epoch_gate
            .as_ref()
            .and_then(|gate| match *gate.borrow() {
                SinkEpochGateState::Begun(admission) if admission.epoch == epoch => Some(admission),
                _ => None,
            })
    }

    pub(crate) fn current_begun_epoch_admission(&self) -> Option<SinkEpochAdmission> {
        self.epoch_gate
            .as_ref()
            .and_then(|gate| match *gate.borrow() {
                SinkEpochGateState::Begun(admission) => Some(admission),
                _ => None,
            })
    }

    /// Publish only after the coordinator has preflighted the whole group and made its allocator
    /// reservation Ready. There is deliberately no await or fallible work in this phase.
    pub(crate) fn publish_open_epoch(
        &self,
        admission: SinkEpochAdmission,
    ) -> Result<(), ConnectorError> {
        self.ensure_open()?;
        let Some(gate) = self.epoch_gate.as_ref() else {
            return Ok(());
        };
        let changed = gate.send_if_modified(|state| {
            if *state == SinkEpochGateState::Begun(admission) {
                *state = SinkEpochGateState::Open(admission);
                true
            } else {
                false
            }
        });
        if changed {
            Ok(())
        } else {
            Err(self.epoch_gate_error(
                format!("begun epoch admission {admission:?}"),
                *gate.borrow(),
            ))
        }
    }

    pub(crate) fn fail_epoch_transition(&self, admission: SinkEpochAdmission) {
        let Some(gate) = self.epoch_gate.as_ref() else {
            return;
        };
        gate.send_if_modified(|state| {
            let same_generation = match *state {
                SinkEpochGateState::Open(current)
                | SinkEpochGateState::Sealed(current)
                | SinkEpochGateState::Opening(current)
                | SinkEpochGateState::Begun(current) => current.generation == admission.generation,
                SinkEpochGateState::Failed { generation } => generation == admission.generation,
                SinkEpochGateState::Unopened => false,
            };
            if same_generation && !matches!(*state, SinkEpochGateState::Failed { .. }) {
                *state = SinkEpochGateState::Failed {
                    generation: admission.generation,
                };
                true
            } else {
                false
            }
        });
    }

    pub(crate) fn fail_epoch_gate(&self) {
        if let Some(gate) = self.epoch_gate.as_ref() {
            let generation = match *gate.borrow() {
                SinkEpochGateState::Unopened => 0,
                SinkEpochGateState::Open(admission)
                | SinkEpochGateState::Sealed(admission)
                | SinkEpochGateState::Opening(admission)
                | SinkEpochGateState::Begun(admission) => admission.generation,
                SinkEpochGateState::Failed { generation } => generation,
            };
            gate.send_replace(SinkEpochGateState::Failed { generation });
        }
    }

    pub(crate) fn open_epoch_admission(
        &self,
        epoch: u64,
    ) -> Result<SinkEpochAdmission, ConnectorError> {
        let gate = self.epoch_gate.as_ref().ok_or_else(|| {
            self.epoch_gate_error("checkpoint-committable sink", SinkEpochGateState::Unopened)
        })?;
        match *gate.borrow() {
            SinkEpochGateState::Open(admission) if admission.epoch == epoch => Ok(admission),
            observed => Err(self.epoch_gate_error(format!("open epoch {epoch}"), observed)),
        }
    }

    pub(crate) async fn seal_epoch_until(
        &self,
        admission: SinkEpochAdmission,
        deadline: Instant,
    ) -> Result<SinkEpochAdmission, ConnectorError> {
        let Some(gate) = self.epoch_gate.as_ref() else {
            return Ok(admission);
        };
        let _admission = tokio::time::timeout_at(deadline, self.admission.lock())
            .await
            .map_err(|_| command_deadline_error(&self.name, "epoch seal", self.write_timeout))?;
        self.ensure_open()?;
        let observed = *gate.borrow();
        if observed != SinkEpochGateState::Open(admission) {
            return Err(
                self.epoch_gate_error(format!("open epoch admission {admission:?}"), observed)
            );
        }
        let sealed = SinkEpochAdmission {
            epoch: admission.epoch,
            generation: admission.generation.checked_add(1).ok_or_else(|| {
                ConnectorError::InvalidState {
                    expected: "non-exhausted sink epoch generation".into(),
                    actual: format!("sink '{}' generation overflow", self.name),
                }
            })?,
        };
        gate.send_replace(SinkEpochGateState::Sealed(sealed));
        Ok(sealed)
    }

    /// Idempotent protocol seal for coordinator APIs that do not own a callback transition
    /// guard. It shares write admission, so every accepted write is ordered before the seal.
    pub(crate) async fn seal_epoch_for_protocol_until(
        &self,
        epoch: u64,
        deadline: Instant,
    ) -> Result<Option<SinkEpochAdmission>, ConnectorError> {
        let Some(gate) = self.epoch_gate.as_ref() else {
            return Ok(None);
        };
        let _admission = tokio::time::timeout_at(deadline, self.admission.lock())
            .await
            .map_err(|_| command_deadline_error(&self.name, "epoch seal", self.write_timeout))?;
        self.ensure_open()?;
        let observed = *gate.borrow();
        match observed {
            SinkEpochGateState::Open(admission) if admission.epoch == epoch => {
                let sealed = SinkEpochAdmission {
                    epoch,
                    generation: admission.generation.checked_add(1).ok_or_else(|| {
                        ConnectorError::InvalidState {
                            expected: "non-exhausted sink epoch generation".into(),
                            actual: format!("sink '{}' generation overflow", self.name),
                        }
                    })?,
                };
                gate.send_replace(SinkEpochGateState::Sealed(sealed));
                Ok(Some(sealed))
            }
            SinkEpochGateState::Sealed(admission) if admission.epoch == epoch => {
                Ok(Some(admission))
            }
            _ => Err(self.epoch_gate_error(format!("open or sealed epoch {epoch}"), observed)),
        }
    }
}
