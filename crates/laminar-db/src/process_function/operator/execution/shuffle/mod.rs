//! Ordered input retention for the existing graph shuffle lifecycle.
//!
//! FIFO order is the order delivered by the graph. This does not certify replay ordering
//! between independent source channels; admission requires one fixed-batch replay channel.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use arrow::array::RecordBatch;

use super::{ProcessExecution, ProcessExecutionAuthority, ProcessFunctionOperator};
use crate::error::DbError;
use crate::operator::RetainedBatch;
use crate::operator_graph::{merge_input_frontier_iter, InputFrontier};

mod checkpoint;
mod input;
mod outbound;

pub(in super::super::super) use checkpoint::Checkpoint;
use outbound::PendingSend;

#[derive(Default)]
pub(in super::super::super) enum ShuffleState {
    #[default]
    Unbound,
    Restored(Box<Checkpoint>),
    Active(Box<ProcessShuffle>),
}

impl ShuffleState {
    pub(in super::super::super) fn active(&self) -> Option<&ProcessShuffle> {
        match self {
            Self::Active(shuffle) => Some(shuffle),
            Self::Unbound | Self::Restored(_) => None,
        }
    }

    pub(in super::super::super) fn checkpoint(&self) -> Option<Checkpoint> {
        match self {
            Self::Unbound => None,
            Self::Restored(checkpoint) => Some(checkpoint.as_ref().clone()),
            Self::Active(shuffle) => Some(shuffle.checkpoint()),
        }
    }

    pub(in super::super::super) fn retained_bytes(&self) -> usize {
        match self {
            Self::Unbound => 0,
            Self::Restored(checkpoint) => checkpoint.retained_bytes(),
            Self::Active(shuffle) => shuffle.retained_bytes(),
        }
    }
}

#[derive(Default)]
struct PeerFrontiers {
    applied: InputFrontier,
    accepted: InputFrontier,
    queued: usize,
}

enum PeerInput {
    Data(RetainedBatch),
    Frontier(InputFrontier),
}

struct PeerEvent {
    peer: u64,
    input: PeerInput,
    bytes: usize,
    rows: usize,
    hold_ms: Option<i64>,
}

enum ApplicationPhase {
    Ready(Vec<RecordBatch>),
    Running,
}

struct Application {
    phase: ApplicationPhase,
    // Keep transport credits until the existing worker scheduler finishes the application.
    retained: Option<RetainedBatch>,
    local_frontier: Option<InputFrontier>,
    bytes: usize,
    rows: usize,
    hold_ms: Option<i64>,
}

enum PendingInput {
    Send(PendingSend),
    Apply(Application),
}

pub(in super::super::super) struct ProcessShuffle {
    stage: String,
    runtime: tokio::runtime::Handle,
    wake: Arc<tokio::sync::Notify>,
    assignment_version: u64,
    assignment_digest: [u8; 32],
    self_id: u64,
    peers: BTreeMap<u64, PeerFrontiers>,
    events: VecDeque<PeerEvent>,
    queued_bytes: usize,
    queued_rows: usize,
    pending: Option<PendingInput>,
    local: InputFrontier,
    last_broadcast: InputFrontier,
    effective: InputFrontier,
}

impl ProcessShuffle {
    pub(in super::super::super) fn new(
        stage: String,
        runtime: tokio::runtime::Handle,
        authority: &ProcessExecutionAuthority,
        restored: &ShuffleState,
    ) -> Result<Self, DbError> {
        let peers = authority
            .assignment
            .owners()
            .iter()
            .filter_map(|owner| {
                (*owner != authority.config.self_id).then_some((owner.0, PeerFrontiers::default()))
            })
            .collect();
        let mut shuffle = Self {
            stage,
            runtime,
            wake: authority.config.receiver.work_ready_notify(),
            assignment_version: authority.assignment.version(),
            assignment_digest: authority
                .config
                .sender
                .active_assignment_digest()
                .ok_or_else(|| {
                    DbError::Checkpoint("process shuffle requires an installed assignment".into())
                })?,
            self_id: authority.config.self_id.0,
            peers,
            events: VecDeque::new(),
            queued_bytes: 0,
            queued_rows: 0,
            pending: None,
            local: InputFrontier::default(),
            last_broadcast: InputFrontier::default(),
            effective: InputFrontier::default(),
        };
        if let ShuffleState::Restored(checkpoint) = restored {
            shuffle.restore_frontiers(checkpoint)?;
        }
        Ok(shuffle)
    }

    pub(in super::super::super) fn pending(&self) -> bool {
        self.pending.is_some() || !self.events.is_empty() || self.local != self.last_broadcast
    }

    pub(in super::super::super) fn runnable(&self) -> bool {
        match &self.pending {
            Some(PendingInput::Send(send)) => send.runnable(),
            Some(PendingInput::Apply(_)) => false,
            None => !self.events.is_empty() || self.local != self.last_broadcast,
        }
    }

    pub(in super::super::super) fn frontier(&self) -> InputFrontier {
        let pending_hold = match &self.pending {
            Some(PendingInput::Send(send)) => send.hold_ms(),
            Some(PendingInput::Apply(application)) => application.hold_ms,
            None => None,
        };
        // Only cached batch metadata is inspected; never revisit retained Arrow rows here.
        let hold = self
            .events
            .iter()
            .filter_map(|event| event.hold_ms)
            .chain(pending_hold)
            .min();
        InputFrontier {
            idle: self.effective.idle && !self.pending(),
            ..self.effective
        }
        .held_at(hold)
    }

    pub(in super::super::super) fn retained_bytes(&self) -> usize {
        let pending = match &self.pending {
            Some(PendingInput::Send(send)) => send.retained_bytes(),
            Some(PendingInput::Apply(apply)) => apply.bytes,
            None => 0,
        };
        // Estimate B-tree allocations, including a sparse root and unused queue slots.
        std::mem::size_of::<Self>()
            .saturating_add(1_024)
            .saturating_add(self.stage.capacity())
            .saturating_add(
                self.peers
                    .len()
                    .saturating_mul(std::mem::size_of::<PeerFrontiers>() + 128),
            )
            .saturating_add(
                self.events
                    .capacity()
                    .saturating_mul(std::mem::size_of::<PeerEvent>()),
            )
            .saturating_add(self.queued_bytes)
            .saturating_add(pending)
    }

    fn pending_rows(&self) -> usize {
        match &self.pending {
            Some(PendingInput::Send(send)) => send.rows(),
            Some(PendingInput::Apply(apply)) => apply.rows,
            None => 0,
        }
    }

    fn merged_frontier(&self, local: InputFrontier) -> Result<InputFrontier, DbError> {
        let remote = self.peers.values().map(|peer| {
            if peer.queued == 0 {
                peer.applied
            } else {
                crate::operator::frontier::normalize_restored_local_frontier(
                    InputFrontier {
                        idle: false,
                        ..peer.applied
                    },
                    peer.applied,
                    self.effective.watermark,
                )
            }
        });
        let merged = merge_input_frontier_iter(std::iter::once(local).chain(remote), i64::MIN);
        input::validate_frontier(self.effective, merged)?;
        Ok(merged)
    }

    fn step(
        &mut self,
        operator: &mut ProcessFunctionOperator,
        inputs: &[Vec<RecordBatch>],
        frontier: InputFrontier,
        graph_budget: usize,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let has_data = inputs.iter().flatten().any(|batch| batch.num_rows() != 0);
        if self.pending() && has_data {
            return Err(DbError::StatefulOperatorPartialApply(
                "process accepted local input before its ordered shuffle cut drained".into(),
            ));
        }
        if let Some(pending) = self.pending.take() {
            return match pending {
                PendingInput::Send(mut send) => {
                    self.retry_send(&mut send, operator)?;
                    if let Some(application) = send.poll(operator)? {
                        self.apply(operator, application, graph_budget)
                    } else {
                        self.pending = Some(PendingInput::Send(send));
                        Ok(Vec::new())
                    }
                }
                PendingInput::Apply(application) => self.apply(operator, application, graph_budget),
            };
        }
        if let Some(event) = self.events.pop_front() {
            self.queued_bytes -= event.bytes;
            self.queued_rows -= event.rows;
            let channel = self.peers.get_mut(&event.peer).ok_or_else(|| {
                DbError::StatefulOperatorPartialApply(
                    "process retained an unknown shuffle peer".into(),
                )
            })?;
            channel.queued -= 1;
            return match event.input {
                PeerInput::Data(batch) => {
                    let input = vec![batch.batch().clone()];
                    self.apply(
                        operator,
                        Application {
                            phase: ApplicationPhase::Ready(input),
                            retained: Some(batch),
                            local_frontier: None,
                            bytes: event.bytes,
                            rows: event.rows,
                            hold_ms: event.hold_ms,
                        },
                        graph_budget,
                    )
                }
                PeerInput::Frontier(frontier) => {
                    input::validate_frontier(channel.applied, frontier)?;
                    channel.applied = frontier;
                    self.apply(
                        operator,
                        Application {
                            phase: ApplicationPhase::Ready(Vec::new()),
                            retained: None,
                            local_frontier: None,
                            bytes: 0,
                            rows: 0,
                            hold_ms: None,
                        },
                        graph_budget,
                    )
                }
            };
        }
        self.begin_local(operator, inputs, frontier, graph_budget)
    }

    fn apply(
        &mut self,
        operator: &mut ProcessFunctionOperator,
        mut application: Application,
        graph_budget: usize,
    ) -> Result<Vec<RecordBatch>, DbError> {
        if application
            .retained
            .as_ref()
            .is_some_and(|batch| batch.assignment_version() != Some(self.assignment_version))
        {
            return Err(DbError::StatefulOperatorPartialApply(
                "process retained input crossed its assignment boundary".into(),
            ));
        }
        let effective = self.merged_frontier(application.local_frontier.unwrap_or(self.local))?;
        operator.graph_budget = graph_budget
            .checked_sub(self.retained_bytes())
            .and_then(|bytes| bytes.checked_sub(application.bytes))
            .ok_or_else(|| {
                DbError::StatefulOperatorPartialApply(
                    "process shuffle exhausts its state budget".into(),
                )
            })?;
        let phase = std::mem::replace(&mut application.phase, ApplicationPhase::Running);
        let output = match phase {
            ApplicationPhase::Ready(input) => operator.apply_process_step(&[input], effective)?,
            ApplicationPhase::Running => operator.apply_process_step(&[], effective)?,
        };
        operator.require_execution_current()?;
        if operator.worker_pending() {
            self.pending = Some(PendingInput::Apply(application));
        } else {
            if let Some(local) = application.local_frontier {
                self.local = local;
                self.last_broadcast = local;
            }
            self.effective = effective;
        }
        Ok(output)
    }
}

impl ProcessFunctionOperator {
    pub(in super::super::super) fn process_shuffled(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontier: InputFrontier,
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.require_execution_current()?;
        let ShuffleState::Active(mut shuffle) = std::mem::take(&mut self.shuffle) else {
            return Err(DbError::StatefulOperatorPartialApply(
                "process shuffle state is not bound".into(),
            ));
        };
        let budget = self.graph_budget;
        let result = shuffle.step(self, inputs, frontier, budget);
        self.graph_budget = budget;
        self.shuffle = ShuffleState::Active(shuffle);
        result.map_err(|error| {
            if error.requires_pipeline_halt() || error.requires_pipeline_recovery() {
                error
            } else {
                DbError::StatefulOperatorPartialApply(format!(
                    "process ordered shuffle failed; recover the graph: {error}"
                ))
            }
        })
    }
}
