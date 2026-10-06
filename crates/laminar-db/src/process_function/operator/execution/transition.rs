//! Prepare replacement execution before the graph publishes a vnode transition.

use super::shuffle::{Checkpoint, ProcessShuffle, ShuffleState};
use super::{ProcessExecution, ProcessExecutionAuthority, ProcessFunctionOperator};
use crate::error::DbError;
use crate::operator_graph::InputFrontier;
use laminar_core::checkpoint::CheckpointAssignmentFence;

impl ProcessExecution {
    pub(in super::super) fn local_id(&self) -> Option<super::NodeId> {
        match self {
            Self::Local => None,
            Self::AwaitingAssignment { self_id, .. } => Some(*self_id),
            Self::SingleOwner(authority) | Self::Distributed(authority) => {
                Some(authority.config.self_id)
            }
        }
    }

    pub(in super::super) fn retained_bytes(&self) -> usize {
        match self {
            Self::Local => 0,
            Self::AwaitingAssignment { stage, .. } => stage.capacity(),
            Self::SingleOwner(authority) | Self::Distributed(authority) => {
                authority.stage.capacity()
            }
        }
    }
}

impl ProcessFunctionOperator {
    pub(in super::super) fn prepare_transition_execution(
        &self,
        target: &CheckpointAssignmentFence,
        cut: InputFrontier,
    ) -> Result<(Option<ProcessExecution>, ShuffleState), DbError> {
        let authority = match &self.execution {
            ProcessExecution::Local => return Ok((None, ShuffleState::Unbound)),
            ProcessExecution::AwaitingAssignment { self_id, .. } => {
                if !target.contains(self_id.0) {
                    return Err(DbError::Checkpoint(
                        "process bootstrap has no target ownership".into(),
                    ));
                }
                let shuffle = Checkpoint::reassigned(target, self_id.0, cut)
                    .map_or(ShuffleState::Unbound, |checkpoint| {
                        ShuffleState::Restored(Box::new(checkpoint))
                    });
                return Ok((None, shuffle));
            }
            ProcessExecution::SingleOwner(authority) | ProcessExecution::Distributed(authority) => {
                authority
            }
        };
        if !target.contains(authority.config.self_id.0) {
            // Keep the retired authority fenced by the new state assignment. No intake can
            // resume after final-owner exit, even if the operator is called outside the graph.
            return Ok((None, ShuffleState::Unbound));
        }
        let replacement = ProcessExecutionAuthority::bind(
            &authority.config,
            target,
            std::sync::Arc::clone(&authority.deadline),
            authority.stage.clone(),
            authority.runtime.clone(),
        )?;
        replacement.require_current()?;
        if !replacement.is_distributed() {
            return Ok((
                Some(ProcessExecution::SingleOwner(replacement)),
                ShuffleState::Unbound,
            ));
        }
        let checkpoint = Checkpoint::reassigned(target, authority.config.self_id.0, cut)
            .ok_or_else(|| {
                DbError::Checkpoint("process transition has no target peer roster".into())
            })?;
        let restored = ShuffleState::Restored(Box::new(checkpoint));
        let shuffle = ProcessShuffle::new(
            replacement.stage.clone(),
            replacement.runtime.clone(),
            &replacement,
            &restored,
        )?;
        Ok((
            Some(ProcessExecution::Distributed(replacement)),
            ShuffleState::Active(Box::new(shuffle)),
        ))
    }
}
