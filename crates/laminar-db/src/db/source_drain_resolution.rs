//! Resolving this node's source drains, with the committed cursors a commit hands over.

use std::collections::BTreeMap;
use std::sync::Arc;

use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::connector::{SourceDrainOutcome, SourceDrainResolution};
use laminar_core::checkpoint::{AssignmentDrainTransition, CommittedCheckpointRef};

use super::{DbError, LaminarDB};
use crate::pipeline::streaming_coordinator::{
    owned_source_holds_drain, resolve_owned_source_drain,
};

impl LaminarDB {
    /// Resolve the exact local source drain after target commit or abort.
    ///
    /// A commit hands every source still holding the drain its merged cursor at the handoff
    /// checkpoint, so a channel this node acquires resumes after everything its donor consumed. The
    /// node's own history of that channel predates the donor's progress and must not position it.
    pub(crate) async fn resolve_local_source_drain(
        &self,
        transition: &AssignmentDrainTransition,
        outcome: SourceDrainOutcome,
        handoff: Option<&CommittedCheckpointRef>,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let holds_drain = owned_source_holds_drain(&self.owned_source_tasks, transition.id());
        let committed = match outcome {
            SourceDrainOutcome::Commit if holds_drain => {
                self.committed_handoff_cursors(transition, handoff, deadline)
                    .await?
            }
            SourceDrainOutcome::Commit | SourceDrainOutcome::Abort => BTreeMap::new(),
        };
        resolve_owned_source_drain(
            &self.owned_source_tasks,
            SourceDrainResolution {
                round: transition.id(),
                outcome,
            },
            &committed,
            deadline,
        )
        .await
        .map_err(|error| DbError::Checkpoint(format!("source drain resolution failed: {error}")))
    }

    async fn committed_handoff_cursors(
        &self,
        transition: &AssignmentDrainTransition,
        handoff: Option<&CommittedCheckpointRef>,
        deadline: tokio::time::Instant,
    ) -> Result<BTreeMap<String, Arc<SourceCheckpoint>>, DbError> {
        let handoff = handoff.ok_or_else(|| {
            DbError::Checkpoint(format!(
                "[LDB-6050] committed drain {:?} has no handoff checkpoint",
                transition.id()
            ))
        })?;
        let guard = tokio::time::timeout_at(deadline, self.coordinator.lock())
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] drain {:?} timed out waiting for the checkpoint coordinator",
                    transition.id()
                ))
            })?;
        let coordinator = guard.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "[LDB-6050] drain {:?} has no checkpoint coordinator",
                transition.id()
            ))
        })?;
        coordinator
            .load_handoff_source_cursors(handoff, &transition.predecessor, deadline)
            .await
    }
}
