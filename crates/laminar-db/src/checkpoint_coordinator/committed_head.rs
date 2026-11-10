//! A process outside the current assignment's checkpoint participants learns of no commit from
//! the checkpoint protocol, so its committed reads and read pin follow the authority's committed
//! head instead.

use std::sync::Arc;
use std::time::Duration;

use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointStore, PipelineIdentity};
use laminar_core::cluster::control::LeaderLeaseStore;

use super::committed_reads::{publish_unless_older, CommittedCut, CommittedCutSender};
use super::CheckpointCoordinator;
use crate::error::DbError;

/// How often a process outside the participants looks for a newer commit.
const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Bound on one look, so an unresponsive store cannot stall the follower.
const POLL_TIMEOUT: Duration = Duration::from_secs(10);

impl CheckpointCoordinator {
    /// Follow the committed head while this process takes part in no checkpoint, until the
    /// coordinator is dropped.
    pub(crate) fn follow_committed_head(&mut self) -> Result<(), DbError> {
        let Some(controller) = self.cluster_controller.clone() else {
            return Ok(());
        };
        let authority = controller
            .checkpoint_authority()
            .map_err(|error| DbError::Checkpoint(format!("committed-head authority: {error}")))?;
        let pipeline = self.pipeline_identity.clone().ok_or_else(|| {
            DbError::Checkpoint("committed-head follower has no pipeline identity".into())
        })?;
        let follower = HeadFollower {
            cuts: self.committed_cut.clone(),
            fence: controller.checkpoint_assignment_watch(),
            participant: controller.instance_id().0,
            authority,
            store: Arc::clone(&self.store),
            pipeline,
        };
        if let Some(previous) = self.head_follower.replace(tokio::spawn(follower.run())) {
            previous.abort();
        }
        Ok(())
    }
}

struct HeadFollower {
    cuts: CommittedCutSender,
    fence: tokio::sync::watch::Receiver<Option<CheckpointAssignmentFence>>,
    participant: u64,
    authority: Arc<LeaderLeaseStore>,
    store: Arc<dyn CheckpointStore>,
    pipeline: PipelineIdentity,
}

impl HeadFollower {
    /// Runs until the owning coordinator aborts it.
    async fn run(self) {
        let mut tick = tokio::time::interval(POLL_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            if self.takes_part() {
                continue;
            }
            match tokio::time::timeout(POLL_TIMEOUT, self.publish_newer_head()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(%error, "following the committed head failed"),
                Err(_) => tracing::warn!("following the committed head timed out"),
            }
        }
    }

    /// A participant publishes each cut as it commits, so it needs no follower.
    fn takes_part(&self) -> bool {
        self.fence
            .borrow()
            .as_ref()
            .is_some_and(|fence| fence.participant_incarnation(self.participant).is_some())
    }

    async fn publish_newer_head(&self) -> Result<(), DbError> {
        let authority_error =
            |error| DbError::Checkpoint(format!("committed-head authority: {error}"));
        let Some(head) = self
            .authority
            .highest_cluster_committed_outcome()
            .await
            .map_err(authority_error)?
        else {
            return Ok(());
        };
        let published = self.cuts.borrow().as_ref().map(|cut| cut.index.epoch);
        if published.is_some_and(|epoch| epoch >= head.epoch) {
            return Ok(());
        }
        let Some((outcome, Some(index))) = self
            .authority
            .cluster_outcome_with_committed_checkpoint(head.epoch)
            .await
            .map_err(authority_error)?
        else {
            return Ok(());
        };
        // Reads do not check a cut's topology, so a cut of another pipeline waits until this
        // process installs it and takes part again.
        if index.pipeline_identity != self.pipeline {
            return Ok(());
        }
        let reference = outcome.committed_checkpoint.clone().ok_or_else(|| {
            DbError::Checkpoint("Commit outcome has no committed checkpoint reference".into())
        })?;
        let manifests = super::load_index_manifests(self.store.as_ref(), &index).await?;
        publish_unless_older(
            &self.cuts,
            Arc::new(CommittedCut::new(index, reference, &manifests)),
        );
        Ok(())
    }
}
