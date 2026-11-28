//! The external cursor a committed checkpoint publishes after. A sink's commit namespace carries
//! the pipeline identity, which a topology migration changes: the target's first checkpoint finds
//! no cursor in its own namespace, because its predecessor, the migration cut, was published under
//! the parent's, or, for a sink the migration added, never published at all.

use laminar_connectors::connector::{CoordinatedCommitCursor, CoordinatedCommitNamespace};
use laminar_core::checkpoint::{
    CheckpointAttempt, CommittedCheckpointIndex, CommittedCheckpointRef,
};

use super::commit_index::load_committed_manifest_until;
use super::{CheckpointCoordinator, DbError, RegisteredSink};

const ZERO_CURSOR: CoordinatedCommitCursor = CoordinatedCommitCursor {
    checkpoint_id: 0,
    fencing_token: 0,
};

/// Where a sink stands for one committed checkpoint.
pub(super) enum ExternalPublication {
    /// The checkpoint is already published.
    Published,
    /// Publish after this cursor of the sink's own namespace.
    After(CoordinatedCommitCursor),
}

impl CheckpointCoordinator {
    pub(super) async fn external_publication_until(
        &self,
        sink: &RegisteredSink,
        namespace: &CoordinatedCommitNamespace,
        attempt: CheckpointAttempt,
        fencing_token: u64,
        predecessor: Option<&CommittedCheckpointRef>,
        deadline: tokio::time::Instant,
    ) -> Result<ExternalPublication, DbError> {
        let cursor = read_external_cursor_until(sink, namespace, deadline).await?;
        if let Some(cursor) = cursor.filter(|cursor| cursor.checkpoint_id == attempt.checkpoint_id)
        {
            if cursor.fencing_token != fencing_token {
                return Err(DbError::Checkpoint(format!(
                    "sink '{}' checkpoint {} was committed under fencing token {}, expected {fencing_token}",
                    sink.name, attempt.checkpoint_id, cursor.fencing_token
                )));
            }
            return Ok(ExternalPublication::Published);
        }
        let Some(predecessor) = predecessor else {
            return Ok(ExternalPublication::After(ZERO_CURSOR));
        };
        if let Some(cursor) =
            cursor.filter(|cursor| cursor.checkpoint_id == predecessor.checkpoint_id)
        {
            return Ok(ExternalPublication::After(cursor));
        }
        self.require_published_under_parent_until(sink, namespace, predecessor, cursor, deadline)
            .await?;
        Ok(ExternalPublication::After(cursor.unwrap_or(ZERO_CURSOR)))
    }

    /// A namespace that does not hold the predecessor publishes after its own cursor, empty or
    /// left by an earlier topology with the same identity, only when the predecessor is a
    /// migration cut bound to another pipeline identity: that identity's namespace must hold
    /// exactly the cut, unless the cut has no descriptor for this sink, which the migration added.
    async fn require_published_under_parent_until(
        &self,
        sink: &RegisteredSink,
        namespace: &CoordinatedCommitNamespace,
        predecessor: &CommittedCheckpointRef,
        cursor: Option<CoordinatedCommitCursor>,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let unpublished = || {
            match cursor {
            None => no_cursor_for(sink, predecessor),
            Some(cursor) => DbError::Checkpoint(format!(
                "sink '{}' external cursor {} trails committed predecessor {}; recovery must publish the missing cut first",
                sink.name, cursor.checkpoint_id, predecessor.checkpoint_id
            )),
        }
        };
        let store = self.decision_store.as_ref().ok_or_else(unpublished)?;
        let cut = tokio::time::timeout_at(deadline, store.load_committed_checkpoint(predecessor))
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "sink '{}' committed predecessor read timed out",
                    sink.name
                ))
            })?
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "sink '{}' committed predecessor read failed: {error}",
                    sink.name
                ))
            })?;
        if cut.pipeline_identity == namespace.pipeline_identity
            || cut.deployment_id != namespace.deployment_id
        {
            return Err(unpublished());
        }
        let parent_namespace = CoordinatedCommitNamespace::try_new(
            cut.pipeline_identity.clone(),
            cut.deployment_id.clone(),
            sink.name.clone(),
        )
        .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        match read_external_cursor_until(sink, &parent_namespace, deadline).await? {
            Some(parent) if parent.checkpoint_id == predecessor.checkpoint_id => Ok(()),
            Some(parent) => Err(DbError::Checkpoint(format!(
                "sink '{}' parent-topology cursor {} is not the committed migration cut {}",
                sink.name, parent.checkpoint_id, predecessor.checkpoint_id
            ))),
            None if self
                .cut_prepares_sink_until(&sink.name, &cut, deadline)
                .await? =>
            {
                Err(no_cursor_for(sink, predecessor))
            }
            None => Ok(()),
        }
    }

    /// Whether the committed `cut` carries a prepared descriptor for `sink`. Every participant of
    /// a committed checkpoint carries one for each checkpoint-committable sink, so one manifest
    /// says.
    async fn cut_prepares_sink_until(
        &self,
        sink: &str,
        cut: &CommittedCheckpointIndex,
        deadline: tokio::time::Instant,
    ) -> Result<bool, DbError> {
        let participant = cut.participants.first().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "committed checkpoint {} has no participants",
                cut.checkpoint_id
            ))
        })?;
        let manifest = load_committed_manifest_until(
            self.store.as_ref(),
            participant,
            cut.checkpoint_id,
            deadline,
        )
        .await
        .map_err(|reason| DbError::Checkpoint(format!("sink '{sink}' migration cut {reason}")))?;
        Ok(manifest
            .prepared_sinks
            .iter()
            .any(|descriptor| descriptor.sink_name == sink))
    }
}

fn no_cursor_for(sink: &RegisteredSink, predecessor: &CommittedCheckpointRef) -> DbError {
    DbError::Checkpoint(format!(
        "sink '{}' has no external cursor for committed predecessor {}",
        sink.name, predecessor.checkpoint_id
    ))
}

async fn read_external_cursor_until(
    sink: &RegisteredSink,
    namespace: &CoordinatedCommitNamespace,
    deadline: tokio::time::Instant,
) -> Result<Option<CoordinatedCommitCursor>, DbError> {
    tokio::time::timeout_at(deadline, sink.handle.committed_cursor(namespace.clone()))
        .await
        .map_err(|_| {
            DbError::Checkpoint(format!(
                "sink '{}' committed-cursor read timed out",
                sink.name
            ))
        })?
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "sink '{}' committed-cursor read failed: {error}",
                sink.name
            ))
        })
}
