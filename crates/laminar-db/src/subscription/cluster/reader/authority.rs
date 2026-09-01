//! Authoritative committed-checkpoint selection and continuity.

use std::collections::BTreeMap;
use std::sync::Arc;

use laminar_core::checkpoint::{
    CheckpointStore, CommittedCheckpointIndex, CommittedCheckpointRef,
    OutputDistributionCertificate, OutputPartitionId, PartitionSequence,
};
use laminar_core::cluster::control::{ClusterCheckpointAuthorityError, LeaderLeaseStore};

use super::{GATEWAY_IO_TIMEOUT, MAX_GATEWAY_CATCHUP_CHECKPOINTS};
use crate::error::DbError;
use crate::subscription::cluster::manifest::{load_checkpoint, LoadedCheckpoint, LoadedStreamCut};
use crate::subscription::{ClusterSubscriptionError, SubscribeStart};

pub(super) struct GatewayCursor {
    pub(super) current: Option<CommittedCheckpointRef>,
    pub(super) current_index: Option<CommittedCheckpointIndex>,
    pub(super) expected: BTreeMap<OutputPartitionId, PartitionSequence>,
    pub(super) generation_seen: bool,
    pub(super) delivery_sequence: u64,
}

impl GatewayCursor {
    pub(super) async fn open(
        authority: &LeaderLeaseStore,
        store: &Arc<dyn CheckpointStore>,
        certificate: &OutputDistributionCertificate,
        start: SubscribeStart,
    ) -> Result<Self, DbError> {
        let mut cursor = Self {
            current: None,
            current_index: None,
            expected: initial_frontiers(certificate)?,
            generation_seen: false,
            delivery_sequence: 0,
        };
        match start {
            SubscribeStart::Tail => {
                let Some(outcome) = authority
                    .highest_cluster_committed_outcome()
                    .await
                    .map_err(map_authority_error)?
                else {
                    return Ok(cursor);
                };
                let loaded =
                    load_outcome_checkpoint(authority, store, certificate, &outcome).await?;
                cursor.attach_without_replay(loaded)?;
            }
            SubscribeStart::AsOfEpoch(epoch) => {
                cursor
                    .attach_as_of(authority, store, certificate, epoch)
                    .await?;
            }
        }
        Ok(cursor)
    }

    async fn attach_as_of(
        &mut self,
        authority: &LeaderLeaseStore,
        store: &Arc<dyn CheckpointStore>,
        certificate: &OutputDistributionCertificate,
        epoch: u64,
    ) -> Result<(), DbError> {
        let boundary = authority
            .cluster_outcome_retention_boundary()
            .await
            .map_err(map_authority_error)?;
        if epoch < boundary.artifact_before_epoch {
            return Err(ClusterSubscriptionError::ReplayPruned { requested: epoch }.into());
        }
        let Some((outcome, index)) = authority
            .cluster_outcome_with_committed_checkpoint(epoch)
            .await
            .map_err(map_authority_error)?
        else {
            let error = if epoch < boundary.terminal_before_epoch {
                ClusterSubscriptionError::ReplayPruned { requested: epoch }
            } else {
                ClusterSubscriptionError::EpochNotCommitted { requested: epoch }
            };
            return Err(error.into());
        };
        let Some(index) = index else {
            return Err(ClusterSubscriptionError::EpochNotCommitted { requested: epoch }.into());
        };
        let loaded = load_index_for_outcome(store, certificate, &outcome, index).await?;
        if loaded.stream.is_none() {
            return Err(ClusterSubscriptionError::GenerationMismatch.into());
        }
        self.attach_without_replay(loaded)
    }

    fn attach_without_replay(&mut self, loaded: LoadedCheckpoint) -> Result<(), DbError> {
        tracing::debug!(
            selected_epoch = loaded.index.epoch,
            selected_checkpoint_id = loaded.index.checkpoint_id,
            partitions = loaded
                .stream
                .as_ref()
                .map_or(0, |stream| stream.ranges.len()),
            "selected committed cluster subscription checkpoint"
        );
        if let Some(stream) = loaded.stream.as_ref() {
            replace_frontiers(&mut self.expected, stream)?;
            self.generation_seen = true;
        }
        self.current = Some(loaded.reference);
        self.current_index = Some(loaded.index);
        Ok(())
    }
}

pub(super) async fn next_committed_indexes(
    authority: &LeaderLeaseStore,
    cursor: &GatewayCursor,
) -> Result<Vec<CommittedCheckpointIndex>, DbError> {
    let Some(latest) = tokio::time::timeout(
        GATEWAY_IO_TIMEOUT,
        authority.highest_cluster_committed_outcome(),
    )
    .await
    .map_err(|_| ClusterSubscriptionError::BackendUnavailable)?
    .map_err(map_authority_error)?
    else {
        return Ok(Vec::new());
    };
    let latest_ref = latest.committed_checkpoint.as_ref().ok_or_else(|| {
        manifest_error("latest cluster Commit has no committed checkpoint reference")
    })?;
    if cursor.current.as_ref() == Some(latest_ref) {
        return Ok(Vec::new());
    }
    if cursor
        .current
        .as_ref()
        .is_some_and(|current| current.epoch >= latest_ref.epoch)
    {
        return Err(manifest_error(
            "cluster committed checkpoint authority regressed",
        ));
    }

    let mut reverse = Vec::new();
    let mut reference = latest_ref.clone();
    loop {
        if reverse.len() == MAX_GATEWAY_CATCHUP_CHECKPOINTS {
            return Err(ClusterSubscriptionError::SubscriberLagged.into());
        }
        let index = tokio::time::timeout(
            GATEWAY_IO_TIMEOUT,
            authority.load_committed_checkpoint(&reference),
        )
        .await
        .map_err(|_| ClusterSubscriptionError::BackendUnavailable)?
        .map_err(map_authority_error)?;
        let predecessor = index.predecessor.clone();
        reverse.push(index);
        match (cursor.current.as_ref(), predecessor) {
            (Some(current), Some(predecessor)) if &predecessor == current => break,
            (Some(_) | None, Some(predecessor)) => reference = predecessor,
            (Some(_), None) => return Err(ClusterSubscriptionError::RetentionLost.into()),
            (None, None) => break,
        }
    }
    reverse.reverse();
    validate_index_chain(cursor.current_index.as_ref(), &reverse)?;
    Ok(reverse)
}

fn validate_index_chain(
    predecessor: Option<&CommittedCheckpointIndex>,
    indexes: &[CommittedCheckpointIndex],
) -> Result<(), DbError> {
    let mut predecessor = predecessor;
    for index in indexes {
        match predecessor {
            Some(previous) => index
                .validate_predecessor_index(previous)
                .map_err(manifest_error)?,
            None if index.predecessor.is_none() => {}
            None => return Err(ClusterSubscriptionError::RetentionLost.into()),
        }
        predecessor = Some(index);
    }
    Ok(())
}

async fn load_outcome_checkpoint(
    authority: &LeaderLeaseStore,
    store: &Arc<dyn CheckpointStore>,
    certificate: &OutputDistributionCertificate,
    outcome: &laminar_core::checkpoint_decision::CheckpointOutcome,
) -> Result<LoadedCheckpoint, DbError> {
    let reference = outcome
        .committed_checkpoint
        .as_ref()
        .ok_or_else(|| manifest_error("cluster Commit has no committed checkpoint reference"))?;
    let index = authority
        .load_committed_checkpoint(reference)
        .await
        .map_err(map_authority_error)?;
    load_index_for_outcome(store, certificate, outcome, index).await
}

async fn load_index_for_outcome(
    store: &Arc<dyn CheckpointStore>,
    certificate: &OutputDistributionCertificate,
    outcome: &laminar_core::checkpoint_decision::CheckpointOutcome,
    index: CommittedCheckpointIndex,
) -> Result<LoadedCheckpoint, DbError> {
    let expected = outcome
        .committed_checkpoint
        .as_ref()
        .ok_or_else(|| manifest_error("cluster Commit has no committed checkpoint reference"))?;
    let loaded = load_checkpoint(store, index, certificate).await?;
    if &loaded.reference != expected {
        return Err(manifest_error(
            "committed checkpoint does not match its authoritative outcome",
        ));
    }
    Ok(loaded)
}

fn initial_frontiers(
    certificate: &OutputDistributionCertificate,
) -> Result<BTreeMap<OutputPartitionId, PartitionSequence>, DbError> {
    (0..certificate.distribution.partition_count())
        .map(|partition| Ok((OutputPartitionId::new(partition), PartitionSequence::FIRST)))
        .collect()
}

pub(super) fn replace_frontiers(
    expected: &mut BTreeMap<OutputPartitionId, PartitionSequence>,
    stream: &LoadedStreamCut,
) -> Result<(), DbError> {
    if stream.manifest.frontiers.len() != expected.len() {
        return Err(manifest_error(
            "committed stream frontier roster is incomplete",
        ));
    }
    for (frontier, partition) in stream.manifest.frontiers.iter().zip(expected.keys()) {
        if frontier.partition != *partition {
            return Err(manifest_error(
                "committed stream frontier roster is noncanonical",
            ));
        }
    }
    for frontier in &stream.manifest.frontiers {
        expected.insert(frontier.partition, frontier.through_sequence);
    }
    Ok(())
}

pub(super) fn map_authority_error(error: ClusterCheckpointAuthorityError) -> DbError {
    match error {
        ClusterCheckpointAuthorityError::Decision(error) => manifest_error(error.to_string()),
        _ => ClusterSubscriptionError::BackendUnavailable.into(),
    }
}

fn manifest_error(reason: impl Into<String>) -> DbError {
    ClusterSubscriptionError::ManifestCorrupt {
        reason: reason.into(),
    }
    .into()
}

pub(super) fn into_subscription_error(error: DbError) -> ClusterSubscriptionError {
    match error {
        DbError::Subscription(error) => error,
        DbError::CheckpointStore(laminar_core::checkpoint::CheckpointStoreError::ObjectStore(
            _,
        )) => ClusterSubscriptionError::BackendUnavailable,
        other => ClusterSubscriptionError::ManifestCorrupt {
            reason: other.to_string(),
        },
    }
}
