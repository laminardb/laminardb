use std::collections::BTreeMap;
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use laminar_core::checkpoint::{
    CheckpointAssignmentFence, CheckpointManifest, CheckpointScope, CheckpointStore,
    CommittedCheckpointIndex, CommittedCheckpointRef, CommittedParticipantRef, PipelineIdentity,
    StateFrameKey,
};
use laminar_core::state::NodeId;

use super::{CheckpointCoordinator, MAX_RETENTION_IO_CONCURRENCY};
use crate::error::DbError;
use crate::recovery_manager::{
    load_verified_state_frames, RecoveredStateFrame, VerifiedStateFramePlan,
};

struct ValidatedHandoffCut {
    committed: CommittedCheckpointIndex,
    pipeline_identity: PipelineIdentity,
    deployment_id: String,
}

struct HandoffDonor {
    participant: CommittedParticipantRef,
    requested: Vec<u16>,
    expected: Vec<u16>,
}

fn handoff_error(message: impl Into<String>) -> DbError {
    DbError::Checkpoint(format!("[LDB-6050] {}", message.into()))
}

fn validate_handoff_request(
    store: &dyn CheckpointStore,
    predecessor: &CheckpointAssignmentFence,
    predecessor_owners: &[NodeId],
    acquired_vnodes: &[u32],
    deadline: tokio::time::Instant,
) -> Result<(), DbError> {
    if tokio::time::Instant::now() >= deadline {
        return Err(handoff_error("vnode handoff read timed out"));
    }
    let owner_ids = predecessor_owners
        .iter()
        .map(|owner| owner.0)
        .collect::<Vec<_>>();
    let key_group_count = store.key_group_count();
    if !predecessor.is_canonical()
        || predecessor.vnode_count != u32::from(key_group_count)
        || owner_ids.len() != usize::from(key_group_count.get())
        || !predecessor.matches_owner_map(&owner_ids)
    {
        return Err(handoff_error(
            "predecessor fence does not match the exact vnode owner map",
        ));
    }
    if acquired_vnodes.is_empty()
        || acquired_vnodes.windows(2).any(|pair| pair[0] >= pair[1])
        || acquired_vnodes
            .iter()
            .any(|vnode| *vnode >= predecessor.vnode_count)
    {
        return Err(handoff_error(
            "acquired vnode roster must be nonempty, canonical, and in range",
        ));
    }
    Ok(())
}

impl CheckpointCoordinator {
    async fn load_validated_handoff_cut_until(
        &self,
        pinned: &CommittedCheckpointRef,
        predecessor: &CheckpointAssignmentFence,
        deadline: tokio::time::Instant,
    ) -> Result<ValidatedHandoffCut, DbError> {
        let decision_store = self.decision_store.as_ref().ok_or_else(|| {
            handoff_error("vnode handoff requires a durable checkpoint decision store")
        })?;
        let committed =
            tokio::time::timeout_at(deadline, decision_store.load_committed_checkpoint(pinned))
                .await
                .map_err(|_| handoff_error("committed handoff checkpoint read timed out"))?
                .map_err(|error| {
                    handoff_error(format!("committed handoff checkpoint read failed: {error}"))
                })?;
        let pipeline_identity = self.expected_pipeline_identity()?;
        let deployment_id = self.expected_deployment_id()?.to_owned();
        if committed.pipeline_identity != pipeline_identity {
            return Err(handoff_error(
                "handoff checkpoint pipeline identity does not match the active pipeline",
            ));
        }
        if committed.deployment_id != deployment_id {
            return Err(handoff_error(
                "handoff checkpoint deployment does not match the active deployment",
            ));
        }
        if committed.scope != CheckpointScope::Cluster {
            return Err(handoff_error(
                "vnode handoff requires a cluster-scoped committed checkpoint",
            ));
        }
        if !committed.reassignment_portable
            || committed.assignment_fence.as_ref() != Some(predecessor)
            || u32::from(committed.vnode_count) != predecessor.vnode_count
        {
            return Err(handoff_error(
                "handoff checkpoint is not portable or does not cover the exact predecessor assignment",
            ));
        }
        Ok(ValidatedHandoffCut {
            committed,
            pipeline_identity,
            deployment_id,
        })
    }
}

fn select_handoff_donors(
    committed: &CommittedCheckpointIndex,
    predecessor_owners: &[NodeId],
    acquired_vnodes: &[u32],
) -> Result<Vec<HandoffDonor>, DbError> {
    let mut requested_by_donor = BTreeMap::<u64, Vec<u16>>::new();
    for &vnode in acquired_vnodes {
        let vnode16 = u16::try_from(vnode)
            .map_err(|_| handoff_error(format!("acquired vnode {vnode} exceeds u16")))?;
        let donor = predecessor_owners[vnode as usize].0;
        requested_by_donor.entry(donor).or_default().push(vnode16);
    }
    let mut expected_by_donor = requested_by_donor
        .keys()
        .copied()
        .map(|donor| (donor, Vec::new()))
        .collect::<BTreeMap<_, Vec<u16>>>();
    for (vnode, owner) in predecessor_owners.iter().enumerate() {
        if let Some(expected) = expected_by_donor.get_mut(&owner.0) {
            expected.push(u16::try_from(vnode).map_err(|_| {
                handoff_error("predecessor vnode owner map exceeds the checkpoint ABI")
            })?);
        }
    }

    requested_by_donor
        .into_iter()
        .map(|(participant_id, requested)| {
            let participant = committed
                .participants
                .binary_search_by_key(&participant_id, |entry| entry.participant_id)
                .ok()
                .map(|index| committed.participants[index].clone())
                .ok_or_else(|| {
                    handoff_error(format!(
                        "vnode donor {participant_id} is absent from the committed checkpoint"
                    ))
                })?;
            let expected = expected_by_donor.remove(&participant_id).ok_or_else(|| {
                handoff_error(format!(
                    "vnode donor {participant_id} has no predecessor ownership roster"
                ))
            })?;
            Ok(HandoffDonor {
                participant,
                requested,
                expected,
            })
        })
        .collect()
}

fn validate_handoff_manifest_budget(
    donors: &[HandoffDonor],
    max_payload_bytes: usize,
) -> Result<(), DbError> {
    let manifest_bytes = donors.iter().try_fold(0usize, |total, donor| {
        let bytes = usize::try_from(donor.participant.manifest_len).map_err(|_| {
            DbError::ManagedStateBudgetExceeded {
                context: "[LDB-6050] vnode handoff manifests".into(),
                accounted_bytes: usize::MAX,
                limit_bytes: max_payload_bytes,
            }
        })?;
        total
            .checked_add(bytes)
            .ok_or_else(|| DbError::ManagedStateBudgetExceeded {
                context: "[LDB-6050] vnode handoff manifests".into(),
                accounted_bytes: usize::MAX,
                limit_bytes: max_payload_bytes,
            })
    })?;
    if manifest_bytes > max_payload_bytes {
        return Err(DbError::ManagedStateBudgetExceeded {
            context: "[LDB-6050] vnode handoff manifests".into(),
            accounted_bytes: manifest_bytes,
            limit_bytes: max_payload_bytes,
        });
    }
    Ok(())
}

async fn load_handoff_manifest_until(
    store: &dyn CheckpointStore,
    participant: &CommittedParticipantRef,
    checkpoint_id: u64,
    deadline: tokio::time::Instant,
) -> Result<CheckpointManifest, DbError> {
    let participant_id = participant.participant_id;
    tokio::time::timeout_at(
        deadline,
        store.load_manifest_verified(
            participant_id,
            checkpoint_id,
            participant.manifest_len,
            &participant.manifest_sha256,
        ),
    )
    .await
    .map_err(|_| {
        handoff_error(format!(
            "participant {participant_id} handoff manifest read timed out"
        ))
    })?
    .map_err(|error| {
        handoff_error(format!(
            "participant {participant_id} handoff manifest read failed: {error}"
        ))
    })?
    .ok_or_else(|| {
        handoff_error(format!(
            "participant {participant_id} handoff manifest is missing"
        ))
    })
}

fn validate_handoff_manifest(
    manifest: &CheckpointManifest,
    donor: &HandoffDonor,
    cut: &ValidatedHandoffCut,
    predecessor: &CheckpointAssignmentFence,
) -> Result<(), DbError> {
    let participant = &donor.participant;
    let participant_id = participant.participant_id;
    if manifest.participant_id != participant_id
        || manifest.node_data.chunk.participant_id != participant_id
        || manifest.node_data.object_length != participant.node_data_len
        || manifest.node_data.sha256 != participant.node_data_sha256
        || manifest.epoch != cut.committed.epoch
        || manifest.checkpoint_id != cut.committed.checkpoint_id
        || manifest.deployment_id != cut.deployment_id.as_str()
        || manifest.pipeline_identity != cut.pipeline_identity
        || manifest.vnode_count != cut.committed.vnode_count
        || manifest.assignment_fence.as_ref() != Some(predecessor)
        || manifest.owned_vnodes != donor.expected
    {
        return Err(handoff_error(format!(
            "participant {participant_id} manifest does not match the exact handoff cut"
        )));
    }
    Ok(())
}

fn select_handoff_plan(
    manifest: &CheckpointManifest,
    requested: &[u16],
    include_whole: bool,
    max_payload_bytes: usize,
) -> Result<(VerifiedStateFramePlan, usize), DbError> {
    let selected = manifest
        .state_frames
        .iter()
        .filter(|frame| match &frame.key {
            StateFrameKey::OperatorWhole { operator_id } => {
                include_whole
                    && operator_id
                        .strip_prefix("graph:")
                        .is_some_and(|suffix| !suffix.is_empty())
            }
            StateFrameKey::Vnode { operator_id, vnode } => {
                operator_id
                    .strip_prefix("graph:")
                    .is_some_and(|suffix| !suffix.is_empty())
                    && requested.binary_search(vnode).is_ok()
            }
        })
        .cloned()
        .collect::<Vec<_>>();
    let selected_bytes = selected.iter().try_fold(0usize, |total, frame| {
        let bytes = usize::try_from(frame.range.length).map_err(|_| {
            DbError::ManagedStateBudgetExceeded {
                context: "[LDB-6050] vnode handoff payload".into(),
                accounted_bytes: usize::MAX,
                limit_bytes: max_payload_bytes,
            }
        })?;
        total
            .checked_add(bytes)
            .ok_or_else(|| DbError::ManagedStateBudgetExceeded {
                context: "[LDB-6050] vnode handoff payload".into(),
                accounted_bytes: usize::MAX,
                limit_bytes: max_payload_bytes,
            })
    })?;
    let plan = VerifiedStateFramePlan::new(manifest, &selected)?;
    Ok((plan, selected_bytes))
}

async fn load_handoff_donor_plan_until(
    store: Arc<dyn CheckpointStore>,
    donor: HandoffDonor,
    cut: &ValidatedHandoffCut,
    predecessor: &CheckpointAssignmentFence,
    include_whole: bool,
    max_payload_bytes: usize,
    deadline: tokio::time::Instant,
) -> Result<(VerifiedStateFramePlan, usize), DbError> {
    let manifest = load_handoff_manifest_until(
        store.as_ref(),
        &donor.participant,
        cut.committed.checkpoint_id,
        deadline,
    )
    .await?;
    validate_handoff_manifest(&manifest, &donor, cut, predecessor)?;
    select_handoff_plan(
        &manifest,
        &donor.requested,
        include_whole,
        max_payload_bytes,
    )
}

impl CheckpointCoordinator {
    async fn load_handoff_plans_until(
        &self,
        cut: &ValidatedHandoffCut,
        donors: Vec<HandoffDonor>,
        predecessor: &CheckpointAssignmentFence,
        include_whole: bool,
        max_payload_bytes: usize,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<(VerifiedStateFramePlan, usize)>, DbError> {
        let manifest_reads = donors.into_iter().map(|donor| {
            let store = Arc::clone(&self.store);
            async move {
                load_handoff_donor_plan_until(
                    store,
                    donor,
                    cut,
                    predecessor,
                    include_whole,
                    max_payload_bytes,
                    deadline,
                )
                .await
            }
        });
        futures::stream::iter(manifest_reads)
            .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY)
            .try_collect()
            .await
    }
}

fn validate_handoff_payload_budget(
    loaded: &[(VerifiedStateFramePlan, usize)],
    max_payload_bytes: usize,
) -> Result<(), DbError> {
    let payload_bytes = loaded.iter().try_fold(0usize, |total, (_, bytes)| {
        total
            .checked_add(*bytes)
            .ok_or_else(|| DbError::ManagedStateBudgetExceeded {
                context: "[LDB-6050] vnode handoff payload".into(),
                accounted_bytes: usize::MAX,
                limit_bytes: max_payload_bytes,
            })
    })?;
    if payload_bytes > max_payload_bytes {
        return Err(DbError::ManagedStateBudgetExceeded {
            context: "[LDB-6050] vnode handoff payload".into(),
            accounted_bytes: payload_bytes,
            limit_bytes: max_payload_bytes,
        });
    }
    Ok(())
}

impl CheckpointCoordinator {
    pub(crate) async fn load_handoff_state_frames(
        &self,
        pinned: &CommittedCheckpointRef,
        predecessor: &CheckpointAssignmentFence,
        predecessor_owners: &[NodeId],
        acquired_vnodes: &[u32],
        include_whole: bool,
        max_payload_bytes: usize,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<RecoveredStateFrame>, DbError> {
        validate_handoff_request(
            self.store.as_ref(),
            predecessor,
            predecessor_owners,
            acquired_vnodes,
            deadline,
        )?;
        let cut = self
            .load_validated_handoff_cut_until(pinned, predecessor, deadline)
            .await?;
        let donors = select_handoff_donors(&cut.committed, predecessor_owners, acquired_vnodes)?;
        validate_handoff_manifest_budget(&donors, max_payload_bytes)?;
        let loaded = self
            .load_handoff_plans_until(
                &cut,
                donors,
                predecessor,
                include_whole,
                max_payload_bytes,
                deadline,
            )
            .await?;
        validate_handoff_payload_budget(&loaded, max_payload_bytes)?;
        if tokio::time::Instant::now() >= deadline {
            return Err(handoff_error("vnode handoff read timed out"));
        }

        let plans = loaded.into_iter().map(|(plan, _)| plan).collect();
        tokio::time::timeout_at(
            deadline,
            load_verified_state_frames(self.store.as_ref(), plans),
        )
        .await
        .map_err(|_| handoff_error("vnode handoff frame read timed out"))?
    }
}
