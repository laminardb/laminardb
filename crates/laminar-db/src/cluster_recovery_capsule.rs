//! Assembly and validation of the global cluster recovery image.

#![cfg(feature = "cluster")]
#![allow(clippy::disallowed_types)] // Cold-path durable JSON uses ordered maps.

use std::collections::{BTreeMap, BTreeSet};

use laminar_core::checkpoint::{
    canonical_json_sha256, CheckpointWatermark, ClusterRecoveryCapsule, ParticipantRecoveryRef,
    CLUSTER_RECOVERY_CAPSULE_VERSION, MAX_RECOVERY_CAPSULE_BYTES,
};
use laminar_core::state::{CheckpointAttempt, CheckpointSealInventory};
use laminar_core::storage::checkpoint_manifest::{
    CheckpointManifest, DurableCheckpointPhase, OperatorCheckpoint, PipelineIdentity,
};

use crate::error::DbError;

pub(crate) const PARTICIPANT_READY_VERSION: u16 = 5;
pub(crate) const PARTICIPANT_READY_PREFIX: &str = "participant-ready/v5/participant=";
pub(crate) const MAX_PARTICIPANT_READY_AGGREGATE_BYTES: u64 = MAX_RECOVERY_CAPSULE_BYTES as u64;
pub(crate) const MAX_PARTICIPANT_READY_READ_CONCURRENCY: usize = 8;
// `buffer_unordered` may retain every in-flight body until the aggregate loop is polled.
pub(crate) const MAX_PARTICIPANT_READY_BYTES: u64 =
    MAX_PARTICIPANT_READY_AGGREGATE_BYTES / MAX_PARTICIPANT_READY_READ_CONCURRENCY as u64;

pub(crate) fn checked_participant_ready_total(
    retained_bytes: u64,
    record_bytes: usize,
) -> Result<u64, DbError> {
    let record_bytes = u64::try_from(record_bytes).map_err(|_| {
        DbError::Checkpoint("[LDB-6041] participant readiness length is not representable".into())
    })?;
    if record_bytes > MAX_PARTICIPANT_READY_BYTES {
        return Err(DbError::Checkpoint(format!(
            "[LDB-6041] participant readiness is {record_bytes} bytes; maximum is {MAX_PARTICIPANT_READY_BYTES}"
        )));
    }
    let total = retained_bytes.checked_add(record_bytes).ok_or_else(|| {
        DbError::Checkpoint("[LDB-6041] participant readiness byte total overflowed".into())
    })?;
    if total > MAX_PARTICIPANT_READY_AGGREGATE_BYTES {
        return Err(DbError::Checkpoint(format!(
            "[LDB-6041] sealed participant readiness inventory is {total} bytes; maximum is {MAX_PARTICIPANT_READY_AGGREGATE_BYTES}"
        )));
    }
    Ok(total)
}

/// Durable proof that one participant persisted its complete local contribution to a cut.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ParticipantReady {
    pub(crate) version: u16,
    pub(crate) attempt: CheckpointAttempt,
    pub(crate) participant_id: u64,
    pub(crate) assignment_fence: laminar_core::checkpoint::CheckpointAssignmentFence,
    pub(crate) deployment_id: String,
    pub(crate) pipeline_identity: PipelineIdentity,
    pub(crate) owned_vnodes: Vec<u32>,
    pub(crate) source_offsets: BTreeMap<String, BTreeMap<String, String>>,
    pub(crate) source_metadata: BTreeMap<String, BTreeMap<String, String>>,
    pub(crate) source_assignment_versions: BTreeMap<String, std::num::NonZeroU64>,
    pub(crate) source_watermarks: BTreeMap<String, i64>,
    pub(crate) local_watermark: CheckpointWatermark,
    pub(crate) manifest_sha256: String,
    pub(crate) portable_state_sha256: String,
}

#[derive(serde::Serialize)]
struct PortableManifestState<'a> {
    manifest_version: u32,
    attempt: CheckpointAttempt,
    operator_states: &'a std::collections::HashMap<String, OperatorCheckpoint>,
    state_checksum: &'a Option<String>,
    source_names: &'a [String],
    sink_names: &'a [String],
    pipeline_identity: &'a PipelineIdentity,
    deployment_id: &'a str,
    vnode_count: u16,
}

/// Digests of a persisted manifest after normalizing its publication-only phase.
pub(crate) fn manifest_digests(manifest: &CheckpointManifest) -> Result<(String, String), DbError> {
    if !manifest.table_offsets.is_empty() || manifest.table_store_checkpoint_path.is_some() {
        return Err(DbError::Checkpoint(
            "[LDB-6041] cluster recovery does not admit participant-local reference-table paths or cursors"
                .into(),
        ));
    }

    let mut normalized = manifest.clone();
    normalized.durable_phase = DurableCheckpointPhase::Prepared;
    let manifest_sha256 = canonical_json_sha256(&normalized).map_err(|error| {
        DbError::Checkpoint(format!(
            "[LDB-6041] canonical checkpoint manifest encode failed: {error}"
        ))
    })?;
    let portable = PortableManifestState {
        manifest_version: manifest.version,
        attempt: CheckpointAttempt::new(manifest.epoch, manifest.checkpoint_id),
        operator_states: &manifest.operator_states,
        state_checksum: &manifest.state_checksum,
        source_names: &manifest.source_names,
        sink_names: &manifest.sink_names,
        pipeline_identity: &manifest.pipeline_identity,
        deployment_id: &manifest.deployment_id,
        vnode_count: manifest.vnode_count,
    };
    let portable_state_sha256 = canonical_json_sha256(&portable).map_err(|error| {
        DbError::Checkpoint(format!(
            "[LDB-6041] canonical portable checkpoint state encode failed: {error}"
        ))
    })?;
    Ok((manifest_sha256, portable_state_sha256))
}

#[must_use]
pub(crate) fn participant_ready_key(participant_id: u64) -> String {
    format!("{PARTICIPANT_READY_PREFIX}{participant_id}")
}

pub(crate) fn participant_from_ready_key(key: &str) -> Option<u64> {
    let value = key.strip_prefix(PARTICIPANT_READY_PREFIX)?;
    let participant = value.parse::<u64>().ok()?;
    (participant != 0 && participant.to_string() == value).then_some(participant)
}

fn merge_source_map(
    target: &mut BTreeMap<String, BTreeMap<String, String>>,
    incoming: BTreeMap<String, BTreeMap<String, String>>,
    kind: &str,
    attempt: CheckpointAttempt,
) -> Result<(), DbError> {
    for (source, entries) in incoming {
        let merged = target.entry(source.clone()).or_default();
        for (key, value) in entries {
            if let Some(existing) = merged.insert(key.clone(), value.clone()) {
                if existing != value {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6033] conflicting {kind} for source '{source}' key '{key}' at checkpoint {} epoch {}",
                        attempt.checkpoint_id, attempt.epoch
                    )));
                }
            }
        }
    }
    Ok(())
}

fn merge_source_assignment_versions(
    target: &mut BTreeMap<String, std::num::NonZeroU64>,
    incoming: BTreeMap<String, std::num::NonZeroU64>,
    attempt: CheckpointAttempt,
) -> Result<(), DbError> {
    for (source, version) in incoming {
        match target.get(&source) {
            None => {
                target.insert(source, version);
            }
            Some(existing) if *existing == version => {}
            Some(existing) => {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] participants disagree on source '{source}' assignment version at checkpoint {} epoch {}: {existing} vs {version}",
                    attempt.checkpoint_id, attempt.epoch
                )));
            }
        }
    }
    Ok(())
}

/// Assemble one compact global image from the readiness records admitted by an exact seal.
#[allow(clippy::too_many_lines)]
pub(crate) fn assemble_capsule(
    inventory: &CheckpointSealInventory,
    readiness: Vec<(String, ParticipantReady)>,
    expected_deployment: &str,
    expected_identity: &PipelineIdentity,
    cluster_watermark: CheckpointWatermark,
    recovery_watermark_frontier: Option<i64>,
) -> Result<ClusterRecoveryCapsule, DbError> {
    let attempt = inventory.attempt;
    let fence = inventory.assignment_fence.as_ref().ok_or_else(|| {
        DbError::Checkpoint(format!(
            "[LDB-6041] cluster checkpoint {} seal has no assignment certificate",
            attempt.checkpoint_id
        ))
    })?;
    if !fence.is_canonical() || inventory.assignment_version != fence.assignment_version {
        return Err(DbError::Checkpoint(format!(
            "[LDB-6041] checkpoint {} epoch {} has a non-canonical seal assignment",
            attempt.checkpoint_id, attempt.epoch
        )));
    }
    if fence.vnode_count > laminar_core::state::MAX_KEY_GROUP_COUNT {
        return Err(DbError::Checkpoint(format!(
            "[LDB-6041] checkpoint assignment vnode count {} exceeds the production limit {}",
            fence.vnode_count,
            laminar_core::state::MAX_KEY_GROUP_COUNT
        )));
    }
    if !inventory
        .required_vnodes
        .iter()
        .copied()
        .eq(0..fence.vnode_count)
        || !inventory
            .sealed_partials
            .iter()
            .map(|partial| partial.vnode)
            .eq(inventory.required_vnodes.iter().copied())
    {
        return Err(DbError::Checkpoint(
            "[LDB-6041] checkpoint seal does not exactly cover the certified vnode domain".into(),
        ));
    }

    let expected_participants: BTreeSet<u64> = fence.participant_ids().into_iter().collect();
    let sealed_ready: BTreeSet<u64> = inventory
        .required_descriptors
        .iter()
        .filter_map(|key| participant_from_ready_key(key))
        .collect();
    if sealed_ready != expected_participants || readiness.len() != expected_participants.len() {
        return Err(DbError::Checkpoint(format!(
            "[LDB-6041] sealed readiness participants {sealed_ready:?} do not match assignment {expected_participants:?}"
        )));
    }

    let sealed_vnodes: BTreeSet<u32> = inventory.required_vnodes.iter().copied().collect();

    let mut observed_participants = BTreeSet::new();
    let mut observed_vnodes = BTreeSet::new();
    let mut observed_owners = BTreeMap::new();
    let mut source_offsets = BTreeMap::new();
    let mut source_metadata = BTreeMap::new();
    let mut source_assignment_versions = BTreeMap::new();
    let mut source_assignment_presence = BTreeMap::<String, bool>::new();
    let mut source_watermarks = BTreeMap::<String, i64>::new();
    let mut participant_watermark = None;
    let mut participants = Vec::with_capacity(readiness.len());
    let mut portable_state_sha256 = None;

    for (key, ready) in readiness {
        let key_participant = participant_from_ready_key(&key).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "[LDB-6041] invalid participant readiness key '{key}'"
            ))
        })?;
        let readiness_sha256 = canonical_json_sha256(&ready).map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6041] participant {} readiness digest failed: {error}",
                ready.participant_id
            ))
        })?;
        let mut canonical_vnodes = ready.owned_vnodes.clone();
        canonical_vnodes.sort_unstable();
        canonical_vnodes.dedup();
        if ready.version != PARTICIPANT_READY_VERSION
            || ready.attempt != attempt
            || ready.participant_id != key_participant
            || ready.assignment_fence != *fence
            || ready.deployment_id != expected_deployment
            || ready.pipeline_identity != *expected_identity
            || canonical_vnodes != ready.owned_vnodes
            || ready.source_offsets.keys().ne(ready.source_metadata.keys())
            || ready
                .source_assignment_versions
                .keys()
                .any(|source| !ready.source_offsets.contains_key(source))
            || ready
                .source_assignment_versions
                .values()
                .any(|version| version.get() != fence.assignment_version)
            || !observed_participants.insert(ready.participant_id)
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] participant readiness marker '{key}' does not match its sealed cut"
            )));
        }
        if ready.local_watermark.validate().is_err()
            || ready
                .source_watermarks
                .values()
                .any(|value| *value == i64::MIN)
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] participant {} readiness contains a reserved or invalid watermark",
                ready.participant_id
            )));
        }
        for source in ready.source_offsets.keys() {
            let assignment_bound = ready.source_assignment_versions.contains_key(source);
            match source_assignment_presence.get(source) {
                None => {
                    source_assignment_presence.insert(source.clone(), assignment_bound);
                }
                Some(expected) if *expected == assignment_bound => {}
                Some(_) => {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6041] participants disagree on whether source '{source}' is assignment-bound at checkpoint {} epoch {}",
                        attempt.checkpoint_id, attempt.epoch
                    )));
                }
            }
        }
        for vnode in ready.owned_vnodes {
            if vnode >= fence.vnode_count {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] participant {} claims out-of-range vnode {vnode}",
                    ready.participant_id
                )));
            }
            if !observed_vnodes.insert(vnode) {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] vnode {vnode} is claimed by multiple checkpoint participants"
                )));
            }
            observed_owners.insert(vnode, ready.participant_id);
        }
        merge_source_map(
            &mut source_offsets,
            ready.source_offsets,
            "handoff offset",
            attempt,
        )?;
        merge_source_map(
            &mut source_metadata,
            ready.source_metadata,
            "handoff metadata",
            attempt,
        )?;
        merge_source_assignment_versions(
            &mut source_assignment_versions,
            ready.source_assignment_versions,
            attempt,
        )?;
        for (source, watermark) in ready.source_watermarks {
            source_watermarks
                .entry(source)
                .and_modify(|current| *current = (*current).min(watermark))
                .or_insert(watermark);
        }
        participant_watermark = Some(
            participant_watermark.map_or(ready.local_watermark, |current: CheckpointWatermark| {
                current.cluster_min(ready.local_watermark)
            }),
        );
        match portable_state_sha256.as_ref() {
            None => portable_state_sha256 = Some(ready.portable_state_sha256.clone()),
            Some(expected) if expected == &ready.portable_state_sha256 => {}
            Some(_) => {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] participant {} has divergent non-vnode checkpoint state",
                    ready.participant_id
                )));
            }
        }
        participants.push(ParticipantRecoveryRef {
            participant_id: ready.participant_id,
            readiness_sha256,
            manifest_sha256: ready.manifest_sha256,
            portable_state_sha256: ready.portable_state_sha256,
        });
    }

    if observed_participants != expected_participants || observed_vnodes != sealed_vnodes {
        return Err(DbError::Checkpoint(
            "[LDB-6041] participant readiness inventory does not cover the exact sealed assignment"
                .to_string(),
        ));
    }
    let owners: Vec<u64> = (0..fence.vnode_count)
        .map(|vnode| {
            observed_owners.get(&vnode).copied().ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] checkpoint readiness is missing assignment owner for vnode {vnode}"
                ))
            })
        })
        .collect::<Result<_, _>>()?;
    if !fence.matches_owner_map(&owners) {
        return Err(DbError::Checkpoint(
            "[LDB-6041] participant readiness owner map does not match the exact assignment certificate"
                .into(),
        ));
    }
    if inventory.sealed_partials.len() != owners.len() {
        return Err(DbError::Checkpoint(format!(
            "[LDB-6041] checkpoint seal has {} vnode attestations for {} certified owners",
            inventory.sealed_partials.len(),
            owners.len()
        )));
    }
    for partial in &inventory.sealed_partials {
        let expected_owner = owners
            .get(usize::try_from(partial.vnode).unwrap_or(usize::MAX))
            .copied()
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] checkpoint seal contains out-of-range vnode {}",
                    partial.vnode
                ))
            })?;
        let writer = partial.writer.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "[LDB-6041] checkpoint vnode {} has no certified cluster writer",
                partial.vnode
            ))
        })?;
        if partial.assignment_version != fence.assignment_version
            || writer.node_id != expected_owner
            || !writer.matches_fence(fence)
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] checkpoint vnode {} writer does not match its certified assignment owner",
                partial.vnode
            )));
        }
    }
    participants.sort_unstable_by_key(|participant| participant.participant_id);
    let observed_watermark = participant_watermark.unwrap_or_default();
    if observed_watermark != cluster_watermark {
        return Err(DbError::Checkpoint(format!(
            "[LDB-6041] readiness watermark {observed_watermark:?} does not match capture quorum {cluster_watermark:?}"
        )));
    }
    let portable_state_sha256 = portable_state_sha256
        .ok_or_else(|| DbError::Checkpoint("[LDB-6041] empty portable state inventory".into()))?;
    let seal_inventory_sha256 = canonical_json_sha256(inventory).map_err(|error| {
        DbError::Checkpoint(format!(
            "[LDB-6041] canonical seal inventory encode failed: {error}"
        ))
    })?;
    let capsule = ClusterRecoveryCapsule {
        version: CLUSTER_RECOVERY_CAPSULE_VERSION,
        attempt,
        deployment_id: expected_deployment.to_owned(),
        pipeline_identity: expected_identity.clone(),
        assignment_fence: fence.clone(),
        seal_inventory_sha256,
        participants,
        source_offsets,
        source_metadata,
        source_assignment_versions,
        source_watermarks,
        cluster_watermark,
        recovery_watermark_frontier,
        portable_state_sha256,
    };
    capsule
        .validate()
        .map_err(|error| DbError::Checkpoint(format!("[LDB-6041] {error}")))?;
    Ok(capsule)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use laminar_core::checkpoint::{
        CheckpointAssignmentFence, CheckpointParticipant, LeaderProof, LeaderProofOwner,
    };
    use laminar_core::state::{CheckpointAttempt, InProcessBackend, StateBackend};

    use super::*;

    #[test]
    fn readiness_budget_rejects_one_oversized_record() {
        assert!(MAX_PARTICIPANT_READY_BYTES < MAX_PARTICIPANT_READY_AGGREGATE_BYTES);
        assert_eq!(
            MAX_PARTICIPANT_READY_BYTES * MAX_PARTICIPANT_READY_READ_CONCURRENCY as u64,
            MAX_PARTICIPANT_READY_AGGREGATE_BYTES
        );
        let oversized = usize::try_from(MAX_PARTICIPANT_READY_BYTES + 1).unwrap();

        let error = checked_participant_ready_total(0, oversized).unwrap_err();

        assert!(error.to_string().contains("participant readiness is"));
    }

    #[test]
    fn readiness_budget_rejects_many_individually_valid_records() {
        const RECORD_COUNT: u64 = 16;
        let record_bytes = MAX_PARTICIPANT_READY_AGGREGATE_BYTES / RECORD_COUNT;
        assert!(record_bytes < MAX_PARTICIPANT_READY_BYTES);
        let record_bytes = usize::try_from(record_bytes).unwrap();
        let mut retained = 0;
        for _ in 0..RECORD_COUNT {
            retained = checked_participant_ready_total(retained, record_bytes).unwrap();
        }
        assert_eq!(retained, MAX_PARTICIPANT_READY_AGGREGATE_BYTES);

        let error = checked_participant_ready_total(retained, record_bytes).unwrap_err();

        assert!(error
            .to_string()
            .contains("sealed participant readiness inventory"));
        assert_eq!(retained, MAX_PARTICIPANT_READY_AGGREGATE_BYTES);
    }

    #[test]
    fn source_assignment_versions_merge_only_one_generation() {
        let attempt = CheckpointAttempt::new(4, 4);
        let mut merged = BTreeMap::new();
        let version = std::num::NonZeroU64::new(7).unwrap();
        merge_source_assignment_versions(
            &mut merged,
            BTreeMap::from([("events".into(), version)]),
            attempt,
        )
        .unwrap();
        merge_source_assignment_versions(
            &mut merged,
            BTreeMap::from([("events".into(), version)]),
            attempt,
        )
        .unwrap();

        let error = merge_source_assignment_versions(
            &mut merged,
            BTreeMap::from([("events".into(), std::num::NonZeroU64::new(8).unwrap())]),
            attempt,
        )
        .unwrap_err();
        assert!(error.to_string().contains("7 vs 8"), "{error}");
    }

    fn fence() -> CheckpointAssignmentFence {
        CheckpointAssignmentFence::from_owner_map(
            7,
            &[1, 2],
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: uuid::Uuid::from_u128(11),
                },
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::from_u128(22),
                },
            ],
        )
        .unwrap()
    }

    fn ready(
        attempt: CheckpointAttempt,
        fence: &CheckpointAssignmentFence,
        participant_id: u64,
        owned_vnodes: Vec<u32>,
    ) -> ParticipantReady {
        ParticipantReady {
            version: PARTICIPANT_READY_VERSION,
            attempt,
            participant_id,
            assignment_fence: fence.clone(),
            deployment_id: "deployment".into(),
            pipeline_identity: PipelineIdentity::empty(),
            owned_vnodes,
            source_offsets: BTreeMap::new(),
            source_metadata: BTreeMap::new(),
            source_assignment_versions: BTreeMap::new(),
            source_watermarks: BTreeMap::new(),
            local_watermark: CheckpointWatermark::Uninitialized,
            manifest_sha256: format!("{participant_id:064x}"),
            portable_state_sha256: "11".repeat(32),
        }
    }

    fn leader_proof(fence: &CheckpointAssignmentFence) -> LeaderProof {
        LeaderProof {
            owner: LeaderProofOwner {
                node_id: 1,
                boot_id: fence.participant_incarnation(1).unwrap(),
                process_term: 1,
            },
            fencing_token: 1,
        }
    }

    async fn sealed_inventory(
        attempt: CheckpointAttempt,
        fence: &CheckpointAssignmentFence,
        vnode_writers: [(u32, u64); 2],
    ) -> CheckpointSealInventory {
        let backend = InProcessBackend::new(2);
        for (vnode, writer) in vnode_writers {
            backend
                .write_certified_partial(
                    attempt,
                    vnode,
                    fence,
                    writer,
                    Bytes::from_static(b"state"),
                )
                .await
                .unwrap();
        }
        let keys = [participant_ready_key(1), participant_ready_key(2)];
        let proof = leader_proof(fence);
        for (participant_id, key) in [1, 2].into_iter().zip(&keys) {
            backend
                .write_certified_commit_descriptor(
                    attempt,
                    key,
                    fence,
                    participant_id,
                    &proof,
                    Bytes::from_static(b"ready"),
                )
                .await
                .unwrap();
        }
        assert!(backend
            .seal_checkpoint(attempt, Some(fence), &[0, 1], &keys)
            .await
            .unwrap());
        backend
            .checkpoint_seal_inventory(attempt)
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn assembly_rejects_mixed_source_assignment_presence() {
        let attempt = CheckpointAttempt::new(30, 30);
        let fence = fence();
        let inventory = sealed_inventory(attempt, &fence, [(0, 1), (1, 2)]).await;
        let version = std::num::NonZeroU64::new(fence.assignment_version).unwrap();

        for stamped_participant in [1, 2] {
            let readiness = [1_u64, 2]
                .into_iter()
                .map(|participant_id| {
                    let mut ready = ready(
                        attempt,
                        &fence,
                        participant_id,
                        vec![u32::try_from(participant_id - 1).unwrap()],
                    );
                    ready.source_offsets.insert(
                        "events".into(),
                        BTreeMap::from([(
                            format!("partition:{}", participant_id - 1),
                            "41".into(),
                        )]),
                    );
                    ready.source_metadata.insert(
                        "events".into(),
                        BTreeMap::from([("connector".into(), "partitioned".into())]),
                    );
                    if participant_id == stamped_participant {
                        ready
                            .source_assignment_versions
                            .insert("events".into(), version);
                    }
                    (participant_ready_key(participant_id), ready)
                })
                .collect();

            let error = assemble_capsule(
                &inventory,
                readiness,
                "deployment",
                &PipelineIdentity::empty(),
                CheckpointWatermark::Uninitialized,
                None,
            )
            .expect_err("one participant cannot lend its assignment binding to another");
            assert!(
                error.to_string().contains(
                    "participants disagree on whether source 'events' is assignment-bound"
                ),
                "{error}"
            );
        }
    }

    #[tokio::test]
    async fn assembly_rejects_forged_vnode_owner() {
        let attempt = CheckpointAttempt::new(30, 30);
        let fence = fence();
        // Node 2 forges vnode 0 even though the certified owner map assigns it to node 1.
        let inventory = sealed_inventory(attempt, &fence, [(0, 2), (1, 2)]).await;
        let keys = [participant_ready_key(1), participant_ready_key(2)];
        let readiness = vec![
            (keys[0].clone(), ready(attempt, &fence, 1, vec![0])),
            (keys[1].clone(), ready(attempt, &fence, 2, vec![1])),
        ];

        let error = assemble_capsule(
            &inventory,
            readiness,
            "deployment",
            &PipelineIdentity::empty(),
            CheckpointWatermark::Uninitialized,
            None,
        )
        .expect_err("a roster member cannot write another node's vnode");
        assert!(
            error
                .to_string()
                .contains("vnode 0 writer does not match its certified assignment owner"),
            "{error}"
        );
    }
}
