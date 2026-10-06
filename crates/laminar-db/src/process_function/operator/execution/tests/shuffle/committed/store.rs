use super::*;
use crate::checkpoint_coordinator::{CheckpointConfig, CheckpointCoordinator};
use crate::recovery_manager::{RecoveredStateFrame, RecoveryManager};
use bytes::Bytes;
use laminar_core::checkpoint::{
    checkpoint_sha256, ByteRange, ChannelProgress, CheckpointAttempt, CheckpointBarrier,
    CheckpointManifest, CheckpointScope, CheckpointStore, CommittedCheckpointIndex,
    CommittedCheckpointRef, CommittedParticipantRef, ConnectorCheckpoint,
    ObjectStoreCheckpointStore, StateFrame, StateFrameKey, COMMITTED_CHECKPOINT_INDEX_VERSION,
};
use laminar_core::checkpoint_decision::{
    CheckpointArtifactInventory, CheckpointDecisionStore, CheckpointVerdict,
};
use laminar_core::cluster::control::snapshot::{
    AssignmentSnapshot, AssignmentSnapshotStore, RotateOutcome,
};
use laminar_core::cluster::control::AssignmentDrainDecision;
use laminar_core::cluster::control::{LeaderLeaseOwner, LeaderLeaseStore, LeaseOutcome};
use std::collections::{BTreeMap, HashMap};

pub(super) struct SharedCut {
    pub(super) objects: Arc<dyn object_store::ObjectStore>,
    decisions: Arc<CheckpointDecisionStore>,
    pub(super) leader: Arc<LeaderLeaseStore>,
    pub(super) proof: laminar_core::checkpoint::LeaderProof,
    pub(super) reference: CommittedCheckpointRef,
    pub(super) fence: CheckpointAssignmentFence,
    deployment: String,
}

impl SharedCut {
    pub(super) async fn open(objects: Arc<dyn object_store::ObjectStore>) -> Self {
        let decisions = Arc::new(CheckpointDecisionStore::new(Arc::clone(&objects)));
        let leader = Arc::new(LeaderLeaseStore::new(Arc::clone(&objects), 60_000));
        let outcome = leader
            .highest_cluster_committed_outcome()
            .await
            .unwrap()
            .unwrap();
        let reference = outcome.committed_checkpoint.unwrap();
        let index = decisions
            .load_committed_checkpoint(&reference)
            .await
            .unwrap();
        Self {
            objects,
            decisions,
            leader,
            proof: outcome.leader_proof.unwrap(),
            reference,
            fence: index.assignment_fence.unwrap(),
            deployment: index.deployment_id,
        }
    }

    pub(super) async fn publish_target(
        &self,
        predecessor_owners: [u64; 4],
        target: &CheckpointAssignmentFence,
        owners: [u64; 4],
    ) {
        let assignments = AssignmentSnapshotStore::new(Arc::clone(&self.objects));
        let mut snapshot = AssignmentSnapshot {
            version: 1,
            partitioning_abi_version: self.fence.partitioning_abi_version,
            vnodes: AssignmentSnapshot::vnodes_from_vec(&predecessor_owners.map(NodeId)),
            participants: self.fence.participants.clone(),
            updated_at_ms: 0,
            draining: false,
            drain_transition: None,
        };
        assert!(assignments
            .save_if_absent(&snapshot)
            .await
            .unwrap()
            .is_some());
        for version in 2..=self.fence.assignment_version {
            snapshot.version = version;
            assert!(matches!(
                assignments
                    .save_if_version(&snapshot, version - 1)
                    .await
                    .unwrap(),
                RotateOutcome::Rotated
            ));
        }
        let proposal = snapshot
            .next_draining(
                AssignmentSnapshot::vnodes_from_vec(&owners.map(NodeId)),
                target.participants.clone(),
                self.proof.clone(),
            )
            .unwrap();
        assert_eq!(proposal.assignment_fence().unwrap(), *target);
        assert!(matches!(
            self.leader
                .publish_assignment_drain(&self.proof, &assignments, &proposal)
                .await
                .unwrap(),
            RotateOutcome::Rotated
        ));
        let transition = proposal.drain_transition.as_ref().unwrap();
        self.leader
            .record_assignment_drain_decision(
                &self.proof,
                AssignmentDrainDecision::commit(
                    transition,
                    self.proof.clone(),
                    self.reference.clone(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let committed = proposal.committed_target().unwrap();
        assignments
            .finalize_drain(&proposal, &committed)
            .await
            .unwrap();
        let installed = assignments.load().await.unwrap().unwrap();
        assert!(!installed.draining);
        assert_eq!(installed.assignment_fence().unwrap(), *target);
        assert_eq!(
            self.leader
                .assignment_handoff_checkpoint(target)
                .await
                .unwrap(),
            Some(self.reference.clone())
        );
    }

    pub(super) async fn persist(pair: &Pair, graphs: &mut [OperatorGraph]) -> Self {
        let fence = pair.nodes[0].binding.assignment().clone();
        for (index, node) in pair.nodes.iter().enumerate() {
            node.scope
                .sender
                .fan_out_barrier(
                    &[pair.nodes[1 - index].scope.self_id.0],
                    CheckpointBarrier::new(1, 1),
                    &fence,
                )
                .await
                .unwrap();
        }
        for graph in graphs.iter_mut() {
            graph
                .align_shuffle_barriers(
                    CheckpointAttempt::canonical(1),
                    105,
                    &fence,
                    tokio::time::Instant::now() + DEADLINE,
                    None,
                )
                .await
                .unwrap();
            assert!(graph.checkpoint_is_quiescent());
        }
        let writer = CheckpointWriter::begin(Arc::clone(&pair.objects), fence).await;
        let mut manifests = Vec::new();
        for (graph, node) in graphs.iter_mut().zip(&pair.nodes) {
            let (manifest, payload) = capture(graph, node, &writer.deployment);
            let store = checkpoint_store(Arc::clone(&pair.objects), node.scope.self_id.0);
            let encoded = store.save_checkpoint(&manifest, &[payload]).await.unwrap();
            manifests.push((manifest, encoded));
        }
        let cut = writer.commit(manifests).await;
        for node in &pair.nodes {
            node.scope
                .receiver
                .retire_checkpoint_barriers(CheckpointAttempt::canonical(1), cut.fence.digest())
                .unwrap();
        }
        cut
    }

    pub(super) async fn recover(&self, node: &Fixture) -> Result<RecoveredState, DbError> {
        let outcome = self
            .leader
            .highest_cluster_committed_outcome()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.committed_checkpoint.as_ref(), Some(&self.reference));
        let index = self
            .decisions
            .load_committed_checkpoint(&self.reference)
            .await
            .unwrap();
        let store = checkpoint_store(Arc::clone(&self.objects), node.scope.self_id.0);
        let owned_vnodes = node
            .scope
            .registry
            .versioned_snapshot()
            .owners()
            .iter()
            .enumerate()
            .filter_map(|(vnode, owner)| {
                (*owner == node.scope.self_id).then_some(u32::try_from(vnode).unwrap())
            })
            .collect();
        RecoveryManager::new(
            &store,
            &index.pipeline_identity,
            &self.deployment,
            CheckpointScope::Cluster,
        )
        .recover_committed_for_target(
            &outcome,
            &index,
            Some(ClusterRecoveryTarget {
                assignment: node.binding.assignment().clone(),
                owned_vnodes,
                max_graph_payload_bytes: 1024 * 1024,
            }),
        )
        .await
    }

    pub(super) async fn handoff(
        &self,
        node: &Fixture,
        acquired: &[u32],
    ) -> Vec<RecoveredStateFrame> {
        let store = checkpoint_store(Arc::clone(&self.objects), node.scope.self_id.0);
        let mut coordinator =
            CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
        coordinator
            .bind_durable_decision_store(Arc::clone(&self.decisions))
            .await
            .unwrap();
        coordinator
            .bind_pipeline_identity(PipelineIdentity::empty())
            .unwrap();
        coordinator
            .load_handoff_state_frames(
                &self.reference,
                &self.fence,
                &[NodeId(7), NodeId(8), NodeId(7), NodeId(8)],
                acquired,
                true,
                1024 * 1024,
                tokio::time::Instant::now() + DEADLINE,
            )
            .await
            .unwrap()
    }
}

pub(super) struct CheckpointWriter {
    objects: Arc<dyn object_store::ObjectStore>,
    decisions: Arc<CheckpointDecisionStore>,
    leader: Arc<LeaderLeaseStore>,
    proof: laminar_core::checkpoint::LeaderProof,
    pub(super) deployment: String,
    fence: CheckpointAssignmentFence,
}

impl CheckpointWriter {
    pub(super) async fn begin(
        objects: Arc<dyn object_store::ObjectStore>,
        fence: CheckpointAssignmentFence,
    ) -> Self {
        let decisions = Arc::new(CheckpointDecisionStore::new(Arc::clone(&objects)));
        let deployment = decisions.load_or_create_deployment_id().await.unwrap();
        let leader = Arc::new(LeaderLeaseStore::new(Arc::clone(&objects), 60_000));
        let participant = fence.participants.last().unwrap();
        let owner = LeaderLeaseOwner {
            node: NodeId(participant.node_id),
            boot: participant.boot_incarnation,
            process_term: 1,
        };
        let LeaseOutcome::Acquired(lease) = leader.begin_new_term(&owner, 0).await.unwrap() else {
            panic!("fixture leader must acquire its shared CAS term");
        };
        let proof = lease.proof();
        leader
            .begin_cluster_checkpoint_artifacts(
                &proof,
                CheckpointArtifactInventory {
                    deployment_id: deployment.clone(),
                    pipeline_identity: PipelineIdentity::empty(),
                    attempt: CheckpointAttempt::canonical(1),
                    assignment_fence: Some(fence.clone()),
                    sink_artifact_intent_protocol: true,
                },
            )
            .await
            .unwrap();
        Self {
            objects,
            decisions,
            leader,
            proof,
            deployment,
            fence,
        }
    }

    pub(super) async fn commit(self, manifests: Vec<(CheckpointManifest, Bytes)>) -> SharedCut {
        let Self {
            objects,
            decisions,
            leader,
            proof,
            deployment,
            fence,
        } = self;
        let participants = manifests
            .iter()
            .map(|(manifest, encoded)| {
                CommittedParticipantRef::from_manifest(manifest, encoded).unwrap()
            })
            .collect();
        let mut offsets = HashMap::new();
        for (manifest, _) in &manifests {
            offsets.extend(manifest.source_offsets["events"].offsets.clone());
        }
        let index = CommittedCheckpointIndex {
            version: COMMITTED_CHECKPOINT_INDEX_VERSION,
            deployment_id: deployment.clone(),
            pipeline_identity: PipelineIdentity::empty(),
            epoch: 1,
            checkpoint_id: 1,
            predecessor: None,
            scope: CheckpointScope::Cluster,
            vnode_count: 4,
            assignment_fence: Some(fence.clone()),
            reassignment_portable: true,
            participants,
            source_names: vec!["events".into()],
            source_offsets: BTreeMap::from([(
                "events".into(),
                ConnectorCheckpoint::with_offsets(offsets),
            )]),
            channel_progress: manifests
                .iter()
                .flat_map(|(manifest, _)| manifest.channel_progress.clone())
                .collect(),
            source_watermarks: BTreeMap::from([("events".into(), 105)]),
            checkpoint_watermark: Some(105),
        };
        index
            .validate_participant_manifests(
                &manifests
                    .iter()
                    .map(|(manifest, bytes)| (manifest, bytes.as_ref()))
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        let reference = decisions.create_committed_checkpoint(&index).await.unwrap();
        leader
            .record_cluster_outcome(
                &proof,
                1,
                1,
                fence.clone(),
                CheckpointVerdict::Commit,
                Some(reference.clone()),
            )
            .await
            .unwrap();
        let cut = SharedCut {
            objects,
            decisions,
            leader,
            proof,
            reference,
            fence,
            deployment,
        };
        assert_eq!(
            cut.leader
                .highest_cluster_committed_outcome()
                .await
                .unwrap()
                .unwrap()
                .committed_checkpoint,
            Some(cut.reference.clone())
        );
        cut
    }
}

pub(super) fn checkpoint_store(
    objects: Arc<dyn object_store::ObjectStore>,
    participant: u64,
) -> ObjectStoreCheckpointStore {
    ObjectStoreCheckpointStore::new(objects, "process-shared-cut")
        .with_key_group_count(KeyGroupCount::try_from(4_u16).unwrap())
        .with_participant_id(participant)
}

pub(super) fn capture(
    graph: &mut OperatorGraph,
    node: &Fixture,
    deployment: &str,
) -> (CheckpointManifest, Bytes) {
    let mut manifest =
        CheckpointManifest::new_with_key_group_count(1, 1, KeyGroupCount::try_from(4_u16).unwrap());
    manifest.bind_participant(node.scope.self_id.0);
    manifest.deployment_id = deployment.into();
    manifest.pipeline_identity = PipelineIdentity::empty();
    manifest.assignment_fence = Some(node.binding.assignment().clone());
    manifest.reassignment_portable = true;
    manifest.owned_vnodes = node
        .scope
        .registry
        .versioned_snapshot()
        .owners()
        .iter()
        .enumerate()
        .filter_map(|(vnode, owner)| {
            (*owner == node.scope.self_id).then_some(u16::try_from(vnode).unwrap())
        })
        .collect();
    manifest.source_names = vec!["events".into()];
    let partition = format!("partition-{}", node.scope.self_id.0);
    manifest.source_offsets.insert(
        "events".into(),
        ConnectorCheckpoint::with_offsets(HashMap::from([(partition.clone(), "2".into())])),
    );
    manifest.channel_progress.push(ChannelProgress {
        participant_id: node.scope.self_id.0,
        source_name: "events".into(),
        input_channel: partition.into_bytes(),
        watermark: Some(105),
        idle: false,
    });
    manifest.checkpoint_watermark = Some(105);
    let (whole, vnodes) = materialize(graph.capture_state(1024 * 1024).unwrap());
    let mut entries = whole
        .into_iter()
        .map(|(name, payload)| {
            (
                StateFrameKey::OperatorWhole {
                    operator_id: format!("graph:{name}"),
                },
                payload,
            )
        })
        .chain(vnodes.into_iter().map(|(name, vnode, payload)| {
            (
                StateFrameKey::Vnode {
                    operator_id: format!("graph:{name}"),
                    vnode: u16::try_from(vnode).unwrap(),
                },
                payload,
            )
        }))
        .collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let mut data = Vec::new();
    for (key, payload) in entries {
        manifest.state_frames.push(StateFrame {
            key,
            chunk: manifest.node_data.chunk,
            range: ByteRange {
                offset: u64::try_from(data.len()).unwrap(),
                length: u64::try_from(payload.len()).unwrap(),
            },
            sha256: checkpoint_sha256(&payload),
        });
        data.extend_from_slice(&payload);
    }
    manifest.node_data.object_length = u64::try_from(data.len()).unwrap();
    manifest.node_data.sha256 = checkpoint_sha256(&data);
    (manifest, Bytes::from(data))
}
