use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use laminar_core::checkpoint::{
    checkpoint_manifest_bytes, checkpoint_sha256, ByteRange, ChangelogMode,
    CheckpointAssignmentFence, CheckpointAttempt, CheckpointManifest, CheckpointParticipant,
    CheckpointStore, CommittedCheckpointIndex, CommittedCheckpointRef, CommittedParticipantRef,
    LeaderProof, NodePartitionRange, NodeSubscriptionManifest, NodeSubscriptionStreamManifest,
    ObjectStoreCheckpointStore, OutputDistribution, OutputDistributionCertificate,
    OutputPartitionId, OutputSegmentRef, PartitionSequence, PipelineIdentity, StateFrame,
    StateFrameKey, StreamGeneration, SubscriptionDigest, SubscriptionProtocolVersion,
    COMMITTED_CHECKPOINT_INDEX_VERSION, OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
};
use laminar_core::checkpoint_decision::{
    CheckpointArtifactInventory, CheckpointDecisionStore, CheckpointScope, CheckpointVerdict,
};
use laminar_core::cluster::control::{LeaderLeaseOwner, LeaderLeaseStore, LeaseOutcome};
use laminar_core::cluster::discovery::NodeId;
use laminar_core::state::{KeyGroupCount, PARTITIONING_ABI_VERSION};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use uuid::Uuid;

use super::super::{
    encode_output_segment, ClusterReaderFrame, ClusterReaderRead, ClusterSubscriptionReader,
    OutputSegmentIdentity, OutputWriterAuthority,
};
use crate::error::DbError;
use crate::subscription::{ClusterSubscriptionError, SubscribeStart};

const PARTICIPANT_ID: u64 = 1;
const VNODE_COUNT: u16 = 2;
const VNODE_COUNT_USIZE: usize = 2;
const MULTI_VNODE_COUNT: u16 = 6;
const MULTI_VNODE_COUNT_USIZE: usize = 6;
const MULTI_OWNER_MAP: [u64; MULTI_VNODE_COUNT_USIZE] = [1, 1, 2, 2, 3, 3];

struct GatewayFixture {
    objects: Arc<dyn ObjectStore>,
    authority: Arc<LeaderLeaseStore>,
    store: Arc<dyn CheckpointStore>,
    proof: LeaderProof,
    participant: CheckpointParticipant,
    assignment: CheckpointAssignmentFence,
    certificate: Arc<OutputDistributionCertificate>,
    deployment_id: String,
    predecessor: Option<CommittedCheckpointRef>,
    frontiers: [PartitionSequence; VNODE_COUNT_USIZE],
}

impl GatewayFixture {
    async fn new() -> Self {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let decisions = CheckpointDecisionStore::new(Arc::clone(&objects));
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&objects), 30_000));
        let owner = LeaderLeaseOwner {
            node: NodeId(PARTICIPANT_ID),
            boot: Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
            process_term: 1,
        };
        let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap()
        else {
            panic!("fresh authority must be acquired");
        };
        let participant = CheckpointParticipant {
            node_id: PARTICIPANT_ID,
            boot_incarnation: owner.boot,
        };
        let assignment = CheckpointAssignmentFence::from_owner_map(
            7,
            &[PARTICIPANT_ID; VNODE_COUNT_USIZE],
            vec![participant],
        )
        .unwrap();
        let schema = value_schema();
        let certificate = Arc::new(OutputDistributionCertificate {
            version: OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            stream_id: "positions".into(),
            catalog_generation: 1,
            stream_generation: StreamGeneration::from_digest(SubscriptionDigest::from_bytes(
                [1; 32],
            )),
            final_operator_id: "stream:positions".into(),
            distribution: OutputDistribution::VnodePartitioned {
                key_expressions_fingerprint: SubscriptionDigest::from_bytes([2; 32]),
                partition_abi: PARTITIONING_ABI_VERSION,
                vnode_count: VNODE_COUNT,
            },
            schema_fingerprint: crate::pipeline_identity::subscription_schema_fingerprint(
                schema.as_ref(),
            )
            .unwrap(),
            changelog_mode: ChangelogMode::WeightedRetractInsert,
            history_retention_bytes: 1024 * 1024,
            query_fingerprint: SubscriptionDigest::from_bytes([3; 32]),
            pipeline_identity: PipelineIdentity::empty(),
        });
        let store = checkpoint_store(Arc::clone(&objects));
        Self {
            objects,
            authority,
            store,
            proof: lease.proof(),
            participant,
            assignment,
            certificate,
            deployment_id,
            predecessor: None,
            frontiers: [PartitionSequence::FIRST; VNODE_COUNT_USIZE],
        }
    }

    fn fresh_store(&self) -> Arc<dyn CheckpointStore> {
        checkpoint_store(Arc::clone(&self.objects))
    }

    fn fresh_authority(&self) -> Arc<LeaderLeaseStore> {
        Arc::new(LeaderLeaseStore::new(Arc::clone(&self.objects), 30_000))
    }

    async fn commit(
        &mut self,
        epoch: u64,
        partition_values: [Vec<i64>; VNODE_COUNT_USIZE],
    ) -> Vec<OutputSegmentRef> {
        let attempt = CheckpointAttempt::canonical(epoch);
        self.authority
            .begin_cluster_checkpoint_artifacts(
                &self.proof,
                CheckpointArtifactInventory {
                    deployment_id: self.deployment_id.clone(),
                    pipeline_identity: PipelineIdentity::empty(),
                    attempt,
                    assignment_fence: Some(self.assignment.clone()),
                },
            )
            .await
            .unwrap();

        let mut ranges = Vec::with_capacity(usize::from(VNODE_COUNT));
        let mut segments = Vec::new();
        for (partition, values) in partition_values.into_iter().enumerate() {
            let partition = u16::try_from(partition).unwrap();
            let first_sequence = self.frontiers[usize::from(partition)];
            let frames = values.into_iter().map(value_batch).collect::<Vec<_>>();
            if !frames.is_empty() {
                let identity = OutputSegmentIdentity {
                    deployment_id: &self.deployment_id,
                    stream_id: &self.certificate.stream_id,
                    stream_generation: self.certificate.stream_generation,
                    partition: OutputPartitionId::new(partition),
                    schema_fingerprint: self.certificate.schema_fingerprint,
                    attempt,
                    authority: OutputWriterAuthority {
                        participant: self.participant,
                        process_term: 1,
                        assignment_version: self.assignment.assignment_version,
                        assignment_digest: self.assignment.digest(),
                    },
                };
                let encoded = encode_output_segment(&identity, &frames, first_sequence).unwrap();
                self.store
                    .save_subscription_segment(&encoded.reference, encoded.bytes)
                    .await
                    .unwrap();
                self.frontiers[usize::from(partition)] = encoded.reference.exclusive_end_sequence;
                segments.push(encoded.reference);
            }
            ranges.push(NodePartitionRange {
                partition: OutputPartitionId::new(partition),
                first_sequence,
                through_sequence: self.frontiers[usize::from(partition)],
            });
        }
        let mut output = NodeSubscriptionManifest {
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            epoch,
            checkpoint_id: epoch,
            participant_id: PARTICIPANT_ID,
            assignment_certificate: self.assignment.clone(),
            streams: vec![NodeSubscriptionStreamManifest {
                distribution_certificate: (*self.certificate).clone(),
                ranges,
                segments: segments.clone(),
            }],
            manifest_digest: SubscriptionDigest::from_bytes([0; 32]),
        };
        output.seal(&[0, 1]).unwrap();
        let (manifest, payload) = self.checkpoint_manifest(epoch, output);
        let encoded_manifest = self
            .store
            .save_checkpoint(&manifest, std::slice::from_ref(&payload))
            .await
            .unwrap();
        let participant =
            CommittedParticipantRef::from_manifest(&manifest, &encoded_manifest).unwrap();
        let index = CommittedCheckpointIndex {
            version: COMMITTED_CHECKPOINT_INDEX_VERSION,
            deployment_id: self.deployment_id.clone(),
            pipeline_identity: PipelineIdentity::empty(),
            epoch,
            checkpoint_id: epoch,
            scope: CheckpointScope::Cluster,
            vnode_count: VNODE_COUNT,
            assignment_fence: Some(self.assignment.clone()),
            reassignment_portable: true,
            predecessor: self.predecessor.clone(),
            participants: vec![participant],
            source_names: Vec::new(),
            source_offsets: BTreeMap::new(),
            channel_progress: Vec::new(),
            source_watermarks: BTreeMap::new(),
            checkpoint_watermark: None,
        };
        let reference = self
            .authority
            .create_committed_checkpoint(&index)
            .await
            .unwrap();
        self.authority
            .record_cluster_outcome(
                &self.proof,
                epoch,
                epoch,
                self.assignment.clone(),
                CheckpointVerdict::Commit,
                Some(reference.clone()),
            )
            .await
            .unwrap();
        self.predecessor = Some(reference);
        segments
    }

    async fn abort(&self, epoch: u64) {
        self.authority
            .record_cluster_outcome(
                &self.proof,
                epoch,
                epoch,
                self.assignment.clone(),
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
    }

    fn checkpoint_manifest(
        &self,
        epoch: u64,
        output: NodeSubscriptionManifest,
    ) -> (CheckpointManifest, Bytes) {
        let key_groups = KeyGroupCount::try_from(VNODE_COUNT).unwrap();
        let mut manifest = CheckpointManifest::new_with_key_group_count(epoch, epoch, key_groups);
        manifest.bind_participant(PARTICIPANT_ID);
        manifest.deployment_id.clone_from(&self.deployment_id);
        manifest.assignment_fence = Some(self.assignment.clone());
        manifest.reassignment_portable = true;
        manifest.owned_vnodes = vec![0, 1];
        let payload = Bytes::from(vec![u8::try_from(epoch).unwrap()]);
        manifest.state_frames = vec![StateFrame {
            key: StateFrameKey::Vnode {
                operator_id: "gateway-fixture".into(),
                vnode: 0,
            },
            chunk: manifest.node_data.chunk,
            range: ByteRange {
                offset: 0,
                length: 1,
            },
            sha256: checkpoint_sha256(&payload),
        }];
        manifest.node_data.object_length = 1;
        manifest.node_data.sha256 = checkpoint_sha256(&payload);
        manifest.subscription_output = Some(output);
        assert!(manifest.validate(key_groups).is_empty());
        assert!(checkpoint_manifest_bytes(&manifest).is_ok());
        (manifest, payload)
    }
}

struct MultiParticipantFixture {
    objects: Arc<dyn ObjectStore>,
    authority: Arc<LeaderLeaseStore>,
    store: Arc<dyn CheckpointStore>,
    proof: LeaderProof,
    assignment: CheckpointAssignmentFence,
    certificate: Arc<OutputDistributionCertificate>,
    deployment_id: String,
    predecessor: Option<CommittedCheckpointRef>,
    frontiers: [PartitionSequence; MULTI_VNODE_COUNT_USIZE],
}

impl MultiParticipantFixture {
    async fn new() -> Self {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let decisions = CheckpointDecisionStore::new(Arc::clone(&objects));
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&objects), 30_000));
        let participants = (1..=3)
            .map(|node_id| CheckpointParticipant {
                node_id,
                boot_incarnation: Uuid::from_u128(u128::from(node_id) << 64 | u128::from(node_id)),
            })
            .collect::<Vec<_>>();
        let owner = LeaderLeaseOwner {
            node: NodeId(1),
            boot: participants[0].boot_incarnation,
            process_term: 1,
        };
        let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap()
        else {
            panic!("fresh authority must be acquired");
        };
        let assignment =
            CheckpointAssignmentFence::from_owner_map(7, &MULTI_OWNER_MAP, participants).unwrap();
        let schema = value_schema();
        let certificate = Arc::new(OutputDistributionCertificate {
            version: OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            stream_id: "positions".into(),
            catalog_generation: 1,
            stream_generation: StreamGeneration::from_digest(SubscriptionDigest::from_bytes(
                [7; 32],
            )),
            final_operator_id: "stream:positions".into(),
            distribution: OutputDistribution::VnodePartitioned {
                key_expressions_fingerprint: SubscriptionDigest::from_bytes([2; 32]),
                partition_abi: PARTITIONING_ABI_VERSION,
                vnode_count: MULTI_VNODE_COUNT,
            },
            schema_fingerprint: crate::pipeline_identity::subscription_schema_fingerprint(
                schema.as_ref(),
            )
            .unwrap(),
            changelog_mode: ChangelogMode::WeightedRetractInsert,
            history_retention_bytes: 1024 * 1024,
            query_fingerprint: SubscriptionDigest::from_bytes([3; 32]),
            pipeline_identity: PipelineIdentity::empty(),
        });
        let store = multi_checkpoint_store(Arc::clone(&objects), 1);
        Self {
            objects,
            authority,
            store,
            proof: lease.proof(),
            assignment,
            certificate,
            deployment_id,
            predecessor: None,
            frontiers: [PartitionSequence::FIRST; MULTI_VNODE_COUNT_USIZE],
        }
    }

    fn fresh_store(&self, participant_id: u64) -> Arc<dyn CheckpointStore> {
        multi_checkpoint_store(Arc::clone(&self.objects), participant_id)
    }

    fn fresh_authority(&self) -> Arc<LeaderLeaseStore> {
        Arc::new(LeaderLeaseStore::new(Arc::clone(&self.objects), 30_000))
    }

    async fn commit(&mut self, epoch: u64, partition_values: [Vec<i64>; MULTI_VNODE_COUNT_USIZE]) {
        let attempt = CheckpointAttempt::canonical(epoch);
        self.authority
            .begin_cluster_checkpoint_artifacts(
                &self.proof,
                CheckpointArtifactInventory {
                    deployment_id: self.deployment_id.clone(),
                    pipeline_identity: PipelineIdentity::empty(),
                    attempt,
                    assignment_fence: Some(self.assignment.clone()),
                },
            )
            .await
            .unwrap();
        let mut committed_participants = Vec::with_capacity(3);
        let participants = self.assignment.participants.clone();
        for participant in participants {
            let owned = MULTI_OWNER_MAP
                .iter()
                .enumerate()
                .filter_map(|(vnode, owner)| {
                    (*owner == participant.node_id).then(|| u16::try_from(vnode).unwrap())
                })
                .collect::<Vec<_>>();
            let (ranges, segments) = self
                .write_participant_output(attempt, participant, &owned, &partition_values)
                .await;
            let mut output = NodeSubscriptionManifest {
                protocol_version: SubscriptionProtocolVersion::CURRENT,
                epoch,
                checkpoint_id: epoch,
                participant_id: participant.node_id,
                assignment_certificate: self.assignment.clone(),
                streams: vec![NodeSubscriptionStreamManifest {
                    distribution_certificate: (*self.certificate).clone(),
                    ranges,
                    segments,
                }],
                manifest_digest: SubscriptionDigest::from_bytes([0; 32]),
            };
            output.seal(&owned).unwrap();
            let payload = Bytes::from(vec![u8::try_from(participant.node_id).unwrap()]);
            let manifest = multi_checkpoint_manifest(
                &self.deployment_id,
                epoch,
                participant.node_id,
                &owned,
                &self.assignment,
                output,
                &payload,
            );
            let encoded = self
                .fresh_store(participant.node_id)
                .save_checkpoint(&manifest, std::slice::from_ref(&payload))
                .await
                .unwrap();
            committed_participants
                .push(CommittedParticipantRef::from_manifest(&manifest, &encoded).unwrap());
        }
        let index = CommittedCheckpointIndex {
            version: COMMITTED_CHECKPOINT_INDEX_VERSION,
            deployment_id: self.deployment_id.clone(),
            pipeline_identity: PipelineIdentity::empty(),
            epoch,
            checkpoint_id: epoch,
            scope: CheckpointScope::Cluster,
            vnode_count: MULTI_VNODE_COUNT,
            assignment_fence: Some(self.assignment.clone()),
            reassignment_portable: true,
            predecessor: self.predecessor.clone(),
            participants: committed_participants,
            source_names: Vec::new(),
            source_offsets: BTreeMap::new(),
            channel_progress: Vec::new(),
            source_watermarks: BTreeMap::new(),
            checkpoint_watermark: None,
        };
        let reference = self
            .authority
            .create_committed_checkpoint(&index)
            .await
            .unwrap();
        self.authority
            .record_cluster_outcome(
                &self.proof,
                epoch,
                epoch,
                self.assignment.clone(),
                CheckpointVerdict::Commit,
                Some(reference.clone()),
            )
            .await
            .unwrap();
        self.predecessor = Some(reference);
    }

    async fn write_participant_output(
        &mut self,
        attempt: CheckpointAttempt,
        participant: CheckpointParticipant,
        owned: &[u16],
        partition_values: &[Vec<i64>; MULTI_VNODE_COUNT_USIZE],
    ) -> (Vec<NodePartitionRange>, Vec<OutputSegmentRef>) {
        let mut ranges = Vec::with_capacity(owned.len());
        let mut segments = Vec::new();
        for partition in owned.iter().copied() {
            let index = usize::from(partition);
            let first_sequence = self.frontiers[index];
            let frames = partition_values[index]
                .iter()
                .copied()
                .map(value_batch)
                .collect::<Vec<_>>();
            if !frames.is_empty() {
                let identity = OutputSegmentIdentity {
                    deployment_id: &self.deployment_id,
                    stream_id: &self.certificate.stream_id,
                    stream_generation: self.certificate.stream_generation,
                    partition: OutputPartitionId::new(partition),
                    schema_fingerprint: self.certificate.schema_fingerprint,
                    attempt,
                    authority: OutputWriterAuthority {
                        participant,
                        process_term: 1,
                        assignment_version: self.assignment.assignment_version,
                        assignment_digest: self.assignment.digest(),
                    },
                };
                let encoded = encode_output_segment(&identity, &frames, first_sequence).unwrap();
                self.store
                    .save_subscription_segment(&encoded.reference, encoded.bytes)
                    .await
                    .unwrap();
                self.frontiers[index] = encoded.reference.exclusive_end_sequence;
                segments.push(encoded.reference);
            }
            ranges.push(NodePartitionRange {
                partition: OutputPartitionId::new(partition),
                first_sequence,
                through_sequence: self.frontiers[index],
            });
        }
        (ranges, segments)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ObservedBatch {
    partition: u16,
    sequence: u64,
    value: i64,
    committed_epoch: u64,
}

async fn read_through_progress(
    reader: &mut ClusterSubscriptionReader,
    expected_epoch: u64,
) -> Vec<ObservedBatch> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut batches = Vec::new();
        loop {
            match reader.next().await {
                ClusterReaderRead::Frame(ClusterReaderFrame::Batch {
                    batch,
                    partition,
                    partition_sequence,
                    committed_epoch,
                    ..
                }) => {
                    let values = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    batches.push(ObservedBatch {
                        partition: partition.get(),
                        sequence: partition_sequence.get(),
                        value: values.value(0),
                        committed_epoch,
                    });
                }
                ClusterReaderRead::Frame(ClusterReaderFrame::Progress {
                    epoch,
                    checkpoint_id,
                    ..
                }) if epoch == expected_epoch => {
                    assert_eq!(checkpoint_id, expected_epoch);
                    return batches;
                }
                ClusterReaderRead::Frame(ClusterReaderFrame::Progress { epoch, .. }) => {
                    panic!("unexpected progress epoch {epoch}");
                }
                ClusterReaderRead::Terminal(error) => panic!("gateway failed: {error}"),
            }
        }
    })
    .await
    .expect("gateway did not reach committed progress")
}

fn checkpoint_store(objects: Arc<dyn ObjectStore>) -> Arc<dyn CheckpointStore> {
    Arc::new(
        ObjectStoreCheckpointStore::new(objects, "")
            .with_key_group_count(KeyGroupCount::try_from(VNODE_COUNT).unwrap())
            .with_participant_id(PARTICIPANT_ID),
    )
}

fn multi_checkpoint_store(
    objects: Arc<dyn ObjectStore>,
    participant_id: u64,
) -> Arc<dyn CheckpointStore> {
    Arc::new(
        ObjectStoreCheckpointStore::new(objects, "")
            .with_key_group_count(KeyGroupCount::try_from(MULTI_VNODE_COUNT).unwrap())
            .with_participant_id(participant_id),
    )
}

fn multi_checkpoint_manifest(
    deployment_id: &str,
    epoch: u64,
    participant_id: u64,
    owned_vnodes: &[u16],
    assignment: &CheckpointAssignmentFence,
    output: NodeSubscriptionManifest,
    payload: &Bytes,
) -> CheckpointManifest {
    let key_groups = KeyGroupCount::try_from(MULTI_VNODE_COUNT).unwrap();
    let mut manifest = CheckpointManifest::new_with_key_group_count(epoch, epoch, key_groups);
    manifest.bind_participant(participant_id);
    manifest.deployment_id = deployment_id.to_owned();
    manifest.assignment_fence = Some(assignment.clone());
    manifest.reassignment_portable = true;
    manifest.owned_vnodes = owned_vnodes.to_vec();
    manifest.state_frames = vec![StateFrame {
        key: StateFrameKey::Vnode {
            operator_id: "multi-gateway-fixture".into(),
            vnode: owned_vnodes[0],
        },
        chunk: manifest.node_data.chunk,
        range: ByteRange {
            offset: 0,
            length: u64::try_from(payload.len()).unwrap(),
        },
        sha256: checkpoint_sha256(payload),
    }];
    manifest.node_data.object_length = u64::try_from(payload.len()).unwrap();
    manifest.node_data.sha256 = checkpoint_sha256(payload);
    manifest.subscription_output = Some(output);
    assert!(manifest.validate(key_groups).is_empty());
    manifest
}

fn value_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn value_batch(value: i64) -> RecordBatch {
    RecordBatch::try_new(
        value_schema(),
        vec![Arc::new(Int64Array::from(vec![value]))],
    )
    .unwrap()
}

#[tokio::test]
async fn tail_excludes_the_existing_committed_cut() {
    let mut fixture = GatewayFixture::new().await;
    fixture.commit(1, [vec![10], vec![20]]).await;
    let mut reader = ClusterSubscriptionReader::open(
        Arc::clone(&fixture.authority),
        Arc::clone(&fixture.store),
        Arc::clone(&fixture.certificate),
        SubscribeStart::Tail,
        None,
    )
    .await
    .unwrap();

    fixture.commit(2, [vec![11], Vec::new()]).await;
    assert_eq!(
        read_through_progress(&mut reader, 2).await,
        vec![ObservedBatch {
            partition: 0,
            sequence: 1,
            value: 11,
            committed_epoch: 2,
        }]
    );
}

#[tokio::test]
async fn as_of_epoch_replays_the_exact_partition_ordered_suffix_from_a_fresh_gateway() {
    let mut fixture = GatewayFixture::new().await;
    fixture.commit(1, [vec![10], vec![20]]).await;
    fixture.commit(2, [vec![11, 12], vec![21]]).await;
    let mut reader = ClusterSubscriptionReader::open(
        fixture.fresh_authority(),
        fixture.fresh_store(),
        Arc::clone(&fixture.certificate),
        SubscribeStart::AsOfEpoch(1),
        None,
    )
    .await
    .unwrap();

    let observed = read_through_progress(&mut reader, 2).await;
    let partition_zero = observed
        .iter()
        .filter(|batch| batch.partition == 0)
        .map(|batch| (batch.sequence, batch.value))
        .collect::<Vec<_>>();
    let partition_one = observed
        .iter()
        .filter(|batch| batch.partition == 1)
        .map(|batch| (batch.sequence, batch.value))
        .collect::<Vec<_>>();
    assert_eq!(partition_zero, vec![(1, 11), (2, 12)]);
    assert_eq!(partition_one, vec![(1, 21)]);
    assert!(observed.iter().all(|batch| batch.committed_epoch == 2));
}

#[tokio::test]
async fn aborted_checkpoint_emits_neither_data_nor_progress() {
    let mut fixture = GatewayFixture::new().await;
    fixture.commit(1, [vec![10], vec![20]]).await;
    let mut reader = ClusterSubscriptionReader::open(
        Arc::clone(&fixture.authority),
        Arc::clone(&fixture.store),
        Arc::clone(&fixture.certificate),
        SubscribeStart::AsOfEpoch(1),
        None,
    )
    .await
    .unwrap();

    fixture.abort(2).await;
    fixture.commit(3, [vec![11], Vec::new()]).await;
    let observed = read_through_progress(&mut reader, 3).await;
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].value, 11);
    assert_eq!(observed[0].committed_epoch, 3);
}

#[tokio::test]
async fn missing_committed_segment_is_a_structured_terminal_error() {
    let mut fixture = GatewayFixture::new().await;
    fixture.commit(1, [vec![10], vec![20]]).await;
    let segments = fixture.commit(2, [vec![11], Vec::new()]).await;
    fixture
        .store
        .delete_subscription_segment(&segments[0].object_key)
        .await
        .unwrap();
    let mut reader = ClusterSubscriptionReader::open(
        fixture.fresh_authority(),
        fixture.fresh_store(),
        Arc::clone(&fixture.certificate),
        SubscribeStart::AsOfEpoch(1),
        None,
    )
    .await
    .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(3), reader.next())
        .await
        .unwrap();
    assert!(matches!(
        result,
        ClusterReaderRead::Terminal(ClusterSubscriptionError::SegmentMissing {
            partition,
            first,
        }) if partition == OutputPartitionId::new(0) && first == PartitionSequence::new(1)
    ));
}

#[tokio::test]
async fn replacement_generation_cannot_attach_to_old_history() {
    let mut fixture = GatewayFixture::new().await;
    fixture.commit(1, [vec![10], vec![20]]).await;
    let mut replacement = (*fixture.certificate).clone();
    replacement.stream_generation =
        StreamGeneration::from_digest(SubscriptionDigest::from_bytes([9; 32]));
    let error = ClusterSubscriptionReader::open(
        fixture.fresh_authority(),
        fixture.fresh_store(),
        Arc::new(replacement),
        SubscribeStart::AsOfEpoch(1),
        None,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        DbError::Subscription(ClusterSubscriptionError::GenerationMismatch)
    ));
}

#[tokio::test]
async fn replay_start_distinguishes_uncommitted_and_disabled_history() {
    let fixture = GatewayFixture::new().await;
    let error = ClusterSubscriptionReader::open(
        fixture.fresh_authority(),
        fixture.fresh_store(),
        Arc::clone(&fixture.certificate),
        SubscribeStart::AsOfEpoch(1),
        None,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        DbError::Subscription(ClusterSubscriptionError::EpochNotCommitted { requested: 1 })
    ));

    let mut no_history = (*fixture.certificate).clone();
    no_history.history_retention_bytes = 0;
    let error = ClusterSubscriptionReader::open(
        fixture.fresh_authority(),
        fixture.fresh_store(),
        Arc::new(no_history),
        SubscribeStart::AsOfEpoch(1),
        None,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        DbError::Subscription(ClusterSubscriptionError::ReplayPruned { requested: 1 })
    ));
}

#[tokio::test]
async fn slow_reader_is_disconnected_at_the_bounded_gateway_queue() {
    let mut fixture = GatewayFixture::new().await;
    fixture.commit(1, [Vec::new(), Vec::new()]).await;
    let registry = prometheus::Registry::new();
    let metrics = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
    let mut reader = ClusterSubscriptionReader::open(
        Arc::clone(&fixture.authority),
        Arc::clone(&fixture.store),
        Arc::clone(&fixture.certificate),
        SubscribeStart::AsOfEpoch(1),
        Some(Arc::clone(&metrics)),
    )
    .await
    .unwrap();
    assert_eq!(metrics.cluster_subscription.active_readers.get(), 1);

    fixture
        .commit(2, [(0..70).map(i64::from).collect(), Vec::new()])
        .await;
    tokio::time::timeout(Duration::from_secs(8), async {
        while metrics
            .cluster_subscription
            .gateway_lag_disconnects_total
            .get()
            == 0
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();

    let terminal = loop {
        if let ClusterReaderRead::Terminal(error) = reader.next().await {
            break error;
        }
    };
    assert_eq!(terminal, ClusterSubscriptionError::SubscriberLagged);
    assert_eq!(metrics.cluster_subscription.active_readers.get(), 0);
    assert_eq!(metrics.cluster_subscription.replay_frames_total.get(), 70);
    assert!(metrics.cluster_subscription.replay_bytes_total.get() > 0);
}

#[tokio::test]
async fn every_gateway_reads_the_complete_three_participant_union() {
    let mut fixture = MultiParticipantFixture::new().await;
    fixture.commit(1, std::array::from_fn(|_| Vec::new())).await;
    let mut readers = Vec::new();
    for participant_id in 1..=3 {
        readers.push(
            ClusterSubscriptionReader::open(
                fixture.fresh_authority(),
                fixture.fresh_store(participant_id),
                Arc::clone(&fixture.certificate),
                SubscribeStart::AsOfEpoch(1),
                None,
            )
            .await
            .unwrap(),
        );
    }
    fixture
        .commit(
            2,
            [
                vec![10, 11],
                vec![12],
                vec![20],
                vec![21, 22],
                vec![30],
                vec![31],
            ],
        )
        .await;

    let expected = [
        (0, vec![(0, 10), (1, 11)]),
        (1, vec![(0, 12)]),
        (2, vec![(0, 20)]),
        (3, vec![(0, 21), (1, 22)]),
        (4, vec![(0, 30)]),
        (5, vec![(0, 31)]),
    ];
    for reader in &mut readers {
        let observed = read_through_progress(reader, 2).await;
        let by_partition = (0..MULTI_VNODE_COUNT)
            .map(|partition| {
                let frames = observed
                    .iter()
                    .filter(|batch| batch.partition == partition)
                    .map(|batch| (batch.sequence, batch.value))
                    .collect::<Vec<_>>();
                (partition, frames)
            })
            .collect::<Vec<_>>();
        assert_eq!(by_partition, expected);
        assert!(observed.iter().all(|batch| batch.committed_epoch == 2));
    }
}
