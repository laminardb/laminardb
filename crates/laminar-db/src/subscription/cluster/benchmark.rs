//! Benchmark-only fixtures for the committed cluster subscription gateway.

use std::collections::{BTreeMap, BTreeSet};
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
    COMMITTED_CHECKPOINT_INDEX_VERSION, MAX_OUTPUT_SEGMENT_BYTES,
    OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
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

use super::reader::{
    MAX_GATEWAY_QUEUE_BYTES, MAX_GATEWAY_QUEUE_FRAMES, MAX_GATEWAY_RETAINED_SEGMENTS,
};
use super::{
    encode_output_segment, ClusterReaderFrame, ClusterReaderRead, ClusterSubscriptionReader,
    OutputSegmentIdentity, OutputWriterAuthority,
};
use crate::error::DbError;
use crate::subscription::{ClusterSubscriptionError, SubscribeStart};

const PARTICIPANT_ID: u64 = 1;
const MAX_BENCHMARK_PARTITIONS: u16 = 1_024;
const MAX_BENCHMARK_FRAMES_PER_PARTITION: u16 = 1_024;
const MAX_BENCHMARK_ROWS_PER_FRAME: u32 = 131_072;
const MAX_BENCHMARK_ROWS: u64 = 16_777_216;
const HISTORY_RETENTION_BYTES: u64 = 512 * 1024 * 1024;

/// Observed work from one replay through the production gateway reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewayReplayObservation {
    /// Data frames decoded and delivered before checkpoint progress.
    pub frames: u64,
    /// Arrow rows carried by the delivered frames.
    pub rows: u64,
    /// Distinct output partitions represented in the replay.
    pub partitions: usize,
}

/// Measured retained queue memory and its production configuration bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlowReaderFootprint {
    /// Batch frames retained by the connection-local receiver.
    pub queued_frames: usize,
    /// Arrow allocation bytes retained by those queued frames.
    pub queued_arrow_bytes: usize,
    /// Byte permits available to a production gateway queue.
    pub queue_byte_limit: usize,
    /// Conservative decoded-segment allowance outside the receiver queue.
    pub segment_read_allowance_bytes: usize,
    /// Queue plus concurrent decoded-segment upper bound.
    pub logical_upper_bound_bytes: usize,
}

/// Immutable two-checkpoint fixture backed by the real object-store reader path.
pub struct ClusterSubscriptionGatewayBenchmark {
    fixture: BenchmarkFixture,
    total_frames: usize,
    rows_per_frame: usize,
}

impl ClusterSubscriptionGatewayBenchmark {
    /// Build an empty epoch followed by one committed data epoch.
    ///
    /// # Errors
    /// Returns an error if the requested fixture exceeds its hard bounds or any committed
    /// checkpoint artifact cannot be constructed.
    pub async fn new(
        partitions: u16,
        frames_per_partition: u16,
        rows_per_frame: u32,
    ) -> Result<Self, DbError> {
        let total_frames =
            validate_configuration(partitions, frames_per_partition, rows_per_frame)?;
        let mut fixture = BenchmarkFixture::new(partitions).await?;
        fixture.commit(1, 0, 0).await?;
        fixture
            .commit(2, frames_per_partition, rows_per_frame)
            .await?;
        Ok(Self {
            fixture,
            total_frames,
            rows_per_frame: usize::try_from(rows_per_frame).map_err(benchmark_error)?,
        })
    }

    /// Replay the committed data epoch and drain through its global progress marker.
    ///
    /// # Errors
    /// Returns an error if the actual gateway reports a terminal integrity failure or its
    /// observation differs from the committed fixture.
    pub async fn replay_once(&self) -> Result<GatewayReplayObservation, DbError> {
        let mut reader = self.fixture.open_reader().await?;
        let observation = read_committed_epoch(&mut reader, 2).await?;
        reader.close_for_benchmark().await;
        let expected_rows = self
            .total_frames
            .checked_mul(self.rows_per_frame)
            .ok_or_else(|| benchmark_message("expected replay row count overflow"))?;
        if observation.frames != u64::try_from(self.total_frames).unwrap_or(u64::MAX)
            || observation.rows != u64::try_from(expected_rows).unwrap_or(u64::MAX)
            || observation.partitions != usize::from(self.fixture.partitions)
        {
            return Err(benchmark_message(
                "gateway replay observation is incomplete",
            ));
        }
        Ok(observation)
    }

    /// Fill a non-consuming reader, cancel it, and measure the retained Arrow receiver queue.
    ///
    /// # Errors
    /// Returns an error if the fixture cannot exceed a production queue bound, the queue does
    /// not reach that bound, or retained receiver memory exceeds its configured byte limit.
    pub async fn slow_reader_footprint(&self) -> Result<SlowReaderFootprint, DbError> {
        let frame_bytes = value_batch(0, self.rows_per_frame)?.get_array_memory_size();
        let byte_limited_frames = MAX_GATEWAY_QUEUE_BYTES / frame_bytes.max(1);
        let expected_frames = MAX_GATEWAY_QUEUE_FRAMES.min(byte_limited_frames);
        if expected_frames == 0 || self.total_frames <= expected_frames {
            return Err(benchmark_message(
                "slow-reader fixture must contain more frames than the gateway can queue",
            ));
        }
        let reader = self.fixture.open_reader().await?;
        wait_for_queue(&reader, expected_frames).await?;
        let (queued_slots, queued_frames, queued_arrow_bytes) =
            reader.capture_queue_for_benchmark().await;
        if queued_slots != queued_frames
            || queued_frames != expected_frames
            || queued_arrow_bytes > MAX_GATEWAY_QUEUE_BYTES
        {
            return Err(benchmark_message(
                "gateway receiver queue exceeded or missed its configured bound",
            ));
        }
        let segment_read_allowance_bytes = MAX_GATEWAY_RETAINED_SEGMENTS * MAX_OUTPUT_SEGMENT_BYTES;
        Ok(SlowReaderFootprint {
            queued_frames,
            queued_arrow_bytes,
            queue_byte_limit: MAX_GATEWAY_QUEUE_BYTES,
            segment_read_allowance_bytes,
            logical_upper_bound_bytes: MAX_GATEWAY_QUEUE_BYTES + segment_read_allowance_bytes,
        })
    }
}

struct BenchmarkFixture {
    objects: Arc<dyn ObjectStore>,
    authority: Arc<LeaderLeaseStore>,
    store: Arc<dyn CheckpointStore>,
    proof: LeaderProof,
    participant: CheckpointParticipant,
    assignment: CheckpointAssignmentFence,
    certificate: Arc<OutputDistributionCertificate>,
    deployment_id: String,
    predecessor: Option<CommittedCheckpointRef>,
    frontiers: Vec<PartitionSequence>,
    schema: Arc<Schema>,
    partitions: u16,
}

impl BenchmarkFixture {
    async fn new(partitions: u16) -> Result<Self, DbError> {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let decisions = CheckpointDecisionStore::new(Arc::clone(&objects));
        let deployment_id = decisions
            .load_or_create_deployment_id()
            .await
            .map_err(benchmark_error)?;
        let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&objects), 30_000));
        let boot = Uuid::from_u128(0x1111_1111_1111_4111_8111_1111_1111_1111);
        let owner = LeaderLeaseOwner {
            node: NodeId(PARTICIPANT_ID),
            boot,
            process_term: 1,
        };
        let LeaseOutcome::Acquired(lease) = authority
            .begin_new_term(&owner, 0)
            .await
            .map_err(benchmark_error)?
        else {
            return Err(benchmark_message(
                "fresh benchmark authority was not acquired",
            ));
        };
        let participant = CheckpointParticipant {
            node_id: PARTICIPANT_ID,
            boot_incarnation: boot,
        };
        let assignment = CheckpointAssignmentFence::from_owner_map(
            7,
            &vec![PARTICIPANT_ID; usize::from(partitions)],
            vec![participant],
        )
        .map_err(benchmark_error)?;
        let schema = value_schema();
        let certificate = Arc::new(output_certificate(partitions, schema.as_ref())?);
        let key_groups = KeyGroupCount::try_from(partitions).map_err(benchmark_error)?;
        let store = checkpoint_store(Arc::clone(&objects), key_groups);
        Ok(Self {
            objects,
            authority,
            store,
            proof: lease.proof(),
            participant,
            assignment,
            certificate,
            deployment_id,
            predecessor: None,
            frontiers: vec![PartitionSequence::FIRST; usize::from(partitions)],
            schema,
            partitions,
        })
    }

    async fn open_reader(&self) -> Result<ClusterSubscriptionReader, DbError> {
        ClusterSubscriptionReader::open(
            Arc::new(LeaderLeaseStore::new(Arc::clone(&self.objects), 30_000)),
            checkpoint_store(
                Arc::clone(&self.objects),
                KeyGroupCount::try_from(self.partitions).map_err(benchmark_error)?,
            ),
            Arc::clone(&self.certificate),
            SubscribeStart::AsOfEpoch(1),
            None,
        )
        .await
    }

    async fn commit(
        &mut self,
        epoch: u64,
        frames_per_partition: u16,
        rows_per_frame: u32,
    ) -> Result<(), DbError> {
        let attempt = CheckpointAttempt::canonical(epoch);
        self.begin_attempt(attempt).await?;
        let (ranges, segments) = self
            .write_output(attempt, frames_per_partition, rows_per_frame)
            .await?;
        let owned = (0..self.partitions).collect::<Vec<_>>();
        let output = self.node_output(epoch, ranges, segments, &owned)?;
        let (manifest, payload) = self.checkpoint_manifest(epoch, output)?;
        let encoded = self
            .store
            .save_checkpoint(&manifest, std::slice::from_ref(&payload))
            .await
            .map_err(benchmark_error)?;
        let participant =
            CommittedParticipantRef::from_manifest(&manifest, &encoded).map_err(benchmark_error)?;
        self.publish_index(epoch, participant).await
    }

    async fn begin_attempt(&self, attempt: CheckpointAttempt) -> Result<(), DbError> {
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
            .map_err(benchmark_error)?;
        Ok(())
    }

    async fn write_output(
        &mut self,
        attempt: CheckpointAttempt,
        frames_per_partition: u16,
        rows_per_frame: u32,
    ) -> Result<(Vec<NodePartitionRange>, Vec<OutputSegmentRef>), DbError> {
        let mut ranges = Vec::with_capacity(usize::from(self.partitions));
        let mut segments = Vec::with_capacity(usize::from(self.partitions));
        for partition in 0..self.partitions {
            let index = usize::from(partition);
            let first_sequence = self.frontiers[index];
            if frames_per_partition > 0 {
                let frames = partition_frames(
                    &self.schema,
                    partition,
                    frames_per_partition,
                    rows_per_frame,
                )?;
                let identity = self.segment_identity(attempt, partition);
                let encoded = encode_output_segment(&identity, &frames, first_sequence)?;
                self.store
                    .save_subscription_segment(&encoded.reference, encoded.bytes)
                    .await
                    .map_err(benchmark_error)?;
                self.frontiers[index] = encoded.reference.exclusive_end_sequence;
                segments.push(encoded.reference);
            }
            ranges.push(NodePartitionRange {
                partition: OutputPartitionId::new(partition),
                first_sequence,
                through_sequence: self.frontiers[index],
            });
        }
        Ok((ranges, segments))
    }

    fn segment_identity(
        &self,
        attempt: CheckpointAttempt,
        partition: u16,
    ) -> OutputSegmentIdentity<'_> {
        OutputSegmentIdentity {
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
        }
    }

    fn node_output(
        &self,
        epoch: u64,
        ranges: Vec<NodePartitionRange>,
        segments: Vec<OutputSegmentRef>,
        owned: &[u16],
    ) -> Result<NodeSubscriptionManifest, DbError> {
        let mut output = NodeSubscriptionManifest {
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            epoch,
            checkpoint_id: epoch,
            participant_id: PARTICIPANT_ID,
            assignment_certificate: self.assignment.clone(),
            streams: vec![NodeSubscriptionStreamManifest {
                distribution_certificate: (*self.certificate).clone(),
                ranges,
                segments,
            }],
            manifest_digest: SubscriptionDigest::from_bytes([0; 32]),
        };
        output.seal(owned).map_err(benchmark_error)?;
        Ok(output)
    }

    fn checkpoint_manifest(
        &self,
        epoch: u64,
        output: NodeSubscriptionManifest,
    ) -> Result<(CheckpointManifest, Bytes), DbError> {
        let key_groups = KeyGroupCount::try_from(self.partitions).map_err(benchmark_error)?;
        let mut manifest = CheckpointManifest::new_with_key_group_count(epoch, epoch, key_groups);
        manifest.bind_participant(PARTICIPANT_ID);
        manifest.deployment_id.clone_from(&self.deployment_id);
        manifest.assignment_fence = Some(self.assignment.clone());
        manifest.reassignment_portable = true;
        manifest.owned_vnodes = (0..self.partitions).collect();
        let payload = Bytes::copy_from_slice(&epoch.to_le_bytes());
        manifest.state_frames = vec![StateFrame {
            key: StateFrameKey::Vnode {
                operator_id: "gateway-benchmark".into(),
                vnode: 0,
            },
            chunk: manifest.node_data.chunk,
            range: ByteRange {
                offset: 0,
                length: u64::try_from(payload.len()).map_err(benchmark_error)?,
            },
            sha256: checkpoint_sha256(&payload),
        }];
        manifest.node_data.object_length = u64::try_from(payload.len()).map_err(benchmark_error)?;
        manifest.node_data.sha256 = checkpoint_sha256(&payload);
        manifest.subscription_output = Some(output);
        let violations = manifest.validate(key_groups);
        if !violations.is_empty() {
            return Err(benchmark_message(format!(
                "benchmark checkpoint manifest is invalid: {violations:?}"
            )));
        }
        checkpoint_manifest_bytes(&manifest).map_err(benchmark_error)?;
        Ok((manifest, payload))
    }

    async fn publish_index(
        &mut self,
        epoch: u64,
        participant: CommittedParticipantRef,
    ) -> Result<(), DbError> {
        let index = CommittedCheckpointIndex {
            version: COMMITTED_CHECKPOINT_INDEX_VERSION,
            deployment_id: self.deployment_id.clone(),
            pipeline_identity: PipelineIdentity::empty(),
            epoch,
            checkpoint_id: epoch,
            scope: CheckpointScope::Cluster,
            vnode_count: self.partitions,
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
            .map_err(benchmark_error)?;
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
            .map_err(benchmark_error)?;
        self.predecessor = Some(reference);
        Ok(())
    }
}

fn validate_configuration(
    partitions: u16,
    frames_per_partition: u16,
    rows_per_frame: u32,
) -> Result<usize, DbError> {
    if partitions == 0
        || partitions > MAX_BENCHMARK_PARTITIONS
        || frames_per_partition == 0
        || frames_per_partition > MAX_BENCHMARK_FRAMES_PER_PARTITION
        || rows_per_frame == 0
        || rows_per_frame > MAX_BENCHMARK_ROWS_PER_FRAME
    {
        return Err(benchmark_message(
            "cluster subscription benchmark dimensions exceed their hard bounds",
        ));
    }
    let total_frames = usize::from(partitions)
        .checked_mul(usize::from(frames_per_partition))
        .ok_or_else(|| benchmark_message("benchmark frame count overflow"))?;
    let total_rows = u64::try_from(total_frames)
        .unwrap_or(u64::MAX)
        .checked_mul(u64::from(rows_per_frame))
        .ok_or_else(|| benchmark_message("benchmark row count overflow"))?;
    if total_rows > MAX_BENCHMARK_ROWS {
        return Err(benchmark_message(
            "cluster subscription benchmark exceeds its total row bound",
        ));
    }
    KeyGroupCount::try_from(partitions).map_err(benchmark_error)?;
    Ok(total_frames)
}

fn output_certificate(
    partitions: u16,
    schema: &Schema,
) -> Result<OutputDistributionCertificate, DbError> {
    Ok(OutputDistributionCertificate {
        version: OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
        protocol_version: SubscriptionProtocolVersion::CURRENT,
        stream_id: "benchmark_positions".into(),
        catalog_generation: 1,
        stream_generation: StreamGeneration::from_digest(SubscriptionDigest::from_bytes([1; 32])),
        final_operator_id: "stream:benchmark_positions".into(),
        distribution: OutputDistribution::VnodePartitioned {
            key_expressions_fingerprint: SubscriptionDigest::from_bytes([2; 32]),
            partition_abi: PARTITIONING_ABI_VERSION,
            vnode_count: partitions,
        },
        schema_fingerprint: crate::pipeline_identity::subscription_schema_fingerprint(schema)?,
        changelog_mode: ChangelogMode::WeightedRetractInsert,
        history_retention_bytes: HISTORY_RETENTION_BYTES,
        query_fingerprint: SubscriptionDigest::from_bytes([3; 32]),
        pipeline_identity: PipelineIdentity::empty(),
    })
}

fn checkpoint_store(
    objects: Arc<dyn ObjectStore>,
    key_groups: KeyGroupCount,
) -> Arc<dyn CheckpointStore> {
    Arc::new(
        ObjectStoreCheckpointStore::new(objects, "")
            .with_key_group_count(key_groups)
            .with_participant_id(PARTICIPANT_ID),
    )
}

fn partition_frames(
    schema: &Arc<Schema>,
    partition: u16,
    frame_count: u16,
    rows_per_frame: u32,
) -> Result<Vec<RecordBatch>, DbError> {
    let rows = usize::try_from(rows_per_frame).map_err(benchmark_error)?;
    (0..frame_count)
        .map(|frame| {
            let value = i64::from(partition) * i64::from(frame_count) + i64::from(frame);
            RecordBatch::try_new(
                Arc::clone(schema),
                vec![Arc::new(Int64Array::from_value(value, rows))],
            )
            .map_err(benchmark_error)
        })
        .collect()
}

fn value_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn value_batch(value: i64, rows: usize) -> Result<RecordBatch, DbError> {
    RecordBatch::try_new(
        value_schema(),
        vec![Arc::new(Int64Array::from_value(value, rows))],
    )
    .map_err(benchmark_error)
}

async fn read_committed_epoch(
    reader: &mut ClusterSubscriptionReader,
    expected_epoch: u64,
) -> Result<GatewayReplayObservation, DbError> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut frames = 0_u64;
        let mut rows = 0_u64;
        let mut partitions = BTreeSet::new();
        loop {
            match reader.next().await {
                ClusterReaderRead::Frame(ClusterReaderFrame::Batch {
                    batch, partition, ..
                }) => {
                    frames = frames
                        .checked_add(1)
                        .ok_or_else(|| benchmark_message("observed frame count overflow"))?;
                    rows = rows
                        .checked_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX))
                        .ok_or_else(|| benchmark_message("observed row count overflow"))?;
                    partitions.insert(partition);
                }
                ClusterReaderRead::Frame(ClusterReaderFrame::Progress { epoch, .. })
                    if epoch == expected_epoch =>
                {
                    return Ok(GatewayReplayObservation {
                        frames,
                        rows,
                        partitions: partitions.len(),
                    });
                }
                ClusterReaderRead::Frame(ClusterReaderFrame::Progress { epoch, .. }) => {
                    return Err(benchmark_message(format!(
                        "unexpected benchmark progress epoch {epoch}"
                    )));
                }
                ClusterReaderRead::Terminal(error) => return Err(error.into()),
            }
        }
    })
    .await
    .map_err(|_| ClusterSubscriptionError::BackendUnavailable)?
}

async fn wait_for_queue(
    reader: &ClusterSubscriptionReader,
    expected_frames: usize,
) -> Result<(), DbError> {
    tokio::time::timeout(Duration::from_secs(10), async {
        while reader.queued_frames() < expected_frames {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| benchmark_message("gateway queue did not reach its expected bound"))
}

fn benchmark_error(error: impl std::fmt::Display) -> DbError {
    benchmark_message(error.to_string())
}

fn benchmark_message(message: impl Into<String>) -> DbError {
    DbError::Checkpoint(format!(
        "cluster subscription benchmark fixture: {}",
        message.into()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn production_gateway_fixture_replays_and_bounds_a_slow_reader() {
        let replay = ClusterSubscriptionGatewayBenchmark::new(4, 2, 8)
            .await
            .unwrap();
        assert_eq!(
            replay.replay_once().await.unwrap(),
            GatewayReplayObservation {
                frames: 8,
                rows: 64,
                partitions: 4,
            }
        );

        let slow = ClusterSubscriptionGatewayBenchmark::new(2, 35, 8)
            .await
            .unwrap()
            .slow_reader_footprint()
            .await
            .unwrap();
        assert_eq!(slow.queued_frames, MAX_GATEWAY_QUEUE_FRAMES);
        assert!(slow.queued_arrow_bytes <= slow.queue_byte_limit);
        assert_eq!(
            slow.logical_upper_bound_bytes,
            slow.queue_byte_limit + slow.segment_read_allowance_bytes
        );
    }
}
