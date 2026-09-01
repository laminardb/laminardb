//! Fair bounded replay and delivery across committed output partitions.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::RecordBatch;
use futures::stream::{FuturesUnordered, StreamExt};
use laminar_core::checkpoint::{
    CheckpointStore, OutputDistributionCertificate, OutputPartitionId, PartitionSequence,
    StreamGeneration,
};
use laminar_core::cluster::control::LeaderLeaseStore;
use tokio::sync::{mpsc, Semaphore};

use super::authority::{
    into_subscription_error, next_committed_indexes, replace_frontiers, GatewayCursor,
};
use super::pin::{finish_initial_replay, renew_replay_pin, GatewayReplayPin};
use super::{
    ClusterReaderFrame, GATEWAY_IO_TIMEOUT, GATEWAY_SEND_TIMEOUT, MANIFEST_REFRESH_INTERVAL,
    MAX_GATEWAY_QUEUE_BYTES, MAX_GATEWAY_SEGMENT_READS,
};
use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
use crate::subscription::cluster::decode_bound_output_segment;
use crate::subscription::cluster::manifest::{
    load_checkpoint, BoundOutputSegment, LoadedCheckpoint, LoadedStreamCut,
};
use crate::subscription::ClusterSubscriptionError;

type SegmentFuture = Pin<
    Box<dyn Future<Output = (usize, BoundOutputSegment, Result<Vec<RecordBatch>, DbError>)> + Send>,
>;

pub(super) async fn run_gateway(
    authority: Arc<LeaderLeaseStore>,
    store: Arc<dyn CheckpointStore>,
    certificate: Arc<OutputDistributionCertificate>,
    mut cursor: GatewayCursor,
    sender: &mpsc::Sender<ClusterReaderFrame>,
    metrics: Option<&Arc<EngineMetrics>>,
    replay_pin: &mut Option<GatewayReplayPin>,
) -> Result<(), ClusterSubscriptionError> {
    let queue_budget = Arc::new(Semaphore::new(MAX_GATEWAY_QUEUE_BYTES));
    loop {
        if sender.is_closed() {
            return Ok(());
        }
        renew_replay_pin(&authority, replay_pin).await?;
        let refresh_started = Instant::now();
        let indexes = next_committed_indexes(&authority, &cursor).await;
        if let Some(metrics) = metrics {
            metrics
                .cluster_subscription
                .gateway_manifest_refresh_seconds
                .observe(refresh_started.elapsed().as_secs_f64());
        }
        let indexes = indexes.map_err(into_subscription_error)?;
        if indexes.is_empty() {
            finish_initial_replay(&authority, replay_pin, certificate.stream_generation).await?;
            tokio::time::sleep(MANIFEST_REFRESH_INTERVAL).await;
            continue;
        }
        for index in indexes {
            let loaded = tokio::time::timeout(
                GATEWAY_IO_TIMEOUT,
                load_checkpoint(&store, index, &certificate),
            )
            .await
            .map_err(|_| ClusterSubscriptionError::BackendUnavailable)?
            .map_err(into_subscription_error)?;
            process_checkpoint(
                &store,
                &certificate,
                &mut cursor,
                loaded,
                sender,
                &queue_budget,
                metrics,
                &authority,
                replay_pin,
            )
            .await?;
        }
        finish_initial_replay(&authority, replay_pin, certificate.stream_generation).await?;
    }
}

#[allow(clippy::too_many_arguments)] // checkpoint, queue, and pin authority remain explicit
async fn process_checkpoint(
    store: &Arc<dyn CheckpointStore>,
    certificate: &OutputDistributionCertificate,
    cursor: &mut GatewayCursor,
    loaded: LoadedCheckpoint,
    sender: &mpsc::Sender<ClusterReaderFrame>,
    queue_budget: &Arc<Semaphore>,
    metrics: Option<&Arc<EngineMetrics>>,
    authority: &LeaderLeaseStore,
    replay_pin: &mut Option<GatewayReplayPin>,
) -> Result<(), ClusterSubscriptionError> {
    let epoch = loaded.index.epoch;
    let checkpoint_id = loaded.index.checkpoint_id;
    let Some(stream) = loaded.stream else {
        if cursor.generation_seen {
            return Err(ClusterSubscriptionError::GenerationMismatch);
        }
        cursor.current = Some(loaded.reference);
        cursor.current_index = Some(loaded.index);
        return Ok(());
    };
    validate_interval(&cursor.expected, &stream)?;
    replay_interval(
        store,
        certificate.stream_generation,
        epoch,
        &stream,
        cursor,
        sender,
        queue_budget,
        metrics,
        authority,
        replay_pin,
    )
    .await?;
    replace_frontiers(&mut cursor.expected, &stream).map_err(into_subscription_error)?;
    cursor.generation_seen = true;
    send_progress(
        cursor,
        sender,
        certificate.stream_generation,
        epoch,
        checkpoint_id,
    )
    .await?;
    cursor.current = Some(loaded.reference);
    cursor.current_index = Some(loaded.index);
    Ok(())
}

fn validate_interval(
    expected: &std::collections::BTreeMap<OutputPartitionId, PartitionSequence>,
    stream: &LoadedStreamCut,
) -> Result<(), ClusterSubscriptionError> {
    if stream.ranges.len() != expected.len() {
        return Err(ClusterSubscriptionError::ManifestCorrupt {
            reason: "committed stream range roster is incomplete".into(),
        });
    }
    for (range, (partition, sequence)) in stream.ranges.iter().zip(expected) {
        if range.partition != *partition {
            return Err(ClusterSubscriptionError::ManifestCorrupt {
                reason: "committed stream range roster is noncanonical".into(),
            });
        }
        if range.first_sequence != *sequence {
            return Err(ClusterSubscriptionError::PartitionSequenceGap {
                partition: *partition,
                expected: *sequence,
                actual: range.first_sequence,
            });
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // replay authority and bounded delivery state are distinct
async fn replay_interval(
    store: &Arc<dyn CheckpointStore>,
    generation: StreamGeneration,
    epoch: u64,
    stream: &LoadedStreamCut,
    cursor: &mut GatewayCursor,
    sender: &mpsc::Sender<ClusterReaderFrame>,
    queue_budget: &Arc<Semaphore>,
    metrics: Option<&Arc<EngineMetrics>>,
    authority: &LeaderLeaseStore,
    replay_pin: &mut Option<GatewayReplayPin>,
) -> Result<(), ClusterSubscriptionError> {
    let mut partitions = partition_replays(stream);
    let mut in_flight = FuturesUnordered::<SegmentFuture>::new();
    let mut schedule_cursor = 0;
    let mut ready_cursor = 0;
    loop {
        renew_replay_pin(authority, replay_pin).await?;
        schedule_segment_reads(store, &mut partitions, &mut in_flight, &mut schedule_cursor);
        if let Some(index) = ready_partition(&partitions, ready_cursor) {
            let (batch, partition, sequence) = partitions[index].pop_frame()?;
            ready_cursor = (index + 1) % partitions.len().max(1);
            send_batch(
                cursor,
                sender,
                queue_budget,
                batch,
                generation,
                partition,
                sequence,
                epoch,
            )
            .await?;
            continue;
        }
        if partitions.iter().all(PartitionReplay::complete) {
            return Ok(());
        }
        let Some((index, segment, result)) = in_flight.next().await else {
            return Err(ClusterSubscriptionError::ManifestCorrupt {
                reason: "committed partition replay stopped before its frontier".into(),
            });
        };
        let frames = result.map_err(into_subscription_error)?;
        let frame_count = u64::try_from(frames.len()).unwrap_or(u64::MAX);
        partitions[index].finish_load(&segment, frames)?;
        if let Some(metrics) = metrics {
            metrics
                .cluster_subscription
                .replay_bytes_total
                .inc_by(segment.reference.encoded_length);
            metrics
                .cluster_subscription
                .replay_frames_total
                .inc_by(frame_count);
        }
    }
}

struct PartitionReplay {
    partition: OutputPartitionId,
    next_sequence: PartitionSequence,
    through_sequence: PartitionSequence,
    segments: VecDeque<BoundOutputSegment>,
    frames: VecDeque<RecordBatch>,
    loading: bool,
}

impl PartitionReplay {
    fn complete(&self) -> bool {
        self.next_sequence == self.through_sequence
            && self.segments.is_empty()
            && self.frames.is_empty()
            && !self.loading
    }

    fn pop_frame(
        &mut self,
    ) -> Result<(RecordBatch, OutputPartitionId, PartitionSequence), ClusterSubscriptionError> {
        let batch =
            self.frames
                .pop_front()
                .ok_or_else(|| ClusterSubscriptionError::ManifestCorrupt {
                    reason: "ready partition has no decoded frame".into(),
                })?;
        let sequence = self.next_sequence;
        self.next_sequence =
            sequence
                .checked_next()
                .map_err(|_| ClusterSubscriptionError::ManifestCorrupt {
                    reason: "committed partition sequence overflowed".into(),
                })?;
        if self.next_sequence > self.through_sequence {
            return Err(ClusterSubscriptionError::PartitionSequenceGap {
                partition: self.partition,
                expected: self.through_sequence,
                actual: self.next_sequence,
            });
        }
        Ok((batch, self.partition, sequence))
    }

    fn finish_load(
        &mut self,
        segment: &BoundOutputSegment,
        frames: Vec<RecordBatch>,
    ) -> Result<(), ClusterSubscriptionError> {
        self.loading = false;
        if segment.reference.partition != self.partition
            || segment.reference.first_sequence != self.next_sequence
            || u64::try_from(frames.len()).ok() != Some(segment.reference.frame_count)
        {
            return Err(ClusterSubscriptionError::PartitionSequenceGap {
                partition: self.partition,
                expected: self.next_sequence,
                actual: segment.reference.first_sequence,
            });
        }
        self.frames = frames.into();
        Ok(())
    }
}

fn partition_replays(stream: &LoadedStreamCut) -> Vec<PartitionReplay> {
    let mut segments = stream.segments.iter().cloned().peekable();
    stream
        .ranges
        .iter()
        .map(|range| {
            let mut partition_segments = VecDeque::new();
            while segments
                .peek()
                .is_some_and(|segment| segment.reference.partition == range.partition)
            {
                if let Some(segment) = segments.next() {
                    partition_segments.push_back(segment);
                }
            }
            PartitionReplay {
                partition: range.partition,
                next_sequence: range.first_sequence,
                through_sequence: range.through_sequence,
                segments: partition_segments,
                frames: VecDeque::new(),
                loading: false,
            }
        })
        .collect()
}

fn schedule_segment_reads(
    store: &Arc<dyn CheckpointStore>,
    partitions: &mut [PartitionReplay],
    in_flight: &mut FuturesUnordered<SegmentFuture>,
    cursor: &mut usize,
) {
    while in_flight.len() < MAX_GATEWAY_SEGMENT_READS {
        let Some(index) = schedulable_partition(partitions, *cursor) else {
            break;
        };
        let Some(segment) = partitions[index].segments.pop_front() else {
            break;
        };
        partitions[index].loading = true;
        *cursor = (index + 1) % partitions.len().max(1);
        let store = Arc::clone(store);
        let task_segment = segment.clone();
        in_flight.push(Box::pin(async move {
            let result = load_segment(&store, &task_segment).await;
            (index, segment, result)
        }));
    }
}

fn schedulable_partition(partitions: &[PartitionReplay], start: usize) -> Option<usize> {
    (0..partitions.len())
        .map(|offset| (start + offset) % partitions.len())
        .find(|index| {
            let partition = &partitions[*index];
            !partition.loading && partition.frames.is_empty() && !partition.segments.is_empty()
        })
}

fn ready_partition(partitions: &[PartitionReplay], start: usize) -> Option<usize> {
    (0..partitions.len())
        .map(|offset| (start + offset) % partitions.len())
        .find(|index| !partitions[*index].frames.is_empty())
}

async fn load_segment(
    store: &Arc<dyn CheckpointStore>,
    segment: &BoundOutputSegment,
) -> Result<Vec<RecordBatch>, DbError> {
    tracing::trace!(
        stream_generation = %segment.reference.stream_generation,
        partition = segment.reference.partition.get(),
        first_sequence = segment.reference.first_sequence.get(),
        exclusive_end_sequence = segment.reference.exclusive_end_sequence.get(),
        encoded_length = segment.reference.encoded_length,
        "loading committed cluster subscription segment"
    );
    let bytes = tokio::time::timeout(
        GATEWAY_IO_TIMEOUT,
        store.load_subscription_segment(&segment.reference),
    )
    .await
    .map_err(|_| ClusterSubscriptionError::BackendUnavailable)?
    .map_err(|error| match error {
        laminar_core::checkpoint::CheckpointStoreError::ObjectStore(_) => {
            DbError::Subscription(ClusterSubscriptionError::BackendUnavailable)
        }
        _ => DbError::Subscription(ClusterSubscriptionError::SegmentCorrupt {
            partition: segment.reference.partition,
            first: segment.reference.first_sequence,
        }),
    })?
    .ok_or(ClusterSubscriptionError::SegmentMissing {
        partition: segment.reference.partition,
        first: segment.reference.first_sequence,
    })?;
    decode_bound_output_segment(&segment.reference, &bytes, &segment.binding()).map_err(|_| {
        ClusterSubscriptionError::SegmentCorrupt {
            partition: segment.reference.partition,
            first: segment.reference.first_sequence,
        }
        .into()
    })
}

#[allow(clippy::too_many_arguments)] // complete frame identity is kept out of user columns
async fn send_batch(
    cursor: &mut GatewayCursor,
    sender: &mpsc::Sender<ClusterReaderFrame>,
    budget: &Arc<Semaphore>,
    batch: RecordBatch,
    generation: StreamGeneration,
    partition: OutputPartitionId,
    partition_sequence: PartitionSequence,
    committed_epoch: u64,
) -> Result<(), ClusterSubscriptionError> {
    let bytes = batch.get_array_memory_size().max(1);
    let permits = u32::try_from(bytes).map_err(|_| ClusterSubscriptionError::SubscriberLagged)?;
    let permit = tokio::time::timeout(
        GATEWAY_SEND_TIMEOUT,
        Arc::clone(budget).acquire_many_owned(permits),
    )
    .await
    .map_err(|_| ClusterSubscriptionError::SubscriberLagged)?
    .map_err(|_| ClusterSubscriptionError::BackendUnavailable)?;
    let delivery_sequence = cursor.delivery_sequence;
    cursor.delivery_sequence = delivery_sequence.checked_add(1).ok_or_else(|| {
        ClusterSubscriptionError::ManifestCorrupt {
            reason: "gateway delivery sequence overflowed".into(),
        }
    })?;
    tokio::time::timeout(
        GATEWAY_SEND_TIMEOUT,
        sender.send(ClusterReaderFrame::Batch {
            batch,
            delivery_sequence,
            stream_generation: generation,
            partition,
            partition_sequence,
            committed_epoch,
            permit: Arc::new(permit),
        }),
    )
    .await
    .map_err(|_| ClusterSubscriptionError::SubscriberLagged)?
    .map_err(|_| ClusterSubscriptionError::BackendUnavailable)
}

async fn send_progress(
    cursor: &mut GatewayCursor,
    sender: &mpsc::Sender<ClusterReaderFrame>,
    generation: StreamGeneration,
    epoch: u64,
    checkpoint_id: u64,
) -> Result<(), ClusterSubscriptionError> {
    let delivery_sequence = cursor.delivery_sequence;
    cursor.delivery_sequence = delivery_sequence.checked_add(1).ok_or_else(|| {
        ClusterSubscriptionError::ManifestCorrupt {
            reason: "gateway delivery sequence overflowed".into(),
        }
    })?;
    tokio::time::timeout(
        GATEWAY_SEND_TIMEOUT,
        sender.send(ClusterReaderFrame::Progress {
            delivery_sequence,
            through_sequence: delivery_sequence,
            stream_generation: generation,
            epoch,
            checkpoint_id,
        }),
    )
    .await
    .map_err(|_| ClusterSubscriptionError::SubscriberLagged)?
    .map_err(|_| ClusterSubscriptionError::BackendUnavailable)
}
