use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use laminar_core::checkpoint::{
    CheckpointAttempt, NodePartitionRange, OutputDistributionCertificate, OutputFrameId,
    OutputPartitionId, PartitionSequence, StreamGeneration,
};

use super::OutputWriterAuthority;
use crate::error::DbError;
use crate::subscription::{
    CertifiedSubscriptionFrontiers, ClusterSubscriptionError, PreparedSubscriptionOutput,
};

const MAX_OUTPUT_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MAX_PENDING_PARTITION_BYTES: usize = 32 * 1024 * 1024;
const MAX_PENDING_STREAM_BYTES: usize = 128 * 1024 * 1024;
const MAX_PENDING_OUTPUT_BYTES: usize = 256 * 1024 * 1024;
const MAX_PENDING_OUTPUT_FRAMES: usize = 65_536;

#[derive(Debug, Clone)]
pub(crate) struct BufferedSubscriptionFrame {
    pub(crate) id: OutputFrameId,
    pub(crate) batch: RecordBatch,
    pub(crate) authority: OutputWriterAuthority,
    retained_bytes: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedPartitionSubscriptionOutput {
    pub(crate) range: NodePartitionRange,
    pub(crate) frames: Vec<BufferedSubscriptionFrame>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedStreamSubscriptionOutput {
    pub(crate) certificate: Arc<OutputDistributionCertificate>,
    pub(crate) partitions: Vec<PreparedPartitionSubscriptionOutput>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedNodeSubscriptionOutput {
    pub(crate) attempt: CheckpointAttempt,
    pub(crate) streams: Vec<PreparedStreamSubscriptionOutput>,
    retained_bytes: usize,
    frame_count: usize,
}

#[derive(Default)]
struct PartitionBuffer {
    frames: Vec<BufferedSubscriptionFrame>,
    retained_bytes: usize,
}

struct StreamBuffer {
    certificate: Arc<OutputDistributionCertificate>,
    partitions: BTreeMap<OutputPartitionId, PartitionBuffer>,
    retained_bytes: usize,
    frame_count: usize,
}

#[derive(Default)]
struct OutputBuffer {
    streams: BTreeMap<StreamGeneration, StreamBuffer>,
    retained_bytes: usize,
    frame_count: usize,
}

struct CycleAppend {
    appended: Vec<(StreamGeneration, OutputPartitionId, usize, usize)>,
}

/// Single-owner bounded state between aggregate emission and checkpoint persistence.
pub(crate) struct ClusterSubscriptionOutputState {
    certificates: BTreeMap<String, Arc<OutputDistributionCertificate>>,
    open: OutputBuffer,
    staged_cycle: Option<CycleAppend>,
    reserved_attempt: Option<CheckpointAttempt>,
    prepared: Option<Arc<PreparedNodeSubscriptionOutput>>,
}

impl Default for ClusterSubscriptionOutputState {
    fn default() -> Self {
        Self::new(Vec::new()).expect("an empty subscription certificate roster is canonical")
    }
}

impl ClusterSubscriptionOutputState {
    pub(crate) fn new(
        certificates: Vec<Arc<OutputDistributionCertificate>>,
    ) -> Result<Self, DbError> {
        let mut registered = BTreeMap::new();
        for certificate in certificates {
            if registered
                .insert(certificate.stream_id.clone(), certificate)
                .is_some()
            {
                return Err(DbError::Config(
                    "subscription output certificate roster contains a duplicate stream".into(),
                ));
            }
        }
        Ok(Self {
            certificates: registered,
            open: OutputBuffer::default(),
            staged_cycle: None,
            reserved_attempt: None,
            prepared: None,
        })
    }

    pub(crate) fn enabled(&self) -> bool {
        !self.certificates.is_empty()
    }

    pub(crate) fn stage_cycle(
        &mut self,
        outputs: Vec<PreparedSubscriptionOutput>,
        authority: OutputWriterAuthority,
    ) -> Result<(), DbError> {
        if self.staged_cycle.is_some() {
            return Err(DbError::Pipeline(
                "subscription output from the prior cycle is still staged".into(),
            ));
        }
        if outputs.is_empty() {
            return Ok(());
        }
        let plan = self.validate_cycle(&outputs)?;
        self.reserve_cycle_capacity(&plan)?;
        let mut appended = BTreeMap::<(StreamGeneration, OutputPartitionId), (usize, usize)>::new();
        for output in outputs {
            let generation = output.certificate.stream_generation;
            for frame in output.frames {
                if self.existing_frame(frame.id).is_some() {
                    continue;
                }
                let retained_bytes = frame.batch.get_array_memory_size();
                let stream = self.open.streams.get_mut(&generation).ok_or_else(|| {
                    DbError::Pipeline("subscription output stream reservation disappeared".into())
                })?;
                let partition =
                    stream
                        .partitions
                        .get_mut(&frame.id.partition)
                        .ok_or_else(|| {
                            DbError::Pipeline(
                                "subscription output partition reservation disappeared".into(),
                            )
                        })?;
                partition.frames.push(BufferedSubscriptionFrame {
                    id: frame.id,
                    batch: frame.batch,
                    authority,
                    retained_bytes,
                });
                partition.retained_bytes += retained_bytes;
                stream.retained_bytes += retained_bytes;
                stream.frame_count += 1;
                self.open.retained_bytes += retained_bytes;
                self.open.frame_count += 1;
                let totals = appended
                    .entry((generation, frame.id.partition))
                    .or_default();
                totals.0 += 1;
                totals.1 += retained_bytes;
            }
        }
        self.staged_cycle = Some(CycleAppend {
            appended: appended
                .into_iter()
                .map(|((generation, partition), (count, bytes))| {
                    (generation, partition, count, bytes)
                })
                .collect(),
        });
        Ok(())
    }

    fn validate_cycle(&self, outputs: &[PreparedSubscriptionOutput]) -> Result<CyclePlan, DbError> {
        let mut plan = CyclePlan::default();
        let mut expected =
            BTreeMap::<(StreamGeneration, OutputPartitionId), PartitionSequence>::new();
        let mut seen_streams = BTreeMap::<StreamGeneration, &str>::new();
        let mut seen_frames = BTreeMap::<OutputFrameId, &RecordBatch>::new();
        for output in outputs {
            let certificate = self
                .certificates
                .get(&output.certificate.stream_id)
                .filter(|expected| expected.as_ref() == output.certificate.as_ref())
                .ok_or_else(|| {
                    DbError::Pipeline(format!(
                        "subscription output for '{}' has no matching planner certificate",
                        output.certificate.stream_id
                    ))
                })?;
            if seen_streams
                .insert(certificate.stream_generation, &certificate.stream_id)
                .is_some()
            {
                return Err(DbError::Pipeline(format!(
                    "subscription output for '{}' appears more than once in one cycle",
                    certificate.stream_id
                )));
            }
            for frame in &output.frames {
                self.validate_frame(certificate, frame.id, &frame.batch)?;
                if let Some(existing) = seen_frames.insert(frame.id, &frame.batch) {
                    if existing == &frame.batch {
                        continue;
                    }
                    return Err(ClusterSubscriptionError::ConflictingDuplicateSequence.into());
                }
                let key = (frame.id.stream_generation, frame.id.partition);
                let next = expected.entry(key).or_insert_with(|| {
                    self.next_sequence(key.0, key.1)
                        .unwrap_or(frame.id.sequence)
                });
                if frame.id.sequence < *next {
                    match self.existing_frame(frame.id) {
                        Some(existing) if existing.batch == frame.batch => continue,
                        Some(_) => {
                            return Err(
                                ClusterSubscriptionError::ConflictingDuplicateSequence.into()
                            );
                        }
                        None => {
                            return Err(ClusterSubscriptionError::PartitionSequenceGap {
                                partition: frame.id.partition,
                                expected: *next,
                                actual: frame.id.sequence,
                            }
                            .into());
                        }
                    }
                }
                if frame.id.sequence != *next {
                    return Err(ClusterSubscriptionError::PartitionSequenceGap {
                        partition: frame.id.partition,
                        expected: *next,
                        actual: frame.id.sequence,
                    }
                    .into());
                }
                let retained_bytes = frame.batch.get_array_memory_size();
                plan.add(key, retained_bytes)?;
                *next = next.checked_next().map_err(|error| {
                    DbError::Checkpoint(format!("advance subscription sequence: {error}"))
                })?;
            }
        }
        self.validate_cycle_bounds(&plan)?;
        Ok(plan)
    }

    fn validate_frame(
        &self,
        certificate: &OutputDistributionCertificate,
        id: OutputFrameId,
        batch: &RecordBatch,
    ) -> Result<(), DbError> {
        if id.stream_generation != certificate.stream_generation
            || !certificate.distribution.contains(id.partition)
            || batch.num_rows() == 0
        {
            return Err(DbError::Checkpoint(
                "subscription output frame metadata is inconsistent".into(),
            ));
        }
        let retained_bytes = batch.get_array_memory_size();
        if retained_bytes == 0 || retained_bytes > MAX_OUTPUT_FRAME_BYTES {
            return Err(DbError::Checkpoint(format!(
                "subscription output frame retains {retained_bytes} bytes; maximum is {MAX_OUTPUT_FRAME_BYTES}"
            )));
        }
        let schema_fingerprint =
            crate::pipeline_identity::subscription_schema_fingerprint(&batch.schema())?;
        if schema_fingerprint != certificate.schema_fingerprint {
            return Err(ClusterSubscriptionError::SchemaMismatch.into());
        }
        Ok(())
    }

    fn validate_cycle_bounds(&self, plan: &CyclePlan) -> Result<(), DbError> {
        let retained = self
            .retained_bytes()
            .checked_add(plan.retained_bytes)
            .filter(|bytes| *bytes <= MAX_PENDING_OUTPUT_BYTES)
            .ok_or_else(|| resource_error("total pending output", MAX_PENDING_OUTPUT_BYTES))?;
        let frames = self
            .frame_count()
            .checked_add(plan.frame_count)
            .filter(|frames| *frames <= MAX_PENDING_OUTPUT_FRAMES)
            .ok_or_else(|| resource_error("pending output frames", MAX_PENDING_OUTPUT_FRAMES))?;
        let _ = (retained, frames);
        for ((generation, partition), addition) in &plan.partitions {
            let (partition_bytes, stream_bytes) =
                self.open.streams.get(generation).map_or((0, 0), |stream| {
                    (
                        stream
                            .partitions
                            .get(partition)
                            .map_or(0, |partition| partition.retained_bytes),
                        stream.retained_bytes,
                    )
                });
            if partition_bytes.saturating_add(addition.retained_bytes) > MAX_PENDING_PARTITION_BYTES
            {
                return Err(resource_error(
                    "pending partition output",
                    MAX_PENDING_PARTITION_BYTES,
                ));
            }
            let stream_addition = plan
                .streams
                .get(generation)
                .map_or(0, |stream| stream.retained_bytes);
            if stream_bytes.saturating_add(stream_addition) > MAX_PENDING_STREAM_BYTES {
                return Err(resource_error(
                    "pending stream output",
                    MAX_PENDING_STREAM_BYTES,
                ));
            }
        }
        Ok(())
    }

    fn reserve_cycle_capacity(&mut self, plan: &CyclePlan) -> Result<(), DbError> {
        for ((generation, partition), addition) in &plan.partitions {
            let certificate = self
                .certificates
                .values()
                .find(|certificate| certificate.stream_generation == *generation)
                .cloned()
                .ok_or_else(|| {
                    DbError::Pipeline("subscription output generation is not registered".into())
                })?;
            let stream = self
                .open
                .streams
                .entry(*generation)
                .or_insert_with(|| StreamBuffer {
                    certificate,
                    partitions: BTreeMap::new(),
                    retained_bytes: 0,
                    frame_count: 0,
                });
            stream
                .partitions
                .entry(*partition)
                .or_default()
                .frames
                .try_reserve_exact(addition.frame_count)
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "reserve bounded subscription output frames: {error}"
                    ))
                })?;
        }
        Ok(())
    }

    pub(crate) fn commit_cycle(&mut self) {
        self.staged_cycle = None;
    }

    pub(crate) fn abort_cycle(&mut self) {
        let Some(staged) = self.staged_cycle.take() else {
            return;
        };
        for (generation, partition_id, count, bytes) in staged.appended.into_iter().rev() {
            let Some(stream) = self.open.streams.get_mut(&generation) else {
                continue;
            };
            if let Some(partition) = stream.partitions.get_mut(&partition_id) {
                let keep = partition.frames.len().saturating_sub(count);
                partition.frames.truncate(keep);
                partition.retained_bytes = partition.retained_bytes.saturating_sub(bytes);
                if partition.frames.is_empty() {
                    stream.partitions.remove(&partition_id);
                }
            }
            stream.retained_bytes = stream.retained_bytes.saturating_sub(bytes);
            stream.frame_count = stream.frame_count.saturating_sub(count);
            if stream.partitions.is_empty() {
                self.open.streams.remove(&generation);
            }
            self.open.retained_bytes = self.open.retained_bytes.saturating_sub(bytes);
            self.open.frame_count = self.open.frame_count.saturating_sub(count);
        }
    }

    pub(crate) fn reserve_checkpoint(&mut self, attempt: CheckpointAttempt) -> Result<(), DbError> {
        if !self.enabled() {
            return Ok(());
        }
        if !attempt.is_canonical()
            || self.reserved_attempt.replace(attempt).is_some()
            || self.prepared.is_some()
        {
            return Err(DbError::Checkpoint(
                "subscription output checkpoint reservation overlaps another attempt".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn prepare_checkpoint(
        &mut self,
        attempt: CheckpointAttempt,
        captures: Vec<CertifiedSubscriptionFrontiers>,
    ) -> Result<Option<Arc<PreparedNodeSubscriptionOutput>>, DbError> {
        if !self.enabled() {
            return Ok(None);
        }
        if self.staged_cycle.is_some() || self.reserved_attempt != Some(attempt) {
            return Err(DbError::Checkpoint(
                "subscription output is not at its reserved checkpoint boundary".into(),
            ));
        }
        if let Some(prepared) = self.prepared.as_ref() {
            return (prepared.attempt == attempt)
                .then(|| Some(Arc::clone(prepared)))
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "subscription output has a different prepared checkpoint".into(),
                    )
                });
        }
        self.validate_frontier_captures(&captures)?;
        let prepared = Arc::new(self.take_prepared_checkpoint(attempt, captures));
        self.prepared = Some(Arc::clone(&prepared));
        Ok(Some(prepared))
    }

    fn validate_frontier_captures(
        &self,
        captures: &[CertifiedSubscriptionFrontiers],
    ) -> Result<(), DbError> {
        if captures.len() != self.certificates.len()
            || !captures
                .windows(2)
                .all(|pair| pair[0].certificate.stream_id < pair[1].certificate.stream_id)
        {
            return Err(DbError::Checkpoint(
                "subscription frontier capture differs from the certified stream roster".into(),
            ));
        }
        for capture in captures {
            let expected = self
                .certificates
                .get(&capture.certificate.stream_id)
                .filter(|expected| expected.as_ref() == capture.certificate.as_ref())
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "subscription frontier certificate changed before capture".into(),
                    )
                })?;
            if !capture
                .frontiers
                .windows(2)
                .all(|pair| pair[0].partition < pair[1].partition)
            {
                return Err(DbError::Checkpoint(
                    "subscription checkpoint frontiers are not canonical".into(),
                ));
            }
            let buffered = self.open.streams.get(&expected.stream_generation);
            if let Some(buffered) = buffered {
                if buffered.certificate.as_ref() != expected.as_ref() {
                    return Err(DbError::Checkpoint(
                        "buffered subscription output certificate changed".into(),
                    ));
                }
                for (partition_id, partition) in &buffered.partitions {
                    let frontier = capture
                        .frontiers
                        .binary_search_by_key(partition_id, |frontier| frontier.partition)
                        .ok()
                        .map(|index| capture.frontiers[index].through_sequence)
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "subscription checkpoint omits buffered partition {}",
                                partition_id.get()
                            ))
                        })?;
                    validate_partition_cut(*partition_id, partition, frontier)?;
                }
            }
        }
        if self.open.streams.keys().any(|generation| {
            !captures
                .iter()
                .any(|capture| capture.certificate.stream_generation == *generation)
        }) {
            return Err(DbError::Checkpoint(
                "subscription checkpoint omits a buffered stream generation".into(),
            ));
        }
        Ok(())
    }

    fn take_prepared_checkpoint(
        &mut self,
        attempt: CheckpointAttempt,
        captures: Vec<CertifiedSubscriptionFrontiers>,
    ) -> PreparedNodeSubscriptionOutput {
        let mut open = std::mem::take(&mut self.open);
        let mut streams = Vec::with_capacity(captures.len());
        for capture in captures {
            let mut buffered = open.streams.remove(&capture.certificate.stream_generation);
            let mut partitions = Vec::with_capacity(capture.frontiers.len());
            for frontier in capture.frontiers {
                let frames = buffered
                    .as_mut()
                    .and_then(|stream| stream.partitions.remove(&frontier.partition))
                    .map_or_else(Vec::new, |partition| partition.frames);
                let first_sequence = frames
                    .first()
                    .map_or(frontier.through_sequence, |frame| frame.id.sequence);
                partitions.push(PreparedPartitionSubscriptionOutput {
                    range: NodePartitionRange {
                        partition: frontier.partition,
                        first_sequence,
                        through_sequence: frontier.through_sequence,
                    },
                    frames,
                });
            }
            streams.push(PreparedStreamSubscriptionOutput {
                certificate: capture.certificate,
                partitions,
            });
            debug_assert!(buffered.is_none_or(|stream| stream.partitions.is_empty()));
        }
        debug_assert!(open.streams.is_empty());
        PreparedNodeSubscriptionOutput {
            attempt,
            streams,
            retained_bytes: open.retained_bytes,
            frame_count: open.frame_count,
        }
    }

    pub(crate) fn commit_checkpoint(&mut self, attempt: CheckpointAttempt) -> Result<(), DbError> {
        if !self.enabled() {
            return Ok(());
        }
        let prepared = self.prepared.take().ok_or_else(|| {
            DbError::Checkpoint("subscription output checkpoint was not prepared".into())
        })?;
        if prepared.attempt != attempt || self.reserved_attempt != Some(attempt) {
            self.prepared = Some(prepared);
            return Err(DbError::Checkpoint(
                "subscription output checkpoint commit identity mismatch".into(),
            ));
        }
        self.reserved_attempt = None;
        Ok(())
    }

    pub(crate) fn abort_checkpoint(&mut self, attempt: CheckpointAttempt) -> Result<(), DbError> {
        if !self.enabled() {
            return Ok(());
        }
        if self.reserved_attempt != Some(attempt) {
            return Ok(());
        }
        self.reserved_attempt = None;
        let Some(prepared) = self.prepared.take() else {
            return Ok(());
        };
        if prepared.attempt != attempt {
            self.prepared = Some(prepared);
            return Err(DbError::Checkpoint(
                "subscription output checkpoint abort identity mismatch".into(),
            ));
        }
        self.restore_prepared(prepared)
    }

    fn restore_prepared(
        &mut self,
        prepared: Arc<PreparedNodeSubscriptionOutput>,
    ) -> Result<(), DbError> {
        let prepared = Arc::try_unwrap(prepared).unwrap_or_else(|shared| (*shared).clone());
        for stream in prepared.streams {
            let target = self
                .open
                .streams
                .entry(stream.certificate.stream_generation)
                .or_insert_with(|| StreamBuffer {
                    certificate: Arc::clone(&stream.certificate),
                    partitions: BTreeMap::new(),
                    retained_bytes: 0,
                    frame_count: 0,
                });
            for mut partition in stream.partitions {
                if partition.frames.is_empty() {
                    continue;
                }
                let current = target.partitions.remove(&partition.range.partition);
                let current_count = current.as_ref().map_or(0, |buffer| buffer.frames.len());
                partition
                    .frames
                    .try_reserve_exact(current_count)
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "restore aborted subscription output cut: {error}"
                        ))
                    })?;
                if let Some(current) = current {
                    partition.frames.extend(current.frames);
                }
                let restored_bytes = partition
                    .frames
                    .iter()
                    .map(|frame| frame.retained_bytes)
                    .sum::<usize>();
                target.partitions.insert(
                    partition.range.partition,
                    PartitionBuffer {
                        frames: partition.frames,
                        retained_bytes: restored_bytes,
                    },
                );
            }
            target.retained_bytes = target
                .partitions
                .values()
                .map(|partition| partition.retained_bytes)
                .sum();
            target.frame_count = target
                .partitions
                .values()
                .map(|partition| partition.frames.len())
                .sum();
        }
        self.recompute_open_totals();
        Ok(())
    }

    fn recompute_open_totals(&mut self) {
        self.open.retained_bytes = self
            .open
            .streams
            .values()
            .map(|stream| stream.retained_bytes)
            .sum();
        self.open.frame_count = self
            .open
            .streams
            .values()
            .map(|stream| stream.frame_count)
            .sum();
    }

    fn next_sequence(
        &self,
        generation: StreamGeneration,
        partition: OutputPartitionId,
    ) -> Option<PartitionSequence> {
        self.open
            .streams
            .get(&generation)
            .and_then(|stream| stream.partitions.get(&partition))
            .and_then(|partition| partition.frames.last())
            .or_else(|| {
                self.prepared
                    .as_ref()
                    .and_then(|prepared| prepared_stream(prepared, generation))
                    .and_then(|stream| prepared_partition(stream, partition))
                    .and_then(|partition| partition.frames.last())
            })
            .and_then(|frame| frame.id.sequence.checked_next().ok())
    }

    fn existing_frame(&self, id: OutputFrameId) -> Option<&BufferedSubscriptionFrame> {
        self.open
            .streams
            .get(&id.stream_generation)
            .and_then(|stream| stream.partitions.get(&id.partition))
            .and_then(|partition| partition.frames.iter().find(|frame| frame.id == id))
            .or_else(|| {
                self.prepared
                    .as_ref()
                    .and_then(|prepared| prepared_stream(prepared, id.stream_generation))
                    .and_then(|stream| prepared_partition(stream, id.partition))
                    .and_then(|partition| partition.frames.iter().find(|frame| frame.id == id))
            })
    }

    fn retained_bytes(&self) -> usize {
        self.open.retained_bytes.saturating_add(
            self.prepared
                .as_ref()
                .map_or(0, |prepared| prepared.retained_bytes),
        )
    }

    fn frame_count(&self) -> usize {
        self.open.frame_count.saturating_add(
            self.prepared
                .as_ref()
                .map_or(0, |prepared| prepared.frame_count),
        )
    }
}

#[derive(Default)]
struct CyclePlan {
    partitions: BTreeMap<(StreamGeneration, OutputPartitionId), PlanCount>,
    streams: BTreeMap<StreamGeneration, PlanCount>,
    retained_bytes: usize,
    frame_count: usize,
}

#[derive(Default)]
struct PlanCount {
    retained_bytes: usize,
    frame_count: usize,
}

impl CyclePlan {
    fn add(
        &mut self,
        key: (StreamGeneration, OutputPartitionId),
        retained_bytes: usize,
    ) -> Result<(), DbError> {
        add_plan_count(self.partitions.entry(key).or_default(), retained_bytes)?;
        add_plan_count(self.streams.entry(key.0).or_default(), retained_bytes)?;
        self.retained_bytes = self
            .retained_bytes
            .checked_add(retained_bytes)
            .ok_or_else(|| DbError::Checkpoint("subscription output byte count overflow".into()))?;
        self.frame_count = self.frame_count.checked_add(1).ok_or_else(|| {
            DbError::Checkpoint("subscription output frame count overflow".into())
        })?;
        Ok(())
    }
}

fn add_plan_count(count: &mut PlanCount, retained_bytes: usize) -> Result<(), DbError> {
    count.retained_bytes = count
        .retained_bytes
        .checked_add(retained_bytes)
        .ok_or_else(|| DbError::Checkpoint("subscription output byte count overflow".into()))?;
    count.frame_count = count
        .frame_count
        .checked_add(1)
        .ok_or_else(|| DbError::Checkpoint("subscription output frame count overflow".into()))?;
    Ok(())
}

fn validate_partition_cut(
    partition_id: OutputPartitionId,
    partition: &PartitionBuffer,
    frontier: PartitionSequence,
) -> Result<(), DbError> {
    let Some(first) = partition.frames.first() else {
        return Ok(());
    };
    let mut expected = first.id.sequence;
    for frame in &partition.frames {
        if frame.id.partition != partition_id {
            return Err(ClusterSubscriptionError::ManifestCorrupt {
                reason: "buffered frame belongs to a different output partition".into(),
            }
            .into());
        }
        if frame.id.sequence != expected {
            return Err(ClusterSubscriptionError::PartitionSequenceGap {
                partition: partition_id,
                expected,
                actual: frame.id.sequence,
            }
            .into());
        }
        expected = expected.checked_next().map_err(|error| {
            DbError::Checkpoint(format!("advance subscription sequence: {error}"))
        })?;
    }
    if expected != frontier {
        return Err(ClusterSubscriptionError::PartitionSequenceGap {
            partition: partition_id,
            expected: frontier,
            actual: expected,
        }
        .into());
    }
    Ok(())
}

fn prepared_stream(
    prepared: &PreparedNodeSubscriptionOutput,
    generation: StreamGeneration,
) -> Option<&PreparedStreamSubscriptionOutput> {
    prepared
        .streams
        .iter()
        .find(|stream| stream.certificate.stream_generation == generation)
}

fn prepared_partition(
    stream: &PreparedStreamSubscriptionOutput,
    partition: OutputPartitionId,
) -> Option<&PreparedPartitionSubscriptionOutput> {
    stream
        .partitions
        .binary_search_by_key(&partition, |partition| partition.range.partition)
        .ok()
        .map(|index| &stream.partitions[index])
}

fn resource_error(resource: &str, limit: usize) -> DbError {
    DbError::Checkpoint(format!(
        "cluster subscription {resource} reached its bounded {limit}-byte/count limit"
    ))
}
