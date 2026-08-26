use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use laminar_core::checkpoint::{
    CheckpointAttempt, NodePartitionRange, OutputDistributionCertificate, OutputFrameId,
    OutputPartitionId, PartitionSequence, StreamGeneration,
};
use laminar_core::cluster::control::LocalProcessAuthorityIdentity;

use super::output_admission::{
    resource_error, validate_partition_cut, CyclePlan, MAX_OUTPUT_FRAME_BYTES,
    MAX_PENDING_OUTPUT_BYTES, MAX_PENDING_OUTPUT_FRAMES, MAX_PENDING_PARTITION_BYTES,
    MAX_PENDING_STREAM_BYTES,
};
use super::OutputWriterAuthority;
use crate::error::DbError;
use crate::pipeline::callback::ExternalOutputPressure;
use crate::subscription::{
    CertifiedSubscriptionFrontiers, ClusterSubscriptionError, PreparedSubscriptionOutput,
};

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
    retained_bytes: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedStreamSubscriptionOutput {
    pub(crate) certificate: Arc<OutputDistributionCertificate>,
    pub(crate) partitions: Vec<PreparedPartitionSubscriptionOutput>,
    retained_bytes: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedNodeSubscriptionOutput {
    pub(crate) attempt: CheckpointAttempt,
    pub(crate) streams: Vec<PreparedStreamSubscriptionOutput>,
    retained_bytes: usize,
    frame_count: usize,
}

#[derive(Default)]
pub(super) struct PartitionBuffer {
    pub(super) frames: Vec<BufferedSubscriptionFrame>,
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
    bound_process: Option<LocalProcessAuthorityIdentity>,
    open: OutputBuffer,
    staged_cycle: Option<CycleAppend>,
    reserved_attempt: Option<CheckpointAttempt>,
    pub(super) prepared: Option<Arc<PreparedNodeSubscriptionOutput>>,
    pub(super) output_pressure: ExternalOutputPressure,
}

impl Default for ClusterSubscriptionOutputState {
    fn default() -> Self {
        Self::new(Vec::new(), None).expect("an empty subscription certificate roster is canonical")
    }
}

impl ClusterSubscriptionOutputState {
    pub(crate) fn new(
        certificates: Vec<Arc<OutputDistributionCertificate>>,
        bound_process: Option<LocalProcessAuthorityIdentity>,
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
            bound_process,
            open: OutputBuffer::default(),
            staged_cycle: None,
            reserved_attempt: None,
            prepared: None,
            output_pressure: ExternalOutputPressure::Normal,
        })
    }

    pub(crate) fn enabled(&self) -> bool {
        !self.certificates.is_empty()
    }

    pub(crate) fn bound_process(&self) -> Option<LocalProcessAuthorityIdentity> {
        self.bound_process
    }

    #[cfg(test)]
    pub(crate) fn replace_bound_process_for_test(
        &mut self,
        process: LocalProcessAuthorityIdentity,
    ) {
        self.bound_process = Some(process);
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
        self.update_output_pressure_after_cycle(&plan);
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
        self.validate_cycle_bounds(&mut plan)?;
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

    fn validate_cycle_bounds(&self, plan: &mut CyclePlan) -> Result<(), DbError> {
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
        let (_, at_high_water) = self.pressure_target();
        let mut reaches_high_water = at_high_water(retained, MAX_PENDING_OUTPUT_BYTES)
            || at_high_water(frames, MAX_PENDING_OUTPUT_FRAMES);
        for ((generation, partition), addition) in &plan.partitions {
            let partition_bytes = self.partition_retained_bytes(*generation, *partition);
            let stream_bytes = self.stream_retained_bytes(*generation);
            let partition_bytes = partition_bytes.saturating_add(addition.retained_bytes);
            if partition_bytes > MAX_PENDING_PARTITION_BYTES {
                return Err(resource_error(
                    "pending partition output",
                    MAX_PENDING_PARTITION_BYTES,
                ));
            }
            let stream_addition = plan
                .streams
                .get(generation)
                .map_or(0, |stream| stream.retained_bytes);
            let stream_bytes = stream_bytes.saturating_add(stream_addition);
            if stream_bytes > MAX_PENDING_STREAM_BYTES {
                return Err(resource_error(
                    "pending stream output",
                    MAX_PENDING_STREAM_BYTES,
                ));
            }
            reaches_high_water |= at_high_water(partition_bytes, MAX_PENDING_PARTITION_BYTES)
                || at_high_water(stream_bytes, MAX_PENDING_STREAM_BYTES);
        }
        plan.reaches_high_water = reaches_high_water;
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
        self.recompute_output_pressure();
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
        self.recompute_output_pressure();
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
                let partition = buffered
                    .as_mut()
                    .and_then(|stream| stream.partitions.remove(&frontier.partition));
                let retained_bytes = partition
                    .as_ref()
                    .map_or(0, |partition| partition.retained_bytes);
                let frames = partition.map_or_else(Vec::new, |partition| partition.frames);
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
                    retained_bytes,
                });
            }
            let retained_bytes = partitions
                .iter()
                .map(|partition| partition.retained_bytes)
                .sum();
            streams.push(PreparedStreamSubscriptionOutput {
                certificate: capture.certificate,
                partitions,
                retained_bytes,
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
        self.recompute_output_pressure();
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
        let result = self.restore_prepared(prepared);
        self.recompute_output_pressure();
        result
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

    pub(crate) fn retained_bytes(&self) -> usize {
        self.open.retained_bytes.saturating_add(
            self.prepared
                .as_ref()
                .map_or(0, |prepared| prepared.retained_bytes),
        )
    }

    pub(crate) fn output_pressure(&self) -> ExternalOutputPressure {
        self.output_pressure
    }

    pub(super) fn frame_count(&self) -> usize {
        self.open.frame_count.saturating_add(
            self.prepared
                .as_ref()
                .map_or(0, |prepared| prepared.frame_count),
        )
    }

    pub(super) fn all_output_reaches_high_water(
        &self,
        at_high_water: fn(usize, usize) -> bool,
    ) -> bool {
        if at_high_water(self.retained_bytes(), MAX_PENDING_OUTPUT_BYTES)
            || at_high_water(self.frame_count(), MAX_PENDING_OUTPUT_FRAMES)
        {
            return true;
        }
        if self.open.streams.iter().any(|(generation, stream)| {
            at_high_water(
                self.stream_retained_bytes(*generation),
                MAX_PENDING_STREAM_BYTES,
            ) || stream.partitions.keys().any(|partition| {
                at_high_water(
                    self.partition_retained_bytes(*generation, *partition),
                    MAX_PENDING_PARTITION_BYTES,
                )
            })
        }) {
            return true;
        }
        self.prepared.as_ref().is_some_and(|prepared| {
            prepared.streams.iter().any(|stream| {
                let generation = stream.certificate.stream_generation;
                at_high_water(
                    self.stream_retained_bytes(generation),
                    MAX_PENDING_STREAM_BYTES,
                ) || stream.partitions.iter().any(|partition| {
                    at_high_water(
                        self.partition_retained_bytes(generation, partition.range.partition),
                        MAX_PENDING_PARTITION_BYTES,
                    )
                })
            })
        })
    }

    pub(super) fn stream_retained_bytes(&self, generation: StreamGeneration) -> usize {
        self.open
            .streams
            .get(&generation)
            .map_or(0, |stream| stream.retained_bytes)
            .saturating_add(
                self.prepared
                    .as_ref()
                    .and_then(|prepared| prepared_stream(prepared, generation))
                    .map_or(0, |stream| stream.retained_bytes),
            )
    }

    pub(super) fn partition_retained_bytes(
        &self,
        generation: StreamGeneration,
        partition: OutputPartitionId,
    ) -> usize {
        self.open
            .streams
            .get(&generation)
            .and_then(|stream| stream.partitions.get(&partition))
            .map_or(0, |partition| partition.retained_bytes)
            .saturating_add(
                self.prepared
                    .as_ref()
                    .and_then(|prepared| prepared_stream(prepared, generation))
                    .and_then(|stream| prepared_partition(stream, partition))
                    .map_or(0, |partition| partition.retained_bytes),
            )
    }
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
