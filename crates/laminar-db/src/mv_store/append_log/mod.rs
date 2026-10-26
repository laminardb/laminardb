//! Append-only views. Rows appended since the last capture stay in memory; each capture seals
//! them into one log segment that the checkpoint writes, and retention decides which committed
//! segments the checkpoint keeps. Committed segments are read back from the checkpoint store,
//! and compaction merges them.

mod compaction;
mod read;
mod retention;

use std::ops::Range;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use laminar_core::checkpoint::{AdoptedSegment, EventTimeBounds, ViewLog, ViewLogSegment};
use laminar_core::streaming::retained_arrow_bytes;

use super::admission::{size_overflow, MvLimits};
use crate::error::DbError;
use crate::operator_graph::InputFrontier;
pub(crate) use compaction::{spawn_compactor, TARGET_SEGMENT_BYTES};
use compaction::{CompactedSegment, PreparedCompaction};
pub(crate) use read::{LogRead, LogReadError};
pub(crate) use retention::ViewRetention;

/// Every view log has one partition until rows carry their origin vnode.
pub(super) const LOG_PARTITION: u16 = 0;

/// The event-time frontier a log's retention follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogHorizon {
    /// The view's own frontier on this node.
    Local,
    /// In a cluster, the view's frontier at the newest committed cut, which every participant
    /// shares, so which participant holds a row never decides whether retention keeps it.
    Committed(Option<i64>),
}

/// Metadata of a committed segment; its rows live in the checkpoint store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentMeta {
    pub(crate) partition: u16,
    pub(crate) sequence: u64,
    pub(crate) rows: u64,
    pub(crate) event_time: Option<EventTimeBounds>,
}

impl SegmentMeta {
    const fn position(&self) -> (u16, u64) {
        (self.partition, self.sequence)
    }
}

impl From<&ViewLogSegment> for SegmentMeta {
    fn from(segment: &ViewLogSegment) -> Self {
        Self {
            partition: segment.partition,
            sequence: segment.sequence,
            rows: segment.rows,
            event_time: segment.event_time,
        }
    }
}

#[derive(Clone)]
struct LogBatch {
    batch: RecordBatch,
    event_time: Option<EventTimeBounds>,
    bytes: usize,
}

/// The capture a checkpoint attempt took, installed when that checkpoint commits.
struct InFlightCapture {
    checkpoint_id: u64,
    committed: Vec<SegmentMeta>,
    sealed: Vec<LogBatch>,
    adopted: Vec<AdoptedSegment>,
    compacted: Option<Arc<PreparedCompaction>>,
}

impl InFlightCapture {
    /// Whether an abort must return anything to the log.
    fn holds_rows(&self) -> bool {
        !self.sealed.is_empty() || !self.adopted.is_empty()
    }
}

pub(super) struct AppendLogState {
    retention: ViewRetention,
    staged: std::collections::VecDeque<LogBatch>,
    in_flight: Option<InFlightCapture>,
    /// Segments retained by the last committed checkpoint.
    committed: Vec<SegmentMeta>,
    /// Segments adopted from participants that left, which the next capture lists unfiltered.
    adopted: Vec<AdoptedSegment>,
    next_sequence: u64,
    /// Greatest event-time frontier the view has reached on this node.
    frontier: Option<i64>,
    /// Whether every input of the view is idle; a capture reports it with the frontier.
    idle: bool,
    horizon: LogHorizon,
    /// A merge prepared from a committed checkpoint; the next capture that still retains its
    /// inputs writes it in their place.
    prepared: Option<Arc<PreparedCompaction>>,
    /// The oldest committed checkpoint whose segment positions name the segments this log
    /// holds: the commit that first held the log or last merged its segments. A merged segment
    /// takes its first input's position, so a merge prepared from an older checkpoint may name
    /// a position whose segment has since been replaced.
    positions_since: Option<u64>,
    /// Rows and retained bytes held in memory: staged and sealed batches.
    pub(super) rows: usize,
    pub(super) bytes: usize,
}

/// One cycle's validated append, applied only after every view in the cycle prepared.
pub(super) struct LogUpdate<'a> {
    incoming: smallvec::SmallVec<[LogBatchRef<'a>; 1]>,
    evicted: usize,
    rows: usize,
    bytes: usize,
}

struct LogBatchRef<'a> {
    batch: &'a RecordBatch,
    event_time: Option<EventTimeBounds>,
    bytes: usize,
}

/// One view's log at a checkpoint barrier; its new rows are encoded off the cycle.
pub(crate) struct ViewLogCapture {
    pub(crate) next_sequence: u64,
    pub(crate) frontier: Option<i64>,
    pub(crate) idle: bool,
    pub(crate) retained: Vec<(u16, u64)>,
    /// Adopted segments at their new sequences in partition 0.
    pub(crate) adopted: Vec<(u64, AdoptedSegment)>,
    pub(crate) compacted: Option<CompactedSegment>,
    pub(crate) sealed: Option<SealedSegment>,
}

pub(crate) struct SealedSegment {
    pub(crate) partition: u16,
    pub(crate) sequence: u64,
    pub(crate) rows: u64,
    pub(crate) event_time: Option<EventTimeBounds>,
    pub(crate) batches: Vec<RecordBatch>,
}

impl AppendLogState {
    pub(super) fn new(retention: ViewRetention) -> Self {
        Self {
            retention,
            staged: std::collections::VecDeque::new(),
            in_flight: None,
            committed: Vec::new(),
            adopted: Vec::new(),
            next_sequence: 0,
            frontier: None,
            idle: false,
            horizon: LogHorizon::Local,
            prepared: None,
            positions_since: None,
            rows: 0,
            bytes: 0,
        }
    }

    /// The log committed checkpoint `checkpoint_id` retained, with nothing held in memory.
    pub(super) fn restored(retention: ViewRetention, log: &ViewLog, checkpoint_id: u64) -> Self {
        let mut state = Self::new(retention);
        state.committed = log.segments.iter().map(SegmentMeta::from).collect();
        state.next_sequence = log.next_sequence;
        state.frontier = log.frontier;
        state.positions_since = Some(checkpoint_id);
        state
    }

    pub(super) fn advance_frontier(&mut self, stream: InputFrontier) {
        if let Some(watermark) = stream
            .watermark
            .filter(|watermark| self.frontier.is_none_or(|frontier| *watermark > frontier))
        {
            self.frontier = Some(watermark);
        }
        self.idle = stream.idle;
    }

    pub(super) fn adopt(&mut self, segments: impl IntoIterator<Item = AdoptedSegment>) {
        self.adopted.extend(segments);
    }

    pub(super) fn set_horizon(&mut self, horizon: LogHorizon) {
        self.horizon = horizon;
    }

    /// The frontier retention keeps rows against.
    fn horizon_frontier(&self) -> Option<i64> {
        match self.horizon {
            LogHorizon::Local => self.frontier,
            LogHorizon::Committed(frontier) => frontier,
        }
    }

    pub(super) fn prepare<'a>(
        &self,
        name: &str,
        batches: &'a [RecordBatch],
        limits: MvLimits,
    ) -> Result<LogUpdate<'a>, DbError> {
        let (mut rows, mut bytes) = (self.rows, self.bytes);
        let mut incoming = smallvec::SmallVec::new();
        for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
            let event_time = self.retention.bounds(batch)?;
            if self.retention.expired(event_time, self.horizon_frontier()) {
                continue;
            }
            let charge = retained_arrow_bytes(batch);
            limits.validate(name, batch.num_rows(), charge)?;
            rows = rows
                .checked_add(batch.num_rows())
                .ok_or_else(size_overflow)?;
            bytes = bytes.checked_add(charge).ok_or_else(size_overflow)?;
            incoming.push(LogBatchRef {
                batch,
                event_time,
                bytes: charge,
            });
        }
        let evicted = self.evictable(rows);
        for old in self.staged.iter().take(evicted) {
            rows -= old.batch.num_rows();
            bytes -= old.bytes;
        }
        limits.validate(name, rows, bytes)?;
        Ok(LogUpdate {
            incoming,
            evicted,
            rows,
            bytes,
        })
    }

    /// Leading staged batches retention no longer needs once the log holds `rows` in memory.
    fn evictable(&self, rows: usize) -> usize {
        match &self.retention {
            ViewRetention::EventTime { .. } => self
                .staged
                .iter()
                .take_while(|old| {
                    self.retention
                        .expired(old.event_time, self.horizon_frontier())
                })
                .count(),
            ViewRetention::Unbounded { .. } => 0,
            ViewRetention::Rows(limit) => {
                let limit = usize::try_from(*limit).unwrap_or(usize::MAX);
                let sealed: usize = self.in_flight.as_ref().map_or(0, |capture| {
                    capture.sealed.iter().map(|old| old.batch.num_rows()).sum()
                });
                let mut remaining = rows - sealed;
                self.staged
                    .iter()
                    .take_while(|old| {
                        let keep = remaining - old.batch.num_rows();
                        let evict = keep >= limit;
                        if evict {
                            remaining = keep;
                        }
                        evict
                    })
                    .count()
            }
        }
    }

    pub(super) fn apply(&mut self, update: LogUpdate<'_>) {
        self.staged.drain(..update.evicted);
        self.staged
            .extend(update.incoming.into_iter().map(|incoming| LogBatch {
                batch: incoming.batch.clone(),
                event_time: incoming.event_time,
                bytes: incoming.bytes,
            }));
        self.rows = update.rows;
        self.bytes = update.bytes;
    }

    /// Seal the staged rows into the next segment and decide which committed segments the
    /// checkpoint keeps, writing the prepared merge in place of its inputs when `compact` allows
    /// and the checkpoint still retains them. The live state changes only when the checkpoint
    /// commits or aborts.
    pub(super) fn capture(
        &mut self,
        checkpoint_id: u64,
        compact: bool,
    ) -> Result<ViewLogCapture, DbError> {
        if self
            .in_flight
            .as_ref()
            .is_some_and(InFlightCapture::holds_rows)
        {
            return Err(DbError::Checkpoint(
                "an append view's previous capture has neither committed nor aborted".into(),
            ));
        }
        let horizon = self.horizon_frontier();
        let staged: Vec<LogBatch> = self
            .staged
            .drain(..)
            .filter(|batch| !self.retention.expired(batch.event_time, horizon))
            .collect();
        let staged_rows = staged
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>();
        let staged_rows = u64::try_from(staged_rows).map_err(|_| size_overflow())?;
        let mut committed = self
            .retention
            .retained(&self.committed, horizon, staged_rows);
        let merge = compact.then(|| self.prepared_merge(&committed)).flatten();
        let run = merge.as_ref().map(|(run, _)| run.clone());
        let compacted = merge
            .as_ref()
            .and_then(|(_, prepared)| prepared.output.clone());
        let mut retained: Vec<(u16, u64)> = committed.iter().map(SegmentMeta::position).collect();
        if let Some(run) = run {
            retained.drain(run.clone());
            committed.splice(run, compacted.iter().map(|segment| segment.meta.clone()));
        }
        let adopted = self.number_adopted(&mut committed)?;
        let sealed = if staged.is_empty() {
            None
        } else {
            let sequence = self.next_sequence;
            self.next_sequence = sequence.checked_add(1).ok_or_else(size_overflow)?;
            let event_time =
                staged
                    .iter()
                    .filter_map(|batch| batch.event_time)
                    .reduce(|left, right| EventTimeBounds {
                        min: left.min.min(right.min),
                        max: left.max.max(right.max),
                    });
            committed.push(SegmentMeta {
                partition: LOG_PARTITION,
                sequence,
                rows: staged_rows,
                event_time,
            });
            Some(SealedSegment {
                partition: LOG_PARTITION,
                sequence,
                rows: staged_rows,
                event_time,
                batches: staged.iter().map(|batch| batch.batch.clone()).collect(),
            })
        };
        self.recount(&staged);
        self.in_flight = Some(InFlightCapture {
            checkpoint_id,
            committed,
            sealed: staged,
            adopted: adopted.iter().map(|(_, segment)| segment.clone()).collect(),
            compacted: merge.map(|(_, prepared)| prepared),
        });
        Ok(ViewLogCapture {
            next_sequence: self.next_sequence,
            frontier: self.frontier,
            idle: self.idle,
            retained,
            adopted,
            compacted,
            sealed,
        })
    }

    /// Give each adopted segment the next sequence of partition 0 and list it in `committed`.
    /// Retention does not filter them: the leader requires a departed participant's segments
    /// verbatim in the first cut without it.
    fn number_adopted(
        &mut self,
        committed: &mut Vec<SegmentMeta>,
    ) -> Result<Vec<(u64, AdoptedSegment)>, DbError> {
        let count = u64::try_from(self.adopted.len()).map_err(|_| size_overflow())?;
        let first = self.next_sequence;
        self.next_sequence = first.checked_add(count).ok_or_else(size_overflow)?;
        Ok((first..)
            .zip(std::mem::take(&mut self.adopted))
            .map(|(sequence, adopted)| {
                committed.push(SegmentMeta {
                    partition: LOG_PARTITION,
                    sequence,
                    rows: adopted.segment.rows,
                    event_time: adopted.segment.event_time,
                });
                (sequence, adopted)
            })
            .collect())
    }

    /// Where the prepared merge's inputs sit in `committed`, when they all still do, in order,
    /// and its positions still name them.
    fn prepared_merge(
        &self,
        committed: &[SegmentMeta],
    ) -> Option<(Range<usize>, Arc<PreparedCompaction>)> {
        let prepared = self.prepared.as_ref()?;
        if self
            .positions_since
            .is_none_or(|since| prepared.base < since)
        {
            return None;
        }
        let first = *prepared.inputs.first()?;
        let start = committed
            .iter()
            .position(|segment| segment.position() == first)?;
        let run = start..start.checked_add(prepared.inputs.len())?;
        let held = committed.get(run.clone())?;
        held.iter()
            .map(SegmentMeta::position)
            .eq(prepared.inputs.iter().copied())
            .then(|| (run, Arc::clone(prepared)))
    }

    /// Bytes of the prepared merge the next capture may write.
    pub(super) fn prepared_bytes(&self) -> usize {
        self.prepared
            .as_ref()
            .and_then(|prepared| prepared.output.as_ref())
            .map_or(0, |segment| segment.payload.len())
    }

    pub(super) fn set_prepared(&mut self, prepared: Option<Arc<PreparedCompaction>>) {
        self.prepared = prepared;
    }

    /// Recount memory after capture moved `sealed` out of the staged queue, dropping any
    /// batch that left retention while staged.
    fn recount(&mut self, sealed: &[LogBatch]) {
        let held = sealed.iter().chain(self.staged.iter());
        self.rows = held.clone().map(|batch| batch.batch.num_rows()).sum();
        self.bytes = held.map(|batch| batch.bytes).sum();
    }

    /// Install the capture checkpoint `checkpoint_id` took, if it is the one in flight.
    pub(super) fn commit(&mut self, checkpoint_id: u64) {
        let Some(capture) = self
            .in_flight
            .take_if(|capture| capture.checkpoint_id == checkpoint_id)
        else {
            return;
        };
        self.committed = capture.committed;
        for batch in &capture.sealed {
            self.rows -= batch.batch.num_rows();
            self.bytes -= batch.bytes;
        }
        if capture.compacted.is_some() || self.positions_since.is_none() {
            self.positions_since = Some(checkpoint_id);
        }
        if let Some(compacted) = capture.compacted {
            self.prepared
                .take_if(|prepared| Arc::ptr_eq(prepared, &compacted));
        }
    }

    /// Return the sealed rows and adopted segments of an attempt that will not commit to the
    /// front of the log.
    pub(super) fn abort(&mut self, checkpoint_id: u64) {
        let Some(capture) = self
            .in_flight
            .take_if(|capture| capture.checkpoint_id == checkpoint_id)
        else {
            return;
        };
        for batch in capture.sealed.into_iter().rev() {
            self.staged.push_front(batch);
        }
        self.adopted.splice(0..0, capture.adopted);
    }

    /// Committed segments to read and the rows held in memory, in log order.
    pub(super) fn read_parts(&self) -> (Vec<SegmentMeta>, Vec<RecordBatch>, Option<i64>) {
        let sealed = self
            .in_flight
            .iter()
            .flat_map(|capture| capture.sealed.iter());
        let memory: Vec<RecordBatch> = sealed
            .chain(self.staged.iter())
            .map(|batch| batch.batch.clone())
            .collect();
        let memory_rows = memory.iter().map(RecordBatch::num_rows).sum::<usize>();
        let horizon = self.horizon_frontier();
        let committed = self.retention.retained(
            &self.committed,
            horizon,
            u64::try_from(memory_rows).unwrap_or(u64::MAX),
        );
        (committed, memory, horizon)
    }

    pub(super) const fn retention(&self) -> &ViewRetention {
        &self.retention
    }

    /// The log while every retained row is in memory.
    pub(super) fn memory_batch(&self, schema: &SchemaRef) -> Result<RecordBatch, DbError> {
        let (committed, memory, frontier) = self.read_parts();
        if !committed.is_empty() {
            return Err(DbError::MaterializedView(
                "an append-only view with committed segments is read through its log".into(),
            ));
        }
        let batches = self.retention.apply(memory, frontier)?;
        arrow::compute::concat_batches(schema, &batches)
            .map_err(|error| DbError::Storage(format!("MV batch concat: {error}")))
    }

    /// Retained bytes of the rows the next capture seals.
    pub(super) fn staged_bytes(&self) -> usize {
        self.staged.iter().map(|batch| batch.bytes).sum()
    }
}

#[cfg(test)]
mod tests;
