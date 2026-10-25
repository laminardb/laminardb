//! Append-only views. Rows appended since the last capture stay in memory; each capture seals
//! them into one log segment that the checkpoint writes, and retention decides which committed
//! segments the checkpoint keeps. Committed segments are read back from the checkpoint store.

mod read;
mod retention;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use laminar_core::checkpoint::{EventTimeBounds, ViewLog};
use laminar_core::streaming::retained_arrow_bytes;

use super::admission::{size_overflow, MvLimits};
use crate::error::DbError;
pub(crate) use read::{LogRead, LogReadError, ViewLogReader};
pub(crate) use retention::ViewRetention;

/// Every view log has one partition until rows carry their origin vnode.
const LOG_PARTITION: u16 = 0;

/// Metadata of a committed segment; its rows live in the checkpoint store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentMeta {
    pub(crate) partition: u16,
    pub(crate) sequence: u64,
    pub(crate) rows: u64,
    pub(crate) event_time: Option<EventTimeBounds>,
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
}

pub(super) struct AppendLogState {
    retention: ViewRetention,
    staged: std::collections::VecDeque<LogBatch>,
    in_flight: Option<InFlightCapture>,
    /// Segments retained by the last committed checkpoint.
    committed: Vec<SegmentMeta>,
    next_sequence: u64,
    /// Greatest event-time frontier the view has reached.
    frontier: Option<i64>,
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
    pub(crate) retained: Vec<(u16, u64)>,
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
            next_sequence: 0,
            frontier: None,
            rows: 0,
            bytes: 0,
        }
    }

    /// The log a committed checkpoint retained, with nothing held in memory.
    pub(super) fn restored(retention: ViewRetention, log: &ViewLog) -> Self {
        let mut state = Self::new(retention);
        state.committed = log
            .segments
            .iter()
            .map(|segment| SegmentMeta {
                partition: segment.partition,
                sequence: segment.sequence,
                rows: segment.rows,
                event_time: segment.event_time,
            })
            .collect();
        state.next_sequence = log.next_sequence;
        state.frontier = log.frontier;
        state
    }

    pub(super) fn advance_frontier(&mut self, watermark: i64) {
        if watermark != i64::MIN && self.frontier.is_none_or(|frontier| watermark > frontier) {
            self.frontier = Some(watermark);
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
            if self.retention.expired(event_time, self.frontier) {
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
                .take_while(|old| self.retention.expired(old.event_time, self.frontier))
                .count(),
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
    /// checkpoint keeps. The live state changes only when the checkpoint commits or aborts.
    pub(super) fn capture(&mut self, checkpoint_id: u64) -> Result<ViewLogCapture, DbError> {
        if self
            .in_flight
            .as_ref()
            .is_some_and(|capture| !capture.sealed.is_empty())
        {
            return Err(DbError::Checkpoint(
                "an append view's previous capture has neither committed nor aborted".into(),
            ));
        }
        let staged: Vec<LogBatch> = self
            .staged
            .drain(..)
            .filter(|batch| !self.retention.expired(batch.event_time, self.frontier))
            .collect();
        let staged_rows = staged
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>();
        let staged_rows = u64::try_from(staged_rows).map_err(|_| size_overflow())?;
        let mut committed = self
            .retention
            .retained(&self.committed, self.frontier, staged_rows);
        let retained = committed
            .iter()
            .map(|segment| (segment.partition, segment.sequence))
            .collect();
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
        });
        Ok(ViewLogCapture {
            next_sequence: self.next_sequence,
            frontier: self.frontier,
            retained,
            sealed,
        })
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
    }

    /// Return the sealed rows of an attempt that will not commit to the front of the stage.
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
        let committed = self.retention.retained(
            &self.committed,
            self.frontier,
            u64::try_from(memory_rows).unwrap_or(u64::MAX),
        );
        (committed, memory, self.frontier)
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
