//! Compaction of append-only view logs. After each commit a background pass prepares, for each
//! view, one merge of consecutive committed segments into a single segment without the rows its
//! retention no longer keeps, and leaves it on the view's log. The next capture writes the
//! merged segment into that checkpoint's view-log object in place of its inputs, so the
//! checkpoint writer's fence and the committed index's references cover it like any segment,
//! and retention deletes the inputs' objects once no retained manifest references them.
//!
//! Merges follow aligned sequence blocks: a level-ℓ block spans `FANOUT^ℓ` sequences and its
//! segments merge once the log has moved past it, packed in order up to
//! `TARGET_SEGMENT_BYTES`. A row is rewritten about once per level until its segment fills,
//! and a log keeps about `FANOUT - 1` segments per open level plus its filled segments.

use std::cmp::Reverse;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use laminar_core::checkpoint::{ViewLog, ViewLogSegment};

use super::{LogReadError, SegmentMeta, ViewRetention};
use crate::checkpoint_coordinator::committed_reads::CommittedCuts;
use crate::error::DbError;
use crate::mv_store::checkpoint::batches_to_ipc_bounded;
use crate::mv_store::committed::{CommittedRead, CommittedViews, PinnedCut};
use crate::mv_store::{MvStorageMode, MvStore};

/// Sequences per block at each level above the one below it.
const FANOUT: u64 = 8;
/// Most payload bytes one merge reads, which bounds the segment it writes.
pub(crate) const TARGET_SEGMENT_BYTES: u64 = 16 << 20;
/// Most payload bytes one pass reads across every view.
const PASS_BYTES: u64 = 64 << 20;
/// How long one pass may take to pin, load and rewrite.
const PASS_DEADLINE: Duration = Duration::from_secs(30);

/// A merge prepared from committed checkpoint `base`: the positions of the segments it
/// replaces, and the segment replacing them, or `None` when retention keeps none of their rows.
pub(crate) struct PreparedCompaction {
    pub(super) base: u64,
    pub(super) inputs: Vec<(u16, u64)>,
    pub(super) output: Option<CompactedSegment>,
}

/// A merged segment, encoded, at its first input's position.
#[derive(Clone)]
pub(crate) struct CompactedSegment {
    pub(crate) meta: SegmentMeta,
    pub(crate) payload: Bytes,
}

/// Prepare merges after every commit until the coordinator publishing `cuts` is dropped.
pub(crate) fn spawn_compactor(store: Arc<parking_lot::RwLock<MvStore>>, mut cuts: CommittedCuts) {
    tokio::spawn(async move {
        while cuts.changed().await.is_ok() {
            prepare_pass(&store).await;
        }
    });
}

/// Prepare one merge per append view from the newest committed cut, within one pass's budget
/// and deadline, and leave the results on the views' logs. A view that fails is skipped; its
/// reads report damaged committed state.
async fn prepare_pass(store: &parking_lot::RwLock<MvStore>) {
    let (views, logs) = {
        let store = store.read();
        (store.committed_views(), store.append_logs_to_compact())
    };
    let Some(views) = views.filter(|_| !logs.is_empty()) else {
        return;
    };
    let deadline = Instant::now() + PASS_DEADLINE;
    let cut = match views.pin(CommittedRead::Latest, deadline).await {
        Ok(cut) => cut,
        Err(error) => {
            tracing::debug!(%error, "append-view log compaction found no committed cut");
            return;
        }
    };
    let mut budget = PASS_BYTES;
    let mut prepared = HashMap::new();
    for (view, schema, retention) in logs {
        let Some(log) = cut.local_log(&views, &view) else {
            continue;
        };
        let Some(run) = plan(&log.segments, log.next_sequence) else {
            continue;
        };
        let bytes = log.segments[run.clone()]
            .iter()
            .map(|segment| segment.range.length)
            .sum::<u64>();
        let Some(remaining) = budget.checked_sub(bytes) else {
            continue;
        };
        budget = remaining;
        let merge = Merge {
            views: &views,
            cut: &cut,
            view: &view,
            schema,
            retention,
            log,
        };
        let merged =
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), merge.run(run)).await;
        match merged {
            Ok(Ok(merged)) => {
                prepared.insert(view, Arc::new(merged));
            }
            Ok(Err(error)) => {
                tracing::warn!(view, %error, "append-view log compaction skipped the view");
            }
            Err(_) => {
                tracing::warn!(view, "append-view log compaction passed its deadline");
                break;
            }
        }
    }
    store.write().install_compactions(prepared);
}

/// The run one merge rewrites in `segments` (a log, ordered by partition and sequence, before
/// `next_sequence`): in its first partition that has one, the oldest run of two or more
/// consecutive segments of one closed block that fits in `TARGET_SEGMENT_BYTES`, from the
/// largest block that yields a run there.
pub(super) fn plan(segments: &[ViewLogSegment], next_sequence: u64) -> Option<Range<usize>> {
    let mut offset = 0;
    for partition in segments.chunk_by(|left, right| left.partition == right.partition) {
        if let Some(run) = plan_partition(partition, next_sequence) {
            return Some(run.start + offset..run.end + offset);
        }
        offset += partition.len();
    }
    None
}

fn plan_partition(segments: &[ViewLogSegment], next_sequence: u64) -> Option<Range<usize>> {
    let mut best: Option<Range<usize>> = None;
    let mut span = FANOUT;
    while span <= next_sequence {
        let closed = next_sequence - next_sequence % span;
        if let Some(run) = oldest_run(segments, span, closed) {
            let order = |run: &Range<usize>| (run.start, Reverse(run.end));
            if best.as_ref().is_none_or(|best| order(&run) < order(best)) {
                best = Some(run);
            }
        }
        let Some(wider) = span.checked_mul(FANOUT) else {
            break;
        };
        span = wider;
    }
    best
}

/// The first run of two or more consecutive segments below `closed` that share a `span` block
/// and fit in `TARGET_SEGMENT_BYTES`.
fn oldest_run(segments: &[ViewLogSegment], span: u64, closed: u64) -> Option<Range<usize>> {
    let mut start = 0;
    while let Some(first) = segments
        .get(start)
        .filter(|segment| segment.sequence < closed)
    {
        let block = first.sequence / span;
        let mut bytes = first.range.length;
        let mut end = start + 1;
        while let Some(next) = segments
            .get(end)
            .filter(|segment| segment.sequence / span == block)
        {
            let Some(total) = bytes
                .checked_add(next.range.length)
                .filter(|total| *total <= TARGET_SEGMENT_BYTES)
            else {
                break;
            };
            bytes = total;
            end += 1;
        }
        if end - start >= 2 {
            return Some(start..end);
        }
        start = end;
    }
    None
}

/// One view's merge, read from the pinned cut.
struct Merge<'a> {
    views: &'a CommittedViews,
    cut: &'a PinnedCut,
    view: &'a str,
    schema: SchemaRef,
    retention: ViewRetention,
    log: &'a ViewLog,
}

impl Merge<'_> {
    async fn run(self, run: Range<usize>) -> Result<PreparedCompaction, DbError> {
        let inputs = &self.log.segments[run.clone()];
        let wanted: Vec<SegmentMeta> = inputs.iter().map(SegmentMeta::from).collect();
        let batches = self
            .cut
            .local_segments(self.views, self.view, &self.schema, &wanted)
            .await
            .map_err(|error| match error {
                LogReadError::Failed(error) => error,
                LogReadError::Retired => {
                    DbError::Checkpoint("the cut compaction read was retired".into())
                }
            })?;
        let first = &inputs[0];
        // A row cap keeps only the newest rows of a partition, so the run keeps what the
        // segments after it in its partition leave of the cap.
        let kept_by = match self.retention {
            ViewRetention::Rows(limit) => {
                let newer = self.log.segments[run.end..]
                    .iter()
                    .take_while(|segment| segment.partition == first.partition)
                    .map(|segment| segment.rows)
                    .fold(0u64, u64::saturating_add);
                ViewRetention::Rows(limit.saturating_sub(newer))
            }
            ref retention => retention.clone(),
        };
        let rewrite = Rewrite {
            retention: self.retention,
            kept_by,
            frontier: self.log.frontier,
            schema: self.schema,
            position: (first.partition, first.sequence),
        };
        let output = tokio::task::spawn_blocking(move || rewrite.apply(batches))
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!("view-log merge task failed: {error}"))
            })??;
        Ok(PreparedCompaction {
            base: self.cut.checkpoint_id,
            inputs: wanted.iter().map(SegmentMeta::position).collect(),
            output,
        })
    }
}

/// How a run's rows become one segment.
struct Rewrite {
    retention: ViewRetention,
    kept_by: ViewRetention,
    frontier: Option<i64>,
    schema: SchemaRef,
    position: (u16, u64),
}

impl Rewrite {
    /// The run's rows that retention keeps at the cut's frontier, as one encoded segment.
    fn apply(self, batches: Vec<RecordBatch>) -> Result<Option<CompactedSegment>, DbError> {
        let kept = self.kept_by.apply(batches, self.frontier)?;
        let batch = arrow::compute::concat_batches(&self.schema, &kept)
            .map_err(|error| DbError::Storage(format!("view-log merge concat: {error}")))?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        let event_time = self.retention.bounds(&batch)?;
        let mut payload = batches_to_ipc_bounded(&self.schema, [&batch], u64::MAX)?;
        payload.shrink_to_fit();
        Ok(Some(CompactedSegment {
            meta: SegmentMeta {
                partition: self.position.0,
                sequence: self.position.1,
                rows: u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                event_time,
            },
            payload: Bytes::from(payload),
        }))
    }
}

impl MvStore {
    /// Every append view's name, schema and retention.
    fn append_logs_to_compact(&self) -> Vec<(String, SchemaRef, ViewRetention)> {
        let mut logs: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(name, entry)| match &entry.mode {
                MvStorageMode::AppendLog { retention } => {
                    Some((name.clone(), Arc::clone(&entry.schema), retention.clone()))
                }
                MvStorageMode::Changelog { .. } => None,
            })
            .collect();
        logs.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        logs
    }

    /// Replace every append view's prepared merge with this pass's.
    fn install_compactions(&mut self, mut prepared: HashMap<String, Arc<PreparedCompaction>>) {
        for (name, entry) in &mut self.entries {
            if let Some(log) = entry.log.as_mut() {
                log.set_prepared(prepared.remove(name));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use laminar_core::checkpoint::{ByteRange, EventTimeBounds, StateChunkId};

    use super::*;
    use crate::mv_store::checkpoint::ipc_to_schema_and_batches;

    const KIB: u64 = 1 << 10;
    const MIB: u64 = 1 << 20;

    fn segment(partition: u16, sequence: u64, bytes: u64) -> ViewLogSegment {
        ViewLogSegment {
            partition,
            sequence,
            chunk: StateChunkId {
                participant_id: 1,
                checkpoint_id: sequence + 1,
            },
            range: ByteRange {
                offset: 0,
                length: bytes,
            },
            sha256: String::new(),
            rows: 1,
            event_time: None,
        }
    }

    fn log(sizes: &[u64]) -> Vec<ViewLogSegment> {
        sizes
            .iter()
            .zip(0..)
            .map(|(bytes, sequence)| segment(0, sequence, *bytes))
            .collect()
    }

    #[test]
    fn a_block_merges_once_the_log_has_moved_past_it() {
        let segments = log(&[KIB; 8]);
        assert_eq!(plan(&segments, 7), None);
        assert_eq!(plan(&segments, 8), Some(0..8));
        // Retention dropped the oldest segments; the rest of the block still merges.
        assert_eq!(plan(&segments[3..], 8), Some(0..5));
        // A lone segment in a closed block has nothing to merge with.
        assert_eq!(plan(&segments[7..], 9), None);
    }

    #[test]
    fn a_merge_packs_consecutive_segments_up_to_the_target() {
        assert_eq!(plan(&log(&[6 * MIB; 8]), 8), Some(0..2));
        assert_eq!(plan(&log(&[10 * MIB; 8]), 8), None);
        let mixed = log(&[20 * MIB, 10 * MIB, KIB, KIB, 10 * MIB, 5 * MIB, KIB, KIB]);
        assert_eq!(plan(&mixed, 8), Some(1..4));
    }

    #[test]
    fn the_largest_closed_block_merges_first_and_partitions_plan_in_order() {
        // Level-1 blocks of 8 merged into one segment each; level 2 (64) has closed.
        let mut segments: Vec<ViewLogSegment> =
            (0..8).map(|block| segment(0, block * 8, 8 * KIB)).collect();
        assert_eq!(plan(&segments, 63), None);
        assert_eq!(plan(&segments, 64), Some(0..8));

        segments = vec![segment(0, 0, KIB), segment(1, 0, KIB), segment(1, 1, KIB)];
        assert_eq!(plan(&segments, 8), Some(1..3));
    }

    /// Applies one merge per checkpoint, the preparer's pace, to a log of `sizes` that keeps
    /// the last `window` checkpoints, and returns the most segments the log held, the bytes
    /// merges rewrote and the bytes appended.
    fn simulate(sizes: impl Iterator<Item = u64>, window: u64) -> (usize, u64, u64) {
        let (mut segments, mut next, mut most, mut rewritten, mut written) =
            (Vec::new(), 0, 0, 0, 0);
        for bytes in sizes {
            if let Some(run) = plan(&segments, next) {
                let merged: u64 = segments[run.clone()]
                    .iter()
                    .map(|segment: &ViewLogSegment| segment.range.length)
                    .sum();
                rewritten += merged;
                let first = segments[run.start].sequence;
                segments.splice(run, [segment(0, first, merged)]);
            }
            segments.push(segment(0, next, bytes));
            next += 1;
            written += bytes;
            // A segment expires once the segment after it starts inside the window.
            let horizon = next.saturating_sub(window);
            let expired = segments
                .windows(2)
                .take_while(|pair| pair[1].sequence <= horizon)
                .count();
            segments.drain(..expired);
            most = most.max(segments.len());
        }
        (most, rewritten, written)
    }

    #[test]
    fn steady_appends_keep_few_segments_and_rewrite_each_row_a_few_times() {
        let (most, rewritten, written) = simulate(std::iter::repeat_n(100 * KIB, 20_000), u64::MAX);
        // 1.9 GiB at 100 KiB per checkpoint: about 160 filled segments plus open blocks.
        assert!(most <= 200, "{most} segments");
        assert!(
            rewritten <= 3 * written,
            "{rewritten} of {written} bytes rewritten"
        );
    }

    #[test]
    fn bursty_appends_keep_the_segment_count_proportional_to_bytes() {
        let sizes = (0..20_000u64).map(|checkpoint| {
            if checkpoint % 2 == 0 {
                64 * KIB
            } else {
                3 * MIB
            }
        });
        let (most, _, written) = simulate(sizes, u64::MAX);
        assert!(
            (most as u64) <= 2 * written / TARGET_SEGMENT_BYTES + 64,
            "{most} segments for {written} bytes"
        );
    }

    #[test]
    fn a_bounded_window_stays_small_while_its_oldest_segments_expire() {
        // An hour at 1 s keeps 3,600 segments without compaction.
        let (most, _, _) = simulate(std::iter::repeat_n(64 * KIB, 20_000), 3_600);
        assert!(most <= 48, "{most} segments");
    }

    fn timed_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("id", DataType::Int64, false),
        ]))
    }

    fn timed(times: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            timed_schema(),
            vec![
                Arc::new(TimestampMillisecondArray::from(times.to_vec())),
                Arc::new(Int64Array::from(times.to_vec())),
            ],
        )
        .unwrap()
    }

    fn rewrite(retention: ViewRetention, kept_by: ViewRetention, frontier: i64) -> Rewrite {
        Rewrite {
            retention,
            kept_by,
            frontier: Some(frontier),
            schema: timed_schema(),
            position: (0, 3),
        }
    }

    fn ids(segment: &CompactedSegment) -> Vec<i64> {
        let (_, batches) = ipc_to_schema_and_batches(&segment.payload).unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                ids.values().to_vec()
            })
            .collect()
    }

    #[test]
    fn a_merge_drops_rows_behind_the_horizon_and_recomputes_bounds() {
        let retention = ViewRetention::EventTime {
            column: 0,
            interval_ms: 100,
        };
        let batches = vec![timed(&[10, 160]), timed(&[150, 300])];
        let merged = rewrite(retention.clone(), retention, 250)
            .apply(batches)
            .unwrap()
            .unwrap();
        assert_eq!(ids(&merged), [160, 150, 300]);
        assert_eq!(merged.meta.rows, 3);
        assert_eq!((merged.meta.partition, merged.meta.sequence), (0, 3));
        assert_eq!(
            merged.meta.event_time,
            Some(EventTimeBounds { min: 150, max: 300 })
        );
    }

    #[test]
    fn a_merge_under_a_row_cap_keeps_what_newer_segments_leave_of_it() {
        let batches = vec![timed(&[1, 2]), timed(&[3, 4])];
        let merged = rewrite(ViewRetention::Rows(10), ViewRetention::Rows(3), 0)
            .apply(batches)
            .unwrap()
            .unwrap();
        assert_eq!(ids(&merged), [2, 3, 4]);
        assert_eq!(merged.meta.event_time, None);
    }

    #[test]
    fn an_unbounded_merge_keeps_every_row_and_an_expired_run_writes_nothing() {
        let unbounded = ViewRetention::Unbounded { column: Some(0) };
        let merged = rewrite(unbounded.clone(), unbounded, i64::MAX)
            .apply(vec![timed(&[5]), timed(&[1, 9])])
            .unwrap()
            .unwrap();
        assert_eq!(ids(&merged), [5, 1, 9]);
        assert_eq!(
            merged.meta.event_time,
            Some(EventTimeBounds { min: 1, max: 9 })
        );

        let retention = ViewRetention::EventTime {
            column: 0,
            interval_ms: 10,
        };
        let expired = rewrite(retention.clone(), retention, 1_000)
            .apply(vec![timed(&[5, 9])])
            .unwrap();
        assert!(expired.is_none());
    }
}
