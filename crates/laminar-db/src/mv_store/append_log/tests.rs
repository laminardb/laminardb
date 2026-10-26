use std::sync::Arc;

use arrow::array::{Int64Array, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use super::*;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("id", DataType::Int64, false),
    ]))
}

fn rows(times: &[i64]) -> RecordBatch {
    let ids: Vec<i64> = times.iter().map(|time| time * 10).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampMillisecondArray::from(times.to_vec())),
            Arc::new(Int64Array::from(ids)),
        ],
    )
    .unwrap()
}

const LIMITS: MvLimits = MvLimits {
    rows: 1_000,
    bytes: usize::MAX / 2,
};

fn append(log: &mut AppendLogState, batches: &[RecordBatch]) {
    let update = log.prepare("v", batches, LIMITS).unwrap();
    log.apply(update);
}

fn times(log: &AppendLogState) -> Vec<i64> {
    let batch = log.memory_batch(&schema()).unwrap();
    let column = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    column.values().to_vec()
}

const fn active(watermark: i64) -> InputFrontier {
    InputFrontier {
        watermark: Some(watermark),
        idle: false,
    }
}

fn event_time(interval_ms: i64) -> AppendLogState {
    AppendLogState::new(ViewRetention::EventTime {
        column: 0,
        interval_ms,
    })
}

#[test]
fn event_time_retention_reads_only_rows_at_or_after_the_horizon() {
    let mut log = event_time(100);
    append(&mut log, &[rows(&[10, 20]), rows(&[150, 300])]);
    assert_eq!(times(&log), [10, 20, 150, 300]);

    // Frontier 250 sets the horizon at 150: the earlier rows leave reads at once, and the
    // leading batch that lies wholly behind the horizon leaves memory on the next append.
    log.advance_frontier(active(250));
    assert_eq!(times(&log), [150, 300]);
    append(&mut log, &[rows(&[50]), rows(&[260])]);
    assert_eq!(log.staged.len(), 2);
    assert_eq!(times(&log), [150, 300, 260]);

    // A frontier never moves back, and a stream without a watermark leaves it.
    log.advance_frontier(active(200));
    log.advance_frontier(InputFrontier::default());
    assert_eq!(log.frontier, Some(250));
}

#[test]
fn a_committed_horizon_decides_retention_and_the_capture_reports_the_local_frontier() {
    let mut log = event_time(100);
    log.set_horizon(LogHorizon::Committed(None));
    append(&mut log, &[rows(&[10, 20]), rows(&[150, 300])]);
    // In a cluster this node's own frontier moves no horizon.
    log.advance_frontier(active(250));
    assert_eq!(times(&log), [10, 20, 150, 300]);

    // The committed frontier does: horizon 120 hides the leading batch from reads, ingest
    // drops a batch wholly behind it, and the capture leaves it out.
    log.set_horizon(LogHorizon::Committed(Some(220)));
    assert_eq!(times(&log), [150, 300]);
    append(&mut log, &[rows(&[110]), rows(&[130])]);
    assert_eq!(times(&log), [150, 300, 130]);
    log.advance_frontier(InputFrontier {
        watermark: None,
        idle: true,
    });
    let capture = log.capture(1, true).unwrap();
    assert_eq!(capture.sealed.map(|segment| segment.rows), Some(3));
    assert_eq!((capture.frontier, capture.idle), (Some(250), true));
}

#[test]
fn a_capture_keeps_only_committed_segments_inside_the_horizon() {
    let mut log = event_time(100);
    append(&mut log, &[rows(&[10, 20])]);
    let first = log.capture(1, true).unwrap();
    assert_eq!(
        first.sealed.as_ref().map(|segment| segment.sequence),
        Some(0)
    );
    assert_eq!(
        first.sealed.as_ref().and_then(|segment| segment.event_time),
        Some(EventTimeBounds { min: 10, max: 20 })
    );
    log.commit(1);
    assert_eq!((log.rows, log.bytes), (0, 0));

    append(&mut log, &[rows(&[200])]);
    let second = log.capture(2, true).unwrap();
    assert_eq!(second.retained, [(LOG_PARTITION, 0)]);
    log.commit(2);

    log.advance_frontier(active(250));
    let third = log.capture(3, true).unwrap();
    assert_eq!(third.retained, [(LOG_PARTITION, 1)]);
    assert!(third.sealed.is_none());
    assert_eq!(third.next_sequence, 2);
    assert_eq!(third.frontier, Some(250));
}

#[test]
fn row_retention_counts_committed_segments_with_memory() {
    let mut log = AppendLogState::new(ViewRetention::Rows(5));
    append(&mut log, &[rows(&[1, 2, 3, 4])]);
    log.capture(1, true).unwrap();
    log.commit(1);

    append(&mut log, &[rows(&[5, 6])]);
    let (committed, memory, _) = log.read_parts();
    assert_eq!(committed.len(), 1);
    assert_eq!(memory.len(), 1);
    let kept = log
        .retention
        .apply([rows(&[1, 2, 3, 4]), rows(&[5, 6])].to_vec(), None)
        .unwrap();
    let kept: Vec<i64> = kept
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(kept, [2, 3, 4, 5, 6]);

    // Five staged rows cover the retention, so the capture drops the committed segment.
    append(&mut log, &[rows(&[7, 8, 9])]);
    let capture = log.capture(2, true).unwrap();
    assert!(capture.retained.is_empty());
}

#[test]
fn an_abort_returns_sealed_rows_ahead_of_later_ones() {
    let mut log = AppendLogState::new(ViewRetention::Rows(100));
    append(&mut log, &[rows(&[1, 2])]);
    log.capture(7, true).unwrap();
    append(&mut log, &[rows(&[3])]);
    log.commit(6);
    assert_eq!(times(&log), [1, 2, 3]);
    log.abort(7);
    assert_eq!(times(&log), [1, 2, 3]);
    let capture = log.capture(8, true).unwrap();
    assert_eq!(capture.sealed.map(|segment| segment.rows), Some(3));
}

/// An unbounded log with four committed single-row segments, committed by checkpoint 4.
fn four_committed_segments() -> AppendLogState {
    let mut log = AppendLogState::new(ViewRetention::Unbounded { column: Some(0) });
    for checkpoint in 1..=4 {
        append(&mut log, &[rows(&[i64::try_from(checkpoint).unwrap()])]);
        log.capture(checkpoint, true).unwrap();
        log.commit(checkpoint);
    }
    log
}

fn merged(base: u64, inputs: &[u64], rows: u64) -> Arc<PreparedCompaction> {
    let output = (rows > 0).then(|| CompactedSegment {
        meta: SegmentMeta {
            partition: LOG_PARTITION,
            sequence: inputs[0],
            rows,
            event_time: None,
        },
        payload: bytes::Bytes::from_static(b"merged"),
    });
    Arc::new(PreparedCompaction {
        base,
        inputs: inputs
            .iter()
            .map(|sequence| (LOG_PARTITION, *sequence))
            .collect(),
        output,
    })
}

fn positions(log: &AppendLogState) -> Vec<u64> {
    log.committed
        .iter()
        .map(|segment| segment.sequence)
        .collect()
}

#[test]
fn a_capture_writes_the_prepared_merge_in_place_of_its_inputs() {
    let mut log = four_committed_segments();
    log.set_prepared(Some(merged(4, &[1, 2], 2)));
    append(&mut log, &[rows(&[5])]);
    let capture = log.capture(5, true).unwrap();
    assert_eq!(capture.retained, [(LOG_PARTITION, 0), (LOG_PARTITION, 3)]);
    assert_eq!(
        capture
            .compacted
            .as_ref()
            .map(|merged| merged.meta.sequence),
        Some(1)
    );
    assert_eq!(
        capture.sealed.as_ref().map(|sealed| sealed.sequence),
        Some(4)
    );
    assert_eq!(
        positions(&log),
        [0, 1, 2, 3],
        "nothing changes before commit"
    );

    log.commit(5);
    assert_eq!(positions(&log), [0, 1, 3, 4]);
    assert_eq!(log.committed[1].rows, 2);
    assert!(log.prepared.is_none());
    assert_eq!(log.positions_since, Some(5));
}

#[test]
fn a_merge_is_skipped_when_disallowed_stale_or_its_inputs_are_gone() {
    let mut log = four_committed_segments();
    log.set_prepared(Some(merged(4, &[1, 2], 2)));
    assert!(log.capture(5, false).unwrap().compacted.is_none());
    log.abort(5);

    // Prepared before the log's positions last changed.
    log.positions_since = Some(5);
    assert!(log.capture(6, true).unwrap().compacted.is_none());
    log.abort(6);

    // An input that is no longer retained.
    log.positions_since = Some(4);
    log.set_prepared(Some(merged(4, &[3, 4], 2)));
    assert!(log.capture(7, true).unwrap().compacted.is_none());
    log.abort(7);
}

#[test]
fn an_aborted_merge_is_written_by_the_next_capture_and_an_empty_one_removes_its_inputs() {
    let mut log = four_committed_segments();
    log.set_prepared(Some(merged(4, &[0, 1, 2], 0)));
    let capture = log.capture(5, true).unwrap();
    assert_eq!(capture.retained, [(LOG_PARTITION, 3)]);
    assert!(capture.compacted.is_none());
    log.abort(5);
    assert!(log.prepared.is_some());

    log.capture(6, true).unwrap();
    // A pass replaced the merge while the capture was in flight; the newer one stays.
    let newer = merged(5, &[3], 1);
    log.set_prepared(Some(Arc::clone(&newer)));
    log.commit(6);
    assert_eq!(positions(&log), [3]);
    assert!(log
        .prepared
        .as_ref()
        .is_some_and(|held| Arc::ptr_eq(held, &newer)));
}

fn adopted(sequence: u64, rows: u64, max: i64) -> AdoptedSegment {
    AdoptedSegment {
        segment: ViewLogSegment {
            partition: 3,
            sequence,
            chunk: laminar_core::checkpoint::StateChunkId {
                participant_id: 9,
                checkpoint_id: 4,
            },
            range: laminar_core::checkpoint::ByteRange {
                offset: sequence * 10,
                length: 10,
            },
            sha256: "0".repeat(64),
            rows,
            event_time: Some(EventTimeBounds { min: max - 5, max }),
        },
        object_length: 100,
        object_sha256: "1".repeat(64),
    }
}

#[test]
fn a_capture_lists_adopted_segments_verbatim_before_the_sealed_rows() {
    let mut log = event_time(100);
    append(&mut log, &[rows(&[10])]);
    log.capture(1, true).unwrap();
    log.commit(1);

    // The committed horizon has passed both adopted segments; they are listed anyway.
    log.set_horizon(LogHorizon::Committed(Some(1_000)));
    log.adopt([adopted(7, 2, 20), adopted(8, 3, 30)]);
    append(&mut log, &[rows(&[950])]);
    let capture = log.capture(2, true).unwrap();
    let sequences: Vec<u64> = capture
        .adopted
        .iter()
        .map(|(sequence, _)| *sequence)
        .collect();
    assert_eq!(sequences, [1, 2]);
    assert_eq!(capture.adopted[1].1, adopted(8, 3, 30));
    assert_eq!(
        capture.sealed.as_ref().map(|sealed| sealed.sequence),
        Some(3)
    );
    assert!(capture.retained.is_empty());

    // An abort returns them for the next capture.
    log.abort(2);
    let retry = log.capture(3, true).unwrap();
    assert_eq!(retry.adopted.len(), 2);
    assert_eq!(retry.adopted[0].0, 4);

    // Once committed they are ordinary segments of partition 0, kept by retention from now on.
    log.commit(3);
    assert!(log.adopted.is_empty());
    assert_eq!(positions(&log), [4, 5, 6]);
    let next = log.capture(4, true).unwrap();
    assert!(next.adopted.is_empty());
    assert_eq!(next.retained, [(LOG_PARTITION, 6)]);
}

#[test]
fn a_capture_holding_only_adopted_segments_must_settle_first() {
    let mut log = event_time(100);
    log.adopt([adopted(0, 1, 10)]);
    log.capture(1, true).unwrap();
    assert!(log.capture(2, true).is_err());
}
