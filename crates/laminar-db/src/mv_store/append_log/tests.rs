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
    log.advance_frontier(250);
    assert_eq!(times(&log), [150, 300]);
    append(&mut log, &[rows(&[50]), rows(&[260])]);
    assert_eq!(log.staged.len(), 2);
    assert_eq!(times(&log), [150, 300, 260]);

    // A frontier never moves back, and the uninitialized watermark is ignored.
    log.advance_frontier(200);
    log.advance_frontier(i64::MIN);
    assert_eq!(log.frontier, Some(250));
}

#[test]
fn a_capture_keeps_only_committed_segments_inside_the_horizon() {
    let mut log = event_time(100);
    append(&mut log, &[rows(&[10, 20])]);
    let first = log.capture(1).unwrap();
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
    let second = log.capture(2).unwrap();
    assert_eq!(second.retained, [(LOG_PARTITION, 0)]);
    log.commit(2);

    log.advance_frontier(250);
    let third = log.capture(3).unwrap();
    assert_eq!(third.retained, [(LOG_PARTITION, 1)]);
    assert!(third.sealed.is_none());
    assert_eq!(third.next_sequence, 2);
    assert_eq!(third.frontier, Some(250));
}

#[test]
fn row_retention_counts_committed_segments_with_memory() {
    let mut log = AppendLogState::new(ViewRetention::Rows(5));
    append(&mut log, &[rows(&[1, 2, 3, 4])]);
    log.capture(1).unwrap();
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
    let capture = log.capture(2).unwrap();
    assert!(capture.retained.is_empty());
}

#[test]
fn an_abort_returns_sealed_rows_ahead_of_later_ones() {
    let mut log = AppendLogState::new(ViewRetention::Rows(100));
    append(&mut log, &[rows(&[1, 2])]);
    log.capture(7).unwrap();
    append(&mut log, &[rows(&[3])]);
    log.commit(6);
    assert_eq!(times(&log), [1, 2, 3]);
    log.abort(7);
    assert_eq!(times(&log), [1, 2, 3]);
    let capture = log.capture(8).unwrap();
    assert_eq!(capture.sealed.map(|segment| segment.rows), Some(3));
}
