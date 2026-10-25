//! Declared retention of an append-only view, applied to whole segments when a capture decides
//! what the next checkpoint keeps, and to rows when the log is read.

use arrow::array::{ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use laminar_core::checkpoint::EventTimeBounds;
use laminar_core::time::{filter_batch_by_timestamp, ThresholdOp};

use super::SegmentMeta;
use crate::error::DbError;

/// How long an append-only view keeps its rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ViewRetention {
    /// Rows whose event time is at or after the view's frontier minus `interval_ms`. Exact.
    EventTime { column: usize, interval_ms: i64 },
    /// The latest rows of each log partition. Not exact: rows from several splits can share a
    /// partition, and a source orders rows only within one split.
    Rows(u64),
}

impl ViewRetention {
    /// The earliest event time a read keeps, once the view has a frontier.
    pub(super) fn horizon(&self, frontier: Option<i64>) -> Option<i64> {
        match self {
            Self::EventTime { interval_ms, .. } => {
                frontier.map(|frontier| frontier.saturating_sub(*interval_ms))
            }
            Self::Rows(_) => None,
        }
    }

    /// Whether a run of rows with these bounds has wholly left the retention horizon.
    pub(in crate::mv_store) fn expired(
        &self,
        bounds: Option<EventTimeBounds>,
        frontier: Option<i64>,
    ) -> bool {
        let Some(horizon) = self.horizon(frontier) else {
            return false;
        };
        bounds.is_none_or(|bounds| bounds.max < horizon)
    }

    /// The committed segments still needed alongside `newer_rows` rows held in memory, oldest
    /// first.
    pub(super) fn retained(
        &self,
        committed: &[SegmentMeta],
        frontier: Option<i64>,
        newer_rows: u64,
    ) -> Vec<SegmentMeta> {
        match self {
            Self::EventTime { .. } => committed
                .iter()
                .filter(|segment| !self.expired(segment.event_time, frontier))
                .cloned()
                .collect(),
            Self::Rows(limit) => {
                let mut rows = newer_rows;
                let mut first = committed.len();
                while first > 0 && rows < *limit {
                    first -= 1;
                    rows = rows.saturating_add(committed[first].rows);
                }
                committed[first..].to_vec()
            }
        }
    }

    /// Event-time bounds of `batch` in milliseconds, or `None` without an event-time column or
    /// with only nulls. Reads the column in its own unit rather than casting it.
    pub(super) fn bounds(&self, batch: &RecordBatch) -> Result<Option<EventTimeBounds>, DbError> {
        let Self::EventTime { column, .. } = self else {
            return Ok(None);
        };
        let column = batch.column(*column);
        let DataType::Timestamp(unit, _) = column.data_type() else {
            return Err(DbError::MaterializedView(format!(
                "event-time column must be a timestamp, found {}",
                column.data_type()
            )));
        };
        let (min, max) = match unit {
            TimeUnit::Second => min_max::<TimestampSecondType>(column),
            TimeUnit::Millisecond => min_max::<TimestampMillisecondType>(column),
            TimeUnit::Microsecond => min_max::<TimestampMicrosecondType>(column),
            TimeUnit::Nanosecond => min_max::<TimestampNanosecondType>(column),
        };
        Ok(min.zip(max).map(|(min, max)| EventTimeBounds {
            min: millis(*unit, min, false),
            max: millis(*unit, max, true),
        }))
    }

    /// Keep the rows of `batches`, in log order, that this retention keeps at `frontier`.
    pub(in crate::mv_store) fn apply(
        &self,
        batches: Vec<RecordBatch>,
        frontier: Option<i64>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        match self {
            Self::EventTime { column, .. } => {
                let Some(horizon) = self.horizon(frontier) else {
                    return Ok(batches);
                };
                let mut kept = Vec::with_capacity(batches.len());
                for batch in batches {
                    let name = batch.schema().field(*column).name().clone();
                    if let Some(batch) =
                        filter_batch_by_timestamp(&batch, &name, horizon, ThresholdOp::GreaterEq)
                            .map_err(|error| DbError::MaterializedView(error.to_string()))?
                    {
                        kept.push(batch);
                    }
                }
                Ok(kept)
            }
            Self::Rows(limit) => {
                let mut remaining = usize::try_from(*limit).unwrap_or(usize::MAX);
                let mut kept = Vec::with_capacity(batches.len());
                for batch in batches.into_iter().rev() {
                    if remaining == 0 {
                        break;
                    }
                    let take = batch.num_rows().min(remaining);
                    remaining -= take;
                    kept.push(if take == batch.num_rows() {
                        batch
                    } else {
                        batch.slice(batch.num_rows() - take, take)
                    });
                }
                kept.reverse();
                Ok(kept)
            }
        }
    }
}

fn min_max<T: ArrowPrimitiveType<Native = i64>>(column: &ArrayRef) -> (Option<i64>, Option<i64>) {
    let values = column.as_primitive::<T>();
    (arrow::compute::min(values), arrow::compute::max(values))
}

/// `value` in milliseconds, rounded outward so the bounds always contain the rows.
fn millis(unit: TimeUnit, value: i64, upper: bool) -> i64 {
    let per_milli = match unit {
        TimeUnit::Second => return value.saturating_mul(1000),
        TimeUnit::Millisecond => return value,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1_000_000,
    };
    value.div_euclid(per_milli) + i64::from(upper && value.rem_euclid(per_milli) != 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segment(sequence: u64, rows: u64, max: Option<i64>) -> SegmentMeta {
        SegmentMeta {
            partition: 0,
            sequence,
            rows,
            event_time: max.map(|max| EventTimeBounds { min: max - 5, max }),
        }
    }

    #[test]
    fn event_time_retention_drops_segments_wholly_below_the_horizon() {
        let retention = ViewRetention::EventTime {
            column: 0,
            interval_ms: 100,
        };
        let committed = [
            segment(0, 3, Some(50)),
            segment(1, 3, Some(150)),
            segment(2, 3, None),
            segment(3, 3, Some(400)),
        ];
        assert_eq!(retention.retained(&committed, None, 0).len(), 4);
        // Horizon 150: the segments ending at 50 and with no event time expire.
        assert_eq!(
            retention.retained(&committed, Some(250), 0),
            [committed[1].clone(), committed[3].clone()]
        );
        assert!(retention.retained(&committed, Some(600), 0).is_empty());
    }

    #[test]
    fn row_retention_keeps_the_segments_covering_the_newest_rows() {
        let retention = ViewRetention::Rows(5);
        let committed = [
            segment(0, 4, None),
            segment(1, 4, None),
            segment(2, 4, None),
        ];
        assert_eq!(retention.retained(&committed, None, 0), committed[1..]);
        assert_eq!(retention.retained(&committed, None, 2), committed[2..]);
        assert!(retention.retained(&committed, None, 5).is_empty());
    }
}
