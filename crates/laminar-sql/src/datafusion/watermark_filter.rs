//! Dynamic watermark filter for scan-level late-data pruning
//!
//! Pushes a `ts >= watermark` predicate down to `StreamingScanExec` so
//! late rows are dropped before expression evaluation. The shared
//! `Arc<AtomicI64>` watermark is the same one Ring 0 already updates.

use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::physical_plan::RecordBatchStream;
use datafusion_common::DataFusionError;
use futures::Stream;
use laminar_core::time::{filter_batch_by_timestamp, ThresholdOp};

/// Dynamic filter that drops rows older than the current watermark.
///
/// Holds a shared watermark atomic (same as [`super::watermark_udf::WatermarkUdf`])
/// and a monotonic generation counter that increments on each watermark
/// advance. The generation lets downstream consumers detect stale state
/// without comparing full watermark values.
pub struct WatermarkDynamicFilter {
    watermark_ms: Arc<AtomicI64>,
    generation: Arc<AtomicU64>,
    time_column: String,
}

impl Debug for WatermarkDynamicFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatermarkDynamicFilter")
            .field("watermark_ms", &self.watermark_ms.load(Ordering::Acquire))
            .field("generation", &self.generation.load(Ordering::Acquire))
            .field("time_column", &self.time_column)
            .finish()
    }
}

impl WatermarkDynamicFilter {
    /// Creates a new watermark filter.
    ///
    /// # Arguments
    ///
    /// * `watermark_ms` - Shared atomic holding the current watermark
    ///   in epoch milliseconds. Values < 0 mean "uninitialized".
    /// * `generation` - Monotonic counter incremented on each advance.
    /// * `time_column` - Name of the event-time column in record batches.
    pub fn new(
        watermark_ms: Arc<AtomicI64>,
        generation: Arc<AtomicU64>,
        time_column: String,
    ) -> Self {
        Self {
            watermark_ms,
            generation,
            time_column,
        }
    }

    /// Advances the watermark if `new_ms` exceeds the current value.
    ///
    /// On a successful advance the generation counter is incremented.
    /// No-op when `new_ms <= current`.
    pub fn advance_watermark(&self, new_ms: i64) {
        // Atomic compare-and-swap loop to avoid TOCTOU race where two
        // concurrent callers both see old < new_ms and clobber each other.
        loop {
            let old = self.watermark_ms.load(Ordering::Acquire);
            if new_ms <= old {
                break;
            }
            if self
                .watermark_ms
                .compare_exchange(old, new_ms, Ordering::Release, Ordering::Acquire)
                .is_ok()
            {
                self.generation.fetch_add(1, Ordering::Release);
                break;
            }
        }
    }

    /// Returns the current generation (monotonically increasing).
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Returns the current watermark in epoch milliseconds.
    #[must_use]
    pub fn watermark_ms(&self) -> i64 {
        self.watermark_ms.load(Ordering::Acquire)
    }

    /// Filters a record batch, keeping only rows where `time_column >= watermark`.
    ///
    /// Returns `Ok(None)` when all rows are filtered out. When the
    /// watermark is still uninitialized (< 0) the batch passes through
    /// untouched. Delegates the row-level comparison to
    /// [`filter_batch_by_timestamp`].
    ///
    /// # Errors
    ///
    /// Returns an error if `time_column` is missing from the schema.
    pub fn filter_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        let wm = self.watermark_ms.load(Ordering::Acquire);
        if wm < 0 {
            return Ok(Some(batch.clone()));
        }

        filter_batch_by_timestamp(batch, &self.time_column, wm, ThresholdOp::GreaterEq)
            .map_err(|e| DataFusionError::Plan(format!("watermark filter: {e}")))
    }
}

/// Stream wrapper that applies watermark filtering to each batch.
///
/// Wraps a `SendableRecordBatchStream` and drops rows older than the
/// current watermark before passing them downstream. Follows the same
/// pattern as `ProjectingStream` in `channel_source.rs`.
pub(crate) struct WatermarkFilterStream {
    inner: Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>,
    filter: Arc<WatermarkDynamicFilter>,
    schema: SchemaRef,
}

impl WatermarkFilterStream {
    /// Creates a new watermark-filtered stream.
    pub fn new(
        inner: datafusion::execution::SendableRecordBatchStream,
        filter: Arc<WatermarkDynamicFilter>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            inner,
            filter,
            schema,
        }
    }
}

impl Debug for WatermarkFilterStream {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatermarkFilterStream")
            .field("filter", &self.filter)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl Stream for WatermarkFilterStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => match self.filter.filter_batch(&batch) {
                    Ok(Some(filtered)) => return Poll::Ready(Some(Ok(filtered))),
                    Ok(None) => {
                        // All rows filtered out — loop to try the next batch
                    }
                    Err(e) => return Poll::Ready(Some(Err(e))),
                },
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RecordBatchStream for WatermarkFilterStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, TimestampMillisecondArray, TimestampNanosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    fn make_millis_batch(timestamps: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Int64, false),
        ]));
        #[allow(clippy::cast_possible_wrap)]
        let values: Vec<i64> = (0..timestamps.len() as i64).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(timestamps)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    fn make_nanos_batch(timestamps: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("value", DataType::Int64, false),
        ]));
        #[allow(clippy::cast_possible_wrap)]
        let values: Vec<i64> = (0..timestamps.len() as i64).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(timestamps)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    fn make_filter(wm: i64) -> WatermarkDynamicFilter {
        WatermarkDynamicFilter::new(
            Arc::new(AtomicI64::new(wm)),
            Arc::new(AtomicU64::new(0)),
            "ts".to_string(),
        )
    }

    #[test]
    fn test_filter_skips_late_data() {
        let filter = make_filter(250);
        let batch = make_millis_batch(vec![100, 200, 300, 400]);
        let result = filter.filter_batch(&batch).unwrap().unwrap();
        assert_eq!(result.num_rows(), 2);
        let ts = result
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 300);
        assert_eq!(ts.value(1), 400);
    }

    #[test]
    fn test_filter_passes_on_time_data() {
        let filter = make_filter(50);
        let batch = make_millis_batch(vec![100, 200, 300, 400]);
        let result = filter.filter_batch(&batch).unwrap().unwrap();
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn test_generation_increments_on_advance() {
        let filter = make_filter(100);
        assert_eq!(filter.generation(), 0);
        filter.advance_watermark(200);
        assert_eq!(filter.generation(), 1);
        assert_eq!(filter.watermark_ms(), 200);
        filter.advance_watermark(300);
        assert_eq!(filter.generation(), 2);
    }

    #[test]
    fn test_no_advance_no_generation_change() {
        let filter = make_filter(200);
        assert_eq!(filter.generation(), 0);
        filter.advance_watermark(200);
        assert_eq!(filter.generation(), 0);
        filter.advance_watermark(100);
        assert_eq!(filter.generation(), 0);
        assert_eq!(filter.watermark_ms(), 200);
    }

    #[test]
    fn test_passes_all_when_uninitialized() {
        let filter = make_filter(-1);
        let batch = make_millis_batch(vec![100, 200, 300, 400]);
        let result = filter.filter_batch(&batch).unwrap().unwrap();
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn test_empty_batch_returns_none() {
        let filter = make_filter(500);
        let batch = make_millis_batch(vec![100, 200, 300, 400]);
        let result = filter.filter_batch(&batch).unwrap();
        assert!(result.is_none());
    }

    /// Watermark 250 ms → threshold 250_000_000 ns for a Nanosecond column.
    #[test]
    fn test_nanosecond_timestamp_rescaled_to_watermark() {
        let filter = make_filter(250);
        let batch = make_nanos_batch(vec![100_000_000, 200_000_000, 300_000_000, 400_000_000]);
        let result = filter.filter_batch(&batch).unwrap().unwrap();
        assert_eq!(result.num_rows(), 2);
        let ts = result
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 300_000_000);
        assert_eq!(ts.value(1), 400_000_000);
    }
}
