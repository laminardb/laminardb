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

use arrow::compute::kernels::cmp::gt_eq;
use arrow_array::cast::AsArray;
use arrow_array::types::TimestampMillisecondType;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use datafusion::physical_plan::RecordBatchStream;
use datafusion_common::DataFusionError;
use futures::Stream;

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
        let old = self.watermark_ms.load(Ordering::Acquire);
        if new_ms > old {
            self.watermark_ms.store(new_ms, Ordering::Release);
            self.generation.fetch_add(1, Ordering::Release);
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
    /// Returns `Ok(None)` when all rows are filtered out.
    /// When watermark < 0 (uninitialized), all rows pass through.
    ///
    /// Handles both `Int64` (epoch millis) and `Timestamp(Millisecond, _)` columns.
    ///
    /// # Errors
    ///
    /// Returns an error if the time column is missing or has an unsupported type.
    pub fn filter_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        let wm = self.watermark_ms.load(Ordering::Acquire);
        if wm < 0 {
            return Ok(Some(batch.clone()));
        }

        let schema = batch.schema();
        let col_idx = schema.index_of(&self.time_column).map_err(|_| {
            DataFusionError::Plan(format!(
                "watermark filter: time column '{}' not found in schema",
                self.time_column
            ))
        })?;

        let col = batch.column(col_idx);
        let mask = match col.data_type() {
            DataType::Int64 => {
                let ts_array = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Internal("expected Int64Array".to_string())
                })?;
                let threshold = Int64Array::new_scalar(wm);
                gt_eq(ts_array, &threshold)?
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let ts_array = col.as_primitive::<TimestampMillisecondType>();
                let threshold =
                    arrow_array::TimestampMillisecondArray::new_scalar(wm);
                gt_eq(ts_array, &threshold)?
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "watermark filter: unsupported time column type {other:?}, \
                     expected Int64 or Timestamp(Millisecond)"
                )));
            }
        };

        let filtered = arrow::compute::filter_record_batch(batch, &mask)?;
        if filtered.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(filtered))
        }
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
    use arrow_array::TimestampMillisecondArray;
    use arrow_schema::{Field, Schema};

    fn make_int64_batch(timestamps: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let values: Vec<i64> = (0..timestamps.len() as i64).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    fn make_timestamp_batch(timestamps: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Int64, false),
        ]));
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
        let batch = make_int64_batch(vec![100, 200, 300, 400]);
        let result = filter.filter_batch(&batch).unwrap().unwrap();
        assert_eq!(result.num_rows(), 2);
        let ts = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 300);
        assert_eq!(ts.value(1), 400);
    }

    #[test]
    fn test_filter_passes_on_time_data() {
        let filter = make_filter(50);
        let batch = make_int64_batch(vec![100, 200, 300, 400]);
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
        // Same value — no change
        filter.advance_watermark(200);
        assert_eq!(filter.generation(), 0);
        // Lower value — no change
        filter.advance_watermark(100);
        assert_eq!(filter.generation(), 0);
        assert_eq!(filter.watermark_ms(), 200);
    }

    #[test]
    fn test_passes_all_when_uninitialized() {
        let filter = make_filter(-1);
        let batch = make_int64_batch(vec![100, 200, 300, 400]);
        let result = filter.filter_batch(&batch).unwrap().unwrap();
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn test_empty_batch_returns_none() {
        let filter = make_filter(500);
        let batch = make_int64_batch(vec![100, 200, 300, 400]);
        let result = filter.filter_batch(&batch).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_arrow_timestamp_type() {
        let filter = make_filter(250);
        let batch = make_timestamp_batch(vec![100, 200, 300, 400]);
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
}
