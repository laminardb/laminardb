//! Out-of-Order (O3) merge engine for late data.
//!
//! Inspired by [QuestDB](https://questdb.io/)'s O3 commit path, this module provides a buffer
//! that accumulates late-arriving rows (events with timestamps behind
//! the watermark) and merges them back into the pipeline when triggered.
//!
//! Out-of-order merge engine for late-arriving data in streaming windows.
//!
//! # Strategy
//!
//! The pipeline's `filter_late_rows` currently drops rows behind the
//! watermark. With `LateDataStrategy::Merge`, late rows are buffered
//! per source and periodically merged back into the next processing
//! cycle using Arrow's sort + take kernels.
//!
//! # Design
//!
//! - **Buffer**: Per-source `Vec<RecordBatch>` accumulating late rows.
//! - **Trigger**: Configurable — row count threshold or flush interval.
//! - **Merge**: Concatenates buffered batches with the next cycle's
//!   on-time batches, then sorts by the event-time column so downstream
//!   operators see a correctly ordered stream.

use rustc_hash::FxHashMap;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use arrow::compute::{concat_batches, sort_to_indices, take};

/// Strategy for handling late-arriving data.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum LateDataStrategy {
    /// Drop late rows (current default behavior).
    #[default]
    Drop,
    /// Buffer late rows and merge them back into the pipeline.
    Merge,
    /// Redirect late rows to a side output stream (not yet implemented).
    Redirect,
}

/// Configuration for the O3 merge engine.
#[derive(Debug, Clone)]
pub struct O3MergeConfig {
    /// Strategy for handling late data.
    pub strategy: LateDataStrategy,
    /// Maximum number of buffered late rows before forced flush.
    /// Default: 10,000 rows.
    pub max_buffered_rows: usize,
    /// Maximum time to buffer late rows before forced flush.
    /// Default: 5 seconds.
    pub max_buffer_duration: Duration,
}

impl Default for O3MergeConfig {
    fn default() -> Self {
        Self {
            strategy: LateDataStrategy::Drop,
            max_buffered_rows: 10_000,
            max_buffer_duration: Duration::from_secs(5),
        }
    }
}

impl O3MergeConfig {
    /// Create a merge-enabled configuration.
    #[must_use]
    pub fn merge() -> Self {
        Self {
            strategy: LateDataStrategy::Merge,
            ..Self::default()
        }
    }
}

/// Per-source buffer for late-arriving rows.
#[derive(Debug)]
struct SourceLateBuffer {
    /// Accumulated late batches.
    batches: Vec<RecordBatch>,
    /// Total rows currently buffered.
    row_count: usize,
    /// When the buffer was last flushed (or created).
    last_flush: Instant,
    /// Event-time column name for sort-merge.
    time_column: String,
}

impl SourceLateBuffer {
    fn new(time_column: String) -> Self {
        Self {
            batches: Vec::new(),
            row_count: 0,
            last_flush: Instant::now(),
            time_column,
        }
    }

    /// Returns true if the buffer should be flushed based on config thresholds.
    fn should_flush(&self, config: &O3MergeConfig) -> bool {
        self.row_count >= config.max_buffered_rows
            || self.last_flush.elapsed() >= config.max_buffer_duration
    }

    /// Add a batch of late rows to the buffer.
    fn push(&mut self, batch: RecordBatch) {
        self.row_count += batch.num_rows();
        self.batches.push(batch);
    }

    /// Drain all buffered batches and reset the buffer.
    fn drain(&mut self) -> Vec<RecordBatch> {
        self.row_count = 0;
        self.last_flush = Instant::now();
        std::mem::take(&mut self.batches)
    }

    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}

/// The O3 merge engine.
///
/// Accumulates late-arriving rows and merges them back into the
/// pipeline's normal processing flow. Each source has its own buffer
/// keyed by source name.
pub struct O3MergeEngine {
    /// Configuration.
    config: O3MergeConfig,
    /// Per-source late-row buffers.
    buffers: FxHashMap<String, SourceLateBuffer>,
    /// Total late rows received since creation.
    total_late_rows: u64,
    /// Total late rows merged back.
    total_merged_rows: u64,
}
impl O3MergeEngine {
    /// Create a new O3 merge engine with the given configuration.
    #[must_use]
    pub fn new(config: O3MergeConfig) -> Self {
        Self {
            config,
            buffers: FxHashMap::default(),
            total_late_rows: 0,
            total_merged_rows: 0,
        }
    }

    /// Returns the configured late data strategy.
    #[must_use]
    pub fn strategy(&self) -> &LateDataStrategy {
        &self.config.strategy
    }

    /// Buffer a batch of late rows for a given source.
    ///
    /// The `time_column` is the event-time column used for sort-merge.
    pub fn buffer_late(&mut self, source_name: &str, batch: RecordBatch, time_column: &str) {
        #[allow(clippy::cast_possible_truncation)]
        {
            self.total_late_rows += batch.num_rows() as u64;
        }
        let buf = self
            .buffers
            .entry(source_name.to_string())
            .or_insert_with(|| SourceLateBuffer::new(time_column.to_string()));
        buf.push(batch);
    }

    /// Merge buffered late rows into on-time batches for a source.
    ///
    /// If there are buffered late rows that are ready to flush (or if
    /// `force` is true), concatenates them with `on_time_batches` and
    /// sorts the result by the event-time column.
    ///
    /// Returns the (potentially merged and sorted) batches.
    pub fn merge_into(
        &mut self,
        source_name: &str,
        on_time_batches: Vec<RecordBatch>,
        force: bool,
    ) -> Vec<RecordBatch> {
        let Some(buf) = self.buffers.get_mut(source_name) else {
            return on_time_batches;
        };

        if buf.is_empty() {
            return on_time_batches;
        }

        if !force && !buf.should_flush(&self.config) {
            return on_time_batches;
        }

        let late_batches = buf.drain();
        let late_row_count: usize = late_batches.iter().map(RecordBatch::num_rows).sum();
        let time_column = buf.time_column.clone();

        // Merge late + on-time batches.
        let mut all_batches = late_batches;
        all_batches.extend(on_time_batches);

        if all_batches.is_empty() {
            return Vec::new();
        }

        // Concatenate into a single batch.
        let schema = all_batches[0].schema();
        let concatenated = match concat_batches(&schema, &all_batches) {
            Ok(batch) => batch,
            Err(e) => {
                tracing::warn!(
                    source = %source_name,
                    error = %e,
                    "O3 merge concat failed; returning un-concatenated batches"
                );
                return all_batches;
            }
        };

        #[allow(clippy::cast_possible_truncation)]
        {
            self.total_merged_rows += late_row_count as u64;
        }

        // Sort by event-time column.
        match sort_batch_by_column(&concatenated, &time_column) {
            Ok(sorted) => vec![sorted],
            Err(e) => {
                tracing::warn!(
                    source = %source_name,
                    error = %e,
                    "O3 merge sort failed; returning unsorted merged batch"
                );
                vec![concatenated]
            }
        }
    }

    /// Flush all buffers for all sources (e.g., at shutdown).
    pub fn flush_all(&mut self) -> FxHashMap<String, Vec<RecordBatch>> {
        let mut result = FxHashMap::default();
        for (source, buf) in &mut self.buffers {
            if !buf.is_empty() {
                result.insert(source.clone(), buf.drain());
            }
        }
        result
    }

    /// Get the total number of late rows received.
    #[must_use]
    pub fn total_late_rows(&self) -> u64 {
        self.total_late_rows
    }

    /// Get the total number of late rows merged back.
    #[must_use]
    pub fn total_merged_rows(&self) -> u64 {
        self.total_merged_rows
    }

    /// Get the current number of buffered rows across all sources.
    #[must_use]
    pub fn buffered_rows(&self) -> usize {
        self.buffers.values().map(|b| b.row_count).sum()
    }
}

/// Sort a `RecordBatch` by a named column using Arrow's sort + take kernels.
fn sort_batch_by_column(batch: &RecordBatch, column_name: &str) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let col_idx = schema
        .index_of(column_name)
        .map_err(|e| format!("column {column_name:?} not found: {e}"))?;

    let sort_options = arrow::compute::SortOptions {
        descending: false,
        nulls_first: true,
    };

    let indices = sort_to_indices(batch.column(col_idx), Some(sort_options), None)
        .map_err(|e| format!("sort_to_indices failed: {e}"))?;

    let columns: Vec<_> = batch
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None).map_err(|e| format!("take failed: {e}")))
        .collect::<Result<_, _>>()?;

    RecordBatch::try_new(schema, columns).map_err(|e| format!("rebuild batch: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_batch(timestamps: &[i64], values: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_time", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(StringArray::from(
                    values.iter().map(|s| *s).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_drop_strategy_does_not_buffer() {
        let engine = O3MergeEngine::new(O3MergeConfig::default());
        assert_eq!(engine.strategy(), &LateDataStrategy::Drop);
        assert_eq!(engine.buffered_rows(), 0);
    }

    #[test]
    fn test_buffer_and_merge() {
        let mut engine = O3MergeEngine::new(O3MergeConfig::merge());

        // Buffer some late data.
        let late = make_batch(&[100, 200], &["a", "b"]);
        engine.buffer_late("source1", late, "event_time");
        assert_eq!(engine.buffered_rows(), 2);

        // On-time data arrives.
        let on_time = make_batch(&[300, 400], &["c", "d"]);

        // Force merge.
        let merged = engine.merge_into("source1", vec![on_time], true);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].num_rows(), 4);

        // Verify sorted order.
        let ts_col = merged[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let timestamps: Vec<i64> = ts_col.values().iter().copied().collect();
        assert_eq!(timestamps, vec![100, 200, 300, 400]);
    }

    #[test]
    fn test_merge_without_late_data_passes_through() {
        let mut engine = O3MergeEngine::new(O3MergeConfig::merge());
        let on_time = make_batch(&[100, 200], &["a", "b"]);
        let result = engine.merge_into("source1", vec![on_time.clone()], false);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[test]
    fn test_flush_all() {
        let mut engine = O3MergeEngine::new(O3MergeConfig::merge());

        engine.buffer_late("s1", make_batch(&[1], &["x"]), "event_time");
        engine.buffer_late("s2", make_batch(&[2], &["y"]), "event_time");

        let flushed = engine.flush_all();
        assert_eq!(flushed.len(), 2);
        assert!(flushed.contains_key("s1"));
        assert!(flushed.contains_key("s2"));
        assert_eq!(engine.buffered_rows(), 0);
    }

    #[test]
    fn test_threshold_trigger() {
        let config = O3MergeConfig {
            strategy: LateDataStrategy::Merge,
            max_buffered_rows: 3,
            max_buffer_duration: Duration::from_secs(3600), // long timeout
        };
        let mut engine = O3MergeEngine::new(config);

        // Buffer 2 rows — under threshold.
        engine.buffer_late("s1", make_batch(&[1, 2], &["a", "b"]), "event_time");

        let on_time = make_batch(&[10], &["c"]);
        // Should NOT merge (under threshold, not forced).
        let result = engine.merge_into("s1", vec![on_time.clone()], false);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1); // only on-time

        // Buffer 2 more — now at 4, over threshold of 3.
        engine.buffer_late("s1", make_batch(&[3, 4], &["d", "e"]), "event_time");

        let on_time2 = make_batch(&[20], &["f"]);
        // Should merge now.
        let result = engine.merge_into("s1", vec![on_time2], false);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 5); // 4 late + 1 on-time
    }
}
