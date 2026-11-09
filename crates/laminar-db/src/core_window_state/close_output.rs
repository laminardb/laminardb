//! The rows one window close retires, built into one output batch.
//!
//! A close visits each active vnode and, within it, each window it retires. Building one batch
//! for the whole close, not one per (vnode, window), pays the column, key and batch construction
//! once per close instead of once per vnode.

use std::sync::Arc;

use arrow::array::{ArrayRef, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::row::{Row, Rows};

use super::{CoreWindowState, GroupOutputSource};
use crate::aggregate_state::{ConcreteAggregateState, OutputColumn};
use crate::error::DbError;

/// Closed (window, key) groups in the order the close visits them.
pub(super) struct ClosedRows {
    /// `None` when the window has no key columns.
    keys: Option<Rows>,
    starts: Vec<i64>,
    ends: Vec<i64>,
    columns: Vec<OutputColumn>,
}

impl ClosedRows {
    pub(super) fn push(
        &mut self,
        key: Row<'_>,
        start: i64,
        end: i64,
        accumulators: &[ConcreteAggregateState],
    ) -> Result<(), DbError> {
        if let Some(keys) = &mut self.keys {
            keys.push(key);
        }
        self.starts.push(start);
        self.ends.push(end);
        for (column, accumulator) in self.columns.iter_mut().zip(accumulators) {
            column.push(accumulator.evaluate()?)?;
        }
        Ok(())
    }
}

impl CoreWindowState {
    /// An empty collection for a close at `watermark_ms`, sized for the fixed windows it retires.
    pub(super) fn closed_rows(&self, watermark_ms: i64) -> ClosedRows {
        let capacity = self.fixed_size_ms().map_or(0, |size_ms| {
            self.window_group_counts
                .iter()
                .filter(|(&start, _)| {
                    start
                        .saturating_add(size_ms)
                        .saturating_add(self.allowed_lateness_ms)
                        <= watermark_ms
                })
                .map(|(_, &groups)| groups)
                .sum()
        });
        ClosedRows {
            keys: (self.num_group_cols > 0).then(|| self.row_converter.empty_rows(capacity, 0)),
            starts: Vec::with_capacity(capacity),
            ends: Vec::with_capacity(capacity),
            columns: self
                .agg_specs
                .iter()
                .map(|_| OutputColumn::with_capacity(capacity))
                .collect(),
        }
    }

    /// One batch of `closed` in the output schema, or none when the close retired no group.
    pub(super) fn closed_batch(&self, closed: ClosedRows) -> Result<Vec<RecordBatch>, DbError> {
        if closed.starts.is_empty() {
            return Ok(Vec::new());
        }
        let key_arrays = match &closed.keys {
            Some(keys) => self
                .row_converter
                .convert_rows(keys)
                .map_err(|error| DbError::Pipeline(format!("window key arrays: {error}")))?,
            None => Vec::new(),
        };
        let mut arrays = self.output_group_arrays(&key_arrays, &closed.starts, &closed.ends)?;
        for (column, spec) in closed.columns.into_iter().zip(&self.agg_specs) {
            arrays.push(column.finish(spec)?);
        }
        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), arrays)
            .map_err(|error| DbError::Pipeline(format!("window result batch: {error}")))?;
        Ok(vec![batch])
    }

    fn output_group_arrays(
        &self,
        key_arrays: &[ArrayRef],
        starts: &[i64],
        ends: &[i64],
    ) -> Result<Vec<ArrayRef>, DbError> {
        if key_arrays.len() != self.num_group_cols {
            return Err(DbError::Pipeline(format!(
                "window output has {} key columns; expected {}",
                key_arrays.len(),
                self.num_group_cols
            )));
        }
        if self.group_output_sources.len() == self.num_group_cols {
            return Ok(key_arrays.to_vec());
        }

        let mut output = Vec::with_capacity(self.group_output_sources.len());
        for (output_index, source) in self.group_output_sources.iter().enumerate() {
            match source {
                GroupOutputSource::Key(key_index) => output.push(
                    key_arrays
                        .get(*key_index)
                        .cloned()
                        .ok_or_else(|| DbError::Pipeline("window key layout is invalid".into()))?,
                ),
                GroupOutputSource::WindowStart => {
                    output.push(self.window_boundary_array(output_index, starts)?);
                }
                GroupOutputSource::WindowEnd => {
                    output.push(self.window_boundary_array(output_index, ends)?);
                }
            }
        }
        Ok(output)
    }

    fn window_boundary_array(
        &self,
        output_index: usize,
        boundaries_ms: &[i64],
    ) -> Result<ArrayRef, DbError> {
        let DataType::Timestamp(TimeUnit::Microsecond, timezone) =
            self.output_schema.field(output_index).data_type()
        else {
            return Err(DbError::Pipeline(
                "window boundary output is not a microsecond timestamp".into(),
            ));
        };
        let micros = boundaries_ms
            .iter()
            .map(|&value| {
                value.checked_mul(1000).ok_or_else(|| {
                    DbError::PipelineTerminal(format!(
                        "window boundary {value}ms does not fit microsecond precision"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(
            TimestampMicrosecondArray::from(micros).with_timezone_opt(timezone.clone()),
        ))
    }
}
