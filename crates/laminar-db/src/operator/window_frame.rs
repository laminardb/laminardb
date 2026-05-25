//! Sliding-window covariance/correlation operator.
//!
//! Streams the bivariate statistics over a `ROWS N PRECEDING` frame — `CORR`,
//! `COVAR_SAMP`, `COVAR_POP` — that the batch engine can't slide: `DataFusion`'s
//! accumulators for them have no `retract_batch`, so they are rejected as
//! sliding-window accumulators. All three derive from the *same* window moments;
//! only the final formula differs. The operator recomputes them per window over a
//! bounded retained buffer — two-pass and mean-centered, which stays accurate for
//! large-magnitude series where the textbook `nΣxy − ΣxΣy` form loses precision to
//! cancellation — appends the result column, and re-projects. (`AVG`/`SUM`/… over
//! frames are a separate concern — `DataFusion` slides those itself.)
//!
//! Supports one bivariate call over a single ordered series (no `PARTITION BY`),
//! preceding bounds only (enforced by `plan_frame_query`). Rows are assumed to
//! arrive in `ORDER BY` order (true for processing-time buckets), so the buffer
//! is already ordered and the new arrivals are its tail.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Float64Builder, RecordBatch};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;

use laminar_core::serialization::{deserialize_batch_stream, serialize_batch_stream};

use crate::error::DbError;
use crate::operator::ProjectingJoinState;
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};
use crate::sql_analysis::FRAME_TMP_TABLE;

/// A bivariate moment-carrying statistic over a sliding frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MomentFn {
    /// Pearson correlation.
    Corr,
    /// Sample covariance.
    CovarSamp,
    /// Population covariance.
    CovarPop,
}

impl MomentFn {
    /// Finalize from a window's centered moments — `cxx = Σ(x−x̄)²`, `cyy = Σ(y−ȳ)²`,
    /// `cxy = Σ(x−x̄)(y−ȳ)` over `n` complete pairs. `None` when the frame has too
    /// few pairs (or zero variance, for `CORR`).
    fn finalize(self, n: f64, cxx: f64, cyy: f64, cxy: f64) -> Option<f64> {
        match self {
            MomentFn::Corr => {
                (n >= 2.0 && cxx > 0.0 && cyy > 0.0).then(|| cxy / (cxx * cyy).sqrt())
            }
            MomentFn::CovarSamp => (n >= 2.0).then(|| cxy / (n - 1.0)),
            MomentFn::CovarPop => (n >= 1.0).then(|| cxy / n),
        }
    }
}

/// What the operator computes: the statistic, its two argument columns, the
/// appended output column, and the rows of preceding history to retain.
pub(crate) struct MomentFrameConfig {
    pub func: MomentFn,
    pub x_column: String,
    pub y_column: String,
    pub output_column: String,
    /// `max(PRECEDING)` — retained so each new row gets a full frame.
    pub retain: usize,
}

/// Streams a sliding-window bivariate statistic (see module docs).
pub(crate) struct WindowFrameOperator {
    config: MomentFrameConfig,
    /// Newest `config.retain` input rows, carried across cycles.
    history: Option<RecordBatch>,
    /// The user's SELECT with the stat call rewritten to its alias column.
    projection: ProjectingJoinState,
}

impl WindowFrameOperator {
    pub(crate) fn new(
        name: &str,
        config: MomentFrameConfig,
        projection_sql: Arc<str>,
        ctx: SessionContext,
    ) -> Self {
        Self {
            config,
            history: None,
            projection: ProjectingJoinState::new(name, ctx, Some(projection_sql), FRAME_TMP_TABLE),
        }
    }

    /// The newest `n` rows of `batch` (all of it if shorter).
    fn tail(batch: &RecordBatch, n: usize) -> RecordBatch {
        let rows = batch.num_rows();
        if rows <= n {
            batch.clone()
        } else {
            batch.slice(rows - n, n)
        }
    }

    /// The named column coerced to `Float64`. `CORR`/`COVAR` operate on any
    /// numeric input (as they do in `DataFusion`, which we bypass), so an integer
    /// or `Float32` column is cast up; only a genuinely non-numeric column errors.
    fn f64_column(batch: &RecordBatch, name: &str) -> Result<Float64Array, DbError> {
        let column = batch.column_by_name(name).ok_or_else(|| {
            DbError::InvalidOperation(format!("window frame: argument '{name}' not found"))
        })?;
        let casted = cast(column, &DataType::Float64).map_err(|e| {
            DbError::InvalidOperation(format!(
                "window frame: argument '{name}' is not numeric: {e}"
            ))
        })?;
        Ok(casted
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("cast to Float64 yields a Float64Array")
            .clone())
    }

    /// The statistic over the window `lo..=hi` of `x`/`y`. Two passes — means,
    /// then centered moments — so the result stays precise for large magnitudes.
    fn stat_window(&self, x: &Float64Array, y: &Float64Array, lo: usize, hi: usize) -> Option<f64> {
        let (mut n, mut sx, mut sy) = (0.0_f64, 0.0, 0.0);
        for i in lo..=hi {
            if x.is_null(i) || y.is_null(i) {
                continue;
            }
            n += 1.0;
            sx += x.value(i);
            sy += y.value(i);
        }
        if n < 1.0 {
            return None;
        }
        let (mx, my) = (sx / n, sy / n);
        let (mut cxx, mut cyy, mut cxy) = (0.0_f64, 0.0, 0.0);
        for i in lo..=hi {
            if x.is_null(i) || y.is_null(i) {
                continue;
            }
            let (dx, dy) = (x.value(i) - mx, y.value(i) - my);
            cxx += dx * dx;
            cyy += dy * dy;
            cxy += dx * dy;
        }
        self.config.func.finalize(n, cxx, cyy, cxy)
    }

    /// Append the per-new-row statistic column to the `new` batch.
    fn enrich(&self, new: &RecordBatch, buffer: &RecordBatch) -> Result<RecordBatch, DbError> {
        let x = Self::f64_column(buffer, &self.config.x_column)?;
        let y = Self::f64_column(buffer, &self.config.y_column)?;
        let k = new.num_rows();
        let first_new = buffer.num_rows() - k;

        let mut builder = Float64Builder::with_capacity(k);
        for j in 0..k {
            let hi = first_new + j;
            let lo = hi.saturating_sub(self.config.retain);
            match self.stat_window(&x, &y, lo, hi) {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }

        let mut fields: Vec<Field> = new
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new(
            &self.config.output_column,
            DataType::Float64,
            true,
        ));
        let mut columns: Vec<ArrayRef> = new.columns().to_vec();
        columns.push(Arc::new(builder.finish()));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|e| DbError::InvalidOperation(format!("window frame: build output: {e}")))
    }
}

#[async_trait]
impl GraphOperator for WindowFrameOperator {
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let batches = inputs.first().map_or(&[][..], Vec::as_slice);
        let Some(schema) = batches
            .iter()
            .find(|b| b.num_rows() > 0)
            .map(RecordBatch::schema)
        else {
            return Ok(Vec::new()); // nothing new this cycle
        };

        let new = concat_batches(&schema, batches.iter())
            .map_err(|e| DbError::Pipeline(format!("window frame: concat input: {e}")))?;

        let buffer = match &self.history {
            Some(h) => concat_batches(&schema, [h, &new])
                .map_err(|e| DbError::Pipeline(format!("window frame: concat buffer: {e}")))?,
            None => new.clone(),
        };

        let enriched = self.enrich(&new, &buffer)?;
        self.history = Some(Self::tail(&buffer, self.config.retain));
        self.projection.apply(vec![enriched]).await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        let Some(history) = &self.history else {
            return Ok(None);
        };
        let data = serialize_batch_stream(history)
            .map_err(|e| DbError::Pipeline(format!("window frame: checkpoint: {e}")))?;
        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let batch = deserialize_batch_stream(&checkpoint.data)
            .map_err(|e| DbError::Pipeline(format!("window frame: restore: {e}")))?;
        self.history = Some(batch);
        Ok(())
    }

    fn estimated_state_bytes(&self) -> usize {
        self.history
            .as_ref()
            .map_or(0, RecordBatch::get_array_memory_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    fn batch(buckets: &[i64], x: &[f64], y: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("bucket", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
            Field::new("sentiment", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(buckets.to_vec())),
                Arc::new(Float64Array::from(x.to_vec())),
                Arc::new(Float64Array::from(y.to_vec())),
            ],
        )
        .unwrap()
    }

    fn operator_with(func: MomentFn, retain: usize) -> WindowFrameOperator {
        // Residual passes the bucket + the appended statistic column through.
        WindowFrameOperator::new(
            "stat_test",
            MomentFrameConfig {
                func,
                x_column: "price".to_string(),
                y_column: "sentiment".to_string(),
                output_column: "stat".to_string(),
                retain,
            },
            Arc::from("SELECT bucket, stat FROM __frame_tmp"),
            laminar_sql::create_session_context(),
        )
    }

    fn last_stat(batches: &[RecordBatch]) -> Option<f64> {
        let b = batches.last().expect("a batch");
        let c = b
            .column_by_name("stat")
            .expect("stat column")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("f64");
        let i = c.len() - 1;
        (!c.is_null(i)).then(|| c.value(i))
    }

    /// The sliding `CORR` must equal the analytical Pearson coefficient, computed
    /// across the cross-cycle window (rows arrive in two cycles), not per cycle.
    /// x=[1..5], y=[2,4,5,4,5] → r = 6/√60.
    #[tokio::test]
    async fn windowed_corr_matches_analytical_value_across_cycles() {
        let mut op = operator_with(MomentFn::Corr, 30);

        let out1 = op
            .process(
                &[vec![batch(&[1, 2, 3], &[1.0, 2.0, 3.0], &[2.0, 4.0, 5.0])]],
                &[0],
            )
            .await
            .unwrap();
        assert_eq!(out1.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);

        let out2 = op
            .process(&[vec![batch(&[4, 5], &[4.0, 5.0], &[4.0, 5.0])]], &[0])
            .await
            .unwrap();
        assert_eq!(out2.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);

        let expected = 6.0 / 60.0_f64.sqrt();
        let got = last_stat(&out2).expect("correlation present");
        assert!(
            (got - expected).abs() < 1e-9,
            "got {got}, expected {expected}"
        );
    }

    /// The same moments drive the covariance family: for x=[1..5], y=[2,4,5,4,5]
    /// the cross-deviation sum is 6, so population covariance is 6/5, sample 6/4.
    #[tokio::test]
    async fn windowed_covariance_matches_analytical_values() {
        let rows = [vec![batch(
            &[1, 2, 3, 4, 5],
            &[1.0, 2.0, 3.0, 4.0, 5.0],
            &[2.0, 4.0, 5.0, 4.0, 5.0],
        )]];

        let mut pop = operator_with(MomentFn::CovarPop, 30);
        let out = pop.process(&rows, &[0]).await.unwrap();
        assert!((last_stat(&out).unwrap() - 6.0 / 5.0).abs() < 1e-9);

        let mut samp = operator_with(MomentFn::CovarSamp, 30);
        let out = samp.process(&rows, &[0]).await.unwrap();
        assert!((last_stat(&out).unwrap() - 6.0 / 4.0).abs() < 1e-9);
    }

    /// A single point gives no sample statistic (n < 2) — null, not a panic/NaN.
    #[tokio::test]
    async fn single_point_is_null() {
        let mut op = operator_with(MomentFn::Corr, 30);
        let out = op
            .process(&[vec![batch(&[1], &[1.0], &[2.0])]], &[0])
            .await
            .unwrap();
        assert!(last_stat(&out).is_none());
    }

    /// The frame slides: only the last `retain` rows bound a row's window.
    #[tokio::test]
    async fn frame_is_bounded_by_retain() {
        let mut op = operator_with(MomentFn::Corr, 2);
        // Window of the last row = rows {3,4,5} (retain=2 → 2 preceding + current).
        let out = op
            .process(
                &[vec![batch(
                    &[1, 2, 3, 4, 5],
                    &[100.0, 100.0, 1.0, 2.0, 3.0],
                    &[0.0, 0.0, 1.0, 2.0, 3.0],
                )]],
                &[0],
            )
            .await
            .unwrap();
        // Rows 3,4,5 are perfectly correlated → r = 1.0, unaffected by rows 1,2.
        assert!((last_stat(&out).unwrap() - 1.0).abs() < 1e-9);
    }
}
