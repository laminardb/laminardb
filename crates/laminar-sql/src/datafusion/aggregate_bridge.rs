//! F075: DataFusion Aggregate Bridge
//!
//! Bridges DataFusion's 50+ built-in aggregate functions into LaminarDB's
//! [`DynAccumulator`] / [`DynAggregatorFactory`] traits. This avoids
//! reimplementing statistical functions (STDDEV, VARIANCE, PERCENTILE, etc.)
//! that DataFusion already provides.
//!
//! # Architecture
//!
//! ```text
//! DataFusion World                 LaminarDB World
//! ┌───────────────────┐           ┌──────────────────────┐
//! │ AggregateUDF      │           │ DynAggregatorFactory │
//! │   └─▶ Accumulator │──bridge──▶│   └─▶ DynAccumulator │
//! │       (ScalarValue)│           │       (ScalarResult) │
//! └───────────────────┘           └──────────────────────┘
//! ```
//!
//! # Ring Architecture
//!
//! This bridge is Ring 1 (allocates, uses dynamic dispatch). Ring 0 workloads
//! continue to use the hand-written static-dispatch aggregators.

use std::cell::RefCell;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::{DataType, Field, FieldRef, Schema};
use datafusion::execution::FunctionRegistry;
use datafusion_common::ScalarValue;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::AggregateUDF;

use laminar_core::operator::window::{DynAccumulator, DynAggregatorFactory, ScalarResult};
use laminar_core::operator::Event;

// ============================================================================
// Type Conversion: ScalarValue <-> ScalarResult
// ============================================================================

/// Converts a DataFusion [`ScalarValue`] to a LaminarDB [`ScalarResult`].
///
/// Handles the common numeric types. Non-numeric types map to [`ScalarResult::Null`].
#[must_use]
pub fn scalar_value_to_result(sv: &ScalarValue) -> ScalarResult {
    match sv {
        ScalarValue::Int64(Some(v)) => ScalarResult::Int64(*v),
        ScalarValue::Int64(None) => ScalarResult::OptionalInt64(None),
        ScalarValue::Float64(Some(v)) => ScalarResult::Float64(*v),
        ScalarValue::Float64(None) => ScalarResult::OptionalFloat64(None),
        ScalarValue::UInt64(Some(v)) => ScalarResult::UInt64(*v),
        ScalarValue::UInt64(None) => ScalarResult::Null,
        // Widen smaller int types
        ScalarValue::Int8(Some(v)) => ScalarResult::Int64(i64::from(*v)),
        ScalarValue::Int16(Some(v)) => ScalarResult::Int64(i64::from(*v)),
        ScalarValue::Int32(Some(v)) => ScalarResult::Int64(i64::from(*v)),
        ScalarValue::UInt8(Some(v)) => ScalarResult::UInt64(u64::from(*v)),
        ScalarValue::UInt16(Some(v)) => ScalarResult::UInt64(u64::from(*v)),
        ScalarValue::UInt32(Some(v)) => ScalarResult::UInt64(u64::from(*v)),
        // Widen smaller float types
        ScalarValue::Float32(Some(v)) => ScalarResult::Float64(f64::from(*v)),
        ScalarValue::Float32(None) => ScalarResult::OptionalFloat64(None),
        _ => ScalarResult::Null,
    }
}

/// Converts a [`ScalarResult`] to a DataFusion [`ScalarValue`].
#[must_use]
pub fn result_to_scalar_value(sr: &ScalarResult) -> ScalarValue {
    match sr {
        ScalarResult::Int64(v) => ScalarValue::Int64(Some(*v)),
        ScalarResult::Float64(v) => ScalarValue::Float64(Some(*v)),
        ScalarResult::UInt64(v) => ScalarValue::UInt64(Some(*v)),
        ScalarResult::OptionalInt64(v) => ScalarValue::Int64(*v),
        ScalarResult::OptionalFloat64(v) => ScalarValue::Float64(*v),
        ScalarResult::Null => ScalarValue::Null,
    }
}

// ============================================================================
// DataFusion Accumulator Adapter
// ============================================================================

/// Adapts a DataFusion [`datafusion_expr::Accumulator`] into LaminarDB's
/// [`DynAccumulator`] trait.
///
/// Uses `RefCell` for interior mutability since DataFusion's `evaluate()`
/// and `state()` require `&mut self` but LaminarDB's `result_scalar()`
/// and `serialize()` take `&self`.
pub struct DataFusionAccumulatorAdapter {
    /// The wrapped DataFusion accumulator (RefCell for interior mutability)
    inner: RefCell<Box<dyn datafusion_expr::Accumulator>>,
    /// Column indices to extract from events
    column_indices: Vec<usize>,
    /// Input types (for creating arrays during merge)
    input_types: Vec<DataType>,
    /// Function name (for type_tag/debug)
    function_name: String,
}

// SAFETY: DataFusion accumulators are Send. RefCell is Send when T is Send.
// The adapter is only accessed from a single thread (Ring 1 processing).
unsafe impl Send for DataFusionAccumulatorAdapter {}

impl std::fmt::Debug for DataFusionAccumulatorAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionAccumulatorAdapter")
            .field("function_name", &self.function_name)
            .field("column_indices", &self.column_indices)
            .field("input_types", &self.input_types)
            .finish_non_exhaustive()
    }
}

impl DataFusionAccumulatorAdapter {
    /// Creates a new adapter wrapping a DataFusion accumulator.
    pub fn new(
        inner: Box<dyn datafusion_expr::Accumulator>,
        column_indices: Vec<usize>,
        input_types: Vec<DataType>,
        function_name: String,
    ) -> Self {
        Self {
            inner: RefCell::new(inner),
            column_indices,
            input_types,
            function_name,
        }
    }

    /// Returns the wrapped function name.
    #[must_use]
    pub fn function_name(&self) -> &str {
        &self.function_name
    }

    /// Extracts the relevant columns from a `RecordBatch`.
    fn extract_columns(&self, batch: &arrow_array::RecordBatch) -> Vec<ArrayRef> {
        self.column_indices
            .iter()
            .enumerate()
            .map(|(arg_idx, &col_idx)| {
                if col_idx < batch.num_columns() {
                    Arc::clone(batch.column(col_idx))
                } else {
                    let dt = self
                        .input_types
                        .get(arg_idx)
                        .cloned()
                        .unwrap_or(DataType::Int64);
                    arrow_array::new_null_array(&dt, batch.num_rows())
                }
            })
            .collect()
    }
}

impl DynAccumulator for DataFusionAccumulatorAdapter {
    fn add_event(&mut self, event: &Event) {
        let columns = self.extract_columns(&event.data);
        let _ = self.inner.borrow_mut().update_batch(&columns);
    }

    fn merge_dyn(&mut self, other: &dyn DynAccumulator) {
        let other = other
            .as_any()
            .downcast_ref::<DataFusionAccumulatorAdapter>()
            .expect("merge_dyn: type mismatch, expected DataFusionAccumulatorAdapter");

        if let Ok(state_values) = other.inner.borrow_mut().state() {
            let state_arrays: Vec<ArrayRef> = state_values
                .iter()
                .filter_map(|sv| sv.to_array().ok())
                .collect();
            if !state_arrays.is_empty() {
                let _ = self.inner.borrow_mut().merge_batch(&state_arrays);
            }
        }
    }

    fn result_scalar(&self) -> ScalarResult {
        match self.inner.borrow_mut().evaluate() {
            Ok(sv) => scalar_value_to_result(&sv),
            Err(_) => ScalarResult::Null,
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.borrow().size() <= std::mem::size_of::<Self>()
    }

    fn clone_box(&self) -> Box<dyn DynAccumulator> {
        panic!(
            "clone_box not supported for DataFusion adapter '{}'; \
             use the factory to create new accumulators",
            self.function_name
        )
    }

    #[allow(clippy::cast_possible_truncation)] // Wire format uses fixed-width integers
    fn serialize(&self) -> Vec<u8> {
        match self.inner.borrow_mut().state() {
            Ok(state_values) => {
                let mut buf = Vec::new();
                let count = state_values.len() as u32;
                buf.extend_from_slice(&count.to_le_bytes());
                for sv in &state_values {
                    let bytes = sv.to_string();
                    let len = bytes.len() as u32;
                    buf.extend_from_slice(&len.to_le_bytes());
                    buf.extend_from_slice(bytes.as_bytes());
                }
                buf
            }
            Err(_) => Vec::new(),
        }
    }

    fn result_field(&self) -> Field {
        let result = self.result_scalar();
        let dt = result.data_type();
        let dt = if dt == DataType::Null {
            DataType::Float64
        } else {
            dt
        };
        Field::new(&self.function_name, dt, true)
    }

    fn type_tag(&self) -> &'static str {
        "datafusion_adapter"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// ============================================================================
// DataFusion Aggregate Factory
// ============================================================================

/// Factory for creating [`DataFusionAccumulatorAdapter`] instances.
///
/// Wraps a DataFusion [`AggregateUDF`] and provides the [`DynAggregatorFactory`]
/// interface for use with `CompositeAggregator`.
pub struct DataFusionAggregateFactory {
    /// The DataFusion aggregate UDF
    udf: Arc<AggregateUDF>,
    /// Column indices to extract from events
    column_indices: Vec<usize>,
    /// Input types for the aggregate
    input_types: Vec<DataType>,
}

impl std::fmt::Debug for DataFusionAggregateFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionAggregateFactory")
            .field("name", &self.udf.name())
            .field("column_indices", &self.column_indices)
            .field("input_types", &self.input_types)
            .finish()
    }
}

impl DataFusionAggregateFactory {
    /// Creates a new factory for the given DataFusion aggregate UDF.
    pub fn new(
        udf: Arc<AggregateUDF>,
        column_indices: Vec<usize>,
        input_types: Vec<DataType>,
    ) -> Self {
        Self {
            udf,
            column_indices,
            input_types,
        }
    }

    /// Returns the name of the wrapped aggregate function.
    #[must_use]
    pub fn name(&self) -> &str {
        self.udf.name()
    }

    /// Creates a DataFusion accumulator from the UDF.
    fn create_df_accumulator(&self) -> Box<dyn datafusion_expr::Accumulator> {
        let return_type = self
            .udf
            .return_type(&self.input_types)
            .unwrap_or(DataType::Float64);
        let return_field: FieldRef =
            Arc::new(Field::new(self.udf.name(), return_type, true));
        let schema = Schema::new(
            self.input_types
                .iter()
                .enumerate()
                .map(|(i, dt)| Field::new(format!("col_{i}"), dt.clone(), true))
                .collect::<Vec<_>>(),
        );
        let expr_fields: Vec<FieldRef> = self
            .input_types
            .iter()
            .enumerate()
            .map(|(i, dt)| Arc::new(Field::new(format!("col_{i}"), dt.clone(), true)) as FieldRef)
            .collect();
        let args = AccumulatorArgs {
            return_field,
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: self.udf.name(),
            is_distinct: false,
            exprs: &[],
            expr_fields: &expr_fields,
        };
        self.udf
            .accumulator(args)
            .expect("Failed to create DataFusion accumulator")
    }
}

impl DynAggregatorFactory for DataFusionAggregateFactory {
    fn create_accumulator(&self) -> Box<dyn DynAccumulator> {
        let inner = self.create_df_accumulator();
        Box::new(DataFusionAccumulatorAdapter::new(
            inner,
            self.column_indices.clone(),
            self.input_types.clone(),
            self.udf.name().to_string(),
        ))
    }

    fn result_field(&self) -> Field {
        let return_type = self
            .udf
            .return_type(&self.input_types)
            .unwrap_or(DataType::Float64);
        Field::new(self.udf.name(), return_type, true)
    }

    fn clone_box(&self) -> Box<dyn DynAggregatorFactory> {
        Box::new(DataFusionAggregateFactory {
            udf: Arc::clone(&self.udf),
            column_indices: self.column_indices.clone(),
            input_types: self.input_types.clone(),
        })
    }

    fn type_tag(&self) -> &'static str {
        "datafusion_factory"
    }
}

// ============================================================================
// Built-in Aggregate Lookup
// ============================================================================

/// Looks up a DataFusion built-in aggregate function by name.
///
/// Returns `None` if the function is not a recognized DataFusion aggregate.
#[must_use]
pub fn lookup_aggregate_udf(
    ctx: &datafusion::prelude::SessionContext,
    name: &str,
) -> Option<Arc<AggregateUDF>> {
    let normalized = name.to_lowercase();
    ctx.udaf(&normalized).ok()
}

/// Creates a [`DataFusionAggregateFactory`] for a named built-in aggregate.
///
/// Returns `None` if the function name is not recognized.
#[must_use]
pub fn create_aggregate_factory(
    ctx: &datafusion::prelude::SessionContext,
    name: &str,
    column_indices: Vec<usize>,
    input_types: Vec<DataType>,
) -> Option<DataFusionAggregateFactory> {
    lookup_aggregate_udf(ctx, name)
        .map(|udf| DataFusionAggregateFactory::new(udf, column_indices, input_types))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, RecordBatch};
    use datafusion::prelude::SessionContext;

    fn float_event(ts: i64, values: Vec<f64>) -> Event {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(values))],
        )
        .unwrap();
        Event { timestamp: ts, data: batch }
    }

    fn int_event(ts: i64, values: Vec<i64>) -> Event {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        Event { timestamp: ts, data: batch }
    }

    fn two_col_float_event(ts: i64, col0: Vec<f64>, col1: Vec<f64>) -> Event {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(col0)),
                Arc::new(Float64Array::from(col1)),
            ],
        )
        .unwrap();
        Event { timestamp: ts, data: batch }
    }

    // ── ScalarValue Conversion Tests ────────────────────────────────────

    #[test]
    fn test_scalar_value_to_result_int64() {
        let sv = ScalarValue::Int64(Some(42));
        assert_eq!(scalar_value_to_result(&sv), ScalarResult::Int64(42));
    }

    #[test]
    fn test_scalar_value_to_result_float64() {
        let sv = ScalarValue::Float64(Some(3.14));
        assert_eq!(scalar_value_to_result(&sv), ScalarResult::Float64(3.14));
    }

    #[test]
    fn test_scalar_value_to_result_uint64() {
        let sv = ScalarValue::UInt64(Some(100));
        assert_eq!(scalar_value_to_result(&sv), ScalarResult::UInt64(100));
    }

    #[test]
    fn test_scalar_value_to_result_null_int64() {
        let sv = ScalarValue::Int64(None);
        assert_eq!(
            scalar_value_to_result(&sv),
            ScalarResult::OptionalInt64(None)
        );
    }

    #[test]
    fn test_scalar_value_to_result_null_float64() {
        let sv = ScalarValue::Float64(None);
        assert_eq!(
            scalar_value_to_result(&sv),
            ScalarResult::OptionalFloat64(None)
        );
    }

    #[test]
    fn test_scalar_value_to_result_smaller_ints() {
        assert_eq!(
            scalar_value_to_result(&ScalarValue::Int8(Some(8))),
            ScalarResult::Int64(8)
        );
        assert_eq!(
            scalar_value_to_result(&ScalarValue::Int16(Some(16))),
            ScalarResult::Int64(16)
        );
        assert_eq!(
            scalar_value_to_result(&ScalarValue::Int32(Some(32))),
            ScalarResult::Int64(32)
        );
        assert_eq!(
            scalar_value_to_result(&ScalarValue::UInt8(Some(8))),
            ScalarResult::UInt64(8)
        );
    }

    #[test]
    fn test_scalar_value_to_result_float32() {
        let sv = ScalarValue::Float32(Some(2.5));
        assert_eq!(
            scalar_value_to_result(&sv),
            ScalarResult::Float64(f64::from(2.5f32))
        );
    }

    #[test]
    fn test_scalar_value_to_result_unsupported() {
        let sv = ScalarValue::Utf8(Some("hello".to_string()));
        assert_eq!(scalar_value_to_result(&sv), ScalarResult::Null);
    }

    #[test]
    fn test_result_to_scalar_value_roundtrip() {
        // Exact roundtrip for non-optional variants
        let exact_cases = vec![
            ScalarResult::Int64(42),
            ScalarResult::Float64(3.14),
            ScalarResult::UInt64(100),
        ];
        for sr in &exact_cases {
            let sv = result_to_scalar_value(sr);
            let back = scalar_value_to_result(&sv);
            assert_eq!(&back, sr, "Roundtrip failed for {sr:?}");
        }

        // Optional(Some(v)) normalizes to non-optional through ScalarValue
        // because ScalarValue::Int64(Some(7)) maps back to ScalarResult::Int64(7)
        let sv = result_to_scalar_value(&ScalarResult::OptionalInt64(Some(7)));
        assert_eq!(scalar_value_to_result(&sv), ScalarResult::Int64(7));

        let sv = result_to_scalar_value(&ScalarResult::OptionalFloat64(Some(2.718)));
        assert_eq!(scalar_value_to_result(&sv), ScalarResult::Float64(2.718));

        // Optional None roundtrips back to OptionalNone (ScalarValue preserves type)
        let sv = result_to_scalar_value(&ScalarResult::OptionalInt64(None));
        assert_eq!(scalar_value_to_result(&sv), ScalarResult::OptionalInt64(None));

        let sv = result_to_scalar_value(&ScalarResult::OptionalFloat64(None));
        assert_eq!(scalar_value_to_result(&sv), ScalarResult::OptionalFloat64(None));

        // Null roundtrips correctly
        let sv = result_to_scalar_value(&ScalarResult::Null);
        assert_eq!(scalar_value_to_result(&sv), ScalarResult::Null);
    }

    // ── Factory Tests ───────────────────────────────────────────────────

    #[test]
    fn test_factory_count() {
        let ctx = SessionContext::new();
        let factory = create_aggregate_factory(&ctx, "count", vec![0], vec![DataType::Int64]);
        assert!(factory.is_some(), "count should be a recognized aggregate");
        assert_eq!(factory.unwrap().name(), "count");
    }

    #[test]
    fn test_factory_sum() {
        let ctx = SessionContext::new();
        let factory = create_aggregate_factory(&ctx, "sum", vec![0], vec![DataType::Float64]);
        assert!(factory.is_some());
        assert_eq!(factory.unwrap().name(), "sum");
    }

    #[test]
    fn test_factory_avg() {
        let ctx = SessionContext::new();
        let factory = create_aggregate_factory(&ctx, "avg", vec![0], vec![DataType::Float64]);
        assert!(factory.is_some());
    }

    #[test]
    fn test_factory_stddev() {
        let ctx = SessionContext::new();
        let factory = create_aggregate_factory(&ctx, "stddev", vec![0], vec![DataType::Float64]);
        assert!(factory.is_some(), "stddev should be available in DataFusion");
    }

    #[test]
    fn test_factory_unknown() {
        let ctx = SessionContext::new();
        let factory = create_aggregate_factory(
            &ctx, "nonexistent_aggregate_xyz", vec![0], vec![DataType::Int64],
        );
        assert!(factory.is_none());
    }

    #[test]
    fn test_factory_result_field() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "sum", vec![0], vec![DataType::Float64]).unwrap();
        let field = factory.result_field();
        assert_eq!(field.name(), "sum");
        assert_eq!(field.data_type(), &DataType::Float64);
    }

    #[test]
    fn test_factory_clone_box() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "count", vec![0], vec![DataType::Int64]).unwrap();
        let cloned = factory.clone_box();
        assert_eq!(cloned.type_tag(), "datafusion_factory");
    }

    // ── Adapter Basics ──────────────────────────────────────────────────

    #[test]
    fn test_adapter_count_basic() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "count", vec![0], vec![DataType::Int64]).unwrap();
        let mut acc = factory.create_accumulator();

        let result = acc.result_scalar();
        assert!(
            matches!(result, ScalarResult::Int64(0) | ScalarResult::UInt64(0)),
            "Expected 0, got {result:?}"
        );

        acc.add_event(&int_event(1000, vec![10, 20, 30]));
        let result = acc.result_scalar();
        assert!(
            matches!(result, ScalarResult::Int64(3) | ScalarResult::UInt64(3)),
            "Expected 3, got {result:?}"
        );

        acc.add_event(&int_event(2000, vec![40, 50]));
        let result = acc.result_scalar();
        assert!(
            matches!(result, ScalarResult::Int64(5) | ScalarResult::UInt64(5)),
            "Expected 5, got {result:?}"
        );
    }

    #[test]
    fn test_adapter_sum_float64() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "sum", vec![0], vec![DataType::Float64]).unwrap();
        let mut acc = factory.create_accumulator();

        acc.add_event(&float_event(1000, vec![1.5, 2.5, 3.0]));
        assert_eq!(acc.result_scalar(), ScalarResult::Float64(7.0));
    }

    #[test]
    fn test_adapter_avg_float64() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "avg", vec![0], vec![DataType::Float64]).unwrap();
        let mut acc = factory.create_accumulator();

        acc.add_event(&float_event(1000, vec![10.0, 20.0, 30.0]));
        assert_eq!(acc.result_scalar(), ScalarResult::Float64(20.0));
    }

    #[test]
    fn test_adapter_min_float64() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "min", vec![0], vec![DataType::Float64]).unwrap();
        let mut acc = factory.create_accumulator();

        acc.add_event(&float_event(1000, vec![30.0, 10.0, 20.0]));
        assert_eq!(acc.result_scalar(), ScalarResult::Float64(10.0));
    }

    #[test]
    fn test_adapter_max_float64() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "max", vec![0], vec![DataType::Float64]).unwrap();
        let mut acc = factory.create_accumulator();

        acc.add_event(&float_event(1000, vec![30.0, 10.0, 20.0]));
        assert_eq!(acc.result_scalar(), ScalarResult::Float64(30.0));
    }

    #[test]
    fn test_adapter_sum_int64() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "sum", vec![0], vec![DataType::Int64]).unwrap();
        let mut acc = factory.create_accumulator();

        acc.add_event(&int_event(1000, vec![10, 20, 30]));
        assert_eq!(acc.result_scalar(), ScalarResult::Int64(60));
    }

    #[test]
    fn test_adapter_type_tag() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "sum", vec![0], vec![DataType::Float64]).unwrap();
        let acc = factory.create_accumulator();
        assert_eq!(acc.type_tag(), "datafusion_adapter");
    }

    #[test]
    fn test_adapter_result_field() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "sum", vec![0], vec![DataType::Float64]).unwrap();
        let mut acc = factory.create_accumulator();
        acc.add_event(&float_event(1000, vec![1.0]));
        assert_eq!(acc.result_field().name(), "sum");
    }

    // ── Merge Tests ─────────────────────────────────────────────────────

    #[test]
    fn test_adapter_merge_sum() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "sum", vec![0], vec![DataType::Float64]).unwrap();

        let mut acc1 = factory.create_accumulator();
        acc1.add_event(&float_event(1000, vec![1.0, 2.0]));

        let mut acc2 = factory.create_accumulator();
        acc2.add_event(&float_event(2000, vec![3.0, 4.0]));

        acc1.merge_dyn(acc2.as_ref());
        assert_eq!(acc1.result_scalar(), ScalarResult::Float64(10.0));
    }

    #[test]
    fn test_adapter_merge_count() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "count", vec![0], vec![DataType::Int64]).unwrap();

        let mut acc1 = factory.create_accumulator();
        acc1.add_event(&int_event(1000, vec![1, 2, 3]));

        let mut acc2 = factory.create_accumulator();
        acc2.add_event(&int_event(2000, vec![4, 5]));

        acc1.merge_dyn(acc2.as_ref());
        let result = acc1.result_scalar();
        assert!(
            matches!(result, ScalarResult::Int64(5) | ScalarResult::UInt64(5)),
            "Expected 5 after merge, got {result:?}"
        );
    }

    #[test]
    fn test_adapter_merge_avg() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "avg", vec![0], vec![DataType::Float64]).unwrap();

        let mut acc1 = factory.create_accumulator();
        acc1.add_event(&float_event(1000, vec![10.0, 20.0]));

        let mut acc2 = factory.create_accumulator();
        acc2.add_event(&float_event(2000, vec![30.0]));

        acc1.merge_dyn(acc2.as_ref());
        assert_eq!(acc1.result_scalar(), ScalarResult::Float64(20.0));
    }

    #[test]
    fn test_adapter_merge_empty() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "sum", vec![0], vec![DataType::Float64]).unwrap();

        let mut acc1 = factory.create_accumulator();
        acc1.add_event(&float_event(1000, vec![5.0]));

        let acc2 = factory.create_accumulator();
        acc1.merge_dyn(acc2.as_ref());
        assert_eq!(acc1.result_scalar(), ScalarResult::Float64(5.0));
    }

    // ── Built-in Aggregate Pass-Through Tests ───────────────────────────

    #[test]
    fn test_adapter_stddev() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "stddev", vec![0], vec![DataType::Float64]).unwrap();
        let mut acc = factory.create_accumulator();

        acc.add_event(&float_event(
            1000,
            vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0],
        ));
        let result = acc.result_scalar();
        if let ScalarResult::Float64(v) = result {
            assert!((v - 2.138).abs() < 0.01, "Expected ~2.138, got {v}");
        } else {
            panic!("Expected Float64 result, got {result:?}");
        }
    }

    #[test]
    fn test_adapter_variance() {
        let ctx = SessionContext::new();
        if let Some(factory) =
            create_aggregate_factory(&ctx, "var_samp", vec![0], vec![DataType::Float64])
        {
            let mut acc = factory.create_accumulator();
            acc.add_event(&float_event(
                1000,
                vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0],
            ));
            if let ScalarResult::Float64(v) = acc.result_scalar() {
                assert!((v - 4.571).abs() < 0.01, "Expected ~4.571, got {v}");
            }
        }
    }

    #[test]
    fn test_adapter_median() {
        let ctx = SessionContext::new();
        if let Some(factory) =
            create_aggregate_factory(&ctx, "median", vec![0], vec![DataType::Float64])
        {
            let mut acc = factory.create_accumulator();
            acc.add_event(&float_event(1000, vec![1.0, 2.0, 3.0, 4.0, 5.0]));
            assert_eq!(acc.result_scalar(), ScalarResult::Float64(3.0));
        }
    }

    // ── Serialize Tests ─────────────────────────────────────────────────

    #[test]
    fn test_adapter_serialize() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "sum", vec![0], vec![DataType::Float64]).unwrap();
        let mut acc = factory.create_accumulator();
        acc.add_event(&float_event(1000, vec![1.0, 2.0, 3.0]));
        assert!(!acc.serialize().is_empty());
    }

    #[test]
    fn test_adapter_serialize_empty() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "sum", vec![0], vec![DataType::Float64]).unwrap();
        let acc = factory.create_accumulator();
        assert!(!acc.serialize().is_empty());
    }

    // ── Lookup Tests ────────────────────────────────────────────────────

    #[test]
    fn test_lookup_common_aggregates() {
        let ctx = SessionContext::new();
        for name in &["count", "sum", "min", "max", "avg"] {
            assert!(
                lookup_aggregate_udf(&ctx, name).is_some(),
                "Expected '{name}' to be a recognized aggregate"
            );
        }
    }

    #[test]
    fn test_lookup_statistical_aggregates() {
        let ctx = SessionContext::new();
        for name in &["stddev", "stddev_pop", "median"] {
            // Just verify lookup doesn't panic
            let _ = lookup_aggregate_udf(&ctx, name);
        }
    }

    #[test]
    fn test_lookup_case_insensitive() {
        let ctx = SessionContext::new();
        assert!(lookup_aggregate_udf(&ctx, "COUNT").is_some());
        assert!(lookup_aggregate_udf(&ctx, "Sum").is_some());
        assert!(lookup_aggregate_udf(&ctx, "AVG").is_some());
    }

    // ── Multi-column Tests ──────────────────────────────────────────────

    #[test]
    fn test_adapter_multi_column_covar() {
        let ctx = SessionContext::new();
        if let Some(factory) = create_aggregate_factory(
            &ctx,
            "covar_samp",
            vec![0, 1],
            vec![DataType::Float64, DataType::Float64],
        ) {
            let mut acc = factory.create_accumulator();
            acc.add_event(&two_col_float_event(
                1000,
                vec![1.0, 2.0, 3.0, 4.0, 5.0],
                vec![1.0, 2.0, 3.0, 4.0, 5.0],
            ));
            if let ScalarResult::Float64(v) = acc.result_scalar() {
                assert!((v - 2.5).abs() < 0.01, "Expected covar ~2.5, got {v}");
            }
        }
    }

    // ── Registration Tests ──────────────────────────────────────────────

    #[test]
    fn test_create_aggregate_factory_api() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "count", vec![0], vec![DataType::Int64]).unwrap();
        let acc = factory.create_accumulator();
        assert_eq!(acc.type_tag(), "datafusion_adapter");
    }

    #[test]
    fn test_factory_creates_independent_accumulators() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "sum", vec![0], vec![DataType::Float64]).unwrap();

        let mut acc1 = factory.create_accumulator();
        let mut acc2 = factory.create_accumulator();

        acc1.add_event(&float_event(1000, vec![10.0]));
        acc2.add_event(&float_event(2000, vec![20.0]));

        assert_eq!(acc1.result_scalar(), ScalarResult::Float64(10.0));
        assert_eq!(acc2.result_scalar(), ScalarResult::Float64(20.0));
    }

    #[test]
    fn test_adapter_function_name() {
        let ctx = SessionContext::new();
        let factory =
            create_aggregate_factory(&ctx, "sum", vec![0], vec![DataType::Float64]).unwrap();
        let acc = factory.create_accumulator();
        let adapter = acc
            .as_any()
            .downcast_ref::<DataFusionAccumulatorAdapter>()
            .expect("should be adapter");
        assert_eq!(adapter.function_name(), "sum");
    }
}
