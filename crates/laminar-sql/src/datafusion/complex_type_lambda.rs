//! Lambda higher-order functions for arrays and maps (F-SCHEMA-015 Tier 3).
//!
//! Provides vectorized lambda evaluation over Arrow arrays:
//!
//! | Function | Lambda | Strategy |
//! |----------|--------|----------|
//! | `array_transform(arr, lambda)` | `x -> expr` | Flatten, eval, re-group |
//! | `array_filter(arr, lambda)` | `x -> bool` | Flatten, eval, filter+rebuild |
//! | `array_reduce(arr, init, lambda)` | `(acc, x) -> expr` | Sequential fold |
//! | `map_filter(map, lambda)` | `(k, v) -> bool` | Eval on k+v, filter entries |
//! | `map_transform_values(map, lambda)` | `(k, v) -> expr` | Eval on k+v, replace vals |
//!
//! Lambda expressions are specified as string literal SQL expressions.
//! They are evaluated using DataFusion's SQL engine against a temporary
//! table containing the element values. Native lambda syntax is deferred.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::{Array, ArrayRef, BooleanArray, ListArray, MapArray, StructArray};
use arrow_schema::{Field, Fields, Schema};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::json_udf::expand_args;

/// Registers all lambda HOFs with the given session context.
pub fn register_lambda_functions(ctx: &datafusion::prelude::SessionContext) {
    use datafusion_expr::ScalarUDF;

    ctx.register_udf(ScalarUDF::new_from_impl(ArrayTransform::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(ArrayFilter::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(ArrayReduce::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(MapFilter::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(MapTransformValues::new()));
}

/// Evaluate a SQL expression against a `RecordBatch`, returning the result column.
///
/// The expression can reference columns by name from the batch schema.
fn eval_expr_on_batch(sql_expr: &str, batch: &arrow_array::RecordBatch) -> Result<ArrayRef> {
    // Build a SELECT <expr> FROM <data> query using DataFusion.
    let ctx = datafusion::prelude::SessionContext::new();
    let provider =
        datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])?;
    let rt = tokio::runtime::Handle::try_current().map_err(|e| {
        datafusion_common::DataFusionError::Internal(format!(
            "lambda eval requires tokio runtime: {e}"
        ))
    })?;
    // Use block_in_place to allow blocking inside an already-running tokio runtime.
    tokio::task::block_in_place(|| {
        rt.block_on(async {
            ctx.register_table("__lambda_data", Arc::new(provider))?;
            let df = ctx
                .sql(&format!("SELECT {sql_expr} FROM __lambda_data"))
                .await?;
            let batches = df.collect().await?;
            if batches.is_empty() {
                Err(datafusion_common::DataFusionError::Internal(
                    "lambda expression returned no data".into(),
                ))
            } else {
                // Concatenate all result batches and return the first column.
                let result = arrow::compute::concat_batches(&batches[0].schema(), &batches)?;
                Ok(result.column(0).clone())
            }
        })
    })
}

fn scalar_string_value(cv: &ColumnarValue) -> Result<String> {
    match cv {
        ColumnarValue::Scalar(s) => {
            let arr = s.to_array_of_size(1)?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal("expected Utf8 argument".into())
                })?;
            Ok(str_arr.value(0).to_string())
        }
        ColumnarValue::Array(arr) => {
            let str_arr = arr
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal("expected Utf8 argument".into())
                })?;
            Ok(str_arr.value(0).to_string())
        }
    }
}

// ══════════════════════════════════════════════════════════════════
// array_transform(arr, lambda_str) -> List
// ══════════════════════════════════════════════════════════════════

/// `array_transform(arr, 'x + 1')` — apply a lambda to each element.
#[derive(Debug)]
pub struct ArrayTransform {
    signature: Signature,
}

impl ArrayTransform {
    /// Creates a new `array_transform` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl Default for ArrayTransform {
    fn default() -> Self {
        Self::new()
    }
}
impl PartialEq for ArrayTransform {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for ArrayTransform {}
impl Hash for ArrayTransform {
    fn hash<H: Hasher>(&self, s: &mut H) {
        "array_transform".hash(s);
    }
}

impl ScalarUDFImpl for ArrayTransform {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "array_transform"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::List(f) => Ok(DataType::List(Arc::clone(f))),
            _ => Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            )))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let list_arr = expanded[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "array_transform: first arg must be List".into(),
                )
            })?;

        let lambda_str = scalar_string_value(&args.args[1])?;
        let flat_values = list_arr.values();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            flat_values.data_type().clone(),
            true,
        )]));
        let batch = arrow_array::RecordBatch::try_new(schema, vec![Arc::clone(flat_values)])?;

        let result_arr = eval_expr_on_batch(&lambda_str, &batch)?;

        let new_field = Arc::new(Field::new("item", result_arr.data_type().clone(), true));
        let new_list = ListArray::try_new(
            new_field,
            list_arr.offsets().clone(),
            result_arr,
            list_arr.nulls().cloned(),
        )?;
        Ok(ColumnarValue::Array(Arc::new(new_list)))
    }
}

// ══════════════════════════════════════════════════════════════════
// array_filter(arr, lambda_str) -> List
// ══════════════════════════════════════════════════════════════════

/// `array_filter(arr, 'x > 0')` — filter elements by a boolean lambda.
#[derive(Debug)]
pub struct ArrayFilter {
    signature: Signature,
}

impl ArrayFilter {
    /// Creates a new `array_filter` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl Default for ArrayFilter {
    fn default() -> Self {
        Self::new()
    }
}
impl PartialEq for ArrayFilter {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for ArrayFilter {}
impl Hash for ArrayFilter {
    fn hash<H: Hasher>(&self, s: &mut H) {
        "array_filter".hash(s);
    }
}

impl ScalarUDFImpl for ArrayFilter {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "array_filter"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::List(f) => Ok(DataType::List(Arc::clone(f))),
            _ => Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            )))),
        }
    }

    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_possible_wrap,
        clippy::cast_possible_truncation
    )]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let list_arr = expanded[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "array_filter: first arg must be List".into(),
                )
            })?;

        let lambda_str = scalar_string_value(&args.args[1])?;
        let flat_values = list_arr.values();
        let elem_type = flat_values.data_type().clone();

        let schema = Arc::new(Schema::new(vec![Field::new("x", elem_type.clone(), true)]));
        let batch = arrow_array::RecordBatch::try_new(schema, vec![Arc::clone(flat_values)])?;

        let mask_arr = eval_expr_on_batch(&lambda_str, &batch)?;
        let mask = mask_arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "array_filter: lambda must return Boolean".into(),
                )
            })?;

        let mut offsets = vec![0i32];
        let mut filtered_indices: Vec<usize> = Vec::new();

        for row in 0..list_arr.len() {
            let start = list_arr.value_offsets()[row] as usize;
            let end = list_arr.value_offsets()[row + 1] as usize;

            for i in start..end {
                if !mask.is_null(i) && mask.value(i) {
                    filtered_indices.push(i);
                }
            }
            offsets.push(filtered_indices.len() as i32);
        }

        let indices = arrow_array::UInt32Array::from(
            filtered_indices
                .iter()
                .map(|&i| i as u32)
                .collect::<Vec<_>>(),
        );
        let filtered_values = arrow::compute::take(flat_values.as_ref(), &indices, None)?;

        let new_field = Arc::new(Field::new("item", elem_type, true));
        let new_offsets =
            arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(offsets));
        let new_list = ListArray::try_new(
            new_field,
            new_offsets,
            filtered_values,
            list_arr.nulls().cloned(),
        )?;
        Ok(ColumnarValue::Array(Arc::new(new_list)))
    }
}

// ══════════════════════════════════════════════════════════════════
// array_reduce(arr, init, lambda_str) -> scalar
// ══════════════════════════════════════════════════════════════════

/// `array_reduce(arr, init, '(acc + x)')` — fold/reduce array elements.
#[derive(Debug)]
pub struct ArrayReduce {
    signature: Signature,
}

impl ArrayReduce {
    /// Creates a new `array_reduce` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

impl Default for ArrayReduce {
    fn default() -> Self {
        Self::new()
    }
}
impl PartialEq for ArrayReduce {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for ArrayReduce {}
impl Hash for ArrayReduce {
    fn hash<H: Hasher>(&self, s: &mut H) {
        "array_reduce".hash(s);
    }
}

impl ScalarUDFImpl for ArrayReduce {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "array_reduce"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.get(1).cloned().unwrap_or(DataType::Int64))
    }

    #[allow(clippy::cast_sign_loss)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let list_arr = expanded[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "array_reduce: first arg must be List".into(),
                )
            })?;

        let init_arr = &expanded[1];
        let lambda_str = scalar_string_value(&args.args[2])?;

        let elem_type = list_arr.values().data_type().clone();
        let acc_type = init_arr.data_type().clone();

        let schema = Arc::new(Schema::new(vec![
            Field::new("acc", acc_type, true),
            Field::new("x", elem_type, true),
        ]));

        let mut result_builder: Vec<ArrayRef> = Vec::new();

        for row in 0..list_arr.len() {
            let start = list_arr.value_offsets()[row] as usize;
            let end = list_arr.value_offsets()[row + 1] as usize;

            let mut acc: ArrayRef = init_arr.slice(row, 1);

            for i in start..end {
                let x = list_arr.values().slice(i, 1);
                let batch = arrow_array::RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::clone(&acc), x],
                )?;
                let result_col = eval_expr_on_batch(&lambda_str, &batch)?;
                acc = result_col;
            }

            result_builder.push(acc);
        }

        if result_builder.is_empty() {
            return Ok(ColumnarValue::Array(Arc::clone(init_arr)));
        }

        let refs: Vec<&dyn Array> = result_builder
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect();
        let result = arrow::compute::concat(&refs)?;
        Ok(ColumnarValue::Array(result))
    }
}

// ══════════════════════════════════════════════════════════════════
// map_filter(map, lambda_str) -> Map
// ══════════════════════════════════════════════════════════════════

/// `map_filter(map, '(k <> ''temp'')')` — filter map entries by key+value.
#[derive(Debug)]
pub struct MapFilter {
    signature: Signature,
}

impl MapFilter {
    /// Creates a new `map_filter` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl Default for MapFilter {
    fn default() -> Self {
        Self::new()
    }
}
impl PartialEq for MapFilter {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for MapFilter {}
impl Hash for MapFilter {
    fn hash<H: Hasher>(&self, s: &mut H) {
        "map_filter".hash(s);
    }
}

impl ScalarUDFImpl for MapFilter {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "map_filter"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_possible_wrap,
        clippy::cast_possible_truncation
    )]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let map_arr = expanded[0]
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "map_filter: first arg must be Map".into(),
                )
            })?;

        let lambda_str = scalar_string_value(&args.args[1])?;

        let entries = map_arr.entries();
        let key_col = entries.column(0);
        let val_col = entries.column(1);

        let key_type = key_col.data_type().clone();
        let val_type = val_col.data_type().clone();

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", key_type.clone(), true),
            Field::new("v", val_type.clone(), true),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![Arc::clone(key_col), Arc::clone(val_col)],
        )?;

        let mask_arr = eval_expr_on_batch(&lambda_str, &batch)?;
        let mask_bool = mask_arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "map_filter: lambda must return Boolean".into(),
                )
            })?;

        let mut offsets = vec![0i32];
        let mut keep_indices: Vec<usize> = Vec::new();

        for row in 0..map_arr.len() {
            let start = map_arr.value_offsets()[row] as usize;
            let end = map_arr.value_offsets()[row + 1] as usize;

            for i in start..end {
                if !mask_bool.is_null(i) && mask_bool.value(i) {
                    keep_indices.push(i);
                }
            }
            offsets.push(keep_indices.len() as i32);
        }

        let indices = arrow_array::UInt32Array::from(
            keep_indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
        );
        let new_keys = arrow::compute::take(key_col.as_ref(), &indices, None)?;
        let new_vals = arrow::compute::take(val_col.as_ref(), &indices, None)?;

        let struct_fields = Fields::from(vec![
            Field::new("key", key_type, false),
            Field::new("value", val_type, true),
        ]);
        let new_entries = StructArray::try_new(struct_fields, vec![new_keys, new_vals], None)?;

        let entries_field = Field::new("entries", new_entries.data_type().clone(), false);
        let new_offsets =
            arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(offsets));
        let new_map = MapArray::try_new(
            Arc::new(entries_field),
            new_offsets,
            new_entries,
            map_arr.nulls().cloned(),
            false,
        )?;
        Ok(ColumnarValue::Array(Arc::new(new_map)))
    }
}

// ══════════════════════════════════════════════════════════════════
// map_transform_values(map, lambda_str) -> Map
// ══════════════════════════════════════════════════════════════════

/// `map_transform_values(map, 'v * 2')` — transform map values.
#[derive(Debug)]
pub struct MapTransformValues {
    signature: Signature,
}

impl MapTransformValues {
    /// Creates a new `map_transform_values` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl Default for MapTransformValues {
    fn default() -> Self {
        Self::new()
    }
}
impl PartialEq for MapTransformValues {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for MapTransformValues {}
impl Hash for MapTransformValues {
    fn hash<H: Hasher>(&self, s: &mut H) {
        "map_transform_values".hash(s);
    }
}

impl ScalarUDFImpl for MapTransformValues {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "map_transform_values"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let map_arr = expanded[0]
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "map_transform_values: first arg must be Map".into(),
                )
            })?;

        let lambda_str = scalar_string_value(&args.args[1])?;

        let entries = map_arr.entries();
        let key_col = entries.column(0);
        let val_col = entries.column(1);

        let key_type = key_col.data_type().clone();

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", key_type.clone(), true),
            Field::new("v", val_col.data_type().clone(), true),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![Arc::clone(key_col), Arc::clone(val_col)],
        )?;

        let new_vals = eval_expr_on_batch(&lambda_str, &batch)?;

        let struct_fields = Fields::from(vec![
            Field::new("key", key_type, false),
            Field::new("value", new_vals.data_type().clone(), true),
        ]);
        let new_entries =
            StructArray::try_new(struct_fields, vec![Arc::clone(key_col), new_vals], None)?;

        let entries_field = Field::new("entries", new_entries.data_type().clone(), false);
        let new_map = MapArray::try_new(
            Arc::new(entries_field),
            map_arr.offsets().clone(),
            new_entries,
            map_arr.nulls().cloned(),
            false,
        )?;
        Ok(ColumnarValue::Array(Arc::new(new_map)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::*;
    use datafusion::prelude::*;
    use datafusion_common::config::ConfigOptions;

    // ── array_transform ─────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_array_transform_add_one() {
        let values = Int64Array::from(vec![1, 2, 3, 4, 5, 6]);
        let offsets =
            arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(vec![0i32, 3, 6]));
        let list = ListArray::try_new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            offsets,
            Arc::new(values),
            None,
        )
        .unwrap();

        let udf = ArrayTransform::new();
        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::new(list)),
                    ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some(
                        "x + 1".into(),
                    ))),
                ],
                number_rows: 0,
                arg_fields: vec![],
                return_field: Arc::new(Field::new(
                    "output",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    true,
                )),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let la = arr.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(la.len(), 2);
            let row0 = la.value(0);
            let r0 = row0.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(r0.value(0), 2);
            assert_eq!(r0.value(1), 3);
            assert_eq!(r0.value(2), 4);
        } else {
            panic!("expected Array");
        }
    }

    // ── array_filter ────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_array_filter_positive() {
        let values = Int64Array::from(vec![-1, 2, -3, 4]);
        let offsets =
            arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(vec![0i32, 4]));
        let list = ListArray::try_new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            offsets,
            Arc::new(values),
            None,
        )
        .unwrap();

        let udf = ArrayFilter::new();
        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::new(list)),
                    ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some(
                        "x > 0".into(),
                    ))),
                ],
                number_rows: 0,
                arg_fields: vec![],
                return_field: Arc::new(Field::new(
                    "output",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    true,
                )),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let la = arr.as_any().downcast_ref::<ListArray>().unwrap();
            let row0 = la.value(0);
            let r0 = row0.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(r0.len(), 2);
            assert_eq!(r0.value(0), 2);
            assert_eq!(r0.value(1), 4);
        }
    }

    // ── Registration ────────────────────────────────────────────

    #[test]
    fn test_register_lambda_functions() {
        let ctx = SessionContext::new();
        register_lambda_functions(&ctx);

        use datafusion::execution::FunctionRegistry;
        assert!(ctx.udf("array_transform").is_ok());
        assert!(ctx.udf("array_filter").is_ok());
        assert!(ctx.udf("array_reduce").is_ok());
        assert!(ctx.udf("map_filter").is_ok());
        assert!(ctx.udf("map_transform_values").is_ok());
    }
}
