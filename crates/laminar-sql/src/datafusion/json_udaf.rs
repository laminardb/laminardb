//! PostgreSQL-compatible JSON aggregate UDFs (F-SCHEMA-011).
//!
//! - [`JsonAgg`] — `json_agg(expr) -> jsonb` — collects values into a JSON array
//! - [`JsonObjectAgg`] — `json_object_agg(key, value) -> jsonb` — collects
//!   key-value pairs into a JSON object

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::{Array, ArrayRef, LargeBinaryArray, StringArray};
use arrow_schema::Field;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility};

use super::json_types;

// ══════════════════════════════════════════════════════════════════
// json_agg(expression) -> jsonb
// ══════════════════════════════════════════════════════════════════

/// `json_agg(expression) -> jsonb`
///
/// Collects all values of an expression into a JSON array.
/// Executes in Ring 1 via the DataFusion aggregate bridge.
#[derive(Debug)]
pub struct JsonAgg {
    signature: Signature,
}

impl JsonAgg {
    /// Creates a new `json_agg` UDAF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl Default for JsonAgg {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonAgg {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonAgg {}

impl Hash for JsonAgg {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "json_agg".hash(state);
    }
}

impl AggregateUDFImpl for JsonAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "json_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn state_fields(
        &self,
        _args: datafusion_expr::function::StateFieldsArgs,
    ) -> Result<Vec<Arc<Field>>> {
        // State is a single LargeBinary holding concatenated JSONB values
        Ok(vec![Arc::new(Field::new(
            "json_agg_state",
            DataType::LargeBinary,
            true,
        ))])
    }

    fn accumulator(&self, _args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(JsonAggAccumulator::new()))
    }
}

/// Accumulator for `json_agg`.
///
/// Maintains a `Vec` of JSON values. On `evaluate`, serializes to JSONB array.
#[derive(Debug)]
struct JsonAggAccumulator {
    values: Vec<serde_json::Value>,
}

impl JsonAggAccumulator {
    fn new() -> Self {
        Self { values: Vec::new() }
    }
}

impl Accumulator for JsonAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = &values[0];
        for i in 0..arr.len() {
            if arr.is_null(i) {
                self.values.push(serde_json::Value::Null);
            } else {
                self.values.push(array_value_to_json(arr, i));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let json_arr = serde_json::Value::Array(self.values.clone());
        let bytes = json_types::encode_jsonb(&json_arr);
        Ok(ScalarValue::LargeBinary(Some(bytes)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.values.capacity() * std::mem::size_of::<serde_json::Value>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Serialize current state as a JSONB array
        let json_arr = serde_json::Value::Array(self.values.clone());
        let bytes = json_types::encode_jsonb(&json_arr);
        Ok(vec![ScalarValue::LargeBinary(Some(bytes))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let arr = states[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "json_agg: merge state must be LargeBinary".into(),
                )
            })?;
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                let bytes = arr.value(i);
                // Decode JSONB array and merge elements
                if let Some(json_str) = json_types::jsonb_to_text(bytes) {
                    if let Ok(serde_json::Value::Array(elems)) =
                        serde_json::from_str::<serde_json::Value>(&json_str)
                    {
                        self.values.extend(elems);
                    }
                }
            }
        }
        Ok(())
    }
}

// ══════════════════════════════════════════════════════════════════
// json_object_agg(key, value) -> jsonb
// ══════════════════════════════════════════════════════════════════

/// `json_object_agg(key, value) -> jsonb`
///
/// Collects key-value pairs into a JSON object. Duplicate keys use
/// last-value-wins semantics (consistent with PostgreSQL).
#[derive(Debug)]
pub struct JsonObjectAgg {
    signature: Signature,
}

impl JsonObjectAgg {
    /// Creates a new `json_object_agg` UDAF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl Default for JsonObjectAgg {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonObjectAgg {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonObjectAgg {}

impl Hash for JsonObjectAgg {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "json_object_agg".hash(state);
    }
}

impl AggregateUDFImpl for JsonObjectAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "json_object_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn state_fields(
        &self,
        _args: datafusion_expr::function::StateFieldsArgs,
    ) -> Result<Vec<Arc<Field>>> {
        Ok(vec![Arc::new(Field::new(
            "json_object_agg_state",
            DataType::LargeBinary,
            true,
        ))])
    }

    fn accumulator(&self, _args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(JsonObjectAggAccumulator::new()))
    }
}

/// Accumulator for `json_object_agg`.
///
/// Maintains an ordered map of key-value pairs.
#[derive(Debug)]
struct JsonObjectAggAccumulator {
    entries: serde_json::Map<String, serde_json::Value>,
}

impl JsonObjectAggAccumulator {
    fn new() -> Self {
        Self {
            entries: serde_json::Map::new(),
        }
    }
}

impl Accumulator for JsonObjectAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let key_arr = &values[0];
        let val_arr = &values[1];

        for i in 0..key_arr.len() {
            if key_arr.is_null(i) {
                continue; // Skip null keys (PostgreSQL behavior)
            }
            let key = array_value_to_string(key_arr, i)?;
            let val = if val_arr.is_null(i) {
                serde_json::Value::Null
            } else {
                array_value_to_json(val_arr, i)
            };
            self.entries.insert(key, val); // last-value-wins
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let obj = serde_json::Value::Object(self.entries.clone());
        let bytes = json_types::encode_jsonb(&obj);
        Ok(ScalarValue::LargeBinary(Some(bytes)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.entries.len() * 64 // rough estimate
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let obj = serde_json::Value::Object(self.entries.clone());
        let bytes = json_types::encode_jsonb(&obj);
        Ok(vec![ScalarValue::LargeBinary(Some(bytes))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let arr = states[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "json_object_agg: merge state must be LargeBinary".into(),
                )
            })?;
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                let bytes = arr.value(i);
                if let Some(json_str) = json_types::jsonb_to_text(bytes) {
                    if let Ok(serde_json::Value::Object(map)) =
                        serde_json::from_str::<serde_json::Value>(&json_str)
                    {
                        for (k, v) in map {
                            self.entries.insert(k, v);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

// ── Helpers ──────────────────────────────────────────────────────

/// Convert an Arrow array element to a `serde_json::Value`.
fn array_value_to_json(arr: &ArrayRef, row: usize) -> serde_json::Value {
    if arr.is_null(row) {
        return serde_json::Value::Null;
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return serde_json::Value::String(a.value(row).to_owned());
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow_array::Int64Array>() {
        return serde_json::Value::Number(a.value(row).into());
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow_array::Int32Array>() {
        return serde_json::Value::Number(i64::from(a.value(row)).into());
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow_array::Float64Array>() {
        if let Some(n) = serde_json::Number::from_f64(a.value(row)) {
            return serde_json::Value::Number(n);
        }
        return serde_json::Value::Null;
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow_array::BooleanArray>() {
        return serde_json::Value::Bool(a.value(row));
    }
    // Fallback
    let scalar = ScalarValue::try_from_array(arr, row).ok();
    match scalar {
        Some(s) => serde_json::Value::String(s.to_string()),
        None => serde_json::Value::Null,
    }
}

/// Extract a string key from an Arrow array.
fn array_value_to_string(arr: &ArrayRef, row: usize) -> Result<String> {
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Ok(a.value(row).to_owned());
    }
    // Fallback via ScalarValue display
    let sv = ScalarValue::try_from_array(arr, row)?;
    Ok(sv.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;

    fn make_string_array(vals: &[&str]) -> StringArray {
        StringArray::from(vals.to_vec())
    }

    #[test]
    fn test_json_agg_basic() {
        let mut acc = JsonAggAccumulator::new();
        let vals = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        acc.update_batch(&[vals]).unwrap();
        let result = acc.evaluate().unwrap();
        match result {
            ScalarValue::LargeBinary(Some(bytes)) => {
                assert_eq!(json_types::jsonb_type_name(&bytes), Some("array"));
                let e0 = json_types::jsonb_array_get(&bytes, 0).unwrap();
                assert_eq!(json_types::jsonb_to_text(e0), Some("1".to_owned()));
                let e2 = json_types::jsonb_array_get(&bytes, 2).unwrap();
                assert_eq!(json_types::jsonb_to_text(e2), Some("3".to_owned()));
            }
            other => panic!("Expected LargeBinary, got {other:?}"),
        }
    }

    #[test]
    fn test_json_agg_strings() {
        let mut acc = JsonAggAccumulator::new();
        let vals = Arc::new(make_string_array(&["a", "b", "c"])) as ArrayRef;
        acc.update_batch(&[vals]).unwrap();
        let result = acc.evaluate().unwrap();
        match result {
            ScalarValue::LargeBinary(Some(bytes)) => {
                let e0 = json_types::jsonb_array_get(&bytes, 0).unwrap();
                assert_eq!(json_types::jsonb_to_text(e0), Some("a".to_owned()));
            }
            other => panic!("Expected LargeBinary, got {other:?}"),
        }
    }

    #[test]
    fn test_json_agg_multiple_batches() {
        let mut acc = JsonAggAccumulator::new();
        let v1 = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
        let v2 = Arc::new(Int64Array::from(vec![3])) as ArrayRef;
        acc.update_batch(&[v1]).unwrap();
        acc.update_batch(&[v2]).unwrap();
        let result = acc.evaluate().unwrap();
        match result {
            ScalarValue::LargeBinary(Some(bytes)) => {
                // Should have 3 elements total
                let text = json_types::jsonb_to_text(&bytes).unwrap();
                assert_eq!(text, "[1,2,3]");
            }
            other => panic!("Expected LargeBinary, got {other:?}"),
        }
    }

    #[test]
    fn test_json_object_agg_basic() {
        let mut acc = JsonObjectAggAccumulator::new();
        let keys = Arc::new(make_string_array(&["a", "b", "c"])) as ArrayRef;
        let vals = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        acc.update_batch(&[keys, vals]).unwrap();
        let result = acc.evaluate().unwrap();
        match result {
            ScalarValue::LargeBinary(Some(bytes)) => {
                assert_eq!(json_types::jsonb_type_name(&bytes), Some("object"));
                let a = json_types::jsonb_get_field(&bytes, "a").unwrap();
                assert_eq!(json_types::jsonb_to_text(a), Some("1".to_owned()));
                let c = json_types::jsonb_get_field(&bytes, "c").unwrap();
                assert_eq!(json_types::jsonb_to_text(c), Some("3".to_owned()));
            }
            other => panic!("Expected LargeBinary, got {other:?}"),
        }
    }

    #[test]
    fn test_json_object_agg_last_value_wins() {
        let mut acc = JsonObjectAggAccumulator::new();
        let keys = Arc::new(make_string_array(&["a", "a"])) as ArrayRef;
        let vals = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
        acc.update_batch(&[keys, vals]).unwrap();
        let result = acc.evaluate().unwrap();
        match result {
            ScalarValue::LargeBinary(Some(bytes)) => {
                let a = json_types::jsonb_get_field(&bytes, "a").unwrap();
                assert_eq!(json_types::jsonb_to_text(a), Some("2".to_owned()));
            }
            other => panic!("Expected LargeBinary, got {other:?}"),
        }
    }

    #[test]
    fn test_json_agg_state_merge() {
        let mut acc1 = JsonAggAccumulator::new();
        let v1 = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
        acc1.update_batch(&[v1]).unwrap();
        let state = acc1.state().unwrap();

        let mut acc2 = JsonAggAccumulator::new();
        let v2 = Arc::new(Int64Array::from(vec![3])) as ArrayRef;
        acc2.update_batch(&[v2]).unwrap();

        // Merge state from acc1 into acc2
        let state_arr: ArrayRef = match &state[0] {
            ScalarValue::LargeBinary(Some(b)) => {
                Arc::new(LargeBinaryArray::from_iter_values(vec![b.as_slice()]))
            }
            _ => panic!("expected LargeBinary state"),
        };
        acc2.merge_batch(&[state_arr]).unwrap();

        let result = acc2.evaluate().unwrap();
        match result {
            ScalarValue::LargeBinary(Some(bytes)) => {
                let text = json_types::jsonb_to_text(&bytes).unwrap();
                assert_eq!(text, "[3,1,2]");
            }
            other => panic!("Expected LargeBinary, got {other:?}"),
        }
    }

    #[test]
    fn test_udaf_registration() {
        let json_agg = datafusion_expr::AggregateUDF::new_from_impl(JsonAgg::new());
        assert_eq!(json_agg.name(), "json_agg");

        let json_obj_agg = datafusion_expr::AggregateUDF::new_from_impl(JsonObjectAgg::new());
        assert_eq!(json_obj_agg.name(), "json_object_agg");
    }
}
