//! JSON type inspection, construction, and scalar conversion UDFs.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::{
    builder::{LargeBinaryBuilder, StringBuilder},
    Array, ArrayRef, BooleanArray, LargeBinaryArray, StringArray,
};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::{expand_args, json_types};

/// Returns the outermost JSON type name by reading its tag byte.
#[derive(Debug)]
pub struct JsonTypeof {
    signature: Signature,
}

impl JsonTypeof {
    /// Creates a new `json_typeof` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for JsonTypeof {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonTypeof {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonTypeof {}

impl Hash for JsonTypeof {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "json_typeof".hash(state);
    }
}

impl ScalarUDFImpl for JsonTypeof {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "json_typeof"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let jsonb_arr = expanded[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "json_typeof: arg must be LargeBinary".into(),
                )
            })?;

        let mut builder = StringBuilder::with_capacity(jsonb_arr.len(), jsonb_arr.len() * 8);
        for i in 0..jsonb_arr.len() {
            if jsonb_arr.is_null(i) {
                builder.append_null();
            } else {
                match json_types::jsonb_type_name(jsonb_arr.value(i)) {
                    Some(name) => builder.append_value(name),
                    None => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ══════════════════════════════════════════════════════════════════
// json_build_object(k1, v1, k2, v2, ...) -> jsonb
// ══════════════════════════════════════════════════════════════════

/// `json_build_object(key1, value1, key2, value2, ...) -> jsonb`
///
/// Constructs a JSONB object from alternating key-value pairs.
/// Executes in Ring 1 (allocates JSONB binary buffer).
#[derive(Debug)]
pub struct JsonBuildObject {
    signature: Signature,
}

impl JsonBuildObject {
    /// Creates a new `json_build_object` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl Default for JsonBuildObject {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonBuildObject {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonBuildObject {}

impl Hash for JsonBuildObject {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "json_build_object".hash(state);
    }
}

impl ScalarUDFImpl for JsonBuildObject {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "json_build_object"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !args.args.len().is_multiple_of(2) {
            return Err(datafusion_common::DataFusionError::Execution(
                "json_build_object requires an even number of arguments".into(),
            ));
        }

        let expanded = expand_args(&args.args)?;
        let len = expanded.first().map_or(1, Array::len);

        let mut builder = LargeBinaryBuilder::with_capacity(len, 256);
        for row in 0..len {
            let mut obj = serde_json::Map::new();
            let mut is_null = false;
            for pair in expanded.chunks(2) {
                let key_arr = &pair[0];
                let val_arr = &pair[1];
                if key_arr.is_null(row) {
                    is_null = true;
                    break;
                }
                let key = scalar_to_json_key(key_arr, row)?;
                let val = scalar_to_json_value(val_arr, row);
                obj.insert(key, val);
            }
            if is_null {
                builder.append_null();
            } else {
                let jsonb = json_types::encode_jsonb(&serde_json::Value::Object(obj));
                builder.append_value(&jsonb);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ══════════════════════════════════════════════════════════════════
// json_build_array(v1, v2, ...) -> jsonb
// ══════════════════════════════════════════════════════════════════

/// `json_build_array(v1, v2, ...) -> jsonb`
///
/// Constructs a JSONB array from the given values.
/// Executes in Ring 1 (allocates JSONB binary buffer).
#[derive(Debug)]
pub struct JsonBuildArray {
    signature: Signature,
}

impl JsonBuildArray {
    /// Creates a new `json_build_array` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl Default for JsonBuildArray {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonBuildArray {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonBuildArray {}

impl Hash for JsonBuildArray {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "json_build_array".hash(state);
    }
}

impl ScalarUDFImpl for JsonBuildArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "json_build_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let len = expanded.first().map_or(1, Array::len);

        let mut builder = LargeBinaryBuilder::with_capacity(len, 256);
        for row in 0..len {
            let mut arr = Vec::with_capacity(expanded.len());
            for col in &expanded {
                arr.push(scalar_to_json_value(col, row));
            }
            let jsonb = json_types::encode_jsonb(&serde_json::Value::Array(arr));
            builder.append_value(&jsonb);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ══════════════════════════════════════════════════════════════════
// to_jsonb(any) -> jsonb
// ══════════════════════════════════════════════════════════════════

/// `to_jsonb(value) -> jsonb`
///
/// Converts any SQL value to JSONB binary format.
#[derive(Debug)]
pub struct ToJsonb {
    signature: Signature,
}

impl ToJsonb {
    /// Creates a new `to_jsonb` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl Default for ToJsonb {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for ToJsonb {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for ToJsonb {}

impl Hash for ToJsonb {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "to_jsonb".hash(state);
    }
}

impl ScalarUDFImpl for ToJsonb {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "to_jsonb"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let arr = &expanded[0];
        let len = arr.len();

        let mut builder = LargeBinaryBuilder::with_capacity(len, 64);
        for row in 0..len {
            if arr.is_null(row) {
                let jsonb = json_types::encode_jsonb(&serde_json::Value::Null);
                builder.append_value(&jsonb);
            } else {
                let val = scalar_to_json_value(arr, row);
                let jsonb = json_types::encode_jsonb(&val);
                builder.append_value(&jsonb);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ── Scalar conversion helpers ────────────────────────────────────

/// Convert an Arrow array value at `row` to a JSON key string.
fn scalar_to_json_key(arr: &ArrayRef, row: usize) -> Result<String> {
    if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
        return Ok(s.value(row).to_owned());
    }
    // Fallback: convert to string representation
    Err(datafusion_common::DataFusionError::Execution(
        "json_build_object keys must be text".into(),
    ))
}

/// Convert an Arrow array value at `row` to a `serde_json::Value`.
fn scalar_to_json_value(arr: &ArrayRef, row: usize) -> serde_json::Value {
    if arr.is_null(row) {
        return serde_json::Value::Null;
    }

    // Try common types
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
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return serde_json::Value::Bool(a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
        // Already JSONB — convert to JSON value for re-encoding
        let bytes = a.value(row);
        if let Some(text) = json_types::jsonb_to_text(bytes) {
            // Try to parse the text as JSON
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
                return val;
            }
            return serde_json::Value::String(text);
        }
        return serde_json::Value::Null;
    }

    // Fallback: use display
    let scalar = datafusion_common::ScalarValue::try_from_array(arr, row).ok();
    match scalar {
        Some(s) => serde_json::Value::String(s.to_string()),
        None => serde_json::Value::Null,
    }
}

// ══════════════════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════════════════
