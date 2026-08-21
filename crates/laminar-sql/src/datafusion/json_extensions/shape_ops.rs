//! JSON flattening, reconstruction, column extraction, and schema inference.

use std::any::Any;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::{
    builder::{LargeBinaryBuilder, StringBuilder},
    Array, LargeBinaryArray, StringArray,
};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::super::json_types;
use super::super::json_udf::expand_args;

const MAX_DEPTH: usize = 64;

// ══════════════════════════════════════════════════════════════════
// jsonb_flatten(jsonb, text) -> jsonb
// ══════════════════════════════════════════════════════════════════

/// `jsonb_flatten(jsonb, separator) -> jsonb`
///
/// Flattens a nested JSONB object into a single-level object with
/// dot-path keys. Arrays are indexed numerically (e.g. `tags.0`).
#[derive(Debug)]
pub struct JsonbFlatten {
    signature: Signature,
}

impl JsonbFlatten {
    /// Creates a new `jsonb_flatten` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for JsonbFlatten {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbFlatten {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbFlatten {}

impl Hash for JsonbFlatten {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_flatten".hash(state);
    }
}

fn flatten_value(
    val: &serde_json::Value,
    prefix: &str,
    sep: &str,
    out: &mut serde_json::Map<String, serde_json::Value>,
    depth: usize,
) -> std::result::Result<(), String> {
    if depth > MAX_DEPTH {
        return Err("jsonb_flatten: max depth exceeded".into());
    }
    match val {
        serde_json::Value::Object(obj) => {
            for (k, v) in obj {
                let new_key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}{sep}{k}")
                };
                flatten_value(v, &new_key, sep, out, depth + 1)?;
            }
        }
        serde_json::Value::Array(arr) => {
            for (idx, v) in arr.iter().enumerate() {
                let new_key = if prefix.is_empty() {
                    idx.to_string()
                } else {
                    format!("{prefix}{sep}{idx}")
                };
                flatten_value(v, &new_key, sep, out, depth + 1)?;
            }
        }
        _ => {
            out.insert(prefix.to_owned(), val.clone());
        }
    }
    Ok(())
}

impl ScalarUDFImpl for JsonbFlatten {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_flatten"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let jsonb_arr = expanded[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_flatten: first arg must be LargeBinary".into(),
                )
            })?;
        let sep_arr = expanded[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_flatten: second arg must be Utf8".into(),
                )
            })?;

        let mut builder = LargeBinaryBuilder::with_capacity(jsonb_arr.len(), 256);
        for i in 0..jsonb_arr.len() {
            if jsonb_arr.is_null(i) || sep_arr.is_null(i) {
                builder.append_null();
            } else {
                let sep = sep_arr.value(i);
                match json_types::jsonb_to_value(jsonb_arr.value(i)) {
                    Some(val) => {
                        let mut flat = serde_json::Map::new();
                        flatten_value(&val, "", sep, &mut flat, 0)
                            .map_err(datafusion_common::DataFusionError::Execution)?;
                        builder.append_value(json_types::encode_jsonb(&serde_json::Value::Object(
                            flat,
                        )));
                    }
                    None => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ══════════════════════════════════════════════════════════════════
// jsonb_unflatten(jsonb, text) -> jsonb
// ══════════════════════════════════════════════════════════════════

/// `jsonb_unflatten(jsonb, separator) -> jsonb`
///
/// Rebuilds a nested JSONB object from a flat key-value structure.
/// Keys are split by separator and nested accordingly. Numeric keys
/// stay as object keys (not converted to arrays).
#[derive(Debug)]
pub struct JsonbUnflatten {
    signature: Signature,
}

impl JsonbUnflatten {
    /// Creates a new `jsonb_unflatten` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for JsonbUnflatten {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbUnflatten {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbUnflatten {}

impl Hash for JsonbUnflatten {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_unflatten".hash(state);
    }
}

fn unflatten_insert(root: &mut serde_json::Value, parts: &[&str], value: serde_json::Value) {
    if parts.is_empty() {
        return;
    }
    if parts.len() == 1 {
        if let serde_json::Value::Object(obj) = root {
            obj.insert(parts[0].to_owned(), value);
        }
        return;
    }
    if let serde_json::Value::Object(obj) = root {
        let child = obj
            .entry(parts[0].to_owned())
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        unflatten_insert(child, &parts[1..], value);
    }
}

impl ScalarUDFImpl for JsonbUnflatten {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_unflatten"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let jsonb_arr = expanded[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_unflatten: first arg must be LargeBinary".into(),
                )
            })?;
        let sep_arr = expanded[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_unflatten: second arg must be Utf8".into(),
                )
            })?;

        let mut builder = LargeBinaryBuilder::with_capacity(jsonb_arr.len(), 256);
        for i in 0..jsonb_arr.len() {
            if jsonb_arr.is_null(i) || sep_arr.is_null(i) {
                builder.append_null();
            } else {
                let sep = sep_arr.value(i);
                match json_types::jsonb_to_value(jsonb_arr.value(i)) {
                    Some(serde_json::Value::Object(flat)) => {
                        let mut root = serde_json::Value::Object(serde_json::Map::new());
                        for (key, val) in flat {
                            let parts: Vec<&str> = key.split(sep).collect();
                            unflatten_insert(&mut root, &parts, val);
                        }
                        builder.append_value(json_types::encode_jsonb(&root));
                    }
                    Some(other) => {
                        builder.append_value(json_types::encode_jsonb(&other));
                    }
                    None => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ══════════════════════════════════════════════════════════════════
// json_to_columns(jsonb, text) -> jsonb  (runtime fallback)
// ══════════════════════════════════════════════════════════════════

/// `json_to_columns(jsonb, type_spec) -> jsonb`
///
/// Runtime fallback for structured extraction. Parses the type_spec
/// to determine field names, extracts each from the JSONB object,
/// and returns the result as a new JSONB object containing only
/// those fields. Full plan-time struct rewriting is deferred.
#[derive(Debug)]
pub struct JsonToColumns {
    signature: Signature,
}

impl JsonToColumns {
    /// Creates a new `json_to_columns` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for JsonToColumns {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonToColumns {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonToColumns {}

impl Hash for JsonToColumns {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "json_to_columns".hash(state);
    }
}

/// Parse a type_spec like `"name VARCHAR, age BIGINT, active BOOLEAN"`
/// into a list of field names.
fn parse_type_spec_fields(spec: &str) -> Vec<String> {
    spec.split(',')
        .filter_map(|part| {
            let trimmed = part.trim();
            trimmed.split_whitespace().next().map(ToOwned::to_owned)
        })
        .collect()
}

impl ScalarUDFImpl for JsonToColumns {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "json_to_columns"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let jsonb_arr = expanded[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "json_to_columns: first arg must be LargeBinary".into(),
                )
            })?;
        let spec_arr = expanded[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "json_to_columns: second arg must be Utf8".into(),
                )
            })?;

        let mut builder = LargeBinaryBuilder::with_capacity(jsonb_arr.len(), 256);
        for i in 0..jsonb_arr.len() {
            if jsonb_arr.is_null(i) || spec_arr.is_null(i) {
                builder.append_null();
            } else {
                let fields = parse_type_spec_fields(spec_arr.value(i));
                let field_set: HashSet<&str> = fields.iter().map(String::as_str).collect();
                match json_types::jsonb_to_value(jsonb_arr.value(i)) {
                    Some(serde_json::Value::Object(obj)) => {
                        let picked: serde_json::Map<String, serde_json::Value> = obj
                            .into_iter()
                            .filter(|(k, _)| field_set.contains(k.as_str()))
                            .collect();
                        builder.append_value(json_types::encode_jsonb(&serde_json::Value::Object(
                            picked,
                        )));
                    }
                    Some(other) => {
                        builder.append_value(json_types::encode_jsonb(&other));
                    }
                    None => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ══════════════════════════════════════════════════════════════════
// json_infer_schema(jsonb) -> text
// ══════════════════════════════════════════════════════════════════

/// `json_infer_schema(jsonb) -> text`
///
/// Infers the SQL schema of a JSONB value, returning a JSON object
/// mapping field names to SQL type names.
#[derive(Debug)]
pub struct JsonInferSchema {
    signature: Signature,
}

impl JsonInferSchema {
    /// Creates a new `json_infer_schema` UDF.
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

impl Default for JsonInferSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonInferSchema {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonInferSchema {}

impl Hash for JsonInferSchema {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "json_infer_schema".hash(state);
    }
}

fn infer_type(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "NULL".to_owned(),
        serde_json::Value::Bool(_) => "BOOLEAN".to_owned(),
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "BIGINT".to_owned()
            } else {
                "DOUBLE".to_owned()
            }
        }
        serde_json::Value::String(_) => "VARCHAR".to_owned(),
        serde_json::Value::Array(arr) => {
            let inner = arr.first().map_or("NULL".to_owned(), infer_type);
            format!("ARRAY<{inner}>")
        }
        serde_json::Value::Object(obj) => {
            let fields: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("{k} {}", infer_type(v)))
                .collect();
            format!("STRUCT({})", fields.join(", "))
        }
    }
}

impl ScalarUDFImpl for JsonInferSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "json_infer_schema"
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
                    "json_infer_schema: arg must be LargeBinary".into(),
                )
            })?;

        let mut builder = StringBuilder::with_capacity(jsonb_arr.len(), 256);
        for i in 0..jsonb_arr.len() {
            if jsonb_arr.is_null(i) {
                builder.append_null();
            } else {
                match json_types::jsonb_to_value(jsonb_arr.value(i)) {
                    Some(serde_json::Value::Object(obj)) => {
                        let schema: serde_json::Map<String, serde_json::Value> = obj
                            .iter()
                            .map(|(k, v)| (k.clone(), serde_json::Value::String(infer_type(v))))
                            .collect();
                        builder.append_value(
                            serde_json::to_string(&serde_json::Value::Object(schema))
                                .unwrap_or_default(),
                        );
                    }
                    Some(val) => {
                        builder.append_value(infer_type(&val));
                    }
                    None => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}
