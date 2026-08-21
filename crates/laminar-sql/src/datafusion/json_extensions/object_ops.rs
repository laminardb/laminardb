//! LaminarDB JSON extension UDFs (F-SCHEMA-013).
//!
//! Streaming-specific JSON transformation functions that extend beyond
//! PostgreSQL standard:
//!
//! - **Merge**: `jsonb_merge`, `jsonb_deep_merge`
//! - **Cleanup**: `jsonb_strip_nulls`
//! - **Key ops**: `jsonb_rename_keys`, `jsonb_pick`, `jsonb_except`
//! - **Flatten**: `jsonb_flatten`, `jsonb_unflatten`
//! - **Schema**: `json_to_columns`, `json_infer_schema`

use std::any::Any;
#[allow(clippy::disallowed_types)] // cold path: DataFusion integration
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::{
    builder::LargeBinaryBuilder, Array, LargeBinaryArray, ListArray, MapArray, StringArray,
};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::super::json_types;
use super::super::json_udf::expand_args;

/// Maximum recursion depth for deep merge / flatten.
const MAX_DEPTH: usize = 64;

// ══════════════════════════════════════════════════════════════════
// jsonb_merge(jsonb, jsonb) -> jsonb — shallow merge
// ══════════════════════════════════════════════════════════════════

/// `jsonb_merge(jsonb, jsonb) -> jsonb`
///
/// Shallow-merges two JSONB objects. Keys from the second argument
/// overwrite keys in the first. Non-object inputs: returns second arg.
#[derive(Debug)]
pub struct JsonbMerge {
    signature: Signature,
}

impl JsonbMerge {
    /// Creates a new `jsonb_merge` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::LargeBinary]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for JsonbMerge {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbMerge {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbMerge {}

impl Hash for JsonbMerge {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_merge".hash(state);
    }
}

impl ScalarUDFImpl for JsonbMerge {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_merge"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let left_arr = expanded[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_merge: first arg must be LargeBinary".into(),
                )
            })?;
        let right_arr = expanded[1]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_merge: second arg must be LargeBinary".into(),
                )
            })?;

        let mut builder = LargeBinaryBuilder::with_capacity(left_arr.len(), 256);
        for i in 0..left_arr.len() {
            if left_arr.is_null(i) || right_arr.is_null(i) {
                builder.append_null();
            } else {
                let left_val = json_types::jsonb_to_value(left_arr.value(i));
                let right_val = json_types::jsonb_to_value(right_arr.value(i));
                match (left_val, right_val) {
                    (
                        Some(serde_json::Value::Object(mut l)),
                        Some(serde_json::Value::Object(r)),
                    ) => {
                        for (k, v) in r {
                            l.insert(k, v);
                        }
                        builder
                            .append_value(json_types::encode_jsonb(&serde_json::Value::Object(l)));
                    }
                    (_, Some(r)) => {
                        builder.append_value(json_types::encode_jsonb(&r));
                    }
                    _ => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ══════════════════════════════════════════════════════════════════
// jsonb_deep_merge(jsonb, jsonb) -> jsonb — recursive merge
// ══════════════════════════════════════════════════════════════════

/// `jsonb_deep_merge(jsonb, jsonb) -> jsonb`
///
/// Recursively merges two JSONB objects. When both sides have an object
/// at the same key, the merge recurses. Otherwise second wins.
#[derive(Debug)]
pub struct JsonbDeepMerge {
    signature: Signature,
}

impl JsonbDeepMerge {
    /// Creates a new `jsonb_deep_merge` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::LargeBinary]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for JsonbDeepMerge {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbDeepMerge {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbDeepMerge {}

impl Hash for JsonbDeepMerge {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_deep_merge".hash(state);
    }
}

fn deep_merge(
    left: serde_json::Value,
    right: serde_json::Value,
    depth: usize,
) -> std::result::Result<serde_json::Value, String> {
    if depth > MAX_DEPTH {
        return Err("jsonb_deep_merge: max depth exceeded".into());
    }
    match (left, right) {
        (serde_json::Value::Object(mut l), serde_json::Value::Object(r)) => {
            for (k, rv) in r {
                let merged = if let Some(lv) = l.remove(&k) {
                    deep_merge(lv, rv, depth + 1)?
                } else {
                    rv
                };
                l.insert(k, merged);
            }
            Ok(serde_json::Value::Object(l))
        }
        (_, r) => Ok(r),
    }
}

impl ScalarUDFImpl for JsonbDeepMerge {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_deep_merge"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let left_arr = expanded[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_deep_merge: first arg must be LargeBinary".into(),
                )
            })?;
        let right_arr = expanded[1]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_deep_merge: second arg must be LargeBinary".into(),
                )
            })?;

        let mut builder = LargeBinaryBuilder::with_capacity(left_arr.len(), 256);
        for i in 0..left_arr.len() {
            if left_arr.is_null(i) || right_arr.is_null(i) {
                builder.append_null();
            } else {
                let left_val = json_types::jsonb_to_value(left_arr.value(i));
                let right_val = json_types::jsonb_to_value(right_arr.value(i));
                match (left_val, right_val) {
                    (Some(l), Some(r)) => {
                        let merged = deep_merge(l, r, 0)
                            .map_err(datafusion_common::DataFusionError::Execution)?;
                        builder.append_value(json_types::encode_jsonb(&merged));
                    }
                    _ => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ══════════════════════════════════════════════════════════════════
// jsonb_strip_nulls(jsonb) -> jsonb
// ══════════════════════════════════════════════════════════════════

/// `jsonb_strip_nulls(jsonb) -> jsonb`
///
/// Recursively removes null-valued object fields. Array elements
/// are recursed into but null elements are preserved (PostgreSQL semantics).
#[derive(Debug)]
pub struct JsonbStripNulls {
    signature: Signature,
}

impl JsonbStripNulls {
    /// Creates a new `jsonb_strip_nulls` UDF.
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

impl Default for JsonbStripNulls {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbStripNulls {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbStripNulls {}

impl Hash for JsonbStripNulls {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_strip_nulls".hash(state);
    }
}

fn strip_nulls(val: serde_json::Value) -> serde_json::Value {
    match val {
        serde_json::Value::Object(obj) => {
            let filtered: serde_json::Map<String, serde_json::Value> = obj
                .into_iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k, strip_nulls(v)))
                .collect();
            serde_json::Value::Object(filtered)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(strip_nulls).collect())
        }
        other => other,
    }
}

impl ScalarUDFImpl for JsonbStripNulls {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_strip_nulls"
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
                    "jsonb_strip_nulls: arg must be LargeBinary".into(),
                )
            })?;

        let mut builder = LargeBinaryBuilder::with_capacity(jsonb_arr.len(), 256);
        for i in 0..jsonb_arr.len() {
            if jsonb_arr.is_null(i) {
                builder.append_null();
            } else {
                match json_types::jsonb_to_value(jsonb_arr.value(i)) {
                    Some(val) => {
                        builder.append_value(json_types::encode_jsonb(&strip_nulls(val)));
                    }
                    None => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ══════════════════════════════════════════════════════════════════
// jsonb_rename_keys(jsonb, map<text,text>) -> jsonb
// ══════════════════════════════════════════════════════════════════

/// `jsonb_rename_keys(jsonb, map<text,text>) -> jsonb`
///
/// Renames top-level object keys according to the given rename map.
/// Keys not in the map are preserved unchanged.
#[derive(Debug)]
pub struct JsonbRenameKeys {
    signature: Signature,
}

impl JsonbRenameKeys {
    /// Creates a new `jsonb_rename_keys` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::LargeBinary,
                    DataType::Map(
                        Arc::new(arrow_schema::Field::new(
                            "entries",
                            DataType::Struct(
                                vec![
                                    arrow_schema::Field::new("key", DataType::Utf8, false),
                                    arrow_schema::Field::new("value", DataType::Utf8, true),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for JsonbRenameKeys {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbRenameKeys {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbRenameKeys {}

impl Hash for JsonbRenameKeys {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_rename_keys".hash(state);
    }
}

impl ScalarUDFImpl for JsonbRenameKeys {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_rename_keys"
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
                    "jsonb_rename_keys: first arg must be LargeBinary".into(),
                )
            })?;
        let map_arr = expanded[1]
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_rename_keys: second arg must be Map<Utf8,Utf8>".into(),
                )
            })?;

        let mut builder = LargeBinaryBuilder::with_capacity(jsonb_arr.len(), 256);
        for i in 0..jsonb_arr.len() {
            if jsonb_arr.is_null(i) || map_arr.is_null(i) {
                builder.append_null();
            } else {
                let rename_map = extract_string_map(map_arr, i);
                match json_types::jsonb_to_value(jsonb_arr.value(i)) {
                    Some(serde_json::Value::Object(obj)) => {
                        let renamed: serde_json::Map<String, serde_json::Value> = obj
                            .into_iter()
                            .map(|(k, v)| {
                                let new_key = rename_map.get(k.as_str()).cloned().unwrap_or(k);
                                (new_key, v)
                            })
                            .collect();
                        builder.append_value(json_types::encode_jsonb(&serde_json::Value::Object(
                            renamed,
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

/// Extract a `HashMap<String, String>` from a `MapArray` at row `i`.
#[allow(clippy::disallowed_types)] // cold path: DataFusion integration
fn extract_string_map(map_arr: &MapArray, row: usize) -> std::collections::HashMap<String, String> {
    let mut result = std::collections::HashMap::new();
    let entries = map_arr.value(row);
    let struct_arr = entries
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .unwrap();
    let keys = struct_arr
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let vals = struct_arr
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for j in 0..keys.len() {
        if !keys.is_null(j) && !vals.is_null(j) {
            result.insert(keys.value(j).to_owned(), vals.value(j).to_owned());
        }
    }
    result
}

// ══════════════════════════════════════════════════════════════════
// jsonb_pick(jsonb, text[]) -> jsonb
// ══════════════════════════════════════════════════════════════════

/// `jsonb_pick(jsonb, text[]) -> jsonb`
///
/// Returns a new JSONB object containing only the specified keys.
#[derive(Debug)]
pub struct JsonbPick {
    signature: Signature,
}

impl JsonbPick {
    /// Creates a new `jsonb_pick` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::LargeBinary,
                    DataType::List(Arc::new(arrow_schema::Field::new(
                        "item",
                        DataType::Utf8,
                        true,
                    ))),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for JsonbPick {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbPick {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbPick {}

impl Hash for JsonbPick {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_pick".hash(state);
    }
}

impl ScalarUDFImpl for JsonbPick {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_pick"
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
                    "jsonb_pick: first arg must be LargeBinary".into(),
                )
            })?;
        let keys_arr = expanded[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_pick: second arg must be List<Utf8>".into(),
                )
            })?;

        let mut builder = LargeBinaryBuilder::with_capacity(jsonb_arr.len(), 256);
        for i in 0..jsonb_arr.len() {
            if jsonb_arr.is_null(i) || keys_arr.is_null(i) {
                builder.append_null();
            } else {
                let key_set = extract_string_set(keys_arr, i);
                match json_types::jsonb_to_value(jsonb_arr.value(i)) {
                    Some(serde_json::Value::Object(obj)) => {
                        let picked: serde_json::Map<String, serde_json::Value> = obj
                            .into_iter()
                            .filter(|(k, _)| key_set.contains(k.as_str()))
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

/// Extract a `HashSet<String>` from a `ListArray<Utf8>` at row `i`.
fn extract_string_set(list_arr: &ListArray, row: usize) -> HashSet<String> {
    let values = list_arr.value(row);
    let str_arr = values.as_any().downcast_ref::<StringArray>();
    let mut result = HashSet::new();
    if let Some(arr) = str_arr {
        for j in 0..arr.len() {
            if !arr.is_null(j) {
                result.insert(arr.value(j).to_owned());
            }
        }
    }
    result
}

// ══════════════════════════════════════════════════════════════════
// jsonb_except(jsonb, text[]) -> jsonb
// ══════════════════════════════════════════════════════════════════

/// `jsonb_except(jsonb, text[]) -> jsonb`
///
/// Returns a new JSONB object excluding the specified keys.
#[derive(Debug)]
pub struct JsonbExcept {
    signature: Signature,
}

impl JsonbExcept {
    /// Creates a new `jsonb_except` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::LargeBinary,
                    DataType::List(Arc::new(arrow_schema::Field::new(
                        "item",
                        DataType::Utf8,
                        true,
                    ))),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for JsonbExcept {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbExcept {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbExcept {}

impl Hash for JsonbExcept {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_except".hash(state);
    }
}

impl ScalarUDFImpl for JsonbExcept {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_except"
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
                    "jsonb_except: first arg must be LargeBinary".into(),
                )
            })?;
        let keys_arr = expanded[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_except: second arg must be List<Utf8>".into(),
                )
            })?;

        let mut builder = LargeBinaryBuilder::with_capacity(jsonb_arr.len(), 256);
        for i in 0..jsonb_arr.len() {
            if jsonb_arr.is_null(i) || keys_arr.is_null(i) {
                builder.append_null();
            } else {
                let exclude_set = extract_string_set(keys_arr, i);
                match json_types::jsonb_to_value(jsonb_arr.value(i)) {
                    Some(serde_json::Value::Object(obj)) => {
                        let filtered: serde_json::Map<String, serde_json::Value> = obj
                            .into_iter()
                            .filter(|(k, _)| !exclude_set.contains(k.as_str()))
                            .collect();
                        builder.append_value(json_types::encode_jsonb(&serde_json::Value::Object(
                            filtered,
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
