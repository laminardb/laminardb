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
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::{
    builder::{LargeBinaryBuilder, StringBuilder},
    Array, LargeBinaryArray, ListArray, MapArray, StringArray,
};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::json_types;
use super::json_udf::expand_args;

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

// ══════════════════════════════════════════════════════════════════
// Registration
// ══════════════════════════════════════════════════════════════════

/// Registers all JSON extension UDFs with the given session context.
pub fn register_json_extensions(ctx: &datafusion::prelude::SessionContext) {
    use datafusion_expr::ScalarUDF;

    ctx.register_udf(ScalarUDF::new_from_impl(JsonbMerge::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbDeepMerge::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbStripNulls::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbRenameKeys::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbPick::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbExcept::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbFlatten::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbUnflatten::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonToColumns::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonInferSchema::new()));
}

// ══════════════════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{MapBuilder, StringBuilder as MapSB};
    use arrow_array::ArrayRef;
    use arrow_schema::Field;
    use datafusion_common::config::ConfigOptions;
    use serde_json::json;

    fn enc(v: serde_json::Value) -> Vec<u8> {
        json_types::encode_jsonb(&v)
    }

    fn make_jsonb_array(vals: &[serde_json::Value]) -> LargeBinaryArray {
        let encoded: Vec<Vec<u8>> = vals.iter().map(|v| enc(v.clone())).collect();
        let refs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
        LargeBinaryArray::from_iter_values(refs)
    }

    fn make_args_2(a: ArrayRef, b: ArrayRef) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(a), ColumnarValue::Array(b)],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new("output", DataType::LargeBinary, true)),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    fn make_args_1(a: ArrayRef) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(a)],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new("output", DataType::LargeBinary, true)),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    fn decode_jsonb_result(result: ColumnarValue, row: usize) -> serde_json::Value {
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert!(!bin.is_null(row), "unexpected null at row {row}");
        json_types::jsonb_to_value(bin.value(row)).expect("invalid jsonb")
    }

    fn decode_text_result(result: ColumnarValue, row: usize) -> String {
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        str_arr.value(row).to_owned()
    }

    // ── jsonb_merge tests ─────────────────────────────────────

    #[test]
    fn test_json_ext_merge_objects() {
        let udf = JsonbMerge::new();
        let left = make_jsonb_array(&[json!({"a": 1, "b": 2})]);
        let right = make_jsonb_array(&[json!({"b": 99, "c": 3})]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(left), Arc::new(right)))
            .unwrap();
        assert_eq!(
            decode_jsonb_result(result, 0),
            json!({"a": 1, "b": 99, "c": 3})
        );
    }

    #[test]
    fn test_json_ext_merge_non_object() {
        let udf = JsonbMerge::new();
        let left = make_jsonb_array(&[json!(42)]);
        let right = make_jsonb_array(&[json!("hello")]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(left), Arc::new(right)))
            .unwrap();
        assert_eq!(decode_jsonb_result(result, 0), json!("hello"));
    }

    // ── jsonb_deep_merge tests ────────────────────────────────

    #[test]
    fn test_json_ext_deep_merge() {
        let udf = JsonbDeepMerge::new();
        let left = make_jsonb_array(&[json!({"a": {"x": 1, "y": 2}, "b": 10})]);
        let right = make_jsonb_array(&[json!({"a": {"y": 99, "z": 3}, "c": 20})]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(left), Arc::new(right)))
            .unwrap();
        assert_eq!(
            decode_jsonb_result(result, 0),
            json!({"a": {"x": 1, "y": 99, "z": 3}, "b": 10, "c": 20})
        );
    }

    #[test]
    fn test_json_ext_deep_merge_non_object_override() {
        let udf = JsonbDeepMerge::new();
        let left = make_jsonb_array(&[json!({"a": {"x": 1}})]);
        let right = make_jsonb_array(&[json!({"a": 42})]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(left), Arc::new(right)))
            .unwrap();
        assert_eq!(decode_jsonb_result(result, 0), json!({"a": 42}));
    }

    // ── jsonb_strip_nulls tests ───────────────────────────────

    #[test]
    fn test_json_ext_strip_nulls() {
        let udf = JsonbStripNulls::new();
        let input = make_jsonb_array(&[json!({"a": 1, "b": null, "c": {"d": null, "e": 2}})]);
        let result = udf.invoke_with_args(make_args_1(Arc::new(input))).unwrap();
        assert_eq!(
            decode_jsonb_result(result, 0),
            json!({"a": 1, "c": {"e": 2}})
        );
    }

    #[test]
    fn test_json_ext_strip_nulls_array_preserved() {
        let udf = JsonbStripNulls::new();
        let input = make_jsonb_array(&[json!({"arr": [1, null, 3]})]);
        let result = udf.invoke_with_args(make_args_1(Arc::new(input))).unwrap();
        assert_eq!(decode_jsonb_result(result, 0), json!({"arr": [1, null, 3]}));
    }

    // ── jsonb_rename_keys tests ───────────────────────────────

    #[test]
    fn test_json_ext_rename_keys() {
        let udf = JsonbRenameKeys::new();
        let input = make_jsonb_array(&[json!({"old_name": 1, "keep": 2})]);

        // Build a MapArray with one row: {"old_name": "new_name"}
        let key_builder = MapSB::new();
        let val_builder = MapSB::new();
        let mut map_builder = MapBuilder::new(None, key_builder, val_builder);
        map_builder.keys().append_value("old_name");
        map_builder.values().append_value("new_name");
        map_builder.append(true).unwrap();
        let map_arr = map_builder.finish();

        let result = udf
            .invoke_with_args(make_args_2(Arc::new(input), Arc::new(map_arr)))
            .unwrap();
        assert_eq!(
            decode_jsonb_result(result, 0),
            json!({"keep": 2, "new_name": 1})
        );
    }

    // ── jsonb_pick tests ──────────────────────────────────────

    #[test]
    fn test_json_ext_pick() {
        let udf = JsonbPick::new();
        let input = make_jsonb_array(&[json!({"a": 1, "b": 2, "c": 3})]);
        let keys = make_string_list(&[&["a", "c"]]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(input), Arc::new(keys)))
            .unwrap();
        assert_eq!(decode_jsonb_result(result, 0), json!({"a": 1, "c": 3}));
    }

    // ── jsonb_except tests ────────────────────────────────────

    #[test]
    fn test_json_ext_except() {
        let udf = JsonbExcept::new();
        let input = make_jsonb_array(&[json!({"a": 1, "b": 2, "c": 3})]);
        let keys = make_string_list(&[&["b"]]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(input), Arc::new(keys)))
            .unwrap();
        assert_eq!(decode_jsonb_result(result, 0), json!({"a": 1, "c": 3}));
    }

    // ── jsonb_flatten tests ───────────────────────────────────

    #[test]
    fn test_json_ext_flatten() {
        let udf = JsonbFlatten::new();
        let input = make_jsonb_array(&[json!({"a": {"b": 1, "c": [2, 3]}})]);
        let sep = StringArray::from(vec!["."]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(input), Arc::new(sep)))
            .unwrap();
        assert_eq!(
            decode_jsonb_result(result, 0),
            json!({"a.b": 1, "a.c.0": 2, "a.c.1": 3})
        );
    }

    #[test]
    fn test_json_ext_flatten_custom_sep() {
        let udf = JsonbFlatten::new();
        let input = make_jsonb_array(&[json!({"x": {"y": 42}})]);
        let sep = StringArray::from(vec!["/"]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(input), Arc::new(sep)))
            .unwrap();
        assert_eq!(decode_jsonb_result(result, 0), json!({"x/y": 42}));
    }

    // ── jsonb_unflatten tests ─────────────────────────────────

    #[test]
    fn test_json_ext_unflatten() {
        let udf = JsonbUnflatten::new();
        let input = make_jsonb_array(&[json!({"a.b": 1, "a.c": 2, "d": 3})]);
        let sep = StringArray::from(vec!["."]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(input), Arc::new(sep)))
            .unwrap();
        assert_eq!(
            decode_jsonb_result(result, 0),
            json!({"a": {"b": 1, "c": 2}, "d": 3})
        );
    }

    // ── json_to_columns tests ─────────────────────────────────

    #[test]
    fn test_json_ext_to_columns() {
        let udf = JsonToColumns::new();
        let input = make_jsonb_array(&[json!({"name": "Alice", "age": 30, "active": true})]);
        let spec = StringArray::from(vec!["name VARCHAR, age BIGINT"]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(input), Arc::new(spec)))
            .unwrap();
        assert_eq!(
            decode_jsonb_result(result, 0),
            json!({"age": 30, "name": "Alice"})
        );
    }

    // ── json_infer_schema tests ───────────────────────────────

    #[test]
    fn test_json_ext_infer_schema_object() {
        let udf = JsonInferSchema::new();
        let input = make_jsonb_array(&[json!({"name": "Alice", "age": 30, "active": true})]);
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input))],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args).unwrap();
        let text = decode_text_result(result, 0);
        let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed["name"], "VARCHAR");
        assert_eq!(parsed["age"], "BIGINT");
        assert_eq!(parsed["active"], "BOOLEAN");
    }

    #[test]
    fn test_json_ext_infer_schema_nested() {
        let udf = JsonInferSchema::new();
        let input = make_jsonb_array(&[json!({"tags": [1, 2], "meta": {"x": 1.5}})]);
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input))],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args).unwrap();
        let text = decode_text_result(result, 0);
        let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed["tags"], "ARRAY<BIGINT>");
        assert_eq!(parsed["meta"], "STRUCT(x DOUBLE)");
    }

    #[test]
    fn test_json_ext_infer_schema_scalar() {
        let udf = JsonInferSchema::new();
        let input = make_jsonb_array(&[json!(42)]);
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input))],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args).unwrap();
        assert_eq!(decode_text_result(result, 0), "BIGINT");
    }

    // ── Registration test ─────────────────────────────────────

    #[test]
    fn test_json_ext_all_udfs_register() {
        use datafusion_expr::ScalarUDF;

        let names: Vec<String> = vec![
            ScalarUDF::new_from_impl(JsonbMerge::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(JsonbDeepMerge::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(JsonbStripNulls::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(JsonbRenameKeys::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(JsonbPick::new()).name().to_owned(),
            ScalarUDF::new_from_impl(JsonbExcept::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(JsonbFlatten::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(JsonbUnflatten::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(JsonToColumns::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(JsonInferSchema::new())
                .name()
                .to_owned(),
        ];
        for name in &names {
            assert!(!name.is_empty(), "UDF has empty name");
        }
        assert_eq!(names.len(), 10);
    }

    // ── Null-handling tests ───────────────────────────────────

    #[test]
    fn test_json_ext_merge_null_input() {
        let udf = JsonbMerge::new();
        let left = LargeBinaryArray::new_null(1);
        let right = make_jsonb_array(&[json!({"a": 1})]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(left), Arc::new(right)))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert!(bin.is_null(0));
    }

    // ── Helper: build ListArray<Utf8> ─────────────────────────

    fn make_string_list(rows: &[&[&str]]) -> ListArray {
        use arrow_array::builder::{ListBuilder, StringBuilder};

        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in rows {
            for &s in *row {
                builder.values().append_value(s);
            }
            builder.append(true);
        }
        builder.finish()
    }
}
