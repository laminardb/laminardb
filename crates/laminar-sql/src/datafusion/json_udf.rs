//! PostgreSQL-compatible JSON scalar UDFs (F-SCHEMA-011).
//!
//! Implements:
//!
//! - **Extraction**: `jsonb_get`, `jsonb_get_idx`, `jsonb_get_text`,
//!   `jsonb_get_text_idx`, `jsonb_get_path`, `jsonb_get_path_text`
//! - **Existence**: `jsonb_exists`, `jsonb_exists_any`, `jsonb_exists_all`
//! - **Containment**: `jsonb_contains`, `jsonb_contained_by`
//! - **Interrogation**: `json_typeof`
//! - **Construction**: `json_build_object`, `json_build_array`, `to_jsonb`

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::{
    builder::{LargeBinaryBuilder, StringBuilder},
    Array, ArrayRef, BooleanArray, LargeBinaryArray, ListArray, StringArray,
};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::json_types;

// ── Helpers ──────────────────────────────────────────────────────

/// Determine the output length from args (handling scalar/array combos).
fn output_len(args: &[ColumnarValue]) -> usize {
    for a in args {
        if let ColumnarValue::Array(arr) = a {
            return arr.len();
        }
    }
    1
}

/// Expand all args to arrays of the same length.
///
/// # Errors
///
/// Returns a `DataFusionError` if a scalar value cannot be expanded to the target length.
pub fn expand_args(args: &[ColumnarValue]) -> Result<Vec<ArrayRef>> {
    let len = output_len(args);
    args.iter()
        .map(|a| match a {
            ColumnarValue::Array(arr) => Ok(Arc::clone(arr)),
            ColumnarValue::Scalar(s) => s.to_array_of_size(len),
        })
        .collect()
}

// ══════════════════════════════════════════════════════════════════
// jsonb_get(jsonb, text) -> jsonb  (SQL: ->)
// ══════════════════════════════════════════════════════════════════

/// `jsonb_get(jsonb, text) -> jsonb`
///
/// Extracts a JSON object field by key, returning JSONB.
/// Maps to the `->` operator with a text key.
#[derive(Debug)]
pub struct JsonbGet {
    signature: Signature,
}

impl JsonbGet {
    /// Creates a new `jsonb_get` UDF.
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

impl Default for JsonbGet {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbGet {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbGet {}

impl Hash for JsonbGet {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_get".hash(state);
    }
}

impl ScalarUDFImpl for JsonbGet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_get"
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
                    "jsonb_get: first arg must be LargeBinary".into(),
                )
            })?;
        let key_arr = expanded[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_get: second arg must be Utf8".into(),
                )
            })?;

        let mut builder = LargeBinaryBuilder::with_capacity(jsonb_arr.len(), 256);
        for i in 0..jsonb_arr.len() {
            if jsonb_arr.is_null(i) || key_arr.is_null(i) {
                builder.append_null();
            } else {
                let jsonb = jsonb_arr.value(i);
                let key = key_arr.value(i);
                match json_types::jsonb_get_field(jsonb, key) {
                    Some(val) => builder.append_value(val),
                    None => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ══════════════════════════════════════════════════════════════════
// jsonb_get_idx(jsonb, int32) -> jsonb  (SQL: -> with int)
// ══════════════════════════════════════════════════════════════════

/// `jsonb_get_idx(jsonb, int32) -> jsonb`
///
/// Extracts a JSON array element by index, returning JSONB.
/// Maps to the `->` operator with an integer index.
#[derive(Debug)]
pub struct JsonbGetIdx {
    signature: Signature,
}

impl JsonbGetIdx {
    /// Creates a new `jsonb_get_idx` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Int32]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for JsonbGetIdx {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbGetIdx {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbGetIdx {}

impl Hash for JsonbGetIdx {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_get_idx".hash(state);
    }
}

impl ScalarUDFImpl for JsonbGetIdx {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_get_idx"
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
                    "jsonb_get_idx: first arg must be LargeBinary".into(),
                )
            })?;
        let idx_arr = expanded[1]
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_get_idx: second arg must be Int32".into(),
                )
            })?;

        let mut builder = LargeBinaryBuilder::with_capacity(jsonb_arr.len(), 256);
        for i in 0..jsonb_arr.len() {
            if jsonb_arr.is_null(i) || idx_arr.is_null(i) {
                builder.append_null();
            } else {
                let jsonb = jsonb_arr.value(i);
                let idx = idx_arr.value(i);
                match usize::try_from(idx)
                    .ok()
                    .and_then(|u| json_types::jsonb_array_get(jsonb, u))
                {
                    Some(val) => builder.append_value(val),
                    None => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ══════════════════════════════════════════════════════════════════
// jsonb_get_text(jsonb, text) -> text  (SQL: ->>)
// ══════════════════════════════════════════════════════════════════

/// `jsonb_get_text(jsonb, text) -> text`
///
/// Extracts a JSON object field by key, returning TEXT.
/// Maps to the `->>` operator with a text key.
#[derive(Debug)]
pub struct JsonbGetText {
    signature: Signature,
}

impl JsonbGetText {
    /// Creates a new `jsonb_get_text` UDF.
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

impl Default for JsonbGetText {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbGetText {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbGetText {}

impl Hash for JsonbGetText {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_get_text".hash(state);
    }
}

impl ScalarUDFImpl for JsonbGetText {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_get_text"
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
                    "jsonb_get_text: first arg must be LargeBinary".into(),
                )
            })?;
        let key_arr = expanded[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_get_text: second arg must be Utf8".into(),
                )
            })?;

        let mut builder = StringBuilder::with_capacity(jsonb_arr.len(), 256);
        for i in 0..jsonb_arr.len() {
            if jsonb_arr.is_null(i) || key_arr.is_null(i) {
                builder.append_null();
            } else {
                let jsonb = jsonb_arr.value(i);
                let key = key_arr.value(i);
                match json_types::jsonb_get_field(jsonb, key) {
                    Some(val) => match json_types::jsonb_to_text(val) {
                        Some(text) => builder.append_value(&text),
                        None => builder.append_null(),
                    },
                    None => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ══════════════════════════════════════════════════════════════════
// jsonb_get_text_idx(jsonb, int32) -> text  (SQL: ->> with int)
// ══════════════════════════════════════════════════════════════════

/// `jsonb_get_text_idx(jsonb, int32) -> text`
///
/// Extracts a JSON array element by index, returning TEXT.
/// Maps to the `->>` operator with an integer index.
#[derive(Debug)]
pub struct JsonbGetTextIdx {
    signature: Signature,
}

impl JsonbGetTextIdx {
    /// Creates a new `jsonb_get_text_idx` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Int32]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for JsonbGetTextIdx {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbGetTextIdx {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbGetTextIdx {}

impl Hash for JsonbGetTextIdx {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_get_text_idx".hash(state);
    }
}

impl ScalarUDFImpl for JsonbGetTextIdx {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_get_text_idx"
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
                    "jsonb_get_text_idx: first arg must be LargeBinary".into(),
                )
            })?;
        let idx_arr = expanded[1]
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_get_text_idx: second arg must be Int32".into(),
                )
            })?;

        let mut builder = StringBuilder::with_capacity(jsonb_arr.len(), 256);
        for i in 0..jsonb_arr.len() {
            if jsonb_arr.is_null(i) || idx_arr.is_null(i) {
                builder.append_null();
            } else {
                let jsonb = jsonb_arr.value(i);
                let idx = idx_arr.value(i);
                match usize::try_from(idx)
                    .ok()
                    .and_then(|u| json_types::jsonb_array_get(jsonb, u))
                {
                    Some(val) => match json_types::jsonb_to_text(val) {
                        Some(text) => builder.append_value(&text),
                        None => builder.append_null(),
                    },
                    None => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ══════════════════════════════════════════════════════════════════
// jsonb_get_path(jsonb, text[]) -> jsonb  (SQL: #>)
// ══════════════════════════════════════════════════════════════════

/// `jsonb_get_path(jsonb, text[]) -> jsonb`
///
/// Extracts a JSONB value at a nested path given as a text array.
/// Maps to the `#>` operator.
#[derive(Debug)]
pub struct JsonbGetPath {
    signature: Signature,
}

impl JsonbGetPath {
    /// Creates a new `jsonb_get_path` UDF.
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

impl Default for JsonbGetPath {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbGetPath {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbGetPath {}

impl Hash for JsonbGetPath {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_get_path".hash(state);
    }
}

impl ScalarUDFImpl for JsonbGetPath {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_get_path"
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
                    "jsonb_get_path: first arg must be LargeBinary".into(),
                )
            })?;
        let path_arr = expanded[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_get_path: second arg must be List<Utf8>".into(),
                )
            })?;

        let mut builder = LargeBinaryBuilder::with_capacity(jsonb_arr.len(), 256);
        for i in 0..jsonb_arr.len() {
            if jsonb_arr.is_null(i) || path_arr.is_null(i) {
                builder.append_null();
            } else {
                let jsonb = jsonb_arr.value(i);
                let path_list = path_arr.value(i);
                let path_strings = path_list
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Internal(
                            "jsonb_get_path: path elements must be Utf8".into(),
                        )
                    })?;
                match walk_path(jsonb, path_strings) {
                    Some(val) => builder.append_value(val),
                    None => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Walk a JSONB value through a sequence of string keys.
fn walk_path<'a>(mut jsonb: &'a [u8], path: &StringArray) -> Option<&'a [u8]> {
    for i in 0..path.len() {
        if path.is_null(i) {
            return None;
        }
        let key = path.value(i);
        // Try object field access first
        if let Some(next) = json_types::jsonb_get_field(jsonb, key) {
            jsonb = next;
        } else if let Ok(idx) = key.parse::<usize>() {
            // Fall back to array index
            jsonb = json_types::jsonb_array_get(jsonb, idx)?;
        } else {
            return None;
        }
    }
    Some(jsonb)
}

// ══════════════════════════════════════════════════════════════════
// jsonb_get_path_text(jsonb, text[]) -> text  (SQL: #>>)
// ══════════════════════════════════════════════════════════════════

/// `jsonb_get_path_text(jsonb, text[]) -> text`
///
/// Extracts a text value at a nested path given as a text array.
/// Maps to the `#>>` operator.
#[derive(Debug)]
pub struct JsonbGetPathText {
    signature: Signature,
}

impl JsonbGetPathText {
    /// Creates a new `jsonb_get_path_text` UDF.
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

impl Default for JsonbGetPathText {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbGetPathText {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbGetPathText {}

impl Hash for JsonbGetPathText {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_get_path_text".hash(state);
    }
}

impl ScalarUDFImpl for JsonbGetPathText {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_get_path_text"
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
                    "jsonb_get_path_text: first arg must be LargeBinary".into(),
                )
            })?;
        let path_arr = expanded[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_get_path_text: second arg must be List<Utf8>".into(),
                )
            })?;

        let mut builder = StringBuilder::with_capacity(jsonb_arr.len(), 256);
        for i in 0..jsonb_arr.len() {
            if jsonb_arr.is_null(i) || path_arr.is_null(i) {
                builder.append_null();
            } else {
                let jsonb = jsonb_arr.value(i);
                let path_list = path_arr.value(i);
                let path_strings = path_list
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Internal(
                            "jsonb_get_path_text: path elements must be Utf8".into(),
                        )
                    })?;
                match walk_path(jsonb, path_strings) {
                    Some(val) => match json_types::jsonb_to_text(val) {
                        Some(text) => builder.append_value(&text),
                        None => builder.append_null(),
                    },
                    None => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ══════════════════════════════════════════════════════════════════
// jsonb_exists(jsonb, text) -> bool  (SQL: ?)
// ══════════════════════════════════════════════════════════════════

/// `jsonb_exists(jsonb, text) -> bool`
///
/// Returns true if the JSONB object contains the given key.
/// Maps to the `?` operator.
#[derive(Debug)]
pub struct JsonbExists {
    signature: Signature,
}

impl JsonbExists {
    /// Creates a new `jsonb_exists` UDF.
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

impl Default for JsonbExists {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbExists {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbExists {}

impl Hash for JsonbExists {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_exists".hash(state);
    }
}

impl ScalarUDFImpl for JsonbExists {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_exists"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let jsonb_arr = expanded[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_exists: first arg must be LargeBinary".into(),
                )
            })?;
        let key_arr = expanded[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_exists: second arg must be Utf8".into(),
                )
            })?;

        let result: BooleanArray = (0..jsonb_arr.len())
            .map(|i| {
                if jsonb_arr.is_null(i) || key_arr.is_null(i) {
                    None
                } else {
                    Some(json_types::jsonb_has_key(
                        jsonb_arr.value(i),
                        key_arr.value(i),
                    ))
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ══════════════════════════════════════════════════════════════════
// jsonb_exists_any(jsonb, text[]) -> bool  (SQL: ?|)
// ══════════════════════════════════════════════════════════════════

/// `jsonb_exists_any(jsonb, text[]) -> bool`
///
/// Returns true if the JSONB object contains any of the given keys.
/// Maps to the `?|` operator.
#[derive(Debug)]
pub struct JsonbExistsAny {
    signature: Signature,
}

impl JsonbExistsAny {
    /// Creates a new `jsonb_exists_any` UDF.
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

impl Default for JsonbExistsAny {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbExistsAny {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbExistsAny {}

impl Hash for JsonbExistsAny {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_exists_any".hash(state);
    }
}

impl ScalarUDFImpl for JsonbExistsAny {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_exists_any"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let jsonb_arr = expanded[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_exists_any: first arg must be LargeBinary".into(),
                )
            })?;
        let keys_arr = expanded[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_exists_any: second arg must be List<Utf8>".into(),
                )
            })?;

        let result: BooleanArray =
            (0..jsonb_arr.len())
                .map(|i| {
                    if jsonb_arr.is_null(i) || keys_arr.is_null(i) {
                        return None;
                    }
                    let jsonb = jsonb_arr.value(i);
                    let keys_list = keys_arr.value(i);
                    let keys = keys_list.as_any().downcast_ref::<StringArray>()?;
                    Some((0..keys.len()).any(|k| {
                        !keys.is_null(k) && json_types::jsonb_has_key(jsonb, keys.value(k))
                    }))
                })
                .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ══════════════════════════════════════════════════════════════════
// jsonb_exists_all(jsonb, text[]) -> bool  (SQL: ?&)
// ══════════════════════════════════════════════════════════════════

/// `jsonb_exists_all(jsonb, text[]) -> bool`
///
/// Returns true if the JSONB object contains all of the given keys.
/// Maps to the `?&` operator.
#[derive(Debug)]
pub struct JsonbExistsAll {
    signature: Signature,
}

impl JsonbExistsAll {
    /// Creates a new `jsonb_exists_all` UDF.
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

impl Default for JsonbExistsAll {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbExistsAll {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbExistsAll {}

impl Hash for JsonbExistsAll {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_exists_all".hash(state);
    }
}

impl ScalarUDFImpl for JsonbExistsAll {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_exists_all"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let jsonb_arr = expanded[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_exists_all: first arg must be LargeBinary".into(),
                )
            })?;
        let keys_arr = expanded[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_exists_all: second arg must be List<Utf8>".into(),
                )
            })?;

        let result: BooleanArray =
            (0..jsonb_arr.len())
                .map(|i| {
                    if jsonb_arr.is_null(i) || keys_arr.is_null(i) {
                        return None;
                    }
                    let jsonb = jsonb_arr.value(i);
                    let keys_list = keys_arr.value(i);
                    let keys = keys_list.as_any().downcast_ref::<StringArray>()?;
                    Some((0..keys.len()).all(|k| {
                        !keys.is_null(k) && json_types::jsonb_has_key(jsonb, keys.value(k))
                    }))
                })
                .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ══════════════════════════════════════════════════════════════════
// jsonb_contains(jsonb, jsonb) -> bool  (SQL: @>)
// ══════════════════════════════════════════════════════════════════

/// `jsonb_contains(jsonb, jsonb) -> bool`
///
/// Returns true if the left JSONB value contains the right.
/// Maps to the `@>` operator.
#[derive(Debug)]
pub struct JsonbContains {
    signature: Signature,
}

impl JsonbContains {
    /// Creates a new `jsonb_contains` UDF.
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

impl Default for JsonbContains {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbContains {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbContains {}

impl Hash for JsonbContains {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_contains".hash(state);
    }
}

impl ScalarUDFImpl for JsonbContains {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let left_arr = expanded[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_contains: first arg must be LargeBinary".into(),
                )
            })?;
        let right_arr = expanded[1]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_contains: second arg must be LargeBinary".into(),
                )
            })?;

        let result: BooleanArray = (0..left_arr.len())
            .map(|i| {
                if left_arr.is_null(i) || right_arr.is_null(i) {
                    None
                } else {
                    json_types::jsonb_contains(left_arr.value(i), right_arr.value(i))
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ══════════════════════════════════════════════════════════════════
// jsonb_contained_by(jsonb, jsonb) -> bool  (SQL: <@)
// ══════════════════════════════════════════════════════════════════

/// `jsonb_contained_by(jsonb, jsonb) -> bool`
///
/// Returns true if the left JSONB value is contained by the right.
/// Maps to the `<@` operator.
#[derive(Debug)]
pub struct JsonbContainedBy {
    signature: Signature,
}

impl JsonbContainedBy {
    /// Creates a new `jsonb_contained_by` UDF.
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

impl Default for JsonbContainedBy {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for JsonbContainedBy {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbContainedBy {}

impl Hash for JsonbContainedBy {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_contained_by".hash(state);
    }
}

impl ScalarUDFImpl for JsonbContainedBy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_contained_by"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let left_arr = expanded[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_contained_by: first arg must be LargeBinary".into(),
                )
            })?;
        let right_arr = expanded[1]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "jsonb_contained_by: second arg must be LargeBinary".into(),
                )
            })?;

        // <@ is just @> with swapped args
        let result: BooleanArray = (0..left_arr.len())
            .map(|i| {
                if left_arr.is_null(i) || right_arr.is_null(i) {
                    None
                } else {
                    json_types::jsonb_contains(right_arr.value(i), left_arr.value(i))
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ══════════════════════════════════════════════════════════════════
// json_typeof(jsonb) -> text
// ══════════════════════════════════════════════════════════════════

/// `json_typeof(jsonb) -> text`
///
/// Returns the type of the outermost JSON value as text:
/// "object", "array", "string", "number", "boolean", or "null".
///
/// Reads only the type tag byte — O(1).
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
    fn as_any(&self) -> &dyn Any {
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
    fn as_any(&self) -> &dyn Any {
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
    fn as_any(&self) -> &dyn Any {
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
    fn as_any(&self) -> &dyn Any {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::ScalarUDF;

    fn enc(v: serde_json::Value) -> Vec<u8> {
        json_types::encode_jsonb(&v)
    }

    fn make_jsonb_array(vals: &[serde_json::Value]) -> LargeBinaryArray {
        let encoded: Vec<Vec<u8>> = vals.iter().map(|v| enc(v.clone())).collect();
        let refs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
        LargeBinaryArray::from_iter_values(refs)
    }

    fn make_string_array(vals: &[&str]) -> StringArray {
        StringArray::from(vals.to_vec())
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
            return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    // ── jsonb_get tests ──────────────────────────────────────

    #[test]
    fn test_jsonb_get_object_field() {
        let udf = JsonbGet::new();
        let jsonb = make_jsonb_array(&[serde_json::json!({"name": "Alice", "age": 30})]);
        let keys = make_string_array(&["name"]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(keys)))
            .unwrap();

        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert!(!bin.is_null(0));
        let val = bin.value(0);
        assert_eq!(json_types::jsonb_to_text(val), Some("Alice".to_owned()));
    }

    #[test]
    fn test_jsonb_get_missing_key() {
        let udf = JsonbGet::new();
        let jsonb = make_jsonb_array(&[serde_json::json!({"a": 1})]);
        let keys = make_string_array(&["missing"]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(keys)))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert!(bin.is_null(0));
    }

    // ── jsonb_get_idx tests ──────────────────────────────────

    #[test]
    fn test_jsonb_get_idx() {
        let udf = JsonbGetIdx::new();
        let jsonb = make_jsonb_array(&[serde_json::json!([10, 20, 30])]);
        let idxs = arrow_array::Int32Array::from(vec![1]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(idxs)))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        let val = bin.value(0);
        assert_eq!(json_types::jsonb_to_text(val), Some("20".to_owned()));
    }

    #[test]
    fn test_jsonb_get_idx_out_of_bounds() {
        let udf = JsonbGetIdx::new();
        let jsonb = make_jsonb_array(&[serde_json::json!([1, 2, 3])]);
        let idxs = arrow_array::Int32Array::from(vec![10]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(idxs)))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert!(bin.is_null(0));
    }

    // ── jsonb_get_text tests ─────────────────────────────────

    #[test]
    fn test_jsonb_get_text_string() {
        let udf = JsonbGetText::new();
        let jsonb = make_jsonb_array(&[serde_json::json!({"name": "Alice"})]);
        let keys = make_string_array(&["name"]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(keys)))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "Alice");
    }

    #[test]
    fn test_jsonb_get_text_number() {
        let udf = JsonbGetText::new();
        let jsonb = make_jsonb_array(&[serde_json::json!({"age": 30})]);
        let keys = make_string_array(&["age"]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(keys)))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "30");
    }

    // ── jsonb_get_text_idx tests ─────────────────────────────

    #[test]
    fn test_jsonb_get_text_idx() {
        let udf = JsonbGetTextIdx::new();
        let jsonb = make_jsonb_array(&[serde_json::json!([10, 20, 30])]);
        let idxs = arrow_array::Int32Array::from(vec![2]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(idxs)))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "30");
    }

    // ── jsonb_exists tests ───────────────────────────────────

    #[test]
    fn test_jsonb_exists_true() {
        let udf = JsonbExists::new();
        let jsonb = make_jsonb_array(&[serde_json::json!({"name": "Alice", "age": 30})]);
        let keys = make_string_array(&["name"]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(keys)))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(bool_arr.value(0), true);
    }

    #[test]
    fn test_jsonb_exists_false() {
        let udf = JsonbExists::new();
        let jsonb = make_jsonb_array(&[serde_json::json!({"name": "Alice"})]);
        let keys = make_string_array(&["missing"]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(keys)))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(bool_arr.value(0), false);
    }

    // ── jsonb_contains tests ─────────────────────────────────

    #[test]
    fn test_jsonb_contains_true() {
        let udf = JsonbContains::new();
        let left = make_jsonb_array(&[serde_json::json!({"a": 1, "b": 2, "c": 3})]);
        let right = make_jsonb_array(&[serde_json::json!({"a": 1, "c": 3})]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(left), Arc::new(right)))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(bool_arr.value(0), true);
    }

    #[test]
    fn test_jsonb_contains_false() {
        let udf = JsonbContains::new();
        let left = make_jsonb_array(&[serde_json::json!({"a": 1})]);
        let right = make_jsonb_array(&[serde_json::json!({"a": 1, "b": 2})]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(left), Arc::new(right)))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(bool_arr.value(0), false);
    }

    // ── jsonb_contained_by tests ─────────────────────────────

    #[test]
    fn test_jsonb_contained_by() {
        let udf = JsonbContainedBy::new();
        let left = make_jsonb_array(&[serde_json::json!({"a": 1})]);
        let right = make_jsonb_array(&[serde_json::json!({"a": 1, "b": 2})]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(left), Arc::new(right)))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(bool_arr.value(0), true);
    }

    // ── json_typeof tests ────────────────────────────────────

    #[test]
    fn test_json_typeof_all_types() {
        let udf = JsonTypeof::new();
        let jsonb = make_jsonb_array(&[
            serde_json::json!({"a": 1}),
            serde_json::json!([1, 2]),
            serde_json::json!("hello"),
            serde_json::json!(42),
            serde_json::json!(true),
            serde_json::json!(null),
        ]);
        let result = udf.invoke_with_args(make_args_1(Arc::new(jsonb))).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "object");
        assert_eq!(str_arr.value(1), "array");
        assert_eq!(str_arr.value(2), "string");
        assert_eq!(str_arr.value(3), "number");
        assert_eq!(str_arr.value(4), "boolean");
        assert_eq!(str_arr.value(5), "null");
    }

    // ── json_build_object tests ──────────────────────────────

    #[test]
    fn test_json_build_object() {
        let udf = JsonBuildObject::new();
        let keys = Arc::new(make_string_array(&["name"])) as ArrayRef;
        let vals = Arc::new(make_string_array(&["Alice"])) as ArrayRef;
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(keys), ColumnarValue::Array(vals)],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new("output", DataType::LargeBinary, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        let val = bin.value(0);
        // Verify we can read back the field
        let name = json_types::jsonb_get_field(val, "name").unwrap();
        assert_eq!(json_types::jsonb_to_text(name), Some("Alice".to_owned()));
    }

    #[test]
    fn test_json_build_object_odd_args() {
        let udf = JsonBuildObject::new();
        let a = Arc::new(make_string_array(&["key"])) as ArrayRef;
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(a)],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new("output", DataType::LargeBinary, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        assert!(udf.invoke_with_args(args).is_err());
    }

    // ── json_build_array tests ───────────────────────────────

    #[test]
    fn test_json_build_array() {
        let udf = JsonBuildArray::new();
        let a = Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef;
        let b = Arc::new(make_string_array(&["two"])) as ArrayRef;
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(a), ColumnarValue::Array(b)],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new("output", DataType::LargeBinary, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        let val = bin.value(0);
        assert_eq!(json_types::jsonb_type_name(val), Some("array"));
        let elem0 = json_types::jsonb_array_get(val, 0).unwrap();
        assert_eq!(json_types::jsonb_to_text(elem0), Some("1".to_owned()));
        let elem1 = json_types::jsonb_array_get(val, 1).unwrap();
        assert_eq!(json_types::jsonb_to_text(elem1), Some("two".to_owned()));
    }

    // ── to_jsonb tests ───────────────────────────────────────

    #[test]
    fn test_to_jsonb_int() {
        let udf = ToJsonb::new();
        let a = Arc::new(arrow_array::Int64Array::from(vec![42])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_1(a)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        let val = bin.value(0);
        assert_eq!(json_types::jsonb_to_text(val), Some("42".to_owned()));
    }

    #[test]
    fn test_to_jsonb_string() {
        let udf = ToJsonb::new();
        let a = Arc::new(make_string_array(&["hello"])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_1(a)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        let val = bin.value(0);
        assert_eq!(json_types::jsonb_to_text(val), Some("hello".to_owned()));
    }

    // ── Registration tests ───────────────────────────────────

    #[test]
    fn test_all_udfs_register() {
        let names = [
            ScalarUDF::new_from_impl(JsonbGet::new()).name().to_owned(),
            ScalarUDF::new_from_impl(JsonbGetIdx::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(JsonbGetText::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(JsonbGetTextIdx::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(JsonbExists::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(JsonbContains::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(JsonbContainedBy::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(JsonTypeof::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(JsonBuildObject::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(JsonBuildArray::new())
                .name()
                .to_owned(),
            ScalarUDF::new_from_impl(ToJsonb::new()).name().to_owned(),
        ];
        for name in &names {
            assert!(!name.is_empty(), "UDF has empty name");
        }
        assert_eq!(names.len(), 11);
    }

    // ── Nested extraction tests ──────────────────────────────

    #[test]
    fn test_nested_extraction() {
        // payload -> 'user' -> 'address' ->> 'city'
        let data = serde_json::json!({
            "user": {"address": {"city": "London"}}
        });
        let jsonb_bytes = enc(data);

        // First: get 'user'
        let user = json_types::jsonb_get_field(&jsonb_bytes, "user").unwrap();
        // Then: get 'address'
        let addr = json_types::jsonb_get_field(user, "address").unwrap();
        // Then: get_text 'city'
        let city = json_types::jsonb_to_text(json_types::jsonb_get_field(addr, "city").unwrap());
        assert_eq!(city, Some("London".to_owned()));
    }

    // ── Multiple rows tests ──────────────────────────────────

    #[test]
    fn test_jsonb_get_multiple_rows() {
        let udf = JsonbGet::new();
        let jsonb = make_jsonb_array(&[
            serde_json::json!({"name": "Alice"}),
            serde_json::json!({"name": "Bob"}),
            serde_json::json!({"age": 30}),
        ]);
        let keys = make_string_array(&["name", "name", "name"]);
        let result = udf
            .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(keys)))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert!(!bin.is_null(0)); // Alice
        assert!(!bin.is_null(1)); // Bob
        assert!(bin.is_null(2)); // no "name" field
    }
}
