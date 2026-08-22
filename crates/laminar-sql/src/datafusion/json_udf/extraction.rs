//! JSONB field, index, and path extraction UDFs.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::{
    builder::{LargeBinaryBuilder, StringBuilder},
    Array, LargeBinaryArray, ListArray, StringArray,
};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::{expand_args, json_types};

/// Extracts a JSON object field by key, returning JSONB.
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
    fn as_any(&self) -> &dyn std::any::Any {
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
    fn as_any(&self) -> &dyn std::any::Any {
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
    fn as_any(&self) -> &dyn std::any::Any {
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
    fn as_any(&self) -> &dyn std::any::Any {
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
    fn as_any(&self) -> &dyn std::any::Any {
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
    fn as_any(&self) -> &dyn std::any::Any {
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
