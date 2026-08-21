//! JSONB existence and containment predicates.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::{Array, BooleanArray, LargeBinaryArray, ListArray, StringArray};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::{expand_args, json_types};

/// Tests whether a JSONB object contains a key.
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
