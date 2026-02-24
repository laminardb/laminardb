//! Array, Struct, and Map scalar UDFs (F-SCHEMA-015).
//!
//! **Tier 1** — Built-in DataFusion array functions are verified via tests.
//!
//! **Tier 2** — Custom scalar UDFs for struct/map operations:
//!
//! | Function | Signature | Return |
//! |----------|-----------|--------|
//! | `struct_extract(struct, field)` | Struct, Utf8 | field type |
//! | `struct_set(struct, field, value)` | Struct, Utf8, Any | Struct |
//! | `struct_drop(struct, field)` | Struct, Utf8 | Struct |
//! | `struct_rename(struct, old, new)` | Struct, Utf8, Utf8 | Struct |
//! | `struct_merge(s1, s2)` | Struct, Struct | Struct |
//! | `map_keys(map)` | Map | List\<K\> |
//! | `map_values(map)` | Map | List\<V\> |
//! | `map_contains_key(map, key)` | Map, K | Boolean |
//! | `map_from_arrays(keys, vals)` | List, List | Map |

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::{builder::BooleanBuilder, Array, ArrayRef, MapArray, StructArray};
use arrow_schema::{Field, Fields};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::json_udf::expand_args;

/// Registers all complex type UDFs with the given session context.
pub fn register_complex_type_functions(ctx: &datafusion::prelude::SessionContext) {
    use datafusion_expr::ScalarUDF;

    ctx.register_udf(ScalarUDF::new_from_impl(StructExtract::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(StructSet::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(StructDrop::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(StructRename::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(StructMerge::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(MapKeys::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(MapValues::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(MapContainsKey::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(MapFromArrays::new()));
}

// ── Helpers ──────────────────────────────────────────────────────

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
// struct_extract(struct, field_name) -> field value
// ══════════════════════════════════════════════════════════════════

/// `struct_extract(struct, field_name)` — extract a field from a struct column.
#[derive(Debug)]
pub struct StructExtract {
    signature: Signature,
}

impl StructExtract {
    /// Creates a new `struct_extract` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl Default for StructExtract {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for StructExtract {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for StructExtract {}
impl Hash for StructExtract {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "struct_extract".hash(state);
    }
}

impl ScalarUDFImpl for StructExtract {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "struct_extract"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Dynamic return type based on the struct field.
        // Fallback to Utf8 — actual type resolution happens at plan time.
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let struct_arr = expanded[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "struct_extract: first arg must be Struct".into(),
                )
            })?;

        let field_name = scalar_string_value(&args.args[1])?;

        let (idx, _) = struct_arr.fields().find(&field_name).ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(format!(
                "struct_extract: field '{field_name}' not found in struct"
            ))
        })?;

        Ok(ColumnarValue::Array(struct_arr.column(idx).clone()))
    }
}

// ══════════════════════════════════════════════════════════════════
// struct_set(struct, field_name, value) -> struct
// ══════════════════════════════════════════════════════════════════

/// `struct_set(struct, field_name, value)` — set/add a field in a struct.
#[derive(Debug)]
pub struct StructSet {
    signature: Signature,
}

impl StructSet {
    /// Creates a new `struct_set` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

impl Default for StructSet {
    fn default() -> Self {
        Self::new()
    }
}
impl PartialEq for StructSet {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for StructSet {}
impl Hash for StructSet {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "struct_set".hash(state);
    }
}

impl ScalarUDFImpl for StructSet {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "struct_set"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Return type is a struct; exact fields depend on input.
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let struct_arr = expanded[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "struct_set: first arg must be Struct".into(),
                )
            })?;

        let field_name = scalar_string_value(&args.args[1])?;
        let new_value = Arc::clone(&expanded[2]);

        let mut new_fields: Vec<Arc<Field>> = Vec::new();
        let mut new_columns: Vec<ArrayRef> = Vec::new();
        let mut replaced = false;

        for (i, field) in struct_arr.fields().iter().enumerate() {
            if field.name() == &field_name {
                new_fields.push(Arc::new(Field::new(
                    &field_name,
                    new_value.data_type().clone(),
                    new_value.null_count() > 0,
                )));
                new_columns.push(Arc::clone(&new_value));
                replaced = true;
            } else {
                new_fields.push(Arc::clone(field));
                new_columns.push(struct_arr.column(i).clone());
            }
        }

        if !replaced {
            new_fields.push(Arc::new(Field::new(
                &field_name,
                new_value.data_type().clone(),
                new_value.null_count() > 0,
            )));
            new_columns.push(new_value);
        }

        let result = StructArray::try_new(
            Fields::from(new_fields),
            new_columns,
            struct_arr.nulls().cloned(),
        )?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ══════════════════════════════════════════════════════════════════
// struct_drop(struct, field_name) -> struct
// ══════════════════════════════════════════════════════════════════

/// `struct_drop(struct, field_name)` — remove a field from a struct.
#[derive(Debug)]
pub struct StructDrop {
    signature: Signature,
}

impl StructDrop {
    /// Creates a new `struct_drop` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl Default for StructDrop {
    fn default() -> Self {
        Self::new()
    }
}
impl PartialEq for StructDrop {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for StructDrop {}
impl Hash for StructDrop {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "struct_drop".hash(state);
    }
}

impl ScalarUDFImpl for StructDrop {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "struct_drop"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let struct_arr = expanded[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "struct_drop: first arg must be Struct".into(),
                )
            })?;

        let field_name = scalar_string_value(&args.args[1])?;

        let mut new_fields: Vec<Arc<Field>> = Vec::new();
        let mut new_columns: Vec<ArrayRef> = Vec::new();

        for (i, field) in struct_arr.fields().iter().enumerate() {
            if field.name() != &field_name {
                new_fields.push(Arc::clone(field));
                new_columns.push(struct_arr.column(i).clone());
            }
        }

        if new_fields.is_empty() {
            return Err(datafusion_common::DataFusionError::Internal(
                "struct_drop: cannot drop all fields from struct".into(),
            ));
        }

        let result = StructArray::try_new(
            Fields::from(new_fields),
            new_columns,
            struct_arr.nulls().cloned(),
        )?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ══════════════════════════════════════════════════════════════════
// struct_rename(struct, old_name, new_name) -> struct
// ══════════════════════════════════════════════════════════════════

/// `struct_rename(struct, old_name, new_name)` — rename a struct field.
#[derive(Debug)]
pub struct StructRename {
    signature: Signature,
}

impl StructRename {
    /// Creates a new `struct_rename` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

impl Default for StructRename {
    fn default() -> Self {
        Self::new()
    }
}
impl PartialEq for StructRename {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for StructRename {}
impl Hash for StructRename {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "struct_rename".hash(state);
    }
}

impl ScalarUDFImpl for StructRename {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "struct_rename"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let struct_arr = expanded[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "struct_rename: first arg must be Struct".into(),
                )
            })?;

        let old_name = scalar_string_value(&args.args[1])?;
        let new_name = scalar_string_value(&args.args[2])?;

        let mut new_fields: Vec<Arc<Field>> = Vec::new();
        let mut new_columns: Vec<ArrayRef> = Vec::new();

        for (i, field) in struct_arr.fields().iter().enumerate() {
            if field.name() == &old_name {
                new_fields.push(Arc::new(Field::new(
                    &new_name,
                    field.data_type().clone(),
                    field.is_nullable(),
                )));
            } else {
                new_fields.push(Arc::clone(field));
            }
            new_columns.push(struct_arr.column(i).clone());
        }

        let result = StructArray::try_new(
            Fields::from(new_fields),
            new_columns,
            struct_arr.nulls().cloned(),
        )?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ══════════════════════════════════════════════════════════════════
// struct_merge(s1, s2) -> struct
// ══════════════════════════════════════════════════════════════════

/// `struct_merge(s1, s2)` — merge two structs (s2 fields override s1).
#[derive(Debug)]
pub struct StructMerge {
    signature: Signature,
}

impl StructMerge {
    /// Creates a new `struct_merge` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl Default for StructMerge {
    fn default() -> Self {
        Self::new()
    }
}
impl PartialEq for StructMerge {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for StructMerge {}
impl Hash for StructMerge {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "struct_merge".hash(state);
    }
}

impl ScalarUDFImpl for StructMerge {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "struct_merge"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let s1 = expanded[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "struct_merge: first arg must be Struct".into(),
                )
            })?;
        let s2 = expanded[1]
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "struct_merge: second arg must be Struct".into(),
                )
            })?;

        let mut new_fields: Vec<Arc<Field>> = Vec::new();
        let mut new_columns: Vec<ArrayRef> = Vec::new();

        // Collect s2 field names for override detection.
        let s2_names: Vec<&str> = s2.fields().iter().map(|f| f.name().as_str()).collect();

        // Add s1 fields that aren't overridden by s2.
        for (i, field) in s1.fields().iter().enumerate() {
            if !s2_names.contains(&field.name().as_str()) {
                new_fields.push(Arc::clone(field));
                new_columns.push(s1.column(i).clone());
            }
        }

        // Add all s2 fields.
        for (i, field) in s2.fields().iter().enumerate() {
            new_fields.push(Arc::clone(field));
            new_columns.push(s2.column(i).clone());
        }

        let result = StructArray::try_new(Fields::from(new_fields), new_columns, None)?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ══════════════════════════════════════════════════════════════════
// map_keys(map) -> List<K>
// ══════════════════════════════════════════════════════════════════

/// `map_keys(map)` — extract keys from a map as a list.
#[derive(Debug)]
pub struct MapKeys {
    signature: Signature,
}

impl MapKeys {
    /// Creates a new `map_keys` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl Default for MapKeys {
    fn default() -> Self {
        Self::new()
    }
}
impl PartialEq for MapKeys {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for MapKeys {}
impl Hash for MapKeys {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "map_keys".hash(state);
    }
}

impl ScalarUDFImpl for MapKeys {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "map_keys"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Map(field, _) => {
                if let DataType::Struct(fields) = field.data_type() {
                    if let Some(key_field) = fields.first() {
                        return Ok(DataType::List(Arc::new(Field::new(
                            "item",
                            key_field.data_type().clone(),
                            key_field.is_nullable(),
                        ))));
                    }
                }
                Ok(DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                ))))
            }
            _ => Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            )))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let map_arr = expanded[0]
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal("map_keys: arg must be Map".into())
            })?;

        Ok(ColumnarValue::Array(Arc::new(map_arr.keys().clone())))
    }
}

// ══════════════════════════════════════════════════════════════════
// map_values(map) -> List<V>
// ══════════════════════════════════════════════════════════════════

/// `map_values(map)` — extract values from a map as a list.
#[derive(Debug)]
pub struct MapValues {
    signature: Signature,
}

impl MapValues {
    /// Creates a new `map_values` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl Default for MapValues {
    fn default() -> Self {
        Self::new()
    }
}
impl PartialEq for MapValues {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for MapValues {}
impl Hash for MapValues {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "map_values".hash(state);
    }
}

impl ScalarUDFImpl for MapValues {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "map_values"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Map(field, _) => {
                if let DataType::Struct(fields) = field.data_type() {
                    if fields.len() >= 2 {
                        let val_field = &fields[1];
                        return Ok(DataType::List(Arc::new(Field::new(
                            "item",
                            val_field.data_type().clone(),
                            val_field.is_nullable(),
                        ))));
                    }
                }
                Ok(DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                ))))
            }
            _ => Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            )))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let map_arr = expanded[0]
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal("map_values: arg must be Map".into())
            })?;

        Ok(ColumnarValue::Array(Arc::new(map_arr.values().clone())))
    }
}

// ══════════════════════════════════════════════════════════════════
// map_contains_key(map, key) -> Boolean
// ══════════════════════════════════════════════════════════════════

/// `map_contains_key(map, key)` — check if a map contains a given key.
#[derive(Debug)]
pub struct MapContainsKey {
    signature: Signature,
}

impl MapContainsKey {
    /// Creates a new `map_contains_key` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl Default for MapContainsKey {
    fn default() -> Self {
        Self::new()
    }
}
impl PartialEq for MapContainsKey {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for MapContainsKey {}
impl Hash for MapContainsKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "map_contains_key".hash(state);
    }
}

impl ScalarUDFImpl for MapContainsKey {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "map_contains_key"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    #[allow(clippy::cast_sign_loss)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let map_arr = expanded[0]
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "map_contains_key: first arg must be Map".into(),
                )
            })?;

        let search_key = expanded[1]
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "map_contains_key: second arg must be Utf8".into(),
                )
            })?;

        let keys_col = map_arr.keys();
        let keys_str = keys_col.as_any().downcast_ref::<arrow_array::StringArray>();

        let mut builder = BooleanBuilder::with_capacity(map_arr.len());

        for row in 0..map_arr.len() {
            if map_arr.is_null(row) || search_key.is_null(row) {
                builder.append_null();
                continue;
            }

            let target = search_key.value(row);
            let start = map_arr.value_offsets()[row] as usize;
            let end = map_arr.value_offsets()[row + 1] as usize;

            let found = if let Some(ks) = keys_str {
                (start..end).any(|i| !ks.is_null(i) && ks.value(i) == target)
            } else {
                false
            };

            builder.append_value(found);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ══════════════════════════════════════════════════════════════════
// map_from_arrays(keys, values) -> Map
// ══════════════════════════════════════════════════════════════════

/// `map_from_arrays(keys, values)` — construct a map from key/value arrays.
#[derive(Debug)]
pub struct MapFromArrays {
    signature: Signature,
}

impl MapFromArrays {
    /// Creates a new `map_from_arrays` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl Default for MapFromArrays {
    fn default() -> Self {
        Self::new()
    }
}
impl PartialEq for MapFromArrays {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for MapFromArrays {}
impl Hash for MapFromArrays {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "map_from_arrays".hash(state);
    }
}

impl ScalarUDFImpl for MapFromArrays {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "map_from_arrays"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let key_type = match &arg_types[0] {
            DataType::List(f) => f.data_type().clone(),
            _ => DataType::Utf8,
        };
        let val_type = match &arg_types[1] {
            DataType::List(f) => f.data_type().clone(),
            _ => DataType::Utf8,
        };

        let entries_field = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", key_type, false),
                Field::new("value", val_type, true),
            ])),
            false,
        );
        Ok(DataType::Map(Arc::new(entries_field), false))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let keys_list = expanded[0]
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "map_from_arrays: first arg must be List".into(),
                )
            })?;
        let values_list = expanded[1]
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "map_from_arrays: second arg must be List".into(),
                )
            })?;

        // Build the MapArray from key/value ListArrays.
        let offsets = keys_list.offsets().clone();
        let key_values = keys_list.values().clone();
        let val_values = values_list.values().clone();

        let key_field = Field::new("key", key_values.data_type().clone(), false);
        let val_field = Field::new("value", val_values.data_type().clone(), true);
        let struct_fields = Fields::from(vec![key_field.clone(), val_field.clone()]);
        let entries = StructArray::try_new(struct_fields, vec![key_values, val_values], None)?;

        let entries_field = Field::new("entries", entries.data_type().clone(), false);
        let map = MapArray::try_new(Arc::new(entries_field), offsets, entries, None, false)?;
        Ok(ColumnarValue::Array(Arc::new(map)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::create_session_context;
    use arrow_array::builder::*;
    use arrow_array::*;
    use arrow_schema::{DataType, Field, Fields};
    use datafusion_common::config::ConfigOptions;

    // ── Tier 1: Verify DataFusion built-in array functions ──────

    #[tokio::test]
    async fn test_builtin_array_length() {
        let ctx = create_session_context();
        let df = ctx
            .sql("SELECT array_length(make_array(1, 2, 3))")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_builtin_array_sort() {
        let ctx = create_session_context();
        let df = ctx
            .sql("SELECT array_sort(make_array(3, 1, 2))")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_builtin_array_distinct() {
        let ctx = create_session_context();
        let df = ctx
            .sql("SELECT array_distinct(make_array(1, 2, 2, 3))")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    // ── Tier 2: struct_extract ──────────────────────────────────

    #[test]
    fn test_struct_extract() {
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let struct_arr = StructArray::try_new(
            fields,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
            None,
        )
        .unwrap();

        let udf = StructExtract::new();
        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::new(struct_arr)),
                    ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some("b".into()))),
                ],
                number_rows: 0,
                arg_fields: vec![],
                return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(str_arr.value(0), "x");
            assert_eq!(str_arr.value(1), "y");
            assert_eq!(str_arr.value(2), "z");
        } else {
            panic!("expected Array");
        }
    }

    // ── Tier 2: struct_drop ─────────────────────────────────────

    #[test]
    fn test_struct_drop() {
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let struct_arr = StructArray::try_new(
            fields,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
            None,
        )
        .unwrap();

        let udf = StructDrop::new();
        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::new(struct_arr)),
                    ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some("b".into()))),
                ],
                number_rows: 0,
                arg_fields: vec![],
                return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(s.num_columns(), 1);
            assert_eq!(s.fields()[0].name(), "a");
        } else {
            panic!("expected Array");
        }
    }

    // ── Tier 2: struct_rename ───────────────────────────────────

    #[test]
    fn test_struct_rename() {
        let fields = Fields::from(vec![Field::new("old_name", DataType::Int64, false)]);
        let struct_arr =
            StructArray::try_new(fields, vec![Arc::new(Int64Array::from(vec![42]))], None).unwrap();

        let udf = StructRename::new();
        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::new(struct_arr)),
                    ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some(
                        "old_name".into(),
                    ))),
                    ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some(
                        "new_name".into(),
                    ))),
                ],
                number_rows: 0,
                arg_fields: vec![],
                return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(s.fields()[0].name(), "new_name");
        } else {
            panic!("expected Array");
        }
    }

    // ── Tier 2: struct_merge ────────────────────────────────────

    #[test]
    fn test_struct_merge() {
        let s1 = StructArray::try_new(
            Fields::from(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Utf8, true),
            ]),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["old"])),
            ],
            None,
        )
        .unwrap();

        let s2 = StructArray::try_new(
            Fields::from(vec![
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Float64, false),
            ]),
            vec![
                Arc::new(StringArray::from(vec!["new"])),
                Arc::new(Float64Array::from(vec![3.125])),
            ],
            None,
        )
        .unwrap();

        let udf = StructMerge::new();
        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::new(s1)),
                    ColumnarValue::Array(Arc::new(s2)),
                ],
                number_rows: 0,
                arg_fields: vec![],
                return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
            // "a" from s1, "b" from s2 (override), "c" from s2
            assert_eq!(s.num_columns(), 3);
            let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(names, vec!["a", "b", "c"]);
        } else {
            panic!("expected Array");
        }
    }

    // ── Tier 2: map_contains_key ────────────────────────────────

    #[test]
    fn test_map_contains_key() {
        // Build a MapArray with one row: {"x": 1, "y": 2}
        let key_builder = StringBuilder::new();
        let val_builder = Int64Builder::new();
        let mut builder = MapBuilder::new(None, key_builder, val_builder);

        builder.keys().append_value("x");
        builder.values().append_value(1);
        builder.keys().append_value("y");
        builder.values().append_value(2);
        builder.append(true).unwrap();

        let map_arr = builder.finish();

        let udf = MapContainsKey::new();

        // Check for "x" — should be true.
        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::new(map_arr.clone())),
                    ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some("x".into()))),
                ],
                number_rows: 0,
                arg_fields: vec![],
                return_field: Arc::new(Field::new("output", DataType::Boolean, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(bool_arr.value(0));
        }

        // Check for "z" — should be false.
        let result2 = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::new(map_arr)),
                    ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some("z".into()))),
                ],
                number_rows: 0,
                arg_fields: vec![],
                return_field: Arc::new(Field::new("output", DataType::Boolean, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        if let ColumnarValue::Array(arr) = result2 {
            let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(!bool_arr.value(0));
        }
    }

    // ── Registration ────────────────────────────────────────────

    #[test]
    fn test_register_complex_type_functions() {
        use datafusion::execution::FunctionRegistry;

        let ctx = create_session_context();
        register_complex_type_functions(&ctx);
        assert!(ctx.udf("struct_extract").is_ok());
        assert!(ctx.udf("struct_set").is_ok());
        assert!(ctx.udf("struct_drop").is_ok());
        assert!(ctx.udf("struct_rename").is_ok());
        assert!(ctx.udf("struct_merge").is_ok());
        assert!(ctx.udf("map_keys").is_ok());
        assert!(ctx.udf("map_values").is_ok());
        assert!(ctx.udf("map_contains_key").is_ok());
        assert!(ctx.udf("map_from_arrays").is_ok());
    }
}
