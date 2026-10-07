use std::collections::BTreeSet;

use arrow_schema::{DataType, Field, Schema};

use super::{
    SchemaBinding, SchemaBindingError, SchemaDirection, SchemaFieldMapping, MAX_SCHEMA_FIELDS,
    MAX_SCHEMA_REFERENCES, SCHEMA_BINDING_VERSION,
};

pub(super) fn validate_schema(schema: &Schema) -> Result<(), SchemaBindingError> {
    if schema.fields().is_empty() || schema.fields().len() > MAX_SCHEMA_FIELDS {
        return Err(SchemaBindingError::Invalid(format!(
            "field count must be in 1..={MAX_SCHEMA_FIELDS}"
        )));
    }
    validate_fields(schema.fields().iter().map(AsRef::as_ref))?;
    let mut pending: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| (field.data_type(), 0))
        .collect();
    let mut fields = 0_usize;
    while let Some((data_type, depth)) = pending.pop() {
        fields += 1;
        if fields > MAX_SCHEMA_FIELDS || depth > 32 {
            return Err(SchemaBindingError::Invalid(
                "nested schema exceeds 4096 fields or depth 32".into(),
            ));
        }
        validate_type_layout(data_type)?;
        match data_type {
            DataType::Null => {
                return Err(SchemaBindingError::Invalid(
                    "nested field has no established type".into(),
                ))
            }
            DataType::Struct(children) => {
                validate_fields(children.iter().map(AsRef::as_ref))?;
                pending.extend(children.iter().map(|field| (field.data_type(), depth + 1)));
            }
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::FixedSizeList(child, _)
            | DataType::ListView(child)
            | DataType::LargeListView(child)
            | DataType::Map(child, _) => pending.push((child.data_type(), depth + 1)),
            DataType::Union(children, _) => {
                validate_fields(children.iter().map(|(_, field)| field.as_ref()))?;
                pending.extend(
                    children
                        .iter()
                        .map(|(_, field)| (field.data_type(), depth + 1)),
                );
            }
            DataType::Dictionary(key, value) => {
                pending.push((key, depth + 1));
                pending.push((value, depth + 1));
            }
            DataType::RunEndEncoded(runs, values) => {
                pending.push((runs.data_type(), depth + 1));
                pending.push((values.data_type(), depth + 1));
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_type_layout(data_type: &DataType) -> Result<(), SchemaBindingError> {
    use arrow_schema::TimeUnit;
    let valid = match data_type {
        DataType::FixedSizeBinary(size) | DataType::FixedSizeList(_, size) => *size >= 0,
        DataType::Time32(unit) => matches!(unit, TimeUnit::Second | TimeUnit::Millisecond),
        DataType::Time64(unit) => matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond),
        DataType::Decimal32(precision, scale) => valid_decimal(*precision, *scale, 9),
        DataType::Decimal64(precision, scale) => valid_decimal(*precision, *scale, 18),
        DataType::Decimal128(precision, scale) => valid_decimal(*precision, *scale, 38),
        DataType::Decimal256(precision, scale) => valid_decimal(*precision, *scale, 76),
        DataType::Dictionary(key, _) => key.is_integer(),
        DataType::Map(entries, _) => {
            !entries.is_nullable()
                && matches!(entries.data_type(), DataType::Struct(fields)
                if fields.len() == 2 && !fields[0].is_nullable())
        }
        DataType::RunEndEncoded(runs, _) => {
            !runs.is_nullable()
                && matches!(
                    runs.data_type(),
                    DataType::Int16 | DataType::Int32 | DataType::Int64
                )
        }
        DataType::Union(fields, _) => {
            let ids: BTreeSet<_> = fields.iter().map(|(id, _)| id).collect();
            ids.len() == fields.len() && ids.iter().all(|id| *id >= 0)
        }
        _ => true,
    };
    if !valid {
        return Err(SchemaBindingError::Invalid(
            "malformed Arrow type layout".into(),
        ));
    }
    Ok(())
}

fn valid_decimal(precision: u8, scale: i8, maximum: u8) -> bool {
    precision > 0 && precision <= maximum && (scale < 0 || scale.unsigned_abs() <= precision)
}

/// Compare logical types while retaining nested names, order and nullability.
/// Native field metadata (including stable field IDs) remains in the external contract;
/// its presence alone does not change a user's explicitly declared logical type.
#[must_use]
pub fn same_logical_type(left: &DataType, right: &DataType) -> bool {
    if left == right {
        return true;
    }
    if !left.equals_datatype(right) {
        return false;
    }
    let fields_match = |left: &Field, right: &Field| {
        left.name() == right.name() && same_logical_type(left.data_type(), right.data_type())
    };
    match (left, right) {
        (DataType::Struct(left), DataType::Struct(right)) => left
            .iter()
            .zip(right)
            .all(|(left, right)| fields_match(left, right)),
        (DataType::List(left), DataType::List(right))
        | (DataType::LargeList(left), DataType::LargeList(right))
        | (DataType::ListView(left), DataType::ListView(right))
        | (DataType::LargeListView(left), DataType::LargeListView(right))
        | (DataType::FixedSizeList(left, _), DataType::FixedSizeList(right, _))
        | (DataType::Map(left, _), DataType::Map(right, _)) => fields_match(left, right),
        (
            DataType::Dictionary(left_key, left_value),
            DataType::Dictionary(right_key, right_value),
        ) => same_logical_type(left_key, right_key) && same_logical_type(left_value, right_value),
        (DataType::Union(left, _), DataType::Union(right, _)) => {
            left.iter()
                .zip(right.iter())
                .all(|((left_id, left), (right_id, right))| {
                    left_id == right_id && fields_match(left, right)
                })
        }
        (
            DataType::RunEndEncoded(left_runs, left_values),
            DataType::RunEndEncoded(right_runs, right_values),
        ) => fields_match(left_runs, right_runs) && fields_match(left_values, right_values),
        _ => true,
    }
}

pub(super) fn mapping(
    direction: SchemaDirection,
    logical: &Schema,
    external: &Schema,
    controls: &[String],
) -> Result<Vec<SchemaFieldMapping>, SchemaBindingError> {
    if direction == SchemaDirection::Sink
        && logical.fields().len().saturating_sub(controls.len()) != external.fields().len()
    {
        let missing = external
            .fields()
            .iter()
            .find(|field| logical.index_of(field.name()).is_err());
        let extra = logical.fields().iter().find(|field| {
            !controls.contains(field.name()) && external.index_of(field.name()).is_err()
        });
        return Err(SchemaBindingError::Incompatible(format!(
            "sink must map every input and writer field (missing writer field: {:?}; extra input field: {:?}); use a matching query projection; defaults and omissions require a supported destination policy",
            missing.map(|field| field.name()), extra.map(|field| field.name())
        )));
    }
    logical
        .fields()
        .iter()
        .filter(|field| !controls.contains(field.name()))
        .map(|field| {
            let target = external.field_with_name(field.name()).map_err(|_| {
                SchemaBindingError::Incompatible(format!(
                    "field '{}' is absent from the external contract",
                    field.name()
                ))
            })?;
            let unsafe_nullability = match direction {
                SchemaDirection::Source => target.is_nullable() && !field.is_nullable(),
                SchemaDirection::Sink => field.is_nullable() && !target.is_nullable(),
            };
            if !same_logical_type(field.data_type(), target.data_type()) || unsafe_nullability {
                return Err(SchemaBindingError::Incompatible(format!(
                    "field '{}' differs in type or directional nullability",
                    field.name()
                )));
            }
            Ok(SchemaFieldMapping {
                logical: field.name().clone(),
                external: target.name().clone(),
            })
        })
        .collect()
}

pub(super) fn validate(binding: &SchemaBinding) -> Result<(), SchemaBindingError> {
    if binding.version != SCHEMA_BINDING_VERSION || binding.connector.is_empty() {
        return Err(SchemaBindingError::Invalid(
            "unsupported version or missing connector".into(),
        ));
    }
    validate_schema(&binding.logical)?;
    if !binding.control_fields.is_empty()
        && (binding.direction != SchemaDirection::Sink
            || binding.control_fields.iter().any(|name| {
                !matches!(name.as_str(), "__weight" | "_op" | "_ts_ms")
                    || binding.logical.index_of(name).is_err()
            })
            || binding.control_fields.iter().collect::<BTreeSet<_>>().len()
                != binding.control_fields.len())
    {
        return Err(SchemaBindingError::Invalid(
            "invalid sink changelog field declaration".into(),
        ));
    }
    let external = binding.external.as_ref().unwrap_or(&binding.logical);
    validate_schema(external)?;
    if mapping(
        binding.direction,
        &binding.logical,
        external,
        &binding.control_fields,
    )? != binding.mapping
    {
        return Err(SchemaBindingError::Invalid(
            "mapping differs from the declared fields".into(),
        ));
    }
    for native in binding.value.iter().chain(binding.key.iter()) {
        validate_native_value(&native.definition)?;
        let mut reference_names = BTreeSet::new();
        for reference in &native.references {
            if reference.name.is_empty()
                || reference.identity.is_empty()
                || !reference_names.insert(&reference.name)
            {
                return Err(SchemaBindingError::Invalid(
                    "incomplete or duplicate native reference".into(),
                ));
            }
            validate_native_value(&reference.definition)?;
        }
        if native.format.is_empty()
            || native.identity.is_empty()
            || native.definition.is_null()
            || native.references.len() > MAX_SCHEMA_REFERENCES
        {
            return Err(SchemaBindingError::Invalid(
                "incomplete native identity/definition or too many references".into(),
            ));
        }
    }
    Ok(())
}

fn validate_fields<'a>(fields: impl Iterator<Item = &'a Field>) -> Result<(), SchemaBindingError> {
    let mut names = BTreeSet::new();
    for field in fields {
        if field.name().is_empty() || !names.insert(field.name()) {
            return Err(SchemaBindingError::Invalid(
                "empty or duplicate field name".into(),
            ));
        }
    }
    Ok(())
}

fn validate_native_value(value: &serde_json::Value) -> Result<(), SchemaBindingError> {
    let mut pending = vec![(value, 0)];
    let mut nodes = 0_usize;
    while let Some((value, depth)) = pending.pop() {
        nodes += 1;
        if depth > 64 || nodes > 65_536 {
            return Err(SchemaBindingError::Invalid(
                "native schema exceeds depth 64 or 65536 nodes".into(),
            ));
        }
        match value {
            serde_json::Value::Array(items) => {
                pending.extend(items.iter().map(|value| (value, depth + 1)));
            }
            serde_json::Value::Object(items) => {
                pending.extend(items.values().map(|value| (value, depth + 1)));
            }
            _ => {}
        }
    }
    Ok(())
}
