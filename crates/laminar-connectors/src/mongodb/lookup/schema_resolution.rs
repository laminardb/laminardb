//! Flat, closed validators can establish lookup fields without reading documents.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use mongodb::bson::{Bson, Document};

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::schema::resolution::{
    bind_external, logical_binding, SchemaBinding, SchemaDirection, SchemaOrigin,
};

pub(in crate::mongodb) async fn resolve(
    config: &ConnectorConfig,
    explicit: Option<SchemaRef>,
) -> Result<SchemaBinding, ConnectorError> {
    let client = super::super::schema_metadata::client(config.require("connection.uri")?).await?;
    let database = client.database(config.require("database")?);
    let metadata = super::super::schema_metadata::collection(
        &database,
        config.require("collection")?,
        &super::super::CollectionKind::Standard,
    )
    .await?
    .ok_or_else(|| {
        ConnectorError::ConfigurationError(
            "MongoDB lookup collection is missing; create it separately".into(),
        )
    })?;
    let origin = if explicit.is_some() {
        SchemaOrigin::Explicit
    } else {
        SchemaOrigin::Metadata
    };
    let schema = reader_schema(&metadata.validator, explicit)?;
    let mut binding = logical_binding(config, SchemaDirection::Source, origin, &schema)?;
    bind_external(&mut binding, &schema)?;
    binding.value = Some(metadata.native);
    binding
        .canonical_bytes()
        .map_err(crate::schema::resolution::binding_error)?;
    Ok(binding)
}

fn reader_schema(
    validator: &Document,
    explicit: Option<SchemaRef>,
) -> Result<SchemaRef, ConnectorError> {
    let Some(schema) = validator.get_document("$jsonSchema").ok().filter(|schema| {
        validator.len() == 1
            && schema.keys().all(|key| {
                matches!(
                    key.as_str(),
                    "bsonType"
                        | "properties"
                        | "required"
                        | "additionalProperties"
                        | "description"
                        | "title"
                )
            })
            && !schema
                .get_str("bsonType")
                .is_ok_and(|kind| kind != "object")
    }) else {
        return explicit.map_or_else(|| Err(unsupported()), validate_reader);
    };
    let properties = schema
        .get_document("properties")
        .map_err(|_| unsupported())?;
    let required = schema.get_array("required").ok();
    let is_required = |name: &str| {
        required.is_some_and(|names| names.iter().any(|value| value.as_str() == Some(name)))
    };
    if let Some(explicit) = explicit {
        validate_reader(explicit.clone())?;
        for field in explicit.fields() {
            let Some(property) = properties.get(field.name()) else {
                if schema.get_bool("additionalProperties") == Ok(false) {
                    return Err(unsupported());
                }
                continue;
            };
            let (kind, nullable) = property_type(property, is_required(field.name()))?;
            let same_type = kind == *field.data_type()
                || (kind == DataType::Utf8 && field.data_type() == &DataType::LargeUtf8);
            if !same_type || (nullable && !field.is_nullable()) {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "MongoDB validator cannot establish the declared type/nullability for '{}'",
                    field.name()
                )));
            }
        }
        return Ok(explicit);
    }
    if schema.get_bool("additionalProperties") != Ok(false) {
        return Err(unsupported());
    }
    let mut properties = properties.iter().collect::<Vec<_>>();
    properties.sort_unstable_by_key(|(left, _)| *left);
    let fields = properties
        .into_iter()
        .map(|(name, property)| {
            let (kind, nullable) = property_type(property, is_required(name))?;
            Ok(Field::new(name, kind, nullable))
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    validate_reader(Arc::new(Schema::new(fields)))
}

fn property_type(property: &Bson, required: bool) -> Result<(DataType, bool), ConnectorError> {
    let property = property.as_document().ok_or_else(unsupported)?;
    if property
        .keys()
        .any(|key| !matches!(key.as_str(), "bsonType" | "description" | "title"))
    {
        return Err(unsupported());
    }
    let types = match property.get("bsonType") {
        Some(Bson::String(kind)) => vec![kind.as_str()],
        Some(Bson::Array(kinds)) => kinds
            .iter()
            .map(|value| value.as_str().ok_or_else(unsupported))
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err(unsupported()),
    };
    let kinds = types
        .iter()
        .filter(|kind| **kind != "null")
        .collect::<Vec<_>>();
    if kinds.len() != 1 {
        return Err(unsupported());
    }
    let kind = match *kinds[0] {
        "int" => DataType::Int32,
        "long" => DataType::Int64,
        "double" => DataType::Float64,
        "bool" => DataType::Boolean,
        "string" => DataType::Utf8,
        _ => return Err(unsupported()),
    };
    Ok((kind, !required || types.contains(&"null")))
}

fn validate_reader(schema: SchemaRef) -> Result<SchemaRef, ConnectorError> {
    for field in schema.fields() {
        if !matches!(
            field.data_type(),
            DataType::Int32
                | DataType::Int64
                | DataType::Float64
                | DataType::Boolean
                | DataType::Utf8
                | DataType::LargeUtf8
        ) {
            return Err(unsupported());
        }
    }
    logical_binding(
        &ConnectorConfig::new("mongodb"),
        SchemaDirection::Source,
        SchemaOrigin::Explicit,
        &schema,
    )?;
    Ok(schema)
}

fn unsupported() -> ConnectorError {
    ConnectorError::FeatureUnsupported("MongoDB lookup metadata discovery needs a flat closed $jsonSchema with one supported bsonType per field; ObjectId, nested/heterogeneous fields and complex predicates need an explicit supported projection. Keys remain separately declared and index-validated".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;

    #[test]
    fn closed_validator_derives_sorted_fields_and_optional_nullability() {
        let validator = doc! {"$jsonSchema": {"bsonType":"object", "additionalProperties":false, "required":["id"], "properties":{"label":{"bsonType":"string"},"id":{"bsonType":"long"}}}};
        let schema = reader_schema(&validator, None).unwrap();
        assert_eq!(schema.field(0), &Field::new("id", DataType::Int64, false));
        assert_eq!(schema.field(1), &Field::new("label", DataType::Utf8, true));
        let explicit = Arc::new(Schema::new(vec![Field::new(
            "label",
            DataType::Utf8,
            false,
        )]));
        assert!(reader_schema(&validator, Some(explicit)).is_err());
    }

    #[test]
    fn open_and_complex_validators_need_explicit_supported_projection() {
        let explicit = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        assert!(reader_schema(&Document::new(), None).is_err());
        assert!(reader_schema(&doc! {"$expr":{"$gt":["$id",0]}}, None).is_err());
        assert_eq!(
            reader_schema(&Document::new(), Some(explicit.clone())).unwrap(),
            explicit
        );
        assert!(reader_schema(&doc! {"$jsonSchema":{"additionalProperties":false,"properties":{"id":{"bsonType":["long","string"]}}}}, None).is_err());
    }
}
