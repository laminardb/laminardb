//! Query-derived BSON writers and bounded collection validator checks.

use arrow_schema::{DataType, SchemaRef};
use mongodb::bson::Document;
use std::sync::Arc;

use super::{MongoDbSink, WriteMode};
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::mongodb::config::MongoDbSinkConfig;
use crate::schema::resolution::{
    bind_external, logical_binding, SchemaBinding, SchemaDirection, SchemaOrigin,
};

impl MongoDbSink {
    pub(super) async fn prepare_target(
        &self,
        config: &ConnectorConfig,
        binding: &mut SchemaBinding,
    ) -> Result<(), ConnectorError> {
        if binding.value.is_some() {
            return Ok(());
        }
        let prepare = async {
            let client =
                crate::mongodb::schema_metadata::client(&self.config.connection_uri).await?;
            let database = client.database(&self.config.database);
            let hello = database
                .run_command(mongodb::bson::doc! {"hello": 1})
                .await
                .map_err(|_| {
                    ConnectorError::ConnectionFailed(
                        "MongoDB target preparation needs metadata authorization".into(),
                    )
                })?;
            if hello.get_i32("maxWireVersion").unwrap_or(0) < super::MONGODB_8_WIRE_VERSION {
                return Err(ConnectorError::FeatureUnsupported(
                    "MongoDB sink preparation requires MongoDB 8.0+".into(),
                ));
            }
            match &self.config.collection_kind {
                crate::mongodb::CollectionKind::TimeSeries(spec) => {
                    self.ensure_timeseries_collection(&database, spec).await?;
                }
                crate::mongodb::CollectionKind::Standard if self.config.auto_create => {
                    if let Err(error) = database.create_collection(&self.config.collection).await {
                        if !super::is_namespace_exists(&error) {
                            return Err(ConnectorError::ConnectionFailed(
                                "authorized MongoDB collection creation failed".into(),
                            ));
                        }
                    }
                    self.validate_standard_collection(&database).await?;
                }
                crate::mongodb::CollectionKind::Standard => {
                    return Err(ConnectorError::ConfigurationError(
                        "MongoDB collection creation requires explicit auto.create=true".into(),
                    ))
                }
            }
            let current = inspect(
                &database,
                config,
                &self.config,
                Arc::new(binding.logical.clone()),
            )
            .await?;
            if current.value.is_none() {
                return Err(ConnectorError::SchemaMismatch(
                    "MongoDB target disappeared during authorized preparation".into(),
                ));
            }
            *binding = current;
            Ok(())
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), prepare)
            .await
            .map_err(|_| ConnectorError::Timeout(10_000))?
    }
}

pub(super) async fn resolve(
    config: &ConnectorConfig,
    parsed: &MongoDbSinkConfig,
    input: SchemaRef,
) -> Result<SchemaBinding, ConnectorError> {
    MongoDbSink::validate_schema(&input, parsed)?;
    let client = crate::mongodb::schema_metadata::client(&parsed.connection_uri).await?;
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        inspect(&client.database(&parsed.database), config, parsed, input),
    )
    .await
    .map_err(|_| ConnectorError::Timeout(10_000))?
}

pub(super) async fn inspect(
    database: &mongodb::Database,
    config: &ConnectorConfig,
    parsed: &MongoDbSinkConfig,
    input: SchemaRef,
) -> Result<SchemaBinding, ConnectorError> {
    let mut binding = logical_binding(config, SchemaDirection::Sink, SchemaOrigin::Query, &input)?;
    let business = if matches!(parsed.write_mode, WriteMode::Upsert { .. }) {
        let indices = input
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name() != "__weight")
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        binding.control_fields = input
            .fields()
            .iter()
            .filter(|field| field.name() == "__weight")
            .map(|field| field.name().clone())
            .collect();
        Arc::new(
            input
                .project(&indices)
                .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))?,
        )
    } else {
        input
    };
    bind_external(&mut binding, &business)?;
    let metadata = crate::mongodb::schema_metadata::collection(
        database,
        &parsed.collection,
        &parsed.collection_kind,
    )
    .await?;
    let Some(metadata) = metadata else {
        if !parsed.auto_create
            && matches!(
                parsed.collection_kind,
                crate::mongodb::CollectionKind::Standard
            )
        {
            return Err(ConnectorError::ConfigurationError(
                "MongoDB sink collection is missing; create it separately or explicitly enable auto.create".into(),
            ));
        }
        return Ok(binding);
    };
    validate_validator(
        &business,
        &metadata.validator,
        matches!(parsed.write_mode, WriteMode::CdcReplay),
    )?;
    binding.value = Some(metadata.native);
    binding
        .canonical_bytes()
        .map_err(crate::schema::resolution::binding_error)?;
    Ok(binding)
}

fn validate_validator(
    input: &SchemaRef,
    validator: &Document,
    opaque: bool,
) -> Result<(), ConnectorError> {
    if validator.is_empty() {
        return Ok(());
    }
    if opaque || validator.len() != 1 {
        return Err(unsupported());
    }
    let schema = validator
        .get_document("$jsonSchema")
        .map_err(|_| unsupported())?;
    if schema.keys().any(|key| {
        !matches!(
            key.as_str(),
            "bsonType"
                | "properties"
                | "required"
                | "additionalProperties"
                | "description"
                | "title"
        )
    }) || schema
        .get_str("bsonType")
        .is_ok_and(|kind| kind != "object")
    {
        return Err(unsupported());
    }
    let properties = schema
        .get_document("properties")
        .cloned()
        .unwrap_or_default();
    if let Ok(required) = schema.get_array("required") {
        for name in required {
            let name = name.as_str().ok_or_else(unsupported)?;
            if name != "_id" && input.index_of(name).is_err() {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "MongoDB validator requires unmapped field '{name}'"
                )));
            }
        }
    }
    if schema.get_bool("additionalProperties") == Ok(false) && !properties.contains_key("_id") {
        return Err(unsupported());
    }
    for field in input.fields() {
        let Some(property) = properties.get(field.name()) else {
            if schema.get_bool("additionalProperties") == Ok(false) {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "MongoDB validator rejects extra query field '{}'",
                    field.name()
                )));
            }
            continue;
        };
        let property = property.as_document().ok_or_else(unsupported)?;
        if property
            .keys()
            .any(|key| !matches!(key.as_str(), "bsonType" | "description" | "title"))
        {
            return Err(unsupported());
        }
        let kind = bson_type(field.data_type()).ok_or_else(unsupported)?;
        let accepts = |expected: &str| {
            property.get("bsonType").is_some_and(|value| {
                value.as_str() == Some(expected)
                    || value.as_array().is_some_and(|values| {
                        values.iter().any(|value| value.as_str() == Some(expected))
                    })
            })
        };
        if !accepts(kind) || (field.is_nullable() && !accepts("null")) {
            return Err(ConnectorError::SchemaMismatch(format!(
                "MongoDB validator type/nullability differs for '{}'",
                field.name()
            )));
        }
    }
    Ok(())
}

fn bson_type(data_type: &DataType) -> Option<&'static str> {
    match data_type {
        DataType::Boolean => Some("bool"),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            Some("int")
        }
        DataType::Int64 | DataType::UInt32 => Some("long"),
        DataType::Float32 | DataType::Float64 => Some("double"),
        DataType::Utf8 | DataType::LargeUtf8 => Some("string"),
        DataType::Timestamp(..) => Some("date"),
        _ => None,
    }
}

fn unsupported() -> ConnectorError {
    ConnectorError::FeatureUnsupported("MongoDB validator cannot be proved by this BSON writer: supported policies are flat bsonType/properties/required/additionalProperties; complex predicates, nested validators and opaque CDC replay require a separately compatible destination".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use mongodb::bson::doc;

    #[test]
    fn validator_honors_required_names_types_and_nullability() {
        let input = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let validator = doc! {"$jsonSchema":{"bsonType":"object","required":["id"],"properties":{"id":{"bsonType":"long"},"label":{"bsonType":["string","null"]}}}};
        validate_validator(&input, &validator, false).unwrap();
        let incompatible = doc! {"$jsonSchema":{"required":["other"]}};
        assert!(validate_validator(&input, &incompatible, false).is_err());
        let incompatible = doc! {"$jsonSchema":{"properties":{"label":{"bsonType":"string"}}}};
        assert!(validate_validator(&input, &incompatible, false).is_err());
        assert!(validate_validator(&input, &validator, true).is_err());
        assert!(validate_validator(&input, &doc! {"$expr":{"$gt":["$id",0]}}, false).is_err());
    }
}
