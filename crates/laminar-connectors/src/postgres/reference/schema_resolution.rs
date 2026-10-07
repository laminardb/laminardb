//! Relation metadata for a finite PostgreSQL snapshot; no cursor or rows are read.

use arrow_schema::{Field, Schema, SchemaRef};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::schema::resolution::{
    bind_external, logical_binding, NativeSchema, SchemaBinding, SchemaDirection, SchemaOrigin,
};

pub(super) async fn resolve(
    client: &tokio_postgres::Client,
    config: &ConnectorConfig,
    explicit: Option<SchemaRef>,
) -> Result<SchemaBinding, ConnectorError> {
    let table = config.require("table")?;
    let parts = table.split('.').collect::<Vec<_>>();
    let (namespace, name) = match parts.as_slice() {
        [name] => ("public", *name),
        [namespace, name] => (*namespace, *name),
        _ => {
            return Err(ConnectorError::ConfigurationError(
                "PostgreSQL reference table must be table or schema.table".into(),
            ))
        }
    };
    let (relation, database, columns) =
        crate::postgres::schema_metadata::read_table(client, namespace, name)
            .await?
            .ok_or_else(|| {
                ConnectorError::SchemaMismatch(
                    "PostgreSQL reference relation is absent or invisible".into(),
                )
            })?;
    let fields = columns.iter().filter(|column| explicit.as_ref().is_none_or(|schema| schema.index_of(&column.name).is_ok())).map(|column| {
        let data_type = tokio_postgres::types::Type::from_oid(column.type_oid)
            .as_ref().and_then(super::postgres_type_to_arrow)
            .or_else(|| explicit.as_ref().and_then(|schema| schema.field_with_name(&column.name).ok())
                .filter(|field| field.data_type() == &arrow_schema::DataType::Utf8).map(|field| field.data_type().clone()))
            .ok_or_else(|| ConnectorError::FeatureUnsupported(format!(
                "PostgreSQL column '{}' type '{}' needs an explicit supported projection (UTF8 uses the existing text cast)", column.name, column.type_name)))?;
        Ok(Field::new(&column.name, data_type, !column.required))
    }).collect::<Result<Vec<_>, ConnectorError>>()?;
    let external = Arc::new(Schema::new(fields));
    let origin = if explicit.is_some() {
        SchemaOrigin::Explicit
    } else {
        SchemaOrigin::Metadata
    };
    let logical = explicit.unwrap_or_else(|| Arc::clone(&external));
    let mut binding = logical_binding(config, SchemaDirection::Source, origin, &logical)?;
    bind_external(&mut binding, &external)?;
    binding.value = Some(NativeSchema {
        format: "postgresql-reference".into(),
        identity: BTreeMap::from([
            ("relation_oid".into(), relation.to_string()),
            ("database_oid".into(), database.to_string()),
            ("schema".into(), namespace.into()),
            ("table".into(), name.into()),
        ]),
        definition: serde_json::json!({"columns":columns}),
        references: Vec::new(),
    });
    Ok(binding)
}
