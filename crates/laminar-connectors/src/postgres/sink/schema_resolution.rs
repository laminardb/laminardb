//! Native PostgreSQL write selection. COPY/INSERT use names, and PostgreSQL applies omitted defaults.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::postgres::schema_metadata::{read_table, Column};
use arrow_schema::{Schema, SchemaRef};

use super::{build_pool, build_user_schema, validate_sink_schema, PostgresSink};
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::postgres::sink_config::{PostgresSinkConfig, WriteMode};
use crate::schema::resolution::{
    bind_external, logical_binding, NativeSchema, SchemaBinding, SchemaDirection, SchemaOrigin,
};

pub(super) async fn resolve(
    config: &ConnectorConfig,
    parsed: &PostgresSinkConfig,
    input: SchemaRef,
) -> Result<SchemaBinding, ConnectorError> {
    validate_sink_schema(&input, parsed)?;
    let business = if parsed.changelog_mode {
        build_user_schema(&input)
    } else {
        Arc::clone(&input)
    };
    let mut binding = logical_binding(config, SchemaDirection::Sink, SchemaOrigin::Query, &input)?;
    binding.control_fields = input
        .fields()
        .iter()
        .filter(|field| business.index_of(field.name()).is_err())
        .map(|field| field.name().clone())
        .collect();
    bind_external(&mut binding, &business)?;
    let pool = build_pool(parsed)?;
    let client = pool.get().await.map_err(|_| {
        ConnectorError::ConnectionFailed("PostgreSQL metadata connection unavailable".into())
    })?;
    let Some((relation_oid, database_oid, columns)) =
        read_table(&**client, &parsed.schema_name, &parsed.table_name).await?
    else {
        if parsed.auto_create_table {
            return Ok(binding);
        }
        return Err(ConnectorError::ConfigurationError("PostgreSQL target is absent; create it separately or explicitly enable auto.create.table=true".into()));
    };
    let selected = validate_columns(&business, &columns)?;
    if parsed.write_mode == WriteMode::Upsert {
        let mut keys = parsed.primary_key_columns.clone();
        keys.sort();
        let valid: bool = client.query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid=$1::bigint::oid AND i.indisunique AND i.indisvalid AND i.indpred IS NULL AND i.indexprs IS NULL AND (SELECT array_agg(a.attname::text ORDER BY a.attname) FROM unnest(i.indkey) WITH ORDINALITY k(attnum, ord) JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum WHERE k.ord<=i.indnkeyatts)=$2::text[])",
            &[&relation_oid, &keys]).await.map_err(|_| ConnectorError::SchemaMismatch("PostgreSQL key constraints cannot be validated".into()))?
            .try_get(0).map_err(metadata_error)?;
        if !valid {
            return Err(ConnectorError::SchemaMismatch(
                "upsert keys do not identify an existing non-partial unique constraint".into(),
            ));
        }
    }
    bind_external(&mut binding, &selected)?;
    binding.value = Some(NativeSchema {
        format: "postgresql".into(),
        identity: BTreeMap::from([
            ("database_oid".into(), database_oid.to_string()),
            ("relation_oid".into(), relation_oid.to_string()),
            ("schema".into(), parsed.schema_name.clone()),
            ("table".into(), parsed.table_name.clone()),
        ]),
        definition: serde_json::json!({"columns": columns, "write_columns": business.fields().iter().map(|field| field.name()).collect::<Vec<_>>(), "key_columns": parsed.primary_key_columns}),
        references: Vec::new(),
    });
    Ok(binding)
}

fn validate_columns(input: &SchemaRef, columns: &[Column]) -> Result<SchemaRef, ConnectorError> {
    let names: BTreeSet<_> = input
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    let mut selected = Vec::with_capacity(input.fields().len());
    for field in input.fields() {
        let target = columns
            .iter()
            .find(|column| column.name == *field.name())
            .ok_or_else(|| {
                ConnectorError::SchemaMismatch(format!(
                    "query field '{}' has no exact PostgreSQL column name",
                    field.name()
                ))
            })?;
        let encoding = crate::postgres::types::postgres_type(field.data_type())?;
        if encoding.sql() != target.type_name
            || !target.generated.is_empty()
            || target.identity == "a"
            || (target.required && field.is_nullable())
            || (matches!(target.type_name.as_str(), "timestamp" | "timestamptz")
                && !matches!(target.type_modifier, -1 | 6))
        {
            return Err(ConnectorError::SchemaMismatch(format!("PostgreSQL column '{}' differs in encoding, nullability, precision, or generated-column policy", field.name())));
        }
        selected.push(Arc::new(
            field.as_ref().clone().with_nullable(!target.required),
        ));
    }
    for column in columns {
        if !names.contains(column.name.as_str())
            && column.required
            && column.default.is_none()
            && column.generated.is_empty()
            && column.identity.is_empty()
        {
            return Err(ConnectorError::SchemaMismatch(format!(
                "required PostgreSQL column '{}' has no query field or database default",
                column.name
            )));
        }
    }
    Ok(Arc::new(Schema::new_with_metadata(
        selected,
        input.metadata().clone(),
    )))
}

pub(super) async fn prepare(
    sink: &mut PostgresSink,
    config: &ConnectorConfig,
    binding: &mut SchemaBinding,
) -> Result<(), ConnectorError> {
    if binding.value.is_some() {
        return Ok(());
    }
    if !sink.config.auto_create_table {
        return Err(ConnectorError::ConfigurationError(
            "PostgreSQL table creation is disabled".into(),
        ));
    }
    sink.prepare_statements()?;
    let sql = sink.create_table_sql.as_ref().ok_or_else(|| {
        ConnectorError::ConfigurationError("PostgreSQL create statement is missing".into())
    })?;
    let pool = build_pool(&sink.config)?;
    let client = pool.get().await.map_err(|_| {
        ConnectorError::ConnectionFailed("PostgreSQL preparation connection unavailable".into())
    })?;
    client.batch_execute(sql).await.map_err(|_| {
        ConnectorError::WriteError("authorized PostgreSQL table creation failed".into())
    })?;
    *binding = resolve(config, &sink.config, Arc::new(binding.logical.clone())).await?;
    Ok(())
}

pub(super) fn verify_binding(
    expected: Option<&SchemaBinding>,
    current: &SchemaBinding,
) -> Result<(), ConnectorError> {
    let Some(expected) = expected else {
        return Ok(());
    };
    if expected.value.as_ref().map(|native| &native.identity)
        != current.value.as_ref().map(|native| &native.identity)
    {
        return Err(ConnectorError::SchemaMismatch(
            "PostgreSQL target was replaced; migrate the committed binding".into(),
        ));
    }
    Ok(())
}

fn metadata_error(_: tokio_postgres::Error) -> ConnectorError {
    ConnectorError::SchemaMismatch("PostgreSQL catalog returned malformed metadata".into())
}

/// Hold PostgreSQL's DDL exclusion lock through the same transaction as the write.
pub(super) async fn lock_target(
    transaction: &tokio_postgres::Transaction<'_>,
    parsed: &PostgresSinkConfig,
    binding: Option<&SchemaBinding>,
) -> Result<(), ConnectorError> {
    let expected = binding
        .and_then(|binding| binding.value.as_ref())
        .ok_or_else(|| {
            ConnectorError::SchemaMismatch(
                "PostgreSQL write has no activated native target contract".into(),
            )
        })?;
    transaction
        .batch_execute(&format!(
            "LOCK TABLE {} IN ROW EXCLUSIVE MODE",
            parsed.qualified_table_name()
        ))
        .await
        .map_err(|_| {
            ConnectorError::SchemaMismatch(
                "PostgreSQL target cannot be locked for write validation".into(),
            )
        })?;
    let (relation, database, columns) =
        read_table(transaction, &parsed.schema_name, &parsed.table_name)
            .await?
            .ok_or_else(|| {
                ConnectorError::SchemaMismatch("PostgreSQL target disappeared before write".into())
            })?;
    let columns = serde_json::to_value(columns).map_err(|_| {
        ConnectorError::SchemaMismatch("PostgreSQL target metadata cannot be compared".into())
    })?;
    if expected.identity.get("relation_oid") != Some(&relation.to_string())
        || expected.identity.get("database_oid") != Some(&database.to_string())
        || expected.definition.get("columns") != Some(&columns)
    {
        return Err(ConnectorError::SchemaMismatch("PostgreSQL target identity or column layout changed before write; migrate the catalog binding".into()));
    }
    Ok(())
}
