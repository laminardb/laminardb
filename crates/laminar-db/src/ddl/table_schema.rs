//! Reference-table schema resolution preserves the user's separately declared key.

use arrow::datatypes::{Schema, SchemaRef};
use laminar_core::schema_binding::{SchemaBinding, SchemaDirection, SchemaOrigin};
use sqlparser::ast::{CreateTable, CreateTableOptions};
use std::sync::Arc;

use super::table::{
    build_table_fields_and_primary_key, collect_primary_key_constraints, CreateTableWith,
};
use crate::db::{canonical_object_name, LaminarDB};
use crate::error::DbError;

pub(super) fn options(db: &LaminarDB, create: &CreateTable) -> Result<CreateTableWith, DbError> {
    super::table::validate_create_table_envelope(create)?;
    let options = match &create.table_options {
        CreateTableOptions::With(options) => options.as_slice(),
        CreateTableOptions::None => &[],
        _ => {
            return Err(DbError::InvalidOperation(
                "unsupported CREATE TABLE options".into(),
            ))
        }
    };
    let mut options = super::table::parse_create_table_with(options)?;
    super::table::validate_create_table_with(&options)?;
    db.resolve_connector_option_values(
        options
            .connector_options
            .values_mut()
            .chain(options.format_options.values_mut()),
    )?;
    Ok(options)
}

pub(super) async fn resolve(
    db: &LaminarDB,
    create: &CreateTable,
    options: &CreateTableWith,
) -> Result<(SchemaRef, String, Option<SchemaBinding>), DbError> {
    let explicit = if create.columns.is_empty() {
        None
    } else {
        let (fields, _) = build_table_fields_and_primary_key(create)?;
        Some(Arc::new(Schema::new(fields)))
    };
    let Some(connector) = &options.connector_type else {
        let (fields, key) = build_table_fields_and_primary_key(create)?;
        return Ok((Arc::new(Schema::new(fields)), key, None));
    };
    let config = crate::connector_manager::build_table_config(
        &crate::connector_manager::TableRegistration {
            catalog_generation: 1,
            name: canonical_object_name(&create.name)?,
            schema_binding: None,
            primary_key: String::new(),
            connector_type: Some(connector.clone()),
            connector_options: options.connector_options.clone(),
            format: options.format.clone(),
            format_options: options.format_options.clone(),
            on_demand: false,
            cache_max_bytes: None,
            cache_ttl: None,
        },
    )?;
    let binding = match super::schema_resolution::supplied_binding() {
        Some(binding) => {
            if binding.connector != config.connector_type()
                || binding.direction != SchemaDirection::Source
            {
                return Err(DbError::Checkpoint(
                    "reference-table schema binding has the wrong connector or direction".into(),
                ));
            }
            if explicit
                .as_ref()
                .is_some_and(|schema| schema.fields() != binding.logical.fields())
            {
                return Err(DbError::Checkpoint(
                    "explicit table columns differ from the committed reader".into(),
                ));
            }
            binding
        }
        None if super::schema_resolution::legacy_schema_replay_active() => {
            let explicit = explicit.ok_or_else(|| {
                DbError::Checkpoint(
                    "legacy unresolved reference table needs a controlled catalog migration".into(),
                )
            })?;
            laminar_connectors::schema::resolution::logical_binding(
                &config,
                SchemaDirection::Source,
                SchemaOrigin::Explicit,
                &explicit,
            )?
        }
        None => {
            db.connector_registry
                .resolve_table_schema(&config, explicit)
                .await?
        }
    };
    binding
        .canonical_bytes()
        .map_err(|error| DbError::Checkpoint(error.to_string()))?;
    let key = if create.columns.is_empty() {
        resolved_primary_key(create, &binding.logical)?
    } else {
        build_table_fields_and_primary_key(create)?.1
    };
    Ok((Arc::new(binding.logical.clone()), key, Some(binding)))
}

fn resolved_primary_key(create: &CreateTable, schema: &Schema) -> Result<String, DbError> {
    let constraints = collect_primary_key_constraints(create)?;
    let [keys] = constraints.as_slice() else {
        return Err(DbError::InvalidOperation(
            "omitted table fields still require exactly one PRIMARY KEY constraint".into(),
        ));
    };
    let [key] = keys.as_slice() else {
        return Err(DbError::Unsupported(
            "composite reference-table keys are unsupported".into(),
        ));
    };
    let name = if key.quote_style.is_some() {
        key.value.clone()
    } else {
        key.value.to_ascii_lowercase()
    };
    let field = schema.field_with_name(&name).map_err(|_| {
        DbError::Config(format!(
            "declared primary key '{name}' is absent from the resolved table"
        ))
    })?;
    if field.is_nullable() {
        return Err(DbError::Config(
            "declared reference-table primary key must have a non-null native field".into(),
        ));
    }
    Ok(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};

    fn create(key: &str) -> CreateTable {
        let Statement::CreateTable(create) = Parser::parse_sql(
            &GenericDialect {},
            &format!("CREATE TABLE t (PRIMARY KEY ({key}))"),
        )
        .unwrap()
        .remove(0) else {
            panic!("expected CREATE TABLE")
        };
        create
    }

    #[test]
    fn native_primary_key_respects_sql_identifier_case_and_nullability() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ID", DataType::Int64, false),
            Field::new("optional", DataType::Int64, true),
        ]);
        assert_eq!(resolved_primary_key(&create("ID"), &schema).unwrap(), "id");
        assert_eq!(
            resolved_primary_key(&create("\"ID\""), &schema).unwrap(),
            "ID"
        );
        assert!(resolved_primary_key(&create("\"Id\""), &schema).is_err());
        assert!(resolved_primary_key(&create("optional"), &schema)
            .unwrap_err()
            .to_string()
            .contains("non-null"));
        assert!(resolved_primary_key(&create("id, \"ID\""), &schema)
            .unwrap_err()
            .to_string()
            .contains("composite"));
    }
}
