//! Resolve native lookup readers before planner and cache registration.

use crate::db::{canonical_object_name, LaminarDB};
use crate::error::DbError;
use arrow::datatypes::{Field, Schema};
use laminar_core::schema_binding::{SchemaBinding, SchemaDirection, SchemaOrigin};
use laminar_sql::parser::lookup_table::{
    validate_properties, CreateLookupTableStatement, LookupConnector,
};
use std::sync::Arc;

pub(super) async fn resolve(
    db: &LaminarDB,
    create: &CreateLookupTableStatement,
) -> Result<Option<SchemaBinding>, DbError> {
    if create.or_replace {
        return Err(DbError::InvalidOperation(
            "CREATE OR REPLACE LOOKUP TABLE is not atomic; use DROP/CREATE".into(),
        ));
    }
    let properties = validate_properties(&create.with_options)
        .map_err(|error| DbError::Config(error.to_string()))?;
    db.preflight_lookup_connector(&properties)?;
    let LookupConnector::External(connector) = &properties.connector else {
        return Ok(None);
    };
    if create.primary_key.len() != 1 {
        return Err(DbError::Config(
            "lookup schema resolution requires the separately declared single-column primary key"
                .into(),
        ));
    }
    let (mut options, mut format_options, format) = connector_options(&create.with_options);
    db.resolve_connector_option_values(options.values_mut().chain(format_options.values_mut()))?;
    let config = crate::connector_manager::build_table_config(
        &crate::connector_manager::TableRegistration {
            catalog_generation: 1,
            name: canonical_object_name(&create.name)?,
            schema_binding: None,
            primary_key: create.primary_key[0].clone(),
            connector_type: Some(connector.clone()),
            connector_options: options,
            format,
            format_options,
            on_demand: matches!(
                properties.strategy,
                laminar_sql::parser::lookup_table::LookupStrategy::OnDemand
            ),
            cache_max_bytes: None,
            cache_ttl: None,
        },
    )?;
    let explicit = if create.columns.is_empty() {
        None
    } else {
        Some(Arc::new(Schema::new(
            create
                .columns
                .iter()
                .map(|column| {
                    let data_type = laminar_sql::translator::streaming_ddl::sql_type_to_arrow(
                        &column.data_type,
                    )
                    .map_err(|error| DbError::Config(error.to_string()))?;
                    let nullable = !column.options.iter().any(|option| {
                        matches!(option.option, sqlparser::ast::ColumnOption::NotNull)
                    });
                    Ok(Field::new(&column.name.value, data_type, nullable))
                })
                .collect::<Result<Vec<_>, DbError>>()?,
        )))
    };
    let binding = match super::schema_resolution::supplied_binding() {
        Some(binding) => {
            if binding.connector != config.connector_type()
                || binding.direction != SchemaDirection::Source
                || explicit
                    .as_ref()
                    .is_some_and(|schema| schema.fields() != binding.logical.fields())
            {
                return Err(DbError::Checkpoint(
                    "lookup reader differs from its committed definition".into(),
                ));
            }
            binding
        }
        None if super::schema_resolution::legacy_schema_replay_active() => {
            let explicit = explicit.ok_or_else(|| {
                DbError::Checkpoint(
                    "legacy unresolved lookup table requires a controlled migration".into(),
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
                .resolve_lookup_schema(&config, explicit)
                .await?
        }
    };
    let key = binding
        .logical
        .field_with_name(&create.primary_key[0])
        .map_err(|_| DbError::Config("lookup primary key is absent from resolved fields".into()))?;
    if key.is_nullable() {
        return Err(DbError::Config(
            "lookup primary key must be declared or resolved NOT NULL".into(),
        ));
    }
    Ok(Some(binding))
}

pub(crate) fn connector_options(
    raw: &std::collections::HashMap<String, String>,
) -> (
    std::collections::HashMap<String, String>,
    std::collections::HashMap<String, String>,
    Option<String>,
) {
    let mut options = std::collections::HashMap::new();
    let mut format_options = std::collections::HashMap::new();
    let mut format = None;
    for (key, value) in raw {
        let lower = key.to_ascii_lowercase();
        match lower.as_str() {
            "connector" | "strategy" | "cache.memory" | "cache.ttl" | "pushdown" => {}
            "format" => format = Some(value.clone()),
            _ => {
                if let Some(suffix) = lower.strip_prefix("format.") {
                    format_options.insert(suffix.into(), value.clone());
                } else {
                    options.insert(key.clone(), value.clone());
                }
            }
        }
    }
    (options, format_options, format)
}

impl LaminarDB {
    pub(crate) fn register_lookup_connector(
        &self,
        info: &laminar_sql::planner::LookupTableInfo,
        pk: &str,
    ) -> Result<(), DbError> {
        use laminar_sql::parser::lookup_table::LookupConnector;

        let connector_type = match &info.properties.connector {
            LookupConnector::External(name) => name.clone(),
            LookupConnector::Static => {
                return Err(DbError::Config(
                    "static lookup has no external connector".into(),
                ))
            }
        };

        let (mut connector_options, mut format_options, format) =
            crate::ddl::lookup_schema::connector_options(&info.raw_options);
        self.resolve_connector_option_values(
            connector_options
                .values_mut()
                .chain(format_options.values_mut()),
        )?;
        self.table_store
            .write()
            .set_connector(&info.name, &connector_type);

        // Carry as bytes; the partial lookup cache is byte-weighted, not entry-counted.
        let cache_max_bytes = info
            .properties
            .cache_memory
            .map(|m| usize::try_from(m.as_bytes()).unwrap_or(usize::MAX));

        let cache_ttl = info
            .properties
            .cache_ttl
            .map(std::time::Duration::from_secs);

        self.connector_manager
            .lock()
            .register_table(crate::connector_manager::TableRegistration {
                catalog_generation: 1,
                schema_binding: crate::ddl::schema_resolution::supplied_binding(),
                name: info.name.clone(),
                primary_key: pk.to_string(),
                connector_type: Some(connector_type),
                connector_options,
                format,
                format_options,
                on_demand: matches!(
                    info.properties.strategy,
                    laminar_sql::parser::lookup_table::LookupStrategy::OnDemand
                ),
                cache_max_bytes,
                cache_ttl,
            });
        Ok(())
    }
}
