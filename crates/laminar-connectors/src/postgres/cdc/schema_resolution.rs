//! Declared-schema binding to the captured table. Resolution never creates or advances a slot.

use std::collections::BTreeMap;

use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};

use super::config::{OutputMode, PostgresCdcConfig};
use super::postgres_io::{self, CaptureTable, InspectedSource};
use super::schema::RelationInfo;
use super::typed_rows::{BoundColumn, ColumnSource, RowLayout};
use super::types::{bind_value_kind, ValueKind};
use crate::config::ConnectorConfig;
use crate::connector::ConnectorTaskGuard;
use crate::error::ConnectorError;
use crate::schema::resolution::{
    bind_external, logical_binding, NativeSchema, SchemaBinding, SchemaDirection, SchemaOrigin,
};

const NATIVE_FORMAT: &str = "pgoutput";
const WEIGHT_COLUMN: &str = "__weight";

pub(super) fn declared_primary_key(config: &ConnectorConfig) -> Vec<String> {
    config
        .get("_primary_key_columns")
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(str::to_string)
        .collect()
}

pub(super) fn declared_schema(
    config: &ConnectorConfig,
    explicit: Option<SchemaRef>,
) -> Result<SchemaRef, ConnectorError> {
    explicit.or_else(|| config.arrow_schema()).ok_or_else(|| {
        ConnectorError::ConfigurationError(
            "PostgreSQL CDC requires declared columns and a PRIMARY KEY matching the table".into(),
        )
    })
}

pub(super) async fn resolve(
    config: &ConnectorConfig,
    explicit: Option<SchemaRef>,
    guard: ConnectorTaskGuard,
) -> Result<SchemaBinding, ConnectorError> {
    let parsed = PostgresCdcConfig::from_config(config)?;
    let declared = declared_schema(config, explicit)?;
    let (source, table) = inspect_declared_table(
        &parsed,
        &declared,
        &declared_primary_key(config),
        None,
        guard,
    )
    .await?;

    let mut binding = logical_binding(
        config,
        SchemaDirection::Source,
        SchemaOrigin::Explicit,
        &declared,
    )?;
    bind_external(&mut binding, &declared)?;
    binding.value = Some(NativeSchema {
        format: NATIVE_FORMAT.into(),
        identity: BTreeMap::from([
            (
                "system_identifier".into(),
                source.system_identifier.to_string(),
            ),
            ("database_oid".into(), source.database_oid.to_string()),
            ("publication_oid".into(), source.publication_oid.to_string()),
            ("table_oid".into(), table.relation.relation_id.to_string()),
        ]),
        definition: serde_json::json!({
            "relation": table.relation,
            "output": parsed.output_mode.to_string(),
        }),
        references: Vec::new(),
    });
    Ok(binding)
}

/// Connect, read the live source (with `slot`, if any) and the captured table, and bind the
/// declared columns to the table.
pub(super) async fn inspect_declared_table(
    config: &PostgresCdcConfig,
    declared: &SchemaRef,
    primary_key: &[String],
    slot: Option<&str>,
    guard: ConnectorTaskGuard,
) -> Result<(InspectedSource, CaptureTable), ConnectorError> {
    let connection = postgres_io::connect(config, "laminar", guard).await?;
    let inspected = async {
        let source = postgres_io::inspect_source(connection.client(), config, slot).await?;
        let table = postgres_io::inspect_capture_table(connection.client(), config).await?;
        Ok::<_, ConnectorError>((source, table))
    }
    .await;
    connection.close().await;
    let (source, table) = inspected?;
    bind_layout(config, declared, primary_key, &table)?;
    Ok((source, table))
}

/// The relation layout persisted when the source was created, if a binding was committed.
pub(super) fn committed_relation(
    binding: Option<&SchemaBinding>,
    config: &PostgresCdcConfig,
) -> Result<Option<RelationInfo>, ConnectorError> {
    let Some(binding) = binding else {
        return Ok(None);
    };
    let native = binding.value.as_ref().ok_or_else(|| {
        ConnectorError::SchemaMismatch(
            "committed PostgreSQL CDC binding lacks its table identity; recreate the source".into(),
        )
    })?;
    if native.format != NATIVE_FORMAT
        || native.definition["output"] != config.output_mode.to_string()
    {
        return Err(ConnectorError::SchemaMismatch(
            "committed PostgreSQL CDC binding has a different protocol or output.mode".into(),
        ));
    }
    serde_json::from_value(native.definition["relation"].clone())
        .map(Some)
        .map_err(|_| {
            ConnectorError::SchemaMismatch("invalid committed PostgreSQL table layout".into())
        })
}

/// Bind the declared schema and primary key to the captured table.
///
/// # Errors
/// Returns an actionable error when a declared column is not published, a type has no lossless
/// mapping, nullability could be violated, or the declared key differs from the table's.
pub(super) fn bind_layout(
    config: &PostgresCdcConfig,
    declared: &SchemaRef,
    primary_key: &[String],
    table: &CaptureTable,
) -> Result<RowLayout, ConnectorError> {
    let weighted = config.output_mode == OutputMode::Changelog;
    let fields = declared.fields();
    let visible = if weighted {
        let weight = fields.last().filter(|field| {
            field.name() == WEIGHT_COLUMN
                && field.data_type() == &DataType::Int64
                && !field.is_nullable()
        });
        if weight.is_none() {
            return Err(ConnectorError::ConfigurationError(
                "output.mode=changelog requires a trailing '__weight BIGINT NOT NULL' column"
                    .into(),
            ));
        }
        &fields[..fields.len() - 1]
    } else {
        &fields[..]
    };
    if let Some(field) = visible
        .iter()
        .find(|field| field.name().eq_ignore_ascii_case(WEIGHT_COLUMN))
    {
        return Err(ConnectorError::ConfigurationError(format!(
            "column '{}' is reserved for output.mode=changelog",
            field.name()
        )));
    }
    validate_primary_key(primary_key, table)?;

    let relation = &table.relation;
    let mut columns = Vec::with_capacity(visible.len());
    for field in visible {
        if config.change_time_column.as_deref() == Some(field.name().as_str()) {
            columns.push(change_time_column(field, primary_key, relation)?);
            continue;
        }
        let tuple_index = relation
            .columns
            .iter()
            .position(|column| column.name == *field.name())
            .ok_or_else(|| {
                ConnectorError::SchemaMismatch(format!(
                    "declared column '{}' is not a published column of PostgreSQL table {}.{}",
                    field.name(),
                    relation.namespace,
                    relation.name
                ))
            })?;
        let kind = bind_value_kind(&relation.columns[tuple_index], field.data_type()).map_err(
            |reason| ConnectorError::SchemaMismatch(format!("column '{}': {reason}", field.name())),
        )?;
        if !field.is_nullable() && !table.not_null[tuple_index] {
            return Err(ConnectorError::SchemaMismatch(format!(
                "column '{}' is declared NOT NULL but the PostgreSQL column is nullable",
                field.name()
            )));
        }
        columns.push(BoundColumn {
            name: field.name().clone(),
            source: ColumnSource::Tuple(tuple_index),
            kind,
            nullable: field.is_nullable(),
            is_key: primary_key.contains(field.name()),
        });
    }
    if let Some(column) = config
        .change_time_column
        .as_ref()
        .filter(|name| !columns.iter().any(|column| &column.name == *name))
    {
        return Err(ConnectorError::ConfigurationError(format!(
            "change.time.column names undeclared column '{column}'"
        )));
    }
    if let Some(missing) = primary_key
        .iter()
        .find(|key| !columns.iter().any(|column| &column.name == *key))
    {
        return Err(ConnectorError::SchemaMismatch(format!(
            "PRIMARY KEY column '{missing}' must be declared"
        )));
    }
    Ok(RowLayout {
        columns,
        tuple_width: relation.columns.len(),
        schema: SchemaRef::clone(declared),
        weighted,
    })
}

/// The declared change-time column: a non-key `TIMESTAMP NOT NULL` that no table column feeds.
fn change_time_column(
    field: &Field,
    primary_key: &[String],
    relation: &RelationInfo,
) -> Result<BoundColumn, ConnectorError> {
    let name = field.name();
    if primary_key.contains(name)
        || field.is_nullable()
        || field.data_type() != &DataType::Timestamp(TimeUnit::Microsecond, None)
    {
        return Err(ConnectorError::ConfigurationError(format!(
            "change.time.column '{name}' must be a non-key TIMESTAMP NOT NULL column"
        )));
    }
    if relation.columns.iter().any(|column| column.name == *name) {
        return Err(ConnectorError::ConfigurationError(format!(
            "change.time.column '{name}' is a published column of PostgreSQL table {}.{}; the \
             source fills it, so it must not name a table column",
            relation.namespace, relation.name
        )));
    }
    Ok(BoundColumn {
        name: name.clone(),
        source: ColumnSource::ChangeTime,
        kind: ValueKind::TimestampMicros,
        nullable: false,
        is_key: false,
    })
}

fn validate_primary_key(
    primary_key: &[String],
    table: &CaptureTable,
) -> Result<(), ConnectorError> {
    let mut declared = primary_key.to_vec();
    let mut actual = table.primary_key.clone();
    declared.sort_unstable();
    actual.sort_unstable();
    if declared.is_empty() || declared != actual {
        return Err(ConnectorError::ConfigurationError(format!(
            "declared PRIMARY KEY ({}) must equal the primary key ({}) of PostgreSQL table {}.{}",
            primary_key.join(", "),
            table.primary_key.join(", "),
            table.relation.namespace,
            table.relation.name
        )));
    }
    Ok(())
}

/// Require an announced relation to be exactly the bound layout.
pub(super) fn validate_relation(
    expected: &RelationInfo,
    incoming: &RelationInfo,
) -> Result<(), ConnectorError> {
    if expected.relation_id != incoming.relation_id
        || expected.namespace != incoming.namespace
        || expected.name != incoming.name
        || expected.replica_identity != incoming.replica_identity
        || expected.columns != incoming.columns
    {
        return Err(ConnectorError::SchemaMismatch(format!(
            "PostgreSQL relation {}.{} (oid {}) no longer matches the bound layout of {}.{} \
             (oid {}); a table or replica-identity change requires recreating the source and a \
             fresh snapshot",
            incoming.namespace,
            incoming.name,
            incoming.relation_id,
            expected.namespace,
            expected.name,
            expected.relation_id
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{Field, Schema, TimeUnit};

    use super::super::config::TableName;
    use super::super::types::PgColumn;
    use super::super::types::{ValueKind, INT4_OID, INT8_OID, TEXT_OID, TIMESTAMPTZ_OID};
    use super::*;

    fn table() -> CaptureTable {
        CaptureTable {
            relation: RelationInfo {
                relation_id: 42,
                namespace: "public".into(),
                name: "orders".into(),
                replica_identity: 'f',
                columns: vec![
                    PgColumn::new("id".into(), INT8_OID, -1, true),
                    PgColumn::new("status".into(), TEXT_OID, -1, true),
                    PgColumn::new("qty".into(), INT4_OID, -1, true),
                    PgColumn::new("updated_at".into(), TIMESTAMPTZ_OID, -1, true),
                ],
            },
            not_null: vec![true, true, false, false],
            primary_key: vec!["id".into()],
            row_security: false,
        }
    }

    fn config(output_mode: OutputMode) -> PostgresCdcConfig {
        PostgresCdcConfig {
            table: TableName::parse("public.orders").unwrap(),
            output_mode,
            ..PostgresCdcConfig::default()
        }
    }

    fn schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn declared_subset_binds_by_name_in_declared_order() {
        let declared = schema(vec![
            Field::new("qty", DataType::Int32, true),
            Field::new("id", DataType::Int64, false),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]);
        let layout = bind_layout(
            &config(OutputMode::Upsert),
            &declared,
            &["id".into()],
            &table(),
        )
        .unwrap();
        assert_eq!(layout.tuple_width, 4);
        assert!(!layout.weighted);
        let bound: Vec<_> = layout
            .columns
            .iter()
            .map(|column| (column.tuple_index().unwrap(), column.kind, column.is_key))
            .collect();
        assert_eq!(
            bound,
            [
                (2, ValueKind::Int32, false),
                (0, ValueKind::Int64, true),
                (3, ValueKind::TimestampTzMicros, false)
            ]
        );
    }

    #[test]
    fn changelog_requires_the_trailing_weight() {
        let base = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("qty", DataType::Int32, true),
        ];
        let error = bind_layout(
            &config(OutputMode::Changelog),
            &schema(base.clone()),
            &["id".into()],
            &table(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("__weight"), "{error}");
        let mut weighted = base.clone();
        weighted.push(Field::new("__weight", DataType::Int64, false));
        let layout = bind_layout(
            &config(OutputMode::Changelog),
            &schema(weighted.clone()),
            &["id".into()],
            &table(),
        )
        .unwrap();
        assert!(layout.weighted);
        assert_eq!(layout.columns.len(), 2);
        let error = bind_layout(
            &config(OutputMode::Upsert),
            &schema(weighted),
            &["id".into()],
            &table(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("reserved"), "{error}");
    }

    #[test]
    fn binding_rejects_key_type_nullability_and_missing_columns() {
        let id = Field::new("id", DataType::Int64, false);
        let cases = [
            (vec![id.clone()], vec![], "PRIMARY KEY"),
            (
                vec![id.clone(), Field::new("status", DataType::Utf8, true)],
                vec!["status".to_string()],
                "PRIMARY KEY",
            ),
            (
                vec![Field::new("status", DataType::Utf8, true)],
                vec!["id".to_string()],
                "must be declared",
            ),
            (
                vec![id.clone(), Field::new("qty", DataType::Int64, true)],
                vec!["id".to_string()],
                "qty",
            ),
            (
                vec![id.clone(), Field::new("qty", DataType::Int32, false)],
                vec!["id".to_string()],
                "nullable",
            ),
            (
                vec![id, Field::new("missing", DataType::Utf8, true)],
                vec!["id".to_string()],
                "not a published column",
            ),
        ];
        for (fields, key, expected) in cases {
            let error = bind_layout(&config(OutputMode::Upsert), &schema(fields), &key, &table())
                .unwrap_err();
            assert!(error.to_string().contains(expected), "{expected}: {error}");
        }
    }

    #[test]
    fn change_time_column_is_filled_by_the_source_not_read_from_the_table() {
        let mut timed = config(OutputMode::Upsert);
        timed.change_time_column = Some("changed_at".into());
        let id = Field::new("id", DataType::Int64, false);
        let changed_at = |name: &str, nullable: bool| {
            Field::new(
                name,
                DataType::Timestamp(TimeUnit::Microsecond, None),
                nullable,
            )
        };
        let layout = bind_layout(
            &timed,
            &schema(vec![id.clone(), changed_at("changed_at", false)]),
            &["id".into()],
            &table(),
        )
        .unwrap();
        assert_eq!(layout.columns[1].source, ColumnSource::ChangeTime);
        assert!(!layout.columns[1].is_key);

        let mut table_column = timed.clone();
        table_column.change_time_column = Some("updated_at".into());
        let cases = [
            (
                &timed,
                vec![id.clone(), changed_at("changed_at", true)],
                "TIMESTAMP NOT NULL",
            ),
            (
                &timed,
                vec![id.clone(), Field::new("changed_at", DataType::Int64, false)],
                "TIMESTAMP NOT NULL",
            ),
            (&timed, vec![id.clone()], "undeclared column 'changed_at'"),
            (
                &table_column,
                vec![id.clone(), changed_at("updated_at", false)],
                "must not name a table column",
            ),
        ];
        for (config, fields, expected) in cases {
            let error = bind_layout(config, &schema(fields), &["id".into()], &table()).unwrap_err();
            assert!(error.to_string().contains(expected), "{expected}: {error}");
        }
    }

    #[test]
    fn relation_drift_is_rejected_without_replacing_the_bound_layout() {
        let layout = table().relation;
        validate_relation(&layout, &layout).unwrap();
        let mut reordered = layout.clone();
        reordered.columns.swap(0, 1);
        let mut replaced = layout.clone();
        replaced.relation_id = 43;
        let mut changed_type = layout.clone();
        changed_type.columns[0] = PgColumn::new("id".into(), TEXT_OID, -1, true);
        let mut changed_identity = layout.clone();
        changed_identity.replica_identity = 'd';
        for incoming in [reordered, replaced, changed_type, changed_identity] {
            let error = validate_relation(&layout, &incoming).unwrap_err();
            assert!(error.to_string().contains("fresh snapshot"), "{error}");
        }
    }
}
