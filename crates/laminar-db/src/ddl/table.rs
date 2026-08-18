//! Reference and lookup `CREATE TABLE` statements: envelope rejection, column
//! and PRIMARY KEY shape parsing, `WITH` option parsing, and catalog mutation.
//!
//! The exhaustive `CreateTable` destructuring in `validate_create_table_envelope`
//! is deliberate: a sqlparser upgrade that adds a field must fail compilation
//! here until its semantics are reviewed.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{Field, Schema};

use laminar_core::catalog::CatalogObjectKind;
use laminar_sql::translator::streaming_ddl;

use crate::connector_manager::normalize_connector_type;
use crate::db::{canonical_object_name, exact_table_reference, LaminarDB};
use crate::error::DbError;

use super::catalog::reject_reserved_namespace;
use crate::handle::{DdlInfo, ExecuteResult};

/// Parsed `WITH (...)` clause of a `CREATE TABLE`.
#[derive(Default)]
struct CreateTableWith {
    connector_type: Option<String>,
    connector_options: HashMap<String, String>,
    format: Option<String>,
    format_options: HashMap<String, String>,
    storage: Option<String>,
}

/// One unsupported-clause rejection: a true `condition` yields the typed error.
fn reject_clause(condition: bool, clause: &str) -> Result<(), DbError> {
    if condition {
        return Err(DbError::InvalidOperation(format!(
            "CREATE TABLE clause '{clause}' is unsupported"
        )));
    }
    Ok(())
}

/// Standard `CREATE TABLE` extensions: TEMP/EXTERNAL/ICEBERG, Hive placement,
/// CTAS, and table clones.
fn reject_standard_table_extensions(create: &sqlparser::ast::CreateTable) -> Result<(), DbError> {
    reject_clause(create.temporary, "TEMPORARY")?;
    reject_clause(create.external, "EXTERNAL")?;
    reject_clause(create.dynamic, "DYNAMIC")?;
    reject_clause(create.global.is_some(), "GLOBAL/LOCAL")?;
    reject_clause(create.transient, "TRANSIENT")?;
    reject_clause(create.volatile, "VOLATILE")?;
    reject_clause(create.iceberg, "ICEBERG")?;
    reject_clause(
        !matches!(
            &create.hive_distribution,
            sqlparser::ast::HiveDistributionStyle::NONE
        ),
        "Hive distribution",
    )?;
    reject_clause(
        create
            .hive_formats
            .as_ref()
            .is_some_and(|formats| formats != &sqlparser::ast::HiveFormat::default()),
        "Hive format",
    )?;
    reject_clause(create.file_format.is_some(), "STORED AS")?;
    reject_clause(create.location.is_some(), "LOCATION")?;
    reject_clause(create.query.is_some(), "AS query")?;
    reject_clause(create.without_rowid, "WITHOUT ROWID")?;
    reject_clause(create.like.is_some(), "LIKE")?;
    reject_clause(create.clone.is_some(), "CLONE")?;
    reject_clause(create.version.is_some(), "VERSION")?;
    reject_clause(create.comment.is_some(), "COMMENT")?;
    reject_clause(create.on_commit.is_some(), "ON COMMIT")?;
    reject_clause(create.on_cluster.is_some(), "ON CLUSTER")?;
    Ok(())
}

/// Key, ordering, and inheritance clauses that would need storage semantics
/// `TableStore` does not provide.
fn reject_key_and_layout_clauses(create: &sqlparser::ast::CreateTable) -> Result<(), DbError> {
    reject_clause(create.primary_key.is_some(), "top-level PRIMARY KEY")?;
    reject_clause(create.order_by.is_some(), "ORDER BY")?;
    reject_clause(create.partition_by.is_some(), "PARTITION BY")?;
    reject_clause(create.cluster_by.is_some(), "CLUSTER BY")?;
    reject_clause(create.clustered_by.is_some(), "CLUSTERED BY")?;
    reject_clause(create.inherits.is_some(), "INHERITS")?;
    reject_clause(create.strict, "STRICT")?;
    reject_clause(create.copy_grants, "COPY GRANTS")?;
    Ok(())
}

/// Warehouse policy clauses (Snowflake/Databricks-style governance knobs).
fn reject_policy_clauses(create: &sqlparser::ast::CreateTable) -> Result<(), DbError> {
    reject_clause(
        create.enable_schema_evolution.is_some(),
        "ENABLE_SCHEMA_EVOLUTION",
    )?;
    reject_clause(create.change_tracking.is_some(), "CHANGE_TRACKING")?;
    reject_clause(
        create.data_retention_time_in_days.is_some(),
        "DATA_RETENTION_TIME_IN_DAYS",
    )?;
    reject_clause(
        create.max_data_extension_time_in_days.is_some(),
        "MAX_DATA_EXTENSION_TIME_IN_DAYS",
    )?;
    reject_clause(
        create.default_ddl_collation.is_some(),
        "DEFAULT_DDL_COLLATION",
    )?;
    reject_clause(
        create.with_aggregation_policy.is_some(),
        "AGGREGATION POLICY",
    )?;
    reject_clause(create.with_row_access_policy.is_some(), "ROW ACCESS POLICY")?;
    reject_clause(create.with_tags.is_some(), "TAG")?;
    reject_clause(create.external_volume.is_some(), "EXTERNAL_VOLUME")?;
    reject_clause(create.base_location.is_some(), "BASE_LOCATION")?;
    reject_clause(create.catalog.is_some(), "CATALOG")?;
    reject_clause(create.catalog_sync.is_some(), "CATALOG_SYNC")?;
    reject_clause(
        create.storage_serialization_policy.is_some(),
        "STORAGE_SERIALIZATION_POLICY",
    )?;
    reject_clause(create.target_lag.is_some(), "TARGET_LAG")?;
    reject_clause(create.warehouse.is_some(), "WAREHOUSE")?;
    reject_clause(create.refresh_mode.is_some(), "REFRESH_MODE")?;
    reject_clause(create.initialize.is_some(), "INITIALIZE")?;
    reject_clause(create.require_user, "REQUIRE USER")?;
    Ok(())
}

/// Reject every `sqlparser` CREATE TABLE extension that `LaminarDB` does not
/// implement. Keep this destructuring exhaustive: a parser upgrade that adds a
/// field must make this function fail to compile until its semantics are
/// reviewed.
pub(super) fn validate_create_table_envelope(
    create: &sqlparser::ast::CreateTable,
) -> Result<(), DbError> {
    let sqlparser::ast::CreateTable {
        or_replace: _,
        temporary: _,
        external: _,
        dynamic: _,
        global: _,
        if_not_exists: _,
        transient: _,
        volatile: _,
        iceberg: _,
        name: _,
        columns: _,
        constraints: _,
        hive_distribution: _,
        hive_formats: _,
        table_options: _,
        file_format: _,
        location: _,
        query: _,
        without_rowid: _,
        like: _,
        clone: _,
        version: _,
        comment: _,
        on_commit: _,
        on_cluster: _,
        primary_key: _,
        order_by: _,
        partition_by: _,
        cluster_by: _,
        clustered_by: _,
        inherits: _,
        strict: _,
        copy_grants: _,
        enable_schema_evolution: _,
        change_tracking: _,
        data_retention_time_in_days: _,
        max_data_extension_time_in_days: _,
        default_ddl_collation: _,
        with_aggregation_policy: _,
        with_row_access_policy: _,
        with_tags: _,
        external_volume: _,
        base_location: _,
        catalog: _,
        catalog_sync: _,
        storage_serialization_policy: _,
        target_lag: _,
        warehouse: _,
        refresh_mode: _,
        initialize: _,
        require_user: _,
    } = create;

    if create.or_replace {
        return Err(DbError::InvalidOperation(
            "CREATE OR REPLACE TABLE is unsupported; use DROP TABLE followed by CREATE TABLE"
                .into(),
        ));
    }

    reject_standard_table_extensions(create)?;
    reject_key_and_layout_clauses(create)?;
    reject_policy_clauses(create)?;
    Ok(())
}

/// Quoting-aware column identity: quoted names compare exactly, unquoted
/// names fold to lowercase (PG identifier folding).
fn ident_identity(ident: &sqlparser::ast::Ident) -> String {
    if ident.quote_style.is_some() {
        format!("quoted:{}", ident.value)
    } else {
        format!("unquoted:{}", ident.value.to_ascii_lowercase())
    }
}

/// Per-column identities, explicit NULL/NOT NULL decisions, and inline
/// `PRIMARY KEY` constraints collected from the column list.
type ColumnShape = (HashMap<String, (String, Option<bool>)>, Vec<Vec<String>>);

fn collect_column_shape(create: &sqlparser::ast::CreateTable) -> Result<ColumnShape, DbError> {
    use sqlparser::ast::ColumnOption;

    let mut column_names = HashSet::with_capacity(create.columns.len());
    let mut nullability = HashMap::with_capacity(create.columns.len());
    let mut primary_keys = Vec::new();

    for column in &create.columns {
        let name = column.name.value.clone();
        let identity = ident_identity(&column.name);
        if !column_names.insert(identity.clone()) {
            return Err(DbError::InvalidOperation(format!(
                "duplicate CREATE TABLE column '{name}'"
            )));
        }

        let mut explicit_nullable = None;
        for option in &column.options {
            if option.name.is_some() {
                return Err(DbError::InvalidOperation(format!(
                    "named column constraints are unsupported for '{name}': {option}"
                )));
            }
            match &option.option {
                ColumnOption::Null => {
                    if explicit_nullable.replace(true).is_some() {
                        return Err(DbError::InvalidOperation(format!(
                            "column '{name}' has repeated or conflicting NULL/NOT NULL constraints"
                        )));
                    }
                }
                ColumnOption::NotNull => {
                    if explicit_nullable.replace(false).is_some() {
                        return Err(DbError::InvalidOperation(format!(
                            "column '{name}' has repeated or conflicting NULL/NOT NULL constraints"
                        )));
                    }
                }
                ColumnOption::Unique {
                    is_primary: true,
                    characteristics: None,
                } => primary_keys.push(vec![identity.clone()]),
                unsupported => {
                    return Err(DbError::InvalidOperation(format!(
                        "unsupported CREATE TABLE option for column '{name}': {unsupported}"
                    )));
                }
            }
        }
        nullability.insert(identity, (name, explicit_nullable));
    }
    Ok((nullability, primary_keys))
}

/// Table-level `PRIMARY KEY (...)` constraints as identity lists. Decorations
/// and key expressions are rejected — only plain column lists are supported.
fn collect_primary_key_constraints(
    create: &sqlparser::ast::CreateTable,
) -> Result<Vec<Vec<String>>, DbError> {
    use sqlparser::ast::{Expr, TableConstraint};

    let mut primary_keys = Vec::new();
    for constraint in &create.constraints {
        let TableConstraint::PrimaryKey {
            name,
            index_name,
            index_type,
            columns,
            index_options,
            characteristics,
        } = constraint
        else {
            return Err(DbError::InvalidOperation(format!(
                "unsupported CREATE TABLE constraint: {constraint}"
            )));
        };

        if name.is_some()
            || index_name.is_some()
            || index_type.is_some()
            || !index_options.is_empty()
            || characteristics.is_some()
        {
            return Err(DbError::InvalidOperation(format!(
                "PRIMARY KEY decorations are unsupported: {constraint}"
            )));
        }

        let mut key_columns = Vec::with_capacity(columns.len());
        for column in columns {
            if column.operator_class.is_some()
                || column.column.options.asc.is_some()
                || column.column.options.nulls_first.is_some()
                || column.column.with_fill.is_some()
            {
                return Err(DbError::InvalidOperation(format!(
                    "PRIMARY KEY column decorations are unsupported: {column}"
                )));
            }
            let Expr::Identifier(ident) = &column.column.expr else {
                return Err(DbError::InvalidOperation(format!(
                    "CREATE TABLE primary key expressions are unsupported: {}",
                    column.column.expr
                )));
            };
            key_columns.push(ident_identity(ident));
        }
        primary_keys.push(key_columns);
    }
    Ok(primary_keys)
}

pub(super) fn build_table_fields_and_primary_key(
    create: &sqlparser::ast::CreateTable,
) -> Result<(Vec<Field>, String), DbError> {
    let (nullability, mut primary_keys) = collect_column_shape(create)?;
    primary_keys.extend(collect_primary_key_constraints(create)?);

    let [columns] = primary_keys.as_slice() else {
        return Err(DbError::InvalidOperation(
            "CREATE TABLE requires exactly one PRIMARY KEY constraint".into(),
        ));
    };
    let [primary_key] = columns.as_slice() else {
        return Err(DbError::InvalidOperation(
            "composite PRIMARY KEY is unsupported until TableStore supports composite keys".into(),
        ));
    };
    let Some((primary_key_name, explicit_nullable)) = nullability.get(primary_key) else {
        return Err(DbError::InvalidOperation(
            "PRIMARY KEY references a column that is not declared".into(),
        ));
    };
    if *explicit_nullable == Some(true) {
        return Err(DbError::InvalidOperation(format!(
            "PRIMARY KEY column '{primary_key}' cannot be declared NULL"
        )));
    }

    let fields = create
        .columns
        .iter()
        .map(|column| {
            let data_type =
                streaming_ddl::sql_type_to_arrow(&column.data_type).map_err(|error| {
                    DbError::InvalidOperation(format!(
                        "unsupported column type for '{}': {error}",
                        column.name
                    ))
                })?;
            let name = column.name.value.clone();
            let identity = ident_identity(&column.name);
            let nullable = if identity == primary_key.as_str() {
                false
            } else {
                nullability
                    .get(&identity)
                    .and_then(|(_, nullable)| *nullable)
                    .unwrap_or(true)
            };
            Ok(Field::new(name, data_type, nullable))
        })
        .collect::<Result<Vec<_>, DbError>>()?;

    Ok((fields, primary_key_name.clone()))
}

fn parse_create_table_with(
    with_options: &[sqlparser::ast::SqlOption],
) -> Result<CreateTableWith, DbError> {
    let mut out = CreateTableWith {
        connector_options: HashMap::with_capacity(8),
        format_options: HashMap::with_capacity(4),
        ..Default::default()
    };
    let mut seen = HashSet::with_capacity(with_options.len());
    for opt in with_options {
        let sqlparser::ast::SqlOption::KeyValue { key, value } = opt else {
            return Err(DbError::InvalidOperation(format!(
                "unsupported CREATE TABLE option: {opt}"
            )));
        };
        let k = key.to_string().to_lowercase();
        if !seen.insert(k.clone()) {
            return Err(DbError::InvalidOperation(format!(
                "duplicate CREATE TABLE option '{k}'"
            )));
        }
        let val = value.to_string().trim_matches('\'').to_string();
        match k.as_str() {
            "connector" => out.connector_type = Some(val),
            "format" => out.format = Some(val),
            "refresh" => {
                return Err(DbError::InvalidOperation(
                    "CREATE TABLE option 'refresh' is unsupported; reference tables load one \
                     authoritative startup snapshot"
                        .into(),
                ));
            }
            "cache_mode" | "cache.mode" | "cache_max_entries" | "cache.max_entries"
            | "cache_max_bytes" | "cache.max_bytes" | "cache.memory" | "cache_ttl"
            | "cache.ttl" => {
                return Err(DbError::InvalidOperation(format!(
                    "CREATE TABLE option '{k}' is unsupported; use CREATE LOOKUP TABLE for a bounded on-demand cache"
                )));
            }
            "storage" => out.storage = Some(val),
            kk if kk.starts_with("format.") => {
                out.format_options
                    .insert(kk.strip_prefix("format.").unwrap().to_string(), val);
            }
            _ => {
                out.connector_options.insert(k, val);
            }
        }
    }
    Ok(out)
}

fn validate_create_table_with(opts: &CreateTableWith) -> Result<(), DbError> {
    if let Some(storage) = &opts.storage {
        return Err(DbError::InvalidOperation(format!(
            "CREATE TABLE storage option '{storage}' is unsupported"
        )));
    }

    if !opts.format_options.is_empty() && opts.format.is_none() {
        return Err(DbError::InvalidOperation(
            "format.* options require a format".into(),
        ));
    }

    if opts.connector_type.is_none()
        && (!opts.connector_options.is_empty() || opts.format.is_some())
    {
        return Err(DbError::InvalidOperation(
            "connector and format options require a connector".into(),
        ));
    }

    Ok(())
}

impl LaminarDB {
    pub(crate) fn handle_create_table(
        &self,
        create: &sqlparser::ast::CreateTable,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_offline_topology_ddl_allowed("CREATE TABLE")?;
        validate_create_table_envelope(create)?;
        let name = canonical_object_name(&create.name)?;
        reject_reserved_namespace(&name)?;

        let with_options = match &create.table_options {
            sqlparser::ast::CreateTableOptions::With(opts) => opts.as_slice(),
            sqlparser::ast::CreateTableOptions::None => &[],
            unsupported => {
                return Err(DbError::InvalidOperation(format!(
                    "unsupported CREATE TABLE options: {unsupported}"
                )));
            }
        };
        let mut opts = parse_create_table_with(with_options)?;
        validate_create_table_with(&opts)?;
        if let Some(connector_type) = opts.connector_type.as_mut() {
            let normalized = normalize_connector_type(connector_type);
            if !self.connector_registry.has_table_source(&normalized) {
                return Err(DbError::Connector(format!(
                    "connector '{connector_type}' is not a registered reference-table source. Available: {:?}",
                    self.connector_registry.list_table_sources()
                )));
            }
            *connector_type = normalized;
        }
        let (fields, primary_key) = build_table_fields_and_primary_key(create)?;
        let schema = Arc::new(Schema::new(fields));
        if crate::catalog::schema_has_reserved_mutation_columns(schema.as_ref()) {
            return Err(DbError::InvalidOperation(
                "CREATE TABLE columns _op, __op, and __weight are reserved engine mutation metadata"
                    .into(),
            ));
        }

        let Some(reservation) =
            self.reserve_catalog_name(&name, CatalogObjectKind::Table, create.if_not_exists)?
        else {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "CREATE TABLE".to_string(),
                object_name: name,
                applied: false,
            }));
        };

        self.table_store
            .write()
            .create_table(&name, schema.clone(), &primary_key)?;

        if let Some(ref connector_type) = opts.connector_type {
            {
                let mut ts = self.table_store.write();
                ts.set_connector(&name, connector_type);
            }

            let mut mgr = self.connector_manager.lock();
            mgr.register_table(crate::connector_manager::TableRegistration {
                name: name.clone(),
                primary_key: primary_key.clone(),
                connector_type: opts.connector_type.clone(),
                connector_options: opts.connector_options,
                format: opts.format,
                format_options: opts.format_options,
                on_demand: false,
                cache_max_bytes: None,
                cache_ttl: None,
            });
        }

        // scan() reads current TableStore rows; no re-register needed after INSERTs.
        {
            let provider = crate::table_provider::ReferenceTableProvider::new(
                name.clone(),
                schema.clone(),
                self.table_store.clone(),
            );
            match self
                .ctx
                .register_table(exact_table_reference(&name), Arc::new(provider))
            {
                Ok(None) => {}
                Ok(Some(previous)) => {
                    let _ = self
                        .ctx
                        .register_table(exact_table_reference(&name), previous);
                    return Err(DbError::InvalidOperation(format!(
                        "cannot create table '{name}': its provider was claimed concurrently"
                    )));
                }
                Err(error) => {
                    return Err(DbError::InvalidOperation(format!(
                        "failed to register table '{name}': {error}"
                    )));
                }
            }
        }

        reservation.commit();

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE TABLE".to_string(),
            object_name: name,
            applied: true,
        }))
    }
    /// Re-publish a lookup table's snapshot after a write so lookup joins don't
    /// probe stale rows. No-op for non-lookup tables.
    pub(crate) fn sync_table_to_datafusion(&self, name: &str) -> Result<(), DbError> {
        if self.lookup_registry.get_entry(name).is_none() {
            return Ok(());
        }
        if let Some(batch) = self.table_store.read().to_record_batch(name)? {
            self.lookup_registry
                .register(name, laminar_sql::datafusion::LookupSnapshot { batch });
        }
        Ok(())
    }
}
