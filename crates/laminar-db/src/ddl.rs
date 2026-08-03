//! DDL handlers — reopens `impl LaminarDB` to keep `db.rs` focused on dispatch.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_plan::ExecutionPlan;
use laminar_core::catalog::CatalogObjectKind;
use laminar_sql::parser::StreamingStatement;
use laminar_sql::translator::streaming_ddl::{self, ColumnDefinition};

use crate::connector_manager::normalize_connector_type;
use crate::db::{
    canonical_object_name, exact_table_reference, parse_duration_str, DbState, LaminarDB,
};
use crate::error::DbError;
use crate::handle::{DdlInfo, ExecuteResult};
use crate::pipeline::{ControlMutation, ControlMutationState};

pub(crate) const CONTROL_ACK_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

enum ControlAck {
    Response(Result<(), DbError>),
    Closed,
    TimedOut,
}

async fn resolve_control_ack(
    operation: &str,
    acknowledgement: tokio::sync::oneshot::Receiver<Result<(), DbError>>,
    mutation: &ControlMutation,
) -> Result<(), DbError> {
    let acknowledgement = match tokio::time::timeout(CONTROL_ACK_DEADLINE, acknowledgement).await {
        Ok(Ok(response)) => ControlAck::Response(response),
        Ok(Err(_)) => ControlAck::Closed,
        Err(_) => ControlAck::TimedOut,
    };

    // The mutation CAS, not delivery of the best-effort acknowledgement, is the
    // linearization point. This also closes the timeout/receiver-drop race: either
    // the coordinator applied first or the caller atomically prevents application.
    match mutation.cancel() {
        ControlMutationState::Applied => {
            match acknowledgement {
                ControlAck::Response(Err(ref error)) => tracing::warn!(
                    operation,
                    error = %error,
                    "control mutation was applied before an inconsistent error acknowledgement"
                ),
                ControlAck::Closed => tracing::warn!(
                    operation,
                    "control mutation was applied but its acknowledgement sender closed"
                ),
                ControlAck::TimedOut => tracing::warn!(
                    operation,
                    "control mutation was applied but its acknowledgement missed the deadline"
                ),
                ControlAck::Response(Ok(())) => {}
            }
            Ok(())
        }
        ControlMutationState::Cancelled => match acknowledgement {
            ControlAck::Response(Err(error)) => Err(error),
            ControlAck::Response(Ok(())) => Err(DbError::Pipeline(format!(
                "pipeline acknowledged {operation} without committing it"
            ))),
            ControlAck::Closed => Err(DbError::Pipeline(format!(
                "pipeline stopped before acknowledging {operation}"
            ))),
            ControlAck::TimedOut => Err(DbError::Pipeline(format!(
                "pipeline did not acknowledge {operation} within {} seconds",
                CONTROL_ACK_DEADLINE.as_secs()
            ))),
        },
        ControlMutationState::Pending => {
            unreachable!("cancelling a control mutation must resolve pending state")
        }
    }
}

/// Which connector registry `prepare_connector` validates against.
#[derive(Clone, Copy)]
enum ConnectorKind {
    Source,
    Sink,
}

pub(crate) struct CatalogNameReservation<'a> {
    db: &'a LaminarDB,
    name: String,
    kind: CatalogObjectKind,
    control_mutation: Option<Arc<ControlMutation>>,
    committed: bool,
}

impl CatalogNameReservation<'_> {
    fn bind_control_mutation(&mut self, mutation: Arc<ControlMutation>) {
        debug_assert!(self.control_mutation.is_none());
        self.control_mutation = Some(mutation);
    }

    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for CatalogNameReservation<'_> {
    fn drop(&mut self) {
        let applied = self
            .control_mutation
            .as_ref()
            .is_some_and(|mutation| mutation.state() == ControlMutationState::Applied);
        if !self.committed && !applied {
            self.db.rollback_catalog_create_or_fence(
                &self.name,
                self.kind,
                "catalog create rollback",
            );
        }
    }
}

/// Incremental-emit store decision for a non-windowed MV.
enum IncEmit {
    /// Keyed running aggregate → keyed upsert snapshot (key = GROUP BY column indices).
    Upsert(Vec<usize>),
    /// Projection/filter over a changelog → Z-set multiset snapshot.
    Multiset,
    /// Full-emit (not incremental): replace-all aggregate or append.
    None,
}

struct StreamCreateGuard<'a> {
    db: &'a LaminarDB,
    name: String,
    mutation: Arc<ControlMutation>,
}

impl Drop for StreamCreateGuard<'_> {
    fn drop(&mut self) {
        if self.mutation.cancel() != ControlMutationState::Applied {
            self.db.rollback_catalog_create_or_fence(
                &self.name,
                CatalogObjectKind::Stream,
                "stream create rollback",
            );
        }
    }
}

struct MaterializedViewCreateGuard<'a> {
    db: &'a LaminarDB,
    name: String,
    mutation: Arc<ControlMutation>,
}

impl Drop for MaterializedViewCreateGuard<'_> {
    fn drop(&mut self) {
        if self.mutation.cancel() != ControlMutationState::Applied {
            self.db.rollback_catalog_create_or_fence(
                &self.name,
                CatalogObjectKind::MaterializedView,
                "materialized-view create rollback",
            );
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CatalogDropTarget {
    name: String,
    kind: CatalogObjectKind,
}

struct StreamingDropGuard<'a> {
    db: &'a LaminarDB,
    targets: Vec<CatalogDropTarget>,
    mutation: Arc<ControlMutation>,
    finished: bool,
}

impl StreamingDropGuard<'_> {
    fn finish(mut self) -> Result<(), DbError> {
        debug_assert_eq!(self.mutation.state(), ControlMutationState::Applied);
        let result = self
            .db
            .teardown_catalog_targets(&self.targets, "catalog drop");
        self.finished = true;
        result
    }
}

impl Drop for StreamingDropGuard<'_> {
    fn drop(&mut self) {
        if !self.finished && self.mutation.cancel() == ControlMutationState::Applied {
            self.db
                .teardown_catalog_targets_or_fence(&self.targets, "cancelled catalog drop");
        }
    }
}

/// Reject object names in the reserved `laminar` namespace, which is owned by
/// the system catalog (`laminar.models`, `laminar.ai_calls`).
fn reject_reserved_namespace(name: &str) -> Result<(), DbError> {
    if name.starts_with("laminar.") {
        return Err(DbError::InvalidOperation(format!(
            "'{name}' uses the reserved 'laminar' namespace (system catalog views \
             laminar.models / laminar.ai_calls live there)"
        )));
    }
    Ok(())
}

fn contains_builtin_join_without_cluster_lifecycle(plan: &Arc<dyn ExecutionPlan>) -> bool {
    use datafusion::physical_plan::joins::{
        CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PiecewiseMergeJoinExec, SortMergeJoinExec,
        SymmetricHashJoinExec,
    };

    let plan_type = plan.as_any();
    plan_type.is::<CrossJoinExec>()
        || plan_type.is::<HashJoinExec>()
        || plan_type.is::<NestedLoopJoinExec>()
        || plan_type.is::<PiecewiseMergeJoinExec>()
        || plan_type.is::<SortMergeJoinExec>()
        || plan_type.is::<SymmetricHashJoinExec>()
        || plan
            .children()
            .into_iter()
            .any(contains_builtin_join_without_cluster_lifecycle)
}

pub(crate) fn logical_aggregate_stage_count(plan: &datafusion_expr::LogicalPlan) -> usize {
    usize::from(matches!(plan, datafusion_expr::LogicalPlan::Aggregate(_)))
        + plan
            .inputs()
            .into_iter()
            .map(logical_aggregate_stage_count)
            .sum::<usize>()
}

fn distinct_streaming_aggregate(sql: &str) -> Option<String> {
    let statements = laminar_sql::parse_streaming_sql(sql).ok()?;
    let laminar_sql::parser::StreamingStatement::Standard(statement) = statements.first()? else {
        return None;
    };
    laminar_sql::parser::aggregation_parser::analyze_aggregates(statement.as_ref())
        .aggregates
        .into_iter()
        .find(|aggregate| aggregate.distinct)
        .map(|aggregate| format!("{:?}", aggregate.aggregate_type).to_ascii_lowercase())
}

pub(crate) struct PlannedStreamingQuery {
    pub(crate) emit_clause: Option<laminar_sql::parser::EmitClause>,
    pub(crate) window_config: Option<laminar_sql::translator::WindowOperatorConfig>,
    pub(crate) order_config: Option<laminar_sql::translator::OrderOperatorConfig>,
    pub(crate) join_config: Option<Vec<laminar_sql::translator::JoinOperatorConfig>>,
    pub(crate) has_analytic: bool,
    pub(crate) has_frame: bool,
}

/// Terminality guard error: `consumer` tried to read incremental MV `mv`'s changelog.
pub(crate) fn incremental_mv_consumer_error(mv: &str, consumer: &str) -> DbError {
    DbError::MaterializedView(format!(
        "[LDB-1300] {consumer} cannot consume incremental materialized view '{mv}': it emits a \
         dirty-only changelog, not a full snapshot. Read it with `SELECT * FROM {mv}` (snapshot), \
         or recreate '{mv}' without `incremental_emit`."
    ))
}

/// Parsed `WITH (...)` clause of a `CREATE TABLE`.
#[derive(Default)]
struct CreateTableWith {
    connector_type: Option<String>,
    connector_options: HashMap<String, String>,
    format: Option<String>,
    format_options: HashMap<String, String>,
    storage: Option<String>,
}

/// Reject every `sqlparser` CREATE TABLE extension that `LaminarDB` does not
/// implement. Keep this destructuring exhaustive: a parser upgrade that adds a
/// field must make this function fail to compile until its semantics are
/// reviewed.
fn validate_create_table_envelope(create: &sqlparser::ast::CreateTable) -> Result<(), DbError> {
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

    macro_rules! reject_clause {
        ($condition:expr, $clause:literal) => {
            if $condition {
                return Err(DbError::InvalidOperation(format!(
                    "CREATE TABLE clause '{}' is unsupported",
                    $clause
                )));
            }
        };
    }

    reject_clause!(create.temporary, "TEMPORARY");
    reject_clause!(create.external, "EXTERNAL");
    reject_clause!(create.dynamic, "DYNAMIC");
    reject_clause!(create.global.is_some(), "GLOBAL/LOCAL");
    reject_clause!(create.transient, "TRANSIENT");
    reject_clause!(create.volatile, "VOLATILE");
    reject_clause!(create.iceberg, "ICEBERG");
    reject_clause!(
        !matches!(
            &create.hive_distribution,
            sqlparser::ast::HiveDistributionStyle::NONE
        ),
        "Hive distribution"
    );
    reject_clause!(
        create
            .hive_formats
            .as_ref()
            .is_some_and(|formats| formats != &sqlparser::ast::HiveFormat::default()),
        "Hive format"
    );
    reject_clause!(create.file_format.is_some(), "STORED AS");
    reject_clause!(create.location.is_some(), "LOCATION");
    reject_clause!(create.query.is_some(), "AS query");
    reject_clause!(create.without_rowid, "WITHOUT ROWID");
    reject_clause!(create.like.is_some(), "LIKE");
    reject_clause!(create.clone.is_some(), "CLONE");
    reject_clause!(create.version.is_some(), "VERSION");
    reject_clause!(create.comment.is_some(), "COMMENT");
    reject_clause!(create.on_commit.is_some(), "ON COMMIT");
    reject_clause!(create.on_cluster.is_some(), "ON CLUSTER");
    reject_clause!(create.primary_key.is_some(), "top-level PRIMARY KEY");
    reject_clause!(create.order_by.is_some(), "ORDER BY");
    reject_clause!(create.partition_by.is_some(), "PARTITION BY");
    reject_clause!(create.cluster_by.is_some(), "CLUSTER BY");
    reject_clause!(create.clustered_by.is_some(), "CLUSTERED BY");
    reject_clause!(create.inherits.is_some(), "INHERITS");
    reject_clause!(create.strict, "STRICT");
    reject_clause!(create.copy_grants, "COPY GRANTS");
    reject_clause!(
        create.enable_schema_evolution.is_some(),
        "ENABLE_SCHEMA_EVOLUTION"
    );
    reject_clause!(create.change_tracking.is_some(), "CHANGE_TRACKING");
    reject_clause!(
        create.data_retention_time_in_days.is_some(),
        "DATA_RETENTION_TIME_IN_DAYS"
    );
    reject_clause!(
        create.max_data_extension_time_in_days.is_some(),
        "MAX_DATA_EXTENSION_TIME_IN_DAYS"
    );
    reject_clause!(
        create.default_ddl_collation.is_some(),
        "DEFAULT_DDL_COLLATION"
    );
    reject_clause!(
        create.with_aggregation_policy.is_some(),
        "AGGREGATION POLICY"
    );
    reject_clause!(create.with_row_access_policy.is_some(), "ROW ACCESS POLICY");
    reject_clause!(create.with_tags.is_some(), "TAG");
    reject_clause!(create.external_volume.is_some(), "EXTERNAL_VOLUME");
    reject_clause!(create.base_location.is_some(), "BASE_LOCATION");
    reject_clause!(create.catalog.is_some(), "CATALOG");
    reject_clause!(create.catalog_sync.is_some(), "CATALOG_SYNC");
    reject_clause!(
        create.storage_serialization_policy.is_some(),
        "STORAGE_SERIALIZATION_POLICY"
    );
    reject_clause!(create.target_lag.is_some(), "TARGET_LAG");
    reject_clause!(create.warehouse.is_some(), "WAREHOUSE");
    reject_clause!(create.refresh_mode.is_some(), "REFRESH_MODE");
    reject_clause!(create.initialize.is_some(), "INITIALIZE");
    reject_clause!(create.require_user, "REQUIRE USER");
    Ok(())
}

fn build_table_fields_and_primary_key(
    create: &sqlparser::ast::CreateTable,
) -> Result<(Vec<Field>, String), DbError> {
    use sqlparser::ast::{ColumnOption, Expr, TableConstraint};

    fn ident_identity(ident: &sqlparser::ast::Ident) -> String {
        if ident.quote_style.is_some() {
            format!("quoted:{}", ident.value)
        } else {
            format!("unquoted:{}", ident.value.to_ascii_lowercase())
        }
    }

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
    fn catalog_object_is_present(
        &self,
        name: &str,
        kind: CatalogObjectKind,
    ) -> Result<bool, DbError> {
        match kind {
            CatalogObjectKind::Source => Ok(self.catalog.get_source(name).is_some()
                && self
                    .ctx
                    .table_exist(exact_table_reference(name))
                    .map_err(|error| {
                        DbError::InvalidOperation(format!(
                            "could not verify source provider for '{name}': {error}"
                        ))
                    })?
                && self.planner.lock().get_source(name).is_some()
                && self.mv_registry.lock().is_base_table(name)),
            CatalogObjectKind::Sink => Ok(self.catalog.get_sink_input(name).is_some()
                && self.planner.lock().get_sink(name).is_some()),
            CatalogObjectKind::Table => Ok(self.table_store.read().has_table(name)
                && self
                    .ctx
                    .table_exist(exact_table_reference(name))
                    .map_err(|error| {
                        DbError::InvalidOperation(format!(
                            "could not verify table provider for '{name}': {error}"
                        ))
                    })?),
            CatalogObjectKind::LookupTable => Ok(self.table_store.read().has_table(name)
                && self.planner.lock().get_lookup_table(name).is_some()
                && self
                    .ctx
                    .table_exist(exact_table_reference(name))
                    .map_err(|error| {
                        DbError::InvalidOperation(format!(
                            "could not verify lookup provider for '{name}': {error}"
                        ))
                    })?),
            CatalogObjectKind::Stream => Ok(self.catalog.get_stream_entry(name).is_some()
                && self.connector_manager.lock().streams().contains_key(name)),
            CatalogObjectKind::MaterializedView => Ok(self.mv_registry.lock().get(name).is_some()
                && self.connector_manager.lock().streams().contains_key(name)
                && self.mv_store.read().has_mv(name)
                && self
                    .ctx
                    .table_exist(exact_table_reference(name))
                    .map_err(|error| {
                        DbError::InvalidOperation(format!(
                            "could not verify materialized-view provider for '{name}': {error}"
                        ))
                    })?),
        }
    }

    pub(crate) fn reserve_catalog_name(
        &self,
        name: &str,
        kind: CatalogObjectKind,
        if_not_exists: bool,
    ) -> Result<Option<CatalogNameReservation<'_>>, DbError> {
        self.ensure_catalog_cleanup_unfenced("catalog create")?;
        reject_reserved_namespace(name)?;
        let existing = self.catalog_namespace.lock().get(name).copied();
        if let Some(existing) = existing {
            if existing != kind {
                return Err(DbError::InvalidOperation(format!(
                    "cannot create {kind} '{name}': the identifier is owned by a {existing}"
                )));
            }
            if !self.catalog_object_is_present(name, kind)? {
                return Err(DbError::InvalidOperation(format!(
                    "catalog namespace for {kind} '{name}' is inconsistent"
                )));
            }
            if if_not_exists {
                return Ok(None);
            }
            return Err(DbError::InvalidOperation(format!(
                "{kind} '{name}' already exists"
            )));
        }

        if self
            .ctx
            .table_exist(exact_table_reference(name))
            .map_err(|error| {
                DbError::InvalidOperation(format!(
                    "could not inspect catalog namespace for '{name}': {error}"
                ))
            })?
        {
            return Err(DbError::InvalidOperation(format!(
                "cannot create {kind} '{name}': an untyped table provider already owns the identifier"
            )));
        }

        let replaced = self.catalog_namespace.lock().insert(name.to_string(), kind);
        debug_assert!(replaced.is_none());
        Ok(Some(CatalogNameReservation {
            db: self,
            name: name.to_string(),
            kind,
            control_mutation: None,
            committed: false,
        }))
    }

    pub(crate) fn require_catalog_kind(
        &self,
        name: &str,
        expected: CatalogObjectKind,
        if_exists: bool,
    ) -> Result<bool, DbError> {
        self.ensure_catalog_cleanup_unfenced("catalog drop")?;
        let Some(actual) = self.catalog_namespace.lock().get(name).copied() else {
            if if_exists {
                return Ok(false);
            }
            return Err(match expected {
                CatalogObjectKind::Source => DbError::SourceNotFound(name.to_string()),
                CatalogObjectKind::Sink => DbError::SinkNotFound(name.to_string()),
                CatalogObjectKind::Table => DbError::TableNotFound(name.to_string()),
                CatalogObjectKind::Stream => DbError::StreamNotFound(name.to_string()),
                CatalogObjectKind::MaterializedView => {
                    DbError::MaterializedView(format!("materialized view not found: {name}"))
                }
                CatalogObjectKind::LookupTable => {
                    DbError::InvalidOperation(format!("lookup table '{name}' does not exist"))
                }
            });
        };
        if actual != expected {
            return Err(DbError::InvalidOperation(format!(
                "cannot drop {expected} '{name}': the identifier is owned by a {actual}"
            )));
        }
        if !self.catalog_object_is_present(name, expected)? {
            return Err(DbError::InvalidOperation(format!(
                "catalog namespace for {expected} '{name}' is inconsistent"
            )));
        }
        Ok(true)
    }

    fn deregister_catalog_provider(&self, name: &str, errors: &mut Vec<String>) {
        #[cfg(test)]
        if self.catalog_cleanup_deregister_fault.lock().as_deref() == Some(name) {
            errors.push("injected DataFusion provider deregistration failure".into());
            return;
        }

        if let Err(error) = self.ctx.deregister_table(exact_table_reference(name)) {
            errors.push(format!(
                "DataFusion provider deregistration failed: {error}"
            ));
        }
    }

    fn cleanup_catalog_object(&self, name: &str, kind: CatalogObjectKind) -> Result<(), DbError> {
        let mut errors = Vec::new();

        if matches!(
            kind,
            CatalogObjectKind::Source
                | CatalogObjectKind::Table
                | CatalogObjectKind::LookupTable
                | CatalogObjectKind::Stream
                | CatalogObjectKind::MaterializedView
        ) {
            self.deregister_catalog_provider(name, &mut errors);
        }

        match kind {
            CatalogObjectKind::Source => {
                self.catalog.drop_source(name);
                self.connector_manager.lock().unregister_source(name);
                self.planner.lock().unregister_source(name);
                self.mv_registry.lock().unregister_base_table(name);
            }
            CatalogObjectKind::Sink => {
                self.catalog.drop_sink(name);
                self.connector_manager.lock().unregister_sink(name);
                self.planner.lock().unregister_sink(name);
            }
            CatalogObjectKind::Table => {
                self.table_store.write().drop_table(name);
                self.connector_manager.lock().unregister_table(name);
            }
            CatalogObjectKind::LookupTable => {
                self.table_store.write().drop_table(name);
                self.connector_manager.lock().unregister_table(name);
                self.lookup_registry.unregister(name);
                self.planner.lock().unregister_lookup_table(name);
                self.refresh_lookup_optimizer_rule();
            }
            CatalogObjectKind::Stream => {
                self.catalog.drop_stream(name);
                self.connector_manager.lock().unregister_stream(name);
                self.subscription_registry.drop_name(name);
                self.planner.lock().unregister_query(name);
                self.stream_schemas.write().remove(name);
            }
            CatalogObjectKind::MaterializedView => {
                let mut registry = self.mv_registry.lock();
                if registry.get(name).is_some() {
                    if let Err(error) = registry.unregister(name) {
                        errors.push(format!(
                            "materialized-view registry deregistration failed: {error}"
                        ));
                    }
                }
                drop(registry);
                self.connector_manager.lock().unregister_stream(name);
                self.mv_store.write().drop_mv(name);
                self.subscription_registry.drop_name(name);
                self.planner.lock().unregister_query(name);
                self.stream_schemas.write().remove(name);
            }
        }
        self.connector_manager.lock().remove_ddl(name);

        let mut residues = Vec::new();
        if matches!(
            kind,
            CatalogObjectKind::Source
                | CatalogObjectKind::Table
                | CatalogObjectKind::LookupTable
                | CatalogObjectKind::Stream
                | CatalogObjectKind::MaterializedView
        ) {
            match self.ctx.table_exist(exact_table_reference(name)) {
                Ok(false) => {}
                Ok(true) => residues.push("DataFusion provider"),
                Err(error) => errors.push(format!(
                    "DataFusion provider absence verification failed: {error}"
                )),
            }
        }

        let (has_ddl, has_source, has_sink, has_stream, has_table) = {
            let manager = self.connector_manager.lock();
            (
                manager.get_ddl(name).is_some(),
                manager.sources().contains_key(name),
                manager.sinks().contains_key(name),
                manager.streams().contains_key(name),
                manager.tables().contains_key(name),
            )
        };
        if has_ddl {
            residues.push("stored DDL");
        }
        match kind {
            CatalogObjectKind::Source => {
                if self.catalog.get_source(name).is_some() {
                    residues.push("source catalog");
                }
                if has_source {
                    residues.push("source connector registration");
                }
                if self.planner.lock().get_source(name).is_some() {
                    residues.push("source planner registration");
                }
                if self.mv_registry.lock().is_base_table(name) {
                    residues.push("materialized-view base-table registration");
                }
            }
            CatalogObjectKind::Sink => {
                if self.catalog.get_sink_input(name).is_some() {
                    residues.push("sink catalog");
                }
                if has_sink {
                    residues.push("sink connector registration");
                }
                if self.planner.lock().get_sink(name).is_some() {
                    residues.push("sink planner registration");
                }
            }
            CatalogObjectKind::Table => {
                if self.table_store.read().has_table(name) {
                    residues.push("table store");
                }
                if has_table {
                    residues.push("table connector registration");
                }
            }
            CatalogObjectKind::LookupTable => {
                if self.table_store.read().has_table(name) {
                    residues.push("table store");
                }
                if has_table {
                    residues.push("table connector registration");
                }
                if self.lookup_registry.get_entry(name).is_some() {
                    residues.push("lookup snapshot registry");
                }
                if self.planner.lock().get_lookup_table(name).is_some() {
                    residues.push("lookup planner registration");
                }
            }
            CatalogObjectKind::Stream => {
                if self.catalog.get_stream_entry(name).is_some() {
                    residues.push("stream catalog");
                }
                if has_stream {
                    residues.push("stream registration");
                }
                if self.subscription_registry.contains_name(name) {
                    residues.push("subscription registry");
                }
                if self.planner.lock().has_query(name) {
                    residues.push("query planner registration");
                }
                if self.stream_schemas.read().contains_key(name) {
                    residues.push("stream schema cache");
                }
            }
            CatalogObjectKind::MaterializedView => {
                if self.mv_registry.lock().get(name).is_some() {
                    residues.push("materialized-view registry");
                }
                if has_stream {
                    residues.push("stream registration");
                }
                if self.mv_store.read().has_mv(name) {
                    residues.push("materialized-view store");
                }
                if self.subscription_registry.contains_name(name) {
                    residues.push("subscription registry");
                }
                if self.planner.lock().has_query(name) {
                    residues.push("query planner registration");
                }
                if self.stream_schemas.read().contains_key(name) {
                    residues.push("stream schema cache");
                }
            }
        }
        if !errors.is_empty() || !residues.is_empty() {
            let mut details = errors;
            if !residues.is_empty() {
                details.push(format!("residual state: {}", residues.join(", ")));
            }
            return Err(DbError::InvalidOperation(format!(
                "could not prove complete cleanup of {kind} '{name}': {}",
                details.join("; ")
            )));
        }

        let mut namespace = self.catalog_namespace.lock();
        match namespace.get(name).copied() {
            Some(owner) if owner != kind => {
                return Err(DbError::InvalidOperation(format!(
                    "cannot release cleanup ownership for {kind} '{name}': namespace is owned by {owner}"
                )));
            }
            Some(_) => {
                namespace.remove(name);
            }
            None => {}
        }
        Ok(())
    }

    fn terminal_catalog_cleanup_error(
        &self,
        context: &str,
        name: &str,
        kind: CatalogObjectKind,
        error: &DbError,
    ) -> DbError {
        let reason = format!(
            "[LDB-6044] {context} left {kind} '{name}' incompletely cleaned; this LaminarDB instance is permanently fenced: {error}"
        );
        let was_fenced = self
            .catalog_cleanup_fenced
            .swap(true, std::sync::atomic::Ordering::AcqRel);
        let recorded = {
            let mut fault = self.last_fault.lock();
            if !was_fenced || fault.is_none() {
                *fault = Some(reason);
            }
            fault.clone().unwrap_or_else(|| {
                "[LDB-6044] catalog cleanup failed and the instance is permanently fenced".into()
            })
        };
        DbState::Faulted.store(&self.state);
        self.shutdown_signal.notify_one();
        tracing::error!(reason = %recorded, "catalog cleanup terminally fenced the database");
        DbError::Pipeline(recorded)
    }

    pub(crate) fn rollback_catalog_create(
        &self,
        name: &str,
        kind: CatalogObjectKind,
        context: &str,
    ) -> Result<(), DbError> {
        self.cleanup_catalog_object(name, kind)
            .map_err(|error| self.terminal_catalog_cleanup_error(context, name, kind, &error))
    }

    pub(crate) fn rollback_catalog_create_or_fence(
        &self,
        name: &str,
        kind: CatalogObjectKind,
        context: &str,
    ) {
        if let Err(error) = self.rollback_catalog_create(name, kind, context) {
            tracing::error!(%error, "catalog rollback guard could not complete cleanup");
        }
    }

    pub(crate) fn ensure_catalog_cleanup_unfenced(&self, operation: &str) -> Result<(), DbError> {
        if !self
            .catalog_cleanup_fenced
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(());
        }
        let reason = self.last_fault.lock().clone().unwrap_or_else(|| {
            "[LDB-6044] catalog cleanup is incomplete and this LaminarDB instance is permanently fenced"
                .into()
        });
        Err(DbError::Pipeline(format!(
            "{operation} rejected by terminal catalog cleanup fence: {reason}"
        )))
    }

    /// Streams and materialized views have a synchronous coordinator control path.
    /// Lifecycle transitions never do; a running local, uncheckpointed pipeline is
    /// the only live topology that can admit them safely.
    pub(crate) fn ensure_topology_ddl_allowed(&self, operation: &str) -> Result<(), DbError> {
        self.ensure_catalog_cleanup_unfenced(operation)?;
        if crate::db::catalog_manifest_replay_active() {
            return Ok(());
        }
        match DbState::load(&self.state) {
            DbState::Starting | DbState::ShuttingDown | DbState::Faulted => {
                Err(DbError::Pipeline(format!(
                    "[LDB-6043] {operation} cannot change topology while the pipeline is \
                     starting, shutting down, or faulted; stop the pipeline first"
                )))
            }
            DbState::Running if self.is_cluster_runtime() => Err(DbError::Pipeline(format!(
                "[LDB-6043] {operation} cannot change an active cluster topology until a \
                 replicated topology-version barrier is implemented; stop the pipeline first"
            ))),
            DbState::Running if self.config.checkpoint.is_some() => {
                Err(DbError::Pipeline(format!(
                "[LDB-6043] {operation} cannot change a running checkpointed topology; stop the \
                 pipeline and reset/migrate its checkpoint state first"
            )))
            }
            DbState::Running if self.control_tx.lock().is_none() => {
                Err(DbError::Pipeline(format!(
                    "[LDB-6043] {operation} cannot change a running topology because its control \
                 coordinator is unavailable"
                )))
            }
            DbState::Created | DbState::Running | DbState::Stopped => Ok(()),
        }
    }

    fn apply_without_live_control(
        &self,
        operation: &str,
        mutation: &ControlMutation,
    ) -> Result<(), DbError> {
        if crate::db::catalog_manifest_replay_active()
            || matches!(
                DbState::load(&self.state),
                DbState::Created | DbState::Stopped
            )
        {
            let applied = mutation.try_apply();
            debug_assert!(applied);
            Ok(())
        } else {
            Err(DbError::Pipeline(format!(
                "[LDB-6043] {operation} lost its live control coordinator before admission"
            )))
        }
    }

    /// Connector, source, sink, lookup, continuous-query, and table DDL has no
    /// coordinator control implementation. Reject it in every active runtime mode.
    pub(crate) fn ensure_offline_topology_ddl_allowed(
        &self,
        operation: &str,
    ) -> Result<(), DbError> {
        self.ensure_catalog_cleanup_unfenced(operation)?;
        if crate::db::catalog_manifest_replay_active() {
            return Ok(());
        }
        if matches!(
            DbState::load(&self.state),
            DbState::Starting | DbState::Running | DbState::ShuttingDown | DbState::Faulted
        ) {
            return Err(DbError::Pipeline(format!(
                "[LDB-6043] {operation} is not wired into a live runtime; stop the pipeline first"
            )));
        }
        Ok(())
    }

    /// Resolve `${VAR}` in connector + format options (config vars, then env) and
    /// verify the type is registered + format known — up front, before any
    /// catalog mutation. `None` when no connector is declared.
    fn prepare_connector(
        &self,
        connector_type: Option<&String>,
        connector_options: &HashMap<String, String>,
        format: Option<&laminar_sql::parser::FormatSpec>,
        kind: ConnectorKind,
    ) -> Result<Option<ResolvedConnector>, DbError> {
        let Some(connector_type) = connector_type else {
            if format.is_some() {
                let clause = match kind {
                    ConnectorKind::Source => "FROM",
                    ConnectorKind::Sink => "INTO",
                };
                return Err(DbError::InvalidOperation(format!(
                    "FORMAT requires an explicit {clause} connector"
                )));
            }
            return Ok(None);
        };
        let mut resolved = ResolvedConnector {
            connector_type: Some(connector_type.clone()),
            connector_options: connector_options.clone(),
            format: format.map(|format| format.format_type.clone()),
            format_options: format
                .map(|format| format.options.clone())
                .unwrap_or_default(),
        };
        let kind_name = match kind {
            ConnectorKind::Source => "Source",
            ConnectorKind::Sink => "Sink",
        };
        crate::connector_manager::validate_connector_format_options(
            kind_name,
            &resolved.connector_options,
            resolved.format.as_deref(),
            &resolved.format_options,
        )?;
        let lookup = |name: &str| {
            self.config_vars
                .get(name)
                .cloned()
                .or_else(|| std::env::var(name).ok())
        };
        for value in resolved
            .connector_options
            .values_mut()
            .chain(resolved.format_options.values_mut())
        {
            *value = crate::sql_utils::substitute_vars(value, lookup)?;
        }
        if let Some(ref ct) = resolved.connector_type {
            let normalized = normalize_connector_type(ct);
            let registered = match kind {
                ConnectorKind::Source => self.connector_registry.source_info(&normalized).is_some(),
                ConnectorKind::Sink => self.connector_registry.sink_info(&normalized).is_some(),
            };
            if !registered {
                let (what, available) = match kind {
                    ConnectorKind::Source => ("source", self.connector_registry.list_sources()),
                    ConnectorKind::Sink => ("sink", self.connector_registry.list_sinks()),
                };
                return Err(DbError::Connector(format!(
                    "Unknown {what} connector type '{ct}'. Available: {available:?}"
                )));
            }
            validate_format(resolved.format.as_ref())?;
        }
        Ok(Some(resolved))
    }

    pub(crate) async fn handle_create_source(
        &self,
        create: &laminar_sql::parser::CreateSourceStatement,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_offline_topology_ddl_allowed("CREATE SOURCE")?;
        if create.or_replace {
            return Err(DbError::InvalidOperation(
                "CREATE OR REPLACE SOURCE is not atomic; use DROP SOURCE followed by CREATE SOURCE"
                    .to_string(),
            ));
        }
        let has_connector = create.connector_type.is_some();

        let source_name = canonical_object_name(&create.name)?;
        reject_reserved_namespace(&source_name)?;
        let Some(reservation) = self.reserve_catalog_name(
            &source_name,
            CatalogObjectKind::Source,
            create.if_not_exists,
        )?
        else {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "CREATE SOURCE".to_string(),
                object_name: source_name,
                applied: false,
            }));
        };

        // Validate before mutating catalog/planner — no half-created source on error.
        let resolved = self.prepare_connector(
            create.connector_type.as_ref(),
            &create.connector_options,
            create.format.as_ref(),
            ConnectorKind::Source,
        )?;

        let mut source_def = self
            .build_source_definition(create, resolved.as_ref(), has_connector, &source_name)
            .await?;
        source_def.name.clone_from(&source_name);

        {
            let mut planner = self.planner.lock();
            let stmt = StreamingStatement::CreateSource(Box::new(create.clone()));
            planner.plan(&stmt).map_err(laminar_sql::Error::from)?;
        }

        let entry = self.register_source_entry(create, &source_def)?;
        let name = &source_def.name;

        if let Some(ref entry) = entry {
            let num_partitions = self.ctx.state().config().target_partitions();
            let provider = crate::table_provider::SourceSnapshotProvider::new(
                Arc::clone(entry),
                num_partitions,
            );
            match self
                .ctx
                .register_table(exact_table_reference(name), Arc::new(provider))
            {
                Ok(None) => {}
                Ok(Some(previous)) => {
                    let _ = self
                        .ctx
                        .register_table(exact_table_reference(name), previous);
                    return Err(DbError::InvalidOperation(format!(
                        "cannot create source '{name}': its table provider was claimed concurrently"
                    )));
                }
                Err(error) => {
                    return Err(DbError::InvalidOperation(format!(
                        "failed to register source '{name}' table provider: {error}"
                    )));
                }
            }
        } else {
            return Err(DbError::InvalidOperation(format!(
                "source '{name}' lost its catalog reservation during creation"
            )));
        }

        self.mv_registry.lock().register_base_table(name);

        if let Some(resolved) = resolved {
            if let Some(ct) = resolved.connector_type {
                let mut mgr = self.connector_manager.lock();
                mgr.register_source(crate::connector_manager::SourceRegistration {
                    name: name.clone(),
                    connector_type: Some(ct),
                    connector_options: resolved.connector_options,
                    format: resolved.format,
                    format_options: resolved.format_options,
                });
            }
        }

        reservation.commit();

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE SOURCE".to_string(),
            object_name: name.clone(),
            applied: true,
        }))
    }

    /// Auto-discovers the schema from the connector when no columns are declared.
    async fn build_source_definition(
        &self,
        create: &laminar_sql::parser::CreateSourceStatement,
        resolved: Option<&ResolvedConnector>,
        has_connector: bool,
        source_name: &str,
    ) -> Result<streaming_ddl::SourceDefinition, DbError> {
        if !(create.columns.is_empty() && has_connector) {
            return streaming_ddl::translate_create_source(create.clone())
                .map_err(|e| DbError::Sql(laminar_sql::Error::ParseError(e)));
        }

        let resolved = resolved.expect("has_connector ⇒ Some");
        let connector_type = resolved.connector_type.as_deref().ok_or_else(|| {
            DbError::Config(format!(
                "source '{source_name}': no columns declared and no connector type resolved"
            ))
        })?;
        let normalized = normalize_connector_type(connector_type);

        let mut props = resolved.connector_options.clone();
        if let Some(fmt) = resolved.format.clone() {
            props.insert("format".into(), fmt);
        }
        props.extend(resolved.format_options.clone());

        let discovered = match self
            .connector_registry
            .default_source_schema(&normalized, &props)
            .await
        {
            Ok(Some(s)) => s,
            Ok(None) => {
                return Err(DbError::Config(format!(
                    "source '{source_name}': no columns declared and connector \
                     '{normalized}' could not auto-discover a schema (declare \
                     columns explicitly or check that the format supports \
                     schema discovery)"
                )));
            }
            Err(e) => {
                return Err(DbError::Config(format!(
                    "source '{source_name}': schema auto-discovery failed: {e}"
                )));
            }
        };

        let columns: Vec<ColumnDefinition> = discovered
            .fields()
            .iter()
            .map(|f| ColumnDefinition {
                name: f.name().clone(),
                data_type: f.data_type().clone(),
                nullable: f.is_nullable(),
            })
            .collect();

        streaming_ddl::translate_create_source_with_columns(create.clone(), columns)
            .map_err(|e| DbError::Sql(laminar_sql::Error::ParseError(e)))
    }

    /// `None` when an existing source was kept (`IF NOT EXISTS`).
    fn register_source_entry(
        &self,
        create: &laminar_sql::parser::CreateSourceStatement,
        source_def: &streaming_ddl::SourceDefinition,
    ) -> Result<Option<Arc<crate::catalog::SourceEntry>>, DbError> {
        let name = &source_def.name;
        let schema = source_def.schema.clone();
        let primary_key = source_def.primary_key.clone();
        let watermark_col = source_def.watermark.as_ref().map(|w| w.column.clone());
        let max_ooo = source_def
            .watermark
            .as_ref()
            .map(|w| w.max_out_of_orderness);

        let buffer_size = if source_def.config.buffer_size > 0 {
            Some(source_def.config.buffer_size)
        } else {
            None
        };

        let entry = if create.if_not_exists {
            if self.catalog.get_source(name).is_none() {
                Some(self.catalog.register_source(
                    name,
                    schema,
                    primary_key,
                    watermark_col,
                    max_ooo,
                    buffer_size,
                    None,
                )?)
            } else {
                None
            }
        } else {
            Some(self.catalog.register_source(
                name,
                schema,
                primary_key,
                watermark_col,
                max_ooo,
                buffer_size,
                None,
            )?)
        };

        if let Some(ref wm) = source_def.watermark {
            if wm.is_processing_time {
                if let Some(ref entry) = entry {
                    entry
                        .is_processing_time
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }

        Ok(entry)
    }

    pub(crate) fn handle_create_sink(
        &self,
        create: &laminar_sql::parser::CreateSinkStatement,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_offline_topology_ddl_allowed("CREATE SINK")?;
        if create.or_replace {
            return Err(DbError::InvalidOperation(
                "CREATE OR REPLACE SINK is not atomic; use DROP SINK followed by CREATE SINK"
                    .to_string(),
            ));
        }

        let name = canonical_object_name(&create.name)?;
        reject_reserved_namespace(&name)?;
        let Some(reservation) =
            self.reserve_catalog_name(&name, CatalogObjectKind::Sink, create.if_not_exists)?
        else {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "CREATE SINK".to_string(),
                object_name: name,
                applied: false,
            }));
        };
        let input = match &create.from {
            laminar_sql::parser::SinkFrom::Table(t) => canonical_object_name(t)?,
            laminar_sql::parser::SinkFrom::Query(_) => "query".to_string(),
        };

        // A sink CAN consume an incremental MV's changelog when its connector is upsert- or
        // changelog-capable (e.g. Delta upsert collapses the Z-set via `collapse_changelog`). The
        // capability is only known once the connector is built, so the check is enforced at pipeline
        // start (`pipeline_lifecycle`), not here — a non-capable connector is rejected there with
        // `[LDB-1300]` rather than silently dropping retractions.

        // Validate before mutating catalog/planner — no half-created sink on error.
        let resolved = self.prepare_connector(
            create.connector_type.as_ref(),
            &create.connector_options,
            create.format.as_ref(),
            ConnectorKind::Sink,
        )?;

        {
            let mut planner = self.planner.lock();
            let stmt = StreamingStatement::CreateSink(Box::new(create.clone()));
            planner.plan(&stmt).map_err(laminar_sql::Error::from)?;
        }

        self.catalog.register_sink(&name, &input)?;

        if let Some(resolved) = resolved {
            if let Some(ct) = resolved.connector_type {
                let mut mgr = self.connector_manager.lock();
                mgr.register_sink(crate::connector_manager::SinkRegistration {
                    name: name.clone(),
                    input: input.clone(),
                    connector_type: Some(ct),
                    connector_options: resolved.connector_options,
                    format: resolved.format,
                    format_options: resolved.format_options,
                    filter_expr: create.filter.as_ref().map(std::string::ToString::to_string),
                });
            }
        }

        reservation.commit();

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE SINK".to_string(),
            object_name: name,
            applied: true,
        }))
    }

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

    pub(crate) fn handle_drop_source(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
        cascade: bool,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_offline_topology_ddl_allowed("DROP SOURCE")?;
        let name_str = canonical_object_name(name)?;
        if !self.require_catalog_kind(&name_str, CatalogObjectKind::Source, if_exists)? {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "DROP SOURCE".to_string(),
                object_name: name_str,
                applied: false,
            }));
        }
        let targets = self.build_drop_plan(&name_str, CatalogObjectKind::Source, cascade)?;
        self.teardown_catalog_targets(&targets, "DROP SOURCE")?;
        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP SOURCE".to_string(),
            object_name: name_str,
            applied: true,
        }))
    }

    pub(crate) fn handle_drop_sink(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
        cascade: bool,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_offline_topology_ddl_allowed("DROP SINK")?;
        let name_str = canonical_object_name(name)?;
        if !self.require_catalog_kind(&name_str, CatalogObjectKind::Sink, if_exists)? {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "DROP SINK".to_string(),
                object_name: name_str,
                applied: false,
            }));
        }
        let targets = self.build_drop_plan(&name_str, CatalogObjectKind::Sink, cascade)?;
        self.teardown_catalog_targets(&targets, "DROP SINK")?;
        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP SINK".to_string(),
            object_name: name_str,
            applied: true,
        }))
    }

    // Parsing, admission, catalog mutation, and live-control acknowledgement form one transaction.
    pub(crate) async fn handle_create_stream(
        &self,
        sql: &str,
        name: &sqlparser::ast::ObjectName,
        query: &StreamingStatement,
        emit_clause: Option<&laminar_sql::parser::EmitClause>,
        if_not_exists: bool,
        query_sql: &str,
        retention_bytes: Option<u64>,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_topology_ddl_allowed("CREATE STREAM")?;
        let name_str = canonical_object_name(name)?;
        reject_reserved_namespace(&name_str)?;
        let Some(mut reservation) =
            self.reserve_catalog_name(&name_str, CatalogObjectKind::Stream, if_not_exists)?
        else {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "CREATE STREAM".to_string(),
                object_name: name_str,
                applied: false,
            }));
        };

        self.validate_cluster_query_shape_before_plan("stream", &name_str, query_sql, emit_clause)?;
        let planned =
            self.plan_streaming_query(name, query, emit_clause.cloned(), query_sql, false)?;
        self.reject_unmanaged_temporal_plan(&planned)?;
        self.validate_interval_join_schema(&name_str, query_sql, &planned)
            .await?;
        self.validate_cluster_query_shape("stream", &name_str, query_sql, &planned)
            .await?;
        let PlannedStreamingQuery {
            emit_clause: plan_emit,
            window_config: plan_window,
            order_config: plan_order,
            join_config: plan_joins,
            has_analytic: plan_has_analytic,
            has_frame: plan_has_frame,
        } = planned;

        // A stream over an incremental MV must net the changelog — an aggregate or a simple
        // projection/filter; a complex shape (e.g. a join) is rejected.
        self.reject_unsupported_reading_incremental_mv(query_sql, "a stream")
            .await?;
        let query_sql = query_sql.to_string();

        // The typed namespace reservation prevents rollback from erasing another object.
        self.catalog.register_stream(&name_str)?;
        let mutation = Arc::new(ControlMutation::new());
        reservation.bind_control_mutation(Arc::clone(&mutation));
        let _create_guard = StreamCreateGuard {
            db: self,
            name: name_str.clone(),
            mutation: Arc::clone(&mutation),
        };

        #[cfg(test)]
        let topology_planning_gate = { self.topology_planning_gate.lock().clone() };
        #[cfg(test)]
        if let Some((entered, release)) = topology_planning_gate {
            entered.notify_one();
            release.notified().await;
        }

        let placeholder_schema =
            crate::pipeline_lifecycle::plan_output_schema(&self.ctx, &query_sql).await;

        if let Some(bytes) = retention_bytes {
            let cap = usize::try_from(bytes).unwrap_or(usize::MAX);
            self.subscription_registry.configure(&name_str, cap);
        }

        {
            let mut mgr = self.connector_manager.lock();
            mgr.register_stream(crate::connector_manager::StreamRegistration {
                name: name_str.clone(),
                query_sql: query_sql.clone(),
                emit_clause: plan_emit.clone(),
                window_config: plan_window.clone(),
                order_config: plan_order.clone(),
                join_config: plan_joins.clone(),
                has_analytic: plan_has_analytic,
                has_frame: plan_has_frame,
                incremental: false,
            });
            // Local replay identity participates in the same cancellation guard as
            // graph/catalog admission. Once the coordinator CAS is Applied, caller
            // cancellation must not leave an unreplayable live topology.
            mgr.store_ddl(&name_str, sql);
        }

        // Register as a DataFusion placeholder for plan-time name resolution by downstream MVs.
        if let Some(schema) = placeholder_schema {
            use datafusion::datasource::empty::EmptyTable;
            if let Err(e) = self.ctx.register_table(
                exact_table_reference(&name_str),
                Arc::new(EmptyTable::new(schema)),
            ) {
                return Err(DbError::Pipeline(format!(
                    "could not register stream '{name_str}' for downstream planning: {e}"
                )));
            }
        }

        // Hot-add is acknowledged only after graph admission and wiring complete.
        // The oneshot closes if the pipeline exits, so a rejected/stopped runtime rolls DDL back.
        let admission = {
            let guard = self.control_tx.lock();
            guard.as_ref().map(|tx| {
                let (reply, admission) = tokio::sync::oneshot::channel();
                tx.try_send(crate::pipeline::ControlMsg::add_stream(
                    name_str.clone(),
                    query_sql,
                    plan_emit,
                    plan_window,
                    plan_order,
                    plan_joins,
                    false,
                    reply,
                    Arc::clone(&mutation),
                ))
                .map_err(|e| {
                    DbError::Pipeline(format!(
                        "control channel busy, retry CREATE STREAM '{name_str}': {e}"
                    ))
                })?;
                Ok::<_, DbError>(admission)
            })
        }
        .transpose();
        let admission_result = match admission {
            Ok(Some(admission)) => {
                resolve_control_ack(&format!("CREATE STREAM '{name_str}'"), admission, &mutation)
                    .await
            }
            Ok(None) => {
                self.apply_without_live_control(&format!("CREATE STREAM '{name_str}'"), &mutation)
            }
            Err(error) => Err(error),
        };
        admission_result?;

        reservation.commit();

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE STREAM".to_string(),
            object_name: name_str,
            applied: true,
        }))
    }

    pub(crate) async fn handle_drop_stream(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
        cascade: bool,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_topology_ddl_allowed("DROP STREAM")?;
        let name_str = canonical_object_name(name)?;
        if !self.require_catalog_kind(&name_str, CatalogObjectKind::Stream, if_exists)? {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "DROP STREAM".to_string(),
                object_name: name_str,
                applied: false,
            }));
        }
        let targets = self.build_drop_plan(&name_str, CatalogObjectKind::Stream, cascade)?;
        let graph_names: Vec<String> = targets
            .iter()
            .filter(|target| {
                matches!(
                    target.kind,
                    CatalogObjectKind::Stream | CatalogObjectKind::MaterializedView
                )
            })
            .map(|target| target.name.clone())
            .collect();

        let mutation = Arc::new(ControlMutation::new());
        let drop_guard = StreamingDropGuard {
            db: self,
            targets,
            mutation: Arc::clone(&mutation),
            finished: false,
        };
        self.acknowledge_runtime_drop("DROP STREAM", &graph_names, Arc::clone(&mutation))
            .await?;
        drop_guard.finish()?;

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP STREAM".to_string(),
            object_name: name_str,
            applied: true,
        }))
    }

    async fn acknowledge_runtime_drop(
        &self,
        statement: &str,
        names: &[String],
        mutation: Arc<ControlMutation>,
    ) -> Result<(), DbError> {
        if names.is_empty() {
            return self.apply_without_live_control(statement, &mutation);
        }

        let acknowledgement = {
            let guard = self.control_tx.lock();
            guard.as_ref().map(|tx| {
                let (reply, acknowledgement) = tokio::sync::oneshot::channel();
                tx.try_send(crate::pipeline::ControlMsg::drop_streams(
                    names.to_vec(),
                    reply,
                    Arc::clone(&mutation),
                ))
                .map_err(|error| {
                    DbError::Pipeline(format!("control channel busy, retry {statement}: {error}"))
                })?;
                Ok::<_, DbError>(acknowledgement)
            })
        }
        .transpose()?;

        match acknowledgement {
            Some(acknowledgement) => {
                resolve_control_ack(statement, acknowledgement, &mutation).await
            }
            None => self.apply_without_live_control(statement, &mutation),
        }
    }

    fn direct_dependents(&self, name: &str) -> Result<Vec<CatalogDropTarget>, DbError> {
        let mut names = HashSet::new();
        {
            let manager = self.connector_manager.lock();
            for (stream_name, registration) in manager.streams() {
                if crate::sql_analysis::extract_table_references(&registration.query_sql)
                    .contains(name)
                {
                    names.insert(stream_name.clone());
                }
            }
        }
        for sink_name in self.catalog.list_sinks() {
            if self.catalog.get_sink_input(&sink_name).as_deref() == Some(name) {
                names.insert(sink_name);
            }
        }
        {
            let registry = self.mv_registry.lock();
            names.extend(registry.get_dependents(name).map(str::to_owned));
        }

        let namespace = self.catalog_namespace.lock();
        let mut targets = Vec::with_capacity(names.len());
        for dependent in names {
            let kind = namespace.get(&dependent).copied().ok_or_else(|| {
                DbError::InvalidOperation(format!(
                    "dependent '{dependent}' has no typed catalog owner"
                ))
            })?;
            if !matches!(
                kind,
                CatalogObjectKind::Sink
                    | CatalogObjectKind::Stream
                    | CatalogObjectKind::MaterializedView
            ) {
                return Err(DbError::InvalidOperation(format!(
                    "dependent '{dependent}' has invalid catalog kind {kind}"
                )));
            }
            targets.push(CatalogDropTarget {
                name: dependent,
                kind,
            });
        }
        targets.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(targets)
    }

    fn build_drop_plan(
        &self,
        name: &str,
        expected: CatalogObjectKind,
        cascade: bool,
    ) -> Result<Vec<CatalogDropTarget>, DbError> {
        fn visit(
            db: &LaminarDB,
            target: CatalogDropTarget,
            seen: &mut HashSet<String>,
            result: &mut Vec<CatalogDropTarget>,
        ) -> Result<(), DbError> {
            if !seen.insert(target.name.clone()) {
                return Ok(());
            }
            for dependent in db.direct_dependents(&target.name)? {
                visit(db, dependent, seen, result)?;
            }
            result.push(target);
            Ok(())
        }

        self.require_catalog_kind(name, expected, false)?;
        let direct = self.direct_dependents(name)?;
        if !cascade && !direct.is_empty() {
            let names = direct
                .iter()
                .map(|target| target.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(DbError::InvalidOperation(format!(
                "cannot drop {expected} '{name}': depended on by {names}; use CASCADE"
            )));
        }

        let mut result = Vec::new();
        let mut seen = HashSet::new();
        if cascade {
            for dependent in direct {
                visit(self, dependent, &mut seen, &mut result)?;
            }
        }
        seen.insert(name.to_string());
        result.push(CatalogDropTarget {
            name: name.to_string(),
            kind: expected,
        });

        if DbState::load(&self.state) == DbState::Running
            && result
                .iter()
                .any(|target| target.kind == CatalogObjectKind::Sink)
        {
            return Err(DbError::Pipeline(
                "a running cascade cannot remove sinks because sink teardown is not wired into live control"
                    .into(),
            ));
        }
        Ok(result)
    }

    fn teardown_catalog_targets(
        &self,
        targets: &[CatalogDropTarget],
        context: &str,
    ) -> Result<(), DbError> {
        for target in targets {
            self.rollback_catalog_create(&target.name, target.kind, context)?;
        }
        Ok(())
    }

    fn teardown_catalog_targets_or_fence(&self, targets: &[CatalogDropTarget], context: &str) {
        if let Err(error) = self.teardown_catalog_targets(targets, context) {
            tracing::error!(%error, "catalog teardown guard could not complete cleanup");
        }
    }

    pub(crate) fn handle_set(
        &self,
        set_stmt: &sqlparser::ast::Set,
    ) -> Result<ExecuteResult, DbError> {
        use sqlparser::ast::Set;
        match set_stmt {
            Set::SingleAssignment {
                variable, values, ..
            } => {
                let key = variable.to_string().to_lowercase();
                let value = if values.len() == 1 {
                    values[0].to_string().trim_matches('\'').to_string()
                } else {
                    values
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                };

                if key == "checkpoint_interval" {
                    return self.handle_set_checkpoint_interval(&value);
                }

                self.session_properties.lock().insert(key.clone(), value);
                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "SET".to_string(),
                    object_name: key,
                    applied: true,
                }))
            }
            _ => Err(DbError::InvalidOperation(
                "Only SET key = value syntax is supported".to_string(),
            )),
        }
    }

    pub(crate) fn handle_set_checkpoint_interval(
        &self,
        value: &str,
    ) -> Result<ExecuteResult, DbError> {
        let trimmed = value.trim().to_lowercase();
        let interval = if trimmed == "off" || trimmed == "none" || trimmed == "disabled" {
            None
        } else {
            let duration = parse_duration_str(&trimmed).ok_or_else(|| {
                DbError::InvalidOperation(format!(
                    "Invalid checkpoint_interval: '{value}'. Use a duration like '5s', '1m', '30s', or 'off'."
                ))
            })?;
            Some(duration)
        };

        self.session_properties
            .lock()
            .insert("checkpoint_interval".to_string(), value.to_string());

        tracing::info!(?interval, "Checkpoint interval updated via SET");
        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "SET".to_string(),
            object_name: "checkpoint_interval".to_string(),
            applied: true,
        }))
    }

    fn plan_streaming_query(
        &self,
        name: &sqlparser::ast::ObjectName,
        query: &StreamingStatement,
        emit_clause: Option<laminar_sql::parser::EmitClause>,
        query_sql: &str,
        certify_changelog_enrich: bool,
    ) -> Result<PlannedStreamingQuery, DbError> {
        let admission = if certify_changelog_enrich {
            let incremental_mvs = self.incremental_mv_names();
            if let Some(join) = crate::sql_analysis::detect_changelog_enrich_query(
                query_sql,
                &incremental_mvs,
                &self.static_table_names(),
            ) {
                Some(
                    laminar_sql::planner::ChangelogEnrichAdmission::try_new(
                        join.changelog_table,
                        join.static_table,
                        join.left_keys,
                        join.right_keys,
                        join.left_outer,
                    )
                    .map_err(|error| {
                        DbError::InvalidOperation(format!(
                            "invalid dimension-join admission certificate: {error}"
                        ))
                    })?,
                )
            } else {
                None
            }
        } else {
            None
        };
        let mut planner = self.planner.lock();
        let statement = StreamingStatement::CreateStream {
            name: name.clone(),
            query: Box::new(query.clone()),
            emit_clause,
            or_replace: false,
            if_not_exists: false,
            query_sql: query_sql.to_string(),
            retention_bytes: None,
        };
        let plan_result = if let Some(admission) = admission.as_ref() {
            planner.plan_changelog_enrich(&statement, admission)
        } else {
            planner.plan(&statement)
        };
        let laminar_sql::planner::StreamingPlan::Query(plan) =
            plan_result.map_err(laminar_sql::Error::from)?
        else {
            return Err(DbError::InvalidOperation(format!(
                "planner did not produce a streaming query for '{name}'"
            )));
        };
        Ok(PlannedStreamingQuery {
            emit_clause: plan.emit_clause,
            window_config: plan.window_config,
            order_config: plan.order_config,
            join_config: plan.join_config,
            has_analytic: plan.analytic_config.is_some(),
            has_frame: plan.frame_config.is_some(),
        })
    }

    fn reject_unmanaged_temporal_plan(&self, plan: &PlannedStreamingQuery) -> Result<(), DbError> {
        if plan.join_config.as_ref().is_some_and(|joins| {
            joins.iter().any(|join| {
                matches!(
                    join,
                    laminar_sql::translator::JoinOperatorConfig::Temporal(_)
                )
            })
        }) {
            return Err(DbError::Unsupported(
                "temporal joins require the managed two-input vnode runtime; legacy lookup execution is disabled"
                    .into(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn validate_interval_join_schema(
        &self,
        object_name: &str,
        query_sql: &str,
        plan: &PlannedStreamingQuery,
    ) -> Result<(), DbError> {
        let Some(joins) = &plan.join_config else {
            return Ok(());
        };

        let mut has_interval_join = false;
        for join in joins {
            let laminar_sql::translator::JoinOperatorConfig::StreamStream(config) = join else {
                continue;
            };
            has_interval_join = true;
            let left_schema = self
                .ctx
                .table_provider(exact_table_reference(&config.left_table))
                .await
                .map_err(|error| {
                    DbError::InvalidOperation(format!(
                        "cannot validate interval join '{object_name}' input '{}': {error}",
                        config.left_table
                    ))
                })?
                .schema();
            let right_schema = self
                .ctx
                .table_provider(exact_table_reference(&config.right_table))
                .await
                .map_err(|error| {
                    DbError::InvalidOperation(format!(
                        "cannot validate interval join '{object_name}' input '{}': {error}",
                        config.right_table
                    ))
                })?
                .schema();

            let field = |schema: &Schema, table: &str, column: &str| {
                schema
                    .field_with_name(column)
                    .map(|field| (field.data_type().clone(), field.is_nullable()))
                    .map_err(|_| {
                        DbError::InvalidOperation(format!(
                            "interval join '{object_name}' column '{table}.{column}' does not exist"
                        ))
                    })
            };
            if config.left_keys.is_empty() || config.left_keys.len() != config.right_keys.len() {
                return Err(DbError::InvalidOperation(format!(
                    "interval join '{object_name}' requires non-empty equality-key vectors with matching arity"
                )));
            }
            for (left_column, right_column) in config.left_keys.iter().zip(config.right_keys.iter())
            {
                let (left_key, _) = field(&left_schema, &config.left_table, left_column)?;
                let (right_key, _) = field(&right_schema, &config.right_table, right_column)?;
                if !matches!(&left_key, DataType::Utf8 | DataType::Int64)
                    || !matches!(&right_key, DataType::Utf8 | DataType::Int64)
                    || left_key != right_key
                {
                    return Err(DbError::InvalidOperation(format!(
                        "interval join '{object_name}' key pairs must have the same Utf8 or Int64 type; '{}.{}' is {} and '{}.{}' is {}",
                        config.left_table,
                        left_column,
                        left_key,
                        config.right_table,
                        right_column,
                        right_key
                    )));
                }
            }

            if matches!(
                config.join_type,
                laminar_sql::parser::join_parser::JoinType::Inner
                    | laminar_sql::parser::join_parser::JoinType::Left
                    | laminar_sql::parser::join_parser::JoinType::Right
                    | laminar_sql::parser::join_parser::JoinType::Full
            ) {
                let mut output_names = std::collections::HashSet::new();
                for field in left_schema.fields() {
                    if !output_names.insert(field.name().clone()) {
                        return Err(DbError::InvalidOperation(format!(
                            "interval join '{object_name}' input schema repeats column '{}'",
                            field.name()
                        )));
                    }
                }
                for field in right_schema.fields() {
                    let name = format!("{}_{}", field.name(), config.right_table);
                    if !output_names.insert(name.clone()) {
                        return Err(DbError::InvalidOperation(format!(
                            "interval join '{object_name}' internal output column '{name}' collides; rename the input column or relation"
                        )));
                    }
                }
            }

            for (schema, table, column) in [
                (&left_schema, &config.left_table, &config.left_time_column),
                (
                    &right_schema,
                    &config.right_table,
                    &config.right_time_column,
                ),
            ] {
                let (time, nullable) = field(schema, table, column)?;
                if !matches!(&time, DataType::Timestamp(_, _)) {
                    return Err(DbError::InvalidOperation(format!(
                        "interval join '{object_name}' event-time column '{table}.{column}' must be Timestamp(_), found {}",
                        time
                    )));
                }
                if nullable {
                    return Err(DbError::InvalidOperation(format!(
                        "interval join '{object_name}' event-time column '{table}.{column}' must be declared NOT NULL"
                    )));
                }
            }

            for (side, source_name, time_column) in [
                (
                    "left",
                    config.left_table.as_str(),
                    config.left_time_column.as_str(),
                ),
                (
                    "right",
                    config.right_table.as_str(),
                    config.right_time_column.as_str(),
                ),
            ] {
                let Some(source) = self.catalog.get_source(source_name) else {
                    return Err(DbError::InvalidOperation(format!(
                        "{side} interval join input '{source_name}' must be a directly watermarked source"
                    )));
                };
                if source.watermark_column.as_deref() != Some(time_column)
                    || source
                        .is_processing_time
                        .load(std::sync::atomic::Ordering::Acquire)
                {
                    return Err(DbError::InvalidOperation(format!(
                        "{side} interval join input '{source_name}' must define an event-time watermark on '{time_column}'"
                    )));
                }
            }
        }

        if has_interval_join {
            if crate::sql_analysis::has_unaliased_projection(query_sql) {
                return Err(DbError::InvalidOperation(format!(
                    "interval join '{object_name}' requires every projected expression to have an explicit alias"
                )));
            }
            let dataframe = self.ctx.sql(query_sql).await.map_err(|error| {
                DbError::InvalidOperation(format!(
                    "interval join '{object_name}' could not be validated: {error}"
                ))
            })?;
            if logical_aggregate_stage_count(dataframe.logical_plan()) != 0 {
                return Err(DbError::InvalidOperation(format!(
                    "interval join '{object_name}' cannot contain an aggregate stage"
                )));
            }
        }
        Ok(())
    }

    fn cluster_state_lifecycle_error(object_kind: &str, name: &str, reason: &str) -> DbError {
        DbError::InvalidOperation(format!(
            "[{}] {object_kind} '{name}' is not supported in cluster mode: {reason}",
            laminar_core::error_codes::CLUSTER_STATE_LIFECYCLE_UNSUPPORTED
        ))
    }

    fn validate_cluster_query_shape_before_plan(
        &self,
        object_kind: &str,
        name: &str,
        query_sql: &str,
        emit_clause: Option<&laminar_sql::parser::EmitClause>,
    ) -> Result<(), DbError> {
        use laminar_sql::parser::EmitClause;

        if !self.is_cluster_runtime() {
            return Ok(());
        }
        let reject = |reason: &str| {
            Err(Self::cluster_state_lifecycle_error(
                object_kind,
                name,
                reason,
            ))
        };

        if let Some(aggregate) = distinct_streaming_aggregate(query_sql) {
            return reject(&format!(
                "DISTINCT aggregate '{aggregate}' has unbounded per-key state and no spillable vnode lifecycle"
            ));
        }

        if emit_clause
            .is_some_and(|emit| matches!(emit, EmitClause::OnWindowClose | EmitClause::Final))
        {
            return reject(
                "window-close/final emission has whole-operator window state without a vnode lifecycle",
            );
        }
        if crate::sql_analysis::plan_frame_query(query_sql).is_some() {
            return reject(
                "analytic/window-frame state has no vnode-keyed checkpoint and rebalance lifecycle",
            );
        }
        if !crate::sql_analysis::detect_ai_functions(query_sql).is_empty() {
            return reject(
                "AI inference has checkpointed in-flight rows but no vnode-keyed rebalance lifecycle",
            );
        }
        if !matches!(
            crate::sql_analysis::analyze_temporal_filter(query_sql),
            crate::sql_analysis::TemporalFilterAnalysis::NotPresent
        ) {
            return reject(
                "retracting temporal-filter state has no vnode-keyed checkpoint and rebalance lifecycle",
            );
        }

        if crate::sql_analysis::has_join_clause(query_sql)
            && self.first_incremental_ref(query_sql).is_some()
        {
            return reject(
                "incremental changelog join state has no vnode-keyed checkpoint and rebalance lifecycle",
            );
        }
        if crate::sql_analysis::has_temporal_query(query_sql) {
            return reject(
                "temporal join state has no vnode-keyed checkpoint and rebalance lifecycle",
            );
        }
        Ok(())
    }

    /// Cluster admission is based on configured runtime mode, never the current owner count.
    /// Every stateful route admitted here must implement key shuffle plus vnode capture, restore,
    /// and revoke. Only the bounded interval-join contract is currently admitted.
    pub(crate) async fn validate_cluster_query_shape(
        &self,
        object_kind: &str,
        name: &str,
        query_sql: &str,
        plan: &PlannedStreamingQuery,
    ) -> Result<bool, DbError> {
        use laminar_sql::translator::{JoinOperatorConfig, OrderOperatorConfig};

        if !self.is_cluster_runtime() {
            return Ok(false);
        }
        self.validate_cluster_query_shape_before_plan(
            object_kind,
            name,
            query_sql,
            plan.emit_clause.as_ref(),
        )?;
        let reject = |reason: &str| {
            Err(Self::cluster_state_lifecycle_error(
                object_kind,
                name,
                reason,
            ))
        };

        if plan.has_analytic || plan.has_frame {
            return reject(
                "analytic/window-frame state has no vnode-keyed checkpoint and rebalance lifecycle",
            );
        }
        if plan.window_config.is_some() {
            return reject(
                "windowed aggregate state has no certified watermark eviction lifecycle",
            );
        }
        if plan
            .order_config
            .as_ref()
            .is_some_and(|order| !matches!(order, OrderOperatorConfig::SourceSatisfied))
        {
            return reject(
                "ORDER BY/TOP-K has no distributed merge and vnode-keyed state lifecycle",
            );
        }
        if let Some(joins) = &plan.join_config {
            let [join] = joins.as_slice() else {
                return reject("cluster streaming joins require exactly one two-input stage");
            };
            let JoinOperatorConfig::StreamStream(config) = join else {
                let reason = match join {
                    JoinOperatorConfig::Temporal(_) => {
                        "temporal join state has no vnode-keyed checkpoint and rebalance lifecycle"
                    }
                    JoinOperatorConfig::Lookup(_) => {
                        "lookup join operator and output state have no vnode lifecycle"
                    }
                    JoinOperatorConfig::StreamStream(_) => unreachable!(),
                };
                return reject(reason);
            };
            if config.time_bound.is_zero() || i64::try_from(config.time_bound.as_millis()).is_err()
            {
                return reject(
                    "the distributed join supports only append-only bounded equality joins with a positive finite event-time bound",
                );
            }
            let detected =
                crate::sql_analysis::detect_stream_join_query(query_sql).ok_or_else(|| {
                    Self::cluster_state_lifecycle_error(
                        object_kind,
                        name,
                        "the planner join does not map to the bounded interval-join execution path",
                    )
                })?;
            if detected.config.left_table != config.left_table
                || detected.config.right_table != config.right_table
                || detected.config.join_type != config.join_type
                || detected.config.left_keys != config.left_keys
                || detected.config.right_keys != config.right_keys
                || detected.config.left_time_column != config.left_time_column
                || detected.config.right_time_column != config.right_time_column
                || detected.config.time_bound != config.time_bound
            {
                return reject("planner and interval-join execution metadata disagree");
            }
            #[cfg(feature = "cluster")]
            if self.shuffle_sender.lock().is_none()
                || self.shuffle_receiver.lock().is_none()
                || self.vnode_registry.lock().is_none()
            {
                return reject("interval join has no complete shuffle and vnode ownership scope");
            }
            return Ok(true);
        } else if crate::sql_analysis::detect_stream_join_query(query_sql).is_some() {
            return reject("the planner did not bind the bounded interval-join contract");
        }

        let dataframe = self.ctx.sql(query_sql).await.map_err(|error| {
            Self::cluster_state_lifecycle_error(
                object_kind,
                name,
                &format!("cluster shape could not be validated: {error}"),
            )
        })?;
        let logical_aggregate_stages = logical_aggregate_stage_count(dataframe.logical_plan());
        let physical = self
            .ctx
            .state()
            .create_physical_plan(dataframe.logical_plan())
            .await
            .map_err(|error| {
                Self::cluster_state_lifecycle_error(
                    object_kind,
                    name,
                    &format!("cluster physical plan could not be validated: {error}"),
                )
            })?;
        if contains_builtin_join_without_cluster_lifecycle(&physical) {
            return reject(
                "a built-in DataFusion join has no distributed shuffle and vnode state lifecycle",
            );
        }
        let has_aggregate = match logical_aggregate_stages {
            0 => false,
            1 => true,
            logical => {
                return reject(&format!(
                    "aggregate plan has {logical} logical aggregate stages; cluster admission requires at most one until multi-stage distribution is planner-certified"
                ));
            }
        };
        if has_aggregate {
            #[cfg(feature = "cluster")]
            if self.shuffle_sender.lock().is_none()
                || self.shuffle_receiver.lock().is_none()
                || self.vnode_registry.lock().is_none()
            {
                return reject(
                    "aggregate has no complete distributed shuffle and vnode ownership scope",
                );
            }
            let emit_changelog = plan
                .emit_clause
                .as_ref()
                .is_some_and(|emit| matches!(emit, laminar_sql::parser::EmitClause::Changes));
            let aggregate = match crate::aggregate_state::IncrementalAggState::try_from_sql(
                &self.ctx,
                query_sql,
                emit_changelog,
                self.checkpoint_key_groups(),
            )
            .await
            {
                Ok(Some(aggregate)) => aggregate,
                Ok(None) => {
                    return reject(
                        "aggregate cannot be constructed on the exact incremental execution path; node-local DataFusion fallback would produce partial cluster results",
                    );
                }
                Err(error) => {
                    return reject(&format!(
                        "aggregate incremental execution path could not be constructed: {error}"
                    ));
                }
            };
            let incremental_mvs = self.incremental_mv_names();
            let reads_changelog = crate::sql_analysis::extract_table_references(query_sql)
                .iter()
                .any(|table| incremental_mvs.contains(table));
            if let Some(reason) = aggregate.cluster_state_rejection(reads_changelog) {
                return reject(&reason);
            }
        }
        Ok(has_aggregate)
    }

    /// Register a materialized view and wire it into the running pipeline.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(crate) async fn handle_create_materialized_view(
        &self,
        sql: &str,
        name: &sqlparser::ast::ObjectName,
        query: &StreamingStatement,
        emit_clause: Option<laminar_sql::parser::EmitClause>,
        or_replace: bool,
        if_not_exists: bool,
        query_sql: &str,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_topology_ddl_allowed("CREATE MATERIALIZED VIEW")?;
        let name_str = canonical_object_name(name)?;
        reject_reserved_namespace(&name_str)?;
        if or_replace {
            return Err(DbError::InvalidOperation(
                "CREATE OR REPLACE MATERIALIZED VIEW is not atomic; use DROP MATERIALIZED VIEW followed by CREATE MATERIALIZED VIEW"
                    .into(),
            ));
        }
        if self.is_cluster_runtime() {
            return Err(Self::cluster_state_lifecycle_error(
                "materialized view",
                &name_str,
                "materialized state has no planner-certified distribution and assignment-fenced checkpoint/read lifecycle",
            ));
        }
        let Some(mut reservation) = self.reserve_catalog_name(
            &name_str,
            CatalogObjectKind::MaterializedView,
            if_not_exists,
        )?
        else {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "CREATE MATERIALIZED VIEW".to_string(),
                object_name: name_str,
                applied: false,
            }));
        };

        let planned = self.plan_streaming_query(name, query, emit_clause, query_sql, true)?;
        self.reject_unmanaged_temporal_plan(&planned)?;
        self.validate_interval_join_schema(&name_str, query_sql, &planned)
            .await?;
        let PlannedStreamingQuery {
            emit_clause: plan_emit,
            window_config: plan_window,
            order_config: plan_order,
            join_config: plan_joins,
            has_analytic: plan_has_analytic,
            has_frame: plan_has_frame,
        } = planned;

        let query_sql = query_sql.to_string();
        // A chained MV over an incremental MV must net the changelog — an aggregate or a simple
        // projection/filter; a complex shape (e.g. a join) is rejected.
        self.reject_unsupported_reading_incremental_mv(&query_sql, "a materialized view")
            .await?;
        let schema = self.resolve_mv_schema(&query_sql).await?;
        let sources = self.collect_mv_sources(&query_sql, &name_str);

        {
            let mv =
                laminar_core::mv::MaterializedView::new(&name_str, sql, sources, schema.clone());

            let mut registry = self.mv_registry.lock();

            registry
                .register(mv)
                .map_err(|e| DbError::MaterializedView(e.to_string()))?;
        }
        let mutation = Arc::new(ControlMutation::new());
        reservation.bind_control_mutation(Arc::clone(&mutation));
        let _create_guard = MaterializedViewCreateGuard {
            db: self,
            name: name_str.clone(),
            mutation: Arc::clone(&mutation),
        };

        // An incremental MV emits a dirty-only changelog into a snapshot store; decide the store once
        // so the operator and MV store agree (keyed upsert for aggregates, Z-set for proj/filter).
        let (inc, has_aggregate) = self
            .incremental_emit_mode(&query_sql, plan_window.is_some())
            .await;
        let incremental = !matches!(inc, IncEmit::None);

        {
            let mut mgr = self.connector_manager.lock();
            mgr.register_stream(crate::connector_manager::StreamRegistration {
                name: name_str.clone(),
                query_sql: query_sql.clone(),
                emit_clause: plan_emit.clone(),
                window_config: plan_window.clone(),
                order_config: plan_order.clone(),
                join_config: plan_joins.clone(),
                has_analytic: plan_has_analytic,
                has_frame: plan_has_frame,
                incremental,
            });
            mgr.store_ddl(&name_str, sql);
        }

        self.register_mv_provider(
            &name_str,
            &schema,
            plan_window.is_some(),
            inc,
            has_aggregate,
        )?;

        let admission = {
            let guard = self.control_tx.lock();
            guard.as_ref().map(|tx| {
                let (reply, admission) = tokio::sync::oneshot::channel();
                tx.try_send(crate::pipeline::ControlMsg::add_stream(
                    name_str.clone(),
                    query_sql,
                    plan_emit,
                    plan_window,
                    plan_order,
                    plan_joins,
                    incremental,
                    reply,
                    Arc::clone(&mutation),
                ))
                .map_err(|e| {
                    DbError::Pipeline(format!(
                        "control channel busy, retry CREATE MATERIALIZED VIEW '{name_str}': {e}"
                    ))
                })?;
                Ok::<_, DbError>(admission)
            })
        }
        .transpose();
        let admission_result = match admission {
            Ok(Some(admission)) => {
                resolve_control_ack(
                    &format!("CREATE MATERIALIZED VIEW '{name_str}'"),
                    admission,
                    &mutation,
                )
                .await
            }
            Ok(None) => self.apply_without_live_control(
                &format!("CREATE MATERIALIZED VIEW '{name_str}'"),
                &mutation,
            ),
            Err(error) => Err(error),
        };
        admission_result?;

        reservation.commit();

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE MATERIALIZED VIEW".to_string(),
            object_name: name_str,
            applied: true,
        }))
    }

    /// Falls back to executing the query when static schema planning is unavailable.
    async fn resolve_mv_schema(&self, query_sql: &str) -> Result<Arc<Schema>, DbError> {
        if let Some(s) = crate::pipeline_lifecycle::plan_output_schema(&self.ctx, query_sql).await {
            return Ok(s);
        }
        Ok(match self.handle_query(query_sql).await? {
            ExecuteResult::Query(qh) => qh.schema().clone(),
            _ => Arc::new(Schema::new(vec![Field::new(
                "result",
                DataType::Utf8,
                true,
            )])),
        })
    }

    fn collect_mv_sources(&self, query_sql: &str, name_str: &str) -> Vec<String> {
        let table_refs = crate::sql_analysis::extract_table_references(query_sql);
        let mut sources: Vec<String> = self
            .catalog
            .list_sources()
            .into_iter()
            .filter(|s| table_refs.contains(s.as_str()))
            .collect();
        let registry = self.mv_registry.lock();
        for view in registry.views() {
            if view.name != name_str && table_refs.contains(view.name.as_str()) {
                sources.push(view.name.clone());
            }
        }
        sources
    }

    /// The first table reference in `query_sql` that is an incremental MV, if any.
    fn first_incremental_ref(&self, query_sql: &str) -> Option<String> {
        let refs = crate::sql_analysis::extract_table_references(query_sql);
        let mgr = self.connector_manager.lock();
        refs.iter()
            .find(|r| mgr.streams().get(r.as_str()).is_some_and(|s| s.incremental))
            .cloned()
    }

    /// All registered incremental MV (changelog producer) names.
    fn incremental_mv_names(&self) -> rustc_hash::FxHashSet<String> {
        self.connector_manager
            .lock()
            .streams()
            .iter()
            .filter(|(_, r)| r.incremental)
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// Static (reference/dimension) table names — valid right sides for a changelog enrich join.
    fn static_table_names(&self) -> rustc_hash::FxHashSet<String> {
        self.table_store.read().table_names().into_iter().collect()
    }

    /// A query reading an incremental MV must net the retraction changelog — an aggregate or a
    /// simple projection/filter; complex shapes (e.g. joins) mishandle retractions and are rejected.
    async fn reject_unsupported_reading_incremental_mv(
        &self,
        query_sql: &str,
        consumer: &str,
    ) -> Result<(), DbError> {
        let Some(mv) = self.first_incremental_ref(query_sql) else {
            return Ok(());
        };
        // A changelog may enrich against a static table. Every other join shape is rejected;
        // aggregates and simple projection/filter consumers continue to net retractions.
        let inc = self.incremental_mv_names();
        let changelog_enrich = crate::sql_analysis::detect_changelog_enrich_query(
            query_sql,
            &inc,
            &self.static_table_names(),
        )
        .is_some();
        if crate::sql_analysis::has_join_clause(query_sql) && !changelog_enrich {
            return Err(incremental_mv_consumer_error(&mv, consumer));
        }
        let supported = changelog_enrich
            || self.ctx.sql(query_sql).await.ok().is_some_and(|df| {
                let plan = df.logical_plan();
                crate::aggregate_state::find_aggregate(plan).is_some()
                    || crate::sql_analysis::extract_projection_filter(plan).is_some()
            });
        if supported {
            Ok(())
        } else {
            Err(incremental_mv_consumer_error(&mv, consumer))
        }
    }

    /// Store decision for a non-windowed MV: keyed `Upsert` for a keyed aggregate, `Multiset` for a
    /// projection/filter over a changelog, `None` (full-emit) otherwise (incl. global aggregates).
    /// Returns the store mode and whether the query is an aggregate (threaded to
    /// `register_mv_provider` so it needn't re-plan to pick `Aggregate` vs append storage).
    async fn incremental_emit_mode(&self, query_sql: &str, has_window: bool) -> (IncEmit, bool) {
        if has_window {
            return (IncEmit::None, false);
        }
        let flag = self.config.incremental_emit;
        let reads_incremental = self.first_incremental_ref(query_sql).is_some();
        let Some(df) = self.ctx.sql(query_sql).await.ok() else {
            return (IncEmit::None, false);
        };
        let plan = df.logical_plan();
        if let Some(agg) = crate::aggregate_state::find_aggregate(plan) {
            let n = agg.group_exprs.len();
            // Keyed aggregate → upsert (terminal under the flag, or chained over an incremental
            // MV). A global aggregate (no GROUP BY) is single-row → full-emit.
            let inc = if n > 0 && (flag || reads_incremental) {
                IncEmit::Upsert((0..n).collect())
            } else {
                IncEmit::None
            };
            return (inc, true);
        }
        // Projection/filter over an incremental MV's changelog → Z-set multiset snapshot.
        if reads_incremental && crate::sql_analysis::extract_projection_filter(plan).is_some() {
            return (IncEmit::Multiset, false);
        }
        // `changelog ⋈ static dim` enrich join → Z-set multiset snapshot.
        if reads_incremental
            && crate::sql_analysis::detect_changelog_enrich_query(
                query_sql,
                &self.incremental_mv_names(),
                &self.static_table_names(),
            )
            .is_some()
        {
            return (IncEmit::Multiset, false);
        }
        (IncEmit::None, false)
    }

    fn register_mv_provider(
        &self,
        name_str: &str,
        schema: &Arc<Schema>,
        has_window: bool,
        inc: IncEmit,
        has_aggregate: bool,
    ) -> Result<(), DbError> {
        use crate::mv_store::MvStorageMode;

        // Incremental MVs maintain a snapshot from a dirty-only changelog. Otherwise: non-windowed
        // aggs replace-all every cycle; windowed aggs append (preserving prior windows), as do non-aggregates.
        let mode = match inc {
            IncEmit::Upsert(key_cols) => MvStorageMode::Upsert { key_cols },
            IncEmit::Multiset => MvStorageMode::Multiset,
            IncEmit::None if has_aggregate && !has_window => MvStorageMode::Aggregate,
            IncEmit::None => MvStorageMode::append_default(),
        };

        self.mv_store
            .write()
            .create_mv(name_str, schema.clone(), mode)?;

        let provider: Arc<dyn datafusion::datasource::TableProvider> =
            Arc::new(crate::table_provider::MvTableProvider::new(
                name_str.to_string(),
                schema.clone(),
                self.mv_store.clone(),
            ));

        match self
            .ctx
            .register_table(exact_table_reference(name_str), provider)
        {
            Ok(None) => Ok(()),
            Ok(Some(previous)) => {
                let _ = self
                    .ctx
                    .register_table(exact_table_reference(name_str), previous);
                Err(DbError::MaterializedView(format!(
                    "cannot create materialized view '{name_str}': the table namespace was \
                     claimed concurrently"
                )))
            }
            Err(error) => Err(DbError::MaterializedView(format!(
                "Failed to register MV table provider: {error}"
            ))),
        }
    }

    /// Handle DROP MATERIALIZED VIEW statement.
    pub(crate) async fn handle_drop_materialized_view(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
        cascade: bool,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_topology_ddl_allowed("DROP MATERIALIZED VIEW")?;
        let name_str = canonical_object_name(name)?;
        if !self.require_catalog_kind(&name_str, CatalogObjectKind::MaterializedView, if_exists)? {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "DROP MATERIALIZED VIEW".to_string(),
                object_name: name_str,
                applied: false,
            }));
        }
        let targets =
            self.build_drop_plan(&name_str, CatalogObjectKind::MaterializedView, cascade)?;
        let graph_names: Vec<String> = targets
            .iter()
            .filter(|target| {
                matches!(
                    target.kind,
                    CatalogObjectKind::Stream | CatalogObjectKind::MaterializedView
                )
            })
            .map(|target| target.name.clone())
            .collect();

        let mutation = Arc::new(ControlMutation::new());
        let drop_guard = StreamingDropGuard {
            db: self,
            targets,
            mutation: Arc::clone(&mutation),
            finished: false,
        };
        self.acknowledge_runtime_drop(
            "DROP MATERIALIZED VIEW",
            &graph_names,
            Arc::clone(&mutation),
        )
        .await?;
        drop_guard.finish()?;

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP MATERIALIZED VIEW".to_string(),
            object_name: name_str,
            applied: true,
        }))
    }

    pub(crate) fn handle_drop_lookup_table(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_offline_topology_ddl_allowed("DROP LOOKUP TABLE")?;
        let name = canonical_object_name(name)?;
        if !self.require_catalog_kind(&name, CatalogObjectKind::LookupTable, if_exists)? {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "DROP LOOKUP TABLE".into(),
                object_name: name,
                applied: false,
            }));
        }
        let targets = self.build_drop_plan(&name, CatalogObjectKind::LookupTable, false)?;
        self.teardown_catalog_targets(&targets, "DROP LOOKUP TABLE")?;
        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP LOOKUP TABLE".into(),
            object_name: name,
            applied: true,
        }))
    }

    /// Handle DROP TABLE statement.
    pub(crate) fn handle_drop_table(
        &self,
        names: &[sqlparser::ast::ObjectName],
        if_exists: bool,
        cascade: bool,
    ) -> Result<ExecuteResult, DbError> {
        self.ensure_offline_topology_ddl_allowed("DROP TABLE")?;
        let mut plans = Vec::new();
        for obj_name in names {
            let name_str = canonical_object_name(obj_name)?;
            if self.require_catalog_kind(&name_str, CatalogObjectKind::Table, if_exists)? {
                plans.push(self.build_drop_plan(&name_str, CatalogObjectKind::Table, cascade)?);
            }
        }

        let mut seen = HashSet::new();
        let targets: Vec<_> = plans
            .into_iter()
            .flatten()
            .filter(|target| seen.insert(target.name.clone()))
            .collect();
        let applied = !targets.is_empty();
        self.teardown_catalog_targets(&targets, "DROP TABLE")?;

        let name = names
            .first()
            .map(std::string::ToString::to_string)
            .unwrap_or_default();

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP TABLE".to_string(),
            object_name: name,
            applied,
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

/// Normalized connector info from an explicit source `FROM` or sink `INTO` clause.
pub(crate) struct ResolvedConnector {
    pub connector_type: Option<String>,
    pub connector_options: HashMap<String, String>,
    pub format: Option<String>,
    pub format_options: HashMap<String, String>,
}

/// Validate that a resolved format string is known.
pub(crate) fn validate_format(format: Option<&String>) -> Result<(), DbError> {
    if let Some(fmt_str) = format {
        laminar_connectors::serde::Format::parse(&fmt_str.to_lowercase())
            .map_err(|e| DbError::Connector(format!("Unknown format '{fmt_str}': {e}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod create_table_shape_tests;
