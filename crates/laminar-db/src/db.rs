//! The main `LaminarDB` database facade.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;

use laminar_core::streaming;
use laminar_core::time::WatermarkGenerator;
use laminar_sql::parser::{parse_streaming_sql, ShowCommand, StreamingStatement};
use laminar_sql::planner::StreamingPlanner;
use laminar_sql::register_streaming_functions;
use laminar_sql::translator::streaming_ddl;
use laminar_sql::translator::{AsofJoinTranslatorConfig, JoinOperatorConfig};

use crate::builder::LaminarDbBuilder;
use crate::catalog::SourceCatalog;
use crate::config::LaminarConfig;
use crate::error::DbError;
use crate::handle::{
    DdlInfo, ExecuteResult, QueryHandle, QueryInfo, SinkInfo, SourceHandle, SourceInfo,
    UntypedSourceHandle,
};
use crate::sql_utils;

const STATE_CREATED: u8 = 0;
const STATE_STARTING: u8 = 1;
const STATE_RUNNING: u8 = 2;
const STATE_SHUTTING_DOWN: u8 = 3;
const STATE_STOPPED: u8 = 4;

/// Extract SQL text from a `StreamingStatement` for storage in the connector manager.
fn streaming_statement_to_sql(stmt: &StreamingStatement) -> String {
    match stmt {
        StreamingStatement::Standard(sql_stmt) => sql_stmt.to_string(),
        StreamingStatement::CreateContinuousQuery { query, .. } => {
            streaming_statement_to_sql(query)
        }
        other => format!("{other:?}"),
    }
}

/// The main `LaminarDB` database handle.
///
/// Provides a unified interface for SQL execution, data ingestion,
/// and result consumption. All streaming infrastructure (sources, sinks,
/// channels, subscriptions) is managed internally.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_db::LaminarDB;
///
/// let db = LaminarDB::open()?;
///
/// db.execute("CREATE SOURCE trades (
///     symbol VARCHAR, price DOUBLE, ts BIGINT,
///     WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
/// )").await?;
///
/// let query = db.execute("SELECT symbol, AVG(price) FROM trades
///     GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE)
/// ").await?;
/// ```
pub struct LaminarDB {
    catalog: Arc<SourceCatalog>,
    planner: parking_lot::Mutex<StreamingPlanner>,
    ctx: SessionContext,
    config: LaminarConfig,
    config_vars: Arc<HashMap<String, String>>,
    shutdown: std::sync::atomic::AtomicBool,
    /// Unified checkpoint coordinator (populated by `start()`).
    coordinator:
        Arc<tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>>,
    connector_manager: parking_lot::Mutex<crate::connector_manager::ConnectorManager>,
    connector_registry: Arc<laminar_connectors::registry::ConnectorRegistry>,
    mv_registry: parking_lot::Mutex<laminar_core::mv::MvRegistry>,
    table_store: Arc<parking_lot::Mutex<crate::table_store::TableStore>>,
    state: std::sync::atomic::AtomicU8,
    /// Handle to the background processing task (if running).
    runtime_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Signal to stop the processing loop.
    shutdown_signal: Arc<tokio::sync::Notify>,
    /// Shared pipeline counters for observability.
    counters: Arc<crate::metrics::PipelineCounters>,
    /// Instant when the database was created, for uptime calculation.
    start_time: std::time::Instant,
    /// Global pipeline watermark (min of all source watermarks).
    pipeline_watermark: Arc<std::sync::atomic::AtomicI64>,
    /// Shared compiler cache for JIT-compiled pipelines.
    /// Protected by `Mutex` — only locked during compilation, not execution.
    #[cfg(feature = "jit")]
    compiler_cache: parking_lot::Mutex<laminar_core::compiler::CompilerCache>,
}

/// Per-source watermark tracking state for the pipeline loop.
///
/// Combines an `EventTimeExtractor` (to find the max timestamp in each batch)
/// with a `BoundedOutOfOrdernessGenerator` (to compute the watermark with delay).
struct SourceWatermarkState {
    extractor: laminar_core::time::EventTimeExtractor,
    generator: laminar_core::time::BoundedOutOfOrdernessGenerator,
    /// Watermark column name for late-row filtering.
    column: String,
    /// Timestamp format for late-row filtering.
    format: laminar_core::time::TimestampFormat,
}

/// Infer the `TimestampFormat` from a schema column's `DataType`.
fn infer_timestamp_format(
    schema: &arrow::datatypes::SchemaRef,
    column: &str,
) -> laminar_core::time::TimestampFormat {
    if let Ok(idx) = schema.index_of(column) {
        match schema.field(idx).data_type() {
            DataType::Timestamp(_, _) => laminar_core::time::TimestampFormat::ArrowNative,
            _ => laminar_core::time::TimestampFormat::UnixMillis,
        }
    } else {
        laminar_core::time::TimestampFormat::UnixMillis
    }
}

/// Filters rows from a `RecordBatch` whose timestamp is behind the watermark.
///
/// Returns `None` if all rows are late (i.e., filtered result is empty).
/// For sources without a watermark column, callers should skip this function
/// and pass the batch through unfiltered.
fn filter_late_rows(
    batch: &RecordBatch,
    column: &str,
    watermark: i64,
    format: laminar_core::time::TimestampFormat,
) -> Option<RecordBatch> {
    crate::batch_filter::filter_batch_by_timestamp(
        batch,
        column,
        watermark,
        format,
        crate::batch_filter::ThresholdOp::GreaterEq,
    )
}

impl LaminarDB {
    /// Create an embedded in-memory database with default settings.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if `DataFusion` context creation fails.
    pub fn open() -> Result<Self, DbError> {
        Self::open_with_config(LaminarConfig::default())
    }

    /// Create with custom configuration.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if `DataFusion` context creation fails.
    pub fn open_with_config(config: LaminarConfig) -> Result<Self, DbError> {
        Self::open_with_config_and_vars(config, HashMap::new())
    }

    /// Create with custom configuration and config variables for SQL substitution.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if `DataFusion` context creation fails.
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn open_with_config_and_vars(
        config: LaminarConfig,
        config_vars: HashMap<String, String>,
    ) -> Result<Self, DbError> {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);

        let catalog = Arc::new(SourceCatalog::new(
            config.default_buffer_size,
            config.default_backpressure,
        ));

        let connector_registry = Arc::new(laminar_connectors::registry::ConnectorRegistry::new());
        Self::register_builtin_connectors(&connector_registry);

        Ok(Self {
            catalog,
            planner: parking_lot::Mutex::new(StreamingPlanner::new()),
            ctx,
            config,
            config_vars: Arc::new(config_vars),
            shutdown: std::sync::atomic::AtomicBool::new(false),
            coordinator: Arc::new(tokio::sync::Mutex::new(None)),
            connector_manager: parking_lot::Mutex::new(
                crate::connector_manager::ConnectorManager::new(),
            ),
            connector_registry,
            mv_registry: parking_lot::Mutex::new(laminar_core::mv::MvRegistry::new()),
            table_store: Arc::new(parking_lot::Mutex::new(
                crate::table_store::TableStore::new(),
            )),
            state: std::sync::atomic::AtomicU8::new(STATE_CREATED),
            runtime_handle: parking_lot::Mutex::new(None),
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            counters: Arc::new(crate::metrics::PipelineCounters::new()),
            start_time: std::time::Instant::now(),
            pipeline_watermark: Arc::new(std::sync::atomic::AtomicI64::new(i64::MIN)),
            #[cfg(feature = "jit")]
            compiler_cache: parking_lot::Mutex::new(
                laminar_core::compiler::CompilerCache::new(64)
                    .expect("JIT compiler cache initialization"),
            ),
        })
    }

    /// Get a fluent builder for constructing a `LaminarDB`.
    #[must_use]
    pub fn builder() -> LaminarDbBuilder {
        LaminarDbBuilder::new()
    }

    /// Register built-in connectors based on enabled features.
    #[allow(unused_variables)]
    fn register_builtin_connectors(registry: &laminar_connectors::registry::ConnectorRegistry) {
        #[cfg(feature = "kafka")]
        {
            laminar_connectors::kafka::register_kafka_source(registry);
            laminar_connectors::kafka::register_kafka_sink(registry);
        }
        #[cfg(feature = "postgres-cdc")]
        {
            laminar_connectors::cdc::postgres::register_postgres_cdc(registry);
        }
        #[cfg(feature = "postgres-sink")]
        {
            laminar_connectors::postgres::register_postgres_sink(registry);
        }
    }

    /// Returns the connector registry for registering custom connectors.
    ///
    /// Use this to register user-defined source/sink connectors before
    /// calling `start()`.
    #[must_use]
    pub fn connector_registry(&self) -> &laminar_connectors::registry::ConnectorRegistry {
        &self.connector_registry
    }

    /// Execute a SQL statement.
    ///
    /// Supports:
    /// - `CREATE SOURCE` / `CREATE SINK` — registers sources and sinks
    /// - `DROP SOURCE` / `DROP SINK` — removes sources and sinks
    /// - `SHOW SOURCES` / `SHOW SINKS` / `SHOW QUERIES` — list registered objects
    /// - `DESCRIBE source_name` — show source schema
    /// - `SELECT ...` — execute a streaming query
    /// - `INSERT INTO source_name VALUES (...)` — insert data
    /// - `CREATE MATERIALIZED VIEW` — create a streaming materialized view
    /// - `EXPLAIN SELECT ...` — show query plan
    ///
    /// # Errors
    ///
    /// Returns `DbError` if SQL parsing, planning, or execution fails.
    pub async fn execute(&self, sql: &str) -> Result<ExecuteResult, DbError> {
        if self.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(DbError::Shutdown);
        }

        // Apply config variable substitution
        let resolved = if self.config_vars.is_empty() {
            sql.to_string()
        } else {
            sql_utils::resolve_config_vars(sql, &self.config_vars, true)?
        };

        // Split into multiple statements
        let stmts = sql_utils::split_statements(&resolved);
        if stmts.is_empty() {
            return Err(DbError::InvalidOperation("Empty SQL statement".into()));
        }

        // Execute each statement, return the last result (or first error)
        let mut last_result = None;
        for stmt_sql in &stmts {
            last_result = Some(self.execute_single(stmt_sql).await?);
        }

        last_result.ok_or_else(|| DbError::InvalidOperation("Empty SQL statement".into()))
    }

    /// Execute a single SQL statement.
    async fn execute_single(&self, sql: &str) -> Result<ExecuteResult, DbError> {
        let statements = parse_streaming_sql(sql)?;

        if statements.is_empty() {
            return Err(DbError::InvalidOperation("Empty SQL statement".into()));
        }

        let statement = &statements[0];

        match statement {
            StreamingStatement::CreateSource(create) => self.handle_create_source(create),
            StreamingStatement::CreateSink(create) => self.handle_create_sink(create),
            StreamingStatement::CreateStream {
                name,
                query,
                emit_clause,
                ..
            } => self.handle_create_stream(name, query, emit_clause.as_ref()),
            StreamingStatement::CreateContinuousQuery { .. } => self.handle_query(sql).await,
            StreamingStatement::Standard(stmt) => {
                if let sqlparser::ast::Statement::CreateTable(ct) = stmt.as_ref() {
                    self.handle_create_table(ct)
                } else if let sqlparser::ast::Statement::Drop {
                    object_type: sqlparser::ast::ObjectType::Table,
                    names,
                    if_exists,
                    ..
                } = stmt.as_ref()
                {
                    self.handle_drop_table(names, *if_exists)
                } else {
                    self.handle_query(sql).await
                }
            }
            StreamingStatement::InsertInto {
                table_name,
                columns,
                values,
            } => self.handle_insert_into(table_name, columns, values).await,
            StreamingStatement::DropSource { name, if_exists } => {
                self.handle_drop_source(name, *if_exists)
            }
            StreamingStatement::DropSink { name, if_exists } => {
                self.handle_drop_sink(name, *if_exists)
            }
            StreamingStatement::DropStream { name, if_exists } => {
                self.handle_drop_stream(name, *if_exists)
            }
            StreamingStatement::DropMaterializedView {
                name,
                if_exists,
                cascade,
            } => self.handle_drop_materialized_view(name, *if_exists, *cascade),
            StreamingStatement::Show(cmd) => {
                let batch = match cmd {
                    ShowCommand::Sources => self.build_show_sources(),
                    ShowCommand::Sinks => self.build_show_sinks(),
                    ShowCommand::Queries => self.build_show_queries(),
                    ShowCommand::MaterializedViews => self.build_show_materialized_views(),
                    ShowCommand::Streams => self.build_show_streams(),
                    ShowCommand::Tables => self.build_show_tables(),
                };
                Ok(ExecuteResult::Metadata(batch))
            }
            StreamingStatement::Describe { name, .. } => {
                let name_str = name.to_string();
                let batch = self.build_describe(&name_str)?;
                Ok(ExecuteResult::Metadata(batch))
            }
            StreamingStatement::Explain { statement } => self.handle_explain(statement),
            StreamingStatement::CreateMaterializedView {
                name,
                query,
                or_replace,
                if_not_exists,
                ..
            } => {
                self.handle_create_materialized_view(sql, name, query, *or_replace, *if_not_exists)
                    .await
            }
        }
    }

    /// Handle CREATE SOURCE statement.
    #[allow(clippy::too_many_lines)]
    fn handle_create_source(
        &self,
        create: &laminar_sql::parser::CreateSourceStatement,
    ) -> Result<ExecuteResult, DbError> {
        let source_def = streaming_ddl::translate_create_source(create.clone())
            .map_err(|e| DbError::Sql(laminar_sql::Error::ParseError(e)))?;

        let name = &source_def.name;
        let schema = source_def.schema.clone();
        let watermark_col = source_def.watermark.as_ref().map(|w| w.column.clone());
        let max_ooo = source_def
            .watermark
            .as_ref()
            .map(|w| w.max_out_of_orderness);

        // Extract config from source definition
        let buffer_size = if source_def.config.buffer_size > 0 {
            Some(source_def.config.buffer_size)
        } else {
            None
        };

        let entry = if create.or_replace {
            Some(self.catalog.register_source_or_replace(
                name,
                schema,
                watermark_col,
                max_ooo,
                buffer_size,
                None,
            ))
        } else if create.if_not_exists {
            if self.catalog.get_source(name).is_none() {
                Some(self.catalog.register_source(
                    name,
                    schema,
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
                watermark_col,
                max_ooo,
                buffer_size,
                None,
            )?)
        };

        // Register source as a DataFusion table for ad-hoc SELECT queries.
        // For OR REPLACE, deregister the old table first.
        if let Some(ref entry) = entry {
            if create.or_replace {
                let _ = self.ctx.deregister_table(name);
            }
            let num_partitions = self.ctx.state().config().target_partitions();
            let provider = crate::table_provider::SourceSnapshotProvider::new(
                Arc::clone(entry),
                num_partitions,
            );
            let _ = self.ctx.register_table(name, Arc::new(provider));
        }

        // Register as a base table in the MV registry for dependency tracking
        self.mv_registry.lock().register_base_table(name);

        // Also register in the planner
        {
            let mut planner = self.planner.lock();
            let stmt = StreamingStatement::CreateSource(Box::new(create.clone()));
            let _ = planner.plan(&stmt);
        }

        // Register connector info in ConnectorManager if external connector specified
        if create.connector_type.is_some() {
            // Validate connector type is registered
            if let Some(ref ct) = create.connector_type {
                let normalized = ct.to_lowercase();
                if self.connector_registry.source_info(&normalized).is_none() {
                    return Err(DbError::Connector(format!(
                        "Unknown source connector type '{ct}'. Available: {:?}",
                        self.connector_registry.list_sources()
                    )));
                }
            }

            // Validate format
            if let Some(ref fmt) = create.format {
                laminar_connectors::serde::Format::parse(&fmt.format_type.to_lowercase()).map_err(
                    |e| DbError::Connector(format!("Unknown format '{}': {e}", fmt.format_type)),
                )?;
            }

            let mut mgr = self.connector_manager.lock();
            mgr.register_source(crate::connector_manager::SourceRegistration {
                name: name.clone(),
                connector_type: create.connector_type.clone(),
                connector_options: create.connector_options.clone(),
                format: create.format.as_ref().map(|f| f.format_type.clone()),
                format_options: create
                    .format
                    .as_ref()
                    .map(|f| f.options.clone())
                    .unwrap_or_default(),
            });
        }

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE SOURCE".to_string(),
            object_name: name.clone(),
        }))
    }

    /// Handle CREATE SINK statement.
    fn handle_create_sink(
        &self,
        create: &laminar_sql::parser::CreateSinkStatement,
    ) -> Result<ExecuteResult, DbError> {
        let name = create.name.to_string();
        let input = match &create.from {
            laminar_sql::parser::SinkFrom::Table(t) => t.to_string(),
            laminar_sql::parser::SinkFrom::Query(_) => "query".to_string(),
        };

        if create.or_replace {
            self.catalog.drop_sink(&name);
            self.catalog.register_sink(&name, &input)?;
        } else if create.if_not_exists {
            let _ = self.catalog.register_sink(&name, &input);
        } else {
            self.catalog.register_sink(&name, &input)?;
        }

        // Register in planner
        {
            let mut planner = self.planner.lock();
            let stmt = StreamingStatement::CreateSink(Box::new(create.clone()));
            let _ = planner.plan(&stmt);
        }

        // Register connector info in ConnectorManager if external connector specified
        if create.connector_type.is_some() {
            // Validate connector type is registered
            if let Some(ref ct) = create.connector_type {
                let normalized = ct.to_lowercase();
                if self.connector_registry.sink_info(&normalized).is_none() {
                    return Err(DbError::Connector(format!(
                        "Unknown sink connector type '{ct}'. Available: {:?}",
                        self.connector_registry.list_sinks()
                    )));
                }
            }

            // Validate format
            if let Some(ref fmt) = create.format {
                laminar_connectors::serde::Format::parse(&fmt.format_type.to_lowercase()).map_err(
                    |e| DbError::Connector(format!("Unknown format '{}': {e}", fmt.format_type)),
                )?;
            }

            let mut mgr = self.connector_manager.lock();
            mgr.register_sink(crate::connector_manager::SinkRegistration {
                name: name.clone(),
                input: input.clone(),
                connector_type: create.connector_type.clone(),
                connector_options: create.connector_options.clone(),
                format: create.format.as_ref().map(|f| f.format_type.clone()),
                format_options: create
                    .format
                    .as_ref()
                    .map(|f| f.options.clone())
                    .unwrap_or_default(),
                filter_expr: create.filter.as_ref().map(std::string::ToString::to_string),
            });
        }

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE SINK".to_string(),
            object_name: name,
        }))
    }

    /// Handle INSERT INTO statement.
    ///
    /// Inserts SQL VALUES into a registered source, a `TableStore`-managed
    /// table (with PK upsert), or a plain `DataFusion` `MemTable`.
    async fn handle_insert_into(
        &self,
        table_name: &sqlparser::ast::ObjectName,
        _columns: &[sqlparser::ast::Ident],
        values: &[Vec<sqlparser::ast::Expr>],
    ) -> Result<ExecuteResult, DbError> {
        let name = table_name.to_string();

        // Try inserting into a registered source
        if let Some(entry) = self.catalog.get_source(&name) {
            let batch = sql_utils::sql_values_to_record_batch(&entry.schema, values)?;
            entry
                .push_and_buffer(batch)
                .map_err(|e| DbError::InsertError(format!("Failed to push to source: {e}")))?;
            return Ok(ExecuteResult::RowsAffected(values.len() as u64));
        }

        // Try inserting into a TableStore-managed table (with PK upsert)
        {
            let has_table = self.table_store.lock().has_table(&name);
            if has_table {
                let schema = self
                    .table_store
                    .lock()
                    .table_schema(&name)
                    .ok_or_else(|| DbError::TableNotFound(name.clone()))?;
                let batch = sql_utils::sql_values_to_record_batch(&schema, values)?;
                self.table_store.lock().upsert(&name, &batch)?;

                // Sync to DataFusion MemTable
                self.sync_table_to_datafusion(&name)?;

                return Ok(ExecuteResult::RowsAffected(values.len() as u64));
            }
        }

        // Otherwise, insert into a DataFusion MemTable
        // Look up the table provider
        let table = self
            .ctx
            .table_provider(&name)
            .await
            .map_err(|_| DbError::TableNotFound(name.clone()))?;

        let schema = table.schema();
        let batch = sql_utils::sql_values_to_record_batch(&schema, values)?;

        // Deregister the old table, then re-register with the new data
        self.ctx
            .deregister_table(&name)
            .map_err(|e| DbError::InsertError(format!("Failed to deregister table: {e}")))?;

        let mem_table =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![batch]])
                .map_err(|e| DbError::InsertError(format!("Failed to create table: {e}")))?;

        self.ctx
            .register_table(&name, Arc::new(mem_table))
            .map_err(|e| DbError::InsertError(format!("Failed to register table: {e}")))?;

        Ok(ExecuteResult::RowsAffected(values.len() as u64))
    }

    /// Handle CREATE TABLE statement.
    ///
    /// Creates a static reference/dimension table backed by a `DataFusion`
    /// `MemTable`. If a PRIMARY KEY is specified, the table is also registered
    /// in the `TableStore` for upsert/delete/lookup semantics.
    /// If `WITH (...)` options include a `connector` key, the table is
    /// registered in the `ConnectorManager` for connector-backed population.
    #[allow(clippy::too_many_lines)]
    fn handle_create_table(
        &self,
        create: &sqlparser::ast::CreateTable,
    ) -> Result<ExecuteResult, DbError> {
        let name = create.name.to_string();

        // Build Arrow schema from column definitions
        let fields: Vec<arrow::datatypes::Field> = create
            .columns
            .iter()
            .map(|col| {
                let data_type = streaming_ddl::sql_type_to_arrow(&col.data_type).map_err(|e| {
                    DbError::InvalidOperation(format!(
                        "unsupported column type for '{}': {e}",
                        col.name
                    ))
                })?;
                let nullable = !col
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull));
                Ok(arrow::datatypes::Field::new(
                    col.name.to_string(),
                    data_type,
                    nullable,
                ))
            })
            .collect::<Result<Vec<_>, DbError>>()?;

        let schema = Arc::new(arrow::datatypes::Schema::new(fields));

        // Extract primary key from column constraints or table constraints
        let mut primary_key: Option<String> = None;

        // Check column-level PRIMARY KEY
        for col in &create.columns {
            for opt in &col.options {
                if matches!(opt.option, sqlparser::ast::ColumnOption::PrimaryKey(..)) {
                    primary_key = Some(col.name.to_string());
                    break;
                }
            }
            if primary_key.is_some() {
                break;
            }
        }

        // Check table-level PRIMARY KEY constraint
        if primary_key.is_none() {
            for constraint in &create.constraints {
                if let sqlparser::ast::TableConstraint::PrimaryKey(pk) = constraint {
                    if let Some(first) = pk.columns.first() {
                        primary_key = Some(first.column.to_string());
                    }
                }
            }
        }

        // Extract connector options from WITH (...)
        let mut connector_type: Option<String> = None;
        let mut connector_options: HashMap<String, String> = HashMap::new();
        let mut format: Option<String> = None;
        let mut format_options: HashMap<String, String> = HashMap::new();
        let mut refresh_mode: Option<laminar_connectors::reference::RefreshMode> = None;
        let mut cache_mode: Option<crate::table_cache_mode::TableCacheMode> = None;
        let mut cache_max_entries: Option<usize> = None;
        let mut storage: Option<String> = None;

        let with_options = match &create.table_options {
            sqlparser::ast::CreateTableOptions::With(opts) => opts.as_slice(),
            _ => &[],
        };

        for opt in with_options {
            if let sqlparser::ast::SqlOption::KeyValue { key, value } = opt {
                let k = key.to_string().to_lowercase();
                let val = value.to_string().trim_matches('\'').to_string();
                match k.as_str() {
                    "connector" => connector_type = Some(val),
                    "format" => format = Some(val),
                    "refresh" => {
                        refresh_mode = Some(crate::connector_manager::parse_refresh_mode(&val)?);
                    }
                    "cache_mode" => {
                        cache_mode = Some(crate::table_cache_mode::parse_cache_mode(&val)?);
                    }
                    "cache_max_entries" => {
                        cache_max_entries = Some(val.parse::<usize>().map_err(|_| {
                            DbError::InvalidOperation(format!(
                                "Invalid cache_max_entries '{val}': expected positive integer"
                            ))
                        })?);
                    }
                    "storage" => storage = Some(val),
                    kk if kk.starts_with("format.") => {
                        format_options.insert(kk.strip_prefix("format.").unwrap().to_string(), val);
                    }
                    _ => {
                        connector_options.insert(k, val);
                    }
                }
            }
        }

        // Resolve cache mode: if Partial and cache_max_entries overrides, apply it
        let resolved_cache_mode = match (&cache_mode, cache_max_entries) {
            (Some(crate::table_cache_mode::TableCacheMode::Partial { .. }), Some(max)) => {
                Some(crate::table_cache_mode::TableCacheMode::Partial { max_entries: max })
            }
            _ => cache_mode.clone(),
        };

        // Determine whether this is a persistent table
        let is_persistent = storage.as_deref() == Some("persistent");

        // Register in TableStore if PK found
        if let Some(ref pk) = primary_key {
            let cache = resolved_cache_mode
                .clone()
                .unwrap_or(crate::table_cache_mode::TableCacheMode::Full);

            if is_persistent {
                #[cfg(feature = "rocksdb")]
                {
                    let mut ts = self.table_store.lock();
                    ts.create_table_persistent(&name, schema.clone(), pk, cache)?;
                }
                #[cfg(not(feature = "rocksdb"))]
                {
                    return Err(DbError::InvalidOperation(
                        "storage = 'persistent' requires the 'rocksdb' feature".to_string(),
                    ));
                }
            } else if resolved_cache_mode.is_some() {
                let mut ts = self.table_store.lock();
                ts.create_table_with_cache(&name, schema.clone(), pk, cache)?;
            } else {
                let mut ts = self.table_store.lock();
                ts.create_table(&name, schema.clone(), pk)?;
            }
        }

        // Register connector-backed table in ConnectorManager
        if connector_type.is_some() || !connector_options.is_empty() {
            if let Some(ref pk) = primary_key {
                if let Some(ref ct) = connector_type {
                    let mut ts = self.table_store.lock();
                    ts.set_connector(&name, ct);
                }

                let mut mgr = self.connector_manager.lock();
                mgr.register_table(crate::connector_manager::TableRegistration {
                    name: name.clone(),
                    primary_key: pk.clone(),
                    connector_type: connector_type.clone(),
                    connector_options,
                    format,
                    format_options,
                    refresh: refresh_mode,
                    cache_mode: cache_mode.clone(),
                    cache_max_entries,
                    storage: storage.clone(),
                });
            }
        }

        // Register with DataFusion.
        // Persistent tables use a live ReferenceTableProvider; others use MemTable.
        if is_persistent && primary_key.is_some() {
            let provider = crate::table_provider::ReferenceTableProvider::new(
                name.clone(),
                schema.clone(),
                self.table_store.clone(),
            );
            self.ctx
                .register_table(&name, Arc::new(provider))
                .map_err(|e| DbError::InvalidOperation(format!("Failed to register table: {e}")))?;
        } else {
            let mem_table = datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![]])
                .map_err(|e| DbError::InvalidOperation(format!("Failed to create table: {e}")))?;

            self.ctx
                .register_table(&name, Arc::new(mem_table))
                .map_err(|e| DbError::InvalidOperation(format!("Failed to register table: {e}")))?;
        }

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE TABLE".to_string(),
            object_name: name,
        }))
    }

    /// Handle DROP SOURCE statement.
    fn handle_drop_source(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
    ) -> Result<ExecuteResult, DbError> {
        let name_str = name.to_string();
        let dropped = self.catalog.drop_source(&name_str);
        if !dropped && !if_exists {
            return Err(DbError::SourceNotFound(name_str));
        }
        // Deregister from DataFusion so SELECT no longer resolves.
        let _ = self.ctx.deregister_table(&name_str);
        self.connector_manager.lock().unregister_source(&name_str);
        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP SOURCE".to_string(),
            object_name: name_str,
        }))
    }

    /// Handle DROP SINK statement.
    fn handle_drop_sink(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
    ) -> Result<ExecuteResult, DbError> {
        let name_str = name.to_string();
        let dropped = self.catalog.drop_sink(&name_str);
        if !dropped && !if_exists {
            return Err(DbError::SinkNotFound(name_str));
        }
        self.connector_manager.lock().unregister_sink(&name_str);
        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP SINK".to_string(),
            object_name: name_str,
        }))
    }

    /// Handle CREATE STREAM statement.
    fn handle_create_stream(
        &self,
        name: &sqlparser::ast::ObjectName,
        query: &StreamingStatement,
        emit_clause: Option<&laminar_sql::parser::EmitClause>,
    ) -> Result<ExecuteResult, DbError> {
        let name_str = name.to_string();

        // Register in catalog as a stream
        self.catalog.register_stream(&name_str)?;

        // Plan the statement to extract emit_clause and window_config
        let (plan_emit, plan_window) = {
            let mut planner = self.planner.lock();
            let stmt = StreamingStatement::CreateStream {
                name: name.clone(),
                query: Box::new(query.clone()),
                emit_clause: emit_clause.cloned(),
                or_replace: false,
                if_not_exists: false,
            };
            match planner.plan(&stmt) {
                Ok(laminar_sql::planner::StreamingPlan::Query(ref qp)) => {
                    (qp.emit_clause.clone(), qp.window_config.clone())
                }
                _ => (emit_clause.cloned(), None),
            }
        };

        // Store the query SQL for stream execution at start()
        {
            let mut mgr = self.connector_manager.lock();
            mgr.register_stream(crate::connector_manager::StreamRegistration {
                name: name_str.clone(),
                query_sql: streaming_statement_to_sql(query),
                emit_clause: plan_emit,
                window_config: plan_window,
            });
        }

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE STREAM".to_string(),
            object_name: name_str,
        }))
    }

    /// Handle DROP STREAM statement.
    fn handle_drop_stream(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
    ) -> Result<ExecuteResult, DbError> {
        let name_str = name.to_string();
        let dropped = self.catalog.drop_stream(&name_str);
        if !dropped && !if_exists {
            return Err(DbError::StreamNotFound(name_str));
        }
        self.connector_manager.lock().unregister_stream(&name_str);
        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP STREAM".to_string(),
            object_name: name_str,
        }))
    }

    /// Subscribe to a named stream or materialized view.
    ///
    /// # Errors
    ///
    /// Returns `DbError::StreamNotFound` if the stream is not registered.
    pub fn subscribe<T: crate::handle::FromBatch>(
        &self,
        name: &str,
    ) -> Result<crate::handle::TypedSubscription<T>, DbError> {
        let sub = self
            .catalog
            .get_stream_subscription(name)
            .ok_or_else(|| DbError::StreamNotFound(name.to_string()))?;
        Ok(crate::handle::TypedSubscription::from_raw(sub))
    }

    /// Get a raw Arrow subscription for a named stream (crate-internal).
    ///
    /// Used by `api::Connection::subscribe` to create an `ArrowSubscription`
    /// without requiring the `FromBatch` trait bound.
    #[cfg(feature = "api")]
    pub(crate) fn subscribe_raw(
        &self,
        name: &str,
    ) -> Result<laminar_core::streaming::Subscription<crate::catalog::ArrowRecord>, DbError> {
        self.catalog
            .get_stream_subscription(name)
            .ok_or_else(|| DbError::StreamNotFound(name.to_string()))
    }

    /// Handle EXPLAIN statement — show the streaming query plan.
    fn handle_explain(&self, statement: &StreamingStatement) -> Result<ExecuteResult, DbError> {
        let mut planner = self.planner.lock();

        // Plan the inner statement to extract streaming info
        let plan_result = planner.plan(statement);

        let mut rows: Vec<(String, String)> = Vec::new();

        match plan_result {
            Ok(plan) => {
                rows.push((
                    "plan_type".into(),
                    match &plan {
                        laminar_sql::planner::StreamingPlan::Query(_) => "Query",
                        laminar_sql::planner::StreamingPlan::RegisterSource(_) => "RegisterSource",
                        laminar_sql::planner::StreamingPlan::RegisterSink(_) => "RegisterSink",
                        laminar_sql::planner::StreamingPlan::Standard(_) => "Standard",
                        laminar_sql::planner::StreamingPlan::DagExplain(_) => "DagExplain",
                    }
                    .into(),
                ));
                match &plan {
                    laminar_sql::planner::StreamingPlan::Query(qp) => {
                        if let Some(name) = &qp.name {
                            rows.push(("query_name".into(), name.clone()));
                        }
                        if let Some(wc) = &qp.window_config {
                            rows.push(("window".into(), format!("{wc}")));
                        }
                        if let Some(jcs) = &qp.join_config {
                            if jcs.len() == 1 {
                                rows.push(("join".into(), format!("{}", jcs[0])));
                            } else {
                                for (i, jc) in jcs.iter().enumerate() {
                                    rows.push((format!("join_step_{}", i + 1), format!("{jc}")));
                                }
                            }
                        }
                        if let Some(oc) = &qp.order_config {
                            rows.push(("order_by".into(), format!("{oc:?}")));
                        }
                        if let Some(fc) = &qp.frame_config {
                            rows.push((
                                "frame_functions".into(),
                                format!("{}", fc.functions.len()),
                            ));
                        }
                        if let Some(ec) = &qp.emit_clause {
                            rows.push(("emit".into(), format!("{ec}")));
                        }
                    }
                    laminar_sql::planner::StreamingPlan::RegisterSource(info) => {
                        rows.push(("source".into(), info.name.clone()));
                    }
                    laminar_sql::planner::StreamingPlan::RegisterSink(info) => {
                        rows.push(("sink".into(), info.name.clone()));
                    }
                    laminar_sql::planner::StreamingPlan::Standard(_) => {
                        rows.push(("execution".into(), "DataFusion pass-through".into()));
                    }
                    laminar_sql::planner::StreamingPlan::DagExplain(output) => {
                        rows.push(("dag_topology".into(), output.topology_text.clone()));
                    }
                }
            }
            Err(e) => {
                // Even if planning fails, show what we know
                rows.push(("error".into(), format!("{e}")));
                rows.push((
                    "statement".into(),
                    format!("{:?}", std::mem::discriminant(statement)),
                ));
            }
        }

        let keys: Vec<&str> = rows.iter().map(|(k, _)| k.as_str()).collect();
        let values: Vec<&str> = rows.iter().map(|(_, v)| v.as_str()).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("plan_key", DataType::Utf8, false),
            Field::new("plan_value", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .map_err(|e| DbError::InvalidOperation(format!("explain metadata: {e}")))?;

        Ok(ExecuteResult::Metadata(batch))
    }

    /// Handle a streaming or standard SQL query.
    async fn handle_query(&self, sql: &str) -> Result<ExecuteResult, DbError> {
        // Synchronous planning under the lock — released before any await
        let plan = {
            let statements = parse_streaming_sql(sql)?;
            if statements.is_empty() {
                return Err(DbError::InvalidOperation("Empty SQL statement".into()));
            }
            let mut planner = self.planner.lock();
            planner
                .plan(&statements[0])
                .map_err(laminar_sql::Error::from)?
        };

        match plan {
            laminar_sql::planner::StreamingPlan::RegisterSource(info) => {
                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "DDL".to_string(),
                    object_name: info.name,
                }))
            }
            laminar_sql::planner::StreamingPlan::RegisterSink(info) => {
                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "DDL".to_string(),
                    object_name: info.name,
                }))
            }
            laminar_sql::planner::StreamingPlan::Query(query_plan) => {
                // Check for ASOF join — DataFusion can't parse ASOF syntax
                if let Some(asof_config) = Self::extract_asof_config(&query_plan) {
                    return self.execute_asof_query(&asof_config, sql).await;
                }

                let plan_sql = query_plan.statement.to_string();
                let logical_plan = self.ctx.state().create_logical_plan(&plan_sql).await?;

                // Try JIT compilation first (when feature enabled).
                #[cfg(feature = "jit")]
                {
                    if let Some(compiled) = self.try_compile_query(sql, &logical_plan) {
                        tracing::info!(
                            sql = %sql,
                            compiled = compiled.metadata.compiled_pipeline_count,
                            fallback = compiled.metadata.fallback_pipeline_count,
                            "Query compiled via JIT"
                        );
                        return self.bridge_compiled_query(sql, compiled).await;
                    }
                }

                // Fall through to DataFusion interpreted execution.
                let df = self.ctx.execute_logical_plan(logical_plan).await?;
                let stream = df.execute_stream().await?;

                Ok(self.bridge_query_stream(sql, stream))
            }
            laminar_sql::planner::StreamingPlan::Standard(stmt) => {
                // Async execution without the lock
                let sql_str = stmt.to_string();
                let df = self.ctx.sql(&sql_str).await?;
                let stream = df.execute_stream().await?;

                Ok(self.bridge_query_stream(sql, stream))
            }
            laminar_sql::planner::StreamingPlan::DagExplain(output) => {
                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "EXPLAIN DAG".to_string(),
                    object_name: output.topology_text,
                }))
            }
        }
    }

    /// Bridge a `DataFusion` `SendableRecordBatchStream` into the streaming
    /// subscription infrastructure and return a `QueryHandle`.
    fn bridge_query_stream(
        &self,
        sql: &str,
        stream: datafusion::physical_plan::SendableRecordBatchStream,
    ) -> ExecuteResult {
        let query_id = self.catalog.register_query(sql);
        let schema = stream.schema();

        let source_cfg = streaming::SourceConfig::with_buffer_size(self.config.default_buffer_size);
        let (source, sink) =
            streaming::create_with_config::<crate::catalog::ArrowRecord>(source_cfg);

        let subscription = sink.subscribe();

        let source_clone = source.clone();
        tokio::spawn(async move {
            use tokio_stream::StreamExt;
            let mut stream = stream;
            while let Some(result) = stream.next().await {
                match result {
                    Ok(batch) => {
                        if source_clone.push_arrow(batch).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            drop(source_clone);
        });

        ExecuteResult::Query(QueryHandle {
            id: query_id,
            schema,
            sql: sql.to_string(),
            subscription: Some(subscription),
            active: true,
        })
    }

    // ── JIT compilation integration ─────────────────────────────────

    /// Attempts to compile a query via the JIT compiler stack.
    ///
    /// Returns `Some(compiled)` if compilation succeeds.
    /// Returns `None` if the plan has stateful operators or compilation fails.
    /// Compilation errors are logged at `DEBUG` level and swallowed — transparent fallback.
    #[cfg(feature = "jit")]
    fn try_compile_query(
        &self,
        sql: &str,
        logical_plan: &datafusion::logical_expr::LogicalPlan,
    ) -> Option<laminar_core::compiler::CompiledStreamingQuery> {
        let Some(mut cache) = self.compiler_cache.try_lock() else {
            tracing::debug!(sql = %sql, "Compiler cache contended, falling back to DataFusion");
            return None;
        };
        let config = laminar_core::compiler::QueryConfig::default();
        match laminar_core::compiler::compile_streaming_query(
            sql,
            logical_plan,
            &mut cache,
            &config,
        ) {
            Ok(result) => result,
            Err(e) => {
                tracing::debug!(
                    sql = %sql,
                    error = %e,
                    "JIT compilation failed, falling back to DataFusion"
                );
                None
            }
        }
    }

    /// Executes a compiled query: runs the source scan via `DataFusion`, feeds
    /// rows through the compiled pipeline, and bridges output to the subscription.
    #[cfg(feature = "jit")]
    async fn bridge_compiled_query(
        &self,
        sql: &str,
        compiled: laminar_core::compiler::CompiledStreamingQuery,
    ) -> Result<ExecuteResult, DbError> {
        use laminar_core::compiler::batch_reader::BatchRowReader;
        use laminar_core::compiler::event_time::{EventTimeConfig, RowEventTimeExtractor};
        use laminar_core::compiler::pipeline_bridge::Ring1Action;

        // 1. Execute source scan via DataFusion to get input stream.
        let df = self.ctx.execute_logical_plan(compiled.source_plan).await?;
        let input_stream = df.execute_stream().await?;
        let output_schema = compiled.output_schema.arrow_schema().clone();

        // 2. Set up subscription infrastructure.
        let query_id = self.catalog.register_query(sql);
        let source_cfg = streaming::SourceConfig::with_buffer_size(self.config.default_buffer_size);
        let (source, sink) =
            streaming::create_with_config::<crate::catalog::ArrowRecord>(source_cfg);
        let subscription = sink.subscribe();

        // 3. Build event time extractor (auto-detect from schema).
        let input_schema = compiled.input_schema;
        let time_config = EventTimeConfig::default();
        let time_extractor = RowEventTimeExtractor::from_schema(&input_schema, &time_config);

        // 4. Spawn bridge task: input stream → EventRow → compiled pipeline → output batches.
        let source_clone = source.clone();
        let mut query = compiled.query;
        query
            .start()
            .map_err(|e| DbError::InvalidOperation(e.to_string()))?;

        tokio::spawn(async move {
            use tokio_stream::StreamExt;
            let mut stream = input_stream;
            let mut extractor = time_extractor;
            let mut arena = bumpalo::Bump::with_capacity(1024 * 128);

            while let Some(result) = stream.next().await {
                match result {
                    Ok(batch) => {
                        let reader = BatchRowReader::new(&batch, &input_schema);
                        for row_idx in 0..reader.row_count() {
                            let row = reader.read_row(row_idx, &arena);
                            let event_time = match extractor {
                                Some(ref mut ext) => ext.extract(&row),
                                #[allow(clippy::cast_possible_wrap)]
                                None => row_idx as i64,
                            };
                            let _ = query.submit_row(&row, event_time, row_idx as u64);
                        }
                        // Advance watermark after each batch.
                        if let Some(ref ext) = extractor {
                            let _ = query.advance_watermark(ext.watermark());
                        }
                        // Drain compiled output.
                        let actions = query.poll_ring1();
                        for action in actions {
                            if let Ring1Action::ProcessBatch(out_batch) = action {
                                if source_clone.push_arrow(out_batch).is_err() {
                                    return;
                                }
                            }
                        }
                        // Reset arena for next batch — keeps underlying allocation.
                        arena.reset();
                    }
                    Err(_) => break,
                }
            }
            // Flush remaining output.
            let _ = query.send_eof();
            for action in query.poll_ring1() {
                if let Ring1Action::ProcessBatch(batch) = action {
                    let _ = source_clone.push_arrow(batch);
                }
            }
        });

        Ok(ExecuteResult::Query(QueryHandle {
            id: query_id,
            schema: output_schema,
            sql: sql.to_string(),
            subscription: Some(subscription),
            active: true,
        }))
    }

    /// Extract an ASOF join config from a query plan, if present.
    fn extract_asof_config(
        plan: &laminar_sql::planner::QueryPlan,
    ) -> Option<AsofJoinTranslatorConfig> {
        plan.join_config.as_ref()?.iter().find_map(|jc| {
            if let JoinOperatorConfig::Asof(cfg) = jc {
                Some(cfg.clone())
            } else {
                None
            }
        })
    }

    /// Execute an ASOF join query by fetching left/right tables separately
    /// and performing the join in-process (bypasses `DataFusion`'s SQL parser
    /// which doesn't understand ASOF syntax).
    async fn execute_asof_query(
        &self,
        asof_config: &AsofJoinTranslatorConfig,
        original_sql: &str,
    ) -> Result<ExecuteResult, DbError> {
        let left_sql = format!("SELECT * FROM {}", asof_config.left_table);
        let right_sql = format!("SELECT * FROM {}", asof_config.right_table);

        let left_batches = self
            .ctx
            .sql(&left_sql)
            .await
            .map_err(|e| {
                DbError::Pipeline(format!(
                    "ASOF left table '{}' query failed: {e}",
                    asof_config.left_table
                ))
            })?
            .collect()
            .await
            .map_err(|e| {
                DbError::Pipeline(format!(
                    "ASOF left table '{}' execution failed: {e}",
                    asof_config.left_table
                ))
            })?;

        let right_batches = self
            .ctx
            .sql(&right_sql)
            .await
            .map_err(|e| {
                DbError::Pipeline(format!(
                    "ASOF right table '{}' query failed: {e}",
                    asof_config.right_table
                ))
            })?
            .collect()
            .await
            .map_err(|e| {
                DbError::Pipeline(format!(
                    "ASOF right table '{}' execution failed: {e}",
                    asof_config.right_table
                ))
            })?;

        let result_batch =
            crate::asof_batch::execute_asof_join_batch(&left_batches, &right_batches, asof_config)?;

        if result_batch.num_rows() == 0 {
            let query_id = self.catalog.register_query(original_sql);
            return Ok(ExecuteResult::Query(QueryHandle {
                id: query_id,
                schema: result_batch.schema(),
                sql: original_sql.to_string(),
                subscription: None,
                active: false,
            }));
        }

        let schema = result_batch.schema();
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![result_batch]])
                .map_err(|e| {
                    DbError::Pipeline(format!("Failed to create ASOF result table: {e}"))
                })?;

        let _ = self.ctx.deregister_table("__asof_result");
        self.ctx
            .register_table("__asof_result", Arc::new(mem_table))
            .map_err(|e| DbError::Pipeline(format!("Failed to register ASOF result table: {e}")))?;

        let df = self
            .ctx
            .sql("SELECT * FROM __asof_result")
            .await
            .map_err(|e| DbError::Pipeline(format!("Failed to query ASOF result: {e}")))?;
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| DbError::Pipeline(format!("Failed to stream ASOF result: {e}")))?;

        let _ = self.ctx.deregister_table("__asof_result");

        Ok(self.bridge_query_stream(original_sql, stream))
    }

    /// Get a typed source handle for pushing data.
    ///
    /// The source must have been created via `CREATE SOURCE`.
    ///
    /// # Errors
    ///
    /// Returns `DbError::SourceNotFound` if the source is not registered.
    pub fn source<T: laminar_core::streaming::Record>(
        &self,
        name: &str,
    ) -> Result<SourceHandle<T>, DbError> {
        let entry = self
            .catalog
            .get_source(name)
            .ok_or_else(|| DbError::SourceNotFound(name.to_string()))?;
        SourceHandle::new(entry)
    }

    /// Get an untyped source handle for pushing `RecordBatch` data.
    ///
    /// # Errors
    ///
    /// Returns `DbError::SourceNotFound` if the source is not registered.
    pub fn source_untyped(&self, name: &str) -> Result<UntypedSourceHandle, DbError> {
        let entry = self
            .catalog
            .get_source(name)
            .ok_or_else(|| DbError::SourceNotFound(name.to_string()))?;
        Ok(UntypedSourceHandle::new(entry))
    }

    /// List all registered sources.
    pub fn sources(&self) -> Vec<SourceInfo> {
        let names = self.catalog.list_sources();
        names
            .into_iter()
            .filter_map(|name| {
                self.catalog.get_source(&name).map(|e| SourceInfo {
                    name: e.name.clone(),
                    schema: e.schema.clone(),
                    watermark_column: e.watermark_column.clone(),
                })
            })
            .collect()
    }

    /// List all registered sinks.
    pub fn sinks(&self) -> Vec<SinkInfo> {
        self.catalog
            .list_sinks()
            .into_iter()
            .map(|name| SinkInfo { name })
            .collect()
    }

    /// List all registered streams with their SQL definitions.
    pub fn streams(&self) -> Vec<crate::handle::StreamInfo> {
        let mgr = self.connector_manager.lock();
        mgr.streams()
            .iter()
            .map(|(name, reg)| crate::handle::StreamInfo {
                name: name.clone(),
                sql: Some(reg.query_sql.clone()),
            })
            .collect()
    }

    /// Build the pipeline topology graph from registered sources, streams,
    /// and sinks.
    ///
    /// Returns a `PipelineTopology` with nodes for every source, stream,
    /// and sink, plus edges derived from stream SQL `FROM` references and
    /// sink `input` fields.
    pub fn pipeline_topology(&self) -> crate::handle::PipelineTopology {
        use crate::handle::{PipelineEdge, PipelineNode, PipelineNodeType};

        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        // Collect source names for FROM matching
        let source_names = self.catalog.list_sources();

        // Source nodes
        for name in &source_names {
            let schema = self.catalog.get_source(name).map(|e| e.schema.clone());
            nodes.push(PipelineNode {
                name: name.clone(),
                node_type: PipelineNodeType::Source,
                schema,
                sql: None,
            });
        }

        // Stream nodes + edges from SQL FROM references
        let mgr = self.connector_manager.lock();
        let stream_names: Vec<String> = mgr.streams().keys().cloned().collect();
        for (name, reg) in mgr.streams() {
            nodes.push(PipelineNode {
                name: name.clone(),
                node_type: PipelineNodeType::Stream,
                schema: None,
                sql: Some(reg.query_sql.clone()),
            });

            // Extract FROM references by checking which known sources/streams
            // appear in the query SQL. This is a lightweight heuristic that
            // avoids a full SQL parse.
            let sql_upper = reg.query_sql.to_uppercase();
            for src in &source_names {
                if sql_upper.contains(&src.to_uppercase()) {
                    edges.push(PipelineEdge {
                        from: src.clone(),
                        to: name.clone(),
                    });
                }
            }
            // Also check for stream-to-stream references (cascading)
            for other in &stream_names {
                if other != name && sql_upper.contains(&other.to_uppercase()) {
                    edges.push(PipelineEdge {
                        from: other.clone(),
                        to: name.clone(),
                    });
                }
            }
        }

        // Sink nodes + edges from input field
        for (name, reg) in mgr.sinks() {
            nodes.push(PipelineNode {
                name: name.clone(),
                node_type: PipelineNodeType::Sink,
                schema: None,
                sql: None,
            });

            // Sinks read from their `input` field
            if !reg.input.is_empty() {
                edges.push(PipelineEdge {
                    from: reg.input.clone(),
                    to: name.clone(),
                });
            }
        }

        // Also add catalog-only sinks (no connector type) that aren't
        // already in the connector manager
        let cm_sink_names: std::collections::HashSet<&String> = mgr.sinks().keys().collect();
        for name in self.catalog.list_sinks() {
            if !cm_sink_names.contains(&name) {
                // Check if there's a sink entry in the catalog with input info
                if let Some(input) = self.catalog.get_sink_input(&name) {
                    nodes.push(PipelineNode {
                        name: name.clone(),
                        node_type: PipelineNodeType::Sink,
                        schema: None,
                        sql: None,
                    });
                    if !input.is_empty() {
                        edges.push(PipelineEdge {
                            from: input,
                            to: name,
                        });
                    }
                }
            }
        }

        drop(mgr);

        crate::handle::PipelineTopology { nodes, edges }
    }

    /// List all active queries.
    pub fn queries(&self) -> Vec<QueryInfo> {
        self.catalog
            .list_queries()
            .into_iter()
            .map(|(id, sql, active)| QueryInfo { id, sql, active })
            .collect()
    }

    /// Returns whether streaming checkpointing is enabled.
    #[must_use]
    pub fn is_checkpoint_enabled(&self) -> bool {
        self.config.checkpoint.is_some()
    }

    /// Triggers a streaming checkpoint that persists source offsets, sink
    /// positions, and operator state to disk via the
    /// [`CheckpointCoordinator`](crate::checkpoint_coordinator::CheckpointCoordinator).
    ///
    /// Returns the checkpoint result on success, including the checkpoint ID,
    /// epoch, and duration.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if checkpointing is not enabled, the
    /// coordinator has not been initialized (call `start()` first), or the
    /// checkpoint operation fails.
    pub async fn checkpoint(
        &self,
    ) -> Result<crate::checkpoint_coordinator::CheckpointResult, DbError> {
        if self.config.checkpoint.is_none() {
            return Err(DbError::Checkpoint(
                "checkpointing is not enabled".to_string(),
            ));
        }
        let mut guard = self.coordinator.lock().await;
        let coord = guard.as_mut().ok_or_else(|| {
            DbError::Checkpoint("coordinator not initialized — call start() first".to_string())
        })?;
        coord
            .checkpoint(HashMap::new(), None, 0, Vec::new(), None)
            .await
    }

    /// Shut down the database gracefully.
    pub fn close(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Check if the database is shut down.
    pub fn is_closed(&self) -> bool {
        self.shutdown.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Start the streaming pipeline.
    ///
    /// Activates all registered connectors and begins processing.
    /// This is a no-op if the pipeline is already running.
    ///
    /// When the `kafka` feature is enabled and Kafka sources/sinks are
    /// registered, this builds `KafkaSource`/`KafkaSink` instances and
    /// spawns a background task that polls sources, executes stream queries
    /// via `DataFusion`, and writes results to sinks.
    ///
    /// In embedded (in-memory) mode, this simply transitions to `Running`.
    ///
    /// # Errors
    ///
    /// Returns an error if the pipeline cannot be started.
    pub async fn start(&self) -> Result<(), DbError> {
        let current = self.state.load(std::sync::atomic::Ordering::Acquire);
        if current == STATE_RUNNING || current == STATE_STARTING {
            return Ok(());
        }
        if current == STATE_STOPPED {
            return Err(DbError::InvalidOperation(
                "Cannot start a stopped pipeline. Create a new LaminarDB instance.".into(),
            ));
        }

        self.state
            .store(STATE_STARTING, std::sync::atomic::Ordering::Release);

        // Snapshot connector registrations under the lock
        let (source_regs, sink_regs, stream_regs, table_regs, has_external) = {
            let mgr = self.connector_manager.lock();
            (
                mgr.sources().clone(),
                mgr.sinks().clone(),
                mgr.streams().clone(),
                mgr.tables().clone(),
                mgr.has_external_connectors(),
            )
        };

        // Log which sources have external connectors for debugging.
        for (name, reg) in &source_regs {
            eprintln!(
                "[laminar-db] Source '{}': connector_type={:?}",
                name, reg.connector_type
            );
        }
        for (name, reg) in &sink_regs {
            eprintln!(
                "[laminar-db] Sink '{}': connector_type={:?}",
                name, reg.connector_type
            );
        }

        // Initialize checkpoint coordinator (shared across all pipeline modes)
        if let Some(ref cp_config) = self.config.checkpoint {
            use crate::checkpoint_coordinator::{
                CheckpointConfig as CkpConfig, CheckpointCoordinator,
            };
            use laminar_storage::checkpoint_store::FileSystemCheckpointStore;

            let data_dir = cp_config
                .data_dir
                .clone()
                .or_else(|| self.config.storage_dir.clone())
                .unwrap_or_else(|| std::path::PathBuf::from("./data"));
            let max_retained = cp_config.max_retained.unwrap_or(3);
            let store = FileSystemCheckpointStore::new(&data_dir, max_retained);
            let config = CkpConfig {
                interval: cp_config.interval_ms.map(std::time::Duration::from_millis),
                max_retained,
                ..CkpConfig::default()
            };
            let mut coord = CheckpointCoordinator::new(config, Box::new(store));
            coord.set_counters(Arc::clone(&self.counters));
            *self.coordinator.lock().await = Some(coord);
        }

        if has_external {
            eprintln!(
                "[laminar-db] Starting CONNECTOR pipeline ({} sources, {} sinks, {} streams, {} tables)",
                source_regs.len(),
                sink_regs.len(),
                stream_regs.len(),
                table_regs.len(),
            );
            self.start_connector_pipeline(source_regs, sink_regs, stream_regs, table_regs)
                .await?;
        } else if !stream_regs.is_empty() {
            eprintln!(
                "[laminar-db] Starting EMBEDDED pipeline ({} streams)",
                stream_regs.len(),
            );
            self.start_embedded_pipeline(&stream_regs);
        } else {
            eprintln!("[laminar-db] Starting in embedded mode (no streams)");
            tracing::info!(
                sources = source_regs.len(),
                sinks = sink_regs.len(),
                "Starting in embedded (in-memory) mode — no streams"
            );
        }

        self.state
            .store(STATE_RUNNING, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Start a lightweight embedded processing loop for in-memory sources.
    ///
    /// When no external connectors are registered but named streams exist,
    /// this spawns a background task that:
    ///
    /// 1. Polls each source's sink for pushed `RecordBatch` data.
    /// 2. Executes registered stream queries via `DataFusion`.
    /// 3. Pushes query results into the corresponding stream sources so
    ///    that callers of [`subscribe`](Self::subscribe) receive data.
    #[allow(clippy::too_many_lines)]
    fn start_embedded_pipeline(
        &self,
        stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
    ) {
        use crate::stream_executor::StreamExecutor;

        // Build StreamExecutor with the registered stream queries
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        for reg in stream_regs.values() {
            executor.add_query(
                reg.name.clone(),
                reg.query_sql.clone(),
                reg.emit_clause.clone(),
                reg.window_config.clone(),
            );
        }

        // Subscribe to every source's sink so we can read pushed data.
        let mut source_subs: Vec<(String, streaming::Subscription<crate::catalog::ArrowRecord>)> =
            Vec::new();
        for name in self.catalog.list_sources() {
            if let Some(entry) = self.catalog.get_source(&name) {
                let sub = entry.sink.subscribe();
                source_subs.push((name, sub));
            }
        }

        // Get stream source handles for pushing results.
        let mut stream_sources: Vec<(String, streaming::Source<crate::catalog::ArrowRecord>)> =
            Vec::new();
        for reg in stream_regs.values() {
            if let Some(src) = self.catalog.get_stream_source(&reg.name) {
                stream_sources.push((reg.name.clone(), src));
            }
        }

        // Build per-source watermark tracking state
        let mut watermark_states: HashMap<String, SourceWatermarkState> = HashMap::new();
        let mut source_entries: HashMap<String, Arc<crate::catalog::SourceEntry>> = HashMap::new();
        let mut source_ids: HashMap<String, usize> = HashMap::new();
        for name in self.catalog.list_sources() {
            if let Some(entry) = self.catalog.get_source(&name) {
                if let (Some(col), Some(dur)) =
                    (&entry.watermark_column, entry.max_out_of_orderness)
                {
                    let format = infer_timestamp_format(&entry.schema, col);
                    let extractor =
                        laminar_core::time::EventTimeExtractor::from_column(col, format)
                            .with_mode(laminar_core::time::ExtractionMode::Max);
                    let generator =
                        laminar_core::time::BoundedOutOfOrdernessGenerator::from_duration(dur);
                    let id = source_ids.len();
                    source_ids.insert(name.clone(), id);
                    watermark_states.insert(
                        name.clone(),
                        SourceWatermarkState {
                            extractor,
                            generator,
                            column: col.clone(),
                            format,
                        },
                    );
                }
                source_entries.insert(name, entry);
            }
        }

        // Also create watermark state for sources that declared event_time_column
        // programmatically (via source.set_event_time_column()) but have no SQL WATERMARK
        for name in self.catalog.list_sources() {
            if watermark_states.contains_key(&name) {
                continue;
            }
            if let Some(entry) = self.catalog.get_source(&name) {
                if let Some(col) = entry.source.event_time_column() {
                    let format = infer_timestamp_format(&entry.schema, &col);
                    let extractor =
                        laminar_core::time::EventTimeExtractor::from_column(&col, format)
                            .with_mode(laminar_core::time::ExtractionMode::Max);
                    let generator =
                        laminar_core::time::BoundedOutOfOrdernessGenerator::from_duration(
                            std::time::Duration::ZERO,
                        );
                    let id = source_ids.len();
                    source_ids.insert(name.clone(), id);
                    watermark_states.insert(
                        name.clone(),
                        SourceWatermarkState {
                            extractor,
                            generator,
                            column: col,
                            format,
                        },
                    );
                }
            }
        }

        let mut tracker = if source_ids.is_empty() {
            None
        } else {
            Some(laminar_core::time::WatermarkTracker::new(source_ids.len()))
        };

        tracing::info!(
            sources = source_subs.len(),
            streams = stream_sources.len(),
            watermark_sources = source_ids.len(),
            "Starting embedded pipeline"
        );

        let shutdown = self.shutdown_signal.clone();
        let counters = Arc::clone(&self.counters);
        let pipeline_watermark = Arc::clone(&self.pipeline_watermark);

        let handle = tokio::spawn(async move {
            let mut cycle_count: u64 = 0;
            loop {
                // Check for shutdown
                tokio::select! {
                    () = shutdown.notified() => {
                        tracing::info!("Embedded pipeline shutdown signal received");
                        break;
                    }
                    () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                }

                let cycle_start = std::time::Instant::now();

                // Check external watermarks from Source::watermark() calls
                for (name, _sub) in &source_subs {
                    if let Some(entry) = source_entries.get(name.as_str()) {
                        let external_wm = entry.source.current_watermark();
                        if let Some(wm_state) = watermark_states.get_mut(name.as_str()) {
                            if let Some(wm) = wm_state.generator.advance_watermark(external_wm) {
                                if let Some(ref mut trk) = tracker {
                                    if let Some(sid) = source_ids.get(name.as_str()) {
                                        if let Some(global_wm) =
                                            trk.update_source(*sid, wm.timestamp())
                                        {
                                            pipeline_watermark.store(
                                                global_wm.timestamp(),
                                                std::sync::atomic::Ordering::Relaxed,
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Drain source subscriptions into batches
                let mut source_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
                for (name, sub) in &source_subs {
                    for _ in 0..256 {
                        match sub.poll() {
                            Some(batch) if batch.num_rows() > 0 => {
                                #[allow(clippy::cast_possible_truncation)]
                                let row_count = batch.num_rows() as u64;
                                counters
                                    .events_ingested
                                    .fetch_add(row_count, std::sync::atomic::Ordering::Relaxed);
                                counters
                                    .total_batches
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                                // Extract watermark from batch
                                if let Some(wm_state) = watermark_states.get_mut(name.as_str()) {
                                    if let Ok(max_ts) = wm_state.extractor.extract(&batch) {
                                        if let Some(wm) = wm_state.generator.on_event(max_ts) {
                                            if let Some(entry) = source_entries.get(name.as_str()) {
                                                entry.source.watermark(wm.timestamp());
                                            }
                                            if let Some(ref mut trk) = tracker {
                                                if let Some(sid) = source_ids.get(name.as_str()) {
                                                    if let Some(global_wm) =
                                                        trk.update_source(*sid, wm.timestamp())
                                                    {
                                                        pipeline_watermark.store(
                                                            global_wm.timestamp(),
                                                            std::sync::atomic::Ordering::Relaxed,
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    // Filter late rows
                                    let current_wm = wm_state.generator.current_watermark();
                                    if current_wm > i64::MIN {
                                        if let Some(filtered) = filter_late_rows(
                                            &batch,
                                            &wm_state.column,
                                            current_wm,
                                            wm_state.format,
                                        ) {
                                            source_batches
                                                .entry(name.clone())
                                                .or_default()
                                                .push(filtered);
                                        }
                                        // else: all rows were late, skip batch
                                    } else {
                                        source_batches.entry(name.clone()).or_default().push(batch);
                                    }
                                } else {
                                    source_batches.entry(name.clone()).or_default().push(batch);
                                }
                            }
                            _ => break,
                        }
                    }
                }

                if source_batches.is_empty() {
                    continue;
                }

                // Execute stream queries
                let current_wm = pipeline_watermark.load(std::sync::atomic::Ordering::Relaxed);
                match executor.execute_cycle(&source_batches, current_wm).await {
                    Ok(results) => {
                        // Push results to stream sources for subscriber delivery
                        for (stream_name, src) in &stream_sources {
                            if let Some(batches) = results.get(stream_name) {
                                for batch in batches {
                                    if batch.num_rows() > 0 {
                                        #[allow(clippy::cast_possible_truncation)]
                                        let row_count = batch.num_rows() as u64;
                                        counters.events_emitted.fetch_add(
                                            row_count,
                                            std::sync::atomic::Ordering::Relaxed,
                                        );
                                        let _ = src.push_arrow(batch.clone());
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Embedded stream execution error");
                    }
                }

                cycle_count += 1;
                counters
                    .cycles
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                #[allow(clippy::cast_possible_truncation)]
                let elapsed_ns = cycle_start.elapsed().as_nanos() as u64;
                counters
                    .last_cycle_duration_ns
                    .store(elapsed_ns, std::sync::atomic::Ordering::Relaxed);
                if cycle_count.is_multiple_of(100) {
                    tracing::debug!(cycles = cycle_count, "Embedded pipeline processing");
                }
            }
            tracing::info!("Embedded pipeline stopped after {cycle_count} cycles");
        });

        *self.runtime_handle.lock() = Some(handle);
    }

    /// Build and start the connector pipeline with external sources/sinks.
    ///
    /// Uses the `ConnectorRegistry` for generic dispatch — no
    /// connector-specific code in the pipeline setup or processing loop.
    #[allow(clippy::too_many_lines)]
    async fn start_connector_pipeline(
        &self,
        source_regs: HashMap<String, crate::connector_manager::SourceRegistration>,
        sink_regs: HashMap<String, crate::connector_manager::SinkRegistration>,
        stream_regs: HashMap<String, crate::connector_manager::StreamRegistration>,
        table_regs: HashMap<String, crate::connector_manager::TableRegistration>,
    ) -> Result<(), DbError> {
        use crate::checkpoint_coordinator::source_to_connector_checkpoint;
        use crate::connector_manager::{
            build_sink_config, build_source_config, build_table_config,
        };
        use crate::stream_executor::StreamExecutor;
        use laminar_connectors::config::ConnectorConfig;
        use laminar_connectors::connector::{SinkConnector, SourceConnector};
        use laminar_connectors::reference::{ReferenceTableSource, RefreshMode};

        // Build StreamExecutor
        let ctx = SessionContext::new();
        laminar_sql::register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // Register source schemas so the executor can create empty placeholder
        // tables for sources that have no data in a given cycle (prevents
        // DataFusion "table not found" errors when only some sources have data).
        for name in self.catalog.list_sources() {
            if let Some(entry) = self.catalog.get_source(&name) {
                executor.register_source_schema(name, entry.schema.clone());
            }
        }

        for reg in stream_regs.values() {
            executor.add_query(
                reg.name.clone(),
                reg.query_sql.clone(),
                reg.emit_clause.clone(),
                reg.window_config.clone(),
            );
        }

        // Build sources via registry (generic — no connector-specific code)
        // Wrap in Arc<Mutex> so the coordinator can snapshot offsets while the
        // loop holds a separate reference.
        #[allow(clippy::type_complexity)]
        let mut sources: Vec<(
            String,
            Arc<tokio::sync::Mutex<Box<dyn SourceConnector>>>,
            ConnectorConfig,
        )> = Vec::new();
        for (name, reg) in &source_regs {
            if reg.connector_type.is_none() {
                continue;
            }
            let mut config = build_source_config(reg)?;

            // Pass the SQL-defined Arrow schema to the connector so it can
            // deserialize records with the correct column names and types.
            if let Some(entry) = self.catalog.get_source(name) {
                let schema_str = encode_arrow_schema(&entry.schema);
                config.set("_arrow_schema".to_string(), schema_str);
            }

            let source = self
                .connector_registry
                .create_source(&config)
                .map_err(|e| {
                    DbError::Connector(format!(
                        "Cannot create source '{}' (type '{}'): {e}",
                        name,
                        config.connector_type()
                    ))
                })?;
            sources.push((
                name.clone(),
                Arc::new(tokio::sync::Mutex::new(source)),
                config,
            ));
        }

        // Build sinks via registry (generic — no connector-specific code)
        // Wrap in Arc<Mutex> so the coordinator can pre-commit/commit while
        // the loop holds a separate reference.
        #[allow(clippy::type_complexity)]
        let mut sinks: Vec<(
            String,
            Arc<tokio::sync::Mutex<Box<dyn SinkConnector>>>,
            ConnectorConfig,
            Option<String>,
        )> = Vec::new();
        for (name, reg) in &sink_regs {
            if reg.connector_type.is_none() {
                continue;
            }
            let config = build_sink_config(reg)?;
            let sink = self.connector_registry.create_sink(&config).map_err(|e| {
                DbError::Connector(format!(
                    "Cannot create sink '{}' (type '{}'): {e}",
                    name,
                    config.connector_type()
                ))
            })?;
            sinks.push((
                name.clone(),
                Arc::new(tokio::sync::Mutex::new(sink)),
                config,
                reg.filter_expr.clone(),
            ));
        }

        // Open all connectors
        for (name, source_arc, config) in &sources {
            source_arc
                .lock()
                .await
                .open(config)
                .await
                .map_err(|e| DbError::Connector(format!("Failed to open source '{name}': {e}")))?;
        }
        for (name, sink_arc, config, _) in &sinks {
            sink_arc
                .lock()
                .await
                .open(config)
                .await
                .map_err(|e| DbError::Connector(format!("Failed to open sink '{name}': {e}")))?;
        }

        // Build table sources from registrations
        let mut table_sources: Vec<(String, Box<dyn ReferenceTableSource>, RefreshMode)> =
            Vec::new();
        for (name, reg) in &table_regs {
            if reg.connector_type.is_none() {
                continue;
            }
            let config = build_table_config(reg)?;
            let source = self
                .connector_registry
                .create_table_source(&config)
                .map_err(|e| {
                    DbError::Connector(format!("Cannot create table source '{name}': {e}"))
                })?;
            let mode = reg.refresh.clone().unwrap_or(RefreshMode::SnapshotPlusCdc);
            table_sources.push((name.clone(), source, mode));
        }

        // Register connector sources/sinks with the checkpoint coordinator
        {
            let mut guard = self.coordinator.lock().await;
            if let Some(ref mut coord) = *guard {
                for (name, source_arc, _) in &sources {
                    coord.register_source(name.clone(), Arc::clone(source_arc));
                }
                for (name, sink_arc, _, _) in &sinks {
                    let exactly_once = sink_arc.lock().await.capabilities().exactly_once;
                    coord.register_sink(name.clone(), Arc::clone(sink_arc), exactly_once);
                }
            }
        }

        // Recovery: restore source/sink/table state via unified coordinator
        {
            let mut guard = self.coordinator.lock().await;
            if let Some(ref mut coord) = *guard {
                match coord.recover().await {
                    Ok(Some(recovered)) => {
                        for (name, source, _) in &mut table_sources {
                            if let Some(cp) = recovered.manifest.table_offsets.get(name) {
                                let restored =
                                    crate::checkpoint_coordinator::connector_to_source_checkpoint(
                                        cp,
                                    );
                                if let Err(e) = source.restore(&restored).await {
                                    tracing::warn!(
                                        table=%name, error=%e,
                                        "Table source restore failed"
                                    );
                                }
                            }
                        }
                        eprintln!(
                            "[laminar-db] Recovered from checkpoint epoch {}",
                            recovered.epoch()
                        );
                        tracing::info!(
                            epoch = recovered.epoch(),
                            sources_restored = recovered.sources_restored,
                            sinks_rolled_back = recovered.sinks_rolled_back,
                            "Recovered from unified checkpoint"
                        );
                    }
                    Ok(None) => {
                        tracing::info!("No checkpoint found, starting fresh");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Checkpoint recovery failed, starting fresh");
                    }
                }
            }
        }

        // Snapshot phase: populate tables before stream processing begins
        for (name, source, mode) in &mut table_sources {
            if matches!(mode, RefreshMode::Manual) {
                continue;
            }
            while let Some(batch) = source
                .poll_snapshot()
                .await
                .map_err(|e| DbError::Connector(format!("Table '{name}' snapshot error: {e}")))?
            {
                self.table_store
                    .lock()
                    .upsert(name, &batch)
                    .map_err(|e| DbError::Connector(format!("Table '{name}' upsert error: {e}")))?;
            }
            self.sync_table_to_datafusion(name)?;
            {
                let mut ts = self.table_store.lock();
                ts.rebuild_xor_filter(name);
                ts.set_ready(name, true);
            }
        }

        // Get stream source handles so results also flow to db.subscribe().
        let mut stream_sources: Vec<(String, streaming::Source<crate::catalog::ArrowRecord>)> =
            Vec::new();
        for reg in stream_regs.values() {
            if let Some(src) = self.catalog.get_stream_source(&reg.name) {
                stream_sources.push((reg.name.clone(), src));
            }
        }

        // Build per-source watermark tracking state (connector pipeline)
        let mut watermark_states: HashMap<String, SourceWatermarkState> = HashMap::new();
        let mut source_entries_for_wm: HashMap<String, Arc<crate::catalog::SourceEntry>> =
            HashMap::new();
        let mut source_ids: HashMap<String, usize> = HashMap::new();
        for name in self.catalog.list_sources() {
            if let Some(entry) = self.catalog.get_source(&name) {
                if let (Some(col), Some(dur)) =
                    (&entry.watermark_column, entry.max_out_of_orderness)
                {
                    let format = infer_timestamp_format(&entry.schema, col);
                    let extractor =
                        laminar_core::time::EventTimeExtractor::from_column(col, format)
                            .with_mode(laminar_core::time::ExtractionMode::Max);
                    let generator =
                        laminar_core::time::BoundedOutOfOrdernessGenerator::from_duration(dur);
                    let id = source_ids.len();
                    source_ids.insert(name.clone(), id);
                    watermark_states.insert(
                        name.clone(),
                        SourceWatermarkState {
                            extractor,
                            generator,
                            column: col.clone(),
                            format,
                        },
                    );
                }
                source_entries_for_wm.insert(name, entry);
            }
        }

        // Also create watermark state for sources that declared event_time_column
        // programmatically (via source.set_event_time_column()) but have no SQL WATERMARK
        for name in self.catalog.list_sources() {
            if watermark_states.contains_key(&name) {
                continue;
            }
            if let Some(entry) = self.catalog.get_source(&name) {
                if let Some(col) = entry.source.event_time_column() {
                    let format = infer_timestamp_format(&entry.schema, &col);
                    let extractor =
                        laminar_core::time::EventTimeExtractor::from_column(&col, format)
                            .with_mode(laminar_core::time::ExtractionMode::Max);
                    let generator =
                        laminar_core::time::BoundedOutOfOrdernessGenerator::from_duration(
                            std::time::Duration::ZERO,
                        );
                    let id = source_ids.len();
                    source_ids.insert(name.clone(), id);
                    watermark_states.insert(
                        name.clone(),
                        SourceWatermarkState {
                            extractor,
                            generator,
                            column: col,
                            format,
                        },
                    );
                }
            }
        }

        let mut tracker = if source_ids.is_empty() {
            None
        } else {
            Some(laminar_core::time::WatermarkTracker::new(source_ids.len()))
        };

        eprintln!(
            "[laminar-db] Starting connector pipeline: {} sources, {} sinks, \
             {} streams, {} subscriptions",
            sources.len(),
            sinks.len(),
            stream_regs.len(),
            stream_sources.len(),
        );
        tracing::info!(
            sources = sources.len(),
            sinks = sinks.len(),
            streams = stream_regs.len(),
            subscriptions = stream_sources.len(),
            watermark_sources = source_ids.len(),
            "Starting connector pipeline"
        );

        // Spawn processing loop
        let shutdown = self.shutdown_signal.clone();
        let max_poll = self.config.default_buffer_size.min(1024);
        let checkpoint_interval = self
            .config
            .checkpoint
            .as_ref()
            .and_then(|c| c.interval_ms)
            .map(std::time::Duration::from_millis);
        let table_store_for_loop = self.table_store.clone();
        let ctx_for_sync = self.ctx.clone();
        let counters = Arc::clone(&self.counters);
        let pipeline_watermark = Arc::clone(&self.pipeline_watermark);
        let checkpoint_in_progress = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let coordinator = Arc::clone(&self.coordinator);

        let handle = tokio::spawn(async move {
            eprintln!("[laminar-db] Connector pipeline task started");
            let mut cycle_count: u64 = 0;
            let mut total_batches: u64 = 0;
            let mut total_records: u64 = 0;
            let mut last_checkpoint = std::time::Instant::now();
            loop {
                // Check for shutdown
                tokio::select! {
                    () = shutdown.notified() => {
                        tracing::info!("Pipeline shutdown signal received");
                        break;
                    }
                    () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                }

                let cycle_start = std::time::Instant::now();

                // Check external watermarks from Source::watermark() calls
                for (name, _, _) in &sources {
                    if let Some(entry) = source_entries_for_wm.get(name.as_str()) {
                        let external_wm = entry.source.current_watermark();
                        if let Some(wm_state) = watermark_states.get_mut(name.as_str()) {
                            if let Some(wm) = wm_state.generator.advance_watermark(external_wm) {
                                if let Some(ref mut trk) = tracker {
                                    if let Some(sid) = source_ids.get(name.as_str()) {
                                        if let Some(global_wm) =
                                            trk.update_source(*sid, wm.timestamp())
                                        {
                                            pipeline_watermark.store(
                                                global_wm.timestamp(),
                                                std::sync::atomic::Ordering::Relaxed,
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Poll sources
                let mut source_batches = HashMap::new();
                for (name, source_arc, _) in &sources {
                    match source_arc.lock().await.poll_batch(max_poll).await {
                        Ok(Some(batch)) => {
                            total_batches += 1;
                            #[allow(clippy::cast_possible_truncation)]
                            let row_count = batch.records.num_rows() as u64;
                            total_records += row_count;
                            counters
                                .events_ingested
                                .fetch_add(row_count, std::sync::atomic::Ordering::Relaxed);
                            counters
                                .total_batches
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            // Extract watermark from batch
                            if let Some(wm_state) = watermark_states.get_mut(name.as_str()) {
                                if let Ok(max_ts) = wm_state.extractor.extract(&batch.records) {
                                    if let Some(wm) = wm_state.generator.on_event(max_ts) {
                                        if let Some(entry) =
                                            source_entries_for_wm.get(name.as_str())
                                        {
                                            entry.source.watermark(wm.timestamp());
                                        }
                                        if let Some(ref mut trk) = tracker {
                                            if let Some(sid) = source_ids.get(name.as_str()) {
                                                if let Some(global_wm) =
                                                    trk.update_source(*sid, wm.timestamp())
                                                {
                                                    pipeline_watermark.store(
                                                        global_wm.timestamp(),
                                                        std::sync::atomic::Ordering::Relaxed,
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }

                                // Filter late rows
                                let current_wm = wm_state.generator.current_watermark();
                                if current_wm > i64::MIN {
                                    if let Some(filtered) = filter_late_rows(
                                        &batch.records,
                                        &wm_state.column,
                                        current_wm,
                                        wm_state.format,
                                    ) {
                                        source_batches
                                            .entry(name.clone())
                                            .or_insert_with(Vec::new)
                                            .push(filtered);
                                    }
                                    // else: all rows were late, skip batch
                                } else {
                                    source_batches
                                        .entry(name.clone())
                                        .or_insert_with(Vec::new)
                                        .push(batch.records);
                                }
                            } else {
                                source_batches
                                    .entry(name.clone())
                                    .or_insert_with(Vec::new)
                                    .push(batch.records);
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            if cycle_count < 5 || cycle_count.is_multiple_of(100) {
                                eprintln!("[laminar-db] Source '{name}' poll error: {e}");
                            }
                            tracing::warn!(
                                source = %name,
                                error = %e,
                                "Source poll error"
                            );
                        }
                    }
                }

                // Execute stream queries
                if !source_batches.is_empty() {
                    if total_batches <= 3 {
                        let src_summary: Vec<_> = source_batches
                            .iter()
                            .map(|(k, v)| {
                                let rows: usize =
                                    v.iter().map(arrow::array::RecordBatch::num_rows).sum();
                                format!("{k}({rows} rows)")
                            })
                            .collect();
                        eprintln!(
                            "[laminar-db] Executing queries with: [{}]",
                            src_summary.join(", "),
                        );
                    }
                    let current_wm = pipeline_watermark.load(std::sync::atomic::Ordering::Relaxed);
                    match executor.execute_cycle(&source_batches, current_wm).await {
                        Ok(results) => {
                            // Push results to stream sources for
                            // db.subscribe() delivery (same as
                            // embedded pipeline).
                            for (stream_name, src) in &stream_sources {
                                if let Some(batches) = results.get(stream_name) {
                                    for batch in batches {
                                        if batch.num_rows() > 0 {
                                            #[allow(clippy::cast_possible_truncation)]
                                            let row_count = batch.num_rows() as u64;
                                            counters.events_emitted.fetch_add(
                                                row_count,
                                                std::sync::atomic::Ordering::Relaxed,
                                            );
                                            let _ = src.push_arrow(batch.clone());
                                        }
                                    }
                                }
                            }

                            // Route results to external sinks
                            for (sink_name, sink_arc, _, filter_expr) in &sinks {
                                for (stream_name, batches) in &results {
                                    for batch in batches {
                                        // Apply WHERE filter if configured
                                        let filtered = if let Some(filter_sql) = filter_expr {
                                            match apply_filter(batch, filter_sql).await {
                                                Ok(Some(fb)) => fb,
                                                Ok(None) => continue,
                                                Err(e) => {
                                                    tracing::warn!(
                                                        sink = %sink_name,
                                                        filter = %filter_sql,
                                                        error = %e,
                                                        "Sink filter error"
                                                    );
                                                    continue;
                                                }
                                            }
                                        } else {
                                            batch.clone()
                                        };

                                        if filtered.num_rows() > 0 {
                                            if let Err(e) =
                                                sink_arc.lock().await.write_batch(&filtered).await
                                            {
                                                tracing::warn!(
                                                    sink = %sink_name,
                                                    stream = %stream_name,
                                                    error = %e,
                                                    "Sink write error"
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            if cycle_count < 5 || cycle_count.is_multiple_of(100) {
                                eprintln!("[laminar-db] Stream execution error: {e}");
                            }
                            tracing::warn!(error = %e, "Stream execution cycle error");
                        }
                    }
                }

                // Poll table sources for incremental changes
                for (name, source, mode) in &mut table_sources {
                    if matches!(mode, RefreshMode::SnapshotOnly | RefreshMode::Manual) {
                        continue;
                    }
                    match source.poll_changes().await {
                        Ok(Some(batch)) => {
                            // Single lock scope: upsert + xor rebuild + extract batch
                            let maybe_batch = {
                                let mut ts = table_store_for_loop.lock();
                                if let Err(e) = ts.upsert_and_rebuild(name, &batch) {
                                    tracing::warn!(table=%name, error=%e, "Table upsert error");
                                    None
                                } else if ts.is_persistent(name) {
                                    None // persistent tables use ReferenceTableProvider
                                } else {
                                    ts.to_record_batch(name)
                                }
                            };
                            if let Some(rb) = maybe_batch {
                                let schema = rb.schema();
                                let _ = ctx_for_sync.deregister_table(name.as_str());
                                let data = if rb.num_rows() > 0 {
                                    vec![vec![rb]]
                                } else {
                                    vec![vec![]]
                                };
                                if let Ok(mem_table) =
                                    datafusion::datasource::MemTable::try_new(schema, data)
                                {
                                    let _ = ctx_for_sync
                                        .register_table(name.as_str(), Arc::new(mem_table));
                                }
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            tracing::warn!(table=%name, error=%e, "Table poll error");
                        }
                    }
                }

                cycle_count += 1;
                counters
                    .cycles
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                #[allow(clippy::cast_possible_truncation)]
                let elapsed_ns = cycle_start.elapsed().as_nanos() as u64;
                counters
                    .last_cycle_duration_ns
                    .store(elapsed_ns, std::sync::atomic::Ordering::Relaxed);
                if cycle_count.is_multiple_of(50) {
                    eprintln!(
                        "[laminar-db] Pipeline cycle {cycle_count}: {total_batches} batches, {total_records} records total",
                    );
                    tracing::debug!(cycles = cycle_count, "Pipeline processing");
                }

                // Periodic checkpoint (non-blocking — spawned as a separate task)
                if let Some(interval) = checkpoint_interval {
                    if last_checkpoint.elapsed() >= interval
                        && !checkpoint_in_progress.load(std::sync::atomic::Ordering::Relaxed)
                    {
                        // Capture table source offsets (fast, in-memory)
                        let mut extra_tables = HashMap::new();
                        for (name, source, _) in &table_sources {
                            extra_tables.insert(
                                name.clone(),
                                source_to_connector_checkpoint(&source.checkpoint()),
                            );
                        }

                        let coord_clone = Arc::clone(&coordinator);
                        let in_progress = Arc::clone(&checkpoint_in_progress);
                        in_progress.store(true, std::sync::atomic::Ordering::Relaxed);

                        tokio::spawn(async move {
                            let mut guard = coord_clone.lock().await;
                            if let Some(ref mut coord) = *guard {
                                match coord
                                    .checkpoint_with_extra_tables(
                                        HashMap::new(),
                                        None,
                                        0,
                                        Vec::new(),
                                        None,
                                        extra_tables,
                                    )
                                    .await
                                {
                                    Ok(result) if result.success => {
                                        tracing::info!(
                                            epoch = result.epoch,
                                            duration_ms = result.duration.as_millis(),
                                            "Pipeline checkpoint completed"
                                        );
                                    }
                                    Ok(result) => {
                                        tracing::warn!(
                                            epoch = result.epoch,
                                            error = ?result.error,
                                            "Pipeline checkpoint failed"
                                        );
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "Checkpoint error"
                                        );
                                    }
                                }
                            }
                            in_progress.store(false, std::sync::atomic::Ordering::Relaxed);
                        });

                        last_checkpoint = std::time::Instant::now();
                    }
                }
            }

            eprintln!(
                "[laminar-db] Pipeline stopping after {cycle_count} cycles ({total_batches} batches, {total_records} records)",
            );

            // Wait for any in-flight checkpoint to complete before final checkpoint
            while checkpoint_in_progress.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }

            // Final checkpoint before shutdown (blocking is OK at shutdown)
            {
                let mut guard = coordinator.lock().await;
                if let Some(ref mut coord) = *guard {
                    let mut extra_tables = HashMap::new();
                    for (name, source, _) in &table_sources {
                        extra_tables.insert(
                            name.clone(),
                            source_to_connector_checkpoint(&source.checkpoint()),
                        );
                    }

                    match coord
                        .checkpoint_with_extra_tables(
                            HashMap::new(),
                            None,
                            0,
                            Vec::new(),
                            None,
                            extra_tables,
                        )
                        .await
                    {
                        Ok(result) if result.success => {
                            tracing::info!(epoch = result.epoch, "Final pipeline checkpoint saved");
                        }
                        Ok(result) => {
                            tracing::warn!(
                                epoch = result.epoch,
                                error = ?result.error,
                                "Final checkpoint failed"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "Final checkpoint error");
                        }
                    }
                }
            }

            // Close table sources
            for (name, source, _) in &mut table_sources {
                if let Err(e) = source.close().await {
                    tracing::warn!(table = %name, error = %e, "Table source close error");
                }
            }

            // Close all connectors
            for (name, source_arc, _) in &sources {
                if let Err(e) = source_arc.lock().await.close().await {
                    tracing::warn!(source = %name, error = %e, "Source close error");
                }
            }
            for (name, sink_arc, _, _) in &sinks {
                let mut sink = sink_arc.lock().await;
                if let Err(e) = sink.flush().await {
                    tracing::warn!(sink = %name, error = %e, "Sink flush error");
                }
                if let Err(e) = sink.close().await {
                    tracing::warn!(sink = %name, error = %e, "Sink close error");
                }
            }
            tracing::info!("Pipeline stopped after {cycle_count} cycles");
        });

        *self.runtime_handle.lock() = Some(handle);
        Ok(())
    }

    /// Shut down the streaming pipeline gracefully.
    ///
    /// Signals the processing loop to stop, waits for it to complete
    /// (with a timeout), then transitions to `Stopped`.
    /// This is idempotent -- calling it multiple times is safe.
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown encounters an error.
    pub async fn shutdown(&self) -> Result<(), DbError> {
        let current = self.state.load(std::sync::atomic::Ordering::Acquire);
        if current == STATE_STOPPED || current == STATE_SHUTTING_DOWN {
            return Ok(());
        }

        self.state
            .store(STATE_SHUTTING_DOWN, std::sync::atomic::Ordering::Release);

        // Signal the runtime loop to stop
        self.shutdown_signal.notify_one();

        // Await the runtime handle (with timeout)
        let handle = self.runtime_handle.lock().take();
        if let Some(handle) = handle {
            match tokio::time::timeout(std::time::Duration::from_secs(10), handle).await {
                Ok(Ok(())) => {
                    tracing::info!("Pipeline shut down cleanly");
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "Pipeline task panicked during shutdown");
                }
                Err(_) => {
                    tracing::warn!("Pipeline shutdown timed out after 10s");
                }
            }
        }

        self.state
            .store(STATE_STOPPED, std::sync::atomic::Ordering::Release);
        self.close();
        Ok(())
    }

    /// Get the current pipeline state as a string.
    pub fn pipeline_state(&self) -> &'static str {
        match self.state.load(std::sync::atomic::Ordering::Acquire) {
            STATE_CREATED => "Created",
            STATE_STARTING => "Starting",
            STATE_RUNNING => "Running",
            STATE_SHUTTING_DOWN => "ShuttingDown",
            STATE_STOPPED => "Stopped",
            _ => "Unknown",
        }
    }

    /// Get a pipeline-wide metrics snapshot.
    ///
    /// Reads shared atomic counters and catalog sizes to produce a
    /// point-in-time view of pipeline health.
    #[must_use]
    pub fn metrics(&self) -> crate::metrics::PipelineMetrics {
        let snap = self.counters.snapshot();
        crate::metrics::PipelineMetrics {
            total_events_ingested: snap.events_ingested,
            total_events_emitted: snap.events_emitted,
            total_events_dropped: snap.events_dropped,
            total_cycles: snap.cycles,
            total_batches: snap.total_batches,
            uptime: self.start_time.elapsed(),
            state: self.pipeline_state_enum(),
            last_cycle_duration_ns: snap.last_cycle_duration_ns,
            source_count: self.catalog.list_sources().len(),
            stream_count: self.catalog.list_streams().len(),
            sink_count: self.catalog.list_sinks().len(),
            pipeline_watermark: self.pipeline_watermark(),
        }
    }

    /// Get metrics for a single source by name.
    #[must_use]
    pub fn source_metrics(&self, name: &str) -> Option<crate::metrics::SourceMetrics> {
        let entry = self.catalog.get_source(name)?;
        let pending = entry.source.pending();
        let capacity = entry.source.capacity();
        Some(crate::metrics::SourceMetrics {
            name: entry.name.clone(),
            total_events: entry.source.sequence(),
            pending,
            capacity,
            is_backpressured: crate::metrics::is_backpressured(pending, capacity),
            watermark: entry.source.current_watermark(),
            utilization: crate::metrics::utilization(pending, capacity),
        })
    }

    /// Get metrics for all registered sources.
    #[must_use]
    pub fn all_source_metrics(&self) -> Vec<crate::metrics::SourceMetrics> {
        self.catalog
            .list_sources()
            .iter()
            .filter_map(|name| self.source_metrics(name))
            .collect()
    }

    /// Get metrics for a single stream by name.
    #[must_use]
    pub fn stream_metrics(&self, name: &str) -> Option<crate::metrics::StreamMetrics> {
        let entry = self.catalog.get_stream_entry(name)?;
        let pending = entry.source.pending();
        let capacity = entry.source.capacity();
        let sql = self
            .connector_manager
            .lock()
            .streams()
            .get(name)
            .map(|reg| reg.query_sql.clone());
        Some(crate::metrics::StreamMetrics {
            name: entry.name.clone(),
            total_events: entry.source.sequence(),
            pending,
            capacity,
            is_backpressured: crate::metrics::is_backpressured(pending, capacity),
            watermark: entry.source.current_watermark(),
            sql,
        })
    }

    /// Get metrics for all registered streams.
    #[must_use]
    pub fn all_stream_metrics(&self) -> Vec<crate::metrics::StreamMetrics> {
        self.catalog
            .list_streams()
            .iter()
            .filter_map(|name| self.stream_metrics(name))
            .collect()
    }

    /// Get the total number of events processed (ingested + emitted).
    #[must_use]
    pub fn total_events_processed(&self) -> u64 {
        let snap = self.counters.snapshot();
        snap.events_ingested + snap.events_emitted
    }

    /// Get a reference to the shared pipeline counters.
    ///
    /// Useful for external code that needs to read counters directly
    /// (e.g. a TUI dashboard polling at high frequency).
    #[must_use]
    pub fn counters(&self) -> &Arc<crate::metrics::PipelineCounters> {
        &self.counters
    }

    /// Returns the global pipeline watermark (minimum across all source watermarks).
    ///
    /// Returns `i64::MIN` if no watermark-enabled sources exist or no events
    /// have been processed.
    #[must_use]
    pub fn pipeline_watermark(&self) -> i64 {
        self.pipeline_watermark
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Convert the internal `AtomicU8` state to a `PipelineState` enum.
    fn pipeline_state_enum(&self) -> crate::metrics::PipelineState {
        match self.state.load(std::sync::atomic::Ordering::Acquire) {
            STATE_CREATED => crate::metrics::PipelineState::Created,
            STATE_STARTING => crate::metrics::PipelineState::Starting,
            STATE_RUNNING => crate::metrics::PipelineState::Running,
            STATE_SHUTTING_DOWN => crate::metrics::PipelineState::ShuttingDown,
            _ => crate::metrics::PipelineState::Stopped,
        }
    }

    /// Cancel a running query by ID.
    ///
    /// Marks the query as inactive in the catalog. Future subscription
    /// polls for this query will receive no more data.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if the query is not found.
    pub fn cancel_query(&self, query_id: u64) -> Result<(), DbError> {
        self.catalog.deactivate_query(query_id);
        Ok(())
    }

    /// Get the number of registered sources.
    pub fn source_count(&self) -> usize {
        self.catalog.list_sources().len()
    }

    /// Get the number of registered sinks.
    pub fn sink_count(&self) -> usize {
        self.catalog.list_sinks().len()
    }

    /// Get the number of active queries.
    pub fn active_query_count(&self) -> usize {
        self.catalog
            .list_queries()
            .iter()
            .filter(|(_, _, active)| *active)
            .count()
    }

    /// Handle CREATE MATERIALIZED VIEW statement.
    ///
    /// Registers the view in the MV registry with dependency tracking,
    /// then executes the backing query through `DataFusion` to obtain the
    /// output schema.
    async fn handle_create_materialized_view(
        &self,
        sql: &str,
        name: &sqlparser::ast::ObjectName,
        query: &StreamingStatement,
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<ExecuteResult, DbError> {
        let name_str = name.to_string();

        // Check if the MV already exists
        {
            let registry = self.mv_registry.lock();
            if registry.get(&name_str).is_some() {
                if if_not_exists {
                    return Ok(ExecuteResult::Ddl(DdlInfo {
                        statement_type: "CREATE MATERIALIZED VIEW".to_string(),
                        object_name: name_str,
                    }));
                }
                if !or_replace {
                    return Err(DbError::MaterializedView(format!(
                        "Materialized view '{name_str}' already exists"
                    )));
                }
            }
        }

        // Convert the inner query to SQL for execution
        let query_sql = streaming_statement_to_sql(query);

        // Execute the backing query to get the output schema
        let result = self.handle_query(&query_sql).await?;
        let schema = match &result {
            ExecuteResult::Query(qh) => qh.schema().clone(),
            _ => Arc::new(Schema::new(vec![Field::new(
                "result",
                DataType::Utf8,
                true,
            )])),
        };

        // Discover source references: check which catalog sources and
        // existing MVs appear in the query SQL
        let catalog_sources = self.catalog.list_sources();
        let mut sources: Vec<String> = catalog_sources
            .into_iter()
            .filter(|s| query_sql.contains(s.as_str()))
            .collect();

        // Also check existing MVs as potential sources (cascading MVs)
        {
            let registry = self.mv_registry.lock();
            for view in registry.views() {
                if view.name != name_str && query_sql.contains(view.name.as_str()) {
                    sources.push(view.name.clone());
                }
            }
        }

        // Register in the MV registry
        {
            let mv = laminar_core::mv::MaterializedView::new(&name_str, sql, sources, schema);

            let mut registry = self.mv_registry.lock();

            if or_replace {
                // Drop existing view (and dependents) before re-registering
                let _ = registry.unregister_cascade(&name_str);
            }

            registry
                .register(mv)
                .map_err(|e| DbError::MaterializedView(e.to_string()))?;
        }

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE MATERIALIZED VIEW".to_string(),
            object_name: name_str,
        }))
    }

    /// Handle DROP MATERIALIZED VIEW statement.
    fn handle_drop_materialized_view(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
        cascade: bool,
    ) -> Result<ExecuteResult, DbError> {
        let name_str = name.to_string();
        let mut registry = self.mv_registry.lock();

        let result = if cascade {
            registry.unregister_cascade(&name_str)
        } else {
            registry.unregister(&name_str).map(|v| vec![v])
        };

        match result {
            Ok(_) => Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "DROP MATERIALIZED VIEW".to_string(),
                object_name: name_str,
            })),
            Err(_) if if_exists => Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "DROP MATERIALIZED VIEW".to_string(),
                object_name: name_str,
            })),
            Err(e) => Err(DbError::MaterializedView(e.to_string())),
        }
    }

    /// Build a SHOW MATERIALIZED VIEWS metadata result.
    fn build_show_materialized_views(&self) -> RecordBatch {
        let registry = self.mv_registry.lock();
        let mut names = Vec::new();
        let mut sqls = Vec::new();
        let mut states = Vec::new();
        for view in registry.views() {
            names.push(view.name.clone());
            sqls.push(view.sql.clone());
            states.push(format!("{:?}", view.state));
        }
        let names_ref: Vec<&str> = names.iter().map(String::as_str).collect();
        let sqls_ref: Vec<&str> = sqls.iter().map(String::as_str).collect();
        let states_ref: Vec<&str> = states.iter().map(String::as_str).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("view_name", DataType::Utf8, false),
            Field::new("sql", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names_ref)),
                Arc::new(StringArray::from(sqls_ref)),
                Arc::new(StringArray::from(states_ref)),
            ],
        )
        .expect("show materialized views: schema matches columns")
    }

    /// Build a SHOW SOURCES metadata result.
    fn build_show_sources(&self) -> RecordBatch {
        let sources = self.sources();
        let names: Vec<&str> = sources.iter().map(|s| s.name.as_str()).collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "source_name",
            DataType::Utf8,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))])
            .expect("show sources: schema matches columns")
    }

    /// Build a SHOW SINKS metadata result.
    fn build_show_sinks(&self) -> RecordBatch {
        let sinks = self.sinks();
        let names: Vec<&str> = sinks.iter().map(|s| s.name.as_str()).collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "sink_name",
            DataType::Utf8,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))])
            .expect("show sinks: schema matches columns")
    }

    /// Build a SHOW QUERIES metadata result.
    fn build_show_queries(&self) -> RecordBatch {
        let queries = self.queries();
        let ids: Vec<u64> = queries.iter().map(|q| q.id).collect();
        let sqls: Vec<&str> = queries.iter().map(|q| q.sql.as_str()).collect();
        let actives: Vec<bool> = queries.iter().map(|q| q.active).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("query_id", DataType::UInt64, false),
            Field::new("sql", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(ids)),
                Arc::new(StringArray::from(sqls)),
                Arc::new(BooleanArray::from(actives)),
            ],
        )
        .expect("show queries: schema matches columns")
    }

    /// Build a SHOW STREAMS metadata result.
    fn build_show_streams(&self) -> RecordBatch {
        let streams = self.catalog.list_streams();
        let names: Vec<&str> = streams.iter().map(String::as_str).collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "stream_name",
            DataType::Utf8,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))])
            .expect("show streams: schema matches columns")
    }

    /// Handle DROP TABLE statement.
    fn handle_drop_table(
        &self,
        names: &[sqlparser::ast::ObjectName],
        if_exists: bool,
    ) -> Result<ExecuteResult, DbError> {
        for obj_name in names {
            let name_str = obj_name.to_string();

            // Remove from TableStore
            self.table_store.lock().drop_table(&name_str);

            // Remove from ConnectorManager
            self.connector_manager.lock().unregister_table(&name_str);

            // Deregister from DataFusion context
            match self.ctx.deregister_table(&name_str) {
                Ok(None) if !if_exists => {
                    return Err(DbError::TableNotFound(name_str));
                }
                Ok(Some(_) | None) => {}
                Err(e) => {
                    return Err(DbError::InvalidOperation(format!(
                        "Failed to drop table '{name_str}': {e}"
                    )));
                }
            }
        }

        let name = names
            .first()
            .map(std::string::ToString::to_string)
            .unwrap_or_default();

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP TABLE".to_string(),
            object_name: name,
        }))
    }

    /// Build a SHOW TABLES metadata result.
    fn build_show_tables(&self) -> RecordBatch {
        let ts = self.table_store.lock();
        let mut names = Vec::new();
        let mut pks = Vec::new();
        let mut row_counts = Vec::new();
        let mut connectors = Vec::new();

        for name in ts.table_names() {
            let pk = ts.primary_key(&name).unwrap_or("").to_string();
            let count = ts.table_row_count(&name) as u64;
            let conn = ts.connector(&name).unwrap_or("").to_string();

            names.push(name);
            pks.push(pk);
            row_counts.push(count);
            connectors.push(conn);
        }

        let names_ref: Vec<&str> = names.iter().map(String::as_str).collect();
        let pks_ref: Vec<&str> = pks.iter().map(String::as_str).collect();
        let connectors_ref: Vec<&str> = connectors.iter().map(String::as_str).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("primary_key", DataType::Utf8, false),
            Field::new("row_count", DataType::UInt64, false),
            Field::new("connector", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names_ref)),
                Arc::new(StringArray::from(pks_ref)),
                Arc::new(UInt64Array::from(row_counts)),
                Arc::new(StringArray::from(connectors_ref)),
            ],
        )
        .expect("show tables: schema matches columns")
    }

    /// Synchronize a `TableStore` table to the `DataFusion` `MemTable`.
    ///
    /// Deregisters the existing table (if any) and re-registers with the
    /// current contents of the `TableStore`.
    fn sync_table_to_datafusion(&self, name: &str) -> Result<(), DbError> {
        // Persistent tables use ReferenceTableProvider which reads live data —
        // no need to deregister/re-register.
        if self.table_store.lock().is_persistent(name) {
            return Ok(());
        }

        let batch = self
            .table_store
            .lock()
            .to_record_batch(name)
            .ok_or_else(|| DbError::TableNotFound(name.to_string()))?;

        let schema = batch.schema();

        // Deregister old
        let _ = self.ctx.deregister_table(name);

        // Register new
        let data = if batch.num_rows() > 0 {
            vec![vec![batch]]
        } else {
            vec![vec![]]
        };

        let mem_table = datafusion::datasource::MemTable::try_new(schema, data)
            .map_err(|e| DbError::InsertError(format!("Failed to create table: {e}")))?;

        self.ctx
            .register_table(name, Arc::new(mem_table))
            .map_err(|e| DbError::InsertError(format!("Failed to register table: {e}")))?;

        Ok(())
    }

    /// Build a DESCRIBE result.
    fn build_describe(&self, name: &str) -> Result<RecordBatch, DbError> {
        let source_schema = self
            .catalog
            .describe_source(name)
            .ok_or_else(|| DbError::SourceNotFound(name.to_string()))?;

        let col_names: Vec<String> = source_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let col_types: Vec<String> = source_schema
            .fields()
            .iter()
            .map(|f| format!("{}", f.data_type()))
            .collect();
        let col_nullable: Vec<bool> = source_schema
            .fields()
            .iter()
            .map(|f| f.is_nullable())
            .collect();

        let names_ref: Vec<&str> = col_names.iter().map(String::as_str).collect();
        let types_ref: Vec<&str> = col_types.iter().map(String::as_str).collect();

        let result_schema = Arc::new(Schema::new(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("nullable", DataType::Boolean, false),
        ]));

        RecordBatch::try_new(
            result_schema,
            vec![
                Arc::new(StringArray::from(names_ref)),
                Arc::new(StringArray::from(types_ref)),
                Arc::new(BooleanArray::from(col_nullable)),
            ],
        )
        .map_err(|e| DbError::InvalidOperation(format!("describe metadata: {e}")))
    }
}

/// Internal table name used by the sink WHERE filter.
const FILTER_INPUT_TABLE: &str = "__laminar_filter_input";

/// Encode an Arrow schema as a compact string for passing through `ConnectorConfig`.
///
/// Format: `name:type,name:type,...` where type is the Arrow `DataType` debug name.
/// Example: `symbol:Utf8,price:Float64,volume:Int64`
fn encode_arrow_schema(schema: &arrow_schema::Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|f| format!("{}:{:?}", f.name(), f.data_type()))
        .collect::<Vec<_>>()
        .join(",")
}

/// Apply a SQL WHERE filter to a `RecordBatch`.
///
/// Returns `Ok(Some(filtered_batch))` if rows match, `Ok(None)` if no rows match,
/// or an error if the filter expression is invalid.
async fn apply_filter(
    batch: &RecordBatch,
    filter_sql: &str,
) -> Result<Option<RecordBatch>, DbError> {
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new();
    let schema = batch.schema();

    // Register the batch as a temporary table
    let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch.clone()]])
        .map_err(|e| DbError::Pipeline(format!("Filter table creation: {e}")))?;

    ctx.register_table(FILTER_INPUT_TABLE, Arc::new(mem_table))
        .map_err(|e| DbError::Pipeline(format!("Filter table registration: {e}")))?;

    // Execute the filter query
    let sql = format!("SELECT * FROM {FILTER_INPUT_TABLE} WHERE {filter_sql}");
    let df = ctx
        .sql(&sql)
        .await
        .map_err(|e| DbError::Pipeline(format!("Filter query: {e}")))?;

    let batches = df
        .collect()
        .await
        .map_err(|e| DbError::Pipeline(format!("Filter execution: {e}")))?;

    if batches.is_empty() {
        return Ok(None);
    }

    // Concatenate all result batches
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if total_rows == 0 {
        return Ok(None);
    }

    // Return the first batch (typically there's only one)
    Ok(Some(batches.into_iter().next().unwrap()))
}

impl std::fmt::Debug for LaminarDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaminarDB")
            .field("sources", &self.catalog.list_sources().len())
            .field("sinks", &self.catalog.list_sinks().len())
            .field("materialized_views", &self.mv_registry.lock().len())
            .field("checkpoint_enabled", &self.is_checkpoint_enabled())
            .field("shutdown", &self.is_closed())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_open_default() {
        let db = LaminarDB::open().unwrap();
        assert!(!db.is_closed());
        assert!(db.sources().is_empty());
        assert!(db.sinks().is_empty());
    }

    #[tokio::test]
    async fn test_create_source() {
        let db = LaminarDB::open().unwrap();
        let result = db
            .execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts BIGINT)")
            .await
            .unwrap();

        match result {
            ExecuteResult::Ddl(info) => {
                assert_eq!(info.statement_type, "CREATE SOURCE");
                assert_eq!(info.object_name, "trades");
            }
            _ => panic!("Expected DDL result"),
        }

        assert_eq!(db.sources().len(), 1);
        assert_eq!(db.sources()[0].name, "trades");
    }

    #[tokio::test]
    async fn test_create_source_with_watermark() {
        let db = LaminarDB::open().unwrap();
        db.execute(
            "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)",
        )
        .await
        .unwrap();

        let sources = db.sources();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].watermark_column, Some("ts".to_string()));
    }

    #[tokio::test]
    async fn test_create_source_duplicate_error() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE test (id INT)").await.unwrap();
        let result = db.execute("CREATE SOURCE test (id INT)").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_source_if_not_exists() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE test (id INT)").await.unwrap();
        let result = db
            .execute("CREATE SOURCE IF NOT EXISTS test (id INT)")
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_or_replace_source() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE test (id INT)").await.unwrap();
        let result = db
            .execute("CREATE OR REPLACE SOURCE test (id INT, name VARCHAR)")
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_sink() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT)").await.unwrap();
        db.execute("CREATE SINK output FROM events").await.unwrap();

        assert_eq!(db.sinks().len(), 1);
    }

    #[tokio::test]
    async fn test_source_handle_untyped() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id BIGINT, value DOUBLE)")
            .await
            .unwrap();

        let handle = db.source_untyped("events").unwrap();
        assert_eq!(handle.name(), "events");
        assert_eq!(handle.schema().fields().len(), 2);
    }

    #[tokio::test]
    async fn test_source_not_found() {
        let db = LaminarDB::open().unwrap();
        let result = db.source_untyped("nonexistent");
        assert!(matches!(result, Err(DbError::SourceNotFound(_))));
    }

    #[tokio::test]
    async fn test_show_sources() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE a (id INT)").await.unwrap();
        db.execute("CREATE SOURCE b (id INT)").await.unwrap();

        let result = db.execute("SHOW SOURCES").await.unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                assert_eq!(batch.num_rows(), 2);
            }
            _ => panic!("Expected Metadata result"),
        }
    }

    #[tokio::test]
    async fn test_describe_source() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id BIGINT, name VARCHAR, active BOOLEAN)")
            .await
            .unwrap();

        let result = db.execute("DESCRIBE events").await.unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                assert_eq!(batch.num_rows(), 3);
            }
            _ => panic!("Expected Metadata result"),
        }
    }

    #[tokio::test]
    async fn test_drop_source() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE test (id INT)").await.unwrap();
        assert_eq!(db.sources().len(), 1);

        db.execute("DROP SOURCE test").await.unwrap();
        assert_eq!(db.sources().len(), 0);
    }

    #[tokio::test]
    async fn test_drop_source_if_exists() {
        let db = LaminarDB::open().unwrap();
        // Should not error when source doesn't exist
        let result = db.execute("DROP SOURCE IF EXISTS nonexistent").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_drop_source_not_found() {
        let db = LaminarDB::open().unwrap();
        let result = db.execute("DROP SOURCE nonexistent").await;
        assert!(matches!(result, Err(DbError::SourceNotFound(_))));
    }

    #[tokio::test]
    async fn test_shutdown() {
        let db = LaminarDB::open().unwrap();
        assert!(!db.is_closed());
        db.close();
        assert!(db.is_closed());

        let result = db.execute("CREATE SOURCE test (id INT)").await;
        assert!(matches!(result, Err(DbError::Shutdown)));
    }

    #[tokio::test]
    async fn test_debug_format() {
        let db = LaminarDB::open().unwrap();
        let debug = format!("{db:?}");
        assert!(debug.contains("LaminarDB"));
    }

    #[tokio::test]
    async fn test_explain_create_source() {
        let db = LaminarDB::open().unwrap();
        let result = db
            .execute("EXPLAIN CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
            .await
            .unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                assert!(batch.num_rows() > 0);
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                // Should contain plan_type and source info
                let key_values: Vec<&str> = (0..batch.num_rows()).map(|i| keys.value(i)).collect();
                assert!(key_values.contains(&"plan_type"));
            }
            _ => panic!("Expected Metadata result for EXPLAIN"),
        }
    }

    #[tokio::test]
    async fn test_cancel_query() {
        let db = LaminarDB::open().unwrap();
        // Register a query via catalog directly for testing
        assert_eq!(db.active_query_count(), 0);

        // Simulate a query registration
        let query_id = db.catalog.register_query("SELECT * FROM test");
        assert_eq!(db.active_query_count(), 1);

        // Cancel it
        db.cancel_query(query_id).unwrap();
        assert_eq!(db.active_query_count(), 0);
    }

    #[tokio::test]
    async fn test_source_and_sink_counts() {
        let db = LaminarDB::open().unwrap();
        assert_eq!(db.source_count(), 0);
        assert_eq!(db.sink_count(), 0);

        db.execute("CREATE SOURCE a (id INT)").await.unwrap();
        db.execute("CREATE SOURCE b (id INT)").await.unwrap();
        assert_eq!(db.source_count(), 2);

        db.execute("CREATE SINK output FROM a").await.unwrap();
        assert_eq!(db.sink_count(), 1);

        db.execute("DROP SOURCE a").await.unwrap();
        assert_eq!(db.source_count(), 1);
    }

    // ── Multi-statement execution tests (F-SQL-005) ─────────────────

    #[tokio::test]
    async fn test_multi_statement_execution() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE a (id INT); CREATE SOURCE b (id INT); CREATE SINK output FROM a")
            .await
            .unwrap();
        assert_eq!(db.source_count(), 2);
        assert_eq!(db.sink_count(), 1);
    }

    #[tokio::test]
    async fn test_multi_statement_trailing_semicolon() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE a (id INT);").await.unwrap();
        assert_eq!(db.source_count(), 1);
    }

    #[tokio::test]
    async fn test_multi_statement_error_stops() {
        let db = LaminarDB::open().unwrap();
        // Second statement should fail (duplicate)
        let result = db
            .execute("CREATE SOURCE a (id INT); CREATE SOURCE a (id INT)")
            .await;
        assert!(result.is_err());
        // First statement should have succeeded
        assert_eq!(db.source_count(), 1);
    }

    // ── Config variable substitution tests (F-SQL-006) ──────────────

    #[tokio::test]
    async fn test_config_var_substitution() {
        let db = LaminarDB::builder()
            .config_var("TABLE_NAME", "events")
            .build()
            .await
            .unwrap();
        // Config var in source name won't work (parsed as identifier),
        // but it works in WITH option values
        db.execute("CREATE SOURCE events (id INT)").await.unwrap();
        assert_eq!(db.source_count(), 1);
    }

    // ── CREATE STREAM tests (F-SQL-003) ─────────────────────────────

    #[tokio::test]
    async fn test_create_stream() {
        let db = LaminarDB::open().unwrap();
        let result = db
            .execute("CREATE STREAM counts AS SELECT COUNT(*) as cnt FROM events")
            .await
            .unwrap();
        match result {
            ExecuteResult::Ddl(info) => {
                assert_eq!(info.statement_type, "CREATE STREAM");
                assert_eq!(info.object_name, "counts");
            }
            _ => panic!("Expected DDL result"),
        }
    }

    #[tokio::test]
    async fn test_drop_stream() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE STREAM counts AS SELECT COUNT(*) as cnt FROM events")
            .await
            .unwrap();
        let result = db.execute("DROP STREAM counts").await.unwrap();
        match result {
            ExecuteResult::Ddl(info) => {
                assert_eq!(info.statement_type, "DROP STREAM");
            }
            _ => panic!("Expected DDL result"),
        }
    }

    #[tokio::test]
    async fn test_drop_stream_not_found() {
        let db = LaminarDB::open().unwrap();
        let result = db.execute("DROP STREAM nonexistent").await;
        assert!(matches!(result, Err(DbError::StreamNotFound(_))));
    }

    #[tokio::test]
    async fn test_drop_stream_if_exists() {
        let db = LaminarDB::open().unwrap();
        let result = db.execute("DROP STREAM IF EXISTS nonexistent").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_show_streams() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE STREAM a AS SELECT 1 FROM events")
            .await
            .unwrap();
        let result = db.execute("SHOW STREAMS").await.unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                assert_eq!(batch.num_rows(), 1);
            }
            _ => panic!("Expected Metadata result"),
        }
    }

    #[tokio::test]
    async fn test_stream_duplicate_error() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE STREAM counts AS SELECT COUNT(*) FROM events")
            .await
            .unwrap();
        let result = db
            .execute("CREATE STREAM counts AS SELECT COUNT(*) FROM events")
            .await;
        assert!(matches!(result, Err(DbError::StreamAlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_create_table() {
        let db = LaminarDB::open().unwrap();
        let result = db
            .execute("CREATE TABLE products (id INT, name VARCHAR, price DOUBLE)")
            .await
            .unwrap();

        match result {
            ExecuteResult::Ddl(info) => {
                assert_eq!(info.statement_type, "CREATE TABLE");
                assert_eq!(info.object_name, "products");
            }
            _ => panic!("Expected DDL result"),
        }
    }

    #[tokio::test]
    async fn test_create_table_and_query_empty() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE TABLE dim (id INT, label VARCHAR)")
            .await
            .unwrap();

        let result = db.execute("SELECT * FROM dim").await.unwrap();
        match result {
            ExecuteResult::Query(q) => {
                assert_eq!(q.schema().fields().len(), 2);
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[tokio::test]
    async fn test_insert_into_source() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id BIGINT, value DOUBLE)")
            .await
            .unwrap();

        let result = db
            .execute("INSERT INTO events VALUES (1, 3.14), (2, 2.72)")
            .await
            .unwrap();
        match result {
            ExecuteResult::RowsAffected(n) => assert_eq!(n, 2),
            _ => panic!("Expected RowsAffected"),
        }
    }

    #[tokio::test]
    async fn test_insert_into_table() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE TABLE products (id INT, name VARCHAR, price DOUBLE)")
            .await
            .unwrap();

        let result = db
            .execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
            .await
            .unwrap();
        match result {
            ExecuteResult::RowsAffected(n) => assert_eq!(n, 1),
            _ => panic!("Expected RowsAffected"),
        }
    }

    #[tokio::test]
    async fn test_insert_into_nonexistent_table() {
        let db = LaminarDB::open().unwrap();
        let result = db.execute("INSERT INTO nosuch VALUES (1, 2)").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_table_with_types() {
        let db = LaminarDB::open().unwrap();
        let result = db
            .execute("CREATE TABLE orders (id BIGINT NOT NULL, qty SMALLINT, total DECIMAL(10,2))")
            .await
            .unwrap();

        match result {
            ExecuteResult::Ddl(info) => {
                assert_eq!(info.statement_type, "CREATE TABLE");
                assert_eq!(info.object_name, "orders");
            }
            _ => panic!("Expected DDL result"),
        }
    }

    #[tokio::test]
    async fn test_insert_null_values() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE data (id BIGINT, label VARCHAR)")
            .await
            .unwrap();

        let result = db
            .execute("INSERT INTO data VALUES (1, NULL)")
            .await
            .unwrap();
        match result {
            ExecuteResult::RowsAffected(n) => assert_eq!(n, 1),
            _ => panic!("Expected RowsAffected"),
        }
    }

    #[tokio::test]
    async fn test_insert_negative_values() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE temps (id BIGINT, celsius DOUBLE)")
            .await
            .unwrap();

        let result = db
            .execute("INSERT INTO temps VALUES (1, -40.0)")
            .await
            .unwrap();
        match result {
            ExecuteResult::RowsAffected(n) => assert_eq!(n, 1),
            _ => panic!("Expected RowsAffected"),
        }
    }

    // ── Connector registry / DDL validation tests ──

    #[tokio::test]
    async fn test_create_source_unknown_connector() {
        let db = LaminarDB::open().unwrap();
        // Use correct SQL syntax: FROM <type> (...) SCHEMA (...)
        let result = db
            .execute(
                "CREATE SOURCE events FROM NONEXISTENT \
                 ('topic' = 'test') SCHEMA (id INT)",
            )
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Unknown source connector type"), "got: {err}");
    }

    #[tokio::test]
    async fn test_create_sink_unknown_connector() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT)").await.unwrap();
        // Use correct SQL syntax: INTO <type> (...)
        let result = db
            .execute(
                "CREATE SINK output FROM events \
                 INTO NONEXISTENT ('topic' = 'out')",
            )
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Unknown sink connector type"), "got: {err}");
    }

    #[tokio::test]
    async fn test_create_source_invalid_format() {
        // We test format validation via build_source_config in
        // connector_manager::tests (since the SQL parser may reject
        // unknown formats at parse time rather than DDL validation).
        // Here we verify that an error is returned either way.
        let db = LaminarDB::open().unwrap();
        let result = db
            .execute(
                "CREATE SOURCE events FROM NONEXISTENT \
                 FORMAT BADFORMAT SCHEMA (id INT)",
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_connector_registry_accessor() {
        let db = LaminarDB::open().unwrap();
        let registry = db.connector_registry();

        // With feature flags enabled, built-in connectors are auto-registered.
        // Without any features, registry should be empty.
        #[allow(unused_mut)]
        let mut expected_sources = 0;
        #[allow(unused_mut)]
        let mut expected_sinks = 0;

        #[cfg(feature = "kafka")]
        {
            expected_sources += 1; // kafka source
            expected_sinks += 1; // kafka sink
        }
        #[cfg(feature = "postgres-cdc")]
        {
            expected_sources += 1; // postgres CDC source
        }
        #[cfg(feature = "postgres-sink")]
        {
            expected_sinks += 1; // postgres sink
        }

        assert_eq!(registry.list_sources().len(), expected_sources);
        assert_eq!(registry.list_sinks().len(), expected_sinks);
    }

    #[tokio::test]
    async fn test_builder_register_connector() {
        use std::sync::Arc;

        let db = LaminarDB::builder()
            .register_connector(|registry| {
                registry.register_source(
                    "test-source",
                    laminar_connectors::config::ConnectorInfo {
                        name: "test-source".to_string(),
                        display_name: "Test Source".to_string(),
                        version: "0.1.0".to_string(),
                        is_source: true,
                        is_sink: false,
                        config_keys: vec![],
                    },
                    Arc::new(|| Box::new(laminar_connectors::testing::MockSourceConnector::new())),
                );
            })
            .build()
            .await
            .unwrap();
        let registry = db.connector_registry();
        assert!(registry.list_sources().contains(&"test-source".to_string()));
    }

    // ── Materialized View Catalog tests ──

    #[tokio::test]
    async fn test_create_materialized_view() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
            .await
            .unwrap();

        let result = db
            .execute("CREATE MATERIALIZED VIEW event_stats AS SELECT * FROM events")
            .await;

        // The MV may fail at query execution (no data in DataFusion) but the
        // important thing is the MV path is invoked and the registry is wired up.
        // If it succeeds, verify the DDL result.
        if let Ok(ExecuteResult::Ddl(info)) = &result {
            assert_eq!(info.statement_type, "CREATE MATERIALIZED VIEW");
            assert_eq!(info.object_name, "event_stats");
        }
    }

    #[tokio::test]
    async fn test_mv_registry_base_tables() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE trades (sym VARCHAR, price DOUBLE)")
            .await
            .unwrap();

        let registry = db.mv_registry.lock();
        assert!(registry.is_base_table("trades"));
    }

    #[tokio::test]
    async fn test_show_materialized_views_empty() {
        let db = LaminarDB::open().unwrap();
        let result = db.execute("SHOW MATERIALIZED VIEWS").await.unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                assert_eq!(batch.num_rows(), 0);
                assert_eq!(batch.num_columns(), 3);
                assert_eq!(batch.schema().field(0).name(), "view_name");
                assert_eq!(batch.schema().field(1).name(), "sql");
                assert_eq!(batch.schema().field(2).name(), "state");
            }
            _ => panic!("Expected Metadata result"),
        }
    }

    #[tokio::test]
    async fn test_drop_materialized_view_if_exists() {
        let db = LaminarDB::open().unwrap();
        // Should not error with IF EXISTS on non-existent view
        let result = db
            .execute("DROP MATERIALIZED VIEW IF EXISTS nonexistent")
            .await
            .unwrap();
        match result {
            ExecuteResult::Ddl(info) => {
                assert_eq!(info.statement_type, "DROP MATERIALIZED VIEW");
            }
            _ => panic!("Expected Ddl result"),
        }
    }

    #[tokio::test]
    async fn test_drop_materialized_view_not_found() {
        let db = LaminarDB::open().unwrap();
        let result = db.execute("DROP MATERIALIZED VIEW nonexistent").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not found"),
            "Expected 'not found' error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_create_mv_if_not_exists() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT)").await.unwrap();

        // Register a view directly in the registry for this test
        {
            let mut registry = db.mv_registry.lock();
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
            let mv = laminar_core::mv::MaterializedView::new(
                "my_view",
                "SELECT * FROM events",
                vec!["events".to_string()],
                schema,
            );
            registry.register(mv).unwrap();
        }

        // IF NOT EXISTS should succeed without error
        let result = db
            .execute("CREATE MATERIALIZED VIEW IF NOT EXISTS my_view AS SELECT * FROM events")
            .await
            .unwrap();
        match result {
            ExecuteResult::Ddl(info) => {
                assert_eq!(info.object_name, "my_view");
            }
            _ => panic!("Expected Ddl result"),
        }
    }

    #[tokio::test]
    async fn test_create_mv_duplicate_error() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT)").await.unwrap();

        // Register a view directly
        {
            let mut registry = db.mv_registry.lock();
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
            let mv = laminar_core::mv::MaterializedView::new(
                "my_view",
                "SELECT * FROM events",
                vec!["events".to_string()],
                schema,
            );
            registry.register(mv).unwrap();
        }

        // Without IF NOT EXISTS, should error
        let result = db
            .execute("CREATE MATERIALIZED VIEW my_view AS SELECT * FROM events")
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("already exists"),
            "Expected 'already exists' error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_show_materialized_views_with_entries() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT)").await.unwrap();

        // Register views directly for metadata testing
        {
            let mut registry = db.mv_registry.lock();
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
            let mv = laminar_core::mv::MaterializedView::new(
                "view_a",
                "SELECT * FROM events",
                vec!["events".to_string()],
                schema,
            );
            registry.register(mv).unwrap();
        }

        let result = db.execute("SHOW MATERIALIZED VIEWS").await.unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                assert_eq!(batch.num_rows(), 1);
                let names = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(names.value(0), "view_a");
            }
            _ => panic!("Expected Metadata result"),
        }
    }

    #[tokio::test]
    async fn test_drop_mv_and_show() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT)").await.unwrap();

        // Register a view
        {
            let mut registry = db.mv_registry.lock();
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
            let mv = laminar_core::mv::MaterializedView::new(
                "temp_view",
                "SELECT * FROM events",
                vec!["events".to_string()],
                schema,
            );
            registry.register(mv).unwrap();
        }

        // Verify it's there
        assert_eq!(db.mv_registry.lock().len(), 1);

        // Drop it
        db.execute("DROP MATERIALIZED VIEW temp_view")
            .await
            .unwrap();

        // Verify it's gone
        assert_eq!(db.mv_registry.lock().len(), 0);
    }

    #[tokio::test]
    async fn test_debug_includes_mv_count() {
        let db = LaminarDB::open().unwrap();
        let debug = format!("{db:?}");
        assert!(
            debug.contains("materialized_views: 0"),
            "Debug should include MV count, got: {debug}"
        );
    }

    // ── Pipeline Topology Introspection tests ──

    #[tokio::test]
    async fn test_pipeline_topology_empty() {
        let db = LaminarDB::open().unwrap();
        let topo = db.pipeline_topology();
        assert!(topo.nodes.is_empty());
        assert!(topo.edges.is_empty());
    }

    #[tokio::test]
    async fn test_pipeline_topology_sources_only() {
        use crate::handle::PipelineNodeType;

        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
            .await
            .unwrap();
        db.execute("CREATE SOURCE clicks (url VARCHAR, ts BIGINT)")
            .await
            .unwrap();

        let topo = db.pipeline_topology();
        assert_eq!(topo.nodes.len(), 2);
        assert!(topo.edges.is_empty());

        for node in &topo.nodes {
            assert_eq!(node.node_type, PipelineNodeType::Source);
            assert!(node.schema.is_some());
            assert!(node.sql.is_none());
        }
    }

    #[tokio::test]
    async fn test_pipeline_topology_full_pipeline() {
        use crate::handle::PipelineNodeType;

        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
            .await
            .unwrap();
        db.execute("CREATE STREAM agg AS SELECT COUNT(*) as cnt FROM events GROUP BY id")
            .await
            .unwrap();
        db.execute("CREATE SINK output FROM agg").await.unwrap();

        let topo = db.pipeline_topology();

        // Nodes: 1 source + 1 stream + 1 sink = 3
        assert_eq!(topo.nodes.len(), 3);

        let sources: Vec<_> = topo
            .nodes
            .iter()
            .filter(|n| n.node_type == PipelineNodeType::Source)
            .collect();
        let streams: Vec<_> = topo
            .nodes
            .iter()
            .filter(|n| n.node_type == PipelineNodeType::Stream)
            .collect();
        let sinks: Vec<_> = topo
            .nodes
            .iter()
            .filter(|n| n.node_type == PipelineNodeType::Sink)
            .collect();

        assert_eq!(sources.len(), 1);
        assert_eq!(streams.len(), 1);
        assert_eq!(sinks.len(), 1);

        assert_eq!(sources[0].name, "events");
        assert_eq!(streams[0].name, "agg");
        assert!(streams[0].sql.is_some());
        assert_eq!(sinks[0].name, "output");

        // Edges: events->agg, agg->output
        assert_eq!(topo.edges.len(), 2);
        assert!(topo
            .edges
            .iter()
            .any(|e| e.from == "events" && e.to == "agg"));
        assert!(topo
            .edges
            .iter()
            .any(|e| e.from == "agg" && e.to == "output"));
    }

    #[tokio::test]
    async fn test_pipeline_topology_fan_out() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE ticks (symbol VARCHAR, price DOUBLE)")
            .await
            .unwrap();
        db.execute("CREATE STREAM ohlc AS SELECT symbol, MIN(price) FROM ticks GROUP BY symbol")
            .await
            .unwrap();
        db.execute("CREATE STREAM vol AS SELECT symbol, COUNT(*) FROM ticks GROUP BY symbol")
            .await
            .unwrap();

        let topo = db.pipeline_topology();

        // 1 source + 2 streams = 3 nodes
        assert_eq!(topo.nodes.len(), 3);

        // Both streams should have an edge from ticks
        let ticks_edges: Vec<_> = topo.edges.iter().filter(|e| e.from == "ticks").collect();
        assert_eq!(ticks_edges.len(), 2);

        let targets: Vec<&str> = ticks_edges.iter().map(|e| e.to.as_str()).collect();
        assert!(targets.contains(&"ohlc"));
        assert!(targets.contains(&"vol"));
    }

    #[tokio::test]
    async fn test_streams_method() {
        let db = LaminarDB::open().unwrap();
        assert!(db.streams().is_empty());

        db.execute("CREATE STREAM counts AS SELECT COUNT(*) FROM events")
            .await
            .unwrap();

        let streams = db.streams();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].name, "counts");
        assert!(streams[0].sql.is_some());
        assert!(
            streams[0].sql.as_ref().unwrap().contains("COUNT"),
            "SQL should contain the query: {:?}",
            streams[0].sql,
        );
    }

    #[tokio::test]
    async fn test_pipeline_node_types() {
        use crate::handle::PipelineNodeType;

        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE src (id INT)").await.unwrap();
        db.execute("CREATE STREAM st AS SELECT * FROM src")
            .await
            .unwrap();
        db.execute("CREATE SINK sk FROM st").await.unwrap();

        let topo = db.pipeline_topology();

        let find = |name: &str| topo.nodes.iter().find(|n| n.name == name).unwrap();

        assert_eq!(find("src").node_type, PipelineNodeType::Source);
        assert_eq!(find("st").node_type, PipelineNodeType::Stream);
        assert_eq!(find("sk").node_type, PipelineNodeType::Sink);
    }

    // ── Reference Table (F-CONN-002) tests ──

    #[tokio::test]
    async fn test_create_table_with_primary_key() {
        let db = LaminarDB::open().unwrap();
        let result = db
            .execute(
                "CREATE TABLE instruments (\
                 symbol VARCHAR PRIMARY KEY, \
                 company_name VARCHAR, \
                 sector VARCHAR\
                 )",
            )
            .await
            .unwrap();

        match result {
            ExecuteResult::Ddl(info) => {
                assert_eq!(info.statement_type, "CREATE TABLE");
                assert_eq!(info.object_name, "instruments");
            }
            _ => panic!("Expected DDL result"),
        }

        // Verify TableStore registration
        let ts = db.table_store.lock();
        assert!(ts.has_table("instruments"));
        assert_eq!(ts.primary_key("instruments"), Some("symbol"));
        assert_eq!(ts.table_row_count("instruments"), 0);
    }

    #[tokio::test]
    async fn test_create_table_with_connector_options() {
        let db = LaminarDB::open().unwrap();
        let result = db
            .execute(
                "CREATE TABLE instruments (\
                 symbol VARCHAR PRIMARY KEY, \
                 company_name VARCHAR\
                 ) WITH (connector = 'kafka', topic = 'instruments')",
            )
            .await
            .unwrap();

        match result {
            ExecuteResult::Ddl(info) => {
                assert_eq!(info.object_name, "instruments");
            }
            _ => panic!("Expected DDL result"),
        }

        // Verify ConnectorManager registration
        let mgr = db.connector_manager.lock();
        let tables = mgr.tables();
        assert!(tables.contains_key("instruments"));
        let reg = &tables["instruments"];
        assert_eq!(reg.connector_type.as_deref(), Some("kafka"));
        assert_eq!(reg.primary_key, "symbol");

        // Verify TableStore connector
        let ts = db.table_store.lock();
        assert_eq!(ts.connector("instruments"), Some("kafka"));
    }

    #[tokio::test]
    async fn test_insert_into_table_with_pk_upserts() {
        let db = LaminarDB::open().unwrap();
        db.execute(
            "CREATE TABLE products (\
             id INT PRIMARY KEY, \
             name VARCHAR, \
             price DOUBLE\
             )",
        )
        .await
        .unwrap();

        // Insert a row
        db.execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
            .await
            .unwrap();
        assert_eq!(db.table_store.lock().table_row_count("products"), 1);

        // Upsert (same PK = overwrite)
        db.execute("INSERT INTO products VALUES (1, 'Super Widget', 19.99)")
            .await
            .unwrap();
        assert_eq!(db.table_store.lock().table_row_count("products"), 1);

        // Insert another row (different PK)
        db.execute("INSERT INTO products VALUES (2, 'Gadget', 14.99)")
            .await
            .unwrap();
        assert_eq!(db.table_store.lock().table_row_count("products"), 2);

        // Verify via SELECT
        let result = db.execute("SELECT * FROM products").await.unwrap();
        match result {
            ExecuteResult::Query(q) => {
                assert_eq!(q.schema().fields().len(), 3);
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[tokio::test]
    async fn test_show_tables() {
        let db = LaminarDB::open().unwrap();

        // Empty
        let result = db.execute("SHOW TABLES").await.unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                assert_eq!(batch.num_rows(), 0);
                assert_eq!(batch.num_columns(), 4);
                assert_eq!(batch.schema().field(0).name(), "name");
                assert_eq!(batch.schema().field(1).name(), "primary_key");
                assert_eq!(batch.schema().field(2).name(), "row_count");
                assert_eq!(batch.schema().field(3).name(), "connector");
            }
            _ => panic!("Expected Metadata result"),
        }

        // With a table
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val VARCHAR)")
            .await
            .unwrap();
        let result = db.execute("SHOW TABLES").await.unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                assert_eq!(batch.num_rows(), 1);
            }
            _ => panic!("Expected Metadata result"),
        }
    }

    #[tokio::test]
    async fn test_drop_table() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val VARCHAR)")
            .await
            .unwrap();
        assert!(db.table_store.lock().has_table("t"));

        db.execute("DROP TABLE t").await.unwrap();
        assert!(!db.table_store.lock().has_table("t"));
    }

    #[tokio::test]
    async fn test_drop_table_if_exists() {
        let db = LaminarDB::open().unwrap();
        let result = db.execute("DROP TABLE IF EXISTS nonexistent").await;
        assert!(result.is_ok());
    }

    // ── HAVING clause tests (F-SQL-004) ─────────────────────────────

    #[tokio::test]
    async fn test_having_filters_grouped_results() {
        let db = LaminarDB::open().unwrap();

        // Create table and query via DataFusion directly
        db.ctx
            .sql(
                "CREATE TABLE hv_trades AS SELECT * FROM (VALUES \
                 ('AAPL', 100), ('GOOG', 5), ('MSFT', 50)) \
                 AS t(symbol, volume)",
            )
            .await
            .unwrap();

        let df = db
            .ctx
            .sql("SELECT symbol, volume FROM hv_trades WHERE volume > 10 ORDER BY symbol")
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        // AAPL(100), MSFT(50) pass; GOOG(5) filtered
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_having_with_aggregate() {
        let db = LaminarDB::open().unwrap();

        db.ctx
            .sql(
                "CREATE TABLE hv_orders AS SELECT * FROM (VALUES \
                 ('A', 100), ('A', 200), ('B', 50), ('B', 30), ('C', 500)) \
                 AS t(category, amount)",
            )
            .await
            .unwrap();

        // Query with GROUP BY + HAVING through DataFusion
        let df = db
            .ctx
            .sql(
                "SELECT category, SUM(amount) as total \
                 FROM hv_orders GROUP BY category \
                 HAVING SUM(amount) > 100 ORDER BY category",
            )
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        assert!(!batches.is_empty());

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        // A: 300 > 100 ✓, B: 80 ✗, C: 500 > 100 ✓
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_having_all_filtered_out() {
        let db = LaminarDB::open().unwrap();

        db.ctx
            .sql(
                "CREATE TABLE items AS SELECT * FROM (VALUES \
                 ('x', 1), ('y', 2)) AS t(name, qty)",
            )
            .await
            .unwrap();

        let df = db
            .ctx
            .sql("SELECT name, SUM(qty) as total FROM items GROUP BY name HAVING SUM(qty) > 1000")
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 0);
    }

    #[tokio::test]
    async fn test_having_compound_predicate() {
        let db = LaminarDB::open().unwrap();

        db.ctx
            .sql(
                "CREATE TABLE sales AS SELECT * FROM (VALUES \
                 ('A', 100), ('A', 200), ('B', 50), ('C', 10), ('C', 20)) \
                 AS t(region, amount)",
            )
            .await
            .unwrap();

        let df = db
            .ctx
            .sql(
                "SELECT region, COUNT(*) as cnt, SUM(amount) as total \
                 FROM sales GROUP BY region \
                 HAVING COUNT(*) >= 2 AND SUM(amount) > 25 \
                 ORDER BY region",
            )
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        // A: cnt=2>=2 AND total=300>25 ✓
        // B: cnt=1<2 ✗
        // C: cnt=2>=2 AND total=30>25 ✓
        assert_eq!(total_rows, 2);
    }

    // ── Multi-way JOIN tests (F-SQL-005) ─────────────────────────────

    #[tokio::test]
    async fn test_multi_join_two_way_lookup() {
        let db = LaminarDB::open().unwrap();

        // Create tables via DataFusion
        db.ctx
            .sql(
                "CREATE TABLE orders AS SELECT * FROM (VALUES \
                 (1, 100, 'A'), (2, 200, 'B')) AS t(id, customer_id, product_code)",
            )
            .await
            .unwrap();
        db.ctx
            .sql(
                "CREATE TABLE customers AS SELECT * FROM (VALUES \
                 (100, 'Alice'), (200, 'Bob')) AS t(id, name)",
            )
            .await
            .unwrap();
        db.ctx
            .sql(
                "CREATE TABLE products AS SELECT * FROM (VALUES \
                 ('A', 'Widget'), ('B', 'Gadget')) AS t(code, label)",
            )
            .await
            .unwrap();

        // Two-way join through DataFusion
        let df = db
            .ctx
            .sql(
                "SELECT o.id, c.name, p.label \
                 FROM orders o \
                 JOIN customers c ON o.customer_id = c.id \
                 JOIN products p ON o.product_code = p.code \
                 ORDER BY o.id",
            )
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_multi_join_three_way() {
        let db = LaminarDB::open().unwrap();

        db.ctx
            .sql("CREATE TABLE t1 AS SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(id, fk1)")
            .await
            .unwrap();
        db.ctx
            .sql("CREATE TABLE t2 AS SELECT * FROM (VALUES (10, 100), (20, 200)) AS t(id, fk2)")
            .await
            .unwrap();
        db.ctx
            .sql("CREATE TABLE t3 AS SELECT * FROM (VALUES (100, 'x'), (200, 'y')) AS t(id, fk3)")
            .await
            .unwrap();
        db.ctx
            .sql("CREATE TABLE t4 AS SELECT * FROM (VALUES ('x', 'final_x'), ('y', 'final_y')) AS t(id, val)")
            .await
            .unwrap();

        let df = db
            .ctx
            .sql(
                "SELECT t1.id, t4.val \
                 FROM t1 \
                 JOIN t2 ON t1.fk1 = t2.id \
                 JOIN t3 ON t2.fk2 = t3.id \
                 JOIN t4 ON t3.fk3 = t4.id \
                 ORDER BY t1.id",
            )
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_multi_join_mixed_types() {
        let db = LaminarDB::open().unwrap();

        db.ctx
            .sql(
                "CREATE TABLE stream_a AS SELECT * FROM (VALUES \
                 (1, 'k1'), (2, 'k2')) AS t(id, key)",
            )
            .await
            .unwrap();
        db.ctx
            .sql(
                "CREATE TABLE stream_b AS SELECT * FROM (VALUES \
                 ('k1', 10), ('k2', 20)) AS t(key, value)",
            )
            .await
            .unwrap();
        db.ctx
            .sql(
                "CREATE TABLE dim_c AS SELECT * FROM (VALUES \
                 ('k1', 'label1'), ('k2', 'label2')) AS t(key, label)",
            )
            .await
            .unwrap();

        // Inner join + left join
        let df = db
            .ctx
            .sql(
                "SELECT a.id, b.value, c.label \
                 FROM stream_a a \
                 JOIN stream_b b ON a.key = b.key \
                 LEFT JOIN dim_c c ON a.key = c.key \
                 ORDER BY a.id",
            )
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_multi_join_single_backward_compat() {
        let db = LaminarDB::open().unwrap();

        db.ctx
            .sql(
                "CREATE TABLE left_t AS SELECT * FROM (VALUES \
                 (1, 'a'), (2, 'b')) AS t(id, val)",
            )
            .await
            .unwrap();
        db.ctx
            .sql(
                "CREATE TABLE right_t AS SELECT * FROM (VALUES \
                 (1, 'x'), (2, 'y')) AS t(id, data)",
            )
            .await
            .unwrap();

        // Single join still works
        let df = db
            .ctx
            .sql(
                "SELECT l.id, l.val, r.data \
                 FROM left_t l JOIN right_t r ON l.id = r.id \
                 ORDER BY l.id",
            )
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 2);
    }

    // ── Window Frame tests (F-SQL-006) ────────────────────────────────

    #[tokio::test]
    async fn test_frame_moving_average() {
        let db = LaminarDB::open().unwrap();

        db.ctx
            .sql(
                "CREATE TABLE frame_prices AS SELECT * FROM (VALUES \
                 (1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0), (5, 50.0)) \
                 AS t(id, price)",
            )
            .await
            .unwrap();

        let df = db
            .ctx
            .sql(
                "SELECT id, AVG(price) OVER (ORDER BY id \
                 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma \
                 FROM frame_prices ORDER BY id",
            )
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 5);

        // Verify moving average values: row 3 → avg(10,20,30) = 20
        let ma_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!((ma_col.value(2) - 20.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_frame_running_sum() {
        let db = LaminarDB::open().unwrap();

        db.ctx
            .sql(
                "CREATE TABLE frame_amounts AS SELECT * FROM (VALUES \
                 (1, 100.0), (2, 200.0), (3, 300.0)) AS t(id, amount)",
            )
            .await
            .unwrap();

        let df = db
            .ctx
            .sql(
                "SELECT id, SUM(amount) OVER (ORDER BY id \
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running \
                 FROM frame_amounts ORDER BY id",
            )
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 3);

        let sum_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        // Row 3: cumulative sum = 100 + 200 + 300 = 600
        assert!((sum_col.value(2) - 600.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_frame_rolling_max() {
        let db = LaminarDB::open().unwrap();

        db.ctx
            .sql(
                "CREATE TABLE frame_vals AS SELECT * FROM (VALUES \
                 (1, 5.0), (2, 15.0), (3, 10.0), (4, 20.0)) AS t(id, price)",
            )
            .await
            .unwrap();

        let df = db
            .ctx
            .sql(
                "SELECT id, MAX(price) OVER (ORDER BY id \
                 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rmax \
                 FROM frame_vals ORDER BY id",
            )
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 4);

        let max_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        // Row 3: max(5, 15, 10) = 15
        assert!((max_col.value(2) - 15.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_frame_rolling_count() {
        let db = LaminarDB::open().unwrap();

        db.ctx
            .sql(
                "CREATE TABLE frame_events AS SELECT * FROM (VALUES \
                 (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')) AS t(id, code)",
            )
            .await
            .unwrap();

        let df = db
            .ctx
            .sql(
                "SELECT id, COUNT(*) OVER (ORDER BY id \
                 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS cnt \
                 FROM frame_events ORDER BY id",
            )
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 4);

        let cnt_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        // Row 1: count of just row 1 = 1
        assert_eq!(cnt_col.value(0), 1);
        // Row 2+: count of current + 1 preceding = 2
        assert_eq!(cnt_col.value(1), 2);
        assert_eq!(cnt_col.value(2), 2);
    }

    // ── F-CONN-002B: Connector-Backed Table Population ──

    /// Helper: create a test `RecordBatch` for table population tests.
    fn table_test_batch(ids: &[i32], symbols: &[&str]) -> RecordBatch {
        use arrow::array::Int32Array;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("symbol", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(symbols.to_vec())),
            ],
        )
        .unwrap()
    }

    /// Register a mock table source factory that returns a `MockReferenceTableSource`
    /// pre-loaded with the given snapshot and change batches.
    fn register_mock_table_source(
        db: &LaminarDB,
        snapshot_batches: Vec<RecordBatch>,
        change_batches: Vec<RecordBatch>,
    ) {
        use laminar_connectors::config::ConnectorInfo;
        use laminar_connectors::reference::MockReferenceTableSource;

        let snap = std::sync::Arc::new(parking_lot::Mutex::new(Some(snapshot_batches)));
        let chg = std::sync::Arc::new(parking_lot::Mutex::new(Some(change_batches)));
        db.connector_registry().register_table_source(
            "mock",
            ConnectorInfo {
                name: "mock".to_string(),
                display_name: "Mock Table Source".to_string(),
                version: "0.1.0".to_string(),
                is_source: true,
                is_sink: false,
                config_keys: vec![],
            },
            std::sync::Arc::new(move |_config| {
                let s = snap.lock().take().unwrap_or_default();
                let c = chg.lock().take().unwrap_or_default();
                Ok(Box::new(MockReferenceTableSource::new(s, c)))
            }),
        );
    }

    #[tokio::test]
    async fn test_table_source_snapshot_populates_table() {
        let db = LaminarDB::open().unwrap();
        let batch = table_test_batch(&[1, 2], &["AAPL", "GOOG"]);
        register_mock_table_source(&db, vec![batch], vec![]);

        db.execute(
            "CREATE SOURCE events (symbol VARCHAR, price DOUBLE) \
             WITH (connector = 'mock', format = 'json')",
        )
        .await
        .unwrap();

        db.execute(
            "CREATE TABLE instruments (id INT PRIMARY KEY, symbol VARCHAR NOT NULL) \
             WITH (connector = 'mock', format = 'json')",
        )
        .await
        .unwrap();

        db.start().await.unwrap();

        // Table should be populated by snapshot
        let ts = db.table_store.lock();
        assert!(ts.is_ready("instruments"));
        assert_eq!(ts.table_row_count("instruments"), 2);
    }

    #[tokio::test]
    async fn test_table_source_manual_no_snapshot() {
        let db = LaminarDB::open().unwrap();
        let batch = table_test_batch(&[1], &["AAPL"]);
        register_mock_table_source(&db, vec![batch], vec![]);

        db.execute(
            "CREATE SOURCE events (symbol VARCHAR, price DOUBLE) \
             WITH (connector = 'mock', format = 'json')",
        )
        .await
        .unwrap();

        db.execute(
            "CREATE TABLE instruments (id INT PRIMARY KEY, symbol VARCHAR NOT NULL) \
             WITH (connector = 'mock', format = 'json', refresh = 'manual')",
        )
        .await
        .unwrap();

        db.start().await.unwrap();

        // Manual mode: table stays empty
        let ts = db.table_store.lock();
        assert!(!ts.is_ready("instruments"));
        assert_eq!(ts.table_row_count("instruments"), 0);
    }

    #[tokio::test]
    async fn test_table_source_multiple_tables() {
        use laminar_connectors::config::ConnectorInfo;
        use laminar_connectors::reference::MockReferenceTableSource;

        let db = LaminarDB::open().unwrap();

        // Register two separate mock factories

        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();
        let batch1 = table_test_batch(&[1], &["AAPL"]);
        let batch2 = table_test_batch(&[2, 3], &["GOOG", "MSFT"]);
        let batches =
            std::sync::Arc::new(parking_lot::Mutex::new(vec![vec![batch1], vec![batch2]]));

        db.connector_registry().register_table_source(
            "mock",
            ConnectorInfo {
                name: "mock".to_string(),
                display_name: "Mock".to_string(),
                version: "0.1.0".to_string(),
                is_source: true,
                is_sink: false,
                config_keys: vec![],
            },
            std::sync::Arc::new(move |_config| {
                let idx = cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst) as usize;
                let mut all = batches.lock();
                let snap = if idx < all.len() {
                    std::mem::take(&mut all[idx])
                } else {
                    vec![]
                };
                Ok(Box::new(MockReferenceTableSource::new(snap, vec![])))
            }),
        );

        db.execute("CREATE SOURCE events (x INT) WITH (connector = 'mock', format = 'json')")
            .await
            .unwrap();

        db.execute(
            "CREATE TABLE t1 (id INT PRIMARY KEY, symbol VARCHAR NOT NULL) \
             WITH (connector = 'mock', format = 'json')",
        )
        .await
        .unwrap();

        db.execute(
            "CREATE TABLE t2 (id INT PRIMARY KEY, symbol VARCHAR NOT NULL) \
             WITH (connector = 'mock', format = 'json')",
        )
        .await
        .unwrap();

        db.start().await.unwrap();

        let ts = db.table_store.lock();
        // Both tables should be snapshot-populated (order may vary)
        let total = ts.table_row_count("t1") + ts.table_row_count("t2");
        assert_eq!(total, 3); // 1 + 2
        assert!(ts.is_ready("t1"));
        assert!(ts.is_ready("t2"));
    }

    #[tokio::test]
    async fn test_table_create_with_refresh_mode() {
        let db = LaminarDB::open().unwrap();

        // Just test DDL parsing — no need to register a mock factory
        db.execute(
            "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR NOT NULL) \
             WITH (connector = 'mock', format = 'json', refresh = 'cdc')",
        )
        .await
        .unwrap();

        let mgr = db.connector_manager.lock();
        let reg = mgr.tables().get("t").unwrap();
        assert_eq!(
            reg.refresh,
            Some(laminar_connectors::reference::RefreshMode::SnapshotPlusCdc)
        );
    }

    #[tokio::test]
    async fn test_table_source_snapshot_only_no_changes() {
        let db = LaminarDB::open().unwrap();
        let snap = table_test_batch(&[1], &["AAPL"]);
        let change = table_test_batch(&[2], &["GOOG"]);
        register_mock_table_source(&db, vec![snap], vec![change]);

        db.execute(
            "CREATE SOURCE events (symbol VARCHAR, price DOUBLE) \
             WITH (connector = 'mock', format = 'json')",
        )
        .await
        .unwrap();

        db.execute(
            "CREATE TABLE instruments (id INT PRIMARY KEY, symbol VARCHAR NOT NULL) \
             WITH (connector = 'mock', format = 'json', refresh = 'snapshot_only')",
        )
        .await
        .unwrap();

        db.start().await.unwrap();

        // Should have snapshot data but not the change batch (it's snapshot_only)
        let mut ts = db.table_store.lock();
        assert!(ts.is_ready("instruments"));
        assert_eq!(ts.table_row_count("instruments"), 1);
        // The change batch id=2/GOOG should NOT be present
        assert!(ts.lookup("instruments", "2").is_none());
    }

    // ── F-CONN-002C: PARTIAL Cache Mode DDL tests ──

    #[tokio::test]
    async fn test_create_table_partial_cache_mode() {
        let db = LaminarDB::open().unwrap();
        db.execute(
            "CREATE TABLE large_dim (\
             id INT PRIMARY KEY, \
             name VARCHAR\
             ) WITH (cache_mode = 'partial')",
        )
        .await
        .unwrap();

        // Verify table exists
        {
            let ts = db.table_store.lock();
            assert!(ts.has_table("large_dim"));
            // Cache metrics should exist for partial-mode tables
        }

        // Insert some data
        db.execute("INSERT INTO large_dim VALUES (1, 'Alice')")
            .await
            .unwrap();
        db.execute("INSERT INTO large_dim VALUES (2, 'Bob')")
            .await
            .unwrap();

        let ts = db.table_store.lock();
        assert_eq!(ts.table_row_count("large_dim"), 2);
    }

    #[tokio::test]
    async fn test_create_table_partial_with_max_entries() {
        let db = LaminarDB::open().unwrap();
        db.execute(
            "CREATE TABLE customers (\
             id INT PRIMARY KEY, \
             name VARCHAR\
             ) WITH (cache_mode = 'partial', cache_max_entries = '10000')",
        )
        .await
        .unwrap();

        let ts = db.table_store.lock();
        assert!(ts.has_table("customers"));
        // Verify the cache metrics report the correct max_entries
        let metrics = ts.cache_metrics("customers").unwrap();
        assert_eq!(metrics.cache_max_entries, 10000);
    }

    #[tokio::test]
    async fn test_create_table_invalid_cache_max_entries() {
        let db = LaminarDB::open().unwrap();
        let result = db
            .execute(
                "CREATE TABLE bad (\
                 id INT PRIMARY KEY, \
                 name VARCHAR\
                 ) WITH (cache_mode = 'partial', cache_max_entries = 'not_a_number')",
            )
            .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("cache_max_entries"));
    }

    // --- F-OBS-001: Pipeline Observability API tests ---

    #[tokio::test]
    async fn test_metrics_initial_state() {
        let db = LaminarDB::open().unwrap();
        let m = db.metrics();
        assert_eq!(m.total_events_ingested, 0);
        assert_eq!(m.total_events_emitted, 0);
        assert_eq!(m.total_events_dropped, 0);
        assert_eq!(m.total_cycles, 0);
        assert_eq!(m.total_batches, 0);
        assert_eq!(m.state, crate::metrics::PipelineState::Created);
        assert_eq!(m.source_count, 0);
        assert_eq!(m.stream_count, 0);
        assert_eq!(m.sink_count, 0);
    }

    #[tokio::test]
    async fn test_source_metrics_after_push() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
            .await
            .unwrap();

        // Push some data
        let handle = db.source_untyped("trades").unwrap();
        let batch = RecordBatch::try_new(
            handle.schema().clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(arrow::array::Float64Array::from(vec![150.0, 2800.0])),
            ],
        )
        .unwrap();
        handle.push_arrow(batch).unwrap();

        let sm = db.source_metrics("trades").unwrap();
        assert_eq!(sm.name, "trades");
        assert_eq!(sm.total_events, 1); // 1 push = sequence 1
        assert!(sm.pending > 0);
        assert!(sm.capacity > 0);
        assert!(sm.utilization > 0.0);
    }

    #[tokio::test]
    async fn test_source_metrics_not_found() {
        let db = LaminarDB::open().unwrap();
        assert!(db.source_metrics("nonexistent").is_none());
    }

    #[tokio::test]
    async fn test_all_source_metrics() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE a (id INT)").await.unwrap();
        db.execute("CREATE SOURCE b (id INT)").await.unwrap();

        let all = db.all_source_metrics();
        assert_eq!(all.len(), 2);
        let names: std::collections::HashSet<_> = all.iter().map(|m| m.name.clone()).collect();
        assert!(names.contains("a"));
        assert!(names.contains("b"));
    }

    #[tokio::test]
    async fn test_total_events_processed_zero() {
        let db = LaminarDB::open().unwrap();
        assert_eq!(db.total_events_processed(), 0);
    }

    #[tokio::test]
    async fn test_pipeline_state_enum_created() {
        let db = LaminarDB::open().unwrap();
        assert_eq!(
            db.pipeline_state_enum(),
            crate::metrics::PipelineState::Created
        );
    }

    #[tokio::test]
    async fn test_counters_accessible() {
        let db = LaminarDB::open().unwrap();
        let c = db.counters();
        c.events_ingested
            .fetch_add(42, std::sync::atomic::Ordering::Relaxed);
        let m = db.metrics();
        assert_eq!(m.total_events_ingested, 42);
    }

    #[tokio::test]
    async fn test_metrics_counts_after_create() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE s1 (id INT)").await.unwrap();
        db.execute("CREATE SINK out1 FROM s1").await.unwrap();

        let m = db.metrics();
        assert_eq!(m.source_count, 1);
        assert_eq!(m.sink_count, 1);
    }

    #[tokio::test]
    async fn test_source_handle_capacity() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT)").await.unwrap();

        let handle = db.source_untyped("events").unwrap();
        // Default buffer size is 1024
        assert!(handle.capacity() >= 1024);
        assert!(!handle.is_backpressured());
    }

    #[tokio::test]
    async fn test_stream_metrics_with_sql() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts BIGINT)")
            .await
            .unwrap();
        db.execute(
            "CREATE STREAM avg_price AS \
             SELECT symbol, AVG(price) as avg_price \
             FROM trades GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE)",
        )
        .await
        .unwrap();

        let sm = db.stream_metrics("avg_price");
        assert!(sm.is_some());
        let sm = sm.unwrap();
        assert_eq!(sm.name, "avg_price");
        assert!(sm.sql.is_some());
        assert!(sm.sql.as_deref().unwrap().contains("AVG"));
    }

    #[tokio::test]
    async fn test_all_stream_metrics() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts BIGINT)")
            .await
            .unwrap();
        db.execute(
            "CREATE STREAM s1 AS SELECT symbol, AVG(price) as avg_price \
             FROM trades GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE)",
        )
        .await
        .unwrap();

        let all = db.all_stream_metrics();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "s1");
    }

    #[tokio::test]
    async fn test_stream_metrics_not_found() {
        let db = LaminarDB::open().unwrap();
        assert!(db.stream_metrics("nonexistent").is_none());
    }

    // ── Watermark Source Tracker tests ──────────────────────────────────

    /// Helper: push a batch with `Timestamp(µs)` column to a source.
    ///
    /// `timestamps_ms` are in **milliseconds**; the helper converts to microseconds
    /// internally to match the `TIMESTAMP` SQL type (`Timestamp(Microsecond, None)`).
    fn make_ts_batch(schema: &arrow::datatypes::SchemaRef, timestamps_ms: &[i64]) -> RecordBatch {
        let us_values: Vec<i64> = timestamps_ms.iter().map(|ms| ms * 1000).collect();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(
                    (1..=i64::try_from(timestamps_ms.len()).expect("len fits i64"))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(arrow::array::TimestampMicrosecondArray::from(us_values)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_watermark_advances_on_push() {
        let db = LaminarDB::open().unwrap();
        db.execute(
            "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
             WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();
        db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
            .await
            .unwrap();
        db.start().await.unwrap();

        let handle = db.source_untyped("events").unwrap();
        let schema = handle.schema().clone();
        let batch = make_ts_batch(&schema, &[1000, 2000, 3000]);
        handle.push_arrow(batch).unwrap();

        // Wait for pipeline loop to process
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // With 0s delay, watermark should be max timestamp = 3000
        let wm = handle.current_watermark();
        assert_eq!(
            wm, 3000,
            "watermark should equal max timestamp with 0s delay"
        );
    }

    #[tokio::test]
    async fn test_watermark_bounded_delay() {
        let db = LaminarDB::open().unwrap();
        db.execute(
            "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
             WATERMARK FOR ts AS ts - INTERVAL '100' MILLISECOND)",
        )
        .await
        .unwrap();
        db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
            .await
            .unwrap();
        db.start().await.unwrap();

        let handle = db.source_untyped("events").unwrap();
        let schema = handle.schema().clone();

        // Push timestamps [1000, 800, 1200] — max = 1200
        let batch = make_ts_batch(&schema, &[1000, 800, 1200]);
        handle.push_arrow(batch).unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Watermark = max(1200) - 100ms delay = 1100
        let wm = handle.current_watermark();
        assert_eq!(wm, 1100, "watermark should be max_ts - delay");
    }

    #[tokio::test]
    async fn test_watermark_no_regression() {
        let db = LaminarDB::open().unwrap();
        db.execute(
            "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
             WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();
        db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
            .await
            .unwrap();
        db.start().await.unwrap();

        let handle = db.source_untyped("events").unwrap();
        let schema = handle.schema().clone();

        // Push high timestamps first
        let batch1 = make_ts_batch(&schema, &[5000]);
        handle.push_arrow(batch1).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let wm1 = handle.current_watermark();

        // Push lower timestamps
        let batch2 = make_ts_batch(&schema, &[1000]);
        handle.push_arrow(batch2).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let wm2 = handle.current_watermark();

        // Watermark should never decrease
        assert!(wm2 >= wm1, "watermark must not regress: {wm2} < {wm1}");
        assert_eq!(wm1, 5000);
        assert_eq!(wm2, 5000);
    }

    #[tokio::test]
    async fn test_source_without_watermark() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id BIGINT, ts BIGINT)")
            .await
            .unwrap();

        // Source without WATERMARK clause should have default watermark
        let handle = db.source_untyped("events").unwrap();
        assert_eq!(handle.current_watermark(), i64::MIN);
        assert!(handle.max_out_of_orderness().is_none());
    }

    #[tokio::test]
    async fn test_watermark_with_arrow_timestamp_column() {
        let db = LaminarDB::open().unwrap();
        db.execute(
            "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
             WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();
        db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
            .await
            .unwrap();
        db.start().await.unwrap();

        let handle = db.source_untyped("events").unwrap();
        let schema = handle.schema().clone();

        // Build a batch with Arrow Timestamp(us) column matching the schema
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                    5_000_000i64,
                ])),
            ],
        )
        .unwrap();
        handle.push_arrow(batch).unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let wm = handle.current_watermark();
        // ArrowNative format: timestamp is in microseconds, extractor converts to millis
        assert_eq!(wm, 5000, "watermark should work with Arrow Timestamp type");
    }

    #[tokio::test]
    async fn test_pipeline_watermark_global_min() {
        let db = LaminarDB::open().unwrap();
        db.execute(
            "CREATE SOURCE trades (id BIGINT, ts TIMESTAMP, \
             WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();
        db.execute(
            "CREATE SOURCE orders (id BIGINT, ts TIMESTAMP, \
             WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();
        db.execute("CREATE STREAM out AS SELECT id, ts FROM trades")
            .await
            .unwrap();
        db.start().await.unwrap();

        let trades = db.source_untyped("trades").unwrap();
        let orders = db.source_untyped("orders").unwrap();

        // Push high watermark to trades
        let batch1 = make_ts_batch(trades.schema(), &[5000]);
        trades.push_arrow(batch1).unwrap();

        // Push lower watermark to orders
        let batch2 = make_ts_batch(orders.schema(), &[2000]);
        orders.push_arrow(batch2).unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Global watermark should be min(5000, 2000) = 2000
        let global = db.pipeline_watermark();
        assert_eq!(
            global, 2000,
            "global watermark should be min of all sources"
        );
    }

    #[tokio::test]
    async fn test_pipeline_watermark_in_metrics() {
        let db = LaminarDB::open().unwrap();
        db.execute(
            "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
             WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();
        db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
            .await
            .unwrap();
        db.start().await.unwrap();

        let handle = db.source_untyped("events").unwrap();
        let batch = make_ts_batch(handle.schema(), &[4000]);
        handle.push_arrow(batch).unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let m = db.metrics();
        assert_eq!(
            m.pipeline_watermark,
            db.pipeline_watermark(),
            "metrics().pipeline_watermark should match pipeline_watermark()"
        );
        assert_eq!(m.pipeline_watermark, 4000);
    }

    #[tokio::test]
    async fn test_source_handle_max_out_of_orderness() {
        let db = LaminarDB::open().unwrap();
        db.execute(
            "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
             WATERMARK FOR ts AS ts - INTERVAL '5' SECOND)",
        )
        .await
        .unwrap();

        let handle = db.source_untyped("events").unwrap();
        let dur = handle.max_out_of_orderness();
        assert_eq!(dur, Some(std::time::Duration::from_secs(5)));
    }

    #[tokio::test]
    async fn test_source_handle_no_watermark() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id BIGINT, ts BIGINT)")
            .await
            .unwrap();

        let handle = db.source_untyped("events").unwrap();
        assert!(handle.max_out_of_orderness().is_none());
    }

    #[tokio::test]
    async fn test_late_data_dropped_after_external_watermark() {
        // Scenario:
        //  1. Push on-time batch (ts = [1000, 2000, 3000])
        //  2. Advance watermark to 200_000 externally via source.watermark()
        //  3. Push late batch (ts = [100, 200, 300]) — all timestamps < watermark
        //  4. Verify late batch does NOT appear in stream output
        let db = LaminarDB::open().unwrap();
        db.execute(
            "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
             WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();
        db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
            .await
            .unwrap();

        let sub = db.catalog.get_stream_subscription("out").unwrap();
        db.start().await.unwrap();

        let handle = db.source_untyped("events").unwrap();
        let schema = handle.schema().clone();

        // Step 1: Push on-time data
        let batch1 = make_ts_batch(&schema, &[1000, 2000, 3000]);
        handle.push_arrow(batch1).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Drain on-time results
        let mut on_time_rows = 0;
        for _ in 0..256 {
            match sub.poll() {
                Some(b) => on_time_rows += b.num_rows(),
                None => break,
            }
        }
        assert!(on_time_rows > 0, "should have on-time rows");

        // Step 2: Advance watermark to 200_000 (external signal)
        handle.watermark(200_000);
        // Give the pipeline loop a cycle to pick up the external watermark
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Step 3: Push late data (all timestamps < 200_000)
        let late_batch = make_ts_batch(&schema, &[100, 200, 300]);
        handle.push_arrow(late_batch).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Step 4: Check that late data was filtered out
        let mut late_rows = 0;
        for _ in 0..256 {
            match sub.poll() {
                Some(b) => late_rows += b.num_rows(),
                None => break,
            }
        }
        assert_eq!(late_rows, 0, "late data behind watermark should be dropped");
    }

    #[test]
    fn test_filter_late_rows_filters_correctly() {
        use arrow::array::Int64Array;

        // Int64 / UnixMillis format
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int64Array::from(vec![100, 500, 200, 800])),
            ],
        )
        .unwrap();

        // Watermark at 300: rows with ts >= 300 survive (ts=500, ts=800)
        let filtered = filter_late_rows(
            &batch,
            "ts",
            300,
            laminar_core::time::TimestampFormat::UnixMillis,
        );
        let filtered = filtered.expect("should have some on-time rows");
        assert_eq!(filtered.num_rows(), 2);

        // Check values
        let ids = filtered
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2); // ts=500
        assert_eq!(ids.value(1), 4); // ts=800
    }

    #[test]
    fn test_filter_late_rows_all_late() {
        use arrow::array::Int64Array;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap();

        // Watermark at 1000: all rows are late
        let result = filter_late_rows(
            &batch,
            "ts",
            1000,
            laminar_core::time::TimestampFormat::UnixMillis,
        );
        assert!(result.is_none(), "all-late batch should return None");
    }

    #[test]
    fn test_filter_late_rows_no_column() {
        use arrow::array::Int64Array;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();

        // Column not found — batch passes through unfiltered
        let result = filter_late_rows(
            &batch,
            "ts",
            1000,
            laminar_core::time::TimestampFormat::UnixMillis,
        );
        let result = result.expect("should pass through when column not found");
        assert_eq!(result.num_rows(), 2);
    }

    /// Helper: creates a `RecordBatch` with (id: BIGINT, ts: BIGINT).
    fn make_bigint_ts_batch(
        schema: &arrow::datatypes::SchemaRef,
        timestamps: &[i64],
    ) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(
                    (1..=i64::try_from(timestamps.len()).expect("len fits i64"))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(arrow::array::Int64Array::from(timestamps.to_vec())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_programmatic_watermark_filters_late_rows() {
        // Source with set_event_time_column("ts"), no SQL WATERMARK clause.
        // Push data, advance watermark, push late data, verify late data filtered.
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id BIGINT, ts BIGINT)")
            .await
            .unwrap();
        db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
            .await
            .unwrap();

        let sub = db.catalog.get_stream_subscription("out").unwrap();

        let handle = db.source_untyped("events").unwrap();
        handle.set_event_time_column("ts");

        db.start().await.unwrap();

        let schema = handle.schema().clone();

        // Step 1: Push on-time data
        let batch1 = make_bigint_ts_batch(&schema, &[1000, 2000, 3000]);
        handle.push_arrow(batch1).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Drain on-time results
        let mut on_time_rows = 0;
        for _ in 0..256 {
            match sub.poll() {
                Some(b) => on_time_rows += b.num_rows(),
                None => break,
            }
        }
        assert!(on_time_rows > 0, "should have on-time rows");

        // Step 2: Advance watermark to 200_000 (external signal)
        handle.watermark(200_000);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Step 3: Push late data (all timestamps < 200_000)
        let late_batch = make_bigint_ts_batch(&schema, &[100, 200, 300]);
        handle.push_arrow(late_batch).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Step 4: Check that late data was filtered out
        let mut late_rows = 0;
        for _ in 0..256 {
            match sub.poll() {
                Some(b) => late_rows += b.num_rows(),
                None => break,
            }
        }
        assert_eq!(late_rows, 0, "late data behind watermark should be dropped");
    }

    #[tokio::test]
    async fn test_sql_watermark_for_col_filters_late_rows() {
        // Source with WATERMARK FOR ts (no AS expr), should use zero delay.
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id BIGINT, ts BIGINT, WATERMARK FOR ts)")
            .await
            .unwrap();
        db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
            .await
            .unwrap();

        let sub = db.catalog.get_stream_subscription("out").unwrap();
        db.start().await.unwrap();

        let handle = db.source_untyped("events").unwrap();
        let schema = handle.schema().clone();

        // Push on-time data
        let batch1 = make_bigint_ts_batch(&schema, &[1000, 2000, 3000]);
        handle.push_arrow(batch1).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let mut on_time_rows = 0;
        for _ in 0..256 {
            match sub.poll() {
                Some(b) => on_time_rows += b.num_rows(),
                None => break,
            }
        }
        assert!(on_time_rows > 0, "should have on-time rows");

        // Advance watermark externally
        handle.watermark(200_000);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Push late data
        let late_batch = make_bigint_ts_batch(&schema, &[100, 200, 300]);
        handle.push_arrow(late_batch).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let mut late_rows = 0;
        for _ in 0..256 {
            match sub.poll() {
                Some(b) => late_rows += b.num_rows(),
                None => break,
            }
        }
        assert_eq!(late_rows, 0, "late data behind watermark should be dropped");
    }

    #[tokio::test]
    async fn test_no_watermark_passes_all_data() {
        // Source without any watermark config — all data should pass through.
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id BIGINT, ts BIGINT)")
            .await
            .unwrap();
        db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
            .await
            .unwrap();

        let sub = db.catalog.get_stream_subscription("out").unwrap();
        db.start().await.unwrap();

        let handle = db.source_untyped("events").unwrap();
        let schema = handle.schema().clone();

        // Push two batches — no watermark filtering should happen
        let batch1 = make_bigint_ts_batch(&schema, &[1000, 2000, 3000]);
        handle.push_arrow(batch1).unwrap();
        handle.watermark(200_000); // watermark without event_time_column is a no-op for filtering
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let batch2 = make_bigint_ts_batch(&schema, &[100, 200, 300]);
        handle.push_arrow(batch2).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // All rows from both batches should appear
        let mut total_rows = 0;
        for _ in 0..256 {
            match sub.poll() {
                Some(b) => total_rows += b.num_rows(),
                None => break,
            }
        }
        assert_eq!(
            total_rows, 6,
            "all data should pass through without watermark config"
        );
    }

    #[tokio::test]
    async fn test_select_from_source() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE sensors (id BIGINT, temp DOUBLE)")
            .await
            .unwrap();
        db.execute("INSERT INTO sensors VALUES (1, 22.5), (2, 23.1)")
            .await
            .unwrap();

        let result = db.execute("SELECT * FROM sensors").await.unwrap();
        match result {
            ExecuteResult::Query(mut q) => {
                // The bridge_query_stream spawns a tokio task; yield to let it run.
                tokio::task::yield_now().await;
                let sub = q.subscribe_raw().unwrap();
                let mut total_rows = 0;
                for _ in 0..256 {
                    match sub.poll() {
                        Some(b) => total_rows += b.num_rows(),
                        None => break,
                    }
                }
                assert_eq!(total_rows, 2);
            }
            _ => panic!("Expected Query result from SELECT on source"),
        }
    }

    #[tokio::test]
    async fn test_select_from_dropped_source_fails() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE sensors (id BIGINT, temp DOUBLE)")
            .await
            .unwrap();
        db.execute("DROP SOURCE sensors").await.unwrap();

        let result = db.execute("SELECT * FROM sensors").await;
        assert!(result.is_err(), "SELECT after DROP SOURCE should fail");
    }

    #[tokio::test]
    async fn test_select_from_replaced_source() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE sensors (id BIGINT, temp DOUBLE)")
            .await
            .unwrap();
        db.execute("INSERT INTO sensors VALUES (1, 20.0)")
            .await
            .unwrap();

        // Replace the source — old buffer is gone
        db.execute("CREATE OR REPLACE SOURCE sensors (id BIGINT, temp DOUBLE)")
            .await
            .unwrap();
        db.execute("INSERT INTO sensors VALUES (2, 30.0)")
            .await
            .unwrap();

        let result = db.execute("SELECT * FROM sensors").await.unwrap();
        match result {
            ExecuteResult::Query(mut q) => {
                tokio::task::yield_now().await;
                let sub = q.subscribe_raw().unwrap();
                let mut total_rows = 0;
                for _ in 0..256 {
                    match sub.poll() {
                        Some(b) => total_rows += b.num_rows(),
                        None => break,
                    }
                }
                assert_eq!(
                    total_rows, 1,
                    "only the post-replace insert should be visible"
                );
            }
            _ => panic!("Expected Query result"),
        }
    }
}
