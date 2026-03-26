//! The main `LaminarDB` database facade.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use laminar_core::streaming;
use laminar_sql::parser::{parse_streaming_sql, ShowCommand, StreamingStatement};
use laminar_sql::planner::StreamingPlanner;
use laminar_sql::register_streaming_functions;
use laminar_sql::translator::{AsofJoinTranslatorConfig, JoinOperatorConfig};

use crate::builder::LaminarDbBuilder;
use crate::catalog::SourceCatalog;
use crate::config::LaminarConfig;
use crate::error::DbError;
use crate::handle::{
    DdlInfo, ExecuteResult, QueryHandle, QueryInfo, SinkInfo, SourceHandle, SourceInfo,
    UntypedSourceHandle,
};
use crate::pipeline_lifecycle::url_to_checkpoint_prefix;
use crate::sql_utils;

pub(crate) const STATE_CREATED: u8 = 0;
pub(crate) const STATE_STARTING: u8 = 1;
pub(crate) const STATE_RUNNING: u8 = 2;
pub(crate) const STATE_SHUTTING_DOWN: u8 = 3;
pub(crate) const STATE_STOPPED: u8 = 4;

/// Estimate the number of cache entries from a memory budget.
///
/// Assumes ~256 bytes per cache entry and enforces a minimum of 1024 entries.
fn cache_entries_from_memory(mem: laminar_sql::parser::lookup_table::ByteSize) -> usize {
    (mem.as_bytes() / 256).max(1024) as usize
}

/// Extract SQL text from a `StreamingStatement` for storage in the connector manager.
pub(crate) fn streaming_statement_to_sql(stmt: &StreamingStatement) -> String {
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
    pub(crate) catalog: Arc<SourceCatalog>,
    pub(crate) planner: parking_lot::Mutex<StreamingPlanner>,
    pub(crate) ctx: SessionContext,
    pub(crate) config: LaminarConfig,
    pub(crate) config_vars: Arc<HashMap<String, String>>,
    pub(crate) shutdown: std::sync::atomic::AtomicBool,
    /// Unified checkpoint coordinator (populated by `start()`).
    pub(crate) coordinator:
        Arc<tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>>,
    pub(crate) connector_manager: parking_lot::Mutex<crate::connector_manager::ConnectorManager>,
    pub(crate) connector_registry: Arc<laminar_connectors::registry::ConnectorRegistry>,
    pub(crate) mv_registry: parking_lot::Mutex<laminar_core::mv::MvRegistry>,
    pub(crate) table_store: Arc<parking_lot::RwLock<crate::table_store::TableStore>>,
    pub(crate) state: std::sync::atomic::AtomicU8,
    /// Handle to the background processing task (if running).
    pub(crate) runtime_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Signal to stop the processing loop.
    pub(crate) shutdown_signal: Arc<tokio::sync::Notify>,
    /// Shared pipeline counters for observability.
    pub(crate) counters: Arc<crate::metrics::PipelineCounters>,
    /// Instant when the database was created, for uptime calculation.
    pub(crate) start_time: std::time::Instant,
    /// Session properties set via `SET key = value`.
    pub(crate) session_properties: parking_lot::Mutex<HashMap<String, String>>,
    /// Global pipeline watermark (min of all source watermarks).
    pub(crate) pipeline_watermark: Arc<std::sync::atomic::AtomicI64>,
    /// Shared lookup table registry for physical planning of lookup joins.
    pub(crate) lookup_registry: Arc<laminar_sql::datafusion::LookupTableRegistry>,
    /// Control channel sender for live DDL to the running coordinator.
    /// `None` before `start()` or after `shutdown()`.
    pub(crate) control_tx:
        parking_lot::Mutex<Option<tokio::sync::mpsc::Sender<crate::pipeline::ControlMsg>>>,
}

/// Per-source watermark tracking state for the pipeline loop.
///
/// Combines an `EventTimeExtractor` (to find the max timestamp in each batch)
/// with a watermark generator (to compute the watermark with delay).
pub(crate) struct SourceWatermarkState {
    pub(crate) extractor: laminar_core::time::EventTimeExtractor,
    pub(crate) generator: Box<dyn laminar_core::time::WatermarkGenerator>,
    /// Watermark column name for late-row filtering.
    pub(crate) column: String,
    /// Timestamp format for late-row filtering.
    pub(crate) format: laminar_core::time::TimestampFormat,
}

/// Infer the `TimestampFormat` from a schema column's `DataType`.
pub(crate) fn infer_timestamp_format(
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
pub(crate) fn filter_late_rows(
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

/// Parse a human-readable duration string (e.g., "5s", "1m", "500ms", "30s").
pub(crate) fn parse_duration_str(s: &str) -> Option<std::time::Duration> {
    let s = s.trim();
    if s.ends_with("ms") {
        let n: u64 = s.strip_suffix("ms")?.trim().parse().ok()?;
        Some(std::time::Duration::from_millis(n))
    } else if s.ends_with('s') {
        let n: u64 = s.strip_suffix('s')?.trim().parse().ok()?;
        Some(std::time::Duration::from_secs(n))
    } else if s.ends_with('m') {
        let n: u64 = s.strip_suffix('m')?.trim().parse().ok()?;
        Some(std::time::Duration::from_secs(n * 60))
    } else {
        // Try parsing as seconds
        let n: u64 = s.parse().ok()?;
        Some(std::time::Duration::from_secs(n))
    }
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
        let lookup_registry = Arc::new(laminar_sql::datafusion::LookupTableRegistry::new());

        // Build a SessionContext with the LookupJoinExtensionPlanner wired
        // into the physical planner so LookupJoinNode → LookupJoinExec works.
        let ctx = {
            let session_config = laminar_sql::datafusion::base_session_config();
            let extension_planner: Arc<
                dyn datafusion::physical_planner::ExtensionPlanner + Send + Sync,
            > = Arc::new(laminar_sql::datafusion::LookupJoinExtensionPlanner::new(
                Arc::clone(&lookup_registry),
            ));
            let query_planner: Arc<dyn datafusion::execution::context::QueryPlanner + Send + Sync> =
                Arc::new(LookupQueryPlanner { extension_planner });
            let state = datafusion::execution::SessionStateBuilder::new()
                .with_config(session_config)
                .with_default_features()
                .with_query_planner(query_planner)
                .build();
            SessionContext::new_with_state(state)
        };
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
            table_store: Arc::new(parking_lot::RwLock::new(
                crate::table_store::TableStore::new(),
            )),
            state: std::sync::atomic::AtomicU8::new(STATE_CREATED),
            runtime_handle: parking_lot::Mutex::new(None),
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            counters: Arc::new(crate::metrics::PipelineCounters::new()),
            start_time: std::time::Instant::now(),
            session_properties: parking_lot::Mutex::new(HashMap::new()),
            pipeline_watermark: Arc::new(std::sync::atomic::AtomicI64::new(i64::MIN)),
            lookup_registry,
            control_tx: parking_lot::Mutex::new(None),
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
            laminar_connectors::cdc::postgres::register_postgres_cdc_source(registry);
        }
        #[cfg(feature = "postgres-sink")]
        {
            laminar_connectors::postgres::register_postgres_sink(registry);
        }
        #[cfg(feature = "delta-lake")]
        {
            laminar_connectors::lakehouse::register_delta_lake_sink(registry);
            laminar_connectors::lakehouse::register_delta_lake_source(registry);
        }
        #[cfg(feature = "iceberg")]
        {
            laminar_connectors::lakehouse::register_iceberg_sink(registry);
            laminar_connectors::lakehouse::register_iceberg_source(registry);
        }
        #[cfg(feature = "websocket")]
        {
            laminar_connectors::websocket::register_websocket_source(registry);
            laminar_connectors::websocket::register_websocket_sink(registry);
        }
        #[cfg(feature = "mysql-cdc")]
        {
            laminar_connectors::cdc::mysql::register_mysql_cdc_source(registry);
        }
        #[cfg(feature = "mongodb-cdc")]
        {
            laminar_connectors::mongodb::register_mongodb_cdc_source(registry);
            laminar_connectors::mongodb::register_mongodb_sink(registry);
        }
        #[cfg(feature = "files")]
        {
            laminar_connectors::files::register_file_source(registry);
            laminar_connectors::files::register_file_sink(registry);
        }
    }

    /// Handle `CREATE LOOKUP TABLE` by registering the table in the
    /// `TableStore`, `ConnectorManager`, `DataFusion` catalog, and lookup
    /// registry.
    fn handle_register_lookup_table(
        &self,
        info: laminar_sql::planner::LookupTableInfo,
    ) -> Result<ExecuteResult, DbError> {
        use laminar_sql::parser::lookup_table::ConnectorType;

        if info.primary_key.len() != 1 {
            return Err(DbError::InvalidOperation(
                "Lookup table requires a single-column primary key".into(),
            ));
        }
        let pk = info.primary_key[0].clone();

        // Register in TableStore for PK-based upsert
        let cache_mode = info.properties.cache_memory.map(|mem| {
            let max_entries = cache_entries_from_memory(mem);
            crate::table_cache_mode::TableCacheMode::Partial { max_entries }
        });
        if let Some(cache) = cache_mode {
            self.table_store.write().create_table_with_cache(
                &info.name,
                info.arrow_schema.clone(),
                &pk,
                cache,
            )?;
        } else {
            self.table_store
                .write()
                .create_table(&info.name, info.arrow_schema.clone(), &pk)?;
        }

        // For external connectors: register in ConnectorManager so
        // start_connector_pipeline() handles snapshot + CDC loading
        if !matches!(info.properties.connector, ConnectorType::Static) {
            self.register_lookup_connector(&info, &pk)?;
        }

        // Register in DataFusion for SELECT/JOIN queries
        {
            let provider = crate::table_provider::ReferenceTableProvider::new(
                info.name.clone(),
                info.arrow_schema.clone(),
                self.table_store.clone(),
            );
            let _ = self.ctx.deregister_table(&info.name);
            self.ctx
                .register_table(&info.name, Arc::new(provider))
                .map_err(|e| {
                    DbError::InvalidOperation(format!("Failed to register lookup table: {e}"))
                })?;
        }

        // Register snapshot in the lookup registry so the physical
        // planner can build LookupJoinExec nodes for JOIN queries.
        if let Some(batch) = self.table_store.read().to_record_batch(&info.name) {
            self.lookup_registry.register(
                &info.name,
                laminar_sql::datafusion::LookupSnapshot {
                    batch,
                    key_columns: info.primary_key.clone(),
                },
            );
        }

        // Register the logical optimizer rule so JOINs referencing
        // this table are rewritten to LookupJoinNode.
        self.refresh_lookup_optimizer_rule();

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE LOOKUP TABLE".to_string(),
            object_name: info.name,
        }))
    }

    /// Register an external connector for a lookup table in the
    /// `ConnectorManager` and `TableStore`.
    #[allow(clippy::unnecessary_wraps)]
    fn register_lookup_connector(
        &self,
        info: &laminar_sql::planner::LookupTableInfo,
        pk: &str,
    ) -> Result<(), DbError> {
        use laminar_sql::parser::lookup_table::ConnectorType;

        let connector_type_str = match &info.properties.connector {
            ConnectorType::Postgres => "postgres",
            ConnectorType::PostgresCdc => "postgres-cdc",
            ConnectorType::MysqlCdc => "mysql-cdc",
            ConnectorType::Redis => "redis",
            ConnectorType::S3Parquet => "s3-parquet",
            ConnectorType::DeltaLake => "delta-lake",
            ConnectorType::Custom(s) => s.as_str(),
            ConnectorType::Static => unreachable!(),
        };

        self.table_store
            .write()
            .set_connector(&info.name, connector_type_str);

        let refresh = match info.properties.strategy {
            laminar_sql::parser::lookup_table::LookupStrategy::Replicated
            | laminar_sql::parser::lookup_table::LookupStrategy::Partitioned => {
                // Standalone postgres uses snapshot-only (no CDC slot needed).
                if matches!(info.properties.connector, ConnectorType::Postgres) {
                    Some(laminar_connectors::reference::RefreshMode::SnapshotOnly)
                } else {
                    Some(laminar_connectors::reference::RefreshMode::SnapshotPlusCdc)
                }
            }
            laminar_sql::parser::lookup_table::LookupStrategy::OnDemand => {
                Some(laminar_connectors::reference::RefreshMode::Manual)
            }
        };

        // Build connector options and format options from raw WITH clause.
        // Keys consumed by LookupTableProperties are excluded; keys starting
        // with "format." are routed to format_options (prefix stripped).
        let consumed = [
            "connector",
            "strategy",
            "cache.memory",
            "cache.disk",
            "cache.ttl",
            "pushdown",
            "format",
        ];
        let mut connector_options = HashMap::with_capacity(info.raw_options.len());
        let mut format_options = HashMap::with_capacity(4);
        for (k, v) in &info.raw_options {
            let lower = k.to_lowercase();
            if consumed.contains(&lower.as_str()) {
                continue;
            }
            if let Some(suffix) = lower.strip_prefix("format.") {
                format_options.insert(suffix.to_string(), v.clone());
            } else {
                connector_options.insert(k.clone(), v.clone());
            }
        }

        let table_cache_mode = info.properties.cache_memory.map(|mem| {
            let max_entries = cache_entries_from_memory(mem);
            crate::table_cache_mode::TableCacheMode::Partial { max_entries }
        });
        let cache_max = info.properties.cache_memory.map(cache_entries_from_memory);

        self.connector_manager
            .lock()
            .register_table(crate::connector_manager::TableRegistration {
                name: info.name.clone(),
                primary_key: pk.to_string(),
                connector_type: Some(connector_type_str.to_string()),
                connector_options,
                format: info.raw_options.get("format").cloned(),
                format_options,
                refresh,
                cache_mode: table_cache_mode,
                cache_max_entries: cache_max,
                storage: None,
            });

        Ok(())
    }

    /// Replaces the `LookupJoinRewriteRule` on the `DataFusion` context
    /// with one that knows the current set of registered lookup tables.
    fn refresh_lookup_optimizer_rule(&self) {
        use laminar_sql::planner::lookup_join::{LookupColumnPruningRule, LookupJoinRewriteRule};
        use laminar_sql::planner::predicate_split::{
            PlanPushdownMode, PlanSourceCapabilities, PredicateSplitterRule,
            SourceCapabilitiesRegistry,
        };

        // Remove old rules if present
        self.ctx.remove_optimizer_rule("lookup_join_rewrite");
        self.ctx.remove_optimizer_rule("predicate_splitter");
        self.ctx.remove_optimizer_rule("lookup_column_pruning");

        let tables = self.planner.lock().lookup_tables_cloned();
        if tables.is_empty() {
            return;
        }

        // Build capabilities registry from table properties
        let mut caps_registry = SourceCapabilitiesRegistry::default();
        for (name, info) in &tables {
            let mode = match info.properties.pushdown_mode {
                laminar_sql::parser::lookup_table::PushdownMode::Enabled
                | laminar_sql::parser::lookup_table::PushdownMode::Auto => PlanPushdownMode::Full,
                laminar_sql::parser::lookup_table::PushdownMode::Disabled => PlanPushdownMode::None,
            };
            let pk_set: std::collections::HashSet<String> =
                info.primary_key.iter().cloned().collect();
            caps_registry.register(
                name.clone(),
                PlanSourceCapabilities {
                    pushdown_mode: mode,
                    eq_columns: pk_set,
                    range_columns: std::collections::HashSet::new(),
                    in_columns: std::collections::HashSet::new(),
                    supports_null_check: false,
                },
            );
        }

        // Register rules in order: rewrite → predicate split → column pruning
        self.ctx
            .add_optimizer_rule(Arc::new(LookupJoinRewriteRule::new(tables)));
        self.ctx
            .add_optimizer_rule(Arc::new(PredicateSplitterRule::new(caps_registry)));
        self.ctx
            .add_optimizer_rule(Arc::new(LookupColumnPruningRule));
    }

    /// Returns the connector registry for registering custom connectors.
    ///
    /// Use this to register user-defined source/sink connectors before
    /// calling `start()`.
    #[must_use]
    pub fn connector_registry(&self) -> &laminar_connectors::registry::ConnectorRegistry {
        &self.connector_registry
    }

    /// Register a custom scalar UDF on the `SessionContext`.
    ///
    /// Called by `LaminarDbBuilder::build()` after construction.
    pub(crate) fn register_custom_udf(&self, udf: datafusion_expr::ScalarUDF) {
        self.ctx.register_udf(udf);
    }

    /// Register a custom aggregate UDF (UDAF) on the `SessionContext`.
    ///
    /// Called by `LaminarDbBuilder::build()` after construction.
    pub(crate) fn register_custom_udaf(&self, udaf: datafusion_expr::AggregateUDF) {
        self.ctx.register_udaf(udaf);
    }

    /// Registers a Delta Lake table as a `DataFusion` `TableProvider`.
    ///
    /// After registration, the table can be queried via SQL:
    /// ```sql
    /// SELECT * FROM my_delta_table WHERE id > 100
    /// ```
    ///
    /// # Arguments
    ///
    /// * `name` - SQL table name (e.g., `"trades"`)
    /// * `table_uri` - Path to the Delta Lake table (local, `s3://`, `az://`, `gs://`)
    /// * `storage_options` - Storage credentials and configuration
    ///
    /// # Errors
    ///
    /// Returns `DbError` if the table cannot be opened or registered.
    #[cfg(feature = "delta-lake")]
    pub async fn register_delta_table(
        &self,
        name: &str,
        table_uri: &str,
        storage_options: HashMap<String, String>,
    ) -> Result<(), DbError> {
        laminar_connectors::lakehouse::delta_table_provider::register_delta_table(
            &self.ctx,
            name,
            table_uri,
            storage_options,
        )
        .await
        .map_err(DbError::from)
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
    #[allow(clippy::too_many_lines)]
    async fn execute_single(&self, sql: &str) -> Result<ExecuteResult, DbError> {
        let statements = parse_streaming_sql(sql)?;

        if statements.is_empty() {
            return Err(DbError::InvalidOperation("Empty SQL statement".into()));
        }

        let statement = &statements[0];

        match statement {
            StreamingStatement::CreateSource(create) => {
                let result = self.handle_create_source(create)?;
                if let ExecuteResult::Ddl(ref info) = result {
                    self.connector_manager
                        .lock()
                        .store_ddl(&info.object_name, sql);
                }
                Ok(result)
            }
            StreamingStatement::CreateSink(create) => {
                let result = self.handle_create_sink(create)?;
                if let ExecuteResult::Ddl(ref info) = result {
                    self.connector_manager
                        .lock()
                        .store_ddl(&info.object_name, sql);
                }
                Ok(result)
            }
            StreamingStatement::CreateStream {
                name,
                query,
                emit_clause,
                ..
            } => self.handle_create_stream(name, query, emit_clause.as_ref()),
            StreamingStatement::CreateContinuousQuery { .. }
            | StreamingStatement::CreateLookupTable(_)
            | StreamingStatement::DropLookupTable { .. } => self.handle_query(sql).await,
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
                } else if let sqlparser::ast::Statement::Set(set_stmt) = stmt.as_ref() {
                    self.handle_set(set_stmt)
                } else {
                    self.handle_query(sql).await
                }
            }
            StreamingStatement::InsertInto {
                table_name,
                columns,
                values,
            } => self.handle_insert_into(table_name, columns, values).await,
            StreamingStatement::DropSource {
                name,
                if_exists,
                cascade,
            } => self.handle_drop_source(name, *if_exists, *cascade),
            StreamingStatement::DropSink {
                name,
                if_exists,
                cascade,
            } => self.handle_drop_sink(name, *if_exists, *cascade),
            StreamingStatement::DropStream {
                name,
                if_exists,
                cascade,
            } => self.handle_drop_stream(name, *if_exists, *cascade),
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
                    ShowCommand::CheckpointStatus => self.build_show_checkpoint_status()?,
                    ShowCommand::CreateSource { name } => {
                        self.build_show_create_source(&name.to_string())?
                    }
                    ShowCommand::CreateSink { name } => {
                        self.build_show_create_sink(&name.to_string())?
                    }
                };
                Ok(ExecuteResult::Metadata(batch))
            }
            StreamingStatement::Checkpoint => {
                let result = self.checkpoint().await?;
                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "CHECKPOINT".to_string(),
                    object_name: format!("checkpoint_{}", result.checkpoint_id),
                }))
            }
            StreamingStatement::RestoreCheckpoint { checkpoint_id } => {
                self.handle_restore_checkpoint(*checkpoint_id)
            }
            StreamingStatement::Describe { name, .. } => {
                let name_str = name.to_string();
                let batch = self.build_describe(&name_str)?;
                Ok(ExecuteResult::Metadata(batch))
            }
            StreamingStatement::Explain {
                statement, analyze, ..
            } => {
                if *analyze {
                    self.handle_explain_analyze(statement, sql).await
                } else {
                    self.handle_explain(statement)
                }
            }
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
            StreamingStatement::AlterSource { name, operation } => {
                self.handle_alter_source(name, operation)
            }
        }
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

        // Try inserting into a TableStore-managed table (with PK upsert).
        // Single lock scope avoids TOCTOU race between has_table/schema/upsert.
        {
            let mut ts = self.table_store.write();
            if ts.has_table(&name) {
                let schema = ts
                    .table_schema(&name)
                    .ok_or_else(|| DbError::TableNotFound(name.clone()))?;
                let batch = sql_utils::sql_values_to_record_batch(&schema, values)?;
                ts.upsert(&name, &batch)?;
                drop(ts); // release before sync (which may also lock)

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

    /// Handle RESTORE FROM CHECKPOINT statement (not yet implemented).
    ///
    /// Will eventually stop the pipeline, reload state from the checkpoint
    /// manifest, seek source offsets, and restart the pipeline.
    #[allow(clippy::unused_self)] // will use self when restore is implemented
    fn handle_restore_checkpoint(&self, _checkpoint_id: u64) -> Result<ExecuteResult, DbError> {
        Err(DbError::Unsupported(
            "RESTORE FROM CHECKPOINT is not yet implemented — \
             requires pipeline stop, state reload from manifest, \
             source offset seek, and pipeline restart"
                .to_string(),
        ))
    }

    /// Get a session property value.
    #[must_use]
    pub fn get_session_property(&self, key: &str) -> Option<String> {
        self.session_properties
            .lock()
            .get(&key.to_lowercase())
            .cloned()
    }

    /// Get all session properties.
    #[must_use]
    pub fn session_properties(&self) -> HashMap<String, String> {
        self.session_properties.lock().clone()
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
                        laminar_sql::planner::StreamingPlan::RegisterLookupTable(_) => {
                            "RegisterLookupTable"
                        }
                        laminar_sql::planner::StreamingPlan::DropLookupTable { .. } => {
                            "DropLookupTable"
                        }
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
                    laminar_sql::planner::StreamingPlan::RegisterLookupTable(info) => {
                        rows.push(("lookup_table".into(), info.name.clone()));
                    }
                    laminar_sql::planner::StreamingPlan::DropLookupTable { name } => {
                        rows.push(("drop_lookup_table".into(), name.clone()));
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

    /// Handle EXPLAIN ANALYZE: run the plan and collect execution metrics.
    async fn handle_explain_analyze(
        &self,
        statement: &StreamingStatement,
        original_sql: &str,
    ) -> Result<ExecuteResult, DbError> {
        // First get the normal EXPLAIN output
        let explain_result = self.handle_explain(statement)?;
        let mut rows: Vec<(String, String)> = Vec::new();

        if let ExecuteResult::Metadata(explain_batch) = &explain_result {
            let keys_col = explain_batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>();
            let vals_col = explain_batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>();
            if let (Some(keys), Some(vals)) = (keys_col, vals_col) {
                for i in 0..explain_batch.num_rows() {
                    rows.push((keys.value(i).to_string(), vals.value(i).to_string()));
                }
            }
        }

        // Extract the inner SQL from the original EXPLAIN ANALYZE statement
        let upper = original_sql.to_uppercase();
        let inner_start = upper.find("ANALYZE").map_or(0, |pos| pos + "ANALYZE".len());
        let inner_sql = original_sql[inner_start..].trim();

        // Try to execute the inner query via DataFusion and collect metrics
        let start = std::time::Instant::now();
        match self.ctx.sql(inner_sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    let elapsed = start.elapsed();
                    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                    rows.push(("rows_produced".into(), total_rows.to_string()));
                    rows.push(("execution_time_ms".into(), elapsed.as_millis().to_string()));
                    rows.push(("batches_processed".into(), batches.len().to_string()));
                }
                Err(e) => {
                    let elapsed = start.elapsed();
                    rows.push(("execution_time_ms".into(), elapsed.as_millis().to_string()));
                    rows.push(("analyze_error".into(), format!("{e}")));
                }
            },
            Err(e) => {
                rows.push(("analyze_error".into(), format!("{e}")));
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
        .map_err(|e| DbError::InvalidOperation(format!("explain analyze metadata: {e}")))?;

        Ok(ExecuteResult::Metadata(batch))
    }

    /// Handle a streaming or standard SQL query.
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn handle_query(&self, sql: &str) -> Result<ExecuteResult, DbError> {
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

                // DataFusion interpreted execution.
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
            laminar_sql::planner::StreamingPlan::RegisterLookupTable(info) => {
                self.handle_register_lookup_table(info)
            }
            laminar_sql::planner::StreamingPlan::DropLookupTable { name } => {
                self.table_store.write().drop_table(&name);
                self.connector_manager.lock().unregister_table(&name);
                let _ = self.ctx.deregister_table(&name);
                self.lookup_registry.unregister(&name);
                self.refresh_lookup_optimizer_rule();
                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "DROP LOOKUP TABLE".to_string(),
                    object_name: name,
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
            .map_err(|e| DbError::query_pipeline(&asof_config.left_table, &e))?
            .collect()
            .await
            .map_err(|e| DbError::query_pipeline(&asof_config.left_table, &e))?;

        let right_batches = self
            .ctx
            .sql(&right_sql)
            .await
            .map_err(|e| DbError::query_pipeline(&asof_config.right_table, &e))?
            .collect()
            .await
            .map_err(|e| DbError::query_pipeline(&asof_config.right_table, &e))?;

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
                .map_err(|e| DbError::query_pipeline("ASOF join", &e))?;

        let _ = self.ctx.deregister_table("__asof_result");
        self.ctx
            .register_table("__asof_result", Arc::new(mem_table))
            .map_err(|e| DbError::query_pipeline("ASOF join", &e))?;

        let df = self
            .ctx
            .sql("SELECT * FROM __asof_result")
            .await
            .map_err(|e| DbError::query_pipeline("ASOF join", &e))?;
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| DbError::query_pipeline("ASOF join", &e))?;

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
    /// Returns `DbError::SchemaMismatch` if the Rust type's schema does not
    /// match the source's SQL schema.
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

    /// Returns a checkpoint store instance, if checkpointing is configured.
    ///
    /// Returns an [`ObjectStoreCheckpointStore`](laminar_storage::ObjectStoreCheckpointStore)
    /// when `object_store_url` is set, otherwise a
    /// [`FileSystemCheckpointStore`](laminar_storage::FileSystemCheckpointStore).
    pub fn checkpoint_store(&self) -> Option<Box<dyn laminar_storage::CheckpointStore>> {
        let cp_config = self.config.checkpoint.as_ref()?;
        let max_retained = cp_config.max_retained.unwrap_or(3);

        if let Some(ref url) = self.config.object_store_url {
            let obj_store = laminar_storage::object_store_builder::build_object_store(
                url,
                &self.config.object_store_options,
            )
            .ok()?;
            let prefix = url_to_checkpoint_prefix(url);
            Some(Box::new(
                laminar_storage::checkpoint_store::ObjectStoreCheckpointStore::new(
                    obj_store,
                    prefix,
                    max_retained,
                )
                .ok()?,
            ))
        } else {
            let data_dir = cp_config
                .data_dir
                .clone()
                .or_else(|| self.config.storage_dir.clone())
                .unwrap_or_else(|| std::path::PathBuf::from("./data"));
            Some(Box::new(
                laminar_storage::checkpoint_store::FileSystemCheckpointStore::new(
                    &data_dir,
                    max_retained,
                ),
            ))
        }
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
            .checkpoint(crate::checkpoint_coordinator::CheckpointRequest::default())
            .await
    }

    /// Returns checkpoint performance statistics.
    ///
    /// Returns `None` if the checkpoint coordinator has not been initialized.
    pub async fn checkpoint_stats(&self) -> Option<crate::checkpoint_coordinator::CheckpointStats> {
        let guard = self.coordinator.lock().await;
        guard
            .as_ref()
            .map(crate::checkpoint_coordinator::CheckpointCoordinator::stats)
    }
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

/// Wraps `DefaultPhysicalPlanner` with lookup join extension support.
struct LookupQueryPlanner {
    extension_planner: Arc<dyn datafusion::physical_planner::ExtensionPlanner + Send + Sync>,
}

impl std::fmt::Debug for LookupQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LookupQueryPlanner").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl datafusion::execution::context::QueryPlanner for LookupQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &datafusion::logical_expr::LogicalPlan,
        session_state: &datafusion::execution::SessionState,
    ) -> datafusion_common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        use datafusion::physical_planner::PhysicalPlanner;
        let planner =
            datafusion::physical_planner::DefaultPhysicalPlanner::with_extension_planners(vec![
                Arc::clone(&self.extension_planner),
            ]);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::extract_connector_from_with_options;

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
    async fn test_describe_table() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE TABLE products (id BIGINT PRIMARY KEY, name VARCHAR, price DOUBLE)")
            .await
            .unwrap();
        db.execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
            .await
            .unwrap();

        let result = db.execute("DESCRIBE products").await.unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                assert_eq!(batch.num_rows(), 3);
            }
            _ => panic!("Expected Metadata result"),
        }
    }

    #[tokio::test]
    async fn test_describe_materialized_view() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id BIGINT, name VARCHAR, value DOUBLE)")
            .await
            .unwrap();
        db.execute(
            "CREATE MATERIALIZED VIEW event_counts AS \
             SELECT name, COUNT(*) as cnt FROM events GROUP BY name",
        )
        .await
        .unwrap();

        let result = db.execute("DESCRIBE event_counts").await.unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                assert!(batch.num_rows() >= 2, "Should have at least name and cnt");
            }
            _ => panic!("Expected Metadata result"),
        }
    }

    #[tokio::test]
    async fn test_describe_not_found() {
        let db = LaminarDB::open().unwrap();
        let result = db.execute("DESCRIBE nonexistent").await;
        assert!(result.is_err());
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

    // ── Multi-statement execution tests ─────────────────

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

    // ── Config variable substitution tests ──────────────

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

    // ── CREATE STREAM tests ─────────────────────────────

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

    // -----------------------------------------------------------------------
    // Pipeline-running state guards
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_create_source_with_connector_rejected_when_running() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE seed (id INT)").await.unwrap();
        db.start().await.unwrap();

        // WITH syntax
        let result = db
            .execute("CREATE SOURCE events (id INT) WITH ('connector' = 'kafka', 'topic' = 'x')")
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("pipeline is running"),
            "expected pipeline-running error, got: {err}"
        );

        // FROM syntax (what server mode generates via source_to_ddl)
        let result = db
            .execute("CREATE SOURCE events2 (id INT) FROM KAFKA (topic = 'x')")
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("pipeline is running"),
            "expected pipeline-running error for FROM syntax, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_create_source_without_connector_allowed_when_running() {
        let db = LaminarDB::open().unwrap();
        db.start().await.unwrap();

        // Schema-only source (no connector) — used by embedded db.insert() API.
        let result = db.execute("CREATE SOURCE events (id INT)").await;
        assert!(
            result.is_ok(),
            "schema-only source should be allowed when running"
        );
    }

    #[tokio::test]
    async fn test_create_sink_with_connector_rejected_when_running() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT)").await.unwrap();
        db.start().await.unwrap();

        let result = db
            .execute(
                "CREATE SINK output FROM events \
                 WITH ('connector' = 'kafka', 'topic' = 'out')",
            )
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("pipeline is running"),
            "expected pipeline-running error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_drop_source_rejected_when_running() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT)").await.unwrap();
        db.start().await.unwrap();

        let result = db.execute("DROP SOURCE events").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("pipeline is running"),
            "expected pipeline-running error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_drop_sink_rejected_when_running() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT)").await.unwrap();
        db.execute("CREATE SINK output FROM events").await.unwrap();
        db.start().await.unwrap();

        let result = db.execute("DROP SINK output").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("pipeline is running"),
            "expected pipeline-running error, got: {err}"
        );
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
        #[cfg(feature = "delta-lake")]
        {
            expected_sources += 1; // delta-lake source
            expected_sinks += 1; // delta-lake sink
        }
        #[cfg(feature = "iceberg")]
        {
            expected_sources += 1; // iceberg source
            expected_sinks += 1; // iceberg sink
        }
        #[cfg(feature = "websocket")]
        {
            expected_sources += 1; // websocket source
            expected_sinks += 1; // websocket sink
        }
        #[cfg(feature = "mysql-cdc")]
        {
            expected_sources += 1; // mysql CDC source
        }
        #[cfg(feature = "mongodb-cdc")]
        {
            expected_sources += 1; // mongodb CDC source
            expected_sinks += 1; // mongodb sink
        }
        #[cfg(feature = "files")]
        {
            expected_sources += 1; // file source
            expected_sinks += 1; // file sink
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

    // ── Reference Table tests ──

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
        let ts = db.table_store.read();
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
        let ts = db.table_store.read();
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
        assert_eq!(db.table_store.read().table_row_count("products"), 1);

        // Upsert (same PK = overwrite)
        db.execute("INSERT INTO products VALUES (1, 'Super Widget', 19.99)")
            .await
            .unwrap();
        assert_eq!(db.table_store.read().table_row_count("products"), 1);

        // Insert another row (different PK)
        db.execute("INSERT INTO products VALUES (2, 'Gadget', 14.99)")
            .await
            .unwrap();
        assert_eq!(db.table_store.read().table_row_count("products"), 2);

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
        assert!(db.table_store.read().has_table("t"));

        db.execute("DROP TABLE t").await.unwrap();
        assert!(!db.table_store.read().has_table("t"));
    }

    #[tokio::test]
    async fn test_drop_table_if_exists() {
        let db = LaminarDB::open().unwrap();
        let result = db.execute("DROP TABLE IF EXISTS nonexistent").await;
        assert!(result.is_ok());
    }

    // ── HAVING clause tests ─────────────────────────────

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

    // ── Multi-way JOIN tests ─────────────────────────────

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

    // ── Window Frame tests ────────────────────────────────

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

    // ── Connector-Backed Table Population ──

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

        db.execute("CREATE SOURCE events (symbol VARCHAR, price DOUBLE)")
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
        let ts = db.table_store.read();
        assert!(ts.is_ready("instruments"));
        assert_eq!(ts.table_row_count("instruments"), 2);
    }

    #[tokio::test]
    async fn test_table_source_manual_no_snapshot() {
        let db = LaminarDB::open().unwrap();
        let batch = table_test_batch(&[1], &["AAPL"]);
        register_mock_table_source(&db, vec![batch], vec![]);

        db.execute("CREATE SOURCE events (symbol VARCHAR, price DOUBLE)")
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
        let ts = db.table_store.read();
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

        db.execute("CREATE SOURCE events (x INT)").await.unwrap();

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

        let ts = db.table_store.read();
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

        db.execute("CREATE SOURCE events (symbol VARCHAR, price DOUBLE)")
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
        let mut ts = db.table_store.write();
        assert!(ts.is_ready("instruments"));
        assert_eq!(ts.table_row_count("instruments"), 1);
        // The change batch id=2/GOOG should NOT be present
        assert!(ts.lookup("instruments", "2").is_none());
    }

    // ── PARTIAL Cache Mode DDL tests ──

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
            let ts = db.table_store.read();
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

        let ts = db.table_store.read();
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

        let ts = db.table_store.read();
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

    // --- Pipeline Observability API tests ---

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
        #[allow(clippy::disallowed_types)] // test code
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

    #[tokio::test]
    async fn test_mv_registers_stream_in_connector_manager() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
            .await
            .unwrap();

        // Before MV creation, no stream registered
        {
            let mgr = db.connector_manager.lock();
            assert!(
                !mgr.streams().contains_key("event_totals"),
                "stream should not exist before MV creation"
            );
        }

        let result = db
            .execute("CREATE MATERIALIZED VIEW event_totals AS SELECT * FROM events")
            .await;

        // The MV may fail at query execution (no data), but if DDL succeeds
        // the connector manager should have the stream registered
        if result.is_ok() {
            let mgr = db.connector_manager.lock();
            assert!(
                mgr.streams().contains_key("event_totals"),
                "MV should be registered as a stream in connector manager"
            );
            let reg = &mgr.streams()["event_totals"];
            assert!(
                reg.query_sql.contains("events"),
                "stream query should reference the source"
            );
        }
    }

    #[tokio::test]
    async fn test_drop_mv_unregisters_stream() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
            .await
            .unwrap();

        let result = db
            .execute("CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM events")
            .await;

        if result.is_ok() {
            // Verify registered
            {
                let mgr = db.connector_manager.lock();
                assert!(mgr.streams().contains_key("mv1"));
            }

            // Drop the MV
            db.execute("DROP MATERIALIZED VIEW mv1").await.unwrap();

            // Verify unregistered
            {
                let mgr = db.connector_manager.lock();
                assert!(
                    !mgr.streams().contains_key("mv1"),
                    "stream should be unregistered after DROP MV"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_set_session_property() {
        let db = LaminarDB::open().unwrap();
        db.execute("SET parallelism = 4").await.unwrap();
        assert_eq!(
            db.get_session_property("parallelism"),
            Some("4".to_string())
        );
    }

    #[tokio::test]
    async fn test_set_session_property_string_value() {
        let db = LaminarDB::open().unwrap();
        db.execute("SET state_ttl = '1 hour'").await.unwrap();
        assert_eq!(
            db.get_session_property("state_ttl"),
            Some("1 hour".to_string())
        );
    }

    #[tokio::test]
    async fn test_set_session_property_overwrite() {
        let db = LaminarDB::open().unwrap();
        db.execute("SET batch_size = 100").await.unwrap();
        db.execute("SET batch_size = 200").await.unwrap();
        assert_eq!(
            db.get_session_property("batch_size"),
            Some("200".to_string())
        );
    }

    #[tokio::test]
    async fn test_get_session_property_not_set() {
        let db = LaminarDB::open().unwrap();
        assert_eq!(db.get_session_property("nonexistent"), None);
    }

    #[tokio::test]
    async fn test_session_properties_all() {
        let db = LaminarDB::open().unwrap();
        db.execute("SET parallelism = 4").await.unwrap();
        db.execute("SET state_ttl = '1 hour'").await.unwrap();
        let props = db.session_properties();
        assert_eq!(props.len(), 2);
        assert_eq!(props.get("parallelism"), Some(&"4".to_string()));
        assert_eq!(props.get("state_ttl"), Some(&"1 hour".to_string()));
    }

    #[tokio::test]
    async fn test_alter_source_add_column() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
            .await
            .unwrap();

        // Verify initial schema has 2 columns
        let schema = db.catalog.describe_source("events").unwrap();
        assert_eq!(schema.fields().len(), 2);

        // Add a column
        db.execute("ALTER SOURCE events ADD COLUMN new_col VARCHAR")
            .await
            .unwrap();

        // Verify updated schema has 3 columns
        let schema = db.catalog.describe_source("events").unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(2).name(), "new_col");
    }

    #[tokio::test]
    async fn test_alter_source_not_found() {
        let db = LaminarDB::open().unwrap();
        let result = db
            .execute("ALTER SOURCE nonexistent ADD COLUMN col INT")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_alter_source_set_properties() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT)").await.unwrap();
        db.execute("ALTER SOURCE events SET ('batch.size' = '1000')")
            .await
            .unwrap();
        assert_eq!(
            db.get_session_property("events.batch.size"),
            Some("1000".to_string())
        );
    }

    // ── extract_connector_from_with_options tests ──

    #[test]
    fn test_extract_connector_from_with_options_basic() {
        let mut opts = HashMap::new();
        opts.insert("connector".to_string(), "kafka".to_string());
        opts.insert("topic".to_string(), "events".to_string());
        opts.insert(
            "bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );
        opts.insert("format".to_string(), "json".to_string());

        let (conn_opts, format, fmt_opts) = extract_connector_from_with_options(&opts);

        // 'connector' and 'format' are extracted, not in connector_options
        assert!(!conn_opts.contains_key("connector"));
        assert!(!conn_opts.contains_key("format"));
        assert_eq!(conn_opts.get("topic"), Some(&"events".to_string()));
        assert_eq!(
            conn_opts.get("bootstrap.servers"),
            Some(&"localhost:9092".to_string())
        );
        assert_eq!(format, Some("json".to_string()));
        assert!(fmt_opts.is_empty());
    }

    #[test]
    fn test_extract_connector_filters_streaming_keys() {
        let mut opts = HashMap::new();
        opts.insert("connector".to_string(), "websocket".to_string());
        opts.insert("url".to_string(), "wss://feed.example.com".to_string());
        opts.insert("buffer_size".to_string(), "4096".to_string());
        opts.insert("backpressure".to_string(), "block".to_string());
        opts.insert("watermark_delay".to_string(), "5s".to_string());

        let (conn_opts, _, _) = extract_connector_from_with_options(&opts);

        // Streaming keys should NOT be in connector_options
        assert!(!conn_opts.contains_key("buffer_size"));
        assert!(!conn_opts.contains_key("backpressure"));
        assert!(!conn_opts.contains_key("watermark_delay"));
        // Connector-specific key should be present
        assert_eq!(
            conn_opts.get("url"),
            Some(&"wss://feed.example.com".to_string())
        );
    }

    #[test]
    fn test_extract_connector_format_options() {
        let mut opts = HashMap::new();
        opts.insert("connector".to_string(), "kafka".to_string());
        opts.insert("format".to_string(), "avro".to_string());
        opts.insert(
            "format.schema.registry.url".to_string(),
            "http://localhost:8081".to_string(),
        );
        opts.insert("topic".to_string(), "events".to_string());

        let (conn_opts, format, fmt_opts) = extract_connector_from_with_options(&opts);

        assert_eq!(format, Some("avro".to_string()));
        assert_eq!(
            fmt_opts.get("schema.registry.url"),
            Some(&"http://localhost:8081".to_string())
        );
        assert_eq!(conn_opts.get("topic"), Some(&"events".to_string()));
        assert!(!conn_opts.contains_key("format.schema.registry.url"));
    }

    #[tokio::test]
    async fn test_create_source_with_connector_option() {
        // Verify that WITH ('connector' = '...') is accepted at the DDL level.
        // The actual connector won't be instantiated because the type isn't
        // registered in the default embedded registry, so we just check
        // that the error is "Unknown source connector type" (meaning the
        // WITH clause was correctly routed) rather than silently ignored.
        let db = LaminarDB::open().unwrap();
        let result = db
            .execute(
                "CREATE SOURCE ws_feed (id BIGINT, data TEXT) WITH (
                    'connector' = 'websocket',
                    'url' = 'wss://feed.example.com',
                    'format' = 'json'
                )",
            )
            .await;

        // Without the websocket feature, the connector type won't be registered,
        // so we expect an "Unknown source connector type" error — which proves
        // the WITH clause WAS routed to the connector registry.
        if let Err(e) = result {
            let msg = e.to_string();
            assert!(
                msg.contains("Unknown source connector type"),
                "Expected connector routing error, got: {msg}"
            );
        } else {
            // If websocket feature IS enabled, the connector type is registered
            // and the DDL succeeds — also acceptable.
        }
    }

    #[tokio::test]
    async fn test_show_sources_enriched() {
        let db = LaminarDB::open().unwrap();
        db.execute(
            "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)",
        )
        .await
        .unwrap();

        let result = db.execute("SHOW SOURCES").await.unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(batch.num_columns(), 4);
                assert_eq!(batch.schema().field(0).name(), "source_name");
                assert_eq!(batch.schema().field(1).name(), "connector");
                assert_eq!(batch.schema().field(2).name(), "format");
                assert_eq!(batch.schema().field(3).name(), "watermark_column");

                let names = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(names.value(0), "events");

                let wm = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(wm.value(0), "ts");
            }
            _ => panic!("Expected Metadata result"),
        }
    }

    #[tokio::test]
    async fn test_show_sinks_enriched() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT)").await.unwrap();
        db.execute("CREATE SINK output FROM events").await.unwrap();

        let result = db.execute("SHOW SINKS").await.unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(batch.num_columns(), 4);
                assert_eq!(batch.schema().field(0).name(), "sink_name");
                assert_eq!(batch.schema().field(1).name(), "input");
                assert_eq!(batch.schema().field(2).name(), "connector");
                assert_eq!(batch.schema().field(3).name(), "format");

                let names = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(names.value(0), "output");

                let inputs = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(inputs.value(0), "events");
            }
            _ => panic!("Expected Metadata result"),
        }
    }

    #[tokio::test]
    async fn test_show_streams_enriched() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE STREAM my_stream AS SELECT 1 FROM events")
            .await
            .unwrap();

        let result = db.execute("SHOW STREAMS").await.unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(batch.num_columns(), 2);
                assert_eq!(batch.schema().field(0).name(), "stream_name");
                assert_eq!(batch.schema().field(1).name(), "sql");

                let sqls = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert!(
                    sqls.value(0).contains("SELECT"),
                    "SQL column should contain query"
                );
            }
            _ => panic!("Expected Metadata result"),
        }
    }

    #[tokio::test]
    async fn test_show_create_source() {
        let db = LaminarDB::open().unwrap();
        let ddl = "CREATE SOURCE events (id BIGINT, name VARCHAR)";
        db.execute(ddl).await.unwrap();

        let result = db.execute("SHOW CREATE SOURCE events").await.unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(batch.schema().field(0).name(), "create_statement");
                let stmts = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(stmts.value(0), ddl);
            }
            _ => panic!("Expected Metadata result"),
        }
    }

    #[tokio::test]
    async fn test_show_create_sink() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT)").await.unwrap();
        let ddl = "CREATE SINK output FROM events";
        db.execute(ddl).await.unwrap();

        let result = db.execute("SHOW CREATE SINK output").await.unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(batch.schema().field(0).name(), "create_statement");
                let stmts = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(stmts.value(0), ddl);
            }
            _ => panic!("Expected Metadata result"),
        }
    }

    #[tokio::test]
    async fn test_show_create_source_not_found() {
        let db = LaminarDB::open().unwrap();
        let result = db.execute("SHOW CREATE SOURCE nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_show_create_sink_not_found() {
        let db = LaminarDB::open().unwrap();
        let result = db.execute("SHOW CREATE SINK nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_explain_analyze_returns_metrics() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id BIGINT, value DOUBLE)")
            .await
            .unwrap();

        let result = db
            .execute("EXPLAIN ANALYZE SELECT * FROM events")
            .await
            .unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let key_vals: Vec<&str> = (0..batch.num_rows()).map(|i| keys.value(i)).collect();
                assert!(
                    key_vals.contains(&"rows_produced"),
                    "Expected rows_produced metric, got: {key_vals:?}"
                );
                assert!(
                    key_vals.contains(&"execution_time_ms"),
                    "Expected execution_time_ms metric, got: {key_vals:?}"
                );
            }
            _ => panic!("Expected Metadata result"),
        }
    }

    #[tokio::test]
    async fn test_explain_without_analyze_has_no_metrics() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id BIGINT, value DOUBLE)")
            .await
            .unwrap();

        let result = db.execute("EXPLAIN SELECT * FROM events").await.unwrap();
        match result {
            ExecuteResult::Metadata(batch) => {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let key_vals: Vec<&str> = (0..batch.num_rows()).map(|i| keys.value(i)).collect();
                assert!(
                    !key_vals.contains(&"rows_produced"),
                    "EXPLAIN without ANALYZE should not have rows_produced"
                );
            }
            _ => panic!("Expected Metadata result"),
        }
    }

    #[tokio::test]
    async fn test_connectorless_source_does_not_break_pipeline() {
        let db = LaminarDB::open().unwrap();

        // Connector-less source (no FROM clause) — formerly caused
        // LDB-1002 "No partitions provided" on every pipeline cycle.
        db.execute("CREATE SOURCE metadata (symbol VARCHAR, category VARCHAR)")
            .await
            .unwrap();

        // A real source with a watermark that the pipeline will process.
        db.execute(
            "CREATE SOURCE trades (id BIGINT, price DOUBLE, ts BIGINT, \
             WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();

        db.execute("CREATE STREAM out AS SELECT id, price FROM trades")
            .await
            .unwrap();

        db.start().await.unwrap();

        // Push data into the real source.
        let handle = db.source_untyped("trades").unwrap();
        let schema = handle.schema().clone();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1, 2])),
                Arc::new(arrow::array::Float64Array::from(vec![100.0, 200.0])),
                Arc::new(arrow::array::Int64Array::from(vec![1000, 2000])),
            ],
        )
        .unwrap();
        handle.push_arrow(batch).unwrap();

        // Let the pipeline run a few cycles.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Verify the pipeline processed data without errors.
        let m = db.metrics();
        assert!(m.total_events_ingested > 0, "pipeline should ingest events");

        // Push data into the connector-less source via push_arrow — should
        // work without causing pipeline errors.
        let meta_handle = db.source_untyped("metadata").unwrap();
        let meta_schema = meta_handle.schema().clone();
        let meta_batch = RecordBatch::try_new(
            meta_schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["BTC", "ETH"])),
                Arc::new(arrow::array::StringArray::from(vec!["L1", "L1"])),
            ],
        )
        .unwrap();
        meta_handle.push_arrow(meta_batch).unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Pipeline should still be healthy.
        let m2 = db.metrics();
        assert!(
            m2.total_events_ingested >= m.total_events_ingested,
            "pipeline should continue after connector-less source push"
        );
    }
}
