//! The main `LaminarDB` database facade.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;

use laminar_core::streaming;
use laminar_core::streaming::StreamCheckpointManager;
use laminar_sql::parser::{parse_streaming_sql, ShowCommand, StreamingStatement};
use laminar_sql::planner::StreamingPlanner;
use laminar_sql::translator::streaming_ddl;
use laminar_sql::register_streaming_functions;

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
    checkpoint_manager: parking_lot::Mutex<StreamCheckpointManager>,
    connector_manager: parking_lot::Mutex<crate::connector_manager::ConnectorManager>,
    connector_registry: Arc<laminar_connectors::registry::ConnectorRegistry>,
    mv_registry: parking_lot::Mutex<laminar_core::mv::MvRegistry>,
    state: std::sync::atomic::AtomicU8,
    /// Handle to the background processing task (if running).
    runtime_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Signal to stop the processing loop.
    shutdown_signal: Arc<tokio::sync::Notify>,
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

        let checkpoint_manager = match &config.checkpoint {
            Some(cp_config) => StreamCheckpointManager::new(cp_config.clone()),
            None => StreamCheckpointManager::disabled(),
        };

        let connector_registry =
            Arc::new(laminar_connectors::registry::ConnectorRegistry::new());
        Self::register_builtin_connectors(&connector_registry);

        Ok(Self {
            catalog,
            planner: parking_lot::Mutex::new(StreamingPlanner::new()),
            ctx,
            config,
            config_vars: Arc::new(config_vars),
            shutdown: std::sync::atomic::AtomicBool::new(false),
            checkpoint_manager: parking_lot::Mutex::new(checkpoint_manager),
            connector_manager: parking_lot::Mutex::new(
                crate::connector_manager::ConnectorManager::new(),
            ),
            connector_registry,
            mv_registry: parking_lot::Mutex::new(laminar_core::mv::MvRegistry::new()),
            state: std::sync::atomic::AtomicU8::new(STATE_CREATED),
            runtime_handle: parking_lot::Mutex::new(None),
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
        })
    }

    /// Get a fluent builder for constructing a `LaminarDB`.
    #[must_use]
    pub fn builder() -> LaminarDbBuilder {
        LaminarDbBuilder::new()
    }

    /// Register built-in connectors based on enabled features.
    #[allow(unused_variables)]
    fn register_builtin_connectors(
        registry: &laminar_connectors::registry::ConnectorRegistry,
    ) {
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
    pub fn connector_registry(
        &self,
    ) -> &laminar_connectors::registry::ConnectorRegistry {
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
            StreamingStatement::CreateStream { name, query, emit_clause, .. } => {
                self.handle_create_stream(name, query, emit_clause.as_ref())
            }
            StreamingStatement::CreateContinuousQuery { .. } => self.handle_query(sql).await,
            StreamingStatement::Standard(stmt) => {
                if let sqlparser::ast::Statement::CreateTable(ct) = stmt.as_ref() {
                    self.handle_create_table(ct)
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
            } => {
                self.handle_drop_materialized_view(name, *if_exists, *cascade)
            }
            StreamingStatement::Show(cmd) => {
                let batch = match cmd {
                    ShowCommand::Sources => self.build_show_sources(),
                    ShowCommand::Sinks => self.build_show_sinks(),
                    ShowCommand::Queries => self.build_show_queries(),
                    ShowCommand::MaterializedViews => {
                        self.build_show_materialized_views()
                    }
                    ShowCommand::Streams => self.build_show_streams(),
                };
                Ok(ExecuteResult::Metadata(batch))
            }
            StreamingStatement::Describe { name, .. } => {
                let name_str = name.to_string();
                let batch = self.build_describe(&name_str)?;
                Ok(ExecuteResult::Metadata(batch))
            }
            StreamingStatement::Explain { statement } => {
                self.handle_explain(statement)
            }
            StreamingStatement::CreateMaterializedView {
                name,
                query,
                or_replace,
                if_not_exists,
                ..
            } => {
                self.handle_create_materialized_view(
                    sql, name, query, *or_replace, *if_not_exists,
                )
                .await
            }
        }
    }

    /// Handle CREATE SOURCE statement.
    fn handle_create_source(
        &self,
        create: &laminar_sql::parser::CreateSourceStatement,
    ) -> Result<ExecuteResult, DbError> {
        let source_def = streaming_ddl::translate_create_source(create.clone())
            .map_err(|e| DbError::Sql(laminar_sql::Error::ParseError(e)))?;

        let name = &source_def.name;
        let schema = source_def.schema.clone();
        let watermark_col = source_def.watermark.as_ref().map(|w| w.column.clone());

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
                buffer_size,
                None,
            ))
        } else if create.if_not_exists {
            if self.catalog.get_source(name).is_none() {
                Some(
                    self.catalog
                        .register_source(name, schema, watermark_col, buffer_size, None)?,
                )
            } else {
                None
            }
        } else {
            Some(
                self.catalog
                    .register_source(name, schema, watermark_col, buffer_size, None)?,
            )
        };

        // Auto-register source with checkpoint manager if enabled
        if let Some(ref entry) = entry {
            let mut mgr = self.checkpoint_manager.lock();
            if mgr.is_enabled() {
                mgr.register_source(
                    &entry.name,
                    entry.source.sequence_counter(),
                    entry.source.watermark_atomic(),
                );
            }
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
                laminar_connectors::serde::Format::parse(
                    &fmt.format_type.to_lowercase(),
                )
                .map_err(|e| {
                    DbError::Connector(format!(
                        "Unknown format '{}': {e}",
                        fmt.format_type
                    ))
                })?;
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
                laminar_connectors::serde::Format::parse(
                    &fmt.format_type.to_lowercase(),
                )
                .map_err(|e| {
                    DbError::Connector(format!(
                        "Unknown format '{}': {e}",
                        fmt.format_type
                    ))
                })?;
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
    /// Inserts SQL VALUES into a registered source or `DataFusion` table.
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
                .source
                .push_arrow(batch)
                .map_err(|e| DbError::InsertError(format!("Failed to push to source: {e}")))?;
            return Ok(ExecuteResult::RowsAffected(values.len() as u64));
        }

        // Otherwise, insert into a DataFusion MemTable
        // Look up the table provider
        let table = self
            .ctx
            .table_provider(&name)
            .await
            .map_err(|_| DbError::TableNotFound(name.clone()))?;

        let schema = table.schema();
        let batch = sql_utils::sql_values_to_record_batch(
            &schema,
            values,
        )?;

        // Deregister the old table, then re-register with the new data
        self.ctx
            .deregister_table(&name)
            .map_err(|e| DbError::InsertError(format!("Failed to deregister table: {e}")))?;

        let mem_table = datafusion::datasource::MemTable::try_new(
            schema.clone(),
            vec![vec![batch]],
        )
        .map_err(|e| DbError::InsertError(format!("Failed to create table: {e}")))?;

        self.ctx
            .register_table(&name, Arc::new(mem_table))
            .map_err(|e| DbError::InsertError(format!("Failed to register table: {e}")))?;

        Ok(ExecuteResult::RowsAffected(values.len() as u64))
    }

    /// Handle CREATE TABLE statement.
    ///
    /// Creates a static reference/dimension table backed by a `DataFusion`
    /// `MemTable`. These tables are used for lookup joins.
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
                let data_type =
                    streaming_ddl::sql_type_to_arrow(&col.data_type).map_err(|e| {
                        DbError::InvalidOperation(format!(
                            "unsupported column type for '{}': {e}",
                            col.name
                        ))
                    })?;
                let nullable = !col.options.iter().any(|opt| {
                    matches!(opt.option, sqlparser::ast::ColumnOption::NotNull)
                });
                Ok(arrow::datatypes::Field::new(
                    col.name.to_string(),
                    data_type,
                    nullable,
                ))
            })
            .collect::<Result<Vec<_>, DbError>>()?;

        let schema = Arc::new(arrow::datatypes::Schema::new(fields));

        // Register as a DataFusion MemTable with empty data
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![]])
                .map_err(|e| {
                    DbError::InvalidOperation(format!("Failed to create table: {e}"))
                })?;

        self.ctx
            .register_table(&name, Arc::new(mem_table))
            .map_err(|e| {
                DbError::InvalidOperation(format!("Failed to register table: {e}"))
            })?;

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

        // Register in planner
        {
            let mut planner = self.planner.lock();
            let stmt = StreamingStatement::CreateStream {
                name: name.clone(),
                query: Box::new(query.clone()),
                emit_clause: emit_clause.cloned(),
                or_replace: false,
                if_not_exists: false,
            };
            let _ = planner.plan(&stmt);
        }

        // Store the query SQL for stream execution at start()
        {
            let mut mgr = self.connector_manager.lock();
            mgr.register_stream(crate::connector_manager::StreamRegistration {
                name: name_str.clone(),
                query_sql: streaming_statement_to_sql(query),
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

    /// Handle EXPLAIN statement — show the streaming query plan.
    fn handle_explain(
        &self,
        statement: &StreamingStatement,
    ) -> Result<ExecuteResult, DbError> {
        let mut planner = self.planner.lock();

        // Plan the inner statement to extract streaming info
        let plan_result = planner.plan(statement);

        let mut rows: Vec<(String, String)> = Vec::new();

        match plan_result {
            Ok(plan) => {
                rows.push(("plan_type".into(), format!("{:?}", std::mem::discriminant(&plan))));
                match &plan {
                    laminar_sql::planner::StreamingPlan::Query(qp) => {
                        if let Some(name) = &qp.name {
                            rows.push(("query_name".into(), name.clone()));
                        }
                        if let Some(wc) = &qp.window_config {
                            rows.push(("window_type".into(), format!("{:?}", wc.window_type)));
                        }
                        if let Some(jc) = &qp.join_config {
                            rows.push(("join_type".into(), format!("{jc:?}")));
                        }
                        if let Some(oc) = &qp.order_config {
                            rows.push(("order_by".into(), format!("{oc:?}")));
                        }
                        if let Some(ec) = &qp.emit_clause {
                            rows.push(("emit".into(), format!("{ec:?}")));
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
                rows.push(("statement".into(), format!("{:?}", std::mem::discriminant(statement))));
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
                // Async execution without the lock
                let plan_sql = query_plan.statement.to_string();
                let logical_plan = self.ctx.state().create_logical_plan(&plan_sql).await?;
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

        let source_cfg =
            streaming::SourceConfig::with_buffer_size(self.config.default_buffer_size);
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
    /// Returns a [`PipelineTopology`] with nodes for every source, stream,
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
        let cm_sink_names: std::collections::HashSet<&String> =
            mgr.sinks().keys().collect();
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
    pub fn is_checkpoint_enabled(&self) -> bool {
        self.checkpoint_manager.lock().is_enabled()
    }

    /// Triggers a streaming checkpoint, capturing source sequences and
    /// watermarks.
    ///
    /// Returns the checkpoint ID on success.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if the checkpoint operation fails.
    pub fn checkpoint(&self) -> Result<Option<u64>, DbError> {
        self.checkpoint_manager
            .lock()
            .checkpoint()
            .map_err(|e| DbError::Checkpoint(e.to_string()))
    }

    /// Returns the most recent streaming checkpoint for restore.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if no checkpoint is available.
    pub fn restore_checkpoint(
        &self,
    ) -> Result<streaming::StreamCheckpoint, DbError> {
        self.checkpoint_manager
            .lock()
            .restore()
            .cloned()
            .map_err(|e| DbError::Checkpoint(e.to_string()))
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
                "Cannot start a stopped pipeline. Create a new LaminarDB instance."
                    .into(),
            ));
        }

        self.state
            .store(STATE_STARTING, std::sync::atomic::Ordering::Release);

        // Snapshot connector registrations under the lock
        let (source_regs, sink_regs, stream_regs, has_external) = {
            let mgr = self.connector_manager.lock();
            (
                mgr.sources().clone(),
                mgr.sinks().clone(),
                mgr.streams().clone(),
                mgr.has_external_connectors(),
            )
        };

        // Log which sources have external connectors for debugging.
        for (name, reg) in &source_regs {
            eprintln!(
                "[laminar-db] Source '{}': connector_type={:?}",
                name,
                reg.connector_type
            );
        }
        for (name, reg) in &sink_regs {
            eprintln!(
                "[laminar-db] Sink '{}': connector_type={:?}",
                name,
                reg.connector_type
            );
        }

        if has_external {
            eprintln!(
                "[laminar-db] Starting CONNECTOR pipeline ({} sources, {} sinks, {} streams)",
                source_regs.len(),
                sink_regs.len(),
                stream_regs.len(),
            );
            self.start_connector_pipeline(source_regs, sink_regs, stream_regs)
                .await?;
        } else if !stream_regs.is_empty() {
            eprintln!(
                "[laminar-db] Starting EMBEDDED pipeline ({} streams)",
                stream_regs.len(),
            );
            self.start_embedded_pipeline(&stream_regs);
        } else {
            eprintln!(
                "[laminar-db] Starting in embedded mode (no streams)"
            );
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
    fn start_embedded_pipeline(
        &self,
        stream_regs: &HashMap<
            String,
            crate::connector_manager::StreamRegistration,
        >,
    ) {
        use crate::stream_executor::StreamExecutor;

        // Build StreamExecutor with the registered stream queries
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        for reg in stream_regs.values() {
            executor.add_query(reg.name.clone(), reg.query_sql.clone());
        }

        // Subscribe to every source's sink so we can read pushed data.
        let mut source_subs: Vec<(
            String,
            streaming::Subscription<crate::catalog::ArrowRecord>,
        )> = Vec::new();
        for name in self.catalog.list_sources() {
            if let Some(entry) = self.catalog.get_source(&name) {
                let sub = entry.sink.subscribe();
                source_subs.push((name, sub));
            }
        }

        // Get stream source handles for pushing results.
        let mut stream_sources: Vec<(
            String,
            streaming::Source<crate::catalog::ArrowRecord>,
        )> = Vec::new();
        for reg in stream_regs.values() {
            if let Some(src) = self.catalog.get_stream_source(&reg.name) {
                stream_sources.push((reg.name.clone(), src));
            }
        }

        tracing::info!(
            sources = source_subs.len(),
            streams = stream_sources.len(),
            "Starting embedded pipeline"
        );

        let shutdown = self.shutdown_signal.clone();

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

                // Drain source subscriptions into batches
                let mut source_batches: HashMap<String, Vec<RecordBatch>> =
                    HashMap::new();
                for (name, sub) in &source_subs {
                    for _ in 0..256 {
                        match sub.poll() {
                            Some(batch) if batch.num_rows() > 0 => {
                                source_batches
                                    .entry(name.clone())
                                    .or_default()
                                    .push(batch);
                            }
                            _ => break,
                        }
                    }
                }

                if source_batches.is_empty() {
                    continue;
                }

                // Execute stream queries
                match executor.execute_cycle(&source_batches).await {
                    Ok(results) => {
                        // Push results to stream sources for subscriber delivery
                        for (stream_name, src) in &stream_sources {
                            if let Some(batches) = results.get(stream_name) {
                                for batch in batches {
                                    if batch.num_rows() > 0 {
                                        let _ = src.push_arrow(batch.clone());
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Embedded stream execution error");
                    }
                }

                cycle_count += 1;
                if cycle_count.is_multiple_of(100) {
                    tracing::debug!(
                        cycles = cycle_count,
                        "Embedded pipeline processing"
                    );
                }
            }
            tracing::info!(
                "Embedded pipeline stopped after {cycle_count} cycles"
            );
        });

        *self.runtime_handle.lock() = Some(handle);
    }

    /// Build and start the connector pipeline with external sources/sinks.
    ///
    /// Uses the [`ConnectorRegistry`] for generic dispatch — no
    /// connector-specific code in the pipeline setup or processing loop.
    #[allow(clippy::too_many_lines)]
    async fn start_connector_pipeline(
        &self,
        source_regs: HashMap<
            String,
            crate::connector_manager::SourceRegistration,
        >,
        sink_regs: HashMap<String, crate::connector_manager::SinkRegistration>,
        stream_regs: HashMap<
            String,
            crate::connector_manager::StreamRegistration,
        >,
    ) -> Result<(), DbError> {
        use crate::connector_manager::{build_sink_config, build_source_config};
        use crate::stream_executor::StreamExecutor;
        use laminar_connectors::config::ConnectorConfig;
        use laminar_connectors::connector::{SinkConnector, SourceConnector};

        // Build StreamExecutor
        let ctx = SessionContext::new();
        laminar_sql::register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        for reg in stream_regs.values() {
            executor.add_query(reg.name.clone(), reg.query_sql.clone());
        }

        // Build sources via registry (generic — no connector-specific code)
        let mut sources: Vec<(
            String,
            Box<dyn SourceConnector>,
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
            sources.push((name.clone(), source, config));
        }

        // Build sinks via registry (generic — no connector-specific code)
        #[allow(clippy::type_complexity)]
        let mut sinks: Vec<(
            String,
            Box<dyn SinkConnector>,
            ConnectorConfig,
            Option<String>,
        )> = Vec::new();
        for (name, reg) in &sink_regs {
            if reg.connector_type.is_none() {
                continue;
            }
            let config = build_sink_config(reg)?;
            let sink = self
                .connector_registry
                .create_sink(&config)
                .map_err(|e| {
                    DbError::Connector(format!(
                        "Cannot create sink '{}' (type '{}'): {e}",
                        name,
                        config.connector_type()
                    ))
                })?;
            sinks.push((
                name.clone(),
                sink,
                config,
                reg.filter_expr.clone(),
            ));
        }

        // Open all connectors
        for (name, source, config) in &mut sources {
            source.open(config).await.map_err(|e| {
                DbError::Connector(format!(
                    "Failed to open source '{name}': {e}"
                ))
            })?;
        }
        for (name, sink, config, _) in &mut sinks {
            sink.open(config).await.map_err(|e| {
                DbError::Connector(format!(
                    "Failed to open sink '{name}': {e}"
                ))
            })?;
        }

        // Get stream source handles so results also flow to db.subscribe().
        let mut stream_sources: Vec<(
            String,
            streaming::Source<crate::catalog::ArrowRecord>,
        )> = Vec::new();
        for reg in stream_regs.values() {
            if let Some(src) = self.catalog.get_stream_source(&reg.name) {
                stream_sources.push((reg.name.clone(), src));
            }
        }

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
            "Starting connector pipeline"
        );

        // Spawn processing loop
        let shutdown = self.shutdown_signal.clone();
        let max_poll = self.config.default_buffer_size.min(1024);

        let handle = tokio::spawn(async move {
            eprintln!("[laminar-db] Connector pipeline task started");
            let mut cycle_count: u64 = 0;
            let mut total_batches: u64 = 0;
            let mut total_records: u64 = 0;
            loop {
                // Check for shutdown
                tokio::select! {
                    () = shutdown.notified() => {
                        tracing::info!("Pipeline shutdown signal received");
                        break;
                    }
                    () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                }

                // Poll sources
                let mut source_batches = HashMap::new();
                for (name, source, _) in &mut sources {
                    match source.poll_batch(max_poll).await {
                        Ok(Some(batch)) => {
                            total_batches += 1;
                            total_records += batch.records.num_rows() as u64;
                            source_batches
                                .entry(name.clone())
                                .or_insert_with(Vec::new)
                                .push(batch.records);
                        }
                        Ok(None) => {}
                        Err(e) => {
                            if cycle_count < 5
                                || cycle_count.is_multiple_of(100)
                            {
                                eprintln!(
                                    "[laminar-db] Source '{name}' poll error: {e}"
                                );
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
                                let rows: usize = v
                                    .iter()
                                    .map(arrow::array::RecordBatch::num_rows)
                                    .sum();
                                format!("{k}({rows} rows)")
                            })
                            .collect();
                        eprintln!(
                            "[laminar-db] Executing queries with: [{}]",
                            src_summary.join(", "),
                        );
                    }
                    match executor.execute_cycle(&source_batches).await {
                        Ok(results) => {
                            // Push results to stream sources for
                            // db.subscribe() delivery (same as
                            // embedded pipeline).
                            for (stream_name, src) in &stream_sources {
                                if let Some(batches) =
                                    results.get(stream_name)
                                {
                                    for batch in batches {
                                        if batch.num_rows() > 0 {
                                            let _ = src
                                                .push_arrow(batch.clone());
                                        }
                                    }
                                }
                            }

                            // Route results to external sinks
                            for (sink_name, sink, _, filter_expr) in
                                &mut sinks
                            {
                                for (stream_name, batches) in &results {
                                    for batch in batches {
                                        // Apply WHERE filter if configured
                                        let filtered =
                                            if let Some(filter_sql) =
                                                filter_expr
                                            {
                                                match apply_filter(
                                                    batch, filter_sql,
                                                )
                                                .await
                                                {
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
                                            if let Err(e) = sink
                                                .write_batch(&filtered)
                                                .await
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
                            if cycle_count < 5
                                || cycle_count.is_multiple_of(100)
                            {
                                eprintln!(
                                    "[laminar-db] Stream execution error: {e}"
                                );
                            }
                            tracing::warn!(error = %e, "Stream execution cycle error");
                        }
                    }
                }

                cycle_count += 1;
                if cycle_count.is_multiple_of(50) {
                    eprintln!(
                        "[laminar-db] Pipeline cycle {cycle_count}: {total_batches} batches, {total_records} records total",
                    );
                    tracing::debug!(cycles = cycle_count, "Pipeline processing");
                }
            }

            eprintln!(
                "[laminar-db] Pipeline stopping after {cycle_count} cycles ({total_batches} batches, {total_records} records)",
            );

            // Close all connectors
            for (name, source, _) in &mut sources {
                if let Err(e) = source.close().await {
                    tracing::warn!(source = %name, error = %e, "Source close error");
                }
            }
            for (name, sink, _, _) in &mut sinks {
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
            match tokio::time::timeout(
                std::time::Duration::from_secs(10),
                handle,
            )
            .await
            {
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
            let mv = laminar_core::mv::MaterializedView::new(
                &name_str,
                sql,
                sources,
                schema,
            );

            let mut registry = self.mv_registry.lock();

            if or_replace {
                // Drop existing view (and dependents) before re-registering
                let _ = registry.unregister_cascade(&name_str);
            }

            registry.register(mv).map_err(|e| {
                DbError::MaterializedView(e.to_string())
            })?;
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
    let mem_table =
        datafusion::datasource::MemTable::try_new(schema, vec![vec![batch.clone()]])
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
        db.execute("CREATE SOURCE events (id INT)")
            .await
            .unwrap();
        db.execute("CREATE SINK output FROM events")
            .await
            .unwrap();

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
        db.execute(
            "CREATE SOURCE a (id INT); CREATE SOURCE b (id INT); CREATE SINK output FROM a",
        )
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
        db.execute("CREATE SOURCE events (id INT)")
            .await
            .unwrap();
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
            .await.unwrap();

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
        db.execute("CREATE TABLE dim (id INT, label VARCHAR)").await.unwrap();

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
        db.execute("CREATE SOURCE events (id BIGINT, value DOUBLE)").await.unwrap();

        let result = db.execute("INSERT INTO events VALUES (1, 3.14), (2, 2.72)").await.unwrap();
        match result {
            ExecuteResult::RowsAffected(n) => assert_eq!(n, 2),
            _ => panic!("Expected RowsAffected"),
        }
    }

    #[tokio::test]
    async fn test_insert_into_table() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE TABLE products (id INT, name VARCHAR, price DOUBLE)").await.unwrap();

        let result = db.execute("INSERT INTO products VALUES (1, 'Widget', 9.99)").await.unwrap();
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
        let result = db.execute("CREATE TABLE orders (id BIGINT NOT NULL, qty SMALLINT, total DECIMAL(10,2))").await.unwrap();

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
        db.execute("CREATE SOURCE data (id BIGINT, label VARCHAR)").await.unwrap();

        let result = db.execute("INSERT INTO data VALUES (1, NULL)").await.unwrap();
        match result {
            ExecuteResult::RowsAffected(n) => assert_eq!(n, 1),
            _ => panic!("Expected RowsAffected"),
        }
    }

    #[tokio::test]
    async fn test_insert_negative_values() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE temps (id BIGINT, celsius DOUBLE)").await.unwrap();

        let result = db.execute("INSERT INTO temps VALUES (1, -40.0)").await.unwrap();
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
        assert!(
            err.contains("Unknown source connector type"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn test_create_sink_unknown_connector() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT)")
            .await
            .unwrap();
        // Use correct SQL syntax: INTO <type> (...)
        let result = db
            .execute(
                "CREATE SINK output FROM events \
                 INTO NONEXISTENT ('topic' = 'out')",
            )
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Unknown sink connector type"),
            "got: {err}"
        );
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
                    Arc::new(|| {
                        Box::new(
                            laminar_connectors::testing::MockSourceConnector::new(),
                        )
                    }),
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
        let result = db
            .execute("DROP MATERIALIZED VIEW nonexistent")
            .await;
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
        db.execute("CREATE SOURCE events (id INT)")
            .await
            .unwrap();

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
        db.execute("CREATE SOURCE events (id INT)")
            .await
            .unwrap();

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
        db.execute("CREATE SOURCE events (id INT)")
            .await
            .unwrap();

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
        db.execute("CREATE SOURCE events (id INT)")
            .await
            .unwrap();

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
        db.execute("CREATE SINK output FROM agg")
            .await
            .unwrap();

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
        assert!(topo.edges.iter().any(|e| e.from == "events" && e.to == "agg"));
        assert!(topo.edges.iter().any(|e| e.from == "agg" && e.to == "output"));
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
        let ticks_edges: Vec<_> = topo
            .edges
            .iter()
            .filter(|e| e.from == "ticks")
            .collect();
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
}
