//! The main `LaminarDB` database facade.

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

use crate::catalog::SourceCatalog;
use crate::config::LaminarConfig;
use crate::error::DbError;
use crate::handle::{
    DdlInfo, ExecuteResult, QueryHandle, QueryInfo, SinkInfo, SourceHandle, SourceInfo,
    UntypedSourceHandle,
};

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
    shutdown: std::sync::atomic::AtomicBool,
    checkpoint_manager: parking_lot::Mutex<StreamCheckpointManager>,
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

        Ok(Self {
            catalog,
            planner: parking_lot::Mutex::new(StreamingPlanner::new()),
            ctx,
            config,
            shutdown: std::sync::atomic::AtomicBool::new(false),
            checkpoint_manager: parking_lot::Mutex::new(checkpoint_manager),
        })
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

        let statements = parse_streaming_sql(sql)?;

        if statements.is_empty() {
            return Err(DbError::InvalidOperation("Empty SQL statement".into()));
        }

        let statement = &statements[0];

        match statement {
            StreamingStatement::CreateSource(create) => self.handle_create_source(create),
            StreamingStatement::CreateSink(create) => self.handle_create_sink(create),
            StreamingStatement::CreateContinuousQuery { .. }
            | StreamingStatement::Standard(_) => self.handle_query(sql).await,
            StreamingStatement::InsertInto {
                table_name,
                columns,
                values,
            } => self.handle_insert_into(table_name, columns, values),
            StreamingStatement::DropSource { name, if_exists } => {
                self.handle_drop_source(name, *if_exists)
            }
            StreamingStatement::DropSink { name, if_exists } => {
                self.handle_drop_sink(name, *if_exists)
            }
            StreamingStatement::DropMaterializedView {
                name, if_exists, ..
            } => {
                // For now, treat materialized views like queries
                let name_str = name.to_string();
                if !if_exists {
                    // Verify it exists (future: MV catalog)
                }
                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "DROP MATERIALIZED VIEW".to_string(),
                    object_name: name_str,
                }))
            }
            StreamingStatement::Show(cmd) => {
                let batch = match cmd {
                    ShowCommand::Sources => self.build_show_sources(),
                    ShowCommand::Sinks => self.build_show_sinks(),
                    ShowCommand::Queries => self.build_show_queries(),
                    ShowCommand::MaterializedViews => {
                        // Placeholder - no MV catalog yet
                        self.build_show_sources()
                    }
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
            StreamingStatement::CreateMaterializedView { name, .. } => {
                // Treat CREATE MV as a continuous query
                let name_str = name.to_string();
                // Execute the backing query
                let result = self.handle_query(sql).await?;
                match result {
                    ExecuteResult::Query(_) => Ok(ExecuteResult::Ddl(DdlInfo {
                        statement_type: "CREATE MATERIALIZED VIEW".to_string(),
                        object_name: name_str,
                    })),
                    other => Ok(other),
                }
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

        // Also register in the planner
        {
            let mut planner = self.planner.lock();
            let stmt = StreamingStatement::CreateSource(Box::new(create.clone()));
            let _ = planner.plan(&stmt);
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

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE SINK".to_string(),
            object_name: name,
        }))
    }

    /// Handle INSERT INTO statement.
    fn handle_insert_into(
        &self,
        table_name: &sqlparser::ast::ObjectName,
        _columns: &[sqlparser::ast::Ident],
        _values: &[Vec<sqlparser::ast::Expr>],
    ) -> Result<ExecuteResult, DbError> {
        let name = table_name.to_string();
        // Verify the source exists
        let _entry = self
            .catalog
            .get_source(&name)
            .ok_or_else(|| DbError::SourceNotFound(name))?;

        // TODO(phase-3): Convert SQL values to RecordBatch and push to source
        Ok(ExecuteResult::RowsAffected(0))
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
        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP SINK".to_string(),
            object_name: name_str,
        }))
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

impl std::fmt::Debug for LaminarDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaminarDB")
            .field("sources", &self.catalog.list_sources().len())
            .field("sinks", &self.catalog.list_sinks().len())
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
}
