//! DDL (Data Definition Language) handlers for `LaminarDB`.
//!
//! Reopens `impl LaminarDB` to keep the main `db.rs` focused on dispatch.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use laminar_sql::parser::StreamingStatement;
use laminar_sql::translator::streaming_ddl;

use crate::db::{parse_duration_str, streaming_statement_to_sql, LaminarDB};
use crate::error::DbError;
use crate::handle::{DdlInfo, ExecuteResult};

impl LaminarDB {
    /// Handle CREATE SOURCE statement.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn handle_create_source(
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

        // Mark processing-time sources
        if let Some(ref wm) = source_def.watermark {
            if wm.is_processing_time {
                if let Some(ref entry) = entry {
                    entry
                        .is_processing_time
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }

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
            if let Err(e) = self.ctx.register_table(name, Arc::new(provider)) {
                tracing::warn!(table = %name, error = %e, "failed to register source table in DataFusion");
            }
        }

        // Register as a base table in the MV registry for dependency tracking
        self.mv_registry.lock().register_base_table(name);

        // Also register in the planner
        {
            let mut planner = self.planner.lock();
            let stmt = StreamingStatement::CreateSource(Box::new(create.clone()));
            if let Err(e) = planner.plan(&stmt) {
                tracing::warn!(source = %name, error = %e, "failed to register source in planner");
            }
        }

        // Register connector info in ConnectorManager if external connector specified.
        // Supports two syntax forms:
        //   1. FROM KAFKA ('topic' = 'events', ...) — connector_type is set
        //   2. WITH ('connector' = 'kafka', 'topic' = 'events', ...) — extract from with_options
        let (
            resolved_connector_type,
            resolved_connector_options,
            resolved_format,
            resolved_format_options,
        ) = if create.connector_type.is_some() {
            (
                create.connector_type.clone(),
                create.connector_options.clone(),
                create.format.as_ref().map(|f| f.format_type.clone()),
                create
                    .format
                    .as_ref()
                    .map(|f| f.options.clone())
                    .unwrap_or_default(),
            )
        } else if let Some(ct) = create.with_options.get("connector") {
            // WITH-syntax: extract connector type and options from with_options
            let (conn_opts, fmt, fmt_opts) =
                extract_connector_from_with_options(&create.with_options);
            (Some(ct.to_uppercase()), conn_opts, fmt, fmt_opts)
        } else {
            (None, HashMap::new(), None, HashMap::new())
        };

        if let Some(ref ct) = resolved_connector_type {
            let normalized = ct.to_lowercase();
            if self.connector_registry.source_info(&normalized).is_none() {
                return Err(DbError::Connector(format!(
                    "Unknown source connector type '{ct}'. Available: {:?}",
                    self.connector_registry.list_sources()
                )));
            }

            // Validate format
            if let Some(ref fmt_str) = resolved_format {
                laminar_connectors::serde::Format::parse(&fmt_str.to_lowercase())
                    .map_err(|e| DbError::Connector(format!("Unknown format '{fmt_str}': {e}")))?;
            }

            let mut mgr = self.connector_manager.lock();
            mgr.register_source(crate::connector_manager::SourceRegistration {
                name: name.clone(),
                connector_type: Some(ct.clone()),
                connector_options: resolved_connector_options,
                format: resolved_format,
                format_options: resolved_format_options,
            });
        }

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE SOURCE".to_string(),
            object_name: name.clone(),
        }))
    }

    pub(crate) fn handle_create_sink(
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
            if let Err(e) = planner.plan(&stmt) {
                tracing::warn!(sink = %name, error = %e, "failed to register sink in planner");
            }
        }

        // Register connector info in ConnectorManager if external connector specified.
        // Supports two syntax forms:
        //   1. INTO KAFKA ('topic' = 'events', ...) — connector_type is set
        //   2. WITH ('connector' = 'kafka', 'topic' = 'events', ...) — extract from with_options
        let (
            resolved_connector_type,
            resolved_connector_options,
            resolved_format,
            resolved_format_options,
        ) = if create.connector_type.is_some() {
            (
                create.connector_type.clone(),
                create.connector_options.clone(),
                create.format.as_ref().map(|f| f.format_type.clone()),
                create
                    .format
                    .as_ref()
                    .map(|f| f.options.clone())
                    .unwrap_or_default(),
            )
        } else if let Some(ct) = create.with_options.get("connector") {
            let (conn_opts, fmt, fmt_opts) =
                extract_connector_from_with_options(&create.with_options);
            (Some(ct.to_uppercase()), conn_opts, fmt, fmt_opts)
        } else {
            (None, HashMap::new(), None, HashMap::new())
        };

        if let Some(ref ct) = resolved_connector_type {
            let normalized = ct.to_lowercase();
            if self.connector_registry.sink_info(&normalized).is_none() {
                return Err(DbError::Connector(format!(
                    "Unknown sink connector type '{ct}'. Available: {:?}",
                    self.connector_registry.list_sinks()
                )));
            }

            // Validate format
            if let Some(ref fmt_str) = resolved_format {
                laminar_connectors::serde::Format::parse(&fmt_str.to_lowercase())
                    .map_err(|e| DbError::Connector(format!("Unknown format '{fmt_str}': {e}")))?;
            }

            let mut mgr = self.connector_manager.lock();
            mgr.register_sink(crate::connector_manager::SinkRegistration {
                name: name.clone(),
                input: input.clone(),
                connector_type: Some(ct.clone()),
                connector_options: resolved_connector_options,
                format: resolved_format,
                format_options: resolved_format_options,
                filter_expr: create.filter.as_ref().map(std::string::ToString::to_string),
            });
        }

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE SINK".to_string(),
            object_name: name,
        }))
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn handle_create_table(
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
                    col.name.value.clone(),
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
                if matches!(
                    opt.option,
                    sqlparser::ast::ColumnOption::Unique {
                        is_primary: true,
                        ..
                    }
                ) {
                    primary_key = Some(col.name.value.clone());
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
                if let sqlparser::ast::TableConstraint::PrimaryKey { columns, .. } = constraint {
                    if let Some(first) = columns.first() {
                        primary_key = match &first.column.expr {
                            sqlparser::ast::Expr::Identifier(ident) => Some(ident.value.clone()),
                            other => Some(other.to_string()),
                        };
                    }
                }
            }
        }

        // Extract connector options from WITH (...)
        let mut connector_type: Option<String> = None;
        let mut connector_options: HashMap<String, String> = HashMap::with_capacity(8);
        let mut format: Option<String> = None;
        let mut format_options: HashMap<String, String> = HashMap::with_capacity(4);
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
                return Err(DbError::InvalidOperation(
                    "storage = 'persistent' is no longer supported; use in-memory tables with foyer caching instead".to_string(),
                ));
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

    pub(crate) fn handle_drop_source(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
        cascade: bool,
    ) -> Result<ExecuteResult, DbError> {
        let name_str = name.to_string();

        // Check for dependents: streams and MVs that reference this source
        if cascade {
            self.cascade_drop_dependents(&name_str);
        } else {
            let dependents = self.find_dependents(&name_str);
            if !dependents.is_empty() {
                return Err(DbError::InvalidOperation(format!(
                    "Cannot drop source '{name_str}': depended on by {}. \
                     Use CASCADE to drop dependents.",
                    dependents.join(", ")
                )));
            }
        }

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

    pub(crate) fn handle_alter_source(
        &self,
        name: &sqlparser::ast::ObjectName,
        operation: &laminar_sql::parser::AlterSourceOperation,
    ) -> Result<ExecuteResult, DbError> {
        use laminar_sql::parser::AlterSourceOperation;
        let name_str = name.to_string();

        // Verify source exists
        let existing_schema = self
            .catalog
            .describe_source(&name_str)
            .ok_or_else(|| DbError::SourceNotFound(name_str.clone()))?;

        match operation {
            AlterSourceOperation::AddColumn { column_def } => {
                let col_name = column_def.name.value.clone();
                let arrow_type = laminar_sql::translator::streaming_ddl::sql_type_to_arrow(
                    &column_def.data_type,
                )
                .map_err(|e| DbError::Sql(laminar_sql::Error::ParseError(e)))?;

                // Build new schema with the added column
                let mut fields: Vec<arrow::datatypes::FieldRef> =
                    existing_schema.fields().iter().cloned().collect();
                fields.push(Arc::new(Field::new(&col_name, arrow_type, true)));
                let new_schema = Arc::new(arrow::datatypes::Schema::new(fields));

                // Re-register the source with the new schema
                let entry = self.catalog.get_source(&name_str);
                let (wm_col, max_ooo) = entry.as_ref().map_or((None, None), |e| {
                    (e.watermark_column.clone(), e.max_out_of_orderness)
                });

                self.catalog.register_source_or_replace(
                    &name_str,
                    new_schema.clone(),
                    wm_col,
                    max_ooo,
                    None,
                    None,
                );

                // Update DataFusion registration
                let _ = self.ctx.deregister_table(&name_str);
                let provider = datafusion::datasource::MemTable::try_new(new_schema, vec![vec![]])
                    .map_err(|e| {
                        DbError::InvalidOperation(format!("Failed to re-register table: {e}"))
                    })?;
                self.ctx
                    .register_table(&name_str, Arc::new(provider))
                    .map_err(|e| {
                        DbError::InvalidOperation(format!("Failed to re-register table: {e}"))
                    })?;

                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "ALTER SOURCE".to_string(),
                    object_name: name_str,
                }))
            }
            AlterSourceOperation::SetProperties { properties } => {
                // Store properties in session config for this source
                let mut props = self.session_properties.lock();
                for (key, value) in properties {
                    props.insert(format!("{name_str}.{key}"), value.clone());
                }
                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "ALTER SOURCE".to_string(),
                    object_name: name_str,
                }))
            }
        }
    }

    pub(crate) fn handle_drop_sink(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
        _cascade: bool,
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

    pub(crate) fn handle_create_stream(
        &self,
        name: &sqlparser::ast::ObjectName,
        query: &StreamingStatement,
        emit_clause: Option<&laminar_sql::parser::EmitClause>,
    ) -> Result<ExecuteResult, DbError> {
        let name_str = name.to_string();

        // Register in catalog as a stream
        self.catalog.register_stream(&name_str)?;

        // Plan the statement to extract emit_clause, window_config, and order_config
        let (plan_emit, plan_window, plan_order) = {
            let mut planner = self.planner.lock();
            let stmt = StreamingStatement::CreateStream {
                name: name.clone(),
                query: Box::new(query.clone()),
                emit_clause: emit_clause.cloned(),
                or_replace: false,
                if_not_exists: false,
            };
            match planner.plan(&stmt) {
                Ok(laminar_sql::planner::StreamingPlan::Query(ref qp)) => (
                    qp.emit_clause.clone(),
                    qp.window_config.clone(),
                    qp.order_config.clone(),
                ),
                _ => (emit_clause.cloned(), None, None),
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
                order_config: plan_order,
            });
        }

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE STREAM".to_string(),
            object_name: name_str,
        }))
    }

    pub(crate) fn handle_drop_stream(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
        cascade: bool,
    ) -> Result<ExecuteResult, DbError> {
        let name_str = name.to_string();

        // Check for dependents: MVs that reference this stream
        if cascade {
            self.cascade_drop_dependents(&name_str);
        } else {
            let dependents = self.find_dependents(&name_str);
            if !dependents.is_empty() {
                return Err(DbError::InvalidOperation(format!(
                    "Cannot drop stream '{name_str}': depended on by {}. \
                     Use CASCADE to drop dependents.",
                    dependents.join(", ")
                )));
            }
        }

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

                // Intercept checkpoint_interval for runtime reconfiguration.
                if key == "checkpoint_interval" {
                    return self.handle_set_checkpoint_interval(&value);
                }

                self.session_properties.lock().insert(key.clone(), value);
                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "SET".to_string(),
                    object_name: key,
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
        }))
    }

    /// Find streams and MVs that depend on the given object name.
    pub(crate) fn find_dependents(&self, name: &str) -> Vec<String> {
        let mut dependents = Vec::new();

        // Check streams: scan registered streams for SQL references to `name`
        {
            let mgr = self.connector_manager.lock();
            for (stream_name, reg) in mgr.streams() {
                let refs = crate::stream_executor::extract_table_references(&reg.query_sql);
                if refs.contains(name) {
                    dependents.push(stream_name.clone());
                }
            }
        }

        // Check MVs via the dependency graph
        {
            let registry = self.mv_registry.lock();
            for dep in registry.get_dependents(name) {
                dependents.push(dep.to_string());
            }
        }

        dependents
    }

    /// Drop all streams and MVs that depend on the given object, recursively.
    pub(crate) fn cascade_drop_dependents(&self, name: &str) {
        let dependents = self.find_dependents(name);
        for dep in &dependents {
            // Try dropping as stream first
            if self.catalog.drop_stream(dep) {
                self.connector_manager.lock().unregister_stream(dep);
            }
            // Try dropping as MV (cascade further)
            {
                let mut registry = self.mv_registry.lock();
                if let Ok(views) = registry.unregister_cascade(dep) {
                    drop(registry);
                    let mut mgr = self.connector_manager.lock();
                    for v in &views {
                        mgr.unregister_stream(&v.name);
                    }
                }
            }
        }
    }

    /// Handle CREATE MATERIALIZED VIEW statement.
    ///
    /// Registers the view in the MV registry with dependency tracking,
    /// then executes the backing query through `DataFusion` to obtain the
    /// output schema.
    pub(crate) async fn handle_create_materialized_view(
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

        // Discover source references via AST-based extraction (not substring matching)
        let table_refs = crate::stream_executor::extract_table_references(&query_sql);
        let catalog_sources = self.catalog.list_sources();
        let mut sources: Vec<String> = catalog_sources
            .into_iter()
            .filter(|s| table_refs.contains(s.as_str()))
            .collect();

        // Also check existing MVs as potential sources (cascading MVs)
        {
            let registry = self.mv_registry.lock();
            for view in registry.views() {
                if view.name != name_str && table_refs.contains(view.name.as_str()) {
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

        // Plan the MV's backing query to extract emit_clause, window_config, and order_config
        let (plan_emit, plan_window, plan_order) = {
            let mut planner = self.planner.lock();
            // Wrap the inner query as a CreateStream to reuse the planner
            let stmt = StreamingStatement::CreateStream {
                name: name.clone(),
                query: Box::new(query.clone()),
                emit_clause: None,
                or_replace: false,
                if_not_exists: false,
            };
            match planner.plan(&stmt) {
                Ok(laminar_sql::planner::StreamingPlan::Query(ref qp)) => (
                    qp.emit_clause.clone(),
                    qp.window_config.clone(),
                    qp.order_config.clone(),
                ),
                _ => (None, None, None),
            }
        };

        // Register the MV as a stream so start() picks it up for execution
        {
            let mut mgr = self.connector_manager.lock();
            mgr.register_stream(crate::connector_manager::StreamRegistration {
                name: name_str.clone(),
                query_sql: query_sql.clone(),
                emit_clause: plan_emit,
                window_config: plan_window,
                order_config: plan_order,
            });
        }

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE MATERIALIZED VIEW".to_string(),
            object_name: name_str,
        }))
    }

    /// Handle DROP MATERIALIZED VIEW statement.
    pub(crate) fn handle_drop_materialized_view(
        &self,
        name: &sqlparser::ast::ObjectName,
        if_exists: bool,
        cascade: bool,
    ) -> Result<ExecuteResult, DbError> {
        let name_str = name.to_string();

        // Collect names to unregister from connector manager
        let dropped_names;
        {
            let mut registry = self.mv_registry.lock();

            let result = if cascade {
                registry.unregister_cascade(&name_str)
            } else {
                registry.unregister(&name_str).map(|v| vec![v])
            };

            match result {
                Ok(views) => {
                    dropped_names = views
                        .into_iter()
                        .map(|v| v.name.clone())
                        .collect::<Vec<_>>();
                }
                Err(_) if if_exists => {
                    dropped_names = vec![];
                }
                Err(e) => return Err(DbError::MaterializedView(e.to_string())),
            }
        }

        // Remove stream registrations for dropped MVs
        {
            let mut mgr = self.connector_manager.lock();
            for dropped in &dropped_names {
                mgr.unregister_stream(dropped);
            }
        }

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "DROP MATERIALIZED VIEW".to_string(),
            object_name: name_str,
        }))
    }

    /// Handle DROP TABLE statement.
    pub(crate) fn handle_drop_table(
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

    /// Synchronize a `TableStore` table to the `DataFusion` `MemTable`.
    ///
    /// Deregisters the existing table (if any) and re-registers with the
    /// current contents of the `TableStore`.
    pub(crate) fn sync_table_to_datafusion(&self, name: &str) -> Result<(), DbError> {
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
}

const STREAMING_OPTION_KEYS: &[&str] = &[
    "connector",
    "format",
    "buffer_size",
    "buffersize",
    "backpressure",
    "wait_strategy",
    "waitstrategy",
    "track_stats",
    "trackstats",
    "stats",
    "event_time",
    "watermark_delay",
];

/// Extracts connector-specific options from a `WITH (...)` clause map.
///
/// When a source or sink is created with the `WITH ('connector' = '...', ...)`
/// syntax (as opposed to `FROM KAFKA (...)`), the `connector` key selects the
/// connector type and the `format` key selects the serialisation format.
/// All remaining entries that are **not** known streaming-engine options are
/// forwarded as connector options.
///
/// Returns `(connector_options, format, format_options)`.
pub(crate) fn extract_connector_from_with_options(
    with_options: &HashMap<String, String>,
) -> (
    HashMap<String, String>,
    Option<String>,
    HashMap<String, String>,
) {
    let mut connector_options = HashMap::with_capacity(with_options.len());
    let mut format: Option<String> = None;
    let mut format_options = HashMap::with_capacity(4);

    for (key, value) in with_options {
        let lower = key.to_lowercase();
        if lower == "connector" {
            // Already handled by the caller.
            continue;
        }
        if lower == "format" {
            format = Some(value.clone());
            continue;
        }
        // Keys starting with "format." are format-specific sub-options.
        if let Some(suffix) = lower.strip_prefix("format.") {
            format_options.insert(suffix.to_string(), value.clone());
            continue;
        }
        if STREAMING_OPTION_KEYS.contains(&lower.as_str()) {
            continue;
        }
        connector_options.insert(key.clone(), value.clone());
    }

    (connector_options, format, format_options)
}
