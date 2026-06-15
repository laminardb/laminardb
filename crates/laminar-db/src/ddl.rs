//! DDL handlers — reopens `impl LaminarDB` to keep `db.rs` focused on dispatch.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use laminar_sql::parser::StreamingStatement;
use laminar_sql::translator::streaming_ddl::{self, ColumnDefinition};

use crate::connector_manager::normalize_connector_type;
use crate::db::{parse_duration_str, LaminarDB};
use crate::error::DbError;
use crate::handle::{DdlInfo, ExecuteResult};

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

impl LaminarDB {
    /// Resolve `${VAR}` / `${VAR:-default}` in connector + format option values,
    /// from config vars then the process environment. Scoped to connector
    /// options on purpose — `${...}` elsewhere in a statement is left untouched.
    fn resolve_connector_vars(&self, resolved: &mut ResolvedConnector) -> Result<(), DbError> {
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
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) async fn handle_create_source(
        &self,
        create: &laminar_sql::parser::CreateSourceStatement,
    ) -> Result<ExecuteResult, DbError> {
        // Connectors are instantiated in start() — reject mid-run DDL.
        let has_connector =
            create.connector_type.is_some() || create.with_options.contains_key("connector");
        if has_connector && self.connector_ddl_rejected() {
            let name = &create.name;
            return Err(DbError::Pipeline(format!(
                "Cannot create source '{name}' with connector while pipeline is running. \
                 Stop the pipeline first."
            )));
        }

        let source_name = create.name.to_string();
        reject_reserved_namespace(&source_name)?;
        if create.if_not_exists && self.catalog.get_source(&source_name).is_some() {
            return Ok(ExecuteResult::Ddl(DdlInfo {
                statement_type: "CREATE SOURCE".to_string(),
                object_name: source_name,
            }));
        }

        let source_def = if create.columns.is_empty() && has_connector {
            let mut resolved = resolve_connector_info(
                create.connector_type.as_ref(),
                &create.connector_options,
                create.format.as_ref(),
                &create.with_options,
            );
            self.resolve_connector_vars(&mut resolved)?;
            let connector_type = resolved.connector_type.as_deref().ok_or_else(|| {
                DbError::Config(format!(
                    "source '{source_name}': no columns declared and no connector type resolved"
                ))
            })?;
            let normalized = normalize_connector_type(connector_type);

            // Fail on unknown connector before discovery so a typo isn't reported as a schema failure.
            if self.connector_registry.source_info(&normalized).is_none() {
                return Err(DbError::Config(format!(
                    "source '{source_name}': unknown connector type '{normalized}'"
                )));
            }

            let mut props = resolved.connector_options;
            if let Some(fmt) = resolved.format {
                props.insert("format".into(), fmt);
            }
            props.extend(resolved.format_options);

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
                .map_err(|e| DbError::Sql(laminar_sql::Error::ParseError(e)))?
        } else {
            streaming_ddl::translate_create_source(create.clone())
                .map_err(|e| DbError::Sql(laminar_sql::Error::ParseError(e)))?
        };

        let name = &source_def.name;
        let schema = source_def.schema.clone();
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

        if let Some(ref wm) = source_def.watermark {
            if wm.is_processing_time {
                if let Some(ref entry) = entry {
                    entry
                        .is_processing_time
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }

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

        self.mv_registry.lock().register_base_table(name);

        {
            let mut planner = self.planner.lock();
            let stmt = StreamingStatement::CreateSource(Box::new(create.clone()));
            if let Err(e) = planner.plan(&stmt) {
                tracing::warn!(source = %name, error = %e, "failed to register source in planner");
            }
        }

        let mut resolved = resolve_connector_info(
            create.connector_type.as_ref(),
            &create.connector_options,
            create.format.as_ref(),
            &create.with_options,
        );
        self.resolve_connector_vars(&mut resolved)?;

        if let Some(ref ct) = resolved.connector_type {
            let normalized = normalize_connector_type(ct);
            if self.connector_registry.source_info(&normalized).is_none() {
                return Err(DbError::Connector(format!(
                    "Unknown source connector type '{ct}'. Available: {:?}",
                    self.connector_registry.list_sources()
                )));
            }

            validate_format(resolved.format.as_ref())?;

            let mut mgr = self.connector_manager.lock();
            mgr.register_source(crate::connector_manager::SourceRegistration {
                name: name.clone(),
                connector_type: Some(ct.clone()),
                connector_options: resolved.connector_options,
                format: resolved.format,
                format_options: resolved.format_options,
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
        let has_connector =
            create.connector_type.is_some() || create.with_options.contains_key("connector");
        if has_connector && self.connector_ddl_rejected() {
            let name = &create.name;
            return Err(DbError::Pipeline(format!(
                "Cannot create sink '{name}' with connector while pipeline is running. \
                 Stop the pipeline first."
            )));
        }

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

        {
            let mut planner = self.planner.lock();
            let stmt = StreamingStatement::CreateSink(Box::new(create.clone()));
            if let Err(e) = planner.plan(&stmt) {
                tracing::warn!(sink = %name, error = %e, "failed to register sink in planner");
            }
        }

        let mut resolved = resolve_connector_info(
            create.connector_type.as_ref(),
            &create.connector_options,
            create.format.as_ref(),
            &create.with_options,
        );
        self.resolve_connector_vars(&mut resolved)?;

        if let Some(ref ct) = resolved.connector_type {
            let normalized = normalize_connector_type(ct);
            if self.connector_registry.sink_info(&normalized).is_none() {
                return Err(DbError::Connector(format!(
                    "Unknown sink connector type '{ct}'. Available: {:?}",
                    self.connector_registry.list_sinks()
                )));
            }

            validate_format(resolved.format.as_ref())?;

            let mut mgr = self.connector_manager.lock();
            mgr.register_sink(crate::connector_manager::SinkRegistration {
                name: name.clone(),
                input: input.clone(),
                connector_type: Some(ct.clone()),
                connector_options: resolved.connector_options,
                format: resolved.format,
                format_options: resolved.format_options,
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
        reject_reserved_namespace(&name)?;

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

        let mut primary_key: Option<String> = None;

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

        let mut connector_type: Option<String> = None;
        let mut connector_options: HashMap<String, String> = HashMap::with_capacity(8);
        let mut format: Option<String> = None;
        let mut format_options: HashMap<String, String> = HashMap::with_capacity(4);
        let mut refresh_mode: Option<laminar_connectors::reference::RefreshMode> = None;
        let mut cache_mode: Option<crate::table_cache_mode::TableCacheMode> = None;
        let mut cache_max_entries: Option<usize> = None;
        let mut cache_max_bytes: Option<usize> = None;
        let mut cache_ttl: Option<std::time::Duration> = None;
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
                    "cache_mode" | "cache.mode" => {
                        cache_mode = Some(crate::table_cache_mode::parse_cache_mode(&val)?);
                    }
                    "cache_max_entries" | "cache.max_entries" => {
                        cache_max_entries = Some(val.parse::<usize>().map_err(|_| {
                            DbError::InvalidOperation(format!(
                                "Invalid cache_max_entries '{val}': expected positive integer"
                            ))
                        })?);
                    }
                    "cache_max_bytes" | "cache.max_bytes" | "cache.memory" => {
                        let bytes = val.parse::<usize>().map_err(|_| {
                            DbError::InvalidOperation(format!(
                                "Invalid cache_max_bytes '{val}': expected positive integer"
                            ))
                        })?;
                        if bytes == 0 {
                            return Err(DbError::InvalidOperation(format!(
                                "Invalid cache_max_bytes '{val}': expected positive integer"
                            )));
                        }
                        cache_max_bytes = Some(bytes);
                    }
                    "cache_ttl" | "cache.ttl" => {
                        let secs = val.parse::<u64>().map_err(|_| {
                            DbError::InvalidOperation(format!(
                                "Invalid cache_ttl '{val}': expected positive integer seconds"
                            ))
                        })?;
                        if secs == 0 {
                            return Err(DbError::InvalidOperation(format!(
                                "Invalid cache_ttl '{val}': expected positive integer seconds"
                            )));
                        }
                        cache_ttl = Some(std::time::Duration::from_secs(secs));
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

        let resolved_cache_mode = match (&cache_mode, cache_max_entries) {
            (Some(crate::table_cache_mode::TableCacheMode::Partial { .. }), Some(max)) => {
                Some(crate::table_cache_mode::TableCacheMode::Partial { max_entries: max })
            }
            _ => cache_mode.clone(),
        };

        let is_persistent = storage.as_deref() == Some("persistent");

        if let Some(ref pk) = primary_key {
            let cache = resolved_cache_mode
                .clone()
                .unwrap_or(crate::table_cache_mode::TableCacheMode::Full);

            if is_persistent {
                return Err(DbError::InvalidOperation(
                    "storage = 'persistent' is no longer supported; use in-memory tables with in-memory caching instead".to_string(),
                ));
            } else if resolved_cache_mode.is_some() {
                let mut ts = self.table_store.write();
                ts.create_table_with_cache(&name, schema.clone(), pk, cache)?;
            } else {
                let mut ts = self.table_store.write();
                ts.create_table(&name, schema.clone(), pk)?;
            }
        }

        if connector_type.is_some() || !connector_options.is_empty() {
            if let Some(ref pk) = primary_key {
                if let Some(ref ct) = connector_type {
                    let mut ts = self.table_store.write();
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
                    cache_max_bytes,
                    cache_ttl,
                });
            }
        }

        // scan() reads current TableStore rows; no re-register needed after INSERTs.
        {
            let provider = crate::table_provider::ReferenceTableProvider::new(
                name.clone(),
                schema.clone(),
                self.table_store.clone(),
            );
            self.ctx
                .register_table(&name, Arc::new(provider))
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

        if self.connector_ddl_rejected() {
            return Err(DbError::Pipeline(format!(
                "Cannot drop source '{name_str}' while pipeline is running. \
                 Stop the pipeline first."
            )));
        }

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

                let mut fields: Vec<arrow::datatypes::FieldRef> =
                    existing_schema.fields().iter().cloned().collect();
                fields.push(Arc::new(Field::new(&col_name, arrow_type, true)));
                let new_schema = Arc::new(arrow::datatypes::Schema::new(fields));

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

        if self.connector_ddl_rejected() {
            return Err(DbError::Pipeline(format!(
                "Cannot drop sink '{name_str}' while pipeline is running. \
                 Stop the pipeline first."
            )));
        }

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

    pub(crate) async fn handle_create_stream(
        &self,
        name: &sqlparser::ast::ObjectName,
        query: &StreamingStatement,
        emit_clause: Option<&laminar_sql::parser::EmitClause>,
        query_sql: &str,
        retention_bytes: Option<u64>,
    ) -> Result<ExecuteResult, DbError> {
        let name_str = name.to_string();
        reject_reserved_namespace(&name_str)?;
        self.catalog.register_stream(&name_str)?;

        if let Some(bytes) = retention_bytes {
            let cap = usize::try_from(bytes).unwrap_or(usize::MAX);
            self.subscription_registry.configure(&name_str, cap);
        }

        let query_sql = query_sql.to_string();

        // Planning errors surface to the caller — DDL must not silently fall back.
        let (plan_emit, plan_window, plan_order, plan_joins) = {
            let mut planner = self.planner.lock();
            let stmt = StreamingStatement::CreateStream {
                name: name.clone(),
                query: Box::new(query.clone()),
                emit_clause: emit_clause.cloned(),
                or_replace: false,
                if_not_exists: false,
                query_sql: query_sql.clone(),
                retention_bytes: None,
            };
            match planner.plan(&stmt) {
                Ok(laminar_sql::planner::StreamingPlan::Query(ref qp)) => (
                    qp.emit_clause.clone(),
                    qp.window_config.clone(),
                    qp.order_config.clone(),
                    qp.join_config.clone(),
                ),
                Ok(_) => (emit_clause.cloned(), None, None, None),
                Err(e) => return Err(laminar_sql::Error::from(e).into()),
            }
        };

        {
            let mut mgr = self.connector_manager.lock();
            mgr.register_stream(crate::connector_manager::StreamRegistration {
                name: name_str.clone(),
                query_sql: query_sql.clone(),
                emit_clause: plan_emit.clone(),
                window_config: plan_window.clone(),
                order_config: plan_order.clone(),
                join_config: plan_joins.clone(),
            });
        }

        // Register as a DataFusion placeholder for plan-time name resolution by downstream MVs.
        // Best-effort: if planning fails, the stream still runs.
        if let Some(schema) =
            crate::pipeline_lifecycle::plan_output_schema(&self.ctx, &query_sql).await
        {
            use datafusion::datasource::empty::EmptyTable;
            let _ = self.ctx.deregister_table(&name_str);
            if let Err(e) = self
                .ctx
                .register_table(&name_str, Arc::new(EmptyTable::new(schema)))
            {
                // Roll back the catalog + connector registration so a failed
                // CREATE STREAM doesn't leave the stream half-registered.
                self.catalog.drop_stream(&name_str);
                self.connector_manager.lock().unregister_stream(&name_str);
                self.subscription_registry.drop_name(&name_str);
                return Err(DbError::Pipeline(format!(
                    "could not register stream '{name_str}' for downstream planning: {e}"
                )));
            }
        }

        // Hot-add the query while running; a saturated channel is a retryable error.
        if let Some(ref tx) = *self.control_tx.lock() {
            tx.try_send(crate::pipeline::ControlMsg::AddStream {
                name: name_str.clone(),
                sql: query_sql,
                emit_clause: plan_emit,
                window_config: plan_window,
                order_config: plan_order,
                join_config: plan_joins,
            })
            .map_err(|e| {
                self.catalog.drop_stream(&name_str);
                self.connector_manager.lock().unregister_stream(&name_str);
                self.subscription_registry.drop_name(&name_str);
                DbError::Pipeline(format!(
                    "control channel busy, retry CREATE STREAM '{name_str}': {e}"
                ))
            })?;
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

        self.subscription_registry.drop_name(&name_str);

        // A saturated channel surfaces as a retryable error rather than a silent no-op.
        if let Some(ref tx) = *self.control_tx.lock() {
            tx.try_send(crate::pipeline::ControlMsg::DropStream {
                name: name_str.clone(),
            })
            .map_err(|e| {
                DbError::Pipeline(format!(
                    "control channel busy, retry DROP STREAM '{name_str}': {e}"
                ))
            })?;
        }

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

        {
            let mgr = self.connector_manager.lock();
            for (stream_name, reg) in mgr.streams() {
                let refs = crate::sql_analysis::extract_table_references(&reg.query_sql);
                if refs.contains(name) {
                    dependents.push(stream_name.clone());
                }
            }
        }

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
            if self.catalog.drop_stream(dep) {
                self.connector_manager.lock().unregister_stream(dep);
                self.subscription_registry.drop_name(dep);
            }
            {
                let mut registry = self.mv_registry.lock();
                if let Ok(views) = registry.unregister_cascade(dep) {
                    drop(registry);
                    let mut mgr = self.connector_manager.lock();
                    for v in &views {
                        mgr.unregister_stream(&v.name);
                        self.subscription_registry.drop_name(&v.name);
                    }
                }
            }
        }
    }

    /// Register a materialized view and wire it into the running pipeline.
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)]
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
        let name_str = name.to_string();
        reject_reserved_namespace(&name_str)?;

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

        let query_sql = query_sql.to_string();

        // Planning is cheaper than execution; fall back for ASOF and other joins DataFusion can't lower.
        let schema =
            match crate::pipeline_lifecycle::plan_output_schema(&self.ctx, &query_sql).await {
                Some(s) => s,
                None => match self.handle_query(&query_sql).await? {
                    ExecuteResult::Query(qh) => qh.schema().clone(),
                    _ => Arc::new(Schema::new(vec![Field::new(
                        "result",
                        DataType::Utf8,
                        true,
                    )])),
                },
            };

        let table_refs = crate::sql_analysis::extract_table_references(&query_sql);
        let catalog_sources = self.catalog.list_sources();
        let mut sources: Vec<String> = catalog_sources
            .into_iter()
            .filter(|s| table_refs.contains(s.as_str()))
            .collect();

        {
            let registry = self.mv_registry.lock();
            for view in registry.views() {
                if view.name != name_str && table_refs.contains(view.name.as_str()) {
                    sources.push(view.name.clone());
                }
            }
        }

        {
            let mv =
                laminar_core::mv::MaterializedView::new(&name_str, sql, sources, schema.clone());

            let mut registry = self.mv_registry.lock();

            if or_replace {
                let _ = registry.unregister_cascade(&name_str);
            }

            registry
                .register(mv)
                .map_err(|e| DbError::MaterializedView(e.to_string()))?;
        }

        // EMIT ON WINDOW CLOSE routes to EowcQueryOperator; other emit modes use SqlQueryOperator.
        let (plan_emit, plan_window, plan_order, plan_joins) = {
            let mut planner = self.planner.lock();
            let stmt = StreamingStatement::CreateStream {
                name: name.clone(),
                query: Box::new(query.clone()),
                emit_clause,
                or_replace: false,
                if_not_exists: false,
                query_sql: query_sql.clone(),
                retention_bytes: None,
            };
            match planner.plan(&stmt) {
                Ok(laminar_sql::planner::StreamingPlan::Query(ref qp)) => (
                    qp.emit_clause.clone(),
                    qp.window_config.clone(),
                    qp.order_config.clone(),
                    qp.join_config.clone(),
                ),
                _ => (None, None, None, None),
            }
        };

        {
            let mut mgr = self.connector_manager.lock();
            mgr.register_stream(crate::connector_manager::StreamRegistration {
                name: name_str.clone(),
                query_sql: query_sql.clone(),
                emit_clause: plan_emit.clone(),
                window_config: plan_window.clone(),
                order_config: plan_order.clone(),
                join_config: plan_joins.clone(),
            });
        }

        {
            use crate::mv_store::MvStorageMode;

            // Non-windowed aggs emit all groups every cycle (replace-all); windowed aggs emit
            // only closing windows (append) so previous windows aren't overwritten.
            let has_aggregate = self.ctx.sql(&query_sql).await.ok().is_some_and(|df| {
                crate::aggregate_state::find_aggregate(df.logical_plan()).is_some()
            });
            let mode = if has_aggregate && plan_window.is_none() {
                MvStorageMode::Aggregate
            } else {
                MvStorageMode::append_default()
            };

            self.mv_store
                .write()
                .create_mv(&name_str, schema.clone(), mode);

            let mv_provider = crate::table_provider::MvTableProvider::new(
                name_str.clone(),
                schema.clone(),
                self.mv_store.clone(),
            );

            // In cluster mode each node owns a vnode slice; wrap to union peers on read.
            #[cfg(feature = "cluster")]
            let provider: Arc<dyn datafusion::datasource::TableProvider> =
                if let Some(controller) = self.cluster_controller.lock().clone() {
                    Arc::new(
                        laminar_sql::datafusion::distributed_scan::DistributedTableProvider::new(
                            name_str.clone(),
                            schema,
                            Arc::new(mv_provider),
                            controller,
                        ),
                    )
                } else {
                    Arc::new(mv_provider)
                };
            #[cfg(not(feature = "cluster"))]
            let provider: Arc<dyn datafusion::datasource::TableProvider> = Arc::new(mv_provider);

            let _ = self.ctx.deregister_table(&name_str);
            self.ctx.register_table(&name_str, provider).map_err(|e| {
                DbError::MaterializedView(format!("Failed to register MV table provider: {e}"))
            })?;
        }

        // Hot-add to running pipeline; roll back on a saturated channel so retry is clean.
        if let Some(ref tx) = *self.control_tx.lock() {
            tx.try_send(crate::pipeline::ControlMsg::AddStream {
                name: name_str.clone(),
                sql: query_sql,
                emit_clause: plan_emit,
                window_config: plan_window,
                order_config: plan_order,
                join_config: plan_joins,
            })
            .map_err(|e| {
                let _ = self.ctx.deregister_table(&name_str);
                let _ = self.mv_registry.lock().unregister(&name_str);
                self.connector_manager.lock().unregister_stream(&name_str);
                self.mv_store.write().drop_mv(&name_str);
                DbError::Pipeline(format!(
                    "control channel busy, retry CREATE MATERIALIZED VIEW '{name_str}': {e}"
                ))
            })?;
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

        // Notify before local teardown so a saturated channel stays retryable.
        if let Some(ref tx) = *self.control_tx.lock() {
            for dropped in &dropped_names {
                tx.try_send(crate::pipeline::ControlMsg::DropStream {
                    name: dropped.clone(),
                })
                .map_err(|e| {
                    DbError::Pipeline(format!(
                        "control channel busy, retry DROP MATERIALIZED VIEW '{dropped}': {e}"
                    ))
                })?;
            }
        }

        {
            let mut mgr = self.connector_manager.lock();
            let mut mv_store = self.mv_store.write();
            for dropped in &dropped_names {
                mgr.unregister_stream(dropped);
                mv_store.drop_mv(dropped);
                let _ = self.ctx.deregister_table(dropped);
            }
        }
        for dropped in &dropped_names {
            self.subscription_registry.drop_name(dropped);
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

            self.table_store.write().drop_table(&name_str);
            self.connector_manager.lock().unregister_table(&name_str);
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

    /// Re-publish a lookup table's snapshot after a write so lookup joins don't
    /// probe stale rows. No-op for non-lookup tables.
    #[allow(clippy::unnecessary_wraps)] // signature kept fallible for call sites
    pub(crate) fn sync_table_to_datafusion(&self, name: &str) -> Result<(), DbError> {
        if self.lookup_registry.get_entry(name).is_none() {
            return Ok(());
        }
        if let Some(batch) = self.table_store.read().to_record_batch(name) {
            self.lookup_registry
                .register(name, laminar_sql::datafusion::LookupSnapshot { batch });
        }
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

/// Normalised connector info after merging the two CREATE SOURCE/SINK syntax forms.
pub(crate) struct ResolvedConnector {
    pub connector_type: Option<String>,
    pub connector_options: HashMap<String, String>,
    pub format: Option<String>,
    pub format_options: HashMap<String, String>,
}

/// Merge the `FROM <TYPE> (...)` and `WITH ('connector' = ...)` syntax into one result.
pub(crate) fn resolve_connector_info(
    connector_type: Option<&String>,
    connector_options: &HashMap<String, String>,
    format: Option<&laminar_sql::parser::FormatSpec>,
    with_options: &HashMap<String, String>,
) -> ResolvedConnector {
    if connector_type.is_some() {
        ResolvedConnector {
            connector_type: connector_type.cloned(),
            connector_options: connector_options.clone(),
            format: format.map(|f| f.format_type.clone()),
            format_options: format.map(|f| f.options.clone()).unwrap_or_default(),
        }
    } else if let Some(ct) = with_options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("connector"))
        .map(|(_, v)| v)
    {
        let (conn_opts, fmt, fmt_opts) = extract_connector_from_with_options(with_options);
        ResolvedConnector {
            connector_type: Some(ct.to_uppercase()),
            connector_options: conn_opts,
            format: fmt,
            format_options: fmt_opts,
        }
    } else {
        ResolvedConnector {
            connector_type: None,
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
        }
    }
}

/// Validate that a resolved format string is known.
pub(crate) fn validate_format(format: Option<&String>) -> Result<(), DbError> {
    if let Some(fmt_str) = format {
        laminar_connectors::serde::Format::parse(&fmt_str.to_lowercase())
            .map_err(|e| DbError::Connector(format!("Unknown format '{fmt_str}': {e}")))?;
    }
    Ok(())
}

/// Extract `(connector_options, format, format_options)` from a `WITH (...)` clause.
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
            continue;
        }
        if lower == "format" {
            format = Some(value.clone());
            continue;
        }
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
