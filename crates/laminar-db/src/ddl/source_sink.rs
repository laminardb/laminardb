//! Source and sink statements: connector resolution, source registration, and
//! sink catalog mutation. Connector/format options are resolved and validated
//! before any catalog mutation.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::DataType;
use laminar_connectors::connector::{SourceContract, SourceInputMode};
use laminar_core::catalog::CatalogObjectKind;
use laminar_sql::parser::StreamingStatement;
use laminar_sql::translator::streaming_ddl::{self, ColumnDefinition};

use crate::connector_manager::normalize_connector_type;
use crate::db::{canonical_object_name, exact_table_reference, LaminarDB};
use crate::error::DbError;
use crate::handle::{DdlInfo, ExecuteResult};

use super::catalog::reject_reserved_namespace;

/// Which connector registry `prepare_connector` validates against.
#[derive(Clone, Copy)]
pub(super) enum ConnectorKind {
    Source,
    Sink,
}

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

impl LaminarDB {
    /// Resolve `${VAR}` in connector + format options (config vars, then env) and
    /// verify the type is registered + format known — up front, before any
    /// catalog mutation. `None` when no connector is declared.
    pub(super) fn prepare_connector(
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
        let source_contract = self.validate_source_input_schema_contract(
            &source_name,
            resolved.as_ref(),
            &source_def,
        )?;
        if source_contract
            .is_some_and(|contract| contract.input_mode != SourceInputMode::AppendOnly)
        {
            self.validate_new_mutation_source_consumers(&source_name)?;
        }

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

    fn validate_source_input_schema_contract(
        &self,
        source_name: &str,
        resolved: Option<&ResolvedConnector>,
        source_def: &streaming_ddl::SourceDefinition,
    ) -> Result<Option<SourceContract>, DbError> {
        let weight = laminar_core::changelog::WEIGHT_COLUMN;
        let fields = source_def.schema.fields();
        if let Some(field) = fields.iter().find(|field| {
            ["_op", "__op"]
                .iter()
                .any(|name| field.name().eq_ignore_ascii_case(name))
        }) {
            return Err(DbError::InvalidOperation(format!(
                "CREATE SOURCE column '{}' is reserved engine mutation metadata",
                field.name()
            )));
        }
        let weight_fields = fields
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name().eq_ignore_ascii_case(weight))
            .collect::<Vec<_>>();
        let canonical_weight = matches!(weight_fields.as_slice(), [(index, field)]
            if *index + 1 == fields.len()
                && field.name() == weight
                && field.data_type() == &DataType::Int64
                && !field.is_nullable());

        let contract = if let Some(resolved) = resolved {
            let registration = crate::connector_manager::SourceRegistration {
                name: source_name.to_string(),
                connector_type: resolved.connector_type.clone(),
                connector_options: resolved.connector_options.clone(),
                format: resolved.format.clone(),
                format_options: resolved.format_options.clone(),
            };
            let mut config = crate::connector_manager::build_source_config(&registration)?;
            config.set(
                "_arrow_schema".to_string(),
                crate::pipeline_callback::encode_arrow_schema(&source_def.schema),
            );
            let connector = self
                .connector_registry
                .create_source(&config, None)
                .map_err(|error| {
                    DbError::Config(format!(
                        "cannot construct source '{source_name}' for input-mode validation: {error}"
                    ))
                })?;
            Some(connector.contract(&config).map_err(|error| {
                DbError::Config(format!(
                    "source '{source_name}' has an invalid connector contract: {error}"
                ))
            })?)
        } else {
            None
        };

        match contract.map(|contract| contract.input_mode) {
            Some(SourceInputMode::FullChangelog) if canonical_weight => Ok(()),
            Some(SourceInputMode::FullChangelog) => Err(DbError::InvalidOperation(format!(
                "full-changelog source '{source_name}' requires exact trailing non-null BIGINT '{weight}'"
            ))),
            Some(SourceInputMode::AppendOnly | SourceInputMode::KeyedUpsert) | None
                if weight_fields.is_empty() =>
            {
                Ok(())
            }
            Some(mode) => Err(DbError::InvalidOperation(format!(
                "source '{source_name}' declares '{weight}', but connector input mode is {mode:?}"
            ))),
            None => Err(DbError::InvalidOperation(format!(
                "source '{source_name}' cannot declare engine changelog column '{weight}' without a FullChangelog connector contract"
            ))),
        }?;
        Ok(contract)
    }

    /// A mutation connector may be declared before its stateful consumer, but it cannot be
    /// retrofitted underneath an already-persisted ordinary stream or sink. Full contract
    /// certification is repeated after the stateful route exists and again before startup.
    fn validate_new_mutation_source_consumers(&self, source_name: &str) -> Result<(), DbError> {
        use laminar_sql::translator::JoinOperatorConfig;

        let manager = self.connector_manager.lock();
        if manager.sinks().values().any(|sink| {
            sink.input == source_name || sink.query_inputs.iter().any(|input| input == source_name)
        }) {
            return Err(DbError::Config(format!(
                "mutation source '{source_name}' cannot be created beneath an existing direct sink; mutable sources are exclusive to admitted stateful routes"
            )));
        }
        for stream in manager.streams().values() {
            if !crate::sql_analysis::extract_table_references(&stream.query_sql)
                .contains(source_name)
            {
                continue;
            }
            let potential_stateful_route = match stream.join_config.as_deref() {
                Some([JoinOperatorConfig::Temporal(config)]) => {
                    config.right_table == source_name && config.left_table != source_name
                }
                Some([JoinOperatorConfig::StreamStream(config)]) => {
                    crate::sql_analysis::detect_stream_join_query(&stream.query_sql).is_some_and(
                        |detected| {
                            (config.left_table == source_name || config.right_table == source_name)
                                && detected.left_pre_filter.is_none()
                                && detected.right_pre_filter.is_none()
                                && detected.config.left_table == config.left_table
                                && detected.config.right_table == config.right_table
                                && detected.config.join_type == config.join_type
                                && detected.config.left_keys == config.left_keys
                                && detected.config.right_keys == config.right_keys
                                && detected.config.left_time_column == config.left_time_column
                                && detected.config.right_time_column == config.right_time_column
                                && detected.config.time_bound == config.time_bound
                        },
                    )
                }
                _ => false,
            };
            if !potential_stateful_route {
                return Err(DbError::Config(format!(
                    "mutation source '{source_name}' cannot be created beneath ordinary stream '{}'; mutable sources are exclusive to admitted stateful routes",
                    stream.name
                )));
            }
        }
        Ok(())
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
        let (input, query_inputs) = match &create.from {
            laminar_sql::parser::SinkFrom::Table(table) => {
                (canonical_object_name(table)?, Vec::new())
            }
            laminar_sql::parser::SinkFrom::Query(_) => {
                return Err(DbError::Unsupported(
                    "sink queries require a named internal stream; CREATE STREAM for the query and sink FROM that stream"
                        .into(),
                ));
            }
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

        let candidate = if let Some(resolved) = resolved {
            crate::connector_manager::SinkRegistration {
                name: name.clone(),
                input: input.clone(),
                query_inputs: query_inputs.clone(),
                connector_type: resolved.connector_type,
                connector_options: resolved.connector_options,
                format: resolved.format,
                format_options: resolved.format_options,
                filter_expr: create.filter.as_ref().map(std::string::ToString::to_string),
            }
        } else {
            crate::connector_manager::SinkRegistration {
                name: name.clone(),
                input: input.clone(),
                query_inputs,
                connector_type: None,
                connector_options: HashMap::new(),
                format: None,
                format_options: HashMap::new(),
                filter_expr: create.filter.as_ref().map(std::string::ToString::to_string),
            }
        };
        let (source_regs, mut sink_regs, stream_regs) = {
            let manager = self.connector_manager.lock();
            (
                manager.sources().clone(),
                manager.sinks().clone(),
                manager.streams().clone(),
            )
        };
        for input in std::iter::once(candidate.input.as_str()).chain(
            candidate
                .query_inputs
                .iter()
                .map(std::string::String::as_str),
        ) {
            if self.catalog.get_source(input).is_none() {
                continue;
            }
            if self
                .resolve_registered_source_contract(input, &source_regs)?
                .is_some_and(|(contract, _)| contract.input_mode != SourceInputMode::AppendOnly)
            {
                return Err(DbError::Config(format!(
                    "sink '{name}' cannot directly consume mutation source '{input}'; mutable sources are exclusive to admitted stateful routes"
                )));
            }
        }
        sink_regs.insert(name.clone(), candidate.clone());
        self.validate_persisted_temporal_source_contracts(
            &source_regs,
            &sink_regs,
            &stream_regs,
            self.runtime_mode(),
        )?;

        self.catalog.register_sink(&name, &input)?;

        self.connector_manager.lock().register_sink(candidate);

        reservation.commit();

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE SINK".to_string(),
            object_name: name,
            applied: true,
        }))
    }
}
