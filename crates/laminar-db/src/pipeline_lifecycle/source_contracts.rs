use super::{
    admit_source_recovery_contract, admit_temporal_source_contract,
    has_only_ordered_interval_consumers, has_only_temporal_right_consumers,
    schema_has_reserved_mutation_columns, Arc, DbError, DeliveryGuarantee, FxHashMap, HashMap,
    LaminarDB, OrderedIntervalAdmissions, RuntimeMode, SourceContract, SourceInputMode,
    SourceRowPositionCapability, TemporalSourceRole, CLUSTER_BEST_EFFORT,
};

impl LaminarDB {
    pub(crate) fn validate_temporal_source_metadata(
        &self,
        stream: &str,
        config: &laminar_sql::translator::TemporalJoinTranslatorConfig,
        source_regs: &HashMap<String, crate::connector_manager::SourceRegistration>,
    ) -> Result<
        (
            Arc<crate::catalog::SourceEntry>,
            Arc<crate::catalog::SourceEntry>,
        ),
        DbError,
    > {
        use arrow_schema::DataType;

        if config.left_key_columns.is_empty()
            || config.left_key_columns.len() != config.right_key_columns.len()
        {
            return Err(DbError::Config(format!(
                "temporal stream '{stream}' requires paired equality keys"
            )));
        }
        let left = self.catalog.get_source(&config.left_table).ok_or_else(|| {
            DbError::Config(format!(
                "temporal stream '{stream}' left source '{}' is absent from the source catalog",
                config.left_table
            ))
        })?;
        let right = self
            .catalog
            .get_source(&config.right_table)
            .ok_or_else(|| {
                DbError::Config(format!(
                "temporal stream '{stream}' right source '{}' is absent from the source catalog",
                config.right_table
                ))
            })?;

        for (role, source_name, time_column, entry) in [
            (
                TemporalSourceRole::Left,
                config.left_table.as_str(),
                config.left_time_column.as_str(),
                &left,
            ),
            (
                TemporalSourceRole::Right,
                config.right_table.as_str(),
                config.right_time_column.as_str(),
                &right,
            ),
        ] {
            let source_reg = source_regs.get(source_name).ok_or_else(|| {
                DbError::Config(format!(
                    "temporal stream '{stream}' {} input '{source_name}' must be a direct configured source; catalog bridges and intermediate streams are unsupported",
                    role.name()
                ))
            })?;
            if source_reg.connector_type.is_none() || source_reg.name != source_name {
                return Err(DbError::Config(format!(
                    "temporal stream '{stream}' {} input '{source_name}' must be a direct configured source; catalog bridges and intermediate streams are unsupported",
                    role.name()
                )));
            }
            laminar_connectors::connector::schema_with_source_row_positions(&entry.schema)
                .map_err(|error| {
                    DbError::Config(format!(
                        "temporal stream '{stream}' {} source-position schema: {error}",
                        role.name()
                    ))
                })?;
            if entry
                .is_processing_time
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(DbError::Config(format!(
                    "temporal stream '{stream}' {} source '{source_name}' must use event time, not processing time",
                    role.name()
                )));
            }
            if entry.watermark_column.as_deref() != Some(time_column)
                || entry.max_out_of_orderness.is_none()
            {
                return Err(DbError::Config(format!(
                    "temporal stream '{stream}' {} source '{source_name}' must declare WATERMARK FOR {time_column} with a bounded out-of-orderness policy",
                    role.name()
                )));
            }
            let field = entry.schema.field_with_name(time_column).map_err(|_| {
                DbError::Config(format!(
                    "temporal stream '{stream}' {} time column '{time_column}' is absent",
                    role.name()
                ))
            })?;
            if field.is_nullable() || !matches!(field.data_type(), DataType::Timestamp(_, _)) {
                return Err(DbError::Config(format!(
                    "temporal stream '{stream}' {} time column '{time_column}' must be a non-null timestamp",
                    role.name()
                )));
            }
        }

        let mut key_types = Vec::with_capacity(config.left_key_columns.len());
        for (left_key, right_key) in config
            .left_key_columns
            .iter()
            .zip(&config.right_key_columns)
        {
            let left_field = left.schema.field_with_name(left_key).map_err(|_| {
                DbError::Config(format!(
                    "temporal stream '{stream}' left key column '{left_key}' is absent"
                ))
            })?;
            let right_field = right.schema.field_with_name(right_key).map_err(|_| {
                DbError::Config(format!(
                    "temporal stream '{stream}' right key column '{right_key}' is absent"
                ))
            })?;
            if left_field.data_type() != right_field.data_type() {
                return Err(DbError::Config(format!(
                    "temporal stream '{stream}' key types must match exactly"
                )));
            }
            key_types.push(left_field.data_type().clone());
        }
        laminar_core::state::PartitionKeyCodecV1::try_new(key_types).map_err(|error| {
            DbError::Config(format!(
                "temporal stream '{stream}' key is not partitionable: {error}"
            ))
        })?;

        Ok((left, right))
    }

    pub(crate) fn validate_persisted_temporal_source_contracts(
        &self,
        source_regs: &HashMap<String, crate::connector_manager::SourceRegistration>,
        sink_regs: &HashMap<String, crate::connector_manager::SinkRegistration>,
        stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
        runtime: RuntimeMode,
    ) -> Result<FxHashMap<String, TemporalSourceRole>, DbError> {
        use laminar_sql::translator::JoinOperatorConfig;

        let mut streams: Vec<_> = stream_regs.values().collect();
        streams.sort_by(|left, right| left.name.cmp(&right.name));
        let mut contracts = HashMap::new();
        let mut temporal_source_roles = FxHashMap::default();
        let mut retention_validated = false;
        for stream in streams {
            let Some(joins) = stream.join_config.as_deref() else {
                continue;
            };
            for join in joins {
                let JoinOperatorConfig::Temporal(config) = join else {
                    continue;
                };
                if !retention_validated {
                    crate::config::temporal_join_idle_history_retention_ms(
                        self.config.temporal_join_idle_history_retention,
                    )
                    .map_err(|reason| {
                        DbError::Config(format!("temporal stream '{}': {reason}", stream.name))
                    })?;
                    retention_validated = true;
                }
                let (left_entry, right_entry) =
                    self.validate_temporal_source_metadata(&stream.name, config, source_regs)?;
                for (role, source_name, entry) in [
                    (
                        TemporalSourceRole::Left,
                        config.left_table.as_str(),
                        left_entry.as_ref(),
                    ),
                    (
                        TemporalSourceRole::Right,
                        config.right_table.as_str(),
                        right_entry.as_ref(),
                    ),
                ] {
                    let source_reg = &source_regs[source_name];
                    let contract = if let Some(contract) = contracts.get(source_name).copied() {
                        contract
                    } else {
                        let connector_config = self
                            .build_registered_source_config(source_name, source_reg)
                            .map_err(|error| {
                                DbError::Config(format!(
                                    "temporal source '{source_name}' has invalid connector configuration: {error}"
                                ))
                            })?;
                        let connector = self
                            .connector_registry
                            .create_source(&connector_config, None)
                            .map_err(|error| {
                                DbError::Config(format!(
                                    "cannot construct temporal source '{source_name}' for contract validation: {error}"
                                ))
                            })?;
                        let connector_schema = connector.schema();
                        if !connector_schema.fields().is_empty()
                            && connector_schema.as_ref() != entry.schema.as_ref()
                        {
                            return Err(DbError::Config(format!(
                                "temporal source '{source_name}' connector schema does not match its catalog schema"
                            )));
                        }
                        let contract = connector.contract(&connector_config).map_err(|error| {
                            DbError::Config(format!(
                                "temporal source '{source_name}' has an invalid connector contract: {error}"
                            ))
                        })?;
                        contracts.insert(source_name.to_string(), contract);
                        contract
                    };
                    if matches!(role, TemporalSourceRole::Right)
                        && contract.input_mode == SourceInputMode::KeyedUpsert
                        && !has_only_temporal_right_consumers(source_name, stream_regs, sink_regs)
                    {
                        return Err(DbError::Config(format!(
                            "temporal right mutation source '{source_name}' has a non-temporal-right consumer"
                        )));
                    }
                    admit_temporal_source_contract(
                        contract,
                        role,
                        !entry.primary_key.is_empty(),
                        schema_has_reserved_mutation_columns(entry.schema.as_ref()),
                        self.config.delivery_guarantee,
                        self.config.checkpoint.is_some(),
                        runtime,
                    )
                    .map_err(|reason| {
                        DbError::Config(format!(
                            "temporal stream '{}' {} source '{}' is not admissible in {runtime:?} mode with {} delivery: {reason} (contract: {contract:?})",
                            stream.name,
                            role.name(),
                            source_name,
                            self.config.delivery_guarantee
                        ))
                    })?;
                    temporal_source_roles
                        .entry(source_name.to_string())
                        .or_insert(role);
                }
            }
        }
        Ok(temporal_source_roles)
    }

    pub(crate) fn resolve_registered_source_contract(
        &self,
        source_name: &str,
        source_regs: &HashMap<String, crate::connector_manager::SourceRegistration>,
    ) -> Result<Option<(SourceContract, arrow_schema::SchemaRef)>, DbError> {
        let Some(source_reg) = source_regs
            .get(source_name)
            .filter(|registration| registration.connector_type.is_some())
        else {
            return Ok(None);
        };
        let connector_config = self
            .build_registered_source_config(source_name, source_reg)
            .map_err(|error| {
                DbError::Config(format!(
                    "source '{source_name}' has invalid connector configuration: {error}"
                ))
            })?;
        let connector = self
            .connector_registry
            .create_source(&connector_config, None)
            .map_err(|error| {
                DbError::Config(format!(
                    "cannot construct interval source '{source_name}' for contract validation: {error}"
                ))
            })?;
        let connector_schema = connector.schema();
        let contract = connector.contract(&connector_config).map_err(|error| {
            DbError::Config(format!(
                "source '{source_name}' has an invalid connector contract: {error}"
            ))
        })?;
        Ok(Some((contract, connector_schema)))
    }

    /// Require every configured mutation source to be owned by exactly one certified stateful
    /// route. The role-specific validators also prove consumer exclusivity; this closes sources
    /// that are otherwise absent from both role maps (for example a direct copy or sink).
    pub(crate) fn validate_registered_mutation_source_admission(
        &self,
        source_name: &str,
        source_regs: &HashMap<String, crate::connector_manager::SourceRegistration>,
        temporal_source_roles: &FxHashMap<String, TemporalSourceRole>,
        ordered_interval_admissions: &OrderedIntervalAdmissions,
    ) -> Result<(), DbError> {
        let Some((contract, _)) =
            self.resolve_registered_source_contract(source_name, source_regs)?
        else {
            return Ok(());
        };
        if contract.input_mode == SourceInputMode::AppendOnly {
            return Ok(());
        }
        let temporal_right = temporal_source_roles.get(source_name)
            == Some(&TemporalSourceRole::Right)
            && contract.input_mode == SourceInputMode::KeyedUpsert;
        let ordered_interval =
            ordered_interval_admissions.source_modes.get(source_name) == Some(&contract.input_mode);
        if temporal_right ^ ordered_interval {
            return Ok(());
        }
        Err(DbError::Config(format!(
            "mutation source '{source_name}' is not exclusive to exactly one admitted temporal-right or bounded interval route"
        )))
    }

    pub(super) fn validate_interval_source_metadata(
        &self,
        stream: &str,
        source_name: &str,
        time_column: &str,
        source_regs: &HashMap<String, crate::connector_manager::SourceRegistration>,
    ) -> Result<Arc<crate::catalog::SourceEntry>, DbError> {
        let entry = self.catalog.get_source(source_name).ok_or_else(|| {
            DbError::Config(format!(
                "interval stream '{stream}' input '{source_name}' is absent from the source catalog"
            ))
        })?;
        let direct = source_regs.get(source_name).is_some_and(|registration| {
            registration.connector_type.is_some() && registration.name == source_name
        });
        if !direct {
            return Err(DbError::Config(format!(
                "interval stream '{stream}' input '{source_name}' must be a direct configured source when either input is mutable"
            )));
        }
        if entry
            .is_processing_time
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(DbError::Config(format!(
                "interval stream '{stream}' source '{source_name}' must use event time, not processing time"
            )));
        }
        if entry.watermark_column.as_deref() != Some(time_column)
            || entry.max_out_of_orderness.is_none()
        {
            return Err(DbError::Config(format!(
                "interval stream '{stream}' source '{source_name}' must declare WATERMARK FOR {time_column} with a bounded out-of-orderness policy"
            )));
        }
        let field = entry.schema.field_with_name(time_column).map_err(|_| {
            DbError::Config(format!(
                "interval stream '{stream}' source '{source_name}' time column '{time_column}' is absent"
            ))
        })?;
        if field.is_nullable()
            || !matches!(field.data_type(), arrow_schema::DataType::Timestamp(_, _))
        {
            return Err(DbError::Config(format!(
                "interval stream '{stream}' source '{source_name}' time column '{time_column}' must be a non-null timestamp"
            )));
        }
        Ok(entry)
    }

    pub(super) fn bounded_interval_input_mode(
        stream: &str,
        source_name: &str,
        entry: &crate::catalog::SourceEntry,
        contract: SourceContract,
        join_keys: &[String],
        time_column: &str,
    ) -> Result<crate::operator::interval_join_input::BoundedJoinInputMode, DbError> {
        use crate::operator::interval_join_input::BoundedJoinInputMode;
        use arrow_schema::DataType;

        if contract.row_positions != SourceRowPositionCapability::OrderedDeterministic {
            return Err(DbError::Config(format!(
                "interval stream '{stream}' source '{source_name}' requires ordered deterministic row positions"
            )));
        }
        laminar_connectors::connector::schema_with_source_row_positions(&entry.schema).map_err(
            |error| {
                DbError::Config(format!(
                    "interval stream '{stream}' source '{source_name}' has an invalid source-position schema: {error}"
                ))
            },
        )?;

        let fields = entry.schema.fields();
        let reserved = |name: &str| {
            ["_op", "__op", crate::aggregate_state::WEIGHT_COLUMN]
                .iter()
                .any(|candidate| name.eq_ignore_ascii_case(candidate))
        };
        match contract.input_mode {
            SourceInputMode::AppendOnly => {
                if fields.iter().any(|field| reserved(field.name())) {
                    return Err(DbError::Config(format!(
                        "interval stream '{stream}' append-only source '{source_name}' cannot declare mutation metadata"
                    )));
                }
                Ok(BoundedJoinInputMode::AppendOnly)
            }
            SourceInputMode::KeyedUpsert => {
                if fields.iter().any(|field| reserved(field.name())) {
                    return Err(DbError::Config(format!(
                        "interval stream '{stream}' keyed-upsert source '{source_name}' cannot declare engine-owned mutation columns"
                    )));
                }
                laminar_connectors::connector::schema_with_source_mutations_and_row_positions(
                    &entry.schema,
                )
                .map_err(|error| {
                    DbError::Config(format!(
                        "interval stream '{stream}' source '{source_name}' has an invalid mutation schema: {error}"
                    ))
                })?;
                if entry.primary_key.is_empty() {
                    return Err(DbError::Config(format!(
                        "interval stream '{stream}' keyed-upsert source '{source_name}' requires an explicit PRIMARY KEY"
                    )));
                }
                for required in join_keys {
                    if !entry.primary_key.iter().any(|column| column == required) {
                        return Err(DbError::Config(format!(
                            "interval stream '{stream}' keyed-upsert source '{source_name}' PRIMARY KEY must include join/event-time column '{required}'"
                        )));
                    }
                }
                if !entry.primary_key.iter().any(|column| column == time_column) {
                    return Err(DbError::Config(format!(
                        "interval stream '{stream}' keyed-upsert source '{source_name}' PRIMARY KEY must include join/event-time column '{time_column}'"
                    )));
                }
                let primary_key_indices = entry
                    .primary_key
                    .iter()
                    .map(|column| {
                        entry.schema.index_of(column).map_err(|_| {
                            DbError::Config(format!(
                                "interval stream '{stream}' source '{source_name}' PRIMARY KEY column '{column}' is absent"
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(BoundedJoinInputMode::KeyedUpsert {
                    primary_key_indices,
                })
            }
            SourceInputMode::FullChangelog => {
                let weight = crate::aggregate_state::WEIGHT_COLUMN;
                let Some((last_index, last)) = fields
                    .len()
                    .checked_sub(1)
                    .map(|index| (index, &fields[index]))
                else {
                    return Err(DbError::Config(format!(
                        "interval stream '{stream}' full-changelog source '{source_name}' requires a trailing '{weight}' column"
                    )));
                };
                let reserved_indices = fields
                    .iter()
                    .enumerate()
                    .filter(|(_, field)| reserved(field.name()))
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>();
                if reserved_indices != [last_index]
                    || last.name() != weight
                    || last.data_type() != &DataType::Int64
                    || last.is_nullable()
                {
                    return Err(DbError::Config(format!(
                        "interval stream '{stream}' full-changelog source '{source_name}' requires the sole reserved column to be exact trailing non-null Int64 '{weight}'"
                    )));
                }
                Ok(BoundedJoinInputMode::FullChangelog)
            }
        }
    }

    pub(crate) async fn validate_persisted_interval_source_contracts(
        &self,
        source_regs: &HashMap<String, crate::connector_manager::SourceRegistration>,
        sink_regs: &HashMap<String, crate::connector_manager::SinkRegistration>,
        stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
        runtime: RuntimeMode,
    ) -> Result<OrderedIntervalAdmissions, DbError> {
        use crate::operator::interval_join_input::BoundedJoinInputMode;
        use laminar_sql::translator::JoinOperatorConfig;

        let mut streams = stream_regs.values().collect::<Vec<_>>();
        streams.sort_by(|left, right| left.name.cmp(&right.name));
        let mut contracts = FxHashMap::<String, Option<SourceContract>>::default();
        let mut connector_schemas = FxHashMap::<String, arrow_schema::SchemaRef>::default();
        let mut admission = OrderedIntervalAdmissions::default();

        for stream in streams {
            let Some([JoinOperatorConfig::StreamStream(config)]) = stream.join_config.as_deref()
            else {
                continue;
            };
            let direct_entry = |source_name: &str| {
                if source_regs
                    .get(source_name)
                    .is_none_or(|registration| registration.connector_type.is_none())
                {
                    return Ok(None);
                }
                self.catalog.get_source(source_name).map(Some).ok_or_else(|| {
                    DbError::Config(format!(
                        "interval stream '{}' configured source '{source_name}' is absent from the source catalog",
                        stream.name
                    ))
                })
            };
            let left_entry = direct_entry(&config.left_table)?;
            let right_entry = direct_entry(&config.right_table)?;
            let mut resolve = |source_name: &str| {
                if let Some(contract) = contracts.get(source_name) {
                    return Ok(*contract);
                }
                let contract = self
                    .resolve_registered_source_contract(source_name, source_regs)?
                    .map(|(contract, schema)| {
                        connector_schemas.insert(source_name.to_string(), schema);
                        contract
                    });
                contracts.insert(source_name.to_string(), contract);
                Ok::<_, DbError>(contract)
            };
            let left_contract = left_entry
                .as_deref()
                .map(|_| resolve(&config.left_table))
                .transpose()?
                .flatten();
            let right_contract = right_entry
                .as_deref()
                .map(|_| resolve(&config.right_table))
                .transpose()?
                .flatten();
            let mutable = [left_contract, right_contract]
                .into_iter()
                .flatten()
                .any(|contract| contract.input_mode != SourceInputMode::AppendOnly);
            if !mutable {
                continue;
            }
            // Preserve the legacy append-only path. Once either port is mutable, both direct
            // connector schemas become part of the ordered normalizer ABI and must match the
            // catalog exactly.
            for (source_name, entry) in [
                (config.left_table.as_str(), left_entry.as_deref()),
                (config.right_table.as_str(), right_entry.as_deref()),
            ] {
                let Some(entry) = entry else {
                    continue;
                };
                let connector_schema = connector_schemas.get(source_name).ok_or_else(|| {
                    DbError::Config(format!(
                        "interval source '{source_name}' connector contract disappeared during validation"
                    ))
                })?;
                if !connector_schema.fields().is_empty()
                    && connector_schema.as_ref() != entry.schema.as_ref()
                {
                    return Err(DbError::Config(format!(
                        "interval source '{source_name}' connector schema does not match its catalog schema"
                    )));
                }
            }

            if runtime == RuntimeMode::Cluster
                && self.config.delivery_guarantee == DeliveryGuarantee::BestEffort
            {
                return Err(DbError::Config(format!(
                    "interval stream '{}': {CLUSTER_BEST_EFFORT}",
                    stream.name
                )));
            }
            if self.config.delivery_guarantee != DeliveryGuarantee::BestEffort
                && self.config.checkpoint.is_none()
            {
                return Err(DbError::Config(format!(
                    "interval stream '{}' requires checkpointing for at-least-once/exactly-once state and source-offset recovery",
                    stream.name
                )));
            }
            let detected = crate::sql_analysis::detect_stream_join_query(&stream.query_sql)
                .ok_or_else(|| {
                    DbError::Config(format!(
                        "interval stream '{}' does not map exactly to the bounded interval-join execution path",
                        stream.name
                    ))
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
                return Err(DbError::Config(format!(
                    "interval stream '{}' planner and bounded execution metadata disagree",
                    stream.name
                )));
            }
            if detected.left_pre_filter.is_some() || detected.right_pre_filter.is_some() {
                return Err(DbError::Config(format!(
                    "interval stream '{}' mutable inputs do not support source prefilters",
                    stream.name
                )));
            }
            if crate::sql_analysis::has_unaliased_projection(&stream.query_sql) {
                return Err(DbError::Config(format!(
                    "interval stream '{}' mutable output requires every projected expression to have an explicit alias",
                    stream.name
                )));
            }
            if crate::sql_analysis::has_unqualified_interval_output_column(&stream.query_sql) {
                return Err(DbError::Config(format!(
                    "interval stream '{}' mutable output requires every projected and filtered column to use its left/right source qualifier",
                    stream.name
                )));
            }
            if stream.order_config.is_some()
                || stream.has_analytic
                || stream.has_frame
                || crate::sql_analysis::mutable_changelog_has_unsafe_modifiers(&stream.query_sql)
            {
                return Err(DbError::Config(format!(
                    "interval stream '{}' mutable output does not support DISTINCT, ordering/row limits, analytic frames, grouping, or other row-set modifiers",
                    stream.name
                )));
            }
            if crate::sql_analysis::query_references_weight(&stream.query_sql) {
                return Err(DbError::Config(format!(
                    "interval stream '{}' mutable query cannot reference the engine-owned '{}' input column",
                    stream.name,
                    crate::aggregate_state::WEIGHT_COLUMN
                )));
            }
            let dataframe = self.ctx.sql(&stream.query_sql).await.map_err(|error| {
                DbError::Config(format!(
                    "interval stream '{}' could not plan its replay contract: {error}",
                    stream.name
                ))
            })?;
            if crate::ddl::logical_aggregate_stage_count(dataframe.logical_plan()) != 0 {
                return Err(DbError::Config(format!(
                    "interval stream '{}' mutable output cannot contain an aggregate stage",
                    stream.name
                )));
            }
            if !crate::sql_analysis::planned_functions_are_immutable(dataframe.logical_plan()) {
                return Err(DbError::Config(format!(
                    "interval stream '{}' mutable route contains a planned function that is not replay-immutable",
                    stream.name
                )));
            }
            if dataframe
                .logical_plan()
                .schema()
                .fields()
                .iter()
                .any(|field| {
                    field
                        .name()
                        .eq_ignore_ascii_case(crate::aggregate_state::WEIGHT_COLUMN)
                })
            {
                return Err(DbError::Config(format!(
                    "interval stream '{}' mutable projection cannot declare or alias the engine-owned '{}' output column",
                    stream.name,
                    crate::aggregate_state::WEIGHT_COLUMN
                )));
            }

            let left_entry = self.validate_interval_source_metadata(
                &stream.name,
                &config.left_table,
                &config.left_time_column,
                source_regs,
            )?;
            let right_entry = self.validate_interval_source_metadata(
                &stream.name,
                &config.right_table,
                &config.right_time_column,
                source_regs,
            )?;
            let left_contract = left_contract.ok_or_else(|| {
                DbError::Config(format!(
                    "interval stream '{}' left input '{}' must be a direct configured source when either input is mutable",
                    stream.name, config.left_table
                ))
            })?;
            let right_contract = right_contract.ok_or_else(|| {
                DbError::Config(format!(
                    "interval stream '{}' right input '{}' must be a direct configured source when either input is mutable",
                    stream.name, config.right_table
                ))
            })?;
            let modes = [
                Self::bounded_interval_input_mode(
                    &stream.name,
                    &config.left_table,
                    left_entry.as_ref(),
                    left_contract,
                    &config.left_keys,
                    &config.left_time_column,
                )?,
                Self::bounded_interval_input_mode(
                    &stream.name,
                    &config.right_table,
                    right_entry.as_ref(),
                    right_contract,
                    &config.right_keys,
                    &config.right_time_column,
                )?,
            ];
            for (source_name, contract) in [
                (config.left_table.as_str(), left_contract),
                (config.right_table.as_str(), right_contract),
            ] {
                admit_source_recovery_contract(
                    contract,
                    self.config.delivery_guarantee,
                    self.config.checkpoint.is_some(),
                    runtime,
                )
                .map_err(|reason| {
                    DbError::Config(format!(
                        "interval stream '{}' source '{source_name}' is not recoverable with {} delivery: {reason} (contract: {contract:?})",
                        stream.name, self.config.delivery_guarantee
                    ))
                })?;
                if contract.input_mode != SourceInputMode::AppendOnly {
                    match admission
                        .source_modes
                        .insert(source_name.to_string(), contract.input_mode)
                    {
                        Some(previous) if previous != contract.input_mode => {
                            return Err(DbError::Config(format!(
                                "interval mutation source '{source_name}' resolved conflicting input modes"
                            )));
                        }
                        _ => {}
                    }
                }
            }
            debug_assert!(modes
                .iter()
                .any(|mode| !matches!(mode, BoundedJoinInputMode::AppendOnly)));
            admission.joins.insert(stream.name.clone(), modes);
        }

        for source in admission.source_modes.keys() {
            if !has_only_ordered_interval_consumers(
                source,
                stream_regs,
                sink_regs,
                &admission.joins,
            ) {
                return Err(DbError::Config(format!(
                    "interval mutation source '{source}' has a consumer outside its admitted bounded interval joins"
                )));
            }
        }
        Ok(admission)
    }
}
