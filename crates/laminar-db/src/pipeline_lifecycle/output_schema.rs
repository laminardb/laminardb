use super::{
    exact_table_reference, schema_has_reserved_mutation_columns, Arc, DbError, FxHashMap, HashMap,
};

/// Resolve a query's output schema by planning it. Returns `None` when a
/// dependency is not registered yet or the query is invalid.
pub(crate) async fn plan_output_schema(
    ctx: &datafusion::prelude::SessionContext,
    sql: &str,
) -> Option<arrow_schema::SchemaRef> {
    let plan = ctx.state().create_logical_plan(sql).await.ok()?;
    let fields: Vec<_> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| (**f).clone())
        .collect();
    Some(Arc::new(arrow_schema::Schema::new(fields)))
}

pub(crate) async fn plan_temporal_output_schema(
    ctx: &datafusion::prelude::SessionContext,
    stream: &str,
    sql: &str,
    config: &laminar_sql::translator::TemporalJoinTranslatorConfig,
    left_schema: &arrow_schema::SchemaRef,
    right_schema: &arrow_schema::SchemaRef,
) -> Result<arrow_schema::SchemaRef, DbError> {
    let left = laminar_connectors::connector::schema_with_source_row_positions(left_schema)
        .map_err(|error| {
            DbError::Config(format!(
                "temporal stream '{stream}' left source-position schema: {error}"
            ))
        })?;
    let right = laminar_connectors::connector::schema_with_source_row_positions(right_schema)
        .map_err(|error| {
            DbError::Config(format!(
                "temporal stream '{stream}' right source-position schema: {error}"
            ))
        })?;
    let joined = crate::temporal_join_state::temporal_join_output_schema(
        left.as_ref(),
        right.as_ref(),
        &config.right_table,
        config.join_kind,
        config.probe_alias.is_some(),
    )?;
    let input_table = format!("__temporal_schema_{}", uuid::Uuid::new_v4().simple());
    let projection =
        crate::sql_analysis::temporal_projection_sql_for_input(sql, config, &input_table)?;
    let what = format!("temporal stream '{stream}' projection");
    crate::operator::prepare_post_projection(ctx, &projection, &input_table, &joined, &what)
        .await
        .map(|(_, schema)| schema)
}

pub(crate) async fn resolve_stream_output_schemas(
    ctx: &datafusion::prelude::SessionContext,
    stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
    reference_tables: &rustc_hash::FxHashSet<String>,
    ordered_interval_joins: &FxHashMap<
        String,
        [crate::operator::interval_join_input::BoundedJoinInputMode; 2],
    >,
) -> Result<ResolvedStreamOutputs, DbError> {
    use datafusion::datasource::empty::EmptyTable;

    let mut schemas: HashMap<String, arrow_schema::SchemaRef> =
        HashMap::with_capacity(stream_regs.len());
    let mut shapes: HashMap<String, StreamOutputShape> = HashMap::with_capacity(stream_regs.len());
    let mut pending: Vec<&crate::connector_manager::StreamRegistration> =
        stream_regs.values().collect();
    let mut placeholders: Vec<String> = Vec::new();

    let result: Result<ResolvedStreamOutputs, DbError> = async {
        while !pending.is_empty() {
            let mut next: Vec<&crate::connector_manager::StreamRegistration> = Vec::new();
            let mut progressed = false;
            for reg in pending {
                let temporal = reg.join_config.as_deref().and_then(|joins| match joins {
                    [laminar_sql::translator::JoinOperatorConfig::Temporal(config)] => Some(config),
                    _ => None,
                });
                let (schema, shape) = if let Some(config) = temporal {
                    let left = ctx
                        .table_provider(exact_table_reference(&config.left_table))
                        .await
                        .map_err(|error| {
                            DbError::Pipeline(format!(
                                "temporal stream '{}' cannot resolve left source '{}': {error}",
                                reg.name, config.left_table
                            ))
                        })?;
                    let right = ctx
                        .table_provider(exact_table_reference(&config.right_table))
                        .await
                        .map_err(|error| {
                            DbError::Pipeline(format!(
                                "temporal stream '{}' cannot resolve right source '{}': {error}",
                                reg.name, config.right_table
                            ))
                        })?;
                    (
                        plan_temporal_output_schema(
                            ctx,
                            &reg.name,
                            &reg.query_sql,
                            config,
                            &left.schema(),
                            &right.schema(),
                        )
                        .await?,
                        StreamOutputShape {
                            aggregate: false,
                            projection_filter: false,
                            planned_functions_immutable: true,
                        },
                    )
                } else {
                    let Ok(plan) = ctx.state().create_logical_plan(&reg.query_sql).await else {
                        next.push(reg);
                        continue;
                    };
                    let fields = plan
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| (**field).clone())
                        .collect::<Vec<_>>();
                    (
                        Arc::new(arrow_schema::Schema::new(fields)),
                        StreamOutputShape {
                            aggregate: crate::aggregate_state::find_aggregate(&plan).is_some(),
                            projection_filter: crate::sql_analysis::extract_projection_filter(
                                &plan,
                            )
                            .is_some(),
                            planned_functions_immutable:
                                crate::sql_analysis::planned_functions_are_immutable(&plan),
                        },
                    )
                };
                shapes.insert(reg.name.clone(), shape);

                if !ctx
                    .table_exist(exact_table_reference(&reg.name))
                    .unwrap_or(false)
                {
                    ctx.register_table(
                        exact_table_reference(&reg.name),
                        Arc::new(EmptyTable::new(schema.clone())),
                    )
                    .map_err(|e| {
                        DbError::Pipeline(format!(
                            "could not register placeholder for stream '{}': {e}",
                            reg.name
                        ))
                    })?;
                    placeholders.push(reg.name.clone());
                }
                schemas.insert(reg.name.clone(), schema);
                progressed = true;
            }

            if !progressed {
                let mut unresolved: Vec<&str> = next.iter().map(|r| r.name.as_str()).collect();
                unresolved.sort_unstable();
                let sql = &next[0].query_sql;
                let err = ctx
                    .state()
                    .create_logical_plan(sql)
                    .await
                    .err()
                    .map_or_else(|| "unknown error".to_string(), |e| e.to_string());
                return Err(DbError::Pipeline(format!(
                    "unresolvable stream dependency among [{}]: {err}",
                    unresolved.join(", ")
                )));
            }
            pending = next;
        }

        for reg in stream_regs.values() {
            let shape = shapes.get(&reg.name).ok_or_else(|| {
                DbError::Pipeline(format!(
                    "stream '{}' has no resolved output shape",
                    reg.name
                ))
            })?;
            if !shape.aggregate && reg.window_config.is_none() {
                continue;
            }
            let certified = crate::ddl::validate_managed_aggregate_admission(
                ctx,
                &reg.query_sql,
                reg.window_config.as_ref(),
                reg.emit_clause.as_ref(),
                laminar_core::state::DEFAULT_KEY_GROUP_COUNT,
            )
            .await
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "stream '{}' aggregate could not be certified: {error}",
                    reg.name
                ))
            })?;
            if !certified {
                return Err(DbError::Pipeline(format!(
                    "stream '{}' aggregate has no managed execution path",
                    reg.name
                )));
            }
        }

        let mut changelog_carrying: rustc_hash::FxHashSet<String> =
            ordered_interval_joins.keys().cloned().collect();

        for reg in stream_regs.values() {
            let shape = shapes.get(&reg.name).ok_or_else(|| {
                DbError::Pipeline(format!(
                    "stream '{}' has no resolved output shape",
                    reg.name
                ))
            })?;
            let emit_changelog = reg.incremental
                || reg
                    .emit_clause
                    .as_ref()
                    .is_some_and(|emit| matches!(emit, laminar_sql::parser::EmitClause::Changes));
            if shape.aggregate && emit_changelog {
                changelog_carrying.insert(reg.name.clone());
            }

            if reg.window_config.is_none()
                && !crate::sql_analysis::has_join_clause(&reg.query_sql)
                && reg
                    .emit_clause
                    .as_ref()
                    .is_some_and(|emit| matches!(emit, laminar_sql::parser::EmitClause::Changes))
            {
                use crate::sql_analysis::TemporalFilterAnalysis;
                match crate::sql_analysis::analyze_temporal_filter(&reg.query_sql) {
                    TemporalFilterAnalysis::Recognized(_) => {
                        changelog_carrying.insert(reg.name.clone());
                    }
                    TemporalFilterAnalysis::PresentUnrecognized => {
                        return Err(DbError::Pipeline(format!(
                            "stream '{}' has an unrecognized retracting temporal-filter shape",
                            reg.name
                        )));
                    }
                    TemporalFilterAnalysis::NotPresent => {}
                }
            }
        }

        loop {
            let mut added = false;
            for reg in stream_regs.values() {
                let references = crate::sql_analysis::extract_table_references(&reg.query_sql);
                if !references
                    .iter()
                    .any(|name| changelog_carrying.contains(name))
                {
                    continue;
                }
                if reg.order_config.is_some()
                    || crate::sql_analysis::query_has_order_or_row_limit(&reg.query_sql)
                {
                    return Err(DbError::Pipeline(format!(
                        "stream '{}' cannot apply ordering or row limits to a changelog",
                        reg.name
                    )));
                }
                if reg.window_config.is_some() {
                    return Err(DbError::Pipeline(format!(
                        "stream '{}' cannot safely consume a changelog with window state; window aggregates do not apply input retractions",
                        reg.name
                    )));
                }
                let shape = shapes.get(&reg.name).expect("resolved above");
                if crate::sql_analysis::query_references_weight(&reg.query_sql) {
                    return Err(DbError::Pipeline(format!(
                        "stream '{}' consumes a changelog by explicitly referencing the engine-owned '{}' column",
                        reg.name,
                        crate::aggregate_state::WEIGHT_COLUMN
                    )));
                }
                if !shape.planned_functions_immutable {
                    return Err(DbError::Pipeline(format!(
                        "stream '{}' consumes a changelog through a planned function that is not replay-immutable",
                        reg.name
                    )));
                }
                let temporal_filter = !matches!(
                    crate::sql_analysis::analyze_temporal_filter(&reg.query_sql),
                    crate::sql_analysis::TemporalFilterAnalysis::NotPresent
                );
                let changelog_enrich = crate::sql_analysis::detect_changelog_enrich_query(
                    &reg.query_sql,
                    &changelog_carrying,
                    reference_tables,
                );
                if changelog_enrich.is_some() && shape.aggregate {
                    return Err(DbError::Pipeline(format!(
                        "stream '{}' cannot combine aggregate state with changelog enrichment",
                        reg.name
                    )));
                }
                if let Some(enrich) = &changelog_enrich {
                    let provider = ctx
                        .table_provider(exact_table_reference(&enrich.static_table))
                        .await
                        .map_err(|error| {
                            DbError::Pipeline(format!(
                                "stream '{}' cannot resolve static enrich table '{}': {error}",
                                reg.name, enrich.static_table
                            ))
                        })?;
                    if schema_has_reserved_mutation_columns(provider.schema().as_ref()) {
                        return Err(DbError::Pipeline(format!(
                            "stream '{}' static enrich table '{}' declares reserved engine mutation metadata (_op, __op, or __weight)",
                            reg.name, enrich.static_table
                        )));
                    }
                }
                let changelog_enrich = changelog_enrich.is_some();
                if shape.projection_filter
                    && !shape.aggregate
                    && !changelog_enrich
                    && crate::sql_analysis::projection_sql_preserving_weight(&reg.query_sql)
                        .is_none()
                {
                    return Err(DbError::Pipeline(format!(
                        "stream '{}' changelog projection cannot preserve the engine-owned weight through its exact SQL shape",
                        reg.name
                    )));
                }

                if temporal_filter
                    || (!shape.projection_filter && !shape.aggregate && !changelog_enrich)
                {
                    return Err(DbError::Pipeline(format!(
                        "stream '{}' cannot safely consume a changelog; supported consumers are \
                         a projection/filter, an aggregate, or a certified static-table enrich",
                        reg.name
                    )));
                }

                let emit_changelog = reg.incremental
                    || reg.emit_clause.as_ref().is_some_and(|emit| {
                        matches!(emit, laminar_sql::parser::EmitClause::Changes)
                    });
                let forwards_changelog = shape.projection_filter || changelog_enrich;
                if (forwards_changelog || (shape.aggregate && emit_changelog))
                    && changelog_carrying.insert(reg.name.clone())
                {
                    added = true;
                }
            }
            if !added {
                break;
            }
        }

        for reg in stream_regs.values().filter(|reg| reg.incremental) {
            if !changelog_carrying.contains(&reg.name) {
                return Err(DbError::Pipeline(format!(
                    "stream '{}' is registered as incremental but has no certified changelog \
                     output path",
                    reg.name
                )));
            }
        }
        for (name, schema) in &schemas {
            if !changelog_carrying.contains(name)
                && schema.fields().iter().any(|field| {
                    field
                        .name()
                        .eq_ignore_ascii_case(crate::aggregate_state::WEIGHT_COLUMN)
                })
            {
                return Err(DbError::Pipeline(format!(
                    "stream '{name}' is not a certified changelog producer but declares the reserved engine-owned '{}' column",
                    crate::aggregate_state::WEIGHT_COLUMN
                )));
            }
        }
        for name in &changelog_carrying {
            let schema = schemas.get_mut(name).expect("resolved above");
            *schema = advertise_changelog_schema(name, schema)?;
        }

        Ok(ResolvedStreamOutputs {
            schemas,
            changelog_carrying,
        })
    }
    .await;

    for name in &placeholders {
        let _ = ctx.deregister_table(exact_table_reference(name));
    }

    result
}

#[derive(Debug)]
pub(crate) struct ResolvedStreamOutputs {
    pub(crate) schemas: HashMap<String, arrow_schema::SchemaRef>,
    pub(crate) changelog_carrying: rustc_hash::FxHashSet<String>,
}

pub(super) struct StreamOutputShape {
    aggregate: bool,
    projection_filter: bool,
    planned_functions_immutable: bool,
}

pub(super) fn advertise_changelog_schema(
    stream: &str,
    schema: &arrow_schema::SchemaRef,
) -> Result<arrow_schema::SchemaRef, DbError> {
    use arrow_schema::{DataType, Field, Schema};

    let weight = crate::aggregate_state::WEIGHT_COLUMN;
    let matching = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name().eq_ignore_ascii_case(weight))
        .collect::<Vec<_>>();
    if let [(index, field)] = matching.as_slice() {
        if *index + 1 == schema.fields().len()
            && field.name() == weight
            && field.data_type() == &DataType::Int64
            && !field.is_nullable()
        {
            return Ok(Arc::clone(schema));
        }
        return Err(DbError::Pipeline(format!(
            "stream '{stream}' exposes reserved changelog column '{}' with type {:?} and \
             nullable={}; expected sole exact trailing non-null Int64 '{weight}'",
            field.name(),
            field.data_type(),
            field.is_nullable()
        )));
    }
    if !matching.is_empty() {
        return Err(DbError::Pipeline(format!(
            "stream '{stream}' exposes duplicate reserved changelog column '{weight}'"
        )));
    }

    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(weight, DataType::Int64, false)));
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}
