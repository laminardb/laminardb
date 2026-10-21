//! Per-stream exchange rules, applied over the catalog in dependency order. Each rule mirrors the
//! operator the graph builds for that stream.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use arrow::datatypes::{Field, Schema};
use datafusion::common::Column;
use datafusion::prelude::SessionContext;
use datafusion_expr::{Expr, LogicalPlan, Projection};
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::{JoinOperatorConfig, StreamJoinConfig, TemporalJoinTranslatorConfig};
use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    ChangelogKind, Distribution, EventTime, ExchangePlan, ExchangePlanError, InputPort, KeyColumn,
    PartitionKey, PortExchange, StreamPlan, StreamProperties, GLOBAL_VNODE,
};
use crate::connector_manager::StreamRegistration;
use crate::sql_analysis::ProjectionFilterInfo;

/// Derive the plan of every stream from the logical plans schema resolution built for it.
/// `changelog` names every changelog-carrying source and stream; `reference_tables` are lookup
/// inputs, not ports. Any other input that is not a stream is a source.
///
/// # Errors
/// Returns [`ExchangePlanError::Cycle`] when streams read each other's output, or
/// [`ExchangePlanError::Unplannable`] when a stream's query or join inputs cannot be resolved.
pub(crate) async fn derive(
    ctx: &SessionContext,
    streams: &HashMap<String, StreamRegistration>,
    logical_plans: &HashMap<String, LogicalPlan>,
    reference_tables: &FxHashSet<String>,
    changelog: &FxHashSet<String>,
    source_event_time: &FxHashMap<String, EventTime>,
) -> Result<ExchangePlan, ExchangePlanError> {
    let mut plan = ExchangePlan {
        streams: BTreeMap::new(),
        sources: source_event_time.clone(),
    };
    for registration in dependency_order(streams)? {
        let logical = logical_plans.get(&registration.name);
        let stream = plan_stream(
            ctx,
            registration,
            logical,
            reference_tables,
            changelog,
            &plan,
        )
        .await?;
        #[cfg(all(debug_assertions, feature = "cluster"))]
        super::shadow::assert_runtime_routing(ctx, registration, &stream).await;
        plan.streams.insert(registration.name.clone(), stream);
    }
    Ok(plan)
}

/// Kahn's order over stream-to-stream reads, ties broken by name.
pub(super) fn dependency_order(
    streams: &HashMap<String, StreamRegistration>,
) -> Result<Vec<&StreamRegistration>, ExchangePlanError> {
    let mut upstreams: BTreeMap<&str, BTreeSet<String>> = streams
        .values()
        .map(|registration| {
            let reads = crate::sql_analysis::extract_table_references(&registration.query_sql)
                .into_iter()
                .filter(|input| streams.contains_key(input))
                .collect();
            (registration.name.as_str(), reads)
        })
        .collect();
    let mut order = Vec::with_capacity(streams.len());
    // Each pass removes at least one stream or fails.
    while !upstreams.is_empty() {
        let ready: Vec<&str> = upstreams
            .iter()
            .filter(|(_, reads)| reads.is_empty())
            .map(|(name, _)| *name)
            .collect();
        if ready.is_empty() {
            return Err(ExchangePlanError::Cycle(
                upstreams.keys().map(ToString::to_string).collect(),
            ));
        }
        for name in &ready {
            upstreams.remove(name);
            order.push(&streams[*name]);
        }
        for reads in upstreams.values_mut() {
            reads.retain(|input| !ready.contains(&input.as_str()));
        }
    }
    Ok(order)
}

async fn plan_stream(
    ctx: &SessionContext,
    registration: &StreamRegistration,
    logical: Option<&LogicalPlan>,
    reference_tables: &FxHashSet<String>,
    changelog: &FxHashSet<String>,
    upstream: &ExchangePlan,
) -> Result<StreamPlan, ExchangePlanError> {
    let unplannable = |reason: String| ExchangePlanError::Unplannable {
        stream: registration.name.clone(),
        reason,
    };
    let sql = &registration.query_sql;
    let weighted = changelog.contains(&registration.name);

    let enrich = !changelog.is_empty()
        && crate::sql_analysis::detect_changelog_enrich_query(sql, changelog, reference_tables)
            .is_some();
    let join = if let Some(config) = temporal_join(registration) {
        Some((
            join_ports(
                ctx,
                (&config.left_table, &config.left_key_columns),
                (&config.right_table, &config.right_key_columns),
            )
            .await,
            Distribution::Arbitrary,
        ))
    } else if let Some(config) = interval_join(registration, enrich) {
        Some((
            join_ports(
                ctx,
                (&config.left_table, &config.left_keys),
                (&config.right_table, &config.right_keys),
            )
            .await,
            logical.map_or(Distribution::Arbitrary, |logical| {
                interval_join_output(logical, &config)
            }),
        ))
    } else {
        None
    };
    if let Some((inputs, distribution)) = join {
        return Ok(StreamPlan {
            inputs: inputs.map_err(unplannable)?,
            output: StreamProperties {
                distribution,
                changelog: if weighted {
                    ChangelogKind::Weighted
                } else {
                    ChangelogKind::AppendOnly
                },
                event_time: None,
            },
        });
    }

    let logical = logical.ok_or_else(|| unplannable("schema resolution built no plan".into()))?;
    let inputs: Vec<String> = crate::sql_analysis::extract_table_references(sql)
        .into_iter()
        .filter(|input| !reference_tables.contains(input))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let input_properties = |input: &str| upstream.upstream_properties(input, changelog);
    let closes_windows = closes_windows(registration.emit_clause.as_ref());

    if let (Some(aggregate), [input]) = (crate::aggregate_state::find_aggregate(logical), &*inputs)
    {
        let changelog = if weighted {
            ChangelogKind::Weighted
        } else if closes_windows {
            ChangelogKind::AppendOnly
        } else {
            ChangelogKind::RunningState
        };
        // An EOWC aggregate without a window runs on its local unmanaged path.
        if !closes_windows || registration.window_config.is_some() {
            return Ok(keyed_aggregate(
                logical,
                &aggregate.group_exprs,
                &aggregate.schema,
                (input, &input_properties(input).distribution),
                closes_windows,
                changelog,
            ));
        }
        return Ok(forwarding(
            &inputs,
            Distribution::Arbitrary,
            changelog,
            None,
        ));
    }

    let changelog = if weighted {
        ChangelogKind::Weighted
    } else if inputs
        .iter()
        .any(|input| input_properties(input).changelog == ChangelogKind::RunningState)
    {
        ChangelogKind::RunningState
    } else {
        ChangelogKind::AppendOnly
    };
    let forwarded = match (
        &*inputs,
        crate::sql_analysis::extract_projection_filter(logical),
    ) {
        ([input], Some(projection)) => stateless_output(input_properties(input), &projection),
        _ => (Distribution::Arbitrary, None),
    };
    Ok(forwarding(&inputs, forwarded.0, changelog, forwarded.1))
}

/// What a stateless projection keeps of its input: the hash key and the event-time column, when
/// each is forwarded unchanged.
fn stateless_output(
    input: StreamProperties,
    projection: &ProjectionFilterInfo,
) -> (Distribution, Option<EventTime>) {
    (
        forwarded_distribution(&input.distribution, projection),
        input.event_time.and_then(|event_time| {
            Some(EventTime {
                column: forwarded_name(&event_time.column, projection)?,
                ..event_time
            })
        }),
    )
}

const fn closes_windows(emit: Option<&EmitClause>) -> bool {
    matches!(emit, Some(EmitClause::OnWindowClose | EmitClause::Final))
}

fn temporal_join(registration: &StreamRegistration) -> Option<&TemporalJoinTranslatorConfig> {
    match registration.join_config.as_deref() {
        Some([JoinOperatorConfig::Temporal(config)]) => Some(config),
        _ => None,
    }
}

/// The graph detects the interval join from SQL unless the planner bound a different join kind
/// or the query is a changelog enrichment.
fn interval_join(registration: &StreamRegistration, enrich: bool) -> Option<StreamJoinConfig> {
    let detects = registration.join_config.as_ref().is_none_or(|configs| {
        configs
            .iter()
            .any(|config| matches!(config, JoinOperatorConfig::StreamStream(_)))
    });
    if enrich || !detects {
        return None;
    }
    crate::sql_analysis::detect_stream_join_query(&registration.query_sql)
        .map(|detection| detection.config)
}

/// Each side repartitions on its key columns, typed from the registered input schema.
async fn join_ports(
    ctx: &SessionContext,
    (left, left_keys): (&str, &[String]),
    (right, right_keys): (&str, &[String]),
) -> Result<BTreeMap<InputPort, PortExchange>, String> {
    Ok(BTreeMap::from([
        (
            InputPort::Left(left.to_string()),
            repartition(ctx, left, left_keys).await?,
        ),
        (
            InputPort::Right(right.to_string()),
            repartition(ctx, right, right_keys).await?,
        ),
    ]))
}

async fn repartition(
    ctx: &SessionContext,
    input: &str,
    columns: &[String],
) -> Result<PortExchange, String> {
    let provider = ctx
        .table_provider(crate::db::exact_table_reference(input))
        .await
        .map_err(|error| format!("input '{input}' is not registered: {error}"))?;
    let schema = provider.schema();
    let key = columns
        .iter()
        .map(|name| {
            let field = schema
                .field_with_name(name)
                .map_err(|_| format!("input '{input}' has no key column '{name}'"))?;
            Ok(KeyColumn::Column {
                name: name.clone(),
                data_type: field.data_type().clone(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(PortExchange::Repartition(PartitionKey(key)))
}

fn forwarding(
    inputs: &[String],
    distribution: Distribution,
    changelog: ChangelogKind,
    event_time: Option<EventTime>,
) -> StreamPlan {
    StreamPlan {
        inputs: inputs
            .iter()
            .map(|input| (InputPort::Input(input.clone()), PortExchange::Forward))
            .collect(),
        output: StreamProperties {
            distribution,
            changelog,
            event_time,
        },
    }
}

/// A managed aggregate routes on its group expressions; a window excludes its boundary markers.
/// Without a key it gathers to the global vnode, as partials for an aggregate and as raw rows for
/// a window. A keyed aggregate whose input is already hashed
/// on exactly that key is colocated; only the aggregate operator honours that, so windows always
/// repartition.
fn keyed_aggregate(
    logical: &LogicalPlan,
    group_exprs: &[Expr],
    schema: &Schema,
    (input, upstream): (&str, &Distribution),
    window: bool,
    changelog: ChangelogKind,
) -> StreamPlan {
    let groups: Vec<usize> = (0..group_exprs.len())
        .filter(|&group| !(window && is_window_marker(&group_exprs[group])))
        .collect();
    let (exchange, distribution) = if groups.is_empty() {
        (
            PortExchange::Gather {
                vnode: GLOBAL_VNODE,
                partial: !window,
            },
            Distribution::Singleton {
                vnode: GLOBAL_VNODE,
            },
        )
    } else {
        let key = PartitionKey(
            groups
                .iter()
                .map(|&group| group_key(&group_exprs[group], schema.field(group)))
                .collect(),
        );
        let colocated = !window && matches!(upstream, Distribution::Hash(hashed) if *hashed == key);
        (
            if colocated {
                PortExchange::Colocated(key)
            } else {
                PortExchange::Repartition(key)
            },
            aggregate_output_key(logical, &groups)
                .map_or(Distribution::Arbitrary, Distribution::Hash),
        )
    };
    StreamPlan {
        inputs: BTreeMap::from([(InputPort::Input(input.to_string()), exchange)]),
        output: StreamProperties {
            distribution,
            changelog,
            event_time: None,
        },
    }
}

fn unaliased(mut expr: &Expr) -> &Expr {
    while let Expr::Alias(alias) = expr {
        expr = &alias.expr;
    }
    expr
}

fn is_window_marker(expr: &Expr) -> bool {
    matches!(
        unaliased(expr),
        Expr::ScalarFunction(function)
            if matches!(function.func.name(), "tumble" | "tumble_end" | "hop" | "hop_end" | "session")
    )
}

fn group_key(expr: &Expr, field: &Field) -> KeyColumn {
    let data_type = field.data_type().clone();
    match unaliased(expr) {
        Expr::Column(column) => KeyColumn::Column {
            name: column.name.clone(),
            data_type,
        },
        expr => KeyColumn::Computed {
            expr: expr.to_string(),
            data_type,
        },
    }
}

/// The output columns that forward each group unchanged, or `None` when any group is not
/// projected as a bare column.
fn aggregate_output_key(logical: &LogicalPlan, groups: &[usize]) -> Option<PartitionKey> {
    let projection = crate::aggregate_state::find_post_aggregate_projection(logical).ok()?;
    let columns = groups
        .iter()
        .map(|&group| {
            let index = match projection {
                None => group,
                Some(projection) => forwarding_output(projection, group)?,
            };
            Some(output_column(logical, index))
        })
        .collect::<Option<Vec<_>>>()?;
    Some(PartitionKey(columns))
}

/// An interval join emits every row on the owner of the key of a side the row always carries:
/// either side for inner matches, the preserved side for outer, semi and anti joins. A full outer
/// join's unmatched rows carry only one side, so its output has no key.
fn interval_join_output(logical: &LogicalPlan, config: &StreamJoinConfig) -> Distribution {
    use laminar_sql::parser::join_parser::JoinType;

    let left = (true, config.left_keys.as_slice());
    let right = (false, config.right_keys.as_slice());
    let sides: &[(bool, &[String])] = match config.join_type {
        JoinType::Inner => &[left, right],
        JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => &[left],
        JoinType::Right | JoinType::RightSemi | JoinType::RightAnti => &[right],
        JoinType::Full => &[],
    };
    let LogicalPlan::Projection(projection) = logical else {
        return Distribution::Arbitrary;
    };
    let mut below = projection.input.as_ref();
    while let LogicalPlan::Filter(filter) = below {
        below = filter.input.as_ref();
    }
    let LogicalPlan::Join(join) = below else {
        return Distribution::Arbitrary;
    };
    sides
        .iter()
        .find_map(|&(is_left, keys)| {
            let side = if is_left { &join.left } else { &join.right };
            keys.iter()
                .map(|name| {
                    let (qualifier, _) = side
                        .schema()
                        .qualified_field_with_unqualified_name(name)
                        .ok()?;
                    let input = projection
                        .input
                        .schema()
                        .index_of_column(&Column::new(qualifier.cloned(), name))
                        .ok()?;
                    Some(output_column(
                        logical,
                        forwarding_output(projection, input)?,
                    ))
                })
                .collect::<Option<Vec<_>>>()
        })
        .map_or(Distribution::Arbitrary, |key| {
            Distribution::Hash(PartitionKey(key))
        })
}

/// The output position that forwards input column `input` of `projection` unchanged.
fn forwarding_output(projection: &Projection, input: usize) -> Option<usize> {
    projection.expr.iter().position(|expr| {
        matches!(
            unaliased(expr),
            Expr::Column(column)
                if projection.input.schema().index_of_column(column).ok() == Some(input)
        )
    })
}

fn output_column(logical: &LogicalPlan, index: usize) -> KeyColumn {
    let field = logical.schema().field(index);
    KeyColumn::Column {
        name: field.name().clone(),
        data_type: field.data_type().clone(),
    }
}

/// A stateless projection keeps its input's hash key when every key column is forwarded as a bare
/// column; any other key is lost.
fn forwarded_distribution(input: &Distribution, projection: &ProjectionFilterInfo) -> Distribution {
    let Distribution::Hash(key) = input else {
        return input.clone();
    };
    key.0
        .iter()
        .map(|column| forwarded_column(column, projection))
        .collect::<Option<Vec<_>>>()
        .map_or(Distribution::Arbitrary, |columns| {
            Distribution::Hash(PartitionKey(columns))
        })
}

fn forwarded_column(column: &KeyColumn, projection: &ProjectionFilterInfo) -> Option<KeyColumn> {
    let KeyColumn::Column { name, data_type } = column else {
        return None;
    };
    Some(KeyColumn::Column {
        name: forwarded_name(name, projection)?,
        data_type: data_type.clone(),
    })
}

/// The output name under which a stateless projection forwards input column `name` unchanged.
fn forwarded_name(name: &str, projection: &ProjectionFilterInfo) -> Option<String> {
    let source = projection
        .input_df_schema
        .index_of_column_by_name(None, name)?;
    projection.proj_exprs.iter().find_map(|expr| {
        let output = match expr {
            Expr::Alias(alias) => &alias.name,
            Expr::Column(column) => &column.name,
            _ => return None,
        };
        let Expr::Column(forwarded) = unaliased(expr) else {
            return None;
        };
        (projection.input_df_schema.index_of_column(forwarded).ok() == Some(source))
            .then(|| output.clone())
    })
}
