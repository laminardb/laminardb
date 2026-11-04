//! Per-stream exchange rules, applied over the catalog in dependency order. Each rule mirrors the
//! operator the graph builds for that stream.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use arrow::datatypes::{Field, Schema};
use datafusion::common::Column;
use datafusion::prelude::SessionContext;
use datafusion_expr::{Expr, Join, LogicalPlan, Projection};
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::{JoinOperatorConfig, StreamJoinConfig, TemporalJoinTranslatorConfig};
use rustc_hash::{FxHashMap, FxHashSet};

use super::event_time::{interval_join_event_time, window_event_time};
use super::{
    ChangelogKind, Distribution, EventTime, ExchangePlan, ExchangePlanError, InputPort, JoinSide,
    KeyColumn, PartitionKey, PortExchange, StreamPlan, StreamProperties, WindowBoundary,
    GLOBAL_VNODE,
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
    if let Some((inputs, distribution, event_time)) =
        join_plan(ctx, registration, logical, enrich, changelog, upstream).await
    {
        return Ok(StreamPlan {
            inputs: inputs.map_err(unplannable)?,
            output: StreamProperties {
                distribution,
                changelog: if weighted {
                    ChangelogKind::Weighted
                } else {
                    ChangelogKind::AppendOnly
                },
                event_time,
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
            let input_properties = input_properties(input);
            let event_time = registration
                .window_config
                .as_ref()
                .filter(|_| closes_windows)
                .and_then(|window| {
                    window_event_time(logical, window, input_properties.event_time.as_ref())
                });
            return Ok(keyed_aggregate(
                logical,
                &aggregate.group_exprs,
                &aggregate.schema,
                (input, &input_properties.distribution),
                closes_windows,
                (changelog, event_time),
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

/// A join's input ports, output distribution and event time: a planner-bound temporal join, or an
/// interval join the graph detects from SQL.
async fn join_plan(
    ctx: &SessionContext,
    registration: &StreamRegistration,
    logical: Option<&LogicalPlan>,
    enrich: bool,
    changelog: &FxHashSet<String>,
    upstream: &ExchangePlan,
) -> Option<(
    Result<BTreeMap<InputPort, PortExchange>, String>,
    Distribution,
    Option<EventTime>,
)> {
    if let Some(config) = temporal_join(registration) {
        Some((
            join_ports(
                ctx,
                (&config.left_table, &config.left_key_columns),
                (&config.right_table, &config.right_key_columns),
            )
            .await,
            Distribution::Arbitrary,
            None,
        ))
    } else if let Some(config) = interval_join(registration, enrich) {
        let side_event_time =
            |input: &str| upstream.upstream_properties(input, changelog).event_time;
        let (left, right) = (
            side_event_time(&config.left_table),
            side_event_time(&config.right_table),
        );
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
            logical.and_then(|logical| {
                interval_join_event_time(logical, &config, (left.as_ref(), right.as_ref()))
            }),
        ))
    } else {
        None
    }
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
    (changelog, event_time): (ChangelogKind, Option<EventTime>),
) -> StreamPlan {
    let groups: Vec<usize> = (0..group_exprs.len())
        .filter(|&group| !(window && window_boundary(&group_exprs[group]).is_some()))
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
            event_time,
        },
    }
}

fn unaliased(mut expr: &Expr) -> &Expr {
    while let Expr::Alias(alias) = expr {
        expr = &alias.expr;
    }
    expr
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

pub(crate) fn carried_window_boundary(logical: &LogicalPlan) -> WindowBoundary {
    window_boundary_column(logical).map_or(WindowBoundary::Start, |(_, boundary)| boundary)
}

/// The output column of the first window end marker forwarded as a bare column, otherwise of the
/// first start marker.
pub(super) fn window_boundary_column(logical: &LogicalPlan) -> Option<(usize, WindowBoundary)> {
    let forwarded = forwarded_window_boundaries(logical);
    forwarded
        .iter()
        .find(|(_, boundary)| *boundary == WindowBoundary::End)
        .or_else(|| forwarded.first())
        .copied()
}

/// The output column of the first window marker group forwarded as a bare column. An append view
/// records its segments' event-time bounds on this column, so it stays the view's retention column
/// even when the stream carries the other boundary.
pub(crate) fn window_retention_column(logical: &LogicalPlan) -> Option<(usize, WindowBoundary)> {
    forwarded_window_boundaries(logical).first().copied()
}

/// Each window marker group forwarded as a bare column, in group order, with its output column.
fn forwarded_window_boundaries(logical: &LogicalPlan) -> Vec<(usize, WindowBoundary)> {
    let Some(aggregate) = crate::aggregate_state::find_aggregate(logical) else {
        return Vec::new();
    };
    let Ok(projection) = crate::aggregate_state::find_post_aggregate_projection(logical) else {
        return Vec::new();
    };
    aggregate
        .group_exprs
        .iter()
        .enumerate()
        .filter_map(|(group, expr)| {
            let boundary = window_boundary(expr)?;
            let index = match projection {
                None => group,
                Some(projection) => forwarding_output(projection, group)?,
            };
            Some((index, boundary))
        })
        .collect()
}

fn window_boundary(expr: &Expr) -> Option<WindowBoundary> {
    let Expr::ScalarFunction(function) = unaliased(expr) else {
        return None;
    };
    match function.func.name() {
        "tumble_end" | "hop_end" => Some(WindowBoundary::End),
        "tumble" | "hop" | "session" => Some(WindowBoundary::Start),
        _ => None,
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
    let Some((projection, join)) = interval_join_plan(logical) else {
        return Distribution::Arbitrary;
    };
    JoinSide::present_on_every_row(config.join_type)
        .iter()
        .find_map(|&side| {
            let keys = match side {
                JoinSide::Left => &config.left_keys,
                JoinSide::Right => &config.right_keys,
            };
            keys.iter()
                .map(|name| {
                    let index = projected_join_column(projection, join, side, name)?;
                    Some(output_column(logical, index))
                })
                .collect::<Option<Vec<_>>>()
        })
        .map_or(Distribution::Arbitrary, |key| {
            Distribution::Hash(PartitionKey(key))
        })
}

/// The projection over an interval join statement's (filtered) join.
pub(super) fn interval_join_plan(logical: &LogicalPlan) -> Option<(&Projection, &Join)> {
    let LogicalPlan::Projection(projection) = logical else {
        return None;
    };
    let mut below = projection.input.as_ref();
    while let LogicalPlan::Filter(filter) = below {
        below = filter.input.as_ref();
    }
    let LogicalPlan::Join(join) = below else {
        return None;
    };
    Some((projection, join))
}

/// The output position that forwards `side`'s column `name` unchanged.
pub(super) fn projected_join_column(
    projection: &Projection,
    join: &Join,
    side: JoinSide,
    name: &str,
) -> Option<usize> {
    let input = match side {
        JoinSide::Left => &join.left,
        JoinSide::Right => &join.right,
    };
    let (qualifier, _) = input
        .schema()
        .qualified_field_with_unqualified_name(name)
        .ok()?;
    let column = projection
        .input
        .schema()
        .index_of_column(&Column::new(qualifier.cloned(), name))
        .ok()?;
    forwarding_output(projection, column)
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
