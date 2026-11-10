//! Debug-build check that a derived aggregate or window port routes on exactly the columns the
//! managed state's shuffle hashes, and gathers partials exactly when the state ships them. The
//! state is built against the input schemas the operator graph installs. A failure is a
//! derivation bug, never a reason to loosen the check.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::prelude::SessionContext;
use laminar_core::state::DEFAULT_KEY_GROUP_COUNT;
use laminar_sql::parser::EmitClause;
use rustc_hash::FxHashSet;

use super::{InputPort, KeyColumn, PortExchange, StreamPlan};
use crate::aggregate_state::{CompiledProjection, IncrementalAggState};
use crate::connector_manager::StreamRegistration;
use crate::core_window_state::CoreWindowState;

pub(super) async fn assert_runtime_routing(
    ctx: &SessionContext,
    registration: &StreamRegistration,
    stream: &StreamPlan,
    changelog: &FxHashSet<String>,
) {
    let route = stream
        .inputs
        .iter()
        .find_map(|(port, exchange)| match (port, exchange) {
            (
                InputPort::Input(_),
                PortExchange::Colocated(key) | PortExchange::Repartition(key),
            ) => Some((key.0.as_slice(), false)),
            (InputPort::Input(_), &PortExchange::Gather { partial, .. }) => {
                Some((&[][..], partial))
            }
            _ => None,
        });
    let Some((keys, partial)) = route else {
        return;
    };
    let stream = &registration.name;
    let inputs = graph_inputs(ctx, &registration.query_sql, changelog).await;
    let Some(runtime) = runtime_routing(&inputs, registration).await else {
        return;
    };
    assert_eq!(
        runtime.group_columns,
        keys.len(),
        "stream '{stream}': plan routes on {keys:?}, managed state on {} group columns",
        runtime.group_columns
    );
    assert_eq!(
        runtime.gathers_partials, partial,
        "stream '{stream}': plan gathers partials: {partial}, managed state ships them: {}",
        runtime.gathers_partials
    );
    let Some(schema) = runtime.pre_aggregate else {
        return;
    };
    for (field, key) in schema.fields().iter().zip(keys) {
        let matches = match key {
            KeyColumn::Column { name, data_type } => {
                field.name() == name && field.data_type() == data_type
            }
            KeyColumn::Computed { data_type, .. } => field.data_type() == data_type,
        };
        assert!(
            matches,
            "stream '{stream}': plan key {key} differs from managed pre-aggregate column {field:?}"
        );
    }
}

/// `sql`'s inputs as the graph installs them: each changelog input with its trailing weight.
async fn graph_inputs(
    ctx: &SessionContext,
    sql: &str,
    changelog: &FxHashSet<String>,
) -> SessionContext {
    let mut schemas = HashMap::new();
    for input in crate::sql_analysis::extract_table_references(sql) {
        if !changelog.contains(&input) {
            continue;
        }
        let Ok(provider) = ctx
            .table_provider(crate::db::exact_table_reference(&input))
            .await
        else {
            continue;
        };
        let schema =
            crate::pipeline_lifecycle::advertise_changelog_schema(&input, &provider.schema())
                .expect("a changelog input's registered schema admits its trailing weight");
        schemas.insert(input, schema);
    }
    crate::pipeline_lifecycle::graph_input_context(ctx, sql, &schemas)
        .await
        .expect("the graph's inputs register in a fresh context")
}

/// How the state the graph builds routes its input.
struct RuntimeRouting {
    group_columns: usize,
    gathers_partials: bool,
    pre_aggregate: Option<SchemaRef>,
}

/// The routing of the state the graph builds, or `None` when that state rejects the query: such
/// a stream fails before it routes any row.
async fn runtime_routing(
    ctx: &SessionContext,
    registration: &StreamRegistration,
) -> Option<RuntimeRouting> {
    let sql = &registration.query_sql;
    let emit = registration.emit_clause.as_ref();
    let pre_aggregate =
        |projection: Option<&CompiledProjection>| projection.map(|p| Arc::clone(&p.output_schema));
    if let (Some(window), Some(EmitClause::OnWindowClose | EmitClause::Final)) =
        (&registration.window_config, emit)
    {
        let state = CoreWindowState::try_from_sql(ctx, sql, window, emit, DEFAULT_KEY_GROUP_COUNT)
            .await
            .ok()?
            .expect("plan routes a window the managed window state does not recognize");
        return Some(RuntimeRouting {
            group_columns: state.num_group_cols(),
            gathers_partials: false,
            pre_aggregate: pre_aggregate(state.compiled_projection()),
        });
    }
    let emit_changelog = registration.incremental || matches!(emit, Some(EmitClause::Changes));
    let state =
        IncrementalAggState::try_from_sql(ctx, sql, emit_changelog, DEFAULT_KEY_GROUP_COUNT)
            .await
            .ok()?
            .expect("plan routes an aggregate the managed aggregate state does not recognize");
    Some(RuntimeRouting {
        group_columns: state.num_group_cols(),
        gathers_partials: state.gathers_partials(),
        pre_aggregate: pre_aggregate(state.compiled_projection()),
    })
}
