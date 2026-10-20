//! Debug-build check that a derived aggregate or window port routes on exactly the columns the
//! managed state's shuffle hashes. A failure is a derivation bug, never a reason to loosen the
//! check.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::prelude::SessionContext;
use laminar_core::state::DEFAULT_KEY_GROUP_COUNT;
use laminar_sql::parser::EmitClause;

use super::{InputPort, KeyColumn, PortExchange, StreamPlan};
use crate::aggregate_state::{CompiledProjection, IncrementalAggState};
use crate::connector_manager::StreamRegistration;
use crate::core_window_state::CoreWindowState;

pub(super) async fn assert_runtime_routing(
    ctx: &SessionContext,
    registration: &StreamRegistration,
    stream: &StreamPlan,
) {
    let keys = stream
        .inputs
        .iter()
        .find_map(|(port, exchange)| match (port, exchange) {
            (
                InputPort::Input(_),
                PortExchange::Colocated(key) | PortExchange::Repartition(key),
            ) => Some(key.0.as_slice()),
            (InputPort::Input(_), PortExchange::Gather { .. }) => Some(&[][..]),
            _ => None,
        });
    let Some(keys) = keys else {
        return;
    };
    let stream = &registration.name;
    let Some((group_columns, pre_aggregate)) = runtime_group_columns(ctx, registration).await
    else {
        return;
    };
    assert_eq!(
        group_columns,
        keys.len(),
        "stream '{stream}': plan routes on {keys:?}, managed state on {group_columns} group columns"
    );
    let Some(schema) = pre_aggregate else {
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

/// Group-column count and compiled pre-aggregation schema of the state the graph builds, or
/// `None` when that state rejects the query: such a stream fails before it routes any row.
async fn runtime_group_columns(
    ctx: &SessionContext,
    registration: &StreamRegistration,
) -> Option<(usize, Option<SchemaRef>)> {
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
        return Some((
            state.num_group_cols(),
            pre_aggregate(state.compiled_projection()),
        ));
    }
    let emit_changelog = registration.incremental || matches!(emit, Some(EmitClause::Changes));
    let state =
        IncrementalAggState::try_from_sql(ctx, sql, emit_changelog, DEFAULT_KEY_GROUP_COUNT)
            .await
            .ok()?
            .expect("plan routes an aggregate the managed aggregate state does not recognize");
    Some((
        state.num_group_cols(),
        pre_aggregate(state.compiled_projection()),
    ))
}
