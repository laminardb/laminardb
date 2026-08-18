//! SQL analysis family: table-reference extraction, join and temporal shape detection,
//! changelog-weight admission, and projection rewriting for managed operator routing.
//!
//! Each child module owns one analysis product:
//!
//! - `table_refs` — source-table discovery (references, window TVFs, single-source checks);
//! - `changelog` — changelog `__weight` admission and changelog⋈static enrichment detection;
//! - `lookup` — partial lookup-enrich join detection and projection pushdown;
//! - `join` — bounded stream-join detection and flattened-pair projection rewriting;
//! - `temporal` — temporal-join post-projection validation and rewriting;
//! - `temporal_filter` — retracting `now()`-based temporal-filter recognition;
//! - `hazards` — cluster query-hazard scanning and managed window-source certification;
//! - `plan` — `DataFusion` plan inspection and result-batch shaping;
//! - `ai`, `frame` — managed AI-call and window-frame query routing;
//! - `ast`, `expr_sql` — parsing predicates and expression rendering shared by the above.

mod ai;
mod ast;
mod changelog;
mod expr_sql;
mod frame;
mod hazards;
mod join;
mod lookup;
mod plan;
mod table_refs;
mod temporal;
mod temporal_filter;

pub(crate) use ai::{detect_ai_functions, plan_ai_query, validate_ai_calls, AiQueryPlan};
pub(crate) use changelog::{
    detect_changelog_enrich_query, interval_output_has_nested_query,
    mutable_changelog_has_unsafe_modifiers, predicate_references_weight,
    projection_sql_preserving_weight, query_has_order_or_row_limit, query_references_weight,
    ChangelogEnrichConfig, CHANGELOG_ENRICH_TMP,
};
pub(crate) use frame::{plan_frame_query, FrameQueryPlan, FRAME_TMP_TABLE};
pub(crate) use hazards::{cluster_query_hazards, managed_core_window_source};
pub(crate) use join::{
    detect_stream_join_query, detect_unbounded_join_steps, has_join_clause,
    has_unaliased_projection, has_unqualified_interval_output_column, join_clause_count,
    StreamJoinDetection,
};
pub(crate) use lookup::{compute_lookup_projection, detect_lookup_enrich_query};
pub(crate) use plan::{
    apply_topk_filter, extract_projection_filter, planned_functions_are_immutable,
};
pub(crate) use table_refs::{extract_table_references, single_source_table};
pub(crate) use temporal::{
    has_temporal_query, temporal_projection_sql, temporal_projection_sql_for_input,
};
pub(crate) use temporal_filter::{
    analyze_temporal_filter, TemporalFilterAnalysis, TemporalFilterConfig,
};

#[cfg(test)]
pub(crate) use temporal_filter::TemporalBound;

#[cfg(test)]
pub(crate) use hazards::query_uses_runtime_clock;

#[cfg(test)]
use laminar_sql::parser::EmitStrategy as SqlEmitStrategy;

#[cfg(test)]
pub(crate) fn sql_emit_to_core(
    s: &SqlEmitStrategy,
) -> laminar_core::operator::window::EmitStrategy {
    use laminar_core::operator::window::EmitStrategy as CoreEmit;
    match s {
        SqlEmitStrategy::OnWatermark => CoreEmit::OnWatermark,
        SqlEmitStrategy::OnWindowClose => CoreEmit::OnWindowClose,
        SqlEmitStrategy::Periodic(d) => CoreEmit::Periodic(*d),
        SqlEmitStrategy::OnUpdate => CoreEmit::OnUpdate,
        SqlEmitStrategy::Changelog => CoreEmit::Changelog,
        SqlEmitStrategy::FinalOnly => CoreEmit::Final,
    }
}

#[cfg(test)]
pub(crate) fn emit_clause_to_core(
    clause: &laminar_sql::parser::EmitClause,
) -> Result<laminar_core::operator::window::EmitStrategy, laminar_sql::parser::ParseError> {
    let sql_strategy = clause.to_emit_strategy()?;
    Ok(sql_emit_to_core(&sql_strategy))
}

#[cfg(test)]
mod temporal_filter_recognition_tests;

#[cfg(test)]
mod frame_plan_tests;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod self_join_filter_tests;
