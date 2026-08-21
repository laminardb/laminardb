#![deny(clippy::disallowed_types)]

//! Core window state for tumbling/hopping/session aggregate queries.
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::Arc;

use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};

use arrow::array::ArrayRef;
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_optimizer::analyzer::type_coercion::TypeCoercionRewriter;

use laminar_core::operator::sliding_window::SlidingWindowAssigner;
use laminar_core::operator::window::TumblingWindowAssigner;
use laminar_core::state::{KeyGroupCount, PartitionKeyCodecV1};
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::{WindowOperatorConfig, WindowType};

use crate::aggregate_state::{
    apply_compiled_having, compile_having_filter, extract_clauses, find_aggregate,
    query_fingerprint_with_config, AggFuncSpec, CompiledProjection, GroupCheckpoint, PreAggBuilder,
    WindowCheckpoint,
};
use crate::error::DbError;

/// Sentinel for null timestamps; callers must skip these rows rather than
/// assigning them to the epoch-zero window.
const NULL_TIMESTAMP: i64 = i64::MIN;
// Conservative sparse-node envelope used for managed-state admission, not RSS reporting.
const BTREE_ENTRY_CHARGE: usize = 512;
const MAX_HOP_WINDOWS_PER_EVENT: i64 = 128;

fn extract_i64_timestamps(batch: &RecordBatch, col_index: usize) -> Result<Vec<i64>, DbError> {
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::TimeUnit;

    let col = batch.column(col_index);
    let mut result = Vec::with_capacity(batch.num_rows());

    match col.data_type() {
        DataType::Int64 => {
            let arr = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DbError::Pipeline("expected Int64Array".to_string()))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    result.push(NULL_TIMESTAMP);
                } else {
                    let timestamp = arr.value(i);
                    if timestamp == NULL_TIMESTAMP {
                        return Err(DbError::PipelineTerminal(
                            "event timestamp i64::MIN is reserved for null values".into(),
                        ));
                    }
                    result.push(timestamp);
                }
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .ok_or_else(|| {
                    DbError::Pipeline("expected TimestampMillisecondArray".to_string())
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    result.push(NULL_TIMESTAMP);
                } else {
                    let timestamp = arr.value(i);
                    if timestamp == NULL_TIMESTAMP {
                        return Err(DbError::PipelineTerminal(
                            "event timestamp i64::MIN is reserved for null values".into(),
                        ));
                    }
                    result.push(timestamp);
                }
            }
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampSecondArray>()
                .ok_or_else(|| DbError::Pipeline("expected TimestampSecondArray".to_string()))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    result.push(NULL_TIMESTAMP);
                } else {
                    let seconds = arr.value(i);
                    result.push(seconds.checked_mul(1000).ok_or_else(|| {
                        DbError::PipelineTerminal(format!(
                            "event timestamp {seconds}s does not fit millisecond precision"
                        ))
                    })?);
                }
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    DbError::Pipeline("expected TimestampMicrosecondArray".to_string())
                })?;
            for i in 0..arr.len() {
                result.push(if arr.is_null(i) {
                    NULL_TIMESTAMP
                } else {
                    arr.value(i).div_euclid(1000)
                });
            }
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .ok_or_else(|| {
                    DbError::Pipeline("expected TimestampNanosecondArray".to_string())
                })?;
            for i in 0..arr.len() {
                result.push(if arr.is_null(i) {
                    NULL_TIMESTAMP
                } else {
                    arr.value(i).div_euclid(1_000_000)
                });
            }
        }
        other => {
            return Err(DbError::Pipeline(format!(
                "unsupported timestamp type for EOWC: {other}"
            )));
        }
    }

    Ok(result)
}

enum CoreWindowAssigner {
    Tumbling(TumblingWindowAssigner),
    Hopping(SlidingWindowAssigner),
    Session { gap_ms: i64 },
}

#[derive(Clone, Copy)]
enum GroupOutputSource {
    Key(usize),
    WindowStart,
    WindowEnd,
}

enum WindowBoundaryValues<'a> {
    Fixed { start: i64, end: i64, rows: usize },
    PerRow { starts: &'a [i64], ends: &'a [i64] },
}

fn window_group_source(
    expr: &datafusion_expr::Expr,
    assigner: &CoreWindowAssigner,
) -> Result<Option<GroupOutputSource>, DbError> {
    let expr = match expr {
        datafusion_expr::Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    };
    let datafusion_expr::Expr::ScalarFunction(function) = expr else {
        return Ok(None);
    };
    let name = function.func.name();
    let source = match name {
        "tumble" if matches!(assigner, CoreWindowAssigner::Tumbling(_)) => {
            Some(GroupOutputSource::WindowStart)
        }
        "tumble_end" if matches!(assigner, CoreWindowAssigner::Tumbling(_)) => {
            Some(GroupOutputSource::WindowEnd)
        }
        "hop" if matches!(assigner, CoreWindowAssigner::Hopping(_)) => {
            Some(GroupOutputSource::WindowStart)
        }
        "hop_end" if matches!(assigner, CoreWindowAssigner::Hopping(_)) => {
            Some(GroupOutputSource::WindowEnd)
        }
        "session" if matches!(assigner, CoreWindowAssigner::Session { .. }) => {
            Some(GroupOutputSource::WindowStart)
        }
        "tumble" | "tumble_end" | "hop" | "hop_end" | "session" => {
            return Err(DbError::Unsupported(format!(
                "[{}] SQL window marker `{name}` does not match the configured window",
                laminar_core::error_codes::SQL_UNSUPPORTED
            )));
        }
        _ => None,
    };
    Ok(source)
}

/// Pre-compiled post-aggregate projection (e.g., `SUM(a)/SUM(b) AS ratio`).
struct PostProjection {
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    final_schema: SchemaRef,
}

/// A `WHERE` predicate calling `now()`: kept logical and re-resolved per cycle
/// because a statically compiled `now()` errors at `evaluate()`.
struct NowWhereFilter {
    predicate: datafusion_expr::Expr,
    df_schema: Arc<DFSchema>,
}

fn is_wallclock_fn(name: &str) -> bool {
    name.eq_ignore_ascii_case("now") || name.eq_ignore_ascii_case("current_timestamp")
}

fn expr_uses_wallclock(expr: &datafusion_expr::Expr) -> bool {
    let mut found = false;
    let _ = expr.apply(|e| {
        if let datafusion_expr::Expr::ScalarFunction(f) = e {
            if is_wallclock_fn(f.func.name()) {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Substitute `now()`/`current_timestamp()` with a fixed nanosecond literal
/// matching `DataFusion`'s return type so coercion casts stay valid.
fn substitute_wallclock(
    expr: datafusion_expr::Expr,
    now_ns: i64,
) -> Result<datafusion_expr::Expr, DbError> {
    expr.transform(|e| {
        if let datafusion_expr::Expr::ScalarFunction(ref f) = e {
            if is_wallclock_fn(f.func.name()) {
                return Ok(Transformed::yes(datafusion_expr::Expr::Literal(
                    ScalarValue::TimestampNanosecond(Some(now_ns), Some(Arc::from("+00:00"))),
                    None,
                )));
            }
        }
        Ok(Transformed::no(e))
    })
    .map(|t| t.data)
    .map_err(|e| DbError::Pipeline(format!("[LDB-1002] now() substitution failed: {e}")))
}

struct SessionAccState {
    start: i64,
    end: i64,
    accs: Vec<Box<dyn datafusion_expr::Accumulator>>,
}

struct SessionGroupState {
    sessions: BTreeMap<i64, SessionAccState>,
}

type SessionGroupKey = Arc<arrow::row::OwnedRow>;

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct SessionDeadline {
    deadline_ms: i64,
    session_start: i64,
    key: SessionGroupKey,
}

impl SessionDeadline {
    fn new(
        key: SessionGroupKey,
        session_start: i64,
        session_end: i64,
        allowed_lateness_ms: i64,
    ) -> Self {
        Self {
            deadline_ms: session_end.saturating_add(allowed_lateness_ms),
            session_start,
            key,
        }
    }

    const fn accounted_state_bytes(&self) -> usize {
        BTREE_ENTRY_CHARGE
    }
}

type FixedWindowGroups =
    FxHashMap<arrow::row::OwnedRow, Vec<Box<dyn datafusion_expr::Accumulator>>>;
type FixedWindows = BTreeMap<i64, FixedWindowGroups>;
type SessionGroups = FxHashMap<SessionGroupKey, SessionGroupState>;

struct PreparedCoreWindowRestore {
    state: Option<Box<CoreWindowVnodeState>>,
    frontier_floor_ms: i64,
}

struct CoreWindowVnodeState {
    windows: FixedWindows,
    session_groups: SessionGroups,
    session_deadlines: BTreeSet<SessionDeadline>,
    accounted_state_bytes: usize,
}

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct SessionCheckpoint {
    pub start: i64,
    pub end: i64,
    pub acc_states: Vec<Vec<u8>>,
}

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct SessionGroupCheckpoint {
    pub key: Vec<u8>,
    pub sessions: Vec<SessionCheckpoint>,
}

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct CoreWindowVnodeCheckpoint {
    pub fingerprint: u64,
    pub vnode: u32,
    pub windows: Vec<WindowCheckpoint>,
    pub session_state: Vec<SessionGroupCheckpoint>,
    pub window_type: u8,
    pub frontier_floor_ms: i64,
}

struct CapturedCoreWindowGroup {
    key: arrow::row::OwnedRow,
    accumulator_states: Vec<Vec<ScalarValue>>,
}

struct CapturedFixedWindow {
    window_start: i64,
    groups: Vec<CapturedCoreWindowGroup>,
}

struct CapturedSession {
    start: i64,
    end: i64,
    accumulator_states: Vec<Vec<ScalarValue>>,
}

struct CapturedSessionGroup {
    key: SessionGroupKey,
    sessions: Vec<CapturedSession>,
}

pub(crate) struct CoreWindowVnodeCheckpointCapture {
    fingerprint: u64,
    vnode: u32,
    frontier_floor_ms: i64,
    window_type: u8,
    group_types: Arc<[DataType]>,
    row_converter: Arc<arrow::row::RowConverter>,
    windows: Vec<CapturedFixedWindow>,
    session_state: Vec<CapturedSessionGroup>,
    retained_bytes: usize,
}

pub(crate) struct OwnedCoreWindowVnodeRestore {
    pub(crate) vnode: u32,
    pub(crate) state: CoreWindowVnodeCheckpoint,
}

pub(crate) struct PreparedCoreWindowVnodeTransition {
    replacements: Vec<(u32, Option<Box<CoreWindowVnodeState>>)>,
    final_active_vnodes: Vec<u32>,
    final_active_vnode_positions: Box<[usize]>,
    final_window_group_counts: FxHashMap<i64, usize>,
    final_session_group_count: usize,
    final_high_watermark_ms: i64,
    final_required_frontier_floor_ms: i64,
}

pub(crate) struct RetiredCoreWindowVnodeTransition {
    retired_state: PreparedCoreWindowVnodeTransition,
}

pub(crate) struct PreflightedCoreWindowVnodeArchive<'a> {
    pub(crate) checkpoint: &'a ArchivedCoreWindowVnodeCheckpoint,
}

/// Core window state for tumbling/hopping/session aggregate queries.
pub(crate) struct CoreWindowState {
    assigner: CoreWindowAssigner,
    key_group_count: KeyGroupCount,
    vnode_states: Box<[Option<Box<CoreWindowVnodeState>>]>,
    active_vnodes: Vec<u32>,
    active_vnode_positions: Box<[usize]>,
    window_group_counts: FxHashMap<i64, usize>,
    session_group_count: usize,
    row_converter: Arc<arrow::row::RowConverter>,
    agg_specs: Vec<AggFuncSpec>,
    num_group_cols: usize,
    group_types: Arc<[DataType]>,
    query_sql: String,
    #[cfg(test)]
    pre_agg_sql: String,
    time_col_index: usize,
    output_schema: SchemaRef,
    state_output_schema: SchemaRef,
    group_output_sources: Vec<GroupOutputSource>,
    compiled_projection: Option<CompiledProjection>,
    planned_functions_immutable: bool,
    // Built once; LiveSourceExec leaves carry fresh data per execute.
    cached_pre_agg_physical: Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    // Set when WHERE references `now()`; resolved per cycle.
    now_where: Option<NowWhereFilter>,
    // Compiled `now()` predicate keyed by the second it was compiled for.
    // Err caches a compile failure; retries at the next second roll.
    #[allow(clippy::type_complexity)]
    now_filter_cache: Option<(i64, Result<Arc<dyn PhysicalExpr>, String>)>,
    having_filter: Option<Arc<dyn PhysicalExpr>>,
    max_groups_per_window: usize,
    allowed_lateness_ms: i64,
    high_watermark_ms: i64,
    post_projection: Option<PostProjection>,
    prom: Option<Arc<crate::engine_metrics::EngineMetrics>>,
    scratch_nogroup: FxHashMap<i64, Vec<u32>>,
    // Group ids are dense within a batch and index into scratch_group_keys.
    scratch_grouped: FxHashMap<(u32, i64, u32), Vec<u32>>,
    scratch_group_keys: indexmap::IndexSet<arrow::row::OwnedRow, FxBuildHasher>,
    checkpoint_dirty_vnodes: Box<[bool]>,
    checkpoint_dirty_vnode_roster: Vec<u32>,
    full_vnode_capture_required: bool,
    required_frontier_floor_ms: i64,
}

impl CoreWindowState {
    /// Build state from SQL; returns `None` if the query is not a windowed
    /// aggregate that can be routed through the core pipeline.
    #[allow(clippy::too_many_lines)]
    pub async fn try_from_sql(
        ctx: &SessionContext,
        sql: &str,
        window_config: &WindowOperatorConfig,
        emit_clause: Option<&EmitClause>,
        key_group_count: KeyGroupCount,
    ) -> Result<Option<Self>, DbError> {
        let size_ms = i64::try_from(window_config.size.as_millis()).map_err(|_| {
            DbError::Unsupported(format!(
                "[{}] window size exceeds the i64 millisecond timestamp range",
                laminar_core::error_codes::SQL_UNSUPPORTED,
            ))
        })?;

        let offset_ms = window_config.offset_ms;
        let assigner = match window_config.window_type {
            WindowType::Cumulate => {
                return Err(DbError::Unsupported(
                    "CUMULATE windows are not yet supported in the streaming pipeline. \
                     Use TUMBLE or HOP instead."
                        .into(),
                ));
            }
            WindowType::Tumbling => {
                if size_ms <= 0 {
                    return Ok(None);
                }
                CoreWindowAssigner::Tumbling(
                    TumblingWindowAssigner::from_millis(size_ms).with_offset_ms(offset_ms),
                )
            }
            WindowType::Sliding => {
                let slide_ms = i64::try_from(
                    window_config
                        .slide
                        .unwrap_or(window_config.size)
                        .as_millis(),
                )
                .map_err(|_| {
                    DbError::Unsupported(format!(
                        "[{}] hopping window slide exceeds the i64 millisecond timestamp range",
                        laminar_core::error_codes::SQL_UNSUPPORTED,
                    ))
                })?;
                if size_ms <= 0 || slide_ms <= 0 || slide_ms > size_ms {
                    return Ok(None);
                }
                let wpe = (size_ms - 1) / slide_ms + 1;
                if wpe > MAX_HOP_WINDOWS_PER_EVENT {
                    return Err(DbError::Unsupported(format!(
                        "[{}] hopping window size/slide ratio is {wpe} (size={size_ms}ms, \
                         slide={slide_ms}ms); each event would be assigned to that many \
                         open windows. Cap is {MAX_HOP_WINDOWS_PER_EVENT} — widen `slide` or \
                         narrow `size`.",
                        laminar_core::error_codes::SQL_UNSUPPORTED,
                    )));
                }
                CoreWindowAssigner::Hopping(
                    SlidingWindowAssigner::from_millis(size_ms, slide_ms).with_offset_ms(offset_ms),
                )
            }
            WindowType::Session => {
                let gap_ms = i64::try_from(
                    window_config
                        .gap
                        .unwrap_or(std::time::Duration::ZERO)
                        .as_millis(),
                )
                .map_err(|_| {
                    DbError::Unsupported(format!(
                        "[{}] session window gap exceeds the i64 millisecond timestamp range",
                        laminar_core::error_codes::SQL_UNSUPPORTED,
                    ))
                })?;
                if gap_ms <= 0 {
                    return Ok(None);
                }
                CoreWindowAssigner::Session { gap_ms }
            }
        };
        let allowed_lateness_ms = if matches!(emit_clause, Some(EmitClause::Final)) {
            0
        } else {
            i64::try_from(window_config.allowed_lateness.as_millis()).map_err(|_| {
                DbError::Unsupported(format!(
                    "[{}] allowed lateness exceeds the i64 millisecond timestamp range",
                    laminar_core::error_codes::SQL_UNSUPPORTED,
                ))
            })?
        };

        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| DbError::Pipeline(format!("plan error: {e}")))?;

        let plan = df.logical_plan();
        let planned_functions_immutable =
            crate::sql_analysis::planned_functions_are_immutable(plan);
        let top_schema = Arc::new(plan.schema().as_arrow().clone());

        let Some(agg_info) = find_aggregate(plan) else {
            return Ok(None);
        };

        let group_exprs = agg_info.group_exprs;
        let aggr_exprs = agg_info.aggr_exprs;
        let agg_schema = agg_info.schema;
        let agg_df_schema = agg_info.df_schema;
        let input_schema = agg_info.input_schema;
        let having_predicate = agg_info.having_predicate;

        if aggr_exprs.is_empty() {
            return Ok(None);
        }

        // single_source_table rejects self-joins before attempting expression compile.
        let compile_source = crate::sql_analysis::single_source_table(sql);
        let state_ref = ctx.state();
        let compile_props = state_ref.execution_props();
        let input_df_schema = &agg_info.input_df_schema;

        // Inspect the logical node itself. A same-arity, same-type expression such as
        // `COUNT(*) * 2` still requires a post-aggregate projection.
        let projection_info = crate::aggregate_state::find_post_aggregate_projection(plan)
            .map_err(|()| {
                DbError::Unsupported(format!(
                    "[{}] managed SQL windows require one aggregate stage and at most one post-aggregate projection",
                    laminar_core::error_codes::SQL_UNSUPPORTED
                ))
            })?
            .map(|projection| {
                (
                    projection.expr.as_slice(),
                    projection.input.schema().clone(),
                )
            });
        let has_projection = projection_info.is_some();

        // `now()` in GROUP BY/SELECT/HAVING/aggregate args would freeze at plan time.
        // `Unsupported` (not `Pipeline`) lets the EOWC operator re-propagate.
        let nonwhere_now = group_exprs.iter().any(expr_uses_wallclock)
            || aggr_exprs.iter().any(expr_uses_wallclock)
            || having_predicate.as_ref().is_some_and(expr_uses_wallclock)
            || projection_info
                .as_ref()
                .is_some_and(|(exprs, _)| exprs.iter().any(expr_uses_wallclock));
        if nonwhere_now {
            return Err(DbError::Unsupported(format!(
                "[{}] now()/current_timestamp() is only supported in the WHERE \
                 clause of a windowed query (it would freeze at plan time elsewhere)",
                laminar_core::error_codes::SQL_UNSUPPORTED
            )));
        }
        let where_uses_now = agg_info
            .where_predicate
            .as_ref()
            .is_some_and(expr_uses_wallclock);

        let logical_num_group_cols = group_exprs.len();
        let mut group_output_sources = Vec::with_capacity(logical_num_group_cols);
        let mut output_group_fields = Vec::with_capacity(logical_num_group_cols);
        let mut state_group_fields = Vec::with_capacity(logical_num_group_cols);
        let mut state_group_exprs = Vec::with_capacity(logical_num_group_cols);
        let mut group_types = Vec::with_capacity(logical_num_group_cols);
        let mut has_window_start = false;
        let mut has_window_end = false;
        for (i, group_expr) in group_exprs.iter().enumerate() {
            let name_field = if has_projection {
                agg_schema.field(i)
            } else {
                top_schema.field(i)
            };
            let agg_field = agg_schema.field(i);
            let output_field = Field::new(name_field.name(), agg_field.data_type().clone(), true);
            if let Some(source) = window_group_source(group_expr, &assigner)? {
                let duplicate = match source {
                    GroupOutputSource::WindowStart => {
                        std::mem::replace(&mut has_window_start, true)
                    }
                    GroupOutputSource::WindowEnd => std::mem::replace(&mut has_window_end, true),
                    GroupOutputSource::Key(_) => false,
                };
                if duplicate {
                    return Err(DbError::Unsupported(format!(
                        "[{}] duplicate SQL window boundary in GROUP BY",
                        laminar_core::error_codes::SQL_UNSUPPORTED
                    )));
                }
                if !matches!(
                    agg_field.data_type(),
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _)
                ) {
                    return Err(DbError::Unsupported(format!(
                        "[{}] SQL window markers must produce microsecond timestamps",
                        laminar_core::error_codes::SQL_UNSUPPORTED
                    )));
                }
                group_output_sources.push(source);
            } else {
                let key_index = state_group_exprs.len();
                group_output_sources.push(GroupOutputSource::Key(key_index));
                state_group_exprs.push(group_expr);
                state_group_fields.push(output_field.clone());
                group_types.push(agg_field.data_type().clone());
            }
            output_group_fields.push(output_field);
        }
        let num_group_cols = state_group_exprs.len();

        let compile = |e: &datafusion_expr::Expr| {
            create_physical_expr(e, input_df_schema, compile_props).ok()
        };
        let mut builder = PreAggBuilder::new(
            &input_schema,
            input_df_schema,
            num_group_cols,
            compile_source.is_some(),
        );

        for (i, group_expr) in state_group_exprs.into_iter().enumerate() {
            builder.push_group_expr(i, group_expr, &compile);
        }

        for (i, expr) in aggr_exprs.iter().enumerate() {
            let agg_schema_idx = logical_num_group_cols + i;
            let agg_field = agg_schema.field(agg_schema_idx);
            let output_name = if has_projection {
                agg_field.name().clone()
            } else if agg_schema_idx < top_schema.fields().len() {
                top_schema.field(agg_schema_idx).name().clone()
            } else {
                agg_field.name().clone()
            };
            if !builder.push_aggregate(expr, output_name, &compile)? {
                return Ok(None);
            }
        }

        let mut compile_ok = builder.compile_ok;
        let next_col_idx = builder.next_col_idx;
        let mut pre_agg_select_items = builder.pre_agg_select_items;
        let agg_specs = builder.agg_specs;
        let mut compiled_exprs = builder.compiled_exprs;
        let mut proj_fields = builder.proj_fields;

        if crate::aggregate_state::query_reads_weighted_changelog(ctx, sql).await? {
            return Err(DbError::Unsupported(format!(
                "[{}] window aggregates cannot consume a changelog until window retractions use the managed vnode state path",
                laminar_core::error_codes::SQL_UNSUPPORTED
            )));
        }
        for spec in &agg_specs {
            crate::aggregate_state::ConcreteAggregateState::try_new(
                spec,
                crate::aggregate_state::ConcreteInputMode::AppendOnly,
            )?;
        }

        let time_col_index = next_col_idx;
        pre_agg_select_items.push(format!("\"{}\" AS \"__cw_ts\"", window_config.time_column));

        if compile_ok {
            let time_expr = datafusion_expr::Expr::Column(
                datafusion_common::Column::new_unqualified(&window_config.time_column),
            );
            match create_physical_expr(&time_expr, input_df_schema, compile_props) {
                Ok(phys) => {
                    let dt = phys
                        .data_type(input_df_schema.as_arrow())
                        .unwrap_or(DataType::Int64);
                    proj_fields.push(Field::new("__cw_ts", dt, true));
                    compiled_exprs.push(phys);
                }
                Err(_) => compile_ok = false,
            }
        }

        let clauses = extract_clauses(sql);
        let pre_agg_sql = format!(
            "SELECT {} FROM {}{}",
            pre_agg_select_items.join(", "),
            clauses.from_clause,
            clauses.where_clause,
        );

        let compiled_projection = if compile_ok {
            // A `now()` predicate can't be compiled once; apply it per cycle instead.
            let filter = if where_uses_now {
                None
            } else if let Some(where_pred) = &agg_info.where_predicate {
                if let Ok(phys) = create_physical_expr(where_pred, input_df_schema, compile_props) {
                    Some(phys)
                } else {
                    compile_ok = false;
                    None
                }
            } else {
                None
            };
            if compile_ok {
                Some(CompiledProjection {
                    exprs: compiled_exprs,
                    filter,
                    output_schema: Arc::new(Schema::new(proj_fields)),
                })
            } else {
                None
            }
        } else {
            None
        };

        // The interpreted fallback would freeze `now()` at plan time; fail at CREATE.
        if where_uses_now && compiled_projection.is_none() {
            return Err(DbError::Unsupported(format!(
                "[{}] now()/current_timestamp() in WHERE requires the single-source \
                 compiled path; this query falls back to the interpreted plan where \
                 now() would freeze at plan time",
                laminar_core::error_codes::SQL_UNSUPPORTED
            )));
        }
        let now_where = if where_uses_now {
            agg_info.where_predicate.as_ref().map(|p| NowWhereFilter {
                predicate: p.clone(),
                df_schema: Arc::clone(&agg_info.input_df_schema),
            })
        } else {
            None
        };

        let mut output_fields = output_group_fields;
        let mut state_output_fields = state_group_fields;
        for spec in &agg_specs {
            let field = Field::new(&spec.output_name, spec.return_type.clone(), true);
            output_fields.push(field.clone());
            state_output_fields.push(field);
        }
        let aggregate_output_schema = Arc::new(Schema::new(output_fields));
        // Without a post-aggregate projection this is the outward stream ABI, so retain the
        // logical plan's exact names, types, metadata, and nullability. The synthesized aggregate
        // schema remains internal when a projection still has to run.
        let output_schema = if has_projection {
            Arc::clone(&aggregate_output_schema)
        } else {
            Arc::clone(&top_schema)
        };
        let state_output_schema = if logical_num_group_cols == num_group_cols {
            Arc::clone(&output_schema)
        } else {
            Arc::new(Schema::new(state_output_fields))
        };

        let post_projection = if let Some((proj_exprs, agg_df_schema)) = projection_info {
            // NULLIF/CASE accept `(any, any)` and skip DataFusion's normal cast insertion.
            let mut rewriter = TypeCoercionRewriter::new(&agg_df_schema);
            let mut compiled = Vec::with_capacity(proj_exprs.len());
            for expr in proj_exprs {
                let coerced = expr
                    .clone()
                    .rewrite(&mut rewriter)
                    .map(|t| t.data)
                    .map_err(|e| {
                        DbError::Pipeline(format!("type-coerce post-aggregate projection: {e}"))
                    })?;
                let phys =
                    create_physical_expr(&coerced, &agg_df_schema, compile_props).map_err(|e| {
                        DbError::Pipeline(format!("compile post-aggregate projection: {e}"))
                    })?;
                compiled.push(phys);
            }
            let final_schema = Arc::clone(&top_schema);

            Some(PostProjection {
                exprs: compiled,
                final_schema,
            })
        } else {
            None
        };

        let having_filter = compile_having_filter(ctx, having_predicate.as_ref(), &agg_df_schema)?;

        let cached_pre_agg_physical = if compiled_projection.is_none() {
            let df = ctx
                .sql(&pre_agg_sql)
                .await
                .map_err(|e| DbError::Pipeline(format!("pre-agg SQL planning failed: {e}")))?;
            let logical = df.logical_plan().clone();
            let physical = ctx
                .state()
                .create_physical_plan(&logical)
                .await
                .map_err(|e| DbError::Pipeline(format!("pre-agg physical planning failed: {e}")))?;
            Some(physical)
        } else {
            None
        };

        let sort_fields: Vec<arrow::row::SortField> = group_types
            .iter()
            .map(|dt| arrow::row::SortField::new(dt.clone()))
            .collect();
        let row_converter = Arc::new(
            arrow::row::RowConverter::new(sort_fields)
                .map_err(|e| DbError::Pipeline(format!("row converter init: {e}")))?,
        );
        let vnode_count = usize::from(key_group_count.get());
        let vnode_states = std::iter::repeat_with(|| None)
            .take(vnode_count)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let checkpoint_dirty_vnodes = vec![false; vnode_count].into_boxed_slice();
        let active_vnode_positions = vec![usize::MAX; vnode_count].into_boxed_slice();
        let mut active_vnodes = Vec::new();
        active_vnodes
            .try_reserve_exact(vnode_count)
            .map_err(|error| {
                DbError::Pipeline(format!("Core window vnode roster reserve failed: {error}"))
            })?;
        let mut checkpoint_dirty_vnode_roster = Vec::new();
        checkpoint_dirty_vnode_roster
            .try_reserve_exact(vnode_count)
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "Core window dirty vnode roster reserve failed: {error}"
                ))
            })?;

        Ok(Some(Self {
            assigner,
            key_group_count,
            vnode_states,
            active_vnodes,
            active_vnode_positions,
            window_group_counts: FxHashMap::default(),
            session_group_count: 0,
            row_converter,
            agg_specs,
            num_group_cols,
            group_types: Arc::from(group_types),
            query_sql: sql.to_string(),
            #[cfg(test)]
            pre_agg_sql,
            output_schema,
            state_output_schema,
            group_output_sources,
            time_col_index,
            compiled_projection,
            planned_functions_immutable,
            cached_pre_agg_physical,
            now_where,
            now_filter_cache: None,
            having_filter,
            max_groups_per_window: 1_000_000,
            allowed_lateness_ms,
            high_watermark_ms: i64::MIN,
            post_projection,
            prom: None,
            scratch_nogroup: FxHashMap::default(),
            scratch_grouped: FxHashMap::default(),
            scratch_group_keys: indexmap::IndexSet::default(),
            checkpoint_dirty_vnodes,
            checkpoint_dirty_vnode_roster,
            full_vnode_capture_required: true,
            required_frontier_floor_ms: i64::MIN,
        }))
    }

    fn accumulator_vector_bytes(
        accumulators: &Vec<Box<dyn datafusion_expr::Accumulator>>,
    ) -> usize {
        accumulators
            .capacity()
            .saturating_mul(std::mem::size_of::<Box<dyn datafusion_expr::Accumulator>>())
            .saturating_add(accumulators.iter().fold(0_usize, |bytes, accumulator| {
                bytes.saturating_add(accumulator.size())
            }))
    }

    fn fixed_window_bytes(groups: &FixedWindowGroups) -> usize {
        groups
            .capacity()
            .saturating_mul(std::mem::size_of::<(
                arrow::row::OwnedRow,
                Vec<Box<dyn datafusion_expr::Accumulator>>,
            )>())
            .saturating_add(groups.iter().fold(0_usize, |bytes, (key, accumulators)| {
                bytes
                    .saturating_add(key.as_ref().len())
                    .saturating_add(Self::accumulator_vector_bytes(accumulators))
            }))
            .saturating_add(BTREE_ENTRY_CHARGE)
    }

    fn scratch_nogroup_bytes(scratch: &FxHashMap<i64, Vec<u32>>) -> usize {
        scratch
            .capacity()
            .saturating_mul(std::mem::size_of::<(i64, Vec<u32>)>())
            .saturating_add(scratch.values().fold(0_usize, |bytes, rows| {
                bytes.saturating_add(rows.capacity().saturating_mul(std::mem::size_of::<u32>()))
            }))
    }

    fn scratch_grouped_bytes(scratch: &FxHashMap<(u32, i64, u32), Vec<u32>>) -> usize {
        scratch
            .capacity()
            .saturating_mul(std::mem::size_of::<((u32, i64, u32), Vec<u32>)>())
            .saturating_add(scratch.values().fold(0_usize, |bytes, rows| {
                bytes.saturating_add(rows.capacity().saturating_mul(std::mem::size_of::<u32>()))
            }))
    }

    fn scratch_group_keys_bytes(
        keys: &indexmap::IndexSet<arrow::row::OwnedRow, FxBuildHasher>,
    ) -> usize {
        keys.capacity()
            .saturating_mul(std::mem::size_of::<arrow::row::OwnedRow>())
            .saturating_add(keys.iter().fold(0_usize, |bytes, key| {
                bytes.saturating_add(key.as_ref().len())
            }))
    }

    fn fixed_windows_bytes(windows: &FixedWindows) -> usize {
        windows.values().fold(0_usize, |bytes, groups| {
            bytes.saturating_add(Self::fixed_window_bytes(groups))
        })
    }

    fn session_group_key_bytes(key: &SessionGroupKey) -> usize {
        key.as_ref()
            .as_ref()
            .len()
            .saturating_add(std::mem::size_of::<arrow::row::OwnedRow>())
            .saturating_add(2 * std::mem::size_of::<usize>())
    }

    fn session_group_bytes(key: &SessionGroupKey, group: &SessionGroupState) -> usize {
        Self::session_group_key_bytes(key).saturating_add(group.sessions.values().fold(
            0_usize,
            |bytes, session| {
                bytes
                    .saturating_add(BTREE_ENTRY_CHARGE)
                    .saturating_add(Self::accumulator_vector_bytes(&session.accs))
            },
        ))
    }

    fn session_groups_bytes(
        groups: &SessionGroups,
        deadlines: &BTreeSet<SessionDeadline>,
    ) -> usize {
        groups
            .capacity()
            .saturating_mul(std::mem::size_of::<(SessionGroupKey, SessionGroupState)>())
            .saturating_add(groups.iter().fold(0_usize, |bytes, (key, group)| {
                bytes.saturating_add(Self::session_group_bytes(key, group))
            }))
            .saturating_add(deadlines.iter().fold(0_usize, |bytes, deadline| {
                bytes.saturating_add(deadline.accounted_state_bytes())
            }))
    }

    fn insert_session_deadline(state: &mut CoreWindowVnodeState, deadline: SessionDeadline) {
        let retained_bytes = deadline.accounted_state_bytes();
        assert!(
            state.session_deadlines.insert(deadline),
            "Core session deadline insertion must target a vacant entry"
        );
        state.accounted_state_bytes = state.accounted_state_bytes.saturating_add(retained_bytes);
    }

    fn remove_session_deadline(state: &mut CoreWindowVnodeState, deadline: &SessionDeadline) {
        assert!(
            state.session_deadlines.remove(deadline),
            "Core session deadline removal must target a live entry"
        );
        state.accounted_state_bytes = state
            .accounted_state_bytes
            .checked_sub(deadline.accounted_state_bytes())
            .expect("Core session deadline accounting invariant failed");
    }

    fn validate_session_deadline_replacement(
        state: &CoreWindowVnodeState,
        retired: &[SessionDeadline],
        replacement: &SessionDeadline,
    ) -> Result<(), DbError> {
        let retired_bytes = retired
            .len()
            .checked_mul(BTREE_ENTRY_CHARGE)
            .ok_or_else(|| DbError::Pipeline("Core session deadline accounting overflow".into()))?;
        if state.accounted_state_bytes < retired_bytes {
            return Err(DbError::Pipeline(
                "Core session deadline accounting invariant failed".into(),
            ));
        }
        if retired
            .iter()
            .any(|deadline| !state.session_deadlines.contains(deadline))
        {
            return Err(DbError::Pipeline(
                "Core session deadline index is missing a live interval".into(),
            ));
        }
        if !retired.contains(replacement) && state.session_deadlines.contains(replacement) {
            return Err(DbError::Pipeline(
                "Core session deadline index contains a conflicting interval".into(),
            ));
        }
        Ok(())
    }

    fn commit_session_deadline_replacement(
        state: &mut CoreWindowVnodeState,
        retired: &[SessionDeadline],
        replacement: SessionDeadline,
    ) {
        if retired == std::slice::from_ref(&replacement) {
            return;
        }
        for deadline in retired {
            Self::remove_session_deadline(state, deadline);
        }
        Self::insert_session_deadline(state, replacement);
    }

    fn window_group_counts_bytes(counts: &FxHashMap<i64, usize>) -> usize {
        counts
            .capacity()
            .saturating_mul(std::mem::size_of::<(i64, usize)>())
    }

    #[must_use]
    pub(crate) fn accounted_state_bytes(&self) -> usize {
        self.active_vnodes
            .iter()
            .filter_map(|vnode| self.vnode_states[*vnode as usize].as_deref())
            .fold(0_usize, |bytes, state| {
                bytes
                    .saturating_add(std::mem::size_of::<CoreWindowVnodeState>())
                    .saturating_add(state.accounted_state_bytes)
            })
            .saturating_add(Self::scratch_nogroup_bytes(&self.scratch_nogroup))
            .saturating_add(Self::scratch_grouped_bytes(&self.scratch_grouped))
            .saturating_add(Self::scratch_group_keys_bytes(&self.scratch_group_keys))
            .saturating_add(Self::window_group_counts_bytes(&self.window_group_counts))
            .saturating_add(
                self.vnode_states
                    .len()
                    .saturating_mul(std::mem::size_of::<Option<Box<CoreWindowVnodeState>>>()),
            )
            .saturating_add(
                self.active_vnodes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(
                self.active_vnode_positions
                    .len()
                    .saturating_mul(std::mem::size_of::<usize>()),
            )
            .saturating_add(
                self.checkpoint_dirty_vnodes
                    .len()
                    .saturating_mul(std::mem::size_of::<bool>()),
            )
            .saturating_add(
                self.checkpoint_dirty_vnode_roster
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) const fn key_group_count(&self) -> KeyGroupCount {
        self.key_group_count
    }

    fn routing_vnode_count(&self) -> NonZeroU32 {
        NonZeroU32::from(self.key_group_count.into_non_zero())
    }

    pub(crate) fn validate_vnode_count(&self, requested: u32) -> Result<NonZeroU32, DbError> {
        let requested = KeyGroupCount::try_from(requested)
            .map_err(|error| DbError::Pipeline(format!("Core window vnode_count: {error}")))?;
        if requested != self.key_group_count {
            return Err(DbError::Pipeline(format!(
                "Core window key-group count mismatch: state={}, requested={requested}",
                self.key_group_count
            )));
        }
        Ok(self.routing_vnode_count())
    }

    #[inline]
    fn vnode_for_group_key(&self, key: &arrow::row::OwnedRow) -> u32 {
        if self.num_group_cols == 0 || self.key_group_count.get() == 1 {
            0
        } else {
            PartitionKeyCodecV1::vnode_for_encoded(key.as_ref(), self.routing_vnode_count())
        }
    }

    fn validate_vnode_hint(&self, vnode: u32) -> Result<(), DbError> {
        if vnode >= u32::from(self.key_group_count) {
            return Err(DbError::Pipeline(format!(
                "Core window vnode {vnode} is outside vnode_count {}",
                self.key_group_count
            )));
        }
        Ok(())
    }

    fn vnode_state_mut(&mut self, vnode: u32) -> &mut CoreWindowVnodeState {
        let slot = &mut self.vnode_states[vnode as usize];
        if slot.is_none() {
            *slot = Some(Box::new(CoreWindowVnodeState {
                windows: BTreeMap::new(),
                session_groups: FxHashMap::default(),
                session_deadlines: BTreeSet::new(),
                accounted_state_bytes: 0,
            }));
            debug_assert!(self.active_vnodes.len() < self.active_vnodes.capacity());
            let index = self.active_vnodes.len();
            debug_assert_eq!(self.active_vnode_positions[vnode as usize], usize::MAX);
            self.active_vnodes.push(vnode);
            self.active_vnode_positions[vnode as usize] = index;
        }
        slot.as_deref_mut().expect("Core window vnode was inserted")
    }

    fn insert_fixed_group(
        &mut self,
        vnode: u32,
        window_start: i64,
        key: arrow::row::OwnedRow,
        accumulators: Vec<Box<dyn datafusion_expr::Accumulator>>,
    ) {
        let key_bytes = key.as_ref().len();
        let accumulator_bytes = Self::accumulator_vector_bytes(&accumulators);
        let state = self.vnode_state_mut(vnode);
        let new_window = !state.windows.contains_key(&window_start);
        let groups = state.windows.entry(window_start).or_default();
        let previous_capacity = groups.capacity();
        assert!(
            groups.insert(key, accumulators).is_none(),
            "Core window group insertion must target a vacant key"
        );
        let roster_growth = groups
            .capacity()
            .saturating_sub(previous_capacity)
            .saturating_mul(std::mem::size_of::<(
                arrow::row::OwnedRow,
                Vec<Box<dyn datafusion_expr::Accumulator>>,
            )>());
        state.accounted_state_bytes = state
            .accounted_state_bytes
            .saturating_add(roster_growth)
            .saturating_add(key_bytes)
            .saturating_add(accumulator_bytes)
            .saturating_add(if new_window { BTREE_ENTRY_CHARGE } else { 0 });
        *self.window_group_counts.entry(window_start).or_default() += 1;
    }

    fn drop_empty_vnode(&mut self, vnode: u32) {
        let empty = self.vnode_states[vnode as usize]
            .as_ref()
            .is_some_and(|state| state.windows.is_empty() && state.session_groups.is_empty());
        if empty {
            assert!(
                self.vnode_states[vnode as usize]
                    .as_ref()
                    .is_some_and(|state| state.session_deadlines.is_empty()),
                "empty Core window vnode retained session deadlines"
            );
            self.vnode_states[vnode as usize] = None;
            let index =
                std::mem::replace(&mut self.active_vnode_positions[vnode as usize], usize::MAX);
            assert_ne!(index, usize::MAX, "empty Core window vnode must be active");
            let removed = self.active_vnodes.swap_remove(index);
            debug_assert_eq!(removed, vnode);
            if index < self.active_vnodes.len() {
                let moved = self.active_vnodes[index];
                self.active_vnode_positions[moved as usize] = index;
            }
        }
    }

    #[inline]
    fn mark_checkpoint_vnode_dirty(&mut self, vnode: u32) {
        let dirty = &mut self.checkpoint_dirty_vnodes[vnode as usize];
        if !*dirty {
            *dirty = true;
            debug_assert!(
                self.checkpoint_dirty_vnode_roster.len()
                    < self.checkpoint_dirty_vnode_roster.capacity()
            );
            self.checkpoint_dirty_vnode_roster.push(vnode);
        }
    }

    fn clear_checkpoint_dirty_vnodes(&mut self) {
        for &vnode in &self.checkpoint_dirty_vnode_roster {
            self.checkpoint_dirty_vnodes[vnode as usize] = false;
        }
        self.checkpoint_dirty_vnode_roster.clear();
    }

    /// Update per-window accumulators with a new pre-aggregation batch.
    ///
    /// Session windows use per-row processing because merge depends on insertion order.
    #[cfg(test)]
    pub fn update_batch(&mut self, batch: &RecordBatch) -> Result<(), DbError> {
        self.update_batch_for_vnode(batch, None)
    }

    pub(crate) fn update_batch_for_vnode(
        &mut self,
        batch: &RecordBatch,
        vnode_hint: Option<u32>,
    ) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if let Some(vnode) = vnode_hint {
            self.validate_vnode_hint(vnode)?;
        }

        let ts_array = extract_i64_timestamps(batch, self.time_col_index)?;

        if matches!(self.assigner, CoreWindowAssigner::Session { .. }) {
            return self.update_batch_session(batch, &ts_array, vnode_hint);
        }

        if self.num_group_cols == 0 {
            if vnode_hint.is_some_and(|vnode| vnode != 0) {
                return Err(DbError::Pipeline(
                    "global Core window batch was routed outside vnode zero".into(),
                ));
            }
            self.update_batch_nogroup(batch, &ts_array)
        } else {
            self.update_batch_grouped(batch, &ts_array, vnode_hint)
        }
    }

    fn update_batch_nogroup(
        &mut self,
        batch: &RecordBatch,
        ts_array: &[i64],
    ) -> Result<(), DbError> {
        let empty_key = crate::aggregate_state::global_aggregate_key();
        let mut grouped = std::mem::take(&mut self.scratch_nogroup);
        grouped.clear();
        for (row_idx, &ts_ms) in ts_array.iter().enumerate() {
            if ts_ms == NULL_TIMESTAMP {
                continue;
            }
            #[allow(clippy::cast_possible_truncation)]
            let idx = row_idx as u32;
            match &self.assigner {
                CoreWindowAssigner::Tumbling(a) => {
                    let ws = a
                        .try_assign(ts_ms)
                        .map_err(|error| {
                            DbError::PipelineTerminal(format!(
                                "Core tumbling window assignment failed: {error}"
                            ))
                        })?
                        .start;
                    if self.is_window_closed(ws) {
                        self.record_late_drop(1);
                        continue;
                    }
                    grouped.entry(ws).or_default().push(idx);
                }
                CoreWindowAssigner::Hopping(a) => {
                    let windows = a.try_iter_windows(ts_ms).map_err(|error| {
                        DbError::PipelineTerminal(format!(
                            "Core hopping window assignment failed: {error}"
                        ))
                    })?;
                    for wid in windows {
                        if self.is_window_closed(wid.start) {
                            self.record_late_drop(1);
                            continue;
                        }
                        grouped.entry(wid.start).or_default().push(idx);
                    }
                }
                CoreWindowAssigner::Session { .. } => unreachable!("handled above"),
            }
        }
        for (window_start, indices) in &grouped {
            let needs_insert = self.vnode_states[0]
                .as_ref()
                .and_then(|state| state.windows.get(window_start))
                .is_none_or(|groups| !groups.contains_key(&empty_key));
            if needs_insert {
                let accs = self.create_fresh_accumulators()?;
                self.insert_fixed_group(0, *window_start, empty_key.clone(), accs);
            }
            let agg_specs = &self.agg_specs;
            let state = self.vnode_states[0]
                .as_deref_mut()
                .expect("global Core window vnode must exist");
            let (previous_accumulator_bytes, update, current_accumulator_bytes) = {
                let accs = state
                    .windows
                    .get_mut(window_start)
                    .and_then(|groups| groups.get_mut(&empty_key))
                    .expect("global Core window group must exist");
                let previous = Self::accumulator_vector_bytes(accs);
                let update = crate::aggregate_state::IncrementalAggState::update_group_accumulators(
                    accs, batch, indices, agg_specs, None,
                );
                (previous, update, Self::accumulator_vector_bytes(accs))
            };
            state.accounted_state_bytes = state
                .accounted_state_bytes
                .saturating_sub(previous_accumulator_bytes)
                .saturating_add(current_accumulator_bytes);
            update?;
            self.mark_checkpoint_vnode_dirty(0);
        }
        self.scratch_nogroup = grouped;
        Ok(())
    }

    fn update_batch_grouped(
        &mut self,
        batch: &RecordBatch,
        ts_array: &[i64],
        vnode_hint: Option<u32>,
    ) -> Result<(), DbError> {
        let group_cols: Vec<ArrayRef> = (0..self.num_group_cols)
            .map(|i| Arc::clone(batch.column(i)))
            .collect();
        let rows = self
            .row_converter
            .convert_columns(&group_cols)
            .map_err(|e| DbError::Pipeline(format!("row conversion: {e}")))?;

        let mut grouped = std::mem::take(&mut self.scratch_grouped);
        let mut group_keys = std::mem::take(&mut self.scratch_group_keys);
        grouped.clear();
        group_keys.clear();

        for (row_idx, &ts_ms) in ts_array.iter().enumerate() {
            if ts_ms == NULL_TIMESTAMP {
                continue;
            }
            let row_key = rows.row(row_idx).owned();
            let vnode = self.vnode_for_group_key(&row_key);
            if vnode_hint.is_some_and(|hint| hint != vnode) {
                return Err(DbError::Pipeline(format!(
                    "Core window batch routed to vnode {} contains a key for vnode {vnode}",
                    vnode_hint.expect("checked as Some")
                )));
            }
            let (gid, _) = group_keys.insert_full(row_key);
            #[allow(clippy::cast_possible_truncation)]
            let (gid, idx) = (gid as u32, row_idx as u32);
            match &self.assigner {
                CoreWindowAssigner::Tumbling(a) => {
                    let ws = a
                        .try_assign(ts_ms)
                        .map_err(|error| {
                            DbError::PipelineTerminal(format!(
                                "Core tumbling window assignment failed: {error}"
                            ))
                        })?
                        .start;
                    if self.is_window_closed(ws) {
                        self.record_late_drop(1);
                        continue;
                    }
                    grouped.entry((vnode, ws, gid)).or_default().push(idx);
                }
                CoreWindowAssigner::Hopping(a) => {
                    let windows = a.try_iter_windows(ts_ms).map_err(|error| {
                        DbError::PipelineTerminal(format!(
                            "Core hopping window assignment failed: {error}"
                        ))
                    })?;
                    for wid in windows {
                        if self.is_window_closed(wid.start) {
                            self.record_late_drop(1);
                            continue;
                        }
                        grouped
                            .entry((vnode, wid.start, gid))
                            .or_default()
                            .push(idx);
                    }
                }
                CoreWindowAssigner::Session { .. } => unreachable!("handled above"),
            }
        }

        for ((vnode, window_start, gid), indices) in &grouped {
            let row_key = group_keys
                .get_index(*gid as usize)
                .expect("gid was just produced by insert_full");
            let needs_insert = {
                let window_groups = self.vnode_states[*vnode as usize]
                    .as_ref()
                    .and_then(|state| state.windows.get(window_start));
                if window_groups.is_some_and(|groups| groups.contains_key(row_key)) {
                    false
                } else if self
                    .window_group_counts
                    .get(window_start)
                    .copied()
                    .unwrap_or(0)
                    >= self.max_groups_per_window
                {
                    return Err(DbError::Pipeline(format!(
                        "Core window {window_start} group cardinality limit {} reached",
                        self.max_groups_per_window
                    )));
                } else {
                    true
                }
            };
            if needs_insert {
                let accs = self.create_fresh_accumulators()?;
                self.insert_fixed_group(*vnode, *window_start, row_key.clone(), accs);
            }

            let agg_specs = &self.agg_specs;
            let state = self.vnode_states[*vnode as usize]
                .as_deref_mut()
                .expect("Core window vnode must exist");
            let (previous_accumulator_bytes, update, current_accumulator_bytes) = {
                let accs = state
                    .windows
                    .get_mut(window_start)
                    .and_then(|groups| groups.get_mut(row_key))
                    .expect("Core window group must exist");
                let previous = Self::accumulator_vector_bytes(accs);
                let update = crate::aggregate_state::IncrementalAggState::update_group_accumulators(
                    accs, batch, indices, agg_specs, None,
                );
                (previous, update, Self::accumulator_vector_bytes(accs))
            };
            state.accounted_state_bytes = state
                .accounted_state_bytes
                .saturating_sub(previous_accumulator_bytes)
                .saturating_add(current_accumulator_bytes);
            update?;
            self.mark_checkpoint_vnode_dirty(*vnode);
        }

        self.scratch_grouped = grouped;
        self.scratch_group_keys = group_keys;
        Ok(())
    }

    /// Per-row fallback for session windows (merge depends on insertion order).
    fn update_batch_session(
        &mut self,
        batch: &RecordBatch,
        ts_array: &[i64],
        vnode_hint: Option<u32>,
    ) -> Result<(), DbError> {
        let CoreWindowAssigner::Session { gap_ms } = self.assigner else {
            unreachable!("update_batch_session called on non-session assigner");
        };
        let keys = if self.num_group_cols == 0 {
            vec![crate::aggregate_state::global_aggregate_key(); batch.num_rows()]
        } else {
            let group_cols: Vec<ArrayRef> = (0..self.num_group_cols)
                .map(|index| Arc::clone(batch.column(index)))
                .collect();
            let rows = self
                .row_converter
                .convert_columns(&group_cols)
                .map_err(|error| {
                    DbError::Pipeline(format!("session group key conversion: {error}"))
                })?;
            (0..batch.num_rows())
                .map(|row| rows.row(row).owned())
                .collect()
        };
        let mut vnodes = Vec::with_capacity(batch.num_rows());
        for (key, &ts_ms) in keys.iter().zip(ts_array) {
            let vnode = self.vnode_for_group_key(key);
            if ts_ms != NULL_TIMESTAMP && vnode_hint.is_some_and(|hint| hint != vnode) {
                return Err(DbError::Pipeline(format!(
                    "Core window batch routed to vnode {} contains a key for vnode {vnode}",
                    vnode_hint.expect("checked as Some")
                )));
            }
            vnodes.push(vnode);
        }
        for (row, &ts_ms) in ts_array.iter().enumerate() {
            if ts_ms == NULL_TIMESTAMP {
                continue;
            }
            #[allow(clippy::cast_possible_truncation)]
            let index_array = arrow::array::UInt32Array::from_value(row as u32, 1);
            self.update_session_window(
                vnodes[row],
                ts_ms,
                gap_ms,
                &keys[row],
                batch,
                &index_array,
            )?;
        }
        Ok(())
    }

    /// Update accumulators for a session window, merging overlapping sessions.
    fn update_session_window(
        &mut self,
        vnode: u32,
        ts_ms: i64,
        gap_ms: i64,
        key: &arrow::row::OwnedRow,
        batch: &RecordBatch,
        index_array: &arrow::array::UInt32Array,
    ) -> Result<(), DbError> {
        let new_start = ts_ms;
        let new_end = ts_ms.checked_add(gap_ms).ok_or_else(|| {
            DbError::PipelineTerminal(format!(
                "Core session window ending at {ts_ms} + {gap_ms}ms does not fit in i64"
            ))
        })?;
        let allowed_lateness_ms = self.allowed_lateness_ms;

        let mut overlapping: smallvec::SmallVec<[i64; 2]> = self.vnode_states[vnode as usize]
            .as_ref()
            .and_then(|state| state.session_groups.get(key))
            .map(|g| {
                g.sessions
                    .range(..=new_end)
                    .rev()
                    .take_while(|(_, session)| session.end >= new_start)
                    .map(|(&k, _)| k)
                    .collect()
            })
            .unwrap_or_default();
        overlapping.reverse();

        debug_assert!(
            overlapping
                .iter()
                .zip(overlapping.iter().skip(1))
                .all(|(a, b)| a < b),
            "session window: overlapping keys must be unique and sorted"
        );
        let candidate_end = self.vnode_states[vnode as usize]
            .as_ref()
            .and_then(|state| state.session_groups.get(key))
            .map_or(new_end, |group| {
                overlapping.iter().fold(new_end, |end, session_start| {
                    end.max(
                        group
                            .sessions
                            .get(session_start)
                            .expect("overlapping session was read from this group")
                            .end,
                    )
                })
            });
        if self.is_session_end_closed(candidate_end) {
            self.record_late_drop(1);
            return Ok(());
        }
        let new_group = self.vnode_states[vnode as usize]
            .as_ref()
            .is_none_or(|state| !state.session_groups.contains_key(key));
        if new_group && self.session_group_count >= self.max_groups_per_window {
            return Err(DbError::Pipeline(format!(
                "Core window session group cardinality limit {} reached",
                self.max_groups_per_window
            )));
        }

        match overlapping.len() {
            0 => {
                let mut accs = self.create_fresh_accumulators()?;
                Self::update_accumulators(&mut accs, &self.agg_specs, batch, index_array)?;
                let accumulator_bytes = Self::accumulator_vector_bytes(&accs);
                let group_key = self.vnode_states[vnode as usize]
                    .as_ref()
                    .and_then(|state| state.session_groups.get_key_value(key))
                    .map_or_else(|| Arc::new(key.clone()), |(key, _)| Arc::clone(key));
                let deadline = SessionDeadline::new(
                    Arc::clone(&group_key),
                    new_start,
                    new_end,
                    allowed_lateness_ms,
                );
                if let Some(state) = self.vnode_states[vnode as usize].as_deref() {
                    Self::validate_session_deadline_replacement(state, &[], &deadline)?;
                }
                let state = self.vnode_state_mut(vnode);
                let previous_capacity = state.session_groups.capacity();
                let group = state
                    .session_groups
                    .entry(Arc::clone(&group_key))
                    .or_insert_with(|| SessionGroupState {
                        sessions: BTreeMap::new(),
                    });
                assert!(
                    group
                        .sessions
                        .insert(
                            new_start,
                            SessionAccState {
                                start: new_start,
                                end: new_end,
                                accs,
                            },
                        )
                        .is_none(),
                    "new Core session must have a vacant start"
                );
                state.accounted_state_bytes = state
                    .accounted_state_bytes
                    .saturating_add(
                        state
                            .session_groups
                            .capacity()
                            .saturating_sub(previous_capacity)
                            .saturating_mul(std::mem::size_of::<(
                                SessionGroupKey,
                                SessionGroupState,
                            )>()),
                    )
                    .saturating_add(if new_group {
                        Self::session_group_key_bytes(&group_key)
                    } else {
                        0
                    })
                    .saturating_add(BTREE_ENTRY_CHARGE)
                    .saturating_add(accumulator_bytes);
                Self::commit_session_deadline_replacement(state, &[], deadline);
                if new_group {
                    self.session_group_count = self
                        .session_group_count
                        .checked_add(1)
                        .expect("Core session cardinality accounting overflow");
                }
            }
            1 => {
                let sess_key = overlapping[0];
                let previous_end = self.vnode_states[vnode as usize]
                    .as_ref()
                    .and_then(|state| state.session_groups.get(key))
                    .and_then(|group| group.sessions.get(&sess_key))
                    .map(|session| session.end)
                    .expect("invariant: session key sourced from this map");
                let merged_start = sess_key.min(new_start);
                let merged_end = previous_end.max(new_end);
                let deadline_change = (merged_start != sess_key
                    || merged_end.saturating_add(allowed_lateness_ms)
                        != previous_end.saturating_add(allowed_lateness_ms))
                .then(|| {
                    let group_key = self.vnode_states[vnode as usize]
                        .as_ref()
                        .and_then(|state| state.session_groups.get_key_value(key))
                        .map(|(key, _)| Arc::clone(key))
                        .expect("session group key must exist");
                    (
                        SessionDeadline::new(
                            Arc::clone(&group_key),
                            sess_key,
                            previous_end,
                            allowed_lateness_ms,
                        ),
                        SessionDeadline::new(
                            group_key,
                            merged_start,
                            merged_end,
                            allowed_lateness_ms,
                        ),
                    )
                });
                if let Some((previous_deadline, replacement_deadline)) = &deadline_change {
                    Self::validate_session_deadline_replacement(
                        self.vnode_states[vnode as usize]
                            .as_deref()
                            .expect("session vnode must exist"),
                        std::slice::from_ref(previous_deadline),
                        replacement_deadline,
                    )?;
                }
                let state = self.vnode_states[vnode as usize]
                    .as_deref_mut()
                    .expect("session vnode must exist");
                let (previous_accumulator_bytes, current_accumulator_bytes, update) = {
                    let group = state
                        .session_groups
                        .get_mut(key)
                        .expect("invariant: key present (overlapping derived from same group)");
                    let sess = group
                        .sessions
                        .get_mut(&sess_key)
                        .expect("invariant: session key sourced from this map");
                    let previous_accumulator_bytes = Self::accumulator_vector_bytes(&sess.accs);
                    let update = Self::update_accumulators(
                        &mut sess.accs,
                        &self.agg_specs,
                        batch,
                        index_array,
                    );
                    let current_accumulator_bytes = Self::accumulator_vector_bytes(&sess.accs);
                    if update.is_ok() {
                        sess.start = merged_start;
                        sess.end = merged_end;
                        if merged_start != sess_key {
                            let sess = group
                                .sessions
                                .remove(&sess_key)
                                .expect("invariant: session key just observed above");
                            group.sessions.insert(merged_start, sess);
                        }
                    }
                    (
                        previous_accumulator_bytes,
                        current_accumulator_bytes,
                        update,
                    )
                };
                state.accounted_state_bytes = state
                    .accounted_state_bytes
                    .checked_sub(previous_accumulator_bytes)
                    .expect("Core session state must cover its accumulator charge")
                    .saturating_add(current_accumulator_bytes);
                if update.is_ok() {
                    if let Some((previous_deadline, replacement_deadline)) = deadline_change {
                        Self::commit_session_deadline_replacement(
                            state,
                            std::slice::from_ref(&previous_deadline),
                            replacement_deadline,
                        );
                    }
                }
                update?;
            }
            _ => {
                self.merge_overlapping_sessions(
                    vnode,
                    key,
                    &overlapping,
                    new_start,
                    new_end,
                    batch,
                    index_array,
                )?;
            }
        }

        self.mark_checkpoint_vnode_dirty(vnode);

        Ok(())
    }

    fn merge_overlapping_sessions(
        &mut self,
        vnode: u32,
        key: &arrow::row::OwnedRow,
        overlapping: &[i64],
        new_start: i64,
        new_end: i64,
        batch: &RecordBatch,
        index_array: &arrow::array::UInt32Array,
    ) -> Result<(), DbError> {
        // Stage the merge into a fresh survivor before mutating the group: fold in
        // every overlapping session's state (a non-destructive read) plus the new
        // row, and only remove + insert once all fallible steps succeed — so a
        // mid-merge failure leaves the existing sessions intact.
        let mut accs = self.create_fresh_accumulators()?;
        let allowed_lateness_ms = self.allowed_lateness_ms;
        let group_key = self.vnode_states[vnode as usize]
            .as_ref()
            .and_then(|state| state.session_groups.get_key_value(key))
            .map(|(key, _)| Arc::clone(key))
            .expect("invariant: key present (overlapping derived from same group)");
        let (merged_start, merged_end, retired_bytes, retired_deadlines) = {
            let state = self.vnode_states[vnode as usize]
                .as_deref_mut()
                .expect("session vnode must exist");
            let group = state
                .session_groups
                .get_mut(key)
                .expect("invariant: key present (overlapping derived from same group)");
            let mut merged_start = new_start;
            let mut merged_end = new_end;
            let mut retired_bytes = 0_usize;
            let mut retired_deadlines: smallvec::SmallVec<[SessionDeadline; 2]> =
                smallvec::SmallVec::new();
            for &sess_key in overlapping {
                let sess = group
                    .sessions
                    .get_mut(&sess_key)
                    .expect("invariant: overlapping keys are unique BTreeMap entries");
                retired_bytes = retired_bytes
                    .saturating_add(BTREE_ENTRY_CHARGE)
                    .saturating_add(Self::accumulator_vector_bytes(&sess.accs));
                retired_deadlines.push(SessionDeadline::new(
                    Arc::clone(&group_key),
                    sess_key,
                    sess.end,
                    allowed_lateness_ms,
                ));
                merged_start = merged_start.min(sess.start);
                merged_end = merged_end.max(sess.end);
                for (i, acc) in sess.accs.iter_mut().enumerate() {
                    let state = acc
                        .state()
                        .map_err(|e| DbError::Pipeline(format!("session merge state: {e}")))?;
                    let arrays: Vec<ArrayRef> = state
                        .iter()
                        .map(|sv| {
                            sv.to_array()
                                .map_err(|e| DbError::Pipeline(format!("session merge array: {e}")))
                        })
                        .collect::<Result<_, _>>()?;
                    accs[i]
                        .merge_batch(&arrays)
                        .map_err(|e| DbError::Pipeline(format!("session merge: {e}")))?;
                }
            }
            (merged_start, merged_end, retired_bytes, retired_deadlines)
        };
        let replacement_deadline =
            SessionDeadline::new(group_key, merged_start, merged_end, allowed_lateness_ms);
        Self::validate_session_deadline_replacement(
            self.vnode_states[vnode as usize]
                .as_deref()
                .expect("session vnode must exist"),
            &retired_deadlines,
            &replacement_deadline,
        )?;
        Self::update_accumulators(&mut accs, &self.agg_specs, batch, index_array)?;

        let replacement_bytes =
            BTREE_ENTRY_CHARGE.saturating_add(Self::accumulator_vector_bytes(&accs));
        let state = self.vnode_states[vnode as usize]
            .as_deref_mut()
            .expect("session vnode must exist");
        {
            let group = state
                .session_groups
                .get_mut(key)
                .expect("invariant: key present (overlapping derived from same group)");
            for &sess_key in overlapping {
                assert!(
                    group.sessions.remove(&sess_key).is_some(),
                    "merged Core session removal must target a live interval"
                );
            }
            assert!(
                group
                    .sessions
                    .insert(
                        merged_start,
                        SessionAccState {
                            start: merged_start,
                            end: merged_end,
                            accs,
                        },
                    )
                    .is_none(),
                "merged Core session must have a vacant start"
            );
        }
        state.accounted_state_bytes = state
            .accounted_state_bytes
            .checked_sub(retired_bytes)
            .expect("Core session state must cover merged intervals")
            .saturating_add(replacement_bytes);
        Self::commit_session_deadline_replacement(state, &retired_deadlines, replacement_deadline);
        Ok(())
    }

    fn create_fresh_accumulators(
        &self,
    ) -> Result<Vec<Box<dyn datafusion_expr::Accumulator>>, DbError> {
        let mut accs = Vec::with_capacity(self.agg_specs.len());
        for spec in &self.agg_specs {
            accs.push(spec.create_accumulator()?);
        }
        Ok(accs)
    }

    fn update_accumulators(
        accs: &mut [Box<dyn datafusion_expr::Accumulator>],
        agg_specs: &[AggFuncSpec],
        batch: &RecordBatch,
        index_array: &arrow::array::UInt32Array,
    ) -> Result<(), DbError> {
        for (i, spec) in agg_specs.iter().enumerate() {
            let mut input_arrays: Vec<ArrayRef> = Vec::with_capacity(spec.input_col_indices.len());
            for &col_idx in &spec.input_col_indices {
                let arr = compute::take(batch.column(col_idx), index_array, None)
                    .map_err(|e| DbError::Pipeline(format!("array take: {e}")))?;
                input_arrays.push(arr);
            }

            if let Some(filter_idx) = spec.filter_col_index {
                let filter_arr = compute::take(batch.column(filter_idx), index_array, None)
                    .map_err(|e| DbError::Pipeline(format!("filter take: {e}")))?;
                if let Some(mask) = filter_arr
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                {
                    let mut filtered = Vec::with_capacity(input_arrays.len());
                    for arr in &input_arrays {
                        filtered.push(
                            compute::filter(arr, mask)
                                .map_err(|e| DbError::Pipeline(format!("filter apply: {e}")))?,
                        );
                    }
                    input_arrays = filtered;
                }
            }

            accs[i]
                .update_batch(&input_arrays)
                .map_err(|e| DbError::Pipeline(format!("accumulator update: {e}")))?;
        }
        Ok(())
    }

    /// Per-window predicate so hopping windows whose earlier slices closed still
    /// admit the event into still-open later slices.
    #[inline]
    fn is_window_closed(&self, window_start: i64) -> bool {
        if self.high_watermark_ms == i64::MIN {
            return false;
        }
        let size_ms = match &self.assigner {
            CoreWindowAssigner::Tumbling(a) => a.size_ms(),
            CoreWindowAssigner::Hopping(a) => a.size_ms(),
            CoreWindowAssigner::Session { .. } => return false,
        };
        window_start
            .saturating_add(size_ms)
            .saturating_add(self.allowed_lateness_ms)
            <= self.high_watermark_ms
    }

    #[inline]
    fn is_session_end_closed(&self, session_end_ms: i64) -> bool {
        self.high_watermark_ms != i64::MIN
            && session_end_ms.saturating_add(self.allowed_lateness_ms) <= self.high_watermark_ms
    }

    pub(crate) const fn high_watermark_ms(&self) -> i64 {
        self.high_watermark_ms
    }

    pub(crate) fn is_pristine_for_restore(&self) -> bool {
        self.high_watermark_ms == i64::MIN
            && self.required_frontier_floor_ms == i64::MIN
            && self.active_vnodes.is_empty()
            && self.vnode_states.iter().all(Option::is_none)
            && self.checkpoint_dirty_vnode_roster.is_empty()
            && self.checkpoint_dirty_vnodes.iter().all(|dirty| !dirty)
    }

    /// Close and emit all windows whose end (plus lateness grace) <= watermark.
    pub fn close_windows(&mut self, watermark_ms: i64) -> Result<Vec<RecordBatch>, DbError> {
        if watermark_ms <= self.high_watermark_ms {
            return Ok(Vec::new());
        }
        self.high_watermark_ms = watermark_ms;
        let fixed_size = match &self.assigner {
            CoreWindowAssigner::Tumbling(assigner) => Some(assigner.size_ms()),
            CoreWindowAssigner::Hopping(assigner) => Some(assigner.size_ms()),
            CoreWindowAssigner::Session { .. } => None,
        };
        let mut batches = Vec::new();
        let mut active_index = 0;
        while active_index < self.active_vnodes.len() {
            let vnode = self.active_vnodes[active_index];
            let mut closed = if let Some(size_ms) = fixed_size {
                self.close_fixed_windows(vnode, watermark_ms, size_ms)?
            } else {
                self.close_session_windows(vnode, watermark_ms)?
            };
            batches.append(&mut closed);
            if self.active_vnodes.get(active_index).copied() == Some(vnode) {
                active_index += 1;
            }
        }
        let batches = if let Some(filter) = &self.having_filter {
            apply_compiled_having(&batches, filter)?
        } else {
            batches
        };
        self.apply_post_projection(batches)
    }

    fn close_fixed_windows(
        &mut self,
        vnode: u32,
        watermark_ms: i64,
        size_ms: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let state = self.vnode_states[vnode as usize]
            .as_ref()
            .expect("active Core window vnode must exist");
        if let Some((&first_ws, _)) = state.windows.first_key_value() {
            if first_ws
                .saturating_add(size_ms)
                .saturating_add(self.allowed_lateness_ms)
                > watermark_ms
            {
                return Ok(Vec::new());
            }
        } else {
            return Ok(Vec::new());
        }

        let to_close: Vec<i64> = self.vnode_states[vnode as usize]
            .as_ref()
            .expect("active Core window vnode must exist")
            .windows
            .keys()
            .copied()
            .take_while(|&ws| {
                ws.saturating_add(size_ms)
                    .saturating_add(self.allowed_lateness_ms)
                    <= watermark_ms
            })
            .collect();

        let mut result_batches = Vec::new();
        let mutated = !to_close.is_empty();

        for window_start in to_close {
            let state = self.vnode_states[vnode as usize]
                .as_deref_mut()
                .expect("active Core window vnode must exist");
            let Some(groups) = state.windows.remove(&window_start) else {
                continue;
            };
            let retired_bytes = Self::fixed_window_bytes(&groups);
            state.accounted_state_bytes = state.accounted_state_bytes.saturating_sub(retired_bytes);
            let remaining_groups = self
                .window_group_counts
                .get(&window_start)
                .copied()
                .and_then(|count| count.checked_sub(groups.len()))
                .expect("Core window cardinality accounting must cover closed groups");
            if remaining_groups == 0 {
                self.window_group_counts.remove(&window_start);
            } else {
                self.window_group_counts
                    .insert(window_start, remaining_groups);
            }
            if groups.is_empty() {
                continue;
            }
            if let Some(b) = self.emit_window(window_start, size_ms, groups)? {
                result_batches.push(b);
            }
        }
        if mutated {
            self.mark_checkpoint_vnode_dirty(vnode);
            self.drop_empty_vnode(vnode);
        }

        Ok(result_batches)
    }

    fn close_session_windows(
        &mut self,
        vnode: u32,
        watermark_ms: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let allowed_lateness_ms = self.allowed_lateness_ms;
        let due_count = {
            let state = self.vnode_states[vnode as usize]
                .as_deref()
                .expect("active Core window vnode must exist");
            let mut count = 0_usize;
            for deadline in state
                .session_deadlines
                .iter()
                .take_while(|deadline| deadline.deadline_ms <= watermark_ms)
            {
                let (group_key, group) = state
                    .session_groups
                    .get_key_value(deadline.key.as_ref())
                    .ok_or_else(|| {
                        DbError::Pipeline(
                            "Core session deadline index references a missing group".into(),
                        )
                    })?;
                if !Arc::ptr_eq(group_key, &deadline.key) {
                    return Err(DbError::Pipeline(
                        "Core session deadline index does not share its group key".into(),
                    ));
                }
                let session = group.sessions.get(&deadline.session_start).ok_or_else(|| {
                    DbError::Pipeline(
                        "Core session deadline index references a missing interval".into(),
                    )
                })?;
                if session.start != deadline.session_start
                    || session.end.saturating_add(allowed_lateness_ms) != deadline.deadline_ms
                {
                    return Err(DbError::Pipeline(
                        "Core session deadline index does not match its interval".into(),
                    ));
                }
                count = count.checked_add(1).ok_or_else(|| {
                    DbError::Pipeline("Core session deadline count overflow".into())
                })?;
            }
            let deadline_bytes = count.checked_mul(BTREE_ENTRY_CHARGE).ok_or_else(|| {
                DbError::Pipeline("Core session deadline accounting overflow".into())
            })?;
            if state.accounted_state_bytes < deadline_bytes {
                return Err(DbError::Pipeline(
                    "Core session deadline accounting invariant failed".into(),
                ));
            }
            count
        };
        if due_count == 0 {
            return Ok(Vec::new());
        }

        #[allow(clippy::type_complexity)]
        let mut rows: Vec<(
            i64,
            i64,
            SessionGroupKey,
            Vec<Box<dyn datafusion_expr::Accumulator>>,
        )> = Vec::new();
        rows.try_reserve_exact(due_count).map_err(|error| {
            DbError::Pipeline(format!("Core session close roster reserve failed: {error}"))
        })?;

        let mut retired_bytes = 0_usize;
        let mut removed_groups = 0_usize;

        let state = self.vnode_states[vnode as usize]
            .as_deref_mut()
            .expect("active Core window vnode must exist");
        for _ in 0..due_count {
            let deadline = state
                .session_deadlines
                .pop_first()
                .expect("due Core session deadlines were validated above");
            state.accounted_state_bytes = state
                .accounted_state_bytes
                .checked_sub(deadline.accounted_state_bytes())
                .expect("Core session deadline accounting was validated above");
            let (session, empty_group) = {
                let group = state
                    .session_groups
                    .get_mut(&deadline.key)
                    .expect("Core session deadline groups were validated above");
                let session = group
                    .sessions
                    .remove(&deadline.session_start)
                    .expect("Core session deadline intervals were validated above");
                (session, group.sessions.is_empty())
            };
            retired_bytes = retired_bytes
                .saturating_add(BTREE_ENTRY_CHARGE)
                .saturating_add(Self::accumulator_vector_bytes(&session.accs));
            if empty_group {
                let removed = state
                    .session_groups
                    .remove(&deadline.key)
                    .expect("empty Core session group was observed above");
                debug_assert!(removed.sessions.is_empty());
                retired_bytes =
                    retired_bytes.saturating_add(Self::session_group_key_bytes(&deadline.key));
                removed_groups = removed_groups
                    .checked_add(1)
                    .expect("Core session group closure count overflow");
            }
            rows.push((session.start, session.end, deadline.key, session.accs));
        }

        state.accounted_state_bytes = state
            .accounted_state_bytes
            .checked_sub(retired_bytes)
            .expect("Core session state accounting must cover closed intervals");
        self.session_group_count = self
            .session_group_count
            .checked_sub(removed_groups)
            .expect("Core session cardinality accounting invariant failed");
        self.mark_checkpoint_vnode_dirty(vnode);
        self.drop_empty_vnode(vnode);

        rows.sort_unstable_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| left.2.as_ref().cmp(right.2.as_ref()))
        });

        self.emit_session_rows(rows)
    }

    /// Emit closed session rows as a `RecordBatch`.
    #[allow(clippy::type_complexity)]
    fn emit_session_rows(
        &self,
        rows: Vec<(
            i64,
            i64,
            SessionGroupKey,
            Vec<Box<dyn datafusion_expr::Accumulator>>,
        )>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let num_rows = rows.len();

        let mut row_keys: Vec<SessionGroupKey> = Vec::with_capacity(num_rows);
        let mut window_starts = Vec::with_capacity(num_rows);
        let mut window_ends = Vec::with_capacity(num_rows);
        let mut agg_scalars: Vec<Vec<ScalarValue>> = (0..self.agg_specs.len())
            .map(|_| Vec::with_capacity(num_rows))
            .collect();

        for (window_start, window_end, key, mut accs) in rows {
            row_keys.push(key);
            window_starts.push(window_start);
            window_ends.push(window_end);
            for (i, acc) in accs.iter_mut().enumerate() {
                let sv = acc
                    .evaluate()
                    .map_err(|e| DbError::Pipeline(format!("session accumulator evaluate: {e}")))?;
                agg_scalars[i].push(sv);
            }
        }

        let key_arrays: Vec<ArrayRef> = if self.num_group_cols > 0 {
            let row_refs: Vec<arrow::row::Row<'_>> = row_keys.iter().map(|r| r.row()).collect();
            self.row_converter
                .convert_rows(row_refs)
                .map_err(|e| DbError::Pipeline(format!("session group key arrays: {e}")))?
        } else {
            Vec::new()
        };
        let group_arrays = self.output_group_arrays(
            &key_arrays,
            &WindowBoundaryValues::PerRow {
                starts: &window_starts,
                ends: &window_ends,
            },
        )?;

        let mut agg_arrays: Vec<ArrayRef> = Vec::with_capacity(self.agg_specs.len());
        for (agg_idx, scalars) in agg_scalars.into_iter().enumerate() {
            let array = ScalarValue::iter_to_array(scalars)
                .map_err(|e| DbError::Pipeline(format!("agg result array: {e}")))?;
            let dt = &self.agg_specs[agg_idx].return_type;
            if array.data_type() == dt {
                agg_arrays.push(array);
            } else {
                let casted = arrow::compute::cast(&array, dt).unwrap_or(array);
                agg_arrays.push(casted);
            }
        }

        let mut all_arrays = Vec::with_capacity(group_arrays.len() + agg_arrays.len());
        all_arrays.extend(group_arrays);
        all_arrays.extend(agg_arrays);

        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), all_arrays)
            .map_err(|e| DbError::Pipeline(format!("session result batch: {e}")))?;

        Ok(vec![batch])
    }

    fn output_group_arrays(
        &self,
        key_arrays: &[ArrayRef],
        boundaries: &WindowBoundaryValues<'_>,
    ) -> Result<Vec<ArrayRef>, DbError> {
        if key_arrays.len() != self.num_group_cols {
            return Err(DbError::Pipeline(format!(
                "window output has {} key columns; expected {}",
                key_arrays.len(),
                self.num_group_cols
            )));
        }
        if self.group_output_sources.len() == self.num_group_cols {
            return Ok(key_arrays.to_vec());
        }

        if let WindowBoundaryValues::PerRow { starts, ends } = boundaries {
            if starts.len() != ends.len() {
                return Err(DbError::Pipeline(
                    "window output boundary vectors have different lengths".into(),
                ));
            }
        }

        let mut output = Vec::with_capacity(self.group_output_sources.len());
        for (output_index, source) in self.group_output_sources.iter().enumerate() {
            match source {
                GroupOutputSource::Key(key_index) => output.push(
                    key_arrays
                        .get(*key_index)
                        .cloned()
                        .ok_or_else(|| DbError::Pipeline("window key layout is invalid".into()))?,
                ),
                GroupOutputSource::WindowStart => {
                    output.push(self.window_boundary_array(output_index, boundaries, true)?);
                }
                GroupOutputSource::WindowEnd => {
                    output.push(self.window_boundary_array(output_index, boundaries, false)?);
                }
            }
        }
        Ok(output)
    }

    fn window_boundary_array(
        &self,
        output_index: usize,
        boundaries: &WindowBoundaryValues<'_>,
        start: bool,
    ) -> Result<ArrayRef, DbError> {
        let DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, timezone) =
            self.output_schema.field(output_index).data_type()
        else {
            return Err(DbError::Pipeline(
                "window boundary output is not a microsecond timestamp".into(),
            ));
        };
        let to_micros = |value: i64| {
            value.checked_mul(1000).ok_or_else(|| {
                DbError::PipelineTerminal(format!(
                    "window boundary {value}ms does not fit microsecond precision"
                ))
            })
        };
        let array = match boundaries {
            WindowBoundaryValues::Fixed {
                start: window_start,
                end: window_end,
                rows,
            } => arrow::array::TimestampMicrosecondArray::from_value(
                to_micros(if start { *window_start } else { *window_end })?,
                *rows,
            ),
            WindowBoundaryValues::PerRow { starts, ends } => {
                let values = if start { *starts } else { *ends };
                let micros = values
                    .iter()
                    .map(|&value| to_micros(value))
                    .collect::<Result<Vec<_>, _>>()?;
                arrow::array::TimestampMicrosecondArray::from(micros)
            }
        }
        .with_timezone_opt(timezone.clone());
        Ok(Arc::new(array))
    }

    fn emit_window(
        &self,
        window_start: i64,
        window_size_ms: i64,
        groups: FxHashMap<arrow::row::OwnedRow, Vec<Box<dyn datafusion_expr::Accumulator>>>,
    ) -> Result<Option<RecordBatch>, DbError> {
        let Some(batch) = crate::aggregate_state::emit_window_batch(
            groups,
            &self.row_converter,
            self.num_group_cols,
            &self.agg_specs,
            &self.state_output_schema,
        )?
        else {
            return Ok(None);
        };
        if self.group_output_sources.len() == self.num_group_cols {
            return Ok(Some(batch));
        }
        let window_end = window_start.checked_add(window_size_ms).ok_or_else(|| {
            DbError::PipelineTerminal(format!(
                "window ending at {window_start} + {window_size_ms}ms does not fit in i64"
            ))
        })?;

        let mut columns = self.output_group_arrays(
            &batch.columns()[..self.num_group_cols],
            &WindowBoundaryValues::Fixed {
                start: window_start,
                end: window_end,
                rows: batch.num_rows(),
            },
        )?;
        columns.extend_from_slice(&batch.columns()[self.num_group_cols..]);
        RecordBatch::try_new(Arc::clone(&self.output_schema), columns)
            .map(Some)
            .map_err(|error| DbError::Pipeline(format!("window result batch: {error}")))
    }

    fn apply_post_projection(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let Some(proj) = &self.post_projection else {
            return Ok(batches);
        };

        let mut result = Vec::with_capacity(batches.len());
        for batch in &batches {
            let num_rows = batch.num_rows();
            if num_rows == 0 {
                continue;
            }

            let mut projected_cols = Vec::with_capacity(proj.exprs.len());
            for phys_expr in &proj.exprs {
                let col_val = phys_expr
                    .evaluate(batch)
                    .map_err(|e| DbError::Pipeline(format!("post-projection evaluate: {e}")))?;
                let array = col_val
                    .into_array(num_rows)
                    .map_err(|e| DbError::Pipeline(format!("post-projection into_array: {e}")))?;
                projected_cols.push(array);
            }

            let projected_batch =
                RecordBatch::try_new(Arc::clone(&proj.final_schema), projected_cols)
                    .map_err(|e| DbError::Pipeline(format!("post-projection result batch: {e}")))?;
            result.push(projected_batch);
        }
        Ok(result)
    }

    #[cfg(test)]
    fn pre_agg_sql(&self) -> &str {
        &self.pre_agg_sql
    }

    pub fn compiled_projection(&self) -> Option<&CompiledProjection> {
        self.compiled_projection.as_ref()
    }

    pub(crate) const fn planned_functions_are_immutable(&self) -> bool {
        self.planned_functions_immutable
    }

    #[cfg(feature = "cluster")]
    pub(crate) const fn num_group_cols(&self) -> usize {
        self.num_group_cols
    }

    pub fn cached_pre_agg_physical(
        &self,
    ) -> Option<&Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.cached_pre_agg_physical.as_ref()
    }

    /// Apply the `now()` WHERE predicate with `now()` bound to the watermark
    /// for replay determinism. Returns `Ok(None)` when no filter is set or the
    /// watermark is not yet established. Predicate is compiled once per second.
    pub(crate) fn apply_dynamic_now_filter(
        &mut self,
        ctx: &SessionContext,
        inputs: &[RecordBatch],
        watermark_ms: i64,
    ) -> Result<Option<Vec<RecordBatch>>, DbError> {
        if self.now_where.is_none() || watermark_ms == i64::MIN {
            return Ok(None);
        }
        // Coarsen to seconds so the compiled predicate is reused within the same second.
        let secs = watermark_ms.div_euclid(1000);
        if self
            .now_filter_cache
            .as_ref()
            .is_none_or(|(s, _)| *s != secs)
        {
            let nw = self.now_where.as_ref().expect("checked above");
            // Saturate to avoid panic past year 2262.
            let now_ns = secs.checked_mul(1_000_000_000).unwrap_or(i64::MAX);
            let result = (|| -> Result<Arc<dyn PhysicalExpr>, DbError> {
                let pred = substitute_wallclock(nw.predicate.clone(), now_ns)?;
                let mut coercer = TypeCoercionRewriter::new(&nw.df_schema);
                let pred = pred.rewrite(&mut coercer).map(|t| t.data).map_err(|e| {
                    DbError::Pipeline(format!(
                        "[{}] now() WHERE type-coerce: {e}",
                        laminar_core::error_codes::SQL_PLANNING_FAILED
                    ))
                })?;
                let state = ctx.state();
                create_physical_expr(&pred, &nw.df_schema, state.execution_props()).map_err(|e| {
                    DbError::Pipeline(format!(
                        "[{}] now() WHERE physical compile: {e}",
                        laminar_core::error_codes::SQL_PLANNING_FAILED
                    ))
                })
            })();
            match result {
                Ok(phys) => self.now_filter_cache = Some((secs, Ok(phys))),
                Err(e) => {
                    let msg = e.to_string();
                    self.now_filter_cache = Some((secs, Err(msg.clone())));
                    return Err(DbError::Pipeline(msg));
                }
            }
        }
        let cached = &self.now_filter_cache.as_ref().expect("set above").1;
        let phys = match cached {
            Ok(p) => p,
            Err(msg) => return Err(DbError::Pipeline(msg.clone())),
        };
        let mut out = Vec::with_capacity(inputs.len());
        for b in inputs {
            if let Some(f) = crate::filter_compile::apply(b, phys.as_ref())? {
                out.push(f);
            }
        }
        Ok(Some(out))
    }

    pub(crate) fn attach_metrics(
        &mut self,
        prom: Option<Arc<crate::engine_metrics::EngineMetrics>>,
    ) {
        self.prom = prom;
    }

    #[inline]
    fn record_late_drop(&self, n: usize) {
        if let Some(prom) = &self.prom {
            if n > 0 {
                prom.window_late_dropped
                    .inc_by(u64::try_from(n).unwrap_or(u64::MAX));
            }
        }
    }

    pub(crate) fn query_fingerprint(&self) -> u64 {
        let mut config = Vec::with_capacity(38);
        config.push(4);
        config.extend_from_slice(&laminar_core::state::PARTITIONING_ABI_VERSION.to_le_bytes());
        config.extend_from_slice(&self.key_group_count.get().to_le_bytes());
        match &self.assigner {
            CoreWindowAssigner::Tumbling(t) => {
                config.push(1);
                config.extend_from_slice(&t.size_ms().to_le_bytes());
                config.extend_from_slice(&t.offset_ms().to_le_bytes());
            }
            CoreWindowAssigner::Hopping(s) => {
                config.push(2);
                config.extend_from_slice(&s.size_ms().to_le_bytes());
                config.extend_from_slice(&s.slide_ms().to_le_bytes());
                config.extend_from_slice(&s.offset_ms().to_le_bytes());
            }
            CoreWindowAssigner::Session { gap_ms } => {
                config.push(3);
                config.extend_from_slice(&gap_ms.to_le_bytes());
            }
        }
        config.extend_from_slice(&self.allowed_lateness_ms.to_le_bytes());
        query_fingerprint_with_config(&self.query_sql, &self.output_schema, &config)
    }

    fn window_type_tag(&self) -> u8 {
        match &self.assigner {
            CoreWindowAssigner::Tumbling(_) => 1,
            CoreWindowAssigner::Hopping(_) => 2,
            CoreWindowAssigner::Session { .. } => 3,
        }
    }

    fn capture_full_vnode(
        &mut self,
        vnode: u32,
        fingerprint: u64,
        group_types: Arc<[DataType]>,
        max_retained_bytes: usize,
    ) -> Result<CoreWindowVnodeCheckpointCapture, DbError> {
        fn charge(remaining: &mut usize, bytes: usize, component: &str) -> Result<(), DbError> {
            *remaining = remaining.checked_sub(bytes).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "Core window {component} exceeded the remaining capture budget"
                ))
            })?;
            Ok(())
        }

        fn reserve_roster<T>(
            values: &mut Vec<T>,
            len: usize,
            remaining: &mut usize,
            component: &str,
        ) -> Result<(), DbError> {
            let admitted = len.checked_mul(std::mem::size_of::<T>()).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "Core window {component} capture accounting overflow"
                ))
            })?;
            charge(remaining, admitted, component)?;
            values.try_reserve_exact(len).map_err(|error| {
                DbError::Checkpoint(format!(
                    "Core window {component} capture reserve failed: {error}"
                ))
            })?;
            let retained = values
                .capacity()
                .checked_mul(std::mem::size_of::<T>())
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "Core window {component} capture accounting overflow"
                    ))
                })?;
            if retained > admitted {
                charge(remaining, retained - admitted, component)?;
            }
            Ok(())
        }

        fn capture_accumulator_states(
            accumulators: &mut [Box<dyn datafusion_expr::Accumulator>],
            remaining: &mut usize,
            component: &str,
        ) -> Result<Vec<Vec<ScalarValue>>, DbError> {
            let mut states = Vec::new();
            reserve_roster(&mut states, accumulators.len(), remaining, component)?;
            for accumulator in accumulators {
                let state = accumulator.state().map_err(|error| {
                    DbError::Checkpoint(format!("Core window {component} capture failed: {error}"))
                })?;
                let retained = state
                    .capacity()
                    .checked_mul(std::mem::size_of::<ScalarValue>())
                    .and_then(|bytes| {
                        state.iter().try_fold(bytes, |bytes, scalar| {
                            bytes.checked_add(
                                scalar
                                    .size()
                                    .saturating_sub(std::mem::size_of::<ScalarValue>()),
                            )
                        })
                    })
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "Core window {component} capture accounting overflow"
                        ))
                    })?;
                charge(remaining, retained, component)?;
                states.push(state);
            }
            Ok(states)
        }

        let mut remaining = max_retained_bytes;
        charge(
            &mut remaining,
            std::mem::size_of::<CoreWindowVnodeCheckpointCapture>(),
            "vnode snapshot",
        )?;
        let mut windows = Vec::new();
        let mut session_state = Vec::new();
        if let Some(state) = self.vnode_states[vnode as usize].as_deref_mut() {
            reserve_roster(
                &mut windows,
                state.windows.len(),
                &mut remaining,
                "window roster",
            )?;
            for (&window_start, groups) in &mut state.windows {
                let mut captured_groups = Vec::new();
                reserve_roster(
                    &mut captured_groups,
                    groups.len(),
                    &mut remaining,
                    "group roster",
                )?;
                for (key, accumulators) in groups {
                    charge(&mut remaining, key.as_ref().len(), "group key")?;
                    let accumulator_states = capture_accumulator_states(
                        accumulators,
                        &mut remaining,
                        "accumulator state",
                    )?;
                    captured_groups.push(CapturedCoreWindowGroup {
                        key: key.clone(),
                        accumulator_states,
                    });
                }
                captured_groups
                    .sort_unstable_by(|left, right| left.key.as_ref().cmp(right.key.as_ref()));
                windows.push(CapturedFixedWindow {
                    window_start,
                    groups: captured_groups,
                });
            }
            reserve_roster(
                &mut session_state,
                state.session_groups.len(),
                &mut remaining,
                "session group roster",
            )?;
            for (key, group) in &mut state.session_groups {
                charge(
                    &mut remaining,
                    Self::session_group_key_bytes(key),
                    "session group key",
                )?;
                let mut sessions = Vec::new();
                reserve_roster(
                    &mut sessions,
                    group.sessions.len(),
                    &mut remaining,
                    "session roster",
                )?;
                for session in group.sessions.values_mut() {
                    let accumulator_states = capture_accumulator_states(
                        &mut session.accs,
                        &mut remaining,
                        "session accumulator state",
                    )?;
                    sessions.push(CapturedSession {
                        start: session.start,
                        end: session.end,
                        accumulator_states,
                    });
                }
                session_state.push(CapturedSessionGroup {
                    key: Arc::clone(key),
                    sessions,
                });
            }
            session_state.sort_unstable_by(|left, right| left.key.as_ref().cmp(right.key.as_ref()));
        }
        let retained_bytes = max_retained_bytes - remaining;
        Ok(CoreWindowVnodeCheckpointCapture {
            fingerprint,
            vnode,
            frontier_floor_ms: self.high_watermark_ms,
            window_type: self.window_type_tag(),
            group_types,
            row_converter: Arc::clone(&self.row_converter),
            windows,
            session_state,
            retained_bytes,
        })
    }

    pub(crate) fn capture_checkpoint_vnodes(
        &mut self,
        required_vnodes: &[u32],
        vnode_count: u32,
        max_capture_bytes: u64,
    ) -> Result<Vec<(u32, CoreWindowVnodeCheckpointCapture)>, DbError> {
        let vnode_count = self.validate_vnode_count(vnode_count)?;
        if required_vnodes.windows(2).any(|pair| pair[0] >= pair[1])
            || required_vnodes
                .iter()
                .any(|vnode| *vnode >= vnode_count.get())
        {
            return Err(DbError::Checkpoint(format!(
                "Core window received a non-canonical vnode roster {required_vnodes:?}"
            )));
        }
        let full_capture = self.full_vnode_capture_required;
        if full_capture {
            if let Some(unowned) = self
                .active_vnodes
                .iter()
                .find(|vnode| required_vnodes.binary_search(vnode).is_err())
            {
                return Err(DbError::Checkpoint(format!(
                    "Core window retained state for unowned vnode {unowned}"
                )));
            }
        } else {
            self.checkpoint_dirty_vnode_roster.sort_unstable();
            if let Some(unowned) = self
                .checkpoint_dirty_vnode_roster
                .iter()
                .find(|vnode| required_vnodes.binary_search(vnode).is_err())
            {
                return Err(DbError::Checkpoint(format!(
                    "Core window retained dirty state for unowned vnode {unowned}"
                )));
            }
        }
        let capture_count = if full_capture {
            required_vnodes.len()
        } else {
            self.checkpoint_dirty_vnode_roster.len()
        };
        if capture_count == 0 {
            self.clear_checkpoint_dirty_vnodes();
            self.full_vnode_capture_required = false;
            return Ok(Vec::new());
        }
        let mut captures = Vec::new();
        captures.try_reserve_exact(capture_count).map_err(|error| {
            DbError::Checkpoint(format!(
                "Core window capture roster reserve failed: {error}"
            ))
        })?;
        let fingerprint = self.query_fingerprint();
        let group_types = Arc::clone(&self.group_types);
        let mut remaining = max_capture_bytes;
        let mut index = 0;
        while index < capture_count {
            let vnode = if full_capture {
                required_vnodes[index]
            } else {
                self.checkpoint_dirty_vnode_roster[index]
            };
            let capture = self.capture_full_vnode(
                vnode,
                fingerprint,
                Arc::clone(&group_types),
                usize::try_from(remaining).unwrap_or(usize::MAX),
            )?;
            remaining = remaining
                .checked_sub(u64::try_from(capture.retained_bytes()).unwrap_or(u64::MAX))
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "Core window vnode {vnode} capture exceeded its budget"
                    ))
                })?;
            captures.push((vnode, capture));
            index += 1;
        }
        self.clear_checkpoint_dirty_vnodes();
        self.full_vnode_capture_required = false;
        Ok(captures)
    }

    #[cfg(test)]
    pub(crate) fn checkpoint_vnodes(
        &mut self,
        required_vnodes: &[u32],
        vnode_count: u32,
    ) -> Result<Vec<(u32, CoreWindowVnodeCheckpoint)>, DbError> {
        self.capture_checkpoint_vnodes(required_vnodes, vnode_count, u64::MAX)?
            .into_iter()
            .map(|(vnode, capture)| capture.encode(usize::MAX).map(|state| (vnode, state)))
            .collect()
    }

    pub(crate) fn force_full_vnode_capture(&mut self) {
        self.full_vnode_capture_required = true;
    }

    fn prepare_fixed_window_restore(
        &self,
        checkpoint: &CoreWindowVnodeCheckpoint,
        vnode: u32,
        size_ms: i64,
        alignment_ms: i64,
        offset_ms: i64,
    ) -> Result<PreparedCoreWindowRestore, DbError> {
        if !checkpoint.session_state.is_empty() {
            return Err(DbError::Checkpoint(
                "fixed-window checkpoint contains session state".into(),
            ));
        }

        let mut windows = BTreeMap::new();
        let mut previous_start = None;
        for wc in &checkpoint.windows {
            if previous_start.is_some_and(|previous| previous >= wc.window_start) {
                return Err(DbError::Checkpoint(
                    "fixed-window checkpoint starts are not strictly increasing".into(),
                ));
            }
            previous_start = Some(wc.window_start);
            if (i128::from(wc.window_start) - i128::from(offset_ms))
                .rem_euclid(i128::from(alignment_ms))
                != 0
            {
                return Err(DbError::Checkpoint(format!(
                    "fixed-window checkpoint start {} is not aligned to the configured assigner",
                    wc.window_start
                )));
            }
            let window_end = wc.window_start.checked_add(size_ms).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "fixed-window checkpoint start {} cannot fit its {size_ms}ms size",
                    wc.window_start
                ))
            })?;
            if checkpoint.frontier_floor_ms != i64::MIN
                && window_end.saturating_add(self.allowed_lateness_ms)
                    <= checkpoint.frontier_floor_ms
            {
                return Err(DbError::Checkpoint(format!(
                    "fixed-window checkpoint retains already-closed window {}",
                    wc.window_start
                )));
            }
            if wc.groups.is_empty() {
                return Err(DbError::Checkpoint(format!(
                    "fixed-window checkpoint contains empty window {}",
                    wc.window_start
                )));
            }
            if wc.groups.len() > self.max_groups_per_window {
                return Err(DbError::Checkpoint(format!(
                    "fixed-window checkpoint window {} has {} groups; limit={}",
                    wc.window_start,
                    wc.groups.len(),
                    self.max_groups_per_window
                )));
            }

            let mut groups = FxHashMap::default();
            groups.try_reserve(wc.groups.len()).map_err(|error| {
                DbError::Checkpoint(format!(
                    "fixed-window checkpoint group reserve failed: {error}"
                ))
            })?;
            for gc in &wc.groups {
                let row_key = self.decode_checkpoint_key(&gc.key)?;
                if self.vnode_for_group_key(&row_key) != vnode {
                    return Err(DbError::Checkpoint(format!(
                        "Core window vnode {vnode} checkpoint contains a key for another vnode"
                    )));
                }
                let accs = self.decode_checkpoint_accumulators(&gc.acc_states, "fixed window")?;
                if groups.insert(row_key, accs).is_some() {
                    return Err(DbError::Checkpoint(format!(
                        "fixed-window checkpoint window {} contains a duplicate group key",
                        wc.window_start
                    )));
                }
            }
            windows.insert(wc.window_start, groups);
        }

        let state = (!windows.is_empty()).then(|| {
            let accounted_state_bytes = Self::fixed_windows_bytes(&windows);
            Box::new(CoreWindowVnodeState {
                windows,
                session_groups: FxHashMap::default(),
                session_deadlines: BTreeSet::new(),
                accounted_state_bytes,
            })
        });
        Ok(PreparedCoreWindowRestore {
            state,
            frontier_floor_ms: checkpoint.frontier_floor_ms,
        })
    }

    fn prepare_session_window_restore(
        &self,
        checkpoint: &CoreWindowVnodeCheckpoint,
        vnode: u32,
        gap_ms: i64,
    ) -> Result<PreparedCoreWindowRestore, DbError> {
        if !checkpoint.windows.is_empty() {
            return Err(DbError::Checkpoint(
                "session checkpoint contains fixed-window state".into(),
            ));
        }
        if checkpoint.session_state.len() > self.max_groups_per_window {
            return Err(DbError::Checkpoint(format!(
                "session checkpoint has {} groups; limit={}",
                checkpoint.session_state.len(),
                self.max_groups_per_window
            )));
        }

        let mut session_groups = FxHashMap::default();
        let mut session_deadlines = BTreeSet::new();
        session_groups
            .try_reserve(checkpoint.session_state.len())
            .map_err(|error| {
                DbError::Checkpoint(format!("session checkpoint group reserve failed: {error}"))
            })?;
        for sgc in &checkpoint.session_state {
            if sgc.sessions.is_empty() {
                return Err(DbError::Checkpoint(
                    "session checkpoint contains an empty group".into(),
                ));
            }
            let row_key = Arc::new(self.decode_checkpoint_key(&sgc.key)?);
            if self.vnode_for_group_key(row_key.as_ref()) != vnode {
                return Err(DbError::Checkpoint(format!(
                    "Core window vnode {vnode} checkpoint contains a key for another vnode"
                )));
            }
            let mut sessions = BTreeMap::new();
            let mut previous_end = None;
            for sc in &sgc.sessions {
                let minimum_end = sc.start.checked_add(gap_ms).ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "session checkpoint interval starting at {} cannot fit its {gap_ms}ms gap",
                        sc.start
                    ))
                })?;
                if sc.end < minimum_end {
                    return Err(DbError::Checkpoint(format!(
                        "session checkpoint contains invalid interval [{}, {})",
                        sc.start, sc.end
                    )));
                }
                if previous_end.is_some_and(|end| end >= sc.start) {
                    return Err(DbError::Checkpoint(
                        "session checkpoint intervals are not strictly ordered and disjoint".into(),
                    ));
                }
                if checkpoint.frontier_floor_ms != i64::MIN
                    && sc.end.saturating_add(self.allowed_lateness_ms)
                        <= checkpoint.frontier_floor_ms
                {
                    return Err(DbError::Checkpoint(format!(
                        "session checkpoint retains already-closed interval [{}, {})",
                        sc.start, sc.end
                    )));
                }
                let accs = self.decode_checkpoint_accumulators(&sc.acc_states, "session window")?;
                if !session_deadlines.insert(SessionDeadline::new(
                    Arc::clone(&row_key),
                    sc.start,
                    sc.end,
                    self.allowed_lateness_ms,
                )) {
                    return Err(DbError::Checkpoint(
                        "session checkpoint contains a duplicate deadline".into(),
                    ));
                }
                sessions.insert(
                    sc.start,
                    SessionAccState {
                        start: sc.start,
                        end: sc.end,
                        accs,
                    },
                );
                previous_end = Some(sc.end);
            }
            if session_groups
                .insert(row_key, SessionGroupState { sessions })
                .is_some()
            {
                return Err(DbError::Checkpoint(
                    "session checkpoint contains a duplicate group key".into(),
                ));
            }
        }

        let state = (!session_groups.is_empty()).then(|| {
            let accounted_state_bytes =
                Self::session_groups_bytes(&session_groups, &session_deadlines);
            Box::new(CoreWindowVnodeState {
                windows: BTreeMap::new(),
                session_groups,
                session_deadlines,
                accounted_state_bytes,
            })
        });
        Ok(PreparedCoreWindowRestore {
            state,
            frontier_floor_ms: checkpoint.frontier_floor_ms,
        })
    }

    fn decode_checkpoint_key(&self, bytes: &[u8]) -> Result<arrow::row::OwnedRow, DbError> {
        use crate::aggregate_state::ipc_to_scalars;

        if (self.num_group_cols == 0) != bytes.is_empty() {
            return Err(DbError::Checkpoint(
                "Core window checkpoint group key has a non-canonical encoding".into(),
            ));
        }
        let scalars = ipc_to_scalars(bytes)
            .map_err(|error| DbError::Checkpoint(format!("window group key decode: {error}")))?;
        if scalars.len() != self.group_types.len() {
            return Err(DbError::Checkpoint(format!(
                "Core window checkpoint group key has {} columns; expected {}",
                scalars.len(),
                self.group_types.len()
            )));
        }
        for (index, (scalar, expected)) in scalars.iter().zip(self.group_types.iter()).enumerate() {
            if scalar.data_type() != *expected {
                return Err(DbError::Checkpoint(format!(
                    "Core window checkpoint group key column {index} has type {}; expected {expected}",
                    scalar.data_type()
                )));
            }
        }
        crate::aggregate_state::scalar_key_to_owned_row(
            &self.row_converter,
            &scalars,
            &self.group_types,
        )
        .map_err(|error| DbError::Checkpoint(format!("window group key materialization: {error}")))
    }

    fn decode_checkpoint_accumulators(
        &self,
        states: &[Vec<u8>],
        context: &str,
    ) -> Result<Vec<Box<dyn datafusion_expr::Accumulator>>, DbError> {
        use crate::aggregate_state::ipc_to_scalars;

        if states.len() != self.agg_specs.len() {
            return Err(DbError::Checkpoint(format!(
                "{context} checkpoint contains {} accumulator states; expected {}",
                states.len(),
                self.agg_specs.len()
            )));
        }

        let mut accumulators = Vec::new();
        accumulators
            .try_reserve_exact(self.agg_specs.len())
            .map_err(|error| {
                DbError::Checkpoint(format!("{context} accumulator reserve failed: {error}"))
            })?;
        for (spec, bytes) in self.agg_specs.iter().zip(states) {
            if bytes.is_empty() {
                return Err(DbError::Checkpoint(format!(
                    "{context} checkpoint contains an empty accumulator state"
                )));
            }
            let state_scalars = ipc_to_scalars(bytes).map_err(|error| {
                DbError::Checkpoint(format!("{context} accumulator decode failed: {error}"))
            })?;
            if state_scalars.is_empty() {
                return Err(DbError::Checkpoint(format!(
                    "{context} checkpoint contains an empty accumulator tuple"
                )));
            }
            let mut accumulator = spec.create_accumulator().map_err(|error| {
                DbError::Checkpoint(format!("{context} accumulator creation failed: {error}"))
            })?;
            let expected_state = accumulator.state().map_err(|error| {
                DbError::Checkpoint(format!(
                    "{context} accumulator schema inspection failed: {error}"
                ))
            })?;
            if state_scalars.len() != expected_state.len()
                || state_scalars
                    .iter()
                    .zip(&expected_state)
                    .any(|(saved, expected)| saved.data_type() != expected.data_type())
            {
                return Err(DbError::Checkpoint(format!(
                    "{context} checkpoint accumulator schema does not match the query"
                )));
            }
            let arrays = state_scalars
                .iter()
                .map(|scalar| {
                    scalar.to_array().map_err(|error| {
                        DbError::Checkpoint(format!(
                            "{context} accumulator scalar materialization failed: {error}"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            accumulator.merge_batch(&arrays).map_err(|error| {
                DbError::Checkpoint(format!("{context} accumulator merge failed: {error}"))
            })?;
            accumulators.push(accumulator);
        }
        Ok(accumulators)
    }

    pub(crate) fn preflight_vnode_bytes<'a>(
        &self,
        vnode: u32,
        vnode_count: u32,
        bytes: &'a [u8],
    ) -> Result<PreflightedCoreWindowVnodeArchive<'a>, DbError> {
        let vnode_count = self.validate_vnode_count(vnode_count)?;
        if vnode >= vnode_count.get() {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode {vnode} is outside vnode_count {}",
                vnode_count.get()
            )));
        }
        let checkpoint = rkyv::access::<ArchivedCoreWindowVnodeCheckpoint, rkyv::rancor::Error>(
            bytes,
        )
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "Core window vnode {vnode} checkpoint validation failed: {error}"
            ))
        })?;
        if checkpoint.vnode.to_native() != vnode {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode frame identity mismatch: frame={}, expected={vnode}",
                checkpoint.vnode.to_native()
            )));
        }
        if checkpoint.fingerprint.to_native() != self.query_fingerprint() {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode {vnode} checkpoint fingerprint mismatch"
            )));
        }
        if checkpoint.window_type != self.window_type_tag() {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode {vnode} checkpoint type mismatch"
            )));
        }
        let session = matches!(self.assigner, CoreWindowAssigner::Session { .. });
        if (session && !checkpoint.windows.is_empty())
            || (!session && !checkpoint.session_state.is_empty())
        {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode {vnode} checkpoint has non-canonical state payloads"
            )));
        }
        let expected_accumulators = self.agg_specs.len();
        if session {
            if checkpoint.session_state.len() > self.max_groups_per_window {
                return Err(DbError::Checkpoint(format!(
                    "Core window session group cardinality exceeds limit {}",
                    self.max_groups_per_window
                )));
            }
            let mut total_sessions = 0_usize;
            for group in checkpoint.session_state.iter() {
                if group.sessions.is_empty() || (self.num_group_cols == 0) != group.key.is_empty() {
                    return Err(DbError::Checkpoint(format!(
                        "Core window vnode {vnode} session checkpoint has a non-canonical group"
                    )));
                }
                total_sessions = total_sessions
                    .checked_add(group.sessions.len())
                    .filter(|count| *count <= bytes.len())
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "Core window vnode {vnode} session roster exceeds its frame"
                        ))
                    })?;
                for interval in group.sessions.iter() {
                    if interval.acc_states.len() != expected_accumulators
                        || interval
                            .acc_states
                            .iter()
                            .any(rkyv::vec::ArchivedVec::is_empty)
                    {
                        return Err(DbError::Checkpoint(format!(
                            "Core window vnode {vnode} session accumulator roster is invalid"
                        )));
                    }
                }
            }
        } else {
            let mut previous_start = None;
            let mut total_groups = 0_usize;
            for window in checkpoint.windows.iter() {
                let window_start = window.window_start.to_native();
                if previous_start.is_some_and(|previous| previous >= window_start) {
                    return Err(DbError::Checkpoint(format!(
                        "Core window vnode {vnode} checkpoint starts are not canonical"
                    )));
                }
                previous_start = Some(window_start);
                if window.groups.len() > self.max_groups_per_window {
                    return Err(DbError::Checkpoint(format!(
                        "Core window {window_start} group cardinality exceeds limit {}",
                        self.max_groups_per_window
                    )));
                }
                total_groups = total_groups
                    .checked_add(window.groups.len())
                    .filter(|count| *count <= bytes.len())
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "Core window vnode {vnode} group roster exceeds its frame"
                        ))
                    })?;
                for group in window.groups.iter() {
                    if (self.num_group_cols == 0) != group.key.is_empty()
                        || group.acc_states.len() != expected_accumulators
                        || group
                            .acc_states
                            .iter()
                            .any(rkyv::vec::ArchivedVec::is_empty)
                    {
                        return Err(DbError::Checkpoint(format!(
                            "Core window vnode {vnode} accumulator roster is invalid"
                        )));
                    }
                }
            }
        }
        if checkpoint
            .windows
            .len()
            .saturating_add(checkpoint.session_state.len())
            > bytes.len()
        {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode {vnode} checkpoint declares an impossible state roster"
            )));
        }
        Ok(PreflightedCoreWindowVnodeArchive { checkpoint })
    }

    fn prepare_vnode_restore(
        &self,
        vnode: u32,
        checkpoint: &CoreWindowVnodeCheckpoint,
        final_frontier_ms: i64,
    ) -> Result<PreparedCoreWindowRestore, DbError> {
        if checkpoint.vnode != vnode {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode frame identity mismatch: frame={}, expected={vnode}",
                checkpoint.vnode
            )));
        }
        if checkpoint.fingerprint != self.query_fingerprint() {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode {vnode} checkpoint fingerprint mismatch"
            )));
        }
        if checkpoint.window_type != self.window_type_tag() {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode {vnode} checkpoint type mismatch"
            )));
        }
        let prepared = match &self.assigner {
            CoreWindowAssigner::Session { gap_ms } => {
                self.prepare_session_window_restore(checkpoint, vnode, *gap_ms)
            }
            CoreWindowAssigner::Tumbling(assigner) => self.prepare_fixed_window_restore(
                checkpoint,
                vnode,
                assigner.size_ms(),
                assigner.size_ms(),
                assigner.offset_ms(),
            ),
            CoreWindowAssigner::Hopping(assigner) => self.prepare_fixed_window_restore(
                checkpoint,
                vnode,
                assigner.size_ms(),
                assigner.slide_ms(),
                assigner.offset_ms(),
            ),
        }?;
        if final_frontier_ms < prepared.frontier_floor_ms {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode {vnode} floor {} exceeds restored frontier {}",
                prepared.frontier_floor_ms, final_frontier_ms
            )));
        }
        if let Some(state) = prepared.state.as_deref() {
            self.validate_vnode_state_frontier(state, final_frontier_ms)?;
        }
        Ok(prepared)
    }

    fn subtract_vnode_cardinality(
        window_group_counts: &mut FxHashMap<i64, usize>,
        session_group_count: &mut usize,
        state: &CoreWindowVnodeState,
    ) -> Result<(), DbError> {
        for (&window_start, groups) in &state.windows {
            let remaining = window_group_counts
                .get(&window_start)
                .copied()
                .and_then(|count| count.checked_sub(groups.len()))
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "Core window cardinality accounting invariant failed".into(),
                    )
                })?;
            if remaining == 0 {
                window_group_counts.remove(&window_start);
            } else {
                window_group_counts.insert(window_start, remaining);
            }
        }
        *session_group_count = session_group_count
            .checked_sub(state.session_groups.len())
            .ok_or_else(|| {
                DbError::Checkpoint("Core session cardinality accounting invariant failed".into())
            })?;
        Ok(())
    }

    fn add_checkpoint_cardinality(
        &self,
        window_group_counts: &mut FxHashMap<i64, usize>,
        session_group_count: &mut usize,
        checkpoint: &CoreWindowVnodeCheckpoint,
    ) -> Result<(), DbError> {
        match &self.assigner {
            CoreWindowAssigner::Session { .. } => {
                *session_group_count = session_group_count
                    .checked_add(checkpoint.session_state.len())
                    .filter(|count| *count <= self.max_groups_per_window)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "Core window session group cardinality exceeds limit {}",
                            self.max_groups_per_window
                        ))
                    })?;
            }
            CoreWindowAssigner::Tumbling(_) | CoreWindowAssigner::Hopping(_) => {
                for window in &checkpoint.windows {
                    let count = window_group_counts
                        .get(&window.window_start)
                        .copied()
                        .unwrap_or(0)
                        .checked_add(window.groups.len())
                        .filter(|count| *count <= self.max_groups_per_window)
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "Core window {} group cardinality exceeds limit {}",
                                window.window_start, self.max_groups_per_window
                            ))
                        })?;
                    window_group_counts.insert(window.window_start, count);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn prepare_owned_vnode_transition(
        &self,
        vnode_count: u32,
        final_frontier_ms: i64,
        restores: impl ExactSizeIterator<Item = Result<OwnedCoreWindowVnodeRestore, DbError>>,
        revoked: &FxHashSet<u32>,
    ) -> Result<PreparedCoreWindowVnodeTransition, DbError> {
        let vnode_count = self.validate_vnode_count(vnode_count)?;
        if final_frontier_ms < self.high_watermark_ms {
            return Err(DbError::Checkpoint(format!(
                "Core window transition frontier {final_frontier_ms} regresses from {}",
                self.high_watermark_ms
            )));
        }
        if final_frontier_ms < self.required_frontier_floor_ms {
            return Err(DbError::Checkpoint(format!(
                "Core window transition frontier {final_frontier_ms} precedes vnode floor {}",
                self.required_frontier_floor_ms
            )));
        }
        let restore_count = restores.len();
        let mut final_window_group_counts = FxHashMap::default();
        final_window_group_counts
            .try_reserve(self.window_group_counts.len())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "Core window cardinality roster reserve failed: {error}"
                ))
            })?;
        final_window_group_counts.extend(
            self.window_group_counts
                .iter()
                .map(|(&window_start, &count)| (window_start, count)),
        );
        let mut final_session_group_count = self.session_group_count;
        let mut transitioned = FxHashSet::default();
        transitioned
            .try_reserve(restore_count.saturating_add(revoked.len()))
            .map_err(|error| {
                DbError::Checkpoint(format!("Core window transition roster reserve: {error}"))
            })?;
        for &vnode in revoked {
            if vnode >= vnode_count.get() {
                return Err(DbError::Checkpoint(format!(
                    "revoked Core window vnode {vnode} is outside vnode_count {}",
                    vnode_count.get()
                )));
            }
            transitioned.insert(vnode);
        }
        for &vnode in revoked {
            if let Some(state) = self.vnode_states[vnode as usize].as_deref() {
                Self::subtract_vnode_cardinality(
                    &mut final_window_group_counts,
                    &mut final_session_group_count,
                    state,
                )?;
            }
        }
        let mut replacements = FxHashMap::default();
        replacements.try_reserve(restore_count).map_err(|error| {
            DbError::Checkpoint(format!("Core window replacement roster reserve: {error}"))
        })?;
        let mut restored_vnodes = FxHashSet::default();
        restored_vnodes
            .try_reserve(restore_count)
            .map_err(|error| {
                DbError::Checkpoint(format!("Core window restored roster reserve: {error}"))
            })?;
        let mut required_frontier_floor_ms = self.required_frontier_floor_ms;
        for restore in restores {
            let OwnedCoreWindowVnodeRestore { vnode, state } = restore?;
            if vnode >= vnode_count.get() || !restored_vnodes.insert(vnode) {
                return Err(DbError::Checkpoint(format!(
                    "Core window transition repeats or exceeds vnode {vnode}"
                )));
            }
            if transitioned.insert(vnode) {
                if let Some(current) = self.vnode_states[vnode as usize].as_deref() {
                    Self::subtract_vnode_cardinality(
                        &mut final_window_group_counts,
                        &mut final_session_group_count,
                        current,
                    )?;
                }
            }
            self.add_checkpoint_cardinality(
                &mut final_window_group_counts,
                &mut final_session_group_count,
                &state,
            )?;
            let prepared = self.prepare_vnode_restore(vnode, &state, final_frontier_ms)?;
            required_frontier_floor_ms = required_frontier_floor_ms.max(prepared.frontier_floor_ms);
            if replacements.insert(vnode, prepared.state).is_some() {
                return Err(DbError::Checkpoint(format!(
                    "Core window transition repeats restored vnode {vnode}"
                )));
            }
        }
        if final_frontier_ms > self.high_watermark_ms {
            for &vnode in &self.active_vnodes {
                if transitioned.contains(&vnode) {
                    continue;
                }
                let state = self
                    .vnode_states
                    .get(vnode as usize)
                    .and_then(Option::as_deref)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "Core window active vnode {vnode} has no retained state"
                        ))
                    })?;
                self.validate_vnode_state_frontier(state, final_frontier_ms)?;
            }
        }
        let mut transitioned_vnodes = transitioned.into_iter().collect::<Vec<_>>();
        transitioned_vnodes.sort_unstable();
        let mut replacement_slots = Vec::new();
        replacement_slots
            .try_reserve_exact(transitioned_vnodes.len())
            .map_err(|error| {
                DbError::Checkpoint(format!("Core window transition slot reserve: {error}"))
            })?;
        for &vnode in &transitioned_vnodes {
            replacement_slots.push((vnode, replacements.remove(&vnode).flatten()));
        }
        let mut final_active_vnodes = Vec::new();
        final_active_vnodes
            .try_reserve_exact(usize::from(self.key_group_count.get()))
            .map_err(|error| {
                DbError::Checkpoint(format!("Core window active roster reserve: {error}"))
            })?;
        final_active_vnodes.extend(
            self.active_vnodes
                .iter()
                .copied()
                .filter(|vnode| transitioned_vnodes.binary_search(vnode).is_err()),
        );
        final_active_vnodes.extend(
            replacement_slots
                .iter()
                .filter_map(|(vnode, state)| state.is_some().then_some(*vnode)),
        );
        final_active_vnodes.sort_unstable();
        let mut final_active_vnode_positions = Vec::new();
        final_active_vnode_positions
            .try_reserve_exact(usize::from(self.key_group_count.get()))
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "Core window active position roster reserve failed: {error}"
                ))
            })?;
        final_active_vnode_positions.resize(usize::from(self.key_group_count.get()), usize::MAX);
        for (index, &vnode) in final_active_vnodes.iter().enumerate() {
            final_active_vnode_positions[vnode as usize] = index;
        }
        Ok(PreparedCoreWindowVnodeTransition {
            replacements: replacement_slots,
            final_active_vnodes,
            final_active_vnode_positions: final_active_vnode_positions.into_boxed_slice(),
            final_window_group_counts,
            final_session_group_count,
            final_high_watermark_ms: final_frontier_ms,
            final_required_frontier_floor_ms: required_frontier_floor_ms,
        })
    }

    pub(crate) fn publish_prepared_vnode_transition(
        &mut self,
        mut prepared: PreparedCoreWindowVnodeTransition,
    ) -> RetiredCoreWindowVnodeTransition {
        for (vnode, replacement) in &mut prepared.replacements {
            std::mem::swap(&mut self.vnode_states[*vnode as usize], replacement);
        }
        std::mem::swap(&mut self.active_vnodes, &mut prepared.final_active_vnodes);
        std::mem::swap(
            &mut self.active_vnode_positions,
            &mut prepared.final_active_vnode_positions,
        );
        std::mem::swap(
            &mut self.window_group_counts,
            &mut prepared.final_window_group_counts,
        );
        std::mem::swap(
            &mut self.session_group_count,
            &mut prepared.final_session_group_count,
        );
        std::mem::swap(
            &mut self.high_watermark_ms,
            &mut prepared.final_high_watermark_ms,
        );
        std::mem::swap(
            &mut self.required_frontier_floor_ms,
            &mut prepared.final_required_frontier_floor_ms,
        );
        self.force_full_vnode_capture();
        RetiredCoreWindowVnodeTransition {
            retired_state: prepared,
        }
    }

    pub(crate) fn finish_vnode_transition(retired: RetiredCoreWindowVnodeTransition) {
        drop(retired.retired_state);
    }

    pub(crate) fn restore_vnode(
        &mut self,
        vnode: u32,
        vnode_count: u32,
        state: CoreWindowVnodeCheckpoint,
    ) -> Result<(), DbError> {
        let final_frontier_ms = self.high_watermark_ms;
        let prepared = self.prepare_owned_vnode_transition(
            vnode_count,
            final_frontier_ms,
            std::iter::once(Ok(OwnedCoreWindowVnodeRestore { vnode, state })),
            &FxHashSet::default(),
        )?;
        let retired = self.publish_prepared_vnode_transition(prepared);
        Self::finish_vnode_transition(retired);
        Ok(())
    }

    pub(crate) fn restore_high_watermark_ms(&mut self, watermark_ms: i64) -> Result<(), DbError> {
        if watermark_ms < self.required_frontier_floor_ms {
            return Err(DbError::Checkpoint(format!(
                "Core window restored frontier {watermark_ms} precedes vnode floor {}",
                self.required_frontier_floor_ms
            )));
        }
        for state in self.vnode_states.iter().filter_map(Option::as_deref) {
            self.validate_vnode_state_frontier(state, watermark_ms)?;
        }
        self.high_watermark_ms = watermark_ms;
        Ok(())
    }

    fn validate_vnode_state_frontier(
        &self,
        state: &CoreWindowVnodeState,
        watermark_ms: i64,
    ) -> Result<(), DbError> {
        if state
            .windows
            .keys()
            .any(|window_start| self.is_window_closed_at(*window_start, watermark_ms))
            || state
                .session_deadlines
                .first()
                .is_some_and(|deadline| deadline.deadline_ms <= watermark_ms)
        {
            return Err(DbError::Checkpoint(
                "Core window restored frontier closes retained vnode state".into(),
            ));
        }
        Ok(())
    }

    fn is_window_closed_at(&self, window_start: i64, watermark_ms: i64) -> bool {
        let size_ms = match &self.assigner {
            CoreWindowAssigner::Tumbling(assigner) => assigner.size_ms(),
            CoreWindowAssigner::Hopping(assigner) => assigner.size_ms(),
            CoreWindowAssigner::Session { .. } => return false,
        };
        window_start
            .saturating_add(size_ms)
            .saturating_add(self.allowed_lateness_ms)
            <= watermark_ms
    }
}

impl CoreWindowVnodeCheckpoint {
    pub(crate) fn retained_serialization_bytes(&self) -> Result<usize, DbError> {
        fn add(total: &mut usize, bytes: usize) -> Result<(), DbError> {
            *total = total.checked_add(bytes).ok_or_else(|| {
                DbError::Checkpoint("Core window checkpoint byte accounting overflow".into())
            })?;
            Ok(())
        }

        fn roster<T>(capacity: usize) -> Result<usize, DbError> {
            capacity
                .checked_mul(std::mem::size_of::<T>())
                .ok_or_else(|| {
                    DbError::Checkpoint("Core window checkpoint roster accounting overflow".into())
                })
        }

        let mut bytes = roster::<WindowCheckpoint>(self.windows.capacity())?;
        for window in &self.windows {
            add(
                &mut bytes,
                roster::<GroupCheckpoint>(window.groups.capacity())?,
            )?;
            for group in &window.groups {
                add(&mut bytes, group.key.capacity())?;
                add(&mut bytes, roster::<Vec<u8>>(group.acc_states.capacity())?)?;
                for state in &group.acc_states {
                    add(&mut bytes, state.capacity())?;
                }
            }
        }
        add(
            &mut bytes,
            roster::<SessionGroupCheckpoint>(self.session_state.capacity())?,
        )?;
        for group in &self.session_state {
            add(&mut bytes, group.key.capacity())?;
            add(
                &mut bytes,
                roster::<SessionCheckpoint>(group.sessions.capacity())?,
            )?;
            for session in &group.sessions {
                add(
                    &mut bytes,
                    roster::<Vec<u8>>(session.acc_states.capacity())?,
                )?;
                for state in &session.acc_states {
                    add(&mut bytes, state.capacity())?;
                }
            }
        }
        Ok(bytes)
    }
}

impl CoreWindowVnodeCheckpointCapture {
    #[must_use]
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub(crate) fn encode(
        self,
        max_working_bytes: usize,
    ) -> Result<CoreWindowVnodeCheckpoint, DbError> {
        use crate::aggregate_state::{row_to_scalar_key_with_types, scalars_to_ipc_bounded};

        fn checked_product(left: usize, right: usize, component: &str) -> Result<usize, DbError> {
            left.checked_mul(right).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "Core window {component} checkpoint accounting overflow"
                ))
            })
        }

        fn checked_sum(
            values: impl IntoIterator<Item = usize>,
            component: &str,
        ) -> Result<usize, DbError> {
            values.into_iter().try_fold(0_usize, |total, value| {
                total.checked_add(value).ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "Core window {component} checkpoint accounting overflow"
                    ))
                })
            })
        }

        fn array_scratch_bytes(scalars: &[ScalarValue], component: &str) -> Result<usize, DbError> {
            let payload = checked_sum(scalars.iter().map(ScalarValue::size), component)?;
            checked_sum(
                [
                    checked_product(payload, 2, component)?,
                    checked_product(scalars.len(), 32, component)?,
                    checked_product(scalars.len(), std::mem::size_of::<ArrayRef>(), component)?,
                ],
                component,
            )
        }

        fn row_scratch_bytes(
            payload_bytes: usize,
            columns: usize,
            component: &str,
        ) -> Result<usize, DbError> {
            checked_sum(
                [
                    checked_product(payload_bytes, 2, component)?,
                    checked_product(columns, 32, component)?,
                    checked_product(columns, std::mem::size_of::<ArrayRef>(), component)?,
                ],
                component,
            )
        }

        fn retain_roster<T>(
            remaining: &mut usize,
            capacity: usize,
            component: &str,
        ) -> Result<(), DbError> {
            let bytes = checked_product(capacity, std::mem::size_of::<T>(), component)?;
            *remaining = remaining.checked_sub(bytes).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "Core window {component} exceeded its cumulative encode budget"
                ))
            })?;
            Ok(())
        }

        fn encode_scalars(
            scalars: &[ScalarValue],
            scratch_bytes: usize,
            remaining: &mut usize,
            context: &str,
        ) -> Result<Vec<u8>, DbError> {
            let limit = remaining.checked_sub(scratch_bytes).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "Core window {context} scratch exceeded its cumulative encode budget"
                ))
            })?;
            let encoded = scalars_to_ipc_bounded(scalars, limit).map_err(|error| {
                DbError::Checkpoint(format!("Core window {context} encode failed: {error}"))
            })?;
            *remaining = remaining.checked_sub(encoded.capacity()).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "Core window {context} exceeded its cumulative encode budget"
                ))
            })?;
            Ok(encoded)
        }

        let mut remaining = max_working_bytes;
        retain_roster::<WindowCheckpoint>(&mut remaining, self.windows.len(), "window roster")?;
        let mut windows = Vec::new();
        windows
            .try_reserve_exact(self.windows.len())
            .map_err(|error| {
                DbError::Checkpoint(format!("Core window checkpoint roster reserve: {error}"))
            })?;
        for window in self.windows {
            retain_roster::<GroupCheckpoint>(&mut remaining, window.groups.len(), "group roster")?;
            let mut groups = Vec::new();
            groups
                .try_reserve_exact(window.groups.len())
                .map_err(|error| {
                    DbError::Checkpoint(format!("Core window checkpoint group reserve: {error}"))
                })?;
            for group in window.groups {
                let key_scratch = row_scratch_bytes(
                    group.key.as_ref().len(),
                    self.group_types.len(),
                    "group key",
                )?;
                let key_scalars = row_to_scalar_key_with_types(
                    &self.row_converter,
                    &group.key,
                    &self.group_types,
                )?;
                let key = encode_scalars(&key_scalars, key_scratch, &mut remaining, "group key")?;
                drop(key_scalars);
                retain_roster::<Vec<u8>>(
                    &mut remaining,
                    group.accumulator_states.len(),
                    "accumulator roster",
                )?;
                let mut acc_states = Vec::new();
                acc_states
                    .try_reserve_exact(group.accumulator_states.len())
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "Core window accumulator roster reserve failed: {error}"
                        ))
                    })?;
                for state in &group.accumulator_states {
                    let scratch = array_scratch_bytes(state, "accumulator")?;
                    acc_states.push(encode_scalars(
                        state,
                        scratch,
                        &mut remaining,
                        "accumulator",
                    )?);
                }
                groups.push(GroupCheckpoint { key, acc_states });
            }
            windows.push(WindowCheckpoint {
                window_start: window.window_start,
                groups,
            });
        }
        retain_roster::<SessionGroupCheckpoint>(
            &mut remaining,
            self.session_state.len(),
            "session group roster",
        )?;
        let mut session_state = Vec::new();
        session_state
            .try_reserve_exact(self.session_state.len())
            .map_err(|error| {
                DbError::Checkpoint(format!("Core window session roster reserve: {error}"))
            })?;
        for group in self.session_state {
            let key_scratch = row_scratch_bytes(
                group.key.as_ref().as_ref().len(),
                self.group_types.len(),
                "session key",
            )?;
            let key_scalars = row_to_scalar_key_with_types(
                &self.row_converter,
                group.key.as_ref(),
                &self.group_types,
            )?;
            let key = encode_scalars(&key_scalars, key_scratch, &mut remaining, "session key")?;
            drop(key_scalars);
            retain_roster::<SessionCheckpoint>(
                &mut remaining,
                group.sessions.len(),
                "session roster",
            )?;
            let mut sessions = Vec::new();
            sessions
                .try_reserve_exact(group.sessions.len())
                .map_err(|error| {
                    DbError::Checkpoint(format!("Core window interval roster reserve: {error}"))
                })?;
            for session in group.sessions {
                retain_roster::<Vec<u8>>(
                    &mut remaining,
                    session.accumulator_states.len(),
                    "session accumulator roster",
                )?;
                let mut acc_states = Vec::new();
                acc_states
                    .try_reserve_exact(session.accumulator_states.len())
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "Core window session accumulator roster reserve failed: {error}"
                        ))
                    })?;
                for state in &session.accumulator_states {
                    let scratch = array_scratch_bytes(state, "session accumulator")?;
                    acc_states.push(encode_scalars(
                        state,
                        scratch,
                        &mut remaining,
                        "session accumulator",
                    )?);
                }
                sessions.push(SessionCheckpoint {
                    start: session.start,
                    end: session.end,
                    acc_states,
                });
            }
            session_state.push(SessionGroupCheckpoint { key, sessions });
        }
        let checkpoint = CoreWindowVnodeCheckpoint {
            fingerprint: self.fingerprint,
            vnode: self.vnode,
            windows,
            session_state,
            window_type: self.window_type,
            frontier_floor_ms: self.frontier_floor_ms,
        };
        debug_assert!(checkpoint
            .retained_serialization_bytes()
            .is_ok_and(|bytes| bytes <= max_working_bytes));
        Ok(checkpoint)
    }
}

impl PreparedCoreWindowVnodeTransition {
    #[must_use]
    #[cfg(feature = "cluster")]
    pub(crate) fn accounted_state_bytes(&self) -> usize {
        let roster_bytes = std::mem::size_of::<Self>()
            .saturating_add(
                self.replacements
                    .capacity()
                    .saturating_mul(
                        std::mem::size_of::<(u32, Option<Box<CoreWindowVnodeState>>)>(),
                    ),
            )
            .saturating_add(
                self.final_active_vnodes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(
                self.final_active_vnode_positions
                    .len()
                    .saturating_mul(std::mem::size_of::<usize>()),
            )
            .saturating_add(
                self.final_window_group_counts
                    .capacity()
                    .saturating_mul(std::mem::size_of::<(i64, usize)>()),
            );
        self.replacements
            .iter()
            .filter_map(|(_, state)| state.as_deref())
            .fold(roster_bytes, |bytes, state| {
                bytes
                    .saturating_add(std::mem::size_of::<CoreWindowVnodeState>())
                    .saturating_add(state.accounted_state_bytes)
            })
    }
}

impl RetiredCoreWindowVnodeTransition {
    #[must_use]
    #[cfg(feature = "cluster")]
    pub(crate) fn accounted_state_bytes(&self) -> usize {
        self.retired_state.accounted_state_bytes()
    }
}

#[cfg(test)]
mod tests;
