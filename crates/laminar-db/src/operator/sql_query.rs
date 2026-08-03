//! Standard SQL query operator with lazy initialization.
//!
//! Handles all non-EOWC, non-join queries. On first `process()` call,
//! introspects the SQL via `DataFusion` to determine the execution path:
//! - Aggregate (GROUP BY) -> incremental accumulators
//! - Simple single-source -> compiled `PhysicalExpr` projection
//! - Complex non-aggregate -> cached physical plan (`LiveSourceExec` reads fresh data)

use std::ops::ControlFlow;
use std::sync::Arc;

#[cfg(feature = "cluster")]
use std::collections::VecDeque;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionContext;
#[cfg(feature = "cluster")]
use laminar_core::shuffle::ShuffleMessage;
use laminar_core::state::KeyGroupCount;
use sqlparser::ast::{
    visit_expressions, Expr, GroupByExpr, Query, Select, SetExpr, Statement, TableFactor,
};

#[cfg(all(feature = "cluster", test))]
use crate::aggregate_state::merge_serialized_agg_cps;
#[cfg(all(feature = "cluster", test))]
use crate::aggregate_state::validate_agg_checkpoint_slice;
use crate::aggregate_state::{
    apply_compiled_having, AggStateCheckpoint, CompiledProjection, IncrementalAggState,
};
#[cfg(feature = "cluster")]
use crate::aggregate_state::{
    OwnedAggVnodeRestore, PreparedAggVnodeTransition, RetiredAggVnodeTransition,
};
use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
#[cfg(feature = "cluster")]
use crate::operator::capability::{ManagedStateContract, OperatorStateClass};
use crate::operator::capability::{OperatorCapability, OperatorImplementation};
#[cfg(feature = "cluster")]
use crate::operator_graph::ManagedVnodeTransition;
use crate::operator_graph::{
    try_evaluate_compiled, GraphOperator, ManagedStateAccountingSnapshot, OperatorCheckpoint,
};
use crate::sql_analysis::{extract_projection_filter, single_source_table};

#[cfg(all(feature = "cluster", test))]
use bytes::Bytes;

// Resolved on first `process()` call by introspecting the SQL.
enum QueryState {
    Uninit,
    Agg(Box<IncrementalAggState>),
    Compiled(CompiledProjection),
    // Single-source, non-compilable; `LiveSourceExec` feeds fresh data per cycle.
    CachedPlan(Arc<dyn datafusion::physical_plan::ExecutionPlan>),
    // Multi-source JOIN; the hash table is rebuilt each cycle from fresh live-source data.
    CachedPhysical(Arc<dyn datafusion::physical_plan::ExecutionPlan>),
}

#[cfg(feature = "cluster")]
struct PreparedSqlVnodeTransition {
    aggregate: PreparedAggVnodeTransition,
    next_prev_owned: rustc_hash::FxHashSet<u32>,
}

#[cfg(feature = "cluster")]
enum SqlVnodeTransitionCleanup {
    Aborted(PreparedSqlVnodeTransition),
    Published {
        aggregate: RetiredAggVnodeTransition,
        prev_owned: rustc_hash::FxHashSet<u32>,
    },
}

/// Pre-aggregate row-shuffle config for cluster mode.
#[cfg(feature = "cluster")]
#[derive(Clone)]
pub struct ClusterShuffleConfig {
    pub registry: Arc<laminar_core::state::VnodeRegistry>,
    pub sender: Arc<laminar_core::shuffle::ShuffleSender>,
    pub receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    pub self_id: laminar_core::state::NodeId,
}

#[cfg(feature = "cluster")]
impl std::fmt::Debug for ClusterShuffleConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterShuffleConfig")
            .field("self_id", &self.self_id)
            .finish_non_exhaustive()
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct AggOpCheckpoint {
    agg: Option<AggStateCheckpoint>,
    // Pre-barrier remote rows are channel state. They must be replayed after the cut rather than
    // folded into aggregate state before its corresponding output is emitted.
    aligned_replay: Vec<(u64, i64, Vec<u8>)>,
}

/// Serialize a per-vnode aggregate checkpoint slice (full or a delta's changed-groups) to bytes.
#[cfg(feature = "cluster")]
fn serialize_agg_cp(cp: &AggStateCheckpoint, op_name: &str) -> Result<Vec<u8>, DbError> {
    rkyv::to_bytes::<rkyv::rancor::Error>(cp)
        .map(|v| v.to_vec())
        .map_err(|e| {
            DbError::Pipeline(format!(
                "per-vnode checkpoint serialization for '{op_name}': {e}"
            ))
        })
}

fn is_direct_single_source_shape(query: &Query, select: &Select) -> bool {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
        || select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
        || select.from.len() != 1
        || !select.from[0].joins.is_empty()
    {
        return false;
    }

    matches!(
        &select.from[0].relation,
        TableFactor::Table {
            args: None,
            with_hints,
            version: None,
            with_ordinality: false,
            partitions,
            json_path: None,
            sample: None,
            index_hints,
            ..
        } if with_hints.is_empty() && partitions.is_empty() && index_hints.is_empty()
    )
}

fn is_stream_window_marker(name: &str) -> bool {
    matches!(name, "TUMBLE" | "HOP" | "SLIDE" | "SESSION" | "CUMULATE")
}

/// Conservatively classify the immutable SQL before lazy execution-path initialization.
///
/// The parser analysis is exact for direct aggregate and single-source projection/filter shapes.
/// Complex structure, unknown functions, analytics, and unrecognized grouping remain local-only;
/// the descriptor must never guess "stateless".
fn classify_sql_capability(sql: &str, ctx: &SessionContext) -> OperatorCapability {
    let Ok(statements) = laminar_sql::parse_streaming_sql(sql) else {
        return OperatorCapability::unclassified_sql_query();
    };
    if statements.len() != 1 {
        return OperatorCapability::unclassified_sql_query();
    }
    let Some(laminar_sql::parser::StreamingStatement::Standard(statement)) = statements.first()
    else {
        return OperatorCapability::unclassified_sql_query();
    };
    let Statement::Query(query) = statement.as_ref() else {
        return OperatorCapability::unclassified_sql_query();
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return OperatorCapability::unclassified_sql_query();
    };

    let function_registry = ctx.state();
    let mut has_registered_aggregate = false;
    let mut has_stream_window = false;
    let mut has_ambiguous_expression = false;
    let _ = visit_expressions(statement.as_ref(), |expression| {
        if let Expr::Function(function) = expression {
            if function.over.is_some() {
                has_ambiguous_expression = true;
                return ControlFlow::Break(());
            }
            let name = function.name.to_string();
            let normalized = name.to_ascii_lowercase();
            if function_registry
                .aggregate_functions()
                .contains_key(&normalized)
            {
                has_registered_aggregate = true;
            } else if is_stream_window_marker(&name.to_ascii_uppercase()) {
                has_stream_window = true;
            } else if !function_registry
                .scalar_functions()
                .contains_key(&normalized)
            {
                has_ambiguous_expression = true;
                return ControlFlow::Break(());
            }
        } else if matches!(
            expression,
            Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::Subquery(_)
        ) {
            has_ambiguous_expression = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    if has_ambiguous_expression {
        return OperatorCapability::unclassified_sql_query();
    }

    let aggregation =
        laminar_sql::parser::aggregation_parser::analyze_aggregates(statement.as_ref());
    let has_grouping = match &select.group_by {
        GroupByExpr::Expressions(expressions, _) => !expressions.is_empty(),
        GroupByExpr::All(_) => true,
    };
    if !is_direct_single_source_shape(query, select) {
        return OperatorCapability::unclassified_sql_query();
    }

    if aggregation.has_aggregates() || has_registered_aggregate {
        return if has_grouping {
            if has_stream_window {
                OperatorCapability::windowed_keyed_sql_aggregate()
            } else {
                OperatorCapability::keyed_sql_aggregate()
            }
        } else {
            OperatorCapability::global_sql_aggregate()
        };
    }

    if has_grouping || select.having.is_some() {
        OperatorCapability::unclassified_sql_query()
    } else {
        OperatorCapability::stateless_sql_query()
    }
}

pub(crate) struct SqlQueryOperator {
    op_name: Arc<str>,
    sql: String,
    capability: OperatorCapability,
    ctx: SessionContext,
    task_ctx: Arc<TaskContext>,
    state: QueryState,
    key_group_count: KeyGroupCount,
    prom: Option<Arc<EngineMetrics>>,
    max_retractable_extremum_checkpoint_bytes: usize,
    pending_restore: Option<AggStateCheckpoint>,
    // Validated vnode bases retained in their original serialized form while the SQL aggregate is
    // still uninitialized. Lazy initialization merges the entire set once, avoiding a whole-state
    // clone and IPC rewrite for every recovered vnode.
    #[cfg(all(feature = "cluster", test))]
    pending_restore_slices: Vec<Bytes>,
    #[cfg(all(feature = "cluster", test))]
    pending_restore_slice_fingerprint: Option<u64>,
    execution_path_logged: bool,
    having_cache: Option<super::HavingSqlCache>,
    emit_changelog: bool,
    #[cfg(feature = "cluster")]
    cluster_shuffle: Option<ClusterShuffleConfig>,
    // Vnodes owned at the last capture; the diff vs the current owned-set yields the vnodes acquired
    // since, whose delta chains must re-base FULL (`IncrementalAggState::reset_acquired_vnodes`).
    #[cfg(feature = "cluster")]
    prev_owned: rustc_hash::FxHashSet<u32>,
    // `Some(chain_bound)` enables incremental delta checkpoints with that re-base bound.
    #[cfg(feature = "cluster")]
    delta_chain_bound: Option<u32>,
    // Deltas seen during restart (state Uninit), replayed after `lazy_init` restores the base.
    #[cfg(all(feature = "cluster", test))]
    pending_restore_deltas: Vec<crate::aggregate_state::AggVnodeDelta>,
    // Vnodes revoked while still Uninit: their groups sit in `pending_restore`/`pending_restore_deltas`
    // and can't be dropped yet. Re-applied via `drop_vnodes` once `lazy_init` folds the restore so
    // ownership loss is reflected before any output or later re-acquire.
    #[cfg(all(feature = "cluster", test))]
    deferred_revoke_vnodes: rustc_hash::FxHashSet<u32>,
    #[cfg(feature = "cluster")]
    aligned_replay: VecDeque<(u64, i64, crate::operator::RetainedBatch)>,
    #[cfg(feature = "cluster")]
    prepared_vnode_transition: Option<PreparedSqlVnodeTransition>,
    #[cfg(feature = "cluster")]
    vnode_transition_cleanup: Option<SqlVnodeTransitionCleanup>,
}

/// Classify a failure from a step that may already have changed operator state.
///
/// Ordinary errors are not safe to isolate or retry once mutation may have begun. Existing
/// recovery and halt dispositions retain their stronger classification.
fn stateful_apply_outcome_unknown(op_name: &str, phase: &str, error: DbError) -> DbError {
    if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
        return error;
    }
    DbError::StatefulOperatorPartialApply(format!(
        "aggregate '{op_name}' {phase} failed after state application began; the apply outcome is unknown: {error}"
    ))
}

impl SqlQueryOperator {
    #[cfg(test)]
    pub fn new(
        name: &str,
        sql: &str,
        ctx: SessionContext,
        prom: Option<Arc<EngineMetrics>>,
        emit_changelog: bool,
    ) -> Self {
        Self::new_with_key_groups(
            name,
            sql,
            ctx,
            prom,
            emit_changelog,
            KeyGroupCount::try_from(1_u16).expect("one test key group is valid"),
        )
    }

    pub fn new_with_key_groups(
        name: &str,
        sql: &str,
        ctx: SessionContext,
        prom: Option<Arc<EngineMetrics>>,
        emit_changelog: bool,
        key_group_count: KeyGroupCount,
    ) -> Self {
        let capability = classify_sql_capability(sql, &ctx);
        let task_ctx = ctx.task_ctx();
        Self {
            op_name: Arc::from(name),
            sql: sql.to_string(),
            capability,
            ctx,
            task_ctx,
            state: QueryState::Uninit,
            key_group_count,
            prom,
            max_retractable_extremum_checkpoint_bytes:
                crate::config::DEFAULT_MAX_RETRACTABLE_EXTREMUM_CHECKPOINT_BYTES,
            pending_restore: None,
            #[cfg(all(feature = "cluster", test))]
            pending_restore_slices: Vec::new(),
            #[cfg(all(feature = "cluster", test))]
            pending_restore_slice_fingerprint: None,
            execution_path_logged: false,
            having_cache: None,
            emit_changelog,
            #[cfg(feature = "cluster")]
            cluster_shuffle: None,
            #[cfg(feature = "cluster")]
            prev_owned: rustc_hash::FxHashSet::default(),
            #[cfg(feature = "cluster")]
            delta_chain_bound: None,
            #[cfg(all(feature = "cluster", test))]
            pending_restore_deltas: Vec::new(),
            #[cfg(all(feature = "cluster", test))]
            deferred_revoke_vnodes: rustc_hash::FxHashSet::default(),
            #[cfg(feature = "cluster")]
            aligned_replay: VecDeque::new(),
            #[cfg(feature = "cluster")]
            prepared_vnode_transition: None,
            #[cfg(feature = "cluster")]
            vnode_transition_cleanup: None,
        }
    }

    /// Enable incremental delta checkpoints with `chain_bound` as the re-base bound.
    #[cfg(feature = "cluster")]
    pub fn enable_delta_checkpoints(&mut self, chain_bound: u32) {
        self.delta_chain_bound = Some(chain_bound);
        if let QueryState::Agg(ref mut agg) = self.state {
            agg.set_delta_enabled(true);
        }
    }

    /// Cluster aggregate groups recover only from their assignment-scoped vnode partials. The
    /// portable graph checkpoint may still carry aligned shuffle replay.
    #[cfg(feature = "cluster")]
    fn skip_whole_node_agg(&self) -> bool {
        self.cluster_shuffle.is_some()
    }

    #[cfg(feature = "cluster")]
    pub fn attach_cluster_shuffle(&mut self, config: ClusterShuffleConfig) {
        self.key_group_count = KeyGroupCount::try_from(config.registry.vnode_count())
            .expect("vnode registry count must fit the checkpoint key-group ABI");
        self.cluster_shuffle = Some(config);
    }

    /// Vnodes owned now but not at the last capture; advances `prev_owned`. The agg re-bases their
    /// delta chains FULL (a just-acquired vnode has no parent epoch on this node).
    #[cfg(feature = "cluster")]
    fn take_newly_acquired(&mut self, required_vnodes: &[u32]) -> rustc_hash::FxHashSet<u32> {
        let owned: rustc_hash::FxHashSet<u32> = required_vnodes.iter().copied().collect();
        let newly: rustc_hash::FxHashSet<u32> =
            owned.difference(&self.prev_owned).copied().collect();
        self.prev_owned = owned;
        newly
    }

    #[cfg(all(feature = "cluster", not(test)))]
    fn staged_pending_restore(&self) -> Option<AggStateCheckpoint> {
        self.pending_restore.clone()
    }

    #[cfg(all(feature = "cluster", test))]
    fn staged_pending_restore(&self) -> Result<Option<AggStateCheckpoint>, DbError> {
        if self.pending_restore_slices.is_empty() {
            return Ok(self.pending_restore.clone());
        }
        let mut slices = Vec::with_capacity(
            self.pending_restore_slices
                .len()
                .saturating_add(usize::from(self.pending_restore.is_some())),
        );
        if let Some(checkpoint) = &self.pending_restore {
            slices.push(Bytes::from(serialize_agg_cp(checkpoint, &self.op_name)?));
        }
        slices.extend(self.pending_restore_slices.iter().cloned());
        let merged = merge_serialized_agg_cps(&slices).map_err(|error| {
            DbError::Checkpoint(format!(
                "aggregate '{}' vnode baseline merge failed: {error}",
                self.op_name
            ))
        })?;
        rkyv::from_bytes::<AggStateCheckpoint, rkyv::rancor::Error>(&merged)
            .map(Some)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' merged vnode baseline decode failed: {error}",
                    self.op_name
                ))
            })
    }

    #[cfg(not(feature = "cluster"))]
    fn staged_pending_restore(&self) -> Option<AggStateCheckpoint> {
        self.pending_restore.clone()
    }

    #[allow(clippy::too_many_lines)]
    async fn lazy_init(&mut self) -> Result<(), DbError> {
        match IncrementalAggState::try_from_sql(
            &self.ctx,
            &self.sql,
            self.emit_changelog,
            self.key_group_count,
        )
        .await
        {
            Ok(Some(mut agg_state)) => {
                if self.emit_changelog
                    && (agg_state.having_filter().is_some() || agg_state.having_sql().is_some())
                {
                    return Err(DbError::Pipeline(format!(
                        "aggregate '{}' cannot use HAVING with changelog output until transition-aware HAVING retractions are implemented",
                        self.op_name
                    )));
                }
                agg_state.set_max_retractable_extremum_checkpoint_bytes(
                    self.max_retractable_extremum_checkpoint_bytes,
                );
                #[cfg(feature = "cluster")]
                if self.cluster_shuffle.is_some() {
                    let expected_state_class = if agg_state.num_group_cols() == 0 {
                        OperatorStateClass::GlobalSingleton
                    } else {
                        OperatorStateClass::VnodeKeyed
                    };
                    if self.capability.managed_state != Some(ManagedStateContract::SqlAggregateV1)
                        || self.capability.state_class != expected_state_class
                    {
                        return Err(DbError::Pipeline(format!(
                            "[{}] query '{}': initialized aggregate state does not match its immutable cluster capability ({:?}, {:?})",
                            laminar_core::error_codes::CLUSTER_STATE_LIFECYCLE_UNSUPPORTED,
                            self.op_name,
                            self.capability.state_class,
                            self.capability.managed_state
                        )));
                    }
                }
                #[cfg(all(feature = "cluster", test))]
                let staged_pending_restore = self.staged_pending_restore()?;
                #[cfg(any(not(feature = "cluster"), all(feature = "cluster", not(test))))]
                let staged_pending_restore = self.staged_pending_restore();
                if let Some(ref cp) = staged_pending_restore {
                    let restored = agg_state.restore_groups(cp).map_err(|error| {
                        DbError::Checkpoint(format!(
                            "aggregate '{}' baseline restore failed: {error}",
                            self.op_name
                        ))
                    })?;
                    tracing::info!(
                        query = %self.op_name,
                        groups = restored,
                        "lazy_init fold: restored pending aggregate baseline"
                    );
                }
                #[cfg(all(feature = "cluster", test))]
                for delta in &self.pending_restore_deltas {
                    agg_state.apply_delta(delta).map_err(|error| {
                        DbError::Checkpoint(format!(
                            "aggregate '{}' delta restore failed: {error}",
                            self.op_name
                        ))
                    })?;
                }
                // Vnodes revoked while we were Uninit: drop them now that the restore is folded in,
                // before any output or later re-acquire can expose state this node no longer owns.
                #[cfg(all(feature = "cluster", test))]
                if !self.deferred_revoke_vnodes.is_empty() {
                    let vc = self
                        .cluster_shuffle
                        .as_ref()
                        .map(|c| c.registry.vnode_count())
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "aggregate '{}' cannot apply deferred vnode revocation without cluster ownership",
                                self.op_name
                            ))
                        })?;
                    tracing::info!(
                        query = %self.op_name,
                        vnodes = self.deferred_revoke_vnodes.len(),
                        "lazy_init fold: dropping deferred-revoked vnodes"
                    );
                    agg_state.drop_vnodes(&self.deferred_revoke_vnodes, vc)?;
                }

                self.pending_restore = None;
                #[cfg(all(feature = "cluster", test))]
                {
                    self.pending_restore_slices.clear();
                    self.pending_restore_slice_fingerprint = None;
                    self.pending_restore_deltas.clear();
                    self.deferred_revoke_vnodes.clear();
                }
                #[cfg(feature = "cluster")]
                if self.delta_chain_bound.is_some() {
                    agg_state.set_delta_enabled(true);
                }
                self.log_execution_path(agg_state.compiled_projection().is_some());
                self.state = QueryState::Agg(Box::new(agg_state));
                return Ok(());
            }
            Ok(None) => {}
            Err(e) => {
                tracing::debug!(
                    query = %self.op_name,
                    error = %e,
                    "Could not introspect query plan for aggregate detection, using cached plan"
                );
            }
        }

        let df = self
            .ctx
            .sql(&self.sql)
            .await
            .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
        let plan = df.logical_plan().clone();

        #[cfg(feature = "cluster")]
        if self.cluster_shuffle.is_some() && crate::aggregate_state::find_aggregate(&plan).is_some()
        {
            return Err(DbError::Pipeline(format!(
                "[{}] query '{}': cluster aggregate cannot use a node-local DataFusion fallback; the exact incremental execution path was not constructed",
                laminar_core::error_codes::CLUSTER_STATE_LIFECYCLE_UNSUPPORTED,
                self.op_name
            )));
        }

        if single_source_table(&self.sql).is_some() {
            if let Some(proj) = self.try_build_compiled_projection(&plan) {
                tracing::debug!(
                    query = %self.op_name,
                    "Non-aggregate single-source query compiled to PhysicalExpr"
                );
                self.log_execution_path(true);
                self.state = QueryState::Compiled(proj);
                return Ok(());
            }
            let physical = self
                .ctx
                .state()
                .create_physical_plan(&plan)
                .await
                .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
            self.log_execution_path(false);
            self.state = QueryState::CachedPlan(physical);
        } else {
            let physical = self
                .ctx
                .state()
                .create_physical_plan(&plan)
                .await
                .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
            self.log_execution_path(false);
            self.state = QueryState::CachedPhysical(physical);
        }
        Ok(())
    }

    fn log_execution_path(&mut self, compiled: bool) {
        if self.execution_path_logged {
            return;
        }
        self.execution_path_logged = true;
        if let Some(ref m) = self.prom {
            if compiled {
                m.queries_compiled.inc();
            } else {
                m.queries_cached_plan.inc();
            }
        }
    }

    fn try_build_compiled_projection(
        &self,
        plan: &datafusion_expr::LogicalPlan,
    ) -> Option<CompiledProjection> {
        let info = extract_projection_filter(plan)?;
        let state = self.ctx.state();
        let props = state.execution_props();
        let mut compiled_exprs = Vec::with_capacity(info.proj_exprs.len());
        let mut proj_fields = Vec::with_capacity(info.proj_exprs.len());

        for expr in &info.proj_exprs {
            let phys =
                datafusion::physical_expr::create_physical_expr(expr, &info.input_df_schema, props)
                    .ok()?;
            let dt = phys.data_type(info.input_df_schema.as_arrow()).ok()?;
            let name = match expr {
                datafusion_expr::Expr::Column(col) => col.name.clone(),
                datafusion_expr::Expr::Alias(alias) => alias.name.clone(),
                _ => expr.schema_name().to_string(),
            };
            proj_fields.push(arrow::datatypes::Field::new(name, dt, true));
            compiled_exprs.push(phys);
        }

        let compiled_filter = if let Some(ref pred) = info.filter_predicate {
            Some(
                datafusion::physical_expr::create_physical_expr(pred, &info.input_df_schema, props)
                    .ok()?,
            )
        } else {
            None
        };

        // When the source carries a Z-set weight (it's a changelog), pass `__weight` through so a
        // chained projection/filter propagates retractions. Skipped if the projection selects it.
        let weight = laminar_core::changelog::WEIGHT_COLUMN;
        if info
            .input_df_schema
            .as_arrow()
            .column_with_name(weight)
            .is_some()
            && !proj_fields.iter().any(|f| f.name() == weight)
        {
            let weight_expr = datafusion::physical_expr::create_physical_expr(
                &datafusion_expr::col(weight),
                &info.input_df_schema,
                props,
            )
            .ok()?;
            proj_fields.push(arrow::datatypes::Field::new(
                weight,
                arrow::datatypes::DataType::Int64,
                false,
            ));
            compiled_exprs.push(weight_expr);
        }

        let output_schema = Arc::new(arrow::datatypes::Schema::new(proj_fields));
        Some(CompiledProjection {
            exprs: compiled_exprs,
            filter: compiled_filter,
            output_schema,
        })
    }

    async fn build_and_cache_physical_plan(&mut self) -> Result<(), DbError> {
        let df = self
            .ctx
            .sql(&self.sql)
            .await
            .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
        let plan = df.logical_plan().clone();
        let physical = self
            .ctx
            .state()
            .create_physical_plan(&plan)
            .await
            .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
        self.state = QueryState::CachedPlan(physical);
        Ok(())
    }

    async fn execute_cached_plan(&self) -> Result<Vec<RecordBatch>, DbError> {
        let QueryState::CachedPlan(ref plan) = self.state else {
            return Err(DbError::Pipeline(
                "internal: execute_cached_plan called on non-CachedPlan state".into(),
            ));
        };
        datafusion::physical_plan::collect(plan.clone(), self.task_ctx.clone())
            .await
            .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))
    }

    async fn execute_agg(
        &mut self,
        inputs: &[RecordBatch],
        watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        #[cfg(feature = "cluster")]
        if !self.aligned_replay.is_empty() {
            return self.execute_aligned_replay().await;
        }
        let QueryState::Agg(ref mut agg_state) = self.state else {
            return Err(DbError::Pipeline(
                "internal: execute_agg called on non-agg state".into(),
            ));
        };

        let pre_agg_batches = if let Some(proj) = agg_state.compiled_projection() {
            match try_evaluate_compiled(proj, inputs) {
                Ok(result) => result,
                Err(e) => {
                    tracing::debug!(
                        query = %self.op_name,
                        error = %e,
                        "Compiled pre-agg projection failed, falling back to cached plan"
                    );
                    if let Some(physical) = agg_state.cached_pre_agg_physical() {
                        super::execute_cached_physical(
                            self.task_ctx.clone(),
                            &self.op_name,
                            physical,
                        )
                        .await?
                    } else {
                        return Err(DbError::Pipeline(format!(
                            "[LDB-8051] query '{}': compiled pre-agg failed and no cached plan: {e}",
                            self.op_name
                        )));
                    }
                }
            }
        } else if let Some(physical) = agg_state.cached_pre_agg_physical() {
            super::execute_cached_physical(self.task_ctx.clone(), &self.op_name, physical).await?
        } else {
            return Err(DbError::Pipeline(format!(
                "[LDB-8050] query '{}': no compiled projection or cached plan",
                self.op_name
            )));
        };

        #[cfg(feature = "cluster")]
        let (pre_agg_batches, _shuffle_admission) = {
            let QueryState::Agg(ref agg_state) = self.state else {
                unreachable!();
            };
            let num_group_cols = agg_state.num_group_cols();
            shuffle_pre_agg_batches(
                self.cluster_shuffle.as_ref(),
                &self.op_name,
                num_group_cols,
                pre_agg_batches,
            )
            .await?
        };

        {
            let op_name = self.op_name.as_ref();
            let QueryState::Agg(ref mut aggregate) = self.state else {
                unreachable!();
            };
            #[cfg(feature = "cluster")]
            for (batch, vnode) in &pre_agg_batches {
                aggregate
                    .process_batch_for_vnode(batch, watermark, *vnode)
                    .map_err(|error| {
                        stateful_apply_outcome_unknown(op_name, "state update", error)
                    })?;
            }
            #[cfg(not(feature = "cluster"))]
            for batch in &pre_agg_batches {
                aggregate.process_batch(batch, watermark).map_err(|error| {
                    stateful_apply_outcome_unknown(op_name, "state update", error)
                })?;
            }
        }
        let output = self.emit_agg_output().await;
        output.map_err(|error| {
            stateful_apply_outcome_unknown(&self.op_name, "output construction", error)
        })
    }

    #[cfg(feature = "cluster")]
    async fn execute_aligned_replay(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        let Some(config) = self.cluster_shuffle.as_ref() else {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' has aligned shuffle replay without an active cluster scope",
                self.op_name
            )));
        };
        let active_version = config.registry.versioned_snapshot().version();
        if config.sender.assignment_version() != active_version
            || config.receiver.assignment_version() != active_version
            || self
                .aligned_replay
                .iter()
                .any(|(version, _, _)| *version != active_version)
        {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' aligned shuffle replay crossed its assignment boundary",
                self.op_name
            )));
        }

        // Apply one transport-bounded logical batch per execution cycle. Keep the authoritative
        // queue entry until output emission succeeds. Any failure after aggregate mutation forces
        // coordinated recovery, which restores both state and replay from the last committed cut
        // instead of retrying a partially applied batch in memory.
        let (_, replay_watermark, batch) = self
            .aligned_replay
            .front()
            .cloned()
            .ok_or_else(|| DbError::Checkpoint("aligned replay queue became empty".into()))?;
        let QueryState::Agg(ref mut aggregate) = self.state else {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' aligned shuffle replay targeted non-aggregate state",
                self.op_name
            )));
        };
        aggregate
            .process_batch_for_vnode(batch.batch(), replay_watermark, batch.uniform_vnode())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' aligned shuffle replay failed and requires recovery: {error}",
                    self.op_name
                ))
            })?;
        let output = self.emit_agg_output().await.map_err(|error| {
            DbError::Checkpoint(format!(
                "aggregate '{}' aligned shuffle replay emission failed and requires recovery: {error}",
                self.op_name
            ))
        })?;
        let Some(_completed) = self.aligned_replay.pop_front() else {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' aligned shuffle replay disappeared after emission and requires recovery",
                self.op_name
            )));
        };
        Ok(output)
    }

    async fn emit_agg_output(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        let QueryState::Agg(ref mut agg_state) = self.state else {
            return Err(DbError::Pipeline(
                "internal: emit_agg_output on non-agg".into(),
            ));
        };

        #[cfg(feature = "cluster")]
        let num_group_cols = agg_state.num_group_cols();

        let mut batches = agg_state.emit()?;

        let having_filter = agg_state.having_filter().cloned();
        let having_sql = agg_state.having_sql().map(String::from);
        if let Some(ref filter) = having_filter {
            batches = apply_compiled_having(&batches, filter)?;
        } else if let Some(ref having_sql) = having_sql {
            batches = self.apply_having_sql(&batches, having_sql).await?;
        }

        #[cfg(feature = "cluster")]
        return self.suppress_restoring_output(batches, num_group_cols);
        #[cfg(not(feature = "cluster"))]
        Ok(batches)
    }

    // Drops rows for vnodes still restoring so downstream never sees a partial aggregate.
    #[cfg(feature = "cluster")]
    fn suppress_restoring_output(
        &self,
        batches: Vec<RecordBatch>,
        num_group_cols: usize,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let Some(ref cfg) = self.cluster_shuffle else {
            return Ok(batches);
        };
        if !cfg.registry.any_restoring() {
            return Ok(batches);
        }
        let vnode_count = cfg.registry.vnode_count();
        let mut out = Vec::with_capacity(batches.len());
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let vnodes: Vec<u32> = if num_group_cols == 0 {
                vec![0; batch.num_rows()]
            } else {
                let cols: Vec<usize> = (0..num_group_cols).collect();
                laminar_core::shuffle::row_vnodes(&batch, &cols, vnode_count).map_err(|error| {
                    crate::operator::shuffle_routing_error(
                        &format!("aggregate [{}] restore filter", self.op_name),
                        &error,
                    )
                })?
            };
            let keep: Vec<bool> = vnodes
                .iter()
                .map(|&v| !cfg.registry.is_restoring(v))
                .collect();
            let kept = keep.iter().filter(|&&k| k).count();
            if kept == batch.num_rows() {
                out.push(batch);
            } else if kept > 0 {
                let mask = arrow::array::BooleanArray::from(keep);
                let filtered = arrow::compute::filter_record_batch(&batch, &mask).map_err(|e| {
                    DbError::Pipeline(format!("restoring-vnode emission filter: {e}"))
                })?;
                out.push(filtered);
            }
        }
        Ok(out)
    }

    async fn apply_having_sql(
        &mut self,
        batches: &[RecordBatch],
        having_sql: &str,
    ) -> Result<Vec<RecordBatch>, DbError> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        if self.having_cache.is_none() {
            tracing::warn!(
                query = %self.op_name,
                "HAVING filter compiled to PhysicalExpr failed -- using cached SQL plan"
            );
            let table_name = format!("__having_{}", self.op_name);
            self.having_cache = Some(
                super::HavingSqlCache::build(
                    &self.ctx,
                    &table_name,
                    batches[0].schema(),
                    having_sql,
                )
                .await?,
            );
        }
        self.having_cache
            .as_ref()
            .expect("just initialized")
            .apply(&self.op_name, batches.to_vec())
            .await
    }
}

// Routes pre-aggregate rows by group-key vnode; drains inbound remote rows.
// `num_group_cols == 0` (global aggregate) hashes everything to vnode 0.
#[cfg(feature = "cluster")]
async fn shuffle_pre_agg_batches(
    config: Option<&ClusterShuffleConfig>,
    op_name: &str,
    num_group_cols: usize,
    batches: Vec<RecordBatch>,
) -> Result<
    (
        Vec<(RecordBatch, Option<u32>)>,
        Vec<laminar_core::shuffle::ReceivedBatch>,
    ),
    DbError,
> {
    let Some(cfg) = config else {
        return Ok((
            batches.into_iter().map(|batch| (batch, None)).collect(),
            Vec::new(),
        ));
    };

    let vnode_count = cfg.registry.vnode_count();
    let assignment = cfg.registry.versioned_snapshot();
    let mut local: Vec<(RecordBatch, Option<u32>)> = Vec::new();

    // Build the complete plan before any transport admission. Unassigned ownership can then defer
    // safely, while permanent structural failures halt rather than loop through recovery.
    let mut outbound: Vec<(u64, ShuffleMessage)> = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let context = format!("aggregate [{op_name}] routing");
        let row_vn = hash_rows_to_vnodes(&batch, num_group_cols, vnode_count)
            .map_err(|error| crate::operator::shuffle_routing_error(&context, &error))?;
        let plan = laminar_core::shuffle::route_checkpointed_batch(
            &batch,
            &row_vn,
            &assignment,
            cfg.self_id,
        )
        .map_err(|error| crate::operator::shuffle_routing_error(&context, &error))?;

        for route in plan.local {
            local.push((route.batch, Some(route.vnode)));
        }
        for route in plan.remote {
            outbound.push((
                route.owner.0,
                ShuffleMessage::checkpointed_routed(
                    op_name.to_string(),
                    route.routed_vnodes,
                    route.batch,
                ),
            ));
        }
    }

    // Once any frame is admitted, a later failure requires coordinated recovery because the frame
    // may still reach its peer. A failure before admission may defer only when it is transient.
    crate::operator::send_shuffle_plan(
        &cfg.sender,
        assignment.version(),
        outbound,
        &format!("aggregate [{op_name}] shuffle"),
    )
    .await?;

    let admitted = cfg.receiver.drain_checkpointed_data_for(op_name);
    for received in &admitted {
        if received.batch().num_rows() > 0 {
            local.push((
                received.batch().clone(),
                crate::operator::uniform_vnode_hint(received.routed_vnodes()),
            ));
        }
    }

    Ok((local, admitted))
}

#[cfg(feature = "cluster")]
fn hash_rows_to_vnodes(
    batch: &RecordBatch,
    num_group_cols: usize,
    vnode_count: u32,
) -> Result<Vec<u32>, laminar_core::shuffle::ShuffleRoutingError> {
    if num_group_cols == 0 || batch.num_rows() == 0 {
        return Ok(vec![0; batch.num_rows()]);
    }
    let columns: Vec<usize> = (0..num_group_cols).collect();
    laminar_core::shuffle::row_vnodes(batch, &columns, vnode_count)
}

#[cfg(feature = "cluster")]
impl SqlQueryOperator {
    #[cfg(test)]
    fn stage_uninit_vnode_slice(
        &mut self,
        vnode: u32,
        checkpoint: &AggStateCheckpoint,
        bytes: &[u8],
    ) -> Result<(), DbError> {
        validate_agg_checkpoint_slice(checkpoint).map_err(|error| {
            DbError::Pipeline(format!(
                "per-vnode state validation for '{}' vnode {vnode}: {error}",
                self.op_name
            ))
        })?;
        let expected = self
            .pending_restore
            .as_ref()
            .map(|pending| pending.fingerprint)
            .or(self.pending_restore_slice_fingerprint);
        if expected.is_some_and(|fingerprint| fingerprint != checkpoint.fingerprint) {
            return Err(DbError::Pipeline(format!(
                "per-vnode state fingerprint mismatch for '{}' vnode {vnode}: pending={}, incoming={}",
                self.op_name,
                expected.expect("checked Some"),
                checkpoint.fingerprint
            )));
        }
        self.pending_restore_slice_fingerprint = Some(checkpoint.fingerprint);
        self.pending_restore_slices
            .push(Bytes::copy_from_slice(bytes));
        Ok(())
    }

    #[cfg(test)]
    fn apply_vnode_state(&mut self, vnode: u32, bytes: &[u8]) -> Result<(), DbError> {
        let cp: AggStateCheckpoint = rkyv::from_bytes::<AggStateCheckpoint, rkyv::rancor::Error>(
            bytes,
        )
        .map_err(|error| {
            DbError::Pipeline(format!(
                "per-vnode state deserialization for '{}' vnode {vnode}: {error}",
                self.op_name
            ))
        })?;
        match self.state {
            QueryState::Agg(ref mut agg_state) => {
                let merged = agg_state.merge_groups(&cp)?;
                tracing::debug!(
                    query = %self.op_name, vnode, groups = merged,
                    "applied rehydrated vnode aggregate state"
                );
            }
            QueryState::Uninit => {
                self.stage_uninit_vnode_slice(vnode, &cp, bytes)?;
            }
            _ => {
                return Err(DbError::Pipeline(format!(
                    "per-vnode aggregate state for '{}' vnode {vnode} targeted a non-aggregate query",
                    self.op_name
                )));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl GraphOperator for SqlQueryOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        debug_assert_eq!(
            self.capability.implementation,
            OperatorImplementation::SqlQuery
        );
        self.capability
    }

    fn managed_state_accounting(&self) -> Option<ManagedStateAccountingSnapshot> {
        let QueryState::Agg(aggregate) = &self.state else {
            return None;
        };

        // This is aggregate working-state ownership only. The small `prev_owned`/
        // `next_prev_owned` authority sets are graph lifecycle metadata, not state-backend bytes.
        #[cfg(feature = "cluster")]
        let (prepared_bytes, retired_bytes) = {
            let staged = self
                .prepared_vnode_transition
                .as_ref()
                .map_or(0, |prepared| prepared.aggregate.accounted_state_bytes());
            match self.vnode_transition_cleanup.as_ref() {
                Some(SqlVnodeTransitionCleanup::Aborted(prepared)) => (
                    staged.saturating_add(prepared.aggregate.accounted_state_bytes()),
                    0,
                ),
                Some(SqlVnodeTransitionCleanup::Published { aggregate, .. }) => {
                    (staged, aggregate.accounted_state_bytes())
                }
                None => (staged, 0),
            }
        };
        #[cfg(not(feature = "cluster"))]
        let (prepared_bytes, retired_bytes) = (0, 0);

        Some(ManagedStateAccountingSnapshot {
            live: aggregate.accounted_state_bytes(),
            prepared: prepared_bytes,
            retired: retired_bytes,
        })
    }

    fn set_retractable_extremum_checkpoint_budget(&mut self, bytes: usize) {
        assert!(
            bytes > 0,
            "retractable-extremum checkpoint budget must be nonzero"
        );
        self.max_retractable_extremum_checkpoint_bytes = bytes;
        if let QueryState::Agg(aggregate) = &mut self.state {
            aggregate.set_max_retractable_extremum_checkpoint_bytes(bytes);
        }
    }

    async fn initialize_managed_state(&mut self) -> Result<(), DbError> {
        if matches!(self.state, QueryState::Uninit) {
            self.lazy_init().await?;
        }
        if matches!(self.state, QueryState::Agg(_)) {
            return Ok(());
        }
        // The immutable AST classifier deliberately over-approximates direct aggregates. Local
        // execution may legitimately resolve a derived aggregate to DataFusion instead of the
        // incremental state table. Cluster execution rejects that fallback inside `lazy_init`.
        self.capability.managed_state = None;
        Ok(())
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        if matches!(self.state, QueryState::Uninit) {
            self.lazy_init().await?;
        }
        #[cfg(feature = "cluster")]
        if !self.aligned_replay.is_empty() && !matches!(self.state, QueryState::Agg(_)) {
            return Err(DbError::Checkpoint(format!(
                "non-aggregate SQL operator '{}' restored checkpointed shuffle replay",
                self.op_name
            )));
        }

        let watermark = watermarks.first().copied().unwrap_or(i64::MIN);

        let input_batches = inputs.first().map_or(&[] as &[RecordBatch], Vec::as_slice);

        if input_batches.is_empty() || input_batches.iter().all(|b| b.num_rows() == 0) {
            if matches!(self.state, QueryState::Agg(_)) {
                return self.execute_agg(input_batches, watermark).await;
            }
            return Ok(Vec::new());
        }

        match &self.state {
            QueryState::Uninit => unreachable!("lazy_init already called"),
            QueryState::Agg(_) => self.execute_agg(input_batches, watermark).await,
            QueryState::Compiled(_) => {
                let QueryState::Compiled(ref proj) = self.state else {
                    unreachable!();
                };
                match try_evaluate_compiled(proj, input_batches) {
                    Ok(result) => Ok(result),
                    Err(e) => {
                        tracing::debug!(
                            query = %self.op_name,
                            error = %e,
                            "Compiled projection failed, falling back to cached plan"
                        );
                        self.build_and_cache_physical_plan().await?;
                        self.execute_cached_plan().await
                    }
                }
            }
            QueryState::CachedPlan(_) => match self.execute_cached_plan().await {
                Ok(batches) => Ok(batches),
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("Schema error") || err_str.contains("schema mismatch") {
                        tracing::debug!(
                            query = %self.op_name,
                            error = %e,
                            "Cached physical plan invalidated, re-planning"
                        );
                        self.build_and_cache_physical_plan().await?;
                        self.execute_cached_plan().await
                    } else {
                        Err(e)
                    }
                }
            },
            QueryState::CachedPhysical(ref physical) => {
                super::execute_cached_physical(self.task_ctx.clone(), &self.op_name, physical).await
            }
        }
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        // When the delta chain is authoritative, aggregate groups are NOT captured into the
        // whole-node manifest blob — they live in (and recover from) per-vnode partials.
        #[cfg(feature = "cluster")]
        let skip_whole_node_agg = self.skip_whole_node_agg();
        #[cfg(not(feature = "cluster"))]
        let skip_whole_node_agg = false;
        let agg: Option<AggStateCheckpoint> = if skip_whole_node_agg {
            None
        } else {
            match self.state {
                QueryState::Uninit => {
                    #[cfg(all(feature = "cluster", test))]
                    {
                        self.staged_pending_restore()?
                    }
                    #[cfg(any(not(feature = "cluster"), all(feature = "cluster", not(test))))]
                    {
                        self.staged_pending_restore()
                    }
                }
                QueryState::Agg(ref mut agg_state) => Some(agg_state.checkpoint_groups()?),
                QueryState::Compiled(_)
                | QueryState::CachedPlan(_)
                | QueryState::CachedPhysical(_) => None,
            }
        };
        #[cfg(feature = "cluster")]
        let aligned_replay = self
            .aligned_replay
            .iter()
            .map(|(assignment_version, watermark, batch)| {
                laminar_core::serialization::serialize_batch_stream(batch.batch())
                    .map(|blob| (*assignment_version, *watermark, blob))
                    .map_err(|error| {
                        DbError::Pipeline(format!(
                            "aligned aggregate replay checkpoint for '{}': {error}",
                            self.op_name
                        ))
                    })
            })
            .collect::<Result<Vec<_>, DbError>>()?;
        #[cfg(not(feature = "cluster"))]
        let aligned_replay = Vec::new();

        if agg.is_none() && aligned_replay.is_empty() {
            return Ok(None);
        }
        let cp = AggOpCheckpoint {
            agg,
            aligned_replay,
        };
        let data = rkyv::to_bytes::<rkyv::rancor::Error>(&cp)
            .map(|v| v.to_vec())
            .map_err(|e| {
                DbError::Pipeline(format!(
                    "checkpoint serialization for '{}': {e}",
                    self.op_name
                ))
            })?;
        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let cp: AggOpCheckpoint = rkyv::from_bytes::<AggOpCheckpoint, rkyv::rancor::Error>(
            &checkpoint.data,
        )
        .map_err(|e| {
            DbError::Checkpoint(format!(
                "checkpoint deserialization for '{}': {e}",
                self.op_name
            ))
        })?;

        let AggOpCheckpoint {
            agg,
            aligned_replay,
        } = cp;

        #[cfg(not(feature = "cluster"))]
        if !aligned_replay.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' checkpoint contains cluster shuffle replay without cluster support",
                self.op_name
            )));
        }

        #[cfg(feature = "cluster")]
        let decoded_aligned_replay = aligned_replay
            .into_iter()
            .map(|(assignment_version, watermark, blob)| {
                laminar_core::serialization::deserialize_batch_stream(&blob)
                    .map(|batch| {
                        (
                            assignment_version,
                            watermark,
                            crate::operator::RetainedBatch::local(batch),
                        )
                    })
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "aligned aggregate replay restore for '{}': {error}",
                            self.op_name
                        ))
                    })
            })
            .collect::<Result<VecDeque<_>, DbError>>()?;
        #[cfg(not(feature = "cluster"))]
        let _ = aligned_replay;

        #[cfg(feature = "cluster")]
        if !self.aligned_replay.is_empty() && !decoded_aligned_replay.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' aligned shuffle replay was applied more than once",
                self.op_name
            )));
        }

        match &mut self.state {
            QueryState::Agg(agg_state) => {
                if let Some(ref agg_checkpoint) = agg {
                    agg_state.restore_groups(agg_checkpoint).map_err(|error| {
                        DbError::Checkpoint(format!(
                            "aggregate '{}' checkpoint restore failed: {error}",
                            self.op_name
                        ))
                    })?;
                }
            }
            QueryState::Uninit => {
                if self.pending_restore.is_some() {
                    return Err(DbError::Checkpoint(format!(
                        "aggregate '{}' checkpoint restore was applied more than once",
                        self.op_name
                    )));
                }
                #[cfg(all(feature = "cluster", test))]
                if let (Some(checkpoint), Some(fingerprint)) =
                    (agg.as_ref(), self.pending_restore_slice_fingerprint)
                {
                    if checkpoint.fingerprint != fingerprint {
                        return Err(DbError::Checkpoint(format!(
                            "aggregate '{}' whole-node/vnode restore fingerprint mismatch: manifest={}, vnode={fingerprint}",
                            self.op_name, checkpoint.fingerprint
                        )));
                    }
                }
                self.pending_restore = agg;
            }
            QueryState::Compiled(_) | QueryState::CachedPlan(_) | QueryState::CachedPhysical(_) => {
                return Err(DbError::Checkpoint(format!(
                    "aggregate checkpoint cannot be restored into non-aggregate query '{}'",
                    self.op_name
                )));
            }
        }

        #[cfg(feature = "cluster")]
        self.aligned_replay.extend(decoded_aligned_replay);
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn watermark_hold(&self) -> Option<i64> {
        self.aligned_replay
            .iter()
            .map(|(_, watermark, _)| *watermark)
            .min()
    }

    #[cfg(feature = "cluster")]
    fn restored_output_watermark(&self) -> Option<i64> {
        self.aligned_replay
            .iter()
            .map(|(_, watermark, _)| *watermark)
            .min()
    }

    #[cfg(feature = "cluster")]
    fn wants_input(&self) -> bool {
        self.aligned_replay.is_empty()
    }

    #[cfg(feature = "cluster")]
    fn stage_checkpointed_shuffle(
        &mut self,
        stage: &str,
        batch: crate::operator::RetainedBatch,
        watermark: i64,
    ) -> Result<(), DbError> {
        if self.cluster_shuffle.is_none() || stage != self.op_name.as_ref() {
            return Err(DbError::Pipeline(format!(
                "SQL operator '{}' rejected checkpointed shuffle stage '{stage}' outside its active scope",
                self.op_name
            )));
        }
        if !matches!(self.state, QueryState::Uninit | QueryState::Agg(_)) {
            return Err(DbError::Pipeline(format!(
                "non-aggregate SQL operator '{}' cannot accept checkpointed shuffle data",
                self.op_name
            )));
        }
        // A peer's pre-barrier row is checkpointed channel state. Applying it here would let the
        // aggregate snapshot include the row while its downstream output remains absent forever.
        // The normal post-checkpoint cycle drains this queue before processing new source input.
        let assignment_version = batch.assignment_version().ok_or_else(|| {
            DbError::Pipeline(format!(
                "aggregate '{}' received checkpointed shuffle data without an assignment scope",
                self.op_name
            ))
        })?;
        self.aligned_replay
            .push_back((assignment_version, watermark, batch));
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_by_vnode(
        &mut self,
        required_vnodes: &[u32],
        vnode_count: u32,
    ) -> Result<
        Option<std::collections::HashMap<u32, crate::checkpoint_coordinator::StagedSlice>>,
        DbError,
    > {
        use crate::checkpoint_coordinator::StagedSlice;
        if required_vnodes.windows(2).any(|pair| pair[0] >= pair[1])
            || required_vnodes.iter().any(|vnode| *vnode >= vnode_count)
        {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' received a non-canonical required vnode roster {required_vnodes:?} for vnode_count {vnode_count}",
                self.op_name
            )));
        }
        if let QueryState::Agg(ref agg_state) = self.state {
            agg_state.validate_vnode_count(vnode_count)?;
        }
        // Re-base the delta chain of any vnode acquired since the last capture (its parent epoch is
        // gone), before deciding FULL-vs-DELTA below. Must run before the `agg_state` borrow.
        let newly_acquired = self.take_newly_acquired(required_vnodes);
        let QueryState::Agg(ref mut agg_state) = self.state else {
            return Ok(None);
        };
        agg_state.reset_acquired_vnodes(&newly_acquired);

        // Incremental delta capture: each touched vnode emits a FULL re-base or a DELTA.
        let mut out = if let Some(chain_bound) = self.delta_chain_bound {
            if agg_state.delta_enabled() {
                use crate::aggregate_state::VnodeCapture;
                let captures = agg_state.checkpoint_delta_by_vnode(vnode_count, chain_bound)?;
                let mut out = std::collections::HashMap::with_capacity(captures.len());
                for (vnode, cap) in captures {
                    let slice = match cap {
                        VnodeCapture::Full(cp) => StagedSlice::Bytes(bytes::Bytes::from(
                            serialize_agg_cp(&cp, &self.op_name)?,
                        )),
                        VnodeCapture::Delta(d) => StagedSlice::Delta(bytes::Bytes::from(
                            serialize_agg_cp(&d.changed, &self.op_name)?,
                        )),
                    };
                    out.insert(vnode, slice);
                }
                out
            } else {
                return Err(DbError::Checkpoint(format!(
                    "aggregate '{}' has a delta chain bound without delta tracking enabled",
                    self.op_name
                )));
            }
        } else {
            let per_vnode = agg_state.checkpoint_groups_by_vnode(vnode_count)?;
            let mut out = std::collections::HashMap::with_capacity(per_vnode.len());
            for (vnode, cp) in per_vnode {
                out.insert(
                    vnode,
                    StagedSlice::Bytes(bytes::Bytes::from(serialize_agg_cp(&cp, &self.op_name)?)),
                );
            }
            out
        };

        let required: rustc_hash::FxHashSet<u32> = required_vnodes.iter().copied().collect();
        let mut unexpected: Vec<u32> = out
            .keys()
            .copied()
            .filter(|vnode| !required.contains(vnode))
            .collect();
        unexpected.sort_unstable();
        if !unexpected.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' captured state for vnodes {unexpected:?} outside its required roster {required_vnodes:?}",
                self.op_name
            )));
        }

        if out.len() != required_vnodes.len() {
            let empty = StagedSlice::Bytes(bytes::Bytes::from(serialize_agg_cp(
                &agg_state.empty_checkpoint(),
                &self.op_name,
            )?));
            for vnode in required_vnodes {
                out.entry(*vnode).or_insert_with(|| empty.clone());
            }
        }

        if out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(out))
        }
    }

    #[cfg(feature = "cluster")]
    fn prepare_vnode_transition(
        &mut self,
        transition: ManagedVnodeTransition<'_>,
    ) -> Result<(), DbError> {
        if self.prepared_vnode_transition.is_some() || self.vnode_transition_cleanup.is_some() {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' already owns vnode transition state",
                self.op_name
            )));
        }
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "aggregate '{}' cannot prepare vnode state without cluster ownership",
                self.op_name
            ))
        })?;
        let QueryState::Agg(ref aggregate) = self.state else {
            return Err(DbError::Checkpoint(format!(
                "managed vnode transition for '{}' targeted a non-aggregate query",
                self.op_name
            )));
        };
        aggregate.validate_vnode_count(transition.target.vnode_count)?;

        let assignment = config.registry.versioned_snapshot();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        if transition.target.vnode_count != config.registry.vnode_count()
            || transition.target.assignment_version != assignment.version()
            || !transition.target.matches_owner_map(&owners)
        {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' vnode transition target does not match assignment {}",
                self.op_name,
                assignment.version()
            )));
        }

        // Validate the complete borrowed roster before allocating any owned inner checkpoint.
        let archive_profile = aggregate.vnode_archive_restore_profile();
        let mut preflighted = Vec::new();
        preflighted
            .try_reserve_exact(transition.restores.len())
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' could not reserve inner archive preflight metadata",
                    self.op_name
                ))
            })?;
        let mut restored_lower_bounds = Vec::new();
        restored_lower_bounds
            .try_reserve_exact(transition.restores.len())
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' could not reserve cardinality preflight metadata",
                    self.op_name
                ))
            })?;
        for restore in transition.restores {
            let base = archive_profile.preflight(
                restore.base,
                format_args!(
                    "per-vnode base for '{}' vnode {}",
                    self.op_name, restore.vnode
                ),
            )?;
            let mut vnode_lower_bound = base.group_count();

            let mut deltas = Vec::new();
            deltas
                .try_reserve_exact(restore.deltas.len())
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "aggregate '{}' could not reserve delta archive preflight metadata",
                        self.op_name
                    ))
                })?;
            for (link, changed) in restore.deltas.iter().enumerate() {
                let delta = archive_profile.preflight(
                    changed.as_slice(),
                    format_args!(
                        "per-vnode delta {link} for '{}' vnode {}",
                        self.op_name, restore.vnode
                    ),
                )?;
                vnode_lower_bound = vnode_lower_bound.max(delta.group_count());
                deltas.push(delta);
            }
            restored_lower_bounds.push((restore.vnode, vnode_lower_bound));
            preflighted.push((restore.vnode, base, deltas));
        }
        aggregate.preflight_vnode_transition_cardinality(
            transition.target.vnode_count,
            &restored_lower_bounds,
            transition.revoked,
        )?;
        drop(restored_lower_bounds);

        let mut next_prev_owned = rustc_hash::FxHashSet::default();
        next_prev_owned
            .try_reserve(self.prev_owned.len())
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' could not reserve ownership transition metadata",
                    self.op_name
                ))
            })?;
        next_prev_owned.extend(self.prev_owned.iter().copied());
        for vnode in transition.revoked {
            next_prev_owned.remove(vnode);
        }

        // Deserialization stays lazy: aggregate preparation consumes and stages one vnode before
        // asking this iterator for the next. The complete borrowed pass above has already
        // validated every archive and the transition-wide cardinality lower bound.
        let owned_restores = preflighted.into_iter().map(|(vnode, base, deltas)| {
            let mut owned_deltas = Vec::new();
            owned_deltas.try_reserve_exact(deltas.len()).map_err(|_| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' could not reserve owned delta metadata for vnode {vnode}",
                    self.op_name
                ))
            })?;
            let base = base.deserialize(format_args!(
                "per-vnode base for '{}' vnode {vnode}",
                self.op_name
            ))?;
            for (link, changed) in deltas.into_iter().enumerate() {
                let changed = changed.deserialize(format_args!(
                    "per-vnode delta {link} for '{}' vnode {vnode}",
                    self.op_name
                ))?;
                owned_deltas.push(crate::aggregate_state::AggVnodeDelta { changed });
            }
            Ok(OwnedAggVnodeRestore {
                vnode,
                base,
                deltas: owned_deltas,
            })
        });
        let aggregate = aggregate.prepare_owned_vnode_transition(
            transition.target.vnode_count,
            owned_restores,
            transition.revoked,
        )?;
        self.prepared_vnode_transition = Some(PreparedSqlVnodeTransition {
            aggregate,
            next_prev_owned,
        });
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn abort_vnode_transition(&mut self) {
        let Some(prepared) = self.prepared_vnode_transition.take() else {
            return;
        };
        assert!(
            self.vnode_transition_cleanup.is_none(),
            "aggregate vnode transition cleanup must finish before abort"
        );
        self.vnode_transition_cleanup = Some(SqlVnodeTransitionCleanup::Aborted(prepared));
    }

    #[cfg(feature = "cluster")]
    fn publish_vnode_transition(&mut self) {
        let prepared = self
            .prepared_vnode_transition
            .take()
            .expect("aggregate vnode transition must be prepared before publication");
        assert!(
            self.vnode_transition_cleanup.is_none(),
            "aggregate vnode transition cleanup must finish before publication"
        );
        let QueryState::Agg(ref mut aggregate) = self.state else {
            panic!("managed vnode transition publication targeted a non-aggregate query");
        };
        let retired_aggregate = aggregate.publish_prepared_vnode_transition(prepared.aggregate);
        let retired_prev_owned = std::mem::replace(&mut self.prev_owned, prepared.next_prev_owned);
        self.vnode_transition_cleanup = Some(SqlVnodeTransitionCleanup::Published {
            aggregate: retired_aggregate,
            prev_owned: retired_prev_owned,
        });
    }

    #[cfg(feature = "cluster")]
    fn finish_vnode_transition(&mut self) {
        match self.vnode_transition_cleanup.take() {
            Some(SqlVnodeTransitionCleanup::Aborted(prepared)) => drop(prepared),
            Some(SqlVnodeTransitionCleanup::Published {
                aggregate,
                prev_owned,
            }) => {
                IncrementalAggState::finish_vnode_transition(aggregate);
                drop(prev_owned);
            }
            None => {}
        }
    }

    #[cfg(all(feature = "cluster", test))]
    fn apply_vnode_chain(
        &mut self,
        vnode: u32,
        base: &[u8],
        deltas: &[&[u8]],
    ) -> Result<(), DbError> {
        // Deserialize the chain before touching `self.state` (avoids borrowing `self` twice).
        let base_cp: AggStateCheckpoint =
            rkyv::from_bytes::<AggStateCheckpoint, rkyv::rancor::Error>(base).map_err(|e| {
                DbError::Pipeline(format!(
                    "per-vnode base deserialization for '{}' vnode {vnode}: {e}",
                    self.op_name
                ))
            })?;
        let delta_objs: Vec<crate::aggregate_state::AggVnodeDelta> = deltas
            .iter()
            .map(|changed| {
                let cp: AggStateCheckpoint =
                    rkyv::from_bytes::<AggStateCheckpoint, rkyv::rancor::Error>(changed).map_err(
                        |e| {
                            DbError::Pipeline(format!(
                                "per-vnode delta deserialization for '{}' vnode {vnode}: {e}",
                                self.op_name
                            ))
                        },
                    )?;
                Ok(crate::aggregate_state::AggVnodeDelta { changed: cp })
            })
            .collect::<Result<_, DbError>>()?;
        let vnode_count = self
            .cluster_shuffle
            .as_ref()
            .map(|config| config.registry.vnode_count());

        match self.state {
            QueryState::Agg(ref mut agg_state) => {
                let vnode_count = vnode_count.ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "aggregate '{}' cannot replace vnode state without cluster ownership",
                        self.op_name
                    ))
                })?;
                let merged =
                    agg_state.replace_vnode_chain(vnode, vnode_count, &base_cp, &delta_objs)?;
                tracing::debug!(
                    query = %self.op_name, vnode, groups = merged, deltas = delta_objs.len(),
                    "replaced vnode state from its authoritative recovery chain"
                );
            }
            QueryState::Uninit => {
                // Keep each base in its original serialized form; lazy_init validates disjointness
                // and performs one columnar merge across the complete recovered set.
                self.stage_uninit_vnode_slice(vnode, &base_cp, base)?;
                self.pending_restore_deltas.extend(delta_objs);
            }
            _ => {
                return Err(DbError::Pipeline(format!(
                    "per-vnode aggregate chain for '{}' vnode {vnode} targeted a non-aggregate query",
                    self.op_name
                )));
            }
        }
        Ok(())
    }

    #[cfg(all(feature = "cluster", test))]
    fn drop_owned_vnodes(&mut self, revoked: &rustc_hash::FxHashSet<u32>) -> Result<(), DbError> {
        if revoked.is_empty() {
            return Ok(());
        }
        let vnode_count = self
            .cluster_shuffle
            .as_ref()
            .map(|c| c.registry.vnode_count());
        match self.state {
            QueryState::Agg(ref mut agg_state) => {
                let vc = vnode_count.ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "aggregate '{}' cannot revoke vnode state without cluster ownership",
                        self.op_name
                    ))
                })?;
                agg_state.drop_vnodes(revoked, vc)?;
            }
            // Uninit: the revoked vnode's groups are still in `pending_restore`; defer the drop until
            // `lazy_init` folds them in, else this node could expose state it no longer owns.
            _ => self.deferred_revoke_vnodes.extend(revoked.iter().copied()),
        }
        // A later re-acquire must register in `take_newly_acquired` and force a FULL re-base.
        for vnode in revoked {
            self.prev_owned.remove(vnode);
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn force_full_rebase(&mut self) {
        if let QueryState::Agg(ref mut agg_state) = self.state {
            agg_state.force_full_rebase();
        }
    }
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn sql_capability_classification_is_shape_aware_and_fail_closed() {
        use crate::operator::capability::{
            ClusterExecutionStatus, ManagedStateContract, OperatorStateClass,
        };

        let context = laminar_sql::create_session_context();
        let classify = |sql| classify_sql_capability(sql, &context);

        let stateless = classify("SELECT key, value * 2 FROM events");
        assert_eq!(stateless.state_class, OperatorStateClass::Stateless);
        assert_eq!(stateless.cluster_status, ClusterExecutionStatus::DdlGuarded);

        let scalar = classify("SELECT UPPER(key) FROM events");
        assert_eq!(scalar.state_class, OperatorStateClass::Stateless);
        assert_eq!(scalar.cluster_status, ClusterExecutionStatus::DdlGuarded);

        let global = classify("SELECT COUNT(*) AS n FROM events");
        assert_eq!(global.state_class, OperatorStateClass::GlobalSingleton);
        assert_eq!(global.cluster_status, ClusterExecutionStatus::DdlGuarded);
        assert_eq!(
            global.managed_state,
            Some(ManagedStateContract::SqlAggregateV1)
        );

        let keyed = classify("SELECT key, SUM(value) AS total FROM events GROUP BY key");
        assert_eq!(keyed.state_class, OperatorStateClass::VnodeKeyed);
        assert_eq!(keyed.cluster_status, ClusterExecutionStatus::DdlGuarded);
        assert_eq!(
            keyed.managed_state,
            Some(ManagedStateContract::SqlAggregateV1)
        );

        let window_keyed = classify(
            "SELECT TUMBLE(ts, INTERVAL '1' MINUTE), SUM(value) FROM events \
             GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)",
        );
        assert_eq!(window_keyed.state_class, OperatorStateClass::VnodeKeyed);
        let ClusterExecutionStatus::Rejected { reason } = window_keyed.cluster_status else {
            panic!("windowed aggregate must remain rejected")
        };
        assert!(reason.contains("timer"), "{reason}");
        assert!(reason.contains("watermark"), "{reason}");
        assert_eq!(window_keyed.managed_state, None);

        let analytic = classify("SELECT SUM(value) OVER (PARTITION BY key) AS running FROM events");
        assert_eq!(analytic.state_class, OperatorStateClass::LocalOnly);
        assert!(matches!(
            analytic.cluster_status,
            ClusterExecutionStatus::Rejected { .. }
        ));

        for ambiguous_sql in [
            "SELECT mystery(value) FROM events",
            "SELECT DISTINCT key FROM events",
            "SELECT key FROM events GROUP BY key",
            "SELECT key FROM (SELECT key FROM events) nested",
            "SELECT a.key FROM events a JOIN other b ON a.key = b.key",
            "SELECT COUNT(*) FROM (SELECT * FROM events) nested",
            "WITH nested AS (SELECT * FROM events) SELECT COUNT(*) FROM nested",
            "SELECT COUNT(*) FROM events a JOIN other b ON a.key = b.key",
            "SELECT DISTINCT COUNT(*) FROM events",
            "SELECT key FROM events ORDER BY key",
            "SELECT COUNT(*) AS n FROM events LIMIT 1",
            "SELECT key FROM events; SELECT key FROM events",
        ] {
            let ambiguous = classify(ambiguous_sql);
            assert_eq!(
                ambiguous.state_class,
                OperatorStateClass::LocalOnly,
                "{ambiguous_sql}"
            );
            assert!(
                matches!(
                    ambiguous.cluster_status,
                    ClusterExecutionStatus::Rejected { .. }
                ),
                "{ambiguous_sql}"
            );
        }

        let malformed = classify("not sql");
        assert_eq!(malformed.state_class, OperatorStateClass::LocalOnly);
        assert!(matches!(
            malformed.cluster_status,
            ClusterExecutionStatus::Rejected { .. }
        ));
    }

    #[tokio::test]
    async fn managed_aggregate_initializes_before_receiving_input() {
        let (context, batch) = context_and_batch();
        let key_group_count = KeyGroupCount::try_from(8_u16).unwrap();
        let mut operator = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
            key_group_count,
        );

        operator.initialize_managed_state().await.unwrap();

        let QueryState::Agg(ref aggregate) = operator.state else {
            panic!("expected initialized aggregate state");
        };
        assert_eq!(aggregate.key_group_count(), key_group_count);
        let empty_accounting = operator
            .managed_state_accounting()
            .expect("initialized aggregate must report managed state");
        assert!(empty_accounting.live > 0);
        assert_eq!(empty_accounting.prepared, 0);
        assert_eq!(empty_accounting.retired, 0);

        operator.process(&[vec![batch]], &[i64::MIN]).await.unwrap();
        let populated_accounting = operator.managed_state_accounting().unwrap();
        assert!(populated_accounting.live > empty_accounting.live);
        assert_eq!(populated_accounting.prepared, 0);
        assert_eq!(populated_accounting.retired, 0);
        assert!(operator.checkpoint().unwrap().is_some());
    }

    #[tokio::test]
    async fn changelog_aggregate_having_is_rejected_at_state_startup() {
        let (context, _) = context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "qualified-sums",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key HAVING SUM(value) > 0",
            context,
            None,
            true,
        );

        let error = operator
            .initialize_managed_state()
            .await
            .expect_err("changelog HAVING must fail before state becomes executable");
        assert!(
            error.to_string().contains("transition-aware HAVING"),
            "{error}"
        );
        assert!(matches!(operator.state, QueryState::Uninit));
    }

    pub(super) fn context_and_batch() -> (SessionContext, RecordBatch) {
        let context = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let seed = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["seed"])),
                Arc::new(Int64Array::from(vec![0])),
            ],
        )
        .unwrap();
        let table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![seed]])
                .unwrap();
        context.register_table("events", Arc::new(table)).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();
        (context, batch)
    }

    #[tokio::test]
    async fn aggregate_restore_mismatch_faults_during_lazy_init() {
        let (context, batch) = context_and_batch();
        let mut donor = SqlQueryOperator::new(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context.clone(),
            None,
            false,
        );
        donor.process(&[vec![batch]], &[i64::MIN]).await.unwrap();
        let checkpoint = donor.checkpoint().unwrap().unwrap();

        let mut restored = SqlQueryOperator::new(
            "count",
            "SELECT key, COUNT(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
        );
        restored.restore(checkpoint).unwrap();
        let error = restored.lazy_init().await.unwrap_err();
        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.to_string().contains("fingerprint mismatch"));
        assert!(matches!(restored.state, QueryState::Uninit));
        assert!(restored.pending_restore.is_some());
    }

    #[tokio::test]
    async fn aggregate_checkpoint_cannot_target_initialized_non_aggregate() {
        let (context, batch) = context_and_batch();
        let mut donor = SqlQueryOperator::new(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context.clone(),
            None,
            false,
        );
        donor
            .process(&[vec![batch.clone()]], &[i64::MIN])
            .await
            .unwrap();
        let checkpoint = donor.checkpoint().unwrap().unwrap();

        let mut projection = SqlQueryOperator::new(
            "projection",
            "SELECT key, value FROM events",
            context,
            None,
            false,
        );
        projection
            .process(&[vec![batch]], &[i64::MIN])
            .await
            .unwrap();
        let error = projection.restore(checkpoint).unwrap_err();
        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.to_string().contains("non-aggregate"));
    }

    #[tokio::test]
    async fn local_derived_aggregate_retains_datafusion_fallback() {
        let (context, _) = context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "ratio",
            "SELECT SUM(value) / COUNT(value) AS ratio FROM events",
            context,
            None,
            false,
        );

        operator.initialize_managed_state().await.unwrap();
        assert!(matches!(operator.state, QueryState::CachedPlan(_)));
        assert_eq!(operator.capability.managed_state, None);
    }

    #[test]
    fn stateful_apply_classification_preserves_stronger_dispositions() {
        let ordinary = stateful_apply_outcome_unknown(
            "totals",
            "state update",
            DbError::Pipeline("injected update failure".into()),
        );
        assert!(matches!(ordinary, DbError::StatefulOperatorPartialApply(_)));
        assert!(ordinary.requires_pipeline_recovery());

        let recovery = stateful_apply_outcome_unknown(
            "totals",
            "state update",
            DbError::Checkpoint("injected recovery".into()),
        );
        assert!(matches!(recovery, DbError::Checkpoint(_)));

        let halt = stateful_apply_outcome_unknown(
            "totals",
            "state update",
            DbError::BackpressureFail("injected halt".into()),
        );
        assert!(matches!(halt, DbError::BackpressureFail(_)));
    }

    #[tokio::test]
    async fn later_aggregate_batch_failure_requires_recovery_after_prior_mutation() {
        let (context, seed) = context_and_batch();
        let schema = seed.schema();
        let first = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let later = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["b", "c"])),
                Arc::new(Int64Array::from(vec![2, 3])),
            ],
        )
        .unwrap();
        let mut operator = SqlQueryOperator::new(
            "totals",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
        );
        operator.lazy_init().await.unwrap();
        let QueryState::Agg(ref mut aggregate) = operator.state else {
            panic!("expected incremental aggregate state");
        };
        aggregate.set_max_groups_for_test(2);

        let error = operator
            .process(&[vec![first, later]], &[i64::MIN])
            .await
            .expect_err("the later batch must exceed the aggregate group limit");

        assert!(matches!(
            &error,
            DbError::StatefulOperatorPartialApply(message)
                if message.contains("state update") && message.contains("outcome is unknown")
        ));
        assert!(error.requires_pipeline_recovery());
    }

    #[test]
    fn corrupt_aggregate_checkpoint_is_a_recovery_fault() {
        let (context, _) = context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
        );
        let error = operator
            .restore(OperatorCheckpoint {
                data: b"not-rkyv".to_vec(),
            })
            .unwrap_err();
        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.requires_pipeline_recovery());
    }

    #[cfg(not(feature = "cluster"))]
    #[test]
    fn cluster_shuffle_checkpoint_is_rejected_without_support() {
        let (context, _) = context_and_batch();
        let checkpoint = AggOpCheckpoint {
            agg: None,
            aligned_replay: vec![(3, i64::MIN, Vec::new())],
        };
        let data = rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
            .unwrap()
            .to_vec();
        let mut operator = SqlQueryOperator::new(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
        );
        let error = operator.restore(OperatorCheckpoint { data }).unwrap_err();
        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.to_string().contains("cluster support"));
    }
}

#[cfg(all(test, feature = "cluster"))]
mod delta_primary_tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_core::cluster::control::LeaseDeadline;
    use laminar_core::state::{NodeId, VnodeRegistry};

    async fn single_owner_shuffle_for(vnode_count: u32) -> (ClusterShuffleConfig, u64) {
        let self_id = NodeId(1);
        let incarnation = uuid::Uuid::from_u128(1);
        let registry = Arc::new(VnodeRegistry::single_owner(vnode_count, self_id));
        let receiver = Arc::new(
            laminar_core::shuffle::ShuffleReceiver::bind(
                self_id.0,
                "127.0.0.1:0".parse().unwrap(),
                incarnation,
            )
            .await
            .unwrap(),
        );
        let sender = Arc::new(laminar_core::shuffle::ShuffleSender::new(
            self_id.0,
            incarnation,
        ));
        let process_deadline =
            Arc::new(LeaseDeadline::live_for(std::time::Duration::from_secs(60)));
        receiver
            .install_process_lease_deadline(Arc::clone(&process_deadline))
            .unwrap();
        sender
            .install_process_lease_deadline(process_deadline)
            .unwrap();
        let version = registry.assignment_version();
        let owners = vec![self_id.0; usize::try_from(vnode_count).unwrap()];
        let fence = laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
            version,
            &owners,
            vec![laminar_core::checkpoint::CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: incarnation,
            }],
        )
        .unwrap();
        sender.install_assignment_fence(&fence, &owners).unwrap();
        receiver.install_assignment_fence(&fence, &owners).unwrap();
        (
            ClusterShuffleConfig {
                registry,
                sender,
                receiver,
                self_id,
            },
            version,
        )
    }

    async fn single_owner_shuffle() -> (ClusterShuffleConfig, u64) {
        single_owner_shuffle_for(8).await
    }

    #[tokio::test]
    async fn grouped_managed_aggregate_is_cluster_startup_admissible() {
        use crate::operator::capability::{
            ClusterExecutionStatus, ManagedStateContract, OperatorStateClass,
        };

        let (context, _) = super::checkpoint_tests::context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "totals",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
        );
        assert_eq!(
            operator.cluster_capability(),
            OperatorCapability {
                implementation: OperatorImplementation::SqlQuery,
                state_class: OperatorStateClass::VnodeKeyed,
                cluster_status: ClusterExecutionStatus::DdlGuarded,
                managed_state: Some(ManagedStateContract::SqlAggregateV1),
            }
        );

        let (shuffle, _) = single_owner_shuffle().await;
        operator.attach_cluster_shuffle(shuffle);
        operator.initialize_managed_state().await.unwrap();
        assert!(matches!(operator.state, QueryState::Agg(_)));
    }

    #[tokio::test]
    async fn pre_agg_shuffle_preserves_local_hints_and_marks_configless_batches_mixed() {
        let (_, batch) = super::checkpoint_tests::context_and_batch();
        let (configless, admitted) =
            shuffle_pre_agg_batches(None, "totals", 1, vec![batch.clone()])
                .await
                .unwrap();
        assert!(admitted.is_empty());
        assert_eq!(configless.len(), 1);
        assert_eq!(configless[0].1, None);

        let (shuffle, _) = single_owner_shuffle().await;
        let (routed, admitted) = shuffle_pre_agg_batches(Some(&shuffle), "totals", 1, vec![batch])
            .await
            .unwrap();
        assert!(admitted.is_empty());
        assert_eq!(
            routed
                .iter()
                .map(|(batch, _)| batch.num_rows())
                .sum::<usize>(),
            2
        );
        for (batch, hint) in routed {
            let vnode = hint.expect("a local route must retain its exact vnode");
            assert!(
                hash_rows_to_vnodes(&batch, 1, 8)
                    .unwrap()
                    .into_iter()
                    .all(|derived| derived == vnode),
                "a local route hint must describe every row in its batch",
            );
        }
    }

    #[tokio::test]
    async fn cluster_aggregate_never_falls_back_to_node_local_execution() {
        let (context, batch) = super::checkpoint_tests::context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "ratio",
            "SELECT SUM(value) / COUNT(value) AS ratio FROM events",
            context,
            None,
            false,
        );
        let (shuffle, _) = single_owner_shuffle().await;
        operator.attach_cluster_shuffle(shuffle);

        let error = operator
            .process(&[vec![batch]], &[i64::MIN])
            .await
            .expect_err("a derived aggregate must not execute against node-local input");
        assert!(error.to_string().contains("LDB-4007"), "{error}");
        assert!(
            error
                .to_string()
                .contains("exact incremental execution path"),
            "{error}"
        );
        assert!(matches!(operator.state, QueryState::Uninit));
    }

    #[tokio::test]
    async fn cluster_aggregate_cannot_outgrow_its_immutable_managed_capability() {
        let (context, batch) = super::checkpoint_tests::context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "limited-count",
            "SELECT COUNT(*) AS n FROM events LIMIT 1",
            context,
            None,
            false,
        );
        assert_eq!(operator.capability.managed_state, None);
        let (shuffle, _) = single_owner_shuffle().await;
        operator.attach_cluster_shuffle(shuffle);

        let error = operator
            .process(&[vec![batch]], &[i64::MIN])
            .await
            .expect_err("an unmanaged classifier result must not become cluster aggregate state");

        assert!(error.to_string().contains("LDB-4007"), "{error}");
        assert!(
            error.to_string().contains("immutable cluster capability"),
            "{error}"
        );
        assert!(matches!(operator.state, QueryState::Uninit));
    }

    #[tokio::test]
    async fn empty_aggregate_capture_names_every_required_vnode() {
        for (sql, required) in [
            (
                "SELECT key, SUM(value) AS total FROM events GROUP BY key",
                (0..8).collect::<Vec<_>>(),
            ),
            ("SELECT COUNT(*) AS total FROM events", vec![0]),
        ] {
            let (context, _) = super::checkpoint_tests::context_and_batch();
            let mut operator = SqlQueryOperator::new("totals", sql, context, None, false);
            let (shuffle, _) = single_owner_shuffle().await;
            operator.attach_cluster_shuffle(shuffle);
            operator.initialize_managed_state().await.unwrap();
            let QueryState::Agg(ref aggregate) = operator.state else {
                panic!("expected initialized aggregate state");
            };
            assert_eq!(
                aggregate.key_group_count(),
                KeyGroupCount::try_from(8_u32).unwrap()
            );

            let captured = operator
                .checkpoint_by_vnode(&required, 8)
                .unwrap()
                .expect("a required semantic EMPTY must be explicit");

            let mut actual: Vec<u32> = captured.keys().copied().collect();
            actual.sort_unstable();
            assert_eq!(actual, required);
            for slice in captured.values() {
                let crate::checkpoint_coordinator::StagedSlice::Bytes(bytes) = slice else {
                    panic!("fresh empty aggregate must establish a FULL base")
                };
                let checkpoint =
                    rkyv::from_bytes::<AggStateCheckpoint, rkyv::rancor::Error>(bytes).unwrap();
                assert!(checkpoint.keys_ipc.is_empty());
                assert!(checkpoint.acc_state_ipc.is_empty());
                assert!(checkpoint.last_updated_ms.is_empty());
                assert!(checkpoint.last_emitted.is_empty());
            }
        }
    }

    #[tokio::test]
    async fn checkpoint_count_mismatch_preserves_owned_baseline() {
        let (context, _) = super::checkpoint_tests::context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "total",
            "SELECT COUNT(*) AS total FROM events",
            context,
            None,
            false,
        );
        let (shuffle, _) = single_owner_shuffle().await;
        operator.attach_cluster_shuffle(shuffle);
        operator.initialize_managed_state().await.unwrap();

        let error = operator
            .checkpoint_by_vnode(&[0], 16)
            .expect_err("capture must reject a count outside its immutable topology");
        assert!(error.to_string().contains("state=8, requested=16"));
        assert!(operator.prev_owned.is_empty());

        assert!(operator.checkpoint_by_vnode(&[0], 8).unwrap().is_some());
        assert_eq!(
            operator.prev_owned,
            [0].into_iter().collect::<rustc_hash::FxHashSet<_>>()
        );
    }

    #[tokio::test]
    async fn global_semantic_empty_replaces_groups_and_emission_history() {
        let (context, batch) = super::checkpoint_tests::context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "total",
            "SELECT SUM(value) AS total FROM events",
            context,
            None,
            true,
        );
        let (shuffle, _) = single_owner_shuffle().await;
        operator.attach_cluster_shuffle(shuffle);

        let initial_rows: usize = operator
            .process(&[vec![batch.clone()]], &[i64::MIN])
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(initial_rows, 1);

        let empty = match &operator.state {
            QueryState::Agg(aggregate) => aggregate.empty_checkpoint(),
            _ => panic!("expected initialized aggregate state"),
        };
        let empty = serialize_agg_cp(&empty, &operator.op_name).unwrap();
        operator.apply_vnode_chain(0, &empty, &[]).unwrap();

        let emitted = operator.process(&[vec![batch]], &[i64::MIN]).await.unwrap();
        assert_eq!(emitted.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let weights = emitted[0]
            .column_by_name(laminar_core::changelog::WEIGHT_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(weights.values(), &[1]);
    }

    #[tokio::test]
    async fn prepared_global_restore_aborts_cleanly_then_publishes_exact_image() {
        fn current_state_bytes(operator: &mut SqlQueryOperator) -> Vec<u8> {
            let QueryState::Agg(ref mut aggregate) = operator.state else {
                panic!("expected initialized aggregate state");
            };
            serialize_agg_cp(&aggregate.checkpoint_groups().unwrap(), &operator.op_name).unwrap()
        }

        let (context, batch) = super::checkpoint_tests::context_and_batch();
        let sql = "SELECT SUM(value) AS total FROM events";
        let (donor_shuffle, version) = single_owner_shuffle().await;
        let mut donor = SqlQueryOperator::new("total", sql, context.clone(), None, false);
        donor.attach_cluster_shuffle(donor_shuffle);
        donor
            .process(&[vec![batch.clone()]], &[i64::MIN])
            .await
            .unwrap();
        let donor_slice = donor
            .checkpoint_by_vnode(&[0], 8)
            .unwrap()
            .expect("global vnode must emit an explicit image")
            .remove(&0)
            .expect("global vnode zero image");
        let crate::checkpoint_coordinator::StagedSlice::Bytes(donor_slice) = donor_slice else {
            panic!("global aggregate capture must be materialized bytes");
        };

        let (subject_shuffle, subject_version) = single_owner_shuffle().await;
        assert_eq!(subject_version, version);
        let mut subject = SqlQueryOperator::new("total", sql, context, None, false);
        subject.attach_cluster_shuffle(subject_shuffle);
        subject
            .process(&[vec![batch.clone()]], &[i64::MIN])
            .await
            .unwrap();
        subject.process(&[vec![batch]], &[i64::MIN]).await.unwrap();
        let before = current_state_bytes(&mut subject);
        let target = laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
            version,
            &[1; 8],
            vec![laminar_core::checkpoint::CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(1),
            }],
        )
        .unwrap();
        let restores = [crate::operator_graph::ManagedVnodeRestore {
            vnode: 0,
            base: &donor_slice,
            deltas: &[],
        }];
        let revoked = rustc_hash::FxHashSet::default();

        subject
            .prepare_vnode_transition(ManagedVnodeTransition {
                target: &target,
                revoked: &revoked,
                restores: &restores,
            })
            .unwrap();
        assert_eq!(current_state_bytes(&mut subject), before);
        subject.abort_vnode_transition();
        subject.finish_vnode_transition();
        assert_eq!(current_state_bytes(&mut subject), before);

        subject
            .prepare_vnode_transition(ManagedVnodeTransition {
                target: &target,
                revoked: &revoked,
                restores: &restores,
            })
            .unwrap();
        subject.publish_vnode_transition();
        subject.finish_vnode_transition();
        assert_eq!(current_state_bytes(&mut subject), donor_slice.as_ref());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn complete_inner_roster_preflight_precedes_owned_and_arrow_decode() {
        let (context, _) = super::checkpoint_tests::context_and_batch();
        let sql = "SELECT key, SUM(value) AS total FROM events GROUP BY key";
        let (shuffle, version) = single_owner_shuffle().await;
        let mut operator = SqlQueryOperator::new("totals", sql, context, None, false);
        operator.attach_cluster_shuffle(shuffle);
        operator.initialize_managed_state().await.unwrap();

        let (empty_archive, invalid_ipc_archive, late_invalid_archive) = {
            let QueryState::Agg(ref mut aggregate) = operator.state else {
                panic!("expected initialized aggregate state");
            };
            aggregate.set_max_groups_for_test(1);
            let mut checkpoint = aggregate.empty_checkpoint();
            let empty = serialize_agg_cp(&checkpoint, &operator.op_name).unwrap();
            checkpoint.keys_ipc = vec![0xff];
            checkpoint.acc_state_ipc = vec![vec![0xff]];
            checkpoint.last_updated_ms = vec![i64::MIN];
            let invalid_ipc = serialize_agg_cp(&checkpoint, &operator.op_name).unwrap();
            checkpoint.fingerprint = checkpoint.fingerprint.wrapping_add(1);
            let late_invalid = serialize_agg_cp(&checkpoint, &operator.op_name).unwrap();
            (empty, invalid_ipc, late_invalid)
        };
        let target = laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
            version,
            &[1; 8],
            vec![laminar_core::checkpoint::CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(1),
            }],
        )
        .unwrap();
        let over_limit_restores = [
            crate::operator_graph::ManagedVnodeRestore {
                vnode: 0,
                base: &invalid_ipc_archive,
                deltas: &[],
            },
            crate::operator_graph::ManagedVnodeRestore {
                vnode: 1,
                base: &invalid_ipc_archive,
                deltas: &[],
            },
        ];
        let revoked = rustc_hash::FxHashSet::default();

        let late_invalid_restores = [
            crate::operator_graph::ManagedVnodeRestore {
                vnode: 0,
                base: &invalid_ipc_archive,
                deltas: &[],
            },
            crate::operator_graph::ManagedVnodeRestore {
                vnode: 1,
                base: &late_invalid_archive,
                deltas: &[],
            },
        ];
        let late_invalid_delta = [crate::vnode_restore_input::VnodeRestoreArchive::Borrowed(
            late_invalid_archive.as_slice(),
        )];
        let late_invalid_delta_restore = [crate::operator_graph::ManagedVnodeRestore {
            vnode: 0,
            base: &invalid_ipc_archive,
            deltas: &late_invalid_delta,
        }];
        for restores in [
            late_invalid_restores.as_slice(),
            late_invalid_delta_restore.as_slice(),
        ] {
            crate::aggregate_state::reset_owned_restore_decode_count_for_test();
            let error = operator
                .prepare_vnode_transition(ManagedVnodeTransition {
                    target: &target,
                    revoked: &revoked,
                    restores,
                })
                .expect_err("the late fingerprint mismatch must fail the borrowed pass");
            assert!(
                error.to_string().contains("fingerprint mismatch"),
                "{error}"
            );
            assert_eq!(
                crate::aggregate_state::owned_restore_decode_count_for_test(),
                0,
                "a late archive error must prevent every owned inner decode"
            );
        }

        crate::aggregate_state::reset_owned_restore_decode_count_for_test();
        let error = operator
            .prepare_vnode_transition(ManagedVnodeTransition {
                target: &target,
                revoked: &revoked,
                restores: &over_limit_restores,
            })
            .expect_err("two one-row vnode bases must exceed the one-group lower bound");
        assert!(
            error.to_string().contains("replacement_lower_bound=2"),
            "{error}"
        );
        assert_eq!(
            crate::aggregate_state::owned_restore_decode_count_for_test(),
            0,
            "complete cardinality preflight must run before the first owned inner decode"
        );
        assert!(operator.prepared_vnode_transition.is_none());
        assert!(operator.vnode_transition_cleanup.is_none());
        let QueryState::Agg(ref aggregate) = operator.state else {
            panic!("expected aggregate state after rejected preflight");
        };
        assert_eq!(aggregate.logical_group_count_for_test(), 0);

        // Active negative control: both vnodes pass the complete borrowed/cardinality pass. Arrow
        // rejects the first vnode, so lazy preparation must not deserialize the later EMPTY vnode.
        let arrow_failure_restores = [
            crate::operator_graph::ManagedVnodeRestore {
                vnode: 0,
                base: &invalid_ipc_archive,
                deltas: &[],
            },
            crate::operator_graph::ManagedVnodeRestore {
                vnode: 1,
                base: &empty_archive,
                deltas: &[],
            },
        ];
        crate::aggregate_state::reset_owned_restore_decode_count_for_test();
        let error = operator
            .prepare_vnode_transition(ManagedVnodeTransition {
                target: &target,
                revoked: &revoked,
                restores: &arrow_failure_restores,
            })
            .expect_err("the malformed key IPC must fail after roster preflight");
        assert!(error.to_string().contains("keys IPC decode"), "{error}");
        assert_eq!(
            crate::aggregate_state::owned_restore_decode_count_for_test(),
            1,
            "an earlier Arrow failure must prevent the later vnode's owned decode"
        );
        assert!(operator.prepared_vnode_transition.is_none());
        assert!(operator.vnode_transition_cleanup.is_none());
        let QueryState::Agg(ref aggregate) = operator.state else {
            panic!("expected aggregate state after Arrow rejection");
        };
        assert_eq!(aggregate.logical_group_count_for_test(), 0);
    }

    async fn aggregate_state_checkpoint() -> (SessionContext, AggStateCheckpoint) {
        let (context, batch) = super::checkpoint_tests::context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "totals",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context.clone(),
            None,
            false,
        );
        operator.process(&[vec![batch]], &[i64::MIN]).await.unwrap();
        let QueryState::Agg(ref mut aggregate) = operator.state else {
            panic!("expected aggregate state");
        };
        (context, aggregate.checkpoint_groups().unwrap())
    }

    #[tokio::test]
    async fn checkpointed_aligned_replay_emits_once_after_restore() {
        let (context, batch) = super::checkpoint_tests::context_and_batch();
        let (donor_shuffle, version) = single_owner_shuffle().await;
        let mut donor = SqlQueryOperator::new(
            "totals",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context.clone(),
            None,
            true,
        );
        donor.attach_cluster_shuffle(donor_shuffle);
        donor.lazy_init().await.unwrap();
        donor
            .aligned_replay
            .push_back((version, 42, crate::operator::RetainedBatch::local(batch)));
        let checkpoint = donor.checkpoint().unwrap().unwrap();

        let (restored_shuffle, restored_version) = single_owner_shuffle().await;
        assert_eq!(restored_version, version);
        let mut restored = SqlQueryOperator::new(
            "totals",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            true,
        );
        restored.attach_cluster_shuffle(restored_shuffle);
        restored.restore(checkpoint).unwrap();
        assert!(!restored.wants_input());
        assert_eq!(restored.watermark_hold(), Some(42));
        assert_eq!(restored.restored_output_watermark(), Some(42));

        let output = restored.process(&[Vec::new()], &[100]).await.unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(restored.aligned_replay.is_empty());
        assert!(restored.wants_input());
        assert_eq!(restored.watermark_hold(), None);

        let second = restored.process(&[Vec::new()], &[100]).await.unwrap();
        assert!(
            second.is_empty(),
            "aligned replay must not be applied twice"
        );
    }

    #[tokio::test]
    async fn aligned_replay_processes_one_logical_batch_per_cycle() {
        let (context, batch) = super::checkpoint_tests::context_and_batch();
        let (shuffle, version) = single_owner_shuffle().await;
        let mut operator = SqlQueryOperator::new(
            "totals",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            true,
        );
        operator.attach_cluster_shuffle(shuffle);
        operator.lazy_init().await.unwrap();
        operator.aligned_replay.push_back((
            version,
            41,
            crate::operator::RetainedBatch::local(batch.clone()),
        ));
        operator.aligned_replay.push_back((
            version,
            42,
            crate::operator::RetainedBatch::local(batch),
        ));

        let first = operator.process(&[Vec::new()], &[100]).await.unwrap();
        assert_eq!(first.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert_eq!(operator.aligned_replay.len(), 1);
        assert_eq!(operator.watermark_hold(), Some(42));
        assert!(!operator.wants_input());

        let second = operator.process(&[Vec::new()], &[100]).await.unwrap();
        assert!(!second.is_empty());
        assert!(operator.aligned_replay.is_empty());
        assert!(operator.wants_input());

        let after_drain = operator.process(&[Vec::new()], &[100]).await.unwrap();
        assert!(after_drain.is_empty(), "drained replay must not run again");
    }

    #[tokio::test]
    async fn failed_aligned_replay_remains_queued_for_checkpoint_recovery() {
        let (context, _) = super::checkpoint_tests::context_and_batch();
        let (shuffle, version) = single_owner_shuffle().await;
        let mut operator = SqlQueryOperator::new(
            "totals",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            true,
        );
        operator.attach_cluster_shuffle(shuffle);
        operator.lazy_init().await.unwrap();
        let invalid = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![10])),
            ],
        )
        .unwrap();
        operator.aligned_replay.push_back((
            version,
            41,
            crate::operator::RetainedBatch::local(invalid),
        ));

        let error = operator
            .process(&[Vec::new()], &[100])
            .await
            .expect_err("schema-incompatible replay must fail closed");
        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.requires_pipeline_recovery());
        assert_eq!(operator.aligned_replay.len(), 1);
        assert!(!operator.wants_input());
    }

    #[tokio::test]
    async fn aligned_replay_applies_retained_vnode_hint_before_mutation() {
        let (context, batch) = super::checkpoint_tests::context_and_batch();
        let (shuffle, version) = single_owner_shuffle().await;
        let mut operator = SqlQueryOperator::new(
            "totals",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            true,
        );
        operator.attach_cluster_shuffle(shuffle);
        operator.lazy_init().await.unwrap();
        operator.aligned_replay.push_back((
            version,
            41,
            crate::operator::RetainedBatch {
                batch,
                _admissions: Arc::from([]),
                assignment_version: Some(version),
                uniform_vnode: Some(8),
            },
        ));

        let error = operator
            .process(&[Vec::new()], &[100])
            .await
            .expect_err("an out-of-range retained vnode hint must fail before state mutation");
        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.to_string().contains("outside key-group count 8"));
        assert_eq!(operator.aligned_replay.len(), 1);
        let QueryState::Agg(ref aggregate) = operator.state else {
            panic!("expected initialized aggregate state");
        };
        assert_eq!(aggregate.logical_group_count_for_test(), 0);
    }

    #[tokio::test]
    async fn lazy_delta_restore_failure_requires_checkpoint_recovery() {
        let (context, base) = aggregate_state_checkpoint().await;
        let mut invalid_delta = base.clone();
        invalid_delta.fingerprint ^= 1;
        let mut operator = SqlQueryOperator::new(
            "totals",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
        );
        operator.pending_restore = Some(base);
        operator
            .pending_restore_deltas
            .push(crate::aggregate_state::AggVnodeDelta {
                changed: invalid_delta,
            });

        let error = operator.lazy_init().await.unwrap_err();
        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.to_string().contains("delta fingerprint mismatch"));
        assert!(error.requires_pipeline_recovery());
        assert!(matches!(operator.state, QueryState::Uninit));
        assert!(
            operator.pending_restore.is_some(),
            "a rejected delta must retain the baseline it depends on"
        );
        assert_eq!(operator.pending_restore_deltas.len(), 1);
    }

    #[tokio::test]
    async fn rehydrated_vnode_fingerprint_mismatch_is_not_dropped() {
        let (context, checkpoint) = aggregate_state_checkpoint().await;
        let first = serialize_agg_cp(&checkpoint, "totals").unwrap();
        let mut mismatched = checkpoint;
        mismatched.fingerprint ^= 1;
        let second = serialize_agg_cp(&mismatched, "totals").unwrap();
        let mut operator = SqlQueryOperator::new(
            "totals",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
        );

        operator.apply_vnode_state(1, &first).unwrap();
        let error = operator.apply_vnode_state(2, &second).unwrap_err();
        assert!(matches!(error, DbError::Pipeline(_)));
        assert!(error.to_string().contains("fingerprint mismatch"));
        assert_eq!(operator.pending_restore_slices.len(), 1);
    }

    #[tokio::test]
    async fn uninit_vnode_bases_are_retained_raw_and_bulk_merged_once() {
        let (context, _) = super::checkpoint_tests::context_and_batch();
        let sql = "SELECT key, SUM(value) AS total FROM events GROUP BY key";
        let names = (0..128)
            .map(|index| format!("key-{index}"))
            .collect::<Vec<_>>();
        let name_refs = names.iter().map(String::as_str).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("key", arrow::datatypes::DataType::Utf8, false),
                arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(name_refs)),
                Arc::new(Int64Array::from(vec![1_i64; 128])),
            ],
        )
        .unwrap();
        let mut donor = SqlQueryOperator::new("totals", sql, context.clone(), None, false);
        let (donor_shuffle, _) = single_owner_shuffle_for(64).await;
        donor.attach_cluster_shuffle(donor_shuffle);
        donor.process(&[vec![batch]], &[i64::MIN]).await.unwrap();
        let QueryState::Agg(ref mut aggregate) = donor.state else {
            panic!("expected aggregate state");
        };
        let slices = aggregate.checkpoint_groups_by_vnode(64).unwrap();
        assert!(
            slices.len() > 16,
            "test needs many independently recovered vnodes"
        );

        let mut restored = SqlQueryOperator::new("totals", sql, context, None, false);
        let (restored_shuffle, _) = single_owner_shuffle_for(64).await;
        restored.attach_cluster_shuffle(restored_shuffle);
        for (vnode, checkpoint) in &slices {
            let bytes = serialize_agg_cp(checkpoint, "totals").unwrap();
            restored.apply_vnode_state(*vnode, &bytes).unwrap();
        }
        assert!(restored.pending_restore.is_none());
        assert_eq!(restored.pending_restore_slices.len(), slices.len());

        restored.lazy_init().await.unwrap();
        assert!(restored.pending_restore_slices.is_empty());
        let QueryState::Agg(ref mut aggregate) = restored.state else {
            panic!("expected restored aggregate state");
        };
        assert_eq!(
            aggregate.checkpoint_groups().unwrap().last_updated_ms.len(),
            128
        );
    }

    #[tokio::test]
    async fn deferred_vnode_revoke_without_ownership_faults_restore() {
        let (context, base) = aggregate_state_checkpoint().await;
        let mut operator = SqlQueryOperator::new(
            "totals",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
        );
        operator.pending_restore = Some(base);
        operator.deferred_revoke_vnodes.insert(3);

        let error = operator.lazy_init().await.unwrap_err();
        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.to_string().contains("without cluster ownership"));
        assert!(operator.pending_restore.is_some());
        assert!(operator.deferred_revoke_vnodes.contains(&3));
        assert!(matches!(operator.state, QueryState::Uninit));
    }

    // A cluster-scoped aggregate never captures groups into the portable whole-node manifest;
    // assignment-scoped vnode partials remain authoritative with or without delta encoding.
    #[tokio::test]
    async fn cluster_scope_skips_whole_node_agg_capture() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let batch = |keys: &[&str], vals: &[i64]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(keys.to_vec())),
                    Arc::new(Int64Array::from(vals.to_vec())),
                ],
            )
            .unwrap()
        };

        let ctx = laminar_sql::create_session_context();
        let mem = datafusion::datasource::MemTable::try_new(
            Arc::clone(&schema),
            vec![vec![batch(&["seed"], &[0])]],
        )
        .unwrap();
        ctx.register_table("events", Arc::new(mem)).unwrap();

        let mut op = SqlQueryOperator::new(
            "out",
            "SELECT key, SUM(val) AS total FROM events GROUP BY key",
            ctx,
            None,
            false,
        );

        let registry = Arc::new(VnodeRegistry::new(8));
        registry.set_assignment((0..8).map(|_| NodeId(1)).collect::<Vec<_>>().into());
        let receiver = Arc::new(
            laminar_core::shuffle::ShuffleReceiver::bind(
                1,
                "127.0.0.1:0".parse().unwrap(),
                uuid::Uuid::from_u128(1),
            )
            .await
            .unwrap(),
        );
        op.attach_cluster_shuffle(ClusterShuffleConfig {
            registry,
            sender: Arc::new(laminar_core::shuffle::ShuffleSender::new(
                1,
                uuid::Uuid::from_u128(1),
            )),
            receiver,
            self_id: NodeId(1),
        });
        op.process(&[vec![batch(&["a", "b"], &[1, 2])]], &[i64::MIN])
            .await
            .unwrap();

        assert!(
            op.checkpoint().unwrap().is_none(),
            "cluster aggregate groups must live only in vnode partials"
        );

        op.enable_delta_checkpoints(4);
        assert!(
            op.checkpoint().unwrap().is_none(),
            "delta encoding must keep vnode partials authoritative"
        );
    }

    // A FULL vnode image is authoritative and repeat delivery is idempotent. Revocation still has
    // to remove the vnode immediately so this node does not expose state it no longer owns.
    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn authoritative_full_vnode_restore_replaces_stale_keys_and_restores_revoked_state() {
        async fn populated_op() -> SqlQueryOperator {
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("val", DataType::Int64, false),
            ]));
            let seed = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(vec!["seed"])),
                    Arc::new(Int64Array::from(vec![0_i64])),
                ],
            )
            .unwrap();
            let ctx = laminar_sql::create_session_context();
            let mem =
                datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![seed]])
                    .unwrap();
            ctx.register_table("events", Arc::new(mem)).unwrap();
            let mut op = SqlQueryOperator::new(
                "out",
                "SELECT key, SUM(val) AS total FROM events GROUP BY key",
                ctx,
                None,
                false,
            );
            let registry = Arc::new(VnodeRegistry::new(8));
            registry.set_assignment((0..8).map(|_| NodeId(1)).collect::<Vec<_>>().into());
            let receiver = Arc::new(
                laminar_core::shuffle::ShuffleReceiver::bind(
                    1,
                    "127.0.0.1:0".parse().unwrap(),
                    uuid::Uuid::from_u128(1),
                )
                .await
                .unwrap(),
            );
            op.attach_cluster_shuffle(ClusterShuffleConfig {
                registry,
                sender: Arc::new(laminar_core::shuffle::ShuffleSender::new(
                    1,
                    uuid::Uuid::from_u128(1),
                )),
                receiver,
                self_id: NodeId(1),
            });
            op
        }

        fn total_sum(op: &mut SqlQueryOperator) -> i64 {
            let QueryState::Agg(ref mut agg) = op.state else {
                panic!("expected aggregate state");
            };
            let batches = agg.emit().unwrap();
            let mut sum = 0;
            for b in &batches {
                let total = b
                    .column(b.schema().index_of("total").unwrap())
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..b.num_rows() {
                    sum += total.value(i);
                }
            }
            sum
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let batch = move |keys: &[&str], vals: &[i64]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(keys.to_vec())),
                    Arc::new(Int64Array::from(vals.to_vec())),
                ],
            )
            .unwrap()
        };

        // The durable per-vnode slice for some owned vnode `v`, captured from {a:1, b:2}.
        let mut donor = populated_op().await;
        donor
            .process(&[vec![batch(&["a", "b"], &[1, 2])]], &[i64::MIN])
            .await
            .unwrap();
        let slices = donor
            .checkpoint_by_vnode(&(0..8).collect::<Vec<_>>(), 8)
            .unwrap()
            .expect("per-vnode slices");
        let (v, slice_bytes) = slices
            .iter()
            .find_map(|(v, s)| match s {
                crate::checkpoint_coordinator::StagedSlice::Bytes(b) => {
                    let checkpoint =
                        rkyv::from_bytes::<AggStateCheckpoint, rkyv::rancor::Error>(b).unwrap();
                    (!checkpoint.last_updated_ms.is_empty()).then(|| (*v, b.clone()))
                }
                _ => None,
            })
            .expect("at least one full vnode slice");

        // Replaying an already-applied FULL slice, including retry after a lost ack, is idempotent.
        let mut control = populated_op().await;
        control
            .process(&[vec![batch(&["a", "b"], &[1, 2])]], &[i64::MIN])
            .await
            .unwrap();
        control.apply_vnode_state(v, &slice_bytes).unwrap();
        control.apply_vnode_state(v, &slice_bytes).unwrap();
        assert_eq!(total_sum(&mut control), 3);

        // Revocation removes the vnode immediately; a later authoritative FULL restores it.
        let mut fixed = populated_op().await;
        fixed
            .process(&[vec![batch(&["a", "b"], &[1, 2])]], &[i64::MIN])
            .await
            .unwrap();
        let revoked: rustc_hash::FxHashSet<u32> = [v].into_iter().collect();
        fixed.drop_owned_vnodes(&revoked).unwrap();
        let remaining_after_revoke = total_sum(&mut fixed);
        assert!(remaining_after_revoke < 3);
        fixed.apply_vnode_state(v, &slice_bytes).unwrap();
        assert_eq!(
            total_sum(&mut fixed),
            3,
            "re-acquiring the vnode restores the committed aggregate"
        );

        // The graph callback consumes an authoritative chain, not a merge patch. A key written
        // after the durable cut must disappear even when ownership of the vnode was retained.
        let extra_key = (0..1_024)
            .map(|candidate| format!("post-cut-{candidate}"))
            .find(|candidate| {
                let candidate_batch = batch(&[candidate.as_str()], &[100]);
                hash_rows_to_vnodes(&candidate_batch, 1, 8).unwrap()[0] == v
            })
            .expect("one candidate key must hash to the restored vnode");
        let mut replaced = populated_op().await;
        replaced
            .process(&[vec![batch(&["a", "b"], &[1, 2])]], &[i64::MIN])
            .await
            .unwrap();
        replaced
            .process(&[vec![batch(&[extra_key.as_str()], &[100])]], &[i64::MIN])
            .await
            .unwrap();
        assert!(total_sum(&mut replaced) > 3);
        replaced.apply_vnode_chain(v, &slice_bytes, &[]).unwrap();
        assert_eq!(
            total_sum(&mut replaced),
            3,
            "authoritative restore must remove keys absent from the committed vnode image"
        );

        let empty = match &replaced.state {
            QueryState::Agg(aggregate) => aggregate.empty_checkpoint(),
            _ => panic!("expected initialized aggregate state"),
        };
        let empty = serialize_agg_cp(&empty, &replaced.op_name).unwrap();
        replaced.apply_vnode_chain(v, &empty, &[]).unwrap();
        assert_eq!(
            total_sum(&mut replaced),
            remaining_after_revoke,
            "semantic EMPTY must remove every group in its authoritative vnode"
        );
    }

    // A chain applied while Uninit preserves its last-emitted baseline. Physical ownership
    // movement emits nothing; the next real update replaces the prior logical row exactly once.
    #[tokio::test]
    async fn uninit_chain_restore_is_silent_until_real_change() {
        async fn changelog_op() -> SqlQueryOperator {
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("val", DataType::Int64, false),
            ]));
            let seed = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(vec!["seed"])),
                    Arc::new(Int64Array::from(vec![0_i64])),
                ],
            )
            .unwrap();
            let ctx = laminar_sql::create_session_context();
            let mem =
                datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![seed]])
                    .unwrap();
            ctx.register_table("events", Arc::new(mem)).unwrap();
            let mut op = SqlQueryOperator::new(
                "out",
                "SELECT key, SUM(val) AS total FROM events GROUP BY key",
                ctx,
                None,
                true, // changelog: emission is dirty-gated + last_emitted-deduped
            );
            let registry = Arc::new(VnodeRegistry::new(8));
            registry.set_assignment((0..8).map(|_| NodeId(1)).collect::<Vec<_>>().into());
            let receiver = Arc::new(
                laminar_core::shuffle::ShuffleReceiver::bind(
                    1,
                    "127.0.0.1:0".parse().unwrap(),
                    uuid::Uuid::from_u128(1),
                )
                .await
                .unwrap(),
            );
            op.attach_cluster_shuffle(ClusterShuffleConfig {
                registry,
                sender: Arc::new(laminar_core::shuffle::ShuffleSender::new(
                    1,
                    uuid::Uuid::from_u128(1),
                )),
                receiver,
                self_id: NodeId(1),
            });
            op
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![1_i64, 2])),
            ],
        )
        .unwrap();

        // Donor: process emits {a,b} (populates last_emitted), then capture every full vnode slice.
        let mut donor = changelog_op().await;
        let emitted: usize = donor
            .process(&[vec![batch]], &[i64::MIN])
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(emitted, 2);
        let slices = donor
            .checkpoint_by_vnode(&(0..8).collect::<Vec<_>>(), 8)
            .unwrap()
            .expect("per-vnode slices");

        // Apply the chains while Uninit (the boot-staging shape), then initialize without input.
        let mut subject = changelog_op().await;
        for (v, s) in &slices {
            if let crate::checkpoint_coordinator::StagedSlice::Bytes(b) = s {
                subject.apply_vnode_chain(*v, b, &[]).unwrap();
            }
        }
        assert!(subject.process(&[], &[i64::MIN]).await.unwrap().is_empty());
        let update = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![3_i64])),
            ],
        )
        .unwrap();
        let changed = subject.process(&[vec![update]], &[i64::MIN]).await.unwrap();
        assert_eq!(changed.len(), 1);
        let totals = changed[0]
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let weights = changed[0]
            .column_by_name(laminar_core::changelog::WEIGHT_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(totals.values(), &[1, 4]);
        assert_eq!(weights.values(), &[-1, 1]);
    }

    // The soak signature: chains for MANY vnodes applied while Uninit fold into ONE concatenated
    // pending baseline; every donor group must survive the fold (partial folds = one-burst loss).
    #[tokio::test]
    async fn uninit_chain_fold_restores_every_vnode_group() {
        async fn changelog_op() -> SqlQueryOperator {
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("val", DataType::Int64, false),
            ]));
            let seed = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(vec!["seed"])),
                    Arc::new(Int64Array::from(vec![0_i64])),
                ],
            )
            .unwrap();
            let ctx = laminar_sql::create_session_context();
            let mem =
                datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![seed]])
                    .unwrap();
            ctx.register_table("events", Arc::new(mem)).unwrap();
            let mut op = SqlQueryOperator::new(
                "out",
                "SELECT key, SUM(val) AS total FROM events GROUP BY key",
                ctx,
                None,
                true,
            );
            let registry = Arc::new(VnodeRegistry::new(8));
            registry.set_assignment((0..8).map(|_| NodeId(1)).collect::<Vec<_>>().into());
            let receiver = Arc::new(
                laminar_core::shuffle::ShuffleReceiver::bind(
                    1,
                    "127.0.0.1:0".parse().unwrap(),
                    uuid::Uuid::from_u128(1),
                )
                .await
                .unwrap(),
            );
            op.attach_cluster_shuffle(ClusterShuffleConfig {
                registry,
                sender: Arc::new(laminar_core::shuffle::ShuffleSender::new(
                    1,
                    uuid::Uuid::from_u128(1),
                )),
                receiver,
                self_id: NodeId(1),
            });
            op
        }
        const GROUPS: usize = 500;
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let keys: Vec<String> = (0..GROUPS).map(|k| format!("k{k}")).collect();
        let vals: Vec<i64> = (0_i64..500).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(
                    keys.iter().map(String::as_str).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap();

        let mut donor = changelog_op().await;
        let emitted: usize = donor
            .process(&[vec![batch]], &[i64::MIN])
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(emitted, GROUPS);
        let slices = donor
            .checkpoint_by_vnode(&(0..8).collect::<Vec<_>>(), 8)
            .unwrap()
            .expect("per-vnode slices");

        let mut subject = changelog_op().await;
        let mut applied = 0usize;
        for (v, s) in &slices {
            if let crate::checkpoint_coordinator::StagedSlice::Bytes(b) = s {
                subject.apply_vnode_chain(*v, b, &[]).unwrap();
                applied += 1;
            }
        }
        assert!(
            applied >= 7,
            "expected slices for ~all 8 vnodes, got {applied}"
        );
        assert!(subject.process(&[], &[i64::MIN]).await.unwrap().is_empty());
        let QueryState::Agg(ref mut aggregate) = subject.state else {
            panic!("expected restored aggregate state");
        };
        assert_eq!(
            aggregate.checkpoint_groups().unwrap().last_updated_ms.len(),
            GROUPS,
            "the Uninit fold must restore every donor group across all vnodes without emitting"
        );
    }
}
