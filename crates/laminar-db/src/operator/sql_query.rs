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
use crate::operator_graph::{
    try_evaluate_compiled, CapturedVnodeState, EncodedStateFrame, GraphOperator,
    ManagedStateAccountingSnapshot, OperatorCheckpoint, StateFrameCapture,
};
#[cfg(feature = "cluster")]
use crate::operator_graph::{InputFrontier, ManagedVnodeTransition};
use crate::sql_analysis::{extract_projection_filter, single_source_table};

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
}

#[cfg(feature = "cluster")]
enum SqlVnodeTransitionCleanup {
    Aborted(PreparedSqlVnodeTransition),
    Published(RetiredAggVnodeTransition),
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
    // Pre-barrier remote rows are channel state. They must be replayed after the cut rather than
    // folded into aggregate state before its corresponding output is emitted.
    aligned_replay: Vec<(u64, i64, Vec<u8>)>,
}

fn serialize_agg_cp(
    cp: &AggStateCheckpoint,
    op_name: &str,
    max_encoded_bytes: usize,
) -> Result<EncodedStateFrame, DbError> {
    let writer = rkyv::ser::writer::IoWriter::new(
        laminar_core::serialization::BoundedBytesWriter::new(max_encoded_bytes),
    );
    rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(cp, writer)
        .map(|bytes| EncodedStateFrame::from_vec(bytes.into_inner().into_vec()))
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "aggregate '{op_name}' vnode checkpoint exceeded its {max_encoded_bytes}-byte limit: {error}"
            ))
        })
}

#[cfg(feature = "cluster")]
fn encode_aligned_replay_capture(
    op_name: &str,
    aligned_replay: &[(u64, i64, crate::operator::RetainedBatch)],
    max_working_bytes: usize,
) -> Result<EncodedStateFrame, DbError> {
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(aligned_replay.len())
        .map_err(|_| {
            DbError::Checkpoint(format!(
                "aligned aggregate replay checkpoint for '{op_name}' could not reserve metadata"
            ))
        })?;
    let mut working_bytes = encoded
        .capacity()
        .checked_mul(std::mem::size_of::<(u64, i64, Vec<u8>)>())
        .filter(|bytes| *bytes <= max_working_bytes)
        .ok_or_else(|| {
            DbError::Checkpoint(format!(
                "aligned aggregate replay checkpoint for '{op_name}' metadata exceeds its {max_working_bytes}-byte working limit"
            ))
        })?;

    for (assignment_version, watermark, batch) in aligned_replay {
        let remaining = max_working_bytes
            .checked_sub(working_bytes)
            .filter(|remaining| *remaining != 0)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "aligned aggregate replay checkpoint for '{op_name}' exhausted its {max_working_bytes}-byte working limit"
                ))
            })?;
        let blob = laminar_core::serialization::serialize_batches_stream_bounded(
            batch.batch().schema().as_ref(),
            std::iter::once(batch.batch()),
            remaining,
        )
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "aligned aggregate replay checkpoint for '{op_name}' IPC serialization: {error}"
            ))
        })?;
        working_bytes = working_bytes
            .checked_add(blob.capacity())
            .filter(|bytes| *bytes <= max_working_bytes)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "aligned aggregate replay checkpoint for '{op_name}' IPC exceeds its {max_working_bytes}-byte working limit"
                ))
            })?;
        encoded.push((*assignment_version, *watermark, blob));
    }

    let archive_budget = max_working_bytes
        .checked_sub(working_bytes)
        .ok_or_else(|| {
            DbError::Checkpoint(format!(
                "aligned aggregate replay checkpoint for '{op_name}' exhausted its working limit before archive serialization"
            ))
        })?;
    let writer = rkyv::ser::writer::IoWriter::new(
        laminar_core::serialization::BoundedBytesWriter::new(archive_budget),
    );
    rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(
        &AggOpCheckpoint {
            aligned_replay: encoded,
        },
        writer,
    )
    .map(|bytes| EncodedStateFrame::from_vec(bytes.into_inner().into_vec()))
    .map_err(|error| {
        DbError::Checkpoint(format!(
            "aligned aggregate replay checkpoint for '{op_name}' archive serialization exceeded its {archive_budget}-byte headroom: {error}"
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
    execution_path_logged: bool,
    emit_changelog: bool,
    #[cfg(feature = "cluster")]
    cluster_shuffle: Option<ClusterShuffleConfig>,
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
            execution_path_logged: false,
            emit_changelog,
            #[cfg(feature = "cluster")]
            cluster_shuffle: None,
            #[cfg(feature = "cluster")]
            aligned_replay: VecDeque::new(),
            #[cfg(feature = "cluster")]
            prepared_vnode_transition: None,
            #[cfg(feature = "cluster")]
            vnode_transition_cleanup: None,
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn attach_cluster_shuffle(&mut self, config: ClusterShuffleConfig) {
        debug_assert_eq!(
            config.registry.vnode_count(),
            u32::from(self.key_group_count)
        );
        self.cluster_shuffle = Some(config);
    }

    #[allow(clippy::too_many_lines)]
    async fn lazy_init(&mut self) -> Result<(), DbError> {
        if let Some(agg_state) = IncrementalAggState::try_from_sql(
            &self.ctx,
            &self.sql,
            self.emit_changelog,
            self.key_group_count,
        )
        .await?
        {
            if self.emit_changelog && agg_state.having_filter().is_some() {
                return Err(DbError::Pipeline(format!(
                    "aggregate '{}' cannot use HAVING with changelog output until transition-aware HAVING retractions are implemented",
                    self.op_name
                )));
            }
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
            self.log_execution_path(agg_state.compiled_projection().is_some());
            self.state = QueryState::Agg(Box::new(agg_state));
            return Ok(());
        }

        let df = self
            .ctx
            .sql(&self.sql)
            .await
            .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
        let plan = df.logical_plan().clone();

        if crate::aggregate_state::find_aggregate(&plan).is_some() {
            return Err(DbError::Unsupported(format!(
                "[{}] query '{}': aggregate cannot use the generic DataFusion path; the incremental execution path was not constructed",
                laminar_core::error_codes::SQL_UNSUPPORTED,
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
            return self.execute_aligned_replay();
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
        let output = self.emit_agg_output();
        output.map_err(|error| {
            stateful_apply_outcome_unknown(&self.op_name, "output construction", error)
        })
    }

    #[cfg(feature = "cluster")]
    fn execute_aligned_replay(&mut self) -> Result<Vec<RecordBatch>, DbError> {
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
        let output = self.emit_agg_output().map_err(|error| {
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

    fn emit_agg_output(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        let QueryState::Agg(ref mut agg_state) = self.state else {
            return Err(DbError::Pipeline(
                "internal: emit_agg_output on non-agg".into(),
            ));
        };

        let mut batches = agg_state.emit()?;

        if let Some(filter) = agg_state.having_filter() {
            batches = apply_compiled_having(&batches, filter)?;
        }

        Ok(batches)
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

#[async_trait]
impl GraphOperator for SqlQueryOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        debug_assert_eq!(
            self.capability.implementation,
            OperatorImplementation::SqlQuery
        );
        self.capability
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_aligned_replay_pending(&self) -> bool {
        !self.aligned_replay.is_empty()
    }

    fn managed_state_accounting(&self) -> Option<ManagedStateAccountingSnapshot> {
        let QueryState::Agg(aggregate) = &self.state else {
            return None;
        };

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
                Some(SqlVnodeTransitionCleanup::Published(aggregate)) => {
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

    async fn initialize_managed_state(&mut self) -> Result<(), DbError> {
        if matches!(self.state, QueryState::Uninit) {
            self.lazy_init().await?;
        }
        if matches!(self.state, QueryState::Agg(_)) {
            return Ok(());
        }
        // The immutable AST classifier can over-approximate function syntax. Only an initialized
        // incremental aggregate owns managed state.
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
        #[cfg(not(feature = "cluster"))]
        {
            Ok(None)
        }

        #[cfg(feature = "cluster")]
        {
            if self.aligned_replay.is_empty() {
                return Ok(None);
            }
            let aligned_replay: Vec<(u64, i64, crate::operator::RetainedBatch)> = self
                .aligned_replay
                .iter()
                .map(|(assignment_version, watermark, batch)| {
                    (*assignment_version, *watermark, batch.clone())
                })
                .collect();
            let state = encode_aligned_replay_capture(&self.op_name, &aligned_replay, usize::MAX)?;
            Ok(Some(OperatorCheckpoint {
                data: state.bytes().to_vec(),
            }))
        }
    }

    fn checkpoint_capture(
        &mut self,
        max_capture_bytes: u64,
    ) -> Result<Option<StateFrameCapture>, DbError> {
        #[cfg(not(feature = "cluster"))]
        {
            let _ = max_capture_bytes;
            Ok(None)
        }

        #[cfg(feature = "cluster")]
        {
            if self.aligned_replay.is_empty() {
                return Ok(None);
            }

            let batch_bytes = self
                .aligned_replay
                .iter()
                .try_fold(0usize, |total, (_, _, batch)| {
                    total.checked_add(batch.heap_bytes()?)
                })
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "aligned aggregate replay capture for '{}' overflowed byte accounting",
                        self.op_name
                    ))
                })?;
            let requested_roster_bytes = self
                .aligned_replay
                .len()
                .checked_mul(std::mem::size_of::<(
                    u64,
                    i64,
                    crate::operator::RetainedBatch,
                )>())
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "aligned aggregate replay capture for '{}' overflowed metadata accounting",
                        self.op_name
                    ))
                })?;
            let requested_bytes = batch_bytes
                .checked_add(requested_roster_bytes)
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "aligned aggregate replay capture for '{}' overflowed byte accounting",
                        self.op_name
                    ))
                })?;
            if requested_bytes > max_capture_bytes {
                return Err(DbError::Checkpoint(format!(
                    "aligned aggregate replay capture for '{}' retains {requested_bytes} bytes; capture headroom is {max_capture_bytes} bytes",
                    self.op_name
                )));
            }

            let mut aligned_replay = Vec::new();
            aligned_replay
                .try_reserve_exact(self.aligned_replay.len())
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "aligned aggregate replay capture for '{}' could not reserve metadata",
                        self.op_name
                    ))
                })?;
            let roster_bytes = aligned_replay
                .capacity()
                .checked_mul(std::mem::size_of::<(
                    u64,
                    i64,
                    crate::operator::RetainedBatch,
                )>())
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "aligned aggregate replay capture for '{}' overflowed metadata accounting",
                        self.op_name
                    ))
                })?;
            let retained_bytes = batch_bytes
                .checked_add(roster_bytes)
                .and_then(|bytes| u64::try_from(bytes).ok())
                .filter(|bytes| *bytes <= max_capture_bytes)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "aligned aggregate replay capture for '{}' exceeds its {max_capture_bytes}-byte capture limit",
                        self.op_name
                    ))
                })?;
            aligned_replay.extend(self.aligned_replay.iter().map(
                |(assignment_version, watermark, batch)| {
                    (*assignment_version, *watermark, batch.clone())
                },
            ));

            let op_name = Arc::clone(&self.op_name);
            Ok(Some(StateFrameCapture::deferred(
                retained_bytes,
                move |max_working_bytes| {
                    encode_aligned_replay_capture(
                        op_name.as_ref(),
                        &aligned_replay,
                        max_working_bytes,
                    )
                },
            )))
        }
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let checkpoint = rkyv::from_bytes::<AggOpCheckpoint, rkyv::rancor::Error>(&checkpoint.data)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "checkpoint deserialization for '{}': {error}",
                    self.op_name
                ))
            })?;

        #[cfg(not(feature = "cluster"))]
        if !checkpoint.aligned_replay.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' checkpoint contains cluster shuffle replay without cluster support",
                self.op_name
            )));
        }

        #[cfg(feature = "cluster")]
        {
            let decoded = checkpoint
                .aligned_replay
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
            if !self.aligned_replay.is_empty() && !decoded.is_empty() {
                return Err(DbError::Checkpoint(format!(
                    "aggregate '{}' aligned shuffle replay was applied more than once",
                    self.op_name
                )));
            }
            self.aligned_replay.extend(decoded);
        }

        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn output_frontier(&self, input: InputFrontier) -> InputFrontier {
        input.held_at(
            self.aligned_replay
                .iter()
                .map(|(_, watermark, _)| *watermark)
                .min(),
        )
    }

    #[cfg(feature = "cluster")]
    fn restored_output_frontier(&self) -> Option<InputFrontier> {
        let watermark = self
            .aligned_replay
            .iter()
            .map(|(_, watermark, _)| *watermark)
            .min()?;
        Some(InputFrontier {
            watermark: Some(watermark),
            idle: false,
        })
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

    fn checkpoint_vnodes(
        &mut self,
        required_vnodes: &[u32],
        vnode_count: u32,
        max_capture_bytes: u64,
    ) -> Result<Option<Vec<CapturedVnodeState>>, DbError> {
        let QueryState::Agg(aggregate) = &mut self.state else {
            return Ok(None);
        };
        let checkpoints =
            aggregate.capture_checkpoint_vnodes(required_vnodes, vnode_count, max_capture_bytes)?;
        let mut captured = Vec::with_capacity(checkpoints.len());
        let empty_frame = Arc::new(std::sync::OnceLock::<bytes::Bytes>::new());
        for (vnode, checkpoint) in checkpoints {
            let retained_bytes = checkpoint.retained_bytes();
            let empty_frame = checkpoint.is_empty().then(|| Arc::clone(&empty_frame));
            let op_name = Arc::clone(&self.op_name);
            let state = StateFrameCapture::deferred(retained_bytes, move |max_encoded_bytes| {
                if let Some(encoded) = empty_frame.as_ref().and_then(|frame| frame.get()) {
                    return Ok(EncodedStateFrame::shared(encoded.clone()));
                }
                let checkpoint = checkpoint.encode(max_encoded_bytes)?;
                let retained_serialization_bytes = checkpoint.retained_serialization_bytes()?;
                let archive_budget = max_encoded_bytes
                    .checked_sub(retained_serialization_bytes)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "aggregate '{op_name}' intermediate checkpoint exhausted its frame budget"
                        ))
                    })?;
                let encoded = serialize_agg_cp(&checkpoint, &op_name, archive_budget)?;
                if let Some(empty_frame) = empty_frame {
                    let _ = empty_frame.set(encoded.bytes().clone());
                }
                Ok(encoded)
            });
            captured.push(CapturedVnodeState {
                vnode,
                state: Some(state),
            });
        }
        Ok(Some(captured))
    }

    fn restore_vnode(&mut self, vnode: u32, vnode_count: u32, state: &[u8]) -> Result<(), DbError> {
        let QueryState::Agg(aggregate) = &mut self.state else {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' vnode restore requires initialized managed state",
                self.op_name
            )));
        };
        let profile = aggregate.vnode_archive_restore_profile();
        let checkpoint = profile
            .preflight(
                state,
                format_args!("aggregate '{}' vnode {vnode}", self.op_name),
            )
            .and_then(|archive| {
                archive.deserialize(format_args!("aggregate '{}' vnode {vnode}", self.op_name))
            })
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        aggregate
            .restore_vnode(vnode, vnode_count, checkpoint)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' vnode {vnode} restore: {error}",
                    self.op_name
                ))
            })
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

        let archive_profile = aggregate.vnode_archive_restore_profile();
        let mut preflighted = Vec::new();
        preflighted
            .try_reserve_exact(transition.restores.len())
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' could not reserve archive preflight metadata",
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
            let state = archive_profile.preflight(
                restore.state,
                format_args!(
                    "per-vnode state for '{}' vnode {}",
                    self.op_name, restore.vnode
                ),
            )?;
            restored_lower_bounds.push((restore.vnode, state.group_count()));
            preflighted.push((restore.vnode, state));
        }
        aggregate.preflight_vnode_transition_cardinality(
            transition.target.vnode_count,
            &restored_lower_bounds,
            transition.revoked,
        )?;

        let owned_restores = preflighted.into_iter().map(|(vnode, state)| {
            let state = state.deserialize(format_args!(
                "per-vnode state for '{}' vnode {vnode}",
                self.op_name
            ))?;
            Ok(OwnedAggVnodeRestore { vnode, state })
        });
        let aggregate = aggregate.prepare_owned_vnode_transition(
            transition.target.vnode_count,
            owned_restores,
            transition.revoked,
        )?;
        self.prepared_vnode_transition = Some(PreparedSqlVnodeTransition { aggregate });
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
        self.vnode_transition_cleanup =
            Some(SqlVnodeTransitionCleanup::Published(retired_aggregate));
    }

    #[cfg(feature = "cluster")]
    fn finish_vnode_transition(&mut self) {
        match self.vnode_transition_cleanup.take() {
            Some(SqlVnodeTransitionCleanup::Aborted(prepared)) => drop(prepared),
            Some(SqlVnodeTransitionCleanup::Published(aggregate)) => {
                IncrementalAggState::finish_vnode_transition(aggregate);
            }
            None => {}
        }
    }

    fn force_full_vnode_capture(&mut self) {
        if let QueryState::Agg(aggregate) = &mut self.state {
            aggregate.force_full_vnode_capture();
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
        assert!(operator.checkpoint().unwrap().is_none());
        let captured = operator
            .checkpoint_vnodes(&(0..8).collect::<Vec<_>>(), 8, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(captured.len(), 8);
        assert!(captured.iter().all(|frame| frame.state.is_some()));
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn aligned_replay_checkpoint_capture_is_deferred_and_bounded() {
        let (context, batch) = context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
        );
        operator.aligned_replay.push_back((
            7,
            123,
            crate::operator::RetainedBatch::local(batch.clone()),
        ));

        let error = operator.checkpoint_capture(0).unwrap_err();
        assert!(error.to_string().contains("capture headroom"));

        let capture = operator
            .checkpoint_capture(1 << 20)
            .unwrap()
            .expect("replay state must be captured");
        let mut staged_bytes = capture.retained_bytes();
        let encoded = capture.materialize(&mut staged_bytes, 1 << 20).unwrap();

        let (restored_context, _) = context_and_batch();
        let mut restored = SqlQueryOperator::new(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            restored_context,
            None,
            false,
        );
        restored
            .restore(OperatorCheckpoint {
                data: encoded.to_vec(),
            })
            .unwrap();
        let (assignment, watermark, restored_batch) = restored.aligned_replay.front().unwrap();
        assert_eq!((*assignment, *watermark), (7, 123));
        assert_eq!(restored_batch.batch(), &batch);
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
    async fn derived_aggregate_requires_incremental_execution() {
        let (context, _) = context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "ratio",
            "SELECT SUM(value) / COUNT(value) AS ratio FROM events",
            context,
            None,
            false,
        );

        let error = operator.initialize_managed_state().await.unwrap_err();
        assert!(matches!(error, DbError::Unsupported(_)));
        assert!(format!("{error}").contains(laminar_core::error_codes::SQL_UNSUPPORTED));
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

    #[tokio::test]
    async fn vnode_capture_is_incremental_and_restores_without_whole_state() {
        let (context, batch) = context_and_batch();
        let key_groups = KeyGroupCount::try_from(8_u16).unwrap();
        let mut donor = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context.clone(),
            None,
            false,
            key_groups,
        );
        donor.initialize_managed_state().await.unwrap();
        donor.process(&[vec![batch]], &[100]).await.unwrap();
        let owned = (0..8).collect::<Vec<_>>();
        let baseline = donor
            .checkpoint_vnodes(&owned, 8, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(baseline.len(), owned.len());
        assert!(baseline.iter().all(|frame| frame.state.is_some()));
        assert!(donor
            .checkpoint_vnodes(&owned, 8, u64::MAX)
            .unwrap()
            .unwrap()
            .is_empty());

        let mut restored = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
            key_groups,
        );
        restored.initialize_managed_state().await.unwrap();
        for frame in baseline {
            let capture = frame.state.unwrap();
            let mut staged_bytes = capture.retained_bytes();
            let state = capture.materialize(&mut staged_bytes, u64::MAX).unwrap();
            restored.restore_vnode(frame.vnode, 8, &state).unwrap();
        }
        let QueryState::Agg(aggregate) = &mut restored.state else {
            panic!("expected restored aggregate state");
        };
        assert_eq!(aggregate.logical_group_count_for_test(), 2);

        donor.force_full_vnode_capture();
        let forced = donor
            .checkpoint_vnodes(&owned, 8, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(forced.len(), owned.len());
        assert!(forced.iter().all(|frame| frame.state.is_some()));
    }

    #[cfg(not(feature = "cluster"))]
    #[test]
    fn cluster_shuffle_checkpoint_is_rejected_without_support() {
        let (context, _) = context_and_batch();
        let checkpoint = AggOpCheckpoint {
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
