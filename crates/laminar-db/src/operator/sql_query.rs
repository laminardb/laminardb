//! Standard SQL query operator with lazy initialization.
//!
//! Handles all non-EOWC, non-join queries. On first `process()` call,
//! introspects the SQL via `DataFusion` to determine the execution path:
//! - Aggregate (GROUP BY) -> incremental accumulators
//! - Simple single-source -> compiled `PhysicalExpr` projection
//! - Complex non-aggregate -> cached `LogicalPlan` (physical planning per cycle)

use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion_expr::LogicalPlan;

use crate::aggregate_state::{
    apply_compiled_having, AggStateCheckpoint, CompiledProjection, IncrementalAggState,
};
use crate::error::DbError;
use crate::metrics::PipelineCounters;
use crate::operator_graph::{try_evaluate_compiled, GraphOperator, OperatorCheckpoint};
use crate::sql_analysis::{extract_projection_filter, single_source_table};

/// Internal state for the query operator (lazy initialization).
enum QueryState {
    /// Not yet initialized -- need to introspect SQL on first call.
    Uninit,
    /// Aggregate query -- incremental accumulator path.
    Agg(Box<IncrementalAggState>),
    /// Non-aggregate single-source -- compiled `PhysicalExpr` evaluation.
    /// Carries a fallback `LogicalPlan` for runtime errors (e.g., type mismatches
    /// not caught during compilation).
    Compiled(CompiledProjection, Box<LogicalPlan>),
    /// Non-aggregate complex -- cached `LogicalPlan` (physical planning per cycle).
    CachedPlan(Box<LogicalPlan>),
}

pub(crate) struct SqlQueryOperator {
    op_name: Arc<str>,
    sql: String,
    ctx: SessionContext,
    state: QueryState,
    counters: Option<Arc<PipelineCounters>>,
    pending_restore: Option<AggStateCheckpoint>,
    tier_logged: bool,
    cached_having_plan: Option<LogicalPlan>,
}

impl SqlQueryOperator {
    pub fn new(
        name: &str,
        sql: &str,
        ctx: SessionContext,
        counters: Option<Arc<PipelineCounters>>,
    ) -> Self {
        Self {
            op_name: Arc::from(name),
            sql: sql.to_string(),
            ctx,
            state: QueryState::Uninit,
            counters,
            pending_restore: None,
            tier_logged: false,
            cached_having_plan: None,
        }
    }

    /// Lazily initialize the query state on first `process()` call.
    ///
    /// Introspects the SQL via `DataFusion` to determine whether this is an
    /// aggregate query (GROUP BY) or a non-aggregate query, then selects
    /// the appropriate execution path.
    async fn lazy_init(&mut self) -> Result<(), DbError> {
        // 1. Try aggregate path first
        match IncrementalAggState::try_from_sql(&self.ctx, &self.sql).await {
            Ok(Some(mut agg_state)) => {
                // Apply any pending checkpoint before switching state.
                if let Some(ref cp) = self.pending_restore {
                    if let Err(e) = agg_state.restore_groups(cp) {
                        tracing::warn!(
                            query = %self.op_name,
                            error = %e,
                            "Failed to restore aggregate checkpoint (schema evolution?)"
                        );
                    }
                }
                self.pending_restore = None;

                self.log_tier(agg_state.compiled_projection().is_some());
                self.state = QueryState::Agg(Box::new(agg_state));
                return Ok(());
            }
            Ok(None) => {
                // Not an aggregate query -- fall through to non-agg paths.
            }
            Err(e) => {
                tracing::debug!(
                    query = %self.op_name,
                    error = %e,
                    "Could not introspect query plan for aggregate detection, using cached plan"
                );
            }
        }

        // 2. Non-aggregate: try compiled projection for single-source queries,
        //    otherwise cache the logical plan.
        let df = self
            .ctx
            .sql(&self.sql)
            .await
            .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;

        let plan = df.logical_plan().clone();

        // Only attempt compiled projection for single-source queries
        // (rejects self-joins, multi-source, etc.)
        if single_source_table(&self.sql).is_some() {
            if let Some(proj) = self.try_build_compiled_projection(&plan) {
                tracing::debug!(
                    query = %self.op_name,
                    "Non-aggregate single-source query compiled to PhysicalExpr"
                );
                self.log_tier(true);
                self.state = QueryState::Compiled(proj, Box::new(plan));
                return Ok(());
            }
        }

        self.log_tier(false);
        self.state = QueryState::CachedPlan(Box::new(plan));
        Ok(())
    }

    /// Log the execution tier (compiled vs. cached plan) once per query.
    fn log_tier(&mut self, compiled: bool) {
        if self.tier_logged {
            return;
        }
        self.tier_logged = true;

        if let Some(ref c) = self.counters {
            if compiled {
                c.queries_compiled
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                c.queries_cached_plan
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    /// Try to build a `CompiledProjection` from a logical plan.
    ///
    /// Returns `Some` if the plan is a simple Projection + Filter over a single
    /// `TableScan` and all expressions compile to `PhysicalExpr`.
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

        let output_schema = Arc::new(arrow::datatypes::Schema::new(proj_fields));
        Some(CompiledProjection {
            source_table: info.source_table,
            exprs: compiled_exprs,
            filter: compiled_filter,
            output_schema,
        })
    }

    /// Execute the aggregate path: pre-agg -> `process_batch` -> emit -> HAVING.
    async fn execute_agg(&mut self, inputs: &[RecordBatch]) -> Result<Vec<RecordBatch>, DbError> {
        let QueryState::Agg(ref mut agg_state) = self.state else {
            return Err(DbError::Pipeline(
                "internal: execute_agg called on non-agg state".into(),
            ));
        };

        // Step 1: Pre-aggregation -- project input batches to group-keys + agg-inputs.
        let pre_agg_batches = if let Some(proj) = agg_state.compiled_projection() {
            // Compiled path: direct PhysicalExpr evaluation, no MemTable.
            // Use try_evaluate_compiled to detect errors — silently dropping
            // failing batches would corrupt aggregate totals.
            match try_evaluate_compiled(proj, inputs) {
                Ok(result) => result,
                Err(e) => {
                    tracing::debug!(
                        query = %self.op_name,
                        error = %e,
                        "Compiled pre-agg projection failed, falling back to cached plan"
                    );
                    if let Some(plan) = agg_state.cached_pre_agg_plan() {
                        let plan = plan.clone();
                        Self::execute_plan(&self.ctx, &self.op_name, &plan).await?
                    } else {
                        return Err(DbError::Pipeline(format!(
                            "[LDB-8051] query '{}': compiled pre-agg failed and no cached plan: {e}",
                            self.op_name
                        )));
                    }
                }
            }
        } else if let Some(plan) = agg_state.cached_pre_agg_plan() {
            // Cached plan path: MemTable already registered by the graph.
            let plan = plan.clone();
            Self::execute_plan(&self.ctx, &self.op_name, &plan).await?
        } else {
            return Err(DbError::Pipeline(format!(
                "[LDB-8050] query '{}': no compiled projection or cached plan",
                self.op_name
            )));
        };

        // Re-borrow agg_state mutably after the await point.
        let QueryState::Agg(ref mut agg_state) = self.state else {
            unreachable!();
        };

        // Step 2: Feed pre-agg batches to incremental accumulators.
        for batch in &pre_agg_batches {
            agg_state.process_batch(batch)?;
        }

        // Step 3: Emit running aggregate totals.
        let having_filter = agg_state.having_filter().cloned();
        let having_sql = agg_state.having_sql().map(String::from);
        let mut batches = agg_state.emit()?;

        // Step 4: Apply HAVING filter.
        if let Some(ref filter) = having_filter {
            batches = apply_compiled_having(&batches, filter)?;
        } else if let Some(ref having_sql) = having_sql {
            batches = self.apply_having_sql(&batches, having_sql).await?;
        }

        Ok(batches)
    }

    /// Execute a `LogicalPlan` through `DataFusion` physical planning (no re-plan).
    ///
    /// Used for sub-queries (pre-agg, HAVING) where the plan is always valid.
    async fn execute_plan(
        ctx: &SessionContext,
        op_name: &str,
        plan: &LogicalPlan,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let physical = ctx
            .state()
            .create_physical_plan(plan)
            .await
            .map_err(|e| DbError::query_pipeline(op_name, &e))?;

        let task_ctx = ctx.task_ctx();
        datafusion::physical_plan::collect(physical, task_ctx)
            .await
            .map_err(|e| DbError::query_pipeline(op_name, &e))
    }

    /// Execute a cached `LogicalPlan` through `DataFusion` physical planning.
    ///
    /// `MemTables` are already registered in the `SessionContext` by the graph,
    /// so only physical planning and execution runs per cycle.
    ///
    /// If physical planning fails with a schema error (source schema changed),
    /// re-plans from SQL and updates the cached plan.
    async fn execute_cached_plan_with_invalidation(
        &mut self,
        plan: &LogicalPlan,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let plan_result = self.ctx.state().create_physical_plan(plan).await;

        match plan_result {
            Ok(physical) => {
                let task_ctx = self.ctx.task_ctx();
                datafusion::physical_plan::collect(physical, task_ctx)
                    .await
                    .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))
            }
            Err(e) => {
                // Schema error: re-plan from SQL and update cached plan
                let err_str = e.to_string();
                if err_str.contains("Schema error")
                    || err_str.contains("schema mismatch")
                    || err_str.contains("table") && err_str.contains("not found")
                {
                    tracing::debug!(
                        query = %self.op_name,
                        error = %e,
                        "Cached plan invalidated (schema change), re-planning from SQL"
                    );
                    let df = self
                        .ctx
                        .sql(&self.sql)
                        .await
                        .map_err(|e2| DbError::query_pipeline(&*self.op_name, &e2))?;
                    let new_plan = df.logical_plan().clone();
                    self.state = QueryState::CachedPlan(Box::new(new_plan));
                    df.collect()
                        .await
                        .map_err(|e2| DbError::query_pipeline(&*self.op_name, &e2))
                } else {
                    Err(DbError::query_pipeline(&*self.op_name, &e))
                }
            }
        }
    }

    /// Apply a HAVING predicate via SQL when the compiled `PhysicalExpr` is not
    /// available.
    ///
    /// Registers emitted batches as a temporary `MemTable` and runs the HAVING
    /// SQL against it. Caches the `LogicalPlan` on first call.
    async fn apply_having_sql(
        &mut self,
        batches: &[RecordBatch],
        having_sql: &str,
    ) -> Result<Vec<RecordBatch>, DbError> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        let schema = batches[0].schema();
        let table_name = format!("__having_{}", self.op_name);

        let mem_table =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![batches.to_vec()])
                .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
        let _ = self.ctx.deregister_table(&table_name);
        self.ctx
            .register_table(&table_name, Arc::new(mem_table))
            .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;

        let result = if let Some(ref plan) = self.cached_having_plan {
            Self::execute_plan(&self.ctx, &self.op_name, plan).await
        } else {
            let col_list: Vec<String> = schema
                .fields()
                .iter()
                .map(|f| format!("\"{}\"", f.name()))
                .collect();
            let filter_sql = format!(
                "SELECT {} FROM \"{}\" WHERE {having_sql}",
                col_list.join(", "),
                table_name,
            );
            tracing::warn!(
                query = %self.op_name,
                "HAVING filter compiled to PhysicalExpr failed -- using cached SQL plan"
            );
            match self.ctx.sql(&filter_sql).await {
                Ok(df) => {
                    self.cached_having_plan = Some(df.logical_plan().clone());
                    df.collect()
                        .await
                        .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))
                }
                Err(e) => Err(DbError::query_pipeline(&*self.op_name, &e)),
            }
        };

        let _ = self.ctx.deregister_table(&table_name);
        result
    }
}

#[async_trait]
impl GraphOperator for SqlQueryOperator {
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        // Lazy initialization on first call.
        if matches!(self.state, QueryState::Uninit) {
            self.lazy_init().await?;
        }

        let input_batches = inputs.first().map_or(&[] as &[RecordBatch], Vec::as_slice);

        // Skip if no input data.
        if input_batches.is_empty() || input_batches.iter().all(|b| b.num_rows() == 0) {
            // Aggregate queries still emit running state even with no new input.
            if matches!(self.state, QueryState::Agg(_)) {
                let QueryState::Agg(ref mut agg_state) = self.state else {
                    unreachable!();
                };
                let having_filter = agg_state.having_filter().cloned();
                let having_sql = agg_state.having_sql().map(String::from);
                let mut batches = agg_state.emit()?;
                if let Some(ref filter) = having_filter {
                    batches = apply_compiled_having(&batches, filter)?;
                } else if let Some(ref having_sql) = having_sql {
                    batches = self.apply_having_sql(&batches, having_sql).await?;
                }
                return Ok(batches);
            }
            return Ok(Vec::new());
        }

        match &self.state {
            QueryState::Uninit => unreachable!("lazy_init already called"),
            QueryState::Agg(_) => self.execute_agg(input_batches).await,
            QueryState::Compiled(_, _) => {
                // Direct PhysicalExpr evaluation on input batches.
                // Use try_evaluate_compiled to distinguish errors from empty results.
                let QueryState::Compiled(ref proj, _) = self.state else {
                    unreachable!();
                };
                match try_evaluate_compiled(proj, input_batches) {
                    Ok(result) => Ok(result),
                    Err(e) => {
                        // Compiled evaluation failed (e.g., type mismatch).
                        // Fall back to CachedPlan permanently.
                        tracing::debug!(
                            query = %self.op_name,
                            error = %e,
                            "Compiled projection failed, falling back to cached plan"
                        );
                        let QueryState::Compiled(_, fallback) =
                            std::mem::replace(&mut self.state, QueryState::Uninit)
                        else {
                            unreachable!();
                        };
                        self.state = QueryState::CachedPlan(fallback);
                        let QueryState::CachedPlan(ref plan) = self.state else {
                            unreachable!();
                        };
                        let plan = plan.clone();
                        self.execute_cached_plan_with_invalidation(&plan).await
                    }
                }
            }
            QueryState::CachedPlan(_) => {
                let QueryState::CachedPlan(ref plan) = self.state else {
                    unreachable!();
                };
                let plan = plan.clone();
                self.execute_cached_plan_with_invalidation(&plan).await
            }
        }
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        // If we have a pending restore (Uninit, not yet processed), preserve it
        // so a restore->checkpoint cycle before first process() doesn't lose data.
        if matches!(self.state, QueryState::Uninit) {
            if let Some(ref cp) = self.pending_restore {
                let data = serde_json::to_vec(cp).map_err(|e| {
                    DbError::Pipeline(format!(
                        "checkpoint serialization of pending restore for '{}': {e}",
                        self.op_name
                    ))
                })?;
                return Ok(Some(OperatorCheckpoint { data }));
            }
            return Ok(None);
        }

        let QueryState::Agg(ref mut agg_state) = self.state else {
            // Non-aggregate queries are stateless.
            return Ok(None);
        };

        let cp = agg_state.checkpoint_groups()?;
        let data = serde_json::to_vec(&cp).map_err(|e| {
            DbError::Pipeline(format!(
                "checkpoint serialization for '{}': {e}",
                self.op_name
            ))
        })?;

        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let cp: AggStateCheckpoint = serde_json::from_slice(&checkpoint.data).map_err(|e| {
            DbError::Pipeline(format!(
                "checkpoint deserialization for '{}': {e}",
                self.op_name
            ))
        })?;

        match self.state {
            QueryState::Agg(ref mut agg_state) => {
                // Already initialized -- restore directly.
                agg_state.restore_groups(&cp)?;
            }
            QueryState::Uninit => {
                // Not yet initialized -- defer restoration until lazy_init.
                self.pending_restore = Some(cp);
            }
            QueryState::Compiled(_, _) | QueryState::CachedPlan(_) => {
                // Non-aggregate state received an aggregate checkpoint.
                // This can happen during schema evolution. Log and ignore.
                tracing::warn!(
                    query = %self.op_name,
                    "Ignoring aggregate checkpoint for non-aggregate query (schema evolution?)"
                );
            }
        }

        Ok(())
    }

    fn estimated_state_bytes(&self) -> usize {
        match &self.state {
            QueryState::Agg(ref agg_state) => agg_state.estimated_size_bytes(),
            _ => 0,
        }
    }
}
