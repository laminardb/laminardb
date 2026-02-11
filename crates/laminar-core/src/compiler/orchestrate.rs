//! SQL compiler orchestrator — single entry point for `LogicalPlan` → [`StreamingQuery`].
//!
//! [`compile_streaming_query`] coordinates the full compilation pipeline:
//! 1. Extract pipelines from a `DataFusion` [`LogicalPlan`]
//! 2. Detect breakers (stateful operators) → return `None` for plans that can't be fully compiled
//! 3. Compile each pipeline segment via [`ExecutablePipeline::try_compile`]
//! 4. Wire each compiled pipeline to a [`super::PipelineBridge`] + [`super::BridgeConsumer`]
//! 5. Assemble into a [`StreamingQuery`] ready for execution
//!
//! # Usage
//!
//! ```ignore
//! use laminar_core::compiler::orchestrate::compile_streaming_query;
//! use laminar_core::compiler::{CompilerCache, QueryConfig};
//!
//! let mut cache = CompilerCache::new(64)?;
//! let config = QueryConfig::default();
//! match compile_streaming_query(sql, &logical_plan, &mut cache, &config)? {
//!     Some(compiled) => { /* use compiled.query, compiled.source_plan, etc. */ }
//!     None => { /* fall back to DataFusion interpreted execution */ }
//! }
//! ```

use std::sync::Arc;
use std::time::Instant;

use datafusion_expr::LogicalPlan;

use super::cache::CompilerCache;
use super::error::CompileError;
use super::extractor::PipelineExtractor;
use super::fallback::ExecutablePipeline;
use super::metrics::{QueryConfig, QueryMetadata};
use super::pipeline_bridge::create_pipeline_bridge;
use super::query::{StreamingQuery, StreamingQueryBuilder};
use super::row::RowSchema;

/// Result of successful compilation — ready for execution.
pub struct CompiledStreamingQuery {
    /// The compiled [`StreamingQuery`], in `Ready` state. Call `.start()` before use.
    pub query: StreamingQuery,
    /// [`LogicalPlan`] containing only the source scan (TableScan/EmptyRelation).
    /// The caller executes this via `DataFusion` to get the input record batch stream.
    pub source_plan: LogicalPlan,
    /// Input [`RowSchema`] for `RecordBatch` → `EventRow` conversion.
    pub input_schema: Arc<RowSchema>,
    /// Output [`RowSchema`] (may differ from input if projection changes columns).
    pub output_schema: Arc<RowSchema>,
    /// Compilation metadata for diagnostics.
    pub metadata: QueryMetadata,
}

impl std::fmt::Debug for CompiledStreamingQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledStreamingQuery")
            .field("query", &self.query)
            .field("input_fields", &self.input_schema.field_count())
            .field("output_fields", &self.output_schema.field_count())
            .field("metadata", &self.metadata)
            .finish_non_exhaustive()
    }
}

/// Attempts to compile a `DataFusion` [`LogicalPlan`] into a [`StreamingQuery`].
///
/// Returns `Ok(None)` when the plan contains stateful operators (aggregations,
/// sorts, joins) that cannot be compiled — the caller should fall back to
/// `DataFusion` interpreted execution.
///
/// Returns `Ok(Some(compiled))` with a ready-to-start [`StreamingQuery`], the
/// source scan plan for `DataFusion` to execute, and the input/output schemas
/// for `RecordBatch` ↔ `EventRow` conversion.
///
/// # Errors
///
/// Returns [`CompileError`] only on hard failures (e.g., Cranelift context
/// initialization failure, or the plan has no source scan). Soft failures
/// (unsupported expressions) result in fallback pipelines within the query.
pub fn compile_streaming_query(
    sql: &str,
    plan: &LogicalPlan,
    cache: &mut CompilerCache,
    config: &QueryConfig,
) -> Result<Option<CompiledStreamingQuery>, CompileError> {
    // 1. Extract pipelines from the logical plan.
    let extract_start = Instant::now();
    let Ok(extracted) = PipelineExtractor::extract(plan) else {
        return Ok(None);
    };
    let extract_time = extract_start.elapsed();

    // 2. If there are breakers (stateful operators), return None — can't fully compile.
    if !extracted.breakers.is_empty() {
        return Ok(None);
    }

    // 3. Extract the source scan from the plan.
    let Some(source_plan) = extract_table_scan(plan) else {
        return Ok(None);
    };

    // 4. Build input schema from the source plan's output.
    let source_arrow_schema = Arc::new(source_plan.schema().as_arrow().clone());
    let input_schema = Arc::new(
        RowSchema::from_arrow(&source_arrow_schema)
            .map_err(|e| CompileError::UnsupportedExpr(e.to_string()))?,
    );

    // 5. Compile each pipeline and wire bridges.
    let compile_start = Instant::now();
    let mut builder = StreamingQueryBuilder::new(sql);
    let mut compiled_count = 0_usize;
    let mut fallback_count = 0_usize;
    let mut final_output_schema = Arc::clone(&input_schema);

    for pipeline in &extracted.pipelines {
        let exec = ExecutablePipeline::try_compile(cache, pipeline);

        if exec.is_compiled() {
            compiled_count += 1;
        } else {
            fallback_count += 1;
        }

        // Determine the output schema for this pipeline's bridge.
        let pipeline_output_arrow = Arc::clone(&pipeline.output_schema);
        let pipeline_output_row = Arc::new(
            RowSchema::from_arrow(&pipeline_output_arrow)
                .map_err(|e| CompileError::UnsupportedExpr(e.to_string()))?,
        );

        let (bridge, consumer) = create_pipeline_bridge(
            Arc::clone(&pipeline_output_row),
            config.queue_capacity,
            config.output_buffer_size,
            config.batch_policy.clone(),
            config.backpressure.clone(),
        )
        .map_err(|e| CompileError::UnsupportedExpr(e.to_string()))?;

        builder = builder.add_pipeline(exec, bridge, consumer, Arc::clone(&pipeline_output_row));
        final_output_schema = pipeline_output_row;
    }
    let compile_time = compile_start.elapsed();

    // 6. Build metadata.
    let metadata = QueryMetadata {
        extract_time,
        compile_time,
        compiled_pipeline_count: compiled_count,
        fallback_pipeline_count: fallback_count,
        jit_enabled: true,
        ..Default::default()
    };

    // 7. Build the StreamingQuery.
    let query = builder
        .with_metadata(metadata.clone())
        .build()
        .map_err(|e| CompileError::UnsupportedExpr(e.to_string()))?;

    Ok(Some(CompiledStreamingQuery {
        query,
        source_plan,
        input_schema,
        output_schema: final_output_schema,
        metadata,
    }))
}

/// Recursively walks a [`LogicalPlan`] to extract the first source scan node.
///
/// Returns the scan as a standalone [`LogicalPlan`] suitable for execution
/// via `DataFusion`. Recognizes `TableScan` and `EmptyRelation` as sources.
fn extract_table_scan(plan: &LogicalPlan) -> Option<LogicalPlan> {
    match plan {
        LogicalPlan::TableScan(_) | LogicalPlan::EmptyRelation(_) => Some(plan.clone()),
        _ => plan.inputs().into_iter().find_map(extract_table_scan),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::DFSchema;
    use datafusion_expr::{col, lit, LogicalPlanBuilder};

    fn table_scan_plan(fields: Vec<(&str, DataType)>) -> LogicalPlan {
        let arrow_schema = Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dt)| Field::new(name, dt, true))
                .collect::<Vec<_>>(),
        ));
        let df_schema = DFSchema::try_from(arrow_schema.as_ref().clone()).unwrap();
        LogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(df_schema),
        })
    }

    fn new_cache() -> CompilerCache {
        CompilerCache::new(64).unwrap()
    }

    // ── Compilation tests ───────────────────────────────────────────

    #[test]
    fn compile_simple_filter() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);
        let plan = LogicalPlanBuilder::from(scan)
            .filter(col("x").gt(lit(10_i64)))
            .unwrap()
            .build()
            .unwrap();

        let mut cache = new_cache();
        let config = QueryConfig::default();
        let result =
            compile_streaming_query("SELECT x FROM t WHERE x > 10", &plan, &mut cache, &config);
        let compiled = result.unwrap().unwrap();

        assert_eq!(compiled.input_schema.field_count(), 1);
        assert_eq!(compiled.output_schema.field_count(), 1);
        assert!(
            compiled.metadata.compiled_pipeline_count > 0
                || compiled.metadata.fallback_pipeline_count > 0
        );
        assert!(compiled.metadata.jit_enabled);
    }

    #[test]
    fn compile_filter_and_project() {
        let scan = table_scan_plan(vec![("x", DataType::Int64), ("y", DataType::Int64)]);
        let plan = LogicalPlanBuilder::from(scan)
            .filter(col("x").gt(lit(0_i64)))
            .unwrap()
            .project(vec![col("x"), col("y") + lit(1_i64)])
            .unwrap()
            .build()
            .unwrap();

        let mut cache = new_cache();
        let config = QueryConfig::default();
        let result = compile_streaming_query(
            "SELECT x, y + 1 FROM t WHERE x > 0",
            &plan,
            &mut cache,
            &config,
        );
        let compiled = result.unwrap().unwrap();
        assert_eq!(compiled.input_schema.field_count(), 2);
        assert_eq!(compiled.output_schema.field_count(), 2);
    }

    #[test]
    fn compile_passthrough() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);
        let mut cache = new_cache();
        let config = QueryConfig::default();
        let result = compile_streaming_query("SELECT x FROM t", &scan, &mut cache, &config);
        let compiled = result.unwrap().unwrap();
        assert_eq!(compiled.query.pipeline_count(), 1);
    }

    // ── Breaker detection tests ─────────────────────────────────────

    #[test]
    fn aggregate_returns_none() {
        let scan = table_scan_plan(vec![("key", DataType::Int64), ("val", DataType::Int64)]);
        let agg_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, true)]));
        let df_schema = DFSchema::try_from(agg_schema.as_ref().clone()).unwrap();
        let agg = datafusion_expr::Aggregate::try_new_with_schema(
            Arc::new(scan),
            vec![col("key")],
            vec![],
            Arc::new(df_schema),
        )
        .unwrap();
        let plan = LogicalPlan::Aggregate(agg);

        let mut cache = new_cache();
        let config = QueryConfig::default();
        let result =
            compile_streaming_query("SELECT key FROM t GROUP BY key", &plan, &mut cache, &config);
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn sort_returns_none() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);
        let plan = LogicalPlanBuilder::from(scan)
            .sort(vec![col("x").sort(true, true)])
            .unwrap()
            .build()
            .unwrap();

        let mut cache = new_cache();
        let config = QueryConfig::default();
        let result =
            compile_streaming_query("SELECT x FROM t ORDER BY x", &plan, &mut cache, &config);
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn limit_returns_none() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);
        let plan = LogicalPlanBuilder::from(scan)
            .limit(0, Some(10))
            .unwrap()
            .build()
            .unwrap();

        let mut cache = new_cache();
        let config = QueryConfig::default();
        let result =
            compile_streaming_query("SELECT x FROM t LIMIT 10", &plan, &mut cache, &config);
        assert!(result.unwrap().is_none());
    }

    // ── Source extraction tests ──────────────────────────────────────

    #[test]
    fn extract_scan_from_simple_plan() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);
        let found = extract_table_scan(&scan);
        assert!(found.is_some());
    }

    #[test]
    fn extract_scan_from_nested_plan() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);
        let plan = LogicalPlanBuilder::from(scan)
            .filter(col("x").gt(lit(0_i64)))
            .unwrap()
            .build()
            .unwrap();
        let found = extract_table_scan(&plan);
        assert!(found.is_some());
        assert!(matches!(found.unwrap(), LogicalPlan::EmptyRelation(_)));
    }

    // ── Cache interaction tests ─────────────────────────────────────

    #[test]
    fn repeat_compilation_uses_cache() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);
        let plan = LogicalPlanBuilder::from(scan)
            .filter(col("x").gt(lit(10_i64)))
            .unwrap()
            .build()
            .unwrap();

        let mut cache = new_cache();
        let config = QueryConfig::default();

        // First compilation.
        let _ = compile_streaming_query("SELECT x FROM t WHERE x > 10", &plan, &mut cache, &config)
            .unwrap();
        let cache_after_first = cache.len();

        // Second compilation — should use cached pipeline.
        let _ = compile_streaming_query("SELECT x FROM t WHERE x > 10", &plan, &mut cache, &config)
            .unwrap();
        assert_eq!(cache.len(), cache_after_first);
    }

    #[test]
    fn different_query_different_cache_entry() {
        let scan1 = table_scan_plan(vec![("x", DataType::Int64)]);
        let plan1 = LogicalPlanBuilder::from(scan1)
            .filter(col("x").gt(lit(10_i64)))
            .unwrap()
            .build()
            .unwrap();

        let scan2 = table_scan_plan(vec![("x", DataType::Int64)]);
        let plan2 = LogicalPlanBuilder::from(scan2)
            .filter(col("x").lt(lit(100_i64)))
            .unwrap()
            .build()
            .unwrap();

        let mut cache = new_cache();
        let config = QueryConfig::default();

        let _ = compile_streaming_query("q1", &plan1, &mut cache, &config).unwrap();
        let _ = compile_streaming_query("q2", &plan2, &mut cache, &config).unwrap();
        assert!(cache.len() >= 2);
    }

    // ── Integration tests ───────────────────────────────────────────

    #[test]
    fn metadata_populated() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);
        let plan = LogicalPlanBuilder::from(scan)
            .filter(col("x").gt(lit(5_i64)))
            .unwrap()
            .build()
            .unwrap();

        let mut cache = new_cache();
        let config = QueryConfig::default();
        let compiled = compile_streaming_query("SELECT x WHERE x > 5", &plan, &mut cache, &config)
            .unwrap()
            .unwrap();

        assert!(compiled.metadata.jit_enabled);
        assert!(compiled.metadata.total_pipelines() > 0);
        assert!(!compiled.metadata.extract_time.is_zero());
    }

    #[test]
    fn compiled_query_starts() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);
        let plan = LogicalPlanBuilder::from(scan)
            .filter(col("x").gt(lit(0_i64)))
            .unwrap()
            .build()
            .unwrap();

        let mut cache = new_cache();
        let config = QueryConfig::default();
        let mut compiled =
            compile_streaming_query("SELECT x WHERE x > 0", &plan, &mut cache, &config)
                .unwrap()
                .unwrap();

        assert!(compiled.query.start().is_ok());
        assert!(compiled.query.stop().is_ok());
    }
}
