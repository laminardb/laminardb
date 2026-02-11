//! Pipeline extraction from `DataFusion` logical plans.
//!
//! [`PipelineExtractor`] walks a [`LogicalPlan`] top-down, decomposing it into
//! compilable [`Pipeline`] segments separated by [`PipelineBreaker`]s (stateful
//! operators like aggregations, sorts, and joins).

use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion_expr::LogicalPlan;

use super::error::ExtractError;
use super::pipeline::{Pipeline, PipelineBreaker, PipelineId, PipelineStage};

/// The result of extracting pipelines from a logical plan.
#[derive(Debug)]
pub struct ExtractedPlan {
    /// Compilable pipeline segments.
    pub pipelines: Vec<Pipeline>,
    /// Breakers connecting pipelines: `(upstream_id, breaker, downstream_id)`.
    pub breakers: Vec<(PipelineId, PipelineBreaker, PipelineId)>,
    /// Pipeline IDs that read from sources (table scans).
    pub sources: Vec<PipelineId>,
    /// Pipeline IDs that write to sinks (terminal outputs).
    pub sinks: Vec<PipelineId>,
}

/// Mutable state accumulated during plan extraction.
struct ExtractionContext {
    pipelines: Vec<PipelineBuilder>,
    breakers: Vec<(PipelineId, PipelineBreaker, PipelineId)>,
    sources: Vec<PipelineId>,
    sinks: Vec<PipelineId>,
    next_id: u32,
}

/// In-progress pipeline being built during extraction.
struct PipelineBuilder {
    id: PipelineId,
    stages: Vec<PipelineStage>,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
}

impl ExtractionContext {
    fn new() -> Self {
        Self {
            pipelines: Vec::new(),
            breakers: Vec::new(),
            sources: Vec::new(),
            sinks: Vec::new(),
            next_id: 0,
        }
    }

    /// Creates a new pipeline with the given schemas and returns its ID.
    fn new_pipeline(&mut self, input_schema: SchemaRef, output_schema: SchemaRef) -> PipelineId {
        let id = PipelineId(self.next_id);
        self.next_id += 1;
        self.pipelines.push(PipelineBuilder {
            id,
            stages: Vec::new(),
            input_schema,
            output_schema,
        });
        id
    }

    /// Adds a stage to the pipeline with the given ID.
    fn add_stage(&mut self, pipeline_id: PipelineId, stage: PipelineStage) {
        let builder = self
            .pipelines
            .iter_mut()
            .find(|p| p.id == pipeline_id)
            .expect("pipeline not found");
        builder.stages.push(stage);
    }

    /// Updates the output schema of a pipeline (e.g., after a Projection stage).
    fn update_output_schema(&mut self, pipeline_id: PipelineId, schema: SchemaRef) {
        let builder = self
            .pipelines
            .iter_mut()
            .find(|p| p.id == pipeline_id)
            .expect("pipeline not found");
        builder.output_schema = schema;
    }

    /// Finalizes all pipeline builders into an [`ExtractedPlan`].
    fn finalize(self) -> ExtractedPlan {
        let pipelines = self
            .pipelines
            .into_iter()
            .map(|b| Pipeline {
                id: b.id,
                stages: b.stages,
                input_schema: b.input_schema,
                output_schema: b.output_schema,
            })
            .collect();

        ExtractedPlan {
            pipelines,
            breakers: self.breakers,
            sources: self.sources,
            sinks: self.sinks,
        }
    }
}

/// Extracts compilable pipelines from `DataFusion` logical plans.
pub struct PipelineExtractor;

impl PipelineExtractor {
    /// Extracts pipelines from a [`LogicalPlan`].
    ///
    /// Walks the plan top-down, collecting compilable stages (Filter, Projection)
    /// into pipelines and breaking at stateful operators (Aggregate, Sort, Join).
    ///
    /// # Errors
    ///
    /// Returns [`ExtractError`] if the plan contains unsupported nodes.
    pub fn extract(plan: &LogicalPlan) -> Result<ExtractedPlan, ExtractError> {
        let mut ctx = ExtractionContext::new();
        let terminal_id = extract_impl(plan, &mut ctx)?;

        // The terminal pipeline is a sink.
        ctx.sinks.push(terminal_id);

        Ok(ctx.finalize())
    }
}

/// Converts a `DFSchemaRef` (via the plan's `schema()` method) to an Arrow `SchemaRef`.
fn arrow_schema(plan: &LogicalPlan) -> SchemaRef {
    Arc::new(plan.schema().as_arrow().clone())
}

/// Recursively extracts pipelines from a logical plan node.
#[allow(clippy::too_many_lines)]
fn extract_impl(
    plan: &LogicalPlan,
    ctx: &mut ExtractionContext,
) -> Result<PipelineId, ExtractError> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let schema = Arc::new(scan.projected_schema.as_arrow().clone());
            let pipeline_id = ctx.new_pipeline(Arc::clone(&schema), schema);
            ctx.sources.push(pipeline_id);
            Ok(pipeline_id)
        }

        LogicalPlan::EmptyRelation(empty) => {
            let schema = Arc::new(empty.schema.as_arrow().clone());
            let pipeline_id = ctx.new_pipeline(Arc::clone(&schema), schema);
            ctx.sources.push(pipeline_id);
            Ok(pipeline_id)
        }

        LogicalPlan::Filter(filter) => {
            let child_id = extract_impl(&filter.input, ctx)?;
            ctx.add_stage(
                child_id,
                PipelineStage::Filter {
                    predicate: filter.predicate.clone(),
                },
            );
            // Filter does not change the schema.
            Ok(child_id)
        }

        LogicalPlan::Projection(proj) => {
            let child_id = extract_impl(&proj.input, ctx)?;

            let arrow_out = proj.schema.as_arrow();
            let expressions: Vec<(datafusion_expr::Expr, String)> = proj
                .expr
                .iter()
                .enumerate()
                .map(|(i, expr)| (expr.clone(), arrow_out.field(i).name().clone()))
                .collect();

            ctx.add_stage(child_id, PipelineStage::Project { expressions });
            ctx.update_output_schema(child_id, Arc::new(arrow_out.clone()));
            Ok(child_id)
        }

        LogicalPlan::Aggregate(agg) => {
            let upstream_id = extract_impl(&agg.input, ctx)?;

            // Add KeyExtract stage for group expressions to the upstream pipeline.
            if !agg.group_expr.is_empty() {
                ctx.add_stage(
                    upstream_id,
                    PipelineStage::KeyExtract {
                        key_exprs: agg.group_expr.clone(),
                    },
                );
            }

            // Create downstream pipeline with the aggregate's output schema.
            let output_schema = Arc::new(agg.schema.as_arrow().clone());
            let downstream_id =
                ctx.new_pipeline(Arc::clone(&output_schema), output_schema);

            ctx.breakers.push((
                upstream_id,
                PipelineBreaker::Aggregate {
                    group_exprs: agg.group_expr.clone(),
                    aggr_exprs: agg.aggr_expr.clone(),
                },
                downstream_id,
            ));

            Ok(downstream_id)
        }

        LogicalPlan::Sort(sort) => {
            let upstream_id = extract_impl(&sort.input, ctx)?;

            let output_schema = arrow_schema(plan);
            let downstream_id =
                ctx.new_pipeline(Arc::clone(&output_schema), output_schema);

            let order_exprs = sort.expr.iter().map(|se| se.expr.clone()).collect();
            ctx.breakers.push((
                upstream_id,
                PipelineBreaker::Sort { order_exprs },
                downstream_id,
            ));

            Ok(downstream_id)
        }

        LogicalPlan::Join(join) => {
            let left_id = extract_impl(&join.left, ctx)?;
            let _right_id = extract_impl(&join.right, ctx)?;

            let output_schema = Arc::new(join.schema.as_arrow().clone());
            let downstream_id =
                ctx.new_pipeline(Arc::clone(&output_schema), output_schema);

            let left_keys = join.on.iter().map(|(l, _)| l.clone()).collect();
            let right_keys = join.on.iter().map(|(_, r)| r.clone()).collect();

            ctx.breakers.push((
                left_id,
                PipelineBreaker::Join {
                    join_type: format!("{:?}", join.join_type),
                    left_keys,
                    right_keys,
                },
                downstream_id,
            ));

            Ok(downstream_id)
        }

        LogicalPlan::SubqueryAlias(alias) => extract_impl(&alias.input, ctx),

        LogicalPlan::Limit(limit) => {
            // Limit requires materialization — treat as a breaker.
            let upstream_id = extract_impl(&limit.input, ctx)?;
            let output_schema = arrow_schema(plan);
            let downstream_id =
                ctx.new_pipeline(Arc::clone(&output_schema), output_schema);

            ctx.breakers.push((
                upstream_id,
                PipelineBreaker::Sort {
                    order_exprs: vec![],
                },
                downstream_id,
            ));

            Ok(downstream_id)
        }

        LogicalPlan::Distinct(distinct) => {
            let input = match distinct {
                datafusion_expr::Distinct::All(input) => input.as_ref(),
                datafusion_expr::Distinct::On(d) => d.input.as_ref(),
            };
            let upstream_id = extract_impl(input, ctx)?;
            let output_schema = arrow_schema(plan);
            let downstream_id =
                ctx.new_pipeline(Arc::clone(&output_schema), output_schema);

            ctx.breakers.push((
                upstream_id,
                PipelineBreaker::Sort {
                    order_exprs: vec![],
                },
                downstream_id,
            ));

            Ok(downstream_id)
        }

        // Unknown nodes: try single-input passthrough.
        other => {
            let inputs = other.inputs();
            if inputs.len() == 1 {
                extract_impl(inputs[0], ctx)
            } else if inputs.is_empty() {
                // Unknown leaf — create a source pipeline with the plan's schema.
                let schema = arrow_schema(other);
                let pipeline_id = ctx.new_pipeline(Arc::clone(&schema), schema);
                ctx.sources.push(pipeline_id);
                Ok(pipeline_id)
            } else {
                Err(ExtractError::UnsupportedPlan(format!(
                    "multi-input plan node: {}",
                    other.display()
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::DFSchema;
    use datafusion_expr::{col, lit, LogicalPlanBuilder};

    /// Helper: creates a simple table scan plan.
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

    #[test]
    fn extract_simple_table_scan() {
        let plan = table_scan_plan(vec![("x", DataType::Int64)]);
        let extracted = PipelineExtractor::extract(&plan).unwrap();

        assert_eq!(extracted.pipelines.len(), 1);
        assert_eq!(extracted.sources.len(), 1);
        assert_eq!(extracted.sinks.len(), 1);
        assert_eq!(extracted.breakers.len(), 0);
        assert!(extracted.pipelines[0].stages.is_empty());
    }

    #[test]
    fn extract_filter_only() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);
        let plan = LogicalPlan::Filter(datafusion_expr::Filter::try_new(
            col("x").gt(lit(10_i64)),
            Arc::new(scan),
        ).unwrap());

        let extracted = PipelineExtractor::extract(&plan).unwrap();

        assert_eq!(extracted.pipelines.len(), 1);
        assert_eq!(extracted.pipelines[0].stages.len(), 1);
        assert!(matches!(
            &extracted.pipelines[0].stages[0],
            PipelineStage::Filter { .. }
        ));
        assert_eq!(extracted.breakers.len(), 0);
    }

    #[test]
    fn extract_projection_only() {
        let scan = table_scan_plan(vec![("x", DataType::Int64), ("y", DataType::Int64)]);

        let plan = LogicalPlanBuilder::from(scan)
            .project(vec![col("x"), col("y") + lit(1_i64)])
            .unwrap()
            .build()
            .unwrap();

        let extracted = PipelineExtractor::extract(&plan).unwrap();

        assert_eq!(extracted.pipelines.len(), 1);
        assert_eq!(extracted.pipelines[0].stages.len(), 1);
        assert!(matches!(
            &extracted.pipelines[0].stages[0],
            PipelineStage::Project { expressions } if expressions.len() == 2
        ));
    }

    #[test]
    fn extract_filter_then_project() {
        let scan = table_scan_plan(vec![("x", DataType::Int64), ("y", DataType::Int64)]);

        let plan = LogicalPlanBuilder::from(scan)
            .filter(col("x").gt(lit(0_i64)))
            .unwrap()
            .project(vec![col("x"), col("y") * lit(2_i64)])
            .unwrap()
            .build()
            .unwrap();

        let extracted = PipelineExtractor::extract(&plan).unwrap();

        assert_eq!(extracted.pipelines.len(), 1);
        assert_eq!(extracted.pipelines[0].stages.len(), 2);
        assert!(matches!(
            &extracted.pipelines[0].stages[0],
            PipelineStage::Filter { .. }
        ));
        assert!(matches!(
            &extracted.pipelines[0].stages[1],
            PipelineStage::Project { .. }
        ));
    }

    #[test]
    fn extract_aggregate_manual() {
        let scan = table_scan_plan(vec![("key", DataType::Int64), ("val", DataType::Int64)]);

        // Build aggregate manually. Schema must match group_expr.len() + aggr_expr.len().
        let agg_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
        ]));
        let df_schema = DFSchema::try_from(agg_schema.as_ref().clone()).unwrap();
        let agg = datafusion_expr::Aggregate::try_new_with_schema(
            Arc::new(scan),
            vec![col("key")],
            vec![],
            Arc::new(df_schema),
        )
        .unwrap();

        let plan = LogicalPlan::Aggregate(agg);
        let extracted = PipelineExtractor::extract(&plan).unwrap();

        // Should have 2 pipelines (upstream + downstream) and 1 breaker.
        assert_eq!(extracted.pipelines.len(), 2);
        assert_eq!(extracted.breakers.len(), 1);
        assert!(matches!(
            &extracted.breakers[0].1,
            PipelineBreaker::Aggregate { .. }
        ));
        // Upstream pipeline should have a KeyExtract stage.
        let upstream = &extracted.pipelines[0];
        assert!(upstream
            .stages
            .iter()
            .any(|s| matches!(s, PipelineStage::KeyExtract { .. })));
    }

    #[test]
    fn extract_sort_creates_breaker() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);

        let plan = LogicalPlanBuilder::from(scan)
            .sort(vec![col("x").sort(true, true)])
            .unwrap()
            .build()
            .unwrap();

        let extracted = PipelineExtractor::extract(&plan).unwrap();

        assert_eq!(extracted.pipelines.len(), 2);
        assert_eq!(extracted.breakers.len(), 1);
        assert!(matches!(
            &extracted.breakers[0].1,
            PipelineBreaker::Sort { order_exprs } if order_exprs.len() == 1
        ));
    }

    #[test]
    fn extract_subquery_alias_passthrough() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);

        let plan = LogicalPlanBuilder::from(scan)
            .alias("t")
            .unwrap()
            .filter(col("x").gt(lit(5_i64)))
            .unwrap()
            .build()
            .unwrap();

        let extracted = PipelineExtractor::extract(&plan).unwrap();

        // SubqueryAlias is transparent — should still be 1 pipeline.
        assert_eq!(extracted.pipelines.len(), 1);
        assert_eq!(extracted.pipelines[0].stages.len(), 1);
        assert!(matches!(
            &extracted.pipelines[0].stages[0],
            PipelineStage::Filter { .. }
        ));
    }

    #[test]
    fn extract_nested_filters() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);

        let plan = LogicalPlanBuilder::from(scan)
            .filter(col("x").gt(lit(0_i64)))
            .unwrap()
            .filter(col("x").lt(lit(100_i64)))
            .unwrap()
            .build()
            .unwrap();

        let extracted = PipelineExtractor::extract(&plan).unwrap();

        assert_eq!(extracted.pipelines.len(), 1);
        assert_eq!(extracted.pipelines[0].stages.len(), 2);
        assert!(extracted
            .pipelines[0]
            .stages
            .iter()
            .all(|s| matches!(s, PipelineStage::Filter { .. })));
    }

    #[test]
    fn extract_multi_pipeline_filter_agg_project() {
        let scan = table_scan_plan(vec![("key", DataType::Int64), ("val", DataType::Int64)]);

        // Build: Filter → Aggregate → Project (manually for Aggregate)
        let filtered = LogicalPlanBuilder::from(scan)
            .filter(col("val").gt(lit(0_i64)))
            .unwrap()
            .build()
            .unwrap();

        let agg_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
        ]));
        let df_schema = DFSchema::try_from(agg_schema.as_ref().clone()).unwrap();
        let agg = datafusion_expr::Aggregate::try_new_with_schema(
            Arc::new(filtered),
            vec![col("key")],
            vec![],
            Arc::new(df_schema),
        )
        .unwrap();

        let plan = LogicalPlanBuilder::from(LogicalPlan::Aggregate(agg))
            .project(vec![col("key")])
            .unwrap()
            .build()
            .unwrap();

        let extracted = PipelineExtractor::extract(&plan).unwrap();

        // Pipeline 0: [Filter, KeyExtract] (upstream of aggregate)
        // Pipeline 1: [Project] (downstream of aggregate)
        assert_eq!(extracted.pipelines.len(), 2);
        assert_eq!(extracted.breakers.len(), 1);

        let upstream = &extracted.pipelines[0];
        assert!(upstream
            .stages
            .iter()
            .any(|s| matches!(s, PipelineStage::Filter { .. })));
        assert!(upstream
            .stages
            .iter()
            .any(|s| matches!(s, PipelineStage::KeyExtract { .. })));

        let downstream = &extracted.pipelines[1];
        assert!(downstream
            .stages
            .iter()
            .any(|s| matches!(s, PipelineStage::Project { .. })));
    }

    #[test]
    fn extract_empty_relation() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
        let plan = LogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(df_schema),
        });

        let extracted = PipelineExtractor::extract(&plan).unwrap();
        assert_eq!(extracted.pipelines.len(), 1);
        assert!(extracted.pipelines[0].stages.is_empty());
        assert_eq!(extracted.sources.len(), 1);
    }

    #[test]
    fn extract_preserves_pipeline_ids() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);

        let plan = LogicalPlanBuilder::from(scan)
            .sort(vec![col("x").sort(true, true)])
            .unwrap()
            .build()
            .unwrap();

        let extracted = PipelineExtractor::extract(&plan).unwrap();

        let (upstream_id, _, downstream_id) = &extracted.breakers[0];
        assert_eq!(*upstream_id, extracted.pipelines[0].id);
        assert_eq!(*downstream_id, extracted.pipelines[1].id);
    }

    #[test]
    fn extract_schema_tracking_through_project() {
        let scan = table_scan_plan(vec![
            ("a", DataType::Int64),
            ("b", DataType::Float64),
            ("c", DataType::Boolean),
        ]);

        // Project down to just 2 columns.
        let plan = LogicalPlanBuilder::from(scan)
            .project(vec![col("a"), col("b")])
            .unwrap()
            .build()
            .unwrap();

        let extracted = PipelineExtractor::extract(&plan).unwrap();
        let pipeline = &extracted.pipelines[0];

        // Input schema has 3 fields, output has 2.
        assert_eq!(pipeline.input_schema.fields().len(), 3);
        assert_eq!(pipeline.output_schema.fields().len(), 2);
    }

    #[test]
    fn extract_limit_creates_breaker() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);

        let plan = LogicalPlanBuilder::from(scan)
            .limit(0, Some(10))
            .unwrap()
            .build()
            .unwrap();

        let extracted = PipelineExtractor::extract(&plan).unwrap();

        assert_eq!(extracted.pipelines.len(), 2);
        assert_eq!(extracted.breakers.len(), 1);
    }

}
