//! Streaming scan execution plan for `DataFusion`
//!
//! This module provides `StreamingScanExec`, a `DataFusion` execution plan
//! that reads from a `StreamSource`. It serves as the leaf node in query
//! plans for streaming data.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_schema::{SchemaRef, SortOptions};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{
    expressions::Column, EquivalenceProperties, LexOrdering, Partitioning, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::DataFusionError;
use datafusion_expr::Expr;

use super::source::{SortColumn, StreamSourceRef};

/// A `DataFusion` execution plan that scans from a streaming source.
///
/// This is a leaf node in the query plan tree that pulls data from
/// a `StreamSource` implementation. It handles projection and filter
/// pushdown to the source when supported.
///
/// # Properties
///
/// - Single partition (streaming sources are typically not partitioned)
/// - Unbounded execution mode (streaming)
/// - No inherent ordering (unless specified by source)
pub struct StreamingScanExec {
    /// The streaming source to read from
    source: StreamSourceRef,
    /// Output schema (after projection)
    schema: SchemaRef,
    /// Column projection (None = all columns)
    projection: Option<Vec<usize>>,
    /// Filters pushed down to source
    filters: Vec<Expr>,
    /// Cached plan properties
    properties: Arc<PlanProperties>,
}

impl StreamingScanExec {
    /// Creates a new streaming scan execution plan.
    ///
    /// If the source declares an `output_ordering`, the plan's
    /// `EquivalenceProperties` will include it so `DataFusion` can elide
    /// `SortExec` for matching ORDER BY queries.
    pub fn new(
        source: StreamSourceRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
    ) -> Self {
        let source_schema = source.schema();
        let source_ordering = source.output_ordering();

        let schema = match &projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| source_schema.field(i).clone())
                    .collect();
                Arc::new(arrow_schema::Schema::new(fields))
            }
            None => source_schema,
        };

        let eq_properties = Self::build_equivalence_properties(&schema, source_ordering.as_deref());

        // SchedulingType::NonCooperative causes DataFusion's EnsureCooperative
        // optimizer rule to auto-wrap this leaf with CooperativeExec, which
        // yields to the Tokio executor periodically.
        let properties = Arc::new(
            PlanProperties::new(
                eq_properties,
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Unbounded {
                    requires_infinite_memory: false,
                },
            )
            .with_scheduling_type(SchedulingType::NonCooperative),
        );

        Self {
            source,
            schema,
            projection,
            filters,
            properties,
        }
    }

    /// Builds `EquivalenceProperties` with optional source ordering.
    ///
    /// Converts `SortColumn` declarations into `DataFusion` `PhysicalSortExpr`
    /// entries. Only columns present in the output schema are included.
    fn build_equivalence_properties(
        schema: &SchemaRef,
        ordering: Option<&[SortColumn]>,
    ) -> EquivalenceProperties {
        let mut eq = EquivalenceProperties::new(Arc::clone(schema));

        if let Some(sort_columns) = ordering {
            let sort_exprs: Vec<PhysicalSortExpr> = sort_columns
                .iter()
                .filter_map(|sc| {
                    // Find column index in the output schema
                    schema.index_of(&sc.name).ok().map(|idx| {
                        PhysicalSortExpr::new(
                            Arc::new(Column::new(&sc.name, idx)),
                            SortOptions {
                                descending: sc.descending,
                                nulls_first: sc.nulls_first,
                            },
                        )
                    })
                })
                .collect();

            if !sort_exprs.is_empty() {
                eq.add_ordering(sort_exprs);
            }
        }

        eq
    }

    /// Returns the streaming source.
    #[must_use]
    pub fn source(&self) -> &StreamSourceRef {
        &self.source
    }

    /// Returns the column projection.
    #[must_use]
    pub fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }

    /// Returns the pushed-down filters.
    #[must_use]
    pub fn filters(&self) -> &[Expr] {
        &self.filters
    }
}

impl Debug for StreamingScanExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingScanExec")
            .field("source", &self.source)
            .field("schema", &self.schema)
            .field("projection", &self.projection)
            .field("filters", &self.filters)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for StreamingScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "StreamingScanExec: ")?;
                if let Some(proj) = &self.projection {
                    write!(f, "projection=[{proj:?}]")?;
                } else {
                    write!(f, "projection=[*]")?;
                }
                if !self.filters.is_empty() {
                    write!(f, ", filters={:?}", self.filters)?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => {
                write!(f, "StreamingScanExec")
            }
        }
    }
}

impl ExecutionPlan for StreamingScanExec {
    fn name(&self) -> &'static str {
        "StreamingScanExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Leaf node - no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.is_empty() {
            // No changes needed for leaf node
            Ok(self)
        } else {
            Err(DataFusionError::Plan(
                "StreamingScanExec cannot have children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Plan(format!(
                "StreamingScanExec only supports partition 0, got {partition}"
            )));
        }

        self.source
            .stream(self.projection.clone(), self.filters.clone())
    }
}

// Required for `DataFusion` to use this execution plan
impl datafusion::physical_plan::ExecutionPlanProperties for StreamingScanExec {
    fn output_partitioning(&self) -> &Partitioning {
        self.properties.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        self.properties.output_ordering()
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Unbounded {
            requires_infinite_memory: false,
        }
    }

    fn pipeline_behavior(&self) -> EmissionType {
        EmissionType::Incremental
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        self.properties.equivalence_properties()
    }
}

#[cfg(test)]
mod tests;
