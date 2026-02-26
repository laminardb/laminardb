//! Streaming scan execution plan for `DataFusion`
//!
//! This module provides `StreamingScanExec`, a `DataFusion` execution plan
//! that reads from a `StreamSource`. It serves as the leaf node in query
//! plans for streaming data.

use std::any::Any;
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
use super::watermark_filter::{WatermarkDynamicFilter, WatermarkFilterStream};

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
    properties: PlanProperties,
    /// Optional watermark filter applied at scan level (F-SSQL-006)
    watermark_filter: Option<Arc<WatermarkDynamicFilter>>,
}

impl StreamingScanExec {
    /// Creates a new streaming scan execution plan.
    ///
    /// # Arguments
    ///
    /// * `source` - The streaming source to read from
    /// * `projection` - Optional column projection indices
    /// * `filters` - Filters to push down to the source
    ///
    /// # Returns
    ///
    /// A new `StreamingScanExec` instance.
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

        // Build equivalence properties, optionally with source ordering
        let eq_properties = Self::build_equivalence_properties(&schema, source_ordering.as_deref());

        // Build plan properties for an unbounded streaming source.
        // SchedulingType::NonCooperative causes DataFusion's EnsureCooperative
        // optimizer rule to auto-wrap this leaf with CooperativeExec, which
        // yields to the Tokio executor periodically (F-SSQL-005).
        let properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1), // Single partition for streaming
            EmissionType::Incremental,            // Streaming emits incrementally
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            }, // Streaming is unbounded
        )
        .with_scheduling_type(SchedulingType::NonCooperative);

        Self {
            source,
            schema,
            projection,
            filters,
            properties,
            watermark_filter: None,
        }
    }

    /// Attaches a watermark filter that drops late rows at scan time.
    ///
    /// When set, `execute()` wraps the inner stream with a
    /// [`WatermarkFilterStream`] that applies `ts >= watermark` before
    /// any downstream processing.
    #[must_use]
    pub fn with_watermark_filter(mut self, filter: Arc<WatermarkDynamicFilter>) -> Self {
        self.watermark_filter = Some(filter);
        self
    }

    /// Returns the watermark filter, if set.
    #[must_use]
    pub fn watermark_filter(&self) -> Option<&Arc<WatermarkDynamicFilter>> {
        self.watermark_filter.as_ref()
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
            .field("watermark_filter", &self.watermark_filter)
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
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

        let stream = self
            .source
            .stream(self.projection.clone(), self.filters.clone())?;

        match &self.watermark_filter {
            Some(filter) => {
                let schema = stream.schema();
                Ok(Box::pin(WatermarkFilterStream::new(
                    stream,
                    Arc::clone(filter),
                    schema,
                )))
            }
            None => Ok(stream),
        }
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
mod tests {
    use super::*;
    use crate::datafusion::source::StreamSource;
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;

    #[derive(Debug)]
    struct MockSource {
        schema: SchemaRef,
        ordering: Option<Vec<SortColumn>>,
    }

    impl MockSource {
        fn new(schema: SchemaRef) -> Self {
            Self {
                schema,
                ordering: None,
            }
        }

        fn with_ordering(mut self, ordering: Vec<SortColumn>) -> Self {
            self.ordering = Some(ordering);
            self
        }
    }

    #[async_trait]
    impl StreamSource for MockSource {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn stream(
            &self,
            _projection: Option<Vec<usize>>,
            _filters: Vec<Expr>,
        ) -> Result<SendableRecordBatchStream, DataFusionError> {
            Err(DataFusionError::NotImplemented("mock".to_string()))
        }

        fn output_ordering(&self) -> Option<Vec<SortColumn>> {
            self.ordering.clone()
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    #[test]
    fn test_scan_exec_schema() {
        let schema = test_schema();
        let source: StreamSourceRef = Arc::new(MockSource::new(Arc::clone(&schema)));
        let exec = StreamingScanExec::new(source, None, vec![]);

        assert_eq!(exec.schema(), schema);
    }

    #[test]
    fn test_scan_exec_projection() {
        let schema = test_schema();
        let source: StreamSourceRef = Arc::new(MockSource::new(Arc::clone(&schema)));
        let exec = StreamingScanExec::new(source, Some(vec![0, 2]), vec![]);

        let output_schema = exec.schema();
        assert_eq!(output_schema.fields().len(), 2);
        assert_eq!(output_schema.field(0).name(), "id");
        assert_eq!(output_schema.field(1).name(), "value");
    }

    #[test]
    fn test_scan_exec_properties() {
        use datafusion::physical_plan::ExecutionPlanProperties;

        let schema = test_schema();
        let source: StreamSourceRef = Arc::new(MockSource::new(schema));
        let exec = StreamingScanExec::new(source, None, vec![]);

        // Should be unbounded (streaming)
        assert!(matches!(exec.boundedness(), Boundedness::Unbounded { .. }));

        // Should be single partition
        let partitioning = exec.properties().output_partitioning();
        assert!(matches!(partitioning, Partitioning::UnknownPartitioning(1)));

        // Leaf node has no children
        assert!(exec.children().is_empty());
    }

    #[test]
    fn test_scan_exec_display() {
        let schema = test_schema();
        let source: StreamSourceRef = Arc::new(MockSource::new(schema));
        let exec = StreamingScanExec::new(source, Some(vec![0, 1]), vec![]);

        // Verify it implements DisplayAs by checking the name
        assert_eq!(exec.name(), "StreamingScanExec");
        // Debug format should contain the struct info
        let debug = format!("{exec:?}");
        assert!(debug.contains("StreamingScanExec"));
    }

    #[test]
    fn test_scan_exec_name() {
        let schema = test_schema();
        let source: StreamSourceRef = Arc::new(MockSource::new(schema));
        let exec = StreamingScanExec::new(source, None, vec![]);

        assert_eq!(exec.name(), "StreamingScanExec");
    }

    // --- Tier 1 ordering tests ---

    #[test]
    fn test_scan_exec_no_ordering() {
        use datafusion::physical_plan::ExecutionPlanProperties;

        let schema = test_schema();
        let source: StreamSourceRef = Arc::new(MockSource::new(schema));
        let exec = StreamingScanExec::new(source, None, vec![]);

        // No ordering declared -> output_ordering returns None
        assert!(exec.output_ordering().is_none());
    }

    #[test]
    fn test_scan_exec_with_ordering() {
        use datafusion::physical_plan::ExecutionPlanProperties;

        let schema = test_schema();
        let source: StreamSourceRef = Arc::new(
            MockSource::new(Arc::clone(&schema)).with_ordering(vec![SortColumn::ascending("id")]),
        );
        let exec = StreamingScanExec::new(source, None, vec![]);

        // Source ordering declared -> output_ordering returns Some
        let ordering = exec.output_ordering();
        assert!(ordering.is_some());
        let lex = ordering.unwrap();
        assert_eq!(lex.len(), 1);
    }

    #[test]
    fn test_scan_exec_output_ordering_returns_some() {
        use datafusion::physical_plan::ExecutionPlanProperties;

        let schema = test_schema();
        let source: StreamSourceRef =
            Arc::new(MockSource::new(Arc::clone(&schema)).with_ordering(vec![
                SortColumn::ascending("id"),
                SortColumn::descending("value"),
            ]));
        let exec = StreamingScanExec::new(source, None, vec![]);

        let ordering = exec.output_ordering().unwrap();
        assert_eq!(ordering.len(), 2);
    }

    #[test]
    fn test_scan_exec_ordering_with_projection() {
        use datafusion::physical_plan::ExecutionPlanProperties;

        let schema = test_schema();
        // Source ordered by "id" ascending
        let source: StreamSourceRef = Arc::new(
            MockSource::new(Arc::clone(&schema)).with_ordering(vec![SortColumn::ascending("id")]),
        );
        // Project only "id" and "value" (indices 0, 2)
        let exec = StreamingScanExec::new(source, Some(vec![0, 2]), vec![]);

        // "id" is in the projection -> ordering should still be present
        let ordering = exec.output_ordering();
        assert!(ordering.is_some());
    }

    #[test]
    fn test_scan_exec_ordering_column_not_in_projection() {
        use datafusion::physical_plan::ExecutionPlanProperties;

        let schema = test_schema();
        // Source ordered by "name" ascending
        let source: StreamSourceRef = Arc::new(
            MockSource::new(Arc::clone(&schema)).with_ordering(vec![SortColumn::ascending("name")]),
        );
        // Project only "id" and "value" (indices 0, 2) -- "name" is NOT projected
        let exec = StreamingScanExec::new(source, Some(vec![0, 2]), vec![]);

        // "name" is not in the projection -> ordering should be None
        assert!(exec.output_ordering().is_none());
    }

    // --- Cooperative scheduling tests (F-SSQL-005) ---

    #[test]
    fn test_streaming_scan_exec_scheduling_type() {
        let schema = test_schema();
        let source: StreamSourceRef = Arc::new(MockSource::new(schema));
        let exec = StreamingScanExec::new(source, None, vec![]);

        // StreamingScanExec declares NonCooperative so that DataFusion's
        // EnsureCooperative optimizer auto-wraps it with CooperativeExec.
        assert_eq!(
            exec.properties().scheduling_type,
            SchedulingType::NonCooperative,
        );
    }

    #[tokio::test]
    async fn test_cooperative_exec_wraps_streaming_scan() {
        use crate::datafusion::{
            create_streaming_context, ChannelStreamSource, StreamingTableProvider,
        };
        use arrow_schema::{DataType, Field, Schema};

        let ctx = create_streaming_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let source = Arc::new(ChannelStreamSource::new(Arc::clone(&schema)));
        let _sender = source.take_sender();
        let provider = StreamingTableProvider::new("events", source);
        ctx.register_table("events", Arc::new(provider)).unwrap();

        // Create a physical plan and verify CooperativeExec wrapping
        let df = ctx.sql("SELECT id FROM events").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let plan_str = format!("{}", datafusion::physical_plan::displayable(plan.as_ref()).indent(true));
        assert!(
            plan_str.contains("CooperativeExec"),
            "Expected CooperativeExec wrapper around StreamingScanExec, got:\n{plan_str}"
        );
    }

    // --- Watermark filter tests (F-SSQL-006) ---

    #[test]
    fn test_streaming_scan_with_watermark_filter() {
        use std::sync::atomic::{AtomicI64, AtomicU64};
        use super::WatermarkDynamicFilter;

        let schema = test_schema();
        let source: StreamSourceRef = Arc::new(MockSource::new(schema));
        let filter = Arc::new(WatermarkDynamicFilter::new(
            Arc::new(AtomicI64::new(100)),
            Arc::new(AtomicU64::new(0)),
            "id".to_string(),
        ));

        let exec = StreamingScanExec::new(source, None, vec![])
            .with_watermark_filter(Arc::clone(&filter));

        assert!(exec.watermark_filter().is_some());
        assert_eq!(exec.watermark_filter().unwrap().watermark_ms(), 100);
    }

    #[test]
    fn test_streaming_scan_watermark_filter_preserved() {
        use std::sync::atomic::{AtomicI64, AtomicU64};
        use super::WatermarkDynamicFilter;

        let schema = test_schema();
        let source: StreamSourceRef = Arc::new(MockSource::new(schema));
        let filter = Arc::new(WatermarkDynamicFilter::new(
            Arc::new(AtomicI64::new(200)),
            Arc::new(AtomicU64::new(0)),
            "id".to_string(),
        ));

        let exec = StreamingScanExec::new(source, None, vec![])
            .with_watermark_filter(Arc::clone(&filter));

        // with_new_children(empty) should preserve the watermark filter
        let exec_arc: Arc<dyn ExecutionPlan> = Arc::new(exec);
        let rebuilt = exec_arc.with_new_children(vec![]).unwrap();
        let rebuilt_scan = rebuilt
            .as_any()
            .downcast_ref::<StreamingScanExec>()
            .unwrap();
        assert!(rebuilt_scan.watermark_filter().is_some());
        assert_eq!(rebuilt_scan.watermark_filter().unwrap().watermark_ms(), 200);
    }
}
