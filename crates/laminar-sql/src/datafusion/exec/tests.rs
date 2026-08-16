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

// Cooperative scheduling tests

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
    let plan_str = format!(
        "{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
    );
    assert!(
        plan_str.contains("CooperativeExec"),
        "Expected CooperativeExec wrapper around StreamingScanExec, got:\n{plan_str}"
    );
}
