//! Swappable table provider that eliminates per-cycle catalog churn and
//! enables physical plan caching.
//!
//! Register a [`LiveSourceProvider`] once at pipeline startup. Each cycle,
//! swap batches via [`LiveSourceHandle`], then execute the cached physical
//! plan. The internal `LiveSourceExec` reads from the shared slot at `execute()` time,
//! so the cached plan always sees fresh data.

use std::any::Any;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::Statistics;
use datafusion_expr::TableType;
use parking_lot::Mutex;

/// Shared batch slot used by the provider and every `LiveSourceExec` scan.
///
/// Wrapping the batch list in `Arc` means the hot-path `execute()` clone is
/// an O(1) Arc bump, not a full `Vec<RecordBatch>` clone (which would
/// Arc-bump every column of every batch). `swap()` replaces the Arc
/// wholesale while any in-flight reader keeps its prior snapshot alive.
type BatchSlot = Arc<Mutex<Arc<Vec<RecordBatch>>>>;

fn new_slot() -> BatchSlot {
    Arc::new(Mutex::new(Arc::new(Vec::new())))
}

// ── TableProvider ────────────────────────────────────────────────────

/// Swappable `TableProvider` for streaming micro-batch execution.
///
/// `scan()` returns an internal execution plan that reads from the shared
/// batch slot at `execute()` time — enabling physical plan caching.
pub struct LiveSourceProvider {
    current: BatchSlot,
    schema: SchemaRef,
}

impl LiveSourceProvider {
    /// Creates a provider with the given schema and an empty batch slot.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            current: new_slot(),
            schema,
        }
    }

    /// Returns a handle for swapping batches into this provider.
    #[must_use]
    pub fn handle(&self) -> LiveSourceHandle {
        LiveSourceHandle {
            slot: Arc::clone(&self.current),
        }
    }
}

impl std::fmt::Debug for LiveSourceProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveSourceProvider")
            .field("schema_fields", &self.schema.fields().len())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for LiveSourceProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(LiveSourceExec::new(
            Arc::clone(&self.current),
            self.schema.clone(),
            projection.cloned(),
        )))
    }
}

// ── ExecutionPlan ────────────────────────────────────────────────────

/// Leaf `ExecutionPlan` that reads from a shared batch slot at `execute()`
/// time, not at construction time. This enables physical plan caching:
/// the plan tree is built once, and each `execute()` call sees fresh data.
pub(crate) struct LiveSourceExec {
    slot: BatchSlot,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
}

impl LiveSourceExec {
    fn new(slot: BatchSlot, source_schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        let schema = match &projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| source_schema.field(i).clone())
                    .collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => source_schema,
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            slot,
            schema,
            projection,
            properties,
        }
    }
}

impl std::fmt::Debug for LiveSourceExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveSourceExec")
            .field("schema_fields", &self.schema.fields().len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for LiveSourceExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LiveSourceExec: schema={}", self.schema.fields().len())
            }
            DisplayFormatType::TreeRender => write!(f, "LiveSourceExec"),
        }
    }
}

impl ExecutionPlan for LiveSourceExec {
    fn name(&self) -> &'static str {
        "LiveSourceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(
                "LiveSourceExec is a leaf node".to_string(),
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
                "LiveSourceExec only supports partition 0, got {partition}"
            )));
        }

        // O(1) snapshot: bump the Arc under the lock and release.
        // Any concurrent `swap()` installs a new Arc; this reader keeps
        // its prior snapshot alive until the returned stream is dropped.
        let batches_arc: Arc<Vec<RecordBatch>> = Arc::clone(&self.slot.lock());
        let schema = self.schema.clone();
        let projection = self.projection.clone();

        let output = futures::stream::iter(if batches_arc.is_empty() {
            vec![Ok(RecordBatch::new_empty(schema))]
        } else if let Some(indices) = projection {
            batches_arc
                .iter()
                .map(|batch| batch.project(&indices).map_err(DataFusionError::from))
                .collect()
        } else {
            batches_arc.iter().cloned().map(Ok).collect()
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            output,
        )))
    }

    fn statistics(&self) -> datafusion_common::Result<Statistics> {
        Ok(Statistics::default())
    }
}

impl datafusion::physical_plan::ExecutionPlanProperties for LiveSourceExec {
    fn output_partitioning(&self) -> &Partitioning {
        self.properties.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&datafusion::physical_expr::LexOrdering> {
        self.properties.output_ordering()
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Bounded
    }

    fn pipeline_behavior(&self) -> EmissionType {
        EmissionType::Final
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        self.properties.equivalence_properties()
    }
}

// ── Handle ───────────────────────────────────────────────────────────

/// Handle for swapping batches into a [`LiveSourceProvider`].
#[derive(Clone)]
pub struct LiveSourceHandle {
    slot: BatchSlot,
}

impl LiveSourceHandle {
    /// Replace current batches. In-flight readers that captured the
    /// prior snapshot continue to see the prior data; the next scan
    /// picks up the new one.
    pub fn swap(&self, batches: Vec<RecordBatch>) {
        *self.slot.lock() = Arc::new(batches);
    }

    /// Clear all pending batches.
    pub fn clear(&self) {
        *self.slot.lock() = Arc::new(Vec::new());
    }
}

impl std::fmt::Debug for LiveSourceHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveSourceHandle").finish()
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Float64, true),
        ]))
    }

    fn make_batch(ids: &[i64], names: &[&str], prices: &[f64]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(
                    names.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(prices.to_vec())),
            ],
        )
        .unwrap()
    }

    fn test_ctx() -> datafusion::prelude::SessionContext {
        // Use a plain context (no streaming validator) for unit tests.
        datafusion::prelude::SessionContext::new()
    }

    async fn count_rows(ctx: &datafusion::prelude::SessionContext, sql: &str) -> usize {
        let df = ctx.sql(sql).await.unwrap();
        df.collect()
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    }

    #[test]
    fn test_handle_swap_and_clear() {
        let provider = LiveSourceProvider::new(test_schema());
        let h1 = provider.handle();
        let h2 = h1.clone();

        h1.swap(vec![make_batch(&[1, 2], &["A", "B"], &[1.0, 2.0])]);
        assert_eq!(h2.slot.lock().len(), 1);

        h2.clear();
        assert_eq!(h1.slot.lock().len(), 0);
    }

    #[tokio::test]
    async fn test_scan_reads_fresh_data_each_execute() {
        let provider = Arc::new(LiveSourceProvider::new(test_schema()));
        let handle = provider.handle();
        let ctx = test_ctx();
        ctx.register_table("t", provider).unwrap();

        handle.swap(vec![make_batch(
            &[1, 2, 3],
            &["A", "B", "C"],
            &[10.0, 20.0, 30.0],
        )]);
        assert_eq!(count_rows(&ctx, "SELECT * FROM t").await, 3);
        assert_eq!(count_rows(&ctx, "SELECT * FROM t").await, 3);
    }

    #[tokio::test]
    async fn test_scan_empty() {
        let provider = Arc::new(LiveSourceProvider::new(test_schema()));
        let ctx = test_ctx();
        ctx.register_table("t", provider).unwrap();
        assert_eq!(count_rows(&ctx, "SELECT * FROM t").await, 0);
    }

    #[tokio::test]
    async fn test_projection() {
        let provider = Arc::new(LiveSourceProvider::new(test_schema()));
        let handle = provider.handle();
        let ctx = test_ctx();
        ctx.register_table("t", provider).unwrap();

        handle.swap(vec![make_batch(
            &[1, 2, 3],
            &["A", "B", "C"],
            &[10.0, 20.0, 30.0],
        )]);

        let df = ctx.sql("SELECT id, price FROM t").await.unwrap();
        let result = df.collect().await.unwrap();
        assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        assert_eq!(result[0].schema().fields().len(), 2);
        assert_eq!(result[0].schema().field(0).name(), "id");
        assert_eq!(result[0].schema().field(1).name(), "price");
    }

    #[tokio::test]
    async fn test_multi_cycle() {
        let provider = Arc::new(LiveSourceProvider::new(test_schema()));
        let handle = provider.handle();
        let ctx = test_ctx();
        ctx.register_table("t", provider).unwrap();

        handle.swap(vec![make_batch(&[1], &["A"], &[10.0])]);
        assert_eq!(count_rows(&ctx, "SELECT * FROM t").await, 1);

        handle.swap(vec![make_batch(&[2, 3], &["B", "C"], &[20.0, 30.0])]);
        assert_eq!(count_rows(&ctx, "SELECT * FROM t").await, 2);

        handle.clear();
        assert_eq!(count_rows(&ctx, "SELECT * FROM t").await, 0);
    }

    #[tokio::test]
    async fn test_cached_plan_sees_fresh_data() {
        use datafusion::physical_plan::ExecutionPlanProperties as _;

        let provider = Arc::new(LiveSourceProvider::new(test_schema()));
        let handle = provider.handle();
        let ctx = test_ctx();
        ctx.register_table("t", provider).unwrap();

        handle.swap(vec![make_batch(&[1], &["A"], &[10.0])]);
        let logical = ctx
            .state()
            .create_logical_plan("SELECT * FROM t")
            .await
            .unwrap();
        let physical = ctx.state().create_physical_plan(&logical).await.unwrap();
        assert_eq!(physical.output_partitioning().partition_count(), 1);

        let task_ctx = ctx.task_ctx();
        let r1 = datafusion::physical_plan::collect(physical.clone(), task_ctx.clone())
            .await
            .unwrap();
        assert_eq!(r1.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

        handle.swap(vec![make_batch(
            &[2, 3, 4],
            &["B", "C", "D"],
            &[20.0, 30.0, 40.0],
        )]);
        let r2 = datafusion::physical_plan::collect(physical.clone(), task_ctx.clone())
            .await
            .unwrap();
        assert_eq!(r2.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);

        handle.clear();
        let r3 = datafusion::physical_plan::collect(physical, task_ctx)
            .await
            .unwrap();
        assert_eq!(r3.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }
}
