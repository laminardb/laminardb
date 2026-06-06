//! Pull-path distributed scan: each node holds only its vnode-owned slice of an
//! MV result, so a `SELECT` unions the local slice with every peer's.

#![allow(clippy::disallowed_types)] // cold path: ad-hoc query planning, not the streaming hot path

use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_expr::{Expr, TableProviderFilterPushDown};
use futures::stream::StreamExt;
use laminar_core::cluster::control::{remote_scan_client, ClusterController};
use laminar_core::cluster::discovery::NodeId;

/// Concurrent in-flight `RemoteScan` calls per execution; bounds fan-out.
const MAX_CONCURRENT_REMOTE: usize = 16;

/// A `TableProvider` unioning a table's node-local rows (via `inner`) with the
/// rows pulled from every peer.
pub struct DistributedTableProvider {
    table_name: String,
    schema: SchemaRef,
    inner: Arc<dyn TableProvider>,
    controller: Arc<ClusterController>,
}

impl DistributedTableProvider {
    /// Wrap `inner` so scans fan out across the cluster.
    #[must_use]
    pub fn new(
        table_name: String,
        schema: SchemaRef,
        inner: Arc<dyn TableProvider>,
        controller: Arc<ClusterController>,
    ) -> Self {
        Self {
            table_name,
            schema,
            inner,
            controller,
        }
    }
}

impl Debug for DistributedTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistributedTableProvider")
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for DistributedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // RemoteScan carries no predicate, so claim none; DataFusion's
        // FilterExec above the union filters local and remote rows uniformly.
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Local slice; filters/limit are applied above the union by DataFusion.
        let local = self.inner.scan(state, projection, &[], None).await?;

        let me = self.controller.instance_id();
        let peers: Vec<NodeId> = self
            .controller
            .live_instances()
            .into_iter()
            .filter(|&id| id != me)
            .collect();

        Ok(Arc::new(DistributedScanExec::new(
            local,
            self.table_name.clone(),
            projection.cloned(),
            Arc::clone(&self.controller),
            peers,
        )))
    }
}

/// Physical plan unioning the local slice (its single child) with the rows
/// pulled from each peer.
pub struct DistributedScanExec {
    local: Arc<dyn ExecutionPlan>,
    table_name: String,
    projection: Option<Vec<usize>>,
    controller: Arc<ClusterController>,
    peers: Vec<NodeId>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl DistributedScanExec {
    /// Build the exec over the already-planned local scan.
    #[must_use]
    pub fn new(
        local: Arc<dyn ExecutionPlan>,
        table_name: String,
        projection: Option<Vec<usize>>,
        controller: Arc<ClusterController>,
        peers: Vec<NodeId>,
    ) -> Self {
        let schema = local.schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            local,
            table_name,
            projection,
            controller,
            peers,
            schema,
            properties,
        }
    }
}

impl Debug for DistributedScanExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistributedScanExec")
            .field("table_name", &self.table_name)
            .field("peers", &self.peers.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DistributedScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "DistributedScanExec: table={}, peers={}",
                self.table_name,
                self.peers.len()
            ),
            DisplayFormatType::TreeRender => write!(f, "DistributedScanExec"),
        }
    }
}

impl ExecutionPlan for DistributedScanExec {
    fn name(&self) -> &'static str {
        "DistributedScanExec"
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
        vec![&self.local]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "DistributedScanExec requires exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self::new(
            children.swap_remove(0),
            self.table_name.clone(),
            self.projection.clone(),
            Arc::clone(&self.controller),
            self.peers.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Plan(format!(
                "DistributedScanExec has a single partition; got {partition}"
            )));
        }

        // Local slice: run all child partitions concurrently.
        let local_partitions = self.local.output_partitioning().partition_count();
        let mut local_streams = Vec::with_capacity(local_partitions);
        for p in 0..local_partitions {
            local_streams.push(self.local.execute(p, Arc::clone(&context))?);
        }
        let local = if local_streams.is_empty() {
            futures::stream::empty().boxed()
        } else {
            futures::stream::select_all(local_streams).boxed()
        };

        // Remote slices, fetched concurrently. Each peer streams its rows in
        // row-bounded chunks, so a large slice is never materialized whole here;
        // the per-peer chunk streams are flattened into one. A peer error fails
        // the whole scan (consistency over availability); an empty slice is
        // valid, not an error.
        let pool = Arc::clone(self.controller.query_client_pool());
        let kv = Arc::clone(self.controller.kv());
        let table = self.table_name.clone();
        let projection = self.projection.clone();
        let schema = Arc::clone(&self.schema);
        let remote = futures::stream::iter(self.peers.clone())
            .map(move |peer| {
                let pool = Arc::clone(&pool);
                let kv = Arc::clone(&kv);
                let table = table.clone();
                let projection = projection.clone();
                let schema = Arc::clone(&schema);
                async move {
                    match remote_scan_client(&pool, &kv, peer, &table, projection).await {
                        // Adapt laminar-core's String-error Arrow stream into a
                        // DataFusion record-batch stream of this scan's schema.
                        Ok(chunks) => Box::pin(RecordBatchStreamAdapter::new(
                            schema,
                            chunks.map(|batch| batch.map_err(DataFusionError::Execution)),
                        )) as SendableRecordBatchStream,
                        // Couldn't even open the stream: surface as a one-item
                        // error stream so flatten() propagates the failure.
                        Err(e) => Box::pin(RecordBatchStreamAdapter::new(
                            schema,
                            futures::stream::once(async move {
                                Err::<RecordBatch, _>(DataFusionError::Execution(e))
                            }),
                        )) as SendableRecordBatchStream,
                    }
                }
            })
            .buffer_unordered(MAX_CONCURRENT_REMOTE)
            .flatten()
            .boxed();

        // Run local scan and remote peer fetches concurrently.
        let stream = futures::stream::select(local, remote);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}
