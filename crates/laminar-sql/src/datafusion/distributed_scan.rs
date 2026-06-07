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
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::{Expr, TableProviderFilterPushDown};
use datafusion_sql::unparser;
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
        // Renderable filters push to peers as `Inexact` (coordinator's FilterExec
        // re-applies the exact predicate); the rest stay `Unsupported`.
        Ok(filters
            .iter()
            .map(|f| {
                if expr_to_sql(f).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Local slice; filters/limit are re-applied above the union by
        // DataFusion (pushdown is `Inexact`), so the local scan stays unfiltered.
        let local = self.inner.scan(state, projection, &[], None).await?;

        // Join renderable filters into one SQL predicate for peers; unrenderable
        // ones were reported `Unsupported`, so the coordinator still evaluates them.
        let rendered: Vec<String> = filters.iter().filter_map(expr_to_sql).collect();
        let filter_sql = (!rendered.is_empty()).then(|| rendered.join(" AND "));

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
            filter_sql,
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
    /// Pushed-down predicate (SQL boolean expression) sent to each peer.
    filter_sql: Option<String>,
    controller: Arc<ClusterController>,
    peers: Vec<NodeId>,
    schema: SchemaRef,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl DistributedScanExec {
    /// Build the exec over the already-planned local scan.
    #[must_use]
    pub fn new(
        local: Arc<dyn ExecutionPlan>,
        table_name: String,
        projection: Option<Vec<usize>>,
        filter_sql: Option<String>,
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
            filter_sql,
            controller,
            peers,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
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

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
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
            self.filter_sql.clone(),
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

        // Metrics surface in EXPLAIN ANALYZE: peer establishment failures, peers
        // skipped as unreachable (a partial result), and total rows unioned.
        let peer_failures = MetricBuilder::new(&self.metrics).global_counter("peer_failures");
        let peers_skipped = MetricBuilder::new(&self.metrics).global_counter("peers_skipped");
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);

        // Remote slices fetched concurrently and flattened; each peer streams in
        // chunks. A peer error fails the whole scan; an empty slice is valid.
        let pool = Arc::clone(self.controller.query_client_pool());
        let kv = Arc::clone(self.controller.kv());
        let table = self.table_name.clone();
        let projection = self.projection.clone();
        let filter_sql = self.filter_sql.clone();
        let schema = Arc::clone(&self.schema);
        let remote = futures::stream::iter(self.peers.clone())
            .map(move |peer| {
                let pool = Arc::clone(&pool);
                let kv = Arc::clone(&kv);
                let table = table.clone();
                let projection = projection.clone();
                let filter_sql = filter_sql.clone();
                let schema = Arc::clone(&schema);
                let peer_failures = peer_failures.clone();
                let peers_skipped = peers_skipped.clone();
                async move {
                    match remote_scan_client(&pool, &kv, peer, &table, projection, filter_sql).await
                    {
                        // Adapt laminar-core's String-error Arrow stream into a
                        // DataFusion record-batch stream of this scan's schema.
                        Ok(Some(chunks)) => Box::pin(RecordBatchStreamAdapter::new(
                            schema,
                            chunks.map(|batch| batch.map_err(DataFusionError::Execution)),
                        )) as SendableRecordBatchStream,
                        // Peer unreachable (down/pruned): skip rather than fail the
                        // scan, but count + log so the partial result isn't silent.
                        Ok(None) => {
                            peers_skipped.add(1);
                            tracing::warn!(
                                peer = peer.0,
                                table = %table,
                                "distributed scan: peer unreachable, omitting its slice (partial result)"
                            );
                            Box::pin(RecordBatchStreamAdapter::new(
                                schema,
                                futures::stream::empty::<Result<RecordBatch>>(),
                            )) as SendableRecordBatchStream
                        }
                        // Couldn't even open the stream: surface as a one-item
                        // error stream so flatten() propagates the failure.
                        Err(e) => {
                            peer_failures.add(1);
                            Box::pin(RecordBatchStreamAdapter::new(
                                schema,
                                futures::stream::once(async move {
                                    Err::<RecordBatch, _>(DataFusionError::Execution(e))
                                }),
                            )) as SendableRecordBatchStream
                        }
                    }
                }
            })
            .buffer_unordered(MAX_CONCURRENT_REMOTE)
            .flatten()
            .boxed();

        // Run local scan and remote peer fetches concurrently, counting rows.
        let stream = futures::stream::select(local, remote).inspect(move |r| {
            if let Ok(batch) = r {
                output_rows.add(batch.num_rows());
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

/// Render a predicate to SQL for a peer to re-compile via DataFusion's own
/// `Expr` -> SQL unparser, or `None` if the unparser can't represent it (e.g. a
/// UDF we don't push down). Pushdown is `Inexact`, so the coordinator re-applies
/// the exact predicate above the union — an over-broad render is harmless.
fn expr_to_sql(expr: &Expr) -> Option<String> {
    let unqualified = strip_column_qualifiers(expr);
    unparser::expr_to_sql(&unqualified)
        .ok()
        .map(|ast| ast.to_string())
}

/// Strip the relation qualifier from every column so the rendered predicate
/// resolves against the peer's single (differently-named) table rather than the
/// coordinator's. The transform is infallible; fall back to the input on the
/// (unreachable) error path.
fn strip_column_qualifiers(expr: &Expr) -> Expr {
    expr.clone()
        .transform(|e| match e {
            Expr::Column(mut col) => {
                col.relation = None;
                Ok(Transformed::yes(Expr::Column(col)))
            }
            other => Ok(Transformed::no(other)),
        })
        .map_or_else(|_| expr.clone(), |t| t.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::{col, lit};

    #[test]
    fn renders_comparison() {
        let e = col("price").gt(lit(100_i64));
        assert_eq!(expr_to_sql(&e).as_deref(), Some("(price > 100)"));
    }

    #[test]
    fn strips_column_qualifier() {
        // A column qualified by the coordinator's table renders unqualified so
        // the peer can resolve it against its own single table.
        let e = Expr::Column(Column::new(Some("mv"), "price")).gt(lit(100_i64));
        assert_eq!(expr_to_sql(&e).as_deref(), Some("(price > 100)"));
    }

    #[test]
    fn renders_and_or_not_and_null_checks() {
        let e = col("a").eq(lit(1_i64)).and(col("b").is_null());
        assert_eq!(expr_to_sql(&e).as_deref(), Some("((a = 1) AND b IS NULL)"));

        let e = Expr::Not(Box::new(col("flag").is_not_null()));
        assert_eq!(expr_to_sql(&e).as_deref(), Some("NOT flag IS NOT NULL"));
    }

    #[test]
    fn escapes_string_literals_and_quotes() {
        // `name` is a SQL keyword, so the unparser double-quotes the identifier;
        // the single quote inside the literal is doubled.
        let e = col("name").eq(lit("o'brien"));
        assert_eq!(expr_to_sql(&e).as_deref(), Some("(\"name\" = 'o''brien')"));
    }

    #[test]
    fn renders_booleans_and_finite_floats() {
        assert_eq!(expr_to_sql(&lit(true)).as_deref(), Some("true"));
        assert_eq!(expr_to_sql(&lit(0.1_f64)).as_deref(), Some("0.1"));
    }

    #[test]
    fn like_predicate_now_renders() {
        // The standard unparser supports LIKE (the hand-rolled renderer did
        // not); safe because pushdown stays Inexact and the coordinator
        // re-applies the exact predicate above the union.
        let e = col("name").like(lit("foo%"));
        assert!(expr_to_sql(&e).is_some());
    }

    #[test]
    fn unconvertible_nodes_return_none() {
        // A binary literal has no SQL spelling the unparser supports, so the
        // whole predicate falls back to `None`, stays Unsupported, and is
        // evaluated only on the coordinator.
        let e = col("payload").eq(Expr::Literal(
            ScalarValue::Binary(Some(vec![0xde, 0xad])),
            None,
        ));
        assert_eq!(expr_to_sql(&e), None);
    }
}
