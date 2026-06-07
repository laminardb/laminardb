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
use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, Operator, TableProviderFilterPushDown};
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
        // A filter we can render to SQL is sent to peers so they filter their
        // slice before serialization. It's `Inexact`: DataFusion keeps a
        // FilterExec above the union that re-applies the exact predicate, so an
        // over-broad remote result is still correct. Filters we can't render
        // stay `Unsupported` and are evaluated only on the coordinator.
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

        // Render the filters we can to one SQL predicate for peers to apply
        // locally. A filter that doesn't render is simply dropped here (it was
        // reported `Unsupported`, so the coordinator still evaluates it).
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
    /// Pushed-down predicate (SQL boolean expression) sent to each peer; `None`
    /// means no filter is applied remotely.
    filter_sql: Option<String>,
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

        // Remote slices, fetched concurrently. Each peer streams its rows in
        // row-bounded chunks, so a large slice is never materialized whole here;
        // the per-peer chunk streams are flattened into one. A peer error fails
        // the whole scan (consistency over availability); an empty slice is
        // valid, not an error.
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
                async move {
                    match remote_scan_client(&pool, &kv, peer, &table, projection, filter_sql).await
                    {
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

/// Render a small, safe subset of DataFusion predicates to a SQL string a peer
/// can re-compile against its own copy of the table schema; `None` for anything
/// outside the subset.
///
/// Only expressions that round-trip *faithfully* are rendered. Pushdown is
/// `Inexact`, so the coordinator re-applies the exact predicate above the union:
/// an over-broad remote result is corrected there, but an under-broad one would
/// silently drop rows. Every conversion here therefore preserves the original
/// selectivity exactly, and unconvertible nodes fall back to the coordinator.
fn expr_to_sql(expr: &Expr) -> Option<String> {
    match expr {
        // Emit column names unqualified and double-quoted so the peer resolves
        // them case-sensitively against its single scanned table; the original
        // qualifier names the coordinator's table, not the peer's.
        Expr::Column(col) => Some(format!("\"{}\"", col.name.replace('"', "\"\""))),
        Expr::Literal(value, _) => scalar_to_sql(value),
        Expr::BinaryExpr(be) => {
            let op = binary_op_to_sql(be.op)?;
            let left = expr_to_sql(be.left.as_ref())?;
            let right = expr_to_sql(be.right.as_ref())?;
            Some(format!("({left} {op} {right})"))
        }
        Expr::Not(inner) => Some(format!("(NOT {})", expr_to_sql(inner.as_ref())?)),
        Expr::IsNull(inner) => Some(format!("({} IS NULL)", expr_to_sql(inner.as_ref())?)),
        Expr::IsNotNull(inner) => Some(format!("({} IS NOT NULL)", expr_to_sql(inner.as_ref())?)),
        _ => None,
    }
}

/// SQL spelling of the binary operators we push down; `None` for the rest.
fn binary_op_to_sql(op: Operator) -> Option<&'static str> {
    Some(match op {
        Operator::Eq => "=",
        Operator::NotEq => "<>",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        Operator::And => "AND",
        Operator::Or => "OR",
        Operator::Plus => "+",
        Operator::Minus => "-",
        Operator::Multiply => "*",
        Operator::Divide => "/",
        _ => return None,
    })
}

/// Render a scalar literal to SQL, restricted to types that round-trip exactly
/// (integers, booleans, strings, finite floats); `None` for everything else so
/// the filter falls back to coordinator-side evaluation.
// The integer arms look identical but bind differently-typed locals, so they
// can't be merged with `|`.
#[allow(clippy::match_same_arms)]
fn scalar_to_sql(value: &ScalarValue) -> Option<String> {
    Some(match value {
        ScalarValue::Boolean(Some(b)) => (if *b { "TRUE" } else { "FALSE" }).to_string(),
        ScalarValue::Int8(Some(n)) => n.to_string(),
        ScalarValue::Int16(Some(n)) => n.to_string(),
        ScalarValue::Int32(Some(n)) => n.to_string(),
        ScalarValue::Int64(Some(n)) => n.to_string(),
        ScalarValue::UInt8(Some(n)) => n.to_string(),
        ScalarValue::UInt16(Some(n)) => n.to_string(),
        ScalarValue::UInt32(Some(n)) => n.to_string(),
        ScalarValue::UInt64(Some(n)) => n.to_string(),
        // Widen f32 to f64 before rendering so the text matches the value the
        // peer's comparison sees (f32 columns are widened to f64 there too).
        ScalarValue::Float32(Some(f)) if f.is_finite() => f64::from(*f).to_string(),
        ScalarValue::Float64(Some(f)) if f.is_finite() => f.to_string(),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => format!("'{}'", s.replace('\'', "''")),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::{col, lit};

    #[test]
    fn renders_comparison_with_quoted_column() {
        let e = col("price").gt(lit(100_i64));
        assert_eq!(expr_to_sql(&e).as_deref(), Some("(\"price\" > 100)"));
    }

    #[test]
    fn renders_and_or_not_and_null_checks() {
        let e = col("a").eq(lit(1_i64)).and(col("b").is_null());
        assert_eq!(
            expr_to_sql(&e).as_deref(),
            Some("((\"a\" = 1) AND (\"b\" IS NULL))")
        );

        let e = Expr::Not(Box::new(col("flag").is_not_null()));
        assert_eq!(
            expr_to_sql(&e).as_deref(),
            Some("(NOT (\"flag\" IS NOT NULL))")
        );
    }

    #[test]
    fn escapes_string_literals_and_quotes() {
        let e = col("name").eq(lit("o'brien"));
        assert_eq!(expr_to_sql(&e).as_deref(), Some("(\"name\" = 'o''brien')"));
    }

    #[test]
    fn renders_booleans_and_finite_floats() {
        assert_eq!(
            scalar_to_sql(&ScalarValue::Boolean(Some(true))).as_deref(),
            Some("TRUE")
        );
        assert_eq!(
            scalar_to_sql(&ScalarValue::Float64(Some(0.1))).as_deref(),
            Some("0.1")
        );
    }

    #[test]
    fn unconvertible_nodes_return_none() {
        // Non-finite floats can't round-trip through SQL.
        assert_eq!(scalar_to_sql(&ScalarValue::Float64(Some(f64::NAN))), None);
        // A node we don't render (LIKE) makes the whole predicate fall back, so
        // it stays Unsupported and is evaluated only on the coordinator.
        let e = col("name").like(lit("foo%"));
        assert_eq!(expr_to_sql(&e), None);
    }
}
