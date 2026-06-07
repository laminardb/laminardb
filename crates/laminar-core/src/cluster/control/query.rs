//! In-engine distributed query (pull path): a coordinator fans `RemoteScan` out
//! to peers owning part of a table/MV and unions their Arrow batches.

use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};

use super::barrier::BARRIER_ADDR_KEY;
use super::ClusterKv;
use crate::cluster::discovery::NodeId;
use crate::serialization::{deserialize_batch_stream, serialize_batch_stream};

#[allow(
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::missing_const_for_fn,
    clippy::must_use_candidate,
    clippy::too_many_lines,
    missing_docs
)]
pub(crate) mod query_v1 {
    tonic::include_proto!("laminar.query.v1");
}

use query_v1::query_service_client::QueryServiceClient;
use query_v1::query_service_server::QueryService;
use query_v1::{RemoteScanRequest, RemoteScanResponse};

/// Rows per streamed `RemoteScan` chunk; bounds any single gRPC message.
const REMOTE_SCAN_CHUNK_ROWS: usize = 8192;

/// TCP connect timeout for a peer's lazy channel.
const REMOTE_SCAN_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// Idle (per-chunk) timeout for the first response and each chunk — not a
/// total-call deadline, so large results still stream but a stalled peer fails.
const REMOTE_SCAN_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Handler slot read per request, so registration and server start are
/// order-independent.
pub type QueryHandlerSlot = Arc<parking_lot::RwLock<Option<Arc<dyn RemoteQueryHandler>>>>;

/// Pooled per-peer channels for `RemoteScan`, reused across queries and evicted
/// on RPC failure so a restarted peer is re-resolved.
pub type QueryClientPool =
    Arc<parking_lot::Mutex<rustc_hash::FxHashMap<NodeId, tonic::transport::Channel>>>;

/// Node-local provider of table / materialized-view snapshots for remote scans.
#[async_trait::async_trait]
pub trait RemoteQueryHandler: Send + Sync + 'static {
    /// This node's locally-held rows for `table_name`, optionally projected and
    /// optionally filtered by `filter_sql` before serialization.
    ///
    /// # Errors
    /// Unknown table or invalid projection. A `filter_sql` that fails to compile
    /// is skipped (the coordinator re-applies it), not an error.
    async fn remote_scan(
        &self,
        table_name: &str,
        projection: Option<Vec<usize>>,
        filter_sql: Option<String>,
    ) -> Result<RecordBatch, String>;
}

/// Tonic `QueryService` implementation delegating to a [`RemoteQueryHandler`].
pub(crate) struct QueryServiceImpl {
    handler: QueryHandlerSlot,
}

impl QueryServiceImpl {
    pub(crate) fn new(handler: QueryHandlerSlot) -> Self {
        Self { handler }
    }
}

type RemoteScanStream =
    Pin<Box<dyn Stream<Item = Result<RemoteScanResponse, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl QueryService for QueryServiceImpl {
    type RemoteScanStream = RemoteScanStream;

    async fn remote_scan(
        &self,
        request: tonic::Request<RemoteScanRequest>,
    ) -> Result<tonic::Response<Self::RemoteScanStream>, tonic::Status> {
        let handler = self
            .handler
            .read()
            .clone()
            .ok_or_else(|| tonic::Status::unavailable("no query handler registered"))?;

        let req = request.into_inner();
        let projection = (!req.projection.is_empty())
            .then(|| req.projection.iter().map(|&p| p as usize).collect());
        let filter_sql = (!req.filter_sql.is_empty()).then(|| req.filter_sql.clone());

        let batch = handler
            .remote_scan(&req.table_name, projection, filter_sql)
            .await
            .map_err(tonic::Status::internal)?;

        // Stream the in-memory slice in row-bounded chunks (one IPC encode per
        // poll); a zero-row slice is one valid empty chunk, never a caller error.
        let total = batch.num_rows();
        let ranges: Vec<(usize, usize)> = if total == 0 {
            vec![(0, 0)]
        } else {
            (0..total)
                .step_by(REMOTE_SCAN_CHUNK_ROWS)
                .map(|off| (off, REMOTE_SCAN_CHUNK_ROWS.min(total - off)))
                .collect()
        };
        let stream = futures::stream::iter(ranges).map(move |(off, len)| {
            serialize_batch_stream(&batch.slice(off, len))
                .map(|arrow_ipc| RemoteScanResponse { arrow_ipc })
                .map_err(|e| tonic::Status::internal(format!("arrow ipc encode: {e}")))
        });
        Ok(tonic::Response::new(Box::pin(stream)))
    }
}

/// `QueryService` server reading the registered handler out of `slot` per request.
pub(crate) fn query_service_server(
    slot: QueryHandlerSlot,
) -> query_v1::query_service_server::QueryServiceServer<QueryServiceImpl> {
    query_v1::query_service_server::QueryServiceServer::new(QueryServiceImpl::new(slot))
}

/// Resolve (or reuse) the pooled channel to `peer`, connecting lazily.
async fn connect(
    pool: &QueryClientPool,
    kv: &Arc<dyn ClusterKv>,
    peer: NodeId,
) -> Result<tonic::transport::Channel, String> {
    if let Some(chan) = pool.lock().get(&peer).cloned() {
        return Ok(chan);
    }
    let addr = kv
        .read_from(peer, BARRIER_ADDR_KEY)
        .await
        .ok_or_else(|| format!("no control-plane address for peer {}", peer.0))?;
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|e| e.to_string())?
        .connect_timeout(REMOTE_SCAN_CONNECT_TIMEOUT)
        .connect_lazy();
    Ok(pool.lock().entry(peer).or_insert(channel).clone())
}

/// Lazily-streamed Arrow [`RecordBatch`] chunks from a peer's `RemoteScan`; a
/// decode or mid-stream transport error surfaces as an `Err` item.
pub type RemoteBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, String>> + Send>>;

/// Scan `table_name` on `peer`, returning its Arrow IPC chunks as a lazy stream;
/// evicts the pooled channel on transport/idle error.
///
/// # Errors
/// Discovery/transport/timeout while *establishing* the stream; post-stream
/// failures (decode, dropped connection, idle timeout) surface as `Err` items.
pub async fn remote_scan_client(
    pool: &QueryClientPool,
    kv: &Arc<dyn ClusterKv>,
    peer: NodeId,
    table_name: &str,
    projection: Option<Vec<usize>>,
    filter_sql: Option<String>,
) -> Result<RemoteBatchStream, String> {
    let channel = connect(pool, kv, peer).await?;
    let mut client = QueryServiceClient::new(channel);

    let projection = projection
        .unwrap_or_default()
        .into_iter()
        .map(|p| u32::try_from(p).map_err(|_| format!("projection index {p} out of range")))
        .collect::<Result<Vec<u32>, String>>()?;

    let request = RemoteScanRequest {
        table_name: table_name.to_string(),
        projection,
        filter_sql: filter_sql.unwrap_or_default(),
    };

    // Bound time-to-first-response (not the whole call) so a hung peer can't
    // block the scan, while a large multi-chunk result is free to take its time.
    let stream =
        match tokio::time::timeout(REMOTE_SCAN_IDLE_TIMEOUT, client.remote_scan(request)).await {
            Ok(Ok(resp)) => resp.into_inner(),
            Ok(Err(e)) => {
                pool.lock().remove(&peer);
                return Err(format!("remote_scan to peer {} failed: {e}", peer.0));
            }
            Err(_) => {
                pool.lock().remove(&peer);
                return Err(format!(
                    "remote_scan to peer {} timed out opening stream",
                    peer.0
                ));
            }
        };

    // Decode each chunk, bounding the gap between chunks (not total scan time);
    // a transport error or stall evicts the pooled channel.
    let pool = Arc::clone(pool);
    let decoded = futures::stream::unfold(Some(stream), move |state| {
        let pool = Arc::clone(&pool);
        async move {
            let mut stream = state?;
            match tokio::time::timeout(REMOTE_SCAN_IDLE_TIMEOUT, stream.message()).await {
                Ok(Ok(Some(resp))) => match deserialize_batch_stream(&resp.arrow_ipc) {
                    Ok(batch) => Some((Ok(batch), Some(stream))),
                    Err(e) => Some((Err(e.to_string()), None)),
                },
                Ok(Ok(None)) => None,
                Ok(Err(status)) => {
                    pool.lock().remove(&peer);
                    Some((Err(status.to_string()), None))
                }
                Err(_) => {
                    pool.lock().remove(&peer);
                    Some((
                        Err(format!("remote_scan to peer {} stalled mid-stream", peer.0)),
                        None,
                    ))
                }
            }
        }
    });
    Ok(Box::pin(decoded))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::control::barrier::InMemoryKv;
    use arrow::array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};

    struct StaticHandler(RecordBatch);

    #[async_trait::async_trait]
    impl RemoteQueryHandler for StaticHandler {
        async fn remote_scan(
            &self,
            _table: &str,
            projection: Option<Vec<usize>>,
            _filter_sql: Option<String>,
        ) -> Result<RecordBatch, String> {
            match projection {
                Some(p) => self.0.project(&p).map_err(|e| e.to_string()),
                None => Ok(self.0.clone()),
            }
        }
    }

    /// Start the query service on an ephemeral port, returning a kv that
    /// resolves `peer` to it.
    async fn serve(peer: NodeId, batch: RecordBatch) -> (QueryClientPool, Arc<dyn ClusterKv>) {
        let slot: QueryHandlerSlot = Arc::new(parking_lot::RwLock::new(Some(Arc::new(
            StaticHandler(batch),
        ))));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let _ = tonic::transport::Server::builder()
                .add_service(query_service_server(slot))
                .serve_with_incoming(incoming)
                .await;
        });
        let kv = InMemoryKv::new(NodeId(0));
        kv.seed(peer, BARRIER_ADDR_KEY, addr.to_string());
        let pool: QueryClientPool =
            Arc::new(parking_lot::Mutex::new(rustc_hash::FxHashMap::default()));
        (pool, Arc::new(kv))
    }

    fn int_batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    /// Collect every chunk of a remote scan stream, unwrapping any `Err` item.
    async fn collect_chunks(stream: RemoteBatchStream) -> Vec<RecordBatch> {
        stream
            .map(|item| item.expect("remote scan chunk decode failed"))
            .collect::<Vec<_>>()
            .await
    }

    // A result larger than one chunk arrives as multiple batches that reassemble
    // in stream order on the caller.
    #[tokio::test]
    async fn remote_scan_reassembles_chunks_in_order() {
        let count = i32::try_from(REMOTE_SCAN_CHUNK_ROWS).unwrap() + 100;
        let values: Vec<i32> = (0..count).collect();
        let peer = NodeId(7);
        let (pool, kv) = serve(peer, int_batch(values.clone())).await;

        let stream = remote_scan_client(&pool, &kv, peer, "mv", None, None)
            .await
            .unwrap();
        let batches = collect_chunks(stream).await;
        // More rows than one chunk holds, so the result must span several chunks.
        assert!(batches.len() > 1);
        let got = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let col = got.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(col.values(), values.as_slice());
    }

    // An empty local slice is a valid (schema-only) result, not a query failure.
    #[tokio::test]
    async fn remote_scan_empty_slice_is_not_an_error() {
        let peer = NodeId(7);
        let (pool, kv) = serve(peer, int_batch(vec![])).await;

        let stream = remote_scan_client(&pool, &kv, peer, "mv", None, None)
            .await
            .unwrap();
        let batches = collect_chunks(stream).await;
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 0);
    }
}
