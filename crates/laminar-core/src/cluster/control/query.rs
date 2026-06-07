//! In-engine distributed query (pull path): a coordinator fans `RemoteScan` out
//! to peers owning part of a table/MV and unions their Arrow batches.

use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::Stream;

use super::barrier::BARRIER_ADDR_KEY;
use super::ClusterKv;
use crate::cluster::discovery::NodeId;
use crate::serialization::{BatchStreamDecoder, BatchStreamEncoder};

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

/// Attempts to *establish* a peer stream before giving up; transient connect
/// failures evict the channel and re-resolve the peer between tries.
const REMOTE_SCAN_MAX_ATTEMPTS: u32 = 3;
/// Fixed delay between establishment attempts.
const REMOTE_SCAN_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);

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

        // Stream the slice in row-bounded chunks as one Arrow IPC stream (schema
        // in the first chunk only); a zero-row slice is one valid empty chunk.
        let total = batch.num_rows();
        let ranges: Vec<(usize, usize)> = if total == 0 {
            vec![(0, 0)]
        } else {
            (0..total)
                .step_by(REMOTE_SCAN_CHUNK_ROWS)
                .map(|off| (off, REMOTE_SCAN_CHUNK_ROWS.min(total - off)))
                .collect()
        };
        let encoder = BatchStreamEncoder::new(&batch.schema())
            .map_err(|e| tonic::Status::internal(format!("arrow ipc schema encode: {e}")))?;
        let stream = futures::stream::unfold(
            (encoder, batch, ranges.into_iter().peekable()),
            |(mut encoder, batch, mut ranges)| async move {
                let (off, len) = ranges.next()?;
                // IIFE so the `&mut encoder`/`&mut ranges` borrows end before the
                // state tuple is moved back out below.
                let produced = (|| {
                    let mut bytes = encoder.encode(&batch.slice(off, len))?;
                    // The end-of-stream marker rides the final chunk.
                    if ranges.peek().is_none() {
                        bytes.extend_from_slice(&encoder.finish()?);
                    }
                    Ok::<_, arrow_schema::ArrowError>(bytes)
                })();
                let item = produced
                    .map(|arrow_ipc| RemoteScanResponse { arrow_ipc })
                    .map_err(|e| tonic::Status::internal(format!("arrow ipc encode: {e}")));
                Some((item, (encoder, batch, ranges)))
            },
        );
        Ok(tonic::Response::new(Box::pin(stream)))
    }
}

/// `QueryService` server reading the registered handler out of `slot` per request.
pub(crate) fn query_service_server(
    slot: QueryHandlerSlot,
) -> query_v1::query_service_server::QueryServiceServer<QueryServiceImpl> {
    query_v1::query_service_server::QueryServiceServer::new(QueryServiceImpl::new(slot))
}

/// Pooled channel to `peer`, connecting lazily. `Ok(None)` = no published
/// address (down / not started / pruned) → the caller skips the peer.
async fn connect(
    pool: &QueryClientPool,
    kv: &Arc<dyn ClusterKv>,
    peer: NodeId,
) -> Result<Option<tonic::transport::Channel>, String> {
    if let Some(chan) = pool.lock().get(&peer).cloned() {
        return Ok(Some(chan));
    }
    let Some(addr) = kv.read_from(peer, BARRIER_ADDR_KEY).await else {
        return Ok(None);
    };
    let channel = super::tls::client_endpoint(&addr)?
        .connect_timeout(REMOTE_SCAN_CONNECT_TIMEOUT)
        .connect_lazy();
    Ok(Some(pool.lock().entry(peer).or_insert(channel).clone()))
}

/// Lazily-streamed Arrow [`RecordBatch`] chunks from a peer's `RemoteScan`; a
/// decode or mid-stream transport error surfaces as an `Err` item.
pub type RemoteBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, String>> + Send>>;

/// Scan `table_name` on `peer` as a lazy Arrow-IPC stream. `Ok(None)` means the
/// peer is unreachable (no published address) and should be skipped, not failed.
///
/// # Errors
/// Transport/timeout while establishing; post-stream failures surface as `Err` items.
pub async fn remote_scan_client(
    pool: &QueryClientPool,
    kv: &Arc<dyn ClusterKv>,
    peer: NodeId,
    table_name: &str,
    projection: Option<Vec<usize>>,
    filter_sql: Option<String>,
) -> Result<Option<RemoteBatchStream>, String> {
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

    // Establish the stream, retrying transient connect failures. Each retry
    // evicts the pooled channel so `connect` re-resolves a possibly-restarted
    // peer. The idle timeout bounds time-to-first-response, not the whole call,
    // so a large multi-chunk result is free to take its time once streaming.
    let mut attempt = 0u32;
    let stream = loop {
        attempt += 1;
        let Some(channel) = connect(pool, kv, peer).await? else {
            return Ok(None); // peer has no address (down/pruned) — skip it
        };
        let mut client = QueryServiceClient::new(channel);
        match tokio::time::timeout(
            REMOTE_SCAN_IDLE_TIMEOUT,
            client.remote_scan(request.clone()),
        )
        .await
        {
            Ok(Ok(resp)) => break resp.into_inner(),
            Ok(Err(status)) => {
                pool.lock().remove(&peer);
                // Only `Unavailable` (e.g. a restarting peer) is worth retrying.
                if status.code() == tonic::Code::Unavailable && attempt < REMOTE_SCAN_MAX_ATTEMPTS {
                    tokio::time::sleep(REMOTE_SCAN_RETRY_BACKOFF).await;
                    continue;
                }
                return Err(format!("remote_scan to peer {} failed: {status}", peer.0));
            }
            Err(_) => {
                pool.lock().remove(&peer);
                if attempt < REMOTE_SCAN_MAX_ATTEMPTS {
                    tokio::time::sleep(REMOTE_SCAN_RETRY_BACKOFF).await;
                    continue;
                }
                return Err(format!(
                    "remote_scan to peer {} timed out opening stream",
                    peer.0
                ));
            }
        }
    };

    // One decoder for the whole response (a chunk may complete 0+ batches, so
    // buffer the surplus). Idle timeout bounds the inter-chunk gap, not total time.
    let pool = Arc::clone(pool);
    let decoder = BatchStreamDecoder::new();
    let out = futures::stream::unfold(
        Some((
            stream,
            decoder,
            std::collections::VecDeque::<RecordBatch>::new(),
        )),
        move |state| {
            let pool = Arc::clone(&pool);
            async move {
                let (mut stream, mut decoder, mut pending) = state?;
                loop {
                    if let Some(batch) = pending.pop_front() {
                        return Some((Ok(batch), Some((stream, decoder, pending))));
                    }
                    match tokio::time::timeout(REMOTE_SCAN_IDLE_TIMEOUT, stream.message()).await {
                        Ok(Ok(Some(resp))) => match decoder.decode_chunk(resp.arrow_ipc) {
                            Ok(batches) => pending.extend(batches),
                            Err(e) => return Some((Err(e.to_string()), None)),
                        },
                        Ok(Ok(None)) => return None,
                        Ok(Err(status)) => {
                            pool.lock().remove(&peer);
                            return Some((Err(status.to_string()), None));
                        }
                        Err(_) => {
                            pool.lock().remove(&peer);
                            return Some((
                                Err(format!("remote_scan to peer {} stalled mid-stream", peer.0)),
                                None,
                            ));
                        }
                    }
                }
            }
        },
    );
    Ok(Some(Box::pin(out)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::control::barrier::InMemoryKv;
    use arrow::array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;

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

    /// Records the projection + `filter_sql` it was called with, so a test can
    /// assert the request carried them over the wire.
    type SeenArgs = Arc<parking_lot::Mutex<Option<(Option<Vec<usize>>, Option<String>)>>>;
    struct RecordingHandler {
        batch: RecordBatch,
        seen: SeenArgs,
    }

    #[async_trait::async_trait]
    impl RemoteQueryHandler for RecordingHandler {
        async fn remote_scan(
            &self,
            _table: &str,
            projection: Option<Vec<usize>>,
            filter_sql: Option<String>,
        ) -> Result<RecordBatch, String> {
            *self.seen.lock() = Some((projection.clone(), filter_sql));
            match projection {
                Some(p) => self.batch.project(&p).map_err(|e| e.to_string()),
                None => Ok(self.batch.clone()),
            }
        }
    }

    /// Serve `handler` on an ephemeral port, returning a kv that resolves `peer`
    /// to it plus an empty client pool.
    async fn serve_handler(
        peer: NodeId,
        handler: Arc<dyn RemoteQueryHandler>,
    ) -> (QueryClientPool, Arc<dyn ClusterKv>) {
        let slot: QueryHandlerSlot = Arc::new(parking_lot::RwLock::new(Some(handler)));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let mut builder = tonic::transport::Server::builder();
            // Mirror production: apply control-plane TLS when installed.
            if let Some(tls) = crate::cluster::control::tls::server_tls() {
                builder = builder.tls_config(tls.clone()).unwrap();
            }
            let _ = builder
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

    async fn serve(peer: NodeId, batch: RecordBatch) -> (QueryClientPool, Arc<dyn ClusterKv>) {
        serve_handler(peer, Arc::new(StaticHandler(batch))).await
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
            .unwrap()
            .expect("peer resolvable");
        let batches = collect_chunks(stream).await;
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
            .unwrap()
            .expect("peer resolvable");
        let batches = collect_chunks(stream).await;
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 0);
    }

    // Projection and filter_sql reach the serving handler over the wire, and the
    // projected result streams back.
    #[tokio::test]
    async fn remote_scan_forwards_projection_and_filter() {
        let seen: SeenArgs = Arc::new(parking_lot::Mutex::new(None));
        let handler = Arc::new(RecordingHandler {
            batch: int_batch(vec![10, 20, 30]),
            seen: Arc::clone(&seen),
        });
        let peer = NodeId(7);
        let (pool, kv) = serve_handler(peer, handler).await;

        let stream = remote_scan_client(
            &pool,
            &kv,
            peer,
            "mv",
            Some(vec![0]),
            Some("(\"n\" > 1)".into()),
        )
        .await
        .unwrap()
        .expect("peer resolvable");
        let batches = collect_chunks(stream).await;
        let got = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let col = got.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(col.values(), &[10, 20, 30]);

        let (proj, filter) = seen.lock().clone().expect("handler was called");
        assert_eq!(proj, Some(vec![0]));
        assert_eq!(filter.as_deref(), Some("(\"n\" > 1)"));
    }

    // End-to-end mTLS: a node serves and dials with a cert chained to the shared
    // CA, so a successful scan proves both directions verified (the server
    // requires a client cert; the client verifies the server cert + SAN).
    //
    // Ignored because it installs the process-global cluster TLS, which would
    // make the plaintext tests above use TLS. Run it alone:
    //   cargo test -p laminar-core --features cluster -- --ignored
    #[tokio::test]
    #[ignore = "installs process-global cluster TLS; run with --ignored"]
    async fn remote_scan_over_mtls() {
        const SAN: &str = "laminar-cluster";

        // Mint a throwaway CA and one node cert (both server- and client-auth,
        // SAN = the name the client verifies).
        let mut ca_params = rcgen::CertificateParams::new(vec!["laminar-test-ca".into()]).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        let mut leaf = rcgen::CertificateParams::new(vec![SAN.into()]).unwrap();
        leaf.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let leaf_cert = leaf.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

        crate::cluster::control::set_cluster_tls(crate::cluster::control::ClusterTls::from_pem(
            leaf_cert.pem().as_bytes(),
            leaf_key.serialize_pem().as_bytes(),
            ca_cert.pem().as_bytes(),
            SAN,
        ));

        let peer = NodeId(7);
        let (pool, kv) =
            serve_handler(peer, Arc::new(StaticHandler(int_batch(vec![1, 2, 3])))).await;
        let stream = remote_scan_client(&pool, &kv, peer, "mv", None, None)
            .await
            .unwrap()
            .expect("peer resolvable");
        let batches = collect_chunks(stream).await;
        let got = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let col = got.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(col.values(), &[1, 2, 3]);
    }
}
