//! In-engine distributed query coordination (pull path).
//!
//! A coordinator node serving a `SELECT` against a distributed table /
//! materialized view fans a [`remote_scan_client`] call out to every peer that
//! owns part of the data and unions the per-node Arrow batches. The serving
//! side is the [`QueryService`] Tonic server, which delegates to a node-local
//! [`RemoteQueryHandler`] and streams the resulting [`RecordBatch`] back as
//! Arrow IPC. The service shares the control-plane port with `BarrierSync`
//! (see [`super::barrier`]), so peers are reachable at the address they
//! already publish under [`BARRIER_ADDR_KEY`].

use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::Stream;

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

/// Shared slot holding the node-local handler. The [`QueryServiceImpl`] reads
/// it per request, so registration and server start are order-independent.
pub type QueryHandlerSlot = Arc<parking_lot::RwLock<Option<Arc<dyn RemoteQueryHandler>>>>;

/// Node-local provider of table / materialized-view snapshots for remote scans.
///
/// Implemented by the database layer; the cluster server delegates each
/// incoming [`QueryService::remote_scan`] to it.
#[async_trait::async_trait]
pub trait RemoteQueryHandler: Send + Sync + 'static {
    /// Return this node's locally-held rows for `table_name`, optionally
    /// projected to the given column indices.
    ///
    /// # Errors
    /// Returns a human-readable message when the table is unknown locally or
    /// the projection is invalid.
    async fn remote_scan(
        &self,
        table_name: &str,
        projection: Option<Vec<usize>>,
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
        let projection = if req.projection.is_empty() {
            None
        } else {
            Some(req.projection.iter().map(|&p| p as usize).collect())
        };

        let batch = handler
            .remote_scan(&req.table_name, projection)
            .await
            .map_err(tonic::Status::internal)?;

        let arrow_ipc = serialize_batch_stream(&batch)
            .map_err(|e| tonic::Status::internal(format!("arrow ipc encode: {e}")))?;

        let stream = futures::stream::once(async move { Ok(RemoteScanResponse { arrow_ipc }) });
        Ok(tonic::Response::new(Box::pin(stream)))
    }
}

/// Build the `QueryService` Tonic server for the shared control-plane server,
/// reading the registered handler out of `slot` per request.
pub(crate) fn query_service_server(
    slot: QueryHandlerSlot,
) -> query_v1::query_service_server::QueryServiceServer<QueryServiceImpl> {
    query_v1::query_service_server::QueryServiceServer::new(QueryServiceImpl::new(slot))
}

/// Scan `table_name` on `peer`, returning the peer's locally-held rows.
///
/// Resolves the peer's control-plane address from gossip ([`BARRIER_ADDR_KEY`]),
/// opens (or reuses) a `QueryService` channel, streams every Arrow IPC chunk,
/// and concatenates them into a single [`RecordBatch`].
///
/// # Errors
/// Returns a message on discovery failure, transport error, an empty response,
/// or Arrow IPC (de)serialization failure.
pub async fn remote_scan_client(
    pool: &Arc<parking_lot::Mutex<std::collections::HashMap<NodeId, tonic::transport::Channel>>>,
    kv: &Arc<dyn ClusterKv>,
    peer: NodeId,
    table_name: &str,
    projection: Option<Vec<usize>>,
) -> Result<RecordBatch, String> {
    let channel = {
        let chan = pool.lock().get(&peer).cloned();
        if let Some(chan) = chan {
            chan
        } else {
            let addr = kv
                .read_from(peer, BARRIER_ADDR_KEY)
                .await
                .ok_or_else(|| format!("no control-plane address for peer {}", peer.0))?;

            let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                .map_err(|e| e.to_string())?;
            let channel = endpoint.connect_lazy();

            let mut guard = pool.lock();
            if let Some(chan) = guard.get(&peer) {
                chan.clone()
            } else {
                guard.insert(peer, channel.clone());
                channel
            }
        }
    };
    let mut client = QueryServiceClient::new(channel);

    let projection = projection
        .unwrap_or_default()
        .into_iter()
        .map(|p| u32::try_from(p).map_err(|_| format!("projection index {p} out of range")))
        .collect::<Result<Vec<u32>, String>>()?;

    let request = RemoteScanRequest {
        table_name: table_name.to_string(),
        projection,
    };

    let mut stream = client
        .remote_scan(request)
        .await
        .map_err(|e| format!("remote_scan to peer {} failed: {e}", peer.0))?
        .into_inner();

    let mut batches = Vec::new();
    while let Some(resp) = stream.message().await.map_err(|e| e.to_string())? {
        batches.push(deserialize_batch_stream(&resp.arrow_ipc).map_err(|e| e.to_string())?);
    }

    let Some(first) = batches.first() else {
        return Err(format!("peer {} returned no data for '{table_name}'", peer.0));
    };
    let schema = first.schema();
    arrow::compute::concat_batches(&schema, &batches).map_err(|e| e.to_string())
}
