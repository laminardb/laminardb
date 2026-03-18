//! gRPC service implementations.
//!
//! Each service delegates to a trait-based handler so the server layer
//! stays thin and testable without a running gRPC stack.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use super::codec;
use super::proto::{
    aggregate_exchange_server::AggregateExchange, checkpoint_service_server::CheckpointService,
    partition_router_server::PartitionRouter, remote_state_server::RemoteState, BarrierRequest,
    BarrierResponse, BatchChunk, BatchLookupRequest, BatchLookupResponse, LookupRequest,
    LookupResponse, LookupResult, PartialsRequest, PartialsResponse, RouteBatchRequest,
    RouteBatchResponse, StatusRequest, StatusResponse, StreamBatchesResponse,
};

// ---------------------------------------------------------------------------
// Handler traits — implement these to plug in real logic
// ---------------------------------------------------------------------------

/// Handler for incoming routed batches.
#[tonic::async_trait]
pub trait BatchRouterHandler: Send + Sync + 'static {
    /// Process a single routed batch.
    async fn handle_route_batch(
        &self,
        partition_id: u32,
        batch_ipc: &[u8],
        epoch: u64,
        source_node_id: u64,
    ) -> Result<u64, Status>;
}

/// Handler for remote state lookups.
#[tonic::async_trait]
pub trait StateHandler: Send + Sync + 'static {
    /// Lookup a single key.
    async fn handle_lookup(
        &self,
        store_name: &str,
        key: &[u8],
        partition_id: u32,
    ) -> Result<Option<Vec<u8>>, Status>;
}

/// Handler for checkpoint barrier forwarding.
#[tonic::async_trait]
pub trait CheckpointHandler: Send + Sync + 'static {
    /// Process an incoming barrier.
    async fn handle_barrier(
        &self,
        checkpoint_id: u64,
        epoch: u64,
        coordinator_node_id: u64,
        timestamp_ms: i64,
    ) -> Result<(), Status>;

    /// Query checkpoint status.
    async fn handle_status(&self, checkpoint_id: u64) -> Result<(i32, Vec<u32>, Vec<u32>), Status>;
}

/// Handler for partial aggregation exchange.
#[tonic::async_trait]
pub trait AggregateHandler: Send + Sync + 'static {
    /// Receive partial aggregation results.
    async fn handle_partials(
        &self,
        aggregation_id: &str,
        partials_ipc: &[u8],
        epoch: u64,
        source_node_id: u64,
    ) -> Result<(), Status>;
}

// ---------------------------------------------------------------------------
// PartitionRouter gRPC service
// ---------------------------------------------------------------------------

/// gRPC implementation of `PartitionRouter`.
pub struct PartitionRouterService<H: BatchRouterHandler> {
    handler: Arc<H>,
}

impl<H: BatchRouterHandler> PartitionRouterService<H> {
    /// Create a new service backed by the given handler.
    pub fn new(handler: Arc<H>) -> Self {
        Self { handler }
    }
}

#[tonic::async_trait]
impl<H: BatchRouterHandler> PartitionRouter for PartitionRouterService<H> {
    async fn route_batch(
        &self,
        request: Request<RouteBatchRequest>,
    ) -> Result<Response<RouteBatchResponse>, Status> {
        let req = request.into_inner();
        match self
            .handler
            .handle_route_batch(
                req.partition_id,
                &req.batch_ipc,
                req.epoch,
                req.source_node_id,
            )
            .await
        {
            Ok(rows) => Ok(Response::new(RouteBatchResponse {
                accepted: true,
                error: String::new(),
                rows_written: rows,
            })),
            Err(status) => Ok(Response::new(RouteBatchResponse {
                accepted: false,
                error: status.message().to_string(),
                rows_written: 0,
            })),
        }
    }

    async fn stream_batches(
        &self,
        request: Request<tonic::Streaming<BatchChunk>>,
    ) -> Result<Response<StreamBatchesResponse>, Status> {
        use tokio_stream::StreamExt;

        let mut stream = request.into_inner();
        let mut chunks_accepted: u64 = 0;
        let mut total_rows: u64 = 0;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            match self
                .handler
                .handle_route_batch(
                    chunk.partition_id,
                    &chunk.batch_ipc,
                    chunk.epoch,
                    chunk.source_node_id,
                )
                .await
            {
                Ok(rows) => {
                    chunks_accepted += 1;
                    total_rows += rows;
                }
                Err(status) => {
                    return Ok(Response::new(StreamBatchesResponse {
                        chunks_accepted,
                        total_rows,
                        error: status.message().to_string(),
                    }));
                }
            }
        }

        Ok(Response::new(StreamBatchesResponse {
            chunks_accepted,
            total_rows,
            error: String::new(),
        }))
    }
}

// ---------------------------------------------------------------------------
// RemoteState gRPC service
// ---------------------------------------------------------------------------

/// gRPC implementation of `RemoteState`.
pub struct RemoteStateService<H: StateHandler> {
    handler: Arc<H>,
}

impl<H: StateHandler> RemoteStateService<H> {
    /// Create a new service backed by the given handler.
    pub fn new(handler: Arc<H>) -> Self {
        Self { handler }
    }
}

#[tonic::async_trait]
impl<H: StateHandler> RemoteState for RemoteStateService<H> {
    async fn lookup(
        &self,
        request: Request<LookupRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .handler
            .handle_lookup(&req.store_name, &req.key, req.partition_id)
            .await?;
        Ok(Response::new(LookupResponse {
            found: result.is_some(),
            value_ipc: result.unwrap_or_default(),
        }))
    }

    async fn batch_lookup(
        &self,
        request: Request<BatchLookupRequest>,
    ) -> Result<Response<BatchLookupResponse>, Status> {
        let req = request.into_inner();
        let mut results = Vec::with_capacity(req.keys.len());
        for key in &req.keys {
            let result = self
                .handler
                .handle_lookup(&req.store_name, key, req.partition_id)
                .await?;
            results.push(LookupResult {
                found: result.is_some(),
                value_ipc: result.unwrap_or_default(),
            });
        }
        Ok(Response::new(BatchLookupResponse { results }))
    }
}

// ---------------------------------------------------------------------------
// CheckpointService gRPC service
// ---------------------------------------------------------------------------

/// gRPC implementation of `CheckpointService`.
pub struct CheckpointGrpcService<H: CheckpointHandler> {
    handler: Arc<H>,
}

impl<H: CheckpointHandler> CheckpointGrpcService<H> {
    /// Create a new service backed by the given handler.
    pub fn new(handler: Arc<H>) -> Self {
        Self { handler }
    }
}

#[tonic::async_trait]
impl<H: CheckpointHandler> CheckpointService for CheckpointGrpcService<H> {
    async fn forward_barrier(
        &self,
        request: Request<BarrierRequest>,
    ) -> Result<Response<BarrierResponse>, Status> {
        let req = request.into_inner();
        match self
            .handler
            .handle_barrier(
                req.checkpoint_id,
                req.epoch,
                req.coordinator_node_id,
                req.timestamp_ms,
            )
            .await
        {
            Ok(()) => Ok(Response::new(BarrierResponse {
                acknowledged: true,
                error: String::new(),
            })),
            Err(status) => Ok(Response::new(BarrierResponse {
                acknowledged: false,
                error: status.message().to_string(),
            })),
        }
    }

    async fn checkpoint_status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let req = request.into_inner();
        let (phase, completed, pending) = self.handler.handle_status(req.checkpoint_id).await?;
        Ok(Response::new(StatusResponse {
            phase,
            completed_partitions: completed,
            pending_partitions: pending,
            error: String::new(),
        }))
    }
}

// ---------------------------------------------------------------------------
// AggregateExchange gRPC service
// ---------------------------------------------------------------------------

/// gRPC implementation of `AggregateExchange`.
pub struct AggregateExchangeService<H: AggregateHandler> {
    handler: Arc<H>,
}

impl<H: AggregateHandler> AggregateExchangeService<H> {
    /// Create a new service backed by the given handler.
    pub fn new(handler: Arc<H>) -> Self {
        Self { handler }
    }
}

#[tonic::async_trait]
impl<H: AggregateHandler> AggregateExchange for AggregateExchangeService<H> {
    async fn exchange_partials(
        &self,
        request: Request<PartialsRequest>,
    ) -> Result<Response<PartialsResponse>, Status> {
        let req = request.into_inner();
        match self
            .handler
            .handle_partials(
                &req.aggregation_id,
                &req.partials_ipc,
                req.epoch,
                req.source_node_id,
            )
            .await
        {
            Ok(()) => Ok(Response::new(PartialsResponse {
                accepted: true,
                error: String::new(),
            })),
            Err(status) => Ok(Response::new(PartialsResponse {
                accepted: false,
                error: status.message().to_string(),
            })),
        }
    }
}

// Re-export the generated server types for convenient wiring.
pub use super::proto::{
    aggregate_exchange_server::AggregateExchangeServer,
    checkpoint_service_server::CheckpointServiceServer,
    partition_router_server::PartitionRouterServer, remote_state_server::RemoteStateServer,
};

/// Convenience: the unused `codec` import keeps the module wired for downstream use.
const _: () = {
    // Ensure codec is referenced so it doesn't appear as dead code.
    let _ = codec::encode_batch;
};
