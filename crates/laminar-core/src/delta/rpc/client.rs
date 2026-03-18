//! gRPC client wrappers with a per-peer connection pool.
//!
//! [`RpcPool`] caches `tonic::transport::Channel` instances per peer address
//! so that repeated calls to the same node reuse the same HTTP/2 connection.

use rustc_hash::FxHashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tonic::transport::{Channel, Endpoint};

use super::proto::{
    aggregate_exchange_client::AggregateExchangeClient,
    checkpoint_service_client::CheckpointServiceClient,
    partition_router_client::PartitionRouterClient, remote_state_client::RemoteStateClient,
    BarrierRequest, BatchLookupRequest, LookupRequest, PartialsRequest, RouteBatchRequest,
    StatusRequest,
};
use super::RpcError;

/// Per-peer gRPC connection pool.
///
/// Channels are created lazily on first use and cached for the lifetime
/// of the pool. Each channel multiplexes all four services over one
/// HTTP/2 connection.
#[derive(Debug)]
pub struct RpcPool {
    /// `rpc_address` → cached channel.
    channels: Arc<RwLock<FxHashMap<String, Channel>>>,
}

impl RpcPool {
    /// Create an empty pool.
    #[must_use]
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(FxHashMap::default())),
        }
    }

    /// Get or create a channel for the given peer address.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::Transport`] if the endpoint URI is invalid or
    /// the connection cannot be established.
    pub async fn channel(&self, address: &str) -> Result<Channel, RpcError> {
        // Fast path: read lock
        {
            let guard = self.channels.read();
            if let Some(ch) = guard.get(address) {
                return Ok(ch.clone());
            }
        }

        // Slow path: create and cache
        let uri = format!("http://{address}");
        let endpoint =
            Endpoint::from_shared(uri)?.tcp_keepalive(Some(std::time::Duration::from_secs(30)));
        let channel = endpoint.connect().await?;

        let mut guard = self.channels.write();
        // Double-check: another task may have raced us.
        guard.entry(address.to_string()).or_insert(channel.clone());
        Ok(channel)
    }

    /// Remove a cached channel (e.g., when a peer leaves the cluster).
    pub fn remove(&self, address: &str) {
        self.channels.write().remove(address);
    }

    /// Number of cached channels.
    #[must_use]
    pub fn len(&self) -> usize {
        self.channels.read().len()
    }

    /// Returns `true` if the pool has no cached channels.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.channels.read().is_empty()
    }
}

impl Default for RpcPool {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Typed client wrappers
// ---------------------------------------------------------------------------

/// Client for the `PartitionRouter` service.
#[derive(Debug, Clone)]
pub struct RouterClient {
    inner: PartitionRouterClient<Channel>,
}

impl RouterClient {
    /// Create a client from an existing channel.
    #[must_use]
    pub fn new(channel: Channel) -> Self {
        Self {
            inner: PartitionRouterClient::new(channel),
        }
    }

    /// Route a single batch to the partition owner.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::Status`] on gRPC failure.
    pub async fn route_batch(
        &mut self,
        partition_id: u32,
        batch_ipc: Vec<u8>,
        epoch: u64,
        source_node_id: u64,
    ) -> Result<super::proto::RouteBatchResponse, RpcError> {
        let resp = self
            .inner
            .route_batch(RouteBatchRequest {
                partition_id,
                batch_ipc,
                epoch,
                source_node_id,
            })
            .await?;
        Ok(resp.into_inner())
    }
}

/// Client for the `RemoteState` service.
#[derive(Debug, Clone)]
pub struct StateClient {
    inner: RemoteStateClient<Channel>,
}

impl StateClient {
    /// Create a client from an existing channel.
    #[must_use]
    pub fn new(channel: Channel) -> Self {
        Self {
            inner: RemoteStateClient::new(channel),
        }
    }

    /// Point lookup of a single key.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::Status`] on gRPC failure.
    pub async fn lookup(
        &mut self,
        store_name: String,
        key: Vec<u8>,
        partition_id: u32,
    ) -> Result<super::proto::LookupResponse, RpcError> {
        let resp = self
            .inner
            .lookup(LookupRequest {
                store_name,
                key,
                partition_id,
            })
            .await?;
        Ok(resp.into_inner())
    }

    /// Batch lookup of multiple keys.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::Status`] on gRPC failure.
    pub async fn batch_lookup(
        &mut self,
        store_name: String,
        keys: Vec<Vec<u8>>,
        partition_id: u32,
    ) -> Result<super::proto::BatchLookupResponse, RpcError> {
        let resp = self
            .inner
            .batch_lookup(BatchLookupRequest {
                store_name,
                keys,
                partition_id,
            })
            .await?;
        Ok(resp.into_inner())
    }
}

/// Client for the `CheckpointService`.
#[derive(Debug, Clone)]
pub struct CheckpointClient {
    inner: CheckpointServiceClient<Channel>,
}

impl CheckpointClient {
    /// Create a client from an existing channel.
    #[must_use]
    pub fn new(channel: Channel) -> Self {
        Self {
            inner: CheckpointServiceClient::new(channel),
        }
    }

    /// Forward a checkpoint barrier to a peer.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::Status`] on gRPC failure.
    pub async fn forward_barrier(
        &mut self,
        checkpoint_id: u64,
        epoch: u64,
        coordinator_node_id: u64,
        timestamp_ms: i64,
    ) -> Result<super::proto::BarrierResponse, RpcError> {
        let resp = self
            .inner
            .forward_barrier(BarrierRequest {
                checkpoint_id,
                epoch,
                coordinator_node_id,
                timestamp_ms,
            })
            .await?;
        Ok(resp.into_inner())
    }

    /// Query checkpoint status on a peer.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::Status`] on gRPC failure.
    pub async fn checkpoint_status(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<super::proto::StatusResponse, RpcError> {
        let resp = self
            .inner
            .checkpoint_status(StatusRequest { checkpoint_id })
            .await?;
        Ok(resp.into_inner())
    }
}

/// Client for the `AggregateExchange` service.
#[derive(Debug, Clone)]
pub struct AggregateClient {
    inner: AggregateExchangeClient<Channel>,
}

impl AggregateClient {
    /// Create a client from an existing channel.
    #[must_use]
    pub fn new(channel: Channel) -> Self {
        Self {
            inner: AggregateExchangeClient::new(channel),
        }
    }

    /// Send partial aggregation results for merge.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::Status`] on gRPC failure.
    pub async fn exchange_partials(
        &mut self,
        aggregation_id: String,
        partials_ipc: Vec<u8>,
        epoch: u64,
        source_node_id: u64,
    ) -> Result<super::proto::PartialsResponse, RpcError> {
        let resp = self
            .inner
            .exchange_partials(PartialsRequest {
                aggregation_id,
                partials_ipc,
                epoch,
                source_node_id,
            })
            .await?;
        Ok(resp.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_starts_empty() {
        let pool = RpcPool::new();
        assert!(pool.is_empty());
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn pool_remove_nonexistent_is_noop() {
        let pool = RpcPool::new();
        pool.remove("127.0.0.1:9999");
        assert!(pool.is_empty());
    }
}
