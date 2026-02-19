//! # gRPC Inter-Node Communication
//!
//! Provides the gRPC service implementations and connection pool for
//! inter-node communication in the constellation.
//!
//! ## Services
//!
//! - [`LookupService`](lookup_service): Remote lookup table queries
//! - [`BarrierService`](barrier_service): Checkpoint barrier forwarding
//!
//! ## Infrastructure
//!
//! - [`RpcConnectionPool`]: Lazy, cached gRPC channel pool
//! - [`RpcError`]: Unified error type with gRPC status code mapping

/// Remote lookup service implementation.
pub mod lookup_service;

/// Barrier forwarding service implementation.
pub mod barrier_service;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tonic::transport::Channel;

/// Error codes for constellation RPC operations.
pub mod error_codes {
    /// The operation succeeded.
    pub const OK: u32 = 0;
    /// The target partition is not owned by this node.
    pub const NOT_OWNER: u32 = 1;
    /// The epoch in the request is stale.
    pub const EPOCH_STALE: u32 = 2;
    /// The target partition does not exist.
    pub const PARTITION_NOT_FOUND: u32 = 3;
    /// The target table does not exist.
    pub const TABLE_NOT_FOUND: u32 = 4;
    /// The request timed out.
    pub const TIMEOUT: u32 = 5;
    /// Internal server error.
    pub const INTERNAL: u32 = 100;
}

/// Errors from constellation RPC operations.
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    /// gRPC transport error.
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// gRPC status error.
    #[error("RPC status: {0}")]
    Status(#[from] tonic::Status),

    /// Connection pool error.
    #[error("connection pool error: {0}")]
    Pool(String),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// The epoch is stale (partition ownership has changed).
    #[error("stale epoch: local={local}, request={request}")]
    StaleEpoch {
        /// The node's current epoch.
        local: u64,
        /// The epoch in the request.
        request: u64,
    },

    /// The operation timed out.
    #[error("RPC timed out after {0:?}")]
    Timeout(Duration),
}

/// Configuration for the RPC connection pool.
#[derive(Debug, Clone)]
pub struct RpcPoolConfig {
    /// Connection timeout for new channels.
    pub connect_timeout: Duration,
    /// Timeout for individual RPC calls.
    pub request_timeout: Duration,
    /// Maximum number of cached channels.
    pub max_channels: usize,
    /// TCP keepalive interval.
    pub keepalive_interval: Option<Duration>,
}

impl Default for RpcPoolConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            max_channels: 64,
            keepalive_interval: Some(Duration::from_secs(30)),
        }
    }
}

/// Lazy, cached pool of gRPC channels to peer nodes.
///
/// Channels are created on first use and reused for subsequent requests.
/// tonic channels are internally connection-pooled and multiplexed, so
/// a single channel per peer is typically sufficient.
#[derive(Debug)]
pub struct RpcConnectionPool {
    channels: Arc<RwLock<HashMap<String, Channel>>>,
    config: RpcPoolConfig,
}

impl RpcConnectionPool {
    /// Create a new connection pool with the given configuration.
    #[must_use]
    pub fn new(config: RpcPoolConfig) -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Get or create a channel to the given address.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::Transport`] if the connection cannot be established.
    pub async fn get_channel(&self, address: &str) -> Result<Channel, RpcError> {
        // Fast path: check existing channel
        {
            let channels = self.channels.read();
            if let Some(channel) = channels.get(address) {
                return Ok(channel.clone());
            }
        }

        // Slow path: create new channel
        let endpoint = Channel::from_shared(format!("http://{address}"))
            .map_err(|e| RpcError::Pool(e.to_string()))?
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.request_timeout);

        let endpoint = if let Some(interval) = self.config.keepalive_interval {
            endpoint
                .keep_alive_timeout(interval)
                .http2_keep_alive_interval(interval)
        } else {
            endpoint
        };

        let channel = endpoint.connect().await?;

        let mut channels = self.channels.write();
        // Double-check: another task may have created it
        channels.entry(address.to_string()).or_insert(channel.clone());
        Ok(channel)
    }

    /// Remove a channel for the given address (e.g., on connection failure).
    pub fn remove_channel(&self, address: &str) {
        self.channels.write().remove(address);
    }

    /// Get the number of cached channels.
    #[must_use]
    pub fn channel_count(&self) -> usize {
        self.channels.read().len()
    }

    /// Clear all cached channels.
    pub fn clear(&self) {
        self.channels.write().clear();
    }
}

impl Default for RpcConnectionPool {
    fn default() -> Self {
        Self::new(RpcPoolConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_error_display() {
        let err = RpcError::StaleEpoch {
            local: 5,
            request: 3,
        };
        assert!(err.to_string().contains("stale epoch"));
    }

    #[test]
    fn test_rpc_pool_config_default() {
        let config = RpcPoolConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.max_channels, 64);
    }

    #[test]
    fn test_pool_channel_count() {
        let pool = RpcConnectionPool::default();
        assert_eq!(pool.channel_count(), 0);
    }

    #[test]
    fn test_pool_clear() {
        let pool = RpcConnectionPool::default();
        pool.clear();
        assert_eq!(pool.channel_count(), 0);
    }

    #[test]
    fn test_pool_remove_nonexistent() {
        let pool = RpcConnectionPool::default();
        pool.remove_channel("nonexistent:9000");
        assert_eq!(pool.channel_count(), 0);
    }

    #[tokio::test]
    async fn test_pool_get_channel_invalid_address() {
        let pool = RpcConnectionPool::new(RpcPoolConfig {
            connect_timeout: Duration::from_millis(100),
            ..RpcPoolConfig::default()
        });
        // Connecting to a non-existent address should error
        let result = pool.get_channel("192.0.2.1:1").await;
        assert!(result.is_err());
    }

    #[test]
    fn test_error_codes() {
        assert_eq!(error_codes::OK, 0);
        assert_eq!(error_codes::NOT_OWNER, 1);
        assert_eq!(error_codes::EPOCH_STALE, 2);
    }
}
