//! # gRPC Inter-Node RPC
//!
//! Provides gRPC services for inter-node communication in delta mode:
//!
//! - **PartitionRouter**: Route record batches to partition owners
//! - **RemoteState**: Cross-node state lookups for join enrichment
//! - **CheckpointService**: Distributed checkpoint barrier coordination
//! - **AggregateExchange**: Partial aggregation result exchange

pub mod client;
pub mod codec;
pub mod server;

/// Generated protobuf types and service traits.
#[allow(missing_docs, clippy::all, clippy::pedantic)]
pub mod proto {
    tonic::include_proto!("laminar.rpc");
}

/// Errors from RPC operations.
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    /// gRPC transport error.
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// gRPC status error from a remote call.
    #[error("rpc status: {0}")]
    Status(#[from] tonic::Status),

    /// Arrow IPC serialization/deserialization failure.
    #[error("codec error: {0}")]
    Codec(#[from] arrow_schema::ArrowError),

    /// Peer not found in the connection pool.
    #[error("unknown peer: {0}")]
    UnknownPeer(String),

    /// Epoch mismatch (stale request).
    #[error("epoch fenced: expected {expected}, got {actual}")]
    EpochFenced {
        /// The epoch the receiver expects.
        expected: u64,
        /// The epoch the sender provided.
        actual: u64,
    },
}
