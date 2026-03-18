//! # Delta: Distributed Coordination for LaminarDB
//!
//! This module implements multi-node distribution for LaminarDB, extending
//! the single-process engine with gossip discovery, Raft metadata consensus,
//! epoch-fenced partition ownership, distributed checkpointing, and gRPC
//! inter-node RPC.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
//! │   Node A     │◄──►│   Node B     │◄──►│   Node C     │
//! │  (Leader)    │    │  (Follower)  │    │  (Follower)  │
//! └──────┬───────┘    └──────┬───────┘    └──────┬───────┘
//!        │                   │                   │
//!   ┌────▼────┐         ┌────▼────┐         ┌────▼────┐
//!   │ Raft    │         │ Raft    │         │ Raft    │
//!   │ (meta)  │         │ (meta)  │         │ (meta)  │
//!   └────┬────┘         └────┬────┘         └────┬────┘
//!        │                   │                   │
//!   ┌────▼────┐         ┌────▼────┐         ┌────▼────┐
//!   │Partition│         │Partition│         │Partition│
//!   │ Guards  │         │ Guards  │         │ Guards  │
//!   └─────────┘         └─────────┘         └─────────┘
//! ```
//!
//! ## Modules
//!
//! - `discovery`: Node discovery (static seeds, gossip, Kafka groups)
//! - `coordination`: Raft-based metadata consensus
//! - `partition`: Epoch-fenced partition ownership and migration
//! - `remote_state`: Remote state access and cross-node lookup proxying
//! - `routing`: Partition-aware data routing for cluster mode
//! - `rpc`: gRPC services for inter-node communication

/// Compute the owning partition for a key using xxhash.
///
/// Uses `xxhash-rust`'s xxh3 for fast, high-quality hashing.
#[must_use]
#[allow(clippy::cast_possible_truncation)] // modulo guarantees result fits in u32
pub fn partition_for_key(key: &[u8], num_partitions: u32) -> u32 {
    let hash = xxhash_rust::xxh3::xxh3_64(key);
    (hash % u64::from(num_partitions)) as u32
}

/// Node discovery and membership.
pub mod discovery;

/// Raft-based metadata consensus and coordination.
pub mod coordination;

/// Epoch-fenced partition ownership, assignment, and migration.
pub mod partition;

/// Remote state access and cross-node lookup proxying.
pub mod remote_state;

/// Partition-aware data routing for cluster mode.
pub mod routing;

/// gRPC services for inter-node communication.
pub mod rpc;
