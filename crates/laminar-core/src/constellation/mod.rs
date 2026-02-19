//! # Constellation: Distributed Coordination for LaminarDB
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
//! - [`discovery`]: Node discovery (static seeds, gossip, Kafka groups)
//! - [`coordination`]: Raft-based metadata consensus
//! - [`partition`]: Epoch-fenced partition ownership and migration
//! - [`checkpoint`]: Distributed checkpoint coordination
//! - [`rpc`]: gRPC services for inter-node communication

/// Node discovery and membership.
pub mod discovery;

/// Raft-based metadata consensus and coordination.
pub mod coordination;

/// Epoch-fenced partition ownership, assignment, and migration.
pub mod partition;

/// Distributed checkpoint coordination.
pub mod checkpoint;

/// gRPC inter-node communication services.
pub mod rpc;

/// Generated protobuf types.
pub mod proto {
    #![allow(clippy::all, clippy::pedantic, missing_docs)]
    tonic::include_proto!("laminardb.constellation.v1");
}
