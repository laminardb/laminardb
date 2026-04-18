//! Cluster coordination: discovery and control plane.

/// Node discovery and membership.
pub mod discovery;

/// Control plane: leader election, assignment snapshots, barrier
/// coordination.
pub mod control;

/// In-process harness for cluster integration tests. Gated because
/// it pulls in chitchat on loopback UDP.
#[cfg(feature = "cluster-unstable")]
pub mod testing;
