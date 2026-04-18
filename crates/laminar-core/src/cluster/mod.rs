//! Cluster coordination: discovery, partition ownership, control plane.

/// Node discovery and membership.
pub mod discovery;

/// Lifecycle state machine for cluster nodes.
pub mod coordination;

/// Epoch-fenced partition ownership, assignment, and migration.
pub mod partition;

/// Control plane: leader election, assignment snapshots, barrier
/// coordination. See `docs/plans/distributed-stateful-pipelines.md`
/// §7.
pub mod control;

/// Per-connector split enumeration and assignment. See §3 of the
/// parent design doc.
pub mod split;

/// In-process harness for cluster-control integration tests.
/// Gated on `cluster-unstable` because it pulls in chitchat on
/// loopback UDP.
#[cfg(feature = "cluster-unstable")]
pub mod testing;
