//! # Delta Coordination
//!
//! Implements the orchestration layer for the delta.
//! This module manages the lifecycle of a node in the delta:
//! discovery, partition assignment, and graceful shutdown.

/// Top-level orchestration of the delta lifecycle.
pub mod orchestrator;

pub use orchestrator::{DeltaManager, NodeLifecyclePhase};
