//! Bounded Arrow IPC over a versioned gRPC control stream.
//!
//! Each cluster owner connects to its own loopback worker. Replay admission requires the
//! source-order contract; Python additionally requires a live supervised replay-safe binding.
//! Authenticated nonlocal worker transport is unsupported.

mod client;
mod codec;
mod local_python;
mod python_environment;
mod response;
mod worker;

#[cfg(test)]
mod tests;

pub use client::RemoteProcessClient;
pub use local_python::{LocalPythonWorker, LocalPythonWorkerConfig};
pub use worker::RustReferenceWorker;

#[cfg(all(test, target_os = "linux"))]
pub(crate) use python_environment::read_only_fixture_config;

#[allow(clippy::doc_markdown, clippy::default_trait_access)] // Generated tonic stubs.
pub(crate) mod wire {
    tonic::include_proto!("laminar.process.v1");
}

const PROTOCOL_VERSION: u32 = 1;
const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const MAX_INVOCATION_WIRE_BYTES: usize = 32 * 1024 * 1024;
const MAX_IN_FLIGHT: usize = 32;

/// Host-assigned invocation scope. Logical activation IDs remain independent of both UUIDs.
/// A retry retains activation IDs and batch ID but receives a new attempt ID.
#[derive(Clone, Debug)]
pub struct RemoteInvocationScope {
    /// Stable operator identity within the pipeline.
    pub operator_id: String,
    /// The one vnode represented by this call.
    pub vnode: u32,
    /// Size of the host vnode domain.
    pub vnode_count: u32,
    /// Current ownership fence, assigned by the host.
    pub owner_generation: u64,
    /// Current recovery generation, assigned by the host.
    pub recovery_generation: u64,
    /// Transport batch identity, stable across attempts of this batch.
    pub batch_id: uuid::Uuid,
    /// Unique execution attempt identity.
    pub attempt_id: uuid::Uuid,
    /// Input watermark, separate from activation event timestamps.
    pub input_watermark_us: Option<i64>,
}
