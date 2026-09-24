//! Bounded Arrow IPC over a versioned gRPC control stream.
//!
//! This local transport is an independent reference boundary. The database does not yet
//! register remote workers or admit them into a pipeline. Only explicit loopback endpoints are
//! accepted until authenticated deployment and lifecycle integration are implemented.

mod client;
mod codec;
mod response;
mod worker;

#[cfg(test)]
mod tests;

pub use client::RemoteProcessClient;
pub use worker::RustReferenceWorker;

#[allow(clippy::doc_markdown, clippy::default_trait_access)] // Generated tonic stubs.
pub(crate) mod wire {
    tonic::include_proto!("laminar.process.v1");
}

const PROTOCOL_VERSION: u32 = 1;
const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const MAX_INVOCATION_WIRE_BYTES: usize = 32 * 1024 * 1024;

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
