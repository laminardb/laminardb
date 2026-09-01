//! `SUBSCRIBE` substrate: one shared byte-bounded log per object and cursor portals.

#[cfg(feature = "cluster")]
pub(crate) mod cluster;
pub(crate) mod distribution;
mod error;
#[cfg(feature = "cluster")]
mod frame;
mod portal;
mod registry;

pub(crate) const MAX_SUBSCRIBERS_PER_MV: usize = 64;

pub use error::ClusterSubscriptionError;
#[cfg(feature = "cluster")]
pub(crate) use frame::{
    CertifiedSubscriptionFrontiers, PartitionedOutputBatch, PreparedSubscriptionOutput,
};
pub use portal::{
    ClusterSubscriptionFrameMetadata, PortalFrame, SubscriptionEnvelope, SubscriptionFrameLease,
    SubscriptionPortal,
};
pub use registry::SubscribeStart;
pub(crate) use registry::{SubscriptionOpenError, SubscriptionRegistry};
