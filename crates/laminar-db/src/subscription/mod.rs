//! `SUBSCRIBE` substrate: one shared byte-bounded log per object and cursor portals.

pub(crate) mod distribution;
mod error;
#[cfg(feature = "cluster")]
mod frame;
mod portal;
mod registry;

pub(crate) const MAX_SUBSCRIBERS_PER_MV: usize = 64;

pub use error::ClusterSubscriptionError;
#[cfg(feature = "cluster")]
pub(crate) use frame::{PartitionedOutputBatch, PreparedSubscriptionOutput};
pub use portal::{PortalFrame, SubscriptionFrameLease, SubscriptionPortal};
pub use registry::SubscribeStart;
pub(crate) use registry::{SubscriptionOpenError, SubscriptionRegistry};
