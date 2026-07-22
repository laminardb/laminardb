//! `SUBSCRIBE` substrate: one shared byte-bounded log per object and cursor portals.

mod portal;
mod registry;

pub(crate) const MAX_SUBSCRIBERS_PER_MV: usize = 64;

pub use portal::{PortalFrame, SubscriptionFrameLease, SubscriptionPortal};
pub use registry::SubscribeStart;
pub(crate) use registry::{SubscriptionOpenError, SubscriptionRegistry};
