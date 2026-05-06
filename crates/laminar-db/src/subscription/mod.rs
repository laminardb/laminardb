//! SUBSCRIBE substrate: per-name broadcast registry and per-portal pump.

mod portal;
mod registry;

pub(crate) use portal::MAX_SUBSCRIBERS_PER_MV;
pub use portal::{PortalFrame, SubscriptionPortal};
pub(crate) use registry::SubscriptionRegistry;
