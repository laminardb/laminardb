//! SUBSCRIBE substrate: per-name broadcast registry and per-portal pump.

mod portal;
mod registry;

pub use portal::{PortalFrame, SubscriptionPortal};
pub(crate) use portal::MAX_SUBSCRIBERS_PER_MV;
pub(crate) use registry::SubscriptionRegistry;
