//! SUBSCRIBE substrate: per-name broadcast registry and per-portal pump.

mod portal;
mod registry;

pub(crate) use portal::MAX_SUBSCRIBERS_PER_MV;
pub use portal::{PortalFrame, SubscriptionPortal};
pub use registry::SubscribeStart;
pub(crate) use registry::SubscriptionRegistry;

/// Shuffle stage carrying a stream's SUBSCRIBE output to remote subscribers.
/// The `__sub::` prefix keeps it disjoint from operator shuffle stages.
#[cfg(feature = "cluster")]
pub(crate) fn remote_stage(stream: &str) -> String {
    format!("__sub::{stream}")
}
