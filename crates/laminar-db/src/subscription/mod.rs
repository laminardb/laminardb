//! SUBSCRIBE substrate: per-name broadcast registry and per-portal pump.

mod portal;
mod registry;

pub(crate) use portal::MAX_SUBSCRIBERS_PER_MV;
pub use portal::{PortalFrame, SubscriptionPortal};
pub use registry::SubscribeStart;
pub(crate) use registry::SubscriptionRegistry;

/// Prefix tagging a shuffle stage as SUBSCRIBE output rather than an operator
/// stage. Keeps subscription batches disjoint from operator shuffle stages so
/// the router can lift them all out of the receiver in one pass.
#[cfg(feature = "cluster")]
pub(crate) const REMOTE_STAGE_PREFIX: &str = "__sub::";

/// Shuffle stage carrying a stream's SUBSCRIBE output to remote subscribers.
#[cfg(feature = "cluster")]
pub(crate) fn remote_stage(stream: &str) -> String {
    format!("{REMOTE_STAGE_PREFIX}{stream}")
}

/// Inverse of [`remote_stage`]: the stream name from a `__sub::`-tagged stage,
/// or `None` if `stage` isn't a subscription stage.
#[cfg(feature = "cluster")]
pub(crate) fn stream_from_remote_stage(stage: &str) -> Option<&str> {
    stage.strip_prefix(REMOTE_STAGE_PREFIX)
}
