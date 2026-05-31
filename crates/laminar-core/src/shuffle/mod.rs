//! Cross-instance shuffle over Tonic gRPC client-streaming: one
//! client-streaming call per peer pair carrying [`ShuffleMessage`] frames.
//! Backpressure is the HTTP/2 flow-control window plus the bounded crossfire
//! inbound queue on the consuming side. The real transport is compiled under
//! `cluster-unstable`; the default build keeps a networking-free shim.

pub mod barrier_tracker;
pub mod message;
pub mod routing;
pub mod transport;

pub use barrier_tracker::BarrierTracker;
pub use message::ShuffleMessage;
pub use routing::{row_vnodes, slice_batch_by_vnode, slice_batch_by_vnodes, slice_batch_by_targets};
#[cfg(feature = "cluster-unstable")]
pub use transport::SHUFFLE_ADDR_KEY;
pub use transport::{ShufflePeerId, ShuffleReceiver, ShuffleSender};
