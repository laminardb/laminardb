//! Cross-instance shuffle: one persistent TCP connection per peer pair,
//! length-prefixed frames with a 1-byte tag. Backpressure is carried
//! by the per-partition tokio mpsc on the consuming side.

pub mod barrier_tracker;
pub mod message;
pub mod routing;
pub mod transport;

pub use barrier_tracker::BarrierTracker;
pub use message::ShuffleMessage;
pub use routing::{row_vnodes, slice_batch_by_vnode};
#[cfg(feature = "cluster-unstable")]
pub use transport::SHUFFLE_ADDR_KEY;
pub use transport::{ShufflePeerId, ShuffleReceiver, ShuffleSender};
