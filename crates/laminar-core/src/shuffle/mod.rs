//! Cross-instance shuffle over Tonic gRPC client-streaming: one
//! client-streaming call per peer pair carrying [`ShuffleMessage`](crate::shuffle::message::ShuffleMessage) frames.
//! Backpressure is the HTTP/2 flow-control window plus the bounded crossfire
//! inbound queue on the consuming side. The real transport is compiled under
//! `cluster`; the default build keeps a networking-free shim.

pub mod message;
pub mod routing;
pub mod transport;

pub use message::ShuffleMessage;
pub use routing::{
    logical_batch_bytes, route_checkpointed_batch, row_vnodes, CheckpointRoutePlan, LocalRoute,
    RemoteRoute, ShuffleRoutingError, ROUTE_MAX_BATCH_BYTES, ROUTE_MAX_BATCH_ROWS,
    ROUTE_TARGET_BATCH_BYTES,
};
pub use transport::{
    is_scope_cancelled, ReceivedBatch, ReceivedFrontierCut, ReceivedShuffle, ShuffleBatchAdmission,
    ShufflePeerId, ShuffleReceiver, ShuffleSender,
};
#[cfg(feature = "cluster")]
pub use transport::{shuffle_send_may_have_been_admitted, SHUFFLE_ADDR_KEY};
