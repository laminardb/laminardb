//! Cross-instance shuffle.
//!
//! See `docs/plans/shuffle-protocol.md` for the full design. Short
//! summary: each pair of instances has one persistent async-I/O
//! connection (TCP in production, loopback duplex in tests); messages
//! are length-prefixed with a 1-byte discriminator. Supported
//! discriminators: `Data` (Arrow IPC batch), `Barrier` (24-byte
//! checkpoint barrier), `Credit` (flow-control delta), `Close`
//! (graceful drain).
//!
//! The sender holds a per-connection bytes-denominated
//! [`CreditSemaphore`](flow::CreditSemaphore) and blocks when credit is
//! depleted. The receiver issues credit as it drains the received
//! batch queue. Ordering per (sender, receiver) pair is FIFO via the
//! underlying connection.
//!
//! Current scope (Phase C.3 skeleton):
//! - Message codec over any `AsyncRead + AsyncWrite` transport.
//! - Credit flow primitive.
//! - In-memory duplex tests proving end-to-end round-trips.
//!
//! Deferred to the wiring phase (C.5):
//! - TCP listener + connection pool (it's mechanical once the codec is
//!   proven; see the design doc).
//! - Chitchat KV integration for peer discovery (`members/<id>/shuffle_addr`).
//! - Integration with `CheckpointBarrierInjector` so barriers flow
//!   in-band.

pub mod barrier_tracker;
pub mod flow;
pub mod message;
pub mod transport;

pub use barrier_tracker::BarrierTracker;
pub use flow::CreditSemaphore;
pub use message::{read_message, write_message, ShuffleMessage, TAG_BARRIER, TAG_CLOSE,
    TAG_CREDIT, TAG_DATA, TAG_HELLO, TAG_VNODE_DATA};
pub use transport::{fan_out_barrier, ShufflePeerId, ShuffleReceiver, ShuffleSender};
#[cfg(feature = "cluster-unstable")]
pub use transport::SHUFFLE_ADDR_KEY;
