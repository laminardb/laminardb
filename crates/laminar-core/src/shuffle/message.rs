//! Logical messages carried on a shuffle stream.
//!
//! The wire encoding is gRPC/protobuf (`proto/shuffle.proto`, `ShuffleFrame`);
//! the `<->` conversion lives in [`super::transport`]. A `VnodeData`'s batch is
//! encoded as an Arrow IPC single-batch stream via [`crate::serialization`], so a
//! schema roll on one frame cannot poison the stream.

use arrow_array::RecordBatch;

use crate::checkpoint::barrier::CheckpointBarrier;

/// Maximum Arrow IPC payload accepted for a single `VnodeData` frame: 64 MiB.
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// Logical message carried on a shuffle connection.
#[derive(Debug, Clone, PartialEq)]
pub enum ShuffleMessage {
    /// A checkpoint barrier (Chandy-Lamport).
    Barrier(CheckpointBarrier),
    /// Peer identifying itself during the connection handshake.
    Hello(u64),
    /// A batch of rows pre-routed to `vnode`, tagged with the logical `stage`
    /// (the operator / MV name) it belongs to. The stage lets a receiver shared
    /// by multiple sharded operators demux frames to the correct one instead of
    /// cross-feeding them.
    VnodeData(String, u32, RecordBatch),
    /// Sender announcing graceful shutdown with a brief reason.
    Close(String),
}
