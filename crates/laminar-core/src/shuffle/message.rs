//! Logical messages carried by the ordered shuffle transport.
//!
//! The data/barrier wire encoding is gRPC/protobuf (`proto/shuffle.proto`, `ShuffleFrame`);
//! transport identity is internal to [`super::transport`]. A data message's batch is
//! Arrow IPC-encoded as one self-contained logical payload. Wire fragmentation
//! slices that allocation without copying it or retaining per-stage codec state.

use std::sync::Arc;

use arrow_array::RecordBatch;

use crate::checkpoint::barrier::CheckpointBarrier;

/// Maximum reassembled Arrow IPC payload for one logical shuffled batch: 16 MiB.
pub const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// Logical message carried on a shuffle connection.
#[derive(Debug, Clone, PartialEq)]
pub enum ShuffleMessage {
    /// An aligned checkpoint barrier ordered after preceding data and frontiers.
    Barrier(CheckpointBarrier),
    /// Per-stage event-time progress ordered with the stage's data.
    Frontier {
        /// Stable stage demultiplexing scope.
        stage: String,
        /// Current watermark, or `None` while the channel is uninitialized.
        watermark: Option<i64>,
        /// Whether the channel is excluded from downstream watermark minima.
        idle: bool,
    },
    /// A stage batch with a non-empty canonical route set.
    Data {
        /// Stable stage demultiplexing scope.
        stage: String,
        /// Ascending, duplicate-free receiver-owned vnode route set.
        routed_vnodes: Arc<[u32]>,
        /// User batch, with its schema left unchanged.
        batch: RecordBatch,
    },
}

impl ShuffleMessage {
    /// Construct stateful data covered by checkpoint delivery accounting.
    #[must_use]
    pub fn checkpointed(stage: String, vnode: u32, batch: RecordBatch) -> Self {
        Self::checkpointed_routed(stage, Arc::from([vnode]), batch)
    }

    /// Construct owner-coalesced stateful data with canonical out-of-band vnode metadata.
    #[must_use]
    pub fn checkpointed_routed(
        stage: String,
        routed_vnodes: Arc<[u32]>,
        batch: RecordBatch,
    ) -> Self {
        Self::Data {
            stage,
            routed_vnodes,
            batch,
        }
    }
}
