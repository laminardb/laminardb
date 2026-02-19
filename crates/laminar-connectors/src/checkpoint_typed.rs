//! Typed checkpoint extension for source connectors.
//!
//! The [`CheckpointableSource`] trait extends source connectors with
//! type-safe position tracking, enabling strongly typed checkpoint
//! storage and recovery planning.

use laminar_storage::{SourceId, SourcePosition};

use crate::error::ConnectorError;

/// Extension trait for source connectors that support typed checkpointing.
///
/// Implementors provide a [`SourcePosition`] that captures the connector's
/// native offset format, rather than the generic string-based
/// [`SourceCheckpoint`](crate::checkpoint::SourceCheckpoint).
///
/// This is an EXTENSION of the existing [`SourceConnector`](crate::connector::SourceConnector)
/// trait's `checkpoint()`/`restore()` methods. Connectors that implement
/// both traits get type-safe offset storage for free.
pub trait CheckpointableSource: Send + Sync {
    /// Get the source's unique identifier within the pipeline.
    fn source_id(&self) -> &SourceId;

    /// Return the current position as a typed [`SourcePosition`].
    ///
    /// Called when a checkpoint barrier reaches the source operator.
    /// The returned position will be included in the checkpoint manifest.
    ///
    /// This method MUST be called AFTER the last event emitted before
    /// the barrier, and BEFORE the first event emitted after the barrier.
    fn typed_position(&self) -> SourcePosition;

    /// Restore the connector to the given typed position.
    ///
    /// Called during recovery to seek the source back to its checkpoint
    /// position. After restoring, the next event read from the source
    /// will be the first event AFTER the checkpointed offset.
    ///
    /// # Errors
    ///
    /// Returns an error if the position is incompatible with this connector
    /// or the underlying system rejects the seek.
    fn restore_typed(&mut self, position: &SourcePosition) -> Result<(), ConnectorError>;

    /// Validate that the source can seek to the given position.
    ///
    /// Called before committing to a recovery plan. Returns `false` if
    /// the position is no longer reachable (e.g., Kafka topic compacted
    /// past the offset, replication slot dropped, file deleted).
    ///
    /// Default implementation returns `true` (optimistic).
    fn can_seek_to(&self, _position: &SourcePosition) -> bool {
        true
    }
}
