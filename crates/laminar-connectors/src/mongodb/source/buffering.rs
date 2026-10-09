//! Exact retained-memory ownership for raw change events and snapshot documents.

use std::mem::size_of;

use mongodb::bson::RawDocumentBuf;
use tokio::sync::OwnedSemaphorePermit;

use super::super::change_event::ChangeOperation;
use super::ConnectorError;

/// One change event exactly as the server returned it.
pub(super) struct ChangeRecord {
    pub(super) raw: RawDocumentBuf,
    /// Canonical JSON of the event's own `_id` resume token.
    pub(super) token: String,
    pub(super) operation: ChangeOperation,
}

/// One document copied by the initial snapshot scan.
pub(super) struct SnapshotRecord {
    pub(super) raw: RawDocumentBuf,
    /// Canonical Extended JSON of the document `_id`; the scan resumes after it.
    pub(super) key: String,
}

pub(super) enum BufferedMongoPayload {
    Change(ChangeRecord),
    Snapshot(SnapshotRecord),
    /// Post-batch progress with no event. It never overtakes queued records.
    HighWatermark {
        token: String,
        requires_start_after: bool,
    },
    /// The snapshot scan finished; the stream continues from the snapshot time.
    SnapshotComplete,
}

pub(super) struct BufferedMongoEvent {
    pub(super) payload: BufferedMongoPayload,
    _byte_permit: OwnedSemaphorePermit,
}

impl BufferedMongoEvent {
    pub(super) fn new(payload: BufferedMongoPayload, byte_permit: OwnedSemaphorePermit) -> Self {
        Self {
            payload,
            _byte_permit: byte_permit,
        }
    }

    /// Whether this item becomes an output row.
    pub(super) fn is_record(&self) -> bool {
        matches!(
            self.payload,
            BufferedMongoPayload::Change(_) | BufferedMongoPayload::Snapshot(_)
        )
    }

    pub(super) fn is_invalidate(&self) -> bool {
        matches!(
            &self.payload,
            BufferedMongoPayload::Change(record)
                if record.operation == ChangeOperation::Invalidate
        )
    }
}

/// Bytes owned by one buffered item: the exact BSON bytes plus its fixed and string overhead.
pub(super) fn buffered_retained_bytes(
    payload: &BufferedMongoPayload,
) -> Result<usize, ConnectorError> {
    let variable = match payload {
        BufferedMongoPayload::Change(record) => record
            .raw
            .as_bytes()
            .len()
            .checked_add(record.token.capacity()),
        BufferedMongoPayload::Snapshot(record) => record
            .raw
            .as_bytes()
            .len()
            .checked_add(record.key.capacity()),
        BufferedMongoPayload::HighWatermark { token, .. } => Some(token.capacity()),
        BufferedMongoPayload::SnapshotComplete => Some(0),
    };
    variable
        .and_then(|bytes| bytes.checked_add(size_of::<BufferedMongoEvent>()))
        .ok_or_else(|| ConnectorError::ConfigurationError("MongoDB CDC item size overflow".into()))
}
