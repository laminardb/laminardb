//! Versioned, immutable change-history records.
//!
//! Operation names are data. No column uses the engine's `_op`, `_ts_ms`, or `__weight`
//! changelog names, so append and mutable sinks cannot interpret a history record as a delete.

use std::sync::Arc;

use arrow_array::builder::{
    Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use mongodb::bson::{RawBsonRef, RawDocumentBuf, Timestamp};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::super::change_event::canonical_document_extjson;
use super::buffering::{BufferedMongoPayload, ChangeRecord, SnapshotRecord};
use super::checkpoint::encode_timestamp;
use super::decoding::{decode_change, optional_extjson};
use super::ConnectorError;

/// Version of the history record layout; incremented on any incompatible change.
pub const MONGODB_HISTORY_VERSION: i32 = 1;
/// `operation` value of a document copied by the initial snapshot rather than observed.
pub const SNAPSHOT_OPERATION: &str = "snapshot";

/// Returns the Arrow schema of `MongoDB` change-history records.
///
/// | Column | Type | Nullable | Content |
/// |---|---|---|---|
/// | `event_id` | Utf8 | no | SHA-256 of deployment, collection UUID and resume token (or snapshot id and key) |
/// | `event_version` | Int32 | no | [`MONGODB_HISTORY_VERSION`] |
/// | `operation` | Utf8 | no | Server `operationType`, or `snapshot` for copied documents |
/// | `database` | Utf8 | no | Namespace database |
/// | `collection` | Utf8 | yes | Namespace collection (absent for `dropDatabase`/`invalidate`) |
/// | `collection_uuid` | Utf8 | no | Bound collection UUID |
/// | `document_key` | Utf8 | yes | Canonical Extended JSON document key |
/// | `full_document` | Utf8 | yes | Canonical Extended JSON document image |
/// | `update_description` | Utf8 | yes | Canonical Extended JSON update delta |
/// | `event_details` | Utf8 | yes | Canonical Extended JSON of every other event field |
/// | `resume_token` | Utf8 | yes | Opaque resume token JSON (absent for snapshot rows) |
/// | `cluster_time_seconds` | Int64 | no | Event cluster time, or the snapshot read time |
/// | `cluster_time_increment` | Int64 | no | Cluster time increment |
/// | `wall_time` | Timestamp(us) | yes | Server wall time (millisecond precision) |
/// | `txn_number` | Int64 | yes | Transaction number when the change belongs to a transaction |
/// | `lsid` | Utf8 | yes | Canonical Extended JSON transaction session id |
/// | `snapshot_id` | Utf8 | yes | Snapshot read time `<seconds>.<increment>` for snapshot rows |
#[must_use]
pub fn mongodb_history_schema() -> SchemaRef {
    let utf8 = |name: &str, nullable| Field::new(name, DataType::Utf8, nullable);
    Arc::new(Schema::new(vec![
        utf8("event_id", false),
        Field::new("event_version", DataType::Int32, false),
        utf8("operation", false),
        utf8("database", false),
        utf8("collection", true),
        utf8("collection_uuid", false),
        utf8("document_key", true),
        utf8("full_document", true),
        utf8("update_description", true),
        utf8("event_details", true),
        utf8("resume_token", true),
        Field::new("cluster_time_seconds", DataType::Int64, false),
        Field::new("cluster_time_increment", DataType::Int64, false),
        Field::new(
            "wall_time",
            // Microseconds: lakehouse formats such as Iceberg have no millisecond timestamp.
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("txn_number", DataType::Int64, true),
        utf8("lsid", true),
        utf8("snapshot_id", true),
    ]))
}

/// Physical identity bound before the first record is emitted.
pub(super) struct HistoryIdentity<'a> {
    pub(super) deployment: &'a str,
    pub(super) collection_uuid: Uuid,
    pub(super) database: &'a str,
    pub(super) collection: &'a str,
}

impl HistoryIdentity<'_> {
    fn digest(&self, kind: &[u8], parts: &[&[u8]]) -> String {
        let mut digest = Sha256::new();
        digest.update(b"laminardb-mongodb-event-v1\0");
        digest.update(kind);
        digest.update(self.deployment.as_bytes());
        digest.update(self.collection_uuid.as_bytes());
        for part in parts {
            digest.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_be_bytes());
            digest.update(part);
        }
        format!("{:x}", digest.finalize())
    }
}

struct HistoryBuilders {
    event_id: StringBuilder,
    version: Int32Builder,
    operation: StringBuilder,
    database: StringBuilder,
    collection: StringBuilder,
    collection_uuid: StringBuilder,
    document_key: StringBuilder,
    full_document: StringBuilder,
    update_description: StringBuilder,
    details: StringBuilder,
    resume_token: StringBuilder,
    seconds: Int64Builder,
    increment: Int64Builder,
    wall_time: TimestampMicrosecondBuilder,
    txn_number: Int64Builder,
    lsid: StringBuilder,
    snapshot_id: StringBuilder,
}

impl HistoryBuilders {
    fn with_capacity(rows: usize) -> Self {
        let text = || StringBuilder::with_capacity(rows, rows * 64);
        Self {
            event_id: StringBuilder::with_capacity(rows, rows * 64),
            version: Int32Builder::with_capacity(rows),
            operation: text(),
            database: text(),
            collection: text(),
            collection_uuid: StringBuilder::with_capacity(rows, rows * 36),
            document_key: text(),
            full_document: StringBuilder::with_capacity(rows, rows * 256),
            update_description: text(),
            details: text(),
            resume_token: text(),
            seconds: Int64Builder::with_capacity(rows),
            increment: Int64Builder::with_capacity(rows),
            wall_time: TimestampMicrosecondBuilder::with_capacity(rows),
            txn_number: Int64Builder::with_capacity(rows),
            lsid: text(),
            snapshot_id: text(),
        }
    }

    fn append_time(&mut self, at: Timestamp) {
        self.seconds.append_value(i64::from(at.time));
        self.increment.append_value(i64::from(at.increment));
    }

    fn append_change(
        &mut self,
        record: &ChangeRecord,
        identity: &HistoryIdentity<'_>,
    ) -> Result<(), ConnectorError> {
        let change = decode_change(&record.raw)?;
        if change
            .collection_uuid
            .is_some_and(|uuid| uuid != identity.collection_uuid)
        {
            return Err(ConnectorError::ReadError(format!(
                "MongoDB change event for {}.{} carries a different collection UUID than the \
                 bound collection {}",
                identity.database, identity.collection, identity.collection_uuid
            )));
        }
        let cluster_time = change.cluster_time.ok_or_else(|| {
            ConnectorError::ReadError("MongoDB change event omitted clusterTime".into())
        })?;
        self.event_id
            .append_value(identity.digest(b"change\0", &[record.token.as_bytes()]));
        self.version.append_value(MONGODB_HISTORY_VERSION);
        self.operation.append_value(change.operation);
        self.database
            .append_value(change.database.unwrap_or(identity.database));
        self.collection.append_option(change.collection);
        self.collection_uuid
            .append_value(identity.collection_uuid.hyphenated().to_string());
        self.document_key
            .append_option(optional_extjson(change.document_key)?);
        self.full_document
            .append_option(optional_extjson(change.full_document)?);
        self.update_description
            .append_option(optional_extjson(change.update_description)?);
        self.details
            .append_option(optional_extjson(change.details.as_deref())?);
        self.resume_token.append_value(&record.token);
        self.append_time(cluster_time);
        self.wall_time.append_option(change.wall_time_us);
        self.txn_number.append_option(change.txn_number);
        self.lsid.append_option(optional_extjson(change.lsid)?);
        self.snapshot_id.append_null();
        Ok(())
    }

    fn append_snapshot(
        &mut self,
        record: &SnapshotRecord,
        snapshot_at: Timestamp,
        identity: &HistoryIdentity<'_>,
    ) -> Result<(), ConnectorError> {
        let snapshot_id = encode_timestamp(snapshot_at);
        let id = record
            .raw
            .get("_id")
            .map_err(|error| ConnectorError::ReadError(format!("snapshot document _id: {error}")))?
            .ok_or_else(|| ConnectorError::ReadError("snapshot document has no _id".into()))?;
        let key = id_key(id);
        self.event_id.append_value(identity.digest(
            b"snapshot\0",
            &[snapshot_id.as_bytes(), record.key.as_bytes()],
        ));
        self.version.append_value(MONGODB_HISTORY_VERSION);
        self.operation.append_value(SNAPSHOT_OPERATION);
        self.database.append_value(identity.database);
        self.collection.append_value(identity.collection);
        self.collection_uuid
            .append_value(identity.collection_uuid.hyphenated().to_string());
        self.document_key
            .append_value(canonical_document_extjson(&key)?);
        self.full_document
            .append_value(canonical_document_extjson(&record.raw)?);
        self.update_description.append_null();
        self.details.append_null();
        self.resume_token.append_null();
        self.append_time(snapshot_at);
        self.wall_time.append_null();
        self.txn_number.append_null();
        self.lsid.append_null();
        self.snapshot_id.append_value(snapshot_id);
        Ok(())
    }

    fn finish(mut self, schema: &SchemaRef) -> Result<RecordBatch, ConnectorError> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.event_id.finish()),
            Arc::new(self.version.finish()),
            Arc::new(self.operation.finish()),
            Arc::new(self.database.finish()),
            Arc::new(self.collection.finish()),
            Arc::new(self.collection_uuid.finish()),
            Arc::new(self.document_key.finish()),
            Arc::new(self.full_document.finish()),
            Arc::new(self.update_description.finish()),
            Arc::new(self.details.finish()),
            Arc::new(self.resume_token.finish()),
            Arc::new(self.seconds.finish()),
            Arc::new(self.increment.finish()),
            Arc::new(self.wall_time.finish()),
            Arc::new(self.txn_number.finish()),
            Arc::new(self.lsid.finish()),
            Arc::new(self.snapshot_id.finish()),
        ];
        RecordBatch::try_new(Arc::clone(schema), columns)
            .map_err(|error| ConnectorError::Internal(format!("MongoDB history batch: {error}")))
    }
}

/// Build one history batch from record payloads in their emitted order.
pub(super) fn history_batch<'a>(
    records: impl ExactSizeIterator<Item = &'a BufferedMongoPayload>,
    schema: &SchemaRef,
    identity: &HistoryIdentity<'_>,
    snapshot_at: Option<Timestamp>,
) -> Result<RecordBatch, ConnectorError> {
    let mut builders = HistoryBuilders::with_capacity(records.len());
    for payload in records {
        match payload {
            BufferedMongoPayload::Change(record) => builders.append_change(record, identity)?,
            BufferedMongoPayload::Snapshot(record) => {
                let at = snapshot_at.ok_or_else(|| {
                    ConnectorError::Internal("snapshot row without a snapshot time".into())
                })?;
                builders.append_snapshot(record, at, identity)?;
            }
            BufferedMongoPayload::HighWatermark { .. } | BufferedMongoPayload::SnapshotComplete => {
                return Err(ConnectorError::Internal(
                    "MongoDB history batch received a progress marker as a record".into(),
                ));
            }
        }
    }
    builders.finish(schema)
}

/// `{ "_id": <value> }`, the document key of a replica-set collection.
pub(super) fn id_key(id: RawBsonRef<'_>) -> RawDocumentBuf {
    let mut key = RawDocumentBuf::new();
    key.append("_id", id.to_raw_bson());
    key
}
