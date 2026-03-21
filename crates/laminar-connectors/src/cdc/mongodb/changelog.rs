//! MongoDB CDC changelog conversion.
//!
//! Converts MongoDB change stream events into CDC change events compatible
//! with LaminarDB's Z-set changelog format, then serializes to Arrow
//! `RecordBatch`es.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

/// CDC operation types for MongoDB change events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOperation {
    /// Insert operation.
    Insert,
    /// Update operation.
    Update,
    /// Replace operation.
    Replace,
    /// Delete operation.
    Delete,
}

impl CdcOperation {
    /// Returns the operation code as a string.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            CdcOperation::Insert => "I",
            CdcOperation::Update => "U",
            CdcOperation::Replace => "R",
            CdcOperation::Delete => "D",
        }
    }

    /// Returns the Z-set weight for this operation.
    #[must_use]
    pub fn weight(&self) -> i8 {
        match self {
            CdcOperation::Insert | CdcOperation::Update | CdcOperation::Replace => 1,
            CdcOperation::Delete => -1,
        }
    }

    /// Parses a MongoDB change stream `operationType` string.
    #[must_use]
    pub fn from_operation_type(op: &str) -> Option<Self> {
        match op {
            "insert" => Some(CdcOperation::Insert),
            "update" => Some(CdcOperation::Update),
            "replace" => Some(CdcOperation::Replace),
            "delete" => Some(CdcOperation::Delete),
            _ => None,
        }
    }
}

/// A CDC change event from MongoDB change stream.
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    /// Source collection (database.collection).
    pub collection: String,
    /// Operation type.
    pub operation: CdcOperation,
    /// Timestamp in milliseconds since Unix epoch.
    pub timestamp_ms: i64,
    /// Document ID as JSON string.
    pub document_key: String,
    /// Full document as JSON string (for insert/update/replace).
    pub full_document: Option<String>,
    /// Full document before the change (if pre-image enabled).
    pub full_document_before_change: Option<String>,
    /// Update description as JSON (for update operations).
    pub update_description: Option<String>,
    /// Resume token as JSON string.
    pub resume_token: String,
}

/// Builds the CDC envelope schema for MongoDB CDC records.
///
/// The envelope wraps the actual document data with CDC metadata:
/// - `_collection`: Source collection name
/// - `_op`: Operation type (I/U/R/D)
/// - `_ts_ms`: Event timestamp (milliseconds since epoch)
/// - `_document_key`: Document ID as JSON
/// - `_full_document`: Full document as JSON
/// - `_full_document_before`: Pre-image as JSON
/// - `_update_description`: Update description as JSON
/// - `_resume_token`: Resume token for checkpointing
#[must_use]
pub fn cdc_envelope_schema() -> Schema {
    Schema::new(vec![
        Field::new("_collection", DataType::Utf8, false),
        Field::new("_op", DataType::Utf8, false),
        Field::new("_ts_ms", DataType::Int64, false),
        Field::new("_document_key", DataType::Utf8, false),
        Field::new("_full_document", DataType::Utf8, true),
        Field::new("_full_document_before", DataType::Utf8, true),
        Field::new("_update_description", DataType::Utf8, true),
        Field::new("_resume_token", DataType::Utf8, false),
    ])
}

/// Converts change events to a `RecordBatch` with the CDC envelope schema.
///
/// # Errors
///
/// Returns error if Arrow batch construction fails.
pub fn events_to_record_batch(
    events: &[ChangeEvent],
) -> Result<RecordBatch, arrow_schema::ArrowError> {
    let schema = Arc::new(cdc_envelope_schema());

    if events.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let collections: Vec<&str> = events.iter().map(|e| e.collection.as_str()).collect();
    let ops: Vec<&str> = events.iter().map(|e| e.operation.as_str()).collect();
    let timestamps: Vec<i64> = events.iter().map(|e| e.timestamp_ms).collect();
    let doc_keys: Vec<&str> = events.iter().map(|e| e.document_key.as_str()).collect();
    let full_docs: Vec<Option<&str>> = events.iter().map(|e| e.full_document.as_deref()).collect();
    let full_docs_before: Vec<Option<&str>> = events
        .iter()
        .map(|e| e.full_document_before_change.as_deref())
        .collect();
    let update_descs: Vec<Option<&str>> = events
        .iter()
        .map(|e| e.update_description.as_deref())
        .collect();
    let resume_tokens: Vec<&str> = events.iter().map(|e| e.resume_token.as_str()).collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(collections)),
        Arc::new(StringArray::from(ops)),
        Arc::new(Int64Array::from(timestamps)),
        Arc::new(StringArray::from(doc_keys)),
        Arc::new(StringArray::from(full_docs)),
        Arc::new(StringArray::from(full_docs_before)),
        Arc::new(StringArray::from(update_descs)),
        Arc::new(StringArray::from(resume_tokens)),
    ];

    RecordBatch::try_new(schema, columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_event(op: CdcOperation) -> ChangeEvent {
        ChangeEvent {
            collection: "testdb.users".to_string(),
            operation: op,
            timestamp_ms: 1_704_067_200_000,
            document_key: r#"{"_id": "abc123"}"#.to_string(),
            full_document: Some(r#"{"_id": "abc123", "name": "Alice"}"#.to_string()),
            full_document_before_change: None,
            update_description: None,
            resume_token: r#"{"_data": "token123"}"#.to_string(),
        }
    }

    #[test]
    fn test_cdc_operation_as_str() {
        assert_eq!(CdcOperation::Insert.as_str(), "I");
        assert_eq!(CdcOperation::Update.as_str(), "U");
        assert_eq!(CdcOperation::Replace.as_str(), "R");
        assert_eq!(CdcOperation::Delete.as_str(), "D");
    }

    #[test]
    fn test_cdc_operation_weight() {
        assert_eq!(CdcOperation::Insert.weight(), 1);
        assert_eq!(CdcOperation::Update.weight(), 1);
        assert_eq!(CdcOperation::Replace.weight(), 1);
        assert_eq!(CdcOperation::Delete.weight(), -1);
    }

    #[test]
    fn test_cdc_operation_from_operation_type() {
        assert_eq!(
            CdcOperation::from_operation_type("insert"),
            Some(CdcOperation::Insert)
        );
        assert_eq!(
            CdcOperation::from_operation_type("update"),
            Some(CdcOperation::Update)
        );
        assert_eq!(
            CdcOperation::from_operation_type("replace"),
            Some(CdcOperation::Replace)
        );
        assert_eq!(
            CdcOperation::from_operation_type("delete"),
            Some(CdcOperation::Delete)
        );
        assert_eq!(CdcOperation::from_operation_type("drop"), None);
        assert_eq!(CdcOperation::from_operation_type("invalidate"), None);
    }

    #[test]
    fn test_cdc_envelope_schema() {
        let schema = cdc_envelope_schema();
        assert_eq!(schema.fields().len(), 8);
        assert_eq!(schema.field(0).name(), "_collection");
        assert_eq!(schema.field(1).name(), "_op");
        assert_eq!(schema.field(2).name(), "_ts_ms");
        assert_eq!(schema.field(3).name(), "_document_key");
        assert_eq!(schema.field(4).name(), "_full_document");
        assert_eq!(schema.field(5).name(), "_full_document_before");
        assert_eq!(schema.field(6).name(), "_update_description");
        assert_eq!(schema.field(7).name(), "_resume_token");
    }

    #[test]
    fn test_events_to_record_batch_empty() {
        let batch = events_to_record_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 8);
    }

    #[test]
    fn test_events_to_record_batch_insert() {
        let events = vec![make_test_event(CdcOperation::Insert)];
        let batch = events_to_record_batch(&events).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 8);

        let collections = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(collections.value(0), "testdb.users");

        let ops = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ops.value(0), "I");
    }

    #[test]
    fn test_events_to_record_batch_multiple() {
        let events = vec![
            make_test_event(CdcOperation::Insert),
            make_test_event(CdcOperation::Update),
            make_test_event(CdcOperation::Delete),
        ];
        let batch = events_to_record_batch(&events).unwrap();
        assert_eq!(batch.num_rows(), 3);

        let ops = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ops.value(0), "I");
        assert_eq!(ops.value(1), "U");
        assert_eq!(ops.value(2), "D");
    }

    #[test]
    fn test_events_to_record_batch_with_nulls() {
        use arrow_array::Array;
        let event = ChangeEvent {
            collection: "testdb.users".to_string(),
            operation: CdcOperation::Delete,
            timestamp_ms: 1_704_067_200_000,
            document_key: r#"{"_id": "abc123"}"#.to_string(),
            full_document: None,
            full_document_before_change: None,
            update_description: None,
            resume_token: r#"{"_data": "token123"}"#.to_string(),
        };

        let batch = events_to_record_batch(&[event]).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let full_docs = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(full_docs.is_null(0));
    }
}
