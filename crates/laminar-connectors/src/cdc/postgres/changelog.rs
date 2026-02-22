//! Z-set changelog integration for `PostgreSQL` CDC events.
//!
//! Converts decoded `pgoutput` DML messages into change events compatible
//! with `LaminarDB`'s changelog/retraction model (Z-sets).
//!
//! # Z-Set Semantics
//!
//! - INSERT → weight +1 (new row)
//! - DELETE → weight -1 (retracted row)
//! - UPDATE → weight -1 (old row) + weight +1 (new row)

use std::sync::Arc;

use arrow_array::builder::{Int64Builder, StringBuilder, UInt64Builder};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use super::decoder::{ColumnValue, TupleData};
use super::lsn::Lsn;
use super::schema::{cdc_envelope_schema, RelationInfo};

/// A CDC operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOperation {
    /// Row inserted (Z-set weight +1).
    Insert,
    /// Row updated (produces -1/+1 pair).
    Update,
    /// Row deleted (Z-set weight -1).
    Delete,
}

impl CdcOperation {
    /// Returns the single-character code for the operation.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            CdcOperation::Insert => "I",
            CdcOperation::Update => "U",
            CdcOperation::Delete => "D",
        }
    }
}

/// A single change event from CDC.
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    /// Fully qualified table name.
    pub table: String,
    /// The operation type.
    pub op: CdcOperation,
    /// WAL position of this change.
    pub lsn: Lsn,
    /// Commit timestamp in milliseconds since Unix epoch.
    pub ts_ms: i64,
    /// Old row values as JSON (for UPDATE and DELETE).
    pub before: Option<String>,
    /// New row values as JSON (for INSERT and UPDATE).
    pub after: Option<String>,
}

/// Converts tuple data to a JSON string using column names from the relation.
///
/// Produces a flat JSON object like `{"id": "42", "name": "Alice"}`.
/// All values are serialized as strings (matching `pgoutput` text format).
///
/// Writes JSON directly to a `String` buffer instead of building an
/// intermediate `HashMap`, avoiding per-row map + key/value cloning.
#[must_use]
pub fn tuple_to_json(tuple: &TupleData, relation: &RelationInfo) -> String {
    let mut buf = String::with_capacity(128);
    buf.push('{');
    let mut first = true;
    for (i, col_val) in tuple.columns.iter().enumerate() {
        if let Some(col_info) = relation.columns.get(i) {
            let val = match col_val {
                ColumnValue::Text(s) => Some(s.as_str()),
                ColumnValue::Null => None,
                ColumnValue::Unchanged => continue,
            };
            if !first {
                buf.push(',');
            }
            first = false;
            // Write key (escape quotes in column names)
            buf.push('"');
            escape_json_str(&col_info.name, &mut buf);
            buf.push('"');
            buf.push(':');
            // Write value
            match val {
                Some(s) => {
                    buf.push('"');
                    escape_json_str(s, &mut buf);
                    buf.push('"');
                }
                None => buf.push_str("null"),
            }
        }
    }
    buf.push('}');
    buf
}

/// Escapes a string for JSON output (quotes and control characters).
fn escape_json_str(s: &str, buf: &mut String) {
    for ch in s.chars() {
        match ch {
            '"' => buf.push_str("\\\""),
            '\\' => buf.push_str("\\\\"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\t' => buf.push_str("\\t"),
            c if c.is_control() => {
                use std::fmt::Write;
                let _ = write!(buf, "\\u{:04x}", c as u32);
            }
            c => buf.push(c),
        }
    }
}

/// Converts a batch of [`ChangeEvent`]s into an Arrow [`RecordBatch`]
/// using the CDC envelope schema.
///
/// # Errors
///
/// Returns an error if the Arrow batch construction fails.
pub fn events_to_record_batch(
    events: &[ChangeEvent],
) -> Result<RecordBatch, arrow_schema::ArrowError> {
    let schema: SchemaRef = cdc_envelope_schema();

    let mut table_builder = StringBuilder::with_capacity(events.len(), events.len() * 32);
    let mut op_builder = StringBuilder::with_capacity(events.len(), events.len());
    let mut lsn_builder = UInt64Builder::with_capacity(events.len());
    let mut ts_builder = Int64Builder::with_capacity(events.len());
    let mut before_builder = StringBuilder::with_capacity(events.len(), events.len() * 64);
    let mut after_builder = StringBuilder::with_capacity(events.len(), events.len() * 64);

    for event in events {
        table_builder.append_value(&event.table);
        op_builder.append_value(event.op.as_str());
        lsn_builder.append_value(event.lsn.as_u64());
        ts_builder.append_value(event.ts_ms);

        match &event.before {
            Some(json) => before_builder.append_value(json),
            None => before_builder.append_null(),
        }
        match &event.after {
            Some(json) => after_builder.append_value(json),
            None => after_builder.append_null(),
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(table_builder.finish()),
            Arc::new(op_builder.finish()),
            Arc::new(lsn_builder.finish()),
            Arc::new(ts_builder.finish()),
            Arc::new(before_builder.finish()),
            Arc::new(after_builder.finish()),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::postgres::schema::RelationInfo;
    use crate::cdc::postgres::types::PgColumn;
    use crate::cdc::postgres::types::{INT8_OID, TEXT_OID};

    fn sample_relation() -> RelationInfo {
        RelationInfo {
            relation_id: 16384,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: 'd',
            columns: vec![
                PgColumn::new("id".to_string(), INT8_OID, -1, true),
                PgColumn::new("name".to_string(), TEXT_OID, -1, false),
            ],
        }
    }

    #[test]
    fn test_tuple_to_json() {
        let relation = sample_relation();
        let tuple = TupleData {
            columns: vec![
                ColumnValue::Text("42".to_string()),
                ColumnValue::Text("Alice".to_string()),
            ],
        };

        let json = tuple_to_json(&tuple, &relation);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["id"], "42");
        assert_eq!(parsed["name"], "Alice");
    }

    #[test]
    fn test_tuple_to_json_with_null() {
        let relation = sample_relation();
        let tuple = TupleData {
            columns: vec![ColumnValue::Text("42".to_string()), ColumnValue::Null],
        };

        let json = tuple_to_json(&tuple, &relation);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["id"], "42");
        assert!(parsed["name"].is_null());
    }

    #[test]
    fn test_tuple_to_json_unchanged_omitted() {
        let relation = sample_relation();
        let tuple = TupleData {
            columns: vec![ColumnValue::Text("42".to_string()), ColumnValue::Unchanged],
        };

        let json = tuple_to_json(&tuple, &relation);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["id"], "42");
        // unchanged columns are omitted
        assert!(parsed.get("name").is_none());
    }

    #[test]
    fn test_events_to_record_batch_insert() {
        let events = vec![ChangeEvent {
            table: "users".to_string(),
            op: CdcOperation::Insert,
            lsn: Lsn::new(0x100),
            ts_ms: 1_700_000_000_000,
            before: None,
            after: Some(r#"{"id":"1","name":"Alice"}"#.to_string()),
        }];

        let batch = events_to_record_batch(&events).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 6);
    }

    #[test]
    fn test_events_to_record_batch_mixed() {
        let events = vec![
            ChangeEvent {
                table: "users".to_string(),
                op: CdcOperation::Insert,
                lsn: Lsn::new(0x100),
                ts_ms: 1_700_000_000_000,
                before: None,
                after: Some(r#"{"id":"1"}"#.to_string()),
            },
            ChangeEvent {
                table: "users".to_string(),
                op: CdcOperation::Update,
                lsn: Lsn::new(0x200),
                ts_ms: 1_700_000_000_001,
                before: Some(r#"{"id":"1","name":"Alice"}"#.to_string()),
                after: Some(r#"{"id":"1","name":"Bob"}"#.to_string()),
            },
            ChangeEvent {
                table: "users".to_string(),
                op: CdcOperation::Delete,
                lsn: Lsn::new(0x300),
                ts_ms: 1_700_000_000_002,
                before: Some(r#"{"id":"1"}"#.to_string()),
                after: None,
            },
        ];

        let batch = events_to_record_batch(&events).unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_events_to_record_batch_empty() {
        let events: Vec<ChangeEvent> = vec![];
        let batch = events_to_record_batch(&events).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 6);
    }

    #[test]
    fn test_cdc_operation_as_str() {
        assert_eq!(CdcOperation::Insert.as_str(), "I");
        assert_eq!(CdcOperation::Update.as_str(), "U");
        assert_eq!(CdcOperation::Delete.as_str(), "D");
    }
}
