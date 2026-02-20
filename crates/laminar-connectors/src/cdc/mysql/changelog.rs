//! MySQL CDC changelog conversion to Z-set format.
//!
//! Converts MySQL binlog row events into CDC change events compatible
//! with LaminarDB's Z-set changelog format.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};

use super::decoder::{
    ColumnValue, DeleteMessage, InsertMessage, RowData, UpdateMessage, UpdateRowData,
};
use super::schema::TableInfo;

/// CDC operation types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOperation {
    /// Insert operation.
    Insert,
    /// Update operation (before image).
    UpdateBefore,
    /// Update operation (after image).
    UpdateAfter,
    /// Delete operation.
    Delete,
}

impl CdcOperation {
    /// Returns the operation code as a string.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            CdcOperation::Insert => "I",
            CdcOperation::UpdateBefore => "U-",
            CdcOperation::UpdateAfter => "U+",
            CdcOperation::Delete => "D",
        }
    }

    /// Returns the Z-set weight for this operation.
    #[must_use]
    pub fn weight(&self) -> i8 {
        match self {
            CdcOperation::Insert | CdcOperation::UpdateAfter => 1,
            CdcOperation::Delete | CdcOperation::UpdateBefore => -1,
        }
    }
}

/// A CDC change event from MySQL binlog.
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    /// Source table (database.table).
    pub table: String,
    /// Operation type.
    pub operation: CdcOperation,
    /// Timestamp in milliseconds since Unix epoch.
    pub timestamp_ms: i64,
    /// Binlog filename.
    pub binlog_file: String,
    /// Binlog position.
    pub binlog_position: u64,
    /// GTID (if available).
    pub gtid: Option<String>,
    /// Row data.
    pub row: RowData,
}

impl ChangeEvent {
    /// Creates an insert change event.
    #[must_use]
    pub fn insert(
        table: String,
        timestamp_ms: i64,
        binlog_file: String,
        binlog_position: u64,
        gtid: Option<String>,
        row: RowData,
    ) -> Self {
        Self {
            table,
            operation: CdcOperation::Insert,
            timestamp_ms,
            binlog_file,
            binlog_position,
            gtid,
            row,
        }
    }

    /// Creates a delete change event.
    #[must_use]
    pub fn delete(
        table: String,
        timestamp_ms: i64,
        binlog_file: String,
        binlog_position: u64,
        gtid: Option<String>,
        row: RowData,
    ) -> Self {
        Self {
            table,
            operation: CdcOperation::Delete,
            timestamp_ms,
            binlog_file,
            binlog_position,
            gtid,
            row,
        }
    }
}

/// Converts an INSERT message to change events.
#[must_use]
pub fn insert_to_events(
    msg: &InsertMessage,
    binlog_file: &str,
    gtid: Option<&str>,
) -> Vec<ChangeEvent> {
    let table = format!("{}.{}", msg.database, msg.table);

    msg.rows
        .iter()
        .map(|row| {
            ChangeEvent::insert(
                table.clone(),
                msg.timestamp_ms,
                binlog_file.to_string(),
                msg.binlog_position,
                gtid.map(String::from),
                row.clone(),
            )
        })
        .collect()
}

/// Converts an UPDATE message to change events.
///
/// Each UPDATE row generates two events:
/// - U- (before image, weight -1)
/// - U+ (after image, weight +1)
#[must_use]
pub fn update_to_events(
    msg: &UpdateMessage,
    binlog_file: &str,
    gtid: Option<&str>,
) -> Vec<ChangeEvent> {
    let table = format!("{}.{}", msg.database, msg.table);
    let mut events = Vec::with_capacity(msg.rows.len() * 2);

    for UpdateRowData { before, after } in &msg.rows {
        // Before image (retraction)
        events.push(ChangeEvent {
            table: table.clone(),
            operation: CdcOperation::UpdateBefore,
            timestamp_ms: msg.timestamp_ms,
            binlog_file: binlog_file.to_string(),
            binlog_position: msg.binlog_position,
            gtid: gtid.map(String::from),
            row: before.clone(),
        });

        // After image (insertion)
        events.push(ChangeEvent {
            table: table.clone(),
            operation: CdcOperation::UpdateAfter,
            timestamp_ms: msg.timestamp_ms,
            binlog_file: binlog_file.to_string(),
            binlog_position: msg.binlog_position,
            gtid: gtid.map(String::from),
            row: after.clone(),
        });
    }

    events
}

/// Converts a DELETE message to change events.
#[must_use]
pub fn delete_to_events(
    msg: &DeleteMessage,
    binlog_file: &str,
    gtid: Option<&str>,
) -> Vec<ChangeEvent> {
    let table = format!("{}.{}", msg.database, msg.table);

    msg.rows
        .iter()
        .map(|row| {
            ChangeEvent::delete(
                table.clone(),
                msg.timestamp_ms,
                binlog_file.to_string(),
                msg.binlog_position,
                gtid.map(String::from),
                row.clone(),
            )
        })
        .collect()
}

/// Schema for CDC metadata columns.
#[must_use]
pub fn cdc_metadata_schema() -> Schema {
    Schema::new(vec![
        Field::new("_table", DataType::Utf8, false),
        Field::new("_op", DataType::Utf8, false),
        Field::new("_ts_ms", DataType::Int64, false),
        Field::new("_binlog_file", DataType::Utf8, true),
        Field::new("_binlog_pos", DataType::UInt64, true),
        Field::new("_gtid", DataType::Utf8, true),
    ])
}

/// Converts change events to a `RecordBatch` with CDC metadata + row data.
///
/// # Errors
///
/// Returns error if schema conversion fails.
pub fn events_to_record_batch(
    events: &[ChangeEvent],
    table_info: &TableInfo,
) -> Result<RecordBatch, arrow_schema::ArrowError> {
    if events.is_empty() {
        // Return empty batch with correct schema
        let schema = build_cdc_schema(&table_info.arrow_schema);
        return Ok(RecordBatch::new_empty(Arc::new(schema)));
    }

    let _n = events.len();

    // CDC metadata columns
    let tables: Vec<&str> = events.iter().map(|e| e.table.as_str()).collect();
    let ops: Vec<&str> = events.iter().map(|e| e.operation.as_str()).collect();
    let timestamps: Vec<i64> = events.iter().map(|e| e.timestamp_ms).collect();
    let binlog_files: Vec<Option<&str>> = events
        .iter()
        .map(|e| Some(e.binlog_file.as_str()))
        .collect();
    let binlog_positions: Vec<Option<u64>> =
        events.iter().map(|e| Some(e.binlog_position)).collect();
    let gtids: Vec<Option<&str>> = events.iter().map(|e| e.gtid.as_deref()).collect();

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(tables)),
        Arc::new(StringArray::from(ops)),
        Arc::new(Int64Array::from(timestamps)),
        Arc::new(StringArray::from(binlog_files)),
        Arc::new(UInt64Array::from(binlog_positions)),
        Arc::new(StringArray::from(gtids)),
    ];

    // Row data columns
    for (col_idx, _col_def) in table_info.columns.iter().enumerate() {
        let values: Vec<Option<String>> = events
            .iter()
            .map(|e| {
                e.row.columns.get(col_idx).and_then(|v| {
                    if v.is_null() {
                        None
                    } else {
                        Some(v.to_text())
                    }
                })
            })
            .collect();

        // For simplicity, convert all to strings
        // A production implementation would use proper Arrow types
        let array: ArrayRef = Arc::new(StringArray::from(
            values.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
        ));
        columns.push(array);
    }

    let schema = build_cdc_schema(&table_info.arrow_schema);
    RecordBatch::try_new(Arc::new(schema), columns)
}

/// Builds the full CDC schema including metadata and row columns.
fn build_cdc_schema(row_schema: &Schema) -> Schema {
    let mut fields = vec![
        Field::new("_table", DataType::Utf8, false),
        Field::new("_op", DataType::Utf8, false),
        Field::new("_ts_ms", DataType::Int64, false),
        Field::new("_binlog_file", DataType::Utf8, true),
        Field::new("_binlog_pos", DataType::UInt64, true),
        Field::new("_gtid", DataType::Utf8, true),
    ];

    // Add row data columns (as strings for now)
    for field in row_schema.fields() {
        fields.push(Field::new(field.name(), DataType::Utf8, true));
    }

    Schema::new(fields)
}

/// Converts a single column value to JSON for the CDC envelope.
#[must_use]
pub fn column_value_to_json(value: &ColumnValue) -> serde_json::Value {
    match value {
        ColumnValue::Null => serde_json::Value::Null,
        ColumnValue::SignedInt(v) => serde_json::json!(v),
        ColumnValue::UnsignedInt(v) => serde_json::json!(v),
        ColumnValue::Float(v) => serde_json::json!(v),
        ColumnValue::Double(v) => serde_json::json!(v),
        ColumnValue::String(s) => serde_json::json!(s),
        ColumnValue::Bytes(b) => serde_json::json!(base64_encode(b)),
        ColumnValue::Date(y, m, d) => serde_json::json!(format!("{y:04}-{m:02}-{d:02}")),
        ColumnValue::Time(h, m, s, us) => {
            if *us > 0 {
                serde_json::json!(format!("{h:02}:{m:02}:{s:02}.{us:06}"))
            } else {
                serde_json::json!(format!("{h:02}:{m:02}:{s:02}"))
            }
        }
        ColumnValue::DateTime(y, mo, d, h, mi, s, us) => {
            if *us > 0 {
                serde_json::json!(format!(
                    "{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{us:06}"
                ))
            } else {
                serde_json::json!(format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}"))
            }
        }
        ColumnValue::Timestamp(us) => serde_json::json!(us),
        ColumnValue::Json(s) => serde_json::from_str(s).unwrap_or_else(|_| serde_json::json!(s)),
    }
}

/// Converts a row to JSON object.
#[must_use]
pub fn row_to_json(row: &RowData, columns: &[super::types::MySqlColumn]) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (i, col) in columns.iter().enumerate() {
        if let Some(value) = row.columns.get(i) {
            obj.insert(col.name.clone(), column_value_to_json(value));
        }
    }
    serde_json::Value::Object(obj)
}

/// Simple base64 encoding for binary data.
fn base64_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        let _ = result.write_char(ALPHABET[b0 >> 2] as char);
        let _ = result.write_char(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if chunk.len() > 1 {
            let _ = result.write_char(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            let _ = result.write_char(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_row() -> RowData {
        RowData {
            columns: vec![
                ColumnValue::SignedInt(1),
                ColumnValue::String("Alice".to_string()),
                ColumnValue::Null,
            ],
        }
    }

    #[test]
    fn test_cdc_operation_as_str() {
        assert_eq!(CdcOperation::Insert.as_str(), "I");
        assert_eq!(CdcOperation::UpdateBefore.as_str(), "U-");
        assert_eq!(CdcOperation::UpdateAfter.as_str(), "U+");
        assert_eq!(CdcOperation::Delete.as_str(), "D");
    }

    #[test]
    fn test_cdc_operation_weight() {
        assert_eq!(CdcOperation::Insert.weight(), 1);
        assert_eq!(CdcOperation::UpdateAfter.weight(), 1);
        assert_eq!(CdcOperation::Delete.weight(), -1);
        assert_eq!(CdcOperation::UpdateBefore.weight(), -1);
    }

    #[test]
    fn test_change_event_insert() {
        let event = ChangeEvent::insert(
            "db.users".to_string(),
            1704067200000,
            "mysql-bin.000003".to_string(),
            12345,
            Some("gtid-123".to_string()),
            make_test_row(),
        );

        assert_eq!(event.table, "db.users");
        assert_eq!(event.operation, CdcOperation::Insert);
        assert_eq!(event.timestamp_ms, 1704067200000);
    }

    #[test]
    fn test_insert_to_events() {
        let msg = InsertMessage {
            table_id: 100,
            database: "testdb".to_string(),
            table: "users".to_string(),
            rows: vec![make_test_row(), make_test_row()],
            binlog_position: 12345,
            timestamp_ms: 1704067200000,
        };

        let events = insert_to_events(&msg, "mysql-bin.000003", Some("gtid-1"));
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|e| e.operation == CdcOperation::Insert));
    }

    #[test]
    fn test_update_to_events() {
        let msg = UpdateMessage {
            table_id: 100,
            database: "testdb".to_string(),
            table: "users".to_string(),
            rows: vec![UpdateRowData {
                before: RowData {
                    columns: vec![ColumnValue::String("old".to_string())],
                },
                after: RowData {
                    columns: vec![ColumnValue::String("new".to_string())],
                },
            }],
            binlog_position: 12345,
            timestamp_ms: 1704067200000,
        };

        let events = update_to_events(&msg, "mysql-bin.000003", None);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].operation, CdcOperation::UpdateBefore);
        assert_eq!(events[1].operation, CdcOperation::UpdateAfter);
    }

    #[test]
    fn test_delete_to_events() {
        let msg = DeleteMessage {
            table_id: 100,
            database: "testdb".to_string(),
            table: "users".to_string(),
            rows: vec![make_test_row()],
            binlog_position: 12345,
            timestamp_ms: 1704067200000,
        };

        let events = delete_to_events(&msg, "mysql-bin.000003", Some("gtid-1"));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].operation, CdcOperation::Delete);
    }

    #[test]
    fn test_column_value_to_json() {
        assert_eq!(
            column_value_to_json(&ColumnValue::Null),
            serde_json::Value::Null
        );
        assert_eq!(
            column_value_to_json(&ColumnValue::SignedInt(42)),
            serde_json::json!(42)
        );
        assert_eq!(
            column_value_to_json(&ColumnValue::String("hello".to_string())),
            serde_json::json!("hello")
        );
        assert_eq!(
            column_value_to_json(&ColumnValue::Date(2024, 6, 15)),
            serde_json::json!("2024-06-15")
        );
    }

    #[test]
    fn test_cdc_metadata_schema() {
        let schema = cdc_metadata_schema();
        assert_eq!(schema.fields().len(), 6);
        assert_eq!(schema.field(0).name(), "_table");
        assert_eq!(schema.field(1).name(), "_op");
    }

    #[test]
    fn test_base64_encode() {
        assert_eq!(base64_encode(&[]), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
    }
}
