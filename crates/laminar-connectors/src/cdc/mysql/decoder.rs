//! MySQL binlog event decoder.
//!
//! Wraps the `mysql_cdc` crate's binlog events into our internal
//! [`BinlogMessage`] types for unified CDC processing.

use super::gtid::Gtid;
use super::types::MySqlColumn;

/// A decoded binlog message from MySQL replication.
#[derive(Debug, Clone, PartialEq)]
pub enum BinlogMessage {
    /// Transaction begin (GTID event).
    Begin(BeginMessage),
    /// Transaction commit (XID event).
    Commit(CommitMessage),
    /// Table map event (schema for subsequent row events).
    TableMap(TableMapMessage),
    /// Row insert event.
    Insert(InsertMessage),
    /// Row update event.
    Update(UpdateMessage),
    /// Row delete event.
    Delete(DeleteMessage),
    /// Query event (DDL statements).
    Query(QueryMessage),
    /// Rotate event (binlog file rotation).
    Rotate(RotateMessage),
    /// Heartbeat event.
    Heartbeat,
}

/// Transaction begin message (from GTID event).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginMessage {
    /// GTID of the transaction (if GTID mode is enabled).
    pub gtid: Option<Gtid>,
    /// Binlog filename.
    pub binlog_filename: String,
    /// Position in the binlog file.
    pub binlog_position: u64,
    /// Timestamp in milliseconds since Unix epoch.
    pub timestamp_ms: i64,
}

/// Transaction commit message (from XID event).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitMessage {
    /// XID (transaction ID).
    pub xid: u64,
    /// Binlog position after commit.
    pub binlog_position: u64,
    /// Commit timestamp in milliseconds since Unix epoch.
    pub timestamp_ms: i64,
}

/// Table map message (schema definition for row events).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMapMessage {
    /// Table ID (internal MySQL identifier).
    pub table_id: u64,
    /// Database name.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Column definitions.
    pub columns: Vec<MySqlColumn>,
}

/// Row insert message.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertMessage {
    /// Table ID (references prior TableMapMessage).
    pub table_id: u64,
    /// Database name.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Inserted row data.
    pub rows: Vec<RowData>,
    /// Binlog position.
    pub binlog_position: u64,
    /// Timestamp in milliseconds.
    pub timestamp_ms: i64,
}

/// Row update message.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateMessage {
    /// Table ID (references prior TableMapMessage).
    pub table_id: u64,
    /// Database name.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Updated rows (before and after images).
    pub rows: Vec<UpdateRowData>,
    /// Binlog position.
    pub binlog_position: u64,
    /// Timestamp in milliseconds.
    pub timestamp_ms: i64,
}

/// Row delete message.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteMessage {
    /// Table ID (references prior TableMapMessage).
    pub table_id: u64,
    /// Database name.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Deleted row data.
    pub rows: Vec<RowData>,
    /// Binlog position.
    pub binlog_position: u64,
    /// Timestamp in milliseconds.
    pub timestamp_ms: i64,
}

/// Query event (typically DDL).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryMessage {
    /// Database context.
    pub database: String,
    /// SQL query text.
    pub query: String,
    /// Binlog position.
    pub binlog_position: u64,
    /// Timestamp in milliseconds.
    pub timestamp_ms: i64,
}

/// Rotate event (binlog file change).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotateMessage {
    /// New binlog filename.
    pub next_binlog: String,
    /// Position in the new binlog.
    pub position: u64,
}

/// Row data containing column values.
#[derive(Debug, Clone, PartialEq)]
pub struct RowData {
    /// Column values in ordinal order.
    pub columns: Vec<ColumnValue>,
}

/// Update row data with before and after images.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateRowData {
    /// Before image (old values).
    pub before: RowData,
    /// After image (new values).
    pub after: RowData,
}

/// A single column value.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    /// NULL value.
    Null,
    /// Signed integer.
    SignedInt(i64),
    /// Unsigned integer.
    UnsignedInt(u64),
    /// Float.
    Float(f32),
    /// Double.
    Double(f64),
    /// String or text.
    String(String),
    /// Binary data.
    Bytes(Vec<u8>),
    /// Date (year, month, day).
    Date(i32, u32, u32),
    /// Time (hours, minutes, seconds, microseconds).
    Time(i32, u32, u32, u32),
    /// Datetime (year, month, day, hour, minute, second, microsecond).
    DateTime(i32, u32, u32, u32, u32, u32, u32),
    /// Timestamp (Unix timestamp in microseconds).
    Timestamp(i64),
    /// JSON value (as string).
    Json(String),
}

impl ColumnValue {
    /// Returns the value as a string if applicable.
    #[must_use]
    pub fn as_string(&self) -> Option<&str> {
        match self {
            ColumnValue::String(s) | ColumnValue::Json(s) => Some(s),
            _ => None,
        }
    }

    /// Returns the value as an i64 if applicable.
    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ColumnValue::SignedInt(v) => Some(*v),
            ColumnValue::UnsignedInt(v) => i64::try_from(*v).ok(),
            _ => None,
        }
    }

    /// Returns true if the value is NULL.
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, ColumnValue::Null)
    }

    /// Converts to a string representation for text-based serialization.
    #[must_use]
    pub fn to_text(&self) -> String {
        match self {
            ColumnValue::Null => String::new(),
            ColumnValue::SignedInt(v) => v.to_string(),
            ColumnValue::UnsignedInt(v) => v.to_string(),
            ColumnValue::Float(v) => v.to_string(),
            ColumnValue::Double(v) => v.to_string(),
            ColumnValue::String(s) => s.clone(),
            ColumnValue::Bytes(b) => {
                // Hex-encode binary data
                b.iter().map(|byte| format!("{byte:02x}")).collect()
            }
            ColumnValue::Date(y, m, d) => format!("{y:04}-{m:02}-{d:02}"),
            ColumnValue::Time(h, m, s, us) => {
                if *us > 0 {
                    format!("{h:02}:{m:02}:{s:02}.{us:06}")
                } else {
                    format!("{h:02}:{m:02}:{s:02}")
                }
            }
            ColumnValue::DateTime(y, mo, d, h, mi, s, us) => {
                if *us > 0 {
                    format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}")
                } else {
                    format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}")
                }
            }
            ColumnValue::Timestamp(us) => {
                // Convert microseconds to ISO format
                let secs = us / 1_000_000;
                let micros = (us % 1_000_000) as u32;
                format!("{secs}.{micros:06}")
            }
            ColumnValue::Json(s) => s.clone(),
        }
    }
}

/// Decoder errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DecoderError {
    /// Unknown event type.
    #[error("unknown event type: {0}")]
    UnknownEventType(u8),

    /// Invalid event data.
    #[error("invalid event data: {0}")]
    InvalidData(String),

    /// Missing table map for row event.
    #[error("missing table map for table_id {0}")]
    MissingTableMap(u64),

    /// Unsupported column type.
    #[error("unsupported column type: {0}")]
    UnsupportedType(u8),
}

/// Decodes a timestamp from MySQL epoch to Unix milliseconds.
///
/// MySQL timestamps are seconds since 1970-01-01 00:00:00 UTC.
#[must_use]
pub fn mysql_timestamp_to_unix_ms(timestamp: u32) -> i64 {
    i64::from(timestamp) * 1000
}

/// Position in the MySQL binlog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinlogPosition {
    /// Binlog filename (e.g., "mysql-bin.000003").
    pub filename: String,
    /// Position within the file.
    pub position: u64,
    /// GTID (if GTID mode is enabled).
    pub gtid: Option<Gtid>,
}

impl BinlogPosition {
    /// Creates a new binlog position.
    #[must_use]
    pub fn new(filename: String, position: u64) -> Self {
        Self {
            filename,
            position,
            gtid: None,
        }
    }

    /// Creates a position with GTID.
    #[must_use]
    pub fn with_gtid(filename: String, position: u64, gtid: Gtid) -> Self {
        Self {
            filename,
            position,
            gtid: Some(gtid),
        }
    }
}

impl std::fmt::Display for BinlogPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref gtid) = self.gtid {
            write!(f, "{}:{} (GTID: {})", self.filename, self.position, gtid)
        } else {
            write!(f, "{}:{}", self.filename, self.position)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_value_null() {
        let val = ColumnValue::Null;
        assert!(val.is_null());
        assert_eq!(val.to_text(), "");
    }

    #[test]
    fn test_column_value_int() {
        let val = ColumnValue::SignedInt(-42);
        assert!(!val.is_null());
        assert_eq!(val.as_i64(), Some(-42));
        assert_eq!(val.to_text(), "-42");

        let val = ColumnValue::UnsignedInt(100);
        assert_eq!(val.as_i64(), Some(100));
        assert_eq!(val.to_text(), "100");
    }

    #[test]
    fn test_column_value_float() {
        let val = ColumnValue::Float(3.14);
        assert_eq!(val.to_text(), "3.14");

        let val = ColumnValue::Double(2.718281828);
        assert!(val.to_text().starts_with("2.718"));
    }

    #[test]
    fn test_column_value_string() {
        let val = ColumnValue::String("hello".to_string());
        assert_eq!(val.as_string(), Some("hello"));
        assert_eq!(val.to_text(), "hello");
    }

    #[test]
    fn test_column_value_bytes() {
        let val = ColumnValue::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(val.to_text(), "deadbeef");
    }

    #[test]
    fn test_column_value_date() {
        let val = ColumnValue::Date(2024, 6, 15);
        assert_eq!(val.to_text(), "2024-06-15");
    }

    #[test]
    fn test_column_value_time() {
        let val = ColumnValue::Time(14, 30, 45, 0);
        assert_eq!(val.to_text(), "14:30:45");

        let val = ColumnValue::Time(14, 30, 45, 123456);
        assert_eq!(val.to_text(), "14:30:45.123456");
    }

    #[test]
    fn test_column_value_datetime() {
        let val = ColumnValue::DateTime(2024, 6, 15, 14, 30, 45, 0);
        assert_eq!(val.to_text(), "2024-06-15 14:30:45");

        let val = ColumnValue::DateTime(2024, 6, 15, 14, 30, 45, 500000);
        assert_eq!(val.to_text(), "2024-06-15 14:30:45.500000");
    }

    #[test]
    fn test_column_value_json() {
        let val = ColumnValue::Json(r#"{"key": "value"}"#.to_string());
        assert_eq!(val.as_string(), Some(r#"{"key": "value"}"#));
    }

    #[test]
    fn test_binlog_position() {
        let pos = BinlogPosition::new("mysql-bin.000003".to_string(), 12345);
        assert_eq!(pos.to_string(), "mysql-bin.000003:12345");
        assert!(pos.gtid.is_none());
    }

    #[test]
    fn test_binlog_position_with_gtid() {
        let gtid: Gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:5"
            .parse()
            .unwrap();
        let pos = BinlogPosition::with_gtid("mysql-bin.000003".to_string(), 12345, gtid);
        assert!(pos.gtid.is_some());
        let s = pos.to_string();
        assert!(s.contains("GTID"));
    }

    #[test]
    fn test_mysql_timestamp_to_unix_ms() {
        // 2024-01-01 00:00:00 UTC = 1704067200 seconds
        let unix_ms = mysql_timestamp_to_unix_ms(1704067200);
        assert_eq!(unix_ms, 1704067200000);
    }

    #[test]
    fn test_row_data() {
        let row = RowData {
            columns: vec![
                ColumnValue::SignedInt(1),
                ColumnValue::String("Alice".to_string()),
                ColumnValue::Null,
            ],
        };
        assert_eq!(row.columns.len(), 3);
        assert_eq!(row.columns[0].as_i64(), Some(1));
        assert!(row.columns[2].is_null());
    }

    #[test]
    fn test_update_row_data() {
        let update = UpdateRowData {
            before: RowData {
                columns: vec![ColumnValue::String("old".to_string())],
            },
            after: RowData {
                columns: vec![ColumnValue::String("new".to_string())],
            },
        };
        assert_eq!(update.before.columns[0].as_string(), Some("old"));
        assert_eq!(update.after.columns[0].as_string(), Some("new"));
    }

    #[test]
    fn test_begin_message() {
        let msg = BeginMessage {
            gtid: Some("3E11FA47-71CA-11E1-9E33-C80AA9429562:5".parse().unwrap()),
            binlog_filename: "mysql-bin.000003".to_string(),
            binlog_position: 12345,
            timestamp_ms: 1704067200000,
        };
        assert!(msg.gtid.is_some());
    }

    #[test]
    fn test_insert_message() {
        let msg = InsertMessage {
            table_id: 100,
            database: "mydb".to_string(),
            table: "users".to_string(),
            rows: vec![RowData {
                columns: vec![ColumnValue::SignedInt(1)],
            }],
            binlog_position: 12345,
            timestamp_ms: 1704067200000,
        };
        assert_eq!(msg.rows.len(), 1);
    }
}
