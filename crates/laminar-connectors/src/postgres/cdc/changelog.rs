//! CDC envelope construction for `PostgreSQL` row changes.
//!
//! `_before` contains the old full row for `REPLICA IDENTITY FULL`, or only
//! replica-identity fields for a pgoutput key tuple. Unavailable fields are
//! omitted so they remain distinct from an available SQL `NULL`.

use std::sync::Arc;

use arrow_array::builder::{StringBuilder, TimestampMillisecondBuilder, UInt64Builder};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::error::ConnectorError;

use super::decoder::{ColumnValue, OldTuple, TupleData};
use super::lsn::Lsn;
use super::schema::{cdc_envelope_schema, RelationInfo};

/// A CDC operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOperation {
    /// Row inserted.
    Insert,
    /// Row updated.
    Update,
    /// Row deleted.
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
#[derive(Debug)]
pub struct ChangeEvent {
    /// Fully qualified table name.
    pub table: String,
    /// The operation type.
    pub op: CdcOperation,
    /// WAL position of this change.
    pub lsn: Lsn,
    /// Commit timestamp in milliseconds since Unix epoch.
    pub ts_ms: i64,
    /// Available old identity/full-row values as JSON (for UPDATE and DELETE).
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
pub(crate) fn tuple_json_encoded_len(
    tuple: &TupleData,
    relation: &RelationInfo,
) -> Result<usize, ConnectorError> {
    tuple_json_encoded_len_inner(tuple, relation, false)
}

pub(crate) fn old_tuple_json_encoded_len(
    old_tuple: &OldTuple,
    relation: &RelationInfo,
) -> Result<usize, ConnectorError> {
    let (tuple, identity_only) = old_tuple_parts(old_tuple);
    tuple_json_encoded_len_inner(tuple, relation, identity_only)
}

fn tuple_json_encoded_len_inner(
    tuple: &TupleData,
    relation: &RelationInfo,
    identity_only: bool,
) -> Result<usize, ConnectorError> {
    validate_tuple_columns(tuple, relation)?;
    let mut len = 2_usize; // braces
    let mut first = true;
    for (index, value) in tuple.columns.iter().enumerate() {
        let column = &relation.columns[index];
        if (identity_only && !column.is_key) || matches!(value, ColumnValue::Unchanged) {
            continue;
        }
        if !first {
            checked_add(&mut len, 1, "JSON separator")?;
        }
        first = false;
        checked_add(&mut len, 3, "JSON key quotes and colon")?;
        checked_add(&mut len, escaped_json_len(&column.name)?, "JSON key")?;
        match value {
            ColumnValue::Text(bytes) => {
                let text = std::str::from_utf8(bytes).map_err(|_| {
                    ConnectorError::ReadError("PostgreSQL tuple contains invalid UTF-8".into())
                })?;
                checked_add(&mut len, 2, "JSON value quotes")?;
                checked_add(&mut len, escaped_json_len(text)?, "JSON value")?;
            }
            ColumnValue::Null => checked_add(&mut len, 4, "JSON null")?,
            ColumnValue::Unchanged => unreachable!("unchanged values were skipped"),
        }
    }
    Ok(len)
}

pub(crate) fn tuple_to_json(
    tuple: &TupleData,
    relation: &RelationInfo,
    encoded_len: usize,
) -> Result<String, ConnectorError> {
    tuple_to_json_inner(tuple, relation, encoded_len, false)
}

pub(crate) fn old_tuple_to_json(
    old_tuple: &OldTuple,
    relation: &RelationInfo,
    encoded_len: usize,
) -> Result<String, ConnectorError> {
    let (tuple, identity_only) = old_tuple_parts(old_tuple);
    tuple_to_json_inner(tuple, relation, encoded_len, identity_only)
}

fn tuple_to_json_inner(
    tuple: &TupleData,
    relation: &RelationInfo,
    encoded_len: usize,
    identity_only: bool,
) -> Result<String, ConnectorError> {
    validate_tuple_columns(tuple, relation)?;
    let mut buf = String::new();
    buf.try_reserve_exact(encoded_len).map_err(|error| {
        ConnectorError::ReadError(format!(
            "PostgreSQL CDC could not reserve {encoded_len} JSON bytes: {error}"
        ))
    })?;
    buf.push('{');
    let mut first = true;
    for (col_val, col_info) in tuple.columns.iter().zip(&relation.columns) {
        if identity_only && !col_info.is_key {
            continue;
        }
        let val = match col_val {
            ColumnValue::Text(bytes) => Some(std::str::from_utf8(bytes).map_err(|_| {
                ConnectorError::ReadError("PostgreSQL tuple contains invalid UTF-8".into())
            })?),
            ColumnValue::Null => None,
            ColumnValue::Unchanged => continue,
        };
        if !first {
            buf.push(',');
        }
        first = false;
        buf.push('"');
        escape_json_str(&col_info.name, &mut buf);
        buf.push('"');
        buf.push(':');
        match val {
            Some(s) => {
                buf.push('"');
                escape_json_str(s, &mut buf);
                buf.push('"');
            }
            None => buf.push_str("null"),
        }
    }
    buf.push('}');
    debug_assert_eq!(buf.len(), encoded_len);
    Ok(buf)
}

fn old_tuple_parts(old_tuple: &OldTuple) -> (&TupleData, bool) {
    match old_tuple {
        OldTuple::Key(tuple) => (tuple, true),
        OldTuple::Full(tuple) => (tuple, false),
    }
}

fn validate_tuple_columns(
    tuple: &TupleData,
    relation: &RelationInfo,
) -> Result<(), ConnectorError> {
    if tuple.columns.len() != relation.columns.len() {
        return Err(ConnectorError::ReadError(format!(
            "PostgreSQL tuple column count {} does not match relation {} column count {}",
            tuple.columns.len(),
            relation.relation_id,
            relation.columns.len()
        )));
    }
    Ok(())
}

fn escaped_json_len(value: &str) -> Result<usize, ConnectorError> {
    let mut len = 0_usize;
    for character in value.chars() {
        let bytes = match character {
            '"' | '\\' | '\n' | '\r' | '\t' => 2,
            value if value.is_control() => 6,
            value => value.len_utf8(),
        };
        checked_add(&mut len, bytes, "escaped JSON")?;
    }
    Ok(len)
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
                const HEX: &[u8; 16] = b"0123456789abcdef";
                let code = c as u32;
                buf.push_str("\\u");
                for shift in [12, 8, 4, 0] {
                    buf.push(HEX[((code >> shift) & 0x0f) as usize] as char);
                }
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
#[derive(Debug)]
pub(crate) struct ArrowBatchPlan {
    rows: usize,
    table_bytes: usize,
    before_bytes: usize,
    after_bytes: usize,
    pub(crate) retained_bytes: usize,
}

pub(crate) fn plan_record_batch<'a>(
    events: impl IntoIterator<Item = &'a ChangeEvent>,
) -> Result<ArrowBatchPlan, ConnectorError> {
    let mut rows = 0_usize;
    let mut table_bytes = 0_usize;
    let mut before_bytes = 0_usize;
    let mut after_bytes = 0_usize;
    let mut before_has_null = false;
    let mut after_has_null = false;
    for event in events {
        rows = checked_add_value(rows, 1, "Arrow row count")?;
        checked_add(&mut table_bytes, event.table.len(), "Arrow table values")?;
        checked_add(
            &mut before_bytes,
            event.before.as_ref().map_or(0, String::len),
            "Arrow before values",
        )?;
        checked_add(
            &mut after_bytes,
            event.after.as_ref().map_or(0, String::len),
            "Arrow after values",
        )?;
        before_has_null |= event.before.is_none();
        after_has_null |= event.after.is_none();
    }

    let mut retained_bytes = 0_usize;
    for bytes in [table_bytes, rows, before_bytes, after_bytes] {
        checked_add(
            &mut retained_bytes,
            round_to_arrow_alignment(bytes)?,
            "Arrow string values",
        )?;
    }
    let offset_bytes = round_to_arrow_alignment(checked_mul(
        checked_add_value(rows, 1, "Arrow offset rows")?,
        std::mem::size_of::<i32>(),
        "Arrow offsets",
    )?)?;
    checked_add(
        &mut retained_bytes,
        checked_mul(offset_bytes, 4, "Arrow string offsets")?,
        "Arrow string offsets",
    )?;
    let primitive_bytes = round_to_arrow_alignment(checked_mul(
        rows,
        std::mem::size_of::<u64>(),
        "Arrow primitive values",
    )?)?;
    checked_add(
        &mut retained_bytes,
        checked_mul(primitive_bytes, 2, "Arrow primitive columns")?,
        "Arrow primitive columns",
    )?;
    let nullable_columns = usize::from(before_has_null) + usize::from(after_has_null);
    let validity_bytes = round_to_arrow_alignment(rows.div_ceil(8))?;
    checked_add(
        &mut retained_bytes,
        checked_mul(validity_bytes, nullable_columns, "Arrow validity buffers")?,
        "Arrow validity buffers",
    )?;

    Ok(ArrowBatchPlan {
        rows,
        table_bytes,
        before_bytes,
        after_bytes,
        retained_bytes,
    })
}

pub(crate) fn events_to_record_batch<I>(
    events: I,
    plan: ArrowBatchPlan,
) -> Result<RecordBatch, ConnectorError>
where
    I: IntoIterator<Item = ChangeEvent>,
{
    let schema: SchemaRef = cdc_envelope_schema();

    let mut table_builder = StringBuilder::with_capacity(plan.rows, plan.table_bytes);
    let mut op_builder = StringBuilder::with_capacity(plan.rows, plan.rows);
    let mut lsn_builder = UInt64Builder::with_capacity(plan.rows);
    let mut ts_builder = TimestampMillisecondBuilder::with_capacity(plan.rows);
    let mut before_builder = StringBuilder::with_capacity(plan.rows, plan.before_bytes);
    let mut after_builder = StringBuilder::with_capacity(plan.rows, plan.after_bytes);

    let mut rows = 0_usize;
    for event in events {
        rows = rows.checked_add(1).ok_or_else(|| {
            ConnectorError::Internal("PostgreSQL CDC Arrow row-count overflow".into())
        })?;
        table_builder.append_value(&event.table);
        op_builder.append_value(event.op.as_str());
        lsn_builder.append_value(event.lsn.as_u64());
        ts_builder.append_value(event.ts_ms);

        match event.before {
            Some(json) => before_builder.append_value(&json),
            None => before_builder.append_null(),
        }
        match event.after {
            Some(json) => after_builder.append_value(&json),
            None => after_builder.append_null(),
        }
    }
    if rows != plan.rows {
        return Err(ConnectorError::Internal(
            "PostgreSQL CDC Arrow plan row count changed before construction".into(),
        ));
    }

    let batch = RecordBatch::try_new(
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
    .map_err(|error| ConnectorError::Internal(format!("Arrow batch build: {error}")))?;
    let actual_buffer_bytes = batch.columns().iter().try_fold(0_usize, |total, column| {
        total
            .checked_add(column.get_buffer_memory_size())
            .ok_or_else(|| {
                ConnectorError::Internal(
                    "PostgreSQL CDC Arrow retained-byte accounting overflow".into(),
                )
            })
    })?;
    if actual_buffer_bytes > plan.retained_bytes {
        return Err(ConnectorError::Internal(format!(
            "PostgreSQL CDC Arrow retained-byte plan was too small: actual={actual_buffer_bytes}, planned={}",
            plan.retained_bytes
        )));
    }
    Ok(batch)
}

fn checked_add(total: &mut usize, value: usize, context: &str) -> Result<(), ConnectorError> {
    *total = checked_add_value(*total, value, context)?;
    Ok(())
}

fn checked_add_value(left: usize, right: usize, context: &str) -> Result<usize, ConnectorError> {
    left.checked_add(right)
        .ok_or_else(|| ConnectorError::ReadError(format!("PostgreSQL CDC {context} size overflow")))
}

fn checked_mul(left: usize, right: usize, context: &str) -> Result<usize, ConnectorError> {
    left.checked_mul(right)
        .ok_or_else(|| ConnectorError::ReadError(format!("PostgreSQL CDC {context} size overflow")))
}

fn round_to_arrow_alignment(bytes: usize) -> Result<usize, ConnectorError> {
    Ok(checked_add_value(bytes, 63, "Arrow alignment")? & !63)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::cdc::schema::RelationInfo;
    use crate::postgres::cdc::types::PgColumn;
    use crate::postgres::cdc::types::{INT8_OID, TEXT_OID};
    use bytes::Bytes;

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
                ColumnValue::Text(Bytes::from_static(b"42")),
                ColumnValue::Text(Bytes::from_static(b"Alice")),
            ],
        };

        let encoded_len = tuple_json_encoded_len(&tuple, &relation).unwrap();
        let json = tuple_to_json(&tuple, &relation, encoded_len).unwrap();
        assert_eq!(json.len(), encoded_len);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["id"], "42");
        assert_eq!(parsed["name"], "Alice");
    }

    #[test]
    fn test_tuple_to_json_with_null() {
        let relation = sample_relation();
        let tuple = TupleData {
            columns: vec![
                ColumnValue::Text(Bytes::from_static(b"42")),
                ColumnValue::Null,
            ],
        };

        let encoded_len = tuple_json_encoded_len(&tuple, &relation).unwrap();
        let json = tuple_to_json(&tuple, &relation, encoded_len).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["id"], "42");
        assert!(parsed["name"].is_null());
    }

    #[test]
    fn test_tuple_to_json_unchanged_omitted() {
        let relation = sample_relation();
        let tuple = TupleData {
            columns: vec![
                ColumnValue::Text(Bytes::from_static(b"42")),
                ColumnValue::Unchanged,
            ],
        };

        let encoded_len = tuple_json_encoded_len(&tuple, &relation).unwrap();
        let json = tuple_to_json(&tuple, &relation, encoded_len).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["id"], "42");
        // unchanged columns are omitted
        assert!(parsed.get("name").is_none());
    }

    #[test]
    fn key_old_tuple_omits_unavailable_non_identity_fields() {
        let relation = RelationInfo {
            columns: vec![
                PgColumn::new("id".to_string(), INT8_OID, -1, true),
                PgColumn::new("name".to_string(), TEXT_OID, -1, false),
                PgColumn::new("note".to_string(), TEXT_OID, -1, false),
            ],
            ..sample_relation()
        };
        let old_tuple = OldTuple::Key(TupleData {
            columns: vec![
                ColumnValue::Text(Bytes::from_static(b"42")),
                ColumnValue::Text(Bytes::from_static(b"unavailable")),
                ColumnValue::Null,
            ],
        });

        let encoded_len = old_tuple_json_encoded_len(&old_tuple, &relation).unwrap();
        let json = old_tuple_to_json(&old_tuple, &relation, encoded_len).unwrap();

        assert_eq!(json, r#"{"id":"42"}"#);
        assert_eq!(json.len(), encoded_len);
    }

    #[test]
    fn full_old_tuple_retains_non_key_fields_and_explicit_null() {
        let relation = sample_relation();
        let old_tuple = OldTuple::Full(TupleData {
            columns: vec![
                ColumnValue::Text(Bytes::from_static(b"42")),
                ColumnValue::Null,
            ],
        });

        let encoded_len = old_tuple_json_encoded_len(&old_tuple, &relation).unwrap();
        let json = old_tuple_to_json(&old_tuple, &relation, encoded_len).unwrap();

        assert_eq!(json, r#"{"id":"42","name":null}"#);
        assert_eq!(json.len(), encoded_len);
    }

    #[test]
    fn key_old_tuple_requires_full_relation_cardinality() {
        let relation = sample_relation();
        let old_tuple = OldTuple::Key(TupleData {
            columns: vec![ColumnValue::Text(Bytes::from_static(b"42"))],
        });

        let error = old_tuple_json_encoded_len(&old_tuple, &relation).unwrap_err();
        assert!(error.to_string().contains("column count"), "{error}");
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

        let plan = plan_record_batch(&events).unwrap();
        let batch = events_to_record_batch(events, plan).unwrap();
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

        let plan = plan_record_batch(&events).unwrap();
        let batch = events_to_record_batch(events, plan).unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_events_to_record_batch_empty() {
        let events: Vec<ChangeEvent> = vec![];
        let plan = plan_record_batch(&events).unwrap();
        let batch = events_to_record_batch(events, plan).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 6);
    }

    #[test]
    fn test_cdc_operation_as_str() {
        assert_eq!(CdcOperation::Insert.as_str(), "I");
        assert_eq!(CdcOperation::Update.as_str(), "U");
        assert_eq!(CdcOperation::Delete.as_str(), "D");
    }

    #[test]
    fn json_preflight_matches_all_escape_classes() {
        let relation = RelationInfo {
            columns: vec![PgColumn::new(
                "control\nkey".to_string(),
                TEXT_OID,
                -1,
                false,
            )],
            ..sample_relation()
        };
        let tuple = TupleData {
            columns: vec![ColumnValue::Text(Bytes::from_static(
                b"quote\" slash\\ newline\n tab\t",
            ))],
        };
        let encoded_len = tuple_json_encoded_len(&tuple, &relation).unwrap();
        let json = tuple_to_json(&tuple, &relation, encoded_len).unwrap();
        assert_eq!(json.len(), encoded_len);
        serde_json::from_str::<serde_json::Value>(&json).unwrap();
    }

    #[test]
    fn json_preflight_rejects_invalid_text_and_column_count_drift() {
        let relation = sample_relation();
        let invalid_text = TupleData {
            columns: vec![
                ColumnValue::Text(Bytes::from_static(&[0xff])),
                ColumnValue::Null,
            ],
        };
        assert!(tuple_json_encoded_len(&invalid_text, &relation)
            .unwrap_err()
            .to_string()
            .contains("UTF-8"));

        let truncated = TupleData {
            columns: vec![ColumnValue::Null],
        };
        assert!(tuple_json_encoded_len(&truncated, &relation)
            .unwrap_err()
            .to_string()
            .contains("column count"));
    }

    #[test]
    fn arrow_plan_covers_actual_retained_buffers() {
        let events = vec![
            ChangeEvent {
                table: "public.users".into(),
                op: CdcOperation::Insert,
                lsn: Lsn::new(1),
                ts_ms: 1,
                before: None,
                after: Some("{\"id\":\"1\"}".into()),
            },
            ChangeEvent {
                table: "public.users".into(),
                op: CdcOperation::Update,
                lsn: Lsn::new(2),
                ts_ms: 2,
                before: Some("{\"id\":\"1\"}".into()),
                after: Some("{\"id\":\"2\"}".into()),
            },
        ];
        let plan = plan_record_batch(&events).unwrap();
        let planned = plan.retained_bytes;
        let batch = events_to_record_batch(events, plan).unwrap();
        let actual = batch
            .columns()
            .iter()
            .map(|column| column.get_buffer_memory_size())
            .sum::<usize>();
        assert!(actual <= planned, "{actual} > {planned}");
    }
}
