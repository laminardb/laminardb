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
    plan: &ArrowBatchPlan,
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
mod tests;
