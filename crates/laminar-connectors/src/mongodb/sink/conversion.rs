//! Arrow, JSON, and BSON conversion with exact working-set accounting.

use super::{
    is_supported_mongodb_arrow_type, ConnectorError, DataType, Duration, RecordBatch, WriteResult,
    CDC_ROW_OVERHEAD_BYTES, MATERIALIZED_BYTE_CHARGE, MAX_STANDARD_DOCUMENT_BYTES,
    WRITE_MODEL_OVERHEAD_BYTES,
};

pub(super) fn cdc_row_value(
    batch: &RecordBatch,
    row: usize,
) -> Result<(serde_json::Value, usize), ConnectorError> {
    use arrow_array::{Array, StringArray};

    let mut value = serde_json::Map::with_capacity(5);
    let mut staging_bytes = CDC_ROW_OVERHEAD_BYTES;
    for field_name in [
        "_namespace",
        "_op",
        "_document_key",
        "_full_document",
        "_update_desc",
    ] {
        let column_index = batch.schema().index_of(field_name).map_err(|_| {
            ConnectorError::SchemaMismatch(format!(
                "MongoDB CDC replay batch is missing '{field_name}'"
            ))
        })?;
        let column = batch
            .column(column_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                ConnectorError::SchemaMismatch(format!(
                    "MongoDB CDC replay field '{field_name}' must be Utf8"
                ))
            })?;
        let (field_value, payload_bytes) = if column.is_null(row) {
            (serde_json::Value::Null, 0)
        } else {
            let text = column.value(row);
            (serde_json::Value::String(text.to_owned()), text.len())
        };
        staging_bytes = staging_bytes
            .checked_add(
                std::mem::size_of::<serde_json::Value>()
                    .saturating_add(payload_bytes)
                    .saturating_mul(3),
            )
            .ok_or_else(|| {
                ConnectorError::ConfigurationError("MongoDB CDC staging byte count overflow".into())
            })?;
        value.insert(field_name.to_string(), field_value);
    }
    Ok((serde_json::Value::Object(value), staging_bytes))
}

/// Milliseconds since epoch for a timestamp column cell, normalizing the unit.
/// Sub-millisecond precision is floored toward the preceding millisecond.
pub(super) fn timestamp_millis(
    col: &dyn arrow_array::Array,
    row: usize,
) -> Result<i64, ConnectorError> {
    use arrow_array::{
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    let DataType::Timestamp(unit, _) = col.data_type() else {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB timestamp conversion received a non-timestamp Arrow column".into(),
        ));
    };
    let a = col.as_any();
    let millis = match unit {
        arrow_schema::TimeUnit::Second => a
            .downcast_ref::<TimestampSecondArray>()
            .unwrap()
            .value(row)
            .checked_mul(1000)
            .ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "MongoDB timestamp seconds overflow millisecond range".into(),
                )
            })?,
        arrow_schema::TimeUnit::Millisecond => a
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(row),
        arrow_schema::TimeUnit::Microsecond => a
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(row)
            .div_euclid(1000),
        arrow_schema::TimeUnit::Nanosecond => a
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(row)
            .div_euclid(1_000_000),
    };
    Ok(millis)
}

/// Convert one Arrow cell straight to BSON for the insert/upsert paths,
/// skipping the `serde_json::Value` intermediate (and the number string round-trip)
/// that the CDC path still needs. Integers stay width-faithful and timestamps become
/// BSON dates; unsupported types are rejected during schema validation.
pub(super) fn arrow_value_to_bson(
    col: &dyn arrow_array::Array,
    row: usize,
) -> Result<mongodb::bson::Bson, ConnectorError> {
    use arrow_array::{
        BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        LargeStringArray, StringArray, UInt16Array, UInt32Array, UInt8Array,
    };
    use mongodb::bson::Bson;

    if !is_supported_mongodb_arrow_type(col.data_type()) {
        return Err(ConnectorError::ConfigurationError(format!(
            "MongoDB sink cannot convert unsupported Arrow type {:?}",
            col.data_type()
        )));
    }
    if col.is_null(row) {
        return Ok(Bson::Null);
    }
    let a = col.as_any();
    let i32_of = |v: i32| Bson::Int32(v);
    let value = match col.data_type() {
        DataType::Null => Bson::Null,
        DataType::Boolean => Bson::Boolean(a.downcast_ref::<BooleanArray>().unwrap().value(row)),
        DataType::Int8 => i32_of(i32::from(a.downcast_ref::<Int8Array>().unwrap().value(row))),
        DataType::Int16 => i32_of(i32::from(
            a.downcast_ref::<Int16Array>().unwrap().value(row),
        )),
        DataType::Int32 => i32_of(a.downcast_ref::<Int32Array>().unwrap().value(row)),
        DataType::Int64 => Bson::Int64(a.downcast_ref::<Int64Array>().unwrap().value(row)),
        DataType::UInt8 => i32_of(i32::from(
            a.downcast_ref::<UInt8Array>().unwrap().value(row),
        )),
        DataType::UInt16 => i32_of(i32::from(
            a.downcast_ref::<UInt16Array>().unwrap().value(row),
        )),
        DataType::UInt32 => Bson::Int64(i64::from(
            a.downcast_ref::<UInt32Array>().unwrap().value(row),
        )),
        DataType::Float32 => Bson::Double(f64::from(
            a.downcast_ref::<Float32Array>().unwrap().value(row),
        )),
        DataType::Float64 => Bson::Double(a.downcast_ref::<Float64Array>().unwrap().value(row)),
        DataType::Utf8 => Bson::String(
            a.downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        DataType::LargeUtf8 => Bson::String(
            a.downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        DataType::Timestamp(..) => Bson::DateTime(mongodb::bson::DateTime::from_millis(
            timestamp_millis(col, row)?,
        )),
        _ => unreachable!("unsupported Arrow type rejected before conversion"),
    };
    Ok(value)
}

pub(super) fn retained_batch_bytes(batch: &RecordBatch) -> usize {
    batch.columns().iter().fold(0, |total, column| {
        total.saturating_add(column.get_array_memory_size())
    })
}

pub(super) fn clamp_client_timeout(configured: Option<Duration>, limit: Duration) -> Duration {
    configured
        .filter(|timeout| !timeout.is_zero())
        .map_or(limit, |timeout| timeout.min(limit))
}

pub(super) fn requires_preflush(
    buffered_bytes: usize,
    incoming_bytes: usize,
    retained_limit: usize,
) -> Result<bool, ConnectorError> {
    if incoming_bytes > retained_limit {
        return Err(ConnectorError::ConfigurationError(format!(
            "MongoDB sink input batch retains {incoming_bytes} bytes, exceeding the fixed \
             {retained_limit}-byte per-sink buffer limit; split the batch upstream"
        )));
    }

    Ok(buffered_bytes
        .checked_add(incoming_bytes)
        .is_none_or(|total| total > retained_limit))
}

pub(super) fn accumulate_write_result(total: &mut WriteResult, flushed: &WriteResult) {
    total.records_written = total
        .records_written
        .saturating_add(flushed.records_written);
    total.bytes_written = total.bytes_written.saturating_add(flushed.bytes_written);
}

pub(super) fn mongo_partial_batch_error(
    completed: &WriteResult,
    error: ConnectorError,
) -> ConnectorError {
    if completed.records_written == 0 {
        return error;
    }
    let retryable = error.is_transient();
    ConnectorError::outcome_unknown(
        format!(
            "MongoDB batch failed after {} records and {} bytes were already written: {error}",
            completed.records_written, completed.bytes_written
        ),
        retryable,
    )
}

pub(super) fn checked_converted_total(
    current: u64,
    incoming: usize,
    limit: usize,
    context: &str,
) -> Result<u64, ConnectorError> {
    let incoming = u64::try_from(incoming).unwrap_or(u64::MAX);
    let limit = u64::try_from(limit).unwrap_or(u64::MAX);
    let total = current.checked_add(incoming).ok_or_else(|| {
        ConnectorError::ConfigurationError(format!("{context} encoded byte count overflow"))
    })?;
    if total > limit {
        return Err(ConnectorError::ConfigurationError(format!(
            "{context} encodes to {total} bytes, exceeding the fixed {limit}-byte conversion \
             limit; split the batch upstream"
        )));
    }
    Ok(total)
}

pub(super) fn working_set_charge(
    retained_bytes: usize,
    encoded_bytes: u64,
    model_count: usize,
    staging_bytes: usize,
) -> Option<usize> {
    let encoded_bytes = usize::try_from(encoded_bytes).ok()?;
    retained_bytes
        .checked_add(encoded_bytes.checked_mul(MATERIALIZED_BYTE_CHARGE)?)?
        .checked_add(model_count.checked_mul(WRITE_MODEL_OVERHEAD_BYTES)?)?
        .checked_add(staging_bytes)
}

pub(super) fn ensure_working_set(
    retained_bytes: usize,
    encoded_bytes: u64,
    model_count: usize,
    staging_bytes: usize,
    limit: usize,
    context: &str,
) -> Result<(), ConnectorError> {
    let charge = working_set_charge(retained_bytes, encoded_bytes, model_count, staging_bytes)
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(format!("{context} working-set byte count overflow"))
        })?;
    if charge > limit {
        return Err(ConnectorError::ConfigurationError(format!(
            "{context} requires a conservative {charge}-byte working set, exceeding the fixed \
             {limit}-byte per-sink limit; split the batch upstream"
        )));
    }
    Ok(())
}

pub(super) fn encoded_document_size(
    document: &mongodb::bson::Document,
    limit: usize,
    context: &str,
) -> Result<usize, ConnectorError> {
    let encoded = mongodb::bson::to_vec(document).map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "{context} cannot be represented as BSON: {error}"
        ))
    })?;
    if encoded.len() > limit {
        return Err(ConnectorError::ConfigurationError(format!(
            "{context} encodes to {} bytes, exceeding MongoDB's {limit}-byte BSON document \
             limit",
            encoded.len()
        )));
    }
    Ok(encoded.len())
}

pub(super) fn account_bson_document(
    total: &mut u64,
    document: &mongodb::bson::Document,
    converted_limit: usize,
    context: &str,
) -> Result<(), ConnectorError> {
    let bytes = encoded_document_size(document, MAX_STANDARD_DOCUMENT_BYTES, context)?;
    *total = checked_converted_total(*total, bytes, converted_limit, "MongoDB CDC flush")?;
    Ok(())
}

/// Convert source-envelope JSON to BSON while interpreting `MongoDB` Extended JSON.
/// Serde's structural conversion would store `$date` as a literal sub-document.
pub(super) fn json_to_bson(
    value: &serde_json::Value,
) -> Result<mongodb::bson::Bson, ConnectorError> {
    mongodb::bson::Bson::try_from(value.clone())
        .map_err(|e| ConnectorError::ConfigurationError(format!("JSON to BSON: {e}")))
}

pub(super) fn json_to_bson_document(
    value: &serde_json::Value,
) -> Result<mongodb::bson::Document, ConnectorError> {
    match json_to_bson(value)? {
        mongodb::bson::Bson::Document(doc) => Ok(doc),
        other => Err(ConnectorError::ConfigurationError(format!(
            "expected a BSON document, got {:?}",
            other.element_type()
        ))),
    }
}

pub(super) fn validate_cdc_document_key(
    document: &mongodb::bson::Document,
) -> Result<mongodb::bson::Document, ConnectorError> {
    if document.is_empty() || !document.contains_key("_id") {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC replay document key must contain '_id' and every target shard-key field"
                .into(),
        ));
    }
    let id = document.get("_id").expect("_id presence checked above");
    if id == &mongodb::bson::Bson::Null {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC replay '_id' must be non-null".into(),
        ));
    }
    let mut filter = mongodb::bson::Document::new();
    for (field, value) in document {
        if field.starts_with('$') {
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB CDC document-key field '{field}' must not be an operator"
            )));
        }
        // Explicit equality prevents a document-valued key from being interpreted as a query
        // operator while retaining every shard-key component for targeted writes.
        filter.insert(field.clone(), mongodb::bson::doc! { "$eq": value.clone() });
    }
    Ok(filter)
}

pub(super) fn document_key_value<'a>(
    document: &'a mongodb::bson::Document,
    field: &str,
) -> Option<&'a mongodb::bson::Bson> {
    if let Some(value) = document.get(field) {
        return Some(value);
    }
    let mut path = field.split('.');
    let mut value = document.get(path.next()?)?;
    for component in path {
        value = value.as_document()?.get(component)?;
    }
    Some(value)
}

pub(super) fn validate_cdc_replacement_key(
    document_key: &mongodb::bson::Document,
    replacement: &mongodb::bson::Document,
) -> Result<(), ConnectorError> {
    for (field, expected) in document_key {
        let actual = document_key_value(replacement, field).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "MongoDB CDC replacement is missing document-key field '{field}'"
            ))
        })?;
        if actual != expected {
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB CDC replacement document-key field '{field}' does not match the event"
            )));
        }
    }
    Ok(())
}
