//! Exact retained-memory ownership and Arrow envelope assembly.

use std::mem::size_of;
use std::sync::Arc;

use arrow_array::builder::{StringBuilder, UInt32Builder};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use tokio::sync::OwnedSemaphorePermit;

use super::{ConnectorError, MongoDbChangeEvent, OperationType};

pub(super) enum BufferedMongoPayload {
    Event(Box<MongoDbChangeEvent>),
    HighWatermark {
        token: String,
        requires_start_after: bool,
    },
}

pub(super) struct BufferedMongoEvent {
    pub(super) payload: BufferedMongoPayload,
    _byte_permit: OwnedSemaphorePermit,
}

impl BufferedMongoEvent {
    pub(super) fn new(event: MongoDbChangeEvent, byte_permit: OwnedSemaphorePermit) -> Self {
        Self {
            payload: BufferedMongoPayload::Event(Box::new(event)),
            _byte_permit: byte_permit,
        }
    }

    pub(super) fn high_watermark(
        token: String,
        requires_start_after: bool,
        byte_permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            payload: BufferedMongoPayload::HighWatermark {
                token,
                requires_start_after,
            },
            _byte_permit: byte_permit,
        }
    }

    pub(super) fn event(&self) -> Option<&MongoDbChangeEvent> {
        match &self.payload {
            BufferedMongoPayload::Event(event) => Some(event),
            BufferedMongoPayload::HighWatermark { .. } => None,
        }
    }
}

pub(super) fn checked_size_add(total: &mut usize, value: usize) -> Result<(), ConnectorError> {
    *total = total.checked_add(value).ok_or_else(|| {
        ConnectorError::ConfigurationError("MongoDB CDC event size overflow".into())
    })?;
    Ok(())
}

pub(super) fn json_retained_bytes(value: &serde_json::Value) -> Result<usize, ConnectorError> {
    let mut total = size_of::<serde_json::Value>();
    match value {
        serde_json::Value::String(value) => checked_size_add(&mut total, value.capacity())?,
        serde_json::Value::Array(values) => {
            checked_size_add(
                &mut total,
                values
                    .capacity()
                    .checked_mul(size_of::<serde_json::Value>())
                    .ok_or_else(|| {
                        ConnectorError::ConfigurationError(
                            "MongoDB CDC JSON array size overflow".into(),
                        )
                    })?,
            )?;
            for value in values {
                checked_size_add(&mut total, json_retained_bytes(value)?)?;
            }
        }
        serde_json::Value::Object(values) => {
            // serde_json::Map does not expose its allocation capacity. Charging each live
            // entry plus its recursively owned values is stable across map backends.
            for (key, value) in values {
                checked_size_add(&mut total, size_of::<String>())?;
                checked_size_add(&mut total, key.capacity())?;
                checked_size_add(&mut total, json_retained_bytes(value)?)?;
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
    Ok(total)
}

pub(super) fn mongo_event_retained_bytes(
    event: &MongoDbChangeEvent,
) -> Result<usize, ConnectorError> {
    let mut total = size_of::<BufferedMongoEvent>();
    if let OperationType::Other(value) = &event.operation_type {
        checked_size_add(&mut total, value.capacity())?;
    }
    checked_size_add(&mut total, event.namespace.db.capacity())?;
    checked_size_add(&mut total, event.namespace.coll.capacity())?;
    checked_size_add(&mut total, event.document_key.capacity())?;
    checked_size_add(
        &mut total,
        event.full_document.as_ref().map_or(0, String::capacity),
    )?;
    checked_size_add(&mut total, event.resume_token.capacity())?;

    if let Some(update) = &event.update_description {
        checked_size_add(
            &mut total,
            update
                .updated_fields
                .capacity()
                .checked_mul(size_of::<(String, serde_json::Value)>())
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError(
                        "MongoDB CDC update field size overflow".into(),
                    )
                })?,
        )?;
        for (key, value) in &update.updated_fields {
            checked_size_add(&mut total, key.capacity())?;
            checked_size_add(&mut total, json_retained_bytes(value)?)?;
        }
        checked_size_add(
            &mut total,
            update
                .removed_fields
                .capacity()
                .checked_mul(size_of::<String>())
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError(
                        "MongoDB CDC removed field size overflow".into(),
                    )
                })?,
        )?;
        for field in &update.removed_fields {
            checked_size_add(&mut total, field.capacity())?;
        }
        checked_size_add(
            &mut total,
            update
                .truncated_arrays
                .capacity()
                .checked_mul(size_of::<super::super::change_event::TruncatedArray>())
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError(
                        "MongoDB CDC truncated array size overflow".into(),
                    )
                })?,
        )?;
        for array in &update.truncated_arrays {
            checked_size_add(&mut total, array.field.capacity())?;
        }
        checked_size_add(
            &mut total,
            update
                .disambiguated_paths
                .capacity()
                .checked_mul(size_of::<(String, serde_json::Value)>())
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError(
                        "MongoDB CDC disambiguated path size overflow".into(),
                    )
                })?,
        )?;
        for (key, value) in &update.disambiguated_paths {
            checked_size_add(&mut total, key.capacity())?;
            checked_size_add(&mut total, json_retained_bytes(value)?)?;
        }
    }
    Ok(total.max(1))
}

pub(super) fn mongo_high_watermark_retained_bytes(
    token_capacity: usize,
) -> Result<usize, ConnectorError> {
    let mut total = size_of::<BufferedMongoEvent>();
    checked_size_add(&mut total, token_capacity)?;
    Ok(total)
}
/// Converts a batch of [`MongoDbChangeEvent`]s to an Arrow `RecordBatch`.
#[cfg(test)]
pub(super) fn events_to_record_batch(
    events: &[MongoDbChangeEvent],
    schema: &SchemaRef,
) -> Result<RecordBatch, ConnectorError> {
    let events: Vec<&MongoDbChangeEvent> = events.iter().collect();
    events_to_record_batch_refs(&events, schema)
}

pub(super) fn events_to_record_batch_refs(
    events: &[&MongoDbChangeEvent],
    schema: &SchemaRef,
) -> Result<RecordBatch, ConnectorError> {
    let len = events.len();

    let mut ns_builder = StringBuilder::with_capacity(len, len * 32);
    let mut op_builder = StringBuilder::with_capacity(len, len * 4);
    let mut dk_builder = StringBuilder::with_capacity(len, len * 64);
    let mut cts_builder = UInt32Builder::with_capacity(len);
    let mut ct_inc_builder = UInt32Builder::with_capacity(len);
    let mut wt_builder = arrow_array::builder::TimestampMillisecondBuilder::with_capacity(len);
    let mut fd_builder = StringBuilder::with_capacity(len, len * 128);
    let mut ud_builder = StringBuilder::with_capacity(len, len * 64);
    let mut rt_builder = StringBuilder::with_capacity(len, len * 64);

    for event in events {
        ns_builder.append_value(event.namespace.full_name());
        op_builder.append_value(event.operation_type.as_str());
        dk_builder.append_value(&event.document_key);
        cts_builder.append_value(event.cluster_time_secs);
        ct_inc_builder.append_value(event.cluster_time_inc);
        wt_builder.append_value(event.wall_time_ms);

        match &event.full_document {
            Some(doc) => fd_builder.append_value(doc),
            None => fd_builder.append_null(),
        }

        match &event.update_description {
            Some(desc) => {
                let json = serde_json::to_string(desc)
                    .map_err(|e| ConnectorError::Internal(format!("serialize update_desc: {e}")))?;
                ud_builder.append_value(&json);
            }
            None => ud_builder.append_null(),
        }

        rt_builder.append_value(&event.resume_token);
    }

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(ns_builder.finish()),
            Arc::new(op_builder.finish()),
            Arc::new(dk_builder.finish()),
            Arc::new(cts_builder.finish()),
            Arc::new(ct_inc_builder.finish()),
            Arc::new(wt_builder.finish()),
            Arc::new(fd_builder.finish()),
            Arc::new(ud_builder.finish()),
            Arc::new(rt_builder.finish()),
        ],
    )
    .map_err(|e| ConnectorError::Internal(format!("arrow batch: {e}")))
}
