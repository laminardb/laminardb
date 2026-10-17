//! Rows to `MQMD` descriptors, payloads, and typed message properties.
//!
//! Every value is validated for the whole batch before the worker puts anything.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema, SchemaRef};
use mqi::properties::Value;
use mqi::string::StrCcsidOwned;

use super::super::config::{IbmMqSinkConfig, Persistence, Valued};
use crate::error::ConnectorError;
use crate::serde::Format;

/// One message ready for `MQPUT`.
pub(in crate::ibmmq) struct Outgoing {
    pub(in crate::ibmmq) data: Vec<u8>,
    pub(in crate::ibmmq) md: libmqm_sys::MQMD,
    /// No `MsgId` column value: the queue manager generates one (`MQPMO_NEW_MSG_ID`).
    pub(in crate::ibmmq) new_message_id: bool,
    pub(in crate::ibmmq) properties: Vec<(String, Value)>,
}

const IDENTIFIER: &[DataType] = &[DataType::Binary, DataType::Utf8];
const INTEGERS: &[DataType] = &[
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
];
const PROPERTY_TYPES: &[DataType] = &[
    DataType::Utf8,
    DataType::Binary,
    DataType::Boolean,
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::Float32,
    DataType::Float64,
];

/// Validate descriptor and property columns; return the payload schema without them.
pub(super) fn payload_schema(
    config: &IbmMqSinkConfig,
    input: &SchemaRef,
) -> Result<SchemaRef, ConnectorError> {
    if let Some(name) = &config.message_id_column {
        require(input, name, IDENTIFIER, "Binary or Utf8")?;
    }
    if let Some(name) = &config.correlation_id_column {
        require(input, name, IDENTIFIER, "Binary or Utf8")?;
    }
    if let Some(Valued::Column(name)) = &config.priority {
        require(input, name, INTEGERS, "an integer")?;
    }
    for name in &config.property_columns {
        require(
            input,
            name,
            PROPERTY_TYPES,
            "Utf8, Binary, Boolean, integer, or floating point",
        )?;
    }
    let mapped = config.property_columns();
    let payload: Vec<_> = input
        .fields()
        .iter()
        .filter(|field| !mapped.contains(&field.name().as_str()))
        .cloned()
        .collect();
    Ok(Arc::new(Schema::new_with_metadata(
        payload,
        input.metadata().clone(),
    )))
}

fn require(
    schema: &SchemaRef,
    name: &str,
    allowed: &[DataType],
    expected: &str,
) -> Result<(), ConnectorError> {
    let field = schema.field_with_name(name).map_err(|_| {
        ConnectorError::SchemaMismatch(format!("IBM MQ column '{name}' is not in the query"))
    })?;
    if !allowed.contains(field.data_type()) {
        return Err(ConnectorError::SchemaMismatch(format!(
            "IBM MQ column '{name}' must be {expected}, got {}",
            field.data_type()
        )));
    }
    Ok(())
}

/// The `MQMD` shared by every message of the sink.
pub(super) fn base_descriptor(
    config: &IbmMqSinkConfig,
    payload: &Schema,
) -> Result<libmqm_sys::MQMD, ConnectorError> {
    use mqi::constants;
    let mut md = libmqm_default::MQMD_DEFAULT;
    md.Persistence = match config.persistence {
        Persistence::Persistent => constants::MQPER_PERSISTENT.0,
        Persistence::NotPersistent => constants::MQPER_NOT_PERSISTENT.0,
        Persistence::AsQueueDefinition => constants::MQPER_PERSISTENCE_AS_Q_DEF.0,
    };
    if let Some(Valued::Literal(priority)) = config.priority {
        md.Priority = priority;
    }
    if let Some(expiry) = config.expiry {
        md.Expiry = expiry;
    }
    let binary = config.format == Format::Raw
        && payload
            .fields()
            .first()
            .is_some_and(|field| field.data_type() != &DataType::Utf8);
    let format = match (&config.message_format, binary) {
        (Some(format), _) => format.as_str(),
        (None, true) => "",
        (None, false) => "MQSTR",
    };
    md.Format = fixed::<8>("message.format", format)?;
    md.CodedCharSetId = config.ccsid;
    md.Encoding = constants::MQENC_NATIVE.0;
    if let Some(queue) = &config.reply_to_queue {
        md.ReplyToQ = fixed::<48>("reply.to.queue", queue)?;
    }
    if let Some(queue_manager) = &config.reply_to_queue_manager {
        md.ReplyToQMgr = fixed::<48>("reply.to.queue.manager", queue_manager)?;
    }
    Ok(md)
}

fn fixed<const N: usize>(
    key: &str,
    value: &str,
) -> Result<[mqi::types::MQCHAR; N], ConnectorError> {
    mqi::MqStr::<N>::from_str(value)
        .map(|value| *value.as_mqchar())
        .map_err(|error| ConnectorError::ConfigurationError(format!("'{key}': {error}")))
}

/// Build every message of `batch` from its serialized payloads.
pub(super) fn messages(
    config: &IbmMqSinkConfig,
    base: &libmqm_sys::MQMD,
    batch: &RecordBatch,
    payloads: Vec<Vec<u8>>,
) -> Result<Vec<Outgoing>, ConnectorError> {
    let column = |name: &str| {
        batch.column_by_name(name).ok_or_else(|| {
            ConnectorError::SchemaMismatch(format!("IBM MQ column '{name}' is missing"))
        })
    };
    let message_ids = config
        .message_id_column
        .as_deref()
        .map(column)
        .transpose()?;
    let correlation_ids = config
        .correlation_id_column
        .as_deref()
        .map(column)
        .transpose()?;
    let priorities = match &config.priority {
        Some(Valued::Column(name)) => Some(priorities(column(name)?, name)?),
        _ => None,
    };
    let properties: Vec<(&String, &ArrayRef)> = config
        .property_columns
        .iter()
        .map(|name| column(name).map(|array| (name, array)))
        .collect::<Result<_, _>>()?;
    payloads
        .into_iter()
        .enumerate()
        .map(|(row, data)| {
            let mut md = *base;
            let message_id = message_ids
                .map(|ids| identifier(ids, row))
                .transpose()?
                .flatten();
            if let Some(id) = message_id {
                md.MsgId = id;
            }
            if let Some(id) = correlation_ids
                .map(|ids| identifier(ids, row))
                .transpose()?
                .flatten()
            {
                md.CorrelId = id;
            }
            if let Some(priority) = priorities.as_ref().and_then(|values| values[row]) {
                md.Priority = priority;
            }
            Ok(Outgoing {
                data,
                md,
                new_message_id: message_id.is_none(),
                properties: properties
                    .iter()
                    .filter_map(|(name, array)| {
                        property(array, row).map(|value| ((*name).clone(), value))
                    })
                    .collect(),
            })
        })
        .collect()
}

/// A 24-byte MQ identifier, zero-padded; NULL is `None`.
fn identifier(array: &ArrayRef, row: usize) -> Result<Option<[u8; 24]>, ConnectorError> {
    if array.is_null(row) {
        return Ok(None);
    }
    let bytes: &[u8] = match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().value(row).as_bytes(),
        _ => array.as_binary::<i32>().value(row),
    };
    if bytes.len() > 24 {
        return Err(ConnectorError::SchemaMismatch(format!(
            "an MQ identifier holds at most 24 bytes; row {row} has {}",
            bytes.len()
        )));
    }
    let mut id = [0_u8; 24];
    id[..bytes.len()].copy_from_slice(bytes);
    Ok(Some(id))
}

fn priorities(array: &ArrayRef, name: &str) -> Result<Vec<Option<i32>>, ConnectorError> {
    let cast = arrow_cast::cast(array, &DataType::Int64).map_err(|error| {
        ConnectorError::SchemaMismatch(format!("priority column '{name}': {error}"))
    })?;
    cast.as_primitive::<Int64Type>()
        .iter()
        .map(|value| {
            value
                .map(|priority| match i32::try_from(priority) {
                    Ok(priority @ 0..=9) => Ok(priority),
                    _ => Err(ConnectorError::SchemaMismatch(format!(
                        "priority column '{name}' holds {priority}, outside 0..=9"
                    ))),
                })
                .transpose()
        })
        .collect()
}

/// The typed property value of `row`; NULL omits the property.
fn property(array: &ArrayRef, row: usize) -> Option<Value> {
    if array.is_null(row) {
        return None;
    }
    Some(match array.data_type() {
        DataType::Utf8 => Value::String(StrCcsidOwned::from(array.as_string::<i32>().value(row))),
        DataType::Binary => Value::ByteString(array.as_binary::<i32>().value(row).to_vec()),
        DataType::Boolean => Value::Boolean(array.as_boolean().value(row)),
        DataType::Int8 => Value::Int8(array.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => Value::Int16(array.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => Value::Int32(array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => Value::Int64(array.as_primitive::<Int64Type>().value(row)),
        DataType::Float32 => Value::Float32(array.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => Value::Float64(array.as_primitive::<Float64Type>().value(row)),
        // Schema validation admits only the types above.
        _ => return None,
    })
}
