//! Per-message routing key and AMQP properties bound from query columns.
//!
//! Every value is validated for the whole batch before the first publish, so a malformed row
//! cannot leave a partially published batch.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt8Type,
};
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Schema, SchemaRef};
use lapin::types::{AMQPValue, FieldTable, LongString, ShortString};
use lapin::BasicProperties;

use super::super::config::{RabbitMqSinkConfig, Valued};
use crate::error::ConnectorError;
use crate::serde::Format;

/// AMQP short strings (routing keys, ids) hold at most 255 bytes.
const SHORT_STRING_MAX: usize = 255;

/// Validate that every property column exists with a representable type, and return the payload
/// schema: the input without property columns.
pub(super) fn payload_schema(
    config: &RabbitMqSinkConfig,
    input: &SchemaRef,
) -> Result<SchemaRef, ConnectorError> {
    let text = |name: &str| require(input, name, &[DataType::Utf8], "Utf8");
    if let Valued::Column(name) = &config.routing_key {
        text(name)?;
    }
    if let Some(name) = &config.message_id_column {
        text(name)?;
    }
    if let Some(name) = &config.correlation_id_column {
        text(name)?;
    }
    if let Some(Valued::Column(name)) = &config.priority {
        require(input, name, INTEGERS, "an integer")?;
    }
    for name in &config.header_columns {
        require(
            input,
            name,
            HEADER_TYPES,
            "Utf8, Binary, Boolean, integer, or floating point",
        )?;
    }
    let property_columns = config.property_columns();
    let payload: Vec<_> = input
        .fields()
        .iter()
        .filter(|field| !property_columns.contains(&field.name().as_str()))
        .cloned()
        .collect();
    Ok(Arc::new(Schema::new_with_metadata(
        payload,
        input.metadata().clone(),
    )))
}

const INTEGERS: &[DataType] = &[
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
];

const HEADER_TYPES: &[DataType] = &[
    DataType::Utf8,
    DataType::Binary,
    DataType::Boolean,
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::Float32,
    DataType::Float64,
];

fn require(
    schema: &SchemaRef,
    name: &str,
    allowed: &[DataType],
    expected: &str,
) -> Result<(), ConnectorError> {
    let field = schema.field_with_name(name).map_err(|_| {
        ConnectorError::SchemaMismatch(format!(
            "RabbitMQ property column '{name}' is not in the query"
        ))
    })?;
    if !allowed.contains(field.data_type()) {
        return Err(ConnectorError::SchemaMismatch(format!(
            "RabbitMQ property column '{name}' must be {expected}, got {}",
            field.data_type()
        )));
    }
    Ok(())
}

/// The content type a codec's output carries unless `content.type` overrides it.
pub(super) fn default_content_type(format: Format, payload: &Schema) -> Option<&'static str> {
    match format {
        Format::Json => Some("application/json"),
        Format::Csv => Some("text/csv"),
        Format::Raw => match payload.fields().first().map(|field| field.data_type()) {
            Some(DataType::Utf8) => Some("text/plain; charset=utf-8"),
            _ => Some("application/octet-stream"),
        },
        Format::Debezium | Format::Avro | Format::Fpml => None,
    }
}

/// Properties shared by every message of a sink.
pub(super) fn base_properties(config: &RabbitMqSinkConfig, payload: &Schema) -> BasicProperties {
    let mut properties = BasicProperties::default().with_delivery_mode(config.delivery_mode.amqp());
    let content_type = config
        .content_type
        .as_deref()
        .or_else(|| default_content_type(config.format, payload));
    if let Some(content_type) = content_type {
        properties = properties.with_content_type(content_type.into());
    }
    if let Some(encoding) = &config.content_encoding {
        properties = properties.with_content_encoding(encoding.as_str().into());
    }
    if let Some(Valued::Literal(priority)) = &config.priority {
        properties = properties.with_priority(*priority);
    }
    if let Some(expiration) = config.expiration_ms {
        properties = properties.with_expiration(expiration.to_string().into());
    }
    if let Some(reply_to) = &config.reply_to {
        properties = properties.with_reply_to(reply_to.as_str().into());
    }
    if let Some(kind) = &config.message_type {
        properties = properties.with_type(kind.as_str().into());
    }
    if let Some(app_id) = &config.app_id {
        properties = properties.with_app_id(app_id.as_str().into());
    }
    properties
}

/// Columns of one batch bound to their message properties.
pub(super) struct BoundProperties<'a> {
    routing_key: RoutingKey<'a>,
    message_id: Option<&'a StringArray>,
    correlation_id: Option<&'a StringArray>,
    priority: Option<Vec<Option<u8>>>,
    headers: Vec<(ShortString, &'a ArrayRef)>,
}

enum RoutingKey<'a> {
    Literal(&'a str),
    Column(&'a StringArray),
}

impl<'a> BoundProperties<'a> {
    /// Bind and validate every row of `batch`.
    pub(super) fn bind(
        config: &'a RabbitMqSinkConfig,
        batch: &'a RecordBatch,
    ) -> Result<Self, ConnectorError> {
        let column = |name: &str| {
            batch.column_by_name(name).ok_or_else(|| {
                ConnectorError::SchemaMismatch(format!(
                    "RabbitMQ property column '{name}' is missing"
                ))
            })
        };
        let text = |name: &str| -> Result<&'a StringArray, ConnectorError> {
            let array = batch.column_by_name(name).ok_or_else(|| {
                ConnectorError::SchemaMismatch(format!(
                    "RabbitMQ property column '{name}' is missing"
                ))
            })?;
            let strings = array.as_string_opt::<i32>().ok_or_else(|| {
                ConnectorError::SchemaMismatch(format!(
                    "RabbitMQ property column '{name}' must be Utf8"
                ))
            })?;
            short_strings(strings, name)?;
            Ok(strings)
        };
        let routing_key = match &config.routing_key {
            Valued::Literal(key) => {
                if key.len() > SHORT_STRING_MAX {
                    return Err(ConnectorError::ConfigurationError(
                        "'routing.key' exceeds 255 bytes".into(),
                    ));
                }
                RoutingKey::Literal(key)
            }
            Valued::Column(name) => {
                let keys = text(name)?;
                if keys.null_count() > 0 {
                    return Err(ConnectorError::SchemaMismatch(format!(
                        "routing key column '{name}' contains NULL"
                    )));
                }
                RoutingKey::Column(keys)
            }
        };
        let priority = match &config.priority {
            Some(Valued::Column(name)) => Some(priorities(column(name)?, name)?),
            _ => None,
        };
        let headers = config
            .header_columns
            .iter()
            .map(|name| column(name).map(|array| (ShortString::from(name.as_str()), array)))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            routing_key,
            message_id: config.message_id_column.as_deref().map(text).transpose()?,
            correlation_id: config
                .correlation_id_column
                .as_deref()
                .map(text)
                .transpose()?,
            priority,
            headers,
        })
    }

    /// The routing key of `row`.
    pub(super) fn routing_key(&self, row: usize) -> &str {
        match &self.routing_key {
            RoutingKey::Literal(key) => key,
            RoutingKey::Column(keys) => keys.value(row),
        }
    }

    /// The complete properties of `row`.
    pub(super) fn properties(&self, row: usize, base: &BasicProperties) -> BasicProperties {
        let mut properties = base.clone();
        if let Some(id) = self.message_id.and_then(|ids| value(ids, row)) {
            properties = properties.with_message_id(id.into());
        }
        if let Some(id) = self.correlation_id.and_then(|ids| value(ids, row)) {
            properties = properties.with_correlation_id(id.into());
        }
        if let Some(priority) = self
            .priority
            .as_ref()
            .and_then(|priorities| priorities[row])
        {
            properties = properties.with_priority(priority);
        }
        if !self.headers.is_empty() {
            let mut table = FieldTable::default();
            for (name, array) in &self.headers {
                if let Some(value) = header_value(array, row) {
                    table.insert(name.clone(), value);
                }
            }
            properties = properties.with_headers(table);
        }
        properties
    }
}

fn value(array: &StringArray, row: usize) -> Option<&str> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn short_strings(array: &StringArray, name: &str) -> Result<(), ConnectorError> {
    match array
        .iter()
        .flatten()
        .find(|value| value.len() > SHORT_STRING_MAX)
    {
        Some(_) => Err(ConnectorError::SchemaMismatch(format!(
            "column '{name}' holds a value over the 255-byte AMQP short string limit"
        ))),
        None => Ok(()),
    }
}

fn priorities(array: &ArrayRef, name: &str) -> Result<Vec<Option<u8>>, ConnectorError> {
    let cast = arrow_cast::cast(array, &DataType::Int64).map_err(|error| {
        ConnectorError::SchemaMismatch(format!("priority column '{name}': {error}"))
    })?;
    cast.as_primitive::<Int64Type>()
        .iter()
        .map(|value| {
            value
                .map(|priority| {
                    u8::try_from(priority).map_err(|_| {
                        ConnectorError::SchemaMismatch(format!(
                            "priority column '{name}' holds {priority}, outside 0..=255"
                        ))
                    })
                })
                .transpose()
        })
        .collect()
}

/// The typed AMQP header value of `row`; NULL omits the header.
fn header_value(array: &ArrayRef, row: usize) -> Option<AMQPValue> {
    if array.is_null(row) {
        return None;
    }
    Some(match array.data_type() {
        DataType::Utf8 => {
            AMQPValue::LongString(LongString::from(array.as_string::<i32>().value(row)))
        }
        DataType::Binary => AMQPValue::LongString(LongString::from(
            array.as_binary::<i32>().value(row).to_vec(),
        )),
        DataType::Boolean => AMQPValue::Boolean(array.as_boolean().value(row)),
        DataType::Int8 => AMQPValue::ShortShortInt(array.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => AMQPValue::ShortInt(array.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => AMQPValue::LongInt(array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => AMQPValue::LongLongInt(array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => AMQPValue::ShortShortUInt(array.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => AMQPValue::ShortUInt(array.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => AMQPValue::LongUInt(array.as_primitive::<UInt32Type>().value(row)),
        DataType::Float32 => AMQPValue::Float(array.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => AMQPValue::Double(array.as_primitive::<Float64Type>().value(row)),
        // Schema validation admits only the types above.
        _ => return None,
    })
}
