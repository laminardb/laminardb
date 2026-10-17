//! Typed AMQP delivery metadata: selectable columns appended after the decoded payload columns.
//!
//! Values are the broker/publisher properties as delivered, so a redelivered message reproduces
//! them. No receive-time value is exposed because it would change on replay.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
    TimestampMillisecondBuilder, UInt8Builder,
};
use arrow_array::ArrayRef;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use lapin::message::Delivery;
use lapin::types::{AMQPValue, ShortString};

use crate::error::ConnectorError;

/// One selectable metadata column, in canonical output order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MetadataColumn {
    /// `_exchange`: the exchange the message was published to (empty for the default exchange).
    Exchange,
    /// `_routing_key`: the publish routing key.
    RoutingKey,
    /// `_redelivered`: the broker's redelivery flag.
    Redelivered,
    /// `_delivery_count`: the `x-delivery-count` header quorum queues maintain, else NULL.
    DeliveryCount,
    /// `_message_id`: the `message-id` property.
    MessageId,
    /// `_correlation_id`: the `correlation-id` property.
    CorrelationId,
    /// `_timestamp`: the `timestamp` property (whole seconds) as milliseconds since the epoch.
    Timestamp,
    /// `_content_type`: the `content-type` property.
    ContentType,
    /// `_content_encoding`: the `content-encoding` property.
    ContentEncoding,
    /// `_delivery_mode`: the `delivery-mode` property (1 transient, 2 persistent).
    DeliveryMode,
    /// `_priority`: the `priority` property.
    Priority,
    /// `_expiration`: the `expiration` property, the per-message TTL in milliseconds as text.
    Expiration,
    /// `_reply_to`: the `reply-to` property.
    ReplyTo,
    /// `_type`: the `type` property.
    Type,
    /// `_user_id`: the broker-validated `user-id` property.
    UserId,
    /// `_app_id`: the `app-id` property.
    AppId,
    /// `_headers`: every header entry in table order with its value rendered as bytes.
    Headers,
}

impl MetadataColumn {
    const ALL: [Self; 17] = [
        Self::Exchange,
        Self::RoutingKey,
        Self::Redelivered,
        Self::DeliveryCount,
        Self::MessageId,
        Self::CorrelationId,
        Self::Timestamp,
        Self::ContentType,
        Self::ContentEncoding,
        Self::DeliveryMode,
        Self::Priority,
        Self::Expiration,
        Self::ReplyTo,
        Self::Type,
        Self::UserId,
        Self::AppId,
        Self::Headers,
    ];

    /// Option name used in `metadata.columns`.
    #[must_use]
    pub const fn option_name(self) -> &'static str {
        match self {
            Self::Exchange => "exchange",
            Self::RoutingKey => "routing_key",
            Self::Redelivered => "redelivered",
            Self::DeliveryCount => "delivery_count",
            Self::MessageId => "message_id",
            Self::CorrelationId => "correlation_id",
            Self::Timestamp => "timestamp",
            Self::ContentType => "content_type",
            Self::ContentEncoding => "content_encoding",
            Self::DeliveryMode => "delivery_mode",
            Self::Priority => "priority",
            Self::Expiration => "expiration",
            Self::ReplyTo => "reply_to",
            Self::Type => "type",
            Self::UserId => "user_id",
            Self::AppId => "app_id",
            Self::Headers => "headers",
        }
    }

    fn field(self) -> Field {
        let name = format!("_{}", self.option_name());
        match self {
            Self::Exchange | Self::RoutingKey => Field::new(name, DataType::Utf8, false),
            Self::Redelivered => Field::new(name, DataType::Boolean, false),
            Self::DeliveryCount => Field::new(name, DataType::Int64, true),
            Self::Timestamp => {
                Field::new(name, DataType::Timestamp(TimeUnit::Millisecond, None), true)
            }
            Self::DeliveryMode | Self::Priority => Field::new(name, DataType::UInt8, true),
            Self::Headers => Field::new(
                name,
                DataType::List(Arc::new(Field::new_struct("item", header_fields(), false))),
                false,
            ),
            Self::MessageId
            | Self::CorrelationId
            | Self::ContentType
            | Self::ContentEncoding
            | Self::Expiration
            | Self::ReplyTo
            | Self::Type
            | Self::UserId
            | Self::AppId => Field::new(name, DataType::Utf8, true),
        }
    }
}

fn header_fields() -> Fields {
    Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Binary, true),
    ])
}

/// The metadata columns a source appends, in canonical order without duplicates.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetadataColumns(Vec<MetadataColumn>);

impl MetadataColumns {
    /// Parse a comma-separated `metadata.columns` list such as `routing_key,headers`.
    ///
    /// # Errors
    /// Returns a configuration error for an unknown name.
    pub fn parse(list: Option<&str>) -> Result<Self, ConnectorError> {
        let mut columns = Vec::new();
        for name in list
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            let column = MetadataColumn::ALL
                .into_iter()
                .find(|column| column.option_name().eq_ignore_ascii_case(name))
                .ok_or_else(|| {
                    let known: Vec<&str> = MetadataColumn::ALL
                        .iter()
                        .map(|c| c.option_name())
                        .collect();
                    ConnectorError::ConfigurationError(format!(
                        "unknown RabbitMQ metadata column '{name}' (expected one of {})",
                        known.join(", ")
                    ))
                })?;
            columns.push(column);
        }
        columns.sort_unstable();
        columns.dedup();
        Ok(Self(columns))
    }

    /// Whether no metadata column is selected.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Reject a payload field that collides, case-insensitively, with a selected column.
    ///
    /// # Errors
    /// Returns a schema mismatch naming the colliding field.
    pub fn validate_payload(&self, payload: &Schema) -> Result<(), ConnectorError> {
        let collision = payload.fields().iter().find(|field| {
            self.0.iter().any(|column| {
                field
                    .name()
                    .eq_ignore_ascii_case(&format!("_{}", column.option_name()))
            })
        });
        match collision {
            Some(field) => Err(ConnectorError::SchemaMismatch(format!(
                "RabbitMQ payload schema contains configured metadata column '{}'",
                field.name()
            ))),
            None => Ok(()),
        }
    }

    /// The payload schema followed by the selected metadata fields.
    #[must_use]
    pub fn output_schema(&self, payload: &SchemaRef) -> SchemaRef {
        if self.0.is_empty() {
            return Arc::clone(payload);
        }
        let mut fields = payload.fields().to_vec();
        fields.extend(self.0.iter().map(|column| Arc::new(column.field())));
        Arc::new(Schema::new_with_metadata(
            fields,
            payload.metadata().clone(),
        ))
    }

    /// The payload prefix of an output schema produced by [`Self::output_schema`].
    ///
    /// # Errors
    /// Returns a schema mismatch when `output` does not end with the selected metadata fields.
    pub fn payload_schema(&self, output: &SchemaRef) -> Result<SchemaRef, ConnectorError> {
        let payload_len = output
            .fields()
            .len()
            .checked_sub(self.0.len())
            .ok_or_else(|| {
                ConnectorError::SchemaMismatch("source schema lacks its metadata columns".into())
            })?;
        let payload = Arc::new(
            output
                .project(&(0..payload_len).collect::<Vec<_>>())
                .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))?,
        );
        if self.output_schema(&payload) != *output {
            return Err(ConnectorError::SchemaMismatch(
                "RabbitMQ source schema does not end with its configured metadata columns".into(),
            ));
        }
        Ok(payload)
    }

    /// Build the selected metadata arrays, one row per delivery.
    ///
    /// # Errors
    /// Returns a read error when a property cannot be represented without loss.
    pub(super) fn arrays(&self, deliveries: &[&Delivery]) -> Result<Vec<ArrayRef>, ConnectorError> {
        self.0
            .iter()
            .map(|column| column_array(*column, deliveries))
            .collect()
    }
}

impl std::fmt::Display for MetadataColumns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.0.iter().map(|column| column.option_name()).collect();
        f.write_str(&names.join(","))
    }
}

fn column_array(
    column: MetadataColumn,
    deliveries: &[&Delivery],
) -> Result<ArrayRef, ConnectorError> {
    let text = |value: fn(&Delivery) -> Option<&ShortString>| -> ArrayRef {
        let mut builder = StringBuilder::with_capacity(deliveries.len(), deliveries.len() * 16);
        for delivery in deliveries {
            builder.append_option(value(delivery).map(ShortString::as_str));
        }
        Arc::new(builder.finish())
    };
    let octet = |value: fn(&Delivery) -> Option<u8>| -> ArrayRef {
        let mut builder = UInt8Builder::with_capacity(deliveries.len());
        for delivery in deliveries {
            builder.append_option(value(delivery));
        }
        Arc::new(builder.finish())
    };
    Ok(match column {
        MetadataColumn::Exchange => text(|delivery| Some(&delivery.exchange)),
        MetadataColumn::RoutingKey => text(|delivery| Some(&delivery.routing_key)),
        MetadataColumn::Redelivered => {
            let mut builder = BooleanBuilder::with_capacity(deliveries.len());
            for delivery in deliveries {
                builder.append_value(delivery.redelivered);
            }
            Arc::new(builder.finish())
        }
        MetadataColumn::DeliveryCount => {
            let mut builder = Int64Builder::with_capacity(deliveries.len());
            for delivery in deliveries {
                builder.append_option(delivery_count(delivery)?);
            }
            Arc::new(builder.finish())
        }
        MetadataColumn::MessageId => text(|delivery| delivery.properties.message_id().as_ref()),
        MetadataColumn::CorrelationId => {
            text(|delivery| delivery.properties.correlation_id().as_ref())
        }
        MetadataColumn::Timestamp => {
            let mut builder = TimestampMillisecondBuilder::with_capacity(deliveries.len());
            for delivery in deliveries {
                let millis = delivery
                    .properties
                    .timestamp()
                    .map(|seconds| {
                        i64::try_from(seconds)
                            .ok()
                            .and_then(|seconds| seconds.checked_mul(1_000))
                            .ok_or_else(|| {
                                ConnectorError::ReadError(format!(
                                    "AMQP timestamp {seconds} exceeds the millisecond range"
                                ))
                            })
                    })
                    .transpose()?;
                builder.append_option(millis);
            }
            Arc::new(builder.finish())
        }
        MetadataColumn::ContentType => text(|delivery| delivery.properties.content_type().as_ref()),
        MetadataColumn::ContentEncoding => {
            text(|delivery| delivery.properties.content_encoding().as_ref())
        }
        MetadataColumn::DeliveryMode => octet(|delivery| *delivery.properties.delivery_mode()),
        MetadataColumn::Priority => octet(|delivery| *delivery.properties.priority()),
        MetadataColumn::Expiration => text(|delivery| delivery.properties.expiration().as_ref()),
        MetadataColumn::ReplyTo => text(|delivery| delivery.properties.reply_to().as_ref()),
        MetadataColumn::Type => text(|delivery| delivery.properties.kind().as_ref()),
        MetadataColumn::UserId => text(|delivery| delivery.properties.user_id().as_ref()),
        MetadataColumn::AppId => text(|delivery| delivery.properties.app_id().as_ref()),
        MetadataColumn::Headers => headers_array(deliveries),
    })
}

fn delivery_count(delivery: &Delivery) -> Result<Option<i64>, ConnectorError> {
    let Some(value) = delivery
        .properties
        .headers()
        .as_ref()
        .and_then(|headers| headers.inner().get(&ShortString::from("x-delivery-count")))
    else {
        return Ok(None);
    };
    integer(value).map(Some).ok_or_else(|| {
        ConnectorError::ReadError("x-delivery-count header is not an integer".into())
    })
}

fn integer(value: &AMQPValue) -> Option<i64> {
    match value {
        AMQPValue::ShortShortInt(v) => Some(i64::from(*v)),
        AMQPValue::ShortShortUInt(v) => Some(i64::from(*v)),
        AMQPValue::ShortInt(v) => Some(i64::from(*v)),
        AMQPValue::ShortUInt(v) => Some(i64::from(*v)),
        AMQPValue::LongInt(v) => Some(i64::from(*v)),
        AMQPValue::LongUInt(v) => Some(i64::from(*v)),
        AMQPValue::LongLongInt(v) => Some(*v),
        _ => None,
    }
}

fn headers_array(deliveries: &[&Delivery]) -> ArrayRef {
    let entry = StructBuilder::new(
        header_fields(),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(BinaryBuilder::new()),
        ],
    );
    let mut builder = ListBuilder::new(entry).with_field(Arc::new(Field::new_struct(
        "item",
        header_fields(),
        false,
    )));
    for delivery in deliveries {
        if let Some(headers) = delivery.properties.headers() {
            for (key, value) in headers {
                let entry = builder.values();
                entry
                    .field_builder::<StringBuilder>(0)
                    .expect("header key builder is Utf8")
                    .append_value(key.as_str());
                entry
                    .field_builder::<BinaryBuilder>(1)
                    .expect("header value builder is Binary")
                    .append_option(render_header(value));
                entry.append(true);
            }
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

/// Header value bytes: strings and byte arrays verbatim, scalars as their decimal or
/// `true`/`false` text, timestamps as seconds, decimals as `value` scaled by `10^-scale`
/// (`12345e-2`), nested tables and arrays as JSON, and `void` as NULL.
pub(super) fn render_header(value: &AMQPValue) -> Option<Vec<u8>> {
    match value {
        AMQPValue::LongString(text) => Some(text.as_bytes().to_vec()),
        AMQPValue::ShortString(text) => Some(text.as_str().as_bytes().to_vec()),
        AMQPValue::ByteArray(bytes) => Some(bytes.as_slice().to_vec()),
        AMQPValue::Void => None,
        AMQPValue::FieldTable(_) | AMQPValue::FieldArray(_) => {
            Some(json_value(value).to_string().into_bytes())
        }
        scalar => Some(scalar_text(scalar).into_bytes()),
    }
}

fn scalar_text(value: &AMQPValue) -> String {
    if let Some(integer) = integer(value) {
        return integer.to_string();
    }
    match value {
        AMQPValue::Boolean(flag) => flag.to_string(),
        AMQPValue::Float(float) => float.to_string(),
        AMQPValue::Double(double) => double.to_string(),
        AMQPValue::Timestamp(seconds) => seconds.to_string(),
        AMQPValue::DecimalValue(decimal) => format!("{}e-{}", decimal.value, decimal.scale),
        _ => String::new(),
    }
}

fn json_value(value: &AMQPValue) -> serde_json::Value {
    match value {
        AMQPValue::FieldTable(table) => serde_json::Value::Object(
            table
                .into_iter()
                .map(|(key, value)| (key.as_str().to_string(), json_value(value)))
                .collect(),
        ),
        AMQPValue::FieldArray(array) => {
            serde_json::Value::Array(array.as_slice().iter().map(json_value).collect())
        }
        AMQPValue::Void => serde_json::Value::Null,
        AMQPValue::Boolean(flag) => serde_json::Value::Bool(*flag),
        other => integer(other).map_or_else(
            || {
                render_header(other).map_or(serde_json::Value::Null, |bytes| {
                    serde_json::Value::String(String::from_utf8_lossy(&bytes).into_owned())
                })
            },
            serde_json::Value::from,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lapin::types::{DecimalValue, FieldArray, FieldTable, LongString};

    #[test]
    fn parse_is_canonical_and_rejects_unknown_names() {
        let columns = MetadataColumns::parse(Some("headers, exchange,exchange")).unwrap();
        assert_eq!(columns.to_string(), "exchange,headers");
        assert!(MetadataColumns::parse(Some("partition")).is_err());
        assert!(MetadataColumns::parse(None).unwrap().is_empty());
    }

    #[test]
    fn output_schema_round_trips_and_rejects_collisions() {
        let columns = MetadataColumns::parse(Some("routing_key,priority")).unwrap();
        let payload = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let output = columns.output_schema(&payload);
        assert_eq!(output.fields().len(), 3);
        assert_eq!(columns.payload_schema(&output).unwrap(), payload);
        assert!(columns.payload_schema(&payload).is_err());
        let colliding = Schema::new(vec![Field::new("_ROUTING_KEY", DataType::Utf8, false)]);
        assert!(columns.validate_payload(&colliding).is_err());
    }

    #[test]
    fn header_rendering_keeps_bytes_and_native_scalars() {
        assert_eq!(
            render_header(&AMQPValue::LongString(LongString::from(vec![0xff, 0x00]))),
            Some(vec![0xff, 0x00])
        );
        assert_eq!(
            render_header(&AMQPValue::LongLongInt(-7)),
            Some(b"-7".to_vec())
        );
        assert_eq!(
            render_header(&AMQPValue::Boolean(true)),
            Some(b"true".to_vec())
        );
        assert_eq!(render_header(&AMQPValue::Void), None);
        assert_eq!(
            render_header(&AMQPValue::DecimalValue(DecimalValue {
                scale: 2,
                value: 12345
            })),
            Some(b"12345e-2".to_vec())
        );
        let mut nested = FieldTable::default();
        nested.insert("n".into(), AMQPValue::LongInt(3));
        nested.insert(
            "list".into(),
            AMQPValue::FieldArray(FieldArray::from(vec![AMQPValue::Boolean(false)])),
        );
        assert_eq!(
            render_header(&AMQPValue::FieldTable(nested)),
            Some(br#"{"list":[false],"n":3}"#.to_vec())
        );
    }
}
