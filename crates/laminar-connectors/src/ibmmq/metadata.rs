//! Typed `MQMD` fields and message properties: selectable columns appended after the payload.
//!
//! Identifiers stay binary (24-byte `MsgId`, `CorrelId`, `GroupId`), numeric fields keep their
//! MQI units and sentinels (`Expiry` in tenths of a second, `-1` unlimited), and character fields
//! are trimmed of MQ's space padding. `UserIdentifier` and `PutApplName` are the received
//! context as the queue manager recorded it.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, FixedSizeBinaryBuilder, Int32Builder, ListBuilder, StringBuilder, StructBuilder,
    TimestampMillisecondBuilder,
};
use arrow_array::ArrayRef;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

use crate::error::ConnectorError;

/// Message properties as `(name, value bytes)`; a NULL value is `None`.
pub(super) type RenderedProperties = Vec<(String, Option<Vec<u8>>)>;

/// A received message: payload, descriptor, and properties.
pub(super) struct Received {
    pub(super) data: Vec<u8>,
    pub(super) md: libmqm_sys::MQMD,
    pub(super) properties: RenderedProperties,
}

/// One selectable metadata column, in canonical output order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MetadataColumn {
    /// `_msg_id`: 24-byte `MsgId`.
    MsgId,
    /// `_correl_id`: 24-byte `CorrelId`.
    CorrelId,
    /// `_group_id`: 24-byte `GroupId`.
    GroupId,
    /// `_msg_seq_number`: `MsgSeqNumber` within a group.
    MsgSeqNumber,
    /// `_msg_flags`: `MsgFlags` (group and segment flags).
    MsgFlags,
    /// `_msg_type`: `MsgType`.
    MsgType,
    /// `_format`: `Format`, trimmed.
    Format,
    /// `_ccsid`: `CodedCharSetId` of the delivered payload.
    Ccsid,
    /// `_encoding`: `Encoding` of the delivered payload.
    Encoding,
    /// `_persistence`: `Persistence` (0 not persistent, 1 persistent).
    Persistence,
    /// `_priority`: `Priority`.
    Priority,
    /// `_expiry`: remaining `Expiry` at retrieval, in tenths of a second; `-1` is unlimited.
    Expiry,
    /// `_backout_count`: `BackoutCount`, the number of earlier backed-out retrievals.
    BackoutCount,
    /// `_reply_to_queue`: `ReplyToQ`.
    ReplyToQueue,
    /// `_reply_to_queue_manager`: `ReplyToQMgr`.
    ReplyToQueueManager,
    /// `_put_timestamp`: `PutDate` and `PutTime` (GMT) as milliseconds since the epoch.
    PutTimestamp,
    /// `_put_application_name`: `PutApplName`.
    PutApplicationName,
    /// `_user_identifier`: `UserIdentifier`.
    UserIdentifier,
    /// `_properties`: message properties, values rendered as bytes.
    Properties,
}

impl MetadataColumn {
    const ALL: [Self; 19] = [
        Self::MsgId,
        Self::CorrelId,
        Self::GroupId,
        Self::MsgSeqNumber,
        Self::MsgFlags,
        Self::MsgType,
        Self::Format,
        Self::Ccsid,
        Self::Encoding,
        Self::Persistence,
        Self::Priority,
        Self::Expiry,
        Self::BackoutCount,
        Self::ReplyToQueue,
        Self::ReplyToQueueManager,
        Self::PutTimestamp,
        Self::PutApplicationName,
        Self::UserIdentifier,
        Self::Properties,
    ];

    /// Option name used in `metadata.columns`.
    #[must_use]
    pub const fn option_name(self) -> &'static str {
        match self {
            Self::MsgId => "msg_id",
            Self::CorrelId => "correl_id",
            Self::GroupId => "group_id",
            Self::MsgSeqNumber => "msg_seq_number",
            Self::MsgFlags => "msg_flags",
            Self::MsgType => "msg_type",
            Self::Format => "format",
            Self::Ccsid => "ccsid",
            Self::Encoding => "encoding",
            Self::Persistence => "persistence",
            Self::Priority => "priority",
            Self::Expiry => "expiry",
            Self::BackoutCount => "backout_count",
            Self::ReplyToQueue => "reply_to_queue",
            Self::ReplyToQueueManager => "reply_to_queue_manager",
            Self::PutTimestamp => "put_timestamp",
            Self::PutApplicationName => "put_application_name",
            Self::UserIdentifier => "user_identifier",
            Self::Properties => "properties",
        }
    }

    fn field(self) -> Field {
        let name = format!("_{}", self.option_name());
        match self {
            Self::MsgId | Self::CorrelId | Self::GroupId => {
                Field::new(name, DataType::FixedSizeBinary(24), false)
            }
            Self::Format
            | Self::ReplyToQueue
            | Self::ReplyToQueueManager
            | Self::PutApplicationName
            | Self::UserIdentifier => Field::new(name, DataType::Utf8, false),
            Self::PutTimestamp => {
                Field::new(name, DataType::Timestamp(TimeUnit::Millisecond, None), true)
            }
            Self::Properties => Field::new(
                name,
                DataType::List(Arc::new(Field::new_struct(
                    "item",
                    property_fields(),
                    false,
                ))),
                false,
            ),
            Self::MsgSeqNumber
            | Self::MsgFlags
            | Self::MsgType
            | Self::Ccsid
            | Self::Encoding
            | Self::Persistence
            | Self::Priority
            | Self::Expiry
            | Self::BackoutCount => Field::new(name, DataType::Int32, false),
        }
    }
}

fn property_fields() -> Fields {
    Fields::from(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Binary, true),
    ])
}

/// The metadata columns a source appends, in canonical order without duplicates.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetadataColumns(Vec<MetadataColumn>);

impl MetadataColumns {
    /// Parse a comma-separated `metadata.columns` list such as `msg_id,backout_count`.
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
                        "unknown IBM MQ metadata column '{name}' (expected one of {})",
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

    /// Whether message properties are selected.
    #[must_use]
    pub fn wants_properties(&self) -> bool {
        self.0.contains(&MetadataColumn::Properties)
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
                "IBM MQ payload schema contains configured metadata column '{}'",
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
                "IBM MQ source schema does not end with its configured metadata columns".into(),
            ));
        }
        Ok(payload)
    }

    /// Build the selected metadata arrays, one row per message.
    pub(super) fn arrays(&self, messages: &[&Received]) -> Result<Vec<ArrayRef>, ConnectorError> {
        self.0
            .iter()
            .map(|column| column_array(*column, messages))
            .collect()
    }
}

impl std::fmt::Display for MetadataColumns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.0.iter().map(|column| column.option_name()).collect();
        f.write_str(&names.join(","))
    }
}

/// MQ character fields are platform `MQCHAR` (signed or unsigned); padding is spaces or NULs.
pub(super) fn text(field: &[mqi::types::MQCHAR]) -> String {
    let bytes: Vec<u8> = field
        .iter()
        .map(|character| u8::from_ne_bytes(character.to_ne_bytes()))
        .collect();
    String::from_utf8_lossy(&bytes)
        .trim_end_matches([' ', '\0'])
        .to_string()
}

fn column_array(
    column: MetadataColumn,
    messages: &[&Received],
) -> Result<ArrayRef, ConnectorError> {
    let count = messages.len();
    let id = |value: fn(&libmqm_sys::MQMD) -> &[u8; 24]| -> Result<ArrayRef, ConnectorError> {
        let mut builder = FixedSizeBinaryBuilder::with_capacity(count, 24);
        for message in messages {
            builder
                .append_value(value(&message.md))
                .map_err(|error| ConnectorError::Internal(error.to_string()))?;
        }
        Ok(Arc::new(builder.finish()))
    };
    let int = |value: fn(&libmqm_sys::MQMD) -> i32| -> ArrayRef {
        let mut builder = Int32Builder::with_capacity(count);
        for message in messages {
            builder.append_value(value(&message.md));
        }
        Arc::new(builder.finish())
    };
    let string = |value: fn(&libmqm_sys::MQMD) -> String| -> ArrayRef {
        let mut builder = StringBuilder::with_capacity(count, count * 16);
        for message in messages {
            builder.append_value(value(&message.md));
        }
        Arc::new(builder.finish())
    };
    Ok(match column {
        MetadataColumn::MsgId => id(|md| &md.MsgId)?,
        MetadataColumn::CorrelId => id(|md| &md.CorrelId)?,
        MetadataColumn::GroupId => id(|md| &md.GroupId)?,
        MetadataColumn::MsgSeqNumber => int(|md| md.MsgSeqNumber),
        MetadataColumn::MsgFlags => int(|md| md.MsgFlags),
        MetadataColumn::MsgType => int(|md| md.MsgType),
        MetadataColumn::Format => string(|md| text(&md.Format)),
        MetadataColumn::Ccsid => int(|md| md.CodedCharSetId),
        MetadataColumn::Encoding => int(|md| md.Encoding),
        MetadataColumn::Persistence => int(|md| md.Persistence),
        MetadataColumn::Priority => int(|md| md.Priority),
        MetadataColumn::Expiry => int(|md| md.Expiry),
        MetadataColumn::BackoutCount => int(|md| md.BackoutCount),
        MetadataColumn::ReplyToQueue => string(|md| text(&md.ReplyToQ)),
        MetadataColumn::ReplyToQueueManager => string(|md| text(&md.ReplyToQMgr)),
        MetadataColumn::PutApplicationName => string(|md| text(&md.PutApplName)),
        MetadataColumn::UserIdentifier => string(|md| text(&md.UserIdentifier)),
        MetadataColumn::PutTimestamp => {
            let mut builder = TimestampMillisecondBuilder::with_capacity(count);
            for message in messages {
                builder.append_option(put_timestamp(
                    &text(&message.md.PutDate),
                    &text(&message.md.PutTime),
                ));
            }
            Arc::new(builder.finish())
        }
        MetadataColumn::Properties => properties_array(messages),
    })
}

/// `PutDate` `YYYYMMDD` and `PutTime` `HHMMSSTH` (hundredths), GMT; `None` when blank or invalid.
pub(super) fn put_timestamp(date: &str, time: &str) -> Option<i64> {
    let date = chrono::NaiveDate::parse_from_str(date, "%Y%m%d").ok()?;
    if time.len() != 8 || !time.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let field = |range: std::ops::Range<usize>| time[range].parse::<u32>().ok();
    let moment =
        date.and_hms_milli_opt(field(0..2)?, field(2..4)?, field(4..6)?, field(6..8)? * 10)?;
    Some(moment.and_utc().timestamp_millis())
}

fn properties_array(messages: &[&Received]) -> ArrayRef {
    let entry = StructBuilder::new(
        property_fields(),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(BinaryBuilder::new()),
        ],
    );
    let mut builder = ListBuilder::new(entry).with_field(Arc::new(Field::new_struct(
        "item",
        property_fields(),
        false,
    )));
    for message in messages {
        for (name, value) in &message.properties {
            let entry = builder.values();
            entry
                .field_builder::<StringBuilder>(0)
                .expect("property name builder is Utf8")
                .append_value(name);
            entry
                .field_builder::<BinaryBuilder>(1)
                .expect("property value builder is Binary")
                .append_option(value.as_deref());
            entry.append(true);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_timestamps_use_hundredths_and_reject_blanks() {
        assert_eq!(
            put_timestamp("20261017", "10305912"),
            Some(
                chrono::NaiveDate::from_ymd_opt(2026, 10, 17)
                    .unwrap()
                    .and_hms_milli_opt(10, 30, 59, 120)
                    .unwrap()
                    .and_utc()
                    .timestamp_millis()
            )
        );
        assert_eq!(put_timestamp("", ""), None);
        assert_eq!(put_timestamp("20261017", "2530xx00"), None);
    }

    #[test]
    fn character_fields_trim_mq_padding() {
        let raw: Vec<mqi::types::MQCHAR> = b"MQSTR   "
            .iter()
            .map(|byte| mqi::types::MQCHAR::from_ne_bytes(byte.to_ne_bytes()))
            .collect();
        assert_eq!(text(&raw), "MQSTR");
    }

    #[test]
    fn metadata_columns_are_canonical_and_round_trip() {
        let columns = MetadataColumns::parse(Some("backout_count,msg_id,MSG_ID")).unwrap();
        assert_eq!(columns.to_string(), "msg_id,backout_count");
        let payload = Arc::new(Schema::new(vec![Field::new("v", DataType::Binary, true)]));
        let output = columns.output_schema(&payload);
        assert_eq!(output.field(1).data_type(), &DataType::FixedSizeBinary(24));
        assert_eq!(columns.payload_schema(&output).unwrap(), payload);
        assert!(MetadataColumns::parse(Some("offset")).is_err());
    }
}
