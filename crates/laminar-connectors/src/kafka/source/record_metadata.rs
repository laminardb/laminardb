//! Typed Kafka record metadata: the selectable columns appended after the decoded value columns,
//! and the per-row values captured for them.

use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, ListBuilder, StringBuilder, StructBuilder};
use arrow_array::{
    ArrayRef, BinaryArray, Int32Array, Int64Array, StringArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

use crate::error::ConnectorError;

/// One selectable metadata column, in canonical output order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KafkaMetadataColumn {
    /// `_key`: the record key bytes, NULL for a record without a key.
    Key,
    /// `_topic`: the source topic.
    Topic,
    /// `_partition`: the source partition.
    Partition,
    /// `_offset`: the record offset.
    Offset,
    /// `_timestamp`: the record timestamp in milliseconds, NULL when the broker reports none.
    Timestamp,
    /// `_timestamp_type`: `create_time`, `log_append_time` or `not_available`.
    TimestampType,
    /// `_headers`: every header in order, duplicates kept, values as nullable bytes.
    Headers,
}

impl KafkaMetadataColumn {
    const ALL: [Self; 7] = [
        Self::Key,
        Self::Topic,
        Self::Partition,
        Self::Offset,
        Self::Timestamp,
        Self::TimestampType,
        Self::Headers,
    ];

    /// Output column name.
    #[must_use]
    pub const fn column_name(self) -> &'static str {
        match self {
            Self::Key => "_key",
            Self::Topic => "_topic",
            Self::Partition => "_partition",
            Self::Offset => "_offset",
            Self::Timestamp => "_timestamp",
            Self::TimestampType => "_timestamp_type",
            Self::Headers => "_headers",
        }
    }

    const fn option_name(self) -> &'static str {
        match self {
            Self::Key => "key",
            Self::Topic => "topic",
            Self::Partition => "partition",
            Self::Offset => "offset",
            Self::Timestamp => "timestamp",
            Self::TimestampType => "timestamp_type",
            Self::Headers => "headers",
        }
    }

    fn field(self) -> Field {
        match self {
            Self::Key => Field::new(self.column_name(), DataType::Binary, true),
            Self::Topic | Self::TimestampType => {
                Field::new(self.column_name(), DataType::Utf8, false)
            }
            Self::Partition => Field::new(self.column_name(), DataType::Int32, false),
            Self::Offset => Field::new(self.column_name(), DataType::Int64, false),
            Self::Timestamp => Field::new(
                self.column_name(),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Self::Headers => Field::new(
                self.column_name(),
                DataType::List(Arc::new(Field::new_struct("item", header_fields(), false))),
                false,
            ),
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
pub struct KafkaMetadataColumns(Vec<KafkaMetadataColumn>);

impl KafkaMetadataColumns {
    /// Parse a comma-separated `metadata.columns` list such as `key,offset,headers`.
    ///
    /// # Errors
    /// Returns a configuration error for an unknown name.
    pub fn parse(list: &str) -> Result<Self, ConnectorError> {
        let mut columns = Vec::new();
        for name in list
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            let column = KafkaMetadataColumn::ALL
                .into_iter()
                .find(|column| column.option_name().eq_ignore_ascii_case(name))
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError(format!(
                        "unknown Kafka metadata column '{name}' (expected key, topic, partition, \
                         offset, timestamp, timestamp_type or headers)"
                    ))
                })?;
            columns.push(column);
        }
        Ok(Self::from_columns(columns))
    }

    /// Select `columns`, normalized to canonical order.
    #[must_use]
    pub fn from_columns(mut columns: Vec<KafkaMetadataColumn>) -> Self {
        columns.sort_unstable();
        columns.dedup();
        Self(columns)
    }

    /// Whether `column` is selected.
    #[must_use]
    pub fn contains(&self, column: KafkaMetadataColumn) -> bool {
        self.0.contains(&column)
    }

    /// Whether no metadata column is selected.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Number of selected columns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// The canonical `metadata.columns` option value.
    #[must_use]
    pub fn option_value(&self) -> String {
        self.0
            .iter()
            .map(|column| column.option_name())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Reject a payload field that collides, case-insensitively, with a selected column.
    ///
    /// # Errors
    /// Returns a schema mismatch naming the colliding field.
    pub fn validate_payload(&self, payload: &Schema) -> Result<(), ConnectorError> {
        let collision = payload.fields().iter().find(|field| {
            self.0
                .iter()
                .any(|column| field.name().eq_ignore_ascii_case(column.column_name()))
        });
        match collision {
            Some(field) => Err(ConnectorError::SchemaMismatch(format!(
                "Kafka payload schema contains configured connector metadata column '{}'",
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

    /// Build the selected metadata arrays for `rows`.
    pub(super) fn arrays(
        &self,
        rows: &[KafkaRowMetadata],
    ) -> Result<Vec<ArrayRef>, ConnectorError> {
        self.0
            .iter()
            .map(|column| column_array(*column, rows))
            .collect()
    }
}

/// How the broker assigned a record's timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KafkaTimestampType {
    NotAvailable,
    CreateTime,
    LogAppendTime,
}

impl KafkaTimestampType {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NotAvailable => "not_available",
            Self::CreateTime => "create_time",
            Self::LogAppendTime => "log_append_time",
        }
    }
}

/// One record header: a UTF-8 name and nullable binary value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct KafkaHeader {
    pub(super) key: String,
    pub(super) value: Option<Vec<u8>>,
}

/// Metadata captured for one emitted row. `key` and `headers` are populated only when selected.
#[derive(Debug, Clone)]
pub(super) struct KafkaRowMetadata {
    pub(super) key: Option<Vec<u8>>,
    pub(super) topic: Arc<str>,
    pub(super) partition: i32,
    pub(super) offset: i64,
    pub(super) timestamp_ms: Option<i64>,
    pub(super) timestamp_type: KafkaTimestampType,
    pub(super) headers: Vec<KafkaHeader>,
}

fn column_array(
    column: KafkaMetadataColumn,
    rows: &[KafkaRowMetadata],
) -> Result<ArrayRef, ConnectorError> {
    let array: ArrayRef = match column {
        KafkaMetadataColumn::Key => Arc::new(
            rows.iter()
                .map(|row| row.key.as_deref())
                .collect::<BinaryArray>(),
        ),
        KafkaMetadataColumn::Topic => Arc::new(
            rows.iter()
                .map(|row| Some(row.topic.as_ref()))
                .collect::<StringArray>(),
        ),
        KafkaMetadataColumn::Partition => {
            Arc::new(rows.iter().map(|row| row.partition).collect::<Int32Array>())
        }
        KafkaMetadataColumn::Offset => {
            Arc::new(rows.iter().map(|row| row.offset).collect::<Int64Array>())
        }
        KafkaMetadataColumn::Timestamp => Arc::new(
            rows.iter()
                .map(|row| row.timestamp_ms)
                .collect::<TimestampMillisecondArray>(),
        ),
        KafkaMetadataColumn::TimestampType => Arc::new(
            rows.iter()
                .map(|row| Some(row.timestamp_type.as_str()))
                .collect::<StringArray>(),
        ),
        KafkaMetadataColumn::Headers => headers_array(rows)?,
    };
    Ok(array)
}

fn headers_array(rows: &[KafkaRowMetadata]) -> Result<ArrayRef, ConnectorError> {
    let entries = StructBuilder::new(
        header_fields(),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(BinaryBuilder::new()),
        ],
    );
    let item = Field::new_struct("item", header_fields(), false);
    let mut list = ListBuilder::new(entries).with_field(Arc::new(item));
    for row in rows {
        let entries = list.values();
        for header in &row.headers {
            entries
                .field_builder::<StringBuilder>(0)
                .ok_or_else(|| ConnectorError::Internal("header key builder".into()))?
                .append_value(&header.key);
            entries
                .field_builder::<BinaryBuilder>(1)
                .ok_or_else(|| ConnectorError::Internal("header value builder".into()))?
                .append_option(header.value.as_deref());
            entries.append(true);
        }
        list.append(true);
    }
    Ok(Arc::new(list.finish()))
}

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray;
    use arrow_array::{Array, RecordBatch};

    use super::*;

    fn row(offset: i64, key: Option<&[u8]>, headers: Vec<KafkaHeader>) -> KafkaRowMetadata {
        KafkaRowMetadata {
            key: key.map(<[u8]>::to_vec),
            topic: Arc::from("events"),
            partition: 3,
            offset,
            timestamp_ms: Some(1_000 + offset),
            timestamp_type: KafkaTimestampType::LogAppendTime,
            headers,
        }
    }

    #[test]
    fn parse_normalizes_order_and_rejects_unknown_names() {
        let columns = KafkaMetadataColumns::parse(" headers ,KEY,offset,key").unwrap();
        assert_eq!(columns.option_value(), "key,offset,headers");
        assert!(KafkaMetadataColumns::parse("partition,epoch").is_err());
        assert!(KafkaMetadataColumns::parse("").unwrap().is_empty());
    }

    #[test]
    fn collisions_are_case_insensitive() {
        let columns = KafkaMetadataColumns::parse("topic").unwrap();
        let payload = Schema::new(vec![Field::new("_TOPIC", DataType::Utf8, true)]);
        assert!(columns.validate_payload(&payload).is_err());
        let unselected = KafkaMetadataColumns::parse("offset").unwrap();
        unselected.validate_payload(&payload).unwrap();
    }

    #[test]
    fn arrays_preserve_null_and_empty_keys_and_binary_duplicate_headers() {
        let columns = KafkaMetadataColumns::parse("key,topic,timestamp_type,headers").unwrap();
        let rows = vec![
            row(
                10,
                None,
                vec![
                    KafkaHeader {
                        key: "h".into(),
                        value: Some(vec![0xff, 0x00]),
                    },
                    KafkaHeader {
                        key: "h".into(),
                        value: None,
                    },
                    KafkaHeader {
                        key: "e".into(),
                        value: Some(Vec::new()),
                    },
                ],
            ),
            row(11, Some(b""), Vec::new()),
        ];
        let payload: SchemaRef = Arc::new(Schema::empty());
        let batch = RecordBatch::try_new_with_options(
            columns.output_schema(&payload),
            columns.arrays(&rows).unwrap(),
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(2)),
        )
        .unwrap();

        let keys = batch.column(0).as_binary::<i32>();
        assert!(keys.is_null(0));
        assert_eq!(keys.value(1), b"");
        assert_eq!(
            batch.column(2).as_string::<i32>().value(0),
            "log_append_time"
        );

        let headers = batch.column(3).as_list::<i32>();
        let first = headers.value(0);
        let first = first.as_struct();
        let names = first.column(0).as_string::<i32>();
        let values = first.column(1).as_binary::<i32>();
        assert_eq!((names.value(0), values.value(0)), ("h", &[0xff, 0x00][..]));
        assert_eq!(names.value(1), "h");
        assert!(values.is_null(1));
        assert_eq!((names.value(2), values.value(2)), ("e", &b""[..]));
        assert_eq!(headers.value(1).len(), 0);
        assert!(!headers.is_null(1));
    }
}
