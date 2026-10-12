//! Kafka record-key encoding: typed key formats into one contiguous per-batch buffer.

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type, Int16Type, Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema};

use crate::error::ConnectorError;
use crate::kafka::sink_config::KeyFormat;
use crate::serde::RecordSerializer;

/// All key bytes of one batch in a single allocation with per-row ranges. A `None` range is a
/// NULL key (no Kafka key); `Some` with zero length is a present, empty key.
pub(super) struct KeyBuffer {
    data: Vec<u8>,
    ranges: Vec<Option<(usize, usize)>>,
}

impl KeyBuffer {
    fn with_capacity(rows: usize, average_len: usize) -> Self {
        Self {
            data: Vec::with_capacity(rows.saturating_mul(average_len)),
            ranges: Vec::with_capacity(rows),
        }
    }

    fn push(&mut self, key: &[u8]) {
        let start = self.data.len();
        self.data.extend_from_slice(key);
        self.ranges.push(Some((start, key.len())));
    }

    fn push_null(&mut self) {
        self.ranges.push(None);
    }

    pub(super) fn key(&self, row: usize) -> Option<&[u8]> {
        let (start, len) = self.ranges.get(row).copied().flatten()?;
        self.data.get(start..start + len)
    }

    pub(super) fn len(&self) -> usize {
        self.ranges.len()
    }
}

/// Resolved key columns and the format that turns them into Kafka key bytes.
pub(super) struct KeyEncoder {
    columns: Vec<usize>,
    format: KeyFormat,
}

impl KeyEncoder {
    /// Bind `names` (the configured `key.column` list) to `schema`, rejecting a format that
    /// cannot encode the column types before any record is produced.
    pub(super) fn bind(
        schema: &Schema,
        names: &[String],
        format: KeyFormat,
    ) -> Result<Self, ConnectorError> {
        let columns = names
            .iter()
            .map(|name| {
                schema.index_of(name).map_err(|_| {
                    ConnectorError::ConfigurationError(format!(
                        "key column '{name}' not found in the sink input schema"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if columns.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "key.column names no columns".into(),
            ));
        }
        if columns.len() > 1 && format != KeyFormat::Json {
            return Err(ConnectorError::ConfigurationError(
                "a multi-column key.column requires key.format = 'json'".into(),
            ));
        }
        for &column in &columns {
            let field = schema.field(column);
            if !format.encodes(field.data_type()) {
                return Err(ConnectorError::ConfigurationError(format!(
                    "key column '{}' of type {} cannot use key.format = '{format}'; {}",
                    field.name(),
                    field.data_type(),
                    format.remedy()
                )));
            }
        }
        Ok(Self { columns, format })
    }

    /// Encode every row's key. A NULL value is a NULL key for single-column formats; JSON always
    /// produces an object, so its keys are never NULL.
    pub(super) fn encode(
        &self,
        batch: &RecordBatch,
        json: &dyn RecordSerializer,
    ) -> Result<KeyBuffer, ConnectorError> {
        let rows = batch.num_rows();
        match (self.format, self.columns.as_slice()) {
            (KeyFormat::Json, columns) => {
                let projected = batch.project(columns).map_err(|error| {
                    ConnectorError::Internal(format!("project key columns: {error}"))
                })?;
                let encoded = json.serialize(&projected).map_err(ConnectorError::Serde)?;
                let mut keys = KeyBuffer::with_capacity(rows, 32);
                for key in &encoded {
                    keys.push(key);
                }
                Ok(keys)
            }
            (KeyFormat::Native | KeyFormat::Kafka, [column]) => {
                encode_binary(batch.column(*column), rows)
            }
            (KeyFormat::Text, [column]) => encode_text(batch.column(*column), rows),
            (format, columns) => Err(ConnectorError::Internal(format!(
                "key.format '{format}' bound to {} columns",
                columns.len()
            ))),
        }
    }
}

fn encode_text(array: &ArrayRef, rows: usize) -> Result<KeyBuffer, ConnectorError> {
    use std::fmt::Write as _;
    let formatter = arrow_cast::display::ArrayFormatter::try_new(
        array.as_ref(),
        &arrow_cast::display::FormatOptions::default(),
    )
    .map_err(|error| ConnectorError::Internal(format!("key text formatter: {error}")))?;
    let mut keys = KeyBuffer::with_capacity(rows, 16);
    let mut text = String::with_capacity(64);
    for row in 0..rows {
        if array.is_null(row) {
            keys.push_null();
            continue;
        }
        text.clear();
        write!(text, "{}", formatter.value(row))
            .map_err(|error| ConnectorError::Internal(format!("format key: {error}")))?;
        keys.push(text.as_bytes());
    }
    Ok(keys)
}

/// Bytes produced by Kafka's Java `StringSerializer`, `ByteArraySerializer` and the big-endian
/// `Short`/`Integer`/`Long`/`Float`/`Double` serializers.
fn encode_binary(array: &ArrayRef, rows: usize) -> Result<KeyBuffer, ConnectorError> {
    let mut keys = KeyBuffer::with_capacity(rows, 16);
    for row in 0..rows {
        if array.is_null(row) {
            keys.push_null();
            continue;
        }
        match array.data_type() {
            DataType::Utf8 => keys.push(array.as_string::<i32>().value(row).as_bytes()),
            DataType::LargeUtf8 => keys.push(array.as_string::<i64>().value(row).as_bytes()),
            DataType::Utf8View => keys.push(array.as_string_view().value(row).as_bytes()),
            DataType::Binary => keys.push(array.as_binary::<i32>().value(row)),
            DataType::LargeBinary => keys.push(array.as_binary::<i64>().value(row)),
            DataType::BinaryView => keys.push(array.as_binary_view().value(row)),
            DataType::FixedSizeBinary(_) => keys.push(array.as_fixed_size_binary().value(row)),
            DataType::Int16 => {
                keys.push(&array.as_primitive::<Int16Type>().value(row).to_be_bytes());
            }
            DataType::Int32 => {
                keys.push(&array.as_primitive::<Int32Type>().value(row).to_be_bytes());
            }
            DataType::Int64 => {
                keys.push(&array.as_primitive::<Int64Type>().value(row).to_be_bytes());
            }
            DataType::Float32 => {
                keys.push(&array.as_primitive::<Float32Type>().value(row).to_be_bytes());
            }
            DataType::Float64 => {
                keys.push(&array.as_primitive::<Float64Type>().value(row).to_be_bytes());
            }
            other => {
                return Err(ConnectorError::Internal(format!(
                    "key type {other} passed binding but has no binary encoding"
                )));
            }
        }
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{BinaryArray, Int32Array, Int64Array, StringArray};
    use arrow_schema::Field;

    use super::*;
    use crate::serde::json::JsonSerializer;

    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields = columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns.into_iter().map(|(_, array)| array).collect(),
        )
        .unwrap()
    }

    fn encode(batch: &RecordBatch, names: &[&str], format: KeyFormat) -> KeyBuffer {
        let names = names
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>();
        KeyEncoder::bind(&batch.schema(), &names, format)
            .unwrap()
            .encode(batch, &JsonSerializer::new())
            .unwrap()
    }

    #[test]
    fn native_keys_keep_null_distinct_from_empty() {
        let batch = batch(vec![(
            "k",
            Arc::new(StringArray::from(vec![Some("a"), Some(""), None])),
        )]);
        let keys = encode(&batch, &["k"], KeyFormat::Native);
        assert_eq!(keys.key(0), Some(&b"a"[..]));
        assert_eq!(keys.key(1), Some(&b""[..]));
        assert_eq!(keys.key(2), None);
    }

    #[test]
    fn native_binary_keys_are_raw_bytes() {
        let batch = batch(vec![(
            "k",
            Arc::new(BinaryArray::from(vec![Some(&[0_u8, 0xff, 7][..])])),
        )]);
        assert_eq!(
            encode(&batch, &["k"], KeyFormat::Native).key(0),
            Some(&[0, 0xff, 7][..])
        );
    }

    #[test]
    fn native_rejects_numeric_keys_without_an_explicit_format() {
        let batch = batch(vec![("k", Arc::new(Int64Array::from(vec![1_i64])))]);
        let error = KeyEncoder::bind(&batch.schema(), &["k".into()], KeyFormat::Native)
            .err()
            .unwrap();
        assert!(error.to_string().contains("key.format"), "{error}");
    }

    #[test]
    fn kafka_format_matches_java_serializers() {
        let batch = batch(vec![
            ("i", Arc::new(Int32Array::from(vec![258]))),
            ("l", Arc::new(Int64Array::from(vec![-2_i64]))),
        ]);
        assert_eq!(
            encode(&batch, &["i"], KeyFormat::Kafka).key(0),
            Some(&[0, 0, 1, 2][..])
        );
        assert_eq!(
            encode(&batch, &["l"], KeyFormat::Kafka).key(0),
            Some(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe][..])
        );
    }

    #[test]
    fn text_format_renders_sql_text() {
        let batch = batch(vec![(
            "k",
            Arc::new(Int64Array::from(vec![Some(42), None])),
        )]);
        let keys = encode(&batch, &["k"], KeyFormat::Text);
        assert_eq!(keys.key(0), Some(&b"42"[..]));
        assert_eq!(keys.key(1), None);
    }

    #[test]
    fn json_composite_keys_encode_nulls_explicitly() {
        let batch = batch(vec![
            ("a", Arc::new(Int64Array::from(vec![Some(1), None]))),
            ("b", Arc::new(StringArray::from(vec![Some("x"), Some("y")]))),
        ]);
        let keys = encode(&batch, &["a", "b"], KeyFormat::Json);
        assert_eq!(keys.key(0), Some(&br#"{"a":1,"b":"x"}"#[..]));
        assert_eq!(keys.key(1), Some(&br#"{"a":null,"b":"y"}"#[..]));
    }

    #[test]
    fn composite_keys_require_json() {
        let batch = batch(vec![
            ("a", Arc::new(Int64Array::from(vec![1]))),
            ("b", Arc::new(StringArray::from(vec!["x"]))),
        ]);
        let names = vec!["a".to_string(), "b".to_string()];
        assert!(KeyEncoder::bind(&batch.schema(), &names, KeyFormat::Text).is_err());
    }
}
