//! Raw bytes pass-through serialization.
//!
//! Treats each record as one opaque value in a single column named `value`: `Utf8` (validated
//! UTF-8, the default) or `Binary`/`LargeBinary` (bytes preserved exactly).

use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, LargeBinaryBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::{Format, RecordDeserializer, RecordSerializer};
use crate::error::SerdeError;

/// The default schema for raw bytes: a single `Utf8` column named "value".
pub(crate) fn raw_schema() -> SchemaRef {
    raw_schema_of(DataType::Utf8)
}

fn raw_schema_of(data_type: DataType) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("value", data_type, false)]))
}

#[cfg(any(
    test,
    feature = "kafka",
    feature = "nats",
    feature = "rabbitmq",
    feature = "ibmmq",
    feature = "files"
))]
/// The raw protocol schema selected by a declared schema: a single `value` column of `Binary` or
/// `LargeBinary` keeps payload bytes exactly; anything else selects the UTF-8 default.
pub(crate) fn raw_protocol_for(declared: Option<&SchemaRef>) -> SchemaRef {
    match declared.map(|schema| schema.fields().as_ref()) {
        Some([field]) if is_binary(field.data_type()) => raw_schema_of(field.data_type().clone()),
        _ => raw_schema(),
    }
}

fn is_binary(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Binary | DataType::LargeBinary)
}

fn binary_value_type(schema: &SchemaRef) -> Option<&DataType> {
    match schema.fields().as_ref() {
        [field] if is_binary(field.data_type()) => Some(field.data_type()),
        _ => None,
    }
}

fn build_binary(records: &[&[u8]], data_type: &DataType) -> Result<ArrayRef, SerdeError> {
    let total = records
        .iter()
        .try_fold(0_usize, |total, record| total.checked_add(record.len()))
        .ok_or_else(|| SerdeError::MalformedInput("raw batch size overflows usize".into()))?;
    if *data_type == DataType::LargeBinary {
        let mut builder = LargeBinaryBuilder::with_capacity(records.len(), total);
        for record in records {
            builder.append_value(record);
        }
        return Ok(Arc::new(builder.finish()));
    }
    if i32::try_from(total).is_err() {
        return Err(SerdeError::MalformedInput(format!(
            "raw batch holds {total} bytes, beyond the Binary offset limit; declare LargeBinary"
        )));
    }
    let mut builder = BinaryBuilder::with_capacity(records.len(), total);
    for record in records {
        builder.append_value(record);
    }
    Ok(Arc::new(builder.finish()))
}

/// Raw bytes deserializer.
///
/// Stores each record as one `value`: exact bytes when the schema declares a binary `value`,
/// otherwise validated UTF-8 text.
#[derive(Debug, Clone)]
pub struct RawBytesDeserializer {
    _private: (),
}

impl RawBytesDeserializer {
    /// Creates a new raw bytes deserializer.
    #[must_use]
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Default for RawBytesDeserializer {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordDeserializer for RawBytesDeserializer {
    fn deserialize(&self, data: &[u8], schema: &SchemaRef) -> Result<RecordBatch, SerdeError> {
        if binary_value_type(schema).is_some() {
            return self.deserialize_batch(&[data], schema);
        }
        let s = std::str::from_utf8(data)
            .map_err(|e| SerdeError::MalformedInput(format!("invalid UTF-8: {e}")))?;
        let mut builder = StringBuilder::with_capacity(1, data.len());
        builder.append_value(s);

        let schema = raw_schema();
        RecordBatch::try_new(schema, vec![Arc::new(builder.finish())])
            .map_err(|e| SerdeError::MalformedInput(format!("failed to create batch: {e}")))
    }

    fn deserialize_batch(
        &self,
        records: &[&[u8]],
        schema: &SchemaRef,
    ) -> Result<RecordBatch, SerdeError> {
        if let Some(data_type) = binary_value_type(schema) {
            let column = build_binary(records, data_type)?;
            return RecordBatch::try_new(Arc::clone(schema), vec![column])
                .map_err(|e| SerdeError::MalformedInput(format!("failed to create batch: {e}")));
        }
        let total_bytes: usize = records.iter().map(|r| r.len()).sum();
        let mut builder = StringBuilder::with_capacity(records.len(), total_bytes);

        for data in records {
            let s = std::str::from_utf8(data)
                .map_err(|e| SerdeError::MalformedInput(format!("invalid UTF-8: {e}")))?;
            builder.append_value(s);
        }

        let schema = raw_schema();
        RecordBatch::try_new(schema, vec![Arc::new(builder.finish())])
            .map_err(|e| SerdeError::MalformedInput(format!("failed to create batch: {e}")))
    }

    fn format(&self) -> Format {
        Format::Raw
    }
}

/// Raw bytes serializer.
///
/// Emits the first column of each row: binary values verbatim, text as its UTF-8 bytes.
#[derive(Debug, Clone)]
pub struct RawBytesSerializer {
    _private: (),
}

impl RawBytesSerializer {
    /// Creates a new raw bytes serializer.
    #[must_use]
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Default for RawBytesSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordSerializer for RawBytesSerializer {
    fn serialize(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SerdeError> {
        if batch.num_columns() == 0 {
            return Ok(Vec::new());
        }

        let column = batch.column(0);
        match column.data_type() {
            DataType::Utf8 => Ok(owned(
                column
                    .as_string::<i32>()
                    .iter()
                    .map(|v| v.map(str::as_bytes)),
            )),
            DataType::Binary => Ok(owned(column.as_binary::<i32>().iter())),
            DataType::LargeBinary => Ok(owned(column.as_binary::<i64>().iter())),
            _ => Err(SerdeError::UnsupportedFormat(
                "raw serializer expects a Utf8, Binary, or LargeBinary column".into(),
            )),
        }
    }

    fn format(&self) -> Format {
        Format::Raw
    }
}

fn owned<'a>(values: impl Iterator<Item = Option<&'a [u8]>>) -> Vec<Vec<u8>> {
    values
        .map(|value| value.map_or_else(Vec::new, <[u8]>::to_vec))
        .collect()
}

#[cfg(test)]
mod tests;
