//! CSV serialization and deserialization.
//!
//! Converts between CSV records and Arrow `RecordBatch`.

use std::sync::Arc;

use arrow_array::builder::{Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};

use super::{Format, RecordDeserializer, RecordSerializer};
use crate::error::SerdeError;

/// CSV record deserializer.
///
/// Parses CSV lines and maps columns by position to the schema fields.
/// The first line is treated as data (not header) unless configured otherwise.
///
/// Supports: Int64, Float64, Utf8 field types.
#[derive(Debug, Clone)]
pub struct CsvDeserializer {
    /// Field delimiter character.
    delimiter: u8,
}

impl CsvDeserializer {
    /// Creates a new CSV deserializer with comma delimiter.
    #[must_use]
    pub fn new() -> Self {
        Self { delimiter: b',' }
    }

    /// Creates a CSV deserializer with a custom delimiter.
    #[must_use]
    pub fn with_delimiter(delimiter: u8) -> Self {
        Self { delimiter }
    }

    /// Splits a CSV line into fields, respecting quoted values.
    fn split_fields<'a>(&self, line: &'a str) -> Vec<&'a str> {
        let delim = self.delimiter as char;
        let mut fields = Vec::new();
        let mut start = 0;
        let mut in_quotes = false;

        for (i, ch) in line.char_indices() {
            if ch == '"' {
                in_quotes = !in_quotes;
            } else if ch == delim && !in_quotes {
                fields.push(line[start..i].trim().trim_matches('"'));
                start = i + 1;
            }
        }
        fields.push(line[start..].trim().trim_matches('"'));
        fields
    }
}

impl Default for CsvDeserializer {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordDeserializer for CsvDeserializer {
    fn deserialize(&self, data: &[u8], schema: &SchemaRef) -> Result<RecordBatch, SerdeError> {
        let line = std::str::from_utf8(data)
            .map_err(|e| SerdeError::Csv(format!("invalid UTF-8: {e}")))?;
        let line = line.trim();
        if line.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }

        let fields = self.split_fields(line);

        if fields.len() != schema.fields().len() {
            return Err(SerdeError::Csv(format!(
                "expected {} fields, got {}",
                schema.fields().len(),
                fields.len()
            )));
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        for (idx, field) in schema.fields().iter().enumerate() {
            let raw = fields[idx];
            let array = parse_csv_field(raw, field.data_type(), field.name(), field.is_nullable())?;
            columns.push(array);
        }

        RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| SerdeError::Csv(format!("failed to create RecordBatch: {e}")))
    }

    fn format(&self) -> Format {
        Format::Csv
    }
}

/// CSV record serializer.
///
/// Converts Arrow `RecordBatch` rows to CSV lines.
#[derive(Debug, Clone)]
pub struct CsvSerializer {
    /// Field delimiter character.
    delimiter: u8,
}

impl CsvSerializer {
    /// Creates a new CSV serializer with comma delimiter.
    #[must_use]
    pub fn new() -> Self {
        Self { delimiter: b',' }
    }

    /// Creates a CSV serializer with a custom delimiter.
    #[must_use]
    pub fn with_delimiter(delimiter: u8) -> Self {
        Self { delimiter }
    }
}

impl Default for CsvSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordSerializer for CsvSerializer {
    fn serialize(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SerdeError> {
        let delim = self.delimiter as char;
        let schema = batch.schema();
        let mut records = Vec::with_capacity(batch.num_rows());

        for row in 0..batch.num_rows() {
            let mut line = String::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                if col_idx > 0 {
                    line.push(delim);
                }
                let column = batch.column(col_idx);
                if column.is_null(row) {
                    // Empty field for null
                } else {
                    let s = arrow_column_to_csv_string(column, row, field.data_type())?;
                    // Quote strings that contain delimiter or quotes
                    if s.contains(delim) || s.contains('"') || s.contains('\n') {
                        line.push('"');
                        line.push_str(&s.replace('"', "\"\""));
                        line.push('"');
                    } else {
                        line.push_str(&s);
                    }
                }
            }
            records.push(line.into_bytes());
        }

        Ok(records)
    }

    fn serialize_batch(&self, batch: &RecordBatch) -> Result<Vec<u8>, SerdeError> {
        let records = self.serialize(batch)?;
        let total_len: usize = records.iter().map(|r| r.len() + 1).sum();
        let mut buf = Vec::with_capacity(total_len);
        for record in &records {
            buf.extend_from_slice(record);
            buf.push(b'\n');
        }
        Ok(buf)
    }

    fn format(&self) -> Format {
        Format::Csv
    }
}

/// Parses a CSV field value into a single-element Arrow array.
fn parse_csv_field(
    raw: &str,
    data_type: &DataType,
    field_name: &str,
    nullable: bool,
) -> Result<ArrayRef, SerdeError> {
    if raw.is_empty() || raw.eq_ignore_ascii_case("null") {
        if !nullable {
            return Err(SerdeError::MissingField(field_name.into()));
        }
        // Return a null array of the appropriate type
        return match data_type {
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(1);
                b.append_null();
                Ok(Arc::new(b.finish()))
            }
            DataType::Float64 => {
                let mut b = Float64Builder::with_capacity(1);
                b.append_null();
                Ok(Arc::new(b.finish()))
            }
            DataType::Utf8 => {
                let mut b = StringBuilder::with_capacity(1, 0);
                b.append_null();
                Ok(Arc::new(b.finish()))
            }
            _ => Err(SerdeError::UnsupportedFormat(format!(
                "unsupported type for CSV null: {data_type}"
            ))),
        };
    }

    match data_type {
        DataType::Int64 => {
            let v: i64 = raw.parse().map_err(|e| SerdeError::TypeConversion {
                field: field_name.into(),
                expected: "Int64".into(),
                message: format!("{e}"),
            })?;
            let mut b = Int64Builder::with_capacity(1);
            b.append_value(v);
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let v: f64 = raw.parse().map_err(|e| SerdeError::TypeConversion {
                field: field_name.into(),
                expected: "Float64".into(),
                message: format!("{e}"),
            })?;
            let mut b = Float64Builder::with_capacity(1);
            b.append_value(v);
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(1, raw.len());
            b.append_value(raw);
            Ok(Arc::new(b.finish()))
        }
        other => Err(SerdeError::UnsupportedFormat(format!(
            "unsupported type for CSV: {other}"
        ))),
    }
}

/// Converts an Arrow column value at `row` to a CSV string.
fn arrow_column_to_csv_string(
    column: &ArrayRef,
    row: usize,
    data_type: &DataType,
) -> Result<String, SerdeError> {
    use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray};

    match data_type {
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(arr.value(row).to_string())
        }
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(arr.value(row).to_string())
        }
        DataType::Utf8 => {
            let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(arr.value(row).to_string())
        }
        DataType::Boolean => {
            let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(arr.value(row).to_string())
        }
        other => Err(SerdeError::UnsupportedFormat(format!(
            "unsupported type for CSV serialization: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, true),
        ]))
    }

    #[test]
    fn test_csv_deserialize_basic() {
        let deser = CsvDeserializer::new();
        let schema = test_schema();
        let data = b"1,Alice,95.5";

        let batch = deser.deserialize(data, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);

        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");
    }

    #[test]
    fn test_csv_serialize_roundtrip() {
        let deser = CsvDeserializer::new();
        let ser = CsvSerializer::new();
        let schema = test_schema();

        let data = b"42,Charlie,88.5";
        let batch = deser.deserialize(data, &schema).unwrap();

        let serialized = ser.serialize(&batch).unwrap();
        assert_eq!(serialized.len(), 1);

        let line = std::str::from_utf8(&serialized[0]).unwrap();
        assert!(line.contains("42"));
        assert!(line.contains("Charlie"));
    }

    #[test]
    fn test_csv_null_handling() {
        let deser = CsvDeserializer::new();
        let schema = test_schema();
        let data = b"1,Bob,";

        let batch = deser.deserialize(data, &schema).unwrap();
        assert!(batch.column(2).is_null(0));
    }

    #[test]
    fn test_csv_wrong_field_count() {
        let deser = CsvDeserializer::new();
        let schema = test_schema();
        let data = b"1,Alice";

        let result = deser.deserialize(data, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_csv_quoted_fields() {
        let deser = CsvDeserializer::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("desc", DataType::Utf8, false),
        ]));
        let data = b"1,\"hello, world\"";

        let batch = deser.deserialize(data, &schema).unwrap();
        let descs = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(descs.value(0), "hello, world");
    }
}
