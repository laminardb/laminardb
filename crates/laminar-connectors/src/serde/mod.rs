//! Record serialization and deserialization framework.
//!
//! Provides traits and implementations for converting between external
//! data formats and Arrow `RecordBatch`:
//!
//! - [`RecordDeserializer`]: Converts raw bytes to `RecordBatch`
//! - [`RecordSerializer`]: Converts `RecordBatch` to raw bytes
//! - [`Format`]: Enum of supported serialization formats
//!
//! ## Implementations
//!
//! - [`json`]: JSON format using `serde_json`
//! - [`csv`]: CSV format
//! - [`raw`]: Raw bytes pass-through
//! - [`debezium`]: Debezium CDC envelope format

pub mod csv;
pub mod debezium;
pub mod json;
pub mod raw;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::error::SerdeError;

/// Supported serialization formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Format {
    /// JSON format.
    Json,

    /// CSV format.
    Csv,

    /// Raw bytes (no deserialization).
    Raw,

    /// Debezium CDC envelope format.
    Debezium,

    /// Apache Avro format (with optional Confluent Schema Registry).
    Avro,
}

impl Format {
    /// Returns the format name as a string.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Format::Json => "json",
            Format::Csv => "csv",
            Format::Raw => "raw",
            Format::Debezium => "debezium",
            Format::Avro => "avro",
        }
    }

    /// Parses a format from a string.
    ///
    /// # Errors
    ///
    /// Returns `SerdeError::UnsupportedFormat` if the format is not recognized.
    pub fn parse(s: &str) -> Result<Self, SerdeError> {
        s.parse()
    }
}

impl std::str::FromStr for Format {
    type Err = SerdeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(Format::Json),
            "csv" => Ok(Format::Csv),
            "raw" | "bytes" => Ok(Format::Raw),
            "debezium" | "debezium-json" => Ok(Format::Debezium),
            "avro" | "confluent-avro" => Ok(Format::Avro),
            other => Err(SerdeError::UnsupportedFormat(other.to_string())),
        }
    }
}

impl std::fmt::Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Trait for deserializing raw bytes into Arrow `RecordBatch`.
///
/// Implementations convert from external formats (JSON, CSV, Avro, etc.)
/// into Arrow columnar format for processing.
pub trait RecordDeserializer: Send + Sync {
    /// Deserializes a single record from raw bytes.
    ///
    /// # Errors
    ///
    /// Returns `SerdeError` if the input cannot be parsed or doesn't
    /// match the expected schema.
    fn deserialize(&self, data: &[u8], schema: &SchemaRef) -> Result<RecordBatch, SerdeError>;

    /// Deserializes a batch of records from raw bytes.
    ///
    /// The default implementation calls `deserialize` for each record
    /// and concatenates the results. Implementations should override
    /// this for better performance with batch-oriented formats.
    ///
    /// # Errors
    ///
    /// Returns `SerdeError` if any record cannot be parsed.
    fn deserialize_batch(
        &self,
        records: &[&[u8]],
        schema: &SchemaRef,
    ) -> Result<RecordBatch, SerdeError> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }

        let batches: Result<Vec<RecordBatch>, SerdeError> = records
            .iter()
            .map(|data| self.deserialize(data, schema))
            .collect();
        let batches = batches?;

        arrow_select::concat::concat_batches(schema, &batches)
            .map_err(|e| SerdeError::MalformedInput(format!("failed to concat batches: {e}")))
    }

    /// Returns the format this deserializer handles.
    fn format(&self) -> Format;
}

/// Trait for serializing Arrow `RecordBatch` into raw bytes.
///
/// Implementations convert from Arrow columnar format to external
/// formats for writing to sinks.
pub trait RecordSerializer: Send + Sync {
    /// Serializes a `RecordBatch` into a vector of byte records.
    ///
    /// Each element in the returned vector represents one serialized record.
    ///
    /// # Errors
    ///
    /// Returns `SerdeError` if serialization fails.
    fn serialize(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SerdeError>;

    /// Serializes a `RecordBatch` into a single byte buffer.
    ///
    /// For formats that support batch encoding (e.g., JSON array, CSV),
    /// this may be more efficient than serializing individual records.
    ///
    /// # Errors
    ///
    /// Returns `SerdeError` if serialization fails.
    fn serialize_batch(&self, batch: &RecordBatch) -> Result<Vec<u8>, SerdeError> {
        let records = self.serialize(batch)?;
        let total_len: usize = records.iter().map(Vec::len).sum();
        let mut buf = Vec::with_capacity(total_len);
        for record in &records {
            buf.extend_from_slice(record);
        }
        Ok(buf)
    }

    /// Returns the format this serializer produces.
    fn format(&self) -> Format;
}

/// Creates a deserializer for the given format.
///
/// # Errors
///
/// Returns `SerdeError::UnsupportedFormat` if the format is not supported.
pub fn create_deserializer(format: Format) -> Result<Box<dyn RecordDeserializer>, SerdeError> {
    match format {
        Format::Json => Ok(Box::new(json::JsonDeserializer::new())),
        Format::Csv => Ok(Box::new(csv::CsvDeserializer::new())),
        Format::Raw => Ok(Box::new(raw::RawBytesDeserializer::new())),
        Format::Debezium => Ok(Box::new(debezium::DebeziumDeserializer::new())),
        Format::Avro => Err(SerdeError::UnsupportedFormat(
            "Avro deserialization requires the 'kafka' feature".into(),
        )),
    }
}

/// Creates a serializer for the given format.
///
/// # Errors
///
/// Returns `SerdeError::UnsupportedFormat` if the format is not supported.
pub fn create_serializer(format: Format) -> Result<Box<dyn RecordSerializer>, SerdeError> {
    match format {
        Format::Json => Ok(Box::new(json::JsonSerializer::new())),
        Format::Csv => Ok(Box::new(csv::CsvSerializer::new())),
        Format::Raw => Ok(Box::new(raw::RawBytesSerializer::new())),
        Format::Debezium => Err(SerdeError::UnsupportedFormat(
            "Debezium is a deserialization-only format".into(),
        )),
        Format::Avro => Err(SerdeError::UnsupportedFormat(
            "Avro serialization requires the 'kafka' feature".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_from_str() {
        assert_eq!(Format::parse("json").unwrap(), Format::Json);
        assert_eq!(Format::parse("JSON").unwrap(), Format::Json);
        assert_eq!(Format::parse("csv").unwrap(), Format::Csv);
        assert_eq!(Format::parse("raw").unwrap(), Format::Raw);
        assert_eq!(Format::parse("bytes").unwrap(), Format::Raw);
        assert_eq!(Format::parse("debezium").unwrap(), Format::Debezium);
        assert_eq!(Format::parse("debezium-json").unwrap(), Format::Debezium);
        assert_eq!(Format::parse("avro").unwrap(), Format::Avro);
        assert_eq!(Format::parse("confluent-avro").unwrap(), Format::Avro);
    }

    #[test]
    fn test_format_display() {
        assert_eq!(Format::Json.to_string(), "json");
        assert_eq!(Format::Csv.to_string(), "csv");
        assert_eq!(Format::Raw.to_string(), "raw");
        assert_eq!(Format::Debezium.to_string(), "debezium");
        assert_eq!(Format::Avro.to_string(), "avro");
    }

    #[test]
    fn test_create_deserializer() {
        assert!(create_deserializer(Format::Json).is_ok());
        assert!(create_deserializer(Format::Csv).is_ok());
        assert!(create_deserializer(Format::Raw).is_ok());
        assert!(create_deserializer(Format::Debezium).is_ok());
    }

    #[test]
    fn test_create_serializer() {
        assert!(create_serializer(Format::Json).is_ok());
        assert!(create_serializer(Format::Csv).is_ok());
        assert!(create_serializer(Format::Raw).is_ok());
        assert!(create_serializer(Format::Debezium).is_err()); // deser-only
    }
}
