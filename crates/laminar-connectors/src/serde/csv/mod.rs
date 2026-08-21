//! CSV serialization and deserialization.
//!
//! Implements [`RecordDeserializer`] / [`RecordSerializer`] by delegating
//! to [`CsvDecoder`] and [`CsvEncoder`].

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use super::{Format, RecordDeserializer, RecordSerializer};
use crate::error::SerdeError;
use crate::schema::csv::{CsvDecoder, CsvDecoderConfig, CsvEncoder, CsvEncoderConfig};
use crate::schema::traits::{FormatDecoder, FormatEncoder};
use crate::schema::types::RawRecord;

/// CSV record deserializer. Delegates to [`CsvDecoder`].
#[derive(Debug, Clone)]
pub struct CsvDeserializer {
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
}

impl Default for CsvDeserializer {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordDeserializer for CsvDeserializer {
    fn deserialize(&self, data: &[u8], schema: &SchemaRef) -> Result<RecordBatch, SerdeError> {
        let config = CsvDecoderConfig {
            delimiter: self.delimiter,
            has_header: false,
            ..CsvDecoderConfig::default()
        };
        let decoder = CsvDecoder::with_config(schema.clone(), config);
        let record = RawRecord::new(data.to_vec());
        decoder
            .decode_one(&record)
            .map_err(|e| SerdeError::Csv(e.to_string()))
    }

    fn format(&self) -> Format {
        Format::Csv
    }
}

/// CSV record serializer. Delegates to [`CsvEncoder`].
#[derive(Debug, Clone)]
pub struct CsvSerializer {
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
        let config = CsvEncoderConfig {
            delimiter: self.delimiter,
            has_header: false,
        };
        let encoder = CsvEncoder::with_config(batch.schema(), config);
        encoder
            .encode_batch(batch)
            .map_err(|e| SerdeError::Csv(e.to_string()))
    }

    fn format(&self) -> Format {
        Format::Csv
    }
}

#[cfg(test)]
mod tests;
