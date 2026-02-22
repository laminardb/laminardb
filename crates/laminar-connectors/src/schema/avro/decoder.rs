//! Avro format decoder implementing [`FormatDecoder`].
//!
//! Provides [`AvroFormatDecoder`] for decoding raw Avro and Confluent
//! wire-format payloads into Arrow `RecordBatch`es.

use std::collections::HashSet;

use arrow_array::RecordBatch;
use arrow_avro::reader::ReaderBuilder;
use arrow_avro::schema::{AvroSchema, Fingerprint, FingerprintAlgorithm, SchemaStore};
use arrow_schema::SchemaRef;

use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::traits::FormatDecoder;
use crate::schema::types::RawRecord;

/// Confluent wire format magic byte.
const CONFLUENT_MAGIC: u8 = 0x00;

/// Size of the Confluent wire format header (1 magic + 4 schema ID).
const CONFLUENT_HEADER_SIZE: usize = 5;

/// Decoding mode for the Avro decoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AvroDecoderMode {
    /// Raw Avro binary (no header).
    Raw,
    /// Confluent wire format: `0x00` + 4-byte BE schema ID + Avro payload.
    Confluent,
}

/// Avro format decoder implementing [`FormatDecoder`].
///
/// Supports both raw Avro binary and the Confluent wire format.
/// Uses `arrow-avro` for native Arrow-based decoding.
///
/// # Confluent Wire Format
///
/// When in [`AvroDecoderMode::Confluent`] mode, each record is expected to
/// start with `0x00` followed by a 4-byte big-endian schema ID. The decoder
/// maintains a [`SchemaStore`] cache; schemas must be registered before
/// decoding via [`register_schema`](Self::register_schema).
///
/// # Ring Integration
///
/// - Schema registration: Ring 2 (async fetch from registry)
/// - Decode: Ring 1 (synchronous, lock-free schema lookup)
pub struct AvroFormatDecoder {
    /// The Arrow output schema.
    output_schema: SchemaRef,
    /// Schema store for Avro schemas (keyed by Confluent schema ID).
    schema_store: SchemaStore,
    /// Set of schema IDs already registered.
    known_ids: HashSet<i32>,
    /// Decoding mode (raw or Confluent wire format).
    mode: AvroDecoderMode,
}

impl AvroFormatDecoder {
    /// Creates a new Avro decoder for raw Avro binary payloads.
    ///
    /// The `avro_schema_json` is the Avro JSON schema string used to
    /// interpret the binary payloads.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::DecodeError`] if the schema cannot be parsed.
    pub fn new_raw(output_schema: SchemaRef, avro_schema_json: &str) -> SchemaResult<Self> {
        let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
        let avro_schema = AvroSchema::new(avro_schema_json.to_string());
        // Register with ID 0 for raw mode.
        store
            .set(Fingerprint::Id(0), avro_schema)
            .map_err(|e| SchemaError::DecodeError(format!("failed to register schema: {e}")))?;
        let mut known = HashSet::new();
        known.insert(0);

        Ok(Self {
            output_schema,
            schema_store: store,
            known_ids: known,
            mode: AvroDecoderMode::Raw,
        })
    }

    /// Creates a new Avro decoder for Confluent wire-format payloads.
    ///
    /// Call [`register_schema`](Self::register_schema) to pre-populate the
    /// schema cache before decoding.
    #[must_use]
    pub fn new_confluent(output_schema: SchemaRef) -> Self {
        Self {
            output_schema,
            schema_store: SchemaStore::new_with_type(FingerprintAlgorithm::Id),
            known_ids: HashSet::new(),
            mode: AvroDecoderMode::Confluent,
        }
    }

    /// Registers an Avro schema by its Confluent schema ID.
    ///
    /// Must be called before decoding messages that carry this schema ID
    /// in their Confluent wire-format header.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::DecodeError`] if the schema cannot be registered.
    #[allow(clippy::cast_sign_loss)]
    pub fn register_schema(&mut self, schema_id: i32, avro_schema_json: &str) -> SchemaResult<()> {
        let avro_schema = AvroSchema::new(avro_schema_json.to_string());
        // Use Fingerprint::Id directly — NOT load_fingerprint_id which
        // applies from_be byte-swap meant for raw wire bytes.
        self.schema_store
            .set(Fingerprint::Id(schema_id as u32), avro_schema)
            .map_err(|e| SchemaError::DecodeError(format!("failed to register schema: {e}")))?;
        self.known_ids.insert(schema_id);
        Ok(())
    }

    /// Returns the decoding mode (raw or Confluent).
    #[must_use]
    pub fn mode(&self) -> AvroDecoderMode {
        self.mode
    }

    /// Returns the set of registered schema IDs.
    #[must_use]
    pub fn known_schema_ids(&self) -> &HashSet<i32> {
        &self.known_ids
    }

    /// Extracts the Confluent schema ID from a wire-format message.
    ///
    /// Returns `None` if the message is not in Confluent wire format.
    #[must_use]
    pub fn extract_schema_id(data: &[u8]) -> Option<i32> {
        if data.len() < CONFLUENT_HEADER_SIZE || data[0] != CONFLUENT_MAGIC {
            return None;
        }
        Some(i32::from_be_bytes([data[1], data[2], data[3], data[4]]))
    }

    /// Decodes raw binary slices (pre-stripped of any headers) using
    /// the arrow-avro decoder.
    fn decode_slices(&self, slices: &[&[u8]]) -> SchemaResult<RecordBatch> {
        let mut decoder = ReaderBuilder::new()
            .with_batch_size(slices.len())
            .with_writer_schema_store(self.schema_store.clone())
            .build_decoder()
            .map_err(|e| SchemaError::DecodeError(format!("failed to build decoder: {e}")))?;

        for slice in slices {
            let mut offset = 0;
            while offset < slice.len() {
                let consumed = decoder
                    .decode(&slice[offset..])
                    .map_err(|e| SchemaError::DecodeError(format!("Avro decode error: {e}")))?;
                if consumed == 0 {
                    break;
                }
                offset += consumed;
            }
        }

        decoder
            .flush()
            .map_err(|e| SchemaError::DecodeError(format!("Avro flush error: {e}")))?
            .ok_or_else(|| SchemaError::DecodeError("no records decoded".into()))
    }
}

impl std::fmt::Debug for AvroFormatDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroFormatDecoder")
            .field("mode", &self.mode)
            .field("known_ids", &self.known_ids)
            .finish_non_exhaustive()
    }
}

impl FormatDecoder for AvroFormatDecoder {
    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        }

        match self.mode {
            AvroDecoderMode::Raw => {
                // Raw mode: pass record values directly.
                let slices: Vec<&[u8]> = records.iter().map(|r| r.value.as_slice()).collect();
                self.decode_slices(&slices)
            }
            AvroDecoderMode::Confluent => {
                // Confluent mode: validate and strip 5-byte headers.
                // Records keep their Confluent header — arrow-avro handles it
                // natively when the schema store is configured with
                // FingerprintAlgorithm::Id.
                let slices: Vec<&[u8]> = records.iter().map(|r| r.value.as_slice()).collect();

                // Validate that all records have the Confluent magic byte
                // and that the schema IDs are registered.
                for (i, slice) in slices.iter().enumerate() {
                    if slice.len() < CONFLUENT_HEADER_SIZE {
                        return Err(SchemaError::DecodeError(format!(
                            "record {i} too short for Confluent wire format \
                             ({} bytes, need {CONFLUENT_HEADER_SIZE})",
                            slice.len()
                        )));
                    }
                    if slice[0] != CONFLUENT_MAGIC {
                        return Err(SchemaError::DecodeError(format!(
                            "record {i} missing Confluent magic byte \
                             (got 0x{:02x}, expected 0x{CONFLUENT_MAGIC:02x})",
                            slice[0]
                        )));
                    }
                    let sid = i32::from_be_bytes([slice[1], slice[2], slice[3], slice[4]]);
                    if !self.known_ids.contains(&sid) {
                        return Err(SchemaError::DecodeError(format!(
                            "unknown schema ID {sid} in record {i} — \
                             call register_schema() first"
                        )));
                    }
                }

                self.decode_slices(&slices)
            }
        }
    }

    fn format_name(&self) -> &str {
        match self.mode {
            AvroDecoderMode::Raw => "avro",
            AvroDecoderMode::Confluent => "confluent-avro",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    const TEST_SCHEMA_JSON: &str = r#"{
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
        ]
    }"#;

    fn test_arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    #[test]
    fn test_new_raw_decoder() {
        let dec = AvroFormatDecoder::new_raw(test_arrow_schema(), TEST_SCHEMA_JSON);
        assert!(dec.is_ok());
        let dec = dec.unwrap();
        assert_eq!(dec.mode(), AvroDecoderMode::Raw);
        assert_eq!(dec.format_name(), "avro");
    }

    #[test]
    fn test_new_confluent_decoder() {
        let dec = AvroFormatDecoder::new_confluent(test_arrow_schema());
        assert_eq!(dec.mode(), AvroDecoderMode::Confluent);
        assert_eq!(dec.format_name(), "confluent-avro");
        assert!(dec.known_schema_ids().is_empty());
    }

    #[test]
    fn test_register_schema() {
        let mut dec = AvroFormatDecoder::new_confluent(test_arrow_schema());
        assert!(dec.register_schema(42, TEST_SCHEMA_JSON).is_ok());
        assert!(dec.known_schema_ids().contains(&42));
    }

    #[test]
    fn test_extract_schema_id_valid() {
        let mut data = vec![0x00u8];
        data.extend_from_slice(&42i32.to_be_bytes());
        data.push(0xFF); // payload byte
        assert_eq!(AvroFormatDecoder::extract_schema_id(&data), Some(42));
    }

    #[test]
    fn test_extract_schema_id_too_short() {
        assert_eq!(AvroFormatDecoder::extract_schema_id(&[0x00, 0x00]), None);
    }

    #[test]
    fn test_extract_schema_id_wrong_magic() {
        let data = [0x01, 0x00, 0x00, 0x00, 0x01];
        assert_eq!(AvroFormatDecoder::extract_schema_id(&data), None);
    }

    #[test]
    fn test_decode_empty_batch() {
        let dec = AvroFormatDecoder::new_confluent(test_arrow_schema());
        let result = dec.decode_batch(&[]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().num_rows(), 0);
    }

    #[test]
    fn test_confluent_unknown_schema_id() {
        let dec = AvroFormatDecoder::new_confluent(test_arrow_schema());
        let mut data = vec![0x00u8];
        data.extend_from_slice(&99i32.to_be_bytes());
        data.extend_from_slice(&[0x00, 0x00]); // dummy payload
        let record = RawRecord::new(data);
        let result = dec.decode_batch(&[record]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown schema ID 99"));
    }

    #[test]
    fn test_confluent_missing_magic() {
        let dec = AvroFormatDecoder::new_confluent(test_arrow_schema());
        let record = RawRecord::new(vec![0x01, 0x00, 0x00, 0x00, 0x01, 0x00]);
        let result = dec.decode_batch(&[record]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("missing Confluent magic byte"));
    }

    #[test]
    fn test_confluent_too_short() {
        let dec = AvroFormatDecoder::new_confluent(test_arrow_schema());
        let record = RawRecord::new(vec![0x00, 0x00]);
        let result = dec.decode_batch(&[record]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("too short"));
    }

    #[test]
    fn test_debug_output() {
        let dec = AvroFormatDecoder::new_confluent(test_arrow_schema());
        let debug = format!("{dec:?}");
        assert!(debug.contains("AvroFormatDecoder"));
        assert!(debug.contains("Confluent"));
    }

    #[test]
    fn test_output_schema() {
        let schema = test_arrow_schema();
        let dec = AvroFormatDecoder::new_confluent(Arc::clone(&schema));
        assert_eq!(dec.output_schema(), schema);
    }

    #[test]
    fn test_extract_schema_id_boundary() {
        // Exactly CONFLUENT_HEADER_SIZE bytes
        let data = [0x00, 0x00, 0x00, 0x00, 0x05];
        assert_eq!(AvroFormatDecoder::extract_schema_id(&data), Some(5));

        // i32::MAX
        let mut data = vec![0x00];
        data.extend_from_slice(&i32::MAX.to_be_bytes());
        assert_eq!(AvroFormatDecoder::extract_schema_id(&data), Some(i32::MAX));
    }
}
