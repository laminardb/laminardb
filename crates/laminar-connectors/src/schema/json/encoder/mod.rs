//! JSON format encoder implementing [`FormatEncoder`].

use std::io::Write as _;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use arrow_json::writer::{
    Encoder, EncoderFactory, EncoderOptions, LineDelimited, NullableEncoder, WriterBuilder,
};
use arrow_schema::{ArrowError, DataType, FieldRef, SchemaRef};

use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::traits::FormatEncoder;

/// Encodes Arrow `RecordBatch`es into one JSON object per row via
/// `arrow_json::writer`. `LargeBinary` columns are inlined as JSON
/// when valid (JSONB passthrough), matching `CollectExtra` semantics.
#[derive(Debug)]
pub struct JsonEncoder {
    schema: SchemaRef,
}

impl JsonEncoder {
    /// Creates a new JSON encoder for the given schema.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl FormatEncoder for JsonEncoder {
    fn input_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn encode_batch(&self, batch: &RecordBatch) -> SchemaResult<Vec<Vec<u8>>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }

        let mut buf = Vec::new();
        {
            let mut writer = WriterBuilder::new()
                .with_explicit_nulls(true)
                .with_encoder_factory(Arc::new(JsonbPassthroughFactory))
                .build::<_, LineDelimited>(&mut buf);
            writer
                .write(batch)
                .map_err(|e| SchemaError::DecodeError(format!("JSON encode error: {e}")))?;
            writer
                .finish()
                .map_err(|e| SchemaError::DecodeError(format!("JSON finish error: {e}")))?;
        }

        let output: Vec<Vec<u8>> = buf
            .split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
            .map(<[u8]>::to_vec)
            .collect();

        Ok(output)
    }

    fn format_name(&self) -> &'static str {
        "json"
    }
}

/// Inlines `LargeBinary` as raw JSON when valid; falls through otherwise.
#[derive(Debug)]
struct JsonbPassthroughFactory;

impl EncoderFactory for JsonbPassthroughFactory {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a FieldRef,
        array: &'a dyn Array,
        _options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        if *array.data_type() != DataType::LargeBinary {
            return Ok(None);
        }
        let binary_array = array.as_binary::<i64>();
        let nulls = binary_array.nulls().cloned();
        let encoder = LargeBinaryJsonbEncoder {
            array: binary_array,
        };
        Ok(Some(NullableEncoder::new(Box::new(encoder), nulls)))
    }
}

struct LargeBinaryJsonbEncoder<'a> {
    array: &'a arrow_array::LargeBinaryArray,
}

impl Encoder for LargeBinaryJsonbEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let bytes = self.array.value(idx);
        if serde_json::from_slice::<serde_json::Value>(bytes).is_ok() {
            out.extend_from_slice(bytes);
        } else {
            write!(out, "\"<binary:{} bytes>\"", bytes.len()).unwrap();
        }
    }
}

#[cfg(test)]
mod tests;
