//! Exact JSON encoding shared by SQL responses, checkpoint status, and WS data frames.
//!
//! COMPAT: large integers and decimals are quoted and non-finite floats become
//! `"NaN"`/`"Infinity"` strings — every JSON consumer of these endpoints relies on
//! that exact value contract. Do not switch to arrow-json's default encoders.

use std::io::Write as _;
use std::sync::Arc;

const EXACT_DISPLAY_OPTIONS: arrow_cast::display::FormatOptions<'static> =
    arrow_cast::display::FormatOptions::new().with_display_error(true);

#[derive(Debug)]
struct ExactJsonEncoderFactory;

struct QuotedFormatterEncoder<'a> {
    formatter: arrow_cast::display::ArrayFormatter<'a>,
}

impl arrow_json::writer::Encoder for QuotedFormatterEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.push(b'"');
        write!(out, "{}", self.formatter.value(idx)).expect("writing to Vec cannot fail");
        out.push(b'"');
    }
}

fn encode_non_finite_float(value: f64, out: &mut Vec<u8>) -> bool {
    let value = if value.is_nan() {
        "\"NaN\""
    } else if value == f64::INFINITY {
        "\"Infinity\""
    } else if value == f64::NEG_INFINITY {
        "\"-Infinity\""
    } else {
        return false;
    };
    out.extend_from_slice(value.as_bytes());
    true
}

struct Float16JsonEncoder<'a>(&'a arrow_array::Float16Array);

impl arrow_json::writer::Encoder for Float16JsonEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let value = f32::from(self.0.value(idx));
        if !encode_non_finite_float(f64::from(value), out) {
            serde_json::to_writer(out, &value).expect("finite f32 is valid JSON");
        }
    }
}

struct Float32JsonEncoder<'a>(&'a arrow_array::Float32Array);

impl arrow_json::writer::Encoder for Float32JsonEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let value = self.0.value(idx);
        if !encode_non_finite_float(f64::from(value), out) {
            serde_json::to_writer(out, &value).expect("finite f32 is valid JSON");
        }
    }
}

struct Float64JsonEncoder<'a>(&'a arrow_array::Float64Array);

impl arrow_json::writer::Encoder for Float64JsonEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let value = self.0.value(idx);
        if !encode_non_finite_float(value, out) {
            serde_json::to_writer(out, &value).expect("finite f64 is valid JSON");
        }
    }
}

impl arrow_json::writer::EncoderFactory for ExactJsonEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a arrow_schema::FieldRef,
        array: &'a dyn arrow_array::Array,
        _options: &'a arrow_json::writer::EncoderOptions,
    ) -> Result<Option<arrow_json::writer::NullableEncoder<'a>>, arrow_schema::ArrowError> {
        use arrow_schema::DataType;

        let encoder: Option<Box<dyn arrow_json::writer::Encoder + 'a>> = match array.data_type() {
            // These types cannot be represented exactly by all JSON consumers.
            DataType::Int64
            | DataType::UInt64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => {
                let formatter =
                    arrow_cast::display::ArrayFormatter::try_new(array, &EXACT_DISPLAY_OPTIONS)?;
                Some(Box::new(QuotedFormatterEncoder { formatter }))
            }
            DataType::Float16 => Some(Box::new(Float16JsonEncoder(
                array
                    .as_any()
                    .downcast_ref::<arrow_array::Float16Array>()
                    .expect("Float16 data type must use Float16Array"),
            ))),
            DataType::Float32 => Some(Box::new(Float32JsonEncoder(
                array
                    .as_any()
                    .downcast_ref::<arrow_array::Float32Array>()
                    .expect("Float32 data type must use Float32Array"),
            ))),
            DataType::Float64 => Some(Box::new(Float64JsonEncoder(
                array
                    .as_any()
                    .downcast_ref::<arrow_array::Float64Array>()
                    .expect("Float64 data type must use Float64Array"),
            ))),
            _ => None,
        };

        Ok(encoder.map(|encoder| {
            arrow_json::writer::NullableEncoder::new(encoder, array.nulls().cloned())
        }))
    }
}

pub(super) fn exact_json_encoder_options() -> arrow_json::writer::EncoderOptions {
    arrow_json::writer::EncoderOptions::default()
        .with_explicit_nulls(true)
        .with_encoder_factory(Arc::new(ExactJsonEncoderFactory))
}

/// Serialize Arrow batches using the same exact JSON value contract as WS data frames.
pub(super) fn batches_to_json_string(
    batches: &[arrow_array::RecordBatch],
) -> Result<String, String> {
    let mut buf = Vec::new();
    let mut writer = arrow_json::writer::WriterBuilder::new()
        .with_explicit_nulls(true)
        .with_encoder_factory(Arc::new(ExactJsonEncoderFactory))
        .build::<_, arrow_json::writer::JsonArray>(&mut buf);
    for batch in batches {
        writer.write(batch).map_err(|e| e.to_string())?;
    }
    writer.finish().map_err(|e| e.to_string())?;
    String::from_utf8(buf).map_err(|e| e.to_string())
}

pub(super) fn batches_to_json_raw(
    batches: &[arrow_array::RecordBatch],
) -> Result<Box<serde_json::value::RawValue>, String> {
    let s = batches_to_json_string(batches)?;
    serde_json::value::RawValue::from_string(s).map_err(|e| e.to_string())
}
