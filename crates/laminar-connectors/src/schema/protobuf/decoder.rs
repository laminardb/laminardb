//! Protobuf format decoder implementing [`FormatDecoder`].
//!
//! Decodes binary protobuf messages into Arrow `RecordBatch`es using a
//! pre-compiled `FileDescriptorSet` (produced by `protoc --descriptor_set_out`).
//!
//! # Wire-format decoding
//!
//! Since `prost` requires code-generated structs for typed decoding, this
//! decoder operates at the wire-format level: it parses protobuf field tags
//! and values directly, mapping them to Arrow columns via the descriptor
//! metadata. This enables schema-driven decoding without `build.rs`.
//!
//! # Known Limitation: Repeated Fields
//!
//! Protobuf `repeated` fields (arrays) are not fully supported. When a
//! repeated field is encountered during decoding, the decoder returns an
//! explicit `SchemaError::DecodeError` with the message "repeated fields
//! are not yet supported". In the schema, repeated fields are mapped to
//! `DataType::Utf8` (intended for future JSON-encoded array representation)
//! but the decoding path rejects them at runtime. If your proto schema
//! contains repeated fields, either remove them from the descriptor or
//! pre-process messages to flatten arrays before decoding.

use std::fmt::Write;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use prost::Message;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, FileDescriptorSet,
};

use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::traits::FormatDecoder;
use crate::schema::types::RawRecord;

/// Configuration for the Protobuf decoder.
#[derive(Debug, Clone, Default)]
pub struct ProtobufDecoderConfig {
    /// Fully-qualified message type name (e.g., `"mypackage.MyMessage"`).
    /// If `None`, uses the first message in the first file.
    pub message_type: Option<String>,
}

impl ProtobufDecoderConfig {
    /// Sets the target message type.
    #[must_use]
    pub fn with_message_type(mut self, name: impl Into<String>) -> Self {
        self.message_type = Some(name.into());
        self
    }
}

/// Resolved field information for decoding.
#[derive(Debug, Clone)]
struct ResolvedField {
    /// Protobuf field number (tag).
    field_number: u32,
    /// Arrow column index.
    column_index: usize,
    /// Protobuf wire type.
    proto_type: Type,
    /// Whether the field is repeated (reserved for future array support).
    is_repeated: bool,
}

/// Decodes binary protobuf messages into Arrow `RecordBatch`es.
///
/// Constructed from a [`FileDescriptorSet`] (`.desc` file produced by
/// `protoc --descriptor_set_out`). The decoder parses protobuf wire
/// format directly, without code-generated structs.
pub struct ProtobufDecoder {
    /// Output Arrow schema.
    schema: SchemaRef,
    /// Map from protobuf field number to resolved field info.
    fields: Vec<ResolvedField>,
    /// Decoder configuration.
    #[allow(dead_code)]
    config: ProtobufDecoderConfig,
}

impl std::fmt::Debug for ProtobufDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtobufDecoder")
            .field("schema", &self.schema)
            .field("num_fields", &self.fields.len())
            .field("config", &self.config)
            .finish()
    }
}

impl ProtobufDecoder {
    /// Creates a new Protobuf decoder from raw `FileDescriptorSet` bytes.
    ///
    /// The descriptor bytes are typically produced by:
    /// ```sh
    /// protoc --descriptor_set_out=schema.desc --include_imports schema.proto
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the descriptor cannot be parsed or the target
    /// message type is not found.
    pub fn from_descriptor_bytes(
        descriptor_bytes: &[u8],
        config: ProtobufDecoderConfig,
    ) -> SchemaResult<Self> {
        let fds = FileDescriptorSet::decode(descriptor_bytes).map_err(|e| {
            SchemaError::DecodeError(format!("cannot parse FileDescriptorSet: {e}"))
        })?;

        Self::from_descriptor_set(&fds, config)
    }

    /// Creates a new Protobuf decoder from a parsed `FileDescriptorSet`.
    ///
    /// # Errors
    ///
    /// Returns an error if the target message type is not found.
    pub fn from_descriptor_set(
        fds: &FileDescriptorSet,
        config: ProtobufDecoderConfig,
    ) -> SchemaResult<Self> {
        let (package, descriptor) = find_message_descriptor(fds, config.message_type.as_deref())?;

        let (schema, fields) = build_schema_and_fields(&package, &descriptor);

        Ok(Self {
            schema: Arc::new(schema),
            fields,
            config,
        })
    }

    /// Creates a decoder from an explicit Arrow schema and field mappings.
    ///
    /// Use this when you already know the schema and want to skip descriptor
    /// parsing (e.g., schema came from a registry).
    #[must_use]
    pub fn with_schema(
        schema: SchemaRef,
        field_mappings: Vec<(u32, Type)>,
        config: ProtobufDecoderConfig,
    ) -> Self {
        let fields = field_mappings
            .into_iter()
            .enumerate()
            .map(|(idx, (field_number, proto_type))| ResolvedField {
                field_number,
                column_index: idx,
                proto_type,
                is_repeated: false,
            })
            .collect();

        Self {
            schema,
            fields,
            config,
        }
    }
}

impl FormatDecoder for ProtobufDecoder {
    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let num_rows = records.len();
        let num_cols = self.schema.fields().len();

        // Column builders: one Vec<Option<FieldValue>> per column.
        let mut columns: Vec<Vec<Option<FieldValue>>> = (0..num_cols)
            .map(|_| Vec::with_capacity(num_rows))
            .collect();

        // Initialize all rows to None.
        for col in &mut columns {
            col.resize(num_rows, None);
        }

        // Decode each record.
        for (row_idx, record) in records.iter().enumerate() {
            let parsed = parse_wire_fields(&record.value)?;

            for (field_number, wire_value) in &parsed {
                if let Some(resolved) = self.fields.iter().find(|f| f.field_number == *field_number)
                {
                    if resolved.is_repeated && columns[resolved.column_index][row_idx].is_some() {
                        return Err(SchemaError::DecodeError(
                            "protobuf: repeated fields are not yet supported".into(),
                        ));
                    }
                    let typed = coerce_wire_value(wire_value, resolved.proto_type)?;
                    columns[resolved.column_index][row_idx] = Some(typed);
                }
            }
        }

        // Build Arrow arrays from column data.
        let arrays: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(col_idx, field)| build_arrow_array(field.data_type(), &columns[col_idx]))
            .collect::<SchemaResult<Vec<_>>>()?;

        RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| SchemaError::DecodeError(format!("RecordBatch construction error: {e}")))
    }

    fn format_name(&self) -> &'static str {
        "protobuf"
    }
}

// ── Schema construction from descriptors ─────────────────────────────

/// Finds the target message descriptor in the `FileDescriptorSet`.
fn find_message_descriptor(
    fds: &FileDescriptorSet,
    message_type: Option<&str>,
) -> SchemaResult<(String, DescriptorProto)> {
    for file in &fds.file {
        let package = file.package.clone().unwrap_or_default();

        for msg in &file.message_type {
            let fq_name = if package.is_empty() {
                msg.name.clone().unwrap_or_default()
            } else {
                format!("{}.{}", package, msg.name.as_deref().unwrap_or(""))
            };

            match message_type {
                Some(target) if target == fq_name || Some(target) == msg.name.as_deref() => {
                    return Ok((package, msg.clone()));
                }
                None => {
                    return Ok((package, msg.clone()));
                }
                Some(_) => {}
            }
        }
    }

    match message_type {
        Some(name) => Err(SchemaError::DecodeError(format!(
            "message type '{name}' not found in descriptor"
        ))),
        None => Err(SchemaError::DecodeError(
            "no message types found in descriptor".into(),
        )),
    }
}

/// Builds an Arrow schema and field mappings from a protobuf message descriptor.
fn build_schema_and_fields(
    _package: &str,
    descriptor: &DescriptorProto,
) -> (Schema, Vec<ResolvedField>) {
    let mut arrow_fields = Vec::new();
    let mut resolved_fields = Vec::new();

    for proto_field in &descriptor.field {
        let Some(number) = proto_field.number else {
            // Skip fields without valid field numbers (field 0 is reserved in protobuf)
            continue;
        };
        if number <= 0 {
            continue;
        }
        let name = proto_field.name.clone().unwrap_or_default();
        #[allow(clippy::cast_sign_loss)]
        let field_number = number as u32;
        let proto_type = proto_field.r#type();
        let is_repeated = proto_field.label() == Label::Repeated;
        let is_optional =
            proto_field.label() == Label::Optional || proto_field.proto3_optional.unwrap_or(false);

        let arrow_type = if is_repeated {
            // Repeated fields become JSON-encoded strings for simplicity.
            DataType::Utf8
        } else {
            proto_type_to_arrow(proto_type)
        };

        let nullable = is_optional || is_repeated;
        arrow_fields.push(Field::new(&name, arrow_type, nullable));

        resolved_fields.push(ResolvedField {
            field_number,
            column_index: arrow_fields.len() - 1,
            proto_type,
            is_repeated,
        });
    }

    (Schema::new(arrow_fields), resolved_fields)
}

/// Maps a protobuf field type to an Arrow data type.
fn proto_type_to_arrow(proto_type: Type) -> DataType {
    match proto_type {
        Type::Double => DataType::Float64,
        Type::Float => DataType::Float32,
        Type::Int64 | Type::Sint64 | Type::Sfixed64 => DataType::Int64,
        Type::Uint64 | Type::Fixed64 => DataType::UInt64,
        Type::Int32 | Type::Sint32 | Type::Sfixed32 | Type::Enum => DataType::Int32,
        Type::Uint32 | Type::Fixed32 => DataType::UInt32,
        Type::Bool => DataType::Boolean,
        Type::Bytes => DataType::Binary,
        // Strings and nested messages are both stored as Utf8.
        Type::String | Type::Message | Type::Group => DataType::Utf8,
    }
}

// ── Wire-format parsing ──────────────────────────────────────────────

/// Raw wire-format value.
#[derive(Debug, Clone)]
enum WireValue {
    Varint(u64),
    Fixed64(u64),
    Fixed32(u32),
    LengthDelimited(Vec<u8>),
}

/// Decoded and typed field value.
#[derive(Debug, Clone)]
enum FieldValue {
    Bool(bool),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}

/// Parses protobuf wire-format bytes into a list of `(field_number, wire_value)`.
fn parse_wire_fields(data: &[u8]) -> SchemaResult<Vec<(u32, WireValue)>> {
    let mut fields = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        let (tag, new_pos) = decode_varint(data, pos)?;
        pos = new_pos;

        #[allow(clippy::cast_possible_truncation)]
        let field_number = (tag >> 3) as u32;
        let wire_type = tag & 0x07;

        let (value, new_pos) = match wire_type {
            0 => {
                // Varint
                let (v, p) = decode_varint(data, pos)?;
                (WireValue::Varint(v), p)
            }
            1 => {
                // 64-bit
                if pos + 8 > data.len() {
                    return Err(SchemaError::DecodeError(
                        "protobuf: unexpected end of data for fixed64".into(),
                    ));
                }
                let v = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                (WireValue::Fixed64(v), pos + 8)
            }
            2 => {
                // Length-delimited
                let (len, p) = decode_varint(data, pos)?;
                #[allow(clippy::cast_possible_truncation)]
                let len = len as usize;
                if p + len > data.len() {
                    return Err(SchemaError::DecodeError(
                        "protobuf: unexpected end of data for length-delimited field".into(),
                    ));
                }
                let bytes = data[p..p + len].to_vec();
                (WireValue::LengthDelimited(bytes), p + len)
            }
            5 => {
                // 32-bit
                if pos + 4 > data.len() {
                    return Err(SchemaError::DecodeError(
                        "protobuf: unexpected end of data for fixed32".into(),
                    ));
                }
                let v = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                (WireValue::Fixed32(v), pos + 4)
            }
            3 | 4 => {
                return Err(SchemaError::DecodeError(
                    "protobuf: group wire types are not supported".into(),
                ));
            }
            other => {
                return Err(SchemaError::DecodeError(format!(
                    "protobuf: unknown wire type {other}"
                )));
            }
        };

        fields.push((field_number, value));
        pos = new_pos;
    }

    Ok(fields)
}

/// Decodes a varint from the buffer at the given position.
fn decode_varint(data: &[u8], mut pos: usize) -> SchemaResult<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;

    loop {
        if pos >= data.len() {
            return Err(SchemaError::DecodeError(
                "protobuf: unexpected end of data in varint".into(),
            ));
        }

        let byte = data[pos];
        pos += 1;

        result |= u64::from(byte & 0x7F) << shift;

        if byte & 0x80 == 0 {
            return Ok((result, pos));
        }

        shift += 7;
        if shift >= 64 {
            return Err(SchemaError::DecodeError("protobuf: varint too long".into()));
        }
    }
}

/// Coerces a raw wire value to a typed `FieldValue` based on the protobuf type.
#[allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::match_same_arms
)]
fn coerce_wire_value(wire: &WireValue, proto_type: Type) -> SchemaResult<FieldValue> {
    match (proto_type, wire) {
        // Varint types
        (Type::Bool, WireValue::Varint(v)) => Ok(FieldValue::Bool(*v != 0)),
        (Type::Int32 | Type::Enum, WireValue::Varint(v)) => Ok(FieldValue::I32(*v as i32)),
        (Type::Sint32, WireValue::Varint(v)) => {
            // ZigZag decode
            let n = *v as u32;
            Ok(FieldValue::I32(((n >> 1) as i32) ^ -((n & 1) as i32)))
        }
        (Type::Uint32, WireValue::Varint(v)) => Ok(FieldValue::U32(*v as u32)),
        (Type::Int64, WireValue::Varint(v)) => Ok(FieldValue::I64(*v as i64)),
        (Type::Sint64, WireValue::Varint(v)) => {
            // ZigZag decode
            Ok(FieldValue::I64(((*v >> 1) as i64) ^ -((*v & 1) as i64)))
        }
        (Type::Uint64, WireValue::Varint(v)) => Ok(FieldValue::U64(*v)),

        // Fixed types
        (Type::Fixed64, WireValue::Fixed64(v)) => Ok(FieldValue::U64(*v)),
        (Type::Sfixed64, WireValue::Fixed64(v)) => Ok(FieldValue::I64(*v as i64)),
        (Type::Double, WireValue::Fixed64(v)) => Ok(FieldValue::F64(f64::from_bits(*v))),
        (Type::Fixed32, WireValue::Fixed32(v)) => Ok(FieldValue::U32(*v)),
        (Type::Sfixed32, WireValue::Fixed32(v)) => Ok(FieldValue::I32(*v as i32)),
        (Type::Float, WireValue::Fixed32(v)) => Ok(FieldValue::F32(f32::from_bits(*v))),

        // Length-delimited types
        (Type::String, WireValue::LengthDelimited(bytes)) => {
            let s = String::from_utf8(bytes.clone()).map_err(|e| {
                SchemaError::DecodeError(format!("protobuf: invalid UTF-8 in string field: {e}"))
            })?;
            Ok(FieldValue::String(s))
        }
        (Type::Bytes, WireValue::LengthDelimited(bytes)) => Ok(FieldValue::Bytes(bytes.clone())),
        (Type::Message | Type::Group, WireValue::LengthDelimited(bytes)) => {
            // Nested messages: encode as hex string.
            let hex = bytes.iter().fold(std::string::String::new(), |mut acc, b| {
                let _ = write!(acc, "{b:02x}");
                acc
            });
            Ok(FieldValue::String(hex))
        }

        (ty, wire) => Err(SchemaError::DecodeError(format!(
            "protobuf: type mismatch - expected {ty:?} but got wire value {wire:?}"
        ))),
    }
}

// ── Arrow array construction ─────────────────────────────────────────

/// Builds an Arrow array from a column of decoded field values.
#[allow(clippy::too_many_lines)]
fn build_arrow_array(
    data_type: &DataType,
    values: &[Option<FieldValue>],
) -> SchemaResult<ArrayRef> {
    match data_type {
        DataType::Boolean => {
            let arr: BooleanArray = values
                .iter()
                .map(|v| v.as_ref().map(|fv| matches!(fv, FieldValue::Bool(true))))
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Int32 => {
            let arr: Int32Array = values
                .iter()
                .map(|v| {
                    v.as_ref().map(|fv| match fv {
                        FieldValue::I32(n) => *n,
                        _ => 0,
                    })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Int64 => {
            let arr: Int64Array = values
                .iter()
                .map(|v| {
                    v.as_ref().map(|fv| match fv {
                        FieldValue::I64(n) => *n,
                        _ => 0,
                    })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::UInt32 => {
            let arr: UInt32Array = values
                .iter()
                .map(|v| {
                    v.as_ref().map(|fv| match fv {
                        FieldValue::U32(n) => *n,
                        _ => 0,
                    })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::UInt64 => {
            let arr: UInt64Array = values
                .iter()
                .map(|v| {
                    v.as_ref().map(|fv| match fv {
                        FieldValue::U64(n) => *n,
                        _ => 0,
                    })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Float32 => {
            let arr: Float32Array = values
                .iter()
                .map(|v| {
                    v.as_ref().map(|fv| match fv {
                        FieldValue::F32(n) => *n,
                        _ => 0.0,
                    })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Float64 => {
            let arr: Float64Array = values
                .iter()
                .map(|v| {
                    v.as_ref().map(|fv| match fv {
                        FieldValue::F64(n) => *n,
                        _ => 0.0,
                    })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Utf8 => {
            let arr: StringArray = values
                .iter()
                .map(|v| {
                    v.as_ref().map(|fv| match fv {
                        FieldValue::String(s) => s.as_str(),
                        _ => "",
                    })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Binary => {
            let arr: arrow_array::BinaryArray = values
                .iter()
                .map(|v| {
                    v.as_ref().map(|fv| match fv {
                        FieldValue::Bytes(b) => b.as_slice(),
                        _ => &[] as &[u8],
                    })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        other => Err(SchemaError::DecodeError(format!(
            "protobuf: unsupported Arrow type {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    };

    /// Build a test `FileDescriptorSet` for a simple message:
    /// ```proto
    /// message TestEvent {
    ///   int64 id = 1;
    ///   string name = 2;
    ///   bool active = 3;
    ///   double score = 4;
    /// }
    /// ```
    fn make_test_descriptor() -> FileDescriptorSet {
        FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("test.proto".into()),
                package: Some("test".into()),
                message_type: vec![DescriptorProto {
                    name: Some("TestEvent".into()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("id".into()),
                            number: Some(1),
                            r#type: Some(Type::Int64 as i32),
                            label: Some(Label::Optional as i32),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("name".into()),
                            number: Some(2),
                            r#type: Some(Type::String as i32),
                            label: Some(Label::Optional as i32),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("active".into()),
                            number: Some(3),
                            r#type: Some(Type::Bool as i32),
                            label: Some(Label::Optional as i32),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("score".into()),
                            number: Some(4),
                            r#type: Some(Type::Double as i32),
                            label: Some(Label::Optional as i32),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn encode_test_message(id: i64, name: &str, active: bool, score: f64) -> Vec<u8> {
        let mut buf = Vec::new();

        // Field 1 (id): varint
        encode_varint_field(&mut buf, 1, id as u64);

        // Field 2 (name): length-delimited
        encode_bytes_field(&mut buf, 2, name.as_bytes());

        // Field 3 (active): varint
        encode_varint_field(&mut buf, 3, u64::from(active));

        // Field 4 (score): fixed64 (double)
        let tag = (4 << 3) | 1; // wire type 1 = 64-bit
        encode_raw_varint(&mut buf, tag);
        buf.extend_from_slice(&score.to_le_bytes());

        buf
    }

    fn encode_varint_field(buf: &mut Vec<u8>, field_number: u32, value: u64) {
        let tag = (u64::from(field_number) << 3) | 0; // wire type 0 = varint
        encode_raw_varint(buf, tag);
        encode_raw_varint(buf, value);
    }

    fn encode_bytes_field(buf: &mut Vec<u8>, field_number: u32, data: &[u8]) {
        let tag = (u64::from(field_number) << 3) | 2; // wire type 2 = length-delimited
        encode_raw_varint(buf, tag);
        encode_raw_varint(buf, data.len() as u64);
        buf.extend_from_slice(data);
    }

    fn encode_raw_varint(buf: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            buf.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    #[test]
    fn test_schema_from_descriptor() {
        let fds = make_test_descriptor();
        let config = ProtobufDecoderConfig::default().with_message_type("test.TestEvent");
        let decoder = ProtobufDecoder::from_descriptor_set(&fds, config).unwrap();

        let schema = decoder.output_schema();
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).name(), "active");
        assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(3).name(), "score");
        assert_eq!(schema.field(3).data_type(), &DataType::Float64);
    }

    #[test]
    fn test_decode_single_message() {
        let fds = make_test_descriptor();
        let config = ProtobufDecoderConfig::default();
        let decoder = ProtobufDecoder::from_descriptor_set(&fds, config).unwrap();

        let msg = encode_test_message(42, "alice", true, 9.5);
        let record = RawRecord::new(msg);
        let batch = decoder.decode_batch(&[record]).unwrap();

        assert_eq!(batch.num_rows(), 1);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 42);

        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");

        let actives = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(actives.value(0));

        let scores = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((scores.value(0) - 9.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_decode_multiple_messages() {
        let fds = make_test_descriptor();
        let config = ProtobufDecoderConfig::default();
        let decoder = ProtobufDecoder::from_descriptor_set(&fds, config).unwrap();

        let records: Vec<RawRecord> = vec![
            RawRecord::new(encode_test_message(1, "bob", false, 1.0)),
            RawRecord::new(encode_test_message(2, "carol", true, 2.5)),
            RawRecord::new(encode_test_message(3, "dave", true, 3.7)),
        ];

        let batch = decoder.decode_batch(&records).unwrap();
        assert_eq!(batch.num_rows(), 3);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 3);
    }

    #[test]
    fn test_decode_empty_batch() {
        let fds = make_test_descriptor();
        let config = ProtobufDecoderConfig::default();
        let decoder = ProtobufDecoder::from_descriptor_set(&fds, config).unwrap();

        let batch = decoder.decode_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 4);
    }

    #[test]
    fn test_decode_missing_fields_are_null() {
        let fds = make_test_descriptor();
        let config = ProtobufDecoderConfig::default();
        let decoder = ProtobufDecoder::from_descriptor_set(&fds, config).unwrap();

        // Encode only field 1 (id) -- other fields are missing.
        let mut msg = Vec::new();
        encode_varint_field(&mut msg, 1, 99);
        let record = RawRecord::new(msg);

        let batch = decoder.decode_batch(&[record]).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 99);

        // name should be null
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(names.is_null(0));
    }

    #[test]
    fn test_format_name() {
        let fds = make_test_descriptor();
        let config = ProtobufDecoderConfig::default();
        let decoder = ProtobufDecoder::from_descriptor_set(&fds, config).unwrap();
        assert_eq!(decoder.format_name(), "protobuf");
    }

    #[test]
    fn test_message_not_found() {
        let fds = make_test_descriptor();
        let config = ProtobufDecoderConfig::default().with_message_type("nonexistent.Message");
        let result = ProtobufDecoder::from_descriptor_set(&fds, config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_invalid_protobuf_bytes() {
        let fds = make_test_descriptor();
        let config = ProtobufDecoderConfig::default();
        let decoder = ProtobufDecoder::from_descriptor_set(&fds, config).unwrap();

        // Truncated varint
        let record = RawRecord::new(vec![0x80, 0x80]);
        let result = decoder.decode_batch(&[record]);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_descriptor_bytes() {
        let fds = make_test_descriptor();
        let bytes = fds.encode_to_vec();

        let config = ProtobufDecoderConfig::default();
        let decoder = ProtobufDecoder::from_descriptor_bytes(&bytes, config).unwrap();
        assert_eq!(decoder.output_schema().fields().len(), 4);
    }

    #[test]
    fn test_decode_varint_zigzag_sint32() {
        // ZigZag encoding: 0->0, -1->1, 1->2, -2->3, 2->4, ...
        let wire = WireValue::Varint(1);
        let fv = coerce_wire_value(&wire, Type::Sint32).unwrap();
        assert!(matches!(fv, FieldValue::I32(-1)));

        let wire = WireValue::Varint(4);
        let fv = coerce_wire_value(&wire, Type::Sint32).unwrap();
        assert!(matches!(fv, FieldValue::I32(2)));
    }

    #[test]
    fn test_decode_varint_zigzag_sint64() {
        let wire = WireValue::Varint(1);
        let fv = coerce_wire_value(&wire, Type::Sint64).unwrap();
        assert!(matches!(fv, FieldValue::I64(-1)));
    }

    #[test]
    fn test_proto_type_to_arrow_all_types() {
        assert_eq!(proto_type_to_arrow(Type::Double), DataType::Float64);
        assert_eq!(proto_type_to_arrow(Type::Float), DataType::Float32);
        assert_eq!(proto_type_to_arrow(Type::Int64), DataType::Int64);
        assert_eq!(proto_type_to_arrow(Type::Uint64), DataType::UInt64);
        assert_eq!(proto_type_to_arrow(Type::Int32), DataType::Int32);
        assert_eq!(proto_type_to_arrow(Type::Uint32), DataType::UInt32);
        assert_eq!(proto_type_to_arrow(Type::Bool), DataType::Boolean);
        assert_eq!(proto_type_to_arrow(Type::String), DataType::Utf8);
        assert_eq!(proto_type_to_arrow(Type::Bytes), DataType::Binary);
        assert_eq!(proto_type_to_arrow(Type::Message), DataType::Utf8);
        assert_eq!(proto_type_to_arrow(Type::Enum), DataType::Int32);
    }

    #[test]
    fn test_with_schema_constructor() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Utf8, true),
        ]));
        let mappings = vec![(1, Type::Int64), (2, Type::String)];
        let config = ProtobufDecoderConfig::default();
        let decoder = ProtobufDecoder::with_schema(schema.clone(), mappings, config);

        assert_eq!(decoder.output_schema().fields().len(), 2);
        assert_eq!(decoder.fields.len(), 2);
    }

    /// Repeated fields produce an explicit error rather than silently
    /// overwriting earlier values. This test verifies the error message
    /// so callers know what to expect.
    #[test]
    fn test_repeated_field_produces_error() {
        // Create a descriptor with a repeated field.
        let fds = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("test.proto".into()),
                package: Some("test".into()),
                message_type: vec![DescriptorProto {
                    name: Some("WithRepeated".into()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("id".into()),
                            number: Some(1),
                            r#type: Some(Type::Int64 as i32),
                            label: Some(Label::Optional as i32),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("tags".into()),
                            number: Some(2),
                            r#type: Some(Type::String as i32),
                            label: Some(Label::Repeated as i32),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let config = ProtobufDecoderConfig::default();
        let decoder = ProtobufDecoder::from_descriptor_set(&fds, config).unwrap();

        // Build a message with two occurrences of field 2 (repeated).
        let mut msg = Vec::new();
        encode_varint_field(&mut msg, 1, 42);
        encode_bytes_field(&mut msg, 2, b"tag1");
        encode_bytes_field(&mut msg, 2, b"tag2"); // second occurrence

        let record = RawRecord::new(msg);
        let result = decoder.decode_batch(&[record]);
        assert!(result.is_err(), "repeated field should produce an error");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("repeated fields are not yet supported"),
            "unexpected error: {err_msg}"
        );
    }

    /// Schema construction maps repeated fields to Utf8 (for future JSON
    /// array support). Verify the field metadata is correct.
    #[test]
    fn test_repeated_field_schema_mapping() {
        let fds = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("test.proto".into()),
                package: Some("test".into()),
                message_type: vec![DescriptorProto {
                    name: Some("WithRepeated".into()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("id".into()),
                            number: Some(1),
                            r#type: Some(Type::Int64 as i32),
                            label: Some(Label::Optional as i32),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("tags".into()),
                            number: Some(2),
                            r#type: Some(Type::String as i32),
                            label: Some(Label::Repeated as i32),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let config = ProtobufDecoderConfig::default();
        let decoder = ProtobufDecoder::from_descriptor_set(&fds, config).unwrap();
        let schema = decoder.output_schema();

        // Repeated field mapped to Utf8 and nullable
        let tags_field = schema.field_with_name("tags").unwrap();
        assert_eq!(tags_field.data_type(), &DataType::Utf8);
        assert!(tags_field.is_nullable());
    }
}
