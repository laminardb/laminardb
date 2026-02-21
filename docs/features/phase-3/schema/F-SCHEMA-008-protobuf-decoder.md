# F-SCHEMA-008: Protobuf Format Decoder

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SCHEMA-008 |
| **Status** | Draft |
| **Priority** | P2 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-SCHEMA-001 (FormatDecoder trait), F-SCHEMA-006 (Avro Decoder -- shares registry infrastructure) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-21 |
| **Crate** | `laminar-formats` |
| **Module** | `laminar-formats/src/protobuf/decoder.rs`, `laminar-formats/src/protobuf/descriptor.rs` |

## Summary

Implement `ProtobufDecoder` as a `FormatDecoder` for decoding Protocol Buffers binary data into Arrow `RecordBatch`. The decoder supports two schema source modes: (1) static `.proto` descriptor files compiled at CREATE SOURCE time, and (2) dynamic descriptors fetched from a Confluent Schema Registry (reusing the registry infrastructure from F-SCHEMA-006). Protobuf messages are decoded using `prost` for compiled descriptors and `prost-reflect` for dynamic/registry-provided descriptors.

The decoder shares the `SchemaRegistryAware` trait infrastructure with the Avro decoder, using the `RegistrySchemaType::Protobuf` variant. This allows Kafka sources to transparently switch between Avro and Protobuf wire formats with the same registry configuration pattern.

## Goals

- `ProtobufDecoder` implementing `FormatDecoder` for compiled Protobuf descriptors
- `DynamicProtobufDecoder` implementing `FormatDecoder` for registry-provided descriptors
- Static schema from `.proto` descriptor files (compiled via `prost-build` or loaded at runtime)
- Dynamic schema from Confluent Schema Registry (RegistrySchemaType::Protobuf)
- Complete Protobuf-to-Arrow type mapping for all scalar, composite, and well-known types
- Confluent wire format support for Protobuf (same 5-byte header as Avro, plus message index)
- Schema registry integration via existing `SchemaRegistryAware` trait (shared with F-SCHEMA-006)
- `FormatEncoder` implementation for Protobuf sink encoding
- Nested message support with automatic Arrow Struct flattening
- Repeated field support mapped to Arrow List types
- Map field support mapped to Arrow Map types
- Well-known types: `Timestamp`, `Duration`, `Any`, `Struct`, `Value`, wrapper types

## Non-Goals

- gRPC service support or streaming RPC
- Protobuf-to-Protobuf schema evolution (handled by `SchemaEvolvable` trait)
- Proto2 extension fields (proto3 only for initial release)
- Custom Protobuf options or annotations
- Protobuf text format parsing
- Proto file compilation (`prost-build`) -- descriptor files are provided pre-compiled
- Protobuf reflection-based field access at Ring 0 (all decoding happens in Ring 1)

## Technical Design

### Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Schema Resolution (Ring 2)                  │
│                                                                │
│  Path A: Static Descriptor                                     │
│    CREATE SOURCE ... FORMAT PROTOBUF                           │
│      USING DESCRIPTOR '/path/to/descriptor.bin'                │
│      MESSAGE 'com.example.Trade'                               │
│    → Load FileDescriptorSet → find message → Arrow schema      │
│                                                                │
│  Path B: Schema Registry                                       │
│    CREATE SOURCE ... FORMAT PROTOBUF                           │
│      USING SCHEMA REGISTRY (url = '...', subject = '...')      │
│    → Fetch Protobuf schema → parse descriptor → Arrow schema   │
│    → Reuses SchemaRegistryAware from F-SCHEMA-006              │
│    → Uses RegistrySchemaType::Protobuf variant                 │
└──────────────────────────┬─────────────────────────────────────┘
                           │
┌──────────────────────────┼─────────────────────────────────────┐
│                    Ring 1 (Decode Path)                         │
│                          ▼                                     │
│  ProtobufDecoder                                               │
│    ├── Static mode: prost-generated structs                    │
│    │   → Direct field access, compile-time type safety         │
│    │   → Fastest decode path                                   │
│    │                                                           │
│    └── Dynamic mode: prost-reflect DynamicMessage              │
│        → Runtime descriptor, no codegen needed                 │
│        → Slightly slower but fully flexible                    │
│                                                                │
│  Confluent Wire Format (Protobuf variant):                     │
│    [0x00][schema_id: 4B BE][msg_index: varint][protobuf payload]│
│     │         │                │                    │           │
│     ▼         ▼                ▼                    ▼           │
│   Magic   DashMap cache   Message type          prost /        │
│   byte    (shared w/ Avro) selector           prost-reflect    │
│                                                                │
│  Output: Arrow RecordBatch → Ring 0 hot path                   │
└────────────────────────────────────────────────────────────────┘
```

### Ring Integration

| Operation | Ring | Latency Budget | Notes |
|-----------|------|----------------|-------|
| Descriptor file load / parse | Ring 2 | 1-50ms | One-time at CREATE SOURCE |
| Schema registry fetch (Protobuf) | Ring 2 | 50-500ms | Async HTTP, one-time per schema ID |
| Protobuf-to-Arrow schema conversion | Ring 2 | < 5ms | Walk message descriptor tree |
| DashMap cache lookup (shared with Avro) | Ring 1 | < 50ns | Lock-free concurrent read |
| Protobuf binary decode (prost) | Ring 1 | 1-5us per batch | Compiled path, zero-alloc where possible |
| Protobuf binary decode (prost-reflect) | Ring 1 | 5-15us per batch | Dynamic path, reflection overhead |
| Arrow RecordBatch assembly | Ring 1 | 1-5us | Column array construction |
| **Ring 0 impact** | **Ring 0** | **0ns** | **No Ring 0 involvement -- all decode in Ring 1** |

### API Design

```rust
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use prost_reflect::{
    DescriptorPool, DynamicMessage, MessageDescriptor,
};

use crate::schema::{
    FormatDecoder, FormatEncoder, RawRecord, RegistryConfig,
    RegistrySchemaType, SchemaError, SchemaResult,
};

// ── Protobuf → Arrow Type Mapping ──────────────────────────────

/// Maps Protobuf field types to Arrow DataTypes.
///
/// | Protobuf Type       | Arrow Type                      |
/// |---------------------|---------------------------------|
/// | double              | Float64                         |
/// | float               | Float32                         |
/// | int32               | Int32                           |
/// | int64               | Int64                           |
/// | uint32              | UInt32                          |
/// | uint64              | UInt64                          |
/// | sint32              | Int32                           |
/// | sint64              | Int64                           |
/// | fixed32             | UInt32                          |
/// | fixed64             | UInt64                          |
/// | sfixed32            | Int32                           |
/// | sfixed64            | Int64                           |
/// | bool                | Boolean                         |
/// | string              | Utf8                            |
/// | bytes               | Binary                          |
/// | enum                | Dictionary(Int32, Utf8)          |
/// | message (nested)    | Struct                          |
/// | repeated T          | List(T)                         |
/// | map<K, V>           | Map(K, V)                       |
/// | oneof               | Union (dense) or nullable fields |
///
/// Well-Known Types:
/// | google.protobuf.Timestamp | Timestamp(Microsecond, UTC) |
/// | google.protobuf.Duration  | Duration(Microsecond)       |
/// | google.protobuf.Any       | Struct { type_url: Utf8, value: Binary } |
/// | google.protobuf.Struct    | Utf8 (JSON string)          |
/// | google.protobuf.Value     | Utf8 (JSON string)          |
/// | google.protobuf.BoolValue | Boolean (nullable)          |
/// | google.protobuf.Int32Value| Int32 (nullable)            |
/// | google.protobuf.Int64Value| Int64 (nullable)            |
/// | google.protobuf.FloatValue| Float32 (nullable)          |
/// | google.protobuf.DoubleValue| Float64 (nullable)         |
/// | google.protobuf.StringValue| Utf8 (nullable)            |
/// | google.protobuf.BytesValue| Binary (nullable)           |
/// | google.protobuf.UInt32Value| UInt32 (nullable)          |
/// | google.protobuf.UInt64Value| UInt64 (nullable)          |
fn protobuf_to_arrow_type(
    field_descriptor: &prost_reflect::FieldDescriptor,
) -> SchemaResult<DataType> {
    // Handle well-known types first, then general mapping.
    todo!()
}

/// Converts a Protobuf message descriptor to an Arrow Schema.
///
/// Walks the message descriptor tree recursively, mapping each
/// field to an Arrow field. Nested messages become Struct fields.
/// Repeated fields become List fields. Map fields become Map fields.
fn message_to_arrow_schema(
    descriptor: &MessageDescriptor,
) -> SchemaResult<SchemaRef> {
    let mut fields = Vec::new();

    for field_desc in descriptor.fields() {
        let name = field_desc.name().to_string();
        let data_type = protobuf_to_arrow_type(&field_desc)?;

        // In proto3, all singular fields are implicitly optional
        // (they have a default zero value). We mark them as
        // nullable in Arrow for compatibility.
        let nullable = !field_desc.is_list()
            && !field_desc.is_map();

        fields.push(Field::new(&name, data_type, nullable));
    }

    Ok(Arc::new(Schema::new(fields)))
}

// ── Static Protobuf Decoder ─────────────────────────────────────

/// Decodes Protobuf messages using a pre-loaded descriptor file.
///
/// The descriptor file (`.bin` from `protoc --descriptor_set_out`)
/// is parsed at construction time. The specified message type is
/// located in the descriptor pool and used for all subsequent
/// decoding.
///
/// This is the preferred mode for production deployments where the
/// `.proto` schema is known at build time.
#[derive(Debug)]
pub struct ProtobufDecoder {
    /// The output Arrow schema derived from the message descriptor.
    output_schema: SchemaRef,
    /// The parsed descriptor pool containing all message types.
    descriptor_pool: DescriptorPool,
    /// The specific message descriptor for decoding.
    message_descriptor: MessageDescriptor,
}

impl ProtobufDecoder {
    /// Creates a decoder from a compiled descriptor file.
    ///
    /// # Arguments
    ///
    /// * `descriptor_bytes` - The serialized `FileDescriptorSet`
    ///   (output of `protoc --descriptor_set_out`).
    /// * `message_name` - Fully qualified message name
    ///   (e.g., "com.example.Trade").
    ///
    /// # Errors
    ///
    /// Returns `SchemaError::Incompatible` if the descriptor is
    /// invalid or the message name is not found.
    pub fn new(
        descriptor_bytes: &[u8],
        message_name: &str,
    ) -> SchemaResult<Self> {
        let descriptor_pool = DescriptorPool::decode(descriptor_bytes)
            .map_err(|e| SchemaError::Incompatible(format!(
                "failed to parse Protobuf descriptor: {e}"
            )))?;

        let message_descriptor = descriptor_pool
            .get_message_by_name(message_name)
            .ok_or_else(|| SchemaError::Incompatible(format!(
                "message '{message_name}' not found in descriptor. \
                 Available messages: {:?}",
                descriptor_pool.all_messages()
                    .map(|m| m.full_name().to_string())
                    .collect::<Vec<_>>(),
            )))?;

        let output_schema = message_to_arrow_schema(
            &message_descriptor,
        )?;

        Ok(Self {
            output_schema,
            descriptor_pool,
            message_descriptor,
        })
    }

    /// Decodes a single Protobuf binary message into a DynamicMessage.
    fn decode_message(
        &self,
        data: &[u8],
    ) -> SchemaResult<DynamicMessage> {
        DynamicMessage::decode(
            self.message_descriptor.clone(),
            data,
        ).map_err(|e| SchemaError::DecodeError(format!(
            "Protobuf decode error: {e}"
        )))
    }
}

impl FormatDecoder for ProtobufDecoder {
    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn decode_batch(
        &self,
        records: &[RawRecord],
    ) -> SchemaResult<RecordBatch> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(
                self.output_schema.clone(),
            ));
        }

        // Decode all messages into DynamicMessage instances.
        let messages: Vec<DynamicMessage> = records
            .iter()
            .map(|r| self.decode_message(&r.value))
            .collect::<SchemaResult<Vec<_>>>()?;

        // Convert DynamicMessages to Arrow column arrays.
        dynamic_messages_to_record_batch(
            &messages,
            &self.message_descriptor,
            &self.output_schema,
        )
    }

    fn format_name(&self) -> &str {
        "protobuf"
    }
}

// ── Confluent Wire Format Protobuf Decoder ──────────────────────

/// Confluent wire format header for Protobuf.
///
/// Unlike Avro, the Confluent Protobuf wire format includes an
/// additional message index after the schema ID:
///
/// ```text
/// [0x00][schema_id: 4B BE][msg_index_count: varint][msg_indices: varint*][payload]
/// ```
///
/// The message index identifies which message type within the
/// `.proto` schema is being used (a single schema can define
/// multiple message types). An index of \[0\] (length 1, value 0)
/// means the first message in the schema.
const CONFLUENT_MAGIC: u8 = 0x00;

/// Decodes Confluent wire-format Protobuf messages.
///
/// Shares the `DashMap<u32, ...>` schema cache pattern with the
/// Avro decoder (F-SCHEMA-006). The cache key is the registry
/// schema ID; the value is the parsed `DescriptorPool` and its
/// derived Arrow schema.
///
/// Registry integration reuses the `SchemaRegistryAware` trait,
/// using `RegistrySchemaType::Protobuf` to indicate that the
/// registry stores Protobuf schemas rather than Avro.
#[derive(Debug)]
pub struct ConfluentProtobufDecoder {
    /// The frozen output Arrow schema.
    output_schema: SchemaRef,
    /// Lock-free schema cache: schema_id → descriptor + Arrow schema.
    schema_cache: Arc<DashMap<u32, Arc<CachedProtobufSchema>>>,
    /// Schema registry client (shared with Avro decoder).
    registry: Arc<SchemaRegistryClient>,
    /// Default message name (from CREATE SOURCE).
    default_message_name: String,
}

/// Cached Protobuf schema for a specific registry schema ID.
#[derive(Debug)]
struct CachedProtobufSchema {
    /// The parsed descriptor pool.
    descriptor_pool: DescriptorPool,
    /// The target message descriptor.
    message_descriptor: MessageDescriptor,
    /// The derived Arrow schema.
    arrow_schema: SchemaRef,
    /// Registry schema version.
    version: u32,
}

impl ConfluentProtobufDecoder {
    /// Creates a decoder from a RegisteredSchema.
    ///
    /// The registry returns the Protobuf schema as a serialized
    /// `FileDescriptorSet` or as `.proto` text that is compiled
    /// at runtime.
    pub fn new(
        registered: &RegisteredSchema,
        registry: Arc<SchemaRegistryClient>,
        message_name: &str,
    ) -> SchemaResult<Self> {
        let descriptor_pool = parse_registry_protobuf_schema(
            &registered.raw_schema,
        )?;

        let message_descriptor = descriptor_pool
            .get_message_by_name(message_name)
            .ok_or_else(|| SchemaError::Incompatible(format!(
                "message '{message_name}' not found in registry schema"
            )))?;

        let arrow_schema = message_to_arrow_schema(
            &message_descriptor,
        )?;

        let cache = Arc::new(DashMap::new());
        cache.insert(registered.schema_id, Arc::new(CachedProtobufSchema {
            descriptor_pool: descriptor_pool.clone(),
            message_descriptor: message_descriptor.clone(),
            arrow_schema: arrow_schema.clone(),
            version: registered.version,
        }));

        Ok(Self {
            output_schema: arrow_schema,
            schema_cache: cache,
            registry,
            default_message_name: message_name.to_string(),
        })
    }

    /// Parses the Confluent Protobuf wire format header.
    ///
    /// Returns (schema_id, message_index, payload_offset).
    ///
    /// The message index is a zigzag-varint-encoded array length
    /// followed by that many varint message indices. An empty
    /// array (length 0) or \[0\] both indicate the first message.
    pub fn parse_confluent_header(
        data: &[u8],
    ) -> SchemaResult<(u32, Vec<i32>, usize)> {
        if data.len() < 5 || data[0] != CONFLUENT_MAGIC {
            return Err(SchemaError::DecodeError(
                "not in Confluent Protobuf wire format".into()
            ));
        }

        let schema_id = u32::from_be_bytes([
            data[1], data[2], data[3], data[4],
        ]);

        // Parse message index array (varint-encoded).
        let mut offset = 5;
        let (array_len, bytes_read) = decode_varint(&data[offset..])
            .map_err(|e| SchemaError::DecodeError(format!(
                "failed to decode message index length: {e}"
            )))?;
        offset += bytes_read;

        let mut indices = Vec::with_capacity(array_len as usize);
        for _ in 0..array_len {
            let (idx, bytes_read) = decode_varint(&data[offset..])
                .map_err(|e| SchemaError::DecodeError(format!(
                    "failed to decode message index: {e}"
                )))?;
            offset += bytes_read;
            indices.push(idx as i32);
        }

        Ok((schema_id, indices, offset))
    }

    /// Fetches and caches a Protobuf schema from the registry.
    async fn fetch_and_cache(
        &self,
        schema_id: u32,
    ) -> SchemaResult<Arc<CachedProtobufSchema>> {
        let registered = self.registry
            .get_by_id(schema_id as i32)
            .await
            .map_err(|e| SchemaError::RegistryError(format!(
                "failed to fetch Protobuf schema {schema_id}: {e}"
            )))?;

        let descriptor_pool = parse_registry_protobuf_schema(
            registered.schema_str.as_bytes(),
        )?;

        let message_descriptor = descriptor_pool
            .get_message_by_name(&self.default_message_name)
            .ok_or_else(|| SchemaError::Incompatible(format!(
                "message '{}' not found in registry schema ID {}",
                self.default_message_name, schema_id,
            )))?;

        let arrow_schema = message_to_arrow_schema(
            &message_descriptor,
        )?;

        let cached = Arc::new(CachedProtobufSchema {
            descriptor_pool,
            message_descriptor,
            arrow_schema,
            version: registered.version as u32,
        });

        self.schema_cache.insert(schema_id, Arc::clone(&cached));
        Ok(cached)
    }
}

impl FormatDecoder for ConfluentProtobufDecoder {
    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn decode_batch(
        &self,
        records: &[RawRecord],
    ) -> SchemaResult<RecordBatch> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(
                self.output_schema.clone(),
            ));
        }

        let mut all_messages = Vec::with_capacity(records.len());

        for record in records {
            let (schema_id, _msg_indices, payload_offset) =
                Self::parse_confluent_header(&record.value)?;

            let cached = self.schema_cache
                .get(&schema_id)
                .ok_or_else(|| SchemaError::DecodeError(format!(
                    "unknown Protobuf schema ID {schema_id} — call \
                     ensure_schema_registered() first"
                )))?;

            let payload = &record.value[payload_offset..];
            let message = DynamicMessage::decode(
                cached.message_descriptor.clone(),
                payload,
            ).map_err(|e| SchemaError::DecodeError(format!(
                "Protobuf decode error (schema ID {schema_id}): {e}"
            )))?;

            all_messages.push(message);
        }

        dynamic_messages_to_record_batch(
            &all_messages,
            // Use the first cached descriptor for schema.
            &self.schema_cache
                .iter()
                .next()
                .unwrap()
                .message_descriptor,
            &self.output_schema,
        )
    }

    fn format_name(&self) -> &str {
        "confluent-protobuf"
    }
}

// ── Helper: DynamicMessage → Arrow RecordBatch ──────────────────

/// Converts a batch of decoded DynamicMessages into an Arrow
/// RecordBatch.
///
/// Iterates over the message descriptor's fields, extracting
/// values from each DynamicMessage to build columnar Arrow arrays.
/// Nested messages are flattened into Struct arrays. Repeated
/// fields become List arrays. Map fields become Map arrays.
fn dynamic_messages_to_record_batch(
    messages: &[DynamicMessage],
    descriptor: &MessageDescriptor,
    schema: &SchemaRef,
) -> SchemaResult<RecordBatch> {
    // For each field in the schema:
    // 1. Extract values from all messages
    // 2. Build the appropriate Arrow array builder
    // 3. Append values
    // 4. Finish into Arc<dyn Array>
    //
    // Nested messages recurse into StructBuilder.
    // Repeated fields use ListBuilder.
    // Map fields use MapBuilder with Utf8 keys.
    todo!()
}

// ── Protobuf Encoder (for sinks) ────────────────────────────────

/// Encodes Arrow RecordBatches as Protobuf binary messages.
///
/// Supports both raw Protobuf encoding and Confluent wire-format
/// encoding (with schema ID header and message index).
#[derive(Debug)]
pub struct ProtobufEncoder {
    /// The Arrow schema this encoder expects.
    input_schema: SchemaRef,
    /// The target message descriptor.
    message_descriptor: MessageDescriptor,
    /// Optional schema ID for Confluent wire-format encoding.
    /// None means raw Protobuf (no header).
    schema_id: Option<u32>,
}

impl FormatEncoder for ProtobufEncoder {
    fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }

    fn encode_batch(
        &self,
        batch: &RecordBatch,
    ) -> SchemaResult<Vec<Vec<u8>>> {
        let mut encoded = Vec::with_capacity(batch.num_rows());

        for row_idx in 0..batch.num_rows() {
            // Build DynamicMessage from Arrow row.
            let message = arrow_row_to_dynamic_message(
                batch,
                row_idx,
                &self.message_descriptor,
            )?;

            let proto_bytes = message.encode_to_vec();

            let output = if let Some(schema_id) = self.schema_id {
                // Confluent wire format.
                let mut buf = Vec::with_capacity(
                    5 + 1 + proto_bytes.len()
                );
                buf.push(CONFLUENT_MAGIC);
                buf.extend_from_slice(&schema_id.to_be_bytes());
                // Message index: [0] (first message, length=1).
                buf.push(0x00); // array length 0 = default message
                buf.extend_from_slice(&proto_bytes);
                buf
            } else {
                proto_bytes
            };

            encoded.push(output);
        }

        Ok(encoded)
    }

    fn format_name(&self) -> &str {
        "protobuf"
    }
}
```

### SQL Interface

```sql
-- Mode 1: Static descriptor file
CREATE SOURCE trades (
    -- Schema derived from the Protobuf message descriptor.
    -- Column list is optional; if omitted, all message fields
    -- are included in the Arrow schema.
) FROM KAFKA (
    brokers     = 'broker1:9092',
    topic       = 'trades',
    group_id    = 'laminar-trades'
)
FORMAT PROTOBUF
USING DESCRIPTOR '/opt/schemas/trades.desc'
MESSAGE 'com.example.Trade';

-- Mode 2: Schema Registry (Confluent wire format)
CREATE SOURCE orders
FROM KAFKA (
    brokers     = 'broker1:9092',
    topic       = 'orders',
    group_id    = 'laminar-orders'
)
FORMAT PROTOBUF
USING SCHEMA REGISTRY (
    url         = 'http://schema-registry:8081',
    subject     = 'orders-value',
    credentials = SECRET sr_credentials
)
MESSAGE 'com.example.Order';

-- Protobuf sink with schema registration
CREATE SINK notifications INTO KAFKA (
    brokers = 'broker1:9092',
    topic   = 'notifications'
)
FORMAT PROTOBUF
USING SCHEMA REGISTRY (
    url = 'http://schema-registry:8081'
)
MESSAGE 'com.example.Notification'
FROM notification_view;

-- Describe the decoded schema
DESCRIBE SOURCE trades;
-- Column     | Type       | Nullable | Source
-- symbol     | VARCHAR    | YES      | protobuf
-- price      | DOUBLE     | YES      | protobuf
-- quantity   | INT64      | YES      | protobuf
-- timestamp  | TIMESTAMP  | YES      | protobuf (google.protobuf.Timestamp)
```

### Data Structures

```rust
/// Configuration for the Protobuf decoder.
#[derive(Debug, Clone)]
pub struct ProtobufDecoderConfig {
    /// Schema source: descriptor file or schema registry.
    pub schema_source: ProtobufSchemaSource,
    /// Fully qualified message name within the schema.
    pub message_name: String,
}

/// How the Protobuf schema is provided.
#[derive(Debug, Clone)]
pub enum ProtobufSchemaSource {
    /// Static descriptor file (FileDescriptorSet binary).
    Descriptor {
        /// Path to the descriptor file or raw bytes.
        path: String,
    },
    /// Schema registry (Confluent or compatible).
    Registry {
        /// Registry configuration (shared with Avro).
        config: RegistryConfig,
    },
}

/// Protobuf decode statistics for observability.
#[derive(Debug, Default)]
pub struct ProtobufDecodeStats {
    /// Total messages decoded.
    pub messages_decoded: u64,
    /// Total decode errors.
    pub decode_errors: u64,
    /// Total schema cache hits.
    pub cache_hits: u64,
    /// Total schema cache misses.
    pub cache_misses: u64,
    /// Total bytes decoded.
    pub bytes_decoded: u64,
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `SchemaError::Incompatible("failed to parse descriptor")` | Descriptor file is corrupt or not a valid FileDescriptorSet | Regenerate descriptor with `protoc --descriptor_set_out` |
| `SchemaError::Incompatible("message 'X' not found")` | Message name not found in descriptor | Check fully-qualified name, list available messages |
| `SchemaError::DecodeError("not in Confluent Protobuf wire format")` | Missing magic byte or truncated header | Verify FORMAT PROTOBUF is correct, check wire format |
| `SchemaError::DecodeError("Protobuf decode error")` | Payload does not match the message descriptor | Producer schema drift, regenerate descriptor |
| `SchemaError::DecodeError("unknown Protobuf schema ID N")` | Cache miss without prior schema fetch | Internal error -- connector should pre-fetch |
| `SchemaError::RegistryError("failed to fetch schema N")` | Registry unreachable or schema ID not found | Check registry URL, credentials, network |
| `SchemaError::DecodeError("failed to decode message index")` | Malformed Confluent header varint encoding | Check producer serializer version |

## Implementation Plan

### Phase 1: Type Mapping and Schema Conversion (1-2 days)

- [ ] Implement `protobuf_to_arrow_type()` for all scalar types
- [ ] Implement `message_to_arrow_schema()` with recursive message handling
- [ ] Handle well-known types (Timestamp, Duration, wrapper types)
- [ ] Handle enum types (Dictionary encoding)
- [ ] Handle oneof fields (Union or nullable fields)
- [ ] Unit tests for all type mappings

### Phase 2: Static Descriptor Decoder (1-2 days)

- [ ] Implement `ProtobufDecoder::new()` with descriptor loading
- [ ] Implement `decode_message()` via `prost-reflect` DynamicMessage
- [ ] Implement `dynamic_messages_to_record_batch()` conversion
- [ ] Handle nested messages (Struct arrays)
- [ ] Handle repeated fields (List arrays)
- [ ] Handle map fields (Map arrays)
- [ ] Unit tests for simple and complex message decoding

### Phase 3: Confluent Wire Format (1-2 days)

- [ ] Implement `ConfluentProtobufDecoder` with DashMap cache
- [ ] Implement `parse_confluent_header()` with varint message index
- [ ] Implement `fetch_and_cache()` for registry schema fetch
- [ ] Wire `RegistrySchemaType::Protobuf` into existing registry client
- [ ] Integration tests with Confluent wire-format payloads

### Phase 4: Encoder and SQL Wiring (1 day)

- [ ] Implement `ProtobufEncoder` with `FormatEncoder` trait
- [ ] Implement `arrow_row_to_dynamic_message()` conversion
- [ ] Wire `FORMAT PROTOBUF USING DESCRIPTOR` DDL parsing
- [ ] Wire `FORMAT PROTOBUF USING SCHEMA REGISTRY` DDL parsing
- [ ] Parse `MESSAGE 'fully.qualified.Name'` clause
- [ ] End-to-end test: Kafka source (Protobuf) -> pipeline -> Kafka sink (Protobuf)

## Testing Strategy

### Unit Tests (18+)

| Module | Tests | What |
|--------|-------|------|
| `type_mapping` | 8 | Each Protobuf scalar type, nested message, repeated, map, enum, oneof |
| `well_known_types` | 5 | Timestamp, Duration, Any, Struct/Value, wrapper types (BoolValue, Int32Value, etc.) |
| `confluent_header` | 3 | Valid header, message index parsing, malformed header |
| `schema_conversion` | 2 | Simple message to Arrow, complex nested message to Arrow |

### Integration Tests

| Test | Feature Gate | What |
|------|-------------|------|
| `test_protobuf_decode_simple` | `kafka` | Flat message with primitives |
| `test_protobuf_decode_nested` | `kafka` | Nested message with Struct fields |
| `test_protobuf_decode_repeated` | `kafka` | Repeated field decoded as Arrow List |
| `test_protobuf_decode_map` | `kafka` | Map field decoded as Arrow Map |
| `test_protobuf_decode_enum` | `kafka` | Enum field decoded as Dictionary |
| `test_protobuf_decode_wellknown_timestamp` | `kafka` | google.protobuf.Timestamp to Arrow Timestamp |
| `test_confluent_protobuf_roundtrip` | `kafka` | Confluent wire format encode -> decode identity |
| `test_registry_fetch_protobuf` | `kafka` | Mock registry with Protobuf schema type |
| `test_protobuf_encode_simple` | `kafka` | Arrow to Protobuf binary encoding |
| `test_descriptor_file_loading` | (base) | Load .desc file, verify message discovery |

### Benchmarks

| Benchmark | Target | Description |
|-----------|--------|-------------|
| `bench_protobuf_decode_simple` | < 5us / 1000 records | Flat message, 5 scalar fields |
| `bench_protobuf_decode_nested` | < 15us / 1000 records | 3-level nested message |
| `bench_confluent_header_parse` | < 10ns / record | Header + varint message index |
| `bench_protobuf_encode` | < 10us / 1000 records | Arrow row to Protobuf binary |

## Success Criteria

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Decode throughput | > 300K records/sec/core | `cargo bench --bench protobuf_bench` with flat schema |
| Nested message decode | > 100K records/sec/core | Benchmark with 3-level nesting |
| Confluent header parse | < 10ns per record | Benchmark header extraction only |
| Type coverage | 100% Protobuf scalar + well-known types | Unit tests for every type |
| Round-trip fidelity | Bit-exact Arrow->Protobuf->Arrow | Round-trip tests |
| Registry integration | Shared cache with Avro decoder | Verify DashMap reuse |
| Test count | 25+ tests | `cargo test --features kafka` |

## Competitive Comparison

| Feature | LaminarDB | Flink | RisingWave | Materialize | ClickHouse |
|---------|-----------|-------|------------|-------------|------------|
| Protobuf decode | prost-reflect (dynamic) | Generated Java classes | protobuf crate | No native support | Limited |
| Confluent wire format | Shared DashMap cache with Avro | Per-format cache | Per-format cache | N/A | N/A |
| Schema registry (Protobuf) | RegistrySchemaType::Protobuf | Confluent native | Confluent native | N/A | N/A |
| Dynamic descriptors | prost-reflect DynamicMessage | Descriptor pool | Compile-time only | N/A | N/A |
| Well-known types | Full (Timestamp, Duration, wrappers) | Full | Partial | N/A | Partial |
| Nested message support | Full → Arrow Struct | Full | Full | N/A | Limited |
| Decode latency | < 5us/batch (Ring 1) | ~ms | ~100ms | N/A | ~ms |
| Static + dynamic modes | Both | Generated only | Generated only | N/A | Generated only |

## References

- [F-SCHEMA-001: FormatDecoder Trait](F-SCHEMA-001-format-decoder.md)
- [F-SCHEMA-006: Avro Decoder](F-SCHEMA-006-avro-decoder.md) (shared registry infrastructure)
- [F025: Kafka Source](../F025-kafka-source.md)
- [F026: Kafka Sink](../F026-kafka-sink.md)
- [Schema Inference Design Research](../../../research/schema-inference-design.md)
- [Extensible Schema Traits Research](../../../research/extensible-schema-traits.md)
- [Protocol Buffers Language Guide](https://protobuf.dev/programming-guides/proto3/)
- [Confluent Schema Registry - Protobuf](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-protobuf.html)
- [`prost` Crate](https://crates.io/crates/prost)
- [`prost-reflect` Crate](https://crates.io/crates/prost-reflect)
- [`dashmap` Crate](https://crates.io/crates/dashmap)
