# F-CONN-003: Avro Serialization Hardening

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-CONN-003 |
| **Status** | ✅ Done |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | M (2-3 days) |
| **Dependencies** | F025/F026 (Kafka Connectors) |

## Summary

Harden the Avro serialization stack for production use: add round-trip
integration tests, support complex Avro types (arrays, maps, nested records),
add schema registry cache eviction, enforce schema compatibility on sink
registration, and improve error diagnostics.

## Motivation

The Avro serializer/deserializer and Schema Registry client are functionally
complete but have gaps that would surface in production:

1. **No round-trip integration tests**: Individual unit tests exist but no
   serialize → deserialize → verify identity test
2. **Complex types fall back to Utf8**: Arrays, maps, and nested records are
   serialized as JSON strings instead of native Arrow types
3. **Schema registry cache never evicts**: Long-running pipelines could leak
   memory with many schema versions
4. **No compatibility enforcement**: Sinks can register incompatible schemas
5. **Generic error codes**: All Avro failures return `SerdeError::MalformedInput`,
   making root cause analysis difficult

## Design

### 1. Round-Trip Integration Tests

Add tests in `crates/laminar-connectors/src/kafka/tests/`:

```rust
#[test]
fn test_avro_roundtrip_primitives() {
    let schema = Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("volume", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]);
    let batch = create_test_batch(&schema);

    let serializer = AvroSerializer::new(schema.clone().into(), 1);
    let serialized = serializer.serialize(&batch)?;

    let mut deserializer = AvroDeserializer::new();
    deserializer.register_schema(1, &arrow_to_avro_schema(&schema, "test")?)?;
    let deserialized = deserializer.deserialize_batch(
        &serialized.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
        &schema.into(),
    )?;

    assert_batches_equal(&batch, &deserialized);
}

#[test]
fn test_avro_roundtrip_nullable_fields() { ... }

#[test]
fn test_avro_roundtrip_all_arrow_types() { ... }

#[test]
fn test_avro_confluent_wire_format_roundtrip() {
    // Verify 5-byte header: 0x00 + 4-byte schema ID
}
```

### 2. Complex Avro Type Support

Extend `parse_avro_type()` in `schema_registry.rs`:

```rust
fn parse_avro_type(avro_type: &serde_json::Value) -> DataType {
    match avro_type {
        // Existing primitives...

        // NEW: Array type
        Value::Object(obj) if obj.get("type") == Some(&Value::String("array".into())) => {
            let items = obj.get("items").unwrap();
            DataType::List(Arc::new(Field::new("item", parse_avro_type(items), true)))
        }

        // NEW: Map type
        Value::Object(obj) if obj.get("type") == Some(&Value::String("map".into())) => {
            let values = obj.get("values").unwrap();
            DataType::Map(
                Arc::new(Field::new("entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", parse_avro_type(values), true),
                    ])),
                    false,
                )),
                false,
            )
        }

        // NEW: Nested record
        Value::Object(obj) if obj.get("type") == Some(&Value::String("record".into())) => {
            let fields: Vec<Field> = obj.get("fields").unwrap()
                .as_array().unwrap()
                .iter()
                .map(|f| {
                    let name = f["name"].as_str().unwrap();
                    let dtype = parse_avro_type(&f["type"]);
                    Field::new(name, dtype, true)
                })
                .collect();
            DataType::Struct(Fields::from(fields))
        }

        // NEW: Enum → Dictionary
        Value::Object(obj) if obj.get("type") == Some(&Value::String("enum".into())) => {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        }

        // NEW: Fixed → FixedSizeBinary
        Value::Object(obj) if obj.get("type") == Some(&Value::String("fixed".into())) => {
            let size = obj["size"].as_u64().unwrap() as i32;
            DataType::FixedSizeBinary(size)
        }
    }
}
```

Also extend `arrow_to_avro_schema()` for the reverse direction.

### 3. Schema Registry Cache Eviction

Add LRU cache with configurable TTL:

```rust
pub struct SchemaRegistryCacheConfig {
    /// Maximum number of cached schemas. Default: 1000.
    pub max_entries: usize,
    /// TTL for cache entries. Default: 1 hour. None = no eviction.
    pub ttl: Option<Duration>,
}

struct CachedSchema {
    schema_id: i32,
    avro_schema: String,
    arrow_schema: SchemaRef,
    inserted_at: Instant,  // NEW: for TTL
}
```

Replace `HashMap<i32, CachedSchema>` with a simple LRU that evicts on:
- Size limit exceeded (evict least recently used)
- TTL expired (lazy eviction on access)

### 4. Schema Compatibility Enforcement

Call `check_compatibility()` during sink schema registration:

```rust
// In KafkaSink::open():
if let Some(registry) = &self.schema_registry {
    let mut sr = registry.lock();
    let subject = format!("{}-value", self.config.topic);

    // Check compatibility before registering
    let avro_schema = arrow_to_avro_schema(&self.schema, &self.config.topic)?;
    match sr.check_compatibility(&subject, &avro_schema).await {
        Ok(true) => { /* compatible, proceed */ }
        Ok(false) => {
            return Err(ConnectorError::SchemaIncompatible {
                subject,
                message: "New schema is not compatible with existing schema".into(),
            });
        }
        Err(e) if e.is_404() => { /* no existing schema, first registration */ }
        Err(e) => return Err(e),
    }

    let schema_id = sr.register_schema(&subject, &avro_schema).await?;
    self.serializer.set_schema_id(schema_id);
}
```

### 5. Improved Error Diagnostics

Add specific Avro error variants:

```rust
pub enum SerdeError {
    // Existing...
    MalformedInput(String),

    // NEW: Avro-specific errors
    /// Schema ID not found in registry
    SchemaNotFound { schema_id: i32 },
    /// Confluent wire format magic byte mismatch
    InvalidConfluentHeader { expected: u8, got: u8 },
    /// Schema incompatible with existing version
    SchemaIncompatible { subject: String, message: String },
    /// Avro decode failure for specific column
    AvroDecodeError { column: String, avro_type: String, message: String },
    /// Record count mismatch after serialization
    RecordCountMismatch { expected: usize, got: usize },
}
```

## Test Plan

| Module | Tests | What |
|--------|-------|------|
| `avro` | 6 | Round-trip: primitives, nullables, all types, Confluent wire format |
| `avro_serializer` | 4 | Complex types: arrays, maps, nested records, enums |
| `schema_registry` | 6 | Cache eviction, TTL, LRU ordering, compatibility check |
| `error_handling` | 4 | Schema not found, invalid header, incompatible, decode error |
| `integration` | 3 | Sink registration + compatibility, source unknown schema resolve |

## Files

- `crates/laminar-connectors/src/kafka/avro.rs` — Complex type deserialization
- `crates/laminar-connectors/src/kafka/avro_serializer.rs` — Complex type serialization
- `crates/laminar-connectors/src/kafka/schema_registry.rs` — Cache eviction, compat enforcement
- `crates/laminar-connectors/src/serde/mod.rs` — New error variants
- `crates/laminar-connectors/src/kafka/sink.rs` — Compatibility check in open()
