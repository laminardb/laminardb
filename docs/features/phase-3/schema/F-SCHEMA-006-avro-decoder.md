# F-SCHEMA-006: Avro Format Decoder with Schema Registry Integration

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SCHEMA-006 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F-SCHEMA-001 (FormatDecoder trait), F-SCHEMA-003 (SchemaRegistryAware trait) |
| **Blocks** | F-SCHEMA-008 (Protobuf Decoder — shares registry infrastructure) |
| **Owner** | TBD |
| **Created** | 2026-02-21 |
| **Crate** | `laminar-formats` |
| **Module** | `laminar-formats/src/avro/decoder.rs`, `laminar-formats/src/avro/confluent.rs` |

## Summary

Implement `AvroDecoder` and `ConfluentAvroDecoder` as `FormatDecoder` trait implementations for decoding Apache Avro binary data into Arrow `RecordBatch`. The `ConfluentAvroDecoder` handles the Confluent wire format (magic byte `0x00` + 4-byte big-endian schema ID + Avro payload) with a lock-free schema ID cache backed by `DashMap`. Integrates with the `SchemaRegistryAware` trait from F-SCHEMA-003 for automatic schema fetching, caching, and compatibility checking.

This feature provides the primary Avro ingestion path for Kafka sources using Confluent Schema Registry, the most common production Avro deployment pattern.

## Goals

- `AvroDecoder` implementing `FormatDecoder` for raw Avro binary payloads
- `ConfluentAvroDecoder` implementing `FormatDecoder` for the Confluent wire format
- Lock-free schema ID cache: `DashMap<u32, Arc<AvroSchema>>` for concurrent access without hot-path locks
- Cache hit path stays in Ring 1 (microsecond-level decode, no I/O)
- Cache miss enqueues async Ring 2 fetch, buffers the message until schema arrives
- `SchemaRegistryAware` trait implementation for Kafka+Avro sources
- `RegistryConfig` with URL, subject, credentials, and compatibility mode
- `RegisteredSchema` with schema_id, version, and raw schema string
- Zero-copy path for fixed-width Avro types (int, long, float, double, fixed)
- Full Avro type mapping to Arrow: primitives, complex types (records, arrays, maps, enums, unions, fixed)
- Avro logical type support: date, time-millis, time-micros, timestamp-millis, timestamp-micros, decimal, uuid
- Schema evolution support via reader/writer schema resolution
- `FormatEncoder` implementation for Avro sink encoding

## Non-Goals

- Avro Object Container File (OCF) reading (handled by file source connectors)
- Custom Avro codecs beyond deflate/snappy (out of scope for wire-format decoder)
- Avro RPC protocol support
- Schema inference from Avro data (Avro is always schema-driven)
- Schema Registry UI or management tools
- Multi-registry federation (single registry per source)

## Technical Design

### Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    CREATE SOURCE (Ring 2)                      │
│                                                                │
│  SchemaResolver                                                │
│    ├── source.as_schema_registry_aware() → Some(KafkaSource)   │
│    └── fetch_schema(registry_config) → RegisteredSchema        │
│         ├── schema_id: 42                                      │
│         ├── version: 3                                         │
│         └── raw_schema: "{\"type\":\"record\",...}"             │
│                          │                                     │
│                          ▼                                     │
│            ConfluentAvroDecoder::new(registered)               │
│              └── pre-populates cache[42] = Arc<AvroSchema>     │
└──────────────────────────┬─────────────────────────────────────┘
                           │
┌──────────────────────────┼─────────────────────────────────────┐
│                    Ring 1 (Hot Path)                            │
│                          ▼                                     │
│  Kafka Message: [0x00][schema_id: 4B BE][avro payload]         │
│                   │         │                    │              │
│                   ▼         ▼                    ▼              │
│              Magic byte  DashMap lookup     apache-avro         │
│              validation  (lock-free)        decode              │
│                                                                │
│  Cache HIT  ─────────────────────────────┐                     │
│    decode_batch() → RecordBatch           │ < 10us per batch   │
│                                           │                    │
│  Cache MISS ─────────────────────────────┐                     │
│    buffer message                         │                    │
│    enqueue schema fetch → Ring 2          │                    │
│    Ring 2 fetches, registers, retries     │                    │
└───────────────────────────────────────────┴────────────────────┘
```

### Ring Integration

| Operation | Ring | Latency Budget | Notes |
|-----------|------|----------------|-------|
| Schema registry fetch | Ring 2 | 50-500ms | Async HTTP to registry, one-time per schema ID |
| Schema parsing / compilation | Ring 2 | 1-10ms | Parse Avro JSON schema, build reader schema |
| DashMap cache lookup | Ring 1 | < 50ns | Lock-free concurrent read |
| Avro binary decode | Ring 1 | 1-10us per batch | `apache-avro` decoder with pre-compiled schema |
| Schema ID extraction (header) | Ring 1 | < 10ns | 5-byte header parse |
| Arrow RecordBatch assembly | Ring 1 | 1-5us | Column array construction |
| **Ring 0 impact** | **Ring 0** | **0ns** | **No Ring 0 involvement — all decode in Ring 1** |

### API Design

```rust
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;

use crate::schema::{
    FormatDecoder, FormatEncoder, RawRecord, SchemaError, SchemaResult,
};

// ── Avro → Arrow Type Mapping ───────────────────────────────────

/// Maps Avro schema types to Arrow DataTypes.
///
/// | Avro Type          | Arrow Type                      |
/// |--------------------|---------------------------------|
/// | null               | Null                            |
/// | boolean            | Boolean                         |
/// | int                | Int32                           |
/// | long               | Int64                           |
/// | float              | Float32                         |
/// | double             | Float64                         |
/// | bytes              | Binary                          |
/// | string             | Utf8                            |
/// | record             | Struct                          |
/// | enum               | Dictionary(Int32, Utf8)         |
/// | array              | List                            |
/// | map                | Map(Utf8, V)                    |
/// | union (nullable)   | nullable field                  |
/// | union (complex)    | Union (dense)                   |
/// | fixed              | FixedSizeBinary(N)              |
/// | date (logical)     | Date32                          |
/// | time-millis        | Time32(Millisecond)             |
/// | time-micros        | Time64(Microsecond)             |
/// | timestamp-millis   | Timestamp(Millisecond, UTC)     |
/// | timestamp-micros   | Timestamp(Microsecond, UTC)     |
/// | decimal (logical)  | Decimal128(precision, scale)    |
/// | uuid (logical)     | Utf8 (or FixedSizeBinary(16))   |
fn avro_to_arrow_type(avro_type: &AvroSchemaNode) -> SchemaResult<DataType> {
    // ...
}

// ── Raw Avro Decoder ────────────────────────────────────────────

/// Decodes raw Avro binary payloads (no Confluent header) into
/// Arrow RecordBatches.
///
/// Used when the schema is known at CREATE SOURCE time and messages
/// do not carry Confluent wire-format headers (e.g., Avro files,
/// non-Confluent Kafka deployments).
#[derive(Debug)]
pub struct AvroDecoder {
    /// The frozen output Arrow schema.
    output_schema: SchemaRef,
    /// Pre-compiled Avro reader schema for fast decoding.
    reader_schema: Arc<apache_avro::Schema>,
}

impl AvroDecoder {
    /// Creates a new raw Avro decoder with the given Arrow schema.
    ///
    /// Converts the Arrow schema to an Avro reader schema at
    /// construction time. Subsequent decode calls use the
    /// pre-compiled schema with zero schema lookup overhead.
    ///
    /// # Errors
    ///
    /// Returns `SchemaError::Incompatible` if the Arrow schema
    /// cannot be converted to a valid Avro schema.
    pub fn new(
        output_schema: SchemaRef,
        avro_schema_json: &str,
    ) -> SchemaResult<Self> {
        let reader_schema = apache_avro::Schema::parse_str(avro_schema_json)
            .map_err(|e| SchemaError::Incompatible(
                format!("invalid Avro schema: {e}")
            ))?;
        Ok(Self {
            output_schema,
            reader_schema: Arc::new(reader_schema),
        })
    }
}

impl FormatDecoder for AvroDecoder {
    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn decode_batch(
        &self,
        records: &[RawRecord],
    ) -> SchemaResult<RecordBatch> {
        // 1. For each record, decode Avro binary using self.reader_schema
        // 2. Accumulate into Arrow column builders
        // 3. Build RecordBatch
        // Zero-copy path for fixed-width types: read directly into
        // Arrow buffers without intermediate Value allocation.
        todo!()
    }

    fn format_name(&self) -> &str {
        "avro"
    }
}

// ── Confluent Wire Format Decoder ───────────────────────────────

/// Magic byte for the Confluent wire format.
const CONFLUENT_MAGIC: u8 = 0x00;

/// Size of the Confluent header: 1 magic byte + 4 schema ID bytes.
const CONFLUENT_HEADER_SIZE: usize = 5;

/// Decodes Confluent wire-format Avro messages.
///
/// Wire format: `[0x00][schema_id: 4 bytes BE][avro payload]`
///
/// The decoder maintains a lock-free schema cache (`DashMap`) keyed
/// by schema ID. On cache hit, decoding proceeds inline in Ring 1.
/// On cache miss, the message is buffered and an async schema fetch
/// is enqueued to Ring 2.
///
/// **Important**: Uses `Fingerprint::Id(n)` directly for schema ID
/// registration -- NOT `load_fingerprint_id()` which applies a
/// `from_be` byte-swap meant only for raw wire bytes.
#[derive(Debug)]
pub struct ConfluentAvroDecoder {
    /// The frozen output Arrow schema (from the initial registry fetch).
    output_schema: SchemaRef,
    /// Lock-free concurrent cache: schema_id → compiled Avro schema.
    /// DashMap provides lock-free reads and fine-grained write locks
    /// per shard, avoiding any contention on the Ring 1 hot path.
    schema_cache: Arc<DashMap<u32, Arc<CachedAvroSchema>>>,
    /// Schema registry client for fetching unknown schema IDs.
    registry: Arc<SchemaRegistryClient>,
    /// Messages buffered while awaiting schema fetch (Ring 2).
    pending_buffer: Vec<BufferedRecord>,
}

/// A compiled Avro schema cached for fast decode.
#[derive(Debug)]
struct CachedAvroSchema {
    /// The parsed Avro schema.
    avro_schema: apache_avro::Schema,
    /// The Arrow schema derived from this Avro schema.
    arrow_schema: SchemaRef,
    /// Registry schema version (for diagnostics).
    version: u32,
}

/// A record buffered while its schema ID is being fetched.
#[derive(Debug)]
struct BufferedRecord {
    /// The schema ID from the Confluent header.
    schema_id: u32,
    /// The raw Avro payload (after stripping the 5-byte header).
    payload: Vec<u8>,
    /// Original record metadata for reassembly.
    metadata: SourceMetadata,
}

impl ConfluentAvroDecoder {
    /// Creates a decoder from a RegisteredSchema (fetched at
    /// CREATE SOURCE time).
    ///
    /// Pre-populates the schema cache with the initial schema ID
    /// so the first messages decode without a cache miss.
    pub fn new(
        registered: &RegisteredSchema,
        registry: Arc<SchemaRegistryClient>,
    ) -> SchemaResult<Self> {
        let avro_schema = apache_avro::Schema::parse_str(
            std::str::from_utf8(&registered.raw_schema)
                .map_err(|e| SchemaError::DecodeError(
                    format!("schema not valid UTF-8: {e}")
                ))?
        ).map_err(|e| SchemaError::Incompatible(
            format!("invalid Avro schema from registry: {e}")
        ))?;

        let cache = Arc::new(DashMap::new());
        cache.insert(registered.schema_id, Arc::new(CachedAvroSchema {
            avro_schema,
            arrow_schema: registered.schema.clone(),
            version: registered.version,
        }));

        Ok(Self {
            output_schema: registered.schema.clone(),
            schema_cache: cache,
            registry,
            pending_buffer: Vec::new(),
        })
    }

    /// Extracts the Confluent schema ID from a wire-format message.
    ///
    /// Returns `None` if the message is not in Confluent wire format
    /// (wrong magic byte or insufficient length).
    #[inline]
    pub fn extract_schema_id(data: &[u8]) -> Option<u32> {
        if data.len() < CONFLUENT_HEADER_SIZE
            || data[0] != CONFLUENT_MAGIC
        {
            return None;
        }
        let id = u32::from_be_bytes([
            data[1], data[2], data[3], data[4],
        ]);
        Some(id)
    }

    /// Fetches and caches a schema by ID from the registry.
    ///
    /// This is a Ring 2 operation — called asynchronously when a
    /// cache miss occurs during decode.
    async fn fetch_and_cache(
        &self,
        schema_id: u32,
    ) -> SchemaResult<Arc<CachedAvroSchema>> {
        let registered = self.registry
            .get_by_id(schema_id as i32)
            .await
            .map_err(|e| SchemaError::RegistryError(
                format!("failed to fetch schema {schema_id}: {e}")
            ))?;

        let avro_schema = apache_avro::Schema::parse_str(
            &registered.schema_str
        ).map_err(|e| SchemaError::Incompatible(
            format!("invalid Avro schema for ID {schema_id}: {e}")
        ))?;

        let arrow_schema = avro_to_arrow_schema(&avro_schema)?;
        let cached = Arc::new(CachedAvroSchema {
            avro_schema,
            arrow_schema,
            version: registered.version as u32,
        });

        self.schema_cache.insert(schema_id, Arc::clone(&cached));
        Ok(cached)
    }
}

impl FormatDecoder for ConfluentAvroDecoder {
    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn decode_batch(
        &self,
        records: &[RawRecord],
    ) -> SchemaResult<RecordBatch> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        }

        // Group records by schema_id for batch decode efficiency.
        let mut groups: HashMap<u32, Vec<&[u8]>> = HashMap::new();

        for record in records {
            let data = &record.value;
            let schema_id = Self::extract_schema_id(data)
                .ok_or_else(|| SchemaError::DecodeError(
                    "message is not in Confluent wire format".into()
                ))?;

            // Strip the 5-byte Confluent header.
            let payload = &data[CONFLUENT_HEADER_SIZE..];

            // Cache lookup — lock-free via DashMap.
            if self.schema_cache.contains_key(&schema_id) {
                groups.entry(schema_id).or_default().push(payload);
            } else {
                // Cache miss: this record cannot be decoded yet.
                // In practice, the connector polls ensure_schema()
                // before calling decode_batch.
                return Err(SchemaError::DecodeError(format!(
                    "unknown schema ID {schema_id} — call \
                     ensure_schema_registered() first"
                )));
            }
        }

        // Decode each group with its cached schema.
        let mut batches = Vec::with_capacity(groups.len());
        for (schema_id, payloads) in &groups {
            let cached = self.schema_cache.get(schema_id)
                .expect("verified above");

            let batch = decode_avro_payloads(
                payloads,
                &cached.avro_schema,
                &self.output_schema,
            )?;
            batches.push(batch);
        }

        // Concatenate all schema-group batches into one.
        if batches.len() == 1 {
            Ok(batches.into_iter().next().unwrap())
        } else {
            arrow::compute::concat_batches(
                &self.output_schema,
                &batches,
            ).map_err(|e| SchemaError::Arrow(e))
        }
    }

    fn format_name(&self) -> &str {
        "confluent-avro"
    }
}

// ── Avro Encoder (for sinks) ────────────────────────────────────

/// Encodes Arrow RecordBatches as Confluent wire-format Avro.
///
/// Prepends each encoded record with the 5-byte Confluent header
/// containing the magic byte and the registered schema ID.
#[derive(Debug)]
pub struct ConfluentAvroEncoder {
    /// The Arrow schema this encoder expects as input.
    input_schema: SchemaRef,
    /// The compiled Avro writer schema.
    writer_schema: Arc<apache_avro::Schema>,
    /// The registry-assigned schema ID for the Confluent header.
    schema_id: u32,
}

impl FormatEncoder for ConfluentAvroEncoder {
    fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }

    fn encode_batch(
        &self,
        batch: &RecordBatch,
    ) -> SchemaResult<Vec<Vec<u8>>> {
        let mut encoded = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let avro_value = arrow_row_to_avro(
                batch, row_idx, &self.writer_schema,
            )?;
            let avro_bytes = apache_avro::to_avro_datum(
                &self.writer_schema, avro_value,
            ).map_err(|e| SchemaError::DecodeError(
                format!("Avro encode failed: {e}")
            ))?;

            // Prepend Confluent wire-format header.
            let mut buf = Vec::with_capacity(
                CONFLUENT_HEADER_SIZE + avro_bytes.len()
            );
            buf.push(CONFLUENT_MAGIC);
            buf.extend_from_slice(&self.schema_id.to_be_bytes());
            buf.extend_from_slice(&avro_bytes);
            encoded.push(buf);
        }
        Ok(encoded)
    }

    fn format_name(&self) -> &str {
        "confluent-avro"
    }
}
```

### SQL Interface

```sql
-- Mode 1: Explicit schema + Avro format (no registry)
CREATE SOURCE trades (
    symbol      VARCHAR NOT NULL,
    price       DOUBLE NOT NULL,
    quantity    BIGINT NOT NULL,
    ts          TIMESTAMP NOT NULL
) FROM KAFKA (
    brokers     = 'broker1:9092,broker2:9092',
    topic       = 'trades',
    group_id    = 'laminar-trades'
)
FORMAT AVRO;

-- Mode 2: Schema from Confluent Schema Registry (recommended)
CREATE SOURCE market_data
FROM KAFKA (
    brokers     = 'broker1:9092',
    topic       = 'market-data',
    group_id    = 'laminar-md'
)
FORMAT AVRO
USING SCHEMA REGISTRY (
    url         = 'http://schema-registry:8081',
    subject     = 'market-data-value',
    credentials = SECRET sr_credentials,
    evolution   = 'forward_compatible'
);

-- Avro sink with schema registration
CREATE SINK alerts INTO KAFKA (
    brokers = 'broker1:9092',
    topic   = 'alerts'
)
FORMAT AVRO
USING SCHEMA REGISTRY (
    url         = 'http://schema-registry:8081',
    compatibility = 'backward'
)
FROM alert_view;

-- Decode Avro inline in SQL queries
SELECT from_avro(raw_bytes, SCHEMA REGISTRY 'http://registry:8081', 'subject-value')
FROM raw_source;
```

### Data Structures

```rust
/// Configuration for schema registry integration.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// Schema Registry URL (e.g., "http://schema-registry:8081").
    pub url: String,
    /// Subject name. Defaults to "{topic}-value" for Kafka sources.
    pub subject: Option<String>,
    /// Schema type: Avro, Protobuf, or JsonSchema.
    pub schema_type: RegistrySchemaType,
    /// Compatibility mode for schema evolution.
    pub compatibility: CompatibilityMode,
    /// Authentication credentials.
    pub credentials: Option<RegistryCredentials>,
}

/// A schema resolved from the registry with full metadata.
#[derive(Debug, Clone)]
pub struct RegisteredSchema {
    /// The Arrow schema derived from the registry schema.
    pub schema: SchemaRef,
    /// Registry-assigned schema ID (the 4-byte Confluent ID).
    pub schema_id: u32,
    /// Schema version within its subject.
    pub version: u32,
    /// Raw schema definition string (Avro JSON).
    pub raw_schema: Vec<u8>,
    /// Subject this schema belongs to.
    pub subject: String,
}

/// Compatibility modes matching Confluent Schema Registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityMode {
    None,
    Backward,
    Forward,
    Full,
    BackwardTransitive,
    ForwardTransitive,
    FullTransitive,
}

/// Schema type variants for the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrySchemaType {
    Avro,
    Protobuf,
    JsonSchema,
}

/// Registry authentication credentials.
#[derive(Debug, Clone)]
pub struct RegistryCredentials {
    /// HTTP Basic Auth username (Confluent Cloud API key).
    pub username: String,
    /// HTTP Basic Auth password (Confluent Cloud API secret).
    pub password: String,
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `SchemaError::DecodeError("not in Confluent wire format")` | Message missing `0x00` magic byte or < 5 bytes | Check source format config, verify FORMAT AVRO is correct |
| `SchemaError::RegistryError("failed to fetch schema N")` | Registry unreachable or schema ID not found | Check registry URL, credentials, network connectivity |
| `SchemaError::Incompatible("invalid Avro schema")` | Registry returned malformed Avro JSON | Verify schema in registry, check subject name |
| `SchemaError::DecodeError("unknown schema ID N")` | Cache miss without prior `ensure_schema_registered()` | Internal error -- connector should pre-fetch |
| `SchemaError::DecodeError("Avro decode failed: ...")` | Payload does not match the registered Avro schema | Producer schema drift, check compatibility settings |
| `SchemaError::EvolutionRejected("...")` | New schema version fails compatibility check | Fix producer schema or relax compatibility mode |
| `SchemaError::Arrow(...)` | Arrow array construction failed during batch assembly | Internal error, file a bug |

## Implementation Plan

### Phase 1: Core AvroDecoder (3-4 days)

- [ ] Define Avro-to-Arrow type mapping function (`avro_to_arrow_type`)
- [ ] Implement `AvroDecoder` with `FormatDecoder` trait
- [ ] Implement zero-copy path for fixed-width Avro primitives
- [ ] Handle Avro logical types (date, time, timestamp, decimal, uuid)
- [ ] Handle complex types (record, array, map, enum, union, fixed)
- [ ] Unit tests for all type mappings

### Phase 2: Confluent Wire Format (3-4 days)

- [ ] Implement `ConfluentAvroDecoder` with `DashMap` schema cache
- [ ] Implement `extract_schema_id()` header parser
- [ ] Implement schema-group batched decoding
- [ ] Implement `fetch_and_cache()` for Ring 2 schema resolution
- [ ] Wire `ensure_schema_registered()` into the Kafka source poll loop
- [ ] Integration tests with Confluent wire-format payloads

### Phase 3: Schema Registry Integration (2-3 days)

- [ ] Implement `SchemaRegistryAware` for the Avro decoder factory
- [ ] Implement `fetch_schema()`, `fetch_schema_by_id()`, `check_compatibility()`
- [ ] Implement `register_schema()` for sink-side registration
- [ ] Implement `build_registry_decoder()` factory method
- [ ] Add credential handling (Basic Auth for Confluent Cloud)
- [ ] Integration tests with mock schema registry

### Phase 4: Encoder and SQL Wiring (2-3 days)

- [ ] Implement `ConfluentAvroEncoder` with `FormatEncoder` trait
- [ ] Implement `arrow_row_to_avro()` conversion
- [ ] Wire `FORMAT AVRO USING SCHEMA REGISTRY (...)` DDL parsing
- [ ] Register `from_avro()` / `to_avro()` SQL functions
- [ ] End-to-end test: Kafka source (Avro) -> pipeline -> Kafka sink (Avro)

## Testing Strategy

### Unit Tests (20+)

| Module | Tests | What |
|--------|-------|------|
| `avro_types` | 12 | Each Avro type → Arrow mapping: null, bool, int, long, float, double, bytes, string, record, array, map, enum, union, fixed |
| `logical_types` | 6 | date, time-millis, time-micros, timestamp-millis, timestamp-micros, decimal, uuid |
| `confluent_header` | 5 | Valid header, wrong magic byte, too short, boundary (exactly 5 bytes), i32::MAX ID |
| `schema_cache` | 4 | Cache hit, cache miss, concurrent reads, schema ID eviction |

### Integration Tests (10+)

| Test | Feature Gate | What |
|------|-------------|------|
| `test_avro_roundtrip_primitives` | `kafka` | Serialize Arrow -> Avro -> deserialize -> verify identity |
| `test_avro_roundtrip_complex_types` | `kafka` | Arrays, maps, nested records, enums |
| `test_avro_roundtrip_nullable` | `kafka` | Nullable unions with null defaults |
| `test_confluent_wire_format_roundtrip` | `kafka` | Full 5-byte header + payload round-trip |
| `test_schema_evolution_add_column` | `kafka` | Writer adds nullable column, reader uses old schema |
| `test_schema_evolution_remove_column` | `kafka` | Writer removes column with default, reader ignores |
| `test_registry_fetch_and_cache` | `kafka` | Mock registry, verify DashMap cache population |
| `test_registry_compatibility_check` | `kafka` | Forward, backward, full compatibility modes |
| `test_multi_schema_batch` | `kafka` | Batch containing messages with different schema IDs |
| `test_sink_registration` | `kafka` | Sink registers schema, verifies compatibility, encodes |

### Benchmarks

| Benchmark | Target | Description |
|-----------|--------|-------------|
| `bench_avro_decode_simple` | < 5us / 1000 records | Flat schema, 5 primitive fields |
| `bench_avro_decode_complex` | < 20us / 1000 records | Nested record with array and map |
| `bench_confluent_header_parse` | < 5ns / record | Schema ID extraction only |
| `bench_dashmap_cache_hit` | < 50ns / lookup | Concurrent DashMap read under contention |
| `bench_avro_encode` | < 10us / 1000 records | Arrow to Confluent Avro encoding |

## Success Criteria

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Decode throughput | > 500K records/sec/core | `cargo bench --bench avro_bench` with 5-field flat schema |
| Cache hit decode latency | < 10us per 1000-record batch | Benchmark with pre-populated cache |
| Cache miss recovery time | < 100ms | Time from miss to successful decode |
| Schema ID cache concurrency | Lock-free reads, no contention | DashMap read benchmark under 8 threads |
| Type coverage | 100% Avro types mapped | Unit tests for every Avro primitive + logical type |
| Round-trip fidelity | Bit-exact Arrow->Avro->Arrow | Round-trip integration tests |
| Test count | 30+ tests | `cargo test --features kafka` |

## Competitive Comparison

| Feature | LaminarDB | Flink | RisingWave | Materialize | ClickHouse |
|---------|-----------|-------|------------|-------------|------------|
| Confluent wire format | Lock-free DashMap cache | HashMap + lock | HashMap + lock | HashMap + lock | No native SR |
| Cache miss handling | Async Ring 2 fetch, buffer | Block consumer | Block consumer | Block consumer | N/A |
| Schema evolution | Reader/writer resolution | Reader/writer | Reader only | Reader only | No evolution |
| Complex types | Full (record, array, map, enum, union, fixed) | Full | Full | Partial (no union) | Partial |
| Logical types | Full (date, time, timestamp, decimal, uuid) | Full | Partial | Full | Partial |
| Avro sink + registration | Schema registration + compat check | Schema registration | No auto-register | No auto-register | No |
| Decode latency | < 10us/batch (Ring 1) | ~ms | ~100ms | ~ms | ~ms |

## References

- [F-SCHEMA-001: FormatDecoder Trait](F-SCHEMA-001-format-decoder.md)
- [F-SCHEMA-003: SchemaRegistryAware Trait](F-SCHEMA-003-schema-registry.md)
- [F-CONN-003: Avro Hardening](../F-CONN-003-avro-hardening.md)
- [F025: Kafka Source](../F025-kafka-source.md)
- [F026: Kafka Sink](../F026-kafka-sink.md)
- [Schema Inference Design Research](../../../research/schema-inference-design.md)
- [Extensible Schema Traits Research](../../../research/extensible-schema-traits.md)
- [Apache Avro Specification](https://avro.apache.org/docs/current/specification/)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/)
- [`apache-avro` Crate](https://crates.io/crates/apache-avro)
- [`dashmap` Crate](https://crates.io/crates/dashmap)
