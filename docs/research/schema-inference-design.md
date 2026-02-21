# LaminarDB Schema Inference, Source/Sink Formats & Transformation Functions

## Design Specification — February 2026

---

## 1. Executive Summary

This document specifies how LaminarDB handles schema inference, format decoding/encoding, and data transformation functions across its connector ecosystem. The design draws from production patterns in RisingWave, Apache Flink, Materialize, ClickHouse, and DuckDB — adapted for LaminarDB's sub-500ns hot path constraint.

**Core Principle**: Schema resolution happens at source/sink creation time (Ring 1/Ring 2), never on the Ring 0 hot path. Once resolved, the Arrow schema is frozen and all records flow through zero-copy deserialization.

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Schema Resolution (Ring 2)                 │
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐ │
│  │ Schema       │  │ Schema Hints │  │ Auto-Inference     │ │
│  │ Registry     │  │ (User DDL)   │  │ (Sample-based)     │ │
│  └──────┬───────┘  └──────┬───────┘  └────────┬───────────┘ │
│         │                 │                    │              │
│         └────────────┬────┴────────────────────┘              │
│                      ▼                                        │
│              ┌──────────────┐                                 │
│              │ Arrow Schema │  ← Frozen at CREATE SOURCE time │
│              └──────┬───────┘                                 │
└─────────────────────┼───────────────────────────────────────┘
                      │
┌─────────────────────┼───────────────────────────────────────┐
│                 Format Layer (Ring 1)                         │
│                      ▼                                        │
│  ┌────────┐ ┌──────┐ ┌─────┐ ┌─────────┐ ┌───────┐ ┌─────┐ │
│  │  Avro  │ │ JSON │ │ CSV │ │ Parquet │ │Protobuf│ │ Raw │ │
│  │Decoder │ │Decode│ │Decode│ │ Reader  │ │Decoder│ │Bytes│ │
│  └────────┘ └──────┘ └─────┘ └─────────┘ └───────┘ └─────┘ │
│                      │                                        │
│              ┌───────▼────────┐                               │
│              │ Arrow RecordBatch │  → Ring 0 hot path         │
│              └────────────────┘                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. SQL DDL — Source/Sink Creation

### 3.1 Three Schema Modes

LaminarDB supports three schema resolution modes, inspired by RisingWave and Confluent Flink.

#### Mode 1: Explicit Schema (Recommended for Production)

```sql
CREATE SOURCE market_data (
    symbol      VARCHAR NOT NULL,
    price       DOUBLE NOT NULL,
    quantity    BIGINT NOT NULL,
    timestamp   TIMESTAMP NOT NULL,
    metadata    JSONB
) FROM KAFKA (
    brokers     = 'broker1:9092,broker2:9092',
    topic       = 'trades',
    group_id    = 'laminar-trades',
    start_offset = 'latest'
)
FORMAT JSON
ENCODE UTF8;
```

#### Mode 2: Schema Registry (Avro/Protobuf — Automatic)

```sql
CREATE SOURCE market_data
FROM KAFKA (
    brokers     = 'broker1:9092',
    topic       = 'trades',
    group_id    = 'laminar-trades'
)
FORMAT AVRO
USING SCHEMA REGISTRY (
    url         = 'http://schema-registry:8081',
    subject     = 'trades-value',                -- optional: defaults to '{topic}-value'
    credentials = SECRET sr_credentials,
    evolution   = 'forward_compatible'           -- forward|backward|full|none
);
```

Schema registry integration:
- Fetches Avro/Protobuf/JSON Schema from registry at `CREATE SOURCE` time
- Caches schema locally with the schema ID
- On incoming records, reads the 5-byte header (magic byte + 4-byte schema ID)
- If schema ID matches cached → zero-overhead decode
- If schema ID differs → Ring 2 fetches new schema, validates compatibility, updates decoder

#### Mode 3: Auto-Inference (Development / Exploration)

```sql
CREATE SOURCE raw_events
FROM KAFKA (
    brokers = 'localhost:9092',
    topic   = 'events'
)
FORMAT JSON
INFER SCHEMA (
    sample_size     = 1000,           -- number of messages to sample
    sample_timeout  = '5s',           -- max time to wait for samples
    null_as         = 'VARCHAR',      -- default type for null-only fields
    number_as       = 'DOUBLE',       -- or 'DECIMAL(38,18)' for precision
    timestamp_formats = ('iso8601', '%Y-%m-%d %H:%M:%S', 'epoch_millis'),
    max_depth       = 5,              -- max nesting depth to flatten
    array_strategy  = 'first_element' -- 'first_element' | 'union' | 'jsonb'
);
```

### 3.2 Schema Hints (Partial Inference)

Users can provide partial schemas with hints and let LaminarDB infer the rest:

```sql
CREATE SOURCE sensor_data (
    device_id   VARCHAR NOT NULL,     -- user-specified: exact type
    temperature DOUBLE,               -- user-specified: exact type
    *                                 -- wildcard: infer remaining fields
) FROM KAFKA (
    brokers = 'localhost:9092',
    topic   = 'sensors'
)
FORMAT JSON
INFER SCHEMA (
    sample_size = 500,
    wildcard_prefix = 'extra_'        -- prefix inferred fields to avoid collisions
);
```

The wildcard `*` tells LaminarDB:
1. Parse sample messages
2. For fields matching declared column names → use the declared type
3. For all other fields → infer type from samples, prefix with `extra_`
4. Generate the complete Arrow schema

### 3.3 Source Types — Full Catalog

```sql
-- Kafka Source
CREATE SOURCE s FROM KAFKA (brokers, topic, group_id, start_offset, ...)
    FORMAT {JSON | AVRO | CSV | PROTOBUF | RAW}
    [USING SCHEMA REGISTRY (...)]
    [INFER SCHEMA (...)]
    [INCLUDE KEY [AS name] | PARTITION [AS name] | OFFSET [AS name] | TIMESTAMP [AS name]]
    [ENVELOPE {NONE | DEBEZIUM | UPSERT}];

-- WebSocket Source
CREATE SOURCE ws FROM WEBSOCKET (
    url             = 'wss://feed.example.com/v1/stream',
    headers         = MAP['Authorization', SECRET api_key],
    reconnect       = 'exponential(1s, 30s)',
    ping_interval   = '30s',
    message_format  = 'json_lines'      -- 'json_lines' | 'json_array' | 'raw'
)
FORMAT JSON;

-- File Source (Batch + Append Streaming)
CREATE SOURCE logs FROM FILE (
    path            = 's3://bucket/logs/*.parquet',
    pattern         = '**/*.parquet',
    watch_interval  = '10s',             -- poll for new files
    file_sort       = 'modification_time'
)
FORMAT PARQUET;

-- CSV File Source
CREATE SOURCE csv_data FROM FILE (
    path        = '/data/trades.csv',
    watch       = true
)
FORMAT CSV (
    delimiter       = ',',
    has_header      = true,
    null_string     = 'NA',
    quote           = '"',
    escape          = '\\',
    skip_rows       = 0,
    comment         = '#',
    timestamp_format = '%Y-%m-%d %H:%M:%S%.f'
);

-- Iceberg Source (Streaming reads via incremental scan)
CREATE SOURCE iceberg_events FROM ICEBERG (
    catalog_type    = 'rest',            -- 'rest' | 'hive' | 'glue' | 'jdbc'
    catalog_uri     = 'http://iceberg-rest:8181',
    warehouse       = 's3://warehouse/iceberg',
    database        = 'analytics',
    table           = 'events',
    snapshot_mode   = 'incremental'      -- 'incremental' | 'latest' | 'timestamp'
);
-- Schema automatically inferred from Iceberg table metadata (field IDs preserved)

-- RabbitMQ Source
CREATE SOURCE amqp_data FROM RABBITMQ (
    host        = 'rabbitmq.internal',
    queue       = 'events',
    exchange    = 'topic_exchange',
    routing_key = 'events.#'
)
FORMAT JSON;
```

### 3.4 Sink Types

```sql
-- Kafka Sink
CREATE SINK alerts INTO KAFKA (
    brokers = 'broker1:9092',
    topic   = 'alerts'
)
FORMAT JSON                              -- or AVRO USING SCHEMA REGISTRY (...)
FROM alert_view;

-- Iceberg Sink
CREATE SINK lakehouse INTO ICEBERG (
    catalog_type    = 'glue',
    warehouse       = 's3://lake/iceberg',
    database        = 'analytics',
    table           = 'enriched_events',
    partition_by    = (day(event_time)),
    write_format    = 'parquet',
    commit_interval = '60s',
    target_file_size = '128MB'
)
FROM enriched_view;

-- WebSocket Sink
CREATE SINK live_feed INTO WEBSOCKET (
    bind_address = '0.0.0.0:8080',
    path         = '/api/v1/stream',
    format       = 'json_lines'
)
FROM dashboard_view;

-- Parquet File Sink
CREATE SINK archive INTO FILE (
    path            = 's3://bucket/archive/',
    file_format     = 'parquet',
    partition_by    = (year(ts), month(ts), day(ts)),
    rollover        = '100MB',
    naming          = '{partition}/{uuid}.parquet'
)
FROM processed_data;
```

---

## 4. Format Decoders / Encoders

### 4.1 Format Matrix

| Format | Source | Sink | Schema Registry | Auto-Infer | Self-Describing |
|--------|--------|------|-----------------|------------|-----------------|
| JSON | ✅ | ✅ | ✅ (JSON Schema) | ✅ | No |
| Avro | ✅ | ✅ | ✅ (Native) | Via registry | Yes (header) |
| Protobuf | ✅ | ✅ | ✅ (Native) | Via registry | No |
| CSV | ✅ | ✅ | ❌ | ✅ | No |
| Parquet | ✅ | ✅ | ❌ | ✅ (metadata) | Yes |
| Iceberg | ✅ | ✅ | ❌ | ✅ (catalog) | Yes |
| Raw Bytes | ✅ | ✅ | ❌ | ❌ | No |
| MessagePack | ✅ | ✅ | ❌ | ✅ | Partial |

### 4.2 JSON Decoder — Detailed Behavior

```
JSON Value          →  SQL Type (Default)    →  Arrow Type
───────────────────────────────────────────────────────────
true/false          →  BOOLEAN               →  Boolean
123                 →  BIGINT                 →  Int64
1.5                 →  DOUBLE                →  Float64
"hello"             →  VARCHAR               →  Utf8
"2025-01-01T..."    →  TIMESTAMP             →  Timestamp(ns)
null                →  NULL (inferred later)  →  Null
[1, 2, 3]           →  ARRAY<BIGINT>         →  List<Int64>
{"a": 1}            →  STRUCT or JSONB       →  Struct or LargeBinary
```

**Ambiguity Resolution Rules** (for auto-inference):
1. If a field is `"123"` in some records and `123` in others → `VARCHAR` (safest)
2. If a field is `null` in all sampled records → type from `null_as` option (default: `VARCHAR`)
3. If a string matches timestamp patterns → `TIMESTAMP` (user can configure `timestamp_formats`)
4. If a number has decimals in any sample → `DOUBLE` (not `BIGINT`)
5. If an object field has >N unique keys across samples → `JSONB` (not `STRUCT`)

### 4.3 CSV Decoder

Type inference for CSV follows DuckDB's approach:

1. Read `sample_size` rows
2. For each column, attempt parsing in order: `BOOLEAN → BIGINT → DOUBLE → TIMESTAMP → DATE → TIME → VARCHAR`
3. First type that successfully parses ALL sampled values wins
4. User can override any column with explicit types in the DDL

### 4.4 Avro Decoder (Zero-Copy Path)

For Avro with schema registry:

```
Kafka Message:  [0x00] [schema_id: 4 bytes] [avro payload]
                  │          │                    │
                  ▼          ▼                    ▼
             Magic byte  Cache lookup       rkyv zero-copy
                         (Ring 0 inline)    deserialization
```

- Schema ID cache is a lock-free `DashMap<u32, Arc<AvroSchema>>`
- Cache hit → decode Avro directly into Arrow arrays (hot path stays fast)
- Cache miss → enqueue schema fetch to Ring 2, buffer message
- Use `apache-avro` crate with pre-compiled reader schema for fastest decode

### 4.5 Parquet & Iceberg

Both are self-describing formats:
- **Parquet**: Arrow schema read directly from file metadata (exact 1:1 mapping)
- **Iceberg**: Schema read from table metadata JSON, including field IDs for evolution tracking
- Iceberg's field-ID tracking means column renames/reorders are handled transparently

---

## 5. Schema Evolution

### 5.1 Strategy

LaminarDB takes a **compatible-evolution** approach (inspired by Iceberg):

| Change | Handling | Hot Path Impact |
|--------|----------|----------------|
| Add column (nullable) | New column filled with `NULL` | None — Arrow schema extended |
| Add column (with default) | New column filled with default | None |
| Drop column | Column ignored in decode | None — projection excludes it |
| Rename column | Mapped by field ID (Avro/Iceberg) or rejected (JSON/CSV) | None |
| Widen type (int→bigint) | Automatic cast in decoder | Minimal — cast in Ring 1 |
| Narrow type | **Rejected** — breaking change | N/A |
| Reorder columns | Transparent (Arrow uses named fields) | None |

### 5.2 Schema Evolution DDL

```sql
-- View current schema
DESCRIBE SOURCE market_data;

-- Add a column (backfilled with NULL or default)
ALTER SOURCE market_data ADD COLUMN exchange VARCHAR DEFAULT 'UNKNOWN';

-- Drop a column
ALTER SOURCE market_data DROP COLUMN metadata;

-- Widen a type
ALTER SOURCE market_data ALTER COLUMN quantity TYPE DOUBLE;

-- Force schema refresh from registry
ALTER SOURCE market_data REFRESH SCHEMA;

-- View schema history
SELECT * FROM laminar_catalog.schema_history
WHERE source_name = 'market_data'
ORDER BY version DESC;
```

---

## 6. Transformation Functions

### 6.1 JSON Functions (PostgreSQL-Compatible)

These functions operate on `JSONB` columns in SQL queries and materialized views.

#### Extraction Operators

```sql
-- Arrow operator: extract JSON object field as JSONB
payload -> 'user'                        -- {"name": "Alice", "age": 30}

-- Double-arrow operator: extract as TEXT
payload ->> 'user_id'                    -- "12345"

-- Path extraction: nested access
payload -> 'user' -> 'address' ->> 'city'  -- "London"

-- Array element by index (0-based)
payload -> 'items' -> 0                  -- first element as JSONB
payload -> 'items' ->> 0                 -- first element as TEXT

-- Path operator: extract by path array
payload #> ARRAY['user', 'address', 'city']    -- JSONB
payload #>> ARRAY['user', 'address', 'city']   -- TEXT
```

#### JSON Scalar Functions

```sql
-- Type interrogation
json_typeof(payload -> 'price')          -- "number"
json_typeof(payload -> 'tags')           -- "array"

-- Existence checks
payload ? 'user_id'                      -- true if key exists
payload ?| ARRAY['name', 'email']        -- true if ANY key exists
payload ?& ARRAY['name', 'email']        -- true if ALL keys exist

-- Containment
payload @> '{"status": "active"}'::jsonb -- contains
'{"status": "active"}'::jsonb <@ payload -- contained by

-- Construction
json_build_object('key', value, ...)     -- build from k/v pairs
json_build_array(1, 'two', 3.0)         -- build array
to_jsonb(any_value)                      -- convert SQL → JSONB
row_to_json(record)                      -- record → JSON

-- Aggregation
json_agg(column)                         -- aggregate into JSON array
json_object_agg(key_col, value_col)      -- aggregate into JSON object
```

#### JSON Table-Valued Functions (Set-Returning)

These are critical for stream processing — they unnest JSON structures into rows:

```sql
-- Unnest JSON array into rows
SELECT elem.value
FROM sensor_data,
     LATERAL jsonb_array_elements(payload -> 'readings') AS elem;

-- Unnest with ordinality (position tracking)
SELECT elem.value, elem.ordinality
FROM events,
     LATERAL jsonb_array_elements(payload -> 'items')
     WITH ORDINALITY AS elem(value, ordinality);

-- Unnest array of text
SELECT elem
FROM events,
     LATERAL jsonb_array_elements_text(payload -> 'tags') AS elem;

-- Key-value pair iteration
SELECT kv.key, kv.value
FROM events,
     LATERAL jsonb_each(payload -> 'properties') AS kv(key, value);

-- Key-value as text
SELECT kv.key, kv.value
FROM events,
     LATERAL jsonb_each_text(payload -> 'metadata') AS kv(key, value);

-- Extract all keys
SELECT jsonb_object_keys(payload) AS key FROM events;
```

#### JSON Path Queries (SQL/JSON Standard)

```sql
-- Path exists
jsonb_path_exists(payload, '$.users[*] ? (@.age > 25)')

-- Path query (returns set of matches)
jsonb_path_query(payload, '$.items[*].price')

-- Path query as array
jsonb_path_query_array(payload, '$.items[*] ? (@.quantity > 10)')

-- Path match (boolean predicate)
jsonb_path_match(payload, 'exists($.user.premium ? (@ == true))')
```

### 6.2 JSON Transformation Functions (LaminarDB Extensions)

These are purpose-built for streaming data transformation:

```sql
-- Flatten nested JSON to columns (similar to DuckDB's json_transform)
-- Resolves schema at query planning time, not at execution time
SELECT json_to_columns(
    payload,
    'STRUCT(user_id BIGINT, name VARCHAR, address STRUCT(city VARCHAR, zip VARCHAR))'
) AS (user_id, name, address)
FROM raw_events;

-- Infer structure from the data (planning-time operation)
SELECT json_infer_schema(payload) FROM raw_events LIMIT 1;
-- Returns: '{"user_id": "BIGINT", "name": "VARCHAR", "tags": "ARRAY<VARCHAR>"}'

-- Merge/patch JSON objects
SELECT jsonb_merge(
    '{"a": 1, "b": 2}'::jsonb,
    '{"b": 3, "c": 4}'::jsonb
);
-- Returns: {"a": 1, "b": 3, "c": 4}

-- Deep merge (recursive)
SELECT jsonb_deep_merge(base_config, override_config) FROM configs;

-- Strip null fields
SELECT jsonb_strip_nulls(payload) FROM events;

-- Rename keys
SELECT jsonb_rename_keys(
    payload,
    MAP['old_field', 'new_field', 'ts', 'event_time']
) FROM events;

-- Select subset of keys (projection)
SELECT jsonb_pick(payload, ARRAY['user_id', 'timestamp', 'action']) FROM events;

-- Remove keys (anti-projection)
SELECT jsonb_except(payload, ARRAY['internal_id', 'debug_info']) FROM events;

-- Flatten nested object to dot-notation keys
SELECT jsonb_flatten(payload, '.')
FROM events;
-- {"user.name": "Alice", "user.address.city": "London", "tags.0": "vip"}

-- Unflatten dot-notation back to nested
SELECT jsonb_unflatten(flat_data, '.')
FROM flat_events;
```

### 6.3 Conversion Functions (Format Bridges)

```sql
-- Decode binary Avro (with inline schema)
SELECT from_avro(raw_bytes, '<avro_schema_json>') AS decoded FROM raw_source;

-- Decode binary Avro (with schema registry)
SELECT from_avro(raw_bytes, SCHEMA REGISTRY 'http://registry:8081', 'subject-value')
FROM raw_source;

-- Encode to Avro
SELECT to_avro(STRUCT(symbol, price, ts), '<avro_schema_json>') AS encoded
FROM trades;

-- JSON string to typed struct
SELECT from_json(
    json_string,
    'STRUCT(id BIGINT, name VARCHAR, scores ARRAY<DOUBLE>)'
) AS parsed
FROM text_source;

-- Typed struct to JSON string
SELECT to_json(STRUCT(id, name, scores)) AS json_out FROM parsed_data;

-- Parse CSV string to row
SELECT from_csv(line, 'id BIGINT, name VARCHAR, value DOUBLE', ',') FROM raw_lines;

-- MessagePack decode
SELECT from_msgpack(binary_data, 'STRUCT(...)') FROM raw_source;

-- Parse timestamps from various formats
SELECT parse_timestamp(ts_string, '%Y-%m-%d %H:%M:%S%.f', 'UTC') FROM events;
SELECT parse_epoch(epoch_col, 'milliseconds') FROM events;   -- epoch → TIMESTAMP
```

### 6.4 Array Functions

```sql
-- Array operations (work on both SQL arrays and JSON arrays)
array_length(arr)                        -- number of elements
array_contains(arr, value)               -- element existence
array_position(arr, value)               -- index of element
array_distinct(arr)                      -- unique elements
array_sort(arr)                          -- sorted copy
array_slice(arr, start, end)             -- sub-array
array_flatten(nested_arr)                -- flatten one level
array_agg(col ORDER BY ts)              -- aggregate to array
array_transform(arr, x -> x * 2)        -- map function (lambda)
array_filter(arr, x -> x > 10)          -- filter function
array_reduce(arr, 0, (acc, x) -> acc + x) -- fold/reduce
unnest(arr)                              -- explode to rows
```

### 6.5 Struct Functions

```sql
-- Access struct fields
record.field_name                         -- dot notation
struct_extract(record, 'field_name')     -- function form

-- Construct structs
STRUCT(field1, field2, field3)           -- from columns
named_struct('a', 1, 'b', 'hello')      -- from literals

-- Modify structs
struct_set(record, 'field', new_value)   -- set/add field
struct_drop(record, 'internal_field')    -- remove field
struct_rename(record, 'old', 'new')      -- rename field
struct_merge(record1, record2)           -- combine structs
```

### 6.6 Map Functions

```sql
-- Map operations
map_keys(m)                              -- all keys as array
map_values(m)                            -- all values as array
map_contains_key(m, 'key')              -- key existence
map_filter(m, (k, v) -> v > 0)          -- filter entries
map_transform_values(m, (k, v) -> upper(v))
element_at(m, 'key')                     -- get value (NULL if missing)
map_from_arrays(keys_arr, vals_arr)      -- construct from arrays
map_from_entries(array_of_structs)       -- from [{key, value}, ...]
```

---

## 7. Streaming-Specific Patterns

### 7.1 Late-Arriving Schema Changes

When a Kafka topic's schema changes mid-stream:

```
Time ──────────────────────────────────────────────▶

  Schema v1           Schema v2 (new field added)
  ┌──────────┐        ┌──────────────┐
  │ id: INT  │        │ id: INT      │
  │ name: STR│        │ name: STR    │
  └──────────┘        │ email: STR   │  ← new field
                      └──────────────┘

  Ring 2 detects schema change:
  1. Validates compatibility (forward_compatible → OK)
  2. Extends Arrow schema with nullable email column
  3. Atomically swaps decoder in Ring 1
  4. Old records: email = NULL
  5. New records: email = decoded value
  6. Zero downtime, zero hot-path disruption
```

### 7.2 DESCRIBE / INSPECT Commands

```sql
-- Show inferred or declared schema
DESCRIBE SOURCE market_data;
-- ┌───────────┬────────────┬──────────┬─────────┐
-- │ Column    │ Type       │ Nullable │ Source   │
-- ├───────────┼────────────┼──────────┼─────────┤
-- │ symbol    │ VARCHAR    │ NO       │ declared │
-- │ price     │ DOUBLE     │ NO       │ declared │
-- │ quantity  │ BIGINT     │ NO       │ declared │
-- │ timestamp │ TIMESTAMP  │ NO       │ declared │
-- │ metadata  │ JSONB      │ YES      │ declared │
-- └───────────┴────────────┴──────────┴─────────┘

-- Inspect raw format for debugging
SELECT * FROM laminar_inspect('market_data', LIMIT 5);
-- Shows raw bytes, decoded values, schema version, offsets

-- Preview inference without creating source
SELECT * FROM laminar_infer_schema(
    'kafka', 'broker1:9092', 'trades', 'json', 1000
);
```

### 7.3 Dead-Letter Handling

```sql
CREATE SOURCE trades (
    symbol VARCHAR NOT NULL,
    price  DOUBLE NOT NULL,
    ts     TIMESTAMP NOT NULL
) FROM KAFKA (
    brokers = 'broker1:9092',
    topic   = 'trades'
)
FORMAT JSON
ON ERROR (
    strategy = 'dead_letter',             -- 'skip' | 'dead_letter' | 'fail'
    dead_letter_topic = 'trades-dlq',
    max_errors_per_second = 100,
    include_raw_message = true
);
```

---

## 8. Performance Considerations for Ring 0

### 8.1 What Happens at Each Ring

| Operation | Ring | Latency Budget |
|-----------|------|----------------|
| Schema registry fetch | Ring 2 | Seconds (async) |
| Schema inference | Ring 2 | Seconds (one-time) |
| Decoder compilation | Ring 2 | Milliseconds (one-time) |
| Avro/JSON decode | Ring 1 | Microseconds (per batch) |
| Parquet page decode | Ring 1 | Microseconds (per batch) |
| Arrow RecordBatch processing | Ring 0 | < 500ns (per record) |
| JSON function (->>, etc.) | Ring 0 | < 100ns (JSONB binary path) |

### 8.2 Zero-Allocation JSON Access

For JSONB columns on the Ring 0 hot path:
- Store JSONB as pre-parsed binary format (similar to PostgreSQL's JSONB)
- `->` and `->>` operators use binary offset lookup, not string parsing
- Pre-compute field offsets during Ring 1 decode
- For known access patterns, generate specialized accessors at query compile time

### 8.3 Batch-Oriented Decoding

All format decoders produce Arrow `RecordBatch` in Ring 1:
- Batch size configurable: `decode_batch_size = 1024` (default)
- Decoders amortize overhead across batch (e.g., one schema lookup per batch)
- Arrow's columnar format enables SIMD operations in Ring 0 aggregation

---

## 9. Implementation Roadmap

### Phase 3A: Core Formats (Current)
- [ ] JSON decoder with explicit schema
- [ ] CSV decoder with explicit schema
- [ ] Avro decoder with schema registry
- [ ] Kafka source/sink integration
- [ ] Basic JSON functions (`->`, `->>`, `json_typeof`)

### Phase 3B: Inference & WebSocket
- [ ] JSON auto-inference engine (sample-based)
- [ ] CSV auto-inference engine
- [ ] Schema hints with wildcard (`*`)
- [ ] WebSocket source/sink
- [ ] Full JSON function set (array_elements, each, path queries)

### Phase 3C: Lakehouse Formats
- [ ] Parquet reader/writer (via `parquet` crate + Arrow)
- [ ] Iceberg source (incremental scan)
- [ ] Iceberg sink (with compaction)
- [ ] Schema evolution tracking

### Phase 3D: Advanced Functions
- [ ] `from_avro` / `to_avro` SQL functions
- [ ] `from_json` / `to_json` with struct coercion
- [ ] LaminarDB extension functions (flatten, merge, pick, rename)
- [ ] Lambda array/map transforms
- [ ] Protobuf support

---

## 10. Competitive Reference

| Feature | LaminarDB | Flink | RisingWave | Materialize | ClickHouse |
|---------|-----------|-------|------------|-------------|------------|
| JSON auto-infer | ✅ Sample | ❌ DDL only | ❌ DDL only | ❌ DDL only | ✅ File only |
| Schema registry | ✅ | ✅ (Confluent) | ✅ | ✅ | ✅ (Avro) |
| CSV inference | ✅ | ❌ | ❌ | ❌ | ✅ File only |
| Parquet inference | ✅ | ❌ | ✅ file_scan | ❌ | ✅ |
| Iceberg evolution | ✅ | ✅ | ✅ | ❌ | ✅ |
| JSON path queries | ✅ | ❌ | ✅ | ✅ (PG) | ❌ |
| Wildcard schema | ✅ | ❌ | ❌ | ❌ | ❌ |
| Schema hints | ✅ | ❌ | ❌ | ❌ | ❌ |
| Dead letter queue | ✅ | ✅ | ❌ | ❌ | ❌ |
| Hot path latency | <500ns | ~ms | ~100ms | ~ms | ~ms |

---

## 11. Rust Crate Dependencies

| Functionality | Crate | Notes |
|---------------|-------|-------|
| Arrow types | `arrow` | Core data format |
| Parquet R/W | `parquet` | Arrow-native |
| Avro decode | `apache-avro` | Schema registry compat |
| JSON parse | `simd-json` | SIMD-accelerated parsing |
| JSONB binary | Custom | Binary JSONB format for Ring 0 |
| CSV parse | `csv` (BurntSushi) | Zero-copy where possible |
| Protobuf | `prost` | Code-gen or dynamic |
| Schema registry | `schema_registry_converter` | Confluent compat |
| Iceberg | `iceberg-rust` | Apache Iceberg Rust SDK |
| MessagePack | `rmp-serde` | Fast decode |
| SQL planning | `datafusion` | Function registration |

---

## 12. Summary

LaminarDB's schema inference and transformation system is designed around three principles:

1. **Resolve early, execute fast**: All schema resolution, inference, and decoder compilation happens in Ring 2. The hot path only sees pre-compiled Arrow schemas and zero-copy decoders.

2. **Meet developers where they are**: Support explicit schemas for production, schema registries for enterprise, and auto-inference for exploration — all through SQL DDL.

3. **PostgreSQL-compatible, streaming-extended**: JSON functions follow PostgreSQL semantics (operators, path queries, set-returning functions), extended with streaming-specific helpers like `json_flatten`, `jsonb_pick`, and format bridge functions (`from_avro`, `to_json`).
