# F-SCHEMA-007: Parquet Format Decoder

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SCHEMA-007 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-SCHEMA-001 (FormatDecoder trait) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-21 |
| **Crate** | `laminar-formats` |
| **Module** | `laminar-formats/src/parquet/decoder.rs`, `laminar-formats/src/parquet/provider.rs` |

## Summary

Implement `ParquetDecoder` as both a `FormatDecoder` and `SchemaProvider` for Apache Parquet files. Parquet is a self-describing columnar format with an exact 1:1 mapping to Arrow types, making it the highest-fidelity format in the LaminarDB ecosystem. The decoder reads Parquet pages and row groups directly into Arrow `RecordBatch` via the `parquet` crate's native Arrow integration, achieving zero-copy deserialization for most data types.

Because Parquet files carry their schema in the file footer metadata, the `ParquetDecoder` also implements `SchemaProvider` with `is_authoritative() == true`, meaning no schema inference or registry lookup is needed. The schema is read once from the file metadata at CREATE SOURCE time and frozen for the lifetime of the source.

The decoder supports projection pushdown (only decode requested columns) and row group filtering via predicate pushdown against column statistics, both of which reduce I/O and decode time substantially.

## Goals

- `ParquetDecoder` implementing `FormatDecoder` for Arrow-native Parquet decoding
- `ParquetSchemaProvider` implementing `SchemaProvider` (self-describing, authoritative)
- Arrow-native read path via `parquet` crate -- exact 1:1 Parquet-to-Arrow type mapping
- Projection pushdown: only decode columns referenced by downstream queries
- Row group filtering: skip row groups via min/max column statistics
- Page-level decoding in Ring 1 with microsecond-per-batch performance
- Schema metadata extraction: field descriptions, logical types, key-value properties
- `FormatEncoder` implementation for writing Arrow to Parquet (sinks, checkpoint files)
- Configurable batch size and read parallelism

## Non-Goals

- Parquet file discovery, listing, or glob expansion (handled by `ParquetSource` connector in F033)
- Object store integration (S3, ADLS, GCS) -- handled by the source connector
- Hive partition column injection (handled by `ParquetSource`)
- Parquet file writing lifecycle management (rollover, naming, partitioning -- handled by sink connector)
- Schema evolution across multiple Parquet files with different schemas (handled by `SchemaEvolvable` trait)
- Bloom filter pushdown (future enhancement)
- Page-level statistics filtering beyond row-group level
- Parquet encryption/decryption

## Technical Design

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    CREATE SOURCE (Ring 2)                     │
│                                                               │
│  SchemaResolver                                               │
│    ├── source.as_schema_provider() → Some(ParquetDecoder)     │
│    └── provide_schema(config) → SchemaRef                     │
│         └── Read Parquet file footer → Arrow Schema            │
│              is_authoritative() == true                        │
│              No inference needed, no registry needed           │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────┼──────────────────────────────────┐
│                    Ring 1 (Decode Path)                       │
│                          ▼                                    │
│  ParquetDecoder                                               │
│    ├── projection: [col_0, col_2, col_5]  (subset of cols)   │
│    ├── row_group_filter: price > 100.0                        │
│    │                                                          │
│    ├── Row Group 0: stats.max(price) = 50.0 → SKIP           │
│    ├── Row Group 1: stats.max(price) = 200.0 → READ          │
│    │    └── Page decode → Arrow arrays (zero-copy)            │
│    ├── Row Group 2: stats.max(price) = 150.0 → READ          │
│    │    └── Page decode → Arrow arrays (zero-copy)            │
│    └── Row Group 3: stats.max(price) = 80.0 → SKIP           │
│                          │                                    │
│                          ▼                                    │
│              RecordBatch (Arrow columnar)                      │
│              → Ring 0 hot path                                │
└─────────────────────────────────────────────────────────────┘
```

### Ring Integration

| Operation | Ring | Latency Budget | Notes |
|-----------|------|----------------|-------|
| Parquet footer read | Ring 2 | 1-10ms | One-time at CREATE SOURCE, reads file metadata |
| Schema extraction | Ring 2 | < 1ms | Parse schema from Parquet metadata |
| Row group statistics check | Ring 1 | < 1us per row group | Compare predicate against min/max stats |
| Page decode (data) | Ring 1 | 1-50us per batch | Depends on batch size and column count |
| Arrow array assembly | Ring 1 | < 5us | Columnar buffer construction |
| Projection application | Ring 1 | 0ns overhead | Column indices resolved at construction |
| **Ring 0 impact** | **Ring 0** | **0ns** | **No Ring 0 involvement -- all decode in Ring 1** |

### API Design

```rust
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReaderBuilder, RowSelection,
};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;

use crate::schema::{
    FieldMeta, FormatDecoder, FormatEncoder, RawRecord,
    SchemaError, SchemaProvider, SchemaResult, SourceConfig,
};

// ── Parquet → Arrow Type Mapping (Native 1:1) ──────────────────

// The `parquet` crate provides exact 1:1 type mapping via its
// Arrow reader. No custom mapping is needed -- the arrow-rs
// Parquet reader handles all conversions natively:
//
// | Parquet Type              | Arrow Type                    |
// |---------------------------|-------------------------------|
// | BOOLEAN                   | Boolean                       |
// | INT32                     | Int32                         |
// | INT64                     | Int64                         |
// | INT96                     | Timestamp(Nanosecond, None)   |
// | FLOAT                     | Float32                       |
// | DOUBLE                    | Float64                       |
// | BYTE_ARRAY                | Binary or Utf8 (by annotation)|
// | FIXED_LEN_BYTE_ARRAY      | FixedSizeBinary(N)            |
// | INT32 + DATE annotation   | Date32                        |
// | INT32 + TIME_MILLIS       | Time32(Millisecond)           |
// | INT64 + TIME_MICROS       | Time64(Microsecond)           |
// | INT64 + TIMESTAMP_MILLIS  | Timestamp(Millisecond, ...)   |
// | INT64 + TIMESTAMP_MICROS  | Timestamp(Microsecond, ...)   |
// | FIXED + DECIMAL           | Decimal128(p, s)              |
// | BYTE_ARRAY + STRING       | Utf8                          |
// | BYTE_ARRAY + JSON         | Utf8 (metadata: json)         |
// | Repeated group            | List                          |
// | Group                     | Struct                        |
// | Group + MAP annotation    | Map(Utf8, V)                  |

// ── Schema Provider ─────────────────────────────────────────────

/// Provides schema from Parquet file metadata.
///
/// Parquet files are self-describing: the schema is embedded in the
/// file footer along with row group statistics, column encodings,
/// and key-value metadata. This provider reads the footer once at
/// CREATE SOURCE time and returns the Arrow schema directly.
///
/// `is_authoritative()` returns `true` because the Parquet footer
/// is the canonical schema definition -- there is no possibility
/// of schema drift between the metadata and the data.
#[derive(Debug)]
pub struct ParquetSchemaProvider;

#[async_trait::async_trait]
impl SchemaProvider for ParquetSchemaProvider {
    async fn provide_schema(
        &self,
        config: &SourceConfig,
    ) -> SchemaResult<SchemaRef> {
        let path = config.get_required("path")?;
        let metadata = read_parquet_footer(path).await?;
        let schema = parquet_metadata_to_arrow_schema(&metadata)?;
        Ok(schema)
    }

    /// Parquet schema is authoritative -- the file footer is the
    /// single source of truth for the schema definition.
    fn is_authoritative(&self) -> bool {
        true
    }

    async fn field_metadata(
        &self,
        config: &SourceConfig,
    ) -> SchemaResult<HashMap<String, FieldMeta>> {
        let path = config.get_required("path")?;
        let metadata = read_parquet_footer(path).await?;
        let kv = metadata.file_metadata().key_value_metadata();

        let mut field_meta = HashMap::new();
        let schema = metadata.file_metadata().schema();

        for (idx, field) in schema.get_fields().iter().enumerate() {
            let meta = FieldMeta {
                field_id: Some(field.get_basic_info().id()),
                description: None,
                source_type: Some(format!("{:?}", field.get_type())),
                default_expr: None,
                properties: extract_field_properties(kv, idx),
            };
            field_meta.insert(
                field.get_basic_info().name().to_string(),
                meta,
            );
        }

        Ok(field_meta)
    }
}

// ── Parquet Decoder ─────────────────────────────────────────────

/// Parquet format decoder.
///
/// Decodes Parquet page data into Arrow RecordBatches using the
/// `parquet` crate's native Arrow reader. This is the most
/// efficient decode path in LaminarDB because Parquet's internal
/// columnar layout maps directly to Arrow's columnar arrays.
///
/// # Projection Pushdown
///
/// Only columns listed in `projection_columns` are decoded. This
/// is resolved at construction time into a `ProjectionMask` so
/// no column filtering happens during the decode hot path.
///
/// # Row Group Filtering
///
/// Row groups whose column statistics (min/max) do not satisfy the
/// predicate are skipped entirely. This check is O(1) per row
/// group per predicate column and avoids decoding irrelevant data.
///
/// # Zero-Copy Path
///
/// For dictionary-encoded and plain-encoded columns, the `parquet`
/// crate can produce Arrow arrays that reference the underlying
/// Parquet page buffers directly, avoiding data copies.
#[derive(Debug)]
pub struct ParquetDecoder {
    /// The output Arrow schema (may be a projection of the full
    /// file schema).
    output_schema: SchemaRef,
    /// The full file schema (before projection).
    file_schema: SchemaRef,
    /// Column indices to decode (None = all columns).
    projection_indices: Option<Vec<usize>>,
    /// Maximum number of rows per decoded batch.
    batch_size: usize,
}

/// Configuration for the Parquet decoder.
#[derive(Debug, Clone)]
pub struct ParquetDecoderConfig {
    /// Columns to decode. None means all columns.
    pub projection: Option<Vec<String>>,
    /// Maximum rows per decoded batch. Default: 8192.
    pub batch_size: usize,
}

impl Default for ParquetDecoderConfig {
    fn default() -> Self {
        Self {
            projection: None,
            batch_size: 8192,
        }
    }
}

impl ParquetDecoder {
    /// Creates a new Parquet decoder for the given file schema.
    ///
    /// If `config.projection` is set, only the named columns are
    /// included in the output schema and decoded. Column indices
    /// are resolved once at construction.
    ///
    /// # Errors
    ///
    /// Returns `SchemaError::Incompatible` if any projected column
    /// name does not exist in the file schema.
    pub fn new(
        file_schema: SchemaRef,
        config: ParquetDecoderConfig,
    ) -> SchemaResult<Self> {
        let (output_schema, projection_indices) = match &config.projection {
            Some(columns) => {
                let mut indices = Vec::with_capacity(columns.len());
                let mut fields = Vec::with_capacity(columns.len());

                for col_name in columns {
                    let (idx, field) = file_schema
                        .column_with_name(col_name)
                        .ok_or_else(|| SchemaError::Incompatible(
                            format!(
                                "column '{}' not found in Parquet schema. \
                                 Available: {:?}",
                                col_name,
                                file_schema.fields()
                                    .iter()
                                    .map(|f| f.name())
                                    .collect::<Vec<_>>(),
                            )
                        ))?;
                    indices.push(idx);
                    fields.push(field.clone());
                }

                let projected_schema = Arc::new(
                    arrow::datatypes::Schema::new(fields)
                );
                (projected_schema, Some(indices))
            }
            None => (file_schema.clone(), None),
        };

        Ok(Self {
            output_schema,
            file_schema,
            projection_indices,
            batch_size: config.batch_size,
        })
    }

    /// Builds a `ParquetRecordBatchReaderBuilder` with projection
    /// and batch size pre-configured.
    ///
    /// The caller provides the raw Parquet file bytes. The builder
    /// can optionally have row group filters applied before
    /// building the final reader.
    pub fn build_reader<R: parquet::file::reader::ChunkReader>(
        &self,
        reader: R,
    ) -> SchemaResult<ParquetRecordBatchReaderBuilder<R>> {
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(reader)
            .map_err(|e| SchemaError::DecodeError(
                format!("failed to open Parquet reader: {e}")
            ))?;

        builder = builder.with_batch_size(self.batch_size);

        if let Some(ref indices) = self.projection_indices {
            let mask = ProjectionMask::roots(
                builder.parquet_schema(),
                indices.iter().copied(),
            );
            builder = builder.with_projection(mask);
        }

        Ok(builder)
    }
}

impl FormatDecoder for ParquetDecoder {
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

        // For Parquet, each "record" is typically a complete file
        // or file segment. Concatenate all record bytes and decode
        // as a single Parquet source.
        let data: Vec<u8> = records
            .iter()
            .flat_map(|r| r.value.iter().copied())
            .collect();

        let cursor = std::io::Cursor::new(data);
        let reader = self.build_reader(cursor)?
            .build()
            .map_err(|e| SchemaError::DecodeError(
                format!("failed to build Parquet reader: {e}")
            ))?;

        let batches: Result<Vec<RecordBatch>, _> =
            reader.collect();
        let batches = batches.map_err(|e| SchemaError::DecodeError(
            format!("Parquet decode error: {e}")
        ))?;

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(
                self.output_schema.clone(),
            ));
        }

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
        "parquet"
    }
}

// ── Parquet Encoder (for sinks) ─────────────────────────────────

/// Encodes Arrow RecordBatches into Parquet format.
///
/// Used by sink connectors (file sinks, Iceberg sinks, Delta Lake
/// sinks) and for checkpoint state persistence. Supports
/// configurable compression, encoding, and write properties.
#[derive(Debug)]
pub struct ParquetEncoder {
    /// The Arrow schema to encode.
    input_schema: SchemaRef,
    /// Parquet write properties (compression, encoding, etc.).
    write_props: WriterProperties,
}

/// Configuration for Parquet encoding.
#[derive(Debug, Clone)]
pub struct ParquetEncoderConfig {
    /// Compression codec. Default: Zstd(3).
    pub compression: Compression,
    /// Target row group size in bytes. Default: 128 MB.
    pub row_group_size: usize,
    /// Target page size in bytes. Default: 1 MB.
    pub page_size: usize,
    /// Enable dictionary encoding. Default: true.
    pub dictionary_enabled: bool,
    /// Enable column statistics. Default: true.
    pub statistics_enabled: bool,
}

impl Default for ParquetEncoderConfig {
    fn default() -> Self {
        Self {
            compression: Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            row_group_size: 128 * 1024 * 1024,
            page_size: 1024 * 1024,
            dictionary_enabled: true,
            statistics_enabled: true,
        }
    }
}

impl FormatEncoder for ParquetEncoder {
    fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }

    fn encode_batch(
        &self,
        batch: &RecordBatch,
    ) -> SchemaResult<Vec<Vec<u8>>> {
        // Parquet encoding produces a single byte buffer for the
        // entire batch (not per-row). Return as a single-element vec.
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(
            &mut buf,
            batch.schema(),
            Some(self.write_props.clone()),
        ).map_err(|e| SchemaError::DecodeError(
            format!("failed to create Parquet writer: {e}")
        ))?;

        writer.write(batch).map_err(|e| SchemaError::DecodeError(
            format!("Parquet write error: {e}")
        ))?;

        writer.close().map_err(|e| SchemaError::DecodeError(
            format!("Parquet close error: {e}")
        ))?;

        Ok(vec![buf])
    }

    fn format_name(&self) -> &str {
        "parquet"
    }
}
```

### SQL Interface

```sql
-- Parquet source (schema auto-detected from file footer)
CREATE SOURCE historical_trades
FROM FILE (
    path            = 's3://data-lake/trades/*.parquet',
    watch_interval  = '10s'
)
FORMAT PARQUET;

-- Parquet with explicit projection (only decode these columns)
CREATE SOURCE sensor_data
FROM FILE (
    path    = '/warehouse/sensors/',
    columns = 'device_id, temperature, humidity'
)
FORMAT PARQUET;

-- Parquet sink
CREATE SINK archive INTO FILE (
    path            = 's3://bucket/archive/',
    file_format     = 'parquet',
    partition_by    = (year(ts), month(ts), day(ts)),
    rollover        = '128MB',
    naming          = '{partition}/{uuid}.parquet'
)
FROM processed_data;

-- Inspect Parquet schema
DESCRIBE SOURCE historical_trades;
-- Column     | Type       | Nullable | Source
-- symbol     | VARCHAR    | NO       | parquet_metadata
-- price      | DOUBLE     | NO       | parquet_metadata
-- quantity   | BIGINT     | NO       | parquet_metadata
-- ts         | TIMESTAMP  | NO       | parquet_metadata
```

### Data Structures

```rust
/// Information about a Parquet file's row group for predicate pushdown.
#[derive(Debug, Clone)]
pub struct RowGroupStats {
    /// Row group index within the file.
    pub index: usize,
    /// Number of rows in this row group.
    pub num_rows: i64,
    /// Total compressed byte size.
    pub compressed_size: i64,
    /// Per-column min/max statistics.
    pub column_stats: HashMap<String, ColumnStats>,
}

/// Column-level statistics from the Parquet row group metadata.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    /// Minimum value (as Arrow scalar).
    pub min: Option<ScalarValue>,
    /// Maximum value (as Arrow scalar).
    pub max: Option<ScalarValue>,
    /// Number of null values.
    pub null_count: i64,
    /// Number of distinct values (if available).
    pub distinct_count: Option<i64>,
}

/// Row group filter predicate for pushdown.
///
/// Applied against row group statistics to determine whether a
/// row group can be skipped entirely. This is a Ring 1 operation
/// with O(1) cost per row group per predicate column.
#[derive(Debug, Clone)]
pub enum RowGroupPredicate {
    /// Column value equals scalar.
    Eq(String, ScalarValue),
    /// Column value is greater than scalar.
    Gt(String, ScalarValue),
    /// Column value is less than scalar.
    Lt(String, ScalarValue),
    /// Column value is between two scalars (inclusive).
    Between(String, ScalarValue, ScalarValue),
    /// Logical AND of multiple predicates.
    And(Vec<RowGroupPredicate>),
    /// Logical OR of multiple predicates.
    Or(Vec<RowGroupPredicate>),
}

impl RowGroupPredicate {
    /// Evaluates this predicate against row group statistics.
    ///
    /// Returns `true` if the row group might contain matching rows
    /// (must be read), `false` if it definitely does not (can skip).
    pub fn evaluate(&self, stats: &RowGroupStats) -> bool {
        match self {
            Self::Eq(col, val) => {
                stats.column_stats.get(col).map_or(true, |s| {
                    s.min.as_ref().map_or(true, |min| val >= min)
                        && s.max.as_ref().map_or(true, |max| val <= max)
                })
            }
            Self::Gt(col, val) => {
                stats.column_stats.get(col).map_or(true, |s| {
                    s.max.as_ref().map_or(true, |max| max > val)
                })
            }
            Self::Lt(col, val) => {
                stats.column_stats.get(col).map_or(true, |s| {
                    s.min.as_ref().map_or(true, |min| min < val)
                })
            }
            Self::Between(col, lo, hi) => {
                stats.column_stats.get(col).map_or(true, |s| {
                    s.min.as_ref().map_or(true, |min| min <= hi)
                        && s.max.as_ref().map_or(true, |max| max >= lo)
                })
            }
            Self::And(preds) => {
                preds.iter().all(|p| p.evaluate(stats))
            }
            Self::Or(preds) => {
                preds.iter().any(|p| p.evaluate(stats))
            }
        }
    }
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `SchemaError::DecodeError("failed to open Parquet reader")` | File is not valid Parquet or is corrupted | Verify file format, check file path |
| `SchemaError::Incompatible("column 'X' not found")` | Projected column does not exist in file schema | Fix column name in projection list |
| `SchemaError::DecodeError("Parquet decode error")` | Data corruption within a page or row group | Skip file or row group, log error |
| `SchemaError::Arrow(...)` | Arrow array construction failed | Internal error, file a bug |
| `SchemaError::InferenceFailed("path not found")` | File path does not exist or is inaccessible | Check path, storage credentials |

## Implementation Plan

### Phase 1: Schema Provider (1 day)

- [ ] Implement `ParquetSchemaProvider` with `SchemaProvider` trait
- [ ] Implement `read_parquet_footer()` for metadata extraction
- [ ] Implement `field_metadata()` for Parquet key-value properties
- [ ] Unit tests for schema extraction from various Parquet files

### Phase 2: Core Decoder (2-3 days)

- [ ] Implement `ParquetDecoder` with `FormatDecoder` trait
- [ ] Implement `ParquetDecoderConfig` with projection and batch size
- [ ] Implement `build_reader()` with projection mask
- [ ] Implement row group predicate pushdown (`RowGroupPredicate`)
- [ ] Unit tests for all Parquet physical types
- [ ] Unit tests for projection pushdown
- [ ] Unit tests for row group statistics filtering

### Phase 3: Encoder (1-2 days)

- [ ] Implement `ParquetEncoder` with `FormatEncoder` trait
- [ ] Implement `ParquetEncoderConfig` with compression, row group size
- [ ] Round-trip tests: Arrow -> Parquet -> Arrow identity
- [ ] Benchmark encode/decode throughput

### Phase 4: Integration (1 day)

- [ ] Wire into `ParquetSource` connector (F033)
- [ ] Wire into file sink connector
- [ ] SQL integration test: CREATE SOURCE ... FORMAT PARQUET
- [ ] End-to-end test: read Parquet -> pipeline -> write Parquet

## Testing Strategy

### Unit Tests (15+)

| Module | Tests | What |
|--------|-------|------|
| `schema_provider` | 4 | Schema from simple file, nested schema, logical types, key-value metadata |
| `decoder` | 5 | All physical types, projection, batch size, empty file, nested columns |
| `predicate` | 4 | Eq, Gt, Lt, Between, And, Or against row group stats |
| `encoder` | 3 | Simple encode, compression settings, dictionary encoding |
| `roundtrip` | 3 | Primitives roundtrip, nested types roundtrip, nullable fields |

### Integration Tests

| Test | What |
|------|------|
| `test_parquet_source_schema_detection` | CREATE SOURCE auto-detects schema from file footer |
| `test_parquet_projection_pushdown` | Only requested columns are decoded (verify I/O savings) |
| `test_parquet_rowgroup_filter` | Row groups skipped when stats prove no matches |
| `test_parquet_sink_roundtrip` | Write to Parquet sink, read back, verify identity |
| `test_parquet_large_batch` | Decode 1M rows without OOM |

### Benchmarks

| Benchmark | Target | Description |
|-----------|--------|-------------|
| `bench_parquet_decode_flat` | > 2M rows/sec | 5-column flat schema, 8192-row batches |
| `bench_parquet_decode_projected` | > 3M rows/sec | 2 of 10 columns projected |
| `bench_parquet_rowgroup_filter` | < 1us per row group | Statistics check only |
| `bench_parquet_encode` | > 1M rows/sec | Zstd compression, 8192-row batches |
| `bench_parquet_schema_read` | < 5ms | Footer read for 100-column schema |

## Success Criteria

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Decode throughput | > 2M rows/sec/core | `cargo bench --bench parquet_bench` |
| Projection I/O savings | > 50% reduction | Compare projected vs full column reads |
| Row group skip rate | > 30% for selective predicates | Count skipped vs total row groups |
| Schema detection time | < 10ms | Benchmark footer read |
| Type coverage | 100% Parquet physical types | Unit test for every type |
| Round-trip fidelity | Bit-exact Arrow->Parquet->Arrow | Round-trip tests |
| Test count | 20+ tests | `cargo test` |

## Competitive Comparison

| Feature | LaminarDB | Flink | RisingWave | Materialize | DuckDB |
|---------|-----------|-------|------------|-------------|--------|
| Parquet decode path | Arrow-native (zero-copy) | Arrow via JNI | Arrow-native | No native Parquet | Arrow-native |
| Schema auto-detect | Authoritative from footer | DDL required | file_scan() | DDL required | Auto-detect |
| Projection pushdown | Column-level via ProjectionMask | Column-level | Column-level | N/A | Column-level |
| Row group filtering | Min/max statistics | Min/max stats | Min/max stats | N/A | Min/max + bloom |
| Decode latency | < 1us per page (Ring 1) | ~ms | ~100ms | N/A | ~us |
| Encoding support | Full (compression, dict, stats) | Via Flink Parquet | No | No | Full |
| Page-level filtering | Row-group level (bloom future) | Row-group level | Row-group level | N/A | Page-level |

## References

- [F-SCHEMA-001: FormatDecoder Trait](F-SCHEMA-001-format-decoder.md)
- [F033: Parquet Source Connector](../F033-parquet-source.md)
- [F-LOOKUP-008: Parquet Lookup Source](../../delta/lookup/F-LOOKUP-008-parquet-lookup-source.md)
- [Schema Inference Design Research](../../../research/schema-inference-design.md)
- [Extensible Schema Traits Research](../../../research/extensible-schema-traits.md)
- [Apache Parquet Format Specification](https://parquet.apache.org/documentation/latest/)
- [`parquet` Crate (arrow-rs)](https://crates.io/crates/parquet)
- [`arrow` Crate](https://crates.io/crates/arrow)
