# F-SCHEMA-002: Schema Resolver & Merge Engine

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SCHEMA-002 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-SCHEMA-001 (Core Schema Traits) |
| **Blocks** | F-SCHEMA-003 (Format Inference Registry), F-SCHEMA-004 (Schema Evolution) |
| **Owner** | TBD |
| **Created** | 2026-02-21 |
| **Crate** | `laminar-connectors` |
| **Module** | `laminar-connectors/src/schema/resolver.rs`, `laminar-connectors/src/schema/merge.rs` |

## Summary

Implement the `SchemaResolver` orchestrator that determines the final frozen Arrow schema for every `CREATE SOURCE` statement. The resolver implements a five-level priority chain: explicit DDL, schema registry, self-describing source metadata, sample-based inference, and error. When the user provides partial declarations with wildcard hints, the resolver merges user-declared columns with auto-resolved columns to produce the complete schema. The output is a `ResolvedSchema` containing the frozen `Arc<Schema>`, per-field provenance tracking, and any inference warnings.

This is the central coordination point between the schema traits defined in F-SCHEMA-001, the format inference registry (F-SCHEMA-003), and the SQL DDL parser. It runs entirely in Ring 2 (one-time setup) and produces the immutable `Arc<Schema>` that Ring 1 decoders consume on the hot path.

## Goals

- Implement `SchemaResolver::resolve()` with the five-level priority chain
- Implement `merge_with_declared()` for combining user DDL with auto-resolved schemas
- Support wildcard hints (`*` in DDL) with optional prefix for inferred fields
- Track per-field provenance via `FieldOrigin` (UserDeclared, AutoResolved, WildcardInferred, DefaultAdded)
- Produce `ResolvedSchema` with schema, resolution kind, field origins, and warnings
- Wire into the `CREATE SOURCE` DDL execution path
- Support `ALTER SOURCE ADD COLUMN`, `ALTER SOURCE REFRESH SCHEMA` DDL
- Validate type compatibility between declared and resolved schemas
- Emit clear diagnostic warnings when inference falls back or types conflict

## Non-Goals

- Schema evolution at runtime (F-SCHEMA-004)
- Format-specific inference logic (F-SCHEMA-003)
- Schema registry client implementation (existing in `kafka/schema_registry.rs`)
- Hot path decoding (Ring 1 decoders are built from the frozen output)
- Dynamic schema modification after source creation

## Technical Design

### Priority Chain

The resolver attempts schema strategies in strict priority order. The first strategy that succeeds determines the resolution kind.

```
Priority 1: Explicit DDL (fully declared, no wildcard)
    |
    | (not fully declared)
    v
Priority 2: Schema Registry (source is SchemaRegistryAware + registry config provided)
    |
    | (no registry)
    v
Priority 3: Self-Describing Source (source is SchemaProvider)
    |
    | (not self-describing)
    v
Priority 4: Sample Inference (source is SchemaInferable)
    |
    | (not inferable)
    v
Priority 5: Error ("No schema provided and source does not support inference")
```

### Core Types

```rust
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::schema::traits::{
    InferenceConfig, InferenceWarning, RegistryConfig, SchemaError,
    SchemaResult, SourceConfig,
};

/// User-provided schema declaration from DDL.
///
/// Parsed from `CREATE SOURCE name (col1 TYPE, col2 TYPE, ...)`.
/// When `has_wildcard` is true, the DDL contained `*` indicating
/// that remaining fields should be auto-resolved.
#[derive(Debug, Clone)]
pub struct DeclaredSchema {
    /// Explicitly declared columns with types.
    pub columns: Vec<DeclaredColumn>,
    /// Whether a wildcard (`*`) was used (infer remaining fields).
    pub has_wildcard: bool,
    /// Prefix for wildcard-inferred fields (e.g., `"extra_"`).
    pub wildcard_prefix: Option<String>,
}

/// A single column declared in DDL.
#[derive(Debug, Clone)]
pub struct DeclaredColumn {
    /// Column name as written in DDL.
    pub name: String,
    /// Arrow data type parsed from the SQL type.
    pub data_type: DataType,
    /// Whether the column allows NULLs. Defaults to true unless NOT NULL.
    pub nullable: bool,
    /// Optional default value expression (SQL string).
    pub default: Option<String>,
}

/// The result of schema resolution.
///
/// Contains the frozen Arrow schema along with metadata about how
/// each field was resolved. This is the output consumed by
/// `FormatDecoder` construction in Ring 1.
#[derive(Debug, Clone)]
pub struct ResolvedSchema {
    /// The final Arrow schema. Frozen as `Arc<Schema>` for Ring 1.
    pub schema: SchemaRef,
    /// How the schema was resolved.
    pub resolution: ResolutionKind,
    /// Per-field provenance tracking.
    pub field_origins: HashMap<String, FieldOrigin>,
    /// Warnings generated during resolution (type conflicts, low confidence, etc.).
    pub warnings: Vec<InferenceWarning>,
}

/// Describes how the schema was resolved.
#[derive(Debug, Clone)]
pub enum ResolutionKind {
    /// All columns were explicitly declared in DDL.
    Declared,
    /// Schema came from the source itself (Parquet metadata, Iceberg catalog).
    SourceProvided,
    /// Schema fetched from an external schema registry.
    Registry {
        /// The registry-assigned schema ID.
        schema_id: u32,
    },
    /// Schema inferred from data samples.
    Inferred {
        /// Number of samples used for inference.
        sample_count: usize,
        /// Warnings from the inference process.
        warnings: Vec<InferenceWarning>,
    },
}

/// Tracks where each field in the resolved schema came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldOrigin {
    /// Field was explicitly declared in DDL with a specific type.
    UserDeclared,
    /// Field was auto-resolved from source/registry/inference (no DDL).
    AutoResolved,
    /// Field was added by wildcard expansion (DDL had `*`).
    WildcardInferred,
    /// Field was added because it had a DEFAULT but was missing from source.
    DefaultAdded,
}
```

### Schema Resolver

```rust
use std::collections::HashSet;

use crate::connector::SourceConnector;

/// The main schema resolution orchestrator.
///
/// Given a source's capabilities and user's DDL, resolves the final
/// Arrow schema. This is a Ring 2 operation — it may perform I/O
/// (registry fetch, sample collection) and is called exactly once
/// at `CREATE SOURCE` time.
pub struct SchemaResolver;

impl SchemaResolver {
    /// Resolve the schema for a source using the best available strategy.
    ///
    /// # Priority Order
    ///
    /// 1. If user provided a full explicit schema (no wildcard) -> use it
    /// 2. If source is `SchemaRegistryAware` and registry config provided -> fetch
    /// 3. If source is `SchemaProvider` (self-describing) -> use metadata
    /// 4. If source is `SchemaInferable` -> sample and infer
    /// 5. Otherwise -> error
    ///
    /// When the user provides partial declarations with a wildcard,
    /// the resolved schema from steps 2-4 is merged with user declarations.
    ///
    /// # Errors
    ///
    /// Returns `SchemaError::InferenceFailed` if no resolution strategy
    /// is available for the source.
    pub async fn resolve(
        &self,
        declared: Option<&DeclaredSchema>,
        source: &dyn SourceConnector,
        source_config: &SourceConfig,
        format: &str,
        inference_config: Option<&InferenceConfig>,
        registry_config: Option<&RegistryConfig>,
    ) -> SchemaResult<ResolvedSchema> {
        // Case 1: Fully declared, no wildcard
        if let Some(decl) = declared {
            if !decl.has_wildcard {
                return Ok(ResolvedSchema {
                    schema: declared_to_arrow(decl)?,
                    resolution: ResolutionKind::Declared,
                    field_origins: decl
                        .columns
                        .iter()
                        .map(|c| (c.name.clone(), FieldOrigin::UserDeclared))
                        .collect(),
                    warnings: vec![],
                });
            }
        }

        // Case 2: Registry available
        if let (Some(reg_config), Some(registry_source)) = (
            registry_config,
            source.as_schema_registry_aware(),
        ) {
            let registered = registry_source.fetch_schema(reg_config).await?;
            return self.merge_with_declared(
                declared,
                registered.schema.clone(),
                ResolutionKind::Registry {
                    schema_id: registered.schema_id,
                },
            );
        }

        // Case 3: Self-describing source
        if let Some(provider) = source.as_schema_provider() {
            let provided = provider.provide_schema(source_config).await?;
            return self.merge_with_declared(
                declared,
                provided,
                ResolutionKind::SourceProvided,
            );
        }

        // Case 4: Sample-based inference
        if let Some(inferable) = source.as_schema_inferable() {
            let inf_config = inference_config
                .cloned()
                .unwrap_or_default();
            let samples = inferable
                .sample_records(source_config, &inf_config)
                .await?;
            let inferred = inferable
                .infer_from_samples(&samples, format, &inf_config)
                .await?;
            return self.merge_with_declared(
                declared,
                inferred.schema.clone(),
                ResolutionKind::Inferred {
                    sample_count: inferred.sample_count,
                    warnings: inferred.warnings,
                },
            );
        }

        // No strategy available
        Err(SchemaError::InferenceFailed(
            "No schema provided and source does not support inference. \
             Please declare an explicit schema in CREATE SOURCE."
                .into(),
        ))
    }
}
```

### Merge Algorithm

The merge algorithm handles the case where the user provides partial DDL with a wildcard (`*`). User-declared columns always take priority. Auto-resolved columns that do not conflict with declared names are appended, optionally prefixed.

```rust
impl SchemaResolver {
    /// Merge user-declared columns with an auto-resolved schema.
    ///
    /// User declarations always take priority. When a wildcard is present,
    /// auto-resolved fields that do not conflict with declared column names
    /// are appended with an optional prefix.
    ///
    /// # Algorithm
    ///
    /// 1. Add all explicitly declared columns first (preserving DDL order)
    /// 2. If wildcard is present, iterate auto-resolved fields:
    ///    a. Skip fields whose name matches a declared column
    ///    b. Apply wildcard_prefix to remaining field names
    ///    c. Mark all wildcard-added fields as nullable
    /// 3. Build the final Arrow Schema from the merged field list
    fn merge_with_declared(
        &self,
        declared: Option<&DeclaredSchema>,
        resolved: SchemaRef,
        resolution_kind: ResolutionKind,
    ) -> SchemaResult<ResolvedSchema> {
        let Some(decl) = declared else {
            // No user declarations -- use resolved as-is
            return Ok(ResolvedSchema {
                field_origins: resolved
                    .fields()
                    .iter()
                    .map(|f| (f.name().clone(), FieldOrigin::AutoResolved))
                    .collect(),
                schema: resolved,
                resolution: resolution_kind,
                warnings: vec![],
            });
        };

        let mut fields = Vec::new();
        let mut origins = HashMap::new();
        let mut warnings = Vec::new();

        // Step 1: Add all explicitly declared columns
        for col in &decl.columns {
            fields.push(Field::new(&col.name, col.data_type.clone(), col.nullable));
            origins.insert(col.name.clone(), FieldOrigin::UserDeclared);
        }

        // Step 2: If wildcard, add non-conflicting resolved columns
        if decl.has_wildcard {
            let prefix = decl.wildcard_prefix.as_deref().unwrap_or("");
            let declared_names: HashSet<&str> =
                decl.columns.iter().map(|c| c.name.as_str()).collect();

            for field in resolved.fields() {
                if !declared_names.contains(field.name().as_str()) {
                    let name = format!("{prefix}{}", field.name());

                    // Check for name collision after prefixing
                    if origins.contains_key(&name) {
                        warnings.push(InferenceWarning {
                            field_name: name.clone(),
                            message: format!(
                                "Wildcard field '{}' (prefixed from '{}') \
                                 collides with a declared column; skipping",
                                name,
                                field.name()
                            ),
                            severity: WarningSeverity::Warning,
                        });
                        continue;
                    }

                    // Wildcard-inferred fields are always nullable
                    fields.push(Field::new(
                        &name,
                        field.data_type().clone(),
                        true,
                    ));
                    origins.insert(name, FieldOrigin::WildcardInferred);
                }
            }
        }

        Ok(ResolvedSchema {
            schema: Arc::new(Schema::new(fields)),
            resolution: resolution_kind,
            field_origins: origins,
            warnings,
        })
    }
}

/// Convert a `DeclaredSchema` to an Arrow `SchemaRef`.
fn declared_to_arrow(decl: &DeclaredSchema) -> SchemaResult<SchemaRef> {
    let fields: Vec<Field> = decl
        .columns
        .iter()
        .map(|c| Field::new(&c.name, c.data_type.clone(), c.nullable))
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}
```

### The Full CREATE SOURCE Flow

```
User SQL:  CREATE SOURCE trades (...) FROM KAFKA (...) FORMAT JSON INFER SCHEMA (...)
                    |                       |               |           |
                    v                       v               v           v
             DeclaredSchema          ConnectorRegistry  FormatName  InferenceConfig
             (user columns)          .create_source()
                    |                       |
                    |                       v
                    |               KafkaSource (Box<dyn SourceConnector>)
                    |                       |
                    |         +-------------+--------------------+
                    |         | capability discovery              |
                    |         v                                   v
                    |   as_schema_inferable() -> Some       as_schema_registry_aware() -> Some
                    |         |                                   |
                    v         v                                   |
            +-------------------------------+                    |
            |     SchemaResolver            |                    |
            |                               |                    |
            |  1. Has full DDL? -> use it   |                    |
            |  2. Has registry? -> fetch    |<-------------------+
            |  3. Self-describing? -> N/A   |
            |  4. Inferable? -> sample      |
            |  5. Merge with hints          |
            +---------------+---------------+
                            |
                            v
                      ResolvedSchema {
                        schema: Arc<Schema>,
                        resolution: Inferred { ... },
                        field_origins: { symbol: Declared, price: Declared, ... }
                      }
                            |
                            v
                  source.build_decoder(schema, "json", config)
                            |
                            v
                  JsonDecoder (Box<dyn FormatDecoder>)
                            |
                            v
                  Frozen in Ring 1 -- hot path ready
```

### Capability Discovery on SourceConnector

The resolver discovers what a source can do via the `as_*` downcast methods on `SourceConnector`. These return `Option` references -- `None` means the source does not support that capability.

```rust
// On SourceConnector trait (defined in F-SCHEMA-001):
fn as_schema_provider(&self) -> Option<&dyn SchemaProvider> { None }
fn as_schema_inferable(&self) -> Option<&dyn SchemaInferable> { None }
fn as_schema_registry_aware(&self) -> Option<&dyn SchemaRegistryAware> { None }
fn as_schema_evolvable(&self) -> Option<&dyn SchemaEvolvable> { None }
```

Each source implements only the capabilities relevant to it:

| Source | SchemaProvider | SchemaInferable | SchemaRegistryAware |
|--------|:-:|:-:|:-:|
| Kafka (JSON) | - | Yes | - |
| Kafka (Avro) | - | - | Yes |
| WebSocket | - | Yes | - |
| File (Parquet) | Yes | Yes (auto) | - |
| Iceberg | Yes | - | - |
| Postgres CDC | Yes | - | - |

## SQL Interface

### Mode 1: Explicit Schema (No Resolver Needed)

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

Resolution: `ResolutionKind::Declared`. The resolver short-circuits at priority 1 -- no I/O, no sampling.

### Mode 2: Schema Registry (Resolver Fetches from Registry)

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
    subject     = 'trades-value',
    credentials = SECRET sr_credentials,
    evolution   = 'forward_compatible'
);
```

Resolution: `ResolutionKind::Registry { schema_id }`. The resolver calls `as_schema_registry_aware()` which returns `Some` for Kafka+Avro, then fetches the latest schema from the registry.

### Mode 3: Auto-Inference with Wildcard Hints

```sql
CREATE SOURCE sensor_data (
    device_id   VARCHAR NOT NULL,
    temperature DOUBLE,
    *
) FROM KAFKA (
    brokers = 'localhost:9092',
    topic   = 'sensors'
)
FORMAT JSON
INFER SCHEMA (
    sample_size     = 500,
    sample_timeout  = '5s',
    null_as         = 'VARCHAR',
    number_as       = 'DOUBLE',
    wildcard_prefix = 'extra_'
);
```

Resolution: `ResolutionKind::Inferred { sample_count: 500, ... }`. The resolver:
1. Detects `has_wildcard = true`, so it cannot short-circuit at priority 1
2. Falls through to priority 4 (Kafka JSON is `SchemaInferable`)
3. Samples 500 messages, infers schema
4. Merges: `device_id` and `temperature` from DDL, all other fields prefixed with `extra_`

### Schema Mutation DDL

```sql
-- Add a column with a default value.
-- The resolver validates the column does not conflict with existing names.
ALTER SOURCE market_data ADD COLUMN exchange VARCHAR DEFAULT 'UNKNOWN';

-- Refresh schema from registry or re-infer from samples.
-- Only valid for sources that were created with registry or inference mode.
ALTER SOURCE market_data REFRESH SCHEMA;

-- View the resolved schema with provenance information.
DESCRIBE SOURCE market_data;
-- +-------------+------------+----------+-----------+
-- | Column      | Type       | Nullable | Origin    |
-- +-------------+------------+----------+-----------+
-- | device_id   | VARCHAR    | NO       | declared  |
-- | temperature | DOUBLE     | YES      | declared  |
-- | extra_humid | DOUBLE     | YES      | inferred  |
-- | extra_ts    | TIMESTAMP  | YES      | inferred  |
-- +-------------+------------+----------+-----------+
```

### ALTER SOURCE ADD COLUMN

When the user adds a column via `ALTER SOURCE`, the resolver:
1. Validates the column name does not collide with existing fields
2. Appends the field to the schema with `FieldOrigin::DefaultAdded`
3. Atomically swaps the decoder's schema in Ring 1 (new field filled with default/NULL)

```rust
impl SchemaResolver {
    /// Add a column to an existing resolved schema.
    ///
    /// Returns the updated `ResolvedSchema` with the new field appended.
    /// The caller is responsible for atomically swapping the decoder.
    pub fn add_column(
        &self,
        current: &ResolvedSchema,
        column: &DeclaredColumn,
    ) -> SchemaResult<ResolvedSchema> {
        // Validate no name collision
        if current.schema.field_with_name(&column.name).is_ok() {
            return Err(SchemaError::Incompatible(format!(
                "Column '{}' already exists in source schema",
                column.name
            )));
        }

        let mut fields: Vec<Field> = current
            .schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new(
            &column.name,
            column.data_type.clone(),
            column.nullable,
        ));

        let mut origins = current.field_origins.clone();
        origins.insert(column.name.clone(), FieldOrigin::DefaultAdded);

        Ok(ResolvedSchema {
            schema: Arc::new(Schema::new(fields)),
            resolution: current.resolution.clone(),
            field_origins: origins,
            warnings: current.warnings.clone(),
        })
    }
}
```

### ALTER SOURCE REFRESH SCHEMA

Re-runs the original resolution strategy and merges the result. Only valid for sources that were created with registry or inference mode.

```rust
impl SchemaResolver {
    /// Refresh the schema for an existing source.
    ///
    /// Re-runs the original resolution strategy (registry fetch or
    /// sample inference) and merges the result with the current schema.
    /// User-declared columns are preserved; new auto-resolved columns
    /// are appended.
    ///
    /// # Errors
    ///
    /// Returns `SchemaError::Incompatible` if the source was created
    /// with `ResolutionKind::Declared` (explicit DDL cannot be refreshed).
    pub async fn refresh(
        &self,
        current: &ResolvedSchema,
        original_declared: Option<&DeclaredSchema>,
        source: &dyn SourceConnector,
        source_config: &SourceConfig,
        format: &str,
        inference_config: Option<&InferenceConfig>,
        registry_config: Option<&RegistryConfig>,
    ) -> SchemaResult<ResolvedSchema> {
        match &current.resolution {
            ResolutionKind::Declared => {
                Err(SchemaError::Incompatible(
                    "Cannot refresh schema for a source with explicit DDL. \
                     Use ALTER SOURCE ADD COLUMN instead."
                        .into(),
                ))
            }
            _ => {
                self.resolve(
                    original_declared,
                    source,
                    source_config,
                    format,
                    inference_config,
                    registry_config,
                )
                .await
            }
        }
    }
}
```

### Ring Integration

All schema resolution is Ring 2 (one-time setup at `CREATE SOURCE` / `ALTER SOURCE`):

| Operation | Ring | Latency Budget | Frequency |
|-----------|------|----------------|-----------|
| `SchemaResolver::resolve()` | Ring 2 | Seconds (async I/O) | Once per CREATE SOURCE |
| `SchemaResolver::add_column()` | Ring 2 | Microseconds (compute only) | Once per ALTER SOURCE |
| `SchemaResolver::refresh()` | Ring 2 | Seconds (re-fetch/re-sample) | Once per ALTER REFRESH |
| Decoder using frozen `Arc<Schema>` | Ring 1 | < 1us (schema is pre-baked) | Every batch |
| Arrow RecordBatch on hot path | Ring 0 | < 500ns | Every record |

The `ResolvedSchema.schema` is wrapped in `Arc<Schema>` and cloned into the `FormatDecoder` at construction time. The decoder never performs schema lookups on the hot path.

### Module Structure

```
crates/laminar-connectors/src/schema/
    mod.rs           # Public API re-exports
    resolver.rs      # SchemaResolver, resolve(), refresh()
    merge.rs         # merge_with_declared(), declared_to_arrow()
    types.rs         # DeclaredSchema, DeclaredColumn, ResolvedSchema,
                     #   ResolutionKind, FieldOrigin
```

## Implementation Plan

### Step 1: Core Types (1 day)

- Define `DeclaredSchema`, `DeclaredColumn`, `ResolvedSchema`, `ResolutionKind`, `FieldOrigin` in `schema/types.rs`
- Add `schema` submodule to `laminar-connectors/src/lib.rs`
- Unit tests for type construction and `declared_to_arrow()`

### Step 2: Merge Algorithm (1-2 days)

- Implement `merge_with_declared()` in `schema/merge.rs`
- Handle wildcard expansion with prefix
- Handle name collision detection and warnings
- Unit tests for all merge scenarios (no declared, full declared, wildcard, prefix, collisions)

### Step 3: Resolver Core (2 days)

- Implement `SchemaResolver::resolve()` in `schema/resolver.rs`
- Wire capability discovery via `as_schema_provider()`, `as_schema_inferable()`, `as_schema_registry_aware()`
- Implement priority chain with early return
- Unit tests with mock sources implementing different trait combinations

### Step 4: DDL Integration (1-2 days)

- Parse `INFER SCHEMA (...)` clause in `streaming_parser.rs`
- Parse `ALTER SOURCE ADD COLUMN`, `ALTER SOURCE REFRESH SCHEMA`
- Wire parser output to `SchemaResolver` in `db.rs`
- Implement `add_column()` and `refresh()` methods
- Integration tests with end-to-end DDL execution

### Step 5: DESCRIBE Enhancement (0.5 day)

- Extend `DESCRIBE SOURCE` output to include the `Origin` column
- Map `FieldOrigin` variants to human-readable strings

## Testing Strategy

### Unit Tests (25+)

| Category | Tests | Description |
|----------|-------|-------------|
| Types | 4 | `DeclaredSchema` construction, `declared_to_arrow()`, empty schema, nullable defaults |
| Merge (no wildcard) | 4 | No declared schema, full declared (no merge), declared subset with no wildcard |
| Merge (wildcard) | 6 | Wildcard no prefix, wildcard with prefix, prefix collision, empty resolved, all fields declared (wildcard adds nothing), mixed types |
| Priority chain | 5 | Explicit DDL short-circuit, registry beats provider, provider beats inference, inference fallback, no strategy error |
| Add column | 3 | Successful add, name collision error, add with default |
| Refresh | 3 | Refresh on inferred source, refresh on registry source, refresh on declared source (error) |

### Mock Sources for Testing

```rust
/// Mock source that implements no schema capabilities.
struct NoCapabilitySource;
impl SourceConnector for NoCapabilitySource { ... }

/// Mock source that only provides self-describing schema.
struct SelfDescribingSource { schema: SchemaRef }
impl SourceConnector for SelfDescribingSource { ... }
impl SchemaProvider for SelfDescribingSource { ... }

/// Mock source that only supports inference.
struct InferableSource { inferred: InferredSchema }
impl SourceConnector for InferableSource { ... }
impl SchemaInferable for InferableSource { ... }

/// Mock source that supports both registry and inference.
struct FullCapabilitySource { ... }
impl SourceConnector for FullCapabilitySource { ... }
impl SchemaRegistryAware for FullCapabilitySource { ... }
impl SchemaInferable for FullCapabilitySource { ... }
```

### Integration Tests (5+)

| Test | Description |
|------|-------------|
| `test_create_source_explicit_ddl` | Full DDL, resolver short-circuits, decoder built |
| `test_create_source_infer_json` | `INFER SCHEMA` clause, samples collected, schema frozen |
| `test_create_source_wildcard_merge` | Partial DDL + `*`, merge produces combined schema |
| `test_alter_source_add_column` | Add nullable column, DESCRIBE shows new field |
| `test_alter_source_refresh` | Re-infer after source data changed |

### Property-Based Tests

- Merge is idempotent: merging a schema with itself produces the same schema
- User-declared columns are always present in the output
- Wildcard-inferred fields never overwrite user-declared fields
- Field count in output >= field count in declared schema

## Success Criteria

- `SchemaResolver::resolve()` correctly prioritizes all five strategies
- Merge algorithm handles wildcard expansion with prefix without name collisions
- `DESCRIBE SOURCE` shows per-field provenance (declared vs. inferred vs. wildcard)
- `ALTER SOURCE ADD COLUMN` adds fields without disrupting active decoders
- `ALTER SOURCE REFRESH SCHEMA` re-resolves for registry/inference sources
- Explicit DDL resolution has zero I/O (sub-microsecond)
- All resolution runs in Ring 2; no schema lookups on Ring 0/Ring 1 hot path
- 30+ tests passing across unit and integration suites

## Competitive Comparison

| Feature | LaminarDB | Flink | RisingWave | Materialize | ClickHouse |
|---------|-----------|-------|------------|-------------|------------|
| Explicit DDL | Yes | Yes | Yes | Yes | Yes |
| Schema registry | Yes | Yes (Confluent) | Yes | Yes | Yes (Avro) |
| Auto-inference from samples | Yes | No | No | No | File only |
| Wildcard hints (`*`) | Yes | No | No | No | No |
| Wildcard prefix | Yes | N/A | N/A | N/A | N/A |
| Per-field provenance tracking | Yes | No | No | No | No |
| ALTER REFRESH SCHEMA | Yes | No | No | No | No |
| DESCRIBE shows origin | Yes | No | No | No | No |
| Priority chain (multi-strategy) | Yes | No | No | No | No |

LaminarDB is unique in supporting wildcard hints with prefix and per-field provenance tracking. No other streaming database allows partial DDL with auto-inference for remaining fields. The five-level priority chain is also novel -- other systems require the user to choose exactly one strategy.

## References

- [Extensible Schema Framework](../../../research/extensible-schema-traits.md) -- Sections 3, 4, 9
- [Schema Inference Design](../../../research/schema-inference-design.md) -- Sections 3, 5, 7
- F-SCHEMA-001 (Core Schema Traits) -- Trait definitions consumed by the resolver
- F-SCHEMA-003 (Format Inference Registry) -- Pluggable inference called in priority 4

## Files

- `crates/laminar-connectors/src/schema/mod.rs` -- Module root, re-exports
- `crates/laminar-connectors/src/schema/types.rs` -- Core types
- `crates/laminar-connectors/src/schema/resolver.rs` -- SchemaResolver, resolve(), refresh(), add_column()
- `crates/laminar-connectors/src/schema/merge.rs` -- merge_with_declared(), declared_to_arrow()
- `crates/laminar-sql/src/parser/streaming_parser.rs` -- INFER SCHEMA, ALTER SOURCE parsing
- `crates/laminar-db/src/db.rs` -- Wire resolver into CREATE/ALTER SOURCE execution
