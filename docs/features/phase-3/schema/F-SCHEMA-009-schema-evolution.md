# F-SCHEMA-009: Schema Evolution Engine

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SCHEMA-009 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-SCHEMA-001 (Schema Traits), F-SCHEMA-002 (Schema Resolver) |
| **Crate** | laminar-connector-api (traits + engine), laminar-connectors (impls), laminar-sql (DDL) |
| **Created** | 2026-02-21 |
| **Updated** | 2026-02-21 |

## Summary

Implement the `SchemaEvolvable` trait and a general-purpose schema evolution
engine that detects, validates, and applies schema changes to live streaming
sources without downtime. The engine computes diffs between Arrow schemas,
evaluates compatibility under configurable modes (Backward, Forward, Full, and
their transitive variants), and atomically swaps decoders in Ring 1 while the
Ring 0 hot path continues processing. Schema history is tracked in the
`laminar_catalog.schema_history` virtual table.

## Goals

- Full implementation of `SchemaEvolvable` trait: `diff_schemas()`, `evaluate_evolution()`, `apply_evolution()`, `schema_version()`
- Configurable compatibility modes: None, Backward, Forward, Full, BackwardTransitive, ForwardTransitive, FullTransitive
- Zero-downtime schema changes: Ring 2 detects and validates, Ring 1 swaps decoder atomically, Ring 0 is unaffected
- SQL DDL for schema mutations: `ALTER SOURCE ... ADD COLUMN`, `DROP COLUMN`, `ALTER COLUMN TYPE`, `REFRESH SCHEMA`
- Schema version history tracking with queryable `laminar_catalog.schema_history`
- Safe handling of all evolution cases: add nullable, add with default, drop, rename (field-ID sources only), widen, reject narrow
- `ColumnProjection` mapping from old to new schema for seamless batch translation

## Non-Goals

- Cross-source schema federation (merging schemas from multiple sources)
- Automatic data backfill of existing records when columns are added (new columns are NULL for old records)
- Schema evolution for in-memory `CREATE TABLE` sources (tables use explicit DDL only)
- Protobuf descriptor evolution (future work, requires separate descriptor diffing)
- Custom user-defined compatibility rules (only the six standard modes are supported)

## Technical Design

### Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                     Schema Evolution Flow                            │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  Ring 2: Schema Detection & Validation                         │  │
│  │                                                                 │  │
│  │  1. Detect schema change:                                       │  │
│  │     - Schema registry: new schema_id in message header          │  │
│  │     - Iceberg: catalog metadata version bump                    │  │
│  │     - User DDL: ALTER SOURCE statement                          │  │
│  │                                                                 │  │
│  │  2. diff_schemas(current, proposed) -> Vec<SchemaChange>        │  │
│  │  3. evaluate_evolution(changes, mode) -> EvolutionVerdict       │  │
│  │  4. apply_evolution(current, changes) -> (SchemaRef, Projection)│  │
│  │  5. Record version in schema_history                            │  │
│  └───────────────────────────┬────────────────────────────────────┘  │
│                              │                                       │
│                              ▼                                       │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  Ring 1: Atomic Decoder Swap                                   │  │
│  │                                                                 │  │
│  │  - Build new FormatDecoder with merged schema                   │  │
│  │  - AtomicPtr swap: old decoder -> new decoder                   │  │
│  │  - Old decoder dropped after all in-flight batches complete     │  │
│  │  - New batches: all columns present                             │  │
│  │  - In-flight batches from old schema: handled by ColumnProjection│ │
│  └───────────────────────────┬────────────────────────────────────┘  │
│                              │                                       │
│                              ▼                                       │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  Ring 0: Hot Path (unaffected)                                 │  │
│  │                                                                 │  │
│  │  - Receives Arrow RecordBatches as before                       │  │
│  │  - New columns appear as nullable fields with NULL values       │  │
│  │  - Dropped columns excluded from projection                     │  │
│  │  - Widened types auto-cast in Ring 1 before reaching Ring 0     │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### Ring Integration

| Operation | Ring | Latency Budget | Allocations |
|-----------|------|----------------|-------------|
| Detect schema change (registry fetch, catalog poll) | Ring 2 | Seconds (async I/O) | Allowed |
| `diff_schemas()` computation | Ring 2 | Microseconds (O(columns)) | Allowed |
| `evaluate_evolution()` validation | Ring 2 | Microseconds | Allowed |
| `apply_evolution()` schema merge | Ring 2 | Microseconds | Allowed (new SchemaRef) |
| Build new FormatDecoder | Ring 2 | Milliseconds (one-time) | Allowed |
| Atomic decoder swap (`Arc::swap`) | Ring 1 | Nanoseconds | None |
| ColumnProjection batch translation | Ring 1 | Microseconds (per batch) | Minimal (column reorder) |
| RecordBatch processing | Ring 0 | < 500ns (per record) | Zero |

### API Design

```rust
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, SchemaRef};
use async_trait::async_trait;

/// Describes what kind of schema change occurred between two versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaChange {
    /// A new column was added to the schema.
    ColumnAdded {
        name: String,
        data_type: DataType,
        nullable: bool,
        default: Option<String>,
    },
    /// An existing column was removed from the schema.
    ColumnDropped {
        name: String,
    },
    /// A column was renamed (only supported for field-ID-based sources).
    ColumnRenamed {
        old_name: String,
        new_name: String,
    },
    /// A column's type was widened to a compatible wider type.
    TypeWidened {
        name: String,
        from: DataType,
        to: DataType,
    },
    /// A column's type was narrowed (always rejected).
    TypeNarrowed {
        name: String,
        from: DataType,
        to: DataType,
    },
}

impl SchemaChange {
    /// Returns the column name affected by this change.
    pub fn column_name(&self) -> &str {
        match self {
            Self::ColumnAdded { name, .. }
            | Self::ColumnDropped { name }
            | Self::TypeWidened { name, .. }
            | Self::TypeNarrowed { name, .. } => name,
            Self::ColumnRenamed { old_name, .. } => old_name,
        }
    }

    /// Returns true if this change is always safe (no data loss risk).
    pub fn is_safe(&self) -> bool {
        matches!(
            self,
            Self::ColumnAdded { nullable: true, .. }
                | Self::ColumnAdded { default: Some(_), .. }
                | Self::TypeWidened { .. }
        )
    }
}

/// Result of evaluating a set of schema changes against a compatibility mode.
#[derive(Debug, Clone)]
pub enum EvolutionVerdict {
    /// All changes are compatible and can be applied automatically.
    Compatible(Vec<SchemaChange>),
    /// Changes require explicit user confirmation before application.
    /// The String describes why confirmation is needed.
    RequiresConfirmation(Vec<SchemaChange>, String),
    /// Changes are incompatible and cannot be applied.
    Incompatible(String),
}

impl EvolutionVerdict {
    /// Returns true if the verdict allows automatic application.
    pub fn is_compatible(&self) -> bool {
        matches!(self, Self::Compatible(_))
    }

    /// Returns the changes regardless of verdict.
    pub fn changes(&self) -> &[SchemaChange] {
        match self {
            Self::Compatible(c) | Self::RequiresConfirmation(c, _) => c,
            Self::Incompatible(_) => &[],
        }
    }
}

/// Maps column positions from old schema to new schema after evolution.
///
/// For each column index in the NEW schema, `mappings[i]` contains:
/// - `Some(j)` — column `i` in the new schema came from column `j` in the old schema
/// - `None` — column `i` is newly added (fill with default value or NULL)
#[derive(Debug, Clone)]
pub struct ColumnProjection {
    pub mappings: Vec<Option<usize>>,
}

impl ColumnProjection {
    /// Returns true if the projection is an identity mapping (no reordering).
    pub fn is_identity(&self) -> bool {
        self.mappings.iter().enumerate().all(|(i, m)| *m == Some(i))
    }

    /// Returns the number of newly added columns (None mappings).
    pub fn new_column_count(&self) -> usize {
        self.mappings.iter().filter(|m| m.is_none()).count()
    }

    /// Apply this projection to a RecordBatch, producing a batch with the
    /// new schema. New columns are filled with NULL arrays.
    pub fn project_batch(
        &self,
        batch: &RecordBatch,
        new_schema: &SchemaRef,
    ) -> Result<RecordBatch, arrow::error::ArrowError> {
        let mut columns = Vec::with_capacity(self.mappings.len());
        for (i, mapping) in self.mappings.iter().enumerate() {
            match mapping {
                Some(old_idx) => {
                    columns.push(batch.column(*old_idx).clone());
                }
                None => {
                    // New column: create a NULL array of the correct type
                    let field = new_schema.field(i);
                    columns.push(arrow::array::new_null_array(
                        field.data_type(),
                        batch.num_rows(),
                    ));
                }
            }
        }
        RecordBatch::try_new(new_schema.clone(), columns)
    }
}

/// Compatibility modes for schema evolution.
///
/// These follow the Confluent Schema Registry compatibility model:
/// - **Backward**: New schema can read data written by old schema
/// - **Forward**: Old schema can read data written by new schema
/// - **Full**: Both backward and forward compatible
/// - **Transitive**: Compatibility checked against ALL prior versions, not just the latest
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompatibilityMode {
    /// No compatibility checking. Any change is allowed.
    #[default]
    None,
    /// New schema can read old data (consumers upgraded first).
    Backward,
    /// Old schema can read new data (producers upgraded first).
    Forward,
    /// Both backward and forward compatible.
    Full,
    /// Backward compatible with ALL prior schema versions.
    BackwardTransitive,
    /// Forward compatible with ALL prior schema versions.
    ForwardTransitive,
    /// Both backward and forward compatible with ALL prior versions.
    FullTransitive,
}

impl CompatibilityMode {
    /// Returns true if this mode requires backward compatibility.
    pub fn requires_backward(&self) -> bool {
        matches!(
            self,
            Self::Backward | Self::Full | Self::BackwardTransitive | Self::FullTransitive
        )
    }

    /// Returns true if this mode requires forward compatibility.
    pub fn requires_forward(&self) -> bool {
        matches!(
            self,
            Self::Forward | Self::Full | Self::ForwardTransitive | Self::FullTransitive
        )
    }

    /// Returns true if compatibility must hold against all prior versions.
    pub fn is_transitive(&self) -> bool {
        matches!(
            self,
            Self::BackwardTransitive | Self::ForwardTransitive | Self::FullTransitive
        )
    }
}

/// Sources and sinks that support schema evolution.
///
/// Implementors provide the logic to diff, evaluate, and apply schema
/// changes. The `SchemaEvolutionEngine` uses these methods to drive
/// the evolution flow.
#[async_trait]
pub trait SchemaEvolvable: Send + Sync {
    /// Compute the diff between the current and proposed schema.
    ///
    /// Returns a list of individual changes. The order matters:
    /// drops are listed before adds to detect renames (when field IDs match).
    fn diff_schemas(
        &self,
        current: &SchemaRef,
        proposed: &SchemaRef,
    ) -> Vec<SchemaChange>;

    /// Evaluate whether a set of changes is compatible under the given mode.
    ///
    /// Returns a verdict that determines whether the changes can be applied
    /// automatically, require user confirmation, or must be rejected.
    fn evaluate_evolution(
        &self,
        changes: &[SchemaChange],
        compatibility: CompatibilityMode,
    ) -> EvolutionVerdict;

    /// Apply schema evolution: merge the current schema with the changes
    /// and return the new schema plus a column projection.
    ///
    /// The `ColumnProjection` maps old column positions to new positions
    /// so that in-flight batches from the old schema can be translated.
    fn apply_evolution(
        &self,
        current: &SchemaRef,
        changes: &[SchemaChange],
    ) -> Result<(SchemaRef, ColumnProjection), SchemaError>;

    /// Returns the current schema version number (monotonically increasing).
    fn schema_version(&self) -> u64;
}
```

### SQL Interface

```sql
-- Add a nullable column (backfilled with NULL for existing records)
ALTER SOURCE market_data ADD COLUMN exchange VARCHAR;

-- Add a column with a default value
ALTER SOURCE market_data ADD COLUMN currency VARCHAR DEFAULT 'USD';

-- Drop a column (excluded from future decoding)
ALTER SOURCE market_data DROP COLUMN metadata;

-- Widen a column type (safe widening only)
ALTER SOURCE market_data ALTER COLUMN quantity TYPE DOUBLE;

-- Force schema refresh from the external registry
ALTER SOURCE market_data REFRESH SCHEMA;

-- View schema history for a source
SELECT version, change_type, column_name, old_type, new_type, applied_at
FROM laminar_catalog.schema_history
WHERE source_name = 'market_data'
ORDER BY version DESC;

-- View current schema
DESCRIBE SOURCE market_data;
```

**Parser additions** in `streaming_parser.rs`:

```rust
/// ALTER SOURCE statements for schema evolution.
pub enum AlterSourceStatement {
    AddColumn {
        source_name: String,
        column_name: String,
        data_type: DataType,
        nullable: bool,
        default: Option<String>,
    },
    DropColumn {
        source_name: String,
        column_name: String,
    },
    AlterColumnType {
        source_name: String,
        column_name: String,
        new_type: DataType,
    },
    RefreshSchema {
        source_name: String,
    },
}
```

### Data Structures

```rust
use std::time::SystemTime;

/// A record in the schema history table.
#[derive(Debug, Clone)]
pub struct SchemaHistoryEntry {
    /// Source name this entry belongs to.
    pub source_name: String,
    /// Schema version number (monotonically increasing per source).
    pub version: u64,
    /// The full Arrow schema at this version.
    pub schema: SchemaRef,
    /// Individual changes from the previous version.
    pub changes: Vec<SchemaChange>,
    /// Timestamp when this version was applied.
    pub applied_at: SystemTime,
    /// How the change was triggered.
    pub trigger: EvolutionTrigger,
}

/// How a schema evolution was triggered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvolutionTrigger {
    /// User issued ALTER SOURCE DDL.
    Ddl,
    /// Schema registry detected a new schema version.
    Registry { schema_id: u32 },
    /// Iceberg catalog metadata changed.
    CatalogRefresh,
    /// User issued REFRESH SCHEMA.
    ManualRefresh,
}

/// Manages schema version history for all sources.
pub struct SchemaHistory {
    /// Per-source version history, ordered by version.
    entries: HashMap<String, Vec<SchemaHistoryEntry>>,
}

impl SchemaHistory {
    /// Record a new schema version for a source.
    pub fn record(
        &mut self,
        source_name: &str,
        schema: SchemaRef,
        changes: Vec<SchemaChange>,
        trigger: EvolutionTrigger,
    ) -> u64 {
        let entries = self.entries.entry(source_name.to_string()).or_default();
        let version = entries.last().map_or(1, |e| e.version + 1);
        entries.push(SchemaHistoryEntry {
            source_name: source_name.to_string(),
            version,
            schema,
            changes,
            applied_at: SystemTime::now(),
            trigger,
        });
        version
    }

    /// Get all versions for a source.
    pub fn versions(&self, source_name: &str) -> &[SchemaHistoryEntry] {
        self.entries.get(source_name).map_or(&[], |v| v.as_slice())
    }

    /// Get a specific version.
    pub fn get_version(
        &self,
        source_name: &str,
        version: u64,
    ) -> Option<&SchemaHistoryEntry> {
        self.entries.get(source_name)?
            .iter()
            .find(|e| e.version == version)
    }

    /// Get the latest version number for a source.
    pub fn latest_version(&self, source_name: &str) -> Option<u64> {
        self.entries.get(source_name)?.last().map(|e| e.version)
    }
}

/// The schema evolution engine that orchestrates the full evolution flow.
///
/// Used by the connector runtime when a schema change is detected.
pub struct SchemaEvolutionEngine {
    history: SchemaHistory,
    default_compatibility: CompatibilityMode,
}

impl SchemaEvolutionEngine {
    /// Create a new engine with the given default compatibility mode.
    pub fn new(default_compatibility: CompatibilityMode) -> Self {
        Self {
            history: SchemaHistory::default(),
            default_compatibility,
        }
    }

    /// Execute the full evolution flow for a source.
    ///
    /// 1. Compute diff between current and proposed schema
    /// 2. Evaluate compatibility (checking transitive history if needed)
    /// 3. Apply changes and produce new schema + projection
    /// 4. Record in schema history
    pub fn evolve(
        &mut self,
        source_name: &str,
        evolvable: &dyn SchemaEvolvable,
        current: &SchemaRef,
        proposed: &SchemaRef,
        compatibility: Option<CompatibilityMode>,
        trigger: EvolutionTrigger,
    ) -> Result<EvolutionResult, SchemaError> {
        let mode = compatibility.unwrap_or(self.default_compatibility);
        let changes = evolvable.diff_schemas(current, proposed);

        if changes.is_empty() {
            return Ok(EvolutionResult::NoChange);
        }

        // For transitive modes, validate against all prior versions
        if mode.is_transitive() {
            for entry in self.history.versions(source_name) {
                let verdict = evolvable.evaluate_evolution(&changes, mode);
                if let EvolutionVerdict::Incompatible(reason) = verdict {
                    return Err(SchemaError::EvolutionRejected(format!(
                        "incompatible with schema version {}: {reason}",
                        entry.version
                    )));
                }
            }
        }

        let verdict = evolvable.evaluate_evolution(&changes, mode);
        match verdict {
            EvolutionVerdict::Compatible(ref changes) => {
                let (new_schema, projection) =
                    evolvable.apply_evolution(current, changes)?;
                let version = self.history.record(
                    source_name,
                    new_schema.clone(),
                    changes.clone(),
                    trigger,
                );
                Ok(EvolutionResult::Applied {
                    new_schema,
                    projection,
                    version,
                    changes: changes.clone(),
                })
            }
            EvolutionVerdict::RequiresConfirmation(changes, reason) => {
                Ok(EvolutionResult::NeedsConfirmation {
                    changes,
                    reason,
                })
            }
            EvolutionVerdict::Incompatible(reason) => {
                Err(SchemaError::EvolutionRejected(reason))
            }
        }
    }
}

/// Result of an evolution attempt.
#[derive(Debug)]
pub enum EvolutionResult {
    /// No schema change detected.
    NoChange,
    /// Evolution was applied successfully.
    Applied {
        new_schema: SchemaRef,
        projection: ColumnProjection,
        version: u64,
        changes: Vec<SchemaChange>,
    },
    /// Evolution requires user confirmation before proceeding.
    NeedsConfirmation {
        changes: Vec<SchemaChange>,
        reason: String,
    },
}
```

### Schema Evolution Handling Table

| Change | Backward | Forward | Full | Hot Path Impact |
|--------|----------|---------|------|----------------|
| Add column (nullable) | Compatible | Compatible | Compatible | None -- Arrow schema extended, old records get NULL |
| Add column (with default) | Compatible | Compatible | Compatible | None -- old records get default value |
| Drop column | Incompatible | Compatible | Incompatible | None -- projection excludes column |
| Rename column (field-ID) | Compatible | Compatible | Compatible | None -- matched by field ID |
| Rename column (no field-ID) | Incompatible | Incompatible | Incompatible | N/A -- rejected |
| Widen type (e.g., Int32 to Int64) | Compatible | Incompatible | Incompatible | Minimal -- cast in Ring 1 |
| Narrow type | Always rejected | Always rejected | Always rejected | N/A |
| Reorder columns | Compatible | Compatible | Compatible | None -- Arrow uses named fields |

### Safe Type Widening Rules

```rust
/// Returns true if `from` can be safely widened to `to`.
fn is_safe_widening(from: &DataType, to: &DataType) -> bool {
    matches!(
        (from, to),
        // Integer widening
        (DataType::Int8, DataType::Int16 | DataType::Int32 | DataType::Int64)
        | (DataType::Int16, DataType::Int32 | DataType::Int64)
        | (DataType::Int32, DataType::Int64)
        | (DataType::UInt8, DataType::UInt16 | DataType::UInt32 | DataType::UInt64)
        | (DataType::UInt16, DataType::UInt32 | DataType::UInt64)
        | (DataType::UInt32, DataType::UInt64)
        // Float widening
        | (DataType::Float16, DataType::Float32 | DataType::Float64)
        | (DataType::Float32, DataType::Float64)
        // Integer to float (safe for small integers)
        | (DataType::Int8 | DataType::Int16, DataType::Float32 | DataType::Float64)
        | (DataType::Int32, DataType::Float64)
        // String widening
        | (DataType::Utf8, DataType::LargeUtf8)
        // Binary widening
        | (DataType::Binary, DataType::LargeBinary)
    )
}
```

### Live Evolution Flow (Registry-Based)

```
Time ───────────────────────────────────────────────────────▶

  Schema v1                  Schema v2 (new field added)
  ┌──────────────┐           ┌──────────────┐
  │ id: INT      │           │ id: INT      │
  │ name: UTF8   │           │ name: UTF8   │
  └──────────────┘           │ email: UTF8  │  <-- new nullable field
                             └──────────────┘

  Message arrives with schema_id=2 (Ring 1):
    1. Cache miss on schema_id=2 -> enqueue to Ring 2

  Ring 2 handles evolution:
    2. Fetch schema v2 from registry
    3. diff_schemas(v1, v2) -> [ColumnAdded { name: "email", nullable: true }]
    4. evaluate_evolution([...], Backward) -> Compatible
    5. apply_evolution(v1, [...]) -> (merged_schema, projection)
    6. Build new JsonDecoder with merged_schema
    7. Record version in schema_history

  Ring 1 decoder swap:
    8. Arc::swap(old_decoder, new_decoder)  -- atomic, ~5ns
    9. Old records (v1): email = NULL via ColumnProjection
   10. New records (v2): email = decoded value
   11. Zero downtime, zero hot-path disruption
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `SchemaError::EvolutionRejected` | Incompatible change under configured mode | User must ALTER SOURCE manually or change compatibility mode |
| `SchemaError::Incompatible` | Type narrowing detected | Cannot proceed; user must fix upstream schema |
| `ConnectorError::SchemaMismatch` | Decoded batch does not match expected schema | ColumnProjection translates batch; if projection fails, log and skip record |
| `SchemaError::RegistryError` | Schema registry unreachable during evolution | Retry with exponential backoff; continue with old schema in the interim |
| `SchemaError::InferenceFailed` | Cannot determine new schema from samples | Fall back to explicit DDL-based evolution |

## Implementation Plan

| Step | Description | Estimated Effort |
|------|-------------|-----------------|
| 1 | Implement `SchemaChange` enum, `ColumnProjection` struct, `CompatibilityMode` enum | 0.5 day |
| 2 | Implement `diff_schemas()` core algorithm (name-based + field-ID-based matching) | 1 day |
| 3 | Implement `evaluate_evolution()` with all six compatibility modes | 1 day |
| 4 | Implement `apply_evolution()` with schema merge and projection generation | 1 day |
| 5 | Implement `SchemaEvolutionEngine` orchestrator and `SchemaHistory` | 0.5 day |
| 6 | Implement atomic decoder swap in connector runtime (Ring 1 integration) | 1 day |
| 7 | Parse `ALTER SOURCE` DDL statements in `streaming_parser.rs` | 0.5 day |
| 8 | Wire `ALTER SOURCE` execution through `LaminarDB` API | 0.5 day |
| 9 | Implement `laminar_catalog.schema_history` virtual table | 0.5 day |
| 10 | Implement `SchemaEvolvable` for Kafka (via registry) and Iceberg (via catalog) | 1 day |
| 11 | Integration tests and end-to-end validation | 1 day |

## Testing Strategy

### Unit Tests

| Module | Tests | What |
|--------|-------|------|
| `diff_schemas` | 10 | Identical, add column, drop column, rename, widen, narrow, reorder, multiple changes, field-ID matching, nested struct changes |
| `evaluate_evolution` | 12 | Each compatibility mode x safe change, each mode x unsafe change, transitive validation |
| `apply_evolution` | 8 | Add nullable column, add with default, drop column, widen type, rename column, identity projection, multi-change batch |
| `column_projection` | 6 | Identity mapping, new column fill, reorder, project_batch, is_identity, empty batch |
| `schema_history` | 5 | Record version, query versions, latest version, transitive lookup, empty history |
| `safe_widening` | 8 | All integer widening paths, float widening, int-to-float, string widening, binary widening, reject narrowing |

### Integration Tests

| Test | What |
|------|------|
| `test_live_evolution_kafka_avro` | Schema registry returns new version, decoder swaps atomically, old+new records decode correctly |
| `test_alter_source_add_column` | `ALTER SOURCE ... ADD COLUMN` adds column, subsequent queries see it |
| `test_alter_source_drop_column` | `ALTER SOURCE ... DROP COLUMN` excludes column from future batches |
| `test_alter_source_widen_type` | `ALTER SOURCE ... ALTER COLUMN TYPE` widens type, values cast correctly |
| `test_refresh_schema` | `ALTER SOURCE ... REFRESH SCHEMA` fetches latest from registry |
| `test_schema_history_query` | `SELECT * FROM laminar_catalog.schema_history` returns correct entries |
| `test_incompatible_rejected` | Type narrowing under Full mode returns clear error |
| `test_transitive_compatibility` | Change compatible with latest but not v1 is rejected under FullTransitive |

### Property Tests

| Test | What |
|------|------|
| `prop_diff_then_apply_roundtrip` | For any two schemas A and B, `apply(A, diff(A, B))` produces a schema that is a superset of B |
| `prop_projection_preserves_rows` | `project_batch` always produces the same number of rows |
| `prop_compatible_changes_reversible` | Changes that are Compatible under Full mode can be applied in either direction |

## Success Criteria

| Metric | Target |
|--------|--------|
| `diff_schemas` latency | < 10us for schemas with <= 100 columns |
| Decoder swap downtime | 0 (atomic pointer swap) |
| Schema history query | < 1ms for sources with <= 1000 versions |
| All evolution handling table entries | Tested with explicit unit tests |
| ALTER SOURCE DDL | All four variants parse and execute correctly |
| Zero hot-path impact | Ring 0 latency unchanged before/after evolution (< 500ns) |

## Competitive Comparison

| Feature | LaminarDB | Flink | RisingWave | Materialize | Confluent Flink |
|---------|-----------|-------|------------|-------------|-----------------|
| Live schema evolution | Zero-downtime atomic swap | Requires job restart | Manual DDL only | Not supported | Schema registry only |
| Compatibility modes | 7 modes (None through FullTransitive) | None (manual) | None | None | 7 modes (registry) |
| ALTER SOURCE DDL | ADD, DROP, ALTER TYPE, REFRESH | Not supported | ADD COLUMN only | Not supported | Not supported |
| Schema history tracking | Built-in virtual table | Not supported | Not supported | Not supported | Registry UI only |
| Field-ID-based rename | Supported (Avro, Iceberg) | Not supported | Not supported | Not supported | Via registry |
| Type widening | Automatic with safe rules | Manual | Not supported | Not supported | Registry-level only |
| Hot path impact | Zero (Ring 2 validation, Ring 1 swap) | Full restart | Query rebuild | Not applicable | Consumer restart |

## Files

- `crates/laminar-connectors/src/schema/evolution.rs` -- NEW: `SchemaChange`, `EvolutionVerdict`, `ColumnProjection`, `SchemaEvolvable` trait
- `crates/laminar-connectors/src/schema/compatibility.rs` -- NEW: `CompatibilityMode`, evaluation logic
- `crates/laminar-connectors/src/schema/engine.rs` -- NEW: `SchemaEvolutionEngine`, `SchemaHistory`, `EvolutionResult`
- `crates/laminar-connectors/src/schema/mod.rs` -- NEW: module re-exports
- `crates/laminar-sql/src/parser/streaming_parser.rs` -- ALTER SOURCE DDL parsing
- `crates/laminar-sql/src/catalog/schema_history.rs` -- NEW: `laminar_catalog.schema_history` virtual table
- `crates/laminar-connectors/src/kafka/source.rs` -- `SchemaEvolvable` impl for Kafka (registry-based)
- `crates/laminar-connectors/src/lakehouse/iceberg.rs` -- `SchemaEvolvable` impl for Iceberg (catalog-based)
- `crates/laminar-db/src/db.rs` -- Wire ALTER SOURCE execution

## References

- [extensible-schema-traits.md](../../../research/extensible-schema-traits.md) -- Section 2.5: `SchemaEvolvable` trait design
- [schema-inference-design.md](../../../research/schema-inference-design.md) -- Section 5: Schema Evolution
- [F031D: Delta Lake Schema Evolution](../F031D-delta-lake-schema-evolution.md) -- Delta-specific evolution (complementary)
- [F-CONN-003: Avro Hardening](../F-CONN-003-avro-hardening.md) -- Schema registry compatibility checks
- [Confluent Schema Registry Compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html)
- [Apache Iceberg Schema Evolution](https://iceberg.apache.org/docs/latest/evolution/)
