# F-IDX-001: redb Secondary Indexes

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-IDX-001 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 6a |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-STATE-001 (StateStore trait), F-LOOKUP-010 (Remove RocksDB) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-17 |
| **Crate** | `laminar-core`, `laminar-db` |
| **Module** | `laminar-core/src/index/redb.rs`, `laminar-db/src/table_provider.rs` |

## Summary

Use `redb` (pure-Rust embedded B-tree database) for secondary indexes that accelerate ad-hoc `SELECT` queries on streaming sources and materialized views. Indexes are **automatically derived from pipeline definitions** — no `CREATE INDEX` SQL is needed. When a pipeline declares `GROUP BY sensor_id`, a redb table keyed by `sensor_id` is created. Ad-hoc queries hitting the indexed column use a `RedbTableProvider` (DataFusion `TableProvider`) for B-tree lookups instead of full table scans.

redb replaces the secondary index role previously filled by RocksDB (F-CONN-002D). Unlike RocksDB, redb is pure Rust (no C++ FFI, no cmake), ACID-compliant with crash-safe writes, and has a simple single-writer/multi-reader model that fits LaminarDB's thread-per-core architecture.

## Goals

- Automatic secondary index creation from pipeline `GROUP BY` / `JOIN ON` keys
- `RedbTableProvider` implementing DataFusion `TableProvider` for indexed lookups
- redb B-tree tables keyed by pipeline key columns
- Point lookup and range scan support via redb `ReadOnlyTable`
- Crash-safe writes using redb's MVCC + WAL
- Index updates on the push path (Ring 1, not Ring 0)
- Per-source/view index isolation (separate redb tables per logical entity)
- Index lifecycle tied to source/view lifecycle (`DROP SOURCE` removes indexes)

## Non-Goals

- User-facing `CREATE INDEX` / `DROP INDEX` SQL (indexes are automatic)
- Composite/multi-column indexes (single key column per index initially)
- Index-only scans (always fetch full row from primary storage)
- Covering indexes
- Index advisor / statistics-based selection
- Distributed index replication (deferred to constellation tier)

## Technical Design

### Architecture

**Ring**: Ring 1 (index writes are background, not on the Ring 0 hot path). Index reads (ad-hoc queries) are Ring 2 (control plane).

**Crate**: `laminar-core` for the index engine, `laminar-db` for DataFusion integration.

### redb Table Layout

Each logical index maps to one redb table within a shared redb `Database` file:

```
redb Database: laminardb_indexes.redb
├── Table "source:sensors:sensor_id"  → (sensor_id: &[u8]) → (row_offsets: Vec<u64>)
├── Table "view:avg_temp:sensor_id"   → (sensor_id: &[u8]) → (serialized_row: &[u8])
└── Table "source:orders:customer_id" → (customer_id: &[u8]) → (row_offsets: Vec<u64>)
```

Key encoding: Arrow column values are serialized using **memcomparable encoding** — a byte encoding that preserves sort order under lexicographic byte comparison. This is the same approach used by FoundationDB, CockroachDB, and TiKV. redb's B-tree sorts keys lexicographically, so memcomparable keys produce correct ordering for all data types.

> **AUDIT FIX (C2):** The original design used `rkyv` for key encoding, which is **incorrect** — rkyv uses little-endian native byte order. For example, `u32` value `256` serializes as `[0x00, 0x01, 0x00, 0x00]` in little-endian, which sorts BEFORE `1` (`[0x01, 0x00, ...]`) lexicographically. Memcomparable encoding guarantees correct sort order for all types.

### Automatic Index Derivation

When a pipeline is registered, the SQL planner inspects:
1. `GROUP BY` columns → create index keyed by grouping column
2. `JOIN ON` predicates → create index keyed by join key column
3. `WHERE` equality predicates on streaming scans → create index keyed by filter column

No user configuration needed. The `PipelineAnalyzer` extracts key columns and registers them with the `IndexManager`.

### Index Update Path

Index updates happen on the push path, after the snapshot buffer write but before any pipeline processing:

```
push_and_buffer(batch)
  → SPSC channel push (Ring 0, existing)
  → snapshot buffer append (Ring 0, existing)
  → index_manager.update(source_id, batch) (Ring 1, new — spawned to background)
```

The index update is **asynchronous** — it does not block the push path. A `tokio::spawn_blocking` dispatches the redb write transaction to a dedicated blocking thread (redb writes are synchronous and must not run on the tokio async runtime). A single writer task per database serializes all writes, avoiding redb's single-writer contention. Reads see a slightly stale index (eventual consistency within milliseconds), which is acceptable for ad-hoc queries.

> **AUDIT FIX (H20-H21):** The original design used `tokio::spawn` for redb writes, but redb's `WriteTransaction` blocks the thread. Using `spawn_blocking` prevents async runtime starvation. A dedicated writer task with an mpsc channel avoids single-writer lock contention when multiple sources push concurrently.

### Memcomparable Key Encoding

```rust
use arrow::datatypes::DataType;
use datafusion::scalar::ScalarValue;

/// Encode an Arrow scalar value to bytes that preserve sort order
/// under lexicographic byte comparison.
///
/// Encoding rules:
/// - **Unsigned integers** (u8/u16/u32/u64): big-endian bytes
/// - **Signed integers** (i8/i16/i32/i64): flip sign bit + big-endian
///   (so negatives sort before positives)
/// - **Floats** (f32/f64): IEEE 754 order-preserving transform
///   (flip sign bit; if original was negative, flip all bits)
/// - **Strings/Binary**: null-terminated with 0x00 → 0x00 0xFF escaping
/// - **Null**: 0x00 prefix (sorts before all non-null values)
/// - **Non-null**: 0x01 prefix
///
/// # Examples
///
/// ```
/// assert!(encode_comparable(&ScalarValue::Int64(Some(-1)))
///     < encode_comparable(&ScalarValue::Int64(Some(0))));
/// assert!(encode_comparable(&ScalarValue::Int64(Some(0)))
///     < encode_comparable(&ScalarValue::Int64(Some(1))));
/// assert!(encode_comparable(&ScalarValue::Int64(Some(255)))
///     < encode_comparable(&ScalarValue::Int64(Some(256))));
/// ```
pub fn encode_comparable(value: &ScalarValue) -> Vec<u8> {
    match value {
        ScalarValue::Null => vec![0x00],
        ScalarValue::Int8(Some(v)) => {
            vec![0x01, (*v as u8) ^ 0x80] // flip sign bit
        }
        ScalarValue::Int16(Some(v)) => {
            let mut buf = vec![0x01];
            buf.extend_from_slice(&((*v as u16) ^ 0x8000).to_be_bytes());
            buf
        }
        ScalarValue::Int32(Some(v)) => {
            let mut buf = vec![0x01];
            buf.extend_from_slice(&((*v as u32) ^ 0x8000_0000).to_be_bytes());
            buf
        }
        ScalarValue::Int64(Some(v)) => {
            let mut buf = vec![0x01];
            buf.extend_from_slice(&((*v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes());
            buf
        }
        ScalarValue::UInt8(Some(v)) => vec![0x01, *v],
        ScalarValue::UInt16(Some(v)) => {
            let mut buf = vec![0x01];
            buf.extend_from_slice(&v.to_be_bytes());
            buf
        }
        ScalarValue::UInt32(Some(v)) => {
            let mut buf = vec![0x01];
            buf.extend_from_slice(&v.to_be_bytes());
            buf
        }
        ScalarValue::UInt64(Some(v)) => {
            let mut buf = vec![0x01];
            buf.extend_from_slice(&v.to_be_bytes());
            buf
        }
        ScalarValue::Float32(Some(v)) => {
            let bits = v.to_bits();
            let encoded = if bits & 0x8000_0000 != 0 { !bits } else { bits ^ 0x8000_0000 };
            let mut buf = vec![0x01];
            buf.extend_from_slice(&encoded.to_be_bytes());
            buf
        }
        ScalarValue::Float64(Some(v)) => {
            let bits = v.to_bits();
            let encoded = if bits & 0x8000_0000_0000_0000 != 0 { !bits } else { bits ^ 0x8000_0000_0000_0000 };
            let mut buf = vec![0x01];
            buf.extend_from_slice(&encoded.to_be_bytes());
            buf
        }
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            let mut buf = vec![0x01];
            // Escape 0x00 bytes: 0x00 → 0x00 0xFF, then terminate with 0x00 0x00
            for &b in s.as_bytes() {
                buf.push(b);
                if b == 0x00 {
                    buf.push(0xFF);
                }
            }
            buf.extend_from_slice(&[0x00, 0x00]); // terminator
            buf
        }
        ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b)) => {
            let mut buf = vec![0x01];
            for &byte in b.iter() {
                buf.push(byte);
                if byte == 0x00 {
                    buf.push(0xFF);
                }
            }
            buf.extend_from_slice(&[0x00, 0x00]);
            buf
        }
        // Any None variant → null encoding
        _ => vec![0x00],
    }
}

/// Decode a comparable-encoded key back to a ScalarValue.
pub fn decode_comparable(bytes: &[u8], data_type: &DataType) -> Result<ScalarValue, IndexError> {
    if bytes.is_empty() || bytes[0] == 0x00 {
        return Ok(ScalarValue::Null);
    }
    let payload = &bytes[1..]; // skip null prefix
    match data_type {
        DataType::Int64 => {
            let raw = u64::from_be_bytes(payload.try_into().map_err(|_| IndexError::DecodeError)?);
            Ok(ScalarValue::Int64(Some((raw ^ 0x8000_0000_0000_0000) as i64)))
        }
        // ... analogous for other types
        _ => Err(IndexError::UnsupportedType(data_type.clone())),
    }
}
```

### RedbTableProvider

```rust
pub(crate) struct RedbTableProvider {
    db: Arc<redb::Database>,
    table_name: String,
    schema: SchemaRef,
}

impl TableProvider for RedbTableProvider {
    fn scan(&self, ..., filters: &[Expr], ...) -> Result<Arc<dyn ExecutionPlan>> {
        // If filters match indexed column → B-tree range scan
        // Keys encoded via encode_comparable() for correct sort order
        // Otherwise → fall back to full scan via MemTable
    }
}
```

The provider is registered alongside `SourceSnapshotProvider` in DataFusion's `SessionContext`. DataFusion's optimizer pushes predicates down; if the predicate matches an indexed column, `RedbTableProvider` performs a B-tree lookup. Otherwise, it delegates to the snapshot buffer (full scan).

### Index Lifecycle

| Event | Action |
|-------|--------|
| `CREATE SOURCE` / `CREATE VIEW` | Analyze pipeline, create redb tables for key columns |
| Push batch | Async update: insert/update index entries |
| `DROP SOURCE` / `DROP VIEW` | Delete redb tables, reclaim space |
| Checkpoint | Index state is NOT checkpointed — rebuilt from source on recovery |
| Recovery | Indexes are rebuilt by replaying buffered data |

### Configuration

```toml
[indexes]
enabled = true                    # Default: true
db_path = "laminardb_indexes.redb" # Default: data dir
max_db_size = "1GB"               # redb pre-allocates file
auto_derive = true                # Derive from pipeline keys
```

## Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| redb write failure | Disk full, I/O error | Log warning, skip index update. Queries fall back to full scan. |
| Corrupt index file | Crash during compaction | Delete and rebuild from source data on next startup. |
| Schema mismatch | Source schema evolved | Drop and recreate affected indexes. |

## Test Plan

### Unit Tests

- [ ] `IndexManager` creates redb tables from pipeline `GROUP BY` columns
- [ ] `IndexManager` creates redb tables from `JOIN ON` predicates
- [ ] Index update correctly inserts/updates entries
- [ ] Index delete removes entries on `DROP SOURCE`
- [ ] `RedbTableProvider::scan()` with matching predicate uses B-tree lookup
- [ ] `RedbTableProvider::scan()` without predicate falls back to full scan
- [ ] `encode_comparable` i64: -1 < 0 < 1 < 255 < 256 (byte ordering correct)
- [ ] `encode_comparable` i64 boundary: `i64::MIN` < -1 < 0 < 1 < `i64::MAX`
- [ ] `encode_comparable` f64: -1.0 < -0.0 < 0.0 < 1.0 < f64::INFINITY
- [ ] `encode_comparable` f64: `NaN` sorts consistently (not equal to itself)
- [ ] `encode_comparable` string: "a" < "ab" < "b" (lexicographic)
- [ ] `encode_comparable` null: NULL < any non-null value
- [ ] `encode_comparable`/`decode_comparable` roundtrip for all supported types
- [ ] `encode_comparable` string with embedded 0x00 byte: escape/unescape roundtrip

### Integration Tests

- [ ] `CREATE SOURCE → INSERT → SELECT WHERE key = value` uses index
- [ ] `DROP SOURCE` cleans up index tables
- [ ] Recovery after crash rebuilds indexes
- [ ] `cargo build` succeeds without C++ compiler (redb is pure Rust)

### Performance Tests

- [ ] Index point lookup < 10us (single key)
- [ ] Index range scan < 100us (100 rows)
- [ ] Index update overhead < 50us per batch (async, not on hot path)

## Completion Checklist

- [ ] `IndexManager` struct with auto-derive logic
- [ ] `RedbTableProvider` implementing `TableProvider`
- [ ] Integration with `handle_create_source()` / `handle_drop_source()`
- [ ] Async index update on push path
- [ ] Unit and integration tests passing
- [ ] No C++ compiler required for build
- [ ] Feature INDEX.md updated

---

## References

- [redb documentation](https://docs.rs/redb/)
- [F-LOOKUP-010: Remove RocksDB](../lookup/F-LOOKUP-010-rocksdb-removal.md)
- [F-CONN-002D: RocksDB-Backed Persistent Table Store](../../phase-3/F-CONN-002D-rocksdb-table-store.md) (superseded)
- [Storage Architecture Research](../../../research/storage-architecture.md)
