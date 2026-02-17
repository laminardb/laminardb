# F-STATE-001: Revised StateStore Trait

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-STATE-001 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | M (3-5 days) |
| **Dependencies** | None (foundational) |
| **Blocks** | F-STATE-002, F-STATE-003, F-STATE-004 |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/state/mod.rs` |

## Summary

Revised `StateStore` trait for the Constellation Architecture. This is the foundational abstraction all operators use for state access in Ring 0 (synchronous, zero-alloc, zero-lock hot path). The key revision from the Phase 1 trait (F003) is the removal of `get_async` (anti-pattern for Ring 0), preserving `get()` returning `Option<Bytes>` for ownership flexibility (with a new `get_ref()` for zero-copy hot-path access), exclusive `&mut self` ownership semantics for `put()`, unifying `prefix_scan` and `range_scan` into a single `range_scan()` method, and a pluggable `StateSnapshot` trait alongside the existing concrete `StateSnapshot` struct. A separate `AsyncStateStore` trait is deferred to Phase 5 for async state backends like SlateDB.

## Goals

- Define a synchronous-only `StateStore` trait suitable for Ring 0 hot path execution
- Retain `get()` returning `Option<Bytes>` for ergonomic ownership; add `get_ref()` returning `Option<&[u8]>` for zero-copy hot-path access where borrow lifetimes permit
- Exclusive `&mut self` on `put()` and `delete()` enforcing single-partition ownership
- `range_scan()` and `prefix_scan()` retained from Phase 1 for backward compatibility
- `snapshot()` continuing to return concrete `StateSnapshot` (Phase 1 compatible); pluggable `SnapshotStrategy` trait added separately (F-STATE-004)
- `restore()` for recovery from snapshots
- `StateError` enum covering all failure modes with `thiserror` derives
- `StateSnapshot` trait with rkyv-based serialization/deserialization
- Sub-100ns `get()` for in-memory, sub-200ns for mmap backends
- Full backward compatibility migration path from Phase 1 `StateStore`

## Non-Goals

- Async state access (`get_async`, `put_async`) -- deferred to `AsyncStateStore` in Phase 5
- SlateDB or other remote state backends -- deferred to Phase 7
- Cross-partition state access -- violates ownership model
- Transaction semantics (multi-key atomicity) -- separate feature
- Schema enforcement on keys/values -- handled at operator layer
- Compression of stored values -- handled at snapshot/checkpoint layer

## Technical Design

### Architecture

**Ring**: Ring 0 (Hot Path) -- synchronous, zero-allocation, zero-lock
**Crate**: `laminar-core`
**Module**: `laminar-core/src/state/mod.rs`

The `StateStore` trait is the single most critical abstraction in the Constellation Architecture. Every stateful operator (windows, joins, aggregations, materialized views) accesses its partition's state through this trait. Because it runs on the Ring 0 hot path:

1. All methods are synchronous -- no `async`, no futures, no runtime
2. `get()` returns a borrowed slice -- zero allocation on the read path
3. `put()` takes `&mut self` -- the partition reactor owns its state exclusively
4. No locks -- single-threaded access enforced by the partition ownership model
5. Snapshot creation is the bridge to Ring 1, where checkpoint I/O happens

```
┌──────────────────────────────────────────────────────────────────────┐
│                         RING 0: HOT PATH                             │
│                                                                      │
│  ┌──────────────┐    get(&[u8]) -> Option<&[u8]>   ┌─────────────┐  │
│  │   Operator   │ ──────────────────────────────> │  StateStore  │  │
│  │  (Window /   │    put(&mut, &[u8], &[u8])       │  (InMemory/  │  │
│  │   Join /     │ <────────────────────────────── │   Mmap)      │  │
│  │   Agg)       │    range(&[u8], &[u8])           │              │  │
│  └──────────────┘ ──────────────────────────────> │              │  │
│                                                    └──────┬───────┘  │
│                                                           │          │
├───────────────────────────────────────────────────────────┼──────────┤
│                         RING 1: CHECKPOINT                │          │
│                                                           ▼          │
│  ┌──────────────┐    snapshot() -> Box<StateSnapshot>                │
│  │  Checkpoint  │ <──────────────────────────────────────────────── │
│  │  Coordinator │    restore(Box<StateSnapshot>)                     │
│  └──────────────┘                                                    │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use std::ops::Range;
use bytes::Bytes;

/// Type alias for state iteration results.
///
/// Returns owned key-value pairs in lexicographic order.
/// The iterator borrows from the state store, preventing mutation
/// during iteration (enforced by Rust's borrow checker).
pub type StateIter<'a> = Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a>;

/// Core trait for key-value state storage.
///
/// This is the foundational abstraction for all operator state in Ring 0
/// (hot path). Implementations must be synchronous, lock-free, and
/// single-threaded. Each partition reactor owns its state store
/// exclusively via `&mut self`.
///
/// # Compatibility
///
/// This trait evolves the Phase 1 `StateStore` (F003). Key changes:
/// - **`get_async` removed**: Async state access breaks Ring 0 latency
///   guarantees. Use `AsyncStateStore` (Phase 7) for remote backends.
/// - **`get()` returns `Option<Bytes>`**: Preserved from Phase 1 for
///   ownership flexibility. `Bytes` is reference-counted and cheap to
///   clone (~2ns). Operators that need zero-copy access use `get_ref()`.
/// - **`get_ref()` added**: Returns `Option<&[u8]>` for zero-copy reads
///   on the hot path where the borrow lifetime is immediately consumed.
/// - **`put()` takes `&mut self`**: Enforces exclusive partition
///   ownership. No concurrent writes, no locks needed.
/// - **`prefix_scan` and `range_scan` retained**: Both scan methods
///   from Phase 1 are preserved for backward compatibility.
///
/// # Existing Implementations
///
/// - `InMemoryStore` (`BTreeMap<Vec<u8>, Bytes>` backend)
/// - `MmapStateStore` (memory-mapped, file-backed)
/// - `ChangelogAwareStore<S>` (wrapper adding changelog tracking)
/// - `WalStateStore` (WAL-backed, in `laminar-storage`)
///
/// # Thread Safety
///
/// State stores are `Send` but not `Sync`. They are designed for
/// single-threaded access within a partition reactor. Cross-partition
/// communication uses SPSC queues (F014).
///
/// # Performance Requirements
///
/// | Operation | In-Memory Target | Mmap Target |
/// |-----------|-----------------|-------------|
/// | `get()`   | < 100ns         | < 200ns     |
/// | `get_ref()`| < 50ns         | < 100ns     |
/// | `put()`   | < 150ns         | < 300ns     |
/// | `delete()`| < 150ns         | < 300ns     |
/// | `range_scan()` | < 200ns + O(k) | < 300ns + O(k) |
pub trait StateStore: Send {
    /// Get a value by key, returning an owned `Bytes` handle.
    ///
    /// Returns `None` if the key does not exist. `Bytes` is
    /// reference-counted (~2ns clone) and can be stored or sent
    /// across channels. This is the primary read method.
    ///
    /// # Performance
    ///
    /// Target: < 100ns (in-memory), < 200ns (mmap).
    fn get(&self, key: &[u8]) -> Option<Bytes>;

    /// Get a value by key, returning a zero-copy borrowed slice.
    ///
    /// Returns `None` if the key does not exist **or** if the backend
    /// does not support zero-copy access. The returned slice borrows
    /// from the store, so the store cannot be mutated while the
    /// reference is held. Use this when the value is consumed
    /// immediately (e.g., comparison, hashing) to avoid `Bytes` overhead.
    ///
    /// # Important — Fallback Required
    ///
    /// The default implementation returns `None`, indicating zero-copy
    /// is **not available**. Callers **MUST** fall back to `get()` when
    /// `get_ref()` returns `None`:
    ///
    /// ```rust,ignore
    /// let value = store.get_ref(key)
    ///     .map(Bytes::copy_from_slice)
    ///     .or_else(|| store.get(key));
    /// ```
    ///
    /// > **AUDIT FIX (H2):** The default cannot delegate to `get()` because
    /// > `Bytes` does not borrow from `self` — there is no `&[u8]` to return.
    /// > Backends that store values inline (FxHashMap, BTreeMap) override this
    /// > method for true zero-copy. Cache-backed stores (foyer) cannot support
    /// > `get_ref()` due to RAII handle lifetimes and should return `None`.
    ///
    /// # Performance
    ///
    /// Target: < 50ns (in-memory), < 100ns (mmap).
    fn get_ref(&self, key: &[u8]) -> Option<&[u8]> {
        // Default: not available — backends override for true zero-copy.
        // Callers MUST fall back to get() when this returns None.
        let _ = key; // suppress unused warning
        None
    }

    /// Store a key-value pair.
    ///
    /// If the key already exists, the value is overwritten. Takes
    /// `&mut self` to enforce exclusive partition ownership.
    ///
    /// # Errors
    ///
    /// Returns `StateError::CapacityExceeded` if the store is full.
    /// Returns `StateError::Io` for mmap write failures.
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateError>;

    /// Delete a key from the store.
    ///
    /// Returns `Ok(())` even if the key does not exist (idempotent).
    ///
    /// # Errors
    ///
    /// Returns `StateError::Io` for mmap operation failures.
    fn delete(&mut self, key: &[u8]) -> Result<(), StateError>;

    /// Scan all keys with a given prefix.
    ///
    /// Returns an iterator over matching (key, value) pairs in
    /// lexicographic order.
    fn prefix_scan<'a>(&'a self, prefix: &'a [u8])
        -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a>;

    /// Range scan between two keys (exclusive end).
    ///
    /// Returns an iterator over keys where `start <= key < end`
    /// in lexicographic order.
    fn range_scan<'a>(
        &'a self,
        range: Range<&'a [u8]>,
    ) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a>;

    /// Check if a key exists without returning the value.
    ///
    /// Default implementation delegates to `get()`. Backends may
    /// override for efficiency (e.g., bloom filter check).
    fn contains(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Get the number of entries in the store.
    fn len(&self) -> usize;

    /// Check if the store is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get approximate size in bytes (keys + values).
    fn size_bytes(&self) -> usize;

    /// Create a full snapshot for checkpointing.
    ///
    /// Returns a concrete `StateSnapshot` capturing the current state.
    /// This is the bridge from Ring 0 to Ring 1: the snapshot can be
    /// serialized and persisted by the checkpoint coordinator.
    ///
    /// **Warning**: For large state stores (>100K entries), this is O(n)
    /// and blocks Ring 0. Prefer `incremental_snapshot()` when available.
    fn snapshot(&self) -> StateSnapshot;

    /// Create an incremental snapshot from changes since last barrier.
    ///
    /// Returns `Some(delta)` containing only mutations (put/delete) since
    /// the last call to `incremental_snapshot()` or `snapshot()`. Returns
    /// `None` if the store does not support incremental snapshots, in
    /// which case the checkpoint coordinator falls back to `snapshot()`.
    ///
    /// > **AUDIT FIX (C1):** Added to address the snapshot consistency gap
    /// > left by superseding F-STATE-004 (fork/COW). The O(n) full clone in
    /// > `snapshot()` blocks Ring 0 for 50-200ms on 1M entries. With
    /// > incremental snapshots via `ChangelogAwareStore<S>`, barrier
    /// > overhead drops to O(delta) — typically microseconds.
    ///
    /// # Checkpoint coordinator protocol
    ///
    /// 1. At barrier, call `state.incremental_snapshot()`
    /// 2. If `Some(delta)` → serialize delta (microseconds for typical workloads)
    /// 3. If `None` → fall back to full `state.snapshot()` (first checkpoint)
    /// 4. Every N checkpoints (configurable, default 10), force a full
    ///    snapshot for compaction
    /// 5. Recovery: load last full snapshot, then replay all incremental
    ///    deltas in epoch order
    fn incremental_snapshot(&mut self) -> Option<IncrementalSnapshot> {
        None // Default: not supported; coordinator falls back to snapshot()
    }

    /// Restore state from a snapshot.
    ///
    /// Replaces the current state with the snapshot's contents. Called
    /// during recovery after a failure. Takes ownership of the snapshot.
    fn restore(&mut self, snapshot: StateSnapshot);

    /// Clear all entries from the store.
    fn clear(&mut self);

    /// Flush any pending writes to durable storage.
    ///
    /// For in-memory stores, this is a no-op. For memory-mapped or
    /// disk-backed stores, this ensures data is persisted.
    fn flush(&mut self) -> Result<(), StateError> {
        Ok(()) // Default no-op for in-memory stores
    }

    /// Get a value or insert a default.
    ///
    /// If the key doesn't exist, the default is inserted and returned.
    fn get_or_insert(&mut self, key: &[u8], default: &[u8]) -> Result<Bytes, StateError> {
        if let Some(value) = self.get(key) {
            Ok(value)
        } else {
            self.put(key, default)?;
            Ok(Bytes::copy_from_slice(default))
        }
    }
}
```

### StateSnapshot Trait

```rust
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rkyv::util::AlignedVec;

/// Concrete snapshot for state checkpointing.
///
/// This struct is the EXISTING Phase 1 `StateSnapshot`, preserved for
/// backward compatibility. It stores a full copy of all key-value pairs.
/// Serializable via rkyv for zero-copy access during recovery.
///
/// For advanced snapshot strategies (fork/COW, double buffer, incremental),
/// see F-STATE-004 which introduces a `SnapshotStrategy` trait that
/// produces `StateSnapshot` instances more efficiently.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StateSnapshot {
    /// All key-value pairs at snapshot time.
    data: Vec<(Vec<u8>, Vec<u8>)>,
    /// Timestamp in nanoseconds since Unix epoch.
    timestamp_ns: u64,
    /// Format version for forward compatibility.
    version: u32,
}

impl StateSnapshot {
    /// Create a new snapshot from key-value pairs.
    pub fn new(data: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self {
            data,
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
            version: 1,
        }
    }

    /// Serialize the snapshot to bytes using rkyv.
    ///
    /// Returns an aligned byte vector suitable for zero-copy access.
    pub fn to_bytes(&self) -> Result<AlignedVec, StateError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map_err(|e| StateError::Serialization(e.to_string()))
    }

    /// Deserialize from rkyv bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, StateError> {
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(bytes)
            .map_err(|e| StateError::Deserialization(e.to_string()))
    }

    /// Get the number of entries in the snapshot.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the snapshot is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get the timestamp when this snapshot was created.
    pub fn timestamp_ns(&self) -> u64 {
        self.timestamp_ns
    }

    /// Iterate over snapshot entries for restoration.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.data.iter().map(|(k, v)| (k.as_slice(), v.as_slice()))
    }

    /// Consume the snapshot and return owned entries.
    pub fn into_entries(self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.data
    }
}

/// Incremental snapshot containing only mutations since the last barrier.
///
/// AUDIT FIX (C1): Replaces the superseded F-STATE-004 fork/COW mechanism.
/// Uses the existing `ChangelogAwareStore<S>` wrapper to record mutations
/// between checkpoint barriers. At barrier time, only the changelog delta
/// is serialized — not the full state.
///
/// # Recovery Protocol
///
/// 1. Load the most recent **full** `StateSnapshot`
/// 2. Replay all `IncrementalSnapshot` deltas in epoch order
/// 3. Each delta's `ChangeEntry::Put` / `ChangeEntry::Delete` is applied
///    to the restored state store
///
/// # Compaction
///
/// Every N checkpoints (configurable, default 10), the coordinator forces
/// a full `snapshot()` to prevent unbounded delta chains.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct IncrementalSnapshot {
    /// Mutations since last full or incremental snapshot.
    pub changes: Vec<ChangeEntry>,
    /// Epoch of the base snapshot this delta is relative to.
    pub base_epoch: u64,
    /// This snapshot's epoch.
    pub epoch: u64,
    /// Timestamp in nanoseconds since Unix epoch.
    pub timestamp_ns: u64,
}

/// A single mutation in a changelog delta.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum ChangeEntry {
    /// Key-value pair was inserted or updated.
    Put { key: Vec<u8>, value: Vec<u8> },
    /// Key was deleted.
    Delete { key: Vec<u8> },
}

impl IncrementalSnapshot {
    /// Create a new incremental snapshot.
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(changes: Vec<ChangeEntry>, base_epoch: u64, epoch: u64) -> Self {
        Self {
            changes,
            base_epoch,
            epoch,
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
        }
    }

    /// Serialize to rkyv bytes.
    pub fn to_bytes(&self) -> Result<AlignedVec, StateError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map_err(|e| StateError::Serialization(e.to_string()))
    }

    /// Deserialize from rkyv bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, StateError> {
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(bytes)
            .map_err(|e| StateError::Deserialization(e.to_string()))
    }

    /// Number of mutations in this delta.
    pub fn len(&self) -> usize {
        self.changes.len()
    }

    /// Check if the delta is empty.
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}
```

### Data Structures

```rust
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

/// Errors that can occur in state operations.
///
/// Covers all failure modes across state store backends.
/// Uses `thiserror` for ergonomic error handling.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// I/O error (mmap, file operations).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error (rkyv encoding).
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Deserialization error (rkyv decoding, snapshot restore).
    #[error("Deserialization error: {0}")]
    Deserialization(String),

    /// Data corruption detected (invalid magic, checksum mismatch).
    #[error("Corruption error: {0}")]
    Corruption(String),

    /// Operation not supported by this backend.
    #[error("Operation not supported: {0}")]
    NotSupported(String),

    /// Key not found (for operations requiring an existing key).
    #[error("Key not found")]
    KeyNotFound,

    /// Store capacity exceeded (mmap file full, arena exhausted).
    #[error("Store capacity exceeded: {0}")]
    CapacityExceeded(String),
}
```

### Algorithm/Flow

#### Get Operation (Hot Path)

```
1. Operator calls state.get(key) on its partition's StateStore
2. Backend performs lookup:
   - InMemory: HashMap::get(key) -> Option<&[u8]>   (< 100ns)
   - Mmap: hash_index_lookup(key) -> offset, return &mmap[offset..] (< 200ns)
3. Returns Option<&[u8]> -- zero-copy, no allocation
4. Borrow checker prevents mutation while reference is live
```

#### Snapshot Flow (Ring 0 -> Ring 1 Bridge)

```
AUDIT FIX (C1): Updated flow to use incremental snapshots as primary path.

1. Checkpoint coordinator sends barrier event through Ring 0
2. Barrier reaches operator, executor calls state.incremental_snapshot()
3a. If Some(delta) — incremental path (fast, O(delta)):
    - ChangelogAwareStore drains its mutation log into IncrementalSnapshot
    - Delta is passed to Ring 1 via channel
    - Ring 1 serializes delta via rkyv (~microseconds)
    - Overhead target: < 1us at barrier time
3b. If None — full snapshot path (slow, O(n)):
    - Operator calls state.snapshot()
    - InMemory: clones HashMap entries into StateSnapshot
    - Full snapshot is passed to Ring 1 via channel
    - Ring 1 serializes via rkyv
    - Used for first checkpoint and periodic compaction (every N barriers)
4. Serialized bytes are written to S3/local storage
5. Every N checkpoints (default 10), force full snapshot for compaction
```

#### Restore Flow (Recovery)

```
1. Recovery manager loads snapshot bytes from storage
2. Deserialize into Box<dyn StateSnapshot> via rkyv
3. Call state.restore(&snapshot) on the partition's StateStore
4. StateStore clears current state and replays snapshot entries
5. Resume processing from the snapshot epoch
```

### Migration from Phase 1 StateStore

The Phase 1 `StateStore` trait (F003) is largely preserved. Key changes:

1. **Added `get_ref()`**: New zero-copy read method with default implementation. Existing implementations work without changes; backends override for true zero-copy.
2. **`range()` renamed**: Phase 1 has both `prefix_scan()` and `range_scan()`. Both are retained.
3. **`snapshot()` return type**: Continues to return concrete `StateSnapshot` struct. No breaking change.
4. **`restore()` signature**: Continues to take `StateSnapshot` by value. No breaking change.
5. **`StateStoreExt`**: Existing extension trait with `get_typed<T>()`, `put_typed<T>()`, `update<F>()` is unchanged.
6. **`ChangelogAwareStore<S>`**: Existing wrapper is fully compatible; `get_ref()` delegates to inner store.

Migration effort: **Minimal** — the only new method (`get_ref()`) has a default implementation.

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `StateError::Io` | Mmap write failure, disk full | Retry after space reclaim, fail partition |
| `StateError::Serialization` | rkyv encoding failure on snapshot | Log + skip snapshot, retry next barrier |
| `StateError::Deserialization` | Corrupt snapshot data on restore | Fall back to older snapshot |
| `StateError::Corruption` | Invalid magic number, checksum fail | Full state rebuild from changelog |
| `StateError::CapacityExceeded` | Arena/mmap region exhausted | Trigger compaction, grow backing store |
| `StateError::KeyNotFound` | Operator expected key to exist | Application-level handling |
| `StateError::NotSupported` | Calling unsupported method on backend | Use correct backend or feature gate |

## Performance Targets

| Metric | Target | Backend | Measurement |
|--------|--------|---------|-------------|
| `get()` latency | < 100ns | InMemory | `bench_state_get_inmemory` |
| `get()` latency | < 200ns | Mmap | `bench_state_get_mmap` |
| `put()` latency | < 150ns | InMemory | `bench_state_put_inmemory` |
| `put()` latency | < 300ns | Mmap | `bench_state_put_mmap` |
| `delete()` latency | < 150ns | InMemory | `bench_state_delete_inmemory` |
| `range()` seek | < 200ns | InMemory | `bench_state_range_seek` |
| Barrier processing | < 50ns | All | `bench_barrier_overhead` |
| `snapshot()` (fork/COW) | < 1ms | Mmap | `bench_snapshot_fork` |
| `snapshot()` (full copy) | O(n) | InMemory | `bench_snapshot_copy` |
| `restore()` | O(n) | All | `bench_state_restore` |
| `get()` zero alloc | 0 allocs | All | Allocation tracking in tests |

## Test Plan

### Unit Tests

- [ ] `test_state_store_get_returns_none_for_missing_key`
- [ ] `test_state_store_get_returns_borrowed_slice`
- [ ] `test_state_store_put_insert_new_key`
- [ ] `test_state_store_put_overwrite_existing_key`
- [ ] `test_state_store_delete_existing_key`
- [ ] `test_state_store_delete_missing_key_is_idempotent`
- [ ] `test_state_store_range_returns_ordered_entries`
- [ ] `test_state_store_range_empty_range_returns_empty_iter`
- [ ] `test_state_store_range_borrows_prevent_mutation`
- [ ] `test_state_store_contains_delegates_to_get`
- [ ] `test_state_store_len_tracks_entry_count`
- [ ] `test_state_store_size_bytes_tracks_memory`
- [ ] `test_state_store_clear_removes_all_entries`
- [ ] `test_state_snapshot_to_bytes_roundtrip`
- [ ] `test_state_snapshot_epoch_preserved`
- [ ] `test_state_snapshot_iter_returns_all_entries`
- [ ] `test_full_state_snapshot_rkyv_serialization`
- [ ] `test_state_error_display_messages`
- [ ] `test_state_error_from_io_error`
- [ ] `test_state_iter_type_alias_works_with_box`
- [ ] `test_incremental_snapshot_default_returns_none` - Default impl returns None
- [ ] `test_incremental_snapshot_rkyv_roundtrip` - Serialize/deserialize delta
- [ ] `test_change_entry_put_and_delete_variants` - Both variants serialize correctly
- [ ] `test_incremental_snapshot_empty_delta` - Empty changelog produces empty delta

### Integration Tests

- [ ] `test_snapshot_and_restore_roundtrip` -- snapshot, mutate, restore, verify
- [ ] `test_snapshot_independence` -- snapshot, mutate store, verify snapshot unchanged
- [ ] `test_restore_from_serialized_bytes` -- serialize snapshot, deserialize, restore
- [ ] `test_state_store_with_operator` -- wire to a window operator, process events
- [ ] `test_state_store_barrier_flow` -- simulate barrier, snapshot, resume
- [ ] `test_migration_adapter_compatibility` -- Phase 1 trait adapter works

### Benchmarks

- [ ] `bench_state_get_inmemory` -- Target: < 100ns, 100K entries
- [ ] `bench_state_get_mmap` -- Target: < 200ns, 100K entries
- [ ] `bench_state_put_inmemory` -- Target: < 150ns
- [ ] `bench_state_put_mmap` -- Target: < 300ns
- [ ] `bench_state_range_seek` -- Target: < 200ns to first result
- [ ] `bench_state_range_iterate` -- Target: < 50ns per item
- [ ] `bench_barrier_overhead` -- Target: < 50ns
- [ ] `bench_snapshot_to_bytes` -- Measure serialization throughput
- [ ] `bench_state_get_zero_alloc` -- Verify zero allocations with `#[global_allocator]` tracking

## Rollout Plan

1. **Phase 1**: Define new `StateStoreV2` trait and `StateSnapshot` trait in `laminar-core/src/state/mod.rs`
2. **Phase 2**: Implement `FullStateSnapshot` with rkyv serialization
3. **Phase 3**: Add `StateStoreV2Adapter` for backward compatibility with Phase 1 trait
4. **Phase 4**: Unit tests for trait contracts and error types
5. **Phase 5**: Integration tests with existing operators
6. **Phase 6**: Benchmarks validating performance targets
7. **Phase 7**: Migrate operators from Phase 1 trait, rename to `StateStore`
8. **Phase 8**: Code review and merge

## Open Questions

- [x] ~~Should `get()` return `Option<&[u8]>` or `Option<Bytes>`?~~ **Resolved**: Keep `Option<Bytes>` for ownership ergonomics; add `get_ref()` for zero-copy hot path. The borrow checker conflict between `get()` returning `&[u8]` and subsequent `put()` calls makes `&[u8]` impractical as the primary return type.
- [x] ~~Should `StateSnapshot` be a trait or concrete struct?~~ **Resolved**: Keep concrete `StateSnapshot` struct from Phase 1 for backward compatibility. Pluggable strategies are handled by F-STATE-004's `SnapshotStrategy` trait.
- [ ] Should `range_scan()` accept `RangeBounds<&[u8]>` generically (like `BTreeMap::range()`) or keep the concrete `Range<&[u8]>`? Current design keeps `Range<&[u8]>` for consistency with Phase 1.
- [ ] Should `get_ref()` have a default that delegates to `get()` and returns `None`, or should it return `Some` by extracting from `Bytes`? Current default returns `None` (backends must override for true zero-copy).

## Completion Checklist

- [ ] `StateStore` trait defined with all methods
- [ ] `StateSnapshot` trait defined with rkyv serialization
- [ ] `StateIter` type alias defined
- [ ] `StateError` enum with all variants
- [ ] `FullStateSnapshot` concrete implementation
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet latency targets
- [ ] Migration adapter for Phase 1 compatibility
- [ ] Documentation updated with `#![deny(missing_docs)]`
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## References

- [F003: State Store Interface (Phase 1)](../../phase-1/F003-state-store-interface.md) -- predecessor trait
- [F002: Memory-Mapped State Store (Phase 1)](../../phase-1/F002-memory-mapped-state-store.md) -- mmap backend
- [F-STATE-002: InMemory State Store](./F-STATE-002-inmemory-state-store.md) -- HashMap backend
- [F-STATE-003: Mmap State Store](./F-STATE-003-mmap-state-store.md) -- mmap backend
- [F-STATE-004: Pluggable Snapshots](./F-STATE-004-pluggable-snapshots.md) -- snapshot strategies
- [F014: SPSC Queues](../../phase-2/F014-spsc-queues.md) -- cross-partition communication
- [F071: Zero-Allocation Enforcement](../../phase-2/F071-zero-allocation-enforcement.md) -- hot path constraints
- [ARCHITECTURE.md](../../../ARCHITECTURE.md) -- Ring model design
