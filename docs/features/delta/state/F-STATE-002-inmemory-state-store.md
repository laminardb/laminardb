# F-STATE-002: InMemory State Store

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-STATE-002 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-STATE-001 (StateStore trait) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/state/in_memory.rs` |

## Summary

`InMemoryStateStore` is the HashMap-based state store backend implementing the revised `StateStore` trait (F-STATE-001). It is the default backend for development, testing, and workloads where state fits in memory. It uses `AHashMap` (collision-resistant, AES-NI accelerated) for O(1) point lookups with sub-100ns latency and a sorted `BTreeMap` index overlay for ordered range scans. Snapshots are produced by cloning the full HashMap contents into a `FullStateSnapshot` with rkyv zero-copy serialization. This backend operates entirely in Ring 0 with no I/O, no locks, and no async.

> **AUDIT FIX (H3):** Changed from `FxHashMap` to `AHashMap`. FxHash is trivially invertible — an adversary can craft keys that all hash to the same bucket, causing O(n) lookup. For user-supplied keys (e.g., from Kafka message keys), this is a DoS vector. AHash uses AES-NI on supported platforms (~3ns vs AHash's ~2ns) with collision resistance. The 1ns overhead is within noise for the < 100ns get target.

## Goals

- Implement `StateStore` trait (F-STATE-001) with `AHashMap` for O(1) point lookups
- Sub-100ns `get()` latency with zero-copy `&[u8]` return
- Sub-150ns `put()` latency for insert and update
- `FullStateSnapshot` implementation with rkyv `Archive`/`Serialize`/`Deserialize` derives
- O(log n + k) range scans via `BTreeMap` secondary index
- Accurate `size_bytes()` tracking for memory monitoring
- Zero allocation on `get()` hot path
- Drop-in replacement for Phase 1 `InMemoryStore` with migration path

## Non-Goals

- Persistence to disk (use `MmapStateStore` for that)
- Larger-than-memory state (bounded by available RAM)
- Concurrent access from multiple threads (single-partition ownership)
- Compression of stored values
- TTL or automatic eviction (handled at operator layer)
- Sorted iteration without the BTreeMap overlay (hash maps are unordered)

## Technical Design

### Architecture

**Ring**: Ring 0 (Hot Path)
**Crate**: `laminar-core`
**Module**: `laminar-core/src/state/in_memory.rs`

The `InMemoryStateStore` is the simplest and fastest state store backend. It stores all key-value pairs in a `AHashMap<Vec<u8>, Vec<u8>>` for O(1) point lookups. For range scans, it maintains a parallel `BTreeMap<Vec<u8>, ()>` index that tracks which keys exist, enabling O(log n) seek to the start of a range followed by O(k) iteration.

```
┌──────────────────────────────────────────────────────────────────────┐
│                    InMemoryStateStore Layout                         │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                  AHashMap<Vec<u8>, Vec<u8>>                    │  │
│  │  ┌──────────┬──────────┬──────────┬──────────┬─────────────┐  │  │
│  │  │ bucket 0 │ bucket 1 │ bucket 2 │  ...     │ bucket N-1  │  │  │
│  │  │ key→val  │ key→val  │ key→val  │          │ key→val     │  │  │
│  │  └──────────┴──────────┴──────────┴──────────┴─────────────┘  │  │
│  │  O(1) get / put / delete / contains                            │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │              BTreeMap<Vec<u8>, ()> (range index)                │  │
│  │  O(log n) seek for range() / prefix_scan()                     │  │
│  │  Kept in sync with AHashMap on put/delete                     │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  size_bytes: usize   (tracks total key + value bytes)               │
│  entry_count: usize  (tracks number of entries)                     │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use ahash::AHashMap;
use std::collections::BTreeMap;
use std::ops::Range;

use crate::state::{
    FullStateSnapshot, StateError, StateIter, StateSnapshot, StateStore,
};

/// High-performance in-memory state store using AHashMap.
///
/// This is the default state store for development, testing, and
/// workloads where state fits entirely in memory. It provides:
///
/// - O(1) point lookups via `AHashMap` (< 100ns)
/// - O(log n + k) range scans via `BTreeMap` secondary index
/// - Zero-copy `get()` returning `&[u8]`
/// - Full snapshot via rkyv serialization
///
/// # Memory Layout
///
/// Keys and values are stored as `Vec<u8>` in the hash map. A
/// secondary `BTreeMap<Vec<u8>, ()>` maintains sorted key order
/// for range scans. The overhead is approximately 1 pointer per
/// key for the BTreeMap index.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::state::{StateStore, InMemoryStateStore};
///
/// let mut store = InMemoryStateStore::new();
/// store.put(b"user:1", b"alice").unwrap();
///
/// assert_eq!(store.get(b"user:1"), Some(b"alice".as_slice()));
/// assert_eq!(store.len(), 1);
/// ```
pub struct InMemoryStateStore {
    /// Primary storage: O(1) point lookups.
    /// AHashMap uses AES-NI accelerated hashing that is both fast
    /// (~3ns) and collision-resistant (prevents hash-flooding DoS).
    data: AHashMap<Vec<u8>, Vec<u8>>,

    /// Secondary index for ordered range scans.
    /// Tracks which keys exist in sorted order. Values are ()
    /// since we look up actual values from the AHashMap.
    sorted_keys: BTreeMap<Vec<u8>, ()>,

    /// Total bytes of keys + values for memory monitoring.
    size_bytes: usize,
}

impl InMemoryStateStore {
    /// Creates a new empty in-memory state store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: AHashMap::default(),
            sorted_keys: BTreeMap::new(),
            size_bytes: 0,
        }
    }

    /// Creates a new in-memory state store with pre-allocated capacity.
    ///
    /// Pre-allocating reduces rehashing overhead during initial population.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Expected number of entries (hint for HashMap)
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: AHashMap::with_capacity(capacity),
            sorted_keys: BTreeMap::new(),
            size_bytes: 0,
        }
    }

    /// Returns the current capacity of the underlying HashMap.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Shrink the HashMap to fit the current number of entries.
    pub fn shrink_to_fit(&mut self) {
        self.data.shrink_to_fit();
    }
}

impl Default for InMemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}
```

### StateStore Implementation

```rust
/// AUDIT FIX (H1): The following implementation matches the authoritative
/// `StateStore` trait signatures from F-STATE-001 exactly. Previous version
/// had mismatches in `get()` return type (`&[u8]` vs `Bytes`), method naming
/// (`range` vs `range_scan`), `snapshot()` return type (`Box<dyn>` vs concrete),
/// and `restore()` signature (borrowed vs owned, Result vs void).
impl StateStore for InMemoryStateStore {
    #[inline]
    fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.data.get(key).map(|v| Bytes::copy_from_slice(v))
    }

    /// Zero-copy get — returns a direct reference into the HashMap.
    /// Callers should prefer this over `get()` when the borrow lifetime
    /// is immediately consumed (avoids Bytes allocation).
    #[inline]
    fn get_ref(&self, key: &[u8]) -> Option<&[u8]> {
        self.data.get(key).map(|v| v.as_slice())
    }

    #[inline]
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateError> {
        let key_vec = key.to_vec();
        let value_vec = value.to_vec();

        // Update size tracking
        if let Some(old_value) = self.data.get(&key_vec) {
            // Overwrite: adjust size by value delta only
            self.size_bytes -= old_value.len();
            self.size_bytes += value.len();
        } else {
            // New entry: add both key and value sizes
            self.size_bytes += key.len() + value.len();
            // Insert into sorted index for range scans
            self.sorted_keys.insert(key_vec.clone(), ());
        }

        self.data.insert(key_vec, value_vec);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        if let Some(old_value) = self.data.remove(key) {
            self.size_bytes -= key.len() + old_value.len();
            self.sorted_keys.remove(key);
        }
        Ok(())
    }

    fn range_scan<'a>(&'a self, range: Range<&'a [u8]>) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a> {
        use std::ops::Bound;
        // Use BTreeMap for ordered key iteration, look up values from HashMap
        Box::new(
            self.sorted_keys
                .range::<[u8], _>((
                    Bound::Included(range.start),
                    Bound::Excluded(range.end),
                ))
                .filter_map(move |(k, _)| {
                    self.data.get(k.as_slice()).map(|v| {
                        (Bytes::copy_from_slice(k), Bytes::copy_from_slice(v))
                    })
                })
        )
    }

    fn snapshot(&self) -> StateSnapshot {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = self.data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        StateSnapshot::new(entries)
    }

    fn restore(&mut self, snapshot: StateSnapshot) {
        self.data.clear();
        self.sorted_keys.clear();
        self.size_bytes = 0;

        for (key, value) in snapshot.into_entries() {
            self.size_bytes += key.len() + value.len();
            self.sorted_keys.insert(key.clone(), ());
            self.data.insert(key, value);
        }
    }

    #[inline]
    fn contains(&self, key: &[u8]) -> bool {
        self.data.contains_key(key)
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    fn clear(&mut self) {
        self.data.clear();
        self.sorted_keys.clear();
        self.size_bytes = 0;
    }
}
```

### Data Structures

```rust
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rkyv::util::AlignedVec;

/// Full state snapshot storing a complete copy of all key-value pairs.
///
/// This is the snapshot type produced by `InMemoryStateStore`. It
/// stores an owned copy of all entries, making it independent of
/// the source state store. Uses rkyv for zero-copy serialization.
///
/// # Serialization Format
///
/// ```text
/// [rkyv header]
/// [entry_count: u64]
/// [epoch: u64]
/// [timestamp_ns: u64]
/// [version: u32]
/// [entries: Vec<(Vec<u8>, Vec<u8>)>]  // rkyv-encoded
/// ```
///
/// The rkyv `Archive` derive enables zero-copy access to the
/// archived form, meaning deserialization can skip copying data
/// when the archived buffer is directly accessible.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct FullStateSnapshot {
    /// All key-value pairs at snapshot time.
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Logical epoch (barrier sequence number).
    epoch: u64,
    /// Wall-clock timestamp in nanoseconds since Unix epoch.
    timestamp_ns: u64,
    /// Format version for forward compatibility.
    version: u32,
}

impl FullStateSnapshot {
    /// Create a new full snapshot from key-value pairs.
    ///
    /// The epoch is set to 0 (will be assigned by checkpoint coordinator).
    /// Timestamp is captured at creation time.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self {
            entries,
            epoch: 0,
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
            version: 1,
        }
    }

    /// Create a snapshot with a specific epoch.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn with_epoch(entries: Vec<(Vec<u8>, Vec<u8>)>, epoch: u64) -> Self {
        Self {
            entries,
            epoch,
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
            version: 1,
        }
    }

    /// Deserialize a `FullStateSnapshot` from rkyv bytes.
    ///
    /// # Errors
    ///
    /// Returns `StateError::Deserialization` if the bytes are invalid.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, StateError> {
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(bytes)
            .map_err(|e| StateError::Deserialization(e.to_string()))
    }
}

impl StateSnapshot for FullStateSnapshot {
    fn to_bytes(&self) -> Result<AlignedVec, StateError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map_err(|e| StateError::Serialization(e.to_string()))
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn size_bytes(&self) -> usize {
        self.entries.iter().map(|(k, v)| k.len() + v.len()).sum()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + '_> {
        Box::new(self.entries.iter().cloned())
    }
}
```

### Algorithm/Flow

#### Get Operation

```
1. Compute AHash of key bytes (AES-NI)               (~5ns)
2. Index into HashMap bucket array            (~2ns)
3. Linear probe within bucket chain           (~10-30ns)
4. Compare key bytes for exact match          (~5-20ns)
5. Return &[u8] slice into value Vec          (0ns, pointer)
Total: ~20-60ns typical, < 100ns p99
```

#### Put Operation

```
1. Allocate Vec<u8> for key (if new)          (~20-50ns)
2. Allocate Vec<u8> for value                 (~20-50ns)
3. Compute AHash of key bytes (AES-NI)                (~5ns)
4. Insert/update HashMap entry                (~20-40ns)
5. Insert into BTreeMap sorted index          (~30-60ns, if new)
6. Update size_bytes counter                  (~2ns)
Total: ~70-120ns typical, < 150ns p99
```

#### Snapshot Operation

```
1. Allocate Vec<(Vec<u8>, Vec<u8>)>           (O(n) allocation)
2. Clone all entries from AHashMap           (O(n) memcpy)
3. Wrap in FullStateSnapshot                  (~10ns)
4. Return Box<dyn StateSnapshot>              (~10ns)
Total: O(n) where n = number of entries
Note: This runs at barrier time, not on per-event hot path
```

#### Range Operation

```
1. Seek to range.start in BTreeMap            (O(log n), ~30-60ns)
2. For each key in BTreeMap range:
   a. Look up value in AHashMap              (O(1), ~20-40ns)
   b. Yield (&[u8], &[u8]) pair              (0ns, zero-copy)
3. Iterator stops at range.end               (O(k) total)
Total: O(log n + k) where k = matching entries
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `StateError::Serialization` | rkyv failed to encode snapshot | Log error, retry on next barrier |
| `StateError::Deserialization` | Corrupt snapshot bytes during restore | Fall back to older snapshot |
| `OOM` (panic) | HashMap allocation failure | Operator-level OOM handling, partition restart |
| None | `get()` for missing key | Returns `None`, no error |
| None | `delete()` for missing key | Idempotent, returns `Ok(())` |

## Performance Targets

| Metric | Target | Method |
|--------|--------|--------|
| `get()` latency (100 entries) | < 50ns | `bench_inmemory_get_small` |
| `get()` latency (100K entries) | < 100ns | `bench_inmemory_get_large` |
| `put()` latency (new key) | < 150ns | `bench_inmemory_put_new` |
| `put()` latency (overwrite) | < 100ns | `bench_inmemory_put_overwrite` |
| `delete()` latency | < 100ns | `bench_inmemory_delete` |
| `range()` seek | < 100ns | `bench_inmemory_range_seek` |
| `range()` per item | < 50ns | `bench_inmemory_range_iterate` |
| `snapshot()` (10K entries) | < 1ms | `bench_inmemory_snapshot` |
| `restore()` (10K entries) | < 2ms | `bench_inmemory_restore` |
| `get()` allocations | 0 | Allocation tracker test |
| Snapshot rkyv serialization | > 500MB/s | `bench_snapshot_serialize` |
| Snapshot rkyv deserialization | > 1GB/s | `bench_snapshot_deserialize` |

## Test Plan

### Unit Tests

- [ ] `test_inmemory_new_creates_empty_store`
- [ ] `test_inmemory_with_capacity_preallocates`
- [ ] `test_inmemory_get_returns_none_for_missing`
- [ ] `test_inmemory_get_returns_borrowed_slice`
- [ ] `test_inmemory_put_new_key`
- [ ] `test_inmemory_put_overwrite_updates_value`
- [ ] `test_inmemory_put_overwrite_updates_size_bytes`
- [ ] `test_inmemory_delete_existing_key`
- [ ] `test_inmemory_delete_missing_key_is_idempotent`
- [ ] `test_inmemory_delete_updates_size_bytes`
- [ ] `test_inmemory_contains_true_for_existing`
- [ ] `test_inmemory_contains_false_for_missing`
- [ ] `test_inmemory_len_tracks_entries`
- [ ] `test_inmemory_is_empty_on_new_store`
- [ ] `test_inmemory_size_bytes_accurate`
- [ ] `test_inmemory_clear_resets_everything`
- [ ] `test_inmemory_range_returns_ordered_entries`
- [ ] `test_inmemory_range_empty_result`
- [ ] `test_inmemory_range_full_store`
- [ ] `test_inmemory_range_with_binary_keys`
- [ ] `test_inmemory_snapshot_captures_all_entries`
- [ ] `test_inmemory_snapshot_independent_of_store`
- [ ] `test_inmemory_restore_replaces_state`
- [ ] `test_inmemory_restore_clears_old_state`
- [ ] `test_full_snapshot_rkyv_roundtrip`
- [ ] `test_full_snapshot_from_bytes_invalid_data`
- [ ] `test_full_snapshot_epoch_preserved`
- [ ] `test_full_snapshot_size_bytes_accurate`
- [ ] `test_full_snapshot_iter_returns_all_entries`
- [ ] `test_inmemory_shrink_to_fit_reduces_capacity`

### Integration Tests

- [ ] `test_inmemory_snapshot_restore_cycle` -- put data, snapshot, mutate, restore, verify
- [ ] `test_inmemory_serialize_snapshot_to_disk` -- snapshot -> to_bytes -> file -> from_bytes -> restore
- [ ] `test_inmemory_with_window_operator` -- wire to tumbling window, process 1K events
- [ ] `test_inmemory_with_join_operator` -- wire to stream-stream join, verify state
- [ ] `test_inmemory_large_state` -- 1M entries, verify correctness and memory usage
- [ ] `test_inmemory_binary_key_value_roundtrip` -- keys/values with null bytes, 0xFF

### Benchmarks

- [ ] `bench_inmemory_get_small` -- 100 entries, target < 50ns
- [ ] `bench_inmemory_get_large` -- 100K entries, target < 100ns
- [ ] `bench_inmemory_put_new` -- sequential inserts, target < 150ns
- [ ] `bench_inmemory_put_overwrite` -- overwrite existing, target < 100ns
- [ ] `bench_inmemory_delete` -- delete existing, target < 100ns
- [ ] `bench_inmemory_range_seek` -- seek to start, target < 100ns
- [ ] `bench_inmemory_range_iterate` -- per-item iteration, target < 50ns/item
- [ ] `bench_inmemory_snapshot_10k` -- 10K entries, target < 1ms
- [ ] `bench_inmemory_snapshot_100k` -- 100K entries, measure scaling
- [ ] `bench_inmemory_restore_10k` -- 10K entries, target < 2ms
- [ ] `bench_snapshot_serialize_throughput` -- bytes/sec via rkyv
- [ ] `bench_snapshot_deserialize_throughput` -- bytes/sec via rkyv
- [ ] `bench_inmemory_get_zero_alloc` -- verify 0 heap allocations on get()

## Rollout Plan

1. **Phase 1**: Implement `InMemoryStateStore` struct with `AHashMap` + `BTreeMap`
2. **Phase 2**: Implement `StateStore` trait for `InMemoryStateStore`
3. **Phase 3**: Implement `FullStateSnapshot` with rkyv serialization
4. **Phase 4**: Unit tests for all methods and edge cases
5. **Phase 5**: Integration tests with existing operators
6. **Phase 6**: Benchmarks validating sub-100ns get, sub-150ns put
7. **Phase 7**: Migration shim from Phase 1 `InMemoryStore`
8. **Phase 8**: Code review and merge

## Open Questions

- [ ] Should we keep the `BTreeMap` secondary index, or use a sorted `Vec<u8>` for lower memory overhead? The BTreeMap adds ~48 bytes per entry overhead. A sorted Vec would be O(n) for inserts but O(log n) for lookups -- bad for write-heavy workloads.
- [ ] Should `with_capacity()` also pre-allocate the BTreeMap? BTreeMap does not support pre-allocation, so this only helps the HashMap.
- [ ] For very large state stores (>1M entries), should we consider a tiered approach where cold entries are moved to an mmap-backed store? This could be a separate `TieredStateStore` wrapper.
- [ ] Should `FullStateSnapshot::entries` be sorted for deterministic serialization? Sorting would add O(n log n) overhead to snapshot creation but make snapshots byte-for-byte reproducible.

## Completion Checklist

- [ ] `InMemoryStateStore` struct implemented
- [ ] `StateStore` trait implementation complete
- [ ] `FullStateSnapshot` with rkyv derives
- [ ] `StateSnapshot` trait implementation for `FullStateSnapshot`
- [ ] AHashMap + BTreeMap dual structure
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets (< 100ns get, < 150ns put)
- [ ] Zero-allocation verification for `get()`
- [ ] Documentation complete with `#![deny(missing_docs)]`
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## References

- [F-STATE-001: StateStore Trait](./F-STATE-001-state-store-trait.md) -- trait definition
- [F003: State Store Interface (Phase 1)](../../phase-1/F003-state-store-interface.md) -- predecessor
- [F-STATE-003: Mmap State Store](./F-STATE-003-mmap-state-store.md) -- mmap backend
- [F071: Zero-Allocation Enforcement](../../phase-2/F071-zero-allocation-enforcement.md) -- hot path constraints
- [ahash crate](https://crates.io/crates/ahash) -- AHashMap implementation (AES-NI accelerated, collision-resistant)
- [rkyv crate](https://crates.io/crates/rkyv) -- zero-copy serialization
