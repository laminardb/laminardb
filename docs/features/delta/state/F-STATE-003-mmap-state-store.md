# F-STATE-003: MmapStateStore

> **❌ SUPERSEDED (2026-02-17)** — This feature has been cancelled. The custom mmap state store is replaced by `FxHashMap`/`BTreeMap` (Ring 0) + `rkyv` serialization + `object_store` persistence. Benchmarks showed FxHashMap achieves ~40ns get/~60ns put vs. mmap's ~200ns/~300ns targets, making the complexity of a custom mmap layout unjustified. Durability is provided by rkyv-serialized checkpoint snapshots uploaded to object storage, not by surviving mmap files on local disk. See the [Storage Architecture Research](../../../research/storage-architecture.md) for the full analysis.
>
> **Superseded by:** F-STATE-001 (Revised StateStore Trait) + F-STATE-002 (InMemoryStateStore using FxHashMap/BTreeMap) + F-DCKP-004 (ObjectStoreCheckpointer with rkyv).

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-STATE-003 |
| **Status** | ❌ Superseded |
| **Priority** | ~P0~ N/A |
| **Phase** | 6a |
| **Effort** | ~L (5-10 days)~ N/A |
| **Dependencies** | F-STATE-001 (StateStore trait) |
| **Blocks** | ~F-STATE-004 (Pluggable Snapshots)~ None (F-STATE-004 also superseded) |
| **Owner** | N/A |
| **Created** | 2026-02-16 |
| **Superseded** | 2026-02-17 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/state/mmap.rs` |

## Summary

`MmapStateStore` is the production-default state store backend using memory-mapped files on local disk. It implements the revised `StateStore` trait (F-STATE-001) with a custom hash index layout for O(1) point lookups at sub-200ns latency. The file format consists of a 64-byte header, a hash index array of `(key_hash, data_ptr)` pairs using xxhash for fast hashing, and a variable-length data region storing serialized key-value entries. The mmap backend provides two-tier recovery: process crash recovery is instant via the surviving mmap file (milliseconds), while machine failure recovery falls back to S3 checkpoint (seconds). Platform-specific optimizations include `MAP_POPULATE` on Linux for page pre-faulting and `madvise` hints for access pattern optimization.

## Goals

- Implement `StateStore` trait (F-STATE-001) with memory-mapped file backing
- Custom hash index layout: `[Header: 64B][Hash Index: N * 16B][Data Region: variable]`
- Sub-200ns `get()` latency via xxhash + direct mmap pointer dereference
- Sub-300ns `put()` latency for append-only writes to the data region
- Two-tier recovery: process crash (ms) via local mmap, machine failure (s) via S3
- `MAP_POPULATE` on Linux for eager page population
- `madvise(MADV_RANDOM)` during normal operations, `madvise(MADV_SEQUENTIAL)` during checkpoint
- Automatic file growth with configurable growth factor
- Compaction to reclaim space from deleted entries
- Pluggable snapshot strategy integration (F-STATE-004)

## Non-Goals

- In-memory-only operation (use `InMemoryStateStore` for that)
- Remote/distributed state storage (deferred to SlateDB, Phase 7)
- Encryption of mmap files (handled by F044 encryption-at-rest layer)
- Concurrent multi-process access to the same mmap file
- NUMA-aware memory placement (handled by F068)
- Custom page sizes or huge page support (deferred to later optimization)

## Technical Design

### Architecture

**Ring**: Ring 0 (Hot Path) for read/write, Ring 1 for snapshot persistence
**Crate**: `laminar-core`
**Module**: `laminar-core/src/state/mmap.rs`

The `MmapStateStore` maps a file into the process address space using `mmap()`. Reads and writes operate directly on virtual memory addresses, with the OS kernel handling page faults and dirty page writeback. This gives us near-native memory access speeds for reads with the added benefit of transparent persistence.

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         Mmap File Layout                                 │
│                                                                          │
│  Offset 0                                                                │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │                    Header (64 bytes)                              │    │
│  │  magic: u64         = 0x004C_414D_494E_4152 ("LAMINAR")         │    │
│  │  version: u32       = 2                                          │    │
│  │  entry_count: u32   = number of live entries                     │    │
│  │  index_offset: u64  = byte offset to hash index start            │    │
│  │  data_offset: u64   = byte offset to data region start           │    │
│  │  index_capacity: u64 = total slots in hash index                 │    │
│  │  snapshot_epoch: u64 = last checkpoint epoch                     │    │
│  │  reserved: [u8; 8]  = zero (future use)                         │    │
│  └──────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  Offset 64                                                               │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │              Hash Index (index_capacity * 16 bytes)               │    │
│  │  ┌───────────────────┬───────────────────┐                       │    │
│  │  │ key_hash: u64     │ data_ptr: u64     │  slot 0               │    │
│  │  ├───────────────────┼───────────────────┤                       │    │
│  │  │ key_hash: u64     │ data_ptr: u64     │  slot 1               │    │
│  │  ├───────────────────┼───────────────────┤                       │    │
│  │  │ 0x0 (empty)       │ 0x0               │  slot 2 (unused)      │    │
│  │  ├───────────────────┼───────────────────┤                       │    │
│  │  │ ...               │ ...               │  ...                  │    │
│  │  ├───────────────────┼───────────────────┤                       │    │
│  │  │ key_hash: u64     │ data_ptr: u64     │  slot N-1             │    │
│  │  └───────────────────┴───────────────────┘                       │    │
│  │  key_hash: xxhash64(key_bytes)                                   │    │
│  │  data_ptr: byte offset into data region                          │    │
│  │  Empty slots: key_hash == 0, data_ptr == 0                       │    │
│  │  Tombstones: key_hash == 0xFFFF_FFFF_FFFF_FFFF                  │    │
│  │  Collision resolution: linear probing                            │    │
│  │  Load factor target: <= 0.75                                     │    │
│  └──────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  Offset data_offset                                                      │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │              Data Region (variable length)                        │    │
│  │                                                                   │    │
│  │  Entry format (packed, no alignment padding):                     │    │
│  │  ┌──────────┬──────────────┬──────────┬────────────────┐         │    │
│  │  │ key_len  │  key_bytes   │ val_len  │  value_bytes   │         │    │
│  │  │ (u32 LE) │ (key_len B)  │ (u32 LE) │ (val_len B)   │         │    │
│  │  └──────────┴──────────────┴──────────┴────────────────┘         │    │
│  │                                                                   │    │
│  │  Entries are append-only. Deleted entries become unreachable      │    │
│  │  (hash index slot cleared). Compaction reclaims space.           │    │
│  └──────────────────────────────────────────────────────────────────┘    │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use memmap2::MmapMut;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::ops::Range;

use crate::state::{StateError, StateIter, StateSnapshot, StateStore};

/// Header size in bytes.
const HEADER_SIZE: usize = 64;
/// Magic number: "LAMINAR\0" as u64.
const MAGIC: u64 = 0x004C_414D_494E_4152;
/// File format version.
const FORMAT_VERSION: u32 = 2;
/// Size of one hash index slot (key_hash: u64 + data_ptr: u64).
const INDEX_SLOT_SIZE: usize = 16;
/// Empty slot sentinel.
const EMPTY_HASH: u64 = 0;
/// Tombstone sentinel (deleted entry).
const TOMBSTONE_HASH: u64 = u64::MAX;
/// Maximum load factor before index resize.
const MAX_LOAD_FACTOR: f64 = 0.75;
/// Default growth factor for file expansion.
const GROWTH_FACTOR: f64 = 1.5;

/// Memory-mapped state store with custom hash index layout.
///
/// This is the production-default state store for LaminarDB. It uses
/// memory-mapped files for transparent persistence and near-native
/// memory access speeds.
///
/// # File Layout
///
/// ```text
/// [Header: 64 bytes]
/// [Hash Index: capacity * 16 bytes]
/// [Data Region: variable]
/// ```
///
/// # Recovery Model
///
/// - **Process crash**: mmap file survives, recovery in milliseconds
///   by re-opening the file and rebuilding the in-memory index from
///   the on-disk hash index.
/// - **Machine failure**: restore from S3 checkpoint, recovery in
///   seconds depending on state size and network bandwidth.
///
/// # Platform Optimizations
///
/// - **Linux**: `MAP_POPULATE` for eager page population,
///   `madvise(MADV_RANDOM)` for random access pattern,
///   `madvise(MADV_SEQUENTIAL)` during checkpoint streaming.
/// - **Windows**: Standard `CreateFileMapping` via memmap2 crate.
/// - **macOS**: Standard mmap, no special flags.
///
/// # Thread Safety
///
/// `Send` but not `Sync`. Designed for single-threaded access within
/// a partition reactor.
pub struct MmapStateStore {
    /// Memory-mapped file handle.
    mmap: MmapMut,
    /// File handle for resize operations.
    file: File,
    /// Path to the backing file.
    path: PathBuf,
    /// Total capacity of the mmap region in bytes.
    file_capacity: usize,
    /// Number of slots in the hash index (power of 2).
    index_capacity: usize,
    /// Bitmask for fast modulo: slot = hash & mask.
    index_mask: u64,
    /// Number of live entries.
    entry_count: usize,
    /// Number of tombstone slots (deleted entries not yet compacted).
    tombstone_count: usize,
    /// Current write position in the data region (append pointer).
    data_write_pos: usize,
    /// Byte offset where the data region starts.
    data_offset: usize,
    /// Last snapshot epoch.
    snapshot_epoch: u64,
    /// Total tracked size in bytes (keys + values, live entries only).
    size_bytes: usize,
}

impl MmapStateStore {
    /// Create a new mmap state store at the given path.
    ///
    /// If the file exists, it is opened and validated. If not, a new
    /// file is created with the given initial capacity.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the mmap file
    /// * `initial_capacity` - Initial number of hash index slots (rounded to power of 2)
    ///
    /// # Errors
    ///
    /// Returns `StateError::Io` for file system errors.
    /// Returns `StateError::Corruption` if existing file is invalid.
    pub fn open(path: &Path, initial_capacity: usize) -> Result<Self, StateError> {
        // ... implementation
        todo!()
    }

    /// Create an in-memory (anonymous) mmap state store.
    ///
    /// Uses an anonymous mmap region (not backed by a file). Useful
    /// for testing and ephemeral state that does not need persistence.
    ///
    /// # Arguments
    ///
    /// * `initial_capacity` - Initial number of hash index slots
    #[must_use]
    pub fn anonymous(initial_capacity: usize) -> Self {
        // ... implementation
        todo!()
    }

    /// Apply platform-specific mmap hints for normal operation.
    ///
    /// - Linux: `madvise(MADV_RANDOM)` for hash table access pattern
    /// - Other platforms: no-op
    fn apply_random_access_hints(&self) {
        #[cfg(target_os = "linux")]
        {
            // SAFETY: mmap pointer is valid and region is mapped.
            unsafe {
                libc::madvise(
                    self.mmap.as_ptr() as *mut libc::c_void,
                    self.file_capacity,
                    libc::MADV_RANDOM,
                );
            }
        }
    }

    /// Apply platform-specific mmap hints for sequential checkpoint reads.
    ///
    /// - Linux: `madvise(MADV_SEQUENTIAL)` for streaming checkpoint
    /// - Other platforms: no-op
    fn apply_sequential_access_hints(&self) {
        #[cfg(target_os = "linux")]
        {
            // SAFETY: mmap pointer is valid and region is mapped.
            unsafe {
                libc::madvise(
                    self.mmap.as_ptr() as *mut libc::c_void,
                    self.file_capacity,
                    libc::MADV_SEQUENTIAL,
                );
            }
        }
    }

    /// Flush dirty pages to disk.
    ///
    /// # Errors
    ///
    /// Returns `StateError::Io` if msync fails.
    pub fn flush(&mut self) -> Result<(), StateError> {
        self.mmap.flush().map_err(StateError::Io)
    }

    /// Compact the store by rewriting live entries.
    ///
    /// Removes tombstones and unreachable data entries, reducing file
    /// size and improving hash table performance.
    ///
    /// # Errors
    ///
    /// Returns `StateError::Io` if file operations fail.
    pub fn compact(&mut self) -> Result<(), StateError> {
        // ... implementation
        todo!()
    }

    /// Get the fragmentation ratio (dead space / total data space).
    #[must_use]
    pub fn fragmentation(&self) -> f64 {
        // ... implementation
        todo!()
    }

    /// Get the current load factor of the hash index.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn load_factor(&self) -> f64 {
        (self.entry_count + self.tombstone_count) as f64 / self.index_capacity as f64
    }

    /// Get the path to the backing file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}
```

### Hash Index Operations

```rust
impl MmapStateStore {
    /// Look up a key in the hash index using linear probing.
    ///
    /// Returns the data region offset if found, None otherwise.
    ///
    /// # Algorithm
    ///
    /// 1. Compute xxhash64 of key bytes
    /// 2. Start at slot = hash & index_mask
    /// 3. Linear probe: compare key_hash, if match, verify full key
    /// 4. Skip tombstones, stop at empty slots
    #[inline]
    fn index_lookup(&self, key: &[u8]) -> Option<usize> {
        let key_hash = xxhash_rust::xxh3::xxh3_64(key);
        // Avoid hash == 0 (empty sentinel) or u64::MAX (tombstone)
        let key_hash = if key_hash == EMPTY_HASH || key_hash == TOMBSTONE_HASH {
            key_hash.wrapping_add(1)
        } else {
            key_hash
        };

        let mut slot = (key_hash & self.index_mask) as usize;

        loop {
            let slot_offset = HEADER_SIZE + slot * INDEX_SLOT_SIZE;
            let stored_hash = u64::from_le_bytes(
                self.mmap[slot_offset..slot_offset + 8].try_into().unwrap()
            );
            let data_ptr = u64::from_le_bytes(
                self.mmap[slot_offset + 8..slot_offset + 16].try_into().unwrap()
            );

            if stored_hash == EMPTY_HASH {
                // Empty slot: key not found
                return None;
            }

            if stored_hash == key_hash && stored_hash != TOMBSTONE_HASH {
                // Hash match: verify full key in data region
                let data_offset = data_ptr as usize;
                let entry_key = self.read_entry_key(data_offset);
                if entry_key == key {
                    return Some(data_offset);
                }
            }

            // Linear probe to next slot
            slot = (slot + 1) & (self.index_capacity - 1);
        }
    }

    /// Read the key bytes from a data region entry.
    #[inline]
    fn read_entry_key(&self, data_offset: usize) -> &[u8] {
        let abs_offset = self.data_offset + data_offset;
        let key_len = u32::from_le_bytes(
            self.mmap[abs_offset..abs_offset + 4].try_into().unwrap()
        ) as usize;
        &self.mmap[abs_offset + 4..abs_offset + 4 + key_len]
    }

    /// Read the value bytes from a data region entry.
    #[inline]
    fn read_entry_value(&self, data_offset: usize, key_len: usize) -> &[u8] {
        let abs_offset = self.data_offset + data_offset;
        let val_offset = abs_offset + 4 + key_len;
        let val_len = u32::from_le_bytes(
            self.mmap[val_offset..val_offset + 4].try_into().unwrap()
        ) as usize;
        &self.mmap[val_offset + 4..val_offset + 4 + val_len]
    }

    /// Insert or update a hash index slot.
    ///
    /// Uses linear probing to find an empty or matching slot.
    /// If load factor exceeds MAX_LOAD_FACTOR, triggers resize.
    #[inline]
    fn index_insert(
        &mut self,
        key: &[u8],
        key_hash: u64,
        data_ptr: u64,
    ) -> Result<bool, StateError> {
        // Check if resize needed
        #[allow(clippy::cast_precision_loss)]
        if (self.entry_count + self.tombstone_count + 1) as f64
            / self.index_capacity as f64
            > MAX_LOAD_FACTOR
        {
            self.resize_index()?;
        }

        let mut slot = (key_hash & self.index_mask) as usize;
        let mut first_tombstone: Option<usize> = None;

        loop {
            let slot_offset = HEADER_SIZE + slot * INDEX_SLOT_SIZE;
            let stored_hash = u64::from_le_bytes(
                self.mmap[slot_offset..slot_offset + 8].try_into().unwrap()
            );

            if stored_hash == EMPTY_HASH {
                // Use tombstone slot if we found one, otherwise use this empty slot
                let insert_slot = first_tombstone.unwrap_or(slot);
                let insert_offset = HEADER_SIZE + insert_slot * INDEX_SLOT_SIZE;
                self.mmap[insert_offset..insert_offset + 8]
                    .copy_from_slice(&key_hash.to_le_bytes());
                self.mmap[insert_offset + 8..insert_offset + 16]
                    .copy_from_slice(&data_ptr.to_le_bytes());
                if first_tombstone.is_some() {
                    self.tombstone_count -= 1;
                }
                return Ok(false); // new entry
            }

            if stored_hash == TOMBSTONE_HASH && first_tombstone.is_none() {
                first_tombstone = Some(slot);
            }

            if stored_hash == key_hash {
                // Hash match: verify full key
                let existing_ptr = u64::from_le_bytes(
                    self.mmap[slot_offset + 8..slot_offset + 16].try_into().unwrap()
                ) as usize;
                let existing_key = self.read_entry_key(existing_ptr);
                if existing_key == key {
                    // Overwrite: update data pointer
                    self.mmap[slot_offset + 8..slot_offset + 16]
                        .copy_from_slice(&data_ptr.to_le_bytes());
                    return Ok(true); // updated existing
                }
            }

            slot = (slot + 1) & (self.index_capacity - 1);
        }
    }

    /// Resize the hash index to double its current capacity.
    fn resize_index(&mut self) -> Result<(), StateError> {
        let new_capacity = self.index_capacity * 2;
        let new_index_size = new_capacity * INDEX_SLOT_SIZE;
        let new_data_offset = HEADER_SIZE + new_index_size;

        // Calculate new file size
        let data_size = self.data_write_pos;
        let new_file_size = new_data_offset + data_size;

        // Grow file
        self.file.set_len(new_file_size as u64)?;

        // SAFETY: File has been resized, we hold exclusive access.
        #[allow(unsafe_code)]
        let new_mmap = unsafe { MmapMut::map_mut(&self.file)? };

        // Copy data region to new offset
        // (must be done carefully to avoid overlap)
        let old_data_offset = self.data_offset;
        let old_mmap = std::mem::replace(&mut self.mmap, new_mmap);

        // Copy data region
        self.mmap[new_data_offset..new_data_offset + data_size]
            .copy_from_slice(&old_mmap[old_data_offset..old_data_offset + data_size]);

        // Clear new index region
        self.mmap[HEADER_SIZE..HEADER_SIZE + new_index_size].fill(0);

        // Rehash all entries
        self.index_capacity = new_capacity;
        self.index_mask = (new_capacity - 1) as u64;
        self.data_offset = new_data_offset;
        self.tombstone_count = 0;

        // Scan data region and re-insert all live entries
        // ... (iterate through old index, re-insert non-tombstone entries)

        Ok(())
    }
}
```

### StateStore Implementation

```rust
impl StateStore for MmapStateStore {
    #[inline]
    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let data_offset = self.index_lookup(key)?;
        let key_len = u32::from_le_bytes(
            self.mmap[self.data_offset + data_offset
                ..self.data_offset + data_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        Some(self.read_entry_value(data_offset, key_len))
    }

    #[inline]
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateError> {
        // 1. Compute hash
        let key_hash = xxhash_rust::xxh3::xxh3_64(key);
        let key_hash = if key_hash == EMPTY_HASH || key_hash == TOMBSTONE_HASH {
            key_hash.wrapping_add(1)
        } else {
            key_hash
        };

        // 2. Write entry to data region (append-only)
        let entry_size = 4 + key.len() + 4 + value.len();
        let data_ptr = self.data_write_pos;
        let abs_offset = self.data_offset + data_ptr;

        // Ensure capacity
        if abs_offset + entry_size > self.file_capacity {
            self.grow_file(abs_offset + entry_size)?;
        }

        // Write: key_len | key | val_len | val
        self.mmap[abs_offset..abs_offset + 4]
            .copy_from_slice(&(key.len() as u32).to_le_bytes());
        self.mmap[abs_offset + 4..abs_offset + 4 + key.len()]
            .copy_from_slice(key);
        self.mmap[abs_offset + 4 + key.len()..abs_offset + 8 + key.len()]
            .copy_from_slice(&(value.len() as u32).to_le_bytes());
        self.mmap[abs_offset + 8 + key.len()..abs_offset + 8 + key.len() + value.len()]
            .copy_from_slice(value);

        self.data_write_pos += entry_size;

        // 3. Insert into hash index
        let was_update = self.index_insert(key, key_hash, data_ptr as u64)?;

        // 4. Update counters
        if was_update {
            // Overwrite: old entry becomes dead space (fragmentation)
            // size_bytes delta: only value size changes
            // Note: we don't track old value size here, approximate
            self.size_bytes += value.len(); // simplified
        } else {
            self.entry_count += 1;
            self.size_bytes += key.len() + value.len();
        }

        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        let key_hash = xxhash_rust::xxh3::xxh3_64(key);
        let key_hash = if key_hash == EMPTY_HASH || key_hash == TOMBSTONE_HASH {
            key_hash.wrapping_add(1)
        } else {
            key_hash
        };

        let mut slot = (key_hash & self.index_mask) as usize;

        loop {
            let slot_offset = HEADER_SIZE + slot * INDEX_SLOT_SIZE;
            let stored_hash = u64::from_le_bytes(
                self.mmap[slot_offset..slot_offset + 8].try_into().unwrap()
            );

            if stored_hash == EMPTY_HASH {
                return Ok(()); // Key not found, idempotent
            }

            if stored_hash == key_hash {
                let data_ptr = u64::from_le_bytes(
                    self.mmap[slot_offset + 8..slot_offset + 16].try_into().unwrap()
                ) as usize;
                let entry_key = self.read_entry_key(data_ptr);
                if entry_key == key {
                    // Mark as tombstone
                    self.mmap[slot_offset..slot_offset + 8]
                        .copy_from_slice(&TOMBSTONE_HASH.to_le_bytes());
                    self.entry_count -= 1;
                    self.tombstone_count += 1;
                    self.size_bytes -= key.len(); // approximate
                    return Ok(());
                }
            }

            slot = (slot + 1) & (self.index_capacity - 1);
        }
    }

    fn range<'a>(&'a self, range: Range<&[u8]>) -> StateIter<'a> {
        // For range scans, we must scan all entries and filter.
        // This is O(n) for hash-based storage. For workloads that
        // need frequent range scans, use InMemoryStateStore instead.
        //
        // Optimization: maintain a sorted key list for range access.
        // Deferred to a future enhancement.
        Box::new(
            MmapRangeIter::new(self, range.start.to_vec(), range.end.to_vec())
        )
    }

    fn snapshot(&self) -> Box<dyn StateSnapshot> {
        // Delegate to the pluggable snapshot strategy (F-STATE-004).
        // Default: iterate all entries, produce FullStateSnapshot.
        self.apply_sequential_access_hints();

        let mut entries = Vec::with_capacity(self.entry_count);
        // Scan hash index for live entries
        for slot in 0..self.index_capacity {
            let slot_offset = HEADER_SIZE + slot * INDEX_SLOT_SIZE;
            let stored_hash = u64::from_le_bytes(
                self.mmap[slot_offset..slot_offset + 8].try_into().unwrap()
            );
            if stored_hash != EMPTY_HASH && stored_hash != TOMBSTONE_HASH {
                let data_ptr = u64::from_le_bytes(
                    self.mmap[slot_offset + 8..slot_offset + 16].try_into().unwrap()
                ) as usize;
                let key = self.read_entry_key(data_ptr).to_vec();
                let key_len = key.len();
                let value = self.read_entry_value(data_ptr, key_len).to_vec();
                entries.push((key, value));
            }
        }

        self.apply_random_access_hints();

        Box::new(FullStateSnapshot::with_epoch(entries, self.snapshot_epoch))
    }

    fn restore(&mut self, snapshot: &dyn StateSnapshot) -> Result<(), StateError> {
        // Clear everything
        self.mmap[HEADER_SIZE..HEADER_SIZE + self.index_capacity * INDEX_SLOT_SIZE].fill(0);
        self.data_write_pos = 0;
        self.entry_count = 0;
        self.tombstone_count = 0;
        self.size_bytes = 0;
        self.snapshot_epoch = snapshot.epoch();

        // Re-insert all entries from snapshot
        for (key, value) in snapshot.iter() {
            self.put(&key, &value)?;
        }

        Ok(())
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.index_lookup(key).is_some()
    }

    fn len(&self) -> usize {
        self.entry_count
    }

    fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    fn clear(&mut self) {
        self.mmap[HEADER_SIZE..HEADER_SIZE + self.index_capacity * INDEX_SLOT_SIZE].fill(0);
        self.data_write_pos = 0;
        self.entry_count = 0;
        self.tombstone_count = 0;
        self.size_bytes = 0;
    }
}
```

### Data Structures

```rust
/// Iterator over a range of entries in the mmap store.
///
/// Since the hash index is unordered, this collects matching entries
/// into a sorted buffer. For large stores, consider using
/// `InMemoryStateStore` if range scans are frequent.
struct MmapRangeIter<'a> {
    /// Collected and sorted entries within the range.
    entries: Vec<(&'a [u8], &'a [u8])>,
    /// Current position in the sorted entries.
    pos: usize,
}

impl<'a> MmapRangeIter<'a> {
    fn new(store: &'a MmapStateStore, start: Vec<u8>, end: Vec<u8>) -> Self {
        let mut entries = Vec::new();

        // Scan all hash index slots for entries in range
        for slot in 0..store.index_capacity {
            let slot_offset = HEADER_SIZE + slot * INDEX_SLOT_SIZE;
            let stored_hash = u64::from_le_bytes(
                store.mmap[slot_offset..slot_offset + 8].try_into().unwrap()
            );
            if stored_hash != EMPTY_HASH && stored_hash != TOMBSTONE_HASH {
                let data_ptr = u64::from_le_bytes(
                    store.mmap[slot_offset + 8..slot_offset + 16].try_into().unwrap()
                ) as usize;
                let key = store.read_entry_key(data_ptr);
                if key >= start.as_slice() && key < end.as_slice() {
                    let key_len = key.len();
                    let value = store.read_entry_value(data_ptr, key_len);
                    entries.push((key, value));
                }
            }
        }

        // Sort by key for ordered iteration
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        Self { entries, pos: 0 }
    }
}

impl<'a> Iterator for MmapRangeIter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.entries.len() {
            let entry = self.entries[self.pos];
            self.pos += 1;
            Some(entry)
        } else {
            None
        }
    }
}

/// Header structure for the mmap file.
///
/// This is the in-memory representation of the 64-byte file header.
/// It is read/written directly to the mmap region.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct MmapHeader {
    /// Magic number for file identification.
    magic: u64,
    /// File format version.
    version: u32,
    /// Number of live entries.
    entry_count: u32,
    /// Byte offset to hash index start (always HEADER_SIZE).
    index_offset: u64,
    /// Byte offset to data region start.
    data_offset: u64,
    /// Total number of hash index slots.
    index_capacity: u64,
    /// Last checkpoint epoch.
    snapshot_epoch: u64,
    /// Reserved for future use.
    reserved: [u8; 8],
}
```

### Algorithm/Flow

#### Get Operation (< 200ns target)

```
1. Compute xxhash64 of key bytes                              (~10ns)
2. Compute slot = hash & index_mask                            (~1ns)
3. Read hash index slot from mmap:
   a. Load key_hash (u64) at mmap[HEADER + slot * 16]         (~5ns, L1 cache hit)
   b. If EMPTY: return None                                    (~1ns)
   c. If hash matches: read data_ptr (u64)                     (~5ns)
4. Follow data_ptr to data region:
   a. Read key_len (u32) at data_offset + data_ptr             (~5ns)
   b. Compare key bytes for exact match                        (~10-30ns)
   c. If match: read val_len + value bytes                     (~5ns)
   d. Return Some(&[u8]) pointing into mmap region             (0ns)
5. If no match: linear probe to next slot, goto 3              (+20ns per probe)
Total: ~40-80ns typical (1-2 probes at < 0.75 load factor)
```

#### Put Operation (< 300ns target)

```
1. Compute xxhash64 of key                                    (~10ns)
2. Append entry to data region:
   a. Write key_len | key | val_len | val to mmap              (~30-50ns)
   b. Advance data_write_pos                                   (~1ns)
3. Insert/update hash index slot:
   a. Linear probe to find empty/matching slot                 (~20-40ns)
   b. Write key_hash + data_ptr to slot                        (~10ns)
4. Update counters (entry_count, size_bytes)                   (~5ns)
Total: ~80-120ns typical
Note: May exceed 300ns if file growth is triggered (rare, amortized)
```

#### Recovery Flow (Process Crash)

```
1. Open existing mmap file                                     (~1ms)
2. Validate header: magic, version                             (~10ns)
3. Read index_capacity, data_offset from header                (~10ns)
4. mmap the file, hash index is immediately accessible         (~100us)
5. Resume operations -- no data replay needed
Total: ~1-5ms (dominated by mmap syscall and page faults)
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `StateError::Io` | mmap creation failure, file permission denied | Check permissions, retry with different path |
| `StateError::Io` | File growth failure (disk full) | Alert operator, trigger compaction, free disk space |
| `StateError::Corruption` | Invalid magic number in existing file | Delete file, restore from checkpoint |
| `StateError::Corruption` | Hash index inconsistency | Full rebuild from data region scan |
| `StateError::CapacityExceeded` | Index load factor > 0.95 after failed resize | Trigger emergency compaction |
| `SIGBUS` / Access violation | mmap page fault on truncated file | OS-level signal, requires process restart |
| `StateError::Serialization` | Snapshot serialization failure | Retry, log warning |

## Performance Targets

| Metric | Target | Method |
|--------|--------|--------|
| `get()` latency (warm cache) | < 200ns | `bench_mmap_get_warm` |
| `get()` latency (cold cache) | < 1us | `bench_mmap_get_cold` |
| `put()` latency | < 300ns | `bench_mmap_put` |
| `delete()` latency | < 200ns | `bench_mmap_delete` |
| `contains()` latency | < 150ns | `bench_mmap_contains` |
| Hash index probes (avg) | < 1.5 | Load factor monitoring |
| Process crash recovery | < 10ms | `bench_mmap_recovery` |
| Compaction throughput | > 100MB/s | `bench_mmap_compact` |
| File growth overhead | amortized O(1) | `bench_mmap_growth` |
| `get()` allocations | 0 | Allocation tracker test |
| xxhash64 throughput | > 10GB/s | `bench_xxhash` |

## Test Plan

### Unit Tests

- [ ] `test_mmap_open_creates_new_file`
- [ ] `test_mmap_open_reads_existing_file`
- [ ] `test_mmap_open_rejects_corrupt_magic`
- [ ] `test_mmap_open_rejects_wrong_version`
- [ ] `test_mmap_get_returns_none_for_missing_key`
- [ ] `test_mmap_get_returns_borrowed_slice_into_mmap`
- [ ] `test_mmap_put_new_key`
- [ ] `test_mmap_put_overwrite_existing_key`
- [ ] `test_mmap_delete_existing_key`
- [ ] `test_mmap_delete_missing_key_is_idempotent`
- [ ] `test_mmap_delete_tombstone_handling`
- [ ] `test_mmap_contains_works`
- [ ] `test_mmap_len_tracks_entries`
- [ ] `test_mmap_size_bytes_tracks_sizes`
- [ ] `test_mmap_clear_resets_everything`
- [ ] `test_mmap_hash_collision_handling`
- [ ] `test_mmap_linear_probing_wraps_around`
- [ ] `test_mmap_index_resize_triggers_at_load_factor`
- [ ] `test_mmap_index_resize_preserves_entries`
- [ ] `test_mmap_file_growth_on_data_region_full`
- [ ] `test_mmap_range_returns_ordered_entries`
- [ ] `test_mmap_range_empty_result`
- [ ] `test_mmap_snapshot_captures_all_entries`
- [ ] `test_mmap_restore_replaces_state`
- [ ] `test_mmap_compact_removes_tombstones`
- [ ] `test_mmap_compact_removes_dead_data`
- [ ] `test_mmap_fragmentation_calculation`
- [ ] `test_mmap_load_factor_calculation`
- [ ] `test_mmap_binary_keys_and_values`
- [ ] `test_mmap_large_values_100kb`
- [ ] `test_mmap_flush_persists_to_disk`

### Integration Tests

- [ ] `test_mmap_process_crash_recovery` -- write data, simulate crash (drop without flush), reopen, verify
- [ ] `test_mmap_snapshot_restore_cycle` -- put data, snapshot, mutate, restore, verify
- [ ] `test_mmap_with_window_operator` -- wire to tumbling window, process events
- [ ] `test_mmap_1m_entries` -- insert 1M entries, verify correctness
- [ ] `test_mmap_concurrent_snapshot_and_writes` -- snapshot while writes continue (via barrier)
- [ ] `test_mmap_compaction_under_load` -- compact while data is being read

### Benchmarks

- [ ] `bench_mmap_get_warm` -- 100K entries, warm cache, target < 200ns
- [ ] `bench_mmap_get_cold` -- 1M entries, cold cache (flush page cache), target < 1us
- [ ] `bench_mmap_put` -- sequential inserts, target < 300ns
- [ ] `bench_mmap_put_overwrite` -- overwrite existing, target < 300ns
- [ ] `bench_mmap_delete` -- delete existing, target < 200ns
- [ ] `bench_mmap_contains` -- target < 150ns
- [ ] `bench_mmap_recovery` -- 100K entries, reopen file, target < 10ms
- [ ] `bench_mmap_compact` -- 100K entries with 50% fragmentation, target > 100MB/s
- [ ] `bench_mmap_get_zero_alloc` -- verify 0 heap allocations
- [ ] `bench_xxhash_throughput` -- key hashing throughput
- [ ] `bench_mmap_snapshot_10k` -- 10K entries snapshot creation

## Rollout Plan

1. **Phase 1**: Define file format and header structure
2. **Phase 2**: Implement hash index operations (lookup, insert, delete, resize)
3. **Phase 3**: Implement `MmapStateStore::open()` with file creation and validation
4. **Phase 4**: Implement `StateStore` trait methods
5. **Phase 5**: Add platform-specific optimizations (MAP_POPULATE, madvise)
6. **Phase 6**: Implement compaction
7. **Phase 7**: Unit tests and integration tests
8. **Phase 8**: Benchmarks and optimization
9. **Phase 9**: Code review and merge

## Open Questions

- [ ] Should the hash index use open addressing (linear probing) or separate chaining? Linear probing is more cache-friendly but degrades at high load factors. Current design uses linear probing with 0.75 max load factor.
- [ ] Should we use Robin Hood hashing for more uniform probe distances? This would reduce worst-case lookup time but adds complexity to insert/delete.
- [ ] How to handle SIGBUS on Linux when the backing file is truncated by another process? We could install a signal handler, but that is complex and error-prone. Current assumption: exclusive file access.
- [ ] Should `range()` maintain a sorted index overlay (like InMemoryStateStore) or accept O(n) scan cost? For the mmap backend, range scans are expected to be rare (mostly point lookups).
- [ ] Should we support `MAP_HUGETLB` for large state stores? This could reduce TLB misses significantly but requires system configuration.

## Completion Checklist

- [ ] File format specification finalized
- [ ] Hash index implementation (lookup, insert, delete, resize)
- [ ] `MmapStateStore::open()` with creation and validation
- [ ] `StateStore` trait implementation complete
- [ ] Platform-specific optimizations (Linux madvise, MAP_POPULATE)
- [ ] Compaction implementation
- [ ] Flush and persistence
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets (< 200ns get, < 300ns put)
- [ ] Zero-allocation verification for `get()`
- [ ] Documentation complete with `#![deny(missing_docs)]`
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## References

- [F-STATE-001: StateStore Trait](./F-STATE-001-state-store-trait.md) -- trait definition
- [F-STATE-004: Pluggable Snapshots](./F-STATE-004-pluggable-snapshots.md) -- snapshot strategies
- [F002: Memory-Mapped State Store (Phase 1)](../../phase-1/F002-memory-mapped-state-store.md) -- predecessor
- [F003: State Store Interface (Phase 1)](../../phase-1/F003-state-store-interface.md) -- Phase 1 trait
- [xxhash-rust crate](https://crates.io/crates/xxhash-rust) -- xxhash64 implementation
- [memmap2 crate](https://crates.io/crates/memmap2) -- memory mapping
- [ARCHITECTURE.md](../../../ARCHITECTURE.md) -- Ring model design
- [Robin Hood Hashing](https://www.sebastiansylvan.com/post/robin-hood-hashing-should-be-your-default-hashing-strategy/) -- alternative probing strategy
