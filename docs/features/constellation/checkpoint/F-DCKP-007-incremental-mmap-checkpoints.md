# F-DCKP-007: Incremental Mmap Checkpoints

> **❌ SUPERSEDED (2026-02-17)** — This feature has been cancelled. Incremental mmap checkpoints relied on `MmapStateStore` (F-STATE-003) and `PluggableSnapshots` (F-STATE-004), both of which have been superseded. The mprotect/SIGSEGV dirty page tracking approach is no longer applicable since the state backend is now `FxHashMap`/`BTreeMap` in heap memory. Incremental checkpointing for the new architecture will use `rkyv` delta serialization — only changed state partitions are re-serialized and uploaded. This is simpler, portable (no Unix-only signal handlers), and eliminates the entire mmap complexity. See the [Storage Architecture Research](../../../research/storage-architecture.md) for the full analysis.
>
> **Superseded by:** F-DCKP-004 (ObjectStoreCheckpointer) with per-partition rkyv snapshots. Only partitions with mutations since the last checkpoint are re-serialized.

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DCKP-007 |
| **Status** | ❌ Superseded |
| **Priority** | ~P2~ N/A |
| **Phase** | 6c |
| **Effort** | ~XL (10-15 days)~ N/A |
| **Dependencies** | ~F-STATE-003 (Mmap State Backend), F-STATE-004 (State Snapshot Interface)~ N/A (both superseded) |
| **Blocks** | None |
| **Owner** | N/A |
| **Created** | 2026-02-16 |
| **Superseded** | 2026-02-17 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/state/incremental.rs` |

## Summary

Implements incremental mmap-based checkpoints that only upload dirty pages since the last checkpoint, reducing checkpoint bandwidth by 90%+ for large state with low mutation rates. Uses `mprotect()` to mark state pages as read-only after a checkpoint, then catches `SIGSEGV` / `SIGBUS` signals on writes to identify dirty pages. Only modified pages are serialized and uploaded as deltas. During recovery, the base snapshot is loaded and deltas are applied in sequence. This is the `MmapSnapshotStrategy::IncrementalDirtyPages` variant that extends the existing mmap state backend.

## Goals

- Track dirty pages in mmap-backed state stores using OS memory protection
- Upload only modified pages (deltas) during checkpoint, not the entire state
- Reduce checkpoint upload bandwidth by 90%+ for large, sparsely-mutated state
- Delta format that supports efficient append and sequential apply
- Recovery by loading base snapshot + applying delta chain
- Periodic full snapshots to bound delta chain length
- Configurable delta chain limit before compaction to full snapshot
- Thread-safe signal handler for `SIGSEGV`/`SIGBUS` dirty page tracking

## Non-Goals

- Implementing the mmap state backend itself (covered by F-STATE-003)
- Object store upload/download (covered by F-DCKP-004)
- Copy-on-write via `fork()` (alternative approach, not used here)
- Page-level compression (deferred optimization)
- Supporting non-mmap state backends (heap, RocksDB use their own incremental strategies)
- Windows support in initial implementation (Unix only for mprotect/signal handler)

## Technical Design

### Architecture

**Ring**: Ring 0 (dirty page tracking is on the hot path); Ring 2 (delta upload is async).

**Crate**: `laminar-core`

**Module**: `laminar-core/src/state/incremental.rs`

The incremental checkpoint strategy sits between the state store and the checkpointer. After each checkpoint, it marks all mmap pages as read-only using `mprotect()`. When the operator writes to state, the CPU raises `SIGSEGV`. A custom signal handler catches this, marks the page as dirty in a bitset, re-enables write access for that page, and returns. At checkpoint time, only dirty pages are serialized and uploaded.

```
┌────────────────────────────────────────────────────────────────────┐
│  Operator (Ring 0)                                                  │
│                                                                     │
│  state_store.put(key, value)                                        │
│    │                                                                │
│    ▼                                                                │
│  ┌─────────────────────────────────────────────────┐               │
│  │  Mmap State Region                               │               │
│  │  [Page 0][Page 1][Page 2]...[Page N]             │               │
│  │    RO      RO      RW       RO                   │               │
│  │                     ↑                             │               │
│  │              Write triggers SIGSEGV               │               │
│  │              Signal handler:                      │               │
│  │                1. Mark page dirty in bitset       │               │
│  │                2. mprotect(page, PROT_READ|WRITE) │               │
│  │                3. Return (write proceeds)         │               │
│  └─────────────────────────────────────────────────┘               │
│                                                                     │
│  At checkpoint time:                                                │
│  ┌─────────────────────────────────────────────────┐               │
│  │  DirtyPageTracker                                │               │
│  │  bitset: [0, 0, 1, 0, 0, 1, ...]               │               │
│  │                                                   │               │
│  │  Only pages 2 and 5 are dirty → upload 2 pages   │               │
│  │  Instead of N pages (full snapshot)               │               │
│  └─────────────────────────────────────────────────┘               │
│                                                                     │
│  After checkpoint:                                                  │
│    mprotect(all pages, PROT_READ) → reset tracking                 │
└────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Page size used for dirty tracking (matches OS page size).
pub const PAGE_SIZE: usize = 4096;

/// Strategy for mmap-based snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MmapSnapshotStrategy {
    /// Full snapshot every time (simple, high bandwidth).
    Full,
    /// Incremental: only upload dirty pages since last checkpoint.
    IncrementalDirtyPages,
    /// Copy-on-write via fork (Unix only, not yet implemented).
    CopyOnWrite,
}

/// Configuration for incremental mmap checkpoints.
#[derive(Debug, Clone)]
pub struct IncrementalMmapConfig {
    /// Snapshot strategy.
    pub strategy: MmapSnapshotStrategy,

    /// Maximum number of deltas before a full snapshot is forced.
    /// This bounds recovery time (at most N deltas to apply).
    pub max_delta_chain_length: usize,

    /// Force a full snapshot every N checkpoints regardless of chain length.
    pub full_snapshot_interval: usize,

    /// Page size for dirty tracking (must match OS page size).
    pub page_size: usize,

    /// Whether to verify delta integrity with checksums.
    pub verify_deltas: bool,
}

impl Default for IncrementalMmapConfig {
    fn default() -> Self {
        Self {
            strategy: MmapSnapshotStrategy::IncrementalDirtyPages,
            max_delta_chain_length: 10,
            full_snapshot_interval: 100,
            page_size: PAGE_SIZE,
            verify_deltas: true,
        }
    }
}

/// The incremental mmap checkpoint manager.
///
/// Tracks dirty pages and produces delta snapshots.
pub trait IncrementalSnapshotProvider: Send + Sync {
    /// Take a snapshot: either full or incremental delta.
    ///
    /// Returns the snapshot type and serialized data.
    fn take_snapshot(&mut self) -> Result<IncrementalSnapshot, IncrementalError>;

    /// Restore state from a base snapshot plus a chain of deltas.
    fn restore(
        &mut self,
        base: &[u8],
        deltas: &[DeltaSnapshot],
    ) -> Result<(), IncrementalError>;

    /// Force the next snapshot to be a full snapshot.
    fn force_full_snapshot(&mut self);

    /// Returns the number of deltas since the last full snapshot.
    fn delta_chain_length(&self) -> usize;

    /// Returns the total number of dirty pages since the last snapshot.
    fn dirty_page_count(&self) -> usize;

    /// Returns metrics for observability.
    fn metrics(&self) -> IncrementalMetrics;
}

/// A snapshot: either full or incremental delta.
#[derive(Debug)]
pub enum IncrementalSnapshot {
    /// Full snapshot of the entire state.
    Full {
        /// Complete state data.
        data: Vec<u8>,
        /// Total number of pages.
        total_pages: usize,
    },
    /// Incremental delta: only dirty pages.
    Delta(DeltaSnapshot),
}

impl IncrementalSnapshot {
    /// Returns the serialized size in bytes.
    pub fn size_bytes(&self) -> usize {
        match self {
            IncrementalSnapshot::Full { data, .. } => data.len(),
            IncrementalSnapshot::Delta(d) => d.size_bytes(),
        }
    }

    /// Returns true if this is a full snapshot.
    pub fn is_full(&self) -> bool {
        matches!(self, IncrementalSnapshot::Full { .. })
    }
}
```

### Data Structures

```rust
/// A delta snapshot containing only dirty pages.
#[derive(Debug, Clone)]
pub struct DeltaSnapshot {
    /// Sequence number of this delta (1-based, relative to base).
    pub sequence: u32,

    /// The checkpoint ID of the base (full) snapshot this delta applies to.
    pub base_checkpoint_id: u64,

    /// Dirty page entries: (page_index, page_data).
    pub pages: Vec<DirtyPage>,

    /// SHA-256 checksum of the delta for integrity verification.
    pub checksum: String,
}

impl DeltaSnapshot {
    /// Total size of this delta in bytes.
    pub fn size_bytes(&self) -> usize {
        self.pages.iter().map(|p| p.data.len()).sum::<usize>()
            + self.pages.len() * std::mem::size_of::<u64>() // page indices
    }
}

/// A single dirty page entry.
#[derive(Debug, Clone)]
pub struct DirtyPage {
    /// Page index within the mmap region.
    pub page_index: u64,
    /// Raw page data (PAGE_SIZE bytes).
    pub data: Vec<u8>,
}

/// Delta serialization format for storage.
///
/// Binary format:
/// ```text
/// [magic: u32][version: u16][sequence: u32][base_id: u64]
/// [num_pages: u64]
/// [page_index: u64][page_data: PAGE_SIZE bytes] * num_pages
/// [checksum: 32 bytes (SHA-256)]
/// ```
pub struct DeltaFormat;

impl DeltaFormat {
    /// Magic number for delta files.
    pub const MAGIC: u32 = 0x4C444250; // "LDBP"

    /// Current format version.
    pub const VERSION: u16 = 1;

    /// Serialize a delta snapshot to bytes.
    pub fn serialize(delta: &DeltaSnapshot) -> Vec<u8> {
        let header_size = 4 + 2 + 4 + 8 + 8; // magic + version + seq + base_id + num_pages
        let pages_size = delta.pages.len() * (8 + PAGE_SIZE); // index + data per page
        let checksum_size = 32;

        let mut buf = Vec::with_capacity(header_size + pages_size + checksum_size);

        buf.extend_from_slice(&Self::MAGIC.to_le_bytes());
        buf.extend_from_slice(&Self::VERSION.to_le_bytes());
        buf.extend_from_slice(&delta.sequence.to_le_bytes());
        buf.extend_from_slice(&delta.base_checkpoint_id.to_le_bytes());
        buf.extend_from_slice(&(delta.pages.len() as u64).to_le_bytes());

        for page in &delta.pages {
            buf.extend_from_slice(&page.page_index.to_le_bytes());
            buf.extend_from_slice(&page.data);
        }

        // Checksum is computed over all preceding bytes
        use sha2::{Sha256, Digest};
        let hash = Sha256::digest(&buf);
        buf.extend_from_slice(&hash);

        buf
    }

    /// Deserialize a delta snapshot from bytes.
    pub fn deserialize(data: &[u8]) -> Result<DeltaSnapshot, IncrementalError> {
        if data.len() < 26 { // minimum header size
            return Err(IncrementalError::CorruptDelta("data too short".into()));
        }

        let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
        if magic != Self::MAGIC {
            return Err(IncrementalError::CorruptDelta("invalid magic number".into()));
        }

        let version = u16::from_le_bytes(data[4..6].try_into().unwrap());
        if version != Self::VERSION {
            return Err(IncrementalError::UnsupportedDeltaVersion(version));
        }

        let sequence = u32::from_le_bytes(data[6..10].try_into().unwrap());
        let base_checkpoint_id = u64::from_le_bytes(data[10..18].try_into().unwrap());
        let num_pages = u64::from_le_bytes(data[18..26].try_into().unwrap()) as usize;

        let mut offset = 26;
        let mut pages = Vec::with_capacity(num_pages);

        for _ in 0..num_pages {
            if offset + 8 + PAGE_SIZE > data.len() - 32 {
                return Err(IncrementalError::CorruptDelta("truncated page data".into()));
            }
            let page_index = u64::from_le_bytes(
                data[offset..offset + 8].try_into().unwrap(),
            );
            offset += 8;
            let page_data = data[offset..offset + PAGE_SIZE].to_vec();
            offset += PAGE_SIZE;
            pages.push(DirtyPage { page_index, data: page_data });
        }

        // Verify checksum
        use sha2::{Sha256, Digest};
        let expected_hash = &data[data.len() - 32..];
        let computed_hash = Sha256::digest(&data[..data.len() - 32]);
        if computed_hash.as_slice() != expected_hash {
            return Err(IncrementalError::ChecksumMismatch);
        }

        Ok(DeltaSnapshot {
            sequence,
            base_checkpoint_id,
            pages,
            checksum: hex::encode(expected_hash),
        })
    }
}

/// Tracks dirty pages using mprotect() and signal handling.
///
/// # Safety
///
/// This struct installs a SIGSEGV signal handler. Only one
/// `DirtyPageTracker` should be active per mmap region.
pub struct DirtyPageTracker {
    /// Base address of the mmap region.
    base_addr: *mut u8,

    /// Total size of the mmap region in bytes.
    region_size: usize,

    /// Number of pages in the region.
    num_pages: usize,

    /// Bitset tracking which pages are dirty.
    /// Stored in a shared atomic array so the signal handler can
    /// access it safely.
    dirty_bitset: Arc<Vec<AtomicU64>>,

    /// Number of dirty pages (cached, updated lazily).
    dirty_count: usize,

    /// Whether tracking is currently active (pages are read-only).
    is_tracking: bool,

    /// Configuration.
    config: IncrementalMmapConfig,

    /// Delta chain length since last full snapshot.
    chain_length: usize,

    /// Checkpoint counter for full_snapshot_interval.
    checkpoint_counter: usize,

    /// Whether to force a full snapshot on next take_snapshot().
    force_full: bool,
}

// SAFETY: The DirtyPageTracker manages mmap memory that is shared
// with the signal handler. The signal handler only writes to atomic
// locations in the dirty_bitset. The tracker itself is only used
// from a single thread (the operator thread).
unsafe impl Send for DirtyPageTracker {}
unsafe impl Sync for DirtyPageTracker {}

impl DirtyPageTracker {
    /// Create a new dirty page tracker for the given mmap region.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `base_addr` points to a valid mmap
    /// region of at least `region_size` bytes, and that the region
    /// remains valid for the lifetime of this tracker.
    pub unsafe fn new(
        base_addr: *mut u8,
        region_size: usize,
        config: IncrementalMmapConfig,
    ) -> Self {
        let num_pages = (region_size + config.page_size - 1) / config.page_size;
        let bitset_words = (num_pages + 63) / 64;
        let dirty_bitset: Vec<AtomicU64> = (0..bitset_words)
            .map(|_| AtomicU64::new(0))
            .collect();

        Self {
            base_addr,
            region_size,
            num_pages,
            dirty_bitset: Arc::new(dirty_bitset),
            dirty_count: 0,
            is_tracking: false,
            config,
            chain_length: 0,
            checkpoint_counter: 0,
            force_full: false,
        }
    }

    /// Enable dirty page tracking by marking all pages as read-only.
    ///
    /// After this call, any write to the mmap region will trigger
    /// SIGSEGV, which the signal handler catches to mark the page dirty
    /// and re-enable write access.
    ///
    /// # Safety
    ///
    /// Must be called from the operator thread. The signal handler
    /// must be installed before calling this method.
    pub unsafe fn enable_tracking(&mut self) -> Result<(), IncrementalError> {
        // Clear dirty bitset
        for word in self.dirty_bitset.iter() {
            word.store(0, Ordering::Relaxed);
        }
        self.dirty_count = 0;

        // Mark all pages as read-only
        // SAFETY: base_addr is a valid mmap region of region_size bytes
        let result = libc::mprotect(
            self.base_addr as *mut libc::c_void,
            self.region_size,
            libc::PROT_READ,
        );

        if result != 0 {
            return Err(IncrementalError::MprotectFailed(
                std::io::Error::last_os_error(),
            ));
        }

        self.is_tracking = true;
        Ok(())
    }

    /// Collect dirty pages and produce a delta or full snapshot.
    pub fn collect_snapshot(&mut self, full_state: &[u8]) -> Result<IncrementalSnapshot, IncrementalError> {
        self.checkpoint_counter += 1;

        // Decide: full or incremental?
        let do_full = self.force_full
            || self.chain_length >= self.config.max_delta_chain_length
            || self.checkpoint_counter % self.config.full_snapshot_interval == 0
            || !self.is_tracking;

        if do_full {
            self.chain_length = 0;
            self.force_full = false;
            return Ok(IncrementalSnapshot::Full {
                total_pages: self.num_pages,
                data: full_state.to_vec(),
            });
        }

        // Collect dirty pages
        let mut dirty_pages = Vec::new();
        for word_idx in 0..self.dirty_bitset.len() {
            let bits = self.dirty_bitset[word_idx].load(Ordering::Acquire);
            if bits == 0 {
                continue;
            }
            for bit in 0..64u64 {
                if bits & (1 << bit) != 0 {
                    let page_index = (word_idx * 64 + bit as usize) as u64;
                    if (page_index as usize) < self.num_pages {
                        let offset = page_index as usize * self.config.page_size;
                        let end = (offset + self.config.page_size).min(self.region_size);
                        // SAFETY: reading from valid mmap region within bounds
                        let data = unsafe {
                            std::slice::from_raw_parts(
                                self.base_addr.add(offset),
                                end - offset,
                            )
                        };
                        dirty_pages.push(DirtyPage {
                            page_index,
                            data: data.to_vec(),
                        });
                    }
                }
            }
        }

        self.dirty_count = dirty_pages.len();
        self.chain_length += 1;

        // Compute checksum
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        for page in &dirty_pages {
            hasher.update(&page.page_index.to_le_bytes());
            hasher.update(&page.data);
        }
        let checksum = hex::encode(hasher.finalize());

        Ok(IncrementalSnapshot::Delta(DeltaSnapshot {
            sequence: self.chain_length as u32,
            base_checkpoint_id: 0, // Set by caller
            pages: dirty_pages,
            checksum,
        }))
    }

    /// Apply a chain of deltas to a base snapshot to produce full state.
    pub fn apply_deltas(
        base: &mut [u8],
        deltas: &[DeltaSnapshot],
        page_size: usize,
    ) -> Result<(), IncrementalError> {
        // Apply deltas in sequence order
        let mut sorted_deltas: Vec<&DeltaSnapshot> = deltas.iter().collect();
        sorted_deltas.sort_by_key(|d| d.sequence);

        for delta in sorted_deltas {
            for page in &delta.pages {
                let offset = page.page_index as usize * page_size;
                let end = offset + page.data.len();

                if end > base.len() {
                    return Err(IncrementalError::DeltaOutOfBounds {
                        page_index: page.page_index,
                        offset,
                        region_size: base.len(),
                    });
                }

                base[offset..end].copy_from_slice(&page.data);
            }
        }

        Ok(())
    }

    /// Returns the number of dirty pages since tracking was enabled.
    pub fn dirty_page_count(&self) -> usize {
        let mut count = 0;
        for word in self.dirty_bitset.iter() {
            count += word.load(Ordering::Relaxed).count_ones() as usize;
        }
        count
    }
}

/// Signal handler context for dirty page tracking.
///
/// This is stored in a global static so the SIGSEGV handler can access it.
/// Only one active tracker per process is supported.
///
/// # Safety
///
/// The signal handler runs in an async-signal-safe context. It only
/// performs atomic stores and mprotect() calls, both of which are
/// async-signal-safe.
pub struct SignalHandlerContext {
    /// Base address of the tracked mmap region.
    pub base_addr: usize,
    /// Size of the tracked region.
    pub region_size: usize,
    /// Page size.
    pub page_size: usize,
    /// Dirty bitset (atomic, signal-safe).
    pub dirty_bitset: Arc<Vec<AtomicU64>>,
}

/// Metrics for incremental checkpoints.
#[derive(Debug, Clone)]
pub struct IncrementalMetrics {
    /// Total full snapshots taken.
    pub full_snapshots: u64,
    /// Total delta snapshots taken.
    pub delta_snapshots: u64,
    /// Total dirty pages tracked.
    pub total_dirty_pages: u64,
    /// Total pages in region.
    pub total_pages: usize,
    /// Current delta chain length.
    pub current_chain_length: usize,
    /// Bytes saved by incremental (vs full) snapshots.
    pub bytes_saved: u64,
    /// Dirty page ratio (0.0 to 1.0) for the last delta.
    pub last_dirty_ratio: f64,
}

/// Errors from incremental checkpoint operations.
#[derive(Debug, thiserror::Error)]
pub enum IncrementalError {
    /// mprotect() system call failed.
    #[error("mprotect failed: {0}")]
    MprotectFailed(std::io::Error),

    /// Signal handler installation failed.
    #[error("failed to install signal handler: {0}")]
    SignalHandlerFailed(String),

    /// Delta data is corrupt.
    #[error("corrupt delta: {0}")]
    CorruptDelta(String),

    /// Delta format version not supported.
    #[error("unsupported delta version: {0}")]
    UnsupportedDeltaVersion(u16),

    /// SHA-256 checksum mismatch on delta.
    #[error("delta checksum mismatch")]
    ChecksumMismatch,

    /// Delta references a page outside the mmap region.
    #[error("delta page {page_index} at offset {offset} exceeds region size {region_size}")]
    DeltaOutOfBounds {
        page_index: u64,
        offset: usize,
        region_size: usize,
    },

    /// Delta chain is too long for efficient recovery.
    #[error("delta chain length {0} exceeds maximum")]
    ChainTooLong(usize),
}
```

### Algorithm/Flow

#### Dirty Page Tracking Flow

```
1. SETUP (after checkpoint or on startup):
   a. Clear dirty bitset to all zeros
   b. mprotect(mmap_region, PROT_READ) → all pages read-only
   c. Install SIGSEGV handler (if not already installed)

2. WRITE (operator writes to state):
   a. CPU raises SIGSEGV (write to read-only page)
   b. Signal handler fires:
      i.   Compute page_index = (fault_addr - base_addr) / PAGE_SIZE
      ii.  Set bit in dirty_bitset: bitset[page_index / 64] |= (1 << (page_index % 64))
      iii. mprotect(page_addr, PAGE_SIZE, PROT_READ | PROT_WRITE)
      iv.  Return (write instruction retries and succeeds)

3. CHECKPOINT (barrier received):
   a. Count dirty pages from bitset
   b. If should_do_full_snapshot():
      - Serialize entire mmap region
      - Reset chain_length to 0
   c. Else:
      - For each dirty page: copy page data into DeltaSnapshot
      - Increment chain_length
   d. Reset: mprotect(all pages, PROT_READ), clear bitset
   e. Upload snapshot (full or delta) to object store

4. RECOVERY:
   a. Download base (full) snapshot
   b. Download all deltas in the chain
   c. Load base into mmap region
   d. Apply deltas in sequence order (overwrite dirty pages)
   e. State is now at the checkpoint point
```

#### Signal Handler (Pseudo-code)

```rust
// SAFETY: This function is async-signal-safe. It only uses
// atomic operations and mprotect(), both of which are safe
// to call from a signal handler context.
extern "C" fn sigsegv_handler(
    sig: libc::c_int,
    info: *mut libc::siginfo_t,
    _context: *mut libc::c_void,
) {
    // Only handle SIGSEGV
    if sig != libc::SIGSEGV {
        return;
    }

    let fault_addr = unsafe { (*info).si_addr() } as usize;
    let ctx = get_global_context(); // thread-local or global static

    // Check if fault is within our tracked region
    if fault_addr < ctx.base_addr
        || fault_addr >= ctx.base_addr + ctx.region_size
    {
        // Not our fault; chain to previous handler or abort
        std::process::abort();
    }

    // Compute page index
    let page_index = (fault_addr - ctx.base_addr) / ctx.page_size;

    // Mark page as dirty (atomic, signal-safe)
    let word_index = page_index / 64;
    let bit_index = page_index % 64;
    ctx.dirty_bitset[word_index].fetch_or(1 << bit_index, Ordering::Relaxed);

    // Re-enable write access for this page
    let page_addr = ctx.base_addr + page_index * ctx.page_size;
    unsafe {
        libc::mprotect(
            page_addr as *mut libc::c_void,
            ctx.page_size,
            libc::PROT_READ | libc::PROT_WRITE,
        );
    }
    // Return: the faulting instruction will be retried and succeed
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `MprotectFailed` | OS rejected mprotect call (permissions, invalid address) | Fall back to full snapshot mode |
| `SignalHandlerFailed` | Could not install SIGSEGV handler | Fall back to full snapshot mode |
| `CorruptDelta` | Delta file is malformed or truncated | Skip delta, try recovery from previous full snapshot |
| `ChecksumMismatch` | Delta data corrupted in storage or transit | Re-download; if persistent, skip delta chain and use previous full |
| `DeltaOutOfBounds` | Delta references page outside mmap region (state size changed) | Cannot apply delta; fall back to previous full snapshot |
| `ChainTooLong` | Too many deltas without compaction | Force full snapshot on next checkpoint |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Signal handler overhead per dirty page | < 2 microseconds | `bench_sigsegv_handler` |
| Dirty page tracking setup (1 GB region) | < 10ms | `bench_mprotect_setup` |
| Delta collection (1000 dirty pages of 256K total) | < 5ms | `bench_delta_collection` |
| Bandwidth reduction (1% mutation rate) | > 95% reduction vs full | `bench_bandwidth_savings` |
| Bandwidth reduction (10% mutation rate) | > 85% reduction vs full | `bench_bandwidth_savings` |
| Delta apply throughput | > 1 GB/s | `bench_delta_apply` |
| Delta serialization throughput | > 500 MB/s | `bench_delta_serialize` |

## Test Plan

### Unit Tests

- [ ] `test_dirty_page_tracker_creation` - Tracker initializes with correct page count
- [ ] `test_dirty_bitset_clear` - Bitset starts all zeros
- [ ] `test_dirty_page_mark_and_count` - Mark pages dirty, count correctly
- [ ] `test_collect_snapshot_full_on_first` - First snapshot is always full
- [ ] `test_collect_snapshot_delta_after_tracking` - Delta contains only dirty pages
- [ ] `test_force_full_snapshot` - Force flag produces full snapshot
- [ ] `test_chain_length_triggers_full` - Exceeding max_delta_chain_length forces full
- [ ] `test_interval_triggers_full` - full_snapshot_interval forces full
- [ ] `test_delta_format_serialize_deserialize` - Round-trip binary format
- [ ] `test_delta_format_checksum_verification` - Checksum catches corruption
- [ ] `test_delta_format_invalid_magic` - Wrong magic number rejected
- [ ] `test_delta_format_version_mismatch` - Wrong version rejected
- [ ] `test_apply_deltas_sequential` - Deltas applied in order
- [ ] `test_apply_deltas_out_of_order_sorted` - Deltas sorted by sequence
- [ ] `test_apply_deltas_out_of_bounds` - Error on invalid page index

### Integration Tests

- [ ] `test_incremental_checkpoint_full_cycle` - Track, checkpoint, recover
- [ ] `test_incremental_with_sparse_writes` - Few pages dirty, large savings
- [ ] `test_incremental_with_dense_writes` - Many pages dirty, falls back to full
- [ ] `test_delta_chain_recovery` - Base + 5 deltas restored correctly
- [ ] `test_mprotect_and_signal_handler` - Write triggers signal, page marked dirty (Unix only)
- [ ] `test_checkpoint_after_no_writes` - Empty delta (zero dirty pages)

### Benchmarks

- [ ] `bench_mprotect_setup_1gb` - Target: < 10ms
- [ ] `bench_dirty_page_tracking_overhead` - Target: < 2us per page fault
- [ ] `bench_delta_collection_1k_pages` - Target: < 5ms
- [ ] `bench_delta_serialize_1mb` - Target: < 2ms
- [ ] `bench_delta_apply_1mb` - Target: < 1ms
- [ ] `bench_bandwidth_savings_1pct_mutation` - Target: > 95% reduction
- [ ] `bench_bandwidth_savings_10pct_mutation` - Target: > 85% reduction

## Rollout Plan

1. **Phase 1**: `DeltaSnapshot` and `DeltaFormat` serialization + tests
2. **Phase 2**: `DirtyPageTracker` with manual dirty marking (no signal handler)
3. **Phase 3**: SIGSEGV signal handler for automatic dirty tracking (Unix)
4. **Phase 4**: `collect_snapshot()` with full/delta decision logic
5. **Phase 5**: `apply_deltas()` recovery path
6. **Phase 6**: Integration with `ObjectStoreCheckpointer` (F-DCKP-004)
7. **Phase 7**: Integration tests with mmap state backend
8. **Phase 8**: Benchmarks and optimization
9. **Phase 9**: Documentation, safety review, and code review

## Open Questions

- [ ] Should we support Windows via `VirtualProtect()`? The signal mechanism differs (structured exception handling), making it significantly more complex.
- [ ] Should we use `userfaultfd` instead of SIGSEGV on Linux? userfaultfd is more modern and avoids signal handler complexity, but requires Linux 4.11+.
- [ ] How should we handle state region resizing? If the mmap region grows between checkpoints, deltas from the old size cannot be applied to the new size.
- [ ] Should dirty page tracking be per-operator or global? Per-operator is safer but means multiple signal handlers or a multiplexed handler.
- [ ] Should we compress individual pages before including them in the delta? LZ4 compression could further reduce bandwidth.

## Completion Checklist

- [ ] `DeltaSnapshot` and `DeltaFormat` implemented with serialization
- [ ] `DirtyPageTracker` with mprotect-based tracking
- [ ] SIGSEGV signal handler (Unix) installed and tested
- [ ] `collect_snapshot()` full/delta decision logic
- [ ] `apply_deltas()` recovery path
- [ ] Delta chain compaction (force full after N deltas)
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets (>90% bandwidth reduction at 1% mutation)
- [ ] Safety documentation for all unsafe code
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [Apache Flink Incremental Checkpoints](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/large_state_tuning/#incremental-checkpoints) - Incremental checkpoint concept
- [mprotect(2)](https://man7.org/linux/man-pages/man2/mprotect.2.html) - Memory protection system call
- [userfaultfd(2)](https://man7.org/linux/man-pages/man2/userfaultfd.2.html) - Alternative to SIGSEGV for page fault handling
- [F-STATE-003: Mmap State Backend](../../phase-1/F002-memory-mapped-state-store.md) - Mmap state store this extends
- [F-DCKP-004: Object Store Checkpointer](F-DCKP-004-object-store-checkpointer.md) - Upload destination for deltas
