# F-STATE-004: Pluggable Snapshot Strategies

> **❌ SUPERSEDED (2026-02-17)** — This feature has been cancelled. The ForkCow, DoubleBuffer, and IncrementalDirtyPages snapshot strategies were designed specifically for `MmapStateStore` (F-STATE-003), which has been superseded. With `FxHashMap`/`BTreeMap` as the Ring 0 state backend, checkpointing uses `rkyv` zero-copy serialization of the in-memory state followed by upload to object storage via `ObjectStoreCheckpointer` (F-DCKP-004). No OS-level memory management (mprotect, SIGSEGV handlers, fork+COW) is needed. See the [Storage Architecture Research](../../../research/storage-architecture.md) for the full analysis.
>
> **Superseded by:** F-DCKP-004 (ObjectStoreCheckpointer) handles checkpoint persistence directly from in-memory state using rkyv serialization.

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-STATE-004 |
| **Status** | ❌ Superseded |
| **Priority** | ~P1~ N/A |
| **Phase** | 6b |
| **Effort** | ~L (5-10 days)~ N/A |
| **Dependencies** | ~F-STATE-003 (MmapStateStore)~ N/A (F-STATE-003 also superseded) |
| **Blocks** | None |
| **Owner** | N/A |
| **Created** | 2026-02-16 |
| **Superseded** | 2026-02-17 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/state/snapshot.rs` |

## Summary

Pluggable snapshot strategies for `MmapStateStore` that trade off between snapshot creation speed, memory overhead, platform compatibility, and checkpoint bandwidth. Three strategies are provided:

1. **ForkCow** (Linux default): Uses `fork()` + copy-on-write pages for O(1) snapshot creation. The child process holds a frozen view of the mmap region while the parent continues processing. Zero memory overhead until pages are modified (COW).
2. **DoubleBuffer** (Windows/portable default): Maintains two mmap buffers and swaps pointers at snapshot time. O(1) snapshot start but 2x memory overhead. Works on all platforms.
3. **IncrementalDirtyPages**: Tracks which pages were modified since the last snapshot and only serializes dirty pages. Lowest checkpoint bandwidth but requires per-page dirty tracking overhead.

A `SnapshotProvider` trait abstracts the strategy, and `MmapSnapshotStrategy` is an enum for configuration. Platform auto-detection selects the optimal default: `ForkCow` on Linux, `DoubleBuffer` on Windows and macOS.

## Goals

- Define `SnapshotProvider` trait for pluggable snapshot strategies
- Implement `ForkCowProvider` using `fork()` + COW semantics (Linux only)
- Implement `DoubleBufferProvider` using pointer-swapped dual mmap buffers (portable)
- Implement `IncrementalDirtyPageProvider` with page-level dirty tracking
- `MmapSnapshotStrategy` enum for configuration and auto-detection
- Platform auto-detection: `ForkCow` on Linux, `DoubleBuffer` on Windows/macOS
- O(1) snapshot creation for `ForkCow` and `DoubleBuffer`
- Minimal checkpoint bandwidth for `IncrementalDirtyPages`
- Integration with `MmapStateStore::snapshot()` method

## Non-Goals

- Snapshot compression (handled at checkpoint persistence layer)
- Snapshot encryption (handled by F044 encryption-at-rest)
- Remote snapshot storage (S3 upload handled by checkpoint coordinator)
- InMemoryStateStore snapshots (those always use `FullStateSnapshot` directly)
- Async snapshot I/O (snapshot creation is synchronous; persistence is async in Ring 1)
- SlateDB-specific snapshot strategies (deferred to Phase 7)

## Technical Design

### Architecture

**Ring**: Ring 0 (snapshot creation), Ring 1 (snapshot serialization/persistence)
**Crate**: `laminar-core`
**Module**: `laminar-core/src/state/snapshot.rs`

The snapshot system bridges Ring 0 and Ring 1. When a checkpoint barrier arrives:

1. Ring 0: The operator calls `state.snapshot()` which delegates to the configured `SnapshotProvider`
2. The provider creates a `Box<dyn StateSnapshot>` with minimal latency impact
3. Ring 1: The checkpoint coordinator calls `snapshot.to_bytes()` to serialize
4. The serialized bytes are written to durable storage (local or S3)

```
┌──────────────────────────────────────────────────────────────────────┐
│                         RING 0: HOT PATH                             │
│                                                                      │
│  ┌──────────┐   barrier    ┌───────────────┐    snapshot()           │
│  │ Operator │ ──────────> │ MmapStateStore │ ─────────────┐         │
│  └──────────┘              └───────────────┘              │         │
│                                                           ▼         │
│                                              ┌─────────────────────┐│
│                                              │ SnapshotProvider    ││
│                                              │ ┌─────────────────┐ ││
│                                              │ │ ForkCow (Linux) │ ││
│                                              │ │ DoubleBuffer    │ ││
│                                              │ │ IncrDirtyPages  │ ││
│                                              │ └─────────────────┘ ││
│                                              └──────────┬──────────┘│
│                                                         │           │
├─────────────────────────────────────────────────────────┼───────────┤
│                         RING 1: CHECKPOINT              │           │
│                                                         ▼           │
│  ┌──────────────────┐    to_bytes()    ┌──────────────────────────┐ │
│  │ Checkpoint Coord  │ <────────────── │ Box<dyn StateSnapshot>   │ │
│  │                   │                 └──────────────────────────┘ │
│  │  write to S3/disk │                                              │
│  └──────────────────┘                                               │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use crate::state::{StateError, StateSnapshot};
use memmap2::MmapMut;
use std::path::Path;

/// Strategy for creating MmapStateStore snapshots.
///
/// Each strategy trades off differently between snapshot creation
/// latency, memory overhead, platform compatibility, and checkpoint
/// bandwidth. Choose based on your deployment environment and
/// performance priorities.
///
/// # Auto-Detection
///
/// Use `MmapSnapshotStrategy::auto()` to select the optimal strategy
/// for the current platform:
/// - Linux: `ForkCow` (fastest creation, zero memory overhead)
/// - Windows/macOS: `DoubleBuffer` (fast creation, 2x memory)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MmapSnapshotStrategy {
    /// Fork + copy-on-write snapshot (Linux only).
    ///
    /// Creates a child process via `fork()`. The child inherits the
    /// parent's mmap pages with COW semantics. The child serializes
    /// the frozen state while the parent continues processing.
    ///
    /// - Creation latency: O(1) (~50us for fork)
    /// - Memory overhead: 0 (COW pages, only modified pages are copied)
    /// - Platform: Linux only (uses fork + COW page semantics)
    /// - Bandwidth: full state on each checkpoint
    ForkCow,

    /// Double-buffer pointer swap snapshot (portable).
    ///
    /// Maintains two mmap buffers. At snapshot time, swaps the active
    /// and standby pointers. The standby buffer holds the frozen
    /// snapshot while writes go to the new active buffer.
    ///
    /// - Creation latency: O(1) (pointer swap)
    /// - Memory overhead: 2x (dual buffers)
    /// - Platform: all (Windows, Linux, macOS)
    /// - Bandwidth: full state on each checkpoint
    DoubleBuffer,

    /// Incremental dirty page tracking snapshot.
    ///
    /// Tracks which pages were modified since the last snapshot.
    /// Only serializes dirty pages, minimizing checkpoint bandwidth.
    /// Requires per-page dirty bit tracking overhead on writes.
    ///
    /// - Creation latency: O(dirty_pages)
    /// - Memory overhead: 1 bit per page (~128KB for 1GB state)
    /// - Platform: all
    /// - Bandwidth: proportional to changes since last checkpoint
    IncrementalDirtyPages,
}

impl MmapSnapshotStrategy {
    /// Auto-detect the optimal strategy for the current platform.
    ///
    /// Returns `ForkCow` on Linux, `DoubleBuffer` on all other platforms.
    #[must_use]
    pub fn auto() -> Self {
        if cfg!(target_os = "linux") {
            Self::ForkCow
        } else {
            Self::DoubleBuffer
        }
    }
}

/// Trait for snapshot creation strategies.
///
/// Implementations handle the mechanics of creating a consistent
/// snapshot of the mmap region with minimal impact on hot-path
/// processing latency.
///
/// # Lifecycle
///
/// 1. `prepare()` -- called before snapshot, may pre-allocate resources
/// 2. `create_snapshot()` -- called at barrier time, must be fast
/// 3. The returned `Box<dyn StateSnapshot>` is handed to Ring 1
/// 4. Ring 1 calls `to_bytes()` on the snapshot for persistence
///
/// # Thread Safety
///
/// Providers are `Send` but not `Sync`. They are owned by a single
/// `MmapStateStore` instance within a partition reactor.
pub trait SnapshotProvider: Send {
    /// Prepare for an upcoming snapshot.
    ///
    /// Called when a barrier is approaching but before the actual
    /// snapshot point. Implementations can use this to pre-allocate
    /// buffers, sync dirty pages, or perform other preparatory work.
    ///
    /// # Errors
    ///
    /// Returns `StateError` if preparation fails.
    fn prepare(&mut self) -> Result<(), StateError> {
        Ok(()) // Default: no preparation needed
    }

    /// Create a snapshot of the current mmap state.
    ///
    /// This is called at barrier time and must minimize latency.
    /// The returned snapshot must be independent of the mmap region
    /// (the region may be modified immediately after this returns).
    ///
    /// # Arguments
    ///
    /// * `mmap` - Reference to the current mmap region
    /// * `entry_count` - Number of live entries
    /// * `epoch` - Current checkpoint epoch
    ///
    /// # Errors
    ///
    /// Returns `StateError` if snapshot creation fails.
    fn create_snapshot(
        &mut self,
        mmap: &MmapMut,
        entry_count: usize,
        epoch: u64,
    ) -> Result<Box<dyn StateSnapshot>, StateError>;

    /// Notify the provider that the previous snapshot has been fully
    /// persisted and its resources can be released.
    ///
    /// For `ForkCow`, this signals that the child process can be reaped.
    /// For `DoubleBuffer`, this signals that the standby buffer can be
    /// reused. For `IncrementalDirtyPages`, this resets the dirty set.
    fn snapshot_committed(&mut self) {
        // Default: no cleanup needed
    }

    /// Get the strategy type for this provider.
    fn strategy(&self) -> MmapSnapshotStrategy;
}
```

### ForkCow Implementation (Linux)

```rust
#[cfg(target_os = "linux")]
pub struct ForkCowProvider {
    /// PID of the child process holding the snapshot, if any.
    child_pid: Option<libc::pid_t>,
    /// Shared memory region for child -> parent snapshot data transfer.
    /// The child writes serialized snapshot bytes here.
    shm_path: std::path::PathBuf,
}

#[cfg(target_os = "linux")]
impl ForkCowProvider {
    /// Create a new ForkCow snapshot provider.
    ///
    /// # Arguments
    ///
    /// * `shm_path` - Path for shared memory file used to transfer
    ///   snapshot data from child to parent process.
    #[must_use]
    pub fn new(shm_path: &Path) -> Self {
        Self {
            child_pid: None,
            shm_path: shm_path.to_path_buf(),
        }
    }
}

#[cfg(target_os = "linux")]
impl SnapshotProvider for ForkCowProvider {
    fn create_snapshot(
        &mut self,
        mmap: &MmapMut,
        entry_count: usize,
        epoch: u64,
    ) -> Result<Box<dyn StateSnapshot>, StateError> {
        // Reap previous child if still running
        if let Some(pid) = self.child_pid.take() {
            // SAFETY: pid is a valid child PID from a previous fork.
            unsafe {
                libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG);
            }
        }

        // SAFETY: fork() is safe when we immediately restrict the child
        // to async-signal-safe operations. The child only reads mmap
        // pages (COW) and writes to a shared memory file.
        let pid = unsafe { libc::fork() };

        match pid {
            -1 => {
                // Fork failed
                Err(StateError::Io(std::io::Error::last_os_error()))
            }
            0 => {
                // Child process: serialize snapshot and write to shm
                // The mmap region is frozen (COW) -- parent's writes
                // trigger page copies, child sees original data.
                //
                // NOTE: In actual implementation, the child would:
                // 1. Iterate the hash index in the mmap
                // 2. Collect all live entries
                // 3. Serialize via rkyv
                // 4. Write to shm_path
                // 5. Exit
                //
                // This is a simplified representation.
                std::process::exit(0);
            }
            child_pid => {
                // Parent process: continue immediately
                self.child_pid = Some(child_pid);

                // Return a snapshot handle that reads from shm when
                // to_bytes() is called. The actual data is written
                // by the child process asynchronously.
                Ok(Box::new(ForkCowSnapshot {
                    shm_path: self.shm_path.clone(),
                    child_pid,
                    epoch,
                    entry_count,
                }))
            }
        }
    }

    fn snapshot_committed(&mut self) {
        // Reap the child process
        if let Some(pid) = self.child_pid.take() {
            // SAFETY: pid is a valid child PID.
            unsafe {
                libc::waitpid(pid, std::ptr::null_mut(), 0);
            }
        }
    }

    fn strategy(&self) -> MmapSnapshotStrategy {
        MmapSnapshotStrategy::ForkCow
    }
}

/// Snapshot handle for the ForkCow strategy.
///
/// The actual snapshot data is being written by the child process.
/// `to_bytes()` waits for the child to finish and reads the shared
/// memory file.
#[cfg(target_os = "linux")]
struct ForkCowSnapshot {
    shm_path: std::path::PathBuf,
    child_pid: libc::pid_t,
    epoch: u64,
    entry_count: usize,
}

#[cfg(target_os = "linux")]
impl StateSnapshot for ForkCowSnapshot {
    fn to_bytes(&self) -> Result<rkyv::util::AlignedVec, StateError> {
        // Wait for child to finish writing
        // SAFETY: child_pid is valid from fork().
        unsafe {
            let mut status: libc::c_int = 0;
            libc::waitpid(self.child_pid, &mut status, 0);
        }

        // Read serialized data from shared memory file
        let data = std::fs::read(&self.shm_path)
            .map_err(StateError::Io)?;

        let mut aligned = rkyv::util::AlignedVec::with_capacity(data.len());
        aligned.extend_from_slice(&data);
        Ok(aligned)
    }

    fn len(&self) -> usize {
        self.entry_count
    }

    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn size_bytes(&self) -> usize {
        // Approximate; exact size is in the child's serialized output
        0
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + '_> {
        // Must deserialize from shm file to iterate
        // This is typically only called during restore, not hot path
        let data = std::fs::read(&self.shm_path).unwrap_or_default();
        let snapshot = FullStateSnapshot::from_bytes(&data)
            .unwrap_or_else(|_| FullStateSnapshot::new(vec![]));
        Box::new(snapshot.iter().collect::<Vec<_>>().into_iter())
    }
}
```

### DoubleBuffer Implementation (Portable)

```rust
use memmap2::MmapMut;

/// Double-buffer snapshot provider using pointer swap.
///
/// Maintains two mmap buffers (active and standby). At snapshot time,
/// the pointers are swapped: the current active becomes the frozen
/// snapshot, and the standby becomes the new active for writes.
///
/// # Memory Overhead
///
/// 2x the state size. For a 1GB state store, this requires 2GB of
/// virtual address space (physical pages are only allocated on write).
///
/// # Platform Compatibility
///
/// Works on all platforms (Linux, Windows, macOS). This is the default
/// on non-Linux platforms.
pub struct DoubleBufferProvider {
    /// The standby buffer (frozen snapshot after swap).
    standby: Option<MmapMut>,
    /// Path prefix for the standby mmap file.
    standby_path: std::path::PathBuf,
    /// Size of each buffer.
    buffer_size: usize,
}

impl DoubleBufferProvider {
    /// Create a new double-buffer provider.
    ///
    /// # Arguments
    ///
    /// * `standby_path` - Path for the standby mmap file
    /// * `buffer_size` - Size of each buffer in bytes
    ///
    /// # Errors
    ///
    /// Returns `StateError::Io` if the standby file cannot be created.
    pub fn new(standby_path: &Path, buffer_size: usize) -> Result<Self, StateError> {
        Ok(Self {
            standby: None,
            standby_path: standby_path.to_path_buf(),
            buffer_size,
        })
    }
}

impl SnapshotProvider for DoubleBufferProvider {
    fn prepare(&mut self) -> Result<(), StateError> {
        // Pre-allocate standby buffer if not already present
        if self.standby.is_none() {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&self.standby_path)?;
            file.set_len(self.buffer_size as u64)?;

            // SAFETY: We just created the file and hold exclusive access.
            #[allow(unsafe_code)]
            let mmap = unsafe { MmapMut::map_mut(&file)? };
            self.standby = Some(mmap);
        }
        Ok(())
    }

    fn create_snapshot(
        &mut self,
        mmap: &MmapMut,
        entry_count: usize,
        epoch: u64,
    ) -> Result<Box<dyn StateSnapshot>, StateError> {
        // Ensure standby buffer exists
        self.prepare()?;

        let mut standby = self.standby.take()
            .ok_or_else(|| StateError::NotSupported(
                "Standby buffer not available".to_string()
            ))?;

        // Copy active -> standby
        let copy_len = mmap.len().min(standby.len());
        standby[..copy_len].copy_from_slice(&mmap[..copy_len]);

        // The standby now holds the frozen snapshot
        Ok(Box::new(DoubleBufferSnapshot {
            buffer: standby,
            epoch,
            entry_count,
        }))
    }

    fn snapshot_committed(&mut self) {
        // The snapshot's buffer will be returned when the snapshot
        // is dropped. We don't need to do anything here -- the next
        // prepare() call will allocate a new standby.
    }

    fn strategy(&self) -> MmapSnapshotStrategy {
        MmapSnapshotStrategy::DoubleBuffer
    }
}

/// Snapshot holding a frozen copy of the mmap buffer.
struct DoubleBufferSnapshot {
    /// Frozen mmap buffer.
    buffer: MmapMut,
    /// Checkpoint epoch.
    epoch: u64,
    /// Number of entries.
    entry_count: usize,
}

impl StateSnapshot for DoubleBufferSnapshot {
    fn to_bytes(&self) -> Result<rkyv::util::AlignedVec, StateError> {
        // Serialize the frozen buffer contents.
        // Walk the hash index in the buffer, collect entries, rkyv encode.
        // Implementation mirrors MmapStateStore::snapshot() but reads
        // from self.buffer instead of the live mmap.
        todo!()
    }

    fn len(&self) -> usize {
        self.entry_count
    }

    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn size_bytes(&self) -> usize {
        self.buffer.len()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + '_> {
        // Walk hash index in the frozen buffer, yield entries
        todo!()
    }
}
```

### IncrementalDirtyPages Implementation

```rust
/// Incremental dirty page tracking snapshot provider.
///
/// Tracks which pages (4KB regions) have been modified since the
/// last snapshot. At checkpoint time, only dirty pages are serialized,
/// minimizing checkpoint bandwidth.
///
/// # Dirty Tracking
///
/// A bitset tracks dirty status per page. When `MmapStateStore::put()`
/// writes to a page, the corresponding bit is set. At snapshot time,
/// only pages with set bits are included in the checkpoint.
///
/// # Memory Overhead
///
/// 1 bit per page = 128KB for 1GB state (32KB for 256MB state).
/// Minimal overhead compared to the state itself.
///
/// # Bandwidth Savings
///
/// For workloads where only a small fraction of state changes between
/// checkpoints (e.g., append-heavy or localized updates), this can
/// reduce checkpoint bandwidth by 10-100x compared to full snapshots.
pub struct IncrementalDirtyPageProvider {
    /// Bitset tracking dirty pages. Index = page_offset / PAGE_SIZE.
    dirty_bits: Vec<u64>,
    /// Page size (typically 4096 bytes).
    page_size: usize,
    /// Total number of pages.
    num_pages: usize,
    /// Number of dirty pages since last snapshot.
    dirty_count: usize,
    /// Previous snapshot's page data for diff computation.
    /// Only dirty pages are stored here.
    baseline: Option<Vec<(usize, Vec<u8>)>>,
}

/// Page size for dirty tracking (matches OS page size).
const PAGE_SIZE: usize = 4096;
/// Number of bits per u64 word in the dirty bitset.
const BITS_PER_WORD: usize = 64;

impl IncrementalDirtyPageProvider {
    /// Create a new incremental dirty page provider.
    ///
    /// # Arguments
    ///
    /// * `total_size` - Total size of the mmap region in bytes
    #[must_use]
    pub fn new(total_size: usize) -> Self {
        let num_pages = (total_size + PAGE_SIZE - 1) / PAGE_SIZE;
        let num_words = (num_pages + BITS_PER_WORD - 1) / BITS_PER_WORD;
        Self {
            dirty_bits: vec![0u64; num_words],
            page_size: PAGE_SIZE,
            num_pages,
            dirty_count: 0,
            baseline: None,
        }
    }

    /// Mark a page as dirty.
    ///
    /// Called by `MmapStateStore::put()` when writing to a page.
    ///
    /// # Arguments
    ///
    /// * `offset` - Byte offset in the mmap region that was written
    #[inline]
    pub fn mark_dirty(&mut self, offset: usize) {
        let page_idx = offset / self.page_size;
        let word_idx = page_idx / BITS_PER_WORD;
        let bit_idx = page_idx % BITS_PER_WORD;

        if word_idx < self.dirty_bits.len() {
            let was_clean = self.dirty_bits[word_idx] & (1u64 << bit_idx) == 0;
            self.dirty_bits[word_idx] |= 1u64 << bit_idx;
            if was_clean {
                self.dirty_count += 1;
            }
        }
    }

    /// Check if a page is dirty.
    #[inline]
    pub fn is_dirty(&self, page_idx: usize) -> bool {
        let word_idx = page_idx / BITS_PER_WORD;
        let bit_idx = page_idx % BITS_PER_WORD;
        word_idx < self.dirty_bits.len()
            && self.dirty_bits[word_idx] & (1u64 << bit_idx) != 0
    }

    /// Get the number of dirty pages.
    #[must_use]
    pub fn dirty_count(&self) -> usize {
        self.dirty_count
    }

    /// Reset all dirty bits (called after snapshot is committed).
    fn reset_dirty_bits(&mut self) {
        self.dirty_bits.fill(0);
        self.dirty_count = 0;
    }
}

impl SnapshotProvider for IncrementalDirtyPageProvider {
    fn create_snapshot(
        &mut self,
        mmap: &MmapMut,
        entry_count: usize,
        epoch: u64,
    ) -> Result<Box<dyn StateSnapshot>, StateError> {
        // Collect dirty pages
        let mut dirty_pages: Vec<(usize, Vec<u8>)> = Vec::with_capacity(self.dirty_count);

        for page_idx in 0..self.num_pages {
            if self.is_dirty(page_idx) {
                let start = page_idx * self.page_size;
                let end = (start + self.page_size).min(mmap.len());
                if start < mmap.len() {
                    dirty_pages.push((page_idx, mmap[start..end].to_vec()));
                }
            }
        }

        Ok(Box::new(IncrementalSnapshot {
            dirty_pages,
            baseline: self.baseline.clone(),
            epoch,
            entry_count,
            page_size: self.page_size,
        }))
    }

    fn snapshot_committed(&mut self) {
        // Reset dirty tracking for next interval
        self.reset_dirty_bits();
    }

    fn strategy(&self) -> MmapSnapshotStrategy {
        MmapSnapshotStrategy::IncrementalDirtyPages
    }
}

/// Snapshot containing only dirty pages since the last checkpoint.
struct IncrementalSnapshot {
    /// Dirty pages: (page_index, page_data).
    dirty_pages: Vec<(usize, Vec<u8>)>,
    /// Previous baseline pages for full reconstruction.
    baseline: Option<Vec<(usize, Vec<u8>)>>,
    /// Checkpoint epoch.
    epoch: u64,
    /// Entry count (approximate).
    entry_count: usize,
    /// Page size.
    page_size: usize,
}

impl StateSnapshot for IncrementalSnapshot {
    fn to_bytes(&self) -> Result<rkyv::util::AlignedVec, StateError> {
        // Serialize dirty pages using rkyv.
        // The checkpoint coordinator merges dirty pages with the
        // previous checkpoint to produce a full state snapshot.
        rkyv::to_bytes::<rkyv::rancor::Error>(&self.dirty_pages)
            .map_err(|e| StateError::Serialization(e.to_string()))
    }

    fn len(&self) -> usize {
        self.entry_count
    }

    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn size_bytes(&self) -> usize {
        self.dirty_pages.iter().map(|(_, data)| data.len()).sum()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + '_> {
        // Incremental snapshots cannot be directly iterated for
        // full state reconstruction without the baseline.
        // For restore, the checkpoint coordinator must merge the
        // incremental with the last full snapshot first.
        Box::new(std::iter::empty())
    }
}
```

### Strategy Comparison

```rust
/// Factory function to create the appropriate SnapshotProvider.
///
/// # Arguments
///
/// * `strategy` - The desired snapshot strategy
/// * `mmap_path` - Path to the mmap file (for standby buffer naming)
/// * `mmap_size` - Size of the mmap region
///
/// # Errors
///
/// Returns `StateError` if provider initialization fails.
pub fn create_provider(
    strategy: MmapSnapshotStrategy,
    mmap_path: &Path,
    mmap_size: usize,
) -> Result<Box<dyn SnapshotProvider>, StateError> {
    match strategy {
        #[cfg(target_os = "linux")]
        MmapSnapshotStrategy::ForkCow => {
            let shm_path = mmap_path.with_extension("shm");
            Ok(Box::new(ForkCowProvider::new(&shm_path)))
        }
        #[cfg(not(target_os = "linux"))]
        MmapSnapshotStrategy::ForkCow => {
            Err(StateError::NotSupported(
                "ForkCow is only supported on Linux".to_string()
            ))
        }
        MmapSnapshotStrategy::DoubleBuffer => {
            let standby_path = mmap_path.with_extension("standby");
            Ok(Box::new(DoubleBufferProvider::new(&standby_path, mmap_size)?))
        }
        MmapSnapshotStrategy::IncrementalDirtyPages => {
            Ok(Box::new(IncrementalDirtyPageProvider::new(mmap_size)))
        }
    }
}
```

### Data Structures

```rust
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

/// Serialized incremental snapshot for persistence.
///
/// This is the on-disk format for incremental checkpoints. The
/// checkpoint coordinator merges incrementals with the last full
/// snapshot to reconstruct the complete state.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct IncrementalCheckpoint {
    /// Dirty pages: (page_index, page_data).
    pub dirty_pages: Vec<(u32, Vec<u8>)>,
    /// Epoch of this incremental.
    pub epoch: u64,
    /// Epoch of the base full snapshot this applies to.
    pub base_epoch: u64,
    /// Page size used for dirty tracking.
    pub page_size: u32,
}
```

### Algorithm/Flow

#### ForkCow Snapshot (Linux)

```
1. Checkpoint barrier arrives at operator
2. Operator calls state.snapshot()
3. MmapStateStore delegates to ForkCowProvider::create_snapshot()
4. Provider calls fork()                            (~50-100us)
5. Parent returns immediately with ForkCowSnapshot   (O(1))
6. Child process (COW pages):
   a. Iterate hash index in mmap                     (sequential)
   b. Collect all live entries                       (O(n))
   c. Serialize via rkyv                             (O(n))
   d. Write to shared memory file                    (O(n))
   e. exit(0)                                        (immediate)
7. Parent continues processing events
8. Ring 1 calls snapshot.to_bytes():
   a. waitpid() for child completion
   b. Read serialized bytes from shm file
9. Checkpoint coordinator persists to S3
10. snapshot_committed() reaps child
```

#### DoubleBuffer Snapshot (Portable)

```
1. Checkpoint barrier arrives at operator
2. Operator calls state.snapshot()
3. MmapStateStore delegates to DoubleBufferProvider::create_snapshot()
4. Provider copies active -> standby buffer          (O(n) memcpy)
5. Returns DoubleBufferSnapshot holding standby      (O(1) after copy)
6. Active buffer continues receiving writes
7. Ring 1 calls snapshot.to_bytes():
   a. Walk hash index in frozen standby buffer
   b. Collect entries, serialize via rkyv
8. Checkpoint coordinator persists to S3
9. snapshot_committed() releases standby buffer
```

#### IncrementalDirtyPages Snapshot

```
1. During normal operation:
   a. Every put()/delete() calls mark_dirty(offset)  (~5ns overhead)
   b. Dirty bit set in the bitset for that page
2. Checkpoint barrier arrives
3. Provider scans dirty bitset                        (O(pages))
4. Copies only dirty pages from mmap                 (O(dirty_pages))
5. Returns IncrementalSnapshot
6. Ring 1 serializes only the dirty pages
7. Checkpoint coordinator writes incremental + base epoch
8. snapshot_committed() resets dirty bits
9. Periodically, a full snapshot replaces the base
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `StateError::Io` | `fork()` fails (ENOMEM, max processes) | Fall back to DoubleBuffer or full copy |
| `StateError::Io` | Standby file creation fails | Alert operator, retry with different path |
| `StateError::NotSupported` | ForkCow requested on non-Linux | Auto-detect should prevent this; use DoubleBuffer |
| `StateError::Io` | Shared memory file write fails | Retry snapshot on next barrier |
| Child crash | Fork child segfaults | Detect via waitpid, retry with full copy |
| `StateError::Serialization` | rkyv encoding fails in child | Log, retry next barrier |

## Performance Targets

### Strategy Comparison Table

| Metric | ForkCow | DoubleBuffer | IncrDirtyPages |
|--------|---------|--------------|----------------|
| Snapshot creation | ~100us | O(n) memcpy | O(dirty_pages) |
| Hot-path overhead | 0ns | 0ns | ~5ns per put (dirty mark) |
| Memory overhead | 0 (COW) | 2x state size | 1 bit/page |
| Checkpoint bandwidth | full state | full state | dirty pages only |
| Platform | Linux only | all | all |
| Recovery complexity | simple | simple | merge with base |
| Recommended for | Production Linux | Windows/macOS | High-frequency checkpoints |

### Latency Targets

| Metric | Target | Method |
|--------|--------|--------|
| ForkCow creation | < 200us | `bench_snapshot_fork_create` |
| DoubleBuffer creation (1MB) | < 500us | `bench_snapshot_double_buffer_1mb` |
| DoubleBuffer creation (100MB) | < 50ms | `bench_snapshot_double_buffer_100mb` |
| IncrementalDirtyPages per-put overhead | < 10ns | `bench_snapshot_dirty_mark` |
| IncrementalDirtyPages creation (1% dirty) | < 1ms | `bench_snapshot_incremental_1pct` |
| Dirty bitset memory (1GB state) | < 256KB | Static calculation |
| Full snapshot to_bytes (10K entries) | < 5ms | `bench_snapshot_serialize_10k` |

## Test Plan

### Unit Tests

- [ ] `test_strategy_auto_returns_correct_default` -- platform-specific
- [ ] `test_strategy_enum_equality`
- [ ] `test_create_provider_fork_cow_on_linux`
- [ ] `test_create_provider_fork_cow_fails_on_non_linux`
- [ ] `test_create_provider_double_buffer`
- [ ] `test_create_provider_incremental`
- [ ] `test_dirty_page_mark_and_check`
- [ ] `test_dirty_page_count_tracking`
- [ ] `test_dirty_page_reset`
- [ ] `test_dirty_page_boundary_pages`
- [ ] `test_dirty_page_multiple_writes_same_page`
- [ ] `test_double_buffer_prepare_creates_standby`
- [ ] `test_double_buffer_create_snapshot_copies_data`
- [ ] `test_double_buffer_snapshot_independent_of_active`
- [ ] `test_incremental_snapshot_contains_only_dirty_pages`
- [ ] `test_incremental_snapshot_empty_when_no_changes`
- [ ] `test_incremental_checkpoint_serialization`
- [ ] `test_snapshot_committed_resets_state`

### Integration Tests

- [ ] `test_fork_cow_full_cycle` -- create store, write, snapshot, modify, verify snapshot unchanged (Linux only)
- [ ] `test_double_buffer_full_cycle` -- create store, write, snapshot, modify, verify
- [ ] `test_incremental_full_cycle` -- write, snapshot, write more, incremental snapshot, verify
- [ ] `test_incremental_merge_with_base` -- full snapshot + incremental = correct state
- [ ] `test_strategy_fallback` -- ForkCow fails, falls back to DoubleBuffer
- [ ] `test_concurrent_reads_during_snapshot` -- snapshot while reading state
- [ ] `test_large_state_snapshot` -- 100K entries, verify all strategies produce same result

### Benchmarks

- [ ] `bench_snapshot_fork_create` -- ForkCow creation latency (Linux)
- [ ] `bench_snapshot_double_buffer_1mb` -- DoubleBuffer creation, 1MB state
- [ ] `bench_snapshot_double_buffer_100mb` -- DoubleBuffer creation, 100MB state
- [ ] `bench_snapshot_dirty_mark` -- per-put overhead for dirty tracking
- [ ] `bench_snapshot_incremental_1pct` -- 1% dirty pages snapshot creation
- [ ] `bench_snapshot_incremental_10pct` -- 10% dirty pages snapshot creation
- [ ] `bench_snapshot_incremental_50pct` -- 50% dirty pages snapshot creation
- [ ] `bench_snapshot_serialize_10k` -- to_bytes() for 10K entries
- [ ] `bench_snapshot_serialize_100k` -- to_bytes() for 100K entries
- [ ] `bench_dirty_bitset_memory` -- memory overhead measurement

## Rollout Plan

1. **Phase 1**: Define `SnapshotProvider` trait and `MmapSnapshotStrategy` enum
2. **Phase 2**: Implement `DoubleBufferProvider` (portable, needed for all platforms)
3. **Phase 3**: Implement `IncrementalDirtyPageProvider` with bitset tracking
4. **Phase 4**: Implement `ForkCowProvider` (Linux only, gated by `#[cfg(target_os = "linux")]`)
5. **Phase 5**: Implement `create_provider()` factory with auto-detection
6. **Phase 6**: Integrate with `MmapStateStore::snapshot()` method
7. **Phase 7**: Unit tests for all strategies
8. **Phase 8**: Integration tests and cross-platform verification
9. **Phase 9**: Benchmarks and performance validation
10. **Phase 10**: Code review and merge

## Open Questions

- [ ] Should `ForkCow` use `posix_spawn` instead of `fork()` on modern Linux? `posix_spawn` avoids the overhead of duplicating the parent's page tables. However, it also means the child cannot directly access the parent's mmap pages. Need to investigate whether `posix_spawn` with appropriate file actions provides the same COW semantics.
- [ ] For `IncrementalDirtyPages`, should we use the OS-provided dirty page tracking (`/proc/self/pagemap` on Linux) instead of manual tracking? This would eliminate the per-put overhead but requires root or `CAP_SYS_ADMIN` capabilities and is Linux-specific.
- [ ] How often should we take full snapshots vs. incremental? Too many incrementals without a full base requires storing many diffs. A policy like "full every 10th checkpoint, incremental otherwise" could work.
- [ ] Should `DoubleBuffer` use `memcpy` or async DMA for the buffer copy? On modern hardware, `memcpy` for < 100MB is fast enough (~10ms), but DMA could help for very large states.
- [ ] For the `ForkCow` shared memory transfer, should we use `shm_open` (POSIX shared memory) instead of a regular file? This would keep the snapshot in RAM and avoid disk I/O for the transfer.

## Completion Checklist

- [ ] `SnapshotProvider` trait defined
- [ ] `MmapSnapshotStrategy` enum with auto-detection
- [ ] `ForkCowProvider` implemented (Linux, `#[cfg]` gated)
- [ ] `DoubleBufferProvider` implemented (portable)
- [ ] `IncrementalDirtyPageProvider` implemented
- [ ] `create_provider()` factory function
- [ ] Integration with `MmapStateStore::snapshot()`
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing on Linux and Windows
- [ ] Benchmarks meet latency targets
- [ ] Documentation complete with `#![deny(missing_docs)]`
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## References

- [F-STATE-001: StateStore Trait](./F-STATE-001-state-store-trait.md) -- `StateSnapshot` trait
- [F-STATE-003: MmapStateStore](./F-STATE-003-mmap-state-store.md) -- consumer of snapshot strategies
- [F022: Incremental Checkpointing](../../phase-2/F022-incremental-checkpointing.md) -- related Phase 2 feature
- [Redis Background Saving](https://redis.io/docs/management/persistence/) -- ForkCow inspiration (Redis uses `fork()` for RDB snapshots)
- [LMDB Copy-on-Write](http://www.lmdb.tech/doc/) -- COW B-tree approach
- [ARCHITECTURE.md](../../../ARCHITECTURE.md) -- Ring model design
