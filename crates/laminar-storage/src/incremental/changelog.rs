//! Ring 0 changelog buffer for incremental checkpointing.
//!
//! This module provides zero-allocation tracking of state mutations for
//! background WAL writes and incremental checkpoints.
//!
//! ## Design
//!
//! The changelog buffer sits on the Ring 0 hot path and must:
//! - Never allocate after warmup (pre-allocated capacity)
//! - Be O(1) for push operations
//! - Signal backpressure when full
//! - Drain efficiently to Ring 1 for WAL writes
//!
//! ## Memory Layout
//!
//! Each [`StateChangelogEntry`] is 32 bytes:
//! ```text
//! ┌─────────┬──────────┬─────────────┬───────────┬────┬─────────┐
//! │ epoch   │ key_hash │ mmap_offset │ value_len │ op │ padding │
//! │ 8 bytes │ 8 bytes  │ 8 bytes     │ 4 bytes   │ 1  │ 3 bytes │
//! └─────────┴──────────┴─────────────┴───────────┴────┴─────────┘
//! ```

use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

use fxhash::FxBuildHasher;

/// State mutation operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StateOp {
    /// Put (insert or update) operation.
    Put = 0,
    /// Delete operation.
    Delete = 1,
}

impl StateOp {
    /// Convert to u8 for compact storage.
    #[inline]
    #[must_use]
    pub const fn to_u8(self) -> u8 {
        self as u8
    }

    /// Convert from u8.
    #[inline]
    #[must_use]
    pub const fn from_u8(val: u8) -> Self {
        match val {
            1 => Self::Delete,
            _ => Self::Put,
        }
    }
}

/// Zero-allocation changelog entry for Ring 0 hot path.
///
/// This struct is designed for minimal memory footprint (32 bytes) and
/// cache-efficient access. It stores offset references into the mmap
/// state store rather than copying key/value data.
///
/// # Memory Layout
///
/// The struct is `repr(C)` to ensure predictable memory layout:
/// - `epoch`: 8 bytes - Logical epoch number for ordering
/// - `key_hash`: 8 bytes - `FxHash` of the key for quick comparison
/// - `mmap_offset`: 8 bytes - Offset into the mmap state store
/// - `value_len`: 4 bytes - Length of the value (0 for deletes)
/// - `op`: 1 byte - Operation type (Put/Delete)
/// - `_padding`: 3 bytes - Alignment padding
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct StateChangelogEntry {
    /// Logical epoch number for ordering changelog entries.
    pub epoch: u64,
    /// `FxHash` of the key (for deduplication and lookup).
    pub key_hash: u64,
    /// Offset into the mmap state store where value is stored.
    pub mmap_offset: u64,
    /// Length of the value in bytes (0 for Delete operations).
    pub value_len: u32,
    /// Operation type.
    op: u8,
    /// Padding for alignment.
    _padding: [u8; 3],
}

impl StateChangelogEntry {
    /// Creates a new Put changelog entry.
    #[inline]
    #[must_use]
    pub fn put(epoch: u64, key_hash: u64, mmap_offset: u64, value_len: u32) -> Self {
        Self {
            epoch,
            key_hash,
            mmap_offset,
            value_len,
            op: StateOp::Put.to_u8(),
            _padding: [0; 3],
        }
    }

    /// Creates a new Delete changelog entry.
    #[inline]
    #[must_use]
    pub fn delete(epoch: u64, key_hash: u64) -> Self {
        Self {
            epoch,
            key_hash,
            mmap_offset: 0,
            value_len: 0,
            op: StateOp::Delete.to_u8(),
            _padding: [0; 3],
        }
    }

    /// Creates a changelog entry from a key with automatic hashing.
    #[inline]
    #[must_use]
    pub fn from_key(key: &[u8], epoch: u64, mmap_offset: u64, value_len: u32, op: StateOp) -> Self {
        let key_hash = Self::hash_key(key);
        Self {
            epoch,
            key_hash,
            mmap_offset,
            value_len,
            op: op.to_u8(),
            _padding: [0; 3],
        }
    }

    /// Returns the operation type.
    #[inline]
    #[must_use]
    pub fn op(&self) -> StateOp {
        StateOp::from_u8(self.op)
    }

    /// Returns true if this is a Put operation.
    #[inline]
    #[must_use]
    pub fn is_put(&self) -> bool {
        self.op == StateOp::Put.to_u8()
    }

    /// Returns true if this is a Delete operation.
    #[inline]
    #[must_use]
    pub fn is_delete(&self) -> bool {
        self.op == StateOp::Delete.to_u8()
    }

    /// Hash a key using `FxHash` (fast, consistent across entries).
    #[inline]
    #[must_use]
    pub fn hash_key(key: &[u8]) -> u64 {
        let hasher_builder = FxBuildHasher::default();
        let mut hasher = hasher_builder.build_hasher();
        hasher.write(key);
        hasher.finish()
    }
}

// Verify the struct is exactly 32 bytes
const _: () = assert!(std::mem::size_of::<StateChangelogEntry>() == 32);

/// Ring 0 SPSC changelog buffer for state mutations.
///
/// This buffer is designed for the hot path and must never allocate after
/// initial warmup. It provides backpressure signaling when full.
///
/// ## Thread Safety
///
/// The buffer is designed for single-producer single-consumer access:
/// - Ring 0 (producer): pushes entries via `push()`
/// - Ring 1 (consumer): drains entries via `drain()`
///
/// Use atomic indices for thread-safe access when needed.
///
/// ## Example
///
/// ```rust,no_run
/// use laminar_storage::incremental::{StateChangelogBuffer, StateChangelogEntry};
///
/// // Pre-allocate buffer for 1024 entries
/// let mut buffer = StateChangelogBuffer::with_capacity(1024);
///
/// // Ring 0: Push state mutations (no allocation)
/// let entry = StateChangelogEntry::put(1, 12345, 0, 100);
/// if !buffer.push(entry) {
///     // Buffer full - apply backpressure
/// }
///
/// // Ring 1: Drain for WAL writes
/// let entries: Vec<_> = buffer.drain_all().collect();
/// ```
pub struct StateChangelogBuffer {
    /// Pre-allocated entry storage.
    entries: Vec<StateChangelogEntry>,
    /// Current write position (producer).
    write_pos: AtomicUsize,
    /// Current read position (consumer).
    read_pos: AtomicUsize,
    /// Buffer capacity.
    capacity: usize,
    /// Current epoch for new entries.
    current_epoch: u64,
    /// Metrics: total entries pushed.
    total_pushed: AtomicUsize,
    /// Metrics: total entries drained.
    total_drained: AtomicUsize,
    /// Metrics: overflow count (backpressure signals).
    overflow_count: AtomicUsize,
}

impl StateChangelogBuffer {
    /// Default buffer capacity (16K entries = 512KB).
    pub const DEFAULT_CAPACITY: usize = 16 * 1024;

    /// Creates a new changelog buffer with the given capacity.
    ///
    /// The buffer is pre-allocated to avoid allocation on the hot path.
    ///
    /// # Panics
    ///
    /// Panics if capacity is 0.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");

        // Round up to power of 2 for fast modulo
        let capacity = capacity.next_power_of_two();

        // Pre-allocate and zero-initialize
        let mut entries = Vec::with_capacity(capacity);
        entries.resize(
            capacity,
            StateChangelogEntry {
                epoch: 0,
                key_hash: 0,
                mmap_offset: 0,
                value_len: 0,
                op: 0,
                _padding: [0; 3],
            },
        );

        Self {
            entries,
            write_pos: AtomicUsize::new(0),
            read_pos: AtomicUsize::new(0),
            capacity,
            current_epoch: 0,
            total_pushed: AtomicUsize::new(0),
            total_drained: AtomicUsize::new(0),
            overflow_count: AtomicUsize::new(0),
        }
    }

    /// Creates a buffer with the default capacity.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(Self::DEFAULT_CAPACITY)
    }

    /// Sets the current epoch for new entries.
    pub fn set_epoch(&mut self, epoch: u64) {
        self.current_epoch = epoch;
    }

    /// Returns the current epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.current_epoch
    }

    /// Advances to the next epoch.
    pub fn advance_epoch(&mut self) -> u64 {
        self.current_epoch += 1;
        self.current_epoch
    }

    /// Pushes an entry to the buffer (zero allocation).
    ///
    /// Returns `true` if successful, `false` if buffer is full (backpressure).
    #[inline]
    pub fn push(&self, entry: StateChangelogEntry) -> bool {
        let write_pos = self.write_pos.load(Ordering::Relaxed);
        let read_pos = self.read_pos.load(Ordering::Acquire);

        // Check if buffer is full
        let next_pos = (write_pos + 1) & (self.capacity - 1);
        if next_pos == read_pos {
            self.overflow_count.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        // Write entry (safe because we have exclusive access to this slot)
        // SAFETY: We're the only writer to this position and haven't published it yet
        let entries_ptr = self.entries.as_ptr().cast_mut();
        #[allow(unsafe_code)]
        unsafe {
            entries_ptr.add(write_pos).write(entry);
        }

        // Publish the entry
        self.write_pos.store(next_pos, Ordering::Release);
        self.total_pushed.fetch_add(1, Ordering::Relaxed);

        true
    }

    /// Pushes a Put operation for the given key.
    #[inline]
    pub fn push_put(&self, key: &[u8], mmap_offset: u64, value_len: u32) -> bool {
        let entry = StateChangelogEntry::from_key(
            key,
            self.current_epoch,
            mmap_offset,
            value_len,
            StateOp::Put,
        );
        self.push(entry)
    }

    /// Pushes a Delete operation for the given key.
    #[inline]
    pub fn push_delete(&self, key: &[u8]) -> bool {
        let entry = StateChangelogEntry::from_key(key, self.current_epoch, 0, 0, StateOp::Delete);
        self.push(entry)
    }

    /// Attempts to pop a single entry (for consumer).
    #[inline]
    pub fn pop(&self) -> Option<StateChangelogEntry> {
        let read_pos = self.read_pos.load(Ordering::Relaxed);
        let write_pos = self.write_pos.load(Ordering::Acquire);

        if read_pos == write_pos {
            return None;
        }

        // Read entry
        let entry = self.entries[read_pos];

        // Advance read position
        let next_pos = (read_pos + 1) & (self.capacity - 1);
        self.read_pos.store(next_pos, Ordering::Release);
        self.total_drained.fetch_add(1, Ordering::Relaxed);

        Some(entry)
    }

    /// Drains up to `max_count` entries from the buffer.
    ///
    /// Returns an iterator over the drained entries.
    pub fn drain(&self, max_count: usize) -> impl Iterator<Item = StateChangelogEntry> + '_ {
        DrainIter {
            buffer: self,
            remaining: max_count,
        }
    }

    /// Drains all available entries from the buffer.
    pub fn drain_all(&self) -> impl Iterator<Item = StateChangelogEntry> + '_ {
        self.drain(usize::MAX)
    }

    /// Returns the number of entries currently in the buffer.
    #[must_use]
    pub fn len(&self) -> usize {
        let write_pos = self.write_pos.load(Ordering::Acquire);
        let read_pos = self.read_pos.load(Ordering::Acquire);
        write_pos.wrapping_sub(read_pos) & (self.capacity - 1)
    }

    /// Returns true if the buffer is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.write_pos.load(Ordering::Acquire) == self.read_pos.load(Ordering::Acquire)
    }

    /// Returns true if the buffer is full.
    #[must_use]
    pub fn is_full(&self) -> bool {
        let write_pos = self.write_pos.load(Ordering::Acquire);
        let read_pos = self.read_pos.load(Ordering::Acquire);
        ((write_pos + 1) & (self.capacity - 1)) == read_pos
    }

    /// Returns the buffer capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns available space in the buffer.
    #[must_use]
    pub fn available(&self) -> usize {
        self.capacity - self.len() - 1
    }

    /// Returns the total number of entries pushed (including overflows).
    #[must_use]
    pub fn total_pushed(&self) -> usize {
        self.total_pushed.load(Ordering::Relaxed)
    }

    /// Returns the total number of entries drained.
    #[must_use]
    pub fn total_drained(&self) -> usize {
        self.total_drained.load(Ordering::Relaxed)
    }

    /// Returns the number of overflow events (backpressure signals).
    #[must_use]
    pub fn overflow_count(&self) -> usize {
        self.overflow_count.load(Ordering::Relaxed)
    }

    /// Clears the buffer (for testing/reset).
    pub fn clear(&self) {
        // Move read position to write position
        let write_pos = self.write_pos.load(Ordering::Acquire);
        self.read_pos.store(write_pos, Ordering::Release);
    }

    /// Creates a checkpoint barrier at the current position.
    ///
    /// Returns the current epoch and write position for recovery.
    #[must_use]
    pub fn checkpoint_barrier(&self) -> (u64, usize) {
        (self.current_epoch, self.write_pos.load(Ordering::Acquire))
    }
}

impl Default for StateChangelogBuffer {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: StateChangelogBuffer is Send because:
// 1. All fields are either Send or thread-safe (atomics)
// 2. The Vec<StateChangelogEntry> is pre-allocated and accessed via atomics
#[allow(unsafe_code)]
unsafe impl Send for StateChangelogBuffer {}

// SAFETY: StateChangelogBuffer is Sync because:
// 1. Access is coordinated via atomic indices
// 2. Single producer (Ring 0) and single consumer (Ring 1) access pattern
#[allow(unsafe_code)]
unsafe impl Sync for StateChangelogBuffer {}

/// Drain iterator for the changelog buffer.
struct DrainIter<'a> {
    buffer: &'a StateChangelogBuffer,
    remaining: usize,
}

impl Iterator for DrainIter<'_> {
    type Item = StateChangelogEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        self.buffer.pop()
    }
}

/// Builder for creating changelog entries with validation.
pub struct ChangelogEntryBuilder {
    epoch: u64,
    hasher_builder: FxBuildHasher,
}

impl ChangelogEntryBuilder {
    /// Creates a new builder for the given epoch.
    #[must_use]
    pub fn new(epoch: u64) -> Self {
        Self {
            epoch,
            hasher_builder: FxBuildHasher::default(),
        }
    }

    /// Sets the epoch.
    #[must_use]
    pub fn epoch(mut self, epoch: u64) -> Self {
        self.epoch = epoch;
        self
    }

    /// Builds a Put entry.
    #[must_use]
    pub fn put(&self, key: &[u8], mmap_offset: u64, value_len: u32) -> StateChangelogEntry {
        let mut hasher = self.hasher_builder.build_hasher();
        hasher.write(key);
        StateChangelogEntry::put(self.epoch, hasher.finish(), mmap_offset, value_len)
    }

    /// Builds a Delete entry.
    #[must_use]
    pub fn delete(&self, key: &[u8]) -> StateChangelogEntry {
        let mut hasher = self.hasher_builder.build_hasher();
        hasher.write(key);
        StateChangelogEntry::delete(self.epoch, hasher.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_op_roundtrip() {
        assert_eq!(StateOp::from_u8(StateOp::Put.to_u8()), StateOp::Put);
        assert_eq!(StateOp::from_u8(StateOp::Delete.to_u8()), StateOp::Delete);
        assert_eq!(StateOp::from_u8(255), StateOp::Put); // Default to Put
    }

    #[test]
    fn test_changelog_entry_size() {
        assert_eq!(std::mem::size_of::<StateChangelogEntry>(), 32);
    }

    #[test]
    fn test_changelog_entry_put() {
        let entry = StateChangelogEntry::put(1, 12345, 100, 50);
        assert_eq!(entry.epoch, 1);
        assert_eq!(entry.key_hash, 12345);
        assert_eq!(entry.mmap_offset, 100);
        assert_eq!(entry.value_len, 50);
        assert!(entry.is_put());
        assert!(!entry.is_delete());
    }

    #[test]
    fn test_changelog_entry_delete() {
        let entry = StateChangelogEntry::delete(2, 67890);
        assert_eq!(entry.epoch, 2);
        assert_eq!(entry.key_hash, 67890);
        assert_eq!(entry.mmap_offset, 0);
        assert_eq!(entry.value_len, 0);
        assert!(entry.is_delete());
        assert!(!entry.is_put());
    }

    #[test]
    fn test_changelog_entry_from_key() {
        let entry = StateChangelogEntry::from_key(b"test_key", 5, 200, 75, StateOp::Put);
        assert_eq!(entry.epoch, 5);
        assert_eq!(entry.mmap_offset, 200);
        assert_eq!(entry.value_len, 75);
        assert!(entry.is_put());

        // Same key should produce same hash
        let entry2 = StateChangelogEntry::from_key(b"test_key", 6, 300, 80, StateOp::Delete);
        assert_eq!(entry.key_hash, entry2.key_hash);
    }

    #[test]
    fn test_buffer_basic_operations() {
        let buffer = StateChangelogBuffer::with_capacity(16);
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 16);

        let entry = StateChangelogEntry::put(1, 100, 0, 10);
        assert!(buffer.push(entry));
        assert!(!buffer.is_empty());
        assert_eq!(buffer.len(), 1);

        let popped = buffer.pop().unwrap();
        assert_eq!(popped.key_hash, 100);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_buffer_full() {
        let buffer = StateChangelogBuffer::with_capacity(4);

        // Fill buffer (capacity - 1 = 3 entries for ring buffer)
        for i in 0..3 {
            assert!(buffer.push(StateChangelogEntry::put(1, i, 0, 10)));
        }

        // Should be full
        assert!(buffer.is_full());
        assert!(!buffer.push(StateChangelogEntry::put(1, 999, 0, 10)));
        assert_eq!(buffer.overflow_count(), 1);
    }

    #[test]
    fn test_buffer_drain() {
        let buffer = StateChangelogBuffer::with_capacity(16);

        for i in 0..5 {
            buffer.push(StateChangelogEntry::put(1, i, 0, 10));
        }

        assert_eq!(buffer.len(), 5);

        // Drain 3 entries
        let drained: Vec<_> = buffer.drain(3).collect();
        assert_eq!(drained.len(), 3);
        assert_eq!(buffer.len(), 2);

        // Drain remaining
        let remaining: Vec<_> = buffer.drain_all().collect();
        assert_eq!(remaining.len(), 2);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_buffer_epoch() {
        let mut buffer = StateChangelogBuffer::with_capacity(16);
        assert_eq!(buffer.epoch(), 0);

        buffer.set_epoch(10);
        assert_eq!(buffer.epoch(), 10);

        assert_eq!(buffer.advance_epoch(), 11);
        assert_eq!(buffer.epoch(), 11);
    }

    #[test]
    fn test_buffer_push_helpers() {
        let buffer = StateChangelogBuffer::with_capacity(16);

        assert!(buffer.push_put(b"key1", 100, 50));
        assert!(buffer.push_delete(b"key2"));

        let entries: Vec<_> = buffer.drain_all().collect();
        assert_eq!(entries.len(), 2);
        assert!(entries[0].is_put());
        assert!(entries[1].is_delete());
    }

    #[test]
    fn test_buffer_clear() {
        let buffer = StateChangelogBuffer::with_capacity(16);

        for i in 0..5 {
            buffer.push(StateChangelogEntry::put(1, i, 0, 10));
        }
        assert_eq!(buffer.len(), 5);

        buffer.clear();
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_buffer_checkpoint_barrier() {
        let mut buffer = StateChangelogBuffer::with_capacity(16);
        buffer.set_epoch(42);

        buffer.push(StateChangelogEntry::put(42, 1, 0, 10));
        buffer.push(StateChangelogEntry::put(42, 2, 0, 10));

        let (epoch, pos) = buffer.checkpoint_barrier();
        assert_eq!(epoch, 42);
        assert_eq!(pos, 2);
    }

    #[test]
    fn test_buffer_metrics() {
        let buffer = StateChangelogBuffer::with_capacity(8);

        for i in 0..5 {
            buffer.push(StateChangelogEntry::put(1, i, 0, 10));
        }
        assert_eq!(buffer.total_pushed(), 5);
        assert_eq!(buffer.total_drained(), 0);

        let _ = buffer.pop();
        let _ = buffer.pop();
        assert_eq!(buffer.total_drained(), 2);
    }

    #[test]
    fn test_entry_builder() {
        let builder = ChangelogEntryBuilder::new(100);

        let put = builder.put(b"mykey", 500, 75);
        assert_eq!(put.epoch, 100);
        assert_eq!(put.mmap_offset, 500);
        assert_eq!(put.value_len, 75);
        assert!(put.is_put());

        let delete = builder.delete(b"mykey");
        assert_eq!(delete.epoch, 100);
        assert!(delete.is_delete());

        // Same key produces same hash
        assert_eq!(put.key_hash, delete.key_hash);
    }

    #[test]
    fn test_buffer_wraparound() {
        let buffer = StateChangelogBuffer::with_capacity(4);

        // Fill and drain multiple times to test wraparound
        for iteration in 0..5 {
            for i in 0..3 {
                assert!(
                    buffer.push(StateChangelogEntry::put(1, i + iteration * 10, 0, 10)),
                    "Failed at iteration {iteration}, entry {i}"
                );
            }

            let drained: Vec<_> = buffer.drain_all().collect();
            assert_eq!(drained.len(), 3, "Failed at iteration {iteration}");
        }
    }

    #[test]
    fn test_key_hash_consistency() {
        let key = b"consistent_key";
        let hash1 = StateChangelogEntry::hash_key(key);
        let hash2 = StateChangelogEntry::hash_key(key);
        assert_eq!(hash1, hash2);

        let different_key = b"different_key";
        let hash3 = StateChangelogEntry::hash_key(different_key);
        assert_ne!(hash1, hash3);
    }
}
