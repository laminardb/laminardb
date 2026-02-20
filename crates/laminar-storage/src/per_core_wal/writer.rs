//! Per-core WAL writer for lock-free segment writes.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;

use super::entry::PerCoreWalEntry;
use super::error::PerCoreWalError;

/// Size of the record header (length + CRC32).
const RECORD_HEADER_SIZE: u64 = 8;

/// Per-core WAL writer.
///
/// Each core owns its own writer, eliminating cross-core synchronization on the write path.
/// Record format is compatible with `[length: 4][crc32: 4][data: length]`
pub struct CoreWalWriter {
    /// Core ID this writer belongs to.
    core_id: usize,
    /// Buffered writer for efficient writes.
    writer: BufWriter<File>,
    /// Path to the segment file.
    path: PathBuf,
    /// Current write position in bytes (includes buffered, un-synced data).
    position: u64,
    /// Last synced position (data confirmed durable via `fdatasync`).
    ///
    /// Only this position is safe for checkpoint manifests — data beyond
    /// it may be lost on crash.
    synced_position: u64,
    /// Current epoch (set by manager during checkpoint).
    epoch: u64,
    /// Core-local sequence number (monotonically increasing).
    sequence: u64,
    /// Last sync time for group commit.
    last_sync: Instant,
    /// Number of entries since last sync.
    entries_since_sync: u64,
}

impl CoreWalWriter {
    /// Creates a new per-core WAL writer.
    ///
    /// # Arguments
    ///
    /// * `core_id` - The core ID for this writer
    /// * `path` - Path to the segment file
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created or opened.
    pub fn new(core_id: usize, path: &Path) -> Result<Self, PerCoreWalError> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        let position = file.metadata()?.len();

        Ok(Self {
            core_id,
            writer: BufWriter::with_capacity(64 * 1024, file), // 64KB buffer
            path: path.to_path_buf(),
            position,
            synced_position: position,
            epoch: 0,
            sequence: 0,
            last_sync: Instant::now(),
            entries_since_sync: 0,
        })
    }

    /// Opens an existing segment file at a specific position.
    ///
    /// Used during recovery to resume writing.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or truncated.
    pub fn open_at(core_id: usize, path: &Path, position: u64) -> Result<Self, PerCoreWalError> {
        let file = OpenOptions::new().write(true).open(path)?;

        // Truncate to the specified position (in case of torn writes)
        file.set_len(position)?;

        let file = OpenOptions::new().append(true).open(path)?;

        Ok(Self {
            core_id,
            writer: BufWriter::with_capacity(64 * 1024, file),
            path: path.to_path_buf(),
            position,
            synced_position: position,
            epoch: 0,
            sequence: 0,
            last_sync: Instant::now(),
            entries_since_sync: 0,
        })
    }

    /// Returns the core ID for this writer.
    #[must_use]
    pub fn core_id(&self) -> usize {
        self.core_id
    }

    /// Returns the current write position in bytes (includes un-synced data).
    #[must_use]
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Returns the last synced position (durable after `fdatasync`).
    ///
    /// Only data up to this position is guaranteed to survive a crash.
    /// Use this for checkpoint manifests instead of [`position()`](Self::position).
    #[must_use]
    pub fn synced_position(&self) -> u64 {
        self.synced_position
    }

    /// Returns the current epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Returns the current sequence number.
    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the path to the segment file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the number of entries since last sync.
    #[must_use]
    pub fn entries_since_sync(&self) -> u64 {
        self.entries_since_sync
    }

    /// Sets the current epoch (called by manager during checkpoint).
    pub fn set_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
    }

    /// Appends a Put operation to the WAL.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or I/O fails.
    #[inline]
    #[allow(clippy::cast_possible_truncation)] // core_id bounded by physical CPU count (< u16::MAX)
    pub fn append_put(&mut self, key: &[u8], value: &[u8]) -> Result<u64, PerCoreWalError> {
        let entry = PerCoreWalEntry::put(
            self.core_id as u16,
            self.epoch,
            self.sequence,
            key.to_vec(),
            value.to_vec(),
        );
        self.append(&entry)
    }

    /// Appends a Delete operation to the WAL.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or I/O fails.
    #[inline]
    #[allow(clippy::cast_possible_truncation)] // core_id bounded by physical CPU count (< u16::MAX)
    pub fn append_delete(&mut self, key: &[u8]) -> Result<u64, PerCoreWalError> {
        let entry =
            PerCoreWalEntry::delete(self.core_id as u16, self.epoch, self.sequence, key.to_vec());
        self.append(&entry)
    }

    /// Appends a raw entry to the WAL.
    ///
    /// Record format: `[length: 4 bytes][crc32: 4 bytes][data: length bytes]`
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or I/O fails.
    pub fn append(&mut self, entry: &PerCoreWalEntry) -> Result<u64, PerCoreWalError> {
        let start_pos = self.position;

        // Serialize the entry using rkyv
        let bytes: AlignedVec = rkyv::to_bytes::<RkyvError>(entry)
            .map_err(|e| PerCoreWalError::Serialization(e.to_string()))?;

        // Compute CRC32C checksum
        let crc = crc32c::crc32c(&bytes);

        // Write record: [length: 4 bytes][crc32: 4 bytes][data: length bytes]
        #[allow(clippy::cast_possible_truncation)]
        // rkyv entry serialization is well below u32::MAX
        let len = bytes.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&bytes)?;

        #[allow(clippy::cast_possible_truncation)] // usize → u64: lossless on 64-bit
        let bytes_len = bytes.len() as u64;
        self.position += RECORD_HEADER_SIZE + bytes_len;
        self.sequence += 1;
        self.entries_since_sync += 1;

        Ok(start_pos)
    }

    /// Appends a Checkpoint marker to the WAL.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or I/O fails.
    #[allow(clippy::cast_possible_truncation)] // core_id bounded by physical CPU count (< u16::MAX)
    pub fn append_checkpoint(&mut self, checkpoint_id: u64) -> Result<u64, PerCoreWalError> {
        let entry = PerCoreWalEntry::checkpoint(
            self.core_id as u16,
            self.epoch,
            self.sequence,
            checkpoint_id,
        );
        self.append(&entry)
    }

    /// Appends an `EpochBarrier` to the WAL.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or I/O fails.
    #[allow(clippy::cast_possible_truncation)] // core_id bounded by physical CPU count (< u16::MAX)
    pub fn append_epoch_barrier(&mut self) -> Result<u64, PerCoreWalError> {
        let entry = PerCoreWalEntry::epoch_barrier(self.core_id as u16, self.epoch, self.sequence);
        self.append(&entry)
    }

    /// Appends a Commit entry to the WAL.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or I/O fails.
    #[allow(clippy::cast_possible_truncation)] // core_id bounded by physical CPU count (< u16::MAX)
    pub fn append_commit(
        &mut self,
        offsets: std::collections::HashMap<String, u64>,
        watermark: Option<i64>,
    ) -> Result<u64, PerCoreWalError> {
        let entry = PerCoreWalEntry::commit(
            self.core_id as u16,
            self.epoch,
            self.sequence,
            offsets,
            watermark,
        );
        self.append(&entry)
    }

    /// Syncs the WAL segment to disk using fdatasync.
    ///
    /// Uses `sync_data()` instead of `sync_all()` for better performance.
    ///
    /// # Errors
    ///
    /// Returns an error if the sync fails.
    pub fn sync(&mut self) -> Result<(), PerCoreWalError> {
        self.writer.flush()?;
        // Use sync_data() (fdatasync) instead of sync_all() (fsync)
        self.writer.get_ref().sync_data()?;
        self.synced_position = self.position;
        self.last_sync = Instant::now();
        self.entries_since_sync = 0;
        Ok(())
    }

    /// Truncates the segment file at the specified position.
    ///
    /// Used after checkpoint to remove entries that have been checkpointed.
    ///
    /// # Errors
    ///
    /// Returns an error if truncation fails.
    pub fn truncate(&mut self, position: u64) -> Result<(), PerCoreWalError> {
        self.sync()?;

        // Close current writer by dropping, then truncate
        let file = OpenOptions::new()
            .write(true)
            .truncate(false)
            .open(&self.path)?;

        file.set_len(position)?;

        // Reopen for append
        let file = OpenOptions::new().append(true).open(&self.path)?;

        self.writer = BufWriter::with_capacity(64 * 1024, file);
        self.position = position;
        self.synced_position = position;

        Ok(())
    }

    /// Resets the segment (truncates to zero).
    ///
    /// Used after successful checkpoint to clear the WAL.
    ///
    /// # Errors
    ///
    /// Returns an error if truncation fails.
    pub fn reset(&mut self) -> Result<(), PerCoreWalError> {
        self.truncate(0)?;
        self.sequence = 0;
        Ok(())
    }
}

impl std::fmt::Debug for CoreWalWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreWalWriter")
            .field("core_id", &self.core_id)
            .field("path", &self.path)
            .field("position", &self.position)
            .field("synced_position", &self.synced_position)
            .field("epoch", &self.epoch)
            .field("sequence", &self.sequence)
            .field("entries_since_sync", &self.entries_since_sync)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_temp_writer(core_id: usize) -> (CoreWalWriter, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join(format!("wal-{core_id}.log"));
        let writer = CoreWalWriter::new(core_id, &path).unwrap();
        (writer, temp_dir)
    }

    #[test]
    fn test_writer_creation() {
        let (writer, _temp_dir) = create_temp_writer(0);
        assert_eq!(writer.core_id(), 0);
        assert_eq!(writer.position(), 0);
        assert_eq!(writer.epoch(), 0);
        assert_eq!(writer.sequence(), 0);
    }

    #[test]
    fn test_append_put() {
        let (mut writer, _temp_dir) = create_temp_writer(0);

        let pos = writer.append_put(b"key1", b"value1").unwrap();
        assert_eq!(pos, 0);
        assert!(writer.position() > 0);
        assert_eq!(writer.sequence(), 1);

        let pos2 = writer.append_put(b"key2", b"value2").unwrap();
        assert!(pos2 > pos);
        assert_eq!(writer.sequence(), 2);
    }

    #[test]
    fn test_append_delete() {
        let (mut writer, _temp_dir) = create_temp_writer(1);

        let pos = writer.append_delete(b"key1").unwrap();
        assert_eq!(pos, 0);
        assert!(writer.position() > 0);
    }

    #[test]
    fn test_sync() {
        let (mut writer, _temp_dir) = create_temp_writer(0);

        writer.append_put(b"key1", b"value1").unwrap();
        assert_eq!(writer.entries_since_sync(), 1);

        writer.sync().unwrap();
        assert_eq!(writer.entries_since_sync(), 0);
    }

    #[test]
    fn test_epoch_setting() {
        let (mut writer, _temp_dir) = create_temp_writer(0);

        assert_eq!(writer.epoch(), 0);
        writer.set_epoch(5);
        assert_eq!(writer.epoch(), 5);
    }

    #[test]
    fn test_truncate() {
        let (mut writer, _temp_dir) = create_temp_writer(0);

        writer.append_put(b"key1", b"value1").unwrap();
        let pos1 = writer.position();

        writer.append_put(b"key2", b"value2").unwrap();
        let pos2 = writer.position();

        assert!(pos2 > pos1);

        writer.truncate(pos1).unwrap();
        assert_eq!(writer.position(), pos1);
    }

    #[test]
    fn test_reset() {
        let (mut writer, _temp_dir) = create_temp_writer(0);

        writer.append_put(b"key1", b"value1").unwrap();
        writer.append_put(b"key2", b"value2").unwrap();
        assert!(writer.position() > 0);
        assert_eq!(writer.sequence(), 2);

        writer.reset().unwrap();
        assert_eq!(writer.position(), 0);
        assert_eq!(writer.sequence(), 0);
    }

    #[test]
    fn test_append_checkpoint() {
        let (mut writer, _temp_dir) = create_temp_writer(0);

        let pos = writer.append_checkpoint(100).unwrap();
        assert_eq!(pos, 0);
        assert!(writer.position() > 0);
    }

    #[test]
    fn test_append_epoch_barrier() {
        let (mut writer, _temp_dir) = create_temp_writer(0);
        writer.set_epoch(5);

        let pos = writer.append_epoch_barrier().unwrap();
        assert_eq!(pos, 0);
        assert!(writer.position() > 0);
    }

    #[test]
    fn test_append_commit() {
        let (mut writer, _temp_dir) = create_temp_writer(0);

        let mut offsets = std::collections::HashMap::new();
        offsets.insert("topic1".to_string(), 100);

        let pos = writer.append_commit(offsets, Some(12345)).unwrap();
        assert_eq!(pos, 0);
        assert!(writer.position() > 0);
    }

    #[test]
    fn test_synced_position_tracks_sync() {
        let (mut writer, _temp_dir) = create_temp_writer(0);

        // Initially both positions are 0
        assert_eq!(writer.position(), 0);
        assert_eq!(writer.synced_position(), 0);

        // After write, position advances but synced_position stays
        writer.append_put(b"key1", b"value1").unwrap();
        assert!(writer.position() > 0);
        assert_eq!(writer.synced_position(), 0);

        // After sync, synced_position catches up
        let pos_before_sync = writer.position();
        writer.sync().unwrap();
        assert_eq!(writer.synced_position(), pos_before_sync);

        // Another write without sync
        writer.append_put(b"key2", b"value2").unwrap();
        assert!(writer.position() > writer.synced_position());
        assert_eq!(writer.synced_position(), pos_before_sync);
    }

    #[test]
    fn test_debug_format() {
        let (writer, _temp_dir) = create_temp_writer(42);
        let debug_str = format!("{writer:?}");
        assert!(debug_str.contains("CoreWalWriter"));
        assert!(debug_str.contains("42"));
    }

    #[test]
    fn test_open_at() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("wal-0.log");

        // Create and write some data
        {
            let mut writer = CoreWalWriter::new(0, &path).unwrap();
            writer.append_put(b"key1", b"value1").unwrap();
            writer.append_put(b"key2", b"value2").unwrap();
            writer.sync().unwrap();
        }

        // Get file size
        let file_size = std::fs::metadata(&path).unwrap().len();

        // Open at position 0 (truncates everything)
        let writer = CoreWalWriter::open_at(0, &path, 0).unwrap();
        assert_eq!(writer.position(), 0);

        // File should be truncated
        let new_size = std::fs::metadata(&path).unwrap().len();
        assert!(new_size < file_size);
    }
}
