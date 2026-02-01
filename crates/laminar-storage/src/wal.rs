//! Write-Ahead Log for durability and exactly-once semantics.
//!
//! The WAL provides durability by persisting all state mutations before they
//! are applied. This enables recovery after crashes and supports exactly-once
//! processing semantics.
//!
//! ## Record Format
//!
//! Each WAL record is stored as:
//! ```text
//! +----------+----------+------------------+
//! | Length   | CRC32C   | Entry Data       |
//! | (4 bytes)| (4 bytes)| (Length bytes)   |
//! +----------+----------+------------------+
//! ```
//!
//! - **Length**: 4-byte little-endian u32, size of Entry Data
//! - **CRC32C**: 4-byte little-endian u32, CRC32C checksum of Entry Data
//! - **Entry Data**: rkyv-serialized `WalEntry`
//!
//! ## Durability
//!
//! Uses `fdatasync` (via `sync_data()`) instead of `fsync` for better performance.
//! This syncs file data without updating metadata (atime, mtime), saving 50-100μs per sync.
//!
//! ## Torn Write Detection
//!
//! On recovery, the WAL reader detects partial writes:
//! - Incomplete length prefix (< 4 bytes at EOF)
//! - Incomplete CRC field (< 4 bytes after length)
//! - Incomplete data (< length bytes after CRC)
//! - CRC32 mismatch (data corruption)
//!
//! Use `repair()` to truncate the WAL to the last valid record.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rkyv::{
    rancor::Error as RkyvError, util::AlignedVec, Archive, Deserialize as RkyvDeserialize,
    Serialize as RkyvSerialize,
};

// Module to contain types that use derive macros with generated code
mod wal_types {
    #![allow(missing_docs)] // Allow for derive-generated code

    use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
    use std::collections::HashMap;

    /// WAL entry types representing different operations.
    #[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
    pub enum WalEntry {
        /// Put a key-value pair.
        Put {
            /// The key to put.
            key: Vec<u8>,
            /// The value to associate with the key.
            value: Vec<u8>,
        },
        /// Delete a key.
        Delete {
            /// The key to delete.
            key: Vec<u8>,
        },
        /// Checkpoint marker.
        Checkpoint {
            /// Checkpoint identifier.
            id: u64,
        },
        /// Commit offsets for exactly-once semantics.
        Commit {
            /// Map of topic/partition to offset.
            offsets: HashMap<String, u64>,
            /// Current watermark at commit time (for recovery).
            watermark: Option<i64>,
        },
    }
}

pub use wal_types::WalEntry;

/// Size of the record header (length + CRC32).
const RECORD_HEADER_SIZE: u64 = 8;

/// WAL position for checkpointing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
#[rkyv(compare(PartialEq))]
pub struct WalPosition {
    /// Offset in the WAL file.
    pub offset: u64,
}

/// Write-Ahead Log implementation.
pub struct WriteAheadLog {
    /// Buffered writer for efficient writes.
    writer: BufWriter<File>,
    /// Path to the current log file.
    path: PathBuf,
    /// Sync interval for group commit.
    sync_interval: Duration,
    /// Last sync time.
    last_sync: Instant,
    /// Current file position.
    position: u64,
    /// Whether to sync on every write (for testing).
    sync_on_write: bool,
}

/// Error type for WAL operations.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    /// IO error during WAL operations.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error when writing entries.
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Deserialization error when reading entries.
    #[error("Deserialization error: {0}")]
    Deserialization(String),

    /// Corrupted WAL entry detected (incomplete record).
    #[error("Corrupted WAL entry at position {position}: {reason}")]
    Corrupted {
        /// Position where corruption was detected.
        position: u64,
        /// Reason for corruption.
        reason: String,
    },

    /// CRC32 checksum mismatch.
    #[error("CRC32 checksum mismatch at position {position}: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch {
        /// Position of the corrupted record.
        position: u64,
        /// Expected CRC32 value.
        expected: u32,
        /// Actual CRC32 value.
        actual: u32,
    },

    /// Torn write detected (partial record at end of WAL).
    #[error("Torn write detected at position {position}: {reason}")]
    TornWrite {
        /// Position where torn write was detected.
        position: u64,
        /// Description of the torn write.
        reason: String,
    },
}

impl WriteAheadLog {
    /// Create a new WAL instance.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the WAL file
    /// * `sync_interval` - Interval for group commit
    ///
    /// # Errors
    ///
    /// Returns `WalError::Io` if the file cannot be created or opened.
    pub fn new<P: AsRef<Path>>(path: P, sync_interval: Duration) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        let position = file.metadata()?.len();

        Ok(Self {
            writer: BufWriter::new(file),
            path,
            sync_interval,
            last_sync: Instant::now(),
            position,
            sync_on_write: false,
        })
    }

    /// Enable sync on every write (for testing).
    pub fn set_sync_on_write(&mut self, enabled: bool) {
        self.sync_on_write = enabled;
    }

    /// Append an entry to the WAL.
    ///
    /// Record format: `[length: 4 bytes][crc32: 4 bytes][data: length bytes]`
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to append
    ///
    /// # Errors
    ///
    /// Returns `WalError::Serialization` if the entry cannot be serialized,
    /// or `WalError::Io` if the write fails.
    pub fn append(&mut self, entry: &WalEntry) -> Result<u64, WalError> {
        let start_pos = self.position;

        // Serialize the entry using rkyv
        let bytes: AlignedVec = rkyv::to_bytes::<RkyvError>(entry)
            .map_err(|e| WalError::Serialization(e.to_string()))?;

        // Compute CRC32C checksum of the serialized data
        let crc = crc32c::crc32c(&bytes);

        // Write record: [length: 4 bytes][crc32: 4 bytes][data: length bytes]
        if bytes.len() > u32::MAX as usize {
            return Err(WalError::Serialization(format!(
                "Entry too large: {} bytes (max {})",
                bytes.len(),
                u32::MAX
            )));
        }
        #[allow(clippy::cast_possible_truncation)]
        let len = bytes.len() as u32;

        // Optimize: Coalesce writes into a single buffer to reduce syscalls/locking overhead
        let mut buffer = Vec::with_capacity(RECORD_HEADER_SIZE as usize + bytes.len());
        buffer.extend_from_slice(&len.to_le_bytes());
        buffer.extend_from_slice(&crc.to_le_bytes());
        buffer.extend_from_slice(&bytes);

        self.writer.write_all(&buffer)?;

        let bytes_len = bytes.len() as u64;
        self.position += RECORD_HEADER_SIZE + bytes_len;

        // Check if we need to sync (group commit)
        if self.sync_on_write || self.last_sync.elapsed() >= self.sync_interval {
            self.sync()?;
        }

        Ok(start_pos)
    }

    /// Force sync to disk using fdatasync.
    ///
    /// Uses `sync_data()` instead of `sync_all()` for better performance.
    /// This syncs file data without updating metadata, saving 50-100μs per sync.
    ///
    /// # Errors
    ///
    /// Returns `WalError::Io` if the sync fails.
    pub fn sync(&mut self) -> Result<(), WalError> {
        self.writer.flush()?;
        // Use sync_data() (fdatasync) instead of sync_all() (fsync)
        // This avoids updating file metadata (atime, mtime), saving ~50-100μs per sync
        self.writer.get_ref().sync_data()?;
        self.last_sync = Instant::now();
        Ok(())
    }

    /// Read entries from a specific position.
    ///
    /// # Arguments
    ///
    /// * `position` - Starting position to read from
    ///
    /// # Errors
    ///
    /// Returns `WalError::Io` if the file cannot be opened or seeked.
    pub fn read_from(&self, position: u64) -> Result<WalReader, WalError> {
        let file = File::open(&self.path)?;
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::new(file);

        // Seek to position
        reader.seek(SeekFrom::Start(position))?;

        Ok(WalReader {
            reader,
            position,
            file_len,
        })
    }

    /// Get the current position in the log.
    #[must_use]
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Get the path to the WAL file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Truncate the log at the specified position.
    ///
    /// Used after successful checkpointing to remove old entries.
    ///
    /// # Errors
    ///
    /// Returns `WalError::Io` if the truncation or file operations fail.
    pub fn truncate(&mut self, position: u64) -> Result<(), WalError> {
        self.sync()?;

        // Close current writer
        let file = OpenOptions::new()
            .write(true)
            .truncate(false)
            .open(&self.path)?;

        // Truncate file
        file.set_len(position)?;

        // Reopen for append
        let file = OpenOptions::new().append(true).open(&self.path)?;

        self.writer = BufWriter::new(file);
        self.position = position;

        Ok(())
    }

    /// Repair the WAL by truncating to the last valid record.
    ///
    /// This should be called during recovery to handle torn writes from crashes.
    /// It reads through the WAL, validates each record, and truncates at the
    /// first invalid record (torn write or corruption).
    ///
    /// # Returns
    ///
    /// Returns `Ok(valid_position)` where `valid_position` is the end of the
    /// last valid record (and the new WAL length).
    ///
    /// # Errors
    ///
    /// Returns `WalError::Io` if file operations fail.
    pub fn repair(&mut self) -> Result<u64, WalError> {
        self.sync()?;

        let file = File::open(&self.path)?;
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::new(file);

        let mut valid_position: u64 = 0;
        let mut current_position: u64 = 0;

        loop {
            // Try to read a complete record
            match Self::validate_record(&mut reader, current_position, file_len) {
                Ok(record_len) => {
                    current_position += record_len;
                    valid_position = current_position;
                }
                Err(WalError::TornWrite { .. }) => {
                    // Torn write detected - truncate here
                    break;
                }
                Err(WalError::ChecksumMismatch { .. }) => {
                    // Corruption detected - truncate here
                    break;
                }
                Err(WalError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Clean EOF - we're done
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        // Truncate to last valid position if needed
        if valid_position < file_len {
            self.truncate(valid_position)?;
        }

        Ok(valid_position)
    }

    /// Validate a single record at the current position.
    ///
    /// Returns the total size of the record (header + data) if valid.
    fn validate_record(
        reader: &mut BufReader<File>,
        position: u64,
        file_len: u64,
    ) -> Result<u64, WalError> {
        let remaining = file_len.saturating_sub(position);

        // Check if we have enough bytes for the header
        if remaining < RECORD_HEADER_SIZE {
            if remaining == 0 {
                // Clean EOF
                return Err(WalError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "end of file",
                )));
            }
            return Err(WalError::TornWrite {
                position,
                reason: format!(
                    "incomplete header: only {remaining} bytes remaining, need {RECORD_HEADER_SIZE}"
                ),
            });
        }

        // Read length
        let mut len_bytes = [0u8; 4];
        reader.read_exact(&mut len_bytes)?;
        let len = u64::from(u32::from_le_bytes(len_bytes));

        // Read expected CRC32
        let mut crc_bytes = [0u8; 4];
        reader.read_exact(&mut crc_bytes)?;
        let expected_crc = u32::from_le_bytes(crc_bytes);

        // Check if we have enough bytes for the data
        let data_remaining = remaining - RECORD_HEADER_SIZE;
        if data_remaining < len {
            return Err(WalError::TornWrite {
                position,
                reason: format!(
                    "incomplete data: only {data_remaining} bytes remaining, need {len}"
                ),
            });
        }

        // Read data and validate CRC
        let mut data = vec![0u8; usize::try_from(len).unwrap_or(usize::MAX)];
        reader.read_exact(&mut data)?;

        let actual_crc = crc32c::crc32c(&data);
        if actual_crc != expected_crc {
            return Err(WalError::ChecksumMismatch {
                position,
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        Ok(RECORD_HEADER_SIZE + len)
    }
}

/// WAL reader for recovery replay.
pub struct WalReader {
    reader: BufReader<File>,
    position: u64,
    file_len: u64,
}

impl WalReader {
    /// Get the current position in the WAL.
    #[must_use]
    pub fn position(&self) -> u64 {
        self.position
    }
}

/// Result of reading a WAL record.
#[derive(Debug)]
pub enum WalReadResult {
    /// Successfully read an entry.
    Entry(WalEntry),
    /// Reached end of valid records.
    Eof,
    /// Torn write detected (partial record at end).
    TornWrite {
        /// Position where torn write was detected.
        position: u64,
        /// Description of what was incomplete.
        reason: String,
    },
    /// CRC32 checksum mismatch.
    ChecksumMismatch {
        /// Position of the corrupted record.
        position: u64,
    },
}

impl WalReader {
    /// Read the next entry, with detailed status.
    ///
    /// Unlike the Iterator implementation, this method distinguishes between
    /// clean EOF, torn writes, and checksum errors.
    ///
    /// # Errors
    ///
    /// Returns `WalError::Io` if file reading fails, or `WalError::Deserialization`
    /// if the entry data cannot be deserialized.
    pub fn read_next(&mut self) -> Result<WalReadResult, WalError> {
        let remaining = self.file_len.saturating_sub(self.position);

        // Check for EOF
        if remaining == 0 {
            return Ok(WalReadResult::Eof);
        }

        // Check for incomplete header
        if remaining < RECORD_HEADER_SIZE {
            return Ok(WalReadResult::TornWrite {
                position: self.position,
                reason: format!(
                    "incomplete header: only {remaining} bytes remaining, need {RECORD_HEADER_SIZE}"
                ),
            });
        }

        let record_start = self.position;

        // Read length
        let mut len_bytes = [0u8; 4];
        self.reader.read_exact(&mut len_bytes)?;
        let len = u64::from(u32::from_le_bytes(len_bytes));
        self.position += 4;

        // Read expected CRC32
        let mut crc_bytes = [0u8; 4];
        self.reader.read_exact(&mut crc_bytes)?;
        let expected_crc = u32::from_le_bytes(crc_bytes);
        self.position += 4;

        // Check for incomplete data
        let data_remaining = self.file_len.saturating_sub(self.position);
        if data_remaining < len {
            return Ok(WalReadResult::TornWrite {
                position: record_start,
                reason: format!(
                    "incomplete data: only {data_remaining} bytes remaining, need {len}"
                ),
            });
        }

        // Read data
        let mut data = vec![0u8; usize::try_from(len).unwrap_or(usize::MAX)];
        self.reader.read_exact(&mut data)?;
        self.position += len;

        // Validate CRC
        let actual_crc = crc32c::crc32c(&data);
        if actual_crc != expected_crc {
            return Ok(WalReadResult::ChecksumMismatch {
                position: record_start,
            });
        }

        // Deserialize entry
        match rkyv::from_bytes::<WalEntry, RkyvError>(&data) {
            Ok(entry) => Ok(WalReadResult::Entry(entry)),
            Err(e) => Err(WalError::Deserialization(e.to_string())),
        }
    }
}

impl Iterator for WalReader {
    type Item = Result<WalEntry, WalError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next() {
            Ok(WalReadResult::Entry(entry)) => Some(Ok(entry)),
            Ok(WalReadResult::Eof) => None,
            Ok(WalReadResult::TornWrite { position, reason }) => {
                Some(Err(WalError::TornWrite { position, reason }))
            }
            Ok(WalReadResult::ChecksumMismatch { position }) => {
                Some(Err(WalError::ChecksumMismatch {
                    position,
                    expected: 0, // We don't have the expected value here
                    actual: 0,
                }))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_wal_append_and_read() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = WriteAheadLog::new(temp_file.path(), Duration::from_secs(1)).unwrap();

        // Append entries
        let pos1 = wal
            .append(&WalEntry::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
            })
            .unwrap();

        let _pos2 = wal
            .append(&WalEntry::Delete {
                key: b"key2".to_vec(),
            })
            .unwrap();

        wal.sync().unwrap();

        // Read entries
        let reader = wal.read_from(pos1).unwrap();
        let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(entries.len(), 2);

        match &entries[0] {
            WalEntry::Put { key, value } => {
                assert_eq!(key, b"key1");
                assert_eq!(value, b"value1");
            }
            _ => panic!("Expected Put entry"),
        }

        match &entries[1] {
            WalEntry::Delete { key } => {
                assert_eq!(key, b"key2");
            }
            _ => panic!("Expected Delete entry"),
        }
    }

    #[test]
    fn test_wal_checkpoint() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = WriteAheadLog::new(temp_file.path(), Duration::from_secs(1)).unwrap();

        // Append entries
        wal.append(&WalEntry::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        })
        .unwrap();

        let checkpoint_pos = wal.append(&WalEntry::Checkpoint { id: 1 }).unwrap();

        wal.append(&WalEntry::Put {
            key: b"key2".to_vec(),
            value: b"value2".to_vec(),
        })
        .unwrap();

        wal.sync().unwrap();

        // Read from checkpoint
        let reader = wal.read_from(checkpoint_pos).unwrap();
        let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(entries.len(), 2); // Checkpoint and Put

        match &entries[0] {
            WalEntry::Checkpoint { id } => {
                assert_eq!(*id, 1);
            }
            _ => panic!("Expected Checkpoint entry"),
        }
    }

    #[test]
    fn test_wal_truncate() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = WriteAheadLog::new(temp_file.path(), Duration::from_secs(1)).unwrap();

        // Append entries
        wal.append(&WalEntry::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        })
        .unwrap();

        let truncate_pos = wal.position();

        wal.append(&WalEntry::Put {
            key: b"key2".to_vec(),
            value: b"value2".to_vec(),
        })
        .unwrap();

        wal.sync().unwrap();

        // Truncate at second entry
        wal.truncate(truncate_pos).unwrap();

        // Read all entries
        let reader = wal.read_from(0).unwrap();
        let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(entries.len(), 1); // Only first entry remains
    }

    #[test]
    fn test_wal_commit_offsets() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = WriteAheadLog::new(temp_file.path(), Duration::from_secs(1)).unwrap();

        // Append commit entry with watermark
        let mut offsets = HashMap::new();
        offsets.insert("topic1".to_string(), 100);
        offsets.insert("topic2".to_string(), 200);

        wal.append(&WalEntry::Commit {
            offsets: offsets.clone(),
            watermark: Some(1000),
        })
        .unwrap();
        wal.sync().unwrap();

        // Read and verify
        let reader = wal.read_from(0).unwrap();
        let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(entries.len(), 1);

        match &entries[0] {
            WalEntry::Commit {
                offsets: read_offsets,
                watermark,
            } => {
                assert_eq!(read_offsets.get("topic1"), Some(&100));
                assert_eq!(read_offsets.get("topic2"), Some(&200));
                assert_eq!(*watermark, Some(1000));
            }
            _ => panic!("Expected Commit entry"),
        }
    }

    #[test]
    fn test_wal_crc32_validation() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = WriteAheadLog::new(temp_file.path(), Duration::from_secs(1)).unwrap();

        // Write a valid entry
        wal.append(&WalEntry::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        })
        .unwrap();
        wal.sync().unwrap();

        // Corrupt the data by modifying a byte in the middle
        {
            use std::io::Write;
            let mut file = OpenOptions::new()
                .write(true)
                .open(temp_file.path())
                .unwrap();
            // Seek past header (8 bytes) and corrupt the data
            file.seek(SeekFrom::Start(10)).unwrap();
            file.write_all(&[0xFF]).unwrap();
            file.sync_all().unwrap();
        }

        // Reading should detect CRC mismatch
        let mut reader = wal.read_from(0).unwrap();
        match reader.read_next().unwrap() {
            WalReadResult::ChecksumMismatch { position } => {
                assert_eq!(position, 0);
            }
            other => panic!("Expected ChecksumMismatch, got {other:?}"),
        }
    }

    #[test]
    fn test_wal_torn_write_detection_incomplete_header() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = WriteAheadLog::new(temp_file.path(), Duration::from_secs(1)).unwrap();

        // Write a valid entry
        wal.append(&WalEntry::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        })
        .unwrap();
        wal.sync().unwrap();

        let valid_pos = wal.position();

        // Simulate torn write: write only 3 bytes of next header (incomplete)
        {
            use std::io::Write;
            let mut file = OpenOptions::new()
                .append(true)
                .open(temp_file.path())
                .unwrap();
            file.write_all(&[0x10, 0x00, 0x00]).unwrap(); // 3 bytes of length
            file.sync_all().unwrap();
        }

        // Reading should get the first entry, then detect torn write
        let mut reader = wal.read_from(0).unwrap();

        // First entry should be valid
        match reader.read_next().unwrap() {
            WalReadResult::Entry(WalEntry::Put { key, .. }) => {
                assert_eq!(key, b"key1");
            }
            other => panic!("Expected valid entry, got {other:?}"),
        }

        // Second read should detect torn write
        match reader.read_next().unwrap() {
            WalReadResult::TornWrite { position, .. } => {
                assert_eq!(position, valid_pos);
            }
            other => panic!("Expected TornWrite, got {other:?}"),
        }
    }

    #[test]
    fn test_wal_torn_write_detection_incomplete_data() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = WriteAheadLog::new(temp_file.path(), Duration::from_secs(1)).unwrap();

        // Write a valid entry
        wal.append(&WalEntry::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        })
        .unwrap();
        wal.sync().unwrap();

        let valid_pos = wal.position();

        // Simulate torn write: write header claiming 100 bytes but only provide 10
        {
            use std::io::Write;
            let mut file = OpenOptions::new()
                .append(true)
                .open(temp_file.path())
                .unwrap();
            // Write length (100 bytes) + CRC (dummy) + only 10 bytes of data
            let len: u32 = 100;
            let crc: u32 = 0x1234_5678;
            file.write_all(&len.to_le_bytes()).unwrap();
            file.write_all(&crc.to_le_bytes()).unwrap();
            file.write_all(&[0u8; 10]).unwrap(); // Only 10 bytes, not 100
            file.sync_all().unwrap();
        }

        // Reading should get the first entry, then detect torn write
        let mut reader = wal.read_from(0).unwrap();

        // First entry should be valid
        match reader.read_next().unwrap() {
            WalReadResult::Entry(WalEntry::Put { key, .. }) => {
                assert_eq!(key, b"key1");
            }
            other => panic!("Expected valid entry, got {other:?}"),
        }

        // Second read should detect torn write (incomplete data)
        match reader.read_next().unwrap() {
            WalReadResult::TornWrite { position, reason } => {
                assert_eq!(position, valid_pos);
                assert!(reason.contains("incomplete data"));
            }
            other => panic!("Expected TornWrite, got {other:?}"),
        }
    }

    #[test]
    fn test_wal_repair() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = WriteAheadLog::new(temp_file.path(), Duration::from_secs(1)).unwrap();

        // Write two valid entries
        wal.append(&WalEntry::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        })
        .unwrap();
        wal.append(&WalEntry::Put {
            key: b"key2".to_vec(),
            value: b"value2".to_vec(),
        })
        .unwrap();
        wal.sync().unwrap();

        let valid_len = wal.position();

        // Simulate torn write: append garbage
        {
            use std::io::Write;
            let mut file = OpenOptions::new()
                .append(true)
                .open(temp_file.path())
                .unwrap();
            file.write_all(&[0xFF, 0xFF, 0xFF]).unwrap();
            file.sync_all().unwrap();
        }

        // Repair should truncate to last valid record
        let repaired_len = wal.repair().unwrap();
        assert_eq!(repaired_len, valid_len);

        // Verify we can still read both valid entries
        let reader = wal.read_from(0).unwrap();
        let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_wal_repair_with_crc_corruption() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = WriteAheadLog::new(temp_file.path(), Duration::from_secs(1)).unwrap();

        // Write one valid entry
        wal.append(&WalEntry::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        })
        .unwrap();
        wal.sync().unwrap();

        let first_entry_end = wal.position();

        // Write another entry
        wal.append(&WalEntry::Put {
            key: b"key2".to_vec(),
            value: b"value2".to_vec(),
        })
        .unwrap();
        wal.sync().unwrap();

        // Corrupt the CRC of the second entry
        {
            use std::io::Write;
            let mut file = OpenOptions::new()
                .write(true)
                .open(temp_file.path())
                .unwrap();
            // Seek to CRC of second entry (first_entry_end + 4 bytes for length)
            file.seek(SeekFrom::Start(first_entry_end + 4)).unwrap();
            file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
            file.sync_all().unwrap();
        }

        // Repair should truncate at the corrupted entry
        let repaired_len = wal.repair().unwrap();
        assert_eq!(repaired_len, first_entry_end);

        // Verify only first entry remains
        let reader = wal.read_from(0).unwrap();
        let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(entries.len(), 1);

        match &entries[0] {
            WalEntry::Put { key, value } => {
                assert_eq!(key, b"key1");
                assert_eq!(value, b"value1");
            }
            _ => panic!("Expected Put entry"),
        }
    }

    #[test]
    fn test_wal_read_next_vs_iterator() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = WriteAheadLog::new(temp_file.path(), Duration::from_secs(1)).unwrap();

        wal.append(&WalEntry::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        })
        .unwrap();
        wal.sync().unwrap();

        // Test read_next()
        let mut reader1 = wal.read_from(0).unwrap();
        match reader1.read_next().unwrap() {
            WalReadResult::Entry(WalEntry::Put { key, .. }) => {
                assert_eq!(key, b"key1");
            }
            other => panic!("Expected Entry, got {other:?}"),
        }
        match reader1.read_next().unwrap() {
            WalReadResult::Eof => {}
            other => panic!("Expected Eof, got {other:?}"),
        }

        // Test Iterator
        let reader2 = wal.read_from(0).unwrap();
        let entries: Vec<_> = reader2.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_wal_empty_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let wal = WriteAheadLog::new(temp_file.path(), Duration::from_secs(1)).unwrap();

        // Reading from empty WAL should return no entries
        let mut reader = wal.read_from(0).unwrap();
        match reader.read_next().unwrap() {
            WalReadResult::Eof => {}
            other => panic!("Expected Eof, got {other:?}"),
        }
    }

    #[test]
    fn test_wal_watermark_in_commit() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = WriteAheadLog::new(temp_file.path(), Duration::from_secs(1)).unwrap();

        // Commit without watermark
        wal.append(&WalEntry::Commit {
            offsets: HashMap::new(),
            watermark: None,
        })
        .unwrap();

        // Commit with watermark
        wal.append(&WalEntry::Commit {
            offsets: HashMap::new(),
            watermark: Some(12345),
        })
        .unwrap();
        wal.sync().unwrap();

        let reader = wal.read_from(0).unwrap();
        let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(entries.len(), 2);

        match &entries[0] {
            WalEntry::Commit { watermark, .. } => assert_eq!(*watermark, None),
            _ => panic!("Expected Commit"),
        }

        match &entries[1] {
            WalEntry::Commit { watermark, .. } => assert_eq!(*watermark, Some(12345)),
            _ => panic!("Expected Commit"),
        }
    }
}
