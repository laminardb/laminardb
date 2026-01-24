//! Write-Ahead Log for durability and exactly-once semantics.
//!
//! The WAL provides durability by persisting all state mutations before they
//! are applied. This enables recovery after crashes and supports exactly-once
//! processing semantics.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rkyv::{
    Archive,
    Deserialize as RkyvDeserialize,
    Serialize as RkyvSerialize,
    rancor::Error as RkyvError,
    util::AlignedVec,
};

// Module to contain types that use derive macros with generated code
mod wal_types {
    #![allow(missing_docs)]  // Allow for derive-generated code

    use std::collections::HashMap;
    use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

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
        },
    }
}

pub use wal_types::WalEntry;

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

    /// Corrupted WAL entry detected.
    #[error("Corrupted WAL entry at position {0}")]
    Corrupted(u64),
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
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

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

        // Write entry length (4 bytes) followed by the entry
        #[allow(clippy::cast_possible_truncation)]
        let len = bytes.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&bytes)?;

        #[allow(clippy::cast_possible_truncation)]
        let bytes_len = bytes.len() as u64;
        self.position += 4 + bytes_len;

        // Check if we need to sync (group commit)
        if self.sync_on_write || self.last_sync.elapsed() >= self.sync_interval {
            self.sync()?;
        }

        Ok(start_pos)
    }

    /// Force sync to disk.
    ///
    /// # Errors
    ///
    /// Returns `WalError::Io` if the sync fails.
    pub fn sync(&mut self) -> Result<(), WalError> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
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
        use std::io::Seek;

        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);

        // Seek to position
        reader.seek(std::io::SeekFrom::Start(position))?;

        Ok(WalReader {
            reader,
            position,
        })
    }

    /// Get the current position in the log.
    #[must_use]
    pub fn position(&self) -> u64 {
        self.position
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
        let file = OpenOptions::new()
            .append(true)
            .open(&self.path)?;

        self.writer = BufWriter::new(file);
        self.position = position;

        Ok(())
    }
}

/// WAL reader for recovery replay.
pub struct WalReader {
    reader: BufReader<File>,
    position: u64,
}

impl Iterator for WalReader {
    type Item = Result<WalEntry, WalError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Read entry length
        let mut len_bytes = [0u8; 4];
        match self.reader.read_exact(&mut len_bytes) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
            Err(e) => return Some(Err(e.into())),
        }

        let len = u32::from_le_bytes(len_bytes) as usize;
        self.position += 4;

        // Read entry data
        let mut entry_bytes = vec![0u8; len];
        if let Err(_e) = self.reader.read_exact(&mut entry_bytes) {
            return Some(Err(WalError::Corrupted(self.position)));
        }

        self.position += len as u64;

        // Deserialize entry
        match rkyv::from_bytes::<WalEntry, RkyvError>(&entry_bytes) {
            Ok(entry) => Some(Ok(entry)),
            Err(e) => Some(Err(WalError::Deserialization(e.to_string()))),
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

        // Append commit entry
        let mut offsets = HashMap::new();
        offsets.insert("topic1".to_string(), 100);
        offsets.insert("topic2".to_string(), 200);

        wal.append(&WalEntry::Commit {
            offsets: offsets.clone(),
        })
        .unwrap();
        wal.sync().unwrap();

        // Read and verify
        let reader = wal.read_from(0).unwrap();
        let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(entries.len(), 1);

        match &entries[0] {
            WalEntry::Commit { offsets: read_offsets } => {
                assert_eq!(read_offsets.get("topic1"), Some(&100));
                assert_eq!(read_offsets.get("topic2"), Some(&200));
            }
            _ => panic!("Expected Commit entry"),
        }
    }
}