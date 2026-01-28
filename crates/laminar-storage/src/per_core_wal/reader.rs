//! Per-core WAL reader for segment file reading.

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use rkyv::rancor::Error as RkyvError;

use super::entry::PerCoreWalEntry;
use super::error::PerCoreWalError;

/// Size of the record header (length + CRC32).
const RECORD_HEADER_SIZE: u64 = 8;

/// Result of reading a WAL record.
#[derive(Debug)]
pub enum WalReadResult {
    /// Successfully read an entry.
    Entry(PerCoreWalEntry),
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

/// Per-core WAL reader for a single segment file.
///
/// Reads entries from a WAL segment, validating CRC32 checksums and
/// detecting torn writes.
pub struct PerCoreWalReader {
    /// Core ID for this segment.
    core_id: usize,
    /// Buffered reader.
    reader: BufReader<File>,
    /// Current position in the file.
    position: u64,
    /// File length.
    file_len: u64,
}

impl PerCoreWalReader {
    /// Opens a WAL segment for reading.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened.
    pub fn open(core_id: usize, path: &Path) -> Result<Self, PerCoreWalError> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        let reader = BufReader::new(file);

        Ok(Self {
            core_id,
            reader,
            position: 0,
            file_len,
        })
    }

    /// Opens a WAL segment and seeks to a specific position.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or seeked.
    pub fn open_from(core_id: usize, path: &Path, position: u64) -> Result<Self, PerCoreWalError> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(position))?;

        Ok(Self {
            core_id,
            reader,
            position,
            file_len,
        })
    }

    /// Returns the core ID for this segment.
    #[must_use]
    pub fn core_id(&self) -> usize {
        self.core_id
    }

    /// Returns the current position in the file.
    #[must_use]
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Returns the file length.
    #[must_use]
    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    /// Reads the next entry with detailed status.
    ///
    /// Unlike the Iterator implementation, this method distinguishes between
    /// clean EOF, torn writes, and checksum errors.
    ///
    /// # Errors
    ///
    /// Returns an error if file reading fails or deserialization fails.
    pub fn read_next(&mut self) -> Result<WalReadResult, PerCoreWalError> {
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
        #[allow(clippy::cast_possible_truncation)]
        let mut data = vec![0u8; len as usize];
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
        match rkyv::from_bytes::<PerCoreWalEntry, RkyvError>(&data) {
            Ok(entry) => Ok(WalReadResult::Entry(entry)),
            Err(e) => Err(PerCoreWalError::Deserialization(e.to_string())),
        }
    }

    /// Reads all valid entries from the segment.
    ///
    /// Stops at EOF, torn write, or checksum mismatch.
    ///
    /// # Errors
    ///
    /// Returns an error if reading fails (not on torn writes).
    pub fn read_all(&mut self) -> Result<Vec<PerCoreWalEntry>, PerCoreWalError> {
        let mut entries = Vec::new();

        while let WalReadResult::Entry(entry) = self.read_next()? {
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Reads entries from the segment up to a specific epoch.
    ///
    /// # Errors
    ///
    /// Returns an error if reading fails.
    pub fn read_up_to_epoch(&mut self, max_epoch: u64) -> Result<Vec<PerCoreWalEntry>, PerCoreWalError> {
        let mut entries = Vec::new();

        while let WalReadResult::Entry(entry) = self.read_next()? {
            if entry.epoch > max_epoch {
                break;
            }
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Finds the last valid position in the segment.
    ///
    /// Used for repair/recovery to truncate at torn writes.
    ///
    /// # Errors
    ///
    /// Returns an error if reading fails.
    pub fn find_valid_end(&mut self) -> Result<u64, PerCoreWalError> {
        let mut valid_position = self.position;

        loop {
            let pos_before = self.position;
            match self.read_next()? {
                WalReadResult::Entry(_) => {
                    valid_position = self.position;
                }
                WalReadResult::Eof => {
                    break;
                }
                WalReadResult::TornWrite { .. } | WalReadResult::ChecksumMismatch { .. } => {
                    // Truncate at the start of the invalid record
                    valid_position = pos_before;
                    break;
                }
            }
        }

        Ok(valid_position)
    }
}

impl Iterator for PerCoreWalReader {
    type Item = Result<PerCoreWalEntry, PerCoreWalError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next() {
            Ok(WalReadResult::Entry(entry)) => Some(Ok(entry)),
            Ok(WalReadResult::Eof) => None,
            Ok(WalReadResult::TornWrite { position, reason }) => {
                Some(Err(PerCoreWalError::TornWrite {
                    core_id: self.core_id,
                    position,
                    reason,
                }))
            }
            Ok(WalReadResult::ChecksumMismatch { position }) => {
                Some(Err(PerCoreWalError::ChecksumMismatch {
                    core_id: self.core_id,
                    position,
                    expected: 0,
                    actual: 0,
                }))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

impl std::fmt::Debug for PerCoreWalReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PerCoreWalReader")
            .field("core_id", &self.core_id)
            .field("position", &self.position)
            .field("file_len", &self.file_len)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::per_core_wal::writer::CoreWalWriter;
    use tempfile::TempDir;

    fn setup_test_segment(core_id: usize) -> (TempDir, std::path::PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join(format!("wal-{core_id}.log"));

        // Write some test entries
        {
            let mut writer = CoreWalWriter::new(core_id, &path).unwrap();
            writer.set_epoch(1);
            writer.append_put(b"key1", b"value1").unwrap();
            writer.append_put(b"key2", b"value2").unwrap();
            writer.set_epoch(2);
            writer.append_put(b"key3", b"value3").unwrap();
            writer.sync().unwrap();
        }

        (temp_dir, path)
    }

    #[test]
    fn test_reader_open() {
        let (_temp_dir, path) = setup_test_segment(0);
        let reader = PerCoreWalReader::open(0, &path).unwrap();
        assert_eq!(reader.core_id(), 0);
        assert_eq!(reader.position(), 0);
        assert!(reader.file_len() > 0);
    }

    #[test]
    fn test_read_all() {
        let (_temp_dir, path) = setup_test_segment(0);
        let mut reader = PerCoreWalReader::open(0, &path).unwrap();

        let entries = reader.read_all().unwrap();
        assert_eq!(entries.len(), 3);

        assert!(entries[0].is_put());
        assert_eq!(entries[0].key(), Some(b"key1".as_slice()));
        assert_eq!(entries[0].epoch, 1);

        assert_eq!(entries[2].epoch, 2);
    }

    #[test]
    fn test_read_up_to_epoch() {
        let (_temp_dir, path) = setup_test_segment(0);
        let mut reader = PerCoreWalReader::open(0, &path).unwrap();

        let entries = reader.read_up_to_epoch(1).unwrap();
        assert_eq!(entries.len(), 2); // Only epoch 1 entries
    }

    #[test]
    fn test_iterator() {
        let (_temp_dir, path) = setup_test_segment(0);
        let reader = PerCoreWalReader::open(0, &path).unwrap();

        let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_open_from_position() {
        let (_temp_dir, path) = setup_test_segment(0);

        // Read first entry to get position after it
        let mut reader1 = PerCoreWalReader::open(0, &path).unwrap();
        let _ = reader1.read_next().unwrap();
        let pos_after_first = reader1.position();

        // Open from that position
        let mut reader2 = PerCoreWalReader::open_from(0, &path, pos_after_first).unwrap();
        let entries = reader2.read_all().unwrap();

        assert_eq!(entries.len(), 2); // Should skip first entry
    }

    #[test]
    fn test_empty_segment() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("wal-0.log");

        // Create empty file
        {
            let _writer = CoreWalWriter::new(0, &path).unwrap();
        }

        let mut reader = PerCoreWalReader::open(0, &path).unwrap();
        match reader.read_next().unwrap() {
            WalReadResult::Eof => {}
            other => panic!("Expected Eof, got {other:?}"),
        }
    }

    #[test]
    fn test_find_valid_end() {
        let (_temp_dir, path) = setup_test_segment(0);
        let mut reader = PerCoreWalReader::open(0, &path).unwrap();

        let valid_end = reader.find_valid_end().unwrap();
        assert_eq!(valid_end, reader.file_len()); // All entries are valid
    }

    #[test]
    fn test_torn_write_detection() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("wal-0.log");

        // Write valid entry
        {
            let mut writer = CoreWalWriter::new(0, &path).unwrap();
            writer.append_put(b"key1", b"value1").unwrap();
            writer.sync().unwrap();
        }

        // Append incomplete data (simulate torn write)
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            // Write incomplete header
            file.write_all(&[0xFF, 0xFF, 0xFF]).unwrap();
            file.sync_all().unwrap();
        }

        let mut reader = PerCoreWalReader::open(0, &path).unwrap();

        // First entry should be valid
        match reader.read_next().unwrap() {
            WalReadResult::Entry(entry) => {
                assert_eq!(entry.key(), Some(b"key1".as_slice()));
            }
            other => panic!("Expected Entry, got {other:?}"),
        }

        // Second read should detect torn write
        match reader.read_next().unwrap() {
            WalReadResult::TornWrite { .. } => {}
            other => panic!("Expected TornWrite, got {other:?}"),
        }
    }

    #[test]
    fn test_checksum_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("wal-0.log");

        // Write valid entry
        {
            let mut writer = CoreWalWriter::new(0, &path).unwrap();
            writer.append_put(b"key1", b"value1").unwrap();
            writer.sync().unwrap();
        }

        // Corrupt the data
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap();
            // Seek past header and corrupt data
            file.seek(SeekFrom::Start(10)).unwrap();
            file.write_all(&[0xFF]).unwrap();
            file.sync_all().unwrap();
        }

        let mut reader = PerCoreWalReader::open(0, &path).unwrap();

        match reader.read_next().unwrap() {
            WalReadResult::ChecksumMismatch { position } => {
                assert_eq!(position, 0);
            }
            other => panic!("Expected ChecksumMismatch, got {other:?}"),
        }
    }

    #[test]
    fn test_debug_format() {
        let (_temp_dir, path) = setup_test_segment(0);
        let reader = PerCoreWalReader::open(0, &path).unwrap();
        let debug_str = format!("{reader:?}");
        assert!(debug_str.contains("PerCoreWalReader"));
        assert!(debug_str.contains("core_id"));
    }
}
