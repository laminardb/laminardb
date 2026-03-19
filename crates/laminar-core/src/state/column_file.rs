//! Column-file window state store.
//!
//! Inspired by QuestDB's column-oriented file storage, this module
//! extends the mmap state store with column-oriented layout for window
//! operator state. Each accumulator field gets its own memory-mapped
//! column file, and column files are time-partitioned by window interval.
//!
//! # Design
//!
//! ```text
//! Window State Directory
//! ├── partition_2024010100/     (window interval: 1 hour)
//! │   ├── sum.col              (mmap'd column file)
//! │   ├── count.col
//! │   └── min.col
//! ├── partition_2024010101/
//! │   ├── sum.col
//! │   ├── count.col
//! │   └── min.col
//! └── meta.json                (partition metadata)
//! ```
//!
//! Hot partitions (recent windows) stay memory-resident. Cold partitions
//! (past windows) are backed by mmap files on disk, enabling window
//! state larger than available RAM.
//!
//! # Performance
//!
//! - **Read hot**: Direct memory access, ~100ns
//! - **Read cold**: Page fault on first access, then cached by OS
//! - **Write**: Always to hot partition, same as mmap store
//! - **Partition rotation**: O(1) metadata update

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use memmap2::MmapMut;

use super::StateError;

/// Default column file size (1 MiB).
const DEFAULT_COLUMN_FILE_SIZE: usize = 1024 * 1024;

/// Maximum number of hot partitions kept in memory.
const DEFAULT_MAX_HOT_PARTITIONS: usize = 4;

/// Configuration for the column-file window state store.
#[derive(Debug, Clone)]
pub struct ColumnFileConfig {
    /// Base directory for partition files.
    pub base_dir: PathBuf,
    /// Initial size for each column file.
    pub column_file_size: usize,
    /// Maximum number of hot (memory-resident) partitions.
    pub max_hot_partitions: usize,
    /// Column names for the accumulator fields.
    pub column_names: Vec<String>,
}

impl ColumnFileConfig {
    /// Create a new configuration.
    #[must_use]
    pub fn new(base_dir: &Path, column_names: Vec<String>) -> Self {
        Self {
            base_dir: base_dir.to_path_buf(),
            column_file_size: DEFAULT_COLUMN_FILE_SIZE,
            max_hot_partitions: DEFAULT_MAX_HOT_PARTITIONS,
            column_names,
        }
    }

    /// Set the initial column file size.
    #[must_use]
    pub fn with_column_file_size(mut self, size: usize) -> Self {
        self.column_file_size = size;
        self
    }

    /// Set the maximum hot partitions.
    #[must_use]
    pub fn with_max_hot_partitions(mut self, max: usize) -> Self {
        self.max_hot_partitions = max;
        self
    }
}

/// Size of the write-position header at the start of each column file.
const HEADER_SIZE: usize = 8;

/// A single mmap'd column file.
///
/// The first 8 bytes of the file store the current `write_pos` as a
/// little-endian u64. This allows the file to be reopened without data
/// loss — the write position is recovered from the header rather than
/// being reset to zero.
struct ColumnFile {
    /// Memory-mapped region.
    mmap: MmapMut,
    /// Backing file.
    _file: File,
    /// Current write position (offset past the header).
    write_pos: usize,
    /// Total capacity.
    capacity: usize,
}

impl ColumnFile {
    /// Create or open a column file.
    fn open(path: &Path, initial_size: usize) -> Result<Self, StateError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let file_meta = file.metadata()?;
        let is_new = file_meta.len() == 0;
        let capacity = if is_new {
            let total = initial_size + HEADER_SIZE;
            file.set_len(total as u64)?;
            total
        } else {
            #[allow(clippy::cast_possible_truncation)]
            let cap = file_meta.len() as usize;
            if cap < HEADER_SIZE {
                return Err(StateError::Corruption(format!(
                    "column file too small ({cap} bytes, minimum {HEADER_SIZE}): {}",
                    path.display()
                )));
            }
            cap
        };

        // SAFETY: We have exclusive access to the file (opened with write).
        // The file descriptor is valid because we just successfully opened it.
        #[allow(unsafe_code)]
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        let write_pos = if is_new {
            // Initialize header to zero (data starts right after header).
            HEADER_SIZE
        } else {
            // Read persisted write_pos from header.
            let stored = u64::from_le_bytes(
                mmap[..HEADER_SIZE]
                    .try_into()
                    .map_err(|_| StateError::Corruption("invalid column file header".into()))?,
            );
            #[allow(clippy::cast_possible_truncation)]
            let pos = stored as usize;
            // Sanity: clamp to capacity, minimum is HEADER_SIZE.
            pos.max(HEADER_SIZE).min(capacity)
        };

        Ok(Self {
            mmap,
            _file: file,
            write_pos,
            capacity,
        })
    }

    /// Persist the current `write_pos` to the 8-byte file header.
    fn persist_header(&mut self) {
        let bytes = (self.write_pos as u64).to_le_bytes();
        self.mmap[..HEADER_SIZE].copy_from_slice(&bytes);
    }

    /// Write data at the current position, growing if needed.
    fn write(&mut self, data: &[u8]) -> Result<usize, StateError> {
        let offset = self.write_pos;
        let end = offset + data.len();

        if end > self.capacity {
            return Err(StateError::Io(std::io::Error::other(
                "column file full — rotate partition",
            )));
        }

        self.mmap[offset..end].copy_from_slice(data);
        self.write_pos = end;
        self.persist_header();
        Ok(offset)
    }

    /// Read data from a given offset.
    fn read(&self, offset: usize, len: usize) -> Option<&[u8]> {
        let end = offset.checked_add(len)?;
        self.mmap.get(offset..end)
    }

    /// Flush to disk.
    fn flush(&mut self) -> Result<(), StateError> {
        self.persist_header();
        self.mmap.flush()?;
        Ok(())
    }

    /// Reset write position to just after the header.
    #[allow(dead_code)]
    fn reset(&mut self) {
        self.write_pos = HEADER_SIZE;
        self.persist_header();
    }
}

/// Tier classification for a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionTier {
    /// Hot: memory-resident, actively written to.
    Hot,
    /// Cold: disk-backed, read via page faults.
    Cold,
}

/// A time-partitioned set of column files.
struct Partition {
    /// Partition ID (typically a window start timestamp).
    id: i64,
    /// Directory containing this partition's files.
    dir: PathBuf,
    /// Column files, one per accumulator field.
    columns: Vec<ColumnFile>,
    /// Column name to index mapping.
    column_indices: BTreeMap<String, usize>,
    /// Current tier.
    tier: PartitionTier,
    /// Key-value index within this partition.
    index: BTreeMap<Vec<u8>, Vec<ValueEntry>>,
    /// Total size tracked.
    size_bytes: usize,
}

/// Entry tracking where a value is stored within a column.
#[derive(Debug, Clone)]
struct ValueEntry {
    /// Column index.
    column_idx: usize,
    /// Offset within the column file.
    offset: usize,
    /// Length of the value.
    len: usize,
}

/// Name of the index sidecar file inside each partition directory.
const INDEX_FILE_NAME: &str = "index.bin";

impl Partition {
    fn new(id: i64, dir: &Path, config: &ColumnFileConfig) -> Result<Self, StateError> {
        fs::create_dir_all(dir)?;

        let mut columns = Vec::with_capacity(config.column_names.len());
        let mut column_indices = BTreeMap::new();

        for (idx, name) in config.column_names.iter().enumerate() {
            let col_path = dir.join(format!("{name}.col"));
            let col = ColumnFile::open(&col_path, config.column_file_size)?;
            columns.push(col);
            column_indices.insert(name.clone(), idx);
        }

        // Restore the key-to-offset index from the sidecar file if it exists.
        let (index, size_bytes) = Self::load_index(dir)?;

        Ok(Self {
            id,
            dir: dir.to_path_buf(),
            columns,
            column_indices,
            tier: PartitionTier::Hot,
            index,
            size_bytes,
        })
    }

    /// Load the persisted index from the sidecar file.
    ///
    /// Format (repeated): `[key_len: u32][key][col_idx: u32][offset: u32][len: u32]`
    #[allow(clippy::type_complexity)]
    fn load_index(dir: &Path) -> Result<(BTreeMap<Vec<u8>, Vec<ValueEntry>>, usize), StateError> {
        let path = dir.join(INDEX_FILE_NAME);
        if !path.exists() {
            return Ok((BTreeMap::new(), 0));
        }

        let mut data = Vec::new();
        File::open(&path)?.read_to_end(&mut data)?;

        let mut index: BTreeMap<Vec<u8>, Vec<ValueEntry>> = BTreeMap::new();
        let mut size_bytes: usize = 0;
        let mut pos = 0;

        while pos + 4 <= data.len() {
            // key_len
            let key_len = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .map_err(|_| StateError::Corruption("index: bad key_len".into()))?,
            ) as usize;
            pos += 4;

            if pos + key_len + 12 > data.len() {
                return Err(StateError::Corruption("index: truncated entry".into()));
            }

            let key = data[pos..pos + key_len].to_vec();
            pos += key_len;

            let col_idx = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .map_err(|_| StateError::Corruption("index: bad col_idx".into()))?,
            ) as usize;
            pos += 4;

            let offset = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .map_err(|_| StateError::Corruption("index: bad offset".into()))?,
            ) as usize;
            pos += 4;

            let len = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .map_err(|_| StateError::Corruption("index: bad len".into()))?,
            ) as usize;
            pos += 4;

            size_bytes += key.len() + len;
            index.entry(key).or_default().push(ValueEntry {
                column_idx: col_idx,
                offset,
                len,
            });
        }

        Ok((index, size_bytes))
    }

    /// Persist the in-memory index to the sidecar file.
    #[allow(clippy::cast_possible_truncation)] // Column file sizes are << 4 GiB.
    fn save_index(&self, dir: &Path) -> Result<(), StateError> {
        let path = dir.join(INDEX_FILE_NAME);
        let mut buf = Vec::new();

        for (key, entries) in &self.index {
            for entry in entries {
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(&(entry.column_idx as u32).to_le_bytes());
                buf.extend_from_slice(&(entry.offset as u32).to_le_bytes());
                buf.extend_from_slice(&(entry.len as u32).to_le_bytes());
            }
        }

        let mut file = File::create(&path)?;
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }

    /// Write a key-value pair to a specific column.
    fn put_column(
        &mut self,
        key: &[u8],
        column_name: &str,
        value: &[u8],
    ) -> Result<(), StateError> {
        let col_idx = *self
            .column_indices
            .get(column_name)
            .ok_or_else(|| StateError::Corruption(format!("unknown column: {column_name}")))?;

        let offset = self.columns[col_idx].write(value)?;
        let entry = ValueEntry {
            column_idx: col_idx,
            offset,
            len: value.len(),
        };

        self.index.entry(key.to_vec()).or_default().push(entry);
        self.size_bytes += key.len() + value.len();
        Ok(())
    }

    /// Read a key's value from a specific column.
    fn get_column(&self, key: &[u8], column_name: &str) -> Option<&[u8]> {
        let col_idx = *self.column_indices.get(column_name)?;
        let entries = self.index.get(key)?;
        // Find the latest entry for this column.
        let entry = entries.iter().rev().find(|e| e.column_idx == col_idx)?;
        self.columns[col_idx].read(entry.offset, entry.len)
    }

    fn flush(&mut self) -> Result<(), StateError> {
        for col in &mut self.columns {
            col.flush()?;
        }
        self.save_index(&self.dir.clone())?;
        Ok(())
    }
}

/// Column-file window state store.
///
/// Provides column-oriented, time-partitioned state storage for window
/// operators. Hot partitions are kept in memory for fast access; cold
/// partitions are disk-backed via mmap.
pub struct ColumnFileStateStore {
    /// Configuration.
    config: ColumnFileConfig,
    /// Partitions ordered by ID (window start timestamp).
    partitions: BTreeMap<i64, Partition>,
    /// Currently active (hot) partition ID.
    active_partition: Option<i64>,
    /// Total entries across all partitions.
    total_entries: usize,
    /// Total size in bytes across all partitions.
    total_size_bytes: usize,
}

impl ColumnFileStateStore {
    /// Create or reopen a column-file state store.
    ///
    /// If the base directory already contains partition sub-directories
    /// (from a previous run), they are reopened and their indexes are
    /// restored from sidecar files. This makes the store restart-safe.
    ///
    /// # Errors
    ///
    /// Returns `StateError::Io` if the base directory cannot be created
    /// or if an existing partition is corrupted.
    pub fn new(config: ColumnFileConfig) -> Result<Self, StateError> {
        fs::create_dir_all(&config.base_dir)?;

        let mut partitions = BTreeMap::new();
        let mut total_entries: usize = 0;
        let mut total_size_bytes: usize = 0;

        // Scan for existing partition directories and reopen them.
        for entry in fs::read_dir(&config.base_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if entry.file_type()?.is_dir() {
                if let Some(id_str) = name_str.strip_prefix("partition_") {
                    if let Ok(id) = id_str.parse::<i64>() {
                        let dir = entry.path();
                        let partition = Partition::new(id, &dir, &config)?;
                        let entry_count: usize = partition.index.values().map(Vec::len).sum();
                        total_entries += entry_count;
                        total_size_bytes += partition.size_bytes;
                        partitions.insert(id, partition);
                    }
                }
            }
        }

        let active_partition = partitions.keys().next_back().copied();

        Ok(Self {
            config,
            partitions,
            active_partition,
            total_entries,
            total_size_bytes,
        })
    }

    /// Get or create a partition for the given window timestamp.
    ///
    /// If the partition doesn't exist, it's created as a hot partition.
    /// If creating it would exceed `max_hot_partitions`, the oldest hot
    /// partition is demoted to cold.
    ///
    /// # Errors
    ///
    /// Returns `StateError::Io` if directory or file creation fails.
    pub fn ensure_partition(&mut self, partition_id: i64) -> Result<(), StateError> {
        if self.partitions.contains_key(&partition_id) {
            return Ok(());
        }

        let dir = self
            .config
            .base_dir
            .join(format!("partition_{partition_id}"));
        let partition = Partition::new(partition_id, &dir, &self.config)?;
        self.partitions.insert(partition_id, partition);
        self.active_partition = Some(partition_id);

        // Demote oldest hot partitions if needed.
        self.enforce_hot_limit();

        Ok(())
    }

    /// Write a value for a key in a specific column and partition.
    ///
    /// # Errors
    ///
    /// Returns an error if the partition doesn't exist or the write fails.
    pub fn put_column(
        &mut self,
        partition_id: i64,
        key: &[u8],
        column_name: &str,
        value: &[u8],
    ) -> Result<(), StateError> {
        let partition = self
            .partitions
            .get_mut(&partition_id)
            .ok_or_else(|| StateError::Corruption(format!("partition {partition_id} not found")))?;
        partition.put_column(key, column_name, value)?;
        self.total_entries += 1;
        self.total_size_bytes += key.len() + value.len();
        Ok(())
    }

    /// Read a value for a key from a specific column and partition.
    #[must_use]
    pub fn get_column(&self, partition_id: i64, key: &[u8], column_name: &str) -> Option<&[u8]> {
        let partition = self.partitions.get(&partition_id)?;
        partition.get_column(key, column_name)
    }

    /// Get the tier of a partition.
    #[must_use]
    pub fn partition_tier(&self, partition_id: i64) -> Option<PartitionTier> {
        self.partitions.get(&partition_id).map(|p| p.tier)
    }

    /// Get the number of partitions.
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Get partition IDs in order.
    #[must_use]
    pub fn partition_ids(&self) -> Vec<i64> {
        self.partitions.keys().copied().collect()
    }

    /// Get the number of hot partitions.
    #[must_use]
    pub fn hot_partition_count(&self) -> usize {
        self.partitions
            .values()
            .filter(|p| p.tier == PartitionTier::Hot)
            .count()
    }

    /// Flush all partitions to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if any flush fails.
    pub fn flush_all(&mut self) -> Result<(), StateError> {
        for partition in self.partitions.values_mut() {
            partition.flush()?;
        }
        Ok(())
    }

    /// Enforce the hot partition limit by demoting oldest hot partitions.
    fn enforce_hot_limit(&mut self) {
        let hot_count = self.hot_partition_count();
        if hot_count <= self.config.max_hot_partitions {
            return;
        }

        let to_demote = hot_count - self.config.max_hot_partitions;
        let mut demoted = 0;

        // Demote oldest partitions first (BTreeMap is sorted by key).
        for partition in self.partitions.values_mut() {
            if demoted >= to_demote {
                break;
            }
            if partition.tier == PartitionTier::Hot {
                partition.tier = PartitionTier::Cold;
                // Flush to ensure data is on disk before demotion.
                if let Err(e) = partition.flush() {
                    tracing::warn!(
                        partition_id = partition.id,
                        error = %e,
                        "Failed to flush partition during demotion"
                    );
                }
                demoted += 1;
            }
        }
    }

    /// Remove a partition and its column files.
    ///
    /// # Errors
    ///
    /// Returns an error if directory removal fails.
    pub fn remove_partition(&mut self, partition_id: i64) -> Result<(), StateError> {
        if let Some(partition) = self.partitions.remove(&partition_id) {
            let entry_count: usize = partition.index.values().map(Vec::len).sum();
            self.total_entries = self.total_entries.saturating_sub(entry_count);
            self.total_size_bytes = self.total_size_bytes.saturating_sub(partition.size_bytes);

            let dir = self
                .config
                .base_dir
                .join(format!("partition_{partition_id}"));
            if dir.exists() {
                fs::remove_dir_all(&dir)?;
            }
        }
        Ok(())
    }

    /// Total entries across all partitions.
    #[must_use]
    pub fn total_entries(&self) -> usize {
        self.total_entries
    }

    /// Total size in bytes.
    #[must_use]
    pub fn total_size_bytes(&self) -> usize {
        self.total_size_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_config(dir: &Path) -> ColumnFileConfig {
        ColumnFileConfig::new(
            dir,
            vec!["sum".to_string(), "count".to_string(), "min".to_string()],
        )
        .with_column_file_size(4096)
        .with_max_hot_partitions(2)
    }

    #[test]
    fn test_create_store() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let store = ColumnFileStateStore::new(config).unwrap();
        assert_eq!(store.partition_count(), 0);
    }

    #[test]
    fn test_ensure_partition() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let mut store = ColumnFileStateStore::new(config).unwrap();

        store.ensure_partition(1000).unwrap();
        assert_eq!(store.partition_count(), 1);
        assert_eq!(store.partition_tier(1000), Some(PartitionTier::Hot));
    }

    #[test]
    fn test_put_and_get_column() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let mut store = ColumnFileStateStore::new(config).unwrap();

        store.ensure_partition(1000).unwrap();

        store
            .put_column(1000, b"key1", "sum", &42u64.to_le_bytes())
            .unwrap();
        store
            .put_column(1000, b"key1", "count", &5u64.to_le_bytes())
            .unwrap();

        let sum = store.get_column(1000, b"key1", "sum").unwrap();
        assert_eq!(u64::from_le_bytes(sum.try_into().unwrap()), 42);

        let count = store.get_column(1000, b"key1", "count").unwrap();
        assert_eq!(u64::from_le_bytes(count.try_into().unwrap()), 5);

        // Non-existent column.
        assert!(store.get_column(1000, b"key1", "max").is_none());
    }

    #[test]
    fn test_hot_partition_limit() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path()); // max_hot = 2
        let mut store = ColumnFileStateStore::new(config).unwrap();

        store.ensure_partition(100).unwrap();
        store.ensure_partition(200).unwrap();
        assert_eq!(store.hot_partition_count(), 2);

        // Adding a 3rd should demote the oldest.
        store.ensure_partition(300).unwrap();
        assert_eq!(store.partition_count(), 3);
        assert_eq!(store.hot_partition_count(), 2);
        assert_eq!(store.partition_tier(100), Some(PartitionTier::Cold));
        assert_eq!(store.partition_tier(200), Some(PartitionTier::Hot));
        assert_eq!(store.partition_tier(300), Some(PartitionTier::Hot));
    }

    #[test]
    fn test_remove_partition() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let mut store = ColumnFileStateStore::new(config).unwrap();

        store.ensure_partition(1000).unwrap();
        store
            .put_column(1000, b"key1", "sum", &42u64.to_le_bytes())
            .unwrap();

        assert_eq!(store.partition_count(), 1);
        store.remove_partition(1000).unwrap();
        assert_eq!(store.partition_count(), 0);
    }

    #[test]
    fn test_partition_ids_sorted() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path()).with_max_hot_partitions(5);
        let mut store = ColumnFileStateStore::new(config).unwrap();

        store.ensure_partition(300).unwrap();
        store.ensure_partition(100).unwrap();
        store.ensure_partition(200).unwrap();

        assert_eq!(store.partition_ids(), vec![100, 200, 300]);
    }

    #[test]
    fn test_flush_all() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let mut store = ColumnFileStateStore::new(config).unwrap();

        store.ensure_partition(1000).unwrap();
        store
            .put_column(1000, b"key1", "sum", &42u64.to_le_bytes())
            .unwrap();

        // Should not error.
        store.flush_all().unwrap();
    }

    #[test]
    fn test_multiple_keys_same_column() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let mut store = ColumnFileStateStore::new(config).unwrap();

        store.ensure_partition(1000).unwrap();

        store
            .put_column(1000, b"key1", "sum", &10u64.to_le_bytes())
            .unwrap();
        store
            .put_column(1000, b"key2", "sum", &20u64.to_le_bytes())
            .unwrap();

        let v1 = store.get_column(1000, b"key1", "sum").unwrap();
        assert_eq!(u64::from_le_bytes(v1.try_into().unwrap()), 10);

        let v2 = store.get_column(1000, b"key2", "sum").unwrap();
        assert_eq!(u64::from_le_bytes(v2.try_into().unwrap()), 20);

        assert_eq!(store.total_entries(), 2);
    }

    #[test]
    fn test_reopen_restores_data() {
        let dir = TempDir::new().unwrap();

        // Phase 1: write data and flush.
        {
            let config = test_config(dir.path());
            let mut store = ColumnFileStateStore::new(config).unwrap();
            store.ensure_partition(1000).unwrap();
            store.ensure_partition(2000).unwrap();

            store
                .put_column(1000, b"key_a", "sum", &100u64.to_le_bytes())
                .unwrap();
            store
                .put_column(1000, b"key_a", "count", &7u64.to_le_bytes())
                .unwrap();
            store
                .put_column(1000, b"key_b", "sum", &200u64.to_le_bytes())
                .unwrap();
            store
                .put_column(2000, b"key_x", "min", &5u64.to_le_bytes())
                .unwrap();

            store.flush_all().unwrap();
            // store is dropped here — simulates process exit.
        }

        // Phase 2: reopen from the same directory and verify all data.
        {
            let config = test_config(dir.path());
            let store = ColumnFileStateStore::new(config).unwrap();

            // Both partitions should be recovered.
            assert_eq!(store.partition_count(), 2);
            assert_eq!(store.partition_ids(), vec![1000, 2000]);

            // Verify values in partition 1000.
            let sum_a = store.get_column(1000, b"key_a", "sum").unwrap();
            assert_eq!(u64::from_le_bytes(sum_a.try_into().unwrap()), 100);

            let count_a = store.get_column(1000, b"key_a", "count").unwrap();
            assert_eq!(u64::from_le_bytes(count_a.try_into().unwrap()), 7);

            let sum_b = store.get_column(1000, b"key_b", "sum").unwrap();
            assert_eq!(u64::from_le_bytes(sum_b.try_into().unwrap()), 200);

            // Verify value in partition 2000.
            let min_x = store.get_column(2000, b"key_x", "min").unwrap();
            assert_eq!(u64::from_le_bytes(min_x.try_into().unwrap()), 5);

            // Entry count should match.
            assert_eq!(store.total_entries(), 4);
        }
    }
}
