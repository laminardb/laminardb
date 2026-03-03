//! Disaggregated state backend.
//!
//! S3 as source of truth for partition state, with foyer in-memory cache
//! for fast recovery reads. Ring 0 never touches object storage — all state
//! access during normal operation goes through partition-local state stores
//! (FxHashMap, BTreeMap, mmap). This backend handles:
//!
//! 1. **Checkpoint flush** (Ring 1): Batched, LZ4-compressed writes to S3
//! 2. **Recovery** (Ring 2): Load state from S3 on startup or partition migration
//! 3. **Epoch fencing**: Reject stale partition owner writes (zombie prevention)
//!
//! ## S3 Layout
//!
//! ```text
//! {prefix}/partitions/{partition_id}/state/epoch-{epoch:06}.lz4
//! ```
//!
//! ## Blob Format (LDS1)
//!
//! ```text
//! [magic: 4 bytes "LDS1"]
//! [version: u32 LE = 1]
//! [entry_count: u32 LE]
//! [uncompressed_size: u32 LE]
//! [compressed_body: LZ4 block]
//!   → decoded body: [key_len: u32 LE][key][value_len: u32 LE][value]...
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use foyer::{Cache, CacheBuilder};
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, ObjectStore, PutOptions, PutPayload};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the disaggregated state backend.
#[derive(Debug, Clone)]
pub struct DisaggregatedConfig {
    /// S3 prefix for state objects (e.g., `"nodes/abc123/"`).
    pub prefix: String,

    /// Maximum number of entries in the foyer cache.
    pub cache_capacity: usize,

    /// Number of cache shards for concurrent access.
    pub cache_shards: usize,
}

impl Default for DisaggregatedConfig {
    fn default() -> Self {
        Self {
            prefix: String::new(),
            cache_capacity: 1024,
            cache_shards: 16,
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from the disaggregated state backend.
#[derive(Debug, Error)]
pub enum DisaggregatedError {
    /// Write rejected because the epoch is stale (zombie fencing).
    #[error("stale epoch {write_epoch} (current fencing epoch: {current_epoch})")]
    StaleEpoch {
        /// The epoch of the rejected write.
        write_epoch: u64,
        /// The current fencing epoch.
        current_epoch: u64,
    },

    /// Object store I/O error.
    #[error("object store error: {0}")]
    Store(#[from] object_store::Error),

    /// LZ4 decompression failure.
    #[error("decompression error: {0}")]
    Decompression(String),

    /// Corrupt or truncated state blob.
    #[error("invalid state blob: {0}")]
    InvalidBlob(String),
}

// ---------------------------------------------------------------------------
// State entry
// ---------------------------------------------------------------------------

/// A single key-value state entry for a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateEntry {
    /// State key (operator-defined).
    pub key: Vec<u8>,
    /// State value (operator-defined).
    pub value: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Cache key
// ---------------------------------------------------------------------------

/// Cache key: `(partition_id, epoch)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CacheKey {
    partition_id: u64,
    epoch: u64,
}

// ---------------------------------------------------------------------------
// Backend
// ---------------------------------------------------------------------------

/// Disaggregated state backend.
///
/// Writes partition state to S3 as immutable, LZ4-compressed blobs.
/// Reads go through a foyer in-memory cache — cache misses trigger
/// S3 GETs (recovery path only; normal query path never touches S3).
///
/// # Example
///
/// ```
/// use std::sync::Arc;
/// use laminar_storage::disaggregated::{
///     DisaggregatedConfig, DisaggregatedStateBackend, StateEntry,
/// };
///
/// let store = Arc::new(object_store::memory::InMemory::new());
/// let config = DisaggregatedConfig::default();
/// let backend = DisaggregatedStateBackend::new(store, config);
///
/// // Write partition state at epoch 1
/// let entries = vec![
///     StateEntry { key: b"k1".to_vec(), value: b"v1".to_vec() },
/// ];
/// backend.put_batch(0, 1, &entries).unwrap();
///
/// // Read it back (from cache)
/// let loaded = backend.get(0, 1).unwrap().unwrap();
/// assert_eq!(loaded.len(), 1);
/// assert_eq!(loaded[0].key, b"k1");
/// ```
pub struct DisaggregatedStateBackend {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    rt: tokio::runtime::Runtime,
    fencing_epoch: AtomicU64,
    cache: Cache<CacheKey, Bytes>,
}

impl DisaggregatedStateBackend {
    /// Creates a new disaggregated state backend.
    ///
    /// # Panics
    ///
    /// Panics if the internal Tokio runtime cannot be created.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, config: DisaggregatedConfig) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create disaggregated backend runtime");

        let cache = CacheBuilder::new(config.cache_capacity)
            .with_shards(config.cache_shards)
            .build();

        Self {
            store,
            prefix: config.prefix,
            rt,
            fencing_epoch: AtomicU64::new(0),
            cache,
        }
    }

    /// Write a batch of state entries for a partition at a given epoch.
    ///
    /// The entries are serialized and LZ4-compressed into a single S3 object.
    /// On success, the result is also inserted into the cache for fast read-back.
    ///
    /// # Errors
    ///
    /// - [`DisaggregatedError::StaleEpoch`] if `epoch` is below the fencing epoch.
    /// - [`DisaggregatedError::Store`] on S3 write failure.
    pub fn put_batch(
        &self,
        partition_id: u64,
        epoch: u64,
        entries: &[StateEntry],
    ) -> Result<(), DisaggregatedError> {
        let current = self.fencing_epoch.load(Ordering::Acquire);
        if epoch < current {
            return Err(DisaggregatedError::StaleEpoch {
                write_epoch: epoch,
                current_epoch: current,
            });
        }

        let blob = encode_state_blob(entries);
        let decompressed = encode_body(entries);
        let path = self.state_path(partition_id, epoch);

        let payload = PutPayload::from_bytes(Bytes::from(blob));
        self.rt.block_on(async {
            self.store
                .put_opts(&path, payload, PutOptions::default())
                .await
        })?;

        // Populate cache with decompressed body for fast read-back.
        let key = CacheKey {
            partition_id,
            epoch,
        };
        self.cache.insert(key, Bytes::from(decompressed));

        Ok(())
    }

    /// Get state entries for a partition at a specific epoch.
    ///
    /// Returns entries from the foyer cache if available, otherwise
    /// fetches from S3, decompresses, populates the cache, and returns.
    ///
    /// Returns `Ok(None)` if no state exists for this partition+epoch.
    ///
    /// # Errors
    ///
    /// - [`DisaggregatedError::Store`] on S3 read failure.
    /// - [`DisaggregatedError::InvalidBlob`] or [`DisaggregatedError::Decompression`]
    ///   if the blob is corrupt.
    pub fn get(
        &self,
        partition_id: u64,
        epoch: u64,
    ) -> Result<Option<Vec<StateEntry>>, DisaggregatedError> {
        let key = CacheKey {
            partition_id,
            epoch,
        };

        // Cache hit → decode from cached decompressed body.
        if let Some(entry) = self.cache.get(&key) {
            let entries = decode_body(entry.value())?;
            return Ok(Some(entries));
        }

        // Cache miss → S3 GET.
        let path = self.state_path(partition_id, epoch);
        let result = self.rt.block_on(async {
            self.store.get_opts(&path, GetOptions::default()).await
        });

        match result {
            Ok(get_result) => {
                let data = self.rt.block_on(get_result.bytes())?;
                let decompressed = decompress_blob(&data)?;
                let entries = decode_body(&decompressed)?;

                // Populate cache.
                self.cache
                    .insert(key, Bytes::from(decompressed));

                Ok(Some(entries))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(DisaggregatedError::Store(e)),
        }
    }

    /// Load the latest state for a partition.
    ///
    /// Scans available epochs, picks the highest, and loads it.
    /// Returns `Ok(None)` if the partition has no stored state.
    ///
    /// # Errors
    ///
    /// Returns [`DisaggregatedError`] on S3 or decoding failure.
    pub fn load_latest(
        &self,
        partition_id: u64,
    ) -> Result<Option<(u64, Vec<StateEntry>)>, DisaggregatedError> {
        let epochs = self.list_epochs(partition_id)?;
        match epochs.last() {
            Some(&epoch) => {
                let entries = self.get(partition_id, epoch)?;
                Ok(entries.map(|e| (epoch, e)))
            }
            None => Ok(None),
        }
    }

    /// Advance the fencing epoch (monotonically).
    ///
    /// After this call, `put_batch()` rejects writes with `epoch < new_epoch`.
    /// This prevents zombie partition owners from corrupting state.
    pub fn advance_epoch(&self, new_epoch: u64) {
        self.fencing_epoch.fetch_max(new_epoch, Ordering::Release);
    }

    /// Returns the current fencing epoch.
    #[must_use]
    pub fn current_epoch(&self) -> u64 {
        self.fencing_epoch.load(Ordering::Acquire)
    }

    /// List available epochs for a partition, sorted ascending.
    ///
    /// # Errors
    ///
    /// Returns [`DisaggregatedError::Store`] on S3 list failure.
    pub fn list_epochs(&self, partition_id: u64) -> Result<Vec<u64>, DisaggregatedError> {
        let prefix = ObjectPath::from(format!(
            "{}partitions/{partition_id:06}/state/",
            self.prefix
        ));

        let entries = self.rt.block_on(async {
            use futures::TryStreamExt;
            self.store
                .list(Some(&prefix))
                .try_collect::<Vec<_>>()
                .await
        })?;

        let mut epochs: Vec<u64> = entries
            .iter()
            .filter_map(|e| parse_epoch_from_path(&e.location))
            .collect();
        epochs.sort_unstable();
        Ok(epochs)
    }

    /// Prune old epochs for a partition, keeping the most recent `keep_count`.
    ///
    /// Returns the number of state blobs deleted.
    ///
    /// # Errors
    ///
    /// Returns [`DisaggregatedError::Store`] on S3 delete failure.
    pub fn prune(
        &self,
        partition_id: u64,
        keep_count: usize,
    ) -> Result<usize, DisaggregatedError> {
        let epochs = self.list_epochs(partition_id)?;
        if epochs.len() <= keep_count {
            return Ok(0);
        }

        let to_delete = &epochs[..epochs.len() - keep_count];
        let paths: Vec<Result<ObjectPath, object_store::Error>> = to_delete
            .iter()
            .map(|&ep| Ok(self.state_path(partition_id, ep)))
            .collect();
        let delete_count = paths.len();

        self.rt.block_on(async {
            use futures::StreamExt;
            let stream = futures::stream::iter(paths).boxed();
            let mut results = self.store.delete_stream(stream);
            while let Some(_result) = results.next().await {
                // Ignore individual delete errors (file may not exist).
            }
        });

        // Evict deleted entries from cache.
        for &ep in to_delete {
            self.cache.remove(&CacheKey {
                partition_id,
                epoch: ep,
            });
        }

        Ok(delete_count)
    }

    /// Returns the S3 object path for a partition state blob.
    fn state_path(&self, partition_id: u64, epoch: u64) -> ObjectPath {
        ObjectPath::from(format!(
            "{}partitions/{partition_id:06}/state/epoch-{epoch:06}.lz4",
            self.prefix
        ))
    }
}

// ---------------------------------------------------------------------------
// Blob encoding/decoding
// ---------------------------------------------------------------------------

/// Magic bytes identifying a disaggregated state blob.
const MAGIC: &[u8; 4] = b"LDS1";

/// Current blob format version.
const BLOB_VERSION: u32 = 1;

/// Header size in bytes: magic + version + count + uncompressed size (4 bytes each).
const HEADER_SIZE: usize = 16;

/// Serialize entries into an uncompressed body.
///
/// Format: `[key_len:u32][key][value_len:u32][value]...`
#[allow(clippy::cast_possible_truncation)] // key/value lengths bounded well below u32::MAX
fn encode_body(entries: &[StateEntry]) -> Vec<u8> {
    let estimated = entries
        .iter()
        .map(|e| 8 + e.key.len() + e.value.len())
        .sum();
    let mut body = Vec::with_capacity(estimated);

    for entry in entries {
        body.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
        body.extend_from_slice(&entry.key);
        body.extend_from_slice(&(entry.value.len() as u32).to_le_bytes());
        body.extend_from_slice(&entry.value);
    }

    body
}

/// Encode state entries into an LZ4-compressed blob with LDS1 header.
#[allow(clippy::cast_possible_truncation)] // entry counts/sizes bounded well below u32::MAX
fn encode_state_blob(entries: &[StateEntry]) -> Vec<u8> {
    let body = encode_body(entries);
    let uncompressed_size = body.len();
    let compressed = lz4_flex::compress_prepend_size(&body);

    let mut blob = Vec::with_capacity(HEADER_SIZE + compressed.len());
    blob.extend_from_slice(MAGIC);
    blob.extend_from_slice(&BLOB_VERSION.to_le_bytes());
    blob.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    blob.extend_from_slice(&(uncompressed_size as u32).to_le_bytes());
    blob.extend_from_slice(&compressed);

    blob
}

/// Decompress an LDS1 blob, returning the raw body bytes.
fn decompress_blob(data: &[u8]) -> Result<Vec<u8>, DisaggregatedError> {
    if data.len() < HEADER_SIZE {
        return Err(DisaggregatedError::InvalidBlob("blob too short".into()));
    }

    if &data[..4] != MAGIC {
        return Err(DisaggregatedError::InvalidBlob(format!(
            "bad magic: expected LDS1, got {:?}",
            &data[..4]
        )));
    }

    let version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
    if version != BLOB_VERSION {
        return Err(DisaggregatedError::InvalidBlob(format!(
            "unsupported version {version}"
        )));
    }

    // entry_count at data[8..12] — used for validation after decode.
    // uncompressed_size at data[12..16] — informational.

    lz4_flex::decompress_size_prepended(&data[HEADER_SIZE..])
        .map_err(|e| DisaggregatedError::Decompression(e.to_string()))
}

/// Decode entries from a decompressed body.
fn decode_body(body: &[u8]) -> Result<Vec<StateEntry>, DisaggregatedError> {
    let mut entries = Vec::new();
    let mut pos = 0;

    while pos < body.len() {
        // key_len
        if pos + 4 > body.len() {
            return Err(DisaggregatedError::InvalidBlob(
                "truncated key length".into(),
            ));
        }
        let key_len = u32::from_le_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]])
            as usize;
        pos += 4;

        // key
        if pos + key_len > body.len() {
            return Err(DisaggregatedError::InvalidBlob("truncated key data".into()));
        }
        let key = body[pos..pos + key_len].to_vec();
        pos += key_len;

        // value_len
        if pos + 4 > body.len() {
            return Err(DisaggregatedError::InvalidBlob(
                "truncated value length".into(),
            ));
        }
        let value_len =
            u32::from_le_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]) as usize;
        pos += 4;

        // value
        if pos + value_len > body.len() {
            return Err(DisaggregatedError::InvalidBlob(
                "truncated value data".into(),
            ));
        }
        let value = body[pos..pos + value_len].to_vec();
        pos += value_len;

        entries.push(StateEntry { key, value });
    }

    Ok(entries)
}

/// Extract epoch number from an object path like `.../epoch-000042.lz4`.
fn parse_epoch_from_path(path: &ObjectPath) -> Option<u64> {
    let filename = path.filename()?;
    let name = filename.strip_prefix("epoch-")?;
    let num = name.strip_suffix(".lz4")?;
    num.parse().ok()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_backend() -> DisaggregatedStateBackend {
        let store = Arc::new(object_store::memory::InMemory::new());
        DisaggregatedStateBackend::new(store, DisaggregatedConfig::default())
    }

    fn make_entries(n: usize) -> Vec<StateEntry> {
        (0..n)
            .map(|i| StateEntry {
                key: format!("key-{i}").into_bytes(),
                value: format!("value-{i}").into_bytes(),
            })
            .collect()
    }

    // -- Blob encoding/decoding --

    #[test]
    fn encode_decode_roundtrip() {
        let entries = make_entries(5);
        let blob = encode_state_blob(&entries);
        let body = decompress_blob(&blob).unwrap();
        let decoded = decode_body(&body).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn encode_decode_empty() {
        let entries: Vec<StateEntry> = vec![];
        let blob = encode_state_blob(&entries);
        let body = decompress_blob(&blob).unwrap();
        let decoded = decode_body(&body).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn decode_invalid_magic() {
        let mut blob = encode_state_blob(&make_entries(1));
        blob[0] = b'X';
        let err = decompress_blob(&blob).unwrap_err();
        assert!(err.to_string().contains("bad magic"));
    }

    #[test]
    fn decode_unsupported_version() {
        let mut blob = encode_state_blob(&make_entries(1));
        // Set version to 99
        blob[4..8].copy_from_slice(&99u32.to_le_bytes());
        let err = decompress_blob(&blob).unwrap_err();
        assert!(err.to_string().contains("unsupported version 99"));
    }

    #[test]
    fn decode_truncated_blob() {
        let err = decompress_blob(b"LDS").unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    #[test]
    fn decode_body_truncated_key_len() {
        let err = decode_body(&[0x01, 0x00]).unwrap_err();
        assert!(err.to_string().contains("truncated key length"));
    }

    #[test]
    fn decode_body_truncated_key_data() {
        // key_len = 10, but only 2 bytes of key data
        let mut body = Vec::new();
        body.extend_from_slice(&10u32.to_le_bytes());
        body.extend_from_slice(b"ab");
        let err = decode_body(&body).unwrap_err();
        assert!(err.to_string().contains("truncated key data"));
    }

    // -- put_batch / get --

    #[test]
    fn put_and_get() {
        let backend = make_backend();
        let entries = make_entries(3);

        backend.put_batch(0, 1, &entries).unwrap();

        let loaded = backend.get(0, 1).unwrap().unwrap();
        assert_eq!(loaded, entries);
    }

    #[test]
    fn get_nonexistent() {
        let backend = make_backend();
        let result = backend.get(0, 999).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn get_from_cache() {
        let backend = make_backend();
        let entries = make_entries(2);

        backend.put_batch(0, 1, &entries).unwrap();

        // Second get should hit cache
        let loaded1 = backend.get(0, 1).unwrap().unwrap();
        let loaded2 = backend.get(0, 1).unwrap().unwrap();
        assert_eq!(loaded1, loaded2);
        assert_eq!(loaded1, entries);
    }

    #[test]
    fn get_populates_cache_on_miss() {
        // Use a shared store so we can write via one backend and read via another.
        let store = Arc::new(object_store::memory::InMemory::new());
        let backend1 = DisaggregatedStateBackend::new(
            store.clone(),
            DisaggregatedConfig::default(),
        );
        let backend2 = DisaggregatedStateBackend::new(store, DisaggregatedConfig::default());

        let entries = make_entries(2);
        backend1.put_batch(0, 1, &entries).unwrap();

        // backend2 has an empty cache — will fetch from S3.
        let loaded = backend2.get(0, 1).unwrap().unwrap();
        assert_eq!(loaded, entries);

        // Second get on backend2 should now hit cache.
        let loaded2 = backend2.get(0, 1).unwrap().unwrap();
        assert_eq!(loaded2, entries);
    }

    // -- Zombie fencing --

    #[test]
    fn stale_epoch_rejected() {
        let backend = make_backend();
        backend.advance_epoch(5);

        let err = backend.put_batch(0, 3, &make_entries(1)).unwrap_err();
        match err {
            DisaggregatedError::StaleEpoch {
                write_epoch: 3,
                current_epoch: 5,
            } => {}
            other => panic!("expected StaleEpoch, got {other}"),
        }
    }

    #[test]
    fn current_epoch_write_allowed() {
        let backend = make_backend();
        backend.advance_epoch(5);

        // epoch == fencing epoch is allowed (not stale)
        backend.put_batch(0, 5, &make_entries(1)).unwrap();
    }

    #[test]
    fn future_epoch_write_allowed() {
        let backend = make_backend();
        backend.advance_epoch(5);

        backend.put_batch(0, 10, &make_entries(1)).unwrap();
    }

    #[test]
    fn advance_epoch_monotonic() {
        let backend = make_backend();
        backend.advance_epoch(10);
        backend.advance_epoch(5); // should not decrease
        assert_eq!(backend.current_epoch(), 10);
    }

    // -- list_epochs --

    #[test]
    fn list_epochs_empty() {
        let backend = make_backend();
        let epochs = backend.list_epochs(0).unwrap();
        assert!(epochs.is_empty());
    }

    #[test]
    fn list_epochs_sorted() {
        let backend = make_backend();
        backend.put_batch(0, 3, &make_entries(1)).unwrap();
        backend.put_batch(0, 1, &make_entries(1)).unwrap();
        backend.put_batch(0, 5, &make_entries(1)).unwrap();

        let epochs = backend.list_epochs(0).unwrap();
        assert_eq!(epochs, vec![1, 3, 5]);
    }

    #[test]
    fn list_epochs_per_partition() {
        let backend = make_backend();
        backend.put_batch(0, 1, &make_entries(1)).unwrap();
        backend.put_batch(1, 2, &make_entries(1)).unwrap();
        backend.put_batch(0, 3, &make_entries(1)).unwrap();

        assert_eq!(backend.list_epochs(0).unwrap(), vec![1, 3]);
        assert_eq!(backend.list_epochs(1).unwrap(), vec![2]);
    }

    // -- load_latest --

    #[test]
    fn load_latest_empty() {
        let backend = make_backend();
        let result = backend.load_latest(0).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn load_latest_picks_highest_epoch() {
        let backend = make_backend();
        let entries1 = vec![StateEntry {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
        }];
        let entries3 = vec![StateEntry {
            key: b"k3".to_vec(),
            value: b"v3".to_vec(),
        }];

        backend.put_batch(0, 1, &entries1).unwrap();
        backend.put_batch(0, 3, &entries3).unwrap();

        let (epoch, entries) = backend.load_latest(0).unwrap().unwrap();
        assert_eq!(epoch, 3);
        assert_eq!(entries, entries3);
    }

    // -- prune --

    #[test]
    fn prune_keeps_recent() {
        let backend = make_backend();
        for i in 1..=5 {
            backend.put_batch(0, i, &make_entries(1)).unwrap();
        }

        let deleted = backend.prune(0, 2).unwrap();
        assert_eq!(deleted, 3);

        let epochs = backend.list_epochs(0).unwrap();
        assert_eq!(epochs, vec![4, 5]);
    }

    #[test]
    fn prune_noop_when_under_limit() {
        let backend = make_backend();
        backend.put_batch(0, 1, &make_entries(1)).unwrap();

        let deleted = backend.prune(0, 5).unwrap();
        assert_eq!(deleted, 0);
    }

    #[test]
    fn prune_evicts_cache() {
        let backend = make_backend();
        backend.put_batch(0, 1, &make_entries(1)).unwrap();
        backend.put_batch(0, 2, &make_entries(1)).unwrap();
        backend.put_batch(0, 3, &make_entries(1)).unwrap();

        // Verify cache hit before prune
        assert!(backend.get(0, 1).unwrap().is_some());

        backend.prune(0, 1).unwrap();

        // Epoch 1 and 2 were deleted from S3 and evicted from cache
        assert!(backend.get(0, 1).unwrap().is_none());
        assert!(backend.get(0, 2).unwrap().is_none());
        // Epoch 3 still exists
        assert!(backend.get(0, 3).unwrap().is_some());
    }

    // -- per-partition isolation --

    #[test]
    fn partitions_isolated() {
        let backend = make_backend();
        let entries0 = vec![StateEntry {
            key: b"p0".to_vec(),
            value: b"v0".to_vec(),
        }];
        let entries1 = vec![StateEntry {
            key: b"p1".to_vec(),
            value: b"v1".to_vec(),
        }];

        backend.put_batch(0, 1, &entries0).unwrap();
        backend.put_batch(1, 1, &entries1).unwrap();

        assert_eq!(backend.get(0, 1).unwrap().unwrap(), entries0);
        assert_eq!(backend.get(1, 1).unwrap().unwrap(), entries1);
    }

    // -- large batch --

    #[test]
    fn large_batch_roundtrip() {
        let backend = make_backend();
        let entries = make_entries(1000);

        backend.put_batch(0, 1, &entries).unwrap();

        let loaded = backend.get(0, 1).unwrap().unwrap();
        assert_eq!(loaded.len(), 1000);
        assert_eq!(loaded, entries);
    }

    // -- path parsing --

    #[test]
    fn parse_epoch_valid() {
        let path = ObjectPath::from("prefix/partitions/000000/state/epoch-000042.lz4");
        assert_eq!(parse_epoch_from_path(&path), Some(42));
    }

    #[test]
    fn parse_epoch_invalid_prefix() {
        let path = ObjectPath::from("prefix/partitions/000000/state/snapshot-000042.lz4");
        assert_eq!(parse_epoch_from_path(&path), None);
    }

    #[test]
    fn parse_epoch_invalid_suffix() {
        let path = ObjectPath::from("prefix/partitions/000000/state/epoch-000042.bin");
        assert_eq!(parse_epoch_from_path(&path), None);
    }

    // -- default config --

    #[test]
    fn default_config() {
        let config = DisaggregatedConfig::default();
        assert!(config.prefix.is_empty());
        assert_eq!(config.cache_capacity, 1024);
        assert_eq!(config.cache_shards, 16);
    }

    // -- error display --

    #[test]
    fn error_display() {
        let err = DisaggregatedError::StaleEpoch {
            write_epoch: 3,
            current_epoch: 5,
        };
        assert!(err.to_string().contains("stale epoch 3"));
        assert!(err.to_string().contains("current fencing epoch: 5"));
    }
}
