//! Checkpoint batching for S3 cost optimization.
//!
//! Accumulates state blobs from multiple partitions/operators into a single
//! compressed object before flushing to object storage. Targets 4-32MB object
//! sizes to minimize PUT costs ($0.005/1000 PUTs on S3).
//!
//! ## Batch Format
//!
//! ```text
//! [magic: 4 bytes "LCB1"]
//! [version: u32 LE = 1]
//! [entry_count: u32 LE]
//! [uncompressed_size: u32 LE]
//! [compressed_body: LZ4 block]
//!   → decoded body is a sequence of frames:
//!     [key_len: u32 LE][key: UTF-8][data_len: u32 LE][data: bytes]
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use object_store::{ObjectStore, PutOptions, PutPayload};

use crate::checkpoint_store::CheckpointStoreError;

/// Magic bytes identifying a checkpoint batch file.
const BATCH_MAGIC: &[u8; 4] = b"LCB1";

/// Current batch format version.
const BATCH_VERSION: u32 = 1;

/// Default flush threshold: 8 MB.
const DEFAULT_FLUSH_THRESHOLD: usize = 8 * 1024 * 1024;

/// Batch header size in bytes (magic + version + count + size).
const HEADER_SIZE: usize = 16;

/// A single entry in the batch buffer.
struct BatchEntry {
    key: String,
    data: Vec<u8>,
}

/// Metrics for checkpoint batching operations.
#[derive(Debug)]
pub struct BatchMetrics {
    /// Total batches flushed to object storage.
    pub batches_flushed: AtomicU64,
    /// Total entries written across all batches.
    pub entries_flushed: AtomicU64,
    /// Total bytes before LZ4 compression.
    pub bytes_before_compression: AtomicU64,
    /// Total bytes after LZ4 compression.
    pub bytes_after_compression: AtomicU64,
    /// Total PUT requests issued.
    pub put_count: AtomicU64,
}

impl BatchMetrics {
    /// Create zeroed metrics.
    #[must_use]
    pub fn new() -> Self {
        Self {
            batches_flushed: AtomicU64::new(0),
            entries_flushed: AtomicU64::new(0),
            bytes_before_compression: AtomicU64::new(0),
            bytes_after_compression: AtomicU64::new(0),
            put_count: AtomicU64::new(0),
        }
    }

    /// Record a completed flush.
    fn record_flush(&self, entries: u64, raw_bytes: u64, compressed_bytes: u64) {
        self.batches_flushed.fetch_add(1, Ordering::Relaxed);
        self.entries_flushed.fetch_add(entries, Ordering::Relaxed);
        self.bytes_before_compression
            .fetch_add(raw_bytes, Ordering::Relaxed);
        self.bytes_after_compression
            .fetch_add(compressed_bytes, Ordering::Relaxed);
        self.put_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Point-in-time snapshot of metrics.
    #[must_use]
    pub fn snapshot(&self) -> BatchMetricsSnapshot {
        BatchMetricsSnapshot {
            batches_flushed: self.batches_flushed.load(Ordering::Relaxed),
            entries_flushed: self.entries_flushed.load(Ordering::Relaxed),
            bytes_before_compression: self.bytes_before_compression.load(Ordering::Relaxed),
            bytes_after_compression: self.bytes_after_compression.load(Ordering::Relaxed),
            put_count: self.put_count.load(Ordering::Relaxed),
        }
    }
}

impl Default for BatchMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Immutable snapshot of [`BatchMetrics`].
#[derive(Debug, Clone, Copy)]
pub struct BatchMetricsSnapshot {
    /// Total batches flushed to object storage.
    pub batches_flushed: u64,
    /// Total entries written across all batches.
    pub entries_flushed: u64,
    /// Total bytes before LZ4 compression.
    pub bytes_before_compression: u64,
    /// Total bytes after LZ4 compression.
    pub bytes_after_compression: u64,
    /// Total PUT requests issued.
    pub put_count: u64,
}

/// Accumulates state blobs and flushes them as a single compressed object.
///
/// Call [`add`](Self::add) for each partition/operator state blob during a
/// checkpoint cycle, then [`flush`](Self::flush) at the end.
/// [`should_flush`](Self::should_flush) returns `true` when the buffer exceeds
/// the configured threshold.
pub struct CheckpointBatcher {
    buffer: Vec<BatchEntry>,
    buffer_size: usize,
    flush_threshold: usize,
    store: Arc<dyn ObjectStore>,
    prefix: String,
    rt: tokio::runtime::Runtime,
    metrics: Arc<BatchMetrics>,
}

impl CheckpointBatcher {
    /// Create a new batcher.
    ///
    /// `prefix` is prepended to all object paths (e.g., `"nodes/abc123/"`).
    /// `flush_threshold` is the uncompressed buffer size that triggers a flush
    /// (default: 8 MB).
    ///
    /// # Panics
    ///
    /// Panics if the internal Tokio runtime cannot be created.
    #[must_use]
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        flush_threshold: Option<usize>,
    ) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create batcher runtime");
        Self {
            buffer: Vec::new(),
            buffer_size: 0,
            flush_threshold: flush_threshold.unwrap_or(DEFAULT_FLUSH_THRESHOLD),
            store,
            prefix,
            rt,
            metrics: Arc::new(BatchMetrics::new()),
        }
    }

    /// Add a state blob to the buffer.
    ///
    /// The `key` identifies the partition/operator (e.g., `"partition-0/agg"`).
    /// Call [`should_flush`](Self::should_flush) after adding to check whether
    /// the buffer exceeds the threshold.
    pub fn add(&mut self, key: String, data: Vec<u8>) {
        self.buffer_size += key.len() + data.len() + 8; // +8 for two u32 length prefixes
        self.buffer.push(BatchEntry { key, data });
    }

    /// Returns `true` if the buffer exceeds the flush threshold.
    #[must_use]
    pub fn should_flush(&self) -> bool {
        self.buffer_size >= self.flush_threshold
    }

    /// Returns the number of entries currently buffered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns `true` if the buffer is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Returns the current uncompressed buffer size in bytes.
    #[must_use]
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Shared metrics handle.
    #[must_use]
    pub fn metrics(&self) -> &Arc<BatchMetrics> {
        &self.metrics
    }

    /// Flush buffered entries as a single LZ4-compressed object.
    ///
    /// The object is written to `{prefix}checkpoints/batch-{epoch:06}.lz4`.
    /// After a successful flush the buffer is cleared.
    ///
    /// Does nothing if the buffer is empty.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on serialization or object store failure.
    pub fn flush(&mut self, epoch: u64) -> Result<(), CheckpointStoreError> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let (raw_size, payload) = encode_batch(&self.buffer);

        let path = object_store::path::Path::from(format!(
            "{}checkpoints/batch-{epoch:06}.lz4",
            self.prefix
        ));

        let compressed_size = payload.content_length();

        self.rt.block_on(async {
            self.store
                .put_opts(&path, payload, PutOptions::default())
                .await
        })?;

        let entry_count = self.buffer.len() as u64;
        self.metrics
            .record_flush(entry_count, raw_size as u64, compressed_size as u64);

        self.buffer.clear();
        self.buffer_size = 0;

        Ok(())
    }
}

/// Encode buffered entries into an LZ4-compressed batch payload.
///
/// Returns `(uncompressed_body_size, payload)`.
#[allow(clippy::cast_possible_truncation)] // Entry counts/sizes are bounded well below u32::MAX
fn encode_batch(entries: &[BatchEntry]) -> (usize, PutPayload) {
    // Serialize entries into an uncompressed body.
    let mut body = Vec::new();
    for entry in entries {
        body.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
        body.extend_from_slice(entry.key.as_bytes());
        body.extend_from_slice(&(entry.data.len() as u32).to_le_bytes());
        body.extend_from_slice(&entry.data);
    }

    let uncompressed_size = body.len();
    let compressed = lz4_flex::compress_prepend_size(&body);

    // Build header + compressed body.
    let mut out = Vec::with_capacity(HEADER_SIZE + compressed.len());
    out.extend_from_slice(BATCH_MAGIC);
    out.extend_from_slice(&BATCH_VERSION.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    out.extend_from_slice(&(uncompressed_size as u32).to_le_bytes());
    out.extend_from_slice(&compressed);

    (
        uncompressed_size,
        PutPayload::from_bytes(bytes::Bytes::from(out)),
    )
}

/// Decode a batch payload into `(key, data)` pairs.
///
/// # Errors
///
/// Returns [`CheckpointStoreError::Io`] if the batch is malformed.
#[allow(clippy::cast_possible_truncation)] // u32→usize is always safe (widens on 64-bit)
pub fn decode_batch(raw: &[u8]) -> Result<Vec<(String, Vec<u8>)>, CheckpointStoreError> {
    if raw.len() < HEADER_SIZE {
        return Err(CheckpointStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "batch too short for header",
        )));
    }

    if &raw[..4] != BATCH_MAGIC {
        return Err(CheckpointStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid batch magic",
        )));
    }

    // Safe: length checked above (HEADER_SIZE = 16 ≥ all slice ends).
    let version = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]);
    if version != BATCH_VERSION {
        return Err(CheckpointStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unsupported batch version {version}"),
        )));
    }

    let entry_count = u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]) as usize;
    let _uncompressed_size = u32::from_le_bytes([raw[12], raw[13], raw[14], raw[15]]);

    let body = lz4_flex::decompress_size_prepended(&raw[HEADER_SIZE..]).map_err(|e| {
        CheckpointStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("LZ4 decompression failed: {e}"),
        ))
    })?;

    let mut entries = Vec::with_capacity(entry_count);
    let mut cursor = 0;

    for _ in 0..entry_count {
        if cursor + 4 > body.len() {
            return Err(CheckpointStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated batch entry (key length)",
            )));
        }
        let key_len = u32::from_le_bytes([
            body[cursor],
            body[cursor + 1],
            body[cursor + 2],
            body[cursor + 3],
        ]) as usize;
        cursor += 4;

        if cursor + key_len > body.len() {
            return Err(CheckpointStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated batch entry (key data)",
            )));
        }
        let key = String::from_utf8_lossy(&body[cursor..cursor + key_len]).into_owned();
        cursor += key_len;

        if cursor + 4 > body.len() {
            return Err(CheckpointStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated batch entry (data length)",
            )));
        }
        let data_len = u32::from_le_bytes([
            body[cursor],
            body[cursor + 1],
            body[cursor + 2],
            body[cursor + 3],
        ]) as usize;
        cursor += 4;

        if cursor + data_len > body.len() {
            return Err(CheckpointStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated batch entry (data)",
            )));
        }
        let data = body[cursor..cursor + data_len].to_vec();
        cursor += data_len;

        entries.push((key, data));
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn make_batcher(threshold: usize) -> (CheckpointBatcher, Arc<dyn ObjectStore>) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let batcher = CheckpointBatcher::new(store.clone(), String::new(), Some(threshold));
        (batcher, store)
    }

    #[test]
    fn test_add_tracks_size() {
        let (mut batcher, _store) = make_batcher(1024);
        assert!(batcher.is_empty());
        assert_eq!(batcher.buffer_size(), 0);

        batcher.add("key1".into(), vec![0u8; 100]);
        assert_eq!(batcher.len(), 1);
        assert!(!batcher.is_empty());
        // key(4) + data(100) + 8 bytes overhead = 112
        assert_eq!(batcher.buffer_size(), 112);
    }

    #[test]
    fn test_should_flush_at_threshold() {
        let (mut batcher, _store) = make_batcher(200);
        assert!(!batcher.should_flush());

        batcher.add("k".into(), vec![0u8; 100]);
        assert!(!batcher.should_flush());

        batcher.add("k".into(), vec![0u8; 100]);
        assert!(batcher.should_flush());
    }

    #[test]
    fn test_flush_empty_is_noop() {
        let (mut batcher, _store) = make_batcher(1024);
        batcher.flush(1).unwrap();
        let snap = batcher.metrics().snapshot();
        assert_eq!(snap.batches_flushed, 0);
        assert_eq!(snap.put_count, 0);
    }

    #[test]
    fn test_flush_writes_object() {
        let (mut batcher, store) = make_batcher(1024 * 1024);

        batcher.add("partition-0/agg".into(), vec![42u8; 256]);
        batcher.add("partition-1/agg".into(), vec![99u8; 128]);
        batcher.flush(7).unwrap();

        assert!(batcher.is_empty());
        assert_eq!(batcher.buffer_size(), 0);

        // Verify object exists at expected path
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(async {
            store
                .get_opts(
                    &object_store::path::Path::from("checkpoints/batch-000007.lz4"),
                    object_store::GetOptions::default(),
                )
                .await
        });
        assert!(result.is_ok());
    }

    #[test]
    fn test_lz4_roundtrip() {
        let (mut batcher, store) = make_batcher(1024 * 1024);

        let entries = vec![
            ("partition-0/state".to_string(), vec![1u8; 500]),
            ("partition-1/state".to_string(), vec![2u8; 300]),
            ("partition-2/agg".to_string(), vec![3u8; 200]),
        ];

        for (k, v) in &entries {
            batcher.add(k.clone(), v.clone());
        }
        batcher.flush(42).unwrap();

        // Read back and decode
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let data = rt.block_on(async {
            let result = store
                .get_opts(
                    &object_store::path::Path::from("checkpoints/batch-000042.lz4"),
                    object_store::GetOptions::default(),
                )
                .await
                .unwrap();
            result.bytes().await.unwrap()
        });

        let decoded = decode_batch(&data).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].0, "partition-0/state");
        assert_eq!(decoded[0].1, vec![1u8; 500]);
        assert_eq!(decoded[1].0, "partition-1/state");
        assert_eq!(decoded[1].1, vec![2u8; 300]);
        assert_eq!(decoded[2].0, "partition-2/agg");
        assert_eq!(decoded[2].1, vec![3u8; 200]);
    }

    #[test]
    fn test_metrics_recorded_on_flush() {
        let (mut batcher, _store) = make_batcher(1024 * 1024);

        batcher.add("k1".into(), vec![0u8; 100]);
        batcher.add("k2".into(), vec![0u8; 200]);
        batcher.flush(1).unwrap();

        let snap = batcher.metrics().snapshot();
        assert_eq!(snap.batches_flushed, 1);
        assert_eq!(snap.entries_flushed, 2);
        assert_eq!(snap.put_count, 1);
        assert!(snap.bytes_before_compression > 0);
        assert!(snap.bytes_after_compression > 0);
    }

    #[test]
    fn test_metrics_accumulate_across_flushes() {
        let (mut batcher, _store) = make_batcher(1024 * 1024);

        batcher.add("k1".into(), vec![0u8; 100]);
        batcher.flush(1).unwrap();

        batcher.add("k2".into(), vec![0u8; 200]);
        batcher.add("k3".into(), vec![0u8; 50]);
        batcher.flush(2).unwrap();

        let snap = batcher.metrics().snapshot();
        assert_eq!(snap.batches_flushed, 2);
        assert_eq!(snap.entries_flushed, 3);
        assert_eq!(snap.put_count, 2);
    }

    #[test]
    fn test_compression_reduces_size() {
        let (mut batcher, _store) = make_batcher(1024 * 1024);

        // Highly compressible data (all zeros)
        batcher.add("big".into(), vec![0u8; 10_000]);
        batcher.flush(1).unwrap();

        let snap = batcher.metrics().snapshot();
        assert!(
            snap.bytes_after_compression < snap.bytes_before_compression,
            "compressed ({}) should be smaller than raw ({})",
            snap.bytes_after_compression,
            snap.bytes_before_compression
        );
    }

    #[test]
    fn test_decode_invalid_magic() {
        let bad = b"XXXX\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let err = decode_batch(bad).unwrap_err();
        assert!(err.to_string().contains("invalid batch magic"));
    }

    #[test]
    fn test_decode_too_short() {
        let err = decode_batch(b"LCB").unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    #[test]
    fn test_decode_bad_version() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"LCB1");
        buf.extend_from_slice(&99u32.to_le_bytes()); // bad version
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        let err = decode_batch(&buf).unwrap_err();
        assert!(err.to_string().contains("unsupported batch version"));
    }

    #[test]
    fn test_flush_clears_buffer() {
        let (mut batcher, _store) = make_batcher(64);

        batcher.add("a".into(), vec![0u8; 50]);
        batcher.add("b".into(), vec![0u8; 50]);
        assert!(batcher.should_flush());

        batcher.flush(1).unwrap();
        assert!(!batcher.should_flush());
        assert!(batcher.is_empty());
        assert_eq!(batcher.buffer_size(), 0);
    }
}
