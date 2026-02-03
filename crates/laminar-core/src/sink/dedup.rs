//! Deduplication store for idempotent sinks

#![allow(clippy::cast_possible_truncation)]

use std::collections::VecDeque;
use std::hash::{Hash, Hasher};

use fxhash::FxHashSet;

/// Unique identifier for a record
///
/// Used to track which records have been written to prevent duplicates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecordId {
    /// The ID bytes (hash or actual ID)
    bytes: Vec<u8>,
}

impl RecordId {
    /// Create a record ID from bytes
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            bytes: bytes.to_vec(),
        }
    }

    /// Create a record ID from a string
    #[must_use]
    pub fn from_string(s: &str) -> Self {
        Self {
            bytes: s.as_bytes().to_vec(),
        }
    }

    /// Create a record ID from a numeric value
    #[must_use]
    pub fn from_u64(value: u64) -> Self {
        Self {
            bytes: value.to_le_bytes().to_vec(),
        }
    }

    /// Create a record ID by hashing data
    ///
    /// Uses `FxHash` for fast, deterministic hashing.
    #[must_use]
    pub fn from_hash(data: &[u8]) -> Self {
        use fxhash::FxHasher;
        let mut hasher = FxHasher::default();
        data.hash(&mut hasher);
        let hash = hasher.finish();
        Self::from_u64(hash)
    }

    /// Create a composite ID from multiple fields
    #[must_use]
    pub fn composite(parts: &[&[u8]]) -> Self {
        let mut bytes = Vec::new();
        for part in parts {
            bytes.extend_from_slice(&(part.len() as u32).to_le_bytes());
            bytes.extend_from_slice(part);
        }
        Self::from_hash(&bytes)
    }

    /// Get the raw bytes
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Trait for deduplication stores
pub trait DeduplicationStore: Send {
    /// Check if a record ID is new (not seen before)
    fn is_new(&self, id: &RecordId) -> bool;

    /// Mark a record ID as seen
    fn mark_seen(&mut self, id: RecordId);

    /// Mark multiple record IDs as seen
    fn mark_seen_batch(&mut self, ids: impl IntoIterator<Item = RecordId>) {
        for id in ids {
            self.mark_seen(id);
        }
    }

    /// Filter a batch to only include new (unseen) IDs
    fn filter_new<'a>(&self, ids: impl IntoIterator<Item = &'a RecordId>) -> Vec<&'a RecordId> {
        ids.into_iter().filter(|id| self.is_new(id)).collect()
    }

    /// Get the number of tracked IDs
    fn len(&self) -> usize;

    /// Check if the store is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all tracked IDs
    fn clear(&mut self);

    /// Serialize the store for checkpointing
    fn to_bytes(&self) -> Vec<u8>;

    /// Restore from serialized bytes
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    fn restore(&mut self, bytes: &[u8]) -> Result<(), String>;
}

/// In-memory deduplication store with bounded capacity
///
/// Uses FIFO eviction when capacity is exceeded - oldest entries
/// are removed first. This is appropriate because:
/// 1. Older records are less likely to be seen again
/// 2. Bounded memory usage
/// 3. Fast O(1) lookup and insertion
pub struct InMemoryDedup {
    /// Set of seen record IDs for O(1) lookup
    seen: FxHashSet<RecordId>,

    /// Queue of IDs in insertion order for FIFO eviction
    order: VecDeque<RecordId>,

    /// Maximum number of entries to keep
    capacity: usize,
}

impl InMemoryDedup {
    /// Create a new in-memory dedup store with the given capacity
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            seen: FxHashSet::default(),
            order: VecDeque::with_capacity(capacity.min(10000)),
            capacity,
        }
    }

    /// Get the current capacity
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Evict oldest entries if over capacity
    fn evict_if_needed(&mut self) {
        while self.order.len() >= self.capacity {
            if let Some(old_id) = self.order.pop_front() {
                self.seen.remove(&old_id);
            }
        }
    }
}

impl DeduplicationStore for InMemoryDedup {
    fn is_new(&self, id: &RecordId) -> bool {
        !self.seen.contains(id)
    }

    fn mark_seen(&mut self, id: RecordId) {
        if self.seen.insert(id.clone()) {
            // Only add to order if it's actually new
            self.order.push_back(id);
            self.evict_if_needed();
        }
    }

    fn len(&self) -> usize {
        self.seen.len()
    }

    fn clear(&mut self) {
        self.seen.clear();
        self.order.clear();
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Version
        bytes.push(1u8);

        // Capacity
        bytes.extend_from_slice(&(self.capacity as u64).to_le_bytes());

        // Number of entries
        bytes.extend_from_slice(&(self.order.len() as u32).to_le_bytes());

        // Each entry (in order)
        for id in &self.order {
            let id_bytes = id.as_bytes();
            bytes.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
            bytes.extend_from_slice(id_bytes);
        }

        bytes
    }

    fn restore(&mut self, bytes: &[u8]) -> Result<(), String> {
        if bytes.is_empty() {
            return Err("Empty dedup data".to_string());
        }

        let mut pos = 0;

        // Version
        let version = bytes[pos];
        pos += 1;
        if version != 1 {
            return Err(format!("Unsupported dedup version: {version}"));
        }

        // Capacity
        if pos + 8 > bytes.len() {
            return Err("Unexpected end of data (capacity)".to_string());
        }
        let capacity = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;
        self.capacity = capacity;

        // Number of entries
        if pos + 4 > bytes.len() {
            return Err("Unexpected end of data (count)".to_string());
        }
        let num_entries = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        // Clear and restore
        self.clear();

        for _ in 0..num_entries {
            if pos + 4 > bytes.len() {
                return Err("Unexpected end of data (entry length)".to_string());
            }
            let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + len > bytes.len() {
                return Err("Unexpected end of data (entry data)".to_string());
            }
            let id = RecordId::from_bytes(&bytes[pos..pos + len]);
            pos += len;

            self.seen.insert(id.clone());
            self.order.push_back(id);
        }

        Ok(())
    }
}

/// Bloom filter-based deduplication for very high throughput
///
/// Uses probabilistic deduplication - may have false positives
/// (treating new records as duplicates) but never false negatives.
/// Good for append-only sinks where occasional duplicates are acceptable.
#[allow(dead_code)] // Public API for Phase 3 connector implementations
pub struct BloomFilterDedup {
    /// Bloom filter bits
    bits: Vec<u64>,

    /// Number of hash functions
    num_hashes: usize,

    /// Number of bits
    num_bits: usize,

    /// Approximate count of elements
    count: usize,
}

#[allow(dead_code)] // Public API for Phase 3 connector implementations
impl BloomFilterDedup {
    /// Create a new bloom filter dedup with target false positive rate
    ///
    /// # Arguments
    ///
    /// * `expected_elements` - Expected number of elements to track
    /// * `false_positive_rate` - Target false positive rate (0.0 to 1.0)
    #[must_use]
    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation
    )]
    pub fn new(expected_elements: usize, false_positive_rate: f64) -> Self {
        // Calculate optimal size
        // m = -n * ln(p) / (ln(2)^2)
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let num_bits =
            (-(expected_elements as f64) * false_positive_rate.ln() / ln2_squared).ceil() as usize;
        let num_bits = num_bits.max(64); // Minimum 64 bits

        // Calculate optimal number of hash functions
        // k = (m/n) * ln(2)
        let num_hashes =
            ((num_bits as f64 / expected_elements as f64) * std::f64::consts::LN_2).ceil() as usize;
        let num_hashes = num_hashes.clamp(1, 16);

        let num_words = num_bits.div_ceil(64);

        Self {
            bits: vec![0u64; num_words],
            num_hashes,
            num_bits,
            count: 0,
        }
    }

    /// Get indices for a record ID
    fn get_indices(&self, id: &RecordId) -> Vec<usize> {
        use fxhash::FxHasher;

        let mut indices = Vec::with_capacity(self.num_hashes);

        // Use double hashing: h(i) = h1 + i * h2
        let mut hasher1 = FxHasher::default();
        id.bytes.hash(&mut hasher1);
        let h1 = hasher1.finish() as usize;

        let mut hasher2 = FxHasher::default();
        hasher1.finish().hash(&mut hasher2);
        let h2 = hasher2.finish() as usize;

        for i in 0..self.num_hashes {
            let index = (h1.wrapping_add(i.wrapping_mul(h2))) % self.num_bits;
            indices.push(index);
        }

        indices
    }

    /// Check if the filter might contain the ID
    fn might_contain(&self, id: &RecordId) -> bool {
        let indices = self.get_indices(id);
        for idx in indices {
            let word = idx / 64;
            let bit = idx % 64;
            if self.bits[word] & (1u64 << bit) == 0 {
                return false;
            }
        }
        true
    }

    /// Add an ID to the filter
    fn add(&mut self, id: &RecordId) {
        let indices = self.get_indices(id);
        for idx in indices {
            let word = idx / 64;
            let bit = idx % 64;
            self.bits[word] |= 1u64 << bit;
        }
        self.count += 1;
    }

    /// Get the approximate false positive rate
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn false_positive_rate(&self) -> f64 {
        // FPR = (1 - e^(-kn/m))^k
        let k = self.num_hashes as f64;
        let n = self.count as f64;
        let m = self.num_bits as f64;
        (1.0 - (-k * n / m).exp()).powf(k)
    }
}

impl DeduplicationStore for BloomFilterDedup {
    fn is_new(&self, id: &RecordId) -> bool {
        !self.might_contain(id)
    }

    fn mark_seen(&mut self, id: RecordId) {
        self.add(&id);
    }

    fn len(&self) -> usize {
        self.count
    }

    fn clear(&mut self) {
        self.bits.fill(0);
        self.count = 0;
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Version
        bytes.push(2u8); // Version 2 for bloom filter

        // Parameters
        bytes.extend_from_slice(&(self.num_bits as u64).to_le_bytes());
        bytes.extend_from_slice(&(self.num_hashes as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.count as u64).to_le_bytes());

        // Bits
        bytes.extend_from_slice(&(self.bits.len() as u32).to_le_bytes());
        for word in &self.bits {
            bytes.extend_from_slice(&word.to_le_bytes());
        }

        bytes
    }

    fn restore(&mut self, bytes: &[u8]) -> Result<(), String> {
        if bytes.is_empty() {
            return Err("Empty bloom filter data".to_string());
        }

        let mut pos = 0;

        // Version
        let version = bytes[pos];
        pos += 1;
        if version != 2 {
            return Err(format!("Unsupported bloom filter version: {version}"));
        }

        // Parameters
        if pos + 20 > bytes.len() {
            return Err("Unexpected end of data (parameters)".to_string());
        }
        self.num_bits = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;
        self.num_hashes = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        self.count = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;

        // Bits
        if pos + 4 > bytes.len() {
            return Err("Unexpected end of data (bits length)".to_string());
        }
        let num_words = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        if pos + num_words * 8 > bytes.len() {
            return Err("Unexpected end of data (bits)".to_string());
        }

        self.bits = Vec::with_capacity(num_words);
        for _ in 0..num_words {
            let word = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap());
            self.bits.push(word);
            pos += 8;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_id_from_bytes() {
        let id = RecordId::from_bytes(b"test-id");
        assert_eq!(id.as_bytes(), b"test-id");
    }

    #[test]
    fn test_record_id_from_u64() {
        let id = RecordId::from_u64(12345);
        assert_eq!(id.as_bytes().len(), 8);
    }

    #[test]
    fn test_record_id_from_hash() {
        let id1 = RecordId::from_hash(b"some data");
        let id2 = RecordId::from_hash(b"some data");
        let id3 = RecordId::from_hash(b"other data");

        assert_eq!(id1, id2); // Same data = same hash
        assert_ne!(id1, id3); // Different data = different hash
    }

    #[test]
    fn test_record_id_composite() {
        let id1 = RecordId::composite(&[b"table", b"key1"]);
        let id2 = RecordId::composite(&[b"table", b"key1"]);
        let id3 = RecordId::composite(&[b"table", b"key2"]);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_in_memory_dedup_basic() {
        let mut dedup = InMemoryDedup::new(1000);

        let id = RecordId::from_string("test");
        assert!(dedup.is_new(&id));

        dedup.mark_seen(id.clone());
        assert!(!dedup.is_new(&id));

        assert_eq!(dedup.len(), 1);
    }

    #[test]
    fn test_in_memory_dedup_capacity() {
        let mut dedup = InMemoryDedup::new(3);

        for i in 0..5 {
            let id = RecordId::from_u64(i);
            dedup.mark_seen(id);
        }

        // Should have evicted oldest entries
        assert!(dedup.len() <= 3);

        // Most recent should still be present
        assert!(!dedup.is_new(&RecordId::from_u64(4)));
    }

    #[test]
    fn test_in_memory_dedup_serialization() {
        let mut dedup = InMemoryDedup::new(100);
        dedup.mark_seen(RecordId::from_string("id1"));
        dedup.mark_seen(RecordId::from_string("id2"));

        let bytes = dedup.to_bytes();

        let mut restored = InMemoryDedup::new(1); // Different initial capacity
        restored.restore(&bytes).unwrap();

        assert_eq!(restored.capacity(), 100);
        assert_eq!(restored.len(), 2);
        assert!(!restored.is_new(&RecordId::from_string("id1")));
        assert!(!restored.is_new(&RecordId::from_string("id2")));
    }

    #[test]
    fn test_in_memory_dedup_filter_new() {
        let mut dedup = InMemoryDedup::new(100);
        dedup.mark_seen(RecordId::from_u64(1));
        dedup.mark_seen(RecordId::from_u64(3));

        let ids = [
            RecordId::from_u64(1),
            RecordId::from_u64(2),
            RecordId::from_u64(3),
            RecordId::from_u64(4),
        ];

        let new_ids: Vec<_> = dedup.filter_new(ids.iter()).into_iter().cloned().collect();
        assert_eq!(new_ids.len(), 2);
        assert!(new_ids.contains(&RecordId::from_u64(2)));
        assert!(new_ids.contains(&RecordId::from_u64(4)));
    }

    #[test]
    fn test_bloom_filter_basic() {
        let mut bloom = BloomFilterDedup::new(1000, 0.01);

        let id = RecordId::from_string("test");
        assert!(bloom.is_new(&id));

        bloom.mark_seen(id.clone());
        assert!(!bloom.is_new(&id));
    }

    #[test]
    fn test_bloom_filter_no_false_negatives() {
        let mut bloom = BloomFilterDedup::new(100, 0.01);

        // Add many IDs
        for i in 0..100 {
            let id = RecordId::from_u64(i);
            bloom.mark_seen(id);
        }

        // All should be found (no false negatives)
        for i in 0..100 {
            let id = RecordId::from_u64(i);
            assert!(!bloom.is_new(&id), "False negative for id {i}");
        }
    }

    #[test]
    fn test_bloom_filter_serialization() {
        let mut bloom = BloomFilterDedup::new(100, 0.01);
        bloom.mark_seen(RecordId::from_string("id1"));
        bloom.mark_seen(RecordId::from_string("id2"));

        let bytes = bloom.to_bytes();

        let mut restored = BloomFilterDedup::new(10, 0.1); // Different params
        restored.restore(&bytes).unwrap();

        assert!(!restored.is_new(&RecordId::from_string("id1")));
        assert!(!restored.is_new(&RecordId::from_string("id2")));
    }
}
