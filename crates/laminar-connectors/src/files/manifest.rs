//! File ingestion manifest with bounded retention.
//!
//! Tracks which files have been ingested across checkpoints. Uses a two-tier
//! approach: an active `HashMap` for exact deduplication of recent files, and
//! a Bloom filter ([`xorf::Xor8`]) for probabilistic dedup of evicted entries.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::checkpoint::SourceCheckpoint;

/// Metadata for a single ingested file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    /// File size in bytes at ingestion time.
    pub size: u64,
    /// Unix timestamp (ms) when the file was discovered.
    pub discovered_at: u64,
    /// Unix timestamp (ms) when the file was ingested.
    pub ingested_at: u64,
}

/// Tracks ingested files with bounded retention.
#[derive(Debug)]
pub struct FileIngestionManifest {
    /// Active entries — exact deduplication.
    active: HashMap<String, FileEntry>,
    /// Hashes of evicted file paths for Bloom filter rebuild.
    bloom_hashes: Vec<u64>,
    /// Compiled Bloom filter for probabilistic dedup of old files.
    bloom: Option<xorf::Xor8>,
}

impl FileIngestionManifest {
    /// Creates an empty manifest.
    #[must_use]
    pub fn new() -> Self {
        Self {
            active: HashMap::new(),
            bloom_hashes: Vec::new(),
            bloom: None,
        }
    }

    /// Returns `true` if the path has been ingested (exact or probabilistic match).
    pub fn contains(&self, path: &str) -> bool {
        if self.active.contains_key(path) {
            return true;
        }
        if let Some(bloom) = &self.bloom {
            return xorf::Filter::contains(bloom, &hash_path(path));
        }
        // Fallback: linear scan for small bloom_hashes (< 2 entries can't build Xor8).
        if !self.bloom_hashes.is_empty() {
            let h = hash_path(path);
            return self.bloom_hashes.contains(&h);
        }
        false
    }

    /// Checks whether a file was previously ingested with a different size.
    ///
    /// Returns `true` if the path is known AND the recorded size differs.
    pub fn size_changed(&self, path: &str, current_size: u64) -> bool {
        self.active
            .get(path)
            .is_some_and(|entry| entry.size != current_size)
    }

    /// Inserts a file entry into the active set.
    pub fn insert(&mut self, path: String, entry: FileEntry) {
        self.active.insert(path, entry);
    }

    /// Returns the number of active entries.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    /// Returns an iterator over active entries for introspection.
    pub fn active_entries(&self) -> impl Iterator<Item = (&str, &FileEntry)> {
        self.active.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Creates a read-only snapshot for the discovery engine's dedup checks.
    ///
    /// The snapshot contains the same active entries and bloom filter, but
    /// will not receive future inserts. This is intentional — the source's
    /// `poll_batch` does a belt-and-suspenders dedup check anyway.
    #[must_use]
    pub fn snapshot_for_dedup(&self) -> Self {
        Self {
            active: self.active.clone(),
            bloom_hashes: self.bloom_hashes.clone(),
            bloom: if self.bloom_hashes.len() >= 2 {
                Some(xorf::Xor8::from(self.bloom_hashes.as_slice()))
            } else {
                None
            },
        }
    }

    /// Evicts entries that exceed the count cap or age limit.
    ///
    /// Evicted entries are added to the Bloom filter for probabilistic dedup.
    pub fn maybe_evict(&mut self, max_count: usize, max_age_ms: u64) {
        use std::collections::HashSet;

        let now_ms = now_millis();

        // Collect paths to evict (by age) — use HashSet for O(1) lookups.
        let mut to_evict: HashSet<String> = self
            .active
            .iter()
            .filter(|(_, entry)| now_ms.saturating_sub(entry.ingested_at) > max_age_ms)
            .map(|(path, _)| path.clone())
            .collect();

        // If still over count cap, evict oldest by ingested_at.
        let remaining = self.active.len().saturating_sub(to_evict.len());
        if remaining > max_count {
            let mut by_age: Vec<_> = self
                .active
                .iter()
                .filter(|(path, _)| !to_evict.contains(path.as_str()))
                .map(|(path, entry)| (path.clone(), entry.ingested_at))
                .collect();
            by_age.sort_by_key(|(_, ts)| *ts);

            let excess = remaining - max_count;
            for (path, _) in by_age.into_iter().take(excess) {
                to_evict.insert(path);
            }
        }

        if to_evict.is_empty() {
            return;
        }

        // Evict and record hashes.
        for path in &to_evict {
            if self.active.remove(path).is_some() {
                self.bloom_hashes.push(hash_path(path));
            }
        }

        // Cap bloom_hashes to prevent unbounded growth. When over 1M entries,
        // drop the oldest half. This increases FPR for very old files but
        // those are unlikely to reappear.
        const MAX_BLOOM_HASHES: usize = 1_000_000;
        if self.bloom_hashes.len() > MAX_BLOOM_HASHES {
            let drop_count = self.bloom_hashes.len() / 2;
            self.bloom_hashes.drain(..drop_count);
        }

        self.rebuild_bloom();
    }

    /// Serializes the manifest into `SourceCheckpoint` fields.
    pub fn to_checkpoint(&self, checkpoint: &mut SourceCheckpoint) {
        let active_json = serde_json::to_string(&self.active).unwrap_or_else(|_| "{}".to_string());
        checkpoint.set_offset("manifest", &active_json);

        if !self.bloom_hashes.is_empty() {
            let hashes_json =
                serde_json::to_string(&self.bloom_hashes).unwrap_or_else(|_| "[]".to_string());
            checkpoint.set_offset("manifest_bloom_hashes", &hashes_json);
        }
    }

    /// Restores the manifest from a checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    pub fn from_checkpoint(checkpoint: &SourceCheckpoint) -> Result<Self, serde_json::Error> {
        let active: HashMap<String, FileEntry> = match checkpoint.get_offset("manifest") {
            Some(json) => serde_json::from_str(json)?,
            None => HashMap::new(),
        };

        let bloom_hashes: Vec<u64> = match checkpoint.get_offset("manifest_bloom_hashes") {
            Some(json) => serde_json::from_str(json)?,
            None => Vec::new(),
        };

        let mut manifest = Self {
            active,
            bloom_hashes,
            bloom: None,
        };
        manifest.rebuild_bloom();
        Ok(manifest)
    }

    /// Rebuilds the Bloom filter from accumulated hashes.
    fn rebuild_bloom(&mut self) {
        if self.bloom_hashes.is_empty() {
            self.bloom = None;
            return;
        }
        // xorf::Xor8 requires at least 2 keys to construct.
        if self.bloom_hashes.len() < 2 {
            // With only 1 hash we fall back to linear scan via contains().
            // This is fine — it only happens transiently during the first eviction.
            self.bloom = None;
            return;
        }
        self.bloom = Some(xorf::Xor8::from(self.bloom_hashes.as_slice()));
    }
}

impl Default for FileIngestionManifest {
    fn default() -> Self {
        Self::new()
    }
}

/// Stable hash for a file path (used for Bloom filter keys).
fn hash_path(path: &str) -> u64 {
    // Use a simple FNV-1a-style hash for deterministic cross-platform results.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in path.as_bytes() {
        h ^= u64::from(*byte);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

#[allow(clippy::cast_possible_truncation)]
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(size: u64, ingested_at: u64) -> FileEntry {
        FileEntry {
            size,
            discovered_at: ingested_at.saturating_sub(100),
            ingested_at,
        }
    }

    #[test]
    fn test_insert_and_contains() {
        let mut m = FileIngestionManifest::new();
        assert!(!m.contains("a.csv"));
        m.insert("a.csv".into(), make_entry(100, 1000));
        assert!(m.contains("a.csv"));
        assert!(!m.contains("b.csv"));
    }

    #[test]
    fn test_size_changed() {
        let mut m = FileIngestionManifest::new();
        m.insert("a.csv".into(), make_entry(100, 1000));
        assert!(!m.size_changed("a.csv", 100));
        assert!(m.size_changed("a.csv", 200));
        assert!(!m.size_changed("unknown.csv", 100));
    }

    #[test]
    fn test_eviction_by_count() {
        let mut m = FileIngestionManifest::new();
        for i in 0..20 {
            m.insert(format!("file_{i}.csv"), make_entry(100, 1000 + i));
        }
        assert_eq!(m.active_count(), 20);

        // Evict to max 10, with no age limit.
        m.maybe_evict(10, u64::MAX);
        assert_eq!(m.active_count(), 10);
        assert_eq!(m.bloom_hashes.len(), 10);
    }

    #[test]
    fn test_evicted_files_detected_by_bloom() {
        let mut m = FileIngestionManifest::new();
        for i in 0..20 {
            m.insert(format!("file_{i}.csv"), make_entry(100, 1000 + i));
        }
        m.maybe_evict(10, u64::MAX);

        // Active files should be found.
        let active_found = (0..20)
            .filter(|i| m.contains(&format!("file_{i}.csv")))
            .count();
        // All 20 should be found (10 exact + 10 via bloom).
        assert_eq!(active_found, 20);

        // Unknown file should almost certainly not match (Xor8 FPR ~0.3%).
        assert!(!m.contains("completely_unknown_file_xyz.csv"));
    }

    #[test]
    fn test_checkpoint_roundtrip() {
        let mut m = FileIngestionManifest::new();
        m.insert("a.csv".into(), make_entry(100, 1000));
        m.insert("b.csv".into(), make_entry(200, 2000));

        let mut cp = SourceCheckpoint::new(1);
        m.to_checkpoint(&mut cp);

        let restored = FileIngestionManifest::from_checkpoint(&cp).unwrap();
        assert_eq!(restored.active_count(), 2);
        assert!(restored.contains("a.csv"));
        assert!(restored.contains("b.csv"));
    }

    #[test]
    fn test_checkpoint_roundtrip_with_bloom() {
        let mut m = FileIngestionManifest::new();
        for i in 0..20 {
            m.insert(format!("file_{i}.csv"), make_entry(100, 1000 + i));
        }
        m.maybe_evict(5, u64::MAX);

        let mut cp = SourceCheckpoint::new(1);
        m.to_checkpoint(&mut cp);

        let restored = FileIngestionManifest::from_checkpoint(&cp).unwrap();
        assert_eq!(restored.active_count(), 5);
        assert!(!restored.bloom_hashes.is_empty());
        // Evicted files should still be detected via bloom.
        let all_found = (0..20)
            .filter(|i| restored.contains(&format!("file_{i}.csv")))
            .count();
        assert_eq!(all_found, 20);
    }

    #[test]
    fn test_empty_manifest_checkpoint() {
        let m = FileIngestionManifest::new();
        let mut cp = SourceCheckpoint::new(0);
        m.to_checkpoint(&mut cp);

        let restored = FileIngestionManifest::from_checkpoint(&cp).unwrap();
        assert_eq!(restored.active_count(), 0);
    }
}
