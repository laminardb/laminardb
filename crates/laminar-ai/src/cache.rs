//! Result cache for AI inference, keyed `(content_hash, model_id,
//! params_version)`.
//!
//! The key versions on both the model and its parameters, so a local model and
//! a remote model — or the same model under different parameters — never
//! collide on the same input text. Local results are deterministic and cacheable
//! permanently for correctness; remote results are cached as a cost-saver. The
//! cache itself does not distinguish the two — that policy lives in the caller.
//!
//! The cache is an in-memory [`foyer::Cache`] with S3-FIFO eviction, the same
//! crate the lookup and schema-registry caches use. A lookup is a memory op:
//! cheap enough to gate the inference worker from the operator without doing the
//! model call inline.

use std::sync::atomic::{AtomicU64, Ordering};

use foyer::{Cache, CacheBuilder};

use crate::provider::InferenceParams;
use crate::registry::Task;

/// Cache key. All fields are `Copy`, so lookups need no allocation and no
/// borrowed-key indirection.
///
/// - `content_hash`: xxh3-128 of the input text (see [`content_hash`]).
/// - `model_id`: a stable per-model integer assigned by the caller (the
///   registry), distinguishing models without hashing their names on lookup.
/// - `params_version`: a hash of the request parameters (see [`params_version`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AiCacheKey {
    /// xxh3-128 hash of the input content.
    pub content_hash: u128,
    /// Stable integer id of the model.
    pub model_id: u32,
    /// The task — a model can serve several (e.g. classify and sentiment), and
    /// they produce different outputs for the same input, so it must key the
    /// cache or results would collide across tasks.
    pub task: Task,
    /// Hash of the request parameters that affect the output.
    pub params_version: u64,
}

/// One row's cached inference output. Mirrors the per-row shape of
/// [`crate::provider::InferenceOutputs`] but singular, since the cache is keyed
/// per row of input.
#[derive(Debug, Clone, PartialEq)]
pub enum CachedOutput {
    /// A text output (label, completion, summary, …).
    Text(String),
    /// A numeric vector output (embedding).
    Vector(Vec<f32>),
}

/// xxh3-128 of the input content. Not cryptographic; a fast, collision-negligible
/// key for a result cache.
#[must_use]
pub fn content_hash(input: &str) -> u128 {
    xxhash_rust::xxh3::xxh3_128(input.as_bytes())
}

/// A stable hash of the parameters that change a model's output for the same
/// input — currently the candidate label set. Computed once per batch (all rows
/// in a batch share parameters), not per row.
///
/// Maintenance contract: every field of [`InferenceParams`] that can change the
/// output must be folded in here. It is hashed field-by-field rather than via a
/// derived `Hash` because parameters added later (e.g. an `f32` temperature)
/// are not `Hash`; fold those in explicitly (e.g. `to_bits()`).
#[must_use]
pub fn params_version(params: &InferenceParams) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    params.labels.hash(&mut hasher);
    hasher.finish()
}

/// Configuration for [`AiResultCache`].
#[derive(Debug, Clone, Copy)]
pub struct AiResultCacheConfig {
    /// Memory budget in bytes. Entries are weighted by payload size, so this
    /// bounds memory directly — an entry count would not, since an embedding
    /// vector is orders of magnitude larger than a one-word label.
    pub capacity_bytes: usize,
    /// Number of shards for concurrent access (power of 2).
    pub shards: usize,
}

impl Default for AiResultCacheConfig {
    fn default() -> Self {
        Self {
            capacity_bytes: 64 * 1024 * 1024,
            shards: 16,
        }
    }
}

/// Weight of one cache entry: its payload bytes plus fixed key/bookkeeping
/// overhead, so tiny entries still count against the budget.
fn entry_weight(_key: &AiCacheKey, value: &CachedOutput) -> usize {
    let payload = match value {
        CachedOutput::Text(s) => s.len(),
        CachedOutput::Vector(v) => v.len() * std::mem::size_of::<f32>(),
    };
    payload + std::mem::size_of::<AiCacheKey>() + 32
}

/// foyer-backed in-memory cache of per-row inference results.
///
/// `foyer::Cache` is internally sharded and lock-free on the read path, so
/// [`AiResultCache`] is `Send + Sync`.
pub struct AiResultCache {
    cache: Cache<AiCacheKey, CachedOutput>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl AiResultCache {
    /// Create a cache with the given configuration.
    #[must_use]
    pub fn new(config: AiResultCacheConfig) -> Self {
        let cache = CacheBuilder::new(config.capacity_bytes)
            .with_shards(config.shards)
            .with_weighter(entry_weight)
            .build();
        Self {
            cache,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Create a cache with default configuration.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(AiResultCacheConfig::default())
    }

    /// Look up a cached result, recording a hit or miss.
    #[must_use]
    pub fn get(&self, key: &AiCacheKey) -> Option<CachedOutput> {
        if let Some(entry) = self.cache.get(key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.value().clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert or update a cached result.
    pub fn insert(&self, key: AiCacheKey, value: CachedOutput) {
        self.cache.insert(key, value);
    }

    /// Total cache hits since creation.
    #[must_use]
    pub fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Total cache misses since creation.
    #[must_use]
    pub fn miss_count(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Number of entries currently cached.
    #[must_use]
    pub fn len(&self) -> usize {
        self.cache.entries()
    }

    /// Whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl std::fmt::Debug for AiResultCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AiResultCache")
            .field("len", &self.len())
            .field("hits", &self.hit_count())
            .field("misses", &self.miss_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(content: &str, model_id: u32, labels: Option<Vec<String>>) -> AiCacheKey {
        let params = InferenceParams { labels };
        AiCacheKey {
            content_hash: content_hash(content),
            model_id,
            task: Task::Sentiment,
            params_version: params_version(&params),
        }
    }

    #[test]
    fn params_version_separates_label_sets() {
        let a = InferenceParams {
            labels: Some(vec!["pos".into(), "neg".into()]),
        };
        let b = InferenceParams {
            labels: Some(vec!["pos".into(), "neg".into(), "neutral".into()]),
        };
        assert_eq!(params_version(&a), params_version(&a));
        assert_ne!(params_version(&a), params_version(&b));
        assert_ne!(
            params_version(&a),
            params_version(&InferenceParams::default())
        );
    }

    #[test]
    fn same_text_different_model_does_not_collide() {
        let cache = AiResultCache::with_defaults();
        let finbert = key("flat quarter", 1, None);
        let remote = key("flat quarter", 2, None);
        cache.insert(finbert, CachedOutput::Text("neutral".into()));
        cache.insert(remote, CachedOutput::Text("negative".into()));
        assert_eq!(
            cache.get(&finbert),
            Some(CachedOutput::Text("neutral".into()))
        );
        assert_eq!(
            cache.get(&remote),
            Some(CachedOutput::Text("negative".into()))
        );
        assert_eq!(cache.hit_count(), 2);
    }
}
