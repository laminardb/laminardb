//! Per-row inference result cache, keyed `(content_hash, model_id, params_version)`.
//!
//! The composite key ensures different models and parameter sets never collide on
//! the same input. Backed by `quick_cache::sync::Cache` with S3-FIFO eviction.

use quick_cache::sync::{Cache, DefaultLifecycle};
use quick_cache::{DefaultHashBuilder, Weighter};

use crate::ai::provider::InferenceParams;
use crate::ai::registry::Task;

/// Cache key. All fields are `Copy`; lookups need no allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AiCacheKey {
    /// xxh3-128 of the input text.
    pub content_hash: u128,
    /// Stable per-model integer assigned by the registry.
    pub model_id: u32,
    /// Task is part of the key because the same model returns different outputs
    /// for classify vs sentiment on the same input.
    pub task: Task,
    /// Hash of the request parameters (see [`params_version`]).
    pub params_version: u64,
}

/// One row's cached inference output.
#[derive(Debug, Clone, PartialEq)]
pub enum CachedOutput {
    /// A text output (label, completion, summary, …).
    Text(String),
    /// A numeric embedding vector.
    Vector(Vec<f32>),
    /// A scalar sentiment score in `[-1, 1]`.
    Score(f64),
}

/// xxh3-128 of the input content.
#[must_use]
pub fn content_hash(input: &str) -> u128 {
    xxhash_rust::xxh3::xxh3_128(input.as_bytes())
}

/// Hash of the parameters that affect model output (currently the label set).
///
/// Fold every output-affecting field of [`InferenceParams`] here explicitly —
/// non-`Hash` fields like `f32` temperature must use `to_bits()`.
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
    /// Memory budget. Entries are weighted by payload size, not count, so
    /// large embeddings and small labels are both bounded correctly.
    pub capacity_bytes: usize,
}

impl Default for AiResultCacheConfig {
    fn default() -> Self {
        Self {
            capacity_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Payload bytes plus key/overhead so tiny entries still count against the budget.
#[derive(Debug, Clone)]
struct OutputWeighter;

impl Weighter<AiCacheKey, CachedOutput> for OutputWeighter {
    fn weight(&self, _key: &AiCacheKey, value: &CachedOutput) -> u64 {
        let payload = match value {
            CachedOutput::Text(s) => s.len(),
            CachedOutput::Vector(v) => v.len() * std::mem::size_of::<f32>(),
            CachedOutput::Score(_) => std::mem::size_of::<f64>(),
        };
        (payload + std::mem::size_of::<AiCacheKey>() + 32) as u64
    }
}

/// `quick_cache`-backed in-memory cache of per-row inference results.
pub struct AiResultCache {
    cache: Cache<AiCacheKey, CachedOutput, OutputWeighter>,
}

impl AiResultCache {
    /// Create a cache with the given configuration.
    #[must_use]
    pub fn new(config: AiResultCacheConfig) -> Self {
        // Rough item count for internal table sizing; ~256 B/entry assumed.
        let estimated_items = (config.capacity_bytes / 256).max(64);
        let cache = Cache::with(
            estimated_items,
            config.capacity_bytes as u64,
            OutputWeighter,
            DefaultHashBuilder::default(),
            DefaultLifecycle::default(),
        );
        Self { cache }
    }

    /// Create a cache with default configuration (64 MiB).
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(AiResultCacheConfig::default())
    }

    /// Look up a cached result.
    #[must_use]
    pub fn get(&self, key: &AiCacheKey) -> Option<CachedOutput> {
        self.cache.get(key)
    }

    /// Insert a result.
    pub fn insert(&self, key: AiCacheKey, value: CachedOutput) {
        self.cache.insert(key, value);
    }

    /// Number of cached entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Whether the cache holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl std::fmt::Debug for AiResultCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AiResultCache")
            .field("len", &self.len())
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
    }
}
