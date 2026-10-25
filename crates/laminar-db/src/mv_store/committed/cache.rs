//! Decoded committed artifacts by digest, under a byte budget. A frame or segment never changes
//! once committed, so its digest names its decoded rows for every later read.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use quick_cache::sync::{Cache, DefaultLifecycle};
use quick_cache::{DefaultHashBuilder, Weighter};

/// Charged per entry on top of its batches, so small artifacts still count.
const ENTRY_OVERHEAD_BYTES: u64 = 128;
/// Expected bytes per entry, used only to size the cache's tables.
const TYPICAL_ENTRY_BYTES: usize = 64 * 1024;

pub(super) type Rows = Arc<[RecordBatch]>;

#[derive(Debug, Clone)]
struct RowsWeighter;

impl Weighter<String, Rows> for RowsWeighter {
    fn weight(&self, digest: &String, rows: &Rows) -> u64 {
        let batches = rows
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .fold(0usize, usize::saturating_add);
        u64::try_from(batches.saturating_add(digest.len()))
            .unwrap_or(u64::MAX)
            .saturating_add(ENTRY_OVERHEAD_BYTES)
    }
}

pub(super) struct ArtifactCache {
    cache: Cache<String, Rows, RowsWeighter>,
}

impl ArtifactCache {
    pub(super) fn new(capacity_bytes: usize) -> Self {
        let estimated_items = (capacity_bytes / TYPICAL_ENTRY_BYTES).max(16);
        Self {
            cache: Cache::with(
                estimated_items,
                u64::try_from(capacity_bytes).unwrap_or(u64::MAX),
                RowsWeighter,
                DefaultHashBuilder::default(),
                DefaultLifecycle::default(),
            ),
        }
    }

    /// The rows decoded from `digest`, when they were decoded for `schema`.
    pub(super) fn get(&self, digest: &str, schema: &SchemaRef) -> Option<Rows> {
        self.cache
            .get(digest)
            .filter(|rows| rows.iter().all(|batch| batch.schema_ref() == schema))
    }

    pub(super) fn insert(&self, digest: &str, rows: Rows) {
        self.cache.insert(digest.to_owned(), rows);
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.cache.len()
    }
}
