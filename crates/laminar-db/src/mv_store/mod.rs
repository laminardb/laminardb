//! Materialized view result storage, queryable via `SELECT * FROM mv_name`.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::SchemaRef;
use laminar_core::changelog::WEIGHT_COLUMN;

use crate::error::DbError;

mod admission;
mod checkpoint;
mod multiset;
mod staging;
mod upsert;
use admission::{MvLimits, PreparedUpdate};
use multiset::MultisetState;
use upsert::UpsertState;

#[cfg(feature = "benchmark-internals")]
pub(crate) mod benchmark;

#[cfg(test)]
use checkpoint::{batches_to_ipc, ipc_to_schema_and_batches, multiset_checkpoint_schema};
pub(crate) use checkpoint::{MvCheckpointCapture, CHECKPOINT_KEY_PREFIX};

/// Default maximum batches retained in append mode.
const DEFAULT_APPEND_MAX_BATCHES: usize = 1000;

/// Existing multiset snapshot expansion byte guard (256 MiB), separate from live quotas.
const DEFAULT_MAX_BYTES: usize = 256 * 1024 * 1024;

/// A multiset read must fit in one Arrow batch. Refuse pathological multiplicities before
/// `RowConverter` allocates its expanded row vector and output arrays.
const MAX_MULTISET_MATERIALIZED_ROWS: usize = 1_000_000;
const MULTISET_MATERIALIZATION_ROW_OVERHEAD: usize = 64;

/// How a materialized view accumulates results.
#[derive(Debug, Clone)]
pub(crate) enum MvStorageMode {
    /// GROUP BY queries: replace the result set each cycle.
    Aggregate,
    /// Non-aggregate queries: append with bounded retention.
    Append { max_batches: usize },
    /// Incremental keyed snapshot from a dirty-only `__weight` changelog; `key_cols` index the GROUP BY columns.
    Upsert { key_cols: Vec<usize> },
    /// Chained projection/filter: Z-set multiset keyed by the full row; handles key-dropping dups.
    Multiset,
}

impl MvStorageMode {
    pub fn append_default() -> Self {
        Self::Append {
            max_batches: DEFAULT_APPEND_MAX_BATCHES,
        }
    }
}

/// Split a `__weight` changelog batch into its Int64 weight column and the non-weight column indices.
fn weight_and_plain_cols(batch: &RecordBatch) -> Result<(&Int64Array, Vec<usize>), DbError> {
    let weight_idx = batch
        .schema_ref()
        .index_of(WEIGHT_COLUMN)
        .map_err(|e| DbError::Storage(format!("MV changelog missing weight: {e}")))?;
    let weights = batch
        .column(weight_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DbError::Storage("MV weight column not Int64".into()))?;
    let plain_cols = (0..batch.num_columns())
        .filter(|&c| c != weight_idx)
        .collect();
    Ok((weights, plain_cols))
}

/// Per-MV result store.
pub(crate) struct MvEntry {
    schema: SchemaRef,
    mode: MvStorageMode,
    batches: VecDeque<RecordBatch>,
    /// Present only in `Upsert` mode.
    upsert: Option<UpsertState>,
    /// Present only in `Multiset` mode.
    multiset: Option<MultisetState>,
    approx_bytes: usize,
    rows: usize,
}

impl MvEntry {
    fn new(schema: SchemaRef, mode: MvStorageMode) -> Result<Self, DbError> {
        let upsert = match &mode {
            MvStorageMode::Upsert { key_cols } => Some(UpsertState::new(&schema, key_cols)?),
            _ => None,
        };
        let multiset = match &mode {
            MvStorageMode::Multiset => Some(MultisetState::new(&schema)?),
            _ => None,
        };
        Ok(Self {
            schema,
            mode,
            batches: VecDeque::new(),
            upsert,
            multiset,
            approx_bytes: 0,
            rows: 0,
        })
    }

    fn to_record_batch(&self) -> Result<RecordBatch, DbError> {
        if let Some(up) = self.upsert.as_ref() {
            return up.to_record_batch(&self.schema);
        }
        if let Some(ms) = self.multiset.as_ref() {
            return ms.to_record_batch(&self.schema);
        }
        if self.batches.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        let refs: Vec<&RecordBatch> = self.batches.iter().collect();
        arrow::compute::concat_batches(&self.schema, refs.iter().copied())
            .map_err(|e| DbError::Storage(format!("MV batch concat: {e}")))
    }
}

/// Store for all materialized view results; shared via `Arc<RwLock<MvStore>>`.
pub(crate) struct MvStore {
    entries: HashMap<String, MvEntry>,
    limits: MvLimits,
    /// Lets the hot path skip the write lock when no MVs exist.
    has_any: Arc<AtomicBool>,
}

impl MvStore {
    #[cfg(any(test, feature = "benchmark-internals"))]
    pub fn new() -> Self {
        Self::from_config(&crate::LaminarConfig::default())
    }

    pub fn from_config(config: &crate::LaminarConfig) -> Self {
        Self {
            entries: HashMap::new(),
            limits: MvLimits {
                rows: config.materialized_view_max_rows,
                bytes: config.materialized_view_max_bytes,
            },
            has_any: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn has_any_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.has_any)
    }

    pub fn create_mv(
        &mut self,
        name: &str,
        schema: SchemaRef,
        mode: MvStorageMode,
    ) -> Result<(), DbError> {
        self.entries
            .insert(name.to_string(), MvEntry::new(schema, mode)?);
        self.has_any.store(true, Ordering::Release);
        Ok(())
    }

    pub fn drop_mv(&mut self, name: &str) -> bool {
        let removed = self.entries.remove(name).is_some();
        if self.entries.is_empty() {
            self.has_any.store(false, Ordering::Release);
        }
        removed
    }

    pub fn has_mv(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    #[cfg(test)]
    pub(crate) fn storage_mode_for_test(&self, name: &str) -> Option<MvStorageMode> {
        self.entries.get(name).map(|entry| entry.mode.clone())
    }

    #[cfg(feature = "cluster")]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Apply one cycle's output atomically to a single MV.
    #[cfg(test)]
    pub fn update_cycle(&mut self, name: &str, batches: &[RecordBatch]) -> Result<(), DbError> {
        if let Some(entry) = self.entries.get_mut(name) {
            let update = entry.prepare_cycle(name, batches, self.limits, false)?;
            entry.apply_prepared(update);
        }
        Ok(())
    }

    /// Preflight every affected view under the caller's existing write lock, then install.
    /// Callers supply each view once. Prepared batch updates borrow input; keyed
    /// updates own only their existing staged deltas, never a clone of the live row map.
    pub fn update_views<'a>(
        &mut self,
        results: impl IntoIterator<Item = (&'a str, &'a [RecordBatch])>,
    ) -> Result<(), DbError> {
        let mut prepared = smallvec::SmallVec::<[(&str, PreparedUpdate<'_>); 4]>::new();
        for (name, batches) in results {
            if let Some(entry) = self.entries.get(name) {
                let update = entry.prepare_cycle(name, batches, self.limits, true)?;
                prepared.push((name, update));
            }
        }
        for (name, update) in prepared {
            if let Some(entry) = self.entries.get_mut(name) {
                entry.apply_prepared(update);
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn update(&mut self, name: &str, batch: &RecordBatch) {
        self.update_cycle(name, std::slice::from_ref(batch))
            .expect("test MV update must succeed");
    }

    pub fn to_record_batch(&self, name: &str) -> Result<Option<RecordBatch>, DbError> {
        self.entries
            .get(name)
            .map(MvEntry::to_record_batch)
            .transpose()
    }

    pub fn total_bytes(&self) -> usize {
        self.entries
            .values()
            .fold(0, |total, entry| total.saturating_add(entry.approx_bytes))
    }

    /// Build an empty image with the current catalog shape and the same hot-path presence flag.
    pub fn fresh_image(&self) -> Result<Self, DbError> {
        let mut entries = HashMap::with_capacity(self.entries.len());
        for (name, entry) in &self.entries {
            entries.insert(
                name.clone(),
                MvEntry::new(Arc::clone(&entry.schema), entry.mode.clone())?,
            );
        }
        Ok(Self {
            entries,
            limits: self.limits,
            has_any: Arc::clone(&self.has_any),
        })
    }

    /// Restore a complete checkpoint into a private image. The live store is never mutated.
    pub fn recovery_image(&self, states: &HashMap<String, Vec<u8>>) -> Result<Self, DbError> {
        let mut image = self.fresh_image().map_err(|error| {
            DbError::Checkpoint(format!("cannot create an empty MV recovery image: {error}"))
        })?;
        let mut restored = HashSet::with_capacity(states.len());

        for (name, bytes) in states {
            if !image.entries.contains_key(name) {
                return Err(DbError::Checkpoint(format!(
                    "MV checkpoint '{name}' has no matching registered materialized view"
                )));
            }
            image.restore_from_ipc(name, bytes).map_err(|error| {
                DbError::Checkpoint(format!("MV restore failed for '{name}': {error}"))
            })?;
            restored.insert(name.as_str());
        }

        let mut missing: Vec<&str> = image
            .entries
            .keys()
            .map(String::as_str)
            .filter(|name| !restored.contains(name))
            .collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            return Err(DbError::Checkpoint(format!(
                "MV checkpoint is missing required state for: {}",
                missing.join(", ")
            )));
        }

        Ok(image)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod quota_tests;

#[cfg(test)]
mod dictionary_tests;
