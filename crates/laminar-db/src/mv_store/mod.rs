//! Materialized view result storage, queryable via `SELECT * FROM mv_name`.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::SchemaRef;
use laminar_core::changelog::WEIGHT_COLUMN;
use laminar_core::state::KeyGroupCount;

use crate::checkpoint_coordinator::committed_reads::CommittedCuts;
use crate::error::DbError;
use crate::operator::capability::OperatorStateClass;
use crate::operator_graph::InputFrontier;

mod admission;
mod append_log;
mod checkpoint;
mod committed;
mod frames;
mod freshness;
mod multiset;
mod slices;
mod staging;
mod statement_snapshot;
#[cfg(feature = "cluster")]
mod transition;
mod upsert;
use admission::{MvLimits, PreparedUpdate};
pub(crate) use append_log::{spawn_compactor as spawn_log_compactor, LogRead, ViewRetention};
use append_log::{AppendLogState, LogHorizon};
pub(crate) use committed::CommittedViews;
pub use committed::{CommittedRead, CommittedReadError};
pub(crate) use frames::RestoredView;
pub(crate) use freshness::{spawn_monitor as spawn_freshness_monitor, FreshnessContracts};
use multiset::MultisetState;
pub(crate) use statement_snapshot::{
    plan_committed_statement, plan_statement, read_views, CommittedStatement,
    StatementViewSnapshot, VIEW_READ_DEADLINE,
};
#[cfg(feature = "cluster")]
pub(crate) use transition::ViewTransition;
use upsert::UpsertState;

#[cfg(feature = "benchmark-internals")]
pub(crate) mod benchmark;

pub(crate) use checkpoint::MvCheckpointCapture;
#[cfg(test)]
use checkpoint::{batches_to_ipc, ipc_to_schema_and_batches, multiset_checkpoint_schema};

/// Existing multiset snapshot expansion byte guard (256 MiB), separate from live quotas.
const DEFAULT_MAX_BYTES: usize = 256 * 1024 * 1024;

/// A multiset read must fit in one Arrow batch. Refuse pathological multiplicities before
/// `RowConverter` allocates its expanded row vector and output arrays.
const MAX_MULTISET_MATERIALIZED_ROWS: usize = 1_000_000;
const MULTISET_MATERIALIZATION_ROW_OVERHEAD: usize = 64;

/// How a changelog view keys its rows and places them on vnodes, from the view stream's
/// planned output distribution. Column indices are positions in the view schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ViewKey {
    /// A singleton output: at most one live row, on vnode 0.
    Global,
    /// A keyed aggregate: rows upserted on these columns and placed by them.
    Keyed(Vec<usize>),
    /// Full rows with multiplicities, placed by the inherited hash key on these columns.
    MultisetByKey(Vec<usize>),
    /// Full rows with multiplicities, placed by the full row.
    MultisetByRow,
}

impl ViewKey {
    pub(crate) const fn state_class(&self) -> OperatorStateClass {
        match self {
            Self::Global => OperatorStateClass::GlobalSingleton,
            Self::Keyed(_) | Self::MultisetByKey(_) | Self::MultisetByRow => {
                OperatorStateClass::VnodeKeyed
            }
        }
    }
}

/// How a materialized view accumulates results.
#[derive(Debug, Clone)]
pub(crate) enum MvStorageMode {
    /// Append-only rows kept as a committed log under a declared retention.
    AppendLog { retention: ViewRetention },
    /// A `__weight` changelog maintained as vnode slices over `key_groups` vnodes.
    Changelog {
        key: ViewKey,
        key_groups: KeyGroupCount,
    },
}

impl MvStorageMode {
    #[cfg(test)]
    pub(crate) fn append_rows(rows: u64) -> Self {
        Self::AppendLog {
            retention: ViewRetention::Rows(rows),
        }
    }

    #[cfg(test)]
    pub(crate) fn changelog(key: ViewKey) -> Self {
        Self::Changelog {
            key,
            key_groups: laminar_core::state::DEFAULT_KEY_GROUP_COUNT,
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
    /// Present only for an append-only view.
    log: Option<AppendLogState>,
    /// Present only for a `Global` or `Keyed` changelog view.
    upsert: Option<UpsertState>,
    /// Present only for a multiset changelog view.
    multiset: Option<MultisetState>,
    approx_bytes: usize,
}

impl MvEntry {
    fn new(schema: SchemaRef, mode: MvStorageMode) -> Result<Self, DbError> {
        let (mut log, mut upsert, mut multiset) = (None, None, None);
        match &mode {
            MvStorageMode::AppendLog { retention } => {
                log = Some(AppendLogState::new(retention.clone()));
            }
            MvStorageMode::Changelog { key, key_groups } => match key {
                ViewKey::Global => upsert = Some(UpsertState::new(&schema, &[], *key_groups)?),
                ViewKey::Keyed(columns) => {
                    upsert = Some(UpsertState::new(&schema, columns, *key_groups)?);
                }
                ViewKey::MultisetByKey(columns) => {
                    multiset = Some(MultisetState::new(&schema, columns, *key_groups)?);
                }
                ViewKey::MultisetByRow => {
                    multiset = Some(MultisetState::new(&schema, &[], *key_groups)?);
                }
            },
        }
        Ok(Self {
            schema,
            mode,
            log,
            upsert,
            multiset,
            approx_bytes: 0,
        })
    }

    const fn places_by_row(&self) -> bool {
        matches!(
            self.mode,
            MvStorageMode::Changelog {
                key: ViewKey::MultisetByRow,
                ..
            }
        )
    }

    fn to_record_batch(&self) -> Result<RecordBatch, DbError> {
        if let Some(up) = self.upsert.as_ref() {
            return up.to_record_batch(&self.schema);
        }
        if let Some(ms) = self.multiset.as_ref() {
            return ms.to_record_batch(&self.schema);
        }
        match &self.log {
            Some(log) => log.memory_batch(&self.schema),
            None => Err(DbError::Storage("MV has no result state".into())),
        }
    }
}

/// One view's contents captured under the store read guard.
pub(crate) enum ViewRead {
    Ready(RecordBatch),
    Log(Box<LogRead>),
}

/// Store for all materialized view results; shared via `Arc<RwLock<MvStore>>`.
pub(crate) struct MvStore {
    entries: HashMap<String, MvEntry>,
    limits: MvLimits,
    /// Lets the hot path skip the write lock when no MVs exist.
    has_any: Arc<AtomicBool>,
    /// Reads committed checkpoints once checkpointing is configured.
    committed: Option<Arc<CommittedViews>>,
    /// The checkpoint whose view logs the store last committed or restored.
    log_checkpoint: Option<u64>,
    /// The checkpoint attempt whose view-log capture awaits its outcome.
    log_capture: Option<u64>,
    /// In a cluster, the committed cuts whose view frontiers append-log retention follows.
    horizon_cuts: Option<CommittedCuts>,
    /// Counts the rows append views drop because they arrived behind the retention horizon.
    horizon_drops: Option<prometheus::IntCounter>,
    freshness: Arc<FreshnessContracts>,
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
            committed: None,
            log_checkpoint: None,
            log_capture: None,
            horizon_cuts: None,
            horizon_drops: None,
            freshness: Arc::default(),
        }
    }

    pub fn has_any_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.has_any)
    }

    /// Read committed checkpoints through `views` from now on.
    pub(crate) fn set_committed_views(&mut self, views: Arc<CommittedViews>) {
        self.committed = Some(views);
    }

    pub(crate) fn committed_views(&self) -> Option<Arc<CommittedViews>> {
        self.committed.clone()
    }

    /// Count on `counter` the rows append views drop behind their retention horizon.
    pub(crate) fn count_horizon_drops(&mut self, counter: prometheus::IntCounter) {
        self.horizon_drops = Some(counter);
    }

    /// In a cluster, keep append-log rows against each view's frontier at the newest of `cuts`
    /// rather than this node's own frontier.
    #[cfg(feature = "cluster")]
    pub(crate) fn follow_committed_frontiers(&mut self, cuts: CommittedCuts) {
        self.horizon_cuts = Some(cuts);
        self.install_log_horizons();
    }

    /// Point every append log's retention at the frontier this store follows.
    fn install_log_horizons(&mut self) {
        let committed = self
            .horizon_cuts
            .as_mut()
            .map(|cuts| cuts.borrow_and_update().clone());
        for (name, entry) in &mut self.entries {
            let Some(log) = entry.log.as_mut() else {
                continue;
            };
            log.set_horizon(match &committed {
                None => LogHorizon::Local,
                Some(cut) => LogHorizon::Committed(
                    cut.as_ref()
                        .and_then(|cut| cut.index.view_frontiers.get(name).copied()),
                ),
            });
        }
    }

    /// Whether view `name` places its rows by the full row, so in a cluster the node that
    /// produces a row is not in general the one that keeps it.
    #[cfg(feature = "cluster")]
    pub(crate) fn places_by_row(&self, name: &str) -> bool {
        self.entries.get(name).is_some_and(MvEntry::places_by_row)
    }

    /// The vnode each row of the changelog `batch` takes in view `name`.
    #[cfg(feature = "cluster")]
    pub(crate) fn row_vnodes(&self, name: &str, batch: &RecordBatch) -> Result<Vec<u32>, DbError> {
        self.entries
            .get(name)
            .and_then(|entry| entry.multiset.as_ref())
            .ok_or_else(|| {
                DbError::MaterializedView(format!("view '{name}' keeps no multiset to place rows"))
            })?
            .row_vnodes(batch)
    }

    /// Whether this store holds only this node's part of each view, as in a cluster, so a
    /// statement reads views only at a committed cut.
    pub(crate) const fn holds_node_part(&self) -> bool {
        self.horizon_cuts.is_some()
    }

    /// Freshness the views declared; it outlives recovery images of this store.
    pub(crate) fn freshness_contracts(&self) -> Arc<FreshnessContracts> {
        Arc::clone(&self.freshness)
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
        self.install_log_horizons();
        Ok(())
    }

    pub fn drop_mv(&mut self, name: &str) -> bool {
        self.freshness.remove(name);
        let removed = self.entries.remove(name).is_some();
        if self.entries.is_empty() {
            self.has_any.store(false, Ordering::Release);
        }
        removed
    }

    /// Whether any view keeps an append log; without one there is no view log to adopt.
    #[cfg(feature = "cluster")]
    pub(crate) fn has_append_views(&self) -> bool {
        self.entries.values().any(|entry| entry.log.is_some())
    }

    pub fn has_mv(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    #[cfg(test)]
    pub(crate) fn storage_mode_for_test(&self, name: &str) -> Option<MvStorageMode> {
        self.entries.get(name).map(|entry| entry.mode.clone())
    }

    /// The vnode placement of `name`'s slices, or `None` for an append view.
    pub(crate) fn view_state_class(&self, name: &str) -> Option<OperatorStateClass> {
        match &self.entries.get(name)?.mode {
            MvStorageMode::Changelog { key, .. } => Some(key.state_class()),
            MvStorageMode::AppendLog { .. } => None,
        }
    }

    /// Apply one cycle's output atomically to a single MV.
    #[cfg(test)]
    pub fn update_cycle(&mut self, name: &str, batches: &[RecordBatch]) -> Result<(), DbError> {
        if let Some(entry) = self.entries.get_mut(name) {
            let update = entry.prepare_cycle(name, batches, self.limits, false, None)?;
            entry.apply_prepared(update);
        }
        Ok(())
    }

    /// Preflight every affected view under the caller's existing write lock, then install.
    /// Callers supply each view once. Prepared batch updates borrow input; keyed
    /// updates own only their existing staged deltas, never a clone of the live row map.
    /// With `routed_by_row`, views placed by their full row are skipped: in a cluster their
    /// result nodes route those rows to their owners.
    pub fn update_views<'a>(
        &mut self,
        results: impl IntoIterator<Item = (&'a str, &'a [RecordBatch])>,
        routed_by_row: bool,
    ) -> Result<(), DbError> {
        let mut prepared = smallvec::SmallVec::<[(&str, PreparedUpdate<'_>); 4]>::new();
        for (name, batches) in results {
            if let Some(entry) = self
                .entries
                .get(name)
                .filter(|entry| !(routed_by_row && entry.places_by_row()))
            {
                let update = entry.prepare_cycle(
                    name,
                    batches,
                    self.limits,
                    true,
                    self.horizon_drops.as_ref(),
                )?;
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

    /// Advance each append view's frontier to its stream's output frontier, and in a cluster
    /// follow a newly committed cut.
    pub(crate) fn advance_view_frontiers(
        &mut self,
        mut stream: impl FnMut(&str) -> Option<InputFrontier>,
    ) {
        if self
            .horizon_cuts
            .as_ref()
            .is_some_and(|cuts| cuts.has_changed().unwrap_or(false))
        {
            self.install_log_horizons();
        }
        for (name, entry) in &mut self.entries {
            if let (Some(log), Some(stream)) = (entry.log.as_mut(), stream(name)) {
                log.advance_frontier(stream);
            }
        }
    }

    #[cfg(test)]
    pub fn update(&mut self, name: &str, batch: &RecordBatch) {
        self.update_cycle(name, std::slice::from_ref(batch))
            .expect("test MV update must succeed");
    }

    /// A changelog view's rows, or an append view's rows while none are committed. An append
    /// view with committed segments is read with [`Self::read`].
    pub fn to_record_batch(&self, name: &str) -> Result<Option<RecordBatch>, DbError> {
        self.entries
            .get(name)
            .map(MvEntry::to_record_batch)
            .transpose()
    }

    /// Capture `name` for one read; an append view's committed segments load after the guard.
    pub(crate) fn read(&self, name: &str) -> Result<Option<ViewRead>, DbError> {
        let Some(entry) = self.entries.get(name) else {
            return Ok(None);
        };
        let Some(log) = &entry.log else {
            return entry
                .to_record_batch()
                .map(|batch| Some(ViewRead::Ready(batch)));
        };
        let (segments, memory, frontier) = log.read_parts();
        let committed = match (&self.committed, self.log_checkpoint) {
            (Some(views), Some(checkpoint)) => Some((Arc::clone(views), checkpoint)),
            _ if segments.is_empty() => None,
            _ => {
                return Err(DbError::MaterializedView(format!(
                    "view '{name}' has committed segments but no checkpoint store to read them"
                )))
            }
        };
        Ok(Some(ViewRead::Log(Box::new(LogRead {
            view: name.to_owned(),
            schema: Arc::clone(&entry.schema),
            retention: log.retention().clone(),
            frontier,
            committed,
            segments,
            memory,
        }))))
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
        let mut image = Self {
            entries,
            limits: self.limits,
            has_any: Arc::clone(&self.has_any),
            committed: self.committed.clone(),
            log_checkpoint: None,
            log_capture: None,
            horizon_cuts: self.horizon_cuts.clone(),
            horizon_drops: self.horizon_drops.clone(),
            freshness: Arc::clone(&self.freshness),
        };
        image.install_log_horizons();
        Ok(image)
    }

    /// Restore a complete checkpoint into a private image: changelog views from the slices their
    /// graph nodes restored, append views from the logs `manifest` retains plus the logs
    /// `adopted` from participants that left. The live store is never mutated.
    pub fn recovery_image(
        &self,
        restored_views: Vec<RestoredView>,
        manifest: Option<&laminar_core::checkpoint::CheckpointManifest>,
        adopted: &[laminar_core::checkpoint::AdoptedViewLog],
    ) -> Result<Self, DbError> {
        let mut image = self.restored_image(restored_views, manifest, |_| true)?;
        image.adopt_view_logs(adopted)?;
        Ok(image)
    }

    /// Restore a topology migration's root cut into a private image: as `recovery_image`, except
    /// that only the `preserved` views take their logs from `manifest`. Views the migration adds
    /// start empty, and the logs of views it drops are left behind.
    #[cfg(feature = "cluster")]
    pub(crate) fn migration_root_image(
        &self,
        restored_views: Vec<RestoredView>,
        manifest: Option<&laminar_core::checkpoint::CheckpointManifest>,
        preserved: &[&str],
    ) -> Result<Self, DbError> {
        self.restored_image(restored_views, manifest, |view| preserved.contains(&view))
    }

    fn restored_image(
        &self,
        restored_views: Vec<RestoredView>,
        manifest: Option<&laminar_core::checkpoint::CheckpointManifest>,
        restores: impl Fn(&str) -> bool,
    ) -> Result<Self, DbError> {
        let mut image = self.fresh_image().map_err(|error| {
            DbError::Checkpoint(format!("cannot create an empty MV recovery image: {error}"))
        })?;
        for view in restored_views {
            view.install(&mut image.entries)?;
        }
        image.restore_view_logs(manifest, restores)?;
        image.install_log_horizons();
        Ok(image)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod quota_tests;

#[cfg(test)]
mod dictionary_tests;
