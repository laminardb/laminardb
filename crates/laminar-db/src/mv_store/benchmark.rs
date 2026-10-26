//! Feature-gated complete MV operations for Criterion.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use laminar_core::state::DEFAULT_KEY_GROUP_COUNT;

use super::{MvStorageMode, MvStore, ViewKey};
use crate::operator_graph::StateFrameCapture;
use crate::DbError;

const VIEWS: [&str; 2] = ["first", "second"];

/// Storage modes exercised by the non-default benchmark feature.
#[derive(Clone, Copy, Debug)]
pub enum MaterializedViewBenchmarkMode {
    /// Keep the latest 8192 rows in an append log.
    Append,
    /// Replace rows by their first-column key, in vnode slices.
    Upsert,
    /// Consolidate signed full-row multiplicities, in vnode slices placed by row.
    Multiset,
}

/// Fixture owning two independent local MVs.
pub struct MaterializedViewBenchmark {
    store: MvStore,
    mode: MaterializedViewBenchmarkMode,
    checkpoint_id: u64,
}

impl MaterializedViewBenchmark {
    /// Create two empty views with the same schema and storage mode.
    ///
    /// # Errors
    /// Returns MV schema or key-converter errors.
    pub fn new(schema: &SchemaRef, mode: MaterializedViewBenchmarkMode) -> Result<Self, DbError> {
        let changelog = |key| MvStorageMode::Changelog {
            key,
            key_groups: DEFAULT_KEY_GROUP_COUNT,
        };
        let storage = match mode {
            MaterializedViewBenchmarkMode::Append => MvStorageMode::AppendLog {
                retention: super::ViewRetention::Rows(8192),
            },
            MaterializedViewBenchmarkMode::Upsert => changelog(ViewKey::Keyed(vec![0])),
            MaterializedViewBenchmarkMode::Multiset => changelog(ViewKey::MultisetByRow),
        };
        let mut store = MvStore::new();
        for name in VIEWS {
            store.create_mv(name, Arc::clone(schema), storage.clone())?;
        }
        Ok(Self {
            store,
            mode,
            checkpoint_id: 0,
        })
    }

    /// Apply a cycle to both views.
    ///
    /// # Errors
    /// Returns state-admission or changelog-validation errors.
    pub fn update(&mut self, batches: &[RecordBatch]) -> Result<(), DbError> {
        self.store
            .update_views(VIEWS.map(|name| (name, batches)), false)
    }

    /// Materialize one view as a query/subscriber snapshot. An append view reads its memory
    /// rows, so call this before the first checkpoint.
    ///
    /// # Errors
    /// Returns Arrow conversion or snapshot-admission errors.
    pub fn snapshot(&self) -> Result<Option<RecordBatch>, DbError> {
        self.store.to_record_batch(VIEWS[0])
    }

    /// Capture the old cut, update both views, then encode the checkpoint. Returns the number
    /// of encoded payloads: one per append view with rows since the previous checkpoint, which
    /// then commits, and one per changed vnode of a changelog view.
    ///
    /// # Errors
    /// Returns checkpoint or update errors.
    pub fn checkpoint_during_update(&mut self, batches: &[RecordBatch]) -> Result<usize, DbError> {
        if let MaterializedViewBenchmarkMode::Append = self.mode {
            self.checkpoint_id += 1;
            let capture = self.store.capture_view_logs(self.checkpoint_id, u64::MAX)?;
            self.update(batches)?;
            let (logs, _) = capture.encode(u64::MAX)?;
            self.store.commit_view_logs(self.checkpoint_id);
            return Ok(logs.iter().map(|log| log.segments.len()).sum());
        }
        let vnode_count = u32::from(DEFAULT_KEY_GROUP_COUNT.get());
        let vnodes: Vec<u32> = (0..vnode_count).collect();
        let mut captures = Vec::new();
        for name in VIEWS {
            captures.extend(self.store.capture_view_vnodes(
                name,
                &vnodes,
                vnode_count,
                u64::MAX,
            )?);
        }
        self.update(batches)?;
        let mut staged = captures
            .iter()
            .filter_map(|captured| captured.state.as_ref())
            .map(StateFrameCapture::retained_bytes)
            .sum();
        let mut frames = 0;
        for capture in captures.into_iter().filter_map(|captured| captured.state) {
            capture.materialize(&mut staged, u64::MAX)?;
            frames += 1;
        }
        Ok(frames)
    }
}
