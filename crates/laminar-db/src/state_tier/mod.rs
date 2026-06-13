//! Disk cold tier for demoted operator state.
//!
//! Stores `(operator, vnode)` checkpoint slices demoted out of memory. A
//! slice may be demoted only once its bytes are captured in a restorable
//! checkpoint, so the tier holds **capacity, not durability** — truth stays
//! in the object-store checkpoint artifacts, the tier runs without per-write
//! fsync, and it is wiped on restart (demoted vnodes rehydrate from their
//! partials). Losing it costs a rehydration, never data.
//!
//! The synchronous fjall handle is only ever touched from the worker
//! (`spawn_worker`) via `spawn_blocking`; the compute thread talks to the
//! tier through the worker's bounded channel, so a cold read never stalls it.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use bytes::Bytes;

use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;

/// fjall database subdirectory under the tier dir.
const DB_DIR: &str = "db";

/// The cold-tier store: one fjall database with a single KV-separated
/// keyspace holding `(operator, vnode) → slice bytes`.
///
/// Synchronous API by design — call it from the worker / `spawn_blocking`,
/// never from the compute thread.
pub(crate) struct StateTierStore {
    /// Owns the fjall database. The `slices` keyspace only holds a sender to
    /// this database's background flush/compaction workers, so the database
    /// must outlive it — held, never read after construction.
    #[allow(dead_code)]
    db: fjall::Database,
    slices: fjall::Keyspace,
    /// Logical accounting (key + value bytes; slice count) for the gauges.
    logical_bytes: AtomicI64,
    logical_slices: AtomicI64,
    metrics: Option<Arc<EngineMetrics>>,
}

impl StateTierStore {
    /// Open the tier under `dir`, wiping any leftover from a previous run —
    /// the tier never survives a restart (demoted state rehydrates from
    /// partials), so starting empty is the contract.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Storage` when the directory cannot be (re)created or
    /// the fjall database fails to open.
    pub(crate) fn open(
        dir: impl Into<std::path::PathBuf>,
        metrics: Option<Arc<EngineMetrics>>,
    ) -> Result<Self, DbError> {
        let dir = dir.into();
        if dir.exists() {
            tracing::info!(dir = %dir.display(), "wiping leftover cold tier on open");
            std::fs::remove_dir_all(&dir)
                .map_err(|e| DbError::Storage(format!("state tier wipe {}: {e}", dir.display())))?;
        }
        std::fs::create_dir_all(&dir)
            .map_err(|e| DbError::Storage(format!("state tier dir {}: {e}", dir.display())))?;

        let db = fjall::Database::builder(dir.join(DB_DIR))
            .open()
            .map_err(|e| DbError::Storage(format!("state tier open: {e}")))?;
        // Slices are multi-KB..MB blobs — exactly what key-value separation
        // is for (dev-box bench: 1.57x write amplification with it).
        let ks = db
            .keyspace("slices", || {
                fjall::KeyspaceCreateOptions::default()
                    .with_kv_separation(Some(fjall::KvSeparationOptions::default()))
            })
            .map_err(|e| DbError::Storage(format!("state tier keyspace: {e}")))?;

        Ok(Self {
            db,
            slices: ks,
            logical_bytes: AtomicI64::new(0),
            logical_slices: AtomicI64::new(0),
            metrics,
        })
    }

    fn key(operator: &str, vnode: u32) -> Vec<u8> {
        // NUL separator between the operator name (a SQL identifier, never
        // NUL) and the vnode — keeps keys unambiguous.
        let mut k = Vec::with_capacity(operator.len() + 5);
        k.extend_from_slice(operator.as_bytes());
        k.push(0);
        k.extend_from_slice(&vnode.to_be_bytes());
        k
    }

    /// Store a demoted slice (overwrites any previous demotion of the same
    /// vnode). No fsync — the tier is rebuildable by contract.
    pub(crate) fn put(&self, operator: &str, vnode: u32, bytes: &[u8]) -> Result<(), DbError> {
        let key = Self::key(operator, vnode);
        let old_len = self
            .slices
            .get(&key)
            .map_err(|e| DbError::Storage(format!("state tier read-before-put: {e}")))?
            .map(|o| o.len());
        self.slices
            .insert(&key, bytes)
            .map_err(|e| DbError::Storage(format!("state tier put: {e}")))?;
        #[allow(clippy::cast_possible_wrap)]
        let new_total = (key.len() + bytes.len()) as i64;
        if let Some(old_len) = old_len {
            #[allow(clippy::cast_possible_wrap)]
            self.logical_bytes
                .fetch_add(new_total - (key.len() + old_len) as i64, Ordering::Relaxed);
        } else {
            self.logical_bytes.fetch_add(new_total, Ordering::Relaxed);
            self.logical_slices.fetch_add(1, Ordering::Relaxed);
        }
        // `state_tier_demote_total` counts *effective* demotions, incremented by
        // the graph only when the operator actually drops the vnode — not here,
        // where a write may still be rolled back if the vnode turns out dirty.
        self.publish_gauges();
        Ok(())
    }

    /// Fetch a slice for promotion. `None` = not resident (e.g. wiped).
    pub(crate) fn get(&self, operator: &str, vnode: u32) -> Result<Option<Bytes>, DbError> {
        let start = std::time::Instant::now();
        let v = self
            .slices
            .get(Self::key(operator, vnode))
            .map_err(|e| DbError::Storage(format!("state tier get: {e}")))?;
        if let Some(ref m) = self.metrics {
            m.state_tier_fetch_total.inc();
            m.state_tier_fetch_duration
                .observe(start.elapsed().as_secs_f64());
        }
        Ok(v.map(|v| Bytes::copy_from_slice(&v)))
    }

    /// Drop a slice (after promotion, or when releasing a vnode).
    pub(crate) fn remove(&self, operator: &str, vnode: u32) -> Result<(), DbError> {
        let key = Self::key(operator, vnode);
        let old = self
            .slices
            .get(&key)
            .map_err(|e| DbError::Storage(format!("state tier read-before-remove: {e}")))?;
        if let Some(old) = old {
            self.slices
                .remove(&key)
                .map_err(|e| DbError::Storage(format!("state tier remove: {e}")))?;
            #[allow(clippy::cast_possible_wrap)]
            self.logical_bytes
                .fetch_sub((key.len() + old.len()) as i64, Ordering::Relaxed);
            self.logical_slices.fetch_sub(1, Ordering::Relaxed);
            self.publish_gauges();
        }
        Ok(())
    }

    /// Logical bytes currently resident (keys + values).
    pub(crate) fn logical_bytes(&self) -> i64 {
        self.logical_bytes.load(Ordering::Relaxed)
    }

    /// Slices currently resident.
    pub(crate) fn logical_slices(&self) -> i64 {
        self.logical_slices.load(Ordering::Relaxed)
    }

    fn publish_gauges(&self) {
        if let Some(ref m) = self.metrics {
            m.state_tier_bytes.set(self.logical_bytes());
            m.state_tier_slices.set(self.logical_slices());
        }
    }
}

mod worker;
pub(crate) use worker::{spawn_worker, TierRequest, TierTx};

#[cfg(test)]
mod tests;
