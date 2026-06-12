//! Disk cold tier for demoted operator state.
//!
//! Stores `(operator, vnode)` checkpoint slices that have been demoted out
//! of memory: a slice may be demoted only when its bytes are already
//! captured in a restorable checkpoint, so the tier holds **capacity, not
//! durability** — truth stays in the object-store checkpoint artifacts, and
//! the tier runs without per-write fsync. Losing it costs a rehydration
//! from the last restorable epoch, never data.
//!
//! Lifecycle contract: a marker file records the engine version, a clean
//! flag, and the logical counters. On open, anything other than a clean
//! marker from the same engine version (unclean shutdown, version change,
//! corruption) **wipes the directory** and starts empty — the demoted state
//! rehydrates through the existing recovery path.
//!
//! Threading contract: the synchronous fjall handle is only ever touched
//! from the worker (`spawn_worker`), which runs on the main tokio runtime
//! and pushes each store call into `spawn_blocking`. The compute thread
//! talks to the tier exclusively through the worker's bounded channel
//! (`try_send` on submit, drain replies on later cycles), so a cold read
//! can never stall it.

#![allow(dead_code)] // dormant until the demotion/promotion wiring lands

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use bytes::Bytes;

use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;

/// Bump when the marker format or the key encoding changes.
const FORMAT_VERSION: u32 = 1;
/// Marker file name, sibling of the fjall directory.
const MARKER_FILE: &str = "marker";
/// fjall database subdirectory.
const DB_DIR: &str = "db";

/// Why an existing tier directory was discarded at open.
#[derive(Debug, PartialEq, Eq)]
enum WipeReason {
    UncleanShutdown,
    FormatMismatch,
    EngineVersionMismatch,
    CorruptMarker,
}

/// Parsed contents of the marker file.
struct Marker {
    format: u32,
    engine: String,
    clean: bool,
    slices: i64,
    bytes: i64,
}

impl Marker {
    fn parse(text: &str) -> Option<Self> {
        let mut format = None;
        let mut engine = None;
        let mut clean = None;
        let mut slices = None;
        let mut bytes = None;
        for line in text.lines() {
            let (k, v) = line.split_once('=')?;
            match k {
                "format" => format = v.parse().ok(),
                "engine" => engine = Some(v.to_string()),
                "clean" => clean = v.parse().ok(),
                "slices" => slices = v.parse().ok(),
                "bytes" => bytes = v.parse().ok(),
                _ => {}
            }
        }
        Some(Self {
            format: format?,
            engine: engine?,
            clean: clean?,
            slices: slices?,
            bytes: bytes?,
        })
    }

    fn render(&self) -> String {
        format!(
            "format={}\nengine={}\nclean={}\nslices={}\nbytes={}\n",
            self.format, self.engine, self.clean, self.slices, self.bytes
        )
    }
}

/// The cold-tier store: one fjall database with a single KV-separated
/// keyspace holding `(operator, vnode) → slice bytes`.
///
/// Synchronous API by design — call it from the worker / `spawn_blocking`,
/// never from the compute thread.
pub(crate) struct StateTierStore {
    db: fjall::Database,
    slices: fjall::Keyspace,
    dir: PathBuf,
    /// Logical accounting (key + value bytes; slice count). Persisted into
    /// the marker at clean shutdown so a reused tier reopens with correct
    /// gauges without scanning blobs.
    logical_bytes: AtomicI64,
    logical_slices: AtomicI64,
    metrics: Option<Arc<EngineMetrics>>,
}

impl StateTierStore {
    /// Open (or wipe-and-create) the tier under `dir`.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Storage` when the directory cannot be created or
    /// the fjall database fails to open. A bad *previous* state is not an
    /// error — it wipes and starts empty by contract.
    pub(crate) fn open(
        dir: impl Into<PathBuf>,
        metrics: Option<Arc<EngineMetrics>>,
    ) -> Result<Self, DbError> {
        let dir = dir.into();
        let mut slices: i64 = 0;
        let mut bytes: i64 = 0;

        if dir.exists() {
            match Self::validate_marker(&dir) {
                Ok(marker) => {
                    slices = marker.slices;
                    bytes = marker.bytes;
                }
                Err(reason) => {
                    tracing::warn!(
                        dir = %dir.display(),
                        reason = ?reason,
                        "state tier not reusable — wiping (demoted state will \
                         rehydrate from the last restorable checkpoint)"
                    );
                    if let Some(ref m) = metrics {
                        m.state_tier_wipes_total.inc();
                    }
                    std::fs::remove_dir_all(&dir).map_err(|e| {
                        DbError::Storage(format!("state tier wipe {}: {e}", dir.display()))
                    })?;
                }
            }
        }
        std::fs::create_dir_all(&dir)
            .map_err(|e| DbError::Storage(format!("state tier dir {}: {e}", dir.display())))?;

        // Mark "running" before any writes: a crash from here on is an
        // unclean shutdown and the next open wipes.
        Self::write_marker(&dir, false, slices, bytes)?;

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

        let store = Self {
            db,
            slices: ks,
            dir,
            logical_bytes: AtomicI64::new(bytes),
            logical_slices: AtomicI64::new(slices),
            metrics,
        };
        store.publish_gauges();
        Ok(store)
    }

    fn validate_marker(dir: &Path) -> Result<Marker, WipeReason> {
        let text = std::fs::read_to_string(dir.join(MARKER_FILE))
            .map_err(|_| WipeReason::CorruptMarker)?;
        let marker = Marker::parse(&text).ok_or(WipeReason::CorruptMarker)?;
        if marker.format != FORMAT_VERSION {
            return Err(WipeReason::FormatMismatch);
        }
        if marker.engine != env!("CARGO_PKG_VERSION") {
            return Err(WipeReason::EngineVersionMismatch);
        }
        if !marker.clean {
            return Err(WipeReason::UncleanShutdown);
        }
        Ok(marker)
    }

    fn write_marker(dir: &Path, clean: bool, slices: i64, bytes: i64) -> Result<(), DbError> {
        let marker = Marker {
            format: FORMAT_VERSION,
            engine: env!("CARGO_PKG_VERSION").to_string(),
            clean,
            slices,
            bytes,
        };
        std::fs::write(dir.join(MARKER_FILE), marker.render())
            .map_err(|e| DbError::Storage(format!("state tier marker: {e}")))
    }

    fn key(operator: &str, vnode: u32) -> Vec<u8> {
        // Operator names are SQL identifiers and never contain NUL, so a
        // NUL separator keeps (operator, vnode) prefixes unambiguous.
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
        if let Some(ref m) = self.metrics {
            m.state_tier_demote_total.inc();
        }
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

    /// Drop every slice of one operator (operator removed from the graph).
    pub(crate) fn remove_operator(&self, operator: &str) -> Result<usize, DbError> {
        let mut prefix = Vec::with_capacity(operator.len() + 1);
        prefix.extend_from_slice(operator.as_bytes());
        prefix.push(0);
        let keys: Vec<_> = self
            .slices
            .prefix(&prefix)
            .map(|guard| guard.into_inner().map(|(k, v)| (k, v.len())))
            .collect::<Result<_, _>>()
            .map_err(|e| DbError::Storage(format!("state tier scan: {e}")))?;
        let count = keys.len();
        for (key, vlen) in keys {
            let klen = key.len();
            self.slices
                .remove(key)
                .map_err(|e| DbError::Storage(format!("state tier remove: {e}")))?;
            #[allow(clippy::cast_possible_wrap)]
            self.logical_bytes
                .fetch_sub((klen + vlen) as i64, Ordering::Relaxed);
            self.logical_slices.fetch_sub(1, Ordering::Relaxed);
        }
        self.publish_gauges();
        Ok(count)
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

    /// Clean shutdown: persist fjall, then write the clean marker so the
    /// next open reuses the tier. Anything short of this wipes on reopen.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Storage` if the persist or the marker write fails
    /// (the tier then reopens empty — safe, just slower).
    pub(crate) fn shutdown(&self) -> Result<(), DbError> {
        self.db
            .persist(fjall::PersistMode::SyncAll)
            .map_err(|e| DbError::Storage(format!("state tier persist: {e}")))?;
        Self::write_marker(&self.dir, true, self.logical_slices(), self.logical_bytes())
    }
}

mod worker;
#[allow(unused_imports)] // consumed by the demotion/promotion wiring (and tests)
pub(crate) use worker::{spawn_worker, TierRequest};

#[cfg(test)]
mod tests;
