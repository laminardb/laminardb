//! Reads of materialized views from committed checkpoints. A read resolves one committed cut,
//! pins it against retention, and reads each view from the artifacts that cut lists across all
//! of its participants: changelog views from their vnode frames, append views from their log
//! segments.

mod artifacts;
mod cache;
mod pruning;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use laminar_core::checkpoint::{
    CheckpointManifest, CheckpointScope, CheckpointStore, CommittedCheckpointRef, ViewLog,
};
use laminar_core::checkpoint_decision::CheckpointDecisionStore;

use super::admission::MvLimits;
use super::MvStorageMode;
use crate::checkpoint_coordinator::committed_reads::{
    CommittedCut, CommittedCuts, PinAcquire, ReadPin, ReadPins,
};
use crate::error::DbError;
use cache::ArtifactCache;

/// Attempts at pinning the newest cut while retention retires the ones a read resolved.
const PIN_ATTEMPTS: u32 = 4;
/// Committed indexes an exact read walks back through from the newest cut.
const MAX_EXACT_WALK: usize = 64;

/// Which committed cut a read observes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommittedRead {
    /// The newest committed cut.
    Latest,
    /// The newest committed cut at or after this epoch. The read waits for it until its
    /// deadline.
    AtLeast(u64),
    /// Exactly this epoch, while retention keeps it.
    Exact(u64),
}

/// Why a committed read could not be served.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommittedReadError {
    /// The pipeline has no checkpoint store to read from.
    #[error("committed reads need checkpointing")]
    CheckpointingDisabled,
    /// No committed cut satisfies the request before the read's deadline.
    #[error("{}", not_committed(*requested, *latest))]
    NotCommitted {
        /// The epoch the read asked for; `None` for the newest cut.
        requested: Option<u64>,
        /// The newest committed epoch, if any.
        latest: Option<u64>,
    },
    /// Retention has retired the requested epoch, or it never committed.
    #[error("epoch {requested} is not retained")]
    Retired {
        /// The epoch the read asked for.
        requested: u64,
    },
    /// Every pin slot is held by other reads.
    #[error("too many committed reads hold checkpoints; retry")]
    PinCapacity,
    /// The read did not finish loading before its deadline.
    #[error("the read of epoch {epoch} passed its deadline")]
    DeadlineExceeded {
        /// The epoch being read.
        epoch: u64,
    },
    /// The view was created after the cut, so the cut holds none of its state.
    #[error("view '{view}' has no committed state at epoch {epoch}")]
    ViewNotCommitted {
        /// The view.
        view: String,
        /// The epoch being read.
        epoch: u64,
    },
    /// The statement scans something other than a materialized view.
    #[error("committed reads scan materialized views only; '{table}' is live state")]
    LiveTable {
        /// The scanned table.
        table: String,
    },
}

fn not_committed(requested: Option<u64>, latest: Option<u64>) -> String {
    let latest = latest.map_or_else(|| "none".to_owned(), |epoch| epoch.to_string());
    match requested {
        Some(epoch) => format!("epoch {epoch} has not committed; the newest is {latest}"),
        None => "no checkpoint has committed yet".to_owned(),
    }
}

impl From<CommittedReadError> for DbError {
    fn from(error: CommittedReadError) -> Self {
        Self::CommittedRead(error)
    }
}

/// What a committed read needs of one view, captured under the store read guard.
#[derive(Clone)]
pub(crate) struct ViewShape {
    pub(super) name: String,
    pub(super) schema: SchemaRef,
    pub(super) mode: MvStorageMode,
    pub(super) limits: MvLimits,
}

impl super::MvStore {
    /// What a committed read of `name` needs, while the view is registered.
    pub(crate) fn view_shape(&self, name: &str) -> Option<ViewShape> {
        let entry = self.entries.get(name)?;
        Some(ViewShape {
            name: name.to_owned(),
            schema: Arc::clone(&entry.schema),
            mode: entry.mode.clone(),
            limits: self.limits,
        })
    }
}

/// Reads views from committed checkpoints of this pipeline.
pub(crate) struct CommittedViews {
    store: Arc<dyn CheckpointStore>,
    decisions: Arc<CheckpointDecisionStore>,
    cuts: CommittedCuts,
    pins: Arc<ReadPins>,
    /// Manifests of the cut read last, by checkpoint ID.
    manifests: parking_lot::Mutex<Option<(u64, Arc<[CheckpointManifest]>)>>,
    artifacts: ArtifactCache,
}

/// One committed cut, held against retention until the read finishes or its deadline passes.
pub(crate) struct PinnedCut {
    pub(crate) epoch: u64,
    pub(crate) checkpoint_id: u64,
    /// Earliest participant capture time of the cut, in Unix milliseconds.
    pub(crate) taken_at_ms: u64,
    manifests: Arc<[CheckpointManifest]>,
    /// The frontier each append view's retention follows at this cut.
    log_frontiers: LogFrontiers,
    deadline: Instant,
    _pin: ReadPin,
}

/// Where a cut records the frontier an append view's retention follows.
enum LogFrontiers {
    /// A local cut: the frontier its one participant logged.
    Logged,
    /// A cluster cut: the committed frontier its index carries for every participant.
    Committed(BTreeMap<String, i64>),
}

impl PinnedCut {
    /// The frontier `log` of `view` keeps rows against at this cut.
    pub(crate) fn log_frontier(&self, view: &str, log: &ViewLog) -> Option<i64> {
        match &self.log_frontiers {
            LogFrontiers::Logged => log.frontier,
            LogFrontiers::Committed(frontiers) => frontiers.get(view).copied(),
        }
    }
}

impl CommittedViews {
    pub(crate) fn new(
        store: Arc<dyn CheckpointStore>,
        decisions: Arc<CheckpointDecisionStore>,
        (cuts, pins): (CommittedCuts, Arc<ReadPins>),
        cache_bytes: usize,
    ) -> Self {
        Self {
            store,
            decisions,
            cuts,
            pins,
            manifests: parking_lot::Mutex::new(None),
            artifacts: ArtifactCache::new(cache_bytes),
        }
    }

    fn latest(&self) -> Option<Arc<CommittedCut>> {
        self.cuts.borrow().clone()
    }

    /// Resolve `at` to one committed cut and pin it until `deadline`.
    pub(crate) async fn pin(
        &self,
        at: CommittedRead,
        deadline: Instant,
    ) -> Result<PinnedCut, DbError> {
        let wait_for = match at {
            CommittedRead::Latest => None,
            CommittedRead::AtLeast(epoch) | CommittedRead::Exact(epoch) => Some(epoch),
        };
        let latest = self.await_epoch(wait_for, deadline).await?;
        if let CommittedRead::Exact(epoch) = at {
            if latest.index.epoch != epoch {
                return self.pin_older(&latest, epoch, deadline).await;
            }
        }
        let mut cut = latest;
        for _ in 0..PIN_ATTEMPTS {
            match self.pins.acquire(&cut.reference, deadline) {
                PinAcquire::Acquired(pin) => return self.pinned(&cut, pin, deadline).await,
                PinAcquire::Capacity => return Err(CommittedReadError::PinCapacity.into()),
                PinAcquire::Pruned if matches!(at, CommittedRead::Exact(_)) => break,
                PinAcquire::Pruned => {}
            }
            cut = self.latest().ok_or(CommittedReadError::NotCommitted {
                requested: wait_for,
                latest: None,
            })?;
        }
        Err(CommittedReadError::Retired {
            requested: cut.index.epoch,
        }
        .into())
    }

    /// The newest cut, once one at or after `epoch` has committed.
    async fn await_epoch(
        &self,
        epoch: Option<u64>,
        deadline: Instant,
    ) -> Result<Arc<CommittedCut>, DbError> {
        let not_committed = |latest: Option<&Arc<CommittedCut>>| CommittedReadError::NotCommitted {
            requested: epoch,
            latest: latest.map(|cut| cut.index.epoch),
        };
        let Some(epoch) = epoch else {
            return self.latest().ok_or_else(|| not_committed(None).into());
        };
        let mut cuts = self.cuts.clone();
        let waited = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            cuts.wait_for(|cut| cut.as_ref().is_some_and(|cut| cut.index.epoch >= epoch)),
        )
        .await;
        match waited {
            Ok(Ok(cut)) => cut.clone().ok_or_else(|| not_committed(None).into()),
            Ok(Err(_)) | Err(_) => Err(not_committed(self.latest().as_ref()).into()),
        }
    }

    /// Pin `epoch`, older than the newest cut, by walking back through committed predecessors.
    async fn pin_older(
        &self,
        latest: &CommittedCut,
        epoch: u64,
        deadline: Instant,
    ) -> Result<PinnedCut, DbError> {
        let retired = || DbError::from(CommittedReadError::Retired { requested: epoch });
        let mut predecessor = latest.index.predecessor.clone();
        for _ in 0..MAX_EXACT_WALK {
            let reference = predecessor.filter(|reference| reference.epoch >= epoch);
            let reference = reference.ok_or_else(retired)?;
            if reference.epoch == epoch {
                let PinAcquire::Acquired(pin) = self.pins.acquire(&reference, deadline) else {
                    return Err(retired());
                };
                let index = self.load_index(&reference).await?.ok_or_else(retired)?;
                let cut = CommittedCut::new(index, reference, std::iter::empty());
                return self.pinned(&cut, pin, deadline).await;
            }
            predecessor = self
                .load_index(&reference)
                .await?
                .ok_or_else(retired)?
                .predecessor;
        }
        Err(retired())
    }

    async fn load_index(
        &self,
        reference: &CommittedCheckpointRef,
    ) -> Result<Option<laminar_core::checkpoint::CommittedCheckpointIndex>, DbError> {
        self.decisions
            .load_committed_checkpoint_optional(reference)
            .await
            .map_err(|error| DbError::Checkpoint(format!("load committed checkpoint: {error}")))
    }

    /// Pin the cut whose segments a store read guard captured as checkpoint `checkpoint_id`.
    /// `None` when retention has moved past it; the caller captures the view again.
    pub(super) async fn pin_checkpoint(
        &self,
        checkpoint_id: u64,
        deadline: Instant,
    ) -> Result<Option<PinnedCut>, DbError> {
        let Some(latest) = self.latest() else {
            return Ok(None);
        };
        if latest.index.checkpoint_id == checkpoint_id {
            return match self.pins.acquire(&latest.reference, deadline) {
                PinAcquire::Acquired(pin) => self.pinned(&latest, pin, deadline).await.map(Some),
                PinAcquire::Pruned => Ok(None),
                PinAcquire::Capacity => Err(CommittedReadError::PinCapacity.into()),
            };
        }
        let Some(reference) = latest
            .index
            .predecessor
            .clone()
            .filter(|reference| reference.checkpoint_id == checkpoint_id)
        else {
            return Ok(None);
        };
        let pin = match self.pins.acquire(&reference, deadline) {
            PinAcquire::Acquired(pin) => pin,
            PinAcquire::Pruned => return Ok(None),
            PinAcquire::Capacity => return Err(CommittedReadError::PinCapacity.into()),
        };
        let Some(index) = self.load_index(&reference).await? else {
            return Ok(None);
        };
        let cut = CommittedCut::new(index, reference, std::iter::empty());
        self.pinned(&cut, pin, deadline).await.map(Some)
    }

    async fn pinned(
        &self,
        cut: &CommittedCut,
        pin: ReadPin,
        deadline: Instant,
    ) -> Result<PinnedCut, DbError> {
        let manifests = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.manifests(cut),
        )
        .await
        .map_err(|_| CommittedReadError::DeadlineExceeded {
            epoch: cut.index.epoch,
        })??;
        let taken_at_ms = manifests
            .iter()
            .map(|manifest| manifest.timestamp_ms)
            .min()
            .unwrap_or(cut.taken_at_ms);
        let log_frontiers = match cut.index.scope {
            CheckpointScope::Local => LogFrontiers::Logged,
            CheckpointScope::Cluster => LogFrontiers::Committed(cut.index.view_frontiers.clone()),
        };
        Ok(PinnedCut {
            epoch: cut.index.epoch,
            checkpoint_id: cut.index.checkpoint_id,
            taken_at_ms,
            manifests,
            log_frontiers,
            deadline,
            _pin: pin,
        })
    }

    /// Every participant manifest of the newest committed cut, if one has committed.
    #[cfg(feature = "cluster")]
    pub(crate) async fn latest_manifests(
        &self,
    ) -> Result<Option<Arc<[CheckpointManifest]>>, DbError> {
        match self.latest() {
            Some(cut) => self.manifests(&cut).await.map(Some),
            None => Ok(None),
        }
    }

    /// Every participant manifest of `cut`, verified against its committed index.
    async fn manifests(&self, cut: &CommittedCut) -> Result<Arc<[CheckpointManifest]>, DbError> {
        let checkpoint_id = cut.index.checkpoint_id;
        if let Some((_, manifests)) = self
            .manifests
            .lock()
            .as_ref()
            .filter(|(cached, _)| *cached == checkpoint_id)
        {
            return Ok(Arc::clone(manifests));
        }
        let manifests: Arc<[CheckpointManifest]> =
            crate::checkpoint_coordinator::load_index_manifests(self.store.as_ref(), &cut.index)
                .await?
                .into();
        *self.manifests.lock() = Some((checkpoint_id, Arc::clone(&manifests)));
        Ok(manifests)
    }
}
