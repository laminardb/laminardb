//! Durable cluster-wide 2PC decision marker.
//!
//! The leader's `Commit` barrier announcement on gossip KV is ephemeral
//! — a leader that crashes between "announce Commit" and "commit own
//! sinks" leaves surviving followers that already observed the
//! announcement in a different state than the new leader, which has no
//! record the announcement ever went out and would otherwise pick
//! `Abort` as the safe default. External sinks (e.g. Kafka
//! transactional producers) then end up half-committed.
//!
//! [`CheckpointDecisionStore`] is the durable pivot of cluster 2PC: the
//! leader writes [`Decision::Committed`] to object storage via a
//! CAS-create **before** announcing `Commit`. After that write, any
//! surviving node can recover the cluster's vote — it's stamped on
//! shared storage. On new-leader election or per-node restart, the
//! coordinator reads this store for its Pending epochs and drives
//! local sinks to the recorded verdict. Absence of a marker means
//! "commit point never reached" — safe to abort.
//!
//! The store is keyed on `epoch` (monotonic per cluster), not
//! `checkpoint_id`, because recovery loads manifests by epoch and the
//! decision needs to match. CAS-create (`PutMode::Create`) ensures the
//! first writer wins; concurrent would-be leaders adopt the persisted
//! verdict rather than contending.
//!
//! Flink and Arroyo both use this pattern; `LaminarDB`'s contribution
//! is that the decision store is independent of any connector or sink
//! — a single object-store key namespace per cluster.

use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

/// Per-epoch 2PC verdict. See module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Decision {
    /// The cluster committed this epoch. All nodes must drive their
    /// sinks to commit (idempotent on retry).
    Committed,
    /// The cluster aborted this epoch. All nodes must roll back.
    ///
    /// Writing `Aborted` is optional for correctness — absence of any
    /// marker is equivalent — but useful for debuggability and for
    /// racing leaders who want to observe "the cluster already gave
    /// up here" rather than re-compute from context.
    Aborted,
}

/// Outcome of [`CheckpointDecisionStore::record`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOutcome {
    /// Our write won the CAS. The decision we passed in is now
    /// durably stored.
    Recorded,
    /// Another writer got here first; this is the persisted verdict
    /// we must adopt (may or may not match what we proposed).
    AlreadyRecorded(Decision),
}

/// Errors raised by [`CheckpointDecisionStore`] operations.
#[derive(Debug, thiserror::Error)]
pub enum DecisionError {
    /// Underlying object-store I/O failure.
    #[error("object store I/O: {0}")]
    Io(String),
    /// JSON de/serialization failure.
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
}

/// Durable store for cluster 2PC decisions, one object per epoch.
///
/// Layout: `checkpoint-decisions/epoch=N/decision.json`. Writes use
/// `PutMode::Create` so the first writer wins; subsequent callers
/// observing the conflict read back the persisted verdict.
pub struct CheckpointDecisionStore {
    store: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for CheckpointDecisionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointDecisionStore")
            .finish_non_exhaustive()
    }
}

#[derive(Serialize, Deserialize)]
struct DecisionFile {
    decision: Decision,
}

impl CheckpointDecisionStore {
    /// Wrap an existing object store.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    fn path(epoch: u64) -> OsPath {
        OsPath::from(format!("checkpoint-decisions/epoch={epoch}/decision.json"))
    }

    /// CAS-create the decision for `epoch`. On success the verdict we
    /// passed in is durably stored. On CAS conflict, returns the
    /// already-persisted verdict — caller **must** adopt it (treat a
    /// returned `AlreadyRecorded(Aborted)` as a binding abort even if
    /// the caller proposed `Committed`, and vice versa).
    ///
    /// # Errors
    ///
    /// `DecisionError::Io` on object-store I/O other than CAS
    /// conflict (the conflict is reported as `Ok(AlreadyRecorded)`);
    /// `DecisionError::Json` if the existing object is malformed.
    pub async fn record(
        &self,
        epoch: u64,
        decision: Decision,
    ) -> Result<RecordOutcome, DecisionError> {
        let path = Self::path(epoch);
        let file = DecisionFile { decision };
        let body = serde_json::to_vec(&file)?;
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(&path, PutPayload::from(Bytes::from(body)), opts)
            .await
        {
            Ok(_) => Ok(RecordOutcome::Recorded),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self
                    .load(epoch)
                    .await?
                    .ok_or_else(|| DecisionError::Io(
                        "CAS reported AlreadyExists but subsequent load returned None"
                            .into(),
                    ))?;
                Ok(RecordOutcome::AlreadyRecorded(existing))
            }
            Err(e) => Err(DecisionError::Io(e.to_string())),
        }
    }

    /// Load the recorded decision for `epoch`. `Ok(None)` means no
    /// decision was ever recorded — safe to treat as Abort in recovery.
    ///
    /// # Errors
    ///
    /// `DecisionError::Io` on object-store I/O; `DecisionError::Json`
    /// on malformed JSON body.
    pub async fn load(&self, epoch: u64) -> Result<Option<Decision>, DecisionError> {
        let path = Self::path(epoch);
        match self.store.get(&path).await {
            Ok(res) => {
                let bytes = res
                    .bytes()
                    .await
                    .map_err(|e| DecisionError::Io(e.to_string()))?;
                let file: DecisionFile = serde_json::from_slice(&bytes)?;
                Ok(Some(file.decision))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(DecisionError::Io(e.to_string())),
        }
    }

    /// Delete every decision for an epoch strictly less than `before`.
    /// Called by the checkpoint coordinator after the corresponding
    /// state-backend prune horizon advances so the bucket doesn't
    /// accumulate one marker per committed epoch forever.
    ///
    /// # Errors
    ///
    /// `DecisionError::Io` on object-store I/O.
    pub async fn prune_before(&self, before: u64) -> Result<(), DecisionError> {
        let root = OsPath::from("checkpoint-decisions/");
        let mut entries = self.store.list(Some(&root));
        let mut victims: Vec<OsPath> = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|e| DecisionError::Io(e.to_string()))?;
            let loc = entry.location.as_ref();
            // Match `checkpoint-decisions/epoch=N/decision.json`; skip
            // anything else so an unrelated sibling object doesn't get
            // clobbered if the bucket is reused.
            let rest = loc.strip_prefix("checkpoint-decisions/").unwrap_or("");
            let Some(seg) = rest.split('/').next() else {
                continue;
            };
            let Some(n) = seg.strip_prefix("epoch=") else {
                continue;
            };
            let Ok(epoch) = n.parse::<u64>() else {
                continue;
            };
            if epoch < before {
                victims.push(entry.location);
            }
        }
        for victim in victims {
            match self.store.delete(&victim).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => tracing::warn!(
                    error = %e,
                    "checkpoint decision prune: delete failed",
                ),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use tempfile::tempdir;

    fn make_store(dir: &std::path::Path) -> CheckpointDecisionStore {
        let fs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
        CheckpointDecisionStore::new(fs)
    }

    #[tokio::test]
    async fn load_missing_returns_none() {
        let dir = tempdir().unwrap();
        let s = make_store(dir.path());
        assert!(s.load(1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn record_then_load_roundtrip() {
        let dir = tempdir().unwrap();
        let s = make_store(dir.path());
        assert_eq!(
            s.record(5, Decision::Committed).await.unwrap(),
            RecordOutcome::Recorded,
        );
        assert_eq!(s.load(5).await.unwrap(), Some(Decision::Committed));
    }

    #[tokio::test]
    async fn second_writer_reads_first_verdict() {
        let dir = tempdir().unwrap();
        let s = make_store(dir.path());
        assert_eq!(
            s.record(7, Decision::Committed).await.unwrap(),
            RecordOutcome::Recorded,
        );
        // Second writer proposing the OPPOSITE verdict must read back
        // the persisted one — otherwise two leaders could disagree.
        assert_eq!(
            s.record(7, Decision::Aborted).await.unwrap(),
            RecordOutcome::AlreadyRecorded(Decision::Committed),
        );
        assert_eq!(s.load(7).await.unwrap(), Some(Decision::Committed));
    }

    #[tokio::test]
    async fn different_epochs_are_independent() {
        let dir = tempdir().unwrap();
        let s = make_store(dir.path());
        s.record(1, Decision::Committed).await.unwrap();
        s.record(2, Decision::Aborted).await.unwrap();
        assert_eq!(s.load(1).await.unwrap(), Some(Decision::Committed));
        assert_eq!(s.load(2).await.unwrap(), Some(Decision::Aborted));
    }

    #[tokio::test]
    async fn prune_drops_only_older_epochs() {
        let dir = tempdir().unwrap();
        let s = make_store(dir.path());
        for e in 1..=5u64 {
            s.record(e, Decision::Committed).await.unwrap();
        }
        s.prune_before(4).await.unwrap();
        for e in 1..=3 {
            assert!(
                s.load(e).await.unwrap().is_none(),
                "epoch {e} should be pruned",
            );
        }
        for e in 4..=5 {
            assert_eq!(
                s.load(e).await.unwrap(),
                Some(Decision::Committed),
                "epoch {e} should be retained",
            );
        }
    }

    #[tokio::test]
    async fn prune_on_empty_store_is_noop() {
        let dir = tempdir().unwrap();
        let s = make_store(dir.path());
        s.prune_before(100).await.unwrap();
    }

    #[tokio::test]
    async fn prune_ignores_unrelated_objects() {
        // If someone reuses the bucket, prune must not touch siblings.
        let dir = tempdir().unwrap();
        let fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        fs.put(
            &OsPath::from("other-thing/data.bin"),
            PutPayload::from(Bytes::from_static(b"keep me")),
        )
        .await
        .unwrap();
        let s = CheckpointDecisionStore::new(Arc::clone(&fs));
        s.record(1, Decision::Committed).await.unwrap();
        s.prune_before(10).await.unwrap();
        assert!(
            fs.head(&OsPath::from("other-thing/data.bin")).await.is_ok(),
            "unrelated sibling was clobbered by prune",
        );
    }

    #[tokio::test]
    async fn object_safe_behind_arc() {
        let dir = tempdir().unwrap();
        let _: Arc<CheckpointDecisionStore> = Arc::new(make_store(dir.path()));
    }
}
