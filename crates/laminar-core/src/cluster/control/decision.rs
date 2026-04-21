//! Durable cluster-wide commit marker for checkpoint 2PC.
//!
//! The leader's `Commit` barrier announcement on gossip KV is
//! ephemeral. A leader that crashes between "announce Commit" and
//! "commit own sinks" leaves surviving followers in a different state
//! than a new leader, which would otherwise pick `Abort` as the safe
//! default. We record a durable marker on shared storage _before_ the
//! announcement so recovery can recover the cluster vote.

use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use tokio_stream::StreamExt;

/// Per-epoch commit marker store. Presence means committed; absence
/// means the leader never reached the commit point (safe to abort).
pub struct CheckpointDecisionStore {
    store: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for CheckpointDecisionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointDecisionStore")
            .finish_non_exhaustive()
    }
}

/// Errors raised by [`CheckpointDecisionStore`] operations.
#[derive(Debug, thiserror::Error)]
pub enum DecisionError {
    /// Underlying object-store I/O failure.
    #[error("object store I/O: {0}")]
    Io(String),
}

impl CheckpointDecisionStore {
    /// Wrap an existing object store.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    fn path(epoch: u64) -> OsPath {
        OsPath::from(format!("checkpoint-decisions/epoch={epoch}/commit"))
    }

    /// CAS-create the commit marker for `epoch`. `Ok(true)` means our
    /// write landed; `Ok(false)` means someone else recorded first
    /// (idempotent — retries after commit are cheap no-ops).
    ///
    /// # Errors
    /// Object-store I/O.
    pub async fn record_committed(&self, epoch: u64) -> Result<bool, DecisionError> {
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(
                &Self::path(epoch),
                PutPayload::from(Bytes::from_static(b"")),
                opts,
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(e) => Err(DecisionError::Io(e.to_string())),
        }
    }

    /// True iff a commit marker exists for `epoch`.
    ///
    /// # Errors
    /// Object-store I/O.
    pub async fn is_committed(&self, epoch: u64) -> Result<bool, DecisionError> {
        match self.store.head(&Self::path(epoch)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(DecisionError::Io(e.to_string())),
        }
    }

    /// Delete commit markers for `epoch < before`. Called by the
    /// checkpoint coordinator after its state-backend prune so
    /// markers don't accumulate one-per-checkpoint forever.
    ///
    /// # Errors
    /// Object-store I/O.
    pub async fn prune_before(&self, before: u64) -> Result<(), DecisionError> {
        if before == 0 {
            return Ok(());
        }
        let root = OsPath::from("checkpoint-decisions/");
        let mut entries = self.store.list(Some(&root));
        let mut victims: Vec<OsPath> = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|e| DecisionError::Io(e.to_string()))?;
            let loc = entry.location.as_ref();
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
                Err(e) => tracing::warn!(error = %e, "decision prune: delete failed"),
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

    fn store_in(dir: &std::path::Path) -> CheckpointDecisionStore {
        let fs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
        CheckpointDecisionStore::new(fs)
    }

    #[tokio::test]
    async fn absent_before_recorded() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        assert!(!s.is_committed(1).await.unwrap());
    }

    #[tokio::test]
    async fn record_then_read() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        assert!(s.record_committed(5).await.unwrap());
        assert!(s.is_committed(5).await.unwrap());
    }

    #[tokio::test]
    async fn second_record_is_noop() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        assert!(s.record_committed(7).await.unwrap());
        assert!(!s.record_committed(7).await.unwrap());
        assert!(s.is_committed(7).await.unwrap());
    }

    #[tokio::test]
    async fn epochs_are_independent() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        s.record_committed(1).await.unwrap();
        assert!(s.is_committed(1).await.unwrap());
        assert!(!s.is_committed(2).await.unwrap());
    }

    #[tokio::test]
    async fn prune_drops_older() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        for e in 1..=5 {
            s.record_committed(e).await.unwrap();
        }
        s.prune_before(4).await.unwrap();
        for e in 1..=3 {
            assert!(
                !s.is_committed(e).await.unwrap(),
                "epoch {e} should be pruned"
            );
        }
        for e in 4..=5 {
            assert!(s.is_committed(e).await.unwrap(), "epoch {e} should remain");
        }
    }
}
