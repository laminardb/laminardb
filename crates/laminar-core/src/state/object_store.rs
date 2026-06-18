//! [`ObjectStoreBackend`] — durable partial-state storage backed by any
//! `object_store` implementation (S3, GCS, Azure, `LocalFileSystem`).
//!
//! `epoch_complete` performs a CAS-commit: if every vnode's `partial.bin`
//! and every required commit descriptor is present, `put(_COMMIT, Create)`
//! seals the epoch. The `_COMMIT` marker is the durability boundary the
//! checkpoint coordinator consults before releasing sinks.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};

use super::backend::{StateBackend, StateBackendError};

/// Every Nth prune does a full listing instead of the incremental window.
const PRUNE_FULL_SCAN_EVERY: u64 = 32;

/// Object-store-backed [`StateBackend`].
pub struct ObjectStoreBackend {
    store: Arc<dyn ObjectStore>,
    instance_id: String,
    /// Pre-encoded audit body for the `_COMMIT` CAS — derived once
    /// from `instance_id` to avoid cloning a String into `Bytes` on
    /// every commit attempt.
    committer_bytes: Bytes,
    vnode_capacity: u32,
    /// Highest prune horizon already covered cleanly: later prunes list only
    /// `epoch={N}/` prefixes in `[latest_pruned_epoch, before)` instead of the
    /// whole store. `0` = no baseline yet; the first prune does one full
    /// listing, then bounds every subsequent one.
    latest_pruned_epoch: AtomicU64,
    /// Prune-call counter driving the periodic full-scan re-baseline, which
    /// bounds how long a straggler write below the cursor can leak.
    prune_passes: AtomicU64,
    /// Authoritative vnode-assignment version known to this backend.
    /// Split-brain fence: [`write_partial`](Self::write_partial) rejects
    /// any caller whose `assignment_version` is strictly less than this
    /// value. Updated via [`set_authoritative_version`](Self::set_authoritative_version)
    /// whenever the host sees a newer `AssignmentSnapshot` rotate in.
    ///
    /// Default is `0`, which disables the fence — unconfigured
    /// callers (most single-instance paths) are accepted unchanged.
    authoritative_version: Arc<AtomicU64>,
}

impl std::fmt::Debug for ObjectStoreBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreBackend")
            .field("instance_id", &self.instance_id)
            .field("vnode_capacity", &self.vnode_capacity)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreBackend {
    /// Wrap an existing [`ObjectStore`].
    #[must_use]
    pub fn new(
        store: Arc<dyn ObjectStore>,
        instance_id: impl Into<String>,
        vnode_capacity: u32,
    ) -> Self {
        let instance_id = instance_id.into();
        let committer_bytes = Bytes::from(instance_id.clone().into_bytes());
        Self {
            store,
            instance_id,
            committer_bytes,
            vnode_capacity,
            latest_pruned_epoch: AtomicU64::new(0),
            prune_passes: AtomicU64::new(0),
            authoritative_version: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Vnode range this backend is configured for.
    #[must_use]
    pub fn vnode_capacity(&self) -> u32 {
        self.vnode_capacity
    }

    /// Shared handle to the authoritative version counter. Callers that
    /// want to bump several objects (e.g. backend plus a future metric)
    /// from a single owner can clone this handle instead of relaying
    /// through [`StateBackend::set_authoritative_version`].
    #[must_use]
    pub fn authoritative_version_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.authoritative_version)
    }

    fn check_vnode(&self, v: u32) -> Result<(), StateBackendError> {
        if v >= self.vnode_capacity {
            Err(StateBackendError::Io(format!(
                "vnode {v} out of range (capacity {})",
                self.vnode_capacity
            )))
        } else {
            Ok(())
        }
    }

    fn partial_path(epoch: u64, vnode: u32) -> OsPath {
        OsPath::from(format!("epoch={epoch}/vnode={vnode}/partial.bin"))
    }

    fn commit_path(epoch: u64) -> OsPath {
        OsPath::from(format!("epoch={epoch}/_COMMIT"))
    }

    fn descriptor_path(epoch: u64, key: &str) -> OsPath {
        OsPath::from(format!("epoch={epoch}/commit/{key}"))
    }

    /// Parse `N` from a location whose first path segment is `epoch=N`.
    /// `None` for any sibling object that doesn't follow the layout.
    /// `str::split` always yields at least one segment.
    fn epoch_of_first_segment(loc: &str) -> Option<u64> {
        let first = loc.split('/').next().unwrap_or("");
        first.strip_prefix("epoch=")?.parse::<u64>().ok()
    }
}

#[async_trait]
impl StateBackend for ObjectStoreBackend {
    async fn write_partial(
        &self,
        vnode: u32,
        epoch: u64,
        assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.check_vnode(vnode)?;
        // Split-brain fence. `authoritative_version == 0` means
        // "unconfigured" — accept every write (matches the legacy
        // single-instance behavior). Non-zero authoritative means we
        // know of a specific assignment generation; writes stamped with
        // an older generation are rejected.
        let authoritative = self.authoritative_version.load(Ordering::Acquire);
        if authoritative > 0 && assignment_version < authoritative {
            return Err(StateBackendError::StaleVersion {
                caller: assignment_version,
                authoritative,
            });
        }
        let path = Self::partial_path(epoch, vnode);
        self.store
            .put(&path, PutPayload::from(bytes))
            .await
            .map_err(|e| StateBackendError::Io(e.to_string()))?;
        Ok(())
    }

    async fn read_partial(
        &self,
        vnode: u32,
        epoch: u64,
    ) -> Result<Option<Bytes>, StateBackendError> {
        self.check_vnode(vnode)?;
        let path = Self::partial_path(epoch, vnode);
        match self.store.get(&path).await {
            Ok(res) => {
                let b = res
                    .bytes()
                    .await
                    .map_err(|e| StateBackendError::Io(e.to_string()))?;
                Ok(Some(b))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(StateBackendError::Io(e.to_string())),
        }
    }

    async fn write_commit_descriptor(
        &self,
        epoch: u64,
        key: &str,
        assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        let authoritative = self.authoritative_version.load(Ordering::Acquire);
        if authoritative > 0 && assignment_version < authoritative {
            return Err(StateBackendError::StaleVersion {
                caller: assignment_version,
                authoritative,
            });
        }
        self.store
            .put(&Self::descriptor_path(epoch, key), PutPayload::from(bytes))
            .await
            .map_err(|e| StateBackendError::Io(e.to_string()))?;
        Ok(())
    }

    async fn read_commit_descriptors(
        &self,
        epoch: u64,
    ) -> Result<Vec<(String, Bytes)>, StateBackendError> {
        use tokio_stream::StreamExt;

        let prefix_str = format!("epoch={epoch}/commit/");
        let mut entries = self.store.list(Some(&OsPath::from(prefix_str.clone())));
        let mut out = Vec::new();
        while let Some(entry) = entries.next().await {
            let loc = entry
                .map_err(|e| StateBackendError::Io(e.to_string()))?
                .location;
            let key = loc
                .as_ref()
                .strip_prefix(&prefix_str)
                .unwrap_or(loc.as_ref())
                .to_string();
            let bytes = self
                .store
                .get(&loc)
                .await
                .map_err(|e| StateBackendError::Io(e.to_string()))?
                .bytes()
                .await
                .map_err(|e| StateBackendError::Io(e.to_string()))?;
            out.push((key, bytes));
        }
        Ok(out)
    }

    async fn epoch_complete(
        &self,
        epoch: u64,
        vnodes: &[u32],
        required_descriptors: &[String],
    ) -> Result<bool, StateBackendError> {
        use rustc_hash::FxHashSet;
        use tokio_stream::StreamExt;

        let commit = Self::commit_path(epoch);
        // Fast path: a marker already exists. Previously we returned
        // `Ok(true)` blindly — that swallowed split-brain (two leaders
        // racing, the loser silently agreed it had committed). Now we
        // read the audit body and reject if the committer isn't us.
        match self.store.head(&commit).await {
            Ok(_) => return self.verify_commit_marker(&commit).await,
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(StateBackendError::Io(e.to_string())),
        }

        for &v in vnodes {
            self.check_vnode(v)?;
        }

        // List the epoch prefix once, then check every required vnode's
        // partial is present — one round trip instead of O(vnodes) HEADs.
        let prefix = OsPath::from(format!("epoch={epoch}/"));
        let mut entries = self.store.list(Some(&prefix));
        let mut found_paths: FxHashSet<OsPath> = FxHashSet::default();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|e| StateBackendError::Io(e.to_string()))?;
            found_paths.insert(entry.location);
        }

        for &v in vnodes {
            let path = Self::partial_path(epoch, v);
            if !found_paths.contains(&path) {
                return Ok(false);
            }
        }
        // Commit descriptors live under `epoch=N/commit/` — already in this listing.
        for key in required_descriptors {
            if !found_paths.contains(&Self::descriptor_path(epoch, key)) {
                return Ok(false);
            }
        }

        // CAS the commit marker; payload is the committer's id for audit.
        let payload = PutPayload::from(self.committer_bytes.clone());
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self.store.put_opts(&commit, payload, opts).await {
            Ok(_) => Ok(true),
            // AlreadyExists means a peer raced us to the CAS. Don't
            // silently agree — verify who actually wrote the marker
            // so a stale leader doesn't keep driving the commit phase.
            Err(object_store::Error::AlreadyExists { .. }) => {
                self.verify_commit_marker(&commit).await
            }
            Err(e) => Err(StateBackendError::Io(e.to_string())),
        }
    }

    async fn sealed_epochs(&self, after: u64) -> Result<Vec<u64>, StateBackendError> {
        use tokio_stream::StreamExt;

        let mut entries = self.store.list(None);
        let mut out = Vec::new();
        while let Some(entry) = entries.next().await {
            let loc = entry
                .map_err(|e| StateBackendError::Io(e.to_string()))?
                .location;
            if !loc.as_ref().ends_with("/_COMMIT") {
                continue;
            }
            if let Some(epoch) = Self::epoch_of_first_segment(loc.as_ref()) {
                if epoch > after {
                    out.push(epoch);
                }
            }
        }
        out.sort_unstable();
        Ok(out)
    }

    async fn prune_before(&self, before: u64) -> Result<(), StateBackendError> {
        use futures::stream::{self, StreamExt};

        let pass = self.prune_passes.fetch_add(1, Ordering::AcqRel);
        let start = if pass.is_multiple_of(PRUNE_FULL_SCAN_EVERY) {
            0
        } else {
            self.latest_pruned_epoch.load(Ordering::Acquire)
        };

        let mut victims: Vec<OsPath> = Vec::new();
        if start == 0 {
            // No baseline yet: one full listing. `epoch={epoch}/...` has a
            // dynamic first segment and the `object_store` API matches whole
            // segments, so a bare `epoch=` prefix would match nothing.
            let mut entries = self.store.list(None);
            while let Some(entry) = entries.next().await {
                let entry = entry.map_err(|e| StateBackendError::Io(e.to_string()))?;
                let Some(epoch) = Self::epoch_of_first_segment(entry.location.as_ref()) else {
                    continue;
                };
                if epoch < before {
                    victims.push(entry.location);
                }
            }
        } else {
            // Only epochs in `[start, before)` can still hold objects, and
            // `epoch={N}/` is an exact segment, so per-epoch listings cost
            // O(epochs-since-last-prune × vnodes) instead of O(store).
            for epoch in start..before {
                let prefix = OsPath::from(format!("epoch={epoch}/"));
                let mut entries = self.store.list(Some(&prefix));
                while let Some(entry) = entries.next().await {
                    let entry = entry.map_err(|e| StateBackendError::Io(e.to_string()))?;
                    victims.push(entry.location);
                }
            }
        }

        // `delete_stream` coalesces into bulk-delete API calls where the store
        // supports them (S3 `DeleteObjects`); a missing object is a no-op.
        let mut delete_failed = false;
        if !victims.is_empty() {
            let locations =
                stream::iter(victims.into_iter().map(Ok::<OsPath, object_store::Error>)).boxed();
            let mut deletes = self.store.delete_stream(locations);
            while let Some(res) = deletes.next().await {
                match res {
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => {
                        delete_failed = true;
                        tracing::warn!(error = %e, "state backend prune: delete failed");
                    }
                }
            }
        }

        // Advance the cursor only on a clean pass: a failed delete must stay
        // above it so the next prune re-lists that epoch and retries, instead
        // of orphaning the object until a process restart. `fetch_max` keeps
        // the cursor monotonic under concurrent prunes.
        if !delete_failed {
            self.latest_pruned_epoch.fetch_max(before, Ordering::AcqRel);
        }
        Ok(())
    }

    async fn latest_committed_epoch(&self) -> Result<Option<u64>, StateBackendError> {
        use tokio_stream::StreamExt;

        // Same listing constraint as `prune_before`: the first path
        // segment (`epoch=N`) is dynamic, so we scan the whole store and
        // filter for the `_COMMIT` markers a sealed epoch leaves behind.
        let mut entries = self.store.list(None);
        let mut highest: Option<u64> = None;
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|e| StateBackendError::Io(e.to_string()))?;
            let loc = entry.location.as_ref();
            if !loc.ends_with("/_COMMIT") {
                continue;
            }
            if let Some(epoch) = Self::epoch_of_first_segment(loc) {
                highest = Some(highest.map_or(epoch, |h| h.max(epoch)));
            }
        }
        Ok(highest)
    }

    fn set_authoritative_version(&self, version: u64) {
        // CAS loop avoids lowering the version on a late call.
        let mut cur = self.authoritative_version.load(Ordering::Acquire);
        while version > cur {
            match self.authoritative_version.compare_exchange(
                cur,
                version,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => cur = observed,
            }
        }
    }

    fn authoritative_version(&self) -> u64 {
        self.authoritative_version.load(Ordering::Acquire)
    }
}

impl ObjectStoreBackend {
    /// Read the epoch's `_COMMIT` marker and compare its audit body
    /// against this backend's `instance_id`. Match → `Ok(true)` (we
    /// committed, a retry or observation is fine). Mismatch →
    /// [`StateBackendError::SplitBrainCommit`] so the caller aborts
    /// rather than double-committing downstream.
    async fn verify_commit_marker(&self, commit: &OsPath) -> Result<bool, StateBackendError> {
        let res = self
            .store
            .get(commit)
            .await
            .map_err(|e| StateBackendError::Io(e.to_string()))?;
        let bytes = res
            .bytes()
            .await
            .map_err(|e| StateBackendError::Io(e.to_string()))?;
        let committer = std::str::from_utf8(&bytes).map_err(|e| {
            StateBackendError::Serialization(format!("commit marker not utf8: {e}"))
        })?;
        if committer == self.instance_id.as_str() {
            Ok(true)
        } else {
            Err(StateBackendError::SplitBrainCommit {
                committer: committer.to_string(),
                self_id: self.instance_id.clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use tempfile::tempdir;

    fn make_store(dir: &std::path::Path) -> Arc<dyn ObjectStore> {
        Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap())
    }

    #[tokio::test]
    async fn write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        backend
            .write_partial(0, 1, 0, Bytes::from_static(b"hello"))
            .await
            .unwrap();
        let got = backend.read_partial(0, 1).await.unwrap().unwrap();
        assert_eq!(&got[..], b"hello");
    }

    #[tokio::test]
    async fn epoch_complete_cas_commit() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        let vnodes = [0u32, 1, 2];

        assert!(!backend.epoch_complete(1, &vnodes, &[]).await.unwrap());
        for v in &vnodes {
            backend
                .write_partial(*v, 1, 0, Bytes::from_static(b"y"))
                .await
                .unwrap();
        }
        assert!(backend.epoch_complete(1, &vnodes, &[]).await.unwrap());
        // Idempotent — same committer id in the audit body.
        assert!(backend.epoch_complete(1, &vnodes, &[]).await.unwrap());
    }

    #[tokio::test]
    async fn epoch_complete_requires_commit_descriptors() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        let vnodes = [0u32];
        let key = "node=node-0/sink=ice";
        let need = [key.to_string()];

        backend
            .write_partial(0, 1, 0, Bytes::from_static(b"s"))
            .await
            .unwrap();
        // Partial present but the descriptor is missing → epoch not sealed.
        assert!(!backend.epoch_complete(1, &vnodes, &need).await.unwrap());

        backend
            .write_commit_descriptor(1, key, 0, Bytes::from_static(b"df"))
            .await
            .unwrap();
        assert!(backend.epoch_complete(1, &vnodes, &need).await.unwrap());

        let descs = backend.read_commit_descriptors(1).await.unwrap();
        assert_eq!(descs, vec![(key.to_string(), Bytes::from_static(b"df"))]);
    }

    /// Split-brain commit protection. Previously the CAS-create's
    /// `AlreadyExists` branch was folded into the success branch, so a
    /// stale leader racing a fresh one would happily agree it had also
    /// committed the epoch. Now the loser reads the marker, sees a
    /// mismatched audit body, and fails loud.
    #[tokio::test]
    async fn epoch_complete_detects_split_brain_committer() {
        let dir = tempdir().unwrap();
        let store = make_store(dir.path());
        let winner = ObjectStoreBackend::new(Arc::clone(&store), "winner", 4);
        let loser = ObjectStoreBackend::new(Arc::clone(&store), "loser", 4);

        let vnodes = [0u32, 1];
        // Both "nodes" wrote partials for the epoch.
        for v in &vnodes {
            winner
                .write_partial(*v, 7, 0, Bytes::from_static(b"w"))
                .await
                .unwrap();
        }

        // Winner CAS-creates the commit marker first.
        assert!(winner.epoch_complete(7, &vnodes, &[]).await.unwrap());

        // Loser finds the marker already there (HEAD fast-path) and
        // must NOT agree it committed — that's the split-brain case.
        let err = loser.epoch_complete(7, &vnodes, &[]).await.unwrap_err();
        match err {
            StateBackendError::SplitBrainCommit { committer, self_id } => {
                assert_eq!(committer, "winner");
                assert_eq!(self_id, "loser");
            }
            other => panic!("expected SplitBrainCommit, got {other:?}"),
        }

        // And the winner's repeated call is still idempotent Ok(true).
        assert!(winner.epoch_complete(7, &vnodes, &[]).await.unwrap());
    }

    /// Same contract on the CAS-loser path: if the marker doesn't exist
    /// at HEAD time but a peer sneaks in between our vnode-presence
    /// check and our own PUT, our `put_opts` fails with `AlreadyExists`.
    /// That branch must also compare committers, not silently succeed.
    #[tokio::test]
    async fn epoch_complete_detects_split_brain_on_cas_loser_path() {
        let dir = tempdir().unwrap();
        let store = make_store(dir.path());
        let winner = ObjectStoreBackend::new(Arc::clone(&store), "winner", 4);
        let loser = ObjectStoreBackend::new(Arc::clone(&store), "loser", 4);

        let vnodes = [0u32, 1];
        for v in &vnodes {
            winner
                .write_partial(*v, 3, 0, Bytes::from_static(b"w"))
                .await
                .unwrap();
        }
        // Manually pre-seed the commit marker under "winner" to
        // simulate the TOCTOU race deterministically — the loser's
        // put_opts will hit AlreadyExists on its own PUT attempt.
        let commit = ObjectStoreBackend::commit_path(3);
        store
            .put(&commit, PutPayload::from(Bytes::from_static(b"winner")))
            .await
            .unwrap();

        let err = loser.epoch_complete(3, &vnodes, &[]).await.unwrap_err();
        assert!(matches!(
            err,
            StateBackendError::SplitBrainCommit { ref committer, .. }
                if committer == "winner"
        ));
    }

    #[tokio::test]
    async fn stale_version_rejected() {
        // Force two "nodes" (backend instances wrapping the same store)
        // to claim the same vnode at different generations. The stale
        // writer must be rejected.
        let dir = tempdir().unwrap();
        let store = make_store(dir.path());
        let stale = ObjectStoreBackend::new(Arc::clone(&store), "node-stale", 4);
        let fresh = ObjectStoreBackend::new(Arc::clone(&store), "node-fresh", 4);

        // Fresh learns about a new assignment generation — e.g. a new
        // snapshot rotated in after a leader election.
        fresh.set_authoritative_version(2);

        // Fresh writes at the current version: accepted.
        fresh
            .write_partial(0, 1, 2, Bytes::from_static(b"fresh"))
            .await
            .unwrap();

        // Stale tries to write at version 1 — but only IF it's also
        // learned of the rotation. Model that by promoting stale's
        // view too; the check is intra-backend here because the
        // durable version-broadcast channel is out of scope for this test.
        stale.set_authoritative_version(2);
        let err = stale
            .write_partial(0, 1, 1, Bytes::from_static(b"stale"))
            .await
            .unwrap_err();
        match err {
            StateBackendError::StaleVersion {
                caller,
                authoritative,
            } => {
                assert_eq!(caller, 1);
                assert_eq!(authoritative, 2);
            }
            other => panic!("expected StaleVersion, got {other:?}"),
        }

        // Fence-disabled backend (authoritative stays at 0) accepts
        // any version — preserves legacy single-instance behavior.
        let unfenced = ObjectStoreBackend::new(Arc::clone(&store), "node-unfenced", 4);
        unfenced
            .write_partial(1, 1, 0, Bytes::from_static(b"ok"))
            .await
            .unwrap();
    }

    #[test]
    fn authoritative_version_is_monotonic() {
        let dir = tempdir().unwrap();
        let b = ObjectStoreBackend::new(make_store(dir.path()), "node", 2);
        assert_eq!(b.authoritative_version(), 0);
        b.set_authoritative_version(3);
        assert_eq!(b.authoritative_version(), 3);
        // Attempts to lower the version are no-ops.
        b.set_authoritative_version(1);
        assert_eq!(b.authoritative_version(), 3);
        b.set_authoritative_version(4);
        assert_eq!(b.authoritative_version(), 4);
    }

    #[tokio::test]
    async fn object_safe_behind_arc() {
        let dir = tempdir().unwrap();
        let _: Arc<dyn StateBackend> =
            Arc::new(ObjectStoreBackend::new(make_store(dir.path()), "node-0", 2));
    }

    #[tokio::test]
    async fn latest_committed_epoch_tracks_highest_sealed() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);

        // Fresh store: nothing committed.
        assert_eq!(backend.latest_committed_epoch().await.unwrap(), None);

        // Seal epochs 3 and 7 (out of order) by writing every vnode's
        // partial and running the CAS commit gate.
        let vnodes = [0u32, 1];
        for &epoch in &[3u64, 7] {
            for v in &vnodes {
                backend
                    .write_partial(*v, epoch, 0, Bytes::from_static(b"s"))
                    .await
                    .unwrap();
            }
            assert!(backend.epoch_complete(epoch, &vnodes, &[]).await.unwrap());
        }

        // Epoch 5 has partials but no commit marker — must be ignored.
        backend
            .write_partial(0, 5, 0, Bytes::from_static(b"uncommitted"))
            .await
            .unwrap();

        assert_eq!(backend.latest_committed_epoch().await.unwrap(), Some(7));
    }

    #[tokio::test]
    async fn prune_before_deletes_old_epochs() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);

        // Seed epochs 1..=5 with one vnode each.
        for epoch in 1..=5u64 {
            backend
                .write_partial(0, epoch, 0, Bytes::from_static(b"x"))
                .await
                .unwrap();
        }

        backend.prune_before(4).await.unwrap();

        for epoch in 1..=3 {
            assert!(
                backend.read_partial(0, epoch).await.unwrap().is_none(),
                "epoch {epoch} should be pruned",
            );
        }
        for epoch in 4..=5 {
            assert!(
                backend.read_partial(0, epoch).await.unwrap().is_some(),
                "epoch {epoch} should be retained",
            );
        }
    }

    /// The horizon cursor must advance so the second prune takes the
    /// bounded `[latest_pruned_epoch, before)` window (hot path) rather
    /// than re-listing the whole store, while still deleting exactly the
    /// epochs below the new horizon.
    #[tokio::test]
    async fn prune_before_is_incremental_and_advances_horizon() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);

        // Seed epochs 1..=6, two vnodes each so deletes touch >1 object.
        for epoch in 1..=6u64 {
            for v in 0..2u32 {
                backend
                    .write_partial(v, epoch, 0, Bytes::from_static(b"x"))
                    .await
                    .unwrap();
            }
        }

        // First prune: cold path (cursor still at the `0` sentinel) — one
        // full scan, drops epochs 1..=2, then advances the cursor to 3.
        backend.prune_before(3).await.unwrap();
        assert_eq!(backend.latest_pruned_epoch.load(Ordering::Relaxed), 3);

        // Second prune: hot path now (cursor == 3), only walks epochs
        // [3, 5) yet must still leave the store as if a full scan ran.
        backend.prune_before(5).await.unwrap();
        assert_eq!(backend.latest_pruned_epoch.load(Ordering::Relaxed), 5);

        for epoch in 1..=4u64 {
            for v in 0..2u32 {
                assert!(
                    backend.read_partial(v, epoch).await.unwrap().is_none(),
                    "epoch {epoch} vnode {v} should be pruned",
                );
            }
        }
        for epoch in 5..=6u64 {
            for v in 0..2u32 {
                assert!(
                    backend.read_partial(v, epoch).await.unwrap().is_some(),
                    "epoch {epoch} vnode {v} should be retained",
                );
            }
        }

        // Idempotent re-prune at the same horizon is a no-op.
        backend.prune_before(5).await.unwrap();
        assert_eq!(backend.latest_pruned_epoch.load(Ordering::Relaxed), 5);
        assert!(backend.read_partial(0, 5).await.unwrap().is_some());
    }

    /// A straggler write below the cursor is invisible to incremental prunes;
    /// the periodic full-scan re-baseline must reclaim it.
    #[tokio::test]
    async fn periodic_full_scan_reclaims_late_write_below_cursor() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);

        backend
            .write_partial(0, 1, 0, Bytes::from_static(b"x"))
            .await
            .unwrap();
        backend.prune_before(3).await.unwrap();
        assert!(backend.read_partial(0, 1).await.unwrap().is_none());

        // Straggler lands in an epoch the cursor has already passed.
        backend
            .write_partial(0, 1, 0, Bytes::from_static(b"late"))
            .await
            .unwrap();
        backend.prune_before(3).await.unwrap();
        assert!(
            backend.read_partial(0, 1).await.unwrap().is_some(),
            "incremental prune cannot see below the cursor",
        );

        // Drive past the re-baseline pass; the full scan reclaims it.
        for _ in 0..PRUNE_FULL_SCAN_EVERY {
            backend.prune_before(3).await.unwrap();
        }
        assert!(backend.read_partial(0, 1).await.unwrap().is_none());
    }
}
