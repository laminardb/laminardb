//! [`ObjectStoreBackend`] — durable partial-state storage backed by any
//! `object_store` implementation (S3, GCS, Azure, `LocalFileSystem`).
//!
//! `seal_checkpoint` performs a CAS seal: if every vnode's `partial.bin`
//! and every required commit descriptor is present, `put(_SEAL, Create)`
//! seals the exact checkpoint attempt. The `_SEAL` marker is the durability boundary the
//! checkpoint coordinator consults before releasing sinks.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};

use super::backend::{
    digest_hex, sha256, CheckpointAttempt, CheckpointSeal, CheckpointSealInventory,
    SealedVnodePartial, StateBackend, StateBackendDurability, StateBackendError,
    CHECKPOINT_SEAL_VERSION,
};

/// Every Nth prune does a full listing instead of the incremental window.
const PRUNE_FULL_SCAN_EVERY: u64 = 32;
const VNODE_PARTIAL_MAGIC: &[u8; 8] = b"LDBVP1\0\0";
const VNODE_PARTIAL_VERSION: u32 = 1;
const VNODE_PARTIAL_HEADER_LEN: usize = 128;
const PARTIAL_ATTESTATION_READ_CONCURRENCY: usize = 32;

/// Object-store-backed [`StateBackend`].
pub struct ObjectStoreBackend {
    store: Arc<dyn ObjectStore>,
    durability_scope: StateBackendDurability,
    instance_id: String,
    /// Fresh for each backend construction, even when `instance_id` is stable across restarts.
    execution_id: uuid::Uuid,
    vnode_capacity: u32,
    /// Highest horizon already pruned; later prunes list only `[latest_pruned_epoch, before)`
    /// instead of the whole store. `0` = no baseline yet, forcing one full listing first.
    latest_pruned_epoch: AtomicU64,
    /// Prune-call counter driving the periodic full-scan re-baseline that bounds
    /// how long a straggler write below the cursor can leak.
    prune_passes: AtomicU64,
    /// Split-brain fence: writes stamped with an `assignment_version` below this are
    /// rejected. `0` disables the fence, accepting unconfigured single-instance callers.
    authoritative_version: Arc<AtomicU64>,
}

impl std::fmt::Debug for ObjectStoreBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreBackend")
            .field("durability_scope", &self.durability_scope)
            .field("instance_id", &self.instance_id)
            .field("execution_id", &self.execution_id)
            .field("vnode_capacity", &self.vnode_capacity)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreBackend {
    /// Wrap an existing [`ObjectStore`] without certifying persistence.
    ///
    /// The opaque trait object does not reveal whether it is an in-memory,
    /// node-local, or shared implementation, so this conservative constructor
    /// reports [`StateBackendDurability::Volatile`]. Production hosts should use
    /// [`Self::node_durable`] or [`Self::cluster_shared`] after establishing the
    /// storage topology.
    #[must_use]
    pub fn new(
        store: Arc<dyn ObjectStore>,
        instance_id: impl Into<String>,
        vnode_capacity: u32,
    ) -> Self {
        Self::with_durability_scope(
            store,
            instance_id,
            vnode_capacity,
            StateBackendDurability::Volatile,
        )
    }

    /// Wrap storage that survives restart on this node but is not guaranteed
    /// to be reachable by cluster peers.
    #[must_use]
    pub fn node_durable(
        store: Arc<dyn ObjectStore>,
        instance_id: impl Into<String>,
        vnode_capacity: u32,
    ) -> Self {
        Self::with_durability_scope(
            store,
            instance_id,
            vnode_capacity,
            StateBackendDurability::NodeDurable,
        )
    }

    /// Wrap durable storage whose namespace is reachable by every cluster node.
    #[must_use]
    pub fn cluster_shared(
        store: Arc<dyn ObjectStore>,
        instance_id: impl Into<String>,
        vnode_capacity: u32,
    ) -> Self {
        Self::with_durability_scope(
            store,
            instance_id,
            vnode_capacity,
            StateBackendDurability::ClusterShared,
        )
    }

    fn with_durability_scope(
        store: Arc<dyn ObjectStore>,
        instance_id: impl Into<String>,
        vnode_capacity: u32,
        durability_scope: StateBackendDurability,
    ) -> Self {
        let instance_id = instance_id.into();
        Self {
            store,
            durability_scope,
            instance_id,
            execution_id: uuid::Uuid::new_v4(),
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

    /// Shared handle to the authoritative version counter, cloneable by a
    /// single owner that drives it without relaying through the trait method.
    #[must_use]
    pub fn authoritative_version_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.authoritative_version)
    }

    #[cfg(test)]
    fn execution_id(&self) -> uuid::Uuid {
        self.execution_id
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

    fn attempt_prefix(attempt: CheckpointAttempt) -> String {
        format!(
            "state-v2/epoch={}/checkpoint={}/",
            attempt.epoch, attempt.checkpoint_id
        )
    }

    fn partial_path(attempt: CheckpointAttempt, vnode: u32) -> OsPath {
        OsPath::from(format!(
            "{}vnode={vnode}/partial.bin",
            Self::attempt_prefix(attempt)
        ))
    }

    fn seal_path(attempt: CheckpointAttempt) -> OsPath {
        OsPath::from(format!("{}_SEAL", Self::attempt_prefix(attempt)))
    }

    fn descriptor_path(attempt: CheckpointAttempt, key: &str) -> OsPath {
        OsPath::from(format!("{}commit/{key}", Self::attempt_prefix(attempt)))
    }

    fn source_offsets_path(attempt: CheckpointAttempt, node_key: &str) -> OsPath {
        OsPath::from(format!(
            "{}srcoff/{node_key}",
            Self::attempt_prefix(attempt)
        ))
    }

    /// Parse an attempt from `state-v2/epoch=N/checkpoint=M/...`.
    fn attempt_from_path(loc: &str) -> Option<CheckpointAttempt> {
        let mut parts = loc.split('/');
        if parts.next()? != "state-v2" {
            return None;
        }
        let epoch = parts.next()?.strip_prefix("epoch=")?.parse().ok()?;
        let checkpoint_id = parts.next()?.strip_prefix("checkpoint=")?.parse().ok()?;
        Some(CheckpointAttempt::new(epoch, checkpoint_id))
    }

    /// Wrap raw operator state in a fixed-width provenance header. The fixed width lets the
    /// durability gate validate hundreds of vnode generations with small concurrent range GETs
    /// instead of downloading every state blob again.
    fn encode_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_version: u64,
        payload: &Bytes,
    ) -> Bytes {
        let writer_digest = sha256(self.instance_id.as_bytes());
        let payload_digest = sha256(payload);
        let mut encoded = Vec::with_capacity(VNODE_PARTIAL_HEADER_LEN + payload.len());
        encoded.extend_from_slice(VNODE_PARTIAL_MAGIC);
        encoded.extend_from_slice(&VNODE_PARTIAL_VERSION.to_be_bytes());
        encoded.extend_from_slice(&attempt.epoch.to_be_bytes());
        encoded.extend_from_slice(&attempt.checkpoint_id.to_be_bytes());
        encoded.extend_from_slice(&vnode.to_be_bytes());
        encoded.extend_from_slice(&assignment_version.to_be_bytes());
        encoded.extend_from_slice(&writer_digest);
        encoded.extend_from_slice(self.execution_id.as_bytes());
        encoded.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        encoded.extend_from_slice(&payload_digest);
        debug_assert_eq!(encoded.len(), VNODE_PARTIAL_HEADER_LEN);
        encoded.extend_from_slice(payload);
        Bytes::from(encoded)
    }

    fn parse_partial_header(
        header: &[u8],
        expected_attempt: CheckpointAttempt,
        expected_vnode: u32,
    ) -> Result<SealedVnodePartial, StateBackendError> {
        fn field<const N: usize>(
            header: &[u8],
            start: usize,
        ) -> Result<[u8; N], StateBackendError> {
            header
                .get(start..start + N)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or_else(|| {
                    StateBackendError::Serialization(
                        "truncated vnode partial provenance header".into(),
                    )
                })
        }

        if header.len() < VNODE_PARTIAL_HEADER_LEN
            || &header[..VNODE_PARTIAL_MAGIC.len()] != VNODE_PARTIAL_MAGIC
        {
            return Err(StateBackendError::Serialization(
                "invalid vnode partial provenance header".into(),
            ));
        }
        let version = u32::from_be_bytes(field(header, 8)?);
        if version != VNODE_PARTIAL_VERSION {
            return Err(StateBackendError::Serialization(format!(
                "unsupported vnode partial version {version}; expected {VNODE_PARTIAL_VERSION}"
            )));
        }
        let attempt = CheckpointAttempt::new(
            u64::from_be_bytes(field(header, 12)?),
            u64::from_be_bytes(field(header, 20)?),
        );
        let vnode = u32::from_be_bytes(field(header, 28)?);
        if attempt != expected_attempt || vnode != expected_vnode {
            return Err(StateBackendError::Conflict {
                resource: Self::partial_path(expected_attempt, expected_vnode).to_string(),
                message: format!(
                    "partial header names attempt {attempt:?} vnode {vnode}, expected attempt \
                     {expected_attempt:?} vnode {expected_vnode}"
                ),
            });
        }
        let assignment_version = u64::from_be_bytes(field(header, 32)?);
        let writer_digest = field::<32>(header, 40)?;
        let execution_id = uuid::Uuid::from_bytes(field(header, 72)?);
        let payload_len = u64::from_be_bytes(field(header, 88)?);
        let payload_digest = field::<32>(header, 96)?;
        Ok(SealedVnodePartial {
            vnode,
            assignment_version,
            writer_id_sha256: digest_hex(&writer_digest),
            execution_id,
            payload_len,
            payload_sha256: digest_hex(&payload_digest),
        })
    }

    fn decode_partial(
        bytes: &Bytes,
        expected_attempt: CheckpointAttempt,
        expected_vnode: u32,
    ) -> Result<Bytes, StateBackendError> {
        const ARCHIVE_ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;

        let metadata = Self::parse_partial_header(bytes, expected_attempt, expected_vnode)?;
        let payload_len = usize::try_from(metadata.payload_len).map_err(|_| {
            StateBackendError::Serialization("vnode partial payload length overflows usize".into())
        })?;
        if bytes.len() != VNODE_PARTIAL_HEADER_LEN.saturating_add(payload_len) {
            return Err(StateBackendError::Serialization(format!(
                "vnode partial payload length mismatch: header={} actual={}",
                metadata.payload_len,
                bytes.len().saturating_sub(VNODE_PARTIAL_HEADER_LEN)
            )));
        }
        let payload = bytes.slice(VNODE_PARTIAL_HEADER_LEN..);
        if metadata.payload_sha256 != digest_hex(&sha256(&payload)) {
            return Err(StateBackendError::Serialization(
                "vnode partial payload checksum mismatch".into(),
            ));
        }
        if payload.is_empty() || payload.as_ptr().align_offset(ARCHIVE_ALIGNMENT) == 0 {
            return Ok(payload);
        }

        // Object-store clients may expose a view into an arbitrarily aligned network buffer.
        // Normalize it once here so recovery-chain consumers can validate and decode the rkyv
        // payload repeatedly without making a fresh aligned copy on every pass.
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
        aligned.extend_from_slice(&payload);
        Ok(Bytes::from_owner(aligned))
    }

    async fn read_partial_attestation(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
    ) -> Result<Option<SealedVnodePartial>, StateBackendError> {
        let path = Self::partial_path(attempt, vnode);
        match self
            .store
            .get_range(&path, 0..VNODE_PARTIAL_HEADER_LEN as u64)
            .await
        {
            Ok(header) => Self::parse_partial_header(&header, attempt, vnode).map(Some),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(StateBackendError::Io(error.to_string())),
        }
    }
}

#[async_trait]
impl StateBackend for ObjectStoreBackend {
    fn durability_scope(&self) -> StateBackendDurability {
        self.durability_scope
    }

    async fn write_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.check_vnode(vnode)?;
        self.check_assignment_version(assignment_version)?;
        let path = Self::partial_path(attempt, vnode);
        let bytes = self.encode_partial(attempt, vnode, assignment_version, &bytes);
        self.put_immutable(&path, bytes).await
    }

    async fn read_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
    ) -> Result<Option<Bytes>, StateBackendError> {
        self.check_vnode(vnode)?;
        let path = Self::partial_path(attempt, vnode);
        match self.store.get(&path).await {
            Ok(res) => {
                let b = res
                    .bytes()
                    .await
                    .map_err(|e| StateBackendError::Io(e.to_string()))?;
                Self::decode_partial(&b, attempt, vnode).map(Some)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(StateBackendError::Io(e.to_string())),
        }
    }

    async fn write_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
        assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.check_assignment_version(assignment_version)?;
        self.put_immutable(&Self::descriptor_path(attempt, key), bytes)
            .await
    }

    async fn read_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
    ) -> Result<Option<Bytes>, StateBackendError> {
        match self.store.get(&Self::descriptor_path(attempt, key)).await {
            Ok(result) => result
                .bytes()
                .await
                .map(Some)
                .map_err(|error| StateBackendError::Io(error.to_string())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(StateBackendError::Io(error.to_string())),
        }
    }

    async fn read_commit_descriptors(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Vec<(String, Bytes)>, StateBackendError> {
        use tokio_stream::StreamExt;

        let prefix_str = format!("{}commit/", Self::attempt_prefix(attempt));
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
        out.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Ok(out)
    }

    async fn write_source_offsets(
        &self,
        attempt: CheckpointAttempt,
        node_key: &str,
        assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.check_assignment_version(assignment_version)?;
        self.put_immutable(&Self::source_offsets_path(attempt, node_key), bytes)
            .await
    }

    async fn read_source_offsets(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Vec<Bytes>, StateBackendError> {
        use tokio_stream::StreamExt;

        let prefix = OsPath::from(format!("{}srcoff/", Self::attempt_prefix(attempt)));
        let mut entries = self.store.list(Some(&prefix));
        let mut out = Vec::new();
        while let Some(entry) = entries.next().await {
            let loc = entry
                .map_err(|e| StateBackendError::Io(e.to_string()))?
                .location;
            let bytes = self
                .store
                .get(&loc)
                .await
                .map_err(|e| StateBackendError::Io(e.to_string()))?
                .bytes()
                .await
                .map_err(|e| StateBackendError::Io(e.to_string()))?;
            out.push((loc.to_string(), bytes));
        }
        out.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Ok(out.into_iter().map(|(_, bytes)| bytes).collect())
    }

    async fn seal_checkpoint(
        &self,
        attempt: CheckpointAttempt,
        assignment_version: u64,
        vnodes: &[u32],
        required_descriptors: &[String],
    ) -> Result<bool, StateBackendError> {
        use rustc_hash::FxHashSet;
        use tokio_stream::StreamExt;

        self.check_assignment_version(assignment_version)?;
        let seal_path = Self::seal_path(attempt);
        // An existing seal is idempotent only for this exact process incarnation
        // and assignment fence.
        match self.store.head(&seal_path).await {
            Ok(_) => {
                let existing = self.read_seal(&seal_path).await?;
                let expected = CheckpointSeal::new(
                    attempt,
                    self.instance_id.clone(),
                    self.execution_id,
                    assignment_version,
                    vnodes,
                    &existing.sealed_partials,
                    required_descriptors,
                );
                return if existing == expected {
                    Ok(true)
                } else {
                    Err(StateBackendError::Conflict {
                        resource: seal_path.to_string(),
                        message: "existing seal does not match this execution, assignment, or artifact inventory".into(),
                    })
                };
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(StateBackendError::Io(e.to_string())),
        }

        for &v in vnodes {
            self.check_vnode(v)?;
        }

        // List the epoch prefix once, then check every required vnode's
        // partial is present — one round trip instead of O(vnodes) HEADs.
        let prefix = OsPath::from(Self::attempt_prefix(attempt));
        let mut entries = self.store.list(Some(&prefix));
        let mut found_paths: FxHashSet<OsPath> = FxHashSet::default();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|e| StateBackendError::Io(e.to_string()))?;
            found_paths.insert(entry.location);
        }

        for &v in vnodes {
            let path = Self::partial_path(attempt, v);
            if !found_paths.contains(&path) {
                return Ok(false);
            }
        }
        // Commit descriptors live under this attempt's `commit/` prefix.
        for key in required_descriptors {
            if !found_paths.contains(&Self::descriptor_path(attempt, key)) {
                return Ok(false);
            }
        }

        // Presence alone is insufficient: a deposed owner can create the canonical immutable
        // vnode path before learning the new assignment generation. Read only the fixed header,
        // in bounded parallel batches, and require the exact generation being sealed.
        let mut required_vnodes = vnodes.to_vec();
        required_vnodes.sort_unstable();
        required_vnodes.dedup();
        let mut sealed_partials = Vec::with_capacity(required_vnodes.len());
        for chunk in required_vnodes.chunks(PARTIAL_ATTESTATION_READ_CONCURRENCY) {
            let attestations = futures::future::try_join_all(
                chunk
                    .iter()
                    .map(|&vnode| self.read_partial_attestation(attempt, vnode)),
            )
            .await?;
            for attestation in attestations {
                let Some(attestation) = attestation else {
                    return Ok(false);
                };
                if attestation.assignment_version != assignment_version {
                    return Err(StateBackendError::Conflict {
                        resource: Self::partial_path(attempt, attestation.vnode).to_string(),
                        message: format!(
                            "partial assignment version {} cannot satisfy seal version {assignment_version}",
                            attestation.assignment_version
                        ),
                    });
                }
                sealed_partials.push(attestation);
            }
        }

        let expected_seal = CheckpointSeal::new(
            attempt,
            self.instance_id.clone(),
            self.execution_id,
            assignment_version,
            &required_vnodes,
            &sealed_partials,
            required_descriptors,
        );

        let bytes = serde_json::to_vec(&expected_seal)
            .map(Bytes::from)
            .map_err(|e| StateBackendError::Serialization(e.to_string()))?;
        let payload = PutPayload::from(bytes);
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self.store.put_opts(&seal_path, payload, opts).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => {
                self.verify_seal(&seal_path, &expected_seal).await
            }
            Err(e) => Err(StateBackendError::Io(e.to_string())),
        }
    }

    async fn sealed_checkpoints(
        &self,
        after_checkpoint_id: u64,
    ) -> Result<Vec<CheckpointAttempt>, StateBackendError> {
        use tokio_stream::StreamExt;

        let mut entries = self.store.list(Some(&OsPath::from("state-v2/")));
        let mut out = Vec::new();
        while let Some(entry) = entries.next().await {
            let loc = entry
                .map_err(|e| StateBackendError::Io(e.to_string()))?
                .location;
            if !loc.as_ref().ends_with("/_SEAL") {
                continue;
            }
            let path_attempt = Self::attempt_from_path(loc.as_ref()).ok_or_else(|| {
                StateBackendError::Serialization(format!(
                    "invalid checkpoint seal path: {}",
                    loc.as_ref()
                ))
            })?;
            let seal = self.read_seal(&loc).await?;
            if seal.attempt != path_attempt {
                return Err(StateBackendError::Conflict {
                    resource: loc.to_string(),
                    message: format!(
                        "seal body names {:?}, path names {path_attempt:?}",
                        seal.attempt
                    ),
                });
            }
            if seal.attempt.checkpoint_id > after_checkpoint_id {
                out.push(seal.attempt);
            }
        }
        out.sort_unstable_by_key(|attempt| attempt.checkpoint_id);
        Ok(out)
    }

    async fn checkpoint_seal_inventory(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Option<CheckpointSealInventory>, StateBackendError> {
        let path = Self::seal_path(attempt);
        match self.store.head(&path).await {
            Ok(_) => {
                let seal = self.read_seal(&path).await?;
                if seal.attempt != attempt {
                    return Err(StateBackendError::Conflict {
                        resource: path.to_string(),
                        message: format!(
                            "seal body names {:?}, requested {attempt:?}",
                            seal.attempt
                        ),
                    });
                }
                Ok(Some(seal.inventory()))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(StateBackendError::Io(error.to_string())),
        }
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
            // No baseline yet: one full state-v2 listing.
            let mut entries = self.store.list(Some(&OsPath::from("state-v2/")));
            while let Some(entry) = entries.next().await {
                let entry = entry.map_err(|e| StateBackendError::Io(e.to_string()))?;
                let Some(attempt) = Self::attempt_from_path(entry.location.as_ref()) else {
                    continue;
                };
                if attempt.epoch < before {
                    victims.push(entry.location);
                }
            }
        } else {
            // Only epochs in `[start, before)` can still hold objects, and
            // `epoch={N}/` is an exact segment, so per-epoch listings cost
            // O(epochs-since-last-prune × vnodes) instead of O(store).
            for epoch in start..before {
                let prefix = OsPath::from(format!("state-v2/epoch={epoch}/"));
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

    async fn truncate_after(&self, after: u64) -> Result<(), StateBackendError> {
        use futures::stream::{self, StreamExt};

        // Full state-v2 scan (dynamic `epoch=N` segment, same constraint as `prune_before`).
        // Recovery-path only, and a truncation failure must fail the rewind closed —
        // surviving artifacts would collide with the reused epoch numbers.
        let mut entries = self.store.list(Some(&OsPath::from("state-v2/")));
        let mut victims: Vec<OsPath> = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|e| StateBackendError::Io(e.to_string()))?;
            let Some(attempt) = Self::attempt_from_path(entry.location.as_ref()) else {
                continue;
            };
            if attempt.epoch > after {
                victims.push(entry.location);
            }
        }
        if victims.is_empty() {
            return Ok(());
        }
        let locations =
            stream::iter(victims.into_iter().map(Ok::<OsPath, object_store::Error>)).boxed();
        let mut deletes = self.store.delete_stream(locations);
        while let Some(res) = deletes.next().await {
            match res {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(StateBackendError::Io(e.to_string())),
            }
        }
        Ok(())
    }

    async fn latest_sealed_checkpoint(
        &self,
    ) -> Result<Option<CheckpointAttempt>, StateBackendError> {
        Ok(self.sealed_checkpoints(0).await?.into_iter().last())
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
    fn check_assignment_version(&self, caller: u64) -> Result<(), StateBackendError> {
        let authoritative = self.authoritative_version.load(Ordering::Acquire);
        if authoritative > 0 && caller < authoritative {
            Err(StateBackendError::StaleVersion {
                caller,
                authoritative,
            })
        } else {
            Ok(())
        }
    }

    /// CAS-create immutable bytes. A retry of the exact bytes succeeds; a
    /// different payload at the same key is a hard conflict.
    async fn put_immutable(&self, path: &OsPath, bytes: Bytes) -> Result<(), StateBackendError> {
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(path, PutPayload::from(bytes.clone()), opts)
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self
                    .store
                    .get(path)
                    .await
                    .map_err(|e| StateBackendError::Io(e.to_string()))?
                    .bytes()
                    .await
                    .map_err(|e| StateBackendError::Io(e.to_string()))?;
                if existing == bytes {
                    Ok(())
                } else {
                    Err(StateBackendError::Conflict {
                        resource: path.to_string(),
                        message: "existing immutable artifact has different bytes".into(),
                    })
                }
            }
            Err(e) => Err(StateBackendError::Io(e.to_string())),
        }
    }

    async fn read_seal(&self, path: &OsPath) -> Result<CheckpointSeal, StateBackendError> {
        let bytes = self
            .store
            .get(path)
            .await
            .map_err(|e| StateBackendError::Io(e.to_string()))?
            .bytes()
            .await
            .map_err(|e| StateBackendError::Io(e.to_string()))?;
        let seal: CheckpointSeal = serde_json::from_slice(&bytes).map_err(|e| {
            StateBackendError::Serialization(format!("invalid checkpoint seal: {e}"))
        })?;
        if seal.version != CHECKPOINT_SEAL_VERSION {
            return Err(StateBackendError::Serialization(format!(
                "unsupported checkpoint seal version {}; expected {CHECKPOINT_SEAL_VERSION}",
                seal.version
            )));
        }
        seal.validate().map_err(|error| {
            StateBackendError::Serialization(format!("invalid checkpoint seal: {error}"))
        })?;
        Ok(seal)
    }

    async fn verify_seal(
        &self,
        path: &OsPath,
        expected: &CheckpointSeal,
    ) -> Result<bool, StateBackendError> {
        let existing = self.read_seal(path).await?;
        if &existing == expected {
            Ok(true)
        } else {
            Err(StateBackendError::Conflict {
                resource: path.to_string(),
                message: format!(
                    "existing seal is attempt {:?}, instance {:?}, execution {}, assignment {}; \
                     caller is attempt {:?}, instance {:?}, execution {}, assignment {}",
                    existing.attempt,
                    existing.instance_id,
                    existing.execution_id,
                    existing.assignment_version,
                    expected.attempt,
                    expected.instance_id,
                    expected.execution_id,
                    expected.assignment_version,
                ),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attempt(epoch: u64) -> CheckpointAttempt {
        CheckpointAttempt::new(epoch, epoch * 10)
    }
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
            .write_partial(attempt(1), 0, 0, Bytes::from_static(b"hello"))
            .await
            .unwrap();
        let got = backend.read_partial(attempt(1), 0).await.unwrap().unwrap();
        assert_eq!(&got[..], b"hello");
    }

    #[test]
    fn decode_partial_realigns_an_unaligned_transport_buffer() {
        const ARCHIVE_ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;

        let backend =
            ObjectStoreBackend::new(Arc::new(object_store::memory::InMemory::new()), "node-0", 4);
        let checkpoint = attempt(1);
        let payload = Bytes::from_static(b"archived vnode state");
        let encoded = backend.encode_partial(checkpoint, 0, 0, &payload);

        let mut transport = bytes::BytesMut::zeroed(encoded.len() + ARCHIVE_ALIGNMENT);
        let offset = (0..ARCHIVE_ALIGNMENT)
            .find(|offset| {
                !(transport.as_ptr() as usize + offset + VNODE_PARTIAL_HEADER_LEN)
                    .is_multiple_of(ARCHIVE_ALIGNMENT)
            })
            .expect("an unaligned offset exists");
        transport[offset..offset + encoded.len()].copy_from_slice(&encoded);
        let transport = transport.freeze().slice(offset..offset + encoded.len());
        assert!(!(transport[VNODE_PARTIAL_HEADER_LEN..].as_ptr() as usize)
            .is_multiple_of(ARCHIVE_ALIGNMENT));

        let decoded = ObjectStoreBackend::decode_partial(&transport, checkpoint, 0).unwrap();
        assert_eq!(decoded, payload);
        assert!((decoded.as_ptr() as usize).is_multiple_of(ARCHIVE_ALIGNMENT));
    }

    #[tokio::test]
    async fn immutable_artifact_accepts_identical_retry_and_rejects_conflict() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        let checkpoint = attempt(1);
        backend
            .write_partial(checkpoint, 0, 0, Bytes::from_static(b"first"))
            .await
            .unwrap();
        backend
            .write_partial(checkpoint, 0, 0, Bytes::from_static(b"first"))
            .await
            .unwrap();
        assert!(matches!(
            backend
                .write_partial(checkpoint, 0, 0, Bytes::from_static(b"different"))
                .await,
            Err(StateBackendError::Conflict { .. })
        ));
        assert_eq!(
            backend.read_partial(checkpoint, 0).await.unwrap().unwrap(),
            Bytes::from_static(b"first")
        );
    }

    #[tokio::test]
    async fn reused_epoch_isolated_by_checkpoint_id() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        let old = CheckpointAttempt::new(5, 50);
        let new = CheckpointAttempt::new(5, 99);
        backend
            .write_partial(old, 0, 0, Bytes::from_static(b"old"))
            .await
            .unwrap();
        backend
            .write_partial(new, 0, 0, Bytes::from_static(b"new"))
            .await
            .unwrap();
        assert_eq!(
            backend.read_partial(old, 0).await.unwrap().unwrap(),
            Bytes::from_static(b"old")
        );
        assert_eq!(
            backend.read_partial(new, 0).await.unwrap().unwrap(),
            Bytes::from_static(b"new")
        );
    }

    #[tokio::test]
    async fn source_offsets_union_and_prune() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);

        backend
            .write_source_offsets(
                attempt(7),
                "node-1",
                0,
                Bytes::from_static(b"{\"events:0\":\"100\"}"),
            )
            .await
            .unwrap();
        backend
            .write_source_offsets(
                attempt(7),
                "node-2",
                0,
                Bytes::from_static(b"{\"events:1\":\"200\"}"),
            )
            .await
            .unwrap();

        let blobs = backend.read_source_offsets(attempt(7)).await.unwrap();
        assert_eq!(blobs.len(), 2);
        assert!(backend
            .read_source_offsets(attempt(8))
            .await
            .unwrap()
            .is_empty());

        // Lives under `epoch=7/srcoff/`, so the epoch-prefix prune reclaims it.
        backend.prune_before(8).await.unwrap();
        assert!(backend
            .read_source_offsets(attempt(7))
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn seal_checkpoint_cas_is_idempotent_for_same_execution() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        let vnodes = [0u32, 1, 2];

        assert!(!backend
            .seal_checkpoint(attempt(1), 0, &vnodes, &[])
            .await
            .unwrap());
        for v in &vnodes {
            backend
                .write_partial(attempt(1), *v, 0, Bytes::from_static(b"y"))
                .await
                .unwrap();
        }
        assert!(backend
            .seal_checkpoint(attempt(1), 0, &vnodes, &[])
            .await
            .unwrap());
        // Idempotent — same committer id in the audit body.
        assert!(backend
            .seal_checkpoint(attempt(1), 0, &vnodes, &[])
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn seal_body_binds_attempt_writer_fence_and_artifact_inventory() {
        let dir = tempdir().unwrap();
        let store = make_store(dir.path());
        let backend = ObjectStoreBackend::new(Arc::clone(&store), "stable-node", 2);
        let checkpoint = CheckpointAttempt::new(4, 401);
        backend.set_authoritative_version(7);
        backend
            .write_partial(checkpoint, 0, 7, Bytes::from_static(b"state"))
            .await
            .unwrap();
        let descriptors = ["participant=7/sink=orders".to_string()];
        backend
            .write_commit_descriptor(
                checkpoint,
                &descriptors[0],
                7,
                Bytes::from_static(b"marker"),
            )
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(checkpoint, 7, &[0], &descriptors)
            .await
            .unwrap());

        let bytes = store
            .get(&ObjectStoreBackend::seal_path(checkpoint))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let seal: CheckpointSeal = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(seal.version, CHECKPOINT_SEAL_VERSION);
        assert_eq!(seal.attempt, checkpoint);
        assert_eq!(seal.instance_id, "stable-node");
        assert_eq!(seal.execution_id, backend.execution_id());
        assert_eq!(seal.assignment_version, 7);
        assert_eq!(seal.required_vnodes, vec![0]);
        assert_eq!(seal.sealed_partials.len(), 1);
        assert_eq!(seal.sealed_partials[0].vnode, 0);
        assert_eq!(seal.sealed_partials[0].assignment_version, 7);
        assert_eq!(seal.sealed_partials[0].payload_len, 5);
        assert_eq!(seal.required_descriptors, descriptors);
        assert_eq!(
            backend
                .checkpoint_seal_inventory(checkpoint)
                .await
                .unwrap()
                .unwrap(),
            seal.inventory()
        );

        let error = backend
            .seal_checkpoint(checkpoint, 7, &[0, 1], &descriptors)
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
    }

    #[tokio::test]
    async fn seal_checkpoint_requires_commit_descriptors() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        let vnodes = [0u32];
        let key = "node=node-0/sink=ice";
        let need = [key.to_string()];

        backend
            .write_partial(attempt(1), 0, 0, Bytes::from_static(b"s"))
            .await
            .unwrap();
        // Partial present but the descriptor is missing → epoch not sealed.
        assert!(!backend
            .seal_checkpoint(attempt(1), 0, &vnodes, &need)
            .await
            .unwrap());

        backend
            .write_commit_descriptor(attempt(1), key, 0, Bytes::from_static(b"df"))
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(attempt(1), 0, &vnodes, &need)
            .await
            .unwrap());

        let descs = backend.read_commit_descriptors(attempt(1)).await.unwrap();
        assert_eq!(descs, vec![(key.to_string(), Bytes::from_static(b"df"))]);
    }

    /// The CAS-create `AlreadyExists` branch must not silently agree it committed:
    /// the loser reads the marker, sees a mismatched audit body, and fails loud.
    #[tokio::test]
    async fn seal_rejects_different_execution_incarnation() {
        let dir = tempdir().unwrap();
        let store = make_store(dir.path());
        let winner = ObjectStoreBackend::new(Arc::clone(&store), "winner", 4);
        let loser = ObjectStoreBackend::new(Arc::clone(&store), "loser", 4);

        let vnodes = [0u32, 1];
        // Both "nodes" wrote partials for the epoch.
        for v in &vnodes {
            winner
                .write_partial(attempt(7), *v, 0, Bytes::from_static(b"w"))
                .await
                .unwrap();
        }

        // Winner CAS-creates the state seal first.
        assert!(winner
            .seal_checkpoint(attempt(7), 0, &vnodes, &[])
            .await
            .unwrap());

        // Loser finds a seal created by a different execution incarnation.
        let err = loser
            .seal_checkpoint(attempt(7), 0, &vnodes, &[])
            .await
            .unwrap_err();
        assert!(matches!(err, StateBackendError::Conflict { .. }));

        // And the winner's repeated call is still idempotent Ok(true).
        assert!(winner
            .seal_checkpoint(attempt(7), 0, &vnodes, &[])
            .await
            .unwrap());
    }

    /// Same contract on the CAS-loser path: if the marker doesn't exist
    /// at HEAD time but a peer sneaks in between our vnode-presence
    /// check and our own PUT, our `put_opts` fails with `AlreadyExists`.
    /// That branch must also compare committers, not silently succeed.
    #[tokio::test]
    async fn seal_cas_loser_rejects_different_execution() {
        let dir = tempdir().unwrap();
        let store = make_store(dir.path());
        let winner = ObjectStoreBackend::new(Arc::clone(&store), "winner", 4);
        let loser = ObjectStoreBackend::new(Arc::clone(&store), "loser", 4);

        let vnodes = [0u32, 1];
        for v in &vnodes {
            winner
                .write_partial(attempt(3), *v, 0, Bytes::from_static(b"w"))
                .await
                .unwrap();
        }
        // Manually pre-seed a structured seal under "winner" to
        // simulate the TOCTOU race deterministically — the loser's
        // put_opts will hit AlreadyExists on its own PUT attempt.
        let commit = ObjectStoreBackend::seal_path(attempt(3));
        let mut sealed_partials = Vec::new();
        for &vnode in &vnodes {
            sealed_partials.push(
                winner
                    .read_partial_attestation(attempt(3), vnode)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        let seal = CheckpointSeal::new(
            attempt(3),
            "winner".into(),
            winner.execution_id(),
            0,
            &vnodes,
            &sealed_partials,
            &[],
        );
        store
            .put(
                &commit,
                PutPayload::from(Bytes::from(serde_json::to_vec(&seal).unwrap())),
            )
            .await
            .unwrap();

        let err = loser
            .seal_checkpoint(attempt(3), 0, &vnodes, &[])
            .await
            .unwrap_err();
        assert!(matches!(err, StateBackendError::Conflict { .. }));
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
            .write_partial(attempt(1), 0, 2, Bytes::from_static(b"fresh"))
            .await
            .unwrap();

        // Stale tries to write at version 1 — but only IF it's also
        // learned of the rotation. Model that by promoting stale's
        // view too; the check is intra-backend here because the
        // durable version-broadcast channel is out of scope for this test.
        stale.set_authoritative_version(2);
        let err = stale
            .write_partial(attempt(1), 0, 1, Bytes::from_static(b"stale"))
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
            .write_partial(attempt(1), 1, 0, Bytes::from_static(b"ok"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn stale_generation_partial_cannot_satisfy_fresh_seal() {
        let dir = tempdir().unwrap();
        let store = make_store(dir.path());
        let stale = ObjectStoreBackend::new(Arc::clone(&store), "node-stale", 4);
        let fresh = ObjectStoreBackend::new(store, "node-fresh", 4);
        let checkpoint = CheckpointAttempt::new(9, 901);

        // The stale process has not learned generation 2 and wins the create-once path first.
        stale
            .write_partial(checkpoint, 0, 1, Bytes::from_static(b"stale-state"))
            .await
            .unwrap();
        fresh.set_authoritative_version(2);

        let error = fresh
            .seal_checkpoint(checkpoint, 2, &[0], &[])
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("cannot satisfy seal version 2"));
        assert!(fresh
            .checkpoint_seal_inventory(checkpoint)
            .await
            .unwrap()
            .is_none());
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

    #[test]
    fn durability_scope_requires_explicit_storage_topology() {
        let dir = tempdir().unwrap();
        assert_eq!(
            ObjectStoreBackend::new(make_store(dir.path()), "uncertified", 2).durability_scope(),
            StateBackendDurability::Volatile
        );
        assert_eq!(
            ObjectStoreBackend::node_durable(make_store(dir.path()), "local", 2).durability_scope(),
            StateBackendDurability::NodeDurable
        );
        assert_eq!(
            ObjectStoreBackend::cluster_shared(make_store(dir.path()), "shared", 2)
                .durability_scope(),
            StateBackendDurability::ClusterShared
        );
    }

    #[tokio::test]
    async fn object_safe_behind_arc() {
        let dir = tempdir().unwrap();
        let _: Arc<dyn StateBackend> =
            Arc::new(ObjectStoreBackend::new(make_store(dir.path()), "node-0", 2));
    }

    #[tokio::test]
    async fn latest_sealed_checkpoint_tracks_highest_attempt() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);

        // Fresh store: nothing committed.
        assert_eq!(backend.latest_sealed_checkpoint().await.unwrap(), None);

        // Seal epochs 3 and 7 (out of order) by writing every vnode's
        // partial and running the CAS commit gate.
        let vnodes = [0u32, 1];
        for &epoch in &[3u64, 7] {
            for v in &vnodes {
                backend
                    .write_partial(attempt(epoch), *v, 0, Bytes::from_static(b"s"))
                    .await
                    .unwrap();
            }
            assert!(backend
                .seal_checkpoint(attempt(epoch), 0, &vnodes, &[])
                .await
                .unwrap());
        }

        // Epoch 5 has partials but no state seal — it must be ignored.
        backend
            .write_partial(attempt(5), 0, 0, Bytes::from_static(b"uncommitted"))
            .await
            .unwrap();

        assert_eq!(
            backend.latest_sealed_checkpoint().await.unwrap(),
            Some(attempt(7))
        );
    }

    #[tokio::test]
    async fn prune_before_deletes_old_epochs() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);

        // Seed epochs 1..=5 with one vnode each.
        for epoch in 1..=5u64 {
            backend
                .write_partial(attempt(epoch), 0, 0, Bytes::from_static(b"x"))
                .await
                .unwrap();
        }

        backend.prune_before(4).await.unwrap();

        for epoch in 1..=3 {
            assert!(
                backend
                    .read_partial(attempt(epoch), 0)
                    .await
                    .unwrap()
                    .is_none(),
                "epoch {epoch} should be pruned",
            );
        }
        for epoch in 4..=5 {
            assert!(
                backend
                    .read_partial(attempt(epoch), 0)
                    .await
                    .unwrap()
                    .is_some(),
                "epoch {epoch} should be retained",
            );
        }
    }

    #[tokio::test]
    async fn truncate_after_removes_abandoned_timeline() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);

        for epoch in 1..=5u64 {
            backend
                .write_partial(attempt(epoch), 0, 0, Bytes::from_static(b"x"))
                .await
                .unwrap();
            backend
                .write_source_offsets(attempt(epoch), "node-0", 0, Bytes::from_static(b"{}"))
                .await
                .unwrap();
            assert!(backend
                .seal_checkpoint(attempt(epoch), 0, &[0], &[])
                .await
                .unwrap());
        }
        assert_eq!(
            backend.latest_sealed_checkpoint().await.unwrap(),
            Some(attempt(5))
        );

        backend.truncate_after(3).await.unwrap();

        for epoch in 1..=3u64 {
            assert!(backend
                .read_partial(attempt(epoch), 0)
                .await
                .unwrap()
                .is_some());
            assert_eq!(
                backend
                    .read_source_offsets(attempt(epoch))
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }
        for epoch in 4..=5u64 {
            assert!(backend
                .read_partial(attempt(epoch), 0)
                .await
                .unwrap()
                .is_none());
            assert!(backend
                .read_source_offsets(attempt(epoch))
                .await
                .unwrap()
                .is_empty());
        }
        // The seal must rewind too — it feeds the adopt path's offset cut and the
        // reused epoch numbers must not find a foreign `_SEAL` marker.
        assert_eq!(
            backend.latest_sealed_checkpoint().await.unwrap(),
            Some(attempt(3))
        );
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
                    .write_partial(attempt(epoch), v, 0, Bytes::from_static(b"x"))
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
                    backend
                        .read_partial(attempt(epoch), v)
                        .await
                        .unwrap()
                        .is_none(),
                    "epoch {epoch} vnode {v} should be pruned",
                );
            }
        }
        for epoch in 5..=6u64 {
            for v in 0..2u32 {
                assert!(
                    backend
                        .read_partial(attempt(epoch), v)
                        .await
                        .unwrap()
                        .is_some(),
                    "epoch {epoch} vnode {v} should be retained",
                );
            }
        }

        // Idempotent re-prune at the same horizon is a no-op.
        backend.prune_before(5).await.unwrap();
        assert_eq!(backend.latest_pruned_epoch.load(Ordering::Relaxed), 5);
        assert!(backend.read_partial(attempt(5), 0).await.unwrap().is_some());
    }

    /// A straggler write below the cursor is invisible to incremental prunes;
    /// the periodic full-scan re-baseline must reclaim it.
    #[tokio::test]
    async fn periodic_full_scan_reclaims_late_write_below_cursor() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);

        backend
            .write_partial(attempt(1), 0, 0, Bytes::from_static(b"x"))
            .await
            .unwrap();
        backend.prune_before(3).await.unwrap();
        assert!(backend.read_partial(attempt(1), 0).await.unwrap().is_none());

        // Straggler lands in an epoch the cursor has already passed.
        backend
            .write_partial(attempt(1), 0, 0, Bytes::from_static(b"late"))
            .await
            .unwrap();
        backend.prune_before(3).await.unwrap();
        assert!(
            backend.read_partial(attempt(1), 0).await.unwrap().is_some(),
            "incremental prune cannot see below the cursor",
        );

        // Drive past the re-baseline pass; the full scan reclaims it.
        for _ in 0..PRUNE_FULL_SCAN_EVERY {
            backend.prune_before(3).await.unwrap();
        }
        assert!(backend.read_partial(attempt(1), 0).await.unwrap().is_none());
    }
}
