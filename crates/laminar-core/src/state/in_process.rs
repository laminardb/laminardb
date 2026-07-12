//! [`InProcessBackend`] — non-durable [`StateBackend`] backed by an
//! in-memory hashmap. Used for tests and embedded single-process runs.

use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;

use super::backend::{
    CheckpointAttempt, CheckpointSeal, CheckpointSealInventory, SealedVnodePartial, StateBackend,
    StateBackendError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredPartial {
    bytes: Bytes,
    attestation: SealedVnodePartial,
}

/// In-process, non-durable state backend.
#[derive(Debug)]
pub struct InProcessBackend {
    partials: RwLock<FxHashMap<(CheckpointAttempt, u32), StoredPartial>>,
    /// `attempt -> key -> descriptor`, mirroring the durable attempt namespace.
    descriptors: RwLock<FxHashMap<CheckpointAttempt, FxHashMap<String, Bytes>>>,
    /// Exact attempts sealed by [`StateBackend::seal_checkpoint`].
    sealed: RwLock<BTreeMap<CheckpointAttempt, CheckpointSeal>>,
    execution_id: uuid::Uuid,
    vnode_capacity: u32,
}

impl InProcessBackend {
    /// Create a new backend sized for `vnode_capacity` vnodes.
    #[must_use]
    pub fn new(vnode_capacity: u32) -> Self {
        Self {
            partials: RwLock::new(FxHashMap::default()),
            descriptors: RwLock::new(FxHashMap::default()),
            sealed: RwLock::new(BTreeMap::new()),
            execution_id: uuid::Uuid::new_v4(),
            vnode_capacity,
        }
    }

    /// Vnode range this backend is configured for.
    #[must_use]
    pub fn vnode_capacity(&self) -> u32 {
        self.vnode_capacity
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
}

#[async_trait]
impl StateBackend for InProcessBackend {
    /// In-process backend opts out of the assignment fence — there's
    /// only one process so the scenario is moot. `assignment_version`
    /// is accepted and ignored.
    async fn write_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.check_vnode(vnode)?;
        let stored = StoredPartial {
            attestation: SealedVnodePartial::new(
                vnode,
                assignment_version,
                "in-process",
                self.execution_id,
                &bytes,
            ),
            bytes,
        };
        let mut partials = self.partials.write();
        match partials.get(&(attempt, vnode)) {
            Some(existing) if existing == &stored => Ok(()),
            Some(_) => Err(StateBackendError::Conflict {
                resource: format!(
                    "state-v2/epoch={}/checkpoint={}/vnode={vnode}/partial.bin",
                    attempt.epoch, attempt.checkpoint_id
                ),
                message: "partial already exists with different bytes".into(),
            }),
            None => {
                partials.insert((attempt, vnode), stored);
                Ok(())
            }
        }
    }

    async fn read_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
    ) -> Result<Option<Bytes>, StateBackendError> {
        self.check_vnode(vnode)?;
        Ok(self
            .partials
            .read()
            .get(&(attempt, vnode))
            .map(|partial| partial.bytes.clone()))
    }

    async fn write_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
        _assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        let mut descriptors = self.descriptors.write();
        let attempt_descriptors = descriptors.entry(attempt).or_default();
        match attempt_descriptors.get(key) {
            Some(existing) if existing == &bytes => Ok(()),
            Some(_) => Err(StateBackendError::Conflict {
                resource: format!(
                    "state-v2/epoch={}/checkpoint={}/commit/{key}",
                    attempt.epoch, attempt.checkpoint_id
                ),
                message: "commit descriptor already exists with different bytes".into(),
            }),
            None => {
                attempt_descriptors.insert(key.to_string(), bytes);
                Ok(())
            }
        }
    }

    async fn read_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
    ) -> Result<Option<Bytes>, StateBackendError> {
        Ok(self
            .descriptors
            .read()
            .get(&attempt)
            .and_then(|descriptors| descriptors.get(key))
            .cloned())
    }

    async fn read_commit_descriptors(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Vec<(String, Bytes)>, StateBackendError> {
        let mut descriptors: Vec<_> = self
            .descriptors
            .read()
            .get(&attempt)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        descriptors.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Ok(descriptors)
    }

    async fn seal_checkpoint(
        &self,
        attempt: CheckpointAttempt,
        assignment_version: u64,
        vnodes: &[u32],
        required_descriptors: &[String],
    ) -> Result<bool, StateBackendError> {
        let sealed_partials = {
            let map = self.partials.read();
            let mut sealed_partials = Vec::with_capacity(vnodes.len());
            for &v in vnodes {
                self.check_vnode(v)?;
                let Some(partial) = map.get(&(attempt, v)) else {
                    return Ok(false);
                };
                if partial.attestation.assignment_version != assignment_version {
                    return Err(StateBackendError::Conflict {
                        resource: format!(
                            "state-v2/epoch={}/checkpoint={}/vnode={v}/partial.bin",
                            attempt.epoch, attempt.checkpoint_id
                        ),
                        message: format!(
                            "partial assignment version {} cannot satisfy seal version {assignment_version}",
                            partial.attestation.assignment_version
                        ),
                    });
                }
                sealed_partials.push(partial.attestation.clone());
            }
            sealed_partials
        };
        {
            let descs = self.descriptors.read();
            let attempt_descs = descs.get(&attempt);
            for key in required_descriptors {
                if !attempt_descs.is_some_and(|m| m.contains_key(key)) {
                    return Ok(false);
                }
            }
        }
        let seal = CheckpointSeal::new(
            attempt,
            "in-process".to_string(),
            self.execution_id,
            assignment_version,
            vnodes,
            &sealed_partials,
            required_descriptors,
        );
        let mut sealed = self.sealed.write();
        match sealed.get(&attempt) {
            Some(existing) if existing == &seal => Ok(true),
            Some(existing) => Err(StateBackendError::Conflict {
                resource: format!(
                    "state-v2/epoch={}/checkpoint={}/_SEAL",
                    attempt.epoch, attempt.checkpoint_id
                ),
                message: format!(
                    "existing seal belongs to execution {}",
                    existing.execution_id
                ),
            }),
            None => {
                sealed.insert(attempt, seal);
                Ok(true)
            }
        }
    }

    async fn sealed_checkpoints(
        &self,
        after_checkpoint_id: u64,
    ) -> Result<Vec<CheckpointAttempt>, StateBackendError> {
        let mut attempts: Vec<_> = self
            .sealed
            .read()
            .keys()
            .filter(|attempt| attempt.checkpoint_id > after_checkpoint_id)
            .copied()
            .collect();
        attempts.sort_unstable_by_key(|attempt| attempt.checkpoint_id);
        Ok(attempts)
    }

    async fn checkpoint_seal_inventory(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Option<CheckpointSealInventory>, StateBackendError> {
        Ok(self
            .sealed
            .read()
            .get(&attempt)
            .map(CheckpointSeal::inventory))
    }

    async fn prune_before(&self, before: u64) -> Result<(), StateBackendError> {
        // Without this, every checkpoint leaks one Bytes per vnode
        // forever.
        self.partials
            .write()
            .retain(|&(attempt, _), _| attempt.epoch >= before);
        self.descriptors
            .write()
            .retain(|attempt, _| attempt.epoch >= before);
        self.sealed
            .write()
            .retain(|attempt, _| attempt.epoch >= before);
        Ok(())
    }

    async fn truncate_after(&self, after: u64) -> Result<(), StateBackendError> {
        self.partials
            .write()
            .retain(|&(attempt, _), _| attempt.epoch <= after);
        self.descriptors
            .write()
            .retain(|attempt, _| attempt.epoch <= after);
        self.sealed
            .write()
            .retain(|attempt, _| attempt.epoch <= after);
        Ok(())
    }

    async fn latest_sealed_checkpoint(
        &self,
    ) -> Result<Option<CheckpointAttempt>, StateBackendError> {
        Ok(self
            .sealed
            .read()
            .keys()
            .max_by_key(|attempt| attempt.checkpoint_id)
            .copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::StateBackendDurability;

    fn attempt(epoch: u64) -> CheckpointAttempt {
        CheckpointAttempt::new(epoch, epoch * 10)
    }

    #[tokio::test]
    async fn write_read_roundtrip() {
        let b = InProcessBackend::new(4);
        let checkpoint = attempt(7);
        let payload = Bytes::from_static(b"hello");
        b.write_partial(checkpoint, 2, 0, payload.clone())
            .await
            .unwrap();
        let got = b.read_partial(checkpoint, 2).await.unwrap().unwrap();
        assert_eq!(got, payload);
        assert!(b.read_partial(attempt(8), 2).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn immutable_partial_accepts_identical_retry_and_rejects_conflict() {
        let b = InProcessBackend::new(4);
        let checkpoint = attempt(1);
        b.write_partial(checkpoint, 0, 0, Bytes::from_static(b"first"))
            .await
            .unwrap();
        b.write_partial(checkpoint, 0, 0, Bytes::from_static(b"first"))
            .await
            .unwrap();
        assert!(matches!(
            b.write_partial(checkpoint, 0, 0, Bytes::from_static(b"different"))
                .await,
            Err(StateBackendError::Conflict { .. })
        ));
        assert_eq!(
            b.read_partial(checkpoint, 0).await.unwrap().unwrap(),
            Bytes::from_static(b"first")
        );
    }

    #[tokio::test]
    async fn descriptors_are_immutable_per_attempt() {
        let b = InProcessBackend::new(2);
        let checkpoint = attempt(2);
        b.write_commit_descriptor(checkpoint, "sink", 0, Bytes::from_static(b"d"))
            .await
            .unwrap();
        b.write_commit_descriptor(checkpoint, "sink", 0, Bytes::from_static(b"d"))
            .await
            .unwrap();
        assert!(matches!(
            b.write_commit_descriptor(checkpoint, "sink", 0, Bytes::from_static(b"other"))
                .await,
            Err(StateBackendError::Conflict { .. })
        ));
    }

    #[tokio::test]
    async fn reused_epoch_isolated_by_checkpoint_id() {
        let b = InProcessBackend::new(2);
        let old = CheckpointAttempt::new(5, 50);
        let new = CheckpointAttempt::new(5, 99);
        b.write_partial(old, 0, 0, Bytes::from_static(b"old"))
            .await
            .unwrap();
        b.write_partial(new, 0, 0, Bytes::from_static(b"new"))
            .await
            .unwrap();
        assert_eq!(
            b.read_partial(old, 0).await.unwrap().unwrap(),
            Bytes::from_static(b"old")
        );
        assert_eq!(
            b.read_partial(new, 0).await.unwrap().unwrap(),
            Bytes::from_static(b"new")
        );
    }

    #[test]
    fn in_process_backend_is_volatile() {
        assert_eq!(
            InProcessBackend::new(4).durability_scope(),
            StateBackendDurability::Volatile
        );
    }

    #[tokio::test]
    async fn seal_checkpoint_requires_every_vnode() {
        let b = InProcessBackend::new(4);
        let checkpoint = attempt(1);
        let vnodes = [0u32, 1, 2];
        assert!(!b
            .seal_checkpoint(checkpoint, 0, &vnodes, &[])
            .await
            .unwrap());
        b.write_partial(checkpoint, 0, 0, Bytes::from_static(b"a"))
            .await
            .unwrap();
        b.write_partial(checkpoint, 1, 0, Bytes::from_static(b"b"))
            .await
            .unwrap();
        assert!(!b
            .seal_checkpoint(checkpoint, 0, &vnodes, &[])
            .await
            .unwrap());
        b.write_partial(checkpoint, 2, 0, Bytes::from_static(b"c"))
            .await
            .unwrap();
        assert!(b
            .seal_checkpoint(checkpoint, 0, &vnodes, &[])
            .await
            .unwrap());
        let inventory = b
            .checkpoint_seal_inventory(checkpoint)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(inventory.attempt, checkpoint);
        assert_eq!(inventory.required_vnodes, vnodes);
        assert_eq!(inventory.required_descriptors, Vec::<String>::new());
        assert_eq!(inventory.sealed_partials.len(), vnodes.len());
        assert!(inventory.sealed_partials.iter().all(|partial| {
            partial.assignment_version == 0
                && partial.payload_len == 1
                && partial.payload_sha256.len() == 64
        }));
        let error = b
            .seal_checkpoint(checkpoint, 0, &[0, 1], &[])
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(!b
            .seal_checkpoint(attempt(2), 0, &vnodes, &[])
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn latest_sealed_checkpoint_follows_attempt_id() {
        let b = InProcessBackend::new(4);
        let vnodes = [0u32, 1];
        assert_eq!(b.latest_sealed_checkpoint().await.unwrap(), None);

        let first = CheckpointAttempt::new(2, 20);
        b.write_partial(first, 0, 0, Bytes::from_static(b"a"))
            .await
            .unwrap();
        assert!(!b.seal_checkpoint(first, 0, &vnodes, &[]).await.unwrap());
        assert_eq!(b.latest_sealed_checkpoint().await.unwrap(), None);

        b.write_partial(first, 1, 0, Bytes::from_static(b"b"))
            .await
            .unwrap();
        assert!(b.seal_checkpoint(first, 0, &vnodes, &[]).await.unwrap());
        assert_eq!(b.latest_sealed_checkpoint().await.unwrap(), Some(first));

        let second = CheckpointAttempt::new(5, 50);
        for v in &vnodes {
            b.write_partial(second, *v, 0, Bytes::from_static(b"c"))
                .await
                .unwrap();
        }
        assert!(b.seal_checkpoint(second, 0, &vnodes, &[]).await.unwrap());
        assert_eq!(b.latest_sealed_checkpoint().await.unwrap(), Some(second));
        assert_eq!(b.sealed_checkpoints(20).await.unwrap(), vec![second]);
    }

    #[tokio::test]
    async fn out_of_range_vnode_errors() {
        let b = InProcessBackend::new(2);
        let r = b
            .write_partial(attempt(1), 5, 0, Bytes::from_static(b"x"))
            .await
            .unwrap_err();
        assert!(matches!(r, StateBackendError::Io(_)));
    }

    #[test]
    fn state_backend_is_object_safe() {
        let _: std::sync::Arc<dyn StateBackend> = std::sync::Arc::new(InProcessBackend::new(2));
    }

    #[tokio::test]
    async fn truncate_after_drops_newer_epochs_and_seals() {
        let b = InProcessBackend::new(4);
        let vnodes = [0u32];
        for epoch in 1..=5 {
            b.write_partial(attempt(epoch), 0, 0, Bytes::from_static(b"x"))
                .await
                .unwrap();
            assert!(b
                .seal_checkpoint(attempt(epoch), 0, &vnodes, &[])
                .await
                .unwrap());
        }
        assert_eq!(
            b.latest_sealed_checkpoint().await.unwrap(),
            Some(attempt(5))
        );

        b.truncate_after(3).await.unwrap();

        for epoch in 1..=3 {
            assert!(b.read_partial(attempt(epoch), 0).await.unwrap().is_some());
        }
        for epoch in 4..=5 {
            assert!(b.read_partial(attempt(epoch), 0).await.unwrap().is_none());
        }
        assert_eq!(
            b.latest_sealed_checkpoint().await.unwrap(),
            Some(attempt(3))
        );
    }

    #[tokio::test]
    async fn prune_before_drops_old_epochs() {
        let b = InProcessBackend::new(4);
        for epoch in 1..=5 {
            b.write_partial(attempt(epoch), 0, 0, Bytes::from_static(b"x"))
                .await
                .unwrap();
            b.write_partial(attempt(epoch), 1, 0, Bytes::from_static(b"y"))
                .await
                .unwrap();
        }
        // Retain epochs >= 4. Entries for 1,2,3 must go away.
        b.prune_before(4).await.unwrap();
        for epoch in 1..=3 {
            assert!(b.read_partial(attempt(epoch), 0).await.unwrap().is_none());
            assert!(b.read_partial(attempt(epoch), 1).await.unwrap().is_none());
        }
        for epoch in 4..=5 {
            assert!(b.read_partial(attempt(epoch), 0).await.unwrap().is_some());
            assert!(b.read_partial(attempt(epoch), 1).await.unwrap().is_some());
        }
    }
}
