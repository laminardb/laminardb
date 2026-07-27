//! [`InProcessBackend`] — non-durable checkpoint-artifact storage backed by
//! an in-memory hashmap. Used for tests and embedded single-process runs.

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;

use crate::checkpoint::{CheckpointAssignmentFence, LeaderProof, PipelineIdentity};

use super::backend::{
    digest_hex, sha256, CheckpointAttempt, CheckpointSeal, CheckpointSealInventory,
    SealedCommitDescriptor, SealedCommitDescriptorWriter, SealedVnodePartial, SealedVnodeWriter,
    StateBackend, StateBackendError, StateNamespaceBinding, STATE_NAMESPACE_RESOURCE,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredPartial {
    bytes: Bytes,
    attestation: SealedVnodePartial,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredCommitDescriptor {
    bytes: Bytes,
    attestation: SealedCommitDescriptor,
}

/// In-process, non-durable checkpoint-artifact backend.
#[derive(Debug)]
pub struct InProcessBackend {
    namespace: RwLock<Option<StateNamespaceBinding>>,
    /// Monotonic retirement boundary. Attempt operations hold a read guard while touching their
    /// maps; pruning takes the write guard before publishing a new floor and deleting entries.
    retired_before_epoch: RwLock<u64>,
    partials: RwLock<FxHashMap<(CheckpointAttempt, u32), StoredPartial>>,
    /// `attempt -> key -> descriptor`, mirroring the durable attempt namespace.
    descriptors: RwLock<FxHashMap<CheckpointAttempt, FxHashMap<String, StoredCommitDescriptor>>>,
    /// Exact attempts sealed by [`StateBackend::seal_checkpoint`].
    sealed: RwLock<FxHashMap<CheckpointAttempt, CheckpointSeal>>,
    execution_id: uuid::Uuid,
    vnode_capacity: u32,
}

impl InProcessBackend {
    /// Create a new backend sized for `vnode_capacity` vnodes.
    #[must_use]
    pub fn new(vnode_capacity: u32) -> Self {
        Self {
            namespace: RwLock::new(None),
            retired_before_epoch: RwLock::new(0),
            partials: RwLock::new(FxHashMap::default()),
            descriptors: RwLock::new(FxHashMap::default()),
            sealed: RwLock::new(FxHashMap::default()),
            execution_id: uuid::Uuid::new_v4(),
            vnode_capacity,
        }
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

    fn ensure_canonical_attempt(attempt: CheckpointAttempt) -> Result<(), StateBackendError> {
        if attempt.is_canonical() {
            Ok(())
        } else {
            Err(StateBackendError::Conflict {
                resource: format!(
                    "state-v2/epoch={}/checkpoint={}",
                    attempt.epoch, attempt.checkpoint_id
                ),
                message: "state attempt must use one nonzero canonical checkpoint ID".into(),
            })
        }
    }

    fn live_attempt_guard(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<parking_lot::RwLockReadGuard<'_, u64>, StateBackendError> {
        Self::ensure_canonical_attempt(attempt)?;
        let guard = self.retired_before_epoch.read();
        if attempt.epoch < *guard {
            return Err(StateBackendError::Conflict {
                resource: format!(
                    "state-v2/epoch={}/checkpoint={}",
                    attempt.epoch, attempt.checkpoint_id
                ),
                message: format!(
                    "checkpoint epoch {} is below the irreversible in-process prune floor {}",
                    attempt.epoch, *guard
                ),
            });
        }
        Ok(guard)
    }

    fn readable_attempt_guard(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Option<parking_lot::RwLockReadGuard<'_, u64>>, StateBackendError> {
        Self::ensure_canonical_attempt(attempt)?;
        let guard = self.retired_before_epoch.read();
        Ok((attempt.epoch >= *guard).then_some(guard))
    }

    fn store_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_version: u64,
        writer: Option<SealedVnodeWriter>,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        let _live_attempt = self.live_attempt_guard(attempt)?;
        self.check_vnode(vnode)?;
        let stored = StoredPartial {
            attestation: SealedVnodePartial::new(vnode, assignment_version, writer, &bytes),
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
                message: "partial already exists with different bytes or writer certificate".into(),
            }),
            None => {
                partials.insert((attempt, vnode), stored);
                Ok(())
            }
        }
    }

    fn store_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
        assignment_version: u64,
        writer: Option<SealedCommitDescriptorWriter>,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        let _live_attempt = self.live_attempt_guard(attempt)?;
        if key.is_empty() {
            return Err(StateBackendError::Conflict {
                resource: format!(
                    "state-v2/epoch={}/checkpoint={}/commit/",
                    attempt.epoch, attempt.checkpoint_id
                ),
                message: "commit descriptor key is empty".into(),
            });
        }
        let stored = StoredCommitDescriptor {
            attestation: SealedCommitDescriptor::new(
                key.to_owned(),
                assignment_version,
                writer,
                &bytes,
            ),
            bytes,
        };
        let mut descriptors = self.descriptors.write();
        let attempt_descriptors = descriptors.entry(attempt).or_default();
        match attempt_descriptors.get(key) {
            Some(existing) if existing == &stored => Ok(()),
            Some(_) => Err(StateBackendError::Conflict {
                resource: format!(
                    "state-v2/epoch={}/checkpoint={}/commit/{key}",
                    attempt.epoch, attempt.checkpoint_id
                ),
                message:
                    "commit descriptor already exists with different bytes or writer certificate"
                        .into(),
            }),
            None => {
                attempt_descriptors.insert(key.to_owned(), stored);
                Ok(())
            }
        }
    }
}

#[async_trait]
impl StateBackend for InProcessBackend {
    fn key_group_capacity(&self) -> u32 {
        self.vnode_capacity
    }

    async fn bind_state_namespace(
        &self,
        deployment_id: &str,
        pipeline_identity: &PipelineIdentity,
    ) -> Result<(), StateBackendError> {
        let requested = StateNamespaceBinding::try_new(deployment_id, pipeline_identity)?;
        let mut namespace = self.namespace.write();
        match namespace.as_ref() {
            Some(existing) if existing == &requested => Ok(()),
            Some(_) => Err(StateBackendError::Conflict {
                resource: STATE_NAMESPACE_RESOURCE.into(),
                message: "in-process state backend is already bound to a different namespace"
                    .into(),
            }),
            None => {
                *namespace = Some(requested);
                Ok(())
            }
        }
    }

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
        self.store_partial(attempt, vnode, assignment_version, None, bytes)
    }

    async fn write_certified_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_fence: &CheckpointAssignmentFence,
        writer_node_id: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        let writer =
            SealedVnodeWriter::from_fence(assignment_fence, writer_node_id).ok_or_else(|| {
                StateBackendError::Conflict {
                    resource: format!(
                        "state-v2/epoch={}/checkpoint={}/vnode={vnode}/partial.bin",
                        attempt.epoch, attempt.checkpoint_id
                    ),
                    message: "partial writer is absent from the canonical assignment certificate"
                        .into(),
                }
            })?;
        if !assignment_fence.is_canonical() {
            return Err(StateBackendError::Conflict {
                resource: format!(
                    "state-v2/epoch={}/checkpoint={}/vnode={vnode}/partial.bin",
                    attempt.epoch, attempt.checkpoint_id
                ),
                message: "assignment certificate is not canonical".into(),
            });
        }
        self.store_partial(
            attempt,
            vnode,
            assignment_fence.assignment_version,
            Some(writer),
            bytes,
        )
    }

    async fn read_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
    ) -> Result<Option<Bytes>, StateBackendError> {
        let Some(_live_attempt) = self.readable_attempt_guard(attempt)? else {
            return Ok(None);
        };
        self.check_vnode(vnode)?;
        Ok(self
            .partials
            .read()
            .get(&(attempt, vnode))
            .map(|partial| partial.bytes.clone()))
    }

    async fn read_sealed_partial_bounded(
        &self,
        attempt: CheckpointAttempt,
        sealed: &SealedVnodePartial,
        max_bytes: u64,
    ) -> Result<Option<Bytes>, StateBackendError> {
        let Some(_live_attempt) = self.readable_attempt_guard(attempt)? else {
            return Ok(None);
        };
        self.check_vnode(sealed.vnode)?;
        let resource = format!(
            "state-v2/epoch={}/checkpoint={}/vnode={}/partial.bin",
            attempt.epoch, attempt.checkpoint_id, sealed.vnode
        );
        if sealed.payload_len > max_bytes {
            return Err(StateBackendError::Conflict {
                resource,
                message: format!(
                    "sealed vnode partial declares {} bytes; read bound is {max_bytes}",
                    sealed.payload_len
                ),
            });
        }
        let partials = self.partials.read();
        let Some(stored) = partials.get(&(attempt, sealed.vnode)) else {
            return Ok(None);
        };
        if &stored.attestation != sealed {
            return Err(StateBackendError::Conflict {
                resource,
                message: "stored vnode partial attestation does not match the checkpoint seal"
                    .into(),
            });
        }
        if stored.bytes.len() as u64 != sealed.payload_len
            || digest_hex(&sha256(&stored.bytes)) != sealed.payload_sha256
        {
            return Err(StateBackendError::Serialization(
                "stored vnode partial payload does not match its sealed length and digest".into(),
            ));
        }
        Ok(Some(stored.bytes.clone()))
    }

    async fn write_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.store_commit_descriptor(attempt, key, 0, None, bytes)
    }

    async fn write_certified_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
        assignment_fence: &CheckpointAssignmentFence,
        writer_node_id: u64,
        leader_proof: &LeaderProof,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        let writer = SealedCommitDescriptorWriter::from_fence(
            assignment_fence,
            writer_node_id,
            leader_proof,
        )
        .ok_or_else(|| StateBackendError::Conflict {
            resource: format!(
                "state-v2/epoch={}/checkpoint={}/commit/{key}",
                attempt.epoch, attempt.checkpoint_id
            ),
            message: "commit descriptor writer or leader is absent from the canonical assignment certificate"
                .into(),
        })?;
        self.store_commit_descriptor(
            attempt,
            key,
            assignment_fence.assignment_version,
            Some(writer),
            bytes,
        )
    }

    async fn read_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
    ) -> Result<Option<Bytes>, StateBackendError> {
        let Some(_live_attempt) = self.readable_attempt_guard(attempt)? else {
            return Ok(None);
        };
        Ok(self
            .descriptors
            .read()
            .get(&attempt)
            .and_then(|descriptors| descriptors.get(key))
            .map(|descriptor| descriptor.bytes.clone()))
    }

    async fn read_sealed_commit_descriptor_bounded(
        &self,
        attempt: CheckpointAttempt,
        sealed: &SealedCommitDescriptor,
        max_bytes: u64,
    ) -> Result<Option<Bytes>, StateBackendError> {
        let Some(_live_attempt) = self.readable_attempt_guard(attempt)? else {
            return Ok(None);
        };
        let resource = format!(
            "state-v2/epoch={}/checkpoint={}/commit/{}",
            attempt.epoch, attempt.checkpoint_id, sealed.key
        );
        if sealed.payload_len > max_bytes {
            return Err(StateBackendError::Conflict {
                resource,
                message: format!(
                    "sealed commit descriptor declares {} bytes; read bound is {max_bytes}",
                    sealed.payload_len
                ),
            });
        }
        let descriptors = self.descriptors.read();
        let Some(stored) = descriptors
            .get(&attempt)
            .and_then(|descriptors| descriptors.get(&sealed.key))
        else {
            return Ok(None);
        };
        if &stored.attestation != sealed {
            return Err(StateBackendError::Conflict {
                resource,
                message: "stored commit descriptor attestation does not match the checkpoint seal"
                    .into(),
            });
        }
        if stored.bytes.len() as u64 != sealed.payload_len
            || digest_hex(&sha256(&stored.bytes)) != sealed.payload_sha256
        {
            return Err(StateBackendError::Serialization(
                "stored commit descriptor payload does not match its sealed length and digest"
                    .into(),
            ));
        }
        Ok(Some(stored.bytes.clone()))
    }

    async fn seal_checkpoint(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&CheckpointAssignmentFence>,
        vnodes: &[u32],
        required_descriptors: &[String],
    ) -> Result<bool, StateBackendError> {
        let _live_attempt = self.live_attempt_guard(attempt)?;
        if assignment_fence.is_some_and(|fence| !fence.is_canonical()) {
            return Err(StateBackendError::Conflict {
                resource: format!(
                    "state-v2/epoch={}/checkpoint={}/_SEAL",
                    attempt.epoch, attempt.checkpoint_id
                ),
                message: "assignment certificate is not canonical".into(),
            });
        }
        let assignment_version = assignment_fence.map_or(0, |fence| fence.assignment_version);
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
        let sealed_descriptors = {
            let descs = self.descriptors.read();
            let attempt_descs = descs.get(&attempt);
            let mut sealed_descriptors = Vec::with_capacity(required_descriptors.len());
            for key in required_descriptors {
                let Some(descriptor) = attempt_descs.and_then(|items| items.get(key)) else {
                    return Ok(false);
                };
                sealed_descriptors.push(descriptor.attestation.clone());
            }
            sealed_descriptors
        };
        let seal = CheckpointSeal::new(
            "in-process".to_string(),
            self.execution_id,
            CheckpointSealInventory {
                attempt,
                assignment_fence: assignment_fence.cloned(),
                assignment_version,
                required_vnodes: vnodes.to_vec(),
                sealed_partials,
                required_descriptors: required_descriptors.to_vec(),
                sealed_descriptors,
            },
        );
        seal.validate()
            .map_err(|message| StateBackendError::Conflict {
                resource: format!(
                    "state-v2/epoch={}/checkpoint={}/_SEAL",
                    attempt.epoch, attempt.checkpoint_id
                ),
                message,
            })?;
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

    async fn checkpoint_seal_inventory(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Option<CheckpointSealInventory>, StateBackendError> {
        let Some(_live_attempt) = self.readable_attempt_guard(attempt)? else {
            return Ok(None);
        };
        Ok(self
            .sealed
            .read()
            .get(&attempt)
            .map(CheckpointSeal::inventory))
    }

    async fn verify_checkpoint_artifact_metadata(
        &self,
        inventory: &CheckpointSealInventory,
    ) -> Result<(), StateBackendError> {
        let attempt = inventory.attempt;
        let _live_attempt = self.live_attempt_guard(attempt)?;
        let resource = format!(
            "state-v2/epoch={}/checkpoint={}",
            attempt.epoch, attempt.checkpoint_id
        );
        let sealed = self.sealed.read();
        let Some(seal) = sealed.get(&attempt) else {
            return Err(StateBackendError::Conflict {
                resource,
                message: "checkpoint seal is missing during metadata verification".into(),
            });
        };
        if seal.inventory() != *inventory {
            return Err(StateBackendError::Conflict {
                resource,
                message: "checkpoint seal changed during metadata verification".into(),
            });
        }

        let partials = self.partials.read();
        for expected in &inventory.sealed_partials {
            let path = format!(
                "state-v2/epoch={}/checkpoint={}/vnode={}/partial.bin",
                attempt.epoch, attempt.checkpoint_id, expected.vnode
            );
            let Some(stored) = partials.get(&(attempt, expected.vnode)) else {
                return Err(StateBackendError::Conflict {
                    resource: path,
                    message: "sealed vnode partial is missing".into(),
                });
            };
            let stored_len =
                u64::try_from(stored.bytes.len()).map_err(|_| StateBackendError::Conflict {
                    resource: path.clone(),
                    message: "sealed vnode partial length is not representable".into(),
                })?;
            if stored.attestation != *expected || stored_len != expected.payload_len {
                return Err(StateBackendError::Conflict {
                    resource: path,
                    message: "sealed vnode partial metadata does not match the seal".into(),
                });
            }
        }

        let descriptors = self.descriptors.read();
        let attempt_descriptors = descriptors.get(&attempt);
        for expected in &inventory.sealed_descriptors {
            let path = format!(
                "state-v2/epoch={}/checkpoint={}/commit/{}",
                attempt.epoch, attempt.checkpoint_id, expected.key
            );
            let Some(stored) = attempt_descriptors.and_then(|entries| entries.get(&expected.key))
            else {
                return Err(StateBackendError::Conflict {
                    resource: path,
                    message: "sealed commit descriptor is missing".into(),
                });
            };
            let stored_len =
                u64::try_from(stored.bytes.len()).map_err(|_| StateBackendError::Conflict {
                    resource: path.clone(),
                    message: "sealed commit descriptor length is not representable".into(),
                })?;
            if stored.attestation != *expected || stored_len != expected.payload_len {
                return Err(StateBackendError::Conflict {
                    resource: path,
                    message: "sealed commit descriptor metadata does not match the seal".into(),
                });
            }
        }
        Ok(())
    }

    async fn prune_before(&self, before: u64) -> Result<(), StateBackendError> {
        // Publish the correctness boundary while excluding every attempt operation, then delete.
        // No writer can pass the old floor and republish an attempt after this sweep.
        let mut floor = self.retired_before_epoch.write();
        *floor = (*floor).max(before);
        let before = *floor;
        self.sealed
            .write()
            .retain(|attempt, _| attempt.epoch >= before);
        self.partials
            .write()
            .retain(|&(attempt, _), _| attempt.epoch >= before);
        self.descriptors
            .write()
            .retain(|attempt, _| attempt.epoch >= before);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::StateBackendDurability;

    fn attempt(checkpoint_id: u64) -> CheckpointAttempt {
        CheckpointAttempt::canonical(checkpoint_id)
    }

    #[tokio::test]
    async fn namespace_binding_is_create_once_and_idempotent() {
        let backend = InProcessBackend::new(1);
        let deployment = uuid::Uuid::from_u128(1).to_string();
        let identity = crate::checkpoint::PipelineIdentity::empty();

        backend
            .bind_state_namespace(&deployment, &identity)
            .await
            .unwrap();
        backend
            .bind_state_namespace(&deployment, &identity)
            .await
            .unwrap();

        let other_deployment = uuid::Uuid::from_u128(2).to_string();
        assert!(matches!(
            backend
                .bind_state_namespace(&other_deployment, &identity)
                .await,
            Err(StateBackendError::Conflict { .. })
        ));
        let mut other_identity = identity;
        other_identity.sha256 = "11".repeat(32);
        assert!(matches!(
            backend
                .bind_state_namespace(&deployment, &other_identity)
                .await,
            Err(StateBackendError::Conflict { .. })
        ));
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
    async fn sealed_partial_read_is_exact_and_bounded() {
        let backend = InProcessBackend::new(1);
        let checkpoint = attempt(7);
        backend
            .write_partial(checkpoint, 0, 0, Bytes::from_static(b"state"))
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(checkpoint, None, &[0], &[])
            .await
            .unwrap());
        let inventory = backend
            .checkpoint_seal_inventory(checkpoint)
            .await
            .unwrap()
            .unwrap();
        let sealed = &inventory.sealed_partials[0];

        assert_eq!(
            backend
                .read_sealed_partial_bounded(checkpoint, sealed, 5)
                .await
                .unwrap(),
            Some(Bytes::from_static(b"state"))
        );

        let mut wrong_provenance = sealed.clone();
        wrong_provenance.assignment_version = 1;
        let error = backend
            .read_sealed_partial_bounded(checkpoint, &wrong_provenance, 5)
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error
            .to_string()
            .contains("does not match the checkpoint seal"));

        let error = backend
            .read_sealed_partial_bounded(checkpoint, sealed, 4)
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("read bound is 4"));
    }

    #[tokio::test]
    async fn noncanonical_attempt_is_rejected_before_state_mutation() {
        let backend = InProcessBackend::new(1);
        let invalid = CheckpointAttempt::new(1, 2);

        let error = backend
            .write_partial(invalid, 0, 0, Bytes::from_static(b"invalid"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("canonical checkpoint ID"));
        assert!(backend.partials.read().is_empty());
        assert!(backend.read_partial(invalid, 0).await.is_err());
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
        b.write_commit_descriptor(checkpoint, "sink", Bytes::from_static(b"d"))
            .await
            .unwrap();
        b.write_commit_descriptor(checkpoint, "sink", Bytes::from_static(b"d"))
            .await
            .unwrap();
        assert!(matches!(
            b.write_commit_descriptor(checkpoint, "sink", Bytes::from_static(b"other"))
                .await,
            Err(StateBackendError::Conflict { .. })
        ));
        assert_eq!(
            b.read_commit_descriptor(checkpoint, "sink").await.unwrap(),
            Some(Bytes::from_static(b"d"))
        );
        assert_eq!(
            b.read_commit_descriptor(checkpoint, "missing")
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn cluster_seal_requires_exact_descriptor_process_and_leader() {
        use crate::checkpoint::{CheckpointParticipant, LeaderProofOwner};

        let boot = uuid::Uuid::from_u128(1);
        let fence = CheckpointAssignmentFence::from_owner_map(
            1,
            &[1],
            vec![CheckpointParticipant {
                node_id: 1,
                boot_incarnation: boot,
            }],
        )
        .unwrap();
        let proof = LeaderProof {
            owner: LeaderProofOwner {
                node_id: 1,
                boot_id: boot,
                process_term: 1,
            },
            fencing_token: 1,
        };
        let key = "participant=1/ready";

        let poisoned = InProcessBackend::new(1);
        poisoned
            .write_commit_descriptor(attempt(3), key, Bytes::from_static(b"stale"))
            .await
            .unwrap();
        assert!(matches!(
            poisoned
                .seal_checkpoint(attempt(3), Some(&fence), &[], &[key.to_owned()])
                .await,
            Err(StateBackendError::Conflict { .. })
        ));

        let valid = InProcessBackend::new(1);
        valid
            .write_certified_commit_descriptor(
                attempt(4),
                key,
                &fence,
                1,
                &proof,
                Bytes::from_static(b"ready"),
            )
            .await
            .unwrap();
        assert!(valid
            .seal_checkpoint(attempt(4), Some(&fence), &[], &[key.to_owned()])
            .await
            .unwrap());
        let inventory = valid
            .checkpoint_seal_inventory(attempt(4))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(inventory.descriptor_leader_proof().unwrap(), Some(&proof));
        let sealed = inventory.sealed_descriptor(key).unwrap();
        assert_eq!(
            valid
                .read_sealed_commit_descriptor_bounded(attempt(4), sealed, 5)
                .await
                .unwrap(),
            Some(Bytes::from_static(b"ready"))
        );

        let mut wrong_attestation = sealed.clone();
        wrong_attestation.payload_sha256 = "00".repeat(32);
        let error = valid
            .read_sealed_commit_descriptor_bounded(attempt(4), &wrong_attestation, 5)
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error
            .to_string()
            .contains("does not match the checkpoint seal"));

        let error = valid
            .read_sealed_commit_descriptor_bounded(attempt(4), sealed, 4)
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("read bound is 4"));
    }

    #[tokio::test]
    async fn checkpoint_attempts_are_isolated() {
        let b = InProcessBackend::new(2);
        let old = CheckpointAttempt::canonical(5);
        let new = CheckpointAttempt::canonical(99);
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
            .seal_checkpoint(checkpoint, None, &vnodes, &[])
            .await
            .unwrap());
        b.write_partial(checkpoint, 0, 0, Bytes::from_static(b"a"))
            .await
            .unwrap();
        b.write_partial(checkpoint, 1, 0, Bytes::from_static(b"b"))
            .await
            .unwrap();
        assert!(!b
            .seal_checkpoint(checkpoint, None, &vnodes, &[])
            .await
            .unwrap());
        b.write_partial(checkpoint, 2, 0, Bytes::from_static(b"c"))
            .await
            .unwrap();
        assert!(b
            .seal_checkpoint(checkpoint, None, &vnodes, &[])
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
        assert!(inventory.sealed_descriptors.is_empty());
        assert_eq!(inventory.sealed_partials.len(), vnodes.len());
        assert!(inventory.sealed_partials.iter().all(|partial| {
            partial.assignment_version == 0
                && partial.payload_len == 1
                && partial.payload_sha256.len() == 64
        }));
        let error = b
            .seal_checkpoint(checkpoint, None, &[0, 1], &[])
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(!b
            .seal_checkpoint(attempt(2), None, &vnodes, &[])
            .await
            .unwrap());
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

    #[tokio::test]
    async fn prune_floor_irreversibly_retires_attempt_name() {
        let b = InProcessBackend::new(1);
        let retired = attempt(1);
        b.write_partial(retired, 0, 0, Bytes::from_static(b"original"))
            .await
            .unwrap();
        b.write_commit_descriptor(retired, "ready", Bytes::from_static(b"ready"))
            .await
            .unwrap();
        assert!(b
            .seal_checkpoint(retired, None, &[0], &["ready".into()])
            .await
            .unwrap());
        let sealed = b.checkpoint_seal_inventory(retired).await.unwrap().unwrap();

        b.prune_before(2).await.unwrap();
        b.prune_before(1).await.unwrap();

        assert!(b
            .checkpoint_seal_inventory(retired)
            .await
            .unwrap()
            .is_none());
        assert!(b
            .read_sealed_partial_bounded(retired, &sealed.sealed_partials[0], u64::MAX)
            .await
            .unwrap()
            .is_none());
        assert!(b
            .read_sealed_commit_descriptor_bounded(retired, &sealed.sealed_descriptors[0], u64::MAX)
            .await
            .unwrap()
            .is_none());
        let retry_error = b
            .write_partial(retired, 0, 0, Bytes::from_static(b"original"))
            .await
            .unwrap_err();
        assert!(matches!(retry_error, StateBackendError::Conflict { .. }));
        let write_error = b
            .write_partial(retired, 0, 0, Bytes::from_static(b"replacement"))
            .await
            .unwrap_err();
        assert!(matches!(write_error, StateBackendError::Conflict { .. }));
        let descriptor_error = b
            .write_commit_descriptor(retired, "ready", Bytes::from_static(b"ready"))
            .await
            .unwrap_err();
        assert!(matches!(
            descriptor_error,
            StateBackendError::Conflict { .. }
        ));
        let seal_error = b
            .seal_checkpoint(retired, None, &[0], &["ready".into()])
            .await
            .unwrap_err();
        assert!(matches!(seal_error, StateBackendError::Conflict { .. }));

        b.write_partial(attempt(2), 0, 0, Bytes::from_static(b"live"))
            .await
            .unwrap();
    }
}
