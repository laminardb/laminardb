use std::sync::Arc;

use super::vnode_chains::{resolve_op_chain, LoadedVnodeChains, SealedVnodeChainReader};
use async_trait::async_trait;
use bytes::Bytes;
use laminar_core::state::{
    CheckpointAttempt, CheckpointSealInventory, InProcessBackend, ObjectStoreBackend, StateBackend,
    StateBackendDurability, StateBackendError,
};
use sha2::{Digest, Sha256};

struct LegacyPartialReadBackend {
    inner: InProcessBackend,
}

#[cfg(feature = "cluster")]
struct ObservingBackend {
    inner: InProcessBackend,
    substituted_parent: Option<CheckpointAttempt>,
    body_reads: parking_lot::Mutex<Vec<CheckpointAttempt>>,
}

#[cfg(feature = "cluster")]
impl ObservingBackend {
    fn new(inner: InProcessBackend, substituted_parent: Option<CheckpointAttempt>) -> Self {
        Self {
            inner,
            substituted_parent,
            body_reads: parking_lot::Mutex::new(Vec::new()),
        }
    }

    fn body_reads(&self) -> Vec<CheckpointAttempt> {
        self.body_reads.lock().clone()
    }
}

#[async_trait]
impl StateBackend for LegacyPartialReadBackend {
    fn key_group_capacity(&self) -> u32 {
        self.inner.key_group_capacity()
    }

    async fn write_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.inner
            .write_partial(attempt, vnode, assignment_version, bytes)
            .await
    }

    async fn read_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
    ) -> Result<Option<Bytes>, StateBackendError> {
        self.inner.read_partial(attempt, vnode).await
    }

    async fn write_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.inner
            .write_commit_descriptor(attempt, key, bytes)
            .await
    }

    async fn read_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
    ) -> Result<Option<Bytes>, StateBackendError> {
        self.inner.read_commit_descriptor(attempt, key).await
    }

    async fn read_sealed_commit_descriptor_bounded(
        &self,
        attempt: CheckpointAttempt,
        sealed: &laminar_core::state::SealedCommitDescriptor,
        max_bytes: u64,
    ) -> Result<Option<Bytes>, StateBackendError> {
        self.inner
            .read_sealed_commit_descriptor_bounded(attempt, sealed, max_bytes)
            .await
    }

    async fn seal_checkpoint(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&laminar_core::checkpoint::CheckpointAssignmentFence>,
        vnodes: &[u32],
        required_descriptors: &[String],
    ) -> Result<bool, StateBackendError> {
        self.inner
            .seal_checkpoint(attempt, assignment_fence, vnodes, required_descriptors)
            .await
    }

    async fn checkpoint_seal_inventory(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Option<CheckpointSealInventory>, StateBackendError> {
        self.inner.checkpoint_seal_inventory(attempt).await
    }

    async fn verify_checkpoint_artifact_metadata(
        &self,
        inventory: &CheckpointSealInventory,
    ) -> Result<(), StateBackendError> {
        self.inner
            .verify_checkpoint_artifact_metadata(inventory)
            .await
    }

    async fn prune_before(&self, before: u64) -> Result<(), StateBackendError> {
        self.inner.prune_before(before).await
    }

    fn durability_scope(&self) -> StateBackendDurability {
        self.inner.durability_scope()
    }
}

#[cfg(feature = "cluster")]
#[async_trait]
impl StateBackend for ObservingBackend {
    fn key_group_capacity(&self) -> u32 {
        self.inner.key_group_capacity()
    }

    async fn write_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.inner
            .write_partial(attempt, vnode, assignment_version, bytes)
            .await
    }

    async fn read_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
    ) -> Result<Option<Bytes>, StateBackendError> {
        self.inner.read_partial(attempt, vnode).await
    }

    async fn read_sealed_partial_bounded(
        &self,
        attempt: CheckpointAttempt,
        sealed: &laminar_core::state::SealedVnodePartial,
        max_bytes: u64,
    ) -> Result<Option<Bytes>, StateBackendError> {
        self.body_reads.lock().push(attempt);
        self.inner
            .read_sealed_partial_bounded(attempt, sealed, max_bytes)
            .await
    }

    async fn write_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.inner
            .write_commit_descriptor(attempt, key, bytes)
            .await
    }

    async fn read_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
    ) -> Result<Option<Bytes>, StateBackendError> {
        self.inner.read_commit_descriptor(attempt, key).await
    }

    async fn read_sealed_commit_descriptor_bounded(
        &self,
        attempt: CheckpointAttempt,
        sealed: &laminar_core::state::SealedCommitDescriptor,
        max_bytes: u64,
    ) -> Result<Option<Bytes>, StateBackendError> {
        self.inner
            .read_sealed_commit_descriptor_bounded(attempt, sealed, max_bytes)
            .await
    }

    async fn seal_checkpoint(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&laminar_core::checkpoint::CheckpointAssignmentFence>,
        vnodes: &[u32],
        required_descriptors: &[String],
    ) -> Result<bool, StateBackendError> {
        self.inner
            .seal_checkpoint(attempt, assignment_fence, vnodes, required_descriptors)
            .await
    }

    async fn checkpoint_seal_inventory(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Option<CheckpointSealInventory>, StateBackendError> {
        let mut inventory = self.inner.checkpoint_seal_inventory(attempt).await?;
        if self.substituted_parent == Some(attempt) {
            if let Some(sealed) = inventory
                .as_mut()
                .and_then(|inventory| inventory.sealed_partials.first_mut())
            {
                sealed.payload_sha256 = if sealed.payload_sha256.starts_with("11") {
                    "22".repeat(32)
                } else {
                    "11".repeat(32)
                };
            }
        }
        Ok(inventory)
    }

    async fn verify_checkpoint_artifact_metadata(
        &self,
        inventory: &CheckpointSealInventory,
    ) -> Result<(), StateBackendError> {
        self.inner
            .verify_checkpoint_artifact_metadata(inventory)
            .await
    }

    async fn prune_before(&self, before: u64) -> Result<(), StateBackendError> {
        self.inner.prune_before(before).await
    }

    fn durability_scope(&self) -> StateBackendDurability {
        self.inner.durability_scope()
    }
}

async fn seal_epoch(backend: &dyn StateBackend, epoch: u64, vnodes: &[u32], tag: &[u8]) {
    let attempt = CheckpointAttempt::canonical(epoch);
    for &v in vnodes {
        let partial = crate::vnode_partial::VnodePartial {
            operators: vec![("agg".into(), tag.to_vec())],
            base: None,
            deltas: Vec::new(),
        };
        backend
            .write_partial(attempt, v, 0, Bytes::from(partial.encode().unwrap()))
            .await
            .unwrap();
    }
    assert!(backend
        .seal_checkpoint(attempt, None, vnodes, &[])
        .await
        .unwrap());
}

#[cfg(feature = "cluster")]
async fn write_and_seal_partial(
    backend: &dyn StateBackend,
    attempt: CheckpointAttempt,
    mut partial: crate::vnode_partial::VnodePartial,
    parent: Option<CheckpointAttempt>,
) {
    partial.base = match parent {
        Some(parent) => Some(sealed_parent_link(backend, parent, 0).await),
        None => None,
    };
    backend
        .write_partial(attempt, 0, 0, Bytes::from(partial.encode().unwrap()))
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(attempt, None, &[0], &[])
        .await
        .unwrap());
}

async fn sealed_parent_link(
    backend: &dyn StateBackend,
    attempt: CheckpointAttempt,
    vnode: u32,
) -> crate::vnode_partial::SealedVnodeParentLink {
    let inventory = backend
        .checkpoint_seal_inventory(attempt)
        .await
        .unwrap()
        .expect("parent attempt must have an exact seal");
    let sealed = inventory
        .sealed_vnode_partial(vnode)
        .expect("parent vnode must be attested by its seal");
    crate::vnode_partial::SealedVnodeParentLink::new(attempt, sealed).unwrap()
}

#[cfg(feature = "cluster")]
fn reader_from_inventory(
    backend: &dyn StateBackend,
    inventory: CheckpointSealInventory,
    max_payload_bytes: u64,
) -> SealedVnodeChainReader<'_> {
    let head =
        crate::checkpoint_coordinator::ValidatedVnodeRestoreHead::from_unchecked_inventory_for_test(
            inventory,
        );
    SealedVnodeChainReader::from_validated_head(backend, &head, max_payload_bytes).unwrap()
}

fn operator_payload(report: &LoadedVnodeChains, vnode: u32) -> Vec<u8> {
    let bytes = &report.chains.get(&vnode).unwrap()[0];
    let partial = crate::vnode_partial::VnodePartial::decode(bytes).unwrap();
    partial.operators[0].1.clone()
}

#[tokio::test]
async fn rehydrate_rejects_noncanonical_attempt_for_empty_vnode_set() {
    let backend = InProcessBackend::new(1);

    let error = SealedVnodeChainReader::new(&backend)
        .load_at(&[], CheckpointAttempt::new(7, 8))
        .await
        .unwrap_err();

    assert!(error.to_string().contains("canonical checkpoint ID"));
}

#[tokio::test]
async fn rehydrate_reads_committed_partials_and_rejects_missing_vnodes() {
    let backend = InProcessBackend::new(4);
    seal_epoch(&backend, 7, &[0, 1, 2], b"v7").await;

    let report = SealedVnodeChainReader::new(&backend)
        .load_at(&[0, 1], CheckpointAttempt::canonical(7))
        .await
        .unwrap();

    assert_eq!(report.attempt, Some(CheckpointAttempt::canonical(7)));
    assert_eq!(report.chain_count(), 2);
    assert_eq!(operator_payload(&report, 0), b"v7");
    assert_eq!(operator_payload(&report, 1), b"v7");

    let error = SealedVnodeChainReader::new(&backend)
        .load_at(&[3], CheckpointAttempt::canonical(7))
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("absent from the exact state seal"));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn validated_head_is_reused_and_rejects_attempt_substitution() {
    let backend = InProcessBackend::new(4);
    let committed = CheckpointAttempt::canonical(7);
    let substitute = CheckpointAttempt::canonical(8);
    seal_epoch(&backend, committed.epoch, &[0], b"committed").await;
    seal_epoch(&backend, substitute.epoch, &[0], b"substitute").await;
    let inventory = backend
        .checkpoint_seal_inventory(committed)
        .await
        .unwrap()
        .expect("committed attempt is sealed");
    let head =
        crate::checkpoint_coordinator::ValidatedVnodeRestoreHead::from_unchecked_inventory_for_test(
            inventory,
        );
    let expected_inventory = head.inventory();
    let reader = SealedVnodeChainReader::from_validated_head(&backend, &head, u64::MAX).unwrap();

    let cached_inventory = reader.sealed_inventory(committed).await.unwrap();
    assert!(
        Arc::ptr_eq(&cached_inventory, &expected_inventory),
        "the reader must retain the exact validated seal rather than reread an equal value"
    );
    let report = reader.load_at(&[0], committed).await.unwrap();
    assert_eq!(operator_payload(&report, 0), b"committed");

    let error = reader
        .load_at(&[0], substitute)
        .await
        .expect_err("a different sealed attempt cannot replace the validated head");
    assert!(error.to_string().contains("validated committed head"));
}

#[tokio::test]
async fn rehydrate_rejects_backend_without_sealed_bounded_partial_reads() {
    let attempt = CheckpointAttempt::canonical(7);
    let backend = LegacyPartialReadBackend {
        inner: InProcessBackend::new(4),
    };
    seal_epoch(&backend, 7, &[0], b"state").await;

    let error = SealedVnodeChainReader::new(&backend)
        .load_at(&[0], attempt)
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("cannot read checkpoint-sealed vnode partials"));
}

/// Boot recovery pins the read to the recovered manifest's epoch so state and source offsets
/// resume from one cut, even when a later epoch sealed.
#[tokio::test]
async fn load_at_pins_the_requested_epoch() {
    let backend = InProcessBackend::new(4);
    seal_epoch(&backend, 3, &[0, 1], b"old").await;
    seal_epoch(&backend, 9, &[0, 1], b"new").await;

    let report = SealedVnodeChainReader::new(&backend)
        .load_at(&[0, 1], CheckpointAttempt::canonical(3))
        .await
        .unwrap();

    assert_eq!(report.attempt, Some(CheckpointAttempt::canonical(3)));
    assert_eq!(operator_payload(&report, 0), b"old");
}

/// A reference partial resolves (one hop) to the
/// full partial it points at.
#[tokio::test]
async fn rehydrate_resolves_reference_partials() {
    let backend = InProcessBackend::new(4);

    let full = crate::vnode_partial::VnodePartial {
        operators: vec![("agg".into(), vec![1, 2, 3])],
        base: None,
        deltas: Vec::new(),
    };
    let full_attempt = CheckpointAttempt::canonical(5);
    backend
        .write_partial(full_attempt, 0, 0, Bytes::from(full.encode().unwrap()))
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(full_attempt, None, &[0], &[])
        .await
        .unwrap());
    let full_link = sealed_parent_link(&backend, full_attempt, 0).await;

    let reference = crate::vnode_partial::VnodePartial {
        operators: Vec::new(),
        base: Some(full_link),
        deltas: Vec::new(),
    };
    let reference_attempt = CheckpointAttempt::canonical(6);
    backend
        .write_partial(
            reference_attempt,
            0,
            0,
            Bytes::from(reference.encode().unwrap()),
        )
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(reference_attempt, None, &[0], &[])
        .await
        .unwrap());

    let report = SealedVnodeChainReader::new(&backend)
        .load_at(&[0], reference_attempt)
        .await
        .unwrap();
    assert_eq!(report.attempt, Some(reference_attempt));
    let chain = report.chains.get(&0).expect("vnode restored");
    assert_eq!(chain.len(), 1, "reference resolves to a single full base");
    let restored = crate::vnode_partial::VnodePartial::decode(&chain[0]).unwrap();
    assert_eq!(
        restored.base, None,
        "the resolved partial must be the full base, not the reference",
    );
    assert_eq!(restored.operators[0].1, vec![1, 2, 3]);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn rehydrate_resolves_sealed_full_delta_delta_chain() {
    let backend = InProcessBackend::new(1);
    let full_attempt = CheckpointAttempt::canonical(1);
    let delta_one_attempt = CheckpointAttempt::canonical(2);
    let delta_two_attempt = CheckpointAttempt::canonical(3);

    write_and_seal_partial(
        &backend,
        full_attempt,
        crate::vnode_partial::VnodePartial {
            operators: vec![("agg".into(), b"full".to_vec())],
            base: None,
            deltas: Vec::new(),
        },
        None,
    )
    .await;
    write_and_seal_partial(
        &backend,
        delta_one_attempt,
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: None,
            deltas: vec![("agg".into(), b"delta-1".to_vec())],
        },
        Some(full_attempt),
    )
    .await;
    write_and_seal_partial(
        &backend,
        delta_two_attempt,
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: None,
            deltas: vec![("agg".into(), b"delta-2".to_vec())],
        },
        Some(delta_one_attempt),
    )
    .await;

    let report = SealedVnodeChainReader::new(&backend)
        .load_at(&[0], delta_two_attempt)
        .await
        .unwrap();
    let decoded: Vec<_> = report.chains[&0]
        .iter()
        .map(|bytes| crate::vnode_partial::VnodePartial::decode(bytes).unwrap())
        .collect();
    assert_eq!(decoded.len(), 3);
    let (base, deltas) = resolve_op_chain(&decoded, "agg").expect("resolved aggregate chain");
    assert_eq!(base, b"full");
    assert_eq!(deltas, vec![b"delta-1".as_slice(), b"delta-2".as_slice()]);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn substituted_parent_attestation_is_rejected_before_parent_body_read() {
    let inner = InProcessBackend::new(1);
    let parent = CheckpointAttempt::canonical(1);
    let head = CheckpointAttempt::canonical(2);
    write_and_seal_partial(
        &inner,
        parent,
        crate::vnode_partial::VnodePartial {
            operators: vec![("agg".into(), b"full".to_vec())],
            base: None,
            deltas: Vec::new(),
        },
        None,
    )
    .await;
    write_and_seal_partial(
        &inner,
        head,
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: None,
            deltas: Vec::new(),
        },
        Some(parent),
    )
    .await;
    let head_inventory = inner
        .checkpoint_seal_inventory(head)
        .await
        .unwrap()
        .unwrap();
    let backend = ObservingBackend::new(inner, Some(parent));

    let error = reader_from_inventory(&backend, head_inventory, u64::MAX)
        .load_at(&[0], head)
        .await
        .expect_err("a replaced parent seal must not change immutable child ancestry");

    assert!(error.to_string().contains("parent attestation"), "{error}");
    assert_eq!(
        backend.body_reads(),
        vec![head],
        "the mismatched parent must be rejected before its body is polled"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn restore_payload_budget_preflights_heads_and_stops_before_discovered_parent() {
    let inner = InProcessBackend::new(2);
    let full_head = CheckpointAttempt::canonical(7);
    seal_epoch(&inner, full_head.epoch, &[0, 1], b"head-state").await;
    let full_inventory = inner
        .checkpoint_seal_inventory(full_head)
        .await
        .unwrap()
        .unwrap();
    let full_total = full_inventory
        .sealed_partials
        .iter()
        .try_fold(0_u64, |total, sealed| total.checked_add(sealed.payload_len))
        .unwrap();
    let backend = ObservingBackend::new(inner, None);

    let error = reader_from_inventory(&backend, full_inventory.clone(), full_total)
        .load_at(&[0, 0], full_head)
        .await
        .expect_err("duplicate vnode requests must fail closed");
    assert!(error.to_string().contains("duplicate vnodes"), "{error}");
    assert!(backend.body_reads().is_empty());

    let error = reader_from_inventory(&backend, full_inventory.clone(), full_total - 1)
        .load_at(&[0, 1], full_head)
        .await
        .expect_err("combined head payloads must respect one reader-wide budget");
    assert!(error.to_string().contains("recovery budget"), "{error}");
    assert!(
        backend.body_reads().is_empty(),
        "known head overflow must be rejected before any body read"
    );

    let restored = reader_from_inventory(&backend, full_inventory, full_total)
        .load_at(&[0, 1], full_head)
        .await
        .expect("the exact cumulative head budget must be admitted");
    assert_eq!(restored.chain_count(), 2);
    assert_eq!(backend.body_reads(), vec![full_head, full_head]);

    let inner = InProcessBackend::new(1);
    let parent = CheckpointAttempt::canonical(10);
    let reference = CheckpointAttempt::canonical(11);
    write_and_seal_partial(
        &inner,
        parent,
        crate::vnode_partial::VnodePartial {
            operators: vec![("agg".into(), b"parent-state".to_vec())],
            base: None,
            deltas: Vec::new(),
        },
        None,
    )
    .await;
    write_and_seal_partial(
        &inner,
        reference,
        crate::vnode_partial::VnodePartial::default(),
        Some(parent),
    )
    .await;
    let parent_len = inner
        .checkpoint_seal_inventory(parent)
        .await
        .unwrap()
        .unwrap()
        .sealed_partials[0]
        .payload_len;
    let reference_inventory = inner
        .checkpoint_seal_inventory(reference)
        .await
        .unwrap()
        .unwrap();
    let reference_len = reference_inventory.sealed_partials[0].payload_len;
    let backend = ObservingBackend::new(inner, None);
    let error = reader_from_inventory(
        &backend,
        reference_inventory,
        reference_len + parent_len - 1,
    )
    .load_at(&[0], reference)
    .await
    .expect_err("a discovered parent must share the same cumulative budget");
    assert!(error.to_string().contains("recovery budget"), "{error}");
    assert_eq!(
        backend.body_reads(),
        vec![reference],
        "the child may reveal ancestry, but an over-budget parent body must not be read"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn retention_keeps_fallback_delta_ancestors_until_rebase() {
    let backend = InProcessBackend::new(1);
    let attempts = [
        CheckpointAttempt::canonical(1),
        CheckpointAttempt::canonical(2),
        CheckpointAttempt::canonical(3),
        CheckpointAttempt::canonical(4),
        CheckpointAttempt::canonical(5),
    ];

    write_and_seal_partial(
        &backend,
        attempts[0],
        crate::vnode_partial::VnodePartial {
            operators: vec![("agg".into(), b"full-1".to_vec())],
            base: None,
            deltas: Vec::new(),
        },
        None,
    )
    .await;
    for (attempt, parent, delta) in [
        (attempts[1], attempts[0], b"delta-2".as_slice()),
        (attempts[2], attempts[1], b"delta-3".as_slice()),
    ] {
        write_and_seal_partial(
            &backend,
            attempt,
            crate::vnode_partial::VnodePartial {
                operators: Vec::new(),
                base: None,
                deltas: vec![("agg".into(), delta.to_vec())],
            },
            Some(parent),
        )
        .await;
    }
    write_and_seal_partial(
        &backend,
        attempts[3],
        crate::vnode_partial::VnodePartial {
            operators: vec![("agg".into(), b"full-4".to_vec())],
            base: None,
            deltas: Vec::new(),
        },
        None,
    )
    .await;
    write_and_seal_partial(
        &backend,
        attempts[4],
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: None,
            deltas: vec![("agg".into(), b"delta-5".to_vec())],
        },
        Some(attempts[3]),
    )
    .await;

    // At E5 with R=3 and C=2, manifests E2/E3 are retained, so state GC must keep their E1
    // FULL ancestor (state horizon 0, not the manifest horizon 2).
    backend.prune_before(0).await.unwrap();
    for attempt in [attempts[1], attempts[2]] {
        SealedVnodeChainReader::new(&backend)
            .load_at(&[0], attempt)
            .await
            .expect("retained fallback cut must keep its FULL ancestor");
    }

    // Once the fallback window starts at the E4 FULL re-base, E1 can be removed. The current
    // E5 chain remains valid, while an older delta cut fails closed instead of starting empty.
    backend.prune_before(2).await.unwrap();
    SealedVnodeChainReader::new(&backend)
        .load_at(&[0], attempts[4])
        .await
        .expect("post-rebase chain must not depend on the old FULL");
    let error = SealedVnodeChainReader::new(&backend)
        .load_at(&[0], attempts[2])
        .await
        .expect_err("a missing delta ancestor must fail closed");
    assert!(error.to_string().contains("delta parent"), "{error}");
    assert!(
        error.to_string().contains("has no exact state seal"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn large_epoch_gap_rebase_keeps_the_earliest_fallback_chain() {
    let backend = InProcessBackend::new(1);
    let obsolete = CheckpointAttempt::canonical(1);
    write_and_seal_partial(
        &backend,
        obsolete,
        crate::vnode_partial::VnodePartial {
            operators: vec![("agg".into(), b"obsolete-full".to_vec())],
            base: None,
            deltas: Vec::new(),
        },
        None,
    )
    .await;

    // An allocator high-watermark jump must re-base at E100. With R=3/C=2 and a current E105,
    // E102 is the earliest retained manifest and state GC may advance only to E100.
    let attempts: Vec<_> = (100..=105).map(CheckpointAttempt::canonical).collect();
    for index in [0_usize, 3] {
        write_and_seal_partial(
            &backend,
            attempts[index],
            crate::vnode_partial::VnodePartial {
                operators: vec![(
                    "agg".into(),
                    format!("full-{}", attempts[index].epoch).into_bytes(),
                )],
                base: None,
                deltas: Vec::new(),
            },
            None,
        )
        .await;
        for child in (index + 1)..=(index + 2) {
            write_and_seal_partial(
                &backend,
                attempts[child],
                crate::vnode_partial::VnodePartial {
                    operators: Vec::new(),
                    base: None,
                    deltas: vec![(
                        "agg".into(),
                        format!("delta-{}", attempts[child].epoch).into_bytes(),
                    )],
                },
                Some(attempts[child - 1]),
            )
            .await;
        }
    }

    backend.prune_before(100).await.unwrap();
    assert!(backend
        .checkpoint_seal_inventory(obsolete)
        .await
        .unwrap()
        .is_none());
    for attempt in [attempts[2], attempts[5]] {
        let report = SealedVnodeChainReader::new(&backend)
            .load_at(&[0], attempt)
            .await
            .expect("fallback and current chains must survive the large-gap rebase prune");
        assert_eq!(report.chains[&0].len(), 3);
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn retention_keeps_reference_then_delta_ancestry() {
    let backend = InProcessBackend::new(1);
    let full = CheckpointAttempt::canonical(100);
    let reference = CheckpointAttempt::canonical(102);
    let delta_one = CheckpointAttempt::canonical(103);
    let delta_two = CheckpointAttempt::canonical(104);

    write_and_seal_partial(
        &backend,
        full,
        crate::vnode_partial::VnodePartial {
            operators: vec![("agg".into(), b"full-100".to_vec())],
            base: None,
            deltas: Vec::new(),
        },
        None,
    )
    .await;
    write_and_seal_partial(
        &backend,
        reference,
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: None,
            deltas: Vec::new(),
        },
        Some(full),
    )
    .await;
    write_and_seal_partial(
        &backend,
        delta_one,
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: None,
            deltas: vec![("agg".into(), b"delta-103".to_vec())],
        },
        Some(reference),
    )
    .await;
    write_and_seal_partial(
        &backend,
        delta_two,
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: None,
            deltas: vec![("agg".into(), b"delta-104".to_vec())],
        },
        Some(delta_one),
    )
    .await;

    // R=3 permits the E102 reference to point two epochs back; C=2 then permits two
    // consecutive deltas. The earliest retained E104 fallback therefore needs additive
    // state slack (R-1)+C=4, preserving E100.
    backend.prune_before(100).await.unwrap();
    let report = SealedVnodeChainReader::new(&backend)
        .load_at(&[0], delta_two)
        .await
        .expect("reference followed by deltas must retain its FULL root");
    assert_eq!(report.chains[&0].len(), 4);
    let decoded: Vec<_> = report.chains[&0]
        .iter()
        .map(|bytes| crate::vnode_partial::VnodePartial::decode(bytes).unwrap())
        .collect();
    let (base, deltas) = resolve_op_chain(&decoded, "agg").expect("resolved aggregate chain");
    assert_eq!(base, b"full-100");
    assert_eq!(
        deltas,
        vec![b"delta-103".as_slice(), b"delta-104".as_slice()]
    );
}

#[tokio::test]
async fn rehydrate_rejects_reference_parent_without_its_own_seal() {
    let backend = InProcessBackend::new(4);
    let base_attempt = CheckpointAttempt::canonical(5);
    let base = crate::vnode_partial::VnodePartial {
        operators: vec![("agg".into(), vec![1, 2, 3])],
        base: None,
        deltas: Vec::new(),
    };
    let base_bytes = Bytes::from(base.encode().unwrap());
    backend
        .write_partial(base_attempt, 0, 0, base_bytes.clone())
        .await
        .unwrap();
    let hypothetical_seal = laminar_core::state::SealedVnodePartial {
        vnode: 0,
        assignment_version: 0,
        writer: None,
        payload_len: base_bytes.len() as u64,
        payload_sha256: format!("{:x}", Sha256::digest(&base_bytes)),
    };
    let base_link =
        crate::vnode_partial::SealedVnodeParentLink::new(base_attempt, &hypothetical_seal).unwrap();

    let head_attempt = CheckpointAttempt::canonical(6);
    let reference = crate::vnode_partial::VnodePartial {
        operators: Vec::new(),
        base: Some(base_link),
        deltas: Vec::new(),
    };
    backend
        .write_partial(head_attempt, 0, 0, Bytes::from(reference.encode().unwrap()))
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(head_attempt, None, &[0], &[])
        .await
        .unwrap());

    let error = SealedVnodeChainReader::new(&backend)
        .load_at(&[0], head_attempt)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("reference base"));
    assert!(error.to_string().contains("has no exact state seal"));
}

#[tokio::test]
async fn load_at_rejects_an_unsealed_attempt() {
    let backend = InProcessBackend::new(4);
    let error = SealedVnodeChainReader::new(&backend)
        .load_at(&[0, 1], CheckpointAttempt::canonical(1))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("has no exact state seal"));
}

#[tokio::test]
async fn rehydrate_empty_request_is_noop() {
    let backend = InProcessBackend::new(4);
    seal_epoch(&backend, 1, &[0], b"x").await;
    let report = SealedVnodeChainReader::new(&backend)
        .load_at(&[], CheckpointAttempt::canonical(1))
        .await
        .unwrap();
    assert_eq!(report.attempt, None);
    assert!(report.chains.is_empty());
}

#[tokio::test]
async fn rehydrate_over_object_store_backend() {
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;

    let dir = tempfile::tempdir().unwrap();
    let store: std::sync::Arc<dyn ObjectStore> =
        std::sync::Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let backend = ObjectStoreBackend::node_durable(store, "node-0", 4);
    seal_epoch(&backend, 5, &[0, 1], b"durable").await;

    let report = SealedVnodeChainReader::new(&backend)
        .load_at(&[0, 1], CheckpointAttempt::canonical(5))
        .await
        .unwrap();

    assert_eq!(report.attempt, Some(CheckpointAttempt::canonical(5)));
    assert_eq!(report.chain_count(), 2);
    assert_eq!(operator_payload(&report, 1), b"durable");
}
