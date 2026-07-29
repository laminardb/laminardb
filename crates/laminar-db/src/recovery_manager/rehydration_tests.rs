#[cfg(feature = "cluster")]
use std::sync::Arc;

#[cfg(feature = "cluster")]
use super::vnode_chains::resolve_op_chain;
use super::vnode_chains::{LoadedVnodeChains, SealedVnodeChainReader};
use async_trait::async_trait;
use bytes::Bytes;
use laminar_core::state::{
    CheckpointAttempt, CheckpointSealInventory, InProcessBackend, ObjectStoreBackend,
    SealedVnodePartial, StateBackend, StateBackendDurability, StateBackendError,
    VnodePartialLineage,
};

#[cfg(feature = "cluster")]
const TEST_PARTIAL_LIMIT_BYTES: u64 = 1024 * 1024;

struct LegacyPartialReadBackend {
    inner: InProcessBackend,
}

pub(super) struct ReadCountingBackend {
    inner: InProcessBackend,
    seal_inventory_reads: std::sync::atomic::AtomicUsize,
    sealed_partial_body_reads: std::sync::atomic::AtomicUsize,
    inventory_failures_remaining: std::sync::atomic::AtomicUsize,
    yield_inventory_reads: bool,
    block_body_reads: bool,
    body_read_entries: tokio::sync::Semaphore,
    body_read_releases: tokio::sync::Semaphore,
    active_body_reads: std::sync::atomic::AtomicUsize,
}

struct ActiveBodyRead<'a>(&'a std::sync::atomic::AtomicUsize);

impl Drop for ActiveBodyRead<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl ReadCountingBackend {
    pub(super) fn new(key_group_capacity: u32) -> Self {
        Self {
            inner: InProcessBackend::new(key_group_capacity),
            seal_inventory_reads: std::sync::atomic::AtomicUsize::new(0),
            sealed_partial_body_reads: std::sync::atomic::AtomicUsize::new(0),
            inventory_failures_remaining: std::sync::atomic::AtomicUsize::new(0),
            yield_inventory_reads: false,
            block_body_reads: false,
            body_read_entries: tokio::sync::Semaphore::new(0),
            body_read_releases: tokio::sync::Semaphore::new(0),
            active_body_reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub(super) fn with_blocking_body_reads(key_group_capacity: u32) -> Self {
        Self {
            block_body_reads: true,
            ..Self::new(key_group_capacity)
        }
    }

    fn with_yielding_inventory_reads(key_group_capacity: u32) -> Self {
        Self {
            yield_inventory_reads: true,
            ..Self::new(key_group_capacity)
        }
    }

    fn with_one_inventory_failure(key_group_capacity: u32) -> Self {
        Self {
            inventory_failures_remaining: std::sync::atomic::AtomicUsize::new(1),
            ..Self::new(key_group_capacity)
        }
    }

    fn seal_inventory_reads(&self) -> usize {
        self.seal_inventory_reads
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(super) fn sealed_partial_body_reads(&self) -> usize {
        self.sealed_partial_body_reads
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(super) async fn wait_for_body_read_entry(&self) {
        self.body_read_entries
            .acquire()
            .await
            .expect("body-read entry semaphore remains open")
            .forget();
    }

    pub(super) fn active_body_reads(&self) -> usize {
        self.active_body_reads
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[async_trait]
impl StateBackend for ReadCountingBackend {
    fn key_group_capacity(&self) -> u32 {
        self.inner.key_group_capacity()
    }

    async fn write_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_version: u64,
        lineage: VnodePartialLineage,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.inner
            .write_partial(attempt, vnode, assignment_version, lineage, bytes)
            .await
    }

    async fn write_certified_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        writer_node_id: u64,
        lineage: VnodePartialLineage,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.inner
            .write_certified_partial(
                attempt,
                vnode,
                assignment_fence,
                writer_node_id,
                lineage,
                bytes,
            )
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
        sealed: &SealedVnodePartial,
        max_bytes: u64,
    ) -> Result<Option<Bytes>, StateBackendError> {
        self.sealed_partial_body_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.block_body_reads {
            self.active_body_reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let _active = ActiveBodyRead(&self.active_body_reads);
            self.body_read_entries.add_permits(1);
            self.body_read_releases
                .acquire()
                .await
                .expect("body-read release semaphore remains open")
                .forget();
        }
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
        self.seal_inventory_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self
            .inventory_failures_remaining
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |remaining| remaining.checked_sub(1),
            )
            .is_ok()
        {
            return Err(StateBackendError::Io(
                "injected transient seal inventory failure".into(),
            ));
        }
        if self.yield_inventory_reads {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
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
        lineage: VnodePartialLineage,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.inner
            .write_partial(attempt, vnode, assignment_version, lineage, bytes)
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

pub(super) async fn seal_epoch(backend: &dyn StateBackend, epoch: u64, vnodes: &[u32], tag: &[u8]) {
    let attempt = CheckpointAttempt::canonical(epoch);
    for &v in vnodes {
        let partial = crate::vnode_partial::VnodePartial {
            operators: vec![("agg".into(), tag.to_vec())],
            base: None,
            deltas: Vec::new(),
        };
        let bytes = Bytes::from(partial.encode().unwrap());
        backend
            .write_partial(
                attempt,
                v,
                0,
                VnodePartialLineage::root(bytes.len() as u64),
                bytes,
            )
            .await
            .unwrap();
    }
    assert!(backend
        .seal_checkpoint(attempt, None, vnodes, &[])
        .await
        .unwrap());
}

async fn write_and_seal_partial(
    backend: &dyn StateBackend,
    attempt: CheckpointAttempt,
    partial: crate::vnode_partial::VnodePartial,
) {
    write_and_seal_vnode_partials(backend, attempt, vec![(0, partial)]).await;
}

async fn write_and_seal_vnode_partials(
    backend: &dyn StateBackend,
    attempt: CheckpointAttempt,
    partials: Vec<(u32, crate::vnode_partial::VnodePartial)>,
) {
    let mut vnodes = Vec::with_capacity(partials.len());
    for (vnode, partial) in partials {
        let parent = partial.base;
        let bytes = Bytes::from(partial.encode().unwrap());
        let lineage = match parent {
            None => VnodePartialLineage::root(bytes.len() as u64),
            Some(parent_attempt) => {
                let inventory = backend
                    .checkpoint_seal_inventory(parent_attempt)
                    .await
                    .unwrap()
                    .expect("parent is sealed before its child is written");
                let parent_lineage = inventory
                    .sealed_partials
                    .iter()
                    .find(|sealed| sealed.vnode == vnode)
                    .unwrap_or_else(|| panic!("parent seal contains vnode {vnode}"))
                    .lineage;
                VnodePartialLineage::extend(
                    attempt,
                    bytes.len() as u64,
                    parent_attempt,
                    parent_lineage,
                )
                .unwrap()
            }
        };
        backend
            .write_partial(attempt, vnode, 0, lineage, bytes)
            .await
            .unwrap();
        vnodes.push(vnode);
    }
    assert!(backend
        .seal_checkpoint(attempt, None, &vnodes, &[])
        .await
        .unwrap());
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
    let reader =
        SealedVnodeChainReader::from_validated_head(&backend, &head, TEST_PARTIAL_LIMIT_BYTES, 6)
            .unwrap();

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

#[cfg(feature = "cluster")]
#[tokio::test]
async fn validated_reader_rejects_invalid_restore_limit_configuration() {
    let backend = InProcessBackend::new(1);
    let attempt = CheckpointAttempt::canonical(7);
    seal_epoch(&backend, attempt.epoch, &[0], b"state").await;
    let inventory = backend
        .checkpoint_seal_inventory(attempt)
        .await
        .unwrap()
        .expect("attempt is sealed");
    let head =
        crate::checkpoint_coordinator::ValidatedVnodeRestoreHead::from_unchecked_inventory_for_test(
            inventory,
        );

    let zero_limit = SealedVnodeChainReader::from_validated_head(&backend, &head, 0, 1)
        .err()
        .expect("a zero artifact limit must be rejected");
    assert!(zero_limit.to_string().contains("must be nonzero"));

    let zero_chain_limit =
        SealedVnodeChainReader::from_validated_head(&backend, &head, TEST_PARTIAL_LIMIT_BYTES, 0)
            .err()
            .expect("a zero chain artifact limit must be rejected");
    assert!(zero_chain_limit.to_string().contains("must be nonzero"));

    let overflow = SealedVnodeChainReader::from_validated_head(&backend, &head, u64::MAX, 2)
        .err()
        .expect("an unrepresentable transition payload limit must be rejected");
    assert!(overflow.to_string().contains("overflows u64"), "{overflow}");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn restore_lineage_payload_cap_accepts_exact_and_rejects_max_plus_one_before_body_io() {
    let backend = ReadCountingBackend::new(2);
    let attempt = CheckpointAttempt::canonical(7);
    seal_epoch(&backend, attempt.epoch, &[0, 1], b"equal-sized-roots").await;
    let inventory = backend
        .inner
        .checkpoint_seal_inventory(attempt)
        .await
        .unwrap()
        .expect("attempt is sealed");
    let per_vnode_bytes = inventory.sealed_partials[0].lineage.total_payload_bytes();
    assert_eq!(
        inventory.sealed_partials[1].lineage.total_payload_bytes(),
        per_vnode_bytes
    );
    let exact_transition_bytes = per_vnode_bytes.checked_mul(2).unwrap();
    let head =
        crate::checkpoint_coordinator::ValidatedVnodeRestoreHead::from_unchecked_inventory_for_test(
            inventory,
        );

    let loaded =
        SealedVnodeChainReader::from_validated_head(&backend, &head, exact_transition_bytes, 1)
            .unwrap()
            .load_at(&[0, 1], attempt)
            .await
            .expect("the exact transition payload envelope is admissible");
    let usage = loaded.input_usage();
    assert_eq!(usage.declared_lineage_bytes(), exact_transition_bytes);
    assert_eq!(usage.verified_body_bytes(), exact_transition_bytes);
    assert_eq!(usage.declared_lineage_artifacts(), 2);
    assert_eq!(usage.verified_body_artifacts(), 2);

    let body_reads_before_rejection = backend.sealed_partial_body_reads();
    let error =
        SealedVnodeChainReader::from_validated_head(&backend, &head, exact_transition_bytes - 1, 1)
            .unwrap()
            .load_at(&[0, 1], attempt)
            .await
            .expect_err("one byte over the transition payload cap must fail closed");
    assert!(error.to_string().contains("exceeding"), "{error}");
    assert_eq!(
        backend.sealed_partial_body_reads(),
        body_reads_before_rejection,
        "lineage preflight must reject before any partial body read"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn metadata_lineage_rejection_performs_zero_partial_body_reads() {
    use laminar_core::checkpoint::{
        CheckpointAssignmentFence, CheckpointParticipant, VnodeRestoreLimits,
    };

    let backend = ReadCountingBackend::new(1);
    let fence = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1],
        vec![CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(1),
        }],
    )
    .unwrap();
    let parent_attempt = CheckpointAttempt::canonical(1);
    backend
        .write_certified_partial(
            parent_attempt,
            0,
            &fence,
            1,
            VnodePartialLineage::root(6),
            Bytes::from_static(b"parent"),
        )
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(parent_attempt, Some(&fence), &[0], &[])
        .await
        .unwrap());
    let child_attempt = CheckpointAttempt::canonical(2);
    let forged_lineage = VnodePartialLineage::extend(
        child_attempt,
        5,
        parent_attempt,
        VnodePartialLineage::root(7),
    )
    .unwrap();
    backend
        .write_certified_partial(
            child_attempt,
            0,
            &fence,
            1,
            forged_lineage,
            Bytes::from_static(b"child"),
        )
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(child_attempt, Some(&fence), &[0], &[])
        .await
        .unwrap());
    let head = Arc::new(
        backend
            .inner
            .checkpoint_seal_inventory(child_attempt)
            .await
            .unwrap()
            .unwrap(),
    );
    let limits = VnodeRestoreLimits::global_singleton_compatibility(12, 2, 1).unwrap();

    let error =
        crate::vnode_restore_lineage::validate_vnode_restore_lineage(&backend, head, limits)
            .await
            .unwrap_err();

    assert!(error.to_string().contains("does not exactly extend parent"));
    assert_eq!(backend.seal_inventory_reads(), 1);
    assert_eq!(backend.sealed_partial_body_reads(), 0);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn restore_lineage_artifact_limit_rejects_before_body_io() {
    let backend = ReadCountingBackend::new(1);
    let parent = CheckpointAttempt::canonical(1);
    let child = CheckpointAttempt::canonical(2);
    write_and_seal_partial(
        &backend,
        parent,
        crate::vnode_partial::VnodePartial {
            operators: vec![("agg".into(), b"full".to_vec())],
            base: None,
            deltas: Vec::new(),
        },
    )
    .await;
    write_and_seal_partial(
        &backend,
        child,
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: Some(parent),
            deltas: vec![("agg".into(), b"delta".to_vec())],
        },
    )
    .await;
    let inventory = backend
        .inner
        .checkpoint_seal_inventory(child)
        .await
        .unwrap()
        .unwrap();
    let head =
        crate::checkpoint_coordinator::ValidatedVnodeRestoreHead::from_unchecked_inventory_for_test(
            inventory,
        );

    let error =
        SealedVnodeChainReader::from_validated_head(&backend, &head, TEST_PARTIAL_LIMIT_BYTES, 1)
            .unwrap()
            .load_at(&[0], child)
            .await
            .expect_err("a two-artifact lineage cannot enter a one-artifact reader");
    assert!(
        error.to_string().contains("declares 2 artifacts"),
        "{error}"
    );
    assert_eq!(backend.sealed_partial_body_reads(), 0);
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
    write_and_seal_partial(&backend, full_attempt, full).await;
    let reference = crate::vnode_partial::VnodePartial {
        operators: Vec::new(),
        base: Some(full_attempt),
        deltas: Vec::new(),
    };
    let reference_attempt = CheckpointAttempt::canonical(6);
    write_and_seal_partial(&backend, reference_attempt, reference).await;

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
    let usage = report.input_usage();
    assert_eq!(usage.declared_lineage_artifacts(), 2);
    assert_eq!(usage.verified_body_artifacts(), 2);
    assert_eq!(usage.declared_lineage_bytes(), usage.verified_body_bytes());
    assert!(
        usage.verified_body_bytes() > chain[0].len() as u64,
        "the verified input receipt must include the consumed reference-only body"
    );
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
    )
    .await;
    write_and_seal_partial(
        &backend,
        delta_one_attempt,
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: Some(full_attempt),
            deltas: vec![("agg".into(), b"delta-1".to_vec())],
        },
    )
    .await;
    write_and_seal_partial(
        &backend,
        delta_two_attempt,
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: Some(delta_one_attempt),
            deltas: vec![("agg".into(), b"delta-2".to_vec())],
        },
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
async fn child_lineage_mismatch_fails_before_parent_body_io() {
    let backend = ReadCountingBackend::new(1);
    let parent = CheckpointAttempt::canonical(1);
    let child = CheckpointAttempt::canonical(2);
    write_and_seal_partial(
        &backend,
        parent,
        crate::vnode_partial::VnodePartial {
            operators: vec![("agg".into(), b"full".to_vec())],
            base: None,
            deltas: Vec::new(),
        },
    )
    .await;
    let parent_inventory = backend
        .inner
        .checkpoint_seal_inventory(parent)
        .await
        .unwrap()
        .unwrap();
    let parent_payload_bytes = parent_inventory.sealed_partials[0]
        .lineage
        .total_payload_bytes();

    let child_partial = crate::vnode_partial::VnodePartial {
        operators: Vec::new(),
        base: Some(parent),
        deltas: vec![("agg".into(), b"delta".to_vec())],
    };
    let child_bytes = Bytes::from(child_partial.encode().unwrap());
    let wrong_parent_lineage = VnodePartialLineage::root(parent_payload_bytes + 1);
    let wrong_child_lineage = VnodePartialLineage::extend(
        child,
        child_bytes.len() as u64,
        parent,
        wrong_parent_lineage,
    )
    .unwrap();
    backend
        .write_partial(child, 0, 0, wrong_child_lineage, child_bytes)
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(child, None, &[0], &[])
        .await
        .unwrap());
    let inventory = backend
        .inner
        .checkpoint_seal_inventory(child)
        .await
        .unwrap()
        .unwrap();
    let head =
        crate::checkpoint_coordinator::ValidatedVnodeRestoreHead::from_unchecked_inventory_for_test(
            inventory,
        );

    let error =
        SealedVnodeChainReader::from_validated_head(&backend, &head, TEST_PARTIAL_LIMIT_BYTES, 2)
            .unwrap()
            .load_at(&[0], child)
            .await
            .expect_err("a child must exactly extend its parent's sealed lineage");
    assert!(
        error.to_string().contains("does not exactly extend"),
        "{error}"
    );
    assert_eq!(
        backend.sealed_partial_body_reads(),
        1,
        "only the child body may be fetched before lineage disagreement is known"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn decoded_base_mismatch_fails_before_parent_body_io() {
    let backend = ReadCountingBackend::new(1);
    let parent = CheckpointAttempt::canonical(1);
    let child = CheckpointAttempt::canonical(2);
    write_and_seal_partial(
        &backend,
        parent,
        crate::vnode_partial::VnodePartial {
            operators: vec![("agg".into(), b"parent".to_vec())],
            base: None,
            deltas: Vec::new(),
        },
    )
    .await;
    let parent_inventory = backend
        .inner
        .checkpoint_seal_inventory(parent)
        .await
        .unwrap()
        .unwrap();
    let parent_lineage = parent_inventory.sealed_partials[0].lineage;
    let child_partial = crate::vnode_partial::VnodePartial {
        operators: vec![("agg".into(), b"child".to_vec())],
        base: None,
        deltas: Vec::new(),
    };
    let child_bytes = Bytes::from(child_partial.encode().unwrap());
    let child_lineage =
        VnodePartialLineage::extend(child, child_bytes.len() as u64, parent, parent_lineage)
            .unwrap();
    backend
        .write_partial(child, 0, 0, child_lineage, child_bytes)
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(child, None, &[0], &[])
        .await
        .unwrap());
    let inventory = backend
        .inner
        .checkpoint_seal_inventory(child)
        .await
        .unwrap()
        .unwrap();
    let head =
        crate::checkpoint_coordinator::ValidatedVnodeRestoreHead::from_unchecked_inventory_for_test(
            inventory,
        );

    let error =
        SealedVnodeChainReader::from_validated_head(&backend, &head, TEST_PARTIAL_LIMIT_BYTES, 2)
            .unwrap()
            .load_at(&[0], child)
            .await
            .expect_err("decoded parent metadata must match the seal lineage");
    assert!(error.to_string().contains("names base None"), "{error}");
    assert_eq!(backend.sealed_partial_body_reads(), 1);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn parent_seal_inventory_load_is_single_flight_across_vnodes() {
    let backend = ReadCountingBackend::with_yielding_inventory_reads(8);
    let parent = CheckpointAttempt::canonical(1);
    let child = CheckpointAttempt::canonical(2);
    let vnodes: Vec<u32> = (0..8).collect();
    let full_partial = crate::vnode_partial::VnodePartial {
        operators: vec![("agg".into(), b"full".to_vec())],
        base: None,
        deltas: Vec::new(),
    };
    let full_bytes = Bytes::from(full_partial.encode().unwrap());
    for &vnode in &vnodes {
        backend
            .write_partial(
                parent,
                vnode,
                0,
                VnodePartialLineage::root(full_bytes.len() as u64),
                full_bytes.clone(),
            )
            .await
            .unwrap();
    }
    assert!(backend
        .seal_checkpoint(parent, None, &vnodes, &[])
        .await
        .unwrap());
    let parent_inventory = backend
        .inner
        .checkpoint_seal_inventory(parent)
        .await
        .unwrap()
        .unwrap();
    let reference_partial = crate::vnode_partial::VnodePartial {
        operators: Vec::new(),
        base: Some(parent),
        deltas: Vec::new(),
    };
    let reference_bytes = Bytes::from(reference_partial.encode().unwrap());
    for &vnode in &vnodes {
        let parent_lineage = parent_inventory
            .sealed_partials
            .iter()
            .find(|partial| partial.vnode == vnode)
            .unwrap()
            .lineage;
        let lineage = VnodePartialLineage::extend(
            child,
            reference_bytes.len() as u64,
            parent,
            parent_lineage,
        )
        .unwrap();
        backend
            .write_partial(child, vnode, 0, lineage, reference_bytes.clone())
            .await
            .unwrap();
    }
    assert!(backend
        .seal_checkpoint(child, None, &vnodes, &[])
        .await
        .unwrap());
    let head_inventory = backend
        .inner
        .checkpoint_seal_inventory(child)
        .await
        .unwrap()
        .unwrap();
    let head =
        crate::checkpoint_coordinator::ValidatedVnodeRestoreHead::from_unchecked_inventory_for_test(
            head_inventory,
        );

    let loaded =
        SealedVnodeChainReader::from_validated_head(&backend, &head, TEST_PARTIAL_LIMIT_BYTES, 2)
            .unwrap()
            .load_at(&vnodes, child)
            .await
            .unwrap();
    assert_eq!(loaded.chain_count(), vnodes.len());
    assert_eq!(loaded.input_usage().verified_body_artifacts(), 16);
    assert_eq!(
        backend.seal_inventory_reads(),
        1,
        "concurrent parent lookups must share one per-attempt seal load"
    );
    assert_eq!(backend.sealed_partial_body_reads(), 16);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn conflicting_parent_reseal_cannot_change_retained_child_restore() {
    let backend = InProcessBackend::new(1);
    let parent = CheckpointAttempt::canonical(1);
    let child = CheckpointAttempt::canonical(2);
    write_and_seal_partial(
        &backend,
        parent,
        crate::vnode_partial::VnodePartial {
            operators: vec![("agg".into(), b"full".to_vec())],
            base: None,
            deltas: Vec::new(),
        },
    )
    .await;
    write_and_seal_partial(
        &backend,
        child,
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: Some(parent),
            deltas: vec![("agg".into(), b"delta".to_vec())],
        },
    )
    .await;

    let conflict = backend
        .seal_checkpoint(parent, None, &[], &[])
        .await
        .unwrap_err();
    assert!(matches!(conflict, StateBackendError::Conflict { .. }));

    let loaded = SealedVnodeChainReader::new(&backend)
        .load_at(&[0], child)
        .await
        .unwrap();
    let decoded: Vec<_> = loaded.chains[&0]
        .iter()
        .map(|bytes| crate::vnode_partial::VnodePartial::decode(bytes).unwrap())
        .collect();
    let (base, deltas) = resolve_op_chain(&decoded, "agg").unwrap();
    assert_eq!(base, b"full");
    assert_eq!(deltas, vec![b"delta".as_slice()]);
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
                base: Some(parent),
                deltas: vec![("agg".into(), delta.to_vec())],
            },
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
    )
    .await;
    write_and_seal_partial(
        &backend,
        attempts[4],
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: Some(attempts[3]),
            deltas: vec![("agg".into(), b"delta-5".to_vec())],
        },
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
        )
        .await;
        for child in (index + 1)..=(index + 2) {
            write_and_seal_partial(
                &backend,
                attempts[child],
                crate::vnode_partial::VnodePartial {
                    operators: Vec::new(),
                    base: Some(attempts[child - 1]),
                    deltas: vec![(
                        "agg".into(),
                        format!("delta-{}", attempts[child].epoch).into_bytes(),
                    )],
                },
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
    )
    .await;
    write_and_seal_partial(
        &backend,
        reference,
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: Some(full),
            deltas: Vec::new(),
        },
    )
    .await;
    write_and_seal_partial(
        &backend,
        delta_one,
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: Some(reference),
            deltas: vec![("agg".into(), b"delta-103".to_vec())],
        },
    )
    .await;
    write_and_seal_partial(
        &backend,
        delta_two,
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: Some(delta_one),
            deltas: vec![("agg".into(), b"delta-104".to_vec())],
        },
    )
    .await;

    // R=3 permits the E102 reference to point two epochs back; C=2 then permits two
    // consecutive deltas. The earliest retained E104 fallback therefore needs additive
    // state slack (R-1)+C=4, preserving E100.
    backend.prune_before(100).await.unwrap();
    let inventory = backend
        .checkpoint_seal_inventory(delta_two)
        .await
        .unwrap()
        .unwrap();
    let head =
        crate::checkpoint_coordinator::ValidatedVnodeRestoreHead::from_unchecked_inventory_for_test(
            inventory,
        );
    let report =
        SealedVnodeChainReader::from_validated_head(&backend, &head, TEST_PARTIAL_LIMIT_BYTES, 4)
            .unwrap()
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

    let error =
        SealedVnodeChainReader::from_validated_head(&backend, &head, TEST_PARTIAL_LIMIT_BYTES, 3)
            .unwrap()
            .load_at(&[0], delta_two)
            .await
            .expect_err("reader must independently reject lineage beyond the writer-derived bound");
    assert!(
        error.to_string().contains("limit of 3 artifacts"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn validated_reader_restores_global_state_and_empty_owned_vnodes_within_current_envelope() {
    const GLOBAL_VNODE: u32 = 0;
    let backend = InProcessBackend::new(3);
    let owned_vnodes = [GLOBAL_VNODE, 1, 2];
    let full_attempt = CheckpointAttempt::canonical(200);
    let reference_attempt = CheckpointAttempt::canonical(201);
    let delta_attempt = CheckpointAttempt::canonical(202);

    write_and_seal_vnode_partials(
        &backend,
        full_attempt,
        vec![
            (
                GLOBAL_VNODE,
                crate::vnode_partial::VnodePartial {
                    operators: vec![("agg".into(), b"global-full".to_vec())],
                    base: None,
                    deltas: Vec::new(),
                },
            ),
            (1, crate::vnode_partial::VnodePartial::default()),
            (2, crate::vnode_partial::VnodePartial::default()),
        ],
    )
    .await;
    write_and_seal_vnode_partials(
        &backend,
        reference_attempt,
        owned_vnodes
            .into_iter()
            .map(|vnode| {
                (
                    vnode,
                    crate::vnode_partial::VnodePartial {
                        operators: Vec::new(),
                        base: Some(full_attempt),
                        deltas: Vec::new(),
                    },
                )
            })
            .collect(),
    )
    .await;
    write_and_seal_vnode_partials(
        &backend,
        delta_attempt,
        owned_vnodes
            .into_iter()
            .map(|vnode| {
                (
                    vnode,
                    crate::vnode_partial::VnodePartial {
                        operators: Vec::new(),
                        base: Some(reference_attempt),
                        deltas: if vnode == GLOBAL_VNODE {
                            vec![("agg".into(), b"global-delta".to_vec())]
                        } else {
                            Vec::new()
                        },
                    },
                )
            })
            .collect(),
    )
    .await;

    let inventory = backend
        .checkpoint_seal_inventory(delta_attempt)
        .await
        .unwrap()
        .expect("the delta attempt is sealed");
    let expected_declared_bytes = inventory
        .sealed_partials
        .iter()
        .map(|partial| partial.lineage.total_payload_bytes())
        .sum::<u64>();
    let expected_declared_artifacts = inventory
        .sealed_partials
        .iter()
        .map(|partial| u64::from(partial.lineage.artifact_count()))
        .sum::<u64>();
    assert_eq!(expected_declared_artifacts, 9);
    let head =
        crate::checkpoint_coordinator::ValidatedVnodeRestoreHead::from_unchecked_inventory_for_test(
            inventory,
        );
    let checkpoint_cap =
        crate::checkpoint_coordinator::CheckpointConfig::default().max_staged_bytes;
    let production_chain_limit = crate::pipeline_lifecycle::MAX_ARTIFACTS_PER_CLUSTER_VNODE_CHAIN;
    assert_eq!(production_chain_limit, 6);
    assert!(
        expected_declared_bytes
            <= checkpoint_cap
                .checked_mul(u64::try_from(production_chain_limit).unwrap())
                .unwrap()
    );

    let loaded = SealedVnodeChainReader::from_validated_head(
        &backend,
        &head,
        checkpoint_cap,
        production_chain_limit,
    )
    .unwrap()
    .load_at(&owned_vnodes, delta_attempt)
    .await
    .expect("the currently admitted global-state shape must fit its compatibility envelope");

    assert_eq!(loaded.chain_count(), owned_vnodes.len());
    let global_chain = loaded.chains[&GLOBAL_VNODE]
        .iter()
        .map(|bytes| crate::vnode_partial::VnodePartial::decode(bytes).unwrap())
        .collect::<Vec<_>>();
    let (base, deltas) = resolve_op_chain(&global_chain, "agg").unwrap();
    assert_eq!(base, b"global-full");
    assert_eq!(deltas, vec![b"global-delta".as_slice()]);
    for vnode in [1, 2] {
        let chain = &loaded.chains[&vnode];
        assert_eq!(chain.len(), 1, "empty vnode {vnode} resolves to its FULL");
        let partial = crate::vnode_partial::VnodePartial::decode(&chain[0]).unwrap();
        assert!(partial.operators.is_empty());
        assert!(partial.base.is_none());
        assert!(partial.deltas.is_empty());
    }

    let usage = loaded.input_usage();
    assert_eq!(usage.declared_lineage_bytes(), expected_declared_bytes);
    assert_eq!(
        usage.declared_lineage_artifacts(),
        expected_declared_artifacts
    );
    assert_eq!(usage.verified_body_bytes(), expected_declared_bytes);
    assert_eq!(
        usage.verified_body_artifacts(),
        expected_declared_artifacts,
        "the receipt must include reference-only bodies consumed while resolving empty vnodes"
    );
    let retained_artifacts = loaded.chains.values().map(Vec::len).sum::<usize>();
    assert!(
        usize::try_from(usage.verified_body_artifacts()).unwrap() > retained_artifacts,
        "verified input accounting must not be inferred from only the retained apply chains"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn validated_reader_enforces_configured_chain_artifact_boundary() {
    let backend = InProcessBackend::new(1);
    let full = CheckpointAttempt::canonical(100);
    let reference = CheckpointAttempt::canonical(102);
    write_and_seal_partial(
        &backend,
        full,
        crate::vnode_partial::VnodePartial {
            operators: vec![("agg".into(), b"full".to_vec())],
            base: None,
            deltas: Vec::new(),
        },
    )
    .await;
    write_and_seal_partial(
        &backend,
        reference,
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: Some(full),
            deltas: Vec::new(),
        },
    )
    .await;
    let mut head_attempt = reference;
    for checkpoint_id in 103..=106 {
        let attempt = CheckpointAttempt::canonical(checkpoint_id);
        write_and_seal_partial(
            &backend,
            attempt,
            crate::vnode_partial::VnodePartial {
                operators: Vec::new(),
                base: Some(head_attempt),
                deltas: vec![("agg".into(), format!("delta-{checkpoint_id}").into_bytes())],
            },
        )
        .await;
        head_attempt = attempt;
    }
    let inventory = backend
        .checkpoint_seal_inventory(head_attempt)
        .await
        .unwrap()
        .unwrap();
    let head =
        crate::checkpoint_coordinator::ValidatedVnodeRestoreHead::from_unchecked_inventory_for_test(
            inventory,
        );
    let production_limit = crate::pipeline_lifecycle::MAX_ARTIFACTS_PER_CLUSTER_VNODE_CHAIN;

    let loaded = SealedVnodeChainReader::from_validated_head(
        &backend,
        &head,
        TEST_PARTIAL_LIMIT_BYTES,
        production_limit,
    )
    .unwrap()
    .load_at(&[0], head_attempt)
    .await
    .unwrap();
    assert_eq!(loaded.chains[&0].len(), production_limit);

    let over_limit_attempt = CheckpointAttempt::canonical(107);
    write_and_seal_partial(
        &backend,
        over_limit_attempt,
        crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: Some(head_attempt),
            deltas: vec![("agg".into(), b"delta-107".to_vec())],
        },
    )
    .await;
    let over_limit_inventory = backend
        .checkpoint_seal_inventory(over_limit_attempt)
        .await
        .unwrap()
        .unwrap();
    let over_limit_head =
        crate::checkpoint_coordinator::ValidatedVnodeRestoreHead::from_unchecked_inventory_for_test(
            over_limit_inventory,
        );
    let error = SealedVnodeChainReader::from_validated_head(
        &backend,
        &over_limit_head,
        TEST_PARTIAL_LIMIT_BYTES,
        production_limit,
    )
    .unwrap()
    .load_at(&[0], over_limit_attempt)
    .await
    .expect_err("the artifact immediately beyond the production limit must fail closed");
    assert!(
        error
            .to_string()
            .contains(&format!("limit of {production_limit} artifacts")),
        "{error}"
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
    let base_lineage = VnodePartialLineage::root(base_bytes.len() as u64);
    backend
        .write_partial(base_attempt, 0, 0, base_lineage, base_bytes)
        .await
        .unwrap();

    let head_attempt = CheckpointAttempt::canonical(6);
    let reference = crate::vnode_partial::VnodePartial {
        operators: Vec::new(),
        base: Some(base_attempt),
        deltas: Vec::new(),
    };
    let reference_bytes = Bytes::from(reference.encode().unwrap());
    let reference_lineage = VnodePartialLineage::extend(
        head_attempt,
        reference_bytes.len() as u64,
        base_attempt,
        base_lineage,
    )
    .unwrap();
    backend
        .write_partial(head_attempt, 0, 0, reference_lineage, reference_bytes)
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
async fn transient_seal_inventory_failure_is_not_cached() {
    let backend = ReadCountingBackend::with_one_inventory_failure(1);
    let attempt = CheckpointAttempt::canonical(1);
    seal_epoch(&backend, attempt.epoch, &[0], b"state").await;
    let reader = SealedVnodeChainReader::new(&backend);

    let first = reader
        .load_at(&[0], attempt)
        .await
        .expect_err("the injected first inventory read must fail");
    assert!(first
        .to_string()
        .contains("transient seal inventory failure"));
    assert_eq!(backend.sealed_partial_body_reads(), 0);

    let loaded = reader
        .load_at(&[0], attempt)
        .await
        .expect("the same reader must retry a transient inventory failure");
    assert_eq!(loaded.chain_count(), 1);
    assert_eq!(backend.seal_inventory_reads(), 2);
    assert_eq!(backend.sealed_partial_body_reads(), 1);
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
