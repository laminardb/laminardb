use std::sync::Arc;
use std::time::Duration;

use laminar_core::state::{CheckpointAttempt, SealedPartialReadEnvelope, StateBackend};
use tokio_util::sync::CancellationToken;

use super::rehydration_tests::{seal_epoch, ReadCountingBackend};
use super::vnode_chains::SealedVnodeChainReader;
use crate::vnode_restore_input::{
    VnodeRestoreArchive, VnodeRestoreInputBudget, VnodeRestoreInputLimits, VnodeRestoreInputUsage,
    MAX_CONCURRENT_VNODE_BODY_READS,
};

const TEST_READ_ENVELOPE: SealedPartialReadEnvelope = SealedPartialReadEnvelope::new(2, 0);
const LARGE_TEST_BYTES: u64 = 1_u64 << 40;
const LARGE_TEST_ARTIFACTS: u64 = 1_u64 << 20;

fn budget_with_envelope(
    bytes: u64,
    artifacts: u64,
    envelope: SealedPartialReadEnvelope,
) -> Arc<VnodeRestoreInputBudget> {
    Arc::new(
        VnodeRestoreInputBudget::new(
            VnodeRestoreInputLimits {
                max_lineage_bytes: bytes,
                max_lineage_artifacts: artifacts,
            },
            envelope,
        )
        .unwrap(),
    )
}

fn budget(bytes: u64, artifacts: u64) -> Arc<VnodeRestoreInputBudget> {
    budget_with_envelope(bytes, artifacts, TEST_READ_ENVELOPE)
}

fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(5)
}

#[test]
fn inner_archive_alignment_is_conditional_and_byte_exact() {
    let mut aligned_owner = rkyv::util::AlignedVec::<16>::new();
    aligned_owner.extend_from_slice(b"aligned");
    let aligned_ptr = aligned_owner.as_ptr();
    let mut aligned = VnodeRestoreArchive::Borrowed(&aligned_owner);
    assert_eq!(aligned.alignment_copy_bytes(), 0);
    aligned.normalize_alignment().unwrap();
    assert_eq!(aligned.as_slice().as_ptr(), aligned_ptr);
    assert!(matches!(aligned, VnodeRestoreArchive::Borrowed(_)));

    let backing = (0_u8..32).collect::<Vec<_>>();
    let offset = (0..16)
        .find(|offset| backing[*offset..].as_ptr().align_offset(16) != 0)
        .unwrap();
    let expected = backing[offset..offset + 4].to_vec();
    let mut unaligned = VnodeRestoreArchive::Borrowed(&backing[offset..offset + 4]);
    assert_eq!(unaligned.alignment_copy_bytes(), expected.len());
    unaligned.normalize_alignment().unwrap();
    assert_eq!(unaligned.as_slice(), expected);
    assert_eq!(unaligned.as_slice().as_ptr().align_offset(16), 0);
    assert!(matches!(unaligned, VnodeRestoreArchive::Aligned(_)));
}

#[tokio::test]
async fn exact_raw_input_charge_is_held_until_loaded_bodies_drop() {
    let backend = ReadCountingBackend::new(2);
    seal_epoch(&backend, 1, &[0, 1], b"state").await;
    let attempt = CheckpointAttempt::canonical(1);
    let inventory = backend
        .checkpoint_seal_inventory(attempt)
        .await
        .unwrap()
        .unwrap();
    let bytes = inventory
        .sealed_partials
        .iter()
        .map(|partial| partial.lineage.total_payload_bytes())
        .sum();
    let artifacts = inventory
        .sealed_partials
        .iter()
        .map(|partial| u64::from(partial.lineage.artifact_count()))
        .sum();
    let resources = budget(bytes, artifacts);
    let cancel = CancellationToken::new();

    let loaded = SealedVnodeChainReader::new(&backend)
        .load_at_reserved(&[0, 1], attempt, &resources, deadline(), &cancel)
        .await
        .unwrap();

    assert_eq!(resources.reserved_for_test(), (bytes, artifacts));
    assert_eq!(
        resources.reserved_read_envelope_bytes_for_test(),
        TEST_READ_ENVELOPE.checked_bytes(bytes, artifacts).unwrap()
    );
    assert_eq!(
        resources.max_read_envelope_bytes_for_test(),
        TEST_READ_ENVELOPE.checked_bytes(bytes, artifacts).unwrap()
    );
    assert_eq!(backend.sealed_partial_body_reads(), 2);
    drop(loaded);
    assert_eq!(resources.reserved_for_test(), (0, 0));
    assert_eq!(resources.reserved_read_envelope_bytes_for_test(), 0);
}

#[tokio::test]
async fn byte_or_artifact_exhaustion_rejects_before_first_body_read() {
    for (byte_shortfall, artifact_shortfall) in [(true, false), (false, true)] {
        let backend = ReadCountingBackend::new(2);
        seal_epoch(&backend, 1, &[0, 1], b"state").await;
        let attempt = CheckpointAttempt::canonical(1);
        let inventory = backend
            .checkpoint_seal_inventory(attempt)
            .await
            .unwrap()
            .unwrap();
        let exact_bytes: u64 = inventory
            .sealed_partials
            .iter()
            .map(|partial| partial.lineage.total_payload_bytes())
            .sum();
        let exact_artifacts: u64 = inventory
            .sealed_partials
            .iter()
            .map(|partial| u64::from(partial.lineage.artifact_count()))
            .sum();
        let resources = budget(
            if byte_shortfall {
                exact_bytes - 1
            } else {
                exact_bytes
            },
            if artifact_shortfall {
                exact_artifacts - 1
            } else {
                exact_artifacts
            },
        );
        let cancel = CancellationToken::new();

        let error = SealedVnodeChainReader::new(&backend)
            .load_at_reserved(&[0, 1], attempt, &resources, deadline(), &cancel)
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("reservation unavailable"),
            "{error}"
        );
        assert_eq!(backend.sealed_partial_body_reads(), 0);
        assert_eq!(resources.reserved_for_test(), (0, 0));
    }
}

#[tokio::test]
async fn deadline_and_cancellation_fail_without_body_reads_or_leaked_charge() {
    for cancelled in [false, true] {
        let backend = ReadCountingBackend::new(1);
        seal_epoch(&backend, 1, &[0], b"state").await;
        let resources = budget(LARGE_TEST_BYTES, LARGE_TEST_ARTIFACTS);
        let cancel = CancellationToken::new();
        if cancelled {
            cancel.cancel();
        }
        let deadline = if cancelled {
            deadline()
        } else {
            tokio::time::Instant::now()
        };

        let error = SealedVnodeChainReader::new(&backend)
            .load_at_reserved(
                &[0],
                CheckpointAttempt::canonical(1),
                &resources,
                deadline,
                &cancel,
            )
            .await
            .unwrap_err();

        assert_eq!(backend.sealed_partial_body_reads(), 0);
        assert_eq!(resources.reserved_for_test(), (0, 0));
        assert!(
            error
                .to_string()
                .contains(if cancelled { "cancelled" } else { "deadline" }),
            "{error}"
        );
    }
}

#[tokio::test]
async fn cancellation_drops_an_inflight_body_read_and_releases_all_resources() {
    let backend = ReadCountingBackend::with_blocking_body_reads(1);
    seal_epoch(&backend, 1, &[0], b"state").await;
    let resources = budget(LARGE_TEST_BYTES, LARGE_TEST_ARTIFACTS);
    let cancel = CancellationToken::new();
    let reader = SealedVnodeChainReader::new(&backend);

    let load = reader.load_at_reserved(
        &[0],
        CheckpointAttempt::canonical(1),
        &resources,
        deadline(),
        &cancel,
    );
    let cancel_after_entry = async {
        backend.wait_for_body_read_entry().await;
        assert_eq!(backend.active_body_reads(), 1);
        cancel.cancel();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(load, cancel_after_entry)
    })
    .await
    .expect("cancelling an in-flight backend read must be prompt");

    let error = result.unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
    assert_eq!(backend.sealed_partial_body_reads(), 1);
    assert_eq!(backend.active_body_reads(), 0);
    assert_eq!(resources.reserved_for_test(), (0, 0));

    let reservation = resources
        .try_reserve(VnodeRestoreInputUsage::declared(1, 1))
        .unwrap();
    let permit_cancel = CancellationToken::new();
    let mut permits = Vec::with_capacity(MAX_CONCURRENT_VNODE_BODY_READS);
    for _ in 0..MAX_CONCURRENT_VNODE_BODY_READS {
        permits.push(
            reservation
                .acquire_body_read(deadline(), &permit_cancel)
                .await
                .unwrap(),
        );
    }
    drop(permits);
    drop(reservation);
    assert_eq!(resources.reserved_for_test(), (0, 0));
}

#[tokio::test]
async fn body_read_slots_are_worker_wide_and_reusable() {
    let resources = budget(1, 1);
    let usage = VnodeRestoreInputUsage::declared(1, 1);
    let reservation = resources.try_reserve(usage).unwrap();
    let cancel = CancellationToken::new();
    let mut permits = Vec::with_capacity(MAX_CONCURRENT_VNODE_BODY_READS);
    for _ in 0..MAX_CONCURRENT_VNODE_BODY_READS {
        permits.push(
            reservation
                .acquire_body_read(deadline(), &cancel)
                .await
                .unwrap(),
        );
    }

    let error = reservation
        .acquire_body_read(tokio::time::Instant::now(), &cancel)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("deadline"), "{error}");

    permits.pop();
    let _permit = reservation
        .acquire_body_read(deadline(), &cancel)
        .await
        .unwrap();
    drop(permits);
    drop(reservation);
    assert_eq!(resources.reserved_for_test(), (0, 0));
}

#[test]
fn failed_reservation_is_atomic_and_retries_after_owner_drop() {
    let resources = budget(10, 2);
    let exact = VnodeRestoreInputUsage::declared(10, 2);
    let owner = resources.try_reserve(exact).unwrap();

    assert!(resources.try_reserve(exact).is_err());
    assert_eq!(resources.reserved_for_test(), (10, 2));
    assert_eq!(resources.reserved_read_envelope_bytes_for_test(), 20);
    drop(owner);

    let retry = resources.try_reserve(exact).unwrap();
    assert_eq!(resources.reserved_for_test(), (10, 2));
    assert_eq!(resources.reserved_read_envelope_bytes_for_test(), 20);
    let alignment = retry.try_reserve_inner_alignment_copy(10).unwrap();
    assert_eq!(resources.reserved_inner_alignment_copy_bytes_for_test(), 10);
    assert!(retry.try_reserve_inner_alignment_copy(1).is_err());
    drop(alignment);
    assert_eq!(resources.reserved_inner_alignment_copy_bytes_for_test(), 0);
    assert!(retry.try_reserve_inner_alignment_copy(11).is_err());
    drop(retry);
    assert_eq!(resources.reserved_for_test(), (0, 0));
    assert_eq!(resources.reserved_read_envelope_bytes_for_test(), 0);
}

#[test]
fn read_envelope_composes_exactly_and_rejects_invalid_arithmetic() {
    let envelope = SealedPartialReadEnvelope::new(3, 7);
    let resources = budget_with_envelope(10, 2, envelope);
    assert_eq!(resources.max_read_envelope_bytes_for_test(), 44);

    let first = resources
        .try_reserve(VnodeRestoreInputUsage::declared(4, 1))
        .unwrap();
    assert_eq!(resources.reserved_read_envelope_bytes_for_test(), 19);
    let second = resources
        .try_reserve(VnodeRestoreInputUsage::declared(6, 1))
        .unwrap();
    assert_eq!(resources.reserved_read_envelope_bytes_for_test(), 44);
    assert!(resources
        .try_reserve(VnodeRestoreInputUsage::declared(1, 1))
        .is_err());
    assert_eq!(resources.reserved_read_envelope_bytes_for_test(), 44);
    drop(first);
    drop(second);
    assert_eq!(resources.reserved_read_envelope_bytes_for_test(), 0);

    let limits = VnodeRestoreInputLimits {
        max_lineage_bytes: 1,
        max_lineage_artifacts: 1,
    };
    let invalid =
        VnodeRestoreInputBudget::new(limits, SealedPartialReadEnvelope::new(1, 0)).unwrap_err();
    assert!(invalid.to_string().contains("alignment copy"), "{invalid}");
    let overflow =
        VnodeRestoreInputBudget::new(limits, SealedPartialReadEnvelope::new(2, u64::MAX))
            .unwrap_err();
    assert!(overflow.to_string().contains("overflows"), "{overflow}");
}

#[tokio::test]
async fn backend_envelope_mismatch_rejects_before_first_body_read() {
    let backend = ReadCountingBackend::new(1);
    seal_epoch(&backend, 1, &[0], b"state").await;
    let resources = budget_with_envelope(
        LARGE_TEST_BYTES,
        LARGE_TEST_ARTIFACTS,
        SealedPartialReadEnvelope::new(2, 1),
    );
    let cancel = CancellationToken::new();

    let error = SealedVnodeChainReader::new(&backend)
        .load_at_reserved(
            &[0],
            CheckpointAttempt::canonical(1),
            &resources,
            deadline(),
            &cancel,
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("does not match"), "{error}");
    assert_eq!(backend.seal_inventory_reads(), 0);
    assert_eq!(backend.sealed_partial_body_reads(), 0);
    assert_eq!(resources.reserved_for_test(), (0, 0));
    assert_eq!(resources.reserved_read_envelope_bytes_for_test(), 0);
}

#[tokio::test]
async fn unaligned_backend_bodies_are_normalized_once_before_retention() {
    let backend = ReadCountingBackend::with_unaligned_body_reads(1);
    seal_epoch(&backend, 1, &[0], b"state").await;
    let attempt = CheckpointAttempt::canonical(1);
    let inventory = backend
        .checkpoint_seal_inventory(attempt)
        .await
        .unwrap()
        .unwrap();
    let bytes = inventory.sealed_partials[0].lineage.total_payload_bytes();
    let artifacts = u64::from(inventory.sealed_partials[0].lineage.artifact_count());
    let resources = budget(bytes, artifacts);
    let cancel = CancellationToken::new();

    let loaded = SealedVnodeChainReader::new(&backend)
        .load_at_reserved(&[0], attempt, &resources, deadline(), &cancel)
        .await
        .unwrap();
    let body = &loaded.chains[&0][0];
    assert_eq!(body.as_ptr().align_offset(16), 0);
    let partial = crate::vnode_partial::VnodePartial::decode(body).unwrap();
    assert_eq!(partial.operators[0].1, b"state");
    assert_eq!(resources.reserved_for_test(), (bytes, artifacts));
    assert_eq!(resources.reserved_read_envelope_bytes_for_test(), 2 * bytes);

    drop(loaded);
    assert_eq!(resources.reserved_for_test(), (0, 0));
    assert_eq!(resources.reserved_read_envelope_bytes_for_test(), 0);
}
