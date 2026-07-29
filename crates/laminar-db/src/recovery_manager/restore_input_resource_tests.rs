use std::sync::Arc;
use std::time::Duration;

use laminar_core::state::{CheckpointAttempt, StateBackend};
use tokio_util::sync::CancellationToken;

use super::rehydration_tests::{seal_epoch, ReadCountingBackend};
use super::vnode_chains::SealedVnodeChainReader;
use crate::vnode_restore_input::{
    VnodeRestoreInputBudget, VnodeRestoreInputLimits, VnodeRestoreInputUsage,
    MAX_CONCURRENT_VNODE_BODY_READS,
};

fn budget(bytes: u64, artifacts: u64) -> Arc<VnodeRestoreInputBudget> {
    Arc::new(
        VnodeRestoreInputBudget::new(VnodeRestoreInputLimits {
            max_lineage_bytes: bytes,
            max_lineage_artifacts: artifacts,
        })
        .unwrap(),
    )
}

fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(5)
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
    assert_eq!(backend.sealed_partial_body_reads(), 2);
    drop(loaded);
    assert_eq!(resources.reserved_for_test(), (0, 0));
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
        let resources = budget(u64::MAX, u64::MAX);
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
    let resources = budget(u64::MAX, u64::MAX);
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
    drop(owner);

    let retry = resources.try_reserve(exact).unwrap();
    assert_eq!(resources.reserved_for_test(), (10, 2));
    drop(retry);
    assert_eq!(resources.reserved_for_test(), (0, 0));
}
