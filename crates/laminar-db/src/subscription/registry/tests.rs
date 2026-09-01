use std::sync::Arc as StdArc;

use arrow_array::{ArrayRef, Int64Array};
use arrow_schema::{DataType, Field, Schema};

use super::*;

fn batch(ids: Vec<i64>) -> RecordBatch {
    let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![StdArc::new(Int64Array::from(ids))]).unwrap()
}

fn earliest_retained(error: SubscriptionOpenError) -> u64 {
    match error {
        SubscriptionOpenError::ReplayPruned { earliest_retained } => earliest_retained,
        SubscriptionOpenError::EpochNotCommitted { .. } => {
            panic!("expected replay-pruned error")
        }
        SubscriptionOpenError::Capacity { .. } => panic!("expected replay-pruned error"),
    }
}

async fn next_update(reader: &mut SubscriptionReader) -> ChargedUpdate {
    match reader.next().await {
        SubscriptionRead::Update { update, .. } => update,
        SubscriptionRead::Lagged(skipped) => panic!("unexpected gap of {skipped} entries"),
        SubscriptionRead::Terminal(message) => panic!("unexpected terminal error: {message}"),
    }
}

#[tokio::test]
async fn tail_starts_at_atomic_attach_cut() {
    let registry = SubscriptionRegistry::new();
    registry.configure("mv", 1 << 20);
    registry.send_batch("mv", batch(vec![1])).unwrap();
    let mut reader = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
    registry.send_batch("mv", batch(vec![2])).unwrap();

    let update = next_update(&mut reader).await;
    assert!(
        matches!(update.as_ref(), MvUpdate::Batch(batch) if batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0) == 2)
    );
}

#[tokio::test]
async fn as_of_starts_strictly_after_exact_retained_barrier() {
    let registry = SubscriptionRegistry::new();
    registry.configure("mv", 1 << 20);
    registry.broadcast_barrier(1, 1);
    registry.send_batch("mv", batch(vec![10])).unwrap();
    registry.broadcast_barrier(2, 2);

    let mut reader = registry
        .subscribe("mv", SubscribeStart::AsOfEpoch(1))
        .unwrap();
    assert!(matches!(
        next_update(&mut reader).await.as_ref(),
        MvUpdate::Batch(_)
    ));
    assert!(matches!(
        next_update(&mut reader).await.as_ref(),
        MvUpdate::Barrier {
            epoch: 2,
            checkpoint_id: 2,
            ..
        }
    ));
}

#[tokio::test]
async fn delayed_commit_preserves_the_aligned_cut_cursor() {
    let registry = SubscriptionRegistry::new();
    registry.configure("mv", 1 << 20);
    registry.send_batch("mv", batch(vec![10])).unwrap();
    let mut live = registry.subscribe("mv", SubscribeStart::Tail).unwrap();

    let attempt = CheckpointAttempt::canonical(1);
    registry.reserve_cut(attempt).unwrap();
    registry.send_batch("mv", batch(vec![20])).unwrap();
    registry.commit_cut(attempt).unwrap();

    assert!(matches!(
        next_update(&mut live).await.as_ref(),
        MvUpdate::Batch(batch)
            if batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0) == 20
    ));
    assert!(matches!(
        next_update(&mut live).await.as_ref(),
        MvUpdate::Barrier {
            epoch: 1,
            checkpoint_id: 1,
            through_sequence: 1,
        }
    ));

    let mut replay = registry
        .subscribe("mv", SubscribeStart::AsOfEpoch(1))
        .unwrap();
    assert!(matches!(
        next_update(&mut replay).await.as_ref(),
        MvUpdate::Batch(batch)
            if batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0) == 20
    ));
}

#[test]
fn cut_reservation_fails_before_capture_when_marker_budget_is_full() {
    let sample = batch(vec![1, 2, 3]);
    let entry_bytes = approx_size(&MvUpdate::Batch(sample.clone()));
    let registry = SubscriptionRegistry::with_storage_budget(entry_bytes);
    registry.configure("mv", 1 << 20);
    registry.send_batch("mv", sample).unwrap();
    let attempt = CheckpointAttempt::canonical(1);

    let error = registry.reserve_cut(attempt).unwrap_err();

    assert!(error.contains("checkpoint markers require"));
    assert_eq!(registry.charged_bytes(), entry_bytes);
    let log = registry.streams.read().get("mv").cloned().unwrap();
    assert_eq!(log.inner.lock().reserved_marker, None);
    assert!(registry.lifecycle.lock().pending_cut.is_none());
}

#[test]
fn abort_releases_marker_headroom_for_the_next_attempt() {
    let registry = SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES);
    registry.configure("mv", BARRIER_ENTRY_BYTES);
    let first = CheckpointAttempt::canonical(1);
    registry.reserve_cut(first).unwrap();
    assert_eq!(registry.charged_bytes(), BARRIER_ENTRY_BYTES);

    registry.abort_cut(first);
    assert_eq!(registry.charged_bytes(), 0);

    let second = CheckpointAttempt::canonical(2);
    registry.reserve_cut(second).unwrap();
    assert_eq!(registry.charged_bytes(), BARRIER_ENTRY_BYTES);
    registry.abort_cut(second);
    assert_eq!(registry.charged_bytes(), 0);
}

#[test]
fn conflicting_attempt_cannot_steal_the_reserved_cut() {
    let registry = SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES);
    registry.configure("mv", BARRIER_ENTRY_BYTES);
    let reserved = CheckpointAttempt::canonical(1);
    let conflicting = CheckpointAttempt::canonical(2);
    registry.reserve_cut(reserved).unwrap();

    assert!(registry.reserve_cut(conflicting).is_err());
    assert!(registry.commit_cut(conflicting).is_err());
    assert_eq!(registry.charged_bytes(), BARRIER_ENTRY_BYTES);
    assert_eq!(
        registry
            .lifecycle
            .lock()
            .pending_cut
            .as_ref()
            .map(|cut| cut.attempt),
        Some(reserved)
    );

    registry.commit_cut(reserved).unwrap();
    assert_eq!(registry.next_sequence("mv"), Some(1));
    assert_eq!(registry.charged_bytes(), BARRIER_ENTRY_BYTES);
}

#[test]
fn noncanonical_cut_attempts_cannot_mutate_registry_state() {
    let registry = SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES);
    registry.configure("mv", BARRIER_ENTRY_BYTES);
    let invalid = CheckpointAttempt::new(1, 2);
    let log = registry.streams.read().get("mv").cloned().unwrap();

    let error = registry.reserve_cut(invalid).unwrap_err();

    assert!(error.contains("canonical checkpoint ID"));
    assert!(registry.lifecycle.lock().pending_cut.is_none());
    assert_eq!(log.inner.lock().reserved_marker, None);
    assert_eq!(registry.charged_bytes(), 0);

    let canonical = CheckpointAttempt::canonical(1);
    registry.reserve_cut(canonical).unwrap();
    assert!(registry.commit_cut(invalid).is_err());
    registry.abort_cut(invalid);

    assert_eq!(
        registry
            .lifecycle
            .lock()
            .pending_cut
            .as_ref()
            .map(|cut| cut.attempt),
        Some(canonical)
    );
    assert_eq!(log.inner.lock().reserved_marker, Some(canonical));
    assert_eq!(registry.charged_bytes(), BARRIER_ENTRY_BYTES);

    registry.abort_cut(canonical);
    assert!(registry.lifecycle.lock().pending_cut.is_none());
    assert_eq!(log.inner.lock().reserved_marker, None);
    assert_eq!(registry.charged_bytes(), 0);
}

#[test]
fn sequence_exhaustion_rejects_cut_reservation_without_claiming_budget() {
    let registry = SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES);
    registry.configure("mv", BARRIER_ENTRY_BYTES);
    let log = registry.streams.read().get("mv").cloned().unwrap();
    {
        let mut inner = log.inner.lock();
        inner.next_sequence = u64::MAX;
        inner.retention_floor = u64::MAX;
    }

    let error = registry
        .reserve_cut(CheckpointAttempt::canonical(1))
        .unwrap_err();

    assert!(error.contains("sequence space"));
    assert_eq!(registry.charged_bytes(), 0);
    assert_eq!(log.inner.lock().reserved_marker, None);
}

#[test]
fn terminal_logs_do_not_claim_checkpoint_marker_capacity() {
    let registry = SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES);
    registry.configure("mv", BARRIER_ENTRY_BYTES);
    let log = registry.streams.read().get("mv").cloned().unwrap();
    log.terminate("injected terminal state");
    let attempt = CheckpointAttempt::canonical(1);

    registry.reserve_cut(attempt).unwrap();

    assert_eq!(registry.charged_bytes(), 0);
    assert_eq!(
        registry
            .lifecycle
            .lock()
            .pending_cut
            .as_ref()
            .map(|cut| cut.markers.len()),
        Some(0)
    );
    registry.commit_cut(attempt).unwrap();
}

#[test]
fn post_cut_data_cannot_consume_the_reserved_marker_sequence() {
    let sample = batch(vec![1]);
    let entry_bytes = approx_size(&MvUpdate::Batch(sample.clone()));
    let registry =
        SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES.saturating_add(entry_bytes));
    registry.configure("mv", 1 << 20);
    let log = registry.streams.read().get("mv").cloned().unwrap();
    {
        let mut inner = log.inner.lock();
        inner.next_sequence = u64::MAX - 1;
        inner.retention_floor = u64::MAX - 1;
    }
    let attempt = CheckpointAttempt::canonical(1);
    registry.reserve_cut(attempt).unwrap();

    let error = registry.send_batch("mv", sample).unwrap_err();

    assert!(error.contains("sequence space exhausted"));
    {
        let inner = log.inner.lock();
        assert_eq!(inner.next_sequence, u64::MAX - 1);
        assert_eq!(inner.reserved_marker, Some(attempt));
        assert!(inner.terminal_error.is_none());
    }
    registry.commit_cut(attempt).unwrap();
    let inner = log.inner.lock();
    assert_eq!(inner.next_sequence, u64::MAX);
    assert_eq!(inner.reserved_marker, None);
}

#[test]
fn reserved_marker_survives_process_budget_contention() {
    let registry = SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES);
    registry.configure("mv", BARRIER_ENTRY_BYTES);
    let log = registry.streams.read().get("mv").cloned().unwrap();
    let attempt = CheckpointAttempt::canonical(1);
    registry.reserve_cut(attempt).unwrap();

    let error = registry.send_batch("mv", batch(vec![1])).unwrap_err();

    assert!(error.contains("process memory budget exhausted"));
    {
        let inner = log.inner.lock();
        assert_eq!(inner.next_sequence, 0);
        assert_eq!(inner.reserved_marker, Some(attempt));
        assert!(inner.terminal_error.is_none());
    }
    registry.commit_cut(attempt).unwrap();
    assert_eq!(registry.next_sequence("mv"), Some(1));
    assert_eq!(registry.charged_bytes(), BARRIER_ENTRY_BYTES);
}

#[test]
fn unobserved_commit_releases_reserved_marker_bytes() {
    let registry = SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES);
    let reader = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
    let attempt = CheckpointAttempt::canonical(1);
    registry.reserve_cut(attempt).unwrap();
    assert_eq!(registry.charged_bytes(), BARRIER_ENTRY_BYTES);

    drop(reader);
    registry.commit_cut(attempt).unwrap();

    assert_eq!(registry.charged_bytes(), 0);
    assert_eq!(registry.next_sequence("mv"), Some(0));
}

#[test]
fn rejected_marker_commit_releases_reserved_bytes_and_reports_failure() {
    let registry = SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES);
    registry.configure("mv", BARRIER_ENTRY_BYTES);
    let log = registry.streams.read().get("mv").cloned().unwrap();
    let attempt = CheckpointAttempt::canonical(1);
    registry.reserve_cut(attempt).unwrap();
    log.inner.lock().terminal_error = Some("injected terminal state".into());

    let error = registry.commit_cut(attempt).unwrap_err();

    assert!(error.contains("terminated before checkpoint marker publication"));
    assert_eq!(registry.charged_bytes(), 0);
    assert_eq!(log.inner.lock().reserved_marker, None);
}

#[test]
fn invalidation_and_object_drop_release_exact_marker_reservations() {
    let budget = StdArc::new(SubscriptionMemoryBudget::new(BARRIER_ENTRY_BYTES));
    let registry = SubscriptionRegistry::with_budget(StdArc::clone(&budget));
    registry.configure("mv", BARRIER_ENTRY_BYTES);
    let first = CheckpointAttempt::canonical(1);
    registry.reserve_cut(first).unwrap();

    registry.invalidate_all("injected recovery");
    assert_eq!(budget.used(), 0);
    assert!(registry.lifecycle.lock().pending_cut.is_none());

    let second = CheckpointAttempt::canonical(2);
    registry.reserve_cut(second).unwrap();
    assert!(registry.drop_name("mv"));
    assert_eq!(budget.used(), 0);
    registry.commit_cut(second).unwrap();
}

#[tokio::test]
async fn recreated_object_is_outside_the_dropped_objects_reserved_cut() {
    let registry = SubscriptionRegistry::with_storage_budget(1 << 20);
    registry.configure("mv", 1 << 20);
    let mut dropped_reader = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
    let attempt = CheckpointAttempt::canonical(1);
    registry.reserve_cut(attempt).unwrap();

    assert!(registry.drop_name("mv"));
    assert!(matches!(
        dropped_reader.next().await,
        SubscriptionRead::Terminal(ref error) if error == "object dropped"
    ));

    registry.configure("mv", 1 << 20);
    let mut recreated_reader = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
    registry.commit_cut(attempt).unwrap();
    assert_eq!(registry.next_sequence("mv"), Some(0));
    assert!(matches!(recreated_reader.try_read(), TryRead::Pending));

    registry.send_batch("mv", batch(vec![7])).unwrap();
    assert!(matches!(
        recreated_reader.next().await,
        SubscriptionRead::Update {
            sequence: 0,
            update,
        } if matches!(update.as_ref(), MvUpdate::Batch(_))
    ));
}

#[test]
fn registry_drop_releases_an_unresolved_marker_reservation() {
    let budget = StdArc::new(SubscriptionMemoryBudget::new(BARRIER_ENTRY_BYTES));
    {
        let registry = SubscriptionRegistry::with_budget(StdArc::clone(&budget));
        registry.configure("mv", BARRIER_ENTRY_BYTES);
        registry
            .reserve_cut(CheckpointAttempt::canonical(1))
            .unwrap();
        assert_eq!(budget.used(), BARRIER_ENTRY_BYTES);
    }
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn recovery_replacement_continues_the_current_object_sequence() {
    let registry = SubscriptionRegistry::new();
    registry.configure("mv", 1 << 20);
    let mut before_recovery = registry.subscribe("mv", SubscribeStart::Tail).unwrap();

    registry.send_batch("mv", batch(vec![10])).unwrap();
    assert!(matches!(
        before_recovery.next().await,
        SubscriptionRead::Update {
            sequence: 0,
            update,
        } if matches!(update.as_ref(), MvUpdate::Batch(_))
    ));
    let abandoned = CheckpointAttempt::canonical(1);
    registry.reserve_cut(abandoned).unwrap();

    registry.invalidate_all("injected recovery");
    assert!(matches!(
        before_recovery.next().await,
        SubscriptionRead::Terminal(message) if message == "injected recovery"
    ));
    assert!(registry.commit_cut(abandoned).is_err());

    let mut after_recovery = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
    registry.send_batch("mv", batch(vec![10])).unwrap();
    assert!(matches!(
        after_recovery.next().await,
        SubscriptionRead::Update {
            sequence: 1,
            update,
        } if matches!(update.as_ref(), MvUpdate::Batch(_))
    ));

    let committed = CheckpointAttempt::canonical(2);
    registry.reserve_cut(committed).unwrap();
    registry.commit_cut(committed).unwrap();
    assert!(matches!(
        after_recovery.next().await,
        SubscriptionRead::Update {
            sequence: 2,
            update,
        } if matches!(
            update.as_ref(),
            MvUpdate::Barrier {
                epoch: 2,
                checkpoint_id: 2,
                through_sequence: 2,
            }
        )
    ));
}

#[test]
fn as_of_classifies_future_missing_and_pruned_epochs() {
    let registry = SubscriptionRegistry::with_storage_budget(1 << 20);
    registry.configure("mv", 1 << 20);
    registry.broadcast_barrier(5, 5);
    registry.broadcast_barrier(7, 7);

    assert!(matches!(
        registry
            .subscribe("mv", SubscribeStart::AsOfEpoch(8))
            .unwrap_err(),
        SubscriptionOpenError::EpochNotCommitted {
            requested: 8,
            latest_committed: Some(7)
        }
    ));
    assert!(matches!(
        registry
            .subscribe("mv", SubscribeStart::AsOfEpoch(6))
            .unwrap_err(),
        SubscriptionOpenError::EpochNotCommitted {
            requested: 6,
            latest_committed: Some(7)
        }
    ));

    registry.configure(
        "mv",
        approx_size(&MvUpdate::Barrier {
            epoch: 0,
            checkpoint_id: 0,
            through_sequence: 0,
        }),
    );
    assert_eq!(
        earliest_retained(
            registry
                .subscribe("mv", SubscribeStart::AsOfEpoch(5))
                .unwrap_err()
        ),
        7
    );
}

#[test]
fn as_of_knows_latest_epoch_without_a_stored_log_entry() {
    let registry = SubscriptionRegistry::with_storage_budget(1 << 20);
    registry.broadcast_barrier(11, 11);

    assert!(matches!(
        registry
            .subscribe("late", SubscribeStart::AsOfEpoch(12))
            .unwrap_err(),
        SubscriptionOpenError::EpochNotCommitted {
            requested: 12,
            latest_committed: Some(11)
        }
    ));
    assert_eq!(
        earliest_retained(
            registry
                .subscribe("late", SubscribeStart::AsOfEpoch(11))
                .unwrap_err()
        ),
        0
    );
}

#[test]
fn zero_retention_never_enables_as_of() {
    let registry = SubscriptionRegistry::new();
    let _reader = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
    registry.broadcast_barrier(1, 1);
    registry.send_batch("mv", batch(vec![1])).unwrap();

    let error = registry
        .subscribe("mv", SubscribeStart::AsOfEpoch(1))
        .unwrap_err();
    assert_eq!(earliest_retained(error), 0);
}

#[tokio::test]
async fn cached_retention_floor_matches_cold_scan_after_hot_path_updates() {
    let registry = SubscriptionRegistry::with_storage_budget(1 << 20);
    registry.configure("mv", 512);
    let mut reader = registry.subscribe("mv", SubscribeStart::Tail).unwrap();

    for value in 0..32_i64 {
        registry.send_batch("mv", batch(vec![value])).unwrap();
        registry.assert_retention_cache("mv");
    }
    for _ in 0..16 {
        let _ = next_update(&mut reader).await;
        registry.assert_retention_cache("mv");
    }

    registry.configure("mv", 4096);
    registry.assert_retention_cache("mv");
    registry.configure("mv", 128);
    registry.assert_retention_cache("mv");
}

#[test]
fn disabling_retention_without_readers_releases_the_log() {
    let registry = SubscriptionRegistry::new();
    registry.configure("mv", 1 << 20);
    let values: ArrayRef = StdArc::new(Int64Array::from(vec![1, 2, 3]));
    let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    registry
        .send_batch(
            "mv",
            RecordBatch::try_new(schema, vec![StdArc::clone(&values)]).unwrap(),
        )
        .unwrap();
    assert_eq!(StdArc::strong_count(&values), 2);

    registry.configure("mv", 0);

    assert_eq!(StdArc::strong_count(&values), 1);
}

#[test]
fn as_of_readers_do_not_clone_retained_arrow_batches() {
    let registry = SubscriptionRegistry::new();
    registry.configure("mv", 1 << 20);
    registry.broadcast_barrier(1, 1);
    let values: ArrayRef = StdArc::new(Int64Array::from(vec![1, 2, 3]));
    let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    registry
        .send_batch(
            "mv",
            RecordBatch::try_new(schema, vec![StdArc::clone(&values)]).unwrap(),
        )
        .unwrap();
    let owners_before_attach = StdArc::strong_count(&values);

    let readers = (0..super::super::MAX_SUBSCRIBERS_PER_MV)
        .map(|_| {
            registry
                .subscribe("mv", SubscribeStart::AsOfEpoch(1))
                .unwrap()
        })
        .collect::<Vec<_>>();

    assert_eq!(readers.len(), super::super::MAX_SUBSCRIBERS_PER_MV);
    assert_eq!(StdArc::strong_count(&values), owners_before_attach);
}

#[tokio::test]
async fn terminal_drop_releases_retained_storage_immediately() {
    let registry = SubscriptionRegistry::with_storage_budget(1 << 20);
    registry.configure("mv", 1 << 20);
    registry.broadcast_barrier(1, 1);
    let values: ArrayRef = StdArc::new(Int64Array::from(vec![1, 2, 3]));
    let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    registry
        .send_batch(
            "mv",
            RecordBatch::try_new(schema, vec![StdArc::clone(&values)]).unwrap(),
        )
        .unwrap();
    let mut reader = registry
        .subscribe("mv", SubscribeStart::AsOfEpoch(1))
        .unwrap();
    assert!(registry.charged_bytes() > 0);
    assert_eq!(StdArc::strong_count(&values), 2);

    assert!(registry.drop_name("mv"));

    assert_eq!(registry.charged_bytes(), 0);
    assert_eq!(StdArc::strong_count(&values), 1);
    assert!(matches!(
        reader.next().await,
        SubscriptionRead::Terminal(message) if message == "object dropped"
    ));
}

#[tokio::test]
async fn local_log_reclaims_while_the_charge_follows_reader_updates() {
    let registry = SubscriptionRegistry::with_storage_budget(1 << 20);
    let mut first = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
    let mut second = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
    registry.send_batch("mv", batch(vec![1, 2, 3])).unwrap();
    let log = registry.streams.read().get("mv").cloned().unwrap();
    let retained = registry.charged_bytes();
    assert!(retained > 0);

    let first_frame = next_update(&mut first).await;
    assert!(matches!(first_frame.as_ref(), MvUpdate::Batch(_)));
    assert_eq!(registry.charged_bytes(), retained);
    assert_eq!(log.inner.lock().bytes, retained);

    let second_frame = next_update(&mut second).await;
    assert!(matches!(second_frame.as_ref(), MvUpdate::Batch(_)));
    assert_eq!(registry.charged_bytes(), retained);
    assert_eq!(log.inner.lock().bytes, 0);

    drop(first_frame);
    assert_eq!(registry.charged_bytes(), retained);
    drop(second_frame);
    assert_eq!(registry.charged_bytes(), 0);
}

#[tokio::test]
async fn process_budget_contention_fails_without_claim_and_release_is_reusable() {
    let sample = batch(vec![1, 2, 3]);
    let entry_bytes = approx_size(&MvUpdate::Batch(sample.clone()));
    let budget = StdArc::new(SubscriptionMemoryBudget::new(entry_bytes));
    let first_registry = SubscriptionRegistry::with_budget(StdArc::clone(&budget));
    let contender_registry = SubscriptionRegistry::with_budget(budget);
    first_registry.configure("first", entry_bytes.saturating_mul(4));
    contender_registry.configure("contender", entry_bytes.saturating_mul(4));

    first_registry.send_batch("first", sample.clone()).unwrap();
    assert_eq!(first_registry.charged_bytes(), entry_bytes);
    let contender_sequence = contender_registry.next_sequence("contender").unwrap();
    assert!(contender_registry
        .send_batch("contender", sample.clone())
        .is_err());
    assert_eq!(
        contender_registry.next_sequence("contender"),
        Some(contender_sequence),
        "failed admission must not claim a sequence"
    );
    let mut contender = contender_registry
        .subscribe("contender", SubscribeStart::Tail)
        .unwrap();
    assert!(matches!(
        contender.next().await,
        SubscriptionRead::Terminal(message)
            if message.contains("process memory budget exhausted")
    ));

    assert!(first_registry.drop_name("first"));
    assert_eq!(contender_registry.charged_bytes(), 0);
    contender_registry.configure("replacement", entry_bytes.saturating_mul(4));
    contender_registry
        .send_batch("replacement", sample)
        .unwrap();
    assert_eq!(contender_registry.charged_bytes(), entry_bytes);
    assert_eq!(contender_registry.next_sequence("replacement"), Some(1));
}

#[tokio::test]
async fn as_of_cursor_reports_exact_gap_after_live_byte_eviction() {
    let registry = SubscriptionRegistry::new();
    registry.configure("mv", 1024);
    registry.broadcast_barrier(1, 1);
    let mut reader = registry
        .subscribe("mv", SubscribeStart::AsOfEpoch(1))
        .unwrap();

    let values_per_batch = (MAX_LIVE_BATCH_BYTES / 2) / std::mem::size_of::<i64>();
    for value in 0..6_i64 {
        registry
            .send_batch("mv", batch(vec![value; values_per_batch]))
            .unwrap();
    }

    let head = registry.head_sequence("mv").unwrap();
    let expected = head.saturating_sub(1);
    assert!(
        expected > 0,
        "test must evict entries beyond the AS-OF cursor"
    );
    assert!(matches!(
        reader.next().await,
        SubscriptionRead::Lagged(skipped) if skipped == expected
    ));
}

#[tokio::test]
async fn dropping_name_is_a_visible_terminal_error() {
    let registry = SubscriptionRegistry::new();
    let mut reader = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
    assert!(registry.drop_name("mv"));
    assert!(matches!(
        reader.next().await,
        SubscriptionRead::Terminal(message) if message == "object dropped"
    ));
}

#[tokio::test]
async fn oversized_batch_is_one_explicit_sequence_not_claim_then_evict() {
    let registry = SubscriptionRegistry::new();
    registry.configure("mv", INTERNAL_LIVE_LOG_BYTES);
    registry.broadcast_barrier(1, 1);
    let mut reader = registry
        .subscribe("mv", SubscribeStart::AsOfEpoch(1))
        .unwrap();
    let before = registry.next_sequence("mv").unwrap();
    let values = vec![0_i64; MAX_LIVE_BATCH_BYTES / std::mem::size_of::<i64>() + 1];

    registry.send_batch("mv", batch(values)).unwrap();

    assert_eq!(registry.next_sequence("mv"), Some(before + 1));
    assert!(matches!(
        next_update(&mut reader).await.as_ref(),
        MvUpdate::Error(message) if message.contains("rows were not delivered")
    ));
}

#[test]
fn subscriber_cap_is_atomic_across_65_simultaneous_attempts() {
    let registry = StdArc::new(SubscriptionRegistry::new());
    let attempts = super::super::MAX_SUBSCRIBERS_PER_MV + 1;
    let start = StdArc::new(std::sync::Barrier::new(attempts));
    let handles = (0..attempts)
        .map(|_| {
            let registry = StdArc::clone(&registry);
            let start = StdArc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                registry.subscribe("mv", SubscribeStart::Tail)
            })
        })
        .collect::<Vec<_>>();

    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    let successes = results.iter().filter(|result| result.is_ok()).count();
    let capacity_failures = results
        .iter()
        .filter(|result| matches!(result, Err(SubscriptionOpenError::Capacity { .. })))
        .count();
    assert_eq!(successes, super::super::MAX_SUBSCRIBERS_PER_MV);
    assert_eq!(capacity_failures, 1);
}
