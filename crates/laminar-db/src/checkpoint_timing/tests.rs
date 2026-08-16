use super::*;
use laminar_core::checkpoint::CheckpointParticipant;
use uuid::Uuid;

fn observation(checkpoint_id: u64) -> CheckpointBarrierTimingObservation {
    CheckpointBarrierTimingObservation {
        process: LocalProcessAuthorityIdentity {
            participant: CheckpointParticipant {
                node_id: 7,
                boot_incarnation: Uuid::from_u128(77),
            },
            process_term: 3,
        },
        attempt: CheckpointAttempt::canonical(checkpoint_id),
        role: CheckpointBarrierRole::Follower,
        assignment_version: 9,
        assignment_digest: [11; 32],
        pipeline_stall_ns: checkpoint_id * 100,
        local_barrier_ns: checkpoint_id * 50,
        aligned_resume_ns: Some(checkpoint_id * 10),
        durable_tail_handoff: true,
        deadline_exhausted: false,
    }
}

fn context(checkpoint_id: u64) -> CheckpointBarrierTimingContext {
    let observation = observation(checkpoint_id);
    CheckpointBarrierTimingContext {
        process: observation.process,
        attempt: observation.attempt,
        role: observation.role,
        assignment_version: observation.assignment_version,
        assignment_digest: observation.assignment_digest,
    }
}

#[test]
fn composite_guard_keeps_histogram_and_ledger_counts_in_lockstep() {
    let registry = prometheus::Registry::new();
    let metrics = crate::engine_metrics::EngineMetrics::new(&registry);
    let ledger = Arc::new(CheckpointBarrierTimingLedger::with_capacity(4));
    {
        let mut guard = CheckpointBarrierTimingGuard::start_with_context(
            || Some(context(1)),
            &metrics,
            &ledger,
            tokio::time::Instant::now() + Duration::from_secs(1),
        );
        guard.finish_local_barrier_with_handoff();
        guard.begin_aligned_resume();
        guard.finish_aligned_resume();
        assert_eq!(
            metrics
                .checkpoint_pipeline_stall_duration
                .get_sample_count(),
            0
        );
        assert_eq!(
            metrics.checkpoint_barrier_local_duration.get_sample_count(),
            0
        );
        assert_eq!(metrics.checkpoint_aligned_resume_wait.get_sample_count(), 0);
    }
    {
        let _early_return = CheckpointBarrierTimingGuard::start_with_context(
            || Some(context(2)),
            &metrics,
            &ledger,
            tokio::time::Instant::now(),
        );
    }

    assert_eq!(
        metrics
            .checkpoint_pipeline_stall_duration
            .get_sample_count(),
        2
    );
    assert_eq!(
        metrics.checkpoint_barrier_local_duration.get_sample_count(),
        2
    );
    assert_eq!(metrics.checkpoint_aligned_resume_wait.get_sample_count(), 1);
    let snapshot = ledger.snapshot_after(0, 4).unwrap();
    assert_eq!(snapshot.process, Some(context(1).process));
    assert_eq!(snapshot.next_sequence, 3);
    assert_eq!(snapshot.recording_loss_count, 0);
    assert_eq!(snapshot.records.len(), 2);
    assert!(snapshot.records[0].durable_tail_handoff);
    assert!(snapshot.records[0].aligned_resume_ns.is_some());
    assert!(!snapshot.records[0].deadline_exhausted);
    assert!(!snapshot.records[1].durable_tail_handoff);
    assert_eq!(snapshot.records[1].aligned_resume_ns, None);
    assert!(snapshot.records[1].deadline_exhausted);
    assert!(snapshot
        .records
        .iter()
        .all(|record| record.local_barrier_ns <= record.pipeline_stall_ns));
}

#[test]
fn context_loss_preserves_histograms_and_is_explicit() {
    let registry = prometheus::Registry::new();
    let metrics = crate::engine_metrics::EngineMetrics::new(&registry);
    let ledger = Arc::new(CheckpointBarrierTimingLedger::with_capacity(2));

    {
        let _guard = CheckpointBarrierTimingGuard::start_with_context(
            || None,
            &metrics,
            &ledger,
            tokio::time::Instant::now() + Duration::from_secs(1),
        );
    }

    assert_eq!(
        metrics
            .checkpoint_pipeline_stall_duration
            .get_sample_count(),
        1
    );
    assert_eq!(
        metrics.checkpoint_barrier_local_duration.get_sample_count(),
        1
    );
    let snapshot = ledger.snapshot_after(0, 2).unwrap();
    assert_eq!(snapshot.process, None);
    assert_eq!(snapshot.recording_loss_count, 1);
    assert!(snapshot.records.is_empty());
}

#[test]
fn ledger_sequences_and_pages_are_exact_and_exclusive() {
    let ledger = CheckpointBarrierTimingLedger::with_capacity(4);
    assert!(ledger.try_record(observation(1)));
    assert!(ledger.try_record(observation(2)));
    assert!(ledger.try_record(observation(3)));

    let first = ledger.snapshot_after(0, 2).unwrap();
    assert_eq!(first.capacity, 4);
    assert_eq!(first.oldest_retained_sequence, Some(1));
    assert_eq!(first.next_sequence, 4);
    assert!(first.has_more);
    assert_eq!(
        first
            .records
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    let second = ledger.snapshot_after(2, 2).unwrap();
    assert!(!second.has_more);
    assert_eq!(second.records, vec![observation(3).with_sequence(3)]);
    assert_eq!(ledger.snapshot_after(3, 1).unwrap().records, Vec::new());
}

#[test]
fn overwrite_and_stale_cursor_are_explicit() {
    let ledger = CheckpointBarrierTimingLedger::with_capacity(2);
    assert!(ledger.try_record(observation(1)));
    assert!(ledger.try_record(observation(2)));
    assert!(ledger.try_record(observation(3)));

    assert_eq!(
        ledger.snapshot_after(0, 2),
        Err(CheckpointBarrierTimingSnapshotError::CursorOverwritten {
            after_sequence: 0,
            oldest_retained_sequence: 2,
        })
    );
    let retained = ledger.snapshot_after(1, 2).unwrap();
    assert_eq!(retained.overwritten_record_count, 1);
    assert_eq!(
        retained
            .records
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
}

#[test]
fn writer_contention_loses_evidence_without_advancing_sequence() {
    let ledger = CheckpointBarrierTimingLedger::with_capacity(2);
    let guard = ledger.state.lock();
    assert!(!ledger.try_record(observation(1)));
    drop(guard);
    assert!(ledger.try_record(observation(2)));

    let snapshot = ledger.snapshot_after(0, 2).unwrap();
    assert_eq!(snapshot.recording_loss_count, 1);
    assert_eq!(snapshot.next_sequence, 2);
    assert_eq!(snapshot.records[0].sequence, 1);
    assert_eq!(snapshot.records[0].attempt, CheckpointAttempt::canonical(2));
}

#[test]
fn invalid_observation_and_sequence_exhaustion_fail_closed() {
    let ledger = CheckpointBarrierTimingLedger::with_capacity(2);
    let mut invalid = observation(1);
    invalid.local_barrier_ns = invalid.pipeline_stall_ns + 1;
    assert!(!ledger.try_record(invalid));
    let mut overlapping = observation(2);
    overlapping.local_barrier_ns = 150;
    overlapping.aligned_resume_ns = Some(100);
    assert!(!ledger.try_record(overlapping));
    {
        let mut state = ledger.state.lock();
        state.next_sequence = u64::MAX;
    }
    assert!(!ledger.try_record(observation(3)));
    let snapshot = ledger.snapshot_after(u64::MAX - 1, 1).unwrap();
    assert_eq!(snapshot.recording_loss_count, 3);
    assert!(snapshot.metadata_exhausted);
    assert!(snapshot.records.is_empty());
}

#[test]
fn ledger_rejects_a_second_process_identity() {
    let ledger = CheckpointBarrierTimingLedger::with_capacity(2);
    let first = observation(1);
    assert!(ledger.try_record(first));
    let mut other_process = observation(2);
    other_process.process.process_term += 1;
    assert!(!ledger.try_record(other_process));

    let snapshot = ledger.snapshot_after(0, 2).unwrap();
    assert_eq!(snapshot.process, Some(first.process));
    assert_eq!(snapshot.recording_loss_count, 1);
    assert_eq!(snapshot.records, vec![first.with_sequence(1)]);
}

#[test]
fn bounds_and_cursor_ahead_are_rejected() {
    let ledger = CheckpointBarrierTimingLedger::with_capacity(2);
    assert_eq!(
        ledger.snapshot_after(0, 0),
        Err(CheckpointBarrierTimingSnapshotError::InvalidLimit { limit: 0 })
    );
    assert_eq!(
        ledger.snapshot_after(1, 1),
        Err(CheckpointBarrierTimingSnapshotError::CursorAhead {
            after_sequence: 1,
            next_sequence: 1,
        })
    );
    assert!(std::mem::size_of::<CheckpointBarrierTimingRecord>() <= 192);
}
