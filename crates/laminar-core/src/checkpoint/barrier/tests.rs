use super::*;

#[test]
fn test_barrier_size() {
    assert_eq!(std::mem::size_of::<CheckpointBarrier>(), 24);
}

#[test]
fn test_barrier_flags() {
    let barrier = CheckpointBarrier::new(1, 1);
    assert!(barrier.is_canonical());
    assert!(!barrier.is_full_snapshot());
    assert!(!barrier.is_drain());
    assert!(!barrier.is_cancel());

    let full = CheckpointBarrier::full_snapshot(1, 1);
    assert!(full.is_full_snapshot());
    assert!(!full.is_drain());

    let drain = CheckpointBarrier {
        checkpoint_id: 1,
        epoch: 1,
        flags: flags::DRAIN,
    };
    assert!(drain.is_drain());
    assert!(!CheckpointBarrier::new(0, 0).is_canonical());
    assert!(!CheckpointBarrier::new(1, 2).is_canonical());
}

#[test]
fn shuffle_flush_flags_roundtrip_bounds_and_activity() {
    for (wave, activity) in [
        (0, false),
        (0, true),
        (1, false),
        (MAX_SHUFFLE_FLUSH_WAVE, true),
    ] {
        let encoded = encode_shuffle_flush_flags(wave, activity).unwrap();
        assert_eq!(decode_shuffle_flush_flags(encoded), Ok((wave, activity)));
    }
    assert!(encode_shuffle_flush_flags(MAX_SHUFFLE_FLUSH_WAVE + 1, false).is_err());
}

#[test]
fn shuffle_flush_flags_reject_untagged_and_reserved_bits() {
    assert!(decode_shuffle_flush_flags(0).is_err());
    assert!(decode_shuffle_flush_flags(flags::HANDOFF).is_err());
    let encoded = encode_shuffle_flush_flags(7, true).unwrap();
    assert!(decode_shuffle_flush_flags(encoded | flags::FULL_SNAPSHOT).is_err());
}

#[test]
fn test_barrier_roundtrip_via_injector() {
    let injector = CheckpointBarrierInjector::new();
    let handle = injector.handle();
    let expected = CheckpointBarrier {
        checkpoint_id: 42,
        epoch: 42,
        flags: flags::DRAIN,
    };

    assert!(injector.trigger(expected));
    assert_eq!(handle.poll(), Some(expected));
    assert!(handle.poll().is_none(), "cleared after one poll");
}

#[test]
fn test_stream_message_variants() {
    let event: StreamMessage<String> = StreamMessage::Event("hello".into());
    assert!(event.is_event());
    assert!(!event.is_barrier());
    assert!(!event.is_watermark());

    let watermark: StreamMessage<String> = StreamMessage::Watermark(1000);
    assert!(watermark.is_watermark());

    let barrier: StreamMessage<String> = StreamMessage::Barrier(CheckpointBarrier::new(1, 1));
    assert!(barrier.is_barrier());
    assert_eq!(barrier.as_barrier().unwrap().checkpoint_id, 1);
}

#[test]
fn test_injector_poll_no_barrier() {
    let injector = CheckpointBarrierInjector::new();
    let handle = injector.handle();

    // No barrier pending
    assert!(handle.poll().is_none());
}

#[test]
fn test_injector_trigger_and_poll() {
    let injector = CheckpointBarrierInjector::new();
    let handle = injector.handle();

    // Trigger barrier
    let expected = CheckpointBarrier::full_snapshot(42, 42);
    assert!(injector.trigger(expected));

    // Poll should return the barrier
    let barrier = handle.poll().unwrap();
    assert_eq!(barrier, expected);
    assert!(barrier.is_full_snapshot());

    // Second poll should return None (already claimed)
    assert!(handle.poll().is_none());
}

#[test]
fn test_injector_multiple_handles() {
    let injector = CheckpointBarrierInjector::new();
    let handle1 = injector.handle();
    let handle2 = injector.handle();

    assert!(injector.trigger(CheckpointBarrier::new(1, 1)));

    // Only one handle should claim it
    let r1 = handle1.poll();
    let r2 = handle2.poll();

    // Exactly one should succeed
    assert!(r1.is_some() || r2.is_some());
    if r1.is_some() {
        assert!(r2.is_none());
    }
}

#[test]
fn test_injector_supports_full_width_id_and_flags() {
    let injector = CheckpointBarrierInjector::new();
    let handle = injector.handle();
    let checkpoint_id = u64::from(u32::MAX) + 17;
    let epoch = checkpoint_id;
    let barrier_flags = 1_u64 << 63;
    let expected = CheckpointBarrier {
        checkpoint_id,
        epoch,
        flags: barrier_flags,
    };

    assert!(injector.trigger(expected));
    assert_eq!(handle.poll(), Some(expected));
}

#[test]
fn test_pending_trigger_is_rejected_without_overwrite() {
    let injector = CheckpointBarrierInjector::new();
    let handle = injector.handle();
    let first = CheckpointBarrier {
        checkpoint_id: u64::MAX,
        epoch: u64::MAX,
        flags: flags::DRAIN,
    };
    let second = CheckpointBarrier::full_snapshot(7, 7);

    assert!(injector.trigger(first));
    assert!(!injector.trigger(second));

    assert_eq!(handle.poll(), Some(first));
    assert!(injector.trigger(second));
}

#[test]
fn test_cancel_exact_removes_matching_pending_barrier() {
    let injector = CheckpointBarrierInjector::new();
    let handle = injector.handle();
    let pending = CheckpointBarrier::full_snapshot(41, 41);

    assert!(injector.trigger(pending));
    assert!(injector.cancel_exact(pending));
    assert!(handle.poll().is_none());
    assert!(injector.can_trigger());
}

#[test]
fn test_cancel_exact_preserves_mismatched_pending_barrier() {
    let injector = CheckpointBarrierInjector::new();
    let handle = injector.handle();
    let pending = CheckpointBarrier::full_snapshot(41, 41);

    assert!(injector.trigger(pending));
    assert!(!injector.cancel_exact(CheckpointBarrier {
        epoch: pending.epoch + 1,
        ..pending
    }));
    assert!(!injector.cancel_exact(CheckpointBarrier {
        checkpoint_id: pending.checkpoint_id + 1,
        ..pending
    }));
    assert_eq!(handle.poll(), Some(pending));
}

#[test]
fn test_cancel_exact_after_poll_does_not_claim_completed_barrier() {
    let injector = CheckpointBarrierInjector::new();
    let handle = injector.handle();
    let pending = CheckpointBarrier::new(41, 41);

    assert!(injector.trigger(pending));
    assert_eq!(handle.poll(), Some(pending));
    assert!(!injector.cancel_exact(pending));
    assert!(injector.can_trigger());
}

#[test]
fn test_cancel_exact_and_poll_have_exactly_one_winner() {
    let injector = Arc::new(CheckpointBarrierInjector::new());
    let handle = injector.handle();
    let pending = CheckpointBarrier::new(41, 41);
    assert!(injector.trigger(pending));

    let start = Arc::new(std::sync::Barrier::new(3));
    let cancel_thread = {
        let injector = Arc::clone(&injector);
        let start = Arc::clone(&start);
        std::thread::spawn(move || {
            start.wait();
            injector.cancel_exact(pending)
        })
    };
    let poll_thread = {
        let start = Arc::clone(&start);
        std::thread::spawn(move || {
            start.wait();
            handle.poll()
        })
    };

    start.wait();
    let cancelled = cancel_thread.join().unwrap();
    let polled = poll_thread.join().unwrap();
    assert_eq!(cancelled, polled.is_none());
    if !cancelled {
        assert_eq!(polled, Some(pending));
    }
    assert!(injector.can_trigger());
}

#[test]
fn test_noncanonical_identity_is_rejected_without_mutation() {
    let injector = CheckpointBarrierInjector::new();
    let handle = injector.handle();

    assert!(!injector.trigger(CheckpointBarrier::new(0, 1)));
    assert!(!injector.trigger(CheckpointBarrier::new(1, 0)));
    assert!(!injector.trigger(CheckpointBarrier::new(1, 2)));
    assert!(injector.can_trigger());
    assert!(handle.poll().is_none());

    let expected = CheckpointBarrier::new(9, 9);
    assert!(injector.trigger(expected));
    assert!(!injector.trigger(CheckpointBarrier::new(0, 11)));
    assert!(!injector.trigger(CheckpointBarrier::new(11, 0)));
    assert!(!injector.trigger(CheckpointBarrier::new(11, 12)));
    assert_eq!(handle.poll(), Some(expected));
}

#[test]
fn test_concurrent_triggers_admit_exactly_one() {
    let injector = Arc::new(CheckpointBarrierInjector::new());
    let handle = injector.handle();
    let mut threads = Vec::new();

    for checkpoint_id in 1..=16 {
        let injector = Arc::clone(&injector);
        threads.push(std::thread::spawn(move || {
            let barrier = CheckpointBarrier {
                checkpoint_id,
                epoch: checkpoint_id,
                flags: checkpoint_id << 40,
            };
            (barrier, injector.trigger(barrier))
        }));
    }

    let accepted = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .filter_map(|(barrier, accepted)| accepted.then_some(barrier))
        .collect::<Vec<_>>();
    assert_eq!(accepted.len(), 1);

    assert_eq!(handle.poll(), Some(accepted[0]));
}
