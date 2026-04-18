//! Fans a barrier across two TCP peers; each peer's `BarrierTracker`
//! aligns once its local + shuffle inputs have both seen the same
//! checkpoint id.

use std::sync::Arc;
use std::time::Duration;

use laminar_core::checkpoint::barrier::{flags, CheckpointBarrier};
use laminar_core::shuffle::{
    fan_out_barrier, BarrierTracker, ShuffleMessage, ShuffleReceiver, ShuffleSender,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn barrier_fan_out_aligns_on_both_receivers() {
    let recv_a = ShuffleReceiver::bind(10, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let recv_b = ShuffleReceiver::bind(11, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();

    let sender = ShuffleSender::new(1);
    sender.register_peer(10, recv_a.local_addr()).await;
    sender.register_peer(11, recv_b.local_addr()).await;

    let tracker_a = Arc::new(BarrierTracker::new(2));
    let tracker_b = Arc::new(BarrierTracker::new(2));
    let barrier = CheckpointBarrier {
        checkpoint_id: 42,
        epoch: 7,
        flags: flags::FULL_SNAPSHOT,
    };

    // The "other" input on each peer had already seen the barrier
    // locally (e.g., from a source that injected it directly).
    assert!(tracker_a.observe(0, barrier).is_none());
    assert!(tracker_b.observe(0, barrier).is_none());

    // Leader fans out across the shuffle. Both receivers should see
    // the frame, feed it to their trackers, and those trackers should
    // fire.
    fan_out_barrier(&sender, &[10, 11], barrier).await.unwrap();

    for (recv, tracker) in [(&recv_a, &tracker_a), (&recv_b, &tracker_b)] {
        let (_from, msg) = tokio::time::timeout(Duration::from_secs(2), recv.recv())
            .await
            .expect("deadline")
            .expect("channel open");
        let got_barrier = match msg {
            ShuffleMessage::Barrier(b) => b,
            other => panic!("expected Barrier, got {other:?}"),
        };
        assert_eq!(got_barrier, barrier, "wire barrier matches sent");
        let fired = tracker
            .observe(1, got_barrier)
            .expect("tracker aligns after shuffle barrier arrives");
        assert_eq!(fired.checkpoint_id, 42);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fan_out_errors_on_unregistered_peer() {
    let sender = ShuffleSender::new(1);
    let barrier = CheckpointBarrier {
        checkpoint_id: 1,
        epoch: 1,
        flags: 0,
    };
    let err = fan_out_barrier(&sender, &[99], barrier).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}
