use std::io;

use super::*;
use crate::checkpoint::CheckpointBarrier;

#[tokio::test]
async fn registered_peer_still_fails_when_transport_is_disabled() {
    let sender = ShuffleSender::new(1, uuid::Uuid::from_u128(2));
    sender.register_peer(2, "127.0.0.1:9000".parse().unwrap());

    let error = sender
        .send_to(2, &ShuffleMessage::Barrier(CheckpointBarrier::new(1, 1)))
        .await
        .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
}

#[tokio::test]
async fn noncanonical_barrier_is_rejected_in_embedded_mode() {
    let sender = ShuffleSender::new(1, uuid::Uuid::from_u128(2));
    let error = sender
        .send_to(2, &ShuffleMessage::Barrier(CheckpointBarrier::new(7, 8)))
        .await
        .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(error.to_string(), NONCANONICAL_BARRIER);
}
