//! Per-name broadcast registry for SUBSCRIBE.

#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;

use arrow_array::RecordBatch;
use parking_lot::RwLock;
use tokio::sync::broadcast;

const BROADCAST_CAPACITY: usize = 256;

#[derive(Clone, Debug)]
pub(crate) enum MvUpdate {
    Batch(RecordBatch),
    Barrier { epoch: u64, checkpoint_id: u64 },
}

pub(crate) struct SubscriptionRegistry {
    senders: RwLock<HashMap<String, broadcast::Sender<MvUpdate>>>,
}

impl SubscriptionRegistry {
    pub(crate) fn new() -> Self {
        Self { senders: RwLock::new(HashMap::new()) }
    }

    /// Attach a receiver, allocating the channel on first call.
    pub(crate) fn subscribe(&self, name: &str) -> broadcast::Receiver<MvUpdate> {
        if let Some(tx) = self.senders.read().get(name) {
            return tx.subscribe();
        }
        self.senders
            .write()
            .entry(name.to_string())
            .or_insert_with(|| broadcast::channel(BROADCAST_CAPACITY).0)
            .subscribe()
    }

    /// Send a batch to subscribers of `name`. Cheap when nobody is attached.
    pub(crate) fn send_batch(&self, name: &str, batch: RecordBatch) {
        if let Some(tx) = self.senders.read().get(name) {
            let _ = tx.send(MvUpdate::Batch(batch));
        }
    }

    /// Broadcast a barrier marker to every registered name.
    pub(crate) fn broadcast_barrier(&self, epoch: u64, checkpoint_id: u64) {
        let msg = MvUpdate::Barrier { epoch, checkpoint_id };
        for tx in self.senders.read().values() {
            let _ = tx.send(msg.clone());
        }
    }

    /// Drop the broadcast for `name`. Existing receivers see `Closed`.
    pub(crate) fn drop_name(&self, name: &str) -> bool {
        self.senders.write().remove(name).is_some()
    }

    pub(crate) fn subscriber_count(&self, name: &str) -> usize {
        self.senders.read().get(name).map_or(0, broadcast::Sender::receiver_count)
    }
}

impl Default for SubscriptionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn batch(ids: &[i64]) -> RecordBatch {
        let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![StdArc::new(Int64Array::from(ids.to_vec()))]).unwrap()
    }

    #[tokio::test]
    async fn round_trip() {
        let reg = SubscriptionRegistry::new();
        let mut rx = reg.subscribe("mv");

        reg.send_batch("mv", batch(&[1, 2, 3]));
        let MvUpdate::Batch(b) = rx.recv().await.unwrap() else {
            panic!("expected batch");
        };
        assert_eq!(b.num_rows(), 3);

        reg.broadcast_barrier(7, 42);
        let MvUpdate::Barrier { epoch, checkpoint_id } = rx.recv().await.unwrap() else {
            panic!("expected barrier");
        };
        assert_eq!((epoch, checkpoint_id), (7, 42));
    }

    #[tokio::test]
    async fn no_subscribers_is_noop() {
        let reg = SubscriptionRegistry::new();
        reg.send_batch("nobody", batch(&[1]));
        reg.broadcast_barrier(1, 1);
        assert_eq!(reg.subscriber_count("nobody"), 0);
    }

    #[tokio::test]
    async fn drop_name_closes_receivers() {
        let reg = SubscriptionRegistry::new();
        let mut rx = reg.subscribe("mv");
        assert!(reg.drop_name("mv"));
        assert!(matches!(rx.recv().await, Err(broadcast::error::RecvError::Closed)));
    }

    #[test]
    fn subscriber_count_tracks_attach_drop() {
        let reg = SubscriptionRegistry::new();
        let r1 = reg.subscribe("mv");
        let r2 = reg.subscribe("mv");
        assert_eq!(reg.subscriber_count("mv"), 2);
        drop(r1);
        assert_eq!(reg.subscriber_count("mv"), 1);
        drop(r2);
        assert_eq!(reg.subscriber_count("mv"), 0);
    }
}
