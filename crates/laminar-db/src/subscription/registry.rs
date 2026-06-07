//! Per-name replayable broadcast log for SUBSCRIBE. Send and subscribe
//! are serialized by one mutex per stream so the (replay snapshot, live
//! receiver) pair an `AS OF EPOCH n` consumer gets has no gap.

#![allow(clippy::disallowed_types)] // cold path

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arrow_array::RecordBatch;
use parking_lot::{Mutex, RwLock};
use tokio::sync::broadcast;

const BROADCAST_CAPACITY: usize = 256;

#[derive(Clone, Debug)]
pub(crate) enum MvUpdate {
    Batch(RecordBatch),
    Barrier { epoch: u64, checkpoint_id: u64 },
}

/// Where a new subscriber should start reading.
#[derive(Clone, Copy, Debug)]
pub enum SubscribeStart {
    /// See only what's sent after subscribing.
    Tail,
    /// Replay everything emitted strictly after the barrier with `epoch == n`.
    AsOfEpoch(u64),
}

#[derive(Debug)]
pub(crate) struct ReplayPruned {
    /// Earliest barrier epoch still in the log, or `0` if none retained.
    pub(crate) earliest_retained: u64,
}

struct StreamLog {
    inner: Mutex<StreamLogInner>,
}

struct StreamLogInner {
    buf: VecDeque<MvUpdate>,
    bytes: usize,
    cap: usize,
    sender: broadcast::Sender<MvUpdate>,
}

impl StreamLog {
    fn new(cap: usize) -> Self {
        let (sender, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            inner: Mutex::new(StreamLogInner {
                buf: VecDeque::new(),
                bytes: 0,
                cap,
                sender,
            }),
        }
    }

    fn send(&self, msg: MvUpdate) {
        let mut g = self.inner.lock();
        if g.cap > 0 {
            g.bytes += approx_size(&msg);
            g.buf.push_back(msg.clone());
            evict_to_cap(&mut g);
        }
        let _ = g.sender.send(msg);
    }

    fn subscribe(
        &self,
        start: SubscribeStart,
    ) -> Result<(Vec<MvUpdate>, broadcast::Receiver<MvUpdate>), ReplayPruned> {
        let g = self.inner.lock();
        let replay = match start {
            SubscribeStart::Tail => Vec::new(),
            SubscribeStart::AsOfEpoch(n) => slice_after_epoch(&g.buf, n)?,
        };
        let rx = g.sender.subscribe();
        Ok((replay, rx))
    }

    fn set_cap(&self, cap: usize) {
        let mut g = self.inner.lock();
        g.cap = cap;
        evict_to_cap(&mut g);
    }

    fn subscriber_count(&self) -> usize {
        self.inner.lock().sender.receiver_count()
    }
}

fn evict_to_cap(g: &mut StreamLogInner) {
    while g.bytes > g.cap && !g.buf.is_empty() {
        if let Some(evicted) = g.buf.pop_front() {
            g.bytes = g.bytes.saturating_sub(approx_size(&evicted));
        }
    }
}

fn slice_after_epoch(buf: &VecDeque<MvUpdate>, n: u64) -> Result<Vec<MvUpdate>, ReplayPruned> {
    let mut found_at = None;
    let mut earliest = u64::MAX;
    for (i, msg) in buf.iter().enumerate() {
        if let MvUpdate::Barrier { epoch, .. } = msg {
            earliest = earliest.min(*epoch);
            if *epoch == n {
                found_at = Some(i);
            }
        }
    }
    match found_at {
        Some(i) => Ok(buf.iter().skip(i + 1).cloned().collect()),
        None => Err(ReplayPruned {
            earliest_retained: if earliest == u64::MAX { 0 } else { earliest },
        }),
    }
}

fn approx_size(msg: &MvUpdate) -> usize {
    match msg {
        MvUpdate::Batch(b) => b.get_array_memory_size(),
        MvUpdate::Barrier { .. } => 16,
    }
}

pub(crate) struct SubscriptionRegistry {
    streams: RwLock<HashMap<String, Arc<StreamLog>>>,
}

impl SubscriptionRegistry {
    pub(crate) fn new() -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
        }
    }

    /// `cap == 0` disables retention; the live broadcast still works.
    pub(crate) fn configure(&self, name: &str, cap: usize) {
        let log = self.get_or_create(name);
        log.set_cap(cap);
    }

    pub(crate) fn subscribe(
        &self,
        name: &str,
        start: SubscribeStart,
    ) -> Result<(Vec<MvUpdate>, broadcast::Receiver<MvUpdate>), ReplayPruned> {
        let log = self.get_or_create(name);
        log.subscribe(start)
    }

    pub(crate) fn send_batch(&self, name: &str, batch: RecordBatch) {
        if let Some(log) = self.streams.read().get(name).cloned() {
            log.send(MvUpdate::Batch(batch));
        }
    }

    pub(crate) fn broadcast_barrier(&self, epoch: u64, checkpoint_id: u64) {
        let msg = MvUpdate::Barrier {
            epoch,
            checkpoint_id,
        };
        for log in self.streams.read().values() {
            log.send(msg.clone());
        }
    }

    pub(crate) fn drop_name(&self, name: &str) -> bool {
        self.streams.write().remove(name).is_some()
    }

    pub(crate) fn subscriber_count(&self, name: &str) -> usize {
        self.streams
            .read()
            .get(name)
            .map_or(0, |log| log.subscriber_count())
    }

    /// Only the cluster subscription router consults this.
    #[cfg(feature = "cluster")]
    pub(crate) fn active_subscription_names(&self) -> Vec<String> {
        self.streams
            .read()
            .iter()
            .filter(|(_, log)| log.subscriber_count() > 0)
            .map(|(name, _)| name.clone())
            .collect()
    }

    fn get_or_create(&self, name: &str) -> Arc<StreamLog> {
        if let Some(log) = self.streams.read().get(name) {
            return Arc::clone(log);
        }
        Arc::clone(
            self.streams
                .write()
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(StreamLog::new(0))),
        )
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
        let (replay, mut rx) = reg.subscribe("mv", SubscribeStart::Tail).unwrap();
        assert!(replay.is_empty());

        reg.send_batch("mv", batch(&[1, 2, 3]));
        let MvUpdate::Batch(b) = rx.recv().await.unwrap() else {
            panic!("expected batch");
        };
        assert_eq!(b.num_rows(), 3);

        reg.broadcast_barrier(7, 42);
        let MvUpdate::Barrier {
            epoch,
            checkpoint_id,
        } = rx.recv().await.unwrap()
        else {
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
        let (_, mut rx) = reg.subscribe("mv", SubscribeStart::Tail).unwrap();
        assert!(reg.drop_name("mv"));
        assert!(matches!(
            rx.recv().await,
            Err(broadcast::error::RecvError::Closed)
        ));
    }

    #[test]
    fn subscriber_count_tracks_attach_drop() {
        let reg = SubscriptionRegistry::new();
        let (_, r1) = reg.subscribe("mv", SubscribeStart::Tail).unwrap();
        let (_, r2) = reg.subscribe("mv", SubscribeStart::Tail).unwrap();
        assert_eq!(reg.subscriber_count("mv"), 2);
        drop(r1);
        assert_eq!(reg.subscriber_count("mv"), 1);
        drop(r2);
        assert_eq!(reg.subscriber_count("mv"), 0);
    }

    #[test]
    fn tail_subscribe_does_not_replay_history() {
        let reg = SubscriptionRegistry::new();
        reg.configure("mv", 1 << 20);
        reg.broadcast_barrier(1, 1);
        reg.send_batch("mv", batch(&[10]));
        reg.broadcast_barrier(2, 2);

        let (replay, _rx) = reg.subscribe("mv", SubscribeStart::Tail).unwrap();
        assert!(replay.is_empty());
    }

    #[test]
    fn as_of_returns_messages_after_matching_barrier() {
        let reg = SubscriptionRegistry::new();
        reg.configure("mv", 1 << 20);
        reg.broadcast_barrier(1, 10);
        reg.send_batch("mv", batch(&[10]));
        reg.broadcast_barrier(2, 20);
        reg.send_batch("mv", batch(&[20]));
        reg.broadcast_barrier(3, 30);

        let (replay, _rx) = reg.subscribe("mv", SubscribeStart::AsOfEpoch(2)).unwrap();
        // Everything after the epoch-2 barrier: one batch, then the epoch-3 barrier.
        assert_eq!(replay.len(), 2);
        assert!(matches!(&replay[0], MvUpdate::Batch(b) if b.num_rows() == 1));
        assert!(matches!(
            &replay[1],
            MvUpdate::Barrier {
                epoch: 3,
                checkpoint_id: 30
            }
        ));
    }

    #[test]
    fn as_of_below_head_is_pruned() {
        let reg = SubscriptionRegistry::new();
        reg.configure("mv", 1 << 20);
        reg.broadcast_barrier(5, 50);
        reg.send_batch("mv", batch(&[1]));
        reg.broadcast_barrier(6, 60);

        let err = reg
            .subscribe("mv", SubscribeStart::AsOfEpoch(3))
            .unwrap_err();
        assert_eq!(err.earliest_retained, 5);
    }

    #[test]
    fn unconfigured_log_does_not_retain() {
        let reg = SubscriptionRegistry::new();
        // No configure(): cap stays at 0; live still works but replay is empty.
        let _ = reg.subscribe("mv", SubscribeStart::Tail).unwrap();
        reg.broadcast_barrier(1, 1);
        reg.send_batch("mv", batch(&[1]));

        // Fresh AS OF EPOCH 1 should be pruned because nothing was buffered.
        let err = reg
            .subscribe("mv", SubscribeStart::AsOfEpoch(1))
            .unwrap_err();
        assert_eq!(err.earliest_retained, 0);
    }

    #[test]
    fn eviction_trims_to_cap() {
        let reg = SubscriptionRegistry::new();
        // Cap small enough that one big batch will evict prior entries.
        let one_batch_bytes = batch(&[0]).get_array_memory_size();
        reg.configure("mv", one_batch_bytes + 1);

        reg.broadcast_barrier(1, 1);
        for i in 0..8i64 {
            reg.send_batch("mv", batch(&[i]));
        }
        reg.broadcast_barrier(9, 9);

        // The early barrier should have been evicted by now.
        let err = reg
            .subscribe("mv", SubscribeStart::AsOfEpoch(1))
            .unwrap_err();
        assert!(err.earliest_retained >= 9 || err.earliest_retained == 0);
    }

    #[test]
    fn single_oversize_message_is_evicted_not_retained() {
        // A batch larger than the entire cap must not stick around — better
        // to surface pruned at AS OF time than silently blow the budget.
        let reg = SubscriptionRegistry::new();
        reg.configure("mv", 8); // tiny cap, smaller than any real batch
        reg.broadcast_barrier(1, 1);
        reg.send_batch("mv", batch(&[1, 2, 3, 4, 5, 6, 7, 8]));

        let err = reg
            .subscribe("mv", SubscribeStart::AsOfEpoch(1))
            .unwrap_err();
        assert_eq!(err.earliest_retained, 0, "buffer should be empty");
    }

    #[test]
    fn lowering_cap_trims_existing_buffer() {
        let reg = SubscriptionRegistry::new();
        reg.configure("mv", 1 << 20);
        for i in 0..4i64 {
            reg.send_batch("mv", batch(&[i]));
        }
        reg.broadcast_barrier(1, 1);

        // Drop the cap to 0; everything should be evicted.
        reg.configure("mv", 0);
        let err = reg
            .subscribe("mv", SubscribeStart::AsOfEpoch(1))
            .unwrap_err();
        assert_eq!(err.earliest_retained, 0);
    }
}
