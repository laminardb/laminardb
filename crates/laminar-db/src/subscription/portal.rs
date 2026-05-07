//! Per-subscriber portal: pump task forwards broadcast updates to the wire.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use crossfire::{mpsc, AsyncRx, MAsyncTx};
use datafusion::physical_expr::PhysicalExpr;
use tokio::sync::{broadcast, Notify};

use super::registry::MvUpdate;

/// One frame emitted toward the wire.
#[derive(Debug, Clone)]
pub enum PortalFrame {
    /// Rows produced in a cycle.
    Batch(RecordBatch),
    /// Rows preceding this marker are durable as of `epoch`.
    Barrier {
        /// Engine checkpoint epoch.
        epoch: u64,
        /// Engine checkpoint id.
        checkpoint_id: u64,
    },
    /// Consumer fell behind by `skipped` messages and the broadcast dropped
    /// them. The portal closes immediately after this frame; the wire layer
    /// translates it into a client-visible error so the disconnect isn't
    /// silent.
    Lagged(u64),
}

const OUTBOUND_CAPACITY: usize = 256;
pub(crate) const MAX_SUBSCRIBERS_PER_MV: usize = 64;

/// One SUBSCRIBE consumer.
#[derive(Debug)]
pub struct SubscriptionPortal {
    schema: SchemaRef,
    outbound: AsyncRx<mpsc::Array<PortalFrame>>,
    closed: Arc<AtomicBool>,
    wake: Arc<Notify>,
}

impl SubscriptionPortal {
    pub(crate) fn open(
        name: impl Into<String>,
        schema: SchemaRef,
        replay: Vec<MvUpdate>,
        rx: broadcast::Receiver<MvUpdate>,
    ) -> Self {
        Self::spawn(name, schema, replay, rx, None)
    }

    pub(crate) fn open_with_filter(
        name: impl Into<String>,
        schema: SchemaRef,
        replay: Vec<MvUpdate>,
        rx: broadcast::Receiver<MvUpdate>,
        filter: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self::spawn(name, schema, replay, rx, Some(filter))
    }

    fn spawn(
        name: impl Into<String>,
        schema: SchemaRef,
        replay: Vec<MvUpdate>,
        rx: broadcast::Receiver<MvUpdate>,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let (tx, outbound) = mpsc::bounded_async::<PortalFrame>(OUTBOUND_CAPACITY);
        let closed = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(Notify::new());
        tokio::spawn(pump_loop(
            name.into(),
            replay,
            rx,
            tx,
            Arc::clone(&closed),
            Arc::clone(&wake),
            filter,
        ));
        Self {
            schema,
            outbound,
            closed,
            wake,
        }
    }

    /// Schema of the subscribed object.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Next frame, or `None` once the pump exits.
    pub async fn next_frame(&mut self) -> Option<PortalFrame> {
        self.outbound.recv().await.ok()
    }

    /// Signal the pump to stop. Idempotent. Wakes the pump if it's parked
    /// on `broadcast_rx.recv()` so it can re-check the flag and exit.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.wake.notify_waiters();
    }

    /// True after `close()` has been called.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl Drop for SubscriptionPortal {
    fn drop(&mut self) {
        self.close();
    }
}

/// Translate one `MvUpdate` into a `PortalFrame`, applying the filter if any.
/// Returns `Ok(None)` for batches the filter excluded entirely, and `Err(())`
/// when the filter itself failed (the pump should close).
fn translate(
    msg: MvUpdate,
    filter: Option<&Arc<dyn PhysicalExpr>>,
    name: &str,
) -> Result<Option<PortalFrame>, ()> {
    match msg {
        MvUpdate::Batch(batch) => match filter {
            Some(f) => match crate::filter_compile::apply(&batch, f.as_ref()) {
                Ok(Some(b)) => Ok(Some(PortalFrame::Batch(b))),
                Ok(None) => Ok(None),
                Err(e) => {
                    tracing::warn!(subscription = %name, error = %e, "filter failed; closing");
                    Err(())
                }
            },
            None => Ok(Some(PortalFrame::Batch(batch))),
        },
        MvUpdate::Barrier {
            epoch,
            checkpoint_id,
        } => Ok(Some(PortalFrame::Barrier {
            epoch,
            checkpoint_id,
        })),
    }
}

async fn pump_loop(
    name: String,
    replay: Vec<MvUpdate>,
    mut broadcast_rx: broadcast::Receiver<MvUpdate>,
    tx: MAsyncTx<mpsc::Array<PortalFrame>>,
    closed: Arc<AtomicBool>,
    wake: Arc<Notify>,
    filter: Option<Arc<dyn PhysicalExpr>>,
) {
    for msg in replay {
        if closed.load(Ordering::Acquire) {
            return;
        }
        match translate(msg, filter.as_ref(), &name) {
            Ok(Some(frame)) => {
                if tx.send(frame).await.is_err() {
                    return;
                }
            }
            Ok(None) => {}
            Err(()) => return,
        }
    }
    while !closed.load(Ordering::Acquire) {
        let recv = tokio::select! {
            biased;
            () = wake.notified() => continue,
            r = broadcast_rx.recv() => r,
        };
        let msg = match recv {
            Ok(m) => m,
            Err(broadcast::error::RecvError::Closed) => return,
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(subscription = %name, skipped = n, "lagged; closing");
                let _ = tx.send(PortalFrame::Lagged(n)).await;
                return;
            }
        };
        match translate(msg, filter.as_ref(), &name) {
            Ok(Some(frame)) => {
                if tx.send(frame).await.is_err() {
                    return;
                }
            }
            Ok(None) => {}
            Err(()) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;
    use std::time::Duration;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    use super::super::registry::{SubscribeStart, SubscriptionRegistry};
    use super::*;

    fn schema() -> SchemaRef {
        StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn batch(ids: &[i64]) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![StdArc::new(Int64Array::from(ids.to_vec()))]).unwrap()
    }

    #[tokio::test]
    async fn portal_forwards_batch_and_barrier() {
        let reg = SubscriptionRegistry::new();
        let (replay, rx) = reg.subscribe("mv", SubscribeStart::Tail).unwrap();
        let mut portal = SubscriptionPortal::open("mv", schema(), replay, rx);

        reg.send_batch("mv", batch(&[1, 2]));
        reg.broadcast_barrier(7, 99);

        let f1 = portal.next_frame().await.expect("frame 1");
        let PortalFrame::Batch(b) = f1 else {
            panic!("expected batch, got {f1:?}");
        };
        assert_eq!(b.num_rows(), 2);

        let f2 = portal.next_frame().await.expect("frame 2");
        let PortalFrame::Barrier {
            epoch,
            checkpoint_id,
        } = f2
        else {
            panic!("expected barrier, got {f2:?}");
        };
        assert_eq!(epoch, 7);
        assert_eq!(checkpoint_id, 99);
    }

    #[tokio::test]
    async fn portal_closes_on_drop_name() {
        let reg = SubscriptionRegistry::new();
        let (replay, rx) = reg.subscribe("mv", SubscribeStart::Tail).unwrap();
        let mut portal = SubscriptionPortal::open("mv", schema(), replay, rx);

        reg.send_batch("mv", batch(&[1]));
        let _ = portal.next_frame().await;

        reg.drop_name("mv");

        let frame = tokio::time::timeout(Duration::from_millis(500), portal.next_frame())
            .await
            .unwrap();
        assert!(frame.is_none());
    }

    #[tokio::test]
    async fn portal_emits_lagged_as_final_frame() {
        let reg = SubscriptionRegistry::new();
        let (replay, rx) = reg.subscribe("mv", SubscribeStart::Tail).unwrap();
        let mut portal = SubscriptionPortal::open("mv", schema(), replay, rx);

        for i in 0..1024 {
            reg.send_batch("mv", batch(&[i]));
        }

        let frames = tokio::time::timeout(Duration::from_secs(1), async {
            let mut frames = Vec::new();
            while let Some(frame) = portal.next_frame().await {
                frames.push(frame);
            }
            frames
        })
        .await
        .expect("portal must close after lag");

        assert!(
            matches!(frames.last(), Some(PortalFrame::Lagged(_))),
            "last frame must be Lagged, got: {:?}",
            frames.last()
        );
    }

    #[tokio::test]
    async fn portal_drains_replay_before_live() {
        let reg = SubscriptionRegistry::new();
        reg.configure("mv", 1 << 20);

        // Two epochs of pre-existing history.
        reg.broadcast_barrier(1, 1);
        reg.send_batch("mv", batch(&[10]));
        reg.broadcast_barrier(2, 2);
        reg.send_batch("mv", batch(&[20]));

        // Subscribe AS OF EPOCH 1: the client has everything up to and
        // including the epoch-1 barrier; replay must cover everything after
        // it. That's batch[10], then barrier(2), then batch[20].
        let (replay, rx) = reg.subscribe("mv", SubscribeStart::AsOfEpoch(1)).unwrap();
        let mut portal = SubscriptionPortal::open("mv", schema(), replay, rx);

        reg.send_batch("mv", batch(&[30]));

        let mut row_seq = Vec::new();
        let mut barriers = Vec::new();
        for _ in 0..4 {
            match portal.next_frame().await.unwrap() {
                PortalFrame::Batch(b) => {
                    let v = b
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0);
                    row_seq.push(v);
                }
                PortalFrame::Barrier { epoch, .. } => barriers.push(epoch),
                PortalFrame::Lagged(n) => panic!("unexpected lag: {n}"),
            }
        }
        assert_eq!(row_seq, vec![10, 20, 30]);
        assert_eq!(barriers, vec![2]);
    }

    #[tokio::test]
    async fn close_is_idempotent() {
        let reg = SubscriptionRegistry::new();
        let (replay, rx) = reg.subscribe("mv", SubscribeStart::Tail).unwrap();
        let portal = SubscriptionPortal::open("mv", schema(), replay, rx);

        portal.close();
        portal.close();
        assert!(portal.is_closed());
    }
}
