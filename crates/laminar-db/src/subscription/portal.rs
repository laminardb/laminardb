//! Per-subscriber portal: pump task forwards broadcast updates to the wire.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use crossfire::{mpsc, AsyncRx, MAsyncTx};
use datafusion::physical_expr::PhysicalExpr;
use tokio::sync::broadcast;

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
}

const OUTBOUND_CAPACITY: usize = 256;
pub(crate) const MAX_SUBSCRIBERS_PER_MV: usize = 64;

/// One SUBSCRIBE consumer.
#[derive(Debug)]
pub struct SubscriptionPortal {
    schema: SchemaRef,
    outbound: AsyncRx<mpsc::Array<PortalFrame>>,
    closed: Arc<AtomicBool>,
}

impl SubscriptionPortal {
    pub(crate) fn open(
        name: impl Into<String>,
        schema: SchemaRef,
        rx: broadcast::Receiver<MvUpdate>,
    ) -> Self {
        Self::spawn(name, schema, rx, None)
    }

    pub(crate) fn open_with_filter(
        name: impl Into<String>,
        schema: SchemaRef,
        rx: broadcast::Receiver<MvUpdate>,
        filter: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self::spawn(name, schema, rx, Some(filter))
    }

    fn spawn(
        name: impl Into<String>,
        schema: SchemaRef,
        rx: broadcast::Receiver<MvUpdate>,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let (tx, outbound) = mpsc::bounded_async::<PortalFrame>(OUTBOUND_CAPACITY);
        let closed = Arc::new(AtomicBool::new(false));
        tokio::spawn(pump_loop(name.into(), rx, tx, Arc::clone(&closed), filter));
        Self {
            schema,
            outbound,
            closed,
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

    /// Signal the pump to stop. Idempotent.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
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

async fn pump_loop(
    name: String,
    mut broadcast_rx: broadcast::Receiver<MvUpdate>,
    tx: MAsyncTx<mpsc::Array<PortalFrame>>,
    closed: Arc<AtomicBool>,
    filter: Option<Arc<dyn PhysicalExpr>>,
) {
    while !closed.load(Ordering::Acquire) {
        let frame = match broadcast_rx.recv().await {
            Ok(MvUpdate::Batch(batch)) => match filter.as_ref() {
                Some(f) => match crate::filter_compile::apply(&batch, f.as_ref()) {
                    Ok(Some(b)) => PortalFrame::Batch(b),
                    Ok(None) => continue,
                    Err(e) => {
                        tracing::warn!(subscription = %name, error = %e, "filter failed; closing");
                        return;
                    }
                },
                None => PortalFrame::Batch(batch),
            },
            Ok(MvUpdate::Barrier {
                epoch,
                checkpoint_id,
            }) => PortalFrame::Barrier {
                epoch,
                checkpoint_id,
            },
            Err(broadcast::error::RecvError::Closed) => return,
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(subscription = %name, skipped = n, "lagged; closing");
                return;
            }
        };
        if tx.send(frame).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;
    use std::time::Duration;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    use super::super::registry::SubscriptionRegistry;
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
        let rx = reg.subscribe("mv");
        let mut portal = SubscriptionPortal::open("mv", schema(), rx);

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
        let rx = reg.subscribe("mv");
        let mut portal = SubscriptionPortal::open("mv", schema(), rx);

        reg.send_batch("mv", batch(&[1]));
        let _ = portal.next_frame().await;

        reg.drop_name("mv");

        let frame = tokio::time::timeout(Duration::from_millis(500), portal.next_frame())
            .await
            .unwrap();
        assert!(frame.is_none());
    }

    #[tokio::test]
    async fn portal_closes_on_lag() {
        let reg = SubscriptionRegistry::new();
        let rx = reg.subscribe("mv");
        let mut portal = SubscriptionPortal::open("mv", schema(), rx);

        for i in 0..1024 {
            reg.send_batch("mv", batch(&[i]));
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            while portal.next_frame().await.is_some() {}
        })
        .await
        .expect("portal must close after lag");
    }

    #[tokio::test]
    async fn close_is_idempotent() {
        let reg = SubscriptionRegistry::new();
        let rx = reg.subscribe("mv");
        let portal = SubscriptionPortal::open("mv", schema(), rx);

        portal.close();
        portal.close();
        assert!(portal.is_closed());
    }
}
