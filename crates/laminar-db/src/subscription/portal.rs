//! Per-subscriber cursor over the shared subscription log.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::physical_expr::PhysicalExpr;
use futures::FutureExt;

use super::registry::{ChargedUpdate, MvUpdate, SubscriptionRead, SubscriptionReader};

/// Keeps the process-wide subscription charge alive with an emitted batch.
#[doc(hidden)]
#[derive(Clone)]
pub struct SubscriptionFrameLease {
    _owner: ChargedUpdate,
}

impl std::fmt::Debug for SubscriptionFrameLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SubscriptionFrameLease")
    }
}

/// One frame emitted toward the wire.
#[derive(Debug, Clone)]
pub enum PortalFrame {
    /// Rows produced in a cycle.
    Batch {
        /// Arrow rows in the shared-log entry.
        batch: RecordBatch,
        /// Physical sequence within this in-memory object incarnation; not a durable resume token.
        sequence: u64,
        /// Internal process-memory ownership token.
        #[doc(hidden)]
        lease: SubscriptionFrameLease,
    },
    /// Progress frontier for a durably committed checkpoint.
    Barrier {
        /// Physical sequence of this progress entry within the current object incarnation.
        sequence: u64,
        /// Engine checkpoint epoch.
        epoch: u64,
        /// Engine checkpoint id.
        checkpoint_id: u64,
        /// Every shared-log entry with sequence below this value is covered by the cut.
        through_sequence: u64,
    },
    /// Consumer fell behind by exactly `skipped` shared-log entries. This is
    /// terminal because continuing would hide rows or checkpoint markers.
    Lagged(u64),
    /// The subscription cannot continue without returning an invalid result.
    Error {
        /// Human-readable failure detail.
        message: String,
    },
}

/// One `SUBSCRIBE` consumer.
#[derive(Debug)]
pub struct SubscriptionPortal {
    name: String,
    schema: SchemaRef,
    reader: Option<SubscriptionReader>,
    closed: bool,
    filter: Option<Arc<dyn PhysicalExpr>>,
}

impl SubscriptionPortal {
    pub(crate) fn open(
        name: impl Into<String>,
        schema: SchemaRef,
        reader: SubscriptionReader,
    ) -> Self {
        Self::new_inner(name, schema, reader, None)
    }

    pub(crate) fn open_with_filter(
        name: impl Into<String>,
        schema: SchemaRef,
        reader: SubscriptionReader,
        filter: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self::new_inner(name, schema, reader, Some(filter))
    }

    fn new_inner(
        name: impl Into<String>,
        schema: SchemaRef,
        reader: SubscriptionReader,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self {
            name: name.into(),
            schema,
            reader: Some(reader),
            closed: false,
            filter,
        }
    }

    /// Schema of the subscribed object.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Next frame, or `None` after a terminal frame or explicit close.
    pub async fn next_frame(&mut self) -> Option<PortalFrame> {
        if self.closed {
            return None;
        }

        loop {
            let read = self.reader.as_mut()?.next().await;
            if let Some(frame) = self.process_read(read) {
                return Some(frame);
            }
        }
    }

    /// Return the next immediately available frame without waiting.
    pub fn try_next_frame(&mut self) -> Option<PortalFrame> {
        if self.closed {
            return None;
        }

        loop {
            let read = self.reader.as_mut()?.next().now_or_never()?;
            if let Some(frame) = self.process_read(read) {
                return Some(frame);
            }
        }
    }

    fn process_read(&mut self, read: SubscriptionRead) -> Option<PortalFrame> {
        let frame = match read {
            SubscriptionRead::Update { sequence, update } => translate(sequence, update),
            SubscriptionRead::Lagged(skipped) => {
                tracing::warn!(
                    subscription = %self.name,
                    skipped,
                    "subscription cursor was evicted; closing"
                );
                self.close();
                return Some(PortalFrame::Lagged(skipped));
            }
            SubscriptionRead::Terminal(message) => {
                tracing::warn!(
                    subscription = %self.name,
                    %message,
                    "subscription log terminated; closing"
                );
                self.close();
                return Some(PortalFrame::Error { message });
            }
        };

        let PortalFrame::Batch {
            batch,
            sequence,
            lease,
        } = frame
        else {
            if matches!(&frame, PortalFrame::Error { .. }) {
                self.close();
            }
            return Some(frame);
        };
        let Some(filter) = self.filter.as_ref() else {
            return Some(PortalFrame::Batch {
                batch,
                sequence,
                lease,
            });
        };
        match crate::filter_compile::apply(&batch, filter.as_ref()) {
            Ok(Some(filtered)) => Some(PortalFrame::Batch {
                batch: filtered,
                sequence,
                lease,
            }),
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(
                    subscription = %self.name,
                    %error,
                    "subscription filter failed; closing"
                );
                let message = error.to_string();
                self.close();
                Some(PortalFrame::Error { message })
            }
        }
    }

    /// Stop reading and release the subscriber registration. Idempotent.
    pub fn close(&mut self) {
        self.closed = true;
        self.reader = None;
    }

    /// True after `close()` has been called.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed
    }
}

impl Drop for SubscriptionPortal {
    fn drop(&mut self) {
        self.close();
    }
}

fn translate(sequence: u64, update: ChargedUpdate) -> PortalFrame {
    match update.as_ref() {
        MvUpdate::Batch(batch) => {
            let batch = batch.clone();
            PortalFrame::Batch {
                batch,
                sequence,
                lease: SubscriptionFrameLease { _owner: update },
            }
        }
        MvUpdate::Barrier {
            epoch,
            checkpoint_id,
            through_sequence,
        } => PortalFrame::Barrier {
            sequence,
            epoch: *epoch,
            checkpoint_id: *checkpoint_id,
            through_sequence: *through_sequence,
        },
        MvUpdate::Error(message) => PortalFrame::Error {
            message: message.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;
    use std::time::Duration;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::super::registry::{
        approx_size, MvUpdate, SubscribeStart, SubscriptionRegistry, MAX_LIVE_BATCH_BYTES,
    };
    use super::*;

    fn schema() -> SchemaRef {
        StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn batch(ids: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![StdArc::new(Int64Array::from(ids))]).unwrap()
    }

    fn open(registry: &SubscriptionRegistry, start: SubscribeStart) -> SubscriptionPortal {
        let reader = registry.subscribe("mv", start).unwrap();
        SubscriptionPortal::open("mv", schema(), reader)
    }

    #[tokio::test]
    async fn portal_forwards_batch_and_barrier() {
        let registry = SubscriptionRegistry::new();
        let mut portal = open(&registry, SubscribeStart::Tail);
        registry.send_batch("mv", batch(vec![1, 2])).unwrap();
        registry.broadcast_barrier(7, 7);

        assert!(matches!(
            portal.next_frame().await,
            Some(PortalFrame::Batch { batch, sequence: 0, .. }) if batch.num_rows() == 2
        ));
        assert!(matches!(
            portal.next_frame().await,
            Some(PortalFrame::Barrier {
                sequence: 1,
                epoch: 7,
                checkpoint_id: 7,
                through_sequence: 1,
            })
        ));
    }

    #[test]
    fn portal_try_next_is_non_blocking_and_ordered() {
        let registry = SubscriptionRegistry::new();
        let mut portal = open(&registry, SubscribeStart::Tail);
        assert!(portal.try_next_frame().is_none());

        registry.send_batch("mv", batch(vec![1])).unwrap();
        registry.broadcast_barrier(3, 3);
        assert!(matches!(
            portal.try_next_frame(),
            Some(PortalFrame::Batch { batch, sequence: 0, .. }) if batch.num_rows() == 1
        ));
        assert!(matches!(
            portal.try_next_frame(),
            Some(PortalFrame::Barrier {
                sequence: 1,
                epoch: 3,
                checkpoint_id: 3,
                through_sequence: 1,
            })
        ));
        assert!(portal.try_next_frame().is_none());
    }

    #[tokio::test]
    async fn portal_reports_object_drop_then_closes() {
        let registry = SubscriptionRegistry::new();
        let mut portal = open(&registry, SubscribeStart::Tail);
        assert!(registry.drop_name("mv"));

        let frame = tokio::time::timeout(Duration::from_millis(500), portal.next_frame())
            .await
            .unwrap();
        assert!(matches!(
            frame,
            Some(PortalFrame::Error { message }) if message == "object dropped"
        ));
        assert!(portal.next_frame().await.is_none());
    }

    #[tokio::test]
    async fn portal_emits_exact_lag_as_final_frame() {
        let registry = SubscriptionRegistry::new();
        let mut portal = open(&registry, SubscribeStart::Tail);
        let rows = (MAX_LIVE_BATCH_BYTES / 2) / std::mem::size_of::<i64>();
        for value in 0..6_i64 {
            registry.send_batch("mv", batch(vec![value; rows])).unwrap();
        }

        assert!(matches!(
            portal.next_frame().await,
            Some(PortalFrame::Lagged(skipped)) if skipped > 0
        ));
        assert!(portal.next_frame().await.is_none());
    }

    #[tokio::test]
    async fn portal_reads_shared_as_of_suffix_before_new_live_entries() {
        let registry = SubscriptionRegistry::new();
        registry.configure("mv", 1 << 20);
        registry.broadcast_barrier(1, 1);
        registry.send_batch("mv", batch(vec![10])).unwrap();
        registry.broadcast_barrier(2, 2);
        registry.send_batch("mv", batch(vec![20])).unwrap();
        let mut portal = open(&registry, SubscribeStart::AsOfEpoch(1));
        registry.send_batch("mv", batch(vec![30])).unwrap();

        let mut rows = Vec::new();
        let mut barriers = Vec::new();
        for _ in 0..4 {
            match portal.next_frame().await.unwrap() {
                PortalFrame::Batch { batch, .. } => rows.push(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                ),
                PortalFrame::Barrier { epoch, .. } => barriers.push(epoch),
                PortalFrame::Lagged(skipped) => panic!("unexpected lag of {skipped}"),
                PortalFrame::Error { message } => panic!("unexpected error: {message}"),
            }
        }
        assert_eq!(rows, vec![10, 20, 30]);
        assert_eq!(barriers, vec![2]);
    }

    #[test]
    fn close_releases_registration_once() {
        let registry = SubscriptionRegistry::new();
        let mut portal = open(&registry, SubscribeStart::Tail);
        assert_eq!(registry.subscriber_count("mv"), 1);
        portal.close();
        portal.close();
        assert_eq!(registry.subscriber_count("mv"), 0);
    }

    #[tokio::test]
    async fn held_batch_frame_blocks_process_budget_reuse() {
        let sample = batch(vec![1, 2, 3]);
        let entry_bytes = approx_size(&MvUpdate::Batch(sample.clone()));
        let registry = SubscriptionRegistry::with_storage_budget(entry_bytes);
        let mut portal = open(&registry, SubscribeStart::Tail);
        registry.send_batch("mv", sample.clone()).unwrap();

        let frame = portal.next_frame().await.unwrap();
        assert!(matches!(&frame, PortalFrame::Batch { .. }));
        assert_eq!(registry.charged_bytes(), entry_bytes);

        registry.configure("contender", entry_bytes.saturating_mul(4));
        assert!(registry.send_batch("contender", sample.clone()).is_err());
        assert_eq!(registry.charged_bytes(), entry_bytes);

        drop(frame);
        assert_eq!(registry.charged_bytes(), 0);
        registry.configure("replacement", entry_bytes.saturating_mul(4));
        registry.send_batch("replacement", sample).unwrap();
        assert_eq!(registry.charged_bytes(), entry_bytes);
    }

    #[tokio::test]
    async fn two_portals_share_one_charge_until_both_frames_drop() {
        let sample = batch(vec![1, 2, 3]);
        let entry_bytes = approx_size(&MvUpdate::Batch(sample.clone()));
        let registry = SubscriptionRegistry::with_storage_budget(entry_bytes);
        let mut first = open(&registry, SubscribeStart::Tail);
        let mut second = open(&registry, SubscribeStart::Tail);
        registry.send_batch("mv", sample).unwrap();

        let first_frame = first.next_frame().await.unwrap();
        let second_frame = second.next_frame().await.unwrap();
        assert_eq!(registry.charged_bytes(), entry_bytes);

        drop(first_frame);
        assert_eq!(registry.charged_bytes(), entry_bytes);
        drop(second_frame);
        assert_eq!(registry.charged_bytes(), 0);
    }

    #[tokio::test]
    async fn filtered_batch_keeps_the_original_entry_charge() {
        let sample = batch(vec![-1, 2]);
        let entry_bytes = approx_size(&MvUpdate::Batch(sample.clone()));
        let registry = SubscriptionRegistry::with_storage_budget(entry_bytes);
        let reader = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
        let context = datafusion::prelude::SessionContext::new();
        let filter = crate::filter_compile::compile(&context, "id > 0", &schema())
            .await
            .unwrap();
        let mut portal = SubscriptionPortal::open_with_filter("mv", schema(), reader, filter);
        registry.send_batch("mv", sample).unwrap();

        let frame = portal.next_frame().await.unwrap();
        assert!(matches!(
            &frame,
            PortalFrame::Batch { batch, .. } if batch.num_rows() == 1
        ));
        assert_eq!(registry.charged_bytes(), entry_bytes);

        drop(frame);
        assert_eq!(registry.charged_bytes(), 0);
    }

    #[tokio::test]
    async fn object_drop_does_not_release_a_held_frame_charge() {
        let sample = batch(vec![1]);
        let entry_bytes = approx_size(&MvUpdate::Batch(sample.clone()));
        let registry = SubscriptionRegistry::with_storage_budget(entry_bytes);
        let mut portal = open(&registry, SubscribeStart::Tail);
        registry.send_batch("mv", sample).unwrap();
        let frame = portal.next_frame().await.unwrap();

        assert!(registry.drop_name("mv"));
        assert_eq!(registry.charged_bytes(), entry_bytes);

        drop(frame);
        assert_eq!(registry.charged_bytes(), 0);
    }

    #[tokio::test]
    async fn filter_evaluation_failure_is_terminal_and_visible() {
        let registry = SubscriptionRegistry::new();
        let reader = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
        let context = datafusion::prelude::SessionContext::new();
        let filter = crate::filter_compile::compile(&context, "id > 0", &schema())
            .await
            .unwrap();
        let mut portal = SubscriptionPortal::open_with_filter("mv", schema(), reader, filter);

        let incompatible_schema =
            StdArc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let incompatible = RecordBatch::try_new(
            incompatible_schema,
            vec![StdArc::new(arrow_array::StringArray::from(vec!["bad"]))],
        )
        .unwrap();
        registry.send_batch("mv", incompatible).unwrap();

        assert!(matches!(
            portal.next_frame().await,
            Some(PortalFrame::Error { message }) if message.contains("filter")
        ));
        assert!(portal.next_frame().await.is_none());
        assert_eq!(registry.subscriber_count("mv"), 0);
    }
}
