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
mod tests;
