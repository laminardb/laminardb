//! Subscription types for FFI.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use super::error::ApiError;
use crate::subscription::{PortalFrame, SubscriptionFrameLease, SubscriptionPortal};

/// One frame from an untyped named subscription.
#[derive(Debug)]
pub enum ArrowSubscriptionFrame {
    /// Rows emitted in one batch.
    Batch {
        /// Arrow rows in the shared-log entry.
        batch: RecordBatch,
        /// Physical sequence within this in-memory object incarnation; not a durable resume token.
        sequence: u64,
        /// Internal process-memory ownership token.
        #[doc(hidden)]
        lease: SubscriptionFrameLease,
    },
    /// Durable progress frontier for this checkpoint.
    Barrier {
        /// Physical sequence of this progress entry within the current object incarnation.
        sequence: u64,
        /// Engine checkpoint epoch.
        epoch: u64,
        /// Engine checkpoint identifier.
        checkpoint_id: u64,
        /// Every logical entry below this sequence is covered by the cut.
        through_sequence: u64,
    },
}

/// Untyped named subscription; no Rust trait bounds, suitable for FFI.
pub struct ArrowSubscription {
    portal: SubscriptionPortal,
    schema: SchemaRef,
    active: bool,
}

impl ArrowSubscription {
    /// Create from internal subscription.
    pub(crate) fn new(portal: SubscriptionPortal) -> Self {
        let schema = portal.schema();
        Self {
            portal,
            schema,
            active: true,
        }
    }

    /// Get the schema.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Blocking wait for the next data or checkpoint frame.
    ///
    /// Returns `Ok(None)` when the subscription is closed.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if delivery fails or when called from an async runtime.
    pub fn next_frame(&mut self) -> Result<Option<ArrowSubscriptionFrame>, ApiError> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(ApiError::subscription(
                "blocking subscription receive is unavailable inside an async runtime; use next_frame_async"
            ));
        }
        futures::executor::block_on(self.next_frame_async())
    }

    /// Asynchronously wait for the next data or checkpoint frame.
    ///
    /// # Errors
    /// Returns `ApiError` if delivery has a gap or the subscribed object fails.
    pub async fn next_frame_async(&mut self) -> Result<Option<ArrowSubscriptionFrame>, ApiError> {
        if !self.active {
            return Ok(None);
        }
        let Some(frame) = self.portal.next_frame().await else {
            self.active = false;
            return Ok(None);
        };
        convert_frame(frame, &mut self.active).map(Some)
    }

    /// Non-blocking poll for the next data or checkpoint frame.
    ///
    /// Returns `Ok(None)` if no frame is currently available.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if delivery has a gap or the subscribed object fails.
    pub fn try_next_frame(&mut self) -> Result<Option<ArrowSubscriptionFrame>, ApiError> {
        if !self.active {
            return Ok(None);
        }

        let Some(frame) = self.portal.try_next_frame() else {
            if self.portal.is_closed() {
                self.active = false;
            }
            return Ok(None);
        };
        convert_frame(frame, &mut self.active).map(Some)
    }

    /// Check if subscription is still active.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.active && !self.portal.is_closed()
    }

    /// Cancel the subscription.
    pub fn cancel(&mut self) {
        self.active = false;
        self.portal.close();
    }
}

fn convert_frame(
    frame: PortalFrame,
    active: &mut bool,
) -> Result<ArrowSubscriptionFrame, ApiError> {
    match frame {
        PortalFrame::Batch {
            batch,
            sequence,
            lease,
        } => Ok(ArrowSubscriptionFrame::Batch {
            batch,
            sequence,
            lease,
        }),
        PortalFrame::Barrier {
            sequence,
            epoch,
            checkpoint_id,
            through_sequence,
        } => Ok(ArrowSubscriptionFrame::Barrier {
            sequence,
            epoch,
            checkpoint_id,
            through_sequence,
        }),
        PortalFrame::Lagged(skipped) => {
            *active = false;
            Err(ApiError::subscription(format!(
                "subscription fell behind by {skipped} entries"
            )))
        }
        PortalFrame::Error { message } => {
            *active = false;
            Err(ApiError::subscription(message))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_send() {
        fn assert_send<T: Send>() {}
        assert_send::<ArrowSubscription>();
        assert_send::<ArrowSubscriptionFrame>();
    }

    #[test]
    fn barrier_is_preserved_for_untyped_consumers() {
        let mut active = true;
        let frame = convert_frame(
            PortalFrame::Barrier {
                sequence: 6,
                epoch: 11,
                checkpoint_id: 11,
                through_sequence: 5,
            },
            &mut active,
        )
        .unwrap();
        assert!(matches!(
            frame,
            ArrowSubscriptionFrame::Barrier {
                sequence: 6,
                epoch: 11,
                checkpoint_id: 11,
                through_sequence: 5,
            }
        ));
        assert!(active);
    }

    #[test]
    fn gap_is_terminal_for_untyped_consumers() {
        let mut active = true;
        let error = convert_frame(PortalFrame::Lagged(5), &mut active).unwrap_err();
        assert!(error.message().contains("5"));
        assert!(!active);
    }
}
