//! Subscription types for FFI.

use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use super::error::ApiError;
use crate::catalog::ArrowRecord;

/// Untyped subscription returning Arrow `RecordBatch`es.
///
/// Unlike `TypedSubscription<T>`, this doesn't require Rust trait bounds,
/// making it suitable for FFI where the binding language handles deserialization.
///
/// # Example
///
/// ```rust,ignore
/// let sub = conn.subscribe("ohlc_stream")?;
/// while let Some(batch) = sub.try_next()? {
///     // Process batch via Arrow C Data Interface
/// }
/// ```
pub struct ArrowSubscription {
    inner: laminar_core::streaming::Subscription<ArrowRecord>,
    schema: SchemaRef,
    active: bool,
}

impl ArrowSubscription {
    /// Create from internal subscription.
    #[allow(dead_code)] // Used when subscribe() is fully implemented
    pub(crate) fn new(
        inner: laminar_core::streaming::Subscription<ArrowRecord>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            inner,
            schema,
            active: true,
        }
    }

    /// Get the schema.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Blocking wait for next batch.
    ///
    /// Returns `Ok(None)` when the subscription is closed.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if receiving fails.
    #[allow(clippy::should_implement_trait)] // FFI method name, not Iterator
    pub fn next(&mut self) -> Result<Option<RecordBatch>, ApiError> {
        if !self.active {
            return Ok(None);
        }

        match self.inner.recv() {
            Ok(batch) => Ok(Some(batch)),
            Err(laminar_core::streaming::RecvError::Disconnected) => {
                self.active = false;
                Ok(None)
            }
            Err(e) => Err(ApiError::subscription(e.to_string())),
        }
    }

    /// Receive with timeout.
    ///
    /// Returns `Ok(None)` when the subscription is closed.
    ///
    /// # Errors
    ///
    /// Returns `ApiError::subscription_timeout()` on timeout.
    pub fn next_timeout(&mut self, timeout: Duration) -> Result<Option<RecordBatch>, ApiError> {
        if !self.active {
            return Ok(None);
        }

        match self.inner.recv_timeout(timeout) {
            Ok(batch) => Ok(Some(batch)),
            Err(laminar_core::streaming::RecvError::Disconnected) => {
                self.active = false;
                Ok(None)
            }
            Err(laminar_core::streaming::RecvError::Timeout) => {
                Err(ApiError::subscription_timeout())
            }
        }
    }

    /// Non-blocking poll for next batch.
    ///
    /// Returns `Ok(None)` if no batch is currently available.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if receiving fails.
    pub fn try_next(&mut self) -> Result<Option<RecordBatch>, ApiError> {
        if !self.active {
            return Ok(None);
        }

        Ok(self.inner.poll())
    }

    /// Check if subscription is still active.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.active && !self.inner.is_disconnected()
    }

    /// Cancel the subscription.
    pub fn cancel(&mut self) {
        self.active = false;
    }
}

// SAFETY: ArrowSubscription wraps Subscription<ArrowRecord> which uses
// lock-free channels with atomic operations.
unsafe impl Send for ArrowSubscription {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_send() {
        fn assert_send<T: Send>() {}
        assert_send::<ArrowSubscription>();
    }
}
