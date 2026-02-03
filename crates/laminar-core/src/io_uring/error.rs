//! Error types for `io_uring` operations.

use std::io;

/// Errors that can occur during `io_uring` operations.
#[derive(Debug, thiserror::Error)]
pub enum IoUringError {
    /// Failed to create the `io_uring` ring.
    #[error("Failed to create io_uring ring: {0}")]
    RingCreation(#[source] io::Error),

    /// Failed to register buffers with the kernel.
    #[error("Failed to register buffers: {0}")]
    BufferRegistration(#[source] io::Error),

    /// Failed to register file descriptors.
    #[error("Failed to register file descriptors: {0}")]
    FdRegistration(#[source] io::Error),

    /// Submission queue is full.
    #[error("Submission queue is full")]
    SubmissionQueueFull,

    /// No buffers available in the pool.
    #[error("Buffer pool exhausted")]
    BufferPoolExhausted,

    /// Invalid buffer index.
    #[error("Invalid buffer index: {0}")]
    InvalidBufferIndex(u16),

    /// Operation failed with error code.
    #[error("I/O operation failed: {message} (error: {errno})")]
    OperationFailed {
        /// Error message.
        message: String,
        /// Error code from the kernel.
        errno: i32,
    },

    /// Submission failed.
    #[error("Submission failed: {0}")]
    SubmissionFailed(#[source] io::Error),

    /// Wait for completions failed.
    #[error("Wait for completions failed: {0}")]
    WaitFailed(#[source] io::Error),

    /// Invalid configuration.
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Feature not supported on this kernel.
    #[error("Feature not supported: {feature} requires Linux {required_version}")]
    FeatureNotSupported {
        /// The feature that is not supported.
        feature: String,
        /// The required kernel version.
        required_version: String,
    },

    /// `io_uring` not available on this platform.
    #[error("io_uring is not available on this platform (requires Linux 5.10+)")]
    NotAvailable,

    /// File descriptor not registered.
    #[error("File descriptor not registered: {0}")]
    FdNotRegistered(i32),

    /// Ring already closed.
    #[error("Ring already closed")]
    RingClosed,

    /// Pending operation not found.
    #[error("Pending operation not found: {0}")]
    PendingNotFound(u64),
}

impl IoUringError {
    /// Create an operation failed error from an error code.
    #[must_use]
    pub fn from_errno(message: impl Into<String>, errno: i32) -> Self {
        Self::OperationFailed {
            message: message.into(),
            errno,
        }
    }

    /// Check if this error indicates the ring should be recreated.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::RingCreation(_)
                | Self::BufferRegistration(_)
                | Self::FdRegistration(_)
                | Self::RingClosed
                | Self::NotAvailable
        )
    }

    /// Check if this error indicates a transient condition.
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        matches!(self, Self::SubmissionQueueFull | Self::BufferPoolExhausted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = IoUringError::BufferPoolExhausted;
        assert_eq!(err.to_string(), "Buffer pool exhausted");

        let err = IoUringError::from_errno("read failed", -5);
        assert!(err.to_string().contains("read failed"));
        assert!(err.to_string().contains("-5"));
    }

    #[test]
    fn test_is_fatal() {
        assert!(IoUringError::NotAvailable.is_fatal());
        assert!(IoUringError::RingClosed.is_fatal());
        assert!(!IoUringError::BufferPoolExhausted.is_fatal());
        assert!(!IoUringError::SubmissionQueueFull.is_fatal());
    }

    #[test]
    fn test_is_transient() {
        assert!(IoUringError::SubmissionQueueFull.is_transient());
        assert!(IoUringError::BufferPoolExhausted.is_transient());
        assert!(!IoUringError::NotAvailable.is_transient());
        assert!(!IoUringError::RingClosed.is_transient());
    }
}
