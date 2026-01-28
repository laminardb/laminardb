//! Streaming API error types.
//!
//! This module defines error types for the streaming API including
//! source, sink, and channel operations.

use std::fmt;

/// Error type for streaming operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamingError {
    /// Channel is full and backpressure strategy is Error.
    ChannelFull,

    /// Channel is closed (all receivers dropped).
    ChannelClosed,

    /// Channel is disconnected (all senders dropped).
    Disconnected,

    /// Invalid configuration provided.
    InvalidConfig(String),

    /// Schema mismatch during `push_arrow` operation.
    SchemaMismatch {
        /// Expected schema field names.
        expected: Vec<String>,
        /// Actual schema field names.
        actual: Vec<String>,
    },

    /// Operation timed out.
    Timeout,

    /// Internal error.
    Internal(String),
}

impl fmt::Display for StreamingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChannelFull => write!(f, "channel is full"),
            Self::ChannelClosed => write!(f, "channel is closed"),
            Self::Disconnected => write!(f, "channel is disconnected"),
            Self::InvalidConfig(msg) => write!(f, "invalid configuration: {msg}"),
            Self::SchemaMismatch { expected, actual } => {
                write!(f, "schema mismatch: expected {expected:?}, got {actual:?}")
            }
            Self::Timeout => write!(f, "operation timed out"),
            Self::Internal(msg) => write!(f, "internal error: {msg}"),
        }
    }
}

impl std::error::Error for StreamingError {}

/// Error returned from `try_push` operations.
#[derive(Debug)]
pub struct TryPushError<T> {
    /// The value that could not be pushed.
    pub value: T,
    /// The error that occurred.
    pub error: StreamingError,
}

impl<T> TryPushError<T> {
    /// Creates a new error indicating the channel is full.
    #[must_use]
    pub fn full(value: T) -> Self {
        Self {
            value,
            error: StreamingError::ChannelFull,
        }
    }

    /// Creates a new error indicating the channel is closed.
    #[must_use]
    pub fn closed(value: T) -> Self {
        Self {
            value,
            error: StreamingError::ChannelClosed,
        }
    }

    /// Returns true if the error is due to a full channel.
    #[must_use]
    pub fn is_full(&self) -> bool {
        matches!(self.error, StreamingError::ChannelFull)
    }

    /// Returns true if the error is due to a closed channel.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        matches!(self.error, StreamingError::ChannelClosed)
    }

    /// Consumes the error and returns the value that could not be pushed.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T: fmt::Debug> fmt::Display for TryPushError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "try_push failed: {}", self.error)
    }
}

impl<T: fmt::Debug> std::error::Error for TryPushError<T> {}

/// Error returned from `recv` operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    /// Channel is disconnected (all senders dropped).
    Disconnected,

    /// Operation timed out.
    Timeout,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disconnected => write!(f, "channel disconnected"),
            Self::Timeout => write!(f, "recv timed out"),
        }
    }
}

impl std::error::Error for RecvError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_error_display() {
        assert_eq!(StreamingError::ChannelFull.to_string(), "channel is full");
        assert_eq!(StreamingError::ChannelClosed.to_string(), "channel is closed");
        assert_eq!(StreamingError::Disconnected.to_string(), "channel is disconnected");
        assert_eq!(
            StreamingError::InvalidConfig("bad".to_string()).to_string(),
            "invalid configuration: bad"
        );
        assert_eq!(StreamingError::Timeout.to_string(), "operation timed out");
    }

    #[test]
    fn test_try_push_error() {
        let err = TryPushError::full(42);
        assert!(err.is_full());
        assert!(!err.is_closed());
        assert_eq!(err.into_inner(), 42);

        let err = TryPushError::closed("test");
        assert!(!err.is_full());
        assert!(err.is_closed());
        assert_eq!(err.into_inner(), "test");
    }

    #[test]
    fn test_recv_error_display() {
        assert_eq!(RecvError::Disconnected.to_string(), "channel disconnected");
        assert_eq!(RecvError::Timeout.to_string(), "recv timed out");
    }

    #[test]
    fn test_schema_mismatch_display() {
        let err = StreamingError::SchemaMismatch {
            expected: vec!["a".to_string(), "b".to_string()],
            actual: vec!["x".to_string(), "y".to_string()],
        };
        assert!(err.to_string().contains("schema mismatch"));
    }
}
