//! Completion routing for three-ring I/O.
//!
//! Tracks pending operations and routes completions to the appropriate
//! handlers based on their ring affinity.

use std::time::Instant;

use rustc_hash::FxHashMap;

use super::affinity::{OperationType, RingAffinity};

/// A pending operation awaiting completion.
#[derive(Debug, Clone)]
pub struct PendingOperation {
    /// User data identifying this operation.
    pub user_data: u64,
    /// Which ring the operation was submitted to.
    pub affinity: RingAffinity,
    /// The type of operation.
    pub op_type: Option<OperationType>,
    /// When the operation was submitted.
    pub submitted_at: Instant,
    /// Buffer index if using registered buffers.
    pub buffer_index: Option<u16>,
    /// File descriptor involved.
    pub fd: Option<i32>,
    /// Expected bytes for read/write operations.
    pub expected_bytes: Option<u32>,
}

impl PendingOperation {
    /// Create a new pending operation with the given user data and ring affinity.
    #[must_use]
    pub fn new(user_data: u64, affinity: RingAffinity) -> Self {
        Self {
            user_data,
            affinity,
            op_type: None,
            submitted_at: Instant::now(),
            buffer_index: None,
            fd: None,
            expected_bytes: None,
        }
    }

    /// Set the operation type.
    #[must_use]
    pub fn with_op_type(mut self, op_type: OperationType) -> Self {
        self.op_type = Some(op_type);
        self
    }

    /// Set the registered buffer index.
    #[must_use]
    pub const fn with_buffer_index(mut self, index: u16) -> Self {
        self.buffer_index = Some(index);
        self
    }

    /// Set the file descriptor.
    #[must_use]
    pub const fn with_fd(mut self, fd: i32) -> Self {
        self.fd = Some(fd);
        self
    }

    /// Set the expected byte count for read/write operations.
    #[must_use]
    pub const fn with_expected_bytes(mut self, bytes: u32) -> Self {
        self.expected_bytes = Some(bytes);
        self
    }

    /// Elapsed time since this operation was submitted.
    #[must_use]
    pub fn latency(&self) -> std::time::Duration {
        self.submitted_at.elapsed()
    }
}

/// A routed completion with metadata.
#[derive(Debug, Clone)]
pub struct RoutedCompletion {
    /// User data identifying the operation.
    pub user_data: u64,
    /// Result code (bytes transferred or negative errno).
    pub result: i32,
    /// Flags from the completion.
    pub flags: u32,
    /// Which ring this completion came from.
    pub affinity: RingAffinity,
    /// Submission timestamp (for latency calculation).
    pub submitted_at: Option<Instant>,
    /// Operation type if known.
    pub op_type: Option<OperationType>,
}

impl RoutedCompletion {
    /// Returns `true` if the operation completed successfully.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        self.result >= 0
    }

    /// Returns the OS error if the operation failed.
    #[must_use]
    pub fn error(&self) -> Option<std::io::Error> {
        if self.result < 0 {
            Some(std::io::Error::from_raw_os_error(-self.result))
        } else {
            None
        }
    }

    /// Returns the number of bytes transferred, or `None` on error.
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    pub const fn bytes_transferred(&self) -> Option<usize> {
        if self.result >= 0 {
            Some(self.result as usize)
        } else {
            None
        }
    }

    /// Get the latency from submission to completion.
    #[must_use]
    pub fn latency(&self) -> Option<std::time::Duration> {
        self.submitted_at.map(|t| t.elapsed())
    }
}

/// Routes completions to appropriate handlers based on ring affinity.
///
/// Tracks pending operations and creates `RoutedCompletion` values
/// with full metadata when completions arrive.
#[derive(Debug, Default)]
pub struct CompletionRouter {
    /// Pending operations by `user_data`.
    pending: FxHashMap<u64, PendingOperation>,
    /// Next `user_data` ID for operations without explicit IDs.
    next_id: u64,
}

impl CompletionRouter {
    /// Create a new completion router.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending: FxHashMap::default(),
            next_id: 0,
        }
    }

    /// Generate the next `user_data` ID.
    pub fn next_user_data(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        id
    }

    /// Track a pending operation.
    pub fn track(&mut self, op: PendingOperation) {
        self.pending.insert(op.user_data, op);
    }

    /// Track a simple operation with just `user_data` and affinity.
    pub fn track_simple(&mut self, user_data: u64, affinity: RingAffinity) {
        self.pending
            .insert(user_data, PendingOperation::new(user_data, affinity));
    }

    /// Route a completion to produce a `RoutedCompletion`.
    ///
    /// Returns the routed completion with metadata from the pending operation
    /// if found, or a basic completion with unknown affinity.
    pub fn route(&mut self, user_data: u64, result: i32, flags: u32) -> RoutedCompletion {
        if let Some(op) = self.pending.remove(&user_data) {
            RoutedCompletion {
                user_data,
                result,
                flags,
                affinity: op.affinity,
                submitted_at: Some(op.submitted_at),
                op_type: op.op_type,
            }
        } else {
            // Unknown operation - default to main ring
            RoutedCompletion {
                user_data,
                result,
                flags,
                affinity: RingAffinity::Main,
                submitted_at: None,
                op_type: None,
            }
        }
    }

    /// Get a pending operation by `user_data`.
    #[must_use]
    pub fn get(&self, user_data: u64) -> Option<&PendingOperation> {
        self.pending.get(&user_data)
    }

    /// Remove a pending operation without routing.
    pub fn remove(&mut self, user_data: u64) -> Option<PendingOperation> {
        self.pending.remove(&user_data)
    }

    /// Check if an operation is pending.
    #[must_use]
    pub fn is_pending(&self, user_data: u64) -> bool {
        self.pending.contains_key(&user_data)
    }

    /// Get the number of pending operations.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Get the number of pending operations by affinity.
    #[must_use]
    pub fn pending_by_affinity(&self, affinity: RingAffinity) -> usize {
        self.pending
            .values()
            .filter(|op| op.affinity == affinity)
            .count()
    }

    /// Clear all pending operations.
    pub fn clear(&mut self) {
        self.pending.clear();
    }

    /// Get all pending `user_data` values.
    #[must_use]
    pub fn pending_ids(&self) -> Vec<u64> {
        self.pending.keys().copied().collect()
    }

    /// Get the oldest pending operation (for timeout detection).
    #[must_use]
    pub fn oldest_pending(&self) -> Option<&PendingOperation> {
        self.pending.values().min_by_key(|op| op.submitted_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_operation_new() {
        let op = PendingOperation::new(42, RingAffinity::Latency);
        assert_eq!(op.user_data, 42);
        assert_eq!(op.affinity, RingAffinity::Latency);
        assert!(op.op_type.is_none());
        assert!(op.buffer_index.is_none());
    }

    #[test]
    fn test_pending_operation_builder() {
        let op = PendingOperation::new(1, RingAffinity::Main)
            .with_op_type(OperationType::WalWrite)
            .with_buffer_index(5)
            .with_fd(10)
            .with_expected_bytes(4096);

        assert_eq!(op.user_data, 1);
        assert_eq!(op.affinity, RingAffinity::Main);
        assert_eq!(op.op_type, Some(OperationType::WalWrite));
        assert_eq!(op.buffer_index, Some(5));
        assert_eq!(op.fd, Some(10));
        assert_eq!(op.expected_bytes, Some(4096));
    }

    #[test]
    fn test_routed_completion_success() {
        let completion = RoutedCompletion {
            user_data: 1,
            result: 100,
            flags: 0,
            affinity: RingAffinity::Latency,
            submitted_at: Some(Instant::now()),
            op_type: Some(OperationType::NetworkRecv),
        };

        assert!(completion.is_success());
        assert!(completion.error().is_none());
        assert_eq!(completion.bytes_transferred(), Some(100));
    }

    #[test]
    fn test_routed_completion_error() {
        let completion = RoutedCompletion {
            user_data: 1,
            result: -5, // EIO
            flags: 0,
            affinity: RingAffinity::Main,
            submitted_at: None,
            op_type: None,
        };

        assert!(!completion.is_success());
        assert!(completion.error().is_some());
        assert!(completion.bytes_transferred().is_none());
    }

    #[test]
    fn test_router_new() {
        let router = CompletionRouter::new();
        assert_eq!(router.pending_count(), 0);
    }

    #[test]
    fn test_router_next_user_data() {
        let mut router = CompletionRouter::new();
        assert_eq!(router.next_user_data(), 0);
        assert_eq!(router.next_user_data(), 1);
        assert_eq!(router.next_user_data(), 2);
    }

    #[test]
    fn test_router_track_and_route() {
        let mut router = CompletionRouter::new();

        let op = PendingOperation::new(42, RingAffinity::Latency)
            .with_op_type(OperationType::NetworkRecv);
        router.track(op);

        assert_eq!(router.pending_count(), 1);
        assert!(router.is_pending(42));

        let completion = router.route(42, 100, 0);
        assert_eq!(completion.user_data, 42);
        assert_eq!(completion.result, 100);
        assert_eq!(completion.affinity, RingAffinity::Latency);
        assert_eq!(completion.op_type, Some(OperationType::NetworkRecv));
        assert!(completion.submitted_at.is_some());

        // Should be removed after routing
        assert_eq!(router.pending_count(), 0);
        assert!(!router.is_pending(42));
    }

    #[test]
    fn test_router_track_simple() {
        let mut router = CompletionRouter::new();
        router.track_simple(1, RingAffinity::Main);
        router.track_simple(2, RingAffinity::Latency);
        router.track_simple(3, RingAffinity::Poll);

        assert_eq!(router.pending_count(), 3);
        assert_eq!(router.pending_by_affinity(RingAffinity::Main), 1);
        assert_eq!(router.pending_by_affinity(RingAffinity::Latency), 1);
        assert_eq!(router.pending_by_affinity(RingAffinity::Poll), 1);
    }

    #[test]
    fn test_router_route_unknown() {
        let mut router = CompletionRouter::new();

        // Route an operation that wasn't tracked
        let completion = router.route(999, 0, 0);
        assert_eq!(completion.user_data, 999);
        assert_eq!(completion.affinity, RingAffinity::Main); // Default
        assert!(completion.submitted_at.is_none());
    }

    #[test]
    fn test_router_get_and_remove() {
        let mut router = CompletionRouter::new();
        router.track_simple(1, RingAffinity::Latency);

        let op = router.get(1);
        assert!(op.is_some());
        assert_eq!(op.unwrap().affinity, RingAffinity::Latency);

        let removed = router.remove(1);
        assert!(removed.is_some());
        assert_eq!(router.pending_count(), 0);
    }

    #[test]
    fn test_router_clear() {
        let mut router = CompletionRouter::new();
        router.track_simple(1, RingAffinity::Latency);
        router.track_simple(2, RingAffinity::Main);

        router.clear();
        assert_eq!(router.pending_count(), 0);
    }

    #[test]
    fn test_router_pending_ids() {
        let mut router = CompletionRouter::new();
        router.track_simple(10, RingAffinity::Latency);
        router.track_simple(20, RingAffinity::Main);
        router.track_simple(30, RingAffinity::Poll);

        let mut ids = router.pending_ids();
        ids.sort_unstable();
        assert_eq!(ids, vec![10, 20, 30]);
    }

    #[test]
    fn test_router_oldest_pending() {
        let mut router = CompletionRouter::new();

        // Track operations with small delays to ensure ordering
        router.track_simple(1, RingAffinity::Latency);
        std::thread::sleep(std::time::Duration::from_millis(1));
        router.track_simple(2, RingAffinity::Main);

        let oldest = router.oldest_pending();
        assert!(oldest.is_some());
        assert_eq!(oldest.unwrap().user_data, 1);
    }
}
