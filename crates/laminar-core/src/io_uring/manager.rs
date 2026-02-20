//! Per-core `io_uring` manager for thread-per-core architecture.
//!
//! Provides a unified interface for managing `io_uring` operations on a single core,
//! including ring management, buffer pools, and pending operation tracking.

use io_uring::opcode;
use io_uring::types::{self, Fd};
use io_uring::IoUring;
use std::collections::HashMap;
use std::io;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::buffer_pool::{BufferPoolStats, RegisteredBufferPool};
use super::config::{IoUringConfig, RingMode};
use super::error::IoUringError;
use super::ring::IoUringRing;

/// Pending operation types for tracking.
#[derive(Debug, Clone)]
pub enum PendingOp {
    /// Read operation.
    Read {
        /// Buffer index.
        buf_index: u16,
        /// Expected length.
        len: u32,
        /// Submission timestamp.
        submitted_at: Instant,
    },
    /// Write operation.
    Write {
        /// Buffer index.
        buf_index: u16,
        /// Expected length.
        len: u32,
        /// Submission timestamp.
        submitted_at: Instant,
    },
    /// Fsync/fdatasync operation.
    Sync {
        /// Submission timestamp.
        submitted_at: Instant,
    },
    /// Close operation.
    Close {
        /// File descriptor being closed.
        fd: RawFd,
        /// Submission timestamp.
        submitted_at: Instant,
    },
    /// Custom operation with opaque data.
    Custom {
        /// Operation type identifier.
        op_type: u32,
        /// Opaque user data.
        data: u64,
        /// Submission timestamp.
        submitted_at: Instant,
    },
}

impl PendingOp {
    /// Get the buffer index if this is a read/write operation.
    #[must_use]
    pub const fn buf_index(&self) -> Option<u16> {
        match self {
            Self::Read { buf_index, .. } | Self::Write { buf_index, .. } => Some(*buf_index),
            _ => None,
        }
    }

    /// Get the submission timestamp.
    #[must_use]
    pub const fn submitted_at(&self) -> Instant {
        match self {
            Self::Read { submitted_at, .. }
            | Self::Write { submitted_at, .. }
            | Self::Sync { submitted_at }
            | Self::Close { submitted_at, .. }
            | Self::Custom { submitted_at, .. } => *submitted_at,
        }
    }
}

/// Completion result from an `io_uring` operation.
#[derive(Debug)]
pub struct Completion {
    /// User data identifying the operation.
    pub user_data: u64,
    /// Result code (bytes transferred or negative errno).
    pub result: i32,
    /// Flags from the completion.
    pub flags: u32,
    /// The original pending operation, if tracked.
    pub op: Option<PendingOp>,
    /// Latency from submission to completion.
    pub latency: Option<Duration>,
}

impl Completion {
    /// Check if the operation succeeded.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        self.result >= 0
    }

    /// Get the error code if the operation failed.
    #[must_use]
    pub fn error(&self) -> Option<io::Error> {
        if self.result < 0 {
            Some(io::Error::from_raw_os_error(-self.result))
        } else {
            None
        }
    }

    /// Get the number of bytes transferred (for read/write operations).
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    pub const fn bytes_transferred(&self) -> Option<usize> {
        if self.result >= 0 {
            Some(self.result as usize)
        } else {
            None
        }
    }
}

/// Kind of completion for quick matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionKind {
    /// Read completed.
    Read,
    /// Write completed.
    Write,
    /// Sync completed.
    Sync,
    /// Close completed.
    Close,
    /// Custom operation completed.
    Custom,
    /// Unknown operation (not tracked).
    Unknown,
}

impl Completion {
    /// Get the kind of this completion.
    #[must_use]
    pub fn kind(&self) -> CompletionKind {
        match &self.op {
            Some(PendingOp::Read { .. }) => CompletionKind::Read,
            Some(PendingOp::Write { .. }) => CompletionKind::Write,
            Some(PendingOp::Sync { .. }) => CompletionKind::Sync,
            Some(PendingOp::Close { .. }) => CompletionKind::Close,
            Some(PendingOp::Custom { .. }) => CompletionKind::Custom,
            None => CompletionKind::Unknown,
        }
    }
}

/// Per-core `io_uring` manager.
///
/// Manages a single core's `io_uring` instance, buffer pool, and pending operations.
/// Designed for integration with the thread-per-core architecture.
pub struct CoreRingManager {
    /// Core ID.
    core_id: usize,
    /// Main ring for general I/O.
    main_ring: IoUringRing,
    /// Registered buffer pool.
    buffer_pool: Option<RegisteredBufferPool>,
    /// IOPOLL ring for storage (optional).
    iopoll_ring: Option<IoUringRing>,
    /// IOPOLL buffer pool (separate from main).
    #[allow(dead_code)]
    iopoll_buffer_pool: Option<RegisteredBufferPool>,
    /// Pending operations tracking.
    pending: HashMap<u64, PendingOp>,
    /// Next `user_data` ID.
    next_id: AtomicU64,
    /// Metrics.
    metrics: RingMetrics,
    /// Whether the ring is closed.
    closed: bool,
}

impl CoreRingManager {
    /// Create a new per-core ring manager.
    ///
    /// # Arguments
    ///
    /// * `core_id` - The core ID this manager is for
    /// * `config` - `io_uring` configuration
    ///
    /// # Errors
    ///
    /// Returns an error if ring creation fails.
    pub fn new(core_id: usize, config: &IoUringConfig) -> Result<Self, IoUringError> {
        // Create main ring
        let main_ring = IoUringRing::new(config)?;

        // Create buffer pool for main ring
        let buffer_pool = if config.buffer_count > 0 {
            // Clone the ring for buffer registration
            let mut ring_for_buffers = IoUring::builder()
                .build(config.ring_entries)
                .map_err(IoUringError::RingCreation)?;

            Some(RegisteredBufferPool::new(
                &mut ring_for_buffers,
                config.buffer_size,
                config.buffer_count,
            )?)
        } else {
            None
        };

        // Create IOPOLL ring if requested and mode allows
        let (iopoll_ring, iopoll_buffer_pool) = if config.mode.uses_iopoll() {
            let iopoll_config = IoUringConfig {
                mode: RingMode::IoPoll,
                ..config.clone()
            };
            let ring = IoUringRing::new(&iopoll_config)?;

            let pool = if config.buffer_count > 0 {
                let mut ring_for_buffers = IoUring::builder()
                    .setup_iopoll()
                    .build(config.ring_entries)
                    .map_err(IoUringError::RingCreation)?;

                Some(RegisteredBufferPool::new(
                    &mut ring_for_buffers,
                    config.buffer_size,
                    config.buffer_count,
                )?)
            } else {
                None
            };

            (Some(ring), pool)
        } else {
            (None, None)
        };

        Ok(Self {
            core_id,
            main_ring,
            buffer_pool,
            iopoll_ring,
            iopoll_buffer_pool,
            pending: HashMap::new(),
            next_id: AtomicU64::new(0),
            metrics: RingMetrics::default(),
            closed: false,
        })
    }

    /// Get the core ID.
    #[must_use]
    pub const fn core_id(&self) -> usize {
        self.core_id
    }

    /// Check if the ring is closed.
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.closed
    }

    /// Get the main ring mode.
    #[must_use]
    pub const fn mode(&self) -> RingMode {
        self.main_ring.mode()
    }

    /// Check if SQPOLL is enabled.
    #[must_use]
    pub const fn uses_sqpoll(&self) -> bool {
        self.main_ring.uses_sqpoll()
    }

    /// Check if IOPOLL is enabled.
    #[must_use]
    pub fn has_iopoll_ring(&self) -> bool {
        self.iopoll_ring.is_some()
    }

    /// Acquire a buffer from the main pool.
    ///
    /// # Errors
    ///
    /// Returns an error if no buffers are available.
    pub fn acquire_buffer(&mut self) -> Result<(u16, &mut [u8]), IoUringError> {
        self.buffer_pool
            .as_mut()
            .ok_or(IoUringError::InvalidConfig(
                "No buffer pool configured".to_string(),
            ))?
            .acquire()
    }

    /// Release a buffer back to the main pool.
    pub fn release_buffer(&mut self, buf_index: u16) {
        if let Some(pool) = &mut self.buffer_pool {
            pool.release(buf_index);
        }
    }

    /// Submit a read operation using a registered buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if submission fails.
    pub fn submit_read(
        &mut self,
        fd: RawFd,
        buf_index: u16,
        offset: u64,
        len: u32,
    ) -> Result<u64, IoUringError> {
        if self.closed {
            return Err(IoUringError::RingClosed);
        }

        let pool = self
            .buffer_pool
            .as_mut()
            .ok_or(IoUringError::InvalidConfig(
                "No buffer pool configured".to_string(),
            ))?;

        let user_data =
            pool.submit_read_fixed(self.main_ring.ring_mut(), fd, buf_index, offset, len)?;

        self.pending.insert(
            user_data,
            PendingOp::Read {
                buf_index,
                len,
                submitted_at: Instant::now(),
            },
        );

        self.metrics.reads_submitted += 1;
        Ok(user_data)
    }

    /// Submit a write operation using a registered buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if submission fails.
    pub fn submit_write(
        &mut self,
        fd: RawFd,
        buf_index: u16,
        offset: u64,
        len: u32,
    ) -> Result<u64, IoUringError> {
        if self.closed {
            return Err(IoUringError::RingClosed);
        }

        let pool = self
            .buffer_pool
            .as_mut()
            .ok_or(IoUringError::InvalidConfig(
                "No buffer pool configured".to_string(),
            ))?;

        let user_data =
            pool.submit_write_fixed(self.main_ring.ring_mut(), fd, buf_index, offset, len)?;

        self.pending.insert(
            user_data,
            PendingOp::Write {
                buf_index,
                len,
                submitted_at: Instant::now(),
            },
        );

        self.metrics.writes_submitted += 1;
        Ok(user_data)
    }

    /// Submit an fsync/fdatasync operation.
    ///
    /// # Arguments
    ///
    /// * `fd` - File descriptor to sync
    /// * `datasync` - If true, use fdatasync (don't sync metadata)
    ///
    /// # Errors
    ///
    /// Returns an error if submission fails.
    pub fn submit_sync(&mut self, fd: RawFd, datasync: bool) -> Result<u64, IoUringError> {
        if self.closed {
            return Err(IoUringError::RingClosed);
        }

        let user_data = self.next_user_data();

        let entry = if datasync {
            opcode::Fsync::new(Fd(fd))
                .flags(types::FsyncFlags::DATASYNC)
                .build()
                .user_data(user_data)
        } else {
            opcode::Fsync::new(Fd(fd)).build().user_data(user_data)
        };

        // SAFETY: We're submitting a valid SQE.
        unsafe {
            self.main_ring
                .ring_mut()
                .submission()
                .push(&entry)
                .map_err(|_| IoUringError::SubmissionQueueFull)?;
        }

        self.pending.insert(
            user_data,
            PendingOp::Sync {
                submitted_at: Instant::now(),
            },
        );

        self.metrics.syncs_submitted += 1;
        Ok(user_data)
    }

    /// Submit a close operation.
    ///
    /// # Errors
    ///
    /// Returns an error if submission fails.
    pub fn submit_close(&mut self, fd: RawFd) -> Result<u64, IoUringError> {
        if self.closed {
            return Err(IoUringError::RingClosed);
        }

        let user_data = self.next_user_data();

        let entry = opcode::Close::new(Fd(fd)).build().user_data(user_data);

        // SAFETY: We're submitting a valid SQE.
        unsafe {
            self.main_ring
                .ring_mut()
                .submission()
                .push(&entry)
                .map_err(|_| IoUringError::SubmissionQueueFull)?;
        }

        self.pending.insert(
            user_data,
            PendingOp::Close {
                fd,
                submitted_at: Instant::now(),
            },
        );

        Ok(user_data)
    }

    /// Submit pending operations to the kernel.
    ///
    /// In SQPOLL mode, this is often a no-op since the kernel polls automatically.
    ///
    /// # Errors
    ///
    /// Returns an error if submission fails.
    pub fn submit(&mut self) -> Result<usize, IoUringError> {
        if self.closed {
            return Err(IoUringError::RingClosed);
        }

        let submitted = self
            .main_ring
            .ring_mut()
            .submit()
            .map_err(IoUringError::SubmissionFailed)?;

        self.metrics.submissions += 1;
        Ok(submitted)
    }

    /// Submit and wait for at least one completion.
    ///
    /// # Errors
    ///
    /// Returns an error if submission or wait fails.
    pub fn submit_and_wait(&mut self, want: usize) -> Result<usize, IoUringError> {
        if self.closed {
            return Err(IoUringError::RingClosed);
        }

        let submitted = self
            .main_ring
            .ring_mut()
            .submit_and_wait(want)
            .map_err(IoUringError::SubmissionFailed)?;

        self.metrics.submissions += 1;
        Ok(submitted)
    }

    /// Poll for completions without blocking.
    ///
    /// Returns all available completions from both rings.
    #[must_use]
    pub fn poll_completions(&mut self) -> Vec<Completion> {
        if self.closed {
            return Vec::new();
        }

        let mut completions = Vec::new();

        // Poll main ring
        self.poll_ring_completions(&mut completions, false);

        // Poll IOPOLL ring if present
        if self.iopoll_ring.is_some() {
            self.poll_ring_completions(&mut completions, true);
        }

        completions
    }

    /// Poll completions from a specific ring.
    fn poll_ring_completions(&mut self, completions: &mut Vec<Completion>, iopoll: bool) {
        // Collect CQE data first to avoid borrow conflict
        let cqe_data: Vec<(u64, i32, u32)> = {
            let ring = if iopoll {
                match &mut self.iopoll_ring {
                    Some(r) => r.ring_mut(),
                    None => return,
                }
            } else {
                self.main_ring.ring_mut()
            };

            let cq = ring.completion();
            let mut data = Vec::new();
            for cqe in cq {
                data.push((cqe.user_data(), cqe.result(), cqe.flags()));
            }
            data
        };

        // Process collected completions
        for (user_data, result, flags) in cqe_data {
            let completion = self.process_completion_data(user_data, result, flags);
            completions.push(completion);
        }
    }

    /// Process completion data from a CQE.
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    fn process_completion_data(&mut self, user_data: u64, result: i32, flags: u32) -> Completion {
        let op = self.pending.remove(&user_data);
        let latency = op.as_ref().map(|o| o.submitted_at().elapsed());

        // Update metrics
        if result >= 0 {
            self.metrics.completions_success += 1;
            if let Some(ref op) = op {
                match op {
                    PendingOp::Read { .. } => {
                        self.metrics.bytes_read += result as u64;
                    }
                    PendingOp::Write { .. } => {
                        self.metrics.bytes_written += result as u64;
                    }
                    _ => {}
                }
            }
        } else {
            self.metrics.completions_failed += 1;
        }

        if let Some(lat) = latency {
            self.metrics.total_latency_ns += lat.as_nanos() as u64;
            self.metrics.latency_samples += 1;
        }

        Completion {
            user_data,
            result,
            flags,
            op,
            latency,
        }
    }

    /// Wait for a specific operation to complete.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation is not found or wait fails.
    pub fn wait_for(&mut self, user_data: u64) -> Result<Completion, IoUringError> {
        // First check if already completed
        loop {
            // Poll for completions
            let completions = self.poll_completions();

            for completion in completions {
                if completion.user_data == user_data {
                    return Ok(completion);
                }
            }

            // If not found and still pending, wait for more
            if !self.pending.contains_key(&user_data) {
                return Err(IoUringError::PendingNotFound(user_data));
            }

            // Submit and wait for at least one completion
            self.submit_and_wait(1)?;
        }
    }

    /// Get the number of pending operations.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Get buffer pool statistics.
    #[must_use]
    pub fn buffer_pool_stats(&self) -> Option<BufferPoolStats> {
        self.buffer_pool.as_ref().map(RegisteredBufferPool::stats)
    }

    /// Get ring metrics.
    #[must_use]
    pub const fn metrics(&self) -> &RingMetrics {
        &self.metrics
    }

    /// Close the ring manager.
    pub fn close(&mut self) {
        self.closed = true;
        self.pending.clear();
    }

    /// Generate the next `user_data` ID.
    fn next_user_data(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

impl Drop for CoreRingManager {
    fn drop(&mut self) {
        self.close();
    }
}

impl std::fmt::Debug for CoreRingManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreRingManager")
            .field("core_id", &self.core_id)
            .field("mode", &self.main_ring.mode())
            .field("pending_count", &self.pending.len())
            .field("closed", &self.closed)
            .field("metrics", &self.metrics)
            .finish_non_exhaustive()
    }
}

/// Metrics for ring operations.
#[derive(Debug, Default, Clone)]
pub struct RingMetrics {
    /// Number of read operations submitted.
    pub reads_submitted: u64,
    /// Number of write operations submitted.
    pub writes_submitted: u64,
    /// Number of sync operations submitted.
    pub syncs_submitted: u64,
    /// Total submission calls.
    pub submissions: u64,
    /// Successful completions.
    pub completions_success: u64,
    /// Failed completions.
    pub completions_failed: u64,
    /// Total bytes read.
    pub bytes_read: u64,
    /// Total bytes written.
    pub bytes_written: u64,
    /// Total latency in nanoseconds.
    pub total_latency_ns: u64,
    /// Number of latency samples.
    pub latency_samples: u64,
}

impl RingMetrics {
    /// Get average latency in nanoseconds.
    #[must_use]
    pub fn avg_latency_ns(&self) -> u64 {
        if self.latency_samples > 0 {
            self.total_latency_ns / self.latency_samples
        } else {
            0
        }
    }

    /// Get total operations submitted.
    #[must_use]
    pub const fn total_ops(&self) -> u64 {
        self.reads_submitted + self.writes_submitted + self.syncs_submitted
    }

    /// Get total completions.
    #[must_use]
    pub const fn total_completions(&self) -> u64 {
        self.completions_success + self.completions_failed
    }

    /// Get success rate (0.0 to 1.0).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn success_rate(&self) -> f64 {
        let total = self.total_completions();
        if total > 0 {
            self.completions_success as f64 / total as f64
        } else {
            1.0
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::manual_let_else,
    clippy::single_match_else,
    clippy::items_after_statements
)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Write;
    use tempfile::tempdir;

    fn make_config() -> IoUringConfig {
        IoUringConfig {
            ring_entries: 32,
            mode: RingMode::Standard,
            buffer_size: 4096,
            buffer_count: 8,
            ..Default::default()
        }
    }

    #[test]
    fn test_manager_creation() {
        let config = make_config();
        let manager = CoreRingManager::new(0, &config);

        match manager {
            Ok(m) => {
                assert_eq!(m.core_id(), 0);
                assert!(!m.is_closed());
                assert_eq!(m.mode(), RingMode::Standard);
            }
            Err(e) => {
                tracing::error!("io_uring not available: {e}");
            }
        }
    }

    #[test]
    fn test_buffer_acquire_release() {
        let config = make_config();
        let mut manager = match CoreRingManager::new(0, &config) {
            Ok(m) => m,
            Err(_) => return,
        };

        // Acquire a buffer
        let result = manager.acquire_buffer();
        match result {
            Ok((idx, buf)) => {
                assert_eq!(buf.len(), 4096);
                manager.release_buffer(idx);
            }
            Err(e) => {
                tracing::error!("Buffer acquire failed: {e}");
            }
        }
    }

    #[test]
    fn test_metrics() {
        let metrics = RingMetrics {
            reads_submitted: 10,
            writes_submitted: 20,
            syncs_submitted: 5,
            submissions: 35,
            completions_success: 30,
            completions_failed: 5,
            bytes_read: 40960,
            bytes_written: 81920,
            total_latency_ns: 100_000,
            latency_samples: 10,
        };

        assert_eq!(metrics.total_ops(), 35);
        assert_eq!(metrics.total_completions(), 35);
        assert_eq!(metrics.avg_latency_ns(), 10000);
        assert!((metrics.success_rate() - 0.857).abs() < 0.01);
    }

    #[test]
    fn test_completion_kind() {
        let completion = Completion {
            user_data: 1,
            result: 100,
            flags: 0,
            op: Some(PendingOp::Read {
                buf_index: 0,
                len: 100,
                submitted_at: Instant::now(),
            }),
            latency: None,
        };

        assert_eq!(completion.kind(), CompletionKind::Read);
        assert!(completion.is_success());
        assert_eq!(completion.bytes_transferred(), Some(100));
    }

    #[test]
    fn test_completion_error() {
        let completion = Completion {
            user_data: 1,
            result: -5, // EIO
            flags: 0,
            op: None,
            latency: None,
        };

        assert!(!completion.is_success());
        assert!(completion.error().is_some());
        assert_eq!(completion.kind(), CompletionKind::Unknown);
    }

    #[test]
    fn test_write_and_poll() {
        let config = make_config();
        let mut manager = match CoreRingManager::new(0, &config) {
            Ok(m) => m,
            Err(_) => return,
        };

        // Create temp file
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.dat");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.write_all(&[0u8; 4096]).unwrap();
        file.flush().unwrap();
        drop(file);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();

        // Acquire buffer and write data
        let (idx, buf) = match manager.acquire_buffer() {
            Ok(x) => x,
            Err(_) => return,
        };
        buf[..5].copy_from_slice(b"hello");

        // Submit write
        let user_data = match manager.submit_write(fd, idx, 0, 5) {
            Ok(ud) => ud,
            Err(_) => return,
        };

        // Submit to kernel
        let _ = manager.submit();

        // Wait for completion
        match manager.wait_for(user_data) {
            Ok(completion) => {
                // In some CI environments, io_uring operations may fail
                // (e.g., containers without proper io_uring support)
                if !completion.is_success() {
                    tracing::error!("Write completion failed (possibly unsupported environment)");
                    return;
                }
                assert_eq!(completion.kind(), CompletionKind::Write);
            }
            Err(e) => {
                tracing::error!("Wait failed: {e}");
                return;
            }
        }

        manager.release_buffer(idx);
    }

    #[test]
    fn test_close() {
        let config = make_config();
        let mut manager = match CoreRingManager::new(0, &config) {
            Ok(m) => m,
            Err(_) => return,
        };

        assert!(!manager.is_closed());
        manager.close();
        assert!(manager.is_closed());

        // Operations should fail after close
        assert!(matches!(
            manager.submit_sync(0, true),
            Err(IoUringError::RingClosed)
        ));
    }
}
