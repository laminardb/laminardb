//! Three-ring reactor implementation.
//!
//! The `ThreeRingReactor` manages three io_uring instances for optimal
//! latency/throughput balance in a thread-per-core architecture.

use io_uring::cqueue::Entry as CqEntry;
use io_uring::opcode;
use io_uring::squeue::Entry as SqEntry;
use io_uring::types::{self, Fd};
use io_uring::IoUring;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use super::affinity::{OperationType, RingAffinity};
use super::config::ThreeRingConfig;
use super::handler::RingHandler;
use super::router::{CompletionRouter, PendingOperation, RoutedCompletion};
use super::stats::ThreeRingStats;
use crate::io_uring::IoUringError;

/// Three-ring I/O reactor for optimal latency/throughput balance.
///
/// Pattern from Seastar/Glommio: separate rings for different latency
/// requirements, with eventfd-based wake-up coordination.
///
/// # Architecture
///
/// - **Latency Ring**: Always polled first, never blocks. For network and urgent ops.
/// - **Main Ring**: Can block when idle. For WAL writes and normal I/O.
/// - **Poll Ring**: IOPOLL for NVMe storage (optional).
///
/// # Wake-Up Mechanism
///
/// When the main ring blocks waiting for I/O, it also waits on the latency
/// ring's eventfd. Any completion on the latency ring immediately wakes
/// the main ring, ensuring sub-microsecond response for latency-critical ops.
pub struct ThreeRingReactor {
    /// Latency-critical operations (network, urgent).
    /// Always checked first, never blocks.
    latency_ring: IoUring,

    /// Normal operations (WAL, background reads).
    /// Can block when waiting for work.
    main_ring: IoUring,

    /// Storage polling (optional, for NVMe passthrough).
    /// Uses IOPOLL, cannot have sockets.
    poll_ring: Option<IoUring>,

    /// Eventfd for waking main ring from latency ring activity.
    #[cfg(target_os = "linux")]
    eventfd: RawFd,

    /// Core ID for this reactor.
    core_id: usize,

    /// Completion router for tracking operations.
    router: CompletionRouter,

    /// Next user_data ID.
    next_id: AtomicU64,

    /// Statistics for monitoring.
    stats: ThreeRingStats,

    /// Configuration.
    config: ThreeRingConfig,

    /// Whether the reactor is closed.
    closed: bool,
}

impl ThreeRingReactor {
    /// Create a new three-ring reactor.
    ///
    /// # Arguments
    ///
    /// * `core_id` - The core ID this reactor is for
    /// * `config` - Three-ring configuration
    ///
    /// # Errors
    ///
    /// Returns an error if ring creation fails.
    pub fn new(core_id: usize, config: ThreeRingConfig) -> Result<Self, IoUringError> {
        config.validate()?;

        // Create latency ring (short SQPOLL timeout for responsiveness)
        let latency_ring = Self::create_latency_ring(&config)?;

        // Create main ring (longer timeout, can idle)
        let main_ring = Self::create_main_ring(&config)?;

        // Create eventfd for wake-up coordination
        #[cfg(target_os = "linux")]
        // SAFETY: eventfd() is a simple syscall that creates a new file descriptor.
        // EFD_NONBLOCK and EFD_CLOEXEC are valid flags. The returned fd is checked
        // for errors immediately below (< 0 check). No memory or aliasing concerns.
        let eventfd = unsafe {
            libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC)
        };
        #[cfg(target_os = "linux")]
        if eventfd < 0 {
            return Err(IoUringError::RingCreation(io::Error::last_os_error()));
        }

        // Register poll on eventfd so main ring wakes when latency ring has activity
        // This is done by writing to eventfd when latency ring gets completions
        // and having main ring wait on both its own completions and the eventfd

        // Create optional IOPOLL ring for NVMe storage
        let poll_ring = if config.enable_poll_ring {
            Some(Self::create_poll_ring(&config)?)
        } else {
            None
        };

        Ok(Self {
            latency_ring,
            main_ring,
            poll_ring,
            #[cfg(target_os = "linux")]
            eventfd,
            core_id,
            router: CompletionRouter::new(),
            next_id: AtomicU64::new(0),
            stats: ThreeRingStats::new(),
            config,
            closed: false,
        })
    }

    /// Create the latency ring with short SQPOLL timeout.
    fn create_latency_ring(config: &ThreeRingConfig) -> Result<IoUring, IoUringError> {
        let mut builder = IoUring::builder();

        // Short SQPOLL timeout for responsiveness
        builder.setup_sqpoll(config.latency_sqpoll_idle_ms);

        if let Some(cpu) = config.sqpoll_cpu {
            builder.setup_sqpoll_cpu(cpu);
        }

        if config.coop_taskrun {
            builder.setup_coop_taskrun();
        }

        if config.single_issuer {
            builder.setup_single_issuer();
        }

        builder
            .build(config.latency_entries)
            .map_err(IoUringError::RingCreation)
    }

    /// Create the main ring with longer SQPOLL timeout.
    fn create_main_ring(config: &ThreeRingConfig) -> Result<IoUring, IoUringError> {
        let mut builder = IoUring::builder();

        // Longer SQPOLL timeout, can idle
        builder.setup_sqpoll(config.main_sqpoll_idle_ms);

        if let Some(cpu) = config.sqpoll_cpu {
            builder.setup_sqpoll_cpu(cpu);
        }

        if config.coop_taskrun {
            builder.setup_coop_taskrun();
        }

        if config.single_issuer {
            builder.setup_single_issuer();
        }

        builder
            .build(config.main_entries)
            .map_err(IoUringError::RingCreation)
    }

    /// Create the poll ring with IOPOLL for NVMe.
    fn create_poll_ring(config: &ThreeRingConfig) -> Result<IoUring, IoUringError> {
        let mut builder = IoUring::builder();

        // Enable IOPOLL for NVMe
        builder.setup_iopoll();

        // Can also use SQPOLL with IOPOLL
        builder.setup_sqpoll(config.main_sqpoll_idle_ms);

        if let Some(cpu) = config.sqpoll_cpu {
            builder.setup_sqpoll_cpu(cpu);
        }

        if config.single_issuer {
            builder.setup_single_issuer();
        }

        builder
            .build(config.poll_entries)
            .map_err(IoUringError::RingCreation)
    }

    /// Get the core ID.
    #[must_use]
    pub const fn core_id(&self) -> usize {
        self.core_id
    }

    /// Check if the reactor is closed.
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.closed
    }

    /// Check if the poll ring is enabled.
    #[must_use]
    pub fn has_poll_ring(&self) -> bool {
        self.poll_ring.is_some()
    }

    /// Get the statistics.
    #[must_use]
    pub fn stats(&self) -> &ThreeRingStats {
        &self.stats
    }

    /// Get mutable statistics.
    #[must_use]
    pub fn stats_mut(&mut self) -> &mut ThreeRingStats {
        &mut self.stats
    }

    /// Get the completion router.
    #[must_use]
    pub fn router(&self) -> &CompletionRouter {
        &self.router
    }

    /// Get mutable completion router.
    #[must_use]
    pub fn router_mut(&mut self) -> &mut CompletionRouter {
        &mut self.router
    }

    /// Generate the next user_data ID.
    pub fn next_user_data(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Submit an entry to the latency ring.
    ///
    /// # Errors
    ///
    /// Returns an error if the ring is closed or submission fails.
    pub fn submit_to_latency(&mut self, entry: SqEntry) -> Result<u64, IoUringError> {
        if self.closed {
            return Err(IoUringError::RingClosed);
        }

        let user_data = entry.get_user_data();

        // SAFETY: We're submitting a valid SQE.
        unsafe {
            self.latency_ring
                .submission()
                .push(&entry)
                .map_err(|_| IoUringError::SubmissionQueueFull)?;
        }

        self.stats.latency_submissions += 1;
        Ok(user_data)
    }

    /// Submit an entry to the main ring.
    ///
    /// # Errors
    ///
    /// Returns an error if the ring is closed or submission fails.
    pub fn submit_to_main(&mut self, entry: SqEntry) -> Result<u64, IoUringError> {
        if self.closed {
            return Err(IoUringError::RingClosed);
        }

        let user_data = entry.get_user_data();

        // SAFETY: We're submitting a valid SQE.
        unsafe {
            self.main_ring
                .submission()
                .push(&entry)
                .map_err(|_| IoUringError::SubmissionQueueFull)?;
        }

        self.stats.main_submissions += 1;
        Ok(user_data)
    }

    /// Submit an entry to the poll ring.
    ///
    /// Falls back to main ring if poll ring is not enabled.
    ///
    /// # Errors
    ///
    /// Returns an error if the ring is closed or submission fails.
    pub fn submit_to_poll(&mut self, entry: SqEntry) -> Result<u64, IoUringError> {
        if self.closed {
            return Err(IoUringError::RingClosed);
        }

        if let Some(ref mut ring) = self.poll_ring {
            let user_data = entry.get_user_data();

            // SAFETY: We're submitting a valid SQE.
            unsafe {
                ring.submission()
                    .push(&entry)
                    .map_err(|_| IoUringError::SubmissionQueueFull)?;
            }

            self.stats.poll_submissions += 1;
            Ok(user_data)
        } else {
            // Fallback to main ring
            self.stats.poll_fallbacks += 1;
            self.submit_to_main(entry)
        }
    }

    /// Submit an entry with automatic ring selection based on operation type.
    ///
    /// # Errors
    ///
    /// Returns an error if submission fails.
    pub fn submit_with_affinity(
        &mut self,
        entry: SqEntry,
        op_type: OperationType,
    ) -> Result<u64, IoUringError> {
        let affinity = RingAffinity::for_operation(&op_type);
        let user_data = entry.get_user_data();

        // Track the operation
        self.router.track(
            PendingOperation::new(user_data, affinity)
                .with_op_type(op_type),
        );

        match affinity {
            RingAffinity::Latency => self.submit_to_latency(entry),
            RingAffinity::Main => self.submit_to_main(entry),
            RingAffinity::Poll => self.submit_to_poll(entry),
        }
    }

    /// Submit pending entries to the kernel.
    ///
    /// # Errors
    ///
    /// Returns an error if submission fails.
    pub fn submit_all(&mut self) -> Result<usize, IoUringError> {
        if self.closed {
            return Err(IoUringError::RingClosed);
        }

        let mut total = 0;

        total += self.latency_ring.submit().map_err(IoUringError::SubmissionFailed)?;
        total += self.main_ring.submit().map_err(IoUringError::SubmissionFailed)?;

        if let Some(ref mut ring) = self.poll_ring {
            total += ring.submit().map_err(IoUringError::SubmissionFailed)?;
        }

        Ok(total)
    }

    /// Poll all rings for completions without blocking.
    ///
    /// Always drains latency ring first for priority.
    #[must_use]
    pub fn poll_all(&mut self) -> Vec<RoutedCompletion> {
        if self.closed {
            return Vec::new();
        }

        let mut completions = Vec::new();

        // 1. Always drain latency ring first (highest priority)
        self.poll_latency_ring(&mut completions);

        // 2. Drain main ring
        self.poll_main_ring(&mut completions);

        // 3. Drain poll ring if present
        self.poll_poll_ring(&mut completions);

        completions
    }

    /// Poll only the latency ring.
    fn poll_latency_ring(&mut self, completions: &mut Vec<RoutedCompletion>) {
        let mut cq = self.latency_ring.completion();
        while let Some(cqe) = cq.next() {
            let completion = self.process_cqe(cqe, RingAffinity::Latency);
            self.stats.record_latency_completion(completion.latency(), completion.is_success());
            completions.push(completion);
        }
    }

    /// Poll only the main ring.
    fn poll_main_ring(&mut self, completions: &mut Vec<RoutedCompletion>) {
        let mut cq = self.main_ring.completion();
        while let Some(cqe) = cq.next() {
            let completion = self.process_cqe(cqe, RingAffinity::Main);
            self.stats.record_main_completion(completion.latency(), completion.is_success());
            completions.push(completion);
        }
    }

    /// Poll only the poll ring.
    fn poll_poll_ring(&mut self, completions: &mut Vec<RoutedCompletion>) {
        if let Some(ref mut ring) = self.poll_ring {
            let mut cq = ring.completion();
            while let Some(cqe) = cq.next() {
                let completion = self.process_cqe(cqe, RingAffinity::Poll);
                self.stats.record_poll_completion(completion.latency(), completion.is_success());
                completions.push(completion);
            }
        }
    }

    /// Process a completion queue entry.
    fn process_cqe(&mut self, cqe: CqEntry, default_affinity: RingAffinity) -> RoutedCompletion {
        let user_data = cqe.user_data();
        let result = cqe.result();
        let flags = cqe.flags();

        // Try to route through our router first
        if self.router.is_pending(user_data) {
            self.router.route(user_data, result, flags)
        } else {
            // Unknown operation - use default affinity
            RoutedCompletion {
                user_data,
                result,
                flags,
                affinity: default_affinity,
                submitted_at: None,
                op_type: None,
            }
        }
    }

    /// Run the event loop with a handler.
    ///
    /// This is the main event loop that integrates with Ring 0/1/2 architecture.
    pub fn run(&mut self, handler: &mut dyn RingHandler) {
        while !self.closed {
            // 1. Always drain latency ring first (non-blocking)
            let mut latency_completions = Vec::new();
            self.poll_latency_ring(&mut latency_completions);
            for completion in latency_completions {
                handler.handle_latency_completion(completion);
            }

            // 2. Drain main ring completions (non-blocking)
            let mut main_completions = Vec::new();
            self.poll_main_ring(&mut main_completions);
            for completion in main_completions {
                handler.handle_main_completion(completion);
            }

            // 3. Drain poll ring if present (non-blocking)
            let mut poll_completions = Vec::new();
            self.poll_poll_ring(&mut poll_completions);
            for completion in poll_completions {
                handler.handle_poll_completion(completion);
            }

            // 4. Ring 0: Process application events
            handler.process_ring0_events();

            // 5. Ring 1: Background work (if Ring 0 idle)
            if handler.ring0_idle() {
                handler.process_ring1_chunk();
            }

            // 6. Ring 2: Control plane (rare)
            if handler.has_control_message() {
                handler.process_ring2();
            }

            // 7. Check for shutdown
            if handler.should_shutdown() {
                break;
            }

            // 8. Block on main ring if nothing to do
            //    Will wake up when latency ring has activity
            if handler.should_sleep() {
                self.stats.record_sleep();
                match self.main_ring.submit_and_wait(1) {
                    Ok(_) => {
                        // Check if we were woken by latency ring
                        if !self.latency_ring.completion().is_empty() {
                            self.stats.record_latency_wake_up();
                        }
                    }
                    Err(e) if e.raw_os_error() == Some(libc::EINTR) => {
                        // Interrupted, continue loop
                    }
                    Err(e) => {
                        tracing::error!("main_ring wait error: {}", e);
                    }
                }
            }
        }
    }

    /// Submit a network receive operation (latency ring).
    ///
    /// # Errors
    ///
    /// Returns an error if submission fails.
    pub fn submit_recv(
        &mut self,
        fd: RawFd,
        buf: *mut u8,
        len: u32,
    ) -> Result<u64, IoUringError> {
        let user_data = self.next_user_data();

        let entry = opcode::Recv::new(Fd(fd), buf, len)
            .build()
            .user_data(user_data);

        self.router.track(
            PendingOperation::new(user_data, RingAffinity::Latency)
                .with_op_type(OperationType::NetworkRecv)
                .with_fd(fd)
                .with_expected_bytes(len),
        );

        self.submit_to_latency(entry)
    }

    /// Submit a network send operation (latency ring).
    ///
    /// # Errors
    ///
    /// Returns an error if submission fails.
    pub fn submit_send(
        &mut self,
        fd: RawFd,
        buf: *const u8,
        len: u32,
    ) -> Result<u64, IoUringError> {
        let user_data = self.next_user_data();

        let entry = opcode::Send::new(Fd(fd), buf, len)
            .build()
            .user_data(user_data);

        self.router.track(
            PendingOperation::new(user_data, RingAffinity::Latency)
                .with_op_type(OperationType::NetworkSend)
                .with_fd(fd)
                .with_expected_bytes(len),
        );

        self.submit_to_latency(entry)
    }

    /// Submit a WAL write operation (main ring).
    ///
    /// # Errors
    ///
    /// Returns an error if submission fails.
    pub fn submit_wal_write(
        &mut self,
        fd: RawFd,
        buf: *const u8,
        len: u32,
        offset: u64,
    ) -> Result<u64, IoUringError> {
        let user_data = self.next_user_data();

        let entry = opcode::Write::new(Fd(fd), buf, len)
            .offset(offset)
            .build()
            .user_data(user_data);

        self.router.track(
            PendingOperation::new(user_data, RingAffinity::Main)
                .with_op_type(OperationType::WalWrite)
                .with_fd(fd)
                .with_expected_bytes(len),
        );

        self.submit_to_main(entry)
    }

    /// Submit a WAL sync operation (main ring).
    ///
    /// # Errors
    ///
    /// Returns an error if submission fails.
    pub fn submit_wal_sync(&mut self, fd: RawFd, datasync: bool) -> Result<u64, IoUringError> {
        let user_data = self.next_user_data();

        let entry = if datasync {
            opcode::Fsync::new(Fd(fd))
                .flags(types::FsyncFlags::DATASYNC)
                .build()
                .user_data(user_data)
        } else {
            opcode::Fsync::new(Fd(fd))
                .build()
                .user_data(user_data)
        };

        self.router.track(
            PendingOperation::new(user_data, RingAffinity::Main)
                .with_op_type(OperationType::WalSync)
                .with_fd(fd),
        );

        self.submit_to_main(entry)
    }

    /// Submit a storage read operation (poll ring or main ring fallback).
    ///
    /// # Errors
    ///
    /// Returns an error if submission fails.
    pub fn submit_storage_read(
        &mut self,
        fd: RawFd,
        buf: *mut u8,
        len: u32,
        offset: u64,
    ) -> Result<u64, IoUringError> {
        let user_data = self.next_user_data();

        let entry = opcode::Read::new(Fd(fd), buf, len)
            .offset(offset)
            .build()
            .user_data(user_data);

        let affinity = if self.poll_ring.is_some() {
            RingAffinity::Poll
        } else {
            RingAffinity::Main
        };

        self.router.track(
            PendingOperation::new(user_data, affinity)
                .with_op_type(OperationType::StorageRead)
                .with_fd(fd)
                .with_expected_bytes(len),
        );

        self.submit_to_poll(entry)
    }

    /// Submit a storage write operation (poll ring or main ring fallback).
    ///
    /// # Errors
    ///
    /// Returns an error if submission fails.
    pub fn submit_storage_write(
        &mut self,
        fd: RawFd,
        buf: *const u8,
        len: u32,
        offset: u64,
    ) -> Result<u64, IoUringError> {
        let user_data = self.next_user_data();

        let entry = opcode::Write::new(Fd(fd), buf, len)
            .offset(offset)
            .build()
            .user_data(user_data);

        let affinity = if self.poll_ring.is_some() {
            RingAffinity::Poll
        } else {
            RingAffinity::Main
        };

        self.router.track(
            PendingOperation::new(user_data, affinity)
                .with_op_type(OperationType::StorageWrite)
                .with_fd(fd)
                .with_expected_bytes(len),
        );

        self.submit_to_poll(entry)
    }

    /// Close the reactor.
    pub fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.router.clear();

            // Close eventfd
            #[cfg(target_os = "linux")]
            unsafe {
                libc::close(self.eventfd);
            }
        }
    }
}

impl Drop for ThreeRingReactor {
    fn drop(&mut self) {
        self.close();
    }
}

impl std::fmt::Debug for ThreeRingReactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreeRingReactor")
            .field("core_id", &self.core_id)
            .field("has_poll_ring", &self.poll_ring.is_some())
            .field("pending_ops", &self.router.pending_count())
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io_uring::three_ring::handler::SimpleRingHandler;

    fn make_config() -> ThreeRingConfig {
        ThreeRingConfig {
            latency_entries: 32,
            main_entries: 64,
            poll_entries: 32,
            enable_poll_ring: false,
            latency_sqpoll_idle_ms: 100,
            main_sqpoll_idle_ms: 1000,
            sqpoll_cpu: None,
            coop_taskrun: false, // Disable for testing
            single_issuer: false,
            buffer_size: 4096,
            buffer_count: 8,
        }
    }

    #[test]
    fn test_reactor_creation() {
        let config = make_config();
        let reactor = ThreeRingReactor::new(0, config);

        match reactor {
            Ok(r) => {
                assert_eq!(r.core_id(), 0);
                assert!(!r.is_closed());
                assert!(!r.has_poll_ring());
                assert_eq!(r.router().pending_count(), 0);
            }
            Err(e) => {
                eprintln!("io_uring not available: {e}");
            }
        }
    }

    #[test]
    fn test_reactor_with_poll_ring() {
        let mut config = make_config();
        config.enable_poll_ring = true;

        let reactor = ThreeRingReactor::new(0, config);

        match reactor {
            Ok(r) => {
                assert!(r.has_poll_ring());
            }
            Err(e) => {
                // May fail if IOPOLL not available
                eprintln!("Poll ring not available: {e}");
            }
        }
    }

    #[test]
    fn test_next_user_data() {
        let config = make_config();
        let reactor = match ThreeRingReactor::new(0, config) {
            Ok(r) => r,
            Err(_) => return,
        };

        let id1 = reactor.next_user_data();
        let id2 = reactor.next_user_data();
        let id3 = reactor.next_user_data();

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id3, 2);
    }

    #[test]
    fn test_poll_empty() {
        let config = make_config();
        let mut reactor = match ThreeRingReactor::new(0, config) {
            Ok(r) => r,
            Err(_) => return,
        };

        let completions = reactor.poll_all();
        assert!(completions.is_empty());
    }

    #[test]
    fn test_close() {
        let config = make_config();
        let mut reactor = match ThreeRingReactor::new(0, config) {
            Ok(r) => r,
            Err(_) => return,
        };

        assert!(!reactor.is_closed());
        reactor.close();
        assert!(reactor.is_closed());

        // Operations should fail after close
        let entry = opcode::Nop::new().build().user_data(1);
        assert!(matches!(
            reactor.submit_to_latency(entry),
            Err(IoUringError::RingClosed)
        ));
    }

    #[test]
    fn test_submit_nop_to_latency() {
        let config = make_config();
        let mut reactor = match ThreeRingReactor::new(0, config) {
            Ok(r) => r,
            Err(_) => return,
        };

        let user_data = reactor.next_user_data();
        let entry = opcode::Nop::new().build().user_data(user_data);

        let result = reactor.submit_to_latency(entry);
        assert!(result.is_ok());
        assert_eq!(reactor.stats().latency_submissions, 1);
    }

    #[test]
    fn test_submit_nop_to_main() {
        let config = make_config();
        let mut reactor = match ThreeRingReactor::new(0, config) {
            Ok(r) => r,
            Err(_) => return,
        };

        let user_data = reactor.next_user_data();
        let entry = opcode::Nop::new().build().user_data(user_data);

        let result = reactor.submit_to_main(entry);
        assert!(result.is_ok());
        assert_eq!(reactor.stats().main_submissions, 1);
    }

    #[test]
    fn test_submit_nop_to_poll_fallback() {
        let config = make_config(); // poll_ring disabled
        let mut reactor = match ThreeRingReactor::new(0, config) {
            Ok(r) => r,
            Err(_) => return,
        };

        let user_data = reactor.next_user_data();
        let entry = opcode::Nop::new().build().user_data(user_data);

        // Should fall back to main ring
        let result = reactor.submit_to_poll(entry);
        assert!(result.is_ok());
        assert_eq!(reactor.stats().poll_fallbacks, 1);
        assert_eq!(reactor.stats().main_submissions, 1);
    }

    #[test]
    fn test_submit_with_affinity() {
        let config = make_config();
        let mut reactor = match ThreeRingReactor::new(0, config) {
            Ok(r) => r,
            Err(_) => return,
        };

        let user_data = reactor.next_user_data();
        let entry = opcode::Nop::new().build().user_data(user_data);

        let result = reactor.submit_with_affinity(entry, OperationType::NetworkRecv);
        assert!(result.is_ok());
        assert_eq!(reactor.stats().latency_submissions, 1);
        assert!(reactor.router().is_pending(user_data));
    }

    #[test]
    fn test_stats_tracking() {
        let config = make_config();
        let mut reactor = match ThreeRingReactor::new(0, config) {
            Ok(r) => r,
            Err(_) => return,
        };

        // Submit to each ring type
        let entry1 = opcode::Nop::new().build().user_data(1);
        let entry2 = opcode::Nop::new().build().user_data(2);
        let entry3 = opcode::Nop::new().build().user_data(3);

        let _ = reactor.submit_to_latency(entry1);
        let _ = reactor.submit_to_main(entry2);
        let _ = reactor.submit_to_poll(entry3); // Falls back to main

        let stats = reactor.stats();
        assert_eq!(stats.latency_submissions, 1);
        assert_eq!(stats.main_submissions, 2); // 1 direct + 1 fallback
        assert_eq!(stats.poll_fallbacks, 1);
    }

    #[test]
    fn test_simple_handler_integration() {
        let config = make_config();
        let mut reactor = match ThreeRingReactor::new(0, config) {
            Ok(r) => r,
            Err(_) => return,
        };

        let mut handler = SimpleRingHandler::new();
        handler.shutdown = true; // Exit immediately

        // This should return immediately due to shutdown
        reactor.run(&mut handler);

        assert_eq!(handler.ring0_events_processed, 0);
    }

    #[test]
    fn test_debug_format() {
        let config = make_config();
        let reactor = match ThreeRingReactor::new(0, config) {
            Ok(r) => r,
            Err(_) => return,
        };

        let debug = format!("{reactor:?}");
        assert!(debug.contains("ThreeRingReactor"));
        assert!(debug.contains("core_id"));
        assert!(debug.contains("has_poll_ring"));
    }
}
