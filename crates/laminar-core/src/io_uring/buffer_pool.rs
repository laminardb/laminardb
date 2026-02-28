//! Pre-registered I/O buffer pool for zero-copy operations.
//!
//! Registers buffers once at startup to avoid per-operation buffer mapping overhead.
//! Uses fixed-size buffers for predictable performance.

use io_uring::types::Fd;
use io_uring::{opcode, IoUring};
use std::collections::VecDeque;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};

use super::error::IoUringError;

/// A pre-registered buffer pool for `io_uring` operations.
///
/// Buffers are registered with the kernel once at creation time, eliminating
/// the per-operation buffer mapping overhead that would otherwise occur.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::io_uring::RegisteredBufferPool;
///
/// let mut pool = RegisteredBufferPool::new(&mut ring, 64 * 1024, 256)?;
///
/// // Acquire a buffer
/// let (idx, buf) = pool.acquire()?;
/// buf[..5].copy_from_slice(b"hello");
///
/// // Use the buffer for I/O
/// pool.submit_write_fixed(fd, idx, 0, 5)?;
///
/// // After completion, release the buffer
/// pool.release(idx);
/// ```
pub struct RegisteredBufferPool {
    /// Pre-allocated buffers.
    buffers: Vec<Vec<u8>>,
    /// Size of each buffer.
    buffer_size: usize,
    /// Free buffer indices.
    free_list: VecDeque<u16>,
    /// Next `user_data` ID for tracking operations.
    next_id: AtomicU64,
    /// Total buffers in pool.
    total_count: usize,
    /// Buffers currently acquired.
    acquired_count: usize,
    /// Cumulative number of successful buffer acquisitions.
    acquisitions: u64,
    /// Cumulative number of times acquisition failed (pool exhausted).
    exhaustions: u64,
}

impl RegisteredBufferPool {
    /// Create a new buffer pool and register it with the kernel.
    ///
    /// # Arguments
    ///
    /// * `ring` - The `io_uring` instance to register buffers with
    /// * `buffer_size` - Size of each buffer in bytes
    /// * `buffer_count` - Number of buffers to allocate
    ///
    /// # Errors
    ///
    /// Returns an error if buffer registration fails.
    pub fn new(
        ring: &mut IoUring,
        buffer_size: usize,
        buffer_count: usize,
    ) -> Result<Self, IoUringError> {
        if buffer_count > u16::MAX as usize {
            return Err(IoUringError::InvalidConfig(format!(
                "buffer_count {} exceeds maximum {}",
                buffer_count,
                u16::MAX
            )));
        }

        // Allocate aligned buffers
        let buffers: Vec<Vec<u8>> = (0..buffer_count)
            .map(|_| {
                // Allocate with page alignment for O_DIRECT compatibility
                vec![0; buffer_size]
            })
            .collect();

        // Create iovec slice for registration
        let iovecs: Vec<libc::iovec> = buffers
            .iter()
            .map(|buf| libc::iovec {
                iov_base: buf.as_ptr() as *mut _,
                iov_len: buf.len(),
            })
            .collect();

        // Register buffers with the kernel
        // SAFETY: The iovecs point to valid, owned memory that will outlive the registration.
        // The buffers Vec owns the memory and is stored in the struct.
        unsafe {
            ring.submitter()
                .register_buffers(&iovecs)
                .map_err(IoUringError::BufferRegistration)?;
        }

        #[allow(clippy::cast_possible_truncation)]
        let free_list = (0..buffer_count as u16).collect();

        Ok(Self {
            buffers,
            buffer_size,
            free_list,
            next_id: AtomicU64::new(0),
            total_count: buffer_count,
            acquired_count: 0,
            acquisitions: 0,
            exhaustions: 0,
        })
    }

    /// Acquire a buffer from the pool.
    ///
    /// Returns the buffer index and a mutable reference to the buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if no buffers are available.
    pub fn acquire(&mut self) -> Result<(u16, &mut [u8]), IoUringError> {
        let Some(idx) = self.free_list.pop_front() else {
            self.exhaustions += 1;
            return Err(IoUringError::BufferPoolExhausted);
        };

        self.acquired_count += 1;
        self.acquisitions += 1;

        Ok((idx, &mut self.buffers[idx as usize]))
    }

    /// Try to acquire a buffer without blocking.
    ///
    /// Returns `None` if no buffers are available.
    #[must_use]
    pub fn try_acquire(&mut self) -> Option<(u16, &mut [u8])> {
        let Some(idx) = self.free_list.pop_front() else {
            self.exhaustions += 1;
            return None;
        };
        self.acquired_count += 1;
        self.acquisitions += 1;
        Some((idx, &mut self.buffers[idx as usize]))
    }

    /// Release a buffer back to the pool.
    ///
    /// # Panics
    ///
    /// Panics if the buffer index is invalid.
    pub fn release(&mut self, buf_index: u16) {
        debug_assert!(
            (buf_index as usize) < self.total_count,
            "Invalid buffer index"
        );
        self.free_list.push_back(buf_index);
        self.acquired_count = self.acquired_count.saturating_sub(1);
    }

    /// Get a reference to a buffer by index.
    ///
    /// # Errors
    ///
    /// Returns an error if the index is invalid.
    pub fn get(&self, buf_index: u16) -> Result<&[u8], IoUringError> {
        self.buffers
            .get(buf_index as usize)
            .map(Vec::as_slice)
            .ok_or(IoUringError::InvalidBufferIndex(buf_index))
    }

    /// Get a mutable reference to a buffer by index.
    ///
    /// # Errors
    ///
    /// Returns an error if the index is invalid.
    pub fn get_mut(&mut self, buf_index: u16) -> Result<&mut [u8], IoUringError> {
        self.buffers
            .get_mut(buf_index as usize)
            .map(Vec::as_mut_slice)
            .ok_or(IoUringError::InvalidBufferIndex(buf_index))
    }

    /// Submit a read operation using a registered buffer.
    ///
    /// The data will be read into the buffer at the given index.
    ///
    /// # Arguments
    ///
    /// * `ring` - The `io_uring` instance
    /// * `fd` - File descriptor to read from
    /// * `buf_index` - Index of the registered buffer
    /// * `offset` - File offset to read from
    /// * `len` - Number of bytes to read
    ///
    /// # Returns
    ///
    /// The `user_data` value that will identify this operation in completions.
    ///
    /// # Errors
    ///
    /// Returns an error if the submission queue is full.
    pub fn submit_read_fixed(
        &mut self,
        ring: &mut IoUring,
        fd: RawFd,
        buf_index: u16,
        offset: u64,
        len: u32,
    ) -> Result<u64, IoUringError> {
        // Get user_data first to avoid borrow conflict
        let user_data = self.next_user_data();

        let buf = self
            .buffers
            .get_mut(buf_index as usize)
            .ok_or(IoUringError::InvalidBufferIndex(buf_index))?;

        let entry = opcode::ReadFixed::new(Fd(fd), buf.as_mut_ptr(), len, buf_index)
            .offset(offset)
            .build()
            .user_data(user_data);

        // SAFETY: We're submitting a valid SQE with properly registered buffer.
        unsafe {
            ring.submission()
                .push(&entry)
                .map_err(|_| IoUringError::SubmissionQueueFull)?;
        }

        Ok(user_data)
    }

    /// Submit a write operation using a registered buffer.
    ///
    /// The data in the buffer at the given index will be written.
    ///
    /// # Arguments
    ///
    /// * `ring` - The `io_uring` instance
    /// * `fd` - File descriptor to write to
    /// * `buf_index` - Index of the registered buffer
    /// * `offset` - File offset to write to
    /// * `len` - Number of bytes to write
    ///
    /// # Returns
    ///
    /// The `user_data` value that will identify this operation in completions.
    ///
    /// # Errors
    ///
    /// Returns an error if the submission queue is full.
    pub fn submit_write_fixed(
        &mut self,
        ring: &mut IoUring,
        fd: RawFd,
        buf_index: u16,
        offset: u64,
        len: u32,
    ) -> Result<u64, IoUringError> {
        let buf = self
            .buffers
            .get(buf_index as usize)
            .ok_or(IoUringError::InvalidBufferIndex(buf_index))?;

        let user_data = self.next_user_data();

        let entry = opcode::WriteFixed::new(Fd(fd), buf.as_ptr(), len, buf_index)
            .offset(offset)
            .build()
            .user_data(user_data);

        // SAFETY: We're submitting a valid SQE with properly registered buffer.
        unsafe {
            ring.submission()
                .push(&entry)
                .map_err(|_| IoUringError::SubmissionQueueFull)?;
        }

        Ok(user_data)
    }

    /// Get the size of each buffer.
    #[must_use]
    pub const fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Get the total number of buffers.
    #[must_use]
    pub const fn total_count(&self) -> usize {
        self.total_count
    }

    /// Get the number of available buffers.
    #[must_use]
    pub fn available_count(&self) -> usize {
        self.free_list.len()
    }

    /// Get the number of acquired buffers.
    #[must_use]
    pub const fn acquired_count(&self) -> usize {
        self.acquired_count
    }

    /// Check if the pool is exhausted.
    #[must_use]
    pub fn is_exhausted(&self) -> bool {
        self.free_list.is_empty()
    }

    /// Generate the next `user_data` ID.
    fn next_user_data(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Get statistics about the buffer pool.
    #[must_use]
    pub fn stats(&self) -> BufferPoolStats {
        BufferPoolStats {
            total_count: self.total_count,
            available_count: self.free_list.len(),
            acquired_count: self.acquired_count,
            buffer_size: self.buffer_size,
            total_bytes: self.total_count * self.buffer_size,
            acquisitions: self.acquisitions,
            exhaustions: self.exhaustions,
        }
    }
}

impl Drop for RegisteredBufferPool {
    fn drop(&mut self) {
        // Buffers will be unregistered when the ring is closed
        // The Vec<Vec<u8>> will be dropped automatically
    }
}

/// Statistics about the buffer pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferPoolStats {
    /// Total number of buffers.
    pub total_count: usize,
    /// Number of available buffers.
    pub available_count: usize,
    /// Number of acquired buffers.
    pub acquired_count: usize,
    /// Size of each buffer in bytes.
    pub buffer_size: usize,
    /// Total bytes allocated.
    pub total_bytes: usize,
    /// Cumulative successful acquisitions.
    pub acquisitions: u64,
    /// Cumulative pool exhaustion events (acquire failed).
    pub exhaustions: u64,
}

impl std::fmt::Display for BufferPoolStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BufferPool({} available/{} total, {}KB each, {}MB total)",
            self.available_count,
            self.total_count,
            self.buffer_size / 1024,
            self.total_bytes / (1024 * 1024)
        )
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

    fn create_test_ring() -> Option<IoUring> {
        IoUring::builder().build(32).ok()
    }

    #[test]
    fn test_buffer_pool_creation() {
        let mut ring = match create_test_ring() {
            Some(r) => r,
            None => {
                tracing::warn!("io_uring not available, skipping test");
                return;
            }
        };

        let pool = RegisteredBufferPool::new(&mut ring, 4096, 16);
        match pool {
            Ok(p) => {
                assert_eq!(p.total_count(), 16);
                assert_eq!(p.buffer_size(), 4096);
                assert_eq!(p.available_count(), 16);
                assert_eq!(p.acquired_count(), 0);
            }
            Err(e) => {
                tracing::warn!("Buffer registration failed: {e}");
            }
        }
    }

    #[test]
    fn test_acquire_release() {
        let mut ring = match create_test_ring() {
            Some(r) => r,
            None => return,
        };

        let mut pool = match RegisteredBufferPool::new(&mut ring, 4096, 4) {
            Ok(p) => p,
            Err(_) => return,
        };

        // Acquire all buffers
        let mut indices = Vec::new();
        for _ in 0..4 {
            let (idx, _buf) = pool.acquire().unwrap();
            indices.push(idx);
        }

        assert_eq!(pool.available_count(), 0);
        assert_eq!(pool.acquired_count(), 4);
        assert!(pool.is_exhausted());

        // Should fail to acquire more
        assert!(pool.acquire().is_err());

        // Release one
        pool.release(indices[0]);
        assert_eq!(pool.available_count(), 1);
        assert!(!pool.is_exhausted());

        // Can acquire again
        let (idx, _) = pool.acquire().unwrap();
        assert_eq!(idx, indices[0]);
    }

    #[test]
    fn test_buffer_access() {
        let mut ring = match create_test_ring() {
            Some(r) => r,
            None => return,
        };

        let mut pool = match RegisteredBufferPool::new(&mut ring, 1024, 4) {
            Ok(p) => p,
            Err(_) => return,
        };

        let (idx, buf) = pool.acquire().unwrap();
        buf[..5].copy_from_slice(b"hello");

        // Release and get again
        pool.release(idx);

        let data = pool.get(idx).unwrap();
        assert_eq!(&data[..5], b"hello");
    }

    #[test]
    fn test_stats() {
        let mut ring = match create_test_ring() {
            Some(r) => r,
            None => return,
        };

        let pool = match RegisteredBufferPool::new(&mut ring, 4096, 16) {
            Ok(p) => p,
            Err(_) => return,
        };

        let stats = pool.stats();
        assert_eq!(stats.total_count, 16);
        assert_eq!(stats.available_count, 16);
        assert_eq!(stats.acquired_count, 0);
        assert_eq!(stats.buffer_size, 4096);
        assert_eq!(stats.total_bytes, 16 * 4096);
        assert_eq!(stats.acquisitions, 0);
        assert_eq!(stats.exhaustions, 0);

        let display = format!("{stats}");
        assert!(display.contains("16"));
        assert!(display.contains("4KB"));
    }

    #[test]
    fn test_buffer_pool_stats_initial() {
        let mut ring = match create_test_ring() {
            Some(r) => r,
            None => return,
        };

        let pool = match RegisteredBufferPool::new(&mut ring, 4096, 8) {
            Ok(p) => p,
            Err(_) => return,
        };

        let stats = pool.stats();
        assert_eq!(stats.total_count, 8);
        assert_eq!(stats.available_count, 8);
        assert_eq!(stats.acquired_count, 0);
        assert_eq!(stats.acquisitions, 0);
        assert_eq!(stats.exhaustions, 0);
    }

    #[test]
    fn test_buffer_pool_stats_after_acquire() {
        let mut ring = match create_test_ring() {
            Some(r) => r,
            None => return,
        };

        let mut pool = match RegisteredBufferPool::new(&mut ring, 4096, 4) {
            Ok(p) => p,
            Err(_) => return,
        };

        let (idx, _) = pool.acquire().unwrap();
        let stats = pool.stats();
        assert_eq!(stats.acquisitions, 1);
        assert_eq!(stats.exhaustions, 0);
        assert_eq!(stats.acquired_count, 1);
        assert_eq!(stats.available_count, 3);

        pool.release(idx);
        let _ = pool.acquire().unwrap();
        let _ = pool.acquire().unwrap();
        let stats = pool.stats();
        assert_eq!(stats.acquisitions, 3);
        assert_eq!(stats.exhaustions, 0);
    }

    #[test]
    fn test_buffer_pool_exhaustion_counter() {
        let mut ring = match create_test_ring() {
            Some(r) => r,
            None => return,
        };

        let mut pool = match RegisteredBufferPool::new(&mut ring, 4096, 2) {
            Ok(p) => p,
            Err(_) => return,
        };

        // Exhaust the pool
        let _ = pool.acquire().unwrap();
        let _ = pool.acquire().unwrap();

        // This should fail and increment exhaustions
        assert!(pool.acquire().is_err());
        assert!(pool.try_acquire().is_none());

        let stats = pool.stats();
        assert_eq!(stats.acquisitions, 2);
        assert_eq!(stats.exhaustions, 2); // One from acquire(), one from try_acquire()
        assert_eq!(stats.acquired_count, 2);
        assert_eq!(stats.available_count, 0);
    }

    #[test]
    fn test_write_read_fixed() {
        let mut ring = match create_test_ring() {
            Some(r) => r,
            None => return,
        };

        let mut pool = match RegisteredBufferPool::new(&mut ring, 4096, 4) {
            Ok(p) => p,
            Err(_) => return,
        };

        // Create a temp file
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

        // Write using registered buffer
        let (idx, buf) = pool.acquire().unwrap();
        buf[..5].copy_from_slice(b"hello");

        let user_data = pool.submit_write_fixed(&mut ring, fd, idx, 0, 5).unwrap();

        // Submit and wait
        ring.submit_and_wait(1).unwrap();

        // Check completion
        let mut cq = ring.completion();
        let cqe = cq.next().unwrap();
        assert_eq!(cqe.user_data(), user_data);
        assert!(cqe.result() >= 0);

        pool.release(idx);
    }
}
