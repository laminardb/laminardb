//! io_uring storage I/O backend (Linux only, requires `io-uring` feature).
//!
//! Wraps [`CoreRingManager`] behind the [`StorageIo`] trait. With SQPOLL enabled,
//! submissions are zero-syscall — the kernel polling thread picks up SQEs
//! automatically. Registered buffers eliminate kernel-side copies.
//!
//! ## Buffer lifecycle
//!
//! `submit_write` acquires a registered buffer, copies data into it, and pushes
//! a WriteFixed SQE. The buffer is marked in-flight until the CQE arrives in
//! `poll_completions`, at which point it is released back to the pool.
//!
//! ## Ordering
//!
//! SQEs within a single ring are processed in submission order. For a per-core
//! WAL writer (single file, single thread), this guarantees that a write
//! followed by an fdatasync will sync all preceding writes.

use std::collections::HashMap;
use std::fs::File;
use std::os::unix::io::{AsRawFd, IntoRawFd, RawFd};

use crate::io_uring::{Completion, CoreRingManager, IoUringConfig, IoUringError};

use super::{IoCompletion, IoFd, StorageIo, StorageIoError};

/// Maximum registered file descriptors.
const MAX_FDS: usize = 64;

/// io_uring storage I/O backend.
///
/// One instance per core. Uses SQPOLL for zero-syscall submission and
/// registered buffers for zero-copy writes.
pub struct UringStorageIo {
    /// The per-core ring manager.
    manager: CoreRingManager,
    /// Map from `IoFd` to OS `RawFd`.
    fd_table: Vec<Option<RawFd>>,
    /// Reverse map for cleanup.
    raw_to_io: HashMap<RawFd, IoFd>,
    /// Pre-allocated scratch buffer for completions (zero-alloc poll path).
    completion_scratch: Vec<Completion>,
}

impl UringStorageIo {
    /// Create a new io_uring backend for a specific core.
    ///
    /// # Errors
    ///
    /// Returns an error if ring creation fails (insufficient privileges for
    /// SQPOLL, unsupported kernel version, etc.).
    pub fn new(core_id: usize, config: &IoUringConfig) -> Result<Self, IoUringError> {
        let manager = CoreRingManager::new(core_id, config)?;
        let mut fd_table = Vec::with_capacity(MAX_FDS);
        fd_table.resize_with(MAX_FDS, || None);
        Ok(Self {
            manager,
            fd_table,
            raw_to_io: HashMap::new(),
            completion_scratch: Vec::with_capacity(config.ring_entries as usize),
        })
    }

    /// Find a free slot in the fd table.
    fn find_free_slot(&self) -> Option<u32> {
        #[allow(clippy::cast_possible_truncation)]
        self.fd_table
            .iter()
            .position(|e| e.is_none())
            .map(|i| i as u32)
    }

    /// Get the raw fd for an IoFd.
    fn raw_fd(&self, fd: IoFd) -> Result<RawFd, StorageIoError> {
        self.fd_table
            .get(fd.0 as usize)
            .and_then(|slot| *slot)
            .ok_or(StorageIoError::BadFd(fd))
    }
}

impl StorageIo for UringStorageIo {
    fn register_fd(&mut self, file: File) -> Result<IoFd, StorageIoError> {
        let slot = self.find_free_slot().ok_or(StorageIoError::QueueFull)?;
        let raw = file.into_raw_fd();
        self.fd_table[slot as usize] = Some(raw);
        let io_fd = IoFd(slot);
        self.raw_to_io.insert(raw, io_fd);
        Ok(io_fd)
    }

    fn deregister_fd(&mut self, fd: IoFd) -> Result<(), StorageIoError> {
        let raw = self.raw_fd(fd)?;
        self.fd_table[fd.0 as usize] = None;
        self.raw_to_io.remove(&raw);

        // Close the fd via io_uring (non-blocking)
        let _ = self.manager.submit_close(raw);
        let _ = self.manager.submit();
        Ok(())
    }

    fn submit_write(
        &mut self,
        fd: IoFd,
        data: &[u8],
        offset: u64,
    ) -> Result<u64, StorageIoError> {
        let raw = self.raw_fd(fd)?;

        // Acquire a registered buffer, copy data into it
        let (buf_idx, buf) = self
            .manager
            .acquire_buffer()
            .map_err(|_| StorageIoError::BufferExhausted)?;
        let len = data.len().min(buf.len());
        buf[..len].copy_from_slice(&data[..len]);

        // Submit WriteFixed SQE
        #[allow(clippy::cast_possible_truncation)]
        let token = self
            .manager
            .submit_write(raw, buf_idx, offset, len as u32)
            .map_err(|e| match e {
                IoUringError::SubmissionQueueFull => StorageIoError::QueueFull,
                IoUringError::RingClosed => StorageIoError::Closed,
                other => StorageIoError::Io(std::io::Error::other(other.to_string())),
            })?;

        Ok(token)
    }

    fn submit_append(
        &mut self,
        fd: IoFd,
        data: &[u8],
    ) -> Result<u64, StorageIoError> {
        // io_uring: offset = u64::MAX means "append" (Linux 6.0+, IORING_FILE_INDEX_ALLOC)
        // For older kernels this falls back to offset -1 which is equivalent for
        // files opened with O_APPEND.
        self.submit_write(fd, data, u64::MAX)
    }

    fn submit_datasync(&mut self, fd: IoFd) -> Result<u64, StorageIoError> {
        let raw = self.raw_fd(fd)?;

        let token = self
            .manager
            .submit_sync(raw, true) // datasync=true → FDATASYNC
            .map_err(|e| match e {
                IoUringError::SubmissionQueueFull => StorageIoError::QueueFull,
                IoUringError::RingClosed => StorageIoError::Closed,
                other => StorageIoError::Io(std::io::Error::other(other.to_string())),
            })?;

        Ok(token)
    }

    fn flush(&mut self) -> Result<usize, StorageIoError> {
        // In SQPOLL mode this is typically a no-op.
        // In standard mode this calls ring.submit().
        self.manager.submit().map_err(|e| match e {
            IoUringError::RingClosed => StorageIoError::Closed,
            other => StorageIoError::Io(std::io::Error::other(other.to_string())),
        })
    }

    fn poll_completions(&mut self, out: &mut Vec<IoCompletion>) {
        // Use the zero-alloc path: poll into pre-allocated scratch buffer,
        // then convert to IoCompletion in the caller's output buffer.
        self.completion_scratch.clear();
        self.manager
            .poll_completions_into(&mut self.completion_scratch);
        for c in self.completion_scratch.drain(..) {
            out.push(IoCompletion {
                token: c.user_data,
                result: c.result,
            });
        }
    }

    fn pending_count(&self) -> usize {
        self.manager.pending_count()
    }
}

impl std::fmt::Debug for UringStorageIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let registered = self.fd_table.iter().filter(|e| e.is_some()).count();
        f.debug_struct("UringStorageIo")
            .field("core_id", &self.manager.core_id())
            .field("uses_sqpoll", &self.manager.uses_sqpoll())
            .field("registered_fds", &registered)
            .field("pending", &self.manager.pending_count())
            .finish()
    }
}
