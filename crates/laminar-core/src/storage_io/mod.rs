//! # Storage I/O Abstraction
//!
//! Platform-abstracted non-blocking storage I/O for the thread-per-core architecture.
//!
//! ## Design
//!
//! Ring 0 cannot block, allocate, or syscall on the hot path. Storage I/O needs
//! a submission-completion model that fits the synchronous spin-loop:
//!
//! 1. **Submit**: push a write or sync request (non-blocking)
//! 2. **Poll**: check for completions on the next loop iteration
//!
//! Two backends implement this interface:
//!
//! - `SyncStorageIo` — executes I/O inline via `std::fs`. Write "completes"
//!   when data is in the kernel page cache. Sync "completes" after `fdatasync`.
//!   Works on all platforms. This is the default.
//!
//! - `UringStorageIo` (Linux + `io-uring` feature) — submits SQEs to a per-core
//!   ring. With SQPOLL, submissions are zero-syscall. Completions arrive via
//!   CQE polling.
//!
//! ## Usage from Ring 0
//!
//! ```text
//! // Cold path (init_core_thread):
//! let storage = SyncStorageIo::new();
//! let wal_fd = storage.register_fd(file);
//!
//! // Hot path (core_thread_main loop):
//! storage.submit_write(fd, &data, offset)?;
//! storage.flush()?;
//! // ... next iteration ...
//! completions.clear();
//! storage.poll_completions(&mut completions);
//! ```

mod sync_backend;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod uring_backend;

pub use sync_backend::SyncStorageIo;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use uring_backend::UringStorageIo;

use std::fmt;

// ── Opaque file descriptor ──────────────────────────────────────────────────

/// Opaque file descriptor managed by a [`StorageIo`] backend.
///
/// Created via [`StorageIo::register_fd`]. The backend maps this to the
/// platform-native handle internally (Unix `RawFd`, Windows `RawHandle`,
/// or `io_uring` registered fd index).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IoFd(pub(crate) u32);

// ── Completion token ────────────────────────────────────────────────────────

/// Completion of a submitted I/O operation.
///
/// Returned by [`StorageIo::poll_completions`]. The `token` matches the
/// value returned by the corresponding `submit_*` call.
#[derive(Debug, Clone, Copy)]
pub struct IoCompletion {
    /// Token that was returned by `submit_write` / `submit_datasync`.
    pub token: u64,
    /// Bytes transferred (for writes) or 0 (for sync).
    /// Negative values indicate a POSIX errno.
    pub result: i32,
}

// ── Error type ──────────────────────────────────────────────────────────────

/// Errors from the storage I/O layer.
///
/// Intentionally small — this is used on the hot path.
#[derive(Debug)]
pub enum StorageIoError {
    /// The submission queue or internal buffer is full.
    QueueFull,
    /// An OS-level I/O error occurred.
    Io(std::io::Error),
    /// The requested `IoFd` is not registered.
    BadFd(IoFd),
    /// The backend is closed / shutting down.
    Closed,
    /// Buffer pool exhausted (`io_uring` only).
    BufferExhausted,
}

impl fmt::Display for StorageIoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueFull => write!(f, "storage I/O submission queue full"),
            Self::Io(e) => write!(f, "storage I/O error: {e}"),
            Self::BadFd(fd) => write!(f, "unregistered IoFd({})", fd.0),
            Self::Closed => write!(f, "storage I/O backend closed"),
            Self::BufferExhausted => write!(f, "registered buffer pool exhausted"),
        }
    }
}

impl std::error::Error for StorageIoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StorageIoError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

// ── Trait ────────────────────────────────────────────────────────────────────

/// Non-blocking storage I/O backend for Ring 0.
///
/// All `submit_*` methods are non-blocking. They enqueue work and return a
/// token immediately. The caller polls for completions on the next loop
/// iteration via [`poll_completions`](StorageIo::poll_completions).
///
/// # Ring 0 contract
///
/// Implementations **must not**:
/// - Block (no mutexes, no condition variables)
/// - Allocate on submit/poll hot path (pre-allocate in `new`)
/// - Panic (return errors instead)
///
/// The sync backend executes I/O inline in `submit_*` and stages completions
/// for the next `poll_completions` call. The `io_uring` backend pushes SQEs
/// and polls CQEs.
#[allow(clippy::missing_errors_doc)] // errors documented per-method below
pub trait StorageIo: Send {
    /// Register an already-opened file with this backend.
    ///
    /// Called on the cold path (init / checkpoint setup). Returns an opaque
    /// [`IoFd`] used for subsequent I/O operations.
    ///
    /// Returns [`StorageIoError::QueueFull`] if the fd table is full.
    ///
    /// # Platform notes
    ///
    /// On Unix the backend extracts `RawFd` via `AsRawFd`.
    /// On Windows the backend extracts `RawHandle` via `AsRawHandle`.
    fn register_fd(&mut self, file: std::fs::File) -> Result<IoFd, StorageIoError>;

    /// Deregister and close a previously registered file.
    ///
    /// Called on the cold path (shutdown / WAL truncate-and-reopen).
    /// Returns [`StorageIoError::BadFd`] if the fd is not registered.
    fn deregister_fd(&mut self, fd: IoFd) -> Result<(), StorageIoError>;

    /// Submit a write at the given file offset. **Non-blocking.**
    ///
    /// `data` is copied into backend-managed storage before returning.
    /// The caller's buffer is free to reuse immediately.
    ///
    /// Returns a token for tracking completion, or [`StorageIoError::BadFd`]
    /// / [`StorageIoError::BufferExhausted`] on failure.
    fn submit_write(&mut self, fd: IoFd, data: &[u8], offset: u64) -> Result<u64, StorageIoError>;

    /// Submit an append (write at current end-of-file). **Non-blocking.**
    ///
    /// Semantically equivalent to `submit_write` at `offset = current_size`.
    /// The sync backend uses the file's append mode. The `io_uring` backend
    /// passes `offset = u64::MAX` which means "append" in Linux 6.0+.
    ///
    /// Returns a token for tracking completion, or [`StorageIoError::BadFd`]
    /// / [`StorageIoError::BufferExhausted`] on failure.
    fn submit_append(&mut self, fd: IoFd, data: &[u8]) -> Result<u64, StorageIoError>;

    /// Submit an `fdatasync` (data-only sync, no metadata). **Non-blocking.**
    ///
    /// Returns a token for tracking completion, or [`StorageIoError::BadFd`]
    /// on failure.
    fn submit_datasync(&mut self, fd: IoFd) -> Result<u64, StorageIoError>;

    /// Flush pending submissions to the kernel.
    ///
    /// - **`io_uring` (SQPOLL)**: no-op — kernel polls the SQ automatically.
    /// - **`io_uring` (standard)**: calls `ring.submit()`.
    /// - **Sync backend**: no-op — I/O was executed inline in `submit_*`.
    ///
    /// Returns the number of operations submitted, or
    /// [`StorageIoError::Closed`] if the backend is shut down.
    fn flush(&mut self) -> Result<usize, StorageIoError>;

    /// Poll for completed operations. **Non-blocking.**
    ///
    /// Appends completions to `out`. The caller owns the Vec and should
    /// clear it between iterations to reuse capacity (zero-alloc pattern).
    fn poll_completions(&mut self, out: &mut Vec<IoCompletion>);

    /// Returns the number of operations submitted but not yet completed.
    fn pending_count(&self) -> usize;
}
