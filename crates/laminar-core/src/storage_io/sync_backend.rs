//! Synchronous storage I/O backend.
//!
//! Executes all I/O inline via `std::fs`. Writes go through the OS page cache.
//! Sync calls `fdatasync`. Works on all platforms.
//!
//! This is the default backend when `io_uring` is not available (Windows, macOS,
//! older Linux kernels, or when the `io-uring` feature is not enabled).
//!
//! ## Completion model
//!
//! `submit_write` executes `pwrite`/`write` immediately and stages the
//! completion internally. `poll_completions` returns the staged completions
//! on the next call. This matches the submission-completion API contract
//! while doing zero async work.

use std::collections::VecDeque;
use std::fmt;
use std::fs::File;
use std::io::Write;

use super::{IoCompletion, IoFd, StorageIo, StorageIoError};

/// Maximum registered file descriptors.
const MAX_FDS: usize = 64;

/// Entry in the file descriptor table.
struct FdEntry {
    /// The owned file handle.
    file: File,
    /// Current append position (tracked for append mode).
    append_offset: u64,
}

/// Synchronous storage I/O backend.
///
/// Executes I/O inline in `submit_*` calls. Completions are staged in an
/// internal ring buffer and returned on the next `poll_completions` call.
///
/// Zero heap allocation on the hot path — the completion buffer and fd table
/// are pre-allocated at construction.
pub struct SyncStorageIo {
    /// Registered file descriptors (sparse array, indexed by `IoFd`).
    fds: Vec<Option<FdEntry>>,
    /// Staged completions from the last submit batch.
    completions: VecDeque<IoCompletion>,
    /// Next token ID.
    next_token: u64,
    /// Number of staged (unpolled) completions.
    pending: usize,
}

impl SyncStorageIo {
    /// Create a new synchronous backend.
    #[must_use]
    pub fn new() -> Self {
        let mut fds = Vec::with_capacity(MAX_FDS);
        fds.resize_with(MAX_FDS, || None);
        Self {
            fds,
            completions: VecDeque::with_capacity(256),
            next_token: 0,
            pending: 0,
        }
    }

    /// Allocate the next token.
    fn next_token(&mut self) -> u64 {
        let t = self.next_token;
        self.next_token = self.next_token.wrapping_add(1);
        t
    }

    /// Find a free slot in the fd table.
    fn find_free_slot(&self) -> Option<u32> {
        #[allow(clippy::cast_possible_truncation)]
        self.fds
            .iter()
            .position(Option::is_none)
            .map(|i| i as u32)
    }

    /// Get a mutable reference to an fd entry.
    fn get_entry(&mut self, fd: IoFd) -> Result<&mut FdEntry, StorageIoError> {
        self.fds
            .get_mut(fd.0 as usize)
            .and_then(|slot| slot.as_mut())
            .ok_or(StorageIoError::BadFd(fd))
    }

    /// Stage a completion for the next poll.
    fn stage_completion(&mut self, token: u64, result: i32) {
        self.completions
            .push_back(IoCompletion { token, result });
        self.pending += 1;
    }
}

impl Default for SyncStorageIo {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageIo for SyncStorageIo {
    fn register_fd(&mut self, file: File) -> Result<IoFd, StorageIoError> {
        let slot = self.find_free_slot().ok_or(StorageIoError::QueueFull)?;

        // Get current file size for append tracking
        let append_offset = file.metadata().map(|m| m.len()).unwrap_or(0);

        self.fds[slot as usize] = Some(FdEntry {
            file,
            append_offset,
        });
        Ok(IoFd(slot))
    }

    fn deregister_fd(&mut self, fd: IoFd) -> Result<(), StorageIoError> {
        let slot = self
            .fds
            .get_mut(fd.0 as usize)
            .ok_or(StorageIoError::BadFd(fd))?;
        if slot.is_none() {
            return Err(StorageIoError::BadFd(fd));
        }
        // Drop the File (closes the fd)
        *slot = None;
        Ok(())
    }

    fn submit_write(
        &mut self,
        fd: IoFd,
        data: &[u8],
        offset: u64,
    ) -> Result<u64, StorageIoError> {
        let token = self.next_token();
        let entry = self.get_entry(fd)?;

        // Execute pwrite immediately
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            match entry.file.write_at(data, offset) {
                Ok(n) => {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                    self.stage_completion(token, n as i32);
                }
                Err(e) => {
                    let errno = e.raw_os_error().unwrap_or(-1);
                    self.stage_completion(token, -errno.abs());
                }
            }
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            match entry.file.seek_write(data, offset) {
                Ok(n) => {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                    self.stage_completion(token, n as i32);
                }
                Err(e) => {
                    let code = e.raw_os_error().unwrap_or(-1);
                    self.stage_completion(token, -code.abs());
                }
            }
        }

        Ok(token)
    }

    fn submit_append(
        &mut self,
        fd: IoFd,
        data: &[u8],
    ) -> Result<u64, StorageIoError> {
        let token = self.next_token();
        let entry = self.get_entry(fd)?;

        // Write at tracked append position, then advance it.
        // Using write_all instead of pwrite for append semantics —
        // the file was opened in append mode, so write() appends atomically.
        match entry.file.write_all(data) {
            Ok(()) => {
                #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                let n = data.len() as i32;
                entry.append_offset += data.len() as u64;
                self.stage_completion(token, n);
            }
            Err(e) => {
                let errno = e.raw_os_error().unwrap_or(-1);
                self.stage_completion(token, -errno.abs());
            }
        }

        Ok(token)
    }

    fn submit_datasync(&mut self, fd: IoFd) -> Result<u64, StorageIoError> {
        let token = self.next_token();
        let entry = self.get_entry(fd)?;

        // Flush + fdatasync immediately
        match entry.file.flush().and_then(|()| entry.file.sync_data()) {
            Ok(()) => self.stage_completion(token, 0),
            Err(e) => {
                let errno = e.raw_os_error().unwrap_or(-1);
                self.stage_completion(token, -errno.abs());
            }
        }

        Ok(token)
    }

    fn flush(&mut self) -> Result<usize, StorageIoError> {
        // No-op: sync backend executes I/O inline.
        Ok(0)
    }

    fn poll_completions(&mut self, out: &mut Vec<IoCompletion>) {
        while let Some(c) = self.completions.pop_front() {
            out.push(c);
            self.pending -= 1;
        }
    }

    fn pending_count(&self) -> usize {
        self.pending
    }
}

impl fmt::Debug for SyncStorageIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let registered = self.fds.iter().filter(|e| e.is_some()).count();
        f.debug_struct("SyncStorageIo")
            .field("registered_fds", &registered)
            .field("pending_completions", &self.completions.len())
            .field("pending", &self.pending)
            .field("next_token", &self.next_token)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use tempfile::NamedTempFile;

    #[test]
    fn test_register_and_write() {
        let mut backend = SyncStorageIo::new();
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Register the file
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap();
        let fd = backend.register_fd(file).unwrap();

        // Submit a write at offset 0
        let token = backend.submit_write(fd, b"hello", 0).unwrap();

        // Poll completions
        let mut completions = Vec::new();
        backend.poll_completions(&mut completions);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].token, token);
        assert_eq!(completions[0].result, 5); // 5 bytes written

        // Verify data
        let mut contents = String::new();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        assert_eq!(contents, "hello");

        // Deregister
        backend.deregister_fd(fd).unwrap();
    }

    #[test]
    fn test_append() {
        let mut backend = SyncStorageIo::new();
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let file = std::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&path)
            .unwrap();
        let fd = backend.register_fd(file).unwrap();

        backend.submit_append(fd, b"hello").unwrap();
        backend.submit_append(fd, b" world").unwrap();

        let mut completions = Vec::new();
        backend.poll_completions(&mut completions);
        assert_eq!(completions.len(), 2);
        assert_eq!(completions[0].result, 5);
        assert_eq!(completions[1].result, 6);

        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "hello world");

        backend.deregister_fd(fd).unwrap();
    }

    #[test]
    fn test_datasync() {
        let mut backend = SyncStorageIo::new();
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap();
        let fd = backend.register_fd(file).unwrap();

        let token = backend.submit_datasync(fd).unwrap();

        let mut completions = Vec::new();
        backend.poll_completions(&mut completions);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].token, token);
        assert_eq!(completions[0].result, 0); // success

        backend.deregister_fd(fd).unwrap();
    }

    #[test]
    fn test_bad_fd() {
        let mut backend = SyncStorageIo::new();
        let bad_fd = IoFd(99);
        assert!(backend.submit_write(bad_fd, b"x", 0).is_err());
        assert!(backend.submit_datasync(bad_fd).is_err());
        assert!(backend.deregister_fd(bad_fd).is_err());
    }

    #[test]
    fn test_pending_count() {
        let mut backend = SyncStorageIo::new();
        assert_eq!(backend.pending_count(), 0);

        let tmp = NamedTempFile::new().unwrap();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(tmp.path())
            .unwrap();
        let fd = backend.register_fd(file).unwrap();

        backend.submit_write(fd, b"x", 0).unwrap();
        assert_eq!(backend.pending_count(), 1);

        backend.submit_write(fd, b"y", 1).unwrap();
        assert_eq!(backend.pending_count(), 2);

        let mut completions = Vec::new();
        backend.poll_completions(&mut completions);
        assert_eq!(backend.pending_count(), 0);
        assert_eq!(completions.len(), 2);
    }

    #[test]
    fn test_flush_is_noop() {
        let mut backend = SyncStorageIo::new();
        assert_eq!(backend.flush().unwrap(), 0);
    }

    #[test]
    fn test_multiple_fds() {
        let mut backend = SyncStorageIo::new();

        let tmp1 = NamedTempFile::new().unwrap();
        let tmp2 = NamedTempFile::new().unwrap();

        let f1 = std::fs::OpenOptions::new()
            .write(true)
            .open(tmp1.path())
            .unwrap();
        let f2 = std::fs::OpenOptions::new()
            .write(true)
            .open(tmp2.path())
            .unwrap();

        let fd1 = backend.register_fd(f1).unwrap();
        let fd2 = backend.register_fd(f2).unwrap();
        assert_ne!(fd1, fd2);

        backend.submit_write(fd1, b"file1", 0).unwrap();
        backend.submit_write(fd2, b"file2", 0).unwrap();

        let mut completions = Vec::new();
        backend.poll_completions(&mut completions);
        assert_eq!(completions.len(), 2);

        assert_eq!(std::fs::read_to_string(tmp1.path()).unwrap(), "file1");
        assert_eq!(std::fs::read_to_string(tmp2.path()).unwrap(), "file2");
    }
}
