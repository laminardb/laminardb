//! `io_uring`-backed Write-Ahead Log for high-performance durability.
//!
//! This module provides an async WAL implementation using `io_uring` for
//! zero-copy I/O operations. It integrates with the registered buffer pool
//! to eliminate per-operation buffer mapping overhead.
//!
//! ## Performance Benefits
//!
//! - **SQPOLL Mode**: Eliminates syscalls under sustained load
//! - **Registered Buffers**: Zero-copy I/O operations
//! - **Async fdatasync**: Non-blocking durability guarantees
//!
//! ## Platform Support
//!
//! Linux 5.10+ only. On other platforms, use the standard `WriteAheadLog`.

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod linux_impl {
    use std::collections::VecDeque;
    use std::fs::{File, OpenOptions};
    use std::io::{self, Write};
    use std::os::unix::io::{AsRawFd, RawFd};
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    use laminar_core::io_uring::{
        Completion, CoreRingManager, IoUringConfig, IoUringError, RingMode,
    };
    use rkyv::rancor::Error as RkyvError;
    use rkyv::util::AlignedVec;

    use crate::wal::{WalEntry, WalError};

    /// Pending write operation for group commit.
    struct PendingWrite {
        /// User data for tracking completion.
        user_data: u64,
        /// Buffer index used for this write.
        buf_index: u16,
        /// Size of the write.
        len: u32,
        /// File offset of the write.
        offset: u64,
    }

    /// `io_uring`-backed Write-Ahead Log.
    ///
    /// Uses registered buffers and SQPOLL mode for maximum throughput.
    /// Supports group commit for batching multiple writes into a single fsync.
    pub struct IoUringWal {
        /// Path to the WAL file.
        path: PathBuf,
        /// File descriptor for WAL.
        fd: RawFd,
        /// Keep the File alive to maintain the fd.
        #[allow(dead_code)]
        file: File,
        /// Per-core ring manager.
        ring_manager: CoreRingManager,
        /// Current write position.
        position: u64,
        /// Pending writes awaiting completion.
        pending_writes: VecDeque<PendingWrite>,
        /// Group commit interval.
        sync_interval: Duration,
        /// Last sync time.
        last_sync: Instant,
        /// Whether to sync on every write (for testing).
        sync_on_write: bool,
        /// Records written since last sync.
        records_since_sync: u64,
    }

    impl IoUringWal {
        /// Create a new `io_uring`-backed WAL.
        ///
        /// # Arguments
        ///
        /// * `path` - Path to the WAL file
        /// * `sync_interval` - Interval for group commit
        /// * `config` - io_uring configuration
        ///
        /// # Errors
        ///
        /// Returns an error if the file cannot be created or io_uring initialization fails.
        pub fn new<P: AsRef<Path>>(
            path: P,
            sync_interval: Duration,
            config: Option<IoUringConfig>,
        ) -> Result<Self, WalError> {
            let path = path.as_ref().to_path_buf();

            // Open file for direct I/O (O_DIRECT for best io_uring performance)
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&path)?;

            let position = file.metadata()?.len();
            let fd = file.as_raw_fd();

            // Create io_uring config with SQPOLL
            let ring_config = config.unwrap_or_else(|| {
                IoUringConfig::builder()
                    .ring_entries(256)
                    .mode(RingMode::SqPoll)
                    .buffer_size(64 * 1024) // 64KB buffers
                    .buffer_count(64)       // 64 buffers for concurrent writes
                    .build_unchecked()
            });

            let ring_manager = CoreRingManager::new(0, &ring_config)
                .map_err(|e| WalError::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?;

            Ok(Self {
                path,
                fd,
                file,
                ring_manager,
                position,
                pending_writes: VecDeque::new(),
                sync_interval,
                last_sync: Instant::now(),
                sync_on_write: false,
                records_since_sync: 0,
            })
        }

        /// Enable sync on every write (for testing).
        pub fn set_sync_on_write(&mut self, enabled: bool) {
            self.sync_on_write = enabled;
        }

        /// Append an entry to the WAL using io_uring.
        ///
        /// This method submits the write asynchronously and may return before
        /// the write is complete. Call `sync()` to ensure durability.
        ///
        /// # Errors
        ///
        /// Returns an error if serialization fails or the buffer pool is exhausted.
        pub fn append(&mut self, entry: &WalEntry) -> Result<u64, WalError> {
            let start_pos = self.position;

            // Serialize the entry
            let bytes: AlignedVec = rkyv::to_bytes::<RkyvError>(entry)
                .map_err(|e| WalError::Serialization(e.to_string()))?;

            // Compute CRC32C
            let crc = crc32c::crc32c(&bytes);

            // Acquire a registered buffer
            let (buf_index, buf) = self.ring_manager.acquire_buffer().map_err(|e| {
                WalError::Io(io::Error::new(io::ErrorKind::Other, e.to_string()))
            })?;

            // Write record format: [length: 4][crc32: 4][data: length]
            #[allow(clippy::cast_possible_truncation)]
            let len = bytes.len() as u32;
            let header_size = 8usize;
            let total_size = header_size + bytes.len();

            // Copy data to registered buffer
            buf[..4].copy_from_slice(&len.to_le_bytes());
            buf[4..8].copy_from_slice(&crc.to_le_bytes());
            buf[8..total_size].copy_from_slice(&bytes);

            // Submit write
            let user_data = self
                .ring_manager
                .submit_write(self.fd, buf_index, self.position, total_size as u32)
                .map_err(|e| WalError::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?;

            // Track pending write
            self.pending_writes.push_back(PendingWrite {
                user_data,
                buf_index,
                len: total_size as u32,
                offset: self.position,
            });

            self.position += total_size as u64;
            self.records_since_sync += 1;

            // Submit to kernel
            self.ring_manager.submit().map_err(|e| {
                WalError::Io(io::Error::new(io::ErrorKind::Other, e.to_string()))
            })?;

            // Check if we need to sync
            if self.sync_on_write || self.last_sync.elapsed() >= self.sync_interval {
                self.sync()?;
            }

            Ok(start_pos)
        }

        /// Wait for all pending writes to complete and sync to disk.
        ///
        /// # Errors
        ///
        /// Returns an error if any write fails or the sync fails.
        pub fn sync(&mut self) -> Result<(), WalError> {
            // Wait for all pending writes to complete
            while !self.pending_writes.is_empty() {
                let completions = self.ring_manager.poll_completions();
                for completion in completions {
                    self.handle_completion(completion)?;
                }

                if !self.pending_writes.is_empty() {
                    // Need to wait for more completions
                    self.ring_manager.submit_and_wait(1).map_err(|e| {
                        WalError::Io(io::Error::new(io::ErrorKind::Other, e.to_string()))
                    })?;
                }
            }

            // Submit fdatasync
            let sync_user_data = self.ring_manager.submit_sync(self.fd, true).map_err(|e| {
                WalError::Io(io::Error::new(io::ErrorKind::Other, e.to_string()))
            })?;

            // Wait for sync to complete
            loop {
                self.ring_manager.submit_and_wait(1).map_err(|e| {
                    WalError::Io(io::Error::new(io::ErrorKind::Other, e.to_string()))
                })?;

                let completions = self.ring_manager.poll_completions();
                for completion in completions {
                    if completion.user_data == sync_user_data {
                        if completion.is_success() {
                            self.last_sync = Instant::now();
                            self.records_since_sync = 0;
                            return Ok(());
                        } else {
                            return Err(WalError::Io(io::Error::new(
                                io::ErrorKind::Other,
                                format!("fdatasync failed: {:?}", completion.error()),
                            )));
                        }
                    }
                    // Handle any straggling write completions
                    self.handle_completion(completion)?;
                }
            }
        }

        /// Handle a write completion.
        fn handle_completion(&mut self, completion: Completion) -> Result<(), WalError> {
            // Find and remove the pending write
            if let Some(pos) = self
                .pending_writes
                .iter()
                .position(|p| p.user_data == completion.user_data)
            {
                // SAFETY: `pos` was just returned by `.position()`, so it's guaranteed to be valid
                let pending = self.pending_writes.remove(pos).unwrap();

                // Release the buffer
                self.ring_manager.release_buffer(pending.buf_index);

                if !completion.is_success() {
                    return Err(WalError::Io(io::Error::new(
                        io::ErrorKind::Other,
                        format!("Write failed at offset {}: {:?}", pending.offset, completion.error()),
                    )));
                }
            }

            Ok(())
        }

        /// Get the current position in the log.
        #[must_use]
        pub fn position(&self) -> u64 {
            self.position
        }

        /// Get the path to the WAL file.
        #[must_use]
        pub fn path(&self) -> &Path {
            &self.path
        }

        /// Get ring metrics.
        #[must_use]
        pub fn metrics(&self) -> &laminar_core::io_uring::RingMetrics {
            self.ring_manager.metrics()
        }

        /// Get the number of pending writes.
        #[must_use]
        pub fn pending_count(&self) -> usize {
            self.pending_writes.len()
        }

        /// Get the number of records written since last sync.
        #[must_use]
        pub fn records_since_sync(&self) -> u64 {
            self.records_since_sync
        }
    }

    impl Drop for IoUringWal {
        fn drop(&mut self) {
            // Try to sync any remaining writes
            if !self.pending_writes.is_empty() {
                let _ = self.sync();
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::collections::HashMap;
        use tempfile::tempdir;

        fn make_config() -> IoUringConfig {
            IoUringConfig::builder()
                .ring_entries(32)
                .mode(RingMode::Standard) // Use standard mode for tests
                .buffer_size(4096)
                .buffer_count(8)
                .build_unchecked()
        }

        #[test]
        fn test_io_uring_wal_create() {
            let dir = tempdir().unwrap();
            let path = dir.path().join("test.wal");

            let result = IoUringWal::new(&path, Duration::from_secs(1), Some(make_config()));
            match result {
                Ok(wal) => {
                    assert_eq!(wal.position(), 0);
                }
                Err(e) => {
                    eprintln!("io_uring WAL not available: {e}");
                }
            }
        }

        #[test]
        fn test_io_uring_wal_append() {
            let dir = tempdir().unwrap();
            let path = dir.path().join("test.wal");

            let mut wal = match IoUringWal::new(&path, Duration::from_secs(1), Some(make_config())) {
                Ok(w) => w,
                Err(_) => return, // Skip if io_uring not available
            };

            wal.set_sync_on_write(true);

            let pos = wal
                .append(&WalEntry::Put {
                    key: b"key1".to_vec(),
                    value: b"value1".to_vec(),
                })
                .unwrap();

            assert_eq!(pos, 0);
            assert!(wal.position() > 0);
        }

        #[test]
        fn test_io_uring_wal_multiple_appends() {
            let dir = tempdir().unwrap();
            let path = dir.path().join("test.wal");

            let mut wal = match IoUringWal::new(&path, Duration::from_secs(1), Some(make_config())) {
                Ok(w) => w,
                Err(_) => return,
            };

            // Append multiple entries
            for i in 0..10 {
                wal.append(&WalEntry::Put {
                    key: format!("key{i}").into_bytes(),
                    value: format!("value{i}").into_bytes(),
                })
                .unwrap();
            }

            // Sync to ensure all writes complete
            wal.sync().unwrap();

            assert_eq!(wal.pending_count(), 0);
            assert!(wal.position() > 0);
        }

        #[test]
        fn test_io_uring_wal_commit() {
            let dir = tempdir().unwrap();
            let path = dir.path().join("test.wal");

            let mut wal = match IoUringWal::new(&path, Duration::from_secs(1), Some(make_config())) {
                Ok(w) => w,
                Err(_) => return,
            };

            let mut offsets = HashMap::new();
            offsets.insert("topic1".to_string(), 100u64);

            wal.append(&WalEntry::Commit {
                offsets,
                watermark: Some(1000),
            })
            .unwrap();

            wal.sync().unwrap();
        }
    }
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use linux_impl::IoUringWal;

/// Check if `io_uring` WAL is available on this platform.
#[must_use]
pub fn is_available() -> bool {
    cfg!(all(target_os = "linux", feature = "io-uring"))
}
