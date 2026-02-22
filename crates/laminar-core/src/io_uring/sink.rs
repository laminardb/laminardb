//! `io_uring`-backed sink for high-performance async output.
//!
//! This module provides a sink implementation that uses `io_uring` for
//! writing outputs to files with minimal syscall overhead.
//!
//! ## Usage
//!
//! ```ignore
//! use laminar_core::io_uring::{IoUringSink, IoUringConfig};
//!
//! let config = IoUringConfig::builder()
//!     .ring_entries(256)
//!     .buffer_size(64 * 1024)
//!     .build_unchecked();
//!
//! let sink = IoUringSink::new("/tmp/output.log", config)?;
//! reactor.set_sink(Box::new(sink));
//! ```

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod linux_impl {
    use std::fs::{File, OpenOptions};
    use std::os::unix::io::AsRawFd;
    use std::path::{Path, PathBuf};

    use crate::io_uring::{CoreRingManager, IoUringConfig, IoUringError};
    use crate::operator::{CheckpointCompleteData, Output, SideOutputData};
    use crate::reactor::{Sink, SinkError};

    /// `io_uring`-backed sink for file output.
    ///
    /// Uses registered buffers and optional SQPOLL mode for maximum throughput.
    pub struct IoUringSink {
        /// Path to the output file.
        path: PathBuf,
        /// File handle.
        file: File,
        /// Per-core ring manager.
        ring_manager: CoreRingManager,
        /// Current write position.
        position: u64,
        /// Pending write count.
        pending_writes: usize,
        /// Maximum pending writes before forced flush.
        max_pending: usize,
    }

    impl IoUringSink {
        /// Creates a new `io_uring`-backed sink.
        ///
        /// # Arguments
        ///
        /// * `path` - Path to the output file
        /// * `config` - `io_uring` configuration
        ///
        /// # Errors
        ///
        /// Returns an error if the file cannot be created or `io_uring` initialization fails.
        pub fn new<P: AsRef<Path>>(path: P, config: &IoUringConfig) -> Result<Self, IoUringError> {
            let path = path.as_ref().to_path_buf();

            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .map_err(IoUringError::RingCreation)?;

            let position = 0;
            let ring_manager = CoreRingManager::new(0, config)?;
            let max_pending = config.buffer_count.min(32);

            Ok(Self {
                path,
                file,
                ring_manager,
                position,
                pending_writes: 0,
                max_pending,
            })
        }

        /// Appends a new file for appending.
        ///
        /// # Errors
        ///
        /// Returns an error if the file cannot be opened or `io_uring` initialization fails.
        pub fn append<P: AsRef<Path>>(
            path: P,
            config: &IoUringConfig,
        ) -> Result<Self, IoUringError> {
            let path = path.as_ref().to_path_buf();

            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(IoUringError::RingCreation)?;

            let position = file.metadata().map_err(IoUringError::RingCreation)?.len();

            let ring_manager = CoreRingManager::new(0, config)?;
            let max_pending = config.buffer_count.min(32);

            Ok(Self {
                path,
                file,
                ring_manager,
                position,
                pending_writes: 0,
                max_pending,
            })
        }

        /// Gets the current write position.
        #[must_use]
        pub fn position(&self) -> u64 {
            self.position
        }

        /// Gets the path to the output file.
        #[must_use]
        pub fn path(&self) -> &Path {
            &self.path
        }

        /// Gets the number of pending writes.
        #[must_use]
        pub fn pending_writes(&self) -> usize {
            self.pending_writes
        }

        /// Waits for all pending writes to complete.
        fn wait_for_pending(&mut self) -> Result<(), SinkError> {
            while self.pending_writes > 0 {
                self.ring_manager
                    .submit_and_wait(1)
                    .map_err(|e| SinkError::FlushFailed(e.to_string()))?;

                let completions = self.ring_manager.poll_completions();
                for completion in completions {
                    if !completion.is_success() {
                        return Err(SinkError::WriteFailed(format!(
                            "io_uring write failed: {:?}",
                            completion.error()
                        )));
                    }
                    self.pending_writes = self.pending_writes.saturating_sub(1);
                }
            }
            Ok(())
        }

        /// Writes a single output using `io_uring`.
        #[allow(clippy::cast_possible_truncation)]
        fn write_output(&mut self, output: &Output) -> Result<(), SinkError> {
            // Serialize the output to bytes
            let bytes = match output {
                Output::Event(event) => {
                    // For events, we serialize a simple representation
                    format!(
                        "EVENT ts={} rows={}\n",
                        event.timestamp,
                        event.data.num_rows()
                    )
                    .into_bytes()
                }
                Output::Watermark(ts) => format!("WATERMARK ts={ts}\n").into_bytes(),
                Output::LateEvent(event) => {
                    format!("LATE_EVENT ts={}\n", event.timestamp).into_bytes()
                }
                Output::SideOutput(data) => {
                    let SideOutputData { name, event } = *data;
                    format!("SIDE_OUTPUT name={} ts={}\n", name, event.timestamp).into_bytes()
                }
                Output::Changelog(record) => {
                    format!("CHANGELOG op={:?}\n", record.operation).into_bytes()
                }
                Output::CheckpointComplete(data) => {
                    format!("CHECKPOINT_COMPLETE id={}\n", data.checkpoint_id).into_bytes()
                }
            };

            // Acquire a buffer
            let (buf_index, buf) = self
                .ring_manager
                .acquire_buffer()
                .map_err(|e| SinkError::WriteFailed(format!("Failed to acquire buffer: {e}")))?;

            // Copy data to buffer
            let len = bytes.len().min(buf.len());
            buf[..len].copy_from_slice(&bytes[..len]);

            // Submit write
            let fd = self.file.as_raw_fd();
            self.ring_manager
                .submit_write(fd, buf_index, self.position, len as u32)
                .map_err(|e| SinkError::WriteFailed(e.to_string()))?;

            self.position += len as u64;
            self.pending_writes += 1;

            // Submit to kernel
            self.ring_manager
                .submit()
                .map_err(|e| SinkError::WriteFailed(e.to_string()))?;

            // If too many pending, wait for some to complete
            if self.pending_writes >= self.max_pending {
                self.wait_for_pending()?;
            }

            Ok(())
        }
    }

    impl Sink for IoUringSink {
        fn write(&mut self, outputs: &mut Vec<Output>) -> Result<(), SinkError> {
            for output in outputs.iter() {
                self.write_output(output)?;
            }
            outputs.clear();
            Ok(())
        }

        fn flush(&mut self) -> Result<(), SinkError> {
            // Wait for all pending writes
            self.wait_for_pending()?;

            // Submit fdatasync
            let fd = self.file.as_raw_fd();
            self.ring_manager
                .submit_sync(fd, true)
                .map_err(|e| SinkError::FlushFailed(e.to_string()))?;

            // Wait for sync to complete
            self.ring_manager
                .submit_and_wait(1)
                .map_err(|e| SinkError::FlushFailed(e.to_string()))?;

            let completions = self.ring_manager.poll_completions();
            for completion in completions {
                if !completion.is_success() {
                    return Err(SinkError::FlushFailed(format!(
                        "fdatasync failed: {:?}",
                        completion.error()
                    )));
                }
            }

            Ok(())
        }
    }

    impl Drop for IoUringSink {
        fn drop(&mut self) {
            // Try to flush any remaining writes
            let _ = self.flush();
        }
    }
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use linux_impl::IoUringSink;

/// Check if `io_uring` sink is available on this platform.
#[allow(dead_code)]
#[must_use]
pub fn is_sink_available() -> bool {
    cfg!(all(target_os = "linux", feature = "io-uring"))
}
