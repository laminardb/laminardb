//! Crash-durable same-directory file publication.
//!
//! Unix durability requires syncing directory metadata after publication.
//! Windows directory handles do not provide the same portable contract, so
//! publication uses `MoveFileExW(MOVEFILE_WRITE_THROUGH)` instead. Unsupported
//! platforms fail closed rather than claiming durable output after a no-op.

use std::io;
use std::path::Path;

/// Whether publication may replace an existing destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableRenameMode {
    /// Refuse to replace an existing destination.
    NoReplace,
    /// Atomically replace an existing destination when present.
    Replace,
}

/// Publishes a synced temporary file under a same-directory destination and
/// does not return until the rename metadata is durably flushed.
///
/// # Errors
///
/// Returns an I/O error when paths are not in the same directory, publication
/// or durability fails, the destination exists in [`DurableRenameMode::NoReplace`]
/// mode, or the target platform has no proven implementation.
pub fn durable_rename(
    source: &Path,
    destination: &Path,
    mode: DurableRenameMode,
) -> io::Result<()> {
    let source_parent = source.parent().unwrap_or_else(|| Path::new("."));
    let destination_parent = destination.parent().unwrap_or_else(|| Path::new("."));
    if source_parent != destination_parent {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "durable rename requires source and destination in the same directory",
        ));
    }

    durable_rename_platform(source, destination, destination_parent, mode)
}

#[cfg(unix)]
fn durable_rename_platform(
    source: &Path,
    destination: &Path,
    parent: &Path,
    mode: DurableRenameMode,
) -> io::Result<()> {
    match mode {
        DurableRenameMode::Replace => {
            std::fs::rename(source, destination)?;
            sync_directory(parent)
        }
        DurableRenameMode::NoReplace => {
            // `hard_link` atomically creates the destination or fails with
            // AlreadyExists. Sync the new name before removing the temporary
            // name, then sync that removal. A crash at either boundary leaves
            // at least one complete name for recovery.
            std::fs::hard_link(source, destination)?;
            sync_directory(parent)?;
            std::fs::remove_file(source)?;
            sync_directory(parent)
        }
    }
}

#[cfg(unix)]
fn sync_directory(parent: &Path) -> io::Result<()> {
    std::fs::File::open(parent)?.sync_all()
}

#[cfg(windows)]
fn durable_rename_platform(
    source: &Path,
    destination: &Path,
    _parent: &Path,
    mode: DurableRenameMode,
) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };

    let source_wide: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
    let destination_wide: Vec<u16> = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect();
    let mut flags = MOVEFILE_WRITE_THROUGH;
    if mode == DurableRenameMode::Replace {
        flags |= MOVEFILE_REPLACE_EXISTING;
    }

    // SAFETY: both buffers are NUL-terminated and remain alive for the call.
    let moved = unsafe { MoveFileExW(source_wide.as_ptr(), destination_wide.as_ptr(), flags) };
    if moved == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(any(unix, windows)))]
fn durable_rename_platform(
    _source: &Path,
    _destination: &Path,
    _parent: &Path,
    _mode: DurableRenameMode,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "no proven crash-durable rename primitive for this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_replace_publishes_and_refuses_existing_destination() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.tmp");
        let destination = directory.path().join("final");
        std::fs::write(&source, b"first").unwrap();
        durable_rename(&source, &destination, DurableRenameMode::NoReplace).unwrap();
        assert_eq!(std::fs::read(&destination).unwrap(), b"first");
        assert!(!source.exists());

        std::fs::write(&source, b"second").unwrap();
        assert!(durable_rename(&source, &destination, DurableRenameMode::NoReplace).is_err());
        assert_eq!(std::fs::read(&destination).unwrap(), b"first");
        assert!(source.exists());
    }

    #[test]
    fn replace_publishes_new_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.tmp");
        let destination = directory.path().join("final");
        std::fs::write(&destination, b"old").unwrap();
        std::fs::write(&source, b"new").unwrap();
        durable_rename(&source, &destination, DurableRenameMode::Replace).unwrap();
        assert_eq!(std::fs::read(&destination).unwrap(), b"new");
        assert!(!source.exists());
    }
}
