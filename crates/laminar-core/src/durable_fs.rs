//! Crash-durable same-directory file publication.
//!
//! Unix durability requires syncing directory metadata after publication.
//! Windows directory handles do not provide the same portable contract, so
//! publication uses `MoveFileExW(MOVEFILE_WRITE_THROUGH)` instead. Unsupported
//! platforms fail closed rather than claiming durable output after a no-op.

use std::io;
use std::path::Path;

/// Creates every missing directory component with a crash-durable parent
/// publication before returning.
///
/// Existing directories are re-synchronized on Unix. Windows treats them as a
/// pre-established namespace because it has no documented directory flush
/// primitive. A concurrent creator is accepted only after the winner is
/// verified to be a real directory.
///
/// # Errors
///
/// Returns an I/O error if a component is not a directory, the path escapes
/// through `..`, or the platform cannot durably publish a missing component.
pub fn ensure_durable_directory(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "durable directory path is empty",
        ));
    }
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };

    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            return establish_existing_directory(&path);
        }
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("{} exists and is not a directory", path.display()),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let mut ancestor = path.as_path();
    loop {
        ancestor = ancestor.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "durable directory has no existing ancestor",
            )
        })?;
        match std::fs::symlink_metadata(ancestor) {
            Ok(metadata) if metadata.file_type().is_dir() => break,
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("{} exists and is not a directory", ancestor.display()),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }

    let relative = path.strip_prefix(ancestor).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "durable directory is outside its existing ancestor",
        )
    })?;
    let mut current = ancestor.to_path_buf();
    for component in relative.components() {
        let std::path::Component::Normal(component) = component else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "durable directory contains a non-normal path component",
            ));
        };
        let destination = current.join(component);
        publish_directory_component(&current, &destination)?;
        current = destination;
    }
    Ok(())
}

#[cfg(unix)]
fn establish_existing_directory(path: &Path) -> io::Result<()> {
    sync_directory(path)?;
    if let Some(parent) = path.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

#[cfg(windows)]
#[allow(clippy::unnecessary_wraps)] // Shared caller is fallible on Unix.
fn establish_existing_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn establish_existing_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

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
fn publish_directory_component(parent: &Path, destination: &Path) -> io::Result<()> {
    match std::fs::create_dir(destination) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let metadata = std::fs::symlink_metadata(destination)?;
            if !metadata.file_type().is_dir() {
                return Err(error);
            }
        }
        Err(error) => return Err(error),
    }
    sync_directory(destination)?;
    sync_directory(parent)
}

#[cfg(windows)]
fn publish_directory_component(parent: &Path, destination: &Path) -> io::Result<()> {
    let temporary = loop {
        let candidate = parent.join(format!(
            ".laminardb-directory#{}",
            uuid::Uuid::new_v4().as_u128()
        ));
        match std::fs::create_dir(&candidate) {
            Ok(()) => break candidate,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    };
    let cleanup = TemporaryDirectory(temporary.clone());
    match durable_rename(&temporary, destination, DurableRenameMode::NoReplace) {
        Ok(()) => {}
        Err(error) => match std::fs::symlink_metadata(destination) {
            Ok(metadata) if metadata.file_type().is_dir() => {}
            _ => {
                return Err(error);
            }
        },
    }
    drop(cleanup);
    Ok(())
}

#[cfg(windows)]
struct TemporaryDirectory(std::path::PathBuf);

#[cfg(windows)]
impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir(&self.0);
    }
}

#[cfg(not(any(unix, windows)))]
fn publish_directory_component(_parent: &Path, _destination: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "no proven crash-durable directory publication primitive for this platform",
    ))
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
pub(crate) fn sync_directory(parent: &Path) -> io::Result<()> {
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

    #[test]
    fn creates_nested_durable_directories_and_is_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let nested = directory.path().join("one").join("two").join("three");
        ensure_durable_directory(&nested).unwrap();
        assert!(nested.is_dir());
        ensure_durable_directory(&nested).unwrap();
    }

    #[test]
    fn rejects_a_file_in_the_directory_path() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("file");
        std::fs::write(&file, b"not a directory").unwrap();
        let error = ensure_durable_directory(&file.join("child")).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    }

    #[cfg(windows)]
    #[test]
    fn directory_publication_accepts_a_competing_real_directory() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("winner");
        std::fs::create_dir(&destination).unwrap();

        publish_directory_component(directory.path(), &destination).unwrap();

        assert!(destination.is_dir());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }
}
