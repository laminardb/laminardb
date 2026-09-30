use std::fs::{File, Metadata, OpenOptions};
use std::path::{Path, PathBuf};

use crate::error::DbError;

/// On Windows, retains read-share handles to existing entries and configured path ancestors.
/// This does not prevent directory additions or survive termination of the host.
#[derive(Default)]
pub(in crate::process_function::remote) struct FileGuards {
    #[cfg(windows)]
    files: Vec<File>,
}

impl FileGuards {
    pub(in crate::process_function::remote) fn canonical_file(
        &mut self,
        path: &Path,
    ) -> Result<PathBuf, DbError> {
        let path = self.retain_ancestors(path)?;
        self.retain(Self::open_file(&path)?);
        path.canonicalize().map_err(|error| {
            DbError::Config(format!(
                "resolve Python inventory '{}': {error}",
                path.display()
            ))
        })
    }

    pub(super) fn retain_ancestors(&mut self, path: &Path) -> Result<PathBuf, DbError> {
        let path = std::path::absolute(path).map_err(|error| {
            DbError::Config(format!(
                "resolve Python inventory '{}': {error}",
                path.display()
            ))
        })?;
        let ancestors: Vec<_> = path.ancestors().skip(1).take(129).collect();
        if ancestors.len() > 128 {
            return Err(DbError::Config("Python path exceeds 128 ancestors".into()));
        }
        // Validate before canonicalization erases links. On Windows, acquire from the root
        // downward to exclude parent rename/deletion while opening its children.
        for ancestor in ancestors.into_iter().rev() {
            self.retain_directory(ancestor)?;
        }
        Ok(path)
    }

    pub(in crate::process_function::remote) fn open_file(path: &Path) -> Result<File, DbError> {
        let file = open(path)?;
        if !regular_metadata(path, file.metadata())?.is_file() {
            return Err(DbError::Config(format!(
                "Python inventory entry is not a file: {}",
                path.display()
            )));
        }
        Ok(file)
    }

    pub(in crate::process_function::remote) fn retain(&mut self, file: File) {
        #[cfg(windows)]
        self.files.push(file);
        #[cfg(not(windows))]
        drop(file);
    }

    pub(super) fn retain_directory(&mut self, path: &Path) -> Result<(), DbError> {
        #[cfg(windows)]
        {
            let file = open(path)?;
            if !regular_metadata(path, file.metadata())?.is_dir() {
                return Err(DbError::Config(format!(
                    "Python inventory entry is not a directory: {}",
                    path.display()
                )));
            }
            self.retain(file);
        }
        #[cfg(not(windows))]
        if !regular_metadata(path, std::fs::symlink_metadata(path))?.is_dir() {
            return Err(DbError::Config(format!(
                "Python inventory entry is not a directory: {}",
                path.display()
            )));
        }
        Ok(())
    }
}

fn open(path: &Path) -> Result<File, DbError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ,
        };

        // Deny existing writers and later write/delete opens; allow Python and native loaders
        // to read. Inspect a reparse point itself instead of following a raced replacement.
        options
            .share_mode(FILE_SHARE_READ)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
    }
    options.open(path).map_err(|error| {
        DbError::Config(format!(
            "open Python inventory '{}': {error}",
            path.display()
        ))
    })
}

pub(super) fn regular_metadata(
    path: &Path,
    metadata: std::io::Result<Metadata>,
) -> Result<Metadata, DbError> {
    let metadata = metadata.map_err(|error| {
        DbError::Config(format!(
            "read Python inventory '{}': {error}",
            path.display()
        ))
    })?;
    #[cfg(windows)]
    let linked = {
        use std::os::windows::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

        metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    };
    #[cfg(not(windows))]
    let linked = metadata.file_type().is_symlink();
    if linked || (!metadata.is_file() && !metadata.is_dir()) {
        return Err(DbError::Config(format!(
            "unsupported Python tree entry: {}",
            path.display()
        )));
    }
    Ok(metadata)
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[test]
    fn guarded_file_retains_ancestors_through_release() {
        let root = tempfile::tempdir().unwrap();
        let deployment = root.path().join("deployment");
        let imports = deployment.join("imports");
        std::fs::create_dir_all(&imports).unwrap();
        let file = imports.join("module.py");
        std::fs::write(&file, b"original").unwrap();
        let mut guards = FileGuards::default();
        assert_eq!(
            guards.canonical_file(&file).unwrap(),
            file.canonicalize().unwrap()
        );
        let moved = root.path().join("moved");
        assert!(std::fs::rename(&deployment, &moved).is_err());
        drop(guards);
        std::fs::rename(&deployment, &moved).unwrap();
    }

    #[test]
    fn path_depth_is_bounded_before_filesystem_access() {
        let root = tempfile::tempdir().unwrap();
        let mut path = root.path().to_path_buf();
        for _ in 0..129 {
            path.push("directory");
        }
        assert!(FileGuards::default()
            .canonical_file(&path.join("module.py"))
            .unwrap_err()
            .to_string()
            .contains("128 ancestors"));
    }

    #[test]
    fn read_sharing_blocks_existing_and_future_writers() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("module.py");
        std::fs::write(&path, b"original").unwrap();
        let writer = OpenOptions::new().write(true).open(&path).unwrap();
        assert!(FileGuards::open_file(&path)
            .unwrap_err()
            .to_string()
            .contains("os error 32"));
        drop(writer);

        let mut guards = FileGuards::default();
        guards.retain(FileGuards::open_file(&path).unwrap());
        assert_eq!(std::fs::read(&path).unwrap(), b"original");
        assert_eq!(
            std::fs::write(&path, b"changed")
                .unwrap_err()
                .raw_os_error(),
            Some(32)
        );
        assert!(std::fs::remove_file(&path).is_err());
        assert!(std::fs::rename(&path, root.path().join("moved.py")).is_err());
        // Sharing does not seal attributes; bytecode selection needs a separate launch policy.
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_WRITE_ATTRIBUTES;
        let attributes = OpenOptions::new()
            .access_mode(FILE_WRITE_ATTRIBUTES)
            .open(&path)
            .unwrap();
        let modified = std::time::UNIX_EPOCH + std::time::Duration::from_secs(86_400);
        attributes
            .set_times(std::fs::FileTimes::new().set_modified(modified))
            .unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().modified().unwrap(),
            modified
        );
        drop(attributes);
        drop(guards);
        std::fs::write(&path, b"changed").unwrap();
    }

    #[test]
    fn directory_guard_blocks_rename_but_does_not_seal_new_entries() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("imports");
        std::fs::create_dir(&directory).unwrap();
        let mut guards = FileGuards::default();
        guards.retain_directory(&directory).unwrap();
        assert!(std::fs::rename(&directory, root.path().join("moved")).is_err());
        // Keep this gap explicit: sharing restrictions do not govern child creation.
        std::fs::write(directory.join("new.py"), b"new import").unwrap();
        drop(guards);
        std::fs::rename(&directory, root.path().join("moved")).unwrap();
    }
}
