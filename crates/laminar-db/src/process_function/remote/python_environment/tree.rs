use std::fs::{File, Metadata};
use std::io::Read;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::error::DbError;
use crate::process_function::descriptor::valid_relative_python_path;

// Packaging and launch share a total budget across all declared trees, including overlap.
pub(super) struct InventoryBudget {
    entries_left: usize,
    bytes_left: u64,
}

impl Default for InventoryBudget {
    fn default() -> Self {
        Self {
            entries_left: 32_768,
            bytes_left: 4 * 1024 * 1024 * 1024,
        }
    }
}

struct Entry {
    path: PathBuf,
    relative: String,
    directory: bool,
    bytes: u64,
}

pub(super) fn canonical_directory(path: &Path) -> Result<PathBuf, DbError> {
    let metadata = regular_metadata(path)?;
    if !metadata.is_dir() {
        return Err(DbError::Config(format!(
            "Python tree root is not a directory: {}",
            path.display()
        )));
    }
    path.canonicalize()
        .map_err(|error| inventory_error(path, &error))
}

pub(super) fn regular_metadata(path: &Path) -> Result<Metadata, DbError> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(|error| inventory_error(path, &error))?;
    #[cfg(windows)]
    let linked = {
        use std::os::windows::fs::MetadataExt;
        // Windows junctions and other reparse points must not escape the declared tree.
        metadata.file_attributes() & 0x400 != 0
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

pub(super) fn fingerprint(root: &Path, budget: &mut InventoryBudget) -> Result<String, DbError> {
    let entries = inventory(root, budget)?;
    let mut digest = Sha256::new();
    digest.update(b"laminardb-python-tree-v1\0");
    for entry in entries {
        let length = u32::try_from(entry.relative.len())
            .map_err(|_| DbError::Config("Python tree path is too long".into()))?;
        digest.update(if entry.directory { b"D" } else { b"F" });
        digest.update(length.to_be_bytes());
        digest.update(entry.relative.as_bytes());
        if entry.directory {
            continue;
        }
        digest.update(entry.bytes.to_be_bytes());
        digest.update(file_digest(&entry.path, entry.bytes)?);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn inventory(root: &Path, budget: &mut InventoryBudget) -> Result<Vec<Entry>, DbError> {
    let mut directories = vec![(root.to_path_buf(), String::new())];
    let mut entries = Vec::new();
    // Every discovered entry consumes the shared budget before another directory is queued.
    while let Some((directory, prefix)) = directories.pop() {
        let children =
            std::fs::read_dir(&directory).map_err(|error| inventory_error(&directory, &error))?;
        for child in children {
            let child = child.map_err(|error| inventory_error(&directory, &error))?;
            budget.entries_left = budget
                .entries_left
                .checked_sub(1)
                .ok_or_else(|| DbError::Config("Python inventory exceeds 32768 entries".into()))?;
            let name = child.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| DbError::Config("Python tree paths must be UTF-8".into()))?;
            let relative = if prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{prefix}/{name}")
            };
            if !valid_relative_python_path(&relative) {
                return Err(DbError::Config("unsupported Python tree path".into()));
            }
            let path = child.path();
            let metadata = regular_metadata(&path)?;
            let directory = metadata.is_dir();
            let bytes = if directory { 0 } else { metadata.len() };
            if bytes > 512 * 1024 * 1024 {
                return Err(DbError::Config(
                    "Python inventory file exceeds 512 MiB".into(),
                ));
            }
            budget.bytes_left = budget
                .bytes_left
                .checked_sub(bytes)
                .ok_or_else(|| DbError::Config("Python inventory exceeds 4 GiB".into()))?;
            if directory {
                directories.push((path.clone(), relative.clone()));
            }
            entries.push(Entry {
                path,
                relative,
                directory,
                bytes,
            });
        }
    }
    entries.sort_unstable_by(|left, right| left.relative.cmp(&right.relative));
    Ok(entries)
}

pub(crate) fn file_sha256(path: &Path) -> Result<String, DbError> {
    let metadata = regular_metadata(path)?;
    if !metadata.is_file() || metadata.len() > 512 * 1024 * 1024 {
        return Err(DbError::Config(
            "Python handler must be a regular file of at most 512 MiB".into(),
        ));
    }
    Ok(format!("{:x}", file_digest(path, metadata.len())?))
}

fn file_digest(path: &Path, expected_bytes: u64) -> Result<sha2::digest::Output<Sha256>, DbError> {
    let file = File::open(path).map_err(|error| inventory_error(path, &error))?;
    let mut file = file.take(expected_bytes + 1);
    let mut digest = Sha256::new();
    // The length captured by inventory bounds this read even if another process grows the file.
    let bytes =
        std::io::copy(&mut file, &mut digest).map_err(|error| inventory_error(path, &error))?;
    if bytes != expected_bytes {
        return Err(DbError::Config(format!(
            "Python file changed during inventory: {}",
            path.display()
        )));
    }
    Ok(digest.finalize())
}

fn inventory_error(path: &Path, error: &std::io::Error) -> DbError {
    DbError::Config(format!(
        "read Python inventory '{}': {error}",
        path.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tree_digest_binds_names_contents_bytecode_and_empty_directories() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("a.py"), b"VALUE = 1\n").unwrap();
        std::fs::create_dir(root.path().join("empty")).unwrap();
        let first = fingerprint(root.path(), &mut InventoryBudget::default()).unwrap();
        // V1 pins path/type framing, byte order and the source-file digest across platforms.
        assert_eq!(
            first,
            "286457705a6926a0a7027895d954559e0287140fecda7cbf3f1eb6cd045fb7e3"
        );
        let moved = tempfile::tempdir().unwrap();
        std::fs::create_dir(moved.path().join("empty")).unwrap();
        std::fs::write(moved.path().join("a.py"), b"VALUE = 1\n").unwrap();
        assert_eq!(
            first,
            fingerprint(moved.path(), &mut InventoryBudget::default()).unwrap()
        );
        std::fs::write(root.path().join("a.py"), b"VALUE = 2\n").unwrap();
        assert_ne!(
            first,
            fingerprint(root.path(), &mut InventoryBudget::default()).unwrap()
        );
        std::fs::write(root.path().join("a.py"), b"VALUE = 1\n").unwrap();
        std::fs::rename(root.path().join("a.py"), root.path().join("b.py")).unwrap();
        assert_ne!(
            first,
            fingerprint(root.path(), &mut InventoryBudget::default()).unwrap()
        );
        std::fs::rename(root.path().join("b.py"), root.path().join("a.py")).unwrap();
        std::fs::write(root.path().join("a.pyc"), b"cached bytecode").unwrap();
        assert_ne!(
            first,
            fingerprint(root.path(), &mut InventoryBudget::default()).unwrap()
        );
        std::fs::remove_file(root.path().join("a.pyc")).unwrap();
        std::fs::remove_dir(root.path().join("empty")).unwrap();
        assert_ne!(
            first,
            fingerprint(root.path(), &mut InventoryBudget::default()).unwrap()
        );
    }

    #[test]
    fn inventory_enforces_entry_byte_and_file_limits() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file"), b"123").unwrap();
        let mut budget = InventoryBudget {
            entries_left: 0,
            bytes_left: 10,
        };
        assert!(fingerprint(root.path(), &mut budget)
            .unwrap_err()
            .to_string()
            .contains("entries"));
        let mut budget = InventoryBudget {
            entries_left: 10,
            bytes_left: 2,
        };
        assert!(fingerprint(root.path(), &mut budget)
            .unwrap_err()
            .to_string()
            .contains("4 GiB"));
        assert!(file_digest(&root.path().join("file"), 2)
            .unwrap_err()
            .to_string()
            .contains("changed"));
        assert!(file_digest(&root.path().join("file"), 4)
            .unwrap_err()
            .to_string()
            .contains("changed"));
        File::create(root.path().join("oversized"))
            .unwrap()
            .set_len(512 * 1024 * 1024 + 1)
            .unwrap();
        assert!(file_sha256(&root.path().join("oversized"))
            .unwrap_err()
            .to_string()
            .contains("512 MiB"));
        assert!(fingerprint(root.path(), &mut InventoryBudget::default())
            .unwrap_err()
            .to_string()
            .contains("512 MiB"));
    }

    #[cfg(unix)]
    #[test]
    fn inventory_rejects_symlinks() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("escape")).unwrap();
        assert!(fingerprint(root.path(), &mut InventoryBudget::default())
            .unwrap_err()
            .to_string()
            .contains("unsupported"));
        assert!(canonical_directory(&root.path().join("escape")).is_err());
    }

    #[cfg(windows)]
    #[test]
    fn inventory_rejects_windows_junctions() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let link = root.path().join("escape");
        let result = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(&link)
            .arg(outside.path())
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(fingerprint(root.path(), &mut InventoryBudget::default())
            .unwrap_err()
            .to_string()
            .contains("unsupported"));
        assert!(canonical_directory(&link).is_err());
    }
}
