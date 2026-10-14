//! Dictionary artifacts: bounded reads, content digests, and durable pinning.
//!
//! The session and the projection always load the pinned copy whose SHA-256 the schema binding
//! recorded at CREATE SOURCE, so a later edit of the configured file cannot change how journaled
//! messages are interpreted.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use laminar_core::durable_fs::{durable_rename, ensure_durable_directory, DurableRenameMode};
use sha2::{Digest, Sha256};

use super::config::{hex, MAX_DICTIONARY_BYTES};
use crate::error::ConnectorError;

/// Read a dictionary file of at most `MAX_DICTIONARY_BYTES` and return it with its SHA-256.
pub(crate) fn read(path: &Path) -> Result<(Vec<u8>, String), ConnectorError> {
    let file = std::fs::File::open(path).map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "read FIX dictionary '{}': {error}",
            path.display()
        ))
    })?;
    let mut bytes = Vec::new();
    file.take(MAX_DICTIONARY_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_DICTIONARY_BYTES {
        return Err(ConnectorError::ConfigurationError(format!(
            "FIX dictionary '{}' exceeds {MAX_DICTIONARY_BYTES} bytes",
            path.display()
        )));
    }
    let digest = hex(&Sha256::digest(&bytes));
    Ok((bytes, digest))
}

/// Return the pinned copy of the dictionary with `sha256`, creating it from `source` when
/// absent. Fails when the configured file changed since the digest was recorded.
pub(crate) fn pin(
    journal_root: &Path,
    sha256: &str,
    source: &Path,
) -> Result<PathBuf, ConnectorError> {
    let directory = journal_root.join("dictionaries");
    ensure_durable_directory(&directory)?;
    let pinned = directory.join(format!("{sha256}.xml"));
    if pinned.exists() {
        let (_, digest) = read(&pinned)?;
        if digest != sha256 {
            return Err(ConnectorError::ConfigurationError(format!(
                "pinned FIX dictionary '{}' does not match its digest",
                pinned.display()
            )));
        }
        return Ok(pinned);
    }
    let (bytes, digest) = read(source)?;
    if digest != sha256 {
        return Err(ConnectorError::ConfigurationError(format!(
            "FIX dictionary '{}' changed since the source was created (expected SHA-256 {sha256}, \
             found {digest}); restore the original file or recreate the source",
            source.display()
        )));
    }
    let temp = directory.join(format!("{sha256}.xml.tmp"));
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    durable_rename(&temp, &pinned, DurableRenameMode::NoReplace)?;
    Ok(pinned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinned_copy_outlives_edits_of_the_configured_file() {
        let dir = tempfile::tempdir().expect("dir");
        let source = dir.path().join("venue.xml");
        std::fs::write(&source, b"<fix/>").expect("write");
        let (_, digest) = read(&source).expect("read");
        let pinned = pin(&dir.path().join("journal"), &digest, &source).expect("pin");
        std::fs::write(&source, b"<fix>changed</fix>").expect("edit");
        assert_eq!(
            pin(&dir.path().join("journal"), &digest, &source).expect("reuse"),
            pinned
        );
        assert_eq!(std::fs::read(&pinned).expect("pinned"), b"<fix/>");
        let other = dir.path().join("other-journal");
        assert!(
            pin(&other, &digest, &source).is_err(),
            "edited source no longer matches"
        );
    }
}
