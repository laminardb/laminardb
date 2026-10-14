//! Dictionary artifacts: bounded reads, content digests, durable pinning, and version checks.
//!
//! The session and the projection always load the pinned copies whose SHA-256 digests the schema
//! binding recorded at CREATE SOURCE, so a later edit of a configured file cannot change how
//! journaled messages are interpreted.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use laminar_core::durable_fs::{durable_rename, ensure_durable_directory, DurableRenameMode};
use sha2::{Digest, Sha256};

use super::config::{hex, FixSourceConfig, FIXT_BEGIN_STRING, MAX_DICTIONARY_BYTES};
use super::engine::{DataDictionary, SessionDictionaries};
use crate::error::ConnectorError;

/// Version the engine reports for every FIX 5.0 application dictionary: it reads only the
/// `type`, `major`, and `minor` attributes, so service packs are indistinguishable.
const FIX50_DICTIONARY_VERSION: &str = "FIX.5.0";

/// A session's dictionary files: the application (or only) dictionary and, under FIXT.1.1, the
/// transport dictionary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DictionaryFiles {
    pub(crate) application: PathBuf,
    pub(crate) transport: Option<PathBuf>,
}

/// SHA-256 digests of a session's dictionary files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DictionaryDigests {
    pub(crate) application: String,
    pub(crate) transport: Option<String>,
}

/// Digest the configured dictionary files.
pub(crate) fn digests(files: &DictionaryFiles) -> Result<DictionaryDigests, ConnectorError> {
    Ok(DictionaryDigests {
        application: read(&files.application)?.1,
        transport: files
            .transport
            .as_deref()
            .map(|path| read(path).map(|(_, digest)| digest))
            .transpose()?,
    })
}

/// Pin every dictionary file under `journal_root`; see [`pin`].
pub(crate) fn pin_all(
    journal_root: &Path,
    digests: &DictionaryDigests,
    sources: &DictionaryFiles,
) -> Result<DictionaryFiles, ConnectorError> {
    let transport =
        match (&digests.transport, &sources.transport) {
            (Some(digest), Some(source)) => Some(pin(journal_root, digest, source)?),
            (None, None) => None,
            _ => return Err(ConnectorError::ConfigurationError(
                "the source's committed dictionaries do not match its FIX version; recreate the \
                 source"
                    .into(),
            )),
        };
    Ok(DictionaryFiles {
        application: pin(journal_root, &digests.application, &sources.application)?,
        transport,
    })
}

/// Load the session's dictionaries and check that their versions match the configured session.
pub(crate) fn load(
    config: &FixSourceConfig,
    files: &DictionaryFiles,
) -> Result<SessionDictionaries, ConnectorError> {
    let application = open(&files.application)?;
    let Some(transport_path) = &files.transport else {
        expect_version(
            &application,
            &files.application,
            &config.session.begin_string,
        )?;
        return Ok(SessionDictionaries::single(application));
    };
    let transport = open(transport_path)?;
    expect_version(&transport, transport_path, FIXT_BEGIN_STRING)?;
    expect_version(&application, &files.application, FIX50_DICTIONARY_VERSION)?;
    Ok(SessionDictionaries::fixt(transport, application))
}

fn open(path: &Path) -> Result<DataDictionary, ConnectorError> {
    Ok(DataDictionary::from_path(&path.display().to_string())?)
}

fn expect_version(
    dictionary: &DataDictionary,
    path: &Path,
    expected: &str,
) -> Result<(), ConnectorError> {
    let version = dictionary.version()?;
    if version != expected {
        return Err(ConnectorError::ConfigurationError(format!(
            "FIX dictionary '{}' describes {version}, but this session needs a {expected} \
             dictionary",
            path.display()
        )));
    }
    Ok(())
}

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
