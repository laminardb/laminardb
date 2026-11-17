//! The file source's checkpoint: the manifest of processed files and the cursor into a partly
//! emitted file.

use crate::checkpoint::SourceCheckpoint;
use crate::connector::SourcePosition;
use crate::error::ConnectorError;

use super::config::local_files_path;
use super::manifest::FileIngestionManifest;

pub(super) const FILE_CHECKPOINT_CONNECTOR: &str = "files";
pub(super) const CHECKPOINT_VERSION_METADATA: &str = "checkpoint.version";
pub(super) const FILE_CHECKPOINT_VERSION: &str = "1";
const FILE_PROGRESS_OFFSET: &str = "file_progress";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileProgress {
    pub(super) path: String,
    pub(super) size: u64,
    pub(super) modified_ms: u64,
    pub(super) content_sha256: String,
    pub(super) next_row: u64,
}

impl FileProgress {
    pub(super) fn encode(&self) -> String {
        serde_json::to_string(self)
            .expect("file progress contains only infallibly serializable fields")
    }
}

/// An empty files checkpoint of `version`, bound to the reader's schema contract.
pub(super) fn file_checkpoint(version: &str, schema_contract: Option<&str>) -> SourceCheckpoint {
    let mut checkpoint = SourceCheckpoint::new();
    checkpoint.set_metadata("connector", FILE_CHECKPOINT_CONNECTOR);
    checkpoint.set_metadata(CHECKPOINT_VERSION_METADATA, version);
    if let Some(binding) = schema_contract {
        checkpoint.set_metadata("schema.contract", binding);
    }
    checkpoint
}

/// Refuse file progress whose path is not an object key.
pub(super) fn validate_object_key(progress: &FileProgress) -> Result<(), ConnectorError> {
    object_store::path::Path::parse(&progress.path)
        .map(|_| ())
        .map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "invalid object key in file progress: {error}"
            ))
        })
}

/// The checkpoint of the processed files and the file in flight.
pub(super) fn ingestion_checkpoint(
    manifest: &FileIngestionManifest,
    progress: Option<&FileProgress>,
    schema_contract: Option<&str>,
) -> SourceCheckpoint {
    let mut checkpoint = file_checkpoint(FILE_CHECKPOINT_VERSION, schema_contract);
    manifest.to_checkpoint(&mut checkpoint);
    if let Some(progress) = progress {
        checkpoint.set_offset(FILE_PROGRESS_OFFSET, progress.encode());
    }
    checkpoint
}

/// The processed paths and in-flight file a start resumes from. A remote source records object
/// keys relative to its directory; a local one records local paths.
pub(super) fn restore_ingestion_position(
    position: SourcePosition,
    is_remote: bool,
) -> Result<(FileIngestionManifest, Option<FileProgress>), ConnectorError> {
    Ok(match position {
        SourcePosition::Initial => (FileIngestionManifest::new(), None),
        SourcePosition::Initialized { .. } => {
            return Err(ConnectorError::ConfigurationError(
                "file source has no sealed topology startup contract".into(),
            ));
        }
        SourcePosition::Resume {
            attempt,
            checkpoint,
        } => {
            validate_file_checkpoint(&checkpoint, FILE_CHECKPOINT_VERSION)?;
            if checkpoint.get_offset("manifest").is_none() {
                return Err(ConnectorError::ConfigurationError(format!(
                    "file checkpoint {attempt:?} is missing required manifest state"
                )));
            }
            let manifest = FileIngestionManifest::from_checkpoint(&checkpoint).map_err(|e| {
                ConnectorError::ConfigurationError(format!(
                    "invalid file manifest in checkpoint {attempt:?}: {e}"
                ))
            })?;
            let progress = checkpoint
                .get_offset(FILE_PROGRESS_OFFSET)
                .map(serde_json::from_str::<FileProgress>)
                .transpose()
                .map_err(|e| {
                    ConnectorError::ConfigurationError(format!(
                        "invalid file progress in checkpoint {attempt:?}: {e}"
                    ))
                })?
                .map(|mut progress| {
                    if is_remote {
                        validate_object_key(&progress)?;
                    } else {
                        progress.path = local_files_path(&progress.path)?;
                    }
                    Ok::<_, ConnectorError>(progress)
                })
                .transpose()?;
            if progress
                .as_ref()
                .is_some_and(|p| manifest.contains(&p.path) || p.next_row == 0)
            {
                return Err(ConnectorError::ConfigurationError(format!(
                    "file checkpoint {attempt:?} contains contradictory progress"
                )));
            }
            (manifest, progress)
        }
    })
}

/// Refuse a checkpoint of another connector or of another files checkpoint `version`: an unsplit
/// source and a source with `splits` cannot resume from each other.
pub(super) fn validate_file_checkpoint(
    checkpoint: &SourceCheckpoint,
    version: &str,
) -> Result<(), ConnectorError> {
    match checkpoint.get_metadata("connector") {
        Some(FILE_CHECKPOINT_CONNECTOR) => {}
        Some(connector) => {
            return Err(ConnectorError::ConfigurationError(format!(
                "file checkpoint belongs to connector '{connector}'"
            )));
        }
        None => {
            return Err(ConnectorError::ConfigurationError(
                "file checkpoint is missing connector identity".into(),
            ));
        }
    }
    if checkpoint.get_metadata(CHECKPOINT_VERSION_METADATA) != Some(version) {
        return Err(ConnectorError::ConfigurationError(format!(
            "file checkpoint requires {CHECKPOINT_VERSION_METADATA}={version}"
        )));
    }
    Ok(())
}
