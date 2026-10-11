//! Durable Delta source cursor: table incarnation plus the next unread replay unit.

use crate::checkpoint::SourceCheckpoint;
use crate::error::ConnectorError;

const FORMAT_KEY: &str = "delta_cursor";
const FORMAT_VERSION: &str = "2";
const TABLE_ID_KEY: &str = "table_id";
const PHASE_KEY: &str = "phase";
const SNAPSHOT_VERSION_KEY: &str = "snapshot_version";
const FILES_DONE_KEY: &str = "files_done";
const LAST_FILE_KEY: &str = "last_file";
const NEXT_VERSION_KEY: &str = "next_version";

/// Position between replay units. A cursor never points inside a commit or a snapshot file:
/// state captured with it contains every earlier unit completely and nothing of later ones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum DeltaPhase {
    /// Emitting the files of the snapshot pinned at `version`, sorted by path; the first
    /// `files_done` are complete and the last of them was `last_file`.
    Snapshot {
        version: u64,
        files_done: u64,
        last_file: Option<String>,
    },
    /// Every commit before `next_version` has been emitted.
    Changes { next_version: u64 },
    /// A `read.mode = 'snapshot'` source emitted its whole snapshot at `version`.
    Complete { version: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DeltaCursor {
    /// Delta `metaData.id`; a table recreated at the same location has a different one.
    pub(super) table_id: String,
    pub(super) phase: DeltaPhase,
}

impl DeltaCursor {
    pub(super) fn to_checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset(FORMAT_KEY, FORMAT_VERSION);
        checkpoint.set_offset(TABLE_ID_KEY, self.table_id.clone());
        match &self.phase {
            DeltaPhase::Snapshot {
                version,
                files_done,
                last_file,
            } => {
                checkpoint.set_offset(PHASE_KEY, "snapshot");
                checkpoint.set_offset(SNAPSHOT_VERSION_KEY, version.to_string());
                checkpoint.set_offset(FILES_DONE_KEY, files_done.to_string());
                if let Some(path) = last_file {
                    checkpoint.set_offset(LAST_FILE_KEY, path.clone());
                }
            }
            DeltaPhase::Changes { next_version } => {
                checkpoint.set_offset(PHASE_KEY, "changes");
                checkpoint.set_offset(NEXT_VERSION_KEY, next_version.to_string());
            }
            DeltaPhase::Complete { version } => {
                checkpoint.set_offset(PHASE_KEY, "complete");
                checkpoint.set_offset(SNAPSHOT_VERSION_KEY, version.to_string());
            }
        }
        checkpoint
    }

    pub(super) fn from_checkpoint(checkpoint: &SourceCheckpoint) -> Result<Self, ConnectorError> {
        let invalid = |reason: &str| {
            ConnectorError::ConfigurationError(format!(
                "Delta source checkpoint is not a valid cursor ({reason}); reset the source to \
                 re-read the table"
            ))
        };
        if checkpoint.get_offset(FORMAT_KEY) != Some(FORMAT_VERSION) {
            return Err(invalid("unsupported or missing cursor format"));
        }
        let table_id = checkpoint
            .get_offset(TABLE_ID_KEY)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| invalid("missing table id"))?
            .to_string();
        let number = |key: &str| {
            checkpoint
                .get_offset(key)
                .ok_or_else(|| invalid(&format!("missing {key}")))?
                .parse::<u64>()
                .map_err(|_| invalid(&format!("malformed {key}")))
        };
        let phase = match checkpoint.get_offset(PHASE_KEY) {
            Some("snapshot") => {
                let files_done = number(FILES_DONE_KEY)?;
                let last_file = checkpoint.get_offset(LAST_FILE_KEY).map(str::to_string);
                if (files_done == 0) != last_file.is_none() {
                    return Err(invalid("snapshot progress and last file disagree"));
                }
                DeltaPhase::Snapshot {
                    version: number(SNAPSHOT_VERSION_KEY)?,
                    files_done,
                    last_file,
                }
            }
            Some("changes") => DeltaPhase::Changes {
                next_version: number(NEXT_VERSION_KEY)?,
            },
            Some("complete") => DeltaPhase::Complete {
                version: number(SNAPSHOT_VERSION_KEY)?,
            },
            _ => return Err(invalid("unknown phase")),
        };
        Ok(Self { table_id, phase })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_phase_round_trips() {
        for phase in [
            DeltaPhase::Snapshot {
                version: 7,
                files_done: 0,
                last_file: None,
            },
            DeltaPhase::Snapshot {
                version: 7,
                files_done: 3,
                last_file: Some("part=a/f.parquet".into()),
            },
            DeltaPhase::Changes { next_version: 0 },
            DeltaPhase::Changes {
                next_version: u64::MAX,
            },
            DeltaPhase::Complete { version: 9 },
        ] {
            let cursor = DeltaCursor {
                table_id: "4c1a".into(),
                phase,
            };
            assert_eq!(
                DeltaCursor::from_checkpoint(&cursor.to_checkpoint()).unwrap(),
                cursor
            );
        }
    }

    #[test]
    fn legacy_and_malformed_cursors_fail_closed() {
        let mut legacy = SourceCheckpoint::new();
        legacy.set_offset("delta_version", "42");
        assert!(DeltaCursor::from_checkpoint(&legacy).is_err());

        let valid = DeltaCursor {
            table_id: "t".into(),
            phase: DeltaPhase::Changes { next_version: 4 },
        }
        .to_checkpoint();
        for (key, value) in [
            (NEXT_VERSION_KEY, "-1"),
            (NEXT_VERSION_KEY, "x"),
            (PHASE_KEY, "draining"),
            (TABLE_ID_KEY, ""),
            (FORMAT_KEY, "1"),
        ] {
            let mut corrupted = valid.clone();
            corrupted.set_offset(key, value);
            assert!(
                DeltaCursor::from_checkpoint(&corrupted).is_err(),
                "{key}={value}"
            );
        }

        let mut inconsistent = DeltaCursor {
            table_id: "t".into(),
            phase: DeltaPhase::Snapshot {
                version: 1,
                files_done: 2,
                last_file: Some("f".into()),
            },
        }
        .to_checkpoint();
        inconsistent.set_offset(FILES_DONE_KEY, "0");
        assert!(DeltaCursor::from_checkpoint(&inconsistent).is_err());
    }
}
