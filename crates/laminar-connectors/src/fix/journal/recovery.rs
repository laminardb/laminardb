//! Journal start: metadata binding and segment recovery.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use laminar_core::durable_fs::{durable_rename, DurableRenameMode};

use super::record::corrupt;
use super::segment::{self, FrameRead, FrameReader, HEADER_LEN};
use super::{
    apply_progress, JournalMeta, JournalOptions, Record, SegmentInfo, SessionSnapshot, State,
    Writer, META_FILE, META_FORMAT,
};
use crate::error::ConnectorError;

pub(super) fn load_or_create_meta(
    root: &Path,
    session: &str,
    capture_contract: &str,
) -> Result<JournalMeta, ConnectorError> {
    let path = root.join(META_FILE);
    if path.exists() {
        let bytes = std::fs::read(&path)?;
        let meta: JournalMeta = serde_json::from_slice(&bytes)
            .map_err(|error| corrupt(&format!("journal metadata: {error}")))?;
        if meta.format != META_FORMAT {
            return Err(ConnectorError::ConfigurationError(format!(
                "FIX journal format {} is not supported by this build",
                meta.format
            )));
        }
        if meta.session != session || meta.capture_contract != capture_contract {
            return Err(ConnectorError::ConfigurationError(format!(
                "FIX journal '{}' belongs to session '{}' with a different capture contract; \
                 an incompatible profile change needs a new journal path (DROP and CREATE the \
                 source)",
                root.display(),
                meta.session
            )));
        }
        return Ok(meta);
    }
    let meta = JournalMeta {
        format: META_FORMAT,
        journal_id: uuid::Uuid::new_v4().simple().to_string(),
        session: session.to_string(),
        capture_contract: capture_contract.to_string(),
    };
    let bytes = serde_json::to_vec_pretty(&meta)
        .map_err(|error| ConnectorError::Internal(error.to_string()))?;
    let temp = root.join(format!("{META_FILE}.tmp"));
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    durable_rename(&temp, &path, DurableRenameMode::NoReplace)?;
    Ok(meta)
}

pub(super) fn parse_journal_id(text: &str) -> Result<[u8; 16], ConnectorError> {
    uuid::Uuid::parse_str(text)
        .map(|id| *id.as_bytes())
        .map_err(|_| corrupt("journal id is not a UUID"))
}

/// Rebuild writer state, progress, and the redelivery window; truncate a torn tail.
pub(super) fn recover(
    dir: &Path,
    journal_id: &[u8; 16],
    options: JournalOptions,
) -> Result<State, ConnectorError> {
    let indices = segment::list_segments(dir)?;
    if indices.is_empty() {
        let mut payload = Vec::new();
        let mut frame = Vec::new();
        Record::SessionSnapshot(SessionSnapshot::default()).encode_into(&mut payload)?;
        segment::encode_frame(&payload, &mut frame)?;
        let file = segment::create_segment(dir, 0, journal_id, &frame)?;
        let bytes = HEADER_LEN + frame.len() as u64;
        return Ok(State::new(
            Writer::new(file, 0, bytes, 1, payload, frame),
            SessionSnapshot::default(),
            VecDeque::from([SegmentInfo {
                first_index: 0,
                bytes,
            }]),
            Vec::new(),
        ));
    }

    let mut progress = SessionSnapshot::default();
    let mut redelivery = Vec::new();
    let mut segments = VecDeque::new();
    let mut payload = Vec::new();
    let mut next_index = indices[0];
    for (position, first_index) in indices.iter().copied().enumerate() {
        if first_index != next_index {
            return Err(corrupt(&format!(
                "segment {first_index} does not continue at record {next_index}"
            )));
        }
        let last = position + 1 == indices.len();
        let path = segment::segment_path(dir, first_index);
        let mut file = File::open(&path)?;
        segment::check_header(&mut file, first_index, journal_id)?;
        let length = file.metadata()?.len();
        let mut frames = FrameReader::new(file, HEADER_LEN, options.max_record_bytes)?;
        loop {
            match frames.next(length, &mut payload)? {
                FrameRead::Frame => {
                    let record = Record::decode(&payload)?;
                    track_redelivery(&mut redelivery, &progress, &record);
                    apply_progress(&mut progress, &record);
                    next_index += 1;
                }
                FrameRead::End => break,
                FrameRead::Torn if last => {
                    let valid = frames.offset();
                    tracing::warn!(
                        segment = first_index,
                        valid_bytes = valid,
                        torn_bytes = length - valid,
                        "truncating torn FIX journal tail left by an interrupted append"
                    );
                    let file = OpenOptions::new().write(true).open(&path)?;
                    file.set_len(valid)?;
                    file.sync_all()?;
                    break;
                }
                FrameRead::Torn | FrameRead::Corrupt => {
                    return Err(corrupt(&format!(
                        "segment {first_index} is damaged at byte {}",
                        frames.offset()
                    )));
                }
            }
        }
        segments.push_back(SegmentInfo {
            first_index,
            bytes: frames.offset(),
        });
    }
    let active = segments
        .back()
        .copied()
        .ok_or_else(|| corrupt("journal has no segments"))?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(segment::segment_path(dir, active.first_index))?;
    file.seek(SeekFrom::End(0))?;
    Ok(State::new(
        Writer::new(
            file,
            active.first_index,
            active.bytes,
            next_index,
            payload,
            Vec::new(),
        ),
        progress,
        segments,
        redelivery,
    ))
}

fn track_redelivery(window: &mut Vec<(u64, u64)>, progress: &SessionSnapshot, record: &Record) {
    match record {
        Record::SeriesStarted { .. } => window.clear(),
        Record::Application(record) if record.series == progress.series => {
            window.push((record.seq, record.content_digest));
        }
        _ => {}
    }
}
