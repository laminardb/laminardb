//! Node-local durable journal of accepted inbound FIX records.
//!
//! The journal is the only path from the session to the projection. Session callbacks append
//! records and return to the engine only after the record reached the configured durability
//! boundary, so the engine's inbound sequence number can never be ahead of the journal.
//! `poll_batch` reads records only up to that boundary, so a LaminarDB checkpoint never names a
//! journal position that a crash inside the declared failure model can lose. Segments wholly
//! before the latest committed source cursor are deleted.
//!
//! Any failed write or sync poisons the writer: the callback disconnects without advancing the
//! session, no later record is appended, and the next start reconciles the file by truncating a
//! torn tail. A failed `fsync` is never retried because its outcome is ambiguous.

mod record;
mod segment;

pub(crate) use record::{ApplicationRecord, Record, SessionSnapshot};

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use laminar_core::durable_fs::{durable_rename, ensure_durable_directory, DurableRenameMode};
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::error::ConnectorError;
use record::corrupt;
use segment::{FrameRead, FrameReader, FRAME_OVERHEAD, HEADER_LEN};

const META_FILE: &str = "journal.json";
const META_FORMAT: u32 = 1;
const LOCK_FILE: &str = ".laminardb-fix.lock";

/// Durability boundary a record reaches before the session acknowledges it to the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncPolicy {
    /// `fdatasync` per record: survives process, operating-system, and power failure of the
    /// host, given storage that honours flushes.
    Fsync,
    /// Written to the operating system: survives process failure only.
    OperatingSystem,
}

/// Journal sizing and durability options.
#[derive(Debug, Clone, Copy)]
pub(crate) struct JournalOptions {
    pub(crate) sync: SyncPolicy,
    pub(crate) segment_bytes: u64,
    /// Retained bytes above which the session stops accepting input.
    pub(crate) max_bytes: u64,
    /// Largest record payload.
    pub(crate) max_record_bytes: u64,
}

/// Identity a journal is bound to: the session and every artifact needed to interpret it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct JournalMeta {
    pub(crate) format: u32,
    /// Random identity of this journal instance; checkpoints bind to it.
    pub(crate) journal_id: String,
    pub(crate) session: String,
    /// SHA-256 of the capture contract (session, dictionary, message selection, validation).
    pub(crate) capture_contract: String,
}

/// A position in the journal: the index of the next record.
pub(crate) type RecordIndex = u64;

#[derive(Debug, Clone, Copy)]
struct SegmentInfo {
    first_index: RecordIndex,
    bytes: u64,
}

/// Published readable end of the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DurableEnd {
    pub(crate) next_index: RecordIndex,
    segment_first: RecordIndex,
    segment_bytes: u64,
}

struct Writer {
    file: File,
    segment_first: RecordIndex,
    segment_bytes: u64,
    next_index: RecordIndex,
    poisoned: Option<String>,
    payload: Vec<u8>,
    frame: Vec<u8>,
}

struct State {
    writer: Writer,
    progress: SessionSnapshot,
    segments: VecDeque<SegmentInfo>,
    retained_bytes: u64,
    /// `(seq, content_digest)` of current-series application records awaiting possible
    /// retransmission after a restart; consumed by `take_redelivery_window`.
    redelivery: Vec<(u64, u64)>,
}

/// An open journal. Holds an exclusive OS lock on its directory while alive.
pub(crate) struct Journal {
    segments_dir: PathBuf,
    id: [u8; 16],
    meta: JournalMeta,
    options: JournalOptions,
    state: Mutex<State>,
    end: Mutex<DurableEnd>,
    over_capacity: AtomicBool,
    data_ready: Arc<Notify>,
    _lock: File,
}

impl Journal {
    /// Open or create the journal under `root`, recovering a torn tail.
    ///
    /// # Errors
    /// Fails when another process owns the directory, when the directory belongs to a different
    /// session or capture contract, or when committed data is corrupt.
    pub(crate) fn open(
        root: &Path,
        session: &str,
        capture_contract: &str,
        options: JournalOptions,
        data_ready: Arc<Notify>,
    ) -> Result<Self, ConnectorError> {
        ensure_durable_directory(root)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join(LOCK_FILE))?;
        lock.try_lock().map_err(|_| {
            ConnectorError::ConfigurationError(format!(
                "FIX journal '{}' is already owned by another live source or process",
                root.display()
            ))
        })?;
        let meta = load_or_create_meta(root, session, capture_contract)?;
        let journal_id = parse_journal_id(&meta.journal_id)?;
        let segments_dir = root.join("segments");
        ensure_durable_directory(&segments_dir)?;
        let recovered = recover(&segments_dir, &journal_id, options)?;
        let end = DurableEnd {
            next_index: recovered.writer.next_index,
            segment_first: recovered.writer.segment_first,
            segment_bytes: recovered.writer.segment_bytes,
        };
        let journal = Self {
            segments_dir,
            id: journal_id,
            meta,
            options,
            over_capacity: AtomicBool::new(false),
            state: Mutex::new(recovered),
            end: Mutex::new(end),
            data_ready,
            _lock: lock,
        };
        journal.refresh_capacity(&journal.state.lock());
        Ok(journal)
    }

    pub(crate) fn meta(&self) -> &JournalMeta {
        &self.meta
    }

    pub(crate) fn sync_policy(&self) -> SyncPolicy {
        self.options.sync
    }

    /// Inbound session progress including every appended record.
    pub(crate) fn progress(&self) -> SessionSnapshot {
        self.state.lock().progress
    }

    /// Whether retained data exceeds `max_bytes`; input must pause until retention frees space.
    pub(crate) fn over_capacity(&self) -> bool {
        self.over_capacity.load(Ordering::Acquire)
    }

    /// Why the writer stopped accepting records, if it did.
    pub(crate) fn poisoned(&self) -> Option<String> {
        self.state.lock().writer.poisoned.clone()
    }

    pub(crate) fn retained_bytes(&self) -> u64 {
        self.state.lock().retained_bytes
    }

    pub(crate) fn durable_end(&self) -> DurableEnd {
        *self.end.lock()
    }

    pub(crate) fn first_retained_index(&self) -> RecordIndex {
        let state = self.state.lock();
        state
            .segments
            .front()
            .map_or(state.writer.next_index, |segment| segment.first_index)
    }

    /// Hand out the redelivery window once: digests of current-series application records with
    /// `seq >= next_target`, the first inbound sequence number the engine will deliver.
    pub(crate) fn take_redelivery_window(&self, next_target: u64) -> Vec<(u64, u64)> {
        let mut state = self.state.lock();
        let mut window = std::mem::take(&mut state.redelivery);
        window.retain(|(seq, _)| *seq >= next_target);
        window
    }

    /// Durably append one record and publish it to readers.
    ///
    /// # Errors
    /// Returns the poisoning cause; after the first failure every append fails.
    pub(crate) fn append(&self, record: &Record) -> Result<RecordIndex, ConnectorError> {
        let mut state = self.state.lock();
        let index = self.append_locked(&mut state, record)?;
        apply_progress(&mut state.progress, record);
        let end = DurableEnd {
            next_index: state.writer.next_index,
            segment_first: state.writer.segment_first,
            segment_bytes: state.writer.segment_bytes,
        };
        self.refresh_capacity(&state);
        drop(state);
        *self.end.lock() = end;
        self.data_ready.notify_one();
        Ok(index)
    }

    fn append_locked(
        &self,
        state: &mut State,
        record: &Record,
    ) -> Result<RecordIndex, ConnectorError> {
        if let Some(cause) = &state.writer.poisoned {
            return Err(ConnectorError::WriteError(format!(
                "FIX journal stopped after an earlier failure: {cause}"
            )));
        }
        let mut payload = std::mem::take(&mut state.writer.payload);
        record.encode_into(&mut payload)?;
        if payload.len() as u64 > self.options.max_record_bytes {
            state.writer.payload = payload;
            return Err(ConnectorError::ConfigurationError(format!(
                "FIX journal record of {} bytes exceeds the configured limit",
                state.writer.payload.len()
            )));
        }
        let frame_bytes = FRAME_OVERHEAD + payload.len() as u64;
        let result = if state.writer.segment_bytes + frame_bytes > self.options.segment_bytes
            && state.writer.segment_bytes > HEADER_LEN
        {
            self.rotate(state)
        } else {
            Ok(())
        }
        .and_then(|()| write_frame(&mut state.writer, &payload, self.options.sync));
        state.writer.payload = payload;
        if let Err(error) = result {
            let cause = error.to_string();
            state.writer.poisoned = Some(cause.clone());
            return Err(ConnectorError::outcome_unknown(
                format!("FIX journal append failed; the session stops: {cause}"),
                false,
            ));
        }
        let index = state.writer.next_index;
        state.writer.next_index += 1;
        if let Some(last) = state.segments.back_mut() {
            last.bytes = state.writer.segment_bytes;
        }
        state.retained_bytes += frame_bytes;
        Ok(index)
    }

    /// Start a new segment whose first record snapshots the current session progress.
    fn rotate(&self, state: &mut State) -> Result<(), ConnectorError> {
        let first_index = state.writer.next_index;
        let snapshot = Record::SessionSnapshot(state.progress);
        snapshot.encode_into(&mut state.writer.payload)?;
        segment::encode_frame(&state.writer.payload, &mut state.writer.frame)?;
        let file = segment::create_segment(
            &self.segments_dir,
            first_index,
            &self.id,
            &state.writer.frame,
        )?;
        let bytes = HEADER_LEN + state.writer.frame.len() as u64;
        state.writer.file = file;
        state.writer.segment_first = first_index;
        state.writer.segment_bytes = bytes;
        state.writer.next_index += 1;
        state.segments.push_back(SegmentInfo { first_index, bytes });
        state.retained_bytes += bytes;
        Ok(())
    }

    /// Delete segments whose every record precedes `committed`, the next record index of a
    /// durably committed checkpoint. The active segment is never deleted.
    ///
    /// # Errors
    /// Returns an error when a segment cannot be deleted; retained data is never lost.
    pub(crate) fn release_before(&self, committed: RecordIndex) -> Result<(), ConnectorError> {
        let mut state = self.state.lock();
        while state.segments.len() > 1 {
            let next_first = state.segments[1].first_index;
            if next_first > committed {
                break;
            }
            let Some(oldest) = state.segments.front().copied() else {
                break;
            };
            std::fs::remove_file(segment::segment_path(
                &self.segments_dir,
                oldest.first_index,
            ))?;
            state.segments.pop_front();
            state.retained_bytes = state.retained_bytes.saturating_sub(oldest.bytes);
        }
        self.refresh_capacity(&state);
        Ok(())
    }

    fn refresh_capacity(&self, state: &State) {
        self.over_capacity.store(
            state.retained_bytes > self.options.max_bytes,
            Ordering::Release,
        );
    }

    /// Open a reader positioned at `start`.
    ///
    /// # Errors
    /// Fails when `start` was already deleted or lies beyond the durable end.
    pub(crate) fn reader(&self, start: RecordIndex) -> Result<JournalReader, ConnectorError> {
        let end = self.durable_end();
        let first = self.first_retained_index();
        if start < first {
            return Err(ConnectorError::ConfigurationError(format!(
                "FIX journal no longer retains record {start} (oldest retained is {first}); \
                 the restore point expired"
            )));
        }
        if start > end.next_index {
            return Err(ConnectorError::ConfigurationError(format!(
                "checkpoint names FIX journal record {start}, beyond the durable end {}; the \
                 journal lost records its durability policy promised to keep",
                end.next_index
            )));
        }
        let segments: Vec<RecordIndex> = {
            let state = self.state.lock();
            state
                .segments
                .iter()
                .map(|segment| segment.first_index)
                .collect()
        };
        let segment_first = segments
            .iter()
            .copied()
            .rfind(|first| *first <= start)
            .ok_or_else(|| corrupt("no segment covers the requested record"))?;
        let mut reader = JournalReader {
            segments_dir: self.segments_dir.clone(),
            journal_id: self.id,
            max_frame: self.options.max_record_bytes,
            segment_first,
            sealed_len: None,
            next_index: segment_first,
            frames: None,
            payload: Vec::new(),
        };
        reader.open_segment(segment_first)?;
        while reader.next_index < start {
            if reader.next(&end)?.is_none() {
                return Err(corrupt("segment ended before the requested record"));
            }
        }
        Ok(reader)
    }
}

/// Sequential reader of durable records.
pub(crate) struct JournalReader {
    segments_dir: PathBuf,
    journal_id: [u8; 16],
    max_frame: u64,
    segment_first: RecordIndex,
    /// Final length of the current segment once it is known to be sealed.
    sealed_len: Option<u64>,
    next_index: RecordIndex,
    frames: Option<FrameReader>,
    payload: Vec<u8>,
}

impl JournalReader {
    pub(crate) fn next_index(&self) -> RecordIndex {
        self.next_index
    }

    fn open_segment(&mut self, first_index: RecordIndex) -> Result<(), ConnectorError> {
        let mut file = File::open(segment::segment_path(&self.segments_dir, first_index))?;
        segment::check_header(&mut file, first_index, &self.journal_id)?;
        self.frames = Some(FrameReader::new(file, HEADER_LEN, self.max_frame)?);
        self.segment_first = first_index;
        self.sealed_len = None;
        self.next_index = first_index;
        Ok(())
    }

    /// Next durable record, or `None` at the durable end.
    ///
    /// # Errors
    /// Fails on I/O errors or a corrupt record before the durable end.
    pub(crate) fn next(
        &mut self,
        end: &DurableEnd,
    ) -> Result<Option<(RecordIndex, Record)>, ConnectorError> {
        loop {
            if self.next_index >= end.next_index {
                return Ok(None);
            }
            let Some(frames) = self.frames.as_mut() else {
                return Err(corrupt("journal reader has no open segment"));
            };
            let sealed = self.segment_first != end.segment_first;
            let limit = match (sealed, self.sealed_len) {
                (false, _) => end.segment_bytes,
                (true, Some(length)) => length,
                (true, None) => *self.sealed_len.insert(frames.file_len()?),
            };
            match frames.next(limit, &mut self.payload)? {
                FrameRead::Frame => {
                    let index = self.next_index;
                    self.next_index += 1;
                    return Ok(Some((index, Record::decode(&self.payload)?)));
                }
                FrameRead::End if sealed => {
                    // A sealed segment ended: continue in the segment that starts here.
                    let next = self.next_index;
                    self.open_segment(next)?;
                }
                _ => {
                    return Err(corrupt(&format!(
                        "record {} is unreadable before the durable end",
                        self.next_index
                    )))
                }
            }
        }
    }
}

fn write_frame(
    writer: &mut Writer,
    payload: &[u8],
    sync: SyncPolicy,
) -> Result<(), ConnectorError> {
    segment::encode_frame(payload, &mut writer.frame)?;
    #[cfg(test)]
    let write = if tests::take_injected_write_failure() {
        let half = writer.frame.len() / 2;
        writer
            .file
            .write_all(&writer.frame[..half])
            .and_then(|()| Err(std::io::Error::other("injected short write")))
    } else {
        writer.file.write_all(&writer.frame)
    };
    #[cfg(not(test))]
    let write = writer.file.write_all(&writer.frame);
    if let Err(error) = write {
        // Remove a partial frame so the segment stays valid for recovery; the writer is
        // poisoned either way.
        let _ = writer.file.set_len(writer.segment_bytes);
        let _ = writer.file.seek(SeekFrom::Start(writer.segment_bytes));
        return Err(error.into());
    }
    if sync == SyncPolicy::Fsync {
        writer.file.sync_data()?;
    }
    writer.segment_bytes += writer.frame.len() as u64;
    Ok(())
}

fn apply_progress(progress: &mut SessionSnapshot, record: &Record) {
    match record {
        Record::SessionSnapshot(snapshot) => *progress = *snapshot,
        Record::SeriesStarted {
            series,
            store_created_unix_seconds,
        } => {
            *progress = SessionSnapshot {
                series: *series,
                store_created_unix_seconds: *store_created_unix_seconds,
                high_water: 0,
            };
        }
        Record::Application(record) => progress.high_water = progress.high_water.max(record.seq),
        Record::GapFill { new_seq, .. } | Record::SequenceReset { new_seq, .. } => {
            progress.high_water = progress.high_water.max(new_seq.saturating_sub(1));
        }
        // The engine also rejects messages that arrived ahead of the expected sequence number
        // without consuming it, so a reject is evidence only, never sequence progress.
        Record::Rejected { .. } => {}
    }
}

fn load_or_create_meta(
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

fn parse_journal_id(text: &str) -> Result<[u8; 16], ConnectorError> {
    uuid::Uuid::parse_str(text)
        .map(|id| *id.as_bytes())
        .map_err(|_| corrupt("journal id is not a UUID"))
}

/// Rebuild writer state, progress, and the redelivery window; truncate a torn tail.
fn recover(
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
        return Ok(State {
            writer: Writer {
                file,
                segment_first: 0,
                segment_bytes: bytes,
                next_index: 1,
                poisoned: None,
                payload,
                frame,
            },
            progress: SessionSnapshot::default(),
            segments: VecDeque::from([SegmentInfo {
                first_index: 0,
                bytes,
            }]),
            retained_bytes: bytes,
            redelivery: Vec::new(),
        });
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
    let retained_bytes = segments.iter().map(|segment| segment.bytes).sum();
    Ok(State {
        writer: Writer {
            file,
            segment_first: active.first_index,
            segment_bytes: active.bytes,
            next_index,
            poisoned: None,
            payload,
            frame: Vec::new(),
        },
        progress,
        segments,
        retained_bytes,
        redelivery,
    })
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

#[cfg(test)]
mod tests;
