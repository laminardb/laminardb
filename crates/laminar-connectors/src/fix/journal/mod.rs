//! Node-local durable journal of accepted inbound FIX records.
//!
//! The journal is the only path from the session to the projection. Session callbacks append
//! records before they return to the engine. Under `SyncPolicy::Fsync` appends are group
//! committed: an append only buffers its frame, and the sync path writes everything buffered so
//! far and makes it durable with one `fdatasync`, so the engine thread never waits on storage (a
//! flush also blocks concurrent writes to the same file on Windows). While a record is not yet
//! durable the journal holds the session store's persisted inbound sequence number at or below
//! the record's sequence number (`DurabilityFloor`), so a crash inside the failure model makes
//! the counterparty retransmit every record it lost. Records that start or reset a sequence
//! series are synced before the append returns.
//!
//! Readers see records only up to the durable end, so a LaminarDB checkpoint never names a
//! journal position that a crash inside the failure model can lose. Segments wholly before the
//! latest committed source cursor are deleted.
//!
//! Any failed write or sync poisons the writer: the callback disconnects without advancing the
//! session, no later record is appended or synced, and the next start reconciles the file by
//! truncating a torn tail. A failed `fsync` is never retried because its outcome is ambiguous.
//!
//! Lock order: `sync_serial`, then `state`, then the floor's own lock. The floor is called only
//! under `state` so that floor changes apply in the order the journal decided them.

mod record;
mod recovery;
mod segment;

pub(crate) use record::{ApplicationRecord, Record, SessionSnapshot};

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;

use laminar_core::durable_fs::ensure_durable_directory;
use parking_lot::{Condvar, Mutex};
use tokio::sync::Notify;

use crate::error::ConnectorError;
use record::corrupt;
use recovery::{load_or_create_meta, parse_journal_id, recover};
use segment::{FrameRead, FrameReader, FRAME_OVERHEAD, HEADER_LEN};

const META_FILE: &str = "journal.json";
const META_FORMAT: u32 = 1;
const LOCK_FILE: &str = ".laminardb-fix.lock";
/// Unsynced bytes above which an append syncs inline, bounding how far durability and reader
/// visibility can trail the session when storage stalls.
const MAX_UNSYNCED_BYTES: u64 = 16 * 1024 * 1024;

/// Failure model of the journal and the session store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncPolicy {
    /// Group-committed `fdatasync`: survives process, operating-system, and power failure of
    /// the host, given storage that honours flushes.
    Fsync,
    /// Written to the operating system: survives process failure only.
    OperatingSystem,
}

/// Receives the lowest inbound sequence number whose journal record is written but not yet
/// durable; `None` when every record is durable. The session store never persists an inbound
/// sequence number above it.
pub(crate) trait DurabilityFloor: Send + Sync {
    fn set(&self, floor: Option<u64>);
}

/// How an appended record constrains the session store until it is durable.
enum Durability {
    /// Neither the engine's sequence progress nor the series depends on it.
    Plain,
    /// The engine may advance past this inbound sequence number once the append returns.
    Hold(u64),
    /// Starts or resets a sequence series: durable before the append returns.
    Barrier,
}

impl Durability {
    fn of(record: &Record) -> Self {
        match record {
            Record::Application(application) => Self::Hold(application.seq),
            Record::GapFill { from_seq, .. } => Self::Hold(*from_seq),
            Record::SeriesStarted { .. } | Record::SequenceReset { .. } => Self::Barrier,
            Record::SessionSnapshot(_) | Record::Rejected { .. } => Self::Plain,
        }
    }
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
    /// Length of the current segment including `pending`.
    segment_bytes: u64,
    next_index: RecordIndex,
    poisoned: Option<String>,
    payload: Vec<u8>,
    frame: Vec<u8>,
    /// Frames buffered under `SyncPolicy::Fsync` that the sync path has not written yet.
    pending: Vec<u8>,
}

impl Writer {
    fn new(
        file: File,
        segment_first: RecordIndex,
        segment_bytes: u64,
        next_index: RecordIndex,
        payload: Vec<u8>,
        frame: Vec<u8>,
    ) -> Self {
        Self {
            file,
            segment_first,
            segment_bytes,
            next_index,
            poisoned: None,
            payload,
            frame,
            pending: Vec::new(),
        }
    }

    /// Write the buffered frames to the end of the current segment.
    fn write_pending(&mut self) -> Result<(), ConnectorError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let written = self.segment_bytes - self.pending.len() as u64;
        write_at_end(&mut self.file, written, &self.pending)?;
        self.pending.clear();
        Ok(())
    }
}

struct State {
    writer: Writer,
    progress: SessionSnapshot,
    segments: VecDeque<SegmentInfo>,
    retained_bytes: u64,
    /// `(seq, content_digest)` of current-series application records awaiting possible
    /// retransmission after a restart; consumed by `take_redelivery_window`.
    redelivery: Vec<(u64, u64)>,
    /// Records before this index are durable.
    synced: RecordIndex,
    unsynced_bytes: u64,
    /// Lowest held sequence number among records the running sync covers.
    hold_syncing: Option<u64>,
    /// Lowest held sequence number among records no sync covers yet.
    hold_unsynced: Option<u64>,
    stopping: bool,
}

impl State {
    /// State of a reopened journal: everything recovered is durable.
    fn new(
        writer: Writer,
        progress: SessionSnapshot,
        segments: VecDeque<SegmentInfo>,
        redelivery: Vec<(u64, u64)>,
    ) -> Self {
        let synced = writer.next_index;
        let retained_bytes = segments.iter().map(|segment| segment.bytes).sum();
        Self {
            writer,
            progress,
            segments,
            retained_bytes,
            redelivery,
            synced,
            unsynced_bytes: 0,
            hold_syncing: None,
            hold_unsynced: None,
            stopping: false,
        }
    }

    fn floor(&self) -> Option<u64> {
        self.hold_syncing.or(self.hold_unsynced)
    }

    fn writer_end(&self) -> DurableEnd {
        DurableEnd {
            next_index: self.writer.next_index,
            segment_first: self.writer.segment_first,
            segment_bytes: self.writer.segment_bytes,
        }
    }
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
    /// Serializes syncs so that each completion covers a prefix of the next one.
    sync_serial: Mutex<()>,
    sync_wanted: Condvar,
    floor: OnceLock<Arc<dyn DurabilityFloor>>,
    _lock: File,
}

/// The group-commit thread of a journal under `SyncPolicy::Fsync`. Dropping it syncs what was
/// appended and joins the thread.
pub(crate) struct GroupCommit {
    journal: Arc<Journal>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for GroupCommit {
    fn drop(&mut self) {
        self.journal.state.lock().stopping = true;
        self.journal.sync_wanted.notify_all();
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                tracing::error!("FIX journal sync thread panicked");
            }
        }
    }
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
        if options.sync == SyncPolicy::Fsync {
            // Recovered bytes may be writes of a crashed process that never reached storage.
            for segment in &recovered.segments {
                OpenOptions::new()
                    .write(true)
                    .open(segment::segment_path(&segments_dir, segment.first_index))?
                    .sync_data()?;
            }
        }
        let end = recovered.writer_end();
        let journal = Self {
            segments_dir,
            id: journal_id,
            meta,
            options,
            over_capacity: AtomicBool::new(false),
            state: Mutex::new(recovered),
            end: Mutex::new(end),
            data_ready,
            sync_serial: Mutex::new(()),
            sync_wanted: Condvar::new(),
            floor: OnceLock::new(),
            _lock: lock,
        };
        journal.refresh_capacity(&journal.state.lock());
        Ok(journal)
    }

    pub(crate) fn meta(&self) -> &JournalMeta {
        &self.meta
    }

    /// Bind the session store that must not persist inbound progress past durable records.
    /// Called once, before the session starts.
    pub(crate) fn attach_floor(
        &self,
        floor: Arc<dyn DurabilityFloor>,
    ) -> Result<(), ConnectorError> {
        self.floor.set(floor).map_err(|_| {
            ConnectorError::Internal("FIX journal durability floor is already attached".into())
        })
    }

    /// Start the group-commit thread; `None` under `SyncPolicy::OperatingSystem`.
    pub(crate) fn start_group_commit(
        self: &Arc<Self>,
    ) -> Result<Option<GroupCommit>, ConnectorError> {
        if self.options.sync != SyncPolicy::Fsync {
            return Ok(None);
        }
        let journal = self.clone();
        let thread = std::thread::Builder::new()
            .name("laminardb-fix-journal-sync".into())
            .spawn(move || journal.run_group_commit())?;
        Ok(Some(GroupCommit {
            journal: self.clone(),
            thread: Some(thread),
        }))
    }

    fn run_group_commit(&self) {
        loop {
            {
                let mut state = self.state.lock();
                while state.synced == state.writer.next_index
                    && !state.stopping
                    && state.writer.poisoned.is_none()
                {
                    self.sync_wanted.wait(&mut state);
                }
                let drained = state.synced == state.writer.next_index;
                if state.writer.poisoned.is_some() || (state.stopping && drained) {
                    return;
                }
            }
            if let Err(error) = self.sync() {
                tracing::error!(error = %error, "FIX journal sync failed; capture stops");
                return;
            }
        }
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

    /// Append one record. Under `SyncPolicy::OperatingSystem` it is durable and readable on
    /// return; under `SyncPolicy::Fsync` it becomes readable when a sync covers it, and until
    /// then holds the durability floor at its sequence number.
    ///
    /// # Errors
    /// Returns the poisoning cause; after the first failure every append fails.
    pub(crate) fn append(&self, record: &Record) -> Result<RecordIndex, ConnectorError> {
        let durability = Durability::of(record);
        let mut state = self.state.lock();
        let index = self.append_locked(&mut state, record)?;
        apply_progress(&mut state.progress, record);
        self.refresh_capacity(&state);
        if self.options.sync == SyncPolicy::OperatingSystem {
            state.synced = state.writer.next_index;
            state.unsynced_bytes = 0;
            let end = state.writer_end();
            drop(state);
            self.publish(end);
            return Ok(index);
        }
        if let (Durability::Hold(seq), None) = (&durability, state.hold_unsynced) {
            let before = state.floor();
            state.hold_unsynced = Some(*seq);
            if before.is_none() {
                self.set_floor(state.floor());
            }
        }
        let sync_now =
            matches!(durability, Durability::Barrier) || state.unsynced_bytes > MAX_UNSYNCED_BYTES;
        drop(state);
        if sync_now {
            self.sync()?;
        } else {
            self.sync_wanted.notify_one();
        }
        Ok(index)
    }

    /// Make every appended record durable and readable.
    ///
    /// # Errors
    /// Fails when the writer is poisoned; a failed sync poisons it.
    pub(crate) fn sync(&self) -> Result<(), ConnectorError> {
        let _serial = self.sync_serial.lock();
        let (file, end, bytes) = {
            let mut state = self.state.lock();
            if let Some(cause) = &state.writer.poisoned {
                return Err(ConnectorError::WriteError(format!(
                    "FIX journal stopped after an earlier failure: {cause}"
                )));
            }
            if state.synced == state.writer.next_index {
                return Ok(());
            }
            if let Err(error) = state.writer.write_pending() {
                return Err(self.poison_sync(state, &error));
            }
            let file = match state.writer.file.try_clone() {
                Ok(file) => file,
                Err(error) => return Err(self.poison_sync(state, &error)),
            };
            state.hold_syncing = state.hold_unsynced.take();
            (file, state.writer_end(), state.unsynced_bytes)
        };
        let result = sync_file(&file);
        let mut state = self.state.lock();
        if let Err(error) = result {
            return Err(self.poison_sync(state, &error));
        }
        state.synced = end.next_index;
        state.unsynced_bytes = state.unsynced_bytes.saturating_sub(bytes);
        let before = state.floor();
        state.hold_syncing = None;
        if state.floor() != before {
            self.set_floor(state.floor());
        }
        drop(state);
        self.publish(end);
        Ok(())
    }

    fn poison_sync(
        &self,
        mut state: parking_lot::MutexGuard<'_, State>,
        error: &dyn std::fmt::Display,
    ) -> ConnectorError {
        let cause = error.to_string();
        state.writer.poisoned = Some(cause.clone());
        drop(state);
        self.sync_wanted.notify_all();
        ConnectorError::outcome_unknown(
            format!("FIX journal sync failed; the session stops: {cause}"),
            false,
        )
    }

    fn set_floor(&self, floor: Option<u64>) {
        if let Some(sink) = self.floor.get() {
            sink.set(floor);
        }
    }

    fn publish(&self, end: DurableEnd) {
        *self.end.lock() = end;
        self.data_ready.notify_one();
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
        .and_then(|()| self.write_frame(&mut state.writer, &payload));
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
        state.unsynced_bytes += frame_bytes;
        Ok(index)
    }

    /// Start a new segment whose first record snapshots the current session progress. The
    /// outgoing segment is synced first, so a durable end in the new segment implies every
    /// earlier record is durable.
    fn rotate(&self, state: &mut State) -> Result<(), ConnectorError> {
        if self.options.sync == SyncPolicy::Fsync {
            state.writer.write_pending()?;
            sync_file(&state.writer.file)?;
        }
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

    fn write_frame(&self, writer: &mut Writer, payload: &[u8]) -> Result<(), ConnectorError> {
        segment::encode_frame(payload, &mut writer.frame)?;
        match self.options.sync {
            SyncPolicy::Fsync => writer.pending.extend_from_slice(&writer.frame),
            SyncPolicy::OperatingSystem => {
                write_at_end(&mut writer.file, writer.segment_bytes, &writer.frame)?;
            }
        }
        writer.segment_bytes += writer.frame.len() as u64;
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

/// Write `bytes` at the end of a segment of `written` bytes.
fn write_at_end(file: &mut File, written: u64, bytes: &[u8]) -> Result<(), ConnectorError> {
    #[cfg(test)]
    let write = if tests::take_injected_write_failure() {
        file.write_all(&bytes[..bytes.len() / 2])
            .and_then(|()| Err(std::io::Error::other("injected short write")))
    } else {
        file.write_all(bytes)
    };
    #[cfg(not(test))]
    let write = file.write_all(bytes);
    if let Err(error) = write {
        // Remove a partial frame so the segment stays valid for recovery; the writer is
        // poisoned either way.
        let _ = file.set_len(written);
        let _ = file.seek(SeekFrom::Start(written));
        return Err(error.into());
    }
    Ok(())
}

fn sync_file(file: &File) -> std::io::Result<()> {
    #[cfg(test)]
    if tests::take_injected_sync_failure() {
        return Err(std::io::Error::other("injected sync failure"));
    }
    file.sync_data()
}

fn apply_progress(progress: &mut SessionSnapshot, record: &Record) {
    match record {
        Record::SessionSnapshot(snapshot) => *progress = *snapshot,
        Record::SeriesStarted {
            series,
            store_created_unix_micros,
        } => {
            *progress = SessionSnapshot {
                series: *series,
                store_created_unix_micros: *store_created_unix_micros,
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

#[cfg(test)]
mod tests;
