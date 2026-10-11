//! Delta kernel reads on the blocking pool with a byte-bounded hand-off to the source.
//!
//! Snapshot files are read one at a time in path order at a pinned version; change data is read
//! with the kernel's `TableChanges`, which applies deletion vectors, prefers `cdc` actions over
//! `add`/`remove` derivation, skips `dataChange = false` rewrites, and rejects column mapping and
//! unsupported reader features. Every replay unit (one snapshot file or one commit) ends with a
//! cursor message; nothing inside a unit is ever resumable.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::SchemaRef;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::scan::state::{transform_to_logical, ScanFile};
use delta_kernel::table_changes::TableChanges;
use delta_kernel::{Engine, EngineData, FileMeta, Snapshot};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use url::Url;

use super::cursor::DeltaPhase;
use crate::connector::ConnectorTaskGuard;
use crate::error::ConnectorError;

/// Bytes of decoded rows the reader may queue ahead of the source.
pub(super) const MAX_QUEUED_BYTES: u32 = 256 * 1024 * 1024;
/// Batches (each one kernel Parquet read) the reader may queue ahead of the source.
const SCAN_CHANNEL_CAPACITY: usize = 4;
/// Upper bound on the files of one pinned snapshot held in memory while it is emitted.
const MAX_SNAPSHOT_FILES: usize = 1_000_000;

const COMMIT_VERSION_COLUMN: &str = "_commit_version";

/// The replay unit a batch belongs to. Units are totally ordered: every snapshot file precedes
/// every commit, files follow path order, commits follow version order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum UnitKey {
    SnapshotFile(u64),
    Commit(u64),
}

impl UnitKey {
    /// Order-preserving encoding: a phase byte followed by the big-endian ordinal.
    pub(super) fn order_key(self) -> [u8; 9] {
        let (phase, ordinal) = match self {
            Self::SnapshotFile(ordinal) => (0_u8, ordinal),
            Self::Commit(version) => (1_u8, version),
        };
        let mut key = [0_u8; 9];
        key[0] = phase;
        key[1..].copy_from_slice(&ordinal.to_be_bytes());
        key
    }
}

pub(super) enum ScanOutput {
    /// Rows of the open unit. `completes` is set on the unit's last batch and holds the cursor
    /// that follows it. The permit returns the rows' bytes to the queue budget once dropped.
    Batch {
        batch: RecordBatch,
        unit: UnitKey,
        permit: OwnedSemaphorePermit,
        completes: Option<DeltaPhase>,
    },
    /// A unit without rows completed; the cursor now stands at `phase`.
    UnitEnd(DeltaPhase),
}

pub(super) struct ScanTask {
    pub(super) receiver: mpsc::Receiver<Result<ScanOutput, ConnectorError>>,
}

/// Inputs shared by every read of one started source.
#[derive(Clone)]
pub(super) struct ReadContext {
    pub(super) engine: Arc<dyn Engine>,
    pub(super) table_root: Url,
    /// Rows are projected to this schema (table columns plus `__weight`) on the reader thread.
    pub(super) output_schema: SchemaRef,
    pub(super) budget: Arc<Semaphore>,
    /// When vended storage credentials are due for renewal. A read past it stops at the next
    /// unit boundary (after completing at least one unit) so the source can renew them.
    pub(super) renew_at: Option<std::time::SystemTime>,
}

impl ReadContext {
    fn renewal_due(&self) -> bool {
        self.renew_at
            .is_some_and(|at| std::time::SystemTime::now() >= at)
    }
}

/// One snapshot replay unit and the cursor that follows it.
struct SnapshotRead {
    version: u64,
    files_done: u64,
    last_file: Option<String>,
    /// Cursor after the final file.
    after: DeltaPhase,
}

struct Held {
    batch: RecordBatch,
    unit: UnitKey,
    permit: OwnedSemaphorePermit,
}

/// Hands batches to the source one behind the read, so the last batch of a unit can carry the
/// cursor that follows it and the source never reports a unit open after its final row.
struct Sender {
    channel: mpsc::Sender<Result<ScanOutput, ConnectorError>>,
    context: ReadContext,
    held: Option<Held>,
}

impl Sender {
    /// Queue `batch` of the open unit. Returns `false` once the source stopped receiving.
    fn push(&mut self, batch: &RecordBatch, unit: UnitKey) -> Result<bool, ConnectorError> {
        if batch.num_rows() == 0 {
            return Ok(true);
        }
        let batch = crate::schema::resolution::project_batch(batch, &self.context.output_schema)?;
        let bytes = u32::try_from(batch.get_array_memory_size())
            .ok()
            .filter(|bytes| *bytes <= MAX_QUEUED_BYTES)
            .ok_or_else(|| {
                ConnectorError::ConfigurationError(format!(
                    "one decoded Delta batch retains {} bytes, above the source's \
                     {MAX_QUEUED_BYTES} byte read bound; rows this wide cannot be streamed",
                    batch.get_array_memory_size()
                ))
            })?;
        // Release the previous batch before waiting for budget so the two never deadlock.
        if let Some(held) = self.held.take() {
            if !self.send(held, None) {
                return Ok(false);
            }
        }
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| ConnectorError::Internal("Delta reader lost its runtime handle".into()))?;
        let Ok(permit) =
            handle.block_on(Arc::clone(&self.context.budget).acquire_many_owned(bytes))
        else {
            return Ok(false);
        };
        self.held = Some(Held {
            batch,
            unit,
            permit,
        });
        Ok(true)
    }

    /// Close the open unit; the cursor that follows it is `phase`.
    fn end_unit(&mut self, phase: DeltaPhase) -> bool {
        match self.held.take() {
            Some(held) => self.send(held, Some(phase)),
            None => self
                .channel
                .blocking_send(Ok(ScanOutput::UnitEnd(phase)))
                .is_ok(),
        }
    }

    fn send(&self, held: Held, completes: Option<DeltaPhase>) -> bool {
        self.channel
            .blocking_send(Ok(ScanOutput::Batch {
                batch: held.batch,
                unit: held.unit,
                permit: held.permit,
                completes,
            }))
            .is_ok()
    }

    fn fail(&self, error: ConnectorError) {
        let _ = self.channel.blocking_send(Err(error));
    }
}

fn spawn(
    context: ReadContext,
    guard: ConnectorTaskGuard,
    read: impl FnOnce(&mut Sender) -> Result<(), ConnectorError> + Send + 'static,
) -> ScanTask {
    let (sender, receiver) = mpsc::channel(SCAN_CHANNEL_CAPACITY);
    let mut sender = Sender {
        channel: sender,
        context,
        held: None,
    };
    // The blocking read cannot be aborted; dropping the receiver or closing the byte budget
    // makes its next hand-off return, and the guard reports its true termination.
    drop(tokio::task::spawn_blocking(move || {
        let _guard = guard;
        if let Err(error) = read(&mut sender) {
            sender.fail(error);
        }
    }));
    ScanTask { receiver }
}

/// Read the snapshot pinned at `version`, resuming after `files_done` completed files.
pub(super) fn spawn_snapshot(
    context: ReadContext,
    guard: ConnectorTaskGuard,
    version: u64,
    files_done: u64,
    last_file: Option<String>,
    after: DeltaPhase,
) -> ScanTask {
    let read = SnapshotRead {
        version,
        files_done,
        last_file,
        after,
    };
    spawn(context, guard, move |sender| read_snapshot(sender, read))
}

/// Read every commit in `start..=end` as one unit per commit.
pub(super) fn spawn_changes(
    context: ReadContext,
    guard: ConnectorTaskGuard,
    start: u64,
    end: u64,
) -> ScanTask {
    spawn(context, guard, move |sender| {
        read_changes(sender, start, end)
    })
}

fn read_snapshot(sender: &mut Sender, read: SnapshotRead) -> Result<(), ConnectorError> {
    let context = sender.context.clone();
    let engine = context.engine.as_ref();
    let snapshot = Snapshot::builder_for(context.table_root.as_str())
        .at_version(read.version)
        .build(engine)
        .map_err(|error| kernel_read_error("open pinned Delta snapshot", &error))?;
    let scan = Arc::clone(&snapshot)
        .scan_builder()
        .build()
        .map_err(|error| kernel_read_error("plan Delta snapshot scan", &error))?;
    let mut files = Vec::new();
    for metadata in scan
        .scan_metadata(engine)
        .map_err(|error| kernel_read_error("list Delta snapshot files", &error))?
    {
        let metadata =
            metadata.map_err(|error| kernel_read_error("list Delta snapshot files", &error))?;
        files = metadata
            .visit_scan_files(files, |files: &mut Vec<ScanFile>, file| files.push(file))
            .map_err(|error| kernel_read_error("list Delta snapshot files", &error))?;
        if files.len() > MAX_SNAPSHOT_FILES {
            return Err(ConnectorError::ConfigurationError(format!(
                "Delta snapshot at version {} has more than {MAX_SNAPSHOT_FILES} files; compact \
                 the table or start from a later version",
                read.version
            )));
        }
    }
    files.sort_unstable_by(|left, right| left.path.cmp(&right.path));

    let skip = usize::try_from(read.files_done).map_err(|_| snapshot_mismatch(&read))?;
    if skip > files.len() || (skip > 0 && Some(&files[skip - 1].path) != read.last_file.as_ref()) {
        return Err(snapshot_mismatch(&read));
    }
    let physical_schema = Arc::clone(scan.physical_schema());
    let logical_schema = Arc::clone(scan.logical_schema());
    for (index, file) in files.iter().enumerate().skip(skip) {
        if index > skip && context.renewal_due() {
            return Ok(());
        }
        let ordinal = u64::try_from(index).map_err(|_| snapshot_mismatch(&read))?;
        let unit = UnitKey::SnapshotFile(ordinal);
        let location = context
            .table_root
            .join(&file.path)
            .map_err(|_| ConnectorError::ReadError("invalid Delta data file path".into()))?;
        let meta = FileMeta {
            location,
            last_modified: file.modification_time,
            size: u64::try_from(file.size)
                .map_err(|_| ConnectorError::ReadError("negative Delta file size".into()))?,
        };
        let mut selection = Selection::new(
            file.dv_info
                .get_selection_vector(engine, &context.table_root)
                .map_err(|error| kernel_read_error("read Delta deletion vector", &error))?,
        );
        for data in engine
            .parquet_handler()
            .read_parquet_files(&[meta], Arc::clone(&physical_schema), None)
            .map_err(|error| kernel_read_error("read Delta data file", &error))?
        {
            let data = data.map_err(|error| kernel_read_error("read Delta data file", &error))?;
            let logical = transform_to_logical(
                engine,
                data,
                &physical_schema,
                &logical_schema,
                file.transform.clone(),
            )
            .map_err(|error| kernel_read_error("map Delta data file to table schema", &error))?;
            let logical = match selection.take(logical.len()) {
                Some(mask) => logical
                    .apply_selection_vector(mask)
                    .map_err(|error| kernel_read_error("apply Delta deletion vector", &error))?,
                None => logical,
            };
            let batch = record_batch(logical)?;
            let weights = Int64Array::from_value(1, batch.num_rows());
            if !sender.push(&with_weights(&batch, weights)?, unit)? {
                return Ok(());
            }
        }
        let files_done = ordinal
            .checked_add(1)
            .ok_or_else(|| snapshot_mismatch(&read))?;
        let phase = if index + 1 == files.len() {
            read.after.clone()
        } else {
            DeltaPhase::Snapshot {
                version: read.version,
                files_done,
                last_file: Some(file.path.clone()),
            }
        };
        if !sender.end_unit(phase) {
            return Ok(());
        }
    }
    if skip == files.len() {
        sender.end_unit(read.after);
    }
    Ok(())
}

fn read_changes(sender: &mut Sender, start: u64, end: u64) -> Result<(), ConnectorError> {
    let context = sender.context.clone();
    let changes = TableChanges::try_new(
        context.table_root.clone(),
        context.engine.as_ref(),
        start,
        Some(end),
    )
    .map_err(|error| kernel_read_error("open Delta change data feed", &error))?;
    let scan = changes
        .into_scan_builder()
        .build()
        .map_err(|error| kernel_read_error("plan Delta change data feed", &error))?;
    let mut open_version: Option<u64> = None;
    for data in scan
        .execute(Arc::clone(&context.engine))
        .map_err(|error| kernel_read_error("read Delta change data feed", &error))?
    {
        let data =
            data.map_err(|error| kernel_read_error("read Delta change data feed", &error))?;
        let batch = record_batch(data)?;
        let Some(version) = single_commit_version(&batch)? else {
            continue;
        };
        if !(start..=end).contains(&version) || open_version.is_some_and(|open| version < open) {
            return Err(ConnectorError::ReadError(format!(
                "Delta change data for version {version} arrived out of commit order"
            )));
        }
        if open_version.is_some_and(|open| version > open)
            && (!sender.end_unit(DeltaPhase::Changes {
                next_version: version,
            }) || context.renewal_due())
        {
            return Ok(());
        }
        open_version = Some(version);
        let changelog = super::super::delta_io::map_cdf_to_changelog(&batch)?;
        if !sender.push(&changelog, UnitKey::Commit(version))? {
            return Ok(());
        }
    }
    let next_version = end
        .checked_add(1)
        .ok_or_else(|| ConnectorError::ReadError("Delta version overflowed".into()))?;
    sender.end_unit(DeltaPhase::Changes { next_version });
    Ok(())
}

/// The commit version of a change batch. A kernel batch comes from one file of one commit.
fn single_commit_version(batch: &RecordBatch) -> Result<Option<u64>, ConnectorError> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let column = batch
        .column_by_name(COMMIT_VERSION_COLUMN)
        .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| {
            ConnectorError::ReadError("Delta change data is missing _commit_version".into())
        })?;
    if column.null_count() != 0 {
        return Err(ConnectorError::ReadError(
            "Delta change data has a null _commit_version".into(),
        ));
    }
    let first = column.value(0);
    if column.values().iter().any(|version| *version != first) {
        return Err(ConnectorError::ReadError(
            "one Delta change batch spans several commits".into(),
        ));
    }
    u64::try_from(first)
        .map(Some)
        .map_err(|_| ConnectorError::ReadError("negative Delta commit version".into()))
}

fn record_batch(data: Box<dyn EngineData>) -> Result<RecordBatch, ConnectorError> {
    let data = ArrowEngineData::try_from_engine_data(data)
        .map_err(|error| kernel_read_error("convert Delta kernel batch", &error))?;
    Ok(data.record_batch().clone())
}

fn with_weights(batch: &RecordBatch, weights: Int64Array) -> Result<RecordBatch, ConnectorError> {
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(arrow_schema::Field::new(
        laminar_core::changelog::WEIGHT_COLUMN,
        arrow_schema::DataType::Int64,
        false,
    )));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(weights));
    RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns)
        .map_err(|error| ConnectorError::ReadError(format!("Delta snapshot batch: {error}")))
}

fn snapshot_mismatch(read: &SnapshotRead) -> ConnectorError {
    ConnectorError::ConfigurationError(format!(
        "the files of the pinned Delta snapshot at version {} no longer match the checkpointed \
         progress; the snapshot cannot be resumed and the source must be reset",
        read.version
    ))
}

/// Deletion-vector keep mask consumed across the batches of one file. Rows past the end of the
/// mask were never deleted.
struct Selection {
    mask: Option<Vec<bool>>,
    offset: usize,
}

impl Selection {
    fn new(mask: Option<Vec<bool>>) -> Self {
        Self { mask, offset: 0 }
    }

    fn take(&mut self, rows: usize) -> Option<Vec<bool>> {
        let mask = self.mask.as_ref()?;
        let start = self.offset.min(mask.len());
        let end = self.offset.saturating_add(rows).min(mask.len());
        let mut selected = mask[start..end].to_vec();
        selected.resize(rows, true);
        self.offset = self.offset.saturating_add(rows);
        Some(selected)
    }
}

/// Transport failures may be retried before a unit emits rows; protocol, feature, schema and
/// missing-history failures are terminal and never fall back to another position.
pub(super) fn kernel_read_error(context: &str, error: &delta_kernel::Error) -> ConnectorError {
    use delta_kernel::Error;

    match error {
        Error::Backtraced { source, .. } => kernel_read_error(context, source),
        Error::ObjectStore(object_store::Error::NotFound { .. })
        | Error::FileNotFound(_)
        | Error::MissingVersion => ConnectorError::ConfigurationError(format!(
            "{context}: required Delta history or data is unavailable (vacuumed, expired, or \
             never written); restarting from another position would skip or duplicate changes"
        )),
        Error::ObjectStore(_) | Error::IOError(_) | Error::Reqwest(_) | Error::JoinFailure(_) => {
            ConnectorError::ReadError(format!("{context}: retryable storage failure"))
        }
        // The kernel reports log gaps, schema changes, and unsupported features as typed or
        // generic table-state errors; none of them resolves by retrying the same position.
        _ => ConnectorError::ConfigurationError(format!("{context}: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_keys_encode_in_replay_order() {
        let keys = [
            UnitKey::SnapshotFile(0),
            UnitKey::SnapshotFile(255),
            UnitKey::SnapshotFile(u64::MAX),
            UnitKey::Commit(0),
            UnitKey::Commit(256),
        ];
        for pair in keys.windows(2) {
            assert!(pair[0] < pair[1]);
            assert!(pair[0].order_key() < pair[1].order_key());
        }
    }

    #[test]
    fn deletion_vector_mask_is_split_across_batches_and_padded() {
        let mut selection = Selection::new(Some(vec![true, false, false]));
        assert_eq!(selection.take(2), Some(vec![true, false]));
        assert_eq!(selection.take(3), Some(vec![false, true, true]));
        assert_eq!(selection.take(1), Some(vec![true]));
        assert_eq!(Selection::new(None).take(4), None);
    }
}
