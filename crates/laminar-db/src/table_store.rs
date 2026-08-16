//! Primary-key-based reference table store for dimension/enrichment tables.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::row::{RowConverter, SortField};

use crate::error::DbError;
use crate::table_rows::TableRows;

pub(crate) const REFERENCE_TABLE_CHECKPOINT_KEY: &str = "__laminar_reference_tables";
const REFERENCE_TABLE_CHECKPOINT_VERSION: u16 = 2;
const MAX_REFERENCE_TABLE_CHECKPOINT_BYTES: usize = 256 * 1024 * 1024;
const MAX_REFERENCE_TABLES: usize = 4_096;
const MAX_REFERENCE_TABLE_NAME_BYTES: usize = 1_024;
const MAX_REFERENCE_TABLE_ROWS: usize = u32::MAX as usize;
const REFERENCE_TABLE_ARCHIVE_ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;
const CHECKPOINT_CAPTURE_TABLE_OVERHEAD: usize = 256;
const CHECKPOINT_CAPTURE_FIELD_OVERHEAD: usize = 128;
const REFERENCE_TABLE_CHECKPOINT_CHUNK_ROWS: usize = 4_096;

pub(crate) struct ReferenceTableCheckpointCapture {
    tables: Vec<CapturedReferenceTable>,
    estimated_bytes: u64,
}

struct CapturedReferenceTable {
    name: String,
    primary_key: String,
    schema: SchemaRef,
    rows: Vec<(Vec<u8>, RecordBatch)>,
    row_count: usize,
}

#[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct ReferenceTableCheckpointArchive {
    version: u16,
    tables: Vec<ReferenceTableCheckpointEntry>,
}

#[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct ReferenceTableCheckpointEntry {
    name: String,
    primary_key: String,
    row_count: u64,
    ipc: Vec<u8>,
}

impl ReferenceTableCheckpointCapture {
    pub(crate) fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    pub(crate) fn encode(self, max_encoded_bytes: u64) -> Result<(bytes::Bytes, u64), DbError> {
        let max_encoded_bytes = max_encoded_bytes.min(MAX_REFERENCE_TABLE_CHECKPOINT_BYTES as u64);
        let mut entries = Vec::with_capacity(self.tables.len());
        let mut retained_payload_bytes = 0u64;
        for mut table in self.tables {
            table
                .rows
                .sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            let remaining_payload_bytes = max_encoded_bytes
                .checked_sub(retained_payload_bytes)
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "reference-table checkpoint exceeded its staged-state budget".into(),
                    )
                })?;
            let remaining_payload_bytes =
                usize::try_from(remaining_payload_bytes).map_err(|_| {
                    DbError::Checkpoint(
                        "reference-table checkpoint budget does not fit usize".into(),
                    )
                })?;
            let mut bounded =
                laminar_core::serialization::BoundedBytesWriter::new(remaining_payload_bytes);
            let mut encoded_rows = 0usize;
            {
                let mut writer =
                    StreamWriter::try_new(&mut bounded, &table.schema).map_err(|error| {
                        DbError::Checkpoint(format!(
                            "reference-table '{}' IPC header serialization failed: {error}",
                            table.name
                        ))
                    })?;
                if table.rows.is_empty() {
                    writer
                        .write(&RecordBatch::new_empty(Arc::clone(&table.schema)))
                        .map_err(|error| {
                            DbError::Checkpoint(format!(
                                "reference-table '{}' IPC serialization failed: {error}",
                                table.name
                            ))
                        })?;
                } else {
                    for rows in table.rows.chunks(REFERENCE_TABLE_CHECKPOINT_CHUNK_ROWS) {
                        let batch = arrow::compute::concat_batches(
                            &table.schema,
                            rows.iter().map(|(_, batch)| batch),
                        )
                        .map_err(|error| {
                            DbError::Checkpoint(format!(
                                "reference-table '{}' snapshot chunk assembly failed: {error}",
                                table.name
                            ))
                        })?;
                        encoded_rows =
                            encoded_rows.checked_add(batch.num_rows()).ok_or_else(|| {
                                DbError::Checkpoint(
                                    "reference-table checkpoint row count overflow".into(),
                                )
                            })?;
                        writer.write(&batch).map_err(|error| {
                            DbError::Checkpoint(format!(
                                "reference-table '{}' IPC serialization failed: {error}",
                                table.name
                            ))
                        })?;
                    }
                }
                writer.finish().map_err(|error| {
                    DbError::Checkpoint(format!(
                        "reference-table '{}' IPC finalization failed: {error}",
                        table.name
                    ))
                })?;
            }
            if encoded_rows != table.row_count {
                return Err(DbError::Checkpoint(format!(
                    "reference-table '{}' changed while its checkpoint was captured",
                    table.name
                )));
            }
            let ipc = bounded.into_vec();
            let retained_ipc_bytes = u64::try_from(ipc.capacity()).map_err(|_| {
                DbError::Checkpoint("reference-table checkpoint size overflow".into())
            })?;
            retained_payload_bytes = retained_payload_bytes
                .checked_add(retained_ipc_bytes)
                .ok_or_else(|| {
                    DbError::Checkpoint("reference-table checkpoint size overflow".into())
                })?;
            entries.push(ReferenceTableCheckpointEntry {
                name: table.name,
                primary_key: table.primary_key,
                row_count: table.row_count as u64,
                ipc,
            });
        }

        let archive_budget = max_encoded_bytes
            .checked_sub(retained_payload_bytes)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "reference-table checkpoint payload exhausted its staged-state budget".into(),
                )
            })?;
        let archive_budget = usize::try_from(archive_budget).map_err(|_| {
            DbError::Checkpoint("reference-table archive budget does not fit usize".into())
        })?;
        let archive = ReferenceTableCheckpointArchive {
            version: REFERENCE_TABLE_CHECKPOINT_VERSION,
            tables: entries,
        };
        let archive_writer = rkyv::ser::writer::IoWriter::new(
            laminar_core::serialization::BoundedBytesWriter::new(archive_budget),
        );
        let encoded =
            rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&archive, archive_writer)
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "reference-table checkpoint exceeded its staged-state budget while serializing the archive: {error}"
                    ))
                })?
                .into_inner()
                .into_vec();
        debug_assert!(
            retained_payload_bytes.saturating_add(encoded.capacity() as u64) <= max_encoded_bytes
        );
        let retained_bytes = u64::try_from(encoded.capacity())
            .map_err(|_| DbError::Checkpoint("reference-table checkpoint size overflow".into()))?;
        Ok((bytes::Bytes::from(encoded), retained_bytes))
    }
}

struct TableState {
    identity: std::sync::Arc<TableIdentity>,
    schema: SchemaRef,
    primary_key: String,
    pk_index: usize,
    key_converter: RowConverter,
    rows: TableRows,
    row_count: usize,
    ready: bool,
    connector: Option<String>,
}

struct TableIdentity;

/// A fully validated replacement for one startup reference-table snapshot.
/// Construction does not mutate the live store; installation revalidates the
/// captured table contract before any prepared table is swapped.
pub(crate) struct PreparedTableSnapshot {
    name: String,
    table_identity: std::sync::Arc<TableIdentity>,
    schema: SchemaRef,
    primary_key: String,
    rows: TableRows,
    row_count: usize,
}

pub(crate) struct TableStore {
    tables: HashMap<String, TableState>,
}

fn checkpoint_capture_size_overflow(name: &str) -> DbError {
    DbError::Checkpoint(format!(
        "reference-table '{name}' checkpoint capture size overflow"
    ))
}

fn add_checkpoint_capture_bytes(total: &mut u64, bytes: usize, name: &str) -> Result<(), DbError> {
    let bytes = u64::try_from(bytes).map_err(|_| checkpoint_capture_size_overflow(name))?;
    *total = total
        .checked_add(bytes)
        .ok_or_else(|| checkpoint_capture_size_overflow(name))?;
    Ok(())
}

fn validate_checkpoint_identifier(kind: &str, value: &str) -> Result<(), DbError> {
    if value.is_empty() || value.len() > MAX_REFERENCE_TABLE_NAME_BYTES {
        return Err(DbError::Checkpoint(format!(
            "reference-table checkpoint {kind} length {} is outside 1..={MAX_REFERENCE_TABLE_NAME_BYTES}",
            value.len()
        )));
    }
    Ok(())
}

fn decode_checkpoint_batches(
    name: &str,
    ipc: &[u8],
    expected_rows: usize,
) -> Result<Vec<RecordBatch>, DbError> {
    if ipc.is_empty() || ipc.len() > MAX_REFERENCE_TABLE_CHECKPOINT_BYTES {
        return Err(DbError::Checkpoint(format!(
            "reference-table '{name}' IPC length {} is invalid",
            ipc.len()
        )));
    }
    let mut reader = StreamReader::try_new(std::io::Cursor::new(ipc), None).map_err(|error| {
        DbError::Checkpoint(format!(
            "reference-table '{name}' IPC header is invalid: {error}"
        ))
    })?;
    let max_batches = expected_rows
        .div_ceil(REFERENCE_TABLE_CHECKPOINT_CHUNK_ROWS)
        .max(1);
    let mut batches = Vec::with_capacity(max_batches);
    for batch in &mut reader {
        let batch = batch.map_err(|error| {
            DbError::Checkpoint(format!(
                "reference-table '{name}' IPC batch is invalid: {error}"
            ))
        })?;
        if batches.len() == max_batches {
            return Err(DbError::Checkpoint(format!(
                "reference-table '{name}' IPC contains more than {max_batches} batches"
            )));
        }
        if expected_rows != 0 && batch.num_rows() == 0 {
            return Err(DbError::Checkpoint(format!(
                "reference-table '{name}' IPC contains an unexpected empty batch"
            )));
        }
        batches.push(batch);
    }
    if batches.is_empty() {
        return Err(DbError::Checkpoint(format!(
            "reference-table '{name}' IPC contains no record batch"
        )));
    }
    if expected_rows == 0 && (batches.len() != 1 || batches[0].num_rows() != 0) {
        return Err(DbError::Checkpoint(format!(
            "reference-table '{name}' empty checkpoint has an invalid batch inventory"
        )));
    }
    Ok(batches)
}

impl TableStore {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Register a new table with the given schema and primary key column.
    ///
    /// # Errors
    ///
    /// Returns an error if the primary key column does not exist in the schema,
    /// or if a table with the same name already exists.
    pub fn create_table(
        &mut self,
        name: &str,
        schema: SchemaRef,
        primary_key: &str,
    ) -> Result<(), DbError> {
        if self.tables.contains_key(name) {
            return Err(DbError::TableAlreadyExists(name.to_string()));
        }
        let pk_index = schema.index_of(primary_key).map_err(|_| {
            DbError::InvalidOperation(format!(
                "Primary key column '{primary_key}' not found in table '{name}'"
            ))
        })?;
        if schema.field(pk_index).is_nullable() {
            return Err(DbError::InvalidOperation(format!(
                "Primary key column '{primary_key}' in table '{name}' must be non-nullable"
            )));
        }
        let key_converter = RowConverter::new(vec![SortField::new(
            schema.field(pk_index).data_type().clone(),
        )])
        .map_err(|error| {
            DbError::InvalidOperation(format!(
                "unsupported primary key type for table '{name}': {error}"
            ))
        })?;

        self.tables.insert(
            name.to_string(),
            TableState {
                identity: std::sync::Arc::new(TableIdentity),
                schema,
                primary_key: primary_key.to_string(),
                pk_index,
                key_converter,
                rows: TableRows::new(),
                row_count: 0,
                ready: false,
                connector: None,
            },
        );
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> bool {
        self.tables.remove(name).is_some()
    }

    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    pub fn table_schema(&self, name: &str) -> Option<SchemaRef> {
        self.tables.get(name).map(|t| t.schema.clone())
    }

    pub fn primary_key(&self, name: &str) -> Option<&str> {
        self.tables.get(name).map(|t| t.primary_key.as_str())
    }

    pub fn table_row_count(&self, name: &str) -> usize {
        self.tables.get(name).map_or(0, |t| t.row_count)
    }

    #[cfg(test)]
    pub fn is_ready(&self, name: &str) -> bool {
        self.tables.get(name).is_some_and(|t| t.ready)
    }

    #[cfg(test)]
    pub fn set_ready(&mut self, name: &str, ready: bool) {
        if let Some(t) = self.tables.get_mut(name) {
            t.ready = ready;
        }
    }

    pub fn set_connector(&mut self, name: &str, connector_type: &str) {
        if let Some(t) = self.tables.get_mut(name) {
            t.connector = Some(connector_type.to_string());
        }
    }

    pub fn connector(&self, name: &str) -> Option<&str> {
        self.tables.get(name).and_then(|t| t.connector.as_deref())
    }

    /// Validate and stage a complete startup snapshot without changing live
    /// table rows. An empty batch list is a valid empty replacement.
    pub(crate) fn prepare_snapshot(
        &self,
        name: &str,
        batches: &[RecordBatch],
    ) -> Result<PreparedTableSnapshot, DbError> {
        let state = self
            .tables
            .get(name)
            .ok_or_else(|| DbError::TableNotFound(name.to_string()))?;

        for batch in batches {
            validate_batch_contract(name, state, batch)?;
        }
        let rows = TableRows::from_batches(batches, state.pk_index, &state.key_converter).map_err(
            |error| {
                DbError::Storage(format!(
                    "reference-table '{name}' snapshot key validation failed: {error}"
                ))
            },
        )?;
        let row_count = rows.row_count();

        Ok(PreparedTableSnapshot {
            name: name.to_string(),
            table_identity: std::sync::Arc::clone(&state.identity),
            schema: state.schema.clone(),
            primary_key: state.primary_key.clone(),
            rows,
            row_count,
        })
    }

    /// Atomically install validated startup snapshots. Every table name and
    /// captured contract is checked before any live rows are replaced.
    pub(crate) fn install_prepared_snapshots(
        &mut self,
        snapshots: Vec<PreparedTableSnapshot>,
    ) -> Result<(), DbError> {
        let mut names = HashSet::with_capacity(snapshots.len());
        for snapshot in &snapshots {
            if !names.insert(snapshot.name.as_str()) {
                return Err(DbError::InvalidOperation(format!(
                    "reference-table snapshot '{}' is duplicated",
                    snapshot.name
                )));
            }
            let state = self
                .tables
                .get(&snapshot.name)
                .ok_or_else(|| DbError::TableNotFound(snapshot.name.clone()))?;
            if state.schema.as_ref() != snapshot.schema.as_ref() {
                return Err(DbError::SchemaMismatch(format!(
                    "reference-table '{}' changed schema while its snapshot was prepared",
                    snapshot.name
                )));
            }
            if state.primary_key != snapshot.primary_key {
                return Err(DbError::SchemaMismatch(format!(
                    "reference-table '{}' changed primary key while its snapshot was prepared",
                    snapshot.name
                )));
            }
            if !std::sync::Arc::ptr_eq(&state.identity, &snapshot.table_identity) {
                return Err(DbError::InvalidOperation(format!(
                    "reference-table '{}' was replaced while its snapshot was prepared",
                    snapshot.name
                )));
            }
            if snapshot.rows.row_count() != snapshot.row_count {
                return Err(DbError::Storage(format!(
                    "reference-table '{}' prepared snapshot row count is inconsistent",
                    snapshot.name
                )));
            }
        }

        for snapshot in snapshots {
            let state = self
                .tables
                .get_mut(&snapshot.name)
                .expect("prepared snapshot table contracts were validated before installation");
            state.rows = snapshot.rows;
            state.row_count = snapshot.row_count;
            state.ready = true;
        }
        Ok(())
    }

    fn checkpoint_capture_estimated_bytes(&self) -> Result<u64, DbError> {
        if self.tables.len() > MAX_REFERENCE_TABLES {
            return Err(DbError::Checkpoint(format!(
                "reference-table inventory contains {} tables; limit is {MAX_REFERENCE_TABLES}",
                self.tables.len()
            )));
        }

        let mut bytes = 0u64;
        for (name, state) in &self.tables {
            validate_checkpoint_identifier("name", name)?;
            validate_checkpoint_identifier("primary-key name", &state.primary_key)?;
            if state.row_count > MAX_REFERENCE_TABLE_ROWS {
                return Err(DbError::Checkpoint(format!(
                    "reference-table '{name}' contains {} rows; limit is {MAX_REFERENCE_TABLE_ROWS}",
                    state.row_count
                )));
            }
            if state.rows.row_count() != state.row_count {
                return Err(DbError::Checkpoint(format!(
                    "reference-table '{name}' row-count metadata is inconsistent"
                )));
            }
            add_checkpoint_capture_bytes(&mut bytes, CHECKPOINT_CAPTURE_TABLE_OVERHEAD, name)?;
            add_checkpoint_capture_bytes(&mut bytes, name.len(), name)?;
            add_checkpoint_capture_bytes(&mut bytes, state.primary_key.len(), name)?;
            for field in state.schema.fields() {
                add_checkpoint_capture_bytes(&mut bytes, CHECKPOINT_CAPTURE_FIELD_OVERHEAD, name)?;
                add_checkpoint_capture_bytes(&mut bytes, field.name().len(), name)?;
            }
            for (key, value) in state.schema.metadata() {
                add_checkpoint_capture_bytes(&mut bytes, key.len(), name)?;
                add_checkpoint_capture_bytes(&mut bytes, value.len(), name)?;
            }
            let row_bytes = state.rows.checkpoint_capture_estimated_bytes()?;
            // The immutable row slices remain live while one bounded concat chunk is assembled.
            // Reserving the full captured row size is conservative and covers even one
            // pathological variable-width row without a second user-facing tuning option.
            bytes = bytes
                .checked_add(row_bytes)
                .and_then(|bytes| bytes.checked_add(row_bytes))
                .ok_or_else(|| checkpoint_capture_size_overflow(name))?;
        }
        Ok(bytes)
    }

    /// Capture immutable row handles for off-lock checkpoint encoding.
    pub(crate) fn capture_checkpoint(
        &self,
        max_bytes: u64,
    ) -> Result<Option<ReferenceTableCheckpointCapture>, DbError> {
        if self.tables.is_empty() {
            return Ok(None);
        }
        let max_encoded_bytes = max_bytes.min(MAX_REFERENCE_TABLE_CHECKPOINT_BYTES as u64);
        let estimated_bytes = self.checkpoint_capture_estimated_bytes()?;
        if estimated_bytes > max_encoded_bytes {
            return Err(DbError::Checkpoint(format!(
                "reference-table checkpoint capture estimate {estimated_bytes} bytes exceeds its remaining staged-state budget of {max_encoded_bytes} bytes"
            )));
        }

        let mut names: Vec<_> = self.tables.keys().collect();
        names.sort_unstable();
        let mut tables = Vec::with_capacity(names.len());
        for name in names {
            validate_checkpoint_identifier("name", name)?;
            let state = &self.tables[name];
            validate_checkpoint_identifier("primary-key name", &state.primary_key)?;
            if state.row_count > MAX_REFERENCE_TABLE_ROWS {
                return Err(DbError::Checkpoint(format!(
                    "reference-table '{name}' contains {} rows; limit is {MAX_REFERENCE_TABLE_ROWS}",
                    state.row_count
                )));
            }
            let rows = state.rows.checkpoint_rows();
            if rows.len() != state.row_count {
                return Err(DbError::Checkpoint(format!(
                    "reference-table '{name}' row-count metadata is inconsistent"
                )));
            }
            tables.push(CapturedReferenceTable {
                name: name.clone(),
                primary_key: state.primary_key.clone(),
                schema: state.schema.clone(),
                rows,
                row_count: state.row_count,
            });
        }
        Ok(Some(ReferenceTableCheckpointCapture {
            tables,
            estimated_bytes,
        }))
    }

    /// Restore an exact table inventory atomically. Every archive entry is
    /// decoded into replacement rows before any live table is changed.
    /// `Ok(true)` means the checkpoint covered the complete non-empty catalog.
    pub(crate) fn restore_checkpoint(&mut self, encoded: &[u8]) -> Result<bool, DbError> {
        if encoded.is_empty() || encoded.len() > MAX_REFERENCE_TABLE_CHECKPOINT_BYTES {
            return Err(DbError::Checkpoint(format!(
                "reference-table checkpoint length {} is invalid",
                encoded.len()
            )));
        }
        if self.tables.is_empty() {
            return Err(DbError::Checkpoint(
                "reference-table checkpoint exists for an empty table catalog".into(),
            ));
        }

        let aligned;
        let encoded = if encoded
            .as_ptr()
            .align_offset(REFERENCE_TABLE_ARCHIVE_ALIGNMENT)
            == 0
        {
            encoded
        } else {
            let mut copy = rkyv::util::AlignedVec::<16>::with_capacity(encoded.len());
            copy.extend_from_slice(encoded);
            aligned = copy;
            &aligned
        };
        let archived = rkyv::access::<
            <ReferenceTableCheckpointArchive as rkyv::Archive>::Archived,
            rkyv::rancor::Error,
        >(encoded)
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "reference-table checkpoint archive is corrupt: {error}"
            ))
        })?;
        if archived.tables.is_empty() || archived.tables.len() > MAX_REFERENCE_TABLES {
            return Err(DbError::Checkpoint(format!(
                "reference-table checkpoint table count {} is outside 1..={MAX_REFERENCE_TABLES}",
                archived.tables.len()
            )));
        }
        let archive: ReferenceTableCheckpointArchive =
            rkyv::from_bytes::<ReferenceTableCheckpointArchive, rkyv::rancor::Error>(encoded)
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "reference-table checkpoint archive is corrupt: {error}"
                    ))
                })?;
        if archive.version != REFERENCE_TABLE_CHECKPOINT_VERSION {
            return Err(DbError::Checkpoint(format!(
                "unsupported reference-table checkpoint version {}; expected {}",
                archive.version, REFERENCE_TABLE_CHECKPOINT_VERSION
            )));
        }
        let mut expected_names: Vec<_> = self.tables.keys().cloned().collect();
        expected_names.sort_unstable();
        let mut archived_names = Vec::with_capacity(archive.tables.len());
        let mut previous_name: Option<&str> = None;
        for entry in &archive.tables {
            validate_checkpoint_identifier("name", &entry.name)?;
            validate_checkpoint_identifier("primary-key name", &entry.primary_key)?;
            if previous_name.is_some_and(|previous| previous >= entry.name.as_str()) {
                return Err(DbError::Checkpoint(
                    "reference-table checkpoint names are duplicated or not canonical".into(),
                ));
            }
            previous_name = Some(&entry.name);
            archived_names.push(entry.name.clone());
        }
        if archived_names != expected_names {
            return Err(DbError::Checkpoint(format!(
                "reference-table checkpoint inventory mismatch: expected {expected_names:?}, found {archived_names:?}"
            )));
        }

        let mut payload_bytes = 0usize;
        let mut replacements = HashMap::with_capacity(archive.tables.len());
        for entry in archive.tables {
            payload_bytes = payload_bytes.checked_add(entry.ipc.len()).ok_or_else(|| {
                DbError::Checkpoint("reference-table checkpoint size overflow".into())
            })?;
            if payload_bytes > MAX_REFERENCE_TABLE_CHECKPOINT_BYTES {
                return Err(DbError::Checkpoint(format!(
                    "reference-table checkpoint payload exceeds the {MAX_REFERENCE_TABLE_CHECKPOINT_BYTES} byte limit"
                )));
            }

            let state = &self.tables[&entry.name];
            if entry.primary_key != state.primary_key {
                return Err(DbError::Checkpoint(format!(
                    "reference-table '{}' primary key mismatch: expected '{}', found '{}'",
                    entry.name, state.primary_key, entry.primary_key
                )));
            }
            let row_count = usize::try_from(entry.row_count).map_err(|_| {
                DbError::Checkpoint(format!(
                    "reference-table '{}' row count does not fit this runtime",
                    entry.name
                ))
            })?;
            if row_count > MAX_REFERENCE_TABLE_ROWS {
                return Err(DbError::Checkpoint(format!(
                    "reference-table '{}' row count {row_count} exceeds {MAX_REFERENCE_TABLE_ROWS}",
                    entry.name
                )));
            }
            let batches = decode_checkpoint_batches(&entry.name, &entry.ipc, row_count)?;
            let decoded_rows = batches.iter().try_fold(0usize, |total, batch| {
                total.checked_add(batch.num_rows()).ok_or_else(|| {
                    DbError::Checkpoint("reference-table checkpoint row count overflow".into())
                })
            })?;
            if decoded_rows != row_count {
                return Err(DbError::Checkpoint(format!(
                    "reference-table '{}' row count mismatch: archive says {row_count}, IPC contains {}",
                    entry.name,
                    decoded_rows
                )));
            }
            for batch in &batches {
                if batch.schema().as_ref() != state.schema.as_ref() {
                    return Err(DbError::Checkpoint(format!(
                        "reference-table '{}' checkpoint schema differs from the catalog schema",
                        entry.name
                    )));
                }
                for (index, field) in state.schema.fields().iter().enumerate() {
                    if !field.is_nullable() && batch.column(index).null_count() != 0 {
                        return Err(DbError::Checkpoint(format!(
                            "reference-table '{}' checkpoint contains NULL values in non-nullable column '{}'",
                            entry.name,
                            field.name()
                        )));
                    }
                }
            }
            let rows = TableRows::from_batches(&batches, state.pk_index, &state.key_converter)
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "reference-table '{}' checkpoint key validation failed: {error}",
                        entry.name
                    ))
                })?;
            replacements.insert(entry.name, (rows, row_count));
        }

        debug_assert_eq!(replacements.len(), self.tables.len());
        for (name, state) in &mut self.tables {
            let (rows, row_count) = replacements
                .remove(name)
                .expect("checkpoint inventory was validated before installation");
            state.rows = rows;
            state.row_count = row_count;
            state.ready = true;
        }
        Ok(true)
    }

    /// Upsert rows from a `RecordBatch`, keyed by the primary key column.
    ///
    /// # Errors
    ///
    /// Returns an error if the table does not exist.
    pub fn upsert(&mut self, name: &str, batch: &RecordBatch) -> Result<usize, DbError> {
        let state = self
            .tables
            .get_mut(name)
            .ok_or_else(|| DbError::TableNotFound(name.to_string()))?;

        validate_batch_contract(name, state, batch)?;
        let pk_col = batch.column(state.pk_index);
        let keys = state
            .key_converter
            .convert_columns(&[std::sync::Arc::clone(pk_col)])
            .map_err(|error| {
                DbError::InsertError(format!(
                    "failed to encode primary key for table '{name}': {error}"
                ))
            })?;
        let count = batch.num_rows();

        for i in 0..count {
            let row = batch.slice(i, 1);
            let existed = state.rows.put(keys.row(i).owned(), row);
            if !existed {
                state.row_count += 1;
            }
        }

        Ok(count)
    }

    pub fn to_record_batch(&self, name: &str) -> Result<Option<RecordBatch>, DbError> {
        let Some(state) = self.tables.get(name) else {
            return Ok(None);
        };
        state.rows.to_record_batch(&state.schema)
    }
}

fn validate_batch_contract(
    name: &str,
    state: &TableState,
    batch: &RecordBatch,
) -> Result<(), DbError> {
    if batch.schema().as_ref() != state.schema.as_ref() {
        return Err(DbError::SchemaMismatch(format!(
            "table '{name}' batch schema differs from its declared schema"
        )));
    }
    for (index, field) in state.schema.fields().iter().enumerate() {
        if !field.is_nullable() && batch.column(index).null_count() != 0 {
            let contract = if index == state.pk_index {
                format!("primary key '{}'", state.primary_key)
            } else {
                format!("non-nullable column '{}'", field.name())
            };
            return Err(DbError::InsertError(format!(
                "table '{name}' {contract} contains NULL"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
        ]))
    }

    fn make_batch(ids: &[i32], names: &[&str], prices: &[f64]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(Float64Array::from(prices.to_vec())),
            ],
        )
        .unwrap()
    }

    fn checkpoint_bytes(store: &TableStore) -> bytes::Bytes {
        store
            .capture_checkpoint(u64::MAX)
            .unwrap()
            .expect("non-empty table inventory")
            .encode(u64::MAX)
            .unwrap()
            .0
    }

    fn rewrite_archive(
        bytes: &[u8],
        mutate: impl FnOnce(&mut ReferenceTableCheckpointArchive),
    ) -> Vec<u8> {
        let mut archive =
            rkyv::from_bytes::<ReferenceTableCheckpointArchive, rkyv::rancor::Error>(bytes)
                .unwrap();
        mutate(&mut archive);
        rkyv::to_bytes::<rkyv::rancor::Error>(&archive)
            .unwrap()
            .to_vec()
    }

    #[test]
    fn test_create_table_validates_pk() {
        let mut store = TableStore::new();
        let result = store.create_table("t", test_schema(), "id");
        assert!(result.is_ok());
        assert!(store.has_table("t"));
    }

    #[test]
    fn test_create_table_rejects_missing_pk() {
        let mut store = TableStore::new();
        let result = store.create_table("t", test_schema(), "nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_create_table_rejects_nullable_pk() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let mut store = TableStore::new();
        let result = store.create_table("t", schema, "id");
        assert!(matches!(result, Err(DbError::InvalidOperation(_))));
        assert!(!store.has_table("t"));
    }

    #[test]
    fn test_create_table_rejects_duplicate() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        let result = store.create_table("t", test_schema(), "id");
        assert!(matches!(result, Err(DbError::TableAlreadyExists(_))));
    }

    #[test]
    fn test_upsert_and_scan() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();

        let batch = make_batch(&[1], &["Widget"], &[9.99]);
        let count = store.upsert("t", &batch).unwrap();
        assert_eq!(count, 1);
        assert_eq!(store.table_row_count("t"), 1);

        let row = store.to_record_batch("t").unwrap().unwrap();
        assert_eq!(row.num_rows(), 1);
        let names = row
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Widget");
    }

    #[test]
    fn test_upsert_multiple_rows() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();

        let batch = make_batch(&[1, 2, 3], &["A", "B", "C"], &[1.0, 2.0, 3.0]);
        let count = store.upsert("t", &batch).unwrap();
        assert_eq!(count, 3);
        assert_eq!(store.table_row_count("t"), 3);
    }

    #[test]
    fn test_upsert_schema_mismatch_is_atomic() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        store
            .upsert("t", &make_batch(&[1], &["original"], &[1.0]))
            .unwrap();

        let incompatible_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let incompatible = RecordBatch::try_new(
            incompatible_schema,
            vec![
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["bad"])),
            ],
        )
        .unwrap();
        assert!(matches!(
            store.upsert("t", &incompatible),
            Err(DbError::SchemaMismatch(_))
        ));

        let snapshot = store.to_record_batch("t").unwrap().unwrap();
        assert_eq!(snapshot.num_rows(), 1);
        assert_eq!(store.table_row_count("t"), 1);
    }

    #[test]
    fn upsert_rejects_null_in_non_nullable_column_without_mutation() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        store
            .upsert("t", &make_batch(&[1], &["original"], &[1.0]))
            .unwrap();
        // SAFETY: field count, data types, and array lengths match. The deliberately invalid
        // nullability contract models a corrupted or foreign Arrow producer.
        let invalid = unsafe {
            RecordBatch::new_unchecked(
                test_schema(),
                vec![
                    Arc::new(Int32Array::from(vec![2, 3])),
                    Arc::new(StringArray::from(vec![Some("valid"), None])),
                    Arc::new(Float64Array::from(vec![2.0, 3.0])),
                ],
                2,
            )
        };

        let error = store.upsert("t", &invalid).unwrap_err();
        assert!(error.to_string().contains("non-nullable column 'name'"));
        assert_eq!(store.table_row_count("t"), 1);
        let snapshot = store.to_record_batch("t").unwrap().unwrap();
        let names = snapshot
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "original");
    }

    #[test]
    fn test_upsert_overwrites_existing() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();

        let batch1 = make_batch(&[1], &["Old"], &[1.0]);
        store.upsert("t", &batch1).unwrap();

        let batch2 = make_batch(&[1], &["New"], &[2.0]);
        store.upsert("t", &batch2).unwrap();

        assert_eq!(store.table_row_count("t"), 1);
        let row = store.to_record_batch("t").unwrap().unwrap();
        let names = row
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "New");
    }

    #[test]
    fn test_table_names_and_counts() {
        let mut store = TableStore::new();
        assert!(store.table_names().is_empty());

        store.create_table("a", test_schema(), "id").unwrap();
        store.create_table("b", test_schema(), "id").unwrap();

        let mut names = store.table_names();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
        assert!(store.has_table("a"));
        assert!(!store.has_table("c"));
    }

    #[test]
    fn test_to_record_batch() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();

        // Empty table returns empty batch
        let batch = store.to_record_batch("t").unwrap().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), test_schema());

        // With data
        store
            .upsert("t", &make_batch(&[1, 2], &["A", "B"], &[1.0, 2.0]))
            .unwrap();
        let batch = store.to_record_batch("t").unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);

        // Missing table
        assert!(store.to_record_batch("nosuch").unwrap().is_none());
    }

    #[test]
    fn test_drop_table() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        assert!(store.drop_table("t"));
        assert!(!store.has_table("t"));
        assert!(!store.drop_table("t"));
    }

    #[test]
    fn test_ready_flag() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        assert!(!store.is_ready("t"));

        store.set_ready("t", true);
        assert!(store.is_ready("t"));

        store.set_ready("t", false);
        assert!(!store.is_ready("t"));
    }

    #[test]
    fn test_connector_tracking() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        assert!(store.connector("t").is_none());

        store.set_connector("t", "kafka");
        assert_eq!(store.connector("t"), Some("kafka"));
    }

    #[test]
    fn test_row_count_tracks_upserts() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        assert_eq!(store.table_row_count("t"), 0);

        store
            .upsert("t", &make_batch(&[1, 2], &["A", "B"], &[1.0, 2.0]))
            .unwrap();
        assert_eq!(store.table_row_count("t"), 2);

        // Upsert existing key — count should not increase
        store
            .upsert("t", &make_batch(&[1], &["X"], &[9.0]))
            .unwrap();
        assert_eq!(store.table_row_count("t"), 2);
    }

    #[test]
    fn prepared_snapshot_rejects_duplicate_keys_across_batches_without_mutation() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        store
            .upsert("t", &make_batch(&[9], &["stale"], &[9.0]))
            .unwrap();
        let before = checkpoint_bytes(&store);
        let batches = vec![
            make_batch(&[1], &["first"], &[1.0]),
            make_batch(&[1], &["duplicate"], &[2.0]),
        ];

        let error = store
            .prepare_snapshot("t", &batches)
            .err()
            .expect("duplicate keys must reject the prepared snapshot");

        assert!(
            error.to_string().contains("duplicate primary keys"),
            "{error}"
        );
        assert_eq!(checkpoint_bytes(&store), before);
        assert!(!store.is_ready("t"));
    }

    #[test]
    fn one_invalid_prepared_table_prevents_every_replacement() {
        let mut target = TableStore::new();
        target.create_table("a", test_schema(), "id").unwrap();
        target.create_table("b", test_schema(), "id").unwrap();
        target
            .upsert("a", &make_batch(&[10], &["old-a"], &[10.0]))
            .unwrap();
        target
            .upsert("b", &make_batch(&[20], &["old-b"], &[20.0]))
            .unwrap();
        let prepared = vec![
            target
                .prepare_snapshot("a", &[make_batch(&[1], &["new-a"], &[1.0])])
                .unwrap(),
            target
                .prepare_snapshot("b", &[make_batch(&[2], &["new-b"], &[2.0])])
                .unwrap(),
        ];

        let incompatible_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("description", DataType::Utf8, false),
        ]));
        assert!(target.drop_table("b"));
        target
            .create_table("b", incompatible_schema.clone(), "id")
            .unwrap();
        let old_b = RecordBatch::try_new(
            incompatible_schema,
            vec![
                Arc::new(Int32Array::from(vec![20])),
                Arc::new(StringArray::from(vec!["old-b"])),
            ],
        )
        .unwrap();
        target.upsert("b", &old_b).unwrap();
        let before = checkpoint_bytes(&target);

        let error = target.install_prepared_snapshots(prepared).unwrap_err();

        assert!(matches!(error, DbError::SchemaMismatch(_)));
        assert_eq!(checkpoint_bytes(&target), before);
        assert!(!target.is_ready("a"));
        assert!(!target.is_ready("b"));
    }

    #[test]
    fn empty_prepared_snapshot_clears_stale_rows_and_marks_ready() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        store
            .upsert("t", &make_batch(&[9], &["stale"], &[9.0]))
            .unwrap();

        let prepared = store.prepare_snapshot("t", &[]).unwrap();
        store.install_prepared_snapshots(vec![prepared]).unwrap();

        assert_eq!(store.table_row_count("t"), 0);
        assert_eq!(store.to_record_batch("t").unwrap().unwrap().num_rows(), 0);
        assert!(store.is_ready("t"));
    }

    #[test]
    fn successful_multi_table_snapshot_install_is_atomic_and_ready() {
        let mut store = TableStore::new();
        store.create_table("a", test_schema(), "id").unwrap();
        store.create_table("b", test_schema(), "id").unwrap();
        let prepared = vec![
            store
                .prepare_snapshot(
                    "a",
                    &[
                        make_batch(&[1], &["a-1"], &[1.0]),
                        make_batch(&[2], &["a-2"], &[2.0]),
                    ],
                )
                .unwrap(),
            store
                .prepare_snapshot("b", &[make_batch(&[3], &["b-3"], &[3.0])])
                .unwrap(),
        ];

        store.install_prepared_snapshots(prepared).unwrap();

        assert_eq!(store.table_row_count("a"), 2);
        assert_eq!(store.table_row_count("b"), 1);
        assert!(store.is_ready("a"));
        assert!(store.is_ready("b"));
    }

    #[test]
    fn duplicate_prepared_table_names_fail_without_mutation() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        store
            .upsert("t", &make_batch(&[9], &["stale"], &[9.0]))
            .unwrap();
        let before = checkpoint_bytes(&store);
        let prepared = vec![
            store
                .prepare_snapshot("t", &[make_batch(&[1], &["first"], &[1.0])])
                .unwrap(),
            store
                .prepare_snapshot("t", &[make_batch(&[2], &["second"], &[2.0])])
                .unwrap(),
        ];

        let error = store.install_prepared_snapshots(prepared).unwrap_err();

        assert!(error.to_string().contains("duplicated"), "{error}");
        assert_eq!(checkpoint_bytes(&store), before);
        assert!(!store.is_ready("t"));
    }

    #[test]
    fn checkpoint_round_trip_is_deterministic_and_marks_complete_inventory_ready() {
        let mut source = TableStore::new();
        source.create_table("b", test_schema(), "id").unwrap();
        source.create_table("a", test_schema(), "id").unwrap();
        source
            .upsert("a", &make_batch(&[2, 1], &["A2", "A1"], &[2.0, 1.0]))
            .unwrap();
        source
            .upsert("b", &make_batch(&[4, 3], &["B4", "B3"], &[4.0, 3.0]))
            .unwrap();

        let first = checkpoint_bytes(&source);
        let second = checkpoint_bytes(&source);
        assert_eq!(first, second, "checkpoint bytes must be canonical");

        let mut restored = TableStore::new();
        restored.create_table("a", test_schema(), "id").unwrap();
        restored.create_table("b", test_schema(), "id").unwrap();
        restored
            .upsert("a", &make_batch(&[99], &["stale"], &[99.0]))
            .unwrap();
        restored
            .upsert("b", &make_batch(&[98], &["stale"], &[98.0]))
            .unwrap();

        assert!(restored.restore_checkpoint(&first).unwrap());
        assert_eq!(restored.table_row_count("a"), 2);
        assert_eq!(restored.table_row_count("b"), 2);
        assert!(restored.is_ready("a"));
        assert!(restored.is_ready("b"));
        assert_eq!(checkpoint_bytes(&restored), first);
    }

    #[test]
    fn checkpoint_capture_cap_rejection_preserves_live_state() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        store
            .upsert("t", &make_batch(&[1, 2], &["first", "second"], &[1.0, 2.0]))
            .unwrap();
        let before = checkpoint_bytes(&store);
        let estimated_bytes = store.checkpoint_capture_estimated_bytes().unwrap();
        assert!(estimated_bytes > 0);

        let error = store
            .capture_checkpoint(estimated_bytes - 1)
            .err()
            .expect("capture above the remaining checkpoint budget must fail");

        assert!(error.to_string().contains("staged-state budget"), "{error}");
        assert_eq!(store.table_row_count("t"), 2);
        assert_eq!(checkpoint_bytes(&store), before);
    }

    #[test]
    fn checkpoint_encoding_enforces_the_worker_remaining_budget() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        store
            .upsert("t", &make_batch(&[1], &["value"], &[1.0]))
            .unwrap();
        let before = checkpoint_bytes(&store);
        let capture = store
            .capture_checkpoint(u64::MAX)
            .unwrap()
            .expect("non-empty table inventory");
        assert_eq!(
            capture.estimated_bytes(),
            store.checkpoint_capture_estimated_bytes().unwrap()
        );

        let error = capture
            .encode(u64::try_from(before.len()).unwrap() - 1)
            .unwrap_err();

        assert!(error.to_string().contains("staged-state budget"), "{error}");
        assert_eq!(store.table_row_count("t"), 1);
        assert_eq!(checkpoint_bytes(&store), before);
    }

    #[test]
    fn checkpoint_capture_is_a_point_in_time_image() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        store
            .upsert("t", &make_batch(&[1], &["old"], &[1.0]))
            .unwrap();
        let capture = store
            .capture_checkpoint(u64::MAX)
            .unwrap()
            .expect("non-empty table inventory");

        store
            .upsert("t", &make_batch(&[1], &["new"], &[2.0]))
            .unwrap();
        let (encoded, retained_bytes) = capture.encode(u64::MAX).unwrap();
        assert!(retained_bytes >= encoded.len() as u64);
        let mut restored = TableStore::new();
        restored.create_table("t", test_schema(), "id").unwrap();
        restored.restore_checkpoint(&encoded).unwrap();

        let restored_batch = restored.to_record_batch("t").unwrap().unwrap();
        let restored_names = restored_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(restored_names.value(0), "old");
        let live_batch = store.to_record_batch("t").unwrap().unwrap();
        let live_names = live_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(live_names.value(0), "new");
    }

    #[test]
    fn checkpoint_chunks_large_tables_and_restores_every_row() {
        let row_count = REFERENCE_TABLE_CHECKPOINT_CHUNK_ROWS + 904;
        let ids: Vec<i32> = (0..i32::try_from(row_count).unwrap()).collect();
        let names: Vec<String> = ids.iter().map(|id| format!("row-{id}")).collect();
        let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let prices: Vec<f64> = ids.iter().map(|id| f64::from(*id)).collect();

        let mut source = TableStore::new();
        source.create_table("t", test_schema(), "id").unwrap();
        source
            .upsert("t", &make_batch(&ids, &name_refs, &prices))
            .unwrap();
        let checkpoint = checkpoint_bytes(&source);

        let archive =
            rkyv::from_bytes::<ReferenceTableCheckpointArchive, rkyv::rancor::Error>(&checkpoint)
                .unwrap();
        let batches = decode_checkpoint_batches("t", &archive.tables[0].ipc, row_count).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), REFERENCE_TABLE_CHECKPOINT_CHUNK_ROWS);
        assert_eq!(batches[1].num_rows(), 904);

        let mut restored = TableStore::new();
        restored.create_table("t", test_schema(), "id").unwrap();
        assert!(restored.restore_checkpoint(&checkpoint).unwrap());
        assert_eq!(restored.table_row_count("t"), row_count);
        assert_eq!(checkpoint_bytes(&restored), checkpoint);
    }

    #[test]
    fn empty_inventory_emits_no_checkpoint_state() {
        assert!(TableStore::new().capture_checkpoint(0).unwrap().is_none());
    }

    #[test]
    fn restore_accepts_an_unaligned_checkpoint_buffer() {
        let mut source = TableStore::new();
        source.create_table("a", test_schema(), "id").unwrap();
        source
            .upsert("a", &make_batch(&[1], &["value"], &[1.0]))
            .unwrap();
        let checkpoint = checkpoint_bytes(&source);

        const ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;
        let mut transport = vec![0_u8; checkpoint.len() + ALIGNMENT];
        let offset = (0..ALIGNMENT)
            .find(|offset| !(transport.as_ptr() as usize + offset).is_multiple_of(ALIGNMENT))
            .unwrap();
        transport[offset..offset + checkpoint.len()].copy_from_slice(&checkpoint);
        let unaligned = &transport[offset..offset + checkpoint.len()];
        assert_ne!(unaligned.as_ptr().align_offset(ALIGNMENT), 0);

        let mut restored = TableStore::new();
        restored.create_table("a", test_schema(), "id").unwrap();
        assert!(restored.restore_checkpoint(unaligned).unwrap());
        assert_eq!(restored.table_row_count("a"), 1);
    }

    #[test]
    fn restore_rejects_corrupt_or_partial_inventory_without_mutation() {
        let mut source = TableStore::new();
        source.create_table("a", test_schema(), "id").unwrap();
        source.create_table("b", test_schema(), "id").unwrap();
        source
            .upsert("a", &make_batch(&[1], &["new-a"], &[1.0]))
            .unwrap();
        source
            .upsert("b", &make_batch(&[2], &["new-b"], &[2.0]))
            .unwrap();
        let valid = checkpoint_bytes(&source);

        let mut target = TableStore::new();
        target.create_table("a", test_schema(), "id").unwrap();
        target.create_table("b", test_schema(), "id").unwrap();
        target
            .upsert("a", &make_batch(&[10], &["old-a"], &[10.0]))
            .unwrap();
        target
            .upsert("b", &make_batch(&[20], &["old-b"], &[20.0]))
            .unwrap();
        let before = checkpoint_bytes(&target);

        let partial = rewrite_archive(&valid, |archive| {
            archive.tables.pop();
        });
        assert!(target.restore_checkpoint(&partial).is_err());
        assert_eq!(checkpoint_bytes(&target), before);

        let wrong_version = rewrite_archive(&valid, |archive| {
            archive.version += 1;
        });
        assert!(target.restore_checkpoint(&wrong_version).is_err());
        assert_eq!(checkpoint_bytes(&target), before);

        assert!(target.restore_checkpoint(b"not an rkyv archive").is_err());
        assert_eq!(checkpoint_bytes(&target), before);
    }

    #[test]
    fn restore_prepares_every_table_before_installing_any() {
        let mut source = TableStore::new();
        source.create_table("a", test_schema(), "id").unwrap();
        source.create_table("b", test_schema(), "id").unwrap();
        source
            .upsert("a", &make_batch(&[1], &["new-a"], &[1.0]))
            .unwrap();
        source
            .upsert("b", &make_batch(&[2], &["new-b"], &[2.0]))
            .unwrap();
        let valid = checkpoint_bytes(&source);

        let incompatible_schema = Arc::new(Schema::new(vec![Field::new(
            "wrong",
            DataType::Int32,
            false,
        )]));
        let incompatible_batch = RecordBatch::try_new(
            incompatible_schema,
            vec![Arc::new(Int32Array::from(vec![2]))],
        )
        .unwrap();
        let incompatible_ipc =
            laminar_core::serialization::serialize_batch_stream(&incompatible_batch).unwrap();
        let invalid = rewrite_archive(&valid, |archive| {
            let second = archive.tables.get_mut(1).unwrap();
            second.ipc = incompatible_ipc;
            second.row_count = 1;
        });

        let mut target = TableStore::new();
        target.create_table("a", test_schema(), "id").unwrap();
        target.create_table("b", test_schema(), "id").unwrap();
        target
            .upsert("a", &make_batch(&[10], &["old-a"], &[10.0]))
            .unwrap();
        target
            .upsert("b", &make_batch(&[20], &["old-b"], &[20.0]))
            .unwrap();
        let before = checkpoint_bytes(&target);

        let error = target.restore_checkpoint(&invalid).unwrap_err();
        assert!(error.to_string().contains("schema differs"), "{error}");
        assert_eq!(checkpoint_bytes(&target), before);
    }

    #[test]
    fn restore_rejects_duplicate_primary_keys() {
        let mut source = TableStore::new();
        source.create_table("a", test_schema(), "id").unwrap();
        source
            .upsert("a", &make_batch(&[1], &["valid"], &[1.0]))
            .unwrap();
        let valid = checkpoint_bytes(&source);
        let duplicate_batch = make_batch(&[7, 7], &["first", "second"], &[1.0, 2.0]);
        let duplicate_ipc =
            laminar_core::serialization::serialize_batch_stream(&duplicate_batch).unwrap();
        let invalid = rewrite_archive(&valid, |archive| {
            archive.tables[0].ipc = duplicate_ipc;
            archive.tables[0].row_count = 2;
        });

        let mut target = TableStore::new();
        target.create_table("a", test_schema(), "id").unwrap();
        target
            .upsert("a", &make_batch(&[10], &["old"], &[10.0]))
            .unwrap();
        let before = checkpoint_bytes(&target);
        let error = target.restore_checkpoint(&invalid).unwrap_err();
        assert!(
            error.to_string().contains("duplicate primary keys"),
            "{error}"
        );
        assert_eq!(checkpoint_bytes(&target), before);
    }

    #[test]
    fn restore_rejects_null_in_non_nullable_column_without_mutation() {
        let mut source = TableStore::new();
        source.create_table("a", test_schema(), "id").unwrap();
        source
            .upsert("a", &make_batch(&[1], &["valid"], &[1.0]))
            .unwrap();
        let valid = checkpoint_bytes(&source);
        // SAFETY: field count, data types, and array lengths match. Nullability is violated
        // intentionally to exercise checkpoint corruption handling.
        let null_batch = unsafe {
            RecordBatch::new_unchecked(
                test_schema(),
                vec![
                    Arc::new(Int32Array::from(vec![7])),
                    Arc::new(StringArray::from(vec![None::<&str>])),
                    Arc::new(Float64Array::from(vec![7.0])),
                ],
                1,
            )
        };
        let null_ipc = laminar_core::serialization::serialize_batch_stream(&null_batch).unwrap();
        let invalid = rewrite_archive(&valid, |archive| {
            archive.tables[0].ipc = null_ipc;
            archive.tables[0].row_count = 1;
        });

        let mut target = TableStore::new();
        target.create_table("a", test_schema(), "id").unwrap();
        target
            .upsert("a", &make_batch(&[10], &["old"], &[10.0]))
            .unwrap();
        let before = checkpoint_bytes(&target);
        let error = target.restore_checkpoint(&invalid).unwrap_err();
        assert!(
            error.to_string().contains("non-nullable") && error.to_string().contains("name"),
            "{error}"
        );
        assert_eq!(checkpoint_bytes(&target), before);
    }
}
