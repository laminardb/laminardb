//! Live-table accounting, separate from logical checkpoint encoding estimates.

use arrow::array::{ArrayData, ArrayRef, RecordBatch};
use arrow::buffer::Buffer;
use rustc_hash::FxHashMap;

use crate::error::DbError;

#[derive(Clone, Copy)]
pub(crate) struct TableLimits {
    pub rows: usize,
    pub bytes: usize,
}

impl TableLimits {
    pub fn validate(self, table: &str, rows: usize, bytes: usize) -> Result<(), DbError> {
        if rows > self.rows || bytes > self.bytes {
            return Err(DbError::ReferenceTableQuotaExceeded {
                table: table.to_owned(),
                rows,
                bytes,
                max_rows: self.rows,
                max_bytes: self.bytes,
            });
        }
        Ok(())
    }
}

pub(super) struct BatchRetention {
    allocations: Vec<(usize, usize)>,
    pub row_bytes: usize,
}

impl BatchRetention {
    pub fn new(batch: &RecordBatch) -> Result<Self, DbError> {
        let mut allocations = FxHashMap::default();
        let mut metadata = std::mem::size_of::<(arrow::row::OwnedRow, super::StoredRow)>();
        for column in batch.columns() {
            // Metadata is conservatively charged for every row, even where Arrow shares it.
            let array_metadata = column
                .get_array_memory_size()
                .checked_sub(column.get_buffer_memory_size())
                .ok_or_else(size_overflow)?;
            add(&mut metadata, array_metadata)?;
            add(&mut metadata, std::mem::size_of::<ArrayRef>())?;
            collect_allocations(&column.to_data(), &mut allocations, &mut metadata)?;
        }
        let allocations: Vec<_> = allocations.into_iter().collect();
        // The shared descriptor is also conservatively charged per row, including capacity.
        add(&mut metadata, std::mem::size_of::<Self>())?;
        add(
            &mut metadata,
            allocations
                .capacity()
                .checked_mul(std::mem::size_of::<(usize, usize)>())
                .ok_or_else(size_overflow)?,
        )?;
        Ok(Self {
            allocations,
            row_bytes: metadata,
        })
    }
}

fn collect_allocations(
    data: &ArrayData,
    allocations: &mut FxHashMap<usize, usize>,
    metadata: &mut usize,
) -> Result<(), DbError> {
    for buffer in data.buffers() {
        collect_buffer(buffer, allocations)?;
        add(metadata, std::mem::size_of::<Buffer>())?;
    }
    if let Some(nulls) = data.nulls() {
        collect_buffer(nulls.buffer(), allocations)?;
    }
    for child in data.child_data() {
        collect_allocations(child, allocations, metadata)?;
    }
    Ok(())
}

fn collect_buffer(
    buffer: &Buffer,
    allocations: &mut FxHashMap<usize, usize>,
) -> Result<(), DbError> {
    let bytes = buffer.capacity();
    if bytes == 0 {
        if buffer.is_empty() {
            return Ok(());
        }
        // A nonempty buffer must expose an allocation extent for admission.
        return Err(DbError::Storage(
            "reference-table Arrow buffer has no accountable capacity".into(),
        ));
    }
    let end = buffer
        .ptr_offset()
        .checked_add(buffer.len())
        .ok_or_else(size_overflow)?;
    if end > bytes {
        return Err(DbError::Storage(
            "reference-table Arrow buffer exceeds its reported capacity".into(),
        ));
    }
    allocations
        .entry(buffer.data_ptr().as_ptr() as usize)
        .and_modify(|size| *size = (*size).max(bytes))
        .or_insert(bytes);
    Ok(())
}

#[derive(Clone, Copy, Default)]
pub(super) struct BufferUsage {
    pub references: usize,
    bytes: usize,
}

#[derive(Default)]
struct BufferDelta {
    added: usize,
    removed: usize,
    bytes: usize,
}

pub(super) struct RetentionDelta {
    bytes: usize,
    buffers: FxHashMap<usize, BufferDelta>,
}

impl RetentionDelta {
    pub fn new(bytes: usize) -> Self {
        Self {
            bytes,
            buffers: FxHashMap::default(),
        }
    }

    pub fn remove_row(&mut self, key_bytes: usize, metadata: usize) -> Result<(), DbError> {
        self.bytes = self
            .bytes
            .checked_sub(key_bytes)
            .and_then(|bytes| bytes.checked_sub(metadata))
            .ok_or_else(size_overflow)?;
        Ok(())
    }

    pub fn add_row(&mut self, key_bytes: usize, metadata: usize) -> Result<(), DbError> {
        add(&mut self.bytes, key_bytes)?;
        add(&mut self.bytes, metadata)
    }

    pub fn remove_batch(
        &mut self,
        batch: &BatchRetention,
        references: usize,
    ) -> Result<(), DbError> {
        for &(identity, _) in &batch.allocations {
            add(
                &mut self.buffers.entry(identity).or_default().removed,
                references,
            )?;
        }
        Ok(())
    }

    pub fn add_batch(&mut self, batch: &BatchRetention, references: usize) -> Result<(), DbError> {
        for &(identity, bytes) in &batch.allocations {
            let delta = self.buffers.entry(identity).or_default();
            add(&mut delta.added, references)?;
            delta.bytes = delta.bytes.max(bytes);
        }
        Ok(())
    }

    pub fn prepare(
        mut self,
        live: &FxHashMap<usize, BufferUsage>,
    ) -> Result<(usize, FxHashMap<usize, BufferUsage>), DbError> {
        let mut prepared = FxHashMap::default();
        for (identity, delta) in self.buffers {
            let old = live.get(&identity).copied().unwrap_or_default();
            let references = old
                .references
                .checked_sub(delta.removed)
                .and_then(|count| count.checked_add(delta.added))
                .ok_or_else(size_overflow)?;
            let bytes = if references == 0 {
                0
            } else {
                old.bytes.max(delta.bytes)
            };
            self.bytes = self
                .bytes
                .checked_sub(old.bytes)
                .ok_or_else(size_overflow)?;
            add(&mut self.bytes, bytes)?;
            prepared.insert(identity, BufferUsage { references, bytes });
        }
        Ok((self.bytes, prepared))
    }
}

fn add(total: &mut usize, bytes: usize) -> Result<(), DbError> {
    *total = total.checked_add(bytes).ok_or_else(size_overflow)?;
    Ok(())
}

pub(super) fn size_overflow() -> DbError {
    DbError::Storage("reference-table retained-memory accounting overflow".into())
}
