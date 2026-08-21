//! Borrowed archive validation and restore-memory accounting.

use super::ipc::preflight_scalar_ipc_restore;
use super::{
    AggStateCheckpoint, ArchivedAggStateCheckpoint, ArchivedEmittedCheckpoint, EmittedCheckpoint,
    IpcRestorePreflight, MAX_ACCUMULATOR_STATE_COLUMNS, RESTORE_CELL_SCRATCH_CHARGE,
    RESTORE_IPC_EXPANSION_FACTOR, RESTORE_ROW_SCRATCH_CHARGE,
};
use crate::error::DbError;

/// Query-plan limits for borrowed aggregate archive validation.
#[derive(Clone, Copy)]
pub(crate) struct AggStateArchiveRestoreProfile {
    fingerprint: u64,
    accumulator_states: usize,
    max_groups: usize,
    group_columns: usize,
    output_columns: usize,
    emit_changelog: bool,
}

/// Allocation bounds derived without materializing the archived checkpoint or any Arrow array.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct AggStateRestorePreflight {
    group_count: usize,
    owned_state_bytes: usize,
    final_state_upper_bytes: usize,
    decode_scratch_bytes: usize,
}

/// A checked archive retained in borrowed form until the complete roster passes.
pub(crate) struct PreflightedAggStateArchive<'a> {
    archived: &'a ArchivedAggStateCheckpoint,
    restore: AggStateRestorePreflight,
}

impl AggStateArchiveRestoreProfile {
    pub(in crate::aggregate_state) const fn new(
        fingerprint: u64,
        accumulator_states: usize,
        max_groups: usize,
        group_columns: usize,
        output_columns: usize,
        emit_changelog: bool,
    ) -> Self {
        Self {
            fingerprint,
            accumulator_states,
            max_groups,
            group_columns,
            output_columns,
            emit_changelog,
        }
    }

    pub(crate) fn preflight<'a>(
        self,
        bytes: &'a [u8],
        context: std::fmt::Arguments<'_>,
    ) -> Result<PreflightedAggStateArchive<'a>, DbError> {
        let archived = rkyv::access::<ArchivedAggStateCheckpoint, rkyv::rancor::Error>(bytes)
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "{context} aggregate archive validation failed: {error}"
                ))
            })?;
        let group_count = self.validate_archive_shape(archived, context)?;
        let owned_state_bytes = archived_owned_state_bytes(archived, context)?;
        let mut ipc = IpcRestoreAccounting::default();
        if group_count != 0 {
            self.preflight_ipc(archived, group_count, context, &mut ipc)?;
        }
        let expanded_ipc_bytes = ipc.expanded_bytes(context)?;

        Ok(PreflightedAggStateArchive {
            archived,
            restore: AggStateRestorePreflight {
                group_count,
                owned_state_bytes,
                final_state_upper_bytes: expanded_ipc_bytes,
                decode_scratch_bytes: expanded_ipc_bytes,
            },
        })
    }

    fn validate_archive_shape(
        self,
        archived: &ArchivedAggStateCheckpoint,
        context: std::fmt::Arguments<'_>,
    ) -> Result<usize, DbError> {
        if archived.fingerprint != self.fingerprint {
            return Err(DbError::Pipeline(format!(
                "{context} fingerprint mismatch: saved={}, current={}",
                archived.fingerprint, self.fingerprint
            )));
        }
        let group_count = archived.last_updated_ms.len();
        if group_count > self.max_groups {
            return Err(DbError::Pipeline(format!(
                "aggregate group limit exceeded during {context} archive preflight: archive={group_count}, limit={}",
                self.max_groups
            )));
        }
        if group_count == 0 {
            self.validate_empty_archive(archived, context)?;
        } else {
            self.validate_nonempty_archive(archived, group_count, context)?;
        }
        Ok(group_count)
    }

    fn validate_empty_archive(
        self,
        archived: &ArchivedAggStateCheckpoint,
        context: std::fmt::Arguments<'_>,
    ) -> Result<(), DbError> {
        if !archived.keys_ipc.is_empty()
            || !archived.acc_state_ipc.is_empty()
            || !archived.input_weights.is_empty()
            || !archived.last_emitted.is_empty()
        {
            return Err(DbError::Pipeline(format!(
                "{context} contains a non-canonical empty aggregate checkpoint"
            )));
        }
        Ok(())
    }

    fn validate_nonempty_archive(
        self,
        archived: &ArchivedAggStateCheckpoint,
        group_count: usize,
        context: std::fmt::Arguments<'_>,
    ) -> Result<(), DbError> {
        if archived.input_weights.len() != group_count {
            return Err(DbError::Pipeline(format!(
                "{context} aggregate checkpoint has {} input weights for {group_count} groups",
                archived.input_weights.len()
            )));
        }
        if archived.input_weights.iter().any(|weight| *weight < 0) {
            return Err(DbError::Pipeline(format!(
                "{context} aggregate checkpoint contains a negative input weight"
            )));
        }
        if archived.acc_state_ipc.len() != self.accumulator_states {
            return Err(DbError::Pipeline(format!(
                "{context} aggregate checkpoint has {} accumulator states; the query requires {}",
                archived.acc_state_ipc.len(),
                self.accumulator_states
            )));
        }
        if archived
            .acc_state_ipc
            .iter()
            .any(rkyv::vec::ArchivedVec::is_empty)
        {
            return Err(DbError::Pipeline(format!(
                "{context} aggregate checkpoint contains an empty accumulator state"
            )));
        }
        if self.group_columns != 0 && archived.keys_ipc.is_empty() {
            return Err(DbError::Pipeline(format!(
                "{context} grouped aggregate checkpoint has {group_count} groups but no key bytes"
            )));
        }
        if self.group_columns == 0 {
            if group_count != 1 {
                return Err(DbError::Pipeline(format!(
                    "{context} global aggregate checkpoint contains {group_count} groups"
                )));
            }
            if !archived.keys_ipc.is_empty() {
                return Err(DbError::Pipeline(format!(
                    "{context} global aggregate checkpoint contains key bytes"
                )));
            }
        }
        if archived.last_emitted.len() > group_count {
            return Err(DbError::Pipeline(format!(
                "{context} aggregate checkpoint contains {} changelog entries for {group_count} groups",
                archived.last_emitted.len()
            )));
        }
        if !self.emit_changelog && !archived.last_emitted.is_empty() {
            return Err(DbError::Pipeline(format!(
                "{context} aggregate checkpoint contains changelog state for a non-changelog query"
            )));
        }
        Ok(())
    }

    fn preflight_ipc(
        self,
        archived: &ArchivedAggStateCheckpoint,
        group_count: usize,
        context: std::fmt::Arguments<'_>,
        accounting: &mut IpcRestoreAccounting,
    ) -> Result<(), DbError> {
        if self.group_columns != 0 {
            let bytes = archived.keys_ipc.as_slice();
            let ipc = preflight_scalar_ipc_restore(
                bytes,
                group_count,
                self.group_columns,
                self.group_columns,
                format_args!("{context} aggregate keys"),
            )?;
            accounting.add(bytes, ipc, context)?;
        }
        for (index, state) in archived.acc_state_ipc.iter().enumerate() {
            let bytes = state.as_slice();
            let ipc = preflight_scalar_ipc_restore(
                bytes,
                group_count,
                1,
                MAX_ACCUMULATOR_STATE_COLUMNS,
                format_args!("{context} aggregate accumulator {index}"),
            )?;
            accounting.add(bytes, ipc, context)?;
        }
        for (index, emitted) in archived.last_emitted.iter().enumerate() {
            self.preflight_emitted(index, emitted, context, accounting)?;
        }
        Ok(())
    }

    fn preflight_emitted(
        self,
        index: usize,
        emitted: &ArchivedEmittedCheckpoint,
        context: std::fmt::Arguments<'_>,
        accounting: &mut IpcRestoreAccounting,
    ) -> Result<(), DbError> {
        if self.group_columns == 0 {
            if !emitted.key.is_empty() {
                return Err(DbError::Pipeline(format!(
                    "{context} global aggregate changelog entry {index} contains key bytes"
                )));
            }
        } else {
            let bytes = emitted.key.as_slice();
            let ipc = preflight_scalar_ipc_restore(
                bytes,
                1,
                self.group_columns,
                self.group_columns,
                format_args!("{context} aggregate changelog key {index}"),
            )?;
            accounting.add(bytes, ipc, context)?;
        }
        let bytes = emitted.values.as_slice();
        let ipc = preflight_scalar_ipc_restore(
            bytes,
            1,
            self.output_columns,
            self.output_columns,
            format_args!("{context} aggregate changelog values {index}"),
        )?;
        accounting.add(bytes, ipc, context)
    }
}

#[derive(Default)]
struct IpcRestoreAccounting {
    encoded_bytes: usize,
    dictionary_expansion_bytes: usize,
    shared_expansion_bytes: usize,
    cell_count: usize,
    decoded_rows: usize,
}

impl IpcRestoreAccounting {
    fn add(
        &mut self,
        bytes: &[u8],
        ipc: IpcRestorePreflight,
        context: std::fmt::Arguments<'_>,
    ) -> Result<(), DbError> {
        self.encoded_bytes = self.encoded_bytes.checked_add(bytes.len()).ok_or_else(|| {
            DbError::Pipeline(format!(
                "{context} aggregate restore IPC byte accounting overflow"
            ))
        })?;
        self.dictionary_expansion_bytes = self
            .dictionary_expansion_bytes
            .checked_add(
                ipc.dictionary_body_bytes
                    .checked_mul(ipc.rows)
                    .ok_or_else(|| {
                        DbError::Pipeline(format!(
                            "{context} aggregate dictionary expansion accounting overflow"
                        ))
                    })?,
            )
            .ok_or_else(|| {
                DbError::Pipeline(format!(
                    "{context} aggregate dictionary expansion accounting overflow"
                ))
            })?;
        self.shared_expansion_bytes = self
            .shared_expansion_bytes
            .checked_add(
                ipc.shared_payload_bytes
                    .checked_mul(ipc.rows)
                    .ok_or_else(|| {
                        DbError::Pipeline(format!(
                            "{context} aggregate shared-buffer expansion accounting overflow"
                        ))
                    })?,
            )
            .ok_or_else(|| {
                DbError::Pipeline(format!(
                    "{context} aggregate shared-buffer expansion accounting overflow"
                ))
            })?;
        let scratch_rows = ipc.rows.checked_add(ipc.dictionary_rows).ok_or_else(|| {
            DbError::Pipeline(format!(
                "{context} aggregate restore row accounting overflow"
            ))
        })?;
        self.cell_count = self
            .cell_count
            .checked_add(scratch_rows.checked_mul(ipc.columns).ok_or_else(|| {
                DbError::Pipeline(format!(
                    "{context} aggregate restore cell accounting overflow"
                ))
            })?)
            .ok_or_else(|| {
                DbError::Pipeline(format!(
                    "{context} aggregate restore cell accounting overflow"
                ))
            })?;
        self.decoded_rows = self.decoded_rows.checked_add(scratch_rows).ok_or_else(|| {
            DbError::Pipeline(format!(
                "{context} aggregate restore row accounting overflow"
            ))
        })?;
        Ok(())
    }

    fn expanded_bytes(self, context: std::fmt::Arguments<'_>) -> Result<usize, DbError> {
        self.encoded_bytes
            .checked_mul(RESTORE_IPC_EXPANSION_FACTOR)
            .and_then(|bytes| bytes.checked_add(self.dictionary_expansion_bytes))
            .and_then(|bytes| bytes.checked_add(self.shared_expansion_bytes))
            .and_then(|bytes| {
                self.cell_count
                    .checked_mul(RESTORE_CELL_SCRATCH_CHARGE)
                    .and_then(|cells| bytes.checked_add(cells))
            })
            .and_then(|bytes| {
                self.decoded_rows
                    .checked_mul(RESTORE_ROW_SCRATCH_CHARGE)
                    .and_then(|rows| bytes.checked_add(rows))
            })
            .ok_or_else(|| {
                DbError::Pipeline(format!(
                    "{context} aggregate restore expansion accounting overflow"
                ))
            })
    }
}

impl PreflightedAggStateArchive<'_> {
    #[cfg(test)]
    pub(crate) const fn group_count(&self) -> usize {
        self.restore.group_count
    }

    pub(crate) const fn restore_preflight(&self) -> AggStateRestorePreflight {
        self.restore
    }

    pub(crate) fn deserialize(
        self,
        context: std::fmt::Arguments<'_>,
    ) -> Result<AggStateCheckpoint, DbError> {
        rkyv::deserialize::<AggStateCheckpoint, rkyv::rancor::Error>(self.archived).map_err(
            |error| {
                DbError::Pipeline(format!(
                    "{context} aggregate deserialization failed: {error}"
                ))
            },
        )
    }
}

impl AggStateRestorePreflight {
    #[cfg(feature = "cluster")]
    pub(crate) const fn group_count(self) -> usize {
        self.group_count
    }

    #[cfg(all(test, feature = "cluster"))]
    pub(crate) const fn owned_state_bytes(self) -> usize {
        self.owned_state_bytes
    }

    pub(crate) const fn final_state_upper_bytes(self) -> usize {
        self.final_state_upper_bytes
    }

    #[cfg(all(test, feature = "cluster"))]
    pub(crate) const fn decode_scratch_bytes(self) -> usize {
        self.decode_scratch_bytes
    }

    pub(crate) fn sequential_decode_bytes(self) -> Option<usize> {
        self.owned_state_bytes
            .checked_add(self.decode_scratch_bytes)
    }
}

fn archived_owned_state_bytes(
    archived: &ArchivedAggStateCheckpoint,
    context: std::fmt::Arguments<'_>,
) -> Result<usize, DbError> {
    fn roster<T>(len: usize) -> Option<usize> {
        len.checked_mul(std::mem::size_of::<T>())
    }

    let mut bytes = archived.keys_ipc.len();
    bytes = bytes
        .checked_add(
            roster::<Vec<u8>>(archived.acc_state_ipc.len()).ok_or_else(|| {
                DbError::Pipeline(format!(
                    "{context} aggregate owned checkpoint roster accounting overflow"
                ))
            })?,
        )
        .ok_or_else(|| {
            DbError::Pipeline(format!(
                "{context} aggregate owned checkpoint accounting overflow"
            ))
        })?;
    for state in archived.acc_state_ipc.iter() {
        bytes = bytes.checked_add(state.len()).ok_or_else(|| {
            DbError::Pipeline(format!(
                "{context} aggregate owned checkpoint accounting overflow"
            ))
        })?;
    }
    for roster_bytes in [
        roster::<i64>(archived.input_weights.len()),
        roster::<i64>(archived.last_updated_ms.len()),
        roster::<EmittedCheckpoint>(archived.last_emitted.len()),
    ] {
        bytes = bytes
            .checked_add(roster_bytes.ok_or_else(|| {
                DbError::Pipeline(format!(
                    "{context} aggregate owned checkpoint roster accounting overflow"
                ))
            })?)
            .ok_or_else(|| {
                DbError::Pipeline(format!(
                    "{context} aggregate owned checkpoint accounting overflow"
                ))
            })?;
    }
    for emitted in archived.last_emitted.iter() {
        bytes = bytes
            .checked_add(emitted.key.len())
            .and_then(|bytes| bytes.checked_add(emitted.values.len()))
            .ok_or_else(|| {
                DbError::Pipeline(format!(
                    "{context} aggregate owned checkpoint accounting overflow"
                ))
            })?;
    }
    Ok(bytes)
}
