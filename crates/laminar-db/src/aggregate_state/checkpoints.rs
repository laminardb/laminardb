//! Serializable checkpoint shapes for aggregate and window state.
//!
//! `AggStateCheckpoint` is columnar; `GroupCheckpoint` (window/EOWC) and
//! `EmittedCheckpoint` hold one-row IPC tuples.

use arrow::datatypes::Schema;
use xxhash_rust::xxh3::Xxh3;

use crate::error::DbError;

// Restore accounting follows the same deterministic charged-byte contract as resident aggregate
// state: allocator metadata and fragmentation are excluded, while every requested payload and
// inline element is included. The expansion factors deliberately dominate Arrow array creation,
// row conversion, scalar extraction, and concrete-state construction for the scalar-only schemas
// admitted below.
const RESTORE_IPC_EXPANSION_FACTOR: usize = 8;
const RESTORE_ROW_SCRATCH_CHARGE: usize = 2_048;
const RESTORE_CELL_SCRATCH_CHARGE: usize = 64;
const MAX_ACCUMULATOR_STATE_COLUMNS: usize = 2;

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct GroupCheckpoint {
    pub key: Vec<u8>,
    pub acc_states: Vec<Vec<u8>>,
}

/// Columnar running-aggregate state: all keys in one IPC batch, each accumulator's
/// state across all groups in one IPC batch. Row `j` of `keys_ipc`, every
/// `acc_state_ipc[i]`, `input_weights[j]`, and `last_updated_ms[j]` refer to the same group.
#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct AggStateCheckpoint {
    pub fingerprint: u64,
    pub keys_ipc: Vec<u8>,
    pub acc_state_ipc: Vec<Vec<u8>>,
    pub input_weights: Vec<i64>,
    pub last_updated_ms: Vec<i64>,
    pub last_emitted: Vec<EmittedCheckpoint>,
}

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct EmittedCheckpoint {
    pub key: Vec<u8>,
    pub values: Vec<u8>,
}

impl AggStateCheckpoint {
    pub(crate) fn retained_serialization_bytes(&self) -> Result<usize, DbError> {
        fn add(total: &mut usize, bytes: usize) -> Result<(), DbError> {
            *total = total.checked_add(bytes).ok_or_else(|| {
                DbError::Checkpoint("aggregate checkpoint serialization accounting overflow".into())
            })?;
            Ok(())
        }

        fn roster<T>(capacity: usize) -> Result<usize, DbError> {
            capacity
                .checked_mul(std::mem::size_of::<T>())
                .ok_or_else(|| {
                    DbError::Checkpoint("aggregate checkpoint serialization roster overflow".into())
                })
        }

        let mut bytes = self.keys_ipc.capacity();
        add(
            &mut bytes,
            roster::<Vec<u8>>(self.acc_state_ipc.capacity())?,
        )?;
        for state in &self.acc_state_ipc {
            add(&mut bytes, state.capacity())?;
        }
        add(&mut bytes, roster::<i64>(self.input_weights.capacity())?)?;
        add(&mut bytes, roster::<i64>(self.last_updated_ms.capacity())?)?;
        add(
            &mut bytes,
            roster::<EmittedCheckpoint>(self.last_emitted.capacity())?,
        )?;
        for emitted in &self.last_emitted {
            add(&mut bytes, emitted.key.capacity())?;
            add(&mut bytes, emitted.values.capacity())?;
        }
        Ok(bytes)
    }
}

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

#[derive(Clone, Copy, Debug, Default)]
struct IpcRestorePreflight {
    rows: usize,
    columns: usize,
    dictionary_rows: usize,
    dictionary_body_bytes: usize,
    shared_payload_bytes: usize,
}

impl AggStateArchiveRestoreProfile {
    pub(super) const fn new(
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
            if !archived.keys_ipc.is_empty()
                || !archived.acc_state_ipc.is_empty()
                || !archived.input_weights.is_empty()
                || !archived.last_emitted.is_empty()
            {
                return Err(DbError::Pipeline(format!(
                    "{context} contains a non-canonical empty aggregate checkpoint"
                )));
            }
        } else {
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
        }

        let owned_state_bytes = archived_owned_state_bytes(archived, context)?;
        let mut ipc_encoded_bytes = 0usize;
        let mut dictionary_expansion_bytes = 0usize;
        let mut shared_expansion_bytes = 0usize;
        let mut cell_count = 0usize;
        let mut decoded_rows = 0usize;
        let mut add_ipc = |bytes: &[u8], ipc: IpcRestorePreflight| -> Result<(), DbError> {
            ipc_encoded_bytes = ipc_encoded_bytes.checked_add(bytes.len()).ok_or_else(|| {
                DbError::Pipeline(format!(
                    "{context} aggregate restore IPC byte accounting overflow"
                ))
            })?;
            dictionary_expansion_bytes =
                dictionary_expansion_bytes
                    .checked_add(ipc.dictionary_body_bytes.checked_mul(ipc.rows).ok_or_else(
                        || {
                            DbError::Pipeline(format!(
                                "{context} aggregate dictionary expansion accounting overflow"
                            ))
                        },
                    )?)
                    .ok_or_else(|| {
                        DbError::Pipeline(format!(
                            "{context} aggregate dictionary expansion accounting overflow"
                        ))
                    })?;
            shared_expansion_bytes =
                shared_expansion_bytes
                    .checked_add(ipc.shared_payload_bytes.checked_mul(ipc.rows).ok_or_else(
                        || {
                            DbError::Pipeline(format!(
                                "{context} aggregate shared-buffer expansion accounting overflow"
                            ))
                        },
                    )?)
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
            cell_count = cell_count
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
            decoded_rows = decoded_rows.checked_add(scratch_rows).ok_or_else(|| {
                DbError::Pipeline(format!(
                    "{context} aggregate restore row accounting overflow"
                ))
            })?;
            Ok(())
        };

        if group_count != 0 {
            if self.group_columns != 0 {
                let bytes = archived.keys_ipc.as_slice();
                let ipc = preflight_scalar_ipc_restore(
                    bytes,
                    group_count,
                    self.group_columns,
                    self.group_columns,
                    format_args!("{context} aggregate keys"),
                )?;
                add_ipc(bytes, ipc)?;
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
                add_ipc(bytes, ipc)?;
            }
            for (index, emitted) in archived.last_emitted.iter().enumerate() {
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
                    add_ipc(bytes, ipc)?;
                }
                let bytes = emitted.values.as_slice();
                let ipc = preflight_scalar_ipc_restore(
                    bytes,
                    1,
                    self.output_columns,
                    self.output_columns,
                    format_args!("{context} aggregate changelog values {index}"),
                )?;
                add_ipc(bytes, ipc)?;
            }
        }

        let expanded_ipc_bytes = ipc_encoded_bytes
            .checked_mul(RESTORE_IPC_EXPANSION_FACTOR)
            .and_then(|bytes| bytes.checked_add(dictionary_expansion_bytes))
            .and_then(|bytes| bytes.checked_add(shared_expansion_bytes))
            .and_then(|bytes| {
                cell_count
                    .checked_mul(RESTORE_CELL_SCRATCH_CHARGE)
                    .and_then(|cells| bytes.checked_add(cells))
            })
            .and_then(|bytes| {
                decoded_rows
                    .checked_mul(RESTORE_ROW_SCRATCH_CHARGE)
                    .and_then(|rows| bytes.checked_add(rows))
            })
            .ok_or_else(|| {
                DbError::Pipeline(format!(
                    "{context} aggregate restore expansion accounting overflow"
                ))
            })?;

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
    #[cfg(any(feature = "cluster", test))]
    pub(crate) const fn group_count(self) -> usize {
        self.group_count
    }

    #[cfg(test)]
    pub(crate) const fn owned_state_bytes(self) -> usize {
        self.owned_state_bytes
    }

    pub(crate) const fn final_state_upper_bytes(self) -> usize {
        self.final_state_upper_bytes
    }

    #[cfg(test)]
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

#[derive(Clone, Copy)]
enum IpcPhysicalShape {
    Null,
    Bits,
    Fixed(usize),
    Variable(usize),
    View,
}

#[derive(Clone, Copy)]
struct IpcDictionaryShape {
    id: i64,
    index_width: usize,
}

#[derive(Clone, Copy)]
struct IpcFieldShape {
    value: IpcPhysicalShape,
    dictionary: Option<IpcDictionaryShape>,
}

fn invalid_ipc_shape(context: std::fmt::Arguments<'_>, detail: &str) -> DbError {
    DbError::Pipeline(format!("{context} IPC {detail}"))
}

fn ipc_field_shape(
    field: arrow_ipc::Field<'_>,
    context: std::fmt::Arguments<'_>,
) -> Result<IpcFieldShape, DbError> {
    if field
        .children()
        .is_some_and(|children| !children.is_empty())
        || field
            .custom_metadata()
            .is_some_and(|metadata| !metadata.is_empty())
    {
        return Err(invalid_ipc_shape(
            context,
            "contains a nested or metadata-bearing field",
        ));
    }

    let missing_type = || invalid_ipc_shape(context, "field type header is missing");
    let value = match field.type_type() {
        arrow_ipc::Type::Null => {
            field.type_as_null().ok_or_else(missing_type)?;
            IpcPhysicalShape::Null
        }
        arrow_ipc::Type::Bool => {
            field.type_as_bool().ok_or_else(missing_type)?;
            IpcPhysicalShape::Bits
        }
        arrow_ipc::Type::Int => {
            let width = field.type_as_int().ok_or_else(missing_type)?.bitWidth();
            match width {
                8 => IpcPhysicalShape::Fixed(1),
                16 => IpcPhysicalShape::Fixed(2),
                32 => IpcPhysicalShape::Fixed(4),
                64 => IpcPhysicalShape::Fixed(8),
                _ => return Err(invalid_ipc_shape(context, "integer width is unsupported")),
            }
        }
        arrow_ipc::Type::FloatingPoint => {
            let float = field.type_as_floating_point().ok_or_else(missing_type)?;
            let width = match float.precision() {
                arrow_ipc::Precision::HALF => 2,
                arrow_ipc::Precision::SINGLE => 4,
                arrow_ipc::Precision::DOUBLE => 8,
                _ => {
                    return Err(invalid_ipc_shape(
                        context,
                        "floating-point precision is unsupported",
                    ));
                }
            };
            IpcPhysicalShape::Fixed(width)
        }
        arrow_ipc::Type::Binary => {
            field.type_as_binary().ok_or_else(missing_type)?;
            IpcPhysicalShape::Variable(4)
        }
        arrow_ipc::Type::Utf8 => {
            field.type_as_utf_8().ok_or_else(missing_type)?;
            IpcPhysicalShape::Variable(4)
        }
        arrow_ipc::Type::LargeBinary => {
            field.type_as_large_binary().ok_or_else(missing_type)?;
            IpcPhysicalShape::Variable(8)
        }
        arrow_ipc::Type::LargeUtf8 => {
            field.type_as_large_utf_8().ok_or_else(missing_type)?;
            IpcPhysicalShape::Variable(8)
        }
        arrow_ipc::Type::BinaryView => {
            field.type_as_binary_view().ok_or_else(missing_type)?;
            IpcPhysicalShape::View
        }
        arrow_ipc::Type::Utf8View => {
            field.type_as_utf_8_view().ok_or_else(missing_type)?;
            IpcPhysicalShape::View
        }
        arrow_ipc::Type::Decimal => {
            let decimal = field.type_as_decimal().ok_or_else(missing_type)?;
            let precision = u8::try_from(decimal.precision())
                .map_err(|_| invalid_ipc_shape(context, "decimal precision is invalid"))?;
            let scale = i8::try_from(decimal.scale())
                .map_err(|_| invalid_ipc_shape(context, "decimal scale is invalid"))?;
            let (width, maximum) = match decimal.bitWidth() {
                32 => (4, 9),
                64 => (8, 18),
                128 => (16, 38),
                256 => (32, 76),
                _ => return Err(invalid_ipc_shape(context, "decimal width is unsupported")),
            };
            if precision == 0
                || precision > maximum
                || scale > maximum.cast_signed()
                || (scale > 0 && scale.cast_unsigned() > precision)
            {
                return Err(invalid_ipc_shape(
                    context,
                    "decimal precision or scale is invalid",
                ));
            }
            IpcPhysicalShape::Fixed(width)
        }
        arrow_ipc::Type::Date => {
            let date = field.type_as_date().ok_or_else(missing_type)?;
            IpcPhysicalShape::Fixed(match date.unit() {
                arrow_ipc::DateUnit::DAY => 4,
                arrow_ipc::DateUnit::MILLISECOND => 8,
                _ => return Err(invalid_ipc_shape(context, "date unit is unsupported")),
            })
        }
        arrow_ipc::Type::Time => {
            let time = field.type_as_time().ok_or_else(missing_type)?;
            let width = match (time.bitWidth(), time.unit()) {
                (32, arrow_ipc::TimeUnit::SECOND | arrow_ipc::TimeUnit::MILLISECOND) => 4,
                (64, arrow_ipc::TimeUnit::MICROSECOND | arrow_ipc::TimeUnit::NANOSECOND) => 8,
                _ => return Err(invalid_ipc_shape(context, "time shape is unsupported")),
            };
            IpcPhysicalShape::Fixed(width)
        }
        arrow_ipc::Type::Timestamp => {
            let timestamp = field.type_as_timestamp().ok_or_else(missing_type)?;
            if !matches!(
                timestamp.unit(),
                arrow_ipc::TimeUnit::SECOND
                    | arrow_ipc::TimeUnit::MILLISECOND
                    | arrow_ipc::TimeUnit::MICROSECOND
                    | arrow_ipc::TimeUnit::NANOSECOND
            ) {
                return Err(invalid_ipc_shape(context, "timestamp unit is unsupported"));
            }
            IpcPhysicalShape::Fixed(8)
        }
        arrow_ipc::Type::Interval => {
            let interval = field.type_as_interval().ok_or_else(missing_type)?;
            IpcPhysicalShape::Fixed(match interval.unit() {
                arrow_ipc::IntervalUnit::YEAR_MONTH => 4,
                arrow_ipc::IntervalUnit::DAY_TIME => 8,
                arrow_ipc::IntervalUnit::MONTH_DAY_NANO => 16,
                _ => return Err(invalid_ipc_shape(context, "interval unit is unsupported")),
            })
        }
        arrow_ipc::Type::FixedSizeBinary => {
            let width = usize::try_from(
                field
                    .type_as_fixed_size_binary()
                    .ok_or_else(missing_type)?
                    .byteWidth(),
            )
            .map_err(|_| invalid_ipc_shape(context, "fixed-size binary width is invalid"))?;
            IpcPhysicalShape::Fixed(width)
        }
        arrow_ipc::Type::Duration => {
            let duration = field.type_as_duration().ok_or_else(missing_type)?;
            if !matches!(
                duration.unit(),
                arrow_ipc::TimeUnit::SECOND
                    | arrow_ipc::TimeUnit::MILLISECOND
                    | arrow_ipc::TimeUnit::MICROSECOND
                    | arrow_ipc::TimeUnit::NANOSECOND
            ) {
                return Err(invalid_ipc_shape(context, "duration unit is unsupported"));
            }
            IpcPhysicalShape::Fixed(8)
        }
        _ => {
            return Err(invalid_ipc_shape(
                context,
                "contains a nested or unsupported field",
            ));
        }
    };

    let dictionary = field
        .dictionary()
        .map(|dictionary| {
            if dictionary.id() < 0
                || dictionary.dictionaryKind() != arrow_ipc::DictionaryKind::DenseArray
            {
                return Err(invalid_ipc_shape(
                    context,
                    "dictionary encoding is unsupported",
                ));
            }
            let index = dictionary
                .indexType()
                .ok_or_else(|| invalid_ipc_shape(context, "dictionary index type is missing"))?;
            let index_width = match index.bitWidth() {
                8 => 1,
                16 => 2,
                32 => 4,
                64 => 8,
                _ => {
                    return Err(invalid_ipc_shape(
                        context,
                        "dictionary index width is unsupported",
                    ));
                }
            };
            Ok(IpcDictionaryShape {
                id: dictionary.id(),
                index_width,
            })
        })
        .transpose()?;

    Ok(IpcFieldShape { value, dictionary })
}

fn validate_ipc_schema(
    schema: arrow_ipc::Schema<'_>,
    minimum_columns: usize,
    maximum_columns: usize,
    context: std::fmt::Arguments<'_>,
) -> Result<(usize, usize), DbError> {
    if schema.endianness() != arrow_ipc::Endianness::Little {
        return Err(invalid_ipc_shape(
            context,
            "schema endianness is unsupported",
        ));
    }
    if schema
        .custom_metadata()
        .is_some_and(|metadata| !metadata.is_empty())
        || schema
            .features()
            .is_some_and(|features| !features.is_empty())
    {
        return Err(invalid_ipc_shape(
            context,
            "schema metadata or features are unsupported",
        ));
    }
    let fields = schema
        .fields()
        .ok_or_else(|| invalid_ipc_shape(context, "schema fields are missing"))?;
    let columns = fields.len();
    if columns < minimum_columns || columns > maximum_columns {
        return Err(DbError::Pipeline(format!(
            "{context} IPC has {columns} columns; expected {minimum_columns}..={maximum_columns}"
        )));
    }

    let mut dictionary_count = 0usize;
    for index in 0..columns {
        let shape = ipc_field_shape(fields.get(index), context)?;
        if let Some(dictionary) = shape.dictionary {
            for previous in 0..index {
                if ipc_field_shape(fields.get(previous), context)?
                    .dictionary
                    .is_some_and(|candidate| candidate.id == dictionary.id)
                {
                    return Err(invalid_ipc_shape(
                        context,
                        "contains duplicate dictionary ids",
                    ));
                }
            }
            dictionary_count = dictionary_count.checked_add(1).ok_or_else(|| {
                invalid_ipc_shape(context, "dictionary-count accounting overflow")
            })?;
        }
    }
    Ok((columns, dictionary_count))
}

fn nth_ipc_dictionary_field<'a>(
    schema: arrow_ipc::Schema<'a>,
    ordinal: usize,
    context: std::fmt::Arguments<'_>,
) -> Result<Option<arrow_ipc::Field<'a>>, DbError> {
    let fields = schema
        .fields()
        .ok_or_else(|| invalid_ipc_shape(context, "schema fields are missing"))?;
    let mut seen = 0usize;
    for field in fields {
        if ipc_field_shape(field, context)?.dictionary.is_some() {
            if seen == ordinal {
                return Ok(Some(field));
            }
            seen += 1;
        }
    }
    Ok(None)
}

fn align_ipc_body_offset(value: usize) -> Option<usize> {
    value.checked_add(63).map(|value| value & !63)
}

fn validate_ipc_batch_layout<F>(
    batch: arrow_ipc::RecordBatch<'_>,
    expected_rows: usize,
    body_len: usize,
    columns: usize,
    mut shape_at: F,
    context: std::fmt::Arguments<'_>,
) -> Result<usize, DbError>
where
    F: FnMut(usize) -> Result<IpcPhysicalShape, DbError>,
{
    if batch.compression().is_some() {
        return Err(invalid_ipc_shape(context, "compression is unsupported"));
    }
    let rows = usize::try_from(batch.length())
        .map_err(|_| invalid_ipc_shape(context, "batch length is negative or too large"))?;
    if rows != expected_rows {
        return Err(DbError::Pipeline(format!(
            "{context} IPC has {rows} rows; expected {expected_rows}"
        )));
    }
    let nodes = batch
        .nodes()
        .ok_or_else(|| invalid_ipc_shape(context, "field nodes are missing"))?;
    if nodes.len() != columns {
        return Err(invalid_ipc_shape(
            context,
            "field-node count does not match the schema",
        ));
    }
    let buffers = batch
        .buffers()
        .ok_or_else(|| invalid_ipc_shape(context, "buffer descriptors are missing"))?;

    let mut cursor = 0usize;
    for buffer in buffers {
        let offset = usize::try_from(buffer.offset())
            .map_err(|_| invalid_ipc_shape(context, "buffer offset is invalid"))?;
        let length = usize::try_from(buffer.length())
            .map_err(|_| invalid_ipc_shape(context, "buffer length is invalid"))?;
        if offset != cursor {
            return Err(invalid_ipc_shape(
                context,
                "buffers overlap or are not canonically aligned",
            ));
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| invalid_ipc_shape(context, "buffer extent overflows"))?;
        if end > body_len {
            return Err(invalid_ipc_shape(
                context,
                "buffer exceeds the message body",
            ));
        }
        cursor = align_ipc_body_offset(end)
            .ok_or_else(|| invalid_ipc_shape(context, "buffer alignment overflows"))?;
    }
    if cursor != body_len {
        return Err(invalid_ipc_shape(
            context,
            "buffer descriptors do not cover the canonical message body",
        ));
    }

    let mut view_count = 0usize;
    for index in 0..columns {
        if matches!(shape_at(index)?, IpcPhysicalShape::View) {
            view_count += 1;
        }
    }
    let variadic = batch.variadicBufferCounts();
    if view_count == 0 {
        if variadic.is_some_and(|counts| !counts.is_empty()) {
            return Err(invalid_ipc_shape(
                context,
                "has variadic counts without View fields",
            ));
        }
    } else if variadic.is_none_or(|counts| counts.len() != view_count) {
        return Err(invalid_ipc_shape(
            context,
            "View variadic-count roster is missing or malformed",
        ));
    }

    let validity_len = rows
        .checked_add(7)
        .ok_or_else(|| invalid_ipc_shape(context, "validity-buffer length overflows"))?
        / 8;
    let buffer_len = |index: usize| -> Result<usize, DbError> {
        if index >= buffers.len() {
            return Err(invalid_ipc_shape(
                context,
                "buffer count does not match the schema",
            ));
        }
        usize::try_from(buffers.get(index).length())
            .map_err(|_| invalid_ipc_shape(context, "buffer length is invalid"))
    };
    let expect_len = |index: usize, expected: usize| -> Result<(), DbError> {
        if buffer_len(index)? != expected {
            return Err(invalid_ipc_shape(
                context,
                "buffer length does not match the canonical field shape",
            ));
        }
        Ok(())
    };

    let mut buffer_index = 0usize;
    let mut variadic_index = 0usize;
    let mut shared_payload_bytes = 0usize;
    for column in 0..columns {
        let node = nodes.get(column);
        let node_rows = usize::try_from(node.length())
            .map_err(|_| invalid_ipc_shape(context, "field-node length is invalid"))?;
        let null_count = usize::try_from(node.null_count())
            .map_err(|_| invalid_ipc_shape(context, "field-node null count is invalid"))?;
        if node_rows != rows || null_count > rows {
            return Err(invalid_ipc_shape(
                context,
                "field-node length or null count is invalid",
            ));
        }

        match shape_at(column)? {
            IpcPhysicalShape::Null => {
                if null_count != rows {
                    return Err(invalid_ipc_shape(
                        context,
                        "Null field node has an invalid null count",
                    ));
                }
            }
            shape => {
                expect_len(buffer_index, validity_len)?;
                buffer_index += 1;
                match shape {
                    IpcPhysicalShape::Bits => {
                        expect_len(buffer_index, validity_len)?;
                        buffer_index += 1;
                    }
                    IpcPhysicalShape::Fixed(width) => {
                        let length = rows.checked_mul(width).ok_or_else(|| {
                            invalid_ipc_shape(context, "fixed-width buffer length overflows")
                        })?;
                        expect_len(buffer_index, length)?;
                        buffer_index += 1;
                    }
                    IpcPhysicalShape::Variable(offset_width) => {
                        // Arrow's canonical V5 writer emits a zero-length offsets buffer for an
                        // empty variable-width array; nonempty arrays retain the terminal offset.
                        let length = if rows == 0 {
                            0
                        } else {
                            rows.checked_add(1)
                                .and_then(|rows| rows.checked_mul(offset_width))
                                .ok_or_else(|| {
                                    invalid_ipc_shape(context, "offset-buffer length overflows")
                                })?
                        };
                        expect_len(buffer_index, length)?;
                        buffer_index += 2;
                        if buffer_index > buffers.len() {
                            return Err(invalid_ipc_shape(
                                context,
                                "variable field buffers are missing",
                            ));
                        }
                    }
                    IpcPhysicalShape::View => {
                        let length = rows.checked_mul(16).ok_or_else(|| {
                            invalid_ipc_shape(context, "View-buffer length overflows")
                        })?;
                        expect_len(buffer_index, length)?;
                        buffer_index += 1;
                        let counts = variadic.expect("View fields require variadic counts");
                        let count = usize::try_from(counts.get(variadic_index)).map_err(|_| {
                            invalid_ipc_shape(context, "View variadic count is invalid")
                        })?;
                        variadic_index += 1;
                        let end = buffer_index.checked_add(count).ok_or_else(|| {
                            invalid_ipc_shape(context, "View buffer count overflows")
                        })?;
                        if end > buffers.len() {
                            return Err(invalid_ipc_shape(
                                context,
                                "View variadic count exceeds the buffer roster",
                            ));
                        }
                        while buffer_index < end {
                            shared_payload_bytes = shared_payload_bytes
                                .checked_add(buffer_len(buffer_index)?)
                                .ok_or_else(|| {
                                    invalid_ipc_shape(context, "View payload accounting overflows")
                                })?;
                            buffer_index += 1;
                        }
                    }
                    IpcPhysicalShape::Null => unreachable!(),
                }
            }
        }
    }
    if buffer_index != buffers.len() || variadic_index != view_count {
        return Err(invalid_ipc_shape(
            context,
            "buffer count does not match the schema",
        ));
    }
    Ok(shared_payload_bytes)
}

fn preflight_scalar_ipc_restore(
    bytes: &[u8],
    expected_rows: usize,
    minimum_columns: usize,
    maximum_columns: usize,
    context: std::fmt::Arguments<'_>,
) -> Result<IpcRestorePreflight, DbError> {
    const CONTINUATION: u32 = u32::MAX;

    if bytes.is_empty() || minimum_columns == 0 || minimum_columns > maximum_columns {
        return Err(DbError::Pipeline(format!(
            "{context} IPC stream is empty or has an invalid expected shape"
        )));
    }
    let mut offset = 0usize;
    let mut schema = None;
    let mut columns = None;
    let mut dictionary_count = 0usize;
    let mut next_dictionary = 0usize;
    let mut rows = None;
    let mut dictionary_rows = 0usize;
    let mut dictionary_body_bytes = 0usize;
    let mut shared_payload_bytes = 0usize;
    loop {
        let prefix_end = offset
            .checked_add(4)
            .ok_or_else(|| invalid_ipc_shape(context, "framing overflows"))?;
        let prefix = bytes
            .get(offset..prefix_end)
            .ok_or_else(|| invalid_ipc_shape(context, "frame is truncated"))?;
        offset = prefix_end;
        if u32::from_le_bytes(prefix.try_into().expect("four-byte prefix")) != CONTINUATION {
            return Err(invalid_ipc_shape(
                context,
                "stream does not use canonical V5 continuation framing",
            ));
        }
        let length_end = offset
            .checked_add(4)
            .ok_or_else(|| invalid_ipc_shape(context, "framing overflows"))?;
        let length = bytes
            .get(offset..length_end)
            .ok_or_else(|| invalid_ipc_shape(context, "continuation is truncated"))?;
        offset = length_end;
        let metadata_len = usize::try_from(u32::from_le_bytes(
            length.try_into().expect("four-byte length"),
        ))
        .map_err(|_| invalid_ipc_shape(context, "metadata length exceeds usize"))?;
        if metadata_len == 0 {
            if offset != bytes.len()
                || schema.is_none()
                || rows.is_none()
                || next_dictionary != dictionary_count
            {
                return Err(invalid_ipc_shape(context, "stream is non-canonical"));
            }
            break;
        }
        if metadata_len
            .checked_add(8)
            .is_none_or(|length| length % 64 != 0)
        {
            return Err(invalid_ipc_shape(
                context,
                "metadata is not canonically aligned",
            ));
        }
        let metadata_end = offset
            .checked_add(metadata_len)
            .ok_or_else(|| invalid_ipc_shape(context, "metadata framing overflows"))?;
        let metadata = bytes
            .get(offset..metadata_end)
            .ok_or_else(|| invalid_ipc_shape(context, "metadata is truncated"))?;
        offset = metadata_end;
        let message = arrow_ipc::root_as_message(metadata).map_err(|error| {
            DbError::Pipeline(format!("{context} IPC metadata is invalid: {error}"))
        })?;
        if message.version() != arrow_ipc::MetadataVersion::V5 {
            return Err(invalid_ipc_shape(context, "metadata version is not V5"));
        }
        let body_len = usize::try_from(message.bodyLength())
            .map_err(|_| invalid_ipc_shape(context, "body length is negative or too large"))?;
        if body_len % 64 != 0 {
            return Err(invalid_ipc_shape(
                context,
                "body is not canonically aligned",
            ));
        }
        let body_end = offset
            .checked_add(body_len)
            .ok_or_else(|| invalid_ipc_shape(context, "body framing overflows"))?;
        if body_end > bytes.len() {
            return Err(invalid_ipc_shape(context, "body is truncated"));
        }

        match message.header_type() {
            arrow_ipc::MessageHeader::Schema if schema.is_none() && rows.is_none() => {
                if body_len != 0 {
                    return Err(invalid_ipc_shape(context, "schema message has a body"));
                }
                let parsed = message
                    .header_as_schema()
                    .ok_or_else(|| invalid_ipc_shape(context, "schema header is missing"))?;
                let (field_count, dictionaries) =
                    validate_ipc_schema(parsed, minimum_columns, maximum_columns, context)?;
                schema = Some(parsed);
                columns = Some(field_count);
                dictionary_count = dictionaries;
            }
            arrow_ipc::MessageHeader::DictionaryBatch if schema.is_some() && rows.is_none() => {
                let parsed_schema = schema.expect("schema is present");
                let field = nth_ipc_dictionary_field(parsed_schema, next_dictionary, context)?
                    .ok_or_else(|| {
                        invalid_ipc_shape(context, "has an unexpected dictionary batch")
                    })?;
                let field_shape = ipc_field_shape(field, context)?;
                let expected_dictionary = field_shape
                    .dictionary
                    .expect("dictionary field has dictionary encoding");
                let dictionary = message.header_as_dictionary_batch().ok_or_else(|| {
                    invalid_ipc_shape(context, "dictionary-batch header is missing")
                })?;
                if dictionary.id() != expected_dictionary.id || dictionary.isDelta() {
                    return Err(invalid_ipc_shape(
                        context,
                        "dictionary roster, order, or replacement mode is non-canonical",
                    ));
                }
                let data = dictionary.data().ok_or_else(|| {
                    invalid_ipc_shape(context, "dictionary-batch data is missing")
                })?;
                let batch_rows = usize::try_from(data.length()).map_err(|_| {
                    invalid_ipc_shape(context, "dictionary-batch length is invalid")
                })?;
                let shared = validate_ipc_batch_layout(
                    data,
                    batch_rows,
                    body_len,
                    1,
                    |_| Ok(field_shape.value),
                    context,
                )?;
                shared_payload_bytes =
                    shared_payload_bytes.checked_add(shared).ok_or_else(|| {
                        invalid_ipc_shape(context, "shared-buffer accounting overflows")
                    })?;
                dictionary_body_bytes =
                    dictionary_body_bytes.checked_add(body_len).ok_or_else(|| {
                        invalid_ipc_shape(context, "dictionary body accounting overflows")
                    })?;
                dictionary_rows = dictionary_rows.checked_add(batch_rows).ok_or_else(|| {
                    invalid_ipc_shape(context, "dictionary row accounting overflows")
                })?;
                next_dictionary += 1;
            }
            arrow_ipc::MessageHeader::RecordBatch
                if schema.is_some() && rows.is_none() && next_dictionary == dictionary_count =>
            {
                let batch = message
                    .header_as_record_batch()
                    .ok_or_else(|| invalid_ipc_shape(context, "record-batch header is missing"))?;
                let parsed_schema = schema.expect("schema is present");
                let fields = parsed_schema.fields().expect("validated schema has fields");
                let shared = validate_ipc_batch_layout(
                    batch,
                    expected_rows,
                    body_len,
                    columns.expect("schema column count is present"),
                    |index| {
                        let shape = ipc_field_shape(fields.get(index), context)?;
                        Ok(shape.dictionary.map_or(shape.value, |dictionary| {
                            IpcPhysicalShape::Fixed(dictionary.index_width)
                        }))
                    },
                    context,
                )?;
                shared_payload_bytes =
                    shared_payload_bytes.checked_add(shared).ok_or_else(|| {
                        invalid_ipc_shape(context, "shared-buffer accounting overflows")
                    })?;
                rows = Some(expected_rows);
            }
            _ => {
                return Err(invalid_ipc_shape(context, "message order is non-canonical"));
            }
        }
        offset = body_end;
    }
    Ok(IpcRestorePreflight {
        rows: rows.expect("canonical stream has one record batch"),
        columns: columns.expect("canonical stream has one schema"),
        dictionary_rows,
        dictionary_body_bytes,
        shared_payload_bytes,
    })
}

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct WindowCheckpoint {
    pub window_start: i64,
    pub groups: Vec<GroupCheckpoint>,
}

/// Stable hash of the state-producing SQL and output schema; invalidates state on query change.
pub(crate) fn query_fingerprint(state_sql: &str, output_schema: &Schema) -> u64 {
    query_fingerprint_with_config(state_sql, output_schema, &[])
}

/// Query fingerprint with an operator-specific binary configuration suffix.
pub(crate) fn query_fingerprint_with_config(
    state_sql: &str,
    output_schema: &Schema,
    config: &[u8],
) -> u64 {
    let mut hasher = Xxh3::new();
    hasher.update(b"laminardb.state-query.v3\0");
    hash_bytes(&mut hasher, state_sql.as_bytes());
    hasher.update(&(output_schema.fields().len() as u64).to_le_bytes());
    for field in output_schema.fields() {
        hash_bytes(&mut hasher, field.name().as_bytes());
        hash_bytes(&mut hasher, field.data_type().to_string().as_bytes());
        hasher.update(&[u8::from(field.is_nullable())]);
    }
    hash_bytes(&mut hasher, config);
    hasher.digest()
}

fn hash_bytes(hasher: &mut Xxh3, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[cfg(all(test, feature = "cluster"))]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, DictionaryArray, FixedSizeBinaryArray, Int32Array, Int64Array, StringArray,
        StringViewArray,
    };
    use arrow::datatypes::{DataType, Field, Int32Type};
    use arrow::record_batch::RecordBatch;
    use arrow_ipc::writer::StreamWriter;

    use super::*;

    const FINGERPRINT: u64 = 7;

    fn grouped_profile() -> AggStateArchiveRestoreProfile {
        AggStateArchiveRestoreProfile::new(FINGERPRINT, 1, 1, 1, 1, false)
    }

    fn ipc(columns: Vec<ArrayRef>) -> Vec<u8> {
        let fields = columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Field::new(format!("c{index}"), column.data_type().clone(), true)
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        laminar_core::serialization::serialize_batch_stream(&batch).unwrap()
    }

    fn message_metadata(bytes: &[u8], header: arrow_ipc::MessageHeader) -> (&[u8], usize) {
        let mut offset = 0usize;
        loop {
            assert_eq!(
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()),
                u32::MAX
            );
            let metadata_len =
                u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
            assert_ne!(metadata_len, 0, "requested IPC message is missing");
            let metadata_start = offset + 8;
            let metadata_end = metadata_start + metadata_len;
            let metadata = &bytes[metadata_start..metadata_end];
            let message = arrow_ipc::root_as_message(metadata).unwrap();
            if message.header_type() == header {
                return (metadata, metadata_start);
            }
            offset = metadata_end + usize::try_from(message.bodyLength()).unwrap();
        }
    }

    fn record_buffer_descriptor_start(bytes: &[u8]) -> usize {
        let (metadata, metadata_start) =
            message_metadata(bytes, arrow_ipc::MessageHeader::RecordBatch);
        let message = arrow_ipc::root_as_message(metadata).unwrap();
        let descriptors = message
            .header_as_record_batch()
            .unwrap()
            .buffers()
            .unwrap()
            .bytes();
        metadata_start + descriptors.as_ptr() as usize - metadata.as_ptr() as usize
    }

    fn record_variadic_count_start(bytes: &[u8]) -> usize {
        let (metadata, metadata_start) =
            message_metadata(bytes, arrow_ipc::MessageHeader::RecordBatch);
        let message = arrow_ipc::root_as_message(metadata).unwrap();
        let counts = message
            .header_as_record_batch()
            .unwrap()
            .variadicBufferCounts()
            .unwrap()
            .bytes();
        metadata_start + counts.as_ptr() as usize - metadata.as_ptr() as usize
    }

    fn dictionary_id_start(bytes: &[u8]) -> usize {
        let mut offset = 0usize;
        loop {
            let metadata_len =
                u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
            assert_ne!(metadata_len, 0, "explicit dictionary id is missing");
            let metadata_start = offset + 8;
            let metadata_end = metadata_start + metadata_len;
            let metadata = &bytes[metadata_start..metadata_end];
            let message = arrow_ipc::root_as_message(metadata).unwrap();
            if let Some(dictionary) = message.header_as_dictionary_batch() {
                let table = dictionary._tab;
                let field = usize::from(table.vtable().get(arrow_ipc::DictionaryBatch::VT_ID));
                if field != 0 {
                    return metadata_start + table.loc() + field;
                }
            }
            offset = metadata_end + usize::try_from(message.bodyLength()).unwrap();
        }
    }

    fn one_group() -> AggStateCheckpoint {
        AggStateCheckpoint {
            fingerprint: FINGERPRINT,
            keys_ipc: ipc(vec![Arc::new(StringArray::from(vec!["key"]))]),
            acc_state_ipc: vec![ipc(vec![Arc::new(Int64Array::from(vec![1]))])],
            input_weights: vec![1],
            last_updated_ms: vec![i64::MIN],
            last_emitted: Vec::new(),
        }
    }

    fn encode(checkpoint: &AggStateCheckpoint) -> rkyv::util::AlignedVec<16> {
        rkyv::to_bytes::<rkyv::rancor::Error>(checkpoint).unwrap()
    }

    #[test]
    fn aggregate_archive_preflight_accepts_canonical_boundaries() {
        let encoded = encode(&one_group());
        let preflighted = grouped_profile()
            .preflight(&encoded, format_args!("test"))
            .unwrap();
        assert_eq!(preflighted.group_count(), 1);

        let empty = AggStateCheckpoint {
            fingerprint: FINGERPRINT,
            keys_ipc: Vec::new(),
            acc_state_ipc: Vec::new(),
            input_weights: Vec::new(),
            last_updated_ms: Vec::new(),
            last_emitted: Vec::new(),
        };
        let encoded = encode(&empty);
        assert_eq!(
            grouped_profile()
                .preflight(&encoded, format_args!("empty"))
                .unwrap()
                .group_count(),
            0
        );

        let mut global = one_group();
        global.keys_ipc.clear();
        let encoded = encode(&global);
        assert_eq!(
            AggStateArchiveRestoreProfile::new(FINGERPRINT, 1, 1, 0, 1, false)
                .preflight(&encoded, format_args!("global"))
                .unwrap()
                .group_count(),
            1
        );
    }

    #[test]
    fn aggregate_archive_preflight_rejects_shape_before_owned_decode() {
        let mut cases = Vec::new();

        let mut wrong_fingerprint = one_group();
        wrong_fingerprint.fingerprint += 1;
        cases.push((wrong_fingerprint, "fingerprint mismatch"));

        let mut wrong_accumulators = one_group();
        wrong_accumulators.acc_state_ipc.clear();
        cases.push((wrong_accumulators, "accumulator states"));

        let mut missing_keys = one_group();
        missing_keys.keys_ipc.clear();
        cases.push((missing_keys, "no key bytes"));

        let mut missing_weights = one_group();
        missing_weights.input_weights.clear();
        cases.push((missing_weights, "input weights"));

        let mut negative_weight = one_group();
        negative_weight.input_weights[0] = -1;
        cases.push((negative_weight, "negative input weight"));

        let mut empty_accumulator = one_group();
        empty_accumulator.acc_state_ipc[0].clear();
        cases.push((empty_accumulator, "empty accumulator state"));

        let mut noncanonical_empty = one_group();
        noncanonical_empty.last_updated_ms.clear();
        cases.push((noncanonical_empty, "non-canonical empty"));

        let mut unexpected_changelog = one_group();
        unexpected_changelog.last_emitted.push(EmittedCheckpoint {
            key: vec![1],
            values: vec![1],
        });
        cases.push((unexpected_changelog, "non-changelog query"));

        let mut too_many_groups = one_group();
        too_many_groups.last_updated_ms.push(0);
        cases.push((too_many_groups, "group limit exceeded"));

        for (checkpoint, expected) in cases {
            let encoded = encode(&checkpoint);
            let error = grouped_profile()
                .preflight(&encoded, format_args!("test"))
                .err()
                .expect("the malformed archive must fail preflight");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn aggregate_archive_preflight_rejects_invalid_global_shapes() {
        let global_profile = AggStateArchiveRestoreProfile::new(FINGERPRINT, 1, 2, 0, 1, false);

        let mut keyed_global = one_group();
        let encoded = encode(&keyed_global);
        let error = global_profile
            .preflight(&encoded, format_args!("global"))
            .err()
            .expect("global key bytes must fail preflight");
        assert!(error.to_string().contains("contains key bytes"), "{error}");

        keyed_global.keys_ipc.clear();
        keyed_global.input_weights.push(1);
        keyed_global.last_updated_ms.push(0);
        let encoded = encode(&keyed_global);
        let error = global_profile
            .preflight(&encoded, format_args!("global"))
            .err()
            .expect("multiple global rows must fail preflight");
        assert!(error.to_string().contains("contains 2 groups"), "{error}");
    }

    #[test]
    fn aggregate_archive_preflight_accounts_owned_decode_before_deserialization() {
        let encoded = encode(&one_group());
        let preflighted = grouped_profile()
            .preflight(&encoded, format_args!("owned accounting"))
            .unwrap();
        let restore = preflighted.restore_preflight();
        let owned = preflighted
            .deserialize(format_args!("owned accounting"))
            .unwrap();

        assert_eq!(
            restore.owned_state_bytes(),
            owned.retained_serialization_bytes().unwrap()
        );
        assert!(restore.decode_scratch_bytes() > restore.owned_state_bytes());
        assert!(restore.final_state_upper_bytes() > 0);
    }

    #[test]
    fn aggregate_archive_preflight_rejects_noncanonical_ipc_before_owned_decode() {
        let canonical = one_group();

        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["key"])) as ArrayRef],
        )
        .unwrap();
        let mut multiple = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut multiple, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let mut trailing = canonical.clone();
        trailing.keys_ipc.push(0);
        let mut wrong_rows = canonical.clone();
        wrong_rows.keys_ipc = ipc(vec![Arc::new(StringArray::from(vec!["a", "b"]))]);
        let mut wide_accumulator = canonical.clone();
        wide_accumulator.acc_state_ipc[0] = ipc(vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![3])),
        ]);
        let mut multiple_batches = canonical;
        multiple_batches.keys_ipc = multiple;

        for (checkpoint, expected) in [
            (trailing, "non-canonical"),
            (wrong_rows, "rows; expected"),
            (wide_accumulator, "columns; expected"),
            (multiple_batches, "message order is non-canonical"),
        ] {
            let encoded = encode(&checkpoint);
            let error = grouped_profile()
                .preflight(&encoded, format_args!("IPC shape"))
                .err()
                .expect("malformed IPC must fail borrowed preflight");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn scalar_ipc_preflight_accepts_canonical_view_and_dictionary_layouts() {
        let view = ipc(vec![Arc::new(StringViewArray::from(vec![
            "a View payload longer than twelve bytes",
        ]))]);
        let view_preflight =
            preflight_scalar_ipc_restore(&view, 1, 1, 1, format_args!("View")).unwrap();
        assert!(view_preflight.shared_payload_bytes > 0);

        let dictionary: DictionaryArray<Int32Type> = ["dictionary value"].into_iter().collect();
        let dictionary = ipc(vec![Arc::new(dictionary)]);
        let dictionary_preflight =
            preflight_scalar_ipc_restore(&dictionary, 1, 1, 1, format_args!("dictionary")).unwrap();
        assert_eq!(dictionary_preflight.dictionary_rows, 1);
        assert!(dictionary_preflight.dictionary_body_bytes > 0);

        let empty_dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![None]),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        )
        .unwrap();
        let empty_dictionary = ipc(vec![Arc::new(empty_dictionary)]);
        let empty_preflight = preflight_scalar_ipc_restore(
            &empty_dictionary,
            1,
            1,
            1,
            format_args!("empty dictionary"),
        )
        .unwrap();
        assert_eq!(empty_preflight.dictionary_rows, 0);

        let preloaded_dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0]),
            Arc::new(StringArray::from(vec!["used", "unused"])),
        )
        .unwrap();
        let preloaded_dictionary = ipc(vec![Arc::new(preloaded_dictionary)]);
        let preloaded_preflight = preflight_scalar_ipc_restore(
            &preloaded_dictionary,
            1,
            1,
            1,
            format_args!("preloaded dictionary"),
        )
        .unwrap();
        assert_eq!(preloaded_preflight.dictionary_rows, 2);

        let zero_width = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            std::iter::once(Some(&[] as &[u8])),
            0,
        )
        .unwrap();
        let zero_width = ipc(vec![Arc::new(zero_width)]);
        preflight_scalar_ipc_restore(&zero_width, 1, 1, 1, format_args!("zero-width fixed"))
            .unwrap();
    }

    #[test]
    fn scalar_ipc_preflight_rejects_corrupt_nested_layouts() {
        let mut oversized = ipc(vec![Arc::new(Int64Array::from(vec![1]))]);
        let descriptor = record_buffer_descriptor_start(&oversized);
        oversized[descriptor + 8..descriptor + 16].copy_from_slice(&i64::MAX.to_le_bytes());

        let mut overlapping = ipc(vec![Arc::new(Int64Array::from(vec![1]))]);
        let descriptor = record_buffer_descriptor_start(&overlapping);
        overlapping[descriptor + 16..descriptor + 24].copy_from_slice(&0_i64.to_le_bytes());

        let mut view = ipc(vec![Arc::new(StringViewArray::from(vec![
            "a View payload longer than twelve bytes",
        ]))]);
        let variadic = record_variadic_count_start(&view);
        view[variadic..variadic + 8].copy_from_slice(&i64::MAX.to_le_bytes());

        let dictionary_a: DictionaryArray<Int32Type> = ["dictionary a"].into_iter().collect();
        let dictionary_b: DictionaryArray<Int32Type> = ["dictionary b"].into_iter().collect();
        let mut dictionary = ipc(vec![Arc::new(dictionary_a), Arc::new(dictionary_b)]);
        let dictionary_id = dictionary_id_start(&dictionary);
        dictionary[dictionary_id..dictionary_id + 8].copy_from_slice(&99_i64.to_le_bytes());

        for (bytes, columns, expected) in [
            (oversized, 1, "buffer exceeds"),
            (overlapping, 1, "overlap"),
            (view, 1, "variadic count exceeds"),
            (dictionary, 2, "dictionary roster"),
        ] {
            let error = preflight_scalar_ipc_restore(
                &bytes,
                1,
                columns,
                columns,
                format_args!("nested corruption"),
            )
            .unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }
    }
}
