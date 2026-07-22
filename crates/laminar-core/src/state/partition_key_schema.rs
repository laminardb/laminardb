//! Persisted schema identity for partitioning ABI v1.
//!
//! This is deliberately narrower than a physical Arrow or state-artifact
//! schema. SQL aliases are not routing identity, dictionary keys are hydrated,
//! and field metadata is rejected by [`PartitionKeySchemaV1::try_new`].

use arrow_schema::{DataType, FieldRef, IntervalUnit, TimeUnit};
use sha2::{Digest, Sha256};

use super::partition_key::{validate_key_type, PartitionKeyCodecError};

/// Version of the partition-key schema descriptor byte format.
pub const PARTITION_KEY_SCHEMA_VERSION: u16 = 1;
/// Hard allocation bound for one partition-key schema descriptor.
pub const MAX_PARTITION_KEY_SCHEMA_BYTES: usize = 128 * 1024;

const MAGIC: &[u8; 8] = b"LDBPKS\0\0";

/// Persistable routing identity for an ordered partition-key schema.
///
/// The descriptor is opaque. Its bytes are a persisted ABI and may only change
/// with an explicit partition-key schema version decision and new goldens.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionKeySchemaV1 {
    bytes: Box<[u8]>,
    sha256: [u8; 32],
}

impl PartitionKeySchemaV1 {
    /// Validate and describe one ordered, resolved partition-key schema.
    ///
    /// Dictionary index widths are physical transport details and recursively
    /// hydrate to the value type. Field names are SQL aliases and are omitted;
    /// order and nullability remain identity.
    ///
    /// # Errors
    /// Returns an error for an empty, over-wide, metadata-bearing, unsupported,
    /// over-nested, or over-sized routing schema.
    pub fn try_new(fields: &[FieldRef]) -> Result<Self, PartitionKeyCodecError> {
        super::partition_key::validate_key_fields(fields)?;

        let mut output = DescriptorWriter::new()?;
        output.write_len(fields.len())?;
        for (index, field) in fields.iter().enumerate() {
            output.write_bool(field.is_nullable())?;
            write_data_type(&mut output, field.data_type(), index)?;
        }

        let sha256 = Sha256::digest(&output.bytes).into();
        Ok(Self {
            bytes: output.bytes.into_boxed_slice(),
            sha256,
        })
    }

    /// Canonical, versioned routing-schema bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// SHA-256 of [`Self::as_bytes`].
    #[must_use]
    pub const fn sha256(&self) -> [u8; 32] {
        self.sha256
    }
}

struct DescriptorWriter {
    bytes: Vec<u8>,
}

impl DescriptorWriter {
    fn new() -> Result<Self, PartitionKeyCodecError> {
        let mut writer = Self { bytes: Vec::new() };
        writer.write_raw(MAGIC)?;
        writer.write_raw(&PARTITION_KEY_SCHEMA_VERSION.to_be_bytes())?;
        Ok(writer)
    }

    fn write_u8(&mut self, value: u8) -> Result<(), PartitionKeyCodecError> {
        self.write_raw(&[value])
    }

    fn write_bool(&mut self, value: bool) -> Result<(), PartitionKeyCodecError> {
        self.write_u8(u8::from(value))
    }

    fn write_len(&mut self, value: usize) -> Result<(), PartitionKeyCodecError> {
        #[allow(clippy::cast_possible_truncation)]
        self.write_raw(&(value as u64).to_be_bytes())
    }

    fn write_bytes(&mut self, value: &[u8]) -> Result<(), PartitionKeyCodecError> {
        self.write_len(value.len())?;
        self.write_raw(value)
    }

    fn write_raw(&mut self, value: &[u8]) -> Result<(), PartitionKeyCodecError> {
        let next_len = self.bytes.len().checked_add(value.len()).ok_or(
            PartitionKeyCodecError::KeySchemaDescriptorTooLarge {
                limit: MAX_PARTITION_KEY_SCHEMA_BYTES,
            },
        )?;
        if next_len > MAX_PARTITION_KEY_SCHEMA_BYTES {
            return Err(PartitionKeyCodecError::KeySchemaDescriptorTooLarge {
                limit: MAX_PARTITION_KEY_SCHEMA_BYTES,
            });
        }
        if next_len > self.bytes.capacity() {
            let target_capacity = self
                .bytes
                .capacity()
                .max(256)
                .saturating_mul(2)
                .max(next_len)
                .min(MAX_PARTITION_KEY_SCHEMA_BYTES);
            self.bytes
                .try_reserve_exact(target_capacity - self.bytes.len())
                .map_err(|_| PartitionKeyCodecError::KeySchemaDescriptorTooLarge {
                    limit: MAX_PARTITION_KEY_SCHEMA_BYTES,
                })?;
        }
        if self.bytes.capacity() > MAX_PARTITION_KEY_SCHEMA_BYTES {
            return Err(PartitionKeyCodecError::KeySchemaDescriptorTooLarge {
                limit: MAX_PARTITION_KEY_SCHEMA_BYTES,
            });
        }
        self.bytes.extend_from_slice(value);
        Ok(())
    }
}

// These tags are LaminarDB's persisted ABI, not Arrow enum discriminants. The
// exhaustive match makes an Arrow upgrade stop here until the routing policy is
// reviewed. `validate_key_type` is shared with the row codec and runs first.
fn write_data_type(
    output: &mut DescriptorWriter,
    data_type: &DataType,
    index: usize,
) -> Result<(), PartitionKeyCodecError> {
    validate_key_type(data_type, index, 0)?;
    match data_type {
        DataType::Null => output.write_u8(0),
        DataType::Boolean => output.write_u8(1),
        DataType::Int8 => output.write_u8(2),
        DataType::Int16 => output.write_u8(3),
        DataType::Int32 => output.write_u8(4),
        DataType::Int64 => output.write_u8(5),
        DataType::UInt8 => output.write_u8(6),
        DataType::UInt16 => output.write_u8(7),
        DataType::UInt32 => output.write_u8(8),
        DataType::UInt64 => output.write_u8(9),
        DataType::Timestamp(unit, timezone) => {
            output.write_u8(10)?;
            write_time_unit(output, unit)?;
            match timezone {
                None => output.write_u8(0),
                Some(timezone) => {
                    output.write_u8(1)?;
                    output.write_bytes(timezone.as_bytes())
                }
            }
        }
        DataType::Date32 => output.write_u8(11),
        DataType::Date64 => output.write_u8(12),
        DataType::Time32(unit) => {
            output.write_u8(13)?;
            write_time_unit(output, unit)
        }
        DataType::Time64(unit) => {
            output.write_u8(14)?;
            write_time_unit(output, unit)
        }
        DataType::Duration(unit) => {
            output.write_u8(15)?;
            write_time_unit(output, unit)
        }
        DataType::Interval(unit) => {
            output.write_u8(16)?;
            write_interval_unit(output, unit)
        }
        DataType::Binary => output.write_u8(17),
        DataType::FixedSizeBinary(width) => {
            output.write_u8(18)?;
            output.write_raw(&width.to_be_bytes())
        }
        DataType::LargeBinary => output.write_u8(19),
        DataType::BinaryView => output.write_u8(20),
        DataType::Utf8 => output.write_u8(21),
        DataType::LargeUtf8 => output.write_u8(22),
        DataType::Utf8View => output.write_u8(23),
        DataType::Dictionary(_, values) => write_data_type(output, values, index),
        DataType::Decimal32(precision, scale) => {
            output.write_u8(24)?;
            output.write_u8(*precision)?;
            output.write_raw(&scale.to_be_bytes())
        }
        DataType::Decimal64(precision, scale) => {
            output.write_u8(25)?;
            output.write_u8(*precision)?;
            output.write_raw(&scale.to_be_bytes())
        }
        DataType::Decimal128(precision, scale) => {
            output.write_u8(26)?;
            output.write_u8(*precision)?;
            output.write_raw(&scale.to_be_bytes())
        }
        DataType::Decimal256(precision, scale) => {
            output.write_u8(27)?;
            output.write_u8(*precision)?;
            output.write_raw(&scale.to_be_bytes())
        }
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::List(_)
        | DataType::ListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::LargeList(_)
        | DataType::LargeListView(_)
        | DataType::Struct(_)
        | DataType::Union(_, _)
        | DataType::Map(_, _)
        | DataType::RunEndEncoded(_, _) => Err(PartitionKeyCodecError::UnsupportedKeyTypeFamily {
            index,
            family: "non-scalar",
        }),
    }
}

fn write_time_unit(
    output: &mut DescriptorWriter,
    unit: &TimeUnit,
) -> Result<(), PartitionKeyCodecError> {
    output.write_u8(match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    })
}

fn write_interval_unit(
    output: &mut DescriptorWriter,
    unit: &IntervalUnit,
) -> Result<(), PartitionKeyCodecError> {
    output.write_u8(match unit {
        IntervalUnit::YearMonth => 0,
        IntervalUnit::DayTime => 1,
        IntervalUnit::MonthDayNano => 2,
    })
}
