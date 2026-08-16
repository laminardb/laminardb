//! Canonical typed partition-key encoding for partitioning ABI v1.
//!
//! The encoded bytes are Arrow row-format bytes produced with ascending,
//! nulls-first sort fields. They are schema-bound rather than self-describing.
//! The separately constructed [`PartitionKeySchemaV1`] freezes the persisted
//! schema descriptor. It is not constructed by routing and adds no work to the
//! record-processing hot path.
//! Changing the admitted type set, row encoding, hash, or vnode mapping requires
//! a partitioning ABI decision.

use std::num::NonZeroU32;

use arrow_array::types::{
    validate_decimal_precision_and_scale, Decimal128Type, Decimal256Type, Decimal32Type,
    Decimal64Type, DecimalType,
};
use arrow_array::ArrayRef;
use arrow_row::{RowConverter, Rows, SortField};
use arrow_schema::{DataType, FieldRef, TimeUnit};

pub use super::partition_key_schema::PartitionKeySchemaV1;
use super::vnode::key_hash;

// These are structural ABI safety ceilings, not deployment SLOs. A numerical
// workload profile may admit less but cannot raise them without an ABI review.
/// Maximum number of columns retained by one ABI-v1 row-converter plan.
pub(crate) const MAX_PARTITION_KEY_COLUMNS: usize = 256;
/// Maximum recursive dictionary wrappers inspected while hydrating one key type.
pub(crate) const MAX_PARTITION_KEY_NESTING: usize = 32;
/// Maximum timezone parameter bytes copied into one routing descriptor.
pub(crate) const MAX_PARTITION_KEY_TIMEZONE_BYTES: usize = 256;

/// Failure to construct the ABI-v1 typed-key encoder.
#[derive(Debug, thiserror::Error)]
pub enum PartitionKeyCodecError {
    /// A keyed partitioning codec requires at least one logical key column.
    #[error("partition key schema is empty")]
    EmptyKeySchema,
    /// The composite key is too wide for bounded plan-time construction.
    #[error("partition key has {count} columns; hard limit is {limit}")]
    TooManyKeyColumns {
        /// Requested key-column count.
        count: usize,
        /// Hard key-column limit.
        limit: usize,
    },
    /// Arrow extension metadata has no routing semantics in partitioning ABI v1.
    #[error("partition key column {index} has metadata unsupported by ABI v1")]
    UnsupportedKeyMetadata {
        /// Zero-based position in the composite key.
        index: usize,
    },
    /// Recursive dictionary hydration is bounded before Arrow row construction.
    #[error("partition key column {index} exceeds dictionary nesting depth {limit}")]
    KeyTypeNestingTooDeep {
        /// Zero-based position in the composite key.
        index: usize,
        /// Hard dictionary nesting limit.
        limit: usize,
    },
    /// A variable type parameter crossed its partition ABI resource bound.
    #[error(
        "partition key column {index} {parameter} occupies {bytes} bytes; hard limit is {limit}"
    )]
    KeyTypeParameterTooLarge {
        /// Zero-based position in the composite key.
        index: usize,
        /// Bounded type parameter.
        parameter: &'static str,
        /// Observed parameter bytes.
        bytes: usize,
        /// Hard parameter limit.
        limit: usize,
    },
    /// The resolved key type has no frozen ABI-v1 equality and encoding contract.
    #[error("partition key column {index} has unsupported ABI-v1 type {data_type}")]
    UnsupportedKeyType {
        /// Zero-based position in the composite key.
        index: usize,
        /// Rejected Arrow type.
        data_type: DataType,
    },
    /// A recursive Arrow family is rejected without cloning attacker-shaped schema trees.
    #[error("partition key column {index} has unsupported ABI-v1 type family {family}")]
    UnsupportedKeyTypeFamily {
        /// Zero-based position in the composite key.
        index: usize,
        /// Stable rejected family label.
        family: &'static str,
    },
    /// Arrow rejected the otherwise admitted row layout.
    #[error("partition key Arrow row encoding: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    /// The opaque routing-schema descriptor crossed its hard allocation bound.
    #[error("partition key schema descriptor exceeds {limit} bytes")]
    KeySchemaDescriptorTooLarge {
        /// Hard encoded descriptor limit.
        limit: usize,
    },
}

/// Vectorized encoder for the exact typed-key representation covered by
/// [`super::PARTITIONING_ABI_VERSION`] 1.
///
/// Floats, nested values, and run-end encoding remain fail-closed. Dictionary
/// indices are physical representation: supported dictionaries encode their
/// hydrated scalar values.
#[derive(Debug)]
pub struct PartitionKeyCodecV1 {
    converter: RowConverter,
}

/// Internal one-pass builder used where key-column lookup and validation must
/// preserve their original request order.
pub(crate) struct PartitionKeyCodecV1Builder {
    fields: Vec<SortField>,
}

impl PartitionKeyCodecV1Builder {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            fields: Vec::with_capacity(capacity.min(MAX_PARTITION_KEY_COLUMNS)),
        }
    }

    pub(crate) fn push(&mut self, data_type: DataType) -> Result<(), PartitionKeyCodecError> {
        let index = self.fields.len();
        if index >= MAX_PARTITION_KEY_COLUMNS {
            return Err(PartitionKeyCodecError::TooManyKeyColumns {
                count: index + 1,
                limit: MAX_PARTITION_KEY_COLUMNS,
            });
        }
        validate_key_type(&data_type, index, 0)?;
        self.fields.push(SortField::new(data_type));
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<PartitionKeyCodecV1, PartitionKeyCodecError> {
        if self.fields.is_empty() {
            return Err(PartitionKeyCodecError::EmptyKeySchema);
        }
        Ok(PartitionKeyCodecV1 {
            converter: RowConverter::new(self.fields)?,
        })
    }
}

impl PartitionKeyCodecV1 {
    /// Construct a codec for one resolved, ordered composite-key schema.
    ///
    /// # Errors
    /// Returns the first unsupported key type or an Arrow row-layout error.
    pub fn try_new(
        data_types: impl IntoIterator<Item = DataType>,
    ) -> Result<Self, PartitionKeyCodecError> {
        let data_types = data_types.into_iter();
        let mut builder = PartitionKeyCodecV1Builder::with_capacity(data_types.size_hint().0);
        for data_type in data_types {
            builder.push(data_type)?;
        }
        builder.finish()
    }

    /// Encode all key columns in one vectorized Arrow-row pass.
    ///
    /// The returned rows borrow no input buffers and can be hashed or copied
    /// into operator state. Column types and arity must match construction.
    ///
    /// # Errors
    /// Returns an Arrow error for a mismatched schema or column length.
    pub fn encode_columns(&self, columns: &[ArrayRef]) -> Result<Rows, arrow_schema::ArrowError> {
        self.converter.convert_columns(columns)
    }

    /// Full ABI-v1 hash of one encoded key.
    #[must_use]
    pub fn hash_encoded(encoded: &[u8]) -> u64 {
        key_hash(encoded)
    }

    /// Map one encoded key into a nonempty vnode space.
    #[must_use]
    pub fn vnode_for_encoded(encoded: &[u8], vnode_count: NonZeroU32) -> u32 {
        #[allow(clippy::cast_possible_truncation)]
        {
            (Self::hash_encoded(encoded) % u64::from(vnode_count.get())) as u32
        }
    }
}

pub(crate) fn validate_key_fields(fields: &[FieldRef]) -> Result<(), PartitionKeyCodecError> {
    if fields.is_empty() {
        return Err(PartitionKeyCodecError::EmptyKeySchema);
    }
    if fields.len() > MAX_PARTITION_KEY_COLUMNS {
        return Err(PartitionKeyCodecError::TooManyKeyColumns {
            count: fields.len(),
            limit: MAX_PARTITION_KEY_COLUMNS,
        });
    }
    for (index, field) in fields.iter().enumerate() {
        if !field.metadata().is_empty() {
            return Err(PartitionKeyCodecError::UnsupportedKeyMetadata { index });
        }
        validate_key_type(field.data_type(), index, 0)?;
    }
    Ok(())
}

pub(crate) fn validate_key_type(
    data_type: &DataType,
    index: usize,
    depth: usize,
) -> Result<(), PartitionKeyCodecError> {
    if depth > MAX_PARTITION_KEY_NESTING {
        return Err(PartitionKeyCodecError::KeyTypeNestingTooDeep {
            index,
            limit: MAX_PARTITION_KEY_NESTING,
        });
    }

    match data_type {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Date32
        | DataType::Date64
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => Ok(()),
        DataType::Timestamp(_, timezone) => {
            let bytes = timezone.as_ref().map_or(0, |value| value.len());
            if bytes <= MAX_PARTITION_KEY_TIMEZONE_BYTES {
                Ok(())
            } else {
                Err(PartitionKeyCodecError::KeyTypeParameterTooLarge {
                    index,
                    parameter: "timezone",
                    bytes,
                    limit: MAX_PARTITION_KEY_TIMEZONE_BYTES,
                })
            }
        }
        DataType::FixedSizeBinary(length) if *length >= 0 => Ok(()),
        DataType::Time32(unit) => {
            if matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) {
                Ok(())
            } else {
                Err(PartitionKeyCodecError::UnsupportedKeyType {
                    index,
                    data_type: data_type.clone(),
                })
            }
        }
        DataType::Time64(unit) => {
            if matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond) {
                Ok(())
            } else {
                Err(PartitionKeyCodecError::UnsupportedKeyType {
                    index,
                    data_type: data_type.clone(),
                })
            }
        }
        DataType::Decimal32(precision, scale)
            if valid_decimal::<Decimal32Type>(*precision, *scale) =>
        {
            Ok(())
        }
        DataType::Decimal64(precision, scale)
            if valid_decimal::<Decimal64Type>(*precision, *scale) =>
        {
            Ok(())
        }
        DataType::Decimal128(precision, scale)
            if valid_decimal::<Decimal128Type>(*precision, *scale) =>
        {
            Ok(())
        }
        DataType::Decimal256(precision, scale)
            if valid_decimal::<Decimal256Type>(*precision, *scale) =>
        {
            Ok(())
        }
        DataType::Dictionary(indices, values)
            if matches!(
                indices.as_ref(),
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
            ) =>
        {
            validate_key_type(values, index, depth + 1)
        }
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::FixedSizeBinary(_)
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => Err(PartitionKeyCodecError::UnsupportedKeyType {
            index,
            data_type: data_type.clone(),
        }),
        DataType::List(_) => unsupported_family(index, "list"),
        DataType::ListView(_) => unsupported_family(index, "list-view"),
        DataType::FixedSizeList(_, _) => unsupported_family(index, "fixed-size-list"),
        DataType::LargeList(_) => unsupported_family(index, "large-list"),
        DataType::LargeListView(_) => unsupported_family(index, "large-list-view"),
        DataType::Struct(_) => unsupported_family(index, "struct"),
        DataType::Union(_, _) => unsupported_family(index, "union"),
        DataType::Map(_, _) => unsupported_family(index, "map"),
        DataType::RunEndEncoded(_, _) => unsupported_family(index, "run-end-encoded"),
        DataType::Dictionary(_, _) => unsupported_family(index, "dictionary-index"),
    }
}

fn unsupported_family(index: usize, family: &'static str) -> Result<(), PartitionKeyCodecError> {
    Err(PartitionKeyCodecError::UnsupportedKeyTypeFamily { index, family })
}

fn valid_decimal<T: DecimalType>(precision: u8, scale: i8) -> bool {
    validate_decimal_precision_and_scale::<T>(precision, scale).is_ok()
}

#[cfg(test)]
mod tests;
