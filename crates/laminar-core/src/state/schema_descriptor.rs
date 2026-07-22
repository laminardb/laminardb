//! Stable, self-contained Arrow schema descriptors for checkpoint artifacts.
//!
//! This is deliberately a handwritten encoding rather than Arrow IPC or
//! `Debug`/serde output. Those formats may change independently of `LaminarDB`'s
//! state compatibility contract. Any change to the bytes emitted here requires
//! a descriptor version decision and new golden vectors.

use arrow_schema::{DataType, Field, FieldRef, IntervalUnit, TimeUnit, UnionMode};
use sha2::{Digest, Sha256};

/// Version of the canonical schema-descriptor byte format.
pub const SCHEMA_DESCRIPTOR_VERSION: u16 = 1;

const MAGIC: &[u8; 8] = b"LDBSCHM\0";

/// Canonical schema-slot bytes and their SHA-256 digest.
///
/// Top-level slots are ordered and encode nullability and data type. Their
/// column names and field metadata are intentionally excluded. Nested Arrow
/// fields are part of a data type and therefore encode name, nullability, and
/// metadata (sorted by metadata key).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaDescriptorV1 {
    bytes: Box<[u8]>,
    sha256: [u8; 32],
}

impl SchemaDescriptorV1 {
    /// Build an exact descriptor for ordered top-level Arrow fields.
    #[must_use]
    pub fn from_fields(fields: &[FieldRef]) -> Self {
        Self::build(fields, DictionaryEncoding::Exact)
    }

    pub(crate) fn from_fields_hydrating_dictionaries(fields: &[FieldRef]) -> Self {
        Self::build(fields, DictionaryEncoding::Hydrated)
    }

    fn build(fields: &[FieldRef], dictionary_encoding: DictionaryEncoding) -> Self {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&SCHEMA_DESCRIPTOR_VERSION.to_be_bytes());
        write_len(&mut bytes, fields.len());
        for field in fields {
            write_bool(&mut bytes, field.is_nullable());
            write_data_type(&mut bytes, field.data_type(), dictionary_encoding);
        }

        let sha256 = Sha256::digest(&bytes).into();
        Self {
            bytes: bytes.into_boxed_slice(),
            sha256,
        }
    }

    /// Canonical, versioned descriptor bytes.
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

#[derive(Clone, Copy)]
enum DictionaryEncoding {
    Exact,
    Hydrated,
}

// These tags and parameter encodings are persisted ABI, not Arrow enum
// discriminants. Keeping this match exhaustive makes an Arrow upgrade fail at
// compile time until every new logical type has an explicit representation.
fn write_data_type(output: &mut Vec<u8>, data_type: &DataType, dictionaries: DictionaryEncoding) {
    match data_type {
        DataType::Null => output.push(0),
        DataType::Boolean => output.push(1),
        DataType::Int8 => output.push(2),
        DataType::Int16 => output.push(3),
        DataType::Int32 => output.push(4),
        DataType::Int64 => output.push(5),
        DataType::UInt8 => output.push(6),
        DataType::UInt16 => output.push(7),
        DataType::UInt32 => output.push(8),
        DataType::UInt64 => output.push(9),
        DataType::Float16 => output.push(10),
        DataType::Float32 => output.push(11),
        DataType::Float64 => output.push(12),
        DataType::Timestamp(unit, timezone) => {
            output.push(13);
            write_time_unit(output, unit);
            match timezone {
                None => output.push(0),
                Some(timezone) => {
                    output.push(1);
                    write_bytes(output, timezone.as_bytes());
                }
            }
        }
        DataType::Date32 => output.push(14),
        DataType::Date64 => output.push(15),
        DataType::Time32(unit) => {
            output.push(16);
            write_time_unit(output, unit);
        }
        DataType::Time64(unit) => {
            output.push(17);
            write_time_unit(output, unit);
        }
        DataType::Duration(unit) => {
            output.push(18);
            write_time_unit(output, unit);
        }
        DataType::Interval(unit) => {
            output.push(19);
            write_interval_unit(output, unit);
        }
        DataType::Binary => output.push(20),
        DataType::FixedSizeBinary(width) => {
            output.push(21);
            output.extend_from_slice(&width.to_be_bytes());
        }
        DataType::LargeBinary => output.push(22),
        DataType::BinaryView => output.push(23),
        DataType::Utf8 => output.push(24),
        DataType::LargeUtf8 => output.push(25),
        DataType::Utf8View => output.push(26),
        DataType::List(field) => {
            output.push(27);
            write_field(output, field, dictionaries);
        }
        DataType::ListView(field) => {
            output.push(28);
            write_field(output, field, dictionaries);
        }
        DataType::FixedSizeList(field, length) => {
            output.push(29);
            write_field(output, field, dictionaries);
            output.extend_from_slice(&length.to_be_bytes());
        }
        DataType::LargeList(field) => {
            output.push(30);
            write_field(output, field, dictionaries);
        }
        DataType::LargeListView(field) => {
            output.push(31);
            write_field(output, field, dictionaries);
        }
        DataType::Struct(fields) => {
            output.push(32);
            write_len(output, fields.len());
            for field in fields {
                write_field(output, field, dictionaries);
            }
        }
        DataType::Union(fields, mode) => {
            output.push(33);
            write_union_mode(output, mode);
            write_len(output, fields.len());
            for (type_id, field) in fields.iter() {
                output.extend_from_slice(&type_id.to_be_bytes());
                write_field(output, field, dictionaries);
            }
        }
        DataType::Dictionary(indices, values) => match dictionaries {
            DictionaryEncoding::Exact => {
                output.push(34);
                write_data_type(output, indices, dictionaries);
                write_data_type(output, values, dictionaries);
            }
            DictionaryEncoding::Hydrated => write_data_type(output, values, dictionaries),
        },
        DataType::Decimal32(precision, scale) => {
            output.push(35);
            output.push(*precision);
            output.extend_from_slice(&scale.to_be_bytes());
        }
        DataType::Decimal64(precision, scale) => {
            output.push(36);
            output.push(*precision);
            output.extend_from_slice(&scale.to_be_bytes());
        }
        DataType::Decimal128(precision, scale) => {
            output.push(37);
            output.push(*precision);
            output.extend_from_slice(&scale.to_be_bytes());
        }
        DataType::Decimal256(precision, scale) => {
            output.push(38);
            output.push(*precision);
            output.extend_from_slice(&scale.to_be_bytes());
        }
        DataType::Map(field, sorted) => {
            output.push(39);
            write_field(output, field, dictionaries);
            write_bool(output, *sorted);
        }
        DataType::RunEndEncoded(run_ends, values) => {
            output.push(40);
            write_field(output, run_ends, dictionaries);
            write_field(output, values, dictionaries);
        }
    }
}

fn write_field(output: &mut Vec<u8>, field: &Field, dictionaries: DictionaryEncoding) {
    write_bytes(output, field.name().as_bytes());
    write_bool(output, field.is_nullable());

    let mut metadata = field.metadata().iter().collect::<Vec<_>>();
    metadata.sort_unstable_by_key(|(key, _)| *key);
    write_len(output, metadata.len());
    for (key, value) in metadata {
        write_bytes(output, key.as_bytes());
        write_bytes(output, value.as_bytes());
    }

    write_data_type(output, field.data_type(), dictionaries);
}

fn write_time_unit(output: &mut Vec<u8>, unit: &TimeUnit) {
    output.push(match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    });
}

fn write_interval_unit(output: &mut Vec<u8>, unit: &IntervalUnit) {
    output.push(match unit {
        IntervalUnit::YearMonth => 0,
        IntervalUnit::DayTime => 1,
        IntervalUnit::MonthDayNano => 2,
    });
}

fn write_union_mode(output: &mut Vec<u8>, mode: &UnionMode) {
    output.push(match mode {
        UnionMode::Sparse => 0,
        UnionMode::Dense => 1,
    });
}

fn write_bool(output: &mut Vec<u8>, value: bool) {
    output.push(u8::from(value));
}

fn write_bytes(output: &mut Vec<u8>, value: &[u8]) {
    write_len(output, value.len());
    output.extend_from_slice(value);
}

fn write_len(output: &mut Vec<u8>, value: usize) {
    #[allow(clippy::cast_possible_truncation)]
    output.extend_from_slice(&(value as u64).to_be_bytes());
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fmt::Write as _;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Fields, IntervalUnit, TimeUnit, UnionFields, UnionMode};

    use super::*;

    fn descriptor(fields: Vec<Field>) -> SchemaDescriptorV1 {
        let fields = fields.into_iter().map(Arc::new).collect::<Vec<_>>();
        SchemaDescriptorV1::from_fields(&fields)
    }

    fn hex(bytes: &[u8]) -> String {
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut output, "{byte:02x}").unwrap();
        }
        output
    }

    #[test]
    fn descriptor_v1_has_golden_bytes_and_digest() {
        let nested = Field::new(
            "items",
            DataType::List(Arc::new(
                Field::new("element", DataType::Decimal128(12, 3), true).with_metadata(
                    HashMap::from([
                        ("zeta".to_owned(), "last".to_owned()),
                        ("alpha".to_owned(), "first".to_owned()),
                    ]),
                ),
            )),
            false,
        );
        let value = descriptor(vec![
            Field::new("ignored_a", DataType::Int64, false),
            Field::new(
                "ignored_b",
                DataType::Timestamp(TimeUnit::Microsecond, Some("Europe/London".into())),
                true,
            ),
            nested,
        ]);

        assert_eq!(
            hex(value.as_bytes()),
            "4c44425343484d00000100000000000000030005010d0201000000000000000d4575726f70652f4c6f6e646f6e001b0000000000000007656c656d656e740100000000000000020000000000000005616c7068610000000000000005666972737400000000000000047a65746100000000000000046c617374250c03"
        );
        assert_eq!(
            hex(&value.sha256()),
            "1296d88d774d948bb4001df60caa202197c72842e3b1872f409a4dc1c4404126"
        );
    }

    #[test]
    fn top_level_names_are_ignored_but_order_and_nullability_are_not() {
        let base = descriptor(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let renamed = descriptor(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, true),
        ]);
        let reordered = descriptor(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int32, false),
        ]);
        let nullability = descriptor(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);

        assert_eq!(base, renamed);
        assert_ne!(base.sha256(), reordered.sha256());
        assert_ne!(base.sha256(), nullability.sha256());
    }

    #[test]
    fn parameters_and_nested_field_identity_cannot_alias() {
        let one = |data_type| descriptor(vec![Field::new("ignored", data_type, true)]);

        assert_ne!(
            one(DataType::Timestamp(TimeUnit::Second, None)),
            one(DataType::Timestamp(TimeUnit::Second, Some("UTC".into())))
        );
        assert_ne!(
            one(DataType::Timestamp(TimeUnit::Second, Some("UTC".into()))),
            one(DataType::Timestamp(
                TimeUnit::Millisecond,
                Some("UTC".into())
            ))
        );
        assert_ne!(
            one(DataType::Decimal128(12, 2)),
            one(DataType::Decimal128(12, 3))
        );
        assert_ne!(
            one(DataType::Decimal128(12, 2)),
            one(DataType::Decimal128(13, 2))
        );

        let child = |name: &str, nullable: bool, metadata: HashMap<String, String>| {
            one(DataType::List(Arc::new(
                Field::new(name, DataType::Int64, nullable).with_metadata(metadata),
            )))
        };
        assert_ne!(
            child("left", false, HashMap::new()),
            child("right", false, HashMap::new())
        );
        assert_ne!(
            child("item", false, HashMap::new()),
            child("item", true, HashMap::new())
        );
        assert_ne!(
            child("item", false, HashMap::new()),
            child(
                "item",
                false,
                HashMap::from([("extension".to_owned(), "v1".to_owned())])
            )
        );
    }

    #[test]
    fn nested_metadata_order_is_canonical() {
        let metadata_a = HashMap::from([
            ("z".to_owned(), "2".to_owned()),
            ("a".to_owned(), "1".to_owned()),
        ]);
        let metadata_b = HashMap::from([
            ("a".to_owned(), "1".to_owned()),
            ("z".to_owned(), "2".to_owned()),
        ]);
        let list = |metadata| {
            descriptor(vec![Field::new(
                "ignored",
                DataType::List(Arc::new(
                    Field::new("item", DataType::Utf8, true).with_metadata(metadata),
                )),
                false,
            )])
        };

        assert_eq!(list(metadata_a), list(metadata_b));
    }

    #[test]
    fn exact_descriptor_preserves_dictionary_parameters() {
        let one = |data_type| descriptor(vec![Field::new("ignored", data_type, true)]);

        assert_ne!(
            one(DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::Utf8)
            )),
            one(DataType::Dictionary(
                Box::new(DataType::Int16),
                Box::new(DataType::Utf8)
            ))
        );
        assert_ne!(
            one(DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::Utf8)
            )),
            one(DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::Binary)
            ))
        );
    }

    #[test]
    fn every_arrow_57_data_type_family_has_an_explicit_encoding() {
        let item = Arc::new(Field::new("item", DataType::Int64, true));
        let struct_fields = Fields::from(vec![Field::new("member", DataType::UInt8, false)]);
        let union_fields =
            UnionFields::try_new([7], [Field::new("member", DataType::UInt16, true)]).unwrap();
        let all = vec![
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Binary,
            DataType::FixedSizeBinary(4),
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::List(Arc::clone(&item)),
            DataType::ListView(Arc::clone(&item)),
            DataType::FixedSizeList(Arc::clone(&item), 3),
            DataType::LargeList(Arc::clone(&item)),
            DataType::LargeListView(Arc::clone(&item)),
            DataType::Struct(struct_fields),
            DataType::Union(union_fields, UnionMode::Dense),
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            DataType::Decimal32(9, 2),
            DataType::Decimal64(18, 2),
            DataType::Decimal128(38, 2),
            DataType::Decimal256(76, 2),
            DataType::Map(Arc::clone(&item), true),
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new("values", DataType::Utf8, true)),
            ),
        ];

        let fields = all
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| Arc::new(Field::new(index.to_string(), data_type, true)))
            .collect::<Vec<_>>();
        let encoded = SchemaDescriptorV1::from_fields(&fields);
        assert!(!encoded.as_bytes().is_empty());
        assert_ne!(encoded.sha256(), [0; 32]);
    }
}
