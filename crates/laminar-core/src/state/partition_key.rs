//! Canonical typed partition-key encoding for partitioning ABI v1.
//!
//! The encoded bytes are Arrow row-format bytes produced with ascending,
//! nulls-first sort fields. They are schema-bound rather than self-describing.
//! This slice freezes the encoder and type gate only; a persisted key-schema
//! descriptor/fingerprint remains required before durable keyed-state restore.
//! Changing the admitted type set, row encoding, hash, or vnode mapping requires
//! a partitioning ABI decision.

use std::num::NonZeroU32;

use arrow_array::types::{
    validate_decimal_precision_and_scale, Decimal128Type, Decimal256Type, Decimal32Type,
    Decimal64Type, DecimalType,
};
use arrow_array::ArrayRef;
use arrow_row::{RowConverter, Rows, SortField};
use arrow_schema::{DataType, TimeUnit};

use super::vnode::key_hash;

/// Failure to construct the ABI-v1 typed-key encoder.
#[derive(Debug, thiserror::Error)]
pub enum PartitionKeyCodecError {
    /// The resolved key type has no frozen ABI-v1 equality and encoding contract.
    #[error("partition key column {index} has unsupported ABI-v1 type {data_type}")]
    UnsupportedKeyType {
        /// Zero-based position in the composite key.
        index: usize,
        /// Rejected Arrow type.
        data_type: DataType,
    },
    /// Arrow rejected the otherwise admitted row layout.
    #[error("partition key Arrow row encoding: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
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

impl PartitionKeyCodecV1 {
    /// Construct a codec for one resolved, ordered composite-key schema.
    ///
    /// # Errors
    /// Returns the first unsupported key type or an Arrow row-layout error.
    pub fn try_new(
        data_types: impl IntoIterator<Item = DataType>,
    ) -> Result<Self, PartitionKeyCodecError> {
        let data_types: Vec<DataType> = data_types.into_iter().collect();
        for (index, data_type) in data_types.iter().enumerate() {
            if !is_supported_key_type(data_type) {
                return Err(PartitionKeyCodecError::UnsupportedKeyType {
                    index,
                    data_type: data_type.clone(),
                });
            }
        }
        let fields = data_types.into_iter().map(SortField::new).collect();
        Ok(Self {
            converter: RowConverter::new(fields)?,
        })
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

fn is_supported_key_type(data_type: &DataType) -> bool {
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
        | DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => true,
        DataType::FixedSizeBinary(length) => *length >= 0,
        DataType::Time32(unit) => {
            matches!(unit, TimeUnit::Second | TimeUnit::Millisecond)
        }
        DataType::Time64(unit) => {
            matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond)
        }
        DataType::Decimal32(precision, scale) => valid_decimal::<Decimal32Type>(*precision, *scale),
        DataType::Decimal64(precision, scale) => valid_decimal::<Decimal64Type>(*precision, *scale),
        DataType::Decimal128(precision, scale) => {
            valid_decimal::<Decimal128Type>(*precision, *scale)
        }
        DataType::Decimal256(precision, scale) => {
            valid_decimal::<Decimal256Type>(*precision, *scale)
        }
        DataType::Dictionary(indices, values) => {
            matches!(
                indices.as_ref(),
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
            ) && is_supported_key_type(values)
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
        | DataType::RunEndEncoded(_, _) => false,
    }
}

fn valid_decimal<T: DecimalType>(precision: u8, scale: i8) -> bool {
    validate_decimal_precision_and_scale::<T>(precision, scale).is_ok()
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;
    use std::sync::Arc;

    use arrow::datatypes::{i256, IntervalDayTime, IntervalMonthDayNano};
    use arrow_array::types::Int8Type;
    use arrow_array::{
        ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
        Decimal128Array, Decimal256Array, Decimal32Array, Decimal64Array, DictionaryArray,
        DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
        DurationSecondArray, FixedSizeBinaryArray, Int16Array, Int32Array, Int64Array, Int8Array,
        IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray,
        LargeStringArray, NullArray, RecordBatch, StringArray, StringViewArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit, UnionFields, UnionMode};

    use super::*;
    use crate::state::PARTITIONING_ABI_VERSION;

    fn encoded_triples(batch: &RecordBatch) -> Vec<(String, u64, u32)> {
        let codec = PartitionKeyCodecV1::try_new(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.data_type().clone()),
        )
        .unwrap();
        let rows = codec.encode_columns(batch.columns()).unwrap();
        let vnode_count = NonZeroU32::new(257).unwrap();
        rows.iter()
            .map(|row| {
                let bytes = row.as_ref();
                let mut encoded = String::with_capacity(bytes.len() * 2);
                for byte in bytes {
                    write!(&mut encoded, "{byte:02x}").unwrap();
                }
                (
                    encoded,
                    PartitionKeyCodecV1::hash_encoded(bytes),
                    PartitionKeyCodecV1::vnode_for_encoded(bytes, vnode_count),
                )
            })
            .collect()
    }

    fn encoded_triples_for_columns(columns: Vec<ArrayRef>) -> Vec<(String, u64, u32)> {
        let fields = columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Field::new(format!("key_{index}"), column.data_type().clone(), true)
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        encoded_triples(&batch)
    }

    fn encoded_triples_for_array(array: ArrayRef) -> Vec<(String, u64, u32)> {
        encoded_triples_for_columns(vec![array])
    }

    type GoldenTriple = (&'static str, u64, u32);

    fn assert_array_golden(array: ArrayRef, expected: &[GoldenTriple]) {
        let expected = expected
            .iter()
            .map(|(bytes, hash, vnode)| ((*bytes).to_owned(), *hash, *vnode))
            .collect::<Vec<_>>();
        assert_eq!(encoded_triples_for_array(array), expected);
    }

    #[test]
    fn partitioning_abi_v1_typed_key_bytes_hashes_and_vnodes_are_golden() {
        assert_eq!(PARTITIONING_ABI_VERSION, 1);

        let decimal = Decimal128Array::from(vec![Some(-12_345), None, Some(0), Some(99_999)])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let timestamp =
            TimestampMillisecondArray::from(vec![Some(-1), Some(0), None, Some(1_700_000_000_123)])
                .with_timezone("UTC");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("tenant", DataType::Utf8, true),
                Field::new("enabled", DataType::Boolean, true),
                Field::new("amount", DataType::Decimal128(10, 2), true),
                Field::new(
                    "event_time",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    true,
                ),
                Field::new("sequence", DataType::Int64, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec![
                    Some(""),
                    Some("alpha"),
                    None,
                    Some("☃"),
                ])),
                Arc::new(BooleanArray::from(vec![
                    None,
                    Some(false),
                    Some(true),
                    Some(true),
                ])),
                Arc::new(decimal),
                Arc::new(timestamp),
                Arc::new(Int64Array::from(vec![
                    Some(i64::MIN),
                    Some(-1),
                    None,
                    Some(i64::MAX),
                ])),
            ],
        )
        .unwrap();

        assert_eq!(
            encoded_triples(&batch),
            vec![
                (
                    "010000017fffffffffffffffffffffffffffcfc7017fffffffffffffff010000000000000000".into(),
                    5_900_132_675_361_340_206,
                    84,
                ),
                (
                    "02616c7068610000000501000000000000000000000000000000000000018000000000000000017fffffffffffffff".into(),
                    7_450_665_380_414_867_767,
                    203,
                ),
                (
                    "0001010180000000000000000000000000000000000000000000000000000000000000000000".into(),
                    2_622_629_670_790_794_035,
                    210,
                ),
                (
                    "02e298830000000000030101018000000000000000000000000001869f018000018bcfe5687b01ffffffffffffffff".into(),
                    14_146_907_616_343_415_997,
                    114,
                ),
            ]
        );
    }

    #[test]
    fn partitioning_abi_v1_integer_widths_and_boundaries_are_golden() {
        assert_array_golden(
            Arc::new(Int8Array::from(vec![i8::MIN, -1, 0, 1, i8::MAX])),
            &[
                ("0100", 12_363_696_039_452_112_576, 54),
                ("017f", 16_243_147_722_386_869_374, 237),
                ("0180", 171_526_420_289_593_512, 129),
                ("0181", 2_547_496_065_887_143_594, 48),
                ("01ff", 646_411_226_463_558_372, 207),
            ],
        );
        assert_array_golden(
            Arc::new(Int16Array::from(vec![i16::MIN, -1, 0, 1, i16::MAX])),
            &[
                ("010000", 13_456_974_060_261_019_140, 62),
                ("017fff", 16_212_905_320_020_644_513, 18),
                ("018000", 3_232_529_264_997_278_809, 51),
                ("018001", 6_266_754_840_588_053_037, 202),
                ("01ffff", 11_692_200_362_880_776_048, 148),
            ],
        );
        assert_array_golden(
            Arc::new(Int32Array::from(vec![i32::MIN, -1, 0, 1, i32::MAX])),
            &[
                ("0100000000", 15_122_404_197_219_825_512, 191),
                ("017fffffff", 3_378_966_536_833_104_354, 255),
                ("0180000000", 3_555_926_625_577_413_108, 15),
                ("0180000001", 17_952_794_488_726_909_771, 80),
                ("01ffffffff", 15_288_252_545_451_306_623, 38),
            ],
        );
        assert_array_golden(
            Arc::new(Int64Array::from(vec![i64::MIN, -1, 0, 1, i64::MAX])),
            &[
                ("010000000000000000", 3_547_760_990_396_968_576, 111),
                ("017fffffffffffffff", 11_800_374_392_842_861_564, 202),
                ("018000000000000000", 6_834_552_262_684_119_129, 90),
                ("018000000000000001", 9_386_959_315_897_320_766, 180),
                ("01ffffffffffffffff", 6_027_492_765_397_423_756, 32),
            ],
        );
        assert_array_golden(
            Arc::new(UInt8Array::from(vec![0, 1, u8::MAX])),
            &[
                ("0100", 12_363_696_039_452_112_576, 54),
                ("0101", 508_203_992_382_727_667, 171),
                ("01ff", 646_411_226_463_558_372, 207),
            ],
        );
        assert_array_golden(
            Arc::new(UInt16Array::from(vec![0, 1, u16::MAX])),
            &[
                ("010000", 13_456_974_060_261_019_140, 62),
                ("010001", 12_599_178_845_565_073_274, 118),
                ("01ffff", 11_692_200_362_880_776_048, 148),
            ],
        );
        assert_array_golden(
            Arc::new(UInt32Array::from(vec![0, 1, u32::MAX])),
            &[
                ("0100000000", 15_122_404_197_219_825_512, 191),
                ("0100000001", 10_146_686_621_065_345_469, 90),
                ("01ffffffff", 15_288_252_545_451_306_623, 38),
            ],
        );
        assert_array_golden(
            Arc::new(UInt64Array::from(vec![0, 1, u64::MAX])),
            &[
                ("010000000000000000", 3_547_760_990_396_968_576, 111),
                ("010000000000000001", 10_971_357_638_593_500_668, 64),
                ("01ffffffffffffffff", 6_027_492_765_397_423_756, 32),
            ],
        );
    }

    #[test]
    fn partitioning_abi_v1_decimal_widths_are_golden() {
        assert_array_golden(
            Arc::new(
                Decimal32Array::from(vec![Some(-123), Some(0), Some(456), None])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
            &[
                ("017fffff85", 14_954_067_538_588_340_281, 16),
                ("0180000000", 3_555_926_625_577_413_108, 15),
                ("01800001c8", 15_848_478_651_704_748_359, 70),
                ("0000000000", 16_745_761_692_019_274_306, 30),
            ],
        );
        assert_array_golden(
            Arc::new(
                Decimal64Array::from(vec![Some(-123), Some(0), Some(456), None])
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            ),
            &[
                ("017fffffffffffff85", 4_310_524_665_588_398_091, 200),
                ("018000000000000000", 6_834_552_262_684_119_129, 90),
                ("0180000000000001c8", 14_260_596_116_349_436_848, 134),
                ("000000000000000000", 3_767_706_845_506_508_887, 114),
            ],
        );
        assert_array_golden(
            Arc::new(
                Decimal128Array::from(vec![Some(-123), Some(0), Some(456), None])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
            &[
                (
                    "017fffffffffffffffffffffffffffff85",
                    15_246_231_399_089_010_184,
                    174,
                ),
                (
                    "0180000000000000000000000000000000",
                    1_048_530_414_878_192_799,
                    131,
                ),
                (
                    "01800000000000000000000000000001c8",
                    12_521_930_288_448_736_565,
                    5,
                ),
                (
                    "0000000000000000000000000000000000",
                    14_020_088_960_994_956_481,
                    156,
                ),
            ],
        );
        assert_array_golden(
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::from_i128(-123)),
                    Some(i256::ZERO),
                    Some(i256::from_i128(456)),
                    None,
                ])
                .with_precision_and_scale(76, 2)
                .unwrap(),
            ),
            &[
                (
                    "017fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff85",
                    4_795_562_651_143_676_500,
                    90,
                ),
                (
                    "018000000000000000000000000000000000000000000000000000000000000000",
                    10_986_801_333_435_985_346,
                    239,
                ),
                (
                    "0180000000000000000000000000000000000000000000000000000000000001c8",
                    7_472_857_208_909_478_252,
                    0,
                ),
                (
                    "000000000000000000000000000000000000000000000000000000000000000000",
                    12_702_501_368_234_104_338,
                    26,
                ),
            ],
        );
    }

    #[test]
    fn partitioning_abi_v1_variable_width_representations_are_equivalent() {
        let strings = vec![
            None,
            Some(""),
            Some("alpha"),
            Some("snowman-☃"),
            Some("longer than one row block"),
        ];
        let expected_strings = [
            ("00", 14_144_645_293_874_801_883, 211),
            ("01", 16_226_181_191_752_404_715, 224),
            ("02616c70686100000005", 11_628_646_776_760_013_362, 44),
            (
                "02736e6f776d616e2dffe29883000000000003",
                1_265_025_307_806_119_948,
                94,
            ),
            (
                "026c6f6e6765722074ff68616e206f6e6520ff726f7720626c6f63ff6b0000000000000001",
                1_854_403_320_800_201_995,
                137,
            ),
        ];
        for array in [
            Arc::new(StringArray::from(strings.clone())) as ArrayRef,
            Arc::new(LargeStringArray::from(strings.clone())),
            Arc::new(StringViewArray::from(strings)),
        ] {
            assert_array_golden(array, &expected_strings);
        }

        let binary = vec![
            None,
            Some(b"" as &[u8]),
            Some(b"\x00\xff" as &[u8]),
            Some(b"long binary payload" as &[u8]),
        ];
        let expected_binary = [
            ("00", 14_144_645_293_874_801_883, 211),
            ("01", 16_226_181_191_752_404_715, 224),
            ("0200ff00000000000002", 5_453_741_840_649_971_099, 224),
            (
                "026c6f6e672062696eff617279207061796cff6f6164000000000003",
                8_331_654_890_429_173_354,
                158,
            ),
        ];
        for array in [
            Arc::new(BinaryArray::from(binary.clone())) as ArrayRef,
            Arc::new(LargeBinaryArray::from(binary.clone())),
            Arc::new(BinaryViewArray::from(binary)),
        ] {
            assert_array_golden(array, &expected_binary);
        }
    }

    #[test]
    fn partitioning_abi_v1_timestamps_null_and_boolean_are_golden() {
        let values = vec![i64::MIN, -1, 0, 1, i64::MAX];
        let expected_timestamps = [
            ("010000000000000000", 3_547_760_990_396_968_576, 111),
            ("017fffffffffffffff", 11_800_374_392_842_861_564, 202),
            ("018000000000000000", 6_834_552_262_684_119_129, 90),
            ("018000000000000001", 9_386_959_315_897_320_766, 180),
            ("01ffffffffffffffff", 6_027_492_765_397_423_756, 32),
        ];
        let timestamps: Vec<ArrayRef> = vec![
            Arc::new(TimestampSecondArray::from(values.clone())),
            Arc::new(TimestampSecondArray::from(values.clone()).with_timezone("UTC")),
            Arc::new(TimestampMillisecondArray::from(values.clone())),
            Arc::new(TimestampMillisecondArray::from(values.clone()).with_timezone("UTC")),
            Arc::new(TimestampMicrosecondArray::from(values.clone())),
            Arc::new(TimestampMicrosecondArray::from(values.clone()).with_timezone("UTC")),
            Arc::new(TimestampNanosecondArray::from(values.clone())),
            Arc::new(TimestampNanosecondArray::from(values).with_timezone("UTC")),
        ];
        for timestamp in timestamps {
            assert_array_golden(timestamp, &expected_timestamps);
        }

        assert_array_golden(
            Arc::new(BooleanArray::from(vec![None, Some(false), Some(true)])),
            &[
                ("0000", 3_685_390_413_631_870_213, 151),
                ("0100", 12_363_696_039_452_112_576, 54),
                ("0101", 508_203_992_382_727_667, 171),
            ],
        );
        assert_array_golden(
            Arc::new(NullArray::new(1)),
            &[("", 3_244_421_341_483_603_138, 39)],
        );
    }

    #[test]
    fn partitioning_abi_v1_composites_are_unambiguous_and_dictionaries_are_hydrated() {
        assert_eq!(
            encoded_triples_for_columns(vec![
                Arc::new(StringArray::from(vec![
                    Some("ab"),
                    Some("a"),
                    None,
                    Some("")
                ])),
                Arc::new(StringArray::from(vec![
                    Some("c"),
                    Some("bc"),
                    Some(""),
                    None
                ])),
            ]),
            vec![
                (
                    "0261620000000000000202630000000000000001".into(),
                    4_471_990_849_109_288_028,
                    183,
                ),
                (
                    "0261000000000000000102626300000000000002".into(),
                    11_041_438_139_486_040_547,
                    162,
                ),
                ("0001".into(), 15_448_577_912_996_533_335, 203),
                ("0100".into(), 12_363_696_039_452_112_576, 54),
            ],
        );

        let dictionary = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(2), None, Some(0), Some(2)]),
            Arc::new(StringArray::from(vec!["", "unused", "alpha"])),
        )
        .unwrap();
        let expected = [
            ("02616c70686100000005", 11_628_646_776_760_013_362, 44),
            ("00", 14_144_645_293_874_801_883, 211),
            ("01", 16_226_181_191_752_404_715, 224),
            ("02616c70686100000005", 11_628_646_776_760_013_362, 44),
        ];
        assert_array_golden(Arc::new(dictionary), &expected);
        assert_array_golden(
            Arc::new(StringArray::from(vec![
                Some("alpha"),
                None,
                Some(""),
                Some("alpha"),
            ])),
            &expected,
        );
    }

    #[test]
    fn partitioning_abi_v1_remaining_physical_families_are_golden() {
        let fixed = FixedSizeBinaryArray::from(vec![
            Some(b"ab" as &[u8]),
            None,
            Some(b"\x00\xff" as &[u8]),
        ]);
        let i32_expected = [
            ("017fffffff", 3_378_966_536_833_104_354, 255),
            ("0180000000", 3_555_926_625_577_413_108, 15),
            ("0180000001", 17_952_794_488_726_909_771, 80),
        ];
        for array in [
            Arc::new(Date32Array::from(vec![-1, 0, 1])) as ArrayRef,
            Arc::new(Time32SecondArray::from(vec![-1, 0, 1])),
            Arc::new(Time32MillisecondArray::from(vec![-1, 0, 1])),
            Arc::new(IntervalYearMonthArray::from(vec![-1, 0, 1])),
        ] {
            assert_array_golden(array, &i32_expected);
        }

        let i64_expected = [
            ("017fffffffffffffff", 11_800_374_392_842_861_564, 202),
            ("018000000000000000", 6_834_552_262_684_119_129, 90),
            ("018000000000000001", 9_386_959_315_897_320_766, 180),
        ];
        for array in [
            Arc::new(Date64Array::from(vec![-1, 0, 1])) as ArrayRef,
            Arc::new(Time64MicrosecondArray::from(vec![-1, 0, 1])),
            Arc::new(Time64NanosecondArray::from(vec![-1, 0, 1])),
            Arc::new(DurationSecondArray::from(vec![-1, 0, 1])),
            Arc::new(DurationMillisecondArray::from(vec![-1, 0, 1])),
            Arc::new(DurationMicrosecondArray::from(vec![-1, 0, 1])),
            Arc::new(DurationNanosecondArray::from(vec![-1, 0, 1])),
        ] {
            assert_array_golden(array, &i64_expected);
        }

        assert_array_golden(
            Arc::new(IntervalDayTimeArray::from(vec![
                IntervalDayTime::MINUS_ONE,
                IntervalDayTime::ZERO,
                IntervalDayTime::ONE,
            ])),
            &[
                ("017fffffff7fffffff", 7_747_151_942_003_063_448, 224),
                ("018000000080000000", 11_762_154_517_460_477_134, 151),
                ("018000000180000001", 15_404_050_100_479_657_048, 175),
            ],
        );
        assert_array_golden(
            Arc::new(IntervalMonthDayNanoArray::from(vec![
                IntervalMonthDayNano::MINUS_ONE,
                IntervalMonthDayNano::ZERO,
                IntervalMonthDayNano::ONE,
            ])),
            &[
                (
                    "017fffffff7fffffff7fffffffffffffff",
                    4_980_349_023_272_577_634,
                    71,
                ),
                (
                    "0180000000800000008000000000000000",
                    109_828_514_413_960_357,
                    55,
                ),
                (
                    "0180000001800000018000000000000001",
                    15_072_365_193_543_068_818,
                    113,
                ),
            ],
        );
        assert_array_golden(
            Arc::new(fixed),
            &[
                ("016162", 5_852_747_380_897_590_585, 183),
                ("000000", 16_959_823_422_411_450_475, 89),
                ("0100ff", 1_562_300_552_326_543_190, 71),
            ],
        );
    }

    #[test]
    fn partitioning_abi_v1_type_gate_is_explicit_and_fail_closed() {
        let item = Arc::new(Field::new("item", DataType::Int64, true));
        let rejected = [
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::List(Arc::clone(&item)),
            DataType::ListView(Arc::clone(&item)),
            DataType::LargeList(Arc::clone(&item)),
            DataType::LargeListView(Arc::clone(&item)),
            DataType::FixedSizeList(Arc::clone(&item), 2),
            DataType::Struct(vec![Field::new("item", DataType::Int64, true)].into()),
            DataType::Union(
                UnionFields::try_new([0], [Field::new("item", DataType::Int64, true)]).unwrap(),
                UnionMode::Sparse,
            ),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::clone(&item),
            ),
            DataType::Dictionary(Box::new(DataType::Float32), Box::new(DataType::Utf8)),
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Float64)),
        ];

        for data_type in rejected {
            assert!(matches!(
                PartitionKeyCodecV1::try_new([data_type.clone()]),
                Err(PartitionKeyCodecError::UnsupportedKeyType {
                    index: 0,
                    data_type: rejected,
                }) if rejected == data_type
            ));
        }
    }

    #[test]
    fn partitioning_abi_v1_validates_temporal_and_decimal_parameters() {
        let accepted = [
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::FixedSizeBinary(0),
            DataType::FixedSizeBinary(2),
            DataType::Decimal32(9, 9),
            DataType::Decimal32(9, -9),
            DataType::Decimal64(18, 18),
            DataType::Decimal64(18, -18),
            DataType::Decimal128(38, 38),
            DataType::Decimal128(38, -38),
            DataType::Decimal256(76, 76),
            DataType::Decimal256(76, -76),
        ];
        for data_type in accepted {
            PartitionKeyCodecV1::try_new([data_type]).unwrap();
        }

        let rejected = [
            DataType::Time32(TimeUnit::Microsecond),
            DataType::Time32(TimeUnit::Nanosecond),
            DataType::Time64(TimeUnit::Second),
            DataType::Time64(TimeUnit::Millisecond),
            DataType::FixedSizeBinary(-1),
            DataType::Decimal32(0, 0),
            DataType::Decimal32(10, 0),
            DataType::Decimal32(4, 5),
            DataType::Decimal64(0, 0),
            DataType::Decimal64(19, 0),
            DataType::Decimal64(9, 10),
            DataType::Decimal128(0, 0),
            DataType::Decimal128(39, 0),
            DataType::Decimal128(19, 20),
            DataType::Decimal256(0, 0),
            DataType::Decimal256(77, 0),
            DataType::Decimal256(75, 76),
        ];
        for data_type in rejected {
            assert!(matches!(
                PartitionKeyCodecV1::try_new([data_type.clone()]),
                Err(PartitionKeyCodecError::UnsupportedKeyType {
                    index: 0,
                    data_type: rejected,
                }) if rejected == data_type
            ));
        }
    }
}
