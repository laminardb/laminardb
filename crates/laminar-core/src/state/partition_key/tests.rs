use std::fmt::Write as _;
use std::sync::Arc;

use arrow::datatypes::{i256, IntervalDayTime, IntervalMonthDayNano};
use arrow_array::types::{Int16Type, Int8Type};
use arrow_array::{
    ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, Decimal256Array, Decimal32Array, Decimal64Array, DictionaryArray,
    DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeBinaryArray, Int16Array, Int32Array, Int64Array, Int8Array,
    IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray,
    LargeStringArray, NullArray, RecordBatch, StringArray, StringViewArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode};

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
        .map(|(index, column)| Field::new(format!("key_{index}"), column.data_type().clone(), true))
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

    let inner = DictionaryArray::<Int16Type>::try_new(
        Int16Array::from(vec![Some(0), Some(1)]),
        Arc::new(StringArray::from(vec!["alpha", ""])),
    )
    .unwrap();
    let nested = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), Some(1), None, Some(0)]),
        Arc::new(inner),
    )
    .unwrap();
    let nested_expected = [
        ("02616c70686100000005", 11_628_646_776_760_013_362, 44),
        ("01", 16_226_181_191_752_404_715, 224),
        ("00", 14_144_645_293_874_801_883, 211),
        ("02616c70686100000005", 11_628_646_776_760_013_362, 44),
    ];
    assert_array_golden(Arc::new(nested), &nested_expected);
    assert_array_golden(
        Arc::new(StringArray::from(vec![
            Some("alpha"),
            Some(""),
            None,
            Some("alpha"),
        ])),
        &nested_expected,
    );
}

#[test]
fn partitioning_abi_v1_remaining_physical_families_are_golden() {
    let fixed =
        FixedSizeBinaryArray::from(vec![Some(b"ab" as &[u8]), None, Some(b"\x00\xff" as &[u8])]);
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
    assert!(matches!(
        PartitionKeyCodecV1::try_new(Vec::new()),
        Err(PartitionKeyCodecError::EmptyKeySchema)
    ));

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
        let error = PartitionKeyCodecV1::try_new([data_type]).unwrap_err();
        assert!(matches!(
            error,
            PartitionKeyCodecError::UnsupportedKeyType { index: 0, .. }
                | PartitionKeyCodecError::UnsupportedKeyTypeFamily { index: 0, .. }
        ));
    }

    assert!(matches!(
        PartitionKeyCodecV1::try_new([DataType::Int64, DataType::Float32, DataType::Float64,]),
        Err(PartitionKeyCodecError::UnsupportedKeyType {
            index: 1,
            data_type: DataType::Float32,
        })
    ));
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

#[test]
fn partition_key_schema_v1_has_golden_bytes_and_digest() {
    let fields = vec![
        Arc::new(Field::new("tenant", DataType::Utf8, false)),
        Arc::new(Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )),
        Arc::new(Field::new("amount", DataType::Decimal128(18, 4), false)),
    ];
    let schema = PartitionKeySchemaV1::try_new(&fields).unwrap();

    let mut descriptor = String::with_capacity(schema.as_bytes().len() * 2);
    for byte in schema.as_bytes() {
        write!(&mut descriptor, "{byte:02x}").unwrap();
    }
    let mut digest = String::with_capacity(64);
    for byte in schema.sha256() {
        write!(&mut digest, "{byte:02x}").unwrap();
    }
    assert_eq!(
        descriptor,
        "4c4442504b530000000100000000000000030015010a02010000000000000003555443001a1204"
    );
    assert_eq!(
        digest,
        "25eec077e4aa1db396aea8df199851993f6ab73acac53ae2757f0902aa2f4ca4"
    );
}

#[test]
fn partition_key_schema_v1_ignores_aliases_but_preserves_order_and_nullability() {
    let describe = |fields: Vec<Field>| {
        let fields = fields.into_iter().map(Arc::new).collect::<Vec<_>>();
        PartitionKeySchemaV1::try_new(&fields).unwrap()
    };
    let base = describe(vec![
        Field::new("tenant", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]);
    let renamed = describe(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("y", DataType::Utf8, true),
    ]);
    let reordered = describe(vec![
        Field::new("region", DataType::Utf8, true),
        Field::new("tenant", DataType::Int64, false),
    ]);
    let changed_nullability = describe(vec![
        Field::new("tenant", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
    ]);

    assert_eq!(base, renamed);
    assert_ne!(base, reordered);
    assert_ne!(base, changed_nullability);
}

#[test]
fn partition_key_schema_v1_freezes_every_admitted_type_family() {
    let data_types = vec![
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
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Duration(TimeUnit::Second),
        DataType::Duration(TimeUnit::Millisecond),
        DataType::Duration(TimeUnit::Microsecond),
        DataType::Duration(TimeUnit::Nanosecond),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::Binary,
        DataType::FixedSizeBinary(4),
        DataType::LargeBinary,
        DataType::BinaryView,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Decimal32(9, 2),
        DataType::Decimal64(18, 2),
        DataType::Decimal128(38, 2),
        DataType::Decimal256(76, 2),
    ];
    let fields = data_types
        .into_iter()
        .enumerate()
        .map(|(index, data_type)| {
            Arc::new(Field::new(index.to_string(), data_type, index % 2 == 0))
        })
        .collect::<Vec<_>>();
    let schema = PartitionKeySchemaV1::try_new(&fields).unwrap();
    let mut digest = String::with_capacity(64);
    for byte in schema.sha256() {
        write!(&mut digest, "{byte:02x}").unwrap();
    }

    assert_eq!(schema.as_bytes().len(), 136);
    assert_eq!(
        digest,
        "9e674163d2cd44f2f5ccc9c4f43191ff6ed55af0ba1672157fd16b6d536b7d59"
    );
}

#[test]
fn partition_key_schema_v1_hydrates_recursive_dictionaries() {
    let hydrated = vec![Arc::new(Field::new("key", DataType::Utf8, true))];
    let int8_dictionary = vec![Arc::new(Field::new(
        "different_name",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    ))];
    let nested_int16_dictionary = vec![Arc::new(Field::new(
        "key",
        DataType::Dictionary(
            Box::new(DataType::UInt32),
            Box::new(DataType::Dictionary(
                Box::new(DataType::Int16),
                Box::new(DataType::Utf8),
            )),
        ),
        true,
    ))];

    let hydrated = PartitionKeySchemaV1::try_new(&hydrated).unwrap();
    let int8_dictionary = PartitionKeySchemaV1::try_new(&int8_dictionary).unwrap();
    let nested_int16_dictionary = PartitionKeySchemaV1::try_new(&nested_int16_dictionary).unwrap();
    assert_eq!(hydrated, int8_dictionary);
    assert_eq!(hydrated, nested_int16_dictionary);
}

#[test]
fn partition_key_schema_v1_reuses_the_codec_type_gate() {
    assert!(matches!(
        PartitionKeySchemaV1::try_new(&[]),
        Err(PartitionKeyCodecError::EmptyKeySchema)
    ));
    let fields = vec![
        Arc::new(Field::new("ok", DataType::Int64, false)),
        Arc::new(Field::new("unsupported", DataType::Float32, false)),
    ];
    assert!(matches!(
        PartitionKeySchemaV1::try_new(&fields),
        Err(PartitionKeyCodecError::UnsupportedKeyType {
            index: 1,
            data_type: DataType::Float32,
        })
    ));
}

#[test]
fn partition_key_plan_limits_fail_closed_before_large_allocation_or_recursion() {
    let too_many_types = std::iter::repeat_n(DataType::Null, MAX_PARTITION_KEY_COLUMNS + 1);
    assert!(matches!(
        PartitionKeyCodecV1::try_new(too_many_types),
        Err(PartitionKeyCodecError::TooManyKeyColumns { .. })
    ));

    let too_many_fields = (0..=MAX_PARTITION_KEY_COLUMNS)
        .map(|index| Arc::new(Field::new(index.to_string(), DataType::Null, true)))
        .collect::<Vec<_>>();
    assert!(matches!(
        PartitionKeySchemaV1::try_new(&too_many_fields),
        Err(PartitionKeyCodecError::TooManyKeyColumns { .. })
    ));

    let metadata = vec![Arc::new(
        Field::new("key", DataType::Utf8, false).with_metadata(std::collections::HashMap::from([
            ("extension".to_owned(), "v1".to_owned()),
        ])),
    )];
    assert!(matches!(
        PartitionKeySchemaV1::try_new(&metadata),
        Err(PartitionKeyCodecError::UnsupportedKeyMetadata { index: 0 })
    ));

    let long_timezone = "x".repeat(MAX_PARTITION_KEY_TIMEZONE_BYTES + 1);
    assert!(matches!(
        PartitionKeyCodecV1::try_new([DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(long_timezone.into())
        )]),
        Err(PartitionKeyCodecError::KeyTypeParameterTooLarge {
            index: 0,
            parameter: "timezone",
            ..
        })
    ));

    let mut nested = DataType::Utf8;
    for _ in 0..=MAX_PARTITION_KEY_NESTING {
        nested = DataType::Dictionary(Box::new(DataType::Int8), Box::new(nested));
    }
    assert!(matches!(
        PartitionKeyCodecV1::try_new([nested]),
        Err(PartitionKeyCodecError::KeyTypeNestingTooDeep { index: 0, .. })
    ));

    let mut rejected_list = DataType::Int64;
    for depth in 0..512 {
        rejected_list =
            DataType::List(Arc::new(Field::new(depth.to_string(), rejected_list, true)));
    }
    assert!(matches!(
        PartitionKeyCodecV1::try_new([rejected_list]),
        Err(PartitionKeyCodecError::UnsupportedKeyTypeFamily {
            index: 0,
            family: "list"
        })
    ));
}
