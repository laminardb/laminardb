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
        .map(|(index, column)| Field::new(format!("c{index}"), column.data_type().clone(), true))
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
    let (metadata, metadata_start) = message_metadata(bytes, arrow_ipc::MessageHeader::RecordBatch);
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
    let (metadata, metadata_start) = message_metadata(bytes, arrow_ipc::MessageHeader::RecordBatch);
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
    let empty_preflight =
        preflight_scalar_ipc_restore(&empty_dictionary, 1, 1, 1, format_args!("empty dictionary"))
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
    preflight_scalar_ipc_restore(&zero_width, 1, 1, 1, format_args!("zero-width fixed")).unwrap();
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
