use super::*;
use arrow_array::Int64Array;
use arrow_schema::{DataType, Field, Schema};

fn aligner() -> KeyAligner {
    KeyAligner::new(vec![SortField::new(DataType::Int64)], vec!["id".into()]).unwrap()
}

fn batch(ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids.to_vec()))]).unwrap()
}

fn encode(ids: &[i64]) -> Vec<Vec<u8>> {
    let conv = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
    let rows = conv
        .convert_columns(&[Arc::new(Int64Array::from(ids.to_vec()))])
        .unwrap();
    (0..ids.len())
        .map(|i| rows.row(i).as_ref().to_vec())
        .collect()
}

#[test]
fn aligns_out_of_order_with_misses_and_dups() {
    let aligner = aligner();
    // Fetched rows arrive in a different order than the keys, and key 99
    // is absent; key 2 is requested twice.
    let fetched = vec![batch(&[2, 5])];
    let keys = encode(&[5, 2, 99, 2]);
    let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();

    let out = aligner.align(&key_refs, &fetched).unwrap();
    let id = |b: &Option<RecordBatch>| {
        b.as_ref().map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        })
    };
    assert_eq!(id(&out[0]), Some(5));
    assert_eq!(id(&out[1]), Some(2));
    assert_eq!(id(&out[2]), None); // miss
    assert_eq!(id(&out[3]), Some(2)); // duplicate key resolves again
}

#[test]
fn decode_round_trips_to_pk_columns() {
    let aligner = aligner();
    let keys = encode(&[7, 8]);
    let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    let cols = aligner.decode_keys(&key_refs).unwrap();
    let ids = cols[0].as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ids.values(), &[7, 8]);
}

#[test]
fn rejects_duplicate_fetched_keys_instead_of_choosing_a_row() {
    let aligner = aligner();
    let keys = encode(&[2]);
    let error = aligner
        .align(&[keys[0].as_slice()], &[batch(&[2, 2])])
        .unwrap_err();
    assert!(error.to_string().contains("multiple rows"), "{error}");
}
