use super::*;
use arrow_array::{Float64Array, Int64Array, StringArray};

fn keys(cols: &[&str]) -> Vec<String> {
    cols.iter().map(|s| (*s).to_string()).collect()
}

/// Build a Z-set changelog batch: schema [region: Utf8, total: Int64, __weight: Int64].
fn zset_batch(rows: &[(&str, i64, i64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("total", DataType::Int64, false),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// Build a CDC changelog batch: schema [id: Int64, value: Float64, _op: Utf8].
fn cdc_batch(rows: &[(i64, f64, &str)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("_op", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn col_str(batch: &RecordBatch, name: &str) -> Vec<String> {
    let idx = batch.schema().index_of(name).unwrap();
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..arr.len()).map(|i| arr.value(i).to_string()).collect()
}

fn col_i64(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let idx = batch.schema().index_of(name).unwrap();
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..arr.len()).map(|i| arr.value(i)).collect()
}

/// Sort output rows by (region/id key, op) so assertions are order-stable.
fn sorted_pairs(regions: &[String], values: &[i64], ops: &[String]) -> Vec<(String, i64, String)> {
    let mut v: Vec<_> = regions
        .iter()
        .zip(values)
        .zip(ops)
        .map(|((r, t), o)| (r.clone(), *t, o.clone()))
        .collect();
    v.sort();
    v
}

#[test]
fn zset_multi_update_per_key_keeps_final_value() {
    // Two emit cycles concatenated: 10→20→35. The intermediate 20 cancels.
    let out = collapse_changelog(
        &zset_batch(&[
            ("east", 10, -1),
            ("east", 20, 1),
            ("east", 20, -1),
            ("east", 35, 1),
        ]),
        &keys(&["region"]),
    )
    .unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(col_str(&out, "_op"), vec!["U"]);
    assert_eq!(col_i64(&out, "total"), vec![35]);
}

#[test]
fn zset_insert_then_delete_within_epoch_is_noop() {
    // +1 then -1 on the same full row nets zero → dropped entirely.
    let out = collapse_changelog(
        &zset_batch(&[("east", 10, 1), ("east", 10, -1)]),
        &keys(&["region"]),
    )
    .unwrap();
    assert_eq!(out.num_rows(), 0);
    assert!(out.schema().index_of(WEIGHT_COLUMN).is_err());
    assert!(out.schema().index_of("_op").is_ok());
}

#[test]
fn zset_multiple_keys_mixed_ops() {
    // east updated, west dropped, north newly inserted — in one epoch.
    let out = collapse_changelog(
        &zset_batch(&[
            ("east", 10, -1),
            ("east", 30, 1),
            ("west", 5, -1),
            ("north", 99, 1),
        ]),
        &keys(&["region"]),
    )
    .unwrap();
    assert_eq!(out.num_rows(), 3);
    let got = sorted_pairs(
        &col_str(&out, "region"),
        &col_i64(&out, "total"),
        &col_str(&out, "_op"),
    );
    assert_eq!(
        got,
        vec![
            ("east".into(), 30, "U".into()),
            ("north".into(), 99, "U".into()),
            ("west".into(), 5, "D".into()),
        ]
    );
}

#[test]
fn zset_higher_multiplicity_is_single_live_row() {
    // Cascaded aggregation can emit weight > 1; still one live row per key.
    let out = collapse_changelog(&zset_batch(&[("east", 10, 3)]), &keys(&["region"])).unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(col_str(&out, "_op"), vec!["U"]);
}

#[test]
fn zset_non_unique_merge_key_errors() {
    // Two distinct live rows for the same key → misdeclared merge key.
    let err = collapse_changelog(
        &zset_batch(&[("east", 10, 1), ("east", 20, 1)]),
        &keys(&["region"]),
    )
    .unwrap_err();
    assert!(
        matches!(err, ConnectorError::ConfigurationError(_)),
        "expected ConfigurationError, got {err:?}"
    );
    assert!(format!("{err}").contains("not unique"));
}

#[test]
fn zset_composite_merge_key() {
    // Merge key over both columns: distinct (region,total) live rows are
    // distinct keys, not a uniqueness violation.
    let out = collapse_changelog(
        &zset_batch(&[("east", 10, 1), ("east", 20, 1)]),
        &keys(&["region", "total"]),
    )
    .unwrap();
    assert_eq!(out.num_rows(), 2);
    assert_eq!(col_str(&out, "_op"), vec!["U", "U"]);
}

#[test]
fn cdc_dedup_keeps_last_arrival() {
    // id=1 inserted then updated within an epoch → one U with the last value.
    let out = collapse_changelog(
        &cdc_batch(&[(1, 10.0, "I"), (1, 15.0, "U"), (2, 20.0, "I")]),
        &keys(&["id"]),
    )
    .unwrap();
    assert_eq!(out.num_rows(), 2);
    let idx = out.schema().index_of("id").unwrap();
    let ids = out
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        (0..ids.len()).map(|i| ids.value(i)).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(col_str(&out, "_op"), vec!["U", "U"]);
    let vidx = out.schema().index_of("value").unwrap();
    let vals = out
        .column(vidx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(
        (vals.value(0) - 15.0).abs() < f64::EPSILON,
        "last value wins"
    );
}

#[test]
fn cdc_delete_is_preserved() {
    let out = collapse_changelog(
        &cdc_batch(&[(1, 10.0, "I"), (1, 10.0, "D")]),
        &keys(&["id"]),
    )
    .unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(col_str(&out, "_op"), vec!["D"]);
}

#[test]
fn cdc_update_before_normalizes_to_delete_and_after_to_upsert() {
    // U- alone → D; the U+ after-image → U.
    let out_before = collapse_changelog(&cdc_batch(&[(1, 10.0, "U-")]), &keys(&["id"])).unwrap();
    assert_eq!(col_str(&out_before, "_op"), vec!["D"]);
    let out_after = collapse_changelog(&cdc_batch(&[(1, 10.0, "U+")]), &keys(&["id"])).unwrap();
    assert_eq!(col_str(&out_after, "_op"), vec!["U"]);
}

#[test]
fn cdc_no_op_column_treated_as_upsert() {
    // Plain MV (no _op, no __weight) → all upserts, deduped by key.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
        ],
    )
    .unwrap();
    let out = collapse_changelog(&batch, &keys(&["id"])).unwrap();
    assert_eq!(out.num_rows(), 2);
    assert!(out.schema().index_of("_op").is_ok());
    assert_eq!(col_str(&out, "_op"), vec!["U", "U"]);
}

#[test]
fn cdc_strips_ts_ms_and_emits_single_op() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("_ts_ms", DataType::Int64, false),
        Field::new("_op", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(StringArray::from(vec!["I", "U"])),
        ],
    )
    .unwrap();
    let out = collapse_changelog(&batch, &keys(&["id"])).unwrap();
    assert!(out.schema().index_of("_ts_ms").is_err(), "_ts_ms stripped");
    assert_eq!(
        out.schema()
            .fields()
            .iter()
            .filter(|f| f.name() == "_op")
            .count(),
        1,
        "exactly one _op column"
    );
}

#[test]
fn empty_batch_yields_empty_with_op_column() {
    let out = collapse_changelog(&zset_batch(&[]), &keys(&["region"])).unwrap();
    assert_eq!(out.num_rows(), 0);
    assert!(out.schema().index_of(WEIGHT_COLUMN).is_err());
    assert!(out.schema().index_of("_op").is_ok());
}

#[test]
fn empty_merge_key_errors() {
    let err = collapse_changelog(&zset_batch(&[("east", 10, 1)]), &[]).unwrap_err();
    assert!(matches!(err, ConnectorError::ConfigurationError(_)));
}

#[test]
fn missing_merge_key_column_errors() {
    let err = collapse_changelog(&zset_batch(&[("east", 10, 1)]), &keys(&["nope"])).unwrap_err();
    assert!(matches!(err, ConnectorError::ConfigurationError(_)));
    assert!(format!("{err}").contains("not present"));
}
