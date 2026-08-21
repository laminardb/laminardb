use super::*;
use arrow_array::{Int64Array, StringArray};
use arrow_row::RowConverter;
use arrow_schema::{DataType, Field, Schema};
use std::collections::HashMap;
use tempfile::TempDir;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn test_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap()
}

fn int_keys(ids: &[i64]) -> Vec<Vec<u8>> {
    let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
    let rows = converter
        .convert_columns(&[Arc::new(Int64Array::from(ids.to_vec()))])
        .unwrap();
    (0..ids.len())
        .map(|i| rows.row(i).as_ref().to_vec())
        .collect()
}

async fn create_delta_table(path: &str, batches: Vec<RecordBatch>) {
    use crate::lakehouse::delta_io;
    use deltalake::protocol::SaveMode;

    let table = delta_io::open_or_create_table(path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    delta_io::write_batches(table, batches, SaveMode::Append, None, false, None, None)
        .await
        .unwrap();
}

async fn open_source(path: &str, table_name: &str) -> DeltaLookupSource {
    DeltaLookupSource::open(DeltaLookupSourceConfig {
        table_path: path.to_string(),
        storage_options: HashMap::new(),
        primary_key_columns: vec!["id".into()],
        table_name: table_name.to_string(),
    })
    .await
    .unwrap()
}

fn id_at(batch: &RecordBatch) -> i64 {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn batched_lookup_aligns_hits_and_misses() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_str().unwrap();
    create_delta_table(path, vec![test_batch(&[1, 2, 3], &["A", "B", "C"])]).await;
    let source = open_source(path, "lk").await;

    // Out-of-table-order with a miss; one batched fetch.
    let keys = int_keys(&[3, 1, 999, 2]);
    let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    let results = source.query(&key_refs, &[], &[]).await.unwrap();

    assert_eq!(results.len(), 4);
    assert_eq!(id_at(results[0].as_ref().unwrap()), 3);
    assert_eq!(id_at(results[1].as_ref().unwrap()), 1);
    assert!(results[2].is_none());
    assert_eq!(id_at(results[3].as_ref().unwrap()), 2);
}

/// The Phase 3 exit criterion: a batched `pk IN (...)` must prune
/// non-matching partition files (per-key cost O(matching files), not
/// O(table)) on a table clustered on the key.
#[tokio::test]
async fn in_list_prunes_partition_files() {
    use datafusion::physical_plan::collect;

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_str().unwrap();

    // 8 distinct keys → 8 partition directories (one Parquet file each).
    {
        use crate::lakehouse::delta_io;
        use deltalake::protocol::SaveMode;
        let t = delta_io::open_or_create_table(path, HashMap::new(), None)
            .await
            .unwrap();
        delta_io::write_batches(
            t,
            vec![test_batch(
                &[0, 1, 2, 3, 4, 5, 6, 7],
                &["a", "b", "c", "d", "e", "f", "g", "h"],
            )],
            SaveMode::Append,
            Some(&["id".to_string()]),
            false,
            None,
            None,
        )
        .await
        .unwrap();
    }

    // Correctness across partition files via the source.
    let source = open_source(path, "lk").await;
    let keys = int_keys(&[5, 2, 100]);
    let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    let results = source.query(&key_refs, &[], &[]).await.unwrap();
    assert!(results[0].is_some() && results[1].is_some() && results[2].is_none());

    // Pruning: the IN-list query reads fewer than all 8 files. (`next`
    // provider reports files read via `count_files_scanned`.)
    let ctx = SessionContext::new();
    crate::lakehouse::delta_table_provider::register_delta_table(&ctx, "lk", path, HashMap::new())
        .await
        .unwrap();
    let plan = ctx
        .sql("SELECT * FROM \"lk\" WHERE \"id\" IN (2, 5)")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let _ = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    let scanned = sum_plan_metric(&plan, "count_files_scanned");
    assert!(
        scanned > 0 && scanned < 8,
        "expected pruning, scanned={scanned}"
    );
}

fn sum_plan_metric(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>, name: &str) -> usize {
    let mut total = plan
        .metrics()
        .and_then(|m| m.sum_by_name(name))
        .map_or(0, |v| v.as_usize());
    for child in plan.children() {
        total += sum_plan_metric(child, name);
    }
    total
}
