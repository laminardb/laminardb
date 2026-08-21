use std::sync::Arc;

use arrow_array::Int32Array;
use arrow_schema::{DataType, Field, Schema};

use super::*;

fn test_batch(values: &[i32]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))]).unwrap()
}

#[tokio::test]
async fn snapshot_exhaustion_and_close_are_stable() {
    let mut source = MockReferenceTableSource::new(vec![test_batch(&[1, 2]), test_batch(&[3])]);

    assert_eq!(source.poll_snapshot().await.unwrap().unwrap().num_rows(), 2);
    assert_eq!(source.poll_snapshot().await.unwrap().unwrap().num_rows(), 1);
    assert!(source.poll_snapshot().await.unwrap().is_none());
    assert!(source.poll_snapshot().await.unwrap().is_none());
    source.close().await.unwrap();
    source.close().await.unwrap();
    assert!(source.closed);
    assert!(source.poll_snapshot().await.is_err());
}
