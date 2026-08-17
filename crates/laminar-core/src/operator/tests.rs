use super::*;
use arrow_array::{Int64Array, RecordBatch};
use std::sync::Arc;

#[test]
fn test_event_creation() {
    let array = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_from_iter(vec![("col1", array as _)]).unwrap();

    let event = Event::new(12345, batch);

    assert_eq!(event.timestamp, 12345);
    assert_eq!(event.data.num_rows(), 3);
}
