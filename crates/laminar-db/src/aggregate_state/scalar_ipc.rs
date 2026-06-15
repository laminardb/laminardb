//! Arrow IPC round-trip for `Vec<ScalarValue>`.
//!
//! Each tuple is a one-row `RecordBatch` encoded via the IPC stream format.
//! Old `serde_json` checkpoints are incompatible; no migration.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;

use crate::error::DbError;

/// Encode a scalar tuple as a one-row Arrow IPC stream; empty input → empty `Vec`.
pub(crate) fn scalars_to_ipc(scalars: &[ScalarValue]) -> Result<Vec<u8>, DbError> {
    if scalars.is_empty() {
        return Ok(Vec::new());
    }
    let fields: Vec<Arc<Field>> = scalars
        .iter()
        .enumerate()
        .map(|(i, sv)| Arc::new(Field::new(format!("c{i}"), sv.data_type(), true)))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let columns: Vec<ArrayRef> = scalars
        .iter()
        .map(|sv| {
            sv.to_array_of_size(1)
                .map_err(|e| DbError::Pipeline(format!("scalar to_array_of_size: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|e| DbError::Pipeline(format!("scalar batch build: {e}")))?;
    laminar_core::serialization::serialize_batch_stream(&batch)
        .map_err(|e| DbError::Pipeline(format!("scalar IPC encode: {e}")))
}

/// Decode bytes previously produced by [`scalars_to_ipc`]; empty input → empty `Vec`.
pub(crate) fn ipc_to_scalars(bytes: &[u8]) -> Result<Vec<ScalarValue>, DbError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let batch = laminar_core::serialization::deserialize_batch_stream(bytes)
        .map_err(|e| DbError::Pipeline(format!("scalar IPC decode: {e}")))?;
    (0..batch.num_columns())
        .map(|i| {
            ScalarValue::try_from_array(batch.column(i), 0)
                .map_err(|e| DbError::Pipeline(format!("scalar from array: {e}")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_roundtrips() {
        let bytes = scalars_to_ipc(&[]).unwrap();
        assert!(bytes.is_empty());
        assert!(ipc_to_scalars(&bytes).unwrap().is_empty());
    }

    #[test]
    fn mixed_scalar_roundtrip() {
        let vals = vec![
            ScalarValue::Int64(Some(42)),
            ScalarValue::Float64(Some(1.5)),
            ScalarValue::Utf8(Some("hello".to_string())),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Null,
        ];
        let bytes = scalars_to_ipc(&vals).unwrap();
        let back = ipc_to_scalars(&bytes).unwrap();
        assert_eq!(vals.len(), back.len());
        // First four compare cleanly; Null decodes as Null of matching shape.
        assert_eq!(back[0], ScalarValue::Int64(Some(42)));
        assert_eq!(back[1], ScalarValue::Float64(Some(1.5)));
        assert_eq!(back[2], ScalarValue::Utf8(Some("hello".to_string())));
        assert_eq!(back[3], ScalarValue::Boolean(Some(true)));
    }

    #[test]
    fn null_variants_roundtrip() {
        let vals = vec![
            ScalarValue::Int64(None),
            ScalarValue::Float64(None),
            ScalarValue::Utf8(None),
        ];
        let bytes = scalars_to_ipc(&vals).unwrap();
        let back = ipc_to_scalars(&bytes).unwrap();
        assert_eq!(back[0], ScalarValue::Int64(None));
        assert_eq!(back[1], ScalarValue::Float64(None));
        assert_eq!(back[2], ScalarValue::Utf8(None));
    }

    #[test]
    fn timestamp_with_tz_roundtrips() {
        let tz: Arc<str> = Arc::from("UTC");
        let v = ScalarValue::TimestampMillisecond(Some(1_700_000_000_000), Some(tz));
        let bytes = scalars_to_ipc(std::slice::from_ref(&v)).unwrap();
        let back = ipc_to_scalars(&bytes).unwrap();
        assert_eq!(back.len(), 1);
        match &back[0] {
            ScalarValue::TimestampMillisecond(Some(ts), Some(tz)) => {
                assert_eq!(*ts, 1_700_000_000_000);
                assert_eq!(tz.as_ref(), "UTC");
            }
            other => panic!("unexpected decoded scalar: {other:?}"),
        }
    }
}
