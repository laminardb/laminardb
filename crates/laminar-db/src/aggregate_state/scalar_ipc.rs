//! Arrow IPC round-trip for `Vec<ScalarValue>`.
//!
//! Each tuple is a one-row `RecordBatch` encoded via the IPC stream format.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamDecoder;
use datafusion_common::ScalarValue;

use crate::error::DbError;

/// Encode a scalar tuple as a one-row Arrow IPC stream; empty input → empty `Vec`.
#[cfg(test)]
pub(crate) fn scalars_to_ipc(scalars: &[ScalarValue]) -> Result<Vec<u8>, DbError> {
    scalars_to_ipc_bounded(scalars, usize::MAX)
}

pub(crate) fn scalars_to_ipc_bounded(
    scalars: &[ScalarValue],
    max_bytes: usize,
) -> Result<Vec<u8>, DbError> {
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
    laminar_core::serialization::serialize_batches_stream_bounded(
        batch.schema().as_ref(),
        std::iter::once(&batch),
        max_bytes,
    )
    .map_err(|e| DbError::Pipeline(format!("scalar IPC encode: {e}")))
}

/// Decode bytes previously produced by [`scalars_to_ipc`]; empty input → empty `Vec`.
pub(crate) fn ipc_to_scalars(bytes: &[u8]) -> Result<Vec<ScalarValue>, DbError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let mut decoder = StreamDecoder::new();
    let mut buffer = arrow::buffer::Buffer::from(bytes);
    let mut batch = None;
    while !buffer.is_empty() {
        let next_batch = decoder.decode(&mut buffer).map_err(|error| {
            if batch.is_some() {
                DbError::Pipeline(format!(
                    "scalar IPC contains trailing bytes after its record batch: {error}"
                ))
            } else {
                DbError::Pipeline(format!("scalar IPC decode: {error}"))
            }
        })?;
        if let Some(next_batch) = next_batch {
            if batch.replace(next_batch).is_some() {
                return Err(DbError::Pipeline(
                    "scalar IPC contains more than one record batch".into(),
                ));
            }
        }
    }
    decoder
        .finish()
        .map_err(|error| DbError::Pipeline(format!("scalar IPC decode: {error}")))?;
    let batch =
        batch.ok_or_else(|| DbError::Pipeline("scalar IPC contains no record batch".into()))?;
    if batch.num_rows() != 1 {
        return Err(DbError::Pipeline(format!(
            "scalar IPC contains {} rows; expected 1",
            batch.num_rows()
        )));
    }
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

    #[test]
    fn rejects_noncanonical_stream_shape() {
        use arrow::array::Int64Array;
        use arrow::datatypes::DataType;
        use arrow_ipc::writer::StreamWriter;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        let mut multiple = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut multiple, &batch.schema()).unwrap();
            writer.write(&batch).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        assert!(ipc_to_scalars(&multiple)
            .unwrap_err()
            .to_string()
            .contains("more than one record batch"));

        let mut trailing = scalars_to_ipc(&[ScalarValue::Int64(Some(1))]).unwrap();
        trailing.push(0);
        assert!(ipc_to_scalars(&trailing)
            .unwrap_err()
            .to_string()
            .contains("trailing bytes"));

        let oversized_header = [0xff; 8];
        assert!(ipc_to_scalars(&oversized_header).is_err());
    }
}
