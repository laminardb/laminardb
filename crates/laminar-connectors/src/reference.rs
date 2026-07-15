//! Startup snapshot sources for reference tables.

#[cfg(any(test, feature = "testing"))]
use std::collections::VecDeque;

use arrow_array::RecordBatch;
#[cfg(any(test, feature = "delta-lake", feature = "iceberg"))]
use arrow_schema::{DataType, Schema, SchemaRef};

use crate::error::ConnectorError;

/// A finite source used to hydrate a reference table before processing starts.
#[async_trait::async_trait]
pub trait ReferenceTableSource: Send {
    /// Returns the next snapshot batch, or `None` after the complete snapshot was delivered.
    async fn poll_snapshot(&mut self) -> Result<Option<RecordBatch>, ConnectorError>;

    /// Releases source resources.
    async fn close(&mut self) -> Result<(), ConnectorError>;
}

/// Reorders a snapshot batch into the declared field order and enforces its schema boundary.
#[cfg(any(test, feature = "delta-lake", feature = "iceberg"))]
pub(crate) fn conform_snapshot_batch(
    batch: &RecordBatch,
    declared_schema: &SchemaRef,
) -> Result<RecordBatch, ConnectorError> {
    validate_snapshot_schema(batch.schema().as_ref(), declared_schema.as_ref())?;

    let mut columns = Vec::with_capacity(declared_schema.fields().len());
    for field in declared_schema.fields() {
        let index = batch.schema().index_of(field.name()).map_err(|_| {
            ConnectorError::ReadError(format!(
                "reference snapshot is missing declared column '{}'",
                field.name()
            ))
        })?;
        let source_column = batch.column(index);
        let column = if source_column.data_type() == field.data_type() {
            source_column.clone()
        } else {
            arrow_cast::cast(source_column, field.data_type()).map_err(|error| {
                ConnectorError::ReadError(format!(
                    "reference snapshot column '{}' cannot be normalized from {} to {}: {error}",
                    field.name(),
                    source_column.data_type(),
                    field.data_type()
                ))
            })?
        };
        if !field.is_nullable() && column.null_count() != 0 {
            return Err(ConnectorError::ReadError(format!(
                "reference snapshot column '{}' contains {} null values but is declared NOT NULL",
                field.name(),
                column.null_count()
            )));
        }
        columns.push(column);
    }

    RecordBatch::try_new(declared_schema.clone(), columns).map_err(|error| {
        ConnectorError::ReadError(format!(
            "reference snapshot does not satisfy the declared schema: {error}"
        ))
    })
}

/// Validates names and Arrow types independently of source field ordering and nullability metadata.
#[cfg(any(test, feature = "delta-lake", feature = "iceberg"))]
pub(crate) fn validate_snapshot_schema(
    source_schema: &Schema,
    declared_schema: &Schema,
) -> Result<(), ConnectorError> {
    if source_schema.fields().len() != declared_schema.fields().len() {
        return Err(ConnectorError::ReadError(format!(
            "reference snapshot has {} columns but {} were declared",
            source_schema.fields().len(),
            declared_schema.fields().len()
        )));
    }

    for declared in declared_schema.fields() {
        let index = source_schema.index_of(declared.name()).map_err(|_| {
            ConnectorError::ReadError(format!(
                "reference snapshot is missing declared column '{}'",
                declared.name()
            ))
        })?;
        let source = source_schema.field(index);
        if !snapshot_types_compatible(source.data_type(), declared.data_type()) {
            return Err(ConnectorError::ReadError(format!(
                "reference snapshot column '{}' has type {} but {} was declared",
                declared.name(),
                source.data_type(),
                declared.data_type()
            )));
        }
    }
    Ok(())
}

#[cfg(any(test, feature = "delta-lake", feature = "iceberg"))]
fn snapshot_types_compatible(source: &DataType, declared: &DataType) -> bool {
    source == declared
        || matches!(
            (source, declared),
            (DataType::Utf8View, DataType::Utf8 | DataType::LargeUtf8)
                | (
                    DataType::BinaryView,
                    DataType::Binary | DataType::LargeBinary
                )
        )
}

/// In-memory finite snapshot source for tests.
#[cfg(any(test, feature = "testing"))]
pub struct MockReferenceTableSource {
    snapshot_batches: VecDeque<RecordBatch>,
    /// Whether [`ReferenceTableSource::close`] has been called.
    pub closed: bool,
}

#[cfg(any(test, feature = "testing"))]
impl MockReferenceTableSource {
    /// Creates a source that drains the supplied snapshot batches in order.
    #[must_use]
    pub fn new(snapshot_batches: Vec<RecordBatch>) -> Self {
        Self {
            snapshot_batches: VecDeque::from(snapshot_batches),
            closed: false,
        }
    }

    /// Creates a source with an empty snapshot.
    #[must_use]
    pub fn empty() -> Self {
        Self::new(Vec::new())
    }
}

#[cfg(any(test, feature = "testing"))]
#[async_trait::async_trait]
impl ReferenceTableSource for MockReferenceTableSource {
    async fn poll_snapshot(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.closed {
            return Err(ConnectorError::InvalidState {
                expected: "open reference snapshot source".into(),
                actual: "closed".into(),
            });
        }
        Ok(self.snapshot_batches.pop_front())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray};
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

    #[test]
    fn declared_non_null_primary_key_is_preserved_and_enforced() {
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int32, true),
        ]));
        let declared_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            source_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
            ],
        )
        .unwrap();

        let conformed = conform_snapshot_batch(&batch, &declared_schema).unwrap();
        assert_eq!(conformed.schema(), declared_schema);
        assert_eq!(conformed.schema().field(0).name(), "id");
        assert!(!conformed.schema().field(0).is_nullable());

        let null_key_batch = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
                Arc::new(Int32Array::from(vec![Some(1), None])),
            ],
        )
        .unwrap();
        assert!(conform_snapshot_batch(&null_key_batch, &declared_schema).is_err());
    }

    #[test]
    fn incompatible_snapshot_type_is_rejected() {
        let source_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let declared_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(StringArray::from(vec!["not-an-integer"]))],
        )
        .unwrap();

        assert!(conform_snapshot_batch(&batch, &declared_schema).is_err());
    }

    #[test]
    fn string_view_is_normalized_to_declared_utf8() {
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::Utf8View,
            false,
        )]));
        let declared_schema =
            Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(arrow_array::StringViewArray::from(vec!["one"]))],
        )
        .unwrap();

        let conformed = conform_snapshot_batch(&batch, &declared_schema).unwrap();
        assert_eq!(conformed.schema(), declared_schema);
        assert_eq!(conformed.column(0).data_type(), &DataType::Utf8);
    }
}
