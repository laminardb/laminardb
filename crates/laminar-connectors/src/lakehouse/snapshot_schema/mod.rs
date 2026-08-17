//! Schema boundary shared by finite lakehouse snapshots.

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema, SchemaRef};

use crate::error::ConnectorError;

/// Reorders a snapshot batch into the declared field order and enforces its schema boundary.
pub(super) fn conform_snapshot_batch(
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
pub(super) fn validate_snapshot_schema(
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

#[cfg(test)]
mod tests;
