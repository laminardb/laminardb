//! Sink columns bound by name from the query.

use arrow_schema::{DataType, Schema};

use crate::error::ConnectorError;

/// Fails unless the query has a column `name` whose type is in `allowed`. `label` names the
/// column's role in the errors, such as "RabbitMQ property column", and `expected` describes
/// `allowed`.
pub(crate) fn require_column(
    schema: &Schema,
    label: &str,
    name: &str,
    allowed: &[DataType],
    expected: &str,
) -> Result<(), ConnectorError> {
    let field = schema.field_with_name(name).map_err(|_| {
        ConnectorError::SchemaMismatch(format!("{label} '{name}' is not in the query"))
    })?;
    if !allowed.contains(field.data_type()) {
        return Err(ConnectorError::SchemaMismatch(format!(
            "{label} '{name}' must be {expected}, got {}",
            field.data_type()
        )));
    }
    Ok(())
}
