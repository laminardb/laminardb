//! Text columns in either Arrow string layout.

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, StringArray};
use arrow_schema::DataType;

use crate::error::ConnectorError;

/// The column as `Utf8`, or `None` when it is not text. A `Utf8View` column, which a SQL string
/// cast such as `CAST(x AS VARCHAR)` produces, is copied into `Utf8` so readers handle one layout.
///
/// # Errors
///
/// Fails when a view holds more bytes than `Utf8` offsets can address.
pub(crate) fn utf8_column(column: &ArrayRef) -> Result<Option<StringArray>, ConnectorError> {
    if !matches!(column.data_type(), DataType::Utf8 | DataType::Utf8View) {
        return Ok(None);
    }
    let strings = arrow_cast::cast(column, &DataType::Utf8).map_err(|error| {
        ConnectorError::SchemaMismatch(format!("copy Utf8View into Utf8: {error}"))
    })?;
    Ok(Some(strings.as_string::<i32>().clone()))
}
