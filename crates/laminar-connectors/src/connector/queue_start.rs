//! The sealed start cursor of a queue source. A queue keeps its own position, so a source that
//! starts sealed reads whatever the queue holds, and its cursor records only that.

use crate::checkpoint::SourceCheckpoint;
use crate::error::ConnectorError;

const START_KEY: &str = "start";
const START_VALUE: &str = "queue";

/// The sealed start cursor of the queue source `connector`.
pub(crate) fn queue_start_cursor(connector: &str) -> SourceCheckpoint {
    let mut cursor = SourceCheckpoint::new();
    cursor.set_offset(START_KEY, START_VALUE);
    cursor.set_metadata("connector", connector);
    cursor
}

/// Refuse a cursor that is not the sealed start of the queue source `connector`, which errors
/// call `display_name`.
pub(crate) fn validate_queue_start_cursor(
    connector: &str,
    display_name: &str,
    checkpoint: &SourceCheckpoint,
) -> Result<(), ConnectorError> {
    if checkpoint.get_metadata("connector") == Some(connector)
        && checkpoint.get_offset(START_KEY) == Some(START_VALUE)
    {
        return Ok(());
    }
    Err(ConnectorError::InvalidState {
        expected: format!("the {display_name} sealed start cursor"),
        actual: format!("{:?}", checkpoint.offsets()),
    })
}
