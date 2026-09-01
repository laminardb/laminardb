//! Exact retained-memory accounting for decoded `PostgreSQL` change events.

use super::{ChangeEvent, ConnectorError};

pub(super) fn retained_event_bytes(event: &ChangeEvent) -> Result<usize, ConnectorError> {
    planned_event_bytes(
        event.table.capacity(),
        event.before.as_ref().map(String::capacity),
        event.after.as_ref().map(String::capacity),
    )
}

pub(super) fn planned_event_bytes(
    table_bytes: usize,
    before_bytes: Option<usize>,
    after_bytes: Option<usize>,
) -> Result<usize, ConnectorError> {
    [
        table_bytes,
        before_bytes.unwrap_or(0),
        after_bytes.unwrap_or(0),
    ]
    .into_iter()
    .try_fold(0_usize, |total, bytes| {
        total.checked_add(bytes).ok_or_else(|| {
            ConnectorError::ReadError(
                "PostgreSQL CDC decoded-event retained-byte size overflow".into(),
            )
        })
    })
}

pub(super) fn conservative_deque_growth_bytes(
    len: usize,
    capacity: usize,
    element_size: usize,
) -> Result<usize, ConnectorError> {
    if len < capacity {
        return Ok(0);
    }
    capacity.max(4).checked_mul(element_size).ok_or_else(|| {
        ConnectorError::ReadError("PostgreSQL CDC container growth size overflow".into())
    })
}
