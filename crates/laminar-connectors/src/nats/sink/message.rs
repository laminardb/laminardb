//! NATS subject, header, and encoded-message validation.
//!
//! Validation completes before a row is published so malformed batches cannot produce a partial
//! prefix. Header-size accounting uses the exact bytes emitted by async-nats.

use super::{
    err, Array, AsArray, ConnectorError, FromStr, HeaderMap, HeaderName, HeaderValue, RecordBatch,
    StringArray, SubjectSpec,
};

pub(super) fn resolve_utf8<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a StringArray, ConnectorError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| err(&format!("column '{name}' not in batch schema")))?;
    col.as_string_opt::<i32>()
        .ok_or_else(|| err(&format!("column '{name}' must be Utf8")))
}

pub(super) fn validate_non_null(
    arr: &StringArray,
    kind: &str,
    name: &str,
) -> Result<(), ConnectorError> {
    if arr.null_count() == 0 {
        return Ok(());
    }
    let row = (0..arr.len())
        .find(|&row| arr.is_null(row))
        .expect("a positive null count has a null row");
    Err(err(&format!("{kind} '{name}' is null at row {row}")))
}

pub(super) fn operation_has_prior_output(
    rows_enqueued: usize,
    pending_acks: usize,
    acknowledged_in_operation: usize,
) -> bool {
    rows_enqueued > 0 || pending_acks > 0 || acknowledged_in_operation > 0
}

pub(super) fn validate_publish_subjects(
    configured: &SubjectSpec,
    subject_column: Option<&StringArray>,
    rows: usize,
) -> Result<(), ConnectorError> {
    for row in 0..rows {
        let subject = match (configured, subject_column) {
            (SubjectSpec::Literal(subject), _) => subject.as_str(),
            (SubjectSpec::Column(_), Some(column)) => column.value(row),
            (SubjectSpec::Column(_), None) => {
                unreachable!("subject column resolved before preflight")
            }
        };
        validate_publish_subject(subject, row)?;
    }
    Ok(())
}

pub(super) fn validate_publish_subject(subject: &str, row: usize) -> Result<(), ConnectorError> {
    let bytes = subject.as_bytes();
    let invalid = bytes.is_empty()
        || bytes.first() == Some(&b'.')
        || bytes.last() == Some(&b'.')
        || bytes.windows(2).any(|pair| pair == b"..")
        || bytes
            .iter()
            .any(|byte| matches!(byte, b' ' | b'\t' | b'\r' | b'\n' | b'*' | b'>'));
    if invalid {
        return Err(err(&format!(
            "invalid NATS publish subject at row {row}: invalid subject format"
        )));
    }
    Ok(())
}

pub(super) fn header_entry_len(name: &str, value: &str) -> usize {
    name.len()
        .saturating_add(b": ".len())
        .saturating_add(value.len())
        .saturating_add(b"\r\n".len())
}

pub(super) fn header_value_is_valid(value: &str) -> bool {
    !value.contains(['\r', '\n'])
}

pub(super) fn validate_headers_and_encoded_len(
    expected_stream: Option<&str>,
    msg_id: Option<&str>,
    header_cols: &[(&HeaderName, &StringArray)],
    row: usize,
) -> Result<usize, ConnectorError> {
    let mut entries_len = 0usize;
    let mut has_headers = false;
    if let Some(stream) = expected_stream {
        if !header_value_is_valid(stream) {
            return Err(err(&format!(
                "invalid expected stream header at row {row}: value cannot contain CR or LF"
            )));
        }
        has_headers = true;
        entries_len = entries_len.saturating_add(header_entry_len("Nats-Expected-Stream", stream));
    }
    if let Some(id) = msg_id {
        if !header_value_is_valid(id) {
            return Err(err(&format!(
                "invalid message deduplication id at row {row}: value cannot contain CR or LF"
            )));
        }
        has_headers = true;
        entries_len = entries_len.saturating_add(header_entry_len("Nats-Msg-Id", id));
    }
    for (name, array) in header_cols {
        if array.is_null(row) {
            continue;
        }
        let name: &str = (*name).as_ref();
        let value = array.value(row);
        if !header_value_is_valid(value) {
            return Err(err(&format!(
                "invalid header '{name}' value at row {row}: value cannot contain CR or LF"
            )));
        }
        has_headers = true;
        entries_len = entries_len.saturating_add(header_entry_len(name, value));
    }
    if has_headers {
        Ok(b"NATS/1.0\r\n"
            .len()
            .saturating_add(entries_len)
            .saturating_add(b"\r\n".len()))
    } else {
        Ok(0)
    }
}

#[cfg(test)]
pub(super) fn encoded_header_len(headers: &HeaderMap) -> usize {
    if headers.is_empty() {
        return 0;
    }
    let mut len = b"NATS/1.0\r\n".len() + b"\r\n".len();
    for (name, values) in headers.iter() {
        let name: &str = name.as_ref();
        for value in values {
            len = len
                .saturating_add(name.len())
                .saturating_add(b": ".len())
                .saturating_add(value.as_str().len())
                .saturating_add(b"\r\n".len());
        }
    }
    len
}

pub(super) fn validate_message_size(
    row: usize,
    payload_len: usize,
    header_len: usize,
    max_payload: usize,
) -> Result<(), ConnectorError> {
    let message_len = payload_len.saturating_add(header_len);
    if message_len > max_payload {
        return Err(err(&format!(
            "NATS message at row {row} is {message_len} bytes including headers, above the current server max_payload of {max_payload} bytes"
        )));
    }
    Ok(())
}

pub(super) fn parse_header_value(
    kind: &str,
    value: &str,
    row: usize,
) -> Result<HeaderValue, ConnectorError> {
    HeaderValue::from_str(value)
        .map_err(|error| err(&format!("invalid {kind} at row {row}: {error}")))
}

pub(super) fn build_headers(
    expected_stream: Option<&str>,
    msg_id: Option<&str>,
    header_cols: &[(&HeaderName, &StringArray)],
    row: usize,
) -> Result<Option<HeaderMap>, ConnectorError> {
    if header_cols.is_empty() && expected_stream.is_none() && msg_id.is_none() {
        return Ok(None);
    }
    let mut h = HeaderMap::new();
    if let Some(s) = expected_stream {
        h.insert(
            HeaderName::from_static("Nats-Expected-Stream"),
            parse_header_value("expected stream header", s, row)?,
        );
    }
    if let Some(id) = msg_id {
        h.insert(
            HeaderName::from_static("Nats-Msg-Id"),
            parse_header_value("message deduplication id", id, row)?,
        );
    }
    for (name, arr) in header_cols {
        if !arr.is_null(row) {
            let header_name: &str = (*name).as_ref();
            let value = parse_header_value(
                &format!("header '{header_name}' value"),
                arr.value(row),
                row,
            )?;
            h.insert((*name).clone(), value);
        }
    }
    Ok(Some(h))
}
