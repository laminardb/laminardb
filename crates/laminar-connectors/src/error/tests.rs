use super::*;

#[test]
fn test_connector_error_display() {
    let err = ConnectorError::ConnectionFailed("host unreachable".into());
    assert_eq!(err.to_string(), "connection failed: host unreachable");
}

#[test]
fn test_serde_error_from_json() {
    let json_err: Result<serde_json::Value, _> = serde_json::from_str("{bad json");
    let serde_err: SerdeError = json_err.unwrap_err().into();
    assert!(matches!(serde_err, SerdeError::Json(_)));
}

#[test]
fn test_serde_error_into_connector_error() {
    let serde_err = SerdeError::MissingField("timestamp".into());
    let conn_err: ConnectorError = serde_err.into();
    assert!(matches!(conn_err, ConnectorError::Serde(_)));
    assert!(conn_err.to_string().contains("timestamp"));
}

#[test]
fn test_invalid_state_error() {
    let err = ConnectorError::InvalidState {
        expected: "Running".into(),
        actual: "Closed".into(),
    };
    assert!(err.to_string().contains("Running"));
    assert!(err.to_string().contains("Closed"));
}

#[test]
fn outcome_unknown_is_recoverable_but_requires_retirement() {
    let err = ConnectorError::outcome_unknown("publish acknowledgement timed out", true);
    assert!(err.is_transient());
    assert!(err.is_outcome_unknown());
    assert_eq!(
        err.to_string(),
        "operation outcome unknown: publish acknowledgement timed out"
    );

    assert!(!ConnectorError::Timeout(10).is_outcome_unknown());
    assert!(!ConnectorError::WriteError("rejected before dispatch".into()).is_outcome_unknown());

    let terminal = ConnectorError::outcome_unknown("permanent rejection after partial work", false);
    assert!(terminal.is_outcome_unknown());
    assert!(!terminal.is_transient());
}

#[test]
fn test_schema_not_found_error() {
    let err = SerdeError::SchemaNotFound { schema_id: 42 };
    assert!(err.to_string().contains("42"));
    assert!(err.to_string().contains("schema not found"));
}

#[test]
fn test_invalid_confluent_header_error() {
    let err = SerdeError::InvalidConfluentHeader {
        expected: 0x00,
        got: 0xFF,
    };
    let msg = err.to_string();
    assert!(msg.contains("0x00"));
    assert!(msg.contains("0xff"));
}

#[test]
fn test_schema_incompatible_error() {
    let err = SerdeError::SchemaIncompatible {
        subject: "orders-value".into(),
        message: "READER_FIELD_MISSING_DEFAULT_VALUE".into(),
    };
    let msg = err.to_string();
    assert!(msg.contains("orders-value"));
    assert!(msg.contains("READER_FIELD_MISSING_DEFAULT_VALUE"));
}

#[test]
fn test_avro_decode_error() {
    let err = SerdeError::AvroDecodeError {
        column: "price".into(),
        avro_type: "double".into(),
        message: "unexpected null".into(),
    };
    let msg = err.to_string();
    assert!(msg.contains("price"));
    assert!(msg.contains("double"));
    assert!(msg.contains("unexpected null"));
}

#[test]
fn test_record_count_mismatch_error() {
    let err = SerdeError::RecordCountMismatch {
        expected: 5,
        got: 3,
    };
    let msg = err.to_string();
    assert!(msg.contains('5'));
    assert!(msg.contains('3'));
}
