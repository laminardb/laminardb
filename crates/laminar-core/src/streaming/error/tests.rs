use super::*;

#[test]
fn test_streaming_error_display() {
    assert_eq!(StreamingError::ChannelFull.to_string(), "channel is full");
    assert_eq!(
        StreamingError::ChannelClosed.to_string(),
        "channel is closed"
    );
    assert_eq!(
        StreamingError::Disconnected.to_string(),
        "channel is disconnected"
    );
    assert_eq!(
        StreamingError::InvalidConfig("bad".to_string()).to_string(),
        "invalid configuration: bad"
    );
    assert_eq!(StreamingError::Timeout.to_string(), "operation timed out");
}

#[test]
fn test_try_push_error() {
    let err = TryPushError::full(42);
    assert_eq!(err.into_inner(), 42);
}

#[test]
fn test_recv_error_display() {
    assert_eq!(RecvError::Disconnected.to_string(), "channel disconnected");
    assert_eq!(RecvError::Timeout.to_string(), "recv timed out");
}

#[test]
fn test_schema_mismatch_display() {
    let err = StreamingError::SchemaMismatch {
        expected: vec!["a".to_string(), "b".to_string()],
        actual: vec!["x".to_string(), "y".to_string()],
    };
    assert!(err.to_string().contains("schema mismatch"));
}
