use super::*;

#[test]
fn hot_path_error_is_copy_and_small() {
    let e = HotPathError::LateEvent;
    let e2 = e; // Copy
    assert_eq!(e, e2);
    assert_eq!(std::mem::size_of::<HotPathError>(), 2);
}

#[test]
fn hot_path_error_codes_are_nonzero() {
    let variants = [
        HotPathError::LateEvent,
        HotPathError::StateKeyMissing,
        HotPathError::Backpressure,
        HotPathError::SerializationOverflow,
        HotPathError::SchemaMismatch,
        HotPathError::AggregateStateCorruption,
        HotPathError::QueueFull,
        HotPathError::ChannelClosed,
    ];
    for v in &variants {
        assert!(v.code() > 0, "{v:?} has zero code");
        assert!(!v.message().is_empty(), "{v:?} has empty message");
        assert!(
            v.ldb_code().starts_with("LDB-"),
            "{v:?} has bad ldb_code: {}",
            v.ldb_code()
        );
    }
}

#[test]
fn hot_path_error_display() {
    let e = HotPathError::LateEvent;
    let s = e.to_string();
    assert!(s.starts_with("[LDB-"));
    assert!(s.contains("watermark"));
}

#[test]
fn error_codes_are_stable_strings() {
    assert_eq!(INVALID_CONFIG, "LDB-0001");
    assert_eq!(SERIALIZATION_FAILED, "LDB-4001");
    assert_eq!(MANAGED_STATE_BUDGET_EXCEEDED, "LDB-4008");
    assert_eq!(EXACTLY_ONCE_SOURCE_UNCERTIFIED, "LDB-5037");
    assert_eq!(SOURCE_PRIMARY_KEY_REQUIRED, "LDB-5038");
    assert_eq!(SOURCE_MUTATION_NOT_ADMITTED, "LDB-5039");
    assert_eq!(CHECKPOINT_FAILED, "LDB-6001");
    assert_eq!(SUBSCRIPTION_PLAN_UNSUPPORTED, "LDB-6020");
    assert_eq!(SUBSCRIPTION_PROTOCOL_UNSUPPORTED, "LDB-6037");
    assert_eq!(INTERNAL, "LDB-8001");
}
