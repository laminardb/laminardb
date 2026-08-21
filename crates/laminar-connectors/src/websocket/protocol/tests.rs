use super::*;

// ── ClientMessage tests ──

#[test]
fn test_deserialize_subscribe_minimal() {
    let json = r#"{"action": "subscribe"}"#;
    let msg: ClientMessage = serde_json::from_str(json).unwrap();
    assert!(matches!(msg, ClientMessage::Subscribe {}));
}

#[test]
fn test_deserialize_unsubscribe() {
    let json = r#"{"action": "unsubscribe", "subscription_id": "sub-123"}"#;
    let msg: ClientMessage = serde_json::from_str(json).unwrap();
    if let ClientMessage::Unsubscribe { subscription_id } = msg {
        assert_eq!(subscription_id, "sub-123");
    } else {
        panic!("expected Unsubscribe variant");
    }
}

#[test]
fn test_removed_subscription_options_are_rejected() {
    for json in [
        r#"{"action":"subscribe","filter":"id > 1"}"#,
        r#"{"action":"subscribe","format":"Json"}"#,
        r#"{"action":"subscribe","last_sequence":42}"#,
        r#"{"action":"ping"}"#,
    ] {
        assert!(serde_json::from_str::<ClientMessage>(json).is_err());
    }
}

#[test]
fn test_deserialize_unknown_action_fails() {
    let json = r#"{"action": "unknown_action"}"#;
    let result = serde_json::from_str::<ClientMessage>(json);
    assert!(result.is_err());
}

// ── ServerMessage tests ──

#[test]
fn test_serialize_subscribed() {
    let msg = ServerMessage::Subscribed {
        subscription_id: "sub-abc".to_string(),
    };
    let json = serde_json::to_value(&msg).unwrap();
    assert_eq!(json["type"], "subscribed");
    assert_eq!(json["subscription_id"], "sub-abc");
}
