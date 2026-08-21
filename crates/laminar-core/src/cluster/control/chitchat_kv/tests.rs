use super::*;

#[test]
fn node_id_encoding_roundtrips() {
    for &v in &[1u64, 42, u64::MAX] {
        let s = encode_node_id(NodeId(v));
        assert!(s.starts_with("node-"), "got: {s}");
        let parsed: u64 = s.strip_prefix("node-").unwrap().parse().unwrap();
        assert_eq!(parsed, v);
    }
}

#[test]
fn decode_rejects_unexpected_formats() {
    let bad = chitchat::ChitchatId::new("foo".to_string(), 0, "127.0.0.1:1".parse().unwrap());
    assert_eq!(decode_chitchat_id(&bad), None);
}

#[test]
fn decode_accepts_valid_format() {
    let good = chitchat::ChitchatId::new("node-42".to_string(), 0, "127.0.0.1:1".parse().unwrap());
    assert_eq!(decode_chitchat_id(&good), Some(NodeId(42)));
}
