use super::*;
use serde_json::json;

#[test]
fn test_encode_null() {
    let mut enc = JsonbEncoder::new();
    let bytes = enc.encode(&json!(null));
    assert_eq!(bytes, vec![tags::NULL]);
}

#[test]
fn test_encode_bool() {
    let mut enc = JsonbEncoder::new();
    assert_eq!(enc.encode(&json!(false)), vec![tags::BOOL_FALSE]);
    assert_eq!(enc.encode(&json!(true)), vec![tags::BOOL_TRUE]);
}

#[test]
fn test_encode_int64() {
    let mut enc = JsonbEncoder::new();
    let bytes = enc.encode(&json!(42));
    assert_eq!(bytes[0], tags::INT64);
    let val = i64::from_le_bytes(bytes[1..9].try_into().unwrap());
    assert_eq!(val, 42);
}

#[test]
fn test_encode_float64() {
    let mut enc = JsonbEncoder::new();
    let bytes = enc.encode(&json!(3.14));
    assert_eq!(bytes[0], tags::FLOAT64);
    let val = f64::from_le_bytes(bytes[1..9].try_into().unwrap());
    assert!((val - 3.14).abs() < f64::EPSILON);
}

#[test]
fn test_encode_string() {
    let mut enc = JsonbEncoder::new();
    let bytes = enc.encode(&json!("hello"));
    assert_eq!(bytes[0], tags::STRING);
    let len = u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as usize;
    assert_eq!(len, 5);
    assert_eq!(&bytes[5..10], b"hello");
}

#[test]
fn test_accessor_null() {
    let mut enc = JsonbEncoder::new();
    let bytes = enc.encode(&json!(null));
    assert!(JsonbAccessor::is_null(&bytes));
    assert!(JsonbAccessor::as_bool(&bytes).is_none());
}

#[test]
fn test_accessor_bool() {
    let mut enc = JsonbEncoder::new();
    assert_eq!(
        JsonbAccessor::as_bool(&enc.encode(&json!(true))),
        Some(true)
    );
    assert_eq!(
        JsonbAccessor::as_bool(&enc.encode(&json!(false))),
        Some(false)
    );
}

#[test]
fn test_accessor_i64() {
    let mut enc = JsonbEncoder::new();
    let bytes = enc.encode(&json!(-99));
    assert_eq!(JsonbAccessor::as_i64(&bytes), Some(-99));
}

#[test]
fn test_accessor_f64() {
    let mut enc = JsonbEncoder::new();
    let bytes = enc.encode(&json!(2.718));
    let val = JsonbAccessor::as_f64(&bytes).unwrap();
    assert!((val - 2.718).abs() < f64::EPSILON);
}

#[test]
fn test_accessor_str() {
    let mut enc = JsonbEncoder::new();
    let bytes = enc.encode(&json!("world"));
    assert_eq!(JsonbAccessor::as_str(&bytes), Some("world"));
}

#[test]
fn test_object_field_access() {
    let mut enc = JsonbEncoder::new();
    let bytes = enc.encode(&json!({"name": "Alice", "age": 30, "active": true}));

    // Fields are sorted: "active", "age", "name".
    let name_val = JsonbAccessor::get_field(&bytes, "name").unwrap();
    assert_eq!(JsonbAccessor::as_str(name_val), Some("Alice"));

    let age_val = JsonbAccessor::get_field(&bytes, "age").unwrap();
    assert_eq!(JsonbAccessor::as_i64(age_val), Some(30));

    let active_val = JsonbAccessor::get_field(&bytes, "active").unwrap();
    assert_eq!(JsonbAccessor::as_bool(active_val), Some(true));

    // Non-existent field.
    assert!(JsonbAccessor::get_field(&bytes, "missing").is_none());
}

#[test]
fn test_object_empty() {
    let mut enc = JsonbEncoder::new();
    let bytes = enc.encode(&json!({}));
    assert_eq!(JsonbAccessor::object_len(&bytes), Some(0));
    assert!(JsonbAccessor::get_field(&bytes, "any").is_none());
}

#[test]
fn test_array_access() {
    let mut enc = JsonbEncoder::new();
    let bytes = enc.encode(&json!([10, 20, 30]));

    assert_eq!(JsonbAccessor::array_len(&bytes), Some(3));

    let elem0 = JsonbAccessor::array_get(&bytes, 0).unwrap();
    assert_eq!(JsonbAccessor::as_i64(elem0), Some(10));

    let elem2 = JsonbAccessor::array_get(&bytes, 2).unwrap();
    assert_eq!(JsonbAccessor::as_i64(elem2), Some(30));

    assert!(JsonbAccessor::array_get(&bytes, 5).is_none());
}

#[test]
fn test_nested_object() {
    let mut enc = JsonbEncoder::new();
    let bytes = enc.encode(&json!({"outer": {"inner": 42}}));

    let outer = JsonbAccessor::get_field(&bytes, "outer").unwrap();
    let inner = JsonbAccessor::get_field(outer, "inner").unwrap();
    assert_eq!(JsonbAccessor::as_i64(inner), Some(42));
}

#[test]
fn test_nested_array_in_object() {
    let mut enc = JsonbEncoder::new();
    let bytes = enc.encode(&json!({"items": [1, 2, 3]}));

    let items = JsonbAccessor::get_field(&bytes, "items").unwrap();
    assert_eq!(JsonbAccessor::array_len(items), Some(3));
    let elem1 = JsonbAccessor::array_get(items, 1).unwrap();
    assert_eq!(JsonbAccessor::as_i64(elem1), Some(2));
}

#[test]
fn test_large_object() {
    let mut enc = JsonbEncoder::new();
    let mut obj = serde_json::Map::new();
    for i in 0..100 {
        obj.insert(format!("field_{i:03}"), json!(i));
    }
    let bytes = enc.encode(&serde_json::Value::Object(obj));

    // Binary search should find any field.
    for i in 0..100 {
        let key = format!("field_{i:03}");
        let val = JsonbAccessor::get_field(&bytes, &key).unwrap();
        assert_eq!(JsonbAccessor::as_i64(val), Some(i));
    }
    assert!(JsonbAccessor::get_field(&bytes, "nonexistent").is_none());
}

#[test]
fn test_unicode_keys() {
    let mut enc = JsonbEncoder::new();
    let bytes = enc.encode(&json!({"名前": "太郎", "年齢": 25}));

    let name = JsonbAccessor::get_field(&bytes, "名前").unwrap();
    assert_eq!(JsonbAccessor::as_str(name), Some("太郎"));

    let age = JsonbAccessor::get_field(&bytes, "年齢").unwrap();
    assert_eq!(JsonbAccessor::as_i64(age), Some(25));
}

#[test]
fn test_type_mismatch_returns_none() {
    let mut enc = JsonbEncoder::new();
    let bytes = enc.encode(&json!(42)); // INT64
    assert!(JsonbAccessor::as_str(&bytes).is_none());
    assert!(JsonbAccessor::as_bool(&bytes).is_none());
    assert!(JsonbAccessor::as_f64(&bytes).is_none());
}

#[test]
fn test_empty_slice() {
    // Empty slice is not null — null is tag 0x00.
    assert!(!JsonbAccessor::is_null(&[]));
    assert!(JsonbAccessor::as_bool(&[]).is_none());
    assert!(JsonbAccessor::as_i64(&[]).is_none());
    assert!(JsonbAccessor::as_f64(&[]).is_none());
    assert!(JsonbAccessor::as_str(&[]).is_none());
    assert!(JsonbAccessor::get_field(&[], "x").is_none());
}
