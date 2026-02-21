//! JSONB type system for JSON UDF evaluation.
//!
//! Minimal JSONB binary access types used by the JSON scalar UDFs.
//! These mirror `laminar-connectors` JSONB tags but live in
//! `laminar-sql` to keep the SQL layer self-contained.

/// JSONB binary format type tags.
///
/// Must be kept in sync with `laminar_connectors::schema::json::jsonb::tags`.
pub mod tags {
    /// Null value.
    pub const NULL: u8 = 0x00;
    /// Boolean false.
    pub const BOOL_FALSE: u8 = 0x01;
    /// Boolean true.
    pub const BOOL_TRUE: u8 = 0x02;
    /// Int64 (8 bytes little-endian).
    pub const INT64: u8 = 0x03;
    /// Float64 (8 bytes IEEE 754 little-endian).
    pub const FLOAT64: u8 = 0x04;
    /// String (4-byte LE length + UTF-8 bytes).
    pub const STRING: u8 = 0x05;
    /// Array (4-byte count + offset table + elements).
    pub const ARRAY: u8 = 0x06;
    /// Object (4-byte count + offset table + key/value data).
    pub const OBJECT: u8 = 0x07;
}

/// Returns the PostgreSQL-compatible type name for the outermost JSONB value.
///
/// Reads only the first byte (type tag) — O(1).
#[must_use]
pub fn jsonb_type_name(jsonb: &[u8]) -> Option<&'static str> {
    Some(match *jsonb.first()? {
        tags::NULL => "null",
        tags::BOOL_FALSE | tags::BOOL_TRUE => "boolean",
        tags::INT64 | tags::FLOAT64 => "number",
        tags::STRING => "string",
        tags::ARRAY => "array",
        tags::OBJECT => "object",
        _ => return None,
    })
}

/// Access a field by name in a JSONB object.
///
/// Returns a byte slice pointing to the field's JSONB value,
/// or `None` if the field does not exist or the value is not an object.
///
/// O(log n) binary search on sorted keys.
#[must_use]
pub fn jsonb_get_field<'a>(jsonb: &'a [u8], field_name: &str) -> Option<&'a [u8]> {
    if jsonb.first()? != &tags::OBJECT {
        return None;
    }

    let field_count = read_u32(jsonb, 1)? as usize;
    if field_count == 0 {
        return None;
    }

    let offset_table_start = 5;
    let data_start = offset_table_start + field_count * 8;

    let mut lo = 0usize;
    let mut hi = field_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let entry = offset_table_start + mid * 8;
        let key_off = read_u32(jsonb, entry)? as usize;

        let key_abs = data_start + key_off;
        let key_len = read_u16(jsonb, key_abs)? as usize;
        let key_bytes = jsonb.get(key_abs + 2..key_abs + 2 + key_len)?;
        let key_str = std::str::from_utf8(key_bytes).ok()?;

        match key_str.cmp(field_name) {
            std::cmp::Ordering::Equal => {
                let val_off = read_u32(jsonb, entry + 4)? as usize;
                return jsonb.get(data_start + val_off..);
            }
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
        }
    }
    None
}

/// Get a JSONB array element by index.
///
/// Returns a byte slice pointing to the element's JSONB value,
/// or `None` if the index is out of bounds or the value is not an array.
#[must_use]
pub fn jsonb_array_get(jsonb: &[u8], index: usize) -> Option<&[u8]> {
    if jsonb.first()? != &tags::ARRAY {
        return None;
    }
    let count = read_u32(jsonb, 1)? as usize;
    if index >= count {
        return None;
    }
    let offset_table_start = 5;
    let data_start = offset_table_start + count * 4;
    let entry_pos = offset_table_start + index * 4;
    let elem_off = read_u32(jsonb, entry_pos)? as usize;
    jsonb.get(data_start + elem_off..)
}

/// Check if a JSONB object contains a given key.
#[must_use]
pub fn jsonb_has_key(jsonb: &[u8], key: &str) -> bool {
    jsonb_get_field(jsonb, key).is_some()
}

/// Convert a JSONB value slice to its text representation.
///
/// For strings, returns the unquoted string value.
/// For other types, returns the JSON representation.
#[must_use]
pub fn jsonb_to_text(jsonb: &[u8]) -> Option<String> {
    let tag = *jsonb.first()?;
    match tag {
        tags::BOOL_FALSE => Some("false".to_owned()),
        tags::BOOL_TRUE => Some("true".to_owned()),
        tags::INT64 => {
            let v = i64::from_le_bytes(jsonb.get(1..9)?.try_into().ok()?);
            Some(v.to_string())
        }
        tags::FLOAT64 => {
            let v = f64::from_le_bytes(jsonb.get(1..9)?.try_into().ok()?);
            Some(v.to_string())
        }
        tags::STRING => {
            let len = read_u32(jsonb, 1)? as usize;
            Some(std::str::from_utf8(jsonb.get(5..5 + len)?).ok()?.to_owned())
        }
        tags::ARRAY | tags::OBJECT => jsonb_to_json_string(jsonb),
        // NULL and unknown tags return None (PostgreSQL returns NULL for null)
        _ => None,
    }
}

/// Convert a JSONB value to a JSON string representation.
fn jsonb_to_json_string(jsonb: &[u8]) -> Option<String> {
    let tag = *jsonb.first()?;
    Some(match tag {
        tags::NULL => "null".to_owned(),
        tags::BOOL_FALSE => "false".to_owned(),
        tags::BOOL_TRUE => "true".to_owned(),
        tags::INT64 => {
            let v = i64::from_le_bytes(jsonb.get(1..9)?.try_into().ok()?);
            v.to_string()
        }
        tags::FLOAT64 => {
            let v = f64::from_le_bytes(jsonb.get(1..9)?.try_into().ok()?);
            v.to_string()
        }
        tags::STRING => {
            let len = read_u32(jsonb, 1)? as usize;
            let s = std::str::from_utf8(jsonb.get(5..5 + len)?).ok()?;
            format!("\"{s}\"")
        }
        tags::ARRAY => {
            let count = read_u32(jsonb, 1)? as usize;
            let mut parts = Vec::with_capacity(count);
            for i in 0..count {
                let elem = jsonb_array_get(jsonb, i)?;
                parts.push(jsonb_to_json_string(elem)?);
            }
            format!("[{}]", parts.join(","))
        }
        tags::OBJECT => {
            let count = read_u32(jsonb, 1)? as usize;
            let offset_table_start = 5;
            let data_start = offset_table_start + count * 8;
            let mut parts = Vec::with_capacity(count);
            for i in 0..count {
                let entry = offset_table_start + i * 8;
                let key_off = read_u32(jsonb, entry)? as usize;
                let key_abs = data_start + key_off;
                let key_len = read_u16(jsonb, key_abs)? as usize;
                let key =
                    std::str::from_utf8(jsonb.get(key_abs + 2..key_abs + 2 + key_len)?).ok()?;
                let val_off = read_u32(jsonb, entry + 4)? as usize;
                let val_slice = jsonb.get(data_start + val_off..)?;
                parts.push(format!("\"{}\":{}", key, jsonb_to_json_string(val_slice)?));
            }
            format!("{{{}}}", parts.join(","))
        }
        _ => return None,
    })
}

/// Convert a JSONB binary value to a `serde_json::Value`.
///
/// Recursively decodes the JSONB binary format into the equivalent
/// `serde_json::Value`, avoiding the text round-trip through JSON strings.
///
/// Returns `None` if the JSONB bytes are malformed.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn jsonb_to_value(jsonb: &[u8]) -> Option<serde_json::Value> {
    let tag = *jsonb.first()?;
    Some(match tag {
        tags::NULL => serde_json::Value::Null,
        tags::BOOL_FALSE => serde_json::Value::Bool(false),
        tags::BOOL_TRUE => serde_json::Value::Bool(true),
        tags::INT64 => {
            let v = i64::from_le_bytes(jsonb.get(1..9)?.try_into().ok()?);
            serde_json::Value::Number(v.into())
        }
        tags::FLOAT64 => {
            let v = f64::from_le_bytes(jsonb.get(1..9)?.try_into().ok()?);
            serde_json::Value::Number(serde_json::Number::from_f64(v)?)
        }
        tags::STRING => {
            let len = read_u32(jsonb, 1)? as usize;
            let s = std::str::from_utf8(jsonb.get(5..5 + len)?).ok()?;
            serde_json::Value::String(s.to_owned())
        }
        tags::ARRAY => {
            let count = read_u32(jsonb, 1)? as usize;
            let mut arr = Vec::with_capacity(count);
            for i in 0..count {
                let elem = jsonb_array_get(jsonb, i)?;
                arr.push(jsonb_to_value(elem)?);
            }
            serde_json::Value::Array(arr)
        }
        tags::OBJECT => {
            let count = read_u32(jsonb, 1)? as usize;
            let offset_table_start = 5;
            let data_start = offset_table_start + count * 8;
            let mut map = serde_json::Map::with_capacity(count);
            for i in 0..count {
                let entry = offset_table_start + i * 8;
                let key_off = read_u32(jsonb, entry)? as usize;
                let key_abs = data_start + key_off;
                let key_len = read_u16(jsonb, key_abs)? as usize;
                let key =
                    std::str::from_utf8(jsonb.get(key_abs + 2..key_abs + 2 + key_len)?).ok()?;
                let val_off = read_u32(jsonb, entry + 4)? as usize;
                let val_slice = jsonb.get(data_start + val_off..)?;
                map.insert(key.to_owned(), jsonb_to_value(val_slice)?);
            }
            serde_json::Value::Object(map)
        }
        _ => return None,
    })
}

/// Check whether JSONB `left` contains `right` (PostgreSQL `@>` semantics).
///
/// An object contains another if every key in `right` exists in `left`
/// with a matching value. An array contains another if it's a superset.
/// Scalars match by equality.
#[must_use]
pub fn jsonb_contains(left: &[u8], right: &[u8]) -> Option<bool> {
    let lt = *left.first()?;
    let rt = *right.first()?;

    if lt != rt {
        return Some(false);
    }

    Some(match lt {
        tags::NULL | tags::BOOL_FALSE | tags::BOOL_TRUE => true, // tags already matched
        tags::INT64 | tags::FLOAT64 => left.get(1..9)? == right.get(1..9)?,
        tags::STRING => {
            let l_len = read_u32(left, 1)? as usize;
            let r_len = read_u32(right, 1)? as usize;
            l_len == r_len && left.get(5..5 + l_len)? == right.get(5..5 + r_len)?
        }
        tags::OBJECT => {
            // Every key in right must exist in left with a contained value.
            let r_count = read_u32(right, 1)? as usize;
            let r_offset_table = 5;
            let r_data_start = r_offset_table + r_count * 8;
            for i in 0..r_count {
                let entry = r_offset_table + i * 8;
                let key_off = read_u32(right, entry)? as usize;
                let key_abs = r_data_start + key_off;
                let key_len = read_u16(right, key_abs)? as usize;
                let key =
                    std::str::from_utf8(right.get(key_abs + 2..key_abs + 2 + key_len)?).ok()?;

                let val_off = read_u32(right, entry + 4)? as usize;
                let r_val = right.get(r_data_start + val_off..)?;

                match jsonb_get_field(left, key) {
                    Some(l_val) => {
                        if jsonb_contains(l_val, r_val) != Some(true) {
                            return Some(false);
                        }
                    }
                    None => return Some(false), // key not found
                }
            }
            true
        }
        tags::ARRAY => {
            // Every element in right must exist somewhere in left.
            let r_count = read_u32(right, 1)? as usize;
            let l_count = read_u32(left, 1)? as usize;
            'outer: for ri in 0..r_count {
                let r_elem = jsonb_array_get(right, ri)?;
                for li in 0..l_count {
                    let l_elem = jsonb_array_get(left, li)?;
                    if jsonb_contains(l_elem, r_elem) == Some(true) {
                        continue 'outer;
                    }
                }
                return Some(false);
            }
            true
        }
        _ => false,
    })
}

/// Encode a `serde_json::Value` into JSONB binary format.
///
/// Used by `json_build_object`, `json_build_array`, `to_jsonb` etc.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn encode_jsonb(value: &serde_json::Value) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    encode_jsonb_into(value, &mut buf);
    buf
}

/// Encode a JSON value into the given buffer.
#[allow(clippy::cast_possible_truncation)]
pub fn encode_jsonb_into(value: &serde_json::Value, buf: &mut Vec<u8>) {
    match value {
        serde_json::Value::Null => buf.push(tags::NULL),
        serde_json::Value::Bool(false) => buf.push(tags::BOOL_FALSE),
        serde_json::Value::Bool(true) => buf.push(tags::BOOL_TRUE),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                buf.push(tags::INT64);
                buf.extend_from_slice(&i.to_le_bytes());
            } else if let Some(f) = n.as_f64() {
                buf.push(tags::FLOAT64);
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
        serde_json::Value::String(s) => {
            buf.push(tags::STRING);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        serde_json::Value::Array(arr) => {
            buf.push(tags::ARRAY);
            buf.extend_from_slice(&(arr.len() as u32).to_le_bytes());
            let offset_table_pos = buf.len();
            buf.resize(buf.len() + arr.len() * 4, 0);
            let data_start = buf.len();
            for (i, elem) in arr.iter().enumerate() {
                let elem_offset = (buf.len() - data_start) as u32;
                let entry_pos = offset_table_pos + i * 4;
                buf[entry_pos..entry_pos + 4].copy_from_slice(&elem_offset.to_le_bytes());
                encode_jsonb_into(elem, buf);
            }
        }
        serde_json::Value::Object(obj) => {
            buf.push(tags::OBJECT);
            let mut keys: Vec<&String> = obj.keys().collect();
            keys.sort();
            buf.extend_from_slice(&(keys.len() as u32).to_le_bytes());
            let offset_table_pos = buf.len();
            buf.resize(buf.len() + keys.len() * 8, 0);
            let data_start = buf.len();
            for (i, key) in keys.iter().enumerate() {
                let key_offset = (buf.len() - data_start) as u32;
                let entry_pos = offset_table_pos + i * 8;
                buf[entry_pos..entry_pos + 4].copy_from_slice(&key_offset.to_le_bytes());
                buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
                buf.extend_from_slice(key.as_bytes());
                let val_offset = (buf.len() - data_start) as u32;
                buf[entry_pos + 4..entry_pos + 8].copy_from_slice(&val_offset.to_le_bytes());
                encode_jsonb_into(&obj[*key], buf);
            }
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────

#[inline]
fn read_u32(buf: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        buf.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

#[inline]
fn read_u16(buf: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        buf.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn enc(v: serde_json::Value) -> Vec<u8> {
        encode_jsonb(&v)
    }

    #[test]
    fn test_type_name() {
        assert_eq!(jsonb_type_name(&enc(json!(null))), Some("null"));
        assert_eq!(jsonb_type_name(&enc(json!(true))), Some("boolean"));
        assert_eq!(jsonb_type_name(&enc(json!(false))), Some("boolean"));
        assert_eq!(jsonb_type_name(&enc(json!(42))), Some("number"));
        assert_eq!(jsonb_type_name(&enc(json!(3.14))), Some("number"));
        assert_eq!(jsonb_type_name(&enc(json!("hi"))), Some("string"));
        assert_eq!(jsonb_type_name(&enc(json!([1]))), Some("array"));
        assert_eq!(jsonb_type_name(&enc(json!({"a": 1}))), Some("object"));
        assert_eq!(jsonb_type_name(&[]), None);
        assert_eq!(jsonb_type_name(&[0xFF]), None);
    }

    #[test]
    fn test_get_field() {
        let b = enc(json!({"name": "Alice", "age": 30}));
        let name = jsonb_get_field(&b, "name").unwrap();
        assert_eq!(jsonb_to_text(name), Some("Alice".to_owned()));
        let age = jsonb_get_field(&b, "age").unwrap();
        assert_eq!(jsonb_to_text(age), Some("30".to_owned()));
        assert!(jsonb_get_field(&b, "missing").is_none());
    }

    #[test]
    fn test_array_get() {
        let b = enc(json!([10, 20, 30]));
        let e1 = jsonb_array_get(&b, 1).unwrap();
        assert_eq!(jsonb_to_text(e1), Some("20".to_owned()));
        assert!(jsonb_array_get(&b, 5).is_none());
    }

    #[test]
    fn test_has_key() {
        let b = enc(json!({"a": 1, "b": 2}));
        assert!(jsonb_has_key(&b, "a"));
        assert!(!jsonb_has_key(&b, "c"));
    }

    #[test]
    fn test_to_text_string() {
        let b = enc(json!("hello"));
        assert_eq!(jsonb_to_text(&b), Some("hello".to_owned()));
    }

    #[test]
    fn test_to_text_null() {
        let b = enc(json!(null));
        assert_eq!(jsonb_to_text(&b), None);
    }

    #[test]
    fn test_to_text_object() {
        let b = enc(json!({"a": 1}));
        assert_eq!(jsonb_to_text(&b), Some("{\"a\":1}".to_owned()));
    }

    #[test]
    fn test_to_text_array() {
        let b = enc(json!([1, "two"]));
        assert_eq!(jsonb_to_text(&b), Some("[1,\"two\"]".to_owned()));
    }

    #[test]
    fn test_contains_object() {
        let left = enc(json!({"a": 1, "b": 2, "c": 3}));
        let right = enc(json!({"a": 1, "c": 3}));
        assert_eq!(jsonb_contains(&left, &right), Some(true));
    }

    #[test]
    fn test_contains_object_false() {
        let left = enc(json!({"a": 1}));
        let right = enc(json!({"a": 1, "b": 2}));
        assert_eq!(jsonb_contains(&left, &right), Some(false));
    }

    #[test]
    fn test_contains_array() {
        let left = enc(json!([1, 2, 3]));
        let right = enc(json!([1, 3]));
        assert_eq!(jsonb_contains(&left, &right), Some(true));
    }

    #[test]
    fn test_contains_scalar() {
        let a = enc(json!(42));
        let b = enc(json!(42));
        let c = enc(json!(99));
        assert_eq!(jsonb_contains(&a, &b), Some(true));
        assert_eq!(jsonb_contains(&a, &c), Some(false));
    }

    #[test]
    fn test_contains_type_mismatch() {
        let a = enc(json!(42));
        let b = enc(json!("42"));
        assert_eq!(jsonb_contains(&a, &b), Some(false));
    }

    #[test]
    fn test_nested_get() {
        let b = enc(json!({"user": {"address": {"city": "London"}}}));
        let user = jsonb_get_field(&b, "user").unwrap();
        let addr = jsonb_get_field(user, "address").unwrap();
        let city = jsonb_get_field(addr, "city").unwrap();
        assert_eq!(jsonb_to_text(city), Some("London".to_owned()));
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let vals = vec![
            json!(null),
            json!(true),
            json!(false),
            json!(42),
            json!(3.14),
            json!("hello"),
            json!([1, "two", null]),
            json!({"key": "value", "num": 42}),
        ];
        for v in vals {
            let b = enc(v.clone());
            let text = jsonb_to_json_string(&b);
            assert!(text.is_some(), "Failed to round-trip: {v:?}");
        }
    }

    #[test]
    fn test_jsonb_to_value_scalars() {
        assert_eq!(jsonb_to_value(&enc(json!(null))), Some(json!(null)));
        assert_eq!(jsonb_to_value(&enc(json!(true))), Some(json!(true)));
        assert_eq!(jsonb_to_value(&enc(json!(false))), Some(json!(false)));
        assert_eq!(jsonb_to_value(&enc(json!(42))), Some(json!(42)));
        assert_eq!(jsonb_to_value(&enc(json!(3.14))), Some(json!(3.14)));
        assert_eq!(jsonb_to_value(&enc(json!("hello"))), Some(json!("hello")));
    }

    #[test]
    fn test_jsonb_to_value_complex() {
        let obj = json!({"a": 1, "b": [2, 3], "c": {"d": true}});
        let bytes = enc(obj.clone());
        assert_eq!(jsonb_to_value(&bytes), Some(obj));
    }

    #[test]
    fn test_jsonb_to_value_empty() {
        assert_eq!(jsonb_to_value(&[]), None);
        assert_eq!(jsonb_to_value(&[0xFF]), None);
    }
}
