//! JSONB binary format for O(log n) field access on Ring 0.
//!
//! The JSONB format is a compact binary encoding of JSON values with
//! pre-computed byte offsets. Object keys are sorted alphabetically,
//! enabling binary-search field lookups in <100ns for typical objects.
//!
//! # Type Tags
//!
//! | Tag | Type | Data |
//! |-----|------|------|
//! | 0x00 | Null | (none) |
//! | 0x01 | Boolean false | (none) |
//! | 0x02 | Boolean true | (none) |
//! | 0x03 | Int64 | 8 bytes LE |
//! | 0x04 | Float64 | 8 bytes IEEE 754 LE |
//! | 0x05 | String | 4-byte LE length + UTF-8 bytes |
//! | 0x06 | Array | 4-byte count + offset table + elements |
//! | 0x07 | Object | 4-byte count + offset table + key-value data |

/// Type tags for the JSONB binary format.
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

/// Encodes `serde_json::Value` into JSONB binary format.
///
/// Used in Ring 1 during JSON decode to pre-compute the binary
/// representation that Ring 0 accesses via [`JsonbAccessor`].
#[derive(Debug)]
pub struct JsonbEncoder {
    buf: Vec<u8>,
}

impl JsonbEncoder {
    /// Creates a new encoder with a default 4 KiB buffer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(4096),
        }
    }

    /// Encodes a JSON value into JSONB binary format, returning the bytes.
    pub fn encode(&mut self, value: &serde_json::Value) -> Vec<u8> {
        self.buf.clear();
        self.encode_value(value);
        self.buf.clone()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn encode_value(&mut self, value: &serde_json::Value) {
        match value {
            serde_json::Value::Null => self.buf.push(tags::NULL),
            serde_json::Value::Bool(false) => self.buf.push(tags::BOOL_FALSE),
            serde_json::Value::Bool(true) => self.buf.push(tags::BOOL_TRUE),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    self.buf.push(tags::INT64);
                    self.buf.extend_from_slice(&i.to_le_bytes());
                } else if let Some(f) = n.as_f64() {
                    self.buf.push(tags::FLOAT64);
                    self.buf.extend_from_slice(&f.to_le_bytes());
                }
            }
            serde_json::Value::String(s) => {
                self.buf.push(tags::STRING);
                self.buf
                    .extend_from_slice(&(s.len() as u32).to_le_bytes());
                self.buf.extend_from_slice(s.as_bytes());
            }
            serde_json::Value::Array(arr) => {
                self.buf.push(tags::ARRAY);
                self.buf
                    .extend_from_slice(&(arr.len() as u32).to_le_bytes());
                // Reserve space for offset table.
                let offset_table_pos = self.buf.len();
                self.buf.resize(self.buf.len() + arr.len() * 4, 0);
                let data_start = self.buf.len();
                for (i, elem) in arr.iter().enumerate() {
                    let elem_offset = (self.buf.len() - data_start) as u32;
                    let entry_pos = offset_table_pos + i * 4;
                    self.buf[entry_pos..entry_pos + 4]
                        .copy_from_slice(&elem_offset.to_le_bytes());
                    self.encode_value(elem);
                }
            }
            serde_json::Value::Object(obj) => {
                self.buf.push(tags::OBJECT);
                // Sort keys for binary search.
                let mut keys: Vec<&String> = obj.keys().collect();
                keys.sort();
                self.buf
                    .extend_from_slice(&(keys.len() as u32).to_le_bytes());
                // Reserve space for offset table (key_off + val_off per field).
                let offset_table_pos = self.buf.len();
                self.buf.resize(self.buf.len() + keys.len() * 8, 0);
                let data_start = self.buf.len();

                for (i, key) in keys.iter().enumerate() {
                    // Write key offset.
                    let key_offset = (self.buf.len() - data_start) as u32;
                    let entry_pos = offset_table_pos + i * 8;
                    self.buf[entry_pos..entry_pos + 4]
                        .copy_from_slice(&key_offset.to_le_bytes());
                    // Write key (u16 length + UTF-8 bytes).
                    self.buf
                        .extend_from_slice(&(key.len() as u16).to_le_bytes());
                    self.buf.extend_from_slice(key.as_bytes());
                    // Write value offset.
                    let val_offset = (self.buf.len() - data_start) as u32;
                    self.buf[entry_pos + 4..entry_pos + 8]
                        .copy_from_slice(&val_offset.to_le_bytes());
                    // Write value.
                    self.encode_value(&obj[*key]);
                }
            }
        }
    }
}

impl Default for JsonbEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Zero-allocation JSONB accessor for Ring 0 hot-path field lookups.
///
/// All operations return byte slices into the original JSONB binary
/// buffer — no heap allocation occurs.
pub struct JsonbAccessor;

impl JsonbAccessor {
    /// Access a field by name in a JSONB object.
    ///
    /// Returns a byte slice pointing to the field's JSONB value,
    /// or `None` if the field does not exist or the value is not an object.
    ///
    /// Performance: O(log n) binary search on sorted keys.
    #[inline]
    #[must_use]
    pub fn get_field<'a>(jsonb: &'a [u8], field_name: &str) -> Option<&'a [u8]> {
        if jsonb.is_empty() || jsonb[0] != tags::OBJECT {
            return None;
        }

        let field_count = u32::from_le_bytes(jsonb.get(1..5)?.try_into().ok()?) as usize;
        if field_count == 0 {
            return None;
        }

        let offset_table_start = 5;
        let offset_table_end = offset_table_start + field_count * 8;
        let data_start = offset_table_end;

        // Binary search on sorted keys.
        let mut lo = 0usize;
        let mut hi = field_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry_offset = offset_table_start + mid * 8;
            let key_off =
                u32::from_le_bytes(jsonb.get(entry_offset..entry_offset + 4)?.try_into().ok()?)
                    as usize;

            let key_abs = data_start + key_off;
            let key_len =
                u16::from_le_bytes(jsonb.get(key_abs..key_abs + 2)?.try_into().ok()?) as usize;
            let key_bytes = jsonb.get(key_abs + 2..key_abs + 2 + key_len)?;
            let key_str = std::str::from_utf8(key_bytes).ok()?;

            match key_str.cmp(field_name) {
                std::cmp::Ordering::Equal => {
                    let val_off = u32::from_le_bytes(
                        jsonb.get(entry_offset + 4..entry_offset + 8)?.try_into().ok()?,
                    ) as usize;
                    let val_abs = data_start + val_off;
                    return jsonb.get(val_abs..);
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    /// Returns `true` if the JSONB value is null (tag 0x00).
    #[inline]
    #[must_use]
    pub fn is_null(jsonb_value: &[u8]) -> bool {
        !jsonb_value.is_empty() && jsonb_value[0] == tags::NULL
    }

    /// Extract a boolean from a JSONB value slice.
    #[inline]
    #[must_use]
    pub fn as_bool(jsonb_value: &[u8]) -> Option<bool> {
        match *jsonb_value.first()? {
            tags::BOOL_FALSE => Some(false),
            tags::BOOL_TRUE => Some(true),
            _ => None,
        }
    }

    /// Extract an i64 from a JSONB value slice.
    #[inline]
    #[must_use]
    pub fn as_i64(jsonb_value: &[u8]) -> Option<i64> {
        if jsonb_value.first()? != &tags::INT64 {
            return None;
        }
        Some(i64::from_le_bytes(
            jsonb_value.get(1..9)?.try_into().ok()?,
        ))
    }

    /// Extract an f64 from a JSONB value slice.
    #[inline]
    #[must_use]
    pub fn as_f64(jsonb_value: &[u8]) -> Option<f64> {
        if jsonb_value.first()? != &tags::FLOAT64 {
            return None;
        }
        Some(f64::from_le_bytes(
            jsonb_value.get(1..9)?.try_into().ok()?,
        ))
    }

    /// Extract a string from a JSONB value slice.
    #[inline]
    #[must_use]
    pub fn as_str(jsonb_value: &[u8]) -> Option<&str> {
        if jsonb_value.first()? != &tags::STRING {
            return None;
        }
        let len =
            u32::from_le_bytes(jsonb_value.get(1..5)?.try_into().ok()?) as usize;
        std::str::from_utf8(jsonb_value.get(5..5 + len)?).ok()
    }

    /// Get the element count of a JSONB array.
    #[inline]
    #[must_use]
    pub fn array_len(jsonb_value: &[u8]) -> Option<usize> {
        if jsonb_value.first()? != &tags::ARRAY {
            return None;
        }
        Some(u32::from_le_bytes(jsonb_value.get(1..5)?.try_into().ok()?) as usize)
    }

    /// Get a JSONB array element by index.
    #[inline]
    #[must_use]
    pub fn array_get(jsonb_value: &[u8], index: usize) -> Option<&[u8]> {
        if jsonb_value.first()? != &tags::ARRAY {
            return None;
        }
        let count =
            u32::from_le_bytes(jsonb_value.get(1..5)?.try_into().ok()?) as usize;
        if index >= count {
            return None;
        }
        let offset_table_start = 5;
        let data_start = offset_table_start + count * 4;
        let entry_pos = offset_table_start + index * 4;
        let elem_off =
            u32::from_le_bytes(jsonb_value.get(entry_pos..entry_pos + 4)?.try_into().ok()?)
                as usize;
        jsonb_value.get(data_start + elem_off..)
    }

    /// Get the field count of a JSONB object.
    #[inline]
    #[must_use]
    pub fn object_len(jsonb_value: &[u8]) -> Option<usize> {
        if jsonb_value.first()? != &tags::OBJECT {
            return None;
        }
        Some(u32::from_le_bytes(jsonb_value.get(1..5)?.try_into().ok()?) as usize)
    }
}

#[cfg(test)]
mod tests {
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
}
