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

/// Re-export canonical JSONB binary format type tags from `laminar-core`.
pub use laminar_core::serialization::jsonb_tags as tags;

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
                self.buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
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
                    self.buf[entry_pos..entry_pos + 4].copy_from_slice(&elem_offset.to_le_bytes());
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
                    self.buf[entry_pos..entry_pos + 4].copy_from_slice(&key_offset.to_le_bytes());
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
                        jsonb
                            .get(entry_offset + 4..entry_offset + 8)?
                            .try_into()
                            .ok()?,
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
        Some(i64::from_le_bytes(jsonb_value.get(1..9)?.try_into().ok()?))
    }

    /// Extract an f64 from a JSONB value slice.
    #[inline]
    #[must_use]
    pub fn as_f64(jsonb_value: &[u8]) -> Option<f64> {
        if jsonb_value.first()? != &tags::FLOAT64 {
            return None;
        }
        Some(f64::from_le_bytes(jsonb_value.get(1..9)?.try_into().ok()?))
    }

    /// Extract a string from a JSONB value slice.
    #[inline]
    #[must_use]
    pub fn as_str(jsonb_value: &[u8]) -> Option<&str> {
        if jsonb_value.first()? != &tags::STRING {
            return None;
        }
        let len = u32::from_le_bytes(jsonb_value.get(1..5)?.try_into().ok()?) as usize;
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
        let count = u32::from_le_bytes(jsonb_value.get(1..5)?.try_into().ok()?) as usize;
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
mod tests;
