//! Memcomparable key encoding for secondary index keys.
//!
//! Encodes `ScalarValue`s into byte sequences that preserve sort order
//! when compared lexicographically. This allows redb (which sorts by raw bytes)
//! to support ordered range scans over typed values.
//!
//! Encoding rules:
//! - **Null**: `0x00` (sorts before all non-null values)
//! - **Bool**: `0x01` + `0x00`/`0x01`
//! - **Signed integers**: `0x02` + big-endian bytes with sign bit flipped
//! - **Float64**: `0x04` + IEEE 754 order-preserving encoding
//! - **Utf8**: `0x05` + escaped bytes + `0x00 0x00` terminator
//! - **Binary**: `0x06` + escaped bytes + `0x00 0x00` terminator

use crate::lookup::ScalarValue;

/// Tag bytes for each type family.
const TAG_NULL: u8 = 0x00;
const TAG_BOOL: u8 = 0x01;
const TAG_SIGNED: u8 = 0x02;
const TAG_FLOAT: u8 = 0x04;
const TAG_UTF8: u8 = 0x05;
const TAG_BINARY: u8 = 0x06;

/// Encodes a `ScalarValue` into a memcomparable byte sequence.
#[must_use]
pub fn encode_comparable(value: &ScalarValue) -> Vec<u8> {
    match value {
        ScalarValue::Null => vec![TAG_NULL],
        ScalarValue::Bool(b) => vec![TAG_BOOL, u8::from(*b)],
        ScalarValue::Int64(v) | ScalarValue::Timestamp(v) => {
            encode_signed(TAG_SIGNED, &v.to_be_bytes())
        }
        ScalarValue::Float64(v) => encode_float64(*v),
        ScalarValue::Utf8(s) => encode_bytes(TAG_UTF8, s.as_bytes()),
        ScalarValue::Binary(b) => encode_bytes(TAG_BINARY, b),
    }
}

/// Decodes a memcomparable byte sequence back into a `ScalarValue`.
///
/// The `tag` in the encoded bytes determines which variant is produced.
/// Use this to round-trip values through the index.
///
/// # Panics
///
/// Panics if the byte sequence is malformed.
#[must_use]
pub fn decode_comparable(bytes: &[u8]) -> ScalarValue {
    if bytes.is_empty() || bytes[0] == TAG_NULL {
        return ScalarValue::Null;
    }

    let tag = bytes[0];
    let payload = &bytes[1..];

    match tag {
        TAG_BOOL => ScalarValue::Bool(payload[0] != 0),
        TAG_SIGNED => ScalarValue::Int64(decode_signed_i64(payload)),
        TAG_FLOAT => ScalarValue::Float64(decode_float64(payload)),
        TAG_UTF8 => {
            let raw = decode_escaped_bytes(payload);
            ScalarValue::Utf8(String::from_utf8(raw).expect("invalid UTF-8 in index"))
        }
        TAG_BINARY => ScalarValue::Binary(decode_escaped_bytes(payload)),
        _ => ScalarValue::Null,
    }
}

fn encode_signed(tag: u8, be_bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + be_bytes.len());
    out.push(tag);
    // Flip sign bit so negative values sort before positive
    let mut flipped = be_bytes.to_vec();
    flipped[0] ^= 0x80;
    out.extend_from_slice(&flipped);
    out
}

/// IEEE 754 order-preserving encoding:
/// - If positive (sign bit 0): flip sign bit -> all positive floats sort correctly
/// - If negative (sign bit 1): flip all bits -> negative floats sort in reverse
fn encode_float64(v: f64) -> Vec<u8> {
    let bits = v.to_bits();
    let encoded = if bits & (1u64 << 63) == 0 {
        bits ^ (1u64 << 63) // positive: flip sign bit
    } else {
        !bits // negative: flip all bits
    };
    let mut out = Vec::with_capacity(9);
    out.push(TAG_FLOAT);
    out.extend_from_slice(&encoded.to_be_bytes());
    out
}

/// Null-terminated byte-stuffing escape for strings/binary:
/// - `0x00` in input -> `0x00 0xFF`
/// - End of data -> `0x00 0x00`
fn encode_bytes(tag: u8, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + data.len() + 2);
    out.push(tag);
    for &b in data {
        if b == 0x00 {
            out.push(0x00);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    out.push(0x00);
    out.push(0x00);
    out
}

fn decode_signed_i64(payload: &[u8]) -> i64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&payload[..8]);
    bytes[0] ^= 0x80; // un-flip sign bit
    i64::from_be_bytes(bytes)
}

fn decode_float64(payload: &[u8]) -> f64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&payload[..8]);
    let encoded = u64::from_be_bytes(bytes);
    let bits = if encoded & (1u64 << 63) != 0 {
        encoded ^ (1u64 << 63) // was positive: un-flip sign bit
    } else {
        !encoded // was negative: un-flip all bits
    };
    f64::from_bits(bits)
}

fn decode_escaped_bytes(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < payload.len() {
        if payload[i] == 0x00 {
            if i + 1 < payload.len() && payload[i + 1] == 0xFF {
                out.push(0x00);
                i += 2;
            } else {
                // 0x00 0x00 terminator
                break;
            }
        } else {
            out.push(payload[i]);
            i += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_int64() {
        for v in [i64::MIN, -1_000_000, -1, 0, 1, 1_000_000, i64::MAX] {
            let sv = ScalarValue::Int64(v);
            let encoded = encode_comparable(&sv);
            let decoded = decode_comparable(&encoded);
            assert_eq!(decoded, sv, "roundtrip failed for {v}");
        }
    }

    #[test]
    fn test_roundtrip_float64() {
        for v in [-1.5, -0.0, 0.0, 1.5, f64::MAX, f64::MIN] {
            let sv = ScalarValue::Float64(v);
            let encoded = encode_comparable(&sv);
            let decoded = decode_comparable(&encoded);
            assert_eq!(decoded, sv, "roundtrip failed for {v}");
        }
    }

    #[test]
    fn test_roundtrip_utf8() {
        for s in ["", "hello", "hello\x00world", "abc\x00\x00def"] {
            let sv = ScalarValue::Utf8(s.to_string());
            let encoded = encode_comparable(&sv);
            let decoded = decode_comparable(&encoded);
            assert_eq!(decoded, sv, "roundtrip failed for {s:?}");
        }
    }

    #[test]
    fn test_roundtrip_binary() {
        let data = vec![0x00, 0x01, 0x00, 0xFF, 0x42];
        let sv = ScalarValue::Binary(data);
        let encoded = encode_comparable(&sv);
        let decoded = decode_comparable(&encoded);
        assert_eq!(decoded, sv);
    }

    #[test]
    fn test_roundtrip_bool() {
        for b in [false, true] {
            let sv = ScalarValue::Bool(b);
            let encoded = encode_comparable(&sv);
            let decoded = decode_comparable(&encoded);
            assert_eq!(decoded, sv);
        }
    }

    #[test]
    fn test_sort_order_signed() {
        let values = [-100i64, -1, 0, 1, 100];
        let encoded: Vec<_> = values
            .iter()
            .map(|v| encode_comparable(&ScalarValue::Int64(*v)))
            .collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order violated: {:?} should be < {:?} (values {} < {})",
                encoded[i],
                encoded[i + 1],
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_sort_order_float() {
        let values = [-1.5f64, -0.5, 0.0, 0.5, 1.5];
        let encoded: Vec<_> = values
            .iter()
            .map(|v| encode_comparable(&ScalarValue::Float64(*v)))
            .collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order violated: {} should be < {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_sort_order_utf8() {
        let values = ["aaa", "aab", "bbb", "ccc"];
        let encoded: Vec<_> = values
            .iter()
            .map(|v| encode_comparable(&ScalarValue::Utf8(v.to_string())))
            .collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order violated: {:?} should be < {:?}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_null_sorts_first() {
        let null_enc = encode_comparable(&ScalarValue::Null);
        let int_enc = encode_comparable(&ScalarValue::Int64(-999_999));
        assert!(null_enc < int_enc);
    }
}
