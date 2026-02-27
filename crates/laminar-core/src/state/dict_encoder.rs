//! Dictionary key encoder for low-cardinality GROUP BY keys.
//!
//! Maps variable-length byte keys (e.g., ticker symbols like `"AAPL"`, region
//! names like `"us-east-1"`) to compact fixed-size `u32` codes. This reduces
//! hash map key sizes from variable-length to 4 bytes, improving:
//!
//! - **Hash throughput**: hashing 4 bytes is ~5x faster than hashing 20+ byte strings
//! - **Cache utilization**: smaller keys mean more entries per cache line
//! - **Comparison cost**: `u32 == u32` is a single instruction vs `memcmp`
//!
//! # Architecture
//!
//! - **Ring 2 (setup)**: Call [`DictionaryKeyEncoder::encode_or_insert`] during
//!   query registration to populate the dictionary with known keys.
//! - **Ring 0 (hot path)**: Call [`DictionaryKeyEncoder::encode`] for O(1) lookup
//!   of pre-populated keys. Unknown keys fall through to the raw-byte path.
//!
//! # Example
//!
//! ```rust
//! use laminar_core::state::DictionaryKeyEncoder;
//!
//! let mut encoder = DictionaryKeyEncoder::new();
//!
//! // Ring 2: populate during query setup
//! let code_aapl = encoder.encode_or_insert(b"AAPL");
//! let code_goog = encoder.encode_or_insert(b"GOOG");
//! assert_ne!(code_aapl, code_goog);
//!
//! // Ring 0: fast lookup on hot path
//! assert_eq!(encoder.encode(b"AAPL"), Some(code_aapl));
//! assert_eq!(encoder.encode(b"UNKNOWN"), None);
//!
//! // Decode back to original key
//! assert_eq!(encoder.decode(code_aapl), Some(b"AAPL".as_slice()));
//! ```

use rustc_hash::{FxBuildHasher, FxHashMap};

/// Compact dictionary encoder that maps variable-length byte keys to `u32` codes.
///
/// Pre-populate during query setup (Ring 2), then use the fast `encode()` path
/// during event processing (Ring 0). The encoder is `Send` for transfer to
/// reactor threads but not `Sync` — it's designed for single-threaded access.
pub struct DictionaryKeyEncoder {
    /// Forward mapping: raw key bytes → compact code.
    key_to_code: FxHashMap<Vec<u8>, u32>,
    /// Reverse mapping: compact code → raw key bytes.
    code_to_key: Vec<Vec<u8>>,
}

impl DictionaryKeyEncoder {
    /// Creates a new empty encoder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            key_to_code: FxHashMap::default(),
            code_to_key: Vec::new(),
        }
    }

    /// Creates a new encoder with pre-allocated capacity.
    ///
    /// Use this when the approximate number of distinct keys is known
    /// (e.g., number of ticker symbols, region names).
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            key_to_code: FxHashMap::with_capacity_and_hasher(capacity, FxBuildHasher),
            code_to_key: Vec::with_capacity(capacity),
        }
    }

    /// Encodes a key to its compact `u32` code (Ring 0 hot path).
    ///
    /// Returns `None` if the key has not been inserted. This is a pure
    /// hash map lookup with no allocation.
    #[inline]
    #[must_use]
    pub fn encode(&self, key: &[u8]) -> Option<u32> {
        self.key_to_code.get(key).copied()
    }

    /// Encodes a key, inserting it if not already present (Ring 2 setup).
    ///
    /// Allocates on first insertion of a new key. Subsequent calls for the
    /// same key return the existing code without allocation.
    ///
    /// # Panics
    ///
    /// Panics if more than `u32::MAX` distinct keys are inserted.
    pub fn encode_or_insert(&mut self, key: &[u8]) -> u32 {
        if let Some(&code) = self.key_to_code.get(key) {
            return code;
        }
        let code =
            u32::try_from(self.code_to_key.len()).expect("dictionary overflow: > u32::MAX keys");
        self.key_to_code.insert(key.to_vec(), code);
        self.code_to_key.push(key.to_vec());
        code
    }

    /// Decodes a compact code back to the original key bytes.
    ///
    /// Returns `None` if the code is out of range.
    #[inline]
    #[must_use]
    pub fn decode(&self, code: u32) -> Option<&[u8]> {
        self.code_to_key.get(code as usize).map(Vec::as_slice)
    }

    /// Returns the encoded key as a 4-byte little-endian slice.
    ///
    /// Useful for passing encoded keys to state stores that expect `&[u8]`.
    #[inline]
    #[must_use]
    pub fn encode_to_bytes(&self, key: &[u8]) -> Option<[u8; 4]> {
        self.encode(key).map(u32::to_le_bytes)
    }

    /// Returns the number of distinct keys in the dictionary.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.code_to_key.len()
    }

    /// Returns true if the dictionary is empty.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.code_to_key.is_empty()
    }

    /// Clears the dictionary, removing all mappings.
    pub fn clear(&mut self) {
        self.key_to_code.clear();
        self.code_to_key.clear();
    }
}

impl Default for DictionaryKeyEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut enc = DictionaryKeyEncoder::new();
        let code = enc.encode_or_insert(b"AAPL");
        assert_eq!(enc.decode(code), Some(b"AAPL".as_slice()));
    }

    #[test]
    fn test_idempotent_insert() {
        let mut enc = DictionaryKeyEncoder::new();
        let c1 = enc.encode_or_insert(b"GOOG");
        let c2 = enc.encode_or_insert(b"GOOG");
        assert_eq!(c1, c2);
        assert_eq!(enc.len(), 1);
    }

    #[test]
    fn test_distinct_codes() {
        let mut enc = DictionaryKeyEncoder::new();
        let c1 = enc.encode_or_insert(b"AAPL");
        let c2 = enc.encode_or_insert(b"GOOG");
        let c3 = enc.encode_or_insert(b"MSFT");
        assert_ne!(c1, c2);
        assert_ne!(c2, c3);
        assert_eq!(enc.len(), 3);
    }

    #[test]
    fn test_encode_unknown_returns_none() {
        let enc = DictionaryKeyEncoder::new();
        assert_eq!(enc.encode(b"UNKNOWN"), None);
    }

    #[test]
    fn test_encode_to_bytes() {
        let mut enc = DictionaryKeyEncoder::new();
        let code = enc.encode_or_insert(b"TEST");
        let bytes = enc.encode_to_bytes(b"TEST").unwrap();
        assert_eq!(bytes, code.to_le_bytes());
        assert_eq!(enc.encode_to_bytes(b"MISSING"), None);
    }

    #[test]
    fn test_decode_out_of_range() {
        let enc = DictionaryKeyEncoder::new();
        assert_eq!(enc.decode(0), None);
        assert_eq!(enc.decode(999), None);
    }

    #[test]
    fn test_with_capacity() {
        let enc = DictionaryKeyEncoder::with_capacity(1000);
        assert!(enc.is_empty());
        assert_eq!(enc.len(), 0);
    }

    #[test]
    fn test_clear() {
        let mut enc = DictionaryKeyEncoder::new();
        enc.encode_or_insert(b"A");
        enc.encode_or_insert(b"B");
        assert_eq!(enc.len(), 2);

        enc.clear();
        assert!(enc.is_empty());
        assert_eq!(enc.encode(b"A"), None);
    }

    #[test]
    fn test_sequential_codes() {
        let mut enc = DictionaryKeyEncoder::new();
        assert_eq!(enc.encode_or_insert(b"first"), 0);
        assert_eq!(enc.encode_or_insert(b"second"), 1);
        assert_eq!(enc.encode_or_insert(b"third"), 2);
    }

    #[test]
    fn test_binary_keys() {
        let mut enc = DictionaryKeyEncoder::new();
        let key = [0x00, 0xFF, 0xDE, 0xAD];
        let code = enc.encode_or_insert(&key);
        assert_eq!(enc.decode(code), Some(key.as_slice()));
    }
}
