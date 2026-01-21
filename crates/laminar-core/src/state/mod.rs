//! # State Store Module
//!
//! High-performance state storage for streaming operators.
//!
//! ## Design Goals
//!
//! - **< 500ns lookup latency** for point queries
//! - **Zero-copy** access where possible
//! - **Lock-free** for single-threaded access
//! - **Memory-mapped** for large state
//!
//! ## State Backends
//!
//! - **InMemory**: HashMap-based, fastest for small state
//! - **MemoryMapped**: mmap-based, supports larger-than-memory state
//! - **Hybrid**: Combination with hot/cold separation

use bytes::Bytes;
use std::ops::Range;

/// Trait for state store implementations
pub trait StateStore: Send {
    /// Get a value by key
    fn get(&self, key: &[u8]) -> Option<Bytes>;

    /// Put a key-value pair
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateError>;

    /// Delete a key
    fn delete(&mut self, key: &[u8]) -> Result<(), StateError>;

    /// Scan keys with a given prefix
    fn prefix_scan<'a>(&'a self, prefix: &'a [u8]) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a>;

    /// Range scan between two keys
    fn range_scan<'a>(&'a self, range: Range<&'a [u8]>) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a>;

    /// Get approximate size in bytes
    fn size_bytes(&self) -> usize;

    /// Flush any pending writes
    fn flush(&mut self) -> Result<(), StateError> {
        Ok(()) // Default no-op
    }
}

/// In-memory state store using FxHashMap
pub struct InMemoryStore {
    // TODO: Use FxHashMap for better performance
    data: std::collections::HashMap<Vec<u8>, Bytes>,
}

impl InMemoryStore {
    /// Creates a new in-memory store
    pub fn new() -> Self {
        Self {
            data: std::collections::HashMap::new(),
        }
    }
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for InMemoryStore {
    fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.data.get(key).cloned()
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateError> {
        self.data.insert(key.to_vec(), Bytes::copy_from_slice(value));
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        self.data.remove(key);
        Ok(())
    }

    fn prefix_scan<'a>(&'a self, prefix: &'a [u8]) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a> {
        Box::new(
            self.data
                .iter()
                .filter(move |(k, _)| k.starts_with(prefix))
                .map(|(k, v)| (Bytes::copy_from_slice(k), v.clone())),
        )
    }

    fn range_scan<'a>(&'a self, range: Range<&'a [u8]>) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a> {
        Box::new(
            self.data
                .iter()
                .filter(move |(k, _)| k.as_slice() >= range.start && k.as_slice() < range.end)
                .map(|(k, v)| (Bytes::copy_from_slice(k), v.clone())),
        )
    }

    fn size_bytes(&self) -> usize {
        self.data
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum()
    }
}

/// Errors that can occur in state operations
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// I/O error (for memory-mapped stores)
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// State corruption detected
    #[error("State corruption: {0}")]
    Corruption(String),

    /// Operation not supported
    #[error("Operation not supported: {0}")]
    NotSupported(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_store_basic() {
        let mut store = InMemoryStore::new();

        // Test put and get
        store.put(b"key1", b"value1").unwrap();
        assert_eq!(store.get(b"key1").unwrap(), Bytes::from("value1"));

        // Test delete
        store.delete(b"key1").unwrap();
        assert!(store.get(b"key1").is_none());
    }

    #[test]
    fn test_prefix_scan() {
        let mut store = InMemoryStore::new();
        store.put(b"prefix:1", b"value1").unwrap();
        store.put(b"prefix:2", b"value2").unwrap();
        store.put(b"other:1", b"value3").unwrap();

        let results: Vec<_> = store.prefix_scan(b"prefix:").collect();
        assert_eq!(results.len(), 2);
    }
}