//! Secondary index support using redb.
//!
//! Provides memcomparable key encoding and a redb-backed index manager
//! for point lookups and range scans on lookup table columns.

pub mod encoding;
pub mod manager;
pub mod redb_index;

pub use encoding::{decode_comparable, encode_comparable};
pub use manager::IndexManager;
pub use redb_index::SecondaryIndex;
