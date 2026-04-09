//! Lookup result type.

use arrow_array::RecordBatch;

/// Result of a lookup operation.
#[derive(Debug, Clone)]
pub enum LookupResult {
    /// Cache hit.
    Hit(RecordBatch),
    /// Async lookup in progress.
    Pending,
    /// Key not found.
    NotFound,
}

impl LookupResult {
    /// Returns `true` if this is a cache hit.
    #[must_use]
    pub const fn is_hit(&self) -> bool {
        matches!(self, Self::Hit(_))
    }

    /// Returns `true` if the key was not found.
    #[must_use]
    pub const fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound)
    }

    /// Extracts the `RecordBatch` from a `Hit`.
    #[must_use]
    pub fn into_batch(self) -> Option<RecordBatch> {
        match self {
            Self::Hit(b) => Some(b),
            _ => None,
        }
    }
}

impl PartialEq for LookupResult {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Hit(a), Self::Hit(b)) => a == b,
            (Self::Pending, Self::Pending) | (Self::NotFound, Self::NotFound) => true,
            _ => false,
        }
    }
}

impl Eq for LookupResult {}
