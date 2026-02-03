//! MySQL GTID (Global Transaction Identifier) type.
//!
//! GTIDs uniquely identify transactions across a MySQL replication topology.
//! Format: `source_id:transaction_id` where source_id is a UUID.
//!
//! # Examples
//!
//! ```
//! use laminar_connectors::cdc::mysql::Gtid;
//!
//! let gtid: Gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:23".parse().unwrap();
//! assert_eq!(gtid.transaction_id(), 23);
//! ```

use std::fmt;
use std::str::FromStr;

/// A MySQL Global Transaction Identifier (GTID).
///
/// GTIDs have the format `source_id:transaction_id` where:
/// - `source_id` is a UUID identifying the server that originated the transaction
/// - `transaction_id` is a 64-bit sequence number
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Gtid {
    /// Server UUID (source_id).
    source_id: uuid::Uuid,
    /// Transaction sequence number.
    transaction_id: u64,
}

impl Gtid {
    /// Creates a new GTID from components.
    #[must_use]
    pub fn new(source_id: uuid::Uuid, transaction_id: u64) -> Self {
        Self {
            source_id,
            transaction_id,
        }
    }

    /// Returns the server UUID (source_id).
    #[must_use]
    pub fn source_id(&self) -> uuid::Uuid {
        self.source_id
    }

    /// Returns the transaction sequence number.
    #[must_use]
    pub fn transaction_id(&self) -> u64 {
        self.transaction_id
    }

    /// Creates GTID from raw string representation.
    ///
    /// # Errors
    ///
    /// Returns error if the string is not a valid GTID format.
    pub fn from_string(s: &str) -> Result<Self, GtidParseError> {
        s.parse()
    }
}

impl fmt::Display for Gtid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.source_id, self.transaction_id)
    }
}

impl FromStr for Gtid {
    type Err = GtidParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(GtidParseError::InvalidFormat(s.to_string()));
        }

        let source_id = uuid::Uuid::parse_str(parts[0])
            .map_err(|e| GtidParseError::InvalidUuid(e.to_string()))?;

        let transaction_id = parts[1]
            .parse::<u64>()
            .map_err(|_| GtidParseError::InvalidTransactionId(parts[1].to_string()))?;

        Ok(Self {
            source_id,
            transaction_id,
        })
    }
}

impl PartialOrd for Gtid {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Gtid {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by source_id first, then transaction_id
        match self.source_id.cmp(&other.source_id) {
            std::cmp::Ordering::Equal => self.transaction_id.cmp(&other.transaction_id),
            other => other,
        }
    }
}

/// A set of GTIDs representing a position in the replication stream.
///
/// Format: `uuid1:interval1,uuid2:interval2,...`
/// where interval can be `n` or `n-m` (inclusive range).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GtidSet {
    /// Map of source_id UUID to executed transaction ranges.
    sets: std::collections::HashMap<uuid::Uuid, Vec<GtidRange>>,
}

/// A range of transaction IDs for a single source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GtidRange {
    /// Start transaction ID (inclusive).
    pub start: u64,
    /// End transaction ID (inclusive).
    pub end: u64,
}

impl GtidRange {
    /// Creates a single-transaction range.
    #[must_use]
    pub fn single(id: u64) -> Self {
        Self { start: id, end: id }
    }

    /// Creates a range from start to end (inclusive).
    #[must_use]
    pub fn range(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// Returns true if this range contains the given transaction ID.
    #[must_use]
    pub fn contains(&self, id: u64) -> bool {
        id >= self.start && id <= self.end
    }
}

impl GtidSet {
    /// Creates an empty GTID set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a GTID to the set.
    pub fn add(&mut self, gtid: &Gtid) {
        let ranges = self.sets.entry(gtid.source_id).or_default();

        // Try to extend existing range or add new one
        let tid = gtid.transaction_id;

        // Simple implementation: just add as new range
        // A production implementation would merge adjacent ranges
        ranges.push(GtidRange::single(tid));
    }

    /// Returns true if the set contains the given GTID.
    #[must_use]
    pub fn contains(&self, gtid: &Gtid) -> bool {
        self.sets.get(&gtid.source_id).map_or(false, |ranges| {
            ranges.iter().any(|r| r.contains(gtid.transaction_id))
        })
    }

    /// Returns true if the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sets.is_empty()
    }

    /// Returns the number of unique source IDs.
    #[must_use]
    pub fn source_count(&self) -> usize {
        self.sets.len()
    }
}

impl fmt::Display for GtidSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();
        for (source_id, ranges) in &self.sets {
            for range in ranges {
                if range.start == range.end {
                    parts.push(format!("{source_id}:{}", range.start));
                } else {
                    parts.push(format!("{source_id}:{}-{}", range.start, range.end));
                }
            }
        }
        write!(f, "{}", parts.join(","))
    }
}

impl FromStr for GtidSet {
    type Err = GtidParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Ok(Self::new());
        }

        let mut sets = std::collections::HashMap::new();

        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            // Format: uuid:n or uuid:n-m
            let colon_pos = part
                .rfind(':')
                .ok_or_else(|| GtidParseError::InvalidFormat(part.to_string()))?;

            let uuid_str = &part[..colon_pos];
            let range_str = &part[colon_pos + 1..];

            let source_id = uuid::Uuid::parse_str(uuid_str)
                .map_err(|e| GtidParseError::InvalidUuid(e.to_string()))?;

            let range = if let Some(dash_pos) = range_str.find('-') {
                let start = range_str[..dash_pos]
                    .parse::<u64>()
                    .map_err(|_| GtidParseError::InvalidTransactionId(range_str.to_string()))?;
                let end = range_str[dash_pos + 1..]
                    .parse::<u64>()
                    .map_err(|_| GtidParseError::InvalidTransactionId(range_str.to_string()))?;
                GtidRange::range(start, end)
            } else {
                let id = range_str
                    .parse::<u64>()
                    .map_err(|_| GtidParseError::InvalidTransactionId(range_str.to_string()))?;
                GtidRange::single(id)
            };

            sets.entry(source_id).or_insert_with(Vec::new).push(range);
        }

        Ok(Self { sets })
    }
}

/// Errors from parsing GTID strings.
#[derive(Debug, Clone, thiserror::Error)]
pub enum GtidParseError {
    /// Invalid GTID format.
    #[error("invalid GTID format: {0}")]
    InvalidFormat(String),

    /// Invalid UUID in GTID.
    #[error("invalid UUID: {0}")]
    InvalidUuid(String),

    /// Invalid transaction ID.
    #[error("invalid transaction ID: {0}")]
    InvalidTransactionId(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_UUID: &str = "3E11FA47-71CA-11E1-9E33-C80AA9429562";

    #[test]
    fn test_gtid_parse() {
        let gtid: Gtid = format!("{TEST_UUID}:23").parse().unwrap();
        assert_eq!(gtid.transaction_id(), 23);
        assert_eq!(
            gtid.source_id(),
            uuid::Uuid::parse_str(TEST_UUID).unwrap()
        );
    }

    #[test]
    fn test_gtid_display() {
        let gtid: Gtid = format!("{TEST_UUID}:42").parse().unwrap();
        let s = gtid.to_string();
        assert!(s.contains(":42"));
    }

    #[test]
    fn test_gtid_parse_invalid() {
        assert!("invalid".parse::<Gtid>().is_err());
        assert!("not-a-uuid:123".parse::<Gtid>().is_err());
        assert!(format!("{TEST_UUID}:not-a-number").parse::<Gtid>().is_err());
    }

    #[test]
    fn test_gtid_ordering() {
        let gtid1: Gtid = format!("{TEST_UUID}:1").parse().unwrap();
        let gtid2: Gtid = format!("{TEST_UUID}:2").parse().unwrap();
        assert!(gtid1 < gtid2);
    }

    #[test]
    fn test_gtid_set_empty() {
        let set = GtidSet::new();
        assert!(set.is_empty());
        assert_eq!(set.source_count(), 0);
    }

    #[test]
    fn test_gtid_set_add_and_contains() {
        let mut set = GtidSet::new();
        let gtid: Gtid = format!("{TEST_UUID}:5").parse().unwrap();

        assert!(!set.contains(&gtid));
        set.add(&gtid);
        assert!(set.contains(&gtid));
        assert_eq!(set.source_count(), 1);
    }

    #[test]
    fn test_gtid_set_parse_single() {
        let set: GtidSet = format!("{TEST_UUID}:1").parse().unwrap();
        let gtid: Gtid = format!("{TEST_UUID}:1").parse().unwrap();
        assert!(set.contains(&gtid));
    }

    #[test]
    fn test_gtid_set_parse_range() {
        let set: GtidSet = format!("{TEST_UUID}:1-10").parse().unwrap();
        let gtid1: Gtid = format!("{TEST_UUID}:1").parse().unwrap();
        let gtid5: Gtid = format!("{TEST_UUID}:5").parse().unwrap();
        let gtid10: Gtid = format!("{TEST_UUID}:10").parse().unwrap();
        let gtid11: Gtid = format!("{TEST_UUID}:11").parse().unwrap();

        assert!(set.contains(&gtid1));
        assert!(set.contains(&gtid5));
        assert!(set.contains(&gtid10));
        assert!(!set.contains(&gtid11));
    }

    #[test]
    fn test_gtid_set_parse_multiple() {
        let uuid2 = "4E11FA47-71CA-11E1-9E33-C80AA9429563";
        let set: GtidSet = format!("{TEST_UUID}:1,{uuid2}:5").parse().unwrap();
        assert_eq!(set.source_count(), 2);
    }

    #[test]
    fn test_gtid_set_display() {
        let mut set = GtidSet::new();
        let gtid: Gtid = format!("{TEST_UUID}:42").parse().unwrap();
        set.add(&gtid);
        let s = set.to_string();
        assert!(s.contains(":42"));
    }

    #[test]
    fn test_gtid_range_contains() {
        let range = GtidRange::range(5, 10);
        assert!(!range.contains(4));
        assert!(range.contains(5));
        assert!(range.contains(7));
        assert!(range.contains(10));
        assert!(!range.contains(11));
    }

    #[test]
    fn test_gtid_range_single() {
        let range = GtidRange::single(5);
        assert!(!range.contains(4));
        assert!(range.contains(5));
        assert!(!range.contains(6));
    }
}
