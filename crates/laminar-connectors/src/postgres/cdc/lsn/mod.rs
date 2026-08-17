//! `PostgreSQL` Log Sequence Number (LSN) type.
//!
//! An LSN is a 64-bit integer representing a byte position in the WAL stream.
//! `PostgreSQL` displays LSNs in the format `X/Y` where X is the upper 32 bits
//! and Y is the lower 32 bits, both in hexadecimal.

use std::fmt;
use std::str::FromStr;

/// A `PostgreSQL` Log Sequence Number (LSN).
///
/// Represents a byte offset in the write-ahead log. Used to track
/// replication progress and checkpoint positions.
///
/// # Format
///
/// LSNs are displayed as `X/YYYYYYYY` where X and Y are hex values.
/// For example: `0/1234ABCD`, `1/0`, `FF/FFFFFFFF`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(u64);

impl Lsn {
    /// The zero LSN, representing the start of the WAL.
    pub const ZERO: Lsn = Lsn(0);

    /// The maximum possible LSN.
    pub const MAX: Lsn = Lsn(u64::MAX);

    /// Creates a new LSN from a raw 64-bit value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Lsn(value)
    }

    /// Returns the raw 64-bit value.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns the upper 32 bits (segment number).
    #[must_use]
    pub const fn segment(self) -> u32 {
        let bytes = self.0.to_be_bytes();
        u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
    }

    /// Returns the lower 32 bits (offset within segment).
    #[must_use]
    pub const fn offset(self) -> u32 {
        let bytes = self.0.to_be_bytes();
        u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]])
    }

    /// Returns the byte difference between two LSNs.
    ///
    /// Returns 0 if `other` is ahead of `self`.
    #[must_use]
    pub const fn diff(self, other: Lsn) -> u64 {
        self.0.saturating_sub(other.0)
    }

    /// Advances the LSN by the given number of bytes.
    #[must_use]
    pub const fn advance(self, bytes: u64) -> Lsn {
        Lsn(self.0.saturating_add(bytes))
    }

    /// Returns `true` if this is the zero LSN.
    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.segment(), self.offset())
    }
}

impl FromStr for Lsn {
    type Err = LsnParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (high, low) = s
            .split_once('/')
            .ok_or_else(|| LsnParseError::InvalidFormat(s.to_string()))?;

        let high = u32::from_str_radix(high, 16)
            .map_err(|_| LsnParseError::InvalidHex(high.to_string()))?;
        let low =
            u32::from_str_radix(low, 16).map_err(|_| LsnParseError::InvalidHex(low.to_string()))?;

        Ok(Lsn((u64::from(high) << 32) | u64::from(low)))
    }
}

impl From<u64> for Lsn {
    fn from(value: u64) -> Self {
        Lsn(value)
    }
}

impl From<Lsn> for u64 {
    fn from(lsn: Lsn) -> Self {
        lsn.0
    }
}

/// Errors that can occur when parsing an LSN string.
#[derive(Debug, Clone, thiserror::Error)]
pub enum LsnParseError {
    /// The string does not contain the expected `X/Y` format.
    #[error("invalid LSN format (expected X/Y): {0}")]
    InvalidFormat(String),

    /// A hex component could not be parsed.
    #[error("invalid hex in LSN: {0}")]
    InvalidHex(String),
}

#[cfg(test)]
mod tests;
