//! Transaction visibility: `pg_snapshot` values, 32-bit stream xids widened to `xid8`, and the
//! guards a chunk snapshot must pass before its rows can be merged with the stream.

use std::collections::BTreeSet;
use std::fmt;

use crate::error::ConnectorError;

const XID_SPACE: u64 = 1 << 32;
const HALF_XID_SPACE: u64 = 1 << 31;

/// A `pg_snapshot`: transactions below `xmin` are finished, those at or above `xmax` had not
/// finished, and `xip` lists the ones in between that were still running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PgSnapshot {
    xmin: u64,
    xmax: u64,
    xip: Vec<u64>,
}

impl PgSnapshot {
    /// Parse the `xmin:xmax:xip,…` text form of `pg_snapshot`.
    ///
    /// # Errors
    /// Returns an error for malformed text or running xids outside `[xmin, xmax)`.
    pub(crate) fn parse(text: &str) -> Result<Self, ConnectorError> {
        let invalid = || ConnectorError::ReadError(format!("invalid pg_snapshot '{text}'"));
        let mut parts = text.splitn(3, ':');
        let number = |part: Option<&str>| {
            part.and_then(|part| part.parse::<u64>().ok())
                .ok_or_else(invalid)
        };
        let xmin = number(parts.next())?;
        let xmax = number(parts.next())?;
        let mut xip = match parts.next() {
            Some("") => Vec::new(),
            Some(list) => list
                .split(',')
                .map(|xid| xid.parse::<u64>().map_err(|_| invalid()))
                .collect::<Result<Vec<_>, _>>()?,
            None => return Err(invalid()),
        };
        xip.sort_unstable();
        xip.dedup();
        let in_range = xip.iter().all(|xid| (xmin..xmax).contains(xid));
        if xmin > xmax || !in_range {
            return Err(invalid());
        }
        Ok(Self { xmin, xmax, xip })
    }

    /// Whether a transaction that committed is visible to this snapshot.
    pub(crate) fn sees(&self, xid: u64) -> bool {
        xid < self.xmin || (xid < self.xmax && self.xip.binary_search(&xid).is_err())
    }

    /// The full transaction id of a 32-bit `pgoutput` xid, taken as the one within 2³¹ of this
    /// snapshot's `xmax`. Wraparound protection keeps every live xid within that distance.
    ///
    /// # Errors
    /// Returns an error when the widened xid would be negative or overflow.
    pub(crate) fn widen(&self, xid: u32) -> Result<u64, ConnectorError> {
        let reference = self.xmax;
        let low = u32::try_from(reference % XID_SPACE).unwrap_or_default();
        let forward = u64::from(xid.wrapping_sub(low));
        let widened = if forward < HALF_XID_SPACE {
            reference.checked_add(forward)
        } else {
            reference.checked_sub(XID_SPACE - forward)
        };
        widened.ok_or_else(|| {
            ConnectorError::ReadError(format!(
                "PostgreSQL xid {xid} cannot be placed near the snapshot xmax {reference}"
            ))
        })
    }

    /// Whether this snapshot sees every transaction `floor` sees. The floor comes from the slot's
    /// snapshot builder, which counts a transaction as committed once it decoded its commit
    /// record; the procarray keeps it running until the commit returns, which a synchronous
    /// standby can delay without bound.
    pub(crate) fn covers(&self, floor: &Self) -> bool {
        if self.xip.iter().any(|&xid| floor.sees(xid)) {
            return false;
        }
        let (low, high) = (self.xmax, floor.xmax);
        if low >= high {
            return true;
        }
        if low < floor.xmin {
            return false;
        }
        let running = floor
            .xip
            .iter()
            .filter(|&&xid| (low..high).contains(&xid))
            .count();
        u64::try_from(running).is_ok_and(|running| running == high - low)
    }

    /// Up to `limit` transactions `floor` sees that this snapshot does not: what holds a copy
    /// behind the floor. Every candidate the floor does not see is in its bounded `xip`, so the
    /// scan ends after at most `xip.len() + limit` candidates.
    pub(crate) fn behind(&self, floor: &Self, limit: usize) -> Vec<u64> {
        let not_started = self.xmax..floor.xmax.max(self.xmax);
        self.xip
            .iter()
            .copied()
            .chain(not_started)
            .filter(|&xid| floor.sees(xid))
            .take(limit)
            .collect()
    }

    /// Drop every pending xid this snapshot sees; `true` when none remain.
    pub(crate) fn settle(&self, pending: &mut BTreeSet<u64>) -> bool {
        pending.retain(|&xid| !self.sees(xid));
        pending.is_empty()
    }
}

impl fmt::Display for PgSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:", self.xmin, self.xmax)?;
        for (index, xid) in self.xip.iter().enumerate() {
            if index > 0 {
                f.write_str(",")?;
            }
            write!(f, "{xid}")?;
        }
        Ok(())
    }
}
