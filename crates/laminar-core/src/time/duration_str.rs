//! Tiny duration-string parser shared by connector configs.
//!
//! Accepts `"5"` (bare seconds), `"100ms"`, `"5s"`, `"10m"`, `"2h"`,
//! `"1d"`. Whitespace is trimmed. Multi-unit strings (`"1h30m"`) are
//! not supported — config files use one unit at a time.

use std::time::Duration;

/// Parses a config duration string. Returns `None` on malformed input.
///
/// See the module docs for the accepted grammar.
#[must_use]
pub fn parse_duration_str(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Longest suffixes first so "ms" doesn't get eaten by "s".
    for (suffix, scale_secs) in [("ms", 0u64), ("s", 1), ("m", 60), ("h", 3600), ("d", 86400)] {
        if let Some(head) = s.strip_suffix(suffix) {
            let n: u64 = head.trim().parse().ok()?;
            return Some(if suffix == "ms" {
                Duration::from_millis(n)
            } else {
                Duration::from_secs(n.checked_mul(scale_secs)?)
            });
        }
    }

    // Bare number: seconds.
    s.parse::<u64>().ok().map(Duration::from_secs)
}

#[cfg(test)]
mod tests;
