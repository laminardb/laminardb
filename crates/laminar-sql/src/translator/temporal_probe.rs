//! Configuration types for the temporal probe join operator.
//!
//! A temporal probe join matches each left event against the right stream at
//! multiple fixed time offsets. For each left row, it produces N output rows
//! (one per offset) with the ASOF-matched right value at `event_time + offset`.

use std::fmt;

/// Offset specification: either a uniform range or an explicit list.
#[derive(Debug, Clone)]
pub enum ProbeOffsetSpec {
    /// `RANGE FROM <start> TO <end> STEP <step>` (all in ms).
    Range {
        /// Inclusive start offset in milliseconds.
        start_ms: i64,
        /// Inclusive end offset in milliseconds.
        end_ms: i64,
        /// Step size in milliseconds.
        step_ms: i64,
    },
    /// `LIST (<ms>, ...)`.
    List(Vec<i64>),
}

impl ProbeOffsetSpec {
    /// Expand into a sorted, deduplicated list of offsets in milliseconds.
    #[must_use]
    pub fn expand(&self) -> Vec<i64> {
        let mut offsets = match self {
            ProbeOffsetSpec::Range {
                start_ms,
                end_ms,
                step_ms,
            } => {
                if *step_ms <= 0 || *start_ms > *end_ms {
                    return vec![*start_ms];
                }
                let mut v = Vec::new();
                let mut cur = *start_ms;
                loop {
                    v.push(cur);
                    match cur.checked_add(*step_ms) {
                        Some(next) if next <= *end_ms => cur = next,
                        _ => break,
                    }
                }
                v
            }
            ProbeOffsetSpec::List(list) => list.clone(),
        };
        offsets.sort_unstable();
        offsets.dedup();
        offsets
    }
}

/// Configuration for the temporal probe join operator.
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub struct TemporalProbeConfig {
    pub left_table: String,
    pub right_table: String,
    pub left_alias: Option<String>,
    pub right_alias: Option<String>,
    pub key_column: String,
    pub left_time_column: String,
    pub right_time_column: String,
    /// Sorted, deduplicated offsets in milliseconds.
    pub expanded_offsets_ms: Vec<i64>,
    /// Probe pseudo-table alias (the `AS p` in the SQL).
    pub probe_alias: String,
}

impl TemporalProbeConfig {
    /// Create a new config, expanding offsets immediately.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        left_table: String,
        right_table: String,
        left_alias: Option<String>,
        right_alias: Option<String>,
        key_column: String,
        left_time_column: String,
        right_time_column: String,
        offsets: &ProbeOffsetSpec,
        probe_alias: String,
    ) -> Self {
        let expanded_offsets_ms = offsets.expand();
        Self {
            left_table,
            right_table,
            left_alias,
            right_alias,
            key_column,
            left_time_column,
            right_time_column,
            expanded_offsets_ms,
            probe_alias,
        }
    }

    /// Maximum offset in milliseconds.
    #[must_use]
    pub fn max_offset_ms(&self) -> i64 {
        self.expanded_offsets_ms.iter().copied().max().unwrap_or(0)
    }

    /// Minimum offset in milliseconds. Negative means look-back.
    #[must_use]
    pub fn min_offset_ms(&self) -> i64 {
        self.expanded_offsets_ms.iter().copied().min().unwrap_or(0)
    }
}

impl fmt::Display for TemporalProbeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TemporalProbe({} × {} on {} offsets={})",
            self.left_table,
            self.right_table,
            self.key_column,
            self.expanded_offsets_ms.len()
        )
    }
}

/// Parse a duration string like `0s`, `5s`, `100ms`, `1m`, `-5s` into milliseconds.
///
/// Supported suffixes: `us` (microseconds), `ms` (milliseconds), `s` (seconds), `m` (minutes).
#[must_use]
pub fn parse_interval_to_ms(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let (negative, s) = if let Some(rest) = s.strip_prefix('-') {
        (true, rest)
    } else {
        (false, s)
    };

    let (num_str, unit) = if let Some(n) = s.strip_suffix("us") {
        (n, "us")
    } else if let Some(n) = s.strip_suffix("ms") {
        (n, "ms")
    } else if let Some(n) = s.strip_suffix('s') {
        (n, "s")
    } else if let Some(n) = s.strip_suffix('m') {
        (n, "m")
    } else {
        // Try as raw milliseconds
        return s.parse::<i64>().ok().map(|v| if negative { -v } else { v });
    };

    let num: i64 = num_str.parse().ok()?;
    let ms = match unit {
        "us" => num / 1000, // truncate sub-ms
        "ms" => num,
        "s" => num.checked_mul(1000)?,
        "m" => num.checked_mul(60_000)?,
        _ => return None,
    };

    Some(if negative { -ms } else { ms })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_interval_seconds() {
        assert_eq!(parse_interval_to_ms("0s"), Some(0));
        assert_eq!(parse_interval_to_ms("1s"), Some(1000));
        assert_eq!(parse_interval_to_ms("60s"), Some(60_000));
        assert_eq!(parse_interval_to_ms("-5s"), Some(-5000));
    }

    #[test]
    fn test_parse_interval_millis() {
        assert_eq!(parse_interval_to_ms("100ms"), Some(100));
        assert_eq!(parse_interval_to_ms("0ms"), Some(0));
    }

    #[test]
    fn test_parse_interval_minutes() {
        assert_eq!(parse_interval_to_ms("1m"), Some(60_000));
        assert_eq!(parse_interval_to_ms("5m"), Some(300_000));
    }

    #[test]
    fn test_parse_interval_micros() {
        assert_eq!(parse_interval_to_ms("5000us"), Some(5));
        assert_eq!(parse_interval_to_ms("500us"), Some(0)); // truncated
    }

    #[test]
    fn test_range_expand() {
        let spec = ProbeOffsetSpec::Range {
            start_ms: 0,
            end_ms: 5000,
            step_ms: 1000,
        };
        assert_eq!(spec.expand(), vec![0, 1000, 2000, 3000, 4000, 5000]);
    }

    #[test]
    fn test_range_expand_with_negative() {
        let spec = ProbeOffsetSpec::Range {
            start_ms: -2000,
            end_ms: 2000,
            step_ms: 1000,
        };
        assert_eq!(spec.expand(), vec![-2000, -1000, 0, 1000, 2000]);
    }

    #[test]
    fn test_list_expand_dedup_sort() {
        let spec = ProbeOffsetSpec::List(vec![5000, 1000, 0, 1000, -5000]);
        assert_eq!(spec.expand(), vec![-5000, 0, 1000, 5000]);
    }

    #[test]
    fn test_config_max_min_offset() {
        let config = TemporalProbeConfig::new(
            "trades".into(),
            "prices".into(),
            None,
            None,
            "symbol".into(),
            "ts".into(),
            "ts".into(),
            &ProbeOffsetSpec::List(vec![-5000, 0, 1000, 5000, 30_000, 60_000]),
            "p".into(),
        );
        assert_eq!(config.max_offset_ms(), 60_000);
        assert_eq!(config.min_offset_ms(), -5000);
    }
}
