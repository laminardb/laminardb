//! Canonical temporal-join planning contract.

use std::ops::ControlFlow;

use sqlparser::ast::{Statement, TableFactor, TableVersion, Visit, Visitor};

/// Hard parser limit for probes produced from one left row.
pub const MAX_TEMPORAL_PROBES_PER_ROW: usize = 256;

/// Hard parser limit for the absolute value of a probe horizon (365 days).
pub const MAX_TEMPORAL_PROBE_HORIZON_MS: i64 = 365 * 24 * 60 * 60 * 1_000;

/// Count every temporal table version in a statement, including nested queries.
#[must_use]
pub fn temporal_table_version_count(statement: &Statement) -> usize {
    #[derive(Default)]
    struct Counter(usize);

    impl Visitor for Counter {
        type Break = ();

        fn pre_visit_table_factor(
            &mut self,
            table_factor: &TableFactor,
        ) -> ControlFlow<Self::Break> {
            if matches!(
                table_factor,
                TableFactor::Table {
                    version: Some(TableVersion::ForSystemTimeAsOf(_)),
                    ..
                }
            ) {
                self.0 += 1;
            }
            ControlFlow::Continue(())
        }
    }

    let mut counter = Counter::default();
    let _ = statement.visit(&mut counter);
    counter.0
}

/// Temporal joins supported by the managed state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporalJoinKind {
    /// Emit only matching left rows.
    Inner,
    /// Emit every left row, null-extending misses.
    Left,
}

/// Validated target-time schedule for one temporal-join state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporalProbeSchedule {
    offsets_ms: Vec<i64>,
    multi_horizon: bool,
}

impl TemporalProbeSchedule {
    /// One lookup at the left row's event time.
    #[must_use]
    pub fn as_of() -> Self {
        Self {
            offsets_ms: vec![0],
            multi_horizon: false,
        }
    }

    /// Build a validated explicit schedule.
    ///
    /// # Errors
    /// Returns an error for an empty list, duplicate offsets, or parser-limit violations.
    pub fn list(offsets_ms: Vec<i64>) -> Result<Self, String> {
        if offsets_ms.is_empty() {
            return Err("temporal probe LIST requires at least one offset".into());
        }
        if offsets_ms.len() > MAX_TEMPORAL_PROBES_PER_ROW {
            return Err(format!(
                "temporal probe schedule exceeds the limit of {MAX_TEMPORAL_PROBES_PER_ROW} offsets per row"
            ));
        }
        for (index, offset) in offsets_ms.iter().enumerate() {
            validate_horizon(*offset)?;
            if offsets_ms[..index].contains(offset) {
                return Err(format!("temporal probe LIST repeats offset {offset}ms"));
            }
        }
        Ok(Self {
            offsets_ms,
            multi_horizon: true,
        })
    }

    /// Build a validated inclusive range schedule.
    ///
    /// # Errors
    /// Returns an error for invalid bounds, a non-positive step, a range that is not
    /// exactly divisible by its step, or parser-limit violations.
    pub fn range(start_ms: i64, end_ms: i64, step_ms: i64) -> Result<Self, String> {
        validate_horizon(start_ms)?;
        validate_horizon(end_ms)?;
        if start_ms > end_ms {
            return Err("temporal probe RANGE start must not exceed its end".into());
        }
        if step_ms <= 0 {
            return Err("temporal probe RANGE STEP must be positive".into());
        }
        let span = end_ms
            .checked_sub(start_ms)
            .ok_or_else(|| "temporal probe RANGE span overflows signed milliseconds".to_string())?;
        if span % step_ms != 0 {
            return Err(
                "temporal probe RANGE end must be reachable by whole STEP increments".into(),
            );
        }
        let count = span
            .checked_div(step_ms)
            .and_then(|steps| steps.checked_add(1))
            .ok_or_else(|| "temporal probe RANGE size overflows".to_string())?;
        let count = usize::try_from(count)
            .map_err(|_| "temporal probe RANGE size cannot be represented".to_string())?;
        if count > MAX_TEMPORAL_PROBES_PER_ROW {
            return Err(format!(
                "temporal probe schedule exceeds the limit of {MAX_TEMPORAL_PROBES_PER_ROW} offsets per row"
            ));
        }
        let mut offsets_ms = Vec::with_capacity(count);
        let mut offset = start_ms;
        for index in 0..count {
            offsets_ms.push(offset);
            if index + 1 < count {
                offset = offset.checked_add(step_ms).ok_or_else(|| {
                    "temporal probe RANGE expansion overflows milliseconds".to_string()
                })?;
            }
        }
        Ok(Self {
            offsets_ms,
            multi_horizon: true,
        })
    }

    /// Number of target-time probes produced per left row.
    #[must_use]
    pub fn len(&self) -> usize {
        self.offsets_ms.len()
    }

    /// Whether this schedule contains no probes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.offsets_ms.is_empty()
    }

    /// Deterministic offsets in milliseconds.
    #[must_use]
    pub fn offsets_ms(&self) -> &[i64] {
        &self.offsets_ms
    }

    /// Whether the syntax requested typed `offset_ms` and `probe_time` outputs.
    #[must_use]
    pub const fn is_multi_horizon(&self) -> bool {
        self.multi_horizon
    }
}

fn validate_horizon(offset_ms: i64) -> Result<(), String> {
    let magnitude = offset_ms
        .checked_abs()
        .ok_or_else(|| "temporal probe offset magnitude overflows".to_string())?;
    if magnitude > MAX_TEMPORAL_PROBE_HORIZON_MS {
        return Err(format!(
            "temporal probe offset exceeds the parser horizon limit of {MAX_TEMPORAL_PROBE_HORIZON_MS}ms"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use sqlparser::parser::Parser;

    #[test]
    fn schedules_preserve_list_order_and_expand_ranges() {
        let list = TemporalProbeSchedule::list(vec![5_000, -1_000, 0]).unwrap();
        assert_eq!(list.offsets_ms(), [5_000, -1_000, 0]);

        let range = TemporalProbeSchedule::range(-1_000, 1_000, 500).unwrap();
        assert_eq!(range.offsets_ms(), [-1_000, -500, 0, 500, 1_000]);
    }

    #[test]
    fn schedules_reject_ambiguous_or_excessive_expansion() {
        assert!(TemporalProbeSchedule::list(vec![0, 0]).is_err());
        assert!(TemporalProbeSchedule::range(0, 1_001, 1_000).is_err());
        assert!(TemporalProbeSchedule::range(0, 256_000, 1_000).is_err());
    }

    #[test]
    fn finds_temporal_versions_in_nested_queries() {
        let statements = Parser::parse_sql(
            &crate::parser::dialect::LaminarDialect::default(),
            "WITH q AS (SELECT * FROM l JOIN r FOR SYSTEM_TIME AS OF l.ts ON l.k = r.k) SELECT * FROM q",
        )
        .unwrap();
        assert_eq!(temporal_table_version_count(&statements[0]), 1);
    }
}
