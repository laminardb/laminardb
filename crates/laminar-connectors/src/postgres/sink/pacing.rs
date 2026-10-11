//! Flush-window and statement sizing from the target's measured apply rate.
//!
//! Every statement runs under the session `statement_timeout`, and every flush under the engine's
//! write deadline. Sizing both from how fast the target applied recent statements keeps a slow
//! target from receiving a window or statement it cannot finish in time, which would fail every
//! replay of the same backlog.

use std::time::Duration;

/// Rows in a statement of a kind not yet measured: a target applying one row per 100 ms still
/// finishes it inside a 30 s statement timeout.
const UNMEASURED_STATEMENT_ROWS: usize = 256;

/// Shortest elapsed time trusted for a rate, so timer granularity cannot inflate it.
const MIN_MEASURED_ELAPSED: Duration = Duration::from_millis(1);

/// A statement the sink issues inside its flush transaction.
#[derive(Clone, Copy, Debug)]
pub(super) enum Statement {
    Copy,
    Upsert,
    Delete,
}

/// Rows applied by one statement and the time it took.
#[derive(Clone, Copy, Debug)]
struct Measured {
    rows: usize,
    elapsed: Duration,
}

impl Measured {
    fn rows_within(self, budget: Duration) -> usize {
        let rows = u128::from(u64::try_from(self.rows).unwrap_or(u64::MAX));
        let expected = rows.saturating_mul(budget.as_micros()) / self.elapsed.as_micros().max(1);
        usize::try_from(expected).unwrap_or(usize::MAX)
    }
}

/// The last measured statement of each kind. Kinds are kept apart because their costs differ:
/// target triggers or indexes can make an upsert far slower than a delete of the same key.
#[derive(Debug, Default)]
pub(super) struct ApplyPacing {
    copy: Option<Measured>,
    upsert: Option<Measured>,
    delete: Option<Measured>,
}

impl ApplyPacing {
    fn measured(&self, statement: Statement) -> Option<Measured> {
        match statement {
            Statement::Copy => self.copy,
            Statement::Upsert => self.upsert,
            Statement::Delete => self.delete,
        }
    }

    pub(super) fn record(&mut self, statement: Statement, rows: usize, elapsed: Duration) {
        let measured = Some(Measured {
            rows,
            elapsed: elapsed.max(MIN_MEASURED_ELAPSED),
        });
        match statement {
            Statement::Copy => self.copy = measured,
            Statement::Upsert => self.upsert = measured,
            Statement::Delete => self.delete = measured,
        }
    }

    /// Rows for the next statement of `remaining`: what the target is expected to apply in a
    /// quarter of the statement timeout, and at least one.
    pub(super) fn statement_rows(
        &self,
        statement: Statement,
        remaining: usize,
        statement_timeout: Duration,
    ) -> usize {
        self.measured(statement)
            .map_or(UNMEASURED_STATEMENT_ROWS, |measured| {
                measured.rows_within(statement_timeout / 4)
            })
            .clamp(1, remaining.max(1))
    }

    /// Whether the buffered window must be flushed before `incoming` rows join it: together they
    /// would take the slowest measured statement kind more than half the statement timeout.
    /// Until a statement is measured every window holds one batch. An empty window always admits
    /// the batch, which may hold a whole source transaction and is never split.
    pub(super) fn window_full(
        &self,
        buffered: usize,
        incoming: usize,
        statement_timeout: Duration,
    ) -> bool {
        if buffered == 0 {
            return false;
        }
        let limit = [self.copy, self.upsert, self.delete]
            .into_iter()
            .flatten()
            .map(|measured| measured.rows_within(statement_timeout / 2))
            .min()
            .unwrap_or(0);
        buffered.saturating_add(incoming) > limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(30);

    #[test]
    fn unmeasured_statements_start_small_and_windows_hold_one_batch() {
        let pacing = ApplyPacing::default();
        assert_eq!(
            pacing.statement_rows(Statement::Upsert, 10_000, TIMEOUT),
            UNMEASURED_STATEMENT_ROWS
        );
        assert_eq!(pacing.statement_rows(Statement::Upsert, 10, TIMEOUT), 10);
        assert!(!pacing.window_full(0, 1_000_000, TIMEOUT));
        assert!(pacing.window_full(1, 1, TIMEOUT));
    }

    #[test]
    fn statements_take_a_quarter_and_windows_half_of_the_timeout() {
        let mut pacing = ApplyPacing::default();
        pacing.record(Statement::Upsert, 100, Duration::from_secs(1));
        assert_eq!(
            pacing.statement_rows(Statement::Upsert, 10_000, TIMEOUT),
            750
        );
        assert_eq!(pacing.statement_rows(Statement::Upsert, 300, TIMEOUT), 300);
        assert!(!pacing.window_full(1_000, 500, TIMEOUT));
        assert!(pacing.window_full(1_000, 501, TIMEOUT));
        assert!(!pacing.window_full(0, 100_000, TIMEOUT));
    }

    #[test]
    fn each_statement_kind_is_sized_by_its_own_rate_and_windows_by_the_slowest() {
        let mut pacing = ApplyPacing::default();
        pacing.record(Statement::Upsert, 100, Duration::from_secs(1));
        pacing.record(Statement::Delete, 100_000, Duration::from_secs(1));
        assert_eq!(
            pacing.statement_rows(Statement::Upsert, 1 << 20, TIMEOUT),
            750
        );
        assert_eq!(
            pacing.statement_rows(Statement::Delete, 1 << 20, TIMEOUT),
            750_000
        );
        assert!(pacing.window_full(1_000, 501, TIMEOUT));
    }

    #[test]
    fn instant_and_stalled_statements_stay_bounded() {
        let mut pacing = ApplyPacing::default();
        pacing.record(Statement::Copy, 10, Duration::ZERO);
        assert_eq!(
            pacing.statement_rows(Statement::Copy, usize::MAX, TIMEOUT),
            75_000
        );
        pacing.record(Statement::Copy, 1, Duration::from_secs(3_600));
        assert_eq!(pacing.statement_rows(Statement::Copy, 50, TIMEOUT), 1);
        assert!(pacing.window_full(1, 1, TIMEOUT));
        pacing.record(Statement::Copy, usize::MAX, Duration::from_secs(1));
        assert!(!pacing.window_full(usize::MAX, usize::MAX, TIMEOUT));
    }
}
