use crate::error::DbError;

/// Preserve stronger dispositions; otherwise a possibly mutated aggregate must recover.
pub(super) fn stateful_apply_outcome_unknown(
    op_name: &str,
    phase: &str,
    error: DbError,
) -> DbError {
    if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
        return error;
    }
    DbError::StatefulOperatorPartialApply(format!(
        "aggregate '{op_name}' {phase} failed after state application began; the apply outcome is unknown: {error}"
    ))
}
