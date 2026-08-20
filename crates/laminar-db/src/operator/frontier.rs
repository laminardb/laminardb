//! Recovery-aware cluster frontier normalization.

use crate::operator_graph::InputFrontier;

/// Keeps restored authority monotonic while the upstream observation catches up.
/// Idle-channel revival also inherits the effective frontier already applied by the operator.
pub(super) fn normalize_restored_local_frontier(
    input: InputFrontier,
    installed: InputFrontier,
    revival_floor: Option<i64>,
) -> InputFrontier {
    // RECOVERY: preserve a literal uninitialized sentinel so the owning operator rejects it.
    if input.watermark == Some(i64::MIN) {
        return input;
    }
    let floor = if input.idle || !installed.idle {
        installed.watermark
    } else {
        max_watermark(installed.watermark, revival_floor)
    };
    InputFrontier {
        watermark: max_watermark(input.watermark, floor),
        ..input
    }
}

fn max_watermark(current: Option<i64>, floor: Option<i64>) -> Option<i64> {
    match (current, floor) {
        (Some(current), Some(floor)) => Some(current.max(floor)),
        (None, floor) => floor,
        (current, None) => current,
    }
}
