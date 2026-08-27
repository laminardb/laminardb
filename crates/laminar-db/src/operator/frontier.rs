//! Recovery-aware cluster frontier normalization.

use crate::operator_graph::InputFrontier;

/// Keeps restored authority monotonic while the upstream observation catches up.
/// Idle observations and revival inherit the effective frontier already applied by the operator.
pub(super) fn normalize_restored_local_frontier(
    input: InputFrontier,
    installed: InputFrontier,
    effective_floor: Option<i64>,
) -> InputFrontier {
    // RECOVERY: preserve a literal uninitialized sentinel so the owning operator rejects it.
    if input.watermark == Some(i64::MIN) {
        return input;
    }
    // RECOVERY: an idle channel is excluded from an active minimum and may therefore trail the
    // operator's committed effective cut. Retaining that lower value after restore can regress an
    // all-idle merge when a pending send completes. Advancing an idle observation to the effective
    // cut is safe because revival was already required to inherit the same floor.
    let floor = max_watermark(installed.watermark, effective_floor);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_observation_inherits_the_restored_effective_cut() {
        let installed = InputFrontier {
            watermark: Some(80),
            idle: true,
        };
        let observed = InputFrontier {
            watermark: Some(90),
            idle: true,
        };

        assert_eq!(
            normalize_restored_local_frontier(observed, installed, Some(100)),
            InputFrontier {
                watermark: Some(100),
                idle: true,
            }
        );
    }

    #[test]
    fn uninitialized_sentinel_remains_rejectable() {
        let sentinel = InputFrontier {
            watermark: Some(i64::MIN),
            idle: false,
        };
        assert_eq!(
            normalize_restored_local_frontier(
                sentinel,
                InputFrontier {
                    watermark: Some(80),
                    idle: true,
                },
                Some(100),
            ),
            sentinel
        );
    }
}
