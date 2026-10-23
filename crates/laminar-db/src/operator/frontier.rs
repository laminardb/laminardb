//! Recovery-aware cluster frontier normalization.

use crate::operator_graph::InputFrontier;

/// Keeps restored authority monotonic while the upstream observation catches up.
/// Idle observations and revival inherit the effective frontier already applied by the operator.
pub(crate) fn normalize_restored_local_frontier(
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

/// Like [`normalize_restored_local_frontier`] for a channel whose peers merge idle frontiers by
/// maximum. An idle observation delivers no rows, so it inherits only this node's own installed
/// frontier: raising it to the cut applied from peers would echo another reader's frontier and let
/// a peer close past rows that reader has not delivered yet.
pub(crate) fn normalize_local_frontier_without_peer_echo(
    input: InputFrontier,
    installed: InputFrontier,
    effective_floor: Option<i64>,
) -> InputFrontier {
    let effective_floor = if input.idle { None } else { effective_floor };
    normalize_restored_local_frontier(input, installed, effective_floor)
}

/// The ceiling an all-idle merge may close to: this node's idle graph input, which the graph caps
/// at the committed cut. An active input imposes none.
pub(crate) fn idle_close_ceiling(input: InputFrontier) -> Option<i64> {
    input.idle.then(|| input.watermark.unwrap_or(i64::MIN))
}

/// Bound an all-idle peer merge. Idle channels never echo the applied cut, so the merge can trail
/// it; hold there instead of regressing. A reader idle at its own higher watermark must not close
/// past `idle_ceiling`, this node's committed cut, where a second idle reader revives.
pub(crate) fn hold_all_idle_merge(
    mut merged: InputFrontier,
    applied: Option<i64>,
    idle_ceiling: Option<i64>,
) -> InputFrontier {
    if merged.idle {
        let closable = match idle_ceiling {
            Some(ceiling) => merged.watermark.map(|watermark| watermark.min(ceiling)),
            None => merged.watermark,
        };
        merged.watermark = max_watermark(closable, applied);
    }
    merged
}

/// Whether a checkpointed effective frontier is an all-idle close that the live merge held at or
/// below `merged`, the raw peer merge recomputed at restore. The committed-cut ceiling that capped
/// it is not part of the checkpoint, so restore cannot recompute the held value exactly.
pub(crate) fn is_held_all_idle_frontier(saved: InputFrontier, merged: InputFrontier) -> bool {
    merged.idle && saved.idle && saved.watermark <= merged.watermark
}

/// Whether a checkpointed effective frontier matches `merged` exactly or as a held all-idle close.
pub(crate) fn restored_frontier_matches(saved: InputFrontier, merged: InputFrontier) -> bool {
    saved == merged || is_held_all_idle_frontier(saved, merged)
}

pub(crate) fn max_watermark(current: Option<i64>, floor: Option<i64>) -> Option<i64> {
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
    fn idle_observation_without_peer_echo_keeps_its_own_installed_frontier() {
        let installed = InputFrontier {
            watermark: Some(80),
            idle: true,
        };
        let idle = InputFrontier {
            watermark: Some(60),
            idle: true,
        };
        let active = InputFrontier {
            watermark: Some(60),
            idle: false,
        };

        assert_eq!(
            normalize_local_frontier_without_peer_echo(idle, installed, Some(100)),
            InputFrontier {
                watermark: Some(80),
                idle: true,
            }
        );
        assert_eq!(
            normalize_local_frontier_without_peer_echo(active, installed, Some(100)),
            InputFrontier {
                watermark: Some(100),
                idle: false,
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
