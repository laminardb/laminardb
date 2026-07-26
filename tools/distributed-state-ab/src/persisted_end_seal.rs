use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::Path;

use distributed_state_ab::{validate_completed_trace, CompletedTraceV1, SealedPlanV1};

/// A single-use capability created only after the exact scheduled trace is file-synced.
pub(super) struct PersistedEndSeal {
    plan_sha256: String,
    trace_sha256: String,
    action_count: u32,
    scheduled_end_ns: u64,
}

pub(super) struct EndBinding {
    pub(super) trace_sha256: String,
    pub(super) action_count: u32,
    pub(super) scheduled_end_ns: u64,
}

pub(super) fn validate_persist_and_seal(
    path: &Path,
    completed: &CompletedTraceV1,
    plan: &SealedPlanV1,
) -> Result<PersistedEndSeal, String> {
    validate_completed_trace(completed, plan).map_err(|error| error.to_string())?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| format!("create artifact {}: {error}", path.display()))?;
    file.write_all(completed.canonical_bytes())
        .map_err(|error| format!("write artifact {}: {error}", path.display()))?;
    file.sync_all()
        .map_err(|error| format!("sync artifact {}: {error}", path.display()))?;
    Ok(PersistedEndSeal {
        plan_sha256: plan.sha256().to_owned(),
        trace_sha256: completed.sha256().to_owned(),
        action_count: completed.action_count(),
        scheduled_end_ns: completed.end_ns(),
    })
}

impl PersistedEndSeal {
    pub(super) fn consume_for(self, plan: &SealedPlanV1) -> Result<EndBinding, String> {
        if self.plan_sha256 != plan.sha256() {
            return Err("persisted end seal belongs to a different base plan".to_owned());
        }
        Ok(EndBinding {
            trace_sha256: self.trace_sha256,
            action_count: self.action_count,
            scheduled_end_ns: self.scheduled_end_ns,
        })
    }
}
