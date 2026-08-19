//! Durable publication of this process's own recovery fault into shared cluster authority.

use std::sync::atomic::Ordering;

use laminar_core::cluster::control::{
    ClusterController, RecoveryFaultDisposition, RecoveryFaultReportOutcome,
};

use super::DECISION_IO_TIMEOUT;
use crate::LaminarDB;

pub(super) fn local_fault_disposition(db: &LaminarDB) -> RecoveryFaultDisposition {
    if db.terminal_pipeline_halt.load(Ordering::Acquire) {
        RecoveryFaultDisposition::Terminal
    } else {
        RecoveryFaultDisposition::Recoverable
    }
}

fn install_new_local_fault_request(
    controller: &ClusterController,
    pending: &std::sync::atomic::AtomicU64,
) -> Result<u64, String> {
    let request = controller.next_recovery_fault_request()?;
    pending.fetch_max(request.sequence(), Ordering::AcqRel);
    Ok(pending.load(Ordering::Acquire))
}

/// Queue one new local fault event, atomically superseding an older outstanding request. The
/// request remains latched until an authorized committed Release consumes it.
pub(crate) fn queue_local_fault(
    controller: &ClusterController,
    pending: &std::sync::atomic::AtomicU64,
) -> Result<(), String> {
    install_new_local_fault_request(controller, pending).map(|_| ())
}

fn retain_local_fault_request(
    controller: &ClusterController,
    pending: &std::sync::atomic::AtomicU64,
) -> Result<u64, String> {
    loop {
        let observed = pending.load(Ordering::Acquire);
        if observed != 0 {
            return Ok(observed);
        }
        let request = controller.next_recovery_fault_request()?.sequence();
        match pending.compare_exchange(0, request, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(request),
            Err(concurrent) if concurrent != 0 => return Ok(concurrent),
            Err(_) => {}
        }
    }
}

async fn persist_local_fault(
    controller: &ClusterController,
    raw_request: u64,
    disposition: RecoveryFaultDisposition,
) -> Result<RecoveryFaultReportOutcome, String> {
    let request = controller.recovery_fault_request(raw_request)?;
    let report = async {
        match disposition {
            RecoveryFaultDisposition::Recoverable => controller.report_fault(request).await,
            RecoveryFaultDisposition::Terminal => controller.report_terminal_fault(request).await,
        }
    };
    match tokio::time::timeout(DECISION_IO_TIMEOUT, report).await {
        Ok(Ok(outcome)) => {
            if outcome == RecoveryFaultReportOutcome::Active {
                tracing::warn!(
                    request_ordinal = raw_request,
                    "reported local fault for coordinated cluster recovery"
                );
            }
            Ok(outcome)
        }
        Ok(Err(error)) => {
            tracing::error!(request_ordinal = raw_request, %error, "could not persist local recovery fault");
            Err(error)
        }
        Err(_) => Err("local recovery fault publication timed out".into()),
    }
}

/// Publish the exact queued request without clearing its terminal-discovery latch.
pub(super) async fn flush_pending_local_fault(
    controller: &ClusterController,
    pending: &std::sync::atomic::AtomicU64,
    disposition: RecoveryFaultDisposition,
) -> Result<RecoveryFaultReportOutcome, String> {
    let raw_request = pending.load(Ordering::Acquire);
    if raw_request == 0 {
        return Ok(RecoveryFaultReportOutcome::AlreadyCleared);
    }
    persist_local_fault(controller, raw_request, disposition).await
}

/// Coalesce a duplicate notification into the outstanding request and make one bounded durable
/// publication attempt.
pub(crate) async fn request_local_fault(
    controller: &ClusterController,
    pending: &std::sync::atomic::AtomicU64,
) -> Result<u64, String> {
    let raw_request = retain_local_fault_request(controller, pending)?;
    match persist_local_fault(
        controller,
        raw_request,
        RecoveryFaultDisposition::Recoverable,
    )
    .await?
    {
        RecoveryFaultReportOutcome::Active => Ok(raw_request),
        RecoveryFaultReportOutcome::AlreadyCleared
        | RecoveryFaultReportOutcome::CoveredByNewerRequest => {
            let concurrent = pending.load(Ordering::Acquire);
            let fresh_request = if concurrent != 0 && concurrent != raw_request {
                concurrent
            } else {
                install_new_local_fault_request(controller, pending)?
            };
            match persist_local_fault(
                controller,
                fresh_request,
                RecoveryFaultDisposition::Recoverable,
            )
            .await?
            {
                RecoveryFaultReportOutcome::Active => Ok(fresh_request),
                RecoveryFaultReportOutcome::AlreadyCleared
                | RecoveryFaultReportOutcome::CoveredByNewerRequest => Err(format!(
                    "fresh recovery fault request {fresh_request} was settled before it became active"
                )),
                RecoveryFaultReportOutcome::TerminalFenceActive => Err(
                    "durable terminal pipeline fault already fences automatic recovery".into(),
                ),
            }
        }
        RecoveryFaultReportOutcome::TerminalFenceActive => {
            Err("durable terminal pipeline fault already fences automatic recovery".into())
        }
    }
}

pub(super) async fn request_fresh_local_fault(
    controller: &ClusterController,
    pending: &std::sync::atomic::AtomicU64,
    disposition: RecoveryFaultDisposition,
) -> Result<u64, String> {
    let raw_request = install_new_local_fault_request(controller, pending)?;
    match persist_local_fault(controller, raw_request, disposition).await? {
        RecoveryFaultReportOutcome::Active => Ok(raw_request),
        RecoveryFaultReportOutcome::AlreadyCleared
        | RecoveryFaultReportOutcome::CoveredByNewerRequest => Err(format!(
            "fresh recovery fault request {raw_request} was settled before it became active"
        )),
        RecoveryFaultReportOutcome::TerminalFenceActive
            if disposition == RecoveryFaultDisposition::Terminal =>
        {
            Ok(raw_request)
        }
        RecoveryFaultReportOutcome::TerminalFenceActive => {
            Err("durable terminal pipeline fault already fences automatic recovery".into())
        }
    }
}

/// Publish (or confirm) the permanent terminal disposition for this process's retained request.
pub(crate) async fn request_local_terminal_fault(
    controller: &ClusterController,
    pending: &std::sync::atomic::AtomicU64,
) -> Result<(u64, RecoveryFaultReportOutcome), String> {
    let raw_request = retain_local_fault_request(controller, pending)?;
    match persist_local_fault(controller, raw_request, RecoveryFaultDisposition::Terminal).await? {
        active @ (RecoveryFaultReportOutcome::Active
        | RecoveryFaultReportOutcome::TerminalFenceActive) => Ok((raw_request, active)),
        RecoveryFaultReportOutcome::AlreadyCleared
        | RecoveryFaultReportOutcome::CoveredByNewerRequest => {
            let fresh_request = install_new_local_fault_request(controller, pending)?;
            let outcome = persist_local_fault(
                controller,
                fresh_request,
                RecoveryFaultDisposition::Terminal,
            )
            .await?;
            Ok((fresh_request, outcome))
        }
    }
}
