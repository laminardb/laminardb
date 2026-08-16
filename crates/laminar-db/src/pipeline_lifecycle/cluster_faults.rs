#[cfg(feature = "cluster")]
use super::Arc;

/// Hand a compute fault to cluster recovery without retaining pipeline lifecycle ownership.
///
/// Recovery stops the pipeline by joining its watcher, so this path must never wait for an active
/// recovery announcement to clear. The request stays latched after publication until an authorized
/// committed Release consumes it; a failed round leaves it available for retry.
#[cfg(feature = "cluster")]
pub(super) async fn report_cluster_compute_fault(
    controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    pending: Arc<std::sync::atomic::AtomicU64>,
) {
    let Some(controller) = controller else {
        tracing::error!("cluster compute fault has no recovery controller; intake remains fenced");
        return;
    };
    if let Err(error) =
        crate::coordinated_recovery::request_local_fault(&controller, &pending).await
    {
        tracing::warn!(%error, "cluster compute fault queued for monitor retry");
    }
}

/// Persist the non-recoverable disposition before allowing a halted generation to disappear.
#[cfg(feature = "cluster")]
pub(crate) async fn report_cluster_terminal_halt(
    controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    pending: Arc<std::sync::atomic::AtomicU64>,
) {
    let Some(controller) = controller else {
        tracing::error!(
            "cluster terminal pipeline halt has no recovery controller; durable publication remains pending"
        );
        return std::future::pending::<()>().await;
    };
    let mut backoff = std::time::Duration::from_millis(25);
    let mut authority_lost = false;
    loop {
        if !authority_lost && controller.process_lease_is_live() {
            match crate::coordinated_recovery::request_local_terminal_fault(&controller, &pending)
                .await
            {
                Ok((
                    _,
                    laminar_core::cluster::control::RecoveryFaultReportOutcome::Active
                    | laminar_core::cluster::control::RecoveryFaultReportOutcome::TerminalFenceActive,
                )) => return,
                Ok((_, outcome)) => {
                    tracing::warn!(
                        ?outcome,
                        "terminal pipeline fault request was not yet admitted"
                    );
                }
                Err(error) => {
                    tracing::error!(%error, "durable terminal pipeline halt publication failed; retrying while shutdown remains fenced");
                }
            }
        }

        if authority_lost || !controller.process_lease_is_live() {
            authority_lost = true;
            let durable = tokio::time::timeout(
                std::time::Duration::from_secs(15),
                controller.read_recovery_fault_inventory(),
            )
            .await;
            if matches!(durable, Ok(Ok(inventory)) if inventory.has_terminal_fault()) {
                return;
            }
            tracing::error!(
                "terminal pipeline fault lost process authority before durable confirmation; \
                 waiting for a namespace-wide terminal marker while shutdown remains fenced"
            );
        }

        tokio::time::sleep(backoff).await;
        backoff = backoff
            .saturating_mul(2)
            .min(std::time::Duration::from_secs(1));
    }
}

/// Queue only a fault that won lifecycle ownership before this runtime generation was cancelled.
#[cfg(feature = "cluster")]
pub(super) fn queue_owned_cluster_compute_fault(
    controller: &laminar_core::cluster::control::ClusterController,
    pending: &std::sync::atomic::AtomicU64,
    owns_fault_state: bool,
    runtime_shutdown: &tokio_util::sync::CancellationToken,
) -> Result<bool, String> {
    if !owns_fault_state || runtime_shutdown.is_cancelled() {
        return Ok(false);
    }
    crate::coordinated_recovery::queue_local_fault(controller, pending)?;
    Ok(true)
}
