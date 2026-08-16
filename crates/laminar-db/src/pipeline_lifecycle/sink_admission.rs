use super::{
    admit_sink_contract, required_recovery_scope, ConnectorCancellationPolicy, DbError,
    DeliveryGuarantee, PreparedSink, SinkAdmissionContext, SinkConnector, SinkContract,
    EXACT_SINK_PROTOCOL,
};

/// Resolve and validate a sink before any external I/O. Keeping this boundary separate from the
/// bounded open stage makes it impossible for one connector to become active before every sink is
/// known to be admissible.
pub(super) fn admit_sink(
    sink: &dyn SinkConnector,
    context: SinkAdmissionContext<'_>,
) -> Result<(SinkContract, Option<u64>), DbError> {
    let SinkAdmissionContext {
        config,
        name,
        input,
        delivery,
        runtime,
        carries_changelog,
        checkpointing_enabled,
        checkpoint_storage_scope,
    } = context;
    let contract = sink.contract(config).map_err(|e| {
        DbError::Config(format!(
            "sink '{name}' (type '{}') has an invalid contract: {e}",
            config.connector_type()
        ))
    })?;

    admit_sink_contract(contract, delivery, runtime, carries_changelog).map_err(|reason| {
        let detail = format!(
            "sink '{name}' is not admissible in {runtime:?} mode with {delivery} delivery: \
             {reason} (contract: {contract:?})"
        );
        if carries_changelog && !contract.accepts_full_changelog() {
            DbError::MaterializedView(format!(
                "[LDB-1300] {detail}. Route '{input}' to a FullChangelog sink or disable \
                 incremental emission."
            ))
        } else {
            DbError::Config(format!("[LDB-5035] {detail}"))
        }
    })?;

    if delivery == DeliveryGuarantee::ExactlyOnce {
        if !checkpointing_enabled {
            return Err(DbError::Config(format!(
                "[LDB-5035] sink '{name}' cannot run exactly-once without checkpointing"
            )));
        }
        let required_scope = required_recovery_scope(runtime);
        if !checkpoint_storage_scope.satisfies(required_scope) {
            return Err(DbError::Config(format!(
                "[LDB-5035] sink '{name}' cannot run exactly-once: committed checkpoints require \
                 {required_scope:?} storage, but the configured checkpoint store is \
                 {checkpoint_storage_scope:?}"
            )));
        }
        if sink.as_coordinated_committer().is_none() {
            return Err(DbError::Config(format!(
                "[LDB-5035] sink '{name}' claims {contract:?} but does not implement the complete \
                 coordinated exact protocol: {EXACT_SINK_PROTOCOL}"
            )));
        }
    } else if sink.as_coordinated_committer().is_some() {
        return Err(DbError::Config(format!(
            "[LDB-5035] sink '{name}' exposes a coordinated committer outside global \
             exactly-once delivery"
        )));
    }

    let configured_timeout = config
        .get_parsed::<u64>("sink.write.timeout.ms")
        .map_err(|e| {
            DbError::Connector(format!(
                "Invalid 'sink.write.timeout.ms' for sink '{name}': {e}"
            ))
        })?;
    if configured_timeout == Some(0) {
        return Err(DbError::Connector(format!(
            "sink '{name}': sink.write.timeout.ms must be > 0"
        )));
    }

    Ok((contract, configured_timeout))
}

pub(super) async fn close_opened_sinks(
    sinks: &mut [PreparedSink],
    cleanup_timeout: std::time::Duration,
    #[cfg(feature = "cluster")] process_authority: Option<
        &laminar_core::cluster::control::ClusterController,
    >,
) {
    let cleanup_deadline = tokio::time::Instant::now() + cleanup_timeout;
    futures::future::join_all(sinks.iter_mut().rev().map(|prepared| {
        close_opened_sink(
            prepared,
            cleanup_deadline,
            #[cfg(feature = "cluster")]
            process_authority,
        )
    }))
    .await;
}

pub(super) async fn close_opened_sink(
    prepared: &mut PreparedSink,
    cleanup_deadline: tokio::time::Instant,
    #[cfg(feature = "cluster")] process_authority: Option<
        &laminar_core::cluster::control::ClusterController,
    >,
) {
    #[cfg(feature = "cluster")]
    if process_authority.is_some_and(|controller| !controller.process_lease_is_live()) {
        return;
    }
    if tokio::time::Instant::now() >= cleanup_deadline {
        tracing::warn!(
            sink = %prepared.name,
            "sink close skipped after the pipeline-startup cleanup deadline"
        );
        return;
    }

    let mut close = std::pin::pin!(prepared.connector.close());
    #[cfg(feature = "cluster")]
    let close_result = if let Some(controller) = process_authority {
        tokio::select! {
            biased;
            () = controller.wait_for_process_lease_loss() => return,
            result = tokio::time::timeout_at(cleanup_deadline, close.as_mut()) => result,
        }
    } else {
        tokio::time::timeout_at(cleanup_deadline, close.as_mut()).await
    };
    #[cfg(not(feature = "cluster"))]
    let close_result = tokio::time::timeout_at(cleanup_deadline, close.as_mut()).await;
    match close_result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(
                sink = %prepared.name,
                %error,
                "sink close failed while rolling back pipeline startup"
            );
        }
        Err(_) => {
            tracing::warn!(
                sink = %prepared.name,
                "sink close exceeded the shared pipeline-startup cleanup deadline"
            );
        }
    }
}

pub(super) enum SinkOpenOutcome<T> {
    Completed(T),
    Deadline,
    #[cfg(feature = "cluster")]
    ProcessAuthorityLost,
}

pub(super) enum SinkOpenFailure {
    Connector(String),
    Retired(String),
    #[cfg(feature = "cluster")]
    ProcessAuthorityLost,
}

pub(super) async fn await_sink_open<T>(
    deadline: tokio::time::Instant,
    #[cfg(feature = "cluster")] process_authority: Option<
        &laminar_core::cluster::control::ClusterController,
    >,
    future: impl std::future::Future<Output = T>,
) -> SinkOpenOutcome<T> {
    if tokio::time::Instant::now() >= deadline {
        return SinkOpenOutcome::Deadline;
    }
    let mut operation = std::pin::pin!(future);

    #[cfg(feature = "cluster")]
    if let Some(controller) = process_authority {
        if !controller.process_lease_is_live() {
            return SinkOpenOutcome::ProcessAuthorityLost;
        }
        return tokio::select! {
            biased;
            () = controller.wait_for_process_lease_loss() => {
                SinkOpenOutcome::ProcessAuthorityLost
            }
            () = tokio::time::sleep_until(deadline) => {
                SinkOpenOutcome::Deadline
            }
            result = &mut operation => {
                if controller.process_lease_is_live() {
                    SinkOpenOutcome::Completed(result)
                } else {
                    SinkOpenOutcome::ProcessAuthorityLost
                }
            }
        };
    }

    match tokio::time::timeout_at(deadline, operation.as_mut()).await {
        Ok(result) => SinkOpenOutcome::Completed(result),
        Err(_) => SinkOpenOutcome::Deadline,
    }
}

pub(super) async fn open_prepared_sinks(
    sinks: &mut [PreparedSink],
    open_timeout: std::time::Duration,
    #[cfg(feature = "cluster")] process_authority: Option<
        &laminar_core::cluster::control::ClusterController,
    >,
) -> Result<(), DbError> {
    let open_deadline = tokio::time::Instant::now() + open_timeout;
    let mut index = 0;
    while index < sinks.len() {
        if tokio::time::Instant::now() >= open_deadline {
            #[cfg(feature = "cluster")]
            if process_authority.is_some_and(|controller| !controller.process_lease_is_live()) {
                // A generic close may publish. Cluster startup therefore drops the unopened
                // generation instead of beginning cleanup that could cross the authority fence.
                return Err(DbError::Connector(format!(
                    "Failed to open sink '{}': cluster process lease expired during sink open",
                    sinks[index].name
                )));
            }
            // Tokio's timeout polls its inner future once even at an expired deadline. Do not
            // construct or poll another connector open after the shared startup budget is gone.
            close_opened_sinks(
                &mut sinks[..index],
                crate::pipeline::PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
                #[cfg(feature = "cluster")]
                process_authority,
            )
            .await;
            return Err(DbError::Connector(format!(
                "Failed to open sink '{}': shared {open_timeout:?} sink-open stage deadline was exhausted before open began",
                sinks[index].name
            )));
        }
        let prepared = &mut sinks[index];
        let name = prepared.name.clone();
        let cancellation_policy = prepared.connector.cancellation_policy();
        let open_error = {
            let open = prepared.connector.open(&prepared.config);
            match await_sink_open(
                open_deadline,
                #[cfg(feature = "cluster")]
                process_authority,
                open,
            )
            .await
            {
                SinkOpenOutcome::Completed(Ok(())) => None,
                SinkOpenOutcome::Completed(Err(error)) => {
                    if error.is_outcome_unknown() {
                        Some(SinkOpenFailure::Retired(error.to_string()))
                    } else {
                        Some(SinkOpenFailure::Connector(error.to_string()))
                    }
                }
                SinkOpenOutcome::Deadline => Some(
                    if cancellation_policy == ConnectorCancellationPolicy::RetireConnector {
                        SinkOpenFailure::Retired(format!(
                            "exceeded the shared {open_timeout:?} sink-open stage deadline"
                        ))
                    } else {
                        SinkOpenFailure::Connector(format!(
                            "exceeded the shared {open_timeout:?} sink-open stage deadline"
                        ))
                    },
                ),
                #[cfg(feature = "cluster")]
                SinkOpenOutcome::ProcessAuthorityLost => {
                    Some(SinkOpenFailure::ProcessAuthorityLost)
                }
            }
        };
        match open_error {
            Some(SinkOpenFailure::Connector(error)) => {
                #[cfg(feature = "cluster")]
                if process_authority.is_some_and(|controller| !controller.process_lease_is_live()) {
                    return Err(DbError::Connector(format!(
                        "Failed to open sink '{name}': cluster process lease expired during sink open"
                    )));
                }
                // A failed/cancelled open may already hold resources, so include the current sink.
                close_opened_sinks(
                    &mut sinks[..=index],
                    crate::pipeline::PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
                    #[cfg(feature = "cluster")]
                    process_authority,
                )
                .await;
                return Err(DbError::Connector(format!(
                    "Failed to open sink '{name}': {error}"
                )));
            }
            Some(SinkOpenFailure::Retired(error)) => {
                #[cfg(feature = "cluster")]
                if process_authority.is_some_and(|controller| !controller.process_lease_is_live()) {
                    return Err(DbError::Connector(format!(
                        "Failed to open sink '{name}': cluster process lease expired during sink open"
                    )));
                }
                // Dropping a timed-out open makes this generation terminal. Clean up only
                // connectors whose opens completed; never invoke another method on the retired
                // candidate.
                close_opened_sinks(
                    &mut sinks[..index],
                    crate::pipeline::PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
                    #[cfg(feature = "cluster")]
                    process_authority,
                )
                .await;
                return Err(DbError::Connector(format!(
                    "Failed to open sink '{name}': {error}"
                )));
            }
            #[cfg(feature = "cluster")]
            Some(SinkOpenFailure::ProcessAuthorityLost) => {
                // Generic close may flush or publish. Once cluster authority is gone, drop the
                // connector generation without invoking any further connector operation.
                return Err(DbError::Connector(format!(
                    "Failed to open sink '{name}': cluster process lease expired during sink open"
                )));
            }
            None => {}
        }
        index += 1;
    }

    #[cfg(feature = "cluster")]
    if process_authority.is_some_and(|controller| !controller.process_lease_is_live()) {
        return Err(DbError::Connector(
            "cluster process lease expired after the sink-open stage".into(),
        ));
    }
    Ok(())
}
