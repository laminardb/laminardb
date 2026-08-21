//! Mechanically extracted coordinator responsibility.

#[cfg(all(test, feature = "cluster"))]
use super::{spawn_source_actor, Arc, AtomicBool};
#[cfg(feature = "cluster")]
use super::{
    validate_source_drain_receipts, AssignmentDrainId, AssignmentDrainTransition,
    CheckpointParticipant, OwnedSourceTasks, SourceDrainCommand, SourceDrainLeaseControl,
    SourceDrainOutcome, SourceDrainReceipt, SourceDrainRequest, SourceDrainResolution,
    SourceDrainTaskStatus, SourceTaskLease,
};

#[cfg(feature = "cluster")]
pub(super) async fn await_source_drain_receipt(
    task: &SourceTaskLease,
    control: &SourceDrainLeaseControl,
    round: AssignmentDrainId,
    deadline: tokio::time::Instant,
) -> Result<SourceDrainReceipt, String> {
    let mut status_rx = control.status_tx.subscribe();
    loop {
        match status_rx.borrow_and_update().clone() {
            SourceDrainTaskStatus::Ready(receipt) if receipt.round == round => {
                return Ok(receipt);
            }
            SourceDrainTaskStatus::Ready(receipt) => {
                return Err(format!(
                    "source '{}' retained stale drain receipt {:?} while waiting for {round:?}",
                    task.name(),
                    receipt.round
                ));
            }
            SourceDrainTaskStatus::Pausing(active) if active != round => {
                return Err(format!(
                    "source '{}' is pausing conflicting drain {active:?}",
                    task.name()
                ));
            }
            SourceDrainTaskStatus::Resolved { round: active, .. } if active == round => {
                return Err(format!(
                    "source '{}' resolved drain {round:?} before publishing a receipt",
                    task.name()
                ));
            }
            SourceDrainTaskStatus::Idle
            | SourceDrainTaskStatus::Pausing(_)
            | SourceDrainTaskStatus::Resolved { .. } => {}
        }
        if task.is_finished() {
            return Err(format!(
                "source '{}' exited while preparing drain {round:?}",
                task.name()
            ));
        }
        let task_finished = task.wait_finished();
        tokio::pin!(task_finished);
        if task.is_finished() {
            continue;
        }
        let wait = async {
            tokio::select! {
                changed = status_rx.changed() => changed.map_err(|_| "source drain status channel closed"),
                () = task_finished.as_mut() => Ok(()),
            }
        };
        match tokio::time::timeout_at(deadline, wait).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {
                return Err(format!(
                    "source '{}' did not reach drain {round:?} before its deadline",
                    task.name()
                ));
            }
        }
    }
}

#[cfg(feature = "cluster")]
pub(crate) async fn prepare_owned_source_drain(
    tasks: &OwnedSourceTasks,
    transition: &AssignmentDrainTransition,
    participant: CheckpointParticipant,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    if !transition.is_canonical()
        || transition
            .predecessor
            .participant_incarnation(participant.node_id)
            != Some(participant.boot_incarnation)
    {
        return Err("local source drain participant is absent from predecessor roster".into());
    }
    let request = SourceDrainRequest::new(transition.id()).map_err(|error| error.to_string())?;
    let snapshot: Vec<(SourceTaskLease, SourceDrainLeaseControl)> = tasks
        .lock()
        .iter()
        .map(|task| {
            task.drain_control()
                .map(|control| (task.clone(), control))
                .ok_or_else(|| format!("source '{}' has no cluster drain control", task.name()))
        })
        .collect::<Result<_, _>>()?;
    for (task, control) in &snapshot {
        if task.is_finished() {
            return Err(format!(
                "source '{}' is not live at drain admission",
                task.name()
            ));
        }
        control
            .command_tx
            .send(Some(SourceDrainCommand::Begin {
                request: request.clone(),
                participant,
                deadline,
            }))
            .map_err(|_| format!("source '{}' drain command channel closed", task.name()))?;
        control.wake.notify_one();
    }
    let mut receipts = Vec::with_capacity(snapshot.len());
    for (task, control) in &snapshot {
        let receipt = await_source_drain_receipt(task, control, request.round, deadline).await?;
        if receipt.source_task_incarnation != control.task_incarnation {
            return Err(format!(
                "source '{}' returned a receipt from a replaced task generation",
                task.name()
            ));
        }
        receipts.push(receipt);
    }
    validate_source_drain_receipts(request.round, participant, &receipts)?;

    // Revalidate the exact active source generation immediately before acknowledging the cut.
    let mut expected: Vec<uuid::Uuid> = snapshot
        .iter()
        .map(|(_, control)| control.task_incarnation)
        .collect();
    let mut current: Vec<uuid::Uuid> = tasks
        .lock()
        .iter()
        .map(|task| {
            task.drain_control()
                .map(|control| control.task_incarnation)
                .ok_or_else(|| format!("source '{}' lost cluster drain control", task.name()))
        })
        .collect::<Result<_, _>>()?;
    expected.sort_unstable();
    current.sort_unstable();
    if current != expected {
        return Err("source task generation changed while preparing the drain".into());
    }
    Ok(())
}

#[cfg(feature = "cluster")]
pub(crate) fn owned_source_drain_resolved(
    tasks: &OwnedSourceTasks,
    resolution: SourceDrainResolution,
) -> Result<bool, String> {
    for task in tasks.lock().iter() {
        let control = task
            .drain_control()
            .ok_or_else(|| format!("source '{}' has no cluster drain control", task.name()))?;
        // The watch value is the retained terminal proof for this exact task generation. A
        // pipeline fault can join the source actor after it published Resolved; task liveness must
        // not erase that proof and make the snapshot watcher try to resolve the same committed
        // drain on an exited actor. A finished task without the exact retained Commit terminal
        // remains unresolved below; Abort is already a safe no-op on a retired actor. A replacement
        // generation therefore still has to reconcile a committed durable cut.
        if matches!(
            control.status_tx.borrow().clone(),
            SourceDrainTaskStatus::Resolved { round, outcome }
                if round == resolution.round && outcome == resolution.outcome
        ) {
            continue;
        }
        if task.is_finished() {
            if resolution.outcome == SourceDrainOutcome::Abort {
                continue;
            }
            return Ok(false);
        }
        return Ok(false);
    }
    Ok(true)
}

#[cfg(feature = "cluster")]
pub(crate) async fn resolve_owned_source_drain(
    tasks: &OwnedSourceTasks,
    resolution: SourceDrainResolution,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    let snapshot: Vec<(SourceTaskLease, SourceDrainLeaseControl)> = tasks
        .lock()
        .iter()
        .map(|task| {
            task.drain_control()
                .map(|control| (task.clone(), control))
                .ok_or_else(|| format!("source '{}' has no cluster drain control", task.name()))
        })
        .collect::<Result<_, _>>()?;
    for (task, control) in &snapshot {
        if task.is_finished() {
            if resolution.outcome == SourceDrainOutcome::Abort {
                continue;
            }
            return Err(format!(
                "source '{}' exited before committing drain {:?}",
                task.name(),
                resolution.round
            ));
        }
        let sent = control
            .command_tx
            .send(Some(SourceDrainCommand::Resolve {
                resolution,
                deadline,
            }))
            .is_ok();
        if !sent {
            if resolution.outcome == SourceDrainOutcome::Abort {
                continue;
            }
            return Err(format!(
                "source '{}' drain command channel closed",
                task.name()
            ));
        }
        control.wake.notify_one();
    }
    for (task, control) in &snapshot {
        if task.is_finished() && resolution.outcome == SourceDrainOutcome::Abort {
            continue;
        }
        let mut status_rx = control.status_tx.subscribe();
        loop {
            match status_rx.borrow_and_update().clone() {
                SourceDrainTaskStatus::Resolved { round, outcome }
                    if round == resolution.round && outcome == resolution.outcome =>
                {
                    break;
                }
                SourceDrainTaskStatus::Resolved { round, .. } if round != resolution.round => {
                    return Err(format!(
                        "source '{}' resolved stale drain {round:?}",
                        task.name()
                    ));
                }
                _ => {}
            }
            if task.is_finished() {
                if resolution.outcome == SourceDrainOutcome::Abort {
                    break;
                }
                return Err(format!(
                    "source '{}' exited while resolving drain {:?}",
                    task.name(),
                    resolution.round
                ));
            }
            let task_finished = task.wait_finished();
            tokio::pin!(task_finished);
            if task.is_finished() {
                continue;
            }
            let wait = async {
                tokio::select! {
                    changed = status_rx.changed() => changed.map_err(|_| "source drain status channel closed"),
                    () = task_finished.as_mut() => Ok(()),
                }
            };
            match tokio::time::timeout_at(deadline, wait).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    return Err(format!("source '{}': {error}", task.name()));
                }
                Err(_) => {
                    return Err(format!(
                        "source '{}' did not resolve drain {:?} before its deadline",
                        task.name(),
                        resolution.round
                    ));
                }
            }
        }
    }
    Ok(())
}

#[cfg(all(test, feature = "cluster"))]
pub(crate) fn install_replacement_source_drain_task_for_test(
    tasks: &OwnedSourceTasks,
    name: &str,
) -> SourceTaskLease {
    let (command_tx, mut command_rx) =
        tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
    let (status_tx, _status_rx) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
    let wake = Arc::new(tokio::sync::Notify::new());
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let actor_wake = Arc::clone(&wake);
    let actor_shutdown = Arc::clone(&shutdown);
    let actor_status = status_tx.clone();
    let runtime = tokio::runtime::Handle::current();
    let (join, actor_terminal) = spawn_source_actor(&runtime, async move {
        loop {
            tokio::select! {
                () = actor_shutdown.notified() => return,
                () = actor_wake.notified() => {
                    let command = command_rx.borrow_and_update().clone();
                    if let Some(SourceDrainCommand::Resolve { resolution, .. }) = command {
                        actor_status.send_replace(SourceDrainTaskStatus::Resolved {
                            round: resolution.round,
                            outcome: resolution.outcome,
                        });
                    }
                }
            }
        }
    });
    let task = SourceTaskLease::supervise(
        Arc::from(name),
        shutdown,
        Arc::new(AtomicBool::new(false)),
        join,
        actor_terminal,
        None,
        &runtime,
    );
    task.install_drain_control(SourceDrainLeaseControl {
        task_incarnation: uuid::Uuid::new_v4(),
        command_tx,
        status_tx,
        wake,
    });
    tasks.lock().push(task.clone());
    task
}
