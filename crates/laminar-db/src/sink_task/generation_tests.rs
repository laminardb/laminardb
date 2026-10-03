//! Same-process actor revocation and late completion boundaries.

use super::*;
use crate::sink_task::operation::{await_connector_operation, ConnectorOperationOutcome};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn sink_generation_revoked_before_admission_never_constructs_connector_future() {
    let generation = CancellationToken::new();
    generation.cancel();
    let polls = AtomicU64::new(0);
    let outcome = await_connector_operation(
        Instant::now() + Duration::from_secs(1),
        &generation,
        #[cfg(feature = "cluster")]
        None,
        || {
            polls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(7)
        },
    )
    .await;
    assert!(matches!(
        outcome,
        ConnectorOperationOutcome::GenerationRetired
    ));
    assert_eq!(polls.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn sink_generation_same_poll_late_success_cannot_cross_revocation() {
    let generation = CancellationToken::new();
    let outcome = await_connector_operation(
        Instant::now() + Duration::from_secs(1),
        &generation,
        #[cfg(feature = "cluster")]
        None,
        || async {
            generation.cancel();
            Ok::<_, ConnectorError>(7)
        },
    )
    .await;
    assert!(matches!(
        outcome,
        ConnectorOperationOutcome::GenerationRetired
    ));
}

#[tokio::test]
async fn sink_generation_revocation_interrupts_pending_operation_and_retires_even_reusable_connector(
) {
    let generation = CancellationToken::new();
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let task_generation = generation.clone();
    let task_entered = Arc::clone(&entered);
    let task = tokio::spawn(async move {
        bounded_connector_operation(
            "retired",
            "pre-commit",
            Instant::now() + Duration::from_secs(5),
            ConnectorCancellationPolicy::CancelSafe,
            &task_generation,
            #[cfg(feature = "cluster")]
            None,
            || async {
                task_entered.add_permits(1);
                std::future::pending::<Result<(), ConnectorError>>().await
            },
        )
        .await
    });
    entered.acquire().await.unwrap().forget();
    generation.cancel();
    let (result, retired) = task.await.unwrap();
    assert!(retired);
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("generation retired"));
}

#[tokio::test]
async fn sink_generation_buffered_success_ack_is_rejected_after_actor_revocation() {
    let (sink, _, _) = CountingSink::new();
    let (original, _events) =
        spawn_with_defaults("buffered-ack", Box::new(sink), Duration::from_secs(5));
    let mut handle = original.clone();
    let (tx, rx) = mpsc::bounded_async::<SinkCommand>(1);
    handle.tx = tx;
    let call = tokio::spawn(async move { handle.sync().await });
    let command = rx.recv().await.unwrap();
    let SinkOperation::Sync { ack } = command.operation else {
        panic!("sync command");
    };
    ack.send(Ok(()));
    // No await between completion publication and revocation: the observer sees buffered Ok.
    original.actor_state.revoked.cancel();
    assert!(call
        .await
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("generation retired"));
    original.task.lock().as_ref().unwrap().abort_actor();
    assert!(original.close().await.is_err());
    assert!(!original.has_unresolved_task());
}

#[tokio::test]
async fn sink_generation_retired_handle_cannot_write_to_same_name_successor() {
    let (sink, old_writes, _) = CountingSink::new();
    let (old, _events) = spawn_with_defaults("same-name", Box::new(sink), Duration::from_secs(5));
    old.task.lock().as_ref().unwrap().abort_actor();
    assert!(old.write_batch(test_batch()).await.is_err());
    assert!(old.close().await.is_err());
    let (sink, new_writes, _) = CountingSink::new();
    let (new, _events) = spawn_with_defaults("same-name", Box::new(sink), Duration::from_secs(5));
    new.write_batch(test_batch()).await.unwrap();
    new.sync().await.unwrap();
    assert_eq!(old_writes.load(Ordering::Acquire), 0);
    assert_eq!(new_writes.load(Ordering::Acquire), 1);
    assert!(old.sync().await.is_err());
    new.close().await.unwrap();
}
