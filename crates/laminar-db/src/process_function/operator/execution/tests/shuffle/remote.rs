use super::super::remote::{remote_operator, DelayedActivity, Worker};
use super::*;
use crate::process_function::ProcessRuntime;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shipped_rows_use_the_existing_remote_scheduler_and_fenced_timer_scope() {
    let pair = Pair::new().await;
    let mut binding = descriptor();
    binding.runtime = ProcessRuntime::RemoteRust;
    let mut worker = Worker::new(binding.clone(), Arc::new(AccountActivity)).await;
    let wake = Arc::new(tokio::sync::Notify::new());
    let mut operator = remote_operator(&pair.nodes[0], &worker, binding, &wake);
    operator
        .process_with_frontiers(&[], &frontier(100))
        .await
        .unwrap();
    drain_local(&mut operator, 100).await;
    let first = key_for(0);
    let second = key_for(2);
    let batch = input_batch(&[
        (&first, 3, 100_000),
        (&second, 1, 100_000),
        (&first, 5, 101_000),
    ]);
    stage(
        &mut operator,
        pair.ship(
            0,
            ShuffleMessage::checkpointed_routed("activity".into(), Arc::from([0, 2]), batch),
        )
        .await,
    );
    stage(
        &mut operator,
        pair.ship(0, peer_frontier(Some(120), false)).await,
    );
    assert!(operator
        .process_with_frontiers(&[], &frontier(120))
        .await
        .unwrap()
        .is_empty());
    assert!(!operator.wants_input());
    assert!(operator.checkpoint().is_err());
    assert_eq!(operator.output_frontier(frontier(120)[0]).watermark, None);
    let updates = activity_rows(&drain_local(&mut operator, 120).await);
    assert_eq!(
        updates
            .iter()
            .filter(|row| row.0 == first)
            .map(|row| row.2)
            .collect::<Vec<_>>(),
        [3, 8]
    );
    assert_eq!(
        updates
            .iter()
            .filter(|row| row.0 == second)
            .map(|row| row.2)
            .collect::<Vec<_>>(),
        [1]
    );
    let mut output = operator
        .process_with_frontiers(&[], &frontier(120))
        .await
        .unwrap();
    output.extend(drain_local(&mut operator, 120).await);
    let timers = activity_rows(&output);
    assert_eq!(timers.len(), 2);
    assert!(timers.iter().all(|row| row.1 == "inactive"));
    assert!(timers.iter().any(|row| row.0 == first && row.2 == 8));
    assert!(timers.iter().any(|row| row.0 == second && row.2 == 1));
    assert_eq!(operator.output_frontier(frontier(120)[0]), frontier(120)[0]);
    let mut scopes = Vec::new();
    while let Ok(scope) = worker.scopes.try_recv() {
        scopes.push(scope);
    }
    assert!(scopes.len() >= 4);
    assert!(scopes
        .iter()
        .all(|scope| (scope.owner_generation, scope.recovery_generation) == (7, 3)));
    assert!(scopes
        .iter()
        .all(|scope| scope.vnode == 0 || scope.vnode == 2));
    let attempts = scopes
        .iter()
        .map(|scope| &scope.attempt_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(attempts.len(), scopes.len());
    worker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frontier_and_queued_data_cannot_overtake_a_delayed_stale_worker_reply() {
    let pair = Pair::new().await;
    let mut binding = descriptor();
    binding.runtime = ProcessRuntime::RemoteRust;
    let (release, receiver) = std::sync::mpsc::channel();
    let handler = Arc::new(DelayedActivity {
        entered: tokio::sync::Notify::new(),
        release: std::sync::Mutex::new(receiver),
    });
    let mut worker = Worker::new(binding.clone(), handler.clone()).await;
    let wake = Arc::new(tokio::sync::Notify::new());
    let mut operator = remote_operator(&pair.nodes[0], &worker, binding, &wake);
    let key = key_for(0);
    let before = state_image(&operator);
    stage(
        &mut operator,
        pair.ship(
            0,
            ShuffleMessage::checkpointed("activity".into(), 0, input_batch(&[(&key, 7, 100_000)])),
        )
        .await,
    );
    stage(
        &mut operator,
        pair.ship(0, peer_frontier(Some(120), false)).await,
    );
    operator
        .process_with_frontiers(&[], &frontier(120))
        .await
        .unwrap();
    tokio::time::timeout(DEADLINE, handler.entered.notified())
        .await
        .unwrap();
    let scope = worker.scopes.recv().await.unwrap();
    assert_eq!(
        (
            scope.owner_generation,
            scope.recovery_generation,
            scope.vnode
        ),
        (7, 3, 0)
    );
    assert!(operator.checkpoint().is_err());
    assert_eq!(operator.watermark_us, i64::MIN);
    assert_eq!(operator.output_frontier(frontier(120)[0]).watermark, None);
    pair.nodes[0]
        .controller
        .process_lease_deadline()
        .unwrap()
        .fence();
    release.send(()).unwrap();
    tokio::time::timeout(DEADLINE, wake.notified())
        .await
        .unwrap();
    assert!(operator
        .process_with_frontiers(&[], &frontier(120))
        .await
        .unwrap_err()
        .requires_pipeline_recovery());
    assert_eq!(state_image(&operator), before);
    drop(operator);
    worker.stop().await;
}
