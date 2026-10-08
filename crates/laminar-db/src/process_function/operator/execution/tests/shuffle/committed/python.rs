use super::*;
use crate::process_function::remote::{read_only_fixture_config, LocalPythonWorker};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a packaged Python runtime on a read-only Linux filesystem"]
async fn python_committed_replay_preserves_callback_ids_after_worker_restart_and_rescale() {
    let config = read_only_fixture_config();
    let worker = LocalPythonWorker::start(config.clone()).await.unwrap();
    let binding = worker.client().descriptor().clone();
    let pair = Pair::new().await;
    let mut original = populated(
        &pair,
        binding.clone(),
        ProcessHandler::Remote(worker.client()),
    )
    .await;
    let cut = SharedCut::persist(&pair, &mut original).await;
    let reference = replay::replay_input(&mut original, 4).await;
    for node in &pair.nodes {
        node.controller.fence_process_lease();
    }
    drop(original);
    let old_client = worker.client();
    worker.shutdown().await.unwrap();
    assert!(!old_client.has_python_replay_binding());

    let replacement = LocalPythonWorker::start(config).await.unwrap();
    let owners = [7, 9, 8, 9];
    let fence = target_fence(8, owners);
    let nodes = target_nodes(&pair, &fence, owners).await;
    cut.publish_target([7, 8, 7, 8], &fence, owners).await;
    let mut graphs = Vec::new();
    for node in &nodes {
        let recovered = cut.recover(node).await.unwrap();
        graphs.push(
            restore_graph(
                node,
                &recovered,
                binding.clone(),
                ProcessHandler::Remote(replacement.client()),
            )
            .unwrap(),
        );
    }
    progress(&mut graphs, 105).await;
    let actual = replay::replay_input(&mut graphs, 1).await;
    let mut expected = activity_rows(&reference);
    let mut actual = activity_rows(&actual);
    expected.sort();
    actual.sort();
    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 12);
    let ids = actual
        .iter()
        .map(|row| row.1.split_once(':').unwrap().1.parse::<u64>().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), 12);
    drop(graphs);
    replacement.shutdown().await.unwrap();
}
