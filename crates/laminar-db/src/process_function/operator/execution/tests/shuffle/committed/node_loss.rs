use super::*;
use futures::TryStreamExt;
use object_store::{local::LocalFileSystem, ObjectStoreExt};

const CHILD_ENV: &str = "LAMINAR_PROCESS_SHARED_CUT_CHILD";
const TEST_NAME: &str = "process_function::operator::execution::tests::shuffle::committed::node_loss::killed_host_restores_only_the_committed_shared_state_and_timer_cut";

fn filesystem(root: &std::path::Path) -> Arc<dyn object_store::ObjectStore> {
    Arc::new(LocalFileSystem::new_with_prefix(root).unwrap())
}

async fn child_run(root: &std::path::Path) {
    let pair = Pair::new().await;
    let mut graphs = populated(
        &pair,
        descriptor(),
        ProcessHandler::Native(Arc::new(AccountActivity)),
    )
    .await;
    let cut = SharedCut::persist(&pair, &mut graphs).await;
    // LocalFileSystem has no conditional update. Preserve the real CAS-committed namespace
    // as a read-only restart image instead of emulating cluster authority on local files.
    let objects = cut
        .objects
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(
        objects.len() <= 64,
        "fixture object inventory must stay bounded"
    );
    let durable = filesystem(root);
    for object in objects {
        let payload = cut
            .objects
            .get(&object.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        durable.put(&object.location, payload.into()).await.unwrap();
    }
    graphs[0]
        .execute_cycle(&source_batch(&[(&key_for(0), 999, 106_000)]), 106, None)
        .await
        .unwrap();
    let output = progress(&mut graphs, 106).await;
    assert_eq!(activity_rows(&output[0])[0].2, 1006);
    std::fs::write(root.join("uncommitted-application"), b"1006").unwrap();
    tokio::time::timeout(Duration::from_secs(30), std::future::pending::<()>())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_host_restores_only_the_committed_shared_state_and_timer_cut() {
    if let Some(root) = std::env::var_os(CHILD_ENV) {
        child_run(std::path::Path::new(&root)).await;
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST_NAME, "--nocapture"])
        .env(CHILD_ENV, root)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let marker = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if root.join("uncommitted-application").exists() {
                break;
            }
            if let Some(status) = child.try_wait().unwrap() {
                panic!("shared checkpoint child exited before the failure cut: {status}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    child.start_kill().unwrap();
    let status = tokio::time::timeout(DEADLINE, child.wait())
        .await
        .unwrap()
        .unwrap();
    marker.expect("child did not persist its shared cut and apply the uncommitted input");
    assert!(!status.success());
    let objects = filesystem(root);
    let cut = SharedCut::open(Arc::clone(&objects)).await;
    let authority = Arc::new(ProcessLeaseAuthority::new(objects, Duration::from_secs(60)).unwrap());
    let node = Fixture::for_assignment(
        authority,
        NodeId(9),
        target_fence(8, [9; 4]),
        [9; 4],
        4,
        Duration::from_secs(60),
    )
    .await;
    let recovered = cut.recover(&node).await.unwrap();
    assert!(recovered.reassigned);
    assert_eq!(recovered.checkpoint_watermark(), Some(105));
    assert_eq!(
        recovered.source_offsets()["events"].offsets["partition-7"],
        "2"
    );
    let mut graph = restore_graph(
        &node,
        &recovered,
        descriptor(),
        ProcessHandler::Native(Arc::new(AccountActivity)),
    )
    .unwrap();
    let output = progress(std::slice::from_mut(&mut graph), 115).await;
    let mut totals = activity_rows(&output[0])
        .into_iter()
        .map(|row| row.2)
        .collect::<Vec<_>>();
    totals.sort_unstable();
    assert_eq!(totals, [7, 11, 13, 17]);
    assert!(progress(std::slice::from_mut(&mut graph), 115).await[0].is_empty());
    let output = graph
        .execute_cycle(&source_batch(&[(&key_for(0), 1, 120_000)]), 120, None)
        .await
        .unwrap();
    assert_eq!(activity_rows(&output["activity"])[0].2, 8);
}

#[tokio::test]
async fn damaged_committed_donor_is_rejected_before_graph_restoration() {
    let pair = Pair::new().await;
    let mut original = populated(
        &pair,
        descriptor(),
        ProcessHandler::Native(Arc::new(AccountActivity)),
    )
    .await;
    let cut = SharedCut::persist(&pair, &mut original).await;
    let nodes = target_nodes(&pair, &target_fence(8, [9; 4]), [9; 4]).await;
    let path = object_store::path::Path::from(
        "process-shared-cut/nodes/8/checkpoints/00000000000000000001/node-data.bin",
    );
    cut.objects
        .put(&path, bytes::Bytes::from_static(b"damaged").into())
        .await
        .unwrap();
    let error = cut.recover(&nodes[0]).await.unwrap_err();
    assert!(
        error.to_string().contains("node data object is 7 bytes"),
        "{error}"
    );
}
