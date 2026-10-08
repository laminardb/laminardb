use super::*;
use crate::process_function::remote::{
    read_only_fixture_config, LocalPythonWorker, RemoteProcessClient,
};
use laminar_connectors::connector::DeliveryGuarantee;
use laminar_core::streaming::checkpoint::StreamCheckpointConfig;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a packaged Python runtime on a read-only Linux filesystem"]
async fn python_database_startup_binds_both_owners_and_matching_timer_cuts() {
    let worker = LocalPythonWorker::start(read_only_fixture_config())
        .await
        .unwrap();
    let binding = worker.client().descriptor().clone();
    let mut rig = runtime::Rig::new(&[7, 8]);
    let result = std::panic::AssertUnwindSafe(async {
        let unbound = RemoteProcessClient::connect_loopback(
            &worker.loopback_endpoint(),
            binding.clone(),
            4,
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        let storage = tempfile::tempdir().unwrap();
        let db = LaminarDB::builder()
            .storage_dir(storage.path())
            .checkpoint(StreamCheckpointConfig::default())
            .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
            .build()
            .await
            .unwrap();
        let rejected = db
            .register_remote_process_function(
                "activity",
                "events",
                binding.clone(),
                Arc::new(unbound),
            )
            .await
            .unwrap_err();
        assert!(rejected
            .to_string()
            .contains("supervised replay-safe worker"));
        db.shutdown().await.unwrap();
        rig.setup_process(binding, ProcessHandler::Remote(worker.client()))
            .await;
        rig.commit_prefix().await;
        rig.finish_suffix().await;
        assert_eq!(rig.output.len(), 9);
        let ids = rig
            .output
            .iter()
            .map(|row| row.1.split_once(':').unwrap().1.parse::<u64>().unwrap())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(ids.len(), 9);
        assert_eq!(
            rig.output
                .iter()
                .filter(|row| row.1.starts_with("inactive:"))
                .count(),
            4
        );
        assert!(rig.output.iter().any(|row| row.0 == key_for(0, "a")
            && row.1.starts_with("inactive:")
            && row.2 == 110
            && row.4 == 118_000));
        assert!(!rig
            .output
            .iter()
            .any(|row| row.1.starts_with("inactive:") && row.4 == 110_000));
    })
    .catch_unwind()
    .await;
    let cleanup = rig.close().await;
    let worker_cleanup = worker.shutdown().await;
    assert!(cleanup.is_empty(), "{cleanup:?}");
    worker_cleanup.unwrap();
    if let Err(primary) = result {
        std::panic::resume_unwind(primary);
    }
}
