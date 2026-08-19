use std::collections::{BinaryHeap, HashMap};
use std::path::PathBuf;

use laminar_core::cluster::discovery::{
    NodeId, NodeInfo, NodeMetadata, NodeState, StaticDiscovery, StaticDiscoveryConfig,
};
use object_store::ObjectStoreExt as _;
use tokio::sync::watch;

use super::assignment::{
    assignment_seed_participants, is_same_formation_genesis, resolve_vnode_assignment,
    startup_leader_authority_timeout, wait_for_startup_assignment_fence,
    wait_for_startup_leader_authority,
};
use super::control_kv::{
    list_control_sequences, object_store_control_key_prefix, object_store_control_record_path,
    recovery_generation_path, retain_oldest_control_record, ObjectStoreClusterKv,
    ObjectStoreControlRecord, StaticClusterKv, OBJECT_STORE_CONTROL_MAX_ENVELOPE_BYTES,
    OBJECT_STORE_CONTROL_PRUNE_BATCH_RECORDS, OBJECT_STORE_CONTROL_SCAN_CONCURRENCY,
    OBJECT_STORE_CONTROL_VERSION, RECOVERY_GENERATION_KEY, RECOVERY_GENERATION_PREFIX,
};
use super::leases::spawn_process_lease_terminal_monitor;
use super::services::start_cluster_http_api_before_activation;
use super::*;
use crate::cluster_config::ClusterConfig;
use crate::config::ServerConfig;

#[tokio::test]
async fn cluster_entry_rejects_invalid_temporal_retention_before_discovery() {
    let mut config: ServerConfig = toml::from_str(
        r#"
node_id = "node-a"

[server]
mode = "cluster"

[discovery]
strategy = "static"
seeds = ["node-a:7946"]
"#,
    )
    .unwrap();
    config.server.temporal_join_idle_history_retention =
        Some(std::time::Duration::from_nanos(999_999));
    let cluster_config = ClusterConfig::from_server_config(&config).unwrap().unwrap();

    let error = match start_cluster(config, cluster_config, PathBuf::from("unused.toml")).await {
        Ok(_) => panic!("invalid temporal retention reached cluster discovery"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("temporal_join_idle_history_retention must be at least 1ms"),
        "{error}"
    );
}

struct DelayedControlPutStore {
    inner: Arc<dyn object_store::ObjectStore>,
    blocked_path: parking_lot::Mutex<Option<object_store::path::Path>>,
    entered: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
    completed: Arc<tokio::sync::Semaphore>,
    blocked_get_path: parking_lot::Mutex<Option<object_store::path::Path>>,
    get_entered: tokio::sync::Semaphore,
    get_release: tokio::sync::Semaphore,
    track_get_concurrency: std::sync::atomic::AtomicBool,
    active_gets: std::sync::atomic::AtomicUsize,
    max_gets: std::sync::atomic::AtomicUsize,
}

impl DelayedControlPutStore {
    fn new(inner: Arc<dyn object_store::ObjectStore>) -> Self {
        Self {
            inner,
            blocked_path: parking_lot::Mutex::new(None),
            entered: Arc::new(tokio::sync::Semaphore::new(0)),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
            completed: Arc::new(tokio::sync::Semaphore::new(0)),
            blocked_get_path: parking_lot::Mutex::new(None),
            get_entered: tokio::sync::Semaphore::new(0),
            get_release: tokio::sync::Semaphore::new(0),
            track_get_concurrency: std::sync::atomic::AtomicBool::new(false),
            active_gets: std::sync::atomic::AtomicUsize::new(0),
            max_gets: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn block_once(&self, path: object_store::path::Path) {
        *self.blocked_path.lock() = Some(path);
    }

    async fn wait_until_blocked(&self) {
        self.entered.acquire().await.unwrap().forget();
    }

    fn release(&self) {
        self.release.add_permits(1);
    }

    async fn wait_until_completed(&self) {
        self.completed.acquire().await.unwrap().forget();
    }

    fn block_get_once(&self, path: object_store::path::Path) {
        *self.blocked_get_path.lock() = Some(path);
    }

    async fn wait_until_get_blocked(&self) {
        self.get_entered.acquire().await.unwrap().forget();
    }

    fn release_get(&self) {
        self.get_release.add_permits(1);
    }

    fn begin_get_concurrency_probe(&self) {
        self.active_gets
            .store(0, std::sync::atomic::Ordering::Release);
        self.max_gets.store(0, std::sync::atomic::Ordering::Release);
        self.track_get_concurrency
            .store(true, std::sync::atomic::Ordering::Release);
    }

    fn finish_get_concurrency_probe(&self) -> usize {
        self.track_get_concurrency
            .store(false, std::sync::atomic::Ordering::Release);
        self.max_gets.load(std::sync::atomic::Ordering::Acquire)
    }
}

impl std::fmt::Debug for DelayedControlPutStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DelayedControlPutStore")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for DelayedControlPutStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DelayedControlPutStore")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for DelayedControlPutStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let should_block = {
            let mut blocked_path = self.blocked_path.lock();
            if blocked_path.as_ref() == Some(location) {
                blocked_path.take();
                true
            } else {
                false
            }
        };
        if should_block {
            self.entered.add_permits(1);
            let inner = Arc::clone(&self.inner);
            let release = Arc::clone(&self.release);
            let completed = Arc::clone(&self.completed);
            let location = location.clone();
            let (sender, receiver) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let result = match release.acquire().await {
                    Ok(permit) => {
                        permit.forget();
                        inner.put_opts(&location, payload, options).await
                    }
                    Err(error) => Err(object_store::Error::Generic {
                        store: "DelayedControlPutStore",
                        source: Box::new(error),
                    }),
                };
                completed.add_permits(1);
                let _ = sender.send(result);
            });
            return receiver
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "DelayedControlPutStore",
                    source: Box::new(error),
                })?;
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let track_concurrency = self
            .track_get_concurrency
            .load(std::sync::atomic::Ordering::Acquire);
        if track_concurrency {
            let active = self
                .active_gets
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                + 1;
            self.max_gets
                .fetch_max(active, std::sync::atomic::Ordering::AcqRel);
            tokio::task::yield_now().await;
        }
        let should_block = {
            let mut blocked_path = self.blocked_get_path.lock();
            if blocked_path.as_ref() == Some(location) {
                blocked_path.take();
                true
            } else {
                false
            }
        };
        if should_block {
            self.get_entered.add_permits(1);
            self.get_release
                .acquire()
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "DelayedControlPutStore",
                    source: Box::new(error),
                })?
                .forget();
        }
        let result = self.inner.get_opts(location, options).await;
        if track_concurrency {
            self.active_gets
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        }
        result
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

async fn acquire_test_process_lease(
    store: Arc<dyn object_store::ObjectStore>,
    node: NodeId,
    owner: uuid::Uuid,
    ttl_ms: i64,
) -> laminar_core::cluster::control::ProcessLease {
    use laminar_core::cluster::control::{ProcessLeaseOutcome, ProcessLeaseStore};

    let authority = ProcessLeaseStore::new(store, node, ttl_ms);
    let ProcessLeaseOutcome::Acquired(lease) = authority.try_acquire(owner, 1).await.unwrap()
    else {
        panic!("test process lease was not acquired");
    };
    lease
}

async fn take_over_test_process_lease(
    store: Arc<dyn object_store::ObjectStore>,
    incumbent: &laminar_core::cluster::control::ProcessLease,
    replacement: uuid::Uuid,
    ttl_ms: i64,
) -> laminar_core::cluster::control::ProcessLease {
    use laminar_core::cluster::control::{ProcessLeaseOutcome, ProcessLeaseStore};

    let authority = ProcessLeaseStore::new(store, incumbent.node, ttl_ms);
    let observation = authority.observe_rival(incumbent).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(
        u64::try_from(ttl_ms).unwrap() + 2,
    ))
    .await;
    let ProcessLeaseOutcome::Acquired(lease) = authority
        .try_takeover(replacement, &observation, 100)
        .await
        .unwrap()
    else {
        panic!("test process lease was not taken over");
    };
    lease
}

fn live_test_process_deadline() -> Arc<laminar_core::cluster::control::LeaseDeadline> {
    Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
        std::time::Duration::from_secs(60),
    ))
}

#[derive(Clone)]
struct SharedTestKv {
    local_id: NodeId,
    values: Arc<parking_lot::Mutex<HashMap<(NodeId, String), String>>>,
}

#[async_trait::async_trait]
impl laminar_core::cluster::control::ClusterKv for SharedTestKv {
    async fn write(&self, key: &str, value: String) {
        self.values
            .lock()
            .insert((self.local_id, key.to_string()), value);
    }

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        self.values.lock().get(&(who, key.to_string())).cloned()
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        self.values
            .lock()
            .iter()
            .filter(|((_, stored_key), _)| stored_key == key)
            .map(|((node, _), value)| (*node, value.clone()))
            .collect()
    }
}

fn shared_test_kvs() -> [Arc<dyn laminar_core::cluster::control::ClusterKv>; 2] {
    let values = Arc::new(parking_lot::Mutex::new(HashMap::new()));
    [
        Arc::new(SharedTestKv {
            local_id: NodeId(1),
            values: Arc::clone(&values),
        }),
        Arc::new(SharedTestKv {
            local_id: NodeId(2),
            values,
        }),
    ]
}

#[test]
fn test_cluster_startup_error_display() {
    let errors: Vec<ClusterStartupError> = vec![
        ClusterStartupError::Discovery("connection refused".into()),
        ClusterStartupError::FormationTimeout {
            found: 1,
            needed: 3,
        },
        ClusterStartupError::EngineConstruction("build failed".into()),
        ClusterStartupError::HttpStartup("port in use".into()),
        ClusterStartupError::AuthorityLost("process lease expired".into()),
    ];
    for err in &errors {
        assert!(!err.to_string().is_empty());
    }
}

#[test]
fn test_formation_timeout_includes_counts() {
    let err = ClusterStartupError::FormationTimeout {
        found: 1,
        needed: 3,
    };
    let msg = err.to_string();
    assert!(msg.contains('1'));
    assert!(msg.contains('3'));
}

#[tokio::test]
async fn terminal_process_signal_preempts_the_os_shutdown_wait() {
    let terminal = tokio_util::sync::CancellationToken::new();
    terminal.cancel();
    let leader_shutdown = tokio_util::sync::CancellationToken::new();
    let leader_task = tokio::spawn(std::future::pending::<()>());
    let mut leader_lease = LeaderLeaseRuntime::new(leader_shutdown, leader_task);
    let mut api_handle = tokio::spawn(std::future::pending::<()>());

    let trigger = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        wait_for_cluster_shutdown_trigger(&terminal, &leader_lease, &api_handle, &[]),
    )
    .await
    .expect("terminal process signal must wake shutdown promptly")
    .unwrap();

    assert_eq!(trigger, ClusterShutdownTrigger::ProcessLeaseLost);
    leader_lease.stop().await;
    let _ = abort_and_join_cluster_task(&mut api_handle, "test HTTP API server").await;
}

#[tokio::test]
async fn exited_http_api_triggers_cluster_runtime_shutdown() {
    let terminal = tokio_util::sync::CancellationToken::new();
    let leader_shutdown = tokio_util::sync::CancellationToken::new();
    let leader_task = tokio::spawn(std::future::pending::<()>());
    let mut leader_lease = LeaderLeaseRuntime::new(leader_shutdown, leader_task);
    let mut api_handle = tokio::spawn(async {});

    let trigger = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        wait_for_cluster_shutdown_trigger(&terminal, &leader_lease, &api_handle, &[]),
    )
    .await
    .expect("an exited HTTP API task must wake cluster shutdown")
    .unwrap();

    assert_eq!(trigger, ClusterShutdownTrigger::HttpApiExited);
    leader_lease.stop().await;
    let _ = abort_and_join_cluster_task(&mut api_handle, "test HTTP API server").await;
}

#[tokio::test]
async fn exited_leader_lease_manager_triggers_cluster_runtime_shutdown() {
    let terminal = tokio_util::sync::CancellationToken::new();
    let leader_shutdown = tokio_util::sync::CancellationToken::new();
    let leader_task = tokio::spawn(async {});
    let mut leader_lease = LeaderLeaseRuntime::new(leader_shutdown, leader_task);
    let mut api_handle = tokio::spawn(std::future::pending::<()>());

    let trigger = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        wait_for_cluster_shutdown_trigger(&terminal, &leader_lease, &api_handle, &[]),
    )
    .await
    .expect("an exited leader lease manager must wake cluster shutdown")
    .unwrap();

    assert_eq!(trigger, ClusterShutdownTrigger::LeaderLeaseExited);
    leader_lease.stop().await;
    let _ = abort_and_join_cluster_task(&mut api_handle, "test HTTP API server").await;
}

#[tokio::test]
async fn exited_rebalance_task_triggers_cluster_runtime_shutdown() {
    let terminal = tokio_util::sync::CancellationToken::new();
    let leader_shutdown = tokio_util::sync::CancellationToken::new();
    let leader_task = tokio::spawn(std::future::pending::<()>());
    let mut leader_lease = LeaderLeaseRuntime::new(leader_shutdown, leader_task);
    let mut api_handle = tokio::spawn(std::future::pending::<()>());
    let mut rebalance_tasks = vec![tokio::spawn(async {})];

    let trigger = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        wait_for_cluster_shutdown_trigger(&terminal, &leader_lease, &api_handle, &rebalance_tasks),
    )
    .await
    .expect("an exited rebalance control task must wake cluster shutdown")
    .unwrap();

    assert_eq!(trigger, ClusterShutdownTrigger::RebalanceTaskExited);
    leader_lease.stop().await;
    let _ = abort_and_join_cluster_task(&mut api_handle, "test HTTP API server").await;
    for task in &mut rebalance_tasks {
        let _ = abort_and_join_cluster_task(task, "test rebalance task").await;
    }
}

#[tokio::test]
async fn shutdown_trigger_prefers_process_lease_loss_when_every_trigger_is_ready() {
    let terminal = tokio_util::sync::CancellationToken::new();
    terminal.cancel();
    let leader_shutdown = tokio_util::sync::CancellationToken::new();
    let leader_task = tokio::spawn(async {});
    let mut leader_lease = LeaderLeaseRuntime::new(leader_shutdown, leader_task);
    let mut api_handle = tokio::spawn(async {});
    let mut rebalance_tasks = vec![tokio::spawn(async {})];

    let trigger = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        wait_for_cluster_shutdown_trigger(&terminal, &leader_lease, &api_handle, &rebalance_tasks),
    )
    .await
    .expect("a ready shutdown trigger must be selected promptly")
    .unwrap();

    assert_eq!(trigger, ClusterShutdownTrigger::ProcessLeaseLost);
    leader_lease.stop().await;
    let _ = abort_and_join_cluster_task(&mut api_handle, "test HTTP API server").await;
    for task in &mut rebalance_tasks {
        let _ = abort_and_join_cluster_task(task, "test rebalance task").await;
    }
}

#[tokio::test]
async fn process_lease_terminal_monitor_starts_before_resource_fencing() {
    let (live_tx, live_rx) = watch::channel(true);
    let terminal = tokio_util::sync::CancellationToken::new();
    let monitor = spawn_process_lease_terminal_monitor(
        live_rx,
        Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
            std::time::Duration::from_secs(10),
        )),
        terminal.clone(),
    );

    live_tx.send_replace(false);
    tokio::time::timeout(std::time::Duration::from_millis(100), terminal.cancelled())
        .await
        .expect("terminal monitor must publish loss without installed resources");
    monitor.await.unwrap();
}

#[tokio::test]
async fn process_lease_terminal_monitor_observes_the_monotonic_deadline() {
    let (_live_tx, live_rx) = watch::channel(true);
    let terminal = tokio_util::sync::CancellationToken::new();
    let monitor = spawn_process_lease_terminal_monitor(
        live_rx,
        Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
            std::time::Duration::from_millis(20),
        )),
        terminal.clone(),
    );

    tokio::time::timeout(std::time::Duration::from_secs(1), terminal.cancelled())
        .await
        .expect("monotonic expiry must publish terminal lease loss");
    monitor.await.unwrap();
}

#[tokio::test]
async fn cancelled_process_terminal_is_synchronously_not_live() {
    let node = NodeId(48);
    let owner = uuid::Uuid::from_u128(488);
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let acquired = acquire_test_process_lease(store, node, owner, 10_000).await;
    let (_live_tx, live_rx) = watch::channel(true);
    let terminal = tokio_util::sync::CancellationToken::new();
    let mut process_lease = ProcessLeaseRuntime {
        acquired,
        deadline: live_test_process_deadline(),
        live_rx,
        shutdown: tokio_util::sync::CancellationToken::new(),
        terminal: terminal.clone(),
        renewal_task: tokio::spawn(std::future::pending::<()>()),
        terminal_task: tokio::spawn(std::future::pending::<()>()),
        fence_task: None,
    };

    assert!(process_lease.is_live());
    terminal.cancel();
    assert!(!process_lease.is_live());
    assert!(!process_lease.disarm_for_shutdown());
}

#[tokio::test]
async fn intentional_process_lease_disarm_cannot_run_the_loss_fence() {
    let node = NodeId(49);
    let owner = uuid::Uuid::from_u128(499);
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let acquired = acquire_test_process_lease(store, node, owner, 10_000).await;
    let deadline = live_test_process_deadline();
    let deadline_observer = Arc::clone(&deadline);
    let (_live_tx, live_rx) = watch::channel(true);
    let terminal = tokio_util::sync::CancellationToken::new();
    let terminal_observer = terminal.clone();
    let terminal_task = spawn_process_lease_terminal_monitor(
        live_rx.clone(),
        Arc::clone(&deadline),
        terminal.clone(),
    );
    let terminal_abort = terminal_task.abort_handle();
    let fence_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fence_terminal = terminal.clone();
    let fence_ran_task = Arc::clone(&fence_ran);
    let fence_task = tokio::spawn(async move {
        fence_terminal.cancelled().await;
        fence_ran_task.store(true, std::sync::atomic::Ordering::Release);
    });
    let fence_abort = fence_task.abort_handle();
    let renewal_task = tokio::spawn(std::future::pending::<()>());
    let renewal_abort = renewal_task.abort_handle();
    let mut process_lease = ProcessLeaseRuntime {
        acquired,
        deadline,
        live_rx,
        shutdown: tokio_util::sync::CancellationToken::new(),
        terminal,
        renewal_task,
        terminal_task,
        fence_task: Some(fence_task),
    };

    assert!(process_lease.disarm_for_shutdown());
    assert!(!deadline_observer.is_live());
    assert!(terminal_observer.is_cancelled());

    let tasks = [terminal_abort, fence_abort, renewal_abort];
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while tasks.iter().any(|task| !task.is_finished()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("disarmed process lease tasks must terminate");
    assert!(!fence_ran.load(std::sync::atomic::Ordering::Acquire));
}

#[tokio::test]
async fn rebalance_tasks_receive_a_graceful_shutdown_before_abort() {
    let shutdown = tokio_util::sync::CancellationToken::new();
    let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    shutdown.cancel();
    let task_shutdown = shutdown.clone();
    let task_stopped = Arc::clone(&stopped);
    let task = tokio::spawn(async move {
        tokio::task::yield_now().await;
        task_shutdown.cancelled().await;
        task_stopped.store(true, std::sync::atomic::Ordering::Release);
    });
    let mut tasks = vec![task];

    assert!(stop_rebalance_tasks(&mut tasks, &shutdown).await);

    assert!(tasks.is_empty());
    assert!(stopped.load(std::sync::atomic::Ordering::Acquire));
}

#[tokio::test]
async fn bootstrap_rebalance_stop_joins_every_task_after_abort() {
    struct DropFlag(Arc<std::sync::atomic::AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Release);
        }
    }

    let shutdown = tokio_util::sync::CancellationToken::new();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let task_dropped = Arc::clone(&dropped);
    let pending = tokio::spawn(async move {
        let _drop_flag = DropFlag(task_dropped);
        let _ = started_tx.send(());
        std::future::pending::<()>().await;
    });
    started_rx.await.unwrap();

    let failed = tokio::spawn(async {
        panic!("intentional bootstrap task failure");
    });
    while !failed.is_finished() {
        tokio::task::yield_now().await;
    }
    let mut tasks = vec![failed, pending];

    assert!(
        !stop_bootstrap_rebalance_tasks(&mut tasks, &shutdown, std::time::Duration::ZERO).await
    );
    assert!(tasks.is_empty());
    assert!(dropped.load(std::sync::atomic::Ordering::Acquire));
}

#[tokio::test]
async fn leader_lease_runtime_cancels_and_joins_its_task() {
    let shutdown = tokio_util::sync::CancellationToken::new();
    let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let task_shutdown = shutdown.clone();
    let task_stopped = Arc::clone(&stopped);
    let task = tokio::spawn(async move {
        task_shutdown.cancelled().await;
        task_stopped.store(true, std::sync::atomic::Ordering::Release);
    });
    let mut runtime = LeaderLeaseRuntime::new(shutdown, task);

    runtime.stop().await;

    assert!(stopped.load(std::sync::atomic::Ordering::Acquire));
    assert!(runtime.task.is_none());
}

#[tokio::test]
async fn dropping_cluster_handle_fences_authority_and_aborts_owned_tasks() {
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};

    let node = NodeId(47);
    let boot = uuid::Uuid::from_u128(477);
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let snapshot_store =
        Arc::new(laminar_core::cluster::control::AssignmentSnapshotStore::new(Arc::clone(&store)));
    let acquired = acquire_test_process_lease(Arc::clone(&store), node, boot, 10_000).await;
    let control: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let participant = laminar_core::checkpoint::CheckpointParticipant {
        node_id: node.0,
        boot_incarnation: boot,
    };
    let verified_namespaces = laminar_core::cluster::control::prove_shared_object_store_namespaces(
        participant,
        &[participant],
        Arc::clone(&control),
        Arc::clone(&store),
        std::time::Duration::from_secs(1),
    )
    .await
    .unwrap();
    let vnode_registry = Arc::new(laminar_core::state::VnodeRegistry::new(1));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node,
        Arc::clone(&control),
        control,
        Some(Arc::clone(&snapshot_store)),
        members_rx,
        boot,
    ));
    let deadline = live_test_process_deadline();
    controller
        .set_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .verified_cluster_namespaces(verified_namespaces)
        .vnode_registry(vnode_registry)
        .build()
        .await
        .unwrap();
    let serving_gate = Arc::new(crate::http::ServingGate::starting());
    assert!(serving_gate.open());

    let local_node = NodeInfo {
        id: node,
        name: "drop-test".into(),
        rpc_address: "127.0.0.1:0".into(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let discovery = DiscoveryImpl::Static(StaticDiscovery::new(StaticDiscoveryConfig {
        local_node: local_node.clone(),
        seeds: vec!["127.0.0.1:1".into()],
        heartbeat_interval: std::time::Duration::from_secs(1),
        suspect_threshold: 3,
        dead_threshold: 10,
        listen_address: "127.0.0.1:0".into(),
        process_generation: acquired.term,
        process_incarnation: boot,
    }));

    let pending_task = || tokio::spawn(std::future::pending::<()>());
    let api_handle = pending_task();
    let api_abort = api_handle.abort_handle();
    let watcher_handle = pending_task();
    let watcher_abort = watcher_handle.abort_handle();
    let membership_handle = pending_task();
    let membership_abort = membership_handle.abort_handle();
    let rebalance_task = pending_task();
    let rebalance_abort = rebalance_task.abort_handle();

    let leader_shutdown = tokio_util::sync::CancellationToken::new();
    let leader_task_shutdown = leader_shutdown.clone();
    let leader_task = tokio::spawn(async move {
        leader_task_shutdown.cancelled().await;
    });
    let leader_abort = leader_task.abort_handle();
    let leader_lease = LeaderLeaseRuntime::new(leader_shutdown.clone(), leader_task);

    let (live_tx, live_rx) = watch::channel(true);
    let process_terminal = tokio_util::sync::CancellationToken::new();
    let process_terminal_observer = process_terminal.clone();
    let process_deadline_observer = Arc::clone(&deadline);
    let terminal_task = spawn_process_lease_terminal_monitor(
        live_rx.clone(),
        Arc::clone(&deadline),
        process_terminal.clone(),
    );
    let terminal_abort = terminal_task.abort_handle();
    let renewal_task = pending_task();
    let renewal_abort = renewal_task.abort_handle();
    let process_lease = ProcessLeaseRuntime {
        acquired,
        deadline,
        live_rx,
        shutdown: tokio_util::sync::CancellationToken::new(),
        terminal: process_terminal,
        renewal_task,
        terminal_task,
        fence_task: None,
    };

    let handle = ClusterHandle {
        db: Arc::clone(&db),
        db_shutdown_complete: false,
        discovery,
        serving_gate: Arc::clone(&serving_gate),
        api_handle,
        watcher_handle: Some(watcher_handle),
        membership_handle,
        local_node,
        cluster_controller: Arc::clone(&controller),
        snapshot_store,
        vnode_count: 1,
        leader_lease,
        process_lease,
        rebalance_tasks: vec![rebalance_task],
        rebalance_shutdown: tokio_util::sync::CancellationToken::new(),
    };
    assert!(controller.process_lease_is_live());

    drop(handle);
    assert!(!process_deadline_observer.is_live());
    assert!(process_terminal_observer.is_cancelled());
    drop(live_tx);

    assert!(!serving_gate.open());
    assert!(!controller.process_lease_is_live());
    assert!(db.cluster_intake_fenced());
    assert!(leader_shutdown.is_cancelled());
    let tasks = [
        api_abort,
        watcher_abort,
        membership_abort,
        rebalance_abort,
        leader_abort,
        terminal_abort,
        renewal_abort,
    ];
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while tasks.iter().any(|task| !task.is_finished()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned runtime tasks must terminate");
}

#[tokio::test]
async fn process_lease_loss_revokes_http_controller_and_database_authority() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};

    let node = NodeId(41);
    let boot = uuid::Uuid::from_u128(411);
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let acquired = acquire_test_process_lease(Arc::clone(&store), node, boot, 10_000).await;
    let control: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let participant = CheckpointParticipant {
        node_id: node.0,
        boot_incarnation: boot,
    };
    let verified_namespaces = laminar_core::cluster::control::prove_shared_object_store_namespaces(
        participant,
        &[participant],
        Arc::clone(&control),
        Arc::clone(&store),
        std::time::Duration::from_secs(1),
    )
    .await
    .unwrap();
    let vnode_registry = Arc::new(laminar_core::state::VnodeRegistry::new(1));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node,
        Arc::clone(&control),
        control,
        None,
        members_rx,
        boot,
    ));
    let deadline = live_test_process_deadline();
    controller
        .set_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    let fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[node.0],
        vec![CheckpointParticipant {
            node_id: node.0,
            boot_incarnation: boot,
        }],
    )
    .unwrap();
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));

    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .verified_cluster_namespaces(verified_namespaces)
        .vnode_registry(vnode_registry)
        .build()
        .await
        .unwrap();
    assert!(controller.process_lease_is_live());
    let serving_gate = Arc::new(crate::http::ServingGate::starting());
    assert!(serving_gate.open());
    let (live_tx, live_rx) = watch::channel(true);
    let terminal = tokio_util::sync::CancellationToken::new();
    let terminal_task = spawn_process_lease_terminal_monitor(
        live_rx.clone(),
        Arc::clone(&deadline),
        terminal.clone(),
    );
    let mut process_lease = ProcessLeaseRuntime {
        acquired,
        deadline,
        live_rx,
        shutdown: tokio_util::sync::CancellationToken::new(),
        terminal,
        renewal_task: tokio::spawn(std::future::pending()),
        terminal_task,
        fence_task: None,
    };
    let terminal = process_lease.terminal_token();
    let leader_shutdown = tokio_util::sync::CancellationToken::new();
    process_lease.install_fence(
        Arc::clone(&db),
        Arc::clone(&controller),
        Arc::clone(&serving_gate),
        leader_shutdown.clone(),
    );

    live_tx.send_replace(false);
    let fence_task = process_lease.fence_task.take().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), fence_task)
        .await
        .expect("process fence must run promptly")
        .unwrap();

    assert!(!controller.process_lease_is_live());
    assert_eq!(controller.checkpoint_assignment_fence(1), None);
    assert!(db.cluster_intake_fenced());
    assert!(!serving_gate.open());
    assert!(leader_shutdown.is_cancelled());
    assert!(terminal.is_cancelled());
}

#[tokio::test]
async fn occupied_http_port_fails_before_local_cluster_activation() {
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};

    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = occupied.local_addr().unwrap().to_string();
    let node = NodeId(41);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let assignment_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let snapshot_store =
        Arc::new(laminar_core::cluster::control::AssignmentSnapshotStore::new(assignment_store));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(
        node,
        kv,
        Some(Arc::clone(&snapshot_store)),
        members_rx,
    ));
    controller.set_active(false);

    let server_config = crate::config::ServerSection {
        bind,
        ..Default::default()
    };
    let config = ServerConfig {
        server: server_config,
        checkpoint: crate::config::CheckpointSection::default(),
        supervision: Default::default(),
        sources: Vec::new(),
        lookups: Vec::new(),
        pipelines: Vec::new(),
        sinks: Vec::new(),
        sql: None,
        discovery: None,
        node_id: None,
        ai: Default::default(),
        models: Default::default(),
    };
    let registry = Arc::new(crate::metrics::build_registry([
        ("instance".into(), "test".into()),
        ("pipeline".into(), "test".into()),
    ]));
    let (_cluster_members_tx, cluster_members_rx) = watch::channel(Vec::new());
    let cluster = crate::http::ClusterComponents {
        controller: Arc::clone(&controller),
        snapshot_store,
        membership_rx: cluster_members_rx,
    };

    let result = start_cluster_http_api_before_activation(
        LaminarDB::open().unwrap(),
        registry,
        PathBuf::from("unused.toml"),
        config,
        Arc::new(crate::http::ServingGate::starting()),
        cluster,
    )
    .await;
    let error = match result {
        Ok(_) => panic!("occupied HTTP port unexpectedly bound"),
        Err(error) => error,
    };
    assert!(matches!(error, ClusterStartupError::HttpStartup(_)));
    assert!(!controller.live_instances().contains(&node));
}

#[tokio::test]
async fn static_cluster_kv_serves_reads_and_scans_from_membership_metadata() {
    use laminar_core::cluster::control::ClusterKv as _;

    fn tagged(node: u64, key: &str, value: &str) -> NodeInfo {
        let mut metadata = NodeMetadata::default();
        metadata.tags.insert(key.to_string(), value.to_string());
        NodeInfo {
            id: NodeId(node),
            name: format!("node-{node}"),
            rpc_address: String::new(),
            state: NodeState::Active,
            metadata,
            last_heartbeat_ms: 0,
        }
    }

    let (membership_tx, membership_rx) = watch::channel(vec![
        tagged(7, "shuffle", "127.0.0.1:7007"),
        tagged(9, "unrelated", "value"),
    ]);
    let kv = StaticClusterKv::new(membership_rx);

    assert_eq!(
        kv.read_from(NodeId(7), "shuffle").await.as_deref(),
        Some("127.0.0.1:7007")
    );
    assert_eq!(kv.read_from(NodeId(9), "shuffle").await, None);
    assert_eq!(kv.read_from(NodeId(1234), "shuffle").await, None);
    assert_eq!(
        kv.scan("shuffle").await,
        vec![(NodeId(7), "127.0.0.1:7007".to_string())]
    );

    kv.write("shuffle", "ignored".to_string()).await;
    assert_eq!(
        kv.read_from(NodeId(7), "shuffle").await.as_deref(),
        Some("127.0.0.1:7007"),
        "static discovery KV writes must remain no-ops"
    );

    membership_tx
        .send(vec![tagged(7, "shuffle", "127.0.0.1:7008")])
        .unwrap();
    assert_eq!(
        kv.read_from(NodeId(7), "shuffle").await.as_deref(),
        Some("127.0.0.1:7008"),
        "reads must track the current membership snapshot"
    );
}

#[tokio::test]
async fn stopping_discovery_releases_the_static_listener() {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen_port = probe.local_addr().unwrap().port();
    drop(probe);

    let local_node = NodeInfo {
        id: NodeId(51),
        name: "stop-test".into(),
        rpc_address: "127.0.0.1:0".into(),
        state: NodeState::Joining,
        metadata: NodeMetadata {
            version: env!("CARGO_PKG_VERSION").to_string(),
            ..NodeMetadata::default()
        },
        last_heartbeat_ms: 0,
    };
    let mut discovery = DiscoveryImpl::Static(StaticDiscovery::new(StaticDiscoveryConfig {
        local_node,
        seeds: vec![format!("127.0.0.1:{listen_port}")],
        heartbeat_interval: std::time::Duration::from_secs(1),
        suspect_threshold: 3,
        dead_threshold: 10,
        listen_address: format!("127.0.0.1:{listen_port}"),
        process_generation: 1,
        process_incarnation: uuid::Uuid::new_v4(),
    }));
    discovery.start().await.unwrap();

    assert!(stop_discovery_with_bound(&mut discovery).await);

    std::net::TcpListener::bind(format!("127.0.0.1:{listen_port}"))
        .expect("the static discovery listener must be released after stop");
}

#[tokio::test]
async fn object_store_control_kv_survives_reconstruction() {
    use laminar_core::cluster::control::ClusterKv;

    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let lease = acquire_test_process_lease(
        Arc::clone(&store),
        NodeId(7),
        uuid::Uuid::from_u128(71),
        1_000,
    )
    .await;
    let first = ObjectStoreClusterKv::new(
        lease.clone(),
        live_test_process_deadline(),
        1_000,
        Arc::clone(&store),
        members_rx.clone(),
    );
    first
        .write_checked("control:recover", "release-13".into())
        .await
        .unwrap();
    drop(first);

    let replacement = ObjectStoreClusterKv::new(
        lease,
        live_test_process_deadline(),
        1_000,
        store,
        members_rx,
    );
    assert_eq!(
        replacement.read_from(NodeId(7), "control:recover").await,
        Some("release-13".into())
    );
    replacement
        .write_checked("control:recover", "release-14".into())
        .await
        .unwrap();
    assert_eq!(
        replacement.read_from(NodeId(7), "control:recover").await,
        Some("release-14".into())
    );
}

#[tokio::test]
async fn object_store_control_kv_rejects_oversized_values_before_body_read() {
    use laminar_core::cluster::control::ClusterKv;

    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let lease = acquire_test_process_lease(
        Arc::clone(&store),
        NodeId(7),
        uuid::Uuid::from_u128(71),
        1_000,
    )
    .await;
    let oversized = usize::try_from(OBJECT_STORE_CONTROL_MAX_ENVELOPE_BYTES + 1).unwrap();
    store
        .put(
            &object_store_control_record_path(&lease, "control:oversized", 1),
            object_store::PutPayload::from(bytes::Bytes::from(vec![0; oversized])),
        )
        .await
        .unwrap();
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let kv = ObjectStoreClusterKv::new(
        lease,
        live_test_process_deadline(),
        1_000,
        store,
        members_rx,
    );
    let error = kv
        .read_from_checked(NodeId(7), "control:oversized")
        .await
        .unwrap_err();
    assert!(error.contains("maximum"), "{error}");
}

#[tokio::test]
async fn object_store_control_kv_rejects_write_after_local_lease_deadline_expires() {
    use laminar_core::cluster::control::ClusterKv;

    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let lease = acquire_test_process_lease(
        Arc::clone(&store),
        NodeId(7),
        uuid::Uuid::from_u128(71),
        1_000,
    )
    .await;
    let kv = ObjectStoreClusterKv::new(
        lease.clone(),
        Arc::new(laminar_core::cluster::control::LeaseDeadline::fenced()),
        1_000,
        Arc::clone(&store),
        members_rx,
    );
    let error = kv
        .write_checked("control:recover", "must-not-publish".into())
        .await
        .unwrap_err();
    assert!(error.contains("deadline expired"), "{error}");
    assert!(list_control_sequences(
        &store,
        &object_store_control_key_prefix(&lease, "control:recover")
    )
    .await
    .unwrap()
    .is_empty());
}

#[tokio::test]
async fn object_store_control_kv_ignores_delayed_previous_term_write() {
    use laminar_core::cluster::control::ClusterKv;

    let inner: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let delayed = Arc::new(DelayedControlPutStore::new(Arc::clone(&inner)));
    let store: Arc<dyn object_store::ObjectStore> = delayed.clone();
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let ttl_ms = 1;
    let first_lease = acquire_test_process_lease(
        Arc::clone(&store),
        NodeId(7),
        uuid::Uuid::from_u128(71),
        ttl_ms,
    )
    .await;
    let first = Arc::new(ObjectStoreClusterKv::new(
        first_lease.clone(),
        live_test_process_deadline(),
        ttl_ms,
        Arc::clone(&store),
        members_rx.clone(),
    ));
    let stale_path = object_store_control_record_path(&first_lease, "control:recover", 1);
    delayed.block_once(stale_path.clone());
    let stale_writer = {
        let first = Arc::clone(&first);
        tokio::spawn(async move { first.write_checked("control:recover", "stale".into()).await })
    };
    delayed.wait_until_blocked().await;
    stale_writer.abort();
    let _ = stale_writer.await;

    let replacement_lease = take_over_test_process_lease(
        Arc::clone(&store),
        &first_lease,
        uuid::Uuid::from_u128(72),
        ttl_ms,
    )
    .await;
    let replacement = ObjectStoreClusterKv::new(
        replacement_lease,
        live_test_process_deadline(),
        ttl_ms,
        Arc::clone(&store),
        members_rx,
    );
    replacement
        .write_checked("control:recover", "current".into())
        .await
        .unwrap();

    delayed.release();
    delayed.wait_until_completed().await;
    assert!(inner.get(&stale_path).await.is_ok());
    assert_eq!(
        replacement.read_from(NodeId(7), "control:recover").await,
        Some("current".into())
    );
}

#[tokio::test]
async fn superseded_process_phase_cannot_clobber_replacement_release() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::cluster::control::{
        ClusterController, ClusterKv, LeaderLeaseOwner, LeaderLeaseStore, LeaseOutcome,
        ProcessLeaseAuthority, RecoverPhase, RecoveryAnnouncement, RecoveryFaultReportOutcome,
        RecoveryRound,
    };

    let inner: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let delayed = Arc::new(DelayedControlPutStore::new(Arc::clone(&inner)));
    let store: Arc<dyn object_store::ObjectStore> = delayed.clone();
    let node = NodeId(7);
    let ttl_ms = 1;
    let process_authority = Arc::new(
        ProcessLeaseAuthority::new(
            Arc::clone(&store),
            std::time::Duration::from_millis(u64::try_from(ttl_ms).unwrap()),
        )
        .unwrap(),
    );
    let old_boot = uuid::Uuid::from_u128(71);
    let old_process = acquire_test_process_lease(Arc::clone(&store), node, old_boot, ttl_ms).await;
    let old_deadline = live_test_process_deadline();
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let old_kv_impl = Arc::new(ObjectStoreClusterKv::new(
        old_process.clone(),
        Arc::clone(&old_deadline),
        ttl_ms,
        Arc::clone(&store),
        members_rx.clone(),
    ));
    let old_kv: Arc<dyn ClusterKv> = old_kv_impl;
    let old_controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node,
        Arc::clone(&old_kv),
        old_kv,
        None,
        members_rx.clone(),
        old_boot,
    ));
    old_controller
        .set_process_lease_deadline(Arc::clone(&old_deadline))
        .unwrap();
    old_controller
        .set_process_lease_authority(Arc::clone(&process_authority))
        .unwrap();
    old_controller
        .publish_leased_recovery_incarnation(&old_process)
        .await
        .unwrap();

    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&store), ttl_ms));
    let old_owner = LeaderLeaseOwner {
        node,
        boot: old_process.owner,
        process_term: old_process.term,
    };
    let LeaseOutcome::Acquired(old_leader) = authority.begin_new_term(&old_owner, 0).await.unwrap()
    else {
        panic!("old process must acquire empty leader authority");
    };
    let (_old_leader_tx, old_leader_rx) = watch::channel(Some(old_leader.clone()));
    old_controller
        .set_leader_lease_watch(old_leader_rx, old_owner, old_deadline)
        .unwrap();
    old_controller.set_leader_lease_store(Arc::clone(&authority));
    let old_fault_request = old_controller.next_recovery_fault_request().unwrap();
    assert_eq!(
        old_controller
            .report_fault(old_fault_request)
            .await
            .unwrap(),
        RecoveryFaultReportOutcome::Active
    );
    let old_fault_inventory = old_controller
        .read_recovery_fault_inventory()
        .await
        .unwrap();

    let old_participant = CheckpointParticipant {
        node_id: node.0,
        boot_incarnation: old_boot,
    };
    let old_round = RecoveryRound::new(
        61,
        old_leader.proof(),
        CheckpointAssignmentFence::from_owner_map(7, &[node.0], vec![old_participant]).unwrap(),
        Vec::new(),
        old_fault_inventory.revision(),
        old_fault_inventory.faults().to_vec(),
    )
    .unwrap();
    old_controller.publish_checkpoint_assignment_fence(Some(old_round.assignment_fence.clone()));
    old_controller
        .announce_recover_prepare(&old_round)
        .await
        .unwrap();
    old_controller
        .announce_recover_start(&old_round, 4)
        .await
        .unwrap();

    let stale_path = object_store_control_record_path(&old_process, "control:recover", 3);
    delayed.block_once(stale_path.clone());
    let stale_release = {
        let controller = Arc::clone(&old_controller);
        let round = old_round.clone();
        tokio::spawn(async move { controller.announce_recover_release(&round, 4).await })
    };
    delayed.wait_until_blocked().await;

    let replacement_boot = uuid::Uuid::from_u128(72);
    let replacement_process =
        take_over_test_process_lease(Arc::clone(&store), &old_process, replacement_boot, ttl_ms)
            .await;
    let replacement_owner = LeaderLeaseOwner {
        node,
        boot: replacement_process.owner,
        process_term: replacement_process.term,
    };
    let leader_observation = authority
        .observe_rival(&replacement_owner, &old_leader)
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(3)).await;
    let LeaseOutcome::Acquired(replacement_leader) = authority
        .try_takeover(&replacement_owner, &leader_observation, 10)
        .await
        .unwrap()
    else {
        panic!("replacement process must take over leader authority");
    };

    let replacement_deadline = live_test_process_deadline();
    let replacement_kv_impl = Arc::new(ObjectStoreClusterKv::new(
        replacement_process.clone(),
        Arc::clone(&replacement_deadline),
        ttl_ms,
        Arc::clone(&store),
        members_rx.clone(),
    ));
    let replacement_kv: Arc<dyn ClusterKv> = replacement_kv_impl;
    let replacement_controller = ClusterController::new_with_recovery_incarnation(
        node,
        Arc::clone(&replacement_kv),
        replacement_kv,
        None,
        members_rx,
        replacement_boot,
    );
    replacement_controller
        .set_process_lease_deadline(Arc::clone(&replacement_deadline))
        .unwrap();
    replacement_controller
        .set_process_lease_authority(process_authority)
        .unwrap();
    replacement_controller
        .publish_leased_recovery_incarnation(&replacement_process)
        .await
        .unwrap();
    let (_replacement_leader_tx, replacement_leader_rx) =
        watch::channel(Some(replacement_leader.clone()));
    replacement_controller
        .set_leader_lease_watch(
            replacement_leader_rx,
            replacement_owner,
            replacement_deadline,
        )
        .unwrap();
    replacement_controller.set_leader_lease_store(authority);
    let replacement_fault_request = replacement_controller
        .next_recovery_fault_request()
        .unwrap();
    assert_eq!(
        replacement_controller
            .report_fault(replacement_fault_request)
            .await
            .unwrap(),
        RecoveryFaultReportOutcome::Active
    );
    let replacement_fault_inventory = replacement_controller
        .read_recovery_fault_inventory()
        .await
        .unwrap();

    let replacement_participant = CheckpointParticipant {
        node_id: node.0,
        boot_incarnation: replacement_boot,
    };
    let replacement_round = RecoveryRound::new(
        62,
        replacement_leader.proof(),
        CheckpointAssignmentFence::from_owner_map(8, &[node.0], vec![replacement_participant])
            .unwrap(),
        Vec::new(),
        replacement_fault_inventory.revision(),
        replacement_fault_inventory.faults().to_vec(),
    )
    .unwrap();
    replacement_controller
        .publish_checkpoint_assignment_fence(Some(replacement_round.assignment_fence.clone()));
    replacement_controller
        .announce_recover_prepare(&replacement_round)
        .await
        .unwrap();
    replacement_controller
        .announce_recover_start(&replacement_round, 5)
        .await
        .unwrap();
    replacement_controller
        .announce_recover_release(&replacement_round, 5)
        .await
        .unwrap();
    let replacement_release = RecoveryAnnouncement {
        round: replacement_round,
        phase: RecoverPhase::Release { epoch: 5 },
    };
    assert_eq!(
        replacement_controller.observe_recover().await.unwrap(),
        Some(replacement_release.clone())
    );

    delayed.release();
    delayed.wait_until_completed().await;
    let stale_error = stale_release.await.unwrap().unwrap_err();
    assert!(
        stale_error.contains("local process lease owner or term changed"),
        "{stale_error}"
    );
    assert!(inner.get(&stale_path).await.is_ok());
    assert_eq!(
        replacement_controller.observe_recover().await.unwrap(),
        Some(replacement_release)
    );
}

#[tokio::test]
async fn object_store_control_kv_delayed_lower_sequence_cannot_regress_same_term() {
    use laminar_core::cluster::control::ClusterKv;

    let inner: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let delayed = Arc::new(DelayedControlPutStore::new(Arc::clone(&inner)));
    let store: Arc<dyn object_store::ObjectStore> = delayed.clone();
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let lease = acquire_test_process_lease(
        Arc::clone(&store),
        NodeId(7),
        uuid::Uuid::from_u128(71),
        1_000,
    )
    .await;
    let kv = Arc::new(ObjectStoreClusterKv::new(
        lease.clone(),
        live_test_process_deadline(),
        1_000,
        Arc::clone(&store),
        members_rx.clone(),
    ));
    let delayed_path = object_store_control_record_path(&lease, "control:recover", 1);
    delayed.block_once(delayed_path.clone());
    let delayed_writer = {
        let kv = Arc::clone(&kv);
        tokio::spawn(async move {
            kv.write_checked("control:recover", "sequence-1".into())
                .await
        })
    };
    delayed.wait_until_blocked().await;
    delayed_writer.abort();
    let _ = delayed_writer.await;

    kv.write_checked("control:recover", "sequence-2".into())
        .await
        .unwrap();
    delayed.release();
    delayed.wait_until_completed().await;
    assert!(inner.get(&delayed_path).await.is_ok());
    assert_eq!(
        kv.read_from(NodeId(7), "control:recover").await,
        Some("sequence-2".into())
    );
}

#[tokio::test]
async fn object_store_control_kv_revalidates_lease_after_record_body_read() {
    use laminar_core::cluster::control::ClusterKv;

    let inner: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let delayed = Arc::new(DelayedControlPutStore::new(Arc::clone(&inner)));
    let store: Arc<dyn object_store::ObjectStore> = delayed.clone();
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let ttl_ms = 1;
    let first_lease = acquire_test_process_lease(
        Arc::clone(&store),
        NodeId(7),
        uuid::Uuid::from_u128(71),
        ttl_ms,
    )
    .await;
    let first = Arc::new(ObjectStoreClusterKv::new(
        first_lease.clone(),
        live_test_process_deadline(),
        ttl_ms,
        Arc::clone(&store),
        members_rx,
    ));
    first
        .write_checked("control:recover", "old-term".into())
        .await
        .unwrap();
    delayed.block_get_once(object_store_control_record_path(
        &first_lease,
        "control:recover",
        1,
    ));
    let reader = {
        let first = Arc::clone(&first);
        tokio::spawn(async move { first.read_from_checked(NodeId(7), "control:recover").await })
    };
    delayed.wait_until_get_blocked().await;
    let _replacement = take_over_test_process_lease(
        Arc::clone(&store),
        &first_lease,
        uuid::Uuid::from_u128(72),
        ttl_ms,
    )
    .await;
    delayed.release_get();
    let error = reader.await.unwrap().unwrap_err();
    assert!(error.contains("changed during control read"), "{error}");
}

#[tokio::test]
async fn object_store_control_kv_stale_local_term_cannot_read_or_scan() {
    use laminar_core::cluster::control::ClusterKv;

    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let ttl_ms = 1;
    let first_lease = acquire_test_process_lease(
        Arc::clone(&store),
        NodeId(7),
        uuid::Uuid::from_u128(71),
        ttl_ms,
    )
    .await;
    let first = ObjectStoreClusterKv::new(
        first_lease.clone(),
        live_test_process_deadline(),
        ttl_ms,
        Arc::clone(&store),
        members_rx,
    );
    first
        .write_checked("control:recover", "old-term".into())
        .await
        .unwrap();
    let _replacement = take_over_test_process_lease(
        Arc::clone(&store),
        &first_lease,
        uuid::Uuid::from_u128(72),
        ttl_ms,
    )
    .await;

    let read_error = first
        .read_from_checked(NodeId(7), "control:recover")
        .await
        .unwrap_err();
    assert!(read_error.contains("owner or term changed"), "{read_error}");
    let scan_error = first.scan_checked("control:recover").await.unwrap_err();
    assert!(scan_error.contains("owner or term changed"), "{scan_error}");
}

#[tokio::test]
async fn object_store_control_scan_bounds_concurrency_and_preserves_order() {
    use laminar_core::cluster::control::ClusterKv;

    let inner: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let delayed = Arc::new(DelayedControlPutStore::new(inner));
    let store: Arc<dyn object_store::ObjectStore> = delayed.clone();
    let (_empty_tx, empty_rx) = watch::channel(Vec::new());
    let node_count = OBJECT_STORE_CONTROL_SCAN_CONCURRENCY + 5;
    let mut leases = Vec::with_capacity(node_count);

    for raw_id in 1..=node_count {
        let id = NodeId(u64::try_from(raw_id).unwrap());
        let lease = acquire_test_process_lease(
            Arc::clone(&store),
            id,
            uuid::Uuid::from_u128(raw_id as u128),
            60_000,
        )
        .await;
        let writer = ObjectStoreClusterKv::new(
            lease.clone(),
            live_test_process_deadline(),
            60_000,
            Arc::clone(&store),
            empty_rx.clone(),
        );
        writer
            .write_checked("control:test-scan", format!("node-{raw_id}"))
            .await
            .unwrap();
        leases.push(lease);
    }

    let members = (1..=node_count)
        .map(|raw_id| NodeInfo {
            id: NodeId(u64::try_from(raw_id).unwrap()),
            name: format!("node-{raw_id}"),
            rpc_address: String::new(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        })
        .collect();
    let (_members_tx, members_rx) = watch::channel(members);
    let scanner = ObjectStoreClusterKv::new(
        leases[0].clone(),
        live_test_process_deadline(),
        60_000,
        store,
        members_rx,
    );

    delayed.begin_get_concurrency_probe();
    let results = scanner.scan_checked("control:test-scan").await.unwrap();
    let max_gets = delayed.finish_get_concurrency_probe();

    assert_eq!(results.len(), node_count);
    assert_eq!(
        results.iter().map(|(id, _)| id.0).collect::<Vec<_>>(),
        (1..=u64::try_from(node_count).unwrap()).collect::<Vec<_>>()
    );
    assert!(max_gets > 1, "the probe must observe concurrent reads");
    assert!(
        max_gets <= OBJECT_STORE_CONTROL_SCAN_CONCURRENCY,
        "observed {max_gets} concurrent GETs"
    );
}

#[tokio::test]
async fn object_store_control_kv_rejects_noncanonical_record_body() {
    use laminar_core::cluster::control::ClusterKv;

    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let lease = acquire_test_process_lease(
        Arc::clone(&store),
        NodeId(7),
        uuid::Uuid::from_u128(71),
        1_000,
    )
    .await;
    let record = ObjectStoreControlRecord {
        version: OBJECT_STORE_CONTROL_VERSION,
        node: lease.node.0,
        owner: lease.owner,
        term: lease.term,
        sequence: 1,
        key: "control:recover".into(),
        value: "release".into(),
    };
    store
        .put(
            &object_store_control_record_path(&lease, &record.key, record.sequence),
            object_store::PutPayload::from(bytes::Bytes::from(
                serde_json::to_vec_pretty(&record).unwrap(),
            )),
        )
        .await
        .unwrap();
    let kv = ObjectStoreClusterKv::new(
        lease,
        live_test_process_deadline(),
        1_000,
        store,
        members_rx,
    );
    let error = kv
        .read_from_checked(NodeId(7), "control:recover")
        .await
        .unwrap_err();
    assert!(error.contains("canonically encoded"), "{error}");
}

#[tokio::test]
async fn object_store_control_kv_prunes_history_but_retains_highest_two() {
    use laminar_core::cluster::control::ClusterKv;

    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let lease = acquire_test_process_lease(
        Arc::clone(&store),
        NodeId(7),
        uuid::Uuid::from_u128(71),
        1_000,
    )
    .await;
    let kv = ObjectStoreClusterKv::new(
        lease.clone(),
        live_test_process_deadline(),
        1_000,
        Arc::clone(&store),
        members_rx.clone(),
    );
    for sequence in 1..=5 {
        kv.write_checked("control:recover", format!("value-{sequence}"))
            .await
            .unwrap();
    }
    let prefix = object_store_control_key_prefix(&lease, "control:recover");
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if list_control_sequences(&store, &prefix).await.unwrap() == [4, 5] {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        kv.read_from(NodeId(7), "control:recover").await,
        Some("value-5".into())
    );
    let reconstructed = ObjectStoreClusterKv::new(
        lease,
        live_test_process_deadline(),
        1_000,
        Arc::clone(&store),
        members_rx,
    );
    reconstructed
        .write_checked("control:recover", "value-6".into())
        .await
        .unwrap();
    assert_eq!(
        reconstructed.read_from(NodeId(7), "control:recover").await,
        Some("value-6".into())
    );
}

#[test]
fn object_store_control_kv_prune_selection_is_order_independent() {
    let prefix = "cluster-control-kv/v2/test/";
    let mut oldest = BinaryHeap::new();
    for index in 0..300u64 {
        let sequence = (index * 73) % 300 + 1;
        let path = object_store::path::Path::from(format!("{prefix}v{sequence:020}.json"));
        retain_oldest_control_record(&mut oldest, sequence, &path);
    }
    assert_eq!(
        oldest
            .into_sorted_vec()
            .into_iter()
            .map(|(sequence, _)| sequence)
            .collect::<Vec<_>>(),
        (1..=u64::try_from(OBJECT_STORE_CONTROL_PRUNE_BATCH_RECORDS).unwrap()).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn object_store_control_kv_pruning_coalesces_per_prefix() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let lease = acquire_test_process_lease(
        Arc::clone(&store),
        NodeId(7),
        uuid::Uuid::from_u128(71),
        1_000,
    )
    .await;
    let kv = ObjectStoreClusterKv::new(
        lease.clone(),
        live_test_process_deadline(),
        1_000,
        store,
        members_rx,
    );
    let key_prefix = object_store_control_key_prefix(&lease, "control:recover");
    let generation_prefix = RECOVERY_GENERATION_PREFIX.to_string();

    kv.schedule_prune(key_prefix.clone());
    kv.schedule_prune(key_prefix.clone());
    kv.schedule_prune(generation_prefix.clone());
    {
        let states = kv
            .prune_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(states.len(), 2);
        assert_eq!(states.get(&key_prefix), Some(&true));
        assert_eq!(states.get(&generation_prefix), Some(&false));
    }

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if kv
                .prune_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn recovery_generation_ignores_delayed_lower_marker_and_survives_new_term() {
    use laminar_core::cluster::control::ClusterKv;

    let inner: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let delayed = Arc::new(DelayedControlPutStore::new(inner));
    let store: Arc<dyn object_store::ObjectStore> = delayed.clone();
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let ttl_ms = 1;
    let first_lease = acquire_test_process_lease(
        Arc::clone(&store),
        NodeId(7),
        uuid::Uuid::from_u128(71),
        ttl_ms,
    )
    .await;
    let first = Arc::new(ObjectStoreClusterKv::new(
        first_lease.clone(),
        live_test_process_deadline(),
        ttl_ms,
        Arc::clone(&store),
        members_rx.clone(),
    ));
    delayed.block_once(recovery_generation_path(1));
    let delayed_writer = {
        let first = Arc::clone(&first);
        tokio::spawn(async move {
            first
                .write_checked(RECOVERY_GENERATION_KEY, "1".into())
                .await
        })
    };
    delayed.wait_until_blocked().await;
    delayed_writer.abort();
    let _ = delayed_writer.await;
    first
        .write_checked(RECOVERY_GENERATION_KEY, "2".into())
        .await
        .unwrap();
    delayed.release();
    delayed.wait_until_completed().await;
    assert_eq!(
        first.read_from(NodeId(7), RECOVERY_GENERATION_KEY).await,
        Some("2".into())
    );

    let replacement_lease = take_over_test_process_lease(
        Arc::clone(&store),
        &first_lease,
        uuid::Uuid::from_u128(72),
        ttl_ms,
    )
    .await;
    let replacement = ObjectStoreClusterKv::new(
        replacement_lease,
        live_test_process_deadline(),
        ttl_ms,
        store,
        members_rx,
    );
    assert_eq!(
        replacement
            .read_from(NodeId(7), RECOVERY_GENERATION_KEY)
            .await,
        Some("2".into())
    );
    replacement
        .write_checked(RECOVERY_GENERATION_KEY, "3".into())
        .await
        .unwrap();
    assert_eq!(
        replacement
            .read_from(NodeId(7), RECOVERY_GENERATION_KEY)
            .await,
        Some("3".into())
    );
}

#[tokio::test]
async fn failed_process_lease_candidate_cannot_overwrite_active_incarnation() {
    use laminar_core::cluster::control::{
        ClusterController, ClusterKv, InMemoryKv, LeaseDeadline, ProcessLeaseAuthority,
        ProcessLeaseOutcome,
    };

    let node = NodeId(7);
    let recovery_impl = Arc::new(InMemoryKv::new(node));
    let recovery: Arc<dyn ClusterKv> = recovery_impl.clone();
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let active_owner = uuid::Uuid::from_u128(1);
    let active = ClusterController::new_with_recovery_incarnation(
        node,
        Arc::clone(&recovery),
        Arc::clone(&recovery),
        None,
        members_rx.clone(),
        active_owner,
    );
    let process_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let process_authority = Arc::new(
        ProcessLeaseAuthority::new(process_store, std::time::Duration::from_millis(1_000)).unwrap(),
    );
    let lease_store = process_authority.store_for(node);
    let ProcessLeaseOutcome::Acquired(active_lease) =
        lease_store.try_acquire(active_owner, 0).await.unwrap()
    else {
        panic!("first process must acquire its stable identity");
    };
    active
        .set_process_lease_authority(Arc::clone(&process_authority))
        .unwrap();
    active
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(60),
        )))
        .unwrap();
    active
        .publish_leased_recovery_incarnation(&active_lease)
        .await
        .unwrap();

    let candidate_owner = uuid::Uuid::from_u128(2);
    let candidate = ClusterController::new_with_recovery_incarnation(
        node,
        Arc::clone(&recovery),
        Arc::clone(&recovery),
        None,
        members_rx,
        candidate_owner,
    );
    let ProcessLeaseOutcome::Held(incumbent) = lease_store
        .try_acquire(candidate_owner, 10_000)
        .await
        .unwrap()
    else {
        panic!("client wall time must not let a candidate steal the lease");
    };
    candidate
        .set_process_lease_authority(process_authority)
        .unwrap();
    candidate
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(60),
        )))
        .unwrap();
    assert!(candidate
        .publish_leased_recovery_incarnation(&incumbent)
        .await
        .is_err());
    assert_eq!(
        recovery_impl
            .read_from(node, "control:recovery-incarnation")
            .await,
        Some(active_owner.to_string())
    );
}

#[tokio::test]
async fn startup_uses_retained_committed_assignment_when_head_is_draining() {
    use std::collections::BTreeMap;

    use laminar_core::checkpoint::{CheckpointParticipant, LeaderProof, LeaderProofOwner};
    use laminar_core::cluster::control::{AssignmentSnapshot, AssignmentSnapshotStore};

    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let store = AssignmentSnapshotStore::new(object_store);
    let participant = CheckpointParticipant {
        node_id: 1,
        boot_incarnation: uuid::Uuid::from_u128(11),
    };
    let committed = AssignmentSnapshot::empty()
        .next_for_participants(BTreeMap::from([(0, NodeId(1))]), vec![participant])
        .unwrap();
    store.save_if_absent(&committed).await.unwrap();
    let draining = committed
        .next_draining(
            BTreeMap::from([(0, NodeId(2))]),
            vec![CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(22),
            }],
            LeaderProof {
                owner: LeaderProofOwner {
                    node_id: participant.node_id,
                    boot_id: participant.boot_incarnation,
                    process_term: 1,
                },
                fencing_token: 1,
            },
        )
        .unwrap();
    store
        .save_if_version(&draining, committed.version)
        .await
        .unwrap();

    let selected = laminar_db::rebalance::startup_committed_assignment(&store, None, draining)
        .await
        .unwrap();
    assert_eq!(selected, committed);
    assert!(!selected.draining);
}

#[tokio::test]
async fn assignment_seed_rejects_peer_tag_that_is_not_durable_lease_owner() {
    use laminar_core::cluster::control::{ProcessLeaseOutcome, ProcessLeaseStore};

    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let self_id = NodeId(1);
    let peer_id = NodeId(2);
    let self_boot = uuid::Uuid::from_u128(11);
    let durable_peer_boot = uuid::Uuid::from_u128(22);
    for (node, boot) in [(self_id, self_boot), (peer_id, durable_peer_boot)] {
        let lease = ProcessLeaseStore::new(Arc::clone(&store), node, 1_000);
        assert!(matches!(
            lease.try_acquire(boot, 0).await.unwrap(),
            ProcessLeaseOutcome::Acquired(_)
        ));
    }

    let mut peer = NodeInfo {
        id: peer_id,
        name: "peer".into(),
        rpc_address: String::new(),
        state: NodeState::Joining,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    peer.metadata.tags.insert(
        PROCESS_INCARNATION_TAG.into(),
        durable_peer_boot.to_string(),
    );
    let participants =
        assignment_seed_participants(self_id, self_boot, &[peer.clone()], &store, 1_000)
            .await
            .unwrap();
    assert_eq!(
        participants,
        vec![
            laminar_core::checkpoint::CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: self_boot,
            },
            laminar_core::checkpoint::CheckpointParticipant {
                node_id: peer_id.0,
                boot_incarnation: durable_peer_boot,
            },
        ]
    );

    peer.metadata.tags.insert(
        PROCESS_INCARNATION_TAG.into(),
        uuid::Uuid::from_u128(222).to_string(),
    );
    let error = assignment_seed_participants(self_id, self_boot, &[peer], &store, 1_000)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("durable lease belongs"),
        "{error}"
    );
}

#[tokio::test]
async fn initial_assignment_roster_excludes_zero_vnode_workers() {
    use laminar_core::checkpoint::CheckpointParticipant;

    let nodes = [NodeId(1), NodeId(2), NodeId(3)];
    let peers: Vec<NodeInfo> = nodes[1..]
        .iter()
        .map(|node| NodeInfo {
            id: *node,
            name: format!("node-{}", node.0),
            rpc_address: String::new(),
            state: NodeState::Joining,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        })
        .collect();
    let participants: Vec<_> = nodes
        .iter()
        .map(|node| CheckpointParticipant {
            node_id: node.0,
            boot_incarnation: uuid::Uuid::from_u128(u128::from(node.0)),
        })
        .collect();
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let deadline =
        laminar_core::cluster::control::LeaseDeadline::live_for(std::time::Duration::from_secs(30));

    let (registry, snapshot_store) =
        resolve_vnode_assignment(nodes[0], &peers, 1, store, &participants, &deadline)
            .await
            .unwrap();
    let snapshot = snapshot_store.load().await.unwrap().unwrap();
    let owner = registry.snapshot()[0];

    assert_eq!(snapshot.participants.len(), 1);
    assert_eq!(snapshot.participants[0].node_id, owner.0);
    assert_eq!(
        nodes.iter().filter(|node| **node != owner).count(),
        2,
        "one vnode across three live workers must leave two workers idle"
    );
}

#[tokio::test]
async fn same_formation_three_node_genesis_is_preinstalled_but_owner_replacement_is_not() {
    use laminar_core::checkpoint::CheckpointParticipant;

    let nodes = [NodeId(1), NodeId(2), NodeId(3)];
    let creator_peers: Vec<NodeInfo> = nodes[1..]
        .iter()
        .map(|node| NodeInfo {
            id: *node,
            name: format!("node-{}", node.0),
            rpc_address: String::new(),
            state: NodeState::Joining,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        })
        .collect();
    let participants: Vec<_> = nodes
        .iter()
        .map(|node| CheckpointParticipant {
            node_id: node.0,
            boot_incarnation: uuid::Uuid::from_u128(u128::from(node.0)),
        })
        .collect();
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let deadline =
        laminar_core::cluster::control::LeaseDeadline::live_for(std::time::Duration::from_secs(30));

    let (creator, snapshots) = resolve_vnode_assignment(
        nodes[0],
        &creator_peers,
        64,
        Arc::clone(&store),
        &participants,
        &deadline,
    )
    .await
    .unwrap();
    let stored = snapshots.load().await.unwrap().unwrap();
    assert_eq!(creator.assignment_version(), 1);

    let mut different_timestamp = stored.clone();
    different_timestamp.updated_at_ms = different_timestamp.updated_at_ms.saturating_add(1);
    assert!(is_same_formation_genesis(&different_timestamp, &stored));

    let observer_peers: Vec<NodeInfo> = [nodes[0], nodes[2]]
        .into_iter()
        .map(|node| NodeInfo {
            id: node,
            name: format!("node-{}", node.0),
            rpc_address: String::new(),
            state: NodeState::Joining,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        })
        .collect();
    let (peer, _) = resolve_vnode_assignment(
        nodes[1],
        &observer_peers,
        64,
        Arc::clone(&store),
        &participants,
        &deadline,
    )
    .await
    .unwrap();
    assert_eq!(peer.assignment_version(), stored.version);
    assert_eq!(peer.snapshot().as_ref(), creator.snapshot().as_ref());

    let replaced_node = stored.participants[0].node_id;
    let mut replacement_participants = participants.clone();
    replacement_participants
        .iter_mut()
        .find(|participant| participant.node_id == replaced_node)
        .unwrap()
        .boot_incarnation = uuid::Uuid::from_u128(10_000 + u128::from(replaced_node));
    let (replacement, _) = resolve_vnode_assignment(
        nodes[1],
        &observer_peers,
        64,
        store,
        &replacement_participants,
        &deadline,
    )
    .await
    .unwrap();
    assert_eq!(replacement.assignment_version(), 0);
    assert!(replacement
        .snapshot()
        .iter()
        .all(|owner| *owner == laminar_core::state::NodeId::UNASSIGNED));

    let mut changed_map = stored.clone();
    *changed_map.vnodes.get_mut(&0).unwrap() = laminar_core::state::NodeId(999);
    assert!(!is_same_formation_genesis(&changed_map, &stored));
    let mut changed_process = stored.clone();
    changed_process.participants[0].boot_incarnation = uuid::Uuid::from_u128(99_999);
    assert!(!is_same_formation_genesis(&changed_process, &stored));
    let mut later_generation = stored.clone();
    later_generation.version = 2;
    assert!(!is_same_formation_genesis(&later_generation, &stored));
}

#[tokio::test]
async fn initial_assignment_cas_loser_does_not_preinstall_a_different_formation() {
    use laminar_core::checkpoint::CheckpointParticipant;

    let inner: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let delayed = Arc::new(DelayedControlPutStore::new(inner));
    delayed.block_once(object_store::path::Path::from(
        "control/assignment-snapshots/v00000000000000000001.json",
    ));

    let loser_node = NodeId(1);
    let loser_participants = vec![CheckpointParticipant {
        node_id: loser_node.0,
        boot_incarnation: uuid::Uuid::from_u128(11),
    }];
    let loser_deadline = Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
        std::time::Duration::from_secs(30),
    ));
    let loser_store: Arc<dyn object_store::ObjectStore> = delayed.clone();
    let loser = tokio::spawn(async move {
        resolve_vnode_assignment(
            loser_node,
            &[],
            1,
            loser_store,
            &loser_participants,
            loser_deadline.as_ref(),
        )
        .await
        .unwrap()
    });
    delayed.wait_until_blocked().await;

    let winner_node = NodeId(2);
    let winner_participants = [CheckpointParticipant {
        node_id: winner_node.0,
        boot_incarnation: uuid::Uuid::from_u128(22),
    }];
    let winner_deadline =
        laminar_core::cluster::control::LeaseDeadline::live_for(std::time::Duration::from_secs(30));
    let winner_store: Arc<dyn object_store::ObjectStore> = delayed.clone();
    let (winner, _) = resolve_vnode_assignment(
        winner_node,
        &[],
        1,
        winner_store,
        &winner_participants,
        &winner_deadline,
    )
    .await
    .unwrap();
    assert_eq!(winner.assignment_version(), 1);
    assert_eq!(
        winner.snapshot().as_ref(),
        &[laminar_core::state::NodeId(2)]
    );

    delayed.release();
    let (loser, snapshots) = loser.await.unwrap();
    assert_eq!(
        snapshots.load().await.unwrap().unwrap().participants,
        winner_participants
    );
    assert_eq!(loser.assignment_version(), 0);
    assert!(loser
        .snapshot()
        .iter()
        .all(|owner| *owner == laminar_core::state::NodeId::UNASSIGNED));
}

#[tokio::test]
async fn fenced_process_cannot_create_the_initial_assignment() {
    use laminar_core::checkpoint::CheckpointParticipant;
    use laminar_core::cluster::control::{AssignmentSnapshotStore, LeaseDeadline};

    let node = NodeId(7);
    let participant = CheckpointParticipant {
        node_id: node.0,
        boot_incarnation: uuid::Uuid::from_u128(77),
    };
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let snapshots = AssignmentSnapshotStore::new(Arc::clone(&store));
    let deadline = LeaseDeadline::fenced();

    let result = resolve_vnode_assignment(node, &[], 1, store, &[participant], &deadline).await;
    let error = match result {
        Ok(_) => panic!("a fenced process created an assignment"),
        Err(error) => error,
    };

    assert!(matches!(error, ClusterStartupError::AuthorityLost(_)));
    assert!(snapshots.load().await.unwrap().is_none());
}

#[tokio::test]
async fn startup_waits_for_exact_local_assignment_certificate() {
    use laminar_core::checkpoint::CheckpointAssignmentFence;
    use laminar_core::cluster::control::{
        CheckpointParticipant, ClusterController, ClusterKv, InMemoryKv,
    };
    use laminar_core::state::{NodeId as StateNodeId, VnodeRegistry};

    let node = NodeId(7);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node, kv, None, members_rx));
    controller.publish_recovery_incarnation().await.unwrap();
    controller.set_active(true);
    let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(node.0)));

    let publish = Arc::clone(&controller);
    tokio::spawn(async move {
        tokio::task::yield_now().await;
        publish.publish_checkpoint_assignment_fence(Some(
            CheckpointAssignmentFence::from_owner_map(
                1,
                &[node.0],
                vec![CheckpointParticipant {
                    node_id: node.0,
                    boot_incarnation: publish.recovery_incarnation(),
                }],
            )
            .unwrap(),
        ));
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        wait_for_startup_assignment_fence(&controller, &registry, &[]),
    )
    .await
    .expect("startup wait did not observe the assignment certificate")
    .unwrap();
}

#[tokio::test]
async fn startup_rechecks_assignment_certificate_when_membership_becomes_active() {
    use laminar_core::checkpoint::CheckpointAssignmentFence;
    use laminar_core::cluster::control::{
        CheckpointParticipant, ClusterController, ClusterKv, InMemoryKv,
    };
    use laminar_core::state::{NodeId as StateNodeId, VnodeRegistry};

    let node = NodeId(7);
    let peer = NodeId(8);
    let peer_boot = uuid::Uuid::from_u128(88);
    let joining_peer = NodeInfo {
        id: peer,
        name: "peer".into(),
        rpc_address: String::new(),
        state: NodeState::Joining,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let (members_tx, members_rx) = watch::channel(vec![joining_peer.clone()]);
    let controller = Arc::new(ClusterController::new(node, kv, None, members_rx));
    controller.set_active(true);
    let registry = VnodeRegistry::new_unassigned(2);
    registry.set_assignment_and_version(vec![StateNodeId(node.0), StateNodeId(peer.0)].into(), 1);
    controller.publish_checkpoint_assignment_fence(Some(
        CheckpointAssignmentFence::from_owner_map(
            1,
            &[node.0, peer.0],
            vec![
                CheckpointParticipant {
                    node_id: node.0,
                    boot_incarnation: controller.recovery_incarnation(),
                },
                CheckpointParticipant {
                    node_id: peer.0,
                    boot_incarnation: peer_boot,
                },
            ],
        )
        .unwrap(),
    ));

    let wait = wait_for_startup_assignment_fence(&controller, &registry, &[]);
    tokio::pin!(wait);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut wait)
            .await
            .is_err(),
        "a Joining assignment participant must keep startup fenced"
    );

    let mut active_peer = joining_peer;
    active_peer.state = NodeState::Active;
    members_tx.send_replace(vec![active_peer]);

    tokio::time::timeout(std::time::Duration::from_secs(1), wait)
        .await
        .expect("membership-only activation did not wake assignment certification")
        .unwrap();
}

#[test]
fn startup_leader_timeout_covers_manager_and_remote_audit_phases() {
    let timeout = startup_leader_authority_timeout(
        laminar_core::cluster::control::LeaderLeaseConfig {
            ttl: std::time::Duration::from_secs(5),
            renew_interval: std::time::Duration::from_secs(2),
        },
        std::time::Duration::from_secs(5),
    )
    .unwrap();
    assert_eq!(timeout, std::time::Duration::from_millis(42_375));
}

#[tokio::test]
async fn startup_leader_authority_requires_a_live_full_owner_and_keeps_intake_fenced() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::cluster::control::{
        ClusterController, ClusterKv, InMemoryKv, LeaderLeaseOwner, LeaderLeaseStore,
        LeaseDeadline, LeaseOutcome, ProcessLeaseAuthority, ProcessLeaseOutcome,
    };
    use laminar_core::state::{NodeId as StateNodeId, VnodeRegistry};

    let node = NodeId(7);
    let stale_boot = uuid::Uuid::from_u128(71);
    let certified_boot = uuid::Uuid::from_u128(72);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = ClusterController::new_with_recovery_incarnation(
        node,
        Arc::clone(&kv),
        kv,
        None,
        members_rx,
        certified_boot,
    );
    controller.publish_checkpoint_assignment_fence(Some(
        CheckpointAssignmentFence::from_owner_map(
            1,
            &[node.0],
            vec![CheckpointParticipant {
                node_id: node.0,
                boot_incarnation: certified_boot,
            }],
        )
        .unwrap(),
    ));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(30),
        )))
        .unwrap();
    let registry = VnodeRegistry::single_owner(1, StateNodeId(node.0));

    let delayed = Arc::new(DelayedControlPutStore::new(Arc::new(
        object_store::memory::InMemory::new(),
    )));
    let backing: Arc<dyn object_store::ObjectStore> = delayed.clone();
    let process_authority = Arc::new(
        ProcessLeaseAuthority::new(Arc::clone(&backing), std::time::Duration::from_millis(1))
            .unwrap(),
    );
    let ProcessLeaseOutcome::Acquired(process_lease) = process_authority
        .store_for(node)
        .try_acquire(certified_boot, 0)
        .await
        .unwrap()
    else {
        panic!("empty process authority must be acquired");
    };
    controller
        .set_process_lease_authority(Arc::clone(&process_authority))
        .unwrap();
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&backing), 1));
    let stale_owner = LeaderLeaseOwner {
        node,
        boot: stale_boot,
        process_term: 1,
    };
    let LeaseOutcome::Acquired(stale_lease) =
        authority.begin_new_term(&stale_owner, 0).await.unwrap()
    else {
        panic!("empty test authority must be acquired");
    };
    let certified_owner = LeaderLeaseOwner {
        node,
        boot: certified_boot,
        process_term: process_lease.term,
    };
    let (leader_tx, leader_rx) = watch::channel(None);
    controller
        .set_leader_lease_watch(
            leader_rx,
            certified_owner.clone(),
            Arc::new(LeaseDeadline::live_for(std::time::Duration::from_secs(30))),
        )
        .unwrap();
    controller.set_leader_lease_store(Arc::clone(&authority));
    let observation = authority
        .observe_rival(&certified_owner, &stale_lease)
        .unwrap();
    let error = wait_for_startup_leader_authority(
        &controller,
        &registry,
        std::time::Duration::from_millis(100),
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains(&certified_boot.to_string()),
        "{error}"
    );

    let LeaseOutcome::Acquired(takeover) = authority
        .try_takeover(&certified_owner, &observation, 10)
        .await
        .unwrap()
    else {
        panic!("certified process must take over stale leader authority");
    };

    let no_grant = wait_for_startup_leader_authority(
        &controller,
        &registry,
        std::time::Duration::from_millis(100),
    )
    .await
    .unwrap_err();
    assert!(
        no_grant.to_string().contains("live certified grant"),
        "{no_grant}"
    );

    leader_tx.send_replace(Some(takeover));

    wait_for_startup_leader_authority(&controller, &registry, std::time::Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn remote_startup_requires_a_live_exact_process_proof_and_resets_with_the_candidate() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::cluster::control::{
        CatalogManifest, CatalogManifestStore, ClusterController, LeaderLeaseOwner,
        LeaderLeaseStore, LeaseDeadline, LeaseOutcome, ProcessLeaseAuthority, ProcessLeaseOutcome,
    };
    use laminar_core::state::{NodeId as StateNodeId, VnodeRegistry};

    fn active_node(node: NodeId) -> NodeInfo {
        NodeInfo {
            id: node,
            name: format!("node-{}", node.0),
            rpc_address: String::new(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        }
    }

    let leader_node = NodeId(1);
    let observer_node = NodeId(2);
    let replacement_node = NodeId(3);
    let leader_boot = uuid::Uuid::from_u128(11);
    let observer_boot = uuid::Uuid::from_u128(22);
    let replacement_boot = uuid::Uuid::from_u128(33);
    let controls = shared_test_kvs();
    let (observer_members_tx, observer_members_rx) = watch::channel(vec![active_node(leader_node)]);
    let (_leader_members_tx, leader_members_rx) = watch::channel(vec![active_node(observer_node)]);
    let observer = Arc::new(ClusterController::new_with_recovery_incarnation(
        observer_node,
        Arc::clone(&controls[1]),
        Arc::clone(&controls[1]),
        None,
        observer_members_rx,
        observer_boot,
    ));
    let leader = Arc::new(ClusterController::new_with_recovery_incarnation(
        leader_node,
        Arc::clone(&controls[0]),
        Arc::clone(&controls[0]),
        None,
        leader_members_rx,
        leader_boot,
    ));
    for controller in [&observer, &leader] {
        controller.install_local_leader_proof_provider();
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
                std::time::Duration::from_secs(30),
            )))
            .unwrap();
        controller.set_active(true);
    }

    let registry = VnodeRegistry::single_owner(1, StateNodeId(leader_node.0));
    let leader_participant = CheckpointParticipant {
        node_id: leader_node.0,
        boot_incarnation: leader_boot,
    };
    let leader_fence =
        CheckpointAssignmentFence::from_owner_map(1, &[leader_node.0], vec![leader_participant])
            .unwrap();
    observer.publish_checkpoint_assignment_fence(Some(leader_fence.clone()));
    leader.publish_checkpoint_assignment_fence(Some(leader_fence));

    let delayed = Arc::new(DelayedControlPutStore::new(Arc::new(
        object_store::memory::InMemory::new(),
    )));
    let backing: Arc<dyn object_store::ObjectStore> = delayed.clone();
    let process_authority = Arc::new(
        ProcessLeaseAuthority::new(Arc::clone(&backing), std::time::Duration::from_secs(1))
            .unwrap(),
    );
    let ProcessLeaseOutcome::Acquired(process_lease) = process_authority
        .store_for(leader_node)
        .try_acquire(leader_boot, 0)
        .await
        .unwrap()
    else {
        panic!("remote process lease must be acquired");
    };
    let ProcessLeaseOutcome::Acquired(observer_process_lease) = process_authority
        .store_for(observer_node)
        .try_acquire(observer_boot, 0)
        .await
        .unwrap()
    else {
        panic!("observer process lease must be acquired");
    };
    observer
        .start_leased_barrier_server(
            "127.0.0.1:0".parse().unwrap(),
            None,
            &observer_process_lease,
        )
        .await
        .unwrap();
    leader
        .start_leased_barrier_server("127.0.0.1:0".parse().unwrap(), None, &process_lease)
        .await
        .unwrap();
    observer
        .set_process_lease_authority(Arc::clone(&process_authority))
        .unwrap();
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&backing), 1_000));
    let owner = LeaderLeaseOwner {
        node: leader_node,
        boot: leader_boot,
        process_term: process_lease.term,
    };
    let LeaseOutcome::Acquired(initial_lease) = authority.begin_new_term(&owner, 0).await.unwrap()
    else {
        panic!("remote leader lease must be acquired");
    };
    observer.set_leader_lease_store(Arc::clone(&authority));
    leader.set_leader_lease_store(Arc::clone(&authority));
    let (leader_grant_tx, leader_grant_rx) = watch::channel(None);
    leader
        .set_leader_lease_watch(
            leader_grant_rx,
            owner,
            Arc::new(LeaseDeadline::live_for(std::time::Duration::from_secs(30))),
        )
        .unwrap();
    let no_grant = wait_for_startup_leader_authority(
        &observer,
        &registry,
        std::time::Duration::from_millis(100),
    )
    .await
    .unwrap_err();
    assert!(no_grant.to_string().contains("live certified grant"));

    delayed.block_get_once(object_store::path::Path::from(
        "control/leader-lease/v0000000000000001.json",
    ));
    let unrelated_wait = wait_for_startup_leader_authority(
        &observer,
        &registry,
        std::time::Duration::from_millis(250),
    );
    tokio::pin!(unrelated_wait);
    tokio::select! {
        biased;
        result = &mut unrelated_wait => panic!("startup audit completed before its first authority read was blocked: {result:?}"),
        () = delayed.wait_until_get_blocked() => {}
    }
    CatalogManifestStore::new(Arc::clone(&authority))
        .seal(&CatalogManifest::default(), &initial_lease.proof())
        .await
        .unwrap();
    delayed.release_get();
    let after_catalog = authority.load().await.unwrap().unwrap();
    assert!(after_catalog.seq > initial_lease.seq);
    assert_eq!(after_catalog.expires_at_ms, initial_lease.expires_at_ms);
    let unrelated_mutation = unrelated_wait.await.unwrap_err();
    assert!(
        unrelated_mutation
            .to_string()
            .contains("live certified grant"),
        "{unrelated_mutation}"
    );

    leader_grant_tx.send_replace(Some(after_catalog));
    wait_for_startup_leader_authority(&observer, &registry, std::time::Duration::from_secs(1))
        .await
        .unwrap();

    leader_grant_tx.send_replace(None);
    let dead_process = wait_for_startup_leader_authority(
        &observer,
        &registry,
        std::time::Duration::from_millis(100),
    )
    .await
    .unwrap_err();
    assert!(dead_process.to_string().contains("live certified grant"));
    leader_grant_tx.send_replace(authority.load().await.unwrap());

    let replacement = CheckpointParticipant {
        node_id: replacement_node.0,
        boot_incarnation: replacement_boot,
    };
    registry.set_assignment_and_version(vec![StateNodeId(replacement_node.0)].into(), 2);
    observer.publish_checkpoint_assignment_fence(Some(
        CheckpointAssignmentFence::from_owner_map(2, &[replacement_node.0], vec![replacement])
            .unwrap(),
    ));
    observer_members_tx
        .send(vec![
            active_node(leader_node),
            active_node(replacement_node),
        ])
        .unwrap();
    let reset = wait_for_startup_leader_authority(
        &observer,
        &registry,
        std::time::Duration::from_millis(100),
    )
    .await
    .unwrap_err();
    assert!(reset.to_string().contains(&replacement_boot.to_string()));
}
