use super::*;

async fn bound_listeners() -> (TcpListener, TcpListener) {
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    (listener1, listener2)
}

fn test_node(id: u64, address: &str, state: NodeState) -> NodeInfo {
    NodeInfo {
        id: NodeId(id),
        name: format!("node-{id}"),
        rpc_address: address.into(),
        state,
        metadata: current_node_metadata(),
        last_heartbeat_ms: 0,
    }
}

fn test_heartbeat(node: NodeInfo, generation: u64, incarnation: u128) -> StaticHeartbeat {
    StaticHeartbeat::new(node, generation, uuid::Uuid::from_u128(incarnation))
}

async fn two_nodes(
    heartbeat_interval: Duration,
) -> (
    StaticDiscovery,
    StaticDiscovery,
    TcpListener,
    TcpListener,
    String,
    NodeInfo,
) {
    let (listener1, listener2) = bound_listeners().await;
    let addr1 = listener1.local_addr().unwrap().to_string();
    let addr2 = listener2.local_addr().unwrap().to_string();
    let node1 = test_node(1, &addr1, NodeState::Active);
    let node2 = test_node(2, &addr2, NodeState::Active);
    let config1 = StaticDiscoveryConfig {
        local_node: node1,
        seeds: vec![addr2.clone()],
        heartbeat_interval,
        suspect_threshold: 1_000,
        dead_threshold: 2_000,
        listen_address: addr1.clone(),
        process_generation: 1,
        process_incarnation: uuid::Uuid::from_u128(1),
    };
    let config2 = StaticDiscoveryConfig {
        local_node: node2.clone(),
        seeds: vec![addr1.clone()],
        heartbeat_interval,
        suspect_threshold: 1_000,
        dead_threshold: 2_000,
        listen_address: addr2,
        process_generation: 1,
        process_incarnation: uuid::Uuid::from_u128(2),
    };
    (
        StaticDiscovery::new(config1),
        StaticDiscovery::new(config2),
        listener1,
        listener2,
        addr1,
        node2,
    )
}

async fn wait_for_peer_state(
    discovery: &StaticDiscovery,
    peer_id: NodeId,
    expected: Option<NodeState>,
) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state = discovery
                .peers()
                .await
                .unwrap()
                .into_iter()
                .find(|peer| peer.id == peer_id)
                .map(|peer| peer.state);
            if state == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("peer {peer_id:?} did not reach state {expected:?}"));
}

#[test]
fn test_config_default() {
    let config = StaticDiscoveryConfig::default();
    assert_eq!(config.heartbeat_interval, Duration::from_secs(1));
    assert_eq!(config.suspect_threshold, 3);
    assert_eq!(config.dead_threshold, 10);
    assert_eq!(
        config.local_node.metadata.version,
        env!("CARGO_PKG_VERSION")
    );
}

#[test]
fn test_serialize_round_trip() {
    let info = NodeInfo {
        id: NodeId(42),
        name: "test".into(),
        rpc_address: "127.0.0.1:9000".into(),
        state: NodeState::Active,
        metadata: current_node_metadata(),
        last_heartbeat_ms: 1000,
    };

    let heartbeat = test_heartbeat(info, 7, 42);
    let data = StaticDiscovery::serialize_heartbeat(&heartbeat).unwrap();
    let back = StaticDiscovery::deserialize_heartbeat(&data).unwrap();
    assert_eq!(back.node.id, NodeId(42));
    assert_eq!(back.node.name, "test");
    assert_eq!(back.process_generation, 7);
    assert_eq!(
        back.process_incarnation,
        uuid::Uuid::from_u128(42).into_bytes()
    );
}

#[test]
fn test_deserialize_invalid() {
    let result = StaticDiscovery::deserialize_heartbeat(&[0xff, 0xff]);
    assert!(result.is_err());

    let mut heartbeat = test_heartbeat(test_node(1, "node:9000", NodeState::Active), 1, 1);
    heartbeat.node.metadata.version = "0.0.0".into();
    let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&heartbeat).unwrap();
    assert!(StaticDiscovery::deserialize_heartbeat(&encoded).is_err());

    let mut heartbeat = test_heartbeat(test_node(1, "node:9000", NodeState::Active), 1, 1);
    heartbeat.protocol_version = STATIC_DISCOVERY_PROTOCOL_VERSION - 1;
    let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&heartbeat).unwrap();
    assert!(StaticDiscovery::deserialize_heartbeat(&encoded).is_err());
}

#[test]
fn higher_term_replacement_escapes_graceful_drain() {
    let mut state = StaticState::default();
    let mut draining = test_node(9, "old:9000", NodeState::Draining);
    let first = test_heartbeat(draining.clone(), 4, 40);
    assert!(matches!(
        state.observe_outbound(first, 1),
        ObservationResult::Accepted(_)
    ));

    draining.state = NodeState::Active;
    draining.rpc_address = "new:9000".into();
    let replacement = test_heartbeat(draining, 5, 50);
    assert!(matches!(
        state.observe_outbound(replacement, 2),
        ObservationResult::Accepted(_)
    ));

    let peer = state.peers.get(&9).unwrap();
    assert_eq!(peer.identity.generation, 5);
    assert_eq!(peer.info.state, NodeState::Active);
    assert_eq!(peer.info.rpc_address, "new:9000");
    assert!(!peer.excluded);
}

#[test]
fn outbound_heartbeat_preserves_advertised_joining_state() {
    let mut state = StaticState::default();
    let joining = test_heartbeat(test_node(9, "node:9000", NodeState::Joining), 4, 40);

    state.observe_outbound(joining, 1);

    assert_eq!(state.peers.get(&9).unwrap().info.state, NodeState::Joining);
}

#[test]
fn lower_term_stale_heartbeat_is_ignored() {
    let mut state = StaticState::default();
    let current = test_heartbeat(test_node(9, "new:9000", NodeState::Active), 5, 50);
    state.observe_outbound(current, 2);

    let stale = test_heartbeat(test_node(9, "old:9000", NodeState::Left), 4, 40);
    assert_eq!(state.observe_outbound(stale, 3), ObservationResult::Ignored);

    let peer = state.peers.get(&9).unwrap();
    assert_eq!(peer.identity.generation, 5);
    assert_eq!(peer.info.state, NodeState::Active);
    assert_eq!(peer.info.rpc_address, "new:9000");
}

#[test]
fn equal_term_incarnation_collision_is_excluded_until_higher_term() {
    let mut state = StaticState::default();
    let incumbent = test_heartbeat(test_node(9, "one:9000", NodeState::Active), 5, 50);
    state.observe_outbound(incumbent.clone(), 1);

    let collision = test_heartbeat(test_node(9, "two:9000", NodeState::Active), 5, 51);
    assert_eq!(
        state.observe_outbound(collision, 2),
        ObservationResult::Collision
    );
    assert!(state.peer_list().is_empty());
    assert_eq!(
        state.observe_outbound(incumbent, 3),
        ObservationResult::Collision
    );
    assert!(state.peer_list().is_empty());

    let replacement = test_heartbeat(test_node(9, "three:9000", NodeState::Joining), 6, 60);
    state.observe_outbound(replacement, 4);
    assert_eq!(state.peer_list()[0].state, NodeState::Joining);
}

#[tokio::test]
async fn zero_process_generation_is_rejected_before_start() {
    let mut config = StaticDiscoveryConfig::default();
    config.listen_address = "127.0.0.1:0".into();
    config.process_generation = 0;
    let mut discovery = StaticDiscovery::new(config);

    let error = discovery.start().await.unwrap_err();
    assert!(error.to_string().contains("generation must be nonzero"));
}

#[tokio::test]
async fn test_not_started_errors() {
    let config = StaticDiscoveryConfig::default();
    let disc = StaticDiscovery::new(config);
    assert!(disc.peers().await.is_err());
}

#[tokio::test]
async fn test_start_stop() {
    let config = StaticDiscoveryConfig {
        listen_address: "127.0.0.1:0".into(),
        ..StaticDiscoveryConfig::default()
    };
    let mut disc = StaticDiscovery::new(config);
    disc.start().await.unwrap();
    assert!(disc.started);
    disc.stop().await.unwrap();
    assert!(!disc.started);
}

#[tokio::test]
async fn start_reports_occupied_listener_address() {
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = occupied.local_addr().unwrap().to_string();
    let config = StaticDiscoveryConfig {
        listen_address: address,
        ..StaticDiscoveryConfig::default()
    };
    let mut discovery = StaticDiscovery::new(config);

    assert!(matches!(
        discovery.start().await,
        Err(DiscoveryError::Bind(_))
    ));
    assert!(!discovery.started);
    assert!(discovery.listener_handle.is_none());
    assert!(discovery.heartbeater_handle.is_none());

    drop(occupied);
    discovery.start().await.unwrap();
    discovery.stop().await.unwrap();
}

#[tokio::test]
async fn test_double_start_ok() {
    let config = StaticDiscoveryConfig {
        listen_address: "127.0.0.1:0".into(),
        ..StaticDiscoveryConfig::default()
    };
    let mut disc = StaticDiscovery::new(config);
    disc.start().await.unwrap();
    disc.start().await.unwrap(); // Should be idempotent
    disc.stop().await.unwrap();
}

#[tokio::test]
async fn test_membership_watch() {
    let config = StaticDiscoveryConfig {
        listen_address: "127.0.0.1:0".into(),
        ..StaticDiscoveryConfig::default()
    };
    let disc = StaticDiscovery::new(config);
    let rx = disc.membership_watch();
    assert!(rx.borrow().is_empty());
}

#[tokio::test]
async fn test_announce_updates_local_advertisement() {
    let config = StaticDiscoveryConfig {
        listen_address: "127.0.0.1:0".into(),
        ..StaticDiscoveryConfig::default()
    };
    let mut disc = StaticDiscovery::new(config);
    disc.start().await.unwrap();

    let mut local = disc.local_info.read().clone();
    local.metadata.version = "0.0.0".into();
    assert!(disc.announce(local.clone()).await.is_err());
    assert_ne!(disc.local_info.read().metadata.version, "0.0.0");

    local.metadata.version = env!("CARGO_PKG_VERSION").into();
    local.state = NodeState::Draining;
    disc.announce(local).await.unwrap();

    let peers = disc.peers().await.unwrap();
    assert!(peers.is_empty());
    assert_eq!(disc.local_info.read().state, NodeState::Draining);

    disc.stop().await.unwrap();
}

#[tokio::test]
async fn test_two_node_heartbeat() {
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap().to_string();

    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap().to_string();

    let config1 = StaticDiscoveryConfig {
        local_node: NodeInfo {
            id: NodeId(1),
            name: "node-1".into(),
            rpc_address: addr1.clone(),
            state: NodeState::Active,
            metadata: current_node_metadata(),
            last_heartbeat_ms: 0,
        },
        seeds: vec![addr2.clone()],
        heartbeat_interval: Duration::from_millis(100),
        listen_address: addr1.clone(),
        ..StaticDiscoveryConfig::default()
    };

    let config2 = StaticDiscoveryConfig {
        local_node: NodeInfo {
            id: NodeId(2),
            name: "node-2".into(),
            rpc_address: addr2.clone(),
            state: NodeState::Active,
            metadata: current_node_metadata(),
            last_heartbeat_ms: 0,
        },
        seeds: vec![addr1],
        heartbeat_interval: Duration::from_millis(100),
        listen_address: addr2,
        ..StaticDiscoveryConfig::default()
    };

    let mut disc1 = StaticDiscovery::new(config1);
    let mut disc2 = StaticDiscovery::new(config2);

    disc1.start_with_bound_listener(listener1).unwrap();
    disc2.start_with_bound_listener(listener2).unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let peers1 = disc1.peers().await.unwrap();
    let peers2 = disc2.peers().await.unwrap();

    assert!(
        !peers1.is_empty() || !peers2.is_empty(),
        "at least one node should have discovered peers"
    );

    disc1.stop().await.unwrap();
    disc2.stop().await.unwrap();
}

#[tokio::test]
async fn test_two_node_draining_is_monotonic_across_healthy_heartbeats() {
    let interval = Duration::from_millis(20);
    let (mut disc1, mut disc2, listener1, listener2, addr1, mut node2) = two_nodes(interval).await;
    disc1.start_with_bound_listener(listener1).unwrap();
    disc2.start_with_bound_listener(listener2).unwrap();
    wait_for_peer_state(&disc1, node2.id, Some(NodeState::Active)).await;

    node2.state = NodeState::Draining;
    disc2.announce(node2.clone()).await.unwrap();
    wait_for_peer_state(&disc1, node2.id, Some(NodeState::Draining)).await;

    disc2.stop().await.unwrap();
    node2.state = NodeState::Active;
    let stale_active = StaticDiscovery::serialize_heartbeat(&StaticHeartbeat::new(
        node2.clone(),
        disc2.config.process_generation,
        disc2.config.process_incarnation,
    ))
    .unwrap();
    StaticDiscovery::send_heartbeat(&addr1, &stale_active)
        .await
        .unwrap();
    tokio::time::sleep(interval * 3).await;
    assert_eq!(
        disc1
            .peers()
            .await
            .unwrap()
            .into_iter()
            .find(|peer| peer.id == node2.id)
            .map(|peer| peer.state),
        Some(NodeState::Draining)
    );

    disc1.stop().await.unwrap();
}

#[tokio::test]
async fn test_two_node_left_is_monotonic_until_higher_term_rejoins() {
    let interval = Duration::from_millis(20);
    let (mut disc1, mut disc2, listener1, listener2, addr1, mut node2) = two_nodes(interval).await;
    disc1.start_with_bound_listener(listener1).unwrap();
    disc2.start_with_bound_listener(listener2).unwrap();
    wait_for_peer_state(&disc1, node2.id, Some(NodeState::Active)).await;

    node2.state = NodeState::Left;
    disc2.announce(node2.clone()).await.unwrap();
    wait_for_peer_state(&disc1, node2.id, Some(NodeState::Left)).await;

    let mut replacement_config = disc2.config.clone();
    disc2.stop().await.unwrap();
    node2.state = NodeState::Active;
    let stale_active = StaticDiscovery::serialize_heartbeat(&StaticHeartbeat::new(
        node2.clone(),
        disc2.config.process_generation,
        disc2.config.process_incarnation,
    ))
    .unwrap();
    StaticDiscovery::send_heartbeat(&addr1, &stale_active)
        .await
        .unwrap();
    tokio::time::sleep(interval * 3).await;
    assert_eq!(
        disc1
            .peers()
            .await
            .unwrap()
            .into_iter()
            .find(|peer| peer.id == node2.id)
            .map(|peer| peer.state),
        Some(NodeState::Left)
    );

    replacement_config.process_generation += 1;
    replacement_config.process_incarnation = uuid::Uuid::from_u128(22);
    let mut replacement = StaticDiscovery::new(replacement_config);
    replacement.start().await.unwrap();
    wait_for_peer_state(&disc1, node2.id, Some(NodeState::Active)).await;

    replacement.stop().await.unwrap();
    disc1.stop().await.unwrap();
}

#[tokio::test]
async fn test_restart_after_stop() {
    let config = StaticDiscoveryConfig {
        listen_address: "127.0.0.1:0".into(),
        ..StaticDiscoveryConfig::default()
    };
    let mut disc = StaticDiscovery::new(config);

    // First start/stop cycle
    disc.start().await.unwrap();
    disc.stop().await.unwrap();

    // Second start should work (fresh CancellationToken)
    disc.start().await.unwrap();
    assert!(disc.started);
    disc.stop().await.unwrap();
}

#[tokio::test]
async fn drop_cancels_background_tasks_and_releases_listener() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let config = StaticDiscoveryConfig {
        listen_address: address.to_string(),
        ..StaticDiscoveryConfig::default()
    };
    let mut discovery = StaticDiscovery::new(config);
    discovery.start_with_bound_listener(listener).unwrap();

    let cancelled = discovery.cancel.clone();
    let listener_task = discovery.listener_handle.as_ref().unwrap().abort_handle();
    let heartbeater_task = discovery
        .heartbeater_handle
        .as_ref()
        .unwrap()
        .abort_handle();

    drop(discovery);
    assert!(cancelled.is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), async {
        while !listener_task.is_finished() || !heartbeater_task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropped static discovery tasks must terminate");

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match TcpListener::bind(address).await {
                Ok(listener) => break listener,
                Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
            }
        }
    })
    .await
    .expect("dropped static discovery must release its listener");
}

#[tokio::test]
async fn stop_aborts_background_tasks_that_exceed_the_shutdown_bound() {
    let mut discovery = StaticDiscovery::new(StaticDiscoveryConfig::default());
    discovery.started = true;
    let cancelled = discovery.cancel.clone();

    let listener = tokio::spawn(std::future::pending::<Result<(), DiscoveryError>>());
    let listener_task = listener.abort_handle();
    discovery.listener_handle = Some(listener);
    let heartbeater = tokio::spawn(std::future::pending::<()>());
    let heartbeater_task = heartbeater.abort_handle();
    discovery.heartbeater_handle = Some(heartbeater);

    tokio::time::timeout(
        Duration::from_secs(1),
        discovery.stop_with_timeout(Duration::from_millis(10)),
    )
    .await
    .expect("bounded static discovery shutdown did not return");

    assert!(cancelled.is_cancelled());
    assert!(!discovery.started);
    assert!(discovery.listener_handle.is_none());
    assert!(discovery.heartbeater_handle.is_none());
    tokio::time::timeout(Duration::from_secs(1), async {
        while !listener_task.is_finished() || !heartbeater_task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("bounded static discovery shutdown left an owned task running");
}
