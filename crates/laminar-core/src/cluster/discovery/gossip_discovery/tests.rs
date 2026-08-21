use super::*;

fn add_process_identity(kvs: &mut HashMap<String, String>, boot: u128) {
    let tags = BTreeMap::from([(
        PROCESS_INCARNATION_TAG,
        uuid::Uuid::from_u128(boot).to_string(),
    )]);
    kvs.insert(
        keys::METADATA_TAGS.into(),
        serde_json::to_string(&tags).unwrap(),
    );
}

fn current_node_kvs(boot: u128) -> HashMap<String, String> {
    let mut kvs = HashMap::from([
        (keys::RPC_ADDRESS.into(), "127.0.0.1:9000".into()),
        (keys::NODE_NAME.into(), "test-node".into()),
        (keys::NODE_STATE.into(), "active".into()),
        (keys::LOAD_CORES.into(), "4".into()),
        (keys::LOAD_MEMORY.into(), "0".into()),
        (keys::NODE_VERSION.into(), env!("CARGO_PKG_VERSION").into()),
        (
            keys::PROTOCOL_VERSION.into(),
            DISCOVERY_PROTOCOL_VERSION.into(),
        ),
    ]);
    add_process_identity(&mut kvs, boot);
    kvs
}

#[test]
fn test_key_namespace() {
    assert_eq!(keys::NODE_STATE, "node:state");
    assert_eq!(keys::RPC_ADDRESS, "node:rpc_addr");
}

#[test]
fn test_gossip_config_default() {
    let config = GossipDiscoveryConfig::default();
    assert_eq!(config.gossip_interval, Duration::from_millis(500));
    assert!((config.phi_threshold - 8.0).abs() < f64::EPSILON);
    assert_eq!(config.process_generation, 1);
}

#[test]
fn test_parse_node_info() {
    let kvs = current_node_kvs(42);

    let info = GossipDiscovery::parse_node_info("node-42", &kvs).unwrap();
    assert_eq!(info.id, NodeId(42));
    assert_eq!(info.name, "test-node");
    assert_eq!(info.metadata.cores, 4);
    assert_eq!(info.metadata.memory_bytes, 0);
    assert_eq!(info.metadata.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.state, NodeState::Active);
}

#[test]
fn test_parse_node_info_invalid_id() {
    let kvs = HashMap::new();
    assert!(GossipDiscovery::parse_node_info("invalid", &kvs).is_none());
}

#[test]
fn test_parse_node_info_requires_current_fields() {
    for key in [
        keys::RPC_ADDRESS,
        keys::NODE_NAME,
        keys::NODE_STATE,
        keys::LOAD_CORES,
        keys::LOAD_MEMORY,
        keys::NODE_VERSION,
        keys::PROTOCOL_VERSION,
        keys::METADATA_TAGS,
    ] {
        let mut kvs = current_node_kvs(1);
        kvs.remove(key);
        assert!(GossipDiscovery::parse_node_info("node-1", &kvs).is_none());
    }
}

#[test]
fn test_parse_node_info_rejects_invalid_current_identity() {
    let mut kvs = current_node_kvs(1);
    kvs.insert(keys::NODE_NAME.into(), String::new());
    assert!(GossipDiscovery::parse_node_info("node-1", &kvs).is_none());

    let mut kvs = current_node_kvs(1);
    kvs.insert(keys::LOAD_CORES.into(), "four".into());
    assert!(GossipDiscovery::parse_node_info("node-1", &kvs).is_none());

    let mut kvs = current_node_kvs(1);
    kvs.insert(keys::LOAD_MEMORY.into(), "unknown".into());
    assert!(GossipDiscovery::parse_node_info("node-1", &kvs).is_none());

    let mut kvs = current_node_kvs(1);
    kvs.insert(keys::NODE_VERSION.into(), "0.0.0".into());
    assert!(GossipDiscovery::parse_node_info("node-1", &kvs).is_none());

    let mut kvs = current_node_kvs(1);
    kvs.insert(keys::PROTOCOL_VERSION.into(), "1".into());
    assert!(GossipDiscovery::parse_node_info("node-1", &kvs).is_none());
}

#[test]
fn test_local_kvs() {
    let tags = HashMap::from([
        (
            "laminardb.process-incarnation".into(),
            uuid::Uuid::from_u128(7).to_string(),
        ),
        ("shuffle:addr".into(), "127.0.0.1:9100".into()),
    ]);
    let info = NodeInfo {
        id: NodeId(1),
        name: "n1".into(),
        rpc_address: "127.0.0.1:9000".into(),
        state: NodeState::Active,
        metadata: NodeMetadata {
            cores: 4,
            memory_bytes: 1024,
            failure_domain: Some("us-east-1a".into()),
            tags,
            version: env!("CARGO_PKG_VERSION").into(),
        },
        last_heartbeat_ms: 0,
    };
    let kvs = GossipDiscovery::local_kvs(&info).unwrap();
    assert!(kvs.iter().any(|(k, _)| k == keys::RPC_ADDRESS));
    assert!(kvs.iter().any(|(k, _)| k == keys::FAILURE_DOMAIN));
    assert!(kvs.iter().any(|(key, value)| {
        key == keys::PROTOCOL_VERSION && value == DISCOVERY_PROTOCOL_VERSION
    }));
    let encoded_tags = kvs
        .iter()
        .find_map(|(key, value)| (key == keys::METADATA_TAGS).then_some(value))
        .expect("metadata tags must be present even during initial formation");
    let decoded: BTreeMap<String, String> = serde_json::from_str(encoded_tags).unwrap();
    assert_eq!(
        decoded
            .get("laminardb.process-incarnation")
            .map(String::as_str),
        Some("00000000-0000-0000-0000-000000000007")
    );

    let mut invalid = info;
    invalid.metadata.version = "0.0.0".into();
    assert!(GossipDiscovery::local_kvs(&invalid).is_err());
}

#[test]
fn fresh_assignment_process_incarnation_survives_gossip_round_trip() {
    let incarnation = uuid::Uuid::from_u128(7).to_string();
    let mut initial = GossipDiscoveryConfig::default().local_node;
    initial.id = NodeId(7);
    initial.name = "node-7".into();
    initial.state = NodeState::Joining;
    initial
        .metadata
        .tags
        .insert(PROCESS_INCARNATION_TAG.into(), incarnation.clone());
    initial
        .metadata
        .tags
        .insert("shuffle:addr".into(), "127.0.0.1:9107".into());

    let wire: HashMap<String, String> = GossipDiscovery::local_kvs(&initial)
        .unwrap()
        .into_iter()
        .collect();
    let observed = GossipDiscovery::parse_node_info("node-7", &wire)
        .expect("a fresh gossip peer must be usable by assignment formation");

    assert_eq!(observed.state, NodeState::Joining);
    assert_eq!(observed.metadata.tags, initial.metadata.tags);
    assert_eq!(
        observed.metadata.tags.get(PROCESS_INCARNATION_TAG),
        Some(&incarnation)
    );
}

fn collapse_test_candidates(
    candidates: Vec<(u64, u64, Option<NodeInfo>)>,
) -> HashMap<u64, NodeInfo> {
    let mut highest = HashMap::new();
    for (node_id, generation, _) in &candidates {
        observe_peer_generation(&mut highest, *node_id, *generation);
    }
    candidates
        .into_iter()
        .filter_map(|(node_id, generation, info)| {
            highest
                .get(&node_id)
                .filter(|candidate| !candidate.ambiguous && candidate.generation == generation)
                .and(info)
                .map(|info| (node_id, info))
        })
        .collect()
}

#[test]
fn durable_generation_wins_despite_wall_clock_rollback() {
    let mut old = GossipDiscoveryConfig::default().local_node;
    old.id = NodeId(7);
    old.state = NodeState::Active;
    old.metadata.tags.insert(
        PROCESS_INCARNATION_TAG.into(),
        uuid::Uuid::from_u128(70).to_string(),
    );
    let mut current = old.clone();
    current.state = NodeState::Draining;
    current.metadata.tags.insert(
        PROCESS_INCARNATION_TAG.into(),
        uuid::Uuid::from_u128(71).to_string(),
    );

    let old_wall_clock_ms = 10_000;
    let restarted_wall_clock_ms = 1;
    assert!(restarted_wall_clock_ms < old_wall_clock_ms);
    for newest_first in [false, true] {
        let candidates = if newest_first {
            vec![(7, 11, Some(current.clone())), (7, 10, Some(old.clone()))]
        } else {
            vec![(7, 10, Some(old.clone())), (7, 11, Some(current.clone()))]
        };
        let info = collapse_test_candidates(candidates)
            .remove(&7)
            .expect("higher durable process term must win");
        assert_eq!(info.state, NodeState::Draining);
        assert_eq!(
            info.metadata.tags.get(PROCESS_INCARNATION_TAG),
            current.metadata.tags.get(PROCESS_INCARNATION_TAG)
        );
    }
}

#[test]
fn equal_generation_process_collision_excludes_stable_node() {
    let mut first = GossipDiscoveryConfig::default().local_node;
    first.id = NodeId(7);
    first.metadata.tags.insert(
        PROCESS_INCARNATION_TAG.into(),
        uuid::Uuid::from_u128(70).to_string(),
    );
    let mut second = first.clone();
    second.metadata.tags.insert(
        PROCESS_INCARNATION_TAG.into(),
        uuid::Uuid::from_u128(71).to_string(),
    );

    let peers = collapse_test_candidates(vec![(7, 11, Some(first)), (7, 11, Some(second))]);
    assert!(!peers.contains_key(&7));
}

#[test]
fn invalid_newest_generation_does_not_resurrect_older_process() {
    let mut old = GossipDiscoveryConfig::default().local_node;
    old.id = NodeId(7);

    let oversized = serde_json::to_string(
        &(0..8)
            .map(|index| {
                (
                    format!("encoded-limit-{index}"),
                    "v".repeat(MAX_METADATA_TAG_VALUE_BYTES),
                )
            })
            .collect::<BTreeMap<_, _>>(),
    )
    .unwrap();
    assert!(oversized.len() > MAX_METADATA_TAGS_ENCODED_BYTES);
    for invalid_tags in [None, Some("not-json".to_string()), Some(oversized)] {
        let mut kvs = current_node_kvs(71);
        match invalid_tags {
            Some(tags) => {
                kvs.insert(keys::METADATA_TAGS.into(), tags);
            }
            None => {
                kvs.remove(keys::METADATA_TAGS);
            }
        }
        let invalid_newest = GossipDiscovery::parse_node_info("node-7", &kvs);
        assert!(invalid_newest.is_none());
        let peers =
            collapse_test_candidates(vec![(7, 10, Some(old.clone())), (7, 11, invalid_newest)]);
        assert!(
            !peers.contains_key(&7),
            "invalid highest generation must exclude the stable node"
        );
    }
}

#[test]
fn invalid_newest_lifecycle_state_excludes_stable_node() {
    let mut old = GossipDiscoveryConfig::default().local_node;
    old.id = NodeId(7);

    let base = current_node_kvs(71);

    for invalid_state in [
        None,
        Some(""),
        Some("ACTIVE"),
        Some("active "),
        Some("retired"),
    ] {
        let mut kvs = base.clone();
        match invalid_state {
            Some(state) => {
                kvs.insert(keys::NODE_STATE.into(), state.into());
            }
            None => {
                kvs.remove(keys::NODE_STATE);
            }
        }

        let invalid_newest = GossipDiscovery::parse_node_info("node-7", &kvs);
        assert!(invalid_newest.is_none());
        let peers =
            collapse_test_candidates(vec![(7, 10, Some(old.clone())), (7, 11, invalid_newest)]);
        assert!(
            !peers.contains_key(&7),
            "invalid highest-generation lifecycle state must exclude the stable node"
        );
    }
}

#[test]
fn malformed_metadata_tags_reject_peer_identity() {
    let mut kvs = current_node_kvs(1);
    kvs.insert(keys::METADATA_TAGS.into(), "not-json".into());
    assert!(GossipDiscovery::parse_node_info("node-1", &kvs).is_none());

    kvs.insert(keys::METADATA_TAGS.into(), "[]".into());
    assert!(GossipDiscovery::parse_node_info("node-1", &kvs).is_none());
}

#[test]
fn metadata_tags_are_bounded_before_gossip_publication() {
    let mut info = GossipDiscoveryConfig::default().local_node;
    info.metadata.tags = (0..=MAX_METADATA_TAGS)
        .map(|index| (format!("key-{index}"), "value".into()))
        .collect();
    assert!(GossipDiscovery::local_kvs(&info).is_err());

    info.metadata.tags =
        HashMap::from([("k".repeat(MAX_METADATA_TAG_KEY_BYTES + 1), "value".into())]);
    assert!(GossipDiscovery::local_kvs(&info).is_err());

    info.metadata.tags =
        HashMap::from([("key".into(), "v".repeat(MAX_METADATA_TAG_VALUE_BYTES + 1))]);
    assert!(GossipDiscovery::local_kvs(&info).is_err());

    info.metadata.tags = (0..8)
        .map(|index| {
            (
                format!("encoded-limit-{index}"),
                "v".repeat(MAX_METADATA_TAG_VALUE_BYTES),
            )
        })
        .collect();
    assert!(GossipDiscovery::local_kvs(&info).is_err());
}

#[test]
fn oversized_remote_metadata_tags_reject_peer_identity() {
    let encoded = serde_json::to_string(
        &(0..8)
            .map(|index| {
                (
                    format!("encoded-limit-{index}"),
                    "v".repeat(MAX_METADATA_TAG_VALUE_BYTES),
                )
            })
            .collect::<BTreeMap<_, _>>(),
    )
    .unwrap();
    assert!(encoded.len() > MAX_METADATA_TAGS_ENCODED_BYTES);
    let mut kvs = current_node_kvs(1);
    kvs.insert(keys::METADATA_TAGS.into(), encoded);
    assert!(GossipDiscovery::parse_node_info("node-1", &kvs).is_none());
}

#[test]
fn test_parse_all_node_states() {
    for (state_str, expected) in [
        ("joining", NodeState::Joining),
        ("active", NodeState::Active),
        ("suspected", NodeState::Suspected),
        ("draining", NodeState::Draining),
        ("left", NodeState::Left),
    ] {
        let mut kvs = current_node_kvs(1);
        kvs.insert(keys::NODE_STATE.into(), state_str.into());

        let info = GossipDiscovery::parse_node_info("node-1", &kvs).unwrap();
        assert_eq!(info.state, expected);
    }
}

#[tokio::test]
async fn test_not_started_errors() {
    let config = GossipDiscoveryConfig::default();
    let disc = GossipDiscovery::new(config);
    assert!(disc.peers().await.is_err());
}

#[tokio::test]
async fn zero_process_generation_is_rejected_before_start() {
    let mut config = GossipDiscoveryConfig::default();
    config.process_generation = 0;
    let mut discovery = GossipDiscovery::new(config);
    assert!(matches!(
        discovery.start().await,
        Err(DiscoveryError::Serialization(message))
            if message.contains("generation must be nonzero")
    ));
}

#[tokio::test]
async fn drop_cancels_membership_and_chitchat_tasks() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);

    let mut config = GossipDiscoveryConfig::default();
    config.gossip_address = address.to_string();
    let mut discovery = GossipDiscovery::new(config);
    discovery.start().await.unwrap();

    let cancelled = discovery.cancel.clone();
    let membership_task = discovery.membership_handle.as_ref().unwrap().abort_handle();
    let chitchat_terminated = discovery
        .chitchat_handle
        .as_ref()
        .unwrap()
        .termination_watcher();

    drop(discovery);
    assert!(cancelled.is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), async {
        while !membership_task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropped gossip membership task must terminate");
    let _ = tokio::time::timeout(Duration::from_secs(1), chitchat_terminated)
        .await
        .expect("dropped gossip server task must terminate");

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match tokio::net::UdpSocket::bind(address).await {
                Ok(socket) => break socket,
                Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
            }
        }
    })
    .await
    .expect("dropped gossip discovery must release its socket");
}

#[tokio::test]
async fn membership_cancellation_does_not_wait_for_the_chitchat_lock() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);

    let mut config = GossipDiscoveryConfig::default();
    config.gossip_address = address.to_string();
    let mut discovery = GossipDiscovery::new(config);
    discovery.start().await.unwrap();

    let chitchat = discovery
        .chitchat_handle
        .as_ref()
        .unwrap()
        .chitchat()
        .clone();
    let guard = chitchat.lock().await;
    tokio::time::sleep(Duration::from_millis(550)).await;
    discovery.cancel.cancel();
    let membership = discovery.membership_handle.take().unwrap();
    tokio::time::timeout(Duration::from_secs(1), membership)
        .await
        .expect("membership shutdown waited for the chitchat lock")
        .unwrap();

    drop(guard);
    discovery.stop_with_timeout(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn stop_aborts_membership_that_exceeds_the_shutdown_bound() {
    let mut discovery = GossipDiscovery::new(GossipDiscoveryConfig::default());
    discovery.started = true;
    let cancelled = discovery.cancel.clone();
    let membership = tokio::spawn(std::future::pending::<()>());
    let membership_task = membership.abort_handle();
    discovery.membership_handle = Some(membership);

    tokio::time::timeout(
        Duration::from_secs(1),
        discovery.stop_with_timeout(Duration::from_millis(10)),
    )
    .await
    .expect("bounded gossip discovery shutdown did not return");

    assert!(cancelled.is_cancelled());
    assert!(!discovery.cancel.is_cancelled());
    assert!(!discovery.started);
    assert!(discovery.membership_handle.is_none());
    tokio::time::timeout(Duration::from_secs(1), async {
        while !membership_task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("bounded gossip discovery shutdown left its membership task running");
}

#[tokio::test]
async fn cancelling_stop_aborts_taken_membership_and_chitchat_ownership() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);

    let mut config = GossipDiscoveryConfig::default();
    config.gossip_address = address.to_string();
    let mut discovery = GossipDiscovery::new(config);
    discovery.start().await.unwrap();

    let membership = discovery.membership_handle.take().unwrap();
    membership.abort();
    let _ = membership.await;
    let membership = tokio::spawn(std::future::pending::<()>());
    let membership_task = membership.abort_handle();
    discovery.membership_handle = Some(membership);
    let cancelled = discovery.cancel.clone();
    let chitchat_terminated = discovery
        .chitchat_handle
        .as_ref()
        .unwrap()
        .termination_watcher();

    let stopping = tokio::spawn(async move {
        discovery.stop_with_timeout(Duration::from_secs(60)).await;
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !cancelled.is_cancelled() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("gossip stop did not publish cancellation");
    stopping.abort();
    let _ = stopping.await;

    tokio::time::timeout(Duration::from_secs(1), async {
        while !membership_task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelling gossip stop detached its taken membership task");
    let _ = tokio::time::timeout(Duration::from_secs(1), chitchat_terminated)
        .await
        .expect("cancelling gossip stop detached its Chitchat server");

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match tokio::net::UdpSocket::bind(address).await {
                Ok(socket) => break socket,
                Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
            }
        }
    })
    .await
    .expect("cancelled gossip stop did not release its socket");
}
