use super::*;
use bytes::Bytes;
use futures::FutureExt;
use object_store::ObjectStoreExt;

struct Peer {
    node: u64,
    pid: u32,
    address: SocketAddr,
    root: PathBuf,
    sequence: u32,
    child: tokio::process::Child,
}

impl Peer {
    async fn response(&mut self, name: &str) -> Response {
        let path = self.root.join(name);
        tokio::time::timeout(PEER_DEADLINE, async {
            loop {
                if path.exists() {
                    return read_message(&path);
                }
                if let Some(status) = self.child.try_wait().unwrap() {
                    panic!(
                        "owner {} exited before {name}: {status}; stderr: {}",
                        self.node,
                        std::fs::read_to_string(self.root.join("stderr.log")).unwrap()
                    );
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("peer response deadline")
    }

    async fn request(&mut self, command: &Command) -> Response {
        let sequence = self.sequence;
        self.sequence += 1;
        write_message(&self.root.join(format!("request-{sequence}.json")), command);
        self.response(&format!("response-{sequence}.json")).await
    }

    async fn done(&mut self, command: &Command) {
        let response = self.request(command).await;
        assert!(matches!(response, Response::Done), "{response:?}");
    }

    async fn kill(&mut self) {
        self.child.start_kill().unwrap();
        let status = tokio::time::timeout(PEER_DEADLINE, self.child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(!status.success());
    }
}

async fn spawn(peers: &mut Vec<Peer>, root: &Path, config: PeerConfig) {
    assert!(peers.len() < 5, "fixture process inventory is bounded");
    let directory = root.join(format!("peer-{}", peers.len()));
    std::fs::create_dir(&directory).unwrap();
    write_message(&directory.join("config.json"), &config);
    let child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST_NAME, "--ignored", "--nocapture"])
        .env(CHILD_ENV, &directory)
        .stdout(std::fs::File::create(directory.join("stdout.log")).unwrap())
        .stderr(std::fs::File::create(directory.join("stderr.log")).unwrap())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    peers.push(Peer {
        node: config.node,
        pid: child.id().unwrap(),
        address: "127.0.0.1:0".parse().unwrap(),
        root: directory,
        sequence: 0,
        child,
    });
    let peer = peers.last_mut().unwrap();
    let Response::Ready { pid, address } = peer.response("ready.json").await else {
        panic!("expected owner readiness")
    };
    assert_eq!(pid, peer.pid);
    assert_ne!(pid, std::process::id());
    peer.address = address;
    println!("owner {} ready in PID {pid} at {address}", peer.node);
}

async fn connect(peers: &mut [Peer], active: &[usize]) {
    for &index in active {
        let addresses = active
            .iter()
            .filter(|&&other| other != index)
            .map(|&other| (peers[other].node, peers[other].address))
            .collect();
        peers[index].done(&Command::Connect(addresses)).await;
    }
}

#[derive(Default)]
struct Applied {
    rows: Vec<ActivityRow>,
    callbacks: Vec<CallbackIdentity>,
}

#[derive(Clone, Copy, Serialize)]
enum Fault {
    ReplacementAndJoin,
    OwnerExit,
    FinalOwnerLoss,
}

async fn drain(peers: &mut [Peer], active: &[usize], watermark: i64, expected: usize) -> Applied {
    tokio::time::timeout(PEER_DEADLINE, async {
        let mut applied = Applied::default();
        loop {
            let mut settled = true;
            for &index in active {
                let Response::Observed {
                    rows,
                    callbacks,
                    quiescent,
                    watermark_us,
                } = peers[index].request(&Command::Observe).await
                else {
                    panic!("expected owner observation")
                };
                applied.rows.extend(rows);
                applied.callbacks.extend(callbacks);
                settled &= quiescent && watermark_us == Some(watermark * 1_000);
            }
            assert!(
                applied.rows.len() <= expected,
                "duplicate or unexpected output"
            );
            assert!(
                applied.callbacks.len() <= expected,
                "duplicate or unexpected invocation"
            );
            if settled && applied.rows.len() == expected && applied.callbacks.len() == expected {
                return applied;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("owners did not apply the expected replay prefix")
}

async fn advance(peers: &mut [Peer], active: &[usize], watermark: i64) {
    for &index in active {
        peers[index].done(&Command::Advance(watermark)).await;
    }
}

async fn replay(peers: &mut [Peer], active: &[usize], width: usize) -> Applied {
    let rows = (0..8_u32)
        .map(|index| {
            (
                key_for(index % 4),
                i64::from(index) + 1,
                106_000 + i64::from(index) * 1_000,
            )
        })
        .collect::<Vec<_>>();
    let mut applied = Applied::default();
    for chunk in rows.chunks(width) {
        peers[active[0]].done(&Command::Input(chunk.to_vec())).await;
        let next = drain(peers, active, 105, chunk.len()).await;
        applied.rows.extend(next.rows);
        applied.callbacks.extend(next.callbacks);
    }
    advance(peers, active, 125).await;
    let timers = drain(peers, active, 125, 4).await;
    applied.rows.extend(timers.rows);
    applied.callbacks.extend(timers.callbacks);
    applied.rows.sort();
    applied.callbacks.sort();
    applied
}

async fn restore(
    peers: &mut [Peer],
    active: &[usize],
    cut: &SharedCut,
    fence: &CheckpointAssignmentFence,
    owners: [u64; 4],
    recovery: u64,
) {
    for &index in active {
        let response = peers[index]
            .request(&Command::Restore {
                fence: fence.clone(),
                owners,
                recovery,
                reference: cut.reference.clone(),
            })
            .await;
        let Response::Restored {
            reference,
            reassigned,
            offsets,
            watermark,
        } = response
        else {
            panic!("{response:?}")
        };
        assert_eq!(reference, cut.reference);
        assert!(reassigned);
        assert_eq!(watermark, Some(105));
        assert_eq!(
            offsets,
            cut.fence
                .participants
                .iter()
                .map(|participant| (format!("partition-{}", participant.node_id), "2".into()))
                .collect::<std::collections::HashMap<String, String>>()
        );
    }
    advance(peers, active, 105).await;
    assert!(drain(peers, active, 105, 0).await.rows.is_empty());
}

async fn supersede(objects: &Arc<dyn ObjectStore>, node: u64, boot: Uuid) {
    let authority = ProcessLeaseAuthority::new(Arc::clone(objects), PEER_TTL).unwrap();
    let store = authority.store_for(NodeId(node));
    let previous = store.load().await.unwrap().unwrap();
    let observation = store.observe_rival(&previous).unwrap();
    tokio::time::sleep(PEER_TTL + Duration::from_millis(20)).await;
    let ProcessLeaseOutcome::Acquired(replacement) =
        store.try_takeover(boot, &observation, 1).await.unwrap()
    else {
        panic!("killed owner lease must be superseded")
    };
    assert!(replacement.term > previous.term);
    assert!(authority
        .verify_current_participant_term(
            CheckpointParticipant {
                node_id: node,
                boot_incarnation: boot,
            },
            replacement.term,
            tokio::time::Instant::now() + PEER_DEADLINE
        )
        .await
        .unwrap());
}

async fn stop(peers: &mut [Peer], active: &[usize]) {
    for &index in active {
        peers[index].done(&Command::Stop).await;
        assert!(
            tokio::time::timeout(PEER_DEADLINE, peers[index].child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    }
}

async fn run(peers: &mut Vec<Peer>, root: &Path, namespace: &str, runtime: Runtime, fault: Fault) {
    let objects = shared_objects(namespace);
    let owners = match fault {
        Fault::ReplacementAndJoin | Fault::OwnerExit => [7, 8, 7, 8],
        Fault::FinalOwnerLoss => [8; 4],
    };
    let fence = target_fence(7, owners);
    for participant in &fence.participants {
        spawn(
            peers,
            root,
            PeerConfig {
                namespace: namespace.into(),
                node: participant.node_id,
                fence: fence.clone(),
                owners,
                runtime,
            },
        )
        .await;
    }
    let initial = (0..peers.len()).collect::<Vec<_>>();
    connect(peers, &initial).await;
    let seed = (0..4_u32)
        .map(|vnode| {
            (
                key_for(vnode),
                [7, 11, 13, 17][vnode as usize],
                100_000 + i64::from(vnode) * 1_000,
            )
        })
        .collect::<Vec<_>>();
    for (index, chunk) in seed.chunks(4 / peers.len()).enumerate() {
        peers[index].done(&Command::Input(chunk.to_vec())).await;
    }
    advance(peers, &initial, 105).await;
    drain(peers, &initial, 105, 4).await;
    let writer = CheckpointWriter::begin(Arc::clone(&objects), fence).await;
    for peer in peers.iter_mut() {
        peer.done(&Command::Barrier).await;
    }
    let mut manifests = Vec::new();
    for peer in peers.iter_mut() {
        let Response::Captured { manifest, encoded } = peer
            .request(&Command::Capture(writer.deployment.clone()))
            .await
        else {
            panic!("expected participant capture")
        };
        manifests.push((*manifest, Bytes::from(encoded)));
    }
    let cut = writer.commit(manifests).await;
    let expected = replay(peers, &initial, 4).await;
    for &index in &initial {
        peers[index]
            .done(&Command::Input(vec![(
                key_for(u32::try_from(index * 3).unwrap()),
                999,
                130_000,
            )]))
            .await;
    }
    let uncommitted = drain(peers, &initial, 125, initial.len()).await;
    assert!(uncommitted.rows.iter().all(|row| row.2 >= 1_000));
    for &index in &initial {
        peers[index].done(&Command::Pause).await;
    }
    let (target, target_owners, active) = match fault {
        Fault::ReplacementAndJoin => {
            let authority = ProcessLeaseAuthority::new(Arc::clone(&objects), PEER_TTL).unwrap();
            let prior_survivor = authority
                .store_for(NodeId(8))
                .load()
                .await
                .unwrap()
                .unwrap();
            peers[0].kill().await;
            let boot = Uuid::from_u128(7_007);
            supersede(&objects, 7, boot).await;
            let survivor = authority
                .store_for(NodeId(8))
                .load()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(survivor.owner, prior_survivor.owner);
            assert!(survivor.seq > prior_survivor.seq);
            assert!(peers[1].child.try_wait().unwrap().is_none());
            let target_owners = [7, 9, 8, 9];
            let mut participants = target_fence(8, target_owners).participants;
            participants
                .iter_mut()
                .find(|participant| participant.node_id == 7)
                .unwrap()
                .boot_incarnation = boot;
            let target =
                CheckpointAssignmentFence::from_owner_map(8, &target_owners, participants).unwrap();
            cut.publish_target(owners, &target, target_owners).await;
            for node in [7, 9] {
                spawn(
                    peers,
                    root,
                    PeerConfig {
                        namespace: namespace.into(),
                        node,
                        fence: target.clone(),
                        owners: target_owners,
                        runtime,
                    },
                )
                .await;
            }
            (target, target_owners, vec![2, 1, 3])
        }
        Fault::OwnerExit => {
            let target = target_fence(8, [8; 4]);
            cut.publish_target(owners, &target, [8; 4]).await;
            stop(peers, &[0]).await;
            (target, [8; 4], vec![1])
        }
        Fault::FinalOwnerLoss => {
            let boot = Uuid::from_u128(8_008);
            let target = CheckpointAssignmentFence::from_owner_map(
                8,
                &[8; 4],
                vec![CheckpointParticipant {
                    node_id: 8,
                    boot_incarnation: boot,
                }],
            )
            .unwrap();
            // Publish the predecessor-bound handoff while its sole owner is still alive,
            // then lose that owner before any target actor is ready.
            cut.publish_target(owners, &target, [8; 4]).await;
            peers[0].kill().await;
            supersede(&objects, 8, boot).await;
            spawn(
                peers,
                root,
                PeerConfig {
                    namespace: namespace.into(),
                    node: 8,
                    fence: target.clone(),
                    owners: [8; 4],
                    runtime,
                },
            )
            .await;
            (target, [8; 4], vec![1])
        }
    };
    connect(peers, &active).await;
    restore(peers, &active, &cut, &target, target_owners, 4).await;
    let actual = replay(peers, &active, 1).await;
    assert_eq!(actual.rows, expected.rows);
    assert_eq!(actual.callbacks, expected.callbacks);
    assert_eq!(actual.callbacks.len(), 12);
    println!(
        "qualified {}: committed cut {}, owner PIDs {:?}, callbacks {}, outputs {}",
        serde_json::to_string(&fault).unwrap(),
        serde_json::to_string(&cut.reference).unwrap(),
        peers
            .iter()
            .map(|peer| (peer.node, peer.pid))
            .collect::<Vec<_>>(),
        serde_json::to_string(&actual.callbacks).unwrap(),
        serde_json::to_string(&actual.rows).unwrap()
    );

    for &index in &active {
        peers[index].done(&Command::Pause).await;
    }
    let donor = object_store::path::Path::from(format!(
        "process-shared-cut/nodes/{}/checkpoints/00000000000000000001/node-data.bin",
        cut.fence.participants[0].node_id
    ));
    objects
        .put(&donor, Bytes::from_static(b"damaged").into())
        .await
        .unwrap();
    let response = peers[active[0]]
        .request(&Command::Restore {
            fence: target,
            owners: target_owners,
            recovery: 5,
            reference: cut.reference.clone(),
        })
        .await;
    let Response::Rejected(error) = response else {
        panic!("damaged selected cut must be rejected")
    };
    assert!(error.contains("node data object is 7 bytes"), "{error}");
    let Response::Observed {
        rows,
        callbacks,
        quiescent,
        watermark_us,
    } = peers[active[0]].request(&Command::Observe).await
    else {
        panic!("expected failed restoration observation")
    };
    assert!(!quiescent);
    assert_eq!(watermark_us, None);
    assert!(rows.is_empty() && callbacks.is_empty());
    assert_eq!(SharedCut::open(objects).await.reference, cut.reference);
    println!("damaged selected donor rejected with no restored graph: {error}");
    stop(peers, &active).await;
}

async fn qualify_fault(runtime: Runtime, fault: Fault) {
    let directory = tempfile::tempdir().unwrap();
    let namespace = format!("process-peers/{}", Uuid::new_v4());
    println!(
        "qualifying {} / {} in namespace {namespace}",
        serde_json::to_string(&runtime).unwrap(),
        serde_json::to_string(&fault).unwrap()
    );
    let mut peers = Vec::new();
    let result = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(90),
        run(&mut peers, directory.path(), &namespace, runtime, fault),
    ))
    .catch_unwind()
    .await;
    let mut cleanup = Vec::new();
    for peer in &mut peers {
        if peer.child.try_wait().is_ok_and(|status| status.is_some()) {
            continue;
        }
        if let Err(error) = peer.child.start_kill() {
            cleanup.push(format!("PID {} kill: {error}", peer.pid));
        }
        match tokio::time::timeout(PEER_DEADLINE, peer.child.wait()).await {
            Ok(Ok(_)) => {}
            other => cleanup.push(format!("PID {} wait: {other:?}", peer.pid)),
        }
    }
    match result {
        Ok(Ok(())) => assert!(cleanup.is_empty(), "cleanup: {cleanup:?}"),
        Ok(Err(error)) => panic!("qualification deadline: {error}; cleanup: {cleanup:?}"),
        Err(error) => {
            let primary = error
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| error.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic");
            assert!(
                cleanup.is_empty(),
                "qualification failure: {primary}; cleanup: {cleanup:?}"
            );
            std::panic::resume_unwind(error);
        }
    }
}

pub(super) async fn qualify(runtime: Runtime) {
    for fault in [
        Fault::ReplacementAndJoin,
        Fault::OwnerExit,
        Fault::FinalOwnerLoss,
    ] {
        qualify_fault(runtime, fault).await;
    }
}
