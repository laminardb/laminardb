use super::*;

pub(super) struct Process {
    node: u64,
    pid: u32,
    root: PathBuf,
    sequence: u32,
    child: tokio::process::Child,
    exit: ExpectedExit,
}

enum ExpectedExit {
    Clean,
    Killed,
}

impl Process {
    async fn response(&mut self, name: &str) -> Result<Response> {
        let path = self.root.join(name);
        tokio::time::timeout(DEADLINE, async {
            loop {
                if path.exists() {
                    return read_message(&path);
                }
                if let Some(status) = self.child.try_wait()? {
                    return Err(anyhow!(
                        "database {} exited before {name}: {status}; stdout: {}; stderr: {}",
                        self.node,
                        std::fs::read_to_string(self.root.join("stdout.log"))?,
                        std::fs::read_to_string(self.root.join("stderr.log"))?
                    ));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("database response deadline expired")?
    }

    pub(super) async fn request(&mut self, command: &Command) -> Result<Response> {
        let sequence = self.sequence;
        self.sequence += 1;
        write_message(&self.root.join(format!("request-{sequence}.json")), command)?;
        self.response(&format!("response-{sequence}.json")).await
    }

    pub(super) async fn done(&mut self, command: &Command) -> Result<()> {
        let response = self.request(command).await?;
        match response {
            Response::Done => Ok(()),
            response => Err(anyhow!(
                "expected database command completion: {response:?}"
            )),
        }
    }

    pub(super) async fn observe(&mut self) -> Result<Observation> {
        match self.request(&Command::Observe).await? {
            Response::Observed(observed) => Ok(*observed),
            response => Err(anyhow!("expected database observation: {response:?}")),
        }
    }

    pub(super) async fn kill(&mut self) -> Result<()> {
        self.child.start_kill()?;
        let status = tokio::time::timeout(DEADLINE, self.child.wait()).await??;
        if status.success() {
            return Err(anyhow!("failed database exited successfully"));
        }
        self.exit = ExpectedExit::Killed;
        println!(
            "killed database {} in PID {}: {status}",
            self.node, self.pid
        );
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(status) = self.child.try_wait()? {
            return match self.exit {
                ExpectedExit::Clean if status.success() => Ok(()),
                ExpectedExit::Killed if !status.success() => Ok(()),
                ExpectedExit::Clean | ExpectedExit::Killed => {
                    Err(anyhow!("unexpected database cleanup exit: {status}"))
                }
            };
        }
        let stopped = self.done(&Command::Stop).await;
        let joined = tokio::time::timeout(DEADLINE, self.child.wait()).await;
        match joined {
            Ok(Ok(status)) if status.success() => stopped,
            Ok(Ok(status)) => Err(anyhow!(
                "database cleanup exit: {status}; command: {stopped:?}"
            )),
            result => {
                let killed = tokio::time::timeout(DEADLINE, self.child.kill()).await;
                Err(anyhow!(
                    "database cleanup join: {result:?}; command: {stopped:?}; kill: {killed:?}"
                ))
            }
        }
    }
}

fn spawn(root: &Path, config: &PeerConfig) -> Result<Process> {
    let directory = root.join(config.node.to_string());
    std::fs::create_dir(&directory)?;
    write_message(&directory.join("config.json"), config)?;
    let child = tokio::process::Command::new(std::env::current_exe()?)
        .args(["--exact", TEST_NAME, "--ignored", "--nocapture"])
        .env(CHILD_ENV, &directory)
        .stdout(std::fs::File::create(directory.join("stdout.log"))?)
        .stderr(std::fs::File::create(directory.join("stderr.log"))?)
        .kill_on_drop(true)
        .spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| anyhow!("fixture child has no process ID"))?;
    Ok(Process {
        node: config.node,
        pid,
        root: directory,
        sequence: 0,
        child,
        exit: ExpectedExit::Clean,
    })
}

pub(super) async fn ready(process: &mut Process) -> Result<std::net::SocketAddr> {
    match process.response("ready.json").await? {
        Response::Ready { pid, address } if pid == process.pid && pid != std::process::id() => {
            println!("database {} ready in PID {pid} at {address}", process.node);
            Ok(address)
        }
        response => Err(anyhow!("invalid database process readiness: {response:?}")),
    }
}

pub(super) async fn wait_open(process: &mut Process, epoch: u64) -> Result<RecoveryAnnouncement> {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let observed = process.observe().await?;
            if !observed.fenced {
                if let Some(release) = observed.release {
                    if release.phase == (RecoverPhase::ReleaseCommitted { epoch }) {
                        return Ok(release);
                    }
                }
            }
            if observed.fault.is_some() {
                return Err(anyhow!(
                    "database fault while waiting for Release: {:?}",
                    observed.fault
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("database did not open after durable Release")?
}

async fn wait_totals(processes: &mut [Process], expected: &BTreeMap<i64, i64>) -> Result<()> {
    let mut last = BTreeMap::new();
    tokio::time::timeout(DEADLINE, async {
        loop {
            let mut totals = BTreeMap::new();
            for process in &mut *processes {
                let observed = process.observe().await?;
                if observed.fault.is_some() {
                    return Err(anyhow!("aggregate fault: {:?}", observed.fault));
                }
                for (key, total) in observed.output {
                    if totals.insert(key, total).is_some() {
                        return Err(anyhow!("more than one owner emitted fixture key {key}"));
                    }
                }
            }
            if &totals == expected {
                return Ok(());
            }
            last = totals;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .with_context(|| format!("database output did not reach {expected:?}; last totals: {last:?}"))?
}

pub(super) async fn held_start(
    survivor: &mut Process,
    reference: &CommittedCheckpointRef,
    generation: u64,
) -> Result<Observation> {
    let mut last = None;
    tokio::time::timeout(DEADLINE, async {
        loop {
            let observed = survivor.observe().await?;
            if let Some(start) = observed.intent.as_ref() {
                if start.round.id.generation > generation
                    && start.phase
                        == (RecoverPhase::Start {
                            epoch: reference.epoch,
                        })
                    && observed
                        .starts
                        .last()
                        .is_some_and(|resume| resume.checkpoint_id == reference.checkpoint_id)
                {
                    return Ok(observed);
                }
            }
            last = Some(observed);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .with_context(|| {
        format!(
            "surviving database did not hold the committed-cut Start; last observation: {last:?}"
        )
    })?
}

pub(super) async fn start_pair(processes: &mut [Process; 2]) -> Result<RecoveryAnnouncement> {
    let [survivor, failed] = processes;
    let (left, right) = tokio::join!(ready(survivor), ready(failed));
    let left = left?;
    let right = right?;
    assert_ne!(survivor.pid, failed.pid);
    survivor.done(&Command::Connect(8, right)).await?;
    failed.done(&Command::Connect(7, left)).await?;
    survivor.done(&Command::Catalog).await?;
    failed.done(&Command::Catalog).await?;
    let (left, right) = tokio::join!(survivor.done(&Command::Start), failed.done(&Command::Start));
    left?;
    right?;
    let first = wait_open(survivor, 0).await?;
    assert_eq!(wait_open(failed, 0).await?.round, first.round);
    Ok(first)
}

async fn qualify(processes: &mut [Process; 2]) -> Result<()> {
    let first = start_pair(processes).await?;
    let [survivor, failed] = processes;
    survivor.done(&Command::Prefix(2)).await?;
    failed.done(&Command::Prefix(2)).await?;
    let keys = connectors::keys()?;
    let committed = BTreeMap::from([(keys[0], 32), (keys[1], 30)]);
    wait_totals(processes, &committed).await?;
    let Response::Committed {
        reference,
        offsets,
        participants,
        ..
    } = processes[0].request(&Command::Checkpoint).await?
    else {
        return Err(anyhow!("expected committed checkpoint"));
    };
    let cursors = BTreeMap::from([
        ("partition-0".into(), "2".into()),
        ("partition-1".into(), "2".into()),
    ]);
    assert_eq!(offsets, cursors);
    assert_eq!(participants, [7, 8]);
    println!("selected shared checkpoint: {reference:?}; offsets: {offsets:?}");
    for process in &mut *processes {
        process.done(&Command::Prefix(3)).await?;
    }
    wait_totals(processes, &BTreeMap::from([(keys[0], 63), (keys[1], 60)])).await?;
    processes[0].done(&Command::HoldRecovery).await?;
    processes[1].kill().await?;
    processes[0].done(&Command::RemoveFailedPeer(8)).await?;
    qualify_recovered_owner(&mut processes[0], &reference, &cursors, &first).await?;
    wait_totals(
        &mut processes[..1],
        &BTreeMap::from([(keys[0], 104), (keys[1], 100)]),
    )
    .await
}

async fn qualify_recovered_owner(
    survivor: &mut Process,
    reference: &CommittedCheckpointRef,
    cursors: &BTreeMap<String, String>,
    first: &RecoveryAnnouncement,
) -> Result<()> {
    let held = held_start(survivor, reference, first.round.id.generation).await?;
    assert!(held.fenced);
    let start = held.intent.as_ref().unwrap();
    assert_eq!(start.round.assignment_fence.assignment_version, 2);
    assert_eq!(start.round.assignment_fence.participant_ids(), [7]);
    assert!(start.round.assignment_fence.matches_owner_map(&[7, 7]));
    assert_eq!(held.handoff.as_ref(), Some(reference));
    let resume = held.starts.last().unwrap();
    assert_eq!(resume.assignment, 2);
    assert_eq!(&resume.offsets, cursors);
    assert_eq!(held.release.as_ref().unwrap().round, first.round);
    survivor.done(&Command::Prefix(4)).await?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let still_held = survivor.observe().await?;
    assert!(still_held.fenced);
    assert_eq!(still_held.polls, held.polls);
    assert!(still_held.output.is_empty());
    survivor.done(&Command::Release).await?;
    let released = wait_open(survivor, reference.epoch).await?;
    assert_eq!(&released.round, &start.round);
    let current = survivor.observe().await?;
    assert_eq!(
        current.assignment,
        Some(start.round.assignment_fence.clone())
    );
    println!(
        "survivor released committed epoch {} in recovery generation {} with assignment {}",
        reference.epoch,
        released.round.id.generation,
        released.round.assignment_fence.assignment_version
    );
    Ok(())
}

pub(super) async fn run() -> Result<()> {
    run_process(Runtime::Aggregate, 8).await.map(|_| ())
}

pub(super) async fn run_process(runtime: Runtime, failed: u64) -> Result<process::Transcript> {
    let root = tempfile::tempdir()?;
    let namespace = format!("committed-database/{}", Uuid::new_v4());
    let objects = shared_store(&namespace)?;
    let assignments = AssignmentSnapshotStore::new(objects);
    let assignment = AssignmentSnapshot::empty().next_for_participants(
        AssignmentSnapshot::vnodes_from_vec(&[NodeId(7), NodeId(8)]),
        participants(),
    )?;
    assignments.save_if_absent(&assignment).await?;
    let first = spawn(
        root.path(),
        &PeerConfig {
            namespace: namespace.clone(),
            node: 7,
            assignment: assignment.clone(),
            runtime,
        },
    )?;
    let second = spawn(
        root.path(),
        &PeerConfig {
            namespace,
            node: 8,
            assignment,
            runtime,
        },
    );
    let mut processes = match second {
        Ok(second) => [first, second],
        Err(error) => {
            let mut first = first;
            let cleanup = first.close().await;
            return Err(error.context(format!("first database cleanup: {cleanup:?}")));
        }
    };
    let outcome = std::panic::AssertUnwindSafe(async {
        match runtime {
            Runtime::Aggregate => qualify(&mut processes)
                .await
                .map(|()| (Vec::new(), Vec::new())),
            Runtime::Native => process::qualify(&mut processes, failed).await,
            #[cfg(feature = "process-remote")]
            Runtime::RemoteRust => process::qualify(&mut processes, failed).await,
        }
    })
    .catch_unwind()
    .await;
    let mut cleanup = Vec::new();
    for process in &mut processes {
        if let Err(error) = process.close().await {
            cleanup.push(error.to_string());
        }
    }
    match outcome {
        Ok(Ok(transcript)) if cleanup.is_empty() => Ok(transcript),
        Ok(Ok(_)) => Err(anyhow!(
            "database qualification cleanup failed: {cleanup:?}"
        )),
        Ok(Err(error)) => {
            Err(error.context(format!("database qualification cleanup: {cleanup:?}")))
        }
        Err(primary) => {
            if !cleanup.is_empty() {
                eprintln!("database qualification cleanup: {cleanup:?}");
            }
            std::panic::resume_unwind(primary);
        }
    }
}
