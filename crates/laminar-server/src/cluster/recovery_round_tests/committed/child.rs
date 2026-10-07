use super::*;
use connectors::ReplayProbe;

struct DatabasePeer {
    peer: Peer,
    probe: Arc<ReplayProbe>,
    sender: Option<Arc<ShuffleSender>>,
    registry: Option<Arc<VnodeRegistry>>,
    assignments: Arc<AssignmentSnapshotStore>,
    rebalance_shutdown: CancellationToken,
    rebalance_tasks: Vec<tokio::task::JoinHandle<()>>,
    runtime: Runtime,
    #[cfg(feature = "process-remote")]
    worker: Option<tokio::task::JoinHandle<Result<(), laminar_db::DbError>>>,
}

impl DatabasePeer {
    fn clear_process_observation(&self) {
        self.probe.callbacks.lock().clear();
        self.probe.activity.lock().clear();
    }

    async fn install_process_catalog(&mut self) -> Result<()> {
        use laminar_db::process_function::NativeProcessFunction;
        let binding = process::descriptor();
        let handler: Arc<dyn NativeProcessFunction> =
            Arc::new(process::Activity(Arc::clone(&self.probe)));
        match self.runtime {
            Runtime::Native => {
                self.db()?
                    .register_native_process_function(
                        "recovery_output",
                        "recovery_input",
                        binding,
                        handler,
                    )
                    .await?
            }
            #[cfg(feature = "process-remote")]
            Runtime::RemoteRust => {
                let mut binding = binding;
                use laminar_db::process_function::{
                    remote::{RemoteProcessClient, RustReferenceWorker},
                    ProcessRuntime,
                };
                binding.runtime = ProcessRuntime::RemoteRust;
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
                let address = listener.local_addr()?;
                let worker = RustReferenceWorker::new(binding.clone(), handler, 4)?;
                self.worker = Some(tokio::spawn(
                    worker.serve_loopback(listener, self.peer.shutdown.clone()),
                ));
                let client = RemoteProcessClient::connect_loopback(
                    &format!("http://{address}"),
                    binding.clone(),
                    4,
                    Duration::from_secs(5),
                )
                .await?;
                self.db()?
                    .register_remote_process_function(
                        "recovery_output",
                        "recovery_input",
                        binding,
                        Arc::new(client),
                    )
                    .await?;
            }
            Runtime::Aggregate => {
                return Err(anyhow!("aggregate fixture cannot install process code"))
            }
        }
        let ddl = [
            "CREATE SOURCE recovery_input (account VARCHAR NOT NULL, amount BIGINT NOT NULL, ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '1' MILLISECOND) FROM \"recovery-cut-probe\" ('fixture' = 'process-fixed-v1')".into(),
            self.db()?.process_function_bootstrap_sql("recovery_output")?,
            "CREATE SINK recovery_probe FROM recovery_output INTO \"recovery-output-probe\" ('fixture' = 'process-fixed-v1')".into(),
        ];
        self.db()?.execute_cluster_bootstrap_batch(&ddl).await?;
        Ok(())
    }

    async fn initialize(
        &mut self,
        config: &PeerConfig,
        objects: Arc<dyn object_store::ObjectStore>,
        root: &Path,
    ) -> Result<()> {
        self.peer.controller.install_local_leader_proof_provider();
        self.peer
            .controller
            .start_leased_barrier_server("127.0.0.1:0".parse()?, None, &self.peer.lease)
            .await
            .map_err(anyhow::Error::msg)?;
        let (sender, address, registry) = self
            .peer
            .initialize(
                objects,
                &config.assignment,
                Arc::clone(&self.assignments),
                connectors::register(
                    Arc::clone(&self.probe),
                    root.join("output.jsonl"),
                    self.runtime,
                ),
            )
            .await?;
        self.sender = Some(sender);
        self.registry = Some(registry);
        write_message(
            &root.join("ready.json"),
            &Response::Ready {
                pid: std::process::id(),
                address,
            },
        )
    }

    fn db(&self) -> Result<&Arc<LaminarDB>> {
        self.peer
            .db
            .as_ref()
            .ok_or_else(|| anyhow!("fixture database is not initialized"))
    }

    async fn observe(&self) -> Result<Observation> {
        let controller = &self.peer.controller;
        let registry = self
            .registry
            .as_ref()
            .ok_or_else(|| anyhow!("fixture registry is absent"))?;
        let starts = self.probe.starts.lock().clone();
        let output = self.probe.output.lock().clone();
        let handoff = if registry.assignment_version() > 1 {
            controller
                .checkpoint_authority()?
                .assignment_recovery_decision(registry.assignment_version())
                .await?
                .map(|decision| decision.recovery_checkpoint)
        } else {
            None
        };
        Ok(Observation {
            fenced: self.db()?.cluster_intake_fenced(),
            polls: self.probe.polls.load(Ordering::Acquire),
            starts,
            output,
            assignment: controller.checkpoint_assignment_fence(registry.assignment_version()),
            handoff,
            intent: controller
                .observe_recover()
                .await
                .map_err(anyhow::Error::msg)?,
            release: controller.latest_committed_recover_release().await?,
            fault: self.db()?.last_fault(),
            callbacks: self.probe.callbacks.lock().clone(),
            activity: self.probe.activity.lock().clone(),
        })
    }

    async fn checkpoint(&self) -> Result<Response> {
        let result = self.db()?.checkpoint_with_timeout(DEADLINE).await?;
        if !result.success || result.error.is_some() {
            return Err(anyhow!("fixture checkpoint failed: {result:?}"));
        }
        let authority = self.peer.controller.checkpoint_authority()?;
        let (outcome, index) = authority
            .cluster_outcome_with_committed_checkpoint(result.epoch)
            .await?
            .ok_or_else(|| anyhow!("checkpoint has no exact cluster outcome"))?;
        let index = index.ok_or_else(|| anyhow!("checkpoint has no committed index"))?;
        index.validate().map_err(anyhow::Error::msg)?;
        if !outcome.is_commit() || !index.reassignment_portable {
            return Err(anyhow!(
                "fixture checkpoint is not a portable committed cut"
            ));
        }
        Ok(Response::Committed {
            reference: outcome
                .committed_checkpoint
                .ok_or_else(|| anyhow!("checkpoint reference is absent"))?,
            offsets: index.source_offsets["recovery_input"]
                .offsets
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            participants: index
                .participants
                .iter()
                .map(|participant| participant.participant_id)
                .collect(),
            channels: index.source_offsets["recovery_input"]
                .input_channels
                .clone()
                .unwrap_or_default(),
            watermark: index
                .channel_progress
                .iter()
                .find(|channel| channel.input_channel == process::CHANNEL)
                .and_then(|channel| channel.watermark),
        })
    }

    fn remove_failed_peer(&mut self, failed: u64) -> Result<()> {
        if self.peer.controller.instance_id() == NodeId(failed) || !self.rebalance_tasks.is_empty()
        {
            return Err(anyhow!(
                "fixture accepts one failed-peer transition on the survivor"
            ));
        }
        let db = Arc::clone(self.db()?);
        let registry = Arc::clone(
            self.registry
                .as_ref()
                .ok_or_else(|| anyhow!("fixture registry is absent"))?,
        );
        self.peer._membership.send(
            membership()
                .into_iter()
                .filter(|member| member.id != NodeId(failed))
                .collect(),
        )?;
        let config = laminar_db::rebalance::RebalanceConfig::test_defaults();
        self.rebalance_tasks = vec![
            laminar_db::rebalance::spawn_snapshot_watcher(
                Arc::clone(&db),
                Arc::clone(&self.assignments),
                Arc::clone(&registry),
                self.rebalance_shutdown.clone(),
                config,
                Some(Arc::clone(&self.peer.controller)),
            ),
            laminar_db::rebalance::spawn_rebalance_controller(
                db,
                Arc::clone(&self.peer.controller),
                Arc::clone(&self.assignments),
                registry,
                self.rebalance_shutdown.clone(),
                config,
            ),
        ];
        Ok(())
    }

    async fn command(&mut self, command: Command) -> Result<Response> {
        match command {
            Command::Connect(node, address) => self
                .sender
                .as_ref()
                .ok_or_else(|| anyhow!("fixture sender is absent"))?
                .register_peer(node, address),
            Command::Catalog => {
                if self.runtime != Runtime::Aggregate {
                    self.install_process_catalog().await?;
                    return Ok(Response::Done);
                }
                let ddl = [
                    "CREATE SOURCE recovery_input (account BIGINT, amount BIGINT, ts TIMESTAMP, WATERMARK FOR ts AS ts - INTERVAL '1' SECOND) FROM \"recovery-cut-probe\" ('fixture' = 'bounded-replay-v1')".into(),
                    "CREATE STREAM recovery_output AS SELECT account, SUM(amount) AS total FROM recovery_input GROUP BY account".into(),
                    "CREATE SINK recovery_probe FROM recovery_output INTO \"recovery-output-probe\" ('fixture' = 'bounded-replay-v1')".into(),
                ];
                self.db()?.execute_cluster_bootstrap_batch(&ddl).await?;
            }
            Command::Start => {
                self.db()?.fence_cluster_startup();
                self.db()?
                    .prepare_cluster_startup_recovery_generation(
                        tokio::time::Instant::now() + DEADLINE,
                    )
                    .await?;
                if self.peer.controller.instance_id() == NodeId(7) {
                    let request = self
                        .peer
                        .controller
                        .next_recovery_fault_request()
                        .map_err(anyhow::Error::msg)?;
                    self.peer
                        .controller
                        .report_fault(request)
                        .await
                        .map_err(anyhow::Error::msg)?;
                }
                self.db()?.start().await?;
                self.db()?.enable_coordinated_recovery()?;
            }
            Command::Prefix(prefix) => {
                if prefix
                    > if self.runtime == Runtime::Aggregate {
                        4
                    } else {
                        7
                    }
                {
                    return Err(anyhow!("fixture source prefix exceeds its record bound"));
                }
                self.probe.prefix.store(prefix, Ordering::Release);
            }
            Command::HoldRecovery => {
                self.probe.hold.store(true, Ordering::Release);
                self.probe.output.lock().clear();
                self.clear_process_observation();
            }
            Command::RemoveFailedPeer(failed) => self.remove_failed_peer(failed)?,
            Command::ClearProcessObservation => self.clear_process_observation(),
            Command::Observe => return Ok(Response::Observed(Box::new(self.observe().await?))),
            Command::Checkpoint => return self.checkpoint().await,
            Command::Release => self.probe.hold.store(false, Ordering::Release),
            Command::Stop => return Err(anyhow!("the peer lifecycle owns Stop")),
        }
        Ok(Response::Done)
    }

    async fn serve(&mut self, root: &Path) -> Result<u32> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
        let mut sequence = 0_u32;
        while tokio::time::Instant::now() < deadline && sequence < 1024 {
            let request = root.join(format!("request-{sequence}.json"));
            if request.exists() {
                let command = read_message(&request)?;
                if matches!(command, Command::Stop) {
                    return Ok(sequence);
                }
                let response = self.command(command).await?;
                write_message(&root.join(format!("response-{sequence}.json")), &response)?;
                sequence += 1;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(anyhow!("fixture command loop expired"))
    }

    async fn close(&mut self) -> Vec<String> {
        self.probe.hold.store(false, Ordering::Release);
        let mut errors = Vec::new();
        if !crate::cluster::stop_rebalance_tasks(
            &mut self.rebalance_tasks,
            &self.rebalance_shutdown,
        )
        .await
        {
            errors.push("rebalance task cleanup failed".into());
        }
        errors.extend(self.peer.close().await);
        #[cfg(feature = "process-remote")]
        if let Some(mut worker) = self.worker.take() {
            match tokio::time::timeout(DEADLINE, &mut worker).await {
                Ok(Ok(Ok(()))) => {}
                result @ Ok(_) => errors.push(format!("process worker cleanup: {result:?}")),
                Err(error) => {
                    errors.push(format!("process worker cleanup: {error}"));
                    worker.abort();
                    if let Err(error) = worker.await {
                        if !error.is_cancelled() {
                            errors.push(error.to_string());
                        }
                    }
                }
            }
        }
        errors
    }
}

pub(super) async fn run(root: &Path) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();
    let config: PeerConfig = read_message(&root.join("config.json"))?;
    let objects = shared_store(&config.namespace)?;
    let peer = Peer::acquire(config.node, Arc::clone(&objects), PROCESS_TTL).await?;
    let mut owner = DatabasePeer {
        peer,
        probe: Arc::new(ReplayProbe::default()),
        sender: None,
        registry: None,
        assignments: Arc::new(AssignmentSnapshotStore::new(Arc::clone(&objects))),
        rebalance_shutdown: CancellationToken::new(),
        rebalance_tasks: Vec::new(),
        runtime: config.runtime,
        #[cfg(feature = "process-remote")]
        worker: None,
    };
    let outcome = std::panic::AssertUnwindSafe(async {
        owner.initialize(&config, objects, root).await?;
        owner.serve(root).await
    })
    .catch_unwind()
    .await;
    let cleanup = owner.close().await;
    match outcome {
        Ok(Ok(sequence)) if cleanup.is_empty() => write_message(
            &root.join(format!("response-{sequence}.json")),
            &Response::Done,
        ),
        Ok(Ok(_)) => Err(anyhow!("database peer cleanup failed: {cleanup:?}")),
        Ok(Err(error)) => Err(error.context(format!("database peer cleanup: {cleanup:?}"))),
        Err(primary) => {
            if !cleanup.is_empty() {
                eprintln!("database peer cleanup: {cleanup:?}");
            }
            std::panic::resume_unwind(primary);
        }
    }
}
