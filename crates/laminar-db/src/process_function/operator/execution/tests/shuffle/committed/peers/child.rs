use super::*;
use futures::FutureExt;

struct Owner {
    fixture: Fixture,
    objects: Arc<dyn ObjectStore>,
    graph: Option<OperatorGraph>,
    descriptor: ProcessFunctionDescriptor,
    handler: ProcessHandler,
    observed: Arc<RecordingActivity>,
    rows: Vec<ActivityRow>,
    watermark: i64,
}

impl Owner {
    async fn tick(&mut self) {
        let Some(graph) = &mut self.graph else { return };
        let output = graph
            .execute_cycle(&rustc_hash::FxHashMap::default(), self.watermark, None)
            .await
            .unwrap();
        self.retain(output);
    }

    fn retain(&mut self, output: rustc_hash::FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        if let Some(batches) = output.get("activity") {
            self.rows.extend(activity_rows(batches));
        }
        assert!(self.rows.len() <= 64);
    }

    fn observe(&mut self) -> Response {
        let mut watermark_us = None;
        let quiescent = self.graph.as_mut().is_some_and(|graph| {
            if !graph.checkpoint_is_quiescent() {
                return false;
            }
            let (whole, _) = materialize(graph.capture_state(MAX_MESSAGE_BYTES).unwrap());
            let metadata: serde_json::Value = serde_json::from_slice(&whole[0].1).unwrap();
            watermark_us = metadata["watermark_us"].as_i64();
            true
        });
        Response::Observed {
            rows: std::mem::take(&mut self.rows),
            callbacks: std::mem::take(&mut *self.observed.callbacks.lock()),
            quiescent,
            watermark_us,
        }
    }

    async fn restore(
        &mut self,
        fence: CheckpointAssignmentFence,
        owners: [u64; 4],
        recovery: u64,
        reference: CommittedCheckpointRef,
    ) -> Result<Response, DbError> {
        let previous = self.graph.take();
        let installed = self.fixture.scope.registry.versioned_snapshot();
        if installed.version() == fence.assignment_version {
            assert_eq!(installed.owners(), &owners.map(NodeId));
        } else {
            self.fixture.scope.registry.set_assignment_and_version(
                Arc::from(owners.map(NodeId)),
                fence.assignment_version,
            );
        }
        self.fixture
            .scope
            .sender
            .install_assignment_fence(&fence, &owners)
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        self.fixture
            .scope
            .receiver
            .install_assignment_fence(&fence, &owners)
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        self.fixture.scope.sender.set_recovery_gen(recovery);
        self.fixture.scope.receiver.set_recovery_gen(recovery);
        if let Some(mut old) = previous {
            assert!(
                old.execute_cycle(&source_batch(&[(&key_for(0), 999, 130_000)]), 130, None)
                    .await
                    .is_err(),
                "the old graph must reject changed execution authority"
            );
        }
        self.fixture.binding = InstalledVnodeStateBinding::new(fence, PipelineIdentity::empty())?;
        let objects = Arc::clone(&self.objects);
        let cut = SharedCut::open(objects).await;
        assert_eq!(
            cut.reference, reference,
            "recovery must use the selected committed cut"
        );
        let recovered = cut.recover(&self.fixture).await?;
        self.watermark = recovered.checkpoint_watermark().unwrap();
        let response = Response::Restored {
            reference,
            reassigned: recovered.reassigned,
            offsets: recovered.source_offsets()["events"].offsets.clone(),
            watermark: recovered.checkpoint_watermark(),
        };
        self.graph = Some(restore_graph(
            &self.fixture,
            &recovered,
            self.descriptor.clone(),
            self.handler.clone(),
        )?);
        Ok(response)
    }

    async fn command(&mut self, command: Command) -> Response {
        match command {
            Command::Connect(peers) => {
                for (peer, address) in peers {
                    self.fixture.scope.sender.register_peer(peer, address);
                }
            }
            Command::Input(rows) => {
                let rows = rows
                    .iter()
                    .map(|(key, amount, time)| (key.as_str(), *amount, *time))
                    .collect::<Vec<_>>();
                let output = self
                    .graph
                    .as_mut()
                    .unwrap()
                    .execute_cycle(&source_batch(&rows), self.watermark, None)
                    .await
                    .unwrap();
                self.retain(output);
            }
            Command::Advance(watermark) => self.watermark = watermark,
            Command::Observe => return self.observe(),
            Command::Barrier => {
                let fence = self.fixture.binding.assignment();
                let peers = fence
                    .participants
                    .iter()
                    .filter_map(|participant| {
                        (participant.node_id != self.fixture.scope.self_id.0)
                            .then_some(participant.node_id)
                    })
                    .collect::<Vec<_>>();
                self.fixture
                    .scope
                    .sender
                    .fan_out_barrier(&peers, CheckpointBarrier::new(1, 1), fence)
                    .await
                    .unwrap();
            }
            Command::Capture(deployment) => {
                let graph = self.graph.as_mut().unwrap();
                graph
                    .align_shuffle_barriers(
                        CheckpointAttempt::canonical(1),
                        105,
                        self.fixture.binding.assignment(),
                        tokio::time::Instant::now() + PEER_DEADLINE,
                        None,
                    )
                    .await
                    .unwrap();
                let (manifest, payload) = capture(graph, &self.fixture, &deployment);
                let store =
                    checkpoint_store(Arc::clone(&self.objects), self.fixture.scope.self_id.0);
                let encoded = store.save_checkpoint(&manifest, &[payload]).await.unwrap();
                self.fixture
                    .scope
                    .receiver
                    .retire_checkpoint_barriers(
                        CheckpointAttempt::canonical(1),
                        self.fixture.binding.assignment().digest(),
                    )
                    .unwrap();
                return Response::Captured {
                    manifest: Box::new(manifest),
                    encoded: encoded.to_vec(),
                };
            }
            Command::Restore {
                fence,
                owners,
                recovery,
                reference,
            } => {
                return self
                    .restore(fence, owners, recovery, reference)
                    .await
                    .unwrap_or_else(|error| Response::Rejected(error.to_string()));
            }
            Command::Pause => {}
            Command::Stop => unreachable!("the command loop owns peer shutdown"),
        }
        Response::Done
    }

    async fn serve(&mut self, root: &Path) {
        let mut sequence = 0_u32;
        let mut running = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        while tokio::time::Instant::now() < deadline {
            let request = root.join(format!("request-{sequence}.json"));
            if request.exists() {
                let command: Command = read_message(&request);
                let stop = matches!(command, Command::Stop);
                match &command {
                    Command::Pause | Command::Restore { .. } => running = false,
                    Command::Input(_) | Command::Advance(_) => running = true,
                    Command::Connect(_)
                    | Command::Observe
                    | Command::Barrier
                    | Command::Capture(_)
                    | Command::Stop => {}
                }
                let response = if stop {
                    Response::Done
                } else {
                    self.command(command).await
                };
                write_message(&root.join(format!("response-{sequence}.json")), &response);
                if stop {
                    return;
                }
                sequence += 1;
                assert!(sequence <= 4_096, "fixture command inventory is bounded");
            }
            if running {
                self.tick().await;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("peer exceeded its fixture lifetime");
    }
}

pub(super) async fn run(root: &Path) {
    let config: PeerConfig = read_message(&root.join("config.json"));
    let objects = shared_objects(&config.namespace);
    let authority = Arc::new(ProcessLeaseAuthority::new(Arc::clone(&objects), PEER_TTL).unwrap());
    let mut fixture = Fixture::for_assignment(
        authority,
        NodeId(config.node),
        config.fence,
        config.owners,
        3,
        PEER_TTL,
    )
    .await;
    let shutdown = CancellationToken::new();
    let renewal = fixture
        .lease_manager
        .take()
        .unwrap()
        .spawn(shutdown.clone());
    let observed = Arc::new(RecordingActivity::default());
    let binding = descriptor();
    #[cfg(feature = "process-remote")]
    let (binding, worker) = match config.runtime {
        Runtime::Native => (binding, None),
        Runtime::RemoteRust => {
            let mut binding = binding;
            binding.runtime = crate::process_function::ProcessRuntime::RemoteRust;
            let worker = crate::process_function::operator::execution::tests::remote::Worker::new(
                binding.clone(),
                observed.clone(),
            )
            .await;
            (binding, Some(worker))
        }
    };
    let handler = match config.runtime {
        Runtime::Native => ProcessHandler::Native(observed.clone()),
        #[cfg(feature = "process-remote")]
        Runtime::RemoteRust => ProcessHandler::Remote(Arc::clone(&worker.as_ref().unwrap().client)),
    };
    let graph = fixture
        .graph(binding.clone(), handler.clone())
        .bind_startup_assignment(&fixture.binding, &fixture.controller)
        .unwrap();
    write_message(
        &root.join("ready.json"),
        &Response::Ready {
            pid: std::process::id(),
            address: fixture.scope.receiver.local_addr(),
        },
    );
    let mut owner = Owner {
        fixture,
        objects,
        graph: Some(graph),
        descriptor: binding,
        handler,
        observed,
        rows: Vec::new(),
        watermark: 95,
    };
    let result = std::panic::AssertUnwindSafe(owner.serve(root))
        .catch_unwind()
        .await;
    drop(owner.graph.take());
    shutdown.cancel();
    tokio::time::timeout(PEER_DEADLINE, renewal)
        .await
        .unwrap()
        .unwrap();
    #[cfg(feature = "process-remote")]
    if let Some(worker) = worker {
        worker.stop().await;
    }
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
