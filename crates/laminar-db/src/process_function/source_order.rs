//! Cold admission for the v1 accepted-arrival replay profile.
#![allow(clippy::disallowed_types)] // connector-manager registration maps are cold path

use std::collections::HashMap;

use laminar_connectors::connector::{
    DeliveryGuarantee, SourceContract, SourceInputMode, SourceReplayOrder, SourceTopology,
};

use super::ProcessFunctionRegistration;
use crate::connector_manager::SourceRegistration;
use crate::{DbError, LaminarDB};

impl LaminarDB {
    pub(crate) fn validate_process_source_order(
        &self,
        output_name: &str,
        source_name: &str,
        sources: &HashMap<String, SourceRegistration>,
    ) -> Result<(), DbError> {
        if self.config.delivery_guarantee != DeliveryGuarantee::AtLeastOnce {
            return Ok(());
        }
        let (contract, _) = self
            .resolve_registered_source_contract(source_name, sources)?
            .ok_or_else(|| {
                DbError::Unsupported(
                    "at-least-once process functions require a replayable connector source".into(),
                )
            })?;
        admit_process_replay_source(output_name, source_name, contract)
    }

    pub(crate) fn validate_process_source_orders(
        &self,
        registrations: &[ProcessFunctionRegistration],
        sources: &HashMap<String, SourceRegistration>,
    ) -> Result<(), DbError> {
        let mut registrations = registrations.iter().collect::<Vec<_>>();
        registrations.sort_unstable_by_key(|registration| &registration.output_name);
        for registration in registrations {
            self.validate_process_source_order(
                &registration.output_name,
                &registration.source_name,
                sources,
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_instantiated_process_source_order(
        &self,
        source_name: &str,
        contract: SourceContract,
    ) -> Result<(), DbError> {
        if self.config.delivery_guarantee != DeliveryGuarantee::AtLeastOnce {
            return Ok(());
        }
        let manager = self.connector_manager.lock();
        let registration = manager
            .process_functions()
            .values()
            .filter(|registration| registration.source_name == source_name)
            .min_by_key(|registration| &registration.output_name);
        if let Some(registration) = registration {
            admit_process_replay_source(&registration.output_name, source_name, contract)?;
        }
        Ok(())
    }
}

fn admit_process_replay_source(
    output_name: &str,
    source_name: &str,
    contract: SourceContract,
) -> Result<(), DbError> {
    if !contract.supports_replay() {
        return Err(DbError::Unsupported(format!(
            "process function '{output_name}' source '{source_name}' must be replayable"
        )));
    }
    if contract.input_mode != SourceInputMode::AppendOnly
        || contract.topology != SourceTopology::Singleton
        || contract.replay_order != SourceReplayOrder::SingleChannel
    {
        return Err(DbError::Unsupported(format!(
            "process function '{output_name}' source '{source_name}' requires an append-only \
             singleton source with an explicit single-channel replay-order contract; \
             per-partition row positions do not define an independent-channel merge"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::Int64Array;
    use arrow_schema::SchemaRef;
    use async_trait::async_trait;
    use laminar_connectors::checkpoint::SourceCheckpoint;
    use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
    use laminar_connectors::connector::{
        SourceBatch, SourceConnector, SourceConsistency, SourcePosition,
        SourceRowPositionCapability, SourceStart,
    };
    use laminar_connectors::error::ConnectorError;
    use laminar_connectors::registry::ConnectorRegistry;

    use super::*;
    use crate::process_function::tests::{descriptor, input_batch};
    use crate::process_function::{
        NativeProcessFunction, ProcessActivation, ProcessActivationResult, ProcessCallback,
        ValueMutation, ValueState,
    };
    use crate::subscription::{PortalFrame, SubscribeStart, SubscriptionPortal};

    const SOURCE: &str = "process-order-probe";

    fn replayable_contract() -> SourceContract {
        SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Singleton,
            SourceInputMode::AppendOnly,
        )
    }

    struct Probe {
        contract: parking_lot::Mutex<SourceContract>,
        available: AtomicU64,
        starts: parking_lot::Mutex<Vec<u64>>,
        activations: parking_lot::Mutex<Vec<u64>>,
        fail_second: AtomicBool,
    }

    impl Probe {
        fn new(contract: SourceContract) -> Arc<Self> {
            Arc::new(Self {
                contract: parking_lot::Mutex::new(contract),
                available: AtomicU64::new(0),
                starts: parking_lot::Mutex::new(Vec::new()),
                activations: parking_lot::Mutex::new(Vec::new()),
                fail_second: AtomicBool::new(false),
            })
        }

        fn register(self: &Arc<Self>, registry: &ConnectorRegistry) -> Result<(), ConnectorError> {
            let probe = Arc::clone(self);
            registry.register_source(
                SOURCE,
                ConnectorInfo {
                    name: SOURCE.into(),
                    display_name: "Ordered replay probe".into(),
                    version: "1".into(),
                    is_source: true,
                    is_sink: false,
                    config_keys: Vec::new(),
                },
                Arc::new(move |_| {
                    Ok(Box::new(OrderedSource {
                        contract: *probe.contract.lock(),
                        probe: Arc::clone(&probe),
                        cursor: 0,
                    }))
                }),
            )
        }
    }

    struct OrderedSource {
        contract: SourceContract,
        probe: Arc<Probe>,
        cursor: u64,
    }

    #[async_trait]
    impl SourceConnector for OrderedSource {
        fn contract(&self, _: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
            Ok(self.contract)
        }

        async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
            self.cursor = match request.into_parts().1 {
                SourcePosition::Initial => 0,
                SourcePosition::Resume { checkpoint, .. } => checkpoint
                    .get_offset("cursor")
                    .and_then(|cursor| cursor.parse::<u64>().ok())
                    .filter(|cursor| *cursor <= 2)
                    .ok_or_else(|| {
                        ConnectorError::ConfigurationError("invalid ordered probe cursor".into())
                    })?,
                SourcePosition::Initialized { .. } => {
                    return Err(ConnectorError::ConfigurationError(
                        "ordered probe does not support topology startup".into(),
                    ));
                }
            };
            let mut starts = self.probe.starts.lock();
            assert!(starts.len() < 8);
            starts.push(self.cursor);
            Ok(())
        }

        async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
            if self.cursor >= self.probe.available.load(Ordering::Acquire) {
                return Ok(None);
            }
            let records = match self.cursor {
                0 => input_batch(&[("a", 60, 100_000)]),
                1 => input_batch(&[("a", 50, 100_050)]),
                _ => return Ok(None),
            };
            self.cursor += 1;
            Ok(Some(
                SourceBatch::new(records).with_checkpoint(self.checkpoint()),
            ))
        }

        fn schema(&self) -> SchemaRef {
            descriptor().input_schema
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            let mut checkpoint = SourceCheckpoint::new();
            checkpoint.set_offset("cursor", self.cursor.to_string());
            checkpoint
                .set_input_channels(vec![b"ordered".to_vec()])
                .unwrap();
            checkpoint
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    struct Sum(Arc<Probe>);

    impl NativeProcessFunction for Sum {
        fn invoke(
            &self,
            activations: &[ProcessActivation],
        ) -> Result<Vec<ProcessActivationResult>, DbError> {
            activations
                .iter()
                .map(|activation| {
                    let ProcessCallback::Input(records) = &activation.callback else {
                        panic!("timer-free fixture received a timer callback");
                    };
                    let mut ids = self.0.activations.lock();
                    assert!(ids.len() < 8);
                    ids.push(activation.id);
                    drop(ids);
                    let amount = records
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0);
                    if amount == 50 && self.0.fail_second.swap(false, Ordering::AcqRel) {
                        return Err(DbError::Pipeline("ordered probe failure".into()));
                    }
                    let prior = match activation.state {
                        ValueState::Absent => 0,
                        ValueState::Value(value) => value,
                        ValueState::Null => panic!("ordered probe stored null state"),
                    };
                    let total = prior + amount;
                    Ok(ProcessActivationResult {
                        activation_id: activation.id,
                        value: ValueMutation::Set(total),
                        timers: Vec::new(),
                        output: vec![input_batch(&[(
                            &activation.key_text,
                            total,
                            activation.event_time_us,
                        )])],
                    })
                })
                .collect()
        }
    }

    fn binding() -> crate::process_function::ProcessFunctionDescriptor {
        let mut binding = descriptor();
        binding.timer_names.clear();
        binding.output_schema = Arc::clone(&binding.input_schema);
        binding
    }

    async fn database(
        path: &std::path::Path,
        probe: &Arc<Probe>,
        delivery: DeliveryGuarantee,
    ) -> Arc<LaminarDB> {
        let probe = Arc::clone(probe);
        let db = LaminarDB::builder()
            .storage_dir(path)
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: None,
                ..Default::default()
            })
            .delivery_guarantee(delivery)
            .register_connector(move |registry| probe.register(registry))
            .build()
            .await
            .unwrap();
        db.execute(&format!(
            "CREATE SOURCE events (account VARCHAR NOT NULL, amount BIGINT NOT NULL, \
             ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND) \
             FROM \"{SOURCE}\""
        ))
        .await
        .unwrap();
        db
    }

    async fn next_total(portal: &mut SubscriptionPortal) -> i64 {
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(frame) = portal.next_frame().await {
                if let PortalFrame::Batch { batch, .. } = frame {
                    return batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0);
                }
            }
            panic!("ordered probe subscription ended");
        })
        .await
        .unwrap()
    }

    #[test]
    fn replay_profile_requires_more_than_per_partition_positions() {
        let ordered = replayable_contract().with_replay_order(SourceReplayOrder::SingleChannel);
        let mut ephemeral = ordered;
        ephemeral.consistency = SourceConsistency::Ephemeral;
        let mut splittable = ordered;
        splittable.topology = SourceTopology::Splittable;
        let mut upsert = ordered;
        upsert.input_mode = SourceInputMode::KeyedUpsert;
        for contract in [
            replayable_contract(),
            replayable_contract()
                .with_row_positions(SourceRowPositionCapability::OrderedDeterministic),
            ephemeral,
            splittable,
            upsert,
        ] {
            assert!(admit_process_replay_source("activity", "events", contract).is_err());
        }
        assert!(admit_process_replay_source("activity", "events", ordered).is_ok());
        let mut coupled = ordered;
        coupled.consistency = SourceConsistency::CommitCoupled;
        assert!(admit_process_replay_source("activity", "events", coupled).is_ok());
    }

    #[tokio::test]
    async fn rejected_order_leaves_registration_available_and_source_unstarted() {
        let directory = tempfile::tempdir().unwrap();
        let probe = Probe::new(
            replayable_contract()
                .with_row_positions(SourceRowPositionCapability::OrderedDeterministic),
        );
        let db = database(directory.path(), &probe, DeliveryGuarantee::AtLeastOnce).await;
        let error = db
            .register_native_process_function(
                "activity",
                "events",
                binding(),
                Arc::new(Sum(Arc::clone(&probe))),
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("single-channel replay-order"),
            "{error}"
        );
        assert!(db.process_functions().is_empty());
        assert!(probe.starts.lock().is_empty());
        *probe.contract.lock() =
            replayable_contract().with_replay_order(SourceReplayOrder::SingleChannel);
        db.register_native_process_function(
            "activity",
            "events",
            binding(),
            Arc::new(Sum(Arc::clone(&probe))),
        )
        .await
        .unwrap();
        assert!(db
            .validate_instantiated_process_source_order("events", replayable_contract())
            .is_err());
        db.start().await.unwrap();
        db.shutdown().await.unwrap();
        assert_eq!(probe.starts.lock().as_slice(), &[0]);
    }

    #[tokio::test]
    async fn startup_rechecks_source_order_before_io() {
        let directory = tempfile::tempdir().unwrap();
        let probe =
            Probe::new(replayable_contract().with_replay_order(SourceReplayOrder::SingleChannel));
        let db = database(directory.path(), &probe, DeliveryGuarantee::AtLeastOnce).await;
        db.register_native_process_function(
            "activity",
            "events",
            binding(),
            Arc::new(Sum(Arc::clone(&probe))),
        )
        .await
        .unwrap();
        *probe.contract.lock() = replayable_contract();
        let error = db.start().await.unwrap_err();
        assert!(
            error.to_string().contains("single-channel replay-order"),
            "{error}"
        );
        assert!(probe.starts.lock().is_empty());
        db.shutdown().await.unwrap();
    }

    #[cfg(feature = "files")]
    #[tokio::test]
    async fn file_source_does_not_qualify_process_replay_order() {
        let directory = tempfile::tempdir().unwrap();
        let db = LaminarDB::builder()
            .storage_dir(directory.path().join("checkpoint"))
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig::default())
            .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
            .build()
            .await
            .unwrap();
        let input = directory.path().join("absent-input");
        let path = input.to_string_lossy().replace('\\', "/");
        db.execute(&format!(
            "CREATE SOURCE events (account VARCHAR NOT NULL, amount BIGINT NOT NULL, \
             ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND) \
             FROM FILES ('path' = '{path}') FORMAT JSON"
        ))
        .await
        .unwrap();
        let probe = Probe::new(replayable_contract());
        let error = db
            .register_native_process_function("activity", "events", binding(), Arc::new(Sum(probe)))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("single-channel replay-order"),
            "{error}"
        );
        assert!(db.process_functions().is_empty());
        assert!(!input.exists());
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn best_effort_retains_accepted_arrival_without_replay_certification() {
        let directory = tempfile::tempdir().unwrap();
        let probe = Probe::new(replayable_contract());
        let db = database(directory.path(), &probe, DeliveryGuarantee::BestEffort).await;
        db.register_native_process_function(
            "activity",
            "events",
            binding(),
            Arc::new(Sum(Arc::clone(&probe))),
        )
        .await
        .unwrap();
        db.start().await.unwrap();
        db.shutdown().await.unwrap();
        assert_eq!(probe.starts.lock().as_slice(), &[0]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ordered_source_restores_committed_state_and_replays_failed_activation() {
        let directory = tempfile::tempdir().unwrap();
        let probe =
            Probe::new(replayable_contract().with_replay_order(SourceReplayOrder::SingleChannel));
        let first = database(directory.path(), &probe, DeliveryGuarantee::AtLeastOnce).await;
        first
            .register_native_process_function(
                "activity",
                "events",
                binding(),
                Arc::new(Sum(Arc::clone(&probe))),
            )
            .await
            .unwrap();
        let mut portal = first
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        first.start().await.unwrap();
        probe.available.store(1, Ordering::Release);
        assert_eq!(next_total(&mut portal).await, 60);
        assert!(first.checkpoint().await.unwrap().success);
        probe.fail_second.store(true, Ordering::Release);
        probe.available.store(2, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(5), async {
            while first.last_fault().is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(first
            .shutdown()
            .await
            .unwrap_err()
            .to_string()
            .contains("ordered probe failure"));
        drop(portal);
        drop(first);

        let restored = database(directory.path(), &probe, DeliveryGuarantee::AtLeastOnce).await;
        restored
            .register_native_process_function(
                "activity",
                "events",
                binding(),
                Arc::new(Sum(Arc::clone(&probe))),
            )
            .await
            .unwrap();
        let mut portal = restored
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        restored.start().await.unwrap();
        assert_eq!(next_total(&mut portal).await, 110);
        restored.shutdown().await.unwrap();
        assert_eq!(probe.starts.lock().as_slice(), &[0, 1]);
        assert_eq!(probe.activations.lock().as_slice(), &[0, 1, 1]);
    }

    #[cfg(feature = "process-remote")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_registration_enforces_the_same_source_order_contract() {
        use crate::process_function::remote::{RemoteProcessClient, RustReferenceWorker};
        use crate::process_function::ProcessRuntime;

        let directory = tempfile::tempdir().unwrap();
        let probe = Probe::new(replayable_contract());
        let db = database(directory.path(), &probe, DeliveryGuarantee::AtLeastOnce).await;
        let mut binding = binding();
        binding.runtime = ProcessRuntime::RemoteRust;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let worker =
            RustReferenceWorker::new(binding.clone(), Arc::new(Sum(Arc::clone(&probe))), 2)
                .unwrap();
        let shutdown = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(worker.serve_loopback(listener, shutdown.clone()));
        let client = Arc::new(
            RemoteProcessClient::connect_loopback(
                &format!("http://{address}"),
                binding.clone(),
                2,
                Duration::from_secs(5),
            )
            .await
            .unwrap(),
        );
        let error = db
            .register_remote_process_function(
                "activity",
                "events",
                binding.clone(),
                Arc::clone(&client),
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("single-channel replay-order"),
            "{error}"
        );
        assert!(db.process_functions().is_empty());
        assert!(probe.starts.lock().is_empty());
        *probe.contract.lock() =
            replayable_contract().with_replay_order(SourceReplayOrder::SingleChannel);
        db.register_remote_process_function("activity", "events", binding, client)
            .await
            .unwrap();
        let mut portal = db
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        db.start().await.unwrap();
        probe.available.store(1, Ordering::Release);
        assert_eq!(next_total(&mut portal).await, 60);
        db.shutdown().await.unwrap();
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
