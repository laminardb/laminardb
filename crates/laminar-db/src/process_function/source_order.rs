//! Cold admission for the v1 accepted-arrival replay profile.
#![allow(clippy::disallowed_types)] // connector-manager registration maps are cold path

use std::collections::HashMap;

use laminar_connectors::connector::{DeliveryGuarantee, SourceContract, SourceTopology};

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
        if sources.len() != 1 || self.catalog.list_sources().len() != 1 {
            return Err(DbError::Unsupported(
                "at-least-once process functions require one logical source; independent-source watermark cuts are not replayable".into(),
            ));
        }
        admit_process_replay_source(
            output_name,
            source_name,
            contract,
            self.is_cluster_runtime(),
        )
    }

    pub(crate) fn validate_process_source_orders(
        &self,
        registrations: &[ProcessFunctionRegistration],
        sources: &HashMap<String, SourceRegistration>,
    ) -> Result<(), DbError> {
        let mut registrations = registrations.iter().collect::<Vec<_>>();
        registrations.sort_unstable_by_key(|registration| &registration.output_name);
        for registration in registrations {
            if self.is_cluster_runtime()
                && self
                    .connector_manager
                    .lock()
                    .get_ddl(&registration.output_name)
                    .is_none()
            {
                return Err(DbError::Unsupported(
                    "cluster process binding must be included in startup catalog bootstrap".into(),
                ));
            }
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
            admit_process_replay_source(
                &registration.output_name,
                source_name,
                contract,
                self.is_cluster_runtime(),
            )?;
        }
        Ok(())
    }
}

fn admit_process_replay_source(
    output_name: &str,
    source_name: &str,
    contract: SourceContract,
    cluster: bool,
) -> Result<(), DbError> {
    if cluster && contract.topology != SourceTopology::Splittable {
        return Err(DbError::Unsupported(
            "cluster process functions require a splittable global physical channel".into(),
        ));
    }
    if !contract.supports_replay() {
        return Err(DbError::Unsupported(format!(
            "process function '{output_name}' source '{source_name}' must be replayable"
        )));
    }
    if !contract.supports_fixed_batch_replay() {
        return Err(DbError::Unsupported(format!(
            "process function '{output_name}' source '{source_name}' requires an append-only \
             single-channel replay-order contract with fixed batches and deterministic row positions; \
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
        SourceBatch, SourceConnector, SourceConsistency, SourceInputMode, SourcePosition,
        SourceReplayOrder, SourceRowPositionCapability, SourceRowPositions, SourceStart,
        SourceTopology,
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

    fn cut_contract() -> SourceContract {
        replayable_contract()
            .with_row_positions(SourceRowPositionCapability::OrderedDeterministic)
            .with_replay_order(SourceReplayOrder::SingleChannelFixedBatches)
    }

    struct Probe {
        contract: parking_lot::Mutex<SourceContract>,
        available: AtomicU64,
        starts: parking_lot::Mutex<Vec<u64>>,
        activations: parking_lot::Mutex<Vec<u64>>,
        fail_second: AtomicBool,
        batches: Vec<arrow::array::RecordBatch>,
        callbacks: parking_lot::Mutex<Vec<CallbackStamp>>,
    }

    impl Probe {
        fn new(contract: SourceContract) -> Arc<Self> {
            Arc::new(Self {
                contract: parking_lot::Mutex::new(contract),
                available: AtomicU64::new(0),
                starts: parking_lot::Mutex::new(Vec::new()),
                activations: parking_lot::Mutex::new(Vec::new()),
                fail_second: AtomicBool::new(false),
                batches: vec![
                    input_batch(&[("a", 60, 100_000)]),
                    input_batch(&[("a", 50, 100_050)]),
                ],
                callbacks: parking_lot::Mutex::new(Vec::new()),
            })
        }

        fn register(self: &Arc<Self>, registry: &ConnectorRegistry) -> Result<(), ConnectorError> {
            let probe = Arc::clone(self);
            registry.register_source(
                SOURCE,
                ConnectorInfo {
                    schema_capabilities:
                        laminar_connectors::schema::resolution::SchemaCapabilities::declared(false),
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
                    .filter(|cursor| {
                        usize::try_from(*cursor)
                            .is_ok_and(|cursor| cursor <= self.probe.batches.len())
                    })
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
            let Some(records) =
                self.probe
                    .batches
                    .get(usize::try_from(self.cursor).map_err(|_| {
                        ConnectorError::ConfigurationError("ordered probe cursor overflow".into())
                    })?)
            else {
                return Ok(None);
            };
            let records = records.clone();
            let batch = if self.contract.row_positions
                == SourceRowPositionCapability::OrderedDeterministic
            {
                let key = self.cursor.to_be_bytes();
                let rows = records.num_rows();
                SourceBatch::positioned(
                    records,
                    SourceRowPositions::try_new(
                        arrow::array::BinaryArray::from_vec(vec![b"ordered"; rows]),
                        arrow::array::BinaryArray::from_vec(vec![key.as_slice(); rows]),
                        arrow::array::UInt32Array::from(
                            (0..u32::try_from(rows).unwrap()).collect::<Vec<_>>(),
                        ),
                    )?,
                )?
            } else {
                SourceBatch::new(records)
            };
            self.cursor += 1;
            Ok(Some(batch.with_checkpoint(self.checkpoint())))
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
        buffer: usize,
    ) -> Arc<LaminarDB> {
        let probe = Arc::clone(probe);
        let db = LaminarDB::builder()
            .buffer_size(buffer)
            .source_idle_timeout(Duration::from_millis(1))
            .pipeline_batch_window(Duration::from_millis(50))
            .pipeline_drain_budget_ns(10_000_000)
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
        let ordered = cut_contract();
        let mut ephemeral = ordered;
        ephemeral.consistency = SourceConsistency::Ephemeral;
        let mut node_local = ordered;
        node_local.topology = SourceTopology::NodeLocalIngress;
        let mut upsert = ordered;
        upsert.input_mode = SourceInputMode::KeyedUpsert;
        for contract in [
            replayable_contract(),
            replayable_contract()
                .with_row_positions(SourceRowPositionCapability::OrderedDeterministic),
            replayable_contract().with_replay_order(SourceReplayOrder::SingleChannel),
            replayable_contract().with_replay_order(SourceReplayOrder::SingleChannelFixedBatches),
            ephemeral,
            node_local,
            upsert,
        ] {
            assert!(admit_process_replay_source("activity", "events", contract, false).is_err());
            assert!(admit_process_replay_source("activity", "events", contract, true).is_err());
        }
        assert!(admit_process_replay_source("activity", "events", ordered, false).is_ok());
        assert!(admit_process_replay_source("activity", "events", ordered, true).is_err());
        let mut splittable = ordered;
        splittable.topology = SourceTopology::Splittable;
        assert!(admit_process_replay_source("activity", "events", splittable, false).is_ok());
        assert!(admit_process_replay_source("activity", "events", splittable, true).is_ok());
        let mut coupled = ordered;
        coupled.consistency = SourceConsistency::CommitCoupled;
        assert!(admit_process_replay_source("activity", "events", coupled, false).is_ok());
    }

    #[tokio::test]
    async fn rejected_order_leaves_registration_available_and_source_unstarted() {
        let directory = tempfile::tempdir().unwrap();
        let probe = Probe::new(
            replayable_contract()
                .with_row_positions(SourceRowPositionCapability::OrderedDeterministic),
        );
        let db = database(
            directory.path(),
            &probe,
            DeliveryGuarantee::AtLeastOnce,
            1024,
        )
        .await;
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
        *probe.contract.lock() = cut_contract();
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
        let probe = Probe::new(cut_contract());
        let db = database(
            directory.path(),
            &probe,
            DeliveryGuarantee::AtLeastOnce,
            1024,
        )
        .await;
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
        let db = database(
            directory.path(),
            &probe,
            DeliveryGuarantee::BestEffort,
            1024,
        )
        .await;
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
        let probe = Probe::new(cut_contract());
        let first = database(
            directory.path(),
            &probe,
            DeliveryGuarantee::AtLeastOnce,
            1024,
        )
        .await;
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

        let restored = database(
            directory.path(),
            &probe,
            DeliveryGuarantee::AtLeastOnce,
            1024,
        )
        .await;
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

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct CallbackStamp {
        id: u64,
        key: String,
        timestamp: i64,
        timer: bool,
        state: ValueState,
    }

    struct TimedActivity(Arc<Probe>);

    impl NativeProcessFunction for TimedActivity {
        fn invoke(
            &self,
            activations: &[ProcessActivation],
        ) -> Result<Vec<ProcessActivationResult>, DbError> {
            let mut observed = self.0.callbacks.lock();
            assert!(observed.len() + activations.len() <= 32);
            for activation in activations {
                observed.push(CallbackStamp {
                    id: activation.id,
                    key: activation.key_text.clone(),
                    timestamp: activation.event_time_us,
                    timer: matches!(activation.callback, ProcessCallback::Timer { .. }),
                    state: activation.state,
                });
                if let ProcessCallback::Input(batch) = &activation.callback {
                    let amount = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0);
                    if amount == 50 && self.0.fail_second.swap(false, Ordering::AcqRel) {
                        return Err(DbError::Pipeline("ordered timer probe failure".into()));
                    }
                }
            }
            drop(observed);
            crate::process_function::tests::AccountActivity.invoke(activations)
        }
    }

    fn timer_probe() -> Arc<Probe> {
        let mut contract = cut_contract();
        contract.topology = SourceTopology::Splittable;
        let mut probe = Probe::new(contract);
        Arc::get_mut(&mut probe).unwrap().batches = vec![
            input_batch(&[("a", 60, 100_000)]),
            input_batch(&[("b", 7, 105_000)]),
            input_batch(&[("a", 50, 108_000)]),
            input_batch(&[("c", 1, 120_000)]),
            input_batch(&[("c", 2, 140_000)]),
            input_batch(&[("d", 0, 160_000)]),
        ];
        probe
    }

    async fn register_timed(
        db: &LaminarDB,
        binding: crate::process_function::ProcessFunctionDescriptor,
        handler: &crate::process_function::ProcessHandler,
    ) {
        match handler {
            crate::process_function::ProcessHandler::Native(handler) => {
                db.register_native_process_function(
                    "activity",
                    "events",
                    binding,
                    Arc::clone(handler),
                )
                .await
                .unwrap();
            }
            #[cfg(feature = "process-remote")]
            crate::process_function::ProcessHandler::Remote(client) => {
                db.register_remote_process_function(
                    "activity",
                    "events",
                    binding,
                    Arc::clone(client),
                )
                .await
                .unwrap();
            }
        }
    }

    type ActivityRow = (String, String, i64, bool, i64);

    async fn activity_rows(portal: &mut SubscriptionPortal, expected: usize) -> Vec<ActivityRow> {
        use arrow::array::{BooleanArray, StringArray, TimestampMicrosecondArray};

        tokio::time::timeout(Duration::from_secs(5), async {
            let mut rows = Vec::new();
            while rows.len() < expected {
                let PortalFrame::Batch { batch, .. } = portal.next_frame().await.unwrap() else {
                    continue;
                };
                let key = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let kind = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let total = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let crossed = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                let timestamp = batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                rows.extend((0..batch.num_rows()).map(|row| {
                    (
                        key.value(row).into(),
                        kind.value(row).into(),
                        total.value(row),
                        crossed.value(row),
                        timestamp.value(row),
                    )
                }));
                assert!(rows.len() <= expected, "duplicate process output");
            }
            rows
        })
        .await
        .unwrap()
    }

    async fn checkpointed_timer_database(
        path: &std::path::Path,
        probe: &Arc<Probe>,
        binding: crate::process_function::ProcessFunctionDescriptor,
        handler: &crate::process_function::ProcessHandler,
        buffer: usize,
    ) -> (Arc<LaminarDB>, SubscriptionPortal) {
        probe.available.store(1, Ordering::Release);
        let db = database(path, probe, DeliveryGuarantee::AtLeastOnce, buffer).await;
        register_timed(&db, binding, handler).await;
        let mut portal = db
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        db.start().await.unwrap();
        let prefix = activity_rows(&mut portal, 1).await;
        assert_eq!(
            prefix[0],
            ("a".into(), "running".into(), 60, false, 100_000)
        );
        assert!(db.checkpoint().await.unwrap().success);
        probe.callbacks.lock().clear();
        (db, portal)
    }

    async fn qualify_timer_cut_recovery(
        probe: &Arc<Probe>,
        binding: crate::process_function::ProcessFunctionDescriptor,
        handler: crate::process_function::ProcessHandler,
    ) {
        let reference_dir = tempfile::tempdir().unwrap();
        let (reference, mut portal) = checkpointed_timer_database(
            reference_dir.path(),
            probe,
            binding.clone(),
            &handler,
            1024,
        )
        .await;
        probe.available.store(6, Ordering::Release);
        let mut expected_rows = activity_rows(&mut portal, 8).await;
        // WHY: remote output order is guaranteed within a key, not across independent keys.
        expected_rows.sort_by(|left, right| left.0.cmp(&right.0));
        reference.shutdown().await.unwrap();
        // INVARIANT: IDs preserve engine order; independent-key RPCs may arrive concurrently.
        let mut expected = probe.callbacks.lock().clone();
        expected.sort_unstable_by_key(|callback| callback.id);
        assert!(expected.iter().any(|callback| callback.key == "a"
            && callback.timer
            && callback.timestamp == 118_000));
        assert!(!expected.iter().any(|callback| callback.key == "a"
            && callback.timer
            && callback.timestamp == 110_000));
        drop(portal);
        drop(reference);

        let directory = tempfile::tempdir().unwrap();
        let (first, mut portal) =
            checkpointed_timer_database(directory.path(), probe, binding.clone(), &handler, 1024)
                .await;
        first.source_untyped("events").unwrap().watermark(9_000_000);
        probe.fail_second.store(true, Ordering::Release);
        probe.available.store(6, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(5), async {
            while first.last_fault().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(first
            .shutdown()
            .await
            .unwrap_err()
            .to_string()
            .contains("ordered timer probe failure"));
        let mut failed = probe.callbacks.lock().clone();
        failed.sort_unstable_by_key(|callback| callback.id);
        assert_eq!(failed, expected[..2]);
        drop(portal);
        drop(first);
        probe.callbacks.lock().clear();

        let restored = database(directory.path(), probe, DeliveryGuarantee::AtLeastOnce, 1).await;
        register_timed(&restored, binding, &handler).await;
        portal = restored
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        restored.start().await.unwrap();
        let mut actual_rows = activity_rows(&mut portal, 8).await;
        actual_rows.sort_by(|left, right| left.0.cmp(&right.0));
        restored.shutdown().await.unwrap();
        let mut actual = probe.callbacks.lock().clone();
        actual.sort_unstable_by_key(|callback| callback.id);
        assert_eq!(actual, expected);
        assert_eq!(actual_rows, expected_rows);
        assert_eq!(probe.starts.lock().as_slice(), &[0, 0, 1]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn splittable_source_replays_matching_input_and_timer_cuts() {
        let probe = timer_probe();
        let handler = crate::process_function::ProcessHandler::Native(Arc::new(TimedActivity(
            Arc::clone(&probe),
        )));
        qualify_timer_cut_recovery(&probe, descriptor(), handler).await;
    }

    #[tokio::test]
    async fn independent_logical_source_is_rejected_before_process_startup() {
        let directory = tempfile::tempdir().unwrap();
        let probe = Probe::new(cut_contract());
        let db = database(
            directory.path(),
            &probe,
            DeliveryGuarantee::AtLeastOnce,
            1024,
        )
        .await;
        db.register_native_process_function(
            "activity",
            "events",
            binding(),
            Arc::new(Sum(Arc::clone(&probe))),
        )
        .await
        .unwrap();
        db.execute("CREATE SOURCE other (ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)").await.unwrap();
        assert!(db
            .start()
            .await
            .unwrap_err()
            .to_string()
            .contains("one logical source"));
        assert!(probe.starts.lock().is_empty());
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn fixed_replay_batch_remains_atomic_above_the_poll_target() {
        let directory = tempfile::tempdir().unwrap();
        let mut probe = Probe::new(cut_contract());
        Arc::get_mut(&mut probe).unwrap().batches =
            vec![input_batch(&[("a", 60, 100_000), ("a", 50, 100_000)])];
        let db = database(directory.path(), &probe, DeliveryGuarantee::AtLeastOnce, 1).await;
        db.register_native_process_function(
            "activity",
            "events",
            binding(),
            Arc::new(Sum(Arc::clone(&probe))),
        )
        .await
        .unwrap();
        let mut portal = db
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        db.start().await.unwrap();
        probe.available.store(1, Ordering::Release);
        let totals = tokio::time::timeout(Duration::from_secs(5), async {
            let mut totals = Vec::new();
            while totals.len() < 2 {
                if let PortalFrame::Batch { batch, .. } = portal.next_frame().await.unwrap() {
                    totals.extend_from_slice(
                        batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .values(),
                    );
                }
            }
            totals
        })
        .await
        .unwrap();
        assert_eq!(totals, [60, 110]);
        assert_eq!(probe.activations.lock().as_slice(), [0, 1]);
        assert!(db.checkpoint().await.unwrap().success);
        db.shutdown().await.unwrap();
    }

    #[cfg(feature = "process-remote")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_splittable_source_replays_matching_input_and_timer_cuts() {
        use crate::process_function::remote::{RemoteProcessClient, RustReferenceWorker};
        use crate::process_function::{ProcessHandler, ProcessRuntime};
        use futures::FutureExt as _;

        let probe = timer_probe();
        let mut binding = descriptor();
        binding.runtime = ProcessRuntime::RemoteRust;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let worker = RustReferenceWorker::new(
            binding.clone(),
            Arc::new(TimedActivity(Arc::clone(&probe))),
            2,
        )
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
        let outcome = std::panic::AssertUnwindSafe(qualify_timer_cut_recovery(
            &probe,
            binding,
            ProcessHandler::Remote(client),
        ))
        .catch_unwind()
        .await;
        shutdown.cancel();
        let cleanup = tokio::time::timeout(Duration::from_secs(5), task).await;
        if let Err(primary) = outcome {
            if !matches!(cleanup, Ok(Ok(Ok(())))) {
                eprintln!("timer replay worker cleanup also failed");
            }
            std::panic::resume_unwind(primary);
        }
        cleanup.unwrap().unwrap().unwrap();
    }

    #[cfg(feature = "process-remote")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_registration_enforces_the_same_source_order_contract() {
        use crate::process_function::remote::{RemoteProcessClient, RustReferenceWorker};
        use crate::process_function::ProcessRuntime;

        let directory = tempfile::tempdir().unwrap();
        let probe = Probe::new(replayable_contract());
        let db = database(
            directory.path(),
            &probe,
            DeliveryGuarantee::AtLeastOnce,
            1024,
        )
        .await;
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
        *probe.contract.lock() = cut_contract();
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
