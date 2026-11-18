use super::*;

fn running_config() -> Arc<IbmMqSourceConfig> {
    let mut config = ConnectorConfig::new("ibmmq");
    config.set("queue", "Q");
    config.set("max.uow.messages", "10");
    config.set("max.uow.bytes", "100");
    config.set("max.batch.bytes", "50");
    config.set("max.message.bytes", "50");
    Arc::new(IbmMqSourceConfig::from_config(&config).unwrap())
}

/// Ledger state of a running source without a worker: plan and limits are pure.
struct Ledger {
    config: Arc<IbmMqSourceConfig>,
    generation: u64,
    gotten: u64,
    committed: u64,
    uow_messages: usize,
    uow_bytes: usize,
    uow_full: bool,
}

impl Ledger {
    fn cursor(&self, gotten: u64) -> SourceCheckpoint {
        let mut cursor = SourceCheckpoint::new();
        cursor.set_offset(GENERATION, format!("{:016x}", self.generation));
        cursor.set_offset(GOTTEN, gotten.to_string());
        cursor
    }
}

fn plan(ledger: &Ledger, cursor: &SourceCheckpoint) -> Result<CommitPlan, ConnectorError> {
    plan_for(ledger.generation, ledger.gotten, ledger.committed, cursor)
}

fn limits(ledger: &Ledger, max_records: usize) -> Option<Limits> {
    limits_for(
        &ledger.config,
        ledger.uow_messages,
        ledger.uow_bytes,
        ledger.uow_full,
        max_records,
    )
}

#[test]
fn a_unit_of_work_commits_only_as_a_whole_for_its_own_generation() {
    let ledger = Ledger {
        config: running_config(),
        generation: 9,
        gotten: 12,
        committed: 4,
        uow_messages: 8,
        uow_bytes: 40,
        uow_full: false,
    };
    assert_eq!(
        plan(&ledger, &SourceCheckpoint::new()).unwrap(),
        CommitPlan::Nothing
    );
    assert_eq!(
        plan(&ledger, &ledger.cursor(12)).unwrap(),
        CommitPlan::Commit
    );
    assert_eq!(
        plan(&ledger, &ledger.cursor(4)).unwrap(),
        CommitPlan::Nothing
    );
    assert!(
        plan(&ledger, &ledger.cursor(8)).is_err(),
        "part of a unit of work cannot commit"
    );
    assert!(plan(&ledger, &ledger.cursor(13)).is_err());
    let mut stale = ledger.cursor(99);
    stale.set_offset(GENERATION, format!("{:016x}", 3));
    assert_eq!(
        plan(&ledger, &stale).unwrap(),
        CommitPlan::Nothing,
        "another generation"
    );
}

#[test]
fn intake_pauses_when_the_unit_of_work_is_full() {
    let mut ledger = Ledger {
        config: running_config(),
        generation: 1,
        gotten: 0,
        committed: 0,
        uow_messages: 0,
        uow_bytes: 0,
        uow_full: false,
    };
    let open = limits(&ledger, 1_000).unwrap();
    assert_eq!((open.messages, open.bytes, open.wait_ms), (10, 50, 100));
    ledger.uow_messages = 7;
    ledger.uow_bytes = 90;
    let tight = limits(&ledger, 1_000).unwrap();
    assert_eq!((tight.messages, tight.bytes), (3, 10));
    ledger.uow_messages = 10;
    assert!(limits(&ledger, 1_000).is_none());
    ledger.uow_messages = 0;
    ledger.uow_full = true;
    assert!(limits(&ledger, 1_000).is_none(), "MAXUMSGS reached");
}

fn json_queue_config() -> ConnectorConfig {
    let mut config = ConnectorConfig::new("ibmmq");
    config.set("queue", "Q");
    config.set("format", "json");
    config
}

#[tokio::test]
async fn a_sealed_start_records_only_that_the_queue_keeps_its_position() {
    let config = json_queue_config();
    let mut source = IbmMqSource::new(Arc::new(arrow_schema::Schema::empty()), None);
    assert!(source.supports_initialized_start());
    let cursor = source.resolve_initial_position(&config).await.unwrap();
    assert_eq!(cursor.get_metadata("connector"), Some("ibmmq"));
    source
        .validate_initial_position(&config, &cursor)
        .await
        .unwrap();
    assert_eq!(
        plan_for(1, 0, 0, &cursor).unwrap(),
        CommitPlan::Nothing,
        "a sealed start commits nothing"
    );

    let mut running = SourceCheckpoint::new();
    running.set_offset(GENERATION, "00000000000000ff");
    running.set_offset(GOTTEN, "4");
    assert!(source
        .validate_initial_position(&config, &running)
        .await
        .is_err());
    let start = SourceStart::new(
        config,
        SourcePosition::Initialized {
            checkpoint: running,
        },
        crate::connector::DeliveryGuarantee::AtLeastOnce,
    )
    .unwrap();
    let error = source.start(start).await.unwrap_err();
    assert!(
        matches!(error, ConnectorError::InvalidState { .. }),
        "a foreign sealed cursor is refused before the queue opens: {error}"
    );
}

#[test]
fn any_one_cluster_node_may_consume_a_queue_on_a_named_queue_manager() {
    let source = IbmMqSource::new(Arc::new(arrow_schema::Schema::empty()), None);
    let placed = |config: &ConnectorConfig| {
        source
            .contract(config)
            .unwrap()
            .supports_cluster_singleton_placement()
    };
    let mut named = json_queue_config();
    assert!(
        !placed(&named),
        "each node would accept whichever queue manager its channel reaches"
    );
    named.set("queue.manager", "QM1");
    assert!(placed(&named));
    #[cfg(feature = "fpml")]
    {
        named.set("format", "fpml");
        assert!(!placed(&named), "FpML is not qualified for cluster sources");
    }
}
