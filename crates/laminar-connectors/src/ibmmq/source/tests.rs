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
