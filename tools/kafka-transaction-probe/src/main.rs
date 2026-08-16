//! Deterministic, single-broker Kafka transaction protocol probe.
//!
//! This executable is deliberately excluded from the LaminarDB workspace. It is a narrow
//! qualification aid, not runtime code and not certification evidence.

mod endtxn_proxy;

use std::array;
use std::collections::BTreeSet;
use std::env;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_executor::block_on;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{DeliveryResult, Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer, ProducerContext};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use sha2::{Digest, Sha256};

use endtxn_proxy::{EndTxnEvidence, EndTxnProxy, FaultOutcome, TargetSpec};

const PARTITIONS: [i32; 3] = [0, 1, 2];
const HEADER_NAME: &str = "__ldb";
const TRACE_HEADER_NAME: &str = "trace-id";
const TRACE_HEADER_VALUE: &[u8] = b"cycle55-preserved";
const DETERMINISTIC_PRODUCER_CLIENT_ID: &str = "ldb-kafka-transaction-probe-producer";
const IO_TIMEOUT: Duration = Duration::from_secs(30);
const CAPTURE_TIMEOUT: Duration = Duration::from_secs(30);
const AMBIGUOUS_COMMIT_TIMEOUT: Duration = Duration::from_millis(750);
const TX_ID_DOMAIN: &[u8] = b"laminardb/kafka/transactional-id/v1\0";
const TX_ID_PREFIX: &str = "ldb.tx.v1.";

const DEPLOYMENT: [u8; 16] = [0x22; 16];
const INCARNATION: [u8; 16] = [0x33; 16];
const FIRST_INTERVAL: [u8; 16] = [0x11; 16];
const SUCCESSOR_INTERVAL: [u8; 16] = [0x12; 16];
#[cfg(test)]
const TEST_INTERVAL: [u8; 16] = [0xb2; 16];

const STABLE_KEY: &[u8] = b"stable-key";
const STABLE_PAYLOAD: &[u8] = b"stable-payload";
const SELECTION_KEY: &[u8] = b"selection-key";
const SELECTION_PAYLOAD: &[u8] = b"selection-payload";
const ABORT_KEYS: [&[u8]; 3] = [b"abort-key-0", b"abort-key-1", b"abort-key-2"];
const ABORT_PAYLOADS: [&[u8]; 3] = [b"abort-payload-0", b"abort-payload-1", b"abort-payload-2"];
const FENCED_KEYS: [&[u8]; 3] = [b"fenced-key-0", b"fenced-key-1", b"fenced-key-2"];
const FENCED_PAYLOADS: [&[u8]; 3] = [
    b"fenced-payload-0",
    b"fenced-payload-1",
    b"fenced-payload-2",
];

// Copied from the dependency-free frozen vectors in independent-soak-contract/wire_v1.rs.
// The probe does not call LaminarDB's encoder.
#[cfg(test)]
const DATA_GOLDEN_HEX: &str = concat!(
    "4c44424f010100000038a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1b2b2b2b2b2b2",
    "b2b2b2b2b2b2b2b2b2b20102030405060708",
);
const FIRST_MARKER_GOLDEN_HEX: &str = concat!(
    "4c44424f01020000013b1111111111111111111111111111111100000000000000000000000000000000222222222222",
    "222222222222222222223333333333333333333333333333333300054444444444444444444444444444444444444444",
    "444444444444444444444444000100010004010203040506070855555555555555555555555555555555555555555555",
    "555555555555555555551112131415161718666666666666666666666666666666662122232425262728313233343536",
    "373831323334353637387777777777777777777777777777777777777777777777777777777777777777515253545556",
    "575888888888888888888888888888888888888888888888888888888888888888889999999999999999999999999999",
    "9999999999999999999999999999999999990473696e6b026f70036f75740573686172640f",
);
const SUCCESSOR_MARKER_GOLDEN_HEX: &str = concat!(
    "4c44424f01020001013b1212121212121212121212121212121211111111111111111111111111111111222222222222",
    "222222222222222222223333333333333333333333333333333300054444444444444444444444444444444444444444",
    "444444444444444444444444000100010004010203040506070855555555555555555555555555555555555555555555",
    "555555555555555555551112131415161718666666666666666666666666666666662122232425262728313233343536",
    "373831323334353637387777777777777777777777777777777777777777777777777777777777777777515253545556",
    "575888888888888888888888888888888888888888888888888888888888888888889999999999999999999999999999",
    "9999999999999999999999999999999999990473696e6b026f70036f75740573686172640f",
);

type ProbeResult<T> = Result<T, String>;

#[derive(Debug, Eq, PartialEq)]
struct Cli {
    brokers: String,
    run_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AmbiguityKind {
    Marker,
    Data,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AmbiguityOutcome {
    Applied,
    Unapplied,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AmbiguityScenario {
    kind: AmbiguityKind,
    outcome: AmbiguityOutcome,
}

#[derive(Debug, Eq, PartialEq)]
struct AmbiguityCli {
    brokers: String,
    proxy_listen: SocketAddr,
    proxy_upstream: SocketAddr,
    run_id: String,
    scenario: AmbiguityScenario,
}

enum Command {
    Run(Cli),
    RunAmbiguity(AmbiguityCli),
    Help,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Record {
    key: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
    header: Vec<u8>,
    other_headers: Vec<(String, Option<Vec<u8>>)>,
}

#[derive(Default)]
struct ProbeProducerContext {
    delivered: AtomicUsize,
    failed: AtomicUsize,
    partition_mask: AtomicUsize,
    invalid_partition: AtomicUsize,
    invalid_offset: AtomicUsize,
}

impl ClientContext for ProbeProducerContext {
    fn error(&self, error: KafkaError, reason: &str) {
        eprintln!("librdkafka-global-error error={error} reason={reason}");
    }
}

impl ProducerContext for ProbeProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {
        match result {
            Ok(message) => {
                self.delivered.fetch_add(1, Ordering::SeqCst);
                match partition_index(message.partition()) {
                    Ok(index) => {
                        self.partition_mask.fetch_or(1 << index, Ordering::SeqCst);
                    }
                    Err(_) => {
                        self.invalid_partition.fetch_add(1, Ordering::SeqCst);
                    }
                }
                if message.offset() < 0 {
                    self.invalid_offset.fetch_add(1, Ordering::SeqCst);
                }
            }
            Err((_error, _message)) => {
                self.failed.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

#[derive(Clone, Copy)]
struct DataFanout<'a> {
    operation_tag: u8,
    interval: &'a [u8; 16],
    sequence_base: u64,
    keys: &'a [&'a [u8]; 3],
    payloads: &'a [&'a [u8]; 3],
}

fn main() {
    println!("NOT CERTIFICATION EVIDENCE");
    println!(
        "scope=validation-only single-broker protocol probes; excludes runtime wiring, replication, failover, durability, latency, soak, and end-to-end exactly-once"
    );

    let command = match parse_cli(env::args().skip(1)) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("ERROR: {error}");
            print_usage();
            process::exit(2);
        }
    };
    match command {
        Command::Help => print_usage(),
        Command::Run(cli) => match run_probe(&cli) {
            Ok(()) => println!("PASS deterministic-single-broker-transaction-probe"),
            Err(error) => {
                eprintln!("FAIL: {error}");
                process::exit(1);
            }
        },
        Command::RunAmbiguity(cli) => match run_ambiguity_probe(&cli) {
            Ok(()) => println!("PASS matched-endtxn-ambiguity-probe"),
            Err(error) => {
                eprintln!("FAIL: {error}");
                process::exit(1);
            }
        },
    }
}

fn print_usage() {
    println!(
        "usage:\n  kafka-transaction-probe --brokers <host:port[,host:port...]> [--run-id <safe-label>]\n  kafka-transaction-probe --brokers <loopback-host:proxy-port> --run-id <safe-label> --ambiguity <marker-applied|marker-unapplied|data-applied|data-unapplied> --proxy-upstream <loopback-host:broker-port>"
    );
}

fn parse_cli(arguments: impl IntoIterator<Item = String>) -> ProbeResult<Command> {
    let arguments = arguments.into_iter().collect::<Vec<_>>();
    if arguments.as_slice() == ["--help"] || arguments.as_slice() == ["-h"] {
        return Ok(Command::Help);
    }

    let mut brokers = None;
    let mut run_id = None;
    let mut ambiguity = None;
    let mut proxy_upstream = None;
    let mut index = 0;
    while index < arguments.len() {
        let flag = &arguments[index];
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag.as_str() {
            "--brokers" if brokers.is_none() => brokers = Some(validate_brokers(value)?),
            "--run-id" if run_id.is_none() => run_id = Some(validate_run_id(value)?),
            "--ambiguity" if ambiguity.is_none() => {
                ambiguity = Some(parse_ambiguity_scenario(value)?)
            }
            "--proxy-upstream" if proxy_upstream.is_none() => {
                proxy_upstream = Some(validate_loopback_socket("--proxy-upstream", value)?)
            }
            "--brokers" | "--run-id" | "--ambiguity" | "--proxy-upstream" => {
                return Err(format!("duplicate option {flag}"));
            }
            _ => return Err(format!("unknown option {flag}")),
        }
        index += 2;
    }

    let brokers = brokers.ok_or_else(|| "--brokers is required".to_owned())?;
    match (ambiguity, proxy_upstream) {
        (None, None) => Ok(Command::Run(Cli { brokers, run_id })),
        (Some(scenario), Some(proxy_upstream)) => {
            let run_id = run_id.ok_or_else(|| "ambiguity mode requires --run-id".to_owned())?;
            let proxy_listen = validate_loopback_socket("--brokers", &brokers)?;
            if proxy_listen == proxy_upstream {
                return Err("proxy listen and upstream endpoints must differ".to_owned());
            }
            Ok(Command::RunAmbiguity(AmbiguityCli {
                brokers,
                proxy_listen,
                proxy_upstream,
                run_id,
                scenario,
            }))
        }
        (Some(_), None) => Err("ambiguity mode requires --proxy-upstream".to_owned()),
        (None, Some(_)) => Err("--proxy-upstream requires --ambiguity".to_owned()),
    }
}

fn parse_ambiguity_scenario(value: &str) -> ProbeResult<AmbiguityScenario> {
    match value {
        "marker-applied" => Ok(AmbiguityScenario {
            kind: AmbiguityKind::Marker,
            outcome: AmbiguityOutcome::Applied,
        }),
        "marker-unapplied" => Ok(AmbiguityScenario {
            kind: AmbiguityKind::Marker,
            outcome: AmbiguityOutcome::Unapplied,
        }),
        "data-applied" => Ok(AmbiguityScenario {
            kind: AmbiguityKind::Data,
            outcome: AmbiguityOutcome::Applied,
        }),
        "data-unapplied" => Ok(AmbiguityScenario {
            kind: AmbiguityKind::Data,
            outcome: AmbiguityOutcome::Unapplied,
        }),
        _ => Err(
            "--ambiguity must be marker-applied, marker-unapplied, data-applied, or data-unapplied"
                .to_owned(),
        ),
    }
}

fn validate_loopback_socket(field: &str, value: &str) -> ProbeResult<SocketAddr> {
    let address = value
        .parse::<SocketAddr>()
        .map_err(|error| format!("{field} must be one numeric socket address: {error}"))?;
    if !address.ip().is_loopback() || address.port() == 0 {
        return Err(format!("{field} must be a nonzero loopback socket address"));
    }
    Ok(address)
}

fn validate_brokers(value: &str) -> ProbeResult<String> {
    if value.is_empty() || value.len() > 1_024 {
        return Err("--brokers must contain 1..=1024 bytes".to_owned());
    }
    if value.trim() != value
        || value
            .bytes()
            .any(|byte| byte == 0 || byte.is_ascii_control())
    {
        return Err("--brokers contains whitespace at an edge, NUL, or a control byte".to_owned());
    }
    Ok(value.to_owned())
}

fn validate_run_id(value: &str) -> ProbeResult<String> {
    if value.is_empty() || value.len() > 48 {
        return Err("--run-id must contain 1..=48 bytes".to_owned());
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err("--run-id accepts only ASCII letters, digits, '.', '_', and '-'".to_owned());
    }
    Ok(value.to_owned())
}

fn run_probe(cli: &Cli) -> ProbeResult<()> {
    print_client_version();
    let first_marker = decode_and_validate_marker(FIRST_MARKER_GOLDEN_HEX, false)?;
    let successor_marker = decode_and_validate_marker(SUCCESSOR_MARKER_GOLDEN_HEX, true)?;
    let tx_id = derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink", "shard")?;
    let topic = unique_topic(cli.run_id.as_deref())?;

    println!(
        "brokers={} topic={topic} transactional_id={tx_id}",
        cli.brokers
    );
    let admin = create_admin(&cli.brokers)?;
    create_topic(&admin, &topic)?;
    require_topic_inventory(&admin, &topic)?;
    println!("step=topic-inventory partitions=0,1,2 result=PASS");

    let old = create_producer(&cli.brokers, &tx_id, DETERMINISTIC_PRODUCER_CLIENT_ID)?;
    old.init_transactions(IO_TIMEOUT)
        .map_err(|error| format!("old producer init_transactions failed: {error}"))?;

    commit_marker(&old, &topic, &first_marker)?;
    println!("step=first-marker-commit result=PASS");

    commit_replay_data(&old, &topic, &FIRST_INTERVAL)?;
    println!("step=data-commit result=PASS");

    let aborted = DataFanout {
        operation_tag: 0xa2,
        interval: &FIRST_INTERVAL,
        sequence_base: 1,
        keys: &ABORT_KEYS,
        payloads: &ABORT_PAYLOADS,
    };
    abort_data(&old, &topic, aborted)?;
    commit_data_fanout(&old, &topic, aborted)?;
    println!("step=confirmed-abort-and-identical-retry result=PASS");

    let fenced = DataFanout {
        operation_tag: 0xa3,
        interval: &FIRST_INTERVAL,
        sequence_base: 4,
        keys: &FENCED_KEYS,
        payloads: &FENCED_PAYLOADS,
    };
    stage_data(&old, &topic, fenced)?;

    let successor = create_producer(&cli.brokers, &tx_id, DETERMINISTIC_PRODUCER_CLIENT_ID)?;
    successor
        .init_transactions(IO_TIMEOUT)
        .map_err(|error| format!("successor init_transactions failed: {error}"))?;
    let commit_error = old.commit_transaction(IO_TIMEOUT);
    let fatal_error = old.client().fatal_error();
    let (commit_code, client_fatal_code) = require_fatal_fence(commit_error, fatal_error)?;
    println!(
        "step=producer-fence commit_error_code={commit_code:?} client_fatal_code={client_fatal_code:?} fatal=true result=PASS"
    );

    commit_marker(&successor, &topic, &successor_marker)?;
    commit_replay_data(&successor, &topic, &SUCCESSOR_INTERVAL)?;
    println!("step=successor-marker-and-replay result=PASS");

    let expected_uncommitted = expected_records(&first_marker, &successor_marker, true)?;
    let expected_committed = expected_records(&first_marker, &successor_marker, false)?;
    let uncommitted = capture(&cli.brokers, &topic, "read_uncommitted")?;
    require_capture("read_uncommitted", &uncommitted, &expected_uncommitted)?;
    require_marker_fanout(&uncommitted, &first_marker, &successor_marker)?;
    println!("step=read-uncommitted records_per_partition=7,5,5 result=PASS");

    let committed_view = capture(&cli.brokers, &topic, "read_committed")?;
    require_capture("read_committed", &committed_view, &expected_committed)?;
    require_marker_fanout(&committed_view, &first_marker, &successor_marker)?;
    println!("step=read-committed records_per_partition=5,3,3 result=PASS");
    require_topic_inventory(&admin, &topic)?;
    println!("step=topic-inventory-post partitions=0,1,2 result=PASS");
    Ok(())
}

fn run_ambiguity_probe(cli: &AmbiguityCli) -> ProbeResult<()> {
    print_client_version();
    let (kind_label, outcome_label, proxy_outcome) = match cli.scenario {
        AmbiguityScenario {
            kind: AmbiguityKind::Marker,
            outcome: AmbiguityOutcome::Applied,
        } => ("marker", "applied", FaultOutcome::AppliedResponseLost),
        AmbiguityScenario {
            kind: AmbiguityKind::Marker,
            outcome: AmbiguityOutcome::Unapplied,
        } => ("marker", "unapplied", FaultOutcome::UnappliedRequestHeld),
        AmbiguityScenario {
            kind: AmbiguityKind::Data,
            outcome: AmbiguityOutcome::Applied,
        } => ("data", "applied", FaultOutcome::AppliedResponseLost),
        AmbiguityScenario {
            kind: AmbiguityKind::Data,
            outcome: AmbiguityOutcome::Unapplied,
        } => ("data", "unapplied", FaultOutcome::UnappliedRequestHeld),
    };
    let first_marker = decode_and_validate_marker(FIRST_MARKER_GOLDEN_HEX, false)?;
    let successor_marker = decode_and_validate_marker(SUCCESSOR_MARKER_GOLDEN_HEX, true)?;
    let shard_id = format!("ambiguity-{}", cli.run_id);
    let tx_id = derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink", &shard_id)?;
    let topic = unique_topic(Some(&cli.run_id))?;
    let old_client_id = format!("ldb-kafka-ambiguity-{kind_label}-{outcome_label}-a");
    let successor_client_id = format!("ldb-kafka-ambiguity-{kind_label}-{outcome_label}-b");

    println!(
        "scenario={kind_label}-{outcome_label} brokers={} proxy_upstream={} topic={topic} transactional_id={tx_id}",
        cli.brokers, cli.proxy_upstream
    );
    let proxy = EndTxnProxy::start(
        cli.proxy_listen,
        cli.proxy_upstream,
        TargetSpec {
            client_id: old_client_id.clone(),
            transactional_id: tx_id.clone(),
            outcome: proxy_outcome,
        },
    )?;
    let admin = create_admin(&cli.brokers)?;
    create_topic(&admin, &topic)?;
    require_topic_inventory(&admin, &topic)?;
    require_proxy_route(&admin, cli.proxy_listen)?;
    println!(
        "step=proxy-route advertised={} origin={} partitions=0,1,2 result=PASS",
        cli.proxy_listen, cli.proxy_upstream
    );

    let old = create_producer(&cli.brokers, &tx_id, &old_client_id)?;
    old.init_transactions(IO_TIMEOUT)
        .map_err(|error| format!("ambiguity producer A init failed: {error}"))?;
    commit_marker(&old, &topic, &first_marker)?;
    match cli.scenario.kind {
        AmbiguityKind::Marker => stage_marker(&old, &topic, &successor_marker)?,
        AmbiguityKind::Data => stage_replay_data(&old, &topic, &FIRST_INTERVAL)?,
    }
    proxy.arm()?;

    let (actuation, commit_join) = thread::scope(|scope| {
        let commit = scope.spawn(|| old.commit_transaction(AMBIGUOUS_COMMIT_TIMEOUT));
        let actuation = proxy.wait_for_actuation(IO_TIMEOUT);
        (actuation, commit.join())
    });
    actuation?;
    let commit_result = commit_join.map_err(|_| "ambiguity commit thread panicked".to_owned())?;
    require_ambiguity_timeout(commit_result)?;
    println!(
        "step=caller-ambiguity code=OperationTimedOut retriable=true fatal=false abortable=false result=PASS"
    );

    drop(old);
    proxy.wait_for_target_connections_closed(IO_TIMEOUT)?;
    let evidence = proxy.finish_target()?;
    require_endtxn_evidence(&evidence, proxy_outcome, &old_client_id, &tx_id)?;
    print_endtxn_evidence(&evidence);

    let successor = create_producer(&cli.brokers, &tx_id, &successor_client_id)?;
    successor
        .init_transactions(IO_TIMEOUT)
        .map_err(|error| format!("ambiguity producer B init failed: {error}"))?;
    println!("step=same-id-successor-after-a-close result=PASS");

    let intermediate_cut = freeze_high_watermarks(&admin, &topic)?;
    let intermediate_uncommitted =
        capture_at_cut(&cli.brokers, &topic, "read_uncommitted", &intermediate_cut)?;
    let intermediate_committed =
        capture_at_cut(&cli.brokers, &topic, "read_committed", &intermediate_cut)?;
    let expected_intermediate_uncommitted = expected_ambiguity_intermediate(
        cli.scenario.kind,
        cli.scenario.outcome,
        true,
        &first_marker,
        &successor_marker,
    )?;
    let expected_intermediate_committed = expected_ambiguity_intermediate(
        cli.scenario.kind,
        cli.scenario.outcome,
        false,
        &first_marker,
        &successor_marker,
    )?;
    require_capture(
        "ambiguity intermediate read_uncommitted",
        &intermediate_uncommitted,
        &expected_intermediate_uncommitted,
    )?;
    require_capture(
        "ambiguity intermediate read_committed",
        &intermediate_committed,
        &expected_intermediate_committed,
    )?;

    match cli.scenario.kind {
        AmbiguityKind::Marker => {
            let selected = reconcile_marker_candidate(&intermediate_committed, &successor_marker)?;
            let expected = if cli.scenario.outcome == AmbiguityOutcome::Applied {
                &SUCCESSOR_INTERVAL
            } else {
                &FIRST_INTERVAL
            };
            if selected != expected {
                return Err(format!(
                    "marker reconciliation selected {selected:02x?}, expected {expected:02x?}"
                ));
            }
            commit_selection_data(&successor, &topic, selected)?;
            println!(
                "step=marker-reconciliation selected_interval={} result=PASS",
                if selected == &SUCCESSOR_INTERVAL {
                    "candidate"
                } else {
                    "last-confirmed"
                }
            );
        }
        AmbiguityKind::Data => {
            let candidate_visible = reconcile_data_candidate(&intermediate_committed)?;
            let expected_visible = cli.scenario.outcome == AmbiguityOutcome::Applied;
            if candidate_visible != expected_visible {
                return Err(format!(
                    "data reconciliation visibility was {candidate_visible}, expected {expected_visible}"
                ));
            }
            commit_marker(&successor, &topic, &successor_marker)?;
            commit_replay_data(&successor, &topic, &SUCCESSOR_INTERVAL)?;
            println!(
                "step=data-reconciliation predecessor_visible={candidate_visible} successor_replay=true result=PASS"
            );
        }
    }

    let final_cut = freeze_high_watermarks(&admin, &topic)?;
    let final_uncommitted = capture_at_cut(&cli.brokers, &topic, "read_uncommitted", &final_cut)?;
    let final_committed = capture_at_cut(&cli.brokers, &topic, "read_committed", &final_cut)?;
    let expected_final_uncommitted = expected_ambiguity_final(
        cli.scenario.kind,
        cli.scenario.outcome,
        true,
        &first_marker,
        &successor_marker,
    )?;
    let expected_final_committed = expected_ambiguity_final(
        cli.scenario.kind,
        cli.scenario.outcome,
        false,
        &first_marker,
        &successor_marker,
    )?;
    require_capture(
        "ambiguity final read_uncommitted",
        &final_uncommitted,
        &expected_final_uncommitted,
    )?;
    require_capture(
        "ambiguity final read_committed",
        &final_committed,
        &expected_final_committed,
    )?;
    require_topic_inventory(&admin, &topic)?;
    println!(
        "step=frozen-cut intermediate={intermediate_cut:?} final={final_cut:?} ru_counts={:?} rc_counts={:?} result=PASS",
        final_uncommitted.each_ref().map(Vec::len),
        final_committed.each_ref().map(Vec::len)
    );

    drop(successor);
    drop(admin);
    proxy.shutdown()?;
    Ok(())
}

fn require_endtxn_evidence(
    evidence: &EndTxnEvidence,
    expected_outcome: FaultOutcome,
    expected_client_id: &str,
    expected_tx_id: &str,
) -> ProbeResult<()> {
    if evidence.classification != expected_outcome.classification()
        || evidence.api_version != 1
        || evidence.client_id != expected_client_id
        || evidence.transactional_id != expected_tx_id
        || !evidence.committed
        || evidence.request_frame.len() != 27 + expected_client_id.len() + expected_tx_id.len()
        || evidence.response_downstream_bytes != 0
    {
        return Err(format!(
            "matched EndTxn evidence identity drift: {evidence:?}"
        ));
    }
    match expected_outcome {
        FaultOutcome::AppliedResponseLost
            if evidence.request_upstream_bytes == evidence.request_frame.len()
                && evidence
                    .response_frame
                    .as_ref()
                    .is_some_and(|frame| frame.len() == 14)
                && evidence.response_throttle_ms.is_some()
                && evidence.response_error_code == Some(0)
                && evidence.response_sha256.is_some() =>
        {
            Ok(())
        }
        FaultOutcome::UnappliedRequestHeld
            if evidence.request_upstream_bytes == 0
                && evidence.response_frame.is_none()
                && evidence.response_throttle_ms.is_none()
                && evidence.response_error_code.is_none()
                && evidence.response_sha256.is_none() =>
        {
            Ok(())
        }
        _ => Err(format!(
            "matched EndTxn byte disposition did not prove {}: {evidence:?}",
            expected_outcome.classification()
        )),
    }
}

fn print_client_version() {
    let (librdkafka_number, librdkafka_version) = rdkafka::util::get_rdkafka_version();
    println!(
        "rdkafka_crate=0.39.0 librdkafka_version={librdkafka_version} librdkafka_number={librdkafka_number:#x}"
    );
}

fn print_endtxn_evidence(evidence: &EndTxnEvidence) {
    println!(
        "step=endtxn-actuation classification={} connection={} api_key=26 api_version={} correlation={} client_id={} transactional_id={} producer_id={} producer_epoch={} committed={} request_bytes={} request_upstream_bytes={} request_sha256={} response_bytes={} response_downstream_bytes={} response_throttle_ms={} response_error={} response_sha256={} result=PASS",
        evidence.classification,
        evidence.connection_id,
        evidence.api_version,
        evidence.correlation_id,
        evidence.client_id,
        evidence.transactional_id,
        evidence.producer_id,
        evidence.producer_epoch,
        evidence.committed,
        evidence.request_frame.len(),
        evidence.request_upstream_bytes,
        evidence.request_sha256,
        evidence.response_frame.as_ref().map_or(0, Vec::len),
        evidence.response_downstream_bytes,
        evidence
            .response_throttle_ms
            .map_or_else(|| "none".to_owned(), |value| value.to_string()),
        evidence
            .response_error_code
            .map_or_else(|| "none".to_owned(), |value| value.to_string()),
        evidence.response_sha256.as_deref().unwrap_or("none"),
    );
    println!("endtxn_request_hex={}", evidence.request_hex());
    println!(
        "endtxn_response_hex={}",
        evidence.response_hex().as_deref().unwrap_or("none")
    );
    println!("endtxn_events={}", evidence.events.join("|"));
}

fn create_admin(brokers: &str) -> ProbeResult<AdminClient<DefaultClientContext>> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("client.id", "ldb-kafka-transaction-probe-admin")
        .set("socket.timeout.ms", "10000")
        .set("request.timeout.ms", "10000")
        .create()
        .map_err(|error| format!("admin client creation failed: {error}"))
}

fn create_topic(admin: &AdminClient<DefaultClientContext>, topic: &str) -> ProbeResult<()> {
    let new_topic = NewTopic::new(topic, 3, TopicReplication::Fixed(1))
        .set("cleanup.policy", "delete")
        .set("compression.type", "uncompressed");
    let options = AdminOptions::new()
        .request_timeout(Some(IO_TIMEOUT))
        .operation_timeout(Some(IO_TIMEOUT));
    let results = block_on(admin.create_topics([&new_topic], &options))
        .map_err(|error| format!("create_topics request failed: {error}"))?;
    if results.len() != 1 {
        return Err(format!(
            "create_topics returned {} results, expected 1",
            results.len()
        ));
    }
    match results.into_iter().next() {
        Some(Ok(created)) if created == topic => Ok(()),
        Some(Ok(created)) => Err(format!(
            "broker reported unexpected created topic {created}"
        )),
        Some(Err((name, error))) => Err(format!("topic creation failed name={name} error={error}")),
        None => Err("create_topics returned no result".to_owned()),
    }
}

fn require_topic_inventory(
    admin: &AdminClient<DefaultClientContext>,
    topic: &str,
) -> ProbeResult<()> {
    let deadline = Instant::now() + IO_TIMEOUT;
    loop {
        match admin
            .inner()
            .fetch_metadata(Some(topic), Duration::from_secs(2))
        {
            Ok(metadata) => {
                let topics = metadata.topics();
                if topics.len() == 1 && topics[0].name() == topic && topics[0].error().is_none() {
                    let mut ids = topics[0]
                        .partitions()
                        .iter()
                        .map(|partition| partition.id())
                        .collect::<Vec<_>>();
                    ids.sort_unstable();
                    if ids == PARTITIONS
                        && topics[0].partitions().iter().all(|partition| {
                            partition.error().is_none()
                                && partition.leader() >= 0
                                && partition.replicas().len() == 1
                                && partition.isr() == partition.replicas()
                        })
                    {
                        return Ok(());
                    }
                }
            }
            Err(error) if Instant::now() >= deadline => {
                return Err(format!("topic metadata remained unavailable: {error}"));
            }
            Err(_) => {}
        }
        if Instant::now() >= deadline {
            return Err("topic metadata never exposed exact ready inventory [0, 1, 2]".to_owned());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn require_proxy_route(
    admin: &AdminClient<DefaultClientContext>,
    expected: SocketAddr,
) -> ProbeResult<()> {
    let metadata = admin
        .inner()
        .fetch_metadata(None, Duration::from_secs(2))
        .map_err(|error| format!("proxy-route metadata failed: {error}"))?;
    let brokers = metadata.brokers();
    if brokers.len() != 1
        || brokers[0].host() != expected.ip().to_string()
        || brokers[0].port() != i32::from(expected.port())
    {
        let observed = brokers
            .iter()
            .map(|broker| format!("{}:{}", broker.host(), broker.port()))
            .collect::<Vec<_>>();
        return Err(format!(
            "broker metadata could bypass proxy: expected [{expected}], observed {observed:?}"
        ));
    }
    Ok(())
}

fn freeze_high_watermarks(
    admin: &AdminClient<DefaultClientContext>,
    topic: &str,
) -> ProbeResult<[i64; 3]> {
    let mut high = [0_i64; 3];
    for (index, partition) in PARTITIONS.into_iter().enumerate() {
        let (low, partition_high) = admin
            .inner()
            .fetch_watermarks(topic, partition, IO_TIMEOUT)
            .map_err(|error| {
                format!("watermark fetch failed for partition {partition}: {error}")
            })?;
        if low != 0 || partition_high < 0 {
            return Err(format!(
                "unexpected frozen cut for partition {partition}: low={low} high={partition_high}"
            ));
        }
        high[index] = partition_high;
    }
    Ok(high)
}

fn create_producer(
    brokers: &str,
    transactional_id: &str,
    client_id: &str,
) -> ProbeResult<BaseProducer<ProbeProducerContext>> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("client.id", client_id)
        .set("transactional.id", transactional_id)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("compression.type", "none")
        .set("transaction.timeout.ms", "20000")
        .set("message.timeout.ms", "15000")
        .set("request.timeout.ms", "10000")
        .set("socket.timeout.ms", "10000")
        .set("max.in.flight.requests.per.connection", "1")
        .set("allow.auto.create.topics", "false")
        .set("queue.buffering.max.messages", "1000")
        .set("queue.buffering.max.kbytes", "1024")
        .set("queue.buffering.max.ms", "0")
        .set("batch.num.messages", "100")
        .create_with_context(ProbeProducerContext::default())
        .map_err(|error| format!("transactional producer creation failed: {error}"))
}

fn commit_marker(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    marker: &[u8],
) -> ProbeResult<()> {
    stage_marker(producer, topic, marker)?;
    producer
        .commit_transaction(IO_TIMEOUT)
        .map_err(|error| format!("commit marker transaction failed: {error}"))
}

fn stage_marker(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    marker: &[u8],
) -> ProbeResult<()> {
    producer
        .begin_transaction()
        .map_err(|error| format!("begin marker transaction failed: {error}"))?;
    for partition in PARTITIONS {
        send_record(producer, topic, partition, None, &[], marker, false)?;
    }
    require_deliveries(producer, "marker", &PARTITIONS)
}

fn commit_replay_data(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    interval: &[u8; 16],
) -> ProbeResult<()> {
    producer
        .begin_transaction()
        .map_err(|error| format!("begin data transaction failed: {error}"))?;
    let header = encode_data_header(&[0xa1; 32], interval, 0)?;
    send_record(
        producer,
        topic,
        PARTITIONS[0],
        Some(STABLE_KEY),
        STABLE_PAYLOAD,
        &header,
        true,
    )?;
    require_deliveries(producer, "data commit", &[PARTITIONS[0]])?;
    producer
        .commit_transaction(IO_TIMEOUT)
        .map_err(|error| format!("commit data transaction failed: {error}"))
}

fn stage_replay_data(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    interval: &[u8; 16],
) -> ProbeResult<()> {
    producer
        .begin_transaction()
        .map_err(|error| format!("begin staged data transaction failed: {error}"))?;
    let header = encode_data_header(&[0xa1; 32], interval, 0)?;
    send_record(
        producer,
        topic,
        PARTITIONS[0],
        Some(STABLE_KEY),
        STABLE_PAYLOAD,
        &header,
        true,
    )?;
    require_deliveries(producer, "staged data", &[PARTITIONS[0]])
}

fn commit_selection_data(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    selected_interval: &[u8; 16],
) -> ProbeResult<()> {
    producer
        .begin_transaction()
        .map_err(|error| format!("begin selection-data transaction failed: {error}"))?;
    let header = encode_data_header(&[0xb1; 32], selected_interval, 0)?;
    send_record(
        producer,
        topic,
        PARTITIONS[0],
        Some(SELECTION_KEY),
        SELECTION_PAYLOAD,
        &header,
        true,
    )?;
    require_deliveries(producer, "selection data", &[PARTITIONS[0]])?;
    producer
        .commit_transaction(IO_TIMEOUT)
        .map_err(|error| format!("commit selection-data transaction failed: {error}"))
}

fn commit_data_fanout(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    fanout: DataFanout<'_>,
) -> ProbeResult<()> {
    producer
        .begin_transaction()
        .map_err(|error| format!("begin retry transaction failed: {error}"))?;
    send_data_fanout(producer, topic, fanout)?;
    require_deliveries(producer, "data retry", &PARTITIONS)?;
    producer
        .commit_transaction(IO_TIMEOUT)
        .map_err(|error| format!("commit retry transaction failed: {error}"))
}

fn abort_data(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    fanout: DataFanout<'_>,
) -> ProbeResult<()> {
    producer
        .begin_transaction()
        .map_err(|error| format!("begin abort transaction failed: {error}"))?;
    send_data_fanout(producer, topic, fanout)?;
    require_deliveries(producer, "data abort", &PARTITIONS)?;
    producer
        .abort_transaction(IO_TIMEOUT)
        .map_err(|error| format!("confirmed abort failed: {error}"))
}

fn stage_data(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    fanout: DataFanout<'_>,
) -> ProbeResult<()> {
    producer
        .begin_transaction()
        .map_err(|error| format!("begin staged transaction failed: {error}"))?;
    send_data_fanout(producer, topic, fanout)?;
    require_deliveries(producer, "staged predecessor", &PARTITIONS)
}

fn send_data_fanout(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    fanout: DataFanout<'_>,
) -> ProbeResult<()> {
    for (index, partition) in PARTITIONS.into_iter().enumerate() {
        let operation_id = operation_id(fanout.operation_tag, index);
        let sequence = fanout
            .sequence_base
            .checked_add(index as u64)
            .ok_or_else(|| "admission sequence overflow".to_owned())?;
        let header = encode_data_header(&operation_id, fanout.interval, sequence)?;
        send_record(
            producer,
            topic,
            partition,
            Some(fanout.keys[index]),
            fanout.payloads[index],
            &header,
            true,
        )?;
    }
    Ok(())
}

fn send_record(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    partition: i32,
    key: Option<&[u8]>,
    payload: &[u8],
    header: &[u8],
    include_trace_header: bool,
) -> ProbeResult<()> {
    if !PARTITIONS.contains(&partition) {
        return Err(format!("refusing unexpected target partition {partition}"));
    }
    let mut headers = OwnedHeaders::new_with_capacity(usize::from(include_trace_header) + 1)
        .insert(Header {
            key: HEADER_NAME,
            value: Some(header),
        });
    if include_trace_header {
        headers = headers.insert(Header {
            key: TRACE_HEADER_NAME,
            value: Some(TRACE_HEADER_VALUE),
        });
    }
    let mut record = BaseRecord::<[u8], [u8]>::to(topic)
        .partition(partition)
        .payload(payload)
        .headers(headers);
    if let Some(key) = key {
        record = record.key(key);
    }
    producer
        .send(record)
        .map_err(|(error, _)| format!("enqueue failed for partition {partition}: {error}"))
}

fn require_deliveries(
    producer: &BaseProducer<ProbeProducerContext>,
    label: &str,
    expected_partitions: &[i32],
) -> ProbeResult<()> {
    producer
        .flush(IO_TIMEOUT)
        .map_err(|error| format!("{label} flush failed: {error}"))?;
    let context = producer.context();
    let delivered = context.delivered.swap(0, Ordering::SeqCst);
    let failed = context.failed.swap(0, Ordering::SeqCst);
    let partition_mask = context.partition_mask.swap(0, Ordering::SeqCst);
    let invalid_partition = context.invalid_partition.swap(0, Ordering::SeqCst);
    let invalid_offset = context.invalid_offset.swap(0, Ordering::SeqCst);
    if delivered + failed != expected_partitions.len() {
        return Err(format!(
            "{label} produced {} delivery reports, expected {}",
            delivered + failed,
            expected_partitions.len()
        ));
    }
    if failed != 0 || invalid_partition != 0 || invalid_offset != 0 {
        return Err(format!(
            "{label} delivery validation failed: delivered={delivered} failed={failed} invalid_partition={invalid_partition} invalid_offset={invalid_offset}"
        ));
    }
    let expected_mask = expected_partitions
        .iter()
        .try_fold(0_usize, |mask, partition| {
            partition_index(*partition).map(|index| mask | (1 << index))
        })?;
    if partition_mask != expected_mask {
        return Err(format!(
            "{label} delivered partition mask {partition_mask:#x}, expected {expected_mask:#x}"
        ));
    }
    Ok(())
}

fn require_fatal_fence(
    result: Result<(), KafkaError>,
    client_fatal: Option<(RDKafkaErrorCode, String)>,
) -> ProbeResult<(RDKafkaErrorCode, RDKafkaErrorCode)> {
    let commit_code = match result {
        Err(KafkaError::Transaction(error)) if error.is_fatal() && is_fence_code(error.code()) => {
            error.code()
        }
        Err(KafkaError::Transaction(error)) => return Err(format!(
            "old producer rejection was not a fatal fence: code={:?} fatal={} retriable={} abortable={} error={error}",
            error.code(),
            error.is_fatal(),
            error.is_retriable(),
            error.txn_requires_abort()
        )),
        Err(error) => return Err(format!(
            "old producer rejection was not a transaction fence: {error}"
        )),
        Ok(()) => return Err("old producer unexpectedly committed after successor initialization".to_owned()),
    };
    match client_fatal {
        Some((code, _reason)) if is_fence_code(code) => Ok((commit_code, code)),
        Some((code, reason)) => Err(format!(
            "old producer client fatal error was not fencing: code={code:?} reason={reason}"
        )),
        None => Err("old producer did not record a client-level fatal fence".to_owned()),
    }
}

fn is_fence_code(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::Fenced
            | RDKafkaErrorCode::ProducerFenced
            | RDKafkaErrorCode::InvalidProducerEpoch
    )
}

fn require_ambiguity_timeout(result: Result<(), KafkaError>) -> ProbeResult<()> {
    match result {
        Err(KafkaError::Transaction(error))
            if error.code() == RDKafkaErrorCode::OperationTimedOut
                && error.is_retriable()
                && !error.is_fatal()
                && !error.txn_requires_abort() =>
        {
            Ok(())
        }
        Err(KafkaError::Transaction(error)) => Err(format!(
            "ambiguous commit did not return the exact retriable local timeout: code={:?} fatal={} retriable={} abortable={} error={error}",
            error.code(),
            error.is_fatal(),
            error.is_retriable(),
            error.txn_requires_abort()
        )),
        Err(error) => Err(format!(
            "ambiguous commit did not return a transaction timeout: {error}"
        )),
        Ok(()) => Err("ambiguous commit unexpectedly returned success to producer A".to_owned()),
    }
}

fn capture(brokers: &str, topic: &str, isolation: &str) -> ProbeResult<[Vec<Record>; 3]> {
    capture_inner(brokers, topic, isolation, None)
}

fn capture_at_cut(
    brokers: &str,
    topic: &str,
    isolation: &str,
    frozen_high: &[i64; 3],
) -> ProbeResult<[Vec<Record>; 3]> {
    capture_inner(brokers, topic, isolation, Some(frozen_high))
}

fn capture_inner(
    brokers: &str,
    topic: &str,
    isolation: &str,
    frozen_high: Option<&[i64; 3]>,
) -> ProbeResult<[Vec<Record>; 3]> {
    let group_id = format!("ldb-tx-probe-{isolation}-{}", process::id());
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set(
            "client.id",
            format!("ldb-kafka-transaction-probe-{isolation}"),
        )
        .set("group.id", group_id)
        .set("isolation.level", isolation)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true")
        .set("allow.auto.create.topics", "false")
        .set("auto.offset.reset", "error")
        .set("fetch.wait.max.ms", "50")
        .set("fetch.error.backoff.ms", "100")
        .set("fetch.message.max.bytes", "1048576")
        .set("queued.max.messages.kbytes", "1024")
        .set("socket.timeout.ms", "10000")
        .create()
        .map_err(|error| format!("{isolation} consumer creation failed: {error}"))?;

    let mut assignment = TopicPartitionList::new();
    for partition in PARTITIONS {
        assignment
            .add_partition_offset(topic, partition, Offset::Beginning)
            .map_err(|error| format!("{isolation} assignment construction failed: {error}"))?;
    }
    consumer
        .assign(&assignment)
        .map_err(|error| format!("{isolation} direct assignment failed: {error}"))?;

    let deadline = Instant::now() + CAPTURE_TIMEOUT;
    let mut eof = BTreeSet::new();
    let mut records: [Vec<Record>; 3] = array::from_fn(|_| Vec::new());
    let mut last_offsets = [-1_i64; 3];
    while eof.len() != PARTITIONS.len() {
        if Instant::now() >= deadline {
            return Err(format!(
                "{isolation} capture timed out; EOF partitions={eof:?} counts={:?}",
                records.each_ref().map(Vec::len)
            ));
        }
        match consumer.poll(Duration::from_millis(100)) {
            None => {}
            Some(Err(KafkaError::PartitionEOF(partition))) => {
                partition_index(partition)?;
                eof.insert(partition);
            }
            Some(Err(error)) => return Err(format!("{isolation} consume failed: {error}")),
            Some(Ok(message)) => {
                if message.topic() != topic {
                    return Err(format!(
                        "{isolation} observed unexpected topic {}",
                        message.topic()
                    ));
                }
                let index = partition_index(message.partition())?;
                if message.offset() <= last_offsets[index] {
                    return Err(format!(
                        "{isolation} offsets did not increase in partition {}: previous={} current={}",
                        message.partition(),
                        last_offsets[index],
                        message.offset()
                    ));
                }
                last_offsets[index] = message.offset();
                records[index].push(detach_record(&message, isolation)?);
            }
        }
    }
    if let Some(expected) = frozen_high {
        let positions = consumer
            .position()
            .map_err(|error| format!("{isolation} position fetch failed: {error}"))?;
        for (index, partition) in PARTITIONS.into_iter().enumerate() {
            let observed = positions
                .find_partition(topic, partition)
                .ok_or_else(|| format!("{isolation} position omitted partition {partition}"))?
                .offset();
            if observed != Offset::Offset(expected[index]) {
                return Err(format!(
                    "{isolation} did not reach frozen cut in partition {partition}: expected={} observed={observed:?}",
                    expected[index]
                ));
            }
        }
    }
    Ok(records)
}

fn detach_record(message: &impl Message, isolation: &str) -> ProbeResult<Record> {
    let headers = message
        .headers()
        .ok_or_else(|| format!("{isolation} record had no headers"))?;
    let mut reserved = None;
    let mut other_headers = Vec::new();
    for header in headers.iter() {
        if header.key == HEADER_NAME {
            if reserved.is_some() {
                return Err(format!(
                    "{isolation} record had duplicate case-sensitive {HEADER_NAME} headers"
                ));
            }
            reserved = Some(
                header
                    .value
                    .ok_or_else(|| format!("{isolation} {HEADER_NAME} header had a null value"))?
                    .to_vec(),
            );
        } else {
            other_headers.push((header.key.to_owned(), header.value.map(<[u8]>::to_vec)));
        }
    }
    let header = reserved
        .ok_or_else(|| format!("{isolation} record had no case-sensitive {HEADER_NAME} header"))?;
    Ok(Record {
        key: message.key().map(<[u8]>::to_vec),
        payload: message.payload().map(<[u8]>::to_vec),
        header,
        other_headers,
    })
}

fn require_capture(
    isolation: &str,
    actual: &[Vec<Record>; 3],
    expected: &[Vec<Record>; 3],
) -> ProbeResult<()> {
    for index in 0..PARTITIONS.len() {
        if actual[index] != expected[index] {
            return Err(format!(
                "{isolation} partition {} mismatch\nexpected={:#?}\nactual={:#?}",
                PARTITIONS[index], expected[index], actual[index]
            ));
        }
    }
    Ok(())
}

fn require_marker_fanout(
    records: &[Vec<Record>; 3],
    first_marker: &[u8],
    successor_marker: &[u8],
) -> ProbeResult<()> {
    for (expected, label) in [
        (first_marker, "first marker"),
        (successor_marker, "successor marker"),
    ] {
        for (index, partition_records) in records.iter().enumerate() {
            let matches = partition_records
                .iter()
                .filter(|record| record.header == expected)
                .collect::<Vec<_>>();
            if matches.len() != 1
                || matches[0].key.is_some()
                || matches[0].payload.as_deref() != Some(&[])
                || !matches[0].other_headers.is_empty()
            {
                return Err(format!(
                    "{label} fanout invalid in partition {}",
                    PARTITIONS[index]
                ));
            }
        }
    }
    Ok(())
}

fn expected_records(
    first_marker: &[u8],
    successor_marker: &[u8],
    include_aborted: bool,
) -> ProbeResult<[Vec<Record>; 3]> {
    let mut expected: [Vec<Record>; 3] = array::from_fn(|_| Vec::new());
    for (index, partition_records) in expected.iter_mut().enumerate() {
        partition_records.push(marker_record(first_marker));
        if index == 0 {
            partition_records.push(stable_replay_record(&FIRST_INTERVAL)?);
        }
        let retry = data_record(
            &ABORT_KEYS,
            &ABORT_PAYLOADS,
            0xa2,
            &FIRST_INTERVAL,
            1,
            index,
        )?;
        if include_aborted {
            partition_records.push(retry.clone());
        }
        partition_records.push(retry);
        if include_aborted {
            partition_records.push(data_record(
                &FENCED_KEYS,
                &FENCED_PAYLOADS,
                0xa3,
                &FIRST_INTERVAL,
                4,
                index,
            )?);
        }
        partition_records.push(marker_record(successor_marker));
        if index == 0 {
            partition_records.push(stable_replay_record(&SUCCESSOR_INTERVAL)?);
        }
    }
    Ok(expected)
}

fn expected_ambiguity_intermediate(
    kind: AmbiguityKind,
    outcome: AmbiguityOutcome,
    include_aborted: bool,
    first_marker: &[u8],
    successor_marker: &[u8],
) -> ProbeResult<[Vec<Record>; 3]> {
    let candidate_visible = outcome == AmbiguityOutcome::Applied || include_aborted;
    let mut expected: [Vec<Record>; 3] = array::from_fn(|_| Vec::new());
    for (index, records) in expected.iter_mut().enumerate() {
        records.push(marker_record(first_marker));
        match kind {
            AmbiguityKind::Marker if candidate_visible => {
                records.push(marker_record(successor_marker));
            }
            AmbiguityKind::Data if candidate_visible && index == 0 => {
                records.push(stable_replay_record(&FIRST_INTERVAL)?);
            }
            AmbiguityKind::Marker | AmbiguityKind::Data => {}
        }
    }
    Ok(expected)
}

fn expected_ambiguity_final(
    kind: AmbiguityKind,
    outcome: AmbiguityOutcome,
    include_aborted: bool,
    first_marker: &[u8],
    successor_marker: &[u8],
) -> ProbeResult<[Vec<Record>; 3]> {
    let mut expected = expected_ambiguity_intermediate(
        kind,
        outcome,
        include_aborted,
        first_marker,
        successor_marker,
    )?;
    match kind {
        AmbiguityKind::Marker => {
            let selected = if outcome == AmbiguityOutcome::Applied {
                &SUCCESSOR_INTERVAL
            } else {
                &FIRST_INTERVAL
            };
            expected[0].push(selection_record(selected)?);
        }
        AmbiguityKind::Data => {
            for records in &mut expected {
                records.push(marker_record(successor_marker));
            }
            expected[0].push(stable_replay_record(&SUCCESSOR_INTERVAL)?);
        }
    }
    Ok(expected)
}

fn selection_record(selected_interval: &[u8; 16]) -> ProbeResult<Record> {
    Ok(Record {
        key: Some(SELECTION_KEY.to_vec()),
        payload: Some(SELECTION_PAYLOAD.to_vec()),
        header: encode_data_header(&[0xb1; 32], selected_interval, 0)?.to_vec(),
        other_headers: trace_header(),
    })
}

fn reconcile_marker_candidate(
    committed: &[Vec<Record>; 3],
    successor_marker: &[u8],
) -> ProbeResult<&'static [u8; 16]> {
    let counts = committed.each_ref().map(|records| {
        records
            .iter()
            .filter(|record| record.header == successor_marker)
            .count()
    });
    match counts {
        [1, 1, 1] => Ok(&SUCCESSOR_INTERVAL),
        [0, 0, 0] => Ok(&FIRST_INTERVAL),
        _ => Err(format!(
            "candidate marker reconciliation was partial, duplicate, or conflicting: {counts:?}"
        )),
    }
}

fn reconcile_data_candidate(committed: &[Vec<Record>; 3]) -> ProbeResult<bool> {
    let expected = stable_replay_record(&FIRST_INTERVAL)?;
    let counts = committed
        .each_ref()
        .map(|records| records.iter().filter(|record| **record == expected).count());
    match counts {
        [1, 0, 0] => Ok(true),
        [0, 0, 0] => Ok(false),
        _ => Err(format!(
            "ambiguous data reconciliation was partial, duplicate, or conflicting: {counts:?}"
        )),
    }
}

fn marker_record(marker: &[u8]) -> Record {
    Record {
        key: None,
        payload: Some(Vec::new()),
        header: marker.to_vec(),
        other_headers: Vec::new(),
    }
}

fn stable_replay_record(interval: &[u8; 16]) -> ProbeResult<Record> {
    Ok(Record {
        key: Some(STABLE_KEY.to_vec()),
        payload: Some(STABLE_PAYLOAD.to_vec()),
        header: encode_data_header(&[0xa1; 32], interval, 0)?.to_vec(),
        other_headers: trace_header(),
    })
}

fn data_record(
    keys: &[&[u8]; 3],
    payloads: &[&[u8]; 3],
    operation_tag: u8,
    interval: &[u8; 16],
    sequence_base: u64,
    index: usize,
) -> ProbeResult<Record> {
    let operation = operation_id(operation_tag, index);
    let sequence = sequence_base
        .checked_add(index as u64)
        .ok_or_else(|| "expected sequence overflow".to_owned())?;
    Ok(Record {
        key: Some(keys[index].to_vec()),
        payload: Some(payloads[index].to_vec()),
        header: encode_data_header(&operation, interval, sequence)?.to_vec(),
        other_headers: trace_header(),
    })
}

fn trace_header() -> Vec<(String, Option<Vec<u8>>)> {
    vec![(
        TRACE_HEADER_NAME.to_owned(),
        Some(TRACE_HEADER_VALUE.to_vec()),
    )]
}

fn partition_index(partition: i32) -> ProbeResult<usize> {
    PARTITIONS
        .iter()
        .position(|expected| *expected == partition)
        .ok_or_else(|| format!("observed unexpected partition {partition}"))
}

fn operation_id(tag: u8, partition_index: usize) -> [u8; 32] {
    let mut operation = [tag; 32];
    operation[31] = (partition_index as u8).saturating_add(1);
    operation
}

fn encode_data_header(
    operation_id: &[u8; 32],
    interval_id: &[u8; 16],
    sequence: u64,
) -> ProbeResult<[u8; 66]> {
    if operation_id.iter().all(|byte| *byte == 0) {
        return Err("operation ID must be nonzero".to_owned());
    }
    if interval_id.iter().all(|byte| *byte == 0) {
        return Err("writer interval ID must be nonzero".to_owned());
    }
    let mut encoded = [0_u8; 66];
    encoded[..4].copy_from_slice(b"LDBO");
    encoded[4] = 1;
    encoded[5] = 1;
    encoded[6..8].copy_from_slice(&0_u16.to_be_bytes());
    encoded[8..10].copy_from_slice(&56_u16.to_be_bytes());
    encoded[10..42].copy_from_slice(operation_id);
    encoded[42..58].copy_from_slice(interval_id);
    encoded[58..66].copy_from_slice(&sequence.to_be_bytes());
    Ok(encoded)
}

fn decode_and_validate_marker(hex: &str, successor: bool) -> ProbeResult<Vec<u8>> {
    let marker = decode_hex(hex)?;
    if marker.len() != 325
        || marker.get(..4) != Some(b"LDBO")
        || marker[4] != 1
        || marker[5] != 2
        || u16::from_be_bytes([marker[8], marker[9]]) != 315
    {
        return Err("frozen marker vector has an invalid envelope".to_owned());
    }
    let expected_flags = if successor { 1 } else { 0 };
    if u16::from_be_bytes([marker[6], marker[7]]) != expected_flags {
        return Err("frozen marker vector has invalid predecessor flags".to_owned());
    }
    if marker[10..26]
        != if successor {
            SUCCESSOR_INTERVAL
        } else {
            FIRST_INTERVAL
        }
    {
        return Err("frozen marker vector has an unexpected current interval".to_owned());
    }
    if successor && marker[26..42] != FIRST_INTERVAL {
        return Err("successor marker does not link to the first interval".to_owned());
    }
    if !successor && marker[26..42].iter().any(|byte| *byte != 0) {
        return Err("first marker unexpectedly has a predecessor".to_owned());
    }
    Ok(marker)
}

fn decode_hex(value: &str) -> ProbeResult<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err("hex vector has odd length".to_owned());
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = nibble(pair[0])?;
            let low = nibble(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn nibble(value: u8) -> ProbeResult<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err("hex vector contains a non-lowercase-hex byte".to_owned()),
    }
}

fn derive_transactional_id(
    deployment: &[u8; 16],
    incarnation: &[u8; 16],
    sink: &str,
    shard: &str,
) -> ProbeResult<String> {
    if deployment.iter().all(|byte| *byte == 0) {
        return Err("deployment UUID must be nonzero".to_owned());
    }
    if incarnation.iter().all(|byte| *byte == 0) {
        return Err("pipeline incarnation must be nonzero".to_owned());
    }
    validate_identity_text("sink", sink, 128)?;
    validate_identity_text("shard", shard, 64)?;

    let mut hasher = Sha256::new();
    hasher.update(TX_ID_DOMAIN);
    hasher.update(deployment);
    hasher.update(incarnation);
    update_u8_length_prefixed(&mut hasher, sink.as_bytes())?;
    update_u8_length_prefixed(&mut hasher, shard.as_bytes())?;
    let digest = hasher.finalize();
    let mut id = String::with_capacity(74);
    id.push_str(TX_ID_PREFIX);
    for byte in digest {
        write!(&mut id, "{byte:02x}").map_err(|_| "transactional ID formatting failed")?;
    }
    if id.len() != 74 {
        return Err(format!(
            "transactional ID length was {}, expected 74",
            id.len()
        ));
    }
    Ok(id)
}

fn validate_identity_text(field: &str, value: &str, maximum: usize) -> ProbeResult<()> {
    if value.is_empty() || value.len() > maximum || value.as_bytes().contains(&0) {
        return Err(format!(
            "{field} identity must contain 1..={maximum} UTF-8 bytes without NUL"
        ));
    }
    Ok(())
}

fn update_u8_length_prefixed(hasher: &mut Sha256, value: &[u8]) -> ProbeResult<()> {
    let length = u8::try_from(value.len()).map_err(|_| "identity field exceeds u8 length")?;
    hasher.update([length]);
    hasher.update(value);
    Ok(())
}

fn unique_topic(run_id: Option<&str>) -> ProbeResult<String> {
    let label = match run_id {
        Some(value) => validate_run_id(value)?,
        None => "auto".to_owned(),
    };
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock precedes Unix epoch: {error}"))?
        .as_nanos();
    let topic = format!("ldb-tx-probe-{label}-{nanos:x}-{:x}", process::id());
    if topic.len() > 249
        || !topic
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err("generated topic is not a safe Kafka topic name".to_owned());
    }
    Ok(topic)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_is_manual_and_fail_closed() {
        let valid = parse_cli(
            ["--brokers", "127.0.0.1:19092", "--run-id", "run_1"]
                .into_iter()
                .map(str::to_owned),
        )
        .unwrap();
        match valid {
            Command::Run(cli) => assert_eq!(
                cli,
                Cli {
                    brokers: "127.0.0.1:19092".to_owned(),
                    run_id: Some("run_1".to_owned())
                }
            ),
            Command::RunAmbiguity(_) => panic!("valid deterministic arguments returned ambiguity"),
            Command::Help => panic!("valid arguments returned help"),
        }
        for hostile in [
            vec![],
            vec!["--unknown", "x"],
            vec!["--brokers"],
            vec!["--brokers", "x", "--brokers", "y"],
            vec!["--brokers=x"],
            vec!["--brokers", " x"],
            vec!["--brokers", "x", "--run-id", "bad/id"],
        ] {
            assert!(parse_cli(hostile.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn ambiguity_cli_is_explicit_loopback_and_fail_closed() {
        let command = parse_cli(
            [
                "--brokers",
                "127.0.0.1:19192",
                "--run-id",
                "cycle56-a",
                "--ambiguity",
                "marker-applied",
                "--proxy-upstream",
                "127.0.0.1:19194",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();
        match command {
            Command::RunAmbiguity(cli) => {
                assert_eq!(cli.proxy_listen, "127.0.0.1:19192".parse().unwrap());
                assert_eq!(cli.proxy_upstream, "127.0.0.1:19194".parse().unwrap());
                assert_eq!(cli.run_id, "cycle56-a");
                assert_eq!(
                    cli.scenario,
                    AmbiguityScenario {
                        kind: AmbiguityKind::Marker,
                        outcome: AmbiguityOutcome::Applied,
                    }
                );
            }
            Command::Run(_) | Command::Help => panic!("valid ambiguity arguments misclassified"),
        }

        for hostile in [
            vec![
                "--brokers",
                "127.0.0.1:19192",
                "--run-id",
                "x",
                "--ambiguity",
                "marker-applied",
            ],
            vec![
                "--brokers",
                "127.0.0.1:19192",
                "--proxy-upstream",
                "127.0.0.1:19194",
            ],
            vec![
                "--brokers",
                "0.0.0.0:19192",
                "--run-id",
                "x",
                "--ambiguity",
                "data-unapplied",
                "--proxy-upstream",
                "127.0.0.1:19194",
            ],
            vec![
                "--brokers",
                "127.0.0.1:19192,127.0.0.1:19193",
                "--run-id",
                "x",
                "--ambiguity",
                "data-applied",
                "--proxy-upstream",
                "127.0.0.1:19194",
            ],
            vec![
                "--brokers",
                "127.0.0.1:19192",
                "--run-id",
                "x",
                "--ambiguity",
                "marker-applied",
                "--proxy-upstream",
                "127.0.0.1:19192",
            ],
            vec![
                "--brokers",
                "127.0.0.1:19192",
                "--run-id",
                "x",
                "--ambiguity",
                "unknown",
                "--proxy-upstream",
                "127.0.0.1:19194",
            ],
        ] {
            assert!(parse_cli(hostile.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn transactional_id_is_stable_bounded_and_axis_sensitive() {
        let base = derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink", "shard").unwrap();
        assert_eq!(
            base,
            "ldb.tx.v1.c49ace6d02eb21ec7a2dc4424d8c3b9680fc3cd828cd754fec079b800a37411a"
        );
        assert_eq!(base.len(), 74);
        assert!(base.starts_with(TX_ID_PREFIX));
        assert!(base[TX_ID_PREFIX.len()..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));

        let mut deployment = DEPLOYMENT;
        deployment[0] ^= 1;
        let mut incarnation = INCARNATION;
        incarnation[0] ^= 1;
        for changed in [
            derive_transactional_id(&deployment, &INCARNATION, "sink", "shard").unwrap(),
            derive_transactional_id(&DEPLOYMENT, &incarnation, "sink", "shard").unwrap(),
            derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink-2", "shard").unwrap(),
            derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink", "shard-2").unwrap(),
        ] {
            assert_ne!(changed, base);
        }
        assert_eq!(
            derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink", "shard").unwrap(),
            base
        );
        assert!(derive_transactional_id(&[0; 16], &INCARNATION, "sink", "shard").is_err());
        assert!(derive_transactional_id(&DEPLOYMENT, &[0; 16], "sink", "shard").is_err());
        assert!(derive_transactional_id(&DEPLOYMENT, &INCARNATION, "", "shard").is_err());
        assert!(derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink", "").is_err());
        assert!(derive_transactional_id(&DEPLOYMENT, &INCARNATION, "a\0b", "shard").is_err());
        assert!(derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink", "a\0b").is_err());
        assert!(
            derive_transactional_id(&DEPLOYMENT, &INCARNATION, "sink", &"h".repeat(65)).is_err()
        );
        assert_ne!(
            derive_transactional_id(&DEPLOYMENT, &INCARNATION, "é", "shard").unwrap(),
            derive_transactional_id(&DEPLOYMENT, &INCARNATION, "e\u{301}", "shard").unwrap()
        );
    }

    #[test]
    fn independently_encoded_data_matches_frozen_golden() {
        let encoded =
            encode_data_header(&[0xa1; 32], &TEST_INTERVAL, 0x0102_0304_0506_0708).unwrap();
        assert_eq!(encoded.as_slice(), decode_hex(DATA_GOLDEN_HEX).unwrap());
        assert!(encode_data_header(&[0; 32], &TEST_INTERVAL, 0).is_err());
        assert!(encode_data_header(&[1; 32], &[0; 16], 0).is_err());
    }

    #[test]
    fn frozen_markers_are_exact_and_linked() {
        let first = decode_and_validate_marker(FIRST_MARKER_GOLDEN_HEX, false).unwrap();
        let successor = decode_and_validate_marker(SUCCESSOR_MARKER_GOLDEN_HEX, true).unwrap();
        assert_eq!(first.len(), 325);
        assert_eq!(successor.len(), 325);
        assert_ne!(first, successor);
        assert_eq!(&first[10..26], &FIRST_INTERVAL);
        assert_eq!(&successor[10..26], &SUCCESSOR_INTERVAL);
        assert_eq!(&successor[26..42], &FIRST_INTERVAL);
    }

    #[test]
    fn expected_visibility_excludes_both_aborted_transactions() {
        let first = decode_and_validate_marker(FIRST_MARKER_GOLDEN_HEX, false).unwrap();
        let successor = decode_and_validate_marker(SUCCESSOR_MARKER_GOLDEN_HEX, true).unwrap();
        let uncommitted = expected_records(&first, &successor, true).unwrap();
        let committed = expected_records(&first, &successor, false).unwrap();
        assert_eq!(uncommitted.each_ref().map(Vec::len), [7, 5, 5]);
        assert_eq!(committed.each_ref().map(Vec::len), [5, 3, 3]);

        assert_eq!(uncommitted[0][0], committed[0][0]);
        assert_eq!(uncommitted[0][1], committed[0][1]);
        assert_eq!(uncommitted[0][2], uncommitted[0][3]);
        assert_eq!(uncommitted[0][3], committed[0][2]);
        assert_eq!(uncommitted[0][5], committed[0][3]);
        assert_eq!(uncommitted[0][6], committed[0][4]);
        assert_eq!(uncommitted[0][1].key, uncommitted[0][6].key);
        assert_eq!(uncommitted[0][1].payload, uncommitted[0][6].payload);
        assert_ne!(uncommitted[0][1].header, uncommitted[0][6].header);

        for index in 1..3 {
            assert_eq!(uncommitted[index][0], committed[index][0]);
            assert_eq!(uncommitted[index][1], uncommitted[index][2]);
            assert_eq!(uncommitted[index][2], committed[index][1]);
            assert_eq!(uncommitted[index][4], committed[index][2]);
        }
        for partition in &uncommitted {
            for record in partition {
                if record.key.is_none() {
                    assert!(record.other_headers.is_empty());
                } else {
                    assert_eq!(record.other_headers, trace_header());
                }
            }
        }
    }

    #[test]
    fn topic_and_identity_boundaries_reject_hostile_values() {
        assert!(validate_run_id(&"a".repeat(48)).is_ok());
        assert!(validate_run_id(&"a".repeat(49)).is_err());
        assert!(validate_run_id("contains space").is_err());
        assert!(derive_transactional_id(
            &DEPLOYMENT,
            &INCARNATION,
            &"s".repeat(128),
            &"h".repeat(64)
        )
        .is_ok());
        assert!(
            derive_transactional_id(&DEPLOYMENT, &INCARNATION, &"s".repeat(129), "shard").is_err()
        );
    }
}
