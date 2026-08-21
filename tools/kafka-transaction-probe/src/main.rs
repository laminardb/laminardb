//! Deterministic, single-broker Kafka transaction protocol probe.
//!
//! This executable is deliberately excluded from the LaminarDB workspace. It is a narrow
//! qualification aid, not runtime code and not certification evidence.

mod ambiguity;
mod broker_io;
mod cli;
mod endtxn_proxy;

use ambiguity::run_ambiguity_probe;
use broker_io::{
    abort_data, commit_data_fanout, commit_marker, commit_replay_data, commit_selection_data,
    create_admin, create_producer, create_topic, freeze_high_watermarks, require_ambiguity_timeout,
    require_fatal_fence, require_proxy_route, require_topic_inventory, stage_data, stage_marker,
    stage_replay_data,
};
use cli::validate_run_id;
use cli::{
    parse_cli, print_usage, AmbiguityCli, AmbiguityKind, AmbiguityOutcome, AmbiguityScenario, Cli,
    Command,
};

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

fn print_client_version() {
    let (librdkafka_number, librdkafka_version) = rdkafka::util::get_rdkafka_version();
    println!(
        "rdkafka_crate=0.39.0 librdkafka_version={librdkafka_version} librdkafka_number={librdkafka_number:#x}"
    );
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
mod tests;
