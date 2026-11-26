//! Three-node at-least-once soak for sinks that apply changes by key, each node writing the keys
//! it owns. A PostgreSQL CDC source in changelog mode reads a table that a writer inserts, updates
//! and deletes in, one node at a time. Four sinks apply its changes by key:
//! - a PostgreSQL replica keyed by `id`, through a projection, so every row comes from the node
//!   reading the source;
//! - a PostgreSQL table, a MongoDB collection and a Kafka upsert topic keyed by `bucket`, from a
//!   keyed `EMIT CHANGES` aggregate, so each bucket comes from the node that owns it.
//!
//! Under a sink that applies changes by key, a crash inside the copy fails closed by design, so the
//! seed is small and the rounds start once the copy has ended. Then the observed leader and each
//! follower are killed inside a checkpoint and rejoin, which moves vnodes back live
//! (`kill_rejoin`). One more round freezes the node streaming the slot with `SIGSTOP` until a survivor takes the slot over;
//! resumed, the frozen node must exit fenced while the survivors keep writing its keys.
//!
//! Once the writer stops, the replica must equal the table, and the bucket table, the collection,
//! the newest committed record of each key in the topic and every node's committed view read must
//! equal the aggregate over the table.
//!
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,postgres-cdc,postgres-sink,mongodb-cdc --test cluster_soak \
//!   keyed_sinks::three_node_postgres_keyed_sinks_kill9_soak -- --ignored --exact --nocapture
//! ```
//!
//! It reads the knobs of the PostgreSQL changelog leg (`LAMINAR_SOAK_POSTGRES_SEED_ROWS` defaults
//! to 2000 here), `LAMINAR_SOAK_MONGODB_URI` and `LAMINAR_SOAK_KAFKA_SOURCE_BROKERS`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mongodb::bson::Bson;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};

use super::bucket_view::{bucket_sql, converged_reads, view_sql, Buckets};
use super::kill_rejoin::{
    boot, cluster_nodes, kill_round, per_node, record_consumers, steady_phase, SoakSettings,
    CHECKPOINTS_COMPLETED,
};
use super::multi_writer_sinks::mongo::MongoDb;
use super::multi_writer_sinks::postgres::Postgres;
use super::multi_writer_sinks::{block_on, Destination};
use super::postgres_changelog_source::{
    create_and_seed, spawn_writer, table_buckets, workload, SOURCE,
};
use super::postgres_source::{paused_owner_round, Capture, CaptureCleanup};
use super::{
    env_u64, kafka_create_compacted_topic, scoped_soak_storage_url, soak_run_id, wait_for,
    wait_for_converged_durable_checkpoint, wait_until, DurableCheckpointStatus, JoinDelivery, Node,
    ResolvedExecutable, NODES,
};

/// How long the sinks may take to apply the last changes after the view converged.
const SINK_SETTLE: Duration = Duration::from_secs(120);
/// How long the boot may take to copy the seed.
const COPY_DEADLINE: Duration = Duration::from_secs(300);
/// Grows on the node streaming the slot, which starts after the copy.
const STREAMED_TRANSACTIONS: &str = "laminardb_postgres_cdc_transactions_total";
/// Partitions of the bucket topic, pinned so every node hashes a key to the same one.
const TOPIC_PARTITIONS: i32 = 4;

/// The tables, collection and topic the keyed sinks write.
struct Outputs {
    replica: String,
    buckets: String,
    brokers: String,
}

#[test]
#[ignore = "spawns 3 real laminardb processes with S3, a logical-replication PostgreSQL and MongoDB"]
fn three_node_postgres_keyed_sinks_kill9_soak() {
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let settings = SoakSettings::from_environment(JoinDelivery::AtLeastOnce);
    let postgres = Postgres::from_environment();
    let mongo = MongoDb::from_environment();
    let run_id = soak_run_id();
    let checkpoint_url =
        scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "checkpoints");
    let suffix = run_id.replace('-', "_");
    let capture = Capture {
        table: format!("keyed_{suffix}"),
        publication: format!("keyed_{suffix}"),
        slot: format!("keyed_{suffix}"),
    };
    let outputs = Outputs {
        replica: format!("replica_{suffix}"),
        buckets: format!("buckets_{suffix}"),
        brokers: std::env::var("LAMINAR_SOAK_KAFKA_SOURCE_BROKERS")
            .expect("the keyed sink soak requires LAMINAR_SOAK_KAFKA_SOURCE_BROKERS"),
    };
    let seed_rows = i64::try_from(env_u64("LAMINAR_SOAK_POSTGRES_SEED_ROWS", 2_000))
        .expect("LAMINAR_SOAK_POSTGRES_SEED_ROWS fits i64");
    let dropped_outputs = format!("{}, {}", outputs.replica, outputs.buckets);
    // Bound before the nodes, so it runs after they stop.
    let _cleanup = CaptureCleanup {
        postgres: &postgres,
        capture: &capture,
        output: Some(&dropped_outputs),
    };
    let _documents = DropCollection {
        mongo: &mongo,
        collection: &outputs.buckets,
    };
    create_and_seed(&postgres, &capture, seed_rows);
    create_outputs(&postgres, &mongo, &outputs);

    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir: PathBuf = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive soak log directory");
    eprintln!("soak: keyed sink node logs in {}", log_dir.display());
    let mut nodes = cluster_nodes(
        &executable,
        dir.path(),
        &log_dir,
        &settings,
        &checkpoint_url,
        &view_sql(SOURCE),
        &format!(
            "{}{}",
            workload(&capture, &postgres.source_properties()),
            keyed_sinks(&postgres, &mongo, &outputs)
        ),
    );
    let mut writer = spawn_writer(&postgres, &capture.table, seed_rows, settings.rps);
    let mut live = || writer.assert_running();
    let mut consumers = BTreeSet::new();
    let mut latest = boot(&mut nodes, &mut live, &settings, CHECKPOINTS_COMPLETED);
    latest = past_the_copy(&mut nodes, &mut live, &settings, latest);
    let mut killed_followers = BTreeSet::new();
    for round in 0..settings.kills {
        (_, latest) = kill_round(
            &mut nodes,
            &mut live,
            round,
            &mut killed_followers,
            &settings,
            CHECKPOINTS_COMPLETED,
            latest,
        );
        record_consumers(&nodes, &mut consumers);
    }
    latest = paused_owner_round(
        &mut nodes,
        &mut live,
        &settings,
        &postgres,
        &capture.slot,
        latest,
    );
    record_consumers(&nodes, &mut consumers);
    steady_phase(
        &mut nodes,
        &mut live,
        settings.steady,
        CHECKPOINTS_COMPLETED,
    );
    let commits = writer.stop();
    let expected = block_on(table_buckets(&postgres, &capture.table));
    let epoch = converged_reads(&mut nodes, &expected, &settings, latest);
    let mut last = None;
    let applied = wait_until(SINK_SETTLE, || {
        let observed = block_on(observe(&postgres, &mongo, &capture, &outputs));
        let applied = observed.replica_differences == (0, 0)
            && observed.buckets == expected
            && observed.documents == expected
            && observed.messages == expected;
        last = Some(observed);
        applied
    });
    assert!(
        applied,
        "the keyed sinks did not converge on the table: expected buckets {expected:?}, last \
         observed {last:?}"
    );
    assert!(
        consumers.len() > 1,
        "only node {consumers:?} ever read the source: it never moved"
    );
    // Every kill round and the paused-owner round remove a process, whose Kafka writer the leader
    // must fence before its successor writes.
    let removals = usize::try_from(settings.kills + 1).expect("the round count fits usize");
    let fences = fenced_rounds(&nodes);
    assert!(
        fences >= removals,
        "the leader fenced the departed Kafka writers {fences} times in {removals} removals"
    );
    eprintln!(
        "soak: keyed sinks across {NODES} nodes: {seed_rows} seeded rows, {commits} writer \
         rounds; the replica equals the table and the bucket table, collection, topic and every \
         node's view read its {} buckets at epoch {epoch}; consumers={consumers:?} kills={} \
         (followers {killed_followers:?})",
        expected.len(),
        settings.kills
    );
}

/// The first committed cut captured after the copy ended. The source streams the slot only after
/// the copy, and the third cut committed after a node streams was captured after the second
/// committed, which was after the stream began.
fn past_the_copy(
    nodes: &mut [Node],
    live: &mut dyn FnMut(),
    settings: &SoakSettings,
    mut latest: DurableCheckpointStatus,
) -> DurableCheckpointStatus {
    wait_for(
        "the copy to end and the slot to stream",
        COPY_DEADLINE,
        || {
            live();
            per_node(nodes, STREAMED_TRANSACTIONS).iter().sum::<f64>() > 0.0
        },
    );
    for _ in 0..3 {
        latest = wait_for_converged_durable_checkpoint(
            nodes,
            Instant::now() + settings.recovery_ceiling,
            "a committed cut past the copy",
            Some(latest),
        );
    }
    latest
}

/// The projection and aggregate the sinks read, and the four sinks.
fn keyed_sinks(postgres: &Postgres, mongo: &MongoDb, outputs: &Outputs) -> String {
    let Outputs {
        replica,
        buckets,
        brokers,
    } = outputs;
    format!(
        r#"
# A cluster sink may not read a source directly.
[[pipeline]]
name = "replicated"
sql = "SELECT id, amount, note FROM {SOURCE}"

[[pipeline]]
name = "bucket_changes"
sql = "{buckets_sql} EMIT CHANGES"

[[sink]]
name = "replica"
pipeline = "replicated"
connector = "postgres-sink"
[sink.properties]
{replica_properties}
[[sink]]
name = "bucket_rows"
pipeline = "bucket_changes"
connector = "postgres-sink"
[sink.properties]
{bucket_properties}
[[sink]]
name = "bucket_documents"
pipeline = "bucket_changes"
connector = "mongodb-sink"
[sink.properties]
{document_properties}
[[sink]]
name = "bucket_records"
pipeline = "bucket_changes"
connector = "kafka"
format = "json"
[sink.properties]
"bootstrap.servers" = "{brokers}"
topic = "{buckets}"
envelope = "upsert"
"key.column" = "bucket"
"key.format" = "text"
"partition.count" = "{TOPIC_PARTITIONS}"
"#,
        buckets_sql = bucket_sql(SOURCE),
        replica_properties = postgres.changelog_sink_properties(replica, "id"),
        bucket_properties = postgres.changelog_sink_properties(buckets, "bucket"),
        document_properties = mongo.upsert_sink_properties(buckets, "bucket"),
    )
}

/// Every node opens the sinks concurrently, so the test creates the tables, the collection and the
/// topic rather than racing three nodes' automatic creation.
fn create_outputs(postgres: &Postgres, mongo: &MongoDb, outputs: &Outputs) {
    let Outputs {
        replica,
        buckets,
        brokers,
    } = outputs;
    mongo.create(buckets);
    mongo.create_unique_index(buckets, "bucket");
    kafka_create_compacted_topic(brokers, buckets, TOPIC_PARTITIONS);
    block_on(async {
        postgres
            .client()
            .await
            .batch_execute(&format!(
                "CREATE TABLE {replica} (id bigint PRIMARY KEY, amount bigint NOT NULL, note text); \
                 CREATE TABLE {buckets} (bucket bigint PRIMARY KEY, n bigint NOT NULL, \
                 total bigint, peak bigint);"
            ))
            .await
            .expect("create the keyed sink tables");
    });
}

/// What the keyed sinks hold at one moment.
#[derive(Debug)]
struct Observed {
    /// Rows in the table but not the replica, and in the replica but not the table.
    replica_differences: (i64, i64),
    buckets: Buckets,
    documents: Buckets,
    /// The newest committed record of each key in the topic, without the keys a tombstone removed.
    messages: Buckets,
}

async fn observe(
    postgres: &Postgres,
    mongo: &MongoDb,
    capture: &Capture,
    outputs: &Outputs,
) -> Observed {
    let (
        table,
        Outputs {
            replica, buckets, ..
        },
    ) = (&capture.table, outputs);
    let client = postgres.client().await;
    let differences = client
        .query_one(
            &format!(
                "SELECT (SELECT count(*) FROM (SELECT id, amount, note FROM {table} \
                 EXCEPT SELECT id, amount, note FROM {replica}) missing), \
                 (SELECT count(*) FROM (SELECT id, amount, note FROM {replica} \
                 EXCEPT SELECT id, amount, note FROM {table}) extra)"
            ),
            &[],
        )
        .await
        .expect("compare the replica with the table");
    let replica_differences = (differences.get(0), differences.get(1));
    let buckets = client
        .query(
            &format!("SELECT bucket, n, total, peak FROM {buckets} ORDER BY bucket"),
            &[],
        )
        .await
        .expect("read the bucket table")
        .into_iter()
        .map(|row| std::array::from_fn(|column| row.get::<_, i64>(column)))
        .collect();
    Observed {
        replica_differences,
        buckets,
        documents: mongo
            .documents(&outputs.buckets, "bucket")
            .await
            .iter()
            .map(|document| {
                ["bucket", "n", "total", "peak"].map(|field| match document.get(field) {
                    Some(Bson::Int64(value)) => *value,
                    Some(Bson::Int32(value)) => i64::from(*value),
                    other => panic!("bucket document field {field} is {other:?}"),
                })
            })
            .collect(),
        messages: topic_buckets(&outputs.brokers, &outputs.buckets),
    }
}

/// The newest record of each key in `topic` that `read_committed` consumers see, in bucket order.
fn topic_buckets(brokers: &str, topic: &str) -> Buckets {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("{topic}_oracle"))
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true")
        .set("isolation.level", "read_committed")
        .create()
        .expect("bucket topic reader");
    let mut assignment = TopicPartitionList::new();
    for partition in 0..TOPIC_PARTITIONS {
        assignment
            .add_partition_offset(topic, partition, Offset::Beginning)
            .expect("assign a bucket topic partition");
    }
    consumer
        .assign(&assignment)
        .expect("assign the bucket topic");
    let mut newest = BTreeMap::new();
    let mut ended = BTreeSet::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while ended.len() < TOPIC_PARTITIONS as usize {
        assert!(
            Instant::now() < deadline,
            "the bucket topic reader never reached the end"
        );
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(message)) => {
                let key = message.key().expect("a bucket record has a key").to_vec();
                newest.insert(key, message.payload().map(<[u8]>::to_vec));
            }
            Some(Err(rdkafka::error::KafkaError::PartitionEOF(partition))) => {
                ended.insert(partition);
            }
            Some(Err(error)) => panic!("bucket topic reader: {error}"),
            None => {}
        }
    }
    let mut buckets: Buckets = newest
        .into_values()
        .flatten()
        .map(|value| {
            let record: serde_json::Value =
                serde_json::from_slice(&value).expect("a bucket record is a JSON object");
            ["bucket", "n", "total", "peak"].map(|field| {
                record[field]
                    .as_i64()
                    .unwrap_or_else(|| panic!("bucket record field {field} is {}", record[field]))
            })
        })
        .collect();
    buckets.sort_unstable();
    buckets
}

/// How many times a leader fenced the departed processes' writers on the bucket topic.
fn fenced_rounds(nodes: &[Node]) -> usize {
    nodes
        .iter()
        .map(|node| {
            std::fs::read_to_string(&node.log_path)
                .expect("read a node log")
                .lines()
                .filter(|line| {
                    line.contains("ran the sink writer fence for departed processes")
                        && line.contains("sink=bucket_records")
                })
                .count()
        })
        .sum()
}

/// Drops the bucket collection when the soak ends, even while a failed soak unwinds.
struct DropCollection<'a> {
    mongo: &'a MongoDb,
    collection: &'a str,
}

impl Drop for DropCollection<'_> {
    fn drop(&mut self) {
        self.mongo.delete(self.collection);
    }
}
