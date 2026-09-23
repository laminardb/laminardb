use std::fs::File;
use std::io::{BufWriter, Write as _};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{anyhow, ensure, Context as _, Result};
use rdkafka::consumer::{BaseConsumer, Consumer as _};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::Message as _;
use serde::Serialize;

use super::latency::{Distribution, Latency};
use super::load::{Counts, Inputs};
use super::spec::Spec;

#[derive(Serialize)]
pub(super) struct Observation {
    pub unique_rows: u64,
    pub duplicates: u64,
    pub latency: Vec<Distribution>,
    pub consumed_offsets: Vec<i64>,
    pub frozen_offsets: Vec<i64>,
}

pub(super) struct Ledger {
    seen: Vec<Vec<bool>>,
    frontiers: Vec<u64>,
    pub unique_rows: u64,
    duplicates: u64,
    latency: Vec<Latency>,
    inputs: Inputs,
    spec: Spec,
}

impl Ledger {
    pub fn new(spec: &Spec) -> Result<Self> {
        let rows = usize::try_from(spec.rows_per_pipeline())?;
        Ok(Self {
            seen: vec![vec![false; rows]; spec.pipelines],
            frontiers: vec![0; spec.pipelines],
            unique_rows: 0,
            duplicates: 0,
            latency: (0..spec.pipelines)
                .map(|_| Latency::new())
                .collect::<Result<_>>()?,
            inputs: Inputs::new(spec),
            spec: spec.clone(),
        })
    }

    pub fn observe(
        &mut self,
        pipeline: usize,
        value: &serde_json::Value,
        elapsed: Duration,
        counts: &Counts,
    ) -> Result<bool> {
        let id = value["id"].as_u64().context("output omitted unsigned id")?;
        ensure!(
            pipeline < self.seen.len() && id < self.spec.rows_per_pipeline(),
            "impossible output id/pipeline"
        );
        ensure!(
            value["origin"].as_u64() == Some(pipeline as u64),
            "output source mismatch for pipeline {pipeline}, id {id}"
        );
        ensure!(
            value["key"].as_u64() == Some(self.inputs.key(id)),
            "output key mismatch for {id}"
        );
        ensure!(
            value["value"].as_u64() == Some(id * 3 + 7),
            "SQL result mismatch for {id}"
        );
        let payload = value["payload"]
            .as_str()
            .context("output omitted payload")?;
        ensure!(
            payload.len() == self.spec.payload_bytes && payload.bytes().all(|b| b == b'X'),
            "output payload mismatch for {id}"
        );
        let scheduled = self.spec.scheduled(id);
        ensure!(
            elapsed >= scheduled,
            "output appeared before its scheduled arrival"
        );
        if self.seen[pipeline][id as usize] {
            self.duplicates += 1;
            return Ok(false);
        }
        self.seen[pipeline][id as usize] = true;
        self.unique_rows += 1;
        let frontier = &mut self.frontiers[pipeline];
        while *frontier < self.spec.rows_per_pipeline() && self.seen[pipeline][*frontier as usize] {
            *frontier += 1;
        }
        counts.frontiers[pipeline].store(*frontier, Ordering::Release);
        counts.observed.fetch_add(1, Ordering::Release);
        if scheduled >= Duration::from_secs(self.spec.warmup_seconds) {
            self.latency[pipeline].observe((elapsed - scheduled).as_secs_f64());
        }
        Ok(true)
    }
}

pub(super) struct Observer {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<Result<Observation>>>,
}

impl Observer {
    pub fn spawn(
        spec: Spec,
        brokers: &str,
        topics: Vec<String>,
        start: Instant,
        counts: Arc<Counts>,
        directory: &Path,
    ) -> Result<Self> {
        let consumer: BaseConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", format!("qualification-{}", topics[0]))
            .set("enable.auto.commit", "false")
            .set("isolation.level", "read_committed")
            .set("fetch.wait.max.ms", "10")
            .create()?;
        let mut assignment = rdkafka::TopicPartitionList::new();
        for topic in &topics {
            assignment.add_partition_offset(topic, 0, rdkafka::Offset::Beginning)?;
        }
        consumer.assign(&assignment)?;
        let ledger = Ledger::new(&spec)?;
        let raw = BufWriter::new(File::create(directory.join("visibility.jsonl"))?);
        let stop = Arc::new(AtomicBool::new(false));
        let cancel = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            observe(spec, consumer, topics, start, counts, cancel, ledger, raw)
        });
        Ok(Self {
            stop,
            handle: Some(handle),
        })
    }

    pub fn is_finished(&self) -> bool {
        self.handle.as_ref().is_some_and(JoinHandle::is_finished)
    }

    pub fn finish(&mut self) -> Result<Observation> {
        self.stop.store(true, Ordering::Release);
        self.handle
            .take()
            .context("observer already joined")?
            .join()
            .map_err(|_| anyhow!("observer thread panicked"))?
    }
}

impl Drop for Observer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn observe(
    spec: Spec,
    consumer: BaseConsumer,
    topics: Vec<String>,
    start: Instant,
    counts: Arc<Counts>,
    stop: Arc<AtomicBool>,
    mut ledger: Ledger,
    mut raw: BufWriter<File>,
) -> Result<Observation> {
    let deadline = start + Duration::from_secs(spec.seconds + spec.drain_seconds + 10);
    let mut consumed = vec![0; topics.len()];
    let mut frozen = None;
    let mut stable_since = Instant::now();
    while Instant::now() < deadline {
        if let Some(result) = consumer.poll(Duration::from_millis(10)) {
            let message = match result {
                Err(KafkaError::MessageConsumption(
                    code @ (RDKafkaErrorCode::BrokerTransportFailure
                    | RDKafkaErrorCode::AllBrokersDown),
                )) => {
                    // Broker fault injection also disconnects this independent consumer. Keep
                    // its ledger and original deadline; malformed output still fails below.
                    eprintln!("qualification observer reconnecting after {code}");
                    continue;
                }
                result => result?,
            };
            let elapsed = start.elapsed();
            let pipeline = topics
                .iter()
                .position(|t| t == message.topic())
                .context("unexpected topic")?;
            ensure!(message.partition() == 0, "unexpected output partition");
            let value = serde_json::from_slice(message.payload().context("unexpected tombstone")?)?;
            let first = ledger.observe(pipeline, &value, elapsed, &counts)?;
            consumed[pipeline] = message.offset() + 1;
            writeln!(
                raw,
                "{}",
                serde_json::json!({"pipeline":pipeline, "origin":value["origin"],
                "id":value["id"], "key":value["key"], "value":value["value"],
                "offset":message.offset(), "observed_ns":elapsed.as_nanos(), "first":first})
            )?;
        }
        if !stop.load(Ordering::Acquire) {
            continue;
        }
        // Freeze public Kafka boundaries, then require a drained, stable reread before finishing.
        if frozen.is_none() || stable_since.elapsed() >= Duration::from_secs(2) {
            let boundary = topics
                .iter()
                .map(|t| {
                    consumer
                        .fetch_watermarks(t, 0, Duration::from_secs(1))
                        .map(|(_, high)| high)
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if frozen.as_ref() == Some(&boundary)
                && consumed.iter().zip(&boundary).all(|(c, b)| c >= b)
            {
                raw.flush()?;
                ensure!(
                    ledger.unique_rows == spec.total_rows(),
                    "missing output: observed {}/{}",
                    ledger.unique_rows,
                    spec.total_rows()
                );
                return Ok(Observation {
                    unique_rows: ledger.unique_rows,
                    duplicates: ledger.duplicates,
                    latency: ledger.latency.iter().map(Latency::distribution).collect(),
                    consumed_offsets: consumed,
                    frozen_offsets: boundary,
                });
            }
            frozen = Some(boundary);
            stable_since = Instant::now();
        }
    }
    anyhow::bail!(
        "external observer deadline: {}/{} rows",
        ledger.unique_rows,
        spec.total_rows()
    )
}
