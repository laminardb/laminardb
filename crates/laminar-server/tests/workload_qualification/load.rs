use std::fs::File;
use std::io::{BufWriter, Write as _};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{anyhow, ensure, Context as _, Result};
use futures::{stream::FuturesUnordered, StreamExt as _};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};

use super::latency::Latency;
use super::spec::{Distribution, Spec};

#[derive(Default)]
pub(super) struct Counts {
    pub enqueued: AtomicU64,
    pub acknowledged: AtomicU64,
    pub observed: AtomicU64,
    pub frontiers: [AtomicU64; 4],
}

pub(super) struct Inputs {
    spec: Spec,
    zipf: super::super::ZipfSampler,
    payload: String,
}

impl Inputs {
    pub fn new(spec: &Spec) -> Self {
        Self {
            spec: spec.clone(),
            zipf: super::super::ZipfSampler::new(spec.keys, 1_000),
            payload: "x".repeat(spec.payload_bytes),
        }
    }

    pub fn key(&self, id: u64) -> u64 {
        let id = id ^ self.spec.seed;
        match self.spec.distribution {
            Distribution::Uniform => super::super::splitmix64(id) % self.spec.keys,
            Distribution::Zipf => self.zipf.sample(id),
            Distribution::HotKey => 0,
        }
    }

    fn payload(&self, pipeline: usize, id: u64) -> String {
        serde_json::json!({"origin": pipeline, "id": id, "key": self.key(id), "payload": self.payload}).to_string()
    }
}

pub(super) struct Producer {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<Result<super::latency::Distribution>>>,
}

impl Producer {
    pub fn spawn(
        spec: Spec,
        brokers: String,
        topics: Vec<String>,
        start: Instant,
        counts: Arc<Counts>,
        directory: &Path,
    ) -> Result<Self> {
        let ledger = BufWriter::new(File::create(directory.join("source.jsonl"))?);
        let stop = Arc::new(AtomicBool::new(false));
        let cancel = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let deadline = start + Duration::from_secs(spec.seconds + spec.drain_seconds);
            runtime.block_on(async {
                tokio::time::timeout_at(
                    deadline.into(),
                    produce(&spec, &brokers, &topics, start, &counts, &cancel, ledger),
                )
                .await
                .context("producer exceeded load/drain deadline")?
            })
        });
        Ok(Self {
            stop,
            handle: Some(handle),
        })
    }

    pub fn is_finished(&self) -> bool {
        self.handle.as_ref().is_some_and(JoinHandle::is_finished)
    }

    pub fn finish(&mut self) -> Result<super::latency::Distribution> {
        self.handle
            .take()
            .context("producer already joined")?
            .join()
            .map_err(|_| anyhow!("producer thread panicked"))?
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

async fn delivery(
    future: DeliveryFuture,
    pipeline: usize,
    id: u64,
) -> Result<(usize, u64, i32, i64)> {
    let delivered = future
        .await
        .context("Kafka delivery cancelled")?
        .map_err(|(error, _)| anyhow!("Kafka input delivery: {error}"))?;
    Ok((pipeline, id, delivered.partition, delivered.offset))
}

fn acknowledge(
    result: Result<(usize, u64, i32, i64)>,
    start: Instant,
    counts: &Counts,
    ledger: &mut BufWriter<File>,
) -> Result<()> {
    let (pipeline, id, partition, offset) = result?;
    writeln!(
        ledger,
        "{}",
        serde_json::json!({"pipeline":pipeline, "id":id,
        "partition":partition, "offset":offset, "acknowledged_ns":start.elapsed().as_nanos()})
    )?;
    counts.acknowledged.fetch_add(1, Ordering::Release);
    Ok(())
}

async fn produce(
    spec: &Spec,
    brokers: &str,
    topics: &[String],
    start: Instant,
    counts: &Counts,
    stop: &AtomicBool,
    mut ledger: BufWriter<File>,
) -> Result<super::latency::Distribution> {
    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "10000")
        .set("queue.buffering.max.messages", "8192")
        .create()?;
    let inputs = Inputs::new(spec);
    let lag = Latency::new()?;
    let mut pending = FuturesUnordered::new();
    for id in 0..spec.rows_per_pipeline() {
        let scheduled = start + spec.scheduled(id);
        while Instant::now() < scheduled || pending.len() >= 4_096 {
            ensure!(!stop.load(Ordering::Acquire), "producer cancelled");
            tokio::select! {
                Some(result) = pending.next(), if !pending.is_empty() => {
                    acknowledge(result, start, counts, &mut ledger)?;
                }
                () = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
        }
        ensure!(!stop.load(Ordering::Acquire), "producer cancelled");
        let key = inputs.key(id).to_string();
        for (pipeline, topic) in topics.iter().enumerate() {
            let payload = inputs.payload(pipeline, id);
            // The original schedule is never rebased after broker/producer stalls.
            lag.observe(
                Instant::now()
                    .saturating_duration_since(scheduled)
                    .as_secs_f64(),
            );
            let future = producer
                .send_result(FutureRecord::to(topic).key(&key).payload(&payload))
                .map_err(|(error, _)| anyhow!("Kafka input enqueue: {error}"))?;
            pending.push(delivery(future, pipeline, id));
            counts.enqueued.fetch_add(1, Ordering::Release);
        }
    }
    while !pending.is_empty() {
        ensure!(
            !stop.load(Ordering::Acquire),
            "producer cancelled while draining"
        );
        tokio::select! {
            Some(result) = pending.next() => acknowledge(result, start, counts, &mut ledger)?,
            () = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
    ledger.flush()?;
    Ok(lag.distribution())
}
