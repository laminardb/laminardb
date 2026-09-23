//! Source-owned Kafka reader lag sampling, independent of intake backpressure.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prometheus::core::Collector;
use prometheus::{Gauge, GaugeVec, IntGaugeVec, Opts, Registry};
use rdkafka::Offset;

use super::{
    fetch_partition_watermarks, join_background_task, Arc, ConnectorError, Consumer,
    KafkaBlockingTasks, KafkaPartitionBaselines, KafkaPartitionSet, KafkaSource,
    LaminarConsumerContext, Ordering, StreamConsumer,
};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct ProgressMetrics {
    reader_lag: IntGaugeVec,
    available: IntGaugeVec,
    sampled_at: GaugeVec,
    last_batch: Gauge,
}

impl ProgressMetrics {
    fn new(source: &str) -> Self {
        let opts = |name, help| Opts::new(name, help).const_label("source", source);
        let labels = &["topic", "partition"];
        Self {
            reader_lag: IntGaugeVec::new(
                opts("kafka_source_reader_lag_offsets", "Broker high watermark minus next Kafka reader offset; not processed or recovery lag"),
                labels,
            ).expect("static Kafka metric descriptors are valid"),
            available: IntGaugeVec::new(
                opts("kafka_source_lag_sample_available", "1 when the last Kafka lag collection succeeded with a numeric reader position"),
                labels,
            ).expect("static Kafka metric descriptors are valid"),
            sampled_at: GaugeVec::new(
                opts("kafka_source_lag_sample_timestamp_seconds", "Unix timestamp of the last successful Kafka reader lag sample"),
                labels,
            ).expect("static Kafka metric descriptors are valid"),
            last_batch: Gauge::with_opts(opts(
                "kafka_source_last_batch_timestamp_seconds",
                "Unix timestamp of the last nonempty successful source poll; zero until observed",
            )).expect("static Kafka metric descriptors are valid"),
        }
    }

    fn collectors(&self) -> [Box<dyn Collector>; 4] {
        [
            Box::new(self.reader_lag.clone()),
            Box::new(self.available.clone()),
            Box::new(self.sampled_at.clone()),
            Box::new(self.last_batch.clone()),
        ]
    }

    fn reconcile(&self, previous: &mut KafkaPartitionSet, current: &KafkaPartitionSet) {
        for (topic, partition) in previous.difference(current) {
            let partition = partition.to_string();
            let labels = &[topic.as_str(), partition.as_str()];
            let _ = self.reader_lag.remove_label_values(labels);
            let _ = self.available.remove_label_values(labels);
            let _ = self.sampled_at.remove_label_values(labels);
        }
        for (topic, partition) in current.difference(previous) {
            self.available
                .with_label_values(&[topic, &partition.to_string()])
                .set(0);
        }
        previous.clone_from(current);
    }

    fn publish(
        &self,
        assigned: &KafkaPartitionSet,
        high_watermarks: Option<&KafkaPartitionBaselines>,
        positions: &KafkaPartitionBaselines,
        now: f64,
    ) {
        for key @ (topic, partition) in assigned {
            let partition = partition.to_string();
            let labels = &[topic.as_str(), partition.as_str()];
            let lag = high_watermarks.and_then(|highs| {
                let high = *highs.get(key)?;
                let position = *positions.get(key)?;
                // A reader can advance beyond the earlier watermark while the query is in flight.
                // Unknown or racing positions are unavailable, never a fabricated zero backlog.
                (position >= 0 && position <= high).then(|| high - position)
            });
            if let Some(lag) = lag {
                self.reader_lag.with_label_values(labels).set(lag);
                self.sampled_at.with_label_values(labels).set(now);
                self.available.with_label_values(labels).set(1);
            } else {
                self.available.with_label_values(labels).set(0);
                let _ = self.reader_lag.remove_label_values(labels);
            }
        }
    }
}

pub(super) struct KafkaProgress {
    registry: Registry,
    metrics: ProgressMetrics,
    task: Option<tokio::task::JoinHandle<()>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl KafkaProgress {
    pub(super) fn register(registry: &Registry, source: &str) -> Result<Self, ConnectorError> {
        let metrics = ProgressMetrics::new(source);
        // Prometheus sums descriptor IDs for composite collectors; distinct source labels can
        // collide in that sum. Register families separately and roll back only this attempt.
        for (registered, collector) in metrics.collectors().into_iter().enumerate() {
            let Err(error) = registry.register(collector) else {
                continue;
            };
            let mut message =
                format!("register Kafka progress metrics for source '{source}': {error}");
            for previous in metrics.collectors().into_iter().take(registered) {
                if let Err(cleanup_error) = registry.unregister(previous) {
                    let _ = write!(message, "; registration rollback failed: {cleanup_error}");
                }
            }
            return Err(ConnectorError::ConfigurationError(message));
        }
        Ok(Self {
            registry: registry.clone(),
            metrics,
            task: None,
            shutdown: None,
        })
    }

    pub(super) fn record_batch(&self, rows: usize) {
        if rows > 0 {
            self.metrics.last_batch.set(timestamp_seconds());
        }
    }

    pub(super) fn stop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }

    pub(super) async fn close(&mut self, deadline: tokio::time::Instant) {
        self.stop();
        join_background_task(&mut self.task, deadline, "progress sampler").await;
    }
}

impl Drop for KafkaProgress {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            task.abort();
        }
        // Only this source-owned registration can unregister. Worker clones never own cleanup,
        // so a late sample cannot remove or overwrite a replacement source's collectors.
        for collector in self.metrics.collectors() {
            let _ = self.registry.unregister(collector);
        }
    }
}

impl KafkaSource {
    pub(super) fn start_progress(&mut self) {
        let (Some(progress), Some(consumer)) = (&mut self.progress, &self.consumer) else {
            return;
        };
        let sampler = ProgressSampler {
            consumer: Arc::clone(consumer),
            blocking_tasks: self.blocking_tasks.clone(),
            metrics: progress.metrics.clone(),
            revoke_generation: Arc::clone(&self.revoke_generation),
            assign_generation: Arc::clone(&self.assign_generation),
            assignment_version: Arc::clone(&self.reconciled_assignment_version),
            published: HashSet::new(),
        };
        let guard = self
            .task_owner
            .track()
            .expect("live source admits progress sampler");
        let (shutdown, receiver) = tokio::sync::oneshot::channel();
        progress.shutdown = Some(shutdown);
        progress.task = Some(tokio::spawn(async move {
            let _guard = guard;
            sampler.run(receiver, SAMPLE_INTERVAL).await;
        }));
    }
}

struct ProgressSampler {
    consumer: Arc<StreamConsumer<LaminarConsumerContext>>,
    blocking_tasks: KafkaBlockingTasks,
    metrics: ProgressMetrics,
    revoke_generation: Arc<AtomicU64>,
    assign_generation: Arc<AtomicU64>,
    assignment_version: Arc<AtomicU64>,
    published: KafkaPartitionSet,
}

impl ProgressSampler {
    fn generation(&self) -> (u64, u64, u64) {
        (
            self.revoke_generation.load(Ordering::Acquire),
            self.assign_generation.load(Ordering::Acquire),
            self.assignment_version.load(Ordering::Acquire),
        )
    }

    fn assignment(&self) -> KafkaPartitionSet {
        self.consumer.assignment().map_or_else(
            |_| HashSet::new(),
            |assignment| {
                assignment
                    .elements()
                    .iter()
                    .map(|entry| (entry.topic().to_owned(), entry.partition()))
                    .collect()
            },
        )
    }

    async fn sample(&mut self) {
        let generation = self.generation();
        let assigned = self.assignment();
        self.metrics.reconcile(&mut self.published, &assigned);
        if assigned.is_empty() {
            return;
        }
        if !self.blocking_tasks.is_idle().await {
            // Native calls may outlive their async timeout. Do not accumulate another round
            // behind a delayed worker. Also yield when assignment/recovery work is observed.
            self.metrics.publish(
                &assigned,
                None,
                &KafkaPartitionBaselines::new(),
                timestamp_seconds(),
            );
            return;
        }
        let watermarks = fetch_partition_watermarks(
            self.blocking_tasks.clone(),
            Arc::clone(&self.consumer),
            &assigned,
        )
        .await;
        let positions = self.consumer.position().map_or_else(
            |_| KafkaPartitionBaselines::new(),
            |positions| {
                positions
                    .elements()
                    .iter()
                    .filter_map(|entry| match entry.offset() {
                        Offset::Offset(offset) => {
                            Some(((entry.topic().to_owned(), entry.partition()), offset))
                        }
                        _ => None,
                    })
                    .collect()
            },
        );
        let current = self.assignment();
        self.metrics.reconcile(&mut self.published, &current);
        let high = watermarks
            .as_ref()
            .ok()
            .filter(|_| generation == self.generation() && current == assigned)
            .map(|(_, high)| high);
        self.metrics
            .publish(&current, high, &positions, timestamp_seconds());
    }

    async fn run(mut self, mut shutdown: tokio::sync::oneshot::Receiver<()>, period: Duration) {
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = interval.tick() => {}
            }
            // One bounded round at a time; the existing blocking-task owner retains native
            // calls if cancellation wins while librdkafka is still returning from a timeout.
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                () = self.sample() => {}
            }
        }
    }
}

fn timestamp_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[cfg(test)]
mod tests;
