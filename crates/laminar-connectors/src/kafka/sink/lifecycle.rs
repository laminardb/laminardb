//! Tracked off-runtime destruction for Kafka producer generations.

use rdkafka::producer::FutureProducer;

use super::KafkaSink;

impl KafkaSink {
    /// Destroy final producer references away from Tokio workers. rdkafka purges, flushes for up
    /// to 500 ms, and joins its polling thread in `Drop`.
    pub(super) fn retire_producers(&mut self) {
        if let Some(producer) = self.producer.take() {
            self.spawn_producer_drop(producer, "main");
        }
        if let Some(producer) = self.dlq_producer.take() {
            self.spawn_producer_drop(producer, "DLQ");
        }
    }

    fn spawn_producer_drop(&self, producer: FutureProducer, role: &'static str) {
        let Some(terminal_guard) = self.task_owner.track() else {
            // The owner is a field on this live connector, so sealing before Drop is an invariant
            // violation rather than a recoverable state.
            tracing::error!(
                role,
                "Kafka producer teardown could not enter the terminal task tracker"
            );
            return;
        };
        let teardown = move || {
            let _terminal_guard = terminal_guard;
            drop(producer);
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            drop(runtime.spawn_blocking(teardown));
        } else if let Err(error) = std::thread::Builder::new()
            .name("laminardb-kafka-producer-drop".into())
            .spawn(teardown)
        {
            // This branch cannot run on a Tokio worker. The closure has already been dropped by
            // `spawn`, so only report the resource failure.
            tracing::error!(role, %error, "failed to start Kafka producer teardown thread");
        }
    }
}
