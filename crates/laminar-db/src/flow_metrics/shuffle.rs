//! Per-peer shuffle send series and shuffle budget use (cluster).

use std::sync::{Arc, Weak};

use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
use parking_lot::Mutex;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{CounterVec, IntCounterVec, IntGaugeVec, Opts, Registry};

struct ShuffleFamilies {
    endpoints: Mutex<Option<(Weak<ShuffleSender>, Weak<ShuffleReceiver>)>>,
    send_wait: CounterVec,
    sent_bytes: IntCounterVec,
    sent_rows: IntCounterVec,
    budget_used: IntGaugeVec,
}

impl ShuffleFamilies {
    fn new() -> Result<Self, prometheus::Error> {
        let peer = |name: &str, help: &str| IntCounterVec::new(Opts::new(name, help), &["peer"]);
        Ok(Self {
            endpoints: Mutex::new(None),
            send_wait: CounterVec::new(
                Opts::new(
                    "shuffle_send_wait_seconds_total",
                    "Time shuffle sends to each peer waited for outbound byte budget or a stream queue slot, excluding waits for another send's turn",
                ),
                &["peer"],
            )?,
            sent_bytes: peer(
                "shuffle_sent_bytes_total",
                "Arrow IPC bytes of data frames queued to each peer; control frames are excluded",
            )?,
            sent_rows: peer(
                "shuffle_sent_rows_total",
                "Rows of data frames queued to each peer",
            )?,
            budget_used: IntGaugeVec::new(
                Opts::new(
                    "shuffle_budget_used_bytes",
                    "Bytes held from this node's 128 MiB shuffle data budget: direction=outbound or inbound",
                ),
                &["direction"],
            )?,
        })
    }

    fn refresh(&self, sender: Option<&ShuffleSender>, receiver: Option<&ShuffleReceiver>) {
        self.send_wait.reset();
        self.sent_bytes.reset();
        self.sent_rows.reset();
        self.budget_used.reset();
        for flow in sender.map(ShuffleSender::peer_flow).unwrap_or_default() {
            let peer = [flow.peer.to_string()];
            self.send_wait
                .with_label_values(&peer)
                .inc_by(flow.send_wait.as_secs_f64());
            self.sent_bytes
                .with_label_values(&peer)
                .inc_by(flow.sent_bytes);
            self.sent_rows
                .with_label_values(&peer)
                .inc_by(flow.sent_rows);
        }
        let budgets = [
            (
                "outbound",
                sender.map(ShuffleSender::outbound_budget_used_bytes),
            ),
            (
                "inbound",
                receiver.map(ShuffleReceiver::inbound_budget_used_bytes),
            ),
        ];
        for (direction, used) in budgets {
            if let Some(used) = used {
                self.budget_used
                    .with_label_values(&[direction])
                    .set(i64::try_from(used).unwrap_or(i64::MAX));
            }
        }
    }
}

struct ShuffleCollector(Arc<ShuffleFamilies>);

impl Collector for ShuffleCollector {
    fn desc(&self) -> Vec<&Desc> {
        let families = &*self.0;
        let mut descs = families.send_wait.desc();
        descs.extend(families.sent_bytes.desc());
        descs.extend(families.sent_rows.desc());
        descs.extend(families.budget_used.desc());
        descs
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let families = &*self.0;
        // Held until collected: a concurrent scrape's reset must not empty these counters.
        let endpoints = families.endpoints.lock();
        let (sender, receiver) = endpoints
            .as_ref()
            .map_or((None, None), |(sender, receiver)| {
                (sender.upgrade(), receiver.upgrade())
            });
        families.refresh(sender.as_deref(), receiver.as_deref());
        let mut collected = families.send_wait.collect();
        collected.extend(families.sent_bytes.collect());
        collected.extend(families.sent_rows.collect());
        collected.extend(families.budget_used.collect());
        collected
    }
}

/// Per-peer send series and shuffle budget use of this process's shuffle endpoints, read when
/// Prometheus scrapes. Totals come from the endpoints, which live as long as the process.
pub(crate) struct ShuffleFlowMetrics(Arc<ShuffleFamilies>);

impl ShuffleFlowMetrics {
    pub(super) fn register(registry: &Registry) -> Result<Self, prometheus::Error> {
        let families = Arc::new(ShuffleFamilies::new()?);
        registry.register(Box::new(ShuffleCollector(Arc::clone(&families))))?;
        Ok(Self(families))
    }

    /// Held weakly, so a scrape never keeps the endpoints alive.
    pub(crate) fn observe(&self, sender: &Arc<ShuffleSender>, receiver: &Arc<ShuffleReceiver>) {
        *self.0.endpoints.lock() = Some((Arc::downgrade(sender), Arc::downgrade(receiver)));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::cluster::control::LeaseDeadline;
    use laminar_core::shuffle::{ShuffleMessage, ShuffleReceiver, ShuffleSender};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shuffle_series_report_each_peer_and_both_budgets() {
        let owners = [2, 1, 2, 1, 2, 1, 2, 1];
        let participants = [1, 2]
            .map(|node_id| CheckpointParticipant {
                node_id,
                boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
            })
            .to_vec();
        let fence = CheckpointAssignmentFence::from_owner_map(1, &owners, participants).unwrap();
        let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
        let receiver = Arc::new(
            ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(2))
                .await
                .unwrap(),
        );
        let sender = Arc::new(ShuffleSender::new(1, uuid::Uuid::from_u128(1)));
        receiver
            .install_process_lease_deadline(Arc::clone(&deadline))
            .unwrap();
        sender.install_process_lease_deadline(deadline).unwrap();
        receiver.install_assignment_fence(&fence, &owners).unwrap();
        sender.install_assignment_fence(&fence, &owners).unwrap();
        sender.register_peer(2, receiver.local_addr());

        let registry = prometheus::Registry::new();
        let metrics = crate::EngineMetrics::new(&registry);
        metrics.flow.shuffle.observe(&sender, &receiver);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        sender
            .send_to(2, &ShuffleMessage::checkpointed("stage".into(), 0, batch))
            .await
            .unwrap();

        let families = registry.gather();
        let value = |name: &str, label: &str| {
            let family = families.iter().find(|family| family.name() == name)?;
            let metric = family
                .get_metric()
                .iter()
                .find(|metric| metric.get_label()[0].value() == label)?;
            Some(match family.get_field_type() {
                prometheus::proto::MetricType::GAUGE => metric.get_gauge().value(),
                _ => metric.get_counter().value(),
            })
        };
        assert_eq!(value("shuffle_sent_rows_total", "2"), Some(3.0));
        assert!(value("shuffle_sent_bytes_total", "2").is_some_and(|bytes| bytes > 0.0));
        assert!(value("shuffle_send_wait_seconds_total", "2").is_some());
        assert!(value("shuffle_budget_used_bytes", "outbound").is_some());
        assert!(value("shuffle_budget_used_bytes", "inbound").is_some());
    }
}
