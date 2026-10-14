//! Prometheus metrics for one FIX source, labelled only by the catalog source name.

use prometheus::core::Collector;
use prometheus::{IntCounter, IntGauge, Opts, Registry};

use crate::error::ConnectorError;

/// Per-source FIX metrics. Labels are bounded: one `source` label, no message identifiers.
pub(crate) struct FixMetrics {
    pub(crate) logged_on: IntGauge,
    pub(crate) next_target_seq: IntGauge,
    pub(crate) journal_retained_bytes: IntGauge,
    pub(crate) journal_unread_records: IntGauge,
    pub(crate) journal_paused: IntGauge,
    pub(crate) feed_incomplete: IntGauge,
    pub(crate) messages_captured: IntCounter,
    pub(crate) messages_excluded: IntCounter,
    pub(crate) duplicates: IntCounter,
    pub(crate) duplicates_unverified: IntCounter,
    pub(crate) duplicate_conflicts: IntCounter,
    pub(crate) rejects_sent: IntCounter,
    pub(crate) gap_fill_seqs: IntCounter,
    pub(crate) sequence_resets: IntCounter,
    pub(crate) rows_emitted: IntCounter,
    pub(crate) projection_errors: IntCounter,
    registration: Option<Registry>,
}

impl FixMetrics {
    /// Metrics that are not exported; used before registration and in tests.
    pub(crate) fn local(source: &str) -> Result<Self, ConnectorError> {
        let gauge = |name: &str, help: &str| {
            IntGauge::with_opts(Opts::new(name, help).const_label("source", source))
        };
        let counter = |name: &str, help: &str| {
            IntCounter::with_opts(Opts::new(name, help).const_label("source", source))
        };
        let build = || -> Result<Self, prometheus::Error> {
            Ok(Self {
                logged_on: gauge(
                    "fix_source_logged_on",
                    "1 while the FIX session is logged on",
                )?,
                next_target_seq: gauge(
                    "fix_source_next_target_seq",
                    "Next expected inbound MsgSeqNum after the last captured message",
                )?,
                journal_retained_bytes: gauge(
                    "fix_source_journal_retained_bytes",
                    "Bytes retained by the FIX receive journal",
                )?,
                journal_unread_records: gauge(
                    "fix_source_journal_unread_records",
                    "Durable journal records not yet projected",
                )?,
                journal_paused: gauge(
                    "fix_source_journal_paused",
                    "1 while intake pauses because the journal is full",
                )?,
                feed_incomplete: gauge(
                    "fix_source_feed_incomplete",
                    "1 after an unrecoverable inbound sequence reset in this run",
                )?,
                messages_captured: counter(
                    "fix_source_messages_captured_total",
                    "Application messages durably journaled",
                )?,
                messages_excluded: counter(
                    "fix_source_messages_excluded_total",
                    "Application messages outside the configured message types",
                )?,
                duplicates: counter(
                    "fix_source_duplicates_total",
                    "Retransmissions matching their journaled original",
                )?,
                duplicates_unverified: counter(
                    "fix_source_duplicates_unverified_total",
                    "Retransmissions whose original is no longer retained",
                )?,
                duplicate_conflicts: counter(
                    "fix_source_duplicate_conflicts_total",
                    "Retransmissions conflicting with their journaled original",
                )?,
                rejects_sent: counter(
                    "fix_source_rejects_sent_total",
                    "Session-level rejects sent for invalid inbound messages",
                )?,
                gap_fill_seqs: counter(
                    "fix_source_gap_fill_seqs_total",
                    "Inbound sequence numbers covered by SequenceReset-GapFill",
                )?,
                sequence_resets: counter(
                    "fix_source_sequence_resets_total",
                    "Unrecoverable SequenceReset-Reset jumps",
                )?,
                rows_emitted: counter(
                    "fix_source_rows_emitted_total",
                    "Rows emitted to the pipeline",
                )?,
                projection_errors: counter(
                    "fix_source_projection_errors_total",
                    "Messages that failed projection",
                )?,
                registration: None,
            })
        };
        build().map_err(|error| ConnectorError::Internal(format!("FIX metric descriptor: {error}")))
    }

    fn collectors(&self) -> Vec<Box<dyn Collector>> {
        vec![
            Box::new(self.logged_on.clone()),
            Box::new(self.next_target_seq.clone()),
            Box::new(self.journal_retained_bytes.clone()),
            Box::new(self.journal_unread_records.clone()),
            Box::new(self.journal_paused.clone()),
            Box::new(self.feed_incomplete.clone()),
            Box::new(self.messages_captured.clone()),
            Box::new(self.messages_excluded.clone()),
            Box::new(self.duplicates.clone()),
            Box::new(self.duplicates_unverified.clone()),
            Box::new(self.duplicate_conflicts.clone()),
            Box::new(self.rejects_sent.clone()),
            Box::new(self.gap_fill_seqs.clone()),
            Box::new(self.sequence_resets.clone()),
            Box::new(self.rows_emitted.clone()),
            Box::new(self.projection_errors.clone()),
        ]
    }

    /// Register every family, rolling back this attempt on failure.
    pub(crate) fn registered(source: &str, registry: &Registry) -> Result<Self, ConnectorError> {
        let mut metrics = Self::local(source)?;
        for (index, collector) in metrics.collectors().into_iter().enumerate() {
            if let Err(error) = registry.register(collector) {
                for previous in metrics.collectors().into_iter().take(index) {
                    let _ = registry.unregister(previous);
                }
                return Err(ConnectorError::ConfigurationError(format!(
                    "register FIX metrics for source '{source}': {error}"
                )));
            }
        }
        metrics.registration = Some(registry.clone());
        Ok(metrics)
    }
}

impl Drop for FixMetrics {
    fn drop(&mut self) {
        if let Some(registry) = self.registration.take() {
            for collector in self.collectors() {
                let _ = registry.unregister(collector);
            }
        }
    }
}
