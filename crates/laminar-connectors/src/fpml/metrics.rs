//! Prometheus metrics for one FpML-decoding source. Labels are bounded: the catalog source name
//! and, for rejections, the stage. No trade identifiers or document-controlled text.

use prometheus::core::Collector;
use prometheus::{Histogram, HistogramOpts, IntCounter, IntCounterVec, Opts, Registry};

use super::error::Stage;
use crate::error::ConnectorError;

pub(crate) struct FpmlMetrics {
    pub(crate) documents_accepted: IntCounter,
    pub(crate) documents_rejected: IntCounterVec,
    pub(crate) rows: IntCounter,
    pub(crate) document_bytes: IntCounter,
    pub(crate) decode_seconds: Histogram,
    registration: Option<Registry>,
}

impl FpmlMetrics {
    pub(crate) fn local(source: &str) -> Result<Self, ConnectorError> {
        let counter = |name: &str, help: &str| {
            IntCounter::with_opts(Opts::new(name, help).const_label("source", source))
        };
        let build = || -> Result<Self, prometheus::Error> {
            Ok(Self {
                documents_accepted: counter(
                    "fpml_documents_accepted_total",
                    "FpML documents validated and projected",
                )?,
                documents_rejected: IntCounterVec::new(
                    Opts::new(
                        "fpml_documents_rejected_total",
                        "FpML documents rejected, by decoding stage",
                    )
                    .const_label("source", source),
                    &["stage"],
                )?,
                rows: counter("fpml_rows_total", "Rows projected from accepted FpML documents")?,
                document_bytes: counter(
                    "fpml_document_bytes_total",
                    "Bytes of FpML documents submitted for decoding",
                )?,
                decode_seconds: Histogram::with_opts(
                    HistogramOpts::new(
                        "fpml_decode_seconds",
                        "Pre-scan, parse, schema validation, reference checks, and projection time per document",
                    )
                    .const_label("source", source)
                    .buckets(vec![
                        0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1,
                        0.25, 0.5, 1.0, 2.5,
                    ]),
                )?,
                registration: None,
            })
        };
        build()
            .map_err(|error| ConnectorError::Internal(format!("FpML metric descriptor: {error}")))
    }

    fn collectors(&self) -> Vec<Box<dyn Collector>> {
        vec![
            Box::new(self.documents_accepted.clone()),
            Box::new(self.documents_rejected.clone()),
            Box::new(self.rows.clone()),
            Box::new(self.document_bytes.clone()),
            Box::new(self.decode_seconds.clone()),
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
                    "cannot register FpML metrics for source '{source}': {error}"
                )));
            }
        }
        metrics.registration = Some(registry.clone());
        Ok(metrics)
    }

    pub(crate) fn rejected(&self, stage: Stage) {
        self.documents_rejected
            .with_label_values(&[stage.label()])
            .inc();
    }
}

impl Drop for FpmlMetrics {
    fn drop(&mut self) {
        if let Some(registry) = self.registration.take() {
            for collector in self.collectors() {
                let _ = registry.unregister(collector);
            }
        }
    }
}
