/// Metrics snapshot returned from a connector's `metrics()` method.
#[derive(Debug, Clone, Default)]
pub struct ConnectorMetrics {
    /// Total number of records processed.
    pub records_total: u64,

    /// Total bytes processed.
    pub bytes_total: u64,

    /// Number of errors encountered.
    pub errors_total: u64,

    /// Current lag (records behind for sources, pending for sinks).
    pub lag: u64,

    /// Additional connector-specific metrics.
    pub custom: Vec<(String, f64)>,
}

impl ConnectorMetrics {
    /// Creates empty metrics.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a custom metric.
    pub fn add_custom(&mut self, name: impl Into<String>, value: f64) {
        self.custom.push((name.into(), value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_metrics() {
        let mut metrics = ConnectorMetrics::new();
        metrics.records_total = 1000;
        metrics.bytes_total = 50_000;
        metrics.add_custom("kafka.lag", 42.0);

        assert_eq!(metrics.records_total, 1000);
        assert_eq!(metrics.custom.len(), 1);
        assert_eq!(metrics.custom[0].0, "kafka.lag");
    }
}
