//! Prometheus metrics setup for LaminarDB server.

use prometheus::{Encoder, IntCounter, IntGauge, Registry, TextEncoder};

/// Create the prometheus registry with const labels and process collector.
pub fn build_registry(const_labels: impl IntoIterator<Item = (String, String)>) -> Registry {
    let labels: std::collections::HashMap<_, _> = const_labels.into_iter().collect();
    let registry = Registry::new_custom(Some("laminardb".into()), Some(labels))
        .expect("registry construction is infallible with valid label names");

    #[cfg(target_os = "linux")]
    {
        let pc = prometheus::process_collector::ProcessCollector::for_self();
        registry
            .register(Box::new(pc))
            .expect("process collector registration");
    }

    registry
}

/// Render the registry as Prometheus text format 0.0.4.
pub fn render(registry: &Registry) -> Vec<u8> {
    let mf = registry.gather();
    let mut buf = Vec::with_capacity(4096);
    TextEncoder::new()
        .encode(&mf, &mut buf)
        .expect("encoding is infallible");
    buf
}

/// Server-level metrics (reload, uptime, connections).
pub struct ServerMetrics {
    pub reload_total: IntCounter,
    pub uptime_seconds: IntGauge,
    pub ws_connections: IntGauge,
}

impl ServerMetrics {
    /// Register server metrics. Startup only.
    #[must_use]
    pub fn new(registry: &Registry) -> Self {
        macro_rules! reg {
            ($m:expr) => {{
                let m = $m;
                registry.register(Box::new(m.clone())).unwrap();
                m
            }};
        }
        Self {
            reload_total: reg!(IntCounter::new("reload_total", "Config reload count").unwrap()),
            uptime_seconds: reg!(
                IntGauge::new("uptime_seconds", "Server uptime in seconds").unwrap()
            ),
            ws_connections: reg!(
                IntGauge::new("ws_connections", "Active WebSocket connections").unwrap()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laminar_db::EngineMetrics;

    #[test]
    fn registry_renders_engine_metrics() {
        let registry = build_registry([
            ("instance".into(), "test".into()),
            ("pipeline".into(), "smoke".into()),
        ]);
        let engine = EngineMetrics::new(&registry);

        engine.events_ingested.inc_by(100);
        engine.events_emitted.inc_by(42);
        engine.cycles.inc();

        let text = String::from_utf8(render(&registry)).unwrap();
        assert!(text.contains("laminardb_events_ingested_total"));
        assert!(text.contains("laminardb_events_emitted_total"));
        assert!(text.contains("laminardb_cycles_total"));
    }

    #[test]
    fn server_metrics_appear_in_output() {
        let registry = build_registry([
            ("instance".into(), "test".into()),
            ("pipeline".into(), "t".into()),
        ]);
        let srv = ServerMetrics::new(&registry);
        srv.reload_total.inc();
        srv.uptime_seconds.set(60);
        srv.ws_connections.set(3);

        let text = String::from_utf8(render(&registry)).unwrap();
        assert!(text.contains("laminardb_reload_total"));
        assert!(text.contains("laminardb_uptime_seconds"));
        assert!(text.contains("laminardb_ws_connections"));
    }
}
