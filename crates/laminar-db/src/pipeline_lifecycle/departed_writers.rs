//! Fencing the sink writers of processes a recovery removes from the cluster.

use laminar_core::checkpoint::CheckpointParticipant;

use crate::connector_manager::{build_sink_config, normalize_connector_type};
use crate::db::LaminarDB;

impl LaminarDB {
    /// Fence the writers `departed` processes hold on every sink whose connector type registered
    /// a writer fence, before the recovery that removes them is recorded and a successor writes
    /// their keys. Fences take each sink's registration, not a built sink: a sink needs the
    /// pipeline's resolved schema to be built, and the leader may not have started its pipeline.
    pub(crate) async fn fence_departed_sink_writers(
        &self,
        departed: &[CheckpointParticipant],
        deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        if departed.is_empty() {
            return Ok(());
        }
        let registrations = self
            .connector_manager
            .lock()
            .sinks()
            .values()
            .filter_map(|registration| {
                let connector_type = registration.connector_type.as_deref()?;
                let fence = self
                    .connector_registry
                    .sink_writer_fence(&normalize_connector_type(connector_type))?;
                Some((registration.clone(), fence))
            })
            .collect::<Vec<_>>();
        for (registration, fence) in registrations {
            let name = &registration.name;
            let config = build_sink_config(&registration, self.config.delivery_guarantee)
                .map_err(|error| format!("cannot configure sink '{name}': {error}"))?;
            fence(config, name.clone(), departed.to_vec(), deadline)
                .await
                .map_err(|error| {
                    format!(
                        "cannot fence the writers of departed processes on sink '{name}': {error}"
                    )
                })?;
            tracing::info!(sink = %name, ?departed, "ran the sink writer fence for departed processes");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use laminar_connectors::config::ConnectorInfo;
    use laminar_connectors::error::ConnectorError;
    use laminar_connectors::schema::resolution::SchemaCapabilities;
    use laminar_connectors::testing::MockSinkConnector;
    use parking_lot::Mutex;

    use super::*;

    /// The sinks and processes the probe's writer fence was asked to fence, and how many probe
    /// sinks were built.
    #[derive(Default)]
    struct Probe {
        fenced: Mutex<Vec<(String, Vec<CheckpointParticipant>)>>,
        built: AtomicUsize,
    }

    fn info(name: &str) -> ConnectorInfo {
        ConnectorInfo {
            schema_capabilities: SchemaCapabilities::declared(true),
            name: name.into(),
            display_name: name.into(),
            version: "1".into(),
            is_source: false,
            is_sink: true,
            config_keys: vec![],
        }
    }

    /// `fenced-probe` registers a writer fence that refuses when its `refuse` option is set;
    /// `unfenced-probe` registers none.
    async fn database(probe: &Arc<Probe>, sinks: &[&str]) -> Arc<LaminarDB> {
        let probe = Arc::clone(probe);
        let db = LaminarDB::builder()
            .register_connector(move |registry| {
                for name in ["fenced-probe", "unfenced-probe"] {
                    let built = Arc::clone(&probe);
                    registry.register_sink(
                        name,
                        info(name),
                        Arc::new(move |_, _| {
                            built.built.fetch_add(1, Ordering::SeqCst);
                            Ok(Box::new(MockSinkConnector::new()))
                        }),
                    )?;
                }
                registry.register_sink_writer_fence(
                    "fenced-probe",
                    Arc::new(move |config, sink_id, departed, _deadline| {
                        let probe = Arc::clone(&probe);
                        Box::pin(async move {
                            if config.get("refuse") == Some("true") {
                                return Err(ConnectorError::ConnectionFailed(
                                    "broker unreachable".into(),
                                ));
                            }
                            probe.fenced.lock().push((sink_id, departed));
                            Ok(())
                        })
                    }),
                )
            })
            .build()
            .await
            .unwrap();
        db.execute("CREATE SOURCE input (id BIGINT NOT NULL)")
            .await
            .unwrap();
        for sink in sinks {
            db.execute(sink).await.unwrap();
        }
        db
    }

    fn departed(node_id: u64) -> CheckpointParticipant {
        CheckpointParticipant {
            node_id,
            boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id) + 100),
        }
    }

    fn deadline() -> tokio::time::Instant {
        tokio::time::Instant::now() + std::time::Duration::from_secs(5)
    }

    #[tokio::test]
    async fn fences_run_from_registrations_without_building_a_sink() {
        let probe = Arc::new(Probe::default());
        let db = database(
            &probe,
            &[
                "CREATE SINK keyed FROM input INTO \"fenced-probe\" ('refuse'='false')",
                "CREATE SINK other FROM input INTO \"unfenced-probe\" ('refuse'='false')",
            ],
        )
        .await;
        let built = probe.built.load(Ordering::SeqCst);

        db.fence_departed_sink_writers(&[departed(2), departed(3)], deadline())
            .await
            .unwrap();

        assert_eq!(
            *probe.fenced.lock(),
            [("keyed".to_string(), vec![departed(2), departed(3)])]
        );
        assert_eq!(probe.built.load(Ordering::SeqCst), built);
    }

    #[tokio::test]
    async fn a_writer_that_cannot_be_fenced_fails_the_recovery() {
        let probe = Arc::new(Probe::default());
        let db = database(
            &probe,
            &["CREATE SINK keyed FROM input INTO \"fenced-probe\" ('refuse'='true')"],
        )
        .await;

        let error = db
            .fence_departed_sink_writers(&[departed(2)], deadline())
            .await
            .unwrap_err();

        assert!(
            error.contains("sink 'keyed'") && error.contains("broker unreachable"),
            "{error}"
        );
        db.fence_departed_sink_writers(&[], deadline())
            .await
            .unwrap();
    }
}
