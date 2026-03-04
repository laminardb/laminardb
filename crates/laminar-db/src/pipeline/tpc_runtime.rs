//! Thread-per-core runtime managing N `CoreHandle`s and their source I/O threads.
//!
//! The `TpcRuntime` is the entry point for TPC mode. It spawns core threads,
//! attaches source connectors via [`SourceIoThread`]s, and provides a unified
//! interface for polling all core outboxes.

use std::sync::Arc;

use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::SourceConnector;
use laminar_core::checkpoint::CheckpointBarrierInjector;
use laminar_core::tpc::TaggedOutput;
use laminar_core::tpc::{CoreConfig, CoreHandle, TpcConfig, TpcError};

use super::config::PipelineConfig;
use super::source_adapter::{SourceIoMetrics, SourceIoThread};

/// Thread-per-core runtime managing N `CoreHandle`s and their source I/O threads.
pub struct TpcRuntime {
    #[allow(dead_code)] // Used in Phase 5 for key-spec routing
    config: TpcConfig,
    cores: Vec<CoreHandle>,
    source_threads: Vec<SourceIoThread>,
    source_names: Vec<String>,
    /// `source_idx` → `core_id` routing (round-robin for now).
    routing: Vec<usize>,
    /// Round-robin counter for source attachment.
    next_core: usize,
}

impl TpcRuntime {
    /// Create a new TPC runtime with the given configuration.
    ///
    /// Spawns `config.num_cores` core threads immediately.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid or core threads
    /// fail to spawn.
    pub fn new(config: TpcConfig) -> Result<Self, TpcError> {
        config.validate()?;

        let mut cores = Vec::with_capacity(config.num_cores);
        for i in 0..config.num_cores {
            let core_config = CoreConfig {
                core_id: i,
                cpu_affinity: if config.cpu_pinning {
                    Some(config.cpu_start + i)
                } else {
                    None
                },
                inbox_capacity: config.inbox_capacity,
                outbox_capacity: config.outbox_capacity,
                reactor_config: config.reactor_config.clone(),
                backpressure: laminar_core::tpc::BackpressureConfig::default(),
                numa_aware: config.numa_aware,
            };
            cores.push(CoreHandle::spawn(core_config)?);
        }

        Ok(Self {
            config,
            cores,
            source_threads: Vec::new(),
            source_names: Vec::new(),
            routing: Vec::new(),
            next_core: 0,
        })
    }

    /// Attach a source to a core, spawning an I/O thread.
    ///
    /// Sources are assigned to cores in round-robin order.
    pub fn attach_source(
        &mut self,
        source_idx: usize,
        name: String,
        connector: Box<dyn SourceConnector>,
        connector_config: ConnectorConfig,
        pipeline_config: &PipelineConfig,
    ) {
        let core_id = self.next_core % self.cores.len();
        self.next_core += 1;

        let target_inbox = Arc::clone(self.cores[core_id].inbox());

        let io_thread = SourceIoThread::spawn(
            source_idx,
            name.clone(),
            connector,
            connector_config,
            target_inbox,
            pipeline_config.max_poll_records,
            pipeline_config.fallback_poll_interval,
        );

        self.source_threads.push(io_thread);
        self.source_names.push(name);
        self.routing.push(core_id);
    }

    /// Drain all core outboxes into the buffer. Returns total outputs collected.
    pub fn poll_all_outputs(&self, buffer: &mut Vec<TaggedOutput>) -> usize {
        let mut total = 0;
        for core in &self.cores {
            total += core.poll_outputs_into(buffer, 4096);
        }
        total
    }

    /// Get the barrier injector for a source (used by coordinator for checkpoint).
    #[must_use]
    pub fn injector(&self, source_idx: usize) -> &CheckpointBarrierInjector {
        &self.source_threads[source_idx].injector
    }

    /// Get checkpoint snapshot for a source (lock-free watch read).
    #[must_use]
    pub fn source_checkpoint(&self, source_idx: usize) -> SourceCheckpoint {
        self.source_threads[source_idx]
            .checkpoint_rx
            .borrow()
            .clone()
    }

    /// Get metrics for a source.
    #[must_use]
    pub fn source_metrics(&self, source_idx: usize) -> &Arc<SourceIoMetrics> {
        &self.source_threads[source_idx].metrics
    }

    /// Shutdown all I/O threads and core handles. Returns connectors for `close()`.
    pub fn shutdown(&mut self) -> Vec<(String, Box<dyn SourceConnector>)> {
        let mut connectors = Vec::new();

        // Shutdown source I/O threads first (they push to core inboxes)
        for (i, thread) in self.source_threads.iter_mut().enumerate() {
            if let Some(connector) = thread.shutdown_and_join() {
                connectors.push((self.source_names[i].clone(), connector));
            }
        }

        // Shutdown core threads
        for core in &self.cores {
            core.shutdown();
        }

        connectors
    }

    /// Returns the number of cores.
    #[must_use]
    pub fn num_cores(&self) -> usize {
        self.cores.len()
    }

    /// Returns the source names.
    #[must_use]
    pub fn source_names(&self) -> &[String] {
        &self.source_names
    }

    /// Returns the number of attached sources.
    #[must_use]
    pub fn num_sources(&self) -> usize {
        self.source_threads.len()
    }
}

impl std::fmt::Debug for TpcRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TpcRuntime")
            .field("num_cores", &self.cores.len())
            .field("num_sources", &self.source_threads.len())
            .field("source_names", &self.source_names)
            .field("routing", &self.routing)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laminar_core::tpc::TpcConfig;

    #[test]
    fn test_tpc_runtime_creation() {
        let config = TpcConfig {
            num_cores: 2,
            cpu_pinning: false,
            ..Default::default()
        };
        let runtime = TpcRuntime::new(config).unwrap();
        assert_eq!(runtime.num_cores(), 2);
        assert_eq!(runtime.num_sources(), 0);
    }

    #[test]
    fn test_tpc_runtime_invalid_config() {
        let config = TpcConfig {
            num_cores: 0,
            ..Default::default()
        };
        assert!(TpcRuntime::new(config).is_err());
    }

    #[test]
    fn test_tpc_runtime_poll_empty() {
        let config = TpcConfig {
            num_cores: 1,
            cpu_pinning: false,
            ..Default::default()
        };
        let runtime = TpcRuntime::new(config).unwrap();
        let mut buffer = Vec::new();
        let count = runtime.poll_all_outputs(&mut buffer);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_tpc_runtime_debug() {
        let config = TpcConfig {
            num_cores: 1,
            cpu_pinning: false,
            ..Default::default()
        };
        let runtime = TpcRuntime::new(config).unwrap();
        let debug = format!("{runtime:?}");
        assert!(debug.contains("TpcRuntime"));
        assert!(debug.contains("num_cores"));
    }
}
