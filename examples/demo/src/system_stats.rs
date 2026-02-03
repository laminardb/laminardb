//! System resource statistics via sysinfo (single-PID refresh).

use std::collections::VecDeque;

use sysinfo::{Pid, ProcessesToUpdate, System};

use crate::types::SystemStats;

const CPU_HISTORY_LEN: usize = 60;

/// Collects CPU and memory stats for the current process.
pub struct StatsCollector {
    system: System,
    pid: Pid,
    num_cpus: f32,
}

impl Default for StatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl StatsCollector {
    pub fn new() -> Self {
        let pid = Pid::from_u32(std::process::id());
        let mut system = System::new();
        // Initial refresh to establish baseline CPU measurement
        system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        system.refresh_memory();
        // sysinfo's cpu_usage() returns 0..num_cpus*100, so we normalize
        let num_cpus = std::thread::available_parallelism()
            .map(|n| n.get() as f32)
            .unwrap_or(1.0);
        Self {
            system,
            pid,
            num_cpus,
        }
    }

    /// Refresh and return current stats. Call once per render cycle (~200ms).
    pub fn refresh(&mut self) -> SystemStats {
        self.system
            .refresh_processes(ProcessesToUpdate::Some(&[self.pid]), true);
        self.system.refresh_memory();

        let (cpu_usage, memory_mb) = self
            .system
            .process(self.pid)
            .map(|p| {
                // Normalize: sysinfo returns 0..num_cpus*100, scale to 0..100
                let cpu = p.cpu_usage() / self.num_cpus;
                let mem = p.memory() as f64 / (1024.0 * 1024.0);
                (cpu, mem)
            })
            .unwrap_or((0.0, 0.0));

        let total_memory_mb = self.system.total_memory() as f64 / (1024.0 * 1024.0);

        SystemStats {
            cpu_usage,
            memory_mb,
            total_memory_mb,
            cpu_history: VecDeque::with_capacity(CPU_HISTORY_LEN),
        }
    }
}
