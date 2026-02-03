//! # NUMA Topology Detection
//!
//! Detects system NUMA topology using:
//! 1. hwlocality crate (if `hwloc` feature enabled)
//! 2. sysfs parsing on Linux (fallback)
//! 3. Single-node fallback on other platforms

#[cfg(feature = "hwloc")]
use super::NumaError;

/// NUMA topology information for the system.
///
/// Provides information about NUMA nodes, which CPUs belong to each node,
/// and memory available per node.
#[derive(Debug, Clone)]
pub struct NumaTopology {
    /// Number of NUMA nodes
    num_nodes: usize,
    /// CPUs per node (index = node ID)
    cpus_per_node: Vec<Vec<usize>>,
    /// Memory per node in bytes (index = node ID)
    memory_per_node: Vec<u64>,
    /// Total number of CPUs
    num_cpus: usize,
    /// CPU to NUMA node mapping
    cpu_to_node: Vec<usize>,
}

impl NumaTopology {
    /// Detect the system's NUMA topology.
    ///
    /// Uses the best available method for the platform:
    /// - Linux: Reads from sysfs (`/sys/devices/system/node/`)
    /// - Other: Returns a single-node topology
    ///
    /// This method never fails - it falls back to a single-node topology
    /// if detection fails.
    #[must_use]
    pub fn detect() -> Self {
        // Try hwlocality first if available
        #[cfg(feature = "hwloc")]
        {
            if let Ok(topo) = Self::detect_hwlocality() {
                return topo;
            }
        }

        // Try sysfs on Linux
        #[cfg(target_os = "linux")]
        {
            if let Ok(topo) = Self::detect_sysfs() {
                return topo;
            }
        }

        // Fallback to single-node topology
        Self::single_node_fallback()
    }

    /// Detect topology using hwlocality crate.
    #[cfg(feature = "hwloc")]
    fn detect_hwlocality() -> Result<Self, NumaError> {
        use hwlocality::Topology;

        let hwloc_topo = Topology::new()
            .map_err(|e| NumaError::TopologyError(format!("hwlocality init failed: {e}")))?;

        let numa_nodes: Vec<_> = hwloc_topo
            .objects_with_type(hwlocality::object::types::ObjectType::NUMANode)
            .collect();

        if numa_nodes.is_empty() {
            return Err(NumaError::TopologyError(
                "No NUMA nodes found via hwlocality".to_string(),
            ));
        }

        let num_nodes = numa_nodes.len();
        let mut cpus_per_node = vec![Vec::new(); num_nodes];
        let mut memory_per_node = vec![0u64; num_nodes];

        // Get total CPUs
        let num_cpus = hwloc_topo
            .objects_with_type(hwlocality::object::types::ObjectType::PU)
            .count();

        let mut cpu_to_node = vec![0usize; num_cpus];

        for (node_idx, numa_node) in numa_nodes.iter().enumerate() {
            // Get memory for this node
            if let Some(memory) = numa_node.total_memory() {
                memory_per_node[node_idx] = memory;
            }

            // Get CPUs for this node
            if let Some(cpuset) = numa_node.cpuset() {
                for cpu in cpuset.iter_set() {
                    let cpu_idx = cpu as usize;
                    if cpu_idx < num_cpus {
                        cpus_per_node[node_idx].push(cpu_idx);
                        cpu_to_node[cpu_idx] = node_idx;
                    }
                }
            }
        }

        Ok(Self {
            num_nodes,
            cpus_per_node,
            memory_per_node,
            num_cpus,
            cpu_to_node,
        })
    }

    /// Detect topology from sysfs on Linux.
    #[cfg(target_os = "linux")]
    fn detect_sysfs() -> Result<Self, NumaError> {
        use std::fs;
        use std::path::Path;

        let node_path = Path::new("/sys/devices/system/node");
        if !node_path.exists() {
            return Err(NumaError::TopologyError(
                "sysfs node path not found".to_string(),
            ));
        }

        // Count NUMA nodes
        let mut node_dirs: Vec<usize> = Vec::new();
        for entry in fs::read_dir(node_path)
            .map_err(|e| NumaError::TopologyError(format!("Failed to read node dir: {e}")))?
        {
            let entry = entry
                .map_err(|e| NumaError::TopologyError(format!("Failed to read entry: {e}")))?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with("node") {
                if let Ok(node_id) = name_str[4..].parse::<usize>() {
                    node_dirs.push(node_id);
                }
            }
        }

        if node_dirs.is_empty() {
            return Err(NumaError::TopologyError("No NUMA nodes found".to_string()));
        }

        node_dirs.sort_unstable();
        let num_nodes = node_dirs.iter().max().map_or(1, |m| m + 1);

        // Get CPU count
        let num_cpus = Self::get_cpu_count()?;

        let mut cpus_per_node = vec![Vec::new(); num_nodes];
        let mut memory_per_node = vec![0u64; num_nodes];
        let mut cpu_to_node = vec![0usize; num_cpus];

        for node_id in &node_dirs {
            let node_dir = node_path.join(format!("node{node_id}"));

            // Parse cpulist
            let cpulist_path = node_dir.join("cpulist");
            if let Ok(cpulist) = fs::read_to_string(&cpulist_path) {
                let cpus = Self::parse_cpulist(cpulist.trim());
                for cpu in &cpus {
                    if *cpu < num_cpus {
                        cpu_to_node[*cpu] = *node_id;
                    }
                }
                cpus_per_node[*node_id] = cpus;
            }

            // Parse meminfo
            let meminfo_path = node_dir.join("meminfo");
            if let Ok(meminfo) = fs::read_to_string(&meminfo_path) {
                memory_per_node[*node_id] = Self::parse_meminfo(&meminfo);
            }
        }

        Ok(Self {
            num_nodes,
            cpus_per_node,
            memory_per_node,
            num_cpus,
            cpu_to_node,
        })
    }

    /// Get CPU count from sysfs.
    #[cfg(target_os = "linux")]
    fn get_cpu_count() -> Result<usize, NumaError> {
        use std::fs;

        // Try reading from /sys/devices/system/cpu/online
        let online_path = "/sys/devices/system/cpu/online";
        if let Ok(online) = fs::read_to_string(online_path) {
            let cpus = Self::parse_cpulist(online.trim());
            if let Some(max) = cpus.iter().max() {
                return Ok(max + 1);
            }
        }

        // Fallback to num_cpus crate
        Ok(num_cpus::get())
    }

    /// Parse a CPU list string like "0-7,16-23".
    #[cfg(target_os = "linux")]
    fn parse_cpulist(s: &str) -> Vec<usize> {
        let mut cpus = Vec::new();

        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            if let Some((start, end)) = part.split_once('-') {
                if let (Ok(start), Ok(end)) = (start.parse::<usize>(), end.parse::<usize>()) {
                    cpus.extend(start..=end);
                }
            } else if let Ok(cpu) = part.parse::<usize>() {
                cpus.push(cpu);
            }
        }

        cpus
    }

    /// Parse meminfo file for total memory.
    #[cfg(target_os = "linux")]
    fn parse_meminfo(s: &str) -> u64 {
        for line in s.lines() {
            // Looking for "Node X MemTotal: NNNN kB"
            if line.contains("MemTotal") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                // Find the numeric value
                for (i, part) in parts.iter().enumerate() {
                    if let Ok(val) = part.parse::<u64>() {
                        // Check if next part is "kB"
                        if parts.get(i + 1).map_or(false, |&u| u == "kB") {
                            return val * 1024;
                        }
                        return val;
                    }
                }
            }
        }
        0
    }

    /// Create a single-node fallback topology.
    fn single_node_fallback() -> Self {
        let num_cpus = num_cpus::get();
        let cpus: Vec<usize> = (0..num_cpus).collect();

        Self {
            num_nodes: 1,
            cpus_per_node: vec![cpus],
            memory_per_node: vec![Self::estimate_system_memory()],
            num_cpus,
            cpu_to_node: vec![0; num_cpus],
        }
    }

    /// Estimate total system memory.
    fn estimate_system_memory() -> u64 {
        #[cfg(target_os = "linux")]
        {
            use std::fs;
            if let Ok(meminfo) = fs::read_to_string("/proc/meminfo") {
                for line in meminfo.lines() {
                    if line.starts_with("MemTotal:") {
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if let Some(val) = parts.get(1) {
                            if let Ok(kb) = val.parse::<u64>() {
                                return kb * 1024;
                            }
                        }
                    }
                }
            }
        }

        // Fallback: estimate 16GB
        16 * 1024 * 1024 * 1024
    }

    /// Returns the number of NUMA nodes.
    #[must_use]
    pub fn num_nodes(&self) -> usize {
        self.num_nodes
    }

    /// Returns the total number of CPUs.
    #[must_use]
    pub fn num_cpus(&self) -> usize {
        self.num_cpus
    }

    /// Returns the CPUs belonging to a specific NUMA node.
    ///
    /// Returns an empty slice if the node ID is invalid.
    #[must_use]
    pub fn cpus_for_node(&self, node: usize) -> &[usize] {
        self.cpus_per_node.get(node).map_or(&[], Vec::as_slice)
    }

    /// Returns the memory (in bytes) for a specific NUMA node.
    #[must_use]
    pub fn memory_for_node(&self, node: usize) -> u64 {
        self.memory_per_node.get(node).copied().unwrap_or(0)
    }

    /// Returns the NUMA node for a given CPU.
    ///
    /// Returns 0 if the CPU ID is invalid.
    #[must_use]
    pub fn node_for_cpu(&self, cpu: usize) -> usize {
        self.cpu_to_node.get(cpu).copied().unwrap_or(0)
    }

    /// Returns the NUMA node for the current CPU.
    #[must_use]
    pub fn current_node(&self) -> usize {
        let cpu = Self::current_cpu();
        self.node_for_cpu(cpu)
    }

    /// Returns the current CPU ID.
    #[must_use]
    pub fn current_cpu() -> usize {
        #[cfg(target_os = "linux")]
        {
            // SAFETY: sched_getcpu is a simple syscall that returns the current CPU
            let cpu = unsafe { libc::sched_getcpu() };
            if cpu >= 0 {
                return cpu as usize;
            }
        }

        // Fallback: return 0
        0
    }

    /// Check if the system has multiple NUMA nodes.
    #[must_use]
    pub fn is_numa(&self) -> bool {
        self.num_nodes > 1
    }

    /// Log the detected topology for debugging.
    pub fn log_topology(&self) {
        tracing::info!(
            "NUMA Topology: {} nodes, {} CPUs",
            self.num_nodes,
            self.num_cpus
        );
        for node in 0..self.num_nodes {
            let cpus = self.cpus_for_node(node);
            let memory_gb = self.memory_for_node(node) / (1024 * 1024 * 1024);
            tracing::info!(
                "  Node {}: {} CPUs ({:?}), {} GB memory",
                node,
                cpus.len(),
                cpus,
                memory_gb
            );
        }
    }

    /// Get a summary string of the topology.
    #[must_use]
    pub fn summary(&self) -> String {
        use std::fmt::Write;

        let mut s = format!("NUMA: {} nodes, {} CPUs", self.num_nodes, self.num_cpus);
        for node in 0..self.num_nodes {
            let cpus = self.cpus_for_node(node);
            let memory_gb = self.memory_for_node(node) / (1024 * 1024 * 1024);
            let _ = write!(
                s,
                "\n  Node {}: {} CPUs, {} GB",
                node,
                cpus.len(),
                memory_gb
            );
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect() {
        let topo = NumaTopology::detect();
        assert!(topo.num_nodes() >= 1);
        assert!(topo.num_cpus() >= 1);
    }

    #[test]
    fn test_cpus_for_node() {
        let topo = NumaTopology::detect();
        let cpus = topo.cpus_for_node(0);
        assert!(!cpus.is_empty());
    }

    #[test]
    fn test_node_for_cpu() {
        let topo = NumaTopology::detect();
        let node = topo.node_for_cpu(0);
        assert!(node < topo.num_nodes());
    }

    #[test]
    fn test_current_node() {
        let topo = NumaTopology::detect();
        let node = topo.current_node();
        assert!(node < topo.num_nodes());
    }

    #[test]
    fn test_summary() {
        let topo = NumaTopology::detect();
        let summary = topo.summary();
        assert!(summary.contains("NUMA"));
        assert!(summary.contains("nodes"));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_parse_cpulist() {
        assert_eq!(NumaTopology::parse_cpulist("0"), vec![0]);
        assert_eq!(NumaTopology::parse_cpulist("0-3"), vec![0, 1, 2, 3]);
        assert_eq!(NumaTopology::parse_cpulist("0,2,4"), vec![0, 2, 4]);
        assert_eq!(
            NumaTopology::parse_cpulist("0-3,8-11"),
            vec![0, 1, 2, 3, 8, 9, 10, 11]
        );
    }

    #[test]
    fn test_single_node_fallback() {
        let topo = NumaTopology::single_node_fallback();
        assert_eq!(topo.num_nodes(), 1);
        assert!(topo.num_cpus() >= 1);
        assert_eq!(topo.node_for_cpu(0), 0);
    }
}
