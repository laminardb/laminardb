//! Per-node memory budgets and the container memory limit they should fit under.
//!
//! The budgets fail closed independently; none of them bounds process memory. When they add up
//! to more than the cgroup allows, the kernel can kill the process before any budget is reached,
//! so startup says so. Per-view, per-table and per-source limits scale with the catalog and are
//! not counted.

use std::path::Path;

use crate::config::{LaminarConfig, DEFAULT_MAX_MANAGED_STATE_BYTES};
use crate::db::RuntimeMode;

/// cgroup v1 reports no limit as `i64::MAX` rounded down to a page.
const CGROUP_V1_UNLIMITED: u64 = 1 << 62;

/// Each per-node byte budget the configuration and mode imply, labelled by area.
pub(crate) fn node_budgets(config: &LaminarConfig, mode: RuntimeMode) -> Vec<(&'static str, u64)> {
    let managed_state = config
        .pipeline_max_managed_state_bytes
        .unwrap_or(DEFAULT_MAX_MANAGED_STATE_BYTES);
    let mut budgets = vec![
        ("managed_state", bytes(managed_state)),
        ("datafusion", bytes(config.datafusion_memory_limit_bytes)),
        ("source_queue", bytes(config.source_queue_max_bytes)),
        (
            "view_read_cache",
            bytes(config.materialized_view_read_cache_bytes),
        ),
    ];
    if let Some(capture) = config
        .checkpoint
        .as_ref()
        .and_then(|c| c.max_node_data_bytes)
    {
        budgets.push(("checkpoint_capture", capture));
    }
    match mode {
        RuntimeMode::Local => budgets.push((
            "subscription_logs",
            bytes(crate::subscription::PROCESS_SUBSCRIPTION_BYTES),
        )),
        #[cfg(feature = "cluster")]
        RuntimeMode::Cluster => budgets.extend([
            (
                "subscription_output",
                bytes(crate::subscription::cluster::MAX_PENDING_OUTPUT_BYTES),
            ),
            (
                "shuffle_outbound",
                bytes(laminar_core::shuffle::SHUFFLE_NODE_BUDGET_BYTES),
            ),
            (
                "shuffle_inbound",
                bytes(laminar_core::shuffle::SHUFFLE_NODE_BUDGET_BYTES),
            ),
        ]),
        #[cfg(not(feature = "cluster"))]
        RuntimeMode::Cluster => {}
    }
    budgets
}

fn bytes(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// The memory limit of this process's cgroup, read now. `None` outside a cgroup with a memory
/// limit, including on platforms without cgroups.
pub(crate) fn cgroup_memory_max_bytes() -> Option<u64> {
    let membership = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    cgroup_memory_limit(&membership, Path::new("/sys/fs/cgroup"))
}

/// The lowest limit from the process's cgroup up to the hierarchy's mount, since a parent's
/// limit also applies. A v1 memory controller takes precedence over the v2 unified hierarchy,
/// because on hybrid hosts the v1 controller is the one enforcing. A path that does not exist
/// under the mount (a v1 container sees its own cgroup at the root) falls through to the root.
fn cgroup_memory_limit(membership: &str, root: &Path) -> Option<u64> {
    let v1 = membership.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        let (_, controllers, path) = (fields.next()?, fields.next()?, fields.next()?);
        controllers
            .split(',')
            .any(|controller| controller == "memory")
            .then(|| (root.join("memory"), "memory.limit_in_bytes", path))
    });
    let v2 = || {
        membership
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .map(|path| (root.to_path_buf(), "memory.max", path))
    };
    let (mount, file, path) = v1.or_else(v2)?;
    let leaf = mount.join(path.trim_start_matches('/'));
    leaf.ancestors()
        .take_while(|dir| dir.starts_with(&mount))
        .filter_map(|dir| read_limit(&dir.join(file)))
        .min()
}

/// `None` for `max` (v2), the v1 no-limit value, or a missing file.
fn read_limit(file: &Path) -> Option<u64> {
    std::fs::read_to_string(file)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|limit| *limit < CGROUP_V1_UNLIMITED)
}

/// Warns when this database's per-node budgets add up to more than its cgroup memory limit.
pub(crate) fn warn_if_over_cgroup_limit(config: &LaminarConfig, mode: RuntimeMode) {
    let Some(limit) = cgroup_memory_max_bytes() else {
        return;
    };
    let budgets = node_budgets(config, mode);
    let Some(total) = total_over_limit(&budgets, limit) else {
        return;
    };
    let areas = budgets
        .iter()
        .map(|(area, budget)| format!("{area}={budget}"))
        .collect::<Vec<_>>()
        .join(", ");
    tracing::warn!(
        budget_bytes = total,
        cgroup_memory_max_bytes = limit,
        areas = %areas,
        "per-node memory budgets add up to more than the cgroup memory limit, so the process can \
         be killed for memory before a budget fails; per-view, per-table and per-source limits \
         come on top"
    );
}

fn total_over_limit(budgets: &[(&str, u64)], limit: u64) -> Option<u64> {
    let total = budgets
        .iter()
        .fold(0_u64, |sum, (_, budget)| sum.saturating_add(*budget));
    (total > limit).then_some(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, file: &str, contents: &str) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join(file), contents).unwrap();
    }

    #[test]
    fn v2_limit_is_the_lowest_from_the_process_cgroup_up_to_the_mount() {
        let root = tempfile::tempdir().unwrap();
        let pod = root.path().join("kubepods/pod1");
        write(&pod, "memory.max", "2147483648\n");
        write(&pod.join("app"), "memory.max", "max\n");

        let limit = cgroup_memory_limit("0::/kubepods/pod1/app\n", root.path());
        assert_eq!(limit, Some(2_147_483_648));
        assert_eq!(cgroup_memory_limit("0::/\n", root.path()), None);
        assert_eq!(cgroup_memory_limit("", root.path()), None);
    }

    #[test]
    fn v1_memory_controller_wins_and_reports_no_limit_as_none() {
        let root = tempfile::tempdir().unwrap();
        let memory = root.path().join("memory");
        write(&memory, "memory.limit_in_bytes", "9223372036854771712\n");
        write(root.path(), "memory.max", "1073741824\n");
        let membership = "12:cpu,cpuacct:/docker/abc\n4:memory:/docker/abc\n0::/\n";
        assert_eq!(cgroup_memory_limit(membership, root.path()), None);

        // A container sees its own v1 cgroup at the mount, not under the host path.
        write(&memory, "memory.limit_in_bytes", "536870912\n");
        assert_eq!(
            cgroup_memory_limit(membership, root.path()),
            Some(536_870_912)
        );
    }

    #[test]
    fn default_local_budgets_exceed_a_one_gib_limit_but_fit_two() {
        const MIB: u64 = 1024 * 1024;
        let budgets = node_budgets(&LaminarConfig::default(), RuntimeMode::Local);
        assert_eq!(total_over_limit(&budgets, 1024 * MIB), Some(1088 * MIB));
        assert_eq!(total_over_limit(&budgets, 1088 * MIB), None);
        assert_eq!(total_over_limit(&budgets, 2048 * MIB), None);
    }

    #[test]
    fn budgets_follow_the_mode_and_checkpointing() {
        let mut config = LaminarConfig::default();
        let areas = |config: &LaminarConfig, mode| {
            node_budgets(config, mode)
                .into_iter()
                .map(|(area, _)| area)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            areas(&config, RuntimeMode::Local),
            [
                "managed_state",
                "datafusion",
                "source_queue",
                "view_read_cache",
                "subscription_logs"
            ]
        );
        config.checkpoint = Some(laminar_core::streaming::StreamCheckpointConfig {
            max_node_data_bytes: Some(1024),
            ..Default::default()
        });
        assert!(node_budgets(&config, RuntimeMode::Local).contains(&("checkpoint_capture", 1024)));
        #[cfg(feature = "cluster")]
        assert_eq!(
            areas(&config, RuntimeMode::Cluster)[5..],
            ["subscription_output", "shuffle_outbound", "shuffle_inbound"]
        );
    }
}
