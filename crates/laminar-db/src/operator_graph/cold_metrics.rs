//! Graph metrics sampled every `STATS_SAMPLE_INTERVAL` cycles: input buffer bytes, managed-state
//! charge and budget ratio, and how each operator's state spreads over this node's vnodes.

use super::{ManagedStateAccountingSnapshot, OperatorGraph, STATS_SAMPLE_INTERVAL};

/// How one operator's accounted state spreads over the vnodes this node holds state for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct VnodeLoad {
    pub(crate) max_bytes: usize,
    pub(crate) mean_bytes: usize,
    pub(crate) max_groups: usize,
    /// The vnode holding `max_bytes`, lowest id on ties.
    pub(crate) hottest_vnode: u32,
}

impl VnodeLoad {
    /// Summarise `(vnode, accounted bytes, groups)` triples; no vnodes gives all zeros.
    pub(crate) fn from_vnodes(vnodes: impl IntoIterator<Item = (u32, usize, usize)>) -> Self {
        let mut load = Self::default();
        let mut count = 0_usize;
        let mut total = 0_usize;
        for (vnode, bytes, groups) in vnodes {
            if count == 0
                || bytes > load.max_bytes
                || (bytes == load.max_bytes && vnode < load.hottest_vnode)
            {
                load.max_bytes = bytes;
                load.hottest_vnode = vnode;
            }
            load.max_groups = load.max_groups.max(groups);
            total = total.saturating_add(bytes);
            count += 1;
        }
        load.mean_bytes = total.checked_div(count).unwrap_or(0);
        load
    }
}

fn gauge_value(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

impl OperatorGraph {
    pub(super) fn sample_buffer_stats(&mut self) {
        self.stats_tick = self.stats_tick.wrapping_add(1);
        if !self.stats_tick.is_multiple_of(STATS_SAMPLE_INTERVAL) {
            return;
        }
        self.publish_buffer_stats();
    }

    pub(super) fn publish_buffer_stats(&mut self) {
        let Some(prom) = self.prom.as_ref() else {
            return;
        };
        for (id, ports) in self.input_buf_bytes.iter().enumerate() {
            if self.nodes[id].removed {
                continue;
            }
            let total: usize = ports.iter().sum();
            prom.input_buf_bytes
                .with_label_values(&[&self.nodes[id].name])
                .set(gauge_value(total));
        }
        let flow = &prom.flow;
        let mut accounted_bytes = 0_usize;
        for (id, node) in self.nodes.iter().enumerate() {
            let name: &str = &node.name;
            if node.removed {
                for phase in ["live", "prepared", "retired"] {
                    let _ = prom
                        .managed_state_accounted_bytes
                        .remove_label_values(&[name, phase]);
                }
                for gauge in [
                    &flow.vnode_state_bytes_max,
                    &flow.vnode_state_bytes_mean,
                    &flow.vnode_groups_max,
                    &flow.vnode_hottest,
                ] {
                    let _ = gauge.remove_label_values(&[name]);
                }
                self.managed_state_accounting_peaks[id] = ManagedStateAccountingSnapshot::default();
                continue;
            }
            if let Some(load) = node.operator.vnode_load() {
                flow.vnode_state_bytes_max
                    .with_label_values(&[name])
                    .set(gauge_value(load.max_bytes));
                flow.vnode_state_bytes_mean
                    .with_label_values(&[name])
                    .set(gauge_value(load.mean_bytes));
                flow.vnode_groups_max
                    .with_label_values(&[name])
                    .set(gauge_value(load.max_groups));
                flow.vnode_hottest
                    .with_label_values(&[name])
                    .set(i64::from(load.hottest_vnode));
            }
            let Some(mut accounting) = node.operator.managed_state_accounting() else {
                continue;
            };
            accounted_bytes = accounted_bytes.saturating_add(accounting.total_bytes());
            let peaks = std::mem::take(&mut self.managed_state_accounting_peaks[id]);
            accounting.prepared = accounting.prepared.max(peaks.prepared);
            accounting.retired = accounting.retired.max(peaks.retired);
            for (phase, bytes) in [
                ("live", accounting.live),
                ("prepared", accounting.prepared),
                ("retired", accounting.retired),
            ] {
                prom.managed_state_accounted_bytes
                    .with_label_values(&[name, phase])
                    .set(gauge_value(bytes));
            }
        }
        // Gauge precision is enough for a pressure ratio.
        #[allow(clippy::cast_precision_loss)]
        let ratio = accounted_bytes as f64 / self.max_managed_state_bytes as f64;
        flow.managed_state_budget_ratio.set(ratio);
    }

    /// Retain transient managed-state ownership until the next cold metrics sample.
    ///
    /// This performs no Prometheus work and is called only from vnode lifecycle paths.
    #[cfg(feature = "cluster")]
    pub(super) fn observe_managed_state_accounting(&mut self, node_indices: &[usize]) {
        for &node_idx in node_indices {
            let node = &self.nodes[node_idx];
            if node.removed {
                continue;
            }
            if let Some(accounting) = node.operator.managed_state_accounting() {
                self.managed_state_accounting_peaks[node_idx].observe_transient(accounting);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::VnodeLoad;

    #[test]
    fn vnode_load_reports_the_largest_vnode_lowest_id_first() {
        assert_eq!(VnodeLoad::from_vnodes([]), VnodeLoad::default());
        let load = VnodeLoad::from_vnodes([(9, 40, 1), (3, 40, 7), (5, 10, 2)]);
        assert_eq!(
            load,
            VnodeLoad {
                max_bytes: 40,
                mean_bytes: 30,
                max_groups: 7,
                hottest_vnode: 3,
            }
        );
    }
}
