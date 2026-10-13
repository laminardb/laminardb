//! Placement gauges published from the assignment this process adopted.

use laminar_core::state::{owners_per_domain, Locality, NodeId, VnodeRegistry};

use crate::engine_metrics::PlacementMetrics;

/// Publish this process's owned-vnode count and the per-domain owner counts. Resets the domain
/// gauge so disappeared domains don't leave stale series.
pub(super) fn publish(
    metrics: &PlacementMetrics,
    registry: &VnodeRegistry,
    me: NodeId,
    nodes: &[(NodeId, Locality)],
    isolation_tier: usize,
) {
    let owners = registry.snapshot();
    let total = u32::try_from(owners.len().max(1)).unwrap_or(u32::MAX);
    let owned = owners.iter().filter(|owner| **owner == me).count();
    metrics
        .owned_vnodes
        .set(i64::try_from(owned).unwrap_or(i64::MAX));

    let counts = owners_per_domain(&owners, nodes, isolation_tier);
    metrics.vnodes_per_domain.reset();
    let mut max = 0u32;
    for (domain, &count) in &counts {
        let label = if domain.is_empty() {
            "unknown"
        } else {
            domain.as_str()
        };
        metrics
            .vnodes_per_domain
            .with_label_values(&[label])
            .set(i64::from(count));
        max = max.max(count);
    }
    metrics
        .blast_radius_ratio
        .set(f64::from(max) / f64::from(total));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publishes_owned_vnodes_and_domain_counts() {
        let prom = prometheus::Registry::new();
        let metrics = crate::engine_metrics::EngineMetrics::new(&prom);

        // 4 vnodes: node 1 owns two, node 2 owns one, one is unassigned.
        let vreg = VnodeRegistry::new(4);
        vreg.set_assignment(vec![NodeId(1), NodeId(1), NodeId(2), NodeId::UNASSIGNED].into());
        let nodes = vec![
            (NodeId(1), Locality::parse("region=r;zone=z1")),
            (NodeId(2), Locality::parse("region=r;zone=z2")),
        ];

        publish(&metrics.placement, &vreg, NodeId(2), &nodes, 1); // isolation_tier 1 = zone

        let placement = &metrics.placement;
        assert_eq!(placement.owned_vnodes.get(), 1);
        let domains = &placement.vnodes_per_domain;
        assert_eq!(domains.with_label_values(&["r;z1"]).get(), 2);
        assert_eq!(domains.with_label_values(&["r;z2"]).get(), 1);
        assert_eq!(domains.with_label_values(&["unknown"]).get(), 1); // the unassigned vnode
                                                                      // Blast radius = largest domain (2) / total vnodes (4).
        assert!((placement.blast_radius_ratio.get() - 0.5).abs() < 1e-9);
    }
}
