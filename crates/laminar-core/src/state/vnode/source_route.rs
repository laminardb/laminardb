//! Route from a split source's input channel to the vnode whose owner reads it.

use std::num::NonZeroU32;

use serde::Serialize;

use super::key_hash;

const SOURCE_CHANNEL_ROUTE_V1_DOMAIN: &[u8] = b"laminardb.source-channel-route.v1\0";

/// Versioned route from `(source identity, channel index)` to a vnode.
///
/// A route version is part of pipeline identity: changing its function moves input channels
/// between nodes, so it needs a new version rather than an edit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceChannelRoute {
    /// Channel `i` goes to vnode `(base + i) % vnode_count`, where `base` hashes the source
    /// identity. Up to `vnode_count` channels of one source land on distinct vnodes.
    V1,
}

impl SourceChannelRoute {
    /// The vnode that owns channel `channel` of the source named `source_identity`.
    #[must_use]
    pub fn vnode(self, source_identity: &str, channel: u32, vnode_count: NonZeroU32) -> u32 {
        match self {
            Self::V1 => {
                let mut key = Vec::with_capacity(
                    SOURCE_CHANNEL_ROUTE_V1_DOMAIN.len() + 8 + source_identity.len(),
                );
                key.extend_from_slice(SOURCE_CHANNEL_ROUTE_V1_DOMAIN);
                key.extend_from_slice(&(source_identity.len() as u64).to_le_bytes());
                key.extend_from_slice(source_identity.as_bytes());
                let count = u64::from(vnode_count.get());
                let vnode = (key_hash(&key) % count + u64::from(channel) % count) % count;
                // The remainder of a division by a u32 count fits in u32.
                #[allow(clippy::cast_possible_truncation)]
                let vnode = vnode as u32;
                vnode
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(vnodes: u32) -> NonZeroU32 {
        NonZeroU32::new(vnodes).unwrap()
    }

    #[test]
    fn v1_is_pinned() {
        let route = SourceChannelRoute::V1;
        let vnodes: Vec<u32> = (0..4).map(|c| route.vnode("gen", c, count(12))).collect();
        assert_eq!(vnodes, PINNED_GEN_12);
        assert_eq!(route.vnode("orders", 0, count(256)), PINNED_ORDERS_256);
    }

    #[test]
    fn v1_spreads_channels_over_distinct_vnodes() {
        let route = SourceChannelRoute::V1;
        for vnodes in [1, 3, 12, 256] {
            let mut seen: Vec<u32> = (0..vnodes)
                .map(|c| route.vnode("events", c, count(vnodes)))
                .collect();
            assert!(seen.iter().all(|vnode| *vnode < vnodes));
            seen.sort_unstable();
            seen.dedup();
            assert_eq!(seen.len(), vnodes as usize);
        }
        assert_eq!(
            route.vnode("events", 5, count(12)),
            route.vnode("events", 17, count(12))
        );
    }

    #[test]
    fn v1_separates_sources_by_identity() {
        let route = SourceChannelRoute::V1;
        let bases: std::collections::BTreeSet<u32> = ["a", "b", "c", "d", "e", "f"]
            .into_iter()
            .map(|source| route.vnode(source, 0, count(256)))
            .collect();
        assert!(bases.len() > 1);
    }

    const PINNED_GEN_12: [u32; 4] = [1, 2, 3, 4];
    const PINNED_ORDERS_256: u32 = 235;
}
