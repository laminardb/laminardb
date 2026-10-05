use serde::{Deserialize, Serialize};

use super::{PeerFrontiers, ProcessShuffle};
use crate::error::DbError;
use crate::operator_graph::InputFrontier;

#[derive(Clone, Deserialize, Serialize)]
pub(in super::super::super::super) struct Checkpoint {
    version: u8,
    assignment_version: u64,
    assignment_digest: [u8; 32],
    self_id: u64,
    local: SavedFrontier,
    effective: SavedFrontier,
    peers: Vec<(u64, SavedFrontier)>,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
struct SavedFrontier {
    watermark: Option<i64>,
    idle: bool,
}

impl From<InputFrontier> for SavedFrontier {
    fn from(frontier: InputFrontier) -> Self {
        Self {
            watermark: frontier.watermark,
            idle: frontier.idle,
        }
    }
}

impl From<SavedFrontier> for InputFrontier {
    fn from(frontier: SavedFrontier) -> Self {
        Self {
            watermark: frontier.watermark,
            idle: frontier.idle,
        }
    }
}

impl Checkpoint {
    pub(in super::super::super::super) fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(
            self.peers
                .capacity()
                .saturating_mul(std::mem::size_of::<(u64, SavedFrontier)>()),
        )
    }

    pub(in super::super::super::super) fn validate(
        &self,
        vnode_count: u32,
        watermark_us: i64,
    ) -> Result<(), DbError> {
        let effective = InputFrontier::from(self.effective);
        if self.version != 1
            || self.assignment_version == 0
            || self.peers.is_empty()
            || self.peers.len() >= vnode_count as usize
            || self.peers.windows(2).any(|pair| pair[0].0 >= pair[1].0)
            || self.peers.iter().any(|(peer, _)| *peer == self.self_id)
            || watermark_us
                != effective
                    .watermark
                    .map_or(i64::MIN, |ms| ms.saturating_mul(1_000))
        {
            return Err(DbError::Checkpoint(
                "process shuffle checkpoint identity or frontier is invalid".into(),
            ));
        }
        for frontier in std::iter::once(self.local)
            .chain(std::iter::once(self.effective))
            .chain(self.peers.iter().map(|(_, frontier)| *frontier))
        {
            super::input::validate_frontier(InputFrontier::default(), frontier.into()).map_err(
                |error| DbError::Checkpoint(format!("process shuffle checkpoint: {error}")),
            )?;
        }
        let merged = crate::operator_graph::merge_input_frontier_iter(
            std::iter::once(self.local.into())
                .chain(self.peers.iter().map(|(_, frontier)| (*frontier).into())),
            i64::MIN,
        );
        if merged != effective {
            return Err(DbError::Checkpoint(
                "process shuffle checkpoint contains an inconsistent effective frontier".into(),
            ));
        }
        Ok(())
    }
}

impl ProcessShuffle {
    pub(super) fn checkpoint(&self) -> Checkpoint {
        debug_assert!(
            !self.pending(),
            "ordered process input must drain before capture"
        );
        Checkpoint {
            version: 1,
            assignment_version: self.assignment_version,
            assignment_digest: self.assignment_digest,
            self_id: self.self_id,
            local: self.local.into(),
            effective: self.effective.into(),
            peers: self
                .peers
                .iter()
                .map(|(&peer, channel)| (peer, channel.applied.into()))
                .collect(),
        }
    }

    pub(super) fn restore_frontiers(&mut self, checkpoint: &Checkpoint) -> Result<(), DbError> {
        if checkpoint.assignment_version != self.assignment_version
            || checkpoint.assignment_digest != self.assignment_digest
            || checkpoint.self_id != self.self_id
            || !self
                .peers
                .keys()
                .copied()
                .eq(checkpoint.peers.iter().map(|(peer, _)| *peer))
        {
            return Err(DbError::Checkpoint(
                "process shuffle restore requires its exact assignment and local peer roster"
                    .into(),
            ));
        }
        self.local = checkpoint.local.into();
        self.effective = checkpoint.effective.into();
        for (peer, frontier) in &checkpoint.peers {
            let channel = self.peers.get_mut(peer).ok_or_else(|| {
                DbError::Checkpoint("process shuffle restore names an unknown peer".into())
            })?;
            *channel = PeerFrontiers {
                applied: (*frontier).into(),
                accepted: (*frontier).into(),
                queued: 0,
            };
        }
        Ok(())
    }
}
