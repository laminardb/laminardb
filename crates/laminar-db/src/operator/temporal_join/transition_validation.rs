use super::{DbError, ManagedTemporalJoinOperator};

impl ManagedTemporalJoinOperator {
    pub(super) fn validate_vnode_roster(
        &self,
        required_vnodes: &[u32],
        vnode_count: u32,
    ) -> Result<(), DbError> {
        if vnode_count != u32::from(self.key_group_count)
            || required_vnodes.windows(2).any(|pair| pair[0] >= pair[1])
            || required_vnodes.iter().any(|vnode| *vnode >= vnode_count)
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] received a non-canonical vnode roster {required_vnodes:?} for vnode_count {vnode_count}",
                self.name
            )));
        }
        if let Some(unowned) = self
            .resident_vnodes
            .iter()
            .copied()
            .find(|vnode| required_vnodes.binary_search(vnode).is_err())
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] retained unowned vnode state {unowned}",
                self.name
            )));
        }
        Ok(())
    }
}
