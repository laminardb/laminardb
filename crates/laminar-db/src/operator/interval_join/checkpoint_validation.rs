use super::{
    join_type_tag, ArchivedIntervalJoinOperatorCheckpoint, DbError, IntervalHandoffCut,
    IntervalJoinOperator, IntervalJoinOperatorCheckpoint, IntervalJoinVnodeState, StreamJoinConfig,
    OPERATOR_CHECKPOINT_VERSION,
};

impl IntervalJoinOperator {
    pub(super) fn validate_handoff_cutoffs(
        left_evicted_cutoff: i64,
        right_evicted_cutoff: i64,
        left_nonempty: bool,
        right_nonempty: bool,
        config: &StreamJoinConfig,
        context: &str,
        cut: IntervalHandoffCut,
    ) -> Result<(), DbError> {
        let bound_ms = i64::try_from(config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(format!(
                "{context}: configured time bound exceeds the supported millisecond range"
            ))
        })?;
        let expected_left_cutoff = cut.right_watermark.saturating_sub(bound_ms);
        let expected_right_cutoff = cut.left_watermark;
        if left_evicted_cutoff > expected_left_cutoff
            || right_evicted_cutoff > expected_right_cutoff
            || (left_nonempty && left_evicted_cutoff != expected_left_cutoff)
            || (right_nonempty && right_evicted_cutoff != expected_right_cutoff)
        {
            return Err(DbError::Checkpoint(format!(
                "{context}: vnode eviction state is inconsistent with the portable handoff cut"
            )));
        }
        Ok(())
    }

    pub(super) fn validate_ordered_core_cutoffs(
        state: &IntervalJoinVnodeState,
        config: &StreamJoinConfig,
        context: &str,
        cut: IntervalHandoffCut,
    ) -> Result<(), DbError> {
        let bound_ms = i64::try_from(config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(format!(
                "{context}: configured time bound exceeds the supported millisecond range"
            ))
        })?;
        let expected = (
            cut.right_watermark.saturating_sub(bound_ms),
            cut.left_watermark,
        );
        if state.evicted_cutoffs() != expected {
            return Err(DbError::Checkpoint(format!(
                "{context}: weighted core cutoffs disagree with authoritative normalizer cutoffs"
            )));
        }
        Ok(())
    }

    pub(super) fn validate_checkpoint_config(
        &self,
        checkpoint: &IntervalJoinOperatorCheckpoint,
    ) -> Result<(), DbError> {
        let bound_ms = i64::try_from(self.config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(format!(
                "interval join [{}] configured time bound exceeds the supported millisecond range",
                self.projection.op_name
            ))
        })?;
        if checkpoint.version != OPERATOR_CHECKPOINT_VERSION
            || checkpoint.ordered_input_fingerprints
                != self
                    .ordered_input_spec
                    .as_ref()
                    .map(|spec| [spec.left.fingerprint, spec.right.fingerprint])
            || checkpoint.join_type != join_type_tag(self.config.join_type)
            || checkpoint.left_keys != self.config.left_keys
            || checkpoint.right_keys != self.config.right_keys
            || checkpoint.left_time_column != self.config.left_time_column
            || checkpoint.right_time_column != self.config.right_time_column
            || checkpoint.left_table != self.config.left_table
            || checkpoint.right_table != self.config.right_table
            || checkpoint.bound_ms != bound_ms
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint version or configuration does not match the operator",
                self.projection.op_name
            )));
        }
        Ok(())
    }

    pub(super) fn validate_archived_checkpoint_config(
        &self,
        checkpoint: &ArchivedIntervalJoinOperatorCheckpoint,
    ) -> Result<(), DbError> {
        let bound_ms = i64::try_from(self.config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(format!(
                "interval join [{}] configured bound is not checkpointable",
                self.projection.op_name
            ))
        })?;
        let expected_fingerprints = self
            .ordered_input_spec
            .as_ref()
            .map(|spec| [spec.left.fingerprint, spec.right.fingerprint]);
        let fingerprints_match = match (
            checkpoint.ordered_input_fingerprints.as_ref(),
            expected_fingerprints.as_ref(),
        ) {
            (None, None) => true,
            (Some(archived), Some(expected)) => archived.as_slice() == expected.as_slice(),
            _ => false,
        };
        let left_keys_match = checkpoint.left_keys.len() == self.config.left_keys.len()
            && checkpoint
                .left_keys
                .iter()
                .zip(&self.config.left_keys)
                .all(|(archived, expected)| archived.as_str() == expected.as_str());
        let right_keys_match = checkpoint.right_keys.len() == self.config.right_keys.len()
            && checkpoint
                .right_keys
                .iter()
                .zip(&self.config.right_keys)
                .all(|(archived, expected)| archived.as_str() == expected.as_str());
        if checkpoint.version != OPERATOR_CHECKPOINT_VERSION
            || !fingerprints_match
            || checkpoint.join_type != join_type_tag(self.config.join_type)
            || !left_keys_match
            || !right_keys_match
            || checkpoint.left_time_column.as_str() != self.config.left_time_column.as_str()
            || checkpoint.right_time_column.as_str() != self.config.right_time_column.as_str()
            || checkpoint.left_table.as_str() != self.config.left_table.as_str()
            || checkpoint.right_table.as_str() != self.config.right_table.as_str()
            || checkpoint.bound_ms != bound_ms
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] archived checkpoint version or configuration does not match the operator",
                self.projection.op_name
            )));
        }
        Ok(())
    }
}
