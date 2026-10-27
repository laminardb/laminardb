//! An aggregate's emitted rows at a migration cut, which seed a view the migration adds over it.

use arrow::array::RecordBatch;

use super::{build_weighted_batch, DbError, IncrementalAggState};

impl IncrementalAggState {
    /// The rows a changelog aggregate has emitted, one `+1` row per group. HAVING never applies:
    /// a changelog aggregate rejects it.
    pub(crate) fn emitted_changelog(&self) -> Result<Vec<RecordBatch>, DbError> {
        let (keys, values): (Vec<_>, Vec<_>) = self
            .vnode_states
            .iter()
            .flat_map(|(_, state)| state.last_emitted.iter())
            .map(|(key, values)| (key.clone(), values.clone()))
            .unzip();
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let batch = build_weighted_batch(
            &keys,
            &values,
            &vec![1; keys.len()],
            &self.row_converter,
            self.num_group_cols,
            &self.agg_specs,
            &self.output_schema,
        )?;
        Ok(vec![batch])
    }
}
