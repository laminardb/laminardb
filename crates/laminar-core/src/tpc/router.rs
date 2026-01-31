//! # Key Router
//!
//! Routes events to cores based on key hash for state locality.
//!
//! ## Design
//!
//! Events are partitioned across cores by hashing their key columns. The same
//! key always routes to the same core, ensuring that all state for a key is
//! local to one core (no cross-core state access).
//!
//! ## Key Extraction
//!
//! Keys can be extracted from:
//! - Specific columns in a `RecordBatch`
//! - A single column index
//! - A custom key function
//!
//! ## Hash Function
//!
//! Uses `FxHash` for consistent, fast hashing. The hash is reduced to a core ID
//! via modulo: `core_id = hash(key) % num_cores`.

use arrow_array::{Array, RecordBatch};
use fxhash::FxHasher;
use std::hash::{Hash, Hasher};

use crate::operator::Event;

// RouterError - Static error variants for zero-allocation error paths

/// Routing errors with no heap allocation.
///
/// All error variants use static strings to avoid allocation on error paths,
/// which is critical for Ring 0 hot path performance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RouterError {
    /// A column specified by name was not found in the schema.
    #[error("column not found by name")]
    ColumnNotFoundByName,

    /// A column index is out of range for the batch.
    #[error("column index out of range")]
    ColumnIndexOutOfRange,

    /// A row index is out of range for the array.
    #[error("row index out of range")]
    RowIndexOutOfRange,

    /// The data type is not supported for key extraction.
    #[error("unsupported data type for routing")]
    UnsupportedDataType,

    /// The batch is empty and cannot be routed.
    #[error("empty batch")]
    EmptyBatch,
}

/// Specifies how to extract routing keys from events.
///
/// The key determines which core processes an event. All events with the
/// same key are guaranteed to go to the same core.
#[derive(Debug, Clone, Default)]
pub enum KeySpec {
    /// Use specific column names as the key.
    ///
    /// The columns are concatenated in order to form the key.
    Columns(Vec<String>),

    /// Use column indices as the key.
    ///
    /// Useful when column names are not known or for performance.
    ColumnIndices(Vec<usize>),

    /// Round-robin distribution (no key).
    ///
    /// Events are distributed evenly across cores without regard to content.
    /// Use this when state locality is not required.
    #[default]
    RoundRobin,

    /// Use all columns as the key.
    ///
    /// The entire row is hashed to determine routing.
    AllColumns,
}

/// Routes events to cores based on key hash.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::tpc::{KeyRouter, KeySpec};
///
/// let router = KeyRouter::new(4, KeySpec::Columns(vec!["user_id".to_string()]));
///
/// let core_id = router.route(&event);
/// // core_id is 0, 1, 2, or 3 based on user_id hash
/// ```
pub struct KeyRouter {
    /// Number of cores to route to
    num_cores: usize,
    /// Key extraction specification
    key_spec: KeySpec,
    /// Round-robin counter for `RoundRobin` mode
    round_robin_counter: std::sync::atomic::AtomicUsize,
}

impl KeyRouter {
    /// Creates a new key router.
    ///
    /// # Panics
    ///
    /// Panics if `num_cores` is 0.
    #[must_use]
    pub fn new(num_cores: usize, key_spec: KeySpec) -> Self {
        assert!(num_cores > 0, "num_cores must be > 0");
        Self {
            num_cores,
            key_spec,
            round_robin_counter: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Returns the number of cores this router distributes to.
    #[must_use]
    pub fn num_cores(&self) -> usize {
        self.num_cores
    }

    /// Returns a reference to the key specification.
    #[must_use]
    pub fn key_spec(&self) -> &KeySpec {
        &self.key_spec
    }

    /// Routes an event to a core ID.
    ///
    /// Returns the core ID (`0..num_cores`) that should process this event.
    ///
    /// # Errors
    ///
    /// Returns an error if key extraction fails (e.g., column not found).
    pub fn route(&self, event: &Event) -> Result<usize, super::TpcError> {
        match &self.key_spec {
            KeySpec::RoundRobin => {
                // Atomic increment with wrap-around
                let counter = self.round_robin_counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(counter % self.num_cores)
            }
            KeySpec::Columns(columns) => {
                self.route_by_columns(event, columns)
            }
            KeySpec::ColumnIndices(indices) => {
                self.route_by_indices(event, indices)
            }
            KeySpec::AllColumns => {
                self.route_all_columns(event)
            }
        }
    }

    /// Routes an event for a specific row to a core ID.
    ///
    /// Use this when processing batches row-by-row.
    ///
    /// # Errors
    ///
    /// Returns an error if key extraction fails.
    pub fn route_row(&self, batch: &RecordBatch, row: usize) -> Result<usize, super::TpcError> {
        match &self.key_spec {
            KeySpec::RoundRobin => {
                let counter = self.round_robin_counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(counter % self.num_cores)
            }
            KeySpec::Columns(columns) => {
                self.route_row_by_columns(batch, row, columns)
            }
            KeySpec::ColumnIndices(indices) => {
                self.route_row_by_indices(batch, row, indices)
            }
            KeySpec::AllColumns => {
                self.route_row_all_columns(batch, row)
            }
        }
    }

    /// Convert hash to core index.
    #[allow(clippy::cast_possible_truncation)]
    fn hash_to_core(&self, hash: u64) -> usize {
        // Truncation is intentional here - we're using the hash value modulo num_cores
        (hash as usize) % self.num_cores
    }

    /// Route by column names
    fn route_by_columns(&self, event: &Event, columns: &[String]) -> Result<usize, super::TpcError> {
        let batch = &event.data;
        let mut hasher = FxHasher::default();

        for col_name in columns {
            let col_idx = batch
                .schema()
                .index_of(col_name)
                .map_err(|_| RouterError::ColumnNotFoundByName)?;

            hash_column(&mut hasher, batch.column(col_idx))?;
        }

        Ok(self.hash_to_core(hasher.finish()))
    }

    /// Route by column indices
    fn route_by_indices(&self, event: &Event, indices: &[usize]) -> Result<usize, super::TpcError> {
        let batch = &event.data;
        let mut hasher = FxHasher::default();

        for &idx in indices {
            if idx >= batch.num_columns() {
                return Err(RouterError::ColumnIndexOutOfRange.into());
            }
            hash_column(&mut hasher, batch.column(idx))?;
        }

        Ok(self.hash_to_core(hasher.finish()))
    }

    /// Route using all columns
    fn route_all_columns(&self, event: &Event) -> Result<usize, super::TpcError> {
        let batch = &event.data;
        let mut hasher = FxHasher::default();

        for col in batch.columns() {
            hash_column(&mut hasher, col)?;
        }

        Ok(self.hash_to_core(hasher.finish()))
    }

    /// Route a specific row by column names
    fn route_row_by_columns(
        &self,
        batch: &RecordBatch,
        row: usize,
        columns: &[String],
    ) -> Result<usize, super::TpcError> {
        let mut hasher = FxHasher::default();

        for col_name in columns {
            let col_idx = batch
                .schema()
                .index_of(col_name)
                .map_err(|_| RouterError::ColumnNotFoundByName)?;

            hash_row_value(&mut hasher, batch.column(col_idx), row)?;
        }

        Ok(self.hash_to_core(hasher.finish()))
    }

    /// Route a specific row by column indices
    fn route_row_by_indices(
        &self,
        batch: &RecordBatch,
        row: usize,
        indices: &[usize],
    ) -> Result<usize, super::TpcError> {
        let mut hasher = FxHasher::default();

        for &idx in indices {
            if idx >= batch.num_columns() {
                return Err(RouterError::ColumnIndexOutOfRange.into());
            }
            hash_row_value(&mut hasher, batch.column(idx), row)?;
        }

        Ok(self.hash_to_core(hasher.finish()))
    }

    /// Route a specific row using all columns
    fn route_row_all_columns(
        &self,
        batch: &RecordBatch,
        row: usize,
    ) -> Result<usize, super::TpcError> {
        let mut hasher = FxHasher::default();

        for col in batch.columns() {
            hash_row_value(&mut hasher, col, row)?;
        }

        Ok(self.hash_to_core(hasher.finish()))
    }
}

/// Hash an entire column (for single-row batches)
fn hash_column(hasher: &mut FxHasher, array: &dyn Array) -> Result<(), RouterError> {
    // Hash first row for single-row batches (common case for events)
    if array.is_empty() {
        0u8.hash(hasher);
        return Ok(());
    }

    hash_row_value(hasher, array, 0)
}

/// Hash a specific row value from an array
fn hash_row_value(hasher: &mut FxHasher, array: &dyn Array, row: usize) -> Result<(), RouterError> {
    use arrow_array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow_schema::DataType;

    if row >= array.len() {
        return Err(RouterError::RowIndexOutOfRange);
    }

    if array.is_null(row) {
        // Hash a special null marker
        0xDEAD_BEEF_u64.hash(hasher);
        return Ok(());
    }

    match array.data_type() {
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.value(row).hash(hasher);
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(row).hash(hasher);
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(row).hash(hasher);
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(row).hash(hasher);
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            arr.value(row).hash(hasher);
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            arr.value(row).hash(hasher);
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.value(row).hash(hasher);
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.value(row).hash(hasher);
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            // Convert to bits for consistent hashing
            arr.value(row).to_bits().hash(hasher);
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            arr.value(row).to_bits().hash(hasher);
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            arr.value(row).hash(hasher);
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            arr.value(row).hash(hasher);
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.value(row).hash(hasher);
        }
        _ => {
            // For unsupported types, hash the string representation
            // This is slower but ensures we can route any data type
            let formatted = arrow_cast::display::array_value_to_string(array, row)
                .unwrap_or_default();
            formatted.hash(hasher);
        }
    }

    Ok(())
}

impl std::fmt::Debug for KeyRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyRouter")
            .field("num_cores", &self.num_cores)
            .field("key_spec", &self.key_spec)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use std::sync::Arc;

    fn make_event(user_id: i64, name: &str, timestamp: i64) -> Event {
        let user_ids = Arc::new(Int64Array::from(vec![user_id]));
        let names = Arc::new(StringArray::from(vec![name]));
        let batch = RecordBatch::try_from_iter(vec![
            ("user_id", user_ids as _),
            ("name", names as _),
        ]).unwrap();
        Event::new(timestamp, batch)
    }

    #[test]
    fn test_round_robin() {
        let router = KeyRouter::new(4, KeySpec::RoundRobin);

        // Should cycle through 0, 1, 2, 3, 0, 1, ...
        let event = make_event(1, "alice", 1000);

        let mut cores = Vec::new();
        for _ in 0..8 {
            cores.push(router.route(&event).unwrap());
        }

        assert_eq!(cores, vec![0, 1, 2, 3, 0, 1, 2, 3]);
    }

    #[test]
    fn test_route_by_column_name() {
        let router = KeyRouter::new(4, KeySpec::Columns(vec!["user_id".to_string()]));

        // Same user_id should always route to the same core
        let event1 = make_event(100, "alice", 1000);
        let event2 = make_event(100, "bob", 2000);  // Different name, same user_id
        let event3 = make_event(200, "charlie", 3000);  // Different user_id

        let core1 = router.route(&event1).unwrap();
        let core2 = router.route(&event2).unwrap();
        let core3 = router.route(&event3).unwrap();

        // Same user_id -> same core
        assert_eq!(core1, core2);
        // Different user_id may or may not be same core (depends on hash)
        assert!(core1 < 4 && core3 < 4);
    }

    #[test]
    fn test_route_by_column_index() {
        let router = KeyRouter::new(4, KeySpec::ColumnIndices(vec![0])); // user_id is column 0

        let event1 = make_event(100, "alice", 1000);
        let event2 = make_event(100, "bob", 2000);

        let core1 = router.route(&event1).unwrap();
        let core2 = router.route(&event2).unwrap();

        assert_eq!(core1, core2);
    }

    #[test]
    fn test_route_all_columns() {
        let router = KeyRouter::new(4, KeySpec::AllColumns);

        let event1 = make_event(100, "alice", 1000);
        let event2 = make_event(100, "alice", 2000);  // Same data, different timestamp
        let event3 = make_event(100, "bob", 3000);    // Different name

        let core1 = router.route(&event1).unwrap();
        let core2 = router.route(&event2).unwrap();
        let core3 = router.route(&event3).unwrap();

        // Same data -> same core
        assert_eq!(core1, core2);
        // Different data -> may be different core
        assert!(core1 < 4 && core3 < 4);
    }

    #[test]
    fn test_route_column_not_found() {
        let router = KeyRouter::new(4, KeySpec::Columns(vec!["nonexistent".to_string()]));
        let event = make_event(100, "alice", 1000);

        let result = router.route(&event);
        assert!(matches!(
            result,
            Err(super::super::TpcError::RouterError(RouterError::ColumnNotFoundByName))
        ));
    }

    #[test]
    fn test_route_index_out_of_range() {
        let router = KeyRouter::new(4, KeySpec::ColumnIndices(vec![10])); // Only 2 columns
        let event = make_event(100, "alice", 1000);

        let result = router.route(&event);
        assert!(matches!(
            result,
            Err(super::super::TpcError::RouterError(RouterError::ColumnIndexOutOfRange))
        ));
    }

    #[test]
    fn test_router_error_no_allocation() {
        // Verify that RouterError variants are Copy (no heap allocation)
        let err1 = RouterError::ColumnNotFoundByName;
        let err2 = err1; // Copy, not move
        assert_eq!(err1, err2);

        let err3 = RouterError::ColumnIndexOutOfRange;
        let err4 = RouterError::RowIndexOutOfRange;
        let err5 = RouterError::UnsupportedDataType;
        let err6 = RouterError::EmptyBatch;

        // All errors should be different
        assert_ne!(err1, err3);
        assert_ne!(err3, err4);
        assert_ne!(err4, err5);
        assert_ne!(err5, err6);
    }

    #[test]
    fn test_distribution() {
        // Test that keys are reasonably distributed across cores
        let router = KeyRouter::new(4, KeySpec::Columns(vec!["user_id".to_string()]));

        let mut counts = [0usize; 4];
        for user_id in 0..1000 {
            let event = make_event(user_id, "user", 1000);
            let core = router.route(&event).unwrap();
            counts[core] += 1;
        }

        // Each core should have roughly 250 events (allow 20% variance)
        for count in &counts {
            assert!(*count > 150, "Core count too low: {count}");
            assert!(*count < 350, "Core count too high: {count}");
        }
    }

    #[test]
    fn test_route_row() {
        let router = KeyRouter::new(4, KeySpec::Columns(vec!["user_id".to_string()]));

        // Create batch with multiple rows
        let user_ids = Arc::new(Int64Array::from(vec![100, 200, 100, 300]));
        let names = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
        let batch = RecordBatch::try_from_iter(vec![
            ("user_id", user_ids as _),
            ("name", names as _),
        ]).unwrap();

        // Same user_id should route to same core
        let core0 = router.route_row(&batch, 0).unwrap();
        let core2 = router.route_row(&batch, 2).unwrap();
        assert_eq!(core0, core2); // Both have user_id = 100

        // All cores should be valid
        for row in 0..4 {
            let core = router.route_row(&batch, row).unwrap();
            assert!(core < 4);
        }
    }

    #[test]
    fn test_null_handling() {
        let router = KeyRouter::new(4, KeySpec::ColumnIndices(vec![0]));

        // Create batch with null value
        let user_ids = Arc::new(Int64Array::from(vec![Some(100), None, Some(100)]));
        let batch = RecordBatch::try_from_iter(vec![
            ("user_id", user_ids as _),
        ]).unwrap();

        // Should handle nulls without error
        let core0 = router.route_row(&batch, 0).unwrap();
        let core1 = router.route_row(&batch, 1).unwrap(); // null
        let core2 = router.route_row(&batch, 2).unwrap();

        // Non-null same values should route same
        assert_eq!(core0, core2);
        // Null routes somewhere valid
        assert!(core1 < 4);
    }

    #[test]
    #[should_panic(expected = "num_cores must be > 0")]
    fn test_zero_cores_panics() {
        let _ = KeyRouter::new(0, KeySpec::RoundRobin);
    }

    #[test]
    fn test_debug() {
        let router = KeyRouter::new(4, KeySpec::Columns(vec!["user_id".to_string()]));
        let debug_str = format!("{router:?}");
        assert!(debug_str.contains("KeyRouter"));
        assert!(debug_str.contains("num_cores"));
    }
}
