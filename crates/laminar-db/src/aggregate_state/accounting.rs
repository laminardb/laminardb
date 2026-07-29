//! Saturating accounting components for resident aggregate state.
//!
//! This module reports a deterministic charged-byte estimate, not allocator usage or RSS. It
//! deliberately separates inline logical storage from owned payloads so callers can account for
//! every retained key clone without counting the inline value twice.
//!
//! The estimate excludes `AHashMap`/`AHashSet` bucket and control-byte allocations, allocator
//! metadata and fragmentation, unique attribution of shared `Arc` allocations, and process RSS.
//! It also excludes transient batch/output scratch and serialized checkpoint/capture bytes. Those
//! require separate budgets rather than being hidden inside the live-state number. Nested payloads
//! retained by changelog `ScalarValue`s are also excluded: inspecting every value would put an
//! allocation-dependent traversal on the record-processing hot path.

use std::mem::size_of;

use arrow::row::OwnedRow;
use datafusion_common::ScalarValue;
use datafusion_expr::Accumulator;

/// Categorized charged bytes retained by aggregate state.
///
/// Collection element storage describes the inline key/value or vector-element bytes for logical
/// entries. Hash-table bucket/control allocation is intentionally excluded because `AHashMap`
/// does not expose its physical allocation layout as a stable contract.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct AggregateStateUsage {
    topology_element_storage_bytes: usize,
    vnode_inline_bytes: usize,
    logical_collection_element_storage_bytes: usize,
    owned_row_payload_bytes: usize,
    accumulator_reported_bytes: usize,
    /// True when arithmetic was clamped and this snapshot is a lower-bound signal only.
    saturated: bool,
}

impl AggregateStateUsage {
    #[must_use]
    pub(super) fn from_parts(
        topology_element_storage_bytes: usize,
        vnode_inline_bytes: usize,
        logical_collection_element_storage_bytes: usize,
        owned_row_payload_bytes: usize,
        accumulator_reported_bytes: usize,
    ) -> Self {
        let mut usage = Self {
            topology_element_storage_bytes,
            vnode_inline_bytes,
            logical_collection_element_storage_bytes,
            owned_row_payload_bytes,
            accumulator_reported_bytes,
            saturated: false,
        };
        usage.saturated = usage.sum_overflows();
        usage
    }

    #[must_use]
    #[cfg(test)]
    pub(super) const fn logical_collection_element_storage_bytes(self) -> usize {
        self.logical_collection_element_storage_bytes
    }

    #[must_use]
    #[cfg(test)]
    pub(super) const fn owned_row_payload_bytes(self) -> usize {
        self.owned_row_payload_bytes
    }

    #[must_use]
    pub(super) const fn accumulator_reported_bytes(self) -> usize {
        self.accumulator_reported_bytes
    }

    /// Add two usage snapshots. Overflow clamps the affected category to `usize::MAX` and marks
    /// the result saturated; accounting must never fail a data or delivery path.
    #[must_use]
    pub(super) fn saturating_add(self, other: Self) -> Self {
        let (topology_element_storage_bytes, topology_saturated) = saturating_add_with_flag(
            self.topology_element_storage_bytes,
            other.topology_element_storage_bytes,
        );
        let (vnode_inline_bytes, vnode_saturated) =
            saturating_add_with_flag(self.vnode_inline_bytes, other.vnode_inline_bytes);
        let (logical_collection_element_storage_bytes, collection_saturated) =
            saturating_add_with_flag(
                self.logical_collection_element_storage_bytes,
                other.logical_collection_element_storage_bytes,
            );
        let (owned_row_payload_bytes, row_saturated) =
            saturating_add_with_flag(self.owned_row_payload_bytes, other.owned_row_payload_bytes);
        let (accumulator_reported_bytes, accumulator_saturated) = saturating_add_with_flag(
            self.accumulator_reported_bytes,
            other.accumulator_reported_bytes,
        );

        Self::from_parts_with_saturation(
            topology_element_storage_bytes,
            vnode_inline_bytes,
            logical_collection_element_storage_bytes,
            owned_row_payload_bytes,
            accumulator_reported_bytes,
            self.saturated
                || other.saturated
                || topology_saturated
                || vnode_saturated
                || collection_saturated
                || row_saturated
                || accumulator_saturated,
        )
    }

    /// Subtract one usage snapshot category-by-category. Underflow clamps the affected category to
    /// zero and marks the result saturated because exact accounting has been lost.
    #[must_use]
    pub(super) fn saturating_sub(self, other: Self) -> Self {
        let (topology_element_storage_bytes, topology_saturated) = saturating_sub_with_flag(
            self.topology_element_storage_bytes,
            other.topology_element_storage_bytes,
        );
        let (vnode_inline_bytes, vnode_saturated) =
            saturating_sub_with_flag(self.vnode_inline_bytes, other.vnode_inline_bytes);
        let (logical_collection_element_storage_bytes, collection_saturated) =
            saturating_sub_with_flag(
                self.logical_collection_element_storage_bytes,
                other.logical_collection_element_storage_bytes,
            );
        let (owned_row_payload_bytes, row_saturated) =
            saturating_sub_with_flag(self.owned_row_payload_bytes, other.owned_row_payload_bytes);
        let (accumulator_reported_bytes, accumulator_saturated) = saturating_sub_with_flag(
            self.accumulator_reported_bytes,
            other.accumulator_reported_bytes,
        );

        Self::from_parts_with_saturation(
            topology_element_storage_bytes,
            vnode_inline_bytes,
            logical_collection_element_storage_bytes,
            owned_row_payload_bytes,
            accumulator_reported_bytes,
            self.saturated
                || other.saturated
                || topology_saturated
                || vnode_saturated
                || collection_saturated
                || row_saturated
                || accumulator_saturated,
        )
    }

    /// Sum all categories, clamping to `usize::MAX` on overflow.
    #[must_use]
    pub(super) fn total_bytes(self) -> usize {
        [
            self.topology_element_storage_bytes,
            self.vnode_inline_bytes,
            self.logical_collection_element_storage_bytes,
            self.owned_row_payload_bytes,
            self.accumulator_reported_bytes,
        ]
        .into_iter()
        .fold(0_usize, usize::saturating_add)
    }

    /// Whether an overflow or underflow has made this snapshot advisory rather than exact.
    #[must_use]
    pub(super) const fn is_saturated(self) -> bool {
        self.saturated
    }

    fn from_parts_with_saturation(
        topology_element_storage_bytes: usize,
        vnode_inline_bytes: usize,
        logical_collection_element_storage_bytes: usize,
        owned_row_payload_bytes: usize,
        accumulator_reported_bytes: usize,
        saturated: bool,
    ) -> Self {
        let mut usage = Self::from_parts(
            topology_element_storage_bytes,
            vnode_inline_bytes,
            logical_collection_element_storage_bytes,
            owned_row_payload_bytes,
            accumulator_reported_bytes,
        );
        usage.saturated |= saturated;
        usage
    }

    fn sum_overflows(self) -> bool {
        [
            self.topology_element_storage_bytes,
            self.vnode_inline_bytes,
            self.logical_collection_element_storage_bytes,
            self.owned_row_payload_bytes,
            self.accumulator_reported_bytes,
        ]
        .into_iter()
        .try_fold(0_usize, usize::checked_add)
        .is_none()
    }
}

/// Inline storage for a fixed topology table or roster with `element_count` elements.
pub(super) fn topology_element_usage<T>(element_count: usize) -> AggregateStateUsage {
    let (bytes, saturated) = saturating_element_storage::<T>(element_count);
    AggregateStateUsage::from_parts_with_saturation(bytes, 0, 0, 0, 0, saturated)
}

/// Inline storage for `vnode_count` boxed vnode values; box pointer storage belongs to topology.
pub(super) fn vnode_inline_usage<T>(vnode_count: usize) -> AggregateStateUsage {
    let (bytes, saturated) = saturating_element_storage::<T>(vnode_count);
    AggregateStateUsage::from_parts_with_saturation(0, bytes, 0, 0, 0, saturated)
}

/// Inline storage reserved for logical collection elements.
pub(super) fn logical_collection_element_usage<T>(element_capacity: usize) -> AggregateStateUsage {
    let (bytes, saturated) = saturating_element_storage::<T>(element_capacity);
    AggregateStateUsage::from_parts_with_saturation(0, 0, bytes, 0, 0, saturated)
}

/// Payload bytes owned by `retained_copies` physical `OwnedRow` clones.
///
/// The inline `OwnedRow` value is accounted as collection element storage. Its boxed byte slice has
/// no spare capacity, so the encoded row length is the physical owned payload for each clone.
pub(super) fn owned_row_payload_usage(
    key: &OwnedRow,
    retained_copies: usize,
) -> AggregateStateUsage {
    let (payload, saturated) = saturating_mul_with_flag(key.as_ref().len(), retained_copies);
    AggregateStateUsage::from_parts_with_saturation(0, 0, 0, payload, 0, saturated)
}

/// Reserved accumulator-vector elements plus bytes reported by every live accumulator.
///
/// `Accumulator::size()` includes the accumulator value and its nested allocations. The vector
/// category counts only its `Box<dyn Accumulator>` elements, so these components do not overlap.
#[allow(clippy::ptr_arg)] // Capacity, not only the initialized slice, is part of the allocation.
pub(super) fn accumulator_usage(accumulators: &Vec<Box<dyn Accumulator>>) -> AggregateStateUsage {
    let (vector_elements, vector_saturated) =
        saturating_element_storage::<Box<dyn Accumulator>>(accumulators.capacity());
    let (reported, reported_saturated) =
        accumulators
            .iter()
            .fold((0_usize, false), |(total, saturated), accumulator| {
                let (total, addition_saturated) =
                    saturating_add_with_flag(total, accumulator.size());
                (total, saturated || addition_saturated)
            });
    AggregateStateUsage::from_parts_with_saturation(
        0,
        0,
        vector_elements,
        0,
        reported,
        vector_saturated || reported_saturated,
    )
}

/// Reserved inline elements for a retained changelog `Vec<ScalarValue>`.
///
/// This is constant-time with respect to initialized values: nested `ScalarValue` allocations and
/// shared `Arc` payload/control blocks are deliberately excluded rather than calling
/// `ScalarValue::size()` on the record-processing hot path.
#[allow(clippy::ptr_arg)] // Capacity, not only the initialized slice, is part of the allocation.
pub(super) fn retained_changelog_vector_element_usage(
    values: &Vec<ScalarValue>,
) -> AggregateStateUsage {
    logical_collection_element_usage::<ScalarValue>(values.capacity())
}

fn saturating_element_storage<T>(count: usize) -> (usize, bool) {
    saturating_mul_with_flag(count, size_of::<T>())
}

fn saturating_mul_with_flag(left: usize, right: usize) -> (usize, bool) {
    match left.checked_mul(right) {
        Some(value) => (value, false),
        None => (usize::MAX, true),
    }
}

fn saturating_add_with_flag(left: usize, right: usize) -> (usize, bool) {
    match left.checked_add(right) {
        Some(value) => (value, false),
        None => (usize::MAX, true),
    }
}

fn saturating_sub_with_flag(left: usize, right: usize) -> (usize, bool) {
    match left.checked_sub(right) {
        Some(value) => (value, false),
        None => (0, true),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::DataType;
    use arrow::row::{RowConverter, SortField};
    use datafusion_common::{Result as DataFusionResult, ScalarValue};

    use super::*;

    #[test]
    fn saturating_usage_arithmetic_preserves_categories_and_never_wraps() {
        assert_eq!(AggregateStateUsage::default().total_bytes(), 0);
        assert!(!AggregateStateUsage::default().is_saturated());

        let base = AggregateStateUsage::from_parts(1, 2, 3, 4, 5);
        let increment = AggregateStateUsage::from_parts(10, 20, 30, 40, 50);

        let combined = base.saturating_add(increment);
        assert_eq!(
            combined,
            AggregateStateUsage::from_parts(11, 22, 33, 44, 55)
        );
        assert_eq!(combined.total_bytes(), 165);
        assert_eq!(combined.saturating_sub(increment), base);
        assert!(!combined.is_saturated());

        let total_overflow = AggregateStateUsage::from_parts(usize::MAX, 1, 0, 0, 0);
        assert_eq!(total_overflow.total_bytes(), usize::MAX);
        assert!(total_overflow.is_saturated());

        let category_overflow = AggregateStateUsage::from_parts(usize::MAX, 0, 0, 0, 0)
            .saturating_add(AggregateStateUsage::from_parts(1, 0, 0, 0, 0));
        assert_eq!(category_overflow.total_bytes(), usize::MAX);
        assert!(category_overflow.is_saturated());

        let category_underflow = base.saturating_sub(increment);
        assert_eq!(category_underflow.total_bytes(), 0);
        assert!(category_underflow.is_saturated());
    }

    #[test]
    fn owned_row_payload_counts_every_retained_clone() {
        let converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let column: ArrayRef = Arc::new(StringArray::from(vec!["accounted-key"]));
        let rows = converter.convert_columns(&[column]).unwrap();
        let key = rows.row(0).owned();

        let usage = owned_row_payload_usage(&key, 3);
        assert_eq!(
            usage.owned_row_payload_bytes(),
            key.as_ref().len().checked_mul(3).unwrap()
        );
        assert_eq!(usage.total_bytes(), usage.owned_row_payload_bytes());
        assert!(!usage.is_saturated());
    }

    #[test]
    fn changelog_vector_usage_excludes_nested_scalar_allocations() {
        let mut compact_values = Vec::with_capacity(4);
        compact_values.push(ScalarValue::Int64(Some(7)));
        let mut allocated_values = Vec::with_capacity(4);
        allocated_values.push(ScalarValue::Utf8(Some("x".repeat(32 * 1024))));

        let compact_usage = retained_changelog_vector_element_usage(&compact_values);
        let allocated_usage = retained_changelog_vector_element_usage(&allocated_values);
        let expected_inline = compact_values.capacity() * size_of::<ScalarValue>();

        assert_eq!(
            compact_usage.logical_collection_element_storage_bytes(),
            expected_inline
        );
        assert_eq!(compact_usage, allocated_usage);
        assert_eq!(compact_usage.total_bytes(), expected_inline);
        assert!(!compact_usage.is_saturated());
    }

    #[derive(Debug)]
    struct ReportedSizeAccumulator(usize);

    impl Accumulator for ReportedSizeAccumulator {
        fn update_batch(&mut self, _values: &[ArrayRef]) -> DataFusionResult<()> {
            Ok(())
        }

        fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
            Ok(ScalarValue::Null)
        }

        fn size(&self) -> usize {
            self.0
        }

        fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
            Ok(vec![ScalarValue::Null])
        }

        fn merge_batch(&mut self, _states: &[ArrayRef]) -> DataFusionResult<()> {
            Ok(())
        }
    }

    #[test]
    fn accumulator_usage_counts_capacity_and_reported_sizes_without_overlap() {
        let mut accumulators: Vec<Box<dyn Accumulator>> = Vec::with_capacity(3);
        accumulators.push(Box::new(ReportedSizeAccumulator(11)));
        accumulators.push(Box::new(ReportedSizeAccumulator(17)));

        let usage = accumulator_usage(&accumulators);
        assert_eq!(
            usage.logical_collection_element_storage_bytes(),
            accumulators.capacity() * size_of::<Box<dyn Accumulator>>()
        );
        assert_eq!(usage.accumulator_reported_bytes(), 28);
        assert_eq!(
            usage.total_bytes(),
            accumulators.capacity() * size_of::<Box<dyn Accumulator>>() + 28
        );
        assert!(!usage.is_saturated());
    }

    #[test]
    fn helper_multiplication_overflow_is_clamped_and_reported() {
        let usage = logical_collection_element_usage::<u64>(usize::MAX);

        assert_eq!(usage.total_bytes(), usize::MAX);
        assert!(usage.is_saturated());
    }
}
