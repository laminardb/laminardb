# F-POPT-001: Zero-Copy Prefix Scan

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-POPT-001 |
| **Status** | Draft |
| **Priority** | P2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-STATE-001 |
| **Crate** | `laminar-core` |
| **Module** | `state/mod.rs` |
| **Audit Ref** | M1 |

## Summary

`prefix_scan()` currently returns `Box<dyn Iterator>`, allocating on every
call. Replace with a generic associated type (GAT) or concrete iterator per
store implementation to eliminate the heap allocation and vtable dispatch.

## Problem

`state/mod.rs:579` — Every prefix scan allocates a `Box<dyn Iterator>` even
though the concrete type is known at each call site. In session window and
join operators that scan by key prefix, this adds a heap allocation per event
on the hot path.

## Proposed Fix

Add a GAT `type PrefixIter<'a>` to the `StateStore` trait, returning a
concrete iterator type per implementation. This eliminates the `Box` and
allows the compiler to inline the iteration logic.

```rust
trait StateStore {
    type PrefixIter<'a>: Iterator<Item = (&'a [u8], &'a [u8])> where Self: 'a;
    fn prefix_scan(&self, prefix: &[u8]) -> Self::PrefixIter<'_>;
}
```

## Trade-offs

- GATs require Rust 1.65+ (already met)
- Makes the trait non-object-safe for prefix_scan; callers using `dyn StateStore`
  would need an enum dispatch wrapper
- Alternative: return `SmallVec<[(&[u8], &[u8]); 8]>` for small result sets
