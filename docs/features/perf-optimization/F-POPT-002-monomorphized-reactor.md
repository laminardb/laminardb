# F-POPT-002: Monomorphized Reactor Dispatch

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-POPT-002 |
| **Status** | Draft |
| **Priority** | P2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F001 |
| **Crate** | `laminar-core` |
| **Module** | `reactor/mod.rs` |
| **Audit Ref** | M16 |

## Summary

The reactor's per-event processing loop dispatches through `dyn Operator`
and `dyn StateStore` trait objects. Each call incurs vtable indirection,
preventing inlining and causing pipeline stalls. Monomorphize the hot loop
over concrete operator/store types.

## Problem

`reactor/mod.rs:98` — The core reactor loop calls `operator.process()` and
`store.get()`/`store.put()` through trait object pointers. At 500K+ events/sec,
the indirect call overhead and lost inlining opportunities measurably affect
throughput and tail latency.

## Proposed Fix

Option A: **Enum dispatch** — Define `OperatorEnum` and `StoreEnum` enums
wrapping all concrete types. Match once per event, calling concrete methods
directly. Compiler can inline through the match.

Option B: **Generic reactor** — Make `Reactor<O: Operator, S: StateStore>`
generic. Each pipeline instantiates a monomorphized reactor. Higher binary
size but zero dispatch overhead.

## Trade-offs

- Enum dispatch: easy to add new variants, moderate binary size increase
- Generic reactor: best performance, but increases compile time and binary
  size proportionally to the number of operator/store combinations
- Current virtual dispatch overhead is ~2-5ns per call; significant only at
  extreme throughput targets
