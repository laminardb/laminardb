# Plan Compiler Feature Index

## Overview

The Plan Compiler transforms DataFusion logical plans into zero-allocation, event-at-a-time compiled functions for Ring 0 execution. DataFusion remains the SQL "brain" (parsing, optimization, logical planning); Cranelift JIT becomes the "hands" (native code execution in Ring 0).

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     Plan Compiler Pipeline                    │
│                                                              │
│  SQL String                                                  │
│      │                                                       │
│      ▼                                                       │
│  DataFusion ──► LogicalPlan ──► PipelineExtractor            │
│                                      │                       │
│                         ┌────────────┴────────────┐          │
│                         │                         │          │
│                    Pipeline 1              Pipeline Breaker   │
│                  (filter+project)        (window/join/agg)   │
│                         │                         │          │
│                         ▼                         │          │
│                 ExprCompiler ──►                   │          │
│                 Cranelift JIT                      │          │
│                         │                         │          │
│                         ▼                         ▼          │
│              ┌─────────────────┐      ┌───────────────────┐  │
│  Ring 0:     │ CompiledPipeline│      │                   │  │
│              │ fn(*u8,*u8)->u8 │─────►│ PipelineBridge    │  │
│              └─────────────────┘      └───────┬───────────┘  │
│                                               │              │
│  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ │
│                                               │              │
│  Ring 1:                              ┌───────▼───────────┐  │
│                                       │ Ring 1 Operators  │  │
│                                       │ (Window/Join/Agg) │  │
│                                       └───────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

## Features

| ID | Feature | Priority | Effort | Status | Spec |
|----|---------|----------|--------|--------|------|
| F078 | Event Row Format | P0 | S | 📝 Draft | [Link](F078-event-row-format.md) |
| F079 | Compiled Expression Evaluator | P0 | L | 📝 Draft | [Link](F079-compiled-expression-evaluator.md) |
| F080 | Plan Compiler Core | P0 | XL | 📝 Draft | [Link](F080-plan-compiler-core.md) |
| F081 | Ring 0/Ring 1 Pipeline Bridge | P0 | M | 📝 Draft | [Link](F081-ring0-ring1-pipeline-bridge.md) |
| F082 | Streaming Query Lifecycle | P0 | L | 📝 Draft | [Link](F082-streaming-query-lifecycle.md) |

## Implementation Order

```
F078 Event Row Format           [S, no deps]
 └──► F079 Compiled Expr Eval   [L, depends on F078]
       └──► F080 Plan Compiler  [XL, depends on F079]
             └──► F081 Bridge   [M, depends on F080, F014]
                   └──► F082 Query Lifecycle [L, depends on F081, F005, F001]
```

## Dependencies on Existing Features

| Dependency | Why |
|------------|-----|
| F001 (Reactor) | Bridge integrates with `Reactor::poll()` |
| F005 (DataFusion) | LogicalPlan is the input to the compiler |
| F014 (SPSC Queues) | Bridge uses SPSC for Ring 0 → Ring 1 |
| F022 (Checkpointing) | Checkpoint barriers propagate through bridge |
| F071 (Zero-Alloc) | Compiled code must pass `HotPathGuard` |

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| JIT backend | Cranelift | Rust-native, ~0.2ms compile, 85% of LLVM quality |
| Row format | Custom `EventRow` | Fixed layout for compiled offset access |
| Compilation scope | Stateless ops only | Windows/joins too complex for JIT, marginal benefit |
| Fallback | DataFusion interpreted | Transparent degradation for unsupported expressions |
| Feature gating | `jit` feature flag | Cranelift is optional; everything else works without it |
