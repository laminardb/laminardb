# Plan Compiler Research — 2026 State of the Art

> Last Updated: February 2026

## Executive Summary

This document surveys query compilation techniques relevant to LaminarDB's plan compiler — a component that JIT-compiles DataFusion logical plans into zero-allocation, event-at-a-time native functions for Ring 0 execution. The key finding: **Cranelift is the optimal JIT backend for Rust-based streaming systems**, offering ~0.2ms compilation times (10-100x faster than LLVM) with code quality only ~14% below LLVM — a tradeoff that is irrelevant for streaming queries running for hours.

---

## 1. Foundational Work: Query Compilation

### 1.1 Neumann's Produce/Consume Model (HyPer, 2011)

**Paper**: Thomas Neumann, "Efficiently Compiling Efficient Query Plans for Modern Hardware," PVLDB 4(9), 2011.

The foundational approach to query compilation. Key ideas:

- **Push-based data flow**: Instead of Volcano's pull-based `next()` model, operators *produce* tuples that are *consumed* by the next operator in the pipeline.
- **Pipeline boundaries**: Operators that need to materialize all input before producing output (hash join build, sort, aggregation) are "pipeline breakers." Everything between breakers forms a "pipeline" that can be compiled into a tight loop.
- **Register allocation**: Within a pipeline, tuple attributes stay in CPU registers — no materialization to memory between operators.
- **Code generation via LLVM**: The original HyPer system generated LLVM IR, compiled it via LLVM's JIT, and achieved performance matching hand-written C code.

**Key result**: Query execution with compilation was 2-5x faster than the best vectorized (batch-at-a-time) approaches for OLAP workloads.

**Relevance to LaminarDB**: The produce/consume model maps directly to our pipeline extraction. Ring 0 compiled pipelines are the "pipeline" segments; Ring 1 operators (windows, joins) are the pipeline breakers. The critical insight is that filter → project → key extract chains should compile into a single function with register-to-register data flow.

### 1.2 Evolution of a Compiling Query Engine (Umbra, 2021)

**Paper**: Thomas Neumann and Michael Freitag, "Evolution of a Compiling Query Engine," PVLDB 14(13), 2021.

The evolution from HyPer to Umbra revealed key lessons:

- **LLVM is too slow for JIT**: LLVM compilation times of 10-265ms per function were unacceptable for interactive queries. HyPer mitigated this with parallel compilation, but startup latency remained high.
- **Two-tier compilation**: Umbra uses "Flying Start" — a fast single-pass compiler (~0.2ms per function) for initial execution, with LLVM compilation running in the background for hot functions. When LLVM finishes, the system swaps in the optimized code.
- **Morsel-driven parallelism**: Work-stealing scheduler with fixed-size "morsels" of tuples. Each thread runs the compiled function on its morsel independently.

**Relevance to LaminarDB**: The two-tier pattern is unnecessary for streaming — queries run for hours/days, so even a 5ms compilation is amortized to nothing. But the Flying Start compiler validates that simple, fast backends (like Cranelift) produce perfectly adequate code quality for sustained workloads.

### 1.3 Tidy Tuples and Flying Start (2021)

**Paper**: Timo Kersten, Viktor Leis, Thomas Neumann, "Tidy Tuples and Flying Start: Fast Compilation and Fast Execution," PVLDB 14(11), 2021.

Details Umbra's Flying Start compiler:

- **Single-pass register allocation**: Unlike LLVM's multi-pass approach, Flying Start does a single forward pass, allocating registers greedily. This is ~100x faster than LLVM for compilation.
- **Row-oriented intermediate format**: Tuples are stored in a compact row format between pipeline breakers. Fixed-size header with null bitmap, fixed fields inline, variable-length fields at end — identical to our proposed `EventRow`.
- **Code quality**: Flying Start code runs at ~85% of LLVM speed, which is within noise for most workloads (the bottleneck is usually memory access, not computation).

**Relevance to LaminarDB**: Direct validation of our `EventRow` format design. The row layout matches Umbra's "Tidy Tuples" almost exactly. Also validates Cranelift as a compilation backend — its code quality is comparable to Flying Start's.

### 1.4 Adaptive Execution (2018)

**Paper**: André Kohn, Viktor Leis, Thomas Neumann, "Adaptive Execution of Compiled Queries," SIGMOD 2018.

Introduces a hybrid approach:

- **Bytecode VM**: Compile queries to a custom bytecode that runs on a register-based VM. This is near-instant (< 0.1ms) but ~10-50x slower than native code.
- **Background JIT**: While the VM executes, LLVM compiles the query in the background. When compilation finishes, the system switches from VM to native code.
- **Seamless transition**: The VM and native code share the same state format, so the switch is a pointer swap.

**Relevance to LaminarDB**: The VM approach maps to our DataFusion fallback. When JIT compilation is disabled or fails, DataFusion's interpreted evaluation serves the same role as the VM. The key architectural insight is that the "slow path" and "fast path" must share the same data format — which our `EventRow` achieves.

---

## 2. Compilation Backends for Rust

### 2.1 Cranelift

**Source**: https://cranelift.dev/ (part of the Wasmtime project)

Cranelift is a Rust-native code generator designed for JIT compilation:

- **Language**: Pure Rust (no C++ dependencies)
- **Compile speed**: ~0.2ms per function (measured on typical query expressions)
- **Code quality**: ~85% of LLVM (14% slower on average per the CGO 2024 paper)
- **Target support**: x86-64, AArch64, RISC-V, s390x
- **JIT support**: First-class via `cranelift-jit` crate — allocate executable memory, emit code, get function pointers
- **Safety**: The `cranelift-frontend` API prevents generating invalid IR (typed SSA construction)
- **Ecosystem**: Used by Wasmtime (production WebAssembly runtime), rustc_codegen_cranelift (experimental Rust backend)

**Key APIs for LaminarDB**:
```rust
// Core crates:
cranelift = "0.113"          // IR types and instructions
cranelift-jit = "0.113"      // JIT compilation and memory management
cranelift-module = "0.113"   // Function/data object management
cranelift-native = "0.113"   // Host ISA detection
cranelift-frontend = "0.113" // SSA construction helpers (FunctionBuilder)
```

**Compilation flow**:
1. Create `JITModule` with target ISA settings
2. Declare function signature (params + returns)
3. Build function body using `FunctionBuilder` (typed SSA construction)
4. Call `module.define_function()` → `module.finalize_definitions()`
5. Get function pointer via `module.get_finalized_function()`

### 2.2 LLVM (via inkwell)

**Crate**: `inkwell` — Rust bindings to LLVM C API

- **Compile speed**: 10-265ms per function
- **Code quality**: Best available (reference implementation for benchmarks)
- **Build complexity**: Requires LLVM installation (1GB+ download, 10+ minute build)
- **Rust integration**: FFI-based, requires careful lifetime management

**Why not for LaminarDB**: The 50-1000x compilation overhead vs Cranelift provides only ~14% better code quality. For streaming queries that run for hours, this tradeoff is unacceptable — startup latency matters, and the code quality difference is noise compared to memory access patterns.

### 2.3 Interpreted VM (Custom Bytecode)

A custom stack or register VM is the simplest approach:

- **Compile speed**: Effectively zero (bytecode generation is trivial)
- **Code quality**: 10-50x slower than native (interpretation overhead)
- **Complexity**: Low — simple `match` loop over opcodes
- **Flexibility**: Easy to add new operations

**Why not as primary**: The 10-50x slowdown would push per-event processing from ~50ns to ~500ns-2.5µs, blowing the Ring 0 latency budget. However, a bytecode interpreter could serve as a tier-0 fallback (before even DataFusion), though DataFusion's interpreted evaluation already fills this role.

### 2.4 Comparison Summary

| Backend | Compile Time | Code Quality | Build Complexity | Recommendation |
|---------|-------------|-------------|------------------|----------------|
| **Cranelift** | ~0.2ms | ~85% of LLVM | Low (pure Rust) | **Primary backend** |
| LLVM | 10-265ms | 100% (reference) | High (C++ dep) | Not recommended |
| Custom VM | ~0ms | 10-50x slower | Low | Not needed (DataFusion fallback) |
| Rust codegen (AOT) | N/A | Best | None | For fixed queries only |

---

## 3. Streaming-Specific Considerations

### 3.1 Flink's Operator Fusion Codegen (FLIP-315)

**Source**: Apache Flink FLIP-315 — Operator Fusion Codegen

Flink is implementing operator fusion codegen for streaming SQL:

- **Scope**: Fuses chains of stateless operators (filter, project, calc) into a single generated Java method
- **Approach**: Generates Java source code, compiles via Janino (in-memory Java compiler)
- **State handling**: Stateful operators (windows, joins) are NOT fused — they remain as pipeline breakers
- **Checkpoint handling**: Checkpoint barriers pass through fused operators unchanged
- **Performance gains**: 2-3x for stateless operator chains (less for stateful pipelines where state access dominates)

**Key finding for LaminarDB**: Flink confirmed that **compiling stateful operators provides diminishing returns** — the bottleneck shifts to state access (hash map lookups, serialization) rather than computation. This validates our decision to only compile Ring 0 stateless operations and keep windows/joins as hand-optimized Rust in Ring 1.

### 3.2 State Access Dominance

Multiple systems have found that for streaming workloads:

- **Stateless operations**: Compilation provides 2-10x improvement (virtual dispatch elimination, register allocation)
- **Stateful operations**: Compilation provides < 1.5x improvement (state access latency dominates)

This means the plan compiler's largest impact is on:
1. High-selectivity filters (most events filtered, few reach Ring 1)
2. Wide projections (many columns computed per event)
3. Complex expression chains (nested arithmetic, CASE/WHEN, multiple predicates)

For aggregation-heavy queries (e.g., `SELECT COUNT(*) FROM events GROUP BY key`), the compilation benefit is smaller because the window operator's state management dominates.

### 3.3 Event-at-a-Time vs Micro-Batch

| Model | Latency | Throughput | Compilation Benefit |
|-------|---------|------------|---------------------|
| Event-at-a-time | < 1µs | Lower (function call overhead) | High (eliminate dispatch per event) |
| Micro-batch (1K rows) | 1-10ms | Higher (SIMD, cache-friendly) | Lower (amortized dispatch) |
| Hybrid (our approach) | < 1µs for Ring 0, batch for Ring 1 | Best of both | Targeted |

LaminarDB's hybrid approach:
- **Ring 0**: Event-at-a-time through compiled pipelines (filter, project, key extract)
- **Ring 1**: Batch-oriented through existing operators (window, join, aggregation)
- **Bridge**: Accumulates Ring 0 output into batches for Ring 1, with watermark-aware flush

This gives sub-microsecond latency for the hot path while maintaining throughput for stateful operations.

---

## 4. Row Format Design

### 4.1 Umbra's Tidy Tuples

Umbra's row format (the direct inspiration for our `EventRow`):

```
┌────────┬──────────┬─────────────┬────────────────┐
│ Header │ Null Map │ Fixed Cols  │ Variable Cols  │
│ 8 bytes│ N bits   │ inline      │ offset+length  │
└────────┴──────────┴─────────────┴────────────────┘
```

- Header: row length + metadata
- Null bitmap: 1 bit per column (aligned to byte boundary)
- Fixed columns: 1/2/4/8 byte values, naturally aligned
- Variable columns: (offset, length) pair in fixed region, data at end

### 4.2 Arrow Row Format (arrow-row crate)

Arrow has its own row format for sorting/comparison, but it's designed for different goals:
- Lexicographic comparability (for sorting) — adds overhead for our use case
- Not designed for JIT access patterns
- Variable-length encoding uses continuation bytes

Our `EventRow` is simpler and faster for the specific use case of compiled field access.

### 4.3 Design Decision

We use a custom `EventRow` format inspired by Umbra's Tidy Tuples because:
1. **Compile-time offsets**: Schema is known at JIT time, so field offsets are constants in generated code
2. **No sorting overhead**: We don't need lexicographic comparability
3. **Arena-friendly**: Fixed-size allocation for all-fixed-width schemas
4. **Zero-copy bridge**: The same bytes flow from Ring 0 to Ring 1 without transformation

---

## 5. DataFusion Integration Points

### 5.1 Current State (DataFusion 52.0)

LaminarDB uses DataFusion for:
- SQL parsing (via sqlparser-rs 0.60)
- Logical plan optimization
- Physical plan generation
- Expression evaluation (`PhysicalExpr::evaluate()`)
- Type coercion and validation
- Built-in function library (50+ aggregates, scalar functions)

### 5.2 Post-Compiler State

After the plan compiler, DataFusion's role narrows to:
- SQL parsing (unchanged)
- Logical plan optimization (unchanged)
- **Logical plan → Pipeline extraction** (new: `PipelineExtractor` walks the `LogicalPlan`)
- Type coercion and validation (unchanged)
- **Fallback execution** (when JIT compilation fails)
- Ring 1 aggregate functions (via F075 DataFusion Aggregate Bridge)

DataFusion's physical execution layer (`ExecutionPlan`, `PhysicalExpr`) is bypassed in the JIT path but preserved as a fallback.

### 5.3 Expr Translation Strategy

DataFusion's `Expr` enum has ~40 variants. Our plan compiler supports a subset:

| Expr Variant | Phase 1 | Phase 2 | Notes |
|-------------|---------|---------|-------|
| `Column` | ✅ | ✅ | Direct memory offset |
| `Literal` | ✅ | ✅ | Constant folding |
| `BinaryExpr` (+, -, *, /, %) | ✅ | ✅ | Type-specialized |
| `BinaryExpr` (=, !=, <, >, <=, >=) | ✅ | ✅ | Type-specialized |
| `BinaryExpr` (AND, OR) | ✅ | ✅ | Short-circuit |
| `Not` | ✅ | ✅ | Bit flip |
| `IsNull` / `IsNotNull` | ✅ | ✅ | Bitmap check |
| `Cast` | ✅ | ✅ | Numeric conversions |
| `Case` / `Coalesce` | ✅ | ✅ | Branching |
| `Like` / `ILike` | ❌ | ✅ | String matching |
| `InList` | ❌ | ✅ | Hash set lookup |
| `Between` | ❌ | ✅ | Range check |
| `ScalarFunction` | ❌ | ✅ | Trampoline to Rust |
| `AggregateFunction` | ❌ | ❌ | Ring 1 only |
| `WindowFunction` | ❌ | ❌ | Ring 1 only |
| `Subquery` | ❌ | ❌ | Not applicable |

Unsupported expressions trigger the DataFusion fallback path.

---

## 6. Risk Assessment

### 6.1 Cranelift Stability

Cranelift is mature (used in production by Wasmtime, Firefox, and the experimental rustc backend) but evolves rapidly. Risks:
- **API changes**: Cranelift follows semver but makes breaking changes between major versions. Mitigated by pinning to a specific version.
- **Platform support**: x86-64 and AArch64 are well-tested; other targets less so. LaminarDB targets x86-64 primarily.
- **Code quality regressions**: Rare but possible. Mitigated by round-trip testing (compiled output must match DataFusion).

### 6.2 Complexity Budget

The plan compiler adds significant complexity:
- ~5 new modules in `laminar-core`
- Cranelift dependency (~2MB added to binary)
- New failure modes (compilation errors, JIT memory management)

Mitigated by:
- Feature-gated (`jit` flag) — the system works without Cranelift
- Transparent fallback to DataFusion
- Round-trip testing ensures correctness

### 6.3 Diminishing Returns for Stateful Queries

For queries where state access dominates (wide windows, large state), the plan compiler's benefit is limited to the filter/project prefix. Measurement recommendation: benchmark the ratio of Ring 0 time to Ring 1 time for representative queries before investing in further optimization.

---

## 7. References

1. **Neumann, T. (2011)**, "Efficiently Compiling Efficient Query Plans for Modern Hardware," PVLDB 4(9), 539-550.

2. **Neumann, T., Freitag, M. (2021)**, "Evolution of a Compiling Query Engine," PVLDB 14(13), 3207-3210.

3. **Kersten, T., Leis, V., Neumann, T. (2021)**, "Tidy Tuples and Flying Start: Fast Compilation and Fast Execution," PVLDB 14(11), 2086-2098.

4. **Kohn, A., Leis, V., Neumann, T. (2018)**, "Adaptive Execution of Compiled Queries," SIGMOD 2018, 197-212.

5. **Cranelift Project**, https://cranelift.dev/ — Rust-native JIT code generator.

6. **Apache Flink FLIP-315**, "Operator Fusion Codegen," https://cwiki.apache.org/confluence/display/FLINK/FLIP-315

7. **DataFusion Physical Expression Model**, https://docs.rs/datafusion/latest/datafusion/physical_expr/ — Interpreted expression evaluation.

8. **Schulze, T., et al. (CGO 2024)**, "Compile-Time Analysis of Compiler Frameworks for Query Compilation" — Cranelift vs LLVM vs custom backends.

9. **Arrow Row Format**, https://docs.rs/arrow-row/ — Arrow's own row format (designed for sorting, not JIT access).

10. **Wasmtime**, https://wasmtime.dev/ — Production WebAssembly runtime using Cranelift.
