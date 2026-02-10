# F079: Compiled Expression Evaluator

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F079 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 2.5 (Plan Compiler) |
| **Effort** | L (4-5 days) |
| **Dependencies** | F078 |
| **Owner** | TBD |

## Summary

Compile individual DataFusion `Expr` expressions into Cranelift JIT functions that operate directly on `EventRow` byte layouts. This eliminates the per-expression dynamic dispatch and per-batch allocation overhead of DataFusion's `PhysicalExpr::evaluate()`, producing native function pointers that evaluate expressions in < 50ns per event.

## Motivation

DataFusion evaluates expressions via a tree of `PhysicalExpr` trait objects:

```
evaluate(batch) → ColumnarValue
     ↓
  BinaryExpr::evaluate
     ├── left.evaluate(batch)  → ColumnarValue  (dynamic dispatch)
     └── right.evaluate(batch) → ColumnarValue  (dynamic dispatch)
```

For a streaming query like `WHERE price > 100.0 AND symbol = 'AAPL'`, this means:
- 5 virtual function calls per batch (Column, Literal, Gt, Column, Eq, And)
- 5 `ColumnarValue` allocations per batch
- No opportunity for register-to-register data flow

With Cranelift compilation, the same expression becomes a single function:

```
fn filter(row: &[u8]) -> bool {
    let price: f64 = read_f64(row, 16);  // constant offset
    let symbol: &str = read_str(row, 8); // constant offset
    price > 100.0 && symbol == "AAPL"    // short-circuit AND
}
```

Zero allocations, zero virtual dispatch, predictable branches.

## Goals

1. `ExprCompiler` — translates a DataFusion `Expr` tree into Cranelift IR
2. Phase 1 expression support: column refs, literals, arithmetic, comparison, boolean logic, IS NULL, CAST, CASE/COALESCE
3. Proper three-valued null logic via null bitmap checks
4. Type-specialized code paths for i64, f64, bool, and string comparisons
5. Constant folding at compile time (e.g., `1 + 2` → `3`)
6. Short-circuit evaluation for AND/OR chains
7. Interpreted fallback for unsupported expressions

## Non-Goals

- String operations beyond equality (LIKE, REGEX — Phase 2 of the compiler)
- User-defined functions (UDFs — require callback trampolines)
- Aggregate expressions (handled by Ring 1 operators)
- Window functions (handled by Ring 1 operators)

## Technical Design

### Cranelift Integration

```rust
use cranelift::prelude::*;
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::Module;

/// Manages Cranelift JIT compilation state.
///
/// One instance per core (thread-per-core model). Caches compiled
/// functions by expression hash.
pub struct JitContext {
    /// Cranelift JIT module for code emission.
    module: JITModule,
    /// Reusable function builder context (avoids re-allocation).
    builder_ctx: FunctionBuilderContext,
    /// ISA-specific settings (target CPU features).
    isa: Arc<dyn isa::TargetIsa>,
}

impl JitContext {
    pub fn new() -> Self {
        let mut flag_builder = settings::builder();
        flag_builder.set("opt_level", "speed").unwrap();
        flag_builder.set("is_pic", "true").unwrap();
        let flags = settings::Flags::new(flag_builder);
        let isa = cranelift_native::builder()
            .unwrap()
            .finish(flags)
            .unwrap();

        let builder = JITBuilder::with_isa(isa.clone(), default_libcall_names());
        let module = JITModule::new(builder);

        Self {
            module,
            builder_ctx: FunctionBuilderContext::new(),
            isa,
        }
    }
}
```

### ExprCompiler

```rust
/// Compiles a DataFusion Expr into a Cranelift function.
///
/// The compiled function signature depends on usage:
/// - Filter: `fn(row_ptr: *const u8) -> bool`
/// - Scalar: `fn(row_ptr: *const u8, out_ptr: *mut u8) -> bool` (returns is_null)
/// - Key extract: `fn(row_ptr: *const u8, key_ptr: *mut u8) -> u32` (returns key len)
pub struct ExprCompiler<'a> {
    jit: &'a mut JitContext,
    schema: &'a RowSchema,
}

/// The result of compiling an expression.
pub enum CompiledExpr {
    /// A filter function: returns true if the row passes.
    Filter(FilterFn),
    /// A scalar function: writes result to output, returns is_null.
    Scalar(ScalarFn),
}

/// Compiled filter: `fn(row_ptr: *const u8) -> bool`
pub type FilterFn = unsafe extern "C" fn(*const u8) -> bool;

/// Compiled scalar: `fn(row_ptr: *const u8, out_ptr: *mut u8) -> bool`
/// Returns true if result is null.
pub type ScalarFn = unsafe extern "C" fn(*const u8, *mut u8) -> bool;

impl<'a> ExprCompiler<'a> {
    /// Compile a filter expression (WHERE clause).
    pub fn compile_filter(&mut self, expr: &Expr) -> Result<FilterFn, CompileError> {
        let mut sig = self.jit.module.make_signature();
        sig.params.push(AbiParam::new(types::I64)); // row_ptr
        sig.returns.push(AbiParam::new(types::I8));  // bool result

        let func_id = self.jit.module
            .declare_function("filter", Linkage::Local, &sig)?;

        let mut ctx = self.jit.module.make_context();
        ctx.func.signature = sig;

        {
            let mut builder = FunctionBuilder::new(&mut ctx.func, &mut self.jit.builder_ctx);
            let entry = builder.create_block();
            builder.append_block_params_for_function_params(entry);
            builder.switch_to_block(entry);
            builder.seal_block(entry);

            let row_ptr = builder.block_params(entry)[0];
            let result = self.compile_expr_inner(&mut builder, expr, row_ptr)?;

            // Coerce to bool
            let bool_result = self.coerce_to_bool(&mut builder, result);
            builder.ins().return_(&[bool_result]);
            builder.finalize();
        }

        self.jit.module.define_function(func_id, &mut ctx)?;
        self.jit.module.clear_context(&mut ctx);
        self.jit.module.finalize_definitions()?;

        let code_ptr = self.jit.module.get_finalized_function(func_id);
        Ok(unsafe { std::mem::transmute(code_ptr) })
    }

    /// Compile a scalar expression (projection).
    pub fn compile_scalar(
        &mut self,
        expr: &Expr,
        output_type: &FieldType,
    ) -> Result<ScalarFn, CompileError> {
        // Similar to compile_filter but writes result to out_ptr
        todo!("Implementation follows same pattern as compile_filter")
    }
}
```

### Expression Translation (Expr → Cranelift IR)

```rust
/// Internal value representation during compilation.
/// Tracks both the Cranelift SSA value and its nullability.
struct CompiledValue {
    /// The Cranelift SSA value (register).
    value: Value,
    /// Whether this value might be null (requires bitmap check).
    is_nullable: bool,
    /// If nullable, the Cranelift SSA value holding the null flag.
    null_flag: Option<Value>,
    /// The logical type of this value.
    value_type: FieldType,
}

impl<'a> ExprCompiler<'a> {
    /// Recursively compile a DataFusion Expr to Cranelift IR.
    fn compile_expr_inner(
        &self,
        builder: &mut FunctionBuilder<'_>,
        expr: &Expr,
        row_ptr: Value,
    ) -> Result<CompiledValue, CompileError> {
        match expr {
            // --- Column reference: load from EventRow at known offset ---
            Expr::Column(col) => {
                let field_idx = self.resolve_column(col)?;
                let layout = &self.schema.fields[field_idx];

                // Null check: load bitmap byte, test bit
                let null_flag = self.emit_null_check(builder, row_ptr, field_idx);

                // Load value at fixed offset
                let value = match layout.data_type {
                    FieldType::Int64 | FieldType::TimestampMicros => {
                        let addr = builder.ins().iadd_imm(row_ptr, layout.offset as i64);
                        builder.ins().load(types::I64, MemFlags::trusted(), addr, 0)
                    }
                    FieldType::Float64 => {
                        let addr = builder.ins().iadd_imm(row_ptr, layout.offset as i64);
                        builder.ins().load(types::F64, MemFlags::trusted(), addr, 0)
                    }
                    FieldType::Int32 => {
                        let addr = builder.ins().iadd_imm(row_ptr, layout.offset as i64);
                        let raw = builder.ins().load(types::I32, MemFlags::trusted(), addr, 0);
                        builder.ins().sextend(types::I64, raw)
                    }
                    FieldType::Bool => {
                        let addr = builder.ins().iadd_imm(row_ptr, layout.offset as i64);
                        builder.ins().load(types::I8, MemFlags::trusted(), addr, 0)
                    }
                    _ => return Err(CompileError::UnsupportedType(layout.data_type)),
                };

                Ok(CompiledValue {
                    value,
                    is_nullable: true,
                    null_flag: Some(null_flag),
                    value_type: layout.data_type,
                })
            }

            // --- Literal: emit constant ---
            Expr::Literal(scalar) => {
                let (value, vtype) = match scalar {
                    ScalarValue::Int64(Some(v)) => {
                        (builder.ins().iconst(types::I64, *v), FieldType::Int64)
                    }
                    ScalarValue::Float64(Some(v)) => {
                        (builder.ins().f64const(*v), FieldType::Float64)
                    }
                    ScalarValue::Boolean(Some(v)) => {
                        let i = if *v { 1i64 } else { 0i64 };
                        (builder.ins().iconst(types::I8, i), FieldType::Bool)
                    }
                    ScalarValue::Null => {
                        let null_true = builder.ins().iconst(types::I8, 1);
                        return Ok(CompiledValue {
                            value: builder.ins().iconst(types::I64, 0),
                            is_nullable: true,
                            null_flag: Some(null_true),
                            value_type: FieldType::Int64,
                        });
                    }
                    _ => return Err(CompileError::UnsupportedLiteral),
                };
                Ok(CompiledValue {
                    value,
                    is_nullable: false,
                    null_flag: None,
                    value_type: vtype,
                })
            }

            // --- Binary expressions: arithmetic and comparison ---
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let lhs = self.compile_expr_inner(builder, left, row_ptr)?;
                let rhs = self.compile_expr_inner(builder, right, row_ptr)?;
                self.compile_binary_op(builder, &lhs, *op, &rhs)
            }

            // --- Boolean NOT ---
            Expr::Not(inner) => {
                let val = self.compile_expr_inner(builder, inner, row_ptr)?;
                let negated = builder.ins().bxor_imm(val.value, 1);
                Ok(CompiledValue { value: negated, ..val })
            }

            // --- IS NULL / IS NOT NULL ---
            Expr::IsNull(inner) => {
                let val = self.compile_expr_inner(builder, inner, row_ptr)?;
                let result = val.null_flag.unwrap_or_else(|| {
                    builder.ins().iconst(types::I8, 0) // non-nullable → always false
                });
                Ok(CompiledValue {
                    value: result,
                    is_nullable: false,
                    null_flag: None,
                    value_type: FieldType::Bool,
                })
            }
            Expr::IsNotNull(inner) => {
                let val = self.compile_expr_inner(builder, inner, row_ptr)?;
                let null = val.null_flag.unwrap_or_else(|| {
                    builder.ins().iconst(types::I8, 0)
                });
                let result = builder.ins().bxor_imm(null, 1);
                Ok(CompiledValue {
                    value: result,
                    is_nullable: false,
                    null_flag: None,
                    value_type: FieldType::Bool,
                })
            }

            // --- CAST ---
            Expr::Cast(Cast { expr, data_type }) => {
                let val = self.compile_expr_inner(builder, expr, row_ptr)?;
                self.compile_cast(builder, &val, data_type)
            }

            // --- CASE WHEN ---
            Expr::Case(case) => {
                self.compile_case(builder, case, row_ptr)
            }

            // --- Unsupported: fall back ---
            other => Err(CompileError::UnsupportedExpr(format!("{:?}", other))),
        }
    }
}
```

### Binary Operation Compilation

```rust
impl<'a> ExprCompiler<'a> {
    fn compile_binary_op(
        &self,
        builder: &mut FunctionBuilder<'_>,
        lhs: &CompiledValue,
        op: Operator,
        rhs: &CompiledValue,
    ) -> Result<CompiledValue, CompileError> {
        // Propagate nulls: if either input is null, result is null
        let null_flag = self.merge_null_flags(builder, lhs, rhs);

        let result = match (lhs.value_type, op) {
            // --- Integer arithmetic ---
            (FieldType::Int64, Operator::Plus) => builder.ins().iadd(lhs.value, rhs.value),
            (FieldType::Int64, Operator::Minus) => builder.ins().isub(lhs.value, rhs.value),
            (FieldType::Int64, Operator::Multiply) => builder.ins().imul(lhs.value, rhs.value),
            (FieldType::Int64, Operator::Divide) => builder.ins().sdiv(lhs.value, rhs.value),
            (FieldType::Int64, Operator::Modulo) => builder.ins().srem(lhs.value, rhs.value),

            // --- Integer comparison ---
            (FieldType::Int64, Operator::Eq) => {
                builder.ins().icmp(IntCC::Equal, lhs.value, rhs.value)
            }
            (FieldType::Int64, Operator::NotEq) => {
                builder.ins().icmp(IntCC::NotEqual, lhs.value, rhs.value)
            }
            (FieldType::Int64, Operator::Lt) => {
                builder.ins().icmp(IntCC::SignedLessThan, lhs.value, rhs.value)
            }
            (FieldType::Int64, Operator::LtEq) => {
                builder.ins().icmp(IntCC::SignedLessThanOrEqual, lhs.value, rhs.value)
            }
            (FieldType::Int64, Operator::Gt) => {
                builder.ins().icmp(IntCC::SignedGreaterThan, lhs.value, rhs.value)
            }
            (FieldType::Int64, Operator::GtEq) => {
                builder.ins().icmp(IntCC::SignedGreaterThanOrEqual, lhs.value, rhs.value)
            }

            // --- Float arithmetic ---
            (FieldType::Float64, Operator::Plus) => builder.ins().fadd(lhs.value, rhs.value),
            (FieldType::Float64, Operator::Minus) => builder.ins().fsub(lhs.value, rhs.value),
            (FieldType::Float64, Operator::Multiply) => builder.ins().fmul(lhs.value, rhs.value),
            (FieldType::Float64, Operator::Divide) => builder.ins().fdiv(lhs.value, rhs.value),

            // --- Float comparison ---
            (FieldType::Float64, Operator::Eq) => {
                builder.ins().fcmp(FloatCC::Equal, lhs.value, rhs.value)
            }
            (FieldType::Float64, Operator::Lt) => {
                builder.ins().fcmp(FloatCC::LessThan, lhs.value, rhs.value)
            }
            (FieldType::Float64, Operator::Gt) => {
                builder.ins().fcmp(FloatCC::GreaterThan, lhs.value, rhs.value)
            }
            // ... other float comparisons follow the same pattern

            // --- Boolean logic (short-circuit) ---
            (FieldType::Bool, Operator::And) => {
                return self.compile_short_circuit_and(builder, lhs, rhs);
            }
            (FieldType::Bool, Operator::Or) => {
                return self.compile_short_circuit_or(builder, lhs, rhs);
            }

            _ => return Err(CompileError::UnsupportedBinaryOp(lhs.value_type, op)),
        };

        let result_type = match op {
            Operator::Eq | Operator::NotEq | Operator::Lt
            | Operator::LtEq | Operator::Gt | Operator::GtEq => FieldType::Bool,
            _ => lhs.value_type,
        };

        Ok(CompiledValue {
            value: result,
            is_nullable: lhs.is_nullable || rhs.is_nullable,
            null_flag,
            value_type: result_type,
        })
    }
}
```

### Short-Circuit Boolean Evaluation

```rust
impl<'a> ExprCompiler<'a> {
    /// Compile `lhs AND rhs` with short-circuit: if lhs is false, skip rhs.
    fn compile_short_circuit_and(
        &self,
        builder: &mut FunctionBuilder<'_>,
        lhs: &CompiledValue,
        rhs: &CompiledValue,
    ) -> Result<CompiledValue, CompileError> {
        let rhs_block = builder.create_block();
        let merge_block = builder.create_block();
        builder.append_block_param(merge_block, types::I8); // result

        // If lhs is false, short-circuit to merge with false
        builder.ins().brif(lhs.value, rhs_block, &[], merge_block, &[lhs.value]);

        // RHS block: evaluate rhs and jump to merge
        builder.switch_to_block(rhs_block);
        builder.seal_block(rhs_block);
        builder.ins().jump(merge_block, &[rhs.value]);

        // Merge block: result is the phi of lhs (false) or rhs
        builder.switch_to_block(merge_block);
        builder.seal_block(merge_block);
        let result = builder.block_params(merge_block)[0];

        Ok(CompiledValue {
            value: result,
            is_nullable: false,
            null_flag: None,
            value_type: FieldType::Bool,
        })
    }
}
```

### Null Propagation

```rust
impl<'a> ExprCompiler<'a> {
    /// Emit a null bitmap check for the given field.
    fn emit_null_check(
        &self,
        builder: &mut FunctionBuilder<'_>,
        row_ptr: Value,
        field_idx: usize,
    ) -> Value {
        let byte_offset = self.schema.null_bitmap_offset + (field_idx >> 3);
        let bit_idx = field_idx & 7;

        let addr = builder.ins().iadd_imm(row_ptr, byte_offset as i64);
        let byte = builder.ins().load(types::I8, MemFlags::trusted(), addr, 0);
        let mask = builder.ins().iconst(types::I8, 1 << bit_idx);
        let masked = builder.ins().band(byte, mask);

        // Result: 0 = not null, non-zero = null
        masked
    }

    /// Merge null flags: result is null if either input is null.
    fn merge_null_flags(
        &self,
        builder: &mut FunctionBuilder<'_>,
        lhs: &CompiledValue,
        rhs: &CompiledValue,
    ) -> Option<Value> {
        match (lhs.null_flag, rhs.null_flag) {
            (Some(l), Some(r)) => Some(builder.ins().bor(l, r)),
            (Some(l), None) => Some(l),
            (None, Some(r)) => Some(r),
            (None, None) => None,
        }
    }
}
```

### Constant Folding

```rust
/// Pre-pass: fold constant sub-expressions before Cranelift compilation.
///
/// DataFusion's optimizer already does some constant folding, but we
/// do a final pass to catch anything remaining (e.g., CAST(42 AS BIGINT)).
pub fn fold_constants(expr: &Expr) -> Expr {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left = fold_constants(left);
            let right = fold_constants(right);
            match (&left, op, &right) {
                (Expr::Literal(l), Operator::Plus, Expr::Literal(r)) => {
                    if let Some(result) = try_fold_arithmetic(l, *op, r) {
                        return Expr::Literal(result);
                    }
                }
                // ... other arithmetic ops
                _ => {}
            }
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op: *op,
                right: Box::new(right),
            })
        }
        _ => expr.clone(),
    }
}
```

### Compilation Error and Fallback

```rust
/// Errors that can occur during expression compilation.
#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("Unsupported expression: {0}")]
    UnsupportedExpr(String),
    #[error("Unsupported data type: {0:?}")]
    UnsupportedType(FieldType),
    #[error("Unsupported binary operation: {0:?} {1:?}")]
    UnsupportedBinaryOp(FieldType, Operator),
    #[error("Unsupported literal type")]
    UnsupportedLiteral,
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    #[error("Cranelift error: {0}")]
    Cranelift(#[from] cranelift_module::ModuleError),
}

/// Wrapper that tries compiled evaluation, falls back to DataFusion.
pub enum MaybeCompiledExpr {
    /// Successfully compiled — use the native function pointer.
    Compiled(CompiledExpr),
    /// Compilation failed — use DataFusion interpreted evaluation.
    Interpreted {
        physical_expr: Arc<dyn PhysicalExpr>,
        reason: CompileError,
    },
}
```

## Performance Targets

| Metric | Target |
|--------|--------|
| Compile time (single expr) | < 0.5ms |
| Compile time (5-expr chain) | < 2ms |
| Eval: `col > literal` | < 20ns |
| Eval: `col1 * col2 + col3` | < 30ns |
| Eval: `AND` chain (3 terms) | < 40ns (with short-circuit) |
| Eval: 10-column projection | < 100ns |
| Fallback overhead | < 5% vs pure DataFusion |

## Testing

| Module | Tests | What |
|--------|-------|------|
| `ExprCompiler` (filter) | 12 | Simple comparison, compound AND/OR, nested expressions, all comparison ops |
| `ExprCompiler` (scalar) | 10 | Arithmetic, CAST, mixed types, write-to-output |
| Null handling | 8 | NULL propagation, IS NULL, IS NOT NULL, three-valued AND/OR |
| Constant folding | 6 | Literal arithmetic, nested folds, no-op on columns |
| Short-circuit | 4 | AND short-circuit, OR short-circuit, mixed null+bool |
| Fallback | 4 | Unsupported expr → DataFusion, error messages |
| Round-trip | 6 | DataFusion plan → compile → evaluate → verify against DataFusion output |

## Files

- `crates/laminar-core/src/compiler/expr.rs` — NEW: `ExprCompiler`, expression translation
- `crates/laminar-core/src/compiler/jit.rs` — NEW: `JitContext`, Cranelift setup
- `crates/laminar-core/src/compiler/fold.rs` — NEW: Constant folding pass
- `crates/laminar-core/src/compiler/error.rs` — NEW: `CompileError`, `MaybeCompiledExpr`
- `crates/laminar-core/Cargo.toml` — Add `cranelift`, `cranelift-jit`, `cranelift-native`, `cranelift-module` dependencies (behind `jit` feature flag)
