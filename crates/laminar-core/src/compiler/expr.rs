//! Compiled expression evaluator using Cranelift JIT.
//!
//! [`ExprCompiler`] translates `DataFusion` [`Expr`] trees into native machine code
//! via Cranelift. The generated functions operate directly on [`super::EventRow`] byte
//! buffers using pointer arithmetic — zero allocations, zero virtual dispatch.
//!
//! # Supported Expressions
//!
//! - Column references (load from row pointer + field offset)
//! - Literals (`i64`, `f64`, `i32`, `bool`, null)
//! - Binary arithmetic (`+`, `-`, `*`, `/`, `%`) for integer and float types
//! - Comparisons (`=`, `!=`, `<`, `<=`, `>`, `>=`)
//! - Boolean logic (`AND`, `OR`) with short-circuit evaluation
//! - `NOT`, `IS NULL`, `IS NOT NULL`
//! - `CAST` between numeric types
//! - `CASE WHEN ... THEN ... ELSE ... END`
//!
//! # Null Handling
//!
//! Null propagation follows SQL semantics: any arithmetic or comparison with a
//! NULL operand produces NULL. `IS NULL` / `IS NOT NULL` inspect the null flag
//! directly.

use cranelift_codegen::ir::condcodes::{FloatCC, IntCC};
use cranelift_codegen::ir::types::{self as cl_types};
use cranelift_codegen::ir::{
    AbiParam, BlockArg, Function, InstBuilder, MemFlags, Type as CraneliftType, UserFuncName, Value,
};
use cranelift_codegen::Context;
use cranelift_frontend::FunctionBuilder;
use cranelift_module::Module;
use datafusion_common::ScalarValue;
use datafusion_expr::{BinaryExpr, Expr, Operator};

use super::error::{CompileError, FilterFn, ScalarFn};
use super::fold::fold_constants;
use super::jit::JitContext;
use super::row::{FieldType, RowSchema};

/// Pointer type for the target architecture.
const PTR_TYPE: CraneliftType = cl_types::I64;

/// Tracks a compiled SSA value plus its null state.
pub(crate) struct CompiledValue {
    /// The SSA value in Cranelift IR.
    pub(crate) value: Value,
    /// Whether this value can be null.
    pub(crate) is_nullable: bool,
    /// SSA value holding the null flag (i8: 0 = valid, nonzero = null).
    pub(crate) null_flag: Option<Value>,
    /// The [`FieldType`] of this value.
    pub(crate) value_type: FieldType,
}

/// Compiles `DataFusion` expressions into native functions via Cranelift.
///
/// Borrows a [`JitContext`] (for the module/builder) and a [`RowSchema`]
/// (for field layout resolution). Each `compile_*` call produces one native
/// function.
pub struct ExprCompiler<'a> {
    jit: &'a mut JitContext,
    schema: &'a RowSchema,
}

impl<'a> ExprCompiler<'a> {
    /// Creates a new compiler for the given JIT context and row schema.
    pub fn new(jit: &'a mut JitContext, schema: &'a RowSchema) -> Self {
        Self { jit, schema }
    }

    /// Compiles a filter expression into a native `FilterFn`.
    ///
    /// The generated function has signature `fn(*const u8) -> u8` where the
    /// argument is a pointer to an `EventRow` byte buffer. Returns 1 if the
    /// row passes the filter, 0 otherwise. NULL filter results are treated
    /// as false (row is rejected).
    ///
    /// # Errors
    ///
    /// Returns [`CompileError`] if the expression contains unsupported nodes.
    ///
    /// # Panics
    ///
    /// Panics if Cranelift fails to finalize function definitions (internal error).
    pub fn compile_filter(&mut self, expr: &Expr) -> Result<FilterFn, CompileError> {
        let expr = fold_constants(expr);
        let func_name = self.jit.next_func_name("filter");

        let mut sig = self.jit.module().make_signature();
        sig.params.push(AbiParam::new(PTR_TYPE));
        sig.returns.push(AbiParam::new(cl_types::I8));

        let func_id = self.jit.module().declare_function(
            &func_name,
            cranelift_module::Linkage::Local,
            &sig,
        )?;

        let mut func = Function::with_name_signature(UserFuncName::testcase(&func_name), sig);

        {
            let builder_ctx = self.jit.builder_ctx();
            let mut builder = FunctionBuilder::new(&mut func, builder_ctx);
            let entry = builder.create_block();
            builder.append_block_params_for_function_params(entry);
            builder.switch_to_block(entry);
            builder.seal_block(entry);

            let row_ptr = builder.block_params(entry)[0];
            let compiled = compile_expr_inner(&mut builder, self.schema, &expr, row_ptr)?;

            // NULL filter result → 0 (reject row)
            let result = if compiled.is_nullable {
                if let Some(null_flag) = compiled.null_flag {
                    let zero = builder.ins().iconst(cl_types::I8, 0);
                    let is_null = builder.ins().icmp_imm(IntCC::NotEqual, null_flag, 0);
                    builder.ins().select(is_null, zero, compiled.value)
                } else {
                    compiled.value
                }
            } else {
                compiled.value
            };

            builder.ins().return_(&[result]);
            builder.finalize();
        }

        let mut ctx = Context::for_function(func);
        self.jit
            .module()
            .define_function(func_id, &mut ctx)
            .map_err(|e| CompileError::Cranelift(Box::new(e)))?;
        self.jit.module().finalize_definitions().unwrap();

        let code_ptr = self.jit.module().get_finalized_function(func_id);
        // SAFETY: The generated function has the declared ABI signature.
        Ok(unsafe { std::mem::transmute::<*const u8, FilterFn>(code_ptr) })
    }

    /// Compiles a scalar expression into a native `ScalarFn`.
    ///
    /// The generated function has signature `fn(*const u8, *mut u8) -> u8`
    /// where the first argument is a pointer to an `EventRow` byte buffer and
    /// the second is a pointer to the output buffer. Returns 1 if the result
    /// is null, 0 if valid.
    ///
    /// # Errors
    ///
    /// Returns [`CompileError`] if the expression contains unsupported nodes.
    ///
    /// # Panics
    ///
    /// Panics if Cranelift fails to finalize function definitions (internal error).
    pub fn compile_scalar(
        &mut self,
        expr: &Expr,
        output_type: &FieldType,
    ) -> Result<ScalarFn, CompileError> {
        let expr = fold_constants(expr);
        let func_name = self.jit.next_func_name("scalar");

        let mut sig = self.jit.module().make_signature();
        sig.params.push(AbiParam::new(PTR_TYPE));
        sig.params.push(AbiParam::new(PTR_TYPE));
        sig.returns.push(AbiParam::new(cl_types::I8));

        let func_id = self.jit.module().declare_function(
            &func_name,
            cranelift_module::Linkage::Local,
            &sig,
        )?;

        let mut func = Function::with_name_signature(UserFuncName::testcase(&func_name), sig);

        {
            let builder_ctx = self.jit.builder_ctx();
            let mut builder = FunctionBuilder::new(&mut func, builder_ctx);
            let entry = builder.create_block();
            builder.append_block_params_for_function_params(entry);
            builder.switch_to_block(entry);
            builder.seal_block(entry);

            let row_ptr = builder.block_params(entry)[0];
            let out_ptr = builder.block_params(entry)[1];

            let compiled = compile_expr_inner(&mut builder, self.schema, &expr, row_ptr)?;

            // Write the value to the output pointer.
            let mem_flags = MemFlags::trusted();
            builder.ins().store(mem_flags, compiled.value, out_ptr, 0);

            // Return is_null flag.
            let is_null = if compiled.is_nullable {
                compiled
                    .null_flag
                    .unwrap_or_else(|| builder.ins().iconst(cl_types::I8, 0))
            } else {
                builder.ins().iconst(cl_types::I8, 0)
            };

            let is_null_i8 = if builder.func.dfg.value_type(is_null) == cl_types::I8 {
                is_null
            } else {
                builder.ins().ireduce(cl_types::I8, is_null)
            };

            _ = output_type;
            builder.ins().return_(&[is_null_i8]);
            builder.finalize();
        }

        let mut ctx = Context::for_function(func);
        self.jit
            .module()
            .define_function(func_id, &mut ctx)
            .map_err(|e| CompileError::Cranelift(Box::new(e)))?;
        self.jit.module().finalize_definitions().unwrap();

        let code_ptr = self.jit.module().get_finalized_function(func_id);
        // SAFETY: The generated function has the declared ABI signature.
        Ok(unsafe { std::mem::transmute::<*const u8, ScalarFn>(code_ptr) })
    }
}

// ---------------------------------------------------------------------------
// Free functions for IR generation — decoupled from JitContext borrow
// ---------------------------------------------------------------------------

/// Recursively compiles an expression node into Cranelift IR.
pub(crate) fn compile_expr_inner(
    builder: &mut FunctionBuilder,
    schema: &RowSchema,
    expr: &Expr,
    row_ptr: Value,
) -> Result<CompiledValue, CompileError> {
    match expr {
        Expr::Column(col) => compile_column(builder, schema, &col.name, row_ptr),
        Expr::Literal(scalar, _) => compile_literal(builder, scalar),
        Expr::BinaryExpr(binary) => compile_binary(builder, schema, binary, row_ptr),
        Expr::Not(inner) => compile_not(builder, schema, inner, row_ptr),
        Expr::IsNull(inner) => compile_is_null(builder, schema, inner, row_ptr, false),
        Expr::IsNotNull(inner) => compile_is_null(builder, schema, inner, row_ptr, true),
        Expr::Cast(cast) => compile_cast(builder, schema, &cast.expr, &cast.data_type, row_ptr),
        Expr::Case(case) => compile_case(builder, schema, case, row_ptr),
        other => Err(CompileError::UnsupportedExpr(format!("{other}"))),
    }
}

/// Compiles a column reference — loads the field value from the row pointer.
fn compile_column(
    builder: &mut FunctionBuilder,
    schema: &RowSchema,
    name: &str,
    row_ptr: Value,
) -> Result<CompiledValue, CompileError> {
    let field_idx = schema
        .arrow_schema()
        .index_of(name)
        .map_err(|_| CompileError::ColumnNotFound(name.to_string()))?;

    let layout = schema.field(field_idx);
    let cl_type = field_type_to_cranelift(layout.field_type);
    let mem_flags = MemFlags::trusted();

    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let offset = layout.offset as i32;
    let value = builder.ins().load(cl_type, mem_flags, row_ptr, offset);

    let arrow_field = &schema.arrow_schema().fields()[field_idx];
    let is_nullable = arrow_field.is_nullable();

    let null_flag = if is_nullable {
        Some(emit_null_check(builder, schema, field_idx, row_ptr))
    } else {
        None
    };

    Ok(CompiledValue {
        value,
        is_nullable,
        null_flag,
        value_type: layout.field_type,
    })
}

/// Compiles a scalar literal into a Cranelift constant.
fn compile_literal(
    builder: &mut FunctionBuilder,
    scalar: &ScalarValue,
) -> Result<CompiledValue, CompileError> {
    match scalar {
        ScalarValue::Boolean(Some(b)) => Ok(CompiledValue {
            value: builder.ins().iconst(cl_types::I8, i64::from(*b)),
            is_nullable: false,
            null_flag: None,
            value_type: FieldType::Bool,
        }),
        ScalarValue::Int8(Some(v)) => Ok(CompiledValue {
            value: builder.ins().iconst(cl_types::I8, i64::from(*v)),
            is_nullable: false,
            null_flag: None,
            value_type: FieldType::Int8,
        }),
        ScalarValue::Int16(Some(v)) => Ok(CompiledValue {
            value: builder.ins().iconst(cl_types::I16, i64::from(*v)),
            is_nullable: false,
            null_flag: None,
            value_type: FieldType::Int16,
        }),
        ScalarValue::Int32(Some(v)) => Ok(CompiledValue {
            value: builder.ins().iconst(cl_types::I32, i64::from(*v)),
            is_nullable: false,
            null_flag: None,
            value_type: FieldType::Int32,
        }),
        ScalarValue::Int64(Some(v)) => Ok(CompiledValue {
            value: builder.ins().iconst(cl_types::I64, *v),
            is_nullable: false,
            null_flag: None,
            value_type: FieldType::Int64,
        }),
        ScalarValue::UInt8(Some(v)) => Ok(CompiledValue {
            value: builder.ins().iconst(cl_types::I8, i64::from(*v)),
            is_nullable: false,
            null_flag: None,
            value_type: FieldType::UInt8,
        }),
        ScalarValue::UInt16(Some(v)) => Ok(CompiledValue {
            value: builder.ins().iconst(cl_types::I16, i64::from(*v)),
            is_nullable: false,
            null_flag: None,
            value_type: FieldType::UInt16,
        }),
        ScalarValue::UInt32(Some(v)) => Ok(CompiledValue {
            value: builder.ins().iconst(cl_types::I32, i64::from(*v)),
            is_nullable: false,
            null_flag: None,
            value_type: FieldType::UInt32,
        }),
        ScalarValue::UInt64(Some(v)) => Ok(CompiledValue {
            #[allow(clippy::cast_possible_wrap)]
            value: builder.ins().iconst(cl_types::I64, (*v).cast_signed()),
            is_nullable: false,
            null_flag: None,
            value_type: FieldType::UInt64,
        }),
        ScalarValue::Float32(Some(v)) => Ok(CompiledValue {
            value: builder.ins().f32const(*v),
            is_nullable: false,
            null_flag: None,
            value_type: FieldType::Float32,
        }),
        ScalarValue::Float64(Some(v)) => Ok(CompiledValue {
            value: builder.ins().f64const(*v),
            is_nullable: false,
            null_flag: None,
            value_type: FieldType::Float64,
        }),
        // Null literals: produce a zero value with null_flag = 1
        ScalarValue::Boolean(None) => Ok(null_value(builder, FieldType::Bool)),
        ScalarValue::Int64(None) => Ok(null_value(builder, FieldType::Int64)),
        ScalarValue::Float64(None) => Ok(null_value(builder, FieldType::Float64)),
        ScalarValue::Int32(None) => Ok(null_value(builder, FieldType::Int32)),
        _ => Err(CompileError::UnsupportedLiteral),
    }
}

/// Creates a null value of the given type.
pub(crate) fn null_value(builder: &mut FunctionBuilder, field_type: FieldType) -> CompiledValue {
    let cl_type = field_type_to_cranelift(field_type);
    let value = if cl_type.is_float() {
        if cl_type == cl_types::F32 {
            builder.ins().f32const(0.0)
        } else {
            builder.ins().f64const(0.0)
        }
    } else {
        builder.ins().iconst(cl_type, 0)
    };
    let null_flag = builder.ins().iconst(cl_types::I8, 1);
    CompiledValue {
        value,
        is_nullable: true,
        null_flag: Some(null_flag),
        value_type: field_type,
    }
}

/// Compiles a binary expression (arithmetic, comparison, boolean logic).
fn compile_binary(
    builder: &mut FunctionBuilder,
    schema: &RowSchema,
    binary: &BinaryExpr,
    row_ptr: Value,
) -> Result<CompiledValue, CompileError> {
    match binary.op {
        Operator::And => return compile_short_circuit_and(builder, schema, binary, row_ptr),
        Operator::Or => return compile_short_circuit_or(builder, schema, binary, row_ptr),
        _ => {}
    }

    let lhs = compile_expr_inner(builder, schema, &binary.left, row_ptr)?;
    let rhs = compile_expr_inner(builder, schema, &binary.right, row_ptr)?;

    let (is_nullable, merged_null) = merge_null_flags(builder, &lhs, &rhs);
    let result_value = emit_binary_op(builder, &lhs, &rhs, binary.op)?;

    Ok(CompiledValue {
        value: result_value.value,
        is_nullable,
        null_flag: merged_null,
        value_type: result_value.value_type,
    })
}

/// Emits the binary operation IR for the given operands and operator.
fn emit_binary_op(
    builder: &mut FunctionBuilder,
    lhs: &CompiledValue,
    rhs: &CompiledValue,
    op: Operator,
) -> Result<CompiledValue, CompileError> {
    let lhs_type = lhs.value_type;
    let cl_type = field_type_to_cranelift(lhs_type);

    if cl_type.is_int() {
        return emit_int_binary(builder, lhs, rhs, op);
    }
    if cl_type.is_float() {
        return emit_float_binary(builder, lhs, rhs, op);
    }

    Err(CompileError::UnsupportedBinaryOp(lhs_type, op))
}

/// Emits integer arithmetic and comparison operations.
fn emit_int_binary(
    builder: &mut FunctionBuilder,
    lhs: &CompiledValue,
    rhs: &CompiledValue,
    op: Operator,
) -> Result<CompiledValue, CompileError> {
    let lhs_type = lhs.value_type;
    let is_signed = is_signed_type(lhs_type);

    let value = match op {
        Operator::Plus => builder.ins().iadd(lhs.value, rhs.value),
        Operator::Minus => builder.ins().isub(lhs.value, rhs.value),
        Operator::Multiply => builder.ins().imul(lhs.value, rhs.value),
        Operator::Divide => {
            if is_signed {
                builder.ins().sdiv(lhs.value, rhs.value)
            } else {
                builder.ins().udiv(lhs.value, rhs.value)
            }
        }
        Operator::Modulo => {
            if is_signed {
                builder.ins().srem(lhs.value, rhs.value)
            } else {
                builder.ins().urem(lhs.value, rhs.value)
            }
        }
        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq => {
            let cc = int_cmp_cond(op, is_signed);
            let cmp_val = builder.ins().icmp(cc, lhs.value, rhs.value);
            return Ok(CompiledValue {
                value: cmp_val,
                is_nullable: false,
                null_flag: None,
                value_type: FieldType::Bool,
            });
        }
        _ => return Err(CompileError::UnsupportedBinaryOp(lhs_type, op)),
    };

    Ok(CompiledValue {
        value,
        is_nullable: false,
        null_flag: None,
        value_type: lhs_type,
    })
}

/// Emits floating-point arithmetic and comparison operations.
fn emit_float_binary(
    builder: &mut FunctionBuilder,
    lhs: &CompiledValue,
    rhs: &CompiledValue,
    op: Operator,
) -> Result<CompiledValue, CompileError> {
    let lhs_type = lhs.value_type;

    let value = match op {
        Operator::Plus => builder.ins().fadd(lhs.value, rhs.value),
        Operator::Minus => builder.ins().fsub(lhs.value, rhs.value),
        Operator::Multiply => builder.ins().fmul(lhs.value, rhs.value),
        Operator::Divide => builder.ins().fdiv(lhs.value, rhs.value),
        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq => {
            let cc = float_cmp_cond(op);
            let cmp_val = builder.ins().fcmp(cc, lhs.value, rhs.value);
            return Ok(CompiledValue {
                value: cmp_val,
                is_nullable: false,
                null_flag: None,
                value_type: FieldType::Bool,
            });
        }
        _ => return Err(CompileError::UnsupportedBinaryOp(lhs_type, op)),
    };

    Ok(CompiledValue {
        value,
        is_nullable: false,
        null_flag: None,
        value_type: lhs_type,
    })
}

/// Compiles `NOT expr` — flips a boolean i8 value.
fn compile_not(
    builder: &mut FunctionBuilder,
    schema: &RowSchema,
    inner: &Expr,
    row_ptr: Value,
) -> Result<CompiledValue, CompileError> {
    let compiled = compile_expr_inner(builder, schema, inner, row_ptr)?;
    let one = builder.ins().iconst(cl_types::I8, 1);
    let flipped = builder.ins().bxor(compiled.value, one);
    Ok(CompiledValue {
        value: flipped,
        is_nullable: compiled.is_nullable,
        null_flag: compiled.null_flag,
        value_type: FieldType::Bool,
    })
}

/// Compiles `IS NULL` / `IS NOT NULL`.
fn compile_is_null(
    builder: &mut FunctionBuilder,
    schema: &RowSchema,
    inner: &Expr,
    row_ptr: Value,
    invert: bool,
) -> Result<CompiledValue, CompileError> {
    let compiled = compile_expr_inner(builder, schema, inner, row_ptr)?;

    let result = if let Some(nf) = compiled.null_flag {
        if invert {
            builder.ins().icmp_imm(IntCC::Equal, nf, 0)
        } else {
            builder.ins().icmp_imm(IntCC::NotEqual, nf, 0)
        }
    } else {
        builder.ins().iconst(cl_types::I8, i64::from(invert))
    };

    Ok(CompiledValue {
        value: result,
        is_nullable: false,
        null_flag: None,
        value_type: FieldType::Bool,
    })
}

/// Compiles a CAST expression between numeric types.
fn compile_cast(
    builder: &mut FunctionBuilder,
    schema: &RowSchema,
    inner: &Expr,
    target_dt: &arrow_schema::DataType,
    row_ptr: Value,
) -> Result<CompiledValue, CompileError> {
    let compiled = compile_expr_inner(builder, schema, inner, row_ptr)?;
    let out_type = FieldType::from_arrow(target_dt)
        .ok_or_else(|| CompileError::UnsupportedExpr(format!("CAST to {target_dt}")))?;
    let target_cl = field_type_to_cranelift(out_type);
    let source_cl = field_type_to_cranelift(compiled.value_type);

    if source_cl == target_cl {
        return Ok(CompiledValue {
            value_type: out_type,
            ..compiled
        });
    }

    let value = if source_cl.is_int() && target_cl.is_int() {
        if target_cl.bits() > source_cl.bits() {
            if is_signed_type(compiled.value_type) {
                builder.ins().sextend(target_cl, compiled.value)
            } else {
                builder.ins().uextend(target_cl, compiled.value)
            }
        } else {
            builder.ins().ireduce(target_cl, compiled.value)
        }
    } else if source_cl.is_int() && target_cl.is_float() {
        if is_signed_type(compiled.value_type) {
            builder.ins().fcvt_from_sint(target_cl, compiled.value)
        } else {
            builder.ins().fcvt_from_uint(target_cl, compiled.value)
        }
    } else if source_cl.is_float() && target_cl.is_int() {
        if is_signed_type(out_type) {
            builder.ins().fcvt_to_sint(target_cl, compiled.value)
        } else {
            builder.ins().fcvt_to_uint(target_cl, compiled.value)
        }
    } else if source_cl.is_float() && target_cl.is_float() {
        if target_cl.bits() > source_cl.bits() {
            builder.ins().fpromote(target_cl, compiled.value)
        } else {
            builder.ins().fdemote(target_cl, compiled.value)
        }
    } else {
        return Err(CompileError::UnsupportedExpr(format!(
            "CAST from {source_cl} to {target_cl}"
        )));
    };

    Ok(CompiledValue {
        value,
        is_nullable: compiled.is_nullable,
        null_flag: compiled.null_flag,
        value_type: out_type,
    })
}

/// Compiles a CASE WHEN expression.
fn compile_case(
    builder: &mut FunctionBuilder,
    schema: &RowSchema,
    case: &datafusion_expr::Case,
    row_ptr: Value,
) -> Result<CompiledValue, CompileError> {
    let merge_block = builder.create_block();

    if case.when_then_expr.is_empty() {
        return Err(CompileError::UnsupportedExpr("empty CASE".to_string()));
    }

    let first_then = &case.when_then_expr[0].1;
    let result_type = infer_expr_type(schema, first_then)?;
    let cl_type = field_type_to_cranelift(result_type);
    builder.append_block_param(merge_block, cl_type);
    builder.append_block_param(merge_block, cl_types::I8);

    for (when_expr, then_expr) in &case.when_then_expr {
        let then_block = builder.create_block();
        let else_block = builder.create_block();

        let cond = compile_expr_inner(builder, schema, when_expr, row_ptr)?;
        builder
            .ins()
            .brif(cond.value, then_block, &[], else_block, &[]);

        builder.switch_to_block(then_block);
        builder.seal_block(then_block);
        let then_val = compile_expr_inner(builder, schema, then_expr, row_ptr)?;
        let then_null = then_val
            .null_flag
            .unwrap_or_else(|| builder.ins().iconst(cl_types::I8, 0));
        builder.ins().jump(
            merge_block,
            &[BlockArg::Value(then_val.value), BlockArg::Value(then_null)],
        );

        builder.switch_to_block(else_block);
        builder.seal_block(else_block);
    }

    let else_val = if let Some(else_expr) = &case.else_expr {
        compile_expr_inner(builder, schema, else_expr, row_ptr)?
    } else {
        null_value(builder, result_type)
    };
    let else_null = else_val
        .null_flag
        .unwrap_or_else(|| builder.ins().iconst(cl_types::I8, 0));
    builder.ins().jump(
        merge_block,
        &[BlockArg::Value(else_val.value), BlockArg::Value(else_null)],
    );

    builder.switch_to_block(merge_block);
    builder.seal_block(merge_block);

    let result_value = builder.block_params(merge_block)[0];
    let result_null = builder.block_params(merge_block)[1];

    Ok(CompiledValue {
        value: result_value,
        is_nullable: true,
        null_flag: Some(result_null),
        value_type: result_type,
    })
}

/// Compiles AND with short-circuit: if LHS is false, skip RHS.
fn compile_short_circuit_and(
    builder: &mut FunctionBuilder,
    schema: &RowSchema,
    binary: &BinaryExpr,
    row_ptr: Value,
) -> Result<CompiledValue, CompileError> {
    let merge_block = builder.create_block();
    builder.append_block_param(merge_block, cl_types::I8);
    let rhs_block = builder.create_block();

    let lhs = compile_expr_inner(builder, schema, &binary.left, row_ptr)?;

    let false_val = builder.ins().iconst(cl_types::I8, 0);
    builder.ins().brif(
        lhs.value,
        rhs_block,
        &[],
        merge_block,
        &[BlockArg::Value(false_val)],
    );

    builder.switch_to_block(rhs_block);
    builder.seal_block(rhs_block);
    let rhs = compile_expr_inner(builder, schema, &binary.right, row_ptr)?;
    builder
        .ins()
        .jump(merge_block, &[BlockArg::Value(rhs.value)]);

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

/// Compiles OR with short-circuit: if LHS is true, skip RHS.
fn compile_short_circuit_or(
    builder: &mut FunctionBuilder,
    schema: &RowSchema,
    binary: &BinaryExpr,
    row_ptr: Value,
) -> Result<CompiledValue, CompileError> {
    let merge_block = builder.create_block();
    builder.append_block_param(merge_block, cl_types::I8);
    let rhs_block = builder.create_block();

    let lhs = compile_expr_inner(builder, schema, &binary.left, row_ptr)?;

    let true_val = builder.ins().iconst(cl_types::I8, 1);
    builder.ins().brif(
        lhs.value,
        merge_block,
        &[BlockArg::Value(true_val)],
        rhs_block,
        &[],
    );

    builder.switch_to_block(rhs_block);
    builder.seal_block(rhs_block);
    let rhs = compile_expr_inner(builder, schema, &binary.right, row_ptr)?;
    builder
        .ins()
        .jump(merge_block, &[BlockArg::Value(rhs.value)]);

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

/// Loads the null bitmap bit for the given field index.
pub(crate) fn emit_null_check(
    builder: &mut FunctionBuilder,
    schema: &RowSchema,
    field_idx: usize,
    row_ptr: Value,
) -> Value {
    let layout = schema.field(field_idx);
    let null_bit = layout.null_bit;
    let byte_idx = RowSchema::header_size() + null_bit / 8;
    let bit_idx = null_bit % 8;

    let mem_flags = MemFlags::trusted();
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let byte_offset = byte_idx as i32;
    let byte_val = builder
        .ins()
        .load(cl_types::I8, mem_flags, row_ptr, byte_offset);
    let mask = builder.ins().iconst(cl_types::I8, 1 << bit_idx);
    builder.ins().band(byte_val, mask)
}

/// Merges null flags from two operands (SQL null propagation).
pub(crate) fn merge_null_flags(
    builder: &mut FunctionBuilder,
    lhs: &CompiledValue,
    rhs: &CompiledValue,
) -> (bool, Option<Value>) {
    match (lhs.null_flag, rhs.null_flag) {
        (Some(l), Some(r)) => {
            let merged = builder.ins().bor(l, r);
            (true, Some(merged))
        }
        (Some(f), None) | (None, Some(f)) => (true, Some(f)),
        (None, None) => (false, None),
    }
}

/// Infers the output [`FieldType`] of an expression without compiling it.
pub(crate) fn infer_expr_type(schema: &RowSchema, expr: &Expr) -> Result<FieldType, CompileError> {
    match expr {
        Expr::Column(col) => {
            let idx = schema
                .arrow_schema()
                .index_of(&col.name)
                .map_err(|_| CompileError::ColumnNotFound(col.name.clone()))?;
            Ok(schema.field(idx).field_type)
        }
        Expr::Literal(scalar, _) => match scalar {
            ScalarValue::Boolean(_) => Ok(FieldType::Bool),
            ScalarValue::Int8(_) => Ok(FieldType::Int8),
            ScalarValue::Int16(_) => Ok(FieldType::Int16),
            ScalarValue::Int32(_) => Ok(FieldType::Int32),
            ScalarValue::Int64(_) => Ok(FieldType::Int64),
            ScalarValue::UInt8(_) => Ok(FieldType::UInt8),
            ScalarValue::UInt16(_) => Ok(FieldType::UInt16),
            ScalarValue::UInt32(_) => Ok(FieldType::UInt32),
            ScalarValue::UInt64(_) => Ok(FieldType::UInt64),
            ScalarValue::Float32(_) => Ok(FieldType::Float32),
            ScalarValue::Float64(_) => Ok(FieldType::Float64),
            _ => Err(CompileError::UnsupportedLiteral),
        },
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::And
            | Operator::Or => Ok(FieldType::Bool),
            _ => infer_expr_type(schema, &binary.left),
        },
        Expr::Not(_) | Expr::IsNull(_) | Expr::IsNotNull(_) => Ok(FieldType::Bool),
        Expr::Cast(cast) => FieldType::from_arrow(&cast.data_type)
            .ok_or_else(|| CompileError::UnsupportedExpr(format!("CAST to {}", cast.data_type))),
        Expr::Case(case) => {
            if let Some((_, then_expr)) = case.when_then_expr.first() {
                infer_expr_type(schema, then_expr)
            } else {
                Err(CompileError::UnsupportedExpr("empty CASE".to_string()))
            }
        }
        other => Err(CompileError::UnsupportedExpr(format!("{other}"))),
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Maps a [`FieldType`] to the corresponding Cranelift IR type.
pub(crate) fn field_type_to_cranelift(ft: FieldType) -> CraneliftType {
    match ft {
        FieldType::Bool | FieldType::Int8 | FieldType::UInt8 => cl_types::I8,
        FieldType::Int16 | FieldType::UInt16 => cl_types::I16,
        FieldType::Int32 | FieldType::UInt32 => cl_types::I32,
        FieldType::Float32 => cl_types::F32,
        FieldType::Int64
        | FieldType::UInt64
        | FieldType::TimestampMicros
        | FieldType::Utf8
        | FieldType::Binary => cl_types::I64,
        FieldType::Float64 => cl_types::F64,
    }
}

/// Returns `true` for signed integer types.
fn is_signed_type(ft: FieldType) -> bool {
    matches!(
        ft,
        FieldType::Int8
            | FieldType::Int16
            | FieldType::Int32
            | FieldType::Int64
            | FieldType::TimestampMicros
    )
}

/// Maps a comparison [`Operator`] to a Cranelift integer condition code.
fn int_cmp_cond(op: Operator, signed: bool) -> IntCC {
    match (op, signed) {
        (Operator::Eq, _) => IntCC::Equal,
        (Operator::NotEq, _) => IntCC::NotEqual,
        (Operator::Lt, true) => IntCC::SignedLessThan,
        (Operator::Lt, false) => IntCC::UnsignedLessThan,
        (Operator::LtEq, true) => IntCC::SignedLessThanOrEqual,
        (Operator::LtEq, false) => IntCC::UnsignedLessThanOrEqual,
        (Operator::Gt, true) => IntCC::SignedGreaterThan,
        (Operator::Gt, false) => IntCC::UnsignedGreaterThan,
        (Operator::GtEq, true) => IntCC::SignedGreaterThanOrEqual,
        (Operator::GtEq, false) => IntCC::UnsignedGreaterThanOrEqual,
        _ => unreachable!("int_cmp_cond called with non-comparison operator"),
    }
}

/// Maps a comparison [`Operator`] to a Cranelift float condition code.
fn float_cmp_cond(op: Operator) -> FloatCC {
    match op {
        Operator::Eq => FloatCC::Equal,
        Operator::NotEq => FloatCC::NotEqual,
        Operator::Lt => FloatCC::LessThan,
        Operator::LtEq => FloatCC::LessThanOrEqual,
        Operator::Gt => FloatCC::GreaterThan,
        Operator::GtEq => FloatCC::GreaterThanOrEqual,
        _ => unreachable!("float_cmp_cond called with non-comparison operator"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::row::{MutableEventRow, RowSchema};
    use arrow_schema::{DataType, Field, Schema};
    use bumpalo::Bump;
    use datafusion_expr::{col, lit};
    use std::sync::Arc;

    fn make_schema(fields: Vec<(&str, DataType, bool)>) -> Arc<Schema> {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dt, nullable)| Field::new(name, dt, nullable))
                .collect::<Vec<_>>(),
        ))
    }

    fn make_row_bytes<'a>(
        arena: &'a Bump,
        schema: &'a RowSchema,
        values: &[(usize, i64)],
        nulls: &[usize],
    ) -> &'a [u8] {
        let mut row = MutableEventRow::new_in(arena, schema, 0);
        for &(idx, val) in values {
            row.set_i64(idx, val);
        }
        for &idx in nulls {
            row.set_null(idx, true);
        }
        row.freeze().data()
    }

    // ---- Filter: column vs literal comparisons ----

    #[test]
    fn filter_col_gt_literal() {
        let arrow = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = col("val").gt(lit(100_i64));
        let filter = compiler.compile_filter(&expr).unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 200)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 1);

        let bytes = make_row_bytes(&arena, &rs, &[(0, 50)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 0);
    }

    #[test]
    fn filter_col_eq_literal() {
        let arrow = make_schema(vec![("id", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = col("id").eq(lit(42_i64));
        let filter = compiler.compile_filter(&expr).unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 42)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 1);

        let bytes = make_row_bytes(&arena, &rs, &[(0, 43)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 0);
    }

    #[test]
    fn filter_col_lt_literal() {
        let arrow = make_schema(vec![("x", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = col("x").lt(lit(10_i64));
        let filter = compiler.compile_filter(&expr).unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 5)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 1);

        let bytes = make_row_bytes(&arena, &rs, &[(0, 15)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 0);
    }

    #[test]
    fn filter_all_comparison_ops() {
        let arrow = make_schema(vec![("x", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let arena = Bump::new();

        // lt_eq
        let f = compiler
            .compile_filter(&col("x").lt_eq(lit(10_i64)))
            .unwrap();
        let b = make_row_bytes(&arena, &rs, &[(0, 10)], &[]);
        assert_eq!(unsafe { f(b.as_ptr()) }, 1);
        let b = make_row_bytes(&arena, &rs, &[(0, 11)], &[]);
        assert_eq!(unsafe { f(b.as_ptr()) }, 0);

        // gt_eq
        let f = compiler
            .compile_filter(&col("x").gt_eq(lit(10_i64)))
            .unwrap();
        let b = make_row_bytes(&arena, &rs, &[(0, 10)], &[]);
        assert_eq!(unsafe { f(b.as_ptr()) }, 1);
        let b = make_row_bytes(&arena, &rs, &[(0, 9)], &[]);
        assert_eq!(unsafe { f(b.as_ptr()) }, 0);

        // not_eq
        let f = compiler
            .compile_filter(&col("x").not_eq(lit(42_i64)))
            .unwrap();
        let b = make_row_bytes(&arena, &rs, &[(0, 43)], &[]);
        assert_eq!(unsafe { f(b.as_ptr()) }, 1);
        let b = make_row_bytes(&arena, &rs, &[(0, 42)], &[]);
        assert_eq!(unsafe { f(b.as_ptr()) }, 0);
    }

    // ---- Filter: compound boolean ----

    #[test]
    fn filter_and_compound() {
        let arrow = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Int64, false),
        ]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = col("a").gt(lit(1_i64)).and(col("b").lt(lit(10_i64)));
        let filter = compiler.compile_filter(&expr).unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 5), (1, 3)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 1);

        let bytes = make_row_bytes(&arena, &rs, &[(0, 0), (1, 3)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 0);

        let bytes = make_row_bytes(&arena, &rs, &[(0, 5), (1, 20)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 0);
    }

    #[test]
    fn filter_or_compound() {
        let arrow = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Int64, false),
        ]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = col("a").eq(lit(42_i64)).or(col("b").eq(lit(99_i64)));
        let filter = compiler.compile_filter(&expr).unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 42), (1, 0)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 1);

        let bytes = make_row_bytes(&arena, &rs, &[(0, 0), (1, 99)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 1);

        let bytes = make_row_bytes(&arena, &rs, &[(0, 0), (1, 0)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 0);
    }

    #[test]
    fn filter_nested_and_or() {
        let arrow = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Int64, false),
            ("c", DataType::Int64, false),
        ]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = col("a")
            .gt(lit(0_i64))
            .and(col("b").gt(lit(0_i64)))
            .or(col("c").gt(lit(0_i64)));
        let filter = compiler.compile_filter(&expr).unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 1), (1, 1), (2, 0)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 1);

        let bytes = make_row_bytes(&arena, &rs, &[(0, 0), (1, 0), (2, 1)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 1);

        let bytes = make_row_bytes(&arena, &rs, &[(0, 0), (1, 0), (2, 0)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 0);
    }

    #[test]
    fn filter_not() {
        let arrow = make_schema(vec![("x", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = Expr::Not(Box::new(col("x").gt(lit(10_i64))));
        let filter = compiler.compile_filter(&expr).unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 5)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 1);

        let bytes = make_row_bytes(&arena, &rs, &[(0, 20)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 0);
    }

    // ---- Scalar: arithmetic ----

    #[test]
    fn scalar_add_i64() {
        let arrow = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = col("val") + lit(10_i64);
        let scalar = compiler.compile_scalar(&expr, &FieldType::Int64).unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 32)], &[]);
        let mut output = [0u8; 8];
        let is_null = unsafe { scalar(bytes.as_ptr(), output.as_mut_ptr()) };
        assert_eq!(is_null, 0);
        assert_eq!(i64::from_le_bytes(output), 42);
    }

    #[test]
    fn scalar_multiply_i64() {
        let arrow = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = col("val") * lit(7_i64);
        let scalar = compiler.compile_scalar(&expr, &FieldType::Int64).unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 6)], &[]);
        let mut output = [0u8; 8];
        let is_null = unsafe { scalar(bytes.as_ptr(), output.as_mut_ptr()) };
        assert_eq!(is_null, 0);
        assert_eq!(i64::from_le_bytes(output), 42);
    }

    #[test]
    fn scalar_sub_div_mod() {
        let arrow = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);
        let arena = Bump::new();
        let mut output = [0u8; 8];

        // sub
        let s = compiler
            .compile_scalar(&(col("val") - lit(8_i64)), &FieldType::Int64)
            .unwrap();
        let b = make_row_bytes(&arena, &rs, &[(0, 50)], &[]);
        assert_eq!(unsafe { s(b.as_ptr(), output.as_mut_ptr()) }, 0);
        assert_eq!(i64::from_le_bytes(output), 42);

        // div
        let s = compiler
            .compile_scalar(&(col("val") / lit(2_i64)), &FieldType::Int64)
            .unwrap();
        let b = make_row_bytes(&arena, &rs, &[(0, 84)], &[]);
        assert_eq!(unsafe { s(b.as_ptr(), output.as_mut_ptr()) }, 0);
        assert_eq!(i64::from_le_bytes(output), 42);

        // mod
        let s = compiler
            .compile_scalar(&(col("val") % lit(10_i64)), &FieldType::Int64)
            .unwrap();
        let b = make_row_bytes(&arena, &rs, &[(0, 42)], &[]);
        assert_eq!(unsafe { s(b.as_ptr(), output.as_mut_ptr()) }, 0);
        assert_eq!(i64::from_le_bytes(output), 2);
    }

    #[test]
    fn scalar_f64_add() {
        let arrow = make_schema(vec![("val", DataType::Float64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = col("val") + lit(1.5_f64);
        let scalar = compiler.compile_scalar(&expr, &FieldType::Float64).unwrap();

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_f64(0, 2.5);
        let frozen = row.freeze();
        let mut output = [0u8; 8];
        let is_null = unsafe { scalar(frozen.data().as_ptr(), output.as_mut_ptr()) };
        assert_eq!(is_null, 0);
        assert!((f64::from_le_bytes(output) - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn scalar_column_passthrough() {
        let arrow = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let scalar = compiler
            .compile_scalar(&col("val"), &FieldType::Int64)
            .unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 999)], &[]);
        let mut output = [0u8; 8];
        assert_eq!(unsafe { scalar(bytes.as_ptr(), output.as_mut_ptr()) }, 0);
        assert_eq!(i64::from_le_bytes(output), 999);
    }

    // ---- Null handling ----

    #[test]
    fn filter_null_column_rejects() {
        let arrow = make_schema(vec![("val", DataType::Int64, true)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let filter = compiler
            .compile_filter(&col("val").gt(lit(10_i64)))
            .unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 999)], &[0]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 0);
    }

    #[test]
    fn scalar_null_propagation() {
        let arrow = make_schema(vec![("val", DataType::Int64, true)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let scalar = compiler
            .compile_scalar(&(col("val") + lit(10_i64)), &FieldType::Int64)
            .unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 0)], &[0]);
        let mut output = [0u8; 8];
        assert_ne!(unsafe { scalar(bytes.as_ptr(), output.as_mut_ptr()) }, 0);
    }

    #[test]
    fn filter_is_null() {
        let arrow = make_schema(vec![("val", DataType::Int64, true)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let filter = compiler.compile_filter(&col("val").is_null()).unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 0)], &[0]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 1);

        let bytes = make_row_bytes(&arena, &rs, &[(0, 42)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 0);
    }

    #[test]
    fn filter_is_not_null() {
        let arrow = make_schema(vec![("val", DataType::Int64, true)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let filter = compiler.compile_filter(&col("val").is_not_null()).unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 42)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 1);

        let bytes = make_row_bytes(&arena, &rs, &[(0, 0)], &[0]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 0);
    }

    #[test]
    fn null_literal_produces_null() {
        let arrow = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("val")),
            Operator::Plus,
            Box::new(Expr::Literal(ScalarValue::Int64(None), None)),
        ));
        let scalar = compiler.compile_scalar(&expr, &FieldType::Int64).unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 42)], &[]);
        let mut output = [0u8; 8];
        assert_ne!(unsafe { scalar(bytes.as_ptr(), output.as_mut_ptr()) }, 0);
    }

    #[test]
    fn null_and_true_short_circuits_false() {
        let arrow = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Literal(ScalarValue::Boolean(None), None)),
            Operator::And,
            Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)), None)),
        ));
        let filter = compiler.compile_filter(&expr).unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 0)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 0);
    }

    // ---- CAST ----

    #[test]
    fn cast_i32_to_i64() {
        let arrow = make_schema(vec![("val", DataType::Int32, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = Expr::Cast(datafusion_expr::Cast {
            expr: Box::new(col("val")),
            data_type: DataType::Int64,
        });
        let scalar = compiler.compile_scalar(&expr, &FieldType::Int64).unwrap();

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_i32(0, -42);
        let frozen = row.freeze();
        let mut output = [0u8; 8];
        assert_eq!(
            unsafe { scalar(frozen.data().as_ptr(), output.as_mut_ptr()) },
            0
        );
        assert_eq!(i64::from_le_bytes(output), -42);
    }

    #[test]
    fn cast_i64_to_f64() {
        let arrow = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = Expr::Cast(datafusion_expr::Cast {
            expr: Box::new(col("val")),
            data_type: DataType::Float64,
        });
        let scalar = compiler.compile_scalar(&expr, &FieldType::Float64).unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 42)], &[]);
        let mut output = [0u8; 8];
        assert_eq!(unsafe { scalar(bytes.as_ptr(), output.as_mut_ptr()) }, 0);
        assert!((f64::from_le_bytes(output) - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn cast_f64_to_i64() {
        let arrow = make_schema(vec![("val", DataType::Float64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = Expr::Cast(datafusion_expr::Cast {
            expr: Box::new(col("val")),
            data_type: DataType::Int64,
        });
        let scalar = compiler.compile_scalar(&expr, &FieldType::Int64).unwrap();

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_f64(0, 42.9);
        let frozen = row.freeze();
        let mut output = [0u8; 8];
        assert_eq!(
            unsafe { scalar(frozen.data().as_ptr(), output.as_mut_ptr()) },
            0
        );
        assert_eq!(i64::from_le_bytes(output), 42);
    }

    #[test]
    fn cast_bool_to_i64() {
        let arrow = make_schema(vec![("flag", DataType::Boolean, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = Expr::Cast(datafusion_expr::Cast {
            expr: Box::new(col("flag")),
            data_type: DataType::Int64,
        });
        let scalar = compiler.compile_scalar(&expr, &FieldType::Int64).unwrap();

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_bool(0, true);
        let frozen = row.freeze();
        let mut output = [0u8; 8];
        assert_eq!(
            unsafe { scalar(frozen.data().as_ptr(), output.as_mut_ptr()) },
            0
        );
        assert_eq!(i64::from_le_bytes(output), 1);
    }

    // ---- CASE/WHEN ----

    #[test]
    fn case_simple() {
        let arrow = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = Expr::Case(datafusion_expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(col("val").gt(lit(10_i64))), Box::new(lit(1_i64)))],
            else_expr: Some(Box::new(lit(0_i64))),
        });
        let scalar = compiler.compile_scalar(&expr, &FieldType::Int64).unwrap();

        let arena = Bump::new();
        let mut output = [0u8; 8];

        let bytes = make_row_bytes(&arena, &rs, &[(0, 20)], &[]);
        unsafe { scalar(bytes.as_ptr(), output.as_mut_ptr()) };
        assert_eq!(i64::from_le_bytes(output), 1);

        let bytes = make_row_bytes(&arena, &rs, &[(0, 5)], &[]);
        unsafe { scalar(bytes.as_ptr(), output.as_mut_ptr()) };
        assert_eq!(i64::from_le_bytes(output), 0);
    }

    #[test]
    fn case_multiple_whens() {
        let arrow = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = Expr::Case(datafusion_expr::Case {
            expr: None,
            when_then_expr: vec![
                (Box::new(col("val").gt(lit(100_i64))), Box::new(lit(3_i64))),
                (Box::new(col("val").gt(lit(10_i64))), Box::new(lit(2_i64))),
            ],
            else_expr: Some(Box::new(lit(1_i64))),
        });
        let scalar = compiler.compile_scalar(&expr, &FieldType::Int64).unwrap();

        let arena = Bump::new();
        let mut output = [0u8; 8];

        let b = make_row_bytes(&arena, &rs, &[(0, 200)], &[]);
        unsafe { scalar(b.as_ptr(), output.as_mut_ptr()) };
        assert_eq!(i64::from_le_bytes(output), 3);

        let b = make_row_bytes(&arena, &rs, &[(0, 50)], &[]);
        unsafe { scalar(b.as_ptr(), output.as_mut_ptr()) };
        assert_eq!(i64::from_le_bytes(output), 2);

        let b = make_row_bytes(&arena, &rs, &[(0, 5)], &[]);
        unsafe { scalar(b.as_ptr(), output.as_mut_ptr()) };
        assert_eq!(i64::from_le_bytes(output), 1);
    }

    #[test]
    fn case_no_else_returns_null() {
        let arrow = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = Expr::Case(datafusion_expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(col("val").gt(lit(100_i64))), Box::new(lit(1_i64)))],
            else_expr: None,
        });
        let scalar = compiler.compile_scalar(&expr, &FieldType::Int64).unwrap();

        let arena = Bump::new();
        let mut output = [0u8; 8];
        let bytes = make_row_bytes(&arena, &rs, &[(0, 5)], &[]);
        let is_null = unsafe { scalar(bytes.as_ptr(), output.as_mut_ptr()) };
        assert_ne!(is_null, 0);
    }

    // ---- Fallback / error cases ----

    #[test]
    fn unsupported_expr_error() {
        let arrow = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = Expr::Unnest(datafusion_expr::expr::Unnest::new(col("val")));
        assert!(compiler.compile_filter(&expr).is_err());
    }

    #[test]
    fn column_not_found_error() {
        let arrow = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let result = compiler.compile_filter(&col("nonexistent").gt(lit(1_i64)));
        assert!(matches!(result, Err(CompileError::ColumnNotFound(_))));
    }

    // ---- Float comparison ----

    #[test]
    fn filter_f64_comparison() {
        let arrow = make_schema(vec![("price", DataType::Float64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let filter = compiler
            .compile_filter(&col("price").gt(lit(100.0_f64)))
            .unwrap();

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_f64(0, 150.0);
        assert_eq!(unsafe { filter(row.freeze().data().as_ptr()) }, 1);

        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_f64(0, 50.0);
        assert_eq!(unsafe { filter(row.freeze().data().as_ptr()) }, 0);
    }

    // ---- Multi-column ----

    #[test]
    fn scalar_two_columns_add() {
        let arrow = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Int64, false),
        ]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let scalar = compiler
            .compile_scalar(&(col("a") + col("b")), &FieldType::Int64)
            .unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 30), (1, 12)], &[]);
        let mut output = [0u8; 8];
        unsafe { scalar(bytes.as_ptr(), output.as_mut_ptr()) };
        assert_eq!(i64::from_le_bytes(output), 42);
    }

    // ---- Multiple compilations ----

    #[test]
    fn multiple_functions_same_context() {
        let arrow = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let f1 = compiler
            .compile_filter(&col("val").gt(lit(10_i64)))
            .unwrap();
        let f2 = compiler
            .compile_filter(&col("val").lt(lit(100_i64)))
            .unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 50)], &[]);
        assert_eq!(unsafe { f1(bytes.as_ptr()) }, 1);
        assert_eq!(unsafe { f2(bytes.as_ptr()) }, 1);

        let bytes = make_row_bytes(&arena, &rs, &[(0, 5)], &[]);
        assert_eq!(unsafe { f1(bytes.as_ptr()) }, 0);
        assert_eq!(unsafe { f2(bytes.as_ptr()) }, 1);
    }

    // ---- Literal-only ----

    #[test]
    fn filter_literal_true_false() {
        let arrow = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);
        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 0)], &[]);

        let ft = compiler.compile_filter(&lit(true)).unwrap();
        assert_eq!(unsafe { ft(bytes.as_ptr()) }, 1);

        let ff = compiler.compile_filter(&lit(false)).unwrap();
        assert_eq!(unsafe { ff(bytes.as_ptr()) }, 0);
    }

    // ---- Constant folding integration ----

    #[test]
    fn constant_folding_in_compile() {
        let arrow = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&arrow).unwrap();
        let mut jit = JitContext::new().unwrap();
        let mut compiler = ExprCompiler::new(&mut jit, &rs);

        let expr = col("val").gt(lit(2_i64) + lit(3_i64));
        let filter = compiler.compile_filter(&expr).unwrap();

        let arena = Bump::new();
        let bytes = make_row_bytes(&arena, &rs, &[(0, 10)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 1);

        let bytes = make_row_bytes(&arena, &rs, &[(0, 3)], &[]);
        assert_eq!(unsafe { filter(bytes.as_ptr()) }, 0);
    }
}
