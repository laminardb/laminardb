//! Cranelift code generation for compiled pipelines.
//!
//! [`PipelineCompiler`] takes a [`Pipeline`] (a chain of Filter, Project, and
//! `KeyExtract` stages) and generates a single native function that processes
//! one input row and writes the result to an output row.
//!
//! The generated function has signature:
//! ```text
//! fn(input_row: *const u8, output_row: *mut u8) -> u8
//! ```
//!
//! Returns `0` (Drop), `1` (Emit), or `2` (Error).

use std::sync::Arc;

use cranelift_codegen::ir::types::{self as cl_types};
use cranelift_codegen::ir::{AbiParam, Function, InstBuilder, MemFlags, UserFuncName, Value};
use cranelift_codegen::Context;
use cranelift_frontend::FunctionBuilder;
use cranelift_module::Module;

use super::error::CompileError;
use super::expr::{compile_expr_inner, CompiledValue};
use super::jit::JitContext;
use super::pipeline::{CompiledPipeline, Pipeline, PipelineFn, PipelineStage};
use super::row::RowSchema;

/// Pointer type for the target architecture.
const PTR_TYPE: cranelift_codegen::ir::Type = cl_types::I64;

/// Compiles [`Pipeline`]s into native functions via Cranelift.
pub struct PipelineCompiler<'a> {
    jit: &'a mut JitContext,
}

impl<'a> PipelineCompiler<'a> {
    /// Creates a new pipeline compiler using the given JIT context.
    pub fn new(jit: &'a mut JitContext) -> Self {
        Self { jit }
    }

    /// Compiles a [`Pipeline`] into a native [`CompiledPipeline`].
    ///
    /// # Errors
    ///
    /// Returns [`CompileError`] if any expression in the pipeline cannot be compiled.
    pub fn compile(&mut self, pipeline: &Pipeline) -> Result<CompiledPipeline, CompileError> {
        let input_schema = RowSchema::from_arrow(&pipeline.input_schema)
            .map_err(|e| CompileError::UnsupportedExpr(format!("input schema: {e}")))?;
        let output_schema = RowSchema::from_arrow(&pipeline.output_schema)
            .map_err(|e| CompileError::UnsupportedExpr(format!("output schema: {e}")))?;

        let func_ptr = self.compile_function(pipeline, &input_schema, &output_schema)?;

        Ok(CompiledPipeline::new(
            pipeline.id,
            func_ptr,
            Arc::new(input_schema),
            Arc::new(output_schema),
        ))
    }

    /// Generates the Cranelift function for the pipeline.
    fn compile_function(
        &mut self,
        pipeline: &Pipeline,
        input_schema: &RowSchema,
        output_schema: &RowSchema,
    ) -> Result<PipelineFn, CompileError> {
        let func_name = self
            .jit
            .next_func_name(&format!("pipeline_{}", pipeline.id.0));

        let mut sig = self.jit.module().make_signature();
        sig.params.push(AbiParam::new(PTR_TYPE)); // input_row
        sig.params.push(AbiParam::new(PTR_TYPE)); // output_row
        sig.returns.push(AbiParam::new(cl_types::I8)); // action

        let func_id = self
            .jit
            .module()
            .declare_function(&func_name, cranelift_module::Linkage::Local, &sig)?;

        let mut func = Function::with_name_signature(UserFuncName::testcase(&func_name), sig);

        {
            let builder_ctx = self.jit.builder_ctx();
            let mut builder = FunctionBuilder::new(&mut func, builder_ctx);

            let entry = builder.create_block();
            builder.append_block_params_for_function_params(entry);
            builder.switch_to_block(entry);
            builder.seal_block(entry);

            let input_ptr = builder.block_params(entry)[0];
            let output_ptr = builder.block_params(entry)[1];

            // Create the "drop" exit block.
            let drop_block = builder.create_block();

            // Process each stage sequentially.
            for stage in &pipeline.stages {
                match stage {
                    PipelineStage::Filter { predicate } => {
                        emit_filter_stage(
                            &mut builder,
                            input_schema,
                            predicate,
                            input_ptr,
                            drop_block,
                        )?;
                    }
                    PipelineStage::Project { expressions } => {
                        emit_project_stage(
                            &mut builder,
                            input_schema,
                            output_schema,
                            expressions,
                            input_ptr,
                            output_ptr,
                        )?;
                    }
                    PipelineStage::KeyExtract { key_exprs } => {
                        emit_key_extract_stage(
                            &mut builder,
                            input_schema,
                            output_schema,
                            key_exprs,
                            input_ptr,
                            output_ptr,
                        )?;
                    }
                }
            }

            // Emit path: return 1.
            let emit_val = builder.ins().iconst(cl_types::I8, 1);
            builder.ins().return_(&[emit_val]);

            // Drop path: return 0.
            builder.switch_to_block(drop_block);
            builder.seal_block(drop_block);
            let drop_val = builder.ins().iconst(cl_types::I8, 0);
            builder.ins().return_(&[drop_val]);

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
        Ok(unsafe { std::mem::transmute::<*const u8, PipelineFn>(code_ptr) })
    }
}

/// Emits a filter stage: evaluate predicate, branch to `drop_block` if false/null.
fn emit_filter_stage(
    builder: &mut FunctionBuilder,
    input_schema: &RowSchema,
    predicate: &datafusion_expr::Expr,
    input_ptr: Value,
    drop_block: cranelift_codegen::ir::Block,
) -> Result<(), CompileError> {
    let compiled = compile_expr_inner(builder, input_schema, predicate, input_ptr)?;

    // If the result is nullable, treat null as false (drop).
    let result = if compiled.is_nullable {
        if let Some(null_flag) = compiled.null_flag {
            let zero = builder.ins().iconst(cl_types::I8, 0);
            let is_null = builder
                .ins()
                .icmp_imm(cranelift_codegen::ir::condcodes::IntCC::NotEqual, null_flag, 0);
            builder.ins().select(is_null, zero, compiled.value)
        } else {
            compiled.value
        }
    } else {
        compiled.value
    };

    // Branch: if result == 0, jump to drop_block.
    let continue_block = builder.create_block();
    builder
        .ins()
        .brif(result, continue_block, &[], drop_block, &[]);
    builder.switch_to_block(continue_block);
    builder.seal_block(continue_block);

    Ok(())
}

/// Emits a projection stage: evaluate each expression and store to output row.
fn emit_project_stage(
    builder: &mut FunctionBuilder,
    input_schema: &RowSchema,
    output_schema: &RowSchema,
    expressions: &[(datafusion_expr::Expr, String)],
    input_ptr: Value,
    output_ptr: Value,
) -> Result<(), CompileError> {
    let mem_flags = MemFlags::trusted();

    for (i, (expr, _name)) in expressions.iter().enumerate() {
        let compiled = compile_expr_inner(builder, input_schema, expr, input_ptr)?;

        // Write value to the output row at the field's offset.
        let out_layout = output_schema.field(i);
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let out_offset = out_layout.offset as i32;
        builder
            .ins()
            .store(mem_flags, compiled.value, output_ptr, out_offset);

        // Write null bit in output null bitmap.
        write_null_bit(builder, output_schema, i, output_ptr, &compiled);
    }

    Ok(())
}

/// Emits a key-extract stage: evaluate key expressions and write to output row.
fn emit_key_extract_stage(
    builder: &mut FunctionBuilder,
    input_schema: &RowSchema,
    output_schema: &RowSchema,
    key_exprs: &[datafusion_expr::Expr],
    input_ptr: Value,
    output_ptr: Value,
) -> Result<(), CompileError> {
    let mem_flags = MemFlags::trusted();

    for (i, expr) in key_exprs.iter().enumerate() {
        if i >= output_schema.field_count() {
            break;
        }

        let compiled = compile_expr_inner(builder, input_schema, expr, input_ptr)?;

        let out_layout = output_schema.field(i);
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let out_offset = out_layout.offset as i32;
        builder
            .ins()
            .store(mem_flags, compiled.value, output_ptr, out_offset);

        write_null_bit(builder, output_schema, i, output_ptr, &compiled);
    }

    Ok(())
}

/// Writes the null bit for a field in the output row's null bitmap.
fn write_null_bit(
    builder: &mut FunctionBuilder,
    schema: &RowSchema,
    field_idx: usize,
    row_ptr: Value,
    compiled: &CompiledValue,
) {
    let layout = schema.field(field_idx);
    let null_bit = layout.null_bit;
    let byte_idx = RowSchema::header_size() + null_bit / 8;
    let bit_idx = null_bit % 8;

    let mem_flags = MemFlags::trusted();
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let byte_offset = byte_idx as i32;

    if compiled.is_nullable {
        if let Some(null_flag) = compiled.null_flag {
            // Load current byte, set or clear the null bit based on null_flag.
            let current_byte = builder
                .ins()
                .load(cl_types::I8, mem_flags, row_ptr, byte_offset);

            let mask = builder.ins().iconst(cl_types::I8, 1 << bit_idx);
            let inv_mask = builder.ins().iconst(cl_types::I8, !(1_i64 << bit_idx));

            // If null_flag != 0, set the bit; otherwise clear it.
            let is_null = builder.ins().icmp_imm(
                cranelift_codegen::ir::condcodes::IntCC::NotEqual,
                null_flag,
                0,
            );
            let set_byte = builder.ins().bor(current_byte, mask);
            let clear_byte = builder.ins().band(current_byte, inv_mask);
            let new_byte = builder.ins().select(is_null, set_byte, clear_byte);
            builder.ins().store(mem_flags, new_byte, row_ptr, byte_offset);
        }
    } else {
        // Non-nullable: ensure the bit is clear (valid).
        let current_byte = builder
            .ins()
            .load(cl_types::I8, mem_flags, row_ptr, byte_offset);
        let inv_mask = builder.ins().iconst(cl_types::I8, !(1_i64 << bit_idx));
        let new_byte = builder.ins().band(current_byte, inv_mask);
        builder.ins().store(mem_flags, new_byte, row_ptr, byte_offset);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::pipeline::PipelineId;
    use crate::compiler::row::{MutableEventRow, RowSchema};
    use arrow_schema::{DataType, Field, Schema};
    use bumpalo::Bump;
    use datafusion_expr::{col, lit};

    fn make_schema(fields: Vec<(&str, DataType, bool)>) -> Arc<arrow_schema::Schema> {
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

    #[test]
    fn compile_filter_only_pipeline() {
        let schema = make_schema(vec![("x", DataType::Int64, false)]);
        let pipeline = Pipeline {
            id: PipelineId(0),
            stages: vec![PipelineStage::Filter {
                predicate: col("x").gt(lit(10_i64)),
            }],
            input_schema: Arc::clone(&schema),
            output_schema: schema,
        };

        let mut jit = JitContext::new().unwrap();
        let mut compiler = PipelineCompiler::new(&mut jit);
        let compiled = compiler.compile(&pipeline).unwrap();

        let arena = Bump::new();
        let input_schema = &compiled.input_schema;

        // x = 20 → should pass (Emit)
        let input = make_row_bytes(&arena, input_schema, &[(0, 20)], &[]);
        let mut output = vec![0u8; input_schema.min_row_size()];
        let action = unsafe { compiled.execute(input.as_ptr(), output.as_mut_ptr()) };
        assert_eq!(action, crate::compiler::pipeline::PipelineAction::Emit);

        // x = 5 → should fail (Drop)
        let input = make_row_bytes(&arena, input_schema, &[(0, 5)], &[]);
        let action = unsafe { compiled.execute(input.as_ptr(), output.as_mut_ptr()) };
        assert_eq!(action, crate::compiler::pipeline::PipelineAction::Drop);
    }

    #[test]
    fn compile_project_only_pipeline() {
        let input_schema = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Int64, false),
        ]);
        let output_schema = make_schema(vec![
            ("a", DataType::Int64, false),
            ("a_plus_b", DataType::Int64, false),
        ]);

        let pipeline = Pipeline {
            id: PipelineId(0),
            stages: vec![PipelineStage::Project {
                expressions: vec![
                    (col("a"), "a".to_string()),
                    (col("a") + col("b"), "a_plus_b".to_string()),
                ],
            }],
            input_schema,
            output_schema: Arc::clone(&output_schema),
        };

        let mut jit = JitContext::new().unwrap();
        let mut compiler = PipelineCompiler::new(&mut jit);
        let compiled = compiler.compile(&pipeline).unwrap();

        let arena = Bump::new();
        let in_rs = &compiled.input_schema;
        let out_rs = &compiled.output_schema;

        let input = make_row_bytes(&arena, in_rs, &[(0, 30), (1, 12)], &[]);
        let mut output_buf = vec![0u8; out_rs.min_row_size()];
        let action = unsafe { compiled.execute(input.as_ptr(), output_buf.as_mut_ptr()) };
        assert_eq!(action, crate::compiler::pipeline::PipelineAction::Emit);

        // Check output field 0 (a = 30) and field 1 (a+b = 42).
        let out_row = crate::compiler::row::EventRow::new(&output_buf, out_rs);
        assert_eq!(out_row.get_i64(0), 30);
        assert_eq!(out_row.get_i64(1), 42);
    }

    #[test]
    fn compile_filter_then_project_pipeline() {
        let input_schema = make_schema(vec![
            ("x", DataType::Int64, false),
            ("y", DataType::Int64, false),
        ]);
        let output_schema = make_schema(vec![
            ("x", DataType::Int64, false),
            ("y_doubled", DataType::Int64, false),
        ]);

        let pipeline = Pipeline {
            id: PipelineId(0),
            stages: vec![
                PipelineStage::Filter {
                    predicate: col("x").gt(lit(0_i64)),
                },
                PipelineStage::Project {
                    expressions: vec![
                        (col("x"), "x".to_string()),
                        (col("y") * lit(2_i64), "y_doubled".to_string()),
                    ],
                },
            ],
            input_schema,
            output_schema: Arc::clone(&output_schema),
        };

        let mut jit = JitContext::new().unwrap();
        let mut compiler = PipelineCompiler::new(&mut jit);
        let compiled = compiler.compile(&pipeline).unwrap();

        let arena = Bump::new();
        let in_rs = &compiled.input_schema;
        let out_rs = &compiled.output_schema;

        // x=5, y=21 → passes filter, emits (x=5, y_doubled=42)
        let input = make_row_bytes(&arena, in_rs, &[(0, 5), (1, 21)], &[]);
        let mut output_buf = vec![0u8; out_rs.min_row_size()];
        let action = unsafe { compiled.execute(input.as_ptr(), output_buf.as_mut_ptr()) };
        assert_eq!(action, crate::compiler::pipeline::PipelineAction::Emit);
        let out_row = crate::compiler::row::EventRow::new(&output_buf, out_rs);
        assert_eq!(out_row.get_i64(0), 5);
        assert_eq!(out_row.get_i64(1), 42);

        // x=-1, y=21 → fails filter (Drop)
        let input = make_row_bytes(&arena, in_rs, &[(0, -1), (1, 21)], &[]);
        let action = unsafe { compiled.execute(input.as_ptr(), output_buf.as_mut_ptr()) };
        assert_eq!(action, crate::compiler::pipeline::PipelineAction::Drop);
    }

    #[test]
    fn compile_key_extract_pipeline() {
        let input_schema = make_schema(vec![
            ("key", DataType::Int64, false),
            ("val", DataType::Int64, false),
        ]);
        let output_schema = make_schema(vec![("key", DataType::Int64, false)]);

        let pipeline = Pipeline {
            id: PipelineId(0),
            stages: vec![PipelineStage::KeyExtract {
                key_exprs: vec![col("key")],
            }],
            input_schema,
            output_schema: Arc::clone(&output_schema),
        };

        let mut jit = JitContext::new().unwrap();
        let mut compiler = PipelineCompiler::new(&mut jit);
        let compiled = compiler.compile(&pipeline).unwrap();

        let arena = Bump::new();
        let in_rs = &compiled.input_schema;
        let out_rs = &compiled.output_schema;

        let input = make_row_bytes(&arena, in_rs, &[(0, 42), (1, 99)], &[]);
        let mut output_buf = vec![0u8; out_rs.min_row_size()];
        let action = unsafe { compiled.execute(input.as_ptr(), output_buf.as_mut_ptr()) };
        assert_eq!(action, crate::compiler::pipeline::PipelineAction::Emit);

        let out_row = crate::compiler::row::EventRow::new(&output_buf, out_rs);
        assert_eq!(out_row.get_i64(0), 42);
    }

    #[test]
    fn compile_null_handling_in_filter() {
        let input_schema = make_schema(vec![("x", DataType::Int64, true)]);

        let pipeline = Pipeline {
            id: PipelineId(0),
            stages: vec![PipelineStage::Filter {
                predicate: col("x").gt(lit(10_i64)),
            }],
            input_schema: Arc::clone(&input_schema),
            output_schema: input_schema,
        };

        let mut jit = JitContext::new().unwrap();
        let mut compiler = PipelineCompiler::new(&mut jit);
        let compiled = compiler.compile(&pipeline).unwrap();

        let arena = Bump::new();
        let in_rs = &compiled.input_schema;

        // null x → should be dropped
        let input = make_row_bytes(&arena, in_rs, &[(0, 0)], &[0]);
        let mut output_buf = vec![0u8; in_rs.min_row_size()];
        let action = unsafe { compiled.execute(input.as_ptr(), output_buf.as_mut_ptr()) };
        assert_eq!(action, crate::compiler::pipeline::PipelineAction::Drop);
    }

    #[test]
    fn compile_null_propagation_in_project() {
        let input_schema = make_schema(vec![
            ("x", DataType::Int64, true),
            ("y", DataType::Int64, false),
        ]);
        let output_schema = make_schema(vec![("sum", DataType::Int64, true)]);

        let pipeline = Pipeline {
            id: PipelineId(0),
            stages: vec![PipelineStage::Project {
                expressions: vec![(col("x") + col("y"), "sum".to_string())],
            }],
            input_schema,
            output_schema: Arc::clone(&output_schema),
        };

        let mut jit = JitContext::new().unwrap();
        let mut compiler = PipelineCompiler::new(&mut jit);
        let compiled = compiler.compile(&pipeline).unwrap();

        let arena = Bump::new();
        let in_rs = &compiled.input_schema;
        let out_rs = &compiled.output_schema;

        // x is null → output should be null
        let input = make_row_bytes(&arena, in_rs, &[(0, 0), (1, 10)], &[0]);
        let mut output_buf = vec![0u8; out_rs.min_row_size()];
        let action = unsafe { compiled.execute(input.as_ptr(), output_buf.as_mut_ptr()) };
        assert_eq!(action, crate::compiler::pipeline::PipelineAction::Emit);

        let out_row = crate::compiler::row::EventRow::new(&output_buf, out_rs);
        assert!(out_row.is_null(0));
    }

    #[test]
    fn compile_multi_type_project() {
        let input_schema = make_schema(vec![
            ("i", DataType::Int64, false),
            ("f", DataType::Float64, false),
        ]);
        let output_schema = make_schema(vec![
            ("i", DataType::Int64, false),
            ("f", DataType::Float64, false),
        ]);

        let pipeline = Pipeline {
            id: PipelineId(0),
            stages: vec![PipelineStage::Project {
                expressions: vec![
                    (col("i") + lit(1_i64), "i".to_string()),
                    (col("f"), "f".to_string()),
                ],
            }],
            input_schema,
            output_schema: Arc::clone(&output_schema),
        };

        let mut jit = JitContext::new().unwrap();
        let mut compiler = PipelineCompiler::new(&mut jit);
        let compiled = compiler.compile(&pipeline).unwrap();

        let arena = Bump::new();
        let in_rs = &compiled.input_schema;
        let out_rs = &compiled.output_schema;

        let mut in_row = MutableEventRow::new_in(&arena, in_rs, 0);
        in_row.set_i64(0, 41);
        in_row.set_f64(1, 3.14);
        let frozen = in_row.freeze();

        let mut output_buf = vec![0u8; out_rs.min_row_size()];
        let action = unsafe { compiled.execute(frozen.data().as_ptr(), output_buf.as_mut_ptr()) };
        assert_eq!(action, crate::compiler::pipeline::PipelineAction::Emit);

        let out_row = crate::compiler::row::EventRow::new(&output_buf, out_rs);
        assert_eq!(out_row.get_i64(0), 42);
        assert!((out_row.get_f64(1) - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn compile_empty_pipeline_always_emits() {
        let schema = make_schema(vec![("x", DataType::Int64, false)]);
        let pipeline = Pipeline {
            id: PipelineId(0),
            stages: vec![],
            input_schema: Arc::clone(&schema),
            output_schema: schema,
        };

        let mut jit = JitContext::new().unwrap();
        let mut compiler = PipelineCompiler::new(&mut jit);
        let compiled = compiler.compile(&pipeline).unwrap();

        let arena = Bump::new();
        let in_rs = &compiled.input_schema;
        let input = make_row_bytes(&arena, in_rs, &[(0, 42)], &[]);
        let mut output_buf = vec![0u8; in_rs.min_row_size()];
        let action = unsafe { compiled.execute(input.as_ptr(), output_buf.as_mut_ptr()) };
        assert_eq!(action, crate::compiler::pipeline::PipelineAction::Emit);
    }

    #[test]
    fn compile_multiple_pipelines_same_jit() {
        let schema = make_schema(vec![("x", DataType::Int64, false)]);

        let p1 = Pipeline {
            id: PipelineId(0),
            stages: vec![PipelineStage::Filter {
                predicate: col("x").gt(lit(10_i64)),
            }],
            input_schema: Arc::clone(&schema),
            output_schema: Arc::clone(&schema),
        };
        let p2 = Pipeline {
            id: PipelineId(1),
            stages: vec![PipelineStage::Filter {
                predicate: col("x").lt(lit(100_i64)),
            }],
            input_schema: Arc::clone(&schema),
            output_schema: schema,
        };

        let mut jit = JitContext::new().unwrap();
        let mut compiler = PipelineCompiler::new(&mut jit);
        let c1 = compiler.compile(&p1).unwrap();
        let c2 = compiler.compile(&p2).unwrap();

        let arena = Bump::new();
        let in_rs = &c1.input_schema;

        let input = make_row_bytes(&arena, in_rs, &[(0, 50)], &[]);
        let mut output = vec![0u8; in_rs.min_row_size()];

        let a1 = unsafe { c1.execute(input.as_ptr(), output.as_mut_ptr()) };
        let a2 = unsafe { c2.execute(input.as_ptr(), output.as_mut_ptr()) };
        assert_eq!(a1, crate::compiler::pipeline::PipelineAction::Emit);
        assert_eq!(a2, crate::compiler::pipeline::PipelineAction::Emit);
    }

    #[test]
    fn compile_chained_filters() {
        let schema = make_schema(vec![("x", DataType::Int64, false)]);

        let pipeline = Pipeline {
            id: PipelineId(0),
            stages: vec![
                PipelineStage::Filter {
                    predicate: col("x").gt(lit(0_i64)),
                },
                PipelineStage::Filter {
                    predicate: col("x").lt(lit(100_i64)),
                },
            ],
            input_schema: Arc::clone(&schema),
            output_schema: schema,
        };

        let mut jit = JitContext::new().unwrap();
        let mut compiler = PipelineCompiler::new(&mut jit);
        let compiled = compiler.compile(&pipeline).unwrap();

        let arena = Bump::new();
        let in_rs = &compiled.input_schema;

        // x=50 → passes both
        let input = make_row_bytes(&arena, in_rs, &[(0, 50)], &[]);
        let mut output = vec![0u8; in_rs.min_row_size()];
        assert_eq!(
            unsafe { compiled.execute(input.as_ptr(), output.as_mut_ptr()) },
            crate::compiler::pipeline::PipelineAction::Emit
        );

        // x=-5 → fails first filter
        let input = make_row_bytes(&arena, in_rs, &[(0, -5)], &[]);
        assert_eq!(
            unsafe { compiled.execute(input.as_ptr(), output.as_mut_ptr()) },
            crate::compiler::pipeline::PipelineAction::Drop
        );

        // x=200 → passes first, fails second
        let input = make_row_bytes(&arena, in_rs, &[(0, 200)], &[]);
        assert_eq!(
            unsafe { compiled.execute(input.as_ptr(), output.as_mut_ptr()) },
            crate::compiler::pipeline::PipelineAction::Drop
        );
    }
}
