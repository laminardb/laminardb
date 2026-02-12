//! Cranelift JIT compilation context.
//!
//! [`JitContext`] owns the Cranelift [`JITModule`] and provides shared
//! compilation infrastructure for [`ExprCompiler`](super::expr::ExprCompiler).

use std::sync::Arc;

use cranelift_codegen::isa::TargetIsa;
use cranelift_codegen::settings::{self, Configurable};
use cranelift_frontend::FunctionBuilderContext;
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::Module;

use super::error::CompileError;

/// Owns the Cranelift JIT module and builder context used for compiling expressions.
///
/// Create one per compilation session (typically per query). Each compiled function
/// gets a unique name via [`next_func_name`](Self::next_func_name).
pub struct JitContext {
    module: JITModule,
    builder_ctx: FunctionBuilderContext,
    func_counter: u32,
}

impl JitContext {
    /// Creates a new JIT context targeting the host CPU with `opt_level = speed`.
    ///
    /// # Errors
    ///
    /// Returns [`CompileError::Cranelift`] if the native ISA cannot be detected.
    pub fn new() -> Result<Self, CompileError> {
        let isa = Self::create_isa()?;
        let builder = JITBuilder::with_isa(isa, cranelift_module::default_libcall_names());
        let module = JITModule::new(builder);

        Ok(Self {
            module,
            builder_ctx: FunctionBuilderContext::new(),
            func_counter: 0,
        })
    }

    /// Returns a mutable reference to the underlying [`JITModule`].
    pub fn module(&mut self) -> &mut JITModule {
        &mut self.module
    }

    /// Returns a mutable reference to the shared [`FunctionBuilderContext`].
    pub fn builder_ctx(&mut self) -> &mut FunctionBuilderContext {
        &mut self.builder_ctx
    }

    /// Returns the target ISA used by this context.
    pub fn isa(&self) -> &dyn TargetIsa {
        self.module.isa()
    }

    /// Generates a unique function name with the given prefix.
    pub fn next_func_name(&mut self, prefix: &str) -> String {
        let id = self.func_counter;
        self.func_counter += 1;
        format!("{prefix}_{id}")
    }

    /// Creates a host-native ISA with speed optimization.
    fn create_isa() -> Result<Arc<dyn TargetIsa>, CompileError> {
        let mut flag_builder = settings::builder();
        flag_builder
            .set("opt_level", "speed")
            .expect("valid opt_level setting");
        let isa_builder = cranelift_native::builder()
            .map_err(|e| CompileError::UnsupportedExpr(format!("native ISA: {e}")))?;
        let flags = settings::Flags::new(flag_builder);
        Ok(isa_builder.finish(flags).expect("valid ISA flags"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jit_context_creation() {
        let ctx = JitContext::new().unwrap();
        let isa = ctx.isa();
        // Should detect some valid target triple.
        let triple = isa.triple().to_string();
        assert!(!triple.is_empty());
    }

    #[test]
    fn unique_func_names() {
        let mut ctx = JitContext::new().unwrap();
        let n1 = ctx.next_func_name("filter");
        let n2 = ctx.next_func_name("filter");
        let n3 = ctx.next_func_name("scalar");
        assert_eq!(n1, "filter_0");
        assert_eq!(n2, "filter_1");
        assert_eq!(n3, "scalar_2");
    }

    #[test]
    fn module_accessible() {
        let mut ctx = JitContext::new().unwrap();
        // Just verify we can access the module without panicking.
        let _m = ctx.module();
    }

    #[test]
    fn builder_ctx_accessible() {
        let mut ctx = JitContext::new().unwrap();
        let _b = ctx.builder_ctx();
    }
}
