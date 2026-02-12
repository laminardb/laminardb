//! Fallback mechanism for pipeline compilation.
//!
//! [`ExecutablePipeline`] wraps either a successfully compiled pipeline or
//! a fallback marker indicating that compilation failed. The caller in
//! `laminar-sql` or `laminar-db` is responsible for providing interpreted
//! `DataFusion` execution when the fallback variant is returned.

use super::cache::CompilerCache;
use super::error::CompileError;
use super::pipeline::{CompiledPipeline, Pipeline, PipelineId};
use std::sync::Arc;

/// A pipeline that is either compiled to native code or marked for fallback.
#[derive(Debug)]
pub enum ExecutablePipeline {
    /// Successfully compiled to native code.
    Compiled(Arc<CompiledPipeline>),
    /// Compilation failed; the caller should use `DataFusion` interpreted execution.
    Fallback {
        /// The pipeline ID for correlation.
        pipeline_id: PipelineId,
        /// The reason compilation failed.
        reason: CompileError,
    },
}

impl ExecutablePipeline {
    /// Attempts to compile the pipeline, returning a fallback on failure.
    pub fn try_compile(cache: &mut CompilerCache, pipeline: &Pipeline) -> Self {
        match cache.get_or_compile(pipeline) {
            Ok(compiled) => Self::Compiled(compiled),
            Err(reason) => Self::Fallback {
                pipeline_id: pipeline.id,
                reason,
            },
        }
    }

    /// Returns `true` if the pipeline was successfully compiled.
    #[must_use]
    pub fn is_compiled(&self) -> bool {
        matches!(self, Self::Compiled(_))
    }

    /// Returns the compiled pipeline if available.
    #[must_use]
    pub fn as_compiled(&self) -> Option<&Arc<CompiledPipeline>> {
        match self {
            Self::Compiled(c) => Some(c),
            Self::Fallback { .. } => None,
        }
    }

    /// Returns the pipeline ID.
    #[must_use]
    pub fn pipeline_id(&self) -> PipelineId {
        match self {
            Self::Compiled(c) => c.id,
            Self::Fallback { pipeline_id, .. } => *pipeline_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::pipeline::{Pipeline, PipelineId, PipelineStage};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_expr::{col, lit};
    use std::sync::Arc;

    fn make_schema(fields: Vec<(&str, DataType)>) -> Arc<Schema> {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dt)| Field::new(name, dt, false))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn try_compile_success() {
        let mut cache = CompilerCache::new(10).unwrap();
        let schema = make_schema(vec![("x", DataType::Int64)]);
        let pipeline = Pipeline {
            id: PipelineId(0),
            stages: vec![PipelineStage::Filter {
                predicate: col("x").gt(lit(10_i64)),
            }],
            input_schema: Arc::clone(&schema),
            output_schema: schema,
        };

        let exec = ExecutablePipeline::try_compile(&mut cache, &pipeline);
        assert!(exec.is_compiled());
        assert!(exec.as_compiled().is_some());
        assert_eq!(exec.pipeline_id(), PipelineId(0));
    }

    #[test]
    fn try_compile_fallback() {
        let mut cache = CompilerCache::new(10).unwrap();
        let schema = make_schema(vec![("x", DataType::Int64)]);
        let pipeline = Pipeline {
            id: PipelineId(1),
            stages: vec![PipelineStage::Filter {
                // Reference a non-existent column to trigger a compile error.
                predicate: col("nonexistent").gt(lit(10_i64)),
            }],
            input_schema: Arc::clone(&schema),
            output_schema: schema,
        };

        let exec = ExecutablePipeline::try_compile(&mut cache, &pipeline);
        assert!(!exec.is_compiled());
        assert!(exec.as_compiled().is_none());
        assert_eq!(exec.pipeline_id(), PipelineId(1));
    }

    #[test]
    fn fallback_debug() {
        let mut cache = CompilerCache::new(10).unwrap();
        let schema = make_schema(vec![("x", DataType::Int64)]);
        let pipeline = Pipeline {
            id: PipelineId(2),
            stages: vec![PipelineStage::Filter {
                predicate: col("missing").gt(lit(1_i64)),
            }],
            input_schema: Arc::clone(&schema),
            output_schema: schema,
        };

        let exec = ExecutablePipeline::try_compile(&mut cache, &pipeline);
        let dbg = format!("{exec:?}");
        assert!(dbg.contains("Fallback"));
    }

    #[test]
    fn compiled_pipeline_id_matches() {
        let mut cache = CompilerCache::new(10).unwrap();
        let schema = make_schema(vec![("x", DataType::Int64)]);
        let pipeline = Pipeline {
            id: PipelineId(42),
            stages: vec![],
            input_schema: Arc::clone(&schema),
            output_schema: schema,
        };

        let exec = ExecutablePipeline::try_compile(&mut cache, &pipeline);
        assert_eq!(exec.pipeline_id(), PipelineId(42));
    }
}
