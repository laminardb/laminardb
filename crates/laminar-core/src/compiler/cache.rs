//! Compiler cache for compiled pipelines.
//!
//! [`CompilerCache`] avoids redundant compilation by caching compiled pipelines
//! keyed by a hash of their stage definitions. It owns a [`JitContext`] and
//! provides `get_or_compile` semantics.

use std::sync::Arc;

use fxhash::FxHashMap;

use super::error::CompileError;
use super::jit::JitContext;
use super::pipeline::{CompiledPipeline, Pipeline};
use super::pipeline_compiler::PipelineCompiler;

/// Cache for compiled pipelines, keyed by a hash of pipeline stages.
pub struct CompilerCache {
    cache: FxHashMap<u64, Arc<CompiledPipeline>>,
    jit: JitContext,
    max_entries: usize,
    /// Insertion order for simple FIFO eviction.
    insertion_order: Vec<u64>,
}

impl CompilerCache {
    /// Creates a new compiler cache with the given maximum number of entries.
    ///
    /// # Errors
    ///
    /// Returns [`CompileError`] if the JIT context cannot be initialized.
    pub fn new(max_entries: usize) -> Result<Self, CompileError> {
        Ok(Self {
            cache: FxHashMap::default(),
            jit: JitContext::new()?,
            max_entries,
            insertion_order: Vec::new(),
        })
    }

    /// Returns a cached compiled pipeline, or compiles and caches it.
    ///
    /// # Errors
    ///
    /// Returns [`CompileError`] if compilation fails.
    pub fn get_or_compile(
        &mut self,
        pipeline: &Pipeline,
    ) -> Result<Arc<CompiledPipeline>, CompileError> {
        let hash = hash_pipeline(pipeline);

        if let Some(cached) = self.cache.get(&hash) {
            return Ok(Arc::clone(cached));
        }

        // Evict oldest entry if at capacity.
        if self.cache.len() >= self.max_entries && !self.insertion_order.is_empty() {
            let oldest_key = self.insertion_order.remove(0);
            self.cache.remove(&oldest_key);
        }

        let mut comp = PipelineCompiler::new(&mut self.jit);
        let result = Arc::new(comp.compile(pipeline)?);

        self.cache.insert(hash, Arc::clone(&result));
        self.insertion_order.push(hash);

        Ok(result)
    }

    /// Returns the number of cached entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Returns `true` if the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Clears all cached entries.
    pub fn clear(&mut self) {
        self.cache.clear();
        self.insertion_order.clear();
    }
}

impl std::fmt::Debug for CompilerCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompilerCache")
            .field("entries", &self.cache.len())
            .field("max_entries", &self.max_entries)
            .finish_non_exhaustive()
    }
}

/// Computes a hash of a pipeline's stages for cache keying.
///
/// Uses the `Debug` representation of each stage, which includes all expression
/// details. This is not cryptographically secure but sufficient for caching.
fn hash_pipeline(pipeline: &Pipeline) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = fxhash::FxHasher::default();

    for stage in &pipeline.stages {
        let stage_repr = format!("{stage:?}");
        stage_repr.hash(&mut hasher);
    }

    // Include schemas in the hash to distinguish pipelines with same stages
    // but different input/output types.
    let in_repr = format!("{:?}", pipeline.input_schema);
    in_repr.hash(&mut hasher);
    let out_repr = format!("{:?}", pipeline.output_schema);
    out_repr.hash(&mut hasher);

    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::pipeline::{PipelineId, PipelineStage};
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

    fn simple_filter_pipeline(id: u32) -> Pipeline {
        let schema = make_schema(vec![("x", DataType::Int64)]);
        Pipeline {
            id: PipelineId(id),
            stages: vec![PipelineStage::Filter {
                predicate: col("x").gt(lit(10_i64)),
            }],
            input_schema: Arc::clone(&schema),
            output_schema: schema,
        }
    }

    #[test]
    fn cache_miss_then_hit() {
        let mut cache = CompilerCache::new(10).unwrap();
        assert!(cache.is_empty());

        let pipeline = simple_filter_pipeline(0);
        let first = cache.get_or_compile(&pipeline).unwrap();
        assert_eq!(cache.len(), 1);

        let second = cache.get_or_compile(&pipeline).unwrap();
        assert_eq!(cache.len(), 1);

        // Should return the same Arc (cache hit).
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn cache_different_pipelines() {
        let mut cache = CompilerCache::new(10).unwrap();

        let p1 = simple_filter_pipeline(0);
        let schema = make_schema(vec![("x", DataType::Int64)]);
        let p2 = Pipeline {
            id: PipelineId(1),
            stages: vec![PipelineStage::Filter {
                predicate: col("x").lt(lit(100_i64)),
            }],
            input_schema: Arc::clone(&schema),
            output_schema: schema,
        };

        cache.get_or_compile(&p1).unwrap();
        cache.get_or_compile(&p2).unwrap();
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn cache_eviction() {
        let mut cache = CompilerCache::new(2).unwrap();

        let schema = make_schema(vec![("x", DataType::Int64)]);
        for i in 0..3 {
            let pipeline = Pipeline {
                id: PipelineId(i),
                stages: vec![PipelineStage::Filter {
                    predicate: col("x").gt(lit(i64::from(i))),
                }],
                input_schema: Arc::clone(&schema),
                output_schema: Arc::clone(&schema),
            };
            cache.get_or_compile(&pipeline).unwrap();
        }

        // Should have evicted the first entry.
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn cache_clear() {
        let mut cache = CompilerCache::new(10).unwrap();
        let pipeline = simple_filter_pipeline(0);
        cache.get_or_compile(&pipeline).unwrap();
        assert_eq!(cache.len(), 1);

        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn hash_stability() {
        let p1 = simple_filter_pipeline(0);
        let p2 = simple_filter_pipeline(1); // Same stages, same schema

        let h1 = hash_pipeline(&p1);
        let h2 = hash_pipeline(&p2);

        // Same stages + schema should produce same hash regardless of pipeline ID.
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_different_for_different_stages() {
        let schema = make_schema(vec![("x", DataType::Int64)]);
        let p1 = Pipeline {
            id: PipelineId(0),
            stages: vec![PipelineStage::Filter {
                predicate: col("x").gt(lit(10_i64)),
            }],
            input_schema: Arc::clone(&schema),
            output_schema: Arc::clone(&schema),
        };
        let p2 = Pipeline {
            id: PipelineId(0),
            stages: vec![PipelineStage::Filter {
                predicate: col("x").lt(lit(10_i64)),
            }],
            input_schema: Arc::clone(&schema),
            output_schema: schema,
        };

        assert_ne!(hash_pipeline(&p1), hash_pipeline(&p2));
    }
}
