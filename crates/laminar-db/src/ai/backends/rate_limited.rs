//! Client-side rate limiting for remote providers.
//!
//! Wraps any [`InferenceProvider`] in a token bucket (`governor`, the standard
//! Rust limiter) so request bursts are shaped to a steady requests-per-second
//! rather than sent unbounded. One cell is acquired per input row before the
//! batch is dispatched. The wait happens on the Ring 1 inference worker — the
//! only place `infer_batch` is awaited — never on Ring 0; the limiter itself is
//! lock-free (atomics), so nothing blocking is reachable from the compute thread.

use std::num::NonZeroU32;
use std::sync::Arc;

use async_trait::async_trait;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};

use crate::ai::provider::{InferenceProvider, InferenceRequest, InferenceResponse, ProviderError};

/// An [`InferenceProvider`] that paces calls to a steady rate.
pub struct RateLimitedProvider {
    inner: Arc<dyn InferenceProvider>,
    limiter: DefaultDirectRateLimiter,
}

impl RateLimitedProvider {
    /// Wrap `inner`, limiting to `requests_per_second` (burst up to the same).
    #[must_use]
    pub fn new(inner: Arc<dyn InferenceProvider>, requests_per_second: NonZeroU32) -> Self {
        Self {
            inner,
            limiter: RateLimiter::direct(Quota::per_second(requests_per_second)),
        }
    }
}

#[async_trait]
impl InferenceProvider for RateLimitedProvider {
    async fn infer_batch(
        &self,
        request: InferenceRequest,
    ) -> Result<InferenceResponse, ProviderError> {
        // One permit per row: the batch waits until the bucket has paced it.
        for _ in 0..request.inputs.len().max(1) {
            self.limiter.until_ready().await;
        }
        self.inner.infer_batch(request).await
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn intrinsic_labels(&self, model: &str) -> Option<Vec<String>> {
        self.inner.intrinsic_labels(model)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::provider::{InferenceOutputs, InferenceParams, Usage};
    use crate::ai::registry::Task;
    use std::time::Instant;

    struct Echo;

    #[async_trait]
    impl InferenceProvider for Echo {
        async fn infer_batch(
            &self,
            request: InferenceRequest,
        ) -> Result<InferenceResponse, ProviderError> {
            Ok(InferenceResponse {
                outputs: InferenceOutputs::Text(request.inputs),
                usage: Usage::ZERO,
            })
        }
        fn name(&self) -> &'static str {
            "echo"
        }
    }

    fn request(rows: usize) -> InferenceRequest {
        InferenceRequest {
            task: Task::Sentiment,
            model: "m".into(),
            inputs: vec!["x".to_string(); rows],
            params: InferenceParams::default(),
        }
    }

    /// A burst beyond the per-second budget is delayed, not dropped or sent
    /// unbounded; the output still passes through unchanged.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn burst_beyond_the_rate_is_paced() {
        // 100 rps → ~10 ms/cell, burst 100. 130 rows = 30 over budget ⇒ ≥ ~300 ms.
        let p = RateLimitedProvider::new(Arc::new(Echo), NonZeroU32::new(100).unwrap());
        let start = Instant::now();
        let resp = p.infer_batch(request(130)).await.unwrap();
        assert_eq!(resp.outputs.len(), 130);
        assert!(
            start.elapsed() >= std::time::Duration::from_millis(200),
            "burst was not paced: {:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn name_delegates_to_inner() {
        let p = RateLimitedProvider::new(Arc::new(Echo), NonZeroU32::new(1000).unwrap());
        assert_eq!(p.name(), "echo");
    }
}
