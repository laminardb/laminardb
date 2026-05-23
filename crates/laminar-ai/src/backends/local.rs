//! Local ONNX inference via tract (pure Rust). Encoder models only — the
//! BERT / DistilBERT / MiniLM family: classification/sentiment yield logits
//! (the adapter argmaxes), embedding yields a mean-pooled vector. Generative
//! tasks are rejected. A model is loaded once per source and cached; tract runs
//! on `spawn_blocking`, off the Ring 1 task, under a deadline so a pathological
//! model can never stall the inference worker (and the watermark behind it).
//!
//! A `source` resolves to a directory holding `model.onnx` + `tokenizer.json`
//! (+ optional `config.json` for labels): `hf:org/repo` → `<cache_dir>/org/repo`,
//! `file://<path>` or a bare path used as-is. A missing `hf:` repo is downloaded
//! from the Hugging Face CDN on first use (public repos only). Note: classifier
//! labels are read from `config.json` at startup, so a model that downloads
//! lazily must instead carry an explicit `labels` argument until it is cached.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tract_onnx::prelude::*;

use crate::provider::{
    InferenceOutputs, InferenceProvider, InferenceRequest, InferenceResponse, ProviderError, Usage,
};
use crate::registry::Task;

type Plan = TypedRunnableModel<TypedModel>;

/// Per-batch deadline for the synchronous tract forward pass. Bounds the
/// inference worker (and the held watermark) against a wedged model. On timeout
/// the blocking thread is abandoned — it cannot be cancelled — and the batch
/// fails, releasing the hold.
const INFERENCE_TIMEOUT: Duration = Duration::from_secs(60);

/// A loaded model: the tract plan, its tokenizer, and its input arity (2 or 3:
/// `input_ids`, `attention_mask`, and optionally `token_type_ids`).
struct LoadedModel {
    plan: Plan,
    tokenizer: tokenizers::Tokenizer,
    input_arity: usize,
}

/// Local ONNX provider, backed by a model cache directory.
pub struct LocalProvider {
    cache_dir: PathBuf,
    loaded: Mutex<HashMap<String, Arc<LoadedModel>>>,
    http: reqwest::Client,
}

impl LocalProvider {
    /// Create a provider that resolves models under `cache_dir`.
    #[must_use]
    pub fn new(cache_dir: impl Into<PathBuf>) -> Self {
        // A connect deadline plus a generous total cap bounds a hung download
        // without killing a legitimately large model fetch.
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(600))
            .build()
            .unwrap_or_default();
        Self {
            cache_dir: cache_dir.into(),
            loaded: Mutex::new(HashMap::new()),
            http,
        }
    }

    /// Resolve a model to a loaded, cached plan: serve from cache, else download
    /// (for an absent `hf:` repo) and compile on the blocking pool.
    async fn ensure_model(&self, source: &str) -> Result<Arc<LoadedModel>, ProviderError> {
        if let Some(model) = self.loaded.lock().get(source) {
            return Ok(Arc::clone(model));
        }
        let dir = model_dir(&self.cache_dir, source);
        if let Some(repo) = source.strip_prefix("hf:") {
            download_if_missing(&self.http, repo, &dir).await?;
        }
        // Compiling the ONNX graph is heavy, blocking work — keep it off Ring 1.
        let loaded = tokio::task::spawn_blocking(move || load_model(&dir))
            .await
            .map_err(|e| ProviderError::Transport(format!("model load task: {e}")))??;
        let loaded = Arc::new(loaded);
        self.loaded
            .lock()
            .insert(source.to_string(), Arc::clone(&loaded));
        Ok(loaded)
    }
}

/// Download `model.onnx`, `tokenizer.json` (required) and `config.json`
/// (optional — labels only) from the Hugging Face CDN if they are not already
/// present. Public repos only; no auth.
async fn download_if_missing(
    http: &reqwest::Client,
    repo: &str,
    dir: &Path,
) -> Result<(), ProviderError> {
    if dir.join("model.onnx").exists() && dir.join("tokenizer.json").exists() {
        return Ok(());
    }
    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|e| ProviderError::Transport(format!("create {}: {e}", dir.display())))?;
    for (file, required) in [
        ("model.onnx", true),
        ("tokenizer.json", true),
        ("config.json", false),
    ] {
        let dest = dir.join(file);
        if dest.exists() {
            continue;
        }
        let url = format!("https://huggingface.co/{repo}/resolve/main/{file}");
        let resp = http
            .get(&url)
            .send()
            .await
            .map_err(|e| ProviderError::Transport(format!("download {url}: {e}")))?;
        if !resp.status().is_success() {
            if !required {
                continue;
            }
            return Err(ProviderError::Transport(format!(
                "download {url}: HTTP {}",
                resp.status()
            )));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| ProviderError::Transport(format!("download {url}: {e}")))?;
        tokio::fs::write(&dest, &bytes)
            .await
            .map_err(|e| ProviderError::Transport(format!("write {}: {e}", dest.display())))?;
    }
    Ok(())
}

/// On-disk directory for a model `source`: `hf:org/repo` → `<cache_dir>/org/repo`,
/// `file://<path>` or a bare path used as-is.
#[must_use]
pub fn model_dir(cache_dir: &Path, source: &str) -> PathBuf {
    if let Some(repo) = source.strip_prefix("hf:") {
        cache_dir.join(repo)
    } else if let Some(path) = source.strip_prefix("file://") {
        PathBuf::from(path)
    } else {
        PathBuf::from(source)
    }
}

/// Classifier labels from a model's `config.json` `id2label`, ordered by index.
/// Empty if the file is absent or has no `id2label` — the registry uses this to
/// auto-derive a local classifier's labels.
#[must_use]
pub fn load_labels(cache_dir: &Path, source: &str) -> Vec<String> {
    std::fs::read_to_string(model_dir(cache_dir, source).join("config.json"))
        .ok()
        .map(|text| parse_id2label(&text))
        .unwrap_or_default()
}

fn parse_id2label(config_json: &str) -> Vec<String> {
    let Ok(json) = serde_json::from_str::<serde_json::Value>(config_json) else {
        return Vec::new();
    };
    let Some(map) = json.get("id2label").and_then(serde_json::Value::as_object) else {
        return Vec::new();
    };
    let mut indexed: Vec<(usize, String)> = map
        .iter()
        .filter_map(|(k, v)| Some((k.parse().ok()?, v.as_str()?.to_string())))
        .collect();
    indexed.sort_by_key(|(index, _)| *index);
    indexed.into_iter().map(|(_, label)| label).collect()
}

#[async_trait]
impl InferenceProvider for LocalProvider {
    async fn infer_batch(
        &self,
        request: InferenceRequest,
    ) -> Result<InferenceResponse, ProviderError> {
        if matches!(
            request.task,
            Task::Complete | Task::Summarize | Task::Translate | Task::Gen | Task::Extract
        ) {
            return Err(ProviderError::UnsupportedTask(request.task));
        }
        let loaded = self.ensure_model(&request.model).await?;
        let task = request.task;
        let inputs = request.inputs;
        // tract is synchronous CPU work — keep it off the Ring 1 task, under a
        // deadline so a wedged model cannot stall the worker indefinitely.
        let run = tokio::task::spawn_blocking(move || run(&loaded, task, &inputs));
        let outputs = match tokio::time::timeout(INFERENCE_TIMEOUT, run).await {
            Ok(joined) => {
                joined.map_err(|e| ProviderError::Transport(format!("inference task: {e}")))??
            }
            Err(_) => {
                return Err(ProviderError::Timeout(
                    u64::try_from(INFERENCE_TIMEOUT.as_millis()).unwrap_or(u64::MAX),
                ))
            }
        };
        Ok(InferenceResponse {
            outputs,
            usage: Usage::ZERO,
        })
    }

    fn name(&self) -> &'static str {
        "local"
    }
}

fn load_model(dir: &Path) -> Result<LoadedModel, ProviderError> {
    let onnx_path = dir.join("model.onnx");
    let tokenizer_path = dir.join("tokenizer.json");
    if !onnx_path.exists() || !tokenizer_path.exists() {
        return Err(ProviderError::Transport(format!(
            "local model files not found in {} (expected model.onnx + tokenizer.json)",
            dir.display()
        )));
    }
    let model = tract_onnx::onnx()
        .model_for_path(&onnx_path)
        .map_err(|e| ProviderError::Transport(format!("load onnx: {e}")))?;
    let input_arity = model.inputs.len();
    let plan = model
        .into_optimized()
        .map_err(|e| ProviderError::Transport(format!("optimize onnx: {e}")))?
        .into_runnable()
        .map_err(|e| ProviderError::Transport(format!("plan onnx: {e}")))?;
    let tokenizer = tokenizers::Tokenizer::from_file(&tokenizer_path)
        .map_err(|e| ProviderError::Transport(format!("load tokenizer: {e}")))?;
    Ok(LoadedModel {
        plan,
        tokenizer,
        input_arity,
    })
}

fn run(
    loaded: &LoadedModel,
    task: Task,
    inputs: &[String],
) -> Result<InferenceOutputs, ProviderError> {
    let mut vectors = Vec::with_capacity(inputs.len());
    for text in inputs {
        let encoding = loaded
            .tokenizer
            .encode(text.as_str(), true)
            .map_err(|e| ProviderError::BadResponse(format!("tokenize: {e}")))?;
        let ids: Vec<i64> = encoding.get_ids().iter().map(|&u| i64::from(u)).collect();
        let mask: Vec<i64> = encoding
            .get_attention_mask()
            .iter()
            .map(|&u| i64::from(u))
            .collect();
        let seq = ids.len();

        let tensor = |values: Vec<i64>| -> Result<TValue, ProviderError> {
            tract_ndarray::Array2::from_shape_vec((1, seq), values)
                .map_err(|e| ProviderError::Transport(format!("build tensor: {e}")))
                .map(|a| a.into_tensor().into())
        };
        let mut tensors: TVec<TValue> = tvec!(tensor(ids)?, tensor(mask.clone())?);
        if loaded.input_arity >= 3 {
            tensors.push(tensor(vec![0i64; seq])?);
        }

        let result = loaded
            .plan
            .run(tensors)
            .map_err(|e| ProviderError::Transport(format!("inference: {e}")))?;
        let out = result[0]
            .to_array_view::<f32>()
            .map_err(|e| ProviderError::BadResponse(format!("read output: {e}")))?;
        let data: Vec<f32> = out.iter().copied().collect();

        if task == Task::Embed && out.ndim() == 3 {
            // last_hidden_state [1, seq, hidden] → mean-pool over real tokens.
            vectors.push(mean_pool(&data, out.shape()[1], out.shape()[2], &mask));
        } else {
            // classify/sentiment logits, or a pre-pooled embedding.
            vectors.push(data);
        }
    }
    Ok(InferenceOutputs::Vectors(vectors))
}

/// Mean-pool a row-major `[seq, hidden]` block over non-masked tokens.
fn mean_pool(data: &[f32], seq: usize, hidden: usize, mask: &[i64]) -> Vec<f32> {
    let mut pooled = vec![0.0_f32; hidden];
    let mut count = 0.0_f32;
    for t in 0..seq {
        if mask.get(t).copied().unwrap_or(0) == 0 {
            continue;
        }
        count += 1.0;
        for h in 0..hidden {
            pooled[h] += data[t * hidden + h];
        }
    }
    if count > 0.0 {
        for value in &mut pooled {
            *value /= count;
        }
    }
    pooled
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_dir_resolution() {
        let cache = Path::new("/models");
        assert_eq!(
            model_dir(cache, "hf:onnx-community/finbert"),
            Path::new("/models/onnx-community/finbert")
        );
        assert_eq!(model_dir(cache, "file:///abs/dir"), Path::new("/abs/dir"));
        assert_eq!(model_dir(cache, "/some/path"), Path::new("/some/path"));
    }

    #[test]
    fn parse_id2label_orders_by_index() {
        let config = r#"{"id2label": {"2": "positive", "0": "negative", "1": "neutral"}}"#;
        assert_eq!(
            parse_id2label(config),
            vec!["negative", "neutral", "positive"]
        );
        assert!(parse_id2label("{}").is_empty());
    }

    #[test]
    fn mean_pool_ignores_masked_tokens() {
        // seq=3, hidden=2; token 2 is padding (mask 0).
        let data = [1.0, 2.0, 3.0, 4.0, 100.0, 100.0];
        let pooled = mean_pool(&data, 3, 2, &[1, 1, 0]);
        assert_eq!(pooled, vec![2.0, 3.0]); // mean of rows 0 and 1 only
    }
}
