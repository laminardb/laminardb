//! Local inference via ONNX Runtime (`ort`, loaded dynamically). Encoder models
//! only — the BERT / DistilBERT / MiniLM family: classification/sentiment yield
//! logits (the adapter argmaxes), embedding yields a mean-pooled vector.
//! Generative tasks are rejected. A model is loaded once per source and cached;
//! the forward pass runs on `spawn_blocking`, off the Ring 1 task, under a
//! deadline so a pathological model can never stall the worker (and the
//! watermark behind it). ONNX Runtime is loaded at runtime, so `onnxruntime.dll`
//! / `.so` (ORT >= 1.24) must be on the search path or named by `ORT_DYLIB_PATH`.
//!
//! A `source` resolves to a directory laid out like a Hugging Face export —
//! `onnx/model.onnx` + `tokenizer.json` (+ optional `config.json` for labels):
//! `hf:org/repo` → `<cache_dir>/org/repo`, `file://<path>` or a bare path used
//! as-is. A missing `hf:` repo is downloaded from the Hugging Face CDN on first
//! use (public repos only). Classifier labels come from the model's own
//! `config.json` `id2label`; [`LocalProvider::intrinsic_labels`] resolves them on
//! demand, so a model that downloads lazily scores correctly once it is cached —
//! no restart, no externally supplied label list.

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ort::session::{Session, SessionInputValue};
use ort::value::Tensor;
use parking_lot::Mutex;

use crate::ai::provider::{
    InferenceOutputs, InferenceProvider, InferenceRequest, InferenceResponse, ProviderError, Usage,
};
use crate::ai::registry::Task;

/// Per-batch deadline for the synchronous ONNX Runtime forward pass. Bounds the
/// inference worker (and the held watermark) against a wedged model. On timeout
/// the blocking thread is abandoned — it cannot be cancelled — and the batch
/// fails, releasing the hold.
const INFERENCE_TIMEOUT: Duration = Duration::from_secs(60);

/// A loaded model: an ONNX Runtime session, its tokenizer, and the model's input
/// names (`input_ids`, `attention_mask`, and optionally `token_type_ids`) that
/// drive how each row is fed. `Session::run` takes `&mut`, so it sits behind a
/// mutex — a model serves one batch at a time.
struct LoadedModel {
    session: Mutex<Session>,
    tokenizer: tokenizers::Tokenizer,
    input_names: Vec<String>,
    /// `config.json` `id2label`, read once at load; empty if absent.
    labels: Vec<String>,
}

/// Local ONNX provider, backed by a model cache directory.
pub struct LocalProvider {
    cache_dir: PathBuf,
    loaded: Mutex<HashMap<String, Arc<LoadedModel>>>,
    /// Serializes the download-and-compile path so concurrent misses for the
    /// same model don't fetch and build it more than once.
    load_lock: tokio::sync::Mutex<()>,
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
            load_lock: tokio::sync::Mutex::new(()),
            http,
        }
    }

    /// Resolve a model to a loaded, cached plan: serve from cache, else download
    /// (for an absent `hf:` repo) and compile on the blocking pool.
    async fn ensure_model(&self, source: &str) -> Result<Arc<LoadedModel>, ProviderError> {
        if let Some(model) = self.loaded.lock().get(source) {
            return Ok(Arc::clone(model));
        }
        // Hold the load lock across download+compile and re-check the cache: a
        // concurrent miss may have finished loading this model while we waited.
        let _load = self.load_lock.lock().await;
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
    if onnx_path(dir).exists() && dir.join("tokenizer.json").exists() {
        return Ok(());
    }
    // Paths mirror the repo layout (the graph lives under `onnx/`); config.json
    // is optional and only feeds label derivation, so any failure to fetch it —
    // transport error or a 404 — is non-fatal.
    for (rel, required) in [
        ("onnx/model.onnx", true),
        ("tokenizer.json", true),
        ("config.json", false),
    ] {
        let dest = dir.join(rel);
        if dest.exists() {
            continue;
        }
        let url = format!("https://huggingface.co/{repo}/resolve/main/{rel}");
        match download_file(http, &url, &dest).await {
            Err(e) if required => return Err(e),
            // Success, or an optional file (config.json) we couldn't fetch — skip.
            Ok(()) | Err(_) => {}
        }
    }
    Ok(())
}

/// Fetch one file from `url` into `dest`, creating parent directories. Errors on
/// transport failure, a non-success status, or a write failure.
async fn download_file(
    http: &reqwest::Client,
    url: &str,
    dest: &Path,
) -> Result<(), ProviderError> {
    let resp = http
        .get(url)
        .send()
        .await
        .map_err(|e| ProviderError::Transport(format!("download {url}: {e}")))?;
    if !resp.status().is_success() {
        return Err(ProviderError::Transport(format!(
            "download {url}: HTTP {}",
            resp.status()
        )));
    }
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| ProviderError::Transport(format!("download {url}: {e}")))?;
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| ProviderError::Transport(format!("create {}: {e}", parent.display())))?;
    }
    tokio::fs::write(dest, &bytes)
        .await
        .map_err(|e| ProviderError::Transport(format!("write {}: {e}", dest.display())))?;
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
        // The forward pass is synchronous CPU work — keep it off the Ring 1 task,
        // under a deadline so a wedged model cannot stall the worker indefinitely.
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

    fn intrinsic_labels(&self, model: &str) -> Option<Vec<String>> {
        // Served from the loaded model (read once at load); only an unloaded model
        // — i.e. before its first inference — touches disk here.
        let labels = self
            .loaded
            .lock()
            .get(model)
            .map_or_else(|| load_labels(&self.cache_dir, model), |m| m.labels.clone());
        (!labels.is_empty()).then_some(labels)
    }
}

/// The model graph: the `onnx/model.onnx` that Optimum / transformers.js exports
/// produce, falling back to a flat `model.onnx` for a hand-placed directory.
fn onnx_path(dir: &Path) -> PathBuf {
    let nested = dir.join("onnx").join("model.onnx");
    if nested.exists() {
        nested
    } else {
        dir.join("model.onnx")
    }
}

fn load_model(dir: &Path) -> Result<LoadedModel, ProviderError> {
    let onnx = onnx_path(dir);
    let tokenizer_path = dir.join("tokenizer.json");
    if !onnx.exists() || !tokenizer_path.exists() {
        return Err(ProviderError::Transport(format!(
            "local model files not found in {} (expected onnx/model.onnx + tokenizer.json)",
            dir.display()
        )));
    }
    let session = Session::builder()
        .map_err(|e| ProviderError::Transport(format!("ort init: {e}")))?
        .commit_from_file(&onnx)
        .map_err(|e| ProviderError::Transport(format!("load onnx: {e}")))?;
    let input_names = session
        .inputs()
        .iter()
        .map(|i| i.name().to_string())
        .collect();
    let tokenizer = tokenizers::Tokenizer::from_file(&tokenizer_path)
        .map_err(|e| ProviderError::Transport(format!("load tokenizer: {e}")))?;
    let labels = std::fs::read_to_string(dir.join("config.json"))
        .ok()
        .map(|text| parse_id2label(&text))
        .unwrap_or_default();
    Ok(LoadedModel {
        session: Mutex::new(session),
        tokenizer,
        input_names,
        labels,
    })
}

fn run(
    loaded: &LoadedModel,
    task: Task,
    inputs: &[String],
) -> Result<InferenceOutputs, ProviderError> {
    let mut session = loaded.session.lock();
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
        let seq = i64::try_from(ids.len()).unwrap_or(i64::MAX);

        // Feed each input the model declares (batch of 1, [1, seq]); BERT adds
        // token_type_ids (all zero, single segment), DistilBERT omits it.
        let mut feeds: Vec<(Cow<str>, SessionInputValue)> =
            Vec::with_capacity(loaded.input_names.len());
        for name in &loaded.input_names {
            let row = match name.as_str() {
                "input_ids" => ids.clone(),
                "attention_mask" => mask.clone(),
                "token_type_ids" => vec![0i64; ids.len()],
                other => {
                    return Err(ProviderError::BadResponse(format!(
                        "model expects unsupported input '{other}'"
                    )))
                }
            };
            let tensor = Tensor::from_array((vec![1i64, seq], row))
                .map_err(|e| ProviderError::Transport(format!("build tensor: {e}")))?;
            feeds.push((Cow::Owned(name.clone()), SessionInputValue::from(tensor)));
        }

        let outputs = session
            .run(feeds)
            .map_err(|e| ProviderError::Transport(format!("inference: {e}")))?;
        let (shape, data) = outputs[0]
            .try_extract_tensor::<f32>()
            .map_err(|e| ProviderError::BadResponse(format!("read output: {e}")))?;

        if task == Task::Embed && shape.len() == 3 {
            // last_hidden_state [1, seq, hidden] → mean-pool over real tokens.
            let seq_out = usize::try_from(shape[1]).unwrap_or(0);
            let hidden = usize::try_from(shape[2]).unwrap_or(0);
            vectors.push(mean_pool(data, seq_out, hidden, &mask));
        } else {
            // classify/sentiment logits, or a pre-pooled embedding.
            vectors.push(data.to_vec());
        }
    }
    drop(session);
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
    fn intrinsic_labels_read_from_a_cached_models_config() {
        let cache = tempfile::tempdir().expect("tempdir");
        let provider = LocalProvider::new(cache.path());
        let source = "hf:org/repo";

        // Absent until the model (its config.json) is on disk.
        assert!(provider.intrinsic_labels(source).is_none());

        let dir = model_dir(cache.path(), source);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("config.json"),
            r#"{"id2label": {"0": "NEGATIVE", "1": "POSITIVE"}}"#,
        )
        .unwrap();
        assert_eq!(
            provider.intrinsic_labels(source),
            Some(vec!["NEGATIVE".into(), "POSITIVE".into()])
        );
    }

    #[test]
    fn mean_pool_ignores_masked_tokens() {
        // seq=3, hidden=2; token 2 is padding (mask 0).
        let data = [1.0, 2.0, 3.0, 4.0, 100.0, 100.0];
        let pooled = mean_pool(&data, 3, 2, &[1, 1, 0]);
        assert_eq!(pooled, vec![2.0, 3.0]); // mean of rows 0 and 1 only
    }

    /// End-to-end against a real export: resolve the `onnx/` layout, download a
    /// DistilBERT SST-2 sentiment classifier from the Hugging Face CDN, tokenize,
    /// run it through ONNX Runtime, and check the argmax labels match the
    /// sentiment of clearly positive and negative inputs.
    ///
    /// Opt-in: network + a ~268 MB model download, and ONNX Runtime must be
    /// loadable at runtime (`ORT_DYLIB_PATH=/path/to/onnxruntime.dll`, ORT >= 1.24).
    #[tokio::test]
    #[ignore = "downloads a model + needs ORT_DYLIB_PATH; run with --ignored"]
    async fn classifies_with_a_real_onnx_community_model() {
        use crate::ai::adapter::parse_response;
        use crate::ai::provider::InferenceParams;
        use crate::ai::registry::BackendKind;

        let cache = tempfile::tempdir().expect("tempdir");
        let provider = LocalProvider::new(cache.path());
        let source = "hf:onnx-community/distilbert-base-uncased-finetuned-sst-2-english-ONNX";
        let request = InferenceRequest {
            task: Task::Classify,
            model: source.to_string(),
            inputs: vec![
                "this film was absolutely wonderful, I loved every minute".into(),
                "a complete waste of time, dull and disappointing".into(),
            ],
            params: InferenceParams::default(),
        };

        let response = provider.infer_batch(request).await.expect("inference");
        let InferenceOutputs::Vectors(rows) = &response.outputs else {
            panic!("local classify returns logits");
        };
        let labels = load_labels(cache.path(), source);
        assert!(!labels.is_empty(), "id2label should load from config.json");
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter().all(|r| r.len() == labels.len()),
            "logit dimension must equal the label count",
        );

        let InferenceOutputs::Text(out) = parse_response(
            Task::Classify,
            BackendKind::Local,
            response.outputs,
            Some(&labels),
        )
        .expect("adapt") else {
            panic!("argmax yields labels");
        };
        assert_eq!(out, vec!["POSITIVE".to_string(), "NEGATIVE".to_string()]);
    }
}
