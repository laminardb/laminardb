use super::{detect_ai_functions, validate_ai_calls, AiCallSpec};
use crate::ai::{ModelBackend, ModelEntry, ModelRegistry, Task};

fn spec(task: Task, model: Option<&str>, labels: Option<Vec<&str>>) -> AiCallSpec {
    AiCallSpec {
        task,
        model: model.map(str::to_string),
        labels: labels.map(|ls| ls.into_iter().map(str::to_string).collect()),
        input: "x".to_string(),
        output_alias: None,
        parse_errors: Vec::new(),
    }
}

fn registry() -> ModelRegistry {
    let mut reg = ModelRegistry::new();
    reg.register(ModelEntry {
        id: "finbert".into(),
        tasks: vec![Task::Classify, Task::Sentiment],
        backend: ModelBackend::Local {
            source: "hf:onnx-community/finbert".into(),
            labels: Some(vec!["positive".into(), "negative".into(), "neutral".into()]),
        },
    })
    .unwrap();
    reg.register(ModelEntry {
        id: "haiku".into(),
        tasks: vec![Task::Classify, Task::Complete, Task::Sentiment],
        backend: ModelBackend::Remote {
            provider: "anthropic".into(),
            model: "claude-haiku-4-5-20251001".into(),
        },
    })
    .unwrap();
    reg.set_default(Task::Sentiment, "finbert");
    reg
}

#[test]
fn detects_single_classify_with_model_and_alias() {
    let calls = detect_ai_functions(
        "SELECT id, ai_classify(headline, model => 'finbert') AS label FROM news",
    );
    assert_eq!(calls.len(), 1);
    let call = &calls[0];
    assert_eq!(call.task, Task::Classify);
    assert_eq!(call.model.as_deref(), Some("finbert"));
    assert_eq!(call.input, "headline");
    assert_eq!(call.output_alias.as_deref(), Some("label"));
    assert!(call.labels.is_none());
}

#[test]
fn detects_labels_array() {
    let calls = detect_ai_functions(
        "SELECT ai_classify(text, model => 'haiku', labels => ARRAY['up','down']) FROM s",
    );
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0].labels,
        Some(vec!["up".to_string(), "down".to_string()])
    );
}

#[test]
fn ignores_queries_without_ai_functions() {
    assert!(detect_ai_functions("SELECT a, b FROM s").is_empty());
}

#[test]
fn malformed_arguments_are_rejected() {
    // Non-column input, missing input, and wrong-typed model/labels each
    // record a parse error that validation surfaces (not silently dropped).
    let cases = [
        "SELECT ai_classify(UPPER(headline), model => 'finbert') AS x FROM s",
        "SELECT ai_classify(model => 'finbert') AS x FROM s",
        "SELECT ai_classify(headline, model => 123) AS x FROM s",
        "SELECT ai_classify(headline, model => 'finbert', labels => 'up') AS x FROM s",
        "SELECT ai_classify(headline, extra, model => 'finbert') AS x FROM s",
        "SELECT ai_classify(headline, model => 'finbert', who => 'me') AS x FROM s",
    ];
    for sql in cases {
        let calls = detect_ai_functions(sql);
        assert_eq!(calls.len(), 1, "{sql}");
        assert!(!calls[0].parse_errors.is_empty(), "{sql}");
        assert!(validate_ai_calls(&registry(), &calls).is_err(), "{sql}");
    }
}

#[test]
fn plan_rewrites_projection_over_tmp_table() {
    let plan = super::plan_ai_query(
        "SELECT id, ai_classify(headline, model => 'finbert') AS label FROM news WHERE id > 0",
    )
    .expect("single aliased AI call is plannable");
    assert_eq!(plan.source_table, "news");
    assert_eq!(plan.call.output_alias.as_deref(), Some("label"));
    let sql = plan.projection_sql.to_lowercase();
    assert!(sql.contains("__ai_tmp"), "{sql}");
    assert!(!sql.contains("ai_classify"), "{sql}");
    assert!(sql.contains("label"));
    assert!(sql.contains("where id > 0"), "{sql}");
}

#[test]
fn plan_requires_alias_and_single_call() {
    assert!(super::plan_ai_query("SELECT ai_classify(t, model => 'm') FROM s").is_none());
    assert!(super::plan_ai_query(
        "SELECT ai_classify(a, model => 'm') AS x, ai_embed(b, model => 'e') AS y FROM s"
    )
    .is_none());
    assert!(super::plan_ai_query("SELECT a FROM s").is_none());
}

#[test]
fn unknown_model_is_rejected() {
    let calls = [spec(Task::Classify, Some("ghost"), Some(vec!["a"]))];
    assert!(validate_ai_calls(&registry(), &calls).is_err());
}

#[test]
fn unsupported_task_is_rejected() {
    let calls = [spec(Task::Summarize, Some("finbert"), None)];
    assert!(validate_ai_calls(&registry(), &calls).is_err());
}

#[test]
fn local_labels_must_be_a_subset() {
    let bad = [spec(Task::Classify, Some("finbert"), Some(vec!["bullish"]))];
    assert!(validate_ai_calls(&registry(), &bad).is_err());
    let ok = [spec(
        Task::Classify,
        Some("finbert"),
        Some(vec!["positive"]),
    )];
    assert!(validate_ai_calls(&registry(), &ok).is_ok());
}

#[test]
fn local_labels_are_optional() {
    let calls = [spec(Task::Classify, Some("finbert"), None)];
    assert!(validate_ai_calls(&registry(), &calls).is_ok());
}

#[test]
fn remote_classification_requires_labels() {
    let without = [spec(Task::Classify, Some("haiku"), None)];
    assert!(validate_ai_calls(&registry(), &without).is_err());
    let with = [spec(Task::Classify, Some("haiku"), Some(vec!["a", "b"]))];
    assert!(validate_ai_calls(&registry(), &with).is_ok());
}

#[test]
fn remote_sentiment_needs_no_labels() {
    // Sentiment is numeric — a remote model scores it without a candidate set.
    let calls = [spec(Task::Sentiment, Some("haiku"), None)];
    assert!(validate_ai_calls(&registry(), &calls).is_ok());
}

#[test]
fn default_model_resolves_or_fails() {
    // Sentiment has a default (finbert).
    let defaulted = [spec(Task::Sentiment, None, None)];
    assert!(validate_ai_calls(&registry(), &defaulted).is_ok());
    // Embed has no default.
    let no_default = [spec(Task::Embed, None, None)];
    assert!(validate_ai_calls(&registry(), &no_default).is_err());
}
