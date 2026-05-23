//! Resolve a raw provider response into a task's per-row output, given the
//! `(task, backend)` pair. Local classification takes argmax over logits (equal
//! to argmax of softmax — v0.1 returns only the label, so no softmax). Stays
//! Arrow-free: returns [`InferenceOutputs`]; the operator builds the column.

use thiserror::Error;

use crate::provider::InferenceOutputs;
use crate::registry::{BackendKind, Task};

/// Errors from adapting a response to a task output.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AdapterError {
    /// A local classifier produced logits but no labels were supplied to map
    /// them to.
    #[error("classification requires the model's labels but none were provided")]
    MissingLabels,

    /// argmax landed on an index with no corresponding label.
    #[error("classifier chose index {index} but only {len} labels are defined")]
    LabelIndexOutOfRange {
        /// The chosen index.
        index: usize,
        /// The number of labels available.
        len: usize,
    },

    /// A classifier returned an empty logit vector for a row.
    #[error("classifier returned no logits for a row")]
    EmptyLogits,

    /// The raw output shape did not match what the task/backend produces.
    #[error("task '{task}' on the {kind:?} backend expected {expected} output, got {got}")]
    UnexpectedOutputShape {
        /// The task.
        task: Task,
        /// The backend kind.
        kind: BackendKind,
        /// The shape that was expected (`text` or `vectors`).
        expected: &'static str,
        /// The shape actually produced.
        got: &'static str,
    },

    /// The task cannot run on this backend kind (e.g. generation on local).
    #[error("task '{task}' is not supported on the {kind:?} backend")]
    UnsupportedCombination {
        /// The task.
        task: Task,
        /// The backend kind.
        kind: BackendKind,
    },
}

/// Resolve a raw provider response into the task's per-row output column.
///
/// `labels` carries the candidate/intrinsic label set for classification — the
/// model's `id2label` for a local classifier (required), ignored otherwise.
///
/// # Errors
///
/// Returns [`AdapterError`] if the output shape is wrong for the task, a local
/// classifier has no labels or argmaxes out of range, or the task cannot run on
/// the given backend kind.
pub fn parse_response(
    task: Task,
    kind: BackendKind,
    raw: InferenceOutputs,
    labels: Option<&[String]>,
) -> Result<InferenceOutputs, AdapterError> {
    match task {
        Task::Classify | Task::Sentiment => match kind {
            BackendKind::Local => classify_from_logits(kind, raw, labels),
            BackendKind::Remote => coerce_remote_classification(raw, labels),
        },
        Task::Embed => expect_vectors(task, kind, raw),
        Task::Complete | Task::Summarize | Task::Translate | Task::Gen | Task::Extract => {
            if kind != BackendKind::Remote {
                // Narrow ONNX token-classification extraction is out of v0.1
                // scope; generation is remote by definition.
                return Err(AdapterError::UnsupportedCombination { task, kind });
            }
            expect_text(task, kind, raw)
        }
    }
}

/// argmax over each row's logits, mapped to the model's labels.
fn classify_from_logits(
    kind: BackendKind,
    raw: InferenceOutputs,
    labels: Option<&[String]>,
) -> Result<InferenceOutputs, AdapterError> {
    let rows = match raw {
        InferenceOutputs::Vectors(rows) => rows,
        InferenceOutputs::Text(_) => {
            return Err(AdapterError::UnexpectedOutputShape {
                task: Task::Classify,
                kind,
                expected: "vectors",
                got: "text",
            });
        }
    };
    let labels = labels.ok_or(AdapterError::MissingLabels)?;
    let mut out = Vec::with_capacity(rows.len());
    for logits in rows {
        let index = argmax(&logits).ok_or(AdapterError::EmptyLogits)?;
        let label = labels
            .get(index)
            .ok_or(AdapterError::LabelIndexOutOfRange {
                index,
                len: labels.len(),
            })?;
        out.push(label.clone());
    }
    Ok(InferenceOutputs::Text(out))
}

/// Coerce a remote model's free text toward the candidate label set.
///
/// LLMs sometimes wrap the answer ("The sentiment is Positive."). When labels
/// are known, normalize each reply to a canonical label by exact (case-
/// insensitive) match, then by containment; otherwise fall back to the trimmed
/// raw text rather than dropping the row.
fn coerce_remote_classification(
    raw: InferenceOutputs,
    labels: Option<&[String]>,
) -> Result<InferenceOutputs, AdapterError> {
    let texts = match raw {
        InferenceOutputs::Text(texts) => texts,
        InferenceOutputs::Vectors(_) => {
            return Err(AdapterError::UnexpectedOutputShape {
                task: Task::Classify,
                kind: BackendKind::Remote,
                expected: "text",
                got: "vectors",
            });
        }
    };
    let coerced = texts
        .into_iter()
        .map(|text| match labels {
            Some(labels) => coerce_label(&text, labels),
            None => text.trim().to_string(),
        })
        .collect();
    Ok(InferenceOutputs::Text(coerced))
}

/// Map free text to a canonical label, or the trimmed text if none matches.
fn coerce_label(text: &str, labels: &[String]) -> String {
    let trimmed = text.trim();
    if let Some(label) = labels.iter().find(|l| l.eq_ignore_ascii_case(trimmed)) {
        return label.clone();
    }
    let lower = trimmed.to_ascii_lowercase();
    if let Some(label) = labels
        .iter()
        .find(|l| lower.contains(&l.to_ascii_lowercase()))
    {
        return label.clone();
    }
    trimmed.to_string()
}

/// Pass text outputs through, rejecting a vector shape.
fn expect_text(
    task: Task,
    kind: BackendKind,
    raw: InferenceOutputs,
) -> Result<InferenceOutputs, AdapterError> {
    match raw {
        InferenceOutputs::Text(text) => Ok(InferenceOutputs::Text(text)),
        InferenceOutputs::Vectors(_) => Err(AdapterError::UnexpectedOutputShape {
            task,
            kind,
            expected: "text",
            got: "vectors",
        }),
    }
}

/// Pass vector outputs through, rejecting a text shape.
fn expect_vectors(
    task: Task,
    kind: BackendKind,
    raw: InferenceOutputs,
) -> Result<InferenceOutputs, AdapterError> {
    match raw {
        InferenceOutputs::Vectors(vectors) => Ok(InferenceOutputs::Vectors(vectors)),
        InferenceOutputs::Text(_) => Err(AdapterError::UnexpectedOutputShape {
            task,
            kind,
            expected: "vectors",
            got: "text",
        }),
    }
}

/// Index of the maximum value, or `None` for an empty slice.
fn argmax(values: &[f32]) -> Option<usize> {
    let mut best: Option<(usize, f32)> = None;
    for (index, &value) in values.iter().enumerate() {
        match best {
            Some((_, current)) if value <= current => {}
            _ => best = Some((index, value)),
        }
    }
    best.map(|(index, _)| index)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels() -> Vec<String> {
        vec!["negative".into(), "positive".into(), "neutral".into()]
    }

    #[test]
    fn argmax_picks_the_largest() {
        assert_eq!(argmax(&[0.1, 0.9, 0.2]), Some(1));
        assert_eq!(argmax(&[3.0, 1.0, 2.0]), Some(0));
        assert_eq!(argmax(&[]), None);
    }

    #[test]
    fn local_classify_maps_logits_to_labels() {
        let raw = InferenceOutputs::Vectors(vec![vec![0.1, 0.9, 0.2], vec![5.0, 0.0, 0.0]]);
        let out = parse_response(Task::Classify, BackendKind::Local, raw, Some(&labels())).unwrap();
        assert_eq!(
            out,
            InferenceOutputs::Text(vec!["positive".into(), "negative".into()])
        );
    }

    #[test]
    fn local_classify_requires_labels() {
        let raw = InferenceOutputs::Vectors(vec![vec![0.1, 0.9]]);
        assert_eq!(
            parse_response(Task::Sentiment, BackendKind::Local, raw, None),
            Err(AdapterError::MissingLabels)
        );
    }

    #[test]
    fn local_classify_rejects_out_of_range_index() {
        let two = vec!["a".to_string(), "b".to_string()];
        let raw = InferenceOutputs::Vectors(vec![vec![0.0, 0.0, 9.0]]);
        assert_eq!(
            parse_response(Task::Classify, BackendKind::Local, raw, Some(&two)),
            Err(AdapterError::LabelIndexOutOfRange { index: 2, len: 2 })
        );
    }

    #[test]
    fn remote_classify_passes_text_through() {
        let raw = InferenceOutputs::Text(vec!["positive".into()]);
        let out =
            parse_response(Task::Classify, BackendKind::Remote, raw, Some(&labels())).unwrap();
        assert_eq!(out, InferenceOutputs::Text(vec!["positive".into()]));
    }

    #[test]
    fn remote_classify_coerces_to_canonical_label() {
        // Wrapped reply + different casing → canonical label by containment.
        let raw = InferenceOutputs::Text(vec![
            "The sentiment is Positive.".into(),
            "NEGATIVE".into(),
            "totally unrelated".into(),
        ]);
        let out =
            parse_response(Task::Sentiment, BackendKind::Remote, raw, Some(&labels())).unwrap();
        assert_eq!(
            out,
            InferenceOutputs::Text(vec![
                "positive".into(),          // containment + canonical casing
                "negative".into(),          // exact, case-insensitive
                "totally unrelated".into(), // no match → trimmed raw fallback
            ])
        );
    }

    #[test]
    fn embed_passes_vectors_through() {
        let raw = InferenceOutputs::Vectors(vec![vec![0.1, 0.2, 0.3]]);
        let out = parse_response(Task::Embed, BackendKind::Remote, raw, None).unwrap();
        assert_eq!(out, InferenceOutputs::Vectors(vec![vec![0.1, 0.2, 0.3]]));
    }

    #[test]
    fn generation_is_remote_only() {
        let raw = InferenceOutputs::Vectors(vec![vec![0.0]]);
        assert_eq!(
            parse_response(Task::Complete, BackendKind::Local, raw, None),
            Err(AdapterError::UnsupportedCombination {
                task: Task::Complete,
                kind: BackendKind::Local,
            })
        );
    }

    #[test]
    fn wrong_shape_is_rejected() {
        let raw = InferenceOutputs::Vectors(vec![vec![0.0]]);
        let err = parse_response(Task::Complete, BackendKind::Remote, raw, None).unwrap_err();
        assert_eq!(
            err,
            AdapterError::UnexpectedOutputShape {
                task: Task::Complete,
                kind: BackendKind::Remote,
                expected: "text",
                got: "vectors",
            }
        );
    }
}
