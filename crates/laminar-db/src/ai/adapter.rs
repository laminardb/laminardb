//! Adapt a raw provider response to a task's per-row output. Stays Arrow-free;
//! the operator builds the column from the returned [`InferenceOutputs`].

use thiserror::Error;

use crate::ai::provider::InferenceOutputs;
use crate::ai::registry::{BackendKind, Task};

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
        /// Number of available labels.
        len: usize,
    },

    /// A classifier returned an empty logit vector for a row.
    #[error("classifier returned no logits for a row")]
    EmptyLogits,

    /// Sentiment scoring needs both a `positive` and a `negative` label.
    #[error(
        "sentiment scoring requires 'positive' and 'negative' among the labels, got {labels:?}"
    )]
    SentimentLabelsUnusable {
        /// Labels that were available.
        labels: Vec<String>,
    },

    /// A remote sentiment reply contained no parseable number.
    #[error("remote sentiment reply had no parseable score: {reply:?}")]
    UnparseableScore {
        /// The unparseable reply string.
        reply: String,
    },

    /// The raw output shape did not match what the task/backend produces.
    #[error("task '{task}' on the {kind:?} backend expected {expected} output, got {got}")]
    UnexpectedOutputShape {
        /// The task.
        task: Task,
        /// The backend kind.
        kind: BackendKind,
        /// Expected shape name.
        expected: &'static str,
        /// Actual shape name.
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

/// Map a raw provider response to the task's per-row output.
///
/// `labels` is the candidate/intrinsic label set for classification; required
/// for local classifiers, ignored otherwise.
///
/// # Errors
///
/// Returns [`AdapterError`] if the output shape is wrong, a local classifier
/// has no labels or an out-of-range argmax, or the task/backend combination is
/// unsupported.
pub fn parse_response(
    task: Task,
    kind: BackendKind,
    raw: InferenceOutputs,
    labels: Option<&[String]>,
) -> Result<InferenceOutputs, AdapterError> {
    match task {
        Task::Classify => match kind {
            BackendKind::Local => classify_from_logits(kind, raw, labels),
            BackendKind::Remote => coerce_remote_classification(raw, labels),
        },
        Task::Sentiment => match kind {
            BackendKind::Local => sentiment_from_logits(raw, labels),
            BackendKind::Remote => sentiment_from_text(raw),
        },
        Task::Embed => expect_vectors(task, kind, raw),
        Task::Complete | Task::Summarize | Task::Translate | Task::Gen | Task::Extract => {
            if kind != BackendKind::Remote {
                return Err(AdapterError::UnsupportedCombination { task, kind });
            }
            expect_text(task, kind, raw)
        }
    }
}

/// argmax over each row's logits, mapped to labels.
fn classify_from_logits(
    kind: BackendKind,
    raw: InferenceOutputs,
    labels: Option<&[String]>,
) -> Result<InferenceOutputs, AdapterError> {
    let rows = match raw {
        InferenceOutputs::Vectors(rows) => rows,
        other => {
            return Err(AdapterError::UnexpectedOutputShape {
                task: Task::Classify,
                kind,
                expected: "vectors",
                got: shape_name(&other),
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

/// Normalize each reply to a canonical label (exact then containment match),
/// or fall back to trimmed raw text if none matches.
fn coerce_remote_classification(
    raw: InferenceOutputs,
    labels: Option<&[String]>,
) -> Result<InferenceOutputs, AdapterError> {
    let texts = match raw {
        InferenceOutputs::Text(texts) => texts,
        other => {
            return Err(AdapterError::UnexpectedOutputShape {
                task: Task::Classify,
                kind: BackendKind::Remote,
                expected: "text",
                got: shape_name(&other),
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

/// Exact (case-insensitive) then containment match; falls back to trimmed text.
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

/// Pass text outputs through unchanged, rejecting other shapes.
fn expect_text(
    task: Task,
    kind: BackendKind,
    raw: InferenceOutputs,
) -> Result<InferenceOutputs, AdapterError> {
    match raw {
        InferenceOutputs::Text(text) => Ok(InferenceOutputs::Text(text)),
        other => Err(AdapterError::UnexpectedOutputShape {
            task,
            kind,
            expected: "text",
            got: shape_name(&other),
        }),
    }
}

/// Pass vector outputs through unchanged, rejecting other shapes.
fn expect_vectors(
    task: Task,
    kind: BackendKind,
    raw: InferenceOutputs,
) -> Result<InferenceOutputs, AdapterError> {
    match raw {
        InferenceOutputs::Vectors(vectors) => Ok(InferenceOutputs::Vectors(vectors)),
        other => Err(AdapterError::UnexpectedOutputShape {
            task,
            kind,
            expected: "vectors",
            got: shape_name(&other),
        }),
    }
}

/// Static shape name for error messages.
fn shape_name(raw: &InferenceOutputs) -> &'static str {
    match raw {
        InferenceOutputs::Text(_) => "text",
        InferenceOutputs::Vectors(_) => "vectors",
        InferenceOutputs::Scores(_) => "scores",
    }
}

/// Softmax each row's logits, return `P(positive) − P(negative)` in `[-1, 1]`.
fn sentiment_from_logits(
    raw: InferenceOutputs,
    labels: Option<&[String]>,
) -> Result<InferenceOutputs, AdapterError> {
    let rows = match raw {
        InferenceOutputs::Vectors(rows) => rows,
        other => {
            return Err(AdapterError::UnexpectedOutputShape {
                task: Task::Sentiment,
                kind: BackendKind::Local,
                expected: "vectors",
                got: shape_name(&other),
            });
        }
    };
    let labels = labels.ok_or(AdapterError::MissingLabels)?;
    let pos = labels
        .iter()
        .position(|l| l.eq_ignore_ascii_case("positive"));
    let neg = labels
        .iter()
        .position(|l| l.eq_ignore_ascii_case("negative"));
    let (Some(pos), Some(neg)) = (pos, neg) else {
        return Err(AdapterError::SentimentLabelsUnusable {
            labels: labels.to_vec(),
        });
    };
    let mut scores = Vec::with_capacity(rows.len());
    for logits in rows {
        if logits.is_empty() {
            return Err(AdapterError::EmptyLogits);
        }
        let probs = softmax(&logits);
        let p_pos = probs.get(pos).copied().unwrap_or(0.0);
        let p_neg = probs.get(neg).copied().unwrap_or(0.0);
        scores.push(f64::from(p_pos - p_neg));
    }
    Ok(InferenceOutputs::Scores(scores))
}

/// Parse a number from each reply and clamp to `[-1, 1]`.
fn sentiment_from_text(raw: InferenceOutputs) -> Result<InferenceOutputs, AdapterError> {
    let texts = match raw {
        InferenceOutputs::Text(texts) => texts,
        other => {
            return Err(AdapterError::UnexpectedOutputShape {
                task: Task::Sentiment,
                kind: BackendKind::Remote,
                expected: "text",
                got: shape_name(&other),
            });
        }
    };
    let mut scores = Vec::with_capacity(texts.len());
    for reply in texts {
        let value = parse_score(&reply).ok_or(AdapterError::UnparseableScore { reply })?;
        scores.push(value.clamp(-1.0, 1.0));
    }
    Ok(InferenceOutputs::Scores(scores))
}

/// Numerically stable softmax.
fn softmax(logits: &[f32]) -> Vec<f32> {
    let max = logits.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    let mut exps: Vec<f32> = logits.iter().map(|&v| (v - max).exp()).collect();
    let sum: f32 = exps.iter().sum();
    if sum > 0.0 {
        for e in &mut exps {
            *e /= sum;
        }
    }
    exps
}

/// Extract the first number from a reply string, or `None`.
fn parse_score(reply: &str) -> Option<f64> {
    let trimmed = reply.trim();
    if let Ok(v) = trimmed.parse::<f64>() {
        return Some(v);
    }
    trimmed.split_whitespace().find_map(|token| {
        // Strip punctuation: leading sign/digit may start; only a digit may end
        // (so a sentence-final '.' isn't kept).
        token
            .trim_start_matches(|c: char| !(c.is_ascii_digit() || c == '-'))
            .trim_end_matches(|c: char| !c.is_ascii_digit())
            .parse::<f64>()
            .ok()
    })
}

/// Index of the maximum value, or `None` if empty.
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
    fn local_classify_argmaxes_logits_to_labels() {
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
    fn remote_classify_coerces_to_canonical_label() {
        // Exact (case-insensitive), containment in a wrapped reply, and the
        // no-match fallback to trimmed raw text.
        let raw = InferenceOutputs::Text(vec![
            "The sentiment is Positive.".into(),
            "NEGATIVE".into(),
            "totally unrelated".into(),
        ]);
        let out =
            parse_response(Task::Classify, BackendKind::Remote, raw, Some(&labels())).unwrap();
        assert_eq!(
            out,
            InferenceOutputs::Text(vec![
                "positive".into(),
                "negative".into(),
                "totally unrelated".into(),
            ])
        );
    }

    #[test]
    fn local_sentiment_softmaxes_logits_to_a_signed_score() {
        // labels = [negative, positive, neutral]. Row 1 favours positive, row 2
        // favours negative. Score = P(pos) − P(neg) ∈ [-1, 1].
        let raw = InferenceOutputs::Vectors(vec![vec![0.0, 2.0, 0.0], vec![3.0, 0.0, 0.0]]);
        let out =
            parse_response(Task::Sentiment, BackendKind::Local, raw, Some(&labels())).unwrap();
        let InferenceOutputs::Scores(scores) = out else {
            panic!("sentiment is numeric");
        };
        // Row 1: softmax([0,2,0]) → P(neg)=P(neu)=e^0/(2+e^2), P(pos)=e^2/(2+e^2).
        let denom = 2.0 + std::f32::consts::E.powi(2);
        let expected0 = f64::from((std::f32::consts::E.powi(2) - 1.0) / denom);
        assert!((scores[0] - expected0).abs() < 1e-6, "got {}", scores[0]);
        assert!(scores[0] > 0.0 && scores[1] < 0.0);
        assert!((-1.0..=1.0).contains(&scores[0]) && (-1.0..=1.0).contains(&scores[1]));
    }

    #[test]
    fn local_sentiment_needs_positive_and_negative_labels() {
        let raw = InferenceOutputs::Vectors(vec![vec![0.1, 0.9]]);
        let stars = vec!["one_star".to_string(), "five_star".to_string()];
        assert!(matches!(
            parse_response(Task::Sentiment, BackendKind::Local, raw, Some(&stars)),
            Err(AdapterError::SentimentLabelsUnusable { .. })
        ));
    }

    #[test]
    fn remote_sentiment_parses_and_clamps_a_number() {
        let raw = InferenceOutputs::Text(vec![
            "0.8".into(),
            "The sentiment is -0.5.".into(),
            "2.0".into(), // out of range → clamped
        ]);
        let out = parse_response(Task::Sentiment, BackendKind::Remote, raw, None).unwrap();
        let InferenceOutputs::Scores(scores) = out else {
            panic!("sentiment is numeric");
        };
        assert!((scores[0] - 0.8).abs() < 1e-12);
        assert!((scores[1] + 0.5).abs() < 1e-12);
        assert!((scores[2] - 1.0).abs() < 1e-12);
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
}
