//! Typed, bounded document rejections.

use std::fmt;

/// The decoding stage that rejected a document. Used as a bounded metrics label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stage {
    /// Encoding, DTD, processing-instruction and size checks before libxml2 sees the bytes.
    Prescan,
    /// XML well-formedness and namespaces (libxml2 parser).
    Parse,
    /// XML Schema validation against the pinned closure.
    Schema,
    /// Root element, view namespace, and `fpmlVersion` checks.
    Root,
    /// `id` uniqueness, `href` resolution, and reference target types.
    References,
    /// Conversion into the profile's Arrow contract.
    Projection,
    /// The validator worker stopped or failed internally.
    Worker,
}

impl Stage {
    /// Stable lower-case label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Prescan => "prescan",
            Self::Parse => "parse",
            Self::Schema => "schema",
            Self::Root => "root",
            Self::References => "references",
            Self::Projection => "projection",
            Self::Worker => "worker",
        }
    }
}

impl fmt::Display for Stage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Longest rejection detail kept; diagnostics never carry whole documents.
const MAX_DETAIL: usize = 1024;

/// Why one document was rejected. The detail is bounded and never contains raw document text
/// beyond schema-vocabulary names and line numbers.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("FpML document rejected at {stage}: {detail}")]
pub struct Rejection {
    /// Stage that rejected the document.
    pub stage: Stage,
    /// Bounded human-readable reason.
    pub detail: String,
}

impl Rejection {
    pub(crate) fn new(stage: Stage, detail: impl Into<String>) -> Self {
        let mut detail = detail.into();
        if detail.len() > MAX_DETAIL {
            let mut end = MAX_DETAIL;
            while !detail.is_char_boundary(end) {
                end -= 1;
            }
            detail.truncate(end);
            detail.push('…');
        }
        Self { stage, detail }
    }
}
