//! Compiled, interned projection plan.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::{SchemaRef, TimeUnit};

use crate::fpml::document::{Interner, NameId};

/// A compiled path: alternatives whose results are concatenated in order. Names are interned
/// in the plan's interner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Selector {
    pub(crate) alternatives: Vec<Alternative>,
    /// Source text, for diagnostics.
    pub(crate) text: Arc<str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Alternative {
    pub(crate) steps: Vec<Step>,
    pub(crate) terminal: Terminal,
}

impl Selector {
    /// Whether every alternative yields elements (no value step).
    pub(crate) fn yields_elements(&self) -> bool {
        self.alternatives
            .iter()
            .all(|alternative| matches!(alternative.terminal, Terminal::Text))
    }

    /// Whether any alternative searches all descendants.
    pub(crate) fn has_descendants(&self) -> bool {
        self.alternatives
            .iter()
            .any(|alternative| alternative.steps.contains(&Step::Descendants))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Step {
    Root,
    Child {
        test: NameTest,
        predicates: Vec<NameTest>,
    },
    Descendants,
    Parent,
    Deref,
    /// Keep the current elements that have a child matching each of these tests.
    Filter(Vec<NameTest>),
    /// Keep the current elements whose name matches.
    SelfTest(NameTest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NameTest {
    Name(NameId),
    Set(Arc<HashSet<NameId>>),
    Any,
}

impl NameTest {
    pub(crate) fn matches(&self, name: NameId) -> bool {
        match self {
            Self::Name(expected) => *expected == name,
            Self::Set(set) => set.contains(&name),
            Self::Any => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Terminal {
    Text,
    Attribute(NameId),
    Name,
    Type,
    Locator,
}

/// Connector metadata a column may carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Meta {
    /// SHA-256 of the exact document bytes (integrity, not identity).
    DocumentSha256,
    /// Size of the document in bytes.
    DocumentBytes,
    /// Local name of the document root.
    Root,
    /// The root's `fpmlVersion` attribute.
    FpmlVersion,
    /// Zero-based ordinal of the row within its document.
    Row,
    /// Rows the document produced.
    RowCount,
    /// Position path of the row element, such as `/dataDocument/trade[2]`.
    RowLocator,
}

pub(crate) const META: &[(&str, Meta)] = &[
    ("document_sha256", Meta::DocumentSha256),
    ("document_bytes", Meta::DocumentBytes),
    ("root", Meta::Root),
    ("fpml_version", Meta::FpmlVersion),
    ("row", Meta::Row),
    ("row_count", Meta::RowCount),
    ("row_locator", Meta::RowLocator),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Source {
    Path(Selector),
    Meta(Meta),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Conversion {
    Utf8 {
        collapse: bool,
    },
    Bool,
    Int32,
    Int64,
    UInt32,
    UInt64,
    Decimal {
        precision: u8,
        scale: i8,
    },
    Date32,
    DateTime,
    Timestamp {
        unit: TimeUnit,
        utc: bool,
        offset_optional: bool,
    },
    Struct(Vec<Column>),
    List(Box<Column>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) source: Source,
    pub(crate) conversion: Conversion,
    pub(crate) required: bool,
}

/// A compiled profile.
#[derive(Debug)]
pub(crate) struct Plan {
    pub(crate) schema: SchemaRef,
    pub(crate) roots: HashSet<NameId>,
    pub(crate) rows: Selector,
    pub(crate) columns: Vec<Column>,
    /// Names the plan uses; the worker extends a clone of it per document.
    pub(crate) interner: Interner,
    pub(crate) fpml_version: NameId,
    pub(crate) release: String,
    /// Upper bound on fixed Arrow bytes one row stages, excluding list items and text.
    pub(crate) row_bytes: usize,
}
