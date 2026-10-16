//! The coverage manifest gate.
//!
//! The oracle is the pinned closure, not a hand-written list: every `product` substitution
//! member, every loan facility and loan event representation, and every global element derived
//! from `Document`. The manifest must list exactly those, and every one must be `typed`: name a
//! bundled profile and a column that profile has, with goldens showing that column populated
//! for that representation. An `outstanding` entry is a release blocker and fails the gate.
//!
//! The same evidence renders `docs/FPML_SUPPORT_MATRIX.md`; the test fails when that file is
//! stale, and `FPML_GOLDEN_REGEN=1` rewrites it.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde::Deserialize;

use super::goldens::{golden_root, walk};
use crate::fpml::artifacts::Artifact;
use crate::fpml::profile::{Profile, ProfileSource};
use crate::fpml::schema_model::{QName, SchemaModel};

const LOAN_GROUPS: &[&str] = &[
    "facilityGroup",
    "facilityEventGroup",
    "loanContractEventGroup",
    "lcEventGroup",
    "loanAllocationEventGroup",
    "loanTradeEventGroup",
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    closure: Closure,
    product: Vec<Entry>,
    loan: Vec<Entry>,
    document: Vec<Entry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Closure {
    artifact: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    element: String,
    #[serde(rename = "type")]
    ty: String,
    #[serde(default)]
    asset_class: Option<String>,
    #[serde(default)]
    group: Option<String>,
    status: Status,
    #[serde(default)]
    profile: Option<String>,
    #[serde(default)]
    column: Option<String>,
    /// Synthetic fixture file names, for representations without official examples.
    #[serde(default)]
    synthetic: Vec<String>,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Status {
    Typed,
    Outstanding,
}

/// Which golden fields identify the representation a row belongs to.
#[derive(Clone, Copy)]
enum Section {
    Product,
    Loan,
    Document,
}

impl Section {
    fn discriminators(self) -> &'static [&'static str] {
        match self {
            Self::Product => &["product_type"],
            Self::Loan => &["event_type", "facility_type"],
            Self::Document => &["document_root"],
        }
    }
}

pub(super) fn manifest_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fpml/coverage.toml")
}

#[test]
fn coverage_manifest_matches_the_closure_and_its_evidence() {
    let manifest: Manifest =
        toml::from_str(&std::fs::read_to_string(manifest_path()).unwrap()).unwrap();
    let artifact = Artifact::bundled().unwrap();
    assert_eq!(manifest.closure.artifact, artifact.id);
    let model = SchemaModel::for_artifact(&artifact).unwrap();
    let fpml = |local: &str| -> QName { (artifact.namespace.clone(), local.to_owned()) };
    let locals = |names: BTreeSet<QName>| -> BTreeSet<String> {
        names.into_iter().map(|(_, local)| local).collect()
    };

    let products = locals(model.substitution_members(&fpml("product")));
    let loans = locals(
        LOAN_GROUPS
            .iter()
            .flat_map(|group| model.substitution_members(&fpml(group)))
            .collect(),
    );
    let documents = locals(model.globals_deriving_from(&fpml("Document")));
    let mut problems = Vec::new();
    let mut summary = BTreeMap::new();
    let mut goldens = Goldens::default();
    let mut matrix = String::new();
    for (section, label, oracle, entries) in [
        (Section::Product, "product", products, &manifest.product),
        (Section::Loan, "loan", loans, &manifest.loan),
        (Section::Document, "document", documents, &manifest.document),
    ] {
        let listed: BTreeSet<String> = entries.iter().map(|entry| entry.element.clone()).collect();
        for missing in oracle.difference(&listed) {
            problems.push(format!(
                "{label} '{missing}' is in the closure but not the manifest"
            ));
        }
        for extra in listed.difference(&oracle) {
            problems.push(format!(
                "{label} '{extra}' is in the manifest but not the closure"
            ));
        }
        if listed.len() != entries.len() {
            problems.push(format!("{label} entries are listed more than once"));
        }
        matrix_section(&mut matrix, label);
        for entry in entries {
            check_types(&model, &artifact, entry, &mut problems);
            let evidence = match entry.status {
                Status::Typed => check_typed(
                    section,
                    label,
                    entry,
                    &artifact,
                    &mut goldens,
                    &mut problems,
                ),
                Status::Outstanding => {
                    problems.push(format!(
                        "{label} '{}' is outstanding: a release blocker",
                        entry.element
                    ));
                    None
                }
            };
            matrix_row(&mut matrix, entry, evidence);
            let counts = summary.entry(label).or_insert((0, 0));
            match entry.status {
                Status::Typed => counts.0 += 1,
                Status::Outstanding => counts.1 += 1,
            }
        }
    }
    for (label, (typed, outstanding)) in &summary {
        eprintln!("FpML coverage: {label}: {typed} typed, {outstanding} outstanding");
    }
    check_matrix(&manifest.closure.artifact, &summary, &matrix, &mut problems);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

const MATRIX: &str = "../../docs/FPML_SUPPORT_MATRIX.md";

fn matrix_section(out: &mut String, label: &str) {
    let classification = if label == "document" { "Kind" } else { "Class" };
    let _ = write!(
        out,
        "\n## {}\n\n| {classification} | Element | Schema type | Profile | Column | Official rows | Synthetic rows | Synthetic fixtures |\n|---|---|---|---|---|---|---|---|\n",
        match label {
            "product" => "Products",
            "loan" => "Loan facilities and events",
            _ => "Documents",
        }
    );
}

fn matrix_row(out: &mut String, entry: &Entry, evidence: Option<(usize, usize)>) {
    let (official, synthetic) = evidence.unwrap_or_default();
    let classification = entry
        .asset_class
        .as_deref()
        .or(entry.group.as_deref())
        .unwrap_or("document");
    let _ = writeln!(
        out,
        "| {classification} | `{}` | `{}` | {} | {} | {official} | {synthetic} | {} |",
        entry.element,
        entry.ty,
        entry
            .profile
            .as_deref()
            .map_or("outstanding".into(), |profile| format!("`{profile}`")),
        entry
            .column
            .as_deref()
            .map_or(String::new(), |column| format!("`{column}`")),
        entry.synthetic.join(", "),
    );
}

fn check_matrix(
    artifact: &str,
    summary: &BTreeMap<&str, (usize, usize)>,
    matrix: &str,
    problems: &mut Vec<String>,
) {
    let typed = |label: &str| summary.get(label).copied().unwrap_or_default();
    let line = |label: &str, what: &str| {
        let (typed, outstanding) = typed(label);
        format!("{typed} of {} {what}", typed + outstanding)
    };
    let rendered = format!(
        "# FpML support matrix\n\n\
         Generated from `crates/laminar-connectors/fpml/coverage.toml` and the golden outputs by the\n\
         coverage test; do not edit by hand (`FPML_GOLDEN_REGEN=1` with the `fpml` feature rewrites\n\
         it). Closure `{artifact}`. Typed: {}, {}, {}.\n\n\
         Rows are golden outputs of the bundled profile for that representation: official FpML\n\
         examples, and LaminarDB synthetic fixtures where the official corpus has none. Synthetic\n\
         fixtures are test evidence, not venue certification.\n{matrix}",
        line("product", "products"),
        line("loan", "loan representations"),
        line("document", "document roots"),
    );
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(MATRIX);
    if std::env::var_os("FPML_GOLDEN_REGEN").is_some() {
        std::fs::write(&path, &rendered).unwrap();
    } else if std::fs::read_to_string(&path).ok().as_deref() != Some(rendered.as_str()) {
        problems.push(format!(
            "{} is stale (FPML_GOLDEN_REGEN=1 rewrites it)",
            path.display()
        ));
    }
}

fn check_types(
    model: &SchemaModel,
    artifact: &Artifact,
    entry: &Entry,
    problems: &mut Vec<String>,
) {
    let declared = model
        .globals
        .get(&(artifact.namespace.clone(), entry.element.clone()))
        .cloned()
        .flatten()
        .map(|(_, local)| local);
    if declared.as_deref() != Some(entry.ty.as_str()) {
        problems.push(format!(
            "'{}' is declared with type {declared:?}, the manifest says '{}'",
            entry.element, entry.ty
        ));
    }
    if entry.asset_class.as_deref() == Some("") || entry.group.as_deref() == Some("") {
        problems.push(format!("'{}' has an empty classification", entry.element));
    }
}

/// Golden rows per profile, read once, each marked synthetic or official.
#[derive(Default)]
struct Goldens(BTreeMap<String, Vec<(bool, serde_json::Value)>>);

impl Goldens {
    fn rows(&mut self, profile: &str) -> &[(bool, serde_json::Value)] {
        self.0.entry(profile.to_owned()).or_insert_with(|| {
            let root = golden_root(profile);
            let mut rows = Vec::new();
            for path in walk(&root) {
                let synthetic = path.strip_prefix(&root).is_ok_and(|relative| {
                    relative
                        .components()
                        .next()
                        .is_some_and(|first| first.as_os_str() == "synthetic")
                });
                let Ok(text) = std::fs::read_to_string(&path) else {
                    continue;
                };
                if let Ok(golden) = serde_json::from_str::<Vec<serde_json::Value>>(&text) {
                    rows.extend(golden.into_iter().map(|row| (synthetic, row)));
                }
            }
            rows
        })
    }
}

/// Check a typed entry; returns its evidence as (official, synthetic) golden rows.
fn check_typed(
    section: Section,
    label: &str,
    entry: &Entry,
    artifact: &Artifact,
    goldens: &mut Goldens,
    problems: &mut Vec<String>,
) -> Option<(usize, usize)> {
    if entry.notes.as_deref().unwrap_or("").is_empty() {
        problems.push(format!(
            "typed {label} '{}' needs notes disclosing what is mapped and what is retained only",
            entry.element
        ));
    }
    let (Some(profile_name), Some(column)) = (&entry.profile, &entry.column) else {
        problems.push(format!(
            "typed {label} '{}' needs a profile and a column",
            entry.element
        ));
        return None;
    };
    let profile = match Profile::load(&ProfileSource::Bundled(profile_name.clone()), artifact) {
        Ok(profile) => profile,
        Err(error) => {
            problems.push(format!("typed {label} '{}': {error}", entry.element));
            return None;
        }
    };
    if profile.schema().field_with_name(column).is_err() {
        problems.push(format!(
            "typed {label} '{}': profile '{profile_name}' has no column '{column}'",
            entry.element
        ));
    }
    for name in &entry.synthetic {
        let path = super::fixtures_root().join("synthetic").join(name);
        if !path.exists() {
            problems.push(format!(
                "typed {label} '{}': missing synthetic fixture {name}",
                entry.element
            ));
        }
    }
    let (mut official, mut synthetic) = (0, 0);
    for (from_synthetic, row) in goldens.rows(profile_name) {
        let matches = section
            .discriminators()
            .iter()
            .any(|field| row[*field] == entry.element.as_str());
        if matches && !row[column.as_str()].is_null() {
            if *from_synthetic {
                synthetic += 1;
            } else {
                official += 1;
            }
        }
    }
    if official + synthetic == 0 {
        problems.push(format!(
            "typed {label} '{}': no golden row of profile '{profile_name}' has '{column}' populated",
            entry.element
        ));
    }
    Some((official, synthetic))
}
