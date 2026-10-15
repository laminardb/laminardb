//! The coverage manifest gate.
//!
//! The oracle is the pinned closure, not a hand-written list: every `product` substitution
//! member, every loan facility and loan event representation, and every global element derived
//! from `Document`. The manifest must list exactly those. A `typed` entry must name a bundled
//! profile and a column that profile has, and goldens must show that column populated for that
//! representation. `outstanding` entries are reported; `FPML_RELEASE_GATE=1` fails on any.

use std::collections::{BTreeMap, BTreeSet};

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

/// Which golden field identifies the representation a row belongs to.
#[derive(Clone, Copy)]
enum Section {
    Product,
    Loan,
    Document,
}

impl Section {
    fn discriminator(self) -> &'static str {
        match self {
            Self::Product => "product_type",
            Self::Loan => "event_type",
            Self::Document => "document_root",
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
        for entry in entries {
            check_types(&model, &artifact, entry, &mut problems);
            if entry.status == Status::Typed {
                check_typed(
                    section,
                    label,
                    entry,
                    &artifact,
                    &mut goldens,
                    &mut problems,
                );
            }
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
    assert!(problems.is_empty(), "{}", problems.join("\n"));
    if std::env::var_os("FPML_RELEASE_GATE").is_some() {
        let outstanding: usize = summary.values().map(|(_, outstanding)| outstanding).sum();
        assert_eq!(
            outstanding, 0,
            "the release gate requires every entry typed"
        );
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

/// Golden rows per profile, read once.
#[derive(Default)]
struct Goldens(BTreeMap<String, Vec<serde_json::Value>>);

impl Goldens {
    fn rows(&mut self, profile: &str) -> &[serde_json::Value] {
        self.0.entry(profile.to_owned()).or_insert_with(|| {
            walk(&golden_root(profile))
                .into_iter()
                .filter_map(|path| std::fs::read_to_string(path).ok())
                .filter_map(|text| serde_json::from_str::<Vec<serde_json::Value>>(&text).ok())
                .flatten()
                .collect()
        })
    }
}

fn check_typed(
    section: Section,
    label: &str,
    entry: &Entry,
    artifact: &Artifact,
    goldens: &mut Goldens,
    problems: &mut Vec<String>,
) {
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
        return;
    };
    let profile = match Profile::load(&ProfileSource::Bundled(profile_name.clone()), artifact) {
        Ok(profile) => profile,
        Err(error) => {
            problems.push(format!("typed {label} '{}': {error}", entry.element));
            return;
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
    let evidence = goldens
        .rows(profile_name)
        .iter()
        .filter(|row| row[section.discriminator()] == entry.element.as_str())
        .filter(|row| !row[column.as_str()].is_null())
        .count();
    if evidence == 0 {
        problems.push(format!(
            "typed {label} '{}': no golden row of profile '{profile_name}' has '{column}' populated",
            entry.element
        ));
    }
}
