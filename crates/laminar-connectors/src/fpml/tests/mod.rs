//! FpML qualification suites over the official corpus and LaminarDB's synthetic fixtures.

mod corpus;
mod coverage;
mod goldens;

use std::path::{Path, PathBuf};

use super::{FpmlConfig, FpmlDecoder, Limits, ProfileSource, ReferencePolicy};

#[derive(serde::Deserialize)]
pub(super) struct Corpus {
    pub(super) example: Vec<CorpusEntry>,
}

#[derive(serde::Deserialize)]
pub(super) struct CorpusEntry {
    pub(super) path: String,
    pub(super) status: String,
    pub(super) errata: Option<String>,
}

pub(super) fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fpml")
}

pub(super) fn corpus_root() -> PathBuf {
    fixtures_root().join("fpml-5-13-8-rec-2")
}

pub(super) fn corpus() -> Corpus {
    toml::from_str(&std::fs::read_to_string(corpus_root().join("corpus.toml")).unwrap()).unwrap()
}

/// Schema-valid official examples, as paths relative to the corpus `confirmation/` directory.
pub(super) fn valid_examples() -> Vec<String> {
    corpus()
        .example
        .into_iter()
        .filter(|entry| entry.status == "xsd-valid")
        .map(|entry| entry.path)
        .collect()
}

/// LaminarDB-authored fixtures (not FpML examples), as paths relative to `synthetic/`.
pub(super) fn synthetic_fixtures() -> Vec<String> {
    let root = fixtures_root().join("synthetic");
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(&root) else {
        return found;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|extension| extension == "xml") {
            found.push(path.file_name().unwrap().to_string_lossy().into_owned());
        }
    }
    found.sort();
    found
}

/// Schema-valid official examples a bundled profile rejects, with the reason. Each is an FpML
/// example erratum: a reference the profile projects through targets an element of the wrong
/// type, which the schema annotation forbids and projection never follows.
const EXPECTED_REJECTIONS: &[(&str, &str, &str)] = &[];

pub(super) fn expected_rejection(profile: &str, path: &str) -> Option<&'static str> {
    EXPECTED_REJECTIONS
        .iter()
        .find(|(rejecting, rejected, _)| *rejecting == profile && *rejected == path)
        .map(|(_, _, reason)| *reason)
}

pub(super) fn example(path: &str) -> Vec<u8> {
    std::fs::read(corpus_root().join("confirmation").join(path)).unwrap()
}

pub(super) fn decoder_for(profile: &str) -> FpmlDecoder {
    let config = FpmlConfig {
        profile: ProfileSource::Bundled(profile.into()),
        schema_path: None,
        references: ReferencePolicy::Resolve,
        limits: Limits::default(),
    };
    let (artifact, profile) = config.resolve().unwrap();
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(FpmlDecoder::start(
            artifact, &profile, &config, "test", None, None,
        ))
        .unwrap()
}
