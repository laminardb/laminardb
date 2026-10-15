//! Golden outputs of every bundled profile over every schema-valid official example and every
//! synthetic fixture. A document without rows still has a golden (`[]`), so zero-row documents
//! are covered too. `FPML_GOLDEN_REGEN=1` rewrites them; `tools/fpml/oracle.py` re-derives every
//! golden from the profile and the XML with an independent parser.

use std::collections::BTreeSet;
use std::path::PathBuf;

use super::{decoder_for, example, fixtures_root, synthetic_fixtures, valid_examples};
use crate::fpml::{golden, BUNDLED_PROFILES};

/// `(golden path relative to the profile directory, document bytes)` for every fixture.
pub(super) fn fixtures() -> Vec<(String, Vec<u8>)> {
    let mut out: Vec<(String, Vec<u8>)> = valid_examples()
        .into_iter()
        .map(|path| (format!("official/{path}.json"), example(&path)))
        .collect();
    for name in synthetic_fixtures() {
        let bytes = std::fs::read(fixtures_root().join("synthetic").join(&name)).unwrap();
        out.push((format!("synthetic/{name}.json"), bytes));
    }
    out
}

pub(super) fn golden_root(profile: &str) -> PathBuf {
    fixtures_root().join("goldens").join(profile)
}

#[test]
fn every_profile_matches_its_goldens_over_every_fixture() {
    let regenerate = std::env::var_os("FPML_GOLDEN_REGEN").is_some();
    let fixtures = fixtures();
    let mut mismatches = Vec::new();
    for (profile, _) in BUNDLED_PROFILES {
        let decoder = decoder_for(profile);
        let root = golden_root(profile);
        let mut expected = BTreeSet::new();
        for (relative, bytes) in &fixtures {
            let batch = decoder
                .decode(bytes)
                .unwrap_or_else(|error| panic!("{profile} {relative}: {error}"));
            let mut rendered = serde_json::to_string_pretty(&golden::rows(&batch)).unwrap();
            rendered.push('\n');
            let path = relative
                .split('/')
                .fold(root.clone(), |path, part| path.join(part));
            expected.insert(path.clone());
            if regenerate {
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(&path, rendered).unwrap();
            } else if std::fs::read_to_string(&path).ok().as_deref() != Some(rendered.as_str()) {
                mismatches.push(format!("{profile}: {relative}"));
            }
        }
        for stale in walk(&root) {
            if !expected.contains(&stale) {
                if regenerate {
                    std::fs::remove_file(&stale).unwrap();
                } else {
                    mismatches.push(format!("{profile}: stale golden {}", stale.display()));
                }
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} goldens differ (FPML_GOLDEN_REGEN=1 rewrites them):\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

pub(super) fn walk(root: &std::path::Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}
