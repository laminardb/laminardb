//! Golden outputs of every bundled profile over every schema-valid official example and every
//! synthetic fixture. A document that yields rows has a JSON golden; the documents that yield none
//! are listed in the profile's `zero-rows.txt`, so every fixture's outcome is pinned; an expected
//! rejection's golden records the error. `FPML_GOLDEN_REGEN=1` rewrites them;
//! `tools/fpml_oracle.py` re-derives every golden from the profile and the XML with an independent
//! parser.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use super::{
    decoder_for, example, expected_rejection, fixtures_root, synthetic_fixtures, valid_examples,
};
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

const ZERO_ROWS: &str = "zero-rows.txt";

#[test]
fn every_profile_matches_its_goldens_over_every_fixture() {
    let regenerate = std::env::var_os("FPML_GOLDEN_REGEN").is_some();
    let fixtures = fixtures();
    let mut mismatches = Vec::new();
    for (profile, _) in BUNDLED_PROFILES {
        let decoder = decoder_for(profile);
        let root = golden_root(profile);
        let mut expected = BTreeSet::new();
        let mut zero_rows = Vec::new();
        for (relative, bytes) in &fixtures {
            let official = relative
                .strip_prefix("official/")
                .and_then(|path| path.strip_suffix(".json"));
            let rejection = official.and_then(|path| expected_rejection(profile, path));
            let value = match (decoder.decode(bytes), rejection) {
                (Ok(batch), None) => serde_json::Value::Array(golden::rows(&batch)),
                (Err(error), Some(reason)) if error.to_string().contains(reason) => {
                    serde_json::json!({ "rejected": error.to_string() })
                }
                (Ok(_), Some(reason)) => {
                    mismatches.push(format!("{profile} {relative}: expected {reason}"));
                    continue;
                }
                (Err(error), _) => {
                    mismatches.push(format!("{profile} {relative}: {error}"));
                    continue;
                }
            };
            if value.as_array().is_some_and(Vec::is_empty) {
                zero_rows.push(relative.trim_end_matches(".json").to_owned());
                continue;
            }
            let mut rendered = serde_json::to_string_pretty(&value).unwrap();
            rendered.push('\n');
            let path = relative
                .split('/')
                .fold(root.clone(), |path, part| path.join(part));
            check_golden(&path, &rendered, regenerate, &mut mismatches);
            expected.insert(path);
        }
        zero_rows.sort();
        let listing = zero_rows.iter().fold(String::new(), |mut listing, path| {
            listing.push_str(path);
            listing.push('\n');
            listing
        });
        let path = root.join(ZERO_ROWS);
        check_golden(&path, &listing, regenerate, &mut mismatches);
        expected.insert(path);
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

fn check_golden(path: &Path, rendered: &str, regenerate: bool, mismatches: &mut Vec<String>) {
    if regenerate {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, rendered).unwrap();
    } else if std::fs::read_to_string(path).ok().as_deref() != Some(rendered) {
        mismatches.push(path.display().to_string());
    }
}

pub(super) fn walk(root: &Path) -> Vec<PathBuf> {
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
