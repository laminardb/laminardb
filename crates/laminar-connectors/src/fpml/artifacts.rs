//! Pinned schema closures: the bundled FpML release and operator-supplied closures.
//!
//! A closure is a flat set of named schema files, an entry point, and a manifest of SHA-256
//! digests. Every file is verified before use, and its digest set defines the closure's
//! fingerprint, which is recorded in the source's schema contract. Nothing is ever downloaded.

use std::collections::BTreeMap;
use std::path::{Component, Path};
use std::sync::Arc;

use serde::de::IgnoredAny;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::xsd::SchemaFiles;
use crate::error::ConnectorError;

/// Identifier of the bundled FpML 5.13 REC-2 confirmation-view closure.
pub const BUNDLED_CONFIRMATION: &str = "fpml-5-13-8-rec-2/confirmation";

const BUNDLED_MANIFEST: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fpml/artifacts/fpml-5-13-8-rec-2/closure.toml"
));

macro_rules! bundled_files {
    ($($name:literal),* $(,)?) => {
        &[$(($name, include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fpml/artifacts/fpml-5-13-8-rec-2/confirmation/",
            $name
        )) as &[u8])),*]
    };
}

const BUNDLED_FILES: &[(&str, &[u8])] = bundled_files!(
    "fpml-accumulator-5-13.xsd",
    "fpml-asset-5-13.xsd",
    "fpml-bond-option-5-13.xsd",
    "fpml-business-events-5-13.xsd",
    "fpml-cd-5-13.xsd",
    "fpml-clearing-processes-5-13.xsd",
    "fpml-com-5-13.xsd",
    "fpml-confirmation-processes-5-13.xsd",
    "fpml-correlation-swaps-5-13.xsd",
    "fpml-credit-event-notification-5-13.xsd",
    "fpml-dividend-swaps-5-13.xsd",
    "fpml-doc-5-13.xsd",
    "fpml-enum-5-13.xsd",
    "fpml-eq-shared-5-13.xsd",
    "fpml-eqd-5-13.xsd",
    "fpml-fx-5-13.xsd",
    "fpml-fx-accruals-5-13.xsd",
    "fpml-fx-targets-5-13.xsd",
    "fpml-generic-5-13.xsd",
    "fpml-ird-5-13.xsd",
    "fpml-loan-5-13.xsd",
    "fpml-main-5-13.xsd",
    "fpml-mktenv-5-13.xsd",
    "fpml-msg-5-13.xsd",
    "fpml-option-shared-5-13.xsd",
    "fpml-reg-reporting-shared-5-13.xsd",
    "fpml-repo-5-13.xsd",
    "fpml-return-swaps-5-13.xsd",
    "fpml-riskdef-5-13.xsd",
    "fpml-sec-lending-5-13.xsd",
    "fpml-shared-5-13.xsd",
    "fpml-standard-5-13.xsd",
    "fpml-valuation-5-13.xsd",
    "fpml-variance-swaps-5-13.xsd",
    "fpml-volatility-swaps-5-13.xsd",
    "xmldsig-core-schema.xsd",
);

/// Largest schema file accepted in an operator-supplied closure.
const MAX_SCHEMA_FILE_BYTES: u64 = 16 * 1024 * 1024;
/// Most files in one closure.
const MAX_SCHEMA_FILES: usize = 256;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    artifact: String,
    /// Bundled closure whose files this closure extends.
    base: Option<String>,
    release: String,
    #[serde(default)]
    build: Option<u32>,
    /// Provenance only: publication status, download location, and retrieval date.
    #[serde(default, rename = "status")]
    _status: Option<IgnoredAny>,
    #[serde(default, rename = "source")]
    _source: Option<IgnoredAny>,
    #[serde(default, rename = "retrieved")]
    _retrieved: Option<IgnoredAny>,
    view: String,
    namespace: String,
    /// Additional target namespaces whose elements carry FpML `id`/`href` semantics.
    #[serde(default)]
    extension_namespaces: Vec<String>,
    entry: String,
    files: BTreeMap<String, String>,
}

/// A verified schema closure.
#[derive(Debug, Clone)]
pub struct Artifact {
    /// Closure identifier (for example `fpml-5-13-8-rec-2/confirmation`).
    pub id: String,
    /// Publication build number, when the closure states one.
    pub build: Option<u32>,
    /// FpML release required in each document's `fpmlVersion` attribute (for example `5-13`).
    pub release: String,
    /// FpML view (for example `confirmation`).
    pub view: String,
    /// Namespace of document roots.
    pub namespace: String,
    /// Namespaces whose elements carry `id`/`href` semantics.
    pub reference_namespaces: Vec<String>,
    /// SHA-256 over the entry point, namespaces, and every file digest.
    pub fingerprint: String,
    pub(crate) files: SchemaFiles,
}

impl Artifact {
    /// The bundled FpML 5.13 REC-2 confirmation-view closure, verified against its manifest.
    ///
    /// # Errors
    /// Returns an error if an embedded file does not match its pinned digest.
    pub fn bundled() -> Result<Self, ConnectorError> {
        let manifest = parse_manifest(BUNDLED_MANIFEST, "bundled closure manifest")?;
        let files = BUNDLED_FILES
            .iter()
            .map(|(name, bytes)| ((*name).to_owned(), Arc::<[u8]>::from(*bytes)))
            .collect::<BTreeMap<_, _>>();
        Self::verify(manifest, files, None)
    }

    /// Resolve `fpml.schema.path` (a directory holding `closure.toml`) or the bundled closure.
    ///
    /// # Errors
    /// Returns an error for a missing, malformed, oversized, or digest-mismatched closure.
    pub fn resolve(path: Option<&str>) -> Result<Self, ConnectorError> {
        match path {
            None => Self::bundled(),
            Some(directory) => Self::load(Path::new(directory)),
        }
    }

    fn load(directory: &Path) -> Result<Self, ConnectorError> {
        let manifest_path = directory.join("closure.toml");
        let text = std::fs::read_to_string(&manifest_path).map_err(|error| {
            config_error(format!(
                "cannot read schema closure manifest '{}': {error}",
                manifest_path.display()
            ))
        })?;
        let manifest = parse_manifest(&text, "schema closure manifest")?;
        let mut files = BTreeMap::new();
        let base = match manifest.base.as_deref() {
            None => None,
            Some(BUNDLED_CONFIRMATION) => Some(Self::bundled()?),
            Some(other) => {
                return Err(config_error(format!(
                    "schema closure base '{other}' is not a bundled closure"
                )))
            }
        };
        if manifest.files.len() > MAX_SCHEMA_FILES {
            return Err(config_error(format!(
                "schema closure lists more than {MAX_SCHEMA_FILES} files"
            )));
        }
        for name in manifest.files.keys() {
            let relative = Path::new(name);
            if relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
            {
                return Err(config_error(format!(
                    "schema closure file '{name}' must be a relative path inside the closure"
                )));
            }
            let path = directory.join(relative);
            let size = std::fs::metadata(&path)
                .map_err(|error| {
                    config_error(format!("cannot read schema file '{}': {error}", path.display()))
                })?
                .len();
            if size > MAX_SCHEMA_FILE_BYTES {
                return Err(config_error(format!(
                    "schema file '{name}' exceeds {MAX_SCHEMA_FILE_BYTES} bytes"
                )));
            }
            let bytes = std::fs::read(&path).map_err(|error| {
                config_error(format!("cannot read schema file '{}': {error}", path.display()))
            })?;
            files.insert(name.clone(), Arc::<[u8]>::from(bytes));
        }
        Self::verify(manifest, files, base)
    }

    fn verify(
        manifest: Manifest,
        mut files: BTreeMap<String, Arc<[u8]>>,
        base: Option<Self>,
    ) -> Result<Self, ConnectorError> {
        let listed = manifest.files.keys().collect::<Vec<_>>();
        let present = files.keys().collect::<Vec<_>>();
        if listed != present {
            return Err(config_error(format!(
                "schema closure '{}' files do not match its manifest",
                manifest.artifact
            )));
        }
        for (name, expected) in &manifest.files {
            let actual = hex(&Sha256::digest(&files[name]));
            if !actual.eq_ignore_ascii_case(expected) {
                return Err(config_error(format!(
                    "schema closure '{}' file '{name}' has SHA-256 {actual}, manifest pins {expected}",
                    manifest.artifact
                )));
            }
        }
        let mut digest = Sha256::new();
        let mut reference_namespaces = vec![manifest.namespace.clone()];
        if let Some(base) = &base {
            digest.update(base.fingerprint.as_bytes());
            for namespace in &base.reference_namespaces {
                if !reference_namespaces.contains(namespace) {
                    reference_namespaces.push(namespace.clone());
                }
            }
            for (name, bytes) in &base.files.files {
                if files.insert(name.clone(), Arc::clone(bytes)).is_some() {
                    return Err(config_error(format!(
                        "schema closure '{}' redefines base file '{name}'",
                        manifest.artifact
                    )));
                }
            }
        }
        for namespace in &manifest.extension_namespaces {
            if !reference_namespaces.contains(namespace) {
                reference_namespaces.push(namespace.clone());
            }
        }
        if !files.contains_key(&manifest.entry) {
            return Err(config_error(format!(
                "schema closure entry '{}' is not a closure file",
                manifest.entry
            )));
        }
        for part in [&manifest.artifact, &manifest.entry, &manifest.namespace] {
            digest.update(part.as_bytes());
            digest.update([0]);
        }
        for namespace in &reference_namespaces {
            digest.update(namespace.as_bytes());
            digest.update([0]);
        }
        for (name, sha) in &manifest.files {
            digest.update(name.as_bytes());
            digest.update([0]);
            digest.update(sha.to_ascii_lowercase().as_bytes());
            digest.update([b'\n']);
        }
        Ok(Self {
            id: manifest.artifact,
            build: manifest.build,
            release: manifest.release,
            view: manifest.view,
            namespace: manifest.namespace,
            reference_namespaces,
            fingerprint: hex(&digest.finalize()),
            files: SchemaFiles {
                entry: manifest.entry,
                files,
            },
        })
    }
}

fn parse_manifest(text: &str, what: &str) -> Result<Manifest, ConnectorError> {
    toml::from_str(text).map_err(|error| config_error(format!("invalid {what}: {error}")))
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn config_error(message: String) -> ConnectorError {
    ConnectorError::ConfigurationError(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_closure_matches_its_pinned_digests() {
        let artifact = Artifact::bundled().unwrap();
        assert_eq!(artifact.id, BUNDLED_CONFIRMATION);
        assert_eq!(artifact.release, "5-13");
        assert_eq!(artifact.files.files.len(), 36);
        assert_eq!(artifact.files.entry, "fpml-main-5-13.xsd");
        assert_eq!(Artifact::bundled().unwrap().fingerprint, artifact.fingerprint);
    }

    #[test]
    fn tampered_or_escaping_closures_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("a.xsd"), "<x/>").unwrap();
        let manifest = |files: &str| {
            format!(
                "artifact = \"t\"\nrelease = \"5-13\"\nview = \"confirmation\"\nnamespace = \"urn:t\"\nentry = \"a.xsd\"\n[files]\n{files}\n"
            )
        };
        std::fs::write(
            directory.path().join("closure.toml"),
            manifest("\"a.xsd\" = \"0000000000000000000000000000000000000000000000000000000000000000\""),
        )
        .unwrap();
        let error = Artifact::resolve(Some(directory.path().to_str().unwrap())).unwrap_err();
        assert!(error.to_string().contains("manifest pins"), "{error}");

        std::fs::write(
            directory.path().join("closure.toml"),
            manifest("\"../a.xsd\" = \"00\""),
        )
        .unwrap();
        let error = Artifact::resolve(Some(directory.path().to_str().unwrap())).unwrap_err();
        assert!(error.to_string().contains("relative path"), "{error}");
    }
}
