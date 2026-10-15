//! Projection profiles: which elements become rows and how values map to typed Arrow columns.
//!
//! A profile is a TOML file with a `[profile]` header, `[[columns]]`, and optional
//! `[namespaces]`, `[sets]`, and `[structs.<template>]` sections. `include` pulls in other files
//! (relative to the including file, or `bundled:<path>` for a bundled file) that contribute
//! namespaces, sets, and struct templates. Profiles are compiled against the pinned schema
//! closure before activation: unknown element or attribute names, unknown templates, and
//! incompatible types fail configuration, never ingestion.

mod bundled;
mod compile;
mod plan;
mod selector;
mod spec;
mod types;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha2::{Digest, Sha256};

pub(crate) use plan::{
    Alternative, Column, Conversion, Meta, Plan, Selector, Source, Step, Terminal,
};
pub(crate) use types::UTC_ZONE;

use self::compile::Merged;
use self::spec::ProfileFile;
use super::artifacts::{hex, Artifact};
use super::schema_model::SchemaModel;
use crate::error::ConnectorError;

pub use bundled::BUNDLED_PROFILES;

const BUNDLED_PREFIX: &str = "bundled:";
const MAX_INCLUDE_DEPTH: usize = 8;
const MAX_PROFILE_FILES: usize = 256;
const MAX_PROFILE_FILE_BYTES: u64 = 4 * 1024 * 1024;

/// Where a profile comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileSource {
    /// A profile shipped with LaminarDB, by name (see [`BUNDLED_PROFILES`]).
    Bundled(String),
    /// An operator-supplied profile file.
    File(PathBuf),
}

/// A compiled profile with its identity.
#[derive(Debug, Clone)]
pub struct Profile {
    /// Profile name from its header.
    pub name: String,
    /// Profile version from its header.
    pub version: u32,
    /// Header description.
    pub description: String,
    /// SHA-256 over the closure fingerprint and every profile file, in load order.
    pub fingerprint: String,
    pub(crate) plan: Arc<Plan>,
}

impl Profile {
    /// Load, merge, and compile `source` against `artifact`.
    ///
    /// # Errors
    /// Returns a configuration error for an unreadable, malformed, or incompatible profile.
    pub fn load(source: &ProfileSource, artifact: &Artifact) -> Result<Self, ConnectorError> {
        let mut loader = Loader::default();
        let root = match source {
            ProfileSource::Bundled(name) => {
                let path = bundled::root(name).ok_or_else(|| {
                    config_error(format!(
                        "unknown bundled FpML profile '{name}' (available: {})",
                        BUNDLED_PROFILES
                            .iter()
                            .map(|(name, _)| *name)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ))
                })?;
                Location::Bundled(path.to_owned())
            }
            ProfileSource::File(path) => Location::Disk(path.clone()),
        };
        let mut merged = Merged::default();
        loader.load(&root, "<root>".into(), 0, true, &mut merged)?;
        let model = SchemaModel::for_artifact(artifact)?;
        let plan = compile::compile(&merged, artifact, &model).map_err(config_error)?;
        let header = merged
            .header
            .as_ref()
            .ok_or_else(|| config_error("the root profile file has no [profile] header".into()))?;
        let mut digest = Sha256::new();
        digest.update(artifact.fingerprint.as_bytes());
        for (name, content) in &loader.files {
            digest.update(name.as_bytes());
            digest.update([0]);
            digest.update(content.as_bytes());
            digest.update([0]);
        }
        Ok(Self {
            name: header.name.clone(),
            version: header.version,
            description: header.description.clone(),
            fingerprint: hex(&digest.finalize()),
            plan: Arc::new(plan),
        })
    }

    /// The Arrow schema every batch from this profile has.
    #[must_use]
    pub fn schema(&self) -> arrow_schema::SchemaRef {
        Arc::clone(&self.plan.schema)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Location {
    Bundled(String),
    Disk(PathBuf),
}

#[derive(Default)]
struct Loader {
    files: Vec<(String, String)>,
    visited: BTreeSet<Location>,
}

impl Loader {
    /// `identity` names the file in the fingerprint: the bundled path, or the include text as
    /// written, so moving an operator profile directory does not change its fingerprint.
    fn load(
        &mut self,
        location: &Location,
        identity: String,
        depth: usize,
        root: bool,
        merged: &mut Merged,
    ) -> Result<(), ConnectorError> {
        if depth > MAX_INCLUDE_DEPTH {
            return Err(config_error(format!(
                "profile includes nest deeper than {MAX_INCLUDE_DEPTH}"
            )));
        }
        if !self.visited.insert(location.clone()) {
            return Ok(());
        }
        if self.files.len() >= MAX_PROFILE_FILES {
            return Err(config_error(format!(
                "a profile may load at most {MAX_PROFILE_FILES} files"
            )));
        }
        let (name, content) = read(location)?;
        let file: ProfileFile = toml::from_str(&content)
            .map_err(|error| config_error(format!("profile file '{name}': {error}")))?;
        self.files.push((identity, content));
        let located = |message: String| config_error(format!("profile file '{name}': {message}"));
        match (root, file.profile) {
            (true, Some(header)) => merged.header = Some(header),
            (true, None) => return Err(located("the root file needs a [profile] header".into())),
            (false, Some(_)) => {
                return Err(located(
                    "an included file must not have a [profile] header".into(),
                ))
            }
            (false, None) => {}
        }
        if !root && !file.columns.is_empty() {
            return Err(located("an included file must not declare columns".into()));
        }
        merged.columns.extend(file.columns);
        for (prefix, uri) in file.namespaces {
            if let Some(existing) = merged.namespaces.get(&prefix) {
                if existing != &uri {
                    return Err(located(format!(
                        "prefix '{prefix}' is bound to '{existing}' and '{uri}'"
                    )));
                }
            }
            merged.namespaces.insert(prefix, uri);
        }
        for (set, spec) in file.sets {
            if merged
                .sets
                .get(&set)
                .is_some_and(|existing| existing != &spec)
            {
                return Err(located(format!("set '{set}' is defined twice differently")));
            }
            merged.sets.insert(set, spec);
        }
        for (template, spec) in file.structs {
            if merged
                .structs
                .get(&template)
                .is_some_and(|existing| existing != &spec)
            {
                return Err(located(format!(
                    "struct template '{template}' is defined twice differently"
                )));
            }
            merged.structs.insert(template, spec);
        }
        for include in &file.include {
            let next = resolve(location, include).map_err(located)?;
            let identity = match &next {
                Location::Bundled(path) => format!("{BUNDLED_PREFIX}{path}"),
                Location::Disk(_) => include.clone(),
            };
            self.load(&next, identity, depth + 1, false, merged)?;
        }
        Ok(())
    }
}

fn resolve(from: &Location, include: &str) -> Result<Location, String> {
    if let Some(path) = include.strip_prefix(BUNDLED_PREFIX) {
        return bundled::file(path)
            .map(|_| Location::Bundled(path.to_owned()))
            .ok_or_else(|| format!("unknown bundled profile file '{path}'"));
    }
    if Path::new(include).is_absolute() || include.contains('\\') {
        return Err(format!(
            "include '{include}' must be a relative '/'-separated path"
        ));
    }
    match from {
        Location::Bundled(path) => {
            let joined = bundled::join(path, include)?;
            bundled::file(&joined)
                .map(|_| Location::Bundled(joined.clone()))
                .ok_or_else(|| format!("unknown bundled profile file '{joined}'"))
        }
        Location::Disk(path) => Ok(Location::Disk(
            path.parent()
                .unwrap_or_else(|| Path::new("."))
                .join(include),
        )),
    }
}

fn read(location: &Location) -> Result<(String, String), ConnectorError> {
    match location {
        Location::Bundled(path) => bundled::file(path)
            .map(|content| (format!("{BUNDLED_PREFIX}{path}"), content.to_owned()))
            .ok_or_else(|| config_error(format!("unknown bundled profile file '{path}'"))),
        Location::Disk(path) => {
            let size = std::fs::metadata(path)
                .map_err(|error| {
                    config_error(format!("cannot read profile '{}': {error}", path.display()))
                })?
                .len();
            if size > MAX_PROFILE_FILE_BYTES {
                return Err(config_error(format!(
                    "profile '{}' exceeds {MAX_PROFILE_FILE_BYTES} bytes",
                    path.display()
                )));
            }
            let content = std::fs::read_to_string(path).map_err(|error| {
                config_error(format!("cannot read profile '{}': {error}", path.display()))
            })?;
            Ok((path.display().to_string(), content))
        }
    }
}

fn config_error(message: String) -> ConnectorError {
    ConnectorError::ConfigurationError(message)
}
