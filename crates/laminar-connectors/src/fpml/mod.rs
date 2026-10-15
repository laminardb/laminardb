//! FpML document decoding: secure parsing, pinned XML Schema validation, reference integrity,
//! and configured typed projection into Arrow.
//!
//! One complete document per transport record (file or message). Each document passes, in
//! order: a streaming pre-scan (UTF-8 only, no DTDs or processing instructions, structural
//! limits), libxml2 parsing and XML Schema validation against a digest-pinned closure,
//! document-local reference checks, and projection through a compiled profile. Any failure
//! rejects the whole document before a row is returned.

// FpML, XML Schema, and libxml2 names are vocabulary in prose, not code identifiers.
#![allow(clippy::doc_markdown)]

mod artifacts;
mod decoder;
mod document;
mod error;
#[cfg(test)]
mod golden;
mod limits;
mod metrics;
mod prescan;
mod profile;
mod projection;
mod references;
mod schema_model;
#[cfg(test)]
mod tests;
mod xsd;

pub use artifacts::{Artifact, BUNDLED_CONFIRMATION};
pub use decoder::{FpmlConfig, FpmlDecoder, DEFAULT_PROFILE};
pub use error::{Rejection, Stage};
pub use limits::Limits;
pub use profile::{Profile, ProfileSource, BUNDLED_PROFILES};
pub use references::ReferencePolicy;
