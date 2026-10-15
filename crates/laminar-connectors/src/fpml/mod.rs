//! FpML document decoding: secure parsing, pinned XML Schema validation, reference integrity,
//! and configured typed projection into Arrow.

mod artifacts;
mod document;
mod error;
mod limits;
mod xsd;

pub use artifacts::{Artifact, BUNDLED_CONFIRMATION};
pub use error::{Rejection, Stage};
pub use limits::Limits;
