//! Logical graph identity at the existing ordered-stream boundary.

use std::io;
use std::num::NonZeroU64;

/// Exact logical topology and catalog digest, independent of assignment and recovery counters.
/// Installing this transport fence is a trusted control-path action, not an output or Release permit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShuffleTopologyFence {
    version: NonZeroU64,
    manifest_sha256: [u8; 32],
}

impl ShuffleTopologyFence {
    /// Construct a nonzero version and nonzero manifest digest.
    ///
    /// # Errors
    /// Rejects reserved zero identities.
    pub fn new(version: u64, manifest_sha256: [u8; 32]) -> io::Result<Self> {
        let version = NonZeroU64::new(version).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "zero shuffle topology version")
        })?;
        if manifest_sha256 == [0; 32] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "zero shuffle topology digest",
            ));
        }
        Ok(Self {
            version,
            manifest_sha256,
        })
    }

    /// Logical topology version, never an assignment, recovery or serialization version.
    #[must_use]
    pub const fn version(self) -> u64 {
        self.version.get()
    }

    /// Exact content identity of the installed catalog.
    #[must_use]
    pub const fn manifest_sha256(self) -> [u8; 32] {
        self.manifest_sha256
    }

    /// Derive a fence from a validated catalog reference on the control path.
    ///
    /// # Errors
    /// Rejects malformed references and reserved identities.
    #[cfg(feature = "cluster")]
    pub fn from_manifest(
        version: crate::cluster::control::TopologyVersion,
        manifest: &crate::cluster::control::CatalogManifestRef,
    ) -> io::Result<Self> {
        manifest.validate().map_err(io::Error::other)?;
        let mut digest = [0; 32];
        for (byte, encoded) in digest
            .iter_mut()
            .zip(manifest.sha256.as_bytes().as_chunks::<2>().0)
        {
            let encoded = std::str::from_utf8(encoded).map_err(io::Error::other)?;
            *byte = u8::from_str_radix(encoded, 16).map_err(io::Error::other)?;
        }
        Self::new(version.get(), digest)
    }
}
