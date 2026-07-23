use std::io::{ErrorKind, Read};

use sha2::{Digest as _, Sha256};

use crate::CheckErrors;

const POPULATION_DOMAIN: &[u8] = b"LDB-SBQ-OBSERVATION-POPULATION-V1\0";

pub(crate) struct ExactReader<R> {
    inner: R,
    declared_bytes: u64,
    consumed_bytes: u64,
    label: &'static str,
}

impl<R: Read> ExactReader<R> {
    pub(crate) fn new(
        inner: R,
        declared_bytes: u64,
        maximum_bytes: u64,
        label: &'static str,
    ) -> Result<Self, CheckErrors> {
        if declared_bytes > maximum_bytes {
            return Err(CheckErrors::one(format!(
                "{label} artifact is {declared_bytes} bytes; maximum is {maximum_bytes}"
            )));
        }
        Ok(Self {
            inner,
            declared_bytes,
            consumed_bytes: 0,
            label,
        })
    }

    pub(crate) fn read_vec(&mut self, length: usize) -> Result<Vec<u8>, CheckErrors> {
        let mut bytes = vec![0_u8; length];
        self.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    pub(crate) fn read_array<const N: usize>(&mut self) -> Result<[u8; N], CheckErrors> {
        let mut bytes = [0_u8; N];
        self.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    pub(crate) fn read_into(&mut self, bytes: &mut [u8]) -> Result<(), CheckErrors> {
        self.read_exact(bytes)
    }

    pub(crate) fn require_total_length(&self, expected: u64) -> Result<(), CheckErrors> {
        if self.declared_bytes != expected {
            return Err(CheckErrors::one(format!(
                "{} stream is {} bytes; expected exactly {expected}",
                self.label, self.declared_bytes
            )));
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<(), CheckErrors> {
        if self.consumed_bytes != self.declared_bytes {
            return Err(CheckErrors::one(format!(
                "{} parser consumed {} of {} declared bytes",
                self.label, self.consumed_bytes, self.declared_bytes
            )));
        }
        let mut trailing = [0_u8; 1];
        match self.inner.read(&mut trailing) {
            Ok(0) => Ok(()),
            Ok(_) => Err(CheckErrors::one(format!(
                "{} contains bytes beyond its declared length",
                self.label
            ))),
            Err(error) => Err(CheckErrors::one(format!(
                "read {} trailing sentinel: {error}",
                self.label
            ))),
        }
    }

    fn read_exact(&mut self, bytes: &mut [u8]) -> Result<(), CheckErrors> {
        let requested = u64::try_from(bytes.len()).map_err(|_| {
            CheckErrors::one(format!("{} read length does not fit u64", self.label))
        })?;
        let next = self
            .consumed_bytes
            .checked_add(requested)
            .ok_or_else(|| CheckErrors::one(format!("{} read offset overflow", self.label)))?;
        if next > self.declared_bytes {
            return Err(CheckErrors::one(format!(
                "{} parser would read beyond {} declared bytes",
                self.label, self.declared_bytes
            )));
        }
        self.inner.read_exact(bytes).map_err(|error| {
            if error.kind() == ErrorKind::UnexpectedEof {
                CheckErrors::one(format!("{} stream is truncated", self.label))
            } else {
                CheckErrors::one(format!("read {}: {error}", self.label))
            }
        })?;
        self.consumed_bytes = next;
        Ok(())
    }
}

pub(crate) struct PopulationHasher(Sha256);

impl PopulationHasher {
    pub(crate) fn new(record_count: u64) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(POPULATION_DOMAIN);
        hasher.update(record_count.to_be_bytes());
        Self(hasher)
    }

    pub(crate) fn update(&mut self, tag: u8, index: u64, begin_ns: u64, end_ns: u64) {
        self.0.update([tag]);
        self.0.update(index.to_be_bytes());
        self.0.update(begin_ns.to_be_bytes());
        self.0.update(end_ns.to_be_bytes());
    }

    pub(crate) fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}

pub(crate) struct HashingReader<R> {
    inner: R,
    hasher: Sha256,
    bytes_read: u64,
}

impl<R> HashingReader<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            bytes_read: 0,
        }
    }

    pub(crate) fn finish(self) -> ([u8; 32], u64) {
        (self.hasher.finalize().into(), self.bytes_read)
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let count = self.inner.read(buffer)?;
        self.hasher.update(&buffer[..count]);
        let count_u64 = u64::try_from(count).map_err(|_| {
            std::io::Error::new(ErrorKind::InvalidData, "reader byte count does not fit u64")
        })?;
        self.bytes_read = self.bytes_read.checked_add(count_u64).ok_or_else(|| {
            std::io::Error::new(ErrorKind::InvalidData, "reader byte count overflow")
        })?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn exact_reader_cap_is_inclusive_and_plus_one_fails_before_reading() {
        assert!(ExactReader::new(Cursor::new([]), 256, 256, "fixture").is_ok());
        assert!(ExactReader::new(Cursor::new([]), 257, 256, "fixture")
            .err()
            .unwrap()
            .to_string()
            .contains("maximum is 256"));
    }
}
