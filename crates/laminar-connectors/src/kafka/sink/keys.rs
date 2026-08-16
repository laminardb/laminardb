//! Contiguous per-batch Kafka key storage.

/// Contiguous key buffer — stores all key bytes in a single allocation
/// with per-row `(offset, length)` pairs. Avoids N separate heap
/// allocations for N rows.
pub(super) struct KeyBuffer {
    data: Vec<u8>,
    offsets: Vec<(usize, usize)>,
}

impl KeyBuffer {
    pub(super) fn with_capacity(num_rows: usize, avg_key_len: usize) -> Self {
        Self {
            data: Vec::with_capacity(num_rows * avg_key_len),
            offsets: Vec::with_capacity(num_rows),
        }
    }

    pub(super) fn push(&mut self, key: &[u8]) {
        let start = self.data.len();
        self.data.extend_from_slice(key);
        self.offsets.push((start, key.len()));
    }

    pub(super) fn push_empty(&mut self) {
        self.offsets.push((0, 0));
    }

    pub(super) fn key(&self, i: usize) -> &[u8] {
        let (start, len) = self.offsets[i];
        &self.data[start..start + len]
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.offsets.len()
    }
}

impl std::ops::Index<usize> for KeyBuffer {
    type Output = [u8];

    fn index(&self, i: usize) -> &[u8] {
        self.key(i)
    }
}
