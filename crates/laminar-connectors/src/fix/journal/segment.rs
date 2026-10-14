//! Segment files and record framing.
//!
//! A segment is `{first_index:020}.seg`: a 32-byte header (magic, first record index, journal
//! id) followed by frames `[len: u32][xxh3_64(payload): u64][payload]`. Segments are created as
//! a fully synced temporary file and published with a durable no-replace rename, so a published
//! segment always has a complete header. Only the newest segment can have a torn tail.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use laminar_core::durable_fs::{durable_rename, DurableRenameMode};
use xxhash_rust::xxh3::xxh3_64;

use super::record::corrupt;
use crate::error::ConnectorError;

const MAGIC: &[u8; 8] = b"LDBFIXJ1";
const HEADER_SIZE: usize = 32;
const FRAME_HEADER_SIZE: usize = 12;
pub(super) const HEADER_LEN: u64 = HEADER_SIZE as u64;
pub(super) const FRAME_OVERHEAD: u64 = FRAME_HEADER_SIZE as u64;
const SUFFIX: &str = ".seg";
const TEMP_SUFFIX: &str = ".seg.tmp";

pub(super) fn segment_path(dir: &Path, first_index: u64) -> PathBuf {
    dir.join(format!("{first_index:020}{SUFFIX}"))
}

/// Published segments in index order. Unpublished temporary segments are removed: they were
/// never renamed into place, so no record in them was ever acknowledged.
pub(super) fn list_segments(dir: &Path) -> Result<Vec<u64>, ConnectorError> {
    let mut segments = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.ends_with(TEMP_SUFFIX) {
            std::fs::remove_file(entry.path())?;
            continue;
        }
        let Some(stem) = name.strip_suffix(SUFFIX) else {
            continue;
        };
        let first_index = stem
            .parse::<u64>()
            .map_err(|_| corrupt(&format!("unexpected segment file name '{name}'")))?;
        segments.push(first_index);
    }
    segments.sort_unstable();
    Ok(segments)
}

fn header_bytes(first_index: u64, journal_id: &[u8; 16]) -> [u8; HEADER_SIZE] {
    let mut header = [0_u8; HEADER_SIZE];
    header[..8].copy_from_slice(MAGIC);
    header[8..16].copy_from_slice(&first_index.to_le_bytes());
    header[16..].copy_from_slice(journal_id);
    header
}

/// Durably publish a new segment whose first frame is `first_frame`, and open it for appends.
pub(super) fn create_segment(
    dir: &Path,
    first_index: u64,
    journal_id: &[u8; 16],
    first_frame: &[u8],
) -> Result<File, ConnectorError> {
    let path = segment_path(dir, first_index);
    let temp = dir.join(format!("{first_index:020}{TEMP_SUFFIX}"));
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(&header_bytes(first_index, journal_id))?;
        file.write_all(first_frame)?;
        file.sync_all()?;
    }
    durable_rename(&temp, &path, DurableRenameMode::NoReplace)?;
    let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
    file.seek(SeekFrom::End(0))?;
    Ok(file)
}

/// Validate a segment header against its file name and journal.
pub(super) fn check_header(
    file: &mut impl Read,
    first_index: u64,
    journal_id: &[u8; 16],
) -> Result<(), ConnectorError> {
    let mut header = [0_u8; HEADER_SIZE];
    file.read_exact(&mut header)
        .map_err(|_| corrupt(&format!("segment {first_index} has a truncated header")))?;
    if header != header_bytes(first_index, journal_id) {
        return Err(corrupt(&format!(
            "segment {first_index} header does not match this journal"
        )));
    }
    Ok(())
}

pub(super) fn encode_frame(payload: &[u8], output: &mut Vec<u8>) -> Result<(), ConnectorError> {
    let len = u32::try_from(payload.len())
        .map_err(|_| ConnectorError::Internal("journal frame exceeds u32::MAX bytes".into()))?;
    output.clear();
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(&xxh3_64(payload).to_le_bytes());
    output.extend_from_slice(payload);
    Ok(())
}

/// Outcome of reading the next frame.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum FrameRead {
    /// A complete, checksum-valid frame was read into the buffer.
    Frame,
    /// Clean end: no bytes remain before `end`.
    End,
    /// The bytes from the current offset to `end` do not form a complete valid frame and
    /// nothing follows it: a torn append.
    Torn,
    /// An invalid frame is followed by more bytes: not explainable by a torn append.
    Corrupt,
}

/// Sequential frame reader bounded by a byte offset.
pub(super) struct FrameReader {
    file: BufReader<File>,
    offset: u64,
    max_frame: u64,
}

impl FrameReader {
    pub(super) fn new(file: File, offset: u64, max_frame: u64) -> Result<Self, ConnectorError> {
        let mut file = BufReader::with_capacity(64 * 1024, file);
        file.seek(SeekFrom::Start(offset))?;
        Ok(Self {
            file,
            offset,
            max_frame,
        })
    }

    pub(super) fn offset(&self) -> u64 {
        self.offset
    }

    /// Current file length; final once the segment is sealed.
    pub(super) fn file_len(&self) -> Result<u64, ConnectorError> {
        Ok(self.file.get_ref().metadata()?.len())
    }

    /// Read the next frame wholly before `end` into `payload`.
    pub(super) fn next(
        &mut self,
        end: u64,
        payload: &mut Vec<u8>,
    ) -> Result<FrameRead, ConnectorError> {
        let remaining = end.saturating_sub(self.offset);
        if remaining == 0 {
            return Ok(FrameRead::End);
        }
        if remaining < FRAME_OVERHEAD {
            return Ok(FrameRead::Torn);
        }
        let mut header = [0_u8; FRAME_HEADER_SIZE];
        self.file.read_exact(&mut header)?;
        let len = u64::from(u32::from_le_bytes([
            header[0], header[1], header[2], header[3],
        ]));
        let mut digest = [0_u8; 8];
        digest.copy_from_slice(&header[4..]);
        let frame_end = self.offset + FRAME_OVERHEAD + len;
        if len > self.max_frame || frame_end > end {
            self.reset_to(self.offset)?;
            return Ok(FrameRead::Torn);
        }
        payload.clear();
        // Bounded by `max_frame`, which configuration limits.
        payload.resize(
            usize::try_from(len).map_err(|_| corrupt("frame length overflow"))?,
            0,
        );
        self.file.read_exact(payload)?;
        if xxh3_64(payload) != u64::from_le_bytes(digest) {
            self.reset_to(self.offset)?;
            return Ok(if frame_end == end {
                FrameRead::Torn
            } else {
                FrameRead::Corrupt
            });
        }
        self.offset = frame_end;
        Ok(FrameRead::Frame)
    }

    fn reset_to(&mut self, offset: u64) -> Result<(), ConnectorError> {
        self.file.seek(SeekFrom::Start(offset))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_frames(dir: &Path, payloads: &[&[u8]]) -> (PathBuf, Vec<u64>) {
        let id = [7_u8; 16];
        let mut frame = Vec::new();
        encode_frame(payloads[0], &mut frame).expect("frame");
        let mut file = create_segment(dir, 0, &id, &frame).expect("segment");
        let mut ends = vec![HEADER_LEN + frame.len() as u64];
        for payload in &payloads[1..] {
            encode_frame(payload, &mut frame).expect("frame");
            file.write_all(&frame).expect("append");
            ends.push(ends.last().copied().unwrap_or(0) + frame.len() as u64);
        }
        (segment_path(dir, 0), ends)
    }

    fn reader(path: &Path) -> FrameReader {
        let mut file = File::open(path).expect("open");
        check_header(&mut file, 0, &[7_u8; 16]).expect("header");
        FrameReader::new(file, HEADER_LEN, 1024).expect("reader")
    }

    #[test]
    fn frames_read_back_in_order_and_stop_at_the_published_end() {
        let dir = tempfile::tempdir().expect("dir");
        let (path, ends) = write_frames(dir.path(), &[b"one", b"two\x00\x01", b"three"]);
        let mut frames = reader(&path);
        let mut payload = Vec::new();
        assert_eq!(
            frames.next(ends[1], &mut payload).expect("read"),
            FrameRead::Frame
        );
        assert_eq!(payload, b"one");
        assert_eq!(
            frames.next(ends[1], &mut payload).expect("read"),
            FrameRead::Frame
        );
        assert_eq!(payload, b"two\x00\x01");
        assert_eq!(
            frames.next(ends[1], &mut payload).expect("read"),
            FrameRead::End
        );
        assert_eq!(
            frames.next(ends[2], &mut payload).expect("read"),
            FrameRead::Frame
        );
        assert_eq!(payload, b"three");
    }

    #[test]
    fn torn_tail_is_distinguished_from_mid_segment_corruption() {
        let dir = tempfile::tempdir().expect("dir");
        let (path, ends) = write_frames(dir.path(), &[b"one", b"two", b"three"]);
        let full = ends[2];
        let mut payload = Vec::new();
        for cut in [ends[1] + 1, ends[1] + FRAME_OVERHEAD, full - 1] {
            let mut frames = reader(&path);
            assert_eq!(
                frames.next(cut, &mut payload).expect("read"),
                FrameRead::Frame
            );
            assert_eq!(
                frames.next(cut, &mut payload).expect("read"),
                FrameRead::Frame
            );
            assert_eq!(
                frames.next(cut, &mut payload).expect("read"),
                FrameRead::Torn
            );
            assert_eq!(frames.offset(), ends[1]);
        }

        let mut bytes = std::fs::read(&path).expect("read");
        let last = usize::try_from(full - 1).expect("offset");
        bytes[last] ^= 0xff;
        std::fs::write(&path, &bytes).expect("write");
        let mut frames = reader(&path);
        frames.next(full, &mut payload).expect("one");
        frames.next(full, &mut payload).expect("two");
        assert_eq!(
            frames.next(full, &mut payload).expect("read"),
            FrameRead::Torn
        );

        let middle = usize::try_from(ends[0] + FRAME_OVERHEAD).expect("offset");
        bytes[last] ^= 0xff;
        bytes[middle] ^= 0xff;
        std::fs::write(&path, &bytes).expect("write");
        let mut frames = reader(&path);
        frames.next(full, &mut payload).expect("one");
        assert_eq!(
            frames.next(full, &mut payload).expect("read"),
            FrameRead::Corrupt
        );
    }

    #[test]
    fn foreign_headers_and_temporary_segments_are_rejected_or_removed() {
        let dir = tempfile::tempdir().expect("dir");
        let (path, _) = write_frames(dir.path(), &[b"one"]);
        let mut file = File::open(&path).expect("open");
        assert!(check_header(&mut file, 0, &[8_u8; 16]).is_err());
        std::fs::write(dir.path().join(format!("{:020}{TEMP_SUFFIX}", 9)), b"x").expect("temp");
        assert_eq!(list_segments(dir.path()).expect("list"), vec![0]);
        assert!(!dir.path().join(format!("{:020}{TEMP_SUFFIX}", 9)).exists());
    }
}

#[cfg(test)]
mod properties {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Arbitrary bytes after a valid header never panic the reader, never yield a frame past
        /// the bound, and always stop.
        #[test]
        fn arbitrary_segment_tails_are_classified(tail in proptest::collection::vec(any::<u8>(), 0..2048)) {
            let dir = tempfile::tempdir().expect("dir");
            let mut frame = Vec::new();
            encode_frame(b"first", &mut frame).expect("frame");
            let mut file = create_segment(dir.path(), 0, &[1; 16], &frame).expect("segment");
            file.write_all(&tail).expect("tail");
            drop(file);
            let path = segment_path(dir.path(), 0);
            let length = std::fs::metadata(&path).expect("metadata").len();
            let mut reader = File::open(&path).expect("open");
            check_header(&mut reader, 0, &[1; 16]).expect("header");
            let mut frames = FrameReader::new(reader, HEADER_LEN, 1024).expect("reader");
            let mut payload = Vec::new();
            for _ in 0..4096 {
                match frames.next(length, &mut payload).expect("read") {
                    FrameRead::Frame => prop_assert!(frames.offset() <= length),
                    FrameRead::End | FrameRead::Torn | FrameRead::Corrupt => break,
                }
            }
        }
    }
}
