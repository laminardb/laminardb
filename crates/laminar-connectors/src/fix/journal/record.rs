//! Versioned binary encoding of journal records.
//!
//! All integers are little-endian. A record never contains logon credentials: application
//! records hold inbound application messages only, and session records hold sequence numbers.

use crate::error::ConnectorError;

const KIND_SESSION_SNAPSHOT: u8 = 1;
const KIND_SERIES_STARTED: u8 = 2;
const KIND_APPLICATION: u8 = 3;
const KIND_GAP_FILL: u8 = 4;
const KIND_SEQUENCE_RESET: u8 = 5;
const KIND_REJECTED: u8 = 6;

const FLAG_POSS_DUP: u8 = 1;
const FLAG_POSS_RESEND: u8 = 2;

/// Inbound session progress at the start of a segment, so retention can drop older segments
/// without losing the current sequence series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct SessionSnapshot {
    /// Current sequence series ordinal; 0 before the first logon.
    pub(crate) series: u32,
    /// Engine store creation time (whole seconds) that identifies `series`.
    pub(crate) store_created_unix_seconds: i64,
    /// Highest inbound sequence number journaled in `series`; 0 when none.
    pub(crate) high_water: u64,
}

/// One accepted inbound application message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ApplicationRecord {
    pub(crate) series: u32,
    pub(crate) seq: u64,
    /// Wall-clock receive time in nanoseconds since the Unix epoch.
    pub(crate) received_at_nanos: i64,
    pub(crate) poss_dup: bool,
    pub(crate) poss_resend: bool,
    pub(crate) msg_type: String,
    /// Digest of the message without retransmission-variable header fields, used to detect a
    /// retransmission whose content conflicts with the journaled original.
    pub(crate) content_digest: u64,
    /// Engine serialization of the accepted message (`Message::toString`).
    pub(crate) message: Vec<u8>,
}

/// A journal record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Record {
    /// First record of every segment.
    SessionSnapshot(SessionSnapshot),
    /// The engine store was reset; inbound numbering restarts in a new series.
    SeriesStarted {
        series: u32,
        store_created_unix_seconds: i64,
    },
    Application(ApplicationRecord),
    /// SequenceReset-GapFill accepted: `from_seq..new_seq` carry no resent application data.
    GapFill {
        series: u32,
        from_seq: u64,
        new_seq: u64,
    },
    /// SequenceReset-Reset moved the inbound sequence forward: `from_seq..new_seq` were never
    /// delivered and cannot be recovered through the session.
    SequenceReset {
        series: u32,
        from_seq: u64,
        new_seq: u64,
    },
    /// The engine rejected inbound `ref_seq` at the session level.
    Rejected {
        series: u32,
        ref_seq: u64,
        reason: i32,
    },
}

impl Record {
    pub(crate) fn encode_into(&self, output: &mut Vec<u8>) -> Result<(), ConnectorError> {
        output.clear();
        match self {
            Self::SessionSnapshot(snapshot) => {
                output.push(KIND_SESSION_SNAPSHOT);
                output.extend_from_slice(&snapshot.series.to_le_bytes());
                output.extend_from_slice(&snapshot.store_created_unix_seconds.to_le_bytes());
                output.extend_from_slice(&snapshot.high_water.to_le_bytes());
            }
            Self::SeriesStarted {
                series,
                store_created_unix_seconds,
            } => {
                output.push(KIND_SERIES_STARTED);
                output.extend_from_slice(&series.to_le_bytes());
                output.extend_from_slice(&store_created_unix_seconds.to_le_bytes());
            }
            Self::Application(record) => {
                output.push(KIND_APPLICATION);
                output.extend_from_slice(&record.series.to_le_bytes());
                output.extend_from_slice(&record.seq.to_le_bytes());
                output.extend_from_slice(&record.received_at_nanos.to_le_bytes());
                let flags = if record.poss_dup { FLAG_POSS_DUP } else { 0 }
                    | if record.poss_resend {
                        FLAG_POSS_RESEND
                    } else {
                        0
                    };
                output.push(flags);
                output.extend_from_slice(&record.content_digest.to_le_bytes());
                put_bytes(output, record.msg_type.as_bytes())?;
                put_bytes(output, &record.message)?;
            }
            Self::GapFill {
                series,
                from_seq,
                new_seq,
            } => put_range(output, KIND_GAP_FILL, *series, *from_seq, *new_seq),
            Self::SequenceReset {
                series,
                from_seq,
                new_seq,
            } => put_range(output, KIND_SEQUENCE_RESET, *series, *from_seq, *new_seq),
            Self::Rejected {
                series,
                ref_seq,
                reason,
            } => {
                output.push(KIND_REJECTED);
                output.extend_from_slice(&series.to_le_bytes());
                output.extend_from_slice(&ref_seq.to_le_bytes());
                output.extend_from_slice(&reason.to_le_bytes());
            }
        }
        Ok(())
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, ConnectorError> {
        let mut reader = Reader { bytes, offset: 0 };
        let record = match reader.u8()? {
            KIND_SESSION_SNAPSHOT => Self::SessionSnapshot(SessionSnapshot {
                series: reader.u32()?,
                store_created_unix_seconds: reader.i64()?,
                high_water: reader.u64()?,
            }),
            KIND_SERIES_STARTED => Self::SeriesStarted {
                series: reader.u32()?,
                store_created_unix_seconds: reader.i64()?,
            },
            KIND_APPLICATION => {
                let series = reader.u32()?;
                let seq = reader.u64()?;
                let received_at_nanos = reader.i64()?;
                let flags = reader.u8()?;
                let content_digest = reader.u64()?;
                let msg_type = String::from_utf8(reader.bytes()?.to_vec())
                    .map_err(|_| corrupt("application record MsgType is not UTF-8"))?;
                let message = reader.bytes()?.to_vec();
                Self::Application(ApplicationRecord {
                    series,
                    seq,
                    received_at_nanos,
                    poss_dup: flags & FLAG_POSS_DUP != 0,
                    poss_resend: flags & FLAG_POSS_RESEND != 0,
                    msg_type,
                    content_digest,
                    message,
                })
            }
            KIND_GAP_FILL => Self::GapFill {
                series: reader.u32()?,
                from_seq: reader.u64()?,
                new_seq: reader.u64()?,
            },
            KIND_SEQUENCE_RESET => Self::SequenceReset {
                series: reader.u32()?,
                from_seq: reader.u64()?,
                new_seq: reader.u64()?,
            },
            KIND_REJECTED => Self::Rejected {
                series: reader.u32()?,
                ref_seq: reader.u64()?,
                reason: reader.i32()?,
            },
            other => return Err(corrupt(&format!("unknown journal record kind {other}"))),
        };
        if reader.offset != bytes.len() {
            return Err(corrupt("journal record has trailing bytes"));
        }
        Ok(record)
    }
}

fn put_range(output: &mut Vec<u8>, kind: u8, series: u32, from_seq: u64, new_seq: u64) {
    output.push(kind);
    output.extend_from_slice(&series.to_le_bytes());
    output.extend_from_slice(&from_seq.to_le_bytes());
    output.extend_from_slice(&new_seq.to_le_bytes());
}

fn put_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ConnectorError> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        ConnectorError::Internal("journal record field exceeds u32::MAX bytes".into())
    })?;
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

pub(crate) fn corrupt(detail: &str) -> ConnectorError {
    ConnectorError::ConfigurationError(format!("FIX journal is corrupt: {detail}"))
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], ConnectorError> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| corrupt("journal record is truncated"))?;
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ConnectorError> {
        let mut out = [0_u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, ConnectorError> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, ConnectorError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn i32(&mut self) -> Result<i32, ConnectorError> {
        Ok(i32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, ConnectorError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64, ConnectorError> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    fn bytes(&mut self) -> Result<&'a [u8], ConnectorError> {
        let len = self.u32()? as usize;
        self.take(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_record_kind_round_trips() {
        let records = [
            Record::SessionSnapshot(SessionSnapshot {
                series: 3,
                store_created_unix_seconds: 1_760_000_000,
                high_water: 42,
            }),
            Record::SeriesStarted {
                series: 4,
                store_created_unix_seconds: -1,
            },
            Record::Application(ApplicationRecord {
                series: 4,
                seq: 7,
                received_at_nanos: 1_760_000_000_123_456_789,
                poss_dup: true,
                poss_resend: false,
                msg_type: "U1".into(),
                content_digest: 0xfeed,
                message: b"8=FIX.4.4\x019=5\x0135=U1\x01\x00binary\x01".to_vec(),
            }),
            Record::GapFill {
                series: 4,
                from_seq: 8,
                new_seq: 12,
            },
            Record::SequenceReset {
                series: 4,
                from_seq: 12,
                new_seq: 20,
            },
            Record::Rejected {
                series: 4,
                ref_seq: 20,
                reason: 5,
            },
        ];
        let mut buffer = Vec::new();
        for record in records {
            record.encode_into(&mut buffer).expect("encode");
            assert_eq!(Record::decode(&buffer).expect("decode"), record);
        }
    }

    #[test]
    fn truncated_unknown_and_trailing_bytes_are_corrupt() {
        let mut buffer = Vec::new();
        Record::GapFill {
            series: 1,
            from_seq: 2,
            new_seq: 3,
        }
        .encode_into(&mut buffer)
        .expect("encode");
        assert!(Record::decode(&buffer[..buffer.len() - 1]).is_err());
        let mut trailing = buffer.clone();
        trailing.push(0);
        assert!(Record::decode(&trailing).is_err());
        assert!(Record::decode(&[99]).is_err());
        assert!(Record::decode(&[]).is_err());
    }
}

#[cfg(test)]
mod properties {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn application_records_round_trip(
            series in any::<u32>(),
            seq in any::<u64>(),
            received in any::<i64>(),
            poss_dup in any::<bool>(),
            poss_resend in any::<bool>(),
            msg_type in "[0-9A-Za-z]{1,3}",
            digest in any::<u64>(),
            message in proptest::collection::vec(any::<u8>(), 0..512),
        ) {
            let record = Record::Application(ApplicationRecord {
                series, seq, received_at_nanos: received, poss_dup, poss_resend,
                msg_type, content_digest: digest, message,
            });
            let mut buffer = Vec::new();
            record.encode_into(&mut buffer).expect("encode");
            prop_assert_eq!(Record::decode(&buffer).expect("decode"), record);
        }

        #[test]
        fn arbitrary_payloads_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _ = Record::decode(&bytes);
        }
    }
}
