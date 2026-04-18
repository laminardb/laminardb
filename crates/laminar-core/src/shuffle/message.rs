//! Wire format for the shuffle: `[u32 length][u8 tag][payload]`.
//!
//! Each frame is self-contained. The receiver reads a 4-byte
//! big-endian length, a 1-byte tag, then exactly `length − 1` bytes of
//! payload. This avoids entangling the shuffle with Arrow IPC stream
//! framing: a Data message's payload is itself an Arrow IPC single-
//! batch stream (schema + batch), so a schema roll on one message
//! doesn't poison the connection.
//!
//! See `docs/plans/shuffle-protocol.md` §"Message format".

use std::io;

use arrow_array::RecordBatch;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::checkpoint::barrier::CheckpointBarrier;
use crate::serialization::{deserialize_batch_stream, serialize_batch_stream};

/// User data: an Arrow `RecordBatch` in IPC format.
pub const TAG_DATA: u8 = 0x01;
/// Checkpoint barrier flowing in-band with the data stream.
pub const TAG_BARRIER: u8 = 0x02;
/// Flow-control credit replenishment from receiver to sender.
pub const TAG_CREDIT: u8 = 0x03;
/// Connection handshake — one `Hello(node_id)` is the first frame
/// exchanged in each direction. Used by the receiver to attribute
/// incoming frames to the right peer.
pub const TAG_HELLO: u8 = 0x04;
/// Pre-routed data: sender has already classified the batch's rows
/// as all belonging to `vnode`. The receiver forwards directly to
/// that vnode's local partition without re-hashing.
pub const TAG_VNODE_DATA: u8 = 0x05;
/// Graceful-close signal with a short UTF-8 reason string.
pub const TAG_CLOSE: u8 = 0xFF;

/// Logical message carried on a shuffle connection.
#[derive(Debug, Clone, PartialEq)]
pub enum ShuffleMessage {
    /// A batch of user data.
    Data(RecordBatch),
    /// A checkpoint barrier (Chandy-Lamport).
    Barrier(CheckpointBarrier),
    /// Receiver grants `delta_bytes` of additional send credit.
    Credit(u64),
    /// Peer identifying itself during the connection handshake.
    Hello(u64),
    /// Data pre-routed to a specific vnode by the sender. Skips
    /// re-hashing on the receive side.
    VnodeData(u32, RecordBatch),
    /// Sender announcing graceful shutdown with a brief reason.
    Close(String),
}

/// Maximum payload size accepted by the codec: 64 MiB. Sized to match
/// the per-batch cap suggested in `docs/plans/shuffle-protocol.md`
/// ("Max batch size"). Receivers reject oversized frames instead of
/// allocating unbounded memory.
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// Serialize `msg` and write it as one frame on `writer`.
///
/// # Errors
/// Returns [`io::Error`] on I/O failure, Arrow IPC encoding failure
/// (wrapped as `InvalidData`), or payload size over [`MAX_PAYLOAD_BYTES`].
pub async fn write_message<W>(writer: &mut W, msg: &ShuffleMessage) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    let (tag, payload): (u8, Vec<u8>) = match msg {
        ShuffleMessage::Data(batch) => {
            let bytes = serialize_batch_stream(batch)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            (TAG_DATA, bytes)
        }
        ShuffleMessage::Barrier(b) => {
            let mut buf = Vec::with_capacity(24);
            buf.extend_from_slice(&b.checkpoint_id.to_le_bytes());
            buf.extend_from_slice(&b.epoch.to_le_bytes());
            buf.extend_from_slice(&b.flags.to_le_bytes());
            (TAG_BARRIER, buf)
        }
        ShuffleMessage::Credit(delta) => (TAG_CREDIT, delta.to_le_bytes().to_vec()),
        ShuffleMessage::Hello(node_id) => (TAG_HELLO, node_id.to_le_bytes().to_vec()),
        ShuffleMessage::VnodeData(vnode, batch) => {
            let ipc = serialize_batch_stream(batch)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            let mut buf = Vec::with_capacity(4 + ipc.len());
            buf.extend_from_slice(&vnode.to_le_bytes());
            buf.extend_from_slice(&ipc);
            (TAG_VNODE_DATA, buf)
        }
        ShuffleMessage::Close(reason) => (TAG_CLOSE, reason.as_bytes().to_vec()),
    };

    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "shuffle payload {} exceeds MAX_PAYLOAD_BYTES ({MAX_PAYLOAD_BYTES})",
                payload.len()
            ),
        ));
    }

    // Frame: [u32 total_len BE][u8 tag][payload]
    // total_len = 1 (tag) + payload.len()
    let total_len = u32::try_from(payload.len() + 1).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "frame length overflow")
    })?;
    writer.write_all(&total_len.to_be_bytes()).await?;
    writer.write_all(&[tag]).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

/// Read one frame from `reader` and decode it to a `ShuffleMessage`.
///
/// # Errors
/// Returns [`io::Error`] on truncated frames, unknown tags, or Arrow
/// IPC decoding failures. An oversized frame (over
/// [`MAX_PAYLOAD_BYTES`]) is rejected before allocation.
///
/// # Panics
/// Panics only on internal invariants (fixed-width slice conversions
/// after length checks). A correctly-framed stream cannot panic.
pub async fn read_message<R>(reader: &mut R) -> io::Result<ShuffleMessage>
where
    R: AsyncReadExt + Unpin,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let total_len = u32::from_be_bytes(len_buf) as usize;
    if total_len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "zero-length shuffle frame",
        ));
    }
    let payload_len = total_len - 1;
    if payload_len > MAX_PAYLOAD_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("shuffle frame {payload_len} exceeds MAX_PAYLOAD_BYTES"),
        ));
    }

    let mut tag_buf = [0u8; 1];
    reader.read_exact(&mut tag_buf).await?;
    let tag = tag_buf[0];

    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload).await?;

    match tag {
        TAG_DATA => {
            let batch = deserialize_batch_stream(&payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            Ok(ShuffleMessage::Data(batch))
        }
        TAG_BARRIER => {
            if payload.len() != 24 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("barrier payload {} bytes, expected 24", payload.len()),
                ));
            }
            let checkpoint_id = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let epoch = u64::from_le_bytes(payload[8..16].try_into().unwrap());
            let flags = u64::from_le_bytes(payload[16..24].try_into().unwrap());
            Ok(ShuffleMessage::Barrier(CheckpointBarrier {
                checkpoint_id,
                epoch,
                flags,
            }))
        }
        TAG_CREDIT => {
            if payload.len() != 8 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("credit payload {} bytes, expected 8", payload.len()),
                ));
            }
            let delta = u64::from_le_bytes(payload[..].try_into().unwrap());
            Ok(ShuffleMessage::Credit(delta))
        }
        TAG_HELLO => {
            if payload.len() != 8 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("hello payload {} bytes, expected 8", payload.len()),
                ));
            }
            let node_id = u64::from_le_bytes(payload[..].try_into().unwrap());
            Ok(ShuffleMessage::Hello(node_id))
        }
        TAG_VNODE_DATA => {
            if payload.len() < 4 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("vnode-data payload {} bytes, expected ≥4", payload.len()),
                ));
            }
            let vnode = u32::from_le_bytes(payload[..4].try_into().unwrap());
            let batch = deserialize_batch_stream(&payload[4..])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            Ok(ShuffleMessage::VnodeData(vnode, batch))
        }
        TAG_CLOSE => {
            let reason = String::from_utf8(payload).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("close reason: {e}"))
            })?;
            Ok(ShuffleMessage::Close(reason))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown shuffle tag byte: {other:#04x}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use tokio::io::duplex;

    use crate::checkpoint::barrier::flags;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let col = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
        RecordBatch::try_new(schema, vec![col]).unwrap()
    }

    #[tokio::test]
    async fn data_roundtrip() {
        let (mut a, mut b) = duplex(64 * 1024);
        let batch = sample_batch();
        write_message(&mut a, &ShuffleMessage::Data(batch.clone())).await.unwrap();
        let got = read_message(&mut b).await.unwrap();
        match got {
            ShuffleMessage::Data(b2) => assert_eq!(batch, b2),
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn barrier_roundtrip() {
        let (mut a, mut b) = duplex(512);
        let barrier = CheckpointBarrier {
            checkpoint_id: 17,
            epoch: 42,
            flags: flags::FULL_SNAPSHOT,
        };
        write_message(&mut a, &ShuffleMessage::Barrier(barrier)).await.unwrap();
        let got = read_message(&mut b).await.unwrap();
        assert_eq!(got, ShuffleMessage::Barrier(barrier));
    }

    #[tokio::test]
    async fn credit_roundtrip() {
        let (mut a, mut b) = duplex(64);
        write_message(&mut a, &ShuffleMessage::Credit(16 * 1024 * 1024))
            .await
            .unwrap();
        assert_eq!(
            read_message(&mut b).await.unwrap(),
            ShuffleMessage::Credit(16 * 1024 * 1024)
        );
    }

    #[tokio::test]
    async fn vnode_data_roundtrip() {
        let (mut a, mut b) = duplex(64 * 1024);
        let batch = sample_batch();
        write_message(&mut a, &ShuffleMessage::VnodeData(42, batch.clone()))
            .await
            .unwrap();
        match read_message(&mut b).await.unwrap() {
            ShuffleMessage::VnodeData(v, got) => {
                assert_eq!(v, 42);
                assert_eq!(got, batch);
            }
            other => panic!("expected VnodeData, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn hello_roundtrip() {
        let (mut a, mut b) = duplex(64);
        write_message(&mut a, &ShuffleMessage::Hello(0xDEAD_BEEF)).await.unwrap();
        assert_eq!(
            read_message(&mut b).await.unwrap(),
            ShuffleMessage::Hello(0xDEAD_BEEF)
        );
    }

    #[tokio::test]
    async fn close_roundtrip() {
        let (mut a, mut b) = duplex(128);
        write_message(&mut a, &ShuffleMessage::Close("graceful".into()))
            .await
            .unwrap();
        assert_eq!(
            read_message(&mut b).await.unwrap(),
            ShuffleMessage::Close("graceful".into())
        );
    }

    #[tokio::test]
    async fn sequenced_messages_preserve_order() {
        // Multiple messages on the same connection round-trip in FIFO
        // order. This is the property the per-key ordering guarantee
        // relies on (see design doc §3).
        let (mut a, mut b) = duplex(64 * 1024);
        let batch1 = sample_batch();
        let barrier = CheckpointBarrier::new(1, 1);
        let credit = 1024u64;

        write_message(&mut a, &ShuffleMessage::Data(batch1.clone())).await.unwrap();
        write_message(&mut a, &ShuffleMessage::Barrier(barrier)).await.unwrap();
        write_message(&mut a, &ShuffleMessage::Credit(credit)).await.unwrap();

        assert!(matches!(read_message(&mut b).await.unwrap(), ShuffleMessage::Data(_)));
        assert_eq!(
            read_message(&mut b).await.unwrap(),
            ShuffleMessage::Barrier(barrier)
        );
        assert_eq!(
            read_message(&mut b).await.unwrap(),
            ShuffleMessage::Credit(credit)
        );
    }

    #[tokio::test]
    async fn unknown_tag_returns_error() {
        // Write a hand-rolled frame with a bogus tag.
        let (mut a, mut b) = duplex(64);
        a.write_all(&[0u8, 0, 0, 1]).await.unwrap(); // total_len = 1
        a.write_all(&[0x7Fu8]).await.unwrap();       // unknown tag
        drop(a);

        let err = read_message(&mut b).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn oversize_frame_rejected() {
        let (mut a, mut b) = duplex(64);
        let bogus_len = u32::try_from(MAX_PAYLOAD_BYTES)
            .expect("MAX_PAYLOAD_BYTES fits in u32")
            .saturating_add(2);
        a.write_all(&bogus_len.to_be_bytes()).await.unwrap();
        drop(a);
        let err = read_message(&mut b).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
