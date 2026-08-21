//! Bounded Kafka frame I/O and the minimal v1 EndTxn wire parser.
//!
//! The parser accepts only the protocol shapes required by the ambiguity probe and fails closed
//! on unsupported versions, malformed lengths, trailing bytes, or oversized frames.

use super::*;

pub(super) fn read_frame(
    stream: &mut TcpStream,
    global_stop: &AtomicBool,
    connection_stop: &AtomicBool,
) -> ProxyResult<Option<Vec<u8>>> {
    let mut prefix = [0_u8; 4];
    if !read_exact_poll(stream, &mut prefix, global_stop, connection_stop)? {
        return Ok(None);
    }
    let signed = i32::from_be_bytes(prefix);
    let length =
        usize::try_from(signed).map_err(|_| format!("Kafka frame had negative length {signed}"))?;
    if length == 0 || length > MAX_FRAME_PAYLOAD_BYTES {
        return Err(format!(
            "Kafka frame payload length {length} is outside 1..={MAX_FRAME_PAYLOAD_BYTES}"
        ));
    }
    let mut frame = Vec::with_capacity(4 + length);
    frame.extend_from_slice(&prefix);
    frame.resize(4 + length, 0);
    if !read_exact_poll(stream, &mut frame[4..], global_stop, connection_stop)? {
        if global_stop.load(Ordering::SeqCst) || connection_stop.load(Ordering::SeqCst) {
            return Ok(None);
        }
        return Err("Kafka frame ended after its length prefix".to_owned());
    }
    Ok(Some(frame))
}

fn read_exact_poll(
    stream: &mut TcpStream,
    buffer: &mut [u8],
    global_stop: &AtomicBool,
    connection_stop: &AtomicBool,
) -> ProxyResult<bool> {
    let mut read = 0;
    while read < buffer.len() {
        if global_stop.load(Ordering::SeqCst) || connection_stop.load(Ordering::SeqCst) {
            return Ok(false);
        }
        match stream.read(&mut buffer[read..]) {
            Ok(0) if read == 0 => return Ok(false),
            Ok(0) => return Err(format!("TCP EOF after {read}/{} bytes", buffer.len())),
            Ok(count) => read += count,
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(error) => return Err(format!("TCP read failed: {error}")),
        }
    }
    Ok(true)
}

pub(super) fn parse_request_header(frame: &[u8]) -> ProxyResult<RequestHeader> {
    require_wire_length(frame)?;
    if frame.len() < 14 {
        return Err("Kafka request frame is shorter than request-header v1".to_owned());
    }
    let api_key = read_i16(frame, 4)?;
    let api_version = read_i16(frame, 6)?;
    let correlation_id = read_i32(frame, 8)?;
    let client_length = read_i16(frame, 12)?;
    let (client_id, body_offset) = match client_length {
        -1 => (None, 14),
        value if value >= 0 => {
            let length = usize::try_from(value).map_err(|_| "client ID length overflow")?;
            let end = 14_usize
                .checked_add(length)
                .ok_or_else(|| "client ID bound overflow".to_owned())?;
            let bytes = frame
                .get(14..end)
                .ok_or_else(|| "client ID exceeds Kafka request frame".to_owned())?;
            let value = std::str::from_utf8(bytes)
                .map_err(|_| "client ID was not UTF-8".to_owned())?
                .to_owned();
            (Some(value), end)
        }
        value => return Err(format!("invalid nullable client ID length {value}")),
    };
    Ok(RequestHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
        body_offset,
    })
}

pub(super) fn parse_endtxn_v1(
    connection_id: u64,
    frame: &[u8],
    header: &RequestHeader,
) -> ProxyResult<ParsedEndTxnRequest> {
    if header.api_key != END_TXN_API_KEY || header.api_version != REQUIRED_END_TXN_VERSION {
        return Err("selected request is not EndTxn v1".to_owned());
    }
    let transactional_length = read_i16(frame, header.body_offset)?;
    if transactional_length <= 0 {
        return Err(format!(
            "EndTxn transactional ID length must be positive, got {transactional_length}"
        ));
    }
    let length = usize::try_from(transactional_length)
        .map_err(|_| "transactional ID length overflow".to_owned())?;
    let start = header.body_offset + 2;
    let end = start
        .checked_add(length)
        .ok_or_else(|| "transactional ID bound overflow".to_owned())?;
    let transactional_id = std::str::from_utf8(
        frame
            .get(start..end)
            .ok_or_else(|| "transactional ID exceeds EndTxn frame".to_owned())?,
    )
    .map_err(|_| "transactional ID was not UTF-8".to_owned())?
    .to_owned();
    let producer_id = read_i64(frame, end)?;
    let producer_epoch = read_i16(frame, end + 8)?;
    let committed = match frame.get(end + 10) {
        Some(0) => false,
        Some(1) => true,
        Some(value) => {
            return Err(format!(
                "EndTxn committed byte was {value}, expected 0 or 1"
            ))
        }
        None => return Err("EndTxn committed byte is missing".to_owned()),
    };
    if end + 11 != frame.len() {
        return Err(format!(
            "EndTxn v1 frame had {} trailing bytes",
            frame.len().saturating_sub(end + 11)
        ));
    }
    Ok(ParsedEndTxnRequest {
        connection_id,
        api_version: header.api_version,
        correlation_id: header.correlation_id,
        client_id: header
            .client_id
            .clone()
            .ok_or_else(|| "target EndTxn client ID was null".to_owned())?,
        transactional_id,
        producer_id,
        producer_epoch,
        committed,
        frame: frame.to_vec(),
    })
}

pub(super) fn parse_response_correlation(frame: &[u8]) -> ProxyResult<i32> {
    require_wire_length(frame)?;
    if frame.len() < 8 {
        return Err("Kafka response frame is shorter than correlation header".to_owned());
    }
    read_i32(frame, 4)
}

pub(super) fn parse_endtxn_v1_response(
    connection_id: u64,
    frame: &[u8],
) -> ProxyResult<ParsedEndTxnResponse> {
    require_wire_length(frame)?;
    if frame.len() != 14 || read_i32(frame, 0)? != 10 {
        return Err(format!(
            "EndTxn v1 response must be exactly 14 wire bytes, got {}",
            frame.len()
        ));
    }
    Ok(ParsedEndTxnResponse {
        connection_id,
        correlation_id: read_i32(frame, 4)?,
        throttle_ms: read_i32(frame, 8)?,
        error_code: read_i16(frame, 12)?,
        frame: frame.to_vec(),
    })
}

pub(super) fn require_wire_length(frame: &[u8]) -> ProxyResult<()> {
    if frame.len() < 4 {
        return Err("Kafka frame is shorter than its length prefix".to_owned());
    }
    let signed = read_i32(frame, 0)?;
    let length =
        usize::try_from(signed).map_err(|_| format!("Kafka frame had negative length {signed}"))?;
    if length == 0 || length > MAX_FRAME_PAYLOAD_BYTES || frame.len() != length + 4 {
        return Err(format!(
            "Kafka frame length mismatch: prefix={length} wire={}",
            frame.len()
        ));
    }
    Ok(())
}

fn read_i16(bytes: &[u8], offset: usize) -> ProxyResult<i16> {
    let value = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| format!("missing i16 at offset {offset}"))?;
    Ok(i16::from_be_bytes([value[0], value[1]]))
}

fn read_i32(bytes: &[u8], offset: usize) -> ProxyResult<i32> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| format!("missing i32 at offset {offset}"))?;
    Ok(i32::from_be_bytes([value[0], value[1], value[2], value[3]]))
}

fn read_i64(bytes: &[u8], offset: usize) -> ProxyResult<i64> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| format!("missing i64 at offset {offset}"))?;
    Ok(i64::from_be_bytes([
        value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
    ]))
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    lower_hex(&Sha256::digest(bytes))
}

pub(super) fn lower_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}
