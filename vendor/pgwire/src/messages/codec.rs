use std::str;

use bytes::{Buf, BufMut, BytesMut};

use crate::error::{PgWireError, PgWireResult};

/// Get null-terminated string, returns None when empty or
/// non null-terminated cstring read.
///
/// Note that this implementation will also advance cursor by 1 after reading
/// empty cstring. This behaviour works for how postgres wire protocol handling
/// key-value pairs, which is ended by a single `\0`
pub(crate) fn get_cstring(buf: &mut BytesMut) -> PgWireResult<Option<String>> {
    let mut i = 0;

    // with bound check to prevent invalid format
    while i < buf.remaining() && buf[i] != b'\0' {
        i += 1;
    }

    if i == buf.remaining() {
        return Err(PgWireError::MalformedMessage(
            "cstring is not null-terminated",
        ));
    }

    // i+1: include the '\0'
    // move cursor to the end of cstring
    let string_buf = buf.split_to(i + 1);

    if i == 0 {
        Ok(None)
    } else {
        Ok(Some(String::from_utf8_lossy(&string_buf[..i]).into_owned()))
    }
}

pub(crate) fn get_u8(buf: &mut BytesMut) -> PgWireResult<u8> {
    buf.try_get_u8()
        .map_err(|_| PgWireError::MalformedMessage("missing u8 field"))
}

pub(crate) fn get_i16(buf: &mut BytesMut) -> PgWireResult<i16> {
    buf.try_get_i16()
        .map_err(|_| PgWireError::MalformedMessage("missing i16 field"))
}

pub(crate) fn get_u16(buf: &mut BytesMut) -> PgWireResult<u16> {
    buf.try_get_u16()
        .map_err(|_| PgWireError::MalformedMessage("missing u16 field"))
}

pub(crate) fn get_i32(buf: &mut BytesMut) -> PgWireResult<i32> {
    buf.try_get_i32()
        .map_err(|_| PgWireError::MalformedMessage("missing i32 field"))
}

pub(crate) fn get_u32(buf: &mut BytesMut) -> PgWireResult<u32> {
    buf.try_get_u32()
        .map_err(|_| PgWireError::MalformedMessage("missing u32 field"))
}

pub(crate) fn take(buf: &mut BytesMut, len: usize) -> PgWireResult<BytesMut> {
    if buf.remaining() < len {
        return Err(PgWireError::MalformedMessage(
            "field length exceeds message body",
        ));
    }
    Ok(buf.split_to(len))
}

/// Put null-termianted string
///
/// You can put empty string by giving `""` as input.
pub(crate) fn put_cstring(buf: &mut BytesMut, input: &str) {
    buf.put_slice(input.as_bytes());
    buf.put_u8(b'\0');
}

pub(crate) fn put_option_cstring(buf: &mut BytesMut, input: &Option<String>) {
    if let Some(input) = input {
        put_cstring(buf, input);
    } else {
        buf.put_u8(b'\0');
    }
}

/// Try to read message length from buf, without actually move the cursor
pub(crate) fn get_length(buf: &BytesMut, offset: usize) -> PgWireResult<Option<usize>> {
    if buf.remaining() >= 4 + offset {
        let raw = (&buf[offset..4 + offset]).get_i32();
        if raw < 4 {
            return Err(PgWireError::InvalidMessageLength(raw));
        }
        Ok(Some(raw as usize))
    } else {
        Ok(None)
    }
}

/// Check if message_length matches and move the cursor to right position then
/// call the `decode_fn` for the body
pub(crate) fn decode_packet<T, F>(
    buf: &mut BytesMut,
    offset: usize,
    max_size: usize,
    decode_fn: F,
) -> PgWireResult<Option<T>>
where
    F: Fn(&mut BytesMut, usize) -> PgWireResult<T>,
{
    if let Some(msg_len) = get_length(buf, offset)? {
        if msg_len > max_size {
            return Err(PgWireError::MessageTooLarge(max_size, msg_len));
        }

        let packet_len = msg_len
            .checked_add(offset)
            .ok_or(PgWireError::InvalidMessageLength(i32::MAX))?;
        if buf.remaining() >= packet_len {
            buf.advance(offset + 4);
            let mut body = buf.split_to(msg_len - 4);
            let decoded = decode_fn(&mut body, msg_len)?;
            if body.has_remaining() {
                return Err(PgWireError::MalformedMessage(
                    "message decoder left trailing bytes",
                ));
            }
            return Ok(Some(decoded));
        }
    }

    Ok(None)
}

// pub(crate) fn get_and_ensure_message_type(buf: &mut BytesMut, t: u8) -> PgWireResult<()> {
//     let msg_type = buf[0];
//     // ensure the type is corrent
//     if msg_type != t {
//         return Err(PgWireError::InvalidMessageType(t, msg_type));
//     }

//     Ok(())
// }

pub(crate) fn option_string_len(s: &Option<String>) -> usize {
    1 + s.as_ref().map(|s| s.len()).unwrap_or(0)
}

#[cfg(test)]
mod test {
    use super::{decode_packet, get_cstring};
    use crate::error::PgWireError;
    use bytes::{BufMut, BytesMut};

    #[test]
    fn get_cstring_valid() {
        let mut buf = BytesMut::new();
        buf.put(&b"a cstring\0"[..]);
        buf.put(&b"\0"[..]);

        assert_eq!(Some("a cstring".into()), get_cstring(&mut buf).unwrap());
        assert_eq!(None, get_cstring(&mut buf).unwrap());
    }

    #[test]
    fn get_cstring_empty() {
        let mut buf = BytesMut::new();

        assert!(get_cstring(&mut buf).is_err());
    }

    #[test]
    fn get_cstring_without_null() {
        let mut buf = BytesMut::new();
        buf.put(&b"a cstring"[..]);
        assert!(get_cstring(&mut buf).is_err());
    }

    #[test]
    fn rejects_lengths_smaller_than_the_length_field() {
        let mut buf = BytesMut::new();
        buf.put_i32(3);
        let error = decode_packet(&mut buf, 0, 1024, |_, _| Ok(())).unwrap_err();
        assert!(matches!(error, PgWireError::InvalidMessageLength(3)));
        assert_eq!(buf.len(), 4, "invalid header must not advance the buffer");
    }

    #[test]
    fn isolates_one_frame_and_rejects_trailing_body_bytes() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        buf.put_i32(5);
        buf.put_u8(7);
        buf.put_u8(b'S');
        buf.put_i32(4);

        let error = decode_packet(&mut buf, 1, 1024, |_, _| Ok(())).unwrap_err();
        assert!(matches!(error, PgWireError::MalformedMessage(_)));
        assert_eq!(&buf[..], &[b'S', 0, 0, 0, 4]);
    }
}
