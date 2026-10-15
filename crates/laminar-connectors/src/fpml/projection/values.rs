//! Exact conversions from XML Schema lexical values to Arrow values.
//!
//! Non-string values are whitespace-collapsed first, as XML Schema does for these types.
//! Nothing is rounded or truncated: a value that does not fit its column exactly is an error.
//! Calendar dates are never shifted by a timezone designator, and a date-time written without
//! an offset is never assigned one.

use std::borrow::Cow;

use arrow_schema::TimeUnit;
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};

pub(crate) type ValueError = &'static str;

const XML_WHITESPACE: &[char] = &[' ', '\t', '\n', '\r'];

/// XML Schema `collapse`: trim and replace internal whitespace runs with one space.
pub(crate) fn collapse(text: &str) -> Cow<'_, str> {
    let trimmed = text.trim_matches(XML_WHITESPACE);
    let needs_work = trimmed
        .as_bytes()
        .windows(2)
        .any(|pair| is_space(pair[0]) && is_space(pair[1]))
        || trimmed
            .bytes()
            .any(|byte| matches!(byte, b'\t' | b'\n' | b'\r'));
    if !needs_work {
        return Cow::Borrowed(trimmed);
    }
    let mut out = String::with_capacity(trimmed.len());
    let mut space = false;
    for c in trimmed.chars() {
        if XML_WHITESPACE.contains(&c) {
            space = true;
        } else {
            if space {
                out.push(' ');
                space = false;
            }
            out.push(c);
        }
    }
    Cow::Owned(out)
}

fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r')
}

fn token(text: &str) -> &str {
    text.trim_matches(XML_WHITESPACE)
}

/// `xsd:boolean`: `true`, `false`, `1`, or `0`.
pub(crate) fn boolean(text: &str) -> Result<bool, ValueError> {
    match token(text) {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err("value is not an xsd:boolean"),
    }
}

/// `xsd:integer` and its derived types: an optional sign and digits.
pub(crate) fn integer(text: &str) -> Result<i128, ValueError> {
    let text = token(text);
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("value is not an xsd:integer");
    }
    text.parse::<i128>().map_err(|_| "integer is out of range")
}

/// `xsd:decimal` as the unscaled value of `Decimal128(precision, scale)`.
pub(crate) fn decimal(text: &str, precision: u8, scale: i8) -> Result<i128, ValueError> {
    let text = token(text);
    let unsigned = text.strip_prefix('+').unwrap_or(text);
    if unsigned.starts_with(['+', '-']) && text.starts_with('+') {
        return Err("value is not an xsd:decimal");
    }
    crate::schema::decimal_text::unscaled(unsigned, precision, scale)
}

/// `xsd:date` as days since the Unix epoch. A timezone designator is validated but does not
/// move the calendar date.
pub(crate) fn date(text: &str) -> Result<i32, ValueError> {
    let text = token(text);
    let (day, zone) = split_zone(text, 10)?;
    let date = parse_date(day)?;
    if let Some(zone) = zone {
        offset_minutes(zone)?;
    }
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or("epoch")?;
    i32::try_from((date - epoch).num_days()).map_err(|_| "date is out of range")
}

/// An `xsd:dateTime` as written: wall-clock nanoseconds since the Unix epoch and the offset,
/// when one was written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DateTime {
    pub(crate) local_nanos: i64,
    pub(crate) offset_minutes: Option<i32>,
}

impl DateTime {
    /// The UTC instant, when an offset was written.
    pub(crate) fn utc_nanos(self) -> Option<Result<i64, ValueError>> {
        self.offset_minutes.map(|offset| {
            self.local_nanos
                .checked_sub(i64::from(offset) * 60_000_000_000)
                .ok_or("date-time is out of range")
        })
    }
}

/// Parse `YYYY-MM-DDThh:mm:ss[.fraction][Z|±hh:mm]`. `24:00:00` is the start of the next day.
/// Fractions finer than nanoseconds must be zero.
pub(crate) fn date_time(text: &str) -> Result<DateTime, ValueError> {
    let text = token(text);
    let (day, time) = text.split_once('T').ok_or("value is not an xsd:dateTime")?;
    let date = parse_date(day)?;
    let time_len = time.find(['Z', '+', '-']).unwrap_or(time.len());
    let (clock, zone) = time.split_at(time_len);
    let offset = if zone.is_empty() {
        None
    } else {
        Some(offset_minutes(zone)?)
    };
    let (seconds, fraction) = clock.split_once('.').unwrap_or((clock, ""));
    if clock.contains('.') && (fraction.is_empty() || !fraction.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err("date-time fraction is malformed");
    }
    if fraction.len() > 9 && fraction[9..].bytes().any(|byte| byte != b'0') {
        return Err("date-time is more precise than nanoseconds");
    }
    let mut nanos = 0_i64;
    for (index, byte) in fraction.bytes().take(9).enumerate() {
        nanos += i64::from(byte - b'0') * 10_i64.pow(8 - u32::try_from(index).unwrap_or(8));
    }
    let (date, clock) = if seconds == "24:00:00" {
        if nanos != 0 {
            return Err("24:00:00 cannot have a fraction");
        }
        (
            date.succ_opt().ok_or("date-time is out of range")?,
            NaiveTime::MIN,
        )
    } else {
        if seconds.len() != 8 {
            return Err("value is not an xsd:dateTime");
        }
        (
            date,
            NaiveTime::parse_from_str(seconds, "%H:%M:%S")
                .map_err(|_| "value is not an xsd:dateTime")?,
        )
    };
    let local = NaiveDateTime::new(date, clock) + Duration::nanoseconds(nanos);
    Ok(DateTime {
        local_nanos: local
            .and_utc()
            .timestamp_nanos_opt()
            .ok_or("date-time is outside the nanosecond timestamp range")?,
        offset_minutes: offset,
    })
}

/// Nanoseconds as `unit`, refusing to drop precision.
pub(crate) fn nanos_to_unit(nanos: i64, unit: TimeUnit) -> Result<i64, ValueError> {
    let divisor = match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };
    if nanos % divisor != 0 {
        return Err("date-time is more precise than the column unit");
    }
    Ok(nanos / divisor)
}

fn parse_date(text: &str) -> Result<NaiveDate, ValueError> {
    let bytes = text.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit())
    {
        return Err("value is not an xsd:date (YYYY-MM-DD)");
    }
    NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|_| "value is not a valid calendar date")
}

fn split_zone(text: &str, at: usize) -> Result<(&str, Option<&str>), ValueError> {
    if text.len() < at {
        return Err("value is not an xsd:date (YYYY-MM-DD)");
    }
    let (value, zone) = text.split_at(at);
    Ok((value, (!zone.is_empty()).then_some(zone)))
}

fn offset_minutes(zone: &str) -> Result<i32, ValueError> {
    if zone == "Z" {
        return Ok(0);
    }
    let bytes = zone.as_bytes();
    if bytes.len() != 6
        || !matches!(bytes[0], b'+' | b'-')
        || bytes[3] != b':'
        || ![1, 2, 4, 5]
            .iter()
            .all(|index| bytes[*index].is_ascii_digit())
    {
        return Err("timezone must be Z or ±hh:mm");
    }
    let hours = i32::from(bytes[1] - b'0') * 10 + i32::from(bytes[2] - b'0');
    let minutes = i32::from(bytes[4] - b'0') * 10 + i32::from(bytes[5] - b'0');
    if minutes > 59 || hours > 14 || (hours == 14 && minutes != 0) {
        return Err("timezone offset is out of range");
    }
    let total = hours * 60 + minutes;
    Ok(if bytes[0] == b'-' { -total } else { total })
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 86_400_000_000_000;

    #[test]
    fn decimals_follow_xsd_lexical_rules_exactly() {
        assert_eq!(decimal(" +100.25 ", 18, 4), Ok(1_002_500));
        assert_eq!(decimal("-0.5", 18, 2), Ok(-50));
        assert_eq!(decimal("1.230000", 18, 2), Ok(123));
        assert!(decimal("1.234", 18, 2).is_err(), "would round");
        assert!(decimal("+-1", 18, 2).is_err());
        assert!(decimal("1e5", 18, 2).is_err());
        assert!(decimal("", 18, 2).is_err());
        assert_eq!(
            decimal("12345678901234567890.123456789012345678", 38, 18),
            Ok(12_345_678_901_234_567_890_123_456_789_012_345_678)
        );
        assert!(
            decimal("123456789012345678901.0", 38, 18).is_err(),
            "21 integer digits"
        );
    }

    #[test]
    fn dates_keep_the_written_calendar_day() {
        assert_eq!(date("1970-01-02"), Ok(1));
        assert_eq!(date("2009-12-25Z"), date("2009-12-25"));
        assert_eq!(date("2009-12-25-05:00"), date("2009-12-25"));
        assert!(date("2009-12-25+15:00").is_err());
        assert!(date("2009-02-30").is_err());
        assert!(date("09-12-25").is_err());
        assert!(date("2009-12-25T00:00:00").is_err());
    }

    #[test]
    fn date_times_never_invent_an_offset() {
        let local = date_time("1970-01-02T00:00:00").unwrap();
        assert_eq!(
            local,
            DateTime {
                local_nanos: DAY,
                offset_minutes: None
            }
        );
        assert_eq!(local.utc_nanos(), None);
        let offset = date_time("1970-01-02T00:00:00.1234567-05:00").unwrap();
        assert_eq!(offset.offset_minutes, Some(-300));
        assert_eq!(offset.local_nanos, DAY + 123_456_700);
        assert_eq!(
            offset.utc_nanos(),
            Some(Ok(DAY + 5 * 3_600_000_000_000 + 123_456_700))
        );
        assert_eq!(date_time("1970-01-01T24:00:00Z").unwrap().local_nanos, DAY);
        assert_eq!(
            date_time("1970-01-01T00:00:00.1234567890Z")
                .unwrap()
                .local_nanos,
            123_456_789
        );
        assert!(date_time("1970-01-01T00:00:00.1234567891Z").is_err());
        assert!(date_time("1970-01-01T00:00:00.Z").is_err());
        assert!(date_time("1970-01-01 00:00:00").is_err());
        assert_eq!(
            nanos_to_unit(1_500, TimeUnit::Microsecond),
            Err("date-time is more precise than the column unit")
        );
    }

    #[test]
    fn booleans_integers_and_whitespace() {
        assert_eq!(boolean(" 1\n"), Ok(true));
        assert!(boolean("TRUE").is_err());
        assert_eq!(integer("+007"), Ok(7));
        assert!(integer("1.0").is_err());
        assert_eq!(collapse("  a \t\n b  "), "a b");
        assert!(matches!(collapse("a b"), Cow::Borrowed("a b")));
    }
}
