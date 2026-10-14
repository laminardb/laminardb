//! Exact conversions from FIX field text to Arrow values.
//!
//! Conversions never round: a value that does not fit the declared Arrow type exactly is an
//! error, never a silently coerced or truncated value.

use arrow_schema::TimeUnit;
use chrono::{NaiveDate, NaiveDateTime};

/// Why a field value could not be converted.
pub(crate) type ValueError = &'static str;

fn ascii(bytes: &[u8]) -> Result<&str, ValueError> {
    std::str::from_utf8(bytes)
        .ok()
        .filter(|text| text.is_ascii())
        .ok_or("value is not ASCII")
}

fn digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

/// FIX `int`: optional `-` followed by digits.
pub(crate) fn integer(bytes: &[u8]) -> Result<i128, ValueError> {
    let text = ascii(bytes)?;
    let magnitude = text.strip_prefix('-').unwrap_or(text);
    if !digits(magnitude) {
        return Err("value is not a FIX integer");
    }
    text.parse::<i128>().map_err(|_| "integer is out of range")
}

/// FIX `float`/`Price`/`Qty`/`Amt` as an unscaled `Decimal128(precision, scale)` value. Extra
/// non-zero fractional digits or too many integer digits are errors, never rounded.
pub(crate) fn decimal(bytes: &[u8], precision: u8, scale: i8) -> Result<i128, ValueError> {
    let text = ascii(bytes)?;
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if (whole.is_empty() && fraction.is_empty())
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("value is not a FIX decimal");
    }
    let scale = usize::try_from(scale).map_err(|_| "negative decimal scales are not supported")?;
    let kept = fraction.get(..fraction.len().min(scale)).unwrap_or("");
    if fraction.len() > scale && fraction[scale..].bytes().any(|byte| byte != b'0') {
        return Err("value has more fractional digits than the column scale");
    }
    let mut unscaled: i128 = 0;
    let mut significant = 0_usize;
    for byte in whole
        .bytes()
        .chain(kept.bytes())
        .chain(std::iter::repeat_n(b'0', scale - kept.len()))
    {
        if unscaled != 0 || byte != b'0' {
            significant += 1;
        }
        unscaled = unscaled
            .checked_mul(10)
            .and_then(|value| value.checked_add(i128::from(byte - b'0')))
            .ok_or("decimal is out of range")?;
    }
    if significant > usize::from(precision) {
        return Err("decimal exceeds the column precision");
    }
    Ok(if negative { -unscaled } else { unscaled })
}

/// FIX `Boolean`: `Y` or `N`.
pub(crate) fn boolean(bytes: &[u8]) -> Result<bool, ValueError> {
    match bytes {
        b"Y" => Ok(true),
        b"N" => Ok(false),
        _ => Err("value is not a FIX boolean (Y/N)"),
    }
}

/// FIX `UTCTimestamp` (`YYYYMMDD-HH:MM:SS[.f{1,9}]`) in `unit` since the Unix epoch. A value
/// more precise than `unit` is an error unless `truncate` drops the extra digits.
pub(crate) fn utc_timestamp(
    bytes: &[u8],
    unit: TimeUnit,
    truncate: bool,
) -> Result<i64, ValueError> {
    let text = ascii(bytes)?;
    let (seconds_part, fraction) = text.split_once('.').unwrap_or((text, ""));
    if fraction.len() > 9 || (text.contains('.') && !digits(fraction)) {
        return Err("timestamp fraction is not 1-9 digits");
    }
    let date_time = NaiveDateTime::parse_from_str(seconds_part, "%Y%m%d-%H:%M:%S")
        .map_err(|_| "value is not a FIX UTCTimestamp")?;
    let mut nanos: i64 = 0;
    for byte in fraction.bytes() {
        nanos = nanos * 10 + i64::from(byte - b'0');
    }
    for _ in fraction.len()..9 {
        nanos *= 10;
    }
    let seconds = date_time.and_utc().timestamp();
    let (per_second, divisor) = match unit {
        TimeUnit::Second => (1, 1_000_000_000),
        TimeUnit::Millisecond => (1_000, 1_000_000),
        TimeUnit::Microsecond => (1_000_000, 1_000),
        TimeUnit::Nanosecond => (1_000_000_000, 1),
    };
    if nanos % divisor != 0 && !truncate {
        return Err("timestamp is more precise than the column unit; see 'timestamp.precision'");
    }
    seconds
        .checked_mul(per_second)
        .and_then(|value| value.checked_add(nanos / divisor))
        .ok_or("timestamp is out of range")
}

/// FIX `LocalMktDate`/`UTCDateOnly` (`YYYYMMDD`) as days since the Unix epoch.
pub(crate) fn date(bytes: &[u8]) -> Result<i32, ValueError> {
    let text = ascii(bytes)?;
    if text.len() != 8 || !digits(text) {
        return Err("value is not a FIX date (YYYYMMDD)");
    }
    let date =
        NaiveDate::parse_from_str(text, "%Y%m%d").map_err(|_| "value is not a valid date")?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or(NaiveDate::MIN);
    i32::try_from((date - epoch).num_days()).map_err(|_| "date is out of range")
}

/// Wall-clock nanoseconds converted to `unit`, flooring sub-unit precision. Used only for the
/// connector's own receive-time measurement, never for message values.
pub(crate) fn nanos_to_unit(nanos: i64, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => nanos.div_euclid(1_000_000_000),
        TimeUnit::Millisecond => nanos.div_euclid(1_000_000),
        TimeUnit::Microsecond => nanos.div_euclid(1_000),
        TimeUnit::Nanosecond => nanos,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals_are_exact_and_never_rounded() {
        assert_eq!(decimal(b"100.25", 18, 4), Ok(1_002_500));
        assert_eq!(decimal(b"-0.5", 18, 2), Ok(-50));
        assert_eq!(decimal(b"007.10", 5, 2), Ok(710));
        assert_eq!(
            decimal(b"1.230000", 18, 2),
            Ok(123),
            "trailing zeros drop exactly"
        );
        assert_eq!(decimal(b".5", 18, 1), Ok(5));
        assert_eq!(decimal(b"5.", 18, 1), Ok(50));
        assert!(decimal(b"1.234", 18, 2).is_err(), "would round");
        assert!(decimal(b"12345.6", 5, 1).is_err(), "exceeds precision");
        assert_eq!(decimal(b"9999.9", 5, 1), Ok(99_999));
        assert!(decimal(b"1e5", 18, 2).is_err());
        assert!(decimal(b"", 18, 2).is_err());
        assert!(decimal(b".", 18, 2).is_err());
        assert!(decimal(b"+1", 18, 2).is_err());
        assert!(decimal(b"99999999999999999999999999999999999999999", 38, 0).is_err());
        assert_eq!(
            decimal(b"99999999999999999999999999999999999999", 38, 0),
            Ok(99_999_999_999_999_999_999_999_999_999_999_999_999)
        );
    }

    #[test]
    fn timestamps_keep_their_precision_or_fail() {
        let base = 1_760_443_200; // 2025-10-14T12:00:00Z
        assert_eq!(
            utc_timestamp(b"20251014-12:00:00", TimeUnit::Second, false),
            Ok(base)
        );
        assert_eq!(
            utc_timestamp(b"20251014-12:00:00.123", TimeUnit::Millisecond, false),
            Ok(base * 1_000 + 123)
        );
        assert_eq!(
            utc_timestamp(b"20251014-12:00:00.123456789", TimeUnit::Nanosecond, false),
            Ok(base * 1_000_000_000 + 123_456_789)
        );
        assert_eq!(
            utc_timestamp(b"20251014-12:00:00.120", TimeUnit::Millisecond, false),
            Ok(base * 1_000 + 120)
        );
        assert!(utc_timestamp(b"20251014-12:00:00.1234", TimeUnit::Millisecond, false).is_err());
        assert!(utc_timestamp(b"20251014-12:00:00.", TimeUnit::Millisecond, false).is_err());
        assert!(utc_timestamp(b"2025-10-14T12:00:00", TimeUnit::Second, false).is_err());
        assert!(utc_timestamp(b"20251314-12:00:00", TimeUnit::Second, false).is_err());
    }

    #[test]
    fn truncation_is_explicit_and_drops_digits() {
        let base = 1_760_443_200; // 2025-10-14T12:00:00Z
        assert_eq!(
            utc_timestamp(b"20251014-12:00:00.123456789", TimeUnit::Microsecond, true),
            Ok(base * 1_000_000 + 123_456)
        );
        assert!(
            utc_timestamp(b"20251014-12:00:00.123456789", TimeUnit::Microsecond, false).is_err()
        );
    }

    #[test]
    fn integers_dates_and_booleans_are_strict() {
        assert_eq!(integer(b"007"), Ok(7));
        assert_eq!(integer(b"-12"), Ok(-12));
        assert!(integer(b"+1").is_err());
        assert!(integer(b"1.0").is_err());
        assert!(integer(b"").is_err());
        assert_eq!(date(b"19700102"), Ok(1));
        assert!(date(b"20250230").is_err());
        assert_eq!(boolean(b"Y"), Ok(true));
        assert!(boolean(b"y").is_err());
    }
}

#[cfg(test)]
mod properties {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn formatted_decimals_parse_back_exactly(unscaled in any::<i64>(), scale in 0_i8..12) {
            let magnitude = unscaled.unsigned_abs().to_string();
            let digits = usize::try_from(scale).expect("scale");
            let padded = format!("{magnitude:0>width$}", width = digits + 1);
            let (whole, fraction) = padded.split_at(padded.len() - digits);
            let sign = if unscaled < 0 { "-" } else { "" };
            let text = if digits == 0 { format!("{sign}{whole}") } else { format!("{sign}{whole}.{fraction}") };
            prop_assert_eq!(decimal(text.as_bytes(), 38, scale), Ok(i128::from(unscaled)));
        }

        #[test]
        fn arbitrary_text_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            let _ = decimal(&bytes, 18, 4);
            let _ = integer(&bytes);
            let _ = utc_timestamp(&bytes, TimeUnit::Nanosecond, false);
            let _ = date(&bytes);
            let _ = boolean(&bytes);
        }
    }
}
