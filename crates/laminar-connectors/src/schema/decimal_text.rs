//! Exact conversion of decimal text to an unscaled `Decimal128` value.
//!
//! Never rounds: extra non-zero fractional digits or too many significant digits are errors.

/// `[-]digits[.digits]` (at least one digit overall) as the unscaled value of
/// `Decimal128(precision, scale)`.
pub(crate) fn unscaled(text: &str, precision: u8, scale: i8) -> Result<i128, &'static str> {
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if (whole.is_empty() && fraction.is_empty())
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("value is not a decimal");
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
