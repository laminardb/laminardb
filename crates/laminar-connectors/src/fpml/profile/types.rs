//! Column type grammar.
//!
//! ```text
//! type := 'utf8' | 'bool' | 'int32' | 'int64' | 'uint32' | 'uint64'
//!       | 'decimal' | 'decimal(' precision ',' scale ')'
//!       | 'date32' | 'datetime' | 'timestamp(' unit [', UTC'] ')'
//!       | 'struct<' template '>' | 'list<' type '>'
//! ```
//!
//! `decimal` is `decimal(38, 18)`. `datetime` keeps an `xsd:dateTime` losslessly as
//! `struct<utc, local, offset_minutes>`. `timestamp(unit)` holds a wall-clock value written
//! without an offset; `timestamp(unit, UTC)` requires an offset and normalizes to UTC (Arrow
//! zone `+00:00`).

use arrow_schema::TimeUnit;

/// Arrow zone of UTC timestamps; the offset form DataFusion's `now()` uses.
pub(crate) const UTC_ZONE: &str = "+00:00";

/// Default precision and scale of `decimal`.
pub(crate) const DECIMAL_PRECISION: u8 = 38;
pub(crate) const DECIMAL_SCALE: i8 = 18;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedType {
    Utf8,
    Bool,
    Int32,
    Int64,
    UInt32,
    UInt64,
    Decimal { precision: u8, scale: i8 },
    Date32,
    DateTime,
    Timestamp { unit: TimeUnit, utc: bool },
    Struct(String),
    List(Box<ParsedType>),
}

pub(crate) fn parse(text: &str) -> Result<ParsedType, String> {
    let text = text.trim();
    if let Some(inner) = wrapped(text, "list<", ">") {
        let item = parse(inner)?;
        if matches!(item, ParsedType::List(_)) {
            return Err("lists of lists are not supported; wrap the inner list in a struct".into());
        }
        return Ok(ParsedType::List(Box::new(item)));
    }
    if let Some(inner) = wrapped(text, "struct<", ">") {
        let inner = inner.trim();
        if inner.is_empty() {
            return Err("struct<> needs a template name".into());
        }
        return Ok(ParsedType::Struct(inner.to_owned()));
    }
    if let Some(inner) = wrapped(text, "decimal(", ")") {
        let (precision, scale) = inner
            .split_once(',')
            .ok_or_else(|| format!("'{text}' needs decimal(precision, scale)"))?;
        let precision: u8 = precision
            .trim()
            .parse()
            .map_err(|_| format!("invalid decimal precision in '{text}'"))?;
        let scale: i8 = scale
            .trim()
            .parse()
            .map_err(|_| format!("invalid decimal scale in '{text}'"))?;
        if !(1..=38).contains(&precision)
            || scale < 0
            || scale > i8::try_from(precision).unwrap_or(38)
        {
            return Err(format!(
                "'{text}' needs 1 <= precision <= 38 and 0 <= scale <= precision"
            ));
        }
        return Ok(ParsedType::Decimal { precision, scale });
    }
    if let Some(inner) = wrapped(text, "timestamp(", ")") {
        let (unit, zone) = match inner.split_once(',') {
            Some((unit, zone)) => (unit.trim(), Some(zone.trim())),
            None => (inner.trim(), None),
        };
        let unit = match unit {
            "s" => TimeUnit::Second,
            "ms" => TimeUnit::Millisecond,
            "us" => TimeUnit::Microsecond,
            "ns" => TimeUnit::Nanosecond,
            other => return Err(format!("unknown timestamp unit '{other}' (s, ms, us, ns)")),
        };
        let utc = match zone {
            None => false,
            Some("UTC") => true,
            Some(other) => {
                return Err(format!(
                    "timestamp zone must be UTC, got '{other}'; offsets are normalized to UTC"
                ))
            }
        };
        return Ok(ParsedType::Timestamp { unit, utc });
    }
    Ok(match text {
        "utf8" => ParsedType::Utf8,
        "bool" => ParsedType::Bool,
        "int32" => ParsedType::Int32,
        "int64" => ParsedType::Int64,
        "uint32" => ParsedType::UInt32,
        "uint64" => ParsedType::UInt64,
        "decimal" => ParsedType::Decimal {
            precision: DECIMAL_PRECISION,
            scale: DECIMAL_SCALE,
        },
        "date32" => ParsedType::Date32,
        "datetime" => ParsedType::DateTime,
        other => return Err(format!("unknown column type '{other}'")),
    })
}

fn wrapped<'a>(text: &'a str, open: &str, close: &str) -> Option<&'a str> {
    text.strip_prefix(open)?.strip_suffix(close)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_type_grammar() {
        assert_eq!(
            parse("list<struct<money>>").unwrap(),
            ParsedType::List(Box::new(ParsedType::Struct("money".into())))
        );
        assert_eq!(
            parse("decimal(18, 4)").unwrap(),
            ParsedType::Decimal {
                precision: 18,
                scale: 4
            }
        );
        assert_eq!(
            parse("timestamp(ns, UTC)").unwrap(),
            ParsedType::Timestamp {
                unit: TimeUnit::Nanosecond,
                utc: true
            }
        );
        assert_eq!(
            parse("list<utf8>").unwrap(),
            ParsedType::List(Box::new(ParsedType::Utf8))
        );
        for bad in [
            "list<list<utf8>>",
            "decimal(39,2)",
            "decimal(5,6)",
            "timestamp(ms, Europe/London)",
            "float64",
            "struct<>",
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }
}
