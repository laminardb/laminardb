use super::*;

#[test]
fn test_parse_valid_lsn() {
    let lsn: Lsn = "0/1234ABCD".parse().unwrap();
    assert_eq!(lsn.segment(), 0);
    assert_eq!(lsn.offset(), 0x1234_ABCD);
    assert_eq!(lsn.as_u64(), 0x0000_0000_1234_ABCD);
}

#[test]
fn test_parse_with_high_segment() {
    let lsn: Lsn = "1/0".parse().unwrap();
    assert_eq!(lsn.segment(), 1);
    assert_eq!(lsn.offset(), 0);
    assert_eq!(lsn.as_u64(), 0x0000_0001_0000_0000);
}

#[test]
fn test_parse_max_lsn() {
    let lsn: Lsn = "FFFFFFFF/FFFFFFFF".parse().unwrap();
    assert_eq!(lsn, Lsn::MAX);
}

#[test]
fn test_parse_invalid_no_slash() {
    assert!("12345".parse::<Lsn>().is_err());
}

#[test]
fn test_parse_invalid_hex() {
    assert!("ZZ/1234".parse::<Lsn>().is_err());
    assert!("0/GHIJ".parse::<Lsn>().is_err());
}

#[test]
fn test_display() {
    let lsn = Lsn::new(0x0000_0001_1234_ABCD);
    assert_eq!(lsn.to_string(), "1/1234ABCD");
}

#[test]
fn test_display_zero() {
    assert_eq!(Lsn::ZERO.to_string(), "0/0");
}

#[test]
fn test_roundtrip() {
    let original = "A/BC1234";
    let lsn: Lsn = original.parse().unwrap();
    assert_eq!(lsn.to_string(), original);
}

#[test]
fn test_ordering() {
    let a: Lsn = "0/100".parse().unwrap();
    let b: Lsn = "0/200".parse().unwrap();
    let c: Lsn = "1/0".parse().unwrap();
    assert!(a < b);
    assert!(b < c);
    assert!(a < c);
}

#[test]
fn test_diff() {
    let a: Lsn = "0/200".parse().unwrap();
    let b: Lsn = "0/100".parse().unwrap();
    assert_eq!(a.diff(b), 0x100);
    assert_eq!(b.diff(a), 0); // saturating
}

#[test]
fn test_advance() {
    let lsn: Lsn = "0/100".parse().unwrap();
    let advanced = lsn.advance(256);
    assert_eq!(advanced.to_string(), "0/200");
}

#[test]
fn test_is_zero() {
    assert!(Lsn::ZERO.is_zero());
    assert!(!Lsn::new(1).is_zero());
}

#[test]
fn test_from_u64() {
    let lsn = Lsn::from(42u64);
    assert_eq!(lsn.as_u64(), 42);
    let val: u64 = lsn.into();
    assert_eq!(val, 42);
}
