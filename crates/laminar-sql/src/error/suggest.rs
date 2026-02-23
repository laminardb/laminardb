//! Edit-distance based suggestions and case-insensitive column resolution.
//!
//! Provides Levenshtein distance computation, closest-match lookup for
//! generating "Did you mean ...?" hints, and case-insensitive column
//! name resolution for mixed-case Arrow schemas.

/// Computes the Levenshtein edit distance between two byte slices.
fn levenshtein(a: &[u8], b: &[u8]) -> usize {
    let m = a.len();
    let n = b.len();
    let mut prev: Vec<usize> = (0..=n).collect();
    let mut curr = vec![0; n + 1];

    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[n]
}

/// Returns the closest match to `input` from `candidates`, if within
/// `max_distance` edit distance.
///
/// Comparison is case-insensitive. Returns `None` if no candidate is
/// close enough or if `candidates` is empty. Exact matches (distance 0)
/// are excluded since they are not typos.
#[must_use]
pub fn closest_match<'a>(
    input: &str,
    candidates: &[&'a str],
    max_distance: usize,
) -> Option<&'a str> {
    let input_lower = input.to_ascii_lowercase();
    let mut best: Option<(&'a str, usize)> = None;

    for &candidate in candidates {
        let cand_lower = candidate.to_ascii_lowercase();
        let dist = levenshtein(input_lower.as_bytes(), cand_lower.as_bytes());
        if dist > 0 && dist <= max_distance && best.is_none_or(|(_, d)| dist < d) {
            best = Some((candidate, dist));
        }
    }

    best.map(|(s, _)| s)
}

/// Resolves a column name against a list of available columns with
/// case-insensitive fallback.
///
/// 1. **Exact match** — returns the name unchanged.
/// 2. **Unique case-insensitive match** — returns the actual column name
///    from `available`.
/// 3. **Ambiguous** (multiple case-insensitive matches) — returns `Err`
///    listing all matches.
/// 4. **No match** — returns `Err` with an edit-distance suggestion if one
///    is close enough.
///
/// # Errors
///
/// Returns [`ColumnResolveError::NotFound`] when no column matches, or
/// [`ColumnResolveError::Ambiguous`] when multiple columns match
/// case-insensitively.
pub fn resolve_column_name<'a>(
    input: &str,
    available: &[&'a str],
) -> Result<&'a str, ColumnResolveError> {
    // 1. Exact match
    for &col in available {
        if col == input {
            return Ok(col);
        }
    }

    // 2. Case-insensitive match
    let matches: Vec<&'a str> = available
        .iter()
        .copied()
        .filter(|c| c.eq_ignore_ascii_case(input))
        .collect();

    match matches.len() {
        1 => Ok(matches[0]),
        n if n > 1 => Err(ColumnResolveError::Ambiguous {
            input: input.to_string(),
            matches: matches.iter().map(|s| (*s).to_string()).collect(),
        }),
        _ => {
            // 3. No match — provide edit-distance hint
            let hint = closest_match(input, available, 2).map(ToString::to_string);
            Err(ColumnResolveError::NotFound {
                input: input.to_string(),
                suggestion: hint,
            })
        }
    }
}

/// Error from [`resolve_column_name`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnResolveError {
    /// No column matched (exact or case-insensitive).
    NotFound {
        /// The column name the user wrote.
        input: String,
        /// An edit-distance suggestion, if available.
        suggestion: Option<String>,
    },
    /// Multiple columns matched case-insensitively.
    Ambiguous {
        /// The column name the user wrote.
        input: String,
        /// All columns that matched case-insensitively.
        matches: Vec<String>,
    },
}

impl std::fmt::Display for ColumnResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound { input, suggestion } => {
                write!(f, "Column '{input}' not found")?;
                if let Some(s) = suggestion {
                    write!(f, ". Did you mean '{s}'?")?;
                }
                Ok(())
            }
            Self::Ambiguous { input, matches } => {
                write!(
                    f,
                    "Column '{input}' is ambiguous — matches: {}. \
                     Use double quotes for exact match.",
                    matches.join(", ")
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_levenshtein_identical() {
        assert_eq!(levenshtein(b"abc", b"abc"), 0);
    }

    #[test]
    fn test_levenshtein_empty() {
        assert_eq!(levenshtein(b"", b"abc"), 3);
        assert_eq!(levenshtein(b"abc", b""), 3);
    }

    #[test]
    fn test_levenshtein_single_edit() {
        assert_eq!(levenshtein(b"cat", b"car"), 1); // substitution
        assert_eq!(levenshtein(b"cat", b"cats"), 1); // insertion
        assert_eq!(levenshtein(b"cats", b"cat"), 1); // deletion
    }

    #[test]
    fn test_levenshtein_multiple_edits() {
        assert_eq!(levenshtein(b"kitten", b"sitting"), 3);
    }

    #[test]
    fn test_closest_match_found() {
        let candidates = &["user_id", "user_name", "email"];
        assert_eq!(closest_match("user_ie", candidates, 2), Some("user_id"));
    }

    #[test]
    fn test_closest_match_case_insensitive() {
        let candidates = &["UserName", "Email"];
        assert_eq!(closest_match("username", candidates, 2), None); // exact (case-insensitive) = distance 0
        assert_eq!(closest_match("usrname", candidates, 2), Some("UserName"));
    }

    #[test]
    fn test_closest_match_none_too_far() {
        let candidates = &["user_id", "email"];
        assert_eq!(closest_match("completely_different", candidates, 2), None);
    }

    #[test]
    fn test_closest_match_empty_candidates() {
        let candidates: &[&str] = &[];
        assert_eq!(closest_match("foo", candidates, 2), None);
    }

    #[test]
    fn test_closest_match_picks_best() {
        let candidates = &["price", "pride", "prime"];
        // "pric" is distance 1 from "price", distance 2 from "pride"
        assert_eq!(closest_match("pric", candidates, 2), Some("price"));
    }

    // -- resolve_column_name tests --

    #[test]
    fn test_resolve_exact_match() {
        let cols = &["tradeId", "symbol", "lastPrice"];
        assert_eq!(resolve_column_name("tradeId", cols), Ok("tradeId"));
    }

    #[test]
    fn test_resolve_case_insensitive_fallback() {
        let cols = &["tradeId", "symbol", "lastPrice"];
        assert_eq!(resolve_column_name("tradeid", cols), Ok("tradeId"));
        assert_eq!(resolve_column_name("TRADEID", cols), Ok("tradeId"));
        assert_eq!(resolve_column_name("SYMBOL", cols), Ok("symbol"));
        assert_eq!(resolve_column_name("lastprice", cols), Ok("lastPrice"));
    }

    #[test]
    fn test_resolve_ambiguous() {
        let cols = &["price", "Price", "PRICE"];
        let err = resolve_column_name("pRiCe", cols).unwrap_err();
        match err {
            ColumnResolveError::Ambiguous { input, matches } => {
                assert_eq!(input, "pRiCe");
                assert_eq!(matches.len(), 3);
            }
            other @ ColumnResolveError::NotFound { .. } => {
                panic!("expected Ambiguous, got {other}")
            }
        }
    }

    #[test]
    fn test_resolve_not_found_with_suggestion() {
        let cols = &["tradeId", "symbol"];
        let err = resolve_column_name("tadeId", cols).unwrap_err();
        match err {
            ColumnResolveError::NotFound { suggestion, .. } => {
                assert_eq!(suggestion, Some("tradeId".to_string()));
            }
            other @ ColumnResolveError::Ambiguous { .. } => {
                panic!("expected NotFound, got {other}")
            }
        }
    }

    #[test]
    fn test_resolve_not_found_no_suggestion() {
        let cols = &["tradeId", "symbol"];
        let err = resolve_column_name("completely_different", cols).unwrap_err();
        match err {
            ColumnResolveError::NotFound { suggestion, .. } => {
                assert!(suggestion.is_none());
            }
            other @ ColumnResolveError::Ambiguous { .. } => {
                panic!("expected NotFound, got {other}")
            }
        }
    }
}
