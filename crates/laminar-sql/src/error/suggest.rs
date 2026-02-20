//! Edit-distance based suggestions for typo correction.
//!
//! Provides Levenshtein distance computation and closest-match lookup
//! for generating "Did you mean ...?" hints in error messages.

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
            curr[j] = (prev[j] + 1)
                .min(curr[j - 1] + 1)
                .min(prev[j - 1] + cost);
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
        if dist > 0
            && dist <= max_distance
            && best.is_none_or(|(_, d)| dist < d)
        {
            best = Some((candidate, dist));
        }
    }

    best.map(|(s, _)| s)
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
}
