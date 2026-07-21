use super::*;

fn cfg(sql: &str) -> TemporalFilterConfig {
    match analyze_temporal_filter(sql) {
        TemporalFilterAnalysis::Recognized(c) => *c,
        TemporalFilterAnalysis::PresentUnrecognized => {
            panic!("expected Recognized, got PresentUnrecognized: {sql}")
        }
        TemporalFilterAnalysis::NotPresent => {
            panic!("expected Recognized, got NotPresent: {sql}")
        }
    }
}

#[test]
fn projection_list_recognised() {
    let c = cfg("SELECT id, amount FROM events WHERE ts > now() - INTERVAL '1' MINUTE");
    assert_eq!(c.proj_cols, vec!["id".to_string(), "amount".to_string()]);
    assert_eq!(c.time_col, "ts");
    // Expression / aliased / qualified projections stay out of scope.
    assert!(matches!(
        analyze_temporal_filter("SELECT id + 1 FROM events WHERE ts > now() - INTERVAL '1' MINUTE"),
        TemporalFilterAnalysis::PresentUnrecognized
    ));
}

#[test]
fn lower_bound_ttl_strict() {
    let c = cfg("SELECT * FROM events WHERE evt > now() - INTERVAL '10' MINUTE");
    assert_eq!(c.source_table, "events");
    assert!(c.proj_cols.is_empty(), "`SELECT *` ⇒ no explicit columns");
    assert_eq!(c.time_col, "evt");
    assert_eq!(
        c.lower,
        Some(TemporalBound {
            off_ms: -600_000,
            strict: true
        })
    );
    assert_eq!(c.upper, None);
}

#[test]
fn between_inclusive_both_bounds() {
    let c = cfg(
        "SELECT * FROM e WHERE ts BETWEEN now() - INTERVAL '2' MINUTE \
         AND now() + INTERVAL '30' SECOND",
    );
    assert_eq!(
        c.lower,
        Some(TemporalBound {
            off_ms: -120_000,
            strict: false
        })
    );
    assert_eq!(
        c.upper,
        Some(TemporalBound {
            off_ms: 30_000,
            strict: false
        })
    );
}

#[test]
fn unrecognised_when_extra_conjunct() {
    assert!(matches!(
        analyze_temporal_filter(
            "SELECT * FROM e WHERE region = 'us' AND ts > now() - INTERVAL '1' MINUTE"
        ),
        TemporalFilterAnalysis::PresentUnrecognized
    ));
}

#[test]
fn not_present_for_ordinary_query() {
    // No false positives — ordinary queries are untouched.
    assert!(matches!(
        analyze_temporal_filter("SELECT * FROM e WHERE region = 'us'"),
        TemporalFilterAnalysis::NotPresent
    ));
}
