//! FpML decode benchmarks: one official document per bundled asset-class profile, plus a
//! 200-trade data document, measured end to end through `FpmlDecoder` (pre-scan, libxml2 schema
//! validation, reference checks, and typed projection on the decoder's worker thread).
//!
//! Criterion reports the mean time per document. After it, a fixed-iteration run prints
//! p50/p90/p99/p99.9 latency, documents per second, and rows per second for each case.

use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput};
use laminar_connectors::fpml::{FpmlConfig, FpmlDecoder, Limits, ProfileSource, ReferencePolicy};

const CORPUS: &str = "tests/fixtures/fpml/fpml-5-13-8-rec-2/confirmation";
const BULK_TRADES: usize = 200;
const PERCENTILE_ITERATIONS: usize = 2_000;

/// `(case, bundled profile, corpus document)`.
const CASES: &[(&str, &str, &str)] = &[
    (
        "rates_swap",
        "fpml-5-13-confirmation-rates",
        "products/interest-rate-derivatives/ird-ex01-vanilla-swap.xml",
    ),
    (
        "fx_spot",
        "fpml-5-13-confirmation-fx",
        "products/fx-derivatives/fx-ex01-fx-spot.xml",
    ),
    (
        "credit_cds",
        "fpml-5-13-confirmation-credit",
        "products/credit-derivatives/cd-ex01-long-asia-corp-fixreg.xml",
    ),
    (
        "equity_option",
        "fpml-5-13-confirmation-equity-options",
        "products/equity-options/eqd-ex01-american-call-stock-long-form.xml",
    ),
    (
        "equity_return_swap",
        "fpml-5-13-confirmation-equity-swaps",
        "products/equity-swaps/eqs-ex01-single-underlyer-execution-long-form.xml",
    ),
    (
        "equity_variance_swap",
        "fpml-5-13-confirmation-equity-volatility",
        "products/variance-swaps/eqvs-ex01-variance-swap-index.xml",
    ),
    (
        "commodity_gas_swap",
        "fpml-5-13-confirmation-commodity",
        "products/commodity-derivatives/com-ex1-gas-swap-daily-delivery-prices-last.xml",
    ),
    (
        "securities_repo",
        "fpml-5-13-confirmation-securities",
        "products/repo/repo-ex01-repo-fixed-rate.xml",
    ),
    (
        "cross_asset_strategy",
        "fpml-5-13-confirmation-cross-asset",
        "products/fx-derivatives/fx-ex23-straddle.xml",
    ),
    (
        "trade_envelope",
        "fpml-5-13-confirmation-trade",
        "products/interest-rate-derivatives/ird-ex01-vanilla-swap.xml",
    ),
    (
        "document_clearing",
        "fpml-5-13-confirmation-document",
        "business-processes/clearing/msg-ex10-clearingConfirmed-clearing-performed.xml",
    ),
    (
        "loan_servicing_bulk",
        "fpml-5-13-confirmation-loan-servicing",
        "products/loan/Loan_Bulk_ex108(Roll).xml",
    ),
];

fn corpus(path: &str) -> Vec<u8> {
    std::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(CORPUS)
            .join(path),
    )
    .expect("corpus document")
}

/// A data document holding `BULK_TRADES` copies of the FX spot example's trade.
fn bulk_document() -> Vec<u8> {
    let source = String::from_utf8(corpus("products/fx-derivatives/fx-ex01-fx-spot.xml"))
        .expect("utf-8 example");
    let trade_start = source.find("<trade>").expect("trade");
    let trade_end = source.find("</trade>").expect("trade end") + "</trade>".len();
    let parties_start = source.find("<party ").expect("party");
    let parties_end = source.rfind("</party>").expect("party end") + "</party>".len();
    let mut document = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<dataDocument xmlns=\"http://www.fpml.org/FpML-5/confirmation\" fpmlVersion=\"5-13\">\n",
    );
    for _ in 0..BULK_TRADES {
        document.push_str(&source[trade_start..trade_end]);
        document.push('\n');
    }
    document.push_str(&source[parties_start..parties_end]);
    document.push_str("\n</dataDocument>\n");
    document.into_bytes()
}

fn decoder(runtime: &tokio::runtime::Runtime, profile: &str) -> FpmlDecoder {
    let config = FpmlConfig {
        profile: ProfileSource::Bundled(profile.to_owned()),
        schema_path: None,
        references: ReferencePolicy::Resolve,
        limits: Limits::default(),
    };
    let (artifact, compiled) = config.resolve().expect("bundled profile");
    runtime
        .block_on(FpmlDecoder::start(
            artifact, &compiled, &config, "bench", None, None,
        ))
        .expect("decoder")
}

struct Case {
    name: String,
    decoder: FpmlDecoder,
    document: Vec<u8>,
    rows: usize,
}

fn cases(runtime: &tokio::runtime::Runtime) -> Vec<Case> {
    let mut out: Vec<Case> = CASES
        .iter()
        .map(|(name, profile, path)| {
            let decoder = decoder(runtime, profile);
            let document = corpus(path);
            let rows = decoder
                .decode(&document)
                .expect("valid document")
                .num_rows();
            Case {
                name: (*name).to_owned(),
                decoder,
                document,
                rows,
            }
        })
        .collect();
    let decoder = decoder(runtime, "fpml-5-13-confirmation-fx");
    let document = bulk_document();
    let rows = decoder
        .decode(&document)
        .expect("valid document")
        .num_rows();
    out.push(Case {
        name: format!("fx_spot_{BULK_TRADES}_trades"),
        decoder,
        document,
        rows,
    });
    out
}

fn criterion_runs(cases: &[Case]) {
    let mut criterion = Criterion::default()
        .configure_from_args()
        .measurement_time(Duration::from_secs(3));
    let mut group = criterion.benchmark_group("fpml_decoder");
    for case in cases {
        group.throughput(Throughput::Elements(1));
        group.bench_function(&case.name, |b| {
            b.iter(|| {
                black_box(
                    case.decoder
                        .decode(black_box(&case.document))
                        .expect("decode"),
                )
            });
        });
    }
    group.finish();
    criterion.final_summary();
}

fn percentile(sorted: &[Duration], fraction: f64) -> Duration {
    let index = ((sorted.len() as f64 - 1.0) * fraction).round() as usize;
    sorted[index]
}

fn percentile_report(cases: &[Case]) {
    println!(
        "\n{:<26} {:>7} {:>6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>11}",
        "case", "bytes", "rows", "p50 us", "p90 us", "p99 us", "p99.9 us", "docs/s", "rows/s"
    );
    for case in cases {
        let mut samples = Vec::with_capacity(PERCENTILE_ITERATIONS);
        let started = Instant::now();
        for _ in 0..PERCENTILE_ITERATIONS {
            let begin = Instant::now();
            black_box(case.decoder.decode(&case.document).expect("decode"));
            samples.push(begin.elapsed());
        }
        let elapsed = started.elapsed().as_secs_f64();
        samples.sort_unstable();
        let micros = |duration: Duration| duration.as_secs_f64() * 1e6;
        let documents_per_second = PERCENTILE_ITERATIONS as f64 / elapsed;
        println!(
            "{:<26} {:>7} {:>6} {:>10.1} {:>10.1} {:>10.1} {:>10.1} {:>10.0} {:>11.0}",
            case.name,
            case.document.len(),
            case.rows,
            micros(percentile(&samples, 0.50)),
            micros(percentile(&samples, 0.90)),
            micros(percentile(&samples, 0.99)),
            micros(percentile(&samples, 0.999)),
            documents_per_second,
            documents_per_second * case.rows as f64,
        );
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let cases = cases(&runtime);
    criterion_runs(&cases);
    percentile_report(&cases);
}
