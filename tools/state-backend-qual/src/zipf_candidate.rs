//! Provisional, word-in/rank-out numerical feasibility code.
//!
//! This module is not qualification evidence and is not wired to a workload,
//! counter, runner command, or backend. The provisional algorithm and its open
//! gates are documented in `state-backend-zipf-generator-v1.md`.

use std::fmt::{Display, Formatter};

const MAX_DOMAIN: u64 = 2_147_483_647;
const MAX_ATTEMPTS: u8 = 64;

const Q: f64 = f64::from_bits(0x3fef_ae14_7ae1_47ae);
const ONE_MINUS_Q: f64 = f64::from_bits(0x3f84_7ae1_47ae_1480);
const TAYLOR_THRESHOLD: f64 = f64::from_bits(0x3e45_798e_e230_8c3a);
const ONE_HALF: f64 = f64::from_bits(0x3fe0_0000_0000_0000);
const ONE_THIRD: f64 = f64::from_bits(0x3fd5_5555_5555_5555);
const ONE_QUARTER: f64 = f64::from_bits(0x3fd0_0000_0000_0000);
const TWO_POW_MINUS_53: f64 = f64::from_bits(0x3ca0_0000_0000_0000);
const I64_UPPER_EXCLUSIVE: f64 = f64::from_bits(0x43e0_0000_0000_0000);
const ABS_MASK: u64 = 0x7fff_ffff_ffff_ffff;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ZipfCandidateError {
    InvalidDomain,
    InvalidSetup,
    InvalidSample,
    RejectionLimit,
}

impl Display for ZipfCandidateError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidDomain => "zipf_invalid_domain",
            Self::InvalidSetup => "zipf_invalid_setup",
            Self::InvalidSample => "zipf_invalid_sample",
            Self::RejectionLimit => "zipf_rejection_limit",
        })
    }
}

impl std::error::Error for ZipfCandidateError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ZipfSample {
    rank: u64,
    attempts: u8,
}

#[derive(Clone, Copy, Debug)]
struct ZipfCandidate {
    domain: u64,
    h_integral_x1: f64,
    h_integral_domain: f64,
    proposal_range: f64,
    squeeze: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AcceptancePath {
    Squeeze,
    Boundary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AcceptedProposal {
    rank: u64,
    path: AcceptancePath,
}

impl ZipfCandidate {
    fn new(domain: u64) -> Result<Self, ZipfCandidateError> {
        if !(1..=MAX_DOMAIN).contains(&domain) {
            return Err(ZipfCandidateError::InvalidDomain);
        }

        let domain_f64 = domain as f64;
        if domain_f64 as u64 != domain {
            return Err(ZipfCandidateError::InvalidDomain);
        }

        let h_integral_x1 = checked_setup(h_integral(ONE_HALF + 1.0)? - 1.0)?;
        let domain_endpoint = checked_setup(domain_f64 + ONE_HALF)?;
        let h_integral_domain = h_integral(domain_endpoint)?;
        let proposal_range = checked_setup(h_integral_x1 - h_integral_domain)?;

        let h_two = h(2.0)?;
        let h_integral_two_half = h_integral(2.0 + ONE_HALF)?;
        let inverse_input = checked_setup(h_integral_two_half - h_two)?;
        let inverse = h_integral_inverse(inverse_input)?;
        let squeeze = checked_setup(2.0 - inverse)?;

        if !(h_integral_x1 < h_integral_domain
            && proposal_range < 0.0
            && proposal_range.to_bits() != 0x8000_0000_0000_0000
            && squeeze > 0.0
            && squeeze < 1.0)
        {
            return Err(ZipfCandidateError::InvalidSetup);
        }

        Ok(Self {
            domain,
            h_integral_x1,
            h_integral_domain,
            proposal_range,
            squeeze,
        })
    }

    const fn domain(&self) -> u64 {
        self.domain
    }

    fn setup_bits(&self) -> [u64; 4] {
        [
            self.h_integral_x1.to_bits(),
            self.h_integral_domain.to_bits(),
            self.proposal_range.to_bits(),
            self.squeeze.to_bits(),
        ]
    }

    /// Evaluates one injected uniform word. `None` is an ordinary rejection.
    fn proposal_from_word(&self, word: u64) -> Result<Option<u64>, ZipfCandidateError> {
        Ok(self.evaluate_proposal(word)?.map(|proposal| proposal.rank))
    }

    fn evaluate_proposal(&self, word: u64) -> Result<Option<AcceptedProposal>, ZipfCandidateError> {
        let uniform_bits = word >> 11;
        let uniform_integer = uniform_bits as f64;
        if uniform_integer as u64 != uniform_bits {
            return Err(ZipfCandidateError::InvalidSample);
        }
        let uniform = checked_sample(uniform_integer * TWO_POW_MINUS_53)?;
        let scaled = checked_sample(uniform * self.proposal_range)?;
        let proposal = checked_sample(self.h_integral_domain + scaled)?;
        let inverse = h_integral_inverse(proposal).map_err(sample_error)?;
        let rank_one_based = round_and_clamp(inverse, self.domain)?;
        let rank_f64 = rank_one_based as f64;
        let distance = checked_sample(rank_f64 - inverse)?;

        let path = if distance <= self.squeeze {
            Some(AcceptancePath::Squeeze)
        } else {
            let right_endpoint = checked_sample(rank_f64 + ONE_HALF)?;
            let integrated = h_integral(right_endpoint).map_err(sample_error)?;
            let density = h(rank_f64).map_err(sample_error)?;
            let threshold = checked_sample(integrated - density)?;
            (proposal >= threshold).then_some(AcceptancePath::Boundary)
        };

        if let Some(path) = path {
            let rank =
                u64::try_from(rank_one_based - 1).map_err(|_| ZipfCandidateError::InvalidSample)?;
            Ok(Some(AcceptedProposal { rank, path }))
        } else {
            Ok(None)
        }
    }

    /// Samples from at most 64 directly addressed injected words.
    fn sample_with_words(
        &self,
        mut word_at: impl FnMut(u8) -> u64,
    ) -> Result<ZipfSample, ZipfCandidateError> {
        for attempt in 0..MAX_ATTEMPTS {
            if let Some(proposal) = self.evaluate_proposal(word_at(attempt))? {
                return Ok(ZipfSample {
                    rank: proposal.rank,
                    attempts: attempt + 1,
                });
            }
        }
        Err(ZipfCandidateError::RejectionLimit)
    }
}

fn round_and_clamp(inverse: f64, domain: u64) -> Result<i64, ZipfCandidateError> {
    if !(1..=MAX_DOMAIN).contains(&domain) {
        return Err(ZipfCandidateError::InvalidSample);
    }
    if !inverse.is_finite() || inverse < 0.0 {
        return Err(ZipfCandidateError::InvalidSample);
    }
    let rounded_input = checked_sample(inverse + ONE_HALF)?;
    if !(0.0..I64_UPPER_EXCLUSIVE).contains(&rounded_input) {
        return Err(ZipfCandidateError::InvalidSample);
    }
    let mut rank_one_based = rounded_input as i64;
    let domain_i64 = i64::try_from(domain).map_err(|_| ZipfCandidateError::InvalidSample)?;
    rank_one_based = rank_one_based.clamp(1, domain_i64);
    Ok(rank_one_based)
}

fn h_integral(x: f64) -> Result<f64, ZipfCandidateError> {
    let x = checked_setup(x)?;
    let log_x = checked_setup(libm::log(x))?;
    let scaled = checked_setup(ONE_MINUS_Q * log_x)?;
    let ratio = helper2(scaled)?;
    checked_setup(ratio * log_x)
}

fn h(x: f64) -> Result<f64, ZipfCandidateError> {
    let x = checked_setup(x)?;
    let log_x = checked_setup(libm::log(x))?;
    let scaled = checked_setup((-Q) * log_x)?;
    checked_setup(libm::exp(scaled))
}

fn h_integral_inverse(x: f64) -> Result<f64, ZipfCandidateError> {
    let x = checked_setup(x)?;
    let mut scaled = checked_setup(x * ONE_MINUS_Q)?;
    if scaled < -1.0 {
        scaled = -1.0;
    }
    let ratio = helper1(scaled)?;
    let exponent = checked_setup(ratio * x)?;
    checked_setup(libm::exp(exponent))
}

fn helper1(x: f64) -> Result<f64, ZipfCandidateError> {
    let x = checked_setup(x)?;
    if absolute(x) > TAYLOR_THRESHOLD {
        let numerator = checked_setup(libm::log1p(x))?;
        return checked_setup(numerator / x);
    }

    let a = checked_setup(ONE_QUARTER * x)?;
    let b = checked_setup(ONE_THIRD - a)?;
    let c = checked_setup(x * b)?;
    let d = checked_setup(ONE_HALF - c)?;
    let e = checked_setup(x * d)?;
    checked_setup(1.0 - e)
}

fn helper2(x: f64) -> Result<f64, ZipfCandidateError> {
    let x = checked_setup(x)?;
    if absolute(x) > TAYLOR_THRESHOLD {
        let numerator = checked_setup(libm::expm1(x))?;
        return checked_setup(numerator / x);
    }

    let a = checked_setup(ONE_QUARTER * x)?;
    let b = checked_setup(1.0 + a)?;
    let c = checked_setup(x * ONE_THIRD)?;
    let d = checked_setup(c * b)?;
    let e = checked_setup(1.0 + d)?;
    let f = checked_setup(x * ONE_HALF)?;
    let g = checked_setup(f * e)?;
    checked_setup(1.0 + g)
}

fn absolute(value: f64) -> f64 {
    f64::from_bits(value.to_bits() & ABS_MASK)
}

fn checked_setup(value: f64) -> Result<f64, ZipfCandidateError> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(ZipfCandidateError::InvalidSetup)
    }
}

fn checked_sample(value: f64) -> Result<f64, ZipfCandidateError> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(ZipfCandidateError::InvalidSample)
    }
}

fn sample_error(_: ZipfCandidateError) -> ZipfCandidateError {
    ZipfCandidateError::InvalidSample
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use sha2::{Digest, Sha256};

    use super::*;

    const LITERAL_CORPUS: &str =
        include_str!("../tests/fixtures/zipf-candidate-literals-v1.synthetic.json");
    const REJECTED_WORD_AT_MAX_DOMAIN: u64 = 0xf57a_1071_812f_af86;

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct LiteralCorpus {
        record_type: String,
        notice: String,
        corpus_class: String,
        fixture_ineligible: bool,
        qualification_eligible: bool,
        validation_authorizes_execution: bool,
        independently_generated: bool,
        provenance: String,
        setup_vectors: Vec<SetupVector>,
        proposal_vectors: Vec<ProposalVector>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SetupVector {
        domain: u64,
        bits: [String; 4],
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ProposalVector {
        domain: u64,
        word: String,
        rank: Option<u64>,
        acceptance_path: Option<String>,
    }

    fn literal_corpus() -> LiteralCorpus {
        assert_eq!(
            format!("{:x}", Sha256::digest(LITERAL_CORPUS.as_bytes())),
            "dd6c569cfef0a82627e280b4a0072b9a898f5467dc1ab07683c5ffeaf1c97c32"
        );
        let corpus: LiteralCorpus = serde_json::from_str(LITERAL_CORPUS).unwrap();
        assert_eq!(
            corpus.record_type,
            "state-backend-zipf-candidate-literal-corpus/v1"
        );
        assert_eq!(corpus.notice, "NOT QUALIFICATION EVIDENCE");
        assert_eq!(corpus.corpus_class, "provisional_feasibility");
        assert!(corpus.fixture_ineligible);
        assert!(!corpus.qualification_eligible);
        assert!(!corpus.validation_authorizes_execution);
        assert!(!corpus.independently_generated);
        assert_eq!(
            corpus.provenance,
            "transcribed_from_preexisting_cycle_8_implementation_tests"
        );
        assert_eq!(corpus.setup_vectors.len(), 7);
        assert_eq!(corpus.proposal_vectors.len(), 6);
        corpus
    }

    fn parse_hex_u64(value: &str) -> u64 {
        assert_eq!(value.len(), 16);
        assert!(value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
        u64::from_str_radix(value, 16).unwrap()
    }

    #[test]
    fn constants_match_the_provisional_contract() {
        assert_eq!(Q.to_bits(), 0x3fef_ae14_7ae1_47ae);
        assert_eq!(ONE_MINUS_Q.to_bits(), 0x3f84_7ae1_47ae_1480);
        assert_eq!(TAYLOR_THRESHOLD.to_bits(), 0x3e45_798e_e230_8c3a);
        assert_eq!(ONE_HALF.to_bits(), 0x3fe0_0000_0000_0000);
        assert_eq!(ONE_THIRD.to_bits(), 0x3fd5_5555_5555_5555);
        assert_eq!(ONE_QUARTER.to_bits(), 0x3fd0_0000_0000_0000);
        assert_eq!(TWO_POW_MINUS_53.to_bits(), 0x3ca0_0000_0000_0000);
    }

    #[test]
    fn rejects_domains_outside_the_candidate_boundary() {
        assert_eq!(
            ZipfCandidate::new(0).unwrap_err(),
            ZipfCandidateError::InvalidDomain
        );
        assert_eq!(
            ZipfCandidate::new(MAX_DOMAIN + 1).unwrap_err(),
            ZipfCandidateError::InvalidDomain
        );
        assert_eq!(
            ZipfCandidate::new(u64::MAX).unwrap_err(),
            ZipfCandidateError::InvalidDomain
        );
        assert_eq!(ZipfCandidate::new(1).unwrap().domain(), 1);
        assert_eq!(ZipfCandidate::new(MAX_DOMAIN).unwrap().domain(), MAX_DOMAIN);

        assert_eq!(
            ZipfCandidateError::InvalidDomain.to_string(),
            "zipf_invalid_domain"
        );
        assert_eq!(
            ZipfCandidateError::InvalidSetup.to_string(),
            "zipf_invalid_setup"
        );
        assert_eq!(
            ZipfCandidateError::InvalidSample.to_string(),
            "zipf_invalid_sample"
        );
        assert_eq!(
            ZipfCandidateError::RejectionLimit.to_string(),
            "zipf_rejection_limit"
        );
    }

    #[test]
    fn a_single_element_domain_always_returns_zero() {
        let sampler = ZipfCandidate::new(1).unwrap();
        for word in [0, 1, u64::MAX / 2, u64::MAX] {
            assert_eq!(sampler.proposal_from_word(word).unwrap(), Some(0));
        }
    }

    #[test]
    fn helper_branch_boundaries_have_literal_bits() {
        let next = f64::from_bits(TAYLOR_THRESHOLD.to_bits() + 1);
        let negative = f64::from_bits(TAYLOR_THRESHOLD.to_bits() | (1_u64 << 63));
        let negative_next = f64::from_bits(next.to_bits() | (1_u64 << 63));

        let vectors = [
            (0.0, 0x3ff0_0000_0000_0000, 0x3ff0_0000_0000_0000),
            (
                TAYLOR_THRESHOLD,
                0x3fef_ffff_fd50_ce24,
                0x3ff0_0000_0157_98ee,
            ),
            (next, 0x3fef_ffff_fd50_ce24, 0x3ff0_0000_0157_98ef),
            (negative, 0x3ff0_0000_0157_98ee, 0x3fef_ffff_fd50_ce24),
            (negative_next, 0x3ff0_0000_0157_98ef, 0x3fef_ffff_fd50_ce24),
        ];

        for (input, helper1_bits, helper2_bits) in vectors {
            assert_eq!(helper1(input).unwrap().to_bits(), helper1_bits);
            assert_eq!(helper2(input).unwrap().to_bits(), helper2_bits);
        }
        assert_eq!(
            helper1(f64::INFINITY).unwrap_err(),
            ZipfCandidateError::InvalidSetup
        );
        assert_eq!(
            helper2(f64::NEG_INFINITY).unwrap_err(),
            ZipfCandidateError::InvalidSetup
        );
    }

    #[test]
    fn detached_literal_setup_vectors_are_stable() {
        let corpus = literal_corpus();
        for vector in corpus.setup_vectors {
            let expected = vector.bits.map(|bits| parse_hex_u64(&bits));
            assert_eq!(
                ZipfCandidate::new(vector.domain).unwrap().setup_bits(),
                expected
            );
        }
    }

    #[test]
    fn detached_literal_words_cover_mapping_and_both_acceptance_paths() {
        let corpus = literal_corpus();
        let mut saw_squeeze = false;
        let mut saw_boundary = false;
        let mut saw_rejection = false;

        for vector in corpus.proposal_vectors {
            let sampler = ZipfCandidate::new(vector.domain).unwrap();
            let word = parse_hex_u64(&vector.word);
            assert_eq!(sampler.proposal_from_word(word).unwrap(), vector.rank);

            if let Some(expected_path) = vector.acceptance_path.as_deref() {
                let accepted = sampler.evaluate_proposal(word).unwrap().unwrap();
                match expected_path {
                    "squeeze" => {
                        assert_eq!(accepted.path, AcceptancePath::Squeeze);
                        saw_squeeze = true;
                    }
                    "boundary" => {
                        assert_eq!(accepted.path, AcceptancePath::Boundary);
                        saw_boundary = true;
                    }
                    unexpected => panic!("unexpected acceptance path: {unexpected}"),
                }
            } else if vector.rank.is_none() {
                saw_rejection = true;
            }
        }

        assert!(saw_squeeze);
        assert!(saw_boundary);
        assert!(saw_rejection);
    }

    #[test]
    fn injected_numerical_boundaries_are_fail_closed() {
        assert_eq!(round_and_clamp(0.0, 10).unwrap(), 1);
        assert_eq!(round_and_clamp(11.0, 10).unwrap(), 10);
        assert_eq!(
            round_and_clamp(-1.0, 10).unwrap_err(),
            ZipfCandidateError::InvalidSample
        );
        assert_eq!(
            round_and_clamp(-0.25, 10).unwrap_err(),
            ZipfCandidateError::InvalidSample
        );
        assert_eq!(
            round_and_clamp(-0.5, 10).unwrap_err(),
            ZipfCandidateError::InvalidSample
        );
        assert_eq!(
            round_and_clamp(f64::from_bits(0x8000_0000_0000_0001), 10).unwrap_err(),
            ZipfCandidateError::InvalidSample
        );
        assert_eq!(
            round_and_clamp(f64::INFINITY, 10).unwrap_err(),
            ZipfCandidateError::InvalidSample
        );
        assert_eq!(
            round_and_clamp(1.0, 0).unwrap_err(),
            ZipfCandidateError::InvalidSample
        );

        let mut corrupted = ZipfCandidate::new(10).unwrap();
        corrupted.proposal_range = f64::INFINITY;
        assert_eq!(
            corrupted.proposal_from_word(0).unwrap_err(),
            ZipfCandidateError::InvalidSample
        );
    }

    #[test]
    fn injected_rejection_is_bounded_and_never_falls_back() {
        let sampler = ZipfCandidate::new(MAX_DOMAIN).unwrap();
        let mut calls = Vec::new();
        let sample = sampler
            .sample_with_words(|attempt| {
                calls.push(attempt);
                if attempt == 0 {
                    REJECTED_WORD_AT_MAX_DOMAIN
                } else {
                    0
                }
            })
            .unwrap();
        assert_eq!(calls, [0, 1]);
        assert_eq!(sample.rank, MAX_DOMAIN - 1);
        assert_eq!(sample.attempts, 2);

        let mut cap_calls = Vec::new();
        assert_eq!(
            sampler
                .sample_with_words(|attempt| {
                    cap_calls.push(attempt);
                    REJECTED_WORD_AT_MAX_DOMAIN
                })
                .unwrap_err(),
            ZipfCandidateError::RejectionLimit
        );
        assert_eq!(cap_calls.len(), usize::from(MAX_ATTEMPTS));
        assert_eq!(cap_calls.first(), Some(&0));
        assert_eq!(cap_calls.last(), Some(&(MAX_ATTEMPTS - 1)));
    }

    #[test]
    fn provisional_stream_digest_is_stable() {
        let sampler = ZipfCandidate::new(1_288_490_188).unwrap();
        let mut digest = Sha256::new();
        let mut total_attempts = 0_u64;

        for ordinal in 0..100_000_u64 {
            let sample = sampler
                .sample_with_words(|attempt| {
                    ordinal
                        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                        .wrapping_add(u64::from(attempt).wrapping_mul(0xd1b5_4a32_d192_ed03))
                })
                .unwrap();
            digest.update(sample.rank.to_be_bytes());
            digest.update([sample.attempts]);
            total_attempts += u64::from(sample.attempts);
        }

        assert_eq!(total_attempts, 100_071);
        assert_eq!(
            format!("{:x}", digest.finalize()),
            "df676224a5883569f24e3489b38946b7bca630b153d575f6c2fcc67fa23a8ed7"
        );
    }
}
