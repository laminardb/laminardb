use super::*;

fn tls(seed: u8) -> ClusterTls {
    ClusterTls::from_pem(
        &[b'c', seed],
        &[b'k', seed],
        &[b'a', seed],
        &format!("cluster-{seed}"),
    )
}

#[test]
fn install_before_use_freezes_tls_and_identical_repeat_is_idempotent() {
    let state = ClusterTlsState::new();
    state.install(tls(1)).unwrap();
    assert!(state.transport_tls().is_some());
    state.install(tls(1)).unwrap();
}

#[test]
fn conflicting_repeat_is_rejected() {
    let state = ClusterTlsState::new();
    state.install(tls(1)).unwrap();
    let error = state.install(tls(2)).unwrap_err();
    assert!(error.contains("different cluster TLS material"), "{error}");
    assert!(state.transport_tls().is_some());
}

#[test]
fn plaintext_claim_is_idempotent_and_rejects_late_tls_install() {
    let state = ClusterTlsState::new();
    state.claim_plaintext().unwrap();
    state.claim_plaintext().unwrap();
    assert!(state.transport_tls().is_none());
    let error = state.install(tls(1)).unwrap_err();
    assert!(error.contains("after plaintext was selected"), "{error}");
    assert!(state.transport_tls().is_none());
}

#[test]
fn plaintext_claim_rejects_tls_installing_or_installed() {
    let installing = ClusterTlsState::new();
    installing
        .mode
        .store(TRANSPORT_TLS_INSTALLING, Ordering::Release);
    let error = installing.claim_plaintext().unwrap_err();
    assert!(error.contains("TLS installation has begun"), "{error}");

    let installed = ClusterTlsState::new();
    installed.install(tls(1)).unwrap();
    let error = installed.claim_plaintext().unwrap_err();
    assert!(error.contains("TLS installation has begun"), "{error}");
    assert!(installed.transport_tls().is_some());
}

#[test]
fn plaintext_transport_use_rejects_late_tls_install() {
    let state = ClusterTlsState::new();
    assert!(state.transport_tls().is_none());
    let error = state.install(tls(1)).unwrap_err();
    assert!(error.contains("after plaintext was selected"), "{error}");
}

#[test]
fn material_fingerprint_is_length_framed() {
    let left = tls_material_fingerprint(b"ab", b"c", b"d", "e");
    let right = tls_material_fingerprint(b"a", b"bc", b"d", "e");
    assert_ne!(left, right);
}
