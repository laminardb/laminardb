fn main() {
    // The cross-node gRPC stubs (shuffle transport + barrier sync) are only
    // needed for the `cluster-unstable` feature, so a default build pulls in
    // neither protoc nor the generated code. Build scripts don't receive
    // `cfg(feature = "...")`; Cargo instead exposes activated features via
    // `CARGO_FEATURE_<NAME>` env vars, so gate on that.
    if std::env::var_os("CARGO_FEATURE_CLUSTER_UNSTABLE").is_none() {
        return;
    }

    println!("cargo:rerun-if-changed=proto/shuffle.proto");
    println!("cargo:rerun-if-changed=proto/barrier.proto");

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/shuffle.proto", "proto/barrier.proto"], &["proto"])
        .expect("failed to compile cluster proto definitions");
}
