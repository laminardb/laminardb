fn main() {
    // The cross-node gRPC stubs (shuffle transport + barrier sync) are only
    // needed for the `cluster` feature, so a default build pulls in
    // neither protoc nor the generated code. Build scripts don't receive
    // `cfg(feature = "...")`; Cargo instead exposes activated features via
    // `CARGO_FEATURE_<NAME>` env vars, so gate on that.
    if std::env::var_os("CARGO_FEATURE_CLUSTER").is_none() {
        return;
    }

    println!("cargo:rerun-if-changed=proto/shuffle.proto");
    println!("cargo:rerun-if-changed=proto/barrier.proto");
    println!("cargo:rerun-if-changed=proto/query.proto");

    // Use the vendored protoc so codegen doesn't depend on the host's protoc:
    // stock distro versions (e.g. Ubuntu 22.04's 3.12) predate proto3 `optional`
    // (protoc 3.15) and reject barrier.proto.
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc binary");
    std::env::set_var("PROTOC", protoc);

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(
            &[
                "proto/shuffle.proto",
                "proto/barrier.proto",
                "proto/query.proto",
            ],
            &["proto"],
        )
        .expect("failed to compile cluster proto definitions");
}
