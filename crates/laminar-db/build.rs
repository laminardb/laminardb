fn main() {
    if std::env::var_os("CARGO_FEATURE_PROCESS_REMOTE").is_none() {
        return;
    }
    println!("cargo:rerun-if-changed=proto/process_worker.proto");
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc binary");
    std::env::set_var("PROTOC", protoc);
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/process_worker.proto"], &["proto"])
        .expect("failed to compile process worker proto");
}
