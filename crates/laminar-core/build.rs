fn main() {
    #[cfg(feature = "constellation")]
    {
        // Use local protoc if system protoc is not available
        if std::env::var("PROTOC").is_err() {
            let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
            let protoc_path = std::path::Path::new(&manifest_dir)
                .join("..")
                .join("..")
                .join("tools")
                .join("protoc")
                .join("bin")
                .join(if cfg!(target_os = "windows") {
                    "protoc.exe"
                } else {
                    "protoc"
                });
            if protoc_path.exists() {
                std::env::set_var("PROTOC", protoc_path);
            }
        }

        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .compile_protos(
                &["proto/constellation.proto"],
                &["proto/"],
            )
            .expect("Failed to compile constellation proto files");
    }
}
