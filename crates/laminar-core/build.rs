fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "delta")]
    {
        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .compile_protos(&["proto/laminar.proto"], &["proto"])?;
    }
    Ok(())
}
