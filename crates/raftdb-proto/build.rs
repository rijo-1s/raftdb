fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        // Older protoc (pre-3.15, e.g. Ubuntu 22.04's default 3.12.4)
        // requires this flag to allow the `optional` keyword used on
        // leader_hint fields in raftdb.proto. Harmless no-op on newer
        // protoc where it's already the default.
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["proto/raftdb.proto"], &["proto"])?;
    Ok(())
}
