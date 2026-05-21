use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("kv_descriptor.bin"))
        .compile_protos(&["proto/kv.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/kv.proto");
    Ok(())
}
