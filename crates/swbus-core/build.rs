fn main() -> Result<(), Box<dyn std::error::Error>> {
    let builder = tonic_build::configure().message_attribute("swbus.ServicePath", "#[derive(Eq, Hash)]");

    builder.compile(&["proto/swbus.proto"], &["swbus"])?;

    Ok(())
}
