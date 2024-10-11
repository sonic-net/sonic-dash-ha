fn main() -> Result<(), Box<dyn std::error::Error>> {
    let builder = tonic_build::configure().message_attribute("swbus.ServicePath", "#[derive(Eq, Hash)]");

    let includes: &[&str] = &[];
    builder.compile(&["proto/swbus.proto"], includes)?;

    Ok(())
}
