fn main() -> Result<(), Box<dyn std::error::Error>> {
    let builder = tonic_build::configure()
        .enum_attribute("swbus.SwbusErrorCode", "#[derive(strum::Display)]")
        .enum_attribute("swbus.ConnectionType", "#[derive(strum::Display)]")
        .message_attribute("swbus.ServicePath", "#[derive(Eq, Hash)]");

    builder.compile(&["proto/swbus.proto"], &["swbus"])?;

    Ok(())
}
