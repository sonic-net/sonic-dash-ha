fn main() -> Result<(), Box<dyn std::error::Error>> {
    // skiping serializing epoch field in SwbusMessageHeader and nh_id in RouteQueryResultEntry for testing because they are not deterministic.
    let builder = tonic_build::configure()
        .enum_attribute("swbus.SwbusErrorCode", "#[derive(strum::Display)]")
        .enum_attribute("swbus.Scope", "#[derive(strum::Display)]")
        .message_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .enum_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .message_attribute("swbus.ServicePath", "#[derive(Eq, Hash, Ord, PartialOrd)]")
        .field_attribute("swbus.SwbusMessageHeader.epoch", "#[serde(default, skip_serializing)]")
        .field_attribute(
            "swbus.RouteQueryResultEntry.nh_id",
            "#[serde(default, skip_serializing)]",
        )
        .field_attribute(
            "swbus.RouteQueryResult.entries",
            "#[serde(serialize_with = \"sorted_vec_serializer\")]",
        )
        .field_attribute(
            "swbus.SwbusMessageHeader.source",
            "#[serde(serialize_with = \"serialize_service_path_opt\",deserialize_with = \"deserialize_service_path_opt\")]",
        )
        .field_attribute(
            "swbus.SwbusMessageHeader.destination",
            "#[serde(serialize_with = \"serialize_service_path_opt\",deserialize_with = \"deserialize_service_path_opt\")]",
        )
        .field_attribute(
            "swbus.RouteQueryResultEntry.service_path",
            "#[serde(serialize_with = \"serialize_service_path_opt\",deserialize_with = \"deserialize_service_path_opt\")]",
        )
        .field_attribute(
            "swbus.RouteQueryResultEntry.nh_service_path",
            "#[serde(serialize_with = \"serialize_service_path_opt\",deserialize_with = \"deserialize_service_path_opt\")]",
        );

    let includes: &[&str] = &[];
    builder.compile(&["proto/swbus.proto"], includes)?;

    Ok(())
}
