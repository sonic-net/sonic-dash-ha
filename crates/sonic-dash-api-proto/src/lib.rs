pub mod ha_scope_config {
    include!(concat!(env!("OUT_DIR"), "/dash.ha_scope_config.rs"));
}

pub mod ha_set_config {
    include!(concat!(env!("OUT_DIR"), "/dash.ha_set_config.rs"));
}

pub mod types {
    include!(concat!(env!("OUT_DIR"), "/dash.types.rs"));
}
