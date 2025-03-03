use serde::Deserialize;
use std::net::SocketAddr;
use swbus_proto::swbus::*;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SwbusConfig {
    pub endpoint: SocketAddr,
    pub routes: Vec<RouteConfig>,
    pub peers: Vec<PeerConfig>,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, Deserialize)]
pub struct RouteConfig {
    #[serde(deserialize_with = "deserialize_service_path")]
    pub key: ServicePath,
    pub scope: RouteScope,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PeerConfig {
    #[serde(deserialize_with = "deserialize_service_path")]
    pub id: ServicePath,
    pub endpoint: SocketAddr,
    pub conn_type: ConnectionType,
}
