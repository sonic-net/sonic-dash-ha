use getset::{CopyGetters, Getters};
use std::net::SocketAddr;
use swbus_proto::swbus::RouteScope;

#[derive(Debug, Copy, Clone, strum::Display, Eq, PartialEq, Hash)]
pub enum SwbusConnMode {
    Client,
    Server,
}

#[derive(Debug, Clone, Getters, CopyGetters, Eq, PartialEq, Hash)]
pub struct SwbusConnInfo {
    #[getset(get = "pub")]
    id: String,

    #[getset(get_copy = "pub")]
    mode: SwbusConnMode,

    #[getset(get_copy = "pub")]
    remote_addr: SocketAddr,

    #[getset(get_copy = "pub")]
    connection_type: RouteScope,
}

impl SwbusConnInfo {
    pub fn new_client(conn_type: RouteScope, remote_addr: SocketAddr) -> SwbusConnInfo {
        SwbusConnInfo {
            id: format!("swbs-to://{}:{}", remote_addr.ip(), remote_addr.port()),
            mode: SwbusConnMode::Client,
            remote_addr,
            connection_type: conn_type,
        }
    }

    pub fn new_server(conn_type: RouteScope, remote_addr: SocketAddr) -> SwbusConnInfo {
        SwbusConnInfo {
            id: format!("swbs-from://{}:{}", remote_addr.ip(), remote_addr.port()),
            mode: SwbusConnMode::Server,
            remote_addr,
            connection_type: conn_type,
        }
    }
}
