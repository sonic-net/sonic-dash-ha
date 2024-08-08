use crate::contracts::swbus::ConnectionType;
use getset::{CopyGetters, Getters};
use std::net::SocketAddr;

#[derive(Debug, Copy, Clone, strum::Display)]
pub enum SwbusConnMode {
    Client,
    Server,
}

#[derive(Debug, Clone, Getters, CopyGetters)]
pub struct SwbusConnInfo {
    #[getset(get = "pub")]
    id: String,

    #[getset(get_copy = "pub")]
    mode: SwbusConnMode,

    #[getset(get_copy = "pub")]
    remote_addr: SocketAddr,

    #[getset(get_copy = "pub")]
    connection_type: ConnectionType,
}

impl SwbusConnInfo {
    pub fn new_client(conn_type: ConnectionType, remote_addr: SocketAddr) -> SwbusConnInfo {
        SwbusConnInfo {
            id: format!("to-{}:{}", remote_addr.ip(), remote_addr.port()),
            mode: SwbusConnMode::Client,
            remote_addr,
            connection_type: conn_type,
        }
    }

    pub fn server(conn_type: ConnectionType, remote_addr: SocketAddr) -> SwbusConnInfo {
        SwbusConnInfo {
            id: format!("from-{}:{}", remote_addr.ip(), remote_addr.port()),
            mode: SwbusConnMode::Server,
            remote_addr,
            connection_type: conn_type,
        }
    }
}
