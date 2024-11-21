use getset::{CopyGetters, Getters};
use serde::Serialize;
use serde_json;
use std::net::SocketAddr;
use swbus_proto::swbus::Scope;
use swbus_proto::swbus::ServicePath;

#[derive(Debug, Copy, Clone, strum::Display, Eq, PartialEq, Hash, Serialize)]
pub enum SwbusConnMode {
    Client,
    Server,
}

#[derive(Debug, Clone, Getters, CopyGetters, Eq, PartialEq, Hash, Serialize)]
pub struct SwbusConnInfo {
    #[getset(get = "pub")]
    id: String,

    #[getset(get_copy = "pub")]
    mode: SwbusConnMode,

    #[getset(get_copy = "pub")]
    remote_addr: SocketAddr,

    #[getset(get_copy = "pub")]
    connection_type: Scope,

    // Local service path is only used for client mode to send my route to the server
    // this will be removed when we implement route update
    local_service_path: Option<ServicePath>,

    remote_service_path: ServicePath,
}

impl SwbusConnInfo {
    pub fn new_client(
        conn_type: Scope,
        remote_addr: SocketAddr,
        remote_service_path: ServicePath,
        local_service_path: ServicePath,
    ) -> SwbusConnInfo {
        SwbusConnInfo {
            id: format!("swbs-to://{}:{}", remote_addr.ip(), remote_addr.port()),
            mode: SwbusConnMode::Client,
            remote_addr,
            connection_type: conn_type,
            local_service_path: Some(local_service_path),
            remote_service_path,
        }
    }

    pub fn new_server(conn_type: Scope, remote_addr: SocketAddr, remote_service_path: ServicePath) -> SwbusConnInfo {
        SwbusConnInfo {
            id: format!("swbs-from://{}:{}", remote_addr.ip(), remote_addr.port()),
            mode: SwbusConnMode::Server,
            remote_addr,
            connection_type: conn_type,
            local_service_path: None,
            remote_service_path,
        }
    }

    pub fn remote_service_path(&self) -> &ServicePath {
        &self.remote_service_path
    }

    pub fn local_service_path(&self) -> Option<&ServicePath> {
        self.local_service_path.as_ref()
    }
}
