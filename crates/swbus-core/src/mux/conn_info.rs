use getset::{CopyGetters, Getters};
use std::net::SocketAddr;
use swbus_proto::swbus::ConnectionType;
use swbus_proto::swbus::ServicePath;

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
    connection_type: ConnectionType,

    // Local service path is only used for client mode to send my route to the server
    // this will be removed when we implement route update
    local_service_path: Option<ServicePath>,

    remote_service_path: ServicePath,
}

impl SwbusConnInfo {
    pub fn new_client(
        conn_type: ConnectionType,
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

    pub fn new_server(
        conn_type: ConnectionType,
        remote_addr: SocketAddr,
        remote_service_path: ServicePath,
    ) -> SwbusConnInfo {
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
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_new_client() {
        let remote_addr = "127.0.0.1:8080".parse().unwrap();
        let remote_service_path = ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap();
        let local_service_path = ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap();
        let conn_info = SwbusConnInfo::new_client(
            ConnectionType::Cluster,
            remote_addr,
            remote_service_path.clone(),
            local_service_path.clone(),
        );

        assert_eq!(conn_info.id(), "swbs-to://127.0.0.1:8080");
        assert_eq!(conn_info.mode(), SwbusConnMode::Client);
        assert_eq!(conn_info.remote_addr(), remote_addr);
        assert_eq!(conn_info.connection_type(), ConnectionType::Cluster);
        assert_eq!(conn_info.remote_service_path(), &remote_service_path);
        assert_eq!(conn_info.local_service_path(), Some(&local_service_path));
    }

    #[test]
    fn test_new_server() {
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let remote_service_path = ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap();
        let conn_info = SwbusConnInfo::new_server(ConnectionType::Region, remote_addr, remote_service_path.clone());

        assert_eq!(conn_info.id(), "swbs-from://127.0.0.1:8080");
        assert_eq!(conn_info.mode(), SwbusConnMode::Server);
        assert_eq!(conn_info.remote_addr(), remote_addr);
        assert_eq!(conn_info.connection_type(), ConnectionType::Region);
        assert_eq!(conn_info.remote_service_path(), &remote_service_path);
        assert_eq!(conn_info.local_service_path(), None);
    }
}
