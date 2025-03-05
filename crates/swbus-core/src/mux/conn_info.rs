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

    #[getset(get = "pub")]
    remote_service_path: ServicePath,
}

impl SwbusConnInfo {
    pub fn new_client(
        conn_type: ConnectionType,
        remote_addr: SocketAddr,
        remote_service_path: ServicePath,
    ) -> SwbusConnInfo {
        SwbusConnInfo {
            id: format!("swbs-to://{}:{}", remote_addr.ip(), remote_addr.port()),
            mode: SwbusConnMode::Client,
            remote_addr,
            connection_type: conn_type,
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
            remote_service_path,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn new_client_conn_info_should_succeed() {
        let remote_addr = "127.0.0.1:8080".parse().unwrap();
        let remote_service_path = ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap();
        let conn_info = SwbusConnInfo::new_client(ConnectionType::InCluster, remote_addr, remote_service_path.clone());

        assert_eq!(conn_info.id(), "swbs-to://127.0.0.1:8080");
        assert_eq!(conn_info.mode(), SwbusConnMode::Client);
        assert_eq!(conn_info.remote_addr(), remote_addr);
        assert_eq!(conn_info.connection_type(), ConnectionType::InCluster);
        assert_eq!(conn_info.remote_service_path(), &remote_service_path);
    }

    #[test]
    fn new_server_conn_info_should_succeed() {
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let remote_service_path = ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap();
        let conn_info = SwbusConnInfo::new_server(ConnectionType::InRegion, remote_addr, remote_service_path.clone());

        assert_eq!(conn_info.id(), "swbs-from://127.0.0.1:8080");
        assert_eq!(conn_info.mode(), SwbusConnMode::Server);
        assert_eq!(conn_info.remote_addr(), remote_addr);
        assert_eq!(conn_info.connection_type(), ConnectionType::InRegion);
        assert_eq!(conn_info.remote_service_path(), &remote_service_path);
    }
}
