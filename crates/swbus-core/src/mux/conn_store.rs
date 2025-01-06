use crate::mux::conn::SwbusConn;
use crate::mux::route_config::{PeerConfig, RouteConfig};
use crate::mux::SwbusConnInfo;
use crate::mux::SwbusConnMode;
use crate::mux::SwbusMultiplexer;
use dashmap::{DashMap, DashSet};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{error, info};

#[derive(Debug)]
enum ConnTracker {
    SwbusConn(SwbusConn),
    Task(JoinHandle<()>),
}

pub struct SwbusConnStore {
    mux: Arc<SwbusMultiplexer>,
    connections: DashMap<Arc<SwbusConnInfo>, ConnTracker>,
    my_routes: DashSet<RouteConfig>,
}

impl SwbusConnStore {
    pub fn new(mux: Arc<SwbusMultiplexer>) -> Self {
        SwbusConnStore {
            mux,
            connections: DashMap::new(),
            my_routes: DashSet::new(),
        }
    }

    fn start_connect_task(self: &Arc<SwbusConnStore>, conn_info: Arc<SwbusConnInfo>, reconnect: bool) {
        let conn_info_clone = conn_info.clone();

        let retry_interval = match reconnect {
            true => Duration::from_millis(1),
            false => Duration::from_secs(1),
        };
        let mux_clone = self.mux.clone();
        let conn_store = self.clone();
        let retry_task: JoinHandle<()> = tokio::spawn(async move {
            loop {
                match SwbusConn::connect(conn_info.clone(), mux_clone.clone(), conn_store.clone()).await {
                    Ok(conn) => {
                        info!("Successfully connect to peer {}", conn_info.id());
                        // register the new connection and update the route table
                        conn_store.conn_established(conn);
                        break;
                    }
                    Err(_) => {
                        tokio::time::sleep(retry_interval).await;
                    }
                }
            }
        });
        self.connections.insert(conn_info_clone, ConnTracker::Task(retry_task));
    }

    pub fn add_my_route(&self, my_route: RouteConfig) {
        self.my_routes.insert(my_route);
    }

    pub fn add_peer(self: &Arc<SwbusConnStore>, peer: PeerConfig) {
        // todo: assuming only one route for now. Will be improved to send routes in route update message and remove this
        let my_route = self.my_routes.iter().next().expect("My service path is not set");
        let conn_info = Arc::new(SwbusConnInfo::new_client(
            peer.conn_type,
            peer.endpoint,
            peer.id.clone(),
            my_route.key.clone(),
        ));
        self.start_connect_task(conn_info, false);
    }

    pub fn conn_lost(self: &Arc<SwbusConnStore>, conn_info: Arc<SwbusConnInfo>) {
        // First, we remove the connection from the connection table.
        self.connections.remove(&conn_info);

        // If connection is client mode, we start a new connection task.
        if conn_info.mode() == SwbusConnMode::Client {
            self.start_connect_task(conn_info, true /*reconnect from connection loss*/);
        }
    }

    pub fn conn_established(&self, conn: SwbusConn) {
        self.mux.register(conn.info(), conn.new_proxy());
        self.connections
            .insert(conn.info().clone(), ConnTracker::SwbusConn(conn));
    }

    pub async fn shutdown(&self) {
        while let Some(entry) = self.connections.iter().next() {
            match entry.value() {
                ConnTracker::SwbusConn(conn) => {
                    if let Err(swbus_err) = conn.shutdown().await {
                        error!("Failed to shutdown connection: {:?}", swbus_err);
                    }
                }
                ConnTracker::Task(task) => {
                    task.abort();
                }
            }
            self.connections.remove(entry.key());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use swbus_proto::swbus::ConnectionType;
    use swbus_proto::swbus::RouteScope;
    use swbus_proto::swbus::ServicePath;
    use tokio::sync::mpsc;
    #[tokio::test]
    async fn test_add_peer() {
        let mux = Arc::new(SwbusMultiplexer::new());
        let conn_store = Arc::new(SwbusConnStore::new(mux.clone()));
        let peer_config = PeerConfig {
            conn_type: ConnectionType::Local,
            endpoint: "127.0.0.1:8080".to_string().parse().unwrap(),
            id: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
        };
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: RouteScope::Cluster,
        };
        conn_store.add_my_route(route_config);

        conn_store.add_peer(peer_config);

        assert!(conn_store.connections.iter().any(|entry| {
            entry.key().id() == "swbs-to://127.0.0.1:8080" && matches!(entry.value(), ConnTracker::Task(_))
        }));
    }

    #[tokio::test]
    async fn test_add_my_route() {
        let mux = Arc::new(SwbusMultiplexer::new());
        let conn_store = Arc::new(SwbusConnStore::new(mux.clone()));
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: RouteScope::Cluster,
        };

        conn_store.add_my_route(route_config.clone());

        assert!(conn_store.my_routes.contains(&route_config));
    }

    #[tokio::test]
    async fn test_conn_lost() {
        let mux = Arc::new(SwbusMultiplexer::new());
        let conn_store = Arc::new(SwbusConnStore::new(mux.clone()));
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: RouteScope::Cluster,
        };
        conn_store.add_my_route(route_config);

        let conn_info = Arc::new(SwbusConnInfo::new_client(
            ConnectionType::Cluster,
            "127.0.0.1:8080".parse().unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.1-dpu0").unwrap(),
        ));
        conn_store.conn_lost(conn_info.clone());

        assert!(conn_store.connections.iter().any(|entry| {
            entry.key().id() == "swbs-to://127.0.0.1:8080" && matches!(entry.value(), ConnTracker::Task(_))
        }));
    }

    #[tokio::test]
    async fn test_conn_established() {
        let mux = Arc::new(SwbusMultiplexer::new());
        let conn_store = Arc::new(SwbusConnStore::new(mux.clone()));
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: RouteScope::Cluster,
        };
        conn_store.add_my_route(route_config);

        let conn_info = Arc::new(SwbusConnInfo::new_client(
            ConnectionType::Cluster,
            "127.0.0.1:8080".parse().unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.1-dpu0").unwrap(),
        ));
        let (message_queue_tx, _) = mpsc::channel(16);
        let conn = SwbusConn::new(&conn_info, message_queue_tx);
        conn_store.conn_established(conn);

        assert!(conn_store
            .connections
            .iter()
            .any(|entry| entry.key().id() == conn_info.id() && matches!(entry.value(), ConnTracker::SwbusConn(_))));
    }
}
