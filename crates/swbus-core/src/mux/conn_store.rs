use crate::mux::conn::SwbusConn;
use crate::mux::SwbusConnInfo;
use crate::mux::SwbusConnMode;
use crate::mux::SwbusMultiplexer;
use dashmap::DashMap;
use std::sync::Arc;
use swbus_config::PeerConfig;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::*;

#[derive(Debug)]
enum ConnTracker {
    SwbusConn(SwbusConn),
    Task(CancellationToken),
}

pub struct SwbusConnStore {
    mux: Arc<SwbusMultiplexer>,
    connections: DashMap<String, ConnTracker>,
}

impl SwbusConnStore {
    pub fn new(mux: Arc<SwbusMultiplexer>) -> Self {
        SwbusConnStore {
            mux,
            connections: DashMap::new(),
        }
    }

    #[instrument(skip(self, conn_info), fields(conn_id=conn_info.id()))]
    fn start_connect_task(self: &Arc<SwbusConnStore>, conn_info: &SwbusConnInfo, reconnect: bool) {
        info!("Starting connection task to the peer");
        let retry_interval = match reconnect {
            true => Duration::from_millis(1),
            false => Duration::from_secs(1),
        };
        let mux_clone = self.mux.clone();
        let conn_store = self.clone();
        let current_span = Span::current();

        let token = CancellationToken::new();
        let child_token = token.clone();
        let conn_info_clone = conn_info.clone();
        tokio::spawn(
            async move {
                loop {
                    if child_token.is_cancelled() {
                        return;
                    }
                    match SwbusConn::connect(&conn_info_clone, mux_clone.clone(), conn_store.clone()).await {
                        Ok(conn) => {
                            info!("Successfully connect to the peer");
                            // register the new connection and update the route table
                            conn_store.conn_established(conn);
                            return;
                        }
                        Err(_) => {
                            tokio::time::sleep(retry_interval).await;
                        }
                    };
                }
            }
            .instrument(current_span.clone()),
        );
        self.connections
            .insert(conn_info.id().to_string(), ConnTracker::Task(token));
    }

    pub fn add_peer(self: &Arc<SwbusConnStore>, peer: PeerConfig) {
        let conn_info = SwbusConnInfo::new_client(peer.conn_type, peer.endpoint);
        self.start_connect_task(&conn_info, false);
    }

    pub fn conn_lost(self: &Arc<SwbusConnStore>, conn_info: &SwbusConnInfo) {
        // First, we remove the connection from the connection table.
        self.connections.remove(conn_info.id());

        // If connection is client mode, we start a new connection task.
        if conn_info.mode() == SwbusConnMode::Client {
            self.start_connect_task(conn_info, true /*reconnect from connection loss*/);
        }
    }

    pub fn conn_established(&self, conn: SwbusConn) {
        self.mux.register(conn.info(), conn.new_proxy());
        self.connections
            .insert(conn.info().id().to_string(), ConnTracker::SwbusConn(conn));
    }

    pub async fn shutdown(&self) {
        for entry in self.connections.iter() {
            match entry.value() {
                ConnTracker::SwbusConn(conn) => {
                    debug!("shutting down connection: {:?}", conn.info());
                    if let Err(swbus_err) = conn.shutdown().await {
                        error!("Failed to shutdown connection: {:?}", swbus_err);
                    }
                    debug!("Connection is shutdown: {:?}", conn.info());
                }
                ConnTracker::Task(task) => {
                    task.cancel();
                }
            }
        }
        self.connections.clear();
        info!("All connections and reconnect tasks are stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use swbus_config::RouteConfig;
    use swbus_proto::swbus::ConnectionType;
    use swbus_proto::swbus::RouteScope;
    use swbus_proto::swbus::ServicePath;
    use tokio::sync::mpsc;
    #[tokio::test]
    async fn test_add_peer() {
        let peer_config = PeerConfig {
            conn_type: ConnectionType::InNode,
            endpoint: "127.0.0.1:8080".to_string().parse().unwrap(),
        };
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: RouteScope::InCluster,
        };

        let mux = Arc::new(SwbusMultiplexer::new(vec![route_config]));
        let conn_store = Arc::new(SwbusConnStore::new(mux.clone()));

        conn_store.add_peer(peer_config);

        assert!(conn_store.connections.iter().any(|entry| {
            entry.key() == "swbs-to://127.0.0.1:8080" && matches!(entry.value(), ConnTracker::Task(_))
        }));
    }

    #[tokio::test]
    async fn test_conn_lost() {
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: RouteScope::InCluster,
        };
        let mux = Arc::new(SwbusMultiplexer::new(vec![route_config]));
        let conn_store = Arc::new(SwbusConnStore::new(mux.clone()));

        let conn_info = SwbusConnInfo::new_client(ConnectionType::InCluster, "127.0.0.1:8080".parse().unwrap());
        conn_store.conn_lost(&conn_info);

        assert!(conn_store.connections.iter().any(|entry| {
            entry.key() == "swbs-to://127.0.0.1:8080" && matches!(entry.value(), ConnTracker::Task(_))
        }));
    }

    #[tokio::test]
    async fn test_conn_established() {
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: RouteScope::InCluster,
        };

        let mux = Arc::new(SwbusMultiplexer::new(vec![route_config]));
        let conn_store = Arc::new(SwbusConnStore::new(mux.clone()));

        let mut conn_info = SwbusConnInfo::new_client(ConnectionType::InCluster, "127.0.0.1:8080".parse().unwrap());
        conn_info =
            conn_info.with_remote_service_path(ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap());
        let conn_info = Arc::new(conn_info);
        let (send_queue_tx, _) = mpsc::channel(16);
        let conn = SwbusConn::new(&conn_info, send_queue_tx);
        conn_store.conn_established(conn);

        assert!(conn_store
            .connections
            .iter()
            .any(|entry| entry.key() == conn_info.id() && matches!(entry.value(), ConnTracker::SwbusConn(_))));
    }
}
