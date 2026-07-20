use crate::mux::conn::SwbusConn;
use crate::mux::SwbusConnInfo;
use crate::mux::SwbusConnMode;
use crate::mux::SwbusMultiplexer;
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::Mutex;
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
    // Keep peer replacement and registration atomic across concurrent connection handshakes.
    connection_update_lock: Mutex<()>,
}

impl SwbusConnStore {
    pub fn new(mux: Arc<SwbusMultiplexer>) -> Self {
        SwbusConnStore {
            mux,
            connections: DashMap::new(),
            connection_update_lock: Mutex::new(()),
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
        // A single critical section prevents two reconnects from each preserving the other as stale.
        let _guard = self.connection_update_lock.lock().unwrap();
        self.replace_stale_server_connections(conn.info());
        self.mux.register(conn.info(), conn.new_proxy());
        self.connections
            .insert(conn.info().id().to_string(), ConnTracker::SwbusConn(conn));
    }

    /// Remove older accepted connections when the same logical peer reconnects.
    ///
    /// Server connection IDs contain the peer's ephemeral source port, so a reboot can create
    /// a new ID while the old half-open connection remains routable. Match those connections by
    /// peer service path and connection type. Client-mode connections are kept because they are
    /// independently owned outbound paths with stable endpoint-based IDs.
    fn replace_stale_server_connections(&self, new_conn_info: &SwbusConnInfo) {
        if new_conn_info.mode() != SwbusConnMode::Server || new_conn_info.remote_service_path().is_none() {
            return;
        }

        // Collect IDs first so no DashMap entry guard is held while removing connections or routes.
        let stale_connection_ids: Vec<String> = self
            .connections
            .iter()
            .filter_map(|entry| match entry.value() {
                ConnTracker::SwbusConn(existing_conn)
                    if existing_conn.info().id() != new_conn_info.id()
                        && existing_conn.info().mode() == new_conn_info.mode()
                        && existing_conn.info().connection_type() == new_conn_info.connection_type()
                        && existing_conn.info().remote_service_path() == new_conn_info.remote_service_path() =>
                {
                    Some(entry.key().clone())
                }
                _ => None,
            })
            .collect();

        for stale_connection_id in stale_connection_ids {
            let Some((_, ConnTracker::SwbusConn(stale_conn))) = self.connections.remove(&stale_connection_id) else {
                continue;
            };
            info!(
                old_conn_id = stale_conn.info().id(),
                new_conn_id = new_conn_info.id(),
                peer = new_conn_info.remote_service_path().as_ref().unwrap().to_longest_path(),
                "Replacing stale peer connection"
            );
            // Remove stale routes immediately; cancellation then lets the old worker exit cleanly.
            self.mux.unregister(stale_conn.info());
            stale_conn.cancel();
        }
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
    use swbus_proto::swbus::*;
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

    #[tokio::test]
    async fn test_conn_established_replaces_stale_server_connection() {
        let local_sp = ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap();
        let remote_sp = ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap();
        let mux = Arc::new(SwbusMultiplexer::new(vec![RouteConfig {
            key: local_sp.clone(),
            scope: RouteScope::InCluster,
        }]));
        let conn_store = SwbusConnStore::new(mux.clone());

        let client_info = Arc::new(
            SwbusConnInfo::new_client(ConnectionType::InCluster, "127.0.0.1:9000".parse().unwrap())
                .with_remote_service_path(remote_sp.clone()),
        );
        let (client_tx, mut client_rx) = mpsc::channel(1);
        conn_store.conn_established(SwbusConn::new(&client_info, client_tx));

        let old_info = Arc::new(SwbusConnInfo::new_server(
            ConnectionType::InCluster,
            "127.0.0.1:8080".parse().unwrap(),
            remote_sp.clone(),
        ));
        let (old_tx, mut old_rx) = mpsc::channel(1);
        conn_store.conn_established(SwbusConn::new(&old_info, old_tx));

        let new_info = Arc::new(SwbusConnInfo::new_server(
            ConnectionType::InCluster,
            "127.0.0.1:8081".parse().unwrap(),
            remote_sp.clone(),
        ));
        let (new_tx, mut new_rx) = mpsc::channel(1);
        conn_store.conn_established(SwbusConn::new(&new_info, new_tx));

        assert!(!conn_store.connections.contains_key(old_info.id()));
        assert!(conn_store.connections.contains_key(new_info.id()));
        assert!(conn_store.connections.contains_key(client_info.id()));

        let message = SwbusMessage {
            header: Some(SwbusMessageHeader::new(local_sp, remote_sp, 1)),
            body: Some(swbus_message::Body::PingRequest(PingRequest::new())),
        };
        mux.route_message(message.clone()).await.unwrap();

        let mut expected = message;
        expected.header.as_mut().unwrap().ttl -= 1;
        assert_eq!(new_rx.recv().await.unwrap().unwrap(), expected);
        assert!(matches!(
            old_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
        assert!(matches!(client_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }
}
