use super::route_config::{PeerConfig, RouteConfig};
use super::{NextHopType, SwbusConn, SwbusConnInfo, SwbusConnMode, SwbusNextHop};
use dashmap::mapref::entry::*;
use dashmap::{DashMap, DashSet};
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::*;
use tokio::task::JoinHandle;
use tokio::time::Duration;

#[derive(Debug)]
enum ConnTracker {
    SwbusConn(SwbusConn),
    Task(JoinHandle<()>),
}

enum RouteStage {
    Global,
    Region,
    Cluster,
    Local,
}
const ROUTE_STAGES: [RouteStage; 4] = [
    RouteStage::Local,
    RouteStage::Cluster,
    RouteStage::Region,
    RouteStage::Global,
];

#[derive(Debug, Default)]
pub struct SwbusMultiplexer {
    /// Route table. Each entry is a registered prefix to a next hop, which points to a connection.
    routes: DashMap<String, SwbusNextHop>,
    connections: DashMap<Arc<SwbusConnInfo>, ConnTracker>,
    my_routes: DashSet<RouteConfig>,
}

impl SwbusMultiplexer {
    pub fn new() -> Self {
        SwbusMultiplexer {
            routes: DashMap::new(),
            connections: DashMap::new(),
            my_routes: DashSet::new(),
        }
    }

    pub fn set_my_routes(self: &Arc<Self>, routes: Vec<RouteConfig>) {
        for route in routes {
            let sr = route.key.clone_for_local_mgmt();

            self.my_routes.insert(route);

            //Create local service route
            let route_key = sr.to_service_prefix();
            let nexthop = SwbusNextHop::new_local(&self);
            self.update_route(route_key, nexthop);
        }
    }

    fn start_connect_task(self: &Arc<Self>, conn_info: Arc<SwbusConnInfo>, reconnect: bool) {
        let conn_info_clone = conn_info.clone();

        let retry_interval = match reconnect {
            true => Duration::from_millis(1),
            false => Duration::from_secs(1),
        };
        let mux = self.clone();
        let retry_task: JoinHandle<()> = tokio::spawn(async move {
            loop {
                match SwbusConn::from_connect(conn_info.clone(), mux.clone()).await {
                    Ok(conn) => {
                        println!("Successfully connect to peer {}", conn_info.id());
                        // register the new connection and update the route table
                        mux.register(conn);
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

    pub fn add_peer(self: &Arc<Self>, peer: PeerConfig) {
        //todo: assuming only one route for now. Will be improved to send routes in route update message and remove this
        let my_route = self.my_routes.iter().next().expect("My service path is not set");
        let conn_info = Arc::new(SwbusConnInfo::new_client(
            peer.scope,
            peer.endpoint,
            peer.id.clone(),
            my_route.key.clone(),
        ));
        self.start_connect_task(conn_info, false);
    }

    pub fn register(self: &Arc<Self>, conn: SwbusConn) {
        // First, we insert the connection to connection table.
        let conn_info = conn.info().clone();
        let proxy = conn.new_proxy();
        self.connections.insert(conn_info.clone(), ConnTracker::SwbusConn(conn));
        // Next, we update the route table.
        let path = conn_info.remote_service_path();
        let route_key = match conn_info.connection_type() {
            Scope::Global => path.to_regional_prefix(),
            Scope::Region => path.to_cluster_prefix(),
            Scope::Cluster => path.to_node_prefix(),
            Scope::Local => path.to_service_prefix(),
            Scope::Client => path.to_string(),
        };
        let nexthop = SwbusNextHop::new_remote(&self, conn_info, proxy, 1);
        self.update_route(route_key, nexthop);
    }

    pub fn unregister(self: &Arc<Self>, conn_info: Arc<SwbusConnInfo>) {
        // First, we remove the connection from the connection table.
        self.connections.remove(&conn_info);

        // Next, we remove the route entry from the route table.
        let path = conn_info.remote_service_path();
        let route_key = match conn_info.connection_type() {
            Scope::Global => path.to_regional_prefix(),
            Scope::Region => path.to_cluster_prefix(),
            Scope::Cluster => path.to_node_prefix(),
            Scope::Local => path.to_service_prefix(),
            Scope::Client => path.to_string(),
        };
        self.routes.remove(&route_key);
        // If connection is client mode, we start a new connection task.
        if conn_info.mode() == SwbusConnMode::Client {
            self.start_connect_task(conn_info, true /*reconnect from connection loss*/);
        }
    }

    fn update_route(&self, route_key: String, nexthop: SwbusNextHop) {
        // If route entry doesn't exist, we insert the next hop as a new one.
        match self.routes.entry(route_key) {
            Entry::Occupied(mut existing) => {
                let route_entry = existing.get();
                if route_entry.hop_count > nexthop.hop_count {
                    existing.insert(nexthop);
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(nexthop);
            }
        }

        // // If we already have one, then we update the entry only when we have a smaller hop count.
        // // The dashmap RefMut reference will hold a lock to the entry, which makes this function atomic.
        // if route_entry.hop_count > nexthop.hop_count {
        //     *route_entry.value_mut() = nexthop;
        // }
    }
    fn get_my_service_path(&self) -> ServicePath {
        self.my_routes
            .iter()
            .by_ref()
            .next()
            .expect("My route is not set")
            .key
            .clone()
    }

    pub async fn route_message(&self, mut message: SwbusMessage) -> Result<()> {
        let header = match message.header {
            Some(ref header) => header,
            None => {
                return Err(SwbusError::input(
                    SwbusErrorCode::InvalidHeader,
                    format!("missing message header"),
                ))
            }
        };
        let destination = match header.destination {
            Some(ref destination) => destination,
            None => {
                return Err(SwbusError::input(
                    SwbusErrorCode::InvalidDestination,
                    format!("missing message destination"),
                ))
            }
        };
        for stage in &ROUTE_STAGES {
            let route_key = match stage {
                RouteStage::Local => destination.to_service_prefix(),
                RouteStage::Cluster => destination.to_node_prefix(),
                RouteStage::Region => destination.to_cluster_prefix(),
                RouteStage::Global => destination.to_regional_prefix(),
            };
            // If the route entry doesn't exist, we drop the message.
            let nexthop = match self.routes.get(&route_key) {
                Some(entry) => entry,
                None => {
                    continue;
                }
            };
            if matches!(nexthop.value().nh_type, NextHopType::Remote) {
                let header: &mut SwbusMessageHeader = message.header.as_mut().expect("missing header"); //should not happen otherwise it won't reach here
                header.ttl -= 1;
                if header.ttl == 0 {
                    let response = SwbusMessage::new_response(
                        &message,
                        &self.get_my_service_path(),
                        SwbusErrorCode::Unreachable,
                        "TTL expired",
                    );
                    Box::pin(self.route_message(response)).await.unwrap();
                    return Ok(()); //returning here as we don't want to forward the message
                }
            }
            // If the route entry is resolved, we forward the message to the next hop.
            nexthop.queue_message(message).await.unwrap();
            return Ok(());
        }

        let response = SwbusMessage::new_response(
            &message,
            &self.get_my_service_path(),
            SwbusErrorCode::NoRoute,
            "Route not found",
        );
        //todo: add Box::pin to route_message
        Box::pin(self.route_message(response)).await.unwrap();

        Ok(())
    }

    pub fn export_routes(&self, scope: Option<Scope>) -> RouteQueryResult {
        let entries: Vec<RouteQueryResultEntry> = self
            .routes
            .iter()
            .filter(|entry| {
                if matches!(entry.value().nh_type, NextHopType::Local) {
                    return false;
                }
                let connection_type = entry.value().conn_info.as_ref().unwrap().connection_type();
                match scope {
                    Some(s) => connection_type <= s && connection_type >= Scope::Cluster,
                    None => true,
                }
            })
            .map(|entry| RouteQueryResultEntry {
                service_path: Some(
                    ServicePath::from_string(&entry.key())
                        .expect("Not expecting service_path in route table to be invalid"),
                ),
                hop_count: entry.value().hop_count,
                nh_id: entry.value().conn_info.as_ref().unwrap().id().to_string(),
                nh_service_path: Some(entry.value().conn_info.as_ref().unwrap().remote_service_path().clone()),
                nh_scope: entry.value().conn_info.as_ref().unwrap().connection_type() as i32,
            })
            .collect();

        RouteQueryResult { entries }
    }
}

#[cfg(test)]
mod tests {
    use tokio::time;

    use super::*;

    #[tokio::test]
    async fn test_set_my_routes() {
        let mux = Arc::new(SwbusMultiplexer::new());
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: Scope::Cluster,
        };
        mux.set_my_routes(vec![route_config.clone()]);

        assert!(mux.my_routes.contains(&route_config));
        assert!(mux
            .routes
            .contains_key(&route_config.key.clone_for_local_mgmt().to_longest_path()));
    }

    #[tokio::test]
    async fn test_add_peer() {
        let mux = Arc::new(SwbusMultiplexer::new());
        let peer_config = PeerConfig {
            scope: Scope::Local,
            endpoint: "127.0.0.1:8080".to_string().parse().unwrap(),
            id: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
        };
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: Scope::Cluster,
        };
        mux.set_my_routes(vec![route_config]);

        mux.add_peer(peer_config);
        assert!(mux
            .connections
            .iter()
            .any(|entry| entry.key().id() == "swbs-to://127.0.0.1:8080"));
    }

    // #[tokio::test]
    // async fn test_register_and_unregister() {
    //     let mux = SwbusMultiplexer::new();
    //     let conn_info = Arc::new(SwbusConnInfo::new_client(
    //         Scope::Local,
    //         "127.0.0.1:8080".to_string(),
    //         "conn1".to_string(),
    //         ServicePath::from_string("test/service").unwrap(),
    //     ));
    //     let conn = SwbusConn::from_connect(conn_info.clone(), Arc::new(mux.clone())).await.unwrap();

    //     mux.register(conn.clone());
    //     assert!(mux.connections.contains_key(&conn_info));
    //     assert!(mux.routes.contains_key(&conn_info.remote_service_path().to_service_prefix()));

    //     mux.unregister(conn_info.clone());
    //     assert!(!mux.connections.contains_key(&conn_info));
    //     assert!(!mux.routes.contains_key(&conn_info.remote_service_path().to_service_prefix()));
    // }

    async fn test_route_message() {
        let mux = SwbusMultiplexer::new();
        let peer_config = PeerConfig {
            scope: Scope::Local,
            endpoint: "127.0.0.1:8080".to_string().parse().unwrap(),
            id: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
        };
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: Scope::Cluster,
        };

        let message = SwbusMessage {
            header: Some(SwbusMessageHeader {
                destination: Some(ServicePath::from_string("test/service").unwrap()),
                ttl: 10,
                ..Default::default()
            }),
            ..Default::default()
        };

        let result = mux.route_message(message).await;
        assert!(result.is_ok());
    }
}
