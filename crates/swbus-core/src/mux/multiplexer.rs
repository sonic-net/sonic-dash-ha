use super::route_config::{PeerConfig, RouteConfig};
use super::{nexthop, NextHopType, SwbusConn, SwbusConnInfo, SwbusConnMode, SwbusNextHop};
use dashmap::mapref::entry::*;
use dashmap::{DashMap, DashSet};
use std::sync::{Arc, OnceLock};
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

static MUX: OnceLock<Arc<SwbusMultiplexer>> = OnceLock::new();
#[derive(Debug, Default)]
pub struct SwbusMultiplexer {
    /// Route table. Each entry is a registered prefix to a next hop, which points to a connection.
    routes: DashMap<String, SwbusNextHop>,
    connections: DashMap<Arc<SwbusConnInfo>, ConnTracker>,
    my_routes: DashSet<RouteConfig>,
}

impl SwbusMultiplexer {
    pub fn get() -> &'static Arc<SwbusMultiplexer> {
        MUX.get_or_init(|| Arc::new(SwbusMultiplexer::new()))
    }

    fn new() -> Self {
        SwbusMultiplexer {
            routes: DashMap::new(),
            connections: DashMap::new(),
            my_routes: DashSet::new(),
        }
    }

    pub fn set_my_routes(&self, routes: Vec<RouteConfig>) {
        for route in routes {
            let sr = route.key.clone_for_local_mgmt();

            self.my_routes.insert(route);

            //Create local service route
            let route_key = sr.to_service_prefix();
            let nexthop = SwbusNextHop::new_local();
            self.update_route(route_key, nexthop);
        }
    }

    fn start_connect_task(conn_info: Arc<SwbusConnInfo>, reconnect: bool) {
        let conn_info_clone = conn_info.clone();

        let retry_interval = match reconnect {
            true => Duration::from_millis(1),
            false => Duration::from_secs(1),
        };
        let retry_task: JoinHandle<()> = tokio::spawn(async move {
            loop {
                match SwbusConn::from_connect(conn_info.clone(), SwbusMultiplexer::get().clone()).await {
                    Ok(conn) => {
                        println!("Successfully connect to peer {}", conn_info.id());
                        // register the new connection and update the route table
                        SwbusMultiplexer::get().register(conn);
                        break;
                    }
                    Err(_) => {
                        tokio::time::sleep(retry_interval).await;
                    }
                }
            }
        });
        SwbusMultiplexer::get()
            .connections
            .insert(conn_info_clone, ConnTracker::Task(retry_task));
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
        SwbusMultiplexer::start_connect_task(conn_info, false);
    }

    pub fn register(&self, conn: SwbusConn) {
        // First, we insert the connection to connection table.
        let conn_info = conn.info().clone();
        let proxy = conn.new_proxy();
        SwbusMultiplexer::get()
            .connections
            .insert(conn_info.clone(), ConnTracker::SwbusConn(conn));
        // Next, we update the route table.
        let path = conn_info.remote_service_path();
        let route_key = match conn_info.connection_type() {
            Scope::Global => path.to_regional_prefix(),
            Scope::Region => path.to_cluster_prefix(),
            Scope::Cluster => path.to_node_prefix(),
            Scope::Local => path.to_service_prefix(),
            Scope::Client => path.to_string(),
        };
        let nexthop = SwbusNextHop::new_remote(conn_info, proxy, 1);
        self.update_route(route_key, nexthop);
    }

    pub fn unregister(&self, conn_info: Arc<SwbusConnInfo>) {
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
            SwbusMultiplexer::start_connect_task(conn_info, true /*reconnect from connection loss*/);
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
                    Box::pin(self.route_message(response)).await.unwrap()
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
