use super::route_config::{PeerConfig, RouteConfig};
use super::{SwbusConn, SwbusConnInfo, SwbusNextHop};
use dashmap::mapref::entry::*;
use dashmap::{DashMap, DashSet};
use std::sync::{Arc, OnceLock};
use swbus_proto::result::*;
use swbus_proto::swbus::*;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

// pub struct SwbusConnTracker {
//     conn: Option<SwbusConn>,
//     conn_task: Option<tokio::task::JoinHandle<Result<()>>>,
//     task_cancel_watcher: Option<watch::Sender<bool>>,
// }

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
            let mut sr = route.key.clone_for_local_mgmt();

            self.my_routes.insert(route);

            //Create local service route
            let route_key = sr.to_service_prefix();
            let nexthop = SwbusNextHop::new_local();
            self.update_route(route_key, nexthop);
        }
    }

    pub fn start_connect_task(conn_info: Arc<SwbusConnInfo>) {
        let conn_info_clone = conn_info.clone();
        let retry_task: JoinHandle<()> = tokio::spawn(async move {
            loop {
                match SwbusConn::from_connect(conn_info.clone(), SwbusMultiplexer::get().clone()).await {
                    Ok(conn) => {
                        println!("Successfully connect to peer {}", conn_info.id());
                        let proxy = conn.new_proxy();
                        SwbusMultiplexer::get().register(conn);

                        //make a ping message
                        let ping = SwbusMessage {
                            header: Some(SwbusMessageHeader::new(
                                conn_info
                                    .local_service_path()
                                    .expect("local_service_path in client mode conn_info should not be None")
                                    .clone_for_local_mgmt(),
                                conn_info.remote_service_path().clone_for_local_mgmt(),
                            )),
                            body: Some(swbus_message::Body::PingRequest(PingRequest::new())),
                        };
                        let result = SwbusMultiplexer::get().route_message(ping).await;
                        match result {
                            Ok(_) => {
                                println!("Ping message sent successfully");
                            }
                            Err(e) => {
                                println!("Failed to send ping message: {:?}", e);
                            }
                        }
                        break;
                    }
                    Err(e) => {
                        tokio::time::sleep(Duration::from_secs(5)).await;
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
        SwbusMultiplexer::start_connect_task(conn_info);
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

    fn update_route(&self, route_key: String, nexthop: SwbusNextHop) {
        // If route entry doesn't exist, we insert the next hop as a new one.
        match self.routes.entry(route_key) {
            Entry::Occupied(mut existing) => {
                let route_entry = existing.get();
                if route_entry.hop_count > nexthop.hop_count {
                    existing.insert(nexthop);
                }
            }
            Entry::Vacant(mut entry) => {
                entry.insert(nexthop);
            }
        }

        // // If we already have one, then we update the entry only when we have a smaller hop count.
        // // The dashmap RefMut reference will hold a lock to the entry, which makes this function atomic.
        // if route_entry.hop_count > nexthop.hop_count {
        //     *route_entry.value_mut() = nexthop;
        // }
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

            // If the route entry is resolved, we forward the message to the next hop.
            nexthop.queue_message(message).await;
            return Ok(());
        }

        //todo: shall we reply with  route not found message?
        println!("Route not found for {}", destination);
        return Ok(());
    }
}
