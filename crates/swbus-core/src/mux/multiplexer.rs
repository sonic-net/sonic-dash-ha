use super::route_config::PeerConfig;
use super::{SwbusConn, SwbusConnInfo, SwbusNextHop};
use dashmap::DashMap;
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
//todo: move to Mux and make Mux singleton
// static CONNECTION_STORE: OnceLock<DashMap<Arc<SwbusConnInfo>, ConnTracker>> = OnceLock::new();
// pub fn connections() -> &'static DashMap<Arc<SwbusConnInfo>, ConnTracker> {
//     CONNECTION_STORE.get_or_init(|| DashMap::new())
// }
static MUX: OnceLock<Arc<SwbusMultiplexer>> = OnceLock::new();
#[derive(Debug, Default)]
pub struct SwbusMultiplexer {
    /// Route table. Each entry is a registered prefix to a next hop, which points to a connection.
    routes: DashMap<String, Option<SwbusNextHop>>,
    connections: DashMap<Arc<SwbusConnInfo>, ConnTracker>,
}

impl SwbusMultiplexer {
    pub fn get() -> &'static Arc<SwbusMultiplexer> {
        MUX.get_or_init(|| Arc::new(SwbusMultiplexer::new()))
    }

    fn new() -> Self {
        SwbusMultiplexer {
            routes: DashMap::new(),
            connections: DashMap::new(),
        }
    }

    pub fn add_peer(self: &Arc<Self>, peer: PeerConfig) {
        let conn_info = Arc::new(SwbusConnInfo::new_client(peer.scope, peer.endpoint));
        let conn_info_clone = conn_info.clone();
        let retry_task: JoinHandle<()> = tokio::spawn(async move {
            loop {
                match SwbusConn::from_connect(conn_info_clone.clone(), SwbusMultiplexer::get().clone()).await {
                    Ok(conn) => {
                        let proxy = conn.new_proxy();
                        SwbusMultiplexer::get()
                            .connections
                            .insert(conn_info_clone.clone(), ConnTracker::SwbusConn(conn));
                        //make a ping message
                        let ping = SwbusMessage {
                            header: Some(SwbusMessageHeader::new(
                                ServicePath::from_string("region-a/cluster-a/10.0.0.1-dpu0"),
                                ServicePath::from_string("region-a/cluster-a/10.0.0.2-dpu0"),
                            )),
                            body: Some(swbus_message::Body::PingRequest(PingRequest::new())),
                        };
                        proxy.queue_message(Ok(ping)).await;
                        break;
                    }
                    Err(e) => {
                        println!("Failed to connect to peer {}: {}", conn_info_clone.id(), e);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });
        SwbusMultiplexer::get()
            .connections
            .insert(conn_info, ConnTracker::Task(retry_task));
    }
    pub fn add_connection(&self, conn: SwbusConn) {
        let conn_info = conn.info();
        SwbusMultiplexer::get()
            .connections
            .insert(conn_info.clone(), ConnTracker::SwbusConn(conn));
    }

    pub fn register(&self, path: &ServicePath, conn: SwbusConn) {
        // First, we insert the connection to connection table.
        //let conn_info = conn.info();
        //self.connections.insert(conn_info.clone(), conn);

        // Next, we update the route table.
        // let route_key = match conn_type {
        //     ConnectionType::Global => path.to_regional_prefix(),
        //     ConnectionType::Regional => path.to_cluster_prefix(),
        //     ConnectionType::Cluster => path.to_node_prefix(),
        //     ConnectionType::Node => path.to_service_prefix(),
        //     ConnectionType::Client => path.to_string(),
        // };
        // let nexthop = SwbusNextHop::new(conn_id, 1);
        // self.update_route(route_key, nexthop);
    }

    fn update_route(&self, route_key: String, nexthop: SwbusNextHop) {
        // // If route entry doesn't exist, we insert the next hop as a new one.
        // let mut route_entry = self.routes.entry(route_key).or_insert(nexthop.clone());

        // // If we already have one, then we update the entry only when we have a smaller hop count.
        // // The dashmap RefMut reference will hold a lock to the entry, which makes this function atomic.
        // if route_entry.hop_count > nexthop.hop_count {
        //     *route_entry.value_mut() = nexthop;
        // }
    }
}
