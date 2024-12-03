use super::route_config::{PeerConfig, RouteConfig};
use super::{NextHopType, SwbusConn, SwbusConnInfo, SwbusConnMode, SwbusConnProxy, SwbusNextHop};
use dashmap::mapref::entry::*;
use dashmap::{DashMap, DashSet};
use std::sync::{Arc, OnceLock};
use swbus_proto::message_id_generator::MessageIdGenerator;
use swbus_proto::result::*;
use swbus_proto::swbus::*;
use tokio::task::JoinHandle;
use tokio::time::Duration;

// pub struct SwbusConnTracker {
//     conn: Option<SwbusConn>,
//     conn_task: Option<tokio::task::JoinHandle<Result<()>>>,
//     task_cancel_watcher: Option<watch::Sender<bool>>,
// }

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
#[derive(Default)]
pub struct SwbusMultiplexer {
    /// Route table. Each entry is a registered prefix to a next hop, which points to a connection.
    routes: DashMap<String, SwbusNextHop>,
    id_generator: MessageIdGenerator,
}

impl SwbusMultiplexer {
    pub fn get() -> &'static Arc<SwbusMultiplexer> {
        MUX.get_or_init(|| Arc::new(SwbusMultiplexer::new()))
    }

    fn new() -> Self {
        SwbusMultiplexer {
            routes: DashMap::new(),
            id_generator: MessageIdGenerator::new(),
        }
    }

    pub fn generate_message_id(&self) -> u64 {
        self.id_generator.generate()
    }

    pub(crate) fn register(&self, conn_info: &Arc<SwbusConnInfo>, proxy: SwbusConnProxy) {
        // Update the route table.
        let path = conn_info.remote_service_path();
        let route_key = match conn_info.connection_type() {
            RouteScope::ScopeGlobal => path.to_regional_prefix(),
            RouteScope::ScopeRegion => path.to_cluster_prefix(),
            RouteScope::ScopeCluster => path.to_node_prefix(),
            RouteScope::ScopeLocal => path.to_service_prefix(),
            RouteScope::ScopeClient => path.to_string(),
        };
        let nexthop = SwbusNextHop::new_remote(conn_info.clone(), proxy, 1);
        self.update_route(route_key, nexthop);
    }

    pub(crate) fn unregister(&self, conn_info: Arc<SwbusConnInfo>) {
        // remove the route entry from the route table.
        let path = conn_info.remote_service_path();
        let route_key = match conn_info.connection_type() {
            RouteScope::ScopeGlobal => path.to_regional_prefix(),
            RouteScope::ScopeRegion => path.to_cluster_prefix(),
            RouteScope::ScopeCluster => path.to_node_prefix(),
            RouteScope::ScopeLocal => path.to_service_prefix(),
            RouteScope::ScopeClient => path.to_string(),
        };
        self.routes.remove(&route_key);
    }

    pub(crate) fn update_route(&self, route_key: String, nexthop: SwbusNextHop) {
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

    pub async fn route_message(&self, message: SwbusMessage) -> Result<()> {
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
            nexthop.queue_message(message).await.unwrap();

            return Ok(());
        }

        //todo: send response with no_route error

        Ok(())
    }

    pub fn export_routes(&self, scope: Option<RouteScope>) -> RouteQueryResult {
        let entries: Vec<RouteQueryResultEntry> = self
            .routes
            .iter()
            .filter(|entry| {
                if matches!(entry.value().nh_type, NextHopType::Local) {
                    return false;
                }
                let connection_type = entry.value().conn_info.as_ref().unwrap().connection_type();
                match scope {
                    Some(s) => connection_type <= s && connection_type >= RouteScope::ScopeCluster,
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
