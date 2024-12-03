use super::route_config::RouteConfig;
use super::{NextHopType, SwbusConnInfo, SwbusConnProxy, SwbusNextHop};
use dashmap::mapref::entry::*;
use dashmap::{DashMap, DashSet};
use std::sync::Arc;
use swbus_proto::message_id_generator::MessageIdGenerator;
use swbus_proto::result::*;
use swbus_proto::swbus::*;

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

#[derive(Default)]
pub struct SwbusMultiplexer {
    /// Route table. Each entry is a registered prefix to a next hop, which points to a connection.
    routes: DashMap<String, SwbusNextHop>,
    id_generator: MessageIdGenerator,
    my_routes: DashSet<RouteConfig>,
}

impl SwbusMultiplexer {
    pub fn new() -> Self {
        SwbusMultiplexer {
            routes: DashMap::new(),
            id_generator: MessageIdGenerator::new(),
            my_routes: DashSet::new(),
        }
    }

    pub fn generate_message_id(&self) -> u64 {
        self.id_generator.generate()
    }

    pub(crate) fn register(self: &Arc<SwbusMultiplexer>, conn_info: &Arc<SwbusConnInfo>, proxy: SwbusConnProxy) {
        // Update the route table.
        let path = conn_info.remote_service_path();
        let route_key = match conn_info.connection_type() {
            RouteScope::ScopeGlobal => path.to_regional_prefix(),
            RouteScope::ScopeRegion => path.to_cluster_prefix(),
            RouteScope::ScopeCluster => path.to_node_prefix(),
            RouteScope::ScopeLocal => path.to_service_prefix(),
            RouteScope::ScopeClient => path.to_string(),
        };
        let nexthop = SwbusNextHop::new_remote(self, conn_info.clone(), proxy, 1);
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
    pub fn set_my_routes(self: &Arc<Self>, routes: Vec<RouteConfig>) {
        for route in routes {
            self.my_routes.insert(route);
        }
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
                        Some(&self.get_my_service_path()),
                        SwbusErrorCode::Unreachable,
                        "TTL expired",
                        self.id_generator.generate(),
                        None,
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
            Some(&self.get_my_service_path()),
            SwbusErrorCode::NoRoute,
            "Route not found",
            self.id_generator.generate(),
            None,
        );

        Box::pin(self.route_message(response)).await.unwrap();

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

#[cfg(test)]
mod tests {
    use tokio::time;

    use crate::mux::PeerConfig;

    use super::*;

    #[tokio::test]
    async fn test_set_my_routes() {
        let mux = Arc::new(SwbusMultiplexer::new());
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: RouteScope::ScopeCluster,
        };
        mux.set_my_routes(vec![route_config.clone()]);

        assert!(mux.my_routes.contains(&route_config));
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
            scope: RouteScope::ScopeLocal,
            endpoint: "127.0.0.1:8080".to_string().parse().unwrap(),
            id: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
        };
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: RouteScope::ScopeCluster,
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
