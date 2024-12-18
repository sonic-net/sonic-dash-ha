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
            ConnectionType::Global => path.to_regional_prefix(),
            ConnectionType::Region => path.to_cluster_prefix(),
            ConnectionType::Cluster => path.to_node_prefix(),
            ConnectionType::Local => path.to_service_prefix(),
            ConnectionType::Client => path.to_string(),
        };
        let nexthop = SwbusNextHop::new_remote(conn_info.clone(), proxy, 1);
        self.update_route(route_key, nexthop);
    }

    pub(crate) fn unregister(&self, conn_info: Arc<SwbusConnInfo>) {
        // remove the route entry from the route table.
        let path = conn_info.remote_service_path();
        let route_key = match conn_info.connection_type() {
            ConnectionType::Global => path.to_regional_prefix(),
            ConnectionType::Region => path.to_cluster_prefix(),
            ConnectionType::Cluster => path.to_node_prefix(),
            ConnectionType::Local => path.to_service_prefix(),
            ConnectionType::Client => path.to_string(),
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
            let sr = route.key.clone_for_local_mgmt();

            //Create local service route
            let route_key = sr.to_service_prefix();
            let local_nh = SwbusNextHop::new_local();
            self.update_route(route_key, local_nh);

            //Create catch-all route
            let route_key = sr.to_node_prefix();
            let drop_nh = SwbusNextHop::new_drop();
            self.update_route(route_key, drop_nh);
            self.my_routes.insert(route);
        }
    }

    pub fn get_my_service_path(&self) -> ServicePath {
        self.my_routes
            .iter()
            .by_ref()
            .next()
            .expect("My route is not set")
            .key
            .clone()
    }

    pub async fn route_message(&self, message: SwbusMessage) -> Result<()> {
        let header = match message.header {
            Some(ref header) => header,
            None => {
                return Err(SwbusError::input(
                    SwbusErrorCode::InvalidHeader,
                    "missing message header".to_string(),
                ))
            }
        };
        let destination = match header.destination {
            Some(ref destination) => destination,
            None => {
                return Err(SwbusError::input(
                    SwbusErrorCode::InvalidDestination,
                    "missing message destination".to_string(),
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
            let response = nexthop.queue_message(self, message).await.unwrap();
            if let Some(response) = response {
                Box::pin(self.route_message(response)).await.unwrap();
            } else {
                // todo: try another nexthop if there is one
            }
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
        //there is a risk of loop if no route to the source of request.
        // A receives a request from B to C but A doesn't have route to C. It sends response[1] to B
        // but B is also unreachable.
        // Here it will send 'no-route' response[2] for response[1]. response[1] has source SP of A
        // because the response is originated from A. So response[2]'s dest is to A (itself).
        // Response[2] will be sent to a drop nexhop, which should drop the unexpected response packet.
        Box::pin(self.route_message(response)).await.unwrap();

        Ok(())
    }

    pub fn export_routes(&self, scope: Option<RouteScope>) -> RouteQueryResult {
        let entries: Vec<RouteQueryResultEntry> = self
            .routes
            .iter()
            .filter(|entry| {
                if !matches!(entry.value().nh_type, NextHopType::Remote) {
                    return false;
                }
                let route_scope = ServicePath::from_string(entry.key()).unwrap().route_scope();
                match scope {
                    Some(s) => route_scope >= s && route_scope >= RouteScope::Cluster,
                    None => true,
                }
            })
            .map(|entry| RouteQueryResultEntry {
                service_path: Some(
                    ServicePath::from_string(entry.key())
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
    use pretty_assertions::assert_eq;
    use tokio::{sync::mpsc, time};
    use tonic::Status;

    use super::*;
    use crate::mux::SwbusConn;
    use tokio::time::Duration;

    #[test]
    fn test_set_my_routes() {
        let mux = Arc::new(SwbusMultiplexer::new());
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: RouteScope::Cluster,
        };
        mux.set_my_routes(vec![route_config.clone()]);
        assert!(mux.my_routes.contains(&route_config));

        let nh = mux
            .routes
            .get(&route_config.key.clone_for_local_mgmt().to_service_prefix())
            .unwrap();
        assert_eq!(nh.nh_type, NextHopType::Local);

        let nh = mux.routes.get(&route_config.key.to_node_prefix()).unwrap();
        assert_eq!(nh.nh_type, NextHopType::Drop);
    }

    fn add_route(
        mux: &SwbusMultiplexer,
        route_key: &str,
        hop_count: u32,
        nh_sp: &str,
        nh_conn_type: ConnectionType,
    ) -> mpsc::Receiver<Result<SwbusMessage, Status>> {
        let conn_info = Arc::new(SwbusConnInfo::new_client(
            nh_conn_type,
            "127.0.0.1:8080".parse().unwrap(),
            ServicePath::from_string(nh_sp).unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.1-dpu0").unwrap(),
        ));
        let (conn, _, message_queue_rx) = SwbusConn::new_for_test(&conn_info);

        let nexthop_nh1 = SwbusNextHop::new_remote(conn_info.clone(), conn.new_proxy(), hop_count);
        mux.update_route(route_key.to_string(), nexthop_nh1);
        message_queue_rx
    }

    async fn route_message_and_compare(
        mux: &SwbusMultiplexer,
        message_queue_rx: &mut mpsc::Receiver<Result<SwbusMessage, Status>>,
        request: &str,
        expected: &str,
    ) {
        let request_msg: SwbusMessage = serde_json::from_str(request).unwrap();
        let expected_msg: SwbusMessage = serde_json::from_str(expected).unwrap();

        let result = mux.route_message(request_msg).await;
        assert!(result.is_ok());
        match time::timeout(Duration::from_secs(1), message_queue_rx.recv()).await {
            Ok(Some(msg)) => {
                let normalized_msg = swbus_proto::swbus::normalize_msg(&msg.ok().unwrap());

                assert_eq!(normalized_msg, expected_msg);
            }
            _ => {
                panic!("No message received");
            }
        }
    }

    #[tokio::test]
    async fn test_route_message() {
        let mux = Arc::new(SwbusMultiplexer::new());

        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            scope: RouteScope::Cluster,
        };

        mux.set_my_routes(vec![route_config.clone()]);

        let _ = add_route(
            &mux,
            "region-a.cluster-a.10.0.0.1-dpu0",
            1,
            "region-a.cluster-a.10.0.0.1-dpu0",
            ConnectionType::Cluster,
        );
        let mut message_queue_rx3 = add_route(
            &mux,
            "region-a.cluster-a.10.0.0.3-dpu0",
            1,
            "region-a.cluster-a.10.0.0.3-dpu0",
            ConnectionType::Cluster,
        );

        let request = r#"
            {
              "header": {
                "version": 1,
                "id": 0,
                "flag": 0,
                "ttl": 63,
                "source": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0",
                "destination": "region-a.cluster-a.10.0.0.3-dpu0/local-mgmt/0"
              },
              "body": {
                "PingRequest": {}
              }
            }
            "#;
        let expected = r#"
            {
                "header": {
                    "version": 1,
                    "id": 0,
                    "flag": 0,
                    "ttl": 62,
                    "source": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0",
                    "destination": "region-a.cluster-a.10.0.0.3-dpu0/local-mgmt/0"
                },
                "body": {
                    "PingRequest": {}
                }
            }
            "#;
        route_message_and_compare(&mux, &mut message_queue_rx3, request, expected).await;
    }

    #[tokio::test]
    async fn test_route_message_unreachable() {
        let mux = Arc::new(SwbusMultiplexer::new());

        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            scope: RouteScope::Cluster,
        };

        mux.set_my_routes(vec![route_config.clone()]);

        let mut message_queue_rx1 = add_route(
            &mux,
            "region-a.cluster-a.10.0.0.1-dpu0",
            1,
            "region-a.cluster-a.10.0.0.1-dpu0",
            ConnectionType::Cluster,
        );
        let _ = add_route(
            &mux,
            "region-a.cluster-a.10.0.0.3-dpu0",
            1,
            "region-a.cluster-a.10.0.0.3-dpu0",
            ConnectionType::Cluster,
        );

        let request = r#"
            {
                "header": {
                "version": 1,
                "id": 0,
                "flag": 0,
                "ttl": 1,
                "source": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0",
                "destination": "region-a.cluster-a.10.0.0.3-dpu0/local-mgmt/0"
                },
                "body": {
                "PingRequest": {}
                }
            }
            "#;
        let expected = r#"
            {
                "header": {
                    "version": 1,
                    "id": 0,
                    "flag": 0,
                    "ttl": 63,
                    "source": "region-a.cluster-a.10.0.0.2-dpu0",
                    "destination": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0"
                },
                "body": {
                    "Response": {
                    "request_id": 0,
                    "error_code": 303,
                    "error_message": "TTL expired",
                    "response_body": null
                    }
                }
            }
            "#;
        route_message_and_compare(&mux, &mut message_queue_rx1, request, expected).await;
    }

    #[tokio::test]
    async fn test_route_message_noroute() {
        let mux = Arc::new(SwbusMultiplexer::new());

        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            scope: RouteScope::Cluster,
        };

        mux.set_my_routes(vec![route_config.clone()]);

        let mut message_queue_rx1 = add_route(
            &mux,
            "region-a.cluster-a.10.0.0.1-dpu0",
            1,
            "region-a.cluster-a.10.0.0.1-dpu0",
            ConnectionType::Cluster,
        );

        let request = r#"
            {
                "header": {
                "version": 1,
                "id": 0,
                "flag": 0,
                "ttl": 63,
                "source": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0",
                "destination": "region-a.cluster-a.10.0.0.3-dpu0/local-mgmt/0"
                },
                "body": {
                "PingRequest": {}
                }
            }
            "#;
        let expected = r#"
            {
                "header": {
                    "version": 1,
                    "id": 0,
                    "flag": 0,
                    "ttl": 63,
                    "source": "region-a.cluster-a.10.0.0.2-dpu0",
                    "destination": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0"
                },
                "body": {
                    "Response": {
                    "request_id": 0,
                    "error_code": 301,
                    "error_message": "Route not found",
                    "response_body": null
                    }
                }
            }
            "#;
        route_message_and_compare(&mux, &mut message_queue_rx1, request, expected).await;
    }

    #[tokio::test]
    async fn test_route_message_isolated() {
        let mux = Arc::new(SwbusMultiplexer::new());

        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            scope: RouteScope::Cluster,
        };

        mux.set_my_routes(vec![route_config.clone()]);

        let request = r#"
            {
                "header": {
                "version": 1,
                "id": 0,
                "flag": 0,
                "ttl": 63,
                "source": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0",
                "destination": "region-a.cluster-a.10.0.0.3-dpu0/local-mgmt/0"
                },
                "body": {
                "PingRequest": {}
                }
            }
            "#;

        let request_msg: SwbusMessage = serde_json::from_str(request).unwrap();

        let result = mux.route_message(request_msg).await;
        // packet should be dropped and not causing stack overflow
        assert!(result.is_ok());
    }

    #[test]
    fn test_export_routes() {
        let mux = Arc::new(SwbusMultiplexer::new());

        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            scope: RouteScope::Cluster,
        };

        mux.set_my_routes(vec![route_config.clone()]);

        let mut _message_queue_rx1 = add_route(
            &mux,
            "region-a.cluster-a.10.0.0.1-dpu0",
            1,
            "region-a.cluster-a.10.0.0.1-dpu0",
            ConnectionType::Cluster,
        );
        let mut _message_queue_rx3 = add_route(
            &mux,
            "region-a.cluster-b",
            1,
            "region-a.cluster-b.10.0.0.1-dpu0",
            ConnectionType::Region,
        );

        let routes = mux.export_routes(Some(RouteScope::Cluster));
        let json_string = serde_json::to_string(&routes).unwrap();
        let normalized_routes: RouteQueryResult = serde_json::from_str(&json_string).unwrap();

        let entry1 = RouteQueryResultEntry {
            service_path: Some(ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap()),
            hop_count: 1,
            nh_id: "".to_string(),
            nh_service_path: Some(ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap()),
            nh_scope: RouteScope::Cluster as i32,
        };
        let entry2 = RouteQueryResultEntry {
            service_path: Some(ServicePath::from_string("region-a.cluster-b").unwrap()),
            hop_count: 1,
            nh_id: "".to_string(),
            nh_service_path: Some(ServicePath::from_string("region-a.cluster-b.10.0.0.1-dpu0").unwrap()),
            nh_scope: RouteScope::Region as i32,
        };

        let expected = RouteQueryResult {
            entries: vec![entry1.clone(), entry2.clone()],
        };
        assert_eq!(normalized_routes, expected);

        let routes = mux.export_routes(Some(RouteScope::Region));
        let json_string = serde_json::to_string(&routes).unwrap();
        let normalized_routes: RouteQueryResult = serde_json::from_str(&json_string).unwrap();
        let expected = RouteQueryResult { entries: vec![entry2] };
        assert_eq!(normalized_routes, expected);
    }
}
