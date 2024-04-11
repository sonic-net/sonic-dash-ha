use std::fmt;

tonic::include_proto!("swbus");

impl ServicePath {
    pub fn with_region(region_id: &str, service_type: &str, service_id: &str, resource_type: &str, resource_id: &str) -> Self {
        ServicePath {
            region_id: region_id.to_string(),
            cluster_id: "".to_string(),
            node_id: "".to_string(),
            service_type: service_type.to_string(),
            service_id: service_id.to_string(),
            resource_type: resource_type.to_string(),
            resource_id: resource_id.to_string(),
        }
    }

    pub fn with_cluster(region_id: &str, cluster_id: &str, service_type: &str, service_id: &str, resource_type: &str, resource_id: &str) -> Self {
        ServicePath {
            region_id: region_id.to_string(),
            cluster_id: cluster_id.to_string(),
            node_id: "".to_string(),
            service_type: service_type.to_string(),
            service_id: service_id.to_string(),
            resource_type: resource_type.to_string(),
            resource_id: resource_id.to_string(),
        }
    }

    pub fn with_node(region_id: &str, cluster_id: &str, node_id: &str, service_type: &str, service_id: &str, resource_type: &str, resource_id: &str) -> Self {
        ServicePath {
            region_id: region_id.to_string(),
            cluster_id: cluster_id.to_string(),
            node_id: node_id.to_string(),
            service_type: service_type.to_string(),
            service_id: service_id.to_string(),
            resource_type: resource_type.to_string(),
            resource_id: resource_id.to_string(),
        }
    }
}

impl fmt::Display for ServicePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.node_id.is_empty() {
            write!(
                f,
                "{}.{}.{}/{}/{}/{}/{}",
                self.region_id,
                self.cluster_id,
                self.node_id,
                self.service_type,
                self.service_id,
                self.resource_type,
                self.resource_id
            )
        } else if !self.cluster_id.is_empty() {
            write!(
                f,
                "{}.{}/{}/{}/{}/{}",
                self.region_id,
                self.cluster_id,
                self.service_type,
                self.service_id,
                self.resource_type,
                self.resource_id
            )
        } else {
            write!(
                f,
                "{}/{}/{}/{}/{}",
                self.region_id,
                self.service_type,
                self.service_id,
                self.resource_type,
                self.resource_id
            )
        }
    }
}

impl SwbusMessageHeader {
    pub fn new(source: ServicePath, destination: ServicePath) -> Self {
        SwbusMessageHeader {
            magic: 0x512F512F,
            epoch: 0,
            flag: 0,
            ttl: 64,
            source: Some(source),
            destination: Some(destination),
        }
    }
}

impl RequestResponse {
    pub fn ok(request_epoch: u64) -> Self {
        RequestResponse {
            request_epoch,
            error: Some(SwbusError { code: Some(swbus_error::Code::NoError(0)), message: "".to_string() }),
        }
    }

    pub fn infra_error(request_epoch: u64, error_code: SwbusInfraErrorType, error_message: &str) -> Self {
        RequestResponse {
            request_epoch,
            error: Some(SwbusError { code: Some(swbus_error::Code::InfraError(error_code as i32)), message: error_message.to_string() }),
        }
    }

    pub fn app_error(request_epoch: u64, error_code: u64, error_message: &str) -> Self {
        RequestResponse {
            request_epoch,
            error: Some(SwbusError { code: Some(swbus_error::Code::AppError(error_code)), message: error_message.to_string() }),
        }
    }
}

impl PingRequest {
    pub fn new() -> Self {
        PingRequest {}
    }
}

impl TraceRouteRequest {
    pub fn new(trace_id: &str) -> Self {
        TraceRouteRequest {
            trace_id: trace_id.to_string(),
        }
    }
}

impl TraceRouteResponse {
    pub fn new(trace_id: &str) -> Self {
        TraceRouteResponse {
            trace_id: trace_id.to_string(),
        }
    }
}

impl RouteMessageRequest {
    pub fn new(payload: Vec<u8>) -> Self {
        RouteMessageRequest { payload }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn service_path_can_be_converted_to_string() {
        let service_path = ServicePath {
            region_id: "region".to_string(),
            cluster_id: "cluster".to_string(),
            node_id: "node".to_string(),
            service_type: "service_type".to_string(),
            service_id: "service_id".to_string(),
            resource_type: "resource_type".to_string(),
            resource_id: "resource_id".to_string(),
        };
        assert_eq!(
            service_path.to_string(),
            "region.cluster.node/service_type/service_id/resource_type/resource_id"
        );

        let service_path = ServicePath {
            region_id: "region".to_string(),
            cluster_id: "cluster".to_string(),
            node_id: "".to_string(),
            service_type: "service_type".to_string(),
            service_id: "service_id".to_string(),
            resource_type: "resource_type".to_string(),
            resource_id: "resource_id".to_string(),
        };
        assert_eq!(
            service_path.to_string(),
            "region.cluster/service_type/service_id/resource_type/resource_id"
        );

        let service_path = ServicePath {
            region_id: "region".to_string(),
            cluster_id: "".to_string(),
            node_id: "".to_string(),
            service_type: "service_type".to_string(),
            service_id: "service_id".to_string(),
            resource_type: "resource_type".to_string(),
            resource_id: "resource_id".to_string(),
        };
        assert_eq!(
            service_path.to_string(),
            "region/service_type/service_id/resource_type/resource_id"
        );
    }

    #[test]
    fn request_response_can_be_created() {
        let response = RequestResponse::ok(123);
        test_packing_with_swbus_message(swbus_message::Body::Response(response));

        let response = RequestResponse::infra_error(123, SwbusInfraErrorType::NoRoute, "No route is found.");
        test_packing_with_swbus_message(swbus_message::Body::Response(response));

        let response = RequestResponse::app_error(123, 123123, "No route is found.");
        test_packing_with_swbus_message(swbus_message::Body::Response(response));
    }

    #[test]
    fn register_request_can_be_created() {
        let request = RegisterRequest {
            service_paths: vec![create_mock_service_path()],
        };
        test_packing_with_swbus_message(swbus_message::Body::RegisterRequest(request));
    }

    #[test]
    fn unregister_request_can_be_created() {
        let request = UnregisterRequest {
            service_paths: vec![create_mock_service_path()],
        };
        test_packing_with_swbus_message(swbus_message::Body::UnregisterRequest(request));
    }

    #[test]
    fn ping_request_can_be_created() {
        let request = PingRequest::new();
        test_packing_with_swbus_message(swbus_message::Body::PingRequest(request));
    }

    #[test]
    fn trace_route_request_can_be_created() {
        let request = TraceRouteRequest::new("mock-trace-id");
        test_packing_with_swbus_message(swbus_message::Body::TraceRouteRequest(request));
    }

    #[test]
    fn trace_route_response_can_be_created() {
        let response = TraceRouteResponse::new("mock-trace-id");
        test_packing_with_swbus_message(swbus_message::Body::TraceRouteResponse(response));
    }

    #[test]
    fn route_message_request_can_be_created() {
        let request = RouteMessageRequest::new("mock-payload".as_bytes().to_vec());
        test_packing_with_swbus_message(swbus_message::Body::RouteMessageRequest(request));
    }

    fn create_mock_service_path() -> ServicePath {
        ServicePath {
            region_id: "region".to_string(),
            cluster_id: "cluster".to_string(),
            node_id: "node".to_string(),
            service_type: "service_type".to_string(),
            service_id: "service_id".to_string(),
            resource_type: "resource_type".to_string(),
            resource_id: "resource_id".to_string(),
        }
    }

    fn create_mock_swbus_message_header() -> SwbusMessageHeader {
        let mut source = create_mock_service_path();
        source.resource_id = "source_resource_id".to_string();

        let mut destination = create_mock_service_path();
        destination.resource_id = "destination_resource_id".to_string();

        SwbusMessageHeader::new(source, destination)
    }

    fn test_packing_with_swbus_message(body: swbus_message::Body) {
        let swbus_message = create_mock_swbus_message_header();
        let swbus_message = SwbusMessage {
            header: Some(swbus_message),
            body: Some(body),
        };

        assert!(swbus_message.body.is_some());
    }

// RequestResponse
}