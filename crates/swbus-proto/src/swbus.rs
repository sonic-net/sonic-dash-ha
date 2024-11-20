use std::{fmt, time::SystemTime};

tonic::include_proto!("swbus");

impl ServicePath {
    /// Create a new region level service path.
    pub fn with_region(
        region_id: &str,
        service_type: &str,
        service_id: &str,
        resource_type: &str,
        resource_id: &str,
    ) -> Self {
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

    /// Create a new cluster level service path.
    pub fn with_cluster(
        region_id: &str,
        cluster_id: &str,
        service_type: &str,
        service_id: &str,
        resource_type: &str,
        resource_id: &str,
    ) -> Self {
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

    /// Create a new node level service path.
    pub fn with_node(
        region_id: &str,
        cluster_id: &str,
        node_id: &str,
        service_type: &str,
        service_id: &str,
        resource_type: &str,
        resource_id: &str,
    ) -> Self {
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

    pub fn to_regional_prefix(&self) -> String {
        return self.region_id.clone();
    }

    pub fn to_cluster_prefix(&self) -> String {
        format!("{}.{}", self.region_id, self.cluster_id)
    }

    pub fn to_node_prefix(&self) -> String {
        format!("{}.{}.{}", self.region_id, self.cluster_id, self.node_id)
    }

    pub fn to_service_prefix(&self) -> String {
        format!(
            "{}.{}.{}/{}/{}",
            self.region_id, self.cluster_id, self.node_id, self.service_type, self.service_id
        )
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
                self.region_id, self.service_type, self.service_id, self.resource_type, self.resource_id
            )
        }
    }
}

impl SwbusMessageHeader {
    /// To generate sane `id`s, use [`crate::util::SwbusMessageIdGenerator`].
    /// See [`SwbusMessageId`] for notes on id uniqueness.
    pub fn new(source: ServicePath, destination: ServicePath, id: SwbusMessageId) -> Self {
        SwbusMessageHeader {
            version: 1,
            id: Some(id),
            flag: 0,
            ttl: 64,
            source: Some(source),
            destination: Some(destination),
        }
    }
}

impl RequestResponse {
    /// Create a new OK response.
    pub fn ok(request_id: SwbusMessageId) -> Self {
        RequestResponse {
            request_id: Some(request_id),
            error_code: SwbusErrorCode::Ok as i32,
            error_message: "".to_string(),
        }
    }

    /// Create a new infra error response.
    pub fn infra_error(request_id: SwbusMessageId, error_code: SwbusErrorCode, error_message: &str) -> Self {
        RequestResponse {
            request_id: Some(request_id),
            error_code: error_code as i32,
            error_message: error_message.to_string(),
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

impl DataRequest {
    pub fn new(payload: Vec<u8>) -> Self {
        DataRequest { payload }
    }
}

#[cfg(test)]
mod tests {
    use crate::util::SwbusMessageIdGenerator;

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
        let response = RequestResponse::ok(create_mock_message_id());
        test_packing_with_swbus_message(swbus_message::Body::Response(response));

        let response =
            RequestResponse::infra_error(create_mock_message_id(), SwbusErrorCode::NoRoute, "No route is found.");
        test_packing_with_swbus_message(swbus_message::Body::Response(response));
    }

    #[test]
    fn registration_query_request_can_be_created() {
        let request = RegistrationQueryRequest {};
        test_packing_with_swbus_message(swbus_message::Body::RegistrationQueryRequest(request));
    }

    #[test]
    fn registration_query_response_can_be_created() {
        let response = RegistrationQueryResponse {};
        test_packing_with_swbus_message(swbus_message::Body::RegistrationQueryResponse(response));
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
    fn route_data_request_can_be_created() {
        let request = DataRequest::new("mock-payload".as_bytes().to_vec());
        test_packing_with_swbus_message(swbus_message::Body::DataRequest(request));
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

        SwbusMessageHeader::new(source, destination, create_mock_message_id())
    }

    fn create_mock_message_id() -> SwbusMessageId {
        SwbusMessageIdGenerator::new().generate()
    }

    fn test_packing_with_swbus_message(body: swbus_message::Body) {
        let swbus_message = create_mock_swbus_message_header();
        let swbus_message = SwbusMessage {
            header: Some(swbus_message),
            body: Some(body),
        };

        assert!(swbus_message.body.is_some());
    }
}
