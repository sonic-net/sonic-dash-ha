use super::result::*;
use serde::{Deserialize, Serialize};
use std::fmt;
tonic::include_proto!("swbus");

/// Service path attribute in gRPC request meta data
pub const CLIENT_SERVICE_PATH: &str = "x-swbus-service-path";
/// Service path scope of the connection
pub const SERVICE_PATH_SCOPE: &str = "x-swbus-scope";

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
    /// Create a new service path from a string. Service path must be in below format
    ///   [region_id[.cluster_id[.node_id]][/service_type/service_id[/resource_type/resource_id]]
    pub fn from_string(service_path: &str) -> Result<Self> {
        let mut parts: Vec<&str> = service_path.split('/').collect();
        //fill up service and resource with empty string
        parts.resize(5, "");
        //parts[0] is locator region.cluster.node
        let mut loc_parts: Vec<&str> = parts[0].splitn(3, '.').collect();
        loc_parts.resize(3, "");
        Ok(ServicePath {
            region_id: loc_parts[0].to_string(),
            cluster_id: loc_parts[1].to_string(),
            node_id: loc_parts[2].to_string(),
            service_type: parts[1].to_string(),
            service_id: parts[2].to_string(),
            resource_type: parts[3].to_string(),
            resource_id: parts[4].to_string(),
        })
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

    pub fn clone_for_local_mgmt(&self) -> Self {
        ServicePath {
            region_id: self.region_id.clone(),
            cluster_id: self.cluster_id.clone(),
            node_id: self.node_id.clone(),
            service_type: "local-mgmt".to_string(),
            service_id: "0".to_string(),
            resource_type: "".to_string(),
            resource_id: "".to_string(),
        }
    }

    pub fn to_longest_path(&self) -> String {
        let loc_str = vec![self.region_id.as_str(), self.cluster_id.as_str(), self.node_id.as_str()]
            .into_iter()
            .take_while(|x| !x.is_empty())
            .collect::<Vec<&str>>()
            .join(".");
        let rsc_str = vec![
            self.service_type.as_str(),
            self.service_id.as_str(),
            self.resource_type.as_str(),
            self.resource_id.as_str(),
        ]
        .into_iter()
        .take_while(|x| !x.is_empty())
        .collect::<Vec<&str>>()
        .join("/");
        match rsc_str.is_empty() {
            true => format!("{}", loc_str),
            false => format!("{}/{}", loc_str, rsc_str),
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
                self.region_id, self.service_type, self.service_id, self.resource_type, self.resource_id
            )
        }
    }
}

impl SwbusMessageHeader {
    pub fn new(source: ServicePath, destination: ServicePath) -> Self {
        SwbusMessageHeader {
            version: 1,
            epoch: 0,
            flag: 0,
            ttl: 64,
            source: Some(source),
            destination: Some(destination),
        }
    }
}

impl RequestResponse {
    /// Create a new OK response.
    pub fn ok(request_epoch: u64) -> Self {
        RequestResponse {
            request_epoch,
            error_code: SwbusErrorCode::Ok as i32,
            error_message: "".to_string(),
            response_body: None,
        }
    }

    /// Create a new infra error response.
    pub fn infra_error(request_epoch: u64, error_code: SwbusErrorCode, error_message: &str) -> Self {
        RequestResponse {
            request_epoch,
            error_code: error_code as i32,
            error_message: error_message.to_string(),
            response_body: None,
        }
    }
}

impl SwbusMessage {
    /// send response to the sender of the request
    pub fn new_response(
        request: &SwbusMessage,
        my_sp: &ServicePath,
        error_code: SwbusErrorCode,
        error_message: &str,
    ) -> Self {
        let request_response = match error_code {
            SwbusErrorCode::Ok => RequestResponse::ok(request.header.as_ref().unwrap().epoch),
            _ => RequestResponse::infra_error(request.header.as_ref().unwrap().epoch, error_code, error_message),
        };
        SwbusMessage {
            header: Some(SwbusMessageHeader::new(
                my_sp.clone(), //should not happen otherwise it won't reach here
                request
                    .header
                    .as_ref()
                    .unwrap()
                    .source
                    .clone()
                    .expect("missing source service_path"),
            )),
            body: Some(swbus_message::Body::Response(request_response)),
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

impl ManagementRequest {
    pub fn new(request: &str) -> Self {
        ManagementRequest {
            request: request.to_string(),
            arguments: Vec::<ManagementRequestArg>::new(),
        }
    }
}

impl RouteDataRequest {
    pub fn new(payload: Vec<u8>) -> Self {
        RouteDataRequest { payload }
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

        let response = RequestResponse::infra_error(123, SwbusErrorCode::NoRoute, "No route is found.");
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
        let request = RouteDataRequest::new("mock-payload".as_bytes().to_vec());
        test_packing_with_swbus_message(swbus_message::Body::RouteDataRequest(request));
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
    #[test]
    fn test_service_path_to_and_from_string() {
        let service_path = ServicePath {
            region_id: "region-a".to_string(),
            cluster_id: "cluster-a".to_string(),
            node_id: "1.1.1.1-dpu0".to_string(),
            service_type: "".to_string(),
            service_id: "".to_string(),
            resource_type: "".to_string(),
            resource_id: "".to_string(),
        };
        let sp_str = service_path.to_longest_path();
        assert_eq!(sp_str, "region-a.cluster-a.1.1.1.1-dpu0");
        assert_eq!(ServicePath::from_string(sp_str.as_str()).unwrap(), service_path);

        let service_path = ServicePath {
            region_id: "".to_string(),
            cluster_id: "".to_string(),
            node_id: "".to_string(),
            service_type: "hamgrd".to_string(),
            service_id: "0".to_string(),
            resource_type: "".to_string(),
            resource_id: "".to_string(),
        };
        let sp_str = service_path.to_longest_path();
        assert_eq!(sp_str, "/hamgrd/0");
        assert_eq!(ServicePath::from_string(sp_str.as_str()).unwrap(), service_path);
    }
}
