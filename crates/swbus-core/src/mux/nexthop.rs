use super::SwbusConnInfo;
use super::SwbusConnProxy;
use super::SwbusMultiplexer;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::*;
use swbus_proto::swbus::{swbus_message, SwbusMessage};
use tracing::info;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum NextHopType {
    Local,
    Remote,
    Drop,
}
#[derive(Clone)]
pub(crate) struct SwbusNextHop {
    pub nh_type: NextHopType,
    pub conn_info: Option<Arc<SwbusConnInfo>>,
    conn_proxy: Option<SwbusConnProxy>,
    pub hop_count: u32,
}

impl SwbusNextHop {
    pub fn new_remote(conn_info: Arc<SwbusConnInfo>, conn_proxy: SwbusConnProxy, hop_count: u32) -> Self {
        SwbusNextHop {
            nh_type: NextHopType::Remote,
            conn_info: Some(conn_info),
            conn_proxy: Some(conn_proxy),
            hop_count,
        }
    }

    pub fn new_local() -> Self {
        SwbusNextHop {
            nh_type: NextHopType::Local,
            conn_info: None,
            conn_proxy: None,
            hop_count: 0,
        }
    }

    pub fn new_drop() -> Self {
        SwbusNextHop {
            nh_type: NextHopType::Drop,
            conn_info: None,
            conn_proxy: None,
            hop_count: 0,
        }
    }

    pub async fn queue_message(
        &self,
        mux: &SwbusMultiplexer,
        mut message: SwbusMessage,
    ) -> Result<Option<SwbusMessage>> {
        match self.nh_type {
            NextHopType::Drop => self.drop_message(message).await,
            NextHopType::Local => self.process_local_message(mux, message).await,
            NextHopType::Remote => {
                let header: &mut SwbusMessageHeader = message.header.as_mut().expect("missing header"); //should not happen otherwise it won't reach here
                header.ttl -= 1;
                if header.ttl == 0 {
                    let response = SwbusMessage::new_response(
                        &message,
                        Some(&mux.get_my_service_path()),
                        SwbusErrorCode::Unreachable,
                        "TTL expired",
                        mux.generate_message_id(),
                        None,
                    );
                    return Ok(Some(response));
                }
                self.conn_proxy
                    .as_ref()
                    .expect("conn_proxy shouldn't be None in remote nexthop")
                    .try_queue(Ok(message))
                    .await
                    .unwrap();
                Ok(None)
            }
        }
    }

    async fn process_local_message(
        &self,
        mux: &SwbusMultiplexer,
        message: SwbusMessage,
    ) -> Result<Option<SwbusMessage>> {
        // @todo: move to trace
        // process message locally
        let response = match message.body.as_ref() {
            Some(swbus_message::Body::PingRequest(_)) => self.process_ping_request(mux, message).unwrap(),
            Some(swbus_message::Body::ManagementRequest(mgmt_request)) => {
                self.process_mgmt_request(mux, &message, mgmt_request).unwrap()
            }
            _ => {
                return Err(SwbusError::input(
                    SwbusErrorCode::ServiceNotFound,
                    format!("Invalid message type to a local endpoint: {:?}", message),
                ));
            }
        };
        Ok(Some(response))
    }

    fn process_ping_request(&self, mux: &SwbusMultiplexer, message: SwbusMessage) -> Result<SwbusMessage> {
        // @todo: move to trace
        // info!("Received ping request: {:?}", message);
        let id = mux.generate_message_id();
        Ok(SwbusMessage::new_response(
            &message,
            None,
            SwbusErrorCode::Ok,
            "",
            id,
            None,
        ))
    }

    fn process_mgmt_request(
        &self,
        mux: &SwbusMultiplexer,
        message: &SwbusMessage,
        mgmt_request: &ManagementRequest,
    ) -> Result<SwbusMessage> {
        match mgmt_request.request.as_str() {
            "show_route" => {
                let routes = mux.export_routes(None);
                let response_msg = SwbusMessage::new_response(
                    message,
                    None,
                    SwbusErrorCode::Ok,
                    "",
                    mux.generate_message_id(),
                    Some(request_response::ResponseBody::RouteQueryResult(routes)),
                );
                Ok(response_msg)
            }
            _ => Err(SwbusError::input(
                SwbusErrorCode::InvalidArgs,
                format!("Invalid management request: {:?}", mgmt_request),
            )),
        }
    }

    async fn drop_message(&self, message: SwbusMessage) -> Result<Option<SwbusMessage>> {
        // todo: change to trace
        info!("Drop message: {:?}", message);
        // todo: increment drop counter
        Ok(None)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::RouteConfig;
    use crate::mux::SwbusConn;
    use std::sync::Arc;
    use swbus_proto::swbus::SwbusMessage;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_new_remote() {
        let conn_info = Arc::new(SwbusConnInfo::new_client(
            ConnectionType::Cluster,
            "127.0.0.1:8080".parse().unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.1-dpu0").unwrap(),
        ));
        let (message_queue_tx, _) = mpsc::channel(16);
        let conn = SwbusConn::new(&conn_info, message_queue_tx);
        let hop_count = 5;
        let nexthop = SwbusNextHop::new_remote(conn_info.clone(), conn.new_proxy(), hop_count);

        assert_eq!(nexthop.nh_type, NextHopType::Remote);
        assert_eq!(nexthop.conn_info, Some(conn_info));
        assert_eq!(nexthop.hop_count, hop_count);
    }

    #[tokio::test]
    async fn test_new_local() {
        let nexthop = SwbusNextHop::new_local();

        assert_eq!(nexthop.nh_type, NextHopType::Local);
        assert!(nexthop.conn_info.is_none());
        assert!(nexthop.conn_proxy.is_none());
        assert_eq!(nexthop.hop_count, 0);
    }

    #[tokio::test]
    async fn test_new_drop() {
        let nexthop = SwbusNextHop::new_drop();

        assert_eq!(nexthop.nh_type, NextHopType::Drop);
        assert!(nexthop.conn_info.is_none());
        assert!(nexthop.conn_proxy.is_none());
        assert_eq!(nexthop.hop_count, 0);
    }

    #[tokio::test]
    async fn test_queue_message_drop() {
        let nexthop = SwbusNextHop::new_drop();
        let mux = SwbusMultiplexer::default();
        let message = SwbusMessage::default();

        let result = nexthop.queue_message(&mux, message).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_queue_message_local_ping() {
        let nexthop = SwbusNextHop::new_local();
        let mux = Arc::new(SwbusMultiplexer::default());
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
            "destination": "region-a.cluster-a.10.0.0.2-dpu0/local-mgmt/0"
          },
          "body": {
            "PingRequest": {}
          }
        }
        "#;
        let request_msg: SwbusMessage = serde_json::from_str(request).unwrap();

        let result = nexthop.queue_message(&mux, request_msg).await;
        assert!(result.is_ok());
        let response = result.unwrap().unwrap();
        assert_eq!(
            response.header.unwrap().destination,
            Some(ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0").unwrap())
        );
    }

    #[tokio::test]
    async fn test_queue_message_remote_ttl_expired() {
        let conn_info = Arc::new(SwbusConnInfo::new_client(
            ConnectionType::Cluster,
            "127.0.0.1:8080".parse().unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.1-dpu0").unwrap(),
        ));
        let (message_queue_tx, _) = mpsc::channel(16);
        let conn = SwbusConn::new(&conn_info, message_queue_tx);
        let hop_count = 5;
        let nexthop = SwbusNextHop::new_remote(conn_info.clone(), conn.new_proxy(), hop_count);
        let mux = Arc::new(SwbusMultiplexer::default());
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
            "ttl": 1,
            "source": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0",
            "destination": "region-a.cluster-a.10.0.0.3-dpu0/local-mgmt/0"
          },
          "body": {
            "PingRequest": {}
          }
        }
        "#;
        let request_msg: SwbusMessage = serde_json::from_str(request).unwrap();

        let result = nexthop.queue_message(&mux, request_msg).await;
        assert!(result.is_ok());
        let response = result.unwrap().unwrap();
        match response.body.unwrap() {
            swbus_message::Body::Response(response) => {
                assert_eq!(response.error_code, SwbusErrorCode::Unreachable as i32);
                assert_eq!(response.error_message, "TTL expired");
            }
            _ => panic!("Expected response message"),
        }
    }
}
