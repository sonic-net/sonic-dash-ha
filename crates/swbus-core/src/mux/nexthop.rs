use super::SwbusConnInfo;
use super::SwbusConnProxy;
use super::SwbusMultiplexer;
use getset::CopyGetters;
use getset::Getters;
use std::cmp::Ordering;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::*;
use swbus_proto::swbus::{swbus_message, SwbusMessage};
use tracing::*;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum NextHopType {
    Local,
    Remote,
}

#[derive(Clone, Getters, CopyGetters, Debug)]
pub(crate) struct SwbusNextHop {
    #[getset(get_copy = "pub")]
    nh_type: NextHopType,

    #[getset(get = "pub")]
    conn_info: Option<Arc<SwbusConnInfo>>,

    #[getset(get = "pub")]
    conn_proxy: Option<SwbusConnProxy>,

    #[getset(get_copy = "pub")]
    hop_count: u32,
}

impl PartialEq for SwbusNextHop {
    fn eq(&self, other: &Self) -> bool {
        self.hop_count == other.hop_count && self.nh_type == other.nh_type && self.conn_info == other.conn_info
    }
}

impl Eq for SwbusNextHop {}

impl PartialOrd for SwbusNextHop {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SwbusNextHop {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.hop_count.cmp(&other.hop_count) {
            Ordering::Equal => match (&self.conn_info, &other.conn_info) {
                (Some(ref a), Some(ref b)) => a.id().cmp(b.id()),
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (None, None) => Ordering::Equal,
            },
            other => other,
        }
    }
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

    pub fn clone_with_hop_count(&self, hop_count: u32) -> Self {
        SwbusNextHop {
            nh_type: self.nh_type,
            conn_info: self.conn_info.clone(),
            conn_proxy: self.conn_proxy.clone(),
            hop_count,
        }
    }

    /// used in finding existing routes to remove
    pub fn new_dummy_remote(conn_info: Arc<SwbusConnInfo>, hop_count: u32) -> Self {
        SwbusNextHop {
            nh_type: NextHopType::Remote,
            conn_info: Some(conn_info),
            conn_proxy: None,
            hop_count,
        }
    }

    #[instrument(name="queue_message", parent=None, level="debug", skip_all, fields(nh_type=?self.nh_type, conn_info=self.conn_info.as_ref().map(|x| x.id()).unwrap_or(&"None".to_string()), message.id=?message.header.as_ref().unwrap().id))]
    pub async fn queue_message(
        &self,
        mux: &SwbusMultiplexer,
        mut message: SwbusMessage,
    ) -> Result<Option<SwbusMessage>> {
        let current_span = tracing::Span::current();
        debug!("Queue message");
        match self.nh_type {
            NextHopType::Local => {
                self.process_local_message(mux, message)
                    .instrument(current_span.clone())
                    .await
            }
            NextHopType::Remote => {
                let header: &mut SwbusMessageHeader = message.header.as_mut().expect("missing header"); // should not happen otherwise it won't reach here
                header.ttl -= 1;
                if header.ttl == 0 {
                    debug!("TTL expired");
                    let response = SwbusMessage::new_response(
                        message.header.as_ref().unwrap(),
                        Some(mux.get_my_service_path()),
                        SwbusErrorCode::Unreachable,
                        "TTL expired",
                        mux.generate_message_id(),
                        None,
                    );
                    return Ok(Some(response));
                }
                debug!("Sending to the remote endpoint");
                self.conn_proxy
                    .as_ref()
                    .expect("conn_proxy shouldn't be None in remote nexthop")
                    .try_queue(Ok(message))
                    .await?;
                Ok(None)
            }
        }
    }

    async fn process_local_message(
        &self,
        mux: &SwbusMultiplexer,
        message: SwbusMessage,
    ) -> Result<Option<SwbusMessage>> {
        // process message locally
        let dest_sp = message.header.as_ref().unwrap().destination.as_ref().unwrap();
        if !dest_sp.service_type.is_empty() {
            // local nexthop uses swbusd service path. If the dest sp is to a local service and
            // there is no route to the service, the packet will be routed to here. We need to
            // return no route error in this case.
            let response = SwbusMessage::new_response(
                message.header.as_ref().unwrap(),
                None,
                SwbusErrorCode::NoRoute,
                "Route not found",
                mux.generate_message_id(),
                None,
            );
            return Ok(Some(response));
        }
        let response = match message.body.as_ref() {
            Some(swbus_message::Body::PingRequest(_)) => self.process_ping_request(mux, message)?,
            _ => {
                // drop all other messages. This could happen due to message loop or other invaid messages to swbusd.
                debug!("Drop unknown message to a local endpoint");
                return Ok(None);
            }
        };
        Ok(Some(response))
    }

    fn process_ping_request(&self, mux: &SwbusMultiplexer, message: SwbusMessage) -> Result<SwbusMessage> {
        debug!("Received ping request");
        let id = mux.generate_message_id();
        Ok(SwbusMessage::new_response(
            message.header.as_ref().unwrap(),
            None,
            SwbusErrorCode::Ok,
            "",
            id,
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::test_utils::*;
    use crate::mux::SwbusConn;
    use std::sync::Arc;
    use swbus_config::RouteConfig;
    use swbus_proto::swbus::SwbusMessage;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_new_remote() {
        let conn_info = Arc::new(SwbusConnInfo::new_client(
            ConnectionType::InCluster,
            "127.0.0.1:8080".parse().unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap(),
        ));
        let (send_queue_tx, _) = mpsc::channel(16);
        let conn = SwbusConn::new(&conn_info, send_queue_tx);
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
    async fn test_queue_message_drop() {
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: RouteScope::InCluster,
        };

        let mux = Arc::new(SwbusMultiplexer::new(vec![route_config.clone()]));

        let nexthop = SwbusNextHop::new_local();
        let message = SwbusMessage {
            header: Some(SwbusMessageHeader::new(
                ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
                ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
                1,
            )),
            body: None,
        };
        let result = nexthop.queue_message(&mux, message).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_queue_message_local_ping() {
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            scope: RouteScope::InCluster,
        };
        let mux = Arc::new(SwbusMultiplexer::new(vec![route_config.clone()]));

        let nexthop = SwbusNextHop::new_local();

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
            ConnectionType::InCluster,
            "127.0.0.1:8080".parse().unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap(),
        ));
        let (send_queue_tx, _) = mpsc::channel(16);
        let conn = SwbusConn::new(&conn_info, send_queue_tx);
        let hop_count = 5;
        let nexthop = SwbusNextHop::new_remote(conn_info.clone(), conn.new_proxy(), hop_count);

        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            scope: RouteScope::InCluster,
        };

        let mux = Arc::new(SwbusMultiplexer::new(vec![route_config.clone()]));

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

    #[test]
    fn test_clone_with_hop_count() {
        let conn_info = Arc::new(SwbusConnInfo::new_client(
            ConnectionType::InCluster,
            "127.0.0.1:8080".parse().unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap(),
        ));
        let (send_queue_tx, _) = mpsc::channel(16);
        let conn = SwbusConn::new(&conn_info, send_queue_tx);
        let hop_count = 5;
        let nexthop = SwbusNextHop::new_remote(conn_info.clone(), conn.new_proxy(), hop_count);

        let new_hop_count = 10;
        let cloned_nexthop = nexthop.clone_with_hop_count(new_hop_count);

        assert_eq!(cloned_nexthop.nh_type, NextHopType::Remote);
        assert_eq!(cloned_nexthop.conn_info, Some(conn_info));
        assert_eq!(cloned_nexthop.hop_count, new_hop_count);
    }

    #[test]
    fn test_nexthop_ord_and_eq() {
        let (conn1, _) =
            new_conn_for_test_with_endpoint(ConnectionType::InCluster, "region-a.cluster-a.node0", "127.0.0.1:61000");
        let (conn2, _) =
            new_conn_for_test_with_endpoint(ConnectionType::InCluster, "region-a.cluster-a.node0", "127.0.0.1:61001");

        let nh1 = SwbusNextHop::new_remote(conn1.info().clone(), conn1.new_proxy(), 1);
        let nh2 = SwbusNextHop::new_remote(conn2.info().clone(), conn2.new_proxy(), 1);
        let nh3 = SwbusNextHop::new_remote(conn2.info().clone(), conn2.new_proxy(), 2);

        assert!(nh1 != nh2);
        assert!(nh1 < nh2 && nh2 < nh3);

        #[allow(clippy::mutable_key_type)]
        let mut nhs = std::collections::BTreeSet::new();
        assert!(nhs.insert(nh1.clone()));
        assert!(nhs.contains(&nh1));
        assert!(nhs.insert(nh2.clone()));
        assert!(nhs.contains(&nh2));
    }
}
