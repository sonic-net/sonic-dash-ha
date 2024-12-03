use super::SwbusConnInfo;
use super::SwbusConnProxy;
use super::SwbusMultiplexer;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::*;

#[derive(Debug, Clone)]
pub(crate) enum NextHopType {
    Local,
    Remote,
}
#[derive(Clone)]
pub(crate) struct SwbusNextHop {
    pub nh_type: NextHopType,
    pub conn_info: Option<Arc<SwbusConnInfo>>,
    conn_proxy: Option<SwbusConnProxy>,
    pub hop_count: u32,
    mux: Arc<SwbusMultiplexer>,
}

impl SwbusNextHop {
    pub fn new_remote(
        mux: &Arc<SwbusMultiplexer>,
        conn_info: Arc<SwbusConnInfo>,
        conn_proxy: SwbusConnProxy,
        hop_count: u32,
    ) -> Self {
        SwbusNextHop {
            nh_type: NextHopType::Remote,
            conn_info: Some(conn_info),
            conn_proxy: Some(conn_proxy),
            hop_count,
            mux: mux.clone(),
        }
    }

    pub fn new_local(mux: &Arc<SwbusMultiplexer>) -> Self {
        SwbusNextHop {
            nh_type: NextHopType::Local,
            conn_info: None,
            conn_proxy: None,
            hop_count: 0,
            mux: mux.clone(),
        }
    }

    pub async fn queue_message(&self, message: SwbusMessage) -> Result<()> {
        match self.nh_type {
            NextHopType::Local => self.process_local_message(message).await,
            NextHopType::Remote => {
                self.conn_proxy
                    .as_ref()
                    .expect("conn_proxy shouldn't be None in remote nexthop")
                    .queue_message(Ok(message))
                    .await
            }
        }
    }

    async fn process_local_message(&self, message: SwbusMessage) -> Result<()> {
        //@todo: move to trace
        // process message locally
        let response = match message.body.as_ref() {
            Some(swbus_message::Body::PingRequest(_)) => self.process_ping_request(message).unwrap(),
            Some(swbus_message::Body::ManagementRequest(mgmt_request)) => {
                self.process_mgmt_request(&message, &mgmt_request).unwrap()
            }
            _ => {
                return Err(SwbusError::input(
                    SwbusErrorCode::ServiceNotFound,
                    format!("Invalid message type to a local endpoint: {:?}", message),
                ));
            }
        };
        Box::pin(self.mux.route_message(response)).await?;
        Ok(())
    }

    fn process_ping_request(&self, message: SwbusMessage) -> Result<SwbusMessage> {
        //@todo: move to trace
        //println!("Received ping request: {:?}", message);
        let id = self.mux.generate_message_id();
        Ok(SwbusMessage::new_response(
            &message,
            None,
            SwbusErrorCode::Ok,
            "",
            id,
            None,
        ))
    }

    fn process_mgmt_request(&self, message: &SwbusMessage, mgmt_request: &ManagementRequest) -> Result<SwbusMessage> {
        match mgmt_request.request.as_str() {
            "show_route" => {
                let routes = self.mux.export_routes(None);
                let response_msg = SwbusMessage::new_response(
                    &message,
                    None,
                    SwbusErrorCode::Ok,
                    "",
                    self.mux.generate_message_id(),
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
}
