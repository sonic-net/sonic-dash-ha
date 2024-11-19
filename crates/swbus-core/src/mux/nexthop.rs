use super::SwbusConnInfo;
use super::SwbusConnProxy;
use super::SwbusMultiplexer;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::*;

#[derive(Debug)]
enum NextHopType {
    Local,
    Remote,
}
#[derive(Debug)]
pub(crate) struct SwbusNextHop {
    nh_type: NextHopType,
    conn_info: Option<Arc<SwbusConnInfo>>,
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

    pub async fn queue_message(&self, mut message: SwbusMessage) -> Result<()> {
        match self.nh_type {
            NextHopType::Local => self.process_local_message(message).await,
            NextHopType::Remote => {
                let header: &mut SwbusMessageHeader = message.header.as_mut().expect("missing header"); //should not happen otherwise it won't reach here
                header.ttl -= 1;
                if header.ttl == 0 {
                    //todo: send response back
                    return Err(SwbusError::input(
                        SwbusErrorCode::Unreachable,
                        "Hop count exceeded".to_string(),
                    ));
                }
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
        match message.body {
            Some(swbus_message::Body::PingRequest(ref ping_request)) => {
                self.process_ping_request(message).await?;
            }
            Some(swbus_message::Body::Response(ref response)) => {
                //println!("Received response: {:?}", message);
            }
            _ => {
                return Err(SwbusError::input(
                    SwbusErrorCode::ServiceNotFound,
                    format!("Invalid message type to a local endpoint: {:?}", message),
                ));
            }
        }
        Ok(())
    }

    async fn process_ping_request(&self, message: SwbusMessage) -> Result<()> {
        //@todo: move to trace
        //println!("Received ping request: {:?}", message);
        match message.header {
            Some(ref header) => {
                let response = SwbusMessage {
                    header: Some(SwbusMessageHeader::new(
                        header.destination.clone().expect("missing destination"), //should not happen otherwise it won't reach here
                        header.source.clone().ok_or(SwbusError::input(
                            SwbusErrorCode::InvalidSource,
                            format!("missing message source: {:?}", message),
                        ))?,
                    )),
                    body: Some(swbus_message::Body::Response(RequestResponse::ok(header.epoch))),
                };
                Box::pin(SwbusMultiplexer::get().route_message(response)).await
            }
            None => {
                return Err(SwbusError::input(
                    SwbusErrorCode::InvalidHeader,
                    "Message missing header".to_string(),
                ));
            }
        }
    }
}
