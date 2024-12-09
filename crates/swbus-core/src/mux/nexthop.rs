use super::SwbusConnInfo;
use super::SwbusConnProxy;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::*;

#[derive(Debug)]
pub(crate) struct SwbusNextHop {
    conn_info: Arc<SwbusConnInfo>,
    conn_proxy: SwbusConnProxy,
    hop_count: u32,
}

impl SwbusNextHop {
    pub fn new(conn_info: Arc<SwbusConnInfo>, conn_proxy: SwbusConnProxy, hop_count: u32) -> Self {
        SwbusNextHop {
            conn_info,
            conn_proxy,
            hop_count,
        }
    }

    pub async fn queue_message(&self, message: SwbusMessage) -> Result<()> {
        self.conn_proxy.queue_message(message).await
    }
}
