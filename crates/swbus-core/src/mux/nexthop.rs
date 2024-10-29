use super::SwbusConnInfo;
use super::SwbusConnProxy;
use crate::contracts::swbus::*;
use std::sync::Arc;

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

    pub async fn queue_message(&self, message: SwbusMessage) -> crate::Result<()> {
        self.conn_proxy.queue_message(message).await
    }
}
