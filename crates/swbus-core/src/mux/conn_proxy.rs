use crate::conn_info::*;
use crate::conn_worker::SwbusConnControlMessage;
use crate::contracts::swbus::*;
use crate::Result;
use crate::{SwbusConn, SwbusError};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug)]
pub(crate) struct SwbusConnProxy {
    pub info: Arc<SwbusConnInfo>,
    pub message_queue_tx: mpsc::Sender<SwbusMessage>,
}

impl SwbusConnProxy {
    pub fn info(&self) -> &Arc<SwbusConnInfo> {
        &self.info
    }

    pub async fn queue_message(&self, message: SwbusMessage) -> Result<()> {
        let tx = self.message_queue_tx.clone();

        tx.send(message)
            .await
            .map_err(|e| SwbusError::internal(SwbusErrorCode::Fail, e.to_string()))
    }
}
