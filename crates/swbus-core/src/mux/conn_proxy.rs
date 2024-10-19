use crate::contracts::swbus::*;
use crate::result::*;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub(crate) struct SwbusConnProxy {
    pub message_queue_tx: mpsc::Sender<SwbusMessage>,
}

impl SwbusConnProxy {
    pub fn new(message_queue_tx: mpsc::Sender<SwbusMessage>) -> Self {
        SwbusConnProxy { message_queue_tx }
    }

    pub async fn queue_message(&self, message: SwbusMessage) -> Result<()> {
        let tx = self.message_queue_tx.clone();

        tx.try_send(message)
            .await
            .map_err(|e| SwbusError::internal(SwbusErrorCode::Fail, e.to_string()))
    }
}
