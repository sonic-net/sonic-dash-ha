use swbus_proto::result::*;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tonic::Status;

#[derive(Debug, Clone)]
pub(crate) struct SwbusConnProxy {
    pub message_queue_tx: mpsc::Sender<Result<SwbusMessage, Status>>,
}

impl SwbusConnProxy {
    pub fn new(message_queue_tx: mpsc::Sender<Result<SwbusMessage, Status>>) -> Self {
        SwbusConnProxy { message_queue_tx }
    }

    pub async fn queue_message(&self, message: Result<SwbusMessage, Status>) -> Result<()> {
        let tx = self.message_queue_tx.clone();

        tx.try_send(message)
            .map_err(|e| SwbusError::internal(SwbusErrorCode::Fail, e.to_string()))
    }
}
