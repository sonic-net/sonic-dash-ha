use swbus_proto::result::*;
use swbus_proto::swbus::SwbusMessage;
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
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_queue_message_success() {
        let (tx, mut rx) = mpsc::channel(1);
        let proxy = SwbusConnProxy::new(tx);

        let message = SwbusMessage::default();
        proxy.queue_message(Ok(message.clone())).await.unwrap();

        let received = rx.recv().await.unwrap().unwrap();
        assert_eq!(received, message);
    }

    #[tokio::test]
    async fn test_queue_message_failure() {
        let (tx, _rx) = mpsc::channel(1);
        let proxy = SwbusConnProxy::new(tx);

        let message = Ok(SwbusMessage::default());
        proxy.queue_message(message.clone()).await.unwrap();

        // This should fail because the channel is full
        let result = proxy.queue_message(message.clone()).await;
        assert!(result.is_err());
    }
}
