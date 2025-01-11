use swbus_proto::result::*;
use swbus_proto::swbus::SwbusMessage;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tonic::Status;

#[derive(Debug, Clone)]
pub(crate) struct SwbusConnProxy {
    pub send_queue_tx: mpsc::Sender<Result<SwbusMessage, Status>>,
}

impl SwbusConnProxy {
    pub fn new(send_queue_tx: mpsc::Sender<Result<SwbusMessage, Status>>) -> Self {
        SwbusConnProxy { send_queue_tx }
    }

    pub async fn try_queue(&self, message: Result<SwbusMessage, Status>) -> Result<()> {
        let tx = self.send_queue_tx.clone();

        match tx.try_send(message) {
            Ok(_) => Ok(()),
            Err(e) => match e {
                TrySendError::Full(_) => Err(SwbusError::route(SwbusErrorCode::QueueFull, e.to_string())),
                _ => Err(SwbusError::route(SwbusErrorCode::NoRoute, e.to_string())),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn conn_proxy_can_queue_message() {
        let (tx, mut rx) = mpsc::channel(1);
        let proxy = SwbusConnProxy::new(tx);

        let message = SwbusMessage::default();
        proxy.try_queue(Ok(message.clone())).await.unwrap();

        let received = rx.recv().await.unwrap().unwrap();
        assert_eq!(received, message);
    }

    #[tokio::test]
    async fn conn_proxy_should_fail_when_queue_full() {
        let (tx, _rx) = mpsc::channel(1);
        let proxy = SwbusConnProxy::new(tx);

        let message = SwbusMessage::default();
        proxy.try_queue(Ok(message.clone())).await.unwrap();

        // This should fail because the channel is full
        let result = proxy.try_queue(Ok(message.clone())).await;
        assert!(result.is_err());

        let error = result.unwrap_err();
        if let SwbusError::RouteError { code, .. } = error {
            assert_eq!(code, SwbusErrorCode::QueueFull);
        } else {
            panic!("Expected RouteError, got {:?}", error);
        }
    }
}
