use crate::core_client::SwbusCoreClient;
use crate::message_handler_proxy::SwbusMessageHandlerProxy;
use crate::message_router::SwbusMessageRouter;
use std::io;
use swbus_proto::result::*;
use swbus_proto::swbus::*;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Sender;
use tracing::info;

pub(crate) const SWBUS_RECV_QUEUE_SIZE: usize = 10000;

pub struct SwbusEdgeRuntime {
    swbus_uri: String,
    message_router: SwbusMessageRouter,
    sender_to_message_router: Sender<SwbusMessage>,
}

impl SwbusEdgeRuntime {
    pub fn new(swbus_uri: String, sp: ServicePath) -> Self {
        let (recv_queue_tx, recv_queue_rx) = channel::<SwbusMessage>(SWBUS_RECV_QUEUE_SIZE);
        let sender_to_message_router = recv_queue_tx.clone();
        let swbus_client = SwbusCoreClient::new(swbus_uri.clone(), sp, recv_queue_tx);
        let message_router = SwbusMessageRouter::new(swbus_client, recv_queue_rx);

        Self {
            swbus_uri,
            message_router,
            sender_to_message_router,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("Starting edge runtime with URI: {}", self.swbus_uri);
        self.message_router.start().await
    }

    pub fn add_handler(&self, svc_path: ServicePath, handler_tx: Sender<SwbusMessage>) -> Result<()> {
        // Create MessageHandlerProxy
        let proxy = SwbusMessageHandlerProxy::new(handler_tx);

        self.message_router.add_route(svc_path, proxy);
        Ok(())
    }

    pub async fn send(&self, message: SwbusMessage) -> Result<()> {
        // Send message to the message router
        match self.sender_to_message_router.send(message).await {
            Ok(_) => Ok(()),
            Err(e) => Err(SwbusError::connection(
                SwbusErrorCode::ConnectionError,
                io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    format!("Message router channel is broken: {}", e),
                ),
            )),
        }
    }
}
