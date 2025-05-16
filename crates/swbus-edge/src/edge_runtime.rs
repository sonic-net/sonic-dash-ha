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
    //base service path with service type and service id
    base_sp: ServicePath,
}

impl SwbusEdgeRuntime {
    pub fn new(swbus_uri: String, sp: ServicePath) -> Self {
        let (local_msg_tx, local_msg_rx) = channel(SWBUS_RECV_QUEUE_SIZE);
        let (remote_msg_tx, remote_msg_rx) = channel(SWBUS_RECV_QUEUE_SIZE);
        let base_sp = sp.clone();
        let swbus_client = SwbusCoreClient::new(swbus_uri.clone(), sp, remote_msg_tx);
        let message_router = SwbusMessageRouter::new(swbus_client, local_msg_rx, remote_msg_rx);

        Self {
            swbus_uri,
            message_router,
            sender_to_message_router: local_msg_tx,
            base_sp,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("Starting edge runtime with URI: {}", self.swbus_uri);
        self.message_router.start().await
    }

    pub fn new_sp(&self, resource_type: &str, resource_id: &str) -> ServicePath {
        let mut new_sp = self.base_sp.clone();
        new_sp.resource_type = resource_type.to_string();
        new_sp.resource_id = resource_id.to_string();
        new_sp
    }

    /// Add handler that can be reached from any swbus client.
    pub fn add_handler(&self, svc_path: ServicePath, handler_tx: Sender<SwbusMessage>) {
        let proxy = SwbusMessageHandlerProxy::new(handler_tx);
        self.message_router.add_route(svc_path, proxy);
    }

    /// Add handler that can only be reached from within this edge runtime.
    pub fn add_private_handler(&self, svc_path: ServicePath, handler_tx: Sender<SwbusMessage>) {
        let proxy = SwbusMessageHandlerProxy::new(handler_tx);
        self.message_router.add_private_route(svc_path, proxy);
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
