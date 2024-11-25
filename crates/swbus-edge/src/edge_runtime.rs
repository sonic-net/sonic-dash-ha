use crate::core_client::SwbusCoreClient;
use crate::message_router::SwbusMessageRouter;
use swbus_proto::result::*;
use swbus_proto::swbus::*;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Sender;

pub(crate) const SWBUS_RECV_QUEUE_SIZE: usize = 10000;

pub struct SwbusEdgeRuntime {
    swbus_uri: String,
    message_router: SwbusMessageRouter,
}

impl SwbusEdgeRuntime {
    pub fn new(swbus_uri: String) -> Self {
        let (recv_queue_tx, recv_queue_rx) = channel::<SwbusMessage>(SWBUS_RECV_QUEUE_SIZE);
        let swbus_client = SwbusCoreClient::new(swbus_uri.clone(), recv_queue_tx);
        let message_router = SwbusMessageRouter::new(swbus_client, recv_queue_rx);

        Self {
            swbus_uri,
            message_router,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        self.message_router.start().await
    }

    pub async fn add_handler(&self, svc_path: ServicePath, handler_tx: Sender<SwbusMessage>) -> Result<()> {
        todo!()
    }

    pub async fn send(&self, message: SwbusMessage) -> Result<()> {
        todo!()
    }
}
