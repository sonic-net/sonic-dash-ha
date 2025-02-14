use crate::core_client::SwbusCoreClient;
use crate::message_handler_proxy::SwbusMessageHandlerProxy;
use dashmap::DashMap;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::*;
use tokio::sync::mpsc::Receiver;
use tokio::task;
use tracing::error;

pub struct SwbusMessageRouter {
    routes: Arc<DashMap<ServicePath, SwbusMessageHandlerProxy>>,

    // Route task related parameters
    route_task: Option<tokio::task::JoinHandle<()>>,
    swbus_client: Option<SwbusCoreClient>,
    recv_rx: Option<Receiver<SwbusMessage>>,
}

impl SwbusMessageRouter {
    pub fn new(swbus_client: SwbusCoreClient, recv_rx: Receiver<SwbusMessage>) -> Self {
        Self {
            routes: Arc::new(DashMap::new()),
            route_task: None,
            swbus_client: Some(swbus_client),
            recv_rx: Some(recv_rx),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let routes = self.routes.clone();
        let mut recv_rx = self.recv_rx.take().unwrap();
        let mut swbus_client = self.swbus_client.take().unwrap();

        let route_task = task::spawn(async move {
            while let Some(message) = recv_rx.recv().await {
                // Route the received message from core_client to the appropriate handler.
                Self::route_message(&mut swbus_client, &routes, message).await;
            }
        });
        self.route_task = Some(route_task);

        Ok(())
    }

    pub fn add_route(&self, svc_path: ServicePath, handler: SwbusMessageHandlerProxy) {
        self.routes.insert(svc_path, handler);
    }

    async fn route_message(
        swbus_client: &mut SwbusCoreClient,
        routes: &DashMap<ServicePath, SwbusMessageHandlerProxy>,
        message: SwbusMessage,
    ) {
        // Route the message via routes, then default to the core client.
        let Some(header) = &message.header else {
            error!("Missing message header");
            return;
        };
        let Some(destination) = &header.destination else {
            error!("Missing message destination");
            return;
        };

        macro_rules! send_to {
            ($recipient:expr) => {{
                if let Err(e) = $recipient.send(message).await {
                    error!("Failed to send message to {}: {:?}", stringify!($recipient), e);
                }
            }};
        }

        macro_rules! try_route {
            ($destination:expr) => {{
                if let Some(handler) = routes.get(&$destination) {
                    send_to!(handler);
                    return;
                }
            }};
        }

        // Try full address
        try_route!(destination);

        // Try stripping the resource id
        let mut partial_dest = destination.clone();
        partial_dest.resource_id = String::new();
        try_route!(partial_dest);

        // Try stripping the resource type
        partial_dest.resource_type = String::new();
        try_route!(partial_dest);

        // Try stripping the service id
        partial_dest.service_id = String::new();
        try_route!(partial_dest);

        // Try stripping the service type
        partial_dest.service_type = String::new();
        try_route!(partial_dest);

        // Give up at this point and send out to swbus
        send_to!(swbus_client);
    }
}
