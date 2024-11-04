use crate::core_client::SwbusCoreClient;
use crate::message_handler_proxy::SwbusMessageHandlerProxy;
use dashmap::DashMap;
use std::sync::Arc;
use swbus_proto::swbus::*;
use tokio::sync::mpsc::Receiver;
use tokio::task;

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
}

impl SwbusMessageRouter {
    pub async fn start(&mut self) {
        let routes_clone = self.routes.clone();
        let recv_rx = self.recv_rx.take().unwrap();
        let swbus_client = self.swbus_client.take().unwrap();

        let route_task = task::spawn(async move {
            let routes = routes_clone;
            let mut swbus_client = swbus_client;
            let mut recv_rx = recv_rx;
            while let Some(message) = recv_rx.recv().await {
                // Route the message to the appropriate handler.
                Self::route_message(&mut swbus_client, &routes, message).await;
            }
        });
        self.route_task = Some(route_task);
    }

    async fn route_message(
        swbus_client: &mut SwbusCoreClient,
        routes: &Arc<DashMap<ServicePath, SwbusMessageHandlerProxy>>,
        message: SwbusMessage,
    ) {
        // Route the message via routes, then default to the core client.
        todo!()
    }
}
