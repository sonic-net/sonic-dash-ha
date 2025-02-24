use crate::core_client::SwbusCoreClient;
use crate::message_handler_proxy::SwbusMessageHandlerProxy;
use dashmap::DashMap;
use std::ops::Deref;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::*;
use tokio::sync::mpsc::Receiver;
use tokio::task;
use tracing::error;

/// How private a route is.
///
/// I.e. if it can be accessed from outside the local client pool.
#[derive(PartialEq, Eq, Clone, Copy)]
enum Privacy {
    /// Route can be reached from any swbus client
    Public,

    /// Route can only be reached from the local swbus edge
    Private,
}

#[derive(Default)]
struct RouteMap(DashMap<ServicePath, (SwbusMessageHandlerProxy, Privacy)>);

impl RouteMap {
    fn insert(&self, svc_path: ServicePath, handler: SwbusMessageHandlerProxy, privacy: Privacy) {
        self.0.insert(svc_path, (handler, privacy));
    }

    fn get(&self, svc_path: &ServicePath, message_privacy: Privacy) -> Option<SwbusMessageHandlerProxy> {
        self.0.get(svc_path).and_then(|pair| {
            let (handler, route_privacy) = pair.deref();
            if *route_privacy == Privacy::Private && message_privacy == Privacy::Public {
                None
            } else {
                Some(handler.clone())
            }
        })
    }
}

pub struct SwbusMessageRouter {
    routes: Arc<RouteMap>,

    // Route task related parameters
    route_task: Option<tokio::task::JoinHandle<()>>,
    swbus_client: Option<SwbusCoreClient>,
    local_msg_rx: Option<Receiver<SwbusMessage>>,
    remote_msg_rx: Option<Receiver<SwbusMessage>>,
}

impl SwbusMessageRouter {
    pub fn new(
        swbus_client: SwbusCoreClient,
        local_msg_rx: Receiver<SwbusMessage>,
        remote_msg_rx: Receiver<SwbusMessage>,
    ) -> Self {
        Self {
            routes: Arc::new(RouteMap::default()),
            route_task: None,
            swbus_client: Some(swbus_client),
            local_msg_rx: Some(local_msg_rx),
            remote_msg_rx: Some(remote_msg_rx),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let routes = self.routes.clone();
        let mut local_msg_rx = self.local_msg_rx.take().unwrap();
        let mut remote_msg_rx = self.remote_msg_rx.take().unwrap();
        let mut swbus_client = self.swbus_client.take().unwrap();

        let swbusd_route_task = task::spawn(async move {
            loop {
                let (msg, privacy) = tokio::select! {
                    msg = local_msg_rx.recv() => (msg.unwrap(), Privacy::Private),
                    msg = remote_msg_rx.recv() => (msg.unwrap(), Privacy::Public),
                };

                Self::route_message(&mut swbus_client, &routes, msg, privacy).await;
            }
        });
        self.route_task = Some(swbusd_route_task);

        Ok(())
    }

    pub fn add_route(&self, svc_path: ServicePath, handler: SwbusMessageHandlerProxy) {
        self.routes.insert(svc_path, handler, Privacy::Public);
    }

    pub fn add_private_route(&self, svc_path: ServicePath, handler: SwbusMessageHandlerProxy) {
        self.routes.insert(svc_path, handler, Privacy::Private);
    }

    async fn route_message(
        swbus_client: &mut SwbusCoreClient,
        routes: &RouteMap,
        message: SwbusMessage,
        privacy: Privacy,
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
                if let Some(handler) = routes.get(&$destination, privacy) {
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
