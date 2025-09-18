use crate::core_client::SwbusCoreClient;
use crate::message_handler_proxy::SwbusMessageHandlerProxy;
use crate::message_router::SwbusMessageRouter;
use crate::RuntimeEnv;
use std::io;
use std::sync::Arc;
use std::sync::RwLock;
use swbus_proto::result::*;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock as AsyncRwLock;
use tracing::info;
pub(crate) const SWBUS_RECV_QUEUE_SIZE: usize = 10000;

pub struct SwbusEdgeRuntime {
    swbus_uri: String,
    message_router: SwbusMessageRouter,
    sender_to_message_router: Sender<SwbusMessage>,
    //base service path with service type and service id
    base_sp: ServicePath,
    runtime_env: RwLock<Option<Box<dyn RuntimeEnv>>>,
    tx_to_swbusd: Arc<AsyncRwLock<Option<mpsc::Sender<SwbusMessage>>>>,
}

impl SwbusEdgeRuntime {
    pub fn new(swbus_uri: String, sp: ServicePath, conn_type: ConnectionType) -> Self {
        assert!(conn_type == ConnectionType::Client || conn_type == ConnectionType::InNode);
        let (local_msg_tx, local_msg_rx) = channel(SWBUS_RECV_QUEUE_SIZE);
        let (remote_msg_tx, remote_msg_rx) = channel(SWBUS_RECV_QUEUE_SIZE);
        let base_sp = sp.clone();
        let swbus_client = SwbusCoreClient::new(swbus_uri.clone(), sp, remote_msg_tx, conn_type);
        let tx_to_swbusd = swbus_client.send_queue_tx.clone();
        let message_router = SwbusMessageRouter::new(swbus_client, local_msg_rx, remote_msg_rx);

        Self {
            swbus_uri,
            message_router,
            sender_to_message_router: local_msg_tx,
            base_sp,
            runtime_env: RwLock::new(None),
            tx_to_swbusd,
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

    pub fn get_base_sp(&self) -> ServicePath {
        self.base_sp.clone()
    }

    /// Add handler that can be reached from any swbus client.
    pub fn add_handler(&self, svc_path: ServicePath, handler_tx: Sender<SwbusMessage>) {
        let proxy = SwbusMessageHandlerProxy::new(handler_tx);
        info!("Added handler for service path: {}", svc_path.to_longest_path());
        self.message_router.add_route(svc_path, proxy);
    }

    /// Add handler that can only be reached from within this edge runtime.
    pub fn add_private_handler(&self, svc_path: ServicePath, handler_tx: Sender<SwbusMessage>) {
        let proxy = SwbusMessageHandlerProxy::new(handler_tx);
        info!("Added private handler for service path: {}", svc_path.to_longest_path());
        self.message_router.add_private_route(svc_path, proxy);
    }

    /// Remove handler by ServicePath.
    pub fn remove_handler(&self, svc_path: &ServicePath) -> bool {
        match self.message_router.remove_route(svc_path) {
            Some(_) => {
                info!("Removed handler for service path: {}", svc_path.to_longest_path());
                true
            }
            None => {
                info!(
                    "No handler found to remove for service path: {}",
                    svc_path.to_longest_path()
                );
                false
            }
        }
    }

    /// Check if a handler exists for the given ServicePath.
    pub fn has_handler(&self, svc_path: &ServicePath) -> bool {
        self.message_router.has_route(svc_path)
    }

    pub async fn send(&self, message: SwbusMessage) -> Result<()> {
        // Send message to the message router
        match self.sender_to_message_router.send(message).await {
            Ok(_) => Ok(()),
            Err(e) => Err(SwbusError::connection(
                SwbusErrorCode::ConnectionError,
                io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    format!("Message router channel is broken: {e}"),
                ),
            )),
        }
    }

    pub fn get_runtime_env(&self) -> std::sync::RwLockReadGuard<'_, Option<Box<dyn RuntimeEnv>>> {
        self.runtime_env.read().unwrap()
    }

    pub fn set_runtime_env(&mut self, runtime_env: Box<dyn RuntimeEnv>) {
        let mut guard = self.runtime_env.write().unwrap();
        if guard.is_none() {
            *guard = Some(runtime_env);
        }
    }

    pub async fn swbusd_connected(&self) -> bool {
        self.tx_to_swbusd.read().await.is_some()
    }
}

#[cfg(test)]
mod tests {
    use crate::SwbusEdgeRuntime;
    use rand::Rng;
    use serde_yaml;
    use sonic_common::log::init_logger_for_test;
    use std::future::Future;
    use std::sync::Arc;
    use swbus_config::SwbusConfig;
    use swbus_core::mux::service::SwbusServiceHost;
    use swbus_proto::swbus::*;
    use tokio::sync::mpsc::{self, Receiver, Sender};
    use tokio::sync::oneshot;
    use tokio::time::{self, timeout, Duration, Instant};

    fn make_swbusd_config() -> SwbusConfig {
        // generate a random port
        let mut rng = rand::thread_rng();
        let port = rng.gen_range(1000..65535);

        let config = format!(
            r#"
        endpoint: "127.0.0.1:{port}"
        routes:
          - key: "region-a.cluster-a.10.0.1.0-dpu0"
            scope: "InCluster"
        peers:
        "#
        );
        serde_yaml::from_str(&config).unwrap()
    }

    fn start_standalone_swbusd(swbus_cfg: SwbusConfig) -> oneshot::Sender<()> {
        // start a standalone swbusd
        let mut service_host = SwbusServiceHost::new(&swbus_cfg.endpoint);
        let shutdown_handle = service_host.take_shutdown_sender().unwrap();
        tokio::spawn(async move {
            let _ = service_host.start(swbus_cfg).await;
        });
        shutdown_handle
    }

    async fn wait_runtime_until<'a, F, Fut>(
        runtime: Arc<SwbusEdgeRuntime>,
        condition: F,
        timeout: u16,
    ) -> Result<(), String>
    where
        F: Fn(Arc<SwbusEdgeRuntime>) -> Fut,
        Fut: Future<Output = bool> + 'a,
        F: 'a,
    {
        let start = Instant::now();
        // wait until swbusd is connected
        loop {
            if condition(runtime.clone()).await {
                return Ok(());
            }
            if start.elapsed() > Duration::from_secs(timeout as u64) {
                return Err("swbusd is not connected".to_string());
            }
            time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn make_a_handler(sp: &str) -> (ServicePath, Receiver<SwbusMessage>, Sender<SwbusMessage>) {
        let (send_queue_tx, send_queue_rx) = mpsc::channel(16);
        (ServicePath::from_string(sp).unwrap(), send_queue_rx, send_queue_tx)
    }

    #[tokio::test]
    async fn test_swbusd_reconnect() {
        // enable trace logging if ENABLE_TRACE env is set
        init_logger_for_test();

        let swbus_config = make_swbusd_config();
        // start swbusd
        let shut_hdl = start_standalone_swbusd(swbus_config.clone());

        // start a swbus edge
        let mut sp = swbus_config.routes[0].key.clone();

        sp.service_type = "swbus-edge".to_string();
        sp.service_id = "test".to_string();
        let mut runtime = SwbusEdgeRuntime::new(
            format!("http://{}", swbus_config.endpoint),
            sp.clone(),
            ConnectionType::InNode,
        );

        runtime.start().await.unwrap();
        let runtime = Arc::new(runtime);

        // wait until swbusd is connected
        wait_runtime_until(runtime.clone(), |x| async move { x.swbusd_connected().await }, 10)
            .await
            .expect("swbusd is not connected");

        // shut swbusd and wait until connection is lost
        shut_hdl.send(()).expect("Failed to send shutdown signal");
        wait_runtime_until(runtime.clone(), |x| async move { !x.swbusd_connected().await }, 10)
            .await
            .expect("swbusd is still connected");

        // restart swbusd and wait until reconnected
        let shut_hdl = start_standalone_swbusd(swbus_config.clone());
        wait_runtime_until(runtime.clone(), |x| async move { x.swbusd_connected().await }, 10)
            .await
            .expect("swbusd is not reconnected");
        shut_hdl.send(()).expect("Failed to send shutdown signal");
    }

    #[tokio::test]
    async fn test_routing_to_handler() {
        // enable trace logging if ENABLE_TRACE env is set
        init_logger_for_test();
        let swbus_config = make_swbusd_config();

        // start a swbus edge
        let mut sp = swbus_config.routes[0].key.clone();

        sp.service_type = "swbus-edge".to_string();
        sp.service_id = "test".to_string();
        let mut runtime =
            SwbusEdgeRuntime::new(format!("http://{}", swbus_config.endpoint), sp, ConnectionType::InNode);
        runtime.start().await.unwrap();

        let base_sp = swbus_config.routes[0].key.to_swbusd_service_path().to_longest_path();

        let mut handlers = vec![];
        let (sp, send_queue_rx, send_queue_tx) =
            make_a_handler(format!("{}{}", base_sp, "/swbus-edge/test/r/1").as_str());
        runtime.add_handler(sp.clone(), send_queue_tx);
        handlers.push((sp, send_queue_rx));

        let (sp, send_queue_rx, send_queue_tx) =
            make_a_handler(format!("{}{}", base_sp, "/swbus-edge/test/r").as_str());
        runtime.add_handler(sp.clone(), send_queue_tx);
        handlers.push((sp, send_queue_rx));

        let (sp, send_queue_rx, send_queue_tx) = make_a_handler(format!("{}{}", base_sp, "/swbus-edge/test").as_str());
        runtime.add_handler(sp.clone(), send_queue_tx);
        handlers.push((sp, send_queue_rx));

        let (sp, send_queue_rx, send_queue_tx) = make_a_handler(format!("{}{}", base_sp, "/swbus-edge").as_str());
        runtime.add_handler(sp.clone(), send_queue_tx);
        handlers.push((sp, send_queue_rx));

        let (sp, send_queue_rx, send_queue_tx) = make_a_handler(&base_sp);
        runtime.add_handler(sp.clone(), send_queue_tx);
        handlers.push((sp, send_queue_rx));

        for (sp, ref mut rx) in handlers {
            let msg = SwbusMessage {
                header: Some(SwbusMessageHeader {
                    source: Some(sp.clone()),
                    destination: Some(sp.clone()),
                    ..Default::default()
                }),
                ..Default::default()
            };

            runtime.send(msg).await.unwrap();
            let recv_msg = timeout(Duration::from_secs(1), rx.recv()).await.unwrap().unwrap();
            assert_eq!(recv_msg.header.unwrap().destination.unwrap(), sp);
        }
    }
}
