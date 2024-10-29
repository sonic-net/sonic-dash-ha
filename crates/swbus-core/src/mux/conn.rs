use super::SwbusConnControlMessage;
use super::SwbusConnInfo;
use super::SwbusConnProxy;
use super::SwbusConnWorker;
use super::SwbusMultiplexer;
use crate::contracts::swbus::swbus_service_client::SwbusServiceClient;
use crate::contracts::swbus::*;
use crate::result::*;
use std::io;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use tracing::error;

pub struct SwbusConn {
    info: Arc<SwbusConnInfo>,
    worker_task: tokio::task::JoinHandle<Result<()>>,
    control_queue_tx: mpsc::Sender<SwbusConnControlMessage>,
    message_queue_tx: mpsc::Sender<SwbusMessage>,
}

// Connection operations
impl SwbusConn {
    pub fn info(&self) -> &Arc<SwbusConnInfo> {
        &self.info
    }

    pub fn new_proxy(&self) -> SwbusConnProxy {
        SwbusConnProxy {
            message_queue_tx: self.message_queue_tx.clone(),
        }
    }

    pub async fn start_shutdown(&self) -> Result<()> {
        self.control_queue_tx
            .send(SwbusConnControlMessage::Shutdown)
            .await
            .map_err(|e| SwbusError::internal(SwbusErrorCode::Fail, e.to_string()))
    }
}

// Client factory and task entry
impl SwbusConn {
    pub async fn connect(
        conn_type: ConnectionType,
        server_addr: SocketAddr,
        mux: Arc<SwbusMultiplexer>,
    ) -> Result<SwbusConn> {
        let conn_info = Arc::new(SwbusConnInfo::new_client(conn_type, server_addr));

        let endpoint = Endpoint::from_str(&format!("http://{}", server_addr)).map_err(|e| {
            SwbusError::input(
                SwbusErrorCode::InvalidArgs,
                format!("Failed to create endpoint: {}.", e),
            )
        })?;

        let channel = match endpoint.connect().await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to connect: {}.", e);
                return Err(SwbusError::connection(
                    SwbusErrorCode::ConnectionError,
                    io::Error::new(io::ErrorKind::ConnectionReset, e.to_string()),
                ));
            }
        };

        let client = SwbusServiceClient::new(channel);
        Self::start_client_worker_task(conn_info, client, mux).await
    }

    async fn start_client_worker_task(
        conn_info: Arc<SwbusConnInfo>,
        client: SwbusServiceClient<Channel>,
        mux: Arc<SwbusMultiplexer>,
    ) -> Result<SwbusConn> {
        let (control_queue_tx, control_queue_rx) = mpsc::channel(1);
        let (message_queue_tx, message_queue_rx) = mpsc::channel(16);

        let conn_info_for_worker = conn_info.clone();
        let worker_task = tokio::spawn(async move {
            Self::run_client_worker_task(conn_info_for_worker, client, control_queue_rx, message_queue_rx, mux).await
        });

        Ok(SwbusConn {
            info: conn_info,
            worker_task,
            control_queue_tx,
            message_queue_tx,
        })
    }

    async fn run_client_worker_task(
        conn_info: Arc<SwbusConnInfo>,
        mut client: SwbusServiceClient<Channel>,
        control_queue_rx: mpsc::Receiver<SwbusConnControlMessage>,
        message_queue_rx: mpsc::Receiver<SwbusMessage>,
        mux: Arc<SwbusMultiplexer>,
    ) -> Result<()> {
        let request_stream = ReceiverStream::new(message_queue_rx);
        let stream_message_request = Request::new(request_stream);

        let response_stream = match client.stream_messages(stream_message_request).await {
            Ok(response) => response.into_inner(),
            Err(e) => {
                error!("Failed to establish message streaming: {}.", e);
                return Err(SwbusError::connection(
                    SwbusErrorCode::ConnectionError,
                    io::Error::new(io::ErrorKind::Unsupported, e.to_string()),
                ));
            }
        };

        let mut conn_worker = SwbusConnWorker::new(conn_info, control_queue_rx, response_stream, mux);
        conn_worker.run().await
    }
}
