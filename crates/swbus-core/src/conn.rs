use crate::conn_info::*;
use crate::conn_worker::SwbusConnControlMessage;
use crate::conn_worker::SwbusConnWorker;
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
use tracing::{error, info};

pub struct SwbusConn {
    info: Arc<SwbusConnInfo>,
    worker_task: tokio::task::JoinHandle<Result<()>>,
    control_queue_tx: mpsc::Sender<SwbusConnControlMessage>,
    message_queue_tx: mpsc::Sender<SwbusMessage>,
}

// Connection operations
impl SwbusConn {
    pub async fn start_shutdown(&self) -> Result<()> {
        self.control_queue_tx
            .send(SwbusConnControlMessage::Shutdown)
            .await
            .map_err(|e| SwbusError::internal(SwbusErrorCode::Fail, e.to_string()))
    }

    pub async fn queue_message(&self, message: SwbusMessage) -> Result<()> {
        self.message_queue_tx
            .send(message)
            .await
            .map_err(|e| SwbusError::internal(SwbusErrorCode::Fail, e.to_string()))
    }
}

// Client factory
impl SwbusConn {
    pub async fn connect(conn_type: ConnectionType, server_addr: SocketAddr) -> Result<SwbusConn> {
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
        let (worker_task, control_queue_tx, message_queue_tx) =
            Self::start_client_worker_task(conn_info.clone(), client).await;

        Ok(SwbusConn {
            info: conn_info,
            worker_task,
            control_queue_tx,
            message_queue_tx,
        })
    }

    async fn start_client_worker_task(
        conn_info: Arc<SwbusConnInfo>,
        mut client: SwbusServiceClient<Channel>,
    ) -> (
        JoinHandle<Result<()>>,
        mpsc::Sender<SwbusConnControlMessage>,
        mpsc::Sender<SwbusMessage>,
    ) {
        let (control_queue_tx, control_queue_rx) = mpsc::channel(1);
        let (message_queue_tx, message_queue_rx) = mpsc::channel(16);

        let worker_task = tokio::spawn(async move {
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

            SwbusConnWorker::run(conn_info, control_queue_rx, response_stream).await
        });

        (worker_task, control_queue_tx, message_queue_tx)
    }
}
