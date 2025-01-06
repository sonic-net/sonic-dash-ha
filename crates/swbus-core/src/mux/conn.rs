use super::conn_store::SwbusConnStore;
use super::SwbusConnInfo;
use super::SwbusConnProxy;
use super::SwbusConnWorker;
use super::SwbusMultiplexer;
use std::io;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::swbus_service_client::SwbusServiceClient;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status, Streaming};
use tracing::error;

#[derive(Debug)]
pub struct SwbusConn {
    // Connection information
    info: Arc<SwbusConnInfo>,

    // Worker task
    worker_task: Option<tokio::task::JoinHandle<Result<()>>>,
    shutdown_ct: CancellationToken,

    // Data message queue
    message_queue_tx: mpsc::Sender<Result<SwbusMessage, Status>>,
}

// Connection operations
impl SwbusConn {
    pub(crate) fn new(
        conn_info: &Arc<SwbusConnInfo>,
        message_queue_tx: mpsc::Sender<Result<SwbusMessage, Status>>,
    ) -> SwbusConn {
        SwbusConn {
            info: conn_info.clone(),
            worker_task: None,
            shutdown_ct: CancellationToken::new(),
            message_queue_tx,
        }
    }

    pub fn info(&self) -> &Arc<SwbusConnInfo> {
        &self.info
    }

    pub(crate) fn new_proxy(&self) -> SwbusConnProxy {
        SwbusConnProxy::new(self.message_queue_tx.clone())
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.shutdown_ct.cancel();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        conn_info: &Arc<SwbusConnInfo>,
    ) -> (SwbusConn, mpsc::Receiver<Result<SwbusMessage, Status>>) {
        let (message_queue_tx, message_queue_rx) = mpsc::channel(1);
        let conn = SwbusConn::new(conn_info, message_queue_tx);
        (conn, message_queue_rx)
    }
}

// Client-side connection factory and task entry
impl SwbusConn {
    pub async fn connect(
        conn_info: Arc<SwbusConnInfo>,
        mux: Arc<SwbusMultiplexer>,
        conn_store: Arc<SwbusConnStore>,
    ) -> Result<SwbusConn> {
        let endpoint = Endpoint::from_str(&format!("http://{}", conn_info.remote_addr())).map_err(|e| {
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
        Self::start_client_worker_task(conn_info, client, mux, conn_store).await
    }

    async fn start_client_worker_task(
        conn_info: Arc<SwbusConnInfo>,
        client: SwbusServiceClient<Channel>,
        mux: Arc<SwbusMultiplexer>,
        conn_store: Arc<SwbusConnStore>,
    ) -> Result<SwbusConn> {
        let (message_queue_tx, message_queue_rx) = mpsc::channel(16);

        let mut conn = SwbusConn::new(&conn_info, message_queue_tx);

        let conn_info_for_worker = conn.info().clone();
        let shutdown_ct_for_worker = conn.shutdown_ct.clone();
        let worker_task = tokio::spawn(async move {
            Self::run_client_worker_task(
                conn_info_for_worker,
                client,
                shutdown_ct_for_worker,
                message_queue_rx,
                mux,
                conn_store,
            )
            .await
        });
        conn.worker_task = Some(worker_task);

        Ok(conn)
    }

    /// This function is the entry point for the client worker task.
    /// It creates a stream of messages from the message queue and sends it to the server.
    /// It also receives messages from the server and forwards them to the message queue.
    ///
    /// parameters:
    /// - conn_info: The connection information.
    /// - client: The SwbusServiceClient.
    /// - control_queue_rx: The control message queue
    /// - message_queue_rx: The outgoing message queue rx end.
    async fn run_client_worker_task(
        conn_info: Arc<SwbusConnInfo>,
        mut client: SwbusServiceClient<Channel>,
        shutdown_ct: CancellationToken,
        message_queue_rx: mpsc::Receiver<Result<SwbusMessage, Status>>,
        mux: Arc<SwbusMultiplexer>,
        conn_store: Arc<SwbusConnStore>,
    ) -> Result<()> {
        let request_stream = ReceiverStream::new(message_queue_rx)
            .map(|result| result.expect("Not expecting grpc client adding messages with error status"));

        let mut stream_message_request = Request::new(request_stream);

        let sp_str = conn_info
            .local_service_path()
            .expect("missing local service path")
            .to_string();

        let meta = stream_message_request.metadata_mut();

        meta.insert(
            SWBUS_CLIENT_SERVICE_PATH,
            MetadataValue::from_str(sp_str.as_str()).unwrap(),
        );
        meta.insert(
            SWBUS_CONNECTION_TYPE,
            MetadataValue::from_str(conn_info.connection_type().as_str_name()).unwrap(),
        );

        let incoming_stream = match client.stream_messages(stream_message_request).await {
            Ok(response) => response.into_inner(),
            Err(e) => {
                error!("Failed to establish message streaming: {}.", e);
                return Err(SwbusError::connection(
                    SwbusErrorCode::ConnectionError,
                    io::Error::new(io::ErrorKind::Unsupported, e.to_string()),
                ));
            }
        };

        let mut conn_worker = SwbusConnWorker::new(conn_info, shutdown_ct, incoming_stream, mux, conn_store);
        conn_worker.run().await
    }
}

// Server-side connection factory and task entry
impl SwbusConn {
    /// This function handles incoming connection from clients. It creates a SwbusConn object
    /// and starts the worker task for incoming messages.
    /// parameters:
    /// - conn_type: The connection type.
    /// - client_addr: The client address.
    /// - incoming_stream: The incoming message stream.
    /// - message_queue_tx: The tx end of outgoing message queue
    /// - mux: The SwbusMultiplexer
    pub async fn from_incoming_stream(
        conn_type: ConnectionType,
        client_addr: SocketAddr,
        remote_service_path: ServicePath,
        incoming_stream: Streaming<SwbusMessage>,
        message_queue_tx: mpsc::Sender<Result<SwbusMessage, Status>>,
        mux: Arc<SwbusMultiplexer>,
        conn_store: Arc<SwbusConnStore>,
    ) -> SwbusConn {
        let conn_info = Arc::new(SwbusConnInfo::new_server(conn_type, client_addr, remote_service_path));

        Self::start_server_worker_task(conn_info, incoming_stream, message_queue_tx, mux, conn_store).await
    }

    async fn start_server_worker_task(
        conn_info: Arc<SwbusConnInfo>,
        incoming_stream: Streaming<SwbusMessage>,
        message_queue_tx: mpsc::Sender<Result<SwbusMessage, Status>>,
        mux: Arc<SwbusMultiplexer>,
        conn_store: Arc<SwbusConnStore>,
    ) -> SwbusConn {
        let mut conn = SwbusConn::new(&conn_info, message_queue_tx);

        let conn_info_for_worker = conn_info.clone();
        let shutdown_ct_for_worker = conn.shutdown_ct.clone();
        let worker_task = tokio::spawn(async move {
            Self::run_server_worker_task(
                conn_info_for_worker,
                incoming_stream,
                shutdown_ct_for_worker,
                mux,
                conn_store,
            )
            .await
        });
        conn.worker_task = Some(worker_task);

        conn
    }

    async fn run_server_worker_task(
        conn_info: Arc<SwbusConnInfo>,
        incoming_stream: Streaming<SwbusMessage>,
        shutdown_ct: CancellationToken,
        mux: Arc<SwbusMultiplexer>,
        conn_store: Arc<SwbusConnStore>,
    ) -> Result<()> {
        let mut conn_worker = SwbusConnWorker::new(conn_info, shutdown_ct, incoming_stream, mux, conn_store);
        conn_worker.run().await
    }
}
