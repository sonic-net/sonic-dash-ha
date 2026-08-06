use super::conn_store::SwbusConnStore;
use super::SwbusConnInfo;
use super::SwbusConnProxy;
use super::SwbusConnWorker;
use super::SwbusMultiplexer;
use hyper_util::rt::TokioIo;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::swbus_service_client::SwbusServiceClient;
use swbus_proto::swbus::*;
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status, Streaming};
use tower::service_fn;
use tracing::*;

async fn connect_from_local_addr(local_addr: IpAddr, remote_addr: SocketAddr) -> io::Result<TcpStream> {
    if local_addr.is_ipv4() != remote_addr.is_ipv4() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Local address {local_addr} and remote address {remote_addr} use different IP families"),
        ));
    }

    let socket = if local_addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.bind(SocketAddr::new(local_addr, 0))?;
    socket.connect(remote_addr).await
}

#[derive(Debug)]
pub struct SwbusConn {
    // Connection information
    info: Arc<SwbusConnInfo>,

    // Worker task
    worker_task: Option<tokio::task::JoinHandle<Result<()>>>,
    shutdown_ct: CancellationToken,

    // Outgoing message queue
    send_queue_tx: mpsc::Sender<Result<SwbusMessage, Status>>,
}

// Connection operations
impl SwbusConn {
    pub(crate) fn new(
        conn_info: &Arc<SwbusConnInfo>,
        send_queue_tx: mpsc::Sender<Result<SwbusMessage, Status>>,
    ) -> SwbusConn {
        SwbusConn {
            info: conn_info.clone(),
            worker_task: None,
            shutdown_ct: CancellationToken::new(),
            send_queue_tx,
        }
    }

    pub fn info(&self) -> &Arc<SwbusConnInfo> {
        &self.info
    }

    pub(crate) fn new_proxy(&self) -> SwbusConnProxy {
        SwbusConnProxy::new(self.send_queue_tx.clone())
    }

    /// Signal the worker to stop without requiring an async shutdown path.
    /// Connection replacement uses this after synchronously removing the old routes.
    pub(crate) fn cancel(&self) {
        self.shutdown_ct.cancel();
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.cancel();
        Ok(())
    }
}

// Client-side connection factory and task entry
impl SwbusConn {
    pub async fn connect(
        conn_info: &SwbusConnInfo,
        local_addr: IpAddr,
        mux: Arc<SwbusMultiplexer>,
        conn_store: Arc<SwbusConnStore>,
    ) -> Result<SwbusConn> {
        let endpoint = Endpoint::from_str(&format!("http://{}", conn_info.remote_addr()))
            .map_err(|e| SwbusError::input(SwbusErrorCode::InvalidArgs, format!("Failed to create endpoint: {e}.")))?;

        let remote_addr = conn_info.remote_addr();
        let connector =
            service_fn(
                move |_| async move { connect_from_local_addr(local_addr, remote_addr).await.map(TokioIo::new) },
            );
        let channel = match endpoint.connect_with_connector(connector).await {
            Ok(c) => c,
            Err(e) => {
                debug!("Failed to connect: {}.", e);
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
        conn_info: &SwbusConnInfo,
        mut client: SwbusServiceClient<Channel>,
        mux: Arc<SwbusMultiplexer>,
        conn_store: Arc<SwbusConnStore>,
    ) -> Result<SwbusConn> {
        let (send_queue_tx, send_queue_rx) = mpsc::channel(16);

        let request_stream = ReceiverStream::new(send_queue_rx).map(|result: Result<SwbusMessage, Status>| {
            result.expect("Not expecting grpc client adding messages with error status")
        });

        let mut stream_message_request = Request::new(request_stream);

        let sp_str = mux.get_my_service_path().to_string();

        let meta = stream_message_request.metadata_mut();

        meta.insert(
            SWBUS_CLIENT_SERVICE_PATH,
            MetadataValue::from_str(sp_str.as_str()).unwrap(),
        );
        meta.insert(
            SWBUS_CONNECTION_TYPE,
            MetadataValue::from_str(conn_info.connection_type().as_str_name()).unwrap(),
        );

        let (incoming_stream, conn_info_for_worker) = match client.stream_messages(stream_message_request).await {
            Ok(response) => {
                // Extract server service path from response metadata and update remote_service_path
                let server_service_path = match response.metadata().get(SWBUS_SERVER_SERVICE_PATH) {
                    Some(path) => match ServicePath::from_string(path.to_str().unwrap()) {
                        Ok(service_path) => {
                            info!("Received server service path: {}", service_path.to_string());
                            service_path
                        }
                        Err(e) => {
                            error!("Failed to parse server service path: {:?}", e);
                            return Err(SwbusError::connection(
                                SwbusErrorCode::InvalidHeader,
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!("Invalid server service path: {:?}", e),
                                ),
                            ));
                        }
                    },
                    None => {
                        error!("Server service path not found in response metadata");
                        return Err(SwbusError::connection(
                            SwbusErrorCode::InvalidHeader,
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "Server service path not found in response metadata",
                            ),
                        ));
                    }
                };

                // Update conn_info's remote_service_path with actual server service path
                let updated_conn_info = Arc::new(conn_info.clone().with_remote_service_path(server_service_path));

                (response.into_inner(), updated_conn_info)
            }
            Err(e) => {
                error!("Failed to establish message streaming: {}.", e);
                return Err(SwbusError::connection(
                    SwbusErrorCode::ConnectionError,
                    io::Error::new(io::ErrorKind::Unsupported, e.to_string()),
                ));
            }
        };
        let mut conn = SwbusConn::new(&conn_info_for_worker, send_queue_tx);
        let shutdown_ct_for_worker = conn.shutdown_ct.clone();

        let worker_task = tokio::spawn(async move {
            Self::run_client_worker_task(
                conn_info_for_worker,
                shutdown_ct_for_worker,
                incoming_stream,
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
    /// - send_queue_rx: The outgoing message queue rx end.
    async fn run_client_worker_task(
        conn_info: Arc<SwbusConnInfo>,
        shutdown_ct: CancellationToken,
        incoming_stream: Streaming<SwbusMessage>,
        mux: Arc<SwbusMultiplexer>,
        conn_store: Arc<SwbusConnStore>,
    ) -> Result<()> {
        let mut conn_worker = SwbusConnWorker::new(conn_info, shutdown_ct, incoming_stream, mux, conn_store);
        conn_worker.run().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn outbound_connection_binds_to_configured_local_address() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let remote_addr = listener.local_addr().unwrap();
        let local_addr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));

        let (client, accepted) = tokio::join!(connect_from_local_addr(local_addr, remote_addr), listener.accept());
        let client = client.unwrap();
        let (_, peer_addr) = accepted.unwrap();

        assert_eq!(client.local_addr().unwrap().ip(), local_addr);
        assert_eq!(peer_addr.ip(), local_addr);
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
    /// - send_queue_tx: The tx end of outgoing message queue
    /// - mux: The SwbusMultiplexer
    pub async fn from_incoming_stream(
        conn_info: Arc<SwbusConnInfo>,
        incoming_stream: Streaming<SwbusMessage>,
        send_queue_tx: mpsc::Sender<Result<SwbusMessage, Status>>,
        mux: Arc<SwbusMultiplexer>,
        conn_store: Arc<SwbusConnStore>,
    ) -> SwbusConn {
        Self::start_server_worker_task(conn_info, incoming_stream, send_queue_tx, mux, conn_store).await
    }

    async fn start_server_worker_task(
        conn_info: Arc<SwbusConnInfo>,
        incoming_stream: Streaming<SwbusMessage>,
        send_queue_tx: mpsc::Sender<Result<SwbusMessage, Status>>,
        mux: Arc<SwbusMultiplexer>,
        conn_store: Arc<SwbusConnStore>,
    ) -> SwbusConn {
        let mut conn = SwbusConn::new(&conn_info, send_queue_tx);

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
