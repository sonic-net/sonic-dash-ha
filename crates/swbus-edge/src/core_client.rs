use contracts::requires;
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::swbus_service_client::SwbusServiceClient;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::Endpoint;
use tonic::Request;
use tonic::Streaming;
use tracing::{debug, error, info};

pub struct SwbusCoreClient {
    uri: String,
    sp: ServicePath,

    // tx queue to send messages to swbusd
    pub(crate) send_queue_tx: Arc<RwLock<Option<mpsc::Sender<SwbusMessage>>>>,
    // tx queue to send messages to message router
    message_processor_tx: mpsc::Sender<SwbusMessage>,

    swbusd_connect_task: Option<tokio::task::JoinHandle<Result<()>>>,
}

// Factory functions
impl SwbusCoreClient {
    pub fn new(uri: String, sp: ServicePath, message_processor_tx: mpsc::Sender<SwbusMessage>) -> Self {
        Self {
            uri,
            sp,
            send_queue_tx: Arc::new(RwLock::new(None)),
            message_processor_tx,
            swbusd_connect_task: None,
        }
    }

    pub fn get_service_path(&self) -> ServicePath {
        self.sp.clone()
    }
}

// Message processing functions
impl SwbusCoreClient {
    pub async fn connect(
        uri: String,
        sp: ServicePath,
        receive_queue_tx: mpsc::Sender<SwbusMessage>,
    ) -> Result<(tokio::task::JoinHandle<Result<()>>, mpsc::Sender<SwbusMessage>)> {
        let (send_queue_tx, send_queue_rx) = mpsc::channel::<SwbusMessage>(100);

        let endpoint = Endpoint::from_str(&uri).map_err(|e| {
            SwbusError::input(
                SwbusErrorCode::InvalidArgs,
                format!("Failed to create endpoint: {}.", e),
            )
        })?;

        let channel = match endpoint.connect().await {
            Ok(c) => c,
            Err(e) => {
                return Err(SwbusError::connection(
                    SwbusErrorCode::ConnectionError,
                    io::Error::new(io::ErrorKind::ConnectionReset, format!("Failed to connect: {:?}", e)),
                ));
            }
        };
        info!("Connected to the server");
        let mut client = SwbusServiceClient::new(channel);

        let send_stream = ReceiverStream::new(send_queue_rx);
        let mut send_stream_request = Request::new(send_stream);

        let meta = send_stream_request.metadata_mut();

        meta.insert(
            SWBUS_CLIENT_SERVICE_PATH,
            MetadataValue::from_str(&sp.to_service_prefix()).unwrap(),
        );

        meta.insert(
            SWBUS_CONNECTION_TYPE,
            MetadataValue::from_str(ConnectionType::Local.as_str_name()).unwrap(),
        );

        let recv_stream = match client.stream_messages(send_stream_request).await {
            Ok(response) => response.into_inner(),
            Err(e) => {
                error!("Failed to establish message streaming: {}.", e);
                return Err(SwbusError::connection(
                    SwbusErrorCode::ConnectionError,
                    io::Error::new(io::ErrorKind::Unsupported, e.to_string()),
                ));
            }
        };

        let message_processor_tx_clone = receive_queue_tx.clone();
        let recv_stream_task =
            tokio::spawn(async move { Self::run_recv_stream_task(recv_stream, message_processor_tx_clone).await });
        Ok((recv_stream_task, send_queue_tx))
    }

    fn spawn_connect_task(&mut self) {
        let uri = self.uri.clone();
        let sp = self.sp.clone();
        let message_processor_tx = self.message_processor_tx.clone();
        let send_queue_tx_arc = self.send_queue_tx.clone();

        let handle = tokio::spawn(async move {
            loop {
                match Self::connect(uri.clone(), sp.clone(), message_processor_tx.clone()).await {
                    Ok((recv_stream_task, send_queue_tx)) => {
                        info!("Successfully connected to swbusd at {}", uri);
                        send_queue_tx_arc.write().await.replace(send_queue_tx);
                        // wait for the recv_stream_task to finish
                        let _ = recv_stream_task.await;
                        // clear the send_queue_tx and retry
                        send_queue_tx_arc.write().await.take();
                        continue;
                    }
                    Err(e) => {
                        debug!("Failed to reconnect: {}.", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
        self.swbusd_connect_task = Some(handle);
    }

    #[requires(self.swbusd_connect_task.is_none())]
    pub fn start(&mut self) {
        self.spawn_connect_task();
    }

    pub async fn send(&self, message: SwbusMessage) -> Result<()> {
        let tx = self.send_queue_tx.read().await;
        if tx.is_none() {
            return Err(SwbusError::connection(
                SwbusErrorCode::ConnectionError,
                io::Error::new(io::ErrorKind::ConnectionReset, "Not connected to swbusd"),
            ));
        }
        let tx = tx.as_ref().unwrap();
        match tx.send(message).await {
            Ok(_) => {}
            Err(e) => {
                error!("Failed to send message: {}.", e);
                return Err(SwbusError::connection(
                    SwbusErrorCode::ConnectionError,
                    io::Error::new(io::ErrorKind::ConnectionReset, e.to_string()),
                ));
            }
        }

        Ok(())
    }

    async fn run_recv_stream_task(
        mut recv_stream: Streaming<SwbusMessage>,
        message_processor_tx: mpsc::Sender<SwbusMessage>,
    ) -> Result<()> {
        loop {
            let message = match recv_stream.message().await {
                Ok(Some(message)) => message,

                // The stream was closed by the sender and no more messages will be delivered.
                Ok(None) => {
                    break;
                }

                // gRPC error was sent by the sender instead of a valid response message.
                Err(e) => {
                    error!("Failed to receive message: {}.", e);
                    return Err(SwbusError::connection(
                        SwbusErrorCode::ConnectionError,
                        io::Error::new(io::ErrorKind::ConnectionReset, e.to_string()),
                    ));
                }
            };

            Self::process_incoming_message(message, &message_processor_tx).await;
        }

        Ok(())
    }

    async fn process_incoming_message(message: SwbusMessage, message_processor_tx: &mpsc::Sender<SwbusMessage>) {
        // send to message router, which will route to the appropriate handler or core client
        match message_processor_tx.try_send(message) {
            Ok(_) => {}
            Err(e) => {
                error!("Failed to send message to processor: {}.", e);
                todo!("Reply with queue full message using send_queue_tx.");
            }
        }
    }
}
