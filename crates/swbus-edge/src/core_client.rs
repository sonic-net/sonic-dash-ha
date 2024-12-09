use contracts::requires;
use dashmap::DashSet;
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::swbus_service_client::SwbusServiceClient;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::Request;
use tonic::Streaming;
use tracing::error;

pub struct SwbusCoreClient {
    uri: String,
    local_services: Arc<DashSet<ServicePath>>,

    client: Option<SwbusServiceClient<Channel>>,
    send_queue_tx: Option<mpsc::Sender<SwbusMessage>>,
    message_processor_tx: mpsc::Sender<SwbusMessage>,

    recv_stream_task: Option<tokio::task::JoinHandle<Result<()>>>,
}

// Factory functions
impl SwbusCoreClient {
    pub fn new(uri: String, message_processor_tx: mpsc::Sender<SwbusMessage>) -> Self {
        Self {
            uri,
            local_services: Arc::new(DashSet::new()),
            client: None,
            send_queue_tx: None,
            message_processor_tx,
            recv_stream_task: None,
        }
    }
}

// Service registration functions
impl SwbusCoreClient {
    pub fn register_svc(&self, svc: ServicePath) {
        self.local_services.insert(svc);
    }

    pub fn unregister_svc(&self, svc: ServicePath) {
        self.local_services.remove(&svc);
    }

    pub fn push_svc(&self) -> Result<()> {
        Ok(())
    }
}

// Message processing functions
impl SwbusCoreClient {
    #[requires(self.recv_stream_task.is_none() && self.client.is_none() && self.send_queue_tx.is_none())]
    pub async fn start(&mut self) -> Result<()> {
        let (send_queue_tx, send_queue_rx) = mpsc::channel::<SwbusMessage>(100);

        let endpoint = Endpoint::from_str(&self.uri).map_err(|e| {
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

        let mut client = SwbusServiceClient::new(channel);

        let send_stream = ReceiverStream::new(send_queue_rx);
        let send_stream_request = Request::new(send_stream);

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

        let send_queue_tx_clone = send_queue_tx.clone();
        let message_processor_tx_clone = self.message_processor_tx.clone();
        let recv_stream_task = tokio::spawn(async move {
            Self::run_recv_stream_task(recv_stream, message_processor_tx_clone, send_queue_tx_clone).await
        });

        self.client = Some(client);
        self.recv_stream_task = Some(recv_stream_task);
        self.send_queue_tx = Some(send_queue_tx);

        Ok(())
    }

    pub async fn send(&self, message: SwbusMessage) -> Result<()> {
        // TODO: Check local registrations
        match self.send_queue_tx.as_ref().unwrap().send(message).await {
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
        send_queue_tx: mpsc::Sender<SwbusMessage>,
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

            Self::process_incoming_message(message, &message_processor_tx, &send_queue_tx).await;
        }

        Ok(())
    }

    async fn process_incoming_message(
        message: SwbusMessage,
        message_processor_tx: &mpsc::Sender<SwbusMessage>,
        send_queue_tx: &mpsc::Sender<SwbusMessage>,
    ) {
        // TODO: Check local registrations
        match message_processor_tx.try_send(message) {
            Ok(_) => {}
            Err(e) => {
                error!("Failed to send message to processor: {}.", e);
                todo!("Reply with queue full message using send_queue_tx.");
            }
        }
    }
}
