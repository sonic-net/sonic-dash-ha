use std::io;
use std::str::FromStr;
use swbus_proto::result::*;
use swbus_proto::swbus::swbus_service_client::SwbusServiceClient;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::Request;
use tracing::error;

pub struct SwbusCoreClient {
    uri: String,

    worker_task: Option<tokio::task::JoinHandle<Result<()>>>,
    client: Option<SwbusServiceClient<Channel>>,
    out_tx: Option<mpsc::Sender<SwbusMessage>>,
    in_tx: mpsc::Sender<SwbusMessage>,
}

impl SwbusCoreClient {
    pub fn new(uri: String, in_tx: mpsc::Sender<SwbusMessage>) -> Result<Self> {
        Ok(Self {
            uri,
            worker_task: None,
            client: None,
            out_tx: None,
            in_tx,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        let (out_tx, out_rx) = mpsc::channel::<SwbusMessage>(100);

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

        self.client = Some(SwbusServiceClient::new(channel));

        let out_stream = ReceiverStream::new(out_rx);
        let out_stream_request = Request::new(out_stream);

        let in_stream = match self.client.as_mut().unwrap().stream_messages(out_stream_request).await {
            Ok(response) => response.into_inner(),
            Err(e) => {
                error!("Failed to establish message streaming: {}.", e);
                return Err(SwbusError::connection(
                    SwbusErrorCode::ConnectionError,
                    io::Error::new(io::ErrorKind::Unsupported, e.to_string()),
                ));
            }
        };

        Ok(())
    }

    pub async fn register_svc(&self, svc: ServicePath) -> Result<()> {
        Ok(())
    }

    pub async fn unregister_svc(&self, svc: ServicePath) -> Result<()> {
        Ok(())
    }

    pub async fn push_svc(&self, svc: ServicePath) -> Result<()> {
        Ok(())
    }

    pub async fn send(&self, message: SwbusMessage) -> Result<()> {
        Ok(())
    }

    pub async fn send_ping(&self, dst: ServicePath) -> Result<()> {
        Ok(())
    }

    pub async fn send_trace(&self, dst: ServicePath) -> Result<()> {
        Ok(())
    }
}
