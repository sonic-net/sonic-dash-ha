use super::SwbusConn;
use super::SwbusMultiplexer;
use std::pin::Pin;
use std::sync::Arc;
use swbus_proto::swbus::swbus_service_server::{SwbusService, SwbusServiceServer};
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{transport::Server, Request, Response, Status, Streaming};

#[derive(Debug, Default)]
pub struct SwbusServiceServerImpl {
    multiplexer: Arc<SwbusMultiplexer>,
}

type SwbusMessageResult<T> = Result<Response<T>, Status>;
type SwbusMessageStream = Pin<Box<dyn Stream<Item = Result<SwbusMessage, Status>> + Send>>;

impl SwbusServiceServerImpl {
    pub fn new(multiplexer: Arc<SwbusMultiplexer>) -> Self {
        SwbusServiceServerImpl { multiplexer }
    }
}
#[tonic::async_trait]
impl SwbusService for SwbusServiceServerImpl {
    type StreamMessagesStream = SwbusMessageStream;

    async fn stream_messages(
        &self,
        request: Request<Streaming<SwbusMessage>>,
    ) -> SwbusMessageResult<SwbusMessageStream> {
        let client_addr = request.remote_addr().unwrap();

        println!("SwbusServiceServer::connection from {} accepted", client_addr);
        let service_path = match request.metadata().get(CLIENT_SERVICE_PATH) {
            Some(path) => match ServicePath::from_string(path.to_str().unwrap()) {
                Ok(service_path) => service_path,
                Err(e) => {
                    println!("SwbusServiceServer::error parsing client service path: {:?}", e);
                    return Err(Status::invalid_argument("Invalid client service path"));
                }
            },
            None => {
                println!("SwbusServiceServer::client service path not found");
                return Err(Status::invalid_argument("Client service path not found"));
            }
        };
        let in_stream = request.into_inner();
        // outgoing message queue
        let (tx, rx) = mpsc::channel(16);

        let conn = SwbusConn::from_receive(
            Scope::Cluster,
            client_addr,
            service_path,
            in_stream,
            tx,
            self.multiplexer.clone(),
        )
        .await;
        self.multiplexer.register(conn);
        let out_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(out_stream) as Self::StreamMessagesStream))
    }
}
