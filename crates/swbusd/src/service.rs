use super::connection::SwbusConnection;
use contracts::swbus::swbus_service_server::*;
use contracts::swbus::*;
use std::pin::Pin;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

#[derive(Debug, Default)]
pub struct SwbusServiceImpl {}

type SwbusMessageResult<T> = Result<Response<T>, Status>;
type SwbusMessageStream = Pin<Box<dyn Stream<Item=Result<SwbusMessage, Status>> + Send>>;

#[tonic::async_trait]
impl SwbusService for SwbusServiceImpl {
    type StreamMessagesStream = SwbusMessageStream;

    async fn stream_messages(
        &self,
        request: Request<Streaming<SwbusMessage>>,
    ) -> SwbusMessageResult<Self::StreamMessagesStream> {
        let in_stream = request.into_inner();

        let out_stream = SwbusConnection::spawn(in_stream);
        Ok(Response::new(Box::pin(out_stream) as Self::StreamMessagesStream))
    }
}
