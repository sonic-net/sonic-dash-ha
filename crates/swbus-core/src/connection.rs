use crate::contracts::swbus::*;
use std::io;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Status, Streaming};

#[derive(Debug, Default, Clone)]
struct SwbusConnectionInfo {
    connection_type: ConnectionType,
}

struct SwbusConnectionWorkerData {
    // Connection info
    info: SwbusConnectionInfo,

    /// Message channel for sending infra related messages to Mux.
    infra_tx: mpsc::Sender<Result<SwbusMessage, Status>>,

    /// Message channel for sending data related messages to client or handler.
    data_tx: mpsc::Sender<Result<SwbusMessage, Status>>,
}

pub struct SwbusConnection {
    info: Arc<Mutex<SwbusConnectionInfo>>,
}

impl SwbusConnection {
    pub fn accept(in_stream: Streaming<SwbusMessage>) -> ReceiverStream<Result<SwbusMessage, Status>> {
        let conn_info = SwbusConnectionInfo::default();

        let (infra_tx, infra_rx) = mpsc::channel::<Result<SwbusMessage, Status>>(128);
        let (data_tx, data_rx) = mpsc::channel::<Result<SwbusMessage, Status>>(128);

        let worker_data = SwbusConnectionWorkerData {
            info: conn_info.clone(),
            infra_tx,
            data_tx,
        };

        let out_stream = ReceiverStream::new(data_rx);
        tokio::spawn(async move {
            self.run_worker_loop(in_stream, worker_data).await;
        });

        let connection = Arc::new(SwbusConnection { info: conn_info });
        out_stream
    }

    async fn run_worker_loop(self, mut in_stream: Streaming<SwbusMessage>) {
        while let Some(result) = in_stream.next().await {
            if let Err(err) = result {
                todo!("connection error handling");
            }

            let message = result.unwrap();
            match Self::process_message(message, connection.clone()).await {
                Ok(_) => {}
                Err(err) => match err.kind() {
                    io::ErrorKind::InvalidData => {
                        todo!("invalid data")
                    }
                    _ => {
                        todo!("error handling")
                    }
                },
            }
        }
    }

    async fn process_message(message: SwbusMessage, connection: Arc<SwbusConnection>) -> Result<(), io::Error> {
        Self::validate_message_common(&message)?;

        if let swbus_message::Body::UpdateConnectionInfoRequest(req) = message.body.as_ref().unwrap() {}

        Ok(())
    }

    fn validate_message_common(message: &SwbusMessage) -> Result<(), io::Error> {
        if message.header.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Message missing header",
            ));
        }

        let message_header = message.header.as_ref().unwrap();
        if message_header.version < 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Message version too low",
            ));
        }

        if message_header.source.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Message missing source",
            ));
        }

        if message_header.destination.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Message missing source",
            ));
        }

        if message.body.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Message missing body",
            ));
        }

        return Ok(());
    }
}
