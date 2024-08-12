use crate::conn_info::*;
use crate::contracts::swbus::*;
use crate::result::*;
use std::io;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::Streaming;
use tracing::{error, info};

pub(crate) enum SwbusConnControlMessage {
    Shutdown,
}

pub struct SwbusConnWorker {
    info: Arc<SwbusConnInfo>,
    control_queue_rx: mpsc::Receiver<SwbusConnControlMessage>,
    message_stream: Streaming<SwbusMessage>,
}

impl SwbusConnWorker {
    pub fn new(
        info: Arc<SwbusConnInfo>,
        control_queue_rx: mpsc::Receiver<SwbusConnControlMessage>,
        message_stream: Streaming<SwbusMessage>,
    ) -> Self {
        Self {
            info,
            control_queue_rx,
            message_stream,
        }
    }

    pub async fn run(
        &mut self,
    ) -> Result<()> {
        loop {
            match self.control_queue_rx.try_recv() {
                Ok(SwbusConnControlMessage::Shutdown) => {
                    info!("Shutting down connection worker.");
                    break;
                }

                Err(mpsc::error::TryRecvError::Empty) => {
                    self.process_incoming_messages().await?;
                }

                // We never close the control queue while the worker is running, so this error should never be hit.
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    unreachable!("Control queue closed unexpectedly.");
                }
            }
        }

        Ok(())
    }

    async fn process_incoming_messages(
        &mut self,
    ) -> Result<()> {
        while let Some(result) = self.message_stream.next().await {
            if let Err(err) = result {
                error!("Failed to receive message: {}.", err);
                return Err(SwbusError::connection(
                    SwbusErrorCode::ConnectionError,
                    io::Error::new(io::ErrorKind::ConnectionReset, err.to_string()),
                ));
            }

            let message = result.unwrap();
            match self.process_incoming_message(message).await {
                Ok(_) => {}
                Err(err) => {
                    error!("Failed to process the incoming message: {}", err);
                }
            }
        }

        Ok(())
    }

    async fn process_incoming_message(
        &mut self,
        message: SwbusMessage,
    ) -> Result<()> {
        Self::validate_message_common(&message)?;
        // TODO: Handle message
        Ok(())
    }

    fn validate_message_common(message: &SwbusMessage) -> Result<()> {
        if message.header.is_none() {
            return Err(SwbusError::input(
                SwbusErrorCode::InvalidHeader,
                "Message missing header".to_string(),
            ));
        }

        let message_header = message.header.as_ref().unwrap();
        if message_header.version < 1 {
            return Err(SwbusError::input(
                SwbusErrorCode::InvalidHeader,
                "Message version too low".to_string(),
            ));
        }

        if message_header.source.is_none() {
            return Err(SwbusError::input(
                SwbusErrorCode::InvalidHeader,
                "Message missing source".to_string(),
            ));
        }

        if message_header.destination.is_none() {
            return Err(SwbusError::input(
                SwbusErrorCode::InvalidHeader,
                "Message missing destination".to_string(),
            ));
        }

        if message.body.is_none() {
            return Err(SwbusError::input(
                SwbusErrorCode::InvalidHeader,
                "Message missing body".to_string(),
            ));
        }

        return Ok(());
    }
}
