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

pub struct SwbusConnWorker {}

impl SwbusConnWorker {
    pub async fn run(
        info: Arc<SwbusConnInfo>,
        mut control_queue_rx: mpsc::Receiver<SwbusConnControlMessage>,
        mut message_stream: Streaming<SwbusMessage>,
        mut dispatch_queue_tx: mpsc::Sender<SwbusMessage>,
    ) -> Result<()> {
        loop {
            match control_queue_rx.try_recv() {
                Ok(SwbusConnControlMessage::Shutdown) => {
                    info!("Shutting down connection worker.");
                    break;
                }

                Err(mpsc::error::TryRecvError::Empty) => {
                    Self::process_incoming_messages(&mut message_stream, &mut dispatch_queue_tx).await?;
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
        message_stream: &mut Streaming<SwbusMessage>,
        dispatch_queue_tx: &mut mpsc::Sender<SwbusMessage>,
    ) -> Result<()> {
        while let Some(result) = message_stream.next().await {
            if let Err(err) = result {
                error!("Failed to receive message: {}.", err);
                return Err(SwbusError::connection(
                    SwbusErrorCode::ConnectionError,
                    io::Error::new(io::ErrorKind::ConnectionReset, err.to_string()),
                ));
            }

            let message = result.unwrap();
            match Self::process_incoming_message(message, dispatch_queue_tx).await {
                Ok(_) => {}
                Err(err) => {
                    error!("Failed to process the incoming message: {}", err);
                }
            }
        }

        Ok(())
    }

    async fn process_incoming_message(
        message: SwbusMessage,
        dispatch_queue_tx: &mut mpsc::Sender<SwbusMessage>,
    ) -> Result<()> {
        Self::validate_message_common(&message)?;
        match dispatch_queue_tx.send(message).await {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to dispatch message: {}.", err);
                return Err(SwbusError::connection(
                    SwbusErrorCode::ConnectionError,
                    io::Error::new(io::ErrorKind::ConnectionReset, err.to_string()),
                ));
            }
        }
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
