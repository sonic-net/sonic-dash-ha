use super::SwbusConnInfo;
use super::SwbusMultiplexer;
use crate::mux::conn_store::SwbusConnStore;
use futures_core::stream::Stream;
use std::io;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::SwbusMessage;
use swbus_proto::swbus::*;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::*;

pub struct SwbusConnWorker<T>
where
    T: Stream<Item = Result<SwbusMessage, Status>> + Unpin,
{
    info: Arc<SwbusConnInfo>,
    shutdown_ct: CancellationToken,
    // incoming message stream
    message_stream: T,
    mux: Arc<SwbusMultiplexer>,
    conn_store: Arc<SwbusConnStore>,
}

// Connection worker facade
impl<T> SwbusConnWorker<T>
where
    T: Stream<Item = Result<SwbusMessage, Status>> + Unpin,
{
    pub(crate) fn new(
        info: Arc<SwbusConnInfo>,
        shutdown_ct: CancellationToken,
        message_stream: T,
        mux: Arc<SwbusMultiplexer>,
        conn_store: Arc<SwbusConnStore>,
    ) -> Self {
        Self {
            info,
            shutdown_ct,
            message_stream,
            mux,
            conn_store,
        }
    }

    pub fn shutdown(&self) {
        self.shutdown_ct.cancel();
    }

    #[instrument(name="ConnWorker", skip(self), fields(conn_id=self.info.id()))]
    pub async fn run(&mut self) -> Result<()> {
        info!("Starting connection worker");
        self.register_to_mux()?;
        let result = self.run_worker_loop().await;
        // unregister from mux
        info!("Unregistering from mux.");
        self.unregister_from_mux()?;
        if result.is_err() {
            info!("Reporting connection lost.");
            self.conn_store.conn_lost(self.info.clone());
        }
        result
    }
}

// Internal worker loop
impl<T> SwbusConnWorker<T>
where
    T: Stream<Item = Result<SwbusMessage, Status>> + Unpin,
{
    fn register_to_mux(&self) -> Result<()> {
        Ok(())
    }

    fn unregister_from_mux(&self) -> Result<()> {
        self.mux.unregister(self.info.clone());
        Ok(())
    }

    async fn run_worker_loop(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                _ = self.shutdown_ct.cancelled() => {
                    info!("Shutting down connection worker.");
                    break;
                }

                data_message = self.message_stream.next() => {
                    match data_message {
                        Some(Ok(message)) => {
                            match self.process_data_message(message).await {
                                Ok(_) => {}
                                Err(err) => {
                                    error!("Failed to process the incoming message: {}", err);
                                }
                            }
                        }
                        Some(Err(err)) => {
                            error!("Failed to receive message: {}.", err);
                            return Err(SwbusError::connection(
                                SwbusErrorCode::ConnectionError,
                                io::Error::new(io::ErrorKind::ConnectionReset, err.to_string()),
                            ));
                        }
                        None => {
                            info!("Message stream closed.");
                            return Err(SwbusError::connection(
                                SwbusErrorCode::ConnectionError,
                                io::Error::new(io::ErrorKind::ConnectionReset, "Message stream closed.".to_string()),
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    #[instrument(name="receive_msg", level="debug", skip_all, fields(message.id=message.header.as_ref().unwrap().id))]
    async fn process_data_message(&mut self, message: SwbusMessage) -> Result<()> {
        debug!("{:?}", &message);
        self.validate_message_common(&message)?;
        match message.body {
            Some(swbus_message::Body::TraceRouteRequest(_)) => {
                info!("Received traceroute request: {:?}", message);
                // self.process_ping_request(&message);
            }
            _ => {
                self.mux.route_message(message).await?;
            }
        }
        Ok(())
    }

    fn validate_message_common(&mut self, message: &SwbusMessage) -> Result<()> {
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

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::RouteConfig;
    use tokio_stream::{self as stream};

    #[tokio::test]
    async fn conn_worker_can_be_shutdown() {
        let shutdown_ct = CancellationToken::new();
        let message_stream = stream::iter(vec![]);
        let mux = Arc::new(SwbusMultiplexer::new());
        let conn_store = Arc::new(SwbusConnStore::new(mux.clone()));

        let conn_info = Arc::new(SwbusConnInfo::new_client(
            ConnectionType::Cluster,
            "127.0.0.1:8080".parse().unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.1-dpu0").unwrap(),
        ));

        let mut worker = SwbusConnWorker::new(conn_info, shutdown_ct.clone(), message_stream, mux, conn_store);
        let worker_task = tokio::spawn(async move { worker.run().await });

        shutdown_ct.cancel();
        let result = worker_task.await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn conn_worker_can_process_data_message() {
        let shutdown_ct = CancellationToken::new();

        let header = SwbusMessageHeader::new(
            ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            1,
        );
        let ping_msg = SwbusMessage {
            header: Some(header),
            body: Some(swbus_message::Body::PingRequest(PingRequest::new())),
        };
        let message_stream = stream::iter(vec![Ok(ping_msg)]);

        let mux = Arc::new(SwbusMultiplexer::new());
        let conn_store = Arc::new(SwbusConnStore::new(mux.clone()));
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: RouteScope::Cluster,
        };
        mux.set_my_routes(vec![route_config.clone()]);

        let conn_info = Arc::new(SwbusConnInfo::new_client(
            ConnectionType::Cluster,
            "127.0.0.1:8080".parse().unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.1-dpu0").unwrap(),
        ));

        let mut worker = SwbusConnWorker::new(conn_info, shutdown_ct.clone(), message_stream, mux, conn_store);
        let worker_task = tokio::spawn(async move { worker.run().await });

        shutdown_ct.cancel();
        let result = worker_task.await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_worker_invalid_message() {
        let shutdown_ct = CancellationToken::new();
        let message_stream = stream::iter(vec![]);
        let mux = Arc::new(SwbusMultiplexer::new());
        let conn_store = Arc::new(SwbusConnStore::new(mux.clone()));

        let conn_info = Arc::new(SwbusConnInfo::new_client(
            ConnectionType::Cluster,
            "127.0.0.1:8080".parse().unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.1-dpu0").unwrap(),
        ));

        let mut worker = SwbusConnWorker::new(conn_info, shutdown_ct.clone(), message_stream, mux, conn_store);

        // verify message without header
        let message = SwbusMessage {
            header: None,
            body: Some(swbus_message::Body::TraceRouteRequest(Default::default())),
        };
        assert!(worker.validate_message_common(&message).is_err());

        // verify message with wrong version
        let header = SwbusMessageHeader {
            version: 0,
            id: 1,
            flag: 0,
            ttl: 64,
            source: Some(ServicePath::from_string("regiona.clustera.10.0.0.1-dpu0").unwrap()),
            destination: Some(ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap()),
        };
        let message = SwbusMessage {
            header: Some(header),
            body: Some(swbus_message::Body::TraceRouteRequest(Default::default())),
        };
        assert!(worker.validate_message_common(&message).is_err());

        // verify message without source
        let header = SwbusMessageHeader {
            version: 1,
            id: 1,
            flag: 0,
            ttl: 64,
            source: None,
            destination: Some(ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap()),
        };
        let message = SwbusMessage {
            header: Some(header),
            body: Some(swbus_message::Body::TraceRouteRequest(Default::default())),
        };
        assert!(worker.validate_message_common(&message).is_err());

        // verify message without dest
        let header = SwbusMessageHeader {
            version: 1,
            id: 1,
            flag: 0,
            ttl: 64,
            source: Some(ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap()),
            destination: None,
        };
        let message = SwbusMessage {
            header: Some(header),
            body: Some(swbus_message::Body::TraceRouteRequest(Default::default())),
        };
        assert!(worker.validate_message_common(&message).is_err());

        // verify message without body
        let header = SwbusMessageHeader {
            version: 1,
            id: 1,
            flag: 0,
            ttl: 64,
            source: Some(ServicePath::from_string("regiona.clustera.10.0.0.1-dpu0").unwrap()),
            destination: Some(ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap()),
        };
        let message = SwbusMessage {
            header: Some(header),
            body: None,
        };
        assert!(worker.validate_message_common(&message).is_err());
    }
}
