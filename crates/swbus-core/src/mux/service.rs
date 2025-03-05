use super::route_annoucer::{RouteAnnounceTask, RouteAnnouncer};
use super::SwbusConn;
use super::SwbusMultiplexer;
use crate::mux::conn_store::SwbusConnStore;
use crate::mux::SwbusConnInfo;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use swbus_config::SwbusConfig;
use swbus_proto::result::*;
use swbus_proto::swbus::swbus_service_server::{SwbusService, SwbusServiceServer};
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tracing::*;

pub struct SwbusServiceHost {
    swbus_server_addr: SocketAddr,
    mux: Option<Arc<SwbusMultiplexer>>,
    conn_store: Option<Arc<SwbusConnStore>>,
}

type SwbusMessageResult<T> = Result<Response<T>, Status>;
type SwbusMessageStream = Pin<Box<dyn Stream<Item = Result<SwbusMessage, Status>> + Send>>;

impl SwbusServiceHost {
    pub fn new(swbus_server_addr: &SocketAddr) -> Self {
        // populate the mux with the routes
        Self {
            swbus_server_addr: *swbus_server_addr,
            mux: None,
            conn_store: None,
        }
    }

    pub async fn start(mut self: SwbusServiceHost, config: SwbusConfig) -> Result<()> {
        info!("Starting SwbusServiceHost at {}", self.swbus_server_addr);
        let addr = self.swbus_server_addr;

        if config.routes.is_empty() {
            return Err(SwbusError::input(
                SwbusErrorCode::InvalidArgs,
                "No routes found in the configuration.".to_string(),
            ));
        }

        // create mux and set route announce task queue
        let mut mux = SwbusMultiplexer::new(config.routes);
        let (route_annouce_task_tx, route_annouce_task_rx) = mpsc::channel::<RouteAnnounceTask>(100);
        let route_announcer_ct = CancellationToken::new();
        mux.set_route_announcer(route_annouce_task_tx, route_announcer_ct.clone());

        let mux = Arc::new(mux);
        let mux_clone = mux.clone();

        // start the route announcer
        tokio::spawn(async move {
            let mut route_announcer = RouteAnnouncer::new(route_annouce_task_rx, route_announcer_ct, mux_clone);
            route_announcer.run().await;
        });

        let conn_store = Arc::new(SwbusConnStore::new(mux.clone()));

        // add peers to the connection store
        for peer in config.peers {
            conn_store.add_peer(peer);
        }

        self.mux = Some(mux);
        self.conn_store = Some(conn_store);

        Server::builder()
            .add_service(SwbusServiceServer::new(self))
            .serve(addr)
            .await
            .map_err(|e| {
                SwbusError::connection(
                    SwbusErrorCode::ConnectionError,
                    io::Error::new(io::ErrorKind::Other, format!("Failed to listen at {}: {}", addr, e)),
                )
            })
    }
}

#[tonic::async_trait]
impl SwbusService for SwbusServiceHost {
    type StreamMessagesStream = SwbusMessageStream;

    #[instrument(name="connection_received", level="info", skip_all, fields(addr=%request.remote_addr().unwrap()))]
    async fn stream_messages(
        &self,
        request: Request<Streaming<SwbusMessage>>,
    ) -> SwbusMessageResult<SwbusMessageStream> {
        let client_addr = request.remote_addr().unwrap();

        info!("SwbusServiceServer::connection from {} accepted", client_addr);
        let service_path = match request.metadata().get(SWBUS_CLIENT_SERVICE_PATH) {
            Some(path) => match ServicePath::from_string(path.to_str().unwrap()) {
                Ok(service_path) => service_path,
                Err(e) => {
                    error!("SwbusServiceServer::error parsing client service path: {:?}", e);
                    return Err(Status::invalid_argument("Invalid client service path"));
                }
            },
            None => {
                error!("SwbusServiceServer::client service path not found");
                return Err(Status::invalid_argument("Client service path not found"));
            }
        };

        let conn_type = match request.metadata().get(SWBUS_CONNECTION_TYPE) {
            Some(conn_type_meta) => match ConnectionType::from_str_name(conn_type_meta.to_str().unwrap()) {
                Some(conn_type) => conn_type,
                None => {
                    return Err(Status::invalid_argument(format!(
                        "Invalid connection_type: {}",
                        conn_type_meta.to_str().unwrap()
                    )));
                }
            },
            None => {
                error!("SwbusServiceServer::service path scope not set");
                return Err(Status::invalid_argument("service path scope not set"));
            }
        };

        let in_stream = request.into_inner();
        info!(
            conn_type = conn_type as i32,
            service_path = service_path.to_longest_path(),
            "Creating SwbusConn"
        );
        // outgoing message queue
        let (out_tx, out_rx) = mpsc::channel(16);

        let conn_info = Arc::new(SwbusConnInfo::new_server(conn_type, client_addr, service_path));
        let conn = SwbusConn::from_incoming_stream(
            conn_info,
            in_stream,
            out_tx,
            self.mux.as_ref().unwrap().clone(),
            self.conn_store.as_ref().unwrap().clone(),
        )
        .await;
        self.conn_store.as_ref().unwrap().conn_established(conn);
        let out_stream = ReceiverStream::new(out_rx);
        Ok(Response::new(Box::pin(out_stream) as Self::StreamMessagesStream))
    }
}
