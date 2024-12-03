use super::SwbusConn;
use super::SwbusMultiplexer;
use crate::mux::conn_store::SwbusConnStore;
use crate::mux::RoutesConfig;
use crate::mux::SwbusNextHop;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::swbus_service_server::{SwbusService, SwbusServiceServer};
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{transport::Server, Request, Response, Status, Streaming};

pub struct SwbusServiceHost {
    swbus_server_addr: String,
    mux: Arc<SwbusMultiplexer>,
    conn_store: Arc<SwbusConnStore>,
}

type SwbusMessageResult<T> = Result<Response<T>, Status>;
type SwbusMessageStream = Pin<Box<dyn Stream<Item = Result<SwbusMessage, Status>> + Send>>;

impl SwbusServiceHost {
    pub fn new(swbus_server_addr: String) -> Self {
        let mux = Arc::new(SwbusMultiplexer::new());
        let conn_store = Arc::new(SwbusConnStore::new(mux.clone()));
        // populate the mux with the routes
        Self {
            swbus_server_addr,
            mux,
            conn_store,
        }
    }

    pub async fn start(self: &Arc<SwbusServiceHost>, routes_config: RoutesConfig) -> Result<()> {
        let addr = self.swbus_server_addr.parse().map_err(|e| {
            SwbusError::input(
                SwbusErrorCode::InvalidArgs,
                format!("Failed to parse server address: {}.", e),
            )
        })?;
        match routes_config.routes.len() {
            0 => {
                return Err(SwbusError::input(
                    SwbusErrorCode::InvalidArgs,
                    "No routes found in the configuration.".to_string(),
                ));
            }
            _ => {}
        }
        let server_instance = self.clone();
        let server_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            Server::builder()
                .add_service(SwbusServiceServer::from_arc(server_instance))
                .serve(addr)
                .await
                .map_err(|e| {
                    SwbusError::connection(
                        SwbusErrorCode::ConnectionError,
                        io::Error::new(io::ErrorKind::Other, format!("Failed to listen at {}: {}", addr, e)),
                    )
                })
        });

        // register local nexthops for local services
        self.mux.set_my_routes(routes_config.routes.clone());
        for route in routes_config.routes {
            let sr = route.key.clone_for_local_mgmt();

            //Create local service route
            let route_key = sr.to_service_prefix();
            let nexthop = SwbusNextHop::new_local(&self.mux);
            self.mux.update_route(route_key, nexthop);
            self.conn_store.add_my_route(route);
        }

        // add peers to the connection store
        for peer in routes_config.peers {
            self.conn_store.add_peer(peer);
        }

        //Wait for server to finish
        match server_handle.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(SwbusError::internal(SwbusErrorCode::Fail, e.to_string())),
        }
    }
}

#[tonic::async_trait]
impl SwbusService for SwbusServiceHost {
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

        let scope = match request.metadata().get(SERVICE_PATH_SCOPE) {
            Some(scope_name) => match RouteScope::from_str_name(scope_name.to_str().unwrap()) {
                Some(scope) => scope,
                None => {
                    return Err(Status::invalid_argument(format!(
                        "Invalid service path scope: {}",
                        scope_name.to_str().unwrap()
                    )));
                }
            },
            None => {
                println!("SwbusServiceServer::service path scope not set");
                return Err(Status::invalid_argument("service path scope not set"));
            }
        };

        let in_stream = request.into_inner();
        // outgoing message queue
        let (tx, rx) = mpsc::channel(16);

        let conn = SwbusConn::from_receive(
            scope,
            client_addr,
            service_path,
            in_stream,
            tx,
            self.mux.clone(),
            self.conn_store.clone(),
        )
        .await;
        self.conn_store.conn_established(conn);
        let out_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(out_stream) as Self::StreamMessagesStream))
    }
}
