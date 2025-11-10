use super::route_annoucer::{RouteAnnounceTask, TriggerType};
use super::{NextHopType, SwbusConnInfo, SwbusConnProxy, SwbusNextHop};
use dashmap::{DashMap, DashSet};
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use swbus_config::RouteConfig;
use swbus_proto::message_id_generator::MessageIdGenerator;
use swbus_proto::result::*;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_util::sync::CancellationToken;
use tracing::*;

enum RouteStage {
    Global,
    Region,
    Cluster,
    Local,
}
const ROUTE_STAGES: [RouteStage; 4] = [
    RouteStage::Local,
    RouteStage::Cluster,
    RouteStage::Region,
    RouteStage::Global,
];

pub struct SwbusMultiplexer {
    /// Route table. Each entry is a registered prefix to a next hop, which points to a connection.
    routes: DashMap<String, BTreeSet<SwbusNextHop>>,
    id_generator: MessageIdGenerator,
    my_routes: HashMap<ServicePath, RouteScope>,
    my_service_path: ServicePath,
    connections: DashSet<Arc<SwbusConnInfo>>,
    route_annouce_task_tx: Option<mpsc::Sender<RouteAnnounceTask>>,
    route_announer_ct: Option<CancellationToken>,
    routes_by_conn: DashMap<Arc<SwbusConnInfo>, BTreeSet<RouteEntry>>,
}

/// `SwbusMultiplexer` is responsible for managing routes and connections within the Swbus system.
/// It maintains a route table and handles route announcements, registrations, and unregistrations of connections.
///
/// # Methods
///
/// - `new(routes: Vec<RouteConfig>) -> Self`
///   - Creates a new `SwbusMultiplexer` instance with the given routes.
///   - Panics if no `InCluster` route is provided.
///
/// - `set_route_announcer(&mut self, tx: mpsc::Sender<RouteAnnounceTask>, ct: CancellationToken)`
///   - Sets the route announcer with the provided sender and cancellation token.
///
/// - `generate_message_id(&self) -> u64`
///   - Generates a new message ID using the internal ID generator.
///
/// - `route_from_conn(&self, conn_info: &Arc<SwbusConnInfo>) -> String`
///   - Determines the route key based on the connection type of the provided connection info.
///
/// - `register(&self, conn_info: &Arc<SwbusConnInfo>, proxy: SwbusConnProxy)`
///   - Registers a new connection and updates the route table accordingly.
///   - Announces the routes to peers if the connection is not of type `Client`.
///
/// - `unregister(&self, conn_info: &Arc<SwbusConnInfo>)`
///   - Unregisters a connection and updates the route table accordingly.
///   - Announces the route removal to peers if necessary.
///
/// - `update_route(&self, route_key: String, nexthop: SwbusNextHop) -> bool`
///   - Updates the route for the given service path with the new next hop.
///   - Returns `true` if the route was updated, otherwise `false`.
///
/// - `remove_route_to_nh(&self, route_key: &str, nexthop: &SwbusNextHop) -> bool`
///   - Removes the specified route to the given next hop.
///   - Returns `true` if the route was removed, otherwise `false`.
///
/// - `remove_route(&self, route_key: &str) -> bool`
///   - Removes the specified route.
///   - Returns `true` if the route was removed, otherwise `false`.
///
/// - `get_nexthop_to_conn(&self, conn_info: &Arc<SwbusConnInfo>) -> Result<SwbusNextHop>`
///   - Retrieves the next hop to the given connection.
///   - Returns an error if the route to the connection is not found.
///
/// - `key_from_route_entry(entry: &RouteEntry) -> Result<String>`
///   - Generates a route key from the given route entry.
///
/// - `process_route_announcement(&self, routes: RouteEntries, conn_info: Arc<SwbusConnInfo>) -> Result<()>`
///   - Processes a route announcement from a connection.
///   - Updates the route table and announces the routes to peers if necessary.
///
/// - `announce_routes(&self, conn_info: &Arc<SwbusConnInfo>, trigger: TriggerType) -> Result<()>`
///   - Announces routes to peers based on the provided trigger type.
///
/// - `get_my_service_path(&self) -> &ServicePath`
///   - Returns the service path of the current instance.
///
/// - `route_message(&self, message: SwbusMessage) -> Result<()>`
///   - Routes a message to the appropriate next hop.
///   - Returns an error if no route is found.
///
/// - `dump_route_table(&self) -> RouteEntries`
///   - Dumps the current route table for display purposes.
///
/// - `export_routes(&self, target_conn: &Arc<SwbusConnInfo>) -> Option<RouteEntries>`
///   - Exports routes for route exchange between Swbus instances.
///   - Skips routes that go through the target connection.
///
/// - `export_connections(&self) -> Vec<Arc<SwbusConnInfo>>`
///   - Exports the list of current connections.
///
/// - `shutdown(&mut self)`
///   - Shuts down the multiplexer by canceling the route announcer task.
impl SwbusMultiplexer {
    pub fn new(routes: Vec<RouteConfig>) -> Self {
        let mut my_service_path = None;
        let initial_routes: DashMap<String, BTreeSet<SwbusNextHop>> = DashMap::new();
        let mut my_routes: HashMap<ServicePath, RouteScope> = HashMap::new();

        for route in routes {
            if route.scope == RouteScope::InCluster {
                // Create local service route
                let route_key = route.key.to_incluster_prefix();
                let local_nh = SwbusNextHop::new_local();
                initial_routes.entry(route_key).or_default().insert(local_nh);
                my_service_path = Some(route.key.clone());
            }
            // my_routes are to be announced to the peers
            my_routes.insert(route.key, route.scope);
        }
        if my_service_path.is_none() {
            panic!("my routes must include a InCluster route");
        }

        SwbusMultiplexer {
            routes: initial_routes,
            id_generator: MessageIdGenerator::new(),
            my_routes,
            my_service_path: my_service_path.unwrap(),
            connections: DashSet::new(),
            route_annouce_task_tx: None,
            route_announer_ct: None,
            routes_by_conn: DashMap::new(),
        }
    }

    pub(crate) fn set_route_announcer(&mut self, tx: mpsc::Sender<RouteAnnounceTask>, ct: CancellationToken) {
        self.route_annouce_task_tx = Some(tx);
        self.route_announer_ct = Some(ct);
    }

    pub fn generate_message_id(&self) -> u64 {
        self.id_generator.generate()
    }

    /// get route from conn_info based on connection type. This is a direct route which means it is to a immediate nexthop (1 hop away).
    fn route_from_conn(&self, conn_info: &Arc<SwbusConnInfo>) -> String {
        let remote_sp = conn_info
            .remote_service_path()
            .as_ref()
            .expect("remote_service_path should be set");
        match conn_info.connection_type() {
            // direct route Global, InRegion, InCluster connection type is always to node level
            ConnectionType::Global | ConnectionType::InRegion | ConnectionType::InCluster => {
                remote_sp.to_incluster_prefix()
            }
            // both Client (for CLI) and InNode route by service prefix. IOW, service prefix uniquely
            // identify the endpoint to the client (CLI or hamgrd)
            ConnectionType::InNode | ConnectionType::Client => remote_sp.to_service_prefix(),
        }
    }

    pub(crate) fn register(&self, conn_info: &Arc<SwbusConnInfo>, proxy: SwbusConnProxy) {
        // Update the route table.
        let remote_sp = conn_info
            .remote_service_path()
            .as_ref()
            .expect("remote_service_path should be set");
        if self.my_routes.contains_key(remote_sp) {
            error!(
                "Conflicting service path to my routes detected from connection: {}",
                conn_info.id()
            );
            return;
        }

        let direct_route = self.route_from_conn(conn_info);

        let nexthop = SwbusNextHop::new_remote(conn_info.clone(), proxy, 1);
        self.update_route(direct_route, nexthop);

        // no need to add client connection or anounce client connection to the peers
        if conn_info.connection_type() == ConnectionType::Client {
            return;
        }

        self.connections.insert(conn_info.clone());

        self.announce_routes(conn_info, TriggerType::ConnectionUp)
            .unwrap_or_else(|e| {
                error!("Failed to send route announcement: {:?}", e);
            });
    }

    pub(crate) fn unregister(&self, conn_info: &Arc<SwbusConnInfo>) {
        // remove the route entry from the route table.
        let direct_route = self.route_from_conn(conn_info);

        // no need to add client connection or anounce client connection to the peers
        if conn_info.connection_type() == ConnectionType::Client {
            self.remove_route(&direct_route);
            return;
        }

        // get old routes, or create an empty one, and hold lock on the entry
        let old_routes = self.routes_by_conn.entry(conn_info.clone()).or_default();

        // create a dummy nexthop from conn_info to compare with the nexthop in the old_routes
        let dummy_nh = SwbusNextHop::new_dummy_remote(conn_info.clone(), 1);
        let mut need_announce = false;

        for entry in old_routes.iter() {
            match Self::key_from_route_entry(entry) {
                Ok(route_key) => {
                    let old_nh = dummy_nh.clone_with_hop_count(entry.hop_count + 1);
                    let route_removed = self.remove_route_to_nh(&route_key, &old_nh);
                    if route_removed {
                        need_announce = true;
                    }
                }
                Err(e) => {
                    error!("Failed to get route key from the route entry: {:?}, {:?}", entry, e);
                }
            }
        }

        // remove the direct route
        let route_removed = self.remove_route_to_nh(&direct_route, &dummy_nh);
        if route_removed {
            need_announce = true;
        }

        self.connections.remove(conn_info);

        // release the lock on the entry
        drop(old_routes);
        self.routes_by_conn.remove(conn_info);

        if need_announce {
            self.announce_routes(conn_info, TriggerType::ConnectionDown)
                .unwrap_or_else(|e| {
                    error!("Failed to send route announcement: {:?}", e);
                });
        }
    }

    // Update route for the give service path with the new nexthop. If the route is updated, return true.
    // Otherwise, return false.
    #[instrument(name = "update_route", level = "info", skip(self, nexthop), fields(nh_type=?nexthop.nh_type(), hop_count=nexthop.hop_count(), conn_info=nexthop.conn_info().as_ref().map(|x| x.id()).unwrap_or(&"None".to_string())))]
    fn update_route(&self, route_key: String, nexthop: SwbusNextHop) -> bool {
        // If route entry doesn't exist, we insert the next hop as a new one.
        let inserted = self.routes.entry(route_key).or_default().insert(nexthop.clone());
        info!("Update route entry: inserted={}", inserted);
        inserted
    }

    /// remove a specified route and to the specified nexthop
    #[instrument(name = "remove_route_to_nh", level = "info", skip(self, nexthop), fields(nh_type=?nexthop.nh_type(), hop_count=nexthop.hop_count(), conn_info=nexthop.conn_info().as_ref().map(|x| x.id()).unwrap_or(&"None".to_string())))]
    fn remove_route_to_nh(&self, route_key: &str, nexthop: &SwbusNextHop) -> bool {
        let mut removed = false;
        let mut remove_all = false;
        if let Some(mut entry) = self.routes.get_mut(route_key) {
            removed = entry.remove(nexthop);
            if entry.is_empty() {
                remove_all = true;
                // can't remove route here because we are holding the lock on the entry.
            }
        }
        if remove_all {
            self.routes.remove(route_key);
        }

        info!("Remove route entry: removed={}", removed);
        removed
    }

    /// remove a specified route
    #[instrument(name = "remove_route", level = "info", skip(self))]
    fn remove_route(&self, route_key: &str) -> bool {
        let removed = self.routes.remove(route_key);
        info!("Remove route entry: removed={}", removed.is_some());
        removed.is_some()
    }

    /// get the next hop to the connection. This is a direct route which means it is to a immediate nexthop (1 hop away).
    fn get_nexthop_to_conn(&self, conn_info: &Arc<SwbusConnInfo>) -> Result<SwbusNextHop> {
        // get nexthop from conn_info
        let direct_route = self.route_from_conn(conn_info);

        let nhs = self.routes.get(&direct_route).ok_or_else(|| {
            // This could happen due to race condition if the connection is just removed.
            SwbusError::internal(
                SwbusErrorCode::Fail,
                format!(
                    "Receive route announcement from {:?} but route to the connection {} is not found ",
                    conn_info.remote_service_path(),
                    direct_route
                ),
            )
        })?;

        let nh = nhs
            .iter()
            .find(|nh| nh.conn_info().as_ref().unwrap().id() == conn_info.id());

        if let Some(nh) = nh {
            debug!("Found direct route to the connection: {}", direct_route);
            Ok(nh.clone())
        } else {
            Err(SwbusError::internal(
                SwbusErrorCode::Fail,
                format!(
                    "Receive route announcement from {:?} but nh of the route is not from same connection: {}",
                    conn_info.remote_service_path(),
                    direct_route
                ),
            ))
        }
    }

    fn key_from_route_entry(entry: &RouteEntry) -> Result<String> {
        let sp: &ServicePath = match entry.service_path {
            None => Err(SwbusError::input(
                SwbusErrorCode::InvalidRoute,
                "Route entry missing service path".to_string(),
            ))?,
            Some(ref sp) => sp,
        };
        Ok(match RouteScope::try_from(entry.route_scope) {
            Ok(RouteScope::Global) => sp.to_global_prefix(),
            Ok(RouteScope::InRegion) => sp.to_inregion_prefix(),
            Ok(RouteScope::InCluster) => sp.to_incluster_prefix(),
            _ => Err(SwbusError::input(
                SwbusErrorCode::InvalidRoute,
                "Invalid route scope".to_string(),
            ))?,
        })
    }

    /// processing route announcement from a connection. RouteEntry in the announcement is a set of routes
    /// that the connection can reach. RouteEntry only includes the service path and hop count from the connection.
    pub(crate) fn process_route_announcement(
        &self,
        routes: RouteEntries,
        conn_info: &Arc<SwbusConnInfo>,
    ) -> Result<()> {
        debug!(
            "Begin processing route announcement from {:?}",
            conn_info.remote_service_path()
        );

        // get nh to the connection, which will be used as the next hop to the routes in the announcement
        let nh = self.get_nexthop_to_conn(conn_info)?;

        let mut new_routes: BTreeSet<RouteEntry> = BTreeSet::new();

        // FILTER OUT MY ROUTES. MY ROUTES already HAVE LOWEST HOP COUNT.
        for entry in routes.entries.into_iter() {
            if entry.service_path.is_none() {
                error!(
                    "Received route announcement with missing service path from {:?}",
                    conn_info.remote_service_path()
                );
                continue;
            }
            if self.my_routes.contains_key(entry.service_path.as_ref().unwrap()) {
                debug!("Skip adding my route: {}", entry.service_path.as_ref().unwrap());
                continue;
            }
            new_routes.insert(entry.clone());
        }
        let mut need_announce = false;

        // get old routes, or create an empty one, and hold lock on the entry
        let mut old_routes = self.routes_by_conn.entry(conn_info.clone()).or_default();
        debug!("Old routes from conn {:?}: {:?}", conn_info.id(), *old_routes);
        debug!("New routes from conn {:?}: {:?}", conn_info.id(), new_routes);
        let routes_to_remove: BTreeSet<RouteEntry> = old_routes.difference(&new_routes).cloned().collect();
        let routes_to_add: BTreeSet<RouteEntry> = new_routes.difference(&old_routes).cloned().collect();

        for entry in routes_to_remove {
            let route_key = match Self::key_from_route_entry(&entry) {
                Ok(route_key) => route_key,
                Err(e) => {
                    error!("Failed to get route key from the route entry: {:?}, {:?}", entry, e);
                    continue;
                }
            };
            let old_nh = nh.clone_with_hop_count(entry.hop_count + 1);
            let route_removed = self.remove_route_to_nh(&route_key, &old_nh);
            if route_removed {
                need_announce = true;
            }
        }

        for entry in routes_to_add {
            // clone the nexthop but increment hop count and use it in the routes from the announcement
            let route_key = match Self::key_from_route_entry(&entry) {
                Ok(route_key) => route_key,
                Err(e) => {
                    error!("Failed to get route key from the route entry: {:?}, {:?}", entry, e);
                    continue;
                }
            };
            let new_nh = nh.clone_with_hop_count(entry.hop_count + 1);

            let route_changed = self.update_route(route_key, new_nh);
            if route_changed {
                need_announce = true;
            }
        }

        // notify route_announcer to send the routes to the other peers only if routes are updated.
        if need_announce {
            self.announce_routes(conn_info, TriggerType::RouteUpdated)?;
        }
        *old_routes = new_routes;
        debug!(
            "Finished processing route announcement from {:?}",
            conn_info.remote_service_path()
        );
        Ok(())
    }

    fn announce_routes(&self, conn_info: &Arc<SwbusConnInfo>, trigger: TriggerType) -> Result<()> {
        if let Some(tx) = &self.route_annouce_task_tx {
            return match tx.try_send(RouteAnnounceTask::new(trigger, conn_info.clone())) {
                Ok(_) => Ok(()),
                Err(e) => match e {
                    TrySendError::Full(_) => Err(SwbusError::route(SwbusErrorCode::QueueFull, e.to_string())),
                    _ => Err(SwbusError::route(SwbusErrorCode::NoRoute, e.to_string())),
                },
            };
        }
        Ok(())
    }

    pub fn get_my_service_path(&self) -> &ServicePath {
        &self.my_service_path
    }

    #[instrument(name="route_message", parent=None, level="debug", skip_all, fields(message_id=?message.header.as_ref().unwrap().id))]
    pub async fn route_message(&self, message: SwbusMessage) -> Result<()> {
        debug!(
            destination = message
                .header
                .as_ref()
                .unwrap()
                .destination
                .as_ref()
                .unwrap()
                .to_longest_path(),
            "Routing message"
        );

        let header = match message.header {
            Some(ref header) => header,
            None => {
                return Err(SwbusError::input(
                    SwbusErrorCode::InvalidHeader,
                    "missing message header".to_string(),
                ))
            }
        };

        let destination = match header.destination {
            Some(ref destination) => destination,
            None => {
                return Err(SwbusError::input(
                    SwbusErrorCode::InvalidDestination,
                    "missing message destination".to_string(),
                ))
            }
        };

        for stage in &ROUTE_STAGES {
            let route_key = match stage {
                RouteStage::Local => destination.to_service_prefix(),
                RouteStage::Cluster => destination.to_incluster_prefix(),
                RouteStage::Region => destination.to_inregion_prefix(),
                RouteStage::Global => destination.to_global_prefix(),
            };
            // If the route entry doesn't exist, we drop the message.
            let nhs = match self.routes.get(&route_key) {
                Some(entry) => entry,
                None => {
                    continue;
                }
            };
            for nh in nhs.iter() {
                // If the route entry is resolved, we forward the message to the next hop.
                match nh.queue_message(self, message.clone()).await {
                    Ok(response) => {
                        if let Some(response) = response {
                            Box::pin(self.route_message(response)).await?;
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        error!(
                            "Failed to queue message to the next hop: {}. Will try with the next nexthop",
                            e
                        );
                    }
                }
            }
        }

        info!("No route found for destination: {}", destination.to_longest_path());
        let response = SwbusMessage::new_response(
            message.header.as_ref().unwrap(),
            Some(self.get_my_service_path()),
            SwbusErrorCode::NoRoute,
            "Route not found",
            self.id_generator.generate(),
            None,
        );
        // there is a risk of loop if no route to the source of request.
        // A receives a request from B to C but A doesn't have route to C. It sends response[1] to B
        // but B is also unreachable.
        // Here it will send 'no-route' response[2] for response[1]. response[1] has source SP of A
        // because the response is originated from A. So response[2]'s dest is to A (itself).
        // Response[2] will be sent to a drop nexhop, which should drop the unexpected response packet.
        Box::pin(self.route_message(response)).await?;

        Ok(())
    }

    /// dump_route_table exports routes for swbus-cli show routes, which needs all the details about routes and nexthops.
    pub(crate) fn dump_route_table(&self) -> RouteEntries {
        debug!("Dumping route table");
        let entries: Vec<RouteEntry> = self
            .routes
            .iter()
            .filter(|entry| {
                if !matches!(entry.value().first(), Some(s) if s.nh_type() == NextHopType::Remote) {
                    return false;
                }
                true
            })
            .flat_map(|entry| {
                // flatten the routes with one entry per nexthop
                debug!("Dumping route {}: {:?}", entry.key(), entry.value());

                entry
                    .value()
                    .iter()
                    .filter(|nh| {
                        // skip Client connection from CLI
                        nh.conn_info().is_some()
                            && nh.conn_info().as_ref().unwrap().connection_type() != ConnectionType::Client
                    })
                    .map(|nh| RouteEntry {
                        service_path: Some(
                            ServicePath::from_string(entry.key())
                                .expect("Not expecting service_path in route table to be invalid"),
                        ),
                        hop_count: nh.hop_count(),
                        nh_id: nh.conn_info().as_ref().unwrap().id().to_string(),
                        nh_service_path: nh.conn_info().as_ref().unwrap().remote_service_path().clone(),
                        route_scope: ServicePath::from_string(entry.key()).unwrap().route_scope() as i32,
                    })
                    .collect::<Vec<RouteEntry>>()
            })
            .collect();
        RouteEntries { entries }
    }

    /// Exports routes based on the connection type and target scope.
    ///
    /// This function filters and processes the routes to be exported to a target connection.
    /// It considers the connection type and target scope to determine which routes are eligible
    /// for export. The function performs the following steps:
    ///
    /// 1. Determines the target scope based on the connection type of the target connection.
    /// 2. Filters the routes to include only those with a `NextHopType::Remote` and a route scope
    ///    greater than or equal to the target scope.
    /// 3. For each eligible route, it finds the lowest hop count and creates new `RouteEntry` objects
    ///    for routes with the lowest hop count and active status, excluding routes that go through
    ///    the peer to which the routes are being exported.
    /// 4. Additionally, it exports the routes from `my_routes` with a hop count of 0 and a route scope
    ///    greater than or equal to the target scope.
    /// 5. Combines the filtered and processed routes into a `RouteEntries` object and returns it.
    ///
    /// # Arguments
    ///
    /// * `target_conn` - A reference to the target connection information.
    ///
    /// # Returns
    ///
    /// An `Option<RouteEntries>` containing the exported routes. If no routes are eligible for export,
    /// an empty `RouteEntries` object is returned to allow the caller to send an empty route announcement
    /// to the peer to remove all routes.usd to reach the destination.
    pub fn export_routes(&self, target_conn: &Arc<SwbusConnInfo>) -> Option<RouteEntries> {
        let conn_type = target_conn.connection_type();
        let target_scope = match conn_type {
            ConnectionType::InCluster => RouteScope::InCluster,
            ConnectionType::InRegion => RouteScope::InRegion,
            ConnectionType::Global => RouteScope::Global,
            _ => return None,
        };

        let mut entries: Vec<RouteEntry> = self
            .routes
            .iter()
            .filter(|entry| {
                if !matches!(entry.value().first(), Some(s) if s.nh_type() == NextHopType::Remote) {
                    return false;
                }
                let route_scope = ServicePath::from_string(entry.key()).unwrap().route_scope();
                route_scope >= target_scope
            })
            .flat_map(|entry| {
                // Get the lowest hop_count (if set is non-empty)
                let lowest_hop = match entry.value().iter().next() {
                    Some(first) => first.hop_count(),
                    None => return vec![], // Empty set, return empty vec
                };

                // Iterate, filter for lowest hop_count and active, create new objects
                entry.value().iter()
                    .take_while(|nh| nh.hop_count() <= lowest_hop + 1) // Stop after lowest group
                    // do not export routes that go through the peer to which the routes are being exported
                    .find(|nh| nh.conn_info().as_ref().unwrap().remote_service_path() != target_conn.remote_service_path())
                    .map(|nh| vec![RouteEntry {
                            service_path: Some(
                                ServicePath::from_string(entry.key())
                                    .expect("Not expecting service_path in route table to be invalid"),
                            ),
                            hop_count: nh.hop_count(),
                            nh_id: "".to_string(),
                            nh_service_path: None,
                            route_scope: ServicePath::from_string(entry.key()).unwrap().route_scope() as i32,
                        }])
                    .unwrap_or(vec![])
            })
            .collect();

        // export my_routes with hop count 0
        let my_route_entries: Vec<RouteEntry> = self
            .my_routes
            .iter()
            .filter(|(_, &value)| value >= target_scope)
            .map(|(key, value)| RouteEntry {
                service_path: Some(key.clone()),
                hop_count: 0,
                nh_id: "".to_string(),
                nh_service_path: None,
                route_scope: *value as i32,
            })
            .collect();

        entries.extend(my_route_entries);
        // even if there is no route, return an empty RouteEntries so the caller can send an empty route announcement to the peer to remove all routes.

        Some(RouteEntries { entries })
    }

    pub fn export_connections(&self) -> Vec<Arc<SwbusConnInfo>> {
        self.connections.iter().map(|conn| conn.clone()).collect()
    }

    pub fn shutdown(&mut self) {
        if let Some(tx) = &self.route_announer_ct {
            tx.cancel();
            self.route_announer_ct = None;
            self.route_annouce_task_tx = None;
        }
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use crate::mux::SwbusConn;
    use tokio::time::Duration;
    use tokio::{sync::mpsc, time};
    use tonic::Status;

    // Helper function to create a new SwbusConn for testing.
    pub(crate) fn new_conn_for_test(
        conn_type: ConnectionType,
        sp: &str,
    ) -> (SwbusConn, mpsc::Receiver<Result<SwbusMessage, Status>>) {
        new_conn_for_test_with_endpoint(conn_type, sp, "127.0.0.1:8080")
    }

    pub(crate) fn new_conn_for_test_with_endpoint(
        conn_type: ConnectionType,
        sp: &str,
        nh_endpoint: &str,
    ) -> (SwbusConn, mpsc::Receiver<Result<SwbusMessage, Status>>) {
        let conn_info = Arc::new(SwbusConnInfo::new_server(
            conn_type,
            nh_endpoint.parse().unwrap(),
            ServicePath::from_string(sp).unwrap(),
        ));
        let (send_queue_tx, send_queue_rx) = mpsc::channel(16);
        (SwbusConn::new(&conn_info, send_queue_tx), send_queue_rx)
    }
    // Helper function to add a route to the multiplexer.
    pub(crate) fn add_route(
        mux: &SwbusMultiplexer,
        route_key: &str,
        hop_count: u32,
        conn: &SwbusConn,
    ) -> Option<RouteEntry> {
        let nexthop_nh1 = SwbusNextHop::new_remote(conn.info().clone(), conn.new_proxy(), hop_count);
        if mux.update_route(route_key.to_string(), nexthop_nh1) {
            Some(RouteEntry {
                service_path: Some(ServicePath::from_string(route_key).unwrap()),
                hop_count,
                nh_id: conn.info().id().to_string(),
                nh_service_path: conn.info().remote_service_path().clone(),
                route_scope: ServicePath::from_string(route_key).unwrap().route_scope() as i32,
            })
        } else {
            None
        }
    }

    pub(crate) async fn receive_and_compare(
        send_queue_rx: &mut mpsc::Receiver<Result<SwbusMessage, Status>>,
        expected: &str,
    ) {
        let expected_msg: SwbusMessage = serde_json::from_str(expected).unwrap();
        match time::timeout(Duration::from_secs(1), send_queue_rx.recv()).await {
            Ok(Some(msg)) => {
                let normalized_msg = swbus_proto::swbus::normalize_msg(&msg.ok().unwrap());

                assert_eq!(normalized_msg, expected_msg);
            }
            _ => {
                panic!("No message received");
            }
        }
    }

    pub(crate) async fn route_message_and_compare(
        mux: &SwbusMultiplexer,
        send_queue_rx: &mut mpsc::Receiver<Result<SwbusMessage, Status>>,
        request: &str,
        expected: &str,
    ) {
        let request_msg: SwbusMessage = serde_json::from_str(request).unwrap();

        let result = mux.route_message(request_msg).await;
        assert!(result.is_ok());
        receive_and_compare(send_queue_rx, expected).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::test_utils::*;
    use crate::mux::SwbusConn;
    use pretty_assertions::assert_eq;
    use tokio::sync::mpsc;

    // Helper function to create a new RouteEntry for route announcement.
    fn new_route_entry_for_ra(sp: &str, hop_count: u32) -> RouteEntry {
        let sp = ServicePath::from_string(sp).unwrap();
        let route_scope = sp.route_scope();
        RouteEntry {
            service_path: Some(sp),
            hop_count,
            nh_id: "".to_string(),
            nh_service_path: None,
            route_scope: route_scope as i32,
        }
    }

    fn route_config_to_route_entry(route_config: &RouteConfig) -> RouteEntry {
        RouteEntry {
            service_path: Some(route_config.key.clone()),
            hop_count: 0,
            nh_id: "".to_string(),
            nh_service_path: None,
            route_scope: route_config.scope as i32,
        }
    }

    fn clone_route_entry_without_nh(route_entry: &RouteEntry) -> RouteEntry {
        RouteEntry {
            service_path: route_entry.service_path.clone(),
            hop_count: route_entry.hop_count,
            nh_id: "".to_string(),
            nh_service_path: None,
            route_scope: route_entry.route_scope,
        }
    }

    #[test]
    fn test_set_my_routes() {
        let incluster_route = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: RouteScope::InCluster,
        };
        let inregion_route = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a").unwrap(),
            scope: RouteScope::InRegion,
        };

        let mux = Arc::new(SwbusMultiplexer::new(vec![
            incluster_route.clone(),
            inregion_route.clone(),
        ]));

        assert_eq!(*mux.my_routes.get(&incluster_route.key).unwrap(), RouteScope::InCluster);
        assert_eq!(*mux.my_routes.get(&inregion_route.key).unwrap(), RouteScope::InRegion);

        let nhs = mux.routes.get(&incluster_route.key.to_incluster_prefix()).unwrap();
        assert_eq!(nhs.first().unwrap().nh_type(), NextHopType::Local);
        assert_eq!(&incluster_route.key, mux.get_my_service_path());
    }

    #[tokio::test]
    async fn test_route_message() {
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            scope: RouteScope::InCluster,
        };

        let mux = Arc::new(SwbusMultiplexer::new(vec![route_config.clone()]));

        let (conn1, _) = new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.10.0.0.1-dpu0");
        add_route(&mux, "region-a.cluster-a.10.0.0.1-dpu0", 1, &conn1).unwrap();

        let (conn3, mut send_queue_rx3) =
            new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.10.0.0.3-dpu0");
        add_route(&mux, "region-a.cluster-a.10.0.0.3-dpu0", 1, &conn3).unwrap();

        let request = r#"
            {
              "header": {
                "version": 1,
                "id": 0,
                "flag": 0,
                "ttl": 63,
                "source": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0",
                "destination": "region-a.cluster-a.10.0.0.3-dpu0/local-mgmt/0"
              },
              "body": {
                "PingRequest": {}
              }
            }
            "#;
        let expected = r#"
            {
                "header": {
                    "version": 1,
                    "id": 0,
                    "flag": 0,
                    "ttl": 62,
                    "source": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0",
                    "destination": "region-a.cluster-a.10.0.0.3-dpu0/local-mgmt/0"
                },
                "body": {
                    "PingRequest": {}
                }
            }
            "#;
        route_message_and_compare(&mux, &mut send_queue_rx3, request, expected).await;
    }

    #[tokio::test]
    async fn test_route_message_unreachable() {
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            scope: RouteScope::InCluster,
        };

        let mux = Arc::new(SwbusMultiplexer::new(vec![route_config.clone()]));

        let (conn1, mut send_queue_rx1) =
            new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.10.0.0.1-dpu0");
        add_route(&mux, "region-a.cluster-a.10.0.0.1-dpu0", 1, &conn1).unwrap();

        let (conn3, _) = new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.10.0.0.3-dpu0");
        add_route(&mux, "region-a.cluster-a.10.0.0.3-dpu0", 1, &conn3).unwrap();

        let request = r#"
            {
                "header": {
                "version": 1,
                "id": 0,
                "flag": 0,
                "ttl": 1,
                "source": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0",
                "destination": "region-a.cluster-a.10.0.0.3-dpu0/local-mgmt/0"
                },
                "body": {
                "PingRequest": {}
                }
            }
            "#;
        let expected = r#"
            {
                "header": {
                    "version": 1,
                    "id": 0,
                    "flag": 0,
                    "ttl": 63,
                    "source": "region-a.cluster-a.10.0.0.2-dpu0",
                    "destination": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0"
                },
                "body": {
                    "Response": {
                    "request_id": 0,
                    "error_code": 303,
                    "error_message": "TTL expired",
                    "response_body": null
                    }
                }
            }
            "#;
        route_message_and_compare(&mux, &mut send_queue_rx1, request, expected).await;
    }

    #[tokio::test]
    async fn test_route_message_noroute() {
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            scope: RouteScope::InCluster,
        };

        let mux = Arc::new(SwbusMultiplexer::new(vec![route_config.clone()]));

        let (conn1, mut send_queue_rx1) =
            new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.10.0.0.1-dpu0");
        add_route(&mux, "region-a.cluster-a.10.0.0.1-dpu0", 1, &conn1).unwrap();

        let request = r#"
            {
                "header": {
                "version": 1,
                "id": 0,
                "flag": 0,
                "ttl": 63,
                "source": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0",
                "destination": "region-a.cluster-a.10.0.0.3-dpu0/local-mgmt/0"
                },
                "body": {
                "PingRequest": {}
                }
            }
            "#;
        let expected = r#"
            {
                "header": {
                    "version": 1,
                    "id": 0,
                    "flag": 0,
                    "ttl": 63,
                    "source": "region-a.cluster-a.10.0.0.2-dpu0",
                    "destination": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0"
                },
                "body": {
                    "Response": {
                    "request_id": 0,
                    "error_code": 301,
                    "error_message": "Route not found",
                    "response_body": null
                    }
                }
            }
            "#;
        route_message_and_compare(&mux, &mut send_queue_rx1, request, expected).await;
    }

    #[tokio::test]
    async fn test_route_message_isolated() {
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            scope: RouteScope::InCluster,
        };

        let mux = Arc::new(SwbusMultiplexer::new(vec![route_config.clone()]));

        let request = r#"
            {
                "header": {
                "version": 1,
                "id": 0,
                "flag": 0,
                "ttl": 63,
                "source": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0",
                "destination": "region-a.cluster-a.10.0.0.3-dpu0/local-mgmt/0"
                },
                "body": {
                "PingRequest": {}
                }
            }
            "#;

        let request_msg: SwbusMessage = serde_json::from_str(request).unwrap();

        let result = mux.route_message(request_msg).await;
        // packet should be dropped and not causing stack overflow
        assert!(result.is_ok());
    }

    #[tokio::test]
    /// Create alternative paths and verify the message is routed to the new path when the primary path is down
    async fn test_route_message_failover() {
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            scope: RouteScope::InCluster,
        };

        let mux = Arc::new(SwbusMultiplexer::new(vec![route_config.clone()]));

        let (conn1, mut send_queue_rx1) =
            new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.10.0.0.1-dpu0");
        add_route(&mux, "region-a.cluster-a.10.0.0.3-dpu0", 1, &conn1).unwrap();

        // add an alternative route
        let (conn3, mut send_queue_rx3) =
            new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.10.0.0.3-dpu0");
        add_route(&mux, "region-a.cluster-a.10.0.0.3-dpu0", 3, &conn3).unwrap();

        let request = r#"
            {
              "header": {
                "version": 1,
                "id": 0,
                "flag": 0,
                "ttl": 63,
                "source": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0",
                "destination": "region-a.cluster-a.10.0.0.3-dpu0/local-mgmt/0"
              },
              "body": {
                "PingRequest": {}
              }
            }
            "#;
        let expected = r#"
            {
                "header": {
                    "version": 1,
                    "id": 0,
                    "flag": 0,
                    "ttl": 62,
                    "source": "region-a.cluster-a.10.0.0.1-dpu0/testsvc/0/ping/0",
                    "destination": "region-a.cluster-a.10.0.0.3-dpu0/local-mgmt/0"
                },
                "body": {
                    "PingRequest": {}
                }
            }
            "#;
        route_message_and_compare(&mux, &mut send_queue_rx1, request, expected).await;

        // close the primary path
        send_queue_rx1.close();
        route_message_and_compare(&mux, &mut send_queue_rx3, request, expected).await;
    }

    #[test]
    fn test_dump_route_table() {
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.node0").unwrap(),
            scope: RouteScope::InCluster,
        };

        let mux = Arc::new(SwbusMultiplexer::new(vec![route_config.clone()]));

        // build route table
        let (conn1, _) = new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.node1");
        let node1_re1 = add_route(&mux, "region-a.cluster-a.node1", 1, &conn1).unwrap();
        let node1_re2 = add_route(&mux, "region-a.cluster-b", 1, &conn1).unwrap();

        let (conn2, _) = new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.node2");
        let node2_re1 = add_route(&mux, "region-a.cluster-a.node2", 1, &conn2).unwrap();
        let node2_re2 = add_route(&mux, "region-a.cluster-b", 2, &conn2).unwrap();

        // dump route table. Expect: all routes are dumped but not my routes
        let routes = mux.dump_route_table();

        let mut actual: Vec<RouteEntry> = routes.entries;
        actual.sort();

        let mut expected = vec![node1_re1, node1_re2, node2_re1, node2_re2];
        expected.sort();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_export_routes() {
        // create a mux with 2 my-routes
        let my_route1 = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.node0").unwrap(),
            scope: RouteScope::InCluster,
        };
        let my_route2 = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a").unwrap(),
            scope: RouteScope::InRegion,
        };
        let mux = Arc::new(SwbusMultiplexer::new(vec![my_route1.clone(), my_route2.clone()]));

        let my_re1 = route_config_to_route_entry(&my_route1);
        let my_re2 = route_config_to_route_entry(&my_route2);

        // build route table with routes over 2 peers
        let (conn1, _) = new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.node1");
        let _ = add_route(&mux, "region-a.cluster-a.node1", 1, &conn1).unwrap();
        // add a regional route
        let node1_re2 = add_route(&mux, "region-a.cluster-b", 2, &conn1).unwrap();
        let (conn2, _) = new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.node2");
        let node2_re1 = add_route(&mux, "region-a.cluster-a.node2", 1, &conn2).unwrap();
        // add a global route
        let node2_re2 = add_route(&mux, "region-a", 2, &conn2).unwrap();
        // add a route with hop-count+1 to the same destination as node1_re2
        let node2_re3 = add_route(&mux, "region-a.cluster-b", 3, &conn2).unwrap();
        // add a route with hop-count+2 to the same destination as node1_re1
        let _ = add_route(&mux, "region-a.cluster-a.node1", 3, &conn2).unwrap();

        let (conn3, _) = new_conn_for_test(ConnectionType::InRegion, "region-a.cluster-b.node1");

        // export routes to one of the connection.
        // Expect:
        //  1. routes via the target peer are suppressed.
        //  2. alternative routes with the same hop count or hop-count+1 are included.
        //  3. alternative routes with hop-count+2 are suppressed.
        let routes = mux.export_routes(conn1.info());
        let mut actual: Vec<RouteEntry> = routes.unwrap().entries;
        actual.sort();
        let mut expected = vec![
            clone_route_entry_without_nh(&node2_re1),
            clone_route_entry_without_nh(&node2_re2),
            clone_route_entry_without_nh(&node2_re3),
            my_re1.clone(),
            my_re2.clone(),
        ];
        expected.sort();
        assert_eq!(actual, expected);

        // export routes to a InRegion connection. Expect: routes with InCluster scope are suppressed.
        let routes = mux.export_routes(conn3.info());
        let mut actual: Vec<RouteEntry> = routes.unwrap().entries;
        actual.sort();
        let mut expected = vec![
            clone_route_entry_without_nh(&node1_re2),
            clone_route_entry_without_nh(&node2_re2),
            my_re2.clone(),
        ];
        expected.sort();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn test_process_route_announcement() {
        // create a mux with my routes and add a dummy route announcer
        // create a mux with 2 my-routes
        let my_route1 = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.node0").unwrap(),
            scope: RouteScope::InCluster,
        };
        let my_route2 = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a").unwrap(),
            scope: RouteScope::InRegion,
        };
        let mut mux = SwbusMultiplexer::new(vec![my_route1.clone(), my_route2.clone()]);
        let (route_announce_task_tx, mut route_announce_task_rx) = mpsc::channel(16);
        mux.set_route_announcer(route_announce_task_tx, CancellationToken::new());
        let mux = Arc::new(mux);
        // add a direct route to the node1
        let (conn1, _) = new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.node1");
        add_route(&mux, "region-a.cluster-a.node1", 1, &conn1).unwrap();
        let conn1_nh = SwbusNextHop::new_remote(conn1.info().clone(), conn1.new_proxy(), 1);

        // create some route entries
        // route entry matches my routes. Should be skipped
        let node1_re1 = new_route_entry_for_ra("region-a.cluster-a.node1", 0);
        // negative test: route announcement with scope lower than InCluster
        let node1_re2 = new_route_entry_for_ra("region-a.cluster-a.node1/ha/0", 1);
        let node2_re1 = new_route_entry_for_ra("region-a.cluster-a.node2", 1);
        let clusterb_re1 = new_route_entry_for_ra("region-a.cluster-b", 2);

        // Step1: process a route announcement
        let routes = RouteEntries {
            entries: vec![
                node1_re1.clone(),
                node1_re2.clone(),
                node2_re1.clone(),
                clusterb_re1.clone(),
            ],
        };
        let expected_route_entries = BTreeSet::from_iter(vec![
            node1_re1.clone(),
            node1_re2.clone(),
            node2_re1.clone(),
            clusterb_re1.clone(),
        ]);
        mux.process_route_announcement(routes.clone(), conn1.info()).unwrap();
        // Expect:
        //   1. new routes are updated
        //   2. my routes are skipped
        //   3. route anouncement task is created
        //   4. route entries are updated in routes_by_conn
        let mut expected = HashMap::new();
        // add my incluster route
        expected.insert(
            "region-a.cluster-a.node0".to_string(),
            BTreeSet::from([SwbusNextHop::new_local()]),
        );
        expected.insert(
            SwbusMultiplexer::key_from_route_entry(&node1_re1).unwrap(),
            BTreeSet::from([conn1_nh.clone_with_hop_count(1)]),
        );
        expected.insert(
            SwbusMultiplexer::key_from_route_entry(&node2_re1).unwrap(),
            BTreeSet::from([conn1_nh.clone_with_hop_count(2)]),
        );
        expected.insert(
            SwbusMultiplexer::key_from_route_entry(&clusterb_re1).unwrap(),
            BTreeSet::from([conn1_nh.clone_with_hop_count(3)]),
        );

        let actual: HashMap<String, BTreeSet<SwbusNextHop>> = mux
            .routes
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(
            route_announce_task_rx.try_recv(),
            Ok(RouteAnnounceTask::new(TriggerType::RouteUpdated, conn1.info().clone()))
        );
        {
            let routes_by_conn = mux.routes_by_conn.get(conn1.info()).unwrap();
            assert_eq!(*routes_by_conn, expected_route_entries);
        }

        // Step 2: process a route announcement with the same routes
        mux.process_route_announcement(routes.clone(), conn1.info()).unwrap();
        // Expect:
        //   1. no route is updated
        //   2. no route anouncement task is created
        assert_eq!(
            route_announce_task_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        );

        // Step 3: process a route announcement with changed and removed route
        let node2_re1_changed = new_route_entry_for_ra("region-a.cluster-a.node2", 2);
        let clusterb_re1 = new_route_entry_for_ra("region-a.cluster-b", 2);
        let node3_re1 = new_route_entry_for_ra("region-a.cluster-a.node3", 2);
        // process a route announcement
        let routes = RouteEntries {
            entries: vec![
                node1_re1.clone(),
                node1_re2.clone(),
                node2_re1_changed.clone(),
                clusterb_re1.clone(),
                node3_re1.clone(),
            ],
        };
        let expected_route_entries = BTreeSet::from_iter(vec![
            node1_re1.clone(),
            node1_re2.clone(),
            node2_re1_changed.clone(),
            clusterb_re1.clone(),
            node3_re1.clone(),
        ]);
        let mut expected = HashMap::new();
        // add my incluster route
        expected.insert(
            "region-a.cluster-a.node0".to_string(),
            BTreeSet::from([SwbusNextHop::new_local()]),
        );
        expected.insert(
            SwbusMultiplexer::key_from_route_entry(&node1_re1).unwrap(),
            BTreeSet::from([conn1_nh.clone_with_hop_count(1)]),
        );
        expected.insert(
            SwbusMultiplexer::key_from_route_entry(&node2_re1_changed).unwrap(),
            BTreeSet::from([conn1_nh.clone_with_hop_count(3)]),
        );
        expected.insert(
            SwbusMultiplexer::key_from_route_entry(&clusterb_re1).unwrap(),
            BTreeSet::from([conn1_nh.clone_with_hop_count(3)]),
        );
        expected.insert(
            SwbusMultiplexer::key_from_route_entry(&node3_re1).unwrap(),
            BTreeSet::from([conn1_nh.clone_with_hop_count(3)]),
        );
        // Expect:
        //   1. updated routes are updated
        //   2. removed routes are removed
        //   3. route anouncement task is created
        //   4. route entries are updated in routes_by_conn
        mux.process_route_announcement(routes.clone(), conn1.info()).unwrap();

        let actual: HashMap<String, BTreeSet<SwbusNextHop>> = mux
            .routes
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(
            route_announce_task_rx.try_recv(),
            Ok(RouteAnnounceTask::new(TriggerType::RouteUpdated, conn1.info().clone()))
        );
        {
            let routes_by_conn = mux.routes_by_conn.get(conn1.info()).unwrap();
            assert_eq!(*routes_by_conn, expected_route_entries);
        }
    }

    #[tokio::test]
    async fn test_get_nexthop_to_conn() {
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            scope: RouteScope::InCluster,
        };

        let mux = Arc::new(SwbusMultiplexer::new(vec![route_config.clone()]));

        let (conn1, _) = new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.10.0.0.1-dpu0");
        add_route(&mux, "region-a.cluster-a.10.0.0.1-dpu0", 1, &conn1).unwrap();

        // add the same route with a different connection
        let conn_info = Arc::new(SwbusConnInfo::new_server(
            ConnectionType::InCluster,
            "127.0.0.1:8081".parse().unwrap(),
            ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
        ));
        let (send_queue_tx, _) = mpsc::channel(16);
        let conn2 = SwbusConn::new(&conn_info, send_queue_tx);
        add_route(&mux, "region-a.cluster-a.10.0.0.1-dpu0", 1, &conn2).unwrap();

        // Test case: verify we get nexthop for both connections
        let result = mux.get_nexthop_to_conn(conn1.info());
        assert!(result.is_ok());
        let nexthop = result.unwrap();
        assert_eq!(nexthop.conn_info().as_ref().unwrap(), conn1.info());

        let result = mux.get_nexthop_to_conn(conn2.info());
        assert!(result.is_ok());
        let nexthop = result.unwrap();
        assert_eq!(nexthop.conn_info().as_ref().unwrap(), conn2.info());

        // Test case: connection not found
        let (conn2, _) = new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.10.0.0.3-dpu0");
        let result = mux.get_nexthop_to_conn(conn2.info());
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(matches!(error, SwbusError::InternalError { code: _, detail: _ }));
    }

    #[test]
    fn test_remove_existing_nexthop() {
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            scope: RouteScope::InCluster,
        };
        let mux = Arc::new(SwbusMultiplexer::new(vec![route_config.clone()]));

        let route_key = "region-a.cluster-a.node1";
        let (conn1, _) = new_conn_for_test_with_endpoint(
            ConnectionType::InCluster,
            "region-a.cluster-a.10.0.0.1-dpu0",
            "127.0.0.1:8080",
        );
        add_route(&mux, route_key, 1, &conn1).unwrap();
        let nexthop1 = SwbusNextHop::new_remote(conn1.info().clone(), conn1.new_proxy(), 1);

        let (conn2, _) = new_conn_for_test_with_endpoint(
            ConnectionType::InCluster,
            "region-a.cluster-a.10.0.0.1-dpu1",
            "127.0.0.1:8081",
        );
        add_route(&mux, route_key, 1, &conn2).unwrap();
        let nexthop2 = SwbusNextHop::new_remote(conn2.info().clone(), conn2.new_proxy(), 1);

        assert!(mux.remove_route_to_nh(route_key, &nexthop1));
        assert!(mux.routes.contains_key(route_key));
        assert!(mux.remove_route_to_nh(route_key, &nexthop2));

        assert!(!mux.routes.contains_key(route_key)); // Ensure route is removed when empty
    }

    #[test]
    fn test_remove_non_existing_nexthop() {
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            scope: RouteScope::InCluster,
        };
        let mux = Arc::new(SwbusMultiplexer::new(vec![route_config.clone()]));

        let route_key = "region-a.cluster-a.node1";
        let (conn1, _) = new_conn_for_test_with_endpoint(
            ConnectionType::InCluster,
            "region-a.cluster-a.10.0.0.1-dpu0",
            "127.0.0.1:8080",
        );
        add_route(&mux, route_key, 1, &conn1).unwrap();

        let (conn2, _) = new_conn_for_test_with_endpoint(
            ConnectionType::InCluster,
            "region-a.cluster-a.10.0.0.1-dpu1",
            "127.0.0.1:8081",
        );
        let nexthop2 = SwbusNextHop::new_remote(conn2.info().clone(), conn2.new_proxy(), 1);

        assert!(!mux.remove_route_to_nh(route_key, &nexthop2));

        assert!(mux.routes.contains_key(route_key)); // Ensure route is not removed
    }

    #[test]
    fn test_register_unregister() {
        // create mux
        // create a mux with my routes and add a dummy route announcer
        // create a mux with 2 my-routes
        let my_route1 = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.node0").unwrap(),
            scope: RouteScope::InCluster,
        };
        let mut mux = SwbusMultiplexer::new(vec![my_route1.clone()]);
        let (route_announce_task_tx, mut route_announce_task_rx) = mpsc::channel(16);
        mux.set_route_announcer(route_announce_task_tx, CancellationToken::new());
        let mux = Arc::new(mux);

        // register a connection
        let (conn1, _) = new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.node1");
        mux.register(conn1.info(), conn1.new_proxy());
        let conn1_nh = SwbusNextHop::new_remote(conn1.info().clone(), conn1.new_proxy(), 1);
        assert_eq!(
            route_announce_task_rx.try_recv(),
            Ok(RouteAnnounceTask::new(TriggerType::ConnectionUp, conn1.info().clone()))
        );
        assert!(mux.connections.contains(conn1.info()));

        // create some route entries
        let node1_re1 = new_route_entry_for_ra("region-a.cluster-a.node1", 0);
        let node2_re1 = new_route_entry_for_ra("region-a.cluster-a.node2", 1);
        let clusterb_re1 = new_route_entry_for_ra("region-a.cluster-b", 2);

        // Step1: process a route announcement
        let routes = RouteEntries {
            entries: vec![node1_re1.clone(), node2_re1.clone(), clusterb_re1.clone()],
        };
        let expected_route_entries =
            BTreeSet::from_iter(vec![node1_re1.clone(), node2_re1.clone(), clusterb_re1.clone()]);
        mux.process_route_announcement(routes.clone(), conn1.info()).unwrap();
        // Expect:
        //   1. new routes are updated
        //   2. my routes are skipped
        //   3. route anouncement task is created
        //   4. route entries are updated in routes_by_conn
        let mut expected = HashMap::new();
        // add my incluster route
        expected.insert(
            "region-a.cluster-a.node0".to_string(),
            BTreeSet::from([SwbusNextHop::new_local()]),
        );
        expected.insert(
            SwbusMultiplexer::key_from_route_entry(&node1_re1).unwrap(),
            BTreeSet::from([conn1_nh.clone_with_hop_count(1)]),
        );
        expected.insert(
            SwbusMultiplexer::key_from_route_entry(&node2_re1).unwrap(),
            BTreeSet::from([conn1_nh.clone_with_hop_count(2)]),
        );
        expected.insert(
            SwbusMultiplexer::key_from_route_entry(&clusterb_re1).unwrap(),
            BTreeSet::from([conn1_nh.clone_with_hop_count(3)]),
        );

        let actual: HashMap<String, BTreeSet<SwbusNextHop>> = mux
            .routes
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        assert_eq!(actual, expected);

        assert_eq!(
            route_announce_task_rx.try_recv(),
            Ok(RouteAnnounceTask::new(TriggerType::RouteUpdated, conn1.info().clone()))
        );
        {
            let routes_by_conn = mux.routes_by_conn.get(conn1.info()).unwrap();
            assert_eq!(*routes_by_conn, expected_route_entries);
        }

        // unregister the connection
        mux.unregister(conn1.info());
        // Expect:
        // the connection is removed from routes_by_conn
        assert!(!mux.routes_by_conn.contains_key(conn1.info()));
        // the routes in route announcement task are removed
        assert!(!mux
            .routes
            .contains_key(&SwbusMultiplexer::key_from_route_entry(&node1_re1).unwrap()));
        assert!(!mux
            .routes
            .contains_key(&SwbusMultiplexer::key_from_route_entry(&node2_re1).unwrap()));
        assert!(!mux
            .routes
            .contains_key(&SwbusMultiplexer::key_from_route_entry(&clusterb_re1).unwrap()));

        assert_eq!(
            route_announce_task_rx.try_recv(),
            Ok(RouteAnnounceTask::new(
                TriggerType::ConnectionDown,
                conn1.info().clone()
            ))
        );
        assert!(!mux.connections.contains(conn1.info()));
    }

    #[test]
    fn test_register_with_conflict_sp() {
        // create mux
        // create a mux with my routes and add a dummy route announcer
        // create a mux with 2 my-routes
        let my_route1 = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.node0").unwrap(),
            scope: RouteScope::InCluster,
        };
        let mut mux = SwbusMultiplexer::new(vec![my_route1.clone()]);
        let (route_announce_task_tx, _) = mpsc::channel(16);
        mux.set_route_announcer(route_announce_task_tx, CancellationToken::new());
        let mux = Arc::new(mux);

        // register a connection
        let (conn1, _) = new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.node0");
        mux.register(conn1.info(), conn1.new_proxy());
        let route_key = mux.route_from_conn(conn1.info());
        // make the conflicting route is not added
        if let Some(nh_set) = mux.routes.get(&route_key) {
            assert!(!nh_set
                .iter()
                .any(|nh| nh.nh_type() == NextHopType::Remote && nh.conn_info().as_ref().unwrap() == conn1.info()));
        };
    }
}
