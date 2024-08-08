use crate::conn::*;
use crate::contracts::swbus::*;
use dashmap::DashMap;

#[derive(Debug, Clone, Default)]
struct SwbusMuxNextHop {
    conn_id: String,
    hop_count: u32,
}

impl SwbusMuxNextHop {
    pub fn new(conn_id: String, hop_count: u32) -> Self {
        SwbusMuxNextHop { conn_id, hop_count }
    }
}

pub struct SwbusMux {
    /// Route table. Each entry is a registered prefix to a next hop, which points to a connection.
    routes: DashMap<String, SwbusMuxNextHop>,

    /// Connection table. Each entry is a connection id to a connection info.
    connections: DashMap<String, SwbusConn>,
}

impl SwbusMux {
    pub fn new() -> Self {
        SwbusMux {
            routes: DashMap::new(),
            connections: DashMap::new(),
        }
    }

    pub fn register(&self, path: &ServicePath, conn: SwbusConn) {
        // First, we insert the connection to connection table.
        let conn_id = conn.id().to_string();
        let conn_type = conn.connection_type();
        self.connections.insert(conn_id.clone(), conn);

        // Next, we update the route table.
        let route_key = match conn_type {
            ConnectionType::Global => path.to_regional_prefix(),
            ConnectionType::Regional => path.to_cluster_prefix(),
            ConnectionType::Cluster => path.to_node_prefix(),
            ConnectionType::Node => path.to_service_prefix(),
            ConnectionType::Client => path.to_string(),
        };
        let nexthop = SwbusMuxNextHop::new(conn_id, 1);
        self.update_route(route_key, nexthop);
    }

    fn update_route(&self, route_key: String, nexthop: SwbusMuxNextHop) {
        // If route entry doesn't exist, we insert the next hop as a new one.
        let mut route_entry = self.routes.entry(route_key).or_insert(nexthop.clone());

        // If we already have one, then we update the entry only when we have a smaller hop count.
        // The dashmap RefMut reference will hold a lock to the entry, which makes this function atomic.
        if route_entry.hop_count > nexthop.hop_count {
            *route_entry.value_mut() = nexthop;
        }
    }
}
