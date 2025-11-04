use super::SwbusConnInfo;
use super::SwbusMultiplexer;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TriggerType {
    ConnectionUp,
    ConnectionDown,
    RouteUpdated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RouteAnnounceTask {
    trigger: TriggerType,
    conn_info: Arc<SwbusConnInfo>,
}

impl RouteAnnounceTask {
    pub fn new(trigger: TriggerType, conn_info: Arc<SwbusConnInfo>) -> Self {
        Self { trigger, conn_info }
    }
}
impl std::fmt::Display for RouteAnnounceTask {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "trigger: {:?}, conn_info: {:?}",
            self.trigger,
            self.conn_info.remote_service_path()
        )
    }
}
pub struct RouteAnnouncer {
    task_rx: mpsc::Receiver<RouteAnnounceTask>,
    ct: CancellationToken,
    mux: Arc<SwbusMultiplexer>,
}
impl RouteAnnouncer {
    pub fn new(task_rx: mpsc::Receiver<RouteAnnounceTask>, ct: CancellationToken, mux: Arc<SwbusMultiplexer>) -> Self {
        Self { task_rx, ct, mux }
    }
    pub async fn run(&mut self) {
        self.run_loop().await;
    }

    async fn run_loop(&mut self) {
        let mut tasks: Vec<RouteAnnounceTask> = Vec::new();
        loop {
            tokio::select! {
                _ = self.ct.cancelled() => {
                    info!("Route announcer stopped. Shutting down immediately.");
                    break;
                }
                // currently, we send routes to all connections on any trigger so we don't need to differentiate
                // between the tasks. If there are multiple tasks in the queue, we only process the last one.
                // This is to avoid sending multiple route updates because only the last one is the latest.
                num_received = self.task_rx.recv_many(&mut tasks, 100) => {
                    if num_received == 0 {
                        info!("Route announcer task channel closed. Shutting down.");
                        break;
                    }
                    let last_task = tasks.pop().expect("Last task is always present since num_received > 0");
                    for task in tasks.drain(..) {
                        info!("skipping route announce task: {}", task);
                    }
                    self.process_task(last_task).await;
                }
            }
        }
    }

    async fn process_task(&self, task: RouteAnnounceTask) {
        info!("Processing route announce task: {}", task);
        let connections = self.mux.export_connections();

        for conn_info in connections {
            let routes = self.mux.export_routes(&conn_info);
            if routes.is_none() {
                continue;
            }
            let _ = self
                .send_route_announcement(&conn_info, &routes.unwrap())
                .await
                .map_err(|e| {
                    error!(
                        "Failed to send route announcement to {:?}: {}",
                        conn_info.remote_service_path(),
                        e
                    );
                });
        }
    }

    #[instrument(name = "send_route_announcement", level = "debug", skip_all)]
    async fn send_route_announcement(&self, conn_info: &SwbusConnInfo, routes: &RouteEntries) -> Result<()> {
        let dest_sp = conn_info
            .remote_service_path()
            .as_ref()
            .expect("remote_service_path should be set")
            .clone();
        let mut msg = SwbusMessage {
            header: Some(SwbusMessageHeader::new(
                self.mux.get_my_service_path().clone(),
                dest_sp,
                self.mux.generate_message_id(),
            )),
            body: Some(swbus_message::Body::RouteAnnouncement(routes.clone())),
        };
        // count itself as 1 hop. The message is only meant for direct neighbors
        msg.header.as_mut().unwrap().ttl = 2;
        debug!(
            "Sending route announcement to {:?}, conn_info {:?}, message {:?}",
            conn_info.remote_service_path(),
            conn_info,
            &msg
        );
        self.mux.route_message(msg).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::test_utils::*;
    use std::sync::Arc;
    use swbus_config::RouteConfig;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_process_task() {
        // create a mux with 2 my-routes
        let my_route1 = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.node0").unwrap(),
            scope: RouteScope::InCluster,
        };
        let my_route2 = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a").unwrap(),
            scope: RouteScope::InRegion,
        };
        let mux = SwbusMultiplexer::new(vec![my_route1.clone(), my_route2.clone()]);
        // Register 2 connections
        let (conn1, mut conn1_sendq_rx) = new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.node1");
        mux.register(conn1.info(), conn1.new_proxy());
        let (conn2, mut conn2_sendq_rx) = new_conn_for_test(ConnectionType::InCluster, "region-a.cluster-a.node2");
        mux.register(conn2.info(), conn2.new_proxy());

        // Add some routes
        // add a regional route
        add_route(&mux, "region-a.cluster-b", 2, &conn1).unwrap();

        // add a global route
        add_route(&mux, "region-a", 2, &conn2).unwrap();

        // Create a route announce task
        let (tx, rx) = mpsc::channel(1);

        let mut announcer = RouteAnnouncer::new(rx, CancellationToken::new(), Arc::new(mux));
        tokio::spawn(async move {
            announcer.run().await;
        });

        let task = RouteAnnounceTask::new(TriggerType::RouteUpdated, conn1.info().clone());
        tx.send(task).await.unwrap();
        // verify that the routes are sent to both connections
        let conn1_ra = r#"
        {
            "header": {
                "version": 1,
                "flag": 0,
                "ttl": 1,
                "source": "region-a.cluster-a.node0",
                "destination": "region-a.cluster-a.node1"
            },
            "body": {
                "RouteAnnouncement": {
                "entries": [
                    {
                    "service_path": "region-a",
                    "nh_service_path": null,
                    "route_scope": 4,
                    "hop_count": 2
                    },
                    {
                    "service_path": "region-a.cluster-a",
                    "nh_service_path": null,
                    "route_scope": 3,
                    "hop_count": 0
                    },
                    {
                    "service_path": "region-a.cluster-a.node0",
                    "nh_service_path": null,
                    "route_scope": 2,
                    "hop_count": 0
                    },
                    {
                    "service_path": "region-a.cluster-a.node2",
                    "nh_service_path": null,
                    "route_scope": 2,
                    "hop_count": 1
                    }
                ]
                }
            }
        }
        "#;
        let _: SwbusMessage = serde_json::from_str(conn1_ra).unwrap();
        receive_and_compare(&mut conn1_sendq_rx, conn1_ra).await;

        let conn2_ra = r#"
        {
            "header": {
                "version": 1,
                "flag": 0,
                "ttl": 1,
                "source": "region-a.cluster-a.node0",
                "destination": "region-a.cluster-a.node2"
            },
            "body": {
                "RouteAnnouncement": {
                "entries": [
                    {
                    "service_path": "region-a.cluster-a",
                    "nh_service_path": null,
                    "route_scope": 3,
                    "hop_count": 0
                    },
                    {
                    "service_path": "region-a.cluster-a.node0",
                    "nh_service_path": null,
                    "route_scope": 2,
                    "hop_count": 0
                    },
                    {
                    "service_path": "region-a.cluster-a.node1",
                    "nh_service_path": null,
                    "route_scope": 2,
                    "hop_count": 1
                    },
                    {
                    "service_path": "region-a.cluster-b",
                    "nh_service_path": null,
                    "route_scope": 3,
                    "hop_count": 2
                    }
                ]
                }
            }
        }
        "#;
        receive_and_compare(&mut conn2_sendq_rx, conn2_ra).await;
    }
}
