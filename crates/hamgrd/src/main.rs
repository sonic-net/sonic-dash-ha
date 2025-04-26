use anyhow::anyhow;
use std::{collections::HashMap, sync::Arc, time::Duration};
use swbus_actor::{set_global_runtime, ActorRuntime};
use swbus_edge::{swbus_proto::swbus::ServicePath, SwbusEdgeRuntime};
use swss_common::DbConnector;
use tokio::time::timeout;

mod actors;

#[tokio::main]
async fn main() {
    // Setup swbus and actor runtime
    let mut swbus_edge = SwbusEdgeRuntime::new("<todo>".to_string(), sp("swbus-edge-runtime", "swbus-edge-runtime"));
    swbus_edge.start().await.unwrap();
    let actor_runtime = ActorRuntime::new(Arc::new(swbus_edge));
    set_global_runtime(actor_runtime);

    actors::dpu::spawn_dpu_actors(HashMap::new()).await.unwrap();
}

fn sp(resource_type: &str, resource_id: &str) -> ServicePath {
    ServicePath::from_string(&format!(
        "unknown.unknown.unknown/hamgrd/unknown/{resource_type}/{resource_id}"
    ))
    .unwrap()
}

async fn db_named(name: &str) -> anyhow::Result<DbConnector> {
    let db = timeout(Duration::from_secs(5), DbConnector::new_named_async(name, true, 11000))
        .await
        .map_err(|_| anyhow!("Connecting to db `{name}` timed out"))?
        .map_err(|e| anyhow!("Connecting to db `{name}`: {e}"))?;
    Ok(db)
}
