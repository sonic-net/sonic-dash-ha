use anyhow::anyhow;
use clap::Parser;
use sonic_common::log;
use std::{sync::Arc, time::Duration};
use swbus_actor::{set_global_runtime, ActorRuntime};
use swbus_config::{swbus_config_from_db, swbus_config_from_yaml};
use swbus_edge::{swbus_proto::swbus::ServicePath, SwbusEdgeRuntime};
use swss_common::DbConnector;
use tokio::signal;
use tokio::time::timeout;
use tracing::error;
mod actors;

#[derive(Parser, Debug)]
#[command(name = "hamgrd")]
struct Args {
    // The slot id of the DPU. If this is set, it will read configuration from DPU table in config_db.
    // Otherwise, it will read configuration from the yaml file and bind to the specified address.
    #[arg(short = 's', long)]
    slot_id: Option<u32>,
    /// swbusd config in yaml file, including routes and peer information.
    #[arg(short = 'c', long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Err(e) = log::init("hamgrd", true) {
        eprintln!("Failed to initialize logging: {}", e);
    }

    // Read swbusd config from redis or yaml file
    let swbus_config = match args.slot_id {
        Some(slot_id) => swbus_config_from_db(slot_id).unwrap(),
        None => {
            let config_path = args.config.expect("route_config is required when slot_id is not set");
            swbus_config_from_yaml(&config_path).unwrap()
        }
    };

    let mut swbus_sp = swbus_config.get_swbusd_service_path().unwrap_or_else(|| {
        error!("No cluster route found in swbusd config");
        std::process::exit(1);
    });

    swbus_sp.service_type = "hamgrd".into();
    swbus_sp.service_id = "0".into();

    // Setup swbus and actor runtime
    let mut swbus_edge = SwbusEdgeRuntime::new(format!("http://{}", swbus_config.endpoint), swbus_sp);
    swbus_edge.start().await.unwrap();
    let actor_runtime = ActorRuntime::new(Arc::new(swbus_edge));
    set_global_runtime(actor_runtime);

    actors::dpu::spawn_dpu_actors().await.unwrap();

    // Wait for Ctrl+C to exit
    signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
}

fn sp(resource_type: &str, resource_id: &str) -> ServicePath {
    let rt = swbus_actor::get_global_runtime().as_ref().unwrap().get_swbus_edge();
    rt.new_sp(resource_type, resource_id)
}

async fn db_named(name: &str) -> anyhow::Result<DbConnector> {
    let db = timeout(Duration::from_secs(5), DbConnector::new_named_async(name, true, 11000))
        .await
        .map_err(|_| anyhow!("Connecting to db `{name}` timed out"))?
        .map_err(|e| anyhow!("Connecting to db `{name}`: {e}"))?;
    Ok(db)
}
