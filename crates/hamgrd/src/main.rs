use anyhow::{anyhow, Ok};
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
mod ha_actor_messages;
use actors::dpu::DpuActor;
use anyhow::Result;

#[derive(Parser, Debug)]
#[command(name = "hamgrd")]
struct Args {
    // The slot id of the DPU. It will read configuration from DPU table in config_db that matches the slot_id.
    #[arg(short = 's', long)]
    slot_id: u32,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Err(e) = log::init("hamgrd", true) {
        eprintln!("Failed to initialize logging: {}", e);
    }

    // Read swbusd config from redis or yaml file
    let swbus_config = swbus_config_from_db(args.slot_id).unwrap();

    let mut swbus_sp = swbus_config.get_swbusd_service_path().unwrap_or_else(|| {
        error!("No cluster route found in swbusd config");
        std::process::exit(1);
    });

    swbus_sp.service_type = "hamgrd".into();
    swbus_sp.service_id = "0".into();

    // Setup swbus and actor runtime
    let mut swbus_edge = SwbusEdgeRuntime::new(format!("http://{}", swbus_config.endpoint), swbus_sp);
    swbus_edge.start().await.unwrap();
    let swbus_edge = Arc::new(swbus_edge);
    let actor_runtime = ActorRuntime::new(swbus_edge.clone());
    set_global_runtime(actor_runtime);

    //actors::dpu::spawn_dpu_actors().await.unwrap();
    start_actor_creators(&swbus_edge).await.unwrap();

    init_actor_supporting_services(&swbus_edge).await.unwrap();
    // Wait for Ctrl+C to exit
    signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
}

fn sp(resource_type: &str, resource_id: &str) -> ServicePath {
    swbus_actor::get_global_runtime()
        .as_ref()
        .unwrap()
        .sp(resource_type, resource_id)
}

async fn db_named(name: &str) -> anyhow::Result<DbConnector> {
    let db = timeout(Duration::from_secs(5), DbConnector::new_named_async(name, true, 11000))
        .await
        .map_err(|_| anyhow!("Connecting to db `{name}` timed out"))?
        .map_err(|e| anyhow!("Connecting to db `{name}`: {e}"))?;
    Ok(db)
}

// actor-creator creates are private swbus message handler to handle messages to actor but actor do not exist.
// The creator will create the actor when it receives the first message to the actor.
async fn start_actor_creators(edge_runtime: &Arc<SwbusEdgeRuntime>) -> Result<()> {
    DpuActor::start_actor_creator(edge_runtime.clone()).await?;
    //VDpuActor::start_actor_creator(edge_runtime.clone()).await?;
    //HaSetActor::start_actor_creator(edge_runtime.clone()).await?;
    Ok(())
}

async fn init_actor_supporting_services(edge_runtime: &Arc<SwbusEdgeRuntime>) -> Result<()> {
    DpuActor::init_supporting_services(edge_runtime).await?;
    //HaSetActor::init_supporting_services(edge_runtime).await?;
    Ok(())
}
