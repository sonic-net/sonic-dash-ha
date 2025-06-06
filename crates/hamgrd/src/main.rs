use anyhow::{anyhow, Ok};
use clap::Parser;
use sonic_common::log;
use std::{sync::Arc, time::Duration};
use swbus_actor::{set_global_runtime, ActorRuntime};
use swbus_config::swbus_config_from_db;
use swbus_edge::{swbus_proto::swbus::ServicePath, SwbusEdgeRuntime};
use swss_common::{DbConnector, KeyOpFieldValues, SubscriberStateTable};
use swss_common_bridge::consumer::spawn_consumer_bridge;
use tokio::signal;
use tokio::time::timeout;
use tracing::error;
mod actors;
use actors::{dpu::DpuActor, ActorCreator};
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
    start_actor_creators(swbus_edge).await.unwrap();

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

async fn start_actor_creators(edge_runtime: Arc<SwbusEdgeRuntime>) -> Result<()> {
    {
        let dpu_ac = ActorCreator::new(
            sp("DPU", ""),
            edge_runtime.clone(),
            false,
            |key: String, fv: &swss_common::FieldValues| -> Result<DpuActor> { DpuActor::new(key, fv) },
        );

        tokio::task::spawn(dpu_ac.run());
        let config_db = crate::db_named("CONFIG_DB").await?;
        let sst = SubscriberStateTable::new_async(config_db, "DPU", None, None).await?;
        let addr = crate::sp("swss-common-bridge", "DPU");
        spawn_consumer_bridge(edge_runtime.clone(), addr, sst, |kfv: &KeyOpFieldValues| {
            (crate::sp("DPU", &kfv.key), "DPU".to_owned())
        });
    }
    Ok(())
}
