use anyhow::{anyhow, Ok};
use clap::Parser;
use sonic_common::log;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::{sync::Arc, time::Duration};
use swbus_actor::{set_global_runtime, ActorRuntime};
use swbus_config::swbus_config_from_db;
use swbus_edge::{simple_client::SimpleSwbusEdgeClient, swbus_proto::swbus::ServicePath, RuntimeEnv, SwbusEdgeRuntime};
use swss_common::DbConnector;
use tokio::{signal, task::JoinHandle, time::timeout};
use tracing::error;
mod actors;
mod db_structs;
mod ha_actor_messages;
use actors::dpu::DpuActor;
use actors::spawn_zmq_producer_bridge;
use anyhow::Result;
use db_structs::Dpu;
use std::any::Any;

use crate::db_structs::BfdSessionTable;

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

    let dpu = db_structs::get_dpu_config_from_db(args.slot_id).unwrap();

    let runtime_data = RuntimeData::new(args.slot_id, swbus_config.npu_ipv4, swbus_config.npu_ipv6);

    // Setup swbus and actor runtime
    let mut swbus_edge = SwbusEdgeRuntime::new(format!("http://{}", swbus_config.endpoint), swbus_sp.clone());
    swbus_edge.set_runtime_env(Box::new(runtime_data));

    swbus_edge.start().await.unwrap();
    let swbus_edge = Arc::new(swbus_edge);
    let actor_runtime = ActorRuntime::new(swbus_edge.clone());
    set_global_runtime(actor_runtime);

    // Start zmq common bridge provider for DPU tables
    let _producer_handles = spawn_producer_bridges(swbus_edge.clone(), &dpu).await.unwrap();

    // run a sink to drain all messages that are not handled by any actor
    let sink = SimpleSwbusEdgeClient::new(swbus_edge.clone(), swbus_sp, true /*public*/, true /*sink*/);
    tokio::task::spawn(async move {
        loop {
            // Drain the sink
            sink.recv().await;
        }
    });

    start_actor_creators(&swbus_edge).await.unwrap();

    // Wait for Ctrl+C to exit
    signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
}

async fn db_named(name: &str) -> anyhow::Result<DbConnector> {
    let db = timeout(Duration::from_secs(5), DbConnector::new_named_async(name, true, 11000))
        .await
        .map_err(|_| anyhow!("Connecting to db `{name}` timed out"))?
        .map_err(|e| anyhow!("Connecting to db `{name}`: {e}"))?;
    Ok(db)
}

// producer bridges are responsible for updating sonic-db optionally sending the update out via zmq
// This function spawns all producer bridges for the hamgrd process. They are static and shared by
// all actors in the process.
async fn spawn_producer_bridges(edge_runtime: Arc<SwbusEdgeRuntime>, dpu: &Dpu) -> Result<Vec<JoinHandle<()>>> {
    let mut handles = Vec::new();
    let zmq_endpoint = format!("{}:{}", dpu.midplane_ipv4, dpu.orchagent_zmq_port);

    // Spawn BFD_SESSION_TABLE zmq producer bridge for DPU actor
    // has service path swss-common-bridge/BFD_SESSION_TABLE.
    let handle = spawn_zmq_producer_bridge::<BfdSessionTable>(edge_runtime.clone(), &zmq_endpoint).await?;
    handles.push(handle);

    Ok(handles)
}

// actor-creator creates are private swbus message handler to handle messages to actor but actor do not exist.
// The creator will create the actor when it receives the first message to the actor.
async fn start_actor_creators(edge_runtime: &Arc<SwbusEdgeRuntime>) -> Result<()> {
    DpuActor::start_actor_creator(edge_runtime.clone()).await?;
    //VDpuActor::start_actor_creator(edge_runtime.clone()).await?;
    //HaSetActor::start_actor_creator(edge_runtime.clone()).await?;
    Ok(())
}

pub fn get_slot_id(swbus_edge: &Arc<SwbusEdgeRuntime>) -> u32 {
    let runtime_env = swbus_edge.get_runtime_env();
    //let raw_ptr = guard.as_any() as *const dyn Any;
    let inner = runtime_env.as_ref().unwrap().as_ref();
    let runtime_env = inner.as_any().downcast_ref::<RuntimeData>().unwrap();
    runtime_env.dpu_id
}

pub fn get_npu_ipv4(swbus_edge: &Arc<SwbusEdgeRuntime>) -> Option<Ipv4Addr> {
    let runtime_env = swbus_edge.get_runtime_env();
    //let raw_ptr = guard.as_any() as *const dyn Any;
    let inner = runtime_env.as_ref().unwrap().as_ref();
    let runtime_env = inner.as_any().downcast_ref::<RuntimeData>().unwrap();
    runtime_env.npu_ipv4
}

pub fn get_npu_ipv6(swbus_edge: &Arc<SwbusEdgeRuntime>) -> Option<Ipv6Addr> {
    let runtime_env = swbus_edge.get_runtime_env();
    //let raw_ptr = guard.as_any() as *const dyn Any;
    let inner = runtime_env.as_ref().unwrap().as_ref();
    let runtime_env = inner.as_any().downcast_ref::<RuntimeData>().unwrap();
    runtime_env.npu_ipv6
}
pub struct RuntimeData {
    dpu_id: u32,
    npu_ipv4: Option<Ipv4Addr>,
    npu_ipv6: Option<Ipv6Addr>,
}

impl RuntimeEnv for RuntimeData {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl RuntimeData {
    pub fn new(dpu_id: u32, npu_ipv4: Option<Ipv4Addr>, npu_ipv6: Option<Ipv6Addr>) -> Self {
        Self {
            dpu_id,
            npu_ipv4,
            npu_ipv6,
        }
    }

    pub fn dpu_id(&self) -> u32 {
        self.dpu_id
    }

    pub fn npu_ipv4(&self) -> Option<Ipv4Addr> {
        self.npu_ipv4
    }

    pub fn npu_ipv6(&self) -> Option<Ipv6Addr> {
        self.npu_ipv6
    }
}

pub fn common_bridge_sp<T>(runtime: &SwbusEdgeRuntime) -> ServicePath
where
    T: swss_common::SonicDbTable + 'static,
{
    let mut new_sp = runtime.get_base_sp();
    new_sp.resource_type = "swss-common-bridge".into();
    new_sp.resource_id = format!("{}/{}", T::db_name(), T::table_name());
    new_sp
}
