use crate::db_structs::*;
use crate::ha_actor_messages::*;
use crate::RuntimeData;
use anyhow::Result;
use serde_json::{json, Value};
use std::{collections::HashMap, future::Future, time::Duration};
use std::{net::Ipv4Addr, net::Ipv6Addr, sync::Arc};
use swbus_actor::{ActorMessage, ActorRuntime};
use swbus_edge::{
    simple_client::{IncomingMessage, MessageBody, OutgoingMessage, SimpleSwbusEdgeClient},
    swbus_proto::swbus::{ServicePath, SwbusErrorCode},
    SwbusEdgeRuntime,
};

async fn timeout<T, Fut: Future<Output = T>>(fut: Fut) -> Result<T, tokio::time::error::Elapsed> {
    const TIMEOUT: Duration = Duration::from_millis(5000);
    tokio::time::timeout(TIMEOUT, fut).await
}

#[macro_export]
macro_rules! send {
    (key: $key:expr, data: $data:tt) => {
        send!(@build $key, $data, swbus_edge::swbus_proto::swbus::ServicePath::from_string("unknown.unknown.unknown/hamgrd/0/test/test").unwrap(), false)
    };
    (key: $key:expr, data: $data:tt, addr: $addr:expr) => {
        send!(@build $key, $data, $addr, false)
    };
    (key: $key:expr, data: $data:tt, fail) => {
        send!(@build $key, $data, swbus_edge::swbus_proto::swbus::ServicePath::from_string("unknown.unknown.unknown/hamgrd/0/test/test").unwrap(), true)
    };
    (key: $key:expr, data: $data:tt, addr: $addr:expr, fail) => {
        send!(@build $key, $data, $addr, true)
    };

    (@build $key:expr, $data:tt, $addr:expr, $fail:expr) => {
        $crate::actors::test::Command::Send {
            key: String::from($key),
            data: serde_json::json!($data),
            addr: $addr,
            fail: $fail,
        }
    };
}
pub use send;

#[macro_export]
macro_rules! recv {
    (key: $key:expr, data: $data:tt, addr: $addr:expr) => {
        $crate::actors::test::Command::Recv {
            key: String::from($key),
            data: serde_json::json!($data),
            addr: $addr.clone(),
        }
    };
}
pub use recv;

pub enum Command {
    Send {
        key: String,
        data: Value,
        addr: ServicePath,
        fail: bool,
    },
    Recv {
        key: String,
        data: Value,
        addr: ServicePath,
    },
}

pub async fn run_commands(runtime: &ActorRuntime, aut: ServicePath, commands: &[Command]) {
    use Command::*;

    let mut clients: HashMap<ServicePath, SimpleSwbusEdgeClient> = HashMap::new();

    // Pre-populate clients
    for cmd in commands {
        match cmd {
            Send { addr, .. } | Recv { addr, .. } => {
                if !clients.contains_key(addr) {
                    let client = SimpleSwbusEdgeClient::new(runtime.get_swbus_edge(), addr.clone(), true, false);
                    clients.insert(addr.clone(), client);
                }
            }
        }
    }

    // Execute commands
    for cmd in commands {
        match cmd {
            Send { key, data, addr, fail } => {
                let client = &clients[addr];
                let am = ActorMessage::new(key, data).unwrap();
                let msg = OutgoingMessage {
                    destination: aut.clone(),
                    body: MessageBody::Request {
                        payload: am.serialize(),
                    },
                };
                let sent_id = client.send(msg).await.unwrap();

                print!("Sent {key}, ");

                if *fail {
                    print!("expecting Fail, ");
                } else {
                    print!("expecting Ok, ");
                }

                let (error_code, error_msg) = match timeout(client.recv()).await {
                    Ok(Some(IncomingMessage {
                        body:
                            MessageBody::Response {
                                request_id,
                                error_code,
                                error_message,
                                response_body: None,
                            },
                        ..
                    })) if request_id == sent_id => (error_code, error_message),
                    Err(_) => {
                        println!("got Timeout");
                        panic!("Timed out waiting for response")
                    }
                    m => unreachable!("Some other message received: {m:#?}"),
                };

                if *fail && error_code == SwbusErrorCode::Fail {
                    println!("got Fail ({error_msg})");
                } else if !*fail && error_code == SwbusErrorCode::Ok {
                    println!("got Ok");
                } else {
                    println!("got {error_code:?} ({error_msg})");
                    panic!("Got unexpected response");
                }
            }

            Recv { key, data, addr } => {
                let client = &clients[addr];
                print!("Receiving {key}, ");
                let (am, request_id) = match timeout(client.recv()).await {
                    Ok(Some(IncomingMessage {
                        body: MessageBody::Request { payload },
                        id: request_id,
                        ..
                    })) => (ActorMessage::deserialize(&payload).unwrap(), request_id),
                    Err(_) => {
                        println!("got Timeout");
                        panic!("Timed out waiting for request")
                    }
                    m => unreachable!("Some other message received: {m:#?}"),
                };

                println!("got {}", am.key);
                assert_eq!(&am.key, key);
                assert_eq!(&am.data, data);

                let ack = OutgoingMessage {
                    destination: aut.clone(),
                    body: MessageBody::Response {
                        request_id,
                        error_code: SwbusErrorCode::Ok,
                        error_message: "".to_string(),
                        response_body: None,
                    },
                };
                client.send(ack).await.unwrap();
            }
        }
    }
}

pub async fn create_edge_runtime() -> SwbusEdgeRuntime {
    let mut swbus_edge: SwbusEdgeRuntime = SwbusEdgeRuntime::new(
        "none".to_string(),
        ServicePath::from_string("unknown.unknown.unknown/hamgrd/0").unwrap(),
    );
    swbus_edge.start().await.unwrap();
    swbus_edge
}

pub async fn create_actor_runtime(slot: u32, npu_ipv4: &str, npu_ipv6: &str) -> ActorRuntime {
    let mut edge = create_edge_runtime().await;

    let runtime_data = RuntimeData::new(
        slot,
        Some(npu_ipv4.parse::<Ipv4Addr>().expect("Bad ipv4 address is provided")),
        Some(npu_ipv6.parse::<Ipv6Addr>().expect("Bad ipv6 address is provided")),
    );

    edge.set_runtime_env(Box::new(runtime_data));
    ActorRuntime::new(Arc::new(edge))
}

fn normalize_ipv6(ipv6: &str) -> String {
    let ipv6_addr = ipv6.parse::<Ipv6Addr>().expect("Bad ipv4 address is provided");
    ipv6_addr.to_string()
}

pub fn make_dash_ha_global_config() -> DashHaGlobalConfig {
    DashHaGlobalConfig {
        dpu_bfd_probe_interval_in_ms: Some(1000),
        dpu_bfd_probe_multiplier: Some(3),
        cp_data_channel_port: Some(12345),
        dp_channel_dst_port: Some(23456),
        dp_channel_src_port_min: Some(34567),
        dp_channel_src_port_max: Some(45678),
        dp_channel_probe_fail_threshold: Some(3),
        dp_channel_probe_interval_ms: Some(1000),
    }
}

pub fn make_dpu_object(switch: u16, dpu: u32) -> Dpu {
    Dpu {
        state: Some("up".to_string()),
        vip_ipv4: Some(format!("3.2.1.{switch}")),
        vip_ipv6: Some(normalize_ipv6(&format!("3:2:1::{switch}"))),
        pa_ipv4: format!("18.0.{switch}.{dpu}"),
        pa_ipv6: Some(normalize_ipv6(&format!("18:0:{switch}::{dpu}"))),
        dpu_id: dpu,
        vdpu_id: Some(format!("vdpu{}", switch * 8 + dpu as u16)),
        orchagent_zmq_port: 8100,
        swbus_port: 23606 + dpu as u16,
        midplane_ipv4: format!("169.254.1.{dpu}"),
    }
}

pub fn make_remote_dpu_object(switch: u16, dpu: u32) -> RemoteDpu {
    RemoteDpu {
        pa_ipv4: format!("4.0.{switch}.{dpu}"),
        pa_ipv6: Some(normalize_ipv6(&format!("4:0:{switch}::{dpu}"))),
        dpu_id: dpu,
        swbus_port: 23606 + dpu as u16,
        npu_ipv4: format!("10.0.{switch}.{dpu}"),
        npu_ipv6: Some(normalize_ipv6(&format!("10:0:{switch}::{dpu}"))),
    }
}
pub fn make_local_dpu_actor_state(switch: u16, dpu: u32, is_managed: bool) -> DpuActorState {
    DpuActorState {
        remote_dpu: false,
        // If true, this is a DPU locally manamaged by this hamgrd.
        is_managed,
        dpu_name: format!("switch{switch}_dpu{dpu}"),
        up: false,
        state: Some("up".to_string()),
        vip_ipv4: Some(format!("3.2.1.{switch}")),
        vip_ipv6: Some(normalize_ipv6(&format!("3:2:1::{switch}"))),
        pa_ipv4: format!("4.0.{switch}.{dpu}"),
        pa_ipv6: Some(normalize_ipv6(&format!("4:0:{switch}::{dpu}"))),
        dpu_id: dpu,
        vdpu_id: Some(format!("vdpu{}", switch * 8 + dpu as u16)),
        orchagent_zmq_port: 8100,
        swbus_port: 23606 + dpu as u16,
        midplane_ipv4: Some(format!("169.254.1.{dpu}")),
        npu_ipv4: format!("10.0.{switch}.{dpu}"),
        npu_ipv6: Some(normalize_ipv6(&format!("10:0:{switch}::{dpu}"))),
    }
}

pub fn make_remote_dpu_actor_state(switch: u16, dpu: u32) -> DpuActorState {
    DpuActorState {
        remote_dpu: true,
        // If true, this is a DPU locally manamaged by this hamgrd.
        is_managed: false,
        dpu_name: format!("switch{switch}_dpu{dpu}"),
        up: false,
        state: Some("up".to_string()),
        vip_ipv4: None,
        vip_ipv6: None,
        pa_ipv4: format!("4.0.{switch}.{dpu}"),
        pa_ipv6: Some(normalize_ipv6(&format!("4:0:{switch}::{dpu}"))),
        dpu_id: dpu,
        vdpu_id: None,
        orchagent_zmq_port: 0,
        swbus_port: 23606 + dpu as u16,
        midplane_ipv4: None,
        npu_ipv4: format!("10.0.{switch}.{dpu}"),
        npu_ipv6: Some(normalize_ipv6(&format!("10:0:{switch}::{dpu}"))),
    }
}

pub fn to_local_dpu(dpu_actor_state: &DpuActorState) -> Dpu {
    if dpu_actor_state.remote_dpu {
        panic!("Cannot convert remote DPU to local DPU");
    }
    Dpu {
        state: dpu_actor_state.state.clone(),
        vip_ipv4: dpu_actor_state.vip_ipv4.clone(),
        vip_ipv6: dpu_actor_state.vip_ipv6.clone(),
        pa_ipv4: dpu_actor_state.pa_ipv4.clone(),
        pa_ipv6: dpu_actor_state.pa_ipv6.clone(),
        dpu_id: dpu_actor_state.dpu_id,
        vdpu_id: dpu_actor_state.vdpu_id.clone(),
        orchagent_zmq_port: dpu_actor_state.orchagent_zmq_port,
        swbus_port: dpu_actor_state.swbus_port,
        midplane_ipv4: dpu_actor_state.midplane_ipv4.as_ref().unwrap().clone(),
    }
}

pub fn to_remote_dpu(dpu_actor_state: &DpuActorState) -> RemoteDpu {
    if dpu_actor_state.remote_dpu {
        panic!("Cannot convert remote DPU to local DPU");
    }
    RemoteDpu {
        pa_ipv4: dpu_actor_state.pa_ipv4.clone(),
        pa_ipv6: dpu_actor_state.pa_ipv6.clone(),
        dpu_id: dpu_actor_state.dpu_id,
        swbus_port: dpu_actor_state.swbus_port,
        npu_ipv4: dpu_actor_state.npu_ipv4.clone(),
        npu_ipv6: dpu_actor_state.npu_ipv6.clone(),
    }
}
