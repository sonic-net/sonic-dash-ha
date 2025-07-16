use crate::db_structs::*;
use crate::ha_actor_messages::*;
use crate::RuntimeData;
use anyhow::Result;
use serde_json::Value;
use sonic_dash_api_proto::{ha_set_config::HaSetConfig, types::ip_address::Ip, types::*};
use std::{collections::HashMap, future::Future, time::Duration};
use std::{net::Ipv4Addr, net::Ipv6Addr, sync::Arc};
use swbus_actor::{ActorMessage, ActorRuntime};
use swbus_edge::{
    simple_client::{IncomingMessage, MessageBody, OutgoingMessage, SimpleSwbusEdgeClient},
    swbus_proto::swbus::{ServicePath, SwbusErrorCode},
    SwbusEdgeRuntime,
};
use swss_common::{FieldValues, Table};

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

#[macro_export]
macro_rules! chkdb {
    (type: $type:ty, key: $key:expr, data: $data:tt) => {
        $crate::actors::test::Command::ChkDb {
            db: String::from(<$type>::db_name()),
            is_dpu: <$type>::is_dpu(),
            table: String::from(<$type>::table_name()),
            key: String::from($key),
            data: serde_json::json!($data),
            exclude: "".to_string(),
        }
    };

    (type: $type:ty, key: $key:expr, data: $data:tt, exclude: $exclude:expr) => {
        $crate::actors::test::Command::ChkDb {
            db: String::from(<$type>::db_name()),
            is_dpu: <$type>::is_dpu(),
            table: String::from(<$type>::table_name()),
            key: String::from($key),
            data: serde_json::json!($data),
            exclude: String::from($exclude),
        }
    };

    (db: $db:expr, is_dpu: $is_dpu:expr, table: $table:expr, key: $key:expr, data: $data:tt, exclude: $exclude:expr) => {
        $crate::actors::test::Command::ChkDb {
            db: String::from($db),
            is_dpu: $is_dpu,
            table: String::from($table),
            key: String::from($key),
            data: serde_json::json!($data),
            exclude: String::from($exclude),
        }
    };
}
pub use chkdb;

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
    ChkDb {
        db: String,
        is_dpu: bool,
        table: String,
        key: String,
        data: Value,
        exclude: String,
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
            ChkDb { .. } => {}
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

            ChkDb {
                db: db_name,
                is_dpu,
                table: table_name,
                key,
                data,
                exclude,
            } => {
                let db = crate::db_named(db_name, *is_dpu).await.unwrap();
                let mut table = Table::new(db, table_name).unwrap();

                let mut actual_data = table.get_async(key).await.unwrap();
                let Some(actual_data) = actual_data.as_mut() else {
                    panic!("Key {key} not found in {table_name}/{db_name}");
                };
                let mut fvs: FieldValues = serde_json::from_value(data.clone()).unwrap();

                exclude
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .for_each(|id| {
                        fvs.remove(id);
                        actual_data.remove(id);
                    });
                assert_eq!(actual_data, &fvs);
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
    let switch_pair_id = switch / 2;
    Dpu {
        state: Some("up".to_string()),
        vip_ipv4: Some(format!("3.2.{switch_pair_id}.{dpu}")),
        vip_ipv6: Some(normalize_ipv6(&format!("3:2:{switch_pair_id}::{dpu}"))),
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
        pa_ipv4: format!("18.0.{switch}.{dpu}"),
        pa_ipv6: Some(normalize_ipv6(&format!("18:0:{switch}::{dpu}"))),
        dpu_id: dpu,
        swbus_port: 23606 + dpu as u16,
        npu_ipv4: format!("10.0.{switch}.{dpu}"),
        npu_ipv6: Some(normalize_ipv6(&format!("10:0:{switch}::{dpu}"))),
    }
}

pub fn make_dpu_pmon_state(all_up: bool) -> DpuState {
    let state = if all_up {
        DpuPmonStateType::Up
    } else {
        DpuPmonStateType::Down
    };

    DpuState {
        dpu_midplane_link_state: state.clone(),
        dpu_midplane_link_time: 1718053542000, // Mon Jun 10 03:15:42 PM UTC 2024
        dpu_control_plane_state: state.clone(),
        dpu_control_plane_time: 1718096215000, // Tue Jun 11 09:30:15 AM UTC 2024
        dpu_data_plane_state: state.clone(),
        dpu_data_plane_time: 1718231130000, // Wed Jun 12 11:45:30 PM UTC 2024
    }
}

pub fn make_dpu_bfd_state(v4_up_sessions: Vec<&str>, v6_up_sessions: Vec<&str>) -> DashBfdProbeState {
    DashBfdProbeState {
        v4_bfd_up_sessions: v4_up_sessions.iter().map(|&s| s.to_string()).collect::<Vec<String>>(),
        v4_bfd_up_sessions_timestamp: 1718053542000, // Mon Jun 10 03:15:42 PM UTC 2024
        v6_bfd_up_sessions: v6_up_sessions.iter().map(|&s| s.to_string()).collect::<Vec<String>>(),
        v6_bfd_up_sessions_timestamp: 1718053542000, // Mon Jun 10 03:15:42 PM UTC 2024
    }
}
pub fn make_local_dpu_actor_state(
    switch: u16,
    dpu: u32,
    is_managed: bool,
    dpu_pmon_state: Option<DpuState>,
    dpu_bfd_state: Option<DashBfdProbeState>,
) -> DpuActorState {
    let dpu_obj = make_dpu_object(switch, dpu);

    DpuActorState::from_dpu(
        &format!("switch{switch}_dpu{dpu}"),
        &dpu_obj,
        is_managed,
        &format!("10.0.{switch}.{dpu}"),
        &Some(normalize_ipv6(&format!("10:0:{switch}::{dpu}"))),
        dpu_pmon_state,
        dpu_bfd_state,
    )
}

pub fn make_remote_dpu_actor_state(switch: u16, dpu: u32) -> DpuActorState {
    let remote_dpu = make_remote_dpu_object(switch, dpu);
    DpuActorState::from_remote_dpu(&format!("switch{switch}_dpu{dpu}"), &remote_dpu)
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

pub fn make_vdpu_actor_state(up: bool, dpu_state: &DpuActorState) -> (String, VDpuActorState) {
    let parts: Vec<&str> = dpu_state
        .dpu_name
        .split(['s', 'w', 'i', 't', 'c', 'h', '_', 'd', 'p', 'u'])
        .filter(|p| !p.is_empty())
        .collect();
    let vdpu_id = match parts.len() {
        2 => format!("vdpu{}-{}", parts[0], parts[1]),
        _ => panic!("Invalid DPU name: {}", &dpu_state.dpu_name),
    };

    (
        vdpu_id,
        VDpuActorState {
            up,
            dpu: dpu_state.clone(),
        },
    )
}

pub fn string_to_ip(s: String) -> Option<IpAddress> {
    if s.is_empty() {
        return None;
    }
    if let Ok(v4) = s.parse::<std::net::Ipv4Addr>() {
        Some(IpAddress {
            ip: Some(Ip::Ipv4(u32::from(v4))),
        })
    } else if let Ok(v6) = s.parse::<std::net::Ipv6Addr>() {
        Some(IpAddress {
            ip: Some(Ip::Ipv6(v6.octets().to_vec())),
        })
    } else {
        None
    }
}

/// Create a DashHaSetConfigTable for a given DPU
/// The allocation scheme is as follows:
/// switch_n/dpu_x and switch_n+1/dpu_x are in the same HA set, where n is even
/// vdpu id is vdpu{switch id * 8}-{dpu}
pub fn make_dpu_scope_ha_set_config(switch: u16, dpu: u16) -> (String, HaSetConfig) {
    let switch_pair_id = switch / 2;
    let vdpu0_id = format!("vdpu{}-{dpu}", switch_pair_id * 2);
    let vdpu1_id = format!("vdpu{}-{dpu}", switch_pair_id * 2 + 1);
    let ha_set = HaSetConfig {
        version: "1".to_string(),
        vip_v4: string_to_ip(format!("3.2.{switch_pair_id}.{dpu}")),
        vip_v6: string_to_ip(normalize_ipv6(&format!("3:2:{switch_pair_id}::{dpu}"))),
        // dpu or switch
        owner: HaOwner::OwnerController as i32,
        // dpu or eni
        scope: HaScope::ScopeDpu as i32,
        vdpu_ids: vec![vdpu0_id.clone(), vdpu1_id.clone()],
        pinned_vdpu_bfd_probe_states: vec!["".to_string()],
        preferred_vdpu_id: vdpu0_id,
        preferred_standalone_vdpu_index: 0,
    };
    (format!("haset{switch_pair_id}-{dpu}"), ha_set)
}

pub fn make_dpu_scope_ha_set_obj(switch: u16, dpu: u16) -> (String, DashHaSetTable) {
    let switch_pair_id = switch / 2;
    let (_, haset_cfg) = make_dpu_scope_ha_set_config(switch, dpu);
    let global_cfg = make_dash_ha_global_config();
    let ha_set = DashHaSetTable {
        version: "1".to_string(),
        vip_v4: ip_to_string(&haset_cfg.vip_v4.unwrap()),
        vip_v6: Some(ip_to_string(&haset_cfg.vip_v6.unwrap())),
        owner: format!("{:?}", haset_cfg.owner).into(),
        scope: format!("{:?}", haset_cfg.scope).into(),
        local_npu_ip: format!("10.0.{switch}.{dpu}"),
        local_ip: format!("18.0.{switch}.{dpu}"),
        peer_ip: format!("18.0.{}.{dpu}", switch_pair_id * 2 + 1),
        cp_data_channel_port: global_cfg.cp_data_channel_port,
        dp_channel_dst_port: global_cfg.dp_channel_dst_port,
        dp_channel_src_port_min: global_cfg.dp_channel_src_port_min,
        dp_channel_src_port_max: global_cfg.dp_channel_src_port_max,
        dp_channel_probe_interval_ms: global_cfg.dp_channel_probe_interval_ms,
        dp_channel_probe_fail_threshold: global_cfg.dp_channel_probe_fail_threshold,
    };
    (format!("haset{switch_pair_id}-{dpu}"), ha_set)
}

pub fn make_npu_ha_scope_state(vdpu_state_obj: &VDpuActorState, ha_set_obj: &DashHaSetTable) -> NpuDashHaScopeState {
    let mut scope_state = NpuDashHaScopeState::default();

    let pmon_state = match vdpu_state_obj.dpu.dpu_pmon_state {
        Some(ref state) => state.clone(),
        None => DpuState::default(),
    };
    let bfd_state = match vdpu_state_obj.dpu.dpu_bfd_state {
        Some(ref state) => state.clone(),
        None => DashBfdProbeState::default(),
    };

    scope_state.vip_v4 = ha_set_obj.vip_v4.clone();
    scope_state.vip_v6 = ha_set_obj.vip_v6.clone();
    scope_state.local_ip = ha_set_obj.local_ip.clone();
    scope_state.peer_ip = ha_set_obj.peer_ip.clone();
    scope_state.local_vdpu_midplane_state = pmon_state.dpu_midplane_link_state.clone();
    scope_state.local_vdpu_midplane_state_last_updated_time_in_ms = pmon_state.dpu_midplane_link_time;
    scope_state.local_vdpu_control_plane_state = pmon_state.dpu_control_plane_state.clone();
    scope_state.local_vdpu_control_plane_state_last_updated_time_in_ms = pmon_state.dpu_control_plane_time;
    scope_state.local_vdpu_data_plane_state = pmon_state.dpu_data_plane_state.clone();
    scope_state.local_vdpu_data_plane_state_last_updated_time_in_ms = pmon_state.dpu_data_plane_time;
    scope_state.local_vdpu_up_bfd_sessions_v4 = bfd_state.v4_bfd_up_sessions.clone();
    scope_state.local_vdpu_up_bfd_sessions_v4_update_time_in_ms = bfd_state.v4_bfd_up_sessions_timestamp;
    scope_state.local_vdpu_up_bfd_sessions_v6 = bfd_state.v6_bfd_up_sessions.clone();
    scope_state.local_vdpu_up_bfd_sessions_v6_update_time_in_ms = bfd_state.v6_bfd_up_sessions_timestamp;

    scope_state
}

pub fn update_npu_ha_scope_state_by_vdpu(
    npu_ha_scope_state: &mut NpuDashHaScopeState,
    vdpu_state_obj: &VDpuActorState,
) {
    let pmon_state = match vdpu_state_obj.dpu.dpu_pmon_state {
        Some(ref state) => state.clone(),
        None => DpuState::default(),
    };
    let bfd_state = match vdpu_state_obj.dpu.dpu_bfd_state {
        Some(ref state) => state.clone(),
        None => DashBfdProbeState::default(),
    };

    npu_ha_scope_state.local_vdpu_midplane_state = pmon_state.dpu_midplane_link_state.clone();
    npu_ha_scope_state.local_vdpu_midplane_state_last_updated_time_in_ms = pmon_state.dpu_midplane_link_time;
    npu_ha_scope_state.local_vdpu_control_plane_state = pmon_state.dpu_control_plane_state.clone();
    npu_ha_scope_state.local_vdpu_control_plane_state_last_updated_time_in_ms = pmon_state.dpu_control_plane_time;
    npu_ha_scope_state.local_vdpu_data_plane_state = pmon_state.dpu_data_plane_state.clone();
    npu_ha_scope_state.local_vdpu_data_plane_state_last_updated_time_in_ms = pmon_state.dpu_data_plane_time;
    npu_ha_scope_state.local_vdpu_up_bfd_sessions_v4 = bfd_state.v4_bfd_up_sessions.clone();
    npu_ha_scope_state.local_vdpu_up_bfd_sessions_v4_update_time_in_ms = bfd_state.v4_bfd_up_sessions_timestamp;
    npu_ha_scope_state.local_vdpu_up_bfd_sessions_v6 = bfd_state.v6_bfd_up_sessions.clone();
    npu_ha_scope_state.local_vdpu_up_bfd_sessions_v6_update_time_in_ms = bfd_state.v6_bfd_up_sessions_timestamp;
}

pub fn update_npu_ha_scope_state_by_dpu_scope_state(
    npu_ha_scope_state: &mut NpuDashHaScopeState,
    dpu_ha_scope_state: &DpuDashHaScopeState,
    target_ha_state: &str,
) {
    npu_ha_scope_state.local_ha_state = Some(dpu_ha_scope_state.ha_role.clone());
    npu_ha_scope_state.local_ha_state_last_updated_time_in_ms = Some(dpu_ha_scope_state.ha_role_start_time);
    npu_ha_scope_state.local_ha_state_last_updated_reason = Some("dpu initiated".to_string());
    npu_ha_scope_state.local_target_asic_ha_state = Some(target_ha_state.to_string());
    npu_ha_scope_state.local_acked_asic_ha_state = Some(dpu_ha_scope_state.ha_role.clone());
    npu_ha_scope_state.local_target_term = Some(dpu_ha_scope_state.ha_term.clone());
    npu_ha_scope_state.local_acked_term = Some(dpu_ha_scope_state.ha_term.clone());
}

pub fn update_npu_ha_scope_state_pending_ops(
    npu_ha_scope_state: &mut NpuDashHaScopeState,
    pending_ops: Vec<(String, String)>,
) {
    let mut pending_operation_ids = vec![];
    let mut pending_operation_types = vec![];
    for (op_id, op_type) in pending_ops {
        pending_operation_ids.push(op_id);
        pending_operation_types.push(op_type);
    }
    npu_ha_scope_state.pending_operation_ids = Some(pending_operation_ids);
    npu_ha_scope_state.pending_operation_types = Some(pending_operation_types);
    npu_ha_scope_state.pending_operation_list_last_updated_time_in_ms = Some(now_in_millis());
}

pub fn make_dpu_ha_scope_state(role: &str) -> DpuDashHaScopeState {
    DpuDashHaScopeState {
        last_updated_time: now_in_millis(),
        // The current HA role confirmed by ASIC. Please refer to the HA states defined in HA HLD.
        ha_role: role.to_string(),
        // The time when HA role is moved into current one in milliseconds.
        ha_role_start_time: now_in_millis(),
        // The current term confirmed by ASIC.
        ha_term: "1".to_string(),
        activate_role_pending: false,
        flow_reconcile_pending: false,
        brainsplit_recover_pending: false,
    }
}
