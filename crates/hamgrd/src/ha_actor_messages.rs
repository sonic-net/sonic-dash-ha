// temporarily disable unused warning until vdpu/ha-set actors are implemented
#![allow(unused)]
use crate::db_structs::{Dpu, RemoteDpu, DashHaSetTable};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::{collections::HashMap, hash::Hash};
use swbus_actor::{state::incoming::Incoming, ActorMessage};
use swbus_edge::swbus_proto::swbus::ServicePath;

#[skip_serializing_none]
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DpuActorState {
    // If true, this is a remote DPU in a different chassis.
    pub remote_dpu: bool,
    // If true, this is a DPU locally manamaged by this hamgrd.
    pub is_managed: bool,
    pub dpu_name: String,
    pub up: bool,
    pub state: Option<String>,
    pub npu_ipv4: String,
    pub npu_ipv6: Option<String>,
    pub vip_ipv4: Option<String>,
    pub vip_ipv6: Option<String>,
    pub pa_ipv4: String,
    pub pa_ipv6: Option<String>,
    pub dpu_id: u32,
    pub vdpu_id: Option<String>,
    pub orchagent_zmq_port: u16,
    pub swbus_port: u16,
    pub midplane_ipv4: Option<String>,
}

impl DpuActorState {
    pub fn from_dpu(dpu_name: &str, dpu: &Dpu, is_managed: bool, npu_ipv4: &str, npu_ipv6: &Option<String>) -> Self {
        Self {
            remote_dpu: false,
            dpu_name: dpu_name.to_string(),
            is_managed,
            up: false,
            state: dpu.state.clone(),
            npu_ipv4: npu_ipv4.to_string(),
            npu_ipv6: npu_ipv6.clone(),
            vip_ipv4: dpu.vip_ipv4.clone(),
            vip_ipv6: dpu.vip_ipv6.clone(),
            pa_ipv4: dpu.pa_ipv4.clone(),
            pa_ipv6: dpu.pa_ipv6.clone(),
            dpu_id: dpu.dpu_id,
            vdpu_id: dpu.vdpu_id.clone(),
            orchagent_zmq_port: dpu.orchagent_zmq_port,
            swbus_port: dpu.swbus_port,
            midplane_ipv4: Some(dpu.midplane_ipv4.clone()),
        }
    }

    pub fn from_remote_dpu(dpu_name: &str, rdpu: &RemoteDpu) -> Self {
        Self {
            remote_dpu: true,
            dpu_name: dpu_name.to_string(),
            is_managed: false,
            up: false,
            state: None,
            npu_ipv4: rdpu.npu_ipv4.clone(),
            npu_ipv6: rdpu.npu_ipv6.clone(),
            vip_ipv4: None,
            vip_ipv6: None,
            pa_ipv4: rdpu.pa_ipv4.clone(),
            pa_ipv6: rdpu.pa_ipv6.clone(),
            dpu_id: rdpu.dpu_id,
            vdpu_id: None,
            orchagent_zmq_port: 0,
            swbus_port: rdpu.swbus_port,
            midplane_ipv4: None,
        }
    }

    pub fn new_actor_msg(my_id: &str, dpu: &DpuActorState) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), &dpu)
    }

    pub fn msg_key_prefix() -> &'static str {
        "DPUStateUpdate|"
    }

    pub fn msg_key(my_id: &str) -> String {
        format!("{}{}", Self::msg_key_prefix(), my_id)
    }

    pub fn is_my_msg(key: &str) -> bool {
        key.starts_with(Self::msg_key_prefix())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct VDpuActorState {
    pub up: bool,
    pub dpu: DpuActorState,
}

impl VDpuActorState {
    pub fn new_actor_msg(up: bool, my_id: &str, dpu: DpuActorState) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), &Self { up, dpu })
    }

    pub fn to_actor_msg(&self, my_id: &str) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), self)
    }

    pub fn msg_key_prefix() -> &'static str {
        "VDPUStateUpdate|"
    }

    pub fn msg_key(my_id: &str) -> String {
        format!("{}{}", Self::msg_key_prefix(), my_id)
    }

    pub fn is_my_msg(key: &str) -> bool {
        key.starts_with(Self::msg_key_prefix())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct HaSetActorState {
    pub up: bool,
    pub ha_set: DashHaSetTable,
}

impl HaSetActorState {
    pub fn new_actor_msg(up: bool, my_id: &str, ha_set: DashHaSetTable) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), &Self { up: true, ha_set })
    }

    pub fn to_actor_msg(&self, my_id: &str) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), self)
    }

    pub fn msg_key_prefix() -> &'static str {
        "HaSetStateUpdate|"
    }

    pub fn msg_key(my_id: &str) -> String {
        format!("{}{}", Self::msg_key_prefix(), my_id)
    }

    pub fn is_my_msg(key: &str) -> bool {
        key.starts_with(Self::msg_key_prefix())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct ActorRegistration {
    pub active: bool,
}
pub enum RegistrationType {
    DPUState,
    VDPUState,
    HaSetState
}

impl ActorRegistration {
    pub fn new_actor_msg(active: bool, reg_type: RegistrationType, my_id: &str) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(reg_type, my_id), &Self { active })
    }

    pub fn msg_key_prefix(reg_type: RegistrationType) -> &'static str {
        match reg_type {
            RegistrationType::DPUState => "DPUStateRegister|",
            RegistrationType::VDPUState => "VDPUStateRegister|",
            RegistrationType::HaSetActorState => "HaSetStateRegister|",
        }
    }

    pub fn msg_key(reg_type: RegistrationType, my_id: &str) -> String {
        format!("{}{}", Self::msg_key_prefix(reg_type), my_id)
    }

    pub fn get_registered_actors(incoming: &Incoming, reg_type: RegistrationType) -> Vec<ServicePath> {
        let registered_actors = incoming.get_by_prefix(Self::msg_key_prefix(reg_type));
        registered_actors
            .iter()
            .filter_map(|entry| {
                let ActorRegistration { active } = entry.msg.deserialize_data().ok()?;
                if active {
                    Some(entry.source.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn is_my_msg(key: &str, reg_type: RegistrationType) -> bool {
        key.starts_with(Self::msg_key_prefix(reg_type))
    }
}
