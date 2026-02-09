// temporarily disable unused warning until vdpu/ha-set actors are implemented
#![allow(unused)]
use crate::db_structs::{DashBfdProbeState, DashHaSetTable, Dpu, DpuState, RemoteDpu};
use anyhow::Result;
use chrono::{format::ParseError, DateTime, TimeZone, Utc};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
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
    pub dpu_pmon_state: Option<DpuState>,
    pub dpu_bfd_state: Option<DashBfdProbeState>,
}

impl DpuActorState {
    pub fn from_dpu(
        dpu_name: &str,
        dpu: &Dpu,
        is_managed: bool,
        npu_ipv4: &str,
        npu_ipv6: &Option<String>,
        pmon_state: Option<DpuState>,
        bfd_state: Option<DashBfdProbeState>,
    ) -> Self {
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
            dpu_pmon_state: pmon_state,
            dpu_bfd_state: bfd_state,
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
            dpu_pmon_state: None,
            dpu_bfd_state: None,
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
    pub vdpu_ids: Vec<String>
}

impl HaSetActorState {
    pub fn new_actor_msg(up: bool, my_id: &str, ha_set: DashHaSetTable, vdpu_ids: &Vec<String>) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), &Self { up: true, ha_set: ha_set, vdpu_ids: vdpu_ids.clone() })
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

#[allow(clippy::enum_variant_names)]
pub enum RegistrationType {
    DPUState,
    VDPUState,
    HaSetState,
    HAStateChanged
}

impl ActorRegistration {
    pub fn new_actor_msg(active: bool, reg_type: RegistrationType, my_id: &str) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(reg_type, my_id), &Self { active })
    }

    pub fn msg_key_prefix(reg_type: RegistrationType) -> &'static str {
        match reg_type {
            RegistrationType::DPUState => "DPUStateRegister|",
            RegistrationType::VDPUState => "VDPUStateRegister|",
            RegistrationType::HaSetState => "HaSetStateRegister|",
            RegistrationType::HAStateChanged => "HAStateChangedRegister|"
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

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct VoteRequest {
    // routing info
    pub dst_actor_id: String,

    // state of the source HA scope
    pub term: int,
    pub state: String,
    pub desired_state: String,
}

impl VoteRequest {
    pub fn new_actor_msg(my_id: &str, dst_id: &str, my_term: int, my_state: &str, my_desired_state: &str) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), &Self {
            dst_actor_id: dst_id.to_string(),
            term: my_term,
            state: my_state.to_string(),
            desired_state: my_desired_state.to_string()
        })
    }

    pub fn to_actor_msg(&self, my_id: &str) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), self)
    }

    pub fn msg_key_prefix() -> &'static str {
        "VoteRequest|"
    }

    pub fn msg_key(my_id: &str) -> String {
        format!("{}{}", Self::msg_key_prefix(), my_id)
    }

    pub fn is_my_msg(key: &str) -> bool {
        key.starts_with(Self::msg_key_prefix())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct VoteReply {
    // routing info
    pub dst_actor_id: String,

    // response as a string
    pub response: String,
}

impl VoteReply {
    pub fn new_actor_msg(my_id: &str, dst_id: &str, response: &str) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), &Self {
            dst_actor_id: dst_id.to_string(),
            response: response.to_string()
        })
    }

    pub fn to_actor_msg(&self, my_id: &str) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), self)
    }

    pub fn msg_key_prefix() -> &'static str {
        "VoteReply|"
    }

    pub fn msg_key(my_id: &str) -> String {
        format!("{}{}", Self::msg_key_prefix(), my_id)
    }

    pub fn is_my_msg(key: &str) -> bool {
        key.starts_with(Self::msg_key_prefix())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct BulkSyncUpdate {
    // routing info
    pub dst_actor_id: String,

    // message meta
    pub finished: bool
}

impl BulkSyncUpdate {
    pub fn new_actor_msg(my_id: &str, dst_id: &str, finished: bool) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), &Self {
            dst_actor_id: dst_id.to_string(),
            finished: finished
        })
    }

    pub fn to_actor_msg(&self, my_id: &str) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), self)
    }

    pub fn msg_key_prefix() -> &'static str {
        "BulkSyncUpdate|"
    }

    pub fn msg_key(my_id: &str) -> String {
        format!("{}{}", Self::msg_key_prefix(), my_id)
    }

    pub fn is_my_msg(key: &str) -> bool {
        key.starts_with(Self::msg_key_prefix())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct HAStateChanged {
    // state transition
    pub prev_state: String,
    pub new_state: String,
    pub timestamp: i64,
    pub term: String
}

impl HAStateChanged {
    pub fn new_actor_msg(my_id: &str, prev_state: &str, new_state: &str, ts: i64, term: &str) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), &Self {
            prev_state: prev_state.to_string(),
            new_state: new_state.to_string(),
            timestamp: ts,
            term: term.to_string()
        })
    }

    pub fn to_actor_msg(&self, my_id: &str) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), self)
    }

    pub fn msg_key_prefix() -> &'static str {
        "HAStateChanged|"
    }

    pub fn msg_key(my_id: &str) -> String {
        format!("{}{}", Self::msg_key_prefix(), my_id)
    }

    pub fn is_my_msg(key: &str) -> bool {
        key.starts_with(Self::msg_key_prefix())
    }
}

pub struct SelfNotification {
    // notifications of ha_events happening in the background
    pub ha_event: String
}

impl SelfNotification {
    pub fn new_actor_msg(my_id: &str, ha_event: &str) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), &Self {
            ha_event: ha_event
        })
    }

    pub fn to_actor_msg(&self, my_id: &str) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), self)
    }

    pub fn msg_key_prefix() -> &'static str {
        "SelfNotification|"
    }

    pub fn msg_key(my_id: &str) -> String {
        format!("{}{}", Self::msg_key_prefix(), my_id)
    }

    pub fn is_my_msg(key: &str) -> bool {
        key.starts_with(Self::msg_key_prefix())
    }
}