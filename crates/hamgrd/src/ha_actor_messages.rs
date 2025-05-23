// temporarily disable unused warning until vdpu/ha-set actors are implemented
#![allow(unused)]
use crate::db_structs::Dpu;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use swbus_actor::{state::incoming::Incoming, ActorMessage};
use swbus_edge::swbus_proto::swbus::ServicePath;

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct DpuActorState {
    pub up: bool,
    pub dpu: Dpu,
}

impl DpuActorState {
    pub fn new_actor_msg(my_id: &str, up: bool, dpu: &Dpu) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), &Self { up, dpu: dpu.clone() })
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
}

impl VDpuActorState {
    pub fn new_actor_msg(up: bool, my_id: &str) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(my_id), &Self { up })
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
pub struct ActorRegistration {
    pub active: bool,
}
pub enum RegistrationType {
    DPUState,
    VDPUState,
}

impl ActorRegistration {
    pub fn new_actor_msg(active: bool, reg_type: RegistrationType, my_id: &str) -> Result<ActorMessage> {
        ActorMessage::new(Self::msg_key(reg_type, my_id), &Self { active })
    }

    pub fn msg_key_prefix(reg_type: RegistrationType) -> &'static str {
        match reg_type {
            RegistrationType::DPUState => "DPUStateRegister|",
            RegistrationType::VDPUState => "VDPUStateRegister|",
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
