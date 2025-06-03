use crate::actors::dpu::DpuActor;
use crate::actors::{ActorCreator, DbBasedActor};
use crate::ha_actor_messages::{ActorRegistration, DpuActorState, RegistrationType, VDpuActorState};
use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use swbus_actor::Context;
use swbus_actor::{state::incoming::Incoming, state::outgoing::Outgoing, Actor, State};
use swbus_edge::SwbusEdgeRuntime;
use swss_common::{KeyOpFieldValues, KeyOperation, SubscriberStateTable};
use swss_common_bridge::consumer::spawn_consumer_bridge;

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2111-dpu--vdpu-definitions>
#[derive(Deserialize, Clone)]
struct VDpu {
    main_dpu_ids: Option<String>,
}

pub struct VDpuActor {
    /// The id of this vdpu
    id: String,
    vdpu: Option<VDpu>,
}

impl DbBasedActor for VDpuActor {
    fn new(key: String) -> Result<Self> {
        let actor = VDpuActor { id: key, vdpu: None };
        Ok(actor)
    }

    fn table_name() -> &'static str {
        "VDPU"
    }

    fn name() -> &'static str {
        "vdpu"
    }
}

impl VDpuActor {
    async fn register_to_dpu_actor(&self, outgoing: &mut Outgoing, active: bool) -> Result<()> {
        if self.vdpu.is_none() {
            return Ok(());
        }
        if let Some(main_dpu_ids) = &self.vdpu.as_ref().unwrap().main_dpu_ids {
            let msg = ActorRegistration::new_actor_msg(active, RegistrationType::DPUState, &self.id)?;
            main_dpu_ids
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .for_each(|id| {
                    outgoing.send(outgoing.from_my_sp(DpuActor::name(), id), msg.clone());
                });
        }
        Ok(())
    }

    async fn handle_vdpu_message(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();
        let dpu_kfv: KeyOpFieldValues = incoming.get(key)?.deserialize_data()?;
        if dpu_kfv.operation == KeyOperation::Del {
            // unregister from the DPU Actor
            self.register_to_dpu_actor(outgoing, false).await?;
            context.stop();
            return Ok(());
        }

        self.vdpu = Some(swss_serde::from_field_values(&dpu_kfv.field_values)?);

        // Subscribe to the DPU Actor for state updates
        self.register_to_dpu_actor(outgoing, true).await?;
        Ok(())
    }

    async fn handle_dpu_state_update(&mut self, incoming: &Incoming, outgoing: &mut Outgoing) -> Result<()> {
        let vdpu_state = self.calculate_vdpu_state(incoming);
        let msg = vdpu_state.to_actor_msg(&self.id)?;
        let peer_actors = ActorRegistration::get_registered_actors(incoming, RegistrationType::VDPUState);
        for actor_sp in peer_actors {
            outgoing.send(actor_sp, msg.clone());
        }
        Ok(())
    }

    fn calculate_vdpu_state(&self, incoming: &Incoming) -> VDpuActorState {
        if let Some(main_dpu_ids) = &self.vdpu.as_ref().unwrap().main_dpu_ids {
            let mut final_state = true;
            let dpus = main_dpu_ids
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .filter_map(|id| {
                    let msg = incoming.get(&format!("{}{}", DpuActorState::msg_key_prefix(), id));

                    if msg.is_err() {
                        final_state = false;
                        return None;
                    }

                    let msg = msg.unwrap();
                    if let Ok(dpu) = msg.deserialize_data::<DpuActorState>() {
                        final_state = final_state && dpu.up;
                        Some((id.to_string(), dpu))
                    } else {
                        final_state = false;
                        None
                    }
                })
                .collect();

            VDpuActorState { up: final_state, dpus }
        } else {
            VDpuActorState {
                up: false,
                dpus: HashMap::new(),
            }
        }
    }

    async fn handle_vdpu_state_registration(
        &mut self,
        key: &str,
        incoming: &Incoming,
        outgoing: &mut Outgoing,
    ) -> Result<()> {
        let entry = incoming.get_entry(key)?;
        let ActorRegistration { active, .. } = entry.msg.deserialize_data()?;
        if active {
            let vdpu_state = self.calculate_vdpu_state(incoming);
            let msg = vdpu_state.to_actor_msg(&self.id)?;
            outgoing.send(entry.source.clone(), msg);
        }
        Ok(())
    }
}

impl Actor for VDpuActor {
    async fn handle_message(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        if key == Self::table_name() {
            return self.handle_vdpu_message(state, key, context).await;
        }

        if self.vdpu.is_none() {
            return Ok(());
        }

        let (_internal, incoming, outgoing) = state.get_all();

        if DpuActorState::is_my_msg(key) {
            return self.handle_dpu_state_update(incoming, outgoing).await;
        } else if ActorRegistration::is_my_msg(key, RegistrationType::VDPUState) {
            return self.handle_vdpu_state_registration(key, incoming, outgoing).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::ha_actor_messages::{ActorRegistration, RegistrationType};

    use crate::{
        actors::{
            dpu::DpuActor,
            ha_set::HaSetActor,
            test::{self, recv, send},
            vdpu::VDpuActor,
            DbBasedActor,
        },
        ha_actor_messages::*,
    };
    use std::time::Duration;

    #[tokio::test]
    async fn vdpu_actor() {
        let runtime = test::create_actor_runtime(1, "10.0.1.0").await;

        let vdpu_actor = VDpuActor {
            id: "test-vdpu".into(),
            vdpu: None,
        };

        let handle = runtime.spawn(vdpu_actor, VDpuActor::name(), "test-vdpu");
        let dpu_obj = serde_json::json!({"pa_ipv4": "1.2.3.4",
            "dpu_id": 1,
            "dpu_name":"switch1_dpu0",
            "is_managed":true,
            "orchagent_zmq_port": 8100,
            "swbus_port": 23606,
            "midplane_ipv4": "127.0.0.1",
            "remote_dpu": false,
            "npu_ipv4": "10.0.1.0",
        });
        let mut down_dpu_obj = dpu_obj.clone();
        down_dpu_obj
            .as_object_mut()
            .unwrap()
            .insert("up".to_string(), serde_json::json!(false));
        let mut up_dpu_obj = dpu_obj.clone();
        up_dpu_obj
            .as_object_mut()
            .unwrap()
            .insert("up".to_string(), serde_json::json!(true));

        #[rustfmt::skip]
        let commands = [
            // Receiving DPU config-db object from swss-common bridge
            send! { key: VDpuActor::table_name(), data: { "key": VDpuActor::table_name(), "operation": "Set", "field_values": {"main_dpu_ids": "switch1_dpu0" }}, addr: runtime.sp("swss-common-bridge", VDpuActor::table_name()) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::DPUState, "test-vdpu"), data: { "active": true }, addr: runtime.sp(DpuActor::name(), "switch1_dpu0") },

            // receive VDPU state registration
            send! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, "test-ha-set"), data: { "active": true}, addr: runtime.sp(HaSetActor::name(), "test-ha-set") },
            // send VDPU state update immediately to ha-set
            recv! { key: VDpuActorState::msg_key("test-vdpu"), data: { "up": false, "dpus": {} }, addr: runtime.sp(HaSetActor::name(), "test-ha-set") },
            // receive 

            // receive DPU state update
            send! { key: DpuActorState::msg_key("switch1_dpu0"), data: up_dpu_obj, addr: runtime.sp(DpuActor::name(), "switch1_dpu0") },
            recv! { key: VDpuActorState::msg_key("test-vdpu"), data: { "up": true, "dpus": {"switch1_dpu0": up_dpu_obj} }, addr: runtime.sp(HaSetActor::name(), "test-ha-set") },

            // receive DPU down update
            send! { key: DpuActorState::msg_key("switch1_dpu0"), data: down_dpu_obj, addr: runtime.sp(DpuActor::name(), "switch1_dpu0") },
            recv! { key: VDpuActorState::msg_key("test-vdpu"), data: { "up": false, "dpus": {"switch1_dpu0": down_dpu_obj} }, addr: runtime.sp(HaSetActor::name(), "test-ha-set") },

            send! { key: VDpuActor::table_name(), data: { "key": VDpuActor::table_name(), "operation": "Del", "field_values": {"main_dpu_ids": "dpu0, dpu1"}}, addr: runtime.sp("swss-common-bridge", VDpuActor::table_name()) },
            
        ];

        test::run_commands(&runtime, runtime.sp(VDpuActor::name(), "test-vdpu"), &commands).await;
        if tokio::time::timeout(Duration::from_secs(1), handle).await.is_err() {
            panic!("timeout waiting for actor to terminate");
        }
    }
}
