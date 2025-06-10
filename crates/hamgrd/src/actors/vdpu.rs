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
use tracing::error;

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
        let Some(vdpu_state) = self.calculate_vdpu_state(incoming) else {
            // vdpu data is not available yet
            return Ok(());
        };
        let msg = vdpu_state.to_actor_msg(&self.id)?;
        let peer_actors = ActorRegistration::get_registered_actors(incoming, RegistrationType::VDPUState);
        for actor_sp in peer_actors {
            outgoing.send(actor_sp, msg.clone());
        }
        Ok(())
    }

    fn calculate_vdpu_state(&self, incoming: &Incoming) -> Option<VDpuActorState> {
        if let Some(main_dpu_ids) = &self.vdpu.as_ref().unwrap().main_dpu_ids {
            // currently we only support one main dpu so we just take the first one
            let dpu_id = main_dpu_ids.split(',').map(str::trim).find(|s| !s.is_empty());

            let Some(dpu_id) = dpu_id else {
                error!("No main DPU is configured");
                return None;
            };

            let msg = incoming.get(&format!("{}{}", DpuActorState::msg_key_prefix(), dpu_id));

            let Ok(msg) = msg else {
                // dpu data is not available yet
                return None;
            };

            if let Ok(dpu) = msg.deserialize_data::<DpuActorState>() {
                let vdpu = VDpuActorState { up: dpu.up, dpu };
                return Some(vdpu);
            } else {
                error!("Failed to deserialize DpuActorState from the message");
                return None;
            }
        }
        None
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
            let Some(vdpu_state) = self.calculate_vdpu_state(incoming) else {
                // vdpu data is not available yet
                return Ok(());
            };
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
            test::{self, make_remote_dpu_actor_state, recv, send},
            vdpu::VDpuActor,
            DbBasedActor,
        },
        ha_actor_messages::*,
    };
    use std::time::Duration;

    #[tokio::test]
    async fn vdpu_actor() {
        let runtime = test::create_actor_runtime(1, "10.0.0.0", "10::").await;

        let dpu_actor_down_state = make_remote_dpu_actor_state(1, 0);
        let mut dpu_actor_up_state = dpu_actor_down_state.clone();
        dpu_actor_up_state.up = true;
        let vdpu_actor = VDpuActor {
            id: "test-vdpu".into(),
            vdpu: None,
        };

        let handle = runtime.spawn(vdpu_actor, VDpuActor::name(), "test-vdpu");

        #[rustfmt::skip]
        let commands = [
            // Receiving DPU config-db object from swss-common bridge
            send! { key: VDpuActor::table_name(), data: { "key": VDpuActor::table_name(), "operation": "Set", "field_values": {"main_dpu_ids": "switch1_dpu0" }}, addr: runtime.sp("swss-common-bridge", VDpuActor::table_name()) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::DPUState, "test-vdpu"), data: { "active": true }, addr: runtime.sp(DpuActor::name(), "switch1_dpu0") },

            // receive VDPU state registration
            send! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, "test-ha-set"), data: { "active": true}, addr: runtime.sp(HaSetActor::name(), "test-ha-set") },

            // receive DPU state update
            send! { key: DpuActorState::msg_key("switch1_dpu0"), data: dpu_actor_up_state, addr: runtime.sp(DpuActor::name(), "switch1_dpu0") },
            recv! { key: VDpuActorState::msg_key("test-vdpu"), data: { "up": true, "dpu": dpu_actor_up_state }, addr: runtime.sp(HaSetActor::name(), "test-ha-set") },

            // receive DPU down update
            send! { key: DpuActorState::msg_key("switch1_dpu0"), data: dpu_actor_down_state, addr: runtime.sp(DpuActor::name(), "switch1_dpu0") },
            recv! { key: VDpuActorState::msg_key("test-vdpu"), data: { "up": false, "dpu": dpu_actor_down_state }, addr: runtime.sp(HaSetActor::name(), "test-ha-set") },

            send! { key: VDpuActor::table_name(), data: { "key": VDpuActor::table_name(), "operation": "Del", "field_values": {"main_dpu_ids": "dpu0, dpu1"}}, addr: runtime.sp("swss-common-bridge", VDpuActor::table_name()) },
            
        ];

        test::run_commands(&runtime, runtime.sp(VDpuActor::name(), "test-vdpu"), &commands).await;
        if tokio::time::timeout(Duration::from_secs(1), handle).await.is_err() {
            panic!("timeout waiting for actor to terminate");
        }
    }
}
