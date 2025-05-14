use crate::actors::dpu::DpuActor;
use crate::actors::ActorCreator;
use crate::ha_actor_messages::{ActorRegistration, DpuActorState, RegistrationType, VDpuActorState};
use anyhow::Result;
use serde::Deserialize;
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

impl VDpuActor {
    pub fn new(key: String) -> Result<Self> {
        let actor = VDpuActor { id: key, vdpu: None };
        Ok(actor)
    }

    pub fn table_name() -> &'static str {
        "VDPU"
    }

    pub fn name() -> &'static str {
        "VDPU"
    }

    pub async fn start_actor_creator(edge_runtime: Arc<SwbusEdgeRuntime>) -> Result<()> {
        let dpu_ac = ActorCreator::new(
            edge_runtime.new_sp(Self::name(), ""),
            edge_runtime.clone(),
            false,
            |key: String| -> Result<Self> { Self::new(key) },
        );

        tokio::task::spawn(dpu_ac.run());
        let config_db = crate::db_named("CONFIG_DB").await?;
        let sst = SubscriberStateTable::new_async(config_db, Self::table_name(), None, None).await?;
        let addr = crate::sp("swss-common-bridge", Self::table_name());
        spawn_consumer_bridge(edge_runtime.clone(), addr, sst, |kfv: &KeyOpFieldValues| {
            (crate::sp(Self::name(), &kfv.key), Self::table_name().to_owned())
        });
        Ok(())
    }

    async fn register_to_dpu_actor(&self, outgoing: &mut Outgoing, active: bool) -> Result<()> {
        if self.vdpu.is_none() {
            return Ok(());
        }
        if let Some(main_dpu_ids) = &self.vdpu.as_ref().unwrap().main_dpu_ids {
            let msg = ActorRegistration::new_actor_msg(active, RegistrationType::DPUHealth, &self.id)?;
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

        // Subscribe to the DPU Actor for health updates
        self.register_to_dpu_actor(outgoing, true).await?;
        Ok(())
    }

    async fn handle_dpu_health_update(&mut self, incoming: &Incoming, outgoing: &mut Outgoing) -> Result<()> {
        let up = self.calculate_vdpu_state(incoming);
        let msg = VDpuActorState::new_actor_msg(up, &self.id)?;
        let peer_actors = ActorRegistration::get_registered_actors(incoming, RegistrationType::VDPUHealth);
        for actor_sp in peer_actors {
            outgoing.send(actor_sp, msg.clone());
        }
        Ok(())
    }

    fn calculate_vdpu_state(&self, incoming: &Incoming) -> bool {
        if let Some(main_dpu_ids) = &self.vdpu.as_ref().unwrap().main_dpu_ids {
            main_dpu_ids
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .all(|id| {
                    let msg = incoming.get(&format!("{}{}", DpuActorState::msg_key_prefix(), id));
                    if msg.is_err() {
                        return false;
                    }
                    let msg = msg.unwrap();
                    if let Ok(DpuActorState { up }) = msg.deserialize_data() {
                        up
                    } else {
                        false
                    }
                })
        } else {
            false
        }
    }

    async fn handle_vdpu_health_registration(
        &mut self,
        key: &str,
        incoming: &Incoming,
        outgoing: &mut Outgoing,
    ) -> Result<()> {
        let entry = incoming.get_entry(key)?;
        let ActorRegistration { active, .. } = entry.msg.deserialize_data()?;
        if active {
            let up = self.calculate_vdpu_state(incoming);
            let msg = VDpuActorState::new_actor_msg(up, &self.id)?;
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
            return self.handle_dpu_health_update(incoming, outgoing).await;
        } else if ActorRegistration::is_my_msg(key, RegistrationType::VDPUHealth) {
            return self.handle_vdpu_health_registration(key, incoming, outgoing).await;
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
        },
        ha_actor_messages::*,
    };
    use std::time::Duration;

    #[tokio::test]
    async fn vdpu_actor() {
        let runtime = test::create_actor_runtime().await;

        let vdpu_actor = VDpuActor {
            id: "test-vdpu".into(),
            vdpu: None,
        };
        let handle = runtime.spawn(vdpu_actor, VDpuActor::name(), "test-vdpu");

        #[rustfmt::skip]
        let commands = [
            // Receiving DPU config-db object from swss-common bridge
            send! { key: VDpuActor::table_name(), data: { "key": VDpuActor::table_name(), "operation": "Set", "field_values": {"main_dpu_ids": "dpu0, dpu1" }}, addr: runtime.sp("swss-common-bridge", VDpuActor::table_name()) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::DPUHealth, "test-vdpu"), data: { "active": true }, addr: runtime.sp(DpuActor::name(), "dpu0") },
            recv! { key: ActorRegistration::msg_key(RegistrationType::DPUHealth, "test-vdpu"), data: { "active": true }, addr: runtime.sp(DpuActor::name(), "dpu1") },

            // receive VDPU health registration
            send! { key: ActorRegistration::msg_key(RegistrationType::VDPUHealth, "test-ha-set"), data: { "active": true}, addr: runtime.sp(HaSetActor::name(), "test-ha-set") },
            // send VDPU health update immediately to ha-set
            recv! { key: VDpuActorState::msg_key("test-vdpu"), data: { "up": false }, addr: runtime.sp(HaSetActor::name(), "test-ha-set") },
            // receive 

            // receive DPU health update
            send! { key: DpuActorState::msg_key("dpu0"), data: { "up": true }, addr: runtime.sp(DpuActor::name(), "dpu0") },
            recv! { key: VDpuActorState::msg_key("test-vdpu"), data: { "up": false }, addr: runtime.sp(HaSetActor::name(), "test-ha-set") },
            send! { key: DpuActorState::msg_key("dpu1"), data: { "up": true }, addr: runtime.sp(DpuActor::name(), "dpu1") },
            recv! { key: VDpuActorState::msg_key("test-vdpu"), data: { "up": true }, addr: runtime.sp(HaSetActor::name(), "test-ha-set") },

            // receive DPU down update
            send! { key: DpuActorState::msg_key("dpu1"), data: { "up": false }, addr: runtime.sp(DpuActor::name(), "dpu1") },
            recv! { key: VDpuActorState::msg_key("test-vdpu"), data: { "up": false }, addr: runtime.sp(HaSetActor::name(), "test-ha-set") },

            send! { key: VDpuActor::table_name(), data: { "key": VDpuActor::table_name(), "operation": "Del", "field_values": {"main_dpu_ids": "dpu0, dpu1"}}, addr: runtime.sp("swss-common-bridge", VDpuActor::table_name()) },
            
        ];

        test::run_commands(&runtime, runtime.sp(VDpuActor::name(), "test-vdpu"), &commands).await;
        if tokio::time::timeout(Duration::from_secs(1), handle).await.is_err() {
            panic!("timeout waiting for actor to terminate");
        }
    }
}
