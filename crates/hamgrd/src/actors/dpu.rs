use crate::actors::ActorCreator;
use crate::ha_actor_messages::{ActorRegistration, DpuActorState, RegistrationType};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use sonicdb_derive::SonicDb;
use std::sync::Arc;
use swbus_actor::{state::incoming::Incoming, state::outgoing::Outgoing, Actor, Context, State};
use swbus_edge::SwbusEdgeRuntime;
use swss_common::{KeyOpFieldValues, KeyOperation, SonicDbTable, SubscriberStateTable};
use swss_common_bridge::consumer::spawn_consumer_bridge;

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2111-dpu--vdpu-definitions>
#[derive(Deserialize, Serialize, Clone, PartialEq, Eq, Debug, SonicDb)]
#[sonicdb(table_name = "DPU", key_separator = "|", db_name = "CONFIG_DB")]
struct Dpu {
    pa_ipv4: String,
}

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/pmon/smartswitch-pmon.md#dpu_state-definition>
#[derive(Serialize, Deserialize)]
struct DpuState {
    dpu_midplane_link_state: String,
    dpu_control_plane_state: String,
    dpu_data_plane_state: String,
}

impl DpuState {
    fn all_up(&self) -> bool {
        self.dpu_midplane_link_state == "up"
            && self.dpu_control_plane_state == "up"
            && self.dpu_data_plane_state == "up"
    }
}

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/BFD/SmartSwitchDpuLivenessUsingBfd.md#27-dpu-bfd-session-state-updates>
#[derive(Serialize, Deserialize)]
struct DashBfdProbeState {
    v4_bfd_up_sessions: Vec<String>,
}

pub struct DpuActor {
    /// The id of this dpu
    id: String,

    /// Data from CONFIG_DB
    dpu: Option<Dpu>,
}
impl DpuActor {
    pub fn new(key: String) -> Result<Self> {
        let actor = DpuActor { id: key, dpu: None };
        Ok(actor)
    }

    pub fn name() -> &'static str {
        "DPU"
    }

    // returns the main Db table name that this actor is associated to
    pub fn table_name() -> &'static str {
        "DPU"
    }

    pub async fn start_actor_creator(edge_runtime: Arc<SwbusEdgeRuntime>) -> Result<()> {
        let dpu_ac = ActorCreator::new(
            edge_runtime.new_sp(Self::name(), ""),
            edge_runtime.clone(),
            false,
            |key: String| -> Result<Self> { Self::new(key) },
        );

        tokio::task::spawn(dpu_ac.run());

        // dpu actor is spawned for both local dpu and remote dpu
        let config_db = crate::db_named(Dpu::db_name()).await?;
        let sst = SubscriberStateTable::new_async(config_db, Dpu::table_name(), None, None).await?;
        let addr = crate::common_bridge_sp::<Dpu>(&edge_runtime);
        let base_addr = edge_runtime.get_base_sp();
        spawn_consumer_bridge(
            edge_runtime.clone(),
            addr,
            sst,
            move |kfv: &KeyOpFieldValues| {
                let mut addr = base_addr.clone();
                addr.resource_type = Self::name().to_owned();
                addr.resource_id = kfv.key.clone();
                (addr, Dpu::table_name().to_owned())
            },
            |_| true,
        );
        Ok(())
    }

    async fn handle_dpu_message(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        let (_internal, incoming, _outgoing) = state.get_all();
        let dpu_kfv: KeyOpFieldValues = incoming.get(key)?.deserialize_data()?;
        if dpu_kfv.operation == KeyOperation::Del {
            context.stop();
            return Ok(());
        }

        self.dpu = Some(swss_serde::from_field_values(&dpu_kfv.field_values)?);
        Ok(())
    }

    fn calculate_dpu_state(&self, incoming: &Incoming) -> Result<bool> {
        // Check pmon state from DPU_STATE table
        let dpu_state_kfv: KeyOpFieldValues = incoming.get("DPU_STATE")?.deserialize_data()?;
        ensure!(dpu_state_kfv.key == self.id);
        let dpu_state: DpuState = swss_serde::from_field_values(&dpu_state_kfv.field_values)?;
        let pmon_dpu_up = dpu_state.all_up();

        // Check bfd state from DASH_BFD_PROBE_STATE
        let bfd_probe_kfv: KeyOpFieldValues = incoming.get("DASH_BFD_PROBE_STATE")?.deserialize_data()?;
        ensure!(bfd_probe_kfv.key == self.id);
        let bfd_probe: DashBfdProbeState = swss_serde::from_field_values(&bfd_probe_kfv.field_values)?;
        let bfd_dpu_up = bfd_probe
            .v4_bfd_up_sessions
            .contains(&self.dpu.as_ref().unwrap().pa_ipv4);

        Ok(pmon_dpu_up && bfd_dpu_up)
    }

    async fn update_dpu_state(&mut self, incoming: &Incoming, outgoing: &mut Outgoing) -> Result<()> {
        if let Ok(up) = self.calculate_dpu_state(incoming) {
            let msg = DpuActorState::new_actor_msg(up, &self.id)?;
            let peer_actors = ActorRegistration::get_registered_actors(incoming, RegistrationType::DPUHealth);
            for actor_sp in peer_actors {
                outgoing.send(actor_sp, msg.clone());
            }
        }
        Ok(())
    }

    async fn handle_dpu_health_registration(
        &mut self,
        key: &str,
        incoming: &Incoming,
        outgoing: &mut Outgoing,
    ) -> Result<()> {
        let entry = incoming.get_entry(key)?;
        let ActorRegistration { active, .. } = entry.msg.deserialize_data()?;
        if active {
            if let Ok(up) = self.calculate_dpu_state(incoming) {
                let msg = DpuActorState::new_actor_msg(up, &self.id)?;
                outgoing.send(entry.source.clone(), msg);
            }
        }
        Ok(())
    }
}

impl Actor for DpuActor {
    async fn handle_message(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();
        if key == "DPU" {
            return self.handle_dpu_message(state, key, context).await;
        }

        // Ignore messages if DPU is not present yet
        if self.dpu.is_none() {
            return Ok(());
        }

        if key == "DPU_STATE" || key == "DASH_BFD_PROBE_STATE" {
            return self.update_dpu_state(incoming, outgoing).await;
        } else if ActorRegistration::is_my_msg(key, RegistrationType::DPUHealth) {
            return self.handle_dpu_health_registration(key, incoming, outgoing).await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::actors::{
        dpu::DpuActor,
        test::{self, recv, send},
    };
    use std::time::Duration;

    #[tokio::test]
    async fn dpu_actor() {
        let runtime = test::create_actor_runtime().await;

        let dpu_actor = DpuActor {
            id: "test-dpu".into(),
            dpu: None,
        };
        let handle = runtime.spawn(dpu_actor, "dpu", "test-dpu");

        #[rustfmt::skip]
        let commands = [
            // Bring up dpu
            send! { key: "DPU_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "dpu_midplane_link_state": "up", "dpu_control_plane_state": "up", "dpu_data_plane_state": "up" }} },
            // Receiving DPU config-db object from swss-common bridge
            send! { key: DpuActor::table_name(), data: { "key": DpuActor::table_name(), "operation": "Set", "field_values": {"pa_ipv4": "1.2.3.4" }}, addr: runtime.sp("swss-common-bridge", DpuActor::table_name()) },
            send! { key: "DPUHealthRegister-vdpu/test-vdpu", data: { "active": true}, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "DASH_BFD_PROBE_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "v4_bfd_up_sessions": "1.2.3.4,1.2.3.5,1.2.3.6" }} },
            recv! { key: "DPUHealthUpdate-test-dpu", data: { "up": true }, addr: runtime.sp("vdpu", "test-vdpu") },

            // Simulate DPU_STATE planes going down then up
            send! { key: "DPU_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "dpu_midplane_link_state": "down", "dpu_control_plane_state": "up", "dpu_data_plane_state": "up" }} },
            recv! { key: "DPUHealthUpdate-test-dpu", data: { "up": false }, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "DPU_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "dpu_midplane_link_state": "down", "dpu_control_plane_state": "up", "dpu_data_plane_state": "down" }} },
            recv! { key: "DPUHealthUpdate-test-dpu", data: { "up": false }, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "DPU_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "dpu_midplane_link_state": "down", "dpu_control_plane_state": "up", "dpu_data_plane_state": "up" }} },
            recv! { key: "DPUHealthUpdate-test-dpu", data: { "up": false }, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "DPU_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "dpu_midplane_link_state": "up", "dpu_control_plane_state": "down", "dpu_data_plane_state": "up" }} },
            recv! { key: "DPUHealthUpdate-test-dpu", data: { "up": false }, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "DPU_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "dpu_midplane_link_state": "up", "dpu_control_plane_state": "up", "dpu_data_plane_state": "up" }} },
            recv! { key: "DPUHealthUpdate-test-dpu", data: { "up": true }, addr: runtime.sp("vdpu", "test-vdpu") },

            // Simulate BFD probe going down
            send! { key: "DASH_BFD_PROBE_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "v4_bfd_up_sessions": "1.2.3.5,1.2.3.6" }} },
            recv! { key: "DPUHealthUpdate-test-dpu", data: { "up": false }, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "DASH_BFD_PROBE_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "v4_bfd_up_sessions": "1.2.3.5,1.2.3.6,1.2.3.7" }} },
            recv! { key: "DPUHealthUpdate-test-dpu", data: { "up": false }, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "DASH_BFD_PROBE_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "v4_bfd_up_sessions": "1.2.3.5,1.2.3.6,1.2.3.7,1.2.3.4" }} },
            recv! { key: "DPUHealthUpdate-test-dpu", data: { "up": true }, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: DpuActor::table_name(), data: { "key": DpuActor::table_name(), "operation": "Del", "field_values": {"pa_ipv4": "1.2.3.4" }}, addr: runtime.sp("swss-common-bridge", DpuActor::table_name()) },
        ];
        test::run_commands(&runtime, runtime.sp("dpu", "test-dpu"), &commands).await;
        if tokio::time::timeout(Duration::from_secs(1), handle).await.is_err() {
            panic!("timeout waiting for actor to terminate");
        }
    }
}
