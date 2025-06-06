use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use swbus_actor::{spawn, Actor, ActorMessage, State};
use swbus_edge::swbus_proto::swbus::ServicePath;
use swss_common::{KeyOpFieldValues, SubscriberStateTable, Table};
use swss_common_bridge::consumer::spawn_consumer_bridge;

pub async fn spawn_dpu_actors() -> Result<()> {
    // Get a list of DPUs from CONFIG_DB::DPU
    let config_db = crate::db_named("CONFIG_DB").await?;
    let mut dpu_table = Table::new_async(config_db, "DPU").await?;
    let dpu_ids = dpu_table.get_keys_async().await?;

    // Spawn DPU actors
    for dpu_id in &dpu_ids {
        let dpu_fvs = dpu_table.get_async(dpu_id).await?.unwrap();
        let dpu: Dpu = swss_serde::from_field_values(&dpu_fvs)?;
        let actor = DpuActor {
            id: dpu_id.clone(),
            dpu: dpu.clone(),
            vdpu_addr: None,
        };
        spawn(actor, crate::sp("dpu", dpu_id));
    }

    let rt = swbus_actor::get_global_runtime().as_ref().unwrap().get_swbus_edge();
    // Spawn the CHASSIS_STATE_DB::DPU_STATE swss-common bridge
    {
        let rt = rt.clone();
        let chassis_state_db = crate::db_named("CHASSIS_STATE_DB").await?;
        let dpu_state_sst = SubscriberStateTable::new_async(chassis_state_db, "DPU_STATE", None, None).await?;
        let addr = crate::sp("swss-common-bridge", "DPU_STATE");
        spawn_consumer_bridge(rt, addr, dpu_state_sst, |kfv| {
            (crate::sp("DPU", &kfv.key), "DPU_STATE".to_owned())
        });
    }

    // Spawn the DASH_BFD_PROBE_STATE bridge
    {
        let rt = rt.clone();
        let dpu_state_db = crate::db_named("DPU_STATE_DB").await?;
        let bfd_state_sst = SubscriberStateTable::new_async(dpu_state_db, "DASH_BFD_PROBE_STATE", None, None).await?;
        let addr = crate::sp("swss-common-bridge", "DASH_BFD_PROBE_STATE");
        spawn_consumer_bridge(rt, addr, bfd_state_sst, |kfv| {
            (crate::sp("DPU", &kfv.key), "DASH_BFD_PROBE_STATE".to_owned())
        });
    }

    Ok(())
}

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2111-dpu--vdpu-definitions>
#[derive(Deserialize, Clone)]
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

struct DpuActor {
    /// The id of this dpu
    id: String,

    /// Data from CONFIG_DB
    dpu: Dpu,

    /// The vdpu actor to send updates to
    vdpu_addr: Option<ServicePath>,
}

impl Actor for DpuActor {
    async fn handle_message(&mut self, state: &mut State, _key: &str) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();

        // Check pmon state from DPU_STATE table
        let dpu_state_kfv: KeyOpFieldValues = incoming.get("DPU_STATE")?.deserialize_data()?;
        ensure!(dpu_state_kfv.key == self.id);
        let dpu_state: DpuState = swss_serde::from_field_values(&dpu_state_kfv.field_values)?;
        let pmon_dpu_up = dpu_state.all_up();

        // Check bfd state from DASH_BFD_PROBE_STATE
        let bfd_probe_kfv: KeyOpFieldValues = incoming.get("DASH_BFD_PROBE_STATE")?.deserialize_data()?;
        ensure!(bfd_probe_kfv.key == self.id);
        let bfd_probe: DashBfdProbeState = swss_serde::from_field_values(&bfd_probe_kfv.field_values)?;
        let bfd_dpu_up = bfd_probe.v4_bfd_up_sessions.contains(&self.dpu.pa_ipv4);

        // Send an update to the vdpu
        let up = pmon_dpu_up && bfd_dpu_up;
        let msg = ActorMessage::new(&self.id, &DpuActorState { up })?;

        if let Some(vdpu_addr) = &self.vdpu_addr {
            outgoing.send(vdpu_addr.clone(), msg);
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct DpuActorState {
    up: bool,
}

#[cfg(test)]
mod test {
    use crate::{
        actors::{
            dpu::{Dpu, DpuActor},
            test::{self, recv, send},
        },
        sp,
    };

    #[tokio::test]
    async fn dpu_actor() {
        test::setup_actor_runtime().await;

        let dpu_actor = DpuActor {
            id: "test-dpu".into(),
            dpu: Dpu {
                pa_ipv4: "1.2.3.4".into(),
            },
            vdpu_addr: Some(sp("vdpu", "test-vdpu")),
        };
        swbus_actor::spawn(dpu_actor, sp("dpu", "test-dpu"));

        #[rustfmt::skip]
        let commands = [
            // Bring up dpu
            send! { key: "DPU_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "dpu_midplane_link_state": "up", "dpu_control_plane_state": "up", "dpu_data_plane_state": "up" }}, fail },
            send! { key: "DASH_BFD_PROBE_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "v4_bfd_up_sessions": "1.2.3.4,1.2.3.5,1.2.3.6" }} },
            recv! { key: "test-dpu", data: { "up": true }, addr: sp("vdpu", "test-vdpu") },

            // Simulate DPU_STATE planes going down then up
            send! { key: "DPU_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "dpu_midplane_link_state": "down", "dpu_control_plane_state": "up", "dpu_data_plane_state": "up" }} },
            recv! { key: "test-dpu", data: { "up": false }, addr: sp("vdpu", "test-vdpu") },
            send! { key: "DPU_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "dpu_midplane_link_state": "down", "dpu_control_plane_state": "up", "dpu_data_plane_state": "down" }} },
            recv! { key: "test-dpu", data: { "up": false }, addr: sp("vdpu", "test-vdpu") },
            send! { key: "DPU_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "dpu_midplane_link_state": "down", "dpu_control_plane_state": "up", "dpu_data_plane_state": "up" }} },
            recv! { key: "test-dpu", data: { "up": false }, addr: sp("vdpu", "test-vdpu") },
            send! { key: "DPU_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "dpu_midplane_link_state": "up", "dpu_control_plane_state": "down", "dpu_data_plane_state": "up" }} },
            recv! { key: "test-dpu", data: { "up": false }, addr: sp("vdpu", "test-vdpu") },
            send! { key: "DPU_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "dpu_midplane_link_state": "up", "dpu_control_plane_state": "up", "dpu_data_plane_state": "up" }} },
            recv! { key: "test-dpu", data: { "up": true }, addr: sp("vdpu", "test-vdpu") },

            // Simulate BFD probe going down
            send! { key: "DASH_BFD_PROBE_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "v4_bfd_up_sessions": "1.2.3.5,1.2.3.6" }} },
            recv! { key: "test-dpu", data: { "up": false }, addr: sp("vdpu", "test-vdpu") },
            send! { key: "DASH_BFD_PROBE_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "v4_bfd_up_sessions": "1.2.3.5,1.2.3.6,1.2.3.7" }} },
            recv! { key: "test-dpu", data: { "up": false }, addr: sp("vdpu", "test-vdpu") },
            send! { key: "DASH_BFD_PROBE_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "v4_bfd_up_sessions": "1.2.3.5,1.2.3.6,1.2.3.7,1.2.3.4" }} },
            recv! { key: "test-dpu", data: { "up": true }, addr: sp("vdpu", "test-vdpu") },
        ];
        test::run_commands(sp("dpu", "test-dpu"), &commands).await;
    }
}
