use crate::actors::{spawn_consumer_bridge_for_actor, spawn_zmq_producer_bridge, ActorCreator};
use crate::db_structs::*;
use crate::ha_actor_messages::{ActorRegistration, DpuActorState, RegistrationType};
use anyhow::{anyhow, ensure, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use swbus_actor::{state::incoming::Incoming, state::outgoing::Outgoing, Actor, ActorMessage, Context, State};
use swbus_edge::SwbusEdgeRuntime;
use swss_common::{KeyOpFieldValues, KeyOperation, SubscriberStateTable};
use swss_common_bridge::consumer::spawn_consumer_bridge;

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

    is_local: bool,
}

impl DpuActor {
    pub fn new(key: String) -> Result<Self> {
        let actor = DpuActor {
            id: key,
            dpu: None,
            is_local: false,
        };
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
        let config_db = crate::db_named("CONFIG_DB").await?;
        let sst = SubscriberStateTable::new_async(config_db, Self::table_name(), None, None).await?;
        let addr = crate::sp("swss-common-bridge", Self::table_name());
        spawn_consumer_bridge(edge_runtime.clone(), addr, sst, |kfv: &KeyOpFieldValues| {
            (crate::sp(Self::name(), &kfv.key), Self::table_name().to_owned())
        });
        Ok(())
    }

    pub async fn init_supporting_services(edge_runtime: &Arc<SwbusEdgeRuntime>) -> Result<()> {
        spawn_consumer_bridge_for_actor(
            edge_runtime.clone(),
            "CHASSIS_STATE_DB",
            "DPU_STATE",
            Self::name(),
            None,
            false,
        )
        .await?;
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
        let dpu = self.dpu.as_ref().unwrap();
        if dpu.dpu_id == crate::get_slot_id(context.get_edge_runtime()) {
            if self.is_local == false {
                self.is_local = true;

                let zmq_endpoint = format!("{}:{}", dpu.midplane_ipv4, dpu.orchagent_zmq_port);

                // DASH_HA_GLOBAL_CONFIG from common-bridge sent to this actor instance only.
                // Key is DASH_HA_GLOBAL_CONFIG
                spawn_consumer_bridge_for_actor(
                    context.get_edge_runtime().clone(),
                    "CONFIG_DB",
                    "DASH_HA_GLOBAL_CONFIG",
                    Self::name(),
                    Some(&self.id),
                    true,
                )
                .await?;

                // REMOTE_DPU from common-bridge sent to this actor instance only.
                // Key is REMOTE_DPU|<remote_dpu_id>
                spawn_consumer_bridge_for_actor(
                    context.get_edge_runtime().clone(),
                    "CONFIG_DB",
                    "REMOTE_DPU",
                    Self::name(),
                    Some(&self.id),
                    false,
                )
                .await?;

                // DASH_BFD_PROBE_STATE from common-bridge sent to this actor instance only.
                // Key is DASH_BFD_PROBE_STATE
                spawn_consumer_bridge_for_actor(
                    context.get_edge_runtime().clone(),
                    "DPU_STATE_DB",
                    "DASH_BFD_PROBE_STATE",
                    Self::name(),
                    Some(&self.id),
                    true,
                )
                .await?;

                // BFD_SESSION_TABLE is managed by zmq producer bridge. This actor instance
                // has service path swss-common-bridge/BFD_SESSION_TABLE.
                spawn_zmq_producer_bridge(
                    context.get_edge_runtime().clone(),
                    "DPU_APPL_DB",
                    "BFD_SESSION_TABLE",
                    &zmq_endpoint,
                )
                .await?;
            }
        } else {
            self.is_local = false;
        }

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

    async fn create_bfd_session(
        &self,
        peer_ip: &str,
        global_cfg: &DashHaGlobalConfig,
        outgoing: &mut Outgoing,
    ) -> Result<()> {
        let bfd_session = BfdSessionTable {
            tx_interval: global_cfg.dpu_bfd_probe_interval_in_ms,
            rx_interval: global_cfg.dpu_bfd_probe_interval_in_ms,
            multiplier: global_cfg.dpu_bfd_probe_multiplier,
            multihop: true,
            local_addr: self.dpu.as_ref().unwrap().pa_ipv4.clone(),
            session_type: Some("passive".to_string()),
            shutdown: false,
        };

        let fv = swss_serde::to_field_values(&bfd_session)?;
        let kfv = KeyOpFieldValues {
            key: format!("default|default|{}", peer_ip),
            operation: KeyOperation::Set,
            field_values: fv,
        };

        let msg = ActorMessage::new(self.id.clone(), &kfv)?;
        outgoing.send(outgoing.from_my_sp("swss-common-bridge", "BFD_SESSION_TABLE"), msg);
        Ok(())
    }

    async fn handle_remote_dpu_message(&mut self, state: &mut State, key: &str) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();
        let dpu_kfv: KeyOpFieldValues = incoming.get(key)?.deserialize_data()?;

        let remote_dpu: RemoteDpu = swss_serde::from_field_values(&dpu_kfv.field_values)?;

        // create bfd session
        let kfv: KeyOpFieldValues = incoming.get("DASH_HA_GLOBAL_CONFIG")?.deserialize_data()?;
        let global_cfg: DashHaGlobalConfig = swss_serde::from_field_values(&kfv.field_values)?;
        self.create_bfd_session(&remote_dpu.npu_ipv4, &global_cfg, outgoing)
            .await?;
        Ok(())
    }

    async fn handle_dash_ha_global_config(
        &mut self,
        state: &mut State,
        key: &str,
        context: &mut Context,
    ) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();
        let kfv: KeyOpFieldValues = incoming.get(key)?.deserialize_data()?;
        let global_cfg: DashHaGlobalConfig = swss_serde::from_field_values(&kfv.field_values)?;

        let remote_dpu_msgs = incoming.get_by_prefix("REMOTE_DPU");
        let mut remote_npus: Vec<String> = remote_dpu_msgs
            .iter()
            .filter_map(|entry| {
                let Ok(kfv) = entry.msg.deserialize_data() as Result<KeyOpFieldValues, _> else {
                    return None;
                };
                let Ok(remote_dpu) = swss_serde::from_field_values(&kfv.field_values) as Result<RemoteDpu, _> else {
                    return None;
                };

                Some(remote_dpu.npu_ipv4.clone())
            })
            .collect::<HashSet<String>>()
            .into_iter()
            .collect();
        let my_npu_ipv4 = crate::get_npu_ipv4(context.get_edge_runtime())
            .ok_or_else(|| anyhow!("npu_ipv4 taken from Loopback0 must be available"))?
            .to_string();
        remote_npus.push(my_npu_ipv4);
        remote_npus.sort();
        for npu in remote_npus {
            self.create_bfd_session(&npu, &global_cfg, outgoing).await?;
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

        if key.starts_with("REMOTE_DPU") {
            return self.handle_remote_dpu_message(state, key).await;
        } else if key == "DASH_HA_GLOBAL_CONFIG" {
            return self.handle_dash_ha_global_config(state, key, context).await;
        } else if key == "DPU_STATE" || key == "DASH_BFD_PROBE_STATE" {
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
    use crate::RuntimeData;
    use std::sync::Arc;
    use std::{net::Ipv4Addr, time::Duration};
    use swbus_actor::ActorRuntime;

    #[tokio::test]
    async fn dpu_actor() {
        let mut edge = test::create_edge_runtime().await;

        let dpu_actor = DpuActor {
            id: "test-dpu".into(),
            dpu: None,
            is_local: true,
        };

        let runtime_data = RuntimeData::new(1, Some(Ipv4Addr::new(10, 0, 1, 0).into()), None);

        edge.set_runtime_env(Box::new(runtime_data));
        let runtime = ActorRuntime::new(Arc::new(edge));
        let handle = runtime.spawn(dpu_actor, "dpu", "test-dpu");

        #[rustfmt::skip]
        let commands = [
            // Bring up dpu
            send! { key: "DPU_STATE", data: { "key": "test-dpu", "operation": "Set", "field_values": { "dpu_midplane_link_state": "up", "dpu_control_plane_state": "up", "dpu_data_plane_state": "up" }} },
            // Receiving DPU config-db object from swss-common bridge
            send! { key: DpuActor::table_name(), data: { "key": DpuActor::table_name(), "operation": "Set", "field_values": {"pa_ipv4": "1.2.3.4", "dpu_id": "1", "orchagent_zmq_port": "8100", "swbus_port": "23606", "midplane_ipv4": "127.0.0.1"}}, addr: runtime.sp("swss-common-bridge", DpuActor::table_name()) },
            send! { key: "DPUHealthRegister-vdpu/test-vdpu", data: { "active": true}, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "REMOTE_DPU|remotedpu1", data: { "key": "REMOTE_DPU|remotedpu1", "operation": "Set", "field_values": { "npu_ipv4": "10.0.1.1", "dpu_id": "0" }}},
            send! { key: "REMOTE_DPU|remotedpu2", data: { "key": "REMOTE_DPU|remotedpu2", "operation": "Set", "field_values": { "npu_ipv4": "10.0.1.2", "dpu_id": "0" }}},
            send! { key: "DASH_HA_GLOBAL_CONFIG", data: { "key": "DASH_HA_GLOBAL_CONFIG", "operation": "Set", "field_values": { "dpu_bfd_probe_interval_in_ms": "1000", "dpu_bfd_probe_multiplier": "3" }} },
            recv! { key: "test-dpu", data: {"key": "default|default|10.0.1.0",  "operation": "Set", "field_values": {"tx_interval": "1000", "rx_interval": "1000", "multiplier":"3", "multihop":"true", "local_addr":"1.2.3.4", "type":"passive", "shutdown":"false"}}, addr: runtime.sp("swss-common-bridge", "BFD_SESSION_TABLE") },
            recv! { key: "test-dpu", data: {"key": "default|default|10.0.1.1",  "operation": "Set", "field_values": {"tx_interval": "1000", "rx_interval": "1000", "multiplier":"3", "multihop":"true", "local_addr":"1.2.3.4", "type":"passive", "shutdown":"false"}}, addr: runtime.sp("swss-common-bridge", "BFD_SESSION_TABLE") },
            recv! { key: "test-dpu", data: {"key": "default|default|10.0.1.2",  "operation": "Set", "field_values": {"tx_interval": "1000", "rx_interval": "1000", "multiplier":"3", "multihop":"true", "local_addr":"1.2.3.4", "type":"passive", "shutdown":"false"}}, addr: runtime.sp("swss-common-bridge", "BFD_SESSION_TABLE") },
            send! { key: "REMOTE_DPU|remotedpu3", data: { "key": "REMOTE_DPU|remotedpu3", "operation": "Set", "field_values": { "npu_ipv4": "10.0.1.3", "dpu_id": "0" }}},
            recv! { key: "test-dpu", data: {"key": "default|default|10.0.1.3",  "operation": "Set", "field_values": {"tx_interval": "1000", "rx_interval": "1000", "multiplier":"3", "multihop":"true", "local_addr":"1.2.3.4", "type":"passive", "shutdown":"false"}}, addr: runtime.sp("swss-common-bridge", "BFD_SESSION_TABLE") },

            send! { key: "DASH_BFD_PROBE_STATE", data: { "key": "", "operation": "Set", "field_values": { "v4_bfd_up_sessions": "1.2.3.4,1.2.3.5,1.2.3.6" }} },
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
            send! { key: "DASH_BFD_PROBE_STATE", data: { "key": "", "operation": "Set", "field_values": { "v4_bfd_up_sessions": "1.2.3.5,1.2.3.6" }} },
            recv! { key: "DPUHealthUpdate-test-dpu", data: { "up": false }, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "DASH_BFD_PROBE_STATE", data: { "key": "", "operation": "Set", "field_values": { "v4_bfd_up_sessions": "1.2.3.5,1.2.3.6,1.2.3.7" }} },
            recv! { key: "DPUHealthUpdate-test-dpu", data: { "up": false }, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "DASH_BFD_PROBE_STATE", data: { "key": "", "operation": "Set", "field_values": { "v4_bfd_up_sessions": "1.2.3.5,1.2.3.6,1.2.3.7,1.2.3.4" }} },
            recv! { key: "DPUHealthUpdate-test-dpu", data: { "up": true }, addr: runtime.sp("vdpu", "test-vdpu") },
            
            // simulate delete of Dpu entry
            send! { key: DpuActor::table_name(), data: { "key": DpuActor::table_name(), "operation": "Del", "field_values": {"pa_ipv4": "1.2.3.4", "dpu_id": "1", "orchagent_zmq_port": "8100", "swbus_port": "23606", "midplane_ipv4": "127.0.0.1"}}, addr: runtime.sp("swss-common-bridge", DpuActor::table_name()) },
        ];
        test::run_commands(&runtime, runtime.sp("dpu", "test-dpu"), &commands).await;
        if tokio::time::timeout(Duration::from_secs(1), handle).await.is_err() {
            panic!("timeout waiting for actor to terminate");
        }
    }
}
