use crate::actors::vdpu::VDpuActor;
use crate::actors::{ha_set, spawn_consumer_bridge_for_actor, ActorCreator, DbBasedActor};
use crate::db_structs::*;
use crate::ha_actor_messages::{ActorRegistration, RegistrationType, VDpuActorState};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use swbus_actor::state::internal::{self, Internal};
use swbus_actor::{state::incoming::Incoming, state::outgoing::Outgoing, Actor, ActorMessage, Context, State};
use swbus_edge::SwbusEdgeRuntime;
use swss_common::{KeyOpFieldValues, KeyOperation, SubscriberStateTable, ZmqClient, ZmqProducerStateTable};
use swss_common_bridge::{consumer::spawn_consumer_bridge, consumer::ConsumerBridge, producer::spawn_producer_bridge};
use tokio::time::error::Elapsed;
use tracing::{debug, error, info};

pub struct HaSetActor {
    id: String,
    dash_ha_set_config: Option<DashHaSetConfigTable>,
    bridges: Vec<ConsumerBridge>,
}

impl DbBasedActor for HaSetActor {
    fn new(key: String) -> Result<Self> {
        let actor = HaSetActor {
            id: key,
            dash_ha_set_config: None,
            bridges: Vec::new(),
        };
        Ok(actor)
    }

    fn table_name() -> &'static str {
        "DASH_HA_SET_CONFIG_TABLE"
    }

    fn name() -> &'static str {
        "ha-set"
    }
}

struct VDpuStateExt {
    vdpu: VDpuActorState,
    is_primary: bool,
}

impl HaSetActor {
    fn update_dash_ha_set_table(
        &self,
        vdpus: &Vec<VDpuStateExt>,
        incoming: &Incoming,
        outgoing: &mut Outgoing,
    ) -> Result<()> {
        let Some(dash_ha_set_config) = self.dash_ha_set_config.as_ref() else {
            return Ok(());
        };

        // only 2 vdpus are supported at the moment. Skip the rest.
        let (local_vdpu, remote_vdpu) = match (vdpus[0].vdpu.dpu.is_managed, vdpus[1].vdpu.dpu.is_managed) {
            (true, _) => (&vdpus[0].vdpu, &vdpus[1].vdpu),
            (false, true) => (&vdpus[1].vdpu, &vdpus[0].vdpu),
            (false, false) => {
                error!("Neither primary nor backup DPU are managed by local HAMGRD. Skip dash-ha-set update");
                return Ok(());
            }
        };
        let global_cfg: DashHaGlobalConfig = incoming.get("DASH_HA_GLOBAL_CONFIG")?.deserialize_data()?;

        let dash_ha_set = DashHaSetTable {
            version: dash_ha_set_config.version.clone(),
            vip_v4: dash_ha_set_config.vip_v4.clone(),
            vip_v6: dash_ha_set_config.vip_v6.clone(),
            owner: dash_ha_set_config.owner.clone(),
            scope: dash_ha_set_config.scope.clone(),
            local_npu_ip: Some(local_vdpu.dpu.npu_ipv4.clone()),
            local_ip: Some(local_vdpu.dpu.pa_ipv4.clone()),
            peer_ip: Some(remote_vdpu.dpu.pa_ipv4.clone()),
            cp_data_channel_port: global_cfg.cp_data_channel_port.clone(),
            dp_channel_dst_port: global_cfg.dp_channel_dst_port.clone(),
            dp_channel_src_port_min: global_cfg.dp_channel_src_port_min.clone(),
            dp_channel_src_port_max: global_cfg.dp_channel_src_port_max.clone(),
            dp_channel_probe_interval_ms: global_cfg.dp_channel_probe_interval_ms.clone(),
            dp_channel_probe_fail_threshold: global_cfg.dp_channel_probe_fail_threshold.clone(),
        };

        let fv = swss_serde::to_field_values(&dash_ha_set)?;
        let kfv = KeyOpFieldValues {
            key: self.id.clone(),
            operation: KeyOperation::Set,
            field_values: fv,
        };

        let msg = ActorMessage::new(self.id.clone(), &kfv)?;
        outgoing.send(outgoing.from_my_sp("swss-common-bridge", "DASH_HA_SET_TABLE"), msg);

        Ok(())
    }

    fn update_vnet_route_tunnel_table(
        &self,
        vdpus: &Vec<VDpuStateExt>,
        incoming: &Incoming,
        internal: &Internal,
    ) -> Result<()> {
        let global_cfg: DashHaGlobalConfig = incoming.get("DASH_HA_GLOBAL_CONFIG")?.deserialize_data()?;

        let mut endpoint = Vec::new();
        let mut endpoint_monitor = Vec::new();
        let mut primary = Vec::new();
        let mut check_directly_connected = false;

        for vdpu_ext in vdpus {
            endpoint.push(vdpu_ext.vdpu.dpu.npu_ipv4.clone());
            endpoint_monitor.push(vdpu_ext.vdpu.dpu.pa_ipv4.clone());
            primary.push(vdpu_ext.is_primary.to_string());
            check_directly_connected |= vdpu_ext.vdpu.dpu.is_managed;
        }

        // get DPU info in vdpu state update message
        let vnet_route = VnetRouteTunnelTable {
            endpoint,
            endpoint_monitor: Some(endpoint_monitor),
            monitoring: None,
            primary: Some(primary),
            rx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            tx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            check_directly_connected: Some(check_directly_connected),
        };
        // for each DPU, create an VNET_ROUTE_TUNNEL_TABLE entry
        Ok(())
    }

    async fn register_to_vdpu_actor(&self, outgoing: &mut Outgoing, active: bool) -> Result<()> {
        let Some(ref dash_ha_set_config) = self.dash_ha_set_config else {
            return Ok(());
        };

        let msg = ActorRegistration::new_actor_msg(active, RegistrationType::VDPUState, &self.id)?;
        dash_ha_set_config
            .vdpu_ids
            .iter()
            .map(|id: &String| id.trim())
            .filter(|s| !s.is_empty())
            .for_each(|id| {
                outgoing.send(outgoing.from_my_sp(VDpuActor::name(), id), msg.clone());
            });

        Ok(())
    }

    // get vdpu data received via vdpu udpate
    fn get_vdpu(&self, incoming: &Incoming, vdpu_id: &str) -> Option<VDpuActorState> {
        let key = VDpuActorState::msg_key(vdpu_id);
        let Ok(msg) = incoming.get(&key) else {
            return None;
        };
        msg.deserialize_data().ok()
    }

    /// Get vdpu data received via vdpu update and return them in a list with primary DPUs first.
    /// All preferred_vdpu_ids are considered primary, followed by backups.
    fn get_vdpus(&self, incoming: &Incoming) -> Vec<(Option<VDpuStateExt>)> {
        let Some(ref ha_set_cfg) = self.dash_ha_set_config else {
            return Vec::new();
        };

        let mut result = Vec::new();

        // Collect all preferred (primary) vdpus first
        let mut seen = std::collections::HashSet::new();
        for id in ha_set_cfg.preferred_vdpu_ids.iter().filter(|id| !id.is_empty()) {
            seen.insert(id);
            result.push(
                self.get_vdpu(incoming, id)
                    .map(|vdpu| VDpuStateExt { vdpu, is_primary: true }),
            );
        }

        // Then collect backups (those not in preferred_vdpu_ids)
        for id in ha_set_cfg
            .vdpu_ids
            .iter()
            .filter(|id| !id.is_empty() && !seen.contains(id))
        {
            result.push(self.get_vdpu(incoming, id).map(|vdpu| VDpuStateExt {
                vdpu,
                is_primary: false,
            }));
        }

        result
    }

    /// Returns a vector of `VDpuStateExt` if all VDPU states are available and at least one is managed by the local HAMGRD.
    /// If any VDPU state is missing or none are managed locally, returns `None`.
    /// This ensures that subsequent operations only proceed when all required DPU information is ready and relevant.
    /// returned vdpus are sorted by primary and backup, with primary first.
    fn get_vdpus_if_ready(&self, incoming: &Incoming) -> Option<Vec<VDpuStateExt>> {
        let vdpus = self.get_vdpus(incoming);
        if !vdpus.iter().all(|vdpu| vdpu.is_some()) {
            info!("Not all DPU info is ready yet");
            return None;
        }

        let vdpus = vdpus.into_iter().map(|vdpu| vdpu.unwrap()).collect::<Vec<_>>();
        if !vdpus.iter().any(|vdpu_ext| vdpu_ext.vdpu.dpu.is_managed) {
            debug!("None of DPUs is managed by local HAMGRD. Skip global config update");
            return None;
        }
        Some(vdpus)
    }

    async fn handle_dash_ha_set_config_table_message(
        &mut self,
        state: &mut State,
        key: &str,
        context: &mut Context,
    ) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();
        let dpu_kfv: KeyOpFieldValues = incoming.get(key)?.deserialize_data()?;
        if dpu_kfv.operation == KeyOperation::Del {
            // unregister from the DPU Actor
            self.register_to_vdpu_actor(outgoing, false).await?;

            context.stop();
            return Ok(());
        }

        if self.dash_ha_set_config.is_none() {
            self.bridges.push(
                spawn_consumer_bridge_for_actor(
                    context.get_edge_runtime().clone(),
                    "CONFIG_DB",
                    "DASH_HA_GLOBAL_CONFIG",
                    Self::name(),
                    Some(&self.id),
                    true,
                )
                .await?,
            );
        }

        self.dash_ha_set_config = Some(swss_serde::from_field_values(&dpu_kfv.field_values)?);

        // Subscribe to the DPU Actor for state updates.
        self.register_to_vdpu_actor(outgoing, true).await?;

        let Some(vdpus) = self.get_vdpus_if_ready(incoming) else {
            return Ok(());
        };

        self.update_dash_ha_set_table(&vdpus, incoming, outgoing)?;

        Ok(())
    }

    async fn handle_dash_ha_global_config(
        &mut self,
        state: &mut State,
        key: &str,
        context: &mut Context,
    ) -> Result<()> {
        let (internal, incoming, outgoing) = state.get_all();
        let Some(vdpus) = self.get_vdpus_if_ready(incoming) else {
            return Ok(());
        };
        // global config update affects Vxlan tunnel and dash-ha-set in DPU
        self.update_dash_ha_set_table(&vdpus, incoming, outgoing)?;
        self.update_vnet_route_tunnel_table(&vdpus, incoming, internal)?;
        Ok(())
    }

    async fn handle_vdpu_state_update(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        let (internal, incoming, outgoing) = state.get_all();
        // vdpu update affects dash-ha-set in DPU and vxlan tunnel
        let Some(vdpus) = self.get_vdpus_if_ready(incoming) else {
            return Ok(());
        };
        self.update_dash_ha_set_table(&vdpus, incoming, outgoing)?;
        self.update_vnet_route_tunnel_table(&vdpus, incoming, internal)?;
        Ok(())
    }
}

impl Actor for HaSetActor {
    async fn handle_message(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        if key == Self::table_name() {
            return self.handle_dash_ha_set_config_table_message(state, key, context).await;
        }

        if self.dash_ha_set_config.is_none() {
            return Ok(());
        }

        if VDpuActorState::is_my_msg(key) {
            return self.handle_vdpu_state_update(state, key, context).await;
        } else if key == "DASH_HA_GLOBAL_CONFIG" {
            return self.handle_dash_ha_global_config(state, key, context).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        actors::{
            ha_set::HaSetActor,
            test::{self, recv, send},
            DbBasedActor,
        },
        ha_actor_messages::*,
    };
    use std::time::Duration;

    #[tokio::test]
    async fn ha_set_actor() {
        let runtime = test::create_actor_runtime(1, "10.0.1.0").await;

        let ha_set_actor = HaSetActor {
            id: "test-ha-set".into(),
            dash_ha_set_config: None,
            bridges: Vec::new(),
        };

        let handle = runtime.spawn(ha_set_actor, HaSetActor::name(), "test-ha-set");

        // Example DASH_HA_SET_CONFIG_TABLE object
        let ha_set_config_obj = serde_json::json!({
            "version": "1",
            "vip_v4": "192.168.0.1",
            "vip_v6": "",
            "owner": "dpu",
            "scope": "dpu",
            "preferred_vdpu_ids": "vdpu1",
            "vdpu_ids": "vdpu0,vdpu1"
        });

        // Example VDPU state
        let vdpu0_state_obj = serde_json::json!({
            "up": true,
            "dpu": {
                "pa_ipv4": "1.2.1.0",
                "dpu_id": 1,
                "dpu_name": "switch1_dpu0",
                "is_managed": true,
                "orchagent_zmq_port": 8100,
                "swbus_port": 23606,
                "midplane_ipv4": "127.0.0.1",
                "remote_dpu": false,
                "npu_ipv4": "10.0.1.0",
            }
        });

        let vdpu1_state_obj = serde_json::json!({
            "up": true,
            "dpu": {
                "pa_ipv4": "1.2.1.0",
                "dpu_id": 1,
                "dpu_name": "switch2_dpu0",
                "is_managed": false,
                "orchagent_zmq_port": 8100,
                "swbus_port": 23606,
                "midplane_ipv4": "127.0.0.1",
                "remote_dpu": false,
                "npu_ipv4": "10.0.1.1",
            }
        });
        #[rustfmt::skip]
        let commands = [
            // Send DASH_HA_SET_CONFIG_TABLE config
            send! { key: HaSetActor::table_name(), data: { "key": HaSetActor::table_name(), "operation": "Set", "field_values": ha_set_config_obj }, addr: runtime.sp("swss-common-bridge", HaSetActor::table_name()) },

            // Simulate VDPU state update for vdpu0
            send! { key: VDpuActorState::msg_key("vdpu0"), data: vdpu0_state_obj, addr: runtime.sp("vdpu", "vdpu0") },

            // Simulate VDPU state update for vdpu1 (backup)
            send! { key: VDpuActorState::msg_key("vdpu1"), data: vdpu1_state_obj, addr: runtime.sp("vdpu", "vdpu1") },

            // Optionally, check for outgoing messages or state changes as needed
        ];

        test::run_commands(&runtime, runtime.sp(HaSetActor::name(), "test-ha-set"), &commands).await;
        if tokio::time::timeout(Duration::from_secs(1), handle).await.is_err() {
            panic!("timeout waiting for actor to terminate");
        }
    }
}
