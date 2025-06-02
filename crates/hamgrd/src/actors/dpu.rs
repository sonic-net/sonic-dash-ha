use crate::actors::{spawn_consumer_bridge_for_actor, spawn_zmq_producer_bridge, ActorCreator};
use crate::db_structs::*;
use crate::ha_actor_messages::{ActorRegistration, DpuActorState, RegistrationType};
use crate::ServicePath;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_with::{formats::CommaSeparator, serde_as, StringWithSeparator};
use std::collections::HashSet;
use std::sync::Arc;
use swbus_actor::{state::incoming::Incoming, state::outgoing::Outgoing, Actor, ActorMessage, Context, State};
use swbus_edge::SwbusEdgeRuntime;
use swss_common::{KeyOpFieldValues, KeyOperation, SubscriberStateTable};
use swss_common_bridge::consumer::{spawn_consumer_bridge, ConsumerBridge};
use tracing::{debug, error, info};

use super::spawn_consumer_bridge_for_actor_with_selector;
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
#[serde_as]
#[derive(Serialize, Deserialize)]
struct DashBfdProbeState {
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    v4_bfd_up_sessions: Vec<String>,
}

pub enum DpuData {
    LocalDpu {
        dpu: Dpu,
        is_managed: bool,
        npu_ipv4: String,
        npu_ipv6: Option<String>,
    },
    RemoteDpu(RemoteDpu),
}

pub struct DpuActor {
    /// The id of this dpu
    id: String,

    /// Data from CONFIG_DB
    dpu: Option<DpuData>,

    bridges: Vec<ConsumerBridge>,
}

impl DpuActor {
    fn new(key: String) -> Result<Self> {
        let actor = DpuActor {
            id: key,
            dpu: None,
            bridges: Vec::new(),
        };
        Ok(actor)
    }

    pub fn name() -> &'static str {
        "dpu"
    }

    // returns the main Db table name that this actor is associated to
    fn dpu_table_name() -> &'static str {
        "DPU"
    }

    fn remote_dpu_table_name() -> &'static str {
        "REMOTE_DPU"
    }

    // DpuActor is spawned in response to swss-common-bridge message for DPU and REMOTE_DPU table
    pub async fn start_actor_creator(edge_runtime: Arc<SwbusEdgeRuntime>) -> Result<()> {
        let dpu_ac = ActorCreator::new(
            edge_runtime.new_sp(Self::name(), ""),
            edge_runtime.clone(),
            false,
            |key: String| -> Result<Self> { Self::new(key) },
        );

        tokio::task::spawn(dpu_ac.run());

        // dpu actor is spawned for both local dpu and remote dpu
        let config_db = crate::db_named("CONFIG_DB").await?;
        let sst = SubscriberStateTable::new_async(config_db, Self::dpu_table_name(), None, None).await?;
        let addr = crate::sp("swss-common-bridge", Self::dpu_table_name());
        spawn_consumer_bridge(
            edge_runtime.clone(),
            addr,
            sst,
            |kfv: &KeyOpFieldValues| (crate::sp(Self::name(), &kfv.key), Self::dpu_table_name().to_owned()),
            |_| true,
        );

        let config_db = crate::db_named("CONFIG_DB").await?;
        let sst = SubscriberStateTable::new_async(config_db, Self::remote_dpu_table_name(), None, None).await?;
        let addr = crate::sp("swss-common-bridge", Self::remote_dpu_table_name());
        spawn_consumer_bridge(
            edge_runtime.clone(),
            addr,
            sst,
            |kfv: &KeyOpFieldValues| {
                (
                    crate::sp(Self::name(), &kfv.key),
                    Self::remote_dpu_table_name().to_owned(),
                )
            },
            |_| true,
        );
        Ok(())
    }

    async fn handle_dpu_message(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();
        let dpu_kfv: KeyOpFieldValues = incoming.get(key)?.deserialize_data()?;
        if dpu_kfv.operation == KeyOperation::Del {
            context.stop();
            return Ok(());
        }

        let dpu: Dpu = swss_serde::from_field_values(&dpu_kfv.field_values)?;
        let npu_ipv4: String = crate::get_npu_ipv4(context.get_edge_runtime())
            .ok_or_else(|| anyhow!("npu_ipv4 taken from Loopback0 must be available"))?
            .to_string();
        let npu_ipv6: Option<String> = crate::get_npu_ipv6(context.get_edge_runtime()).map(|ip| ip.to_string());
        let dpu_id = dpu.dpu_id;
        let is_managed = dpu.dpu_id == crate::get_slot_id(context.get_edge_runtime());
        let zmq_endpoint = format!("{}:{}", dpu.midplane_ipv4, dpu.orchagent_zmq_port);

        let first_time = self.dpu.is_none();
        self.dpu = Some(DpuData::LocalDpu {
            dpu,
            is_managed,
            npu_ipv4,
            npu_ipv6,
        });

        if is_managed {
            if first_time {
                // DASH_HA_GLOBAL_CONFIG from common-bridge sent to this actor instance only.
                // Key is DASH_HA_GLOBAL_CONFIG
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

                // REMOTE_DPU from common-bridge sent to this actor instance only.
                // Key is REMOTE_DPU|<remote_dpu_id>
                self.bridges.push(
                    spawn_consumer_bridge_for_actor(
                        context.get_edge_runtime().clone(),
                        "CONFIG_DB",
                        "REMOTE_DPU",
                        Self::name(),
                        Some(&self.id),
                        false,
                    )
                    .await?,
                );

                // DASH_BFD_PROBE_STATE from common-bridge sent to this actor instance only.
                // Key is DASH_BFD_PROBE_STATE
                self.bridges.push(
                    spawn_consumer_bridge_for_actor(
                        context.get_edge_runtime().clone(),
                        "DPU_STATE_DB",
                        "DASH_BFD_PROBE_STATE",
                        Self::name(),
                        Some(&self.id),
                        true,
                    )
                    .await?,
                );

                // DPU_STATE from common-bridge sent to this actor instance only. The selector closure is
                // used to filter out the DPU_STATE for this DPU instance only.
                self.bridges.push(
                    spawn_consumer_bridge_for_actor_with_selector(
                        context.get_edge_runtime().clone(),
                        "CHASSIS_STATE_DB",
                        "DPU_STATE",
                        Self::name(),
                        Some(&self.id),
                        true, /* key will be DPU_STATE only */
                        move |kfv: &KeyOpFieldValues| {
                            let dev = &kfv.key[3..];
                            if let Ok(slot) = dev.parse::<u32>() {
                                return slot == dpu_id;
                            }
                            false
                        },
                    )
                    .await?,
                );

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
            debug!(
                "DPU {} is not local. local DPU slot is {}",
                self.id,
                crate::get_slot_id(context.get_edge_runtime())
            );
        }

        // notify dependent actors about the state of this DPU
        self.update_dpu_state(incoming, outgoing, None).await?;
        Ok(())
    }

    fn calculate_dpu_state(&self, incoming: &Incoming) -> Result<bool> {
        if let Some(DpuData::RemoteDpu(_)) = self.dpu {
            // we don't care remote dpu
            return Ok(false);
        }
        // Check pmon state from DPU_STATE table
        let dpu_state_kfv: KeyOpFieldValues = incoming.get("DPU_STATE")?.deserialize_data()?;
        let dpu_state: DpuState = swss_serde::from_field_values(&dpu_state_kfv.field_values)?;
        let pmon_dpu_up = dpu_state.all_up();

        // Check bfd state from DASH_BFD_PROBE_STATE
        let bfd_probe_kfv: KeyOpFieldValues = incoming.get("DASH_BFD_PROBE_STATE")?.deserialize_data()?;
        let bfd_probe: DashBfdProbeState = swss_serde::from_field_values(&bfd_probe_kfv.field_values)?;

        // bfd is considered up if there is at least one v4 session up to any peer
        let bfd_dpu_up = !bfd_probe.v4_bfd_up_sessions.is_empty();

        Ok(pmon_dpu_up && bfd_dpu_up)
    }

    // target_actor is the actor that needs to be notified about the DPU state. If None, all
    async fn update_dpu_state(
        &mut self,
        incoming: &Incoming,
        outgoing: &mut Outgoing,
        target_actor: Option<ServicePath>,
    ) -> Result<()> {
        let mut up = false;
        let Some(ref dpu_data) = self.dpu else {
            return Ok(());
        };

        let result = self.calculate_dpu_state(incoming);
        match result {
            Ok(state) => {
                up = state;
            }
            Err(e) => {
                info!(
                    "Not able to calculate DPU state for {}. Assume to be down. Error: {} ",
                    self.id, e
                );
            }
        }

        let mut dpu_state = match dpu_data {
            DpuData::LocalDpu {
                dpu,
                is_managed,
                npu_ipv4,
                npu_ipv6,
            } => DpuActorState::from_dpu(&self.id, dpu, *is_managed, npu_ipv4, npu_ipv6),
            DpuData::RemoteDpu(rdpu) => DpuActorState::from_remote_dpu(&self.id, rdpu),
        };
        dpu_state.up = up;
        let msg = DpuActorState::new_actor_msg(&self.id, &dpu_state)?;

        if let Some(target_actor_sp) = target_actor {
            outgoing.send(target_actor_sp, msg);
            return Ok(());
        } else {
            // no target is specified. Send the message to all registered actors
            let peer_actors = ActorRegistration::get_registered_actors(incoming, RegistrationType::DPUState);
            for actor_sp in peer_actors {
                outgoing.send(actor_sp, msg.clone());
            }
        }
        Ok(())
    }

    // handle DPU state registration request. In response to the request, this actor will send its current state.
    async fn handle_dpu_state_registration(
        &mut self,
        key: &str,
        incoming: &Incoming,
        outgoing: &mut Outgoing,
    ) -> Result<()> {
        let entry = incoming.get_entry(key)?;
        let ActorRegistration { active, .. } = entry.msg.deserialize_data()?;
        if active {
            self.update_dpu_state(incoming, outgoing, Some(entry.source.clone()))
                .await?;
        }
        Ok(())
    }

    async fn create_bfd_session(
        &self,
        peer_ip: &str,
        global_cfg: &DashHaGlobalConfig,
        outgoing: &mut Outgoing,
    ) -> Result<()> {
        // todo: this needs to wait until HaScope has been activated.
        let Some(DpuData::LocalDpu { ref dpu, .. }) = self.dpu else {
            info!("DPU is not managed by this HA instance. Ignore BFD session creation");
            return Ok(());
        };
        let bfd_session = BfdSessionTable {
            tx_interval: global_cfg.dpu_bfd_probe_interval_in_ms,
            rx_interval: global_cfg.dpu_bfd_probe_interval_in_ms,
            multiplier: global_cfg.dpu_bfd_probe_multiplier,
            multihop: true,
            local_addr: dpu.pa_ipv4.clone(),
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

    async fn handle_remote_dpu_message_to_remote_dpu(
        &mut self,
        state: &mut State,
        key: &str,
        context: &mut Context,
    ) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();
        let dpu_kfv: KeyOpFieldValues = incoming.get(key)?.deserialize_data()?;
        if dpu_kfv.operation == KeyOperation::Del {
            context.stop();
            return Ok(());
        }

        let rdpu: RemoteDpu = swss_serde::from_field_values(&dpu_kfv.field_values)?;
        self.dpu = Some(DpuData::RemoteDpu(rdpu));
        // notify dependent actors about the state of this DPU
        self.update_dpu_state(incoming, outgoing, None).await?;
        Ok(())
    }

    async fn handle_remote_dpu_message_to_local_dpu(&mut self, state: &mut State, key: &str) -> Result<()> {
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

    async fn handle_remote_dpu_message(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        if self.dpu.is_none() || matches!(self.dpu.as_ref().unwrap(), DpuData::RemoteDpu(_)) {
            self.handle_remote_dpu_message_to_remote_dpu(state, key, context).await
        } else {
            self.handle_remote_dpu_message_to_local_dpu(state, key).await
        }
    }

    async fn handle_dash_ha_global_config(&mut self, state: &mut State, key: &str) -> Result<()> {
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

        let Some(DpuData::LocalDpu { ref npu_ipv4, .. }) = self.dpu else {
            return Ok(());
        };
        remote_npus.push(npu_ipv4.clone());
        remote_npus.sort();
        for npu in remote_npus {
            self.create_bfd_session(&npu, &global_cfg, outgoing).await?;
        }
        Ok(())
    }

    fn is_local_managed(&self) -> bool {
        if let Some(DpuData::LocalDpu { is_managed, .. }) = self.dpu {
            return is_managed;
        }
        false
    }
}

impl Actor for DpuActor {
    async fn handle_message(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();
        if key == Self::dpu_table_name() {
            return self.handle_dpu_message(state, key, context).await;
        } else if key.starts_with(Self::remote_dpu_table_name()) {
            return self.handle_remote_dpu_message(state, key, context).await;
        }

        if self.dpu.is_none() {
            return Ok(());
        }

        if ActorRegistration::is_my_msg(key, RegistrationType::DPUState) {
            return self.handle_dpu_state_registration(key, incoming, outgoing).await;
        }

        // the rest of the messages are only for locally managed dpu
        if !self.is_local_managed() {
            return Ok(());
        } else if key == "DASH_HA_GLOBAL_CONFIG" {
            return self.handle_dash_ha_global_config(state, key).await;
        } else if key == "DPU_STATE" || key == "DASH_BFD_PROBE_STATE" {
            return self.update_dpu_state(incoming, outgoing, None).await;
        } else {
            error!("Unknown message received: {}", key);
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
    use crate::db_structs::RemoteDpu;
    use crate::ha_actor_messages::DpuActorState;
    use std::time::Duration;

    #[tokio::test]
    async fn dpu_actor() {
        let runtime = test::create_actor_runtime(1, "10.0.1.0").await;

        let dpu_actor = DpuActor {
            id: "test-dpu".into(),
            dpu: None,
            bridges: Vec::new(),
        };

        let handle = runtime.spawn(dpu_actor, "dpu", "test-dpu");
        let dpu_obj = serde_json::json!({"pa_ipv4": "1.2.3.4",
            "dpu_id": 1,
            "dpu_name":"test-dpu",
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
        let mut up_dpu_obj = dpu_obj;
        up_dpu_obj
            .as_object_mut()
            .unwrap()
            .insert("up".to_string(), serde_json::json!(true));
        #[rustfmt::skip]
        let commands = [
            // Bring up dpu
            send! { key: "DPU_STATE", data: { "key": "DPU1", "operation": "Set", "field_values": { "dpu_midplane_link_state": "up", "dpu_control_plane_state": "up", "dpu_data_plane_state": "up" }} },
            // Receiving DPU config-db object from swss-common bridge
            send! { key: DpuActor::dpu_table_name(), data: { "key": "test-dpu", "operation": "Set", "field_values": {"pa_ipv4": "1.2.3.4", "dpu_id": "1", "orchagent_zmq_port": "8100", "swbus_port": "23606", "midplane_ipv4": "127.0.0.1"}}, addr: runtime.sp("swss-common-bridge", DpuActor::dpu_table_name()) },
            send! { key: "DPUStateRegister|vdpu/test-vdpu", data: { "active": true}, addr: runtime.sp("vdpu", "test-vdpu") },
            recv! { key: "DPUStateUpdate|test-dpu", data: down_dpu_obj, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "REMOTE_DPU|remotedpu1", data: { "key": "REMOTE_DPU|remotedpu1", "operation": "Set", "field_values": { "pa_ipv4": "1.2.3.5", "npu_ipv4": "10.0.1.1", "dpu_id": "0", "swbus_port": "23606" }}},
            send! { key: "REMOTE_DPU|remotedpu2", data: { "key": "REMOTE_DPU|remotedpu2", "operation": "Set", "field_values": { "pa_ipv4": "1.2.3.6", "npu_ipv4": "10.0.1.2", "dpu_id": "0", "swbus_port": "23606" }}},
            send! { key: "DASH_HA_GLOBAL_CONFIG", data: { "key": "DASH_HA_GLOBAL_CONFIG", "operation": "Set", "field_values": { "dpu_bfd_probe_interval_in_ms": "1000", "dpu_bfd_probe_multiplier": "3" }} },
            recv! { key: "test-dpu", data: {"key": "default|default|10.0.1.0",  "operation": "Set", "field_values": {"tx_interval": "1000", "rx_interval": "1000", "multiplier":"3", "multihop":"true", "local_addr":"1.2.3.4", "type":"passive", "shutdown":"false"}}, addr: runtime.sp("swss-common-bridge", "BFD_SESSION_TABLE") },
            recv! { key: "test-dpu", data: {"key": "default|default|10.0.1.1",  "operation": "Set", "field_values": {"tx_interval": "1000", "rx_interval": "1000", "multiplier":"3", "multihop":"true", "local_addr":"1.2.3.4", "type":"passive", "shutdown":"false"}}, addr: runtime.sp("swss-common-bridge", "BFD_SESSION_TABLE") },
            recv! { key: "test-dpu", data: {"key": "default|default|10.0.1.2",  "operation": "Set", "field_values": {"tx_interval": "1000", "rx_interval": "1000", "multiplier":"3", "multihop":"true", "local_addr":"1.2.3.4", "type":"passive", "shutdown":"false"}}, addr: runtime.sp("swss-common-bridge", "BFD_SESSION_TABLE") },
            send! { key: "REMOTE_DPU|remotedpu3", data: { "key": "REMOTE_DPU|remotedpu3", "operation": "Set", "field_values": { "pa_ipv4": "1.2.3.7", "npu_ipv4": "10.0.1.3", "dpu_id": "0", "swbus_port": "23606" }}},
            recv! { key: "test-dpu", data: {"key": "default|default|10.0.1.3",  "operation": "Set", "field_values": {"tx_interval": "1000", "rx_interval": "1000", "multiplier":"3", "multihop":"true", "local_addr":"1.2.3.4", "type":"passive", "shutdown":"false"}}, addr: runtime.sp("swss-common-bridge", "BFD_SESSION_TABLE") },

            send! { key: "DASH_BFD_PROBE_STATE", data: { "key": "", "operation": "Set", "field_values": { "v4_bfd_up_sessions": "10.0.1.0,10.0.1.1,10.0.1.2" }} },
            recv! { key: "DPUStateUpdate|test-dpu", data: up_dpu_obj, addr: runtime.sp("vdpu", "test-vdpu") },

            // Simulate DPU_STATE planes going down then up
            send! { key: "DPU_STATE", data: { "key": "DPU1", "operation": "Set", "field_values": { "dpu_midplane_link_state": "down", "dpu_control_plane_state": "up", "dpu_data_plane_state": "up" }} },
            recv! { key: "DPUStateUpdate|test-dpu", data: down_dpu_obj, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "DPU_STATE", data: { "key": "DPU1", "operation": "Set", "field_values": { "dpu_midplane_link_state": "down", "dpu_control_plane_state": "up", "dpu_data_plane_state": "down" }} },
            recv! { key: "DPUStateUpdate|test-dpu", data: down_dpu_obj, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "DPU_STATE", data: { "key": "DPU1", "operation": "Set", "field_values": { "dpu_midplane_link_state": "down", "dpu_control_plane_state": "up", "dpu_data_plane_state": "up" }} },
            recv! { key: "DPUStateUpdate|test-dpu", data: down_dpu_obj, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "DPU_STATE", data: { "key": "DPU1", "operation": "Set", "field_values": { "dpu_midplane_link_state": "up", "dpu_control_plane_state": "down", "dpu_data_plane_state": "up" }} },
            recv! { key: "DPUStateUpdate|test-dpu", data: down_dpu_obj, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "DPU_STATE", data: { "key": "DPU1", "operation": "Set", "field_values": { "dpu_midplane_link_state": "up", "dpu_control_plane_state": "up", "dpu_data_plane_state": "up" }} },
            recv! { key: "DPUStateUpdate|test-dpu", data: up_dpu_obj, addr: runtime.sp("vdpu", "test-vdpu") },

            // Simulate BFD probe going down
            send! { key: "DASH_BFD_PROBE_STATE", data: { "key": "", "operation": "Set", "field_values": { "v4_bfd_up_sessions": "" }} },
            recv! { key: "DPUStateUpdate|test-dpu", data: down_dpu_obj, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "DASH_BFD_PROBE_STATE", data: { "key": "", "operation": "Set", "field_values": { "v4_bfd_up_sessions": "10.0.1.0,10.0.1.1,10.0.1.2" }} },
            recv! { key: "DPUStateUpdate|test-dpu", data: up_dpu_obj, addr: runtime.sp("vdpu", "test-vdpu") },
            
            // simulate delete of Dpu entry
            send! { key: DpuActor::dpu_table_name(), data: { "key": DpuActor::dpu_table_name(), "operation": "Del", "field_values": {"pa_ipv4": "1.2.3.4", "dpu_id": "1", "orchagent_zmq_port": "8100", "swbus_port": "23606", "midplane_ipv4": "127.0.0.1"}}, addr: runtime.sp("swss-common-bridge", DpuActor::dpu_table_name()) },
        ];
        test::run_commands(&runtime, runtime.sp("dpu", "test-dpu"), &commands).await;
        if tokio::time::timeout(Duration::from_secs(1), handle).await.is_err() {
            panic!("timeout waiting for actor to terminate");
        }
    }

    #[tokio::test]
    async fn remote_dpu_actor() {
        let runtime = test::create_actor_runtime(1, "10.0.1.0").await;

        let rdpu_actor = DpuActor {
            id: "test-rdpu".into(),
            dpu: None,
            bridges: Vec::new(),
        };

        let handle = runtime.spawn(rdpu_actor, "dpu", "test-rdpu");

        let rdpu = RemoteDpu {
            dpu_id: 1,
            pa_ipv4: "1.2.3.4".into(),
            pa_ipv6: None,
            npu_ipv4: "10.0.1.0".into(),
            npu_ipv6: None,
            swbus_port: 23606,
        };
        let fvs = swss_serde::to_field_values(&rdpu).unwrap();
        let rdpu_obj = serde_json::to_value(fvs).unwrap();
        let rdpu_state = DpuActorState::from_remote_dpu("test-rdpu", &rdpu);

        #[rustfmt::skip]
        let commands = [
            // Receiving DPU config-db object from swss-common bridge
            send! { key: DpuActor::remote_dpu_table_name(), data: { "key": "test-rdpu", "operation": "Set", "field_values": rdpu_obj}, addr: runtime.sp("swss-common-bridge", DpuActor::remote_dpu_table_name()) },
            send! { key: "DPUStateRegister|vdpu/test-vdpu", data: { "active": true}, addr: runtime.sp("vdpu", "test-vdpu") },
            recv! { key: "DPUStateUpdate|test-rdpu", data: rdpu_state, addr: runtime.sp("vdpu", "test-vdpu") },

            // simulate delete of Dpu entry
            send! { key: DpuActor::remote_dpu_table_name(), data: { "key": DpuActor::remote_dpu_table_name(), "operation": "Del", "field_values": rdpu_obj}, addr: runtime.sp("swss-common-bridge", DpuActor::remote_dpu_table_name()) },
        ];
        test::run_commands(&runtime, runtime.sp("dpu", "test-rdpu"), &commands).await;
        if tokio::time::timeout(Duration::from_secs(1), handle).await.is_err() {
            panic!("timeout waiting for actor to terminate");
        }
    }
}
