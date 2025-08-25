use crate::actors::{spawn_consumer_bridge_for_actor, ActorCreator};
use crate::db_structs::{
    BfdSessionTable, DashBfdProbeState, DashHaGlobalConfig, Dpu, DpuPmonStateType, DpuState, RemoteDpu,
};
use crate::ha_actor_messages::{ActorRegistration, DpuActorState, RegistrationType};
use crate::ServicePath;
use anyhow::{anyhow, Result};
use sonic_common::SonicDbTable;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use swbus_actor::{state::incoming::Incoming, state::outgoing::Outgoing, Actor, ActorMessage, Context, State};
use swbus_edge::SwbusEdgeRuntime;
use swss_common::{KeyOpFieldValues, KeyOperation, SubscriberStateTable};
use swss_common_bridge::consumer::ConsumerBridge;
use tracing::{debug, error, info, instrument};

use super::spawn_consumer_bridge_for_actor_with_selector;

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

    /// Consumer bridges
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
        Dpu::table_name()
    }

    fn remote_dpu_table_name() -> &'static str {
        RemoteDpu::table_name()
    }

    fn get_dpu_state(incoming: &Incoming) -> Result<DpuState> {
        let dpu_state_kfv: KeyOpFieldValues = incoming.get(DpuState::table_name())?.deserialize_data()?;
        Ok(swss_serde::from_field_values(&dpu_state_kfv.field_values)?)
    }

    fn get_bfd_probe_state(incoming: &Incoming) -> Result<DashBfdProbeState> {
        let bfd_probe_kfv: KeyOpFieldValues = incoming.get(DashBfdProbeState::table_name())?.deserialize_data()?;
        Ok(swss_serde::from_field_values(&bfd_probe_kfv.field_values)?)
    }

    fn get_dash_ha_global_config(incoming: &Incoming) -> Result<DashHaGlobalConfig> {
        let ha_global_config_kfv: KeyOpFieldValues =
            incoming.get(DashHaGlobalConfig::table_name())?.deserialize_data()?;
        Ok(swss_serde::from_field_values(&ha_global_config_kfv.field_values)?)
    }

    // DpuActor is spawned in response to swss-common-bridge message for DPU and REMOTE_DPU table
    pub async fn start_actor_creator(edge_runtime: Arc<SwbusEdgeRuntime>) -> Result<Vec<ConsumerBridge>> {
        let mut bridges = Vec::new();
        let dpu_ac = ActorCreator::new(
            edge_runtime.new_sp(Self::name(), ""),
            edge_runtime.clone(),
            false,
            |key: String| -> Result<Self> { Self::new(key) },
        );

        tokio::task::spawn(dpu_ac.run());

        // dpu actor is spawned for both local dpu and remote dpu
        let config_db = crate::db_for_table::<Dpu>().await?;
        let sst = SubscriberStateTable::new_async(config_db, Self::dpu_table_name(), None, None).await?;
        let addr = crate::common_bridge_sp::<Dpu>(&edge_runtime);
        let base_addr = edge_runtime.get_base_sp();
        bridges.push(ConsumerBridge::spawn::<Dpu, _, _, _>(
            edge_runtime.clone(),
            addr,
            sst,
            move |kfv: &KeyOpFieldValues| {
                let mut addr = base_addr.clone();
                addr.resource_type = Self::name().to_owned();
                addr.resource_id = kfv.key.clone();
                (addr, Self::dpu_table_name().to_owned())
            },
            |_| true,
        ));

        let config_db = crate::db_for_table::<RemoteDpu>().await?;
        let sst = SubscriberStateTable::new_async(config_db, Self::remote_dpu_table_name(), None, None).await?;
        let addr = crate::common_bridge_sp::<RemoteDpu>(&edge_runtime);
        let base_addr = edge_runtime.get_base_sp();
        bridges.push(ConsumerBridge::spawn::<RemoteDpu, _, _, _>(
            edge_runtime.clone(),
            addr,
            sst,
            move |kfv: &KeyOpFieldValues| {
                let mut addr = base_addr.clone();
                addr.resource_type = Self::name().to_owned();
                addr.resource_id = kfv.key.clone();
                (addr, Self::remote_dpu_table_name().to_owned())
            },
            |_| true,
        ));
        Ok(bridges)
    }

    fn do_cleanup(&mut self, _context: &mut Context, state: &mut State) {
        if let Err(e) = self.update_bfd_sessions(state, true) {
            error!("Failed to cleanup BFD sessions: {}", e);
        }
    }

    async fn handle_dpu_message(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();
        let dpu_kfv: KeyOpFieldValues = incoming.get(key)?.deserialize_data()?;
        if dpu_kfv.operation == KeyOperation::Del {
            self.do_cleanup(context, state);
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
                    spawn_consumer_bridge_for_actor::<DashHaGlobalConfig>(
                        context.get_edge_runtime().clone(),
                        Self::name(),
                        Some(&self.id),
                        true,
                    )
                    .await?,
                );

                // REMOTE_DPU from common-bridge sent to this actor instance only.
                // Key is REMOTE_DPU|<remote_dpu_id>
                self.bridges.push(
                    spawn_consumer_bridge_for_actor::<RemoteDpu>(
                        context.get_edge_runtime().clone(),
                        Self::name(),
                        Some(&self.id),
                        false,
                    )
                    .await?,
                );

                // DASH_BFD_PROBE_STATE from common-bridge sent to this actor instance only.
                // Key is DASH_BFD_PROBE_STATE
                self.bridges.push(
                    spawn_consumer_bridge_for_actor::<DashBfdProbeState>(
                        context.get_edge_runtime().clone(),
                        Self::name(),
                        Some(&self.id),
                        true,
                    )
                    .await?,
                );

                // DPU_STATE from common-bridge sent to this actor instance only. The selector closure is
                // used to filter out the DPU_STATE for this DPU instance only.
                self.bridges.push(
                    spawn_consumer_bridge_for_actor_with_selector::<DpuState, _>(
                        context.get_edge_runtime().clone(),
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
            }
        } else {
            debug!(
                "DPU {} is not local. local DPU slot is {}",
                self.id,
                crate::get_slot_id(context.get_edge_runtime())
            );
        }

        // notify dependent actors about the state of this DPU
        self.update_dpu_state(incoming, outgoing, None)?;
        Ok(())
    }

    fn calculate_dpu_state(&self, incoming: &Incoming) -> (bool, Option<DpuState>, Option<DashBfdProbeState>) {
        if let Some(DpuData::RemoteDpu(_)) = self.dpu {
            // we don't care remote dpu
            return (false, None, None);
        }
        // Check pmon state from DPU_STATE table
        let dpu_state = match Self::get_dpu_state(incoming) {
            Ok(dpu_state) => Some(dpu_state),
            Err(e) => {
                info!("Not able to get DPU state. Assume DPU is down. Error: {}", e);
                None
            }
        };
        let bfd_probe_state = match Self::get_bfd_probe_state(incoming) {
            Ok(bfd_probe_state) => Some(bfd_probe_state),
            Err(e) => {
                debug!("Not able to get BFD probe state. Error: {}", e);
                None
            }
        };
        let final_state = match (&dpu_state, &bfd_probe_state) {
            (Some(dpu_state), Some(bfd_probe_state)) => {
                let pmon_dpu_up = dpu_state.dpu_midplane_link_state == DpuPmonStateType::Up
                    && dpu_state.dpu_control_plane_state == DpuPmonStateType::Up
                    && dpu_state.dpu_data_plane_state == DpuPmonStateType::Up;
                // bfd is considered up if there is at least one v4 session up to any peer
                let bfd_dpu_up =
                    !bfd_probe_state.v4_bfd_up_sessions.is_empty() || !bfd_probe_state.v6_bfd_up_sessions.is_empty();
                pmon_dpu_up && bfd_dpu_up
            }
            _ => false,
        };

        (final_state, dpu_state, bfd_probe_state)
    }

    // target_actor is the actor that needs to be notified about the DPU state. If None, all
    fn update_dpu_state(
        &mut self,
        incoming: &Incoming,
        outgoing: &mut Outgoing,
        target_actor: Option<ServicePath>,
    ) -> Result<()> {
        let Some(ref dpu_data) = self.dpu else {
            return Ok(());
        };

        let (up, dpu_state, bfd_probe_state) = self.calculate_dpu_state(incoming);

        let mut dpu_state = match dpu_data {
            DpuData::LocalDpu {
                dpu,
                is_managed,
                npu_ipv4,
                npu_ipv6,
            } => DpuActorState::from_dpu(
                &self.id,
                dpu,
                *is_managed,
                npu_ipv4,
                npu_ipv6,
                dpu_state,
                bfd_probe_state,
            ),
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
    fn handle_dpu_state_registration(&mut self, key: &str, incoming: &Incoming, outgoing: &mut Outgoing) -> Result<()> {
        let entry = incoming.get_entry(key)?;
        let ActorRegistration { active, .. } = entry.msg.deserialize_data()?;
        if active {
            self.update_dpu_state(incoming, outgoing, Some(entry.source.clone()))?;
        }
        Ok(())
    }

    fn update_bfd_session(
        &self,
        peer_ip: &str,
        global_cfg: &DashHaGlobalConfig,
        outgoing: &mut Outgoing,
        remove: bool,
    ) -> Result<()> {
        // todo: this needs to wait until HaScope has been activated.
        let Some(DpuData::LocalDpu { ref dpu, .. }) = self.dpu else {
            debug!("DPU is not managed by this HA instance. Ignore BFD session creation");
            return Ok(());
        };

        let sep = BfdSessionTable::key_separator();
        let key = format!("default{sep}default{sep}{peer_ip}");

        let kfv = if remove {
            KeyOpFieldValues {
                key,
                operation: KeyOperation::Del,
                field_values: HashMap::new(),
            }
        } else {
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
            KeyOpFieldValues {
                key,
                operation: KeyOperation::Set,
                field_values: fv,
            }
        };

        let msg = ActorMessage::new(self.id.clone(), &kfv)?;
        outgoing.send(outgoing.common_bridge_sp::<BfdSessionTable>(), msg);
        Ok(())
    }

    fn update_bfd_sessions(&self, state: &mut State, remove: bool) -> Result<()> {
        if !self.is_local_managed() {
            debug!("DPU is not managed by this HA instance. Ignore BFD session creation or deletion");
            return Ok(());
        }
        let (_internal, incoming, outgoing) = state.get_all();
        let global_cfg = Self::get_dash_ha_global_config(incoming)?;
        let remote_dpu_msgs = incoming.get_by_prefix(RemoteDpu::table_name());
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
            self.update_bfd_session(&npu, &global_cfg, outgoing, remove)?;
        }
        Ok(())
    }

    fn handle_remote_dpu_message_to_remote_dpu(
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
        self.update_dpu_state(incoming, outgoing, None)?;
        Ok(())
    }

    fn handle_remote_dpu_message_to_local_dpu(&mut self, state: &mut State, key: &str) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();
        let dpu_kfv: KeyOpFieldValues = incoming.get(key)?.deserialize_data()?;

        let remote_dpu: RemoteDpu = swss_serde::from_field_values(&dpu_kfv.field_values)?;

        // create bfd session
        let global_cfg = Self::get_dash_ha_global_config(incoming)?;
        self.update_bfd_session(&remote_dpu.npu_ipv4, &global_cfg, outgoing, false)?;
        Ok(())
    }

    async fn handle_remote_dpu_message(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        if self.dpu.is_none() || matches!(self.dpu.as_ref().unwrap(), DpuData::RemoteDpu(_)) {
            self.handle_remote_dpu_message_to_remote_dpu(state, key, context)
        } else {
            self.handle_remote_dpu_message_to_local_dpu(state, key)
        }
    }

    fn handle_dash_ha_global_config(&mut self, state: &mut State) -> Result<()> {
        self.update_bfd_sessions(state, false)?;
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
    #[instrument(name="handle_message", level="info", skip_all, fields(actor=format!("dpu/{}", self.id), key=key))]
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
            return self.handle_dpu_state_registration(key, incoming, outgoing);
        }

        // the rest of the messages are only for locally managed dpu
        if !self.is_local_managed() {
            return Ok(());
        } else if key == DashHaGlobalConfig::table_name() {
            return self.handle_dash_ha_global_config(state);
        } else if key == DpuState::table_name() || key == DashBfdProbeState::table_name() {
            return self.update_dpu_state(incoming, outgoing, None);
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
        test::{self, *},
    };
    use crate::db_structs::{BfdSessionTable, DashBfdProbeState, DashHaGlobalConfig, Dpu, DpuState, RemoteDpu};

    use crate::ha_actor_messages::DpuActorState;
    use sonic_common::SonicDbTable;
    use std::time::Duration;
    use swss_common_testing::Redis;
    use swss_serde::to_field_values;

    #[tokio::test]
    async fn dpu_actor() {
        let _ = Redis::start_config_db();
        let runtime = test::create_actor_runtime(0, "10.0.0.0", "10::").await;
        // prepare test data
        let dpu_pmon_up_state = make_dpu_pmon_state(true);
        let dpu_pmon_down_state = make_dpu_pmon_state(false);
        let dpu_bfd_up_state = make_dpu_bfd_state(vec!["10.0.0.0", "10.0.1.0", "10.0.2.0", "10.0.3.0"], vec![]);
        let dpu_bfd_down_state = make_dpu_bfd_state(vec![], vec![]);
        let dpu_actor_state_wo_bfd = make_local_dpu_actor_state(0, 0, true, Some(dpu_pmon_up_state.clone()), None);
        let remote_dpu1_fvs = to_field_values(&make_remote_dpu_object(1, 0)).unwrap();
        let remote_dpu2_fvs = to_field_values(&make_remote_dpu_object(2, 0)).unwrap();
        let remote_dpu3_fvs = to_field_values(&make_remote_dpu_object(3, 0)).unwrap();
        let dash_global_cfg = make_dash_ha_global_config();
        let dash_global_cfg_fvs = serde_json::to_value(to_field_values(&dash_global_cfg).unwrap()).unwrap();

        let mut dpu_actor_up_state = dpu_actor_state_wo_bfd.clone();
        dpu_actor_up_state.up = true;
        dpu_actor_up_state.dpu_bfd_state = Some(dpu_bfd_up_state.clone());

        let mut dpu_actor_pmon_down_state = dpu_actor_up_state.clone();
        dpu_actor_pmon_down_state.up = false;
        dpu_actor_pmon_down_state.dpu_pmon_state = Some(dpu_pmon_down_state.clone());

        let mut dpu_actor_bfd_down_state = dpu_actor_up_state.clone();
        dpu_actor_bfd_down_state.up = false;
        dpu_actor_bfd_down_state.dpu_bfd_state = Some(dpu_bfd_down_state.clone());

        let dpu_fvs = serde_json::to_value(to_field_values(&to_local_dpu(&dpu_actor_state_wo_bfd)).unwrap()).unwrap();
        let bfd = BfdSessionTable {
            tx_interval: dash_global_cfg.dpu_bfd_probe_interval_in_ms,
            rx_interval: dash_global_cfg.dpu_bfd_probe_interval_in_ms,
            multiplier: dash_global_cfg.dpu_bfd_probe_multiplier,
            multihop: true,
            local_addr: dpu_actor_state_wo_bfd.pa_ipv4.clone(),
            session_type: Some("passive".to_string()),
            shutdown: false,
        };
        let bfd_fvs = serde_json::to_value(to_field_values(&bfd).unwrap()).unwrap();

        let dpu_actor = DpuActor {
            id: dpu_actor_state_wo_bfd.dpu_name.clone(),
            dpu: None,
            bridges: Vec::new(),
        };
        let handle = runtime.spawn(dpu_actor, "dpu", "switch0_dpu0");

        #[rustfmt::skip]
        let commands = [
            // Bring up dpu
            send! { key: DpuState::table_name(), data: { "key": "DPU1", "operation": "Set", "field_values": serde_json::to_value(to_field_values(&dpu_pmon_up_state).unwrap()).unwrap()} },
            // Receiving DPU config-db object from swss-common bridge
            send! { key: Dpu::table_name(), data: { "key": "switch0_dpu0", "operation": "Set", "field_values": dpu_fvs},
                    addr: crate::common_bridge_sp::<Dpu>(&runtime.get_swbus_edge()) },
            send! { key: "REMOTE_DPU|switch1_dpu0", data: { "key": "REMOTE_DPU|switch1_dpu0", "operation": "Set", "field_values": serde_json::to_value(&remote_dpu1_fvs).unwrap() }},
            send! { key: DashHaGlobalConfig::table_name(), data: { "key": DashHaGlobalConfig::table_name(), "operation": "Set", "field_values": dash_global_cfg_fvs} },
            recv! { key: "switch0_dpu0", data: {"key": "default:default:10.0.0.0",  "operation": "Set", "field_values": bfd_fvs},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },
            recv! { key: "switch0_dpu0", data: {"key": "default:default:10.0.1.0",  "operation": "Set", "field_values": bfd_fvs},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },

            send! { key: "DPUStateRegister|vdpu/test-vdpu", data: { "active": true}, addr: runtime.sp("vdpu", "test-vdpu") },
            recv! { key: "DPUStateUpdate|switch0_dpu0", data: dpu_actor_state_wo_bfd, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: "REMOTE_DPU|switch2_dpu0", data: { "key": "REMOTE_DPU|switch2_dpu0", "operation": "Set", "field_values": serde_json::to_value(&remote_dpu2_fvs).unwrap()}},
            recv! { key: "switch0_dpu0", data: {"key": "default:default:10.0.2.0",  "operation": "Set", "field_values": bfd_fvs},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },
            send! { key: "REMOTE_DPU|switch3_dpu0", data: { "key": "REMOTE_DPU|switch3_dpu0", "operation": "Set", "field_values": serde_json::to_value(&remote_dpu3_fvs).unwrap()}},
            recv! { key: "switch0_dpu0", data: {"key": "default:default:10.0.3.0",  "operation": "Set", "field_values": bfd_fvs},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },

            send! { key: DashBfdProbeState::table_name(), data: { "key": "", "operation": "Set", "field_values":serde_json::to_value(to_field_values(&dpu_bfd_up_state).unwrap()).unwrap()} },
            recv! { key: "DPUStateUpdate|switch0_dpu0", data: dpu_actor_up_state, addr: runtime.sp("vdpu", "test-vdpu") },

            // Simulate DPU_STATE planes going down then up
            send! { key: DpuState::table_name(), data: { "key": "DPU1", "operation": "Set", "field_values": serde_json::to_value(to_field_values(&dpu_pmon_down_state).unwrap()).unwrap()} },
            recv! { key: "DPUStateUpdate|switch0_dpu0", data: dpu_actor_pmon_down_state, addr: runtime.sp("vdpu", "test-vdpu") },
            send! { key: DpuState::table_name(), data: { "key": "DPU1", "operation": "Set", "field_values": serde_json::to_value(to_field_values(&dpu_pmon_up_state).unwrap()).unwrap()} },
            recv! { key: "DPUStateUpdate|switch0_dpu0", data: dpu_actor_up_state, addr: runtime.sp("vdpu", "test-vdpu") },

            // Simulate BFD probe going down
            send! { key: DashBfdProbeState::table_name(), data: { "key": "", "operation": "Set", "field_values": serde_json::to_value(to_field_values(&dpu_bfd_down_state).unwrap()).unwrap()} },
            recv! { key: "DPUStateUpdate|switch0_dpu0", data: dpu_actor_bfd_down_state, addr: runtime.sp("vdpu", "test-vdpu") },

            // simulate delete of Dpu entry
            send! { key: Dpu::table_name(), data: { "key": DpuActor::dpu_table_name(), "operation": "Del", "field_values": dpu_fvs},
                addr: crate::common_bridge_sp::<Dpu>(&runtime.get_swbus_edge()) },

            recv! { key: "switch0_dpu0", data: {"key": "default:default:10.0.0.0",  "operation": "Del", "field_values": {}},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },
            recv! { key: "switch0_dpu0", data: {"key": "default:default:10.0.1.0",  "operation": "Del", "field_values": {}},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },
            recv! { key: "switch0_dpu0", data: {"key": "default:default:10.0.2.0",  "operation": "Del", "field_values": {}},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },
            recv! { key: "switch0_dpu0", data: {"key": "default:default:10.0.3.0",  "operation": "Del", "field_values": {}},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },
        ];
        test::run_commands(&runtime, runtime.sp("dpu", "switch0_dpu0"), &commands).await;
        if tokio::time::timeout(Duration::from_secs(1), handle).await.is_err() {
            panic!("timeout waiting for actor to terminate");
        }
    }

    #[tokio::test]
    async fn remote_dpu_actor() {
        let _ = Redis::start_config_db();

        let runtime = test::create_actor_runtime(1, "10.0.0.0", "10::").await;

        let rdpu_actor = DpuActor {
            id: "test-rdpu".into(),
            dpu: None,
            bridges: Vec::new(),
        };

        let handle = runtime.spawn(rdpu_actor, "dpu", "test-rdpu");

        let rdpu = make_remote_dpu_object(1, 1);
        let fvs = to_field_values(&rdpu).unwrap();
        let rdpu_obj = serde_json::to_value(fvs).unwrap();
        let rdpu_state = DpuActorState::from_remote_dpu("test-rdpu", &rdpu);

        #[rustfmt::skip]
        let commands = [
            // Receiving DPU config-db object from swss-common bridge
            send! { key: RemoteDpu::table_name(), data: { "key": "test-rdpu", "operation": "Set", "field_values": rdpu_obj}, addr: crate::common_bridge_sp::<RemoteDpu>(&runtime.get_swbus_edge()) },
            send! { key: "DPUStateRegister|vdpu/test-vdpu", data: { "active": true}, addr: runtime.sp("vdpu", "test-vdpu") },
            recv! { key: "DPUStateUpdate|test-rdpu", data: rdpu_state, addr: runtime.sp("vdpu", "test-vdpu") },

            // simulate delete of Dpu entry
            send! { key: RemoteDpu::table_name(), data: { "key": RemoteDpu::table_name(), "operation": "Del", "field_values": rdpu_obj}, addr: crate::common_bridge_sp::<RemoteDpu>(&runtime.get_swbus_edge()) },
        ];
        test::run_commands(&runtime, runtime.sp("dpu", "test-rdpu"), &commands).await;
        if tokio::time::timeout(Duration::from_secs(1), handle).await.is_err() {
            panic!("timeout waiting for actor to terminate");
        }
    }
}
