use crate::actors::vdpu::VDpuActor;
use crate::actors::{spawn_consumer_bridge_for_actor, DbBasedActor};
use crate::db_structs::*;
use crate::ha_actor_messages::{
    ActorRegistration, HaScopeActorState, HaSetActorState, RegistrationType, VDpuActorState,
};
use anyhow::{anyhow, Result};
use sonic_common::SonicDbTable;
use sonic_dash_api_proto::decode_from_field_values;
use sonic_dash_api_proto::ha_set_config::HaSetConfig;
use sonic_dash_api_proto::ip_to_string;
use sonic_dash_api_proto::types::{HaOwner, HaState};
use std::collections::{HashMap, HashSet};
use swbus_actor::{
    state::{incoming::Incoming, outgoing::Outgoing},
    Actor, ActorMessage, Context, State,
};
use swss_common::{KeyOpFieldValues, KeyOperation};
use swss_common_bridge::consumer::ConsumerBridge;
use tracing::{debug, error, info, instrument};

pub struct HaSetActor {
    id: String,
    dash_ha_set_config: Option<HaSetConfig>,
    dp_channel_is_alive: bool,
    ha_owner: HaOwner,
    bridges: Vec<ConsumerBridge>,
    bfd_session_npu_ips: HashSet<String>,
}

impl DbBasedActor for HaSetActor {
    fn new(key: String) -> Result<Self> {
        let actor = HaSetActor {
            id: key,
            dash_ha_set_config: None,
            dp_channel_is_alive: false,
            ha_owner: HaOwner::Unspecified,
            bridges: Vec::new(),
            bfd_session_npu_ips: HashSet::new(),
        };
        Ok(actor)
    }

    fn table_name() -> &'static str {
        HaSetConfig::table_name()
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
    fn get_dash_global_config(incoming: &Incoming) -> Option<DashHaGlobalConfig> {
        let Some(msg) = incoming.get(DashHaGlobalConfig::table_name()) else {
            debug!("DASH_HA_GLOBAL_CONFIG table is not available");
            return None;
        };
        let kfv = match msg.deserialize_data::<KeyOpFieldValues>() {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to deserialize DASH_HA_GLOBAL_CONFIG KeyOpFieldValues: {}", e);
                return None;
            }
        };

        match swss_serde::from_field_values(&kfv.field_values) {
            Ok(state) => Some(state),
            Err(e) => {
                error!("Failed to deserialize DASH_HA_GLOBAL_CONFIG from field values: {}", e);
                None
            }
        }
    }

    fn prepare_dash_ha_set_table_data(
        &self,
        vdpus: &[VDpuStateExt],
        incoming: &Incoming,
    ) -> Result<Option<DashHaSetTable>> {
        let Some(dash_ha_set_config) = self.dash_ha_set_config.as_ref() else {
            return Ok(None);
        };

        if !vdpus.iter().any(|vdpu_ext| vdpu_ext.vdpu.dpu.is_managed) {
            debug!("None of DPUs is managed by local HAMGRD. Skip dash_ha_set update");
            return Ok(None);
        }

        // only 2 vdpus are supported at the moment. Skip the rest.
        let (local_vdpu, remote_vdpu) = match (vdpus[0].vdpu.dpu.is_managed, vdpus[1].vdpu.dpu.is_managed) {
            (true, _) => (&vdpus[0].vdpu, &vdpus[1].vdpu),
            (false, true) => (&vdpus[1].vdpu, &vdpus[0].vdpu),
            (false, false) => {
                error!("Neither primary nor backup DPU are managed by local HAMGRD. Skip dash-ha-set update");
                return Ok(None);
            }
        };

        let Some(global_cfg) = Self::get_dash_global_config(incoming) else {
            return Ok(None);
        };

        let dash_ha_set = DashHaSetTable {
            version: dash_ha_set_config.version.clone(),
            vip_v4: dash_ha_set_config.vip_v4.as_ref().map(ip_to_string).unwrap_or_default(),
            vip_v6: dash_ha_set_config.vip_v6.as_ref().map(ip_to_string),
            owner: None,
            scope: sonic_dash_api_proto::types::HaScope::try_from(dash_ha_set_config.scope)
                .map(|s| {
                    let name = s.as_str_name();
                    name.strip_prefix("HA_SCOPE_").unwrap_or(name).to_lowercase()
                })
                .ok(),
            local_npu_ip: local_vdpu.dpu.npu_ipv4.clone(),
            local_ip: local_vdpu.dpu.pa_ipv4.clone(),
            peer_ip: remote_vdpu.dpu.pa_ipv4.clone(),
            cp_data_channel_port: global_cfg.cp_data_channel_port,
            dp_channel_dst_port: global_cfg.dp_channel_dst_port,
            dp_channel_src_port_min: global_cfg.dp_channel_src_port_min,
            dp_channel_src_port_max: global_cfg.dp_channel_src_port_max,
            dp_channel_probe_interval_ms: global_cfg.dp_channel_probe_interval_ms,
            dp_channel_probe_fail_threshold: global_cfg.dp_channel_probe_fail_threshold,
        };

        Ok(Some(dash_ha_set))
    }

    fn get_dpu_ha_set_state(&self, incoming: &Incoming) -> Option<DpuDashHaSetState> {
        let msg = incoming.get(DpuDashHaSetState::table_name())?;
        let kfv = match msg.deserialize_data::<KeyOpFieldValues>() {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to deserialize DASH_HA_SET_STATE KeyOpFieldValues: {}", e);
                return None;
            }
        };

        match swss_serde::from_field_values(&kfv.field_values) {
            Ok(state) => Some(state),
            Err(e) => {
                error!("Failed to deserialize DASH_HA_SET_STATE from field values: {}", e);
                None
            }
        }
    }

    fn update_dash_ha_set_state(
        &self,
        vdpus: &[VDpuStateExt],
        incoming: &Incoming,
        outgoing: &mut Outgoing,
    ) -> Result<()> {
        let Some(dash_ha_set) = self.prepare_dash_ha_set_table_data(vdpus, incoming)? else {
            return Ok(());
        };
        let fv = swss_serde::to_field_values(&dash_ha_set)?;
        let kfv = KeyOpFieldValues {
            key: self.id.clone(),
            operation: KeyOperation::Set,
            field_values: fv,
        };

        let msg = ActorMessage::new(self.id.clone(), &kfv)?;
        outgoing.send(outgoing.common_bridge_sp::<DashHaSetTable>(), msg);

        let vdpu_ids = self
            .dash_ha_set_config
            .as_ref()
            .map(|c| c.vdpu_ids.clone())
            .unwrap_or_default();
        let msg = HaSetActorState::new_actor_msg(self.dp_channel_is_alive, &self.id, dash_ha_set, &vdpu_ids).unwrap();
        let peer_actors = ActorRegistration::get_registered_actors(incoming, RegistrationType::HaSetState);
        for actor_sp in peer_actors {
            outgoing.send(actor_sp, msg.clone());
        }
        Ok(())
    }

    fn delete_dash_ha_set_table(&self, vdpus: &[VDpuStateExt], outgoing: &mut Outgoing) -> Result<()> {
        if !vdpus.iter().any(|vdpu_ext| vdpu_ext.vdpu.dpu.is_managed) {
            debug!("None of DPUs is managed by local HAMGRD. Skip dash_ha_set deletion");
            return Ok(());
        }

        let kfv = KeyOpFieldValues {
            key: self.id.clone(),
            operation: KeyOperation::Del,
            field_values: HashMap::new(),
        };

        let msg = ActorMessage::new(self.id.clone(), &kfv)?;
        outgoing.send(outgoing.common_bridge_sp::<DashHaSetTable>(), msg);

        Ok(())
    }

    async fn update_vnet_route_tunnel_table(
        &self,
        vdpus: &[VDpuStateExt],
        incoming: &Incoming,
        outgoing: &mut Outgoing,
    ) -> Result<()> {
        let Some(global_cfg) = Self::get_dash_global_config(incoming) else {
            return Ok(());
        };

        if !vdpus.iter().any(|vdpu_ext| vdpu_ext.vdpu.dpu.is_managed) {
            debug!("None of DPUs is managed by local HAMGRD. Skip vnet_route_tunnel update");
            return Ok(());
        }

        let swss_key = format!(
            "{}:{}",
            global_cfg
                .vnet_name
                .ok_or(anyhow!("Missing vnet_name in global config"))?,
            self.dash_ha_set_config
                .as_ref()
                .unwrap()
                .vip_v4
                .as_ref()
                .map(ip_to_string)
                .unwrap_or_default()
        );

        let mut endpoint = Vec::new();
        let mut endpoint_monitor = Vec::new();
        let mut primary = Vec::new();
        let mut check_directly_connected = false;
        let mut any_managed = false;

        for vdpu_ext in vdpus {
            if !vdpu_ext.vdpu.dpu.remote_dpu {
                // if it is locally managed dpu, use PA address as endpoint
                endpoint.push(vdpu_ext.vdpu.dpu.pa_ipv4.clone());
            } else {
                endpoint.push(vdpu_ext.vdpu.dpu.npu_ipv4.clone());
            }

            endpoint_monitor.push(vdpu_ext.vdpu.dpu.pa_ipv4.clone());
            if vdpu_ext.is_primary {
                if !vdpu_ext.vdpu.dpu.remote_dpu {
                    primary.push(vdpu_ext.vdpu.dpu.pa_ipv4.clone());
                } else {
                    primary.push(vdpu_ext.vdpu.dpu.npu_ipv4.clone());
                }
            }
            check_directly_connected |= !vdpu_ext.vdpu.dpu.remote_dpu;
            any_managed |= vdpu_ext.vdpu.dpu.is_managed;
        }

        if check_directly_connected && !any_managed {
            debug!(
                "Skipping VnetRouteTunnelTable update as directly connected DPU and no locally managed DPU are present."
            );
            return Ok(());
        }

        let pinned_state = self
            .dash_ha_set_config
            .as_ref()
            .map(|cfg| cfg.pinned_vdpu_bfd_probe_states.clone());

        // update vnet route tunnel table
        let vnet_route = VnetRouteTunnelTable {
            endpoint,
            endpoint_monitor: Some(endpoint_monitor),
            monitoring: Some("custom_bfd".into()),
            primary: Some(primary),
            rx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            tx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            check_directly_connected: Some(check_directly_connected),
            pinned_state,
        };
        let fvs = swss_serde::to_field_values(&vnet_route)?;

        let kfv = KeyOpFieldValues {
            key: swss_key,
            operation: KeyOperation::Set,
            field_values: fvs,
        };

        let msg = ActorMessage::new(self.id.clone(), &kfv)?;
        outgoing.send(outgoing.common_bridge_sp::<VnetRouteTunnelTable>(), msg);

        Ok(())
    }

    fn delete_vnet_route_tunnel_table(
        &self,
        vdpus: &[VDpuStateExt],
        incoming: &Incoming,
        outgoing: &mut Outgoing,
    ) -> Result<()> {
        if !vdpus.iter().any(|vdpu_ext| vdpu_ext.vdpu.dpu.is_managed) {
            debug!("None of DPUs is managed by local HAMGRD. Skip vnet_route_tunnel deletion");
            return Ok(());
        }

        let Some(global_cfg) = Self::get_dash_global_config(incoming) else {
            return Ok(());
        };

        let swss_key = format!(
            "{}:{}",
            global_cfg
                .vnet_name
                .ok_or(anyhow!("Missing vnet_name in global config"))?,
            self.dash_ha_set_config
                .as_ref()
                .unwrap()
                .vip_v4
                .as_ref()
                .map(ip_to_string)
                .unwrap_or_default()
        );

        let kfv = KeyOpFieldValues {
            key: swss_key,
            operation: KeyOperation::Del,
            field_values: HashMap::new(),
        };

        let msg = ActorMessage::new(self.id.clone(), &kfv)?;
        outgoing.send(outgoing.common_bridge_sp::<VnetRouteTunnelTable>(), msg);
        Ok(())
    }

    fn register_to_vdpu_actor(&self, outgoing: &mut Outgoing, active: bool) -> Result<()> {
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
        let msg = incoming.get(&key)?;
        match msg.deserialize_data() {
            Ok(vdpu) => Some(vdpu),
            Err(e) => {
                error!("Failed to deserialize VDpuActorState from the message: {}", e);
                None
            }
        }
    }

    /// Get vdpu data received via vdpu update and return them in a list with primary DPUs first.
    /// All preferred_vdpu_ids are considered primary, followed by backups.
    fn get_vdpus(&self, incoming: &Incoming) -> Vec<Option<VDpuStateExt>> {
        let Some(ref ha_set_cfg) = self.dash_ha_set_config else {
            return Vec::new();
        };

        let mut result = Vec::new();

        // Collect all preferred (primary) vdpus first
        let mut seen = std::collections::HashSet::new();
        if !ha_set_cfg.preferred_vdpu_id.is_empty() {
            for id in ha_set_cfg
                .preferred_vdpu_id
                .split(',')
                .map(str::trim)
                .filter(|id| !id.is_empty())
            {
                result.push(
                    self.get_vdpu(incoming, id)
                        .map(|vdpu| VDpuStateExt { vdpu, is_primary: true }),
                );
                seen.insert(id.to_string());
            }
        }

        // Then collect backups (those not in preferred_vdpu_ids)
        for id in ha_set_cfg
            .vdpu_ids
            .iter()
            .filter(|id| !id.is_empty() && !seen.contains(*id))
        {
            result.push(self.get_vdpu(incoming, id).map(|vdpu| VDpuStateExt {
                vdpu,
                is_primary: false,
            }));
        }

        result
    }

    /// Returns a vector of `VDpuStateExt` if all VDPU states are available.
    /// If any VDPU state is missing, returns `None`.
    /// This ensures that subsequent operations only proceed when all required DPU information is ready and relevant.
    /// returned vdpus are sorted by primary and backup, with primary first.
    fn get_vdpus_if_ready(&self, incoming: &Incoming) -> Option<Vec<VDpuStateExt>> {
        let vdpus = self.get_vdpus(incoming);
        if !vdpus.iter().all(|vdpu| vdpu.is_some()) {
            info!("Not all DPU info is ready yet");
            return None;
        }

        Some(vdpus.into_iter().map(|vdpu| vdpu.unwrap()).collect())
    }

    /// Return the HaScopeActorState embedded in the message
    fn get_ha_scope_actor_state(&self, incoming: &Incoming, key: &str) -> Option<HaScopeActorState> {
        let msg = incoming.get(key)?;
        match msg.deserialize_data() {
            Ok(ha_scope) => Some(ha_scope),
            Err(e) => {
                error!("Failed to deserialize HaScopeActorState from the message: {}", e);
                None
            }
        }
    }

    fn update_bfd_session(
        &self,
        peer_ip: &str,
        local_addr: &str,
        global_cfg: Option<&DashHaGlobalConfig>,
        outgoing: &mut Outgoing,
        remove: bool,
    ) -> Result<()> {
        let sep = BfdSessionTable::key_separator();
        let key = format!("default{sep}default{sep}{peer_ip}");

        let kfv = if remove {
            KeyOpFieldValues {
                key,
                operation: KeyOperation::Del,
                field_values: HashMap::new(),
            }
        } else {
            let global_cfg = global_cfg.ok_or_else(|| anyhow!("DASH_HA_GLOBAL_CONFIG is missing"))?;
            let bfd_session = BfdSessionTable {
                tx_interval: global_cfg.dpu_bfd_probe_interval_in_ms,
                rx_interval: global_cfg.dpu_bfd_probe_interval_in_ms,
                multiplier: global_cfg.dpu_bfd_probe_multiplier,
                multihop: true,
                local_addr: local_addr.to_string(),
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

    fn update_bfd_sessions(&mut self, incoming: &Incoming, outgoing: &mut Outgoing, remove: bool) -> Result<()> {
        let Some(vdpus) = self.get_vdpus_if_ready(incoming) else {
            return Ok(());
        };

        // If none of the vdpus is managed, skip BFD session creation
        if !vdpus.iter().any(|v| v.vdpu.dpu.is_managed) {
            debug!("None of DPUs is managed by local HAMGRD. Skip BFD session update");
            return Ok(());
        }

        if remove {
            // Delete all tracked BFD sessions
            let mut old_ips: Vec<String> = self.bfd_session_npu_ips.drain().collect();
            old_ips.sort();
            for npu_ip in old_ips {
                self.update_bfd_session(&npu_ip, "", None, outgoing, true)?;
            }
            return Ok(());
        }

        let Some(global_cfg) = Self::get_dash_global_config(incoming) else {
            return Ok(());
        };

        // Find the managed (local) VDPU to get local_addr (pa_ipv4)
        let local_addr = vdpus
            .iter()
            .find(|v| v.vdpu.dpu.is_managed)
            .map(|v| v.vdpu.dpu.pa_ipv4.clone())
            .unwrap();

        // Collect all unique npu_ipv4 addresses from all VDPUs
        let new_npu_ips: HashSet<String> = vdpus.iter().map(|v| v.vdpu.dpu.npu_ipv4.clone()).collect();

        // Delete BFD sessions for IPs no longer in the set
        let mut stale_ips: Vec<String> = self.bfd_session_npu_ips.difference(&new_npu_ips).cloned().collect();
        stale_ips.sort();
        for npu_ip in &stale_ips {
            self.update_bfd_session(npu_ip, "", None, outgoing, true)?;
        }

        // Create/update BFD sessions for current IPs
        let mut sorted_ips: Vec<&String> = new_npu_ips.iter().collect();
        sorted_ips.sort();
        for npu_ip in sorted_ips {
            self.update_bfd_session(npu_ip, &local_addr, Some(&global_cfg), outgoing, false)?;
        }

        self.bfd_session_npu_ips = new_npu_ips;
        Ok(())
    }

    async fn handle_dash_ha_set_config_table_message(
        &mut self,
        state: &mut State,
        key: &str,
        context: &mut Context,
    ) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();
        let dpu_kfv: KeyOpFieldValues = incoming.get_or_fail(key)?.deserialize_data()?;
        if dpu_kfv.operation == KeyOperation::Del {
            // cleanup resources before stopping
            if let Err(e) = self.do_cleanup(state) {
                error!("Failed to cleanup HaSetActor resources: {}", e);
            }
            context.stop();
            return Ok(());
        }
        let first_time = self.dash_ha_set_config.is_none();

        // Save old vdpu_ids before updating config, to unregister removed ones later
        let old_vdpu_ids: HashSet<String> = self
            .dash_ha_set_config
            .as_ref()
            .map(|cfg| {
                cfg.vdpu_ids
                    .iter()
                    .map(|id| id.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        self.dash_ha_set_config = Some(decode_from_field_values(&dpu_kfv.field_values).unwrap());

        // Subscribe to the DPU Actor for state updates.
        self.register_to_vdpu_actor(outgoing, true)?;

        // Unregister from vdpus that are no longer in the config
        if !first_time {
            let new_vdpu_ids: HashSet<String> = self
                .dash_ha_set_config
                .as_ref()
                .map(|cfg| {
                    cfg.vdpu_ids
                        .iter()
                        .map(|id| id.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default();
            let removed_vdpu_ids: Vec<&String> = old_vdpu_ids.difference(&new_vdpu_ids).collect();
            if !removed_vdpu_ids.is_empty() {
                let msg = ActorRegistration::new_actor_msg(false, RegistrationType::VDPUState, &self.id)?;
                for id in removed_vdpu_ids {
                    outgoing.send(outgoing.from_my_sp(VDpuActor::name(), id), msg.clone());
                }
            }
        }

        if first_time {
            self.bridges.push(
                spawn_consumer_bridge_for_actor::<DashHaGlobalConfig>(
                    context.get_edge_runtime().clone(),
                    Self::name(),
                    Some(&self.id),
                    true,
                )
                .await?,
            );

            self.bridges.push(
                spawn_consumer_bridge_for_actor::<DpuDashHaSetState>(
                    context.get_edge_runtime().clone(),
                    Self::name(),
                    Some(&self.id),
                    true,
                )
                .await?,
            );
        }

        let Some(vdpus) = self.get_vdpus_if_ready(incoming) else {
            return Ok(());
        };

        self.update_dash_ha_set_state(&vdpus, incoming, outgoing)?;

        if !first_time {
            // on config update, also update vnet route tunnel table and BFD sessions
            let Some(vdpus) = self.get_vdpus_if_ready(incoming) else {
                return Ok(());
            };

            self.update_vnet_route_tunnel_table(&vdpus, incoming, outgoing).await?;
            self.update_bfd_sessions(incoming, outgoing, false)?;
        }
        Ok(())
    }

    async fn handle_dash_ha_global_config(&mut self, state: &mut State) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();
        let Some(vdpus) = self.get_vdpus_if_ready(incoming) else {
            return Ok(());
        };
        // global config update affects Vxlan tunnel and dash-ha-set in DPU
        self.update_dash_ha_set_state(&vdpus, incoming, outgoing)?;
        if self.ha_owner == HaOwner::Dpu {
            self.update_vnet_route_tunnel_table(&vdpus, incoming, outgoing).await?;
        }
        self.update_bfd_sessions(incoming, outgoing, false)?;
        Ok(())
    }

    async fn handle_vdpu_state_update(&mut self, state: &mut State) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();
        // vdpu update affects dash-ha-set in DPU, vxlan tunnel, and BFD sessions
        let Some(vdpus) = self.get_vdpus_if_ready(incoming) else {
            return Ok(());
        };
        self.update_dash_ha_set_state(&vdpus, incoming, outgoing)?;
        if self.ha_owner == HaOwner::Dpu {
            self.update_vnet_route_tunnel_table(&vdpus, incoming, outgoing).await?;
        }
        // Note: vdpu state update doesn't change the set of npu_ips, but
        // may provide initial vdpu states needed for BFD session creation
        self.update_bfd_sessions(incoming, outgoing, false)?;
        Ok(())
    }

    async fn handle_haset_state_registration(&mut self, state: &mut State, key: &str) -> Result<()> {
        let (_, incoming, outgoing) = state.get_all();

        let entry = incoming
            .get_entry(key)
            .ok_or_else(|| anyhow!("Entry not found for key: {}", key))?;
        let ActorRegistration { active, .. } = entry.msg.deserialize_data()?;
        if active {
            let Some(vdpus) = self.get_vdpus_if_ready(incoming) else {
                return Ok(());
            };
            let Some(dash_ha_set) = self.prepare_dash_ha_set_table_data(&vdpus, incoming)? else {
                return Ok(());
            };

            let vdpu_ids = self
                .dash_ha_set_config
                .as_ref()
                .map(|c| c.vdpu_ids.clone())
                .unwrap_or_default();
            let msg =
                HaSetActorState::new_actor_msg(self.dp_channel_is_alive, &self.id, dash_ha_set, &vdpu_ids).unwrap();

            outgoing.send(entry.source.clone(), msg);
        }
        Ok(())
    }

    async fn handle_ha_scope_state_update(&mut self, state: &mut State, key: &str) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();
        let Some(ha_scope) = self.get_ha_scope_actor_state(incoming, key) else {
            return Ok(());
        };

        self.ha_owner = HaOwner::try_from(ha_scope.owner).unwrap_or(HaOwner::Unspecified);
        let ha_scope_state = ha_scope.ha_scope_state.clone();

        let mut vdpus = Vec::new();
        if ha_scope_state.local_ha_state.as_deref() == Some(HaState::Active.as_str_name()) {
            // primary (Active) DPU
            vdpus.push(
                self.get_vdpu(incoming, &ha_scope.vdpu_id)
                    .map(|vdpu| VDpuStateExt { vdpu, is_primary: true }),
            );
            // secondary (Standby) DPU
            vdpus.push(
                self.get_vdpu(incoming, &ha_scope.peer_vdpu_id)
                    .map(|vdpu| VDpuStateExt {
                        vdpu,
                        is_primary: false,
                    }),
            );
        } else if ha_scope_state.local_ha_state.as_deref() == Some(HaState::Standalone.as_str_name()) {
            // primary (Standalone) DPU
            vdpus.push(
                self.get_vdpu(incoming, &ha_scope.vdpu_id)
                    .map(|vdpu| VDpuStateExt { vdpu, is_primary: true }),
            );
        }

        // update VNET ROUTE Table
        let vdpus: Vec<VDpuStateExt> = vdpus.into_iter().flatten().collect();
        if !vdpus.is_empty() {
            self.update_vnet_route_tunnel_table(&vdpus, incoming, outgoing).await?;
        }

        Ok(())
    }

    fn do_cleanup(&mut self, state: &mut State) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();

        let Some(vdpus) = self.get_vdpus_if_ready(incoming) else {
            debug!("Not all DPU info is ready for cleanup");
            return Ok(());
        };

        if let Err(e) = self.delete_dash_ha_set_table(&vdpus, outgoing) {
            error!("Failed to delete dash_ha_set_table: {}", e);
        }

        if let Err(e) = self.delete_vnet_route_tunnel_table(&vdpus, incoming, outgoing) {
            error!("Failed to delete vnet_route_tunnel_table: {}", e);
        }

        if let Err(e) = self.update_bfd_sessions(incoming, outgoing, true) {
            error!("Failed to cleanup BFD sessions: {}", e);
        }

        self.register_to_vdpu_actor(outgoing, false)?;
        Ok(())
    }
}

impl Actor for HaSetActor {
    #[instrument(name="handle_message", level="info", skip_all, fields(actor=format!("ha-set/{}", self.id), key=key))]
    async fn handle_message(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        if key == Self::table_name() {
            if let Err(e) = self.handle_dash_ha_set_config_table_message(state, key, context).await {
                error!("handle_dash_ha_set_config_table_message failed: {e}");
            }
            return Ok(());
        }

        if self.dash_ha_set_config.is_none() {
            return Ok(());
        }

        if VDpuActorState::is_my_msg(key) {
            return self.handle_vdpu_state_update(state).await;
        } else if key == DashHaGlobalConfig::table_name() {
            return self.handle_dash_ha_global_config(state).await;
        } else if ActorRegistration::is_my_msg(key, RegistrationType::HaSetState) {
            return self.handle_haset_state_registration(state, key).await;
        } else if key.starts_with(DpuDashHaSetState::table_name()) {
            let (_, incoming, _) = state.get_all();
            let Some(ha_set_state) = self.get_dpu_ha_set_state(incoming) else {
                return Ok(());
            };
            self.dp_channel_is_alive = ha_set_state.dp_channel_is_alive;
        } else if HaScopeActorState::is_my_msg(key) {
            return self.handle_ha_scope_state_update(state, key).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::actors::KeyOperation;
    use crate::{
        actors::{
            ha_set::HaSetActor,
            test::{self, *},
            vdpu::VDpuActor,
            DbBasedActor,
        },
        db_structs::*,
        ha_actor_messages::*,
    };
    use sonic_common::SonicDbTable;
    use sonic_dash_api_proto::ha_set_config::HaSetConfig;
    use sonic_dash_api_proto::ip_to_string;
    use sonic_dash_api_proto::types::HaOwner;
    use std::collections::{HashMap, HashSet};
    use std::time::Duration;
    use swss_common::CxxString;
    use swss_common::KeyOpFieldValues;
    use swss_common_testing::*;

    fn protobuf_struct_to_kfv<T: prost::Message + Default + serde::Serialize>(cfg: &T) -> HashMap<String, CxxString> {
        let json = serde_json::to_string(&cfg).unwrap();
        let mut kfv = KeyOpFieldValues {
            key: HaSetActor::table_name().to_string(),
            operation: KeyOperation::Set,
            field_values: HashMap::new(),
        };
        kfv.field_values.clear();
        kfv.field_values.insert("json".to_string(), json.into());
        kfv.field_values.clone()
    }

    #[tokio::test]
    async fn ha_set_actor() {
        // To enable trace, set ENABLE_TRACE=1 to run test
        sonic_common::log::init_logger_for_test();

        let _redis = Redis::start_config_db();
        let runtime = test::create_actor_runtime(0, "10.0.0.0", "10::").await;

        //prepare test data
        let global_cfg = make_dash_ha_global_config();
        let global_cfg_fvs = serde_json::to_value(swss_serde::to_field_values(&global_cfg).unwrap()).unwrap();

        let (ha_set_id, ha_set_cfg) = make_dpu_scope_ha_set_config(0, 0);
        let ha_set_cfg_fvs = protobuf_struct_to_kfv(&ha_set_cfg);
        let mut ha_set_cfg_bfd_pinned = ha_set_cfg.clone();
        ha_set_cfg_bfd_pinned.pinned_vdpu_bfd_probe_states = vec!["down".to_string(), "up".to_string()];
        let ha_set_cfg_fvs_bfd_pinned = protobuf_struct_to_kfv(&ha_set_cfg_bfd_pinned);

        let dpu0 = make_local_dpu_actor_state(0, 0, true, None, None);
        let dpu1 = make_remote_dpu_actor_state(1, 0);
        let (vdpu0_id, vdpu0_state_obj) = make_vdpu_actor_state(true, &dpu0);
        let (vdpu1_id, vdpu1_state_obj) = make_vdpu_actor_state(true, &dpu1);
        let vdpu0_state = serde_json::to_value(&vdpu0_state_obj).unwrap();
        let vdpu1_state = serde_json::to_value(&vdpu1_state_obj).unwrap();

        let (_, ha_set_obj) = make_dpu_scope_ha_set_obj(0, 0);
        let ha_set_obj_fvs = serde_json::to_value(swss_serde::to_field_values(&ha_set_obj).unwrap()).unwrap();

        let bfd = BfdSessionTable {
            tx_interval: global_cfg.dpu_bfd_probe_interval_in_ms,
            rx_interval: global_cfg.dpu_bfd_probe_interval_in_ms,
            multiplier: global_cfg.dpu_bfd_probe_multiplier,
            multihop: true,
            local_addr: vdpu0_state_obj.dpu.pa_ipv4.clone(),
            session_type: Some("passive".to_string()),
            shutdown: false,
        };
        let bfd_fvs = serde_json::to_value(swss_serde::to_field_values(&bfd).unwrap()).unwrap();

        let expected_vnet_route = VnetRouteTunnelTable {
            endpoint: vec![
                vdpu0_state_obj.dpu.pa_ipv4.clone(),
                vdpu1_state_obj.dpu.npu_ipv4.clone(),
            ],
            endpoint_monitor: Some(vec![
                vdpu0_state_obj.dpu.pa_ipv4.clone(),
                vdpu1_state_obj.dpu.pa_ipv4.clone(),
            ]),
            monitoring: Some("custom_bfd".into()),
            primary: Some(vec![vdpu0_state_obj.dpu.pa_ipv4.clone()]),
            rx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            tx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            check_directly_connected: Some(true),
            pinned_state: Some(ha_set_cfg.pinned_vdpu_bfd_probe_states.clone()),
        };
        let expected_vnet_route = swss_serde::to_field_values(&expected_vnet_route).unwrap();

        let expected_vnet_route_bfd_pinned = VnetRouteTunnelTable {
            endpoint: vec![
                vdpu0_state_obj.dpu.pa_ipv4.clone(),
                vdpu1_state_obj.dpu.npu_ipv4.clone(),
            ],
            endpoint_monitor: Some(vec![
                vdpu0_state_obj.dpu.pa_ipv4.clone(),
                vdpu1_state_obj.dpu.pa_ipv4.clone(),
            ]),
            monitoring: Some("custom_bfd".into()),
            primary: Some(vec![vdpu0_state_obj.dpu.pa_ipv4.clone()]),
            rx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            tx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            check_directly_connected: Some(true),
            pinned_state: Some(ha_set_cfg_bfd_pinned.pinned_vdpu_bfd_probe_states.clone()),
        };
        let expected_vnet_route_bfd_pinned = swss_serde::to_field_values(&expected_vnet_route_bfd_pinned).unwrap();

        let ha_set_actor = HaSetActor {
            id: ha_set_id.clone(),
            dash_ha_set_config: None,
            dp_channel_is_alive: false,
            ha_owner: HaOwner::Unspecified,
            bridges: Vec::new(),
            bfd_session_npu_ips: HashSet::new(),
        };

        let handle = runtime.spawn(ha_set_actor, HaSetActor::name(), &ha_set_id);

        #[rustfmt::skip]
        let commands = [
            // Send DASH_HA_SET_CONFIG_TABLE config
            send! { key: HaSetActor::table_name(), data: { "key": HaSetActor::table_name(), "operation": "Set", "field_values": ha_set_cfg_fvs },
                    addr: crate::common_bridge_sp::<HaSetConfig>(&runtime.get_swbus_edge()) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": true },
                    addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": true },
                    addr: runtime.sp(VDpuActor::name(), &vdpu1_id) },
            // Send registration request from ha-scope actor
            send! { key: ActorRegistration::msg_key(RegistrationType::HaSetState, &format!("vdpu0:{ha_set_id}")), data: { "active": true},
                    addr: runtime.sp("ha-scope", &format!("vdpu0:{ha_set_id}")) },
            send! { key: "DASH_HA_GLOBAL_CONFIG", data: { "key": "DASH_HA_GLOBAL_CONFIG", "operation": "Set", "field_values": global_cfg_fvs } },
            // Simulate VDPU state update for vdpu0
            send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state, addr: runtime.sp("vdpu", &vdpu0_id) },
            // Simulate VDPU state update for vdpu1 (backup)
            send! { key: VDpuActorState::msg_key(&vdpu1_id), data: vdpu1_state, addr: runtime.sp("vdpu", &vdpu1_id) },
            // Verify that the DASH_HA_SET_TABLE was updated
            recv! { key: &ha_set_id, data: {"key": &ha_set_id,  "operation": "Set", "field_values": ha_set_obj_fvs},
                    addr: crate::common_bridge_sp::<DashHaSetTable>(&runtime.get_swbus_edge()) },
            // Verify that haset actor state is sent to ha-scope actor
            recv! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": true, "ha_set": &ha_set_obj },
                    addr: runtime.sp("ha-scope", &format!("vdpu0:{ha_set_id}")) },
            recv! { key: &ha_set_id, data: {"key": format!("{}:{}", global_cfg.vnet_name.as_ref().unwrap(), ip_to_string(ha_set_cfg.vip_v4.as_ref().unwrap())),
                      "operation": "Set", "field_values": expected_vnet_route},
                    addr: crate::common_bridge_sp::<VnetRouteTunnelTable>(&runtime.get_swbus_edge()) },
            // Verify BFD sessions created
            recv! { key: &ha_set_id, data: {"key": "default:default:10.0.0.0", "operation": "Set", "field_values": bfd_fvs},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },
            recv! { key: &ha_set_id, data: {"key": "default:default:10.0.1.0", "operation": "Set", "field_values": bfd_fvs},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },

            // simulate pinned_vdpu_bfd_probe_states update. vdpu register and DashHaSetTable update will be triggered but no change
            send! { key: HaSetActor::table_name(), data: { "key": HaSetActor::table_name(), "operation": "Set", "field_values": ha_set_cfg_fvs_bfd_pinned },
                    addr: crate::common_bridge_sp::<HaSetConfig>(&runtime.get_swbus_edge()) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": true },
                    addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": true },
                    addr: runtime.sp(VDpuActor::name(), &vdpu1_id) },
            recv! { key: &ha_set_id, data: {"key": &ha_set_id,  "operation": "Set", "field_values": ha_set_obj_fvs},
                    addr: crate::common_bridge_sp::<DashHaSetTable>(&runtime.get_swbus_edge()) },
            recv! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": true, "ha_set": &ha_set_obj },
                    addr: runtime.sp("ha-scope", &format!("vdpu0:{ha_set_id}")) },
            recv! { key: &ha_set_id, data: {"key": format!("{}:{}", global_cfg.vnet_name.as_ref().unwrap(), ip_to_string(ha_set_cfg.vip_v4.as_ref().unwrap())),
                      "operation": "Set", "field_values": expected_vnet_route_bfd_pinned},
                    addr: crate::common_bridge_sp::<VnetRouteTunnelTable>(&runtime.get_swbus_edge()) },
            // Verify BFD sessions re-created after config update
            recv! { key: &ha_set_id, data: {"key": "default:default:10.0.0.0", "operation": "Set", "field_values": bfd_fvs},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },
            recv! { key: &ha_set_id, data: {"key": "default:default:10.0.1.0", "operation": "Set", "field_values": bfd_fvs},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },

            // simulate delete of ha-set entry
            send! { key: HaSetActor::table_name(), data: { "key": HaSetActor::table_name(), "operation": "Del", "field_values": ha_set_cfg_fvs },
                    addr: crate::common_bridge_sp::<HaSetConfig>(&runtime.get_swbus_edge()) },
            recv! { key: &ha_set_id, data: {"key": &ha_set_id,  "operation": "Del", "field_values": {}},
                    addr: crate::common_bridge_sp::<DashHaSetTable>(&runtime.get_swbus_edge()) },
            recv! { key: &ha_set_id, data: {"key": format!("{}:{}", global_cfg.vnet_name.as_ref().unwrap(), ip_to_string(ha_set_cfg.vip_v4.as_ref().unwrap())),
                       "operation": "Del", "field_values": {}},
                    addr: crate::common_bridge_sp::<VnetRouteTunnelTable>(&runtime.get_swbus_edge()) },
            // Verify BFD sessions deleted
            recv! { key: &ha_set_id, data: {"key": "default:default:10.0.0.0", "operation": "Del", "field_values": {}},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },
            recv! { key: &ha_set_id, data: {"key": "default:default:10.0.1.0", "operation": "Del", "field_values": {}},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": false },
                    addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": false },
                    addr: runtime.sp(VDpuActor::name(), &vdpu1_id) },
        ];

        test::run_commands(&runtime, runtime.sp(HaSetActor::name(), &ha_set_id), &commands).await;
        if tokio::time::timeout(Duration::from_secs(3), handle).await.is_err() {
            panic!("timeout waiting for actor to terminate");
        }
    }

    // test ha-set updated with new vdpu_ids, causing stale BFD sessions removed and new ones added
    #[tokio::test]
    async fn ha_set_actor_vdpu_change() {
        sonic_common::log::init_logger_for_test();

        let _redis = Redis::start_config_db();
        let runtime = test::create_actor_runtime(0, "10.0.0.0", "10::").await;

        // prepare test data
        let global_cfg = make_dash_ha_global_config();
        let global_cfg_fvs = serde_json::to_value(swss_serde::to_field_values(&global_cfg).unwrap()).unwrap();

        // Initial config: switch pair 0 (vdpu0-0 + vdpu1-0)
        let (ha_set_id, ha_set_cfg) = make_dpu_scope_ha_set_config(0, 0);
        let ha_set_cfg_fvs = protobuf_struct_to_kfv(&ha_set_cfg);

        let dpu0 = make_local_dpu_actor_state(0, 0, true, None, None);
        let dpu1 = make_remote_dpu_actor_state(1, 0);
        let (vdpu0_id, vdpu0_state_obj) = make_vdpu_actor_state(true, &dpu0);
        let (vdpu1_id, vdpu1_state_obj) = make_vdpu_actor_state(true, &dpu1);
        let vdpu0_state = serde_json::to_value(&vdpu0_state_obj).unwrap();
        let vdpu1_state = serde_json::to_value(&vdpu1_state_obj).unwrap();

        let (_, ha_set_obj) = make_dpu_scope_ha_set_obj(0, 0);
        let ha_set_obj_fvs = serde_json::to_value(swss_serde::to_field_values(&ha_set_obj).unwrap()).unwrap();

        // Initial BFD sessions (local_addr from managed dpu0: 18.0.0.0)
        let bfd = BfdSessionTable {
            tx_interval: global_cfg.dpu_bfd_probe_interval_in_ms,
            rx_interval: global_cfg.dpu_bfd_probe_interval_in_ms,
            multiplier: global_cfg.dpu_bfd_probe_multiplier,
            multihop: true,
            local_addr: vdpu0_state_obj.dpu.pa_ipv4.clone(),
            session_type: Some("passive".to_string()),
            shutdown: false,
        };
        let bfd_fvs = serde_json::to_value(swss_serde::to_field_values(&bfd).unwrap()).unwrap();

        let expected_vnet_route = VnetRouteTunnelTable {
            endpoint: vec![
                vdpu0_state_obj.dpu.pa_ipv4.clone(),
                vdpu1_state_obj.dpu.npu_ipv4.clone(),
            ],
            endpoint_monitor: Some(vec![
                vdpu0_state_obj.dpu.pa_ipv4.clone(),
                vdpu1_state_obj.dpu.pa_ipv4.clone(),
            ]),
            monitoring: Some("custom_bfd".into()),
            primary: Some(vec![vdpu0_state_obj.dpu.pa_ipv4.clone()]),
            rx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            tx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            check_directly_connected: Some(true),
            pinned_state: Some(ha_set_cfg.pinned_vdpu_bfd_probe_states.clone()),
        };
        let expected_vnet_route = swss_serde::to_field_values(&expected_vnet_route).unwrap();

        // New VDPUs: switch pair 1 (vdpu0-0 + vdpu2-0)
        let dpu2 = make_remote_dpu_actor_state(2, 0);
        let (vdpu2_id, vdpu2_state_obj) = make_vdpu_actor_state(true, &dpu2);
        let vdpu2_state = serde_json::to_value(&vdpu2_state_obj).unwrap();

        // Updated ha_set config with new vdpu_ids
        let mut ha_set_cfg_updated = ha_set_cfg.clone();
        ha_set_cfg_updated.vdpu_ids = vec![vdpu0_id.clone(), vdpu2_id.clone()];
        ha_set_cfg_updated.preferred_vdpu_id = vdpu0_id.clone();
        let ha_set_cfg_fvs_updated = protobuf_struct_to_kfv(&ha_set_cfg_updated);

        // New BFD sessions (local_addr from managed dpu0: 18.0.0.0)
        let bfd_new = BfdSessionTable {
            tx_interval: global_cfg.dpu_bfd_probe_interval_in_ms,
            rx_interval: global_cfg.dpu_bfd_probe_interval_in_ms,
            multiplier: global_cfg.dpu_bfd_probe_multiplier,
            multihop: true,
            local_addr: vdpu0_state_obj.dpu.pa_ipv4.clone(),
            session_type: Some("passive".to_string()),
            shutdown: false,
        };
        let bfd_new_fvs = serde_json::to_value(swss_serde::to_field_values(&bfd_new).unwrap()).unwrap();

        // Expected DashHaSetTable after vdpu change
        let ha_set_obj_updated = DashHaSetTable {
            version: "1".to_string(),
            vip_v4: ip_to_string(ha_set_cfg.vip_v4.as_ref().unwrap()),
            vip_v6: Some(ip_to_string(ha_set_cfg.vip_v6.as_ref().unwrap())),
            owner: None,
            scope: Some("dpu".to_string()),
            local_npu_ip: vdpu0_state_obj.dpu.npu_ipv4.clone(),
            local_ip: vdpu0_state_obj.dpu.pa_ipv4.clone(),
            peer_ip: vdpu2_state_obj.dpu.pa_ipv4.clone(),
            cp_data_channel_port: global_cfg.cp_data_channel_port,
            dp_channel_dst_port: global_cfg.dp_channel_dst_port,
            dp_channel_src_port_min: global_cfg.dp_channel_src_port_min,
            dp_channel_src_port_max: global_cfg.dp_channel_src_port_max,
            dp_channel_probe_interval_ms: global_cfg.dp_channel_probe_interval_ms,
            dp_channel_probe_fail_threshold: global_cfg.dp_channel_probe_fail_threshold,
        };
        let ha_set_obj_updated_fvs =
            serde_json::to_value(swss_serde::to_field_values(&ha_set_obj_updated).unwrap()).unwrap();

        // Expected VnetRouteTunnelTable after vdpu change
        let expected_vnet_route_updated = VnetRouteTunnelTable {
            endpoint: vec![
                vdpu0_state_obj.dpu.pa_ipv4.clone(),
                vdpu2_state_obj.dpu.npu_ipv4.clone(),
            ],
            endpoint_monitor: Some(vec![
                vdpu0_state_obj.dpu.pa_ipv4.clone(),
                vdpu2_state_obj.dpu.pa_ipv4.clone(),
            ]),
            monitoring: Some("custom_bfd".into()),
            primary: Some(vec![vdpu0_state_obj.dpu.pa_ipv4.clone()]),
            rx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            tx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            check_directly_connected: Some(true),
            pinned_state: Some(ha_set_cfg_updated.pinned_vdpu_bfd_probe_states.clone()),
        };
        let expected_vnet_route_updated = swss_serde::to_field_values(&expected_vnet_route_updated).unwrap();

        let ha_set_actor = HaSetActor {
            id: ha_set_id.clone(),
            dash_ha_set_config: None,
            dp_channel_is_alive: false,
            ha_owner: HaOwner::Unspecified,
            bridges: Vec::new(),
            bfd_session_npu_ips: HashSet::new(),
        };

        let handle = runtime.spawn(ha_set_actor, HaSetActor::name(), &ha_set_id);

        #[rustfmt::skip]
        let commands = [
            // === Phase 1: Initial setup with vdpu0-0 and vdpu1-0 ===
            send! { key: HaSetActor::table_name(), data: { "key": HaSetActor::table_name(), "operation": "Set", "field_values": ha_set_cfg_fvs },
                    addr: crate::common_bridge_sp::<HaSetConfig>(&runtime.get_swbus_edge()) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": true },
                    addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": true },
                    addr: runtime.sp(VDpuActor::name(), &vdpu1_id) },
            send! { key: "DASH_HA_GLOBAL_CONFIG", data: { "key": "DASH_HA_GLOBAL_CONFIG", "operation": "Set", "field_values": global_cfg_fvs } },

            // Now send original VDPU states
            send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state, addr: runtime.sp("vdpu", &vdpu0_id) },
            send! { key: VDpuActorState::msg_key(&vdpu1_id), data: vdpu1_state, addr: runtime.sp("vdpu", &vdpu1_id) },
            // Verify initial DashHaSetTable, VnetRoute, and BFD sessions
            recv! { key: &ha_set_id, data: {"key": &ha_set_id, "operation": "Set", "field_values": ha_set_obj_fvs},
                    addr: crate::common_bridge_sp::<DashHaSetTable>(&runtime.get_swbus_edge()) },
            recv! { key: &ha_set_id, data: {"key": format!("{}:{}", global_cfg.vnet_name.as_ref().unwrap(), ip_to_string(ha_set_cfg.vip_v4.as_ref().unwrap())),
                      "operation": "Set", "field_values": expected_vnet_route},
                    addr: crate::common_bridge_sp::<VnetRouteTunnelTable>(&runtime.get_swbus_edge()) },
            recv! { key: &ha_set_id, data: {"key": "default:default:10.0.0.0", "operation": "Set", "field_values": bfd_fvs},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },
            recv! { key: &ha_set_id, data: {"key": "default:default:10.0.1.0", "operation": "Set", "field_values": bfd_fvs},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },

            // === Phase 2: Update ha_set config with new vdpu_ids ===
            send! { key: HaSetActor::table_name(), data: { "key": HaSetActor::table_name(), "operation": "Set", "field_values": ha_set_cfg_fvs_updated },
                    addr: crate::common_bridge_sp::<HaSetConfig>(&runtime.get_swbus_edge()) },
            // Expect registration to new vdpus
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": true },
                    addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": true },
                    addr: runtime.sp(VDpuActor::name(), &vdpu2_id) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": false },
                    addr: runtime.sp(VDpuActor::name(), &vdpu1_id) },
            // Pre-populate new VDPU states in incoming (no output since config references vdpu0-0/vdpu1-0 which aren't ready yet)
            send! { key: VDpuActorState::msg_key(&vdpu2_id), data: vdpu2_state, addr: runtime.sp("vdpu", &vdpu2_id) },

            // Verify updated DashHaSetTable
            recv! { key: &ha_set_id, data: {"key": &ha_set_id, "operation": "Set", "field_values": ha_set_obj_updated_fvs},
                    addr: crate::common_bridge_sp::<DashHaSetTable>(&runtime.get_swbus_edge()) },
            // Verify updated VnetRouteTunnelTable
            recv! { key: &ha_set_id, data: {"key": format!("{}:{}", global_cfg.vnet_name.as_ref().unwrap(), ip_to_string(ha_set_cfg.vip_v4.as_ref().unwrap())),
                      "operation": "Set", "field_values": expected_vnet_route_updated},
                    addr: crate::common_bridge_sp::<VnetRouteTunnelTable>(&runtime.get_swbus_edge()) },
            // Verify stale BFD sessions removed 10.0.1.0
            recv! { key: &ha_set_id, data: {"key": "default:default:10.0.1.0", "operation": "Del", "field_values": {}},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },
            // Verify new BFD sessions created (10.0.0.0, 10.0.2.0)
            recv! { key: &ha_set_id, data: {"key": "default:default:10.0.0.0", "operation": "Set", "field_values": bfd_new_fvs},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },
            recv! { key: &ha_set_id, data: {"key": "default:default:10.0.2.0", "operation": "Set", "field_values": bfd_new_fvs},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },

            // === Phase 3: Delete ha-set entry ===
            send! { key: HaSetActor::table_name(), data: { "key": HaSetActor::table_name(), "operation": "Del", "field_values": ha_set_cfg_fvs_updated },
                    addr: crate::common_bridge_sp::<HaSetConfig>(&runtime.get_swbus_edge()) },
            recv! { key: &ha_set_id, data: {"key": &ha_set_id, "operation": "Del", "field_values": {}},
                    addr: crate::common_bridge_sp::<DashHaSetTable>(&runtime.get_swbus_edge()) },
            recv! { key: &ha_set_id, data: {"key": format!("{}:{}", global_cfg.vnet_name.as_ref().unwrap(), ip_to_string(ha_set_cfg.vip_v4.as_ref().unwrap())),
                       "operation": "Del", "field_values": {}},
                    addr: crate::common_bridge_sp::<VnetRouteTunnelTable>(&runtime.get_swbus_edge()) },
            recv! { key: &ha_set_id, data: {"key": "default:default:10.0.0.0", "operation": "Del", "field_values": {}},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },
            recv! { key: &ha_set_id, data: {"key": "default:default:10.0.2.0", "operation": "Del", "field_values": {}},
                    addr: crate::common_bridge_sp::<BfdSessionTable>(&runtime.get_swbus_edge()) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": false },
                    addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": false },
                    addr: runtime.sp(VDpuActor::name(), &vdpu2_id) },
        ];

        test::run_commands(&runtime, runtime.sp(HaSetActor::name(), &ha_set_id), &commands).await;
        if tokio::time::timeout(Duration::from_secs(3), handle).await.is_err() {
            panic!("timeout waiting for actor to terminate");
        }
    }

    // test remote ha-set, when both vdpus are remote. ha-set is responsible to program vnet route to remote dpus
    #[tokio::test]
    async fn remote_ha_set_actor() {
        // To enable trace, set ENABLE_TRACE=1 to run test
        sonic_common::log::init_logger_for_test();

        let _redis = Redis::start_config_db();
        let runtime = test::create_actor_runtime(0, "10.0.0.0", "10::").await;

        //prepare test data
        let global_cfg = make_dash_ha_global_config();
        let global_cfg_fvs = serde_json::to_value(swss_serde::to_field_values(&global_cfg).unwrap()).unwrap();

        let (ha_set_id, ha_set_cfg) = make_dpu_scope_ha_set_config(2, 0);
        let ha_set_cfg_fvs = protobuf_struct_to_kfv(&ha_set_cfg);
        let mut ha_set_cfg_bfd_pinned = ha_set_cfg.clone();
        ha_set_cfg_bfd_pinned.pinned_vdpu_bfd_probe_states = vec!["down".to_string(), "up".to_string()];
        let ha_set_cfg_fvs_bfd_pinned = protobuf_struct_to_kfv(&ha_set_cfg_bfd_pinned);

        let dpu0 = make_remote_dpu_actor_state(2, 0);
        let dpu1 = make_remote_dpu_actor_state(3, 0);
        let (vdpu0_id, vdpu0_state_obj) = make_vdpu_actor_state(true, &dpu0);
        let (vdpu1_id, vdpu1_state_obj) = make_vdpu_actor_state(true, &dpu1);
        let vdpu0_state = serde_json::to_value(&vdpu0_state_obj).unwrap();
        let vdpu1_state = serde_json::to_value(&vdpu1_state_obj).unwrap();

        let ha_set_actor = HaSetActor {
            id: ha_set_id.clone(),
            dash_ha_set_config: None,
            bridges: Vec::new(),
            bfd_session_npu_ips: HashSet::new(),
        };

        let handle = runtime.spawn(ha_set_actor, HaSetActor::name(), &ha_set_id);

        #[rustfmt::skip]
        let commands = [
            // Send DASH_HA_SET_CONFIG_TABLE config
            send! { key: HaSetActor::table_name(), data: { "key": HaSetActor::table_name(), "operation": "Set", "field_values": ha_set_cfg_fvs },
                    addr: crate::common_bridge_sp::<HaSetConfig>(&runtime.get_swbus_edge()) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": true },
                    addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": true },
                    addr: runtime.sp(VDpuActor::name(), &vdpu1_id) },
            send! { key: DashHaGlobalConfig::table_name(), data: { "key": DashHaGlobalConfig::table_name(), "operation": "Set", "field_values": global_cfg_fvs } },
            // Simulate VDPU state update for vdpu0
            send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state, addr: runtime.sp("vdpu", &vdpu0_id) },
            // Simulate VDPU state update for vdpu1 (backup)
            send! { key: VDpuActorState::msg_key(&vdpu1_id), data: vdpu1_state, addr: runtime.sp("vdpu", &vdpu1_id) },

            // simulate pinned_vdpu_bfd_probe_states update
            send! { key: HaSetActor::table_name(), data: { "key": HaSetActor::table_name(), "operation": "Set", "field_values": ha_set_cfg_fvs_bfd_pinned },
                    addr: crate::common_bridge_sp::<HaSetConfig>(&runtime.get_swbus_edge()) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": true },
                    addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": true },
                    addr: runtime.sp(VDpuActor::name(), &vdpu1_id) },

            // simulate delete of ha-set entry
            send! { key: HaSetActor::table_name(), data: { "key": HaSetActor::table_name(), "operation": "Del", "field_values": ha_set_cfg_fvs },
                    addr: crate::common_bridge_sp::<HaSetConfig>(&runtime.get_swbus_edge()) },

            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": false },
                    addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },

            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": false },
                    addr: runtime.sp(VDpuActor::name(), &vdpu1_id) },
        ];

        test::run_commands(&runtime, runtime.sp(HaSetActor::name(), &ha_set_id), &commands).await;
        if tokio::time::timeout(Duration::from_secs(3), handle).await.is_err() {
            panic!("timeout waiting for actor to terminate");
        }
    }
}
