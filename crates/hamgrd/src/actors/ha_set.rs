use crate::actors::vdpu::VDpuActor;
use crate::actors::{ha_set, spawn_consumer_bridge_for_actor, ActorCreator, DbBasedActor};
use crate::db_structs::*;
use crate::ha_actor_messages::{ActorRegistration, RegistrationType, VDpuActorState};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use swbus_actor::state::incoming;
use swbus_actor::state::internal::{self, Internal};
use swbus_actor::{state::incoming::Incoming, state::outgoing::Outgoing, Actor, ActorMessage, Context, State};
use swbus_edge::SwbusEdgeRuntime;
use swss_common::Table;
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

    fn db_name() -> &'static str {
        "APPL_DB"
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
    fn get_dash_global_config(incoming: &Incoming) -> Result<DashHaGlobalConfig> {
        let kfv: KeyOpFieldValues = incoming.get("DASH_HA_GLOBAL_CONFIG")?.deserialize_data()?;
        let global_cfg = swss_serde::from_field_values(&kfv.field_values)?;
        Ok(global_cfg)
    }

    fn update_dash_ha_set_table(
        &self,
        vdpus: &[VDpuStateExt],
        incoming: &Incoming,
        outgoing: &mut Outgoing,
    ) -> Result<()> {
        let Some(dash_ha_set_config) = self.dash_ha_set_config.as_ref() else {
            return Ok(());
        };

        if !vdpus.iter().any(|vdpu_ext| vdpu_ext.vdpu.dpu.is_managed) {
            debug!("None of DPUs is managed by local HAMGRD. Skip dash_ha_set update");
            return Ok(());
        }

        // only 2 vdpus are supported at the moment. Skip the rest.
        let (local_vdpu, remote_vdpu) = match (vdpus[0].vdpu.dpu.is_managed, vdpus[1].vdpu.dpu.is_managed) {
            (true, _) => (&vdpus[0].vdpu, &vdpus[1].vdpu),
            (false, true) => (&vdpus[1].vdpu, &vdpus[0].vdpu),
            (false, false) => {
                error!("Neither primary nor backup DPU are managed by local HAMGRD. Skip dash-ha-set update");
                return Ok(());
            }
        };

        let global_cfg = Self::get_dash_global_config(incoming)?;

        let dash_ha_set = DashHaSetTable {
            version: dash_ha_set_config.version.clone(),
            vip_v4: dash_ha_set_config.vip_v4.clone(),
            vip_v6: dash_ha_set_config.vip_v6.clone(),
            owner: dash_ha_set_config.owner.clone(),
            scope: dash_ha_set_config.scope.clone(),
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
        internal: &mut Internal,
    ) -> Result<()> {
        let global_cfg = Self::get_dash_global_config(incoming)?;

        let mut endpoint = Vec::new();
        let mut endpoint_monitor = Vec::new();
        let mut primary = Vec::new();
        let mut check_directly_connected = false;

        for vdpu_ext in vdpus {
            if vdpu_ext.vdpu.dpu.is_managed {
                // if it is locally managed dpu, use dpu pa_ipv4 as endpoint
                endpoint.push(vdpu_ext.vdpu.dpu.pa_ipv4.clone());
            } else {
                endpoint.push(vdpu_ext.vdpu.dpu.npu_ipv4.clone());
            }

            endpoint_monitor.push(vdpu_ext.vdpu.dpu.pa_ipv4.clone());
            primary.push(vdpu_ext.is_primary.to_string());
            check_directly_connected |= vdpu_ext.vdpu.dpu.is_managed;
        }

        // update vnet route tunnel table
        let vnet_route = VnetRouteTunnelTable {
            endpoint,
            endpoint_monitor: Some(endpoint_monitor),
            monitoring: None,
            primary: Some(primary),
            rx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            tx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            check_directly_connected: Some(check_directly_connected),
        };
        let fvs = swss_serde::to_field_values(&vnet_route)?;

        internal.get_mut("VNET_ROUTE_TUNNEL_TABLE").clone_from(&fvs);
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
        if let Some(prefered_vdpu_ids) = ha_set_cfg.preferred_vdpu_ids.as_ref() {
            for id in prefered_vdpu_ids.iter().filter(|id| !id.is_empty()) {
                result.push(
                    self.get_vdpu(incoming, id)
                        .map(|vdpu| VDpuStateExt { vdpu, is_primary: true }),
                );
                seen.insert(id);
            }
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

    async fn handle_dash_ha_set_config_table_message(
        &mut self,
        state: &mut State,
        key: &str,
        context: &mut Context,
    ) -> Result<()> {
        let (internal, incoming, outgoing) = state.get_all();
        let dpu_kfv: KeyOpFieldValues = incoming.get(key)?.deserialize_data()?;
        if dpu_kfv.operation == KeyOperation::Del {
            // unregister from the DPU Actor
            self.register_to_vdpu_actor(outgoing, false).await?;

            context.stop();
            return Ok(());
        }
        let first_time = self.dash_ha_set_config.is_none();

        self.dash_ha_set_config = Some(swss_serde::from_field_values(&dpu_kfv.field_values)?);
        let swss_key = format!("default:{}", self.dash_ha_set_config.as_ref().unwrap().vip_v4);
        if !internal.has_entry(key, &swss_key) {
            let db = crate::db_named("APPL_DB").await?;
            let table = Table::new_async(db, "VNET_ROUTE_TUNNEL_TABLE").await?;
            internal.add("VNET_ROUTE_TUNNEL_TABLE", table, swss_key).await;
        }
        // Subscribe to the DPU Actor for state updates.
        self.register_to_vdpu_actor(outgoing, true).await?;

        if first_time {
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
            if let Err(e) = self.handle_dash_ha_set_config_table_message(state, key, context).await {
                let err = format!("handle_dash_ha_set_config_table_message failed: {e}");
                println!("{}", err);
            }
            return Ok(());
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
            ha_set::{self, HaSetActor},
            test::{self, *},
            vdpu::{self, VDpuActor},
            DbBasedActor,
        },
        db_structs::VnetRouteTunnelTable,
        ha_actor_messages::*,
    };
    use std::time::Duration;
    use swss_common::{DbConnector, Table};
    use swss_common_testing::*;

    #[tokio::test]
    async fn ha_set_actor() {
        // To enable trace, set ENABLE_TRACE=1 to run test
        sonic_common::log::init_logger_for_test();

        let redis = Redis::start_config_db();
        let runtime = test::create_actor_runtime(0, "10.0.0.0", "10::").await;

        //prepare test data
        let global_cfg = make_dash_ha_global_config();
        let global_cfg_fvs = serde_json::to_value(swss_serde::to_field_values(&global_cfg).unwrap()).unwrap();

        let (ha_set_id, ha_set_cfg) = make_dpu_scope_ha_set_config(0, 0);
        let ha_set_cfg_fvs = serde_json::to_value(swss_serde::to_field_values(&ha_set_cfg).unwrap()).unwrap();
        let dpu0 = make_local_dpu_actor_state(0, 0, true, None, None);
        let dpu1 = make_remote_dpu_actor_state(1, 0);
        let (vdpu0_id, vdpu0_state_obj) = make_vdpu_actor_state(true, &dpu0);
        let (vdpu1_id, vdpu1_state_obj) = make_vdpu_actor_state(true, &dpu1);
        let vdpu0_state = serde_json::to_value(&vdpu0_state_obj).unwrap();
        let vdpu1_state = serde_json::to_value(&vdpu1_state_obj).unwrap();

        let ha_set_obj = serde_json::json!({
            "version": ha_set_cfg.version,
            "vip_v4": ha_set_cfg.vip_v4,
            "vip_v6": ha_set_cfg.vip_v6,
            "owner": ha_set_cfg.owner,
            "scope": ha_set_cfg.scope,
            "local_npu_ip": vdpu0_state_obj.dpu.npu_ipv4,
            "local_ip": vdpu0_state_obj.dpu.pa_ipv4,
            "peer_ip": vdpu1_state_obj.dpu.pa_ipv4,
            "cp_data_channel_port": global_cfg.cp_data_channel_port.unwrap().to_string(),
            "dp_channel_dst_port": global_cfg.dp_channel_dst_port.unwrap().to_string(),
            "dp_channel_src_port_min": global_cfg.dp_channel_src_port_min.unwrap().to_string(),
            "dp_channel_src_port_max": global_cfg.dp_channel_src_port_max.unwrap().to_string(),
            "dp_channel_probe_interval_ms": global_cfg.dp_channel_probe_interval_ms.unwrap().to_string(),
            "dp_channel_probe_fail_threshold": global_cfg.dp_channel_probe_fail_threshold.unwrap().to_string(),
        });

        let expected_vnet_route = VnetRouteTunnelTable {
            endpoint: vec![
                vdpu0_state_obj.dpu.pa_ipv4.clone(),
                vdpu1_state_obj.dpu.npu_ipv4.clone(),
            ],
            endpoint_monitor: Some(vec![
                vdpu0_state_obj.dpu.pa_ipv4.clone(),
                vdpu1_state_obj.dpu.pa_ipv4.clone(),
            ]),
            monitoring: None,
            primary: Some(vec!["true".to_string(), "false".to_string()]),
            rx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            tx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            check_directly_connected: Some(true),
        };

        let ha_set_actor = HaSetActor {
            id: ha_set_id.clone(),
            dash_ha_set_config: None,
            bridges: Vec::new(),
        };

        let handle = runtime.spawn(ha_set_actor, HaSetActor::name(), &ha_set_id);

        #[rustfmt::skip]
        let commands = [
            // Send DASH_HA_SET_CONFIG_TABLE config
            send! { key: HaSetActor::table_name(), data: { "key": HaSetActor::table_name(), "operation": "Set", "field_values": ha_set_cfg_fvs }, addr: runtime.sp("swss-common-bridge", HaSetActor::table_name()) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": true }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": true }, addr: runtime.sp(VDpuActor::name(), &vdpu1_id) },
            send! { key: "DASH_HA_GLOBAL_CONFIG", data: { "key": "DASH_HA_GLOBAL_CONFIG", "operation": "Set", "field_values": global_cfg_fvs } },
            // Simulate VDPU state update for vdpu0
            send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state, addr: runtime.sp("vdpu", &vdpu0_id) },
            // Simulate VDPU state update for vdpu1 (backup)
            send! { key: VDpuActorState::msg_key(&vdpu1_id), data: vdpu1_state, addr: runtime.sp("vdpu", &vdpu1_id) },
            // Verify that the DASH_HA_SET_TABLE was updated
            recv! { key: &ha_set_id, data: {"key": &ha_set_id,  "operation": "Set", "field_values": ha_set_obj}, addr: runtime.sp("swss-common-bridge", "DASH_HA_SET_TABLE") },
        ];

        test::run_commands(&runtime, runtime.sp(HaSetActor::name(), &ha_set_id), &commands).await;

        // todo: change below to a macro
        // Verify that the VNET_ROUTE_TUNNEL_TABLE was updated
        let db = crate::db_named("APPL_DB").await.unwrap();
        let table = Table::new(db, "VNET_ROUTE_TUNNEL_TABLE").unwrap();
        let mut route: VnetRouteTunnelTable =
            swss_serde::from_table(&table, &format!("default:{}", ha_set_cfg.vip_v4)).unwrap();
        assert_eq!(route, expected_vnet_route);

        let commands = [
            // simulate delete of ha-set entry
            send! { key: HaSetActor::table_name(), data: { "key": HaSetActor::table_name(), "operation": "Del", "field_values": ha_set_cfg_fvs }, addr: runtime.sp("swss-common-bridge", HaSetActor::table_name()) },
        ];

        test::run_commands(&runtime, runtime.sp(HaSetActor::name(), &ha_set_id), &commands).await;
        if tokio::time::timeout(Duration::from_secs(3), handle).await.is_err() {
            panic!("timeout waiting for actor to terminate");
        }
    }

    // test remote ha-set, when both vdpus are remote
    #[tokio::test]
    async fn remote_ha_set_actor() {
        // To enable trace, set ENABLE_TRACE=1 to run test
        sonic_common::log::init_logger_for_test();

        let redis = Redis::start_config_db();
        let runtime = test::create_actor_runtime(0, "10.0.0.0", "10::").await;

        //prepare test data
        let global_cfg = make_dash_ha_global_config();
        let global_cfg_fvs = serde_json::to_value(swss_serde::to_field_values(&global_cfg).unwrap()).unwrap();

        let (ha_set_id, ha_set_cfg) = make_dpu_scope_ha_set_config(2, 0);
        let ha_set_cfg_fvs = serde_json::to_value(swss_serde::to_field_values(&ha_set_cfg).unwrap()).unwrap();
        let dpu0 = make_remote_dpu_actor_state(2, 0);
        let dpu1 = make_remote_dpu_actor_state(3, 0);
        let (vdpu0_id, vdpu0_state_obj) = make_vdpu_actor_state(true, &dpu0);
        let (vdpu1_id, vdpu1_state_obj) = make_vdpu_actor_state(true, &dpu1);
        let vdpu0_state = serde_json::to_value(&vdpu0_state_obj).unwrap();
        let vdpu1_state = serde_json::to_value(&vdpu1_state_obj).unwrap();

        let expected_vnet_route = VnetRouteTunnelTable {
            endpoint: vec![
                vdpu0_state_obj.dpu.npu_ipv4.clone(),
                vdpu1_state_obj.dpu.npu_ipv4.clone(),
            ],
            endpoint_monitor: Some(vec![
                vdpu0_state_obj.dpu.pa_ipv4.clone(),
                vdpu1_state_obj.dpu.pa_ipv4.clone(),
            ]),
            monitoring: None,
            primary: Some(vec!["true".to_string(), "false".to_string()]),
            rx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            tx_monitor_timer: global_cfg.dpu_bfd_probe_interval_in_ms,
            check_directly_connected: Some(false),
        };

        let ha_set_actor = HaSetActor {
            id: ha_set_id.clone(),
            dash_ha_set_config: None,
            bridges: Vec::new(),
        };

        let handle = runtime.spawn(ha_set_actor, HaSetActor::name(), &ha_set_id);

        #[rustfmt::skip]
        let commands = [
            // Send DASH_HA_SET_CONFIG_TABLE config
            send! { key: HaSetActor::table_name(), data: { "key": HaSetActor::table_name(), "operation": "Set", "field_values": ha_set_cfg_fvs }, addr: runtime.sp("swss-common-bridge", HaSetActor::table_name()) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": true }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &ha_set_id), data: { "active": true }, addr: runtime.sp(VDpuActor::name(), &vdpu1_id) },
            send! { key: "DASH_HA_GLOBAL_CONFIG", data: { "key": "DASH_HA_GLOBAL_CONFIG", "operation": "Set", "field_values": global_cfg_fvs } },
            // Simulate VDPU state update for vdpu0
            send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state, addr: runtime.sp("vdpu", &vdpu0_id) },
            // Simulate VDPU state update for vdpu1 (backup)
            send! { key: VDpuActorState::msg_key(&vdpu1_id), data: vdpu1_state, addr: runtime.sp("vdpu", &vdpu1_id) },
        ];

        test::run_commands(&runtime, runtime.sp(HaSetActor::name(), &ha_set_id), &commands).await;

        // todo: change below to a macro
        // Verify that the VNET_ROUTE_TUNNEL_TABLE was updated
        let db = crate::db_named("APPL_DB").await.unwrap();
        let table = Table::new(db, "VNET_ROUTE_TUNNEL_TABLE").unwrap();
        let mut route: VnetRouteTunnelTable =
            swss_serde::from_table(&table, &format!("default:{}", ha_set_cfg.vip_v4)).unwrap();
        assert_eq!(route, expected_vnet_route);

        let commands = [
            // simulate delete of ha-set entry
            send! { key: HaSetActor::table_name(), data: { "key": HaSetActor::table_name(), "operation": "Del", "field_values": ha_set_cfg_fvs }, addr: runtime.sp("swss-common-bridge", HaSetActor::table_name()) },
        ];

        test::run_commands(&runtime, runtime.sp(HaSetActor::name(), &ha_set_id), &commands).await;
        if tokio::time::timeout(Duration::from_secs(3), handle).await.is_err() {
            panic!("timeout waiting for actor to terminate");
        }
    }
}
