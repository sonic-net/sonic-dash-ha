use crate::actors::vdpu::VDpuActor;
use crate::actors::{spawn_consumer_bridge_for_actor, ActorCreator};
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
use swss_common_bridge::{consumer::spawn_consumer_bridge, producer::spawn_producer_bridge};

pub struct HaSetActor {
    id: String,
    dash_ha_set_config: Option<DashHaSetConfigTable>,
}
impl HaSetActor {
    pub fn new(key: String) -> Result<Self> {
        let actor = HaSetActor {
            id: key,
            dash_ha_set_config: None,
        };
        Ok(actor)
    }

    pub fn table_name() -> &'static str {
        "DASH_HA_SET_CONFIG_TABLE"
    }

    pub fn name() -> &'static str {
        "ha-set"
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
        spawn_consumer_bridge(
            edge_runtime.clone(),
            addr,
            sst,
            |kfv: &KeyOpFieldValues| (crate::sp(Self::name(), &kfv.key), Self::table_name().to_owned()),
            |_| true,
        );

        Ok(())
    }

    pub async fn init_supporting_services(edge_runtime: &Arc<SwbusEdgeRuntime>) -> Result<()> {
        Ok(())
    }

    fn update_dash_ha_set_table(&self, incoming: &Incoming, outgoing: &mut Outgoing) -> Result<()> {
        let Some(dash_ha_set_config) = self.dash_ha_set_config.as_ref() else {
            return Ok(());
        };

        let global_cfg: DashHaGlobalConfig = incoming.get("DASH_HA_GLOBAL_CONFIG")?.deserialize_data()?;
        let version = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let dash_ha_set = DashHaSetTable {
            version: Some(version.to_string()),
            vip_v4: dash_ha_set_config.vip_v4.clone(),
            vip_v6: dash_ha_set_config.vip_v6.clone(),
            owner: dash_ha_set_config.owner.clone(),
            scope: dash_ha_set_config.scope.clone(),
            local_npu_ip: None, // todo: need it from dpu table
            local_ip: None,     // todo: need it from dpu table
            peer_ip: None,      // todo: need it from remote-dpu table
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

    fn update_vnet_route_tunnel_table(&self, incoming: &Incoming, internal: &Internal) -> Result<()> {
        todo!("Update VNET_ROUTE_TUNNEL_TABLE with local nexthop ip and remote npu_ip with dpu_ip for monir");
        // get DPU info in vdpu state update message

        // for each DPU, create an VNET_ROUTE_TUNNEL_TABLE entry
        Ok(())
    }
    async fn register_to_vdpu_actor(&self, outgoing: &mut Outgoing, active: bool) -> Result<()> {
        if self.dash_ha_set_config.is_none() {
            return Ok(());
        }
        // todo: we can't assume vdpus are local. We need to find out DPU ID to vDPU ID mapping then based on DPU to know the node id.
        if let Some(main_dpu_ids) = &self.dash_ha_set_config.as_ref().unwrap().vdpu_ids {
            let msg = ActorRegistration::new_actor_msg(active, RegistrationType::VDPUState, &self.id)?;
            main_dpu_ids
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .for_each(|id| {
                    outgoing.send(outgoing.from_my_sp(VDpuActor::name(), id), msg.clone());
                });
        }
        Ok(())
    }

    fn calculate_ha_set_state(&self, incoming: &Incoming) -> bool {
        if let Some(vdpu_ids) = &self.dash_ha_set_config.as_ref().unwrap().vdpu_ids {
            vdpu_ids.split(',').map(str::trim).filter(|s| !s.is_empty()).all(|id| {
                let msg = incoming.get(&format!("{}{}", VDpuActorState::msg_key_prefix(), id));
                if msg.is_err() {
                    return false;
                }
                let msg = msg.unwrap();
                if let Ok(VDpuActorState { up, dpus: _ }) = msg.deserialize_data() {
                    up
                } else {
                    false
                }
            })
        } else {
            false
        }
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
            // @todo: need to destroy DASH_HA_GLOBAL_CONFIG consumer bridge
            context.stop();
            return Ok(());
        }

        if self.dash_ha_set_config.is_none() {
            spawn_consumer_bridge_for_actor(
                context.get_edge_runtime().clone(),
                "CONFIG_DB",
                "DASH_HA_GLOBAL_CONFIG",
                Self::name(),
                Some(&self.id),
                true,
            )
            .await?;
        }

        self.dash_ha_set_config = Some(swss_serde::from_field_values(&dpu_kfv.field_values)?);
        // todo: do we need to collect data from vdpu update?
        self.update_dash_ha_set_table(incoming, outgoing)?;

        // Subscribe to the DPU Actor for state updates
        self.register_to_vdpu_actor(outgoing, true).await?;
        Ok(())
    }

    async fn handle_dash_ha_global_config(
        &mut self,
        state: &mut State,
        key: &str,
        context: &mut Context,
    ) -> Result<()> {
        let (internal, incoming, outgoing) = state.get_all();
        // global config update affects Vxlan tunnel and dash-ha-set in DPU
        self.update_dash_ha_set_table(incoming, outgoing)?;
        self.update_vnet_route_tunnel_table(incoming, internal)?;
        Ok(())
    }

    async fn handle_vdpu_state_update(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        let (internal, incoming, outgoing) = state.get_all();
        // vdpu update affects dash-ha-set in DPU and vxlan tunnel
        self.update_dash_ha_set_table(incoming, outgoing)?;
        self.update_vnet_route_tunnel_table(incoming, internal)?;
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
