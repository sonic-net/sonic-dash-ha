use crate::actors::vdpu::VDpuActor;
use crate::actors::{ha_set, spawn_consumer_bridge_for_actor, ActorCreator, DbBasedActor};
use crate::db_structs::*;
use crate::ha_actor_messages::{ActorRegistration, RegistrationType, VDpuActorState, HaSetActorState};
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

pub struct HaScopeActor {
    id: String,
    ha_scope_id: String,
    vdpu_id: String,
    dash_ha_scope_config: Option<DashHaScopeConfigTable>,
    bridges: Vec<ConsumerBridge>,
}

impl DbBasedActor for HaScopeActor {
    fn new(key: String) -> Result<Self> {
        if let Some((vdpu_id, ha_scope_id)) = key.split_once(':') {
            Ok(HaScopeActor {
                id: key.to_string(),
                vdpu_id: vdpu_id.to_string(),
                ha_scope_id: ha_scope_id.to_string(),
                dash_ha_scope_config: None,
                bridges: Vec::new(),
            })
        } else {
            Err(anyhow::anyhow!("Invalid key format for HA scope actor: {}", key))
        }
    }

    fn db_name() -> &'static str {
        "APPL_DB"
    }

    fn table_name() -> &'static str {
        "DASH_HA_SCOPE_CONFIG_TABLE"
    }

    fn name() -> &'static str {
        "ha-scope"
    }
}

impl HaScopeActor {
    // get vdpu data received via vdpu udpate
    fn get_vdpu(&self, incoming: &Incoming) -> Option<VDpuActorState> {
        let key = VDpuActorState::msg_key(&self.vdpu_id);
        let Ok(msg) = incoming.get(&key) else {
            return None;
        };
        msg.deserialize_data().ok()
    }

    fn get_haset(&self, incoming: &Incoming) -> Option<HaSetActorState> {
        let key = HaSetActorState::msg_key(&self.ha_scope_id);
        let Ok(msg) = incoming.get(&key) else {
            return None;
        };
        msg.deserialize_data().ok()
    }

    async fn register_to_vdpu_actor(&self, outgoing: &mut Outgoing, active: bool) -> Result<()> {
        let Some(ref dash_ha_scope_config) = self.dash_ha_scope_config else {
            return Ok(());
        };

        let msg = ActorRegistration::new_actor_msg(active, RegistrationType::VDPUState, &self.id)?;
        outgoing.send(outgoing.from_my_sp(VDpuActor::name(), &self.vdpu_id), msg);
        Ok(())
    }

    async fn register_to_haset_actor(&self, outgoing: &mut Outgoing, active: bool) -> Result<()> {
        let Some(ref dash_ha_scope_config) = self.dash_ha_scope_config else {
            return Ok(());
        };

        let msg = ActorRegistration::new_actor_msg(active, RegistrationType::HaSetState, &self.id)?;
        outgoing.send(outgoing.from_my_sp(HaScopeActor::name(), &self.ha_scope_id), msg);
        Ok(())
    }

    async fn update_dpu_dash_ha_scope_table(&self, incoming: &Incoming, outgoing: &mut Outgoing) -> Result<()> {
        let Some(dash_ha_scope_config) = self.dash_ha_scope_config.as_ref() else {
            return Ok(());
        };

        let Some(haset) = self.get_haset(incoming) else {
            debug!("HA-SET {} has not been received. Skip dash_ha_scope update", &self.ha_scope_id);
            return Ok(());
        };

        let dash_ha_scope = DashHaScopeTable {
            version: dash_ha_scope_config.version.clone(),
            disable: dash_ha_scope_config.disable,
            ha_role: dash_ha_scope_config.desired_ha_state.clone(), /*todo, how switching_to_active is derived */
            flow_reconcile_requested: false, /*todo */
            activate_role_requested: false, /*todo, where exactly is this from */
        };

        let fv = swss_serde::to_field_values(&dash_ha_scope)?;
        let kfv = KeyOpFieldValues {
            key: self.ha_scope_id.clone(),
            operation: KeyOperation::Set,
            field_values: fv,
        };

        let msg = ActorMessage::new(self.id.clone(), &kfv)?;
        outgoing.send(outgoing.from_my_sp("swss-common-bridge", "DASH_HA_SCOPE_TABLE"), msg);

        Ok(())
    }

    /// Update the DASH_HA_SCOPE_STATE in NPU. This is the notification channel to SDN controller. Its primary sources of data are
    /// - DASH_HA_SCOPE_CONFIG_TABLE
    /// - VDPU state
    /// - HA-SET state
    /// - DPU HA-SCOPE state
    /// 
    fn update_npu_ha_scope_state(
        &self,
        state: &mut State,
    ) -> Result<()> {
        let Some(ref dash_ha_scope_config) = self.dash_ha_scope_config else {
            return Ok(());
        };

        let (internal, incoming, outgoing) = state.get_all();

        let Some(vdpu) = self.get_vdpu(incoming) else {
            debug!("vDPU {} has not been received. Skip DASH_HA_SCOPE_STATE update", &self.vdpu_id);
            return Ok(());
        };


        let dash_ha_scope_state = NpuDashHaScopeState {
            version: dash_ha_scope_config.version.clone(),
            ha_scope_id: self.ha_scope_id.clone(),
            ha_scope_name: dash_ha_scope_config.ha_scope_name.clone(),
            ha_role: dash_ha_scope_config.desired_ha_state.clone(),
            ha_state: vdpu.ha_state.clone(),
            ha_role_change_count: 0, /*todo */
            ha_role_change_timestamp: SystemTime::now(), /*todo */
            ha_role_change_error: String::new(), /*todo */
            ha_role_change_error_timestamp: SystemTime::now(), /*todo */
            ha_role_change_error_count: 0, /*todo */
            ha_role_change_error_last: String::new(), /*todo */
            ha_role_change_error_last_timestamp: SystemTime::now(), /*todo */
            ha_role_change_error_last_count: 0, /*todo */
            ha_role_change_error_last_2: String::new(), /*todo */
            ha_role_change_error_last_timestamp_2: SystemTime::now(), /*todo */
            ha_role_change_error_last_count_2: 0, /*todo */
            ha_role_change_error_last_3: String::new(), /*todo */
            ha_role_change_error_last_timestamp_3: SystemTime::now(), /*todo */
            ha_role_change_error_last_count_3: 0, /*todo */
            ha_role_change_error_last_4: String::new(), /*todo */
            ha_role_change_error_last_timestamp_4: SystemTime::now(), /*todo */
            ha_role_change_error_last_count_4: 0, /*todo */
            ha_role_change_error_last_5: String::new(), /*todo */
            ha_role_change_error_last_timestamp_5: SystemTime::now(), /*todo */
            ha_role_change_error_last_count_5: 0, /*todo */
            ha_role_change_error_last_6: String::new(), /*todo */
            ha_role_change_error_last_timestamp_6: SystemTime::now(), /*todo */
            ha_role_change_error_last_count_6: 0, /*todo */
            ha_role_change_error_last_7: String::new(), /*todo */
            ha_role_change_error_last_timestamp_7: SystemTime::now(), /*todo */
            ha_role_change_error_last_count_7: 0, /*todo */
            ha_role_change_error_last_8: String::new(), /*todo */
            ha_role_change_error_last
        };

        let fvs = swss_serde::to_field_values(&dash_ha_scope_state)?;

        internal.get_mut("DASH_HA_SCOPE_STATE").clone_from(&fvs);

        Ok(())
    }
    /// Handles updates to the DASH_HA_SCOPE_CONFIG_TABLE.
    /// Updates the actor's internal config and performs any necessary initialization or subscriptions.
    async fn handle_dash_ha_scope_config_table_message(
        &mut self,
        state: &mut State,
        key: &str,
        context: &mut Context,
    ) -> Result<()> {
        let (internal, incoming, outgoing) = state.get_all();

        // Retrieve the config update from the incoming message
        let kfv: KeyOpFieldValues = incoming.get(key)?.deserialize_data()?;

        if kfv.operation == KeyOperation::Del {
            // unregister from the vDPU Actor and ha-set actor
            self.register_to_vdpu_actor(outgoing, false).await?;
            self.register_to_haset_actor(outgoing, false).await?;
            context.stop();
            return Ok(());
        }
        let first_time = self.dash_ha_scope_config.is_none();

        // Update internal config
        self.dash_ha_scope_config = Some(swss_serde::from_field_values(&kfv.field_values)?);

        // create an internal entry for npu STATE_DB/DASH_HA_SCOPE_STATE, which will be the 
        // notification channel to SDN controller
        let swss_key = format!("{}:{}", self.vdpu_id, self.ha_scope_id);
        if !internal.has_entry(key, &swss_key) {
            let db = crate::db_named("STATE_DB").await?;
            let table = Table::new_async(db, "DASH_HA_SCOPE_STATE").await?;
            internal.add("DASH_HA_SCOPE_STATE", table, swss_key).await;
        }

        // Subscribe to the vDPU Actor for state updates.
        self.register_to_vdpu_actor(outgoing, true).await?;
        // Subscribe to the ha-set Actor for state updates.
        self.register_to_haset_actor(outgoing, true).await?;

        if first_time {
            // subscribe to DASH_HA_SCOPE_STATE
            self.bridges.push(
                spawn_consumer_bridge_for_actor(
                    context.get_edge_runtime().clone(),
                    "DPU_STATE_DB",
                    "DASH_HA_SCOPE_STATE",
                    Self::name(),
                    Some(&self.id),
                    true,
                )
                .await?,
            );
        }
        
        // update the DASH_HA_SCOPE_STATE in DPU
        self.update_dpu_dash_ha_scope_table(incoming, outgoing).await?;
        Ok(())
    }

    /// Handles VDPU state update messages for this HA scope.
    async fn handle_vdpu_state_update(
        &mut self,
        state: &mut State,
        key: &str,
        context: &mut Context,
    ) -> Result<()> {

        self.update_npu_ha_scope_state(state)?;
        Ok(())
    }

    /// Handles HaSet state update messages for this HA scope.
    async fn handle_haset_state_update(
        &mut self,
        state: &mut State,
        key: &str,
        context: &mut Context,
    ) -> Result<()> {
        
        self.update_npu_ha_scope_state(state)?;
        Ok(())
    }

    async fn handle_dpu_ha_scope_state_update(
        &mut self,
        state: &mut State,
        key: &str,
        context: &mut Context,
    ) -> Result<()> {

        self.update_npu_ha_scope_state(state)?;
        Ok(())
    }
}

impl Actor for HaScopeActor {
    async fn handle_message(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        println!("Received message {key}");
        if key == Self::table_name() {
            if let Err(e) = self.handle_dash_ha_scope_config_table_message(state, key, context).await {
                let err = format!("handle_dash_ha_set_config_table_message failed: {e}");
                println!("{}", err);
            }
            return Ok(());
        }

        if self.dash_ha_scope_config.is_none() {
            return Ok(());
        }

        if VDpuActorState::is_my_msg(key) {
            return self.handle_vdpu_state_update(state, key, context).await;
        } if HaSetActorState::is_my_msg(key) {
            return self.handle_haset_state_update(state, key, context).await;
        } if key.starts_with("DASH_HA_SCOPE_STATE") {
            // dpu ha scope state update
            return self.handle_dpu_ha_scope_state_update(state, key, context).await;
        }

        Ok(())
    }
}
