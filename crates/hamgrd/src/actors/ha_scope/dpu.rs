use crate::actors::{spawn_consumer_bridge_for_actor, DbBasedActor};
use crate::db_structs::*;
use crate::ha_actor_messages::*;
use anyhow::Result;
use sonic_common::SonicDbTable;
use sonic_dash_api_proto::decode_from_field_values;
use sonic_dash_api_proto::ha_scope_config::{DesiredHaState, HaScopeConfig};
use swbus_actor::{ActorMessage, Context, State};
use swss_common::{KeyOpFieldValues, KeyOperation};
use tracing::{debug, error};
use uuid::Uuid;

use super::base::HaScopeBase;
use super::HaScopeActor;

pub struct DpuHaScopeActor {
    pub(super) base: HaScopeBase,
}

impl DpuHaScopeActor {
    /// Main message dispatch for DPU-driven mode
    pub async fn handle_message_inner(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        if key == HaScopeActor::table_name() {
            return self.handle_dash_ha_scope_config_table_message_dpu_driven(state, key);
        }
        if VDpuActorState::is_my_msg(key) {
            return self.handle_vdpu_state_update(state, context).await;
        }
        if HaSetActorState::is_my_msg(key) {
            return self.handle_haset_state_update(state);
        }
        if key.starts_with(DpuDashHaScopeState::table_name()) {
            // dpu ha scope state update
            return self.handle_dpu_ha_scope_state_update(state);
        }
        Ok(())
    }

    /// Handles updates to the DASH_HA_SCOPE_CONFIG_TABLE in the case of DPU-driven HA.
    /// Updates the actor's internal config and performs any necessary initialization or subscriptions.
    /// Update DPU DASH_HA_SCOPE_TABLE
    /// Update NPU DASH_HA_SCOPE_STATE if approved_pending_operation_ids is not empty
    fn handle_dash_ha_scope_config_table_message_dpu_driven(&mut self, state: &mut State, key: &str) -> Result<()> {
        let (_internal, incoming, _outgoing) = state.get_all();

        // Retrieve the config update from the incoming message
        let kfv: KeyOpFieldValues = incoming.get_or_fail(key)?.deserialize_data()?;
        let dash_ha_scope_config: HaScopeConfig = decode_from_field_values(&kfv.field_values)?;

        // Update internal config
        self.base.dash_ha_scope_config = Some(dash_ha_scope_config);

        // this is not a ha_scope for the target vDPU. Skip
        if !self.base.vdpu_is_managed(incoming) {
            return Ok(());
        }

        // update the DASH_HA_SCOPE_TABLE in DPU
        self.update_dpu_ha_scope_table(state)?;

        // update the NPU DASH_HA_SCOPE_STATE because some fields are derived from dash_ha_scope_config
        self.update_npu_ha_scope_state_ha_state(state)?;

        // need to update operation list if approved_pending_operation_ids is not empty
        let approved_pending_operation_ids = self
            .base
            .dash_ha_scope_config
            .as_ref()
            .unwrap()
            .approved_pending_operation_ids
            .clone();

        if !approved_pending_operation_ids.is_empty() {
            self.base.update_npu_ha_scope_state_pending_operations(
                state,
                Vec::new(),
                approved_pending_operation_ids,
            )?;
        }

        Ok(())
    }

    /// Handles VDPU state update messages for this HA scope.
    /// If the vdpu is unmanaged, the actor is put in dormant state. Otherwise, the actor subscribes to the
    /// DASH_HA_SCOPE_STATE table and updates the NPU HA scope state.
    async fn handle_vdpu_state_update(&mut self, state: &mut State, context: &mut Context) -> Result<()> {
        let (internal, incoming, _outgoing) = state.get_all();
        let Some(vdpu) = self.base.get_vdpu(incoming) else {
            error!("Failed to retrieve vDPU {} from incoming state", &self.base.vdpu_id);
            return Ok(());
        };

        if !vdpu.dpu.is_managed {
            debug!("vDPU {} is unmanaged. Put actor in dormant state", &self.base.vdpu_id);
            return Ok(());
        }

        // create an internal entry for npu STATE_DB/DASH_HA_SCOPE_STATE, which will be the
        // notification channel to SDN controller
        let swss_key = format!(
            "{}{}{}",
            self.base.vdpu_id,
            NpuDashHaScopeState::key_separator(),
            self.base.ha_scope_id
        );
        if !internal.has_entry(NpuDashHaScopeState::table_name(), &swss_key) {
            let db = crate::db_for_table::<NpuDashHaScopeState>().await?;
            let table = swss_common::Table::new_async(db, NpuDashHaScopeState::table_name()).await?;
            internal.add(NpuDashHaScopeState::table_name(), table, swss_key).await;
        }

        if self.base.bridges.is_empty() {
            // subscribe to dpu DASH_HA_SCOPE_STATE
            self.base.bridges.push(
                spawn_consumer_bridge_for_actor::<DpuDashHaScopeState>(
                    context.get_edge_runtime().clone(),
                    HaScopeActor::name(),
                    Some(&self.base.id),
                    true,
                )
                .await?,
            );
        }
        // ha_scope_table in dpu has no info derived from vDPU but it won't be programed until we receive vDPU which confirms the vDPU is managed
        self.update_dpu_ha_scope_table(state)?;
        self.base.update_npu_ha_scope_state_base(state)?;
        Ok(())
    }

    /// Handles HaSet state update messages for this HA scope.
    /// Update NPU DASH_HA_SCOPE_STATE
    fn handle_haset_state_update(&mut self, state: &mut State) -> Result<()> {
        // the ha_scope is not managing the target vDPU. Skip
        let incoming = state.incoming();
        if !self.base.vdpu_is_managed(incoming) {
            return Ok(());
        }

        // ha_scope vip_v4 and vip_v6 are derived from ha_set
        self.update_dpu_ha_scope_table(state)?;
        self.base.update_npu_ha_scope_state_base(state)?;
        Ok(())
    }

    /// Handles DPU DASH_HA_SCOPE_STATE update messages for this HA scope.
    /// Update NPU DASH_HA_SCOPE_STATE ha_state related fields
    /// Update NPU DASH_HA_SCOPE_STATE pending operation list if there are new operations requested by DPU
    fn handle_dpu_ha_scope_state_update(&mut self, state: &mut State) -> Result<()> {
        let (_internal, incoming, _) = state.get_all();
        // calculate operation requested by dpu
        let Some(new_dpu_ha_scope_state) = self.base.get_dpu_ha_scope_state(incoming) else {
            // no valid state received from dpu, skip
            return Ok(());
        };
        let mut operations: Vec<(String, String)> = Vec::new();

        // if hamgrd is restarted, we will lose the cached old state. In this case, we will treat
        // all pending operations as new and request the sdn controller via npu dash_ha_scope_state
        // to take action. If these have been notified to sdn controller prior to hamgrd restart,
        // they will be no change to dash_ha_scope_state and no action will be taken by sdn controller.
        let old_dpu_ha_scope_state = self.base.dpu_ha_scope_state.as_ref().cloned().unwrap_or_default();
        if new_dpu_ha_scope_state.activate_role_pending && !old_dpu_ha_scope_state.activate_role_pending {
            operations.push((Uuid::new_v4().to_string(), "activate_role".to_string()));
        }

        if new_dpu_ha_scope_state.brainsplit_recover_pending && !old_dpu_ha_scope_state.brainsplit_recover_pending {
            operations.push((Uuid::new_v4().to_string(), "brainsplit_recover".to_string()));
        }

        if new_dpu_ha_scope_state.flow_reconcile_pending && !old_dpu_ha_scope_state.flow_reconcile_pending {
            operations.push((Uuid::new_v4().to_string(), "flow_reconcile".to_string()));
        }

        self.base.dpu_ha_scope_state = Some(new_dpu_ha_scope_state);

        self.update_npu_ha_scope_state_ha_state(state)?;

        if !operations.is_empty() {
            self.base
                .update_npu_ha_scope_state_pending_operations(state, operations, Vec::new())?;
        }

        Ok(())
    }

    /// Update DPU HA Scope Table purely based on HA Scope & HA Set Config, no flex parameters
    /// Used in DPU-driven mode
    fn update_dpu_ha_scope_table(&self, state: &mut State) -> Result<()> {
        let Some(dash_ha_scope_config) = self.base.dash_ha_scope_config.as_ref() else {
            return Ok(());
        };

        let (internal, incoming, outgoing) = state.get_all();

        let ha_set_id = self.base.get_haset_id().unwrap();
        let Some(haset) = self.base.get_haset(incoming) else {
            debug!(
                "HA-SET {} has not been received. Skip DASH_HA_SCOPE_TABLE update",
                &ha_set_id
            );
            return Ok(());
        };

        let mut activate_role_requested = false;
        let mut flow_reconcile_requested = false;
        let approved_ops = dash_ha_scope_config.approved_pending_operation_ids.clone();
        if !approved_ops.is_empty() {
            let pending_operations = self.base.get_pending_operations(internal, None)?;
            for op_id in approved_ops {
                let Some(op) = pending_operations.get(&op_id) else {
                    // has been removed from pending list
                    continue;
                };
                match op.as_str() {
                    "switchover" => {
                        // todo: this is for switch driven ha
                    }
                    "activate_role" => {
                        activate_role_requested = true;
                    }
                    "flow_reconcile" => {
                        flow_reconcile_requested = true;
                    }
                    "brainsplit_recover" => {
                        // todo: what's the action here?
                    }
                    _ => {
                        error!("Unknown operation type {}", op);
                    }
                }
            }
        }

        let dash_ha_scope = DashHaScopeTable {
            version: dash_ha_scope_config.version.parse().unwrap(),
            disabled: dash_ha_scope_config.disabled,
            ha_set_id: dash_ha_scope_config.ha_set_id.clone(),
            vip_v4: haset.ha_set.vip_v4.clone(),
            vip_v6: haset.ha_set.vip_v6.clone(),
            ha_role: if dash_ha_scope_config.desired_ha_state == DesiredHaState::Unspecified as i32 {
                "standby".to_string()
            } else {
                format!(
                    "{}",
                    DesiredHaState::try_from(dash_ha_scope_config.desired_ha_state).unwrap()
                )
                .to_lowercase()
            }, /*todo, how switching_to_active is derived. Is it relevant to dpu driven mode */
            ha_term: "0".to_string(), // TODO: not clear what need to be done for DPU-driven mode
            flow_reconcile_requested,
            activate_role_requested,
        };

        let fv = swss_serde::to_field_values(&dash_ha_scope)?;
        let kfv = KeyOpFieldValues {
            key: self.base.ha_scope_id.clone(),
            operation: KeyOperation::Set,
            field_values: fv,
        };

        let msg = ActorMessage::new(self.base.ha_scope_id.clone(), &kfv)?;
        outgoing.send(outgoing.common_bridge_sp::<DashHaScopeTable>(), msg);

        Ok(())
    }

    fn update_npu_ha_scope_state_ha_state(&self, state: &mut State) -> Result<()> {
        let Some(ref dash_ha_scope_config) = self.base.dash_ha_scope_config else {
            return Ok(());
        };
        let (internal, incoming, _outgoing) = state.get_all();

        let Some(mut npu_ha_scope_state) = self.base.get_npu_ha_scope_state(internal) else {
            tracing::info!("Cannot update STATE_DB/DASH_HA_SCOPE_STATE until it is populated with basic information",);
            return Ok(());
        };

        let Some(dpu_ha_scope_state) = self.base.get_dpu_ha_scope_state(incoming) else {
            debug!(
                "DPU HA-SCOPE STATE {} is corrupted or has not been received. Skip DASH_HA_SCOPE_STATE update",
                &self.base.id
            );
            return Ok(());
        };

        // in dpu driven mode, local_ha_state is same as dpu acked ha state
        npu_ha_scope_state.local_ha_state = Some(dpu_ha_scope_state.ha_state.clone());
        npu_ha_scope_state.local_ha_state_last_updated_time_in_ms = Some(dpu_ha_scope_state.ha_role_start_time);
        // The reason of the last HA state change.
        npu_ha_scope_state.local_ha_state_last_updated_reason = Some("dpu initiated".to_string());

        // The target HA state in ASIC. This is the state that hamgrd generates and asking DPU to move to.
        npu_ha_scope_state.local_target_asic_ha_state = Some(
            format!(
                "{}",
                DesiredHaState::try_from(dash_ha_scope_config.desired_ha_state).unwrap()
            )
            .to_lowercase(),
        );
        // The HA state that ASIC acked.
        npu_ha_scope_state.local_acked_asic_ha_state = Some(dpu_ha_scope_state.ha_state.clone());

        // The current target term of the HA state machine. in dpu-driven mode, use the term acked by asic
        npu_ha_scope_state.local_target_term = Some(dpu_ha_scope_state.ha_term.clone());
        npu_ha_scope_state.local_acked_term = Some(dpu_ha_scope_state.ha_term);

        let fvs = swss_serde::to_field_values(&npu_ha_scope_state)?;
        internal.get_mut(NpuDashHaScopeState::table_name()).clone_from(&fvs);

        Ok(())
    }
}
