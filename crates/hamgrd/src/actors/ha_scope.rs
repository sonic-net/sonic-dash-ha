use crate::actors::{spawn_consumer_bridge_for_actor, DbBasedActor};
use crate::db_structs::*;
use crate::ha_actor_messages::{ActorRegistration, HaSetActorState, RegistrationType, VDpuActorState};
use crate::{HaSetActor, VDpuActor};
use anyhow::Result;
use sonic_common::SonicDbTable;
use sonic_dash_api_proto::decode_from_field_values;
use sonic_dash_api_proto::ha_scope_config::{DesiredHaState, HaScopeConfig};
use std::collections::HashMap;
use swbus_actor::{
    state::{incoming::Incoming, internal::Internal, outgoing::Outgoing},
    Actor, ActorMessage, Context, State,
};
use swss_common::Table;
use swss_common::{KeyOpFieldValues, KeyOperation};
use swss_common_bridge::consumer::ConsumerBridge;
use tracing::{debug, error, info, instrument};
use uuid::Uuid;

pub struct HaScopeActor {
    id: String,
    ha_scope_id: String,
    vdpu_id: String,
    dash_ha_scope_config: Option<HaScopeConfig>,
    bridges: Vec<ConsumerBridge>,
    // we need to keep track the previous dpu_ha_scope_state to detect state change
    dpu_ha_scope_state: Option<DpuDashHaScopeState>,
}

impl DbBasedActor for HaScopeActor {
    fn new(key: String) -> Result<Self> {
        if let Some((vdpu_id, ha_scope_id)) = key.split_once(HaScopeConfig::key_separator()) {
            Ok(HaScopeActor {
                id: key.to_string(),
                vdpu_id: vdpu_id.to_string(),
                ha_scope_id: ha_scope_id.to_string(),
                dash_ha_scope_config: None,
                bridges: Vec::new(),
                dpu_ha_scope_state: None,
            })
        } else {
            Err(anyhow::anyhow!("Invalid key format for HA scope actor: {}", key))
        }
    }

    fn table_name() -> &'static str {
        HaScopeConfig::table_name()
    }

    fn name() -> &'static str {
        "ha-scope"
    }
}

// Implements getter helper functions for HaScopeActor
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
        let ha_set_id = self.get_haset_id()?;

        let key = HaSetActorState::msg_key(&ha_set_id);
        let Ok(msg) = incoming.get(&key) else {
            return None;
        };
        msg.deserialize_data().ok()
    }

    fn get_haset_id(&self) -> Option<String> {
        let dash_ha_scope_config = self.dash_ha_scope_config.as_ref()?;
        Some(dash_ha_scope_config.ha_set_id.clone())
    }

    fn get_dpu_ha_scope_state(&self, incoming: &Incoming) -> Option<DpuDashHaScopeState> {
        let Ok(msg) = incoming.get(DpuDashHaScopeState::table_name()) else {
            return None;
        };
        let kfv = match msg.deserialize_data::<KeyOpFieldValues>() {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to deserialize DASH_HA_SCOPE_STATE KeyOpFieldValues: {}", e);
                return None;
            }
        };

        match swss_serde::from_field_values(&kfv.field_values) {
            Ok(state) => Some(state),
            Err(e) => {
                error!("Failed to deserialize DASH_HA_SCOPE_STATE from field values: {}", e);
                None
            }
        }
    }

    fn get_npu_ha_scope_state(&self, internal: &Internal) -> Option<NpuDashHaScopeState> {
        // check dash_ha_scope_config is not none to make sure DASH_HA_SCOPE_CONFIG_TABLE has received
        self.dash_ha_scope_config.as_ref()?;

        let fvs = internal.get(NpuDashHaScopeState::table_name());

        if fvs.is_empty() {
            // not created yet
            return None;
        }

        match swss_serde::from_field_values(fvs) {
            Ok(state) => Some(state),
            Err(e) => {
                error!("Failed to deserialize DASH_HA_SCOPE_STATE from field values: {}", e);
                None
            }
        }
    }

    fn get_pending_operations(
        &self,
        internal: &Internal,
        npu_ha_scope_state: Option<&NpuDashHaScopeState>,
    ) -> Result<HashMap<String, String>> {
        let npu_ha_scope_state = match npu_ha_scope_state {
            Some(state) => state,
            None => {
                let fvs = internal.get(NpuDashHaScopeState::table_name());
                if fvs.is_empty() {
                    // not created yet
                    return Ok(HashMap::new());
                }
                &swss_serde::from_field_values(fvs)?
            }
        };

        let pending_operation_ids = &npu_ha_scope_state.pending_operation_ids;
        let pending_operation_types = &npu_ha_scope_state.pending_operation_types;
        if let (Some(pending_operation_ids), Some(pending_operation_types)) =
            (pending_operation_ids, pending_operation_types)
        {
            if pending_operation_ids.len() != pending_operation_types.len() {
                return Err(anyhow::anyhow!(
                    "pending_operation_ids and pending_operation_types have different lengths"
                ));
            }
            let operations = pending_operation_ids
                .iter()
                .zip(pending_operation_types.iter())
                .map(|(id, op)| (id.clone(), op.clone()))
                .collect();
            Ok(operations)
        } else {
            Ok(HashMap::new())
        }
    }

    fn vdpu_is_managed(&self, incoming: &Incoming) -> bool {
        let Some(vdpu) = self.get_vdpu(incoming) else {
            return false;
        };
        vdpu.dpu.is_managed
    }
}

// Implements internal action functions for HaScopeActor
impl HaScopeActor {
    fn register_to_vdpu_actor(&self, outgoing: &mut Outgoing, active: bool) -> Result<()> {
        if self.dash_ha_scope_config.is_none() {
            return Ok(());
        };

        let msg = ActorRegistration::new_actor_msg(active, RegistrationType::VDPUState, &self.id)?;
        outgoing.send(outgoing.from_my_sp(VDpuActor::name(), &self.vdpu_id), msg);
        Ok(())
    }

    fn register_to_haset_actor(&self, outgoing: &mut Outgoing, active: bool) -> Result<()> {
        if self.dash_ha_scope_config.is_none() {
            return Ok(());
        };
        let Some(ha_set_id) = self.get_haset_id() else {
            return Ok(());
        };
        let msg = ActorRegistration::new_actor_msg(active, RegistrationType::HaSetState, &self.id)?;
        outgoing.send(outgoing.from_my_sp(HaSetActor::name(), &ha_set_id), msg);
        Ok(())
    }

    fn delete_dash_ha_scope_table(&self, outgoing: &mut Outgoing) -> Result<()> {
        let kfv = KeyOpFieldValues {
            key: self.ha_scope_id.clone(),
            operation: KeyOperation::Del,
            field_values: HashMap::new(),
        };

        let msg = ActorMessage::new(self.ha_scope_id.clone(), &kfv)?;
        outgoing.send(outgoing.common_bridge_sp::<DashHaScopeTable>(), msg);

        Ok(())
    }

    fn delete_npu_ha_scope_state(&self, internal: &mut Internal) -> Result<()> {
        if self.dash_ha_scope_config.is_none() {
            return Ok(());
        };

        internal.delete(NpuDashHaScopeState::table_name());

        Ok(())
    }

    fn do_cleanup(&mut self, state: &mut State) -> Result<()> {
        let (internal, _incoming, outgoing) = state.get_all();
        self.delete_dash_ha_scope_table(outgoing)?;
        self.delete_npu_ha_scope_state(internal)?;
        self.register_to_vdpu_actor(outgoing, false)?;
        self.register_to_haset_actor(outgoing, false)?;
        Ok(())
    }

    fn update_dpu_ha_scope_table(&self, state: &mut State) -> Result<()> {
        let Some(dash_ha_scope_config) = self.dash_ha_scope_config.as_ref() else {
            return Ok(());
        };

        let (internal, incoming, outgoing) = state.get_all();

        let ha_set_id = self.get_haset_id().unwrap();
        let Some(haset) = self.get_haset(incoming) else {
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
            let pending_operations = self.get_pending_operations(internal, None)?;
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
            flow_reconcile_requested,
            activate_role_requested,
        };

        let fv = swss_serde::to_field_values(&dash_ha_scope)?;
        let kfv = KeyOpFieldValues {
            key: self.ha_scope_id.clone(),
            operation: KeyOperation::Set,
            field_values: fv,
        };

        let msg = ActorMessage::new(self.ha_scope_id.clone(), &kfv)?;
        outgoing.send(outgoing.common_bridge_sp::<DashHaScopeTable>(), msg);

        Ok(())
    }

    fn update_npu_ha_scope_state_base(&self, state: &mut State) -> Result<()> {
        if self.dash_ha_scope_config.is_none() {
            return Ok(());
        };

        let (internal, incoming, _outgoing) = state.get_all();

        let Some(vdpu) = self.get_vdpu(incoming) else {
            debug!(
                "vDPU {} has not been received. Skip DASH_HA_SCOPE_STATE update",
                &self.vdpu_id
            );
            return Ok(());
        };

        if !vdpu.dpu.is_managed {
            debug!("vDPU {} is unmanaged. Skip DASH_HA_SCOPE_STATE update", &self.vdpu_id);
            return Ok(());
        }

        let pmon_state = vdpu.dpu.dpu_pmon_state.unwrap_or_default();
        let bfd_state = vdpu.dpu.dpu_bfd_state.unwrap_or_default();

        let ha_set_id = self.get_haset_id().unwrap();

        let Some(haset) = self.get_haset(incoming) else {
            debug!(
                "HA-SET {} has not been received. Skip DASH_HA_SCOPE_STATE update",
                &ha_set_id
            );
            return Ok(());
        };

        let mut npu_ha_scope_state = self.get_npu_ha_scope_state(internal).unwrap_or_default();

        npu_ha_scope_state.creation_time_in_ms = 0; /*todo */
        npu_ha_scope_state.last_heartbeat_time_in_ms = 0; /* todo: wait until heartbeat is implemented */
        npu_ha_scope_state.vip_v4 = haset.ha_set.vip_v4.clone();
        npu_ha_scope_state.vip_v6 = haset.ha_set.vip_v6.clone();
        npu_ha_scope_state.local_ip = haset.ha_set.local_ip.clone();
        npu_ha_scope_state.peer_ip = haset.ha_set.peer_ip.clone();

        // The state of local vDPU midplane. The value can be "unknown", "up", "down".
        npu_ha_scope_state.local_vdpu_midplane_state = pmon_state.dpu_midplane_link_state;

        // Local vDPU midplane state last updated time in milliseconds.
        npu_ha_scope_state.local_vdpu_midplane_state_last_updated_time_in_ms = pmon_state.dpu_midplane_link_time;
        // The state of local vDPU control plane, which includes DPU OS and certain required firmware. The value can be "unknown", "up", "down".
        npu_ha_scope_state.local_vdpu_control_plane_state = pmon_state.dpu_control_plane_state;
        // Local vDPU control plane state last updated time in milliseconds.
        npu_ha_scope_state.local_vdpu_control_plane_state_last_updated_time_in_ms = pmon_state.dpu_control_plane_time;
        // The state of local vDPU data plane, which includes DPU hardware / ASIC and certain required firmware. The value can be "unknown", "up", "down".
        npu_ha_scope_state.local_vdpu_data_plane_state = pmon_state.dpu_data_plane_state;
        // Local vDPU data plane state last updated time in milliseconds.
        npu_ha_scope_state.local_vdpu_data_plane_state_last_updated_time_in_ms = pmon_state.dpu_data_plane_time;
        // The list of IPv4 peer IPs (NPU IP) of the BFD sessions in up state.
        npu_ha_scope_state.local_vdpu_up_bfd_sessions_v4 = bfd_state.v4_bfd_up_sessions.clone();
        // Local vDPU BFD sessions v4 last updated time in milliseconds.
        npu_ha_scope_state.local_vdpu_up_bfd_sessions_v4_update_time_in_ms = bfd_state.v4_bfd_up_sessions_timestamp;
        // The list of IPv6 peer IPs (NPU IP) of the BFD sessions in up state.
        npu_ha_scope_state.local_vdpu_up_bfd_sessions_v6 = bfd_state.v6_bfd_up_sessions.clone();
        // Local vDPU BFD sessions v6 last updated time in milliseconds.
        npu_ha_scope_state.local_vdpu_up_bfd_sessions_v6_update_time_in_ms = bfd_state.v6_bfd_up_sessions_timestamp;

        let fvs = swss_serde::to_field_values(&npu_ha_scope_state)?;
        internal.get_mut(NpuDashHaScopeState::table_name()).clone_from(&fvs);

        Ok(())
    }

    fn update_npu_ha_scope_state_pending_operations(
        &self,
        state: &mut State,
        new_operations: Vec<(String, String)>,
        approved_operations: Vec<String>,
    ) -> Result<()> {
        info!(
            "Update pending operation list from DPU. New operations: {:?}, Approved operations: {:?}",
            new_operations, approved_operations
        );
        let internal = state.internal();

        let Some(mut npu_ha_scope_state) = self.get_npu_ha_scope_state(internal) else {
            error!("Cannot update STATE_DB/DASH_HA_SCOPE_STATE until it is populated with basic information",);
            return Ok(());
        };
        let mut pending_operations = self.get_pending_operations(internal, Some(&npu_ha_scope_state))?;
        let old_pending_operations = pending_operations.clone();

        for op_id in approved_operations {
            pending_operations.remove(&op_id);
        }
        for (op_id, op_type) in new_operations {
            pending_operations.insert(op_id, op_type);
        }

        if old_pending_operations == pending_operations {
            // no change
            return Ok(());
        }
        let mut pending_operation_ids = Vec::new();
        let mut pending_operation_types = Vec::new();

        for (op_id, op_type) in pending_operations {
            pending_operation_ids.push(op_id);
            pending_operation_types.push(op_type);
        }
        npu_ha_scope_state.pending_operation_ids = Some(pending_operation_ids);
        npu_ha_scope_state.pending_operation_types = Some(pending_operation_types);
        npu_ha_scope_state.pending_operation_list_last_updated_time_in_ms = Some(now_in_millis());

        let fvs = swss_serde::to_field_values(&npu_ha_scope_state)?;
        internal.get_mut(NpuDashHaScopeState::table_name()).clone_from(&fvs);

        Ok(())
    }

    fn update_npu_ha_scope_state_ha_state(&self, state: &mut State) -> Result<()> {
        let Some(ref dash_ha_scope_config) = self.dash_ha_scope_config else {
            return Ok(());
        };
        let (internal, incoming, _outgoing) = state.get_all();

        let Some(mut npu_ha_scope_state) = self.get_npu_ha_scope_state(internal) else {
            info!("Cannot update STATE_DB/DASH_HA_SCOPE_STATE until it is populated with basic information",);
            return Ok(());
        };

        let Some(dpu_ha_scope_state) = self.get_dpu_ha_scope_state(incoming) else {
            debug!(
                "DPU HA-SCOPE STATE {} is corrupted or has not been received. Skip DASH_HA_SCOPE_STATE update",
                &self.id
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

// Implements messages handlers for HaScopeActor
impl HaScopeActor {
    /// Handles updates to the DASH_HA_SCOPE_CONFIG_TABLE.
    /// Updates the actor's internal config and performs any necessary initialization or subscriptions.
    /// Update DPU DASH_HA_SCOPE_TABLE
    /// Update NPU DASH_HA_SCOPE_STATE if approved_pending_operation_ids is not empty
    fn handle_dash_ha_scope_config_table_message(
        &mut self,
        state: &mut State,
        key: &str,
        context: &mut Context,
    ) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();

        // Retrieve the config update from the incoming message
        let kfv: KeyOpFieldValues = incoming.get(key)?.deserialize_data()?;

        if kfv.operation == KeyOperation::Del {
            // cleanup resources before stopping
            if let Err(e) = self.do_cleanup(state) {
                error!("Failed to cleanup HaScopeActor resources: {}", e);
            }
            context.stop();
            return Ok(());
        }
        let first_time = self.dash_ha_scope_config.is_none();
        let dash_ha_scope_config: HaScopeConfig = decode_from_field_values(&kfv.field_values)?;

        // Update internal config
        self.dash_ha_scope_config = Some(dash_ha_scope_config);

        if first_time {
            // Subscribe to the vDPU Actor for state updates.
            self.register_to_vdpu_actor(outgoing, true)?;
            // Subscribe to the ha-set Actor for state updates.
            self.register_to_haset_actor(outgoing, true)?;
        }

        // this is not a ha_scope for the target vDPU. Skip
        if !self.vdpu_is_managed(incoming) {
            return Ok(());
        }

        // update the DASH_HA_SCOPE_TABLE in DPU
        self.update_dpu_ha_scope_table(state)?;

        // update the NPU DASH_HA_SCOPE_STATE because some fields are derived from dash_ha_scope_config
        self.update_npu_ha_scope_state_ha_state(state)?;

        // need to update operation list if approved_pending_operation_ids is not empty
        let approved_pending_operation_ids = self
            .dash_ha_scope_config
            .as_ref()
            .unwrap()
            .approved_pending_operation_ids
            .clone();

        if !approved_pending_operation_ids.is_empty() {
            self.update_npu_ha_scope_state_pending_operations(state, Vec::new(), approved_pending_operation_ids)?;
        }

        Ok(())
    }

    /// Handles VDPU state update messages for this HA scope.
    /// If the vdpu is unmanaged, the actor is put in dormant state. Otherwise, the actor subscribes to the
    /// DASH_HA_SCOPE_STATE table and updates the NPU HA scope state.
    async fn handle_vdpu_state_update(&mut self, state: &mut State, context: &mut Context) -> Result<()> {
        let (internal, incoming, _outgoing) = state.get_all();
        let Some(vdpu) = self.get_vdpu(incoming) else {
            error!("Failed to retrieve vDPU {} from incoming state", &self.vdpu_id);
            return Ok(());
        };

        if !vdpu.dpu.is_managed {
            debug!("vDPU {} is unmanaged. Put actor in dormant state", &self.vdpu_id);
            return Ok(());
        }

        // create an internal entry for npu STATE_DB/DASH_HA_SCOPE_STATE, which will be the
        // notification channel to SDN controller
        let swss_key = format!(
            "{}{}{}",
            self.vdpu_id,
            NpuDashHaScopeState::key_separator(),
            self.ha_scope_id
        );
        if !internal.has_entry(NpuDashHaScopeState::table_name(), &swss_key) {
            let db = crate::db_for_table::<NpuDashHaScopeState>().await?;
            let table = Table::new_async(db, NpuDashHaScopeState::table_name()).await?;
            internal.add(NpuDashHaScopeState::table_name(), table, swss_key).await;
        }

        if self.bridges.is_empty() {
            // subscribe to dpu DASH_HA_SCOPE_STATE
            self.bridges.push(
                spawn_consumer_bridge_for_actor::<DpuDashHaScopeState>(
                    context.get_edge_runtime().clone(),
                    Self::name(),
                    Some(&self.id),
                    true,
                )
                .await?,
            );
        }
        // ha_scope_table in dpu has no info derived from vDPU but it won't be programed until we receive vDPU which confirms the vDPU is managed
        self.update_dpu_ha_scope_table(state)?;
        self.update_npu_ha_scope_state_base(state)?;
        Ok(())
    }

    /// Handles HaSet state update messages for this HA scope.
    /// Update NPU DASH_HA_SCOPE_STATE
    fn handle_haset_state_update(&mut self, state: &mut State) -> Result<()> {
        // the ha_scope is not managing the target vDPU. Skip
        let incoming = state.incoming();
        if !self.vdpu_is_managed(incoming) {
            return Ok(());
        }

        // ha_scope vip_v4 and vip_v6 are derived from ha_set
        self.update_dpu_ha_scope_table(state)?;
        self.update_npu_ha_scope_state_base(state)?;
        Ok(())
    }

    /// Handles DPU DASH_HA_SCOPE_STATE update messages for this HA scope.
    /// Update NPU DASH_HA_SCOPE_STATE ha_state related fields
    /// Update NPU DASH_HA_SCOPE_STATE pending operation list if there are new operations requested by DPU
    fn handle_dpu_ha_scope_state_update(&mut self, state: &mut State) -> Result<()> {
        let (_internal, incoming, _) = state.get_all();
        // calculate operation requested by dpu
        let Some(new_dpu_ha_scope_state) = self.get_dpu_ha_scope_state(incoming) else {
            // no valid state received from dpu, skip
            return Ok(());
        };
        let mut operations: Vec<(String, String)> = Vec::new();

        // if hamgrd is restarted, we will lose the cached old state. In this case, we will treat
        // all pending operations as new and request the sdn controller via npu dash_ha_scope_state
        // to take action. If these have been notified to sdn controller prior to hamgrd restart,
        // they will be no change to dash_ha_scope_state and no action will be taken by sdn controller.
        let old_dpu_ha_scope_state = self.dpu_ha_scope_state.as_ref().cloned().unwrap_or_default();
        if new_dpu_ha_scope_state.activate_role_pending && !old_dpu_ha_scope_state.activate_role_pending {
            operations.push((Uuid::new_v4().to_string(), "activate_role".to_string()));
        }

        if new_dpu_ha_scope_state.brainsplit_recover_pending && !old_dpu_ha_scope_state.brainsplit_recover_pending {
            operations.push((Uuid::new_v4().to_string(), "brainsplit_recover".to_string()));
        }

        if new_dpu_ha_scope_state.flow_reconcile_pending && !old_dpu_ha_scope_state.flow_reconcile_pending {
            operations.push((Uuid::new_v4().to_string(), "flow_reconcile".to_string()));
        }

        self.dpu_ha_scope_state = Some(new_dpu_ha_scope_state);

        self.update_npu_ha_scope_state_ha_state(state)?;

        if !operations.is_empty() {
            self.update_npu_ha_scope_state_pending_operations(state, operations, Vec::new())?;
        }

        Ok(())
    }
}

impl Actor for HaScopeActor {
    #[instrument(name="handle_message", level="info", skip_all, fields(actor=format!("ha-scope/{}", self.id), key=key))]
    async fn handle_message(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        if key == Self::table_name() {
            if let Err(e) = self.handle_dash_ha_scope_config_table_message(state, key, context) {
                error!("handle_dash_ha_scope_config_table_message failed: {e}");
            }
            return Ok(());
        }

        if self.dash_ha_scope_config.is_none() {
            return Ok(());
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
}

#[cfg(test)]
mod test {
    use crate::{
        actors::{
            ha_scope::HaScopeActor,
            ha_set::HaSetActor,
            test::{self, *},
            vdpu::VDpuActor,
            DbBasedActor,
        },
        db_structs::{now_in_millis, DashHaScopeTable, DpuDashHaScopeState, NpuDashHaScopeState},
        ha_actor_messages::*,
    };
    use sonic_common::SonicDbTable;
    use sonic_dash_api_proto::ha_scope_config::{DesiredHaState, HaScopeConfig};
    use sonic_dash_api_proto::types::HaOwner;
    use std::time::Duration;
    use swss_common::Table;
    use swss_common_testing::*;
    use swss_serde::to_field_values;

    #[tokio::test]
    async fn ha_scope_planned_up_then_down() {
        // To enable trace, set ENABLE_TRACE=1 to run test
        sonic_common::log::init_logger_for_test();
        let _redis = Redis::start_config_db();
        let runtime = test::create_actor_runtime(0, "10.0.0.0", "10::").await;

        // prepare test data
        let (ha_set_id, ha_set_obj) = make_dpu_scope_ha_set_obj(0, 0);
        let dpu_mon = make_dpu_pmon_state(true);
        let bfd_state = make_dpu_bfd_state(Vec::new(), Vec::new());
        let dpu0 = make_local_dpu_actor_state(0, 0, true, Some(dpu_mon.clone()), Some(bfd_state));
        let (vdpu0_id, vdpu0_state_obj) = make_vdpu_actor_state(true, &dpu0);

        // Initial state of NPU DASH_HA_SCOPE_STATE
        let npu_ha_scope_state1 = make_npu_ha_scope_state(&vdpu0_state_obj, &ha_set_obj);
        let npu_ha_scope_state_fvs1 = to_field_values(&npu_ha_scope_state1).unwrap();

        // NPU DASH_HA_SCOPE_STATE after DPU DASH_HA_SCOPE_STATE update
        let dpu_ha_state_state2 = make_dpu_ha_scope_state("dead");
        let mut npu_ha_scope_state2 = npu_ha_scope_state1.clone();
        update_npu_ha_scope_state_by_dpu_scope_state(&mut npu_ha_scope_state2, &dpu_ha_state_state2, "active");
        let npu_ha_scope_state_fvs2 = to_field_values(&npu_ha_scope_state2).unwrap();

        // NPU DASH_HA_SCOPE_STATE after DPU DASH_HA_SCOPE_STATE role activation requestion
        let mut dpu_ha_state_state3 = dpu_ha_state_state2.clone();
        dpu_ha_state_state3.activate_role_pending = true;
        dpu_ha_state_state3.last_updated_time = now_in_millis();
        let mut npu_ha_scope_state3 = npu_ha_scope_state2.clone();
        update_npu_ha_scope_state_by_dpu_scope_state(&mut npu_ha_scope_state3, &dpu_ha_state_state3, "active");
        update_npu_ha_scope_state_pending_ops(
            &mut npu_ha_scope_state3,
            vec![("1".to_string(), "activate_role".to_string())],
        );
        let npu_ha_scope_state_fvs3 = to_field_values(&npu_ha_scope_state3).unwrap();

        let scope_id = format!("{vdpu0_id}:{ha_set_id}");
        let scope_id_in_state = format!("{vdpu0_id}|{ha_set_id}");
        let ha_scope_actor = HaScopeActor::new(scope_id.clone()).unwrap();

        let handle = runtime.spawn(ha_scope_actor, HaScopeActor::name(), &scope_id);

        #[rustfmt::skip]
        let commands = [
            // Send DASH_HA_SCOPE_CONFIG_TABLE to actor with admin state disabled
            send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                    "field_values": {"json": format!(r#"{{"version":"1", "disabled":true, "desired_ha_state":{}, "owner":{}, "ha_set_id":"{ha_set_id}", "approved_pending_operation_ids":[]}}"#, DesiredHaState::Active as i32, HaOwner::Dpu as i32)},
                    },
                    addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

            // Recv registration to vDPU and ha-set
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &scope_id), data: { "active": true }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::HaSetState, &scope_id), data: { "active": true }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

            // Send vDPU state to actor
            send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state_obj, addr: runtime.sp("vdpu", &vdpu0_id) },

            // Send ha-set state to actor
            send! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": true, "ha_set": &ha_set_obj }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

            // Recv update to DPU DASH_HA_SCOPE_TABLE with ha_role = active
            recv! { key: &ha_set_id, data: {
                    "key": &ha_set_id,
                    "operation": "Set",
                    "field_values": {
                        "version": "1",
                        "ha_role": "active",
                        "disabled": "true",
                        "ha_set_id": &ha_set_id,
                        "vip_v4": ha_set_obj.vip_v4.clone(),
                        "vip_v6": ha_set_obj.vip_v6.clone(),
                        "activate_role_requested": "false",
                        "flow_reconcile_requested": "false"
                    },
                    },
                    addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge()) },

            // Write to NPU DASH_HA_SCOPE_STATE through internal state
            chkdb! { type: NpuDashHaScopeState, key: &scope_id_in_state, data: npu_ha_scope_state_fvs1 },

            // Send DPU DASH_HA_SCOPE_STATE to actor to simulate response from DPU
            send! { key: DpuDashHaScopeState::table_name(), data: {"key": DpuDashHaScopeState::table_name(), "operation": "Set",
                    "field_values": serde_json::to_value(to_field_values(&dpu_ha_state_state2).unwrap()).unwrap()
                    }},

            // Write to NPU DASH_HA_SCOPE_STATE through internal state
            chkdb! { type: NpuDashHaScopeState, key: &scope_id_in_state, data: npu_ha_scope_state_fvs2 },

            // Send DASH_HA_SCOPE_CONFIG_TABLE to actor with admin state enabled
            send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                    "field_values": {"json": format!(r#"{{"version":"2","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Active as i32, HaOwner::Dpu as i32)},
                    },
                    addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

            // Recv update to DPU DASH_HA_SCOPE_TABLE with disabled = false
            recv! { key: &ha_set_id, data: {
                    "key": &ha_set_id,
                    "operation": "Set",
                    "field_values": {
                        "version": "2",
                        "ha_role": "active",
                        "disabled": "false",
                        "ha_set_id": &ha_set_id,
                        "vip_v4": ha_set_obj.vip_v4.clone(),
                        "vip_v6": ha_set_obj.vip_v6.clone(),
                        "activate_role_requested": "false",
                        "flow_reconcile_requested": "false"
                    },
                    },
                    addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())  },

            // Write to NPU DASH_HA_SCOPE_STATE through internal state
            chkdb! { type: NpuDashHaScopeState, key: &scope_id_in_state, data: npu_ha_scope_state_fvs2 },

            // Send DPU DASH_HA_SCOPE_STATE with role activation request to the actor
            send! { key: DpuDashHaScopeState::table_name(), data: {"key": DpuDashHaScopeState::table_name(), "operation": "Set",
                    "field_values": serde_json::to_value(to_field_values(&dpu_ha_state_state3).unwrap()).unwrap()
                    }},

            // Write to NPU DASH_HA_SCOPE_STATE through internal state with pending activation
            chkdb! { type: NpuDashHaScopeState,
                    key: &scope_id_in_state, data: npu_ha_scope_state_fvs3,
                    exclude: "pending_operation_ids,pending_operation_list_last_updated_time_in_ms" },
        ];

        test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

        // get GUID from DASH_HA_SCOPE_STATE pending_operation_ids
        let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
        let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
        let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
        let op_id = npu_ha_scope_state.pending_operation_ids.unwrap().pop().unwrap();

        // continue the test to activate the role
        let mut npu_ha_scope_state4 = npu_ha_scope_state3.clone();
        update_npu_ha_scope_state_pending_ops(&mut npu_ha_scope_state4, vec![]);
        let npu_ha_scope_state_fvs4 = to_field_values(&npu_ha_scope_state4).unwrap();

        let mut dpu_ha_state_state5 = make_dpu_ha_scope_state("active");
        dpu_ha_state_state5.ha_term = "2".to_string();
        let mut npu_ha_scope_state5: NpuDashHaScopeState = npu_ha_scope_state4.clone();
        update_npu_ha_scope_state_by_dpu_scope_state(&mut npu_ha_scope_state5, &dpu_ha_state_state5, "active");
        let npu_ha_scope_state_fvs5 = to_field_values(&npu_ha_scope_state5).unwrap();

        let bfd_state = make_dpu_bfd_state(vec!["10.0.0.0", "10.0.1.0"], Vec::new());
        let dpu0 = make_local_dpu_actor_state(0, 0, true, Some(dpu_mon), Some(bfd_state));
        let (vdpu0_id, vdpu0_state_obj) = make_vdpu_actor_state(true, &dpu0);
        let mut npu_ha_scope_state6 = npu_ha_scope_state5.clone();
        update_npu_ha_scope_state_by_vdpu(&mut npu_ha_scope_state6, &vdpu0_state_obj);
        let npu_ha_scope_state_fvs6 = to_field_values(&npu_ha_scope_state6).unwrap();

        #[rustfmt::skip]
        let commands = [
            // Send DASH_HA_SCOPE_CONFIG_TABLE with activation approved
            send! { key: HaScopeActor::table_name(), data: { "key": &scope_id, "operation": "Set",
                    "field_values": {"json": format!(r#"{{"version":"3","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":["{op_id}"]}}"#, DesiredHaState::Active as i32, HaOwner::Dpu as i32)},
                    },
                    addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

            // Recv update to DPU DASH_HA_SCOPE_TABLE with activate_role_requested=true
            recv! { key: &ha_set_id, data: {
                    "key": &ha_set_id,
                    "operation": "Set",
                    "field_values": {
                        "version": "3",
                        "ha_role": "active",
                        "disabled": "false",
                        "ha_set_id": &ha_set_id,
                        "vip_v4": ha_set_obj.vip_v4.clone(),
                        "vip_v6": ha_set_obj.vip_v6.clone(),
                        "activate_role_requested": "true",
                        "flow_reconcile_requested": "false"
                    },
                    },
                    addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                },

            // Write to NPU DASH_HA_SCOPE_STATE through internal state with no pending activation
            chkdb! { type: NpuDashHaScopeState,
                    key: &scope_id_in_state, data: npu_ha_scope_state_fvs4,
                    exclude: "pending_operation_list_last_updated_time_in_ms" },

            // Send DPU DASH_HA_SCOPE_STATE with ha_role = active and activate_role_requested = false
            send! { key: DpuDashHaScopeState::table_name(), data: {"key": DpuDashHaScopeState::table_name(), "operation": "Set",
                    "field_values": serde_json::to_value(to_field_values(&dpu_ha_state_state5).unwrap()).unwrap()
                    }},

            // Write to NPU DASH_HA_SCOPE_STATE through internal state with ha_role = active
            chkdb! { type: NpuDashHaScopeState,
                    key: &scope_id_in_state, data: npu_ha_scope_state_fvs5,
                    exclude: "pending_operation_list_last_updated_time_in_ms" },

            // Send vdpu state update after bfd session up
            send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state_obj, addr: runtime.sp("vdpu", &vdpu0_id) },
            // Recv update to DPU DASH_HA_SCOPE_TABLE, triggered by vdpu state update
            recv! { key: &ha_set_id, data: {
                    "key": &ha_set_id,
                    "operation": "Set",
                    "field_values": {
                        "version": "3",
                        "ha_role": "active",
                        "disabled": "false",
                        "ha_set_id": &ha_set_id,
                        "vip_v4": ha_set_obj.vip_v4.clone(),
                        "vip_v6": ha_set_obj.vip_v6.clone(),
                        "activate_role_requested": "false",
                        "flow_reconcile_requested": "false"
                    },
                    },
                    addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                },
            // Write to NPU DASH_HA_SCOPE_STATE through internal state with bfd session up
            chkdb! { type: NpuDashHaScopeState,
                    key: &scope_id_in_state, data: npu_ha_scope_state_fvs6,
                    exclude: "pending_operation_list_last_updated_time_in_ms" },
        ];

        test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

        // execute planned shutdown
        let mut npu_ha_scope_state7 = npu_ha_scope_state6.clone();
        npu_ha_scope_state7.local_target_asic_ha_state = Some("dead".to_string());
        let npu_ha_scope_state_fvs7 = to_field_values(&npu_ha_scope_state7).unwrap();
        #[rustfmt::skip]
        let commands = [
            // Send DASH_HA_SCOPE_CONFIG_TABLE with desired_ha_state = dead
            send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                    "field_values": {"json": format!(r#"{{"version":"4","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Dead as i32, HaOwner::Dpu as i32)},
                    },
                    addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

            recv! { key: &ha_set_id, data: {
                    "key": &ha_set_id,
                    "operation": "Set",
                    "field_values": {
                        "version": "4",
                        "ha_role": "dead",
                        "disabled": "false",
                        "ha_set_id": &ha_set_id,
                        "vip_v4": ha_set_obj.vip_v4.clone(),
                        "vip_v6": ha_set_obj.vip_v6.clone(),
                        "activate_role_requested": "false",
                        "flow_reconcile_requested": "false"
                    },
                    },
                    addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                },
            // Check NPU DASH_HA_SCOPE_STATE is updated with desired_ha_state = dead
            chkdb! { type: NpuDashHaScopeState,
                    key: &scope_id_in_state, data: npu_ha_scope_state_fvs7,
                    exclude: "pending_operation_list_last_updated_time_in_ms" },

            // simulate delete of ha-scope entry
            send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Del",
                    "field_values": {"json": format!(r#"{{"version":"2","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Dead as i32, HaOwner::Dpu as i32)},
                    },
                    addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

            // Verify that cleanup removed the NPU DASH_HA_SCOPE_STATE table entry
            chkdb! { type: NpuDashHaScopeState, key: &scope_id_in_state, nonexist },

            // Recv delete of DPU DASH_HA_SCOPE_TABLE
            recv! { key: &ha_set_id, data: { "key": &ha_set_id, "operation": "Del", "field_values": {} },
                    addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge()) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &scope_id), data: { "active": false }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
            recv! { key: ActorRegistration::msg_key(RegistrationType::HaSetState, &scope_id), data: { "active": false }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

        ];

        test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;
        if tokio::time::timeout(Duration::from_secs(3), handle).await.is_err() {
            panic!("timeout waiting for actor to terminate");
        }
    }
}
