use crate::actors::DbBasedActor;
use crate::db_structs::*;
use crate::ha_actor_messages::*;
use crate::{HaSetActor, VDpuActor};
use anyhow::Result;
use serde::de::DeserializeOwned;
use sonic_common::SonicDbTable;
use sonic_dash_api_proto::decode_from_field_values;
use sonic_dash_api_proto::ha_scope_config::HaScopeConfig;
use sonic_dash_api_proto::types::HaOwner;
use std::collections::HashMap;
use swbus_actor::{
    state::{incoming::Incoming, internal::Internal, outgoing::Outgoing},
    ActorMessage, State,
};
use swss_common::{KeyOpFieldValues, KeyOperation};
use swss_common_bridge::consumer::ConsumerBridge;
use tracing::{debug, error, info};

pub struct HaScopeBase {
    pub(super) id: String,
    pub(super) ha_scope_id: String,
    pub(super) vdpu_id: String,
    pub(super) peer_vdpu_id: Option<String>,
    pub(super) dash_ha_scope_config: Option<HaScopeConfig>,
    pub(super) bridges: Vec<ConsumerBridge>,
    /// We need to keep track of the previous dpu_ha_scope_state to detect state change
    pub(super) dpu_ha_scope_state: Option<DpuDashHaScopeState>,
}

impl HaScopeBase {
    pub fn new(key: String) -> Result<Self> {
        if let Some((vdpu_id, ha_scope_id)) = key.split_once(HaScopeConfig::key_separator()) {
            Ok(HaScopeBase {
                id: key.to_string(),
                vdpu_id: vdpu_id.to_string(),
                ha_scope_id: ha_scope_id.to_string(),
                peer_vdpu_id: None,
                dash_ha_scope_config: None,
                bridges: Vec::new(),
                dpu_ha_scope_state: None,
            })
        } else {
            Err(anyhow::anyhow!("Invalid key format for HA scope actor: {}", key))
        }
    }

    /// Handle first-time config message: decode config, register with vDPU and HaSet actors,
    /// notify HaSet actor of state. Returns the decoded HaOwner for variant selection.
    pub fn handle_first_config_message(&mut self, state: &mut State, key: &str) -> Result<HaOwner> {
        let (_internal, incoming, outgoing) = state.get_all();

        let kfv: KeyOpFieldValues = incoming.get_or_fail(key)?.deserialize_data()?;

        // Directly update the config at the first time
        self.dash_ha_scope_config = Some(decode_from_field_values(&kfv.field_values)?);

        // Subscribe to the vDPU Actor for state updates.
        self.register_to_vdpu_actor(outgoing, true)?;
        // Subscribe to the ha-set Actor for state updates.
        self.register_to_haset_actor(outgoing, true)?;

        // Send a state update message to the ha-set actor
        let owner = self
            .dash_ha_scope_config
            .as_ref()
            .map(|c| c.owner)
            .unwrap_or(HaOwner::Unspecified as i32);
        let npu_state: NpuDashHaScopeState = Default::default();
        if let Ok(msg) = HaScopeActorState::new_actor_msg(
            &self.id,
            owner,
            &npu_state,
            &self.vdpu_id,
            self.peer_vdpu_id.as_deref().unwrap_or(""),
        ) {
            if let Some(ha_set_id) = self.get_haset_id() {
                outgoing.send(outgoing.from_my_sp(HaSetActor::name(), &ha_set_id), msg);
            }
        }

        let ha_owner = HaOwner::try_from(owner).unwrap_or(HaOwner::Unspecified);
        Ok(ha_owner)
    }
}

// Getter helper functions
impl HaScopeBase {
    /// Get vdpu data received via vdpu update
    pub fn get_vdpu(&self, incoming: &Incoming) -> Option<VDpuActorState> {
        let key = VDpuActorState::msg_key(&self.vdpu_id);
        let msg = incoming.get(&key)?;
        match msg.deserialize_data() {
            Ok(data) => Some(data),
            Err(e) => {
                error!("Failed to deserialize VDpuActorState from message: {}", e);
                None
            }
        }
    }

    pub fn get_haset(&self, incoming: &Incoming) -> Option<HaSetActorState> {
        let ha_set_id = self.get_haset_id()?;

        let key = HaSetActorState::msg_key(&ha_set_id);
        let msg = incoming.get(&key)?;
        match msg.deserialize_data() {
            Ok(data) => Some(data),
            Err(e) => {
                error!("Failed to deserialize HaSetActorState from message: {}", e);
                None
            }
        }
    }

    pub fn decode_hascope_actor_message<T>(&self, incoming: &Incoming, key: &str) -> Option<T>
    where
        T: DeserializeOwned,
    {
        let msg = incoming.get(key)?;
        match msg.deserialize_data() {
            Ok(data) => Some(data),
            Err(e) => {
                error!("Failed to deserialize VoteReply from message: {}", e);
                None
            }
        }
    }

    pub fn get_haset_id(&self) -> Option<String> {
        let dash_ha_scope_config = self.dash_ha_scope_config.as_ref()?;
        Some(dash_ha_scope_config.ha_set_id.clone())
    }

    pub fn get_remote_vdpu_id(&self, ha_set: &HaSetActorState) -> Option<String> {
        for i in &ha_set.vdpu_ids {
            if *i != self.vdpu_id {
                return Some(i.clone());
            }
        }
        None
    }

    pub fn get_dpu_ha_scope_state(&self, incoming: &Incoming) -> Option<DpuDashHaScopeState> {
        let msg = incoming.get(DpuDashHaScopeState::table_name())?;
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

    pub fn get_npu_ha_scope_state(&self, internal: &Internal) -> Option<NpuDashHaScopeState> {
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

    pub fn get_pending_operations(
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

    pub fn get_peer_actor_id(&self) -> Option<String> {
        self.peer_vdpu_id.as_ref()?;
        Some(format!(
            "{}{}{}",
            self.peer_vdpu_id.as_deref().unwrap_or_default(),
            HaScopeConfig::key_separator(),
            &self.ha_scope_id
        ))
    }

    pub fn vdpu_is_managed(&self, incoming: &Incoming) -> bool {
        let Some(vdpu) = self.get_vdpu(incoming) else {
            return false;
        };
        vdpu.dpu.is_managed
    }
}

// Registration and cleanup methods
impl HaScopeBase {
    /// Register VDPUStateUpdate from VDPUActor
    pub fn register_to_vdpu_actor(&self, outgoing: &mut Outgoing, active: bool) -> Result<()> {
        if self.dash_ha_scope_config.is_none() {
            return Ok(());
        };

        let msg = ActorRegistration::new_actor_msg(active, RegistrationType::VDPUState, &self.id)?;
        outgoing.send(outgoing.from_my_sp(VDpuActor::name(), &self.vdpu_id), msg);
        Ok(())
    }

    /// Register HaSetStateUpdate from HaSetActor
    pub fn register_to_haset_actor(&self, outgoing: &mut Outgoing, active: bool) -> Result<()> {
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

    pub fn delete_dash_ha_scope_table(&self, outgoing: &mut Outgoing) -> Result<()> {
        let kfv = KeyOpFieldValues {
            key: self.ha_scope_id.clone(),
            operation: KeyOperation::Del,
            field_values: HashMap::new(),
        };

        let msg = ActorMessage::new(self.ha_scope_id.clone(), &kfv)?;
        outgoing.send(outgoing.common_bridge_sp::<DashHaScopeTable>(), msg);

        Ok(())
    }

    pub fn delete_npu_ha_scope_state(&self, internal: &mut Internal) -> Result<()> {
        if self.dash_ha_scope_config.is_none() {
            return Ok(());
        };

        internal.delete(NpuDashHaScopeState::table_name());

        Ok(())
    }

    pub fn do_cleanup(&mut self, state: &mut State) -> Result<()> {
        let (internal, _incoming, outgoing) = state.get_all();
        self.delete_dash_ha_scope_table(outgoing)?;
        self.delete_npu_ha_scope_state(internal)?;
        self.register_to_vdpu_actor(outgoing, false)?;
        self.register_to_haset_actor(outgoing, false)?;

        Ok(())
    }
}

// Shared state update methods
impl HaScopeBase {
    pub fn update_npu_ha_scope_state_base(&self, state: &mut State) -> Result<()> {
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

    pub fn update_npu_ha_scope_state_pending_operations(
        &self,
        state: &mut State,
        new_operations: Vec<(String, String)>,
        approved_operations: Vec<String>,
    ) -> Result<Vec<String>> {
        info!(
            "Update pending operation list from DPU. New operations: {:?}, Approved operations: {:?}",
            new_operations, approved_operations
        );
        let internal = state.internal();

        let Some(mut npu_ha_scope_state) = self.get_npu_ha_scope_state(internal) else {
            error!("Cannot update STATE_DB/DASH_HA_SCOPE_STATE until it is populated with basic information",);
            return Ok(Vec::new());
        };
        let mut pending_operations = self.get_pending_operations(internal, Some(&npu_ha_scope_state))?;
        let old_pending_operations = pending_operations.clone();

        let mut approved_types = Vec::new();
        for op_id in approved_operations {
            if let Some(op_type) = pending_operations.get(&op_id).cloned() {
                approved_types.push(op_type);
            }
            pending_operations.remove(&op_id);
        }
        for (op_id, op_type) in new_operations {
            pending_operations.insert(op_id, op_type);
        }

        if old_pending_operations == pending_operations {
            // no change
            return Ok(approved_types);
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

        Ok(approved_types)
    }
}
