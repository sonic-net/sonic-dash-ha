use crate::actors::{spawn_consumer_bridge_for_actor, DbBasedActor};
use crate::db_structs::*;
use crate::ha_actor_messages::*;
use crate::{HaSetActor, VDpuActor};
use anyhow::Result;
use serde::de::DeserializeOwned;
use sonic_common::SonicDbTable;
use sonic_dash_api_proto::decode_from_field_values;
use sonic_dash_api_proto::ha_scope_config::{DesiredHaState, HaScopeConfig};
use sonic_dash_api_proto::types::{HaOwner, HaRole, HaState};
use std::collections::HashMap;
use std::time::Duration;
use swbus_actor::{
    state::{incoming::Incoming, internal::Internal, outgoing::Outgoing},
    Actor, ActorMessage, Context, State,
};
use swss_common::Table;
use swss_common::{KeyOpFieldValues, KeyOperation};
use swss_common_bridge::consumer::ConsumerBridge;
use tracing::{debug, error, info, instrument};
use uuid::Uuid;

const MAX_RETRIES: u32 = 3;
const RETRY_INTERVAL: u32 = 30; // seconds

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum HaEvent {
    None,
    Launch,
    PeerConnected,
    PeerLost,
    PeerStateChanged,
    LocalFailure,
    BulkSyncCompleted,
    VoteCompleted,
    PendingRoleActivationApproved,
    FlowReconciliationApproved,
    SwitchoverApproved,
    AdminStateChanged,
    DesiredStateChanged,
    DpuStateChanged,
}

impl HaEvent {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "No-op",
            Self::Launch => "Launch",
            Self::PeerConnected => "PeerConnected",
            Self::PeerLost => "PeerLost",
            Self::PeerStateChanged => "PeerStateChanged",
            Self::LocalFailure => "LocalFailure",
            Self::BulkSyncCompleted => "BulkSyncCompleted",
            Self::VoteCompleted => "VoteCompleted",
            Self::PendingRoleActivationApproved => "PendingRoleActivationApproved",
            Self::FlowReconciliationApproved => "FlowReconciliationApproved",
            Self::SwitchoverApproved => "SwitchoverApproved",
            Self::AdminStateChanged => "AdminStateChanged",
            Self::DesiredStateChanged => "DesiredStateChanged",
            Self::DpuStateChanged => "DpuStateChanged",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "" | "No-op" => Some(Self::None),
            "Launch" => Some(Self::Launch),
            "PeerConnected" => Some(Self::PeerConnected),
            "PeerLost" => Some(Self::PeerLost),
            "PeerStateChanged" => Some(Self::PeerStateChanged),
            "LocalFailure" => Some(Self::LocalFailure),
            "BulkSyncCompleted" => Some(Self::BulkSyncCompleted),
            "VoteCompleted" => Some(Self::VoteCompleted),
            "PendingRoleActivationApproved" => Some(Self::PendingRoleActivationApproved),
            "FlowReconciliationApproved" => Some(Self::FlowReconciliationApproved),
            "SwitchoverApproved" => Some(Self::SwitchoverApproved),
            "AdminStateChanged" => Some(Self::AdminStateChanged),
            "DesiredStateChanged" => Some(Self::DesiredStateChanged),
            "DpuStateChanged" => Some(Self::DpuStateChanged),
            _ => None,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum TargetState {
    Active,
    Standby,
    Standalone,
    Dead,
}

pub struct HaScopeActor {
    id: String,
    ha_scope_id: String,
    vdpu_id: String,
    peer_vdpu_id: Option<String>,
    dash_ha_scope_config: Option<HaScopeConfig>,
    bridges: Vec<ConsumerBridge>,
    // we need to keep track the previous dpu_ha_scope_state to detect state change
    dpu_ha_scope_state: Option<DpuDashHaScopeState>,
    // target state that HAmgrd should transition to upon HA events
    target_ha_scope_state: Option<TargetState>,
    // retry count used for voting
    retry_count: u32,
    // is peer connected?
    peer_connected: bool,
}

impl DbBasedActor for HaScopeActor {
    fn new(key: String) -> Result<Self> {
        if let Some((vdpu_id, ha_scope_id)) = key.split_once(HaScopeConfig::key_separator()) {
            Ok(HaScopeActor {
                id: key.to_string(),
                vdpu_id: vdpu_id.to_string(),
                ha_scope_id: ha_scope_id.to_string(),
                peer_vdpu_id: None,
                dash_ha_scope_config: None,
                bridges: Vec::new(),
                dpu_ha_scope_state: None,
                target_ha_scope_state: None,
                retry_count: 0,
                peer_connected: false,
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
        let msg = incoming.get(&key)?;
        match msg.deserialize_data() {
            Ok(data) => Some(data),
            Err(e) => {
                error!("Failed to deserialize VDpuActorState from message: {}", e);
                None
            }
        }
    }

    fn get_haset(&self, incoming: &Incoming) -> Option<HaSetActorState> {
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

    fn decode_hascope_actor_message<T>(&self, incoming: &Incoming, key: &String) -> Option<T>
    where
        T: DeserializeOwned,
    {
        let msg = incoming.get(&key)?;
        match msg.deserialize_data() {
            Ok(data) => Some(data),
            Err(e) => {
                error!("Failed to deserialize VoteReply from message: {}", e);
                None
            }
        }
    }

    fn get_haset_id(&self) -> Option<String> {
        let dash_ha_scope_config = self.dash_ha_scope_config.as_ref()?;
        Some(dash_ha_scope_config.ha_set_id.clone())
    }

    fn get_remote_vdpu_id(&self, ha_set: &HaSetActorState) -> Option<String> {
        for i in &ha_set.vdpu_ids {
            if *i != self.vdpu_id {
                return Some(i.clone());
            }
        }
        None
    }

    fn get_dpu_ha_scope_state(&self, incoming: &Incoming) -> Option<DpuDashHaScopeState> {
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

    fn get_peer_actor_id(&self) -> Option<String> {
        if self.peer_vdpu_id.is_none() {
            return None;
        }
        return Some(format!(
            "{}{}{}",
            self.peer_vdpu_id.as_deref().unwrap_or_default(),
            HaScopeConfig::key_separator(),
            &self.ha_scope_id
        ));
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
    /// Register VDPUStateUpdate from VDPUActor
    fn register_to_vdpu_actor(&self, outgoing: &mut Outgoing, active: bool) -> Result<()> {
        if self.dash_ha_scope_config.is_none() {
            return Ok(());
        };

        let msg = ActorRegistration::new_actor_msg(active, RegistrationType::VDPUState, &self.id)?;
        outgoing.send(outgoing.from_my_sp(VDpuActor::name(), &self.vdpu_id), msg);
        Ok(())
    }

    /// Register HaSetStateUpdate from HaSetActor
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

    /// Register HAStateChanged from (peer) HaScopeActor
    fn register_to_hascope_actor(&self, outgoing: &mut Outgoing, active: bool) -> Result<()> {
        let Some(peer_actor_id) = self.get_peer_actor_id() else {
            // Haven't received the remote peer vDPU info yet
            info!("Haven't received peer vDPU info yet");
            return Ok(());
        };

        let msg = ActorRegistration::new_actor_msg(active, RegistrationType::HAStateChanged, &self.id)?;
        outgoing.send(outgoing.from_my_sp(HaScopeActor::name(), &peer_actor_id), msg);
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
        self.register_to_hascope_actor(outgoing, false)?;
        Ok(())
    }

    /// Update DPU HA Scope Table purely based on HA Scope & HA Set Config, no flex parameters
    /// Used in DPU-driven mode
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
            ha_term: "0".to_string(), // TODO: not clear what need to be done for DPU-driven mode
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

    /// Update DPU HA Scope Table based on configurations and parameters
    fn update_dpu_ha_scope_table_with_params(
        &self,
        state: &mut State,
        ha_role: &String,
        flow_reconcile_requested: bool,
        activate_role_requested: bool,
    ) -> Result<()> {
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

        let dash_ha_scope = DashHaScopeTable {
            version: dash_ha_scope_config.version.parse().unwrap(),
            disabled: dash_ha_scope_config.disabled,
            ha_set_id: dash_ha_scope_config.ha_set_id.clone(),
            vip_v4: haset.ha_set.vip_v4.clone(),
            vip_v6: haset.ha_set.vip_v6.clone(),
            ha_role: ha_role.clone(),
            ha_term: self
                .get_npu_ha_scope_state(internal)
                .and_then(|s| s.local_target_term)
                .unwrap_or_default(),
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

        return Ok(approved_types);
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

    fn increment_npu_ha_scope_state_target_term(&self, state: &mut State) -> Result<()> {
        let Some(ref _dash_ha_scope_config) = self.dash_ha_scope_config else {
            return Ok(());
        };
        let (internal, _incoming, _outgoing) = state.get_all();

        let Some(mut npu_ha_scope_state) = self.get_npu_ha_scope_state(internal) else {
            info!("Cannot update STATE_DB/DASH_HA_SCOPE_STATE until it is populated with basic information",);
            return Ok(());
        };
        let current_term = npu_ha_scope_state
            .local_target_term
            .as_ref()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        npu_ha_scope_state.local_target_term = Some((current_term + 1).to_string());

        let fvs = swss_serde::to_field_values(&npu_ha_scope_state)?;
        internal.get_mut(NpuDashHaScopeState::table_name()).clone_from(&fvs);

        Ok(())
    }

    fn set_npu_local_ha_state(&mut self, state: &mut State, new_state: HaState, reason: &str) -> Result<()> {
        let (internal, _incoming, _outoging) = state.get_all();
        let Some(mut npu_state) = self.get_npu_ha_scope_state(internal) else {
            return Ok(());
        };

        if npu_state.local_ha_state.as_deref() == Some(new_state.as_str_name()) {
            // the HA scope is already in the new state;
            return Ok(());
        }
        npu_state.local_ha_state = Some(new_state.as_str_name().to_string());
        npu_state.local_ha_state_last_updated_time_in_ms = Some(now_in_millis());
        npu_state.local_ha_state_last_updated_reason = Some(reason.to_string());

        let fvs = swss_serde::to_field_values(&npu_state)?;
        internal.get_mut(NpuDashHaScopeState::table_name()).clone_from(&fvs);
        info!(scope=%self.id, state=%new_state.as_str_name(), "HA scope transitioned: {}", reason);
        Ok(())
    }

    fn set_npu_flow_sync_session(
        &mut self,
        state: &mut State,
        flow_sync_session_id: &Option<String>,
        flow_sync_session_state: &Option<String>,
        flow_sync_session_start_time_in_ms: &Option<i64>,
        flow_sync_session_target_server: &Option<String>,
    ) -> Result<()> {
        let internal = state.internal();
        let Some(mut npu_state) = self.get_npu_ha_scope_state(&*internal) else {
            return Ok(());
        };

        if !flow_sync_session_id.is_none() {
            npu_state.flow_sync_session_id = flow_sync_session_id.clone();
        }
        if !flow_sync_session_state.is_none() {
            npu_state.flow_sync_session_state = flow_sync_session_state.clone();
        }
        if !flow_sync_session_start_time_in_ms.is_none() {
            npu_state.flow_sync_session_start_time_in_ms = flow_sync_session_start_time_in_ms.clone();
        }
        if !flow_sync_session_target_server.is_none() {
            npu_state.flow_sync_session_target_server = flow_sync_session_target_server.clone();
        }

        let fvs = swss_serde::to_field_values(&npu_state)?;
        internal.get_mut(NpuDashHaScopeState::table_name()).clone_from(&fvs);
        info!(
            "Update NPU HA Scope State Table with a new flow sync session: {} {} {} {}",
            flow_sync_session_id.as_deref().unwrap_or_default(),
            flow_sync_session_state.as_deref().unwrap_or_default(),
            flow_sync_session_start_time_in_ms.unwrap_or_default(),
            flow_sync_session_target_server.as_deref().unwrap_or_default()
        );
        Ok(())
    }
}

// Implements messages handlers for HaScopeActor (DPU-driven mode)
impl HaScopeActor {
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
        self.dash_ha_scope_config = Some(dash_ha_scope_config);

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

// Implements messages handlers for HaScopeActor (NPU-driven mode))
impl HaScopeActor {
    /// Hanldes updates to the DASH_HA_SCOPE_CONFIG_TABLE in the case of NPU-driven HA.
    fn handle_dash_ha_scope_config_table_message_npu_driven(
        &mut self,
        state: &mut State,
        key: &str,
        _context: &mut Context,
    ) -> Result<HaEvent, String> {
        let (_internal, incoming, _outgoing) = state.get_all();

        // Retrieve the config update from the incoming message
        let kfv: KeyOpFieldValues = incoming
            .get_or_fail(key)
            .map_err(|e| e.to_string())?
            .deserialize_data()
            .map_err(|e| e.to_string())?;
        let dash_ha_scope_config: HaScopeConfig =
            decode_from_field_values(&kfv.field_values).map_err(|e| e.to_string())?;
        let old_ha_scope_config = self.dash_ha_scope_config.clone().unwrap_or_default();

        // Update internal config
        self.dash_ha_scope_config = Some(dash_ha_scope_config);

        // this is not a ha_scope for the target vDPU. Skip
        if !self.vdpu_is_managed(incoming) {
            return Ok(HaEvent::None);
        }

        // admin state change
        if old_ha_scope_config.disabled != self.dash_ha_scope_config.as_ref().unwrap().disabled {
            return Ok(HaEvent::AdminStateChanged);
        }

        // desired state change
        if old_ha_scope_config.desired_ha_state != self.dash_ha_scope_config.as_ref().unwrap().desired_ha_state {
            return Ok(HaEvent::DesiredStateChanged);
        }

        // update operation list if approved_pending_operation_ids is not empty
        let approved_pending_operation_ids = self
            .dash_ha_scope_config
            .as_ref()
            .unwrap()
            .approved_pending_operation_ids
            .clone();

        if !approved_pending_operation_ids.is_empty() {
            match self.update_npu_ha_scope_state_pending_operations(state, Vec::new(), approved_pending_operation_ids) {
                Ok(approved_operation_types) => {
                    if approved_operation_types.len() > 1 {
                        error!("Multiple operations are approved at the same time is not expected in NPU-driven mode")
                    }

                    if let Some(first_op) = approved_operation_types.first() {
                        return match first_op.as_str() {
                            "activate_role" => Ok(HaEvent::PendingRoleActivationApproved),
                            "flow_reconcile" => Ok(HaEvent::FlowReconciliationApproved),
                            "switchover" => Ok(HaEvent::SwitchoverApproved),
                            _ => Ok(HaEvent::None),
                        };
                    }
                }
                Err(_e) => {
                    error!("Encountered error when updating pending operatios!");
                    return Ok(HaEvent::None);
                }
            }
        }

        return Ok(HaEvent::None);
    }

    /// Handles registration messages from peer HA Scope actor
    /// Respond with HAStateChanged when the actor-self is ready
    async fn handle_ha_scope_registration(&mut self, state: &mut State, key: &str) -> Result<HaEvent, String> {
        let (internal, incoming, outgoing) = state.get_all();
        let entry = incoming
            .get_entry(key)
            .ok_or_else(|| format!("Entry not found for key: {}", key))?;
        let ActorRegistration { active, .. } = entry.msg.deserialize_data().map_err(|e| e.to_string())?;
        if active {
            let Some(npu_ha_scope_state) = self.get_npu_ha_scope_state(internal) else {
                info!(
                    "Cannot respond to the peer until STATE_DB/DASH_HA_SCOPE_STATE is populated with basic information",
                );
                return Ok(HaEvent::None);
            };

            let msg = HAStateChanged::new_actor_msg(
                &self.id,
                "",
                npu_ha_scope_state.local_ha_state.as_deref().unwrap_or(""),
                npu_ha_scope_state.local_ha_state_last_updated_time_in_ms.unwrap_or(0),
                npu_ha_scope_state.local_target_term.as_deref().unwrap_or("0"),
            )
            .map_err(|e| e.to_string())?;
            outgoing.send(entry.source.clone(), msg);
        }
        Ok(HaEvent::None)
    }

    /// Handles VDPU state update messages for this HA scope.
    /// If the vdpu is unmanaged, the actor is put in dormant state.
    /// Otherwise, map the update to a HaEvent
    async fn handle_vdpu_state_update_npu_driven_mode(
        &mut self,
        state: &mut State,
        context: &mut Context,
    ) -> Result<HaEvent, String> {
        let (_internal, incoming, _outgoing) = state.get_all();
        let Some(vdpu) = self.get_vdpu(incoming) else {
            error!("Failed to retrieve vDPU {} from incoming state", &self.vdpu_id);
            return Err("Failed to retrieve vDPU from incoming state".to_string());
        };

        if !vdpu.dpu.is_managed {
            debug!("vDPU {} is unmanaged. Put actor in dormant state", &self.vdpu_id);
            return Ok(HaEvent::None);
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
                .await
                .map_err(|e| e.to_string())?,
            );
            // subscribe to dpu DASH_FLOW_SYNC_SESSION_STATE
            self.bridges.push(
                spawn_consumer_bridge_for_actor::<DashFlowSyncSessionState>(
                    context.get_edge_runtime().clone(),
                    Self::name(),
                    Some(&self.id),
                    true,
                )
                .await
                .map_err(|e| e.to_string())?,
            );
        }

        // update basic info of NPU HA scope state
        if let Err(e) = self.update_npu_ha_scope_state_base(state) {
            return Err(e.to_string());
        }

        match vdpu.up {
            true => Ok(HaEvent::None),
            false => Ok(HaEvent::LocalFailure),
        }
    }

    /// Handles HaSet state update messages for this HA scope.
    /// Map the update to a HaEvent
    /// Register messages from a peer HA scope actor
    fn handle_haset_state_update_npu_driven_mode(&mut self, state: &mut State) -> Result<HaEvent, String> {
        // the ha_scope is not managing the target vDPU. Skip
        if !self.vdpu_is_managed(state.incoming()) {
            return Ok(HaEvent::None);
        }

        // update basic info of NPU HA scope state
        let _ = self.update_npu_ha_scope_state_base(state);

        let first_time = self.peer_vdpu_id.is_none();

        let Some(ha_set) = self.get_haset(state.incoming()) else {
            return Ok(HaEvent::None);
        };
        let peer_vdpu_id = self.get_remote_vdpu_id(&ha_set);
        if self.peer_vdpu_id.is_none() || self.peer_vdpu_id != peer_vdpu_id {
            if self.peer_vdpu_id.is_some() {
                // Got a new peer HA scope actor than the old one
                // the behavior in this scenario is currently undefined
                error!("Dynamically changing peer is not supported!");
                return Ok(HaEvent::None);
            } else {
                // Got a fresh new peer HA scope actor
                self.peer_vdpu_id = peer_vdpu_id;

                // register messages from the peer ha scope actor
                let _ = self.register_to_hascope_actor(state.outgoing(), true);

                // Send a signal to itself to schedule a check later
                let outgoing = state.outgoing();
                if let Ok(msg) = SelfNotification::new_actor_msg(&self.id, "CheckPeerConnection") {
                    outgoing.send_with_delay(
                        outgoing.from_my_sp(HaScopeActor::name(), &self.id),
                        msg,
                        Duration::from_secs(RETRY_INTERVAL.into()),
                    );
                }
            }
        }

        if first_time {
            return Ok(HaEvent::Launch);
        } else {
            match ha_set.up {
                true => Ok(HaEvent::None),
                false => Ok(HaEvent::PeerLost),
            }
        }
    }

    /// Handles DPU DASH_HA_SCOPE_STATE update messages for this HA scope.
    /// Update NPU DASH_HA_SCOPE_STATE ha_state related fields that need acknowledgements from DPU
    fn handle_dpu_ha_scope_state_update_npu_driven(&mut self, state: &mut State) -> Result<HaEvent, String> {
        let (internal, incoming, _) = state.get_all();
        // parse dpu ha scope state
        let Some(new_dpu_ha_scope_state) = self.get_dpu_ha_scope_state(incoming) else {
            // no valid state received from dpu, skip
            return Ok(HaEvent::None);
        };
        // get the NPU ha scope state
        let Some(mut npu_ha_scope_state) = self.get_npu_ha_scope_state(internal) else {
            info!("Cannot update STATE_DB/DASH_HA_SCOPE_STATE until it is populated with basic information",);
            return Ok(HaEvent::None);
        };

        npu_ha_scope_state.local_acked_asic_ha_state = Some(new_dpu_ha_scope_state.ha_role.clone());
        npu_ha_scope_state.local_acked_term = Some(new_dpu_ha_scope_state.ha_term.clone());
        self.dpu_ha_scope_state = Some(new_dpu_ha_scope_state);

        let fvs = swss_serde::to_field_values(&npu_ha_scope_state).map_err(|e| e.to_string())?;
        internal.get_mut(NpuDashHaScopeState::table_name()).clone_from(&fvs);

        Ok(HaEvent::DpuStateChanged)
    }

    /// Handle bulk sync update messages for this HA scope
    /// On standby DPU, we expect to receive bluk sync update messages that map to BulkSyncCompleted events
    /// On active DPU, we expect to receive bluk sync update messages that map to BulkSyncCompletedAck events
    fn handle_bulk_sync_update(&mut self, state: &mut State, key: &String) -> Result<HaEvent, String> {
        let (_internal, incoming, _outgoing) = state.get_all();
        let update: Option<BulkSyncUpdate> = self.decode_hascope_actor_message(incoming, key);

        if self.target_ha_scope_state == Some(TargetState::Standby) {
            if let Some(u) = update {
                if u.finished {
                    return Ok(HaEvent::BulkSyncCompleted);
                }
            }
        }
        Ok(HaEvent::None)
    }

    /// Handle DashFlowSyncSessionState updates for this HA scope
    /// Extract the session fields and update NPU HA scope state table
    /// Return HaEvent::BulkSyncCompleted when state is "completed", otherwise HaEvent::None
    fn handle_flow_sync_session_state_update(&mut self, state: &mut State, key: &String) -> Result<HaEvent, String> {
        let (internal, incoming, _outgoing) = state.get_all();

        let msg = incoming.get(key);
        let Some(msg) = msg else {
            return Err("Failed to get DashFlowSyncSessionState message".to_string());
        };

        let kfv = match msg.deserialize_data::<KeyOpFieldValues>() {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to deserialize DashFlowSyncSessionState KeyOpFieldValues: {}", e);
                return Err("Failed to deserialize DashFlowSyncSessionState".to_string());
            }
        };

        let session_state: DashFlowSyncSessionState = match swss_serde::from_field_values(&kfv.field_values) {
            Ok(state) => state,
            Err(e) => {
                error!(
                    "Failed to deserialize DashFlowSyncSessionState from field values: {}",
                    e
                );
                return Err("Failed to deserialize DashFlowSyncSessionState".to_string());
            }
        };

        // Extract session_id from the key
        let session_id = kfv.key.clone();
        let npu_session_id = self
            .get_npu_ha_scope_state(internal)
            .and_then(|s| s.flow_sync_session_id);
        if Some(session_id) != npu_session_id {
            // Not a bulk sync session owned by this HA scope
            return Ok(HaEvent::None);
        }

        // Update NPU HA scope state table with flow sync session info
        if let Err(e) = self.set_npu_flow_sync_session(
            state,
            &None,
            &Some(session_state.state.clone()),
            &Some(session_state.creation_time_in_ms),
            &None,
        ) {
            error!("Failed to update NPU flow sync session state: {}", e);
        }

        // Return BulkSyncCompleted when state is "completed"
        // Also send a notification to the peer
        if session_state.state == "completed" {
            let _ = self.send_bulk_sync_completed_to_peer(state);
            return Ok(HaEvent::BulkSyncCompleted);
        }

        Ok(HaEvent::None)
    }

    /// Handle HA state changed messages for this HA scope
    /// Store the new HA state of peer in Npu
    fn handle_ha_state_change(&mut self, state: &mut State, key: &String) -> Result<HaEvent, String> {
        let Some(ref _dash_ha_scope_config) = self.dash_ha_scope_config else {
            return Err("DASH HA scope config is initialized yet!".to_string());
        };

        let (internal, incoming, _outgoing) = state.get_all();
        let Some(mut npu_ha_scope_state) = self.get_npu_ha_scope_state(internal) else {
            info!("Cannot update STATE_DB/DASH_HA_SCOPE_STATE until it is populated with basic information",);
            return Ok(HaEvent::None);
        };
        let change: Option<HAStateChanged> = self.decode_hascope_actor_message(incoming, key);
        let Some(change) = change else {
            return Err("Failed to decode HAStateChanged message".to_string());
        };

        npu_ha_scope_state.peer_ha_state = Some(change.new_state);
        npu_ha_scope_state.peer_ha_state_last_updated_time_in_ms = Some(change.timestamp);
        npu_ha_scope_state.peer_term = Some(change.term);
        if self.target_ha_scope_state == Some(TargetState::Standby) {
            // Standby HA scope should follow the change of the peer term
            npu_ha_scope_state.local_target_term = npu_ha_scope_state.peer_term.clone();
        }

        // Push Update to DB
        let fvs = swss_serde::to_field_values(&npu_ha_scope_state).map_err(|e| e.to_string())?;
        internal.get_mut(NpuDashHaScopeState::table_name()).clone_from(&fvs);

        if self.peer_connected {
            return Ok(HaEvent::PeerStateChanged);
        } else {
            // we treat the first HaStateChanged message from the peer as a confirmation of connection
            self.peer_connected = true;
            return Ok(HaEvent::PeerConnected);
        }
    }

    /// Handle vote request messages for this HA scope
    /// Folloing procedure documented in <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-hld.md#73-primary-election>
    fn handle_vote_request(&mut self, state: &mut State, key: &str) {
        let response: &str;
        let source_actor_id = key.strip_prefix(VoteRequest::msg_key_prefix()).unwrap_or(key);
        let (internal, incoming, outgoing) = state.get_all();
        let request: Option<VoteRequest> = self.decode_hascope_actor_message(incoming, &key.to_string());
        let Some(request) = request else {
            error!("Failed to decode VoteRequest message");
            return;
        };
        let my_state = self.current_npu_ha_state(internal);
        let my_desired_state = self
            .dash_ha_scope_config
            .as_ref()
            .map(|c| DesiredHaState::try_from(c.desired_ha_state).unwrap_or(DesiredHaState::Unspecified))
            .unwrap_or(DesiredHaState::Unspecified);
        let my_term = self
            .get_npu_ha_scope_state(internal)
            .and_then(|s| s.local_target_term)
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);
        let peer_term = request.term.parse::<i32>().ok().unwrap_or(0);

        if my_desired_state == DesiredHaState::Standalone {
            response = "RetryLater";
        } else if my_state == HaState::Active {
            response = "BecomeStandby";
        } else if (my_desired_state == DesiredHaState::Dead && my_state == HaState::Dead)
            || my_state == HaState::Destroying
        {
            response = "BecomeStandalone";
        } else if my_state == HaState::Dead || my_state == HaState::Connecting {
            if self.retry_count < MAX_RETRIES {
                self.retry_count += 1;
                response = "RetryLater";
            } else {
                response = "BecomeStandalone";
            }
        } else if my_term > peer_term {
            response = "BecomeStandby";
        } else if my_term < peer_term {
            response = "BecomeActive";
        } else if my_desired_state == DesiredHaState::Active
            && DesiredHaState::from_str_name(&request.desired_state) == Some(DesiredHaState::Unspecified)
        {
            response = "BecomeStandby";
        } else if my_desired_state == DesiredHaState::Unspecified
            && DesiredHaState::from_str_name(&request.desired_state) == Some(DesiredHaState::Active)
        {
            response = "BecomeActive";
        } else {
            response = "RetryLater";
            if self.retry_count < MAX_RETRIES {
                self.retry_count += 1;
            } else {
                // TODO: fire alert to SDN controller;
            }
        }

        if response != "RetryLater" {
            // reset retry count when determining the final result;
            self.retry_count = 0;
        }
        if let Ok(msg) = VoteReply::new_actor_msg(&self.id, source_actor_id, response) {
            outgoing.send(outgoing.from_my_sp(HaScopeActor::name(), source_actor_id), msg);
        }
    }

    /// Hanlde vote reply messages for this HA scope
    /// Folloing procedure documented in <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-hld.md#73-primary-election>
    /// Map to HaEvent::VoteCompleted if the reponse is one of [BecomeActive | BecomeStandby | BecomeStandalone ] and set the target state
    fn handle_vote_reply(&mut self, state: &mut State, key: &str) -> Result<HaEvent, String> {
        let (_internal, incoming, _outgoing) = state.get_all();
        let reply: Option<VoteReply> = self.decode_hascope_actor_message(incoming, &key.to_string());
        let Some(reply) = reply else {
            return Err("Failed to decode VoteReply message".to_string());
        };

        match reply.response.as_str() {
            "BecomeActive" => {
                self.target_ha_scope_state = Some(TargetState::Active);
            }
            "BecomeStandby" => {
                self.target_ha_scope_state = Some(TargetState::Standby);
            }
            "BecomeStandalone" => {
                self.target_ha_scope_state = Some(TargetState::Standalone);
            }
            "RetryLater" => {
                // TODO: retry logic
            }
            _ => {
                return Ok(HaEvent::None);
            }
        }
        Ok(HaEvent::VoteCompleted)
    }

    /// Handle async self notification messages
    fn handle_self_notification(&mut self, state: &mut State, key: &str) -> Result<HaEvent, String> {
        let (_internal, incoming, _outgoing) = state.get_all();
        let notification: Option<SelfNotification> = self.decode_hascope_actor_message(incoming, &key.to_string());
        let Some(notification) = notification else {
            return Err("Failed to decode SelfNotification message".to_string());
        };

        if notification.event == "CheckPeerConnection" {
            return self.check_peer_connection_and_retry(state);
        }

        if let Some(event) = HaEvent::from_str(&notification.event) {
            return Ok(event);
        } else {
            return Err("Unknown event".to_string());
        }
    }

    fn current_npu_ha_state(&self, internal: &Internal) -> HaState {
        self.get_npu_ha_scope_state(internal)
            .and_then(|scope| scope.local_ha_state)
            .and_then(|s| HaState::from_str_name(&s))
            .unwrap_or(HaState::Dead)
    }

    fn current_npu_peer_ha_state(&self, internal: &Internal) -> HaState {
        self.get_npu_ha_scope_state(internal)
            .and_then(|scope| scope.peer_ha_state)
            .and_then(|s| HaState::from_str_name(&s))
            .unwrap_or(HaState::Dead)
    }

    fn apply_pending_state_side_effects(
        &mut self,
        state: &mut State,
        current_state: &HaState,
        pending_state: &HaState,
    ) -> Result<()> {
        let _internal = state.internal();
        match pending_state {
            HaState::Connected => {
                // Send VoteRequest to the peer to start primary election
                self.send_vote_request_to_peer(state)?;
            }
            HaState::PendingActiveActivation | HaState::PendingStandbyActivation => {
                let mut operations: Vec<(String, String)> = Vec::new();
                operations.push((Uuid::new_v4().to_string(), "activate_role".to_string()));
                self.update_npu_ha_scope_state_pending_operations(state, operations, Vec::new())?;
            }
            HaState::Standalone => {
                // TODO: Enter the being-standalone process
            }
            HaState::Active => {
                if *current_state == HaState::Standalone {
                    // If staring from Standalone, do bulk sync
                    let _ = self.add_bulk_sync_session(state);
                } else if *current_state == HaState::PendingActiveActivation {
                    // When starting from PendingActiveRoleActivation, no need to do bulk sync.
                    // Send BulkSyncCompleted signal to the peer immediately
                    self.send_bulk_sync_completed_to_peer(state)?;
                }

                // Activate Active role on DPU with a new term
                let _ = self.increment_npu_ha_scope_state_target_term(state);
                let _ = self.update_dpu_ha_scope_table_with_params(
                    state,
                    &HaRole::Active.as_str_name().to_string(),
                    false,
                    false,
                );
            }
            HaState::InitializingToStandby => {
                // Activate Standby role on DPU
                let _ = self.update_dpu_ha_scope_table_with_params(
                    state,
                    &HaRole::Standby.as_str_name().to_string(),
                    false,
                    false,
                );
            }
            HaState::SwitchingToActive => {
                // TODO: Send SwitchOver to the peer
            }
            HaState::Destroying => {
                // Activate Dead role on the DPU
                let _ = self.update_dpu_ha_scope_table_with_params(
                    state,
                    &HaRole::Dead.as_str_name().to_string(),
                    false,
                    false,
                );
            }
            _ => {}
        }
        Ok(())
    }

    fn drive_npu_state_machine(&mut self, state: &mut State, event: &HaEvent) -> Result<()> {
        info!("Drive NPU HA state machine based on {}", event.as_str());
        let Some(config) = self.dash_ha_scope_config.as_ref() else {
            return Ok(());
        };

        let current_state = self.current_npu_ha_state(state.internal());
        let mut event_to_use = event.clone();

        if *event == HaEvent::AdminStateChanged {
            if config.disabled {
                if current_state != HaState::Dead {
                    self.set_npu_local_ha_state(state, HaState::Dead, "admin disabled")?;
                    // Update DPU APPL_DB to activate Dead role on the DPU
                    let _ = self.update_dpu_ha_scope_table_with_params(
                        state,
                        &HaRole::Dead.as_str_name().to_string(),
                        false,
                        false,
                    );
                }
                return Ok(());
            } else {
                // Equivalent to launch
                event_to_use = HaEvent::Launch;
            }
        }

        let _desired_state = DesiredHaState::try_from(config.desired_ha_state).unwrap_or(DesiredHaState::Unspecified);
        let target_state = self.target_ha_scope_state.unwrap_or(TargetState::Dead);

        match self.next_state(state, &target_state, &current_state, &event_to_use) {
            Some((next_state, reason)) if next_state != current_state => {
                let pending_state = next_state;
                self.apply_pending_state_side_effects(state, &current_state, &pending_state)?;
                self.set_npu_local_ha_state(state, pending_state, reason)?;

                // send out HAStateChanged message to the peer
                let (internal, _incoming, outgoing) = state.get_all();
                let npu_state = self.get_npu_ha_scope_state(internal);
                let local_target_term = npu_state.as_ref().and_then(|s| s.local_target_term.as_deref());
                if let Some(peer_actor_id) = self.get_peer_actor_id() {
                    if let Ok(msg) = HAStateChanged::new_actor_msg(
                        &self.id,
                        current_state.as_str_name(),
                        pending_state.as_str_name(),
                        now_in_millis(),
                        local_target_term.unwrap_or("0"),
                    ) {
                        outgoing.send(outgoing.from_my_sp(HaScopeActor::name(), &peer_actor_id), msg);
                    }
                }

                // Send a state update message to the ha-set actor
                let owner = self
                    .dash_ha_scope_config
                    .as_ref()
                    .map(|c| c.owner)
                    .unwrap_or(HaOwner::Unspecified as i32);
                if let Some(ref npu_state) = npu_state {
                    if let Ok(msg) = HaScopeActorState::new_actor_msg(
                        &self.id,
                        owner,
                        npu_state,
                        &self.vdpu_id,
                        self.peer_vdpu_id.as_deref().unwrap_or(""),
                    ) {
                        if let Some(ha_set_id) = self.get_haset_id() {
                            outgoing.send(outgoing.from_my_sp(HaSetActor::name(), &ha_set_id), msg);
                        }
                    }
                }
            }
            _ => {
                // no state transition case
            }
        }

        Ok(())
    }

    fn next_state(
        &mut self,
        state: &mut State,
        target_state: &TargetState,
        current_state: &HaState,
        event: &HaEvent,
    ) -> Option<(HaState, &'static str)> {
        if *target_state == TargetState::Dead {
            return match current_state {
                HaState::Dead => None,
                HaState::Destroying => {
                    if self.dpu_ha_scope_state.as_ref().map(|s| s.ha_role.as_str()) == Some(HaRole::Dead.as_str_name())
                    {
                        // HaEvent::DpuStateChanged should trigger this branch
                        // When the DPU is in dead role, all traffic is drained
                        Some((HaState::Dead, "destroy completed"))
                    } else {
                        None
                    }
                }
                _ => Some((HaState::Destroying, "target dead")),
            };
        }

        match current_state {
            HaState::Dead => {
                // On launch
                if *event == HaEvent::Launch {
                    Some((HaState::Connecting, "ha scope initializing"))
                } else {
                    None
                }
            }
            HaState::Connecting => {
                // Go to Connected if successfully connected to the peer
                // Go to Standalone if detecting problem on the peer and the local DPU is healthy
                if *event == HaEvent::PeerConnected {
                    Some((HaState::Connected, "connectionn with peer established"))
                } else if *event == HaEvent::PeerLost {
                    Some((HaState::Standalone, "remote peer failure while connecting"))
                } else {
                    None
                }
            }
            HaState::Connected => {
                // On VoteCompleted, go to the target state
                if *event == HaEvent::VoteCompleted {
                    match target_state {
                        TargetState::Active => Some((HaState::InitializingToActive, "target active role")),
                        TargetState::Standby => Some((HaState::InitializingToStandby, "target standby role")),
                        TargetState::Standalone => Some((HaState::Standalone, "target standalone role")),
                        TargetState::Dead => None, // Target Dead case should be handled at the beginning of the function
                    }
                } else {
                    None
                }
            }
            HaState::InitializingToActive => {
                // On Peer moving to InitializingToStandby, go to PendingActiveRoleActivation
                // Go to Standalone if detecting problem on the peer and the local DPU is healthy
                // Go to Standby if detecting problem locally
                if *event == HaEvent::PeerStateChanged
                    && self.current_npu_peer_ha_state(state.internal()) == HaState::InitializingToStandby
                {
                    Some((HaState::PendingActiveActivation, "peer is ready"))
                } else if *event == HaEvent::PeerLost {
                    Some((HaState::Standalone, "remote peer failure during initialization"))
                } else if *event == HaEvent::LocalFailure {
                    Some((HaState::Standby, "local failure while init active"))
                } else {
                    None
                }
            }
            HaState::PendingActiveActivation => {
                // On receiving approval from SDN controller, go to Active
                if *event == HaEvent::PendingRoleActivationApproved {
                    Some((HaState::Active, "received approval from SDN controller"))
                } else {
                    None
                }
            }
            HaState::Active => {
                if *target_state == TargetState::Standby {
                    Some((HaState::SwitchingToStandby, "planned switchover to standby"))
                } else if *event == HaEvent::PeerLost {
                    Some((HaState::Standalone, "peer failure while active"))
                } else if *event == HaEvent::LocalFailure {
                    Some((HaState::Standby, "local failure while active"))
                } else {
                    None
                }
            }
            HaState::SwitchingToStandby => {
                if *event == HaEvent::PeerLost {
                    Some((HaState::Standalone, "peer lost during switchover to standby"))
                } else if *event == HaEvent::PeerStateChanged
                    && self.current_npu_peer_ha_state(state.internal()) == HaState::Active
                {
                    Some((HaState::Standby, "peer has been active"))
                } else {
                    None
                }
            }
            HaState::Standby => {
                if *target_state == TargetState::Active {
                    Some((HaState::SwitchingToActive, "planned switchover to active"))
                } else if *event == HaEvent::PeerLost {
                    Some((HaState::Standalone, "peer failure while standby"))
                } else {
                    None
                }
            }
            HaState::SwitchingToActive => {
                if *event == HaEvent::PeerLost {
                    Some((HaState::Standalone, "peer lost during switchover to active"))
                } else if *event == HaEvent::PeerStateChanged
                    && self.current_npu_peer_ha_state(state.internal()) == HaState::SwitchingToStandby
                {
                    Some((HaState::Active, "switchover to active complete"))
                } else {
                    None
                }
            }
            HaState::InitializingToStandby => {
                if *event == HaEvent::BulkSyncCompleted {
                    Some((HaState::PendingStandbyActivation, "bulk sync completed (standby)"))
                } else if *event == HaEvent::LocalFailure {
                    Some((HaState::Standby, "local failure while init standby"))
                } else {
                    None
                }
            }
            HaState::PendingStandbyActivation => {
                // On receiving approval from SDN controller, go to Active
                if *event == HaEvent::PendingRoleActivationApproved {
                    Some((HaState::Standby, "received approval from SDN controller"))
                } else {
                    None
                }
            }
            HaState::Standalone => {
                // Note: PeerActive and PeerStandby events don't exist in HaEvent enum
                // This branch needs to be updated based on actual event definitions
                None
            }
            HaState::Destroying => {
                if self.dpu_ha_scope_state.as_ref().map(|s| s.ha_role.as_str()) == Some(HaRole::Dead.as_str_name()) {
                    // HaEvent::DpuStateChanged should trigger this branch
                    Some((HaState::Dead, "resources drained"))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Check if the peer HA scope is connected
    /// If not, send a self notification to schedule an execution of the same function for later
    /// Upon exceeding retry count threshold, signal a PeerLost event
    fn check_peer_connection_and_retry(&mut self, state: &mut State) -> Result<HaEvent, String> {
        if !self.peer_connected {
            if self.retry_count < MAX_RETRIES {
                // retry sending peer with HAStateChanged Registration Message
                self.retry_count += 1;
                // register messages from the peer ha scope actor
                let _ = self.register_to_hascope_actor(state.outgoing(), true);

                // Send a signal to itself to schedule a check later
                let outgoing = state.outgoing();
                if let Ok(msg) = SelfNotification::new_actor_msg(&self.id, "CheckPeerConnection") {
                    outgoing.send_with_delay(
                        outgoing.from_my_sp(HaScopeActor::name(), &self.id),
                        msg,
                        Duration::from_secs(RETRY_INTERVAL.into()),
                    );
                }
            } else {
                // reset retry count
                self.retry_count = 0;
                return Ok(HaEvent::PeerLost);
            }
        } else {
            // nop-op but resetting retry count, if the peer is connected
            self.retry_count = 0;
        }
        return Ok(HaEvent::None);
    }

    /// Send a VoteRequest message to the peer HA scope actor for primary election
    /// The term and state come from NPU HA scope state table
    /// The desired state comes from dash_ha_scope_config
    fn send_vote_request_to_peer(&self, state: &mut State) -> Result<()> {
        let (internal, _incoming, outgoing) = state.get_all();

        let Some(peer_actor_id) = self.get_peer_actor_id() else {
            info!("Cannot send VoteRequest to peer: peer actor ID not available");
            return Ok(());
        };

        let Some(npu_ha_scope_state) = self.get_npu_ha_scope_state(internal) else {
            info!("Cannot send VoteRequest: NPU HA scope state not available");
            return Ok(());
        };

        let Some(ref config) = self.dash_ha_scope_config else {
            info!("Cannot send VoteRequest: dash_ha_scope_config not available");
            return Ok(());
        };

        // Get term from NPU HA scope state (convert from String to int)
        let term: String = npu_ha_scope_state
            .local_target_term
            .as_ref()
            .map_or("0", |v| v)
            .to_string();

        // Get current state from NPU HA scope state
        let current_state = npu_ha_scope_state.local_ha_state.as_deref().unwrap_or("dead");

        // Get desired state from config
        let desired_state = DesiredHaState::try_from(config.desired_ha_state)
            .map(|s| s.as_str_name())
            .unwrap_or("unspecified");

        let msg = VoteRequest::new_actor_msg(&self.id, &peer_actor_id, &term, current_state, desired_state)?;

        outgoing.send(outgoing.from_my_sp(HaScopeActor::name(), &peer_actor_id), msg);
        info!(
            "Sent VoteRequest to peer {}: term={}, state={}, desired_state={}",
            peer_actor_id, term, current_state, desired_state
        );

        Ok(())
    }

    /// Send a BulkSyncUpdate message with finished=true to the peer HA scope actor
    /// This signals to the peer that bulk sync is complete (e.g., when no actual sync is needed)
    fn send_bulk_sync_completed_to_peer(&self, state: &mut State) -> Result<()> {
        let outgoing = state.outgoing();

        let Some(peer_actor_id) = self.get_peer_actor_id() else {
            info!("Cannot send BulkSyncCompleted to peer: peer actor ID not available");
            return Ok(());
        };

        let msg = BulkSyncUpdate::new_actor_msg(
            &self.id,
            &peer_actor_id,
            true, // finished: true
        )?;

        outgoing.send(outgoing.from_my_sp(HaScopeActor::name(), &peer_actor_id), msg);
        info!("Sent BulkSyncCompleted to peer {}", peer_actor_id);

        Ok(())
    }

    /// Add a new entry in DASH_FLOW_SYNC_SESSION_TABLE to start a bulk sync session
    fn add_bulk_sync_session(&mut self, state: &mut State) -> Result<Option<String>> {
        let (_internal, incoming, outgoing) = state.get_all();

        let ha_set_id = self.get_haset_id().unwrap();
        let Some(haset) = self.get_haset(incoming) else {
            debug!("HA-SET {} has not been received. Cannot do bulk sync!", &ha_set_id);
            return Ok(None);
        };

        let bulk_sync_session = DashFlowSyncSessionTable {
            ha_set_id: ha_set_id.clone(),
            target_server_ip: haset.ha_set.peer_ip.clone(),
            target_server_port: haset
                .ha_set
                .cp_data_channel_port
                .expect("cp_data_channel_port must be configured"),
        };

        let session_id = Uuid::new_v4().to_string();
        let fv = swss_serde::to_field_values(&bulk_sync_session)?;
        let kfv = KeyOpFieldValues {
            key: session_id.clone(),
            operation: KeyOperation::Set,
            field_values: fv,
        };

        let msg = ActorMessage::new(session_id.clone(), &kfv)?;
        outgoing.send(outgoing.common_bridge_sp::<DashFlowSyncSessionTable>(), msg);

        // update NPU HA scope state table
        if let Err(e) = self.set_npu_flow_sync_session(
            state,
            &Some(session_id.clone()),
            &None,
            &Some(now_in_millis()),
            &Some(haset.ha_set.peer_ip),
        ) {
            return Err(e);
        }

        return Ok(Some(session_id));
    }
}

impl Actor for HaScopeActor {
    #[instrument(name="handle_message", level="info", skip_all, fields(actor=format!("ha-scope/{}", self.id), key=key))]
    async fn handle_message(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        let (internal, incoming, outgoing) = state.get_all();

        if key == Self::table_name() {
            // Retrieve the config update from the incoming message
            // so that we can determin whether using DPU-driven mode or NPU-driven mode later
            let kfv: KeyOpFieldValues = incoming.get_or_fail(key)?.deserialize_data()?;

            if kfv.operation == KeyOperation::Del {
                // cleanup resources before stopping
                if let Err(e) = self.do_cleanup(state) {
                    error!("Failed to cleanup HaScopeActor resources: {}", e);
                }
                context.stop();
                return Ok(());
            }
            let first_time = self.dash_ha_scope_config.is_none();

            if first_time {
                // directly update the config at the first time
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
                let npu_state = self.get_npu_ha_scope_state(internal).unwrap_or_default();
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
            }
        }

        if self.dash_ha_scope_config.is_none() {
            return Ok(());
        }

        if self.dash_ha_scope_config.as_ref().map(|c| c.owner) == Some(HaOwner::Dpu as i32) {
            // this is a dpu driven ha scope.
            if key == Self::table_name() {
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
        } else {
            // Npu driven HA scope message handling, map messages to HaEvents
            let mut event: Option<HaEvent> = None;
            if key == Self::table_name() {
                match self.handle_dash_ha_scope_config_table_message_npu_driven(state, key, context) {
                    Ok(incoming_event) => {
                        event = Some(incoming_event);
                    }
                    Err(_e) => {
                        error!("Error when processing HA Scope Config Table Update!")
                    }
                }
            } else if key.starts_with(DpuDashHaScopeState::table_name()) {
                // Update NPU ha scope state based on dpu ha scope state update
                match self.handle_dpu_ha_scope_state_update_npu_driven(state) {
                    Ok(incoming_event) => {
                        event = Some(incoming_event);
                    }
                    Err(_e) => {
                        error!("Error when processing DPU HA Scope State Update!")
                    }
                }
            } else if HaSetActorState::is_my_msg(key) {
                match self.handle_haset_state_update_npu_driven_mode(state) {
                    Ok(incoming_event) => {
                        event = Some(incoming_event);
                    }
                    Err(_e) => {
                        error!("Invalid HA Set State Update!")
                    }
                }
            } else if VDpuActorState::is_my_msg(key) {
                match self.handle_vdpu_state_update_npu_driven_mode(state, context).await {
                    Ok(incoming_event) => {
                        event = Some(incoming_event);
                    }
                    Err(_e) => {
                        error!("Invalid VDpu State Update!")
                    }
                }
            } else if BulkSyncUpdate::is_my_msg(key) {
                match self.handle_bulk_sync_update(state, &key.to_string()) {
                    Ok(incoming_event) => {
                        event = Some(incoming_event);
                    }
                    Err(_e) => {
                        error!("Invalid Bulk Sync Update!")
                    }
                }
            } else if key.starts_with(DashFlowSyncSessionState::table_name()) {
                match self.handle_flow_sync_session_state_update(state, &key.to_string()) {
                    Ok(incoming_event) => {
                        event = Some(incoming_event);
                    }
                    Err(e) => {
                        error!("Invalid Flow Sync Session State Update: {}", e)
                    }
                }
            } else if VoteRequest::is_my_msg(key) {
                self.handle_vote_request(state, key);
                event = Some(HaEvent::None);
            } else if VoteReply::is_my_msg(key) {
                match self.handle_vote_reply(state, key) {
                    Ok(incoming_event) => {
                        event = Some(incoming_event);
                    }
                    Err(_e) => {
                        error!("Invalid Vote Reply Update!")
                    }
                }
            } else if HAStateChanged::is_my_msg(key) {
                match self.handle_ha_state_change(state, &key.to_string()) {
                    Ok(incoming_event) => {
                        event = Some(incoming_event);
                    }
                    Err(_e) => {
                        error!("Invalid HA State Change!")
                    }
                }
            } else if ActorRegistration::is_my_msg(key, RegistrationType::HAStateChanged) {
                match self.handle_ha_scope_registration(state, key).await {
                    Ok(incoming_event) => {
                        event = Some(incoming_event);
                    }
                    Err(_e) => {
                        error!("Invalid HA Scope Registration!")
                    }
                }
            } else if SelfNotification::is_my_msg(key) {
                match self.handle_self_notification(state, key) {
                    Ok(incoming_event) => {
                        event = Some(incoming_event);
                    }
                    Err(_e) => {
                        error!("Invalid Self Notification!")
                    }
                }
            }

            // drive the state machine based on the information gained from message processing
            self.drive_npu_state_machine(state, &event.unwrap_or(HaEvent::None))?;
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
