use crate::actors::ha_scope::INLINE_SYNC_PKT_DROP_ALERT_THRESHOLD;
use crate::actors::{spawn_consumer_bridge_for_actor, DbBasedActor};
use crate::db_structs::*;
use crate::ha_actor_messages::*;
use anyhow::{anyhow, Result};
use sonic_common::SonicDbTable;
use sonic_dash_api_proto::decode_from_field_values;
use sonic_dash_api_proto::ha_scope_config::{DesiredHaState, HaScopeConfig};
use sonic_dash_api_proto::types::{HaOwner, HaRole, HaState};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use swbus_actor::{state::internal::Internal, ActorMessage, Context, State};
use swss_common::{KeyOpFieldValues, KeyOperation};
use tracing::{debug, error, info};
use uuid::Uuid;

use super::base::HaScopeBase;
use super::{HaEvent, HaScopeActor, TargetState, BULK_SYNC_TIMEOUT, MAX_RETRIES, RETRY_INTERVAL};

/// Convert an `HaRole` protobuf enum string name (e.g. `"HA_ROLE_ACTIVE"`) to its
/// lowercase equivalent (e.g. `"active"`).  Returns the input unchanged when the
/// `HA_ROLE_` prefix is absent.
fn ha_role_to_string(ha_role: &str) -> String {
    ha_role.strip_prefix("HA_ROLE_").unwrap_or(ha_role).to_lowercase()
}

pub struct NpuHaScopeActor {
    pub(super) base: HaScopeBase,
    /// Target state that HAmgrd should transition to upon HA events
    pub(super) target_ha_scope_state: Option<TargetState>,
    /// Retry count
    pub(super) retry_count: u32,
    /// Is peer connected?
    pub(super) peer_connected: bool,
    /// Counter object IDs collected from COUNTERS_ENI_NAME_MAP (values of the ENI-to-OID map)
    pub(super) counter_object_ids: HashSet<String>,
    /// Counter statistics for tracked object IDs, keyed by object ID
    pub(super) counter_stats: HashMap<String, HashMap<String, String>>,
}

impl NpuHaScopeActor {
    pub fn new(base: HaScopeBase) -> Self {
        Self {
            base,
            target_ha_scope_state: None,
            retry_count: 0,
            peer_connected: false,
            counter_object_ids: HashSet::new(),
            counter_stats: HashMap::new(),
        }
    }

    /// Get the peer HA scope actor's ServicePath.
    fn peer_sp(&self) -> Option<swbus_edge::swbus_proto::swbus::ServicePath> {
        self.base.get_peer_actor_id()?;
        if let Some(sp) = self.base.get_peer_sp() {
            Some(sp.clone())
        } else {
            error!("The Service Path of peer actor is not resolved!");
            None
        }
    }

    /// Main message dispatch for NPU-driven mode
    pub async fn handle_message_inner(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        // Npu driven HA scope message handling, map messages to HaEvents
        let mut event: Option<HaEvent> = None;
        if key == HaScopeActor::table_name() {
            match self.handle_dash_ha_scope_config_table_message(state, key, context) {
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
            match self.handle_haset_state_update(state, context, key).await {
                Ok(incoming_event) => {
                    event = Some(incoming_event);
                }
                Err(_e) => {
                    error!("Invalid HA Set State Update!")
                }
            }
        } else if VDpuActorState::is_my_msg(key) {
            match self.handle_vdpu_state_update(state, context).await {
                Ok(incoming_event) => {
                    event = Some(incoming_event);
                }
                Err(_e) => {
                    error!("Invalid VDpu State Update!")
                }
            }
        } else if BulkSyncUpdate::is_my_msg(key) {
            match self.handle_bulk_sync_update(state, key) {
                Ok(incoming_event) => {
                    event = Some(incoming_event);
                }
                Err(_e) => {
                    error!("Invalid Bulk Sync Update!")
                }
            }
        } else if key.starts_with(DashFlowSyncSessionState::table_name()) {
            match self.handle_flow_sync_session_state_update(state, key) {
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
        } else if SwitchoverRequest::is_my_msg(key) {
            match self.handle_switchover_request(state, key) {
                Ok(incoming_event) => {
                    event = Some(incoming_event);
                }
                Err(_e) => {
                    error!("Invalid Switchover Request!")
                }
            }
        } else if HaScopeActorState::is_my_msg(key) {
            match self.handle_ha_state_change(state, key) {
                Ok(incoming_event) => {
                    event = Some(incoming_event);
                }
                Err(_e) => {
                    error!("Invalid HA State Change!")
                }
            }
        } else if PeerHeartbeat::is_my_msg(key) {
            match self.handle_peer_heartbeat(state, key).await {
                Ok(incoming_event) => {
                    event = Some(incoming_event);
                }
                Err(_e) => {
                    error!("Invalid HA Scope Heartbeat!")
                }
            }
        } else if ShutdownRequest::is_my_msg(key) {
            match self.handle_shutdown_request(state, key) {
                Ok(incoming_event) => {
                    event = Some(incoming_event);
                }
                Err(_e) => {
                    error!("Invalid Shutdown Request!")
                }
            }
        } else if ShutdownReply::is_my_msg(key) {
            match self.handle_shutdown_reply(state, key) {
                Ok(incoming_event) => {
                    event = Some(incoming_event);
                }
                Err(_e) => {
                    error!("Invalid Shutdown Reply!")
                }
            }
        } else if DPURequestEnterStandalone::is_my_msg(key) {
            match self.handle_dpu_request_enter_standalone(state, key) {
                Ok(incoming_event) => {
                    event = Some(incoming_event);
                }
                Err(_e) => {
                    error!("Invalid DPURequestEnterStandalone!")
                }
            }
        } else if DPURequestEnterStandaloneReply::is_my_msg(key) {
            match self.handle_dpu_request_enter_standalone_reply(state, key) {
                Ok(incoming_event) => {
                    event = Some(incoming_event);
                }
                Err(_e) => {
                    error!("Invalid DPURequestEnterStandaloneReply!")
                }
            }
        } else if SelfNotification::is_my_msg(key) {
            match self.handle_self_notification(state, key, context).await {
                Ok(incoming_event) => {
                    event = Some(incoming_event);
                }
                Err(_e) => {
                    error!("Invalid Self Notification!")
                }
            }
        } else if key.starts_with(CountersEniNameMapTable::table_name()) {
            match self.handle_counters_eni_name_map_update(state, key) {
                Ok(incoming_event) => {
                    event = Some(incoming_event);
                }
                Err(e) => {
                    error!("Invalid CountersEniNameMap Update: {}", e)
                }
            }
        } else if key.starts_with(CountersTable::table_name()) {
            match self.handle_counters_table_update(state, key) {
                Ok(incoming_event) => {
                    event = Some(incoming_event);
                }
                Err(e) => {
                    error!("Invalid Counters Table Update: {}", e)
                }
            }
        }

        // drive the state machine based on the information gained from message processing
        self.drive_npu_state_machine(state, &event.unwrap_or(HaEvent::None))?;

        Ok(())
    }
}

// NPU message handlers
impl NpuHaScopeActor {
    /// Handles updates to the DASH_HA_SCOPE_CONFIG_TABLE in the case of NPU-driven HA.
    fn handle_dash_ha_scope_config_table_message(
        &mut self,
        state: &mut State,
        key: &str,
        _context: &mut Context,
    ) -> Result<HaEvent> {
        let (_internal, incoming, _outgoing) = state.get_all();

        // Retrieve the config update from the incoming message
        let kfv: KeyOpFieldValues = incoming.get_or_fail(key)?.deserialize_data()?;
        let dash_ha_scope_config: HaScopeConfig = decode_from_field_values(&kfv.field_values)?;
        let old_ha_scope_config = self.base.dash_ha_scope_config.clone().unwrap_or_default();

        // Update internal config
        self.base.dash_ha_scope_config = Some(dash_ha_scope_config);

        // this is not a ha_scope for the target vDPU. Skip
        if !self.base.vdpu_is_managed(incoming) {
            return Ok(HaEvent::None);
        }

        // admin state change
        if old_ha_scope_config.disabled != self.base.dash_ha_scope_config.as_ref().unwrap().disabled {
            return Ok(HaEvent::AdminStateChanged);
        }

        // desired state change
        if old_ha_scope_config.desired_ha_state != self.base.dash_ha_scope_config.as_ref().unwrap().desired_ha_state {
            // Note: assume that desired state change in config has highest priority
            let value = self.base.dash_ha_scope_config.as_ref().unwrap().desired_ha_state;
            let desired_state =
                DesiredHaState::try_from(value).map_err(|_| anyhow!("invalid DesiredHaState value: {}", value))?;
            match desired_state {
                DesiredHaState::Active => {
                    self.target_ha_scope_state = Some(TargetState::Active);
                }
                DesiredHaState::Unspecified => {
                    self.target_ha_scope_state = Some(TargetState::Standby);
                }
                DesiredHaState::Dead => {
                    self.target_ha_scope_state = Some(TargetState::Dead);
                }
                _ => {}
            }
            return Ok(HaEvent::DesiredStateChanged);
        }

        // update operation list if approved_pending_operation_ids is not empty
        let approved_pending_operation_ids = self
            .base
            .dash_ha_scope_config
            .as_ref()
            .unwrap()
            .approved_pending_operation_ids
            .clone();

        if !approved_pending_operation_ids.is_empty() {
            match self.base.update_npu_ha_scope_state_pending_operations(
                state,
                Vec::new(),
                approved_pending_operation_ids,
            ) {
                Ok(approved_operation_types) => {
                    if approved_operation_types.len() > 1 {
                        error!("Multiple operations are approved at the same time is not expected in NPU-driven mode")
                    }

                    if let Some(first_op) = approved_operation_types.first() {
                        return match first_op.as_str() {
                            "activate_role" => Ok(HaEvent::PendingRoleActivationApproved),
                            "flow_reconcile" => Ok(HaEvent::FlowReconciliationApproved),
                            "switchover" => {
                                let switchover_id = self
                                    .base
                                    .get_npu_ha_scope_state(state.internal())
                                    .and_then(|s| s.switchover_id)
                                    .unwrap_or_default();
                                self.set_npu_switchover_state(state, &switchover_id, "approved")?;
                                Ok(HaEvent::SwitchoverApproved)
                            }
                            _ => Ok(HaEvent::None),
                        };
                    }
                }
                Err(_e) => {
                    error!("Encountered error when updating pending operations!");
                    return Ok(HaEvent::None);
                }
            }
        }

        Ok(HaEvent::None)
    }

    /// Handles registration messages from peer HA Scope actor
    /// Respond with HaScopeActorState when the actor-self is ready
    async fn handle_peer_heartbeat(&mut self, state: &mut State, key: &str) -> Result<HaEvent> {
        let (internal, incoming, outgoing) = state.get_all();
        let entry = incoming
            .get_entry(key)
            .ok_or_else(|| anyhow!("Entry not found for key: {}", key))?;

        let Some(npu_ha_scope_state) = self.base.get_npu_ha_scope_state(internal) else {
            info!("Cannot respond to the peer until STATE_DB/DASH_HA_SCOPE_STATE is populated with basic information",);
            return Ok(HaEvent::None);
        };

        let owner = self
            .base
            .dash_ha_scope_config
            .as_ref()
            .map(|c| c.owner)
            .unwrap_or(HaOwner::Unspecified as i32);
        let msg = HaScopeActorState::new_actor_msg(
            &self.base.id,
            owner,
            npu_ha_scope_state.local_ha_state.as_deref().unwrap_or(""),
            npu_ha_scope_state.local_ha_state_last_updated_time_in_ms.unwrap_or(0),
            npu_ha_scope_state.local_target_term.as_deref().unwrap_or("0"),
            &self.base.vdpu_id,
            self.base.peer_vdpu_id.as_deref().unwrap_or(""),
        )?;
        outgoing.send(entry.source.clone(), msg);

        Ok(HaEvent::None)
    }

    /// Handles VDPU state update messages for this HA scope.
    /// If the vdpu is unmanaged, the actor is put in dormant state.
    /// Otherwise, map the update to a HaEvent
    async fn handle_vdpu_state_update(&mut self, state: &mut State, context: &mut Context) -> Result<HaEvent> {
        let (internal, incoming, _outgoing) = state.get_all();
        let Some(vdpu) = self.base.get_vdpu(incoming) else {
            error!("Failed to retrieve vDPU {} from incoming state", &self.base.vdpu_id);
            return Err(anyhow!("Failed to retrieve vDPU from incoming state"));
        };

        if !vdpu.dpu.is_managed {
            info!("vDPU {} is unmanaged. Put actor in dormant state", &self.base.vdpu_id);
            return Ok(HaEvent::None);
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

        let mut first_time: bool = false;
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
            // subscribe to dpu DASH_FLOW_SYNC_SESSION_STATE
            self.base.bridges.push(
                spawn_consumer_bridge_for_actor::<DashFlowSyncSessionState>(
                    context.get_edge_runtime().clone(),
                    HaScopeActor::name(),
                    Some(&self.base.id),
                    false,
                )
                .await?,
            );
            // subscribe to dpu COUNTERS_ENI_NAME_MAP
            self.base.bridges.push(
                spawn_consumer_bridge_for_actor::<CountersEniNameMapTable>(
                    context.get_edge_runtime().clone(),
                    HaScopeActor::name(),
                    Some(&self.base.id),
                    true,
                )
                .await?,
            );
            // subscribe to dpu COUNTERS
            self.base.bridges.push(
                spawn_consumer_bridge_for_actor::<CountersTable>(
                    context.get_edge_runtime().clone(),
                    HaScopeActor::name(),
                    Some(&self.base.id),
                    false,
                )
                .await?,
            );
            first_time = true;
        }

        // update basic info of NPU HA scope state
        self.base.update_npu_ha_scope_state_base(state)?;

        if first_time {
            Ok(HaEvent::Launch)
        } else {
            match vdpu.up {
                true => Ok(HaEvent::None),
                false => Ok(HaEvent::LocalFailure),
            }
        }
    }

    /// Handles HaSet state update messages for this HA scope.
    /// Map the update to a HaEvent
    /// Register messages from a peer HA scope actor
    async fn handle_haset_state_update(
        &mut self,
        state: &mut State,
        context: &mut Context,
        key: &str,
    ) -> Result<HaEvent> {
        let Some(ha_set) = self.base.get_haset(state.incoming()) else {
            error!("Invalid HA set state update!");
            return Ok(HaEvent::None);
        };
        let peer_vdpu_id = self.base.get_remote_vdpu_id(&ha_set);

        // Extract the pinned BFD state for the local vDPU from the HA set state
        self.base.pinned_bfd_state = ha_set
            .vdpu_ids
            .iter()
            .position(|id| id == &self.base.vdpu_id)
            .and_then(|idx| ha_set.pinned_vdpu_bfd_probe_states.get(idx))
            .filter(|s| !s.is_empty())
            .cloned();

        let first_time = self.base.peer_vdpu_id.is_none();
        if first_time {
            // bookkeep the SP of local HA set actor at the first time
            if let Some(entry) = state.incoming().get_entry(key) {
                let sp = entry.source.clone();
                if !self.base.ha_set_sp.iter().any(|existing| existing == &sp) {
                    self.base.ha_set_sp.push(sp);
                }
            }

            // update the peer vDPU ID
            self.base.peer_vdpu_id = peer_vdpu_id.clone();

            // Resolve the remote peer's ServicePath via REMOTE_DPU + swbusd
            match self.base.resolve_peer_sp(context.get_edge_runtime()).await {
                Ok(sp) => {
                    info!("Resolved peer HA scope SP: {}", sp.to_longest_path());
                    self.base.peer_sp = Some(sp);
                }
                Err(e) => {
                    error!("Failed to resolve peer SP: {e}. Will retry on next heartbeat.");
                }
            }
        } else if self.base.peer_vdpu_id != peer_vdpu_id {
            // Got a new peer HA scope actor other than the old one
            info!("Got a new HA Peer: {:?}", peer_vdpu_id);
            // update the peer vDPU ID
            self.base.peer_vdpu_id = peer_vdpu_id.clone();
            self.base.peer_sp = None;
            // Resolve the remote peer's ServicePath via REMOTE_DPU + swbusd
            match self.base.resolve_peer_sp(context.get_edge_runtime()).await {
                Ok(sp) => {
                    info!("Resolved peer HA scope SP: {}", sp.to_longest_path());
                    self.base.peer_sp = Some(sp);
                }
                Err(e) => {
                    error!("Failed to resolve peer SP: {e}. Will retry on next heartbeat.");
                    self.peer_connected = false;
                    // Send a signal to itself to schedule a check later
                    self.send_self_notification(state, "CheckPeerConnection", RETRY_INTERVAL)?;
                }
            }
        }

        // the ha_scope is not managing the target vDPU. Skip updating NPU HA scope state
        if !self.base.vdpu_is_managed(state.incoming()) {
            return Ok(HaEvent::None);
        }
        // update basic info of NPU HA scope state
        let _ = self.base.update_npu_ha_scope_state_base(state);

        if first_time {
            Ok(HaEvent::Launch)
        } else {
            match ha_set.up {
                true => Ok(HaEvent::None),
                false => Ok(HaEvent::PeerLost),
            }
        }
    }

    /// Handles DPU DASH_HA_SCOPE_STATE update messages for this HA scope.
    /// Update NPU DASH_HA_SCOPE_STATE ha_state related fields that need acknowledgements from DPU
    fn handle_dpu_ha_scope_state_update_npu_driven(&mut self, state: &mut State) -> Result<HaEvent> {
        let (internal, incoming, _) = state.get_all();
        // parse dpu ha scope state
        let Some(new_dpu_ha_scope_state) = self.base.get_dpu_ha_scope_state(incoming) else {
            // no valid state received from dpu, skip
            return Ok(HaEvent::None);
        };
        // get the NPU ha scope state
        let Some(mut npu_ha_scope_state) = self.base.get_npu_ha_scope_state(internal) else {
            info!("Cannot update STATE_DB/DASH_HA_SCOPE_STATE until it is populated with basic information",);
            return Ok(HaEvent::None);
        };

        info!(
            "Received New DPU_HA_SCOPE_STATE_TABLE: ha_role-{}, ha_term-{:?}",
            new_dpu_ha_scope_state.ha_role.clone(),
            new_dpu_ha_scope_state.ha_term.clone()
        );

        npu_ha_scope_state.local_acked_asic_ha_state = Some(new_dpu_ha_scope_state.ha_role.clone());
        npu_ha_scope_state.local_acked_term = new_dpu_ha_scope_state.ha_term.clone();
        self.base.dpu_ha_scope_state = Some(new_dpu_ha_scope_state);

        let fvs = swss_serde::to_field_values(&npu_ha_scope_state)?;
        internal.get_mut(NpuDashHaScopeState::table_name()).clone_from(&fvs);

        Ok(HaEvent::DpuStateChanged)
    }

    /// Handle bulk sync update messages for this HA scope
    /// On standby DPU, we expect to receive bulk sync update messages that map to BulkSyncCompleted events
    /// On active DPU, we expect to receive bulk sync update messages that map to BulkSyncCompletedAck events
    fn handle_bulk_sync_update(&mut self, state: &mut State, key: &str) -> Result<HaEvent> {
        let (_internal, incoming, _outgoing) = state.get_all();
        let update: Option<BulkSyncUpdate> = self.base.decode_hascope_actor_message(incoming, key);

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
    fn handle_flow_sync_session_state_update(&mut self, state: &mut State, key: &str) -> Result<HaEvent> {
        let (internal, incoming, _outgoing) = state.get_all();

        let msg = incoming.get(key);
        let Some(msg) = msg else {
            return Err(anyhow!("Failed to get DashFlowSyncSessionState message"));
        };

        let kfv = msg.deserialize_data::<KeyOpFieldValues>()?;

        let session_state: DashFlowSyncSessionState = swss_serde::from_field_values(&kfv.field_values)?;

        // Extract session_id from the key
        let session_id = kfv.key.clone();
        let npu_session_id = self
            .base
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
    fn handle_ha_state_change(&mut self, state: &mut State, key: &str) -> Result<HaEvent> {
        let Some(ref _dash_ha_scope_config) = self.base.dash_ha_scope_config else {
            return Err(anyhow!("DASH HA scope config is not initialized yet!"));
        };

        let (internal, incoming, _outgoing) = state.get_all();
        let Some(mut npu_ha_scope_state) = self.base.get_npu_ha_scope_state(internal) else {
            info!("Cannot update STATE_DB/DASH_HA_SCOPE_STATE until it is populated with basic information",);
            return Ok(HaEvent::None);
        };
        let change: Option<HaScopeActorState> = self.base.decode_hascope_actor_message(incoming, key);
        let Some(change) = change else {
            return Err(anyhow!("Failed to decode HaScopeActorState message"));
        };

        npu_ha_scope_state.peer_ha_state = Some(change.new_state);
        npu_ha_scope_state.peer_ha_state_last_updated_time_in_ms = Some(change.timestamp);
        npu_ha_scope_state.peer_term = Some(change.term);
        if self.target_ha_scope_state == Some(TargetState::Standby) {
            // Standby HA scope should follow the change of the peer term
            npu_ha_scope_state.local_target_term = npu_ha_scope_state.peer_term.clone();
        }

        // Push Update to DB
        let fvs = swss_serde::to_field_values(&npu_ha_scope_state)?;
        internal.get_mut(NpuDashHaScopeState::table_name()).clone_from(&fvs);

        if self.peer_connected {
            Ok(HaEvent::PeerStateChanged)
        } else {
            // we treat the first HaScopeActorState message from the peer as a confirmation of connection
            self.peer_connected = true;
            Ok(HaEvent::PeerConnected)
        }
    }

    /// Handle shutdown request messages for this HA scope.
    /// Accepts the request if the current state is Active, rejects otherwise.
    fn handle_shutdown_request(&mut self, state: &mut State, key: &str) -> Result<HaEvent> {
        let (internal, incoming, outgoing) = state.get_all();
        let request: Option<ShutdownRequest> = self.base.decode_hascope_actor_message(incoming, key);
        let Some(_request) = request else {
            return Err(anyhow!("Failed to decode ShutdownRequest message"));
        };

        let my_state = self.current_npu_ha_state(internal);
        let response = if my_state == HaState::Active {
            "accepted"
        } else {
            "rejected"
        };
        if let Ok(msg) = ShutdownReply::new_actor_msg(&self.base.id, response) {
            if let Some(peer_sp) = self.peer_sp() {
                outgoing.send(peer_sp, msg);
            }
        }

        if response == "accepted" {
            Ok(HaEvent::PeerShutdownRequested)
        } else {
            Ok(HaEvent::None)
        }
    }

    /// Handle shutdown reply messages for this HA scope.
    /// Emits HaEvent::Shutdown on "accepted". On "rejected", retries with a delay if desired state is still Dead.
    fn handle_shutdown_reply(&mut self, state: &mut State, key: &str) -> Result<HaEvent> {
        let (_internal, incoming, outgoing) = state.get_all();
        let reply: Option<ShutdownReply> = self.base.decode_hascope_actor_message(incoming, key);
        let Some(reply) = reply else {
            return Err(anyhow!("Failed to decode ShutdownReply message"));
        };

        match reply.response.as_str() {
            "accepted" => Ok(HaEvent::Shutdown),
            "rejected" => {
                let desired_state = self
                    .base
                    .dash_ha_scope_config
                    .as_ref()
                    .and_then(|c| DesiredHaState::try_from(c.desired_ha_state).ok())
                    .unwrap_or(DesiredHaState::Unspecified);

                if desired_state == DesiredHaState::Dead {
                    let Some(peer_sp) = self.peer_sp() else {
                        info!("Haven't resolved peer vDPU service path yet, cannot retry shutdown");
                        return Ok(HaEvent::None);
                    };

                    let msg = ShutdownRequest::new_actor_msg(&self.base.id, "planned shutdown")?;
                    outgoing.send_with_delay(peer_sp, msg, Duration::from_secs(RETRY_INTERVAL.into()));
                    info!("Shutdown rejected, retrying with delay");
                }

                Ok(HaEvent::None)
            }
            other => {
                error!("Unexpected shutdown reply response: {}", other);
                Ok(HaEvent::None)
            }
        }
    }

    /// Handle vote request messages for this HA scope
    /// Following procedure documented in <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-hld.md#73-primary-election>
    fn handle_vote_request(&mut self, state: &mut State, key: &str) {
        let response: &str;
        let source_actor_id = key.strip_prefix(VoteRequest::msg_key_prefix()).unwrap_or(key);
        let (internal, incoming, outgoing) = state.get_all();
        let request: Option<VoteRequest> = self.base.decode_hascope_actor_message(incoming, key);
        let Some(request) = request else {
            error!("Failed to decode VoteRequest message");
            return;
        };
        let my_state = self.current_npu_ha_state(internal);
        let my_desired_state = self
            .base
            .dash_ha_scope_config
            .as_ref()
            .map(|c| DesiredHaState::try_from(c.desired_ha_state).unwrap_or(DesiredHaState::Unspecified))
            .unwrap_or(DesiredHaState::Unspecified);
        let my_term = self
            .base
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
        if let Ok(msg) = VoteReply::new_actor_msg(&self.base.id, source_actor_id, response) {
            if let Some(peer_sp) = self.peer_sp() {
                outgoing.send(peer_sp, msg);
            }
        }
    }

    /// Handle vote reply messages for this HA scope
    /// Following procedure documented in <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-hld.md#73-primary-election>
    /// Map to HaEvent::VoteCompleted if the response is one of [BecomeActive | BecomeStandby | BecomeStandalone ] and set the target state
    fn handle_vote_reply(&mut self, state: &mut State, key: &str) -> Result<HaEvent> {
        let (_internal, incoming, _outgoing) = state.get_all();
        let reply: Option<VoteReply> = self.base.decode_hascope_actor_message(incoming, key);
        let Some(reply) = reply else {
            return Err(anyhow!("Failed to decode VoteReply message"));
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
                self.send_vote_request_to_peer(state, true)?;
                return Ok(HaEvent::None);
            }
            _ => {
                return Ok(HaEvent::None);
            }
        }
        Ok(HaEvent::VoteCompleted)
    }

    /// Handle switchover request messages from peer HA scope actor.
    /// When the standby side transitions to SwitchingToActive, it sends a SwitchoverRequest to the active side.
    /// The active side validates and transitions to SwitchingToStandby if appropriate.
    /// Following procedure documented in <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-hld.md#82-planned-switchover>
    fn handle_switchover_request(&mut self, state: &mut State, key: &str) -> Result<HaEvent> {
        let request: Option<SwitchoverRequest> = self.base.decode_hascope_actor_message(state.incoming(), key);
        let Some(request) = request else {
            return Err(anyhow!("Failed to decode SwitchoverRequest message"));
        };

        if request.flag == MessageMetaFlags::Fin {
            info!(
                "The switchover request has been accepted by the peer: switchover_id={}",
                request.switchover_id
            );
            self.retry_count = 0;
            return Ok(HaEvent::None);
        } else if request.flag == MessageMetaFlags::Rst {
            info!(
                "The switchover request has been rejected by the peer: switchover_id={}",
                request.switchover_id
            );

            if self.retry_count < 3 {
                self.send_switchover_request_to_peer(state, &request.switchover_id, MessageMetaFlags::Syn, true)?;
                self.retry_count += 1;
                return Ok(HaEvent::None);
            } else {
                self.retry_count = 0;
                info!(
                    "The switchover failed after 3 requests rejected by the peer: switchover_id={}",
                    request.switchover_id
                );
                return Ok(HaEvent::SwitchoverFailed);
            }
        }

        // Per HLD: If DPU0 is not in Active state or also has desired state set to Active,
        // it will reject the SwitchOver request.
        let my_state = self.current_npu_ha_state(state.internal());
        if my_state != HaState::Active {
            info!(
                "Rejecting SwitchoverRequest from peer: local state is {} (not Active)",
                my_state.as_str_name()
            );
            self.send_switchover_request_to_peer(state, &request.switchover_id, MessageMetaFlags::Rst, false)?;
            return Ok(HaEvent::None);
        }

        let my_desired_state = self
            .base
            .dash_ha_scope_config
            .as_ref()
            .map(|c| DesiredHaState::try_from(c.desired_ha_state).unwrap_or(DesiredHaState::Unspecified))
            .unwrap_or(DesiredHaState::Unspecified);
        if my_desired_state == DesiredHaState::Active {
            info!("Rejecting SwitchoverRequest from peer: local desired state is also Active");
            self.send_switchover_request_to_peer(state, &request.switchover_id, MessageMetaFlags::Rst, false)?;
            return Ok(HaEvent::None);
        }

        // Accept the switchover request — update switchover tracking state
        info!(
            "Accepting SwitchoverRequest from peer with switchover_id={}",
            request.switchover_id
        );
        self.send_switchover_request_to_peer(state, &request.switchover_id, MessageMetaFlags::Fin, false)?;
        self.set_npu_switchover_state(state, &request.switchover_id, "in_progress")?;

        Ok(HaEvent::SwitchoverRequested)
    }

    /// Handle async self notification messages
    async fn handle_self_notification(
        &mut self,
        state: &mut State,
        key: &str,
        context: &mut Context,
    ) -> Result<HaEvent> {
        let (_internal, incoming, _outgoing) = state.get_all();
        let notification: Option<SelfNotification> = self.base.decode_hascope_actor_message(incoming, key);
        let Some(notification) = notification else {
            return Err(anyhow!("Failed to decode SelfNotification message"));
        };

        if notification.event == "CheckPeerConnection" {
            return self.check_peer_connection_and_retry(state, context).await;
        }

        if let Some(event) = HaEvent::from_str(&notification.event) {
            Ok(event)
        } else {
            Err(anyhow!("Unknown event"))
        }
    }

    /// Handle a DPURequestEnterStandalone message from the peer HA scope actor.
    fn handle_dpu_request_enter_standalone(&mut self, state: &mut State, key: &str) -> Result<HaEvent> {
        let (_internal, incoming, _outgoing) = state.get_all();
        let request: Option<DPURequestEnterStandalone> = self.base.decode_hascope_actor_message(incoming, key);
        let Some(request) = request else {
            return Err(anyhow!("Failed to decode DPURequestEnterStandalone message"));
        };

        let local_dpu_up = self.base.get_vdpu(incoming).map(|v| v.up).unwrap_or(false);
        let local_pinned_bfd_up = self.base.pinned_bfd_state.as_deref() == Some("up");

        if !request.local_dpu_up {
            info!("Asking peer HA scope to become standby because of remote DPU down!");
            self.send_dpu_request_enter_standalone_reply(state, "EnterStandby");
            return Ok(HaEvent::EnterStandalone);
        }

        if local_dpu_up && local_pinned_bfd_up {
            // Local DPU is healthy and pinned BFD is up
            info!("Denying DPURequestEnterStandalone because both DPU are healthy!");
            self.send_dpu_request_enter_standalone_reply(state, "Deny");
            return Ok(HaEvent::None);
        }

        if !local_dpu_up {
            // Peer DPU is healthy while local DPU is not — allow the request
            info!("Asking peer HA scope to become standalone because of local DPU down!");
            self.send_dpu_request_enter_standalone_reply(state, "EnterStandalone");
            return Ok(HaEvent::EnterStandby);
        }

        if !local_pinned_bfd_up && request.pinned_vdpu_bfd_probe_state == "up" {
            // Remote DPU has BFD pinned up while local DPU has BFD pinned down
            // Yield to the remote DPU
            info!("Asking peer HA scope to become standalone because of local DPU pinned BFD down!");
            self.send_dpu_request_enter_standalone_reply(state, "EnterStandalone");
            return Ok(HaEvent::EnterStandby);
        }

        if request.inline_sync_packet_drops {
            // Dataplane gray failures detected, pick the DPU with higher desired state to become Standalone
            let desired_ha_state = self.base.dash_ha_scope_config.as_ref().unwrap().desired_ha_state;
            if desired_ha_state == DesiredHaState::Standalone as i32
                || desired_ha_state == DesiredHaState::Active as i32
            {
                info!("Asking peer HA scope to become standby because local DPU is preferred!");
                self.send_dpu_request_enter_standalone_reply(state, "EnterStandby");
                return Ok(HaEvent::EnterStandalone);
            } else {
                info!("Asking peer HA scope to become standalone because remote DPU is preferred!");
                self.send_dpu_request_enter_standalone_reply(state, "EnterStandalone");
                return Ok(HaEvent::EnterStandby);
            }
        }

        // Neither side meets the conditions — deny
        info!("Denying DPURequestEnterStandalone because peer DPU is not healthy either");
        self.send_dpu_request_enter_standalone_reply(state, "Deny");
        Ok(HaEvent::None)
    }

    /// Handle a DPURequestEnterStandaloneReply from the peer HA scope actor.
    /// On "Deny", emit LeavingStandalone. Otherwise, enter corresponding state.
    fn handle_dpu_request_enter_standalone_reply(&mut self, state: &mut State, key: &str) -> Result<HaEvent> {
        let (_internal, incoming, _outgoing) = state.get_all();
        let reply: Option<DPURequestEnterStandaloneReply> = self.base.decode_hascope_actor_message(incoming, key);
        let Some(reply) = reply else {
            return Err(anyhow!("Failed to decode DPURequestEnterStandaloneReply message"));
        };

        match reply.response.as_str() {
            "EnterStandalone" => {
                info!("Peer allowed DPURequestEnterStandalone, entering standalone");
                Ok(HaEvent::EnterStandalone)
            }
            "EnterStandby" => {
                info!("Peer allowed DPURequestEnterStandalone, entering standby");
                Ok(HaEvent::EnterStandby)
            }
            "Deny" => {
                info!("Peer denied DPURequestEnterStandalone, leaving standalone");
                Ok(HaEvent::LeavingStandalone)
            }
            other => {
                error!("Unexpected DPURequestEnterStandaloneReply response: {}", other);
                Ok(HaEvent::None)
            }
        }
    }

    /// Handle COUNTERS_ENI_NAME_MAP updates.
    /// Collects all counter object IDs (the map values) so we can match them against
    /// incoming COUNTERS table entries.
    fn handle_counters_eni_name_map_update(&mut self, state: &mut State, key: &str) -> Result<HaEvent> {
        let (_internal, incoming, _outgoing) = state.get_all();
        let msg = incoming
            .get(key)
            .ok_or_else(|| anyhow!("Failed to get CountersEniNameMapTable message"))?;
        let kfv = msg.deserialize_data::<KeyOpFieldValues>()?;

        let eni_name_map: CountersEniNameMapTable = swss_serde::from_field_values(&kfv.field_values)?;

        // Collect all counter object IDs (map values)
        self.counter_object_ids.clear();
        for oid in eni_name_map.eni_to_counters_map.values() {
            self.counter_object_ids.insert(oid.clone());
        }

        info!(
            "Updated counter object IDs from COUNTERS_ENI_NAME_MAP: {} entries",
            self.counter_object_ids.len()
        );

        Ok(HaEvent::None)
    }

    /// Handle COUNTERS table updates.
    /// If the entry key matches a tracked counter object ID from COUNTERS_ENI_NAME_MAP,
    /// store the counter statistics.
    fn handle_counters_table_update(&mut self, state: &mut State, key: &str) -> Result<HaEvent> {
        let (_internal, incoming, _outgoing) = state.get_all();
        let msg = incoming
            .get(key)
            .ok_or_else(|| anyhow!("Failed to get CountersTable message"))?;
        let kfv = msg.deserialize_data::<KeyOpFieldValues>()?;

        // Check if this counter entry's key matches a tracked object ID
        if self.counter_object_ids.contains(&kfv.key) {
            match kfv.operation {
                KeyOperation::Set => {
                    let counters: CountersTable = swss_serde::from_field_values(&kfv.field_values)?;
                    info!(
                        "Storing counter stats for object ID {}: {} fields",
                        kfv.key,
                        counters.counters_stats.len()
                    );
                    let new_rx = counters
                        .counters_stats
                        .get(ENI_INLINE_FLOW_SYNC_RX_PKTS)
                        .and_then(|v| v.parse::<u64>().ok());
                    let new_tx = counters
                        .counters_stats
                        .get(ENI_INLINE_FLOW_SYNC_TX_PKTS)
                        .and_then(|v| v.parse::<u64>().ok());
                    if let Some(old_stats) = self.counter_stats.insert(kfv.key.clone(), counters.counters_stats) {
                        let old_rx = old_stats
                            .get(ENI_INLINE_FLOW_SYNC_RX_PKTS)
                            .and_then(|v| v.parse::<u64>().ok());
                        let old_tx = old_stats
                            .get(ENI_INLINE_FLOW_SYNC_TX_PKTS)
                            .and_then(|v| v.parse::<u64>().ok());
                        if let (Some(new_rx), Some(new_tx), Some(old_rx), Some(old_tx)) =
                            (new_rx, new_tx, old_rx, old_tx)
                        {
                            let rx_diff = new_rx - old_rx;
                            let tx_diff = new_tx - old_tx;
                            if tx_diff - rx_diff > INLINE_SYNC_PKT_DROP_ALERT_THRESHOLD as u64 {
                                return Ok(HaEvent::HighInlineSyncDrops);
                            }
                        }
                    }
                }
                KeyOperation::Del => {
                    info!("Removing counter stats for object ID {}", kfv.key);
                    self.counter_stats.remove(&kfv.key);
                }
            }
        }

        Ok(HaEvent::None)
    }
}

// State machine
impl NpuHaScopeActor {
    pub(super) fn current_npu_ha_state(&self, internal: &Internal) -> HaState {
        self.base
            .get_npu_ha_scope_state(internal)
            .and_then(|scope| scope.local_ha_state)
            .and_then(|s| HaState::from_str_name(&s))
            .unwrap_or(HaState::Dead)
    }

    fn current_npu_peer_ha_state(&self, internal: &Internal) -> HaState {
        self.base
            .get_npu_ha_scope_state(internal)
            .and_then(|scope| scope.peer_ha_state)
            .and_then(|s| HaState::from_str_name(&s))
            .unwrap_or(HaState::Dead)
    }

    fn apply_pending_state_side_effects(
        &mut self,
        state: &mut State,
        current_state: &HaState,
        pending_state: &HaState,
        event: &HaEvent,
    ) -> Result<()> {
        let _internal = state.internal();
        match pending_state {
            HaState::Connecting => {
                // Send a heartbeat message to the peer ha scope actor as a request to connect
                self.send_heartbeat_to_peer(state)?;
                // Send a signal to itself to schedule a check later
                self.send_self_notification(state, "CheckPeerConnection", RETRY_INTERVAL)?;
            }
            HaState::Connected => {
                // Send VoteRequest to the peer to start primary election
                self.send_vote_request_to_peer(state, false)?;
            }
            HaState::InitializingToActive => {
                // If the peer is already in InitializingToStandby
                if self.current_npu_peer_ha_state(state.internal()) == HaState::InitializingToStandby {
                    // Note: it does not really move the HA scope into Active immediately since we need SDN approvals
                    self.send_self_notification(state, "EnterActive", 0)?;
                }
            }
            HaState::PendingActiveActivation | HaState::PendingStandbyActivation => {
                let operations: Vec<(String, String)> = vec![(Uuid::new_v4().to_string(), "activate_role".to_string())];
                self.base
                    .update_npu_ha_scope_state_pending_operations(state, operations, Vec::new())?;
            }
            HaState::Standalone => {
                // Activate Standalone role on DPU with a new term
                let _ = self.increment_npu_ha_scope_state_target_term(state);
                let _ = self.update_dpu_ha_scope_table_with_params(state, HaRole::Standalone.as_str_name());
            }
            HaState::SwitchingToStandalone => {
                // Perform checks and corresponding steps to enter the standalone setup
                if self.base.dash_ha_scope_config.as_ref().unwrap().desired_ha_state
                    == DesiredHaState::Standalone as i32
                {
                    // Pinning to Standalone
                    self.send_self_notification(state, "EnterStandalone", 0)?;
                } else if *event == HaEvent::PeerLost {
                    // Peer DPU lost
                    self.send_self_notification(state, "EnterStandalone", 0)?;
                } else if *event == HaEvent::PeerShutdownRequested {
                    // Peer DPU planned shutdown
                    self.send_self_notification(state, "EnterStandalone", 0)?;
                } else {
                    // Send DPURequestEnterStandalone to peer with local health signals
                    let inline_sync_drops = *event == HaEvent::HighInlineSyncDrops;
                    if let Err(e) = self.send_dpu_request_enter_standalone(state, inline_sync_drops) {
                        // Cannot connect with peer, enter standalone right away
                        error!("Failed to send DPURequestEnterStandalone to peer: {e}");
                        self.send_self_notification(state, "EnterStandalone", 0)?;
                    }
                }
            }
            HaState::Active => {
                if *current_state == HaState::Standalone {
                    // If staring from Standalone, do bulk sync
                    let _ = self.add_bulk_sync_session(state);
                } else if *current_state == HaState::PendingActiveActivation {
                    // When starting from PendingActiveRoleActivation, no need to do bulk sync.
                    // Send BulkSyncCompleted signal to the peer immediately
                    self.send_bulk_sync_completed_to_peer(state)?;
                } else if *current_state == HaState::SwitchingToActive {
                    // Per HLD Section 8.2.1 Step 5: Switchover to active is complete.
                    // Mark switchover as completed on the new active side.
                    self.complete_switchover(state, "completed")?;
                }

                // Activate Active role on DPU with a new term
                let _ = self.increment_npu_ha_scope_state_target_term(state);
                let _ = self.update_dpu_ha_scope_table_with_params(state, HaRole::Active.as_str_name());
            }
            HaState::InitializingToStandby => {
                // Activate Standby role on DPU
                let _ = self.update_dpu_ha_scope_table_with_params(state, HaRole::Standby.as_str_name());
            }
            HaState::Standby => {
                if *current_state == HaState::SwitchingToStandby {
                    // Per HLD Section 8.2.1 Step 6: Switchover to standby is complete.
                    // Mark switchover as completed on the new standby side.
                    self.complete_switchover(state, "completed")?;
                } else if *current_state == HaState::SwitchingToActive {
                    self.complete_switchover(state, "failed")?;
                }
                // The Standby role on DPU may have been activated, this operation is to update the term and also ensure the DPU is in standby role if not done previously
                let _ = self.update_dpu_ha_scope_table_with_params(state, HaRole::Standby.as_str_name());
            }
            HaState::SwitchingToActive => {
                // Activate switching_to_active role on DPU
                let _ = self.update_dpu_ha_scope_table_with_params(state, HaRole::SwitchingToActive.as_str_name());

                // Per HLD Section 8.2.1 Step 3: Send SwitchoverRequest to the active peer.
                let switchover_id = self
                    .base
                    .get_npu_ha_scope_state(state.internal())
                    .and_then(|s| s.switchover_id)
                    .unwrap_or_default();
                self.send_switchover_request_to_peer(state, &switchover_id, MessageMetaFlags::Syn, false)?;
                self.set_npu_switchover_state(state, &switchover_id, "in_progress")?;
            }
            HaState::SwitchingToStandby => {
                // Per HLD Section 8.2.1 Step 4: The active node received a SwitchoverRequest
                // and is now transitioning to standby. Activate standby role on DPU.
                let _ = self.update_dpu_ha_scope_table_with_params(state, HaRole::Standby.as_str_name());
            }
            HaState::Destroying => {
                // Activate Dead role on the DPU
                let _ = self.update_dpu_ha_scope_table_with_params(state, HaRole::Dead.as_str_name());
            }
            _ => {}
        }
        Ok(())
    }

    fn drive_npu_state_machine(&mut self, state: &mut State, event: &HaEvent) -> Result<()> {
        info!("Drive NPU HA state machine based on {}", event.as_str());
        let Some(config) = self.base.dash_ha_scope_config.as_ref() else {
            info!("The HA Scope Config hasn't been initialized yet!");
            return Ok(());
        };

        let Some(vdpu) = self.base.get_vdpu(state.incoming()) else {
            info!("vDPU {} has not been initialized yet", &self.base.vdpu_id);
            return Ok(());
        };
        if !vdpu.dpu.is_managed {
            info!("vDPU {} is unmanaged. Put actor in dormant state", &self.base.vdpu_id);
            return Ok(());
        }

        let ha_set_id = self.base.get_haset_id().unwrap_or_default();
        let Some(_haset) = self.base.get_haset(state.incoming()) else {
            info!("HA-SET {} has not been initialized yet", &ha_set_id);
            return Ok(());
        };

        let current_state = self.current_npu_ha_state(state.internal());
        let mut event_to_use = *event;

        // always check the admin state before doing anything
        if config.disabled {
            if current_state != HaState::Dead {
                info!("Ha Scope {} is disabled!", self.base.id);
                self.set_npu_local_ha_state(state, HaState::Dead, "admin disabled")?;
                // Update DPU APPL_DB to activate Dead role on the DPU
                let _ = self.update_dpu_ha_scope_table_with_params(state, HaRole::Dead.as_str_name());

                // Send HaScopeActorState update to peer and ha-set
                self.broadcast_ha_scope_state(state, HaState::Dead);
            }
            return Ok(());
        } else if event_to_use == HaEvent::AdminStateChanged {
            // Equivalent to launch
            event_to_use = HaEvent::Launch;
        }

        let target_state = self.target_ha_scope_state.unwrap_or(TargetState::Unspecified);

        match self.next_state(state, &target_state, &current_state, &event_to_use) {
            Some((next_state, reason)) if next_state != current_state => {
                info!("Pending next state: {}, reason: {}", next_state.as_str_name(), reason);
                let pending_state = next_state;
                self.apply_pending_state_side_effects(state, &current_state, &pending_state, &event_to_use)?;
                self.set_npu_local_ha_state(state, pending_state, reason)?;

                // send out HaScopeActorState message to the peer and ha-set actor
                self.broadcast_ha_scope_state(state, pending_state);
            }
            _ => {
                // No state transition. Handle special cases that require side effects
                // without an immediate state change.

                // Per HLD Section 8.2.1 Steps 1-2: When in Standby and desired state changes
                // to Active, create a pending "switchover" operation and wait for upstream approval.
                if current_state == HaState::Standby
                    && event_to_use == HaEvent::DesiredStateChanged
                    && target_state == TargetState::Active
                {
                    let switchover_id = Uuid::new_v4().to_string();
                    info!("Creating pending switchover operation: {}", &switchover_id);

                    // Record switchover tracking fields in NPU HA scope state
                    self.set_npu_switchover_state(state, &switchover_id, "pending_approval")?;

                    // Create pending operation so upstream can approve
                    let operations: Vec<(String, String)> = vec![(switchover_id, "switchover".to_string())];
                    self.base
                        .update_npu_ha_scope_state_pending_operations(state, operations, Vec::new())?;
                } else if event_to_use == HaEvent::DesiredStateChanged && target_state == TargetState::Dead {
                    let outgoing = state.outgoing();
                    let Some(peer_sp) = self.peer_sp() else {
                        // Haven't received the remote peer vDPU info yet
                        info!("Haven't received peer vDPU info yet");
                        return Ok(());
                    };

                    let msg = ShutdownRequest::new_actor_msg(&self.base.id, "planned shutdown")?;
                    outgoing.send(peer_sp, msg);
                }
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
        if *event == HaEvent::Shutdown {
            return match current_state {
                HaState::Dead => None,
                HaState::Destroying => {
                    if self.base.dpu_ha_scope_state.as_ref().map(|s| s.ha_role.as_str())
                        == Some(HaRole::Dead.as_str_name())
                    {
                        // When the DPU is in dead role, all traffic is drained
                        Some((HaState::Dead, "destroy completed"))
                    } else {
                        None
                    }
                }
                _ => Some((HaState::Destroying, "planned shutdown")),
            };
        }

        match current_state {
            HaState::Unspecified | HaState::Dead => {
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
                if *event == HaEvent::PeerConnected || *event == HaEvent::PeerStateChanged {
                    Some((HaState::Connected, "connection with peer established"))
                } else if *event == HaEvent::PeerLost {
                    Some((HaState::SwitchingToStandalone, "remote peer failure while connecting"))
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
                        TargetState::Standalone => Some((HaState::SwitchingToStandalone, "target standalone role")),
                        _ => None, // Target Dead case should be handled at the beginning of the function
                    }
                } else {
                    None
                }
            }
            HaState::InitializingToActive => {
                // On Peer moving to InitializingToStandby, go to PendingActiveRoleActivation
                // Go to Standalone if detecting problem on the peer and the local DPU is healthy
                // Go to Standby if detecting problem locally
                if self.current_npu_peer_ha_state(state.internal()) == HaState::InitializingToStandby {
                    Some((HaState::PendingActiveActivation, "peer is ready"))
                } else if *event == HaEvent::PeerLost {
                    Some((
                        HaState::SwitchingToStandalone,
                        "remote peer failure during initialization",
                    ))
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
                // Per HLD Section 8.2.1: The active side transitions to SwitchingToStandby
                // only when it accepts a SwitchoverRequest from the peer (standby->SwitchingToActive).
                if *event == HaEvent::SwitchoverRequested {
                    Some((HaState::SwitchingToStandby, "peer requested switchover"))
                } else if *event == HaEvent::PeerLost {
                    Some((HaState::SwitchingToStandalone, "peer failure while active"))
                } else if *event == HaEvent::LocalFailure {
                    Some((HaState::SwitchingToStandalone, "local failure while active"))
                } else if *event == HaEvent::PeerShutdownRequested {
                    Some((HaState::SwitchingToStandalone, "peer requested shutdown"))
                } else if *event == HaEvent::HighInlineSyncDrops {
                    Some((HaState::SwitchingToStandalone, "high inline-sync packet drops"))
                } else {
                    None
                }
            }
            HaState::SwitchingToStandby => {
                if *event == HaEvent::PeerLost {
                    Some((HaState::SwitchingToStandalone, "peer lost during switchover to standby"))
                } else if *event == HaEvent::PeerStateChanged
                    && self.current_npu_peer_ha_state(state.internal()) == HaState::Active
                {
                    Some((HaState::Standby, "peer has been active"))
                } else {
                    None
                }
            }
            HaState::Standby => {
                // Per HLD Section 8.2.1: Planned switchover is initiated from the standby side.
                // Step 1-2: When desired state changes to Active, create a pending "switchover"
                // operation and wait for upstream approval.
                // Step 3: When SwitchoverApproved, transition to SwitchingToActive.
                if *event == HaEvent::SwitchoverApproved {
                    Some((
                        HaState::SwitchingToActive,
                        "switchover approved, transitioning to active",
                    ))
                } else if *event == HaEvent::PeerLost {
                    Some((HaState::SwitchingToStandalone, "peer failure while standby"))
                } else {
                    None
                }
            }
            HaState::SwitchingToActive => {
                if *event == HaEvent::PeerLost {
                    Some((HaState::SwitchingToStandalone, "peer lost during switchover to active"))
                } else if *event == HaEvent::PeerStateChanged
                    && self.current_npu_peer_ha_state(state.internal()) == HaState::SwitchingToStandby
                {
                    Some((HaState::Active, "switchover to active complete"))
                } else if *event == HaEvent::SwitchoverFailed {
                    Some((HaState::Standby, "switchover failed"))
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
                // Transition out of Standalone on peer state updates.
                // When the peer starts initializing to standby, this node becomes Active.
                if *event == HaEvent::PeerStateChanged
                    && self.current_npu_peer_ha_state(state.internal()) == HaState::InitializingToStandby
                {
                    Some((HaState::Active, "peer initializing to standby"))
                } else {
                    None
                }
            }
            HaState::SwitchingToStandalone => {
                if *event == HaEvent::EnterStandalone {
                    Some((HaState::Standalone, "won the standalone selection"))
                } else if *event == HaEvent::EnterStandby {
                    Some((HaState::Standby, "peer became standalone, entering standby"))
                } else if *event == HaEvent::LeavingStandalone {
                    match target_state {
                        TargetState::Active => Some((HaState::Active, "failed to enter standalone")),
                        TargetState::Standby => Some((HaState::Standby, "failed to enter standalone")),
                        _ => None, // Target Dead and Standalone case should not be handled at this place
                    }
                } else {
                    None
                }
            }
            HaState::Destroying => {
                if self.base.dpu_ha_scope_state.as_ref().map(|s| s.ha_role.as_str()) == Some(HaRole::Dead.as_str_name())
                {
                    // HaEvent::DpuStateChanged should trigger this branch
                    Some((HaState::Dead, "resources drained"))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

// Peer/sync methods
impl NpuHaScopeActor {
    /// Build an HaScopeActorState message for the given HA state and send it
    /// to the peer HA scope actor and all known HA set actors.
    fn broadcast_ha_scope_state(&self, state: &mut State, ha_state: HaState) {
        let (internal, _incoming, outgoing) = state.get_all();
        let npu_state = self.base.get_npu_ha_scope_state(internal);
        let local_target_term = npu_state.as_ref().and_then(|s| s.local_target_term.as_deref());
        let owner = self
            .base
            .dash_ha_scope_config
            .as_ref()
            .map(|c| c.owner)
            .unwrap_or(HaOwner::Unspecified as i32);
        let new_state = ha_state.as_str_name();
        let timestamp = now_in_millis();
        let term = local_target_term.unwrap_or("0");

        if let Ok(msg) = HaScopeActorState::new_actor_msg(
            &self.base.id,
            owner,
            new_state,
            timestamp,
            term,
            &self.base.vdpu_id,
            self.base.peer_vdpu_id.as_deref().unwrap_or(""),
        ) {
            if let Some(peer_sp) = self.peer_sp() {
                outgoing.send(peer_sp, msg.clone());
            }
            for ha_set_sp in &self.base.ha_set_sp {
                outgoing.send(ha_set_sp.clone(), msg.clone());
            }
        }
    }

    /// Send a DPURequestEnterStandaloneReply message to the peer HA scope actor.
    fn send_dpu_request_enter_standalone_reply(&self, state: &mut State, response: &str) {
        let outgoing = state.outgoing();
        if let Ok(msg) = DPURequestEnterStandaloneReply::new_actor_msg(&self.base.id, response) {
            if let Some(peer_sp) = self.peer_sp() {
                outgoing.send(peer_sp, msg);
            }
        }
    }

    /// Send a DPURequestEnterStandalone message to the peer HA scope actor
    /// carrying the local DPU's critical health signals.
    fn send_dpu_request_enter_standalone(&self, state: &mut State, inline_sync_drops_detected: bool) -> Result<()> {
        let (_internal, incoming, outgoing) = state.get_all();

        let Some(peer_sp) = self.peer_sp() else {
            let err = anyhow!("Cannot send DPURequestEnterStandalone to peer: peer service path not resolved");
            info!("{err}");
            return Err(err);
        };

        let vdpu_up = self.base.get_vdpu(incoming).map(|v| v.up).unwrap_or(false);
        let dp_channel_is_alive = self.base.get_haset(incoming).map(|h| h.up).unwrap_or(false);
        let pinned_bfd_state = self.base.pinned_bfd_state.clone().unwrap_or_default();

        let msg = DPURequestEnterStandalone::new_actor_msg(
            &self.base.id,
            dp_channel_is_alive,
            vdpu_up,
            pinned_bfd_state,
            inline_sync_drops_detected,
        )?;
        outgoing.send(peer_sp, msg);
        info!(
            "Sent DPURequestEnterStandalone to peer: dp_channel_is_alive={}, local_dpu_up={}, pinned_bfd_state={}, inline_sync_packet_drops={}",
            dp_channel_is_alive,
            vdpu_up,
            self.base.pinned_bfd_state.as_deref().unwrap_or(""),
            inline_sync_drops_detected
        );

        Ok(())
    }

    /// Check if the peer HA scope is connected
    /// If not, send a self notification to schedule an execution of the same function for later
    /// Upon exceeding retry count threshold, signal a PeerLost event
    async fn check_peer_connection_and_retry(&mut self, state: &mut State, context: &mut Context) -> Result<HaEvent> {
        if !self.peer_connected {
            if self.retry_count < MAX_RETRIES {
                // retry connecting with peer
                self.retry_count += 1;

                // try to resolve the remote peer's ServicePath via REMOTE_DPU + swbusd
                match self.base.resolve_peer_sp(context.get_edge_runtime()).await {
                    Ok(sp) => {
                        info!("Resolved peer HA scope SP: {}", sp.to_longest_path());
                        self.base.peer_sp = Some(sp);
                    }
                    Err(e) => {
                        error!("Failed to resolve peer SP: {e}. Will retry on next heartbeat.");
                    }
                }

                // send another heartbeat message
                self.send_heartbeat_to_peer(state)?;

                // Send a signal to itself to schedule a check later
                self.send_self_notification(state, "CheckPeerConnection", RETRY_INTERVAL)?;
            } else {
                // reset retry count
                // report peer lost
                self.retry_count = 0;
                return Ok(HaEvent::PeerLost);
            }
        } else {
            // no-op but resetting retry count, if the peer is connected
            self.retry_count = 0;
        }
        Ok(HaEvent::None)
    }

    /// Send heartbeat to the peer HaScopeActor
    /// Serve as peer connect request during launch process
    fn send_heartbeat_to_peer(&self, state: &mut State) -> Result<()> {
        let outgoing = state.outgoing();
        let Some(peer_sp) = self.peer_sp() else {
            // Haven't received the remote peer vDPU info yet
            info!("Haven't resolved peer service path yet");
            return Ok(());
        };

        let peer_actor_id = self.base.get_peer_actor_id().unwrap();
        let msg = PeerHeartbeat::new_actor_msg(&self.base.id, &peer_actor_id)?;
        outgoing.send(peer_sp, msg);
        Ok(())
    }

    /// Send a VoteRequest message to the peer HA scope actor for primary election
    /// The term and state come from NPU HA scope state table
    /// The desired state comes from dash_ha_scope_config
    fn send_vote_request_to_peer(&self, state: &mut State, delay: bool) -> Result<()> {
        let (internal, _incoming, outgoing) = state.get_all();

        let Some(peer_sp) = self.peer_sp() else {
            info!("Cannot send VoteRequest to peer: peer actor ID not available");
            return Ok(());
        };
        let peer_actor_id = self.base.get_peer_actor_id().unwrap();

        let Some(npu_ha_scope_state) = self.base.get_npu_ha_scope_state(internal) else {
            info!("Cannot send VoteRequest: NPU HA scope state not available");
            return Ok(());
        };

        let Some(ref config) = self.base.dash_ha_scope_config else {
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

        let msg = VoteRequest::new_actor_msg(&self.base.id, &peer_actor_id, &term, current_state, desired_state)?;

        if delay {
            outgoing.send_with_delay(peer_sp, msg, Duration::from_secs(RETRY_INTERVAL.into()));
            info!(
                "Sent VoteRequest to peer {} with delay: term={}, state={}, desired_state={}",
                peer_actor_id, term, current_state, desired_state
            );
        } else {
            outgoing.send(peer_sp, msg);
            info!(
                "Sent VoteRequest to peer {}: term={}, state={}, desired_state={}",
                peer_actor_id, term, current_state, desired_state
            );
        }

        Ok(())
    }

    /// Send a BulkSyncUpdate message with finished=true to the peer HA scope actor
    /// This signals to the peer that bulk sync is complete (e.g., when no actual sync is needed)
    fn send_bulk_sync_completed_to_peer(&self, state: &mut State) -> Result<()> {
        let outgoing = state.outgoing();

        let Some(peer_sp) = self.peer_sp() else {
            info!("Cannot send BulkSyncCompleted to peer: peer actor ID not available");
            return Ok(());
        };
        let peer_actor_id = self.base.get_peer_actor_id().unwrap();

        let msg = BulkSyncUpdate::new_actor_msg(
            &self.base.id,
            &peer_actor_id,
            true, // finished: true
        )?;

        outgoing.send(peer_sp, msg);
        info!("Sent BulkSyncCompleted to peer {}", peer_actor_id);

        Ok(())
    }

    /// Send a SwitchoverRequest to the active peer to initiate planned switchover.
    fn send_switchover_request_to_peer(
        &self,
        state: &mut State,
        switchover_id: &str,
        flag: MessageMetaFlags,
        delay: bool,
    ) -> Result<()> {
        let (_internal, _incoming, outgoing) = state.get_all();

        let Some(peer_sp) = self.peer_sp() else {
            info!("Cannot send SwitchoverRequest to peer: peer actor ID not available");
            return Ok(());
        };
        let peer_actor_id = self.base.get_peer_actor_id().unwrap();

        let msg = SwitchoverRequest::new_actor_msg(&self.base.id, &peer_actor_id, switchover_id, flag)?;

        if delay {
            outgoing.send_with_delay(peer_sp, msg, Duration::from_secs(RETRY_INTERVAL.into()));
            info!(
                "Retry sending SwitchoverRequest to peer {} with a delay: switchover_id={}, flags={}",
                peer_actor_id, switchover_id, flag as i32
            );
        } else {
            outgoing.send(peer_sp, msg);
            info!(
                "Sent SwitchoverRequest to peer {}: switchover_id={}, flags={}",
                peer_actor_id, switchover_id, flag as i32
            );
        }

        Ok(())
    }

    fn send_self_notification(&mut self, state: &mut State, message: &str, delay: u32) -> Result<()> {
        let outgoing = state.outgoing();
        if let Ok(msg) = SelfNotification::new_actor_msg(&self.base.id, message) {
            outgoing.send_with_delay(
                outgoing.from_my_sp(HaScopeActor::name(), &self.base.id),
                msg,
                Duration::from_secs(delay.into()),
            );
        }
        Ok(())
    }

    /// Add a new entry in DASH_FLOW_SYNC_SESSION_TABLE to start a bulk sync session
    fn add_bulk_sync_session(&mut self, state: &mut State) -> Result<Option<String>> {
        let (_internal, incoming, outgoing) = state.get_all();

        let ha_set_id = self.base.get_haset_id().unwrap();
        let Some(haset) = self.base.get_haset(incoming) else {
            debug!("HA-SET {} has not been received. Cannot do bulk sync!", &ha_set_id);
            return Ok(None);
        };

        let bulk_sync_session = DashFlowSyncSessionTable {
            session_type: "bulk_sync".to_string(),
            ha_set_id: ha_set_id.clone(),
            target_server_ip: haset.ha_set.peer_ip.clone(),
            target_server_port: haset
                .ha_set
                .cp_data_channel_port
                .expect("cp_data_channel_port must be configured"),
            timeout: BULK_SYNC_TIMEOUT,
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
        self.set_npu_flow_sync_session(
            state,
            &Some(session_id.clone()),
            &None,
            &Some(now_in_millis()),
            &Some(haset.ha_set.peer_ip),
        )?;

        Ok(Some(session_id))
    }
}

// NPU-specific state update methods
impl NpuHaScopeActor {
    /// Update DPU HA Scope Table based on configurations and parameters
    fn update_dpu_ha_scope_table_with_params(&self, state: &mut State, ha_role: &str) -> Result<()> {
        let Some(dash_ha_scope_config) = self.base.dash_ha_scope_config.as_ref() else {
            return Ok(());
        };

        let (internal, incoming, outgoing) = state.get_all();

        let ha_set_id = self.base.get_haset_id().unwrap();
        let Some(_haset) = self.base.get_haset(incoming) else {
            debug!(
                "HA-SET {} has not been received. Skip DASH_HA_SCOPE_TABLE update",
                &ha_set_id
            );
            return Ok(());
        };

        let dash_ha_scope = DashHaScopeTable {
            version: dash_ha_scope_config.version.parse().unwrap(),
            disabled: None,
            ha_set_id: dash_ha_scope_config.ha_set_id.clone(),
            vip_v4: None,
            vip_v6: None,
            ha_role: ha_role_to_string(ha_role),
            ha_term: self
                .base
                .get_npu_ha_scope_state(internal)
                .and_then(|s| s.local_target_term)
                .unwrap_or("0".to_string()),
            flow_reconcile_requested: None,
            activate_role_requested: None,
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

    fn increment_npu_ha_scope_state_target_term(&self, state: &mut State) -> Result<()> {
        let Some(ref _dash_ha_scope_config) = self.base.dash_ha_scope_config else {
            return Ok(());
        };
        let (internal, _incoming, _outgoing) = state.get_all();

        let Some(mut npu_ha_scope_state) = self.base.get_npu_ha_scope_state(internal) else {
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
        let Some(mut npu_state) = self.base.get_npu_ha_scope_state(internal) else {
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
        info!(scope=%self.base.id, state=%new_state.as_str_name(), "HA scope transitioned: {}", reason);
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
        let Some(mut npu_state) = self.base.get_npu_ha_scope_state(&*internal) else {
            return Ok(());
        };

        if !flow_sync_session_id.is_none() {
            npu_state.flow_sync_session_id = flow_sync_session_id.clone();
        }
        if !flow_sync_session_state.is_none() {
            npu_state.flow_sync_session_state = flow_sync_session_state.clone();
        }
        if !flow_sync_session_start_time_in_ms.is_none() {
            npu_state.flow_sync_session_start_time_in_ms = *flow_sync_session_start_time_in_ms;
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

    /// Update switchover tracking fields in NPU HA scope state.
    /// Per HLD Section 8.2: These fields track the planned switchover lifecycle:
    ///   - "pending_approval": Switchover requested, waiting for upstream approval
    ///   - "approved": Upstream approved the switchover
    ///   - "in_progress": Switchover is actively in progress
    ///   - "completed": Switchover finished successfully
    ///   - "failed": Switchover failed
    fn set_npu_switchover_state(
        &mut self,
        state: &mut State,
        switchover_id: &str,
        switchover_state: &str,
    ) -> Result<()> {
        let internal = state.internal();
        let Some(mut npu_state) = self.base.get_npu_ha_scope_state(&*internal) else {
            return Ok(());
        };

        npu_state.switchover_id = Some(switchover_id.to_string());
        npu_state.switchover_state = Some(switchover_state.to_string());

        let now = now_in_millis();
        match switchover_state {
            "pending_approval" => {
                npu_state.switchover_start_time_in_ms = Some(now);
                npu_state.switchover_end_time_in_ms = None;
                npu_state.switchover_approved_time_in_ms = None;
            }
            "approved" => {
                npu_state.switchover_approved_time_in_ms = Some(now);
            }
            "completed" | "failed" => {
                npu_state.switchover_end_time_in_ms = Some(now);
            }
            _ => {}
        }

        let fvs = swss_serde::to_field_values(&npu_state)?;
        internal.get_mut(NpuDashHaScopeState::table_name()).clone_from(&fvs);
        info!(
            scope=%self.base.id,
            "Switchover state updated: id={}, state={}",
            switchover_id, switchover_state
        );

        Ok(())
    }

    /// Mark the current switchover as completed or failed.
    /// Called when the node finishes transitioning (SwitchingToActive → Active
    /// or SwitchingToStandby → Standby).
    fn complete_switchover(&mut self, state: &mut State, outcome: &str) -> Result<()> {
        let npu_state = self.base.get_npu_ha_scope_state(state.internal());
        if let Some(ref switchover_id) = npu_state.and_then(|s| s.switchover_id.clone()) {
            if !switchover_id.is_empty() {
                self.set_npu_switchover_state(state, switchover_id, outcome)?;
            }
        }
        Ok(())
    }
}
