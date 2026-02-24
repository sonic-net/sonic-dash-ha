use crate::actors::{spawn_consumer_bridge_for_actor, DbBasedActor};
use crate::db_structs::*;
use crate::ha_actor_messages::*;
use crate::HaSetActor;
use anyhow::{anyhow, Result};
use sonic_common::SonicDbTable;
use sonic_dash_api_proto::decode_from_field_values;
use sonic_dash_api_proto::ha_scope_config::{DesiredHaState, HaScopeConfig};
use sonic_dash_api_proto::types::{HaOwner, HaRole, HaState};
use std::time::Duration;
use swbus_actor::{state::internal::Internal, ActorMessage, Context, State};
use swss_common::{KeyOpFieldValues, KeyOperation};
use tracing::{debug, error, info};
use uuid::Uuid;

use super::base::HaScopeBase;
use super::{HaEvent, HaScopeActor, TargetState, MAX_RETRIES, RETRY_INTERVAL};

pub struct NpuHaScopeActor {
    pub(super) base: HaScopeBase,
    /// Target state that HAmgrd should transition to upon HA events
    pub(super) target_ha_scope_state: Option<TargetState>,
    /// Retry count used for voting
    pub(super) retry_count: u32,
    /// Is peer connected?
    pub(super) peer_connected: bool,
}

impl NpuHaScopeActor {
    pub fn new(base: HaScopeBase) -> Self {
        Self {
            base,
            target_ha_scope_state: None,
            retry_count: 0,
            peer_connected: false,
        }
    }

    /// Main message dispatch for NPU-driven mode
    pub async fn handle_message_inner(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        // Npu driven HA scope message handling, map messages to HaEvents
        let mut event: Option<HaEvent> = None;
        if key == HaScopeActor::table_name() {
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
        } else if HAStateChanged::is_my_msg(key) {
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

        Ok(())
    }
}

// NPU message handlers
impl NpuHaScopeActor {
    /// Handles updates to the DASH_HA_SCOPE_CONFIG_TABLE in the case of NPU-driven HA.
    fn handle_dash_ha_scope_config_table_message_npu_driven(
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
                            "switchover" => Ok(HaEvent::SwitchoverApproved),
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
    /// Respond with HAStateChanged when the actor-self is ready
    async fn handle_peer_heartbeat(&mut self, state: &mut State, key: &str) -> Result<HaEvent> {
        let (internal, incoming, outgoing) = state.get_all();
        let entry = incoming
            .get_entry(key)
            .ok_or_else(|| anyhow!("Entry not found for key: {}", key))?;

        let Some(npu_ha_scope_state) = self.base.get_npu_ha_scope_state(internal) else {
            info!("Cannot respond to the peer until STATE_DB/DASH_HA_SCOPE_STATE is populated with basic information",);
            return Ok(HaEvent::None);
        };

        let msg = HAStateChanged::new_actor_msg(
            &self.base.id,
            "",
            npu_ha_scope_state.local_ha_state.as_deref().unwrap_or(""),
            npu_ha_scope_state.local_ha_state_last_updated_time_in_ms.unwrap_or(0),
            npu_ha_scope_state.local_target_term.as_deref().unwrap_or("0"),
        )?;
        outgoing.send(entry.source.clone(), msg);

        Ok(HaEvent::None)
    }

    /// Handles VDPU state update messages for this HA scope.
    /// If the vdpu is unmanaged, the actor is put in dormant state.
    /// Otherwise, map the update to a HaEvent
    async fn handle_vdpu_state_update_npu_driven_mode(
        &mut self,
        state: &mut State,
        context: &mut Context,
    ) -> Result<HaEvent> {
        let (internal, incoming, _outgoing) = state.get_all();
        let Some(vdpu) = self.base.get_vdpu(incoming) else {
            error!("Failed to retrieve vDPU {} from incoming state", &self.base.vdpu_id);
            return Err(anyhow!("Failed to retrieve vDPU from incoming state"));
        };

        if !vdpu.dpu.is_managed {
            debug!("vDPU {} is unmanaged. Put actor in dormant state", &self.base.vdpu_id);
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
                    true,
                )
                .await?,
            );
            first_time = true;
        }

        // update basic info of NPU HA scope state
        self.base.update_npu_ha_scope_state_base(state)?;

        if first_time {
            self.set_npu_local_ha_state(state, HaState::Dead, "initialized to be dead")?;
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
    fn handle_haset_state_update_npu_driven_mode(&mut self, state: &mut State) -> Result<HaEvent> {
        // the ha_scope is not managing the target vDPU. Skip
        if !self.base.vdpu_is_managed(state.incoming()) {
            return Ok(HaEvent::None);
        }

        // update basic info of NPU HA scope state
        let _ = self.base.update_npu_ha_scope_state_base(state);

        let first_time = self.base.peer_vdpu_id.is_none();

        let Some(ha_set) = self.base.get_haset(state.incoming()) else {
            return Ok(HaEvent::None);
        };
        let peer_vdpu_id = self.base.get_remote_vdpu_id(&ha_set);
        if self.base.peer_vdpu_id.is_none() || self.base.peer_vdpu_id != peer_vdpu_id {
            if self.base.peer_vdpu_id.is_some() {
                // Got a new peer HA scope actor than the old one
                // the behavior in this scenario is currently undefined
                error!("Dynamically changing peer is not supported!");
                return Ok(HaEvent::None);
            } else {
                // Got a fresh new peer HA scope actor
                self.base.peer_vdpu_id = peer_vdpu_id;

                // Send a heartbeat message to the peer ha scope actor as a request to connect
                self.send_heartbeat_to_peer(state)?;
                // Send a signal to itself to schedule a check later
                let outgoing = state.outgoing();
                if let Ok(msg) = SelfNotification::new_actor_msg(&self.base.id, "CheckPeerConnection") {
                    outgoing.send_with_delay(
                        outgoing.from_my_sp(HaScopeActor::name(), &self.base.id),
                        msg,
                        Duration::from_secs(RETRY_INTERVAL.into()),
                    );
                }
            }
        }

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
            "Received New DPU_HA_SCOPE_STATE_TABLE: ha_role-{}, ha_term-{}",
            new_dpu_ha_scope_state.ha_role.clone(),
            new_dpu_ha_scope_state.ha_term.clone()
        );

        npu_ha_scope_state.local_acked_asic_ha_state = Some(new_dpu_ha_scope_state.ha_role.clone());
        npu_ha_scope_state.local_acked_term = Some(new_dpu_ha_scope_state.ha_term.clone());
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
        let change: Option<HAStateChanged> = self.base.decode_hascope_actor_message(incoming, key);
        let Some(change) = change else {
            return Err(anyhow!("Failed to decode HAStateChanged message"));
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
            // we treat the first HaStateChanged message from the peer as a confirmation of connection
            self.peer_connected = true;
            Ok(HaEvent::PeerConnected)
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
            outgoing.send(outgoing.from_my_sp(HaScopeActor::name(), source_actor_id), msg);
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
                // TODO: retry logic
            }
            _ => {
                return Ok(HaEvent::None);
            }
        }
        Ok(HaEvent::VoteCompleted)
    }

    /// Handle async self notification messages
    fn handle_self_notification(&mut self, state: &mut State, key: &str) -> Result<HaEvent> {
        let (_internal, incoming, _outgoing) = state.get_all();
        let notification: Option<SelfNotification> = self.base.decode_hascope_actor_message(incoming, key);
        let Some(notification) = notification else {
            return Err(anyhow!("Failed to decode SelfNotification message"));
        };

        if notification.event == "CheckPeerConnection" {
            return self.check_peer_connection_and_retry(state);
        }

        if let Some(event) = HaEvent::from_str(&notification.event) {
            Ok(event)
        } else {
            Err(anyhow!("Unknown event"))
        }
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
    ) -> Result<()> {
        let _internal = state.internal();
        match pending_state {
            HaState::Connected => {
                // Send VoteRequest to the peer to start primary election
                self.send_vote_request_to_peer(state)?;
            }
            HaState::PendingActiveActivation | HaState::PendingStandbyActivation => {
                let operations: Vec<(String, String)> = vec![(Uuid::new_v4().to_string(), "activate_role".to_string())];
                self.base
                    .update_npu_ha_scope_state_pending_operations(state, operations, Vec::new())?;
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
                let _ = self.update_dpu_ha_scope_table_with_params(state, HaRole::Active.as_str_name(), false, false);
            }
            HaState::InitializingToStandby => {
                // Activate Standby role on DPU
                let _ = self.update_dpu_ha_scope_table_with_params(state, HaRole::Standby.as_str_name(), false, false);
            }
            HaState::Standby => {
                // The Standby role on DPU should have been activated, this operation is to update the term
                let _ = self.update_dpu_ha_scope_table_with_params(state, HaRole::Standby.as_str_name(), false, false);
            }
            HaState::SwitchingToActive => {
                // TODO: Send SwitchOver to the peer
            }
            HaState::Destroying => {
                // Activate Dead role on the DPU
                let _ = self.update_dpu_ha_scope_table_with_params(state, HaRole::Dead.as_str_name(), false, false);
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
        let Some(_vdpu) = self.base.get_vdpu(state.incoming()) else {
            info!("vDPU {} has not been initialized yet", &self.base.vdpu_id);
            return Ok(());
        };

        let current_state = self.current_npu_ha_state(state.internal());
        let mut event_to_use = *event;

        if *event == HaEvent::AdminStateChanged {
            if config.disabled {
                if current_state != HaState::Dead {
                    self.set_npu_local_ha_state(state, HaState::Dead, "admin disabled")?;
                    // Update DPU APPL_DB to activate Dead role on the DPU
                    let _ = self.update_dpu_ha_scope_table_with_params(state, HaRole::Dead.as_str_name(), false, false);
                }
                return Ok(());
            } else {
                // Equivalent to launch
                event_to_use = HaEvent::Launch;
            }
        }

        let target_state = self.target_ha_scope_state.unwrap_or(TargetState::Unspecified);

        match self.next_state(state, &target_state, &current_state, &event_to_use) {
            Some((next_state, reason)) if next_state != current_state => {
                debug!("Pending next state: {}, reason: {}", next_state.as_str_name(), reason);
                let pending_state = next_state;
                self.apply_pending_state_side_effects(state, &current_state, &pending_state)?;
                self.set_npu_local_ha_state(state, pending_state, reason)?;

                // send out HAStateChanged message to the peer
                let (internal, _incoming, outgoing) = state.get_all();
                let npu_state = self.base.get_npu_ha_scope_state(internal);
                let local_target_term = npu_state.as_ref().and_then(|s| s.local_target_term.as_deref());
                if let Some(peer_actor_id) = self.base.get_peer_actor_id() {
                    if let Ok(msg) = HAStateChanged::new_actor_msg(
                        &self.base.id,
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
                    .base
                    .dash_ha_scope_config
                    .as_ref()
                    .map(|c| c.owner)
                    .unwrap_or(HaOwner::Unspecified as i32);
                if let Some(ref npu_state) = npu_state {
                    if let Ok(msg) = HaScopeActorState::new_actor_msg(
                        &self.base.id,
                        owner,
                        npu_state,
                        &self.base.vdpu_id,
                        self.base.peer_vdpu_id.as_deref().unwrap_or(""),
                    ) {
                        if let Some(ha_set_id) = self.base.get_haset_id() {
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
                    if self.base.dpu_ha_scope_state.as_ref().map(|s| s.ha_role.as_str())
                        == Some(HaRole::Dead.as_str_name())
                    {
                        // HaEvent::DpuStateChanged should trigger this branch
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
                    Some((HaState::Connected, "connection with peer established"))
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
                if *event == HaEvent::DesiredStateChanged && *target_state == TargetState::Standby {
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
    /// Check if the peer HA scope is connected
    /// If not, send a self notification to schedule an execution of the same function for later
    /// Upon exceeding retry count threshold, signal a PeerLost event
    fn check_peer_connection_and_retry(&mut self, state: &mut State) -> Result<HaEvent> {
        if !self.peer_connected {
            if self.retry_count < MAX_RETRIES {
                // retry sending peer with HAStateChanged Registration Message
                self.retry_count += 1;

                // send another heartbeat message
                self.send_heartbeat_to_peer(state)?;

                // Send a signal to itself to schedule a check later
                let outgoing = state.outgoing();
                if let Ok(msg) = SelfNotification::new_actor_msg(&self.base.id, "CheckPeerConnection") {
                    outgoing.send_with_delay(
                        outgoing.from_my_sp(HaScopeActor::name(), &self.base.id),
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
        Ok(HaEvent::None)
    }

    /// Send heartbeat to the peer HaScopeActor
    /// Serve as peer connect request during launch process
    pub fn send_heartbeat_to_peer(&self, state: &mut State) -> Result<()> {
        let outgoing = state.outgoing();
        let Some(peer_actor_id) = self.base.get_peer_actor_id() else {
            // Haven't received the remote peer vDPU info yet
            info!("Haven't received peer vDPU info yet");
            return Ok(());
        };

        let msg = PeerHeartbeat::new_actor_msg(&self.base.id, &peer_actor_id)?;
        outgoing.send(outgoing.from_my_sp(HaScopeActor::name(), &peer_actor_id), msg);
        Ok(())
    }

    /// Send a VoteRequest message to the peer HA scope actor for primary election
    /// The term and state come from NPU HA scope state table
    /// The desired state comes from dash_ha_scope_config
    fn send_vote_request_to_peer(&self, state: &mut State) -> Result<()> {
        let (internal, _incoming, outgoing) = state.get_all();

        let Some(peer_actor_id) = self.base.get_peer_actor_id() else {
            info!("Cannot send VoteRequest to peer: peer actor ID not available");
            return Ok(());
        };

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

        let Some(peer_actor_id) = self.base.get_peer_actor_id() else {
            info!("Cannot send BulkSyncCompleted to peer: peer actor ID not available");
            return Ok(());
        };

        let msg = BulkSyncUpdate::new_actor_msg(
            &self.base.id,
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

        let ha_set_id = self.base.get_haset_id().unwrap();
        let Some(haset) = self.base.get_haset(incoming) else {
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
    fn update_dpu_ha_scope_table_with_params(
        &self,
        state: &mut State,
        ha_role: &str,
        flow_reconcile_requested: bool,
        activate_role_requested: bool,
    ) -> Result<()> {
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

        let dash_ha_scope = DashHaScopeTable {
            version: dash_ha_scope_config.version.parse().unwrap(),
            disabled: dash_ha_scope_config.disabled,
            ha_set_id: dash_ha_scope_config.ha_set_id.clone(),
            vip_v4: haset.ha_set.vip_v4.clone(),
            vip_v6: haset.ha_set.vip_v6.clone(),
            ha_role: ha_role.to_owned(),
            ha_term: self
                .base
                .get_npu_ha_scope_state(internal)
                .and_then(|s| s.local_target_term)
                .unwrap_or("0".to_string()),
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
}
