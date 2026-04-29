mod base;
mod dpu;
mod npu;

use crate::actors::DbBasedActor;
use anyhow::Result;
use sonic_common::SonicDbTable;
use sonic_dash_api_proto::ha_scope_config::HaScopeConfig;
use sonic_dash_api_proto::types::HaOwner;
use swbus_actor::{Actor, Context, State};
use swss_common::{KeyOpFieldValues, KeyOperation};
use tracing::{error, info, instrument};

use base::HaScopeBase;
use dpu::DpuHaScopeActor;
use npu::NpuHaScopeActor;

const MAX_RETRIES: u32 = 3;
const RETRY_INTERVAL: u32 = 30; // seconds
const BULK_SYNC_TIMEOUT: u32 = 150; // seconds
const INLINE_SYNC_PKT_DROP_ALERT_THRESHOLD: u32 = 30; // packets

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
    SwitchoverRequested,
    SwitchoverFailed,
    AdminStateChanged,
    DesiredStateChanged,
    DpuStateChanged,
    EnterStandalone,
    EnterActive,
    EnterStandby,
    LeavingStandalone,
    Shutdown,
    PeerShutdownRequested,
    HighInlineSyncDrops,
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
            Self::SwitchoverRequested => "SwitchoverRequested",
            Self::SwitchoverFailed => "SwitchoverFailed",
            Self::AdminStateChanged => "AdminStateChanged",
            Self::DesiredStateChanged => "DesiredStateChanged",
            Self::DpuStateChanged => "DpuStateChanged",
            Self::EnterStandalone => "EnterStandalone",
            Self::EnterActive => "EnterActive",
            Self::EnterStandby => "EnterStandby",
            Self::LeavingStandalone => "LeavingStandalone",
            Self::Shutdown => "Shutdown",
            Self::PeerShutdownRequested => "PeerShutdownRequested",
            Self::HighInlineSyncDrops => "HighInlineSyncDrops",
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
            "SwitchoverRequested" => Some(Self::SwitchoverRequested),
            "SwitchoverFailed" => Some(Self::SwitchoverFailed),
            "AdminStateChanged" => Some(Self::AdminStateChanged),
            "DesiredStateChanged" => Some(Self::DesiredStateChanged),
            "DpuStateChanged" => Some(Self::DpuStateChanged),
            "EnterStandalone" => Some(Self::EnterStandalone),
            "EnterActive" => Some(Self::EnterActive),
            "EnterStandby" => Some(Self::EnterStandby),
            "LeavingStandalone" => Some(Self::LeavingStandalone),
            "Shutdown" => Some(Self::Shutdown),
            "PeerShutdownRequested" => Some(Self::PeerShutdownRequested),
            "HighInlineSyncDrops" => Some(Self::HighInlineSyncDrops),
            _ => None,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum TargetState {
    Unspecified,
    Active,
    Standby,
    Standalone,
    Dead,
}

pub enum HaScopeActor {
    Uninitialized(Option<HaScopeBase>),
    Dpu(DpuHaScopeActor),
    Npu(NpuHaScopeActor),
}

impl HaScopeActor {
    fn base(&self) -> &HaScopeBase {
        match self {
            HaScopeActor::Uninitialized(Some(base)) => base,
            HaScopeActor::Dpu(actor) => &actor.base,
            HaScopeActor::Npu(actor) => &actor.base,
            HaScopeActor::Uninitialized(None) => {
                panic!("HaScopeActor::Uninitialized(None) is unreachable after construction")
            }
        }
    }

    fn base_mut(&mut self) -> &mut HaScopeBase {
        match self {
            HaScopeActor::Uninitialized(Some(base)) => base,
            HaScopeActor::Dpu(actor) => &mut actor.base,
            HaScopeActor::Npu(actor) => &mut actor.base,
            HaScopeActor::Uninitialized(None) => {
                panic!("HaScopeActor::Uninitialized(None) is unreachable after construction")
            }
        }
    }
}

impl DbBasedActor for HaScopeActor {
    fn new(key: String) -> Result<Self> {
        Ok(HaScopeActor::Uninitialized(Some(HaScopeBase::new(key)?)))
    }

    fn table_name() -> &'static str {
        HaScopeConfig::table_name()
    }

    fn name() -> &'static str {
        "ha-scope"
    }
}

impl Actor for HaScopeActor {
    #[instrument(name="handle_message", level="info", skip_all, fields(actor=format!("ha-scope/{}", self.base().id), key=key))]
    async fn handle_message(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        // Handle delete messages on any variant
        if key == Self::table_name() {
            let incoming = state.incoming();
            let kfv: KeyOpFieldValues = incoming.get_or_fail(key)?.deserialize_data()?;

            if kfv.operation == KeyOperation::Del {
                // cleanup resources before stopping
                if let Err(e) = self.base_mut().do_cleanup(state) {
                    error!("Failed to cleanup HaScopeActor resources: {}", e);
                }
                context.stop();
                info!("Ha Scope Actor Context was stopped!");
                return Ok(());
            }
        }

        // Handle Uninitialized state
        if let HaScopeActor::Uninitialized(ref mut opt_base) = self {
            if key == Self::table_name() {
                // First config message — take ownership of the base, determine variant
                let mut base = opt_base.take().expect("HaScopeBase should be Some in Uninitialized");
                let owner = base.handle_first_config_message(state, key)?;

                *self = match owner {
                    HaOwner::Dpu => HaScopeActor::Dpu(DpuHaScopeActor { base }),
                    _ => HaScopeActor::Npu(NpuHaScopeActor::new(base)),
                };
                info!("Initialize HA Scope based on owner {}", owner.as_str_name());

                // Immediately delegate the first config message to the new variant
                match self {
                    HaScopeActor::Dpu(actor) => return actor.handle_message_inner(state, key, context).await,
                    HaScopeActor::Npu(actor) => return actor.handle_message_inner(state, key, context).await,
                    _ => unreachable!(),
                }
            } else {
                // Non-config message on uninitialized actor — ignore
                return Ok(());
            }
        }

        // Dispatch to the appropriate variant
        match self {
            HaScopeActor::Dpu(actor) => actor.handle_message_inner(state, key, context).await,
            HaScopeActor::Npu(actor) => actor.handle_message_inner(state, key, context).await,
            HaScopeActor::Uninitialized(_) => unreachable!(),
        }
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
        db_structs::{
            now_in_millis, CountersEniNameMapTable, CountersTable, DashFlowSyncSessionTable, DashHaScopeTable,
            DpuDashHaScopeState, NpuDashHaScopeState, ENI_INLINE_FLOW_SYNC_RX_PKTS, ENI_INLINE_FLOW_SYNC_TX_PKTS,
        },
        ha_actor_messages::*,
    };
    use sonic_common::SonicDbTable;
    use sonic_dash_api_proto::ha_scope_config::{DesiredHaState, HaScopeConfig};
    use sonic_dash_api_proto::types::{HaOwner, HaRole, HaState};
    use std::time::Duration;
    use swss_common::Table;
    use swss_common_testing::*;
    use swss_serde::to_field_values;

    mod dpu_driven {
        use super::*;

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
            let dpu1 = make_remote_dpu_actor_state(1, 0);
            let (vdpu0_id, vdpu0_state_obj) = make_vdpu_actor_state(true, &dpu0);
            let (vdpu1_id, _vdpu1_state_obj) = make_vdpu_actor_state(true, &dpu1);

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

                // Recv HaScopeActorState destined to the ha-set actor
                recv!( key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Dpu as i32, "new_state": HaState::Unspecified.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": "" }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp"),

                // Send vDPU state to actor
                send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state_obj, addr: runtime.sp("vdpu", &vdpu0_id) },

                // Send ha-set state to actor
                send! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": true, "ha_set": &ha_set_obj, "vdpu_ids": vec![vdpu0_id.clone(), vdpu1_id.clone()], "pinned_vdpu_bfd_probe_states": vec!["".to_string()] }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

                // Recv update to DPU DASH_HA_SCOPE_TABLE with ha_role = active
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "1",
                            "ha_role": "active",
                            "ha_term": "0",
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
                            "ha_term": "0",
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
            dpu_ha_state_state5.ha_term = Some("2".to_string());
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
                            "ha_term": "0",
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
                            "ha_term": "0",
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
                            "ha_term": "0",
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

    mod npu_driven {
        use super::*;

        #[tokio::test]
        async fn ha_scope_npu_launch_to_active_then_down() {
            sonic_common::log::init_logger_for_test();
            let _redis = Redis::start_config_db();
            test::setup_remote_dpu_in_db(1, 0);
            let runtime = test::create_actor_runtime(2, "10.0.2.0", "10:0:2::").await;
            test::setup_mock_swbusd_resolve_peer_sp(&runtime.get_swbus_edge());

            let (ha_set_id, ha_set_obj) = make_dpu_scope_ha_set_obj(2, 0);
            let dpu_mon = make_dpu_pmon_state(true);
            let bfd_state = make_dpu_bfd_state(Vec::new(), Vec::new());
            let dpu0 = make_local_dpu_actor_state(0, 0, true, Some(dpu_mon), Some(bfd_state));
            let dpu1 = make_remote_dpu_actor_state(1, 0);
            let (vdpu0_id, vdpu0_state_obj) = make_vdpu_actor_state(true, &dpu0);
            let (vdpu1_id, _vdpu1_state_obj) = make_vdpu_actor_state(true, &dpu1);

            let scope_id = format!("{vdpu0_id}:{ha_set_id}");
            let scope_id_in_state = format!("{vdpu0_id}|{ha_set_id}");
            let peer_scope_id = format!("{vdpu1_id}:{ha_set_id}");

            // Expected NPU DASH_HA_SCOPE_STATE in PendingActiveActivation
            let mut npu_ha_scope_state_pending_active_activation =
                make_npu_ha_scope_state(&vdpu0_state_obj, &ha_set_obj);
            npu_ha_scope_state_pending_active_activation.local_ha_state =
                Some(HaState::PendingActiveActivation.as_str_name().to_string());
            npu_ha_scope_state_pending_active_activation.peer_ha_state =
                Some(HaState::InitializingToStandby.as_str_name().to_string());
            npu_ha_scope_state_pending_active_activation.peer_term = Some("0".to_string());
            npu_ha_scope_state_pending_active_activation.pending_operation_types =
                Some(vec!["activate_role".to_string()]);
            let npu_ha_scope_state_pending_active_activation_fvs =
                to_field_values(&npu_ha_scope_state_pending_active_activation).unwrap();

            let ha_scope_actor = HaScopeActor::new(scope_id.clone()).unwrap();
            let handle = runtime.spawn(ha_scope_actor, HaScopeActor::name(), &scope_id);

            // Launch to PendingActiveActivation
            #[rustfmt::skip]
            let commands = [
                // Init HA Scope Config with owner as Switch
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"1","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                // Expect initial registration messages
                recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &scope_id), data: { "active": true }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::HaSetState, &scope_id), data: { "active": true }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Unspecified.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": "" }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock initial DPU & HA set updates
                send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state_obj, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                send! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": true, "ha_set": &ha_set_obj, "vdpu_ids": vec![vdpu0_id.clone(), vdpu1_id.clone()], "pinned_vdpu_bfd_probe_states": vec!["".to_string()] }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

                // A PeerHeartbeat should be triggered
                recv! { key: PeerHeartbeat::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },
                // Mock the init HaScopeActorState from the peer HA scope
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::Dead.as_str_name(), "term": "0", "vdpu_id": "", "peer_vdpu_id": "" } },
                // Expect HaScopeActorState: dead -> connecting
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connecting.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connecting.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                // Expect a VoteRequest to be sent
                recv!( key: VoteRequest::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "term": "0", "state": HaState::Connecting.as_str_name(), "desired_state": DesiredHaState::Active.as_str_name() }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) ),
                // Expect HaScopeActorState: connecting -> connected
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connected.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connected.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock a VoteReply
                send! { key: VoteReply::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id, "response": "BecomeActive" } },
                // Expect HaScopeActorState: connected -> initializing_to_active
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::InitializingToActive.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::InitializingToActive.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                // Mock the HaScopeActorState from the peer HA scope
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::InitializingToStandby.as_str_name(), "term": "0", "vdpu_id": "", "peer_vdpu_id": "" } },
                // Expect HaScopeActorState: initializing_to_active -> pending_active_activation
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::PendingActiveActivation.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::PendingActiveActivation.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                

                // Expect a pending role activation request in NPU DASH_HA_SCOPE_STATE table
                chkdb! { type: NpuDashHaScopeState,
                        key: &scope_id_in_state, data: npu_ha_scope_state_pending_active_activation_fvs,
                        exclude: "pending_operation_list_last_updated_time_in_ms,pending_operation_ids,local_ha_state_last_updated_time_in_ms,local_ha_state_last_updated_reason,peer_ha_state_last_updated_time_in_ms" },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let mut npu_ha_scope_state: NpuDashHaScopeState =
                swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            let op_id = npu_ha_scope_state
                .pending_operation_ids
                .as_mut()
                .unwrap()
                .pop()
                .unwrap();

            // Approval of pending operation to Active state
            #[rustfmt::skip]
            let commands = [
                // New HA scope config containing approved op_id
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"2","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":["{op_id}"]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },
                
                // Expect a DPU DASH_HA_SCOPE_TABLE update
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "2",
                            "ha_role": "active",
                            "ha_term": "1",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },

                // Expect a bulkSyncCompleted message
                recv! { key: BulkSyncUpdate::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "finished": true }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },

                // Expect HaScopeActorState: pending_active_activation -> active
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Active.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Active.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Active.as_str_name())
            );
            assert_eq!(npu_ha_scope_state.local_target_term.as_deref(), Some("1"));

            let mut dpu_ha_state_state = make_dpu_ha_scope_state(HaRole::Dead.as_str_name());
            dpu_ha_state_state.ha_term = Some("1".to_string());

            // Shutdown
            // Note: Active node can only be forced shutdown by setting disabled to true
            #[rustfmt::skip]
            let commands = [
                // Set the disabled to true
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"3","disabled": true,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                // Expect a DPU DASH_HA_SCOPE_TABLE update
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "3",
                            "ha_role": "dead",
                            "ha_term": "1",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },

                // Expect HaScopeActorState: Active -> Dead
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Dead.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Dead.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Delete the HA scope config entry
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Del",
                        "field_values": {"json": format!(r#"{{"version":"3","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Dead as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                chkdb! { type: NpuDashHaScopeState, key: &scope_id_in_state, nonexist },
                recv! { key: &ha_set_id, data: { "key": &ha_set_id, "operation": "Del", "field_values": {} }, addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge()) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &scope_id), data: { "active": false }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::HaSetState, &scope_id), data: { "active": false }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;
            if tokio::time::timeout(Duration::from_secs(5), handle).await.is_err() {
                panic!("timeout waiting for actor to terminate");
            }
        }

        #[tokio::test]
        async fn ha_scope_npu_launch_to_standby_then_down() {
            sonic_common::log::init_logger_for_test();
            let _redis = Redis::start_config_db();
            test::setup_remote_dpu_in_db(1, 0);
            let runtime = test::create_actor_runtime(4, "10.0.4.0", "10:0:4::").await;
            test::setup_mock_swbusd_resolve_peer_sp(&runtime.get_swbus_edge());

            let (ha_set_id, ha_set_obj) = make_dpu_scope_ha_set_obj(4, 0);
            let dpu_mon = make_dpu_pmon_state(true);
            let bfd_state = make_dpu_bfd_state(Vec::new(), Vec::new());
            let dpu0 = make_local_dpu_actor_state(0, 0, true, Some(dpu_mon), Some(bfd_state));
            let dpu1 = make_remote_dpu_actor_state(1, 0);
            let (vdpu0_id, vdpu0_state_obj) = make_vdpu_actor_state(true, &dpu0);
            let (vdpu1_id, _vdpu1_state_obj) = make_vdpu_actor_state(true, &dpu1);

            let scope_id = format!("{vdpu0_id}:{ha_set_id}");
            let scope_id_in_state = format!("{vdpu0_id}|{ha_set_id}");
            let peer_scope_id = format!("{vdpu1_id}:{ha_set_id}");

            // Expected NPU DASH_HA_SCOPE_STATE in PendingStandbyActivation
            let mut npu_ha_scope_state_pending_standby_activation =
                make_npu_ha_scope_state(&vdpu0_state_obj, &ha_set_obj);
            npu_ha_scope_state_pending_standby_activation.local_ha_state =
                Some(HaState::PendingStandbyActivation.as_str_name().to_string());
            npu_ha_scope_state_pending_standby_activation.local_target_term = Some("1".to_string());
            npu_ha_scope_state_pending_standby_activation.peer_ha_state =
                Some(HaState::Active.as_str_name().to_string());
            npu_ha_scope_state_pending_standby_activation.peer_term = Some("1".to_string());
            npu_ha_scope_state_pending_standby_activation.pending_operation_types =
                Some(vec!["activate_role".to_string()]);
            let npu_ha_scope_state_pending_standby_activation_fvs =
                to_field_values(&npu_ha_scope_state_pending_standby_activation).unwrap();

            let ha_scope_actor = HaScopeActor::new(scope_id.clone()).unwrap();
            let handle = runtime.spawn(ha_scope_actor, HaScopeActor::name(), &scope_id);

            // Launch to PendingStandbyActivation
            #[rustfmt::skip]
            let commands = [
                // Init HA Scope Config with owner as Switch
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"1","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Unspecified as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                // Expect initial registration messages
                recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &scope_id), data: { "active": true }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::HaSetState, &scope_id), data: { "active": true }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Unspecified.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": "" }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock initial DPU & HA set updates
                send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state_obj, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                send! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": true, "ha_set": &ha_set_obj, "vdpu_ids": vec![vdpu0_id.clone(), vdpu1_id.clone()], "pinned_vdpu_bfd_probe_states": vec!["".to_string()] }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

                // A PeerHeartbeat should be triggered
                recv! { key: PeerHeartbeat::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },
                // Expect HaScopeActorState: dead -> connecting
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connecting.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connecting.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock the init HaScopeActorState from the peer HA scope
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::Dead.as_str_name(), "term": "0", "vdpu_id": "", "peer_vdpu_id": "" } },
                // Expect a VoteRequest to be sent
                recv!( key: VoteRequest::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "term": "0", "state": HaState::Connecting.as_str_name(), "desired_state": DesiredHaState::Unspecified.as_str_name() }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) ),
                // Expect HaScopeActorState: connecting -> connected
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connected.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connected.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock a VoteReply
                send! { key: VoteReply::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id, "response": "BecomeStandby" } },
                // Expect a DPU DASH_HA_SCOPE_TABLE update
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "1",
                            "ha_role": "standby",
                            "ha_term": "0",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },
                // Expect a HaScopeActorState: connected -> initializing_to_standby
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::InitializingToStandby.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::InitializingToStandby.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                
                // Mock a bulkSyncCompleted message from the peer
                send! { key: BulkSyncUpdate::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id, "finished": true }},
                // Expect a HaScopeActorState: initializing_to_standby -> pending_standby_activation
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::PendingStandbyActivation.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::PendingStandbyActivation.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                // Mock the HaScopeActorState from the peer HA scope that carries the new term
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::Active.as_str_name(), "term": "1", "vdpu_id": "", "peer_vdpu_id": "" } },

                // Expect a pending role activation request in NPU DASH_HA_SCOPE_STATE table
                chkdb! { type: NpuDashHaScopeState,
                        key: &scope_id_in_state, data: npu_ha_scope_state_pending_standby_activation_fvs,
                        exclude: "pending_operation_list_last_updated_time_in_ms,pending_operation_ids,local_ha_state_last_updated_time_in_ms,local_ha_state_last_updated_reason,peer_ha_state_last_updated_time_in_ms" },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let mut npu_ha_scope_state: NpuDashHaScopeState =
                swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            let op_id = npu_ha_scope_state
                .pending_operation_ids
                .as_mut()
                .unwrap()
                .pop()
                .unwrap();

            // Approval of pending operation to Active state
            #[rustfmt::skip]
            let commands = [
                // New HA scope config containing approved op_id
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"2","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":["{op_id}"]}}"#, DesiredHaState::Unspecified as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                // Expect a DPU DASH_HA_SCOPE_TABLE update to set the new term
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "2",
                            "ha_role": "standby",
                            "ha_term": "1",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },
                // Expect a HaScopeActorState: pending_standby_activation -> standby
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standby.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standby.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Standby.as_str_name())
            );
            assert_eq!(npu_ha_scope_state.local_target_term.as_deref(), Some("1"));

            let mut dpu_ha_state_state = make_dpu_ha_scope_state(HaRole::Dead.as_str_name());
            dpu_ha_state_state.ha_term = Some("1".to_string());

            // Planned Shutdown: Standby sends ShutdownRequest to peer, waits for acceptance
            #[rustfmt::skip]
            let commands = [
                // Set the desired state to Dead
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"3","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Dead as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                // Expect a ShutdownRequest to be sent to the peer
                recv! { key: ShutdownRequest::msg_key(&scope_id), data: { "reason": "planned shutdown" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },

                // Mock the peer's ShutdownReply with "accepted"
                send! { key: ShutdownReply::msg_key(&peer_scope_id), data: { "response": "accepted" } },

                // Expect a DPU DASH_HA_SCOPE_TABLE update (Destroying side effect)
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "3",
                            "ha_role": "dead",
                            "ha_term": "1",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },

                // Expect HaScopeActorState: Standby -> Destroying
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Destroying.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Destroying.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Send DPU DASH_HA_SCOPE_STATE with ha_role = dead
                send! { key: DpuDashHaScopeState::table_name(), data: {"key": DpuDashHaScopeState::table_name(), "operation": "Set",
                        "field_values": serde_json::to_value(to_field_values(&dpu_ha_state_state).unwrap()).unwrap()
                        }},

                // Expect HaScopeActorState: Destroying -> Dead
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Dead.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Dead.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Delete the HA scope config entry
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Del",
                        "field_values": {"json": format!(r#"{{"version":"3","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Dead as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                chkdb! { type: NpuDashHaScopeState, key: &scope_id_in_state, nonexist },
                recv! { key: &ha_set_id, data: { "key": &ha_set_id, "operation": "Del", "field_values": {} }, addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge()) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &scope_id), data: { "active": false }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::HaSetState, &scope_id), data: { "active": false }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;
            if tokio::time::timeout(Duration::from_secs(5), handle).await.is_err() {
                panic!("timeout waiting for actor to terminate");
            }
        }

        #[tokio::test]
        async fn ha_scope_npu_active_to_standalone_on_peer_shutdown_and_peer_rejoin() {
            sonic_common::log::init_logger_for_test();
            let _redis = Redis::start_config_db();
            test::setup_remote_dpu_in_db(1, 0);
            let runtime = test::create_actor_runtime(8, "10.0.8.0", "10:0:8::").await;
            test::setup_mock_swbusd_resolve_peer_sp(&runtime.get_swbus_edge());

            let (ha_set_id, ha_set_obj) = make_dpu_scope_ha_set_obj(8, 0);
            let dpu_mon = make_dpu_pmon_state(true);
            let bfd_state = make_dpu_bfd_state(Vec::new(), Vec::new());
            let dpu0 = make_local_dpu_actor_state(0, 0, true, Some(dpu_mon), Some(bfd_state));
            let dpu1 = make_remote_dpu_actor_state(1, 0);
            let (vdpu0_id, vdpu0_state_obj) = make_vdpu_actor_state(true, &dpu0);
            let (vdpu1_id, _vdpu1_state_obj) = make_vdpu_actor_state(true, &dpu1);

            let scope_id = format!("{vdpu0_id}:{ha_set_id}");
            let scope_id_in_state = format!("{vdpu0_id}|{ha_set_id}");
            let peer_scope_id = format!("{vdpu1_id}:{ha_set_id}");

            let ha_scope_actor = HaScopeActor::new(scope_id.clone()).unwrap();
            let handle = runtime.spawn(ha_scope_actor, HaScopeActor::name(), &scope_id);

            // ============================================================
            // Phase 1: Launch to Active (same as launch_to_active test)
            // ============================================================
            #[rustfmt::skip]
            let commands = [
                // Init HA Scope Config with owner as Switch and desired state Active
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"1","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                // Expect initial registration messages
                recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &scope_id), data: { "active": true }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::HaSetState, &scope_id), data: { "active": true }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Unspecified.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": "" }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock initial DPU & HA set updates
                send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state_obj, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                send! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": true, "ha_set": &ha_set_obj, "vdpu_ids": vec![vdpu0_id.clone(), vdpu1_id.clone()], "pinned_vdpu_bfd_probe_states": vec!["".to_string()] }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

                // A PeerHeartbeat should be triggered
                recv! { key: PeerHeartbeat::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },
                // Mock the init HaScopeActorState from the peer HA scope
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::Dead.as_str_name(), "term": "0", "vdpu_id": "", "peer_vdpu_id": "" } },
                // Expect HaScopeActorState: dead -> connecting
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connecting.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connecting.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                // Expect a VoteRequest to be sent
                recv!( key: VoteRequest::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "term": "0", "state": HaState::Connecting.as_str_name(), "desired_state": DesiredHaState::Active.as_str_name() }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) ),
                // Expect HaScopeActorState: connecting -> connected
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connected.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connected.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock a VoteReply: BecomeActive
                send! { key: VoteReply::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id, "response": "BecomeActive" } },
                // Expect HaScopeActorState: connected -> initializing_to_active
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::InitializingToActive.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::InitializingToActive.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                // Mock the HaScopeActorState from the peer HA scope
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::InitializingToStandby.as_str_name(), "term": "0", "vdpu_id": "", "peer_vdpu_id": "" } },
                // Expect HaScopeActorState: initializing_to_active -> pending_active_activation
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::PendingActiveActivation.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::PendingActiveActivation.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Approve pending activation to reach Active state
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let mut npu_ha_scope_state: NpuDashHaScopeState =
                swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            let op_id = npu_ha_scope_state
                .pending_operation_ids
                .as_mut()
                .unwrap()
                .pop()
                .unwrap();

            #[rustfmt::skip]
            let commands = [
                // New HA scope config containing approved op_id
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"2","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":["{op_id}"]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                // Expect a DPU DASH_HA_SCOPE_TABLE update
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "2",
                            "ha_role": "active",
                            "ha_term": "1",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },

                // Expect a bulkSyncCompleted message
                recv! { key: BulkSyncUpdate::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "finished": true }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },

                // Expect HaScopeActorState: pending_active_activation -> active
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Active.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Active.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify we are in Active state
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Active.as_str_name())
            );
            assert_eq!(npu_ha_scope_state.local_target_term.as_deref(), Some("1"));

            // ============================================================
            // Phase 2: Standby peer sends ShutdownRequest → Active transitions
            // to SwitchingToStandalone → Standalone
            // ============================================================
            #[rustfmt::skip]
            let commands = [
                // Standby peer sends ShutdownRequest to the active node
                send! { key: ShutdownRequest::msg_key(&peer_scope_id), data: { "reason": "planned shutdown" } },

                // Expect ShutdownReply("accepted") sent back to the standby peer
                recv! { key: ShutdownReply::msg_key(&scope_id), data: { "response": "accepted" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },

                // Expect HaScopeActorState: Active -> SwitchingToStandalone
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::SwitchingToStandalone.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::SwitchingToStandalone.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // EnterStandalone self-notification is processed immediately:
                // Expect DPU DASH_HA_SCOPE_TABLE update with standalone role and incremented term
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "2",
                            "ha_role": "standalone",
                            "ha_term": "2",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },

                // Expect HaScopeActorState: SwitchingToStandalone -> Standalone
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standalone.as_str_name(), "term": "2", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standalone.as_str_name(), "term": "2", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify final state is Standalone with incremented term
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Standalone.as_str_name()),
                "Node should be in Standalone state after standby peer shutdown"
            );
            assert_eq!(
                npu_ha_scope_state.local_target_term.as_deref(),
                Some("2"),
                "Term should be incremented after entering Standalone"
            );

            // ============================================================
            // Phase 3: New peer launches from Dead state and rejoins.
            // Standalone node should transition to Active (term=3)
            // while the new peer enters Standby.
            // ============================================================
            // Spawn a background client to receive and ack the DashFlowSyncSessionTable
            // message sent by add_bulk_sync_session during Standalone → Active transition.
            // Without this, the actor's outgoing queue has an unacked message and won't terminate.
            {
                use swbus_edge::simple_client::{MessageBody, OutgoingMessage, SimpleSwbusEdgeClient};
                use swbus_edge::swbus_proto::swbus::SwbusErrorCode;
                let client = SimpleSwbusEdgeClient::new(
                    runtime.get_swbus_edge(),
                    crate::common_bridge_sp::<DashFlowSyncSessionTable>(&runtime.get_swbus_edge()),
                    true,
                    false,
                );
                let aut_sp = runtime.sp(HaScopeActor::name(), &scope_id);
                tokio::spawn(async move {
                    while let Some(msg) = client.recv().await {
                        if let MessageBody::Request { .. } = &msg.body {
                            let _ = client
                                .send(OutgoingMessage {
                                    destination: aut_sp.clone(),
                                    body: MessageBody::Response {
                                        request_id: msg.id,
                                        error_code: SwbusErrorCode::Ok,
                                        error_message: "".to_string(),
                                        response_body: None,
                                    },
                                })
                                .await;
                        }
                    }
                });
            }
            #[rustfmt::skip]
            let commands = [
                // New peer sends PeerHeartbeat — Standalone node responds with its state
                send! { key: PeerHeartbeat::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standalone.as_str_name(), "term": "2", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },

                // New peer sends VoteRequest (term=0, state=Connecting)
                // Standalone node has term=2 > peer term=0, so it replies BecomeStandby
                send! { key: VoteRequest::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id, "term": "0", "state": HaState::Connecting.as_str_name(), "desired_state": DesiredHaState::Unspecified.as_str_name() }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },
                recv! { key: VoteReply::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "response": "BecomeStandby" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },

                // New peer transitions to InitializingToStandby and broadcasts state
                // This triggers PeerStateChanged on the Standalone node → transitions to Active
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::InitializingToStandby.as_str_name(), "term": "0", "vdpu_id": "", "peer_vdpu_id": "" } },

                // Expect DPU HA scope table update with Active role and term=3
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "2",
                            "ha_role": "active",
                            "ha_term": "3",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },

                // Expect HaScopeActorState: Standalone -> Active with term=3
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Active.as_str_name(), "term": "3", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Active.as_str_name(), "term": "3", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify state is Active with term=3
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Active.as_str_name()),
                "Standalone node should transition to Active when peer rejoins"
            );
            assert_eq!(
                npu_ha_scope_state.local_target_term.as_deref(),
                Some("3"),
                "Term should be incremented to 3 after Standalone -> Active"
            );
            assert_eq!(
                npu_ha_scope_state.peer_ha_state.as_deref(),
                Some(HaState::InitializingToStandby.as_str_name()),
                "Peer should be in InitializingToStandby state"
            );

            // ============================================================
            // Phase 4: Cleanup — force shutdown the actor
            // ============================================================
            #[rustfmt::skip]
            let commands = [
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"3","disabled":true,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "3",
                            "ha_role": "dead",
                            "ha_term": "3",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },

                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Dead.as_str_name(), "term": "3", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Dead.as_str_name(), "term": "3", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Del",
                        "field_values": {"json": format!(r#"{{"version":"3","disabled":true,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                chkdb! { type: NpuDashHaScopeState, key: &scope_id_in_state, nonexist },
                recv! { key: &ha_set_id, data: { "key": &ha_set_id, "operation": "Del", "field_values": {} }, addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge()) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &scope_id), data: { "active": false }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::HaSetState, &scope_id), data: { "active": false }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;
            if tokio::time::timeout(Duration::from_secs(5), handle).await.is_err() {
                panic!("timeout waiting for actor to terminate");
            }
        }

        #[tokio::test]
        async fn ha_scope_npu_planned_switchover() {
            sonic_common::log::init_logger_for_test();
            let _redis = Redis::start_config_db();
            test::setup_remote_dpu_in_db(1, 0);
            let runtime = test::create_actor_runtime(6, "10.0.6.0", "10:0:6::").await;
            test::setup_mock_swbusd_resolve_peer_sp(&runtime.get_swbus_edge());

            let (ha_set_id, ha_set_obj) = make_dpu_scope_ha_set_obj(6, 0);
            let dpu_mon = make_dpu_pmon_state(true);
            let bfd_state = make_dpu_bfd_state(Vec::new(), Vec::new());
            let dpu0 = make_local_dpu_actor_state(0, 0, true, Some(dpu_mon), Some(bfd_state));
            let dpu1 = make_remote_dpu_actor_state(1, 0);
            let (vdpu0_id, vdpu0_state_obj) = make_vdpu_actor_state(true, &dpu0);
            let (vdpu1_id, _vdpu1_state_obj) = make_vdpu_actor_state(true, &dpu1);

            let scope_id = format!("{vdpu0_id}:{ha_set_id}");
            let scope_id_in_state = format!("{vdpu0_id}|{ha_set_id}");
            let peer_scope_id = format!("{vdpu1_id}:{ha_set_id}");

            let ha_scope_actor = HaScopeActor::new(scope_id.clone()).unwrap();
            let handle = runtime.spawn(ha_scope_actor, HaScopeActor::name(), &scope_id);

            // ============================================================
            // Phase 1: Launch to Standby (same flow as standby test)
            // ============================================================
            #[rustfmt::skip]
            let commands = [
                // Init HA Scope Config with desired_ha_state = Unspecified (will become standby)
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"1","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Unspecified as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                // Expect initial registration messages
                recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &scope_id), data: { "active": true }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::HaSetState, &scope_id), data: { "active": true }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Unspecified.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": "" }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock initial DPU & HA set updates
                send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state_obj, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                send! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": true, "ha_set": &ha_set_obj, "vdpu_ids": vec![vdpu0_id.clone(), vdpu1_id.clone()], "pinned_vdpu_bfd_probe_states": vec!["".to_string()] }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

                // A PeerHeartbeat should be triggered
                recv! { key: PeerHeartbeat::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },
                // Expect HaScopeActorState: dead -> connecting
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connecting.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connecting.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock the init HaScopeActorState from the peer HA scope
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::Dead.as_str_name(), "term": "0", "vdpu_id": "", "peer_vdpu_id": "" } },
                // Expect a VoteRequest to be sent
                recv!( key: VoteRequest::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "term": "0", "state": HaState::Connecting.as_str_name(), "desired_state": DesiredHaState::Unspecified.as_str_name() }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) ),
                // Expect HaScopeActorState: connecting -> connected
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connected.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connected.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock a VoteReply: BecomeStandby
                send! { key: VoteReply::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id, "response": "BecomeStandby" } },
                // Expect DPU DASH_HA_SCOPE_TABLE update with standby role
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "1",
                            "ha_role": "standby",
                            "ha_term": "0",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },
                // Expect HaScopeActorState: connected -> initializing_to_standby
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::InitializingToStandby.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::InitializingToStandby.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock a BulkSyncCompleted message from the peer
                send! { key: BulkSyncUpdate::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id, "finished": true }},
                // Expect HaScopeActorState: initializing_to_standby -> pending_standby_activation
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::PendingStandbyActivation.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::PendingStandbyActivation.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                // Mock the HaScopeActorState from the peer with new term (peer is now Active)
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::Active.as_str_name(), "term": "1", "vdpu_id": "", "peer_vdpu_id": "" } },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Approve pending standby activation
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let mut npu_ha_scope_state: NpuDashHaScopeState =
                swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            let op_id = npu_ha_scope_state
                .pending_operation_ids
                .as_mut()
                .unwrap()
                .pop()
                .unwrap();

            #[rustfmt::skip]
            let commands = [
                // Approve the pending standby activation
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"2","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":["{op_id}"]}}"#, DesiredHaState::Unspecified as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },
                // Expect DPU update for standby term
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "2",
                            "ha_role": "standby",
                            "ha_term": "1",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },
                // Expect HaScopeActorState: pending_standby_activation -> standby
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standby.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standby.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify we are in Standby state
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Standby.as_str_name())
            );
            assert_eq!(npu_ha_scope_state.local_target_term.as_deref(), Some("1"));

            // ============================================================
            // Phase 2: Planned Switchover — Standby initiates switchover
            // HLD Section 8.2.1 Steps 1-2: Change desired state to Active,
            // which creates a pending "switchover" operation.
            // ============================================================
            #[rustfmt::skip]
            let commands = [
                // Change desired_ha_state to Active — triggers pending switchover creation
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"3","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },
                // No state transition expected — Standby stays in Standby.
                // A pending "switchover" operation should be created in NPU HA scope state.
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify state is still Standby and a pending switchover operation was created
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let mut npu_ha_scope_state: NpuDashHaScopeState =
                swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Standby.as_str_name()),
                "State should remain Standby while waiting for switchover approval"
            );
            assert_eq!(
                npu_ha_scope_state.pending_operation_types.as_deref(),
                Some(&["switchover".to_string()][..]),
                "A pending 'switchover' operation should have been created"
            );
            assert_eq!(
                npu_ha_scope_state.switchover_state.as_deref(),
                Some("pending_approval"),
                "Switchover state should be 'pending_approval'"
            );
            assert!(
                npu_ha_scope_state.switchover_id.is_some(),
                "Switchover ID should be set"
            );

            let switchover_op_id = npu_ha_scope_state
                .pending_operation_ids
                .as_mut()
                .unwrap()
                .pop()
                .unwrap();

            // ============================================================
            // Phase 3: Upstream approves the switchover → Standby transitions
            // to SwitchingToActive and sends SwitchoverRequest(Syn) to peer
            // ============================================================
            #[rustfmt::skip]
            let commands = [
                // Approve the pending switchover operation
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"4","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":["{switchover_op_id}"]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },
                // Expect DPU DASH_HA_SCOPE_TABLE update with switching_to_active role
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "4",
                            "ha_role": "switching_to_active",
                            "ha_term": "1",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },
                // Expect SwitchoverRequest(Syn) sent to the peer
                recv! { key: SwitchoverRequest::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "flag": "Syn" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "switchover_id" },
                // Expect HaScopeActorState: standby -> switching_to_active
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::SwitchingToActive.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::SwitchingToActive.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify state is SwitchingToActive and switchover is in_progress
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::SwitchingToActive.as_str_name())
            );
            assert_eq!(
                npu_ha_scope_state.switchover_state.as_deref(),
                Some("in_progress"),
                "Switchover state should be 'in_progress' after approval"
            );

            // ============================================================
            // Phase 4: Peer accepts and transitions to SwitchingToStandby.
            // When peer state is SwitchingToStandby, local transitions
            // SwitchingToActive → Active (switchover complete).
            // ============================================================
            #[rustfmt::skip]
            let commands = [
                // Mock the peer accepting the switchover (Fin response)
                send! { key: SwitchoverRequest::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id, "switchover_id": "", "flag": "Fin" } },
                // Mock peer state change to SwitchingToStandby
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::SwitchingToStandby.as_str_name(), "term": "1", "vdpu_id": "", "peer_vdpu_id": "" } },
                // Expect DPU DASH_HA_SCOPE_TABLE update with active role and new term
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "4",
                            "ha_role": "active",
                            "ha_term": "2",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },
                // Expect HaScopeActorState: switching_to_active -> active
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Active.as_str_name(), "term": "2", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Active.as_str_name(), "term": "2", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify switchover completed successfully
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Active.as_str_name()),
                "Node should now be Active after planned switchover"
            );
            assert_eq!(
                npu_ha_scope_state.local_target_term.as_deref(),
                Some("2"),
                "Term should be incremented after switchover to active"
            );
            assert_eq!(
                npu_ha_scope_state.switchover_state.as_deref(),
                Some("completed"),
                "Switchover state should be 'completed'"
            );
            assert!(
                npu_ha_scope_state.switchover_end_time_in_ms.is_some(),
                "Switchover end time should be set"
            );

            // ============================================================
            // Phase 5: Cleanup — shutdown the actor
            // Active node uses disabled=true for forced shutdown
            // ============================================================
            let mut dpu_ha_state_state = make_dpu_ha_scope_state(HaRole::Dead.as_str_name());
            dpu_ha_state_state.ha_term = Some("2".to_string());

            #[rustfmt::skip]
            let commands = [
                // Set the disabled to true (Active node forced shutdown)
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"5","disabled":true,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },
                // Expect DPU update with Dead role
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "5",
                            "ha_role": "dead",
                            "ha_term": "2",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },
                // Expect HaScopeActorState: Active -> Dead
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Dead.as_str_name(), "term": "2", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Dead.as_str_name(), "term": "2", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                // Delete the HA scope config entry
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Del",
                        "field_values": {"json": format!(r#"{{"version":"5","disabled":true,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },
                chkdb! { type: NpuDashHaScopeState, key: &scope_id_in_state, nonexist },
                recv! { key: &ha_set_id, data: { "key": &ha_set_id, "operation": "Del", "field_values": {} }, addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge()) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &scope_id), data: { "active": false }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::HaSetState, &scope_id), data: { "active": false }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;
            if tokio::time::timeout(Duration::from_secs(5), handle).await.is_err() {
                panic!("timeout waiting for actor to terminate");
            }
        }

        /// Tests that an Active HA scope actor detects local DPU down (vDPU goes down)
        /// and transitions to SwitchingToStandalone, sends DPURequestEnterStandalone to the peer,
        /// and upon receiving "EnterStandby" reply, transitions to Standby (peer becomes standalone).
        ///
        /// Flow:
        /// 1. Launch the actor and bring it to Active state
        /// 2. Send a vDPU state update with up=false → LocalFailure event
        /// 3. Active → SwitchingToStandalone (sends DPURequestEnterStandalone to peer with local_dpu_up=false)
        /// 4. Peer replies "EnterStandby" → SwitchingToStandalone → Standby
        #[tokio::test]
        async fn ha_scope_npu_active_local_dpu_down_to_standby() {
            sonic_common::log::init_logger_for_test();
            let _redis = Redis::start_config_db();
            test::setup_remote_dpu_in_db(1, 0);
            let runtime = test::create_actor_runtime(12, "10.0.12.0", "10:0:12::").await;
            test::setup_mock_swbusd_resolve_peer_sp(&runtime.get_swbus_edge());

            let (ha_set_id, ha_set_obj) = make_dpu_scope_ha_set_obj(12, 0);
            let dpu_mon = make_dpu_pmon_state(true);
            let bfd_state = make_dpu_bfd_state(Vec::new(), Vec::new());
            let dpu0 = make_local_dpu_actor_state(0, 0, true, Some(dpu_mon), Some(bfd_state));
            let dpu1 = make_remote_dpu_actor_state(1, 0);
            let (vdpu0_id, vdpu0_state_obj) = make_vdpu_actor_state(true, &dpu0);
            let (vdpu1_id, _vdpu1_state_obj) = make_vdpu_actor_state(true, &dpu1);

            let scope_id = format!("{vdpu0_id}:{ha_set_id}");
            let scope_id_in_state = format!("{vdpu0_id}|{ha_set_id}");
            let peer_scope_id = format!("{vdpu1_id}:{ha_set_id}");

            let ha_scope_actor = HaScopeActor::new(scope_id.clone()).unwrap();
            let _handle = runtime.spawn(ha_scope_actor, HaScopeActor::name(), &scope_id);

            // ============================================================
            // Phase 1: Launch to Active (same as other tests)
            // ============================================================
            #[rustfmt::skip]
            let commands = [
                // Init HA Scope Config with owner as Switch and desired state Active
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"1","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                // Expect initial registration messages
                recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &scope_id), data: { "active": true }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::HaSetState, &scope_id), data: { "active": true }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Unspecified.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": "" }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock initial DPU & HA set updates
                send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state_obj, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                send! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": true, "ha_set": &ha_set_obj, "vdpu_ids": vec![vdpu0_id.clone(), vdpu1_id.clone()], "pinned_vdpu_bfd_probe_states": vec!["".to_string()] }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

                // A PeerHeartbeat should be triggered
                recv! { key: PeerHeartbeat::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },
                // Mock the init HaScopeActorState from the peer HA scope
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::Dead.as_str_name(), "term": "0", "vdpu_id": "", "peer_vdpu_id": "" } },
                // Expect HaScopeActorState: dead -> connecting
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connecting.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connecting.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                // Expect a VoteRequest to be sent
                recv!( key: VoteRequest::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "term": "0", "state": HaState::Connecting.as_str_name(), "desired_state": DesiredHaState::Active.as_str_name() }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) ),
                // Expect HaScopeActorState: connecting -> connected
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connected.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connected.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock a VoteReply: BecomeActive
                send! { key: VoteReply::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id, "response": "BecomeActive" } },
                // Expect HaScopeActorState: connected -> initializing_to_active
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::InitializingToActive.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::InitializingToActive.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                // Mock the HaScopeActorState from the peer HA scope
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::InitializingToStandby.as_str_name(), "term": "0", "vdpu_id": "", "peer_vdpu_id": "" } },
                // Expect HaScopeActorState: initializing_to_active -> pending_active_activation
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::PendingActiveActivation.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::PendingActiveActivation.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];
            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Approve pending activation to reach Active state
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let mut npu_ha_scope_state: NpuDashHaScopeState =
                swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            let op_id = npu_ha_scope_state
                .pending_operation_ids
                .as_mut()
                .unwrap()
                .pop()
                .unwrap();

            #[rustfmt::skip]
            let commands = [
                // New HA scope config containing approved op_id
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"2","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":["{op_id}"]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                // Expect a DPU DASH_HA_SCOPE_TABLE update
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "2",
                            "ha_role": "active",
                            "ha_term": "1",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },

                // Expect a bulkSyncCompleted message
                recv! { key: BulkSyncUpdate::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "finished": true }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },

                // Expect HaScopeActorState: pending_active_activation -> active
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Active.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Active.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];
            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify we are in Active state
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Active.as_str_name())
            );
            assert_eq!(npu_ha_scope_state.local_target_term.as_deref(), Some("1"));

            // ============================================================
            // Phase 2: Local DPU goes down → Active transitions to
            // SwitchingToStandalone and sends DPURequestEnterStandalone
            // to peer with local_dpu_up=false
            // ============================================================

            // Create a vDPU state with up=false (local DPU down)
            let (_, vdpu0_down_state_obj) = make_vdpu_actor_state(false, &dpu0);

            #[rustfmt::skip]
            let commands = [
                // Send vDPU state update with up=false → triggers LocalFailure event
                send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_down_state_obj, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },

                // Side effect: DPURequestEnterStandalone sent to peer with local_dpu_up=false
                recv! { key: DPURequestEnterStandalone::msg_key(&scope_id), data: { "dp_channel_is_alive": true, "local_dpu_up": false, "pinned_vdpu_bfd_probe_state": "", "inline_sync_packet_drops": false }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },

                // Expect HaScopeActorState: Active -> SwitchingToStandalone
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::SwitchingToStandalone.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::SwitchingToStandalone.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];
            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify the state is now SwitchingToStandalone
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::SwitchingToStandalone.as_str_name())
            );
            assert_eq!(
                npu_ha_scope_state.local_ha_state_last_updated_reason.as_deref(),
                Some("local failure while active")
            );

            // ============================================================
            // Phase 3: Peer replies "EnterStandby" → SwitchingToStandalone → Standby
            // (peer becomes standalone, this node enters standby)
            // ============================================================
            #[rustfmt::skip]
            let commands = [
                // Peer replies EnterStandby (peer will become standalone)
                send! { key: DPURequestEnterStandaloneReply::msg_key(&peer_scope_id), data: { "response": "EnterStandby" } },

                // Expect DPU DASH_HA_SCOPE_TABLE update with standby role (term stays the same)
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "2",
                            "ha_role": "standby",
                            "ha_term": "1",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },

                // Expect HaScopeActorState: SwitchingToStandalone -> Standby
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standby.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standby.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];
            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify final state is Standby
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Standby.as_str_name()),
                "Node should be in Standby state after local DPU down and peer becoming standalone"
            );
            assert_eq!(
                npu_ha_scope_state.local_target_term.as_deref(),
                Some("1"),
                "Term should remain unchanged when entering Standby from SwitchingToStandalone"
            );
        }

        /// Tests that a Standby HA scope actor detects peer DPU down via PeerLost event
        /// (HA set goes down) and transitions to Standalone.
        ///
        /// Flow:
        /// 1. Launch the actor and bring it to Standby state
        /// 2. Send an HaSetActorState update with up=false → PeerLost event
        /// 3. Standby → SwitchingToStandalone (self-notification EnterStandalone, no peer negotiation)
        /// 4. SwitchingToStandalone → Standalone (term incremented)
        #[tokio::test]
        async fn ha_scope_npu_standby_to_standalone_on_peer_lost() {
            sonic_common::log::init_logger_for_test();
            let _redis = Redis::start_config_db();
            test::setup_remote_dpu_in_db(1, 0);
            let runtime = test::create_actor_runtime(14, "10.0.14.0", "10:0:14::").await;
            test::setup_mock_swbusd_resolve_peer_sp(&runtime.get_swbus_edge());

            let (ha_set_id, ha_set_obj) = make_dpu_scope_ha_set_obj(14, 0);
            let dpu_mon = make_dpu_pmon_state(true);
            let bfd_state = make_dpu_bfd_state(Vec::new(), Vec::new());
            let dpu0 = make_local_dpu_actor_state(0, 0, true, Some(dpu_mon), Some(bfd_state));
            let dpu1 = make_remote_dpu_actor_state(1, 0);
            let (vdpu0_id, vdpu0_state_obj) = make_vdpu_actor_state(true, &dpu0);
            let (vdpu1_id, _vdpu1_state_obj) = make_vdpu_actor_state(true, &dpu1);

            let scope_id = format!("{vdpu0_id}:{ha_set_id}");
            let scope_id_in_state = format!("{vdpu0_id}|{ha_set_id}");
            let peer_scope_id = format!("{vdpu1_id}:{ha_set_id}");

            let ha_scope_actor = HaScopeActor::new(scope_id.clone()).unwrap();
            let _handle = runtime.spawn(ha_scope_actor, HaScopeActor::name(), &scope_id);

            // ============================================================
            // Phase 1: Launch to Standby
            // ============================================================
            #[rustfmt::skip]
            let commands = [
                // Init HA Scope Config with owner as Switch and desired_ha_state = Unspecified (will become standby)
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"1","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Unspecified as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                // Expect initial registration messages
                recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &scope_id), data: { "active": true }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::HaSetState, &scope_id), data: { "active": true }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Unspecified.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": "" }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock initial DPU & HA set updates
                send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state_obj, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                send! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": true, "ha_set": &ha_set_obj, "vdpu_ids": vec![vdpu0_id.clone(), vdpu1_id.clone()], "pinned_vdpu_bfd_probe_states": vec!["".to_string()] }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

                // A PeerHeartbeat should be triggered
                recv! { key: PeerHeartbeat::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },
                // Expect HaScopeActorState: dead -> connecting
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connecting.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connecting.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock the init HaScopeActorState from the peer HA scope
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::Dead.as_str_name(), "term": "0", "vdpu_id": "", "peer_vdpu_id": "" } },
                // Expect a VoteRequest to be sent
                recv!( key: VoteRequest::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "term": "0", "state": HaState::Connecting.as_str_name(), "desired_state": DesiredHaState::Unspecified.as_str_name() }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) ),
                // Expect HaScopeActorState: connecting -> connected
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connected.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connected.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock a VoteReply: BecomeStandby
                send! { key: VoteReply::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id, "response": "BecomeStandby" } },
                // Expect DPU DASH_HA_SCOPE_TABLE update with standby role
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "1",
                            "ha_role": "standby",
                            "ha_term": "0",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },
                // Expect HaScopeActorState: connected -> initializing_to_standby
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::InitializingToStandby.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::InitializingToStandby.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock a BulkSyncCompleted message from the peer
                send! { key: BulkSyncUpdate::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id, "finished": true }},
                // Expect HaScopeActorState: initializing_to_standby -> pending_standby_activation
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::PendingStandbyActivation.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::PendingStandbyActivation.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                // Mock the HaScopeActorState from the peer with new term (peer is now Active)
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::Active.as_str_name(), "term": "1", "vdpu_id": "", "peer_vdpu_id": "" } },
            ];
            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Approve pending standby activation
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let mut npu_ha_scope_state: NpuDashHaScopeState =
                swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            let op_id = npu_ha_scope_state
                .pending_operation_ids
                .as_mut()
                .unwrap()
                .pop()
                .unwrap();

            #[rustfmt::skip]
            let commands = [
                // Approve the pending standby activation
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"2","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":["{op_id}"]}}"#, DesiredHaState::Unspecified as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },
                // Expect DPU update for standby term
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "2",
                            "ha_role": "standby",
                            "ha_term": "1",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },
                // Expect HaScopeActorState: pending_standby_activation -> standby
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standby.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standby.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];
            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify we are in Standby state
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Standby.as_str_name())
            );
            assert_eq!(npu_ha_scope_state.local_target_term.as_deref(), Some("1"));

            // ============================================================
            // Phase 2: Peer DPU goes down (HA set down) → Standby detects
            // PeerLost → SwitchingToStandalone → Standalone
            // ============================================================
            #[rustfmt::skip]
            let commands = [
                // Send HaSetActorState with up=false → triggers PeerLost event
                send! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": false, "ha_set": &ha_set_obj, "vdpu_ids": vec![vdpu0_id.clone(), vdpu1_id.clone()], "pinned_vdpu_bfd_probe_states": vec!["".to_string()] }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

                // Expect HaScopeActorState: Standby -> SwitchingToStandalone
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::SwitchingToStandalone.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::SwitchingToStandalone.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // EnterStandalone self-notification is processed immediately (PeerLost path):
                // Expect DPU DASH_HA_SCOPE_TABLE update with standalone role and incremented term
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "2",
                            "ha_role": "standalone",
                            "ha_term": "2",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },

                // Expect HaScopeActorState: SwitchingToStandalone -> Standalone
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standalone.as_str_name(), "term": "2", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standalone.as_str_name(), "term": "2", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];
            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify final state is Standalone with incremented term
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Standalone.as_str_name()),
                "Node should be in Standalone state after peer DPU down"
            );
            assert_eq!(
                npu_ha_scope_state.local_target_term.as_deref(),
                Some("2"),
                "Term should be incremented after entering Standalone"
            );
            assert_eq!(
                npu_ha_scope_state.local_ha_state_last_updated_reason.as_deref(),
                Some("won the standalone selection"),
                "Reason should reflect standalone selection"
            );
        }

        /// Tests that an Active HA scope actor detects high inline sync packet drops
        /// from the COUNTERS table and transitions to SwitchingToStandalone.
        ///
        /// Flow:
        /// 1. Launch the actor and bring it to Active state (same as ha_scope_npu_launch_to_active_then_down)
        /// 2. Send a COUNTERS_ENI_NAME_MAP update to register counter object IDs
        /// 3. Send a first COUNTERS update (baseline) for one of the tracked OIDs
        /// 4. Send a second COUNTERS update with tx_pkts - rx_pkts diff > threshold (31 > 30)
        /// 5. Verify the actor transitions from Active to SwitchingToStandalone
        #[tokio::test]
        async fn ha_scope_npu_active_high_inline_sync_drops_to_standalone() {
            sonic_common::log::init_logger_for_test();
            let _redis = Redis::start_config_db();
            test::setup_remote_dpu_in_db(1, 0);
            let runtime = test::create_actor_runtime(10, "10.0.10.0", "10:0:10::").await;
            test::setup_mock_swbusd_resolve_peer_sp(&runtime.get_swbus_edge());

            let (ha_set_id, ha_set_obj) = make_dpu_scope_ha_set_obj(10, 0);
            let dpu_mon = make_dpu_pmon_state(true);
            let bfd_state = make_dpu_bfd_state(Vec::new(), Vec::new());
            let dpu0 = make_local_dpu_actor_state(0, 0, true, Some(dpu_mon), Some(bfd_state));
            let dpu1 = make_remote_dpu_actor_state(1, 0);
            let (vdpu0_id, vdpu0_state_obj) = make_vdpu_actor_state(true, &dpu0);
            let (vdpu1_id, _vdpu1_state_obj) = make_vdpu_actor_state(true, &dpu1);

            let scope_id = format!("{vdpu0_id}:{ha_set_id}");
            let scope_id_in_state = format!("{vdpu0_id}|{ha_set_id}");
            let peer_scope_id = format!("{vdpu1_id}:{ha_set_id}");

            let ha_scope_actor = HaScopeActor::new(scope_id.clone()).unwrap();
            let _handle = runtime.spawn(ha_scope_actor, HaScopeActor::name(), &scope_id);

            // ============================================================
            // Phase 1: Launch to Active (same sequence as the existing test)
            // ============================================================
            #[rustfmt::skip]
            let commands = [
                // Init HA Scope Config with owner as Switch, desired_ha_state = Active
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"1","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },
                // Expect initial registration messages
                recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &scope_id), data: { "active": true }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::HaSetState, &scope_id), data: { "active": true }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Unspecified.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": "" }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                // Mock initial DPU & HA set updates
                send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state_obj, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                send! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": true, "ha_set": &ha_set_obj, "vdpu_ids": vec![vdpu0_id.clone(), vdpu1_id.clone()], "pinned_vdpu_bfd_probe_states": vec!["".to_string()] }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },
                // PeerHeartbeat
                recv! { key: PeerHeartbeat::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },
                // Peer sends HaScopeActorState
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::Dead.as_str_name(), "term": "0", "vdpu_id": "", "peer_vdpu_id": "" } },
                // Expect: Dead -> Connecting
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connecting.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connecting.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                // VoteRequest
                recv!( key: VoteRequest::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "term": "0", "state": HaState::Connecting.as_str_name(), "desired_state": DesiredHaState::Active.as_str_name() }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) ),
                // Expect: Connecting -> Connected
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connected.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connected.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                // VoteReply: BecomeActive
                send! { key: VoteReply::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id, "response": "BecomeActive" } },
                // Expect: Connected -> InitializingToActive
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::InitializingToActive.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::InitializingToActive.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                // Peer moves to InitializingToStandby
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::InitializingToStandby.as_str_name(), "term": "0", "vdpu_id": "", "peer_vdpu_id": "" } },
                // Expect: InitializingToActive -> PendingActiveActivation
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::PendingActiveActivation.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::PendingActiveActivation.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];
            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Get the pending operation ID for approval
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let mut npu_ha_scope_state: NpuDashHaScopeState =
                swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            let op_id = npu_ha_scope_state
                .pending_operation_ids
                .as_mut()
                .unwrap()
                .pop()
                .unwrap();

            // Approve the pending operation to transition to Active
            #[rustfmt::skip]
            let commands = [
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"2","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":["{op_id}"]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id, "operation": "Set",
                        "field_values": { "version": "2", "ha_role": "active", "ha_term": "1", "ha_set_id": &ha_set_id },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge()) },
                recv! { key: BulkSyncUpdate::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "finished": true }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },
                // PendingActiveActivation -> Active
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Active.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Active.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];
            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify we're in Active state
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Active.as_str_name())
            );

            // ============================================================
            // Phase 2: Send counter updates to trigger HighInlineSyncDrops
            // ============================================================
            let eni_oid = "oid:0x03000000000001";

            // Step 2a: Send COUNTERS_ENI_NAME_MAP to register the ENI OID
            #[rustfmt::skip]
            let commands = [
                send! { key: CountersEniNameMapTable::table_name(), data: {
                        "key": CountersEniNameMapTable::table_name(), "operation": "Set",
                        "field_values": { "eni1": eni_oid }
                        }},
            ];
            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Step 2b: Send first COUNTERS update (baseline)
            let counters_key = format!("{}|{}", CountersTable::table_name(), eni_oid);
            #[rustfmt::skip]
            let commands = [
                send! { key: &counters_key, data: {
                        "key": eni_oid, "operation": "Set",
                        "field_values": {
                            ENI_INLINE_FLOW_SYNC_TX_PKTS: "100",
                            ENI_INLINE_FLOW_SYNC_RX_PKTS: "100"
                        }
                        }},
            ];
            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Step 2c: Send second COUNTERS update with high drops (tx=200, rx=169 → diff 31 > threshold 30)
            #[rustfmt::skip]
            let commands = [
                send! { key: &counters_key, data: {
                        "key": eni_oid, "operation": "Set",
                        "field_values": {
                            ENI_INLINE_FLOW_SYNC_TX_PKTS: "200",
                            ENI_INLINE_FLOW_SYNC_RX_PKTS: "169"
                        }
                        }},
                // Side effect runs first: DPURequestEnterStandalone sent to peer
                recv! { key: DPURequestEnterStandalone::msg_key(&scope_id), data: { "dp_channel_is_alive": true, "local_dpu_up": true, "pinned_vdpu_bfd_probe_state": "", "inline_sync_packet_drops": true }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },
                // Then broadcast: Active -> SwitchingToStandalone
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::SwitchingToStandalone.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::SwitchingToStandalone.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];
            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify the state is now SwitchingToStandalone
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::SwitchingToStandalone.as_str_name())
            );
            assert_eq!(
                npu_ha_scope_state.local_ha_state_last_updated_reason.as_deref(),
                Some("high inline-sync packet drops")
            );

            // ============================================================
            // Phase 3: Peer replies EnterStandalone -> Standalone
            // ============================================================
            #[rustfmt::skip]
            let commands = [
                // Peer allows entering standalone
                send! { key: DPURequestEnterStandaloneReply::msg_key(&peer_scope_id), data: { "response": "EnterStandalone" } },
                // Expect DPU DASH_HA_SCOPE_TABLE update with standalone role
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id, "operation": "Set",
                        "field_values": { "version": "2", "ha_role": "standalone", "ha_term": "2", "ha_set_id": &ha_set_id },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge()) },
                // Expect: SwitchingToStandalone -> Standalone
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standalone.as_str_name(), "term": "2", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standalone.as_str_name(), "term": "2", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];
            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify the final state is Standalone
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Standalone.as_str_name())
            );
        }

        /// Verify the scenario where an Active DPU has its standby peer shutdown and then
        /// a new standby peer (different vDPU) is paired to replace the old one.
        ///
        /// Flow:
        ///   Phase 1: Launch to Active (with peer vdpu1-0)
        ///   Phase 2: Peer vdpu1-0 sends ShutdownRequest → Active → SwitchingToStandalone → Standalone
        ///   Phase 3: HA set update arrives with new peer vdpu3-0 → new peer connects → Standalone → Active
        ///   Phase 4: Cleanup
        #[tokio::test]
        async fn ha_scope_npu_active_standby_shutdown_then_repair_with_new_peer() {
            sonic_common::log::init_logger_for_test();
            let _redis = Redis::start_config_db();
            test::setup_remote_dpu_in_db(1, 0);
            test::setup_remote_dpu_in_db(3, 0); // new peer
            let runtime = test::create_actor_runtime(16, "10.0.16.0", "10:0:16::").await;
            test::setup_mock_swbusd_resolve_peer_sp(&runtime.get_swbus_edge());

            let (ha_set_id, ha_set_obj) = make_dpu_scope_ha_set_obj(16, 0);
            let dpu_mon = make_dpu_pmon_state(true);
            let bfd_state = make_dpu_bfd_state(Vec::new(), Vec::new());
            let dpu0 = make_local_dpu_actor_state(0, 0, true, Some(dpu_mon), Some(bfd_state));
            let dpu1 = make_remote_dpu_actor_state(1, 0);
            let dpu3 = make_remote_dpu_actor_state(3, 0); // new peer DPU
            let (vdpu0_id, vdpu0_state_obj) = make_vdpu_actor_state(true, &dpu0);
            let (vdpu1_id, _vdpu1_state_obj) = make_vdpu_actor_state(true, &dpu1);
            let (vdpu3_id, _vdpu3_state_obj) = make_vdpu_actor_state(true, &dpu3); // new peer vDPU

            let scope_id = format!("{vdpu0_id}:{ha_set_id}");
            let scope_id_in_state = format!("{vdpu0_id}|{ha_set_id}");
            let peer_scope_id = format!("{vdpu1_id}:{ha_set_id}");
            let new_peer_scope_id = format!("{vdpu3_id}:{ha_set_id}");

            let ha_scope_actor = HaScopeActor::new(scope_id.clone()).unwrap();
            let handle = runtime.spawn(ha_scope_actor, HaScopeActor::name(), &scope_id);

            // ============================================================
            // Phase 1: Launch to Active (with original peer vdpu1-0)
            // ============================================================
            #[rustfmt::skip]
            let commands = [
                // Init HA Scope Config with owner as Switch and desired state Active
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"1","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                // Expect initial registration messages
                recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &scope_id), data: { "active": true }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::HaSetState, &scope_id), data: { "active": true }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Unspecified.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": "" }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock initial DPU & HA set updates
                send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state_obj, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                send! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": true, "ha_set": &ha_set_obj, "vdpu_ids": vec![vdpu0_id.clone(), vdpu1_id.clone()], "pinned_vdpu_bfd_probe_states": vec!["".to_string()] }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

                // A PeerHeartbeat should be triggered
                recv! { key: PeerHeartbeat::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },
                // Mock the init HaScopeActorState from the peer HA scope
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::Dead.as_str_name(), "term": "0", "vdpu_id": "", "peer_vdpu_id": "" } },
                // Expect HaScopeActorState: dead -> connecting
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connecting.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connecting.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                // Expect a VoteRequest to be sent
                recv!( key: VoteRequest::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "term": "0", "state": HaState::Connecting.as_str_name(), "desired_state": DesiredHaState::Active.as_str_name() }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) ),
                // Expect HaScopeActorState: connecting -> connected
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connected.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Connected.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                // Mock a VoteReply: BecomeActive
                send! { key: VoteReply::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id, "response": "BecomeActive" } },
                // Expect HaScopeActorState: connected -> initializing_to_active
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::InitializingToActive.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::InitializingToActive.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
                // Mock the HaScopeActorState from the peer HA scope
                send! { key: HaScopeActorState::msg_key(&peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::InitializingToStandby.as_str_name(), "term": "0", "vdpu_id": "", "peer_vdpu_id": "" } },
                // Expect HaScopeActorState: initializing_to_active -> pending_active_activation
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::PendingActiveActivation.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::PendingActiveActivation.as_str_name(), "term": "0", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Approve pending activation to reach Active state
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let mut npu_ha_scope_state: NpuDashHaScopeState =
                swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            let op_id = npu_ha_scope_state
                .pending_operation_ids
                .as_mut()
                .unwrap()
                .pop()
                .unwrap();

            #[rustfmt::skip]
            let commands = [
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"2","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":["{op_id}"]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "2",
                            "ha_role": "active",
                            "ha_term": "1",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },

                recv! { key: BulkSyncUpdate::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "finished": true }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },

                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Active.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Active.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify we are in Active state
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Active.as_str_name())
            );
            assert_eq!(npu_ha_scope_state.local_target_term.as_deref(), Some("1"));

            // ============================================================
            // Phase 2: Standby peer (vdpu1-0) sends ShutdownRequest →
            // Active → SwitchingToStandalone → Standalone
            // ============================================================
            #[rustfmt::skip]
            let commands = [
                send! { key: ShutdownRequest::msg_key(&peer_scope_id), data: { "reason": "planned shutdown" } },

                recv! { key: ShutdownReply::msg_key(&scope_id), data: { "response": "accepted" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },

                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::SwitchingToStandalone.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::SwitchingToStandalone.as_str_name(), "term": "1", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "2",
                            "ha_role": "standalone",
                            "ha_term": "2",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },

                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standalone.as_str_name(), "term": "2", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standalone.as_str_name(), "term": "2", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify Standalone state
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Standalone.as_str_name()),
                "Node should be in Standalone state after standby peer shutdown"
            );
            assert_eq!(
                npu_ha_scope_state.local_target_term.as_deref(),
                Some("2"),
                "Term should be incremented after entering Standalone"
            );

            // ============================================================
            // Phase 3: New standby peer (vdpu3-0) arrives via HA set update.
            // This triggers the new peer vDPU inflight logic:
            //   - peer_vdpu_id changes from vdpu1-0 to vdpu3-0
            //   - peer_sp is re-resolved
            // Then the new peer connects and Standalone → Active
            // ============================================================
            {
                use swbus_edge::simple_client::{MessageBody, OutgoingMessage, SimpleSwbusEdgeClient};
                use swbus_edge::swbus_proto::swbus::SwbusErrorCode;
                let client = SimpleSwbusEdgeClient::new(
                    runtime.get_swbus_edge(),
                    crate::common_bridge_sp::<DashFlowSyncSessionTable>(&runtime.get_swbus_edge()),
                    true,
                    false,
                );
                let aut_sp = runtime.sp(HaScopeActor::name(), &scope_id);
                tokio::spawn(async move {
                    while let Some(msg) = client.recv().await {
                        if let MessageBody::Request { .. } = &msg.body {
                            let _ = client
                                .send(OutgoingMessage {
                                    destination: aut_sp.clone(),
                                    body: MessageBody::Response {
                                        request_id: msg.id,
                                        error_code: SwbusErrorCode::Ok,
                                        error_message: "".to_string(),
                                        response_body: None,
                                    },
                                })
                                .await;
                        }
                    }
                });
            }
            #[rustfmt::skip]
            let commands = [
                // HA set update arrives with the NEW peer vdpu3-0 (replacing vdpu1-0)
                // This is not the first HA set update, so it triggers the new peer inflight logic
                send! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": true, "ha_set": &ha_set_obj, "vdpu_ids": vec![vdpu0_id.clone(), vdpu3_id.clone()], "pinned_vdpu_bfd_probe_states": vec!["".to_string()] }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

                // New peer sends PeerHeartbeat — Standalone node responds with its state
                // Note: now messages go to/from the new peer scope ID
                send! { key: PeerHeartbeat::msg_key(&new_peer_scope_id), data: { "dst_actor_id": &scope_id }, addr: runtime.sp(HaScopeActor::name(), &new_peer_scope_id) },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Standalone.as_str_name(), "term": "2", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu3_id }, addr: runtime.sp(HaScopeActor::name(), &new_peer_scope_id), exclude: "timestamp" },

                // New peer sends VoteRequest (term=0, state=Connecting)
                // Standalone node has term=2 > peer term=0, so it replies BecomeStandby
                send! { key: VoteRequest::msg_key(&new_peer_scope_id), data: { "dst_actor_id": &scope_id, "term": "0", "state": HaState::Connecting.as_str_name(), "desired_state": DesiredHaState::Unspecified.as_str_name() }, addr: runtime.sp(HaScopeActor::name(), &new_peer_scope_id) },
                recv! { key: VoteReply::msg_key(&scope_id), data: { "dst_actor_id": &new_peer_scope_id, "response": "BecomeStandby" }, addr: runtime.sp(HaScopeActor::name(), &new_peer_scope_id) },

                // New peer transitions to InitializingToStandby and broadcasts state
                // This triggers PeerStateChanged on the Standalone node → transitions to Active
                send! { key: HaScopeActorState::msg_key(&new_peer_scope_id), data: { "timestamp": 0, "owner": 0, "new_state": HaState::InitializingToStandby.as_str_name(), "term": "0", "vdpu_id": "", "peer_vdpu_id": "" } },

                // Expect DPU HA scope table update with Active role and term=3
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "2",
                            "ha_role": "active",
                            "ha_term": "3",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },

                // Expect HaScopeActorState: Standalone -> Active with term=3
                // Note: peer_vdpu_id is now the NEW peer vdpu3_id
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Active.as_str_name(), "term": "3", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu3_id }, addr: runtime.sp(HaScopeActor::name(), &new_peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Active.as_str_name(), "term": "3", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu3_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;

            // Verify state is Active with term=3 and the new peer
            let db = crate::db_for_table::<NpuDashHaScopeState>().await.unwrap();
            let table = Table::new(db, NpuDashHaScopeState::table_name()).unwrap();
            let npu_ha_scope_state: NpuDashHaScopeState = swss_serde::from_table(&table, &scope_id_in_state).unwrap();
            assert_eq!(
                npu_ha_scope_state.local_ha_state.as_deref(),
                Some(HaState::Active.as_str_name()),
                "Standalone node should transition to Active when new peer joins"
            );
            assert_eq!(
                npu_ha_scope_state.local_target_term.as_deref(),
                Some("3"),
                "Term should be incremented to 3 after Standalone -> Active"
            );
            assert_eq!(
                npu_ha_scope_state.peer_ha_state.as_deref(),
                Some(HaState::InitializingToStandby.as_str_name()),
                "New peer should be in InitializingToStandby state"
            );

            // ============================================================
            // Phase 4: Cleanup — force shutdown the actor
            // ============================================================
            #[rustfmt::skip]
            let commands = [
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"3","disabled":true,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "3",
                            "ha_role": "dead",
                            "ha_term": "3",
                            "ha_set_id": &ha_set_id
                        },
                        },
                        addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge())
                    },

                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Dead.as_str_name(), "term": "3", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu3_id }, addr: runtime.sp(HaScopeActor::name(), &new_peer_scope_id), exclude: "timestamp" },
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "new_state": HaState::Dead.as_str_name(), "term": "3", "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu3_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "timestamp" },

                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Del",
                        "field_values": {"json": format!(r#"{{"version":"3","disabled":true,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Active as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                chkdb! { type: NpuDashHaScopeState, key: &scope_id_in_state, nonexist },
                recv! { key: &ha_set_id, data: { "key": &ha_set_id, "operation": "Del", "field_values": {} }, addr: crate::common_bridge_sp::<DashHaScopeTable>(&runtime.get_swbus_edge()) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::VDPUState, &scope_id), data: { "active": false }, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                recv! { key: ActorRegistration::msg_key(RegistrationType::HaSetState, &scope_id), data: { "active": false }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },
            ];

            test::run_commands(&runtime, runtime.sp(HaScopeActor::name(), &scope_id), &commands).await;
            if tokio::time::timeout(Duration::from_secs(5), handle).await.is_err() {
                panic!("timeout waiting for actor to terminate");
            }
        }
    }
}
