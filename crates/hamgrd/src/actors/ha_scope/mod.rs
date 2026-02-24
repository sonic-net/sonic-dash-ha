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
        db_structs::{now_in_millis, DashHaScopeTable, DpuDashHaScopeState, NpuDashHaScopeState},
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
                recv!( key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Dpu as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": "" }, addr: runtime.sp(HaSetActor::name(), &ha_set_id)),

                // Send vDPU state to actor
                send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state_obj, addr: runtime.sp("vdpu", &vdpu0_id) },

                // Send ha-set state to actor
                send! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": true, "ha_set": &ha_set_obj, "vdpu_ids": vec![vdpu0_id.clone(), vdpu1_id.clone()] }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

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
            let runtime = test::create_actor_runtime(2, "10.0.2.0", "10:0:2::").await;

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
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": "" }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

                // Mock initial DPU & HA set updates
                send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state_obj, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                send! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": true, "ha_set": &ha_set_obj, "vdpu_ids": vec![vdpu0_id.clone(), vdpu1_id.clone()] }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

                // A PeerHeartbeat should be triggered
                recv! { key: PeerHeartbeat::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },
                // Mock the init HaStateChanged from the peer HA scope
                send! { key: HAStateChanged::msg_key(&peer_scope_id), data: { "prev_state": HaState::Dead.as_str_name(), "new_state": HaState::Dead.as_str_name(), "timestamp": now_in_millis(), "term": "0" } },
                // Expect a HaStateChanged: dead -> connecting
                recv! { key: HAStateChanged::msg_key(&scope_id), data: { "prev_state": HaState::Dead.as_str_name(), "new_state": HaState::Connecting.as_str_name(), "timestamp": now_in_millis(), "term": "0" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                // Expect one HaScopeActorState update: Connecting
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "ha_scope_state" },
                // Expect a VoteRequest to be sent
                recv!( key: VoteRequest::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "term": "0", "state": HaState::Connecting.as_str_name(), "desired_state": DesiredHaState::Active.as_str_name() }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) ),
                // Expect a HaStateChanged: connecting -> connected
                recv! { key: HAStateChanged::msg_key(&scope_id), data: { "prev_state": HaState::Connecting.as_str_name(), "new_state": HaState::Connected.as_str_name(), "timestamp": now_in_millis(), "term": "0" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                // Expect one HaScopeActorState update: connected
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "ha_scope_state" },

                // Mock a VoteReply
                send! { key: VoteReply::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id, "response": "BecomeActive" } },
                // Expect a HaStateChanged: connected -> initializing_to_active
                recv! { key: HAStateChanged::msg_key(&scope_id), data: { "prev_state": HaState::Connected.as_str_name(), "new_state": HaState::InitializingToActive.as_str_name(), "timestamp": now_in_millis(), "term": "0" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                // Expect one last HaScopeActorState update: Initializing_to_active
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "ha_scope_state" },
                // Mock the HaStateChanged from the peer HA scope
                send! { key: HAStateChanged::msg_key(&peer_scope_id), data: { "prev_state": HaState::Connected.as_str_name(), "new_state": HaState::InitializingToStandby.as_str_name(), "timestamp": now_in_millis(), "term": "0" } },
                // Expect a HaStateChanged: initializing_to_active -> pending_active_activation
                recv! { key: HAStateChanged::msg_key(&scope_id), data: { "prev_state": HaState::InitializingToActive.as_str_name(), "new_state": HaState::PendingActiveActivation.as_str_name(), "timestamp": now_in_millis(), "term": "0" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                // Expect one HaScopeActorState update: Pending_active_activation
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "ha_scope_state" },
                

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
                            "ha_role": HaRole::Active.as_str_name(),
                            "ha_term": "1",
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

                // Expect a bulkSyncCompleted message
                recv! { key: BulkSyncUpdate::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "finished": true }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },

                // Expect a HaStateChanged: pending_active_activation -> active
                recv! { key: HAStateChanged::msg_key(&scope_id), data: { "prev_state": HaState::PendingActiveActivation.as_str_name(), "new_state": HaState::Active.as_str_name(), "timestamp": now_in_millis(), "term": "1" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                // Expect one HaScopeActorState update: Active
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "ha_scope_state" },
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
            dpu_ha_state_state.ha_term = "1".to_string();

            // Shutdown
            #[rustfmt::skip]
            let commands = [
                // Set the desired statet to Dead
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"3","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Dead as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                // Expect a DPU DASH_HA_SCOPE_TABLE update
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "3",
                            "ha_role": HaRole::Dead.as_str_name(),
                            "ha_term": "1",
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

                // Expect a HaStateChanged: Active -> Destroying
                recv! { key: HAStateChanged::msg_key(&scope_id), data: { "prev_state": HaState::Active.as_str_name(), "new_state": HaState::Destroying.as_str_name(), "timestamp": now_in_millis(), "term": "1" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },

                // Expect the HaScopeActorState update: Destroying
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "ha_scope_state" },

                // Send DPU DASH_HA_SCOPE_STATE with ha_role = dead
                send! { key: DpuDashHaScopeState::table_name(), data: {"key": DpuDashHaScopeState::table_name(), "operation": "Set",
                        "field_values": serde_json::to_value(to_field_values(&dpu_ha_state_state).unwrap()).unwrap()
                        }},

                // Expect a HaStateChanged: Destroying -> Dead
                recv! { key: HAStateChanged::msg_key(&scope_id), data: { "prev_state": HaState::Destroying.as_str_name(), "new_state": HaState::Dead.as_str_name(), "timestamp": now_in_millis(), "term": "1" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },

                // Expect one last HaScopeActorState update: Dead
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "ha_scope_state" },

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
            let runtime = test::create_actor_runtime(4, "10.0.4.0", "10:0:4::").await;

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
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": "" }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

                // Mock initial DPU & HA set updates
                send! { key: VDpuActorState::msg_key(&vdpu0_id), data: vdpu0_state_obj, addr: runtime.sp(VDpuActor::name(), &vdpu0_id) },
                send! { key: HaSetActorState::msg_key(&ha_set_id), data: { "up": true, "ha_set": &ha_set_obj, "vdpu_ids": vec![vdpu0_id.clone(), vdpu1_id.clone()] }, addr: runtime.sp(HaSetActor::name(), &ha_set_id) },

                // A PeerHeartbeat should be triggered
                recv! { key: PeerHeartbeat::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) },
                // Expect a HaStateChanged: dead -> connecting
                recv! { key: HAStateChanged::msg_key(&scope_id), data: { "prev_state": HaState::Dead.as_str_name(), "new_state": HaState::Connecting.as_str_name(), "timestamp": now_in_millis(), "term": "0" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                // Expect one HaScopeActorState update: Connecting
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "ha_scope_state" },

                // Mock the init HaStateChanged from the peer HA scope
                send! { key: HAStateChanged::msg_key(&peer_scope_id), data: { "prev_state": HaState::Dead.as_str_name(), "new_state": HaState::Dead.as_str_name(), "timestamp": now_in_millis(), "term": "0" } },
                // Expect a VoteRequest to be sent
                recv!( key: VoteRequest::msg_key(&scope_id), data: { "dst_actor_id": &peer_scope_id, "term": "0", "state": HaState::Connecting.as_str_name(), "desired_state": DesiredHaState::Unspecified.as_str_name() }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id) ),
                // Expect a HaStateChanged: connecting -> connected
                recv! { key: HAStateChanged::msg_key(&scope_id), data: { "prev_state": HaState::Connecting.as_str_name(), "new_state": HaState::Connected.as_str_name(), "timestamp": now_in_millis(), "term": "0" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                // Expect one HaScopeActorState update: connected
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "ha_scope_state" },

                // Mock a VoteReply
                send! { key: VoteReply::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id, "response": "BecomeStandby" } },
                // Expect a DPU DASH_HA_SCOPE_TABLE update
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "1",
                            "ha_role": HaRole::Standby.as_str_name(),
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
                // Expect a HaStateChanged: connected -> initializing_to_standby
                recv! { key: HAStateChanged::msg_key(&scope_id), data: { "prev_state": HaState::Connected.as_str_name(), "new_state": HaState::InitializingToStandby.as_str_name(), "timestamp": now_in_millis(), "term": "0" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                // Expect one last HaScopeActorState update: Initializing_to_standby
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "ha_scope_state" },
                
                // Mock a bulkSyncCompleted message from the peer
                send! { key: BulkSyncUpdate::msg_key(&peer_scope_id), data: { "dst_actor_id": &scope_id, "finished": true }},
                // Expect a HaStateChanged: initializing_to_standby -> pending_standby_activation
                recv! { key: HAStateChanged::msg_key(&scope_id), data: { "prev_state": HaState::InitializingToStandby.as_str_name(), "new_state": HaState::PendingStandbyActivation.as_str_name(), "timestamp": now_in_millis(), "term": "0" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                // Mock the HaStateChanged from the peer HA scope that carries the new term
                send! { key: HAStateChanged::msg_key(&peer_scope_id), data: { "prev_state": HaState::PendingActiveActivation.as_str_name(), "new_state": HaState::Active.as_str_name(), "timestamp": now_in_millis(), "term": "1" } },
                // Expect one HaScopeActorState update: Pending_standby_activation
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "ha_scope_state" },
                

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
                            "ha_role": HaRole::Standby.as_str_name(),
                            "ha_term": "1",
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
                // Expect a HaStateChanged: pending_standby_activation -> standby
                recv! { key: HAStateChanged::msg_key(&scope_id), data: { "prev_state": HaState::PendingStandbyActivation.as_str_name(), "new_state": HaState::Standby.as_str_name(), "timestamp": now_in_millis(), "term": "1" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },
                // Expect one HaScopeActorState update: Standby
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "ha_scope_state" },
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
            dpu_ha_state_state.ha_term = "1".to_string();

            // Shutdown
            #[rustfmt::skip]
            let commands = [
                // Set the desired statet to Dead
                send! { key: HaScopeConfig::table_name(), data: { "key": &scope_id, "operation": "Set",
                        "field_values": {"json": format!(r#"{{"version":"3","disabled":false,"desired_ha_state":{},"owner":{},"ha_set_id":"{ha_set_id}","approved_pending_operation_ids":[]}}"#, DesiredHaState::Dead as i32, HaOwner::Switch as i32)},
                        },
                        addr: crate::common_bridge_sp::<HaScopeConfig>(&runtime.get_swbus_edge()) },

                // Expect a DPU DASH_HA_SCOPE_TABLE update
                recv! { key: &ha_set_id, data: {
                        "key": &ha_set_id,
                        "operation": "Set",
                        "field_values": {
                            "version": "3",
                            "ha_role": HaRole::Dead.as_str_name(),
                            "ha_term": "1",
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

                // Expect a HaStateChanged: Standby -> Destroying
                recv! { key: HAStateChanged::msg_key(&scope_id), data: { "prev_state": HaState::Standby.as_str_name(), "new_state": HaState::Destroying.as_str_name(), "timestamp": now_in_millis(), "term": "1" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },

                // Expect the HaScopeActorState update: Destroying
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "ha_scope_state" },

                // Send DPU DASH_HA_SCOPE_STATE with ha_role = dead
                send! { key: DpuDashHaScopeState::table_name(), data: {"key": DpuDashHaScopeState::table_name(), "operation": "Set",
                        "field_values": serde_json::to_value(to_field_values(&dpu_ha_state_state).unwrap()).unwrap()
                        }},

                // Expect a HaStateChanged: Destroying -> Dead
                recv! { key: HAStateChanged::msg_key(&scope_id), data: { "prev_state": HaState::Destroying.as_str_name(), "new_state": HaState::Dead.as_str_name(), "timestamp": now_in_millis(), "term": "1" }, addr: runtime.sp(HaScopeActor::name(), &peer_scope_id), exclude: "timestamp" },

                // Expect one last HaScopeActorState update: Dead
                recv! { key: HaScopeActorState::msg_key(&scope_id), data: { "owner": HaOwner::Switch as i32, "ha_scope_state": NpuDashHaScopeState::default(), "vdpu_id": &vdpu0_id, "peer_vdpu_id": &vdpu1_id }, addr: runtime.sp(HaSetActor::name(), &ha_set_id), exclude: "ha_scope_state" },

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
    }
}
