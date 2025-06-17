# execute below commands in redis-cli to simulate the flow in planned ha up
# approved_pending_operation_ids needs to be updated with the actual value from STATE_DB/DASH_HA_SCOPE_STATE
select 4
HSET DASH_HA_SCOPE_STATE|haset0_0 last_updated_time "1718053542000" ha_role "dead" ha_role_start_time "1718053542000" ha_term "1" activate_role_pending "false" flow_reconcile_pending "false" brainsplit_recover_pending "false"

select 0
HSET DASH_HA_SCOPE_CONFIG_TABLE:vdpu0:haset0_0 disable "true"

select 4
HSET DASH_HA_SCOPE_STATE|haset0_0 activate_role_pending "true"

select 0
HSET DASH_HA_SCOPE_CONFIG_TABLE:vdpu0:haset0_0 version "3" approved_pending_operation_ids "8dda84ea-fdb4-4edd-b88c-55fc96d02a6f"

select 4
HSET DASH_HA_SCOPE_STATE|haset0_0 activate_role_pending "false" ha_role "active" ha_role_start_time "1718053552000" ha_term "2"

select 4
HSET DASH_BFD_PROBE_STATE|dpu0 v4_bfd_up_sessions "10.1.0.3"


# execute below commands in redis-cli to simulate the flow in planned ha down
select 0
HSET DASH_HA_SCOPE_CONFIG_TABLE:vdpu0:haset0_0 version "3" desired_ha_state "dead"