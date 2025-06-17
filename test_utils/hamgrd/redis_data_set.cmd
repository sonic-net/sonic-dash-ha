select 1
hset DEVICE_METADATA|localhost region region-a cluster cluster-a
HSET "LOOPBACK_INTERFACE|Loopback0|127.0.0.1/32" "NULL" "NULL"
hset LOOPBACK_INTERFACE|Loopback0 NULL NULL
HSET "DPU|switch1_dpu0" "dpu_id" "0"
HSET "DPU|switch1_dpu0" "gnmi_port" "50051"
HSET "DPU|switch1_dpu0" "local_port" "8080"
HSET "DPU|switch1_dpu0" "orchagent_zmq_port" "5555"
HSET "DPU|switch1_dpu0" "pa_ipv4" "18.0.202.1"
HSET "DPU|switch1_dpu0" "state" "up"
HSET "DPU|switch1_dpu0" "swbus_port" "23606"
HSET "DPU|switch1_dpu0" "vdpu_id" "vdpu0"
HSET "DPU|switch1_dpu0" "vip_ipv4" "3.2.1.0"
HSET "DPU|switch1_dpu0" "midplane_ipv4" "169.254.0.1"

HSET "REMOTE_DPU|switch2_dpu0" "dpu_id" "0"
HSET "REMOTE_DPU|switch2_dpu0" "type" "cluster"
HSET "REMOTE_DPU|switch2_dpu0" "npu_ipv4" "10.1.0.2"
HSET "REMOTE_DPU|switch2_dpu0" "pa_ipv4" "18.1.202.1"
HSET "REMOTE_DPU|switch2_dpu0" "swbus_port" "23606"
HSET "REMOTE_DPU|switch2_dpu1" "dpu_id" "1"
HSET "REMOTE_DPU|switch2_dpu1" "type" "cluster"
HSET "REMOTE_DPU|switch2_dpu1" "swbus_port" "23607"
HSET "REMOTE_DPU|switch2_dpu1" "npu_ipv4" "10.1.0.2"
HSET "REMOTE_DPU|switch2_dpu1" "pa_ipv4" "18.1.202.2"
HSET "REMOTE_DPU|switch3_dpu0" "dpu_id" "0"
HSET "REMOTE_DPU|switch3_dpu0" "type" "cluster"
HSET "REMOTE_DPU|switch3_dpu0" "npu_ipv4" "10.1.0.3"
HSET "REMOTE_DPU|switch3_dpu0" "pa_ipv4" "18.2.202.1"
HSET "REMOTE_DPU|switch3_dpu0" "swbus_port" "23606"

HSET "VDPU|vdpu0" "main_dpu_ids" "switch1_dpu0"
HSET "VDPU|vdpu1" "main_dpu_ids" "switch2_dpu0"

HSET "DASH_HA_GLOBAL_CONFIG|GLOBAL" "dpu_bfd_probe_interval_in_ms" "1000"
HSET "DASH_HA_GLOBAL_CONFIG|GLOBAL" "dpu_bfd_probe_multiplier" "3" 


select 3
HSET DPU_STATE|dpu0 dpu_midplane_link_state up
HSET DPU_STATE|dpu0 dpu_control_plane_state up
HSET DPU_STATE|dpu0 dpu_data_plane_state up
HSET DPU_STATE|dpu1 dpu_midplane_link_state up
HSET DPU_STATE|dpu1 dpu_control_plane_state up
HSET DPU_STATE|dpu1 dpu_data_plane_state up
HSET DPU_STATE|dpu2 dpu_midplane_link_state up
HSET DPU_STATE|dpu2 dpu_control_plane_state up
HSET DPU_STATE|dpu2 dpu_data_plane_state up
HSET DPU_STATE|dpu3 dpu_midplane_link_state up
HSET DPU_STATE|dpu3 dpu_control_plane_state up
HSET DPU_STATE|dpu3 dpu_data_plane_state up
HSET DPU_STATE|dpu4 dpu_midplane_link_state up
HSET DPU_STATE|dpu4 dpu_control_plane_state up
HSET DPU_STATE|dpu4 dpu_data_plane_state up
HSET DPU_STATE|dpu5 dpu_midplane_link_state up
HSET DPU_STATE|dpu5 dpu_control_plane_state up
HSET DPU_STATE|dpu5 dpu_data_plane_state up
HSET DPU_STATE|dpu6 dpu_midplane_link_state up
HSET DPU_STATE|dpu6 dpu_control_plane_state up
HSET DPU_STATE|dpu6 dpu_data_plane_state up
HSET DPU_STATE|dpu7 dpu_midplane_link_state up
HSET DPU_STATE|dpu7 dpu_control_plane_state up
HSET DPU_STATE|dpu7 dpu_data_plane_state up

select 4
HSET DASH_BFD_PROBE_STATE|dpu0 v4_bfd_up_sessions "10.1.0.32"