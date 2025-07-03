select 4
hset DEVICE_METADATA|localhost region region-a cluster cluster-a
HSET "LOOPBACK_INTERFACE|Loopback0|127.0.0.1/32" "NULL" "NULL"
hset LOOPBACK_INTERFACE|Loopback0 NULL NULL
HSET "DPU|dpu0" "dpu_id" "0"
HSET "DPU|dpu0" "gnmi_port" "50051"
HSET "DPU|dpu0" "local_port" "8080"
HSET "DPU|dpu0" "orchagent_zmq_port" "5555"
HSET "DPU|dpu0" "pa_ipv4" "18.0.202.1"
HSET "DPU|dpu0" "state" "up"
HSET "DPU|dpu0" "swbus_port" "23606"
HSET "DPU|dpu0" "vdpu_id" "vdpu0"
HSET "DPU|dpu0" "vip_ipv4" "3.2.1.0"
HSET "DPU|dpu0" "midplane_ipv4" "169.254.0.1"

HSET "DPU|dpu1" "dpu_id" "1"
HSET "DPU|dpu1" "gnmi_port" "50051"
HSET "DPU|dpu1" "local_port" "8080"
HSET "DPU|dpu1" "orchagent_zmq_port" "5555"
HSET "DPU|dpu1" "pa_ipv4" "18.1.202.1"
HSET "DPU|dpu1" "state" "up"
HSET "DPU|dpu1" "swbus_port" "23607"
HSET "DPU|dpu1" "vdpu_id" "vdpu1"
HSET "DPU|dpu1" "vip_ipv4" "3.2.1.1"
HSET "DPU|dpu1" "midplane_ipv4" "169.254.1.1"

HSET "DPU|dpu2" "dpu_id" "2"
HSET "DPU|dpu2" "gnmi_port" "50051"
HSET "DPU|dpu2" "local_port" "8080"
HSET "DPU|dpu2" "orchagent_zmq_port" "5555"
HSET "DPU|dpu2" "pa_ipv4" "18.2.202.1"
HSET "DPU|dpu2" "state" "up"
HSET "DPU|dpu2" "swbus_port" "23608"
HSET "DPU|dpu2" "vdpu_id" "vdpu2"
HSET "DPU|dpu2" "vip_ipv4" "3.2.1.2"
HSET "DPU|dpu2" "midplane_ipv4" "169.254.2.1"

HSET "DPU|dpu3" "dpu_id" "3"
HSET "DPU|dpu3" "gnmi_port" "50051"
HSET "DPU|dpu3" "local_port" "8080"
HSET "DPU|dpu3" "orchagent_zmq_port" "5555"
HSET "DPU|dpu3" "pa_ipv4" "18.3.202.1"
HSET "DPU|dpu3" "state" "up"
HSET "DPU|dpu3" "swbus_port" "23609"
HSET "DPU|dpu3" "vdpu_id" "vdpu3"
HSET "DPU|dpu3" "vip_ipv4" "3.2.1.3"
HSET "DPU|dpu3" "midplane_ipv4" "169.254.3.1"

HSET "DPU|dpu4" "dpu_id" "4"
HSET "DPU|dpu4" "gnmi_port" "50051"
HSET "DPU|dpu4" "local_port" "8080"
HSET "DPU|dpu4" "orchagent_zmq_port" "5555"
HSET "DPU|dpu4" "pa_ipv4" "18.4.202.1"
HSET "DPU|dpu4" "state" "up"
HSET "DPU|dpu4" "swbus_port" "23610"
HSET "DPU|dpu4" "vdpu_id" "vdpu4"
HSET "DPU|dpu4" "vip_ipv4" "3.2.1.4"
HSET "DPU|dpu4" "midplane_ipv4" "169.254.4.1"

HSET "DPU|dpu5" "dpu_id" "5"
HSET "DPU|dpu5" "gnmi_port" "50051"
HSET "DPU|dpu5" "local_port" "8080"
HSET "DPU|dpu5" "orchagent_zmq_port" "5555"
HSET "DPU|dpu5" "pa_ipv4" "18.5.202.1"
HSET "DPU|dpu5" "state" "up"
HSET "DPU|dpu5" "swbus_port" "23611"
HSET "DPU|dpu5" "vdpu_id" "vdpu5"
HSET "DPU|dpu5" "vip_ipv4" "3.2.1.5"
HSET "DPU|dpu5" "midplane_ipv4" "169.254.5.1"

HSET "DPU|dpu6" "dpu_id" "6"
HSET "DPU|dpu6" "gnmi_port" "50051"
HSET "DPU|dpu6" "local_port" "8080"
HSET "DPU|dpu6" "orchagent_zmq_port" "5555"
HSET "DPU|dpu6" "pa_ipv4" "18.6.202.1"
HSET "DPU|dpu6" "state" "up"
HSET "DPU|dpu6" "swbus_port" "23612"
HSET "DPU|dpu6" "vdpu_id" "vdpu6"
HSET "DPU|dpu6" "vip_ipv4" "3.2.1.6"
HSET "DPU|dpu6" "midplane_ipv4" "169.254.6.1"

HSET "DPU|dpu7" "dpu_id" "7"
HSET "DPU|dpu7" "gnmi_port" "50051"
HSET "DPU|dpu7" "local_port" "8080"
HSET "DPU|dpu7" "orchagent_zmq_port" "5555"
HSET "DPU|dpu7" "pa_ipv4" "18.7.202.1"
HSET "DPU|dpu7" "state" "up"
HSET "DPU|dpu7" "swbus_port" "23613"
HSET "DPU|dpu7" "vdpu_id" "vdpu7"
HSET "DPU|dpu7" "vip_ipv4" "3.2.1.7"
HSET "DPU|dpu7" "midplane_ipv4" "169.254.7.1"

HSET "REMOTE_DPU|switch2_dpu0" "dpu_id" "0"
HSET "REMOTE_DPU|switch2_dpu0" "type" "cluster"
HSET "REMOTE_DPU|switch2_dpu0" "npu_ipv4" "10.1.0.2"
HSET "REMOTE_DPU|switch2_dpu0" "swbus_port" "23606"
HSET "REMOTE_DPU|switch2_dpu1" "dpu_id" "1"
HSET "REMOTE_DPU|switch2_dpu1" "type" "cluster"
HSET "REMOTE_DPU|switch2_dpu1" "swbus_port" "23607"
HSET "REMOTE_DPU|switch2_dpu1" "npu_ipv4" "10.1.0.2"
HSET "REMOTE_DPU|switch3_dpu0" "dpu_id" "0"
HSET "REMOTE_DPU|switch3_dpu0" "type" "cluster"
HSET "REMOTE_DPU|switch3_dpu0" "npu_ipv4" "10.1.0.3"
HSET "REMOTE_DPU|switch3_dpu0" "swbus_port" "23606"

HSET "DASH_HA_GLOBAL_CONFIG|GLOBAL" "dpu_bfd_probe_interval_in_ms" "1000"
HSET "DASH_HA_GLOBAL_CONFIG|GLOBAL" "dpu_bfd_probe_multiplier" "3"


select 13
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

select 17
HSET DASH_BFD_PROBE_STATE|dpu0 v4_bfd_up_sessions "10.1.0.32"