use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use swbus_actor::{spawn, Actor, ActorMessage, State};
use swbus_edge::swbus_proto::swbus::ServicePath;
use swss_common::{KeyOpFieldValues, SubscriberStateTable, Table};
use swss_common_bridge::consumer::spawn_consumer_bridge;

// pub async fn spawn_vdpu_actors() -> Result<()> {
//     // Get a list of vDPUs from CONFIG_DB::VDPU
//     let config_db = crate::db_named("CONFIG_DB").await?;
//     let mut vdpu_table = Table::new_async(config_db, "VDPU").await?;
//     let vdpu_ids = vdpu_table.get_keys_async().await?;

//     // Spawn DPU actors
//     for vdpu_id in &vdpu_ids {
//         let vdpu_fvs = vdpu_table.get_async(vdpu_id).await?.unwrap();
//         let vdpu: VDpu = swss_serde::from_field_values(&vdpu_fvs)?;
//         let actor = VDpuActor {
//             id: vdpu_id.clone(),
//             vdpu: vdpu.clone(),
//         };
//         spawn(actor, crate::sp("dpu", vdpu_id));
//     }

//     let rt = swbus_actor::get_global_runtime().as_ref().unwrap().get_swbus_edge();
//     // Spawn the CONFIG_DB::VDPU swss-common bridge
//     {
//         let rt = rt.clone();
//         let config_db = crate::db_named("CONFIG_DB").await?;
//         let vdpu_sst = SubscriberStateTable::new_async(config_db, "VPUD", None, None).await?;
//         let addr = crate::sp("swss-common-bridge", "VDPU");
//         spawn_consumer_bridge(rt, addr, vdpu_sst, |kfv| {
//             (crate::sp("VDPU", &kfv.key), "VDPU".to_owned())
//         });
//     }
//     Ok(())
// }

// /// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2111-dpu--vdpu-definitions>
// #[derive(Deserialize, Clone)]
// struct VDpu {
//     main_dpu_ids: Option<String>,
// }

// struct VDpuActor {
//     /// The id of this dpu
//     id: String,

//     /// Data from CONFIG_DB
//     vdpu: VDpu,
// }

// impl Actor for VDpuActor {
//     async fn handle_message(&mut self, state: &mut State, key: &str) -> Result<()> {
//         let (_internal, incoming, outgoing, registry) = state.get_all();

//         if key == "VDPU" {}
//         if let Some(vdpu_addr) = &self.vdpu_addr {
//             outgoing.send(vdpu_addr.clone(), msg);
//         }

//         Ok(())
//     }
// }
