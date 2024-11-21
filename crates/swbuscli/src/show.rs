use clap::Parser;
use serde_json;
use std::collections::HashMap;
use swbus_core::mux::nexthop::SwbusNextHopDisplay;
use swbus_proto::swbus::*;
use tabled::{Table, Tabled};
use tokio::sync::mpsc;
use tokio::time::{self, Duration};

const CMD_TIMEOUT: u64 = 10;

#[derive(Parser, Debug)]
pub struct ShowCmd {
    #[command(subcommand)]
    subcommand: ShowSub,
}

#[derive(Parser, Debug)]
enum ShowSub {
    Route(ShowRouteCmd),
}

#[derive(Parser, Debug)]
pub struct ShowRouteCmd {}

trait ShowCmdHandler {
    fn create_request(&self) -> ManagementRequest;
    fn process_response(&self, response: &ManagementResponse);
}

#[derive(Tabled)]
struct RouteDisplay {
    service_path: String,
    hop_count: u32,
    nh_id: String,
    nh_scope: String,
    nh_service_path: String,
}
impl super::CmdHandler for ShowCmd {
    async fn handle(&self, ctx: &super::CommandContext) {
        //Create a channel to receive response
        let (recv_queue_tx, mut recv_queue_rx) = mpsc::channel::<SwbusMessage>(1);
        let mut src_sp = ctx.sp.clone();
        src_sp.resource_type = "show".to_string();
        src_sp.resource_id = "0".to_string();
        let mut dst_sp = ctx.sp.clone_for_local_mgmt();

        //Register the channel to the runtime to receive response
        ctx.runtime
            .lock()
            .await
            .add_handler(src_sp.clone(), recv_queue_tx)
            .await
            .unwrap();

        let sub_cmd = match &self.subcommand {
            ShowSub::Route(show_route_args) => show_route_args,
        };

        let mgmt_request = sub_cmd.create_request();

        let request_msg = SwbusMessage {
            header: Some(SwbusMessageHeader::new(src_sp.clone(), dst_sp.clone())),
            body: Some(swbus_message::Body::ManagementRequest(mgmt_request)),
        };

        //Send request
        ctx.runtime.lock().await.send(request_msg).await.unwrap();

        //wait on the channel to receive response
        match time::timeout(Duration::from_secs(CMD_TIMEOUT), recv_queue_rx.recv()).await {
            Ok(Some(msg)) => match msg.body {
                Some(swbus_message::Body::ManagementResponse(response)) => {
                    sub_cmd.process_response(&response);
                }
                _ => println!("Invalid response"),
            },
            Ok(None) => println!("Channel closed"),
            Err(_) => println!("Request timeout"),
        }
    }
}

impl ShowCmdHandler for ShowRouteCmd {
    fn create_request(&self) -> ManagementRequest {
        ManagementRequest::new(&"show_route")
    }

    fn process_response(&self, response: &ManagementResponse) {
        let routes_json: HashMap<String, SwbusNextHopDisplay> = serde_json::from_str(&response.response).unwrap();

        let routes: Vec<RouteDisplay> = routes_json
            .iter()
            .map(|(key, value)| RouteDisplay {
                service_path: key.clone(),
                hop_count: value.hop_count,
                nh_id: value.nh_id.clone(),
                nh_scope: value.nh_scope.clone(),
                nh_service_path: value.nh_service_path.clone(),
            })
            .collect();
        let table = Table::new(routes);
        println!("{}", table)
    }
}
