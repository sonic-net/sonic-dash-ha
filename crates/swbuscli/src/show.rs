use crate::wait_for_response;
use clap::Parser;
use swbus_proto::swbus::*;
use tabled::{Table, Tabled};
use tokio::sync::mpsc;

const CMD_TIMEOUT: u32 = 10;

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
    fn process_response(&self, response: &RequestResponse);
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
        let dst_sp = ctx.sp.clone_for_local_mgmt();

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
        let header = SwbusMessageHeader::new(src_sp.clone(), dst_sp.clone(), ctx.id_generator.generate());
        let request_id = header.id;
        let request_msg = SwbusMessage {
            header: Some(header),
            body: Some(swbus_message::Body::ManagementRequest(mgmt_request)),
        };

        //Send request
        ctx.runtime.lock().await.send(request_msg).await.unwrap();

        //wait on the channel to receive response
        let result = wait_for_response(&mut recv_queue_rx, request_id, CMD_TIMEOUT).await;
        match result.error_code {
            SwbusErrorCode::Ok => {
                let body = result.msg.unwrap().body.unwrap();
                match body {
                    swbus_message::Body::Response(response) => {
                        sub_cmd.process_response(&response);
                    }
                    _ => {
                        println!("Invalid response");
                    }
                }
            }
            SwbusErrorCode::Timeout => {
                println!("Request timeout");
            }
            _ => {
                println!("{}:{}", result.error_code.as_str_name(), result.error_message);
            }
        }
    }
}

impl ShowCmdHandler for ShowRouteCmd {
    fn create_request(&self) -> ManagementRequest {
        ManagementRequest::new(&"show_route")
    }

    fn process_response(&self, response: &RequestResponse) {
        let routes = match &response.response_body {
            Some(request_response::ResponseBody::RouteQueryResult(route_result)) => route_result,
            _ => {
                println!("Expecting RouteQueryResult but got something else: {:?}", response);
                return;
            }
        };

        let routes: Vec<RouteDisplay> = routes
            .entries
            .iter()
            .map(|entry| RouteDisplay {
                service_path: entry
                    .service_path
                    .as_ref()
                    .expect("service_path in RouteQueryResult cannot be None")
                    .to_longest_path(),
                hop_count: entry.hop_count,
                nh_id: entry.nh_id.clone(),
                nh_scope: Scope::try_from(entry.nh_scope).unwrap().as_str_name().to_string(),
                nh_service_path: entry
                    .nh_service_path
                    .as_ref()
                    .expect("nh_service_path in RouteQueryResult cannot be None")
                    .to_longest_path(),
            })
            .collect();
        let table = Table::new(routes);
        println!("{}", table)
    }
}
