pub mod hamgrd;
pub mod swbusd;
use crate::wait_for_response;
use clap::Parser;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tracing::info;

const CMD_TIMEOUT: u32 = 10;

#[derive(Parser, Debug)]
pub struct ShowCmd {
    #[command(subcommand)]
    subcommand: ShowSubCmd,
}

#[derive(Parser, Debug)]
enum ShowSubCmd {
    Swbusd(swbusd::ShowSwbusdCmd),
    Hamgrd(hamgrd::ShowHamgrdCmd),
}

trait ShowCmdHandler {
    fn create_request(&self, ctx: &super::CommandContext, src_sp: &ServicePath) -> SwbusMessage;
    fn process_response(&self, response: &RequestResponse);
}

impl super::CmdHandler for ShowCmd {
    async fn handle(&self, ctx: &super::CommandContext) {
        // Create a channel to receive response
        let (recv_queue_tx, mut recv_queue_rx) = mpsc::channel::<SwbusMessage>(1);
        let mut src_sp = ctx.sp.clone();
        src_sp.resource_type = "show".to_string();
        src_sp.resource_id = "0".to_string();

        // Register the channel to the runtime to receive response
        ctx.runtime.add_handler(src_sp.clone(), recv_queue_tx);

        let sub_cmd: &dyn ShowCmdHandler = match &self.subcommand {
            ShowSubCmd::Swbusd(swbusd_cmd) => swbusd_cmd,
            ShowSubCmd::Hamgrd(hamgrd_cmd) => hamgrd_cmd,
        };

        let request_msg = sub_cmd.create_request(ctx, &src_sp);

        let request_id = request_msg.header.as_ref().unwrap().id;

        // Send request
        ctx.runtime.send(request_msg).await.unwrap();

        // wait on the channel to receive response
        let result = wait_for_response(&mut recv_queue_rx, request_id, CMD_TIMEOUT).await;
        match result.error_code {
            SwbusErrorCode::Ok => {
                let body = result.msg.unwrap().body.unwrap();
                match body {
                    swbus_message::Body::Response(response) => {
                        sub_cmd.process_response(&response);
                    }
                    _ => {
                        info!("Invalid response");
                    }
                }
            }
            SwbusErrorCode::Timeout => {
                info!("Request timeout");
            }
            _ => {
                info!("{}:{}", result.error_code.as_str_name(), result.error_message);
            }
        }
    }
}
