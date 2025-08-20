mod route;

use clap::Parser;
use swbus_proto::swbus::*;

use crate::show::ShowCmdHandler;
use crate::CommandContext;

#[derive(Parser, Debug)]
pub struct ShowSwbusdCmd {
    #[command(subcommand)]
    subcommand: SwbusdCmd,
}

#[derive(Parser, Debug)]
enum SwbusdCmd {
    Route(route::ShowRouteCmd),
}

impl ShowCmdHandler for ShowSwbusdCmd {
    fn create_request(&self, ctx: &CommandContext, src_sp: &ServicePath) -> SwbusMessage {
        let SwbusdCmd::Route(sub_cmd) = &self.subcommand;
        sub_cmd.create_request(ctx, src_sp)
    }

    fn process_response(&self, ctx: &CommandContext, response: &RequestResponse) {
        let SwbusdCmd::Route(sub_cmd) = &self.subcommand;
        sub_cmd.process_response(ctx, response);
    }
}
