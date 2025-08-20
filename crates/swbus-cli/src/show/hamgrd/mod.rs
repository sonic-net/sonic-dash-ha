mod actor;
use clap::Parser;
use swbus_proto::swbus::*;

use crate::show::ShowCmdHandler;
use crate::CommandContext;

#[derive(Parser, Debug)]
pub struct ShowHamgrdCmd {
    #[command(subcommand)]
    subcommand: HamgrdCmd,
}

#[derive(Parser, Debug)]
enum HamgrdCmd {
    Actor(actor::ShowActorCmd),
}

impl ShowCmdHandler for ShowHamgrdCmd {
    fn create_request(&self, ctx: &CommandContext, src_sp: &ServicePath) -> SwbusMessage {
        let HamgrdCmd::Actor(sub_cmd) = &self.subcommand;
        sub_cmd.create_request(ctx, src_sp)
    }

    fn process_response(&self, ctx: &CommandContext, response: &RequestResponse) {
        let HamgrdCmd::Actor(sub_cmd) = &self.subcommand;
        sub_cmd.process_response(ctx, response);
    }
}
